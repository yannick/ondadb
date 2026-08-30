//! Committed-span index (1.2): the conflict domain a range delete needs.
//!
//! ondaDB's point conflict check is
//! [`ColumnFamily::peek_seq`](crate::column_family::ColumnFamily) — "is the
//! newest sequence at this key above my read sequence". A range delete has no
//! key to peek at: it must ask "did anything in `[start, end)` change since my
//! snapshot", which the LSM cannot answer without scanning the span. This index
//! is the answer: a small, bounded record of *what committed recently*, pruned
//! at the oldest live snapshot.
//!
//! It is **inert until [`CAP_RANGE_DELETES`](crate::format::CAP_RANGE_DELETES)
//! is enabled.** A database that never issues a range delete inserts no marker,
//! takes no lock and allocates nothing here; the gate is one relaxed load of
//! the capability word on the commit path.
//!
//! # Locking
//!
//! [`SpanIndex`]'s mutex sits immediately **after** `DbInner::commit_mu` and
//! **before** `DbInner::manifest_mu` (see `docs/concurrency-and-safety.md`). It
//! is taken while `commit_mu` is held (marker insert, conflict check) and alone
//! (pruning, and the capacity reservation). It is never taken before
//! `commit_mu` on the commit path — the capacity wait happens *ahead* of
//! `commit_mu` precisely so a blocked range writer cannot stall every
//! Snapshot/Serializable commit in the database.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::comparator::ComparatorRef;
use crate::error::{OndaError, Result};

/// One committed write recorded for conflict detection.
///
/// A point write records its key with `end = None`; a range delete records the
/// half-open interval. Keeping both in one ordered structure is what lets a
/// range writer and a point writer consult the same list.
#[derive(Debug, Clone)]
struct Marker {
    cf: u64,
    start: Vec<u8>,
    /// Exclusive upper bound, or `None` for a point marker (covers `start`
    /// alone).
    end: Option<Vec<u8>>,
    seq: u64,
}

impl Marker {
    /// Does this marker touch `[start, end)`?
    fn overlaps(&self, cmp: &ComparatorRef, start: &[u8], end: &[u8]) -> bool {
        match &self.end {
            None => {
                cmp.compare(&self.start, start).is_ge() && cmp.compare(&self.start, end).is_lt()
            }
            Some(mine_end) => {
                cmp.compare(&self.start, end).is_lt() && cmp.compare(start, mine_end).is_lt()
            }
        }
    }

    /// Does this marker cover the single key `key`?
    fn covers(&self, cmp: &ComparatorRef, key: &[u8]) -> bool {
        match &self.end {
            None => cmp.compare(&self.start, key).is_eq(),
            Some(end) => cmp.compare(&self.start, key).is_le() && cmp.compare(end, key).is_gt(),
        }
    }
}

#[derive(Default)]
struct Inner {
    markers: Vec<Marker>,
    /// Slots promised to callers that have reserved but not yet inserted.
    reserved: usize,
}

/// The per-database committed-span index.
pub(crate) struct SpanIndex {
    inner: Mutex<Inner>,
    cond: Condvar,
    /// Maximum markers held at once (`Options::span_index_capacity`).
    capacity: usize,
    /// Markers dropped by pruning, for `CfStats`/tests.
    pruned: AtomicU64,
    /// Highest sequence whose marker the index could **not** record.
    ///
    /// A point-only commit must never block on a bookkeeping structure — the
    /// whole database's write path would stall behind one long-lived snapshot.
    /// So it takes its slots with [`try_reserve`](Self::try_reserve) and, when
    /// the index is full, gives up the marker and raises this watermark
    /// instead. A range writer reading at or below the watermark then conflicts
    /// unconditionally: something in that window committed and the index cannot
    /// say what. Conservative in exactly one direction — it can refuse a range
    /// commit that would have been safe, never admit one that would not.
    ///
    /// Range commits, by contrast, *wait*: they are the rare bulk operation,
    /// and dropping one of their markers would blind every later point writer.
    overflow_seq: AtomicU64,
}

impl std::fmt::Debug for SpanIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpanIndex")
            .field("markers", &self.inner.lock().markers.len())
            .field("capacity", &self.capacity)
            .finish()
    }
}

/// How long a capacity wait sleeps before re-checking.
///
/// Capacity is freed by *pruning*, which is driven by the oldest live snapshot
/// — and a snapshot is released without notifying this index. Polling rather
/// than relying purely on the condvar is what keeps a waiter from sleeping
/// through the release that unblocked it.
const CAPACITY_POLL: Duration = Duration::from_millis(2);

/// How long a range commit waits for span-index room before giving up its
/// markers and raising the overflow watermark instead.
///
/// A bound, not a policy change: capacity is freed by pruning, pruning is
/// driven by the oldest live snapshot, and nothing guarantees that snapshot
/// ever advances. Waiting forever would turn a stuck reader into a stuck
/// writer; timing out degrades to the conservative path a point commit already
/// takes, which refuses range commits it cannot prove safe rather than
/// admitting ones it cannot.
const CAPACITY_WAIT: Duration = Duration::from_secs(2);

/// Capacity reservation, released on drop.
///
/// A reservation is taken *before* `commit_mu` and consumed at insert. Drop
/// returns whatever was not consumed, so every commit exit path — conflict,
/// apply failure, panic — gives the slots back.
pub(crate) struct SpanReservation<'a> {
    index: &'a SpanIndex,
    outstanding: usize,
}

impl std::fmt::Debug for SpanReservation<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpanReservation")
            .field("outstanding", &self.outstanding)
            .finish()
    }
}

impl Drop for SpanReservation<'_> {
    fn drop(&mut self) {
        if self.outstanding == 0 {
            return;
        }
        {
            let mut g = self.index.inner.lock();
            g.reserved = g.reserved.saturating_sub(self.outstanding);
        }
        self.index.cond.notify_all();
    }
}

impl SpanIndex {
    pub(crate) fn new(capacity: usize) -> SpanIndex {
        SpanIndex {
            inner: Mutex::new(Inner::default()),
            cond: Condvar::new(),
            capacity: capacity.max(1),
            pruned: AtomicU64::new(0),
            overflow_seq: AtomicU64::new(0),
        }
    }

    /// Record that a commit at `seq` could not be indexed.
    pub(crate) fn note_overflow(&self, seq: u64) {
        self.overflow_seq.fetch_max(seq, Ordering::Relaxed);
    }

    /// Reserve `n` slots if they are free right now, without blocking.
    ///
    /// The point-commit path: `None` means "commit anyway, and raise the
    /// overflow watermark".
    pub(crate) fn try_reserve(&self, n: usize) -> Option<SpanReservation<'_>> {
        if n == 0 {
            return Some(SpanReservation {
                index: self,
                outstanding: 0,
            });
        }
        let mut g = self.inner.lock();
        if g.markers.len() + g.reserved + n <= self.capacity {
            g.reserved += n;
            return Some(SpanReservation {
                index: self,
                outstanding: n,
            });
        }
        None
    }

    /// Markers currently held.
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().markers.len()
    }

    /// Markers dropped by pruning over this database's lifetime.
    #[cfg(test)]
    pub(crate) fn pruned(&self) -> u64 {
        self.pruned.load(Ordering::Relaxed)
    }

    /// Drop every marker at or below `below` — the oldest live snapshot.
    ///
    /// A marker below the oldest snapshot can no longer be the *newer* side of
    /// any conflict: every transaction still running reads at or above it.
    pub(crate) fn prune(&self, below: u64) {
        let mut g = self.inner.lock();
        let before = g.markers.len();
        g.markers.retain(|m| m.seq > below);
        let dropped = before - g.markers.len();
        drop(g);
        // The watermark ages out with the markers: once every transaction reads
        // at or above it, the window it stood for is closed.
        let _ = self
            .overflow_seq
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                (cur != 0 && cur <= below).then_some(0)
            });
        if dropped > 0 {
            self.pruned.fetch_add(dropped as u64, Ordering::Relaxed);
            self.cond.notify_all();
        }
    }

    /// Reserve `n` marker slots, blocking until they are available.
    ///
    /// **Called with no other lock held**, ahead of `commit_mu`: waiting under
    /// `commit_mu` would stall every Snapshot/Serializable commit in the
    /// database behind one range writer, and could convoy against the pruner.
    ///
    /// `oldest` is re-read on each pass so a snapshot released while waiting
    /// frees capacity here.
    /// `futile` reports that waiting cannot help — the caller's own snapshot is
    /// the prune floor, so no amount of waiting will free a slot. Without it a
    /// Snapshot transaction that filled the index could block on itself.
    pub(crate) fn reserve(
        &self,
        n: usize,
        oldest: impl Fn() -> u64,
        cancelled: impl Fn() -> bool,
        futile: impl Fn() -> bool,
    ) -> Result<Option<SpanReservation<'_>>> {
        if n == 0 {
            return Ok(Some(SpanReservation {
                index: self,
                outstanding: 0,
            }));
        }
        if n > self.capacity {
            return Err(OndaError::TooLarge(format!(
                "transaction needs {n} span-index slots; the index holds {} \
                 (raise Options::span_index_capacity)",
                self.capacity
            )));
        }
        let deadline = std::time::Instant::now() + CAPACITY_WAIT;
        loop {
            self.prune(oldest());
            {
                let mut g = self.inner.lock();
                if g.markers.len() + g.reserved + n <= self.capacity {
                    g.reserved += n;
                    return Ok(Some(SpanReservation {
                        index: self,
                        outstanding: n,
                    }));
                }
                self.cond.wait_for(&mut g, CAPACITY_POLL);
            }
            if cancelled() {
                return Err(OndaError::InvalidDb(
                    "database is closing while waiting for span-index capacity".into(),
                ));
            }
            if futile() || std::time::Instant::now() >= deadline {
                return Ok(None);
            }
        }
    }

    /// Whether any marker newer than `read_seq` overlaps `[start, end)` in
    /// column family `cf` — the range writer's conflict test.
    pub(crate) fn range_conflict(
        &self,
        cf: u64,
        cmp: &ComparatorRef,
        start: &[u8],
        end: &[u8],
        read_seq: u64,
    ) -> Option<u64> {
        let overflow = self.overflow_seq.load(Ordering::Relaxed);
        if overflow > read_seq {
            return Some(overflow);
        }
        let g = self.inner.lock();
        g.markers
            .iter()
            .find(|m| m.cf == cf && m.seq > read_seq && m.overlaps(cmp, start, end))
            .map(|m| m.seq)
    }

    /// Whether a **range** marker newer than `read_seq` covers `key` — the
    /// extra test a point writer performs.
    ///
    /// Point-vs-point conflicts stay with `peek_seq`, which sees the maximum
    /// sequence at a key regardless of kind; this only adds the coverage a
    /// point lookup cannot see.
    pub(crate) fn point_conflict(
        &self,
        cf: u64,
        cmp: &ComparatorRef,
        key: &[u8],
        read_seq: u64,
    ) -> Option<u64> {
        // Symmetric with `range_conflict`: a range commit that could not be
        // indexed raises the same watermark, and a point writer reading below
        // it cannot know whether a tombstone now covers its key.
        let overflow = self.overflow_seq.load(Ordering::Relaxed);
        if overflow > read_seq {
            return Some(overflow);
        }
        let g = self.inner.lock();
        g.markers
            .iter()
            .find(|m| m.cf == cf && m.end.is_some() && m.seq > read_seq && m.covers(cmp, key))
            .map(|m| m.seq)
    }

    /// Record one point write. Consumes a reserved slot.
    pub(crate) fn insert_point(
        &self,
        res: &mut SpanReservation<'_>,
        cf: u64,
        key: &[u8],
        seq: u64,
    ) {
        self.insert(
            res,
            Marker {
                cf,
                start: key.to_vec(),
                end: None,
                seq,
            },
        );
    }

    /// Record one range delete. Consumes a reserved slot.
    pub(crate) fn insert_range(
        &self,
        res: &mut SpanReservation<'_>,
        cf: u64,
        start: &[u8],
        end: &[u8],
        seq: u64,
    ) {
        self.insert(
            res,
            Marker {
                cf,
                start: start.to_vec(),
                end: Some(end.to_vec()),
                seq,
            },
        );
    }

    fn insert(&self, res: &mut SpanReservation<'_>, marker: Marker) {
        debug_assert!(
            res.outstanding > 0,
            "every marker consumes a slot reserved before commit_mu"
        );
        let mut g = self.inner.lock();
        if res.outstanding > 0 {
            res.outstanding -= 1;
            g.reserved = g.reserved.saturating_sub(1);
        }
        g.markers.push(marker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::default_comparator;

    fn idx() -> SpanIndex {
        SpanIndex::new(16)
    }

    fn res(i: &SpanIndex, n: usize) -> SpanReservation<'_> {
        i.reserve(n, || 0, || false, || false)
            .expect("capacity available")
            .expect("slots granted")
    }

    #[test]
    fn range_marker_conflicts_with_an_overlapping_point_marker() {
        let i = idx();
        let cmp = default_comparator();
        let mut r = res(&i, 1);
        i.insert_point(&mut r, 1, b"m", 10);
        assert_eq!(i.range_conflict(1, &cmp, b"a", b"z", 5), Some(10));
        // Not newer than the reader's snapshot: no conflict.
        assert_eq!(i.range_conflict(1, &cmp, b"a", b"z", 10), None);
        // Different column family.
        assert_eq!(i.range_conflict(2, &cmp, b"a", b"z", 5), None);
        // Disjoint span; `m` is outside [a, m).
        assert_eq!(i.range_conflict(1, &cmp, b"a", b"m", 5), None);
    }

    #[test]
    fn point_conflict_sees_only_covering_range_markers() {
        let i = idx();
        let cmp = default_comparator();
        let mut r = res(&i, 2);
        i.insert_range(&mut r, 1, b"a", b"m", 10);
        i.insert_point(&mut r, 1, b"q", 11);
        assert_eq!(i.point_conflict(1, &cmp, b"c", 5), Some(10));
        assert_eq!(i.point_conflict(1, &cmp, b"m", 5), None, "end is exclusive");
        // A point marker is never a point-vs-point conflict here: `peek_seq`
        // owns that comparison.
        assert_eq!(i.point_conflict(1, &cmp, b"q", 5), None);
    }

    #[test]
    fn pruning_drops_markers_at_or_below_the_oldest_snapshot() {
        let i = idx();
        let mut r = res(&i, 3);
        i.insert_point(&mut r, 1, b"a", 5);
        i.insert_point(&mut r, 1, b"b", 10);
        i.insert_point(&mut r, 1, b"c", 15);
        assert_eq!(i.len(), 3);
        i.prune(10);
        assert_eq!(i.len(), 1);
        assert_eq!(i.pruned(), 2);
    }

    #[test]
    fn a_dropped_reservation_returns_its_slots() {
        let i = SpanIndex::new(2);
        {
            let _r = res(&i, 2);
            // Fully reserved: a further reservation cannot be satisfied.
            assert!(i
                .reserve(1, || 0, || true, || false)
                .is_err_and(|e| e.kind() == "invalid_db"));
            // ... and a futile wait degrades to the overflow path at once.
            assert!(i.reserve(1, || 0, || false, || true).unwrap().is_none());
        }
        // The dropped reservation gave both slots back.
        let _r = res(&i, 2);
    }

    #[test]
    fn a_reservation_larger_than_the_index_is_refused_rather_than_blocking() {
        let i = SpanIndex::new(4);
        let e = i.reserve(5, || 0, || false, || false).unwrap_err();
        assert_eq!(e.kind(), "too_large");
    }
}
