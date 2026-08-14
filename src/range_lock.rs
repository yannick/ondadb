//! Per-column-family key-range locks.
//!
//! Before 0.8.0 a single `compact_mu` mutex served two jobs: it kept two
//! compaction runs from racing, and it kept the parts/tiers operations
//! (`detach_part`, `attach_part`, `attach_part_by_ref`, `relocate_part`) from
//! having the bottom level rewritten under them between snapshot and removal.
//! Partial-level compaction makes the first job too coarse — two compactions on
//! disjoint key ranges share no inputs and no outputs, so serializing them
//! wastes every compaction thread but one — while the second job still has to
//! hold.
//!
//! This module replaces that one mutex with a set of held key ranges. A caller
//! declares the span it is about to rewrite and gets a guard; overlapping spans
//! are excluded, disjoint ones proceed together. The two callers want opposite
//! failure behaviour, so both are offered:
//!
//! * [`RangeLocks::try_acquire`] — compaction. Returns `None` on conflict; the
//!   picker skips that candidate and chooses another. Never blocks a worker on
//!   work someone else is already doing.
//! * [`RangeLocks::acquire_blocking`] — the parts/tiers operations. They are
//!   user-initiated and must not fail spuriously, so they wait.
//!
//! Ranges are inclusive `[min, max]` byte spans compared with the column
//! family's comparator. A `None` bound means "unbounded on that side", so a
//! whole-keyspace lock is `(None, None)`.
//!
//! Guards release on drop — including on panic and on the `?` early returns
//! that the parts code is full of, which is why this is a guard and not a
//! release call.

use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::comparator::ComparatorRef;

/// An inclusive key span. `None` is unbounded on that side.
#[derive(Debug, Clone, Default)]
pub(crate) struct KeyRange {
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
}

impl KeyRange {
    /// The whole keyspace — what an operation takes when it cannot name a
    /// narrower span.
    pub(crate) fn all() -> Self {
        KeyRange {
            min: None,
            max: None,
        }
    }

    pub(crate) fn new(min: Vec<u8>, max: Vec<u8>) -> Self {
        KeyRange {
            min: Some(min),
            max: Some(max),
        }
    }

    /// The span covering every key in `spans`, or the whole keyspace if any
    /// span is itself unbounded. `None` when `spans` is empty.
    pub(crate) fn union<'a, I>(spans: I, cmp: &ComparatorRef) -> Option<KeyRange>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    {
        let mut it = spans.into_iter();
        let (lo, hi) = it.next()?;
        let mut min = lo.to_vec();
        let mut max = hi.to_vec();
        for (l, h) in it {
            if cmp.compare(l, &min).is_lt() {
                min = l.to_vec();
            }
            if cmp.compare(h, &max).is_gt() {
                max = h.to_vec();
            }
        }
        Some(KeyRange::new(min, max))
    }

    /// Do the two inclusive spans share any key?
    fn overlaps(&self, other: &KeyRange, cmp: &ComparatorRef) -> bool {
        // Disjoint iff one ends strictly before the other begins. An unbounded
        // side can never end before anything, so it always overlaps.
        let a_before_b = match (&self.max, &other.min) {
            (Some(amax), Some(bmin)) => cmp.compare(amax, bmin).is_lt(),
            _ => false,
        };
        let b_before_a = match (&other.max, &self.min) {
            (Some(bmax), Some(amin)) => cmp.compare(bmax, amin).is_lt(),
            _ => false,
        };
        !(a_before_b || b_before_a)
    }
}

#[derive(Default)]
struct Held {
    /// Ranges currently locked, each with the token that identifies its guard.
    ranges: Vec<(u64, KeyRange)>,
    next_token: u64,
}

/// The set of key ranges currently being rewritten in one column family.
pub(crate) struct RangeLocks {
    held: Mutex<Held>,
    freed: Condvar,
    cmp: ComparatorRef,
}

impl std::fmt::Debug for RangeLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeLocks")
            .field("held", &self.held.lock().ranges.len())
            .finish()
    }
}

impl RangeLocks {
    pub(crate) fn new(cmp: ComparatorRef) -> Arc<Self> {
        Arc::new(RangeLocks {
            held: Mutex::new(Held::default()),
            freed: Condvar::new(),
            cmp,
        })
    }

    /// Take `range` if nothing overlapping is held. Returns `None` on conflict
    /// rather than waiting — the caller is expected to pick different work.
    pub(crate) fn try_acquire(self: &Arc<Self>, range: KeyRange) -> Option<RangeGuard> {
        let mut held = self.held.lock();
        if held
            .ranges
            .iter()
            .any(|(_, r)| r.overlaps(&range, &self.cmp))
        {
            return None;
        }
        let token = held.next_token;
        held.next_token += 1;
        held.ranges.push((token, range));
        Some(RangeGuard {
            locks: Arc::clone(self),
            token,
        })
    }

    /// Take `range`, waiting until every overlapping range is released.
    pub(crate) fn acquire_blocking(self: &Arc<Self>, range: KeyRange) -> RangeGuard {
        let mut held = self.held.lock();
        loop {
            if !held
                .ranges
                .iter()
                .any(|(_, r)| r.overlaps(&range, &self.cmp))
            {
                let token = held.next_token;
                held.next_token += 1;
                held.ranges.push((token, range));
                return RangeGuard {
                    locks: Arc::clone(self),
                    token,
                };
            }
            self.freed.wait(&mut held);
        }
    }

    /// Is anything at all locked? Test/observability hook.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.held.lock().ranges.is_empty()
    }

    fn release(&self, token: u64) {
        let mut held = self.held.lock();
        held.ranges.retain(|(t, _)| *t != token);
        drop(held);
        // Any waiter may now be unblocked; each re-checks its own overlap.
        self.freed.notify_all();
    }
}

/// Releases its range on drop.
pub(crate) struct RangeGuard {
    locks: Arc<RangeLocks>,
    token: u64,
}

impl Drop for RangeGuard {
    fn drop(&mut self) {
        self.locks.release(self.token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::default_comparator;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn locks() -> Arc<RangeLocks> {
        RangeLocks::new(default_comparator())
    }

    fn r(lo: &str, hi: &str) -> KeyRange {
        KeyRange::new(lo.as_bytes().to_vec(), hi.as_bytes().to_vec())
    }

    #[test]
    fn disjoint_ranges_are_held_concurrently() {
        let l = locks();
        let _a = l.try_acquire(r("a", "c")).expect("first");
        let _b = l.try_acquire(r("d", "f")).expect("disjoint must not conflict");
    }

    #[test]
    fn overlapping_ranges_conflict() {
        let l = locks();
        let _a = l.try_acquire(r("a", "m")).expect("first");
        assert!(l.try_acquire(r("f", "z")).is_none(), "overlap must conflict");
    }

    /// Inclusive bounds: sharing exactly one key is still an overlap. A
    /// half-open reading here would let a compaction and a tier move rewrite
    /// the same boundary table.
    #[test]
    fn touching_bounds_overlap() {
        let l = locks();
        let _a = l.try_acquire(r("a", "m")).expect("first");
        assert!(l.try_acquire(r("m", "z")).is_none(), "shared bound overlaps");
    }

    #[test]
    fn unbounded_range_conflicts_with_everything() {
        let l = locks();
        let _a = l.try_acquire(KeyRange::all()).expect("first");
        assert!(l.try_acquire(r("q", "r")).is_none());
    }

    #[test]
    fn guard_releases_on_drop() {
        let l = locks();
        {
            let _a = l.try_acquire(r("a", "z")).expect("first");
            assert!(l.try_acquire(r("a", "z")).is_none());
        }
        assert!(l.is_empty(), "drop must release");
        l.try_acquire(r("a", "z")).expect("released range reusable");
    }

    /// The parts/tiers path must wait rather than fail. Holding a conflicting
    /// range and then dropping it has to let the blocked acquirer through.
    #[test]
    fn acquire_blocking_waits_then_proceeds() {
        let l = locks();
        let held = l.try_acquire(r("a", "z")).expect("first");
        let entered = Arc::new(AtomicBool::new(false));

        std::thread::scope(|s| {
            let l2 = Arc::clone(&l);
            let e2 = Arc::clone(&entered);
            let h = s.spawn(move || {
                let _g = l2.acquire_blocking(r("m", "q"));
                e2.store(true, Ordering::SeqCst);
            });
            std::thread::sleep(Duration::from_millis(50));
            assert!(
                !entered.load(Ordering::SeqCst),
                "must still be waiting on the conflicting range"
            );
            drop(held);
            h.join().unwrap();
        });

        assert!(entered.load(Ordering::SeqCst));
        assert!(l.is_empty());
    }

    #[test]
    fn union_spans_every_input() {
        let cmp = default_comparator();
        let spans: Vec<(&[u8], &[u8])> = vec![
            (b"d".as_ref(), b"f".as_ref()),
            (b"a".as_ref(), b"c".as_ref()),
            (b"x".as_ref(), b"z".as_ref()),
        ];
        let u = KeyRange::union(spans, &cmp).expect("non-empty");
        assert_eq!(u.min.as_deref(), Some(b"a".as_ref()));
        assert_eq!(u.max.as_deref(), Some(b"z".as_ref()));
    }

    #[test]
    fn union_of_nothing_is_none() {
        let cmp = default_comparator();
        let empty: Vec<(&[u8], &[u8])> = Vec::new();
        assert!(KeyRange::union(empty, &cmp).is_none());
    }
}
