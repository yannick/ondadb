//! Range tombstones (1.2): the in-memory set a memtable keeps beside its point
//! shards, the *fragments* that set is turned into at flush and compaction, and
//! the read-side mask that applies them.
//!
//! A range tombstone deletes the half-open comparator interval `[start, end)`
//! at one sequence number. It is stored **once**, never expanded into a
//! tombstone per key, so a bulk delete costs one record instead of one per
//! covered key.
//!
//! Three shapes appear here, and keeping them apart is what makes the rest of
//! the engine simple:
//!
//! * [`RangeTombstoneSet`] — the *live* form. Spans arrive in commit order,
//!   overlap freely, and are ordered by `start` only. This is what a memtable
//!   holds.
//! * [`Fragment`] — the *durable* form. Fragments are disjoint, sorted, and
//!   each carries the stack of sequence numbers covering its interval
//!   (newest → oldest). This is what an SSTable's aux section stores, and it
//!   is produced from a set by [`RangeTombstoneSet::fragments`].
//! * [`RangeMask`] — the *read* form: several fragment lists plus a cursor
//!   each, answering "what is the newest covering sequence at this key".
//!
//! Fragmentation happens only at flush and compaction, never on the write path:
//! a `delete_range` call appends one span and touches nothing else.

use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use crate::comparator::ComparatorRef;
use crate::encoding::{append_uvarint, uvarint};
use crate::error::{OndaError, Result};

/// Fragment vectors materialized for a read (debug builds only).
///
/// The zero-cost gate is a claim about *allocation*, so it is proved with a
/// counter rather than by inspection: a column family that never issues a range
/// delete must leave this at zero across any number of reads and scans. Mirrors
/// [`crate::memtable::snapshot_calls`], and compiles out of release builds.
#[cfg(debug_assertions)]
static MASK_SOURCES: AtomicUsize = AtomicUsize::new(0);

/// Read the debug-build range-mask materialization counter.
#[doc(hidden)]
#[cfg(debug_assertions)]
pub fn mask_sources() -> usize {
    MASK_SOURCES.load(Ordering::Relaxed)
}

/// Reset the debug-build range-mask materialization counter.
#[doc(hidden)]
#[cfg(debug_assertions)]
pub fn reset_mask_sources() {
    MASK_SOURCES.store(0, Ordering::Relaxed);
}

#[inline]
fn note_mask_source() {
    #[cfg(debug_assertions)]
    MASK_SOURCES.fetch_add(1, Ordering::Relaxed);
}

/// One live range tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Span {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: u64,
}

/// One durable range-tombstone fragment: a half-open interval and every
/// sequence number covering it, newest first.
///
/// Fragments of one table are disjoint and sorted by `start`, which is what
/// lets a scan walk them with a single monotonic cursor and a point read find
/// its candidate with one binary search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    pub start: Vec<u8>,
    /// Exclusive upper bound. `end` is never itself deleted by this fragment.
    pub end: Vec<u8>,
    /// Covering sequences, strictly descending.
    pub seqs: Vec<u64>,
}

impl Fragment {
    /// Newest sequence covering this fragment.
    #[inline]
    pub fn max_seq(&self) -> u64 {
        self.seqs.first().copied().unwrap_or(0)
    }

    /// Oldest sequence covering this fragment.
    #[inline]
    pub fn min_seq(&self) -> u64 {
        self.seqs.last().copied().unwrap_or(0)
    }

    /// Newest covering sequence visible at `read_seq`, or `None`.
    ///
    /// `seqs` is descending, so the first entry at or below `read_seq` is the
    /// answer and the walk stops there.
    #[inline]
    fn visible_seq(&self, read_seq: u64) -> Option<u64> {
        self.seqs.iter().copied().find(|&s| s <= read_seq)
    }
}

/// Iterator over the fragments produced by [`RangeTombstoneSet::fragments`].
///
/// A concrete owning iterator rather than `impl Iterator`: the flush and
/// compaction paths store it, and the fragments must outlive the lock the set
/// was read under.
pub struct FragmentIter {
    inner: std::vec::IntoIter<Fragment>,
}

impl std::iter::Iterator for FragmentIter {
    type Item = Fragment;
    fn next(&mut self) -> Option<Fragment> {
        self.inner.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for FragmentIter {}

impl std::fmt::Debug for FragmentIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentIter")
            .field("remaining", &self.inner.len())
            .finish()
    }
}

/// Spans ordered by `start`, plus the running maximum of `end` that bounds a
/// backward scan.
#[derive(Default)]
struct SetInner {
    spans: Vec<Span>,
    /// `prefix_max_end[i]` is the greatest `end` among `spans[..=i]`.
    ///
    /// This is the bound the design calls "the set's longest span": a backward
    /// scan from the greatest `start <= key` may stop the moment
    /// `prefix_max_end[i] <= key`, because no span at or below `i` can reach
    /// past it. Without it a key below every span's `end` would walk the whole
    /// set.
    prefix_max_end: Vec<Vec<u8>>,
}

/// The set of range tombstones a memtable holds beside its point shards.
///
/// **Never sharded by start key.** A lookup for `k` must find *every* covering
/// span, and a hash on `start` scatters exactly the spans that could cover a
/// given key. The set is small (one entry per `delete_range` call, not per
/// deleted key), so an ordered vector under one lock is both simpler and
/// faster than a concurrent structure here.
pub struct RangeTombstoneSet {
    cmp: ComparatorRef,
    inner: RwLock<SetInner>,
    /// Span count, readable without taking the lock.
    ///
    /// This is the **zero-cost gate**: a column family that never issues a
    /// range delete pays one relaxed load per source per read and never touches
    /// the lock or allocates anything.
    len: AtomicUsize,
}

impl std::fmt::Debug for RangeTombstoneSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeTombstoneSet")
            .field("spans", &self.len.load(Ordering::Relaxed))
            .finish()
    }
}

impl RangeTombstoneSet {
    pub(crate) fn new(cmp: ComparatorRef) -> RangeTombstoneSet {
        RangeTombstoneSet {
            cmp,
            inner: RwLock::new(SetInner::default()),
            len: AtomicUsize::new(0),
        }
    }

    /// Whether the set holds no span — the gate every read path checks first.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len.load(Ordering::Relaxed) == 0
    }

    /// Number of spans held.
    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Insert `[start, end)` at `seq`.
    pub(crate) fn add(&self, start: &[u8], end: &[u8], seq: u64) {
        let mut g = self.inner.write();
        let at = g
            .spans
            .partition_point(|s| self.cmp.compare(&s.start, start).is_le());
        g.spans.insert(
            at,
            Span {
                start: start.to_vec(),
                end: end.to_vec(),
                seq,
            },
        );
        // Only the suffix from `at` can have changed, but the running maximum
        // is a fold, so recomputing it from `at` is the whole rest of the
        // vector anyway. Spans are rare; keeping this obviously correct beats
        // an in-place patch.
        g.prefix_max_end.truncate(at);
        let mut running: Option<Vec<u8>> = g.prefix_max_end.last().cloned();
        for i in at..g.spans.len() {
            let end = &g.spans[i].end;
            running = match running {
                Some(m) if self.cmp.compare(&m, end).is_ge() => Some(m),
                _ => Some(end.clone()),
            };
            g.prefix_max_end
                .push(running.clone().expect("set above on every arm"));
        }
        drop(g);
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Greatest sequence at or below `read_seq` of a span covering `key`, or
    /// `None` when nothing visible covers it.
    pub(crate) fn covering_seq(&self, key: &[u8], read_seq: u64) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        let g = self.inner.read();
        // Spans with `start <= key`; anything above cannot start at or before
        // the key and so cannot cover it.
        let mut i = g
            .spans
            .partition_point(|s| self.cmp.compare(&s.start, key).is_le());
        let mut best: Option<u64> = None;
        while i > 0 {
            i -= 1;
            if self.cmp.compare(&g.prefix_max_end[i], key).is_le() {
                // No span at or below `i` reaches past `key`.
                break;
            }
            let s = &g.spans[i];
            if s.seq <= read_seq && self.cmp.compare(&s.end, key).is_gt() {
                best = Some(best.map_or(s.seq, |b: u64| b.max(s.seq)));
            }
        }
        best
    }

    /// Fragment the set over `[lower, upper)`, `None` meaning unbounded.
    ///
    /// Boundaries are the sorted unique starts and ends clipped into the
    /// interval; each consecutive pair emits the stack of covering sequences,
    /// newest first, and adjacent fragments carrying identical stacks are
    /// merged so a uniform region costs one fragment however many spans built
    /// it.
    pub(crate) fn fragments(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> FragmentIter {
        if self.is_empty() {
            return FragmentIter {
                inner: Vec::new().into_iter(),
            };
        }
        let g = self.inner.read();
        FragmentIter {
            inner: fragment_spans(&self.cmp, &g.spans, lower, upper).into_iter(),
        }
    }

    /// Every span, in `start` order. Used by the WAL-replay and flush paths.
    #[cfg(test)]
    fn spans(&self) -> Vec<Span> {
        self.inner.read().spans.clone()
    }
}

/// Fragment an arbitrary (overlapping, unsorted-stack) span list over
/// `[lower, upper)`.
///
/// Shared by [`RangeTombstoneSet::fragments`] and compaction, which rebuilds a
/// span list out of its inputs' fragments and re-fragments it over the job
/// span.
pub(crate) fn fragment_spans(
    cmp: &ComparatorRef,
    spans: &[Span],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> Vec<Fragment> {
    let clip_low = |k: &[u8]| -> Vec<u8> {
        match lower {
            Some(l) if cmp.compare(k, l).is_lt() => l.to_vec(),
            _ => k.to_vec(),
        }
    };
    let clip_high = |k: &[u8]| -> Vec<u8> {
        match upper {
            Some(u) if cmp.compare(k, u).is_gt() => u.to_vec(),
            _ => k.to_vec(),
        }
    };

    let mut bounds: Vec<Vec<u8>> = Vec::with_capacity(spans.len() * 2);
    for s in spans {
        let a = clip_low(&s.start);
        let b = clip_high(&s.end);
        if cmp.compare(&a, &b).is_ge() {
            continue; // entirely outside the interval
        }
        bounds.push(a);
        bounds.push(b);
    }
    if bounds.is_empty() {
        return Vec::new();
    }
    bounds.sort_by(|a, b| cmp.compare(a, b));
    bounds.dedup_by(|a, b| cmp.compare(a, b).is_eq());

    let mut out: Vec<Fragment> = Vec::new();
    for pair in bounds.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        let mut seqs: Vec<u64> = spans
            .iter()
            .filter(|s| cmp.compare(&s.start, a).is_le() && cmp.compare(&s.end, b).is_ge())
            .map(|s| s.seq)
            .collect();
        if seqs.is_empty() {
            continue; // a gap between two disjoint spans
        }
        seqs.sort_unstable_by(|x, y| y.cmp(x));
        seqs.dedup();
        // Merge with the previous fragment when it abuts and carries the same
        // stack: a region covered by one span must not be split just because a
        // neighbouring span put a boundary inside it.
        if let Some(prev) = out.last_mut() {
            if prev.seqs == seqs && cmp.compare(&prev.end, a).is_eq() {
                prev.end = b.clone();
                continue;
            }
        }
        out.push(Fragment {
            start: a.clone(),
            end: b.clone(),
            seqs,
        });
    }
    out
}

/// Explode fragments back into one span per `(interval, seq)` pair, the input
/// [`fragment_spans`] takes.
pub(crate) fn spans_of_fragments(frags: &[Fragment]) -> Vec<Span> {
    let mut out = Vec::new();
    for f in frags {
        for &seq in &f.seqs {
            out.push(Span {
                start: f.start.clone(),
                end: f.end.clone(),
                seq,
            });
        }
    }
    out
}

// ---- aux-section codec (section tag 1) -------------------------------------

/// Encode a fragment list as the aux block's section-1 payload.
///
/// ```text
/// payload  := count uvarint | fragment x count            (sorted by start)
/// fragment := slen uvarint | start | elen uvarint | end
///           | nseq uvarint | seq uvarint x nseq           (newest -> oldest)
/// ```
///
/// The section is CRC-covered by the enclosing `block.rs` frame, so it needs no
/// checksum of its own (invariant 4).
pub fn encode_fragments(frags: &[Fragment]) -> Vec<u8> {
    let mut b = Vec::new();
    append_uvarint(&mut b, frags.len() as u64);
    for f in frags {
        append_uvarint(&mut b, f.start.len() as u64);
        b.extend_from_slice(&f.start);
        append_uvarint(&mut b, f.end.len() as u64);
        b.extend_from_slice(&f.end);
        append_uvarint(&mut b, f.seqs.len() as u64);
        for &s in &f.seqs {
            append_uvarint(&mut b, s);
        }
    }
    b
}

/// Decode a section-1 payload written by [`encode_fragments`].
///
/// Every failure is `Corruption`: the enclosing block's CRC already verified,
/// so bytes that do not decode were written intact and contradict the format.
pub fn decode_fragments(mut p: &[u8]) -> Result<Vec<Fragment>> {
    let bad = || OndaError::Corruption("sst: malformed range-fragment section".into());
    let take = |p: &mut &[u8]| -> Result<Vec<u8>> {
        let (n, used) = uvarint(p).ok_or_else(bad)?;
        *p = &p[used..];
        let n = n as usize;
        if p.len() < n {
            return Err(bad());
        }
        let (v, rest) = p.split_at(n);
        *p = rest;
        Ok(v.to_vec())
    };
    let (count, used) = uvarint(p).ok_or_else(bad)?;
    p = &p[used..];
    let mut out = Vec::with_capacity((count as usize).min(4096));
    for _ in 0..count {
        let start = take(&mut p)?;
        let end = take(&mut p)?;
        let (nseq, used) = uvarint(p).ok_or_else(bad)?;
        p = &p[used..];
        if nseq == 0 {
            return Err(bad()); // a fragment with no covering sequence is not writable
        }
        let mut seqs = Vec::with_capacity((nseq as usize).min(4096));
        for _ in 0..nseq {
            let (s, used) = uvarint(p).ok_or_else(bad)?;
            p = &p[used..];
            seqs.push(s);
        }
        // The stack is strictly descending by construction; accepting any other
        // order would make `visible_seq`'s early exit silently wrong.
        if seqs.windows(2).any(|w| w[0] <= w[1]) {
            return Err(bad());
        }
        out.push(Fragment { start, end, seqs });
    }
    if !p.is_empty() {
        return Err(bad());
    }
    Ok(out)
}

// ---- read-side mask --------------------------------------------------------

/// Newest sequence at or below `read_seq` covering `key` in a **sorted,
/// disjoint** fragment list, by binary search. The point-read shape.
pub(crate) fn covering_seq_in(
    cmp: &ComparatorRef,
    frags: &[Fragment],
    key: &[u8],
    read_seq: u64,
) -> Option<u64> {
    if frags.is_empty() {
        return None;
    }
    // First fragment with `end > key`; fragments are disjoint, so it is the
    // only one that can contain the key.
    let i = frags.partition_point(|f| cmp.compare(&f.end, key).is_le());
    let f = frags.get(i)?;
    if cmp.compare(&f.start, key).is_gt() {
        return None;
    }
    f.visible_seq(read_seq)
}

/// A monotonic cursor over one source's fragments.
///
/// The scan shape: `covering_seq` walks the cursor to the key rather than
/// binary-searching, so a forward or backward scan costs amortized O(1) per
/// key however many fragments the source holds. Bounds are **owned** copies
/// taken out of the source, so nothing here borrows a pinned block
/// (invariant 8).
struct FragCursor {
    frags: Vec<Fragment>,
    idx: usize,
}

impl FragCursor {
    fn covering_seq(&mut self, cmp: &ComparatorRef, key: &[u8], read_seq: u64) -> Option<u64> {
        let n = self.frags.len();
        // Forward to the first fragment whose `end` is past the key ...
        while self.idx < n && cmp.compare(&self.frags[self.idx].end, key).is_le() {
            self.idx += 1;
        }
        // ... and back, so a reverse scan converges just as cheaply. Starting
        // from any index is correct: the two loops together land on the unique
        // first fragment with `end > key`.
        while self.idx > 0 && cmp.compare(&self.frags[self.idx - 1].end, key).is_gt() {
            self.idx -= 1;
        }
        let f = self.frags.get(self.idx)?;
        if cmp.compare(&f.start, key).is_gt() {
            return None;
        }
        f.visible_seq(read_seq)
    }
}

/// Range-delete coverage across every source of one read.
///
/// Merging is max-over-sources, not a heap: only the greatest covering sequence
/// decides, so there is nothing to order.
#[derive(Default)]
pub(crate) struct RangeMask {
    sources: Vec<FragCursor>,
}

impl std::fmt::Debug for RangeMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeMask")
            .field("sources", &self.sources.len())
            .finish()
    }
}

impl RangeMask {
    /// Add one source's fragments. Empty lists are dropped, so an unused
    /// feature leaves the mask empty and [`is_empty`](Self::is_empty) true.
    pub(crate) fn push(&mut self, frags: Vec<Fragment>) {
        if !frags.is_empty() {
            note_mask_source();
            self.sources.push(FragCursor { frags, idx: 0 });
        }
    }

    /// Whether any source contributes a fragment.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Greatest covering sequence at or below `read_seq` across every source.
    #[inline]
    pub(crate) fn covering_seq(
        &mut self,
        cmp: &ComparatorRef,
        key: &[u8],
        read_seq: u64,
    ) -> Option<u64> {
        if self.sources.is_empty() {
            return None;
        }
        let mut best: Option<u64> = None;
        for src in &mut self.sources {
            if let Some(s) = src.covering_seq(cmp, key, read_seq) {
                best = Some(best.map_or(s, |b: u64| b.max(s)));
            }
        }
        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::default_comparator;

    fn set() -> RangeTombstoneSet {
        RangeTombstoneSet::new(default_comparator())
    }

    fn frags(s: &RangeTombstoneSet) -> Vec<Fragment> {
        s.fragments(None, None).collect()
    }

    fn f(start: &str, end: &str, seqs: &[u64]) -> Fragment {
        Fragment {
            start: start.as_bytes().to_vec(),
            end: end.as_bytes().to_vec(),
            seqs: seqs.to_vec(),
        }
    }

    #[test]
    fn empty_set_reports_is_empty() {
        let s = set();
        assert!(s.is_empty());
        assert_eq!(s.covering_seq(b"anything", u64::MAX), None);
        assert_eq!(frags(&s), Vec::new());
        s.add(b"a", b"b", 1);
        assert!(!s.is_empty());
    }

    #[test]
    fn covering_seq_finds_longest_span() {
        let s = set();
        // A long span inserted FIRST, then many short ones after it. The
        // backward walk must reach past all of them to find it.
        s.add(b"a", b"z", 10);
        for i in 0..8u8 {
            let k = [b'b' + i];
            let e = [b'b' + i, b'~'];
            s.add(&k, &e, 20 + u64::from(i));
        }
        // `m` is covered only by the long span.
        assert_eq!(s.covering_seq(b"m", u64::MAX), Some(10));
        // `c` is covered by both; the newest wins.
        assert_eq!(s.covering_seq(b"c", u64::MAX), Some(21));
    }

    #[test]
    fn covering_seq_respects_read_seq() {
        let s = set();
        s.add(b"a", b"z", 5);
        s.add(b"a", b"z", 50);
        assert_eq!(s.covering_seq(b"m", 100), Some(50));
        assert_eq!(s.covering_seq(b"m", 49), Some(5));
        assert_eq!(s.covering_seq(b"m", 4), None);
    }

    #[test]
    fn end_bound_is_exclusive() {
        let s = set();
        s.add(b"b", b"d", 7);
        assert_eq!(s.covering_seq(b"a", u64::MAX), None);
        assert_eq!(s.covering_seq(b"b", u64::MAX), Some(7));
        assert_eq!(s.covering_seq(b"c", u64::MAX), Some(7));
        assert_eq!(s.covering_seq(b"d", u64::MAX), None, "end is exclusive");
        // And the same through the fragment form.
        let fr = frags(&s);
        assert_eq!(fr, vec![f("b", "d", &[7])]);
        let cmp = default_comparator();
        assert_eq!(covering_seq_in(&cmp, &fr, b"d", u64::MAX), None);
        assert_eq!(covering_seq_in(&cmp, &fr, b"c", u64::MAX), Some(7));
    }

    #[test]
    fn fragments_split_at_unique_boundaries() {
        let s = set();
        s.add(b"a", b"m", 1);
        s.add(b"f", b"z", 2);
        assert_eq!(
            frags(&s),
            vec![f("a", "f", &[1]), f("f", "m", &[2, 1]), f("m", "z", &[2])]
        );
    }

    #[test]
    fn adjacent_fragments_with_equal_stacks_merge() {
        let s = set();
        // Two spans meeting exactly at `m` with the SAME sequence would split
        // at `m` naively; the stacks are equal so the fragments merge.
        s.add(b"a", b"m", 4);
        s.add(b"m", b"z", 4);
        assert_eq!(frags(&s), vec![f("a", "z", &[4])]);

        // A boundary introduced by an unrelated span inside a uniformly
        // covered region also merges away once that span is older *and*
        // identical — but not when the stacks differ.
        let s2 = set();
        s2.add(b"a", b"z", 4);
        s2.add(b"f", b"h", 9);
        assert_eq!(
            frags(&s2),
            vec![f("a", "f", &[4]), f("f", "h", &[9, 4]), f("h", "z", &[4])]
        );
    }

    #[test]
    fn fragments_are_clipped_to_the_requested_interval() {
        let s = set();
        s.add(b"a", b"z", 3);
        assert_eq!(
            s.fragments(Some(b"f"), Some(b"m")).collect::<Vec<_>>(),
            vec![f("f", "m", &[3])]
        );
        // A span entirely outside the interval contributes nothing.
        let s2 = set();
        s2.add(b"a", b"c", 3);
        assert_eq!(s2.fragments(Some(b"f"), Some(b"m")).count(), 0);
    }

    #[test]
    fn fragment_section_round_trips() {
        let want = vec![f("a", "f", &[9, 4, 1]), f("f", "m", &[7])];
        let got = decode_fragments(&encode_fragments(&want)).unwrap();
        assert_eq!(got, want);
        assert_eq!(
            decode_fragments(&encode_fragments(&[])).unwrap(),
            Vec::new()
        );
    }

    #[test]
    fn fragment_section_rejects_malformed_payloads() {
        let good = encode_fragments(&[f("a", "f", &[9, 4])]);
        // Truncated.
        assert!(decode_fragments(&good[..good.len() - 1]).is_err());
        // Trailing bytes past the promised count.
        let mut extra = good.clone();
        extra.push(0);
        assert!(decode_fragments(&extra).is_err());
        // An ascending (therefore un-writable) sequence stack.
        let bad = encode_fragments(&[Fragment {
            start: b"a".to_vec(),
            end: b"f".to_vec(),
            seqs: vec![1, 9],
        }]);
        assert!(decode_fragments(&bad).is_err());
    }

    /// Brute-force oracle: every key of a small space, against every span the
    /// set holds. Both the live set and the fragment form must agree with it,
    /// which is what pins fragmentation as a *representation* change rather
    /// than a semantic one.
    #[test]
    fn random_spans_match_a_brute_force_oracle() {
        let cmp = default_comparator();
        let keys: Vec<Vec<u8>> = (0u8..16).map(|k| vec![b'a' + k]).collect();
        let mut rng = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        for _round in 0..200 {
            let s = set();
            let mut oracle: Vec<Span> = Vec::new();
            let n = 1 + (next() % 6) as usize;
            for i in 0..n {
                let a = (next() % 15) as usize;
                let b = a + 1 + (next() % (16 - a as u64 - 1)) as usize;
                let seq = 1 + i as u64 * 3;
                s.add(&keys[a], &keys[b], seq);
                oracle.push(Span {
                    start: keys[a].clone(),
                    end: keys[b].clone(),
                    seq,
                });
            }
            let fr = frags(&s);
            // Fragments must be sorted and disjoint.
            for w in fr.windows(2) {
                assert!(
                    cmp.compare(&w[0].end, &w[1].start).is_le(),
                    "fragments overlap: {w:?}"
                );
            }
            for read_seq in [0u64, 2, 5, 8, u64::MAX] {
                for key in &keys {
                    let want = oracle
                        .iter()
                        .filter(|sp| {
                            sp.seq <= read_seq
                                && cmp.compare(&sp.start, key).is_le()
                                && cmp.compare(&sp.end, key).is_gt()
                        })
                        .map(|sp| sp.seq)
                        .max();
                    assert_eq!(
                        s.covering_seq(key, read_seq),
                        want,
                        "set disagrees at {key:?}@{read_seq} for {:?}",
                        s.spans()
                    );
                    assert_eq!(
                        covering_seq_in(&cmp, &fr, key, read_seq),
                        want,
                        "fragments disagree at {key:?}@{read_seq} for {fr:?}"
                    );
                    // And the scan-shaped cursor, walked forward then backward.
                    let mut mask = RangeMask::default();
                    mask.push(fr.clone());
                    assert_eq!(mask.covering_seq(&cmp, key, read_seq), want);
                }
                // One cursor walked monotonically over the whole space, in both
                // directions, must match the per-key binary search.
                let mut mask = RangeMask::default();
                mask.push(fr.clone());
                for key in keys.iter().chain(keys.iter().rev()) {
                    assert_eq!(
                        mask.covering_seq(&cmp, key, read_seq),
                        covering_seq_in(&cmp, &fr, key, read_seq),
                        "cursor and binary search disagree at {key:?}"
                    );
                }
            }
        }
    }
}
