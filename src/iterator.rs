//! Read-consistent, bidirectional iterator over a column family at a fixed
//! snapshot sequence.  It merges the active memtable, immutable memtables and all
//! SSTable levels, collapses MVCC versions, hides tombstones and expired entries,
//! and presents one value per visible user key.

use std::ops::Bound;

use crate::comparator::ComparatorRef;
use crate::error::Result;
use crate::memtable::MemIter;
use crate::sst::{Block, SstIterator};

/// A merge-iterator child: a memtable or SSTable iterator.  Yields entries in
/// internal order (user key ascending, sequence descending), every version.
/// An enum (not a trait object) so the merge hot loop dispatches with a branch
/// and can inline the per-entry accessors.
// Boxing the larger variant would add indirection to this scan hot path; the
// size difference under `unsafe-fastpath` is deliberate and benchmarked.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ChildIter {
    Mem(MemIter),
    Sst(SstIterator),
}

impl ChildIter {
    #[inline]
    fn valid(&self) -> bool {
        match self {
            ChildIter::Mem(m) => m.valid(),
            ChildIter::Sst(s) => s.valid(),
        }
    }
    #[inline]
    fn user_key(&self) -> &[u8] {
        match self {
            ChildIter::Mem(m) => m.user_key(),
            ChildIter::Sst(s) => s.user_key(),
        }
    }
    /// Zero-padded 8-byte prefix of the current user key (cached by the SST
    /// iterator at decode time). Comparing prefixes is consistent with
    /// byte-wise ordering whenever they differ.
    #[inline]
    fn key_prefix8(&self) -> u64 {
        match self {
            ChildIter::Mem(m) => m.key_prefix(),
            ChildIter::Sst(s) => s.key_prefix(),
        }
    }
    #[inline]
    fn seq(&self) -> u64 {
        match self {
            ChildIter::Mem(m) => m.seq(),
            ChildIter::Sst(s) => s.seq(),
        }
    }
    #[inline]
    fn ttl(&self) -> i64 {
        match self {
            ChildIter::Mem(m) => m.ttl(),
            ChildIter::Sst(s) => s.ttl(),
        }
    }
    #[inline]
    fn tombstone(&self) -> bool {
        match self {
            ChildIter::Mem(m) => m.is_tombstone(),
            ChildIter::Sst(s) => s.is_tombstone(),
        }
    }
    /// Append the current value to `out` (no intermediate allocation).
    fn value_into(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            ChildIter::Mem(m) => {
                out.extend_from_slice(m.value_ref());
                Ok(())
            }
            ChildIter::Sst(s) => s.value_into(out),
        }
    }
    /// Borrowed handle to the current inline SSTable value: `(block, start,
    /// len)`. `None` for memtable entries and vlog-separated values.
    #[inline]
    fn value_block_ref(&self) -> Option<(&Block, usize, usize)> {
        match self {
            ChildIter::Mem(_) => None,
            ChildIter::Sst(s) => s.value_block_ref(),
        }
    }
    /// Borrowed handle to the current user key within an SSTable data block.
    /// `None` for memtable entries.
    #[inline]
    fn key_block_ref(&self) -> Option<(&Block, usize, usize)> {
        match self {
            ChildIter::Mem(_) => None,
            ChildIter::Sst(s) => s.key_block_ref(),
        }
    }
    #[inline]
    fn next(&mut self) {
        match self {
            ChildIter::Mem(m) => m.next(),
            ChildIter::Sst(s) => s.next(),
        }
    }
    #[inline]
    fn prev(&mut self) {
        match self {
            ChildIter::Mem(m) => m.prev(),
            ChildIter::Sst(s) => s.prev(),
        }
    }
    fn seek_to_first(&mut self) {
        match self {
            ChildIter::Mem(m) => m.seek_to_first(),
            ChildIter::Sst(s) => s.seek_to_first(),
        }
    }
    fn seek_to_last(&mut self) {
        match self {
            ChildIter::Mem(m) => m.seek_to_last(),
            ChildIter::Sst(s) => s.seek_to_last(),
        }
    }
    fn seek_ge(&mut self, k: &[u8], s: u64) {
        match self {
            ChildIter::Mem(m) => m.seek_ge(k, s),
            ChildIter::Sst(it) => it.seek(k, s),
        }
    }
    fn seek_le(&mut self, k: &[u8], s: u64) {
        match self {
            ChildIter::Mem(m) => m.seek_le(k, s),
            ChildIter::Sst(it) => it.seek_for_prev(k, s),
        }
    }
}

/// Heap-based k-way merge over several [`ChildIter`]s.
struct MergingIter {
    children: Vec<ChildIter>,
    heap: Vec<usize>,
    dir: i32, // +1 forward, -1 backward
    cmp: ComparatorRef,
    /// The comparator is plain byte-wise ordering (the default); hot-loop
    /// comparisons then use an inlined slice compare instead of a virtual call.
    bytewise: bool,
}

impl MergingIter {
    fn new(cmp: ComparatorRef, children: Vec<ChildIter>) -> MergingIter {
        let cap = children.len();
        let bytewise = cmp.is_bytewise();
        MergingIter {
            children,
            heap: Vec::with_capacity(cap),
            dir: 1,
            cmp,
            bytewise,
        }
    }

    #[inline]
    fn key_cmp(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        if self.bytewise {
            a.cmp(b)
        } else {
            self.cmp.compare(a, b)
        }
    }

    fn before(&self, i: usize, j: usize) -> bool {
        let ci = &self.children[i];
        let cj = &self.children[j];
        // Byte-wise: decide on the cached 8-byte prefixes when they differ —
        // no key-slice construction, no memcmp.
        let mut c = if self.bytewise {
            match ci.key_prefix8().cmp(&cj.key_prefix8()) {
                std::cmp::Ordering::Equal => ci.user_key().cmp(cj.user_key()),
                ord => ord,
            }
        } else {
            self.cmp.compare(ci.user_key(), cj.user_key())
        };
        if c == std::cmp::Ordering::Equal {
            // Higher sequence sorts first in internal (forward) order.
            c = cj.seq().cmp(&ci.seq());
        }
        if self.dir > 0 {
            c.is_lt()
        } else {
            c.is_gt()
        }
    }

    fn heap_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut best = i;
            if l < n && self.before(self.heap[l], self.heap[best]) {
                best = l;
            }
            if r < n && self.before(self.heap[r], self.heap[best]) {
                best = r;
            }
            if best == i {
                break;
            }
            self.heap.swap(i, best);
            i = best;
        }
    }

    fn rebuild(&mut self) {
        self.heap.clear();
        for (i, c) in self.children.iter().enumerate() {
            if c.valid() {
                self.heap.push(i);
            }
        }
        if self.heap.len() > 1 {
            for i in (0..self.heap.len() / 2).rev() {
                self.heap_down(i);
            }
        }
    }

    fn seek_to_first(&mut self) {
        self.dir = 1;
        for c in &mut self.children {
            c.seek_to_first();
        }
        self.rebuild();
    }
    fn seek_to_last(&mut self) {
        self.dir = -1;
        for c in &mut self.children {
            c.seek_to_last();
        }
        self.rebuild();
    }
    fn seek_ge(&mut self, k: &[u8], s: u64) {
        self.dir = 1;
        for c in &mut self.children {
            c.seek_ge(k, s);
        }
        self.rebuild();
    }
    fn seek_le(&mut self, k: &[u8], s: u64) {
        self.dir = -1;
        for c in &mut self.children {
            c.seek_le(k, s);
        }
        self.rebuild();
    }

    /// Position every child at its last entry with user key strictly below
    /// `k` (used when iteration flips forward->backward: the children sit at
    /// or past the current group and must all be walked back behind it).
    fn seek_lt(&mut self, k: &[u8]) {
        self.dir = -1;
        for c in &mut self.children {
            // (k, MAX) sorts before every live version of k (seq is ordered
            // descending), so seek_le lands strictly below the group already;
            // the loop only guards against a lenient child seek.
            c.seek_le(k, u64::MAX);
            while c.valid() && {
                let key = c.user_key();
                if self.bytewise {
                    key == k
                } else {
                    self.cmp.compare(key, k).is_eq()
                }
            } {
                c.prev();
            }
        }
        self.rebuild();
    }

    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }
    #[inline]
    fn top(&self) -> &ChildIter {
        &self.children[self.heap[0]]
    }
    #[inline]
    fn top_idx(&self) -> usize {
        self.heap[0]
    }

    fn advance(&mut self, forward: bool) {
        if self.heap.is_empty() {
            return;
        }
        let idx = self.heap[0];
        if forward {
            self.children[idx].next();
        } else {
            self.children[idx].prev();
        }
        if !self.children[idx].valid() {
            let last = self.heap.len() - 1;
            self.heap[0] = self.heap[last];
            self.heap.pop();
        }
        if !self.heap.is_empty() {
            self.heap_down(0);
        }
    }
}

/// Where the current entry's value lives.
enum CurVal {
    /// No value (tombstone group, or not positioned).
    Empty,
    /// Bytes were copied into `Iterator::val` (memtable or vlog value).
    Buffered,
    /// Borrowed slice into `Iterator::pinned_val[child]` — an SSTable data block
    /// we hold alive. The pin is refreshed at block-transition granularity (an
    /// `Arc` clone once per ~block, not per entry), so the shared refcount is
    /// not hammered by concurrent scans.
    Pinned {
        child: usize,
        start: usize,
        len: usize,
    },
}

/// Where the current entry's user key lives (same scheme as [`CurVal`], with
/// its own per-child pins: the winning value may sit in a later block of the
/// same child, so key and value pins must not evict each other).
enum CurKey {
    /// Copied into `Iterator::key` (memtable entries, or not positioned).
    Buffered,
    /// Borrowed slice into `Iterator::pinned_key[child]`.
    Pinned {
        child: usize,
        start: usize,
        len: usize,
    },
}

/// Public bidirectional iterator over a column family snapshot.
pub struct Iterator {
    m: MergingIter,
    read_seq: u64,
    now: i64,
    key: Vec<u8>,
    val: Vec<u8>,
    cur_key: CurKey,
    /// 8-byte prefix of the current group key (set with `cur_key`).
    group_pfx: u64,
    cur_val: CurVal,
    /// Per-child pinned data block backing a `CurVal::Pinned` value.
    pinned_val: Vec<Option<Block>>,
    /// Per-child pinned data block backing a `CurKey::Pinned` key.
    pinned_key: Vec<Option<Block>>,
    valid: bool,
    err: Option<crate::error::OndaError>,
    /// Declared key bounds (see [`Txn::new_iterator_bounded`]
    /// (crate::txn::Txn::new_iterator_bounded)): forward iteration terminates
    /// at the first group past `upper`, backward at the first group below
    /// `lower`. SSTables entirely outside the bounds were pruned at
    /// construction, so seeking outside them yields unspecified (but
    /// memory-safe) completeness.
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl std::fmt::Debug for Iterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iterator")
            .field("valid", &self.valid)
            .field("read_seq", &self.read_seq)
            .finish()
    }
}

fn expired(ttl: i64, now: i64) -> bool {
    ttl != 0 && ttl <= now
}

impl Iterator {
    pub(crate) fn new(
        cmp: ComparatorRef,
        children: Vec<ChildIter>,
        read_seq: u64,
        now: i64,
        bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Iterator {
        let n = children.len();
        Iterator {
            m: MergingIter::new(cmp, children),
            read_seq,
            now,
            key: Vec::new(),
            val: Vec::new(),
            cur_key: CurKey::Buffered,
            group_pfx: 0,
            cur_val: CurVal::Empty,
            pinned_val: (0..n).map(|_| None).collect(),
            pinned_key: (0..n).map(|_| None).collect(),
            valid: false,
            err: None,
            lower: bounds.0,
            upper: bounds.1,
        }
    }

    /// Is the current group key past the declared upper bound (forward
    /// direction)?
    #[inline]
    fn past_upper(&self) -> bool {
        match &self.upper {
            Bound::Unbounded => false,
            Bound::Included(u) => self.m.key_cmp(self.key(), u).is_gt(),
            Bound::Excluded(u) => self.m.key_cmp(self.key(), u).is_ge(),
        }
    }

    /// Is the current group key below the declared lower bound (backward
    /// direction)?
    #[inline]
    fn below_lower(&self) -> bool {
        match &self.lower {
            Bound::Unbounded => false,
            Bound::Included(l) => self.m.key_cmp(self.key(), l).is_lt(),
            Bound::Excluded(l) => self.m.key_cmp(self.key(), l).is_le(),
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }
    pub fn err(&self) -> Option<&crate::error::OndaError> {
        self.err.as_ref()
    }
    #[inline]
    pub fn key(&self) -> &[u8] {
        match &self.cur_key {
            CurKey::Pinned { child, start, len } => match self.pinned_key[*child].as_ref() {
                Some(b) => &b.bytes()[*start..*start + *len],
                None => &[],
            },
            CurKey::Buffered => &self.key,
        }
    }
    pub fn value(&self) -> &[u8] {
        match &self.cur_val {
            CurVal::Pinned { child, start, len } => match self.pinned_val[*child].as_ref() {
                Some(b) => &b.bytes()[*start..*start + *len],
                None => &[],
            },
            CurVal::Buffered => &self.val,
            CurVal::Empty => &[],
        }
    }

    /// Capture the merge top's user key as the current group key: borrowed from
    /// a pinned block for SSTable entries, copied into the reused buffer for
    /// memtable entries.
    #[inline]
    fn capture_group_key(&mut self) {
        let idx = self.m.top_idx();
        let child = &self.m.children[idx];
        self.group_pfx = child.key_prefix8();
        if let Some((blk, start, len)) = child.key_block_ref() {
            let pin = &mut self.pinned_key[idx];
            if !pin.as_ref().is_some_and(|p| p.same_backing(blk)) {
                *pin = Some(blk.clone());
            }
            self.cur_key = CurKey::Pinned {
                child: idx,
                start,
                len,
            };
        } else {
            self.key.clear();
            self.key.extend_from_slice(child.user_key());
            self.cur_key = CurKey::Buffered;
        }
    }

    /// Whether the merge top's user key equals the current group key. Under
    /// byte-wise ordering, differing 8-byte prefixes prove inequality without
    /// touching either key slice.
    #[inline]
    fn top_in_group(&self) -> bool {
        if !self.m.valid() {
            return false;
        }
        if self.m.bytewise {
            if self.m.top().key_prefix8() != self.group_pfx {
                return false;
            }
            return self.m.top().user_key() == self.key();
        }
        self.m.key_cmp(self.m.top().user_key(), self.key()).is_eq()
    }

    /// Record the merge top's value for later retrieval by [`value`](Self::value).
    /// Inline SSTable values are borrowed from a pinned block (the pin is only
    /// refreshed when the child moved to a different block); memtable and vlog
    /// values are copied into the reused buffer.
    #[inline]
    fn capture_value(&mut self) -> Result<()> {
        let idx = self.m.top_idx();
        let child = &self.m.children[idx];
        if let Some((blk, start, len)) = child.value_block_ref() {
            let pin = &mut self.pinned_val[idx];
            if !pin.as_ref().is_some_and(|p| p.same_backing(blk)) {
                *pin = Some(blk.clone());
            }
            self.cur_val = CurVal::Pinned {
                child: idx,
                start,
                len,
            };
        } else {
            self.val.clear();
            child.value_into(&mut self.val)?;
            self.cur_val = CurVal::Buffered;
        }
        Ok(())
    }

    pub fn seek_to_first(&mut self) {
        // Start at the lower bound, not the raw heap minimum: SSTables fully
        // outside the bounds were pruned at construction, but memtables and
        // straddling tables still hold keys below `lower` that must never
        // surface from a bounded iterator.
        match &self.lower {
            Bound::Unbounded => self.m.seek_to_first(),
            // (l, MAX) sorts before every version of l → first group >= l.
            Bound::Included(l) => self.m.seek_ge(l, u64::MAX),
            // (l, 0) sorts after every live version of l → first group > l.
            Bound::Excluded(l) => self.m.seek_ge(l, 0),
        }
        self.advance_forward();
    }
    pub fn seek_to_last(&mut self) {
        // Mirror of `seek_to_first`: start at the upper bound.
        match &self.upper {
            Bound::Unbounded => self.m.seek_to_last(),
            // (u, 0) sorts after every live version of u → last group <= u.
            Bound::Included(u) => self.m.seek_le(u, 0),
            Bound::Excluded(u) => self.m.seek_lt(u),
        }
        self.advance_backward();
    }
    pub fn seek(&mut self, user_key: &[u8]) {
        self.m.seek_ge(user_key, u64::MAX);
        self.advance_forward();
    }
    pub fn seek_for_prev(&mut self, user_key: &[u8]) {
        self.m.seek_le(user_key, 0);
        self.advance_backward();
    }
    pub fn next(&mut self) {
        // Direction switch backward->forward: the children sit below the
        // current group (and forward-exhausted ones were dropped from the
        // heap), so reposition everyone just past it. (k, seq 0) sorts after
        // every live version of k — sequences start at 1.
        if self.valid && self.m.dir < 0 {
            let k = self.key().to_vec();
            self.m.seek_ge(&k, 0);
        }
        self.advance_forward();
    }
    pub fn prev(&mut self) {
        // Direction switch forward->backward: mirror of `next`.
        if self.valid && self.m.dir > 0 {
            let k = self.key().to_vec();
            self.m.seek_lt(&k);
        }
        self.advance_backward();
    }

    fn advance_forward(&mut self) {
        while self.m.valid() {
            // Capture the current user key as the group key (borrowed or copied).
            self.capture_group_key();
            let mut visible = false;
            let mut deleted = false;
            let mut ttl = 0i64;
            self.cur_val = CurVal::Empty;
            while self.top_in_group() {
                if !visible && self.m.top().seq() <= self.read_seq {
                    visible = true;
                    if self.m.top().tombstone() {
                        deleted = true;
                    } else {
                        if let Err(e) = self.capture_value() {
                            self.err = Some(e);
                            self.valid = false;
                            return;
                        }
                        ttl = self.m.top().ttl();
                    }
                }
                self.m.advance(true);
            }
            if visible && !deleted && !expired(ttl, self.now) {
                self.valid = true;
                // Terminate at the first group past the declared upper bound.
                if self.past_upper() {
                    self.valid = false;
                }
                return;
            }
        }
        self.valid = false;
    }

    fn advance_backward(&mut self) {
        while self.m.valid() {
            self.capture_group_key();
            let mut have = false;
            let mut best_seq = 0u64;
            let mut best_tomb = false;
            let mut best_ttl = 0i64;
            self.cur_val = CurVal::Empty;
            while self.top_in_group() {
                let s = self.m.top().seq();
                if s <= self.read_seq && (!have || s > best_seq) {
                    have = true;
                    best_seq = s;
                    if self.m.top().tombstone() {
                        best_tomb = true;
                        self.cur_val = CurVal::Empty;
                    } else {
                        best_tomb = false;
                        if let Err(e) = self.capture_value() {
                            self.err = Some(e);
                            self.valid = false;
                            return;
                        }
                        best_ttl = self.m.top().ttl();
                    }
                }
                self.m.advance(false);
            }
            if have && !best_tomb && !expired(best_ttl, self.now) {
                self.valid = true;
                // Terminate at the first group below the declared lower bound.
                if self.below_lower() {
                    self.valid = false;
                }
                return;
            }
        }
        self.valid = false;
    }
}
