//! Sharded, MVCC, comparator-ordered in-memory write buffer.
//!
//! The memtable is split into [`NUM_SHARDS`] independent lock-free skip lists
//! (`crossbeam_skiplist::SkipMap`).  All versions of a given user key hash to the
//! same shard, so point reads touch a single shard and writers to different
//! shards never contend.
//!
//! Keys are stored as *internal keys* (`user_key || !seq` big-endian) ordered by
//! `(user_key ascending, seq descending)` using the column family's comparator,
//! so a point read for `read_seq` is a single `lower_bound` to the newest visible
//! version.
//!
//! Ordered *read* iteration ([`Memtable::iter`]) is **lazy**: [`LazyMemIter`]
//! runs a k-way merge directly over the (already sorted) shard skip lists, one
//! persistent crossbeam cursor per shard, so constructing an iterator is
//! `O(shards)` — independent of the number of live entries.  This matters because
//! a scan reading a single record used to pay for cloning and sorting the whole
//! memtable up front.  The [`snapshot`](Memtable::snapshot) materialization path
//! is retained for flush.  Under `arena-memtable` the read iterator stays on the
//! snapshot path (the arena shard cursor is forward-only); see [`Memtable::iter`].

use std::cmp::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtOrd};
use std::sync::Arc;

#[cfg(not(feature = "arena-memtable"))]
use crossbeam_skiplist::map::Entry as SkipEntry;
#[cfg(not(feature = "arena-memtable"))]
use crossbeam_skiplist::SkipMap;
#[cfg(not(feature = "arena-memtable"))]
use std::ops::Bound;

use self_cell::self_cell;

use crate::comparator::ComparatorRef;
use crate::format::flags;
#[cfg(not(feature = "arena-memtable"))]
use crate::format::{self, make_internal_key};

#[cfg(feature = "arena-memtable")]
use crate::memtable_arena::ArenaShard;

/// Number of independent shards. 256 keeps per-shard skip lists shallow and
/// makes writer collisions rare even with many committing threads.
pub const NUM_SHARDS: usize = 256;

/// The selected per-shard storage: a lock-free `crossbeam-skiplist` by default,
/// or an arena-backed skip list under `arena-memtable`.
#[cfg(not(feature = "arena-memtable"))]
type ShardImpl = SkipMap<IKey, Val>;
#[cfg(feature = "arena-memtable")]
type ShardImpl = ArenaShard;

/// Internal key: `user_key || !seq` (big-endian), ordered via the comparator.
/// `cmp` is `None` for plain byte-wise ordering (the default), so the hot
/// skip-list comparisons use an inlined slice compare with no per-key `Arc`
/// clone and no virtual call.
#[cfg(not(feature = "arena-memtable"))]
#[derive(Clone)]
struct IKey {
    ik: Box<[u8]>,
    cmp: Option<ComparatorRef>,
}

#[cfg(not(feature = "arena-memtable"))]
mod skipmap_key {
    use super::*;

    impl IKey {
        pub(super) fn new(user_key: &[u8], seq: u64, cmp: &ComparatorRef) -> IKey {
            IKey {
                ik: make_internal_key(user_key, seq).into_boxed_slice(),
                cmp: if cmp.is_bytewise() {
                    None
                } else {
                    Some(cmp.clone())
                },
            }
        }
    }

    impl Ord for IKey {
        fn cmp(&self, other: &Self) -> Ordering {
            let (ua, sa) = format::split_internal_key(&self.ik);
            let (ub, sb) = format::split_internal_key(&other.ik);
            let key_ord = match &self.cmp {
                None => ua.cmp(ub),
                Some(c) => c.compare(ua, ub),
            };
            key_ord.then_with(|| sb.cmp(&sa)) // seq descending
        }
    }
    impl PartialOrd for IKey {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl PartialEq for IKey {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other) == Ordering::Equal
        }
    }
    impl Eq for IKey {}
}

/// Stored value payload.
#[cfg(not(feature = "arena-memtable"))]
#[derive(Clone)]
struct Val {
    value: Vec<u8>,
    ttl: i64,
    flags: u8,
}

/// A decoded memtable record (snapshot copy).
#[derive(Debug, Clone)]
pub struct Entry {
    pub user_key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    pub ttl: i64,
    pub tombstone: bool,
    pub single_delete: bool,
}

/// Result of a memtable point read.
#[derive(Debug, Clone, Default)]
pub struct Lookup {
    pub value: Vec<u8>,
    pub seq: u64,
    pub found: bool,
    pub deleted: bool,
}

/// A sharded MVCC memtable.
pub struct Memtable {
    shards: Vec<ShardImpl>,
    cmp: ComparatorRef,
    approx_size: AtomicI64,
    num_entries: AtomicI64,
    max_seq: AtomicU64,
}

impl std::fmt::Debug for Memtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memtable")
            .field("entries", &self.num_entries.load(AtOrd::Relaxed))
            .field("approx_size", &self.approx_size.load(AtOrd::Relaxed))
            .finish()
    }
}

#[inline]
fn shard_index(user_key: &[u8]) -> usize {
    // xxh3 processes the whole key in wide lanes — measurably cheaper than a
    // byte-at-a-time FNV on every put/get. Routing only needs in-process
    // consistency, so the hash choice is not a format concern.
    let h = xxhash_rust::xxh3::xxh3_64(user_key);
    ((h ^ (h >> 32)) & (NUM_SHARDS as u64 - 1)) as usize
}

pub(crate) fn flag_bits(tombstone: bool, single_delete: bool, ttl: i64) -> u8 {
    let mut f = 0;
    if tombstone {
        f |= flags::TOMBSTONE;
    }
    if single_delete {
        f |= flags::SINGLE_DELETE;
    }
    if ttl != 0 {
        f |= flags::HAS_TTL;
    }
    f
}

impl Memtable {
    /// Create an empty memtable ordering user keys with `cmp`.
    pub fn new(cmp: ComparatorRef) -> Arc<Memtable> {
        #[cfg(not(feature = "arena-memtable"))]
        let shards = (0..NUM_SHARDS).map(|_| SkipMap::new()).collect();
        #[cfg(feature = "arena-memtable")]
        let shards = (0..NUM_SHARDS)
            .map(|_| ArenaShard::new(cmp.clone()))
            .collect();
        Arc::new(Memtable {
            shards,
            cmp,
            approx_size: AtomicI64::new(0),
            num_entries: AtomicI64::new(0),
            max_seq: AtomicU64::new(0),
        })
    }

    /// The comparator used by this memtable.
    pub fn comparator(&self) -> &ComparatorRef {
        &self.cmp
    }

    /// Insert a new version of `user_key`.  `ttl` is an absolute Unix-nanosecond
    /// expiry, or `0` for none.  `seq` must be unique per key and monotonic.
    pub fn put(
        &self,
        user_key: &[u8],
        value: Vec<u8>,
        seq: u64,
        ttl: i64,
        tombstone: bool,
        single_delete: bool,
    ) {
        let fl = flag_bits(tombstone, single_delete, ttl);
        let shard = &self.shards[shard_index(user_key)];
        let added = user_key.len() + value.len() + 64;
        #[cfg(not(feature = "arena-memtable"))]
        shard.insert(
            IKey::new(user_key, seq, &self.cmp),
            Val {
                value,
                ttl,
                flags: fl,
            },
        );
        #[cfg(feature = "arena-memtable")]
        shard.put(user_key, &value, seq, ttl, fl);
        self.after_insert(added, seq);
    }

    /// Like [`put`](Self::put) but with a borrowed value (the commit path writes
    /// straight out of the transaction buffer; the memtable copies internally).
    pub fn put_ref(
        &self,
        user_key: &[u8],
        value: &[u8],
        seq: u64,
        ttl: i64,
        tombstone: bool,
        single_delete: bool,
    ) {
        let fl = flag_bits(tombstone, single_delete, ttl);
        let shard = &self.shards[shard_index(user_key)];
        let added = user_key.len() + value.len() + 64;
        #[cfg(not(feature = "arena-memtable"))]
        shard.insert(
            IKey::new(user_key, seq, &self.cmp),
            Val {
                value: value.to_vec(),
                ttl,
                flags: fl,
            },
        );
        #[cfg(feature = "arena-memtable")]
        shard.put(user_key, value, seq, ttl, fl);
        self.after_insert(added, seq);
    }

    #[inline]
    fn after_insert(&self, added: usize, seq: u64) {
        self.approx_size.fetch_add(added as i64, AtOrd::Relaxed);
        self.num_entries.fetch_add(1, AtOrd::Relaxed);
        let mut cur = self.max_seq.load(AtOrd::Relaxed);
        while seq > cur {
            match self
                .max_seq
                .compare_exchange_weak(cur, seq, AtOrd::Relaxed, AtOrd::Relaxed)
            {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }

    /// Insert a whole committed batch. Records are grouped by shard so each
    /// shard's writer lock is taken once per batch (not per record), nodes are
    /// prepared outside the locks, and the shared size/count/max-seq counters —
    /// contended cache lines under multi-threaded commits — are updated once.
    pub fn put_batch(&self, recs: &[crate::wal::RecordRef<'_>]) {
        let n = recs.len();
        if n == 0 {
            return;
        }
        if n == 1 {
            let r = &recs[0];
            self.put_ref(r.key, r.value, r.seq, r.ttl, r.tombstone, r.single_delete);
            return;
        }

        // Counting-sort record indices into per-shard runs.
        let mut shard_of: Vec<u16> = Vec::with_capacity(n);
        let mut counts = [0u32; NUM_SHARDS];
        for r in recs {
            let s = shard_index(r.key);
            shard_of.push(s as u16);
            counts[s] += 1;
        }
        let mut cursor = [0u32; NUM_SHARDS];
        let mut acc = 0u32;
        for (c, cnt) in cursor.iter_mut().zip(counts.iter()) {
            *c = acc;
            acc += cnt;
        }
        let starts = cursor;
        let mut order: Vec<u32> = vec![0; n];
        for (i, &s) in shard_of.iter().enumerate() {
            let s = s as usize;
            order[cursor[s] as usize] = i as u32;
            cursor[s] += 1;
        }

        let mut added = 0usize;
        let mut batch_max = 0u64;
        for r in recs {
            added += r.key.len() + r.value.len() + 64;
            if r.seq > batch_max {
                batch_max = r.seq;
            }
        }

        for s in 0..NUM_SHARDS {
            let (lo, hi) = (starts[s] as usize, cursor[s] as usize);
            if lo == hi {
                continue;
            }
            let group = &order[lo..hi];
            #[cfg(feature = "arena-memtable")]
            self.shards[s].put_group(recs, group);
            #[cfg(not(feature = "arena-memtable"))]
            for &i in group {
                let r = &recs[i as usize];
                let fl = flag_bits(r.tombstone, r.single_delete, r.ttl);
                self.shards[s].insert(
                    IKey::new(r.key, r.seq, &self.cmp),
                    Val {
                        value: r.value.to_vec(),
                        ttl: r.ttl,
                        flags: fl,
                    },
                );
            }
        }

        self.approx_size.fetch_add(added as i64, AtOrd::Relaxed);
        self.num_entries.fetch_add(n as i64, AtOrd::Relaxed);
        let mut cur = self.max_seq.load(AtOrd::Relaxed);
        while batch_max > cur {
            match self
                .max_seq
                .compare_exchange_weak(cur, batch_max, AtOrd::Relaxed, AtOrd::Relaxed)
            {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }

    /// Resolve `user_key` as of `read_seq`.  `now_nanos` is the current time for
    /// TTL expiry evaluation.
    #[cfg_attr(feature = "arena-memtable", allow(clippy::needless_return))]
    pub fn get(&self, user_key: &[u8], read_seq: u64, now_nanos: i64) -> Lookup {
        // Cold-read fast path: a fresh (or drained) memtable holds nothing, so
        // skip the shard hash entirely — it hashes the whole key.
        if self.num_entries.load(AtOrd::Relaxed) == 0 {
            return Lookup::default();
        }
        let shard = &self.shards[shard_index(user_key)];
        #[cfg(feature = "arena-memtable")]
        {
            return shard.get(user_key, read_seq, now_nanos);
        }
        #[cfg(not(feature = "arena-memtable"))]
        {
            let probe = IKey::new(user_key, read_seq, &self.cmp);
            let entry = match shard.lower_bound(std::ops::Bound::Included(&probe)) {
                Some(e) => e,
                None => return Lookup::default(),
            };
            let (uk, seq) = format::split_internal_key(&entry.key().ik);
            if self.cmp.compare(uk, user_key) != Ordering::Equal {
                return Lookup::default();
            }
            let v = entry.value();
            if v.flags & flags::TOMBSTONE != 0 {
                return Lookup {
                    seq,
                    found: true,
                    deleted: true,
                    ..Default::default()
                };
            }
            if v.ttl != 0 && v.ttl <= now_nanos {
                return Lookup {
                    seq,
                    found: true,
                    deleted: true,
                    ..Default::default()
                };
            }
            Lookup {
                value: v.value.clone(),
                seq,
                found: true,
                deleted: false,
            }
        }
    }

    /// Approximate in-memory footprint in bytes.
    pub fn approx_size(&self) -> i64 {
        self.approx_size.load(AtOrd::Relaxed)
    }

    /// Number of versioned entries stored.
    pub fn num_entries(&self) -> i64 {
        self.num_entries.load(AtOrd::Relaxed)
    }

    /// Largest sequence number inserted.
    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(AtOrd::Relaxed)
    }

    /// Whether the memtable holds no entries.
    pub fn is_empty(&self) -> bool {
        self.num_entries.load(AtOrd::Relaxed) == 0
    }

    /// Materialize all live entries in internal order
    /// (user key ascending, sequence descending).
    pub fn snapshot(&self) -> Vec<Entry> {
        let mut out = Vec::with_capacity(self.num_entries.load(AtOrd::Relaxed).max(0) as usize);
        #[cfg(not(feature = "arena-memtable"))]
        for shard in &self.shards {
            for e in shard.iter() {
                let (uk, seq) = format::split_internal_key(&e.key().ik);
                let v = e.value();
                out.push(Entry {
                    user_key: uk.to_vec(),
                    value: v.value.clone(),
                    seq,
                    ttl: v.ttl,
                    tombstone: v.flags & flags::TOMBSTONE != 0,
                    single_delete: v.flags & flags::SINGLE_DELETE != 0,
                });
            }
        }
        #[cfg(feature = "arena-memtable")]
        for shard in &self.shards {
            shard.collect(&mut out);
        }
        // The concatenated shard runs are each pre-sorted; the stable sort
        // merges them. Byte-wise ordering uses the inlined slice comparison
        // instead of a virtual comparator call per comparison (~20 per entry).
        if self.cmp.is_bytewise() {
            out.sort_by(|a, b| a.user_key.cmp(&b.user_key).then_with(|| b.seq.cmp(&a.seq)));
        } else {
            out.sort_by(|a, b| {
                self.cmp
                    .compare(&a.user_key, &b.user_key)
                    .then_with(|| b.seq.cmp(&a.seq))
            });
        }
        out
    }

    /// A bidirectional, seekable read iterator over a consistent MVCC view.
    ///
    /// Default build: lazy — a [`LazyMemIter`] k-way merge over the shard skip
    /// lists, `O(shards)` to construct (no per-entry cloning or sorting).  Later
    /// inserts the cursors may physically observe are invisible under the
    /// caller's `read_seq` filter; see [`LazyMemIter`] for the argument.
    ///
    /// `arena-memtable` build: forward operations run a lazy k-way merge over
    /// the arena shards ([`LazyArenaIter`]); the first *reverse* operation
    /// falls back to a materialized snapshot (the arena cursor is
    /// forward-only).
    #[cfg(not(feature = "arena-memtable"))]
    pub fn iter(self: &Arc<Self>) -> MemIter {
        LazyMemIter::new(self.clone())
    }

    /// See [`iter`](Self::iter) — lazy-forward variant over the arena shards.
    #[cfg(feature = "arena-memtable")]
    pub fn iter(self: &Arc<Self>) -> MemIter {
        LazyArenaIter::new(self.clone())
    }

    /// A zero-materialization merge over all shards in internal order, for the
    /// flush path: keys and values are borrowed straight from the arena nodes
    /// instead of copied into a sorted `Vec<Entry>`.
    #[cfg(feature = "arena-memtable")]
    pub(crate) fn flush_merge(&self) -> FlushMerge<'_> {
        let cursors: Vec<crate::memtable_arena::ShardCursor<'_>> = self
            .shards
            .iter()
            .map(|s| s.cursor())
            .filter(|c| c.valid())
            .collect();
        FlushMerge::new(cursors, self.cmp.clone())
    }
}

/// The read-iterator type exposed to [`iterator::ChildIter`](crate::iterator):
/// the lazy shard-merge by default, the lazy-forward arena merge under the
/// fast path. Both expose the same inherent methods.
#[cfg(not(feature = "arena-memtable"))]
pub type MemIter = LazyMemIter;
#[cfg(feature = "arena-memtable")]
pub type MemIter = LazyArenaIter;

// ===========================================================================
// Lazy read iterator (default, `crossbeam-skiplist` build)
// ===========================================================================

/// A persistent cursor over one shard's skip list. Holds a `crossbeam` entry,
/// which is a bidirectional cursor: `move_next`/`move_prev` follow the level-0
/// links in `O(1)` and leave the entry pinned by refcount, and (re-)seeks go
/// through the shard's `lower_bound`/`upper_bound`. Keys/values are borrowed
/// straight from the pinned node — no copy at the shard level.
#[cfg(not(feature = "arena-memtable"))]
struct ShardCur<'a> {
    map: &'a SkipMap<IKey, Val>,
    /// `None` once stepped past an end (or seeked to an empty result).
    cur: Option<SkipEntry<'a, IKey, Val>>,
}

#[cfg(not(feature = "arena-memtable"))]
impl<'a> ShardCur<'a> {
    #[inline]
    fn valid(&self) -> bool {
        self.cur.is_some()
    }
    /// The current entry. Callers gate on [`valid`](Self::valid).
    #[inline]
    fn entry(&self) -> &SkipEntry<'a, IKey, Val> {
        self.cur.as_ref().unwrap()
    }
    #[inline]
    fn ik(&self) -> &[u8] {
        &self.entry().key().ik
    }
    #[inline]
    fn user_key(&self) -> &[u8] {
        format::user_key(&self.entry().key().ik)
    }
    #[inline]
    fn seq(&self) -> u64 {
        format::seq(&self.entry().key().ik)
    }
    #[inline]
    fn val(&self) -> &Val {
        self.entry().value()
    }
    #[inline]
    fn seek_first(&mut self) {
        self.cur = self.map.front();
    }
    #[inline]
    fn seek_last(&mut self) {
        self.cur = self.map.back();
    }
    /// First entry `>= probe` in internal order.
    #[inline]
    fn seek_ge(&mut self, probe: &IKey) {
        self.cur = self.map.lower_bound(Bound::Included(probe));
    }
    /// Last entry `<= probe` in internal order.
    #[inline]
    fn seek_le(&mut self, probe: &IKey) {
        self.cur = self.map.upper_bound(Bound::Included(probe));
    }
    /// First entry strictly `> probe` (used when reversing to forward).
    #[inline]
    fn seek_gt(&mut self, probe: &IKey) {
        self.cur = self.map.lower_bound(Bound::Excluded(probe));
    }
    /// Last entry strictly `< probe` (used when reversing to backward).
    #[inline]
    fn seek_lt(&mut self, probe: &IKey) {
        self.cur = self.map.upper_bound(Bound::Excluded(probe));
    }
    #[inline]
    fn step_next(&mut self) {
        if let Some(e) = &mut self.cur {
            // `move_next` returns false at the end and leaves `e` on the last
            // node; mark the cursor exhausted so `valid()` reports it.
            if !e.move_next() {
                self.cur = None;
            }
        }
    }
    #[inline]
    fn step_prev(&mut self) {
        if let Some(e) = &mut self.cur {
            if !e.move_prev() {
                self.cur = None;
            }
        }
    }
}

/// Lazy k-way merge over the memtable's [`NUM_SHARDS`] shard skip lists in
/// internal order (user key ascending, seq descending). Every shard contributes
/// a persistent [`ShardCur`]; positioning them all is `O(shards)`, never
/// `O(entries)`.
///
/// Bidirectional in the LevelDB style: `dir` selects a min-heap (forward) or a
/// max-heap (backward) keyed by internal order. Reversing direction repositions
/// every non-top cursor relative to the current key (see
/// [`flip_to_forward`](Self::flip_to_forward)/[`flip_to_backward`](Self::flip_to_backward)),
/// so arbitrary `next`/`prev` interleaving matches a random-access cursor over
/// the sorted sequence — i.e. the old materialized `snapshot()` `Vec`.
///
/// ## Snapshot consistency under concurrent writers
///
/// The cursors read the live, lock-free skip lists, so they may physically see
/// entries inserted *after* the iterator was built. That is harmless:
///
/// * Sequence numbers are monotonic and become visible only via the gap-free
///   `visible` cursor, so a reader's `read_seq` implies every seq `<= read_seq`
///   was already published (hence already in the memtable) before `read_seq` was
///   observed. Any entry inserted *after* iterator construction therefore has
///   `seq > read_seq`.
/// * This merge does no seq filtering itself — it yields *every* version, exactly
///   as the snapshot path did. The public
///   [`Iterator`](crate::iterator::Iterator) collapses versions and drops
///   `seq > read_seq` during `advance_forward`/`advance_backward`. So a
///   later-inserted version is either skipped outright (its whole user key is a
///   phantom with only `seq > read_seq`) or shadowed by the visible older
///   version. Either way the visible result equals what a point-in-time snapshot
///   at `read_seq` would have produced.
///
/// Immutable memtables are sealed (no writer ever, per the rotation protocol),
/// so iteration over them is trivially stable; only the active memtable can grow.
#[cfg(not(feature = "arena-memtable"))]
struct MemMerge<'a> {
    shards: Vec<ShardCur<'a>>,
    /// Indices into `shards` of the currently valid cursors, kept as a binary
    /// heap under the active direction (`heap[0]` is the merge top).
    heap: Vec<usize>,
    /// `+1` forward (min-heap), `-1` backward (max-heap).
    dir: i8,
    cmp: ComparatorRef,
    bytewise: bool,
}

#[cfg(not(feature = "arena-memtable"))]
impl<'a> MemMerge<'a> {
    fn new(mem: &'a Arc<Memtable>) -> MemMerge<'a> {
        let cmp = mem.cmp.clone();
        let bytewise = cmp.is_bytewise();
        let shards = mem
            .shards
            .iter()
            .map(|s| ShardCur { map: s, cur: None })
            .collect();
        MemMerge {
            shards,
            heap: Vec::new(),
            dir: 1,
            cmp,
            bytewise,
        }
    }

    #[inline]
    fn key_seq(&self, i: usize) -> (&[u8], u64) {
        format::split_internal_key(self.shards[i].ik())
    }

    /// Does cursor `a` sort before cursor `b` under the active direction? Uses
    /// the inlined 8-byte prefix compare for byte-wise orderings.
    fn before(&self, a: usize, b: usize) -> bool {
        let (uka, sa) = self.key_seq(a);
        let (ukb, sb) = self.key_seq(b);
        let key_ord = if self.bytewise {
            match crate::sst::key_prefix8(uka).cmp(&crate::sst::key_prefix8(ukb)) {
                Ordering::Equal => uka.cmp(ukb),
                ord => ord,
            }
        } else {
            self.cmp.compare(uka, ukb)
        };
        // Higher seq sorts first in internal (forward) order.
        let ord = key_ord.then_with(|| sb.cmp(&sa));
        if self.dir >= 0 {
            ord.is_lt()
        } else {
            ord.is_gt()
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
        for (i, c) in self.shards.iter().enumerate() {
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
        for c in &mut self.shards {
            c.seek_first();
        }
        self.dir = 1;
        self.rebuild();
    }

    fn seek_to_last(&mut self) {
        for c in &mut self.shards {
            c.seek_last();
        }
        self.dir = -1;
        self.rebuild();
    }

    fn seek_ge(&mut self, user_key: &[u8], seq: u64) {
        let probe = IKey::new(user_key, seq, &self.cmp);
        for c in &mut self.shards {
            c.seek_ge(&probe);
        }
        self.dir = 1;
        self.rebuild();
    }

    fn seek_le(&mut self, user_key: &[u8], seq: u64) {
        let probe = IKey::new(user_key, seq, &self.cmp);
        for c in &mut self.shards {
            c.seek_le(&probe);
        }
        self.dir = -1;
        self.rebuild();
    }

    fn next(&mut self) {
        if self.heap.is_empty() {
            return;
        }
        if self.dir < 0 {
            self.flip_to_forward();
            return;
        }
        let idx = self.heap[0];
        self.shards[idx].step_next();
        self.settle_top();
    }

    fn prev(&mut self) {
        if self.heap.is_empty() {
            return;
        }
        if self.dir > 0 {
            self.flip_to_backward();
            return;
        }
        let idx = self.heap[0];
        self.shards[idx].step_prev();
        self.settle_top();
    }

    /// After stepping the top cursor, restore the heap invariant (dropping the
    /// top if it went invalid).
    fn settle_top(&mut self) {
        if !self.shards[self.heap[0]].valid() {
            let last = self.heap.len() - 1;
            self.heap[0] = self.heap[last];
            self.heap.pop();
        }
        if !self.heap.is_empty() {
            self.heap_down(0);
        }
    }

    /// Reverse from backward to forward: put every non-top cursor at the first
    /// entry strictly after the current key, step the top forward, re-heap.
    fn flip_to_forward(&mut self) {
        let top = self.heap[0];
        let probe = {
            let (uk, s) = self.key_seq(top);
            IKey::new(uk, s, &self.cmp)
        };
        for (i, c) in self.shards.iter_mut().enumerate() {
            if i != top {
                c.seek_gt(&probe);
            }
        }
        self.shards[top].step_next();
        self.dir = 1;
        self.rebuild();
    }

    /// Reverse from forward to backward: symmetric to
    /// [`flip_to_forward`](Self::flip_to_forward).
    fn flip_to_backward(&mut self) {
        let top = self.heap[0];
        let probe = {
            let (uk, s) = self.key_seq(top);
            IKey::new(uk, s, &self.cmp)
        };
        for (i, c) in self.shards.iter_mut().enumerate() {
            if i != top {
                c.seek_lt(&probe);
            }
        }
        self.shards[top].step_prev();
        self.dir = -1;
        self.rebuild();
    }

    #[inline]
    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }
    #[inline]
    fn top(&self) -> &ShardCur<'a> {
        &self.shards[self.heap[0]]
    }
}

#[cfg(not(feature = "arena-memtable"))]
self_cell!(
    struct MemMergeCell {
        owner: Arc<Memtable>,
        #[covariant]
        dependent: MemMerge,
    }
);

/// Bidirectional, seekable read iterator over the live memtable, lazily merging
/// its shard skip lists. Owns the `Arc<Memtable>` and holds shard cursors
/// borrowing from inside it (via [`self_cell`]); no `unsafe` in this crate.
///
/// Exposes the same inherent surface as [`MemIterator`] so both are drop-in for
/// [`iterator::ChildIter`](crate::iterator).
#[cfg(not(feature = "arena-memtable"))]
pub struct LazyMemIter {
    cell: MemMergeCell,
}

#[cfg(not(feature = "arena-memtable"))]
impl LazyMemIter {
    fn new(mem: Arc<Memtable>) -> LazyMemIter {
        LazyMemIter {
            cell: MemMergeCell::new(mem, |owner| MemMerge::new(owner)),
        }
    }

    pub fn valid(&self) -> bool {
        self.cell.borrow_dependent().valid()
    }
    pub fn seek_to_first(&mut self) {
        self.cell.with_dependent_mut(|_, m| m.seek_to_first());
    }
    pub fn seek_to_last(&mut self) {
        self.cell.with_dependent_mut(|_, m| m.seek_to_last());
    }
    /// Position at the newest version of the first user key `>= user_key`.
    pub fn seek(&mut self, user_key: &[u8]) {
        self.cell
            .with_dependent_mut(|_, m| m.seek_ge(user_key, u64::MAX));
    }
    pub fn seek_ge(&mut self, user_key: &[u8], seq: u64) {
        self.cell
            .with_dependent_mut(|_, m| m.seek_ge(user_key, seq));
    }
    pub fn seek_le(&mut self, user_key: &[u8], seq: u64) {
        self.cell
            .with_dependent_mut(|_, m| m.seek_le(user_key, seq));
    }
    pub fn next(&mut self) {
        self.cell.with_dependent_mut(|_, m| m.next());
    }
    pub fn prev(&mut self) {
        self.cell.with_dependent_mut(|_, m| m.prev());
    }

    pub fn user_key(&self) -> &[u8] {
        self.cell.borrow_dependent().top().user_key()
    }
    /// Zero-padded 8-byte prefix of the current user key (see
    /// `sst::iter::key_prefix8`).
    #[inline]
    pub(crate) fn key_prefix(&self) -> u64 {
        crate::sst::key_prefix8(self.user_key())
    }
    pub fn seq(&self) -> u64 {
        self.cell.borrow_dependent().top().seq()
    }
    pub fn ttl(&self) -> i64 {
        self.cell.borrow_dependent().top().val().ttl
    }
    pub fn is_tombstone(&self) -> bool {
        self.cell.borrow_dependent().top().val().flags & flags::TOMBSTONE != 0
    }
    pub fn value(&self) -> Vec<u8> {
        self.cell.borrow_dependent().top().val().value.clone()
    }
    /// Borrow the current value without copying.
    pub fn value_ref(&self) -> &[u8] {
        &self.cell.borrow_dependent().top().val().value
    }
}

#[cfg(not(feature = "arena-memtable"))]
impl std::fmt::Debug for LazyMemIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyMemIter")
            .field("valid", &self.valid())
            .finish()
    }
}

/// K-way merge over sorted shard cursors (user key ascending, seq descending),
/// comparing cached 8-byte key prefixes first for byte-wise orderings.
#[cfg(feature = "arena-memtable")]
pub(crate) struct FlushMerge<'a> {
    cursors: Vec<crate::memtable_arena::ShardCursor<'a>>,
    heap: Vec<u32>, // indices into `cursors`
    cmp: ComparatorRef,
    bytewise: bool,
}

#[cfg(feature = "arena-memtable")]
impl<'a> FlushMerge<'a> {
    fn new(cursors: Vec<crate::memtable_arena::ShardCursor<'a>>, cmp: ComparatorRef) -> Self {
        let bytewise = cmp.is_bytewise();
        let mut m = FlushMerge {
            heap: (0..cursors.len() as u32).collect(),
            cursors,
            cmp,
            bytewise,
        };
        if m.heap.len() > 1 {
            for i in (0..m.heap.len() / 2).rev() {
                m.heap_down(i);
            }
        }
        m
    }

    #[inline]
    fn before(&self, a: u32, b: u32) -> bool {
        let (ca, cb) = (&self.cursors[a as usize], &self.cursors[b as usize]);
        let key_ord = if self.bytewise {
            match ca.key_prefix().cmp(&cb.key_prefix()) {
                std::cmp::Ordering::Equal => ca.user_key().cmp(cb.user_key()),
                ord => ord,
            }
        } else {
            self.cmp.compare(ca.user_key(), cb.user_key())
        };
        key_ord.then_with(|| cb.seq().cmp(&ca.seq())).is_lt()
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

    #[inline]
    pub(crate) fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    /// The current smallest cursor. Callers must check [`valid`](Self::valid).
    #[inline]
    pub(crate) fn top(&self) -> &crate::memtable_arena::ShardCursor<'a> {
        &self.cursors[self.heap[0] as usize]
    }

    pub(crate) fn advance(&mut self) {
        if self.heap.is_empty() {
            return;
        }
        let idx = self.heap[0] as usize;
        self.cursors[idx].advance();
        if !self.cursors[idx].valid() {
            let last = self.heap.len() - 1;
            self.heap[0] = self.heap[last];
            self.heap.pop();
        }
        if !self.heap.is_empty() {
            self.heap_down(0);
        }
    }
}

/// Bidirectional iterator over a materialized memtable snapshot. Used by the
/// `arena-memtable` read path (the arena shard cursor is forward-only, so it
/// keeps the snapshot path); the default build reads lazily via [`LazyMemIter`].
#[cfg(feature = "arena-memtable")]
#[derive(Debug)]
pub struct MemIterator {
    entries: Vec<Entry>,
    cmp: ComparatorRef,
    pos: usize,
    valid: bool,
}

#[cfg(feature = "arena-memtable")]
impl MemIterator {
    fn new(entries: Vec<Entry>, cmp: ComparatorRef) -> MemIterator {
        MemIterator {
            entries,
            cmp,
            pos: 0,
            valid: false,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid && self.pos < self.entries.len()
    }

    pub fn seek_to_first(&mut self) {
        self.pos = 0;
        self.valid = !self.entries.is_empty();
    }

    pub fn seek_to_last(&mut self) {
        if self.entries.is_empty() {
            self.valid = false;
        } else {
            self.pos = self.entries.len() - 1;
            self.valid = true;
        }
    }

    /// Position at the newest version of the first user key `>= user_key`.
    pub fn seek(&mut self, user_key: &[u8]) {
        self.seek_ge(user_key, u64::MAX);
    }

    /// Position at the first entry whose `(user_key, seq)` is `>=` the target.
    pub fn seek_ge(&mut self, user_key: &[u8], seq: u64) {
        let idx = self.entries.partition_point(|e| {
            match self.cmp.compare(&e.user_key, user_key) {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => e.seq > seq, // seq descending: higher seq comes first
            }
        });
        self.pos = idx;
        self.valid = idx < self.entries.len();
    }

    /// Position at the last entry whose `(user_key, seq)` is `<=` the target.
    pub fn seek_le(&mut self, user_key: &[u8], seq: u64) {
        self.seek_ge(user_key, seq);
        if !self.valid() {
            self.seek_to_last();
        } else {
            // If we landed exactly on the target it is <=; otherwise step back.
            let e = &self.entries[self.pos];
            let cmp = self
                .cmp
                .compare(&e.user_key, user_key)
                .then_with(|| seq.cmp(&e.seq));
            if cmp != Ordering::Equal {
                self.prev();
            }
        }
    }

    pub fn next(&mut self) {
        if self.valid() {
            self.pos += 1;
            self.valid = self.pos < self.entries.len();
        }
    }

    pub fn prev(&mut self) {
        if self.pos == 0 {
            self.valid = false;
        } else {
            self.pos -= 1;
            self.valid = true;
        }
    }

    pub fn entry(&self) -> &Entry {
        &self.entries[self.pos]
    }
    pub fn user_key(&self) -> &[u8] {
        &self.entries[self.pos].user_key
    }
    /// Zero-padded 8-byte prefix of the current user key (see
    /// `sst::iter::key_prefix8`).
    #[inline]
    pub(crate) fn key_prefix(&self) -> u64 {
        crate::sst::key_prefix8(&self.entries[self.pos].user_key)
    }
    pub fn seq(&self) -> u64 {
        self.entries[self.pos].seq
    }
    pub fn ttl(&self) -> i64 {
        self.entries[self.pos].ttl
    }
    pub fn is_tombstone(&self) -> bool {
        self.entries[self.pos].tombstone
    }
    pub fn value(&self) -> Vec<u8> {
        self.entries[self.pos].value.clone()
    }
    /// Borrow the current value without copying.
    pub fn value_ref(&self) -> &[u8] {
        &self.entries[self.pos].value
    }
}

// ===========================================================================
// Lazy read iterator (`arena-memtable` build)
// ===========================================================================

/// Forward-only lazy k-way merge over the arena shards in internal order
/// (user key ascending, seq descending). Positioning (`seek_to_first`,
/// `seek_ge`) is `O(shards)` — one cursor per shard plus a heapify — never
/// `O(entries)`; `next` advances the heap top in `O(log shards)`.
///
/// Snapshot consistency under concurrent writers follows the same argument as
/// [`MemMerge`]: arena nodes are Acquire-published and never freed while the
/// memtable is alive, and any entry inserted after iterator construction has
/// `seq > read_seq`, so the public [`Iterator`](crate::iterator::Iterator)
/// filters it during version collapse.
#[cfg(feature = "arena-memtable")]
struct ArenaMerge<'a> {
    mem: &'a Memtable,
    /// One cursor per shard (invalid cursors stay in place; only `heap`
    /// membership tracks validity).
    cursors: Vec<crate::memtable_arena::ShardCursor<'a>>,
    /// Indices into `cursors` of the currently valid cursors, kept as a
    /// min-heap under internal order (`heap[0]` is the merge top).
    heap: Vec<u32>,
    cmp: ComparatorRef,
    bytewise: bool,
}

#[cfg(feature = "arena-memtable")]
impl<'a> ArenaMerge<'a> {
    fn new(mem: &'a Memtable) -> ArenaMerge<'a> {
        let cmp = mem.cmp.clone();
        let bytewise = cmp.is_bytewise();
        ArenaMerge {
            mem,
            cursors: Vec::new(),
            heap: Vec::new(),
            cmp,
            bytewise,
        }
    }

    /// Does cursor `a` sort before cursor `b` in internal order? Uses the
    /// cached 8-byte key prefix to decide most byte-wise comparisons inline.
    #[inline]
    fn before(&self, a: u32, b: u32) -> bool {
        let (ca, cb) = (&self.cursors[a as usize], &self.cursors[b as usize]);
        let key_ord = if self.bytewise {
            match ca.key_prefix().cmp(&cb.key_prefix()) {
                Ordering::Equal => ca.user_key().cmp(cb.user_key()),
                ord => ord,
            }
        } else {
            self.cmp.compare(ca.user_key(), cb.user_key())
        };
        key_ord.then_with(|| cb.seq().cmp(&ca.seq())).is_lt()
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
        for (i, c) in self.cursors.iter().enumerate() {
            if c.valid() {
                self.heap.push(i as u32);
            }
        }
        if self.heap.len() > 1 {
            for i in (0..self.heap.len() / 2).rev() {
                self.heap_down(i);
            }
        }
    }

    fn seek_to_first(&mut self) {
        let mem = self.mem;
        self.cursors.clear();
        self.cursors.extend(mem.shards.iter().map(|s| s.cursor()));
        self.rebuild();
    }

    fn seek_ge(&mut self, user_key: &[u8], seq: u64) {
        let mem = self.mem;
        self.cursors.clear();
        self.cursors
            .extend(mem.shards.iter().map(|s| s.cursor_ge(user_key, seq)));
        self.rebuild();
    }

    fn next(&mut self) {
        if self.heap.is_empty() {
            return;
        }
        let idx = self.heap[0] as usize;
        self.cursors[idx].advance();
        if !self.cursors[idx].valid() {
            let last = self.heap.len() - 1;
            self.heap[0] = self.heap[last];
            self.heap.pop();
        }
        if !self.heap.is_empty() {
            self.heap_down(0);
        }
    }

    #[inline]
    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    #[inline]
    fn top(&self) -> &crate::memtable_arena::ShardCursor<'a> {
        &self.cursors[self.heap[0] as usize]
    }
}

#[cfg(feature = "arena-memtable")]
self_cell!(
    struct ArenaMergeCell {
        owner: Arc<Memtable>,
        #[covariant]
        dependent: ArenaMerge,
    }
);

#[cfg(feature = "arena-memtable")]
enum ArenaIterState {
    /// Lazy forward merge directly over the arena shards.
    Lazy(ArenaMergeCell),
    /// Materialized-snapshot fallback, entered on the first reverse operation.
    Snap(MemIterator),
}

/// Bidirectional, seekable read iterator for the `arena-memtable` build.
///
/// Forward operations (`seek_to_first`, `seek`/`seek_ge`, `next`) run a lazy
/// k-way merge over the arena shard skip lists — `O(shards)` to position, no
/// per-entry materialization — so `range_first`-style peeks and forward scans
/// no longer pay `O(entries)` per iterator. The arena cursor is forward-only,
/// so the first *reverse* operation (`seek_to_last`, `seek_le`, `prev`) falls
/// back to a materialized sorted snapshot (the pre-existing behavior) and the
/// iterator continues there, repositioned exactly where it stood.
///
/// Exposes the same inherent surface as [`LazyMemIter`] so both are drop-in
/// for [`iterator::ChildIter`](crate::iterator).
#[cfg(feature = "arena-memtable")]
pub struct LazyArenaIter {
    state: ArenaIterState,
}

#[cfg(feature = "arena-memtable")]
impl LazyArenaIter {
    fn new(mem: Arc<Memtable>) -> LazyArenaIter {
        LazyArenaIter {
            state: ArenaIterState::Lazy(ArenaMergeCell::new(mem, |owner| ArenaMerge::new(owner))),
        }
    }

    /// Switch to the materialized-snapshot fallback (idempotent), optionally
    /// repositioning at an exact `(user_key, seq)` the lazy merge stood on.
    fn materialize(&mut self, at: Option<(Vec<u8>, u64)>) -> &mut MemIterator {
        if let ArenaIterState::Lazy(cell) = &self.state {
            let mem = cell.borrow_owner().clone();
            let mut it = MemIterator::new(mem.snapshot(), mem.cmp.clone());
            if let Some((uk, seq)) = &at {
                it.seek_ge(uk, *seq);
            }
            self.state = ArenaIterState::Snap(it);
        }
        match &mut self.state {
            ArenaIterState::Snap(it) => it,
            ArenaIterState::Lazy(_) => unreachable!(),
        }
    }

    pub fn valid(&self) -> bool {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().valid(),
            ArenaIterState::Snap(it) => it.valid(),
        }
    }
    pub fn seek_to_first(&mut self) {
        match &mut self.state {
            ArenaIterState::Lazy(c) => c.with_dependent_mut(|_, m| m.seek_to_first()),
            ArenaIterState::Snap(it) => it.seek_to_first(),
        }
    }
    pub fn seek_to_last(&mut self) {
        if let ArenaIterState::Snap(it) = &mut self.state {
            it.seek_to_last();
            return;
        }
        self.materialize(None).seek_to_last();
    }
    /// Position at the newest version of the first user key `>= user_key`.
    pub fn seek(&mut self, user_key: &[u8]) {
        self.seek_ge(user_key, u64::MAX);
    }
    pub fn seek_ge(&mut self, user_key: &[u8], seq: u64) {
        match &mut self.state {
            ArenaIterState::Lazy(c) => c.with_dependent_mut(|_, m| m.seek_ge(user_key, seq)),
            ArenaIterState::Snap(it) => it.seek_ge(user_key, seq),
        }
    }
    pub fn seek_le(&mut self, user_key: &[u8], seq: u64) {
        if let ArenaIterState::Snap(it) = &mut self.state {
            it.seek_le(user_key, seq);
            return;
        }
        self.materialize(None).seek_le(user_key, seq);
    }
    pub fn next(&mut self) {
        match &mut self.state {
            ArenaIterState::Lazy(c) => c.with_dependent_mut(|_, m| m.next()),
            ArenaIterState::Snap(it) => it.next(),
        }
    }
    pub fn prev(&mut self) {
        if let ArenaIterState::Snap(it) = &mut self.state {
            it.prev();
            return;
        }
        // Forward-only lazy merge: fall back to the snapshot, repositioned at
        // the current entry so `prev` steps back from the right place. The
        // entry cannot disappear (arena nodes are never removed), so the
        // reposition lands exactly.
        let at = {
            let ArenaIterState::Lazy(c) = &self.state else {
                unreachable!()
            };
            let m = c.borrow_dependent();
            if m.valid() {
                Some((m.top().user_key().to_vec(), m.top().seq()))
            } else {
                None
            }
        };
        match at {
            Some(at) => self.materialize(Some(at)).prev(),
            // prev() on an unpositioned iterator is a no-op (the merging
            // parent only calls prev on valid children).
            None => {}
        }
    }

    pub fn user_key(&self) -> &[u8] {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().top().user_key(),
            ArenaIterState::Snap(it) => it.user_key(),
        }
    }
    /// Zero-padded 8-byte prefix of the current user key (see
    /// `sst::iter::key_prefix8`).
    #[inline]
    pub(crate) fn key_prefix(&self) -> u64 {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().top().key_prefix(),
            ArenaIterState::Snap(it) => it.key_prefix(),
        }
    }
    pub fn seq(&self) -> u64 {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().top().seq(),
            ArenaIterState::Snap(it) => it.seq(),
        }
    }
    pub fn ttl(&self) -> i64 {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().top().ttl(),
            ArenaIterState::Snap(it) => it.ttl(),
        }
    }
    pub fn is_tombstone(&self) -> bool {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().top().tombstone(),
            ArenaIterState::Snap(it) => it.is_tombstone(),
        }
    }
    pub fn value(&self) -> Vec<u8> {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().top().value().to_vec(),
            ArenaIterState::Snap(it) => it.value(),
        }
    }
    /// Borrow the current value without copying.
    pub fn value_ref(&self) -> &[u8] {
        match &self.state {
            ArenaIterState::Lazy(c) => c.borrow_dependent().top().value(),
            ArenaIterState::Snap(it) => it.value_ref(),
        }
    }
}

#[cfg(feature = "arena-memtable")]
impl std::fmt::Debug for LazyArenaIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyArenaIter")
            .field("valid", &self.valid())
            .field(
                "materialized",
                &matches!(self.state, ArenaIterState::Snap(_)),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::default_comparator;

    fn mt() -> Arc<Memtable> {
        Memtable::new(default_comparator())
    }

    #[test]
    fn put_get_versions() {
        let m = mt();
        m.put(b"k", b"v1".to_vec(), 1, 0, false, false);
        m.put(b"k", b"v2".to_vec(), 5, 0, false, false);
        // read at seq 10 sees newest (v2)
        let r = m.get(b"k", 10, 0);
        assert!(r.found && !r.deleted);
        assert_eq!(r.value, b"v2");
        assert_eq!(r.seq, 5);
        // read at seq 3 sees v1
        let r = m.get(b"k", 3, 0);
        assert_eq!(r.value, b"v1");
        assert_eq!(r.seq, 1);
        // read at seq 0 sees nothing
        assert!(!m.get(b"k", 0, 0).found);
    }

    #[test]
    fn tombstone_and_ttl() {
        let m = mt();
        m.put(b"a", b"x".to_vec(), 1, 0, false, false);
        m.put(b"a", Vec::new(), 2, 0, true, false);
        let r = m.get(b"a", 10, 0);
        assert!(r.found && r.deleted);

        m.put(b"b", b"y".to_vec(), 3, 100, false, false); // ttl=100ns
        assert!(m.get(b"b", 10, 50).found && !m.get(b"b", 10, 50).deleted); // not expired at now=50
        assert!(m.get(b"b", 10, 200).deleted); // expired at now=200
    }

    #[test]
    fn missing_key() {
        let m = mt();
        m.put(b"a", b"1".to_vec(), 1, 0, false, false);
        assert!(!m.get(b"z", 10, 0).found);
    }

    #[test]
    fn iterator_forward_and_backward() {
        let m = mt();
        for (i, k) in [b"a", b"c", b"b", b"e", b"d"].iter().enumerate() {
            m.put(k.as_slice(), vec![i as u8], (i + 1) as u64, 0, false, false);
        }
        let mut it = m.iter();
        let mut fwd = Vec::new();
        it.seek_to_first();
        while it.valid() {
            fwd.push(it.user_key().to_vec());
            it.next();
        }
        assert_eq!(fwd, vec![b"a", b"b", b"c", b"d", b"e"]);

        let mut bwd = Vec::new();
        it.seek_to_last();
        while it.valid() {
            bwd.push(it.user_key().to_vec());
            it.prev();
        }
        assert_eq!(bwd, vec![b"e", b"d", b"c", b"b", b"a"]);
    }

    #[test]
    fn iterator_seek() {
        let m = mt();
        for k in [b"a", b"c", b"e", b"g"] {
            m.put(k.as_slice(), b"v".to_vec(), 1, 0, false, false);
        }
        let mut it = m.iter();
        it.seek(b"d");
        assert!(it.valid());
        assert_eq!(it.user_key(), b"e");
        it.seek(b"a");
        assert_eq!(it.user_key(), b"a");
        it.seek(b"z");
        assert!(!it.valid());

        it.seek_le(b"d", u64::MAX);
        assert!(it.valid());
        assert_eq!(it.user_key(), b"c");
    }

    #[test]
    fn iterator_mvcc_ordering() {
        // Same key, multiple versions: newest (highest seq) first.
        let m = mt();
        m.put(b"k", b"old".to_vec(), 1, 0, false, false);
        m.put(b"k", b"new".to_vec(), 9, 0, false, false);
        let mut it = m.iter();
        it.seek_to_first();
        assert_eq!(it.seq(), 9);
        it.next();
        assert_eq!(it.seq(), 1);
    }

    #[test]
    fn concurrent_writers() {
        use std::thread;
        let m = mt();
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let m = m.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    let k = format!("k{}", (t * 1000 + i) % 500);
                    m.put(
                        k.as_bytes(),
                        b"v".to_vec(),
                        t * 1000 + i + 1,
                        0,
                        false,
                        false,
                    );
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 500 distinct keys, all should resolve.
        for i in 0..500u64 {
            let k = format!("k{i}");
            assert!(m.get(k.as_bytes(), u64::MAX, 0).found, "missing {k}");
        }
        assert_eq!(m.num_entries(), 8000);
    }

    // ---- Lazy read iterator (default build) ------------------------------
    // These pin the lazy shard-merge against the materialized `snapshot()`
    // reference, exercise seek/reverse/interleave, and prove construction is
    // sub-linear. Under `arena-memtable` the read iterator IS the snapshot
    // path, so the equality checks are vacuous and the tests are gated off.

    #[cfg(not(feature = "arena-memtable"))]
    fn snapshot_triples(m: &Arc<Memtable>) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
        m.snapshot()
            .into_iter()
            .map(|e| (e.user_key, e.seq, e.value))
            .collect()
    }

    #[cfg(not(feature = "arena-memtable"))]
    fn lazy_forward(m: &Arc<Memtable>) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
        let mut it = m.iter();
        let mut out = Vec::new();
        it.seek_to_first();
        while it.valid() {
            out.push((it.user_key().to_vec(), it.seq(), it.value_ref().to_vec()));
            it.next();
        }
        out
    }

    #[cfg(not(feature = "arena-memtable"))]
    fn lazy_backward(m: &Arc<Memtable>) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
        let mut it = m.iter();
        let mut out = Vec::new();
        it.seek_to_last();
        while it.valid() {
            out.push((it.user_key().to_vec(), it.seq(), it.value_ref().to_vec()));
            it.prev();
        }
        out
    }

    #[test]
    #[cfg(not(feature = "arena-memtable"))]
    fn lazy_iter_matches_snapshot_forward_and_reverse() {
        let m = mt();
        // Many keys spread across shards, inserted in scrambled order, with
        // several MVCC versions per key (incl. tombstones).
        let mut seq = 1u64;
        for round in 0..3u64 {
            for i in 0..400u64 {
                let k = format!("key-{:05}", (i * 7 + round) % 400);
                let tomb = round == 2 && i % 5 == 0;
                let v = format!("v{seq}").into_bytes();
                m.put(k.as_bytes(), v, seq, 0, tomb, false);
                seq += 1;
            }
        }
        let reference = snapshot_triples(&m);
        assert_eq!(
            lazy_forward(&m),
            reference,
            "forward lazy iteration must equal the snapshot (all versions, ordered)"
        );
        let mut back = lazy_backward(&m);
        back.reverse();
        assert_eq!(
            back, reference,
            "reverse lazy iteration must equal the reversed snapshot"
        );
    }

    #[test]
    #[cfg(not(feature = "arena-memtable"))]
    fn lazy_iter_seek_and_interleave() {
        let m = mt();
        for k in ["a", "c", "e", "g", "i"] {
            m.put(k.as_bytes(), b"v".to_vec(), 1, 0, false, false);
        }
        let mut it = m.iter();

        // seek(): first key >= target.
        it.seek(b"d");
        assert_eq!(it.user_key(), b"e");
        it.seek(b"e");
        assert_eq!(it.user_key(), b"e");
        it.seek(b"j");
        assert!(!it.valid());

        // seek_le(): last key <= (target, seq). The public `seek_for_prev` path
        // passes seq 0, i.e. the oldest phantom for `target`, so a real version
        // of `target` (seq >= 1, which sorts *before* seq 0) counts as `<=` and
        // is landed on.
        it.seek_le(b"f", 0);
        assert_eq!(it.user_key(), b"e");
        it.seek_le(b"e", 0);
        assert_eq!(it.user_key(), b"e");
        it.seek_le(b"0", 0); // before the first key
        assert!(!it.valid());

        // next/prev interleaving must reverse cleanly (exercises the flip path).
        it.seek_to_first();
        assert_eq!(it.user_key(), b"a");
        it.next();
        it.next();
        assert_eq!(it.user_key(), b"e");
        it.prev();
        assert_eq!(it.user_key(), b"c");
        it.prev();
        assert_eq!(it.user_key(), b"a");
        it.prev();
        assert!(!it.valid());

        // From the tail, walking back then forward again.
        it.seek_to_last();
        assert_eq!(it.user_key(), b"i");
        it.prev();
        assert_eq!(it.user_key(), b"g");
        it.next();
        assert_eq!(it.user_key(), b"i");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    #[cfg(not(feature = "arena-memtable"))]
    fn lazy_iter_mvcc_versions_in_seq_order() {
        // One user key with several versions across the merge: newest seq first.
        let m = mt();
        for &s in &[3u64, 9, 1, 7, 5] {
            m.put(b"k", format!("v{s}").into_bytes(), s, 0, false, false);
        }
        let mut it = m.iter();
        it.seek_to_first();
        let mut seqs = Vec::new();
        while it.valid() {
            seqs.push(it.seq());
            it.next();
        }
        assert_eq!(seqs, vec![9, 7, 5, 3, 1], "versions must be seq-descending");
    }

    #[test]
    #[cfg(not(feature = "arena-memtable"))]
    fn lazy_iter_construction_is_sublinear() {
        use std::time::{Duration, Instant};
        let m = mt();
        for i in 0..100_000u64 {
            let k = format!("k{i:08}");
            m.put(k.as_bytes(), b"value".to_vec(), i + 1, 0, false, false);
        }
        // The work the OLD `.iter()` did: materialize + sort every entry.
        let mut snap_best = Duration::MAX;
        for _ in 0..3 {
            let t = Instant::now();
            let s = m.snapshot();
            snap_best = snap_best.min(t.elapsed());
            assert_eq!(s.len(), 100_000);
        }
        // The work a one-record scan now pays: build the lazy iterator and land
        // on the first record. No per-entry materialization.
        let mut lazy_best = Duration::MAX;
        for _ in 0..50 {
            let t = Instant::now();
            let mut it = m.iter();
            it.seek_to_first();
            assert!(it.valid());
            let _ = it.user_key();
            lazy_best = lazy_best.min(t.elapsed());
        }
        // At least 50x cheaper than materializing, or comfortably under 100µs.
        assert!(
            lazy_best * 50 < snap_best || lazy_best < Duration::from_micros(100),
            "lazy construct+seek {lazy_best:?} not >=50x cheaper than snapshot {snap_best:?}"
        );
    }

    #[test]
    #[cfg(not(feature = "arena-memtable"))]
    fn lazy_iter_stays_ordered_under_concurrent_inserts() {
        use std::sync::atomic::{AtomicBool, Ordering as O};
        use std::thread;
        let m = mt();
        for i in 0..1000u64 {
            let k = format!("k{i:05}");
            m.put(k.as_bytes(), b"old".to_vec(), i + 1, 0, false, false);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let m = m.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                // Bounded insert count: each carries a unique seq, so this only
                // adds versions (not keys) — the cap keeps the memtable, and thus
                // each full scan below, from growing without bound.
                let mut seq = 1_000_000u64;
                while !stop.load(O::Relaxed) && seq < 1_030_000 {
                    let k = format!("k{:05}", seq % 2000);
                    m.put(k.as_bytes(), b"new".to_vec(), seq, 0, false, false);
                    seq += 1;
                }
            })
        };
        // Iterating a live, concurrently-mutated memtable must never panic and
        // must always yield a non-decreasing user-key sequence. (MVCC visibility
        // is the public Iterator's job; here we assert lock-free-cursor safety.)
        for _ in 0..20 {
            let mut it = m.iter();
            it.seek_to_first();
            let mut prev: Option<Vec<u8>> = None;
            while it.valid() {
                let k = it.user_key().to_vec();
                if let Some(p) = &prev {
                    assert!(p.as_slice() <= k.as_slice(), "iteration must stay ordered");
                }
                prev = Some(k);
                it.next();
            }
        }
        stop.store(true, O::Relaxed);
        writer.join().unwrap();
    }
}
