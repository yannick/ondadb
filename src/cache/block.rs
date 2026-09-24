//! Sharded, byte-bounded CLOCK (second-chance) cache of decompressed SSTable
//! bytes keyed by `(namespace, file_id, domain, offset)`.  Cached values are
//! immutable (`Arc<[u8]>`); callers must not mutate them.
//!
//! A `BlockCache` is a **view**: shared storage plus a namespace id. Every
//! private cache ([`BlockCache::new`]) is namespace 0 and owns its storage
//! alone. [`ReadResources`](crate::read_resources::ReadResources) hands each
//! leased database a view of one shared storage under its own namespace, so the
//! databases share one byte budget while table `7` of one can never be served
//! for table `7` of another.
//!
//! Reads are deliberately **non-serializing**: a hit takes the shard's
//! `RwLock` in *read* mode and sets an atomic reference bit — unlike an LRU,
//! it never reorders a recency list, so concurrent readers on the same shard
//! proceed in parallel (the previous `Mutex<LruCache>` made every cache *hit*
//! take an exclusive lock, which showed up as reader serialization on
//! point-read-heavy multi-threaded workloads).  Only `put` (insert +
//! clock-sweep eviction) takes the write lock.  CLOCK approximates LRU: the
//! sweep hand gives referenced entries a second chance, evicting only entries
//! not touched since the hand last passed.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

/// Which of an SSTable's two files an offset addresses.
///
/// A `Reader` owns a klog and a vlog under one `file_id`, and their offset
/// spaces are independent and both start at zero — so `(file_id, 0)` names
/// both the first data block and the first vlog frame. Without this tag a
/// cached vlog value would be returned where a data block was asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockDomain {
    /// A decompressed klog data block.
    Klog,
    /// A decoded (CRC-verified, decompressed) vlog value.
    Vlog,
}

impl BlockDomain {
    /// Hash salt, mixed into `shard_for` so the two domains of one
    /// `(file_id, off)` pair do not land in the same shard in lockstep — vlog
    /// admission would otherwise evict exactly the klog blocks it aliases.
    /// `Klog` keeps the zero salt so its shard placement is unchanged.
    #[inline]
    fn salt(self) -> u64 {
        match self {
            BlockDomain::Klog => 0,
            BlockDomain::Vlog => 0x9E37_79B9_7F4A_7C15,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct BlockKey {
    /// Which database's file-id space `file_id` belongs to; see the module
    /// docs. `0` for every private cache.
    ns: u64,
    file_id: u64,
    off: u64,
    domain: BlockDomain,
}

struct CacheEntry {
    data: Arc<[u8]>,
    /// CLOCK reference bit: set (Relaxed) on every hit, cleared by the sweep
    /// hand. Relaxed is enough — it only biases eviction order.
    referenced: AtomicBool,
}

/// Max entries a single eviction sweep may spare (see [`Shard::evict_to_cap`]).
const CLOCK_SWEEP_BUDGET: usize = 32;

struct Shard {
    map: HashMap<BlockKey, CacheEntry>,
    /// Clock ring in insertion order. Every map entry is in the ring exactly
    /// once; entries leave both together during a sweep.
    ring: VecDeque<BlockKey>,
    used: i64,
    cap: i64,
    /// The [`BlockDomain::Vlog`] share of `map.len()` and `used`, maintained
    /// incrementally. Both domains share one capacity, so telling them apart
    /// is how the cost of vlog admission to klog blocks is observed — and a
    /// `stats()` that walked the map to find out would be O(entries) on a call
    /// that DB-wide stats makes routinely.
    vlog_entries: usize,
    vlog_used: i64,
}

impl Shard {
    /// Drop `k`'s map entry, keeping `used` and the per-domain tallies right.
    /// A no-op when `k` is absent. The caller owns the ring.
    fn unlink(&mut self, k: &BlockKey) {
        let Some(e) = self.map.remove(k) else {
            return;
        };
        let len = e.data.len() as i64;
        self.used -= len;
        if k.domain == BlockDomain::Vlog {
            self.vlog_entries -= 1;
            self.vlog_used -= len;
        }
    }

    /// Evict with the clock hand until under capacity: pop the ring front;
    /// a referenced entry is cleared and pushed to the back (second chance),
    /// an unreferenced one is evicted.
    ///
    /// The sweep runs under the shard's WRITE lock, so second chances are
    /// strictly budgeted: when a workload keeps every entry referenced (hot
    /// scans re-touching the whole shard), an unbounded hand would walk
    /// thousands of slots clearing bits while readers stall. After
    /// [`CLOCK_SWEEP_BUDGET`] spared entries the hand evicts regardless —
    /// put latency stays bounded and capacity always converges.
    fn evict_to_cap(&mut self) -> u64 {
        let mut spared = 0usize;
        let mut evicted = 0u64;
        while self.used > self.cap && self.map.len() > 1 {
            let Some(k) = self.ring.pop_front() else {
                break;
            };
            let Some(e) = self.map.get(&k) else {
                continue; // stale ring slot (shouldn't happen; be tolerant)
            };
            if spared < CLOCK_SWEEP_BUDGET && e.referenced.swap(false, Ordering::Relaxed) {
                spared += 1;
                self.ring.push_back(k); // second chance
            } else {
                self.unlink(&k);
                evicted += 1;
            }
        }
        evicted
    }

    /// Drop every entry and reset the tallies.
    fn clear(&mut self) {
        self.map.clear();
        self.ring.clear();
        self.used = 0;
        self.vlog_entries = 0;
        self.vlog_used = 0;
    }
}

/// Hit/miss/size counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    /// Klog data-block hits. Deliberately **not** a total: vlog value hits are
    /// reported separately so "block cache hit rate" keeps meaning what it
    /// meant before vlog values shared the cache.
    pub hits: u64,
    /// Klog data-block misses.
    pub misses: u64,
    /// Entries of both domains.
    pub entries: usize,
    /// Bytes held by both domains.
    pub bytes: i64,
    /// Decoded vlog values served from the cache.
    pub vlog_hits: u64,
    /// Vlog lookups that found nothing and had to decode the frame.
    pub vlog_misses: u64,
    /// The vlog share of `entries`.
    pub vlog_entries: usize,
    /// The vlog share of `bytes`.
    pub vlog_bytes: i64,
    /// Entries (either domain) the clock hand evicted to stay under capacity.
    /// Explicit removals and namespace purges are not evictions.
    pub evictions: u64,
}

/// The storage behind one or more [`BlockCache`] views.
struct Core {
    shards: Vec<RwLock<Shard>>,
    mask: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    vlog_hits: AtomicU64,
    vlog_misses: AtomicU64,
    evictions: AtomicU64,
}

/// A sharded CLOCK block cache (see module docs): a namespaced view of shared
/// storage. Counters and capacity belong to the storage, so every view of it
/// reports the same [`stats`](Self::stats).
pub struct BlockCache {
    core: Arc<Core>,
    ns: u64,
    /// Whether a *background* read (see [`admits_current_thread`]) inserts
    /// what it misses. `false` for every view unless the database opts back
    /// in with `Options::admit_background_scan_blocks`.
    ///
    /// [`admits_current_thread`]: BlockCache::admits_current_thread
    admit_background: bool,
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("shards", &self.core.shards.len())
            .field("ns", &self.ns)
            .field("admit_background", &self.admit_background)
            .finish()
    }
}

const NUM_SHARDS: usize = 16;

impl BlockCache {
    /// Create a cache with `capacity_bytes` total capacity.  A capacity of zero
    /// (or less) yields a disabled cache (every `get` misses).
    pub fn new(capacity_bytes: i64) -> BlockCache {
        if capacity_bytes <= 0 {
            return BlockCache::from_core(Core {
                shards: Vec::new(),
                mask: 0,
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                vlog_hits: AtomicU64::new(0),
                vlog_misses: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            });
        }
        let per = (capacity_bytes / NUM_SHARDS as i64).max(1);
        let shards = (0..NUM_SHARDS)
            .map(|_| {
                RwLock::new(Shard {
                    map: HashMap::new(),
                    ring: VecDeque::new(),
                    used: 0,
                    cap: per,
                    vlog_entries: 0,
                    vlog_used: 0,
                })
            })
            .collect();
        BlockCache::from_core(Core {
            shards,
            mask: (NUM_SHARDS - 1) as u64,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            vlog_hits: AtomicU64::new(0),
            vlog_misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    fn from_core(core: Core) -> BlockCache {
        BlockCache {
            core: Arc::new(core),
            ns: 0,
            admit_background: false,
        }
    }

    /// Another view of this cache's storage, keyed under namespace `ns`.
    /// Entries of different namespaces never alias, whatever their file ids.
    pub(crate) fn namespaced(&self, ns: u64) -> BlockCache {
        BlockCache {
            core: Arc::clone(&self.core),
            ns,
            admit_background: self.admit_background,
        }
    }

    /// This view with its background-admission policy set (see
    /// [`admits_current_thread`](Self::admits_current_thread)). The policy is
    /// per view, so two databases leasing one shared storage may differ.
    pub(crate) fn with_background_admission(mut self, admit: bool) -> BlockCache {
        self.admit_background = admit;
        self
    }

    /// Should a read on the calling thread insert what it misses, and refresh
    /// the recency of what it hits?
    ///
    /// Yes for foreground reads — point reads and user iterators, whatever
    /// the policy. For a **background** thread (any [`IoClass`] other than
    /// `Foreground`: compaction and its span workers, the part mover, flush
    /// and ingest validation) only when the view opted in. Such a reader walks
    /// every block of a table exactly once and never asks for it again, so
    /// admitting its blocks can only evict ones a foreground reader does want:
    /// one large compaction used to cycle the whole cache and hand the hot set
    /// back cold (wavesdb `ac16c8a`).
    ///
    /// The class is the thread's [`crate::ioctrl`] tag rather than a flag
    /// threaded through every reader, because it is already set at exactly the
    /// places background work starts, and a reader is shared between the two
    /// kinds of caller through the table cache.
    ///
    /// [`IoClass`]: crate::ioctrl::IoClass
    #[inline]
    pub(crate) fn admits_current_thread(&self) -> bool {
        self.admit_background || crate::ioctrl::current() == crate::ioctrl::IoClass::Foreground
    }

    /// Look up without touching the entry's reference bit or the hit/miss
    /// counters: a background read's lookup.
    ///
    /// A background scan still *reads through* the cache — a block a point
    /// read already paid for is served for free — but its hit must not give
    /// the block a second chance, or what stays resident would reflect the
    /// scan instead of foreground demand. It is not counted either, so
    /// `hits`/`misses` keep describing foreground reads, which is what an
    /// operator sizing the cache is looking at.
    pub(crate) fn peek(&self, file_id: u64, off: u64, domain: BlockDomain) -> Option<Arc<[u8]>> {
        if !self.enabled() {
            return None;
        }
        let k = self.key(file_id, off, domain);
        let s = self.shard_for(&k).read();
        s.map.get(&k).map(|e| e.data.clone())
    }

    /// [`get`](Self::get) for a foreground caller, [`peek`](Self::peek) for a
    /// background one — the lookup half of
    /// [`admits_current_thread`](Self::admits_current_thread). Returns the
    /// decision too, so the caller's insert on a miss follows the same one.
    #[inline]
    pub(crate) fn lookup(
        &self,
        file_id: u64,
        off: u64,
        domain: BlockDomain,
    ) -> (Option<Arc<[u8]>>, bool) {
        if self.admits_current_thread() {
            (self.get(file_id, off, domain), true)
        } else {
            (self.peek(file_id, off, domain), false)
        }
    }

    /// Whether the cache stores anything.
    pub fn enabled(&self) -> bool {
        !self.core.shards.is_empty()
    }

    #[inline]
    fn key(&self, file_id: u64, off: u64, domain: BlockDomain) -> BlockKey {
        BlockKey {
            ns: self.ns,
            file_id,
            off,
            domain,
        }
    }

    fn shard_for(&self, k: &BlockKey) -> &RwLock<Shard> {
        let mut h = k.file_id.wrapping_mul(1099511628211) ^ k.off;
        h = h.wrapping_add(k.domain.salt());
        // Namespace 0 (every private cache) keeps its historical placement.
        h ^= k.ns.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 33;
        &self.core.shards[(h & self.core.mask) as usize]
    }

    /// Look up the bytes cached at `(file_id, domain, off)`. Hits take the
    /// shard lock in read mode only — concurrent readers do not serialize.
    pub fn get(&self, file_id: u64, off: u64, domain: BlockDomain) -> Option<Arc<[u8]>> {
        if !self.enabled() {
            return None;
        }
        let k = self.key(file_id, off, domain);
        let out = {
            let s = self.shard_for(&k).read();
            s.map.get(&k).map(|e| {
                e.referenced.store(true, Ordering::Relaxed);
                e.data.clone()
            })
        };
        let (hit, miss) = match domain {
            BlockDomain::Klog => (&self.core.hits, &self.core.misses),
            BlockDomain::Vlog => (&self.core.vlog_hits, &self.core.vlog_misses),
        };
        match &out {
            Some(_) => hit.fetch_add(1, Ordering::Relaxed),
            None => miss.fetch_add(1, Ordering::Relaxed),
        };
        out
    }

    /// Whether `(file_id, domain, off)` is resident, without counting a hit or
    /// a miss or marking the entry referenced: a planner asking "would this
    /// read go to storage?" is not a read, and must neither skew the hit rate
    /// nor keep an entry alive.
    pub(crate) fn contains(&self, file_id: u64, off: u64, domain: BlockDomain) -> bool {
        if !self.enabled() {
            return false;
        }
        let k = self.key(file_id, off, domain);
        self.shard_for(&k).read().map.contains_key(&k)
    }

    /// Insert a value, evicting not-recently-referenced entries if over
    /// capacity.
    pub fn put(&self, file_id: u64, off: u64, domain: BlockDomain, val: Arc<[u8]>) {
        if !self.enabled() {
            return;
        }
        let k = self.key(file_id, off, domain);
        let mut s = self.shard_for(&k).write();
        if let Some(e) = s.map.get(&k) {
            // Already present: blocks are immutable, so keep the existing
            // value and just mark it referenced.
            e.referenced.store(true, Ordering::Relaxed);
            return;
        }
        let len = val.len() as i64;
        s.used += len;
        if domain == BlockDomain::Vlog {
            s.vlog_entries += 1;
            s.vlog_used += len;
        }
        s.map.insert(
            k,
            CacheEntry {
                data: val,
                // Insert unreferenced: a never-again-touched block is evicted
                // on the hand's first pass (scan resistance).
                referenced: AtomicBool::new(false),
            },
        );
        s.ring.push_back(k);
        if s.used > s.cap {
            let evicted = s.evict_to_cap();
            if evicted > 0 {
                self.core.evictions.fetch_add(evicted, Ordering::Relaxed);
            }
        }
    }

    /// Drop the entry at `(file_id, domain, off)`, if present.
    ///
    /// Only the map entry is unlinked; the ring slot is left for the clock
    /// hand to reap, which it already tolerates (`evict_to_cap` skips a ring
    /// key with no map entry). Unlinking from a `VecDeque` would be a linear
    /// scan under the write lock, and removal is a rare corruption path.
    pub fn remove(&self, file_id: u64, off: u64, domain: BlockDomain) {
        if !self.enabled() {
            return;
        }
        let k = self.key(file_id, off, domain);
        let mut s = self.shard_for(&k).write();
        s.unlink(&k);
    }

    /// Drop every entry of this view's namespace.
    ///
    /// Only map entries are unlinked; their ring slots are reaped by the clock
    /// hand, exactly as for [`remove`](Self::remove).
    pub(crate) fn purge_namespace(&self, ns: u64) {
        for shard in &self.core.shards {
            let mut s = shard.write();
            let doomed: Vec<BlockKey> = s.map.keys().filter(|k| k.ns == ns).copied().collect();
            for k in &doomed {
                s.unlink(k);
            }
        }
    }

    /// Drop every entry of every namespace sharing this storage.
    pub(crate) fn clear(&self) {
        for shard in &self.core.shards {
            shard.write().clear();
        }
    }

    /// Aggregate hit/miss counters and approximate size — of the whole
    /// storage, every namespace included.
    pub fn stats(&self) -> CacheStats {
        let mut entries = 0;
        let mut bytes = 0;
        let mut vlog_entries = 0;
        let mut vlog_bytes = 0;
        for shard in &self.core.shards {
            let s = shard.read();
            entries += s.map.len();
            bytes += s.used;
            vlog_entries += s.vlog_entries;
            vlog_bytes += s.vlog_used;
        }
        CacheStats {
            hits: self.core.hits.load(Ordering::Relaxed),
            misses: self.core.misses.load(Ordering::Relaxed),
            entries,
            bytes,
            vlog_hits: self.core.vlog_hits.load(Ordering::Relaxed),
            vlog_misses: self.core.vlog_misses.load(Ordering::Relaxed),
            vlog_entries,
            vlog_bytes,
            evictions: self.core.evictions.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blk(n: usize, byte: u8) -> Arc<[u8]> {
        vec![byte; n].into()
    }

    #[test]
    fn disabled_cache_always_misses() {
        let c = BlockCache::new(0);
        assert!(!c.enabled());
        c.put(1, 0, BlockDomain::Klog, blk(10, 1));
        assert!(c.get(1, 0, BlockDomain::Klog).is_none());
    }

    #[test]
    fn get_after_put() {
        let c = BlockCache::new(1 << 20);
        c.put(1, 4096, BlockDomain::Klog, blk(100, 7));
        let v = c.get(1, 4096, BlockDomain::Klog).expect("hit");
        assert_eq!(v.len(), 100);
        assert_eq!(v[0], 7);
        assert!(c.get(2, 0, BlockDomain::Klog).is_none());
        let st = c.stats();
        assert_eq!(st.hits, 1);
        assert_eq!(st.misses, 1);
    }

    #[test]
    fn evicts_over_capacity() {
        // Small cap so most inserts get evicted.
        let c = BlockCache::new(NUM_SHARDS as i64 * 256);
        for i in 0..1000u64 {
            c.put(1, i * 4096, BlockDomain::Klog, blk(200, i as u8));
        }
        let st = c.stats();
        // Each shard holds <= ~ cap/200 entries; far fewer than 1000.
        assert!(st.entries < 1000, "entries={}", st.entries);
        assert!(st.bytes <= NUM_SHARDS as i64 * 256 + 200);
    }

    #[test]
    fn referenced_entries_survive_eviction_pressure() {
        // Hot key is touched between inserts, cold keys are not; under
        // pressure the hot key must survive (second chance).
        let c = BlockCache::new(NUM_SHARDS as i64 * 1024);
        c.put(1, 0, BlockDomain::Klog, blk(200, 1)); // hot
        for i in 1..200u64 {
            let _ = c.get(1, 0, BlockDomain::Klog); // keep the reference bit set
            c.put(1, i * 4096, BlockDomain::Klog, blk(200, i as u8)); // cold churn
        }
        assert!(
            c.get(1, 0, BlockDomain::Klog).is_some(),
            "hot block was evicted"
        );
    }

    #[test]
    fn domains_do_not_alias() {
        // klog and vlog share a file_id with independent offset spaces, so
        // (7, 0) names both a data block and a vlog frame. The domain tag is
        // what keeps them apart.
        let c = BlockCache::new(1 << 20);
        c.put(7, 0, BlockDomain::Klog, blk(64, 0xAA));
        c.put(7, 0, BlockDomain::Vlog, blk(32, 0xBB));

        let k = c.get(7, 0, BlockDomain::Klog).expect("klog entry");
        let v = c.get(7, 0, BlockDomain::Vlog).expect("vlog entry");
        assert_eq!(k.len(), 64);
        assert_eq!(k[0], 0xAA);
        assert_eq!(v.len(), 32);
        assert_eq!(v[0], 0xBB);
        let st = c.stats();
        assert_eq!(st.entries, 2, "neither insert evicted the other");
        assert_eq!(st.vlog_entries, 1, "only the vlog insert is a vlog entry");
        assert_eq!(st.vlog_bytes, 32);
        assert_eq!(st.hits, 1, "klog hits stay klog-only");
        assert_eq!(st.vlog_hits, 1);
    }

    #[test]
    fn shard_for_separates_domains() {
        // The two domains must not land in lockstep: if the domain were not
        // mixed into the hash, every Klog/Vlog pair would share a shard and
        // vlog admission would evict exactly the klog blocks it aliases.
        let c = BlockCache::new(1 << 20);
        let mut same = 0usize;
        let total = 512usize;
        for i in 0..total as u64 {
            let (file_id, off) = (i / 8 + 1, (i % 8) * 4096);
            let kk = BlockKey {
                ns: 0,
                file_id,
                off,
                domain: BlockDomain::Klog,
            };
            let vk = BlockKey {
                ns: 0,
                file_id,
                off,
                domain: BlockDomain::Vlog,
            };
            if std::ptr::eq(c.shard_for(&kk), c.shard_for(&vk)) {
                same += 1;
            }
        }
        // Independent placement collides ~1/NUM_SHARDS of the time; a hash
        // that ignored the domain would collide every time. Assert only a
        // non-degenerate spread, not a distribution.
        assert!(
            same < total / 2,
            "domains land in the same shard {same}/{total} times"
        );
        // Both domains must still spread over the shards — a domain pinned to
        // one shard would separate the two at the cost of its own capacity.
        for domain in [BlockDomain::Klog, BlockDomain::Vlog] {
            let mut seen = std::collections::HashSet::new();
            for i in 0..total as u64 {
                let k = BlockKey {
                    ns: 0,
                    file_id: i / 8 + 1,
                    off: (i % 8) * 4096,
                    domain,
                };
                seen.insert(self_shard_index(&c, &k));
            }
            assert!(
                seen.len() > NUM_SHARDS / 2,
                "{domain:?} used only {} of {NUM_SHARDS} shards",
                seen.len()
            );
        }
    }

    /// The shard index `shard_for` picked, by pointer identity.
    fn self_shard_index(c: &BlockCache, k: &BlockKey) -> usize {
        let target = c.shard_for(k) as *const _;
        c.core
            .shards
            .iter()
            .position(|s| std::ptr::eq(s, target))
            .expect("shard_for returns one of our shards")
    }

    #[test]
    fn remove_drops_entry_and_survives_sweep() {
        let c = BlockCache::new(NUM_SHARDS as i64 * 1024);
        c.put(3, 0, BlockDomain::Vlog, blk(200, 9));
        assert!(c.get(3, 0, BlockDomain::Vlog).is_some());
        c.remove(3, 0, BlockDomain::Vlog);
        assert!(c.get(3, 0, BlockDomain::Vlog).is_none());
        // The ring still holds the removed key; the sweep must tolerate it.
        for i in 0..500u64 {
            c.put(3, (i + 1) * 4096, BlockDomain::Vlog, blk(200, i as u8));
        }
        let st = c.stats();
        assert!(st.bytes >= 0, "used went negative: {}", st.bytes);
        assert_eq!(
            st.vlog_entries, st.entries,
            "every surviving entry is a vlog entry here"
        );
        assert_eq!(st.vlog_bytes, st.bytes, "per-domain byte tally drifted");
        assert!(
            st.bytes <= NUM_SHARDS as i64 * 1024 + 200,
            "bytes={}",
            st.bytes
        );
        // Removing an absent key is a no-op, not an accounting error.
        c.remove(3, 1 << 40, BlockDomain::Klog);
        assert_eq!(c.stats().bytes, st.bytes);
    }

    #[test]
    fn concurrent_readers_and_writers() {
        use std::sync::Arc as StdArc;
        let c = StdArc::new(BlockCache::new(1 << 20));
        for i in 0..64u64 {
            c.put(1, i * 4096, BlockDomain::Klog, blk(256, i as u8));
        }
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let c = c.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..20_000u64 {
                    let k = (i * 31 + t) % 64;
                    if let Some(v) = c.get(1, k * 4096, BlockDomain::Klog) {
                        assert_eq!(v[0], k as u8);
                    }
                    if i % 512 == 0 {
                        c.put(
                            2,
                            (t * 100_000 + i) * 4096,
                            BlockDomain::Klog,
                            blk(256, t as u8),
                        );
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn namespaces_never_alias_and_purge_alone() {
        let a = BlockCache::new(1 << 20);
        let b = a.namespaced(7);
        a.put(1, 0, BlockDomain::Klog, blk(10, 1));
        b.put(1, 0, BlockDomain::Klog, blk(10, 2));
        assert_eq!(a.get(1, 0, BlockDomain::Klog).unwrap()[0], 1);
        assert_eq!(b.get(1, 0, BlockDomain::Klog).unwrap()[0], 2);
        assert_eq!(a.stats().entries, 2, "one storage, two namespaces");
        a.purge_namespace(7);
        assert!(b.get(1, 0, BlockDomain::Klog).is_none());
        assert_eq!(a.get(1, 0, BlockDomain::Klog).unwrap()[0], 1);
        a.clear();
        assert_eq!(a.stats().entries, 0);
        assert_eq!(a.stats().bytes, 0);
    }

    #[test]
    fn evictions_are_counted() {
        let c = BlockCache::new(NUM_SHARDS as i64 * 1024);
        for i in 0..200u64 {
            c.put(i, 0, BlockDomain::Klog, blk(512, 1));
        }
        let st = c.stats();
        assert!(st.evictions > 0);
        assert_eq!(st.evictions + st.entries as u64, 200);
    }

    /// A peek finds the entry but neither counts nor sets the reference bit,
    /// so the next sweep evicts an entry only a background scan touched.
    #[test]
    fn peek_neither_counts_nor_refreshes() {
        let c = BlockCache::new(NUM_SHARDS as i64 * 1024);
        c.put(1, 0, BlockDomain::Klog, blk(600, 1));
        assert!(c.peek(1, 0, BlockDomain::Klog).is_some());
        assert!(c.peek(2, 0, BlockDomain::Klog).is_none());
        let st = c.stats();
        assert_eq!((st.hits, st.misses), (0, 0), "a peek is not a foreground access");
        let k = c.key(1, 0, BlockDomain::Klog);
        let shard = c.shard_for(&k).read();
        assert!(
            !shard.map[&k].referenced.load(Ordering::Relaxed),
            "a peek must not give the entry a second chance"
        );
    }

    /// The lookup policy follows the thread's IO class, and the opt-in view
    /// admits everywhere.
    #[test]
    fn lookup_admits_only_foreground_unless_opted_in() {
        use crate::ioctrl::{scoped, IoClass};
        let c = BlockCache::new(1 << 20);
        c.put(1, 0, BlockDomain::Klog, blk(10, 1));
        assert!(c.admits_current_thread(), "test threads are foreground");
        {
            let _bg = scoped(IoClass::Compaction);
            assert!(!c.admits_current_thread());
            let (hit, admit) = c.lookup(1, 0, BlockDomain::Klog);
            assert!(hit.is_some() && !admit);
            let opted = c.namespaced(0).with_background_admission(true);
            assert!(opted.admits_current_thread());
            assert!(!c.namespaced(0).admits_current_thread(), "views inherit the policy");
        }
        let (hit, admit) = c.lookup(1, 0, BlockDomain::Klog);
        assert!(hit.is_some() && admit);
        assert_eq!(c.stats().hits, 1, "only the foreground lookup counted");
    }
}
