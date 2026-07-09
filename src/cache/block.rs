//! Sharded, byte-bounded CLOCK (second-chance) cache of decompressed SSTable
//! blocks keyed by `(file_id, offset)`.  Cached values are immutable
//! (`Arc<[u8]>`); callers must not mutate them.
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct BlockKey {
    file_id: u64,
    off: u64,
}

struct CacheEntry {
    data: Arc<[u8]>,
    /// CLOCK reference bit: set (Relaxed) on every hit, cleared by the sweep
    /// hand. Relaxed is enough — it only biases eviction order.
    referenced: AtomicBool,
}

struct Shard {
    map: HashMap<BlockKey, CacheEntry>,
    /// Clock ring in insertion order. Every map entry is in the ring exactly
    /// once; entries leave both together during a sweep.
    ring: VecDeque<BlockKey>,
    used: i64,
    cap: i64,
}

impl Shard {
    /// Evict with the clock hand until under capacity: pop the ring front;
    /// a referenced entry is cleared and pushed to the back (second chance),
    /// an unreferenced one is evicted. Bounded to two full revolutions so a
    /// pathological state cannot spin forever.
    fn evict_to_cap(&mut self) {
        let mut budget = self.ring.len().saturating_mul(2);
        while self.used > self.cap && self.map.len() > 1 && budget > 0 {
            budget -= 1;
            let Some(k) = self.ring.pop_front() else {
                break;
            };
            let Some(e) = self.map.get(&k) else {
                continue; // stale ring slot (shouldn't happen; be tolerant)
            };
            if e.referenced.swap(false, Ordering::Relaxed) {
                self.ring.push_back(k); // second chance
            } else if let Some(e) = self.map.remove(&k) {
                self.used -= e.data.len() as i64;
            }
        }
    }
}

/// Hit/miss/size counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub bytes: i64,
}

/// A sharded CLOCK block cache (see module docs).
pub struct BlockCache {
    shards: Vec<RwLock<Shard>>,
    mask: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("shards", &self.shards.len())
            .finish()
    }
}

const NUM_SHARDS: usize = 16;

impl BlockCache {
    /// Create a cache with `capacity_bytes` total capacity.  A capacity of zero
    /// (or less) yields a disabled cache (every `get` misses).
    pub fn new(capacity_bytes: i64) -> BlockCache {
        if capacity_bytes <= 0 {
            return BlockCache {
                shards: Vec::new(),
                mask: 0,
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            };
        }
        let per = (capacity_bytes / NUM_SHARDS as i64).max(1);
        let shards = (0..NUM_SHARDS)
            .map(|_| {
                RwLock::new(Shard {
                    map: HashMap::new(),
                    ring: VecDeque::new(),
                    used: 0,
                    cap: per,
                })
            })
            .collect();
        BlockCache {
            shards,
            mask: (NUM_SHARDS - 1) as u64,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Whether the cache stores anything.
    pub fn enabled(&self) -> bool {
        !self.shards.is_empty()
    }

    fn shard_for(&self, k: &BlockKey) -> &RwLock<Shard> {
        let mut h = k.file_id.wrapping_mul(1099511628211) ^ k.off;
        h ^= h >> 33;
        &self.shards[(h & self.mask) as usize]
    }

    /// Look up the block cached at `(file_id, off)`. Hits take the shard lock
    /// in read mode only — concurrent readers do not serialize.
    pub fn get(&self, file_id: u64, off: u64) -> Option<Arc<[u8]>> {
        if !self.enabled() {
            return None;
        }
        let k = BlockKey { file_id, off };
        let out = {
            let s = self.shard_for(&k).read();
            s.map.get(&k).map(|e| {
                e.referenced.store(true, Ordering::Relaxed);
                e.data.clone()
            })
        };
        match &out {
            Some(_) => self.hits.fetch_add(1, Ordering::Relaxed),
            None => self.misses.fetch_add(1, Ordering::Relaxed),
        };
        out
    }

    /// Insert a block, evicting not-recently-referenced blocks if over
    /// capacity.
    pub fn put(&self, file_id: u64, off: u64, val: Arc<[u8]>) {
        if !self.enabled() {
            return;
        }
        let k = BlockKey { file_id, off };
        let mut s = self.shard_for(&k).write();
        if let Some(e) = s.map.get(&k) {
            // Already present: blocks are immutable, so keep the existing
            // value and just mark it referenced.
            e.referenced.store(true, Ordering::Relaxed);
            return;
        }
        s.used += val.len() as i64;
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
            s.evict_to_cap();
        }
    }

    /// Aggregate hit/miss counters and approximate size.
    pub fn stats(&self) -> CacheStats {
        let mut entries = 0;
        let mut bytes = 0;
        for shard in &self.shards {
            let s = shard.read();
            entries += s.map.len();
            bytes += s.used;
        }
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries,
            bytes,
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
        c.put(1, 0, blk(10, 1));
        assert!(c.get(1, 0).is_none());
    }

    #[test]
    fn get_after_put() {
        let c = BlockCache::new(1 << 20);
        c.put(1, 4096, blk(100, 7));
        let v = c.get(1, 4096).expect("hit");
        assert_eq!(v.len(), 100);
        assert_eq!(v[0], 7);
        assert!(c.get(2, 0).is_none());
        let st = c.stats();
        assert_eq!(st.hits, 1);
        assert_eq!(st.misses, 1);
    }

    #[test]
    fn evicts_over_capacity() {
        // Small cap so most inserts get evicted.
        let c = BlockCache::new(NUM_SHARDS as i64 * 256);
        for i in 0..1000u64 {
            c.put(1, i * 4096, blk(200, i as u8));
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
        c.put(1, 0, blk(200, 1)); // hot
        for i in 1..200u64 {
            let _ = c.get(1, 0); // keep the reference bit set
            c.put(1, i * 4096, blk(200, i as u8)); // cold churn
        }
        assert!(c.get(1, 0).is_some(), "hot block was evicted");
    }

    #[test]
    fn concurrent_readers_and_writers() {
        use std::sync::Arc as StdArc;
        let c = StdArc::new(BlockCache::new(1 << 20));
        for i in 0..64u64 {
            c.put(1, i * 4096, blk(256, i as u8));
        }
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let c = c.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..20_000u64 {
                    let k = (i * 31 + t) % 64;
                    if let Some(v) = c.get(1, k * 4096) {
                        assert_eq!(v[0], k as u8);
                    }
                    if i % 512 == 0 {
                        c.put(2, (t * 100_000 + i) * 4096, blk(256, t as u8));
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
