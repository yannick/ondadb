//! Sharded, byte-bounded LRU cache of decompressed SSTable blocks keyed by
//! `(file_id, offset)`.  Cached values are immutable (`Arc<[u8]>`); callers must
//! not mutate them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct BlockKey {
    file_id: u64,
    off: u64,
}

struct Shard {
    lru: LruCache<BlockKey, Arc<[u8]>>,
    used: i64,
    cap: i64,
}

/// Hit/miss/size counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub bytes: i64,
}

/// A sharded LRU block cache.
pub struct BlockCache {
    shards: Vec<Mutex<Shard>>,
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
                Mutex::new(Shard {
                    lru: LruCache::unbounded(),
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

    fn shard_for(&self, k: &BlockKey) -> &Mutex<Shard> {
        let mut h = k.file_id.wrapping_mul(1099511628211) ^ k.off;
        h ^= h >> 33;
        &self.shards[(h & self.mask) as usize]
    }

    /// Look up the block cached at `(file_id, off)`.
    pub fn get(&self, file_id: u64, off: u64) -> Option<Arc<[u8]>> {
        if !self.enabled() {
            return None;
        }
        let k = BlockKey { file_id, off };
        let mut s = self.shard_for(&k).lock();
        if let Some(v) = s.lru.get(&k) {
            let v = v.clone();
            drop(s);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(v)
        } else {
            drop(s);
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a block, evicting least-recently-used blocks if over capacity.
    pub fn put(&self, file_id: u64, off: u64, val: Arc<[u8]>) {
        if !self.enabled() {
            return;
        }
        let k = BlockKey { file_id, off };
        let mut s = self.shard_for(&k).lock();
        if s.lru.get(&k).is_some() {
            return; // already present (and now MRU)
        }
        let added = val.len() as i64;
        if let Some(old) = s.lru.put(k, val) {
            s.used -= old.len() as i64;
        }
        s.used += added;
        while s.used > s.cap && s.lru.len() > 1 {
            if let Some((_, v)) = s.lru.pop_lru() {
                s.used -= v.len() as i64;
            } else {
                break;
            }
        }
    }

    /// Aggregate hit/miss counters and approximate size.
    pub fn stats(&self) -> CacheStats {
        let mut entries = 0;
        let mut bytes = 0;
        for shard in &self.shards {
            let s = shard.lock();
            entries += s.lru.len();
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
}
