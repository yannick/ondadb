//! A bounded cache of open SSTable readers — the `max_open_files` equivalent.
//!
//! # Why this exists
//!
//! Opening an SSTable eagerly loads its **block index** (one entry per data
//! block, each with a heap-allocated key) and its **bloom filter**, and both
//! stay resident for as long as the reader does. Before this cache existed,
//! `ColumnFamily::open` opened *every* table named in the manifest and never
//! closed one, so resident memory was proportional to **total stored bytes**
//! rather than to the working set — and it was paid at startup, whether or not
//! a table was ever read.
//!
//! Measured on a real 48 GiB store of 14,051 tables: 6.6 GB resident 12 s into
//! startup, 12 GB at 25 s and still opening. See spada `decisions.md` S-122.
//!
//! RocksDB has the same eager per-table load and is safe because of a bound
//! this engine did not have:
//!
//! > *"If `cache_index_and_filter_blocks` is false (which is default), the
//! > number of index/filter blocks is controlled by option `max_open_files`."*
//! > — [Memory usage in RocksDB](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB)
//!
//! That is exactly this: a bound on how many readers may be open at once, with
//! least-recently-used closure. It bounds memory **by count, independent of
//! store size**, which is the property neither a larger block size nor a
//! partitioned index gives.
//!
//! # Why closing a reader is safe
//!
//! A reader is a pure, re-derivable view of an immutable file. Closing one
//! costs a re-open — a footer read plus an index and bloom decode — and cannot
//! change an answer. An in-flight caller holds an `Arc<Reader>`, so eviction
//! only drops the cache's reference; the reader lives until its last user is
//! done. Memory is therefore bounded by `max_open + concurrent in-flight
//! readers`, not by `max_open` alone, and that is the honest statement of it.
//!
//! # What this deliberately does not do
//!
//! It does not partition the index. RocksDB's
//! [partitioned index/filters](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)
//! exist for ~256 MiB tables whose monolithic index is megabytes; this engine's
//! tables average a few MiB, so at a 16 KiB block size an index is tens of
//! kilobytes and partitioning it would buy nothing. The problem here is table
//! **count**, not per-table index size.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::cache::BlockCache;
use crate::comparator::ComparatorRef;
use crate::error::Result;
use crate::sst::Reader;
use crate::storage::Storage;

/// Default bound on simultaneously open SSTable readers.
///
/// 512 readers of a few MiB each is tens of megabytes of index and bloom —
/// comfortably inside a low-hundreds-of-MB budget — while being far more than
/// any single query's fan-out, so a warm workload rarely evicts. Raise it to
/// trade memory for fewer re-opens; there is no "unlimited" setting, because
/// unlimited is what this exists to remove.
pub const DEFAULT_MAX_OPEN_READERS: usize = 512;

/// Everything needed to (re)open one table's reader.
///
/// Held by the `SstHandle` instead of the reader itself, so the handle stays
/// cheap and the reader becomes a cache lookup.
#[derive(Debug)]
pub(crate) struct TableRef {
    pub klog: String,
    pub storage: Arc<dyn Storage>,
    pub bc: Arc<BlockCache>,
    pub file_id: u64,
    pub cmp: ComparatorRef,
}

struct Inner {
    /// file_id → (reader, last-used tick).
    open: HashMap<u64, (Arc<Reader>, u64)>,
    tick: u64,
}

/// A bounded, least-recently-used cache of open readers.
pub struct TableCache {
    inner: Mutex<Inner>,
    max_open: AtomicUsize,
    opens: AtomicU64,
    hits: AtomicU64,
    closes: AtomicU64,
}

impl std::fmt::Debug for TableCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (open, _, _, _) = self.stats();
        f.debug_struct("TableCache")
            .field("open", &open)
            .field("max_open", &self.max_open.load(Ordering::Relaxed))
            .finish()
    }
}

impl TableCache {
    pub fn new(max_open: usize) -> TableCache {
        TableCache {
            inner: Mutex::new(Inner {
                open: HashMap::new(),
                tick: 0,
            }),
            // Zero would mean "cache nothing", which turns every access into an
            // open; one is the smallest value that still makes progress.
            max_open: AtomicUsize::new(max_open.max(1)),
            opens: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            closes: AtomicU64::new(0),
        }
    }

    /// `(open readers, opens, hits, closes)`.
    pub fn stats(&self) -> (usize, u64, u64, u64) {
        let open = self.inner.lock().map(|i| i.open.len()).unwrap_or(0);
        (
            open,
            self.opens.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
            self.closes.load(Ordering::Relaxed),
        )
    }

    pub fn set_max_open(&self, max_open: usize) {
        self.max_open.store(max_open.max(1), Ordering::Relaxed);
        if let Ok(mut inner) = self.inner.lock() {
            self.evict(&mut inner);
        }
    }

    /// The reader for `t`, opening it if it is not resident.
    ///
    /// The open happens **outside** the lock: it is file I/O plus an index and
    /// bloom decode, and holding a global mutex across it would serialize every
    /// column family's cold reads behind one another. The cost is that two
    /// threads racing on the same cold table may both open it; one insert wins
    /// and the loser's reader is simply dropped, which is cheaper than the
    /// convoy.
    pub(crate) fn get(&self, t: &TableRef) -> Result<Arc<Reader>> {
        {
            let mut inner = self.inner.lock().expect("table cache poisoned");
            inner.tick += 1;
            let tick = inner.tick;
            if let Some(slot) = inner.open.get_mut(&t.file_id) {
                slot.1 = tick;
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::clone(&slot.0));
            }
        }

        // `Reader::open` already yields an `Arc`.
        let reader = Reader::open(
            &t.klog,
            Arc::clone(&t.storage),
            Arc::clone(&t.bc),
            t.file_id,
            t.cmp.clone(),
        )?;
        self.opens.fetch_add(1, Ordering::Relaxed);

        let mut inner = self.inner.lock().expect("table cache poisoned");
        inner.tick += 1;
        let tick = inner.tick;
        // A racing thread may have inserted first; prefer the resident one so
        // both callers share a single decode.
        let out = match inner.open.get_mut(&t.file_id) {
            Some(slot) => {
                slot.1 = tick;
                Arc::clone(&slot.0)
            }
            None => {
                inner.open.insert(t.file_id, (Arc::clone(&reader), tick));
                Arc::clone(&reader)
            }
        };
        self.evict(&mut inner);
        Ok(out)
    }

    /// Drop `file_id` from the cache, returning its reader if it was open.
    ///
    /// The caller closes what comes back. A table that was never opened, or has
    /// already been evicted, needs no close — which is the point of returning
    /// an `Option` rather than opening one in order to close it.
    pub(crate) fn close(&self, file_id: u64) -> Option<Arc<Reader>> {
        let mut inner = self.inner.lock().ok()?;
        inner.open.remove(&file_id).map(|(r, _)| r)
    }

    /// `(resident, index, bloom, open readers, index entries)` across the
    /// readers **currently open**.
    ///
    /// Only open readers count, and that is the honest measure: a closed table
    /// holds nothing. Opening every table to total up its index would be the
    /// bug this cache exists to fix, performed by the instrument meant to
    /// detect it.
    pub fn resident_breakdown(&self) -> (usize, usize, usize, usize, usize) {
        let Ok(inner) = self.inner.lock() else {
            return (0, 0, 0, 0, 0);
        };
        let mut out = (0usize, 0usize, 0usize, 0usize, 0usize);
        for (r, _) in inner.open.values() {
            let (idx, bloom, entries) = r.resident_breakdown();
            out.0 += idx + bloom;
            out.1 += idx;
            out.2 += bloom;
            out.3 += 1;
            out.4 += entries;
        }
        out
    }

    fn evict(&self, inner: &mut Inner) {
        let max = self.max_open.load(Ordering::Relaxed);
        while inner.open.len() > max {
            let Some(&victim) = inner
                .open
                .iter()
                .min_by_key(|(_, (_, tick))| *tick)
                .map(|(id, _)| id)
            else {
                break;
            };
            inner.open.remove(&victim);
            self.closes.fetch_add(1, Ordering::Relaxed);
        }
    }
}
