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
//! # Why a count is not enough
//!
//! A count bound assumes readers cost roughly the same. They do not. Per-reader
//! resident cost is proportional to the table's key count, and measured on
//! spada's staging cluster (2026-08-09) it varied about **30x** with segment
//! size alone: at 256-document segments a reader cost well under a megabyte; at
//! 8192-document segments about 20 MB. The count bound did not change, so the
//! memory it implied moved from a few hundred megabytes to roughly 10 GiB
//! (4096 readers x 20 MB) and repeatedly OOMed nodes. The operator had set a
//! number of readers; what they got was a wall whose height was decided by an
//! unrelated indexing parameter.
//!
//! So the cache carries a **byte budget** as well, and evicts until *both*
//! bounds hold. Bytes are the constraint an operator actually has; count is
//! kept because it still bounds file descriptors and per-reader fixed costs.
//! Either bound may be disabled independently — see [`TableCache::new`] and
//! [`TableCache::with_byte_budget`].
//!
//! # Why closing a reader is safe
//!
//! A reader is a pure, re-derivable view of an immutable file. Closing one
//! costs a re-open — a footer read plus an index and bloom decode — and cannot
//! change an answer. An in-flight caller holds an `Arc<Reader>`, so eviction
//! only drops the cache's reference; the reader lives until its last user is
//! done. Memory is therefore bounded by `max_open + concurrent in-flight
//! readers` — and, in bytes, by `max_open_bytes + the bytes of concurrent
//! in-flight readers`, not by the budget alone. That is the honest statement of
//! it: the budget bounds what the cache **pins**, not the process's peak.
//!
//! # What this deliberately does not do
//!
//! It does not partition the index. RocksDB's
//! [partitioned index/filters](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)
//! exist for ~256 MiB tables whose monolithic index is megabytes; this engine's
//! tables average a few MiB, so at the default 4 KiB block size an index is
//! still modest compared with those tables. Index partitioning would buy
//! little here. The problem is table
//! **count**, not per-table index size.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

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

/// Default byte budget for the resident cost of open readers: 1 GiB.
///
/// Chosen so the default is *bounded* rather than surprising. One GiB holds
/// roughly 50 large readers (~20 MB each, spada's 8192-document segments) or
/// about a thousand small ones (~1 MB, its 256-document segments) — so the
/// count bound stays the binding one for small tables, where it is a sensible
/// proxy, and the byte budget takes over exactly where the count bound stops
/// meaning anything. It is deliberately generous: it is a ceiling that stops a
/// node dying, not a working-set target.
///
/// Zero means "no byte bound", which is what the cache did before this existed.
pub const DEFAULT_MAX_OPEN_READER_BYTES: usize = 1 << 30;

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
    /// The owning family's `max_cached_vlog_value_bytes`. Carried on the ref
    /// because the table cache opens readers lazily, long after the family
    /// that named the table is out of scope.
    pub vlog_cache_limit: usize,
}

/// One cached reader with its second-chance bit.
struct Entry {
    reader: Arc<Reader>,
    /// Set on every hit, cleared by the eviction hand. A relaxed store is
    /// enough: the bit is a heuristic about recency, not a synchronization
    /// edge, and the reader itself is protected by the shard lock.
    referenced: AtomicBool,
    /// Resident cost of this reader, measured once at insert.
    ///
    /// It is fixed for the reader's lifetime — the index and bloom are decoded
    /// at open and never grow — so measuring once is exact, not an estimate,
    /// and it keeps the O(index entries) walk off the eviction path.
    bytes: usize,
}

/// The cache's view of one reader's resident cost: block index plus bloom.
///
/// Deliberately the same two terms [`TableCache::resident_breakdown`] reports,
/// so an operator comparing the budget against the breakdown sees numbers that
/// agree instead of two accountings that nearly do.
fn reader_bytes(reader: &Reader) -> usize {
    let (index, bloom, _entries) = reader.resident_breakdown();
    index + bloom
}

struct Shard {
    open: HashMap<u64, Entry>,
}

/// How many independently locked shards the cache is split into.
///
/// The previous shape was one process-global `Mutex` whose hit path also
/// WROTE a recency tick — exclusive even on reads, shared by every column
/// family. Measured on an 8-core box: point-read throughput through the
/// engine was flat from 1 to 8 threads with SSTs present, and scaled 4.6x
/// with none — the difference was this lock. Sixteen shards with read-locked
/// hits is the same recipe the block cache already uses.
const SHARDS: usize = 16;

/// A bounded cache of open readers: sharded, second-chance (CLOCK) evicted.
///
/// A hit takes one shard **read** lock and one relaxed bit store. Opens and
/// evictions take the shard's write lock; the open itself (file I/O, index
/// and bloom decode) still happens outside any lock.
pub struct TableCache {
    shards: Vec<RwLock<Shard>>,
    /// Total open readers across shards — the bound is GLOBAL and exact
    /// (it is a memory contract, S-123), even though storage is sharded.
    open_count: AtomicUsize,
    max_open: AtomicUsize,
    /// Summed [`Entry::bytes`] of every cached reader. Maintained on insert and
    /// removal so the eviction loop reads one atomic instead of walking indexes.
    open_bytes: AtomicUsize,
    /// Byte budget; `0` disables the byte bound (S-161).
    max_bytes: AtomicUsize,
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
            .field("open_bytes", &self.open_bytes.load(Ordering::Relaxed))
            .field("max_bytes", &self.max_bytes.load(Ordering::Relaxed))
            .finish()
    }
}

impl TableCache {
    /// A cache bounded by reader **count** only.
    ///
    /// Kept byte-unbounded so its meaning has not changed; use
    /// [`TableCache::with_byte_budget`] to add the memory bound, which is what
    /// [`Options`](crate::Options) does by default.
    pub fn new(max_open: usize) -> TableCache {
        TableCache::with_byte_budget(max_open, 0)
    }

    /// A cache bounded by both reader count and total resident bytes.
    ///
    /// `max_bytes == 0` disables the byte bound; `max_open` has no "unlimited"
    /// value (it is clamped to at least one). Eviction runs until **both**
    /// bounds hold.
    pub fn with_byte_budget(max_open: usize, max_bytes: usize) -> TableCache {
        TableCache {
            shards: (0..SHARDS)
                .map(|_| {
                    RwLock::new(Shard {
                        open: HashMap::new(),
                    })
                })
                .collect(),
            open_count: AtomicUsize::new(0),
            // Zero would mean "cache nothing", which turns every access into an
            // open; one is the smallest value that still makes progress.
            max_open: AtomicUsize::new(max_open.max(1)),
            open_bytes: AtomicUsize::new(0),
            max_bytes: AtomicUsize::new(max_bytes),
            opens: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            closes: AtomicU64::new(0),
        }
    }

    fn shard(&self, file_id: u64) -> &RwLock<Shard> {
        // file_ids are sequential, so modulo spreads them evenly.
        &self.shards[(file_id as usize) % SHARDS]
    }

    /// `(open readers, opens, hits, closes)`.
    pub fn stats(&self) -> (usize, u64, u64, u64) {
        let open = self.open_count.load(Ordering::Relaxed);
        (
            open,
            self.opens.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
            self.closes.load(Ordering::Relaxed),
        )
    }

    /// `(bytes held by cached readers, byte budget)`; a budget of `0` means the
    /// byte bound is off.
    ///
    /// The occupancy is the running sum maintained on insert and eviction, so
    /// this is O(1) — unlike [`resident_breakdown`](Self::resident_breakdown),
    /// which recomputes it by walking every index. The two agree by
    /// construction; they are measured the same way.
    pub fn byte_stats(&self) -> (usize, usize) {
        (
            self.open_bytes.load(Ordering::Relaxed),
            self.max_bytes.load(Ordering::Relaxed),
        )
    }

    pub fn set_max_open(&self, max_open: usize) {
        self.max_open.store(max_open.max(1), Ordering::Relaxed);
        self.evict_to_bound(0);
    }

    /// Change the byte budget at runtime; `0` turns the byte bound off.
    ///
    /// Lowering it evicts on the spot rather than waiting for the next insert —
    /// a memory limit that takes hold only on the next read is not a limit
    /// during the incident you set it in.
    pub fn set_max_bytes(&self, max_bytes: usize) {
        self.max_bytes.store(max_bytes, Ordering::Relaxed);
        self.evict_to_bound(0);
    }

    /// The reader for `t`, opening it if it is not resident.
    ///
    /// The open happens **outside** the lock: it is file I/O plus an index and
    /// bloom decode, and holding a lock across it would serialize every
    /// cold read on this shard behind one another. The cost is that two
    /// threads racing on the same cold table may both open it; one insert wins
    /// and the loser's reader is simply dropped, which is cheaper than the
    /// convoy.
    pub(crate) fn get(&self, t: &TableRef) -> Result<Arc<Reader>> {
        {
            let shard = self.shard(t.file_id).read();
            if let Some(e) = shard.open.get(&t.file_id) {
                e.referenced.store(true, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::clone(&e.reader));
            }
        }

        // `Reader::open` already yields an `Arc`.
        let reader = Reader::open(
            &t.klog,
            Arc::clone(&t.storage),
            Arc::clone(&t.bc),
            t.file_id,
            t.cmp.clone(),
            t.vlog_cache_limit,
        )?;
        self.opens.fetch_add(1, Ordering::Relaxed);

        let mut shard = self.shard(t.file_id).write();
        // A racing thread may have inserted first; prefer the resident one so
        // both callers share a single decode.
        let out = match shard.open.get(&t.file_id) {
            Some(e) => {
                e.referenced.store(true, Ordering::Relaxed);
                Arc::clone(&e.reader)
            }
            None => {
                // Measured before the insert so the shard lock is not held
                // across the index walk any longer than the insert needs.
                let bytes = reader_bytes(&reader);
                shard.open.insert(
                    t.file_id,
                    Entry {
                        reader: Arc::clone(&reader),
                        referenced: AtomicBool::new(true),
                        bytes,
                    },
                );
                self.open_count.fetch_add(1, Ordering::Relaxed);
                self.open_bytes.fetch_add(bytes, Ordering::Relaxed);
                reader
            }
        };
        drop(shard);
        self.evict_to_bound((t.file_id as usize) % SHARDS);
        Ok(out)
    }

    /// Drop `file_id` from the cache, returning its reader if it was open.
    ///
    /// The caller closes what comes back. A table that was never opened, or has
    /// already been evicted, needs no close — which is the point of returning
    /// an `Option` rather than opening one in order to close it.
    pub(crate) fn close(&self, file_id: u64) -> Option<Arc<Reader>> {
        let mut shard = self.shard(file_id).write();
        let out = shard.open.remove(&file_id).map(|e| {
            self.open_bytes.fetch_sub(e.bytes, Ordering::Relaxed);
            e.reader
        });
        if out.is_some() {
            self.open_count.fetch_sub(1, Ordering::Relaxed);
        }
        out
    }

    /// `(resident, index, bloom, open readers, index entries)` across the
    /// readers **currently open**.
    ///
    /// Only open readers count, and that is the honest measure: a closed table
    /// holds nothing. Opening every table to total up its index would be the
    /// bug this cache exists to fix, performed by the instrument meant to
    /// detect it.
    pub fn resident_breakdown(&self) -> (usize, usize, usize, usize, usize) {
        let mut out = (0usize, 0usize, 0usize, 0usize, 0usize);
        for shard in &self.shards {
            let s = shard.read();
            for e in s.open.values() {
                let (idx, bloom, entries) = e.reader.resident_breakdown();
                out.0 += idx + bloom;
                out.1 += idx;
                out.2 += bloom;
                out.3 += 1;
                out.4 += entries;
            }
        }
        out
    }

    /// Is either GLOBAL bound exceeded?
    ///
    /// The byte bound stops at the last reader on purpose. A budget smaller
    /// than a single reader would otherwise empty the cache on every insert and
    /// re-open on every access, turning a memory ceiling into a livelock; one
    /// resident reader is the smallest state that still makes progress, exactly
    /// as `max_open` is clamped to one. An operator who sets a budget below one
    /// table's index gets one table's index, and the honest place to see that is
    /// [`byte_stats`](Self::byte_stats), which will read above the budget.
    fn over_bound(&self) -> bool {
        let open = self.open_count.load(Ordering::Relaxed);
        if open > self.max_open.load(Ordering::Relaxed) {
            return true;
        }
        let max_bytes = self.max_bytes.load(Ordering::Relaxed);
        max_bytes > 0 && open > 1 && self.open_bytes.load(Ordering::Relaxed) > max_bytes
    }

    /// Give one shard a CLOCK visit and evict exactly one reader when it is
    /// non-empty. Returns whether the global bounds made progress.
    fn evict_one_shard(&self, shard_index: usize) -> bool {
        let mut shard = self.shards[shard_index].write();
        if shard.open.is_empty() {
            return false;
        }
        let mut victim = None;
        for (id, entry) in &shard.open {
            if entry.referenced.swap(false, Ordering::Relaxed) {
                continue;
            }
            victim = Some(*id);
            break;
        }
        // If every reader used its second chance, evict one anyway so a
        // lowering of the bound takes effect immediately.
        let victim = victim.or_else(|| shard.open.keys().next().copied());
        let Some(victim) = victim else {
            return false;
        };
        // The cache drops its `Arc`; a caller mid-read still holds one. These
        // counters therefore describe what the cache pins, not process peak.
        if let Some(entry) = shard.open.remove(&victim) {
            self.open_bytes.fetch_sub(entry.bytes, Ordering::Relaxed);
        }
        self.open_count.fetch_sub(1, Ordering::Relaxed);
        self.closes.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Enforce the GLOBAL bounds — count and bytes — rotating across shards
    /// from `start`.
    ///
    /// Second-chance per visit: an entry referenced since the last sweep is
    /// spared once (bit cleared); a shard whose entries were all spared
    /// falls back to evicting an arbitrary one, so progress is guaranteed.
    /// Shard locks are taken one at a time — never nested — so this cannot
    /// deadlock against `get`.
    ///
    /// Victim choice is by recency, not by size: evicting the largest reader
    /// would free the budget fastest but would keep re-opening the table most
    /// expensive to open. Recency is the right proxy for what the workload will
    /// ask for next, and the loop simply runs until the bytes fit.
    fn evict_to_bound(&self, start: usize) {
        let mut spin = 0usize;
        while self.over_bound() {
            let mut evicted = false;
            for off in 0..SHARDS {
                if self.evict_one_shard((start + off) % SHARDS) {
                    evicted = true;
                    break;
                }
            }
            spin += 1;
            // Racing readers can re-insert while we evict; give up after a
            // bounded number of rounds rather than convoy — the next insert
            // resumes enforcement.
            if !evicted || spin > 4096 {
                break;
            }
        }
    }
}
