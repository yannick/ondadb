//! Column family: an isolated, independently-configured key-value store within a
//! database, backed by its own memtable, WAL and LSM levels.
//!
//! read path, memtable rotation, flush,
//! recovery, adapted to Rust ownership: SSTable handles are reference-counted
//! with `Arc` (replacing the manual incref/decref), and backpressure uses a
//! `parking_lot` `Mutex`/`Condvar`.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::Sender;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::cache::BlockCache;
use crate::comparator::ComparatorRef;
use crate::config::{ColumnFamilyConfig, PartitionRule};
use crate::error::{OndaError, Result};
use crate::iterator::{ChildIter, Iterator};
use crate::manifest::SstMeta;
use crate::memtable::Memtable;
use crate::sst::{Reader, Writer, WriterOptions};
use crate::storage::TierRegistry;
use crate::util::{coarse_now_nanos, now_nanos};
use crate::wal::{self, Wal};
use smallvec::SmallVec;

/// Historical/default target size of an SSTable data block.
pub(crate) const DEFAULT_DATA_BLOCK_SIZE: usize = crate::sst::DEFAULT_BLOCK_SIZE;
const MAX_MANIFEST_LEVEL: u32 = 64;

/// One operation visible to a commit hook.
#[derive(Debug, Clone)]
pub struct CommitOp {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub tombstone: bool,
    pub ttl: i64,
}

/// Post-commit callback invoked after each committed batch touching the CF.
pub type CommitHookFn = Arc<dyn Fn(u64, &[CommitOp]) + Send + Sync>;

/// Verdict returned by a [`CompactionFilterFn`] for one key/value pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDecision {
    /// Keep the entry.
    Keep,
    /// Drop the entry (at the bottom level) or replace it with a tombstone
    /// (above the bottom, so older versions in lower levels stay shadowed).
    Remove,
}

/// Compaction filter: `(key, value) -> FilterDecision`, consulted during
/// compaction for the newest surviving non-tombstone version of each key at
/// or below the oldest live snapshot. Must be deterministic — it runs at
/// unpredictable times, possibly repeatedly for the same key.
///
/// Removals are **not snapshot-consistent** (same caveat as RocksDB): once a
/// compaction lands, new reads at older snapshots no longer see filtered
/// keys. Open iterators are unaffected — they pin the pre-compaction files.
/// Versions newer than the oldest live snapshot are never filtered.
pub type CompactionFilterFn = Arc<dyn Fn(&[u8], &[u8]) -> FilterDecision + Send + Sync>;

/// A finished SSTable plus its open reader.
#[derive(Debug)]
pub struct SstHandle {
    pub meta: SstMeta,
    /// How to (re)open this table, and the bounded cache that holds it.
    ///
    /// The reader is **not** owned here. Opening one loads the table's whole
    /// block index and bloom filter, and holding every table's reader for the
    /// lifetime of the column family made resident memory proportional to total
    /// stored bytes: 6.6 GB twelve seconds into startup on a 48 GiB store, 12 GB
    /// at twenty-five and still opening. See [`crate::table_cache`].
    tref: crate::table_cache::TableRef,
    cache: Arc<crate::table_cache::TableCache>,
}

impl SstHandle {
    /// This table's reader, opening it if the cache has closed it.
    ///
    /// Fallible where a field access was infallible: a closed reader has to be
    /// re-opened, and that is I/O. Callers that previously could not fail now
    /// propagate — which is correct, because the alternative is panicking on a
    /// disk error deep inside a read path.
    pub fn reader(&self) -> Result<Arc<Reader>> {
        self.cache.get(&self.tref)
    }

    /// Release this table's file handles and drop it from the reader cache.
    ///
    /// A table that is not open needs no close, so nothing is opened here.
    pub fn close(&self) {
        if let Some(r) = self.cache.close(self.tref.file_id) {
            r.close();
        }
    }
}

/// An immutable (sealed) memtable awaiting flush.
#[derive(Debug)]
pub struct ImmMemtable {
    pub mem: Arc<Memtable>,
    pub wal_paths: Vec<String>,
}

/// What a flush produced, before anything is published (2.2).
///
/// `table` is `None` when the sealed memtable held no entries — a legal outcome
/// that adds nothing to the catalog, so its transaction carries no ops.
#[derive(Debug)]
pub(crate) struct FlushOutput {
    pub table: Option<Arc<SstHandle>>,
    pub wal_paths: Vec<String>,
}

/// Shared database context handed to each column family.
pub(crate) struct CfCtx {
    /// Storage-tier registry: resolves a table's tier to its root directory and
    /// [`Storage`](crate::storage::Storage) backend. The default tier is the DB
    /// directory, so untiered tables resolve exactly as before tiering existed.
    pub tiers: Arc<TierRegistry>,
    pub bc: Arc<BlockCache>,
    pub range_fragment_registry: Arc<crate::range_tombstone::FragmentRegistry>,
    /// Bandwidth admission for background IO, or `None` when unlimited. Carried
    /// into every reader this CF opens and every writer it creates: the class
    /// comes from the thread, but the limiter is DB-scoped, so two databases in
    /// one process pace independently.
    pub io_limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>,
    /// Bounded cache of open SSTable readers — see [`crate::table_cache`].
    pub tables: Arc<crate::table_cache::TableCache>,
    /// Whether `bc`/`tables` are views leased from a shared
    /// [`ReadResources`](crate::read_resources::ReadResources). Another
    /// database of the same cache namespace may be reading through the very
    /// readers this one opened, so close must leave them to the lease (which
    /// purges the namespace when its last database goes) instead of closing
    /// them itself.
    pub shared_reads: bool,
    pub flush_tx: Sender<FlushJob>,
    pub compact_tx: Sender<Arc<ColumnFamily>>,
    pub closing: Arc<AtomicBool>,
    pub read_only: bool,
    pub pending_flush: Arc<std::sync::atomic::AtomicUsize>,
    /// Set in unified-memtable mode; CFs route writes/reads through it.
    pub unified: Option<Arc<crate::unified::UnifiedStore>>,
    /// DB-wide fail-stop flag; write commits check it, WALs and background
    /// workers trip it on durability failures.
    pub poison: Arc<crate::util::Poison>,
    /// DB-wide counter of successful physical WAL `sync_data` calls; wired into
    /// every WAL this DB opens (see [`crate::DB::wal_sync_count`]).
    pub wal_syncs: Arc<std::sync::atomic::AtomicU64>,
    /// [`Options::wal_write_buffer_size`](crate::Options::wal_write_buffer_size),
    /// applied to every WAL generation this database opens.
    pub wal_write_buffer_size: usize,
    /// The database-wide bound on parallel data-block reads of batched point
    /// reads ([`Options::max_concurrent_block_reads`](crate::Options::max_concurrent_block_reads)).
    pub block_reads: Arc<crate::util::Semaphore>,
    /// The same `Arc` `DbInner::caps` holds, so a flush landing an L0 table can
    /// see whether `CAP_PERIODIC_AGE` is active without reaching for the whole
    /// database.
    pub caps: Arc<AtomicU64>,
    /// The database's injectable clock (0.3) — read here only to stamp
    /// [`SstMeta::last_compaction_time`](crate::manifest::SstMeta::last_compaction_time).
    pub clock: Arc<crate::util::Clock>,
    /// The same committed-span index `DbInner` holds (1.2), so per-family stats
    /// can report its size without reaching for the whole database.
    pub span_index: Arc<crate::span_index::SpanIndex>,
    /// The on-disk format family this database's files are decoded as.
    /// [`FormatProfile::Epoch1`](crate::format::FormatProfile::Epoch1) always,
    /// except for a 0.9 directory opened read-only through `legacy_onda`.
    pub format: crate::format::FormatProfile,
}

impl std::fmt::Debug for CfCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CfCtx").finish()
    }
}

/// A unit of flush work: a per-CF memtable, or a unified memtable to split.
pub(crate) enum FlushJob {
    PerCf {
        cf: Arc<ColumnFamily>,
        imm: Arc<ImmMemtable>,
    },
    Unified {
        imm: Arc<crate::unified::UnifiedImm>,
    },
}

/// Mutable LSM state guarded by an `RwLock`.
struct CfState {
    mem: Arc<Memtable>,
    wal: Option<Arc<Wal>>,
    wal_gen: u64,
    pending_wals: Vec<String>,
    imm: Vec<Arc<ImmMemtable>>,
    levels: Vec<Vec<Arc<SstHandle>>>,
}

struct RotState {
    active_writers: usize,
    rotating: bool,
}

struct PointReadCandidate {
    value: Option<Vec<u8>>,
    seq: u64,
    found: bool,
    deleted: bool,
    /// Kind of the winning version. A family with no merge operator never reads
    /// it; a merge family uses it as the *decision* whether this key needs the
    /// chain walk at all — see `ColumnFamily::get_with_merge`.
    kind: u64,
    /// Newest range delete (1.2) covering this key, whether or not it beat the
    /// point version. A merge fold needs it even when it lost: operands below
    /// it are masked, so it is the chain's floor.
    range_floor: Option<u64>,
}

impl Default for PointReadCandidate {
    fn default() -> PointReadCandidate {
        PointReadCandidate {
            value: None,
            seq: 0,
            found: false,
            deleted: false,
            kind: crate::format::KIND_PUT,
            range_floor: None,
        }
    }
}

impl PointReadCandidate {
    fn consider(
        &mut self,
        value: Option<Vec<u8>>,
        seq: u64,
        found: bool,
        deleted: bool,
        kind: u64,
    ) {
        if found && (!self.found || seq > self.seq) {
            self.value = value;
            self.seq = seq;
            self.found = true;
            self.deleted = deleted;
            self.kind = kind;
        }
    }

    fn consider_memtable(&mut self, lookup: crate::memtable::Lookup) {
        let value = if lookup.deleted {
            None
        } else {
            Some(lookup.value)
        };
        self.consider(
            value,
            lookup.seq,
            lookup.found,
            lookup.deleted,
            lookup.kind,
        );
    }

    /// Apply range-delete coverage.
    ///
    /// Deleted iff the covering sequence is **above** the surviving point
    /// version's, or there is no point version at all. Sequences within a
    /// commit are distinct by construction (`apply_prepared` assigns
    /// `start + slot`) and own-write overlap is rejected, so the comparison is
    /// total — no tie can arise for it to resolve.
    fn mask(&mut self, covering: Option<u64>) {
        let Some(seq) = covering else { return };
        // Recorded even when the span loses: `fold_point_chain` still has to
        // cut the operand chain at it.
        self.range_floor = Some(seq);
        if !self.found || seq > self.seq {
            self.value = None;
            self.seq = seq;
            self.found = true;
            self.deleted = true;
            // The winning version is now the span, not the point entry — a
            // masked operand must not send the reader down the fold path.
            self.kind = crate::format::KIND_DELETE;
        }
    }

    fn finish(self) -> Result<Vec<u8>> {
        if self.found && !self.deleted {
            Ok(self.value.unwrap_or_default())
        } else {
            Err(OndaError::NotFound)
        }
    }
}

/// Where a point read collects its winning version: the owned
/// [`PointReadCandidate`] behind `get`, or [`BufCandidate`] behind `get_into`.
///
/// One generic resolution pass (`ColumnFamily::resolve_point`) drives both, so
/// the source order, the range-delete rule and the `max_seq` early exit cannot
/// drift apart between the two reads; monomorphization keeps `get`'s path
/// exactly what it was.
trait PointSink {
    fn found(&self) -> bool;
    fn seq(&self) -> u64;
    fn probe_unified(
        &mut self,
        u: &crate::unified::UnifiedStore,
        id: u64,
        key: &[u8],
        read_seq: u64,
        now: i64,
    );
    fn probe_mem(&mut self, mem: &Memtable, key: &[u8], read_seq: u64, now: i64);
    fn probe_table(&mut self, rd: &Reader, key: &[u8], read_seq: u64, now: i64) -> Result<()>;
    fn apply_mask(&mut self, covering: Option<u64>);
}

impl PointSink for PointReadCandidate {
    fn found(&self) -> bool {
        self.found
    }
    fn seq(&self) -> u64 {
        self.seq
    }
    fn probe_unified(
        &mut self,
        u: &crate::unified::UnifiedStore,
        id: u64,
        key: &[u8],
        read_seq: u64,
        now: i64,
    ) {
        self.consider_memtable(u.get(id, key, read_seq, now));
    }
    fn probe_mem(&mut self, mem: &Memtable, key: &[u8], read_seq: u64, now: i64) {
        self.consider_memtable(mem.get(key, read_seq, now));
    }
    fn probe_table(&mut self, rd: &Reader, key: &[u8], read_seq: u64, now: i64) -> Result<()> {
        let (value, seq, found, deleted, kind) = rd.get_unfiltered(key, read_seq, now)?;
        self.consider(value, seq, found, deleted, kind);
        Ok(())
    }
    fn apply_mask(&mut self, covering: Option<u64>) {
        self.mask(covering);
    }
}

/// [`PointReadCandidate`] for `get_into`: the winning value lives in the
/// caller's buffer, at `out[start..start + len]`, and nothing is allocated
/// beyond that buffer's growth.
///
/// Memtable versions are copied straight out of the skiplist through the
/// borrowing `chain` walk. A table's value is appended *after* the current
/// winner and only moved down over it if it wins, so a losing probe costs a
/// truncate, never the winner.
struct BufCandidate<'a> {
    out: &'a mut Vec<u8>,
    start: usize,
    found: bool,
    seq: u64,
    deleted: bool,
    kind: u64,
    range_floor: Option<u64>,
}

impl<'a> BufCandidate<'a> {
    fn new(out: &'a mut Vec<u8>) -> BufCandidate<'a> {
        let start = out.len();
        BufCandidate {
            out,
            start,
            found: false,
            seq: 0,
            deleted: false,
            kind: crate::format::KIND_PUT,
            range_floor: None,
        }
    }

    /// `consider` for a borrowed memtable version: `value` is `None` for a
    /// tombstone or an expired entry, exactly as `Memtable::chain` reports it.
    fn offer(&mut self, seq: u64, kind: u64, value: Option<&[u8]>) {
        if self.found && seq <= self.seq {
            return;
        }
        self.out.truncate(self.start);
        if let Some(v) = value {
            self.out.extend_from_slice(v);
        }
        self.found = true;
        self.seq = seq;
        self.deleted = value.is_none();
        self.kind = kind;
    }

    /// The value's length, or `NotFound` (with the buffer restored).
    fn finish(self) -> Result<usize> {
        if self.found && !self.deleted {
            Ok(self.out.len() - self.start)
        } else {
            self.out.truncate(self.start);
            Err(OndaError::NotFound)
        }
    }
}

impl PointSink for BufCandidate<'_> {
    fn found(&self) -> bool {
        self.found
    }
    fn seq(&self) -> u64 {
        self.seq
    }
    fn probe_unified(
        &mut self,
        u: &crate::unified::UnifiedStore,
        id: u64,
        key: &[u8],
        read_seq: u64,
        now: i64,
    ) {
        // `chain` walks the store's memtables newest first and stops when the
        // callback says so — after the first version, which is what `get`
        // returns.
        u.chain(id, key, read_seq, now, |seq, kind, value| {
            self.offer(seq, kind, value);
            false
        });
    }
    fn probe_mem(&mut self, mem: &Memtable, key: &[u8], read_seq: u64, now: i64) {
        mem.chain(key, read_seq, now, |seq, kind, value| {
            self.offer(seq, kind, value);
            false
        });
    }
    fn probe_table(&mut self, rd: &Reader, key: &[u8], read_seq: u64, now: i64) -> Result<()> {
        let tail = self.out.len();
        let (seq, found, deleted, kind) =
            match rd.get_unfiltered_into(key, read_seq, now, self.out) {
                Ok(r) => r,
                Err(e) => {
                    self.out.truncate(tail);
                    return Err(e);
                }
            };
        if found && (!self.found || seq > self.seq) {
            // The new winner sits after the old one; close the gap.
            self.out.drain(self.start..tail);
            self.found = true;
            self.seq = seq;
            self.deleted = deleted;
            self.kind = kind;
        } else {
            self.out.truncate(tail);
        }
        Ok(())
    }
    fn apply_mask(&mut self, covering: Option<u64>) {
        let Some(seq) = covering else { return };
        self.range_floor = Some(seq);
        if !self.found || seq > self.seq {
            self.out.truncate(self.start);
            self.found = true;
            self.seq = seq;
            self.deleted = true;
            self.kind = crate::format::KIND_DELETE;
        }
    }
}

/// One version of a key gathered while resolving a merge chain.
///
/// Sources are walked newest-first and each contributes its own run — every
/// operand it holds plus, if it has one, the base that terminates them — and
/// the runs are then ordered by sequence. A per-source run is what makes this
/// correct across sources: two memtables and three tables can each hold part of
/// one chain, and only the sequence numbers say how they interleave.
struct ChainVersion {
    seq: u64,
    merge: bool,
    /// Operand bytes, the base's value, or `None` for a deleted or expired
    /// base — the found/deleted split the point-read path already carries.
    value: Option<Vec<u8>>,
}

/// A [`crate::memtable::Memtable::chain`] callback that appends into `out` and
/// stops the walk at the first base.
fn collect_chain(out: &mut Vec<ChainVersion>) -> impl FnMut(u64, u64, Option<&[u8]>) -> bool + '_ {
    move |seq, kind, value| {
        let merge = kind == crate::format::KIND_MERGE;
        out.push(ChainVersion {
            seq,
            merge,
            value: value.map(<[u8]>::to_vec),
        });
        merge
    }
}

/// Resolve a gathered chain into the value a reader sees.
///
/// The fold rule, over the versions of one key newest-first: collect operands
/// until the first `Put`/`Delete`/`SingleDelete`; a `Put` is `existing =
/// Some(value)` and a delete (or an expired put) is `existing = None`; running
/// out of versions is also `existing = None`. The operands are then reversed to
/// oldest-first and handed to the operator.
///
/// A group with **no** operand resolves exactly as it did before 1.1 — no
/// operator call and no allocation beyond the value itself.
fn fold_chain(
    op: &Arc<dyn crate::config::MergeOperator>,
    user_key: &[u8],
    versions: &mut [ChainVersion],
) -> Result<Vec<u8>> {
    // Stable, so equal sequences keep the source order they were gathered in.
    versions.sort_by_key(|v| std::cmp::Reverse(v.seq));
    let mut operands: Vec<&[u8]> = Vec::new();
    let mut existing: Option<&[u8]> = None;
    for version in versions.iter() {
        if version.merge {
            // A merge operand is never `None`: only a base can be deleted.
            operands.push(version.value.as_deref().unwrap_or(&[]));
            continue;
        }
        existing = version.value.as_deref();
        break;
    }
    if operands.is_empty() {
        return match existing {
            Some(value) => Ok(value.to_vec()),
            None => Err(OndaError::NotFound),
        };
    }
    operands.reverse();
    op.full_merge(user_key, existing, &operands).map_err(|e| {
        // The operator is part of the stored format: a chain it cannot fold is
        // a database this binary cannot read, not a missing key.
        OndaError::Corruption(format!(
            "merge operator {:?} failed for key {:?}: {e}",
            op.name(),
            String::from_utf8_lossy(user_key)
        ))
    })
}

struct PointReadSources {
    mem: Arc<Memtable>,
    imms: Vec<Arc<ImmMemtable>>,
    tables: SmallVec<[Arc<SstHandle>; 4]>,
    /// Tables consulted for **range coverage only** (1.2): an L0 table whose
    /// point bounds exclude the key but whose fragment span contains it, and
    /// per level >= 1 the *gap owner* — the table left of the point candidate,
    /// which owns the interval between its own last point key and the next
    /// table's first one.
    ///
    /// Kept apart from `tables` so a gap owner never costs a bloom probe or a
    /// point lookup: it has nothing to say about the key except whether a
    /// tombstone covers it. Empty for every point-only column family.
    range_tables: SmallVec<[Arc<SstHandle>; 2]>,
}

impl PointReadSources {
    /// Whether any source *this struct holds* could contribute range coverage.
    ///
    /// One relaxed load per memtable and one `range_count == 0` comparison per
    /// candidate table. The shared unified store is checked by the caller,
    /// which is the only one that knows whether the family is in unified mode
    /// — leaving it out here is what made a unified range delete invisible in
    /// the first version of this gate.
    #[inline]
    fn has_ranges(&self) -> bool {
        !self.range_tables.is_empty()
            || !self.mem.ranges().is_empty()
            || self.imms.iter().any(|i| !i.mem.ranges().is_empty())
            || self.tables.iter().any(|t| t.meta.has_ranges())
    }
}

/// One state snapshot covering a whole batch of point reads.
///
/// The single-key [`PointReadSources`] stays as it is: `get` is the hot path
/// and must not grow a per-table index vector. This is its batched twin — the
/// same sources, but with each candidate table carrying the *result indices*
/// whose keys it covers, so the reader is opened, filtered and walked once per
/// table instead of once per key.
///
/// `tables` is in the same source order [`PointReadSources`] produces: L0
/// newest-first, then, per level below, the tables that level contributes.
/// A key meets at most one table per level, so replaying a single key's table
/// list out of this yields exactly the order `get` would have used — which is
/// what makes equal-sequence ties resolve identically.
struct BatchReadSources {
    mem: Arc<Memtable>,
    imms: Vec<Arc<ImmMemtable>>,
    tables: BatchTables,
    /// Tables consulted for range coverage only, in the same shape as
    /// `tables` — see [`PointReadSources::range_tables`]. Empty for every
    /// point-only column family.
    range_tables: BatchTables,
}

/// One candidate table plus the result indices of the batch whose keys it
/// covers. Inline budgets mirror `PointReadSources`' `SmallVec<[_; 4]>`: a
/// small batch over a shallow LSM must not cost more heap traffic than the N
/// `get`s it replaces — that is what keeps a one-key batch honest.
type BatchTables = SmallVec<[(Arc<SstHandle>, SmallVec<[usize; 8]>); 4]>;

/// An isolated key-value store within a [`crate::DB`].
pub struct ColumnFamily {
    pub(crate) ctx: Arc<CfCtx>,
    name: String,
    id: u64,
    dir: String,
    pub(crate) opts: ColumnFamilyConfig,
    /// Live partition rules — interior-mutable so [`crate::DB::add_partition_rule`]
    /// can append to a *running* CF. Seeded from `opts.partition_rules` at
    /// create/load; from then on this is the runtime authority for partitioning
    /// (compaction and the manifest encode read it, never `opts.partition_rules`).
    /// A compaction snapshots it once at the start of a run, so a rule added
    /// mid-run only affects the *next* bottom compaction — write-side-only
    /// semantics: existing bottom files keep the stamps they were cut with.
    live_partition_rules: RwLock<Vec<PartitionRule>>,
    cmp: ComparatorRef,

    state: RwLock<CfState>,
    rot: Mutex<RotState>,
    cond: Condvar,

    pub(crate) flushing: AtomicBool,
    /// Per-family flush jobs queued or running. Manual flush waits use this
    /// instead of the database-wide queue so unrelated CFs cannot delay them.
    pub(crate) pending_flushes: AtomicUsize,
    pub(crate) compacting: AtomicBool,
    /// Serializes compaction runs on this CF. Two concurrent runs each
    /// snapshot the level set, merge, and `replace_levels` — the loser's
    /// installed tables would be dropped while its inputs are already
    /// unlinked. Background workers and `DB::compact` can otherwise overlap
    /// (the compact queue may hold the same CF twice across two worker
    /// threads).
    ///
    /// Since 0.8.0 this guards only whole-CF operations — the manual
    /// `DB::compact` sweep. Ordinary compaction and the parts/tiers operations
    /// exclude each other through [`range_locks`](Self::range_locks) instead,
    /// so two jobs on disjoint key ranges run concurrently.
    pub(crate) compact_mu: Mutex<()>,
    /// Key ranges currently being rewritten in this CF. The single exclusion
    /// mechanism shared by compaction (non-blocking) and the parts/tiers
    /// operations (blocking) — see [`crate::range_lock`].
    pub(crate) range_locks: Arc<crate::range_lock::RangeLocks>,
    /// Per-level sweep position: the key a level's next compaction starts
    /// looking from, so successive jobs advance across the keyspace instead of
    /// re-picking the same file. Wraps to the start when it runs off the end.
    pub(crate) compact_cursor: Mutex<std::collections::HashMap<usize, Vec<u8>>>,
    /// Cached compaction debt in bytes — how far the levels sit past their
    /// capacities. Read on every commit to decide pacing, so it is a gauge
    /// rather than a computation: recomputing it would walk every level's file
    /// list per write. Refreshed by whatever changes level sizes (a flush
    /// landing in L0, a compaction completing) via
    /// [`crate::compaction::refresh_compaction_debt`].
    pub(crate) compaction_debt: AtomicU64,
    /// Spans the most recent **bounded** compaction job on this family ran, and
    /// the byte spread between its widest and narrowest span (0.8). `1` and `0`
    /// mean it ran as a single merge — the default, and what a family excluded
    /// from spanning always reports. Written only by the job classes that may
    /// span, so the single-span sweep does not overwrite what they did.
    pub(crate) span_count: AtomicU64,
    pub(crate) span_imbalance_bytes: AtomicU64,
    commit_hook: Mutex<Option<CommitHookFn>>,
    compaction_filter: Mutex<Option<CompactionFilterFn>>,
    /// Mirrors `commit_hook.is_some()`; lets the commit path skip building hook
    /// payloads (a per-op key+value clone) with a relaxed load instead of a lock.
    hook_set: AtomicBool,

    pub(crate) flush_count: AtomicU64,
    pub(crate) compaction_count: AtomicU64,
    /// Subset of `compaction_count` picked by the periodic (age) trigger rather
    /// than by a capacity trigger — see
    /// [`CfStats::periodic_compactions`](crate::maintenance::CfStats::periodic_compactions).
    pub(crate) periodic_compactions: AtomicU64,
    pub(crate) compaction_failures: AtomicU64,
    pub(crate) last_compaction_error: Mutex<Option<String>>,

    // Read-path counters (relaxed; observability only).
    pub(crate) point_reads: AtomicU64,
    /// Range-delete records committed to this family since it was opened
    /// (1.2). Zero for every family that never uses the feature.
    range_deletes: AtomicU64,
    /// Tables retired by delete-only excise (1.2) since this family was opened,
    /// and the klog+vlog bytes they held.
    excised_tables: AtomicU64,
    excised_bytes: AtomicU64,
    pub(crate) bloom_skips: AtomicU64,
    pub(crate) sst_probes: AtomicU64,
    /// `state` acquisitions made by the point-read planners
    /// ([`point_read_sources`](Self::point_read_sources) and
    /// [`batch_read_sources`](Self::batch_read_sources)). Test-only: the whole
    /// point of the batch planner is that it snapshots once for N keys, and
    /// nothing else can observe that.
    #[cfg(test)]
    point_state_reads: AtomicU64,
}

impl std::fmt::Debug for ColumnFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamily")
            .field("name", &self.name)
            .finish()
    }
}

impl ColumnFamily {
    /// Number of SSTables currently in L0.
    ///
    /// Public because L0 growth is the observable symptom of compaction not
    /// running, and a test that cannot see it can only assert on timing.
    pub fn l0_file_count(&self) -> usize {
        self.state.read().levels[0].len()
    }

    /// `(resident, index, bloom, open readers, index entries)` for the
    /// **database-wide** reader cache.
    ///
    /// No longer per-column-family: readers live in one shared bounded cache
    /// (see [`crate::table_cache`]), so attributing resident bytes to a CF would
    /// mean opening its tables to measure them — the very thing the cache
    /// exists to avoid.
    pub fn resident_reader_bytes(&self) -> (usize, usize, usize, usize, usize) {
        self.ctx.tables.resident_breakdown()
    }

    pub(crate) fn wal_path(&self, gen: u64) -> String {
        format!("{}/wal-{}.log", self.dir, gen)
    }

    pub(crate) fn klog_path(&self, id: u64) -> String {
        format!("{}/{}.klog", self.dir, id)
    }

    /// Absolute klog path for `meta`, honoring its storage tier. An untiered
    /// table (`meta.tier == None`) resolves to the default-tier path — the same
    /// `<db_dir>/cf-<name>/<id>.klog` as [`klog_path`](Self::klog_path); a tiered
    /// bottom part resolves under that tier's root.
    pub(crate) fn klog_path_for(&self, meta: &SstMeta) -> String {
        match (meta.tier.as_deref(), meta.object.as_deref()) {
            (None, _) => self.klog_path(meta.id),
            // A2: an object-named table resolves relative to the TIER ROOT —
            // the name was chosen at publish time and never changes, whichever
            // database reads it (the cf-{name}/ component is inside `object`).
            (Some(t), Some(o)) => {
                format!("{}/{o}.klog", self.ctx.tiers.root_for(Some(t)))
            }
            (Some(t), None) => format!(
                "{}/{}.klog",
                self.ctx.tiers.cf_dir(Some(t), &self.name),
                meta.id
            ),
        }
    }

    /// Open a reader for an already-on-disk table described by `meta`, using the
    /// [`Storage`](crate::storage::Storage) backend for its tier (so a no-mmap
    /// tier reads through the buffered path).
    /// Build the handle for `meta`, without opening its reader.
    pub(crate) fn handle_for(&self, meta: SstMeta) -> Arc<SstHandle> {
        let tref = crate::table_cache::TableRef {
            klog: self.klog_path_for(&meta),
            storage: self.ctx.tiers.storage_for(meta.tier.as_deref()),
            bc: self.ctx.bc.clone(),
            file_id: meta.id,
            cmp: self.cmp.clone(),
            vlog_cache_limit: self.opts.max_cached_vlog_value_bytes,
            io_limiter: self.ctx.io_limiter.clone(),
            format: self.ctx.format,
        };
        Arc::new(SstHandle {
            meta,
            tref,
            cache: Arc::clone(&self.ctx.tables),
        })
    }

    pub(crate) fn open_reader_for(&self, meta: &SstMeta) -> Result<Arc<Reader>> {
        let storage = self.ctx.tiers.storage_for(meta.tier.as_deref());
        Reader::open_profiled(
            &self.klog_path_for(meta),
            storage,
            self.ctx.bc.clone(),
            meta.id,
            self.cmp.clone(),
            self.opts.max_cached_vlog_value_bytes,
            self.ctx.io_limiter.clone(),
            self.ctx.format,
        )
    }

    /// Create a fresh column family (directory + generation-0 WAL).
    ///
    /// `unified_id` is the id its keys carry in a unified WAL and memtable —
    /// `unified::cf_id(&name)` unless [`DbInner::choose_unified_id`] had to
    /// pick another (plan C F5′). The caller persists a divergent one.
    ///
    /// [`DbInner::choose_unified_id`]: crate::db::DbInner::choose_unified_id
    pub(crate) fn create(
        ctx: Arc<CfCtx>,
        name: String,
        dir: String,
        opts: ColumnFamilyConfig,
        cmp: ComparatorRef,
        unified_id: u64,
    ) -> Result<Arc<ColumnFamily>> {
        std::fs::create_dir_all(&dir)?;
        let mem = Memtable::new(cmp.clone());
        let wal0 = format!("{dir}/wal-0.log");
        let wal = if ctx.read_only {
            None
        } else {
            let w = Wal::open_buffered(
                &wal0,
                opts.sync_mode,
                opts.sync_interval,
                crate::wal::SegmentId::per_cf(0),
                ctx.wal_write_buffer_size,
            )?;
            w.set_poison(ctx.poison.clone());
            w.set_sync_counter(ctx.wal_syncs.clone());
            Some(Arc::new(w))
        };
        let live_partition_rules = RwLock::new(opts.partition_rules.clone());
        let cf = Arc::new(ColumnFamily {
            ctx,
            id: unified_id,
            name,
            dir,
            opts,
            live_partition_rules,
            cmp: cmp.clone(),
            state: RwLock::new(CfState {
                mem,
                wal,
                wal_gen: 0,
                pending_wals: vec![wal0],
                imm: Vec::new(),
                levels: vec![Vec::new()],
            }),
            rot: Mutex::new(RotState {
                active_writers: 0,
                rotating: false,
            }),
            cond: Condvar::new(),
            flushing: AtomicBool::new(false),
            pending_flushes: AtomicUsize::new(0),
            compacting: AtomicBool::new(false),
            compact_mu: Mutex::new(()),
            range_locks: crate::range_lock::RangeLocks::new(cmp.clone()),
            compact_cursor: Mutex::new(std::collections::HashMap::new()),
            compaction_debt: AtomicU64::new(0),
            span_count: AtomicU64::new(1),
            span_imbalance_bytes: AtomicU64::new(0),
            commit_hook: Mutex::new(None),
            compaction_filter: Mutex::new(None),
            hook_set: AtomicBool::new(false),
            flush_count: AtomicU64::new(0),
            compaction_count: AtomicU64::new(0),
            periodic_compactions: AtomicU64::new(0),
            compaction_failures: AtomicU64::new(0),
            last_compaction_error: Mutex::new(None),
            point_reads: AtomicU64::new(0),
            range_deletes: AtomicU64::new(0),
            excised_tables: AtomicU64::new(0),
            excised_bytes: AtomicU64::new(0),
            bloom_skips: AtomicU64::new(0),
            #[cfg(test)]
            point_state_reads: AtomicU64::new(0),
            sst_probes: AtomicU64::new(0),
        });
        Ok(cf)
    }

    /// Reconstruct a CF from manifest SSTable metadata and replay its WALs.
    /// Returns the CF and the highest sequence seen during replay.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn load(
        ctx: Arc<CfCtx>,
        name: String,
        id: u64,
        dir: String,
        opts: ColumnFamilyConfig,
        cmp: ComparatorRef,
        ssts: &[SstMeta],
    ) -> Result<(Arc<ColumnFamily>, u64)> {
        if let Some(table) = ssts.iter().find(|table| table.level > MAX_MANIFEST_LEVEL) {
            return Err(OndaError::Corruption(format!(
                "column family {name:?} table {} has manifest level {}; supported maximum is {}",
                table.id, table.level, MAX_MANIFEST_LEVEL
            )));
        }
        let mut max_level = 1usize;
        for s in ssts {
            max_level = max_level.max(s.level as usize + 1);
        }
        let mut levels: Vec<Vec<Arc<SstHandle>>> = vec![Vec::new(); max_level];
        for s in ssts {
            // Resolve the tier before opening so a bottom part on another mount
            // (and any no-mmap backend it carries) is read through the right
            // storage. `None` tier == the default path used before tiering.
            // NOT opened here. Opening every table in the manifest at startup
            // is what made resident memory track total stored bytes; the reader
            // is fetched on first use through the bounded table cache.
            // Mirrors `klog_path_for` (which needs a built CF): an
            // object-named table (A2) resolves relative to the tier ROOT.
            let klog = match (s.tier.as_deref(), s.object.as_deref()) {
                (None, _) => format!("{dir}/{}.klog", s.id),
                (Some(t), Some(o)) => {
                    format!("{}/{o}.klog", ctx.tiers.root_for(Some(t)))
                }
                (Some(t), None) => {
                    format!("{}/{}.klog", ctx.tiers.cf_dir(Some(t), &name), s.id)
                }
            };
            let tref = crate::table_cache::TableRef {
                klog,
                storage: ctx.tiers.storage_for(s.tier.as_deref()),
                bc: ctx.bc.clone(),
                file_id: s.id,
                cmp: cmp.clone(),
                vlog_cache_limit: opts.max_cached_vlog_value_bytes,
                io_limiter: ctx.io_limiter.clone(),
                format: ctx.format,
            };
            levels[s.level as usize].push(Arc::new(SstHandle {
                meta: s.clone(),
                tref,
                cache: Arc::clone(&ctx.tables),
            }));
        }
        for lvl in levels.iter_mut().skip(1) {
            lvl.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
        }

        // Replay all existing WAL generations into a fresh memtable.
        let mem = Memtable::new(cmp.clone());
        let gens = existing_wal_gens(&dir)?;
        let mut max_seq = 0u64;
        let mut replay_paths = Vec::new();
        for g in &gens {
            let p = format!("{dir}/wal-{g}.log");
            replay_paths.push(p.clone());
            let mut apply = |rec| {
                match rec {
                    crate::wal::ReplayRecord::Point(r) => {
                        mem.put(&r.key, r.value, r.seq, r.ttl, r.kind);
                    }
                    // Schema 1: both bounds are user keys already.
                    crate::wal::ReplayRecord::RangeDelete { start, end, seq } => {
                        mem.add_range(&start, &end, seq);
                    }
                    // 2PC is unified-only: a per-CF WAL cannot establish a
                    // prepare across independent logs, so this binary never
                    // writes a control frame here. Finding one means the
                    // database was written in unified mode and reopened per-CF.
                    crate::wal::ReplayRecord::Prepare { .. }
                    | crate::wal::ReplayRecord::Decision { .. } => {
                        return Err(OndaError::InvalidArgs(format!(
                            "column family {name:?} has a prepared-transaction record in its \
                             per-column-family WAL; two-phase commit requires \
                             unified_memtable=true"
                        )));
                    }
                }
                Ok(())
            };
            let last = match ctx.format {
                crate::format::FormatProfile::Epoch1 => {
                    Wal::replay(&p, crate::wal::SegmentId::per_cf(*g), &mut apply)?
                }
                #[cfg(feature = "legacy-onda")]
                crate::format::FormatProfile::Onda09 => {
                    crate::legacy_onda::wal::replay(&p, &mut apply)?
                }
            };
            max_seq = max_seq.max(last);
        }

        let next_gen = gens.last().map(|g| g + 1).unwrap_or(0);
        let (wal, pending) = if ctx.read_only {
            (None, replay_paths)
        } else {
            let p = format!("{dir}/wal-{next_gen}.log");
            let w = Wal::open_buffered(
                &p,
                opts.sync_mode,
                opts.sync_interval,
                crate::wal::SegmentId::per_cf(next_gen),
                ctx.wal_write_buffer_size,
            )?;
            w.set_poison(ctx.poison.clone());
            w.set_sync_counter(ctx.wal_syncs.clone());
            let w = Arc::new(w);
            let mut pend = replay_paths;
            pend.push(p);
            (Some(w), pend)
        };

        let live_partition_rules = RwLock::new(opts.partition_rules.clone());
        let cf = Arc::new(ColumnFamily {
            ctx,
            id,
            name,
            dir,
            opts,
            live_partition_rules,
            cmp: cmp.clone(),
            state: RwLock::new(CfState {
                mem,
                wal,
                wal_gen: next_gen,
                pending_wals: pending,
                imm: Vec::new(),
                levels,
            }),
            rot: Mutex::new(RotState {
                active_writers: 0,
                rotating: false,
            }),
            cond: Condvar::new(),
            flushing: AtomicBool::new(false),
            pending_flushes: AtomicUsize::new(0),
            compacting: AtomicBool::new(false),
            compact_mu: Mutex::new(()),
            range_locks: crate::range_lock::RangeLocks::new(cmp.clone()),
            compact_cursor: Mutex::new(std::collections::HashMap::new()),
            compaction_debt: AtomicU64::new(0),
            span_count: AtomicU64::new(1),
            span_imbalance_bytes: AtomicU64::new(0),
            commit_hook: Mutex::new(None),
            compaction_filter: Mutex::new(None),
            hook_set: AtomicBool::new(false),
            flush_count: AtomicU64::new(0),
            compaction_count: AtomicU64::new(0),
            periodic_compactions: AtomicU64::new(0),
            compaction_failures: AtomicU64::new(0),
            last_compaction_error: Mutex::new(None),
            point_reads: AtomicU64::new(0),
            range_deletes: AtomicU64::new(0),
            excised_tables: AtomicU64::new(0),
            excised_bytes: AtomicU64::new(0),
            bloom_skips: AtomicU64::new(0),
            #[cfg(test)]
            point_state_reads: AtomicU64::new(0),
            sst_probes: AtomicU64::new(0),
        });
        Ok((cf, max_seq))
    }

    /// Column family name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn comparator(&self) -> &ComparatorRef {
        &self.cmp
    }

    pub(crate) fn record_compaction_failure(&self, error: &OndaError) {
        self.compaction_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *self.last_compaction_error.lock() = Some(error.to_string());
    }

    /// Install a post-commit hook.
    /// Install (or clear) the compaction filter. Applies to compactions that
    /// start after the call; not persisted (re-install after reopen).
    pub fn set_compaction_filter(&self, f: Option<CompactionFilterFn>) {
        *self.compaction_filter.lock() = f;
    }

    pub(crate) fn compaction_filter(&self) -> Option<CompactionFilterFn> {
        self.compaction_filter.lock().clone()
    }

    pub fn set_commit_hook(&self, hook: Option<CommitHookFn>) {
        let mut g = self.commit_hook.lock();
        self.hook_set.store(hook.is_some(), Ordering::Release);
        *g = hook;
    }

    /// Whether a commit hook is installed (cheap check for the commit path).
    pub(crate) fn has_commit_hook(&self) -> bool {
        self.hook_set.load(Ordering::Acquire)
    }

    pub(crate) fn run_commit_hook(&self, commit_seq: u64, ops: &[CommitOp]) {
        if let Some(h) = self.commit_hook.lock().as_ref() {
            h(commit_seq, ops);
        }
    }

    /// Is compaction debt at or above the hard ceiling, where commits block?
    pub(crate) fn over_hard_compaction_limit(&self) -> bool {
        let hard = self.opts.hard_pending_compaction_bytes;
        hard != 0 && self.compaction_debt.load(Ordering::Relaxed) >= hard
    }

    /// Wake writers parked on the hard compaction limit. Taking `rot` is what
    /// makes the wake-up race-free against a writer that has just evaluated the
    /// predicate but not yet waited.
    pub(crate) fn notify_debt_waiters(&self) {
        let _g = self.rot.lock();
        self.cond.notify_all();
    }

    /// Delay this commit in proportion to how far compaction debt sits past the
    /// soft threshold.
    ///
    /// Without this, ingest runs at memtable speed no matter how far behind
    /// compaction is: writes return fast, debt grows unbounded, and the
    /// throughput a benchmark reports is a rate the engine cannot sustain. The
    /// delay is deliberately small and per-commit — it shapes the ingest rate
    /// rather than stopping it, leaving the hard ceiling to do the stopping.
    fn pace_for_compaction_debt(&self) {
        let soft = self.opts.soft_pending_compaction_bytes;
        if soft == 0 || self.ctx.closing.load(Ordering::Relaxed) {
            return;
        }
        let debt = self.compaction_debt.load(Ordering::Relaxed);
        if debt <= soft {
            return;
        }
        let hard = self.opts.hard_pending_compaction_bytes;
        // Fraction of the way from soft to hard, in [0, 1]. With no hard
        // ceiling configured there is no span to interpolate over, so pace at
        // the floor and let debt be bounded by whatever the operator intended.
        let frac = if hard > soft {
            ((debt - soft) as f64 / (hard - soft) as f64).min(1.0)
        } else {
            0.1
        };
        const MAX_DELAY_US: f64 = 1000.0;
        let delay_us = (frac * MAX_DELAY_US) as u64;
        if delay_us > 0 {
            std::thread::sleep(std::time::Duration::from_micros(delay_us));
        }
    }

    /// Apply a committed batch: append to the WAL and insert into the memtable,
    /// then rotate if the memtable is full. Records borrow the transaction's
    /// buffer; both the WAL and the memtable copy what they need.
    ///
    /// The WAL form is chosen per batch: a point-only batch keeps writing the
    /// legacy record stream (unchanged bytes, unchanged size), while a batch
    /// holding a range delete (1.2) writes **one** envelope frame carrying both
    /// kinds. One frame, because WAL batch atomicity (invariant 3) is per
    /// frame: two frames could replay half a commit.
    pub(crate) fn apply_commit_with_ranges(
        self: &Arc<Self>,
        recs: &[wal::RecordRef<'_>],
        ranges: &[wal::RangeRef<'_>],
    ) -> Result<()> {
        self.ctx.poison.check()?;
        // Soft pacing happens before the lock is taken: the point is to slow
        // this writer down, not to hold anyone else up while it waits.
        self.pace_for_compaction_debt();
        let threshold = self.opts.l0_queue_stall_threshold as usize;
        {
            let mut g = self.rot.lock();
            loop {
                let stalled = g.rotating
                    || ((self.state.read().imm.len() >= threshold
                        || self.over_hard_compaction_limit())
                        && !self.ctx.closing.load(Ordering::Relaxed));
                if stalled {
                    self.cond.wait(&mut g);
                } else {
                    break;
                }
            }
            g.active_writers += 1;
        }

        let (wal, mem) = {
            let s = self.state.read();
            (s.wal.clone(), s.mem.clone())
        };

        let res = (|| {
            let Some(w) = &wal else {
                return Err(OndaError::ReadOnly("wal unavailable".into()));
            };
            // The legacy record has no kind field, so a batch carrying one that
            // is not a point kind — 1.1's operand — or any 1.2 range fragment
            // goes out as an envelope frame instead. Checking the batch rather
            // than the family keeps every ordinary commit byte-identical to
            // what 0.8.2 wrote, including on a family that merely *has* an
            // operator.
            if ranges.is_empty() && recs.iter().all(|r| crate::format::is_point_kind(r.kind)) {
                w.append_batch(recs)?;
            } else {
                let mut batch: Vec<wal::EnvelopeRecord<'_>> =
                    Vec::with_capacity(recs.len() + ranges.len());
                batch.extend(recs.iter().copied().map(wal::EnvelopeRecord::Point));
                batch.extend(ranges.iter().copied().map(wal::EnvelopeRecord::Range));
                w.append_batch_envelope(wal::ENVELOPE_SCHEMA_PER_CF, &batch)?;
            }
            mem.put_batch(recs);
            for r in ranges {
                mem.add_range(r.start, r.end, r.seq);
            }
            self.note_range_delete(ranges.len() as u64);
            Ok(())
        })();

        {
            let mut g = self.rot.lock();
            g.active_writers -= 1;
            self.cond.notify_all();
        }
        res?;

        if mem.approx_size() >= self.opts.write_buffer_size as i64 {
            self.rotate_memtable(false);
        }
        Ok(())
    }

    /// Seal the active memtable and enqueue it for flush.  `force` rotates even a
    /// small (but non-empty) memtable, used on close.
    pub(crate) fn rotate_memtable(self: &Arc<Self>, force: bool) {
        let imm = {
            let mut g = self.rot.lock();
            if g.rotating {
                // A rotation is already in flight. Size-triggered callers can
                // simply return (every committer past the threshold calls this;
                // making the losers wait just serializes them behind the swap).
                // `force` callers (flush/close) must still ensure one happens.
                if !force {
                    return;
                }
                while g.rotating {
                    self.cond.wait(&mut g);
                }
            }
            {
                let s = self.state.read();
                // Range tombstones alone are enough to rotate: they are not
                // point entries, so `is_empty` would drop them on the floor.
                if s.mem.is_empty_including_ranges() {
                    return;
                }
                if !force && s.mem.approx_size() < self.opts.write_buffer_size as i64 {
                    return;
                }
            }
            g.rotating = true;

            // Open the next WAL before draining in-flight writers: the file
            // creation syscall overlaps the drain instead of extending the
            // window during which new commits are gated. Rotations are
            // serialized by `rotating`, so the next generation is stable.
            let (new_gen, new_path) = {
                let s = self.state.read();
                (s.wal_gen + 1, self.wal_path(s.wal_gen + 1))
            };
            drop(g);
            let new_wal = if self.ctx.read_only {
                None
            } else {
                Wal::open_buffered(
                    &new_path,
                    self.opts.sync_mode,
                    self.opts.sync_interval,
                    crate::wal::SegmentId::per_cf(new_gen),
                    self.ctx.wal_write_buffer_size,
                )
                .ok()
                .map(|w| {
                    w.set_poison(self.ctx.poison.clone());
                    w.set_sync_counter(self.ctx.wal_syncs.clone());
                    Arc::new(w)
                })
            };
            let mut g = self.rot.lock();
            while g.active_writers > 0 {
                self.cond.wait(&mut g);
            }

            let old_wal;
            let imm;
            {
                let mut s = self.state.write();
                let old_mem = std::mem::replace(&mut s.mem, Memtable::new(self.cmp.clone()));
                imm = Arc::new(ImmMemtable {
                    mem: old_mem,
                    wal_paths: s.pending_wals.clone(),
                });
                s.imm.push(imm.clone());
                old_wal = s.wal.take();
                s.wal_gen = new_gen;
                s.wal = new_wal;
                s.pending_wals = vec![new_path];
            }
            if let Some(w) = old_wal {
                let _ = w.close();
            }
            g.rotating = false;
            self.cond.notify_all();
            imm
        };

        // Enqueue for the flush worker (which drains the queue on shutdown).
        self.ctx.pending_flush.fetch_add(1, Ordering::SeqCst);
        self.pending_flushes.fetch_add(1, Ordering::SeqCst);
        if self
            .ctx
            .flush_tx
            .send(FlushJob::PerCf {
                cf: self.clone(),
                imm,
            })
            .is_err()
        {
            self.ctx.pending_flush.fetch_sub(1, Ordering::SeqCst);
            self.pending_flushes.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Flush an immutable memtable to a new L0 SSTable with id `file_id`.
    ///
    /// Writes and fsyncs the table and stops there: nothing is published. The
    /// caller wraps [`publish_flush`](Self::publish_flush) in a `catalog_txn`,
    /// so the table becomes visible — and its WAL becomes reclaimable — only
    /// after the edit record's fsync (AGENTS.md invariant 1).
    pub(crate) fn flush_imm(&self, imm: &Arc<ImmMemtable>, file_id: u64) -> Result<FlushOutput> {
        self.flushing.store(true, Ordering::Relaxed);
        let result = self.flush_imm_inner(imm, file_id);
        self.flushing.store(false, Ordering::Relaxed);
        self.cond.notify_all();
        result
    }

    fn flush_imm_inner(&self, imm: &Arc<ImmMemtable>, file_id: u64) -> Result<FlushOutput> {
        // The sealed memtable's range tombstones become this table's fragments.
        // Unclipped: a flush writes ONE L0 file covering the whole memtable, so
        // its owned interval is the memtable's whole keyspace. (Clipping starts
        // at compaction, where a job has several outputs to divide.)
        let fragments: Vec<crate::range_tombstone::Fragment> =
            imm.mem.ranges().fragments(None, None).collect();
        // Fast path: stream entries straight out of the sealed memtable's arena
        // nodes through a k-way merge — no per-entry allocation, no sort.
        #[cfg(feature = "arena-memtable")]
        let table = self.write_l0_streaming(&imm.mem, fragments, file_id)?;
        #[cfg(not(feature = "arena-memtable"))]
        let table = {
            let entries = imm.mem.snapshot();
            self.write_l0(&entries, fragments, file_id)?
        };
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        Ok(FlushOutput {
            table,
            wal_paths: imm.wal_paths.clone(),
        })
    }

    /// Publish step of a flush transaction: install the new L0 table and retire
    /// the sealed memtable it was written from, under one state write-lock so no
    /// reader ever sees the same entries twice.
    pub(crate) fn publish_flush(
        &self,
        imm: &Arc<ImmMemtable>,
        table: Option<Arc<SstHandle>>,
        _p: &crate::db::Publish,
    ) {
        let mut s = self.state.write();
        if let Some(handle) = table {
            s.levels[0].insert(0, handle); // newest first
        }
        if let Some(pos) = s.imm.iter().position(|i| Arc::ptr_eq(i, imm)) {
            imm.mem.ranges().retire();
            s.imm.remove(pos);
        }
    }

    /// Finish `w` (fsync + footer) and open a reader, without installing it.
    pub(crate) fn finish_writer_to_handle(
        &self,
        w: Writer,
        file_id: u64,
    ) -> Result<Arc<SstHandle>> {
        // Flush/ingest output always lands on the default tier (L0), so
        // `meta.tier` is None and `open_reader_for` resolves the default path.
        let meta = self.stamp_l0_meta(w.finish()?.to_sst_meta(file_id, 0));
        Ok(self.handle_for(meta))
    }

    /// The per-table state a fresh L0 table carries beyond what the writer
    /// reports.
    fn stamp_l0_meta(&self, mut meta: SstMeta) -> SstMeta {
        // Stamp the write time as the table's max entry age: flush/ingest output
        // holds freshly committed data, so the file's finish time approximates
        // the newest entry's commit time (see `SstMeta::max_entry_time`).
        meta.max_entry_time = Some(now_nanos());
        // Age state for the periodic trigger (0.3), from the injectable clock
        // rather than `now_nanos` — the two are deliberately separate sources
        // so a test driving periodic time cannot move tier placement. Only a
        // database holding CAP_PERIODIC_AGE may stamp: the manifest must never
        // carry a field a reopen could not attribute to an enabled capability.
        if self.ctx.caps.load(Ordering::SeqCst) & crate::format::CAP_PERIODIC_AGE != 0 {
            meta.last_compaction_time = Some(self.ctx.clock.now());
        }
        meta
    }

    /// Write `entries` (in this family's internal order) plus `fragments` as
    /// an L0 table at `klog` — **outside** this database's directory — and
    /// return its metadata without opening, installing or persisting it.
    ///
    /// The snapshot of a read-only database uses this to carry WAL-replayed
    /// memtable data into the destination: the source may not be written, and
    /// there is no flush worker to write it anyway. Same writer, same options
    /// and same stamps as a flush, so the table is exactly what a flush would
    /// have produced — only its location differs. Returns `None` when there is
    /// nothing to write.
    pub(crate) fn write_detached_l0(
        &self,
        klog: &str,
        entries: &[crate::memtable::Entry],
        fragments: Vec<crate::range_tombstone::Fragment>,
        file_id: u64,
    ) -> Result<Option<SstMeta>> {
        if entries.is_empty() && fragments.is_empty() {
            return Ok(None);
        }
        let written = (|| {
            let mut w = self.new_writer(klog, entries.len())?;
            w.set_range_fragments(fragments);
            for e in entries {
                w.add(&e.user_key, &e.value, e.seq, e.ttl, e.kind)?;
            }
            w.finish()
        })();
        match written {
            Ok(file) => Ok(Some(self.stamp_l0_meta(file.to_sst_meta(file_id, 0)))),
            Err(error) => {
                let _ = std::fs::remove_file(klog);
                let _ = std::fs::remove_file(crate::sst::vlog_path_for(klog));
                Err(error)
            }
        }
    }

    /// This family's sealed memtables' contents, oldest first, as
    /// `(entries in internal order, fragments)` — what a flush of each would
    /// write. Read-only snapshot helper; see
    /// [`write_detached_l0`](Self::write_detached_l0).
    pub(crate) fn sealed_contents(
        &self,
    ) -> Vec<(
        Vec<crate::memtable::Entry>,
        Vec<crate::range_tombstone::Fragment>,
    )> {
        let imms = self.state.read().imm.clone();
        imms.iter()
            .map(|imm| {
                (
                    imm.mem.snapshot(),
                    imm.mem.ranges().fragments(None, None).collect(),
                )
            })
            .collect()
    }

    /// Sort `entries` into this family's internal order (user key by this
    /// family's comparator, newest sequence first) — for a slice split out of
    /// the bytewise-ordered unified memtable.
    pub(crate) fn sort_internal(&self, entries: &mut [crate::memtable::Entry]) {
        entries.sort_by(|a, b| {
            self.cmp
                .compare(&a.user_key, &b.user_key)
                .then_with(|| b.seq.cmp(&a.seq))
        });
    }

    /// Register already-finished SSTables as the newest L0 files, atomically.
    ///
    /// A publication primitive: reachable only from a `catalog_txn` publish
    /// closure (see [`crate::db::Publish`]). The reversal is load-bearing —
    /// `handles` arrives oldest-first and L0 is read newest-first — and the
    /// edit log's `AddTable` replay reproduces exactly this order.
    pub(crate) fn install_handles_l0(&self, handles: Vec<Arc<SstHandle>>, _p: &crate::db::Publish) {
        let mut s = self.state.write();
        for h in handles {
            s.levels[0].insert(0, h); // newest first
        }
    }

    /// Open a fresh SSTable writer for this CF (used by bulk ingestion).
    pub(crate) fn new_sst_writer(&self, file_id: u64, expected: usize) -> Result<Writer> {
        self.new_writer(&self.klog_path(file_id), expected)
    }

    /// Stream a sealed memtable to a new L0 SSTable without materializing
    /// entries (keys/values borrowed from the arena through the merge).
    #[cfg(feature = "arena-memtable")]
    fn write_l0_streaming(
        &self,
        mem: &Memtable,
        fragments: Vec<crate::range_tombstone::Fragment>,
        file_id: u64,
    ) -> Result<Option<Arc<SstHandle>>> {
        let mut m = mem.flush_merge();
        // A memtable holding only range tombstones still has to produce a
        // table: dropping it would lose the deletes it recorded.
        if !m.valid() && fragments.is_empty() {
            return Ok(None);
        }
        let klog = self.klog_path(file_id);
        let mut w = self.new_writer(&klog, mem.num_entries().max(0) as usize)?;
        w.set_range_fragments(fragments);
        while m.valid() {
            let c = m.top();
            w.add(c.user_key(), c.value(), c.seq(), c.ttl(), c.kind())?;
            m.advance();
        }
        self.finish_writer_to_handle(w, file_id).map(Some)
    }

    /// Write `entries` (already in this CF's internal order) plus `fragments`
    /// to a new L0 SSTable.
    fn write_l0(
        &self,
        entries: &[crate::memtable::Entry],
        fragments: Vec<crate::range_tombstone::Fragment>,
        file_id: u64,
    ) -> Result<Option<Arc<SstHandle>>> {
        if entries.is_empty() && fragments.is_empty() {
            return Ok(None);
        }
        let klog = self.klog_path(file_id);
        let mut w = self.new_writer(&klog, entries.len())?;
        w.set_range_fragments(fragments);
        for e in entries {
            w.add(&e.user_key, &e.value, e.seq, e.ttl, e.kind)?;
        }
        self.finish_writer_to_handle(w, file_id).map(Some)
    }

    /// Ingest a CF's slice of a split unified memtable: sort by this CF's
    /// comparator, then write an L0 SSTable.
    /// Also takes that family's slice of the shared store's range tombstones.
    ///
    /// Returns the finished, fsynced table without publishing it: the caller
    /// installs it inside a `catalog_txn`.
    ///
    /// Fragments are refused unless the database holds
    /// [`CAP_RANGE_DELETES`](crate::format::CAP_RANGE_DELETES): the aux block
    /// only exists on an extended table, and a table this database is not
    /// authorized to write is one a reopen could not attribute. This is also
    /// what keeps externally ingested files point-only — nothing outside the
    /// unified flush ever hands fragments in.
    pub(crate) fn ingest_l0(
        &self,
        mut entries: Vec<crate::memtable::Entry>,
        fragments: Vec<crate::range_tombstone::Fragment>,
        file_id: u64,
    ) -> Result<Option<Arc<SstHandle>>> {
        if !fragments.is_empty() && !self.range_deletes_enabled() {
            return Err(OndaError::InvalidArgs(
                "range fragments require the CAP_RANGE_DELETES format capability;                  ingested files are point-only"
                    .into(),
            ));
        }
        entries.sort_by(|a, b| {
            self.cmp
                .compare(&a.user_key, &b.user_key)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        self.flushing.store(true, Ordering::Relaxed);
        let r = self.write_l0(&entries, fragments, file_id);
        self.flushing.store(false, Ordering::Relaxed);
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        r
    }

    /// Whether this database may write range-delete artifacts.
    #[inline]
    pub(crate) fn range_deletes_enabled(&self) -> bool {
        self.ctx.caps.load(Ordering::SeqCst) & crate::format::CAP_RANGE_DELETES != 0
    }

    /// Stable column-family id (used by unified-memtable key prefixing).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// This family's merge operator, or `None` when it has none.
    ///
    /// Resolved once at create/open from `Options::merge_fns`; every read path
    /// branches on `is_none()` here, so a family that never configured one pays
    /// a single null check and takes none of 1.1's code.
    #[inline]
    pub(crate) fn merge_op(&self) -> Option<&Arc<dyn crate::config::MergeOperator>> {
        self.opts.merge_operator.as_ref()
    }

    /// Whether this family may write kind-4 records: it has an operator *and*
    /// the database durably holds the capability that authorizes the envelope
    /// carrying them.
    pub(crate) fn merge_writes_enabled(&self) -> bool {
        self.opts.merge_operator.is_some()
            && self.ctx.caps.load(Ordering::SeqCst) & crate::format::CAPS_MERGE_WRITE
                == crate::format::CAPS_MERGE_WRITE
    }

    fn writer_opts(&self, expected: usize) -> WriterOptions {
        // Delta output is the option AND the durable capability: the bit
        // reaches the manifest before the first byte using it exists, so a
        // binary too old to decode the footer flag refuses the whole database
        // rather than reading delta bytes as legacy ones.
        let prefix_delta = self.opts.enable_prefix_delta_keys
            && self.ctx.caps.load(Ordering::SeqCst) & crate::format::CAPS_PREFIX_DELTA_WRITE
                == crate::format::CAPS_PREFIX_DELTA_WRITE;
        WriterOptions {
            // Flush and ingestion always write L0.
            compression: self.opts.compression_for_level(0),
            compression_rules: self.opts.compression_rules.clone(),
            cmp: self.cmp.clone(),
            enable_bloom: self.opts.enable_bloom_filter,
            // `bottom = false` unconditionally. L0 *is* the bottom level of a
            // young family (`levels` starts as one empty level), so honouring
            // `optimize_filters_for_hits` here would strip the filter from
            // every table such a family has. That option is about compaction
            // output, not about the tables reads hit first.
            // The family's depth now is what auto allocation measures L0
            // against: a young, one-level family's L0 is its bottom.
            bloom_fpr: self.opts.bloom_fpr_in_shape(
                0,
                false,
                self.with_levels(|levels| levels.len().saturating_sub(1)) as u32,
            ),
            klog_value_threshold: self.opts.klog_value_threshold,
            block_size: self.opts.data_block_size,
            expected_entries: expected,
            use_btree: self.opts.use_btree,
            restart_interval: self.opts.block_restart_interval,
            // Extended (kind-bearing) entries are a table-level property fixed
            // at writer construction, and the aux block that carries range
            // fragments only exists on an extended table. A database holding
            // CAP_RANGE_DELETES therefore writes every new table extended,
            // whether or not this particular one ends up with a fragment — the
            // alternative is knowing the answer before the merge has run. A
            // family with a merge operator needs the same layout for a second
            // reason: its operands have nowhere but the kind field to live. A
            // delta table is an extended table too, and `Writer` sets both
            // footer flags.
            extended_entries: self.range_deletes_enabled() || self.merge_writes_enabled(),
            prefix_delta,
        }
    }

    /// A writer for this CF's flush/ingest output, already carrying the
    /// database's IO limiter.
    fn new_writer(&self, klog: &str, expected: usize) -> Result<Writer> {
        Ok(
            Writer::new(klog, self.writer_opts(expected))?
                .with_limiter(self.ctx.io_limiter.clone()),
        )
    }

    fn key_in_range(th: &SstHandle, cmp: &ComparatorRef, user_key: &[u8]) -> bool {
        cmp.compare(user_key, &th.meta.min_key).is_ge()
            && cmp.compare(user_key, &th.meta.max_key).is_le()
    }

    fn find_overlapping(
        level: &[Arc<SstHandle>],
        cmp: &ComparatorRef,
        user_key: &[u8],
    ) -> Option<usize> {
        let (mut lo, mut hi) = (0, level.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if cmp.compare(&level[mid].meta.max_key, user_key).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo < level.len() && cmp.compare(user_key, &level[lo].meta.min_key).is_ge() {
            Some(lo)
        } else {
            None
        }
    }

    /// The **gap owner** at `user_key` in a sorted level: the table whose
    /// fragment interval may contain the key even though its point bounds do
    /// not.
    ///
    /// Fragments are clipped at write time to the interval each output owns —
    /// `[o_i.min_key, o_{i+1}.min_key)` — so at level >= 1 the intervals are
    /// disjoint and ordered exactly as the point bounds are. That leaves one
    /// table [`find_overlapping`] can miss: the one *left* of the key, which
    /// owns everything from its own first point key up to the next table's.
    /// Consulting it (and only it) is what makes the read path complete without
    /// weakening the binary search every other caller relies on.
    ///
    /// `point` is the index [`find_overlapping`] returned, and `lo` its
    /// internal landing position (the first table with `max_key >= user_key`).
    fn gap_owner(
        level: &[Arc<SstHandle>],
        cmp: &ComparatorRef,
        user_key: &[u8],
        point: Option<usize>,
        lo: usize,
    ) -> Option<usize> {
        let candidate = match point {
            Some(i) => i.checked_sub(1)?,
            // `find_overlapping` found nothing: either the key is past every
            // table (`lo == len`) or it fell in the gap ahead of `lo`. Both
            // make `lo - 1` the last table whose `max_key` is below the key.
            None => lo.checked_sub(1)?,
        };
        let meta = &level[candidate].meta;
        // One comparison against zero for every legacy or point-only table.
        if !meta.has_ranges() {
            return None;
        }
        // `range_max_key` is a fragment `end`, and ends are exclusive.
        let end = meta.range_max_key.as_deref()?;
        cmp.compare(user_key, end).is_lt().then_some(candidate)
    }

    /// [`find_overlapping`], also returning the binary search's landing
    /// position so [`gap_owner`](Self::gap_owner) can use it.
    fn find_overlapping_at(
        level: &[Arc<SstHandle>],
        cmp: &ComparatorRef,
        user_key: &[u8],
    ) -> (Option<usize>, usize) {
        let (mut lo, mut hi) = (0, level.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if cmp.compare(&level[mid].meta.max_key, user_key).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let hit = (lo < level.len() && cmp.compare(user_key, &level[lo].meta.min_key).is_ge())
            .then_some(lo);
        (hit, lo)
    }

    /// `state.read()` for the point-read planners, counted under `cfg(test)`
    /// so a test can pin how many snapshots a batch takes.
    #[inline]
    fn read_point_state(&self) -> parking_lot::RwLockReadGuard<'_, CfState> {
        #[cfg(test)]
        self.point_state_reads.fetch_add(1, Ordering::Relaxed);
        self.state.read()
    }

    /// Point-read state acquisitions so far.
    #[cfg(test)]
    fn point_state_reads(&self) -> u64 {
        self.point_state_reads.load(Ordering::Relaxed)
    }

    fn point_read_sources(&self, user_key: &[u8]) -> PointReadSources {
        let s = self.read_point_state();
        let mut tables = SmallVec::new();
        let mut range_tables: SmallVec<[Arc<SstHandle>; 2]> = SmallVec::new();
        for th in &s.levels[0] {
            if Self::key_in_range(th, &self.cmp, user_key) {
                tables.push(th.clone());
            } else if th.meta.has_ranges() && th.meta.span_contains(&self.cmp, user_key) {
                // L0 tables overlap each other, so there is no gap-owner rule
                // here: every table whose SPAN contains the key contributes.
                range_tables.push(th.clone());
            }
        }
        for lvl in s.levels.iter().skip(1) {
            let (hit, lo) = Self::find_overlapping_at(lvl, &self.cmp, user_key);
            if let Some(i) = hit {
                tables.push(lvl[i].clone());
            }
            if let Some(g) = Self::gap_owner(lvl, &self.cmp, user_key, hit, lo) {
                range_tables.push(lvl[g].clone());
            }
        }
        PointReadSources {
            mem: s.mem.clone(),
            imms: s.imm.clone(),
            tables,
            range_tables,
        }
    }

    /// Whether the shared unified store holds a range tombstone. `false` — and
    /// free — for a family not in unified mode.
    #[inline]
    fn unified_has_ranges(&self) -> bool {
        self.ctx.unified.as_ref().is_some_and(|u| u.has_ranges())
    }

    /// Greatest range-delete sequence at or below `read_seq` covering
    /// `user_key`, across every source of this read.
    ///
    /// Max-over-sources, not a heap: only the greatest sequence decides whether
    /// the point version survives, so there is nothing to order. Returns
    /// `None` immediately when no source carries a fragment.
    fn covering_range_seq(
        &self,
        sources: &PointReadSources,
        user_key: &[u8],
        read_seq: u64,
    ) -> Result<Option<u64>> {
        if !sources.has_ranges() && !self.unified_has_ranges() {
            return Ok(None);
        }
        let mut best: Option<u64> = None;
        let mut consulted: u64 = 0;
        let mut take = |seq: Option<u64>| {
            consulted += 1;
            if let Some(s) = seq {
                best = Some(best.map_or(s, |b: u64| b.max(s)));
            }
        };
        if let Some(u) = &self.ctx.unified {
            take(u.covering_seq(self.id, user_key, read_seq));
        }
        take(sources.mem.ranges().covering_seq(user_key, read_seq));
        for imm in &sources.imms {
            take(imm.mem.ranges().covering_seq(user_key, read_seq));
        }
        for th in sources.tables.iter().chain(sources.range_tables.iter()) {
            if !th.meta.has_ranges() {
                continue;
            }
            take(th.reader()?.covering_seq(user_key, read_seq));
        }
        // Cheap enough to be unconditional: the early return above already took
        // every family that never issued a range delete out of this function.
        let masked = u64::from(best.is_some());
        crate::perf::bump(|p| {
            p.range_sources += consulted;
            p.range_masked += masked;
        });
        Ok(best)
    }

    fn consider_sstables<S: PointSink>(
        &self,
        candidate: &mut S,
        tables: &[Arc<SstHandle>],
        user_key: &[u8],
        read_seq: u64,
        now: i64,
        early_exit: bool,
    ) -> Result<()> {
        for th in tables {
            // EARLY EXIT (wavesdb 5ef39df): a table cannot hold a version newer
            // than its `max_seq`, and `consider` only ever replaces the
            // candidate with a strictly newer one — so once the candidate's
            // sequence reaches a table's `max_seq`, probing it cannot change
            // the answer. The gate is per table rather than a `break`, because
            // position does not order sequences: an ingestion carries the
            // sequence reserved at its *start*, so a table flushed later (and
            // stored above it) can hold an older version of the same key. See
            // `tests/read_early_exit.rs`.
            //
            // Range tombstones and merge chains are unaffected: coverage is
            // collected from every source before this loop (and has already
            // been folded into `candidate`), and a winning operand sends the
            // read down `fold_point_chain`, which walks every table itself.
            if early_exit && candidate.found() && th.meta.max_seq <= candidate.seq() {
                continue;
            }
            // One bloom hash + one check per table; the probe below skips the
            // filter (it was just consulted).
            let rd = th.reader()?;
            let h = rd.bloom_hash(user_key);
            // Counted for every candidate, filter or not: `sstable_probes ==
            // bloom_probes - bloom_negatives` is then an invariant a test can
            // assert without knowing the false-positive rate.
            crate::perf::bump(|p| p.bloom_probes += 1);
            if !rd.bloom_may_contain_hash(h) {
                self.bloom_skips.fetch_add(1, Ordering::Relaxed);
                crate::perf::bump(|p| p.bloom_negatives += 1);
                continue;
            }
            self.sst_probes.fetch_add(1, Ordering::Relaxed);
            crate::perf::bump(|p| p.sstable_probes += 1);
            candidate.probe_table(&rd, user_key, read_seq, now)?;
        }
        Ok(())
    }

    /// Gather every version of `user_key` this table holds at or below
    /// `read_seq`, newest first, stopping at (and including) the first base.
    ///
    /// Driven through an [`SstIterator`] rather than the point-read block walk:
    /// a key's versions are contiguous in internal order but may straddle a
    /// block boundary, and the iterator already handles block transitions and
    /// both entry layouts. Only a key whose newest visible version is an
    /// operand reaches this, so the extra iterator per candidate table is paid
    /// by chains and by nothing else.
    fn collect_table_chain(
        &self,
        th: &Arc<SstHandle>,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
        out: &mut Vec<ChainVersion>,
    ) -> Result<()> {
        let rd = th.reader()?;
        // The bloom and probe counters were already charged by the ordinary
        // candidate pass that decided this key needs folding; counting them
        // again would double every probe for a merge family.
        if !rd.bloom_may_contain_hash(rd.bloom_hash(user_key)) {
            return Ok(());
        }
        let mut it = rd.iter();
        // `(user_key, read_seq)` lands on the newest version visible at
        // `read_seq` — entries are ordered user key ascending, sequence
        // descending.
        it.seek(user_key, read_seq);
        while it.valid() {
            if self.cmp.compare(it.user_key(), user_key) != std::cmp::Ordering::Equal {
                break;
            }
            let seq = it.seq();
            if it.kind() == crate::format::KIND_MERGE {
                out.push(ChainVersion {
                    seq,
                    merge: true,
                    value: Some(it.value()?),
                });
                it.next();
                continue;
            }
            let dead = it.is_tombstone() || (it.ttl() != 0 && it.ttl() <= now);
            out.push(ChainVersion {
                seq,
                merge: false,
                value: if dead { None } else { Some(it.value()?) },
            });
            break; // a base terminates this table's contribution
        }
        // A chain that straddles a block boundary reads on into the next
        // block; if that block fails, the older operands (and the base) are
        // missing, and folding what was gathered would be a wrong answer.
        if let Some(error) = it.err() {
            return Err(error.duplicate());
        }
        Ok(())
    }

    /// Gather and fold `user_key`'s whole operand chain across `sources`.
    ///
    /// Only reached for a key whose newest visible version really is an operand
    /// — see [`get`](Self::get).
    #[allow(clippy::too_many_arguments)]
    fn fold_point_chain<'a>(
        &self,
        op: &Arc<dyn crate::config::MergeOperator>,
        mem: &Memtable,
        imms: &[Arc<ImmMemtable>],
        tables: impl std::iter::Iterator<Item = &'a Arc<SstHandle>>,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
        range_floor: Option<u64>,
    ) -> Result<Vec<u8>> {
        let mut versions: Vec<ChainVersion> = Vec::new();
        // A range delete is, for this one key, a tombstone at its sequence.
        // Injecting it as a deleted base is the whole composition of 1.1 with
        // 1.2: `fold_chain` sorts by sequence and stops at the first base, so
        // operands above the span fold onto nothing and everything at or below
        // it — operands and base alike — is masked, with no second rule.
        if let Some(seq) = range_floor {
            versions.push(ChainVersion {
                seq,
                merge: false,
                value: None,
            });
        }
        // Newest source first, so equal sequences (which only a transaction
        // overlay can produce) keep the newer source's version.
        if let Some(u) = &self.ctx.unified {
            u.chain(self.id, user_key, read_seq, now, collect_chain(&mut versions));
        }
        mem.chain(user_key, read_seq, now, collect_chain(&mut versions));
        for imm in imms.iter().rev() {
            imm.mem
                .chain(user_key, read_seq, now, collect_chain(&mut versions));
        }
        for th in tables {
            self.collect_table_chain(th, user_key, read_seq, now, &mut versions)?;
        }
        fold_chain(op, user_key, &mut versions)
    }

    /// Resolve `user_key` as of `read_seq`. Returns the value, or `NotFound`.
    ///
    /// A merge family takes the same candidate pass as everyone else and only
    /// walks the chain when the winning version turns out to be an operand.
    /// That is exact, not an approximation: the fold rule reads the versions of
    /// a key newest first and stops at the first base, so if the newest visible
    /// version across all sources is not an operand, the chain has no operand
    /// above its base and the ordinary answer *is* the folded one.
    pub(crate) fn get(&self, user_key: &[u8], read_seq: u64) -> Result<Vec<u8>> {
        self.get_impl(user_key, read_seq, true)
    }

    /// [`get`](Self::get) with the `max_seq` early exit switched off: every
    /// candidate table is probed. The reference the randomized oracle holds the
    /// early exit to.
    #[cfg(test)]
    pub(crate) fn get_exhaustive(&self, user_key: &[u8], read_seq: u64) -> Result<Vec<u8>> {
        self.get_impl(user_key, read_seq, false)
    }

    fn get_impl(&self, user_key: &[u8], read_seq: u64, early_exit: bool) -> Result<Vec<u8>> {
        self.point_reads.fetch_add(1, Ordering::Relaxed);
        let now = coarse_now_nanos();
        let sources = self.point_read_sources(user_key);
        let mut candidate = PointReadCandidate::default();
        self.resolve_point(&mut candidate, &sources, user_key, read_seq, now, early_exit)?;
        if candidate.kind == crate::format::KIND_MERGE {
            return self.fold_winning_operand(&sources, user_key, read_seq, now, candidate.range_floor);
        }
        candidate.finish()
    }

    /// [`get`](Self::get), **appending** the value to `out` instead of
    /// allocating one. Returns the value's length; on `NotFound` or any error
    /// `out` is exactly as it was.
    ///
    /// No allocation on a hit beyond `out`'s own growth — except for a merge
    /// family whose winning version is an operand, where the operator's fold
    /// produces a fresh value by construction.
    pub(crate) fn get_into(&self, user_key: &[u8], read_seq: u64, out: &mut Vec<u8>) -> Result<usize> {
        self.point_reads.fetch_add(1, Ordering::Relaxed);
        let now = coarse_now_nanos();
        let sources = self.point_read_sources(user_key);
        let start = out.len();
        let mut candidate = BufCandidate::new(out);
        if let Err(e) = self.resolve_point(&mut candidate, &sources, user_key, read_seq, now, true) {
            candidate.out.truncate(start);
            return Err(e);
        }
        if candidate.kind == crate::format::KIND_MERGE {
            let floor = candidate.range_floor;
            candidate.out.truncate(start);
            let value = self.fold_winning_operand(&sources, user_key, read_seq, now, floor)?;
            out.extend_from_slice(&value);
            return Ok(value.len());
        }
        candidate.finish()
    }

    /// The candidate pass shared by `get` and `get_into`: every memtable
    /// source newest first, then range coverage, then the tables (with the
    /// `max_seq` early exit when `early_exit`).
    fn resolve_point<S: PointSink>(
        &self,
        candidate: &mut S,
        sources: &PointReadSources,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
        early_exit: bool,
    ) -> Result<()> {
        // Unified-memtable mode: the shared store holds this CF's hot data.
        if let Some(u) = &self.ctx.unified {
            crate::perf::bump(|p| p.memtable_probes += 1);
            candidate.probe_unified(u, self.id, user_key, read_seq, now);
        }
        crate::perf::bump(|p| p.memtable_probes += 1);
        candidate.probe_mem(&sources.mem, user_key, read_seq, now);
        for imm in sources.imms.iter().rev() {
            crate::perf::bump(|p| p.memtable_probes += 1);
            candidate.probe_mem(&imm.mem, user_key, read_seq, now);
        }
        // Coverage before the tables, as `multi_get` does: `mask` and
        // `consider` commute (each keeps the strictly newer of the two, and a
        // span never shares a sequence with a point write), and applying the
        // span first lets a range delete newer than every table end the read
        // without a single point probe.
        candidate.apply_mask(self.covering_range_seq(sources, user_key, read_seq)?);
        self.consider_sstables(
            candidate,
            &sources.tables,
            user_key,
            read_seq,
            now,
            early_exit,
        )
    }

    /// Resolve a key whose winning version is a merge operand: walk and fold
    /// its whole chain (see [`get`](Self::get) for why the winner decides).
    fn fold_winning_operand(
        &self,
        sources: &PointReadSources,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
        range_floor: Option<u64>,
    ) -> Result<Vec<u8>> {
        if let Some(op) = self.opts.merge_operator.as_ref() {
            return self.fold_point_chain(
                op,
                &sources.mem,
                &sources.imms,
                sources.tables.iter(),
                user_key,
                read_seq,
                now,
                range_floor,
            );
        }
        // An operand with no operator can only come from a hand-edited
        // config blob: `resolve_merge_operator` fails the open otherwise.
        Err(OndaError::Corruption(format!(
            "column family {:?} holds a merge operand for key {:?} but has no merge operator",
            self.name,
            String::from_utf8_lossy(user_key)
        )))
    }

    /// [`point_read_sources`](Self::point_read_sources) for a whole batch, in
    /// **one** `state` acquisition.
    ///
    /// Grouping happens here rather than at read time because the candidate
    /// set is a property of the level layout, and the layout must not change
    /// underneath a batch: a flush or compaction landing between two keys of
    /// the same call would otherwise let them see different table sets.
    fn batch_read_sources(&self, keys: &[&[u8]]) -> BatchReadSources {
        let s = self.read_point_state();
        let mut tables = BatchTables::new();

        // L0 is unsorted and overlapping: every table is a candidate for every
        // key in its range, and the stored order is newest-first.
        let mut range_tables: BatchTables = SmallVec::new();
        for th in &s.levels[0] {
            let mut idxs: SmallVec<[usize; 8]> = SmallVec::new();
            let mut range_idxs: SmallVec<[usize; 8]> = SmallVec::new();
            for (i, key) in keys.iter().enumerate() {
                if Self::key_in_range(th, &self.cmp, key) {
                    idxs.push(i);
                } else if th.meta.has_ranges() && th.meta.span_contains(&self.cmp, key) {
                    range_idxs.push(i);
                }
            }
            if !idxs.is_empty() {
                tables.push((th.clone(), idxs));
            }
            if !range_idxs.is_empty() {
                range_tables.push((th.clone(), range_idxs));
            }
        }

        // Levels below L0 are sorted and disjoint, so each key selects at most
        // one table per level. Collecting `(table position, key)` and sorting
        // by position keeps the level's contribution in key order and, more
        // importantly, deterministic.
        let mut hits: SmallVec<[(usize, usize); 16]> = SmallVec::new();
        for lvl in s.levels.iter().skip(1) {
            if lvl.is_empty() {
                continue;
            }
            hits.clear();
            let mut gaps: SmallVec<[(usize, usize); 16]> = SmallVec::new();
            for (i, key) in keys.iter().enumerate() {
                let (hit, lo) = Self::find_overlapping_at(lvl, &self.cmp, key);
                if let Some(t) = hit {
                    hits.push((t, i));
                }
                if let Some(g) = Self::gap_owner(lvl, &self.cmp, key, hit, lo) {
                    gaps.push((g, i));
                }
            }
            hits.sort_unstable();
            group_level_hits(&mut tables, lvl, &hits);
            if !gaps.is_empty() {
                gaps.sort_unstable();
                group_level_hits(&mut range_tables, lvl, &gaps);
            }
        }

        BatchReadSources {
            mem: s.mem.clone(),
            imms: s.imm.clone(),
            tables,
            range_tables,
        }
    }

    /// Greatest range-delete sequence covering each key of the batch, or
    /// `None` for the whole batch when no source carries a fragment.
    ///
    /// Shaped like the point pass: one reader opened per candidate table,
    /// then every key that table covers answered from it.
    fn batch_covering_range_seqs(
        &self,
        sources: &BatchReadSources,
        keys: &[&[u8]],
        read_seq: u64,
        cands: &mut [PointReadCandidate],
        errs: &mut [Option<OndaError>],
    ) {
        let any = !sources.range_tables.is_empty()
            || !sources.mem.ranges().is_empty()
            || sources.imms.iter().any(|i| !i.mem.ranges().is_empty())
            || sources.tables.iter().any(|(t, _)| t.meta.has_ranges())
            || self.unified_has_ranges();
        if !any {
            return;
        }
        let mut covering: Vec<Option<u64>> = vec![None; keys.len()];
        let take = |cover: &mut Vec<Option<u64>>, i: usize, seq: Option<u64>| {
            if let Some(s) = seq {
                cover[i] = Some(cover[i].map_or(s, |b: u64| b.max(s)));
            }
        };
        for (i, key) in keys.iter().enumerate() {
            if let Some(u) = &self.ctx.unified {
                take(&mut covering, i, u.covering_seq(self.id, key, read_seq));
            }
            take(
                &mut covering,
                i,
                sources.mem.ranges().covering_seq(key, read_seq),
            );
            for imm in &sources.imms {
                take(
                    &mut covering,
                    i,
                    imm.mem.ranges().covering_seq(key, read_seq),
                );
            }
        }
        for (th, idxs) in sources.tables.iter().chain(sources.range_tables.iter()) {
            if !th.meta.has_ranges() {
                continue;
            }
            match th.reader() {
                Ok(rd) => {
                    for &i in idxs.iter() {
                        take(&mut covering, i, rd.covering_seq(keys[i], read_seq));
                    }
                }
                // A fragment source that cannot be opened is a source that
                // might have deleted the key: failing the affected results is
                // the only answer that is not a short answer looking complete.
                Err(e) => {
                    for &i in idxs.iter() {
                        if errs[i].is_none() {
                            errs[i] = Some(e.duplicate());
                        }
                    }
                }
            }
        }
        for (c, seq) in cands.iter_mut().zip(covering) {
            c.mask(seq);
        }
    }

    /// Resolve every key of `idxs` against one already-open table, fetching
    /// each distinct data block once.
    ///
    /// `scratch` is the planner's only per-table allocation and is reused
    /// across tables by the caller. Sorting it by block index makes the targets
    /// of a block contiguous, so the block is materialized once and dropped
    /// before the next one is read — the memory the walk holds live is one
    /// block, not one per key.
    #[allow(clippy::too_many_arguments)]
    fn resolve_table_batch(
        &self,
        rd: &Reader,
        max_seq: u64,
        keys: &[&[u8]],
        idxs: &[usize],
        read_seq: u64,
        now: i64,
        cands: &mut [PointReadCandidate],
        errs: &mut [Option<OndaError>],
        scratch: &mut SmallVec<[(usize, usize); 16]>,
    ) {
        scratch.clear();
        for &i in idxs {
            // `get`'s early exit, per key: a version at or above this table's
            // `max_seq` cannot be displaced by anything the table holds.
            if Self::resolved_above(&cands[i], max_seq) {
                continue;
            }
            // One bloom hash + one check per (key, table), exactly as
            // `consider_sstables` does — the counters must stay comparable
            // between a batch and the N gets it replaces.
            let h = rd.bloom_hash(keys[i]);
            crate::perf::bump(|p| p.bloom_probes += 1);
            if !rd.bloom_may_contain_hash(h) {
                self.bloom_skips.fetch_add(1, Ordering::Relaxed);
                crate::perf::bump(|p| p.bloom_negatives += 1);
                continue;
            }
            self.sst_probes.fetch_add(1, Ordering::Relaxed);
            crate::perf::bump(|p| p.sstable_probes += 1);
            let bi = rd.find_block(keys[i], read_seq);
            if bi >= rd.data_block_count() {
                // Sorts past the last block: no version here, no block to read.
                continue;
            }
            scratch.push((bi, i));
        }
        // By block first; the key index only breaks ties, so a duplicated key
        // stays adjacent to its twin and both ride the same fetch.
        scratch.sort_unstable();

        // Bounded parallel fetch (P5) is considered only where a block read is
        // slow enough to be worth a thread hand-off: see
        // `Reader::block_read_is_remote`. Blocks are fetched a window at a
        // time, so the batch never holds more than one window of blocks live
        // however many keys it carries.
        let limit = self.ctx.block_reads.limit();
        let window = if limit > 1 { limit * 4 } else { usize::MAX };
        let mut prefetched: Vec<(usize, Result<crate::sst::Block>)> = Vec::new();
        let mut next_unplanned = 0; // first scratch position not yet windowed
        let mut pos = 0;
        while pos < scratch.len() {
            let bi = scratch[pos].0;
            if limit > 1 && pos >= next_unplanned {
                prefetched.clear();
                next_unplanned = self.prefetch_window(rd, scratch, pos, window, &mut prefetched);
            }
            let mut end = pos;
            while end < scratch.len() && scratch[end].0 == bi {
                end += 1;
            }
            let group = &scratch[pos..end];
            pos = end;
            // The whole point of the batch: one fetch, `group.len()` lookups.
            crate::perf::bump(|p| p.multiget_blocks_deduped += (group.len() - 1) as u64);

            // A block the window fetched is used as is (it is not re-read even
            // if the cache is off or already evicted it); its error, if any,
            // fails exactly this group, as a sequential read's would.
            let fetched = prefetched
                .iter()
                .position(|(b, _)| *b == bi)
                .map(|at| prefetched.swap_remove(at).1);
            let block = match fetched {
                Some(Ok(crate::sst::Block::Owned(a))) => Ok(crate::sst::BlockRef::Owned(a)),
                Some(Err(e)) => Err(e),
                // An mmap view cannot come from a slow tier; re-reading is
                // merely the correct fallback if one ever did.
                #[cfg(feature = "mmap-reads")]
                Some(Ok(crate::sst::Block::Mapped { .. })) => rd.read_data_block_local(bi),
                // A slow-tier read the window left inline (too few to fan
                // out) still takes a permit, so the bound holds for every
                // slow-tier read batches issue, not only the parallel ones.
                None if limit > 1 && rd.block_read_is_remote(bi) => {
                    let (_permit, waited) = self.ctx.block_reads.acquire();
                    crate::perf::bump(|p| p.multiget_io_waits += u64::from(waited));
                    rd.read_data_block_local(bi)
                }
                None => rd.read_data_block_local(bi),
            };
            let block = match block {
                Ok(b) => b,
                Err(e) => {
                    Self::fail_group(group, cands, errs, &e);
                    continue;
                }
            };
            let (raw, restarts) = match rd.split_block(block.bytes()) {
                Ok(parts) => parts,
                Err(e) => {
                    Self::fail_group(group, cands, errs, &e);
                    continue;
                }
            };
            for &(_, i) in group {
                // The restart search stays per key: it is a binary search on
                // `(user_key, seq)` and every key has a different target. Only
                // the fetch and decompression above were shared.
                let found = rd
                    .restart_scan_offset(raw, restarts, keys[i], read_seq)
                    .and_then(|off| rd.scan_point_entry(raw, off, keys[i], read_seq, now));
                match found {
                    Ok((value, seq, found, deleted, kind)) => {
                        cands[i].consider(value, seq, found, deleted, kind)
                    }
                    Err(e) => Self::record_read_error(i, cands, errs, &e),
                }
            }
        }
    }

    /// Fewest slow-tier block reads in one window that are worth handing to
    /// worker threads (wavesdb's measured threshold: below it the hand-off
    /// costs more than the overlap buys).
    const PARALLEL_BLOCK_MIN: usize = 4;

    /// Plan the window of up to `window` distinct blocks starting at
    /// `scratch[pos]` and, if at least [`Self::PARALLEL_BLOCK_MIN`] of them
    /// would be slow-tier reads, fetch those with bounded parallelism into
    /// `out` as `(block index, result)`. Returns the scratch position just past
    /// the window.
    ///
    /// Workers take a permit from the database-wide semaphore per read, so
    /// concurrent batches share one bound. The calling thread is a worker too;
    /// a spawn failure just means fewer helpers. Each worker counts into its
    /// own perf scope, merged into the caller's.
    fn prefetch_window(
        &self,
        rd: &Reader,
        scratch: &[(usize, usize)],
        pos: usize,
        window: usize,
        out: &mut Vec<(usize, Result<crate::sst::Block>)>,
    ) -> usize {
        let mut cold: SmallVec<[usize; 16]> = SmallVec::new();
        let mut distinct = 0;
        let mut end = pos;
        let mut last = usize::MAX;
        while end < scratch.len() {
            let bi = scratch[end].0;
            if bi != last {
                if distinct == window {
                    break;
                }
                distinct += 1;
                last = bi;
                if rd.block_read_is_remote(bi) {
                    cold.push(bi);
                }
            }
            end += 1;
        }
        if cold.len() < Self::PARALLEL_BLOCK_MIN {
            return end;
        }
        let sem = &*self.ctx.block_reads;
        let workers = sem.limit().min(cold.len());
        let next = std::sync::atomic::AtomicUsize::new(0);
        let want_perf = crate::perf::active();
        let work = || {
            let scope = want_perf.then(crate::perf::enter);
            let mut got = Vec::new();
            loop {
                let k = next.fetch_add(1, Ordering::Relaxed);
                let Some(&bi) = cold.get(k) else { break };
                let (permit, waited) = sem.acquire();
                let r = rd.read_data_block(bi);
                drop(permit);
                crate::perf::bump(|p| {
                    p.multiget_parallel_reads += 1;
                    p.multiget_io_waits += u64::from(waited);
                });
                got.push((bi, r));
            }
            (got, scope.map(|s| s.finish()))
        };
        std::thread::scope(|s| {
            let helpers: Vec<_> = (1..workers)
                .filter_map(|n| {
                    std::thread::Builder::new()
                        .name(format!("onda-mget-{n}"))
                        .spawn_scoped(s, work)
                        .ok()
                })
                .collect();
            let mut results = vec![work()];
            for h in helpers {
                // A worker that panicked re-raises here, as the same read on
                // the calling thread would have.
                results.push(h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)));
            }
            for (got, perf) in results {
                if let Some(perf) = perf {
                    crate::perf::bump(|p| p.absorb(&perf));
                }
                out.extend(got);
            }
        });
        end
    }

    /// Whether `cand` already holds a version no table with this `max_seq` can
    /// replace — the batch form of `consider_sstables`' early exit.
    #[inline]
    fn resolved_above(cand: &PointReadCandidate, max_seq: u64) -> bool {
        cand.found && max_seq <= cand.seq
    }

    /// Attribute one source failure to every key of `group` that still needs
    /// that source.
    fn fail_group(
        group: &[(usize, usize)],
        cands: &[PointReadCandidate],
        errs: &mut [Option<OndaError>],
        e: &OndaError,
    ) {
        for &(_, i) in group {
            Self::record_read_error(i, cands, errs, e);
        }
    }

    /// Record a source failure against result `i` — unless a **strictly newer**
    /// source has already resolved that key.
    ///
    /// Sources are consulted newest-first and a newer source always holds a
    /// newer version of a key, so a key already resolved when an older source
    /// fails did not need that source: failing it would report a corruption
    /// that could not have changed the answer. This is the one place a batch
    /// deliberately differs from N `get`s, which propagate the first error of
    /// any candidate table regardless (documented on
    /// [`crate::DB::multi_get`]). The first error for a key wins; later ones
    /// cannot make the result any less erroneous.
    fn record_read_error(
        i: usize,
        cands: &[PointReadCandidate],
        errs: &mut [Option<OndaError>],
        e: &OndaError,
    ) {
        if cands[i].found || errs[i].is_some() {
            return;
        }
        errs[i] = Some(e.duplicate());
    }

    /// Resolve `keys` as of `read_seq`, one result per key in input order.
    ///
    /// The batch equivalent of [`get`](Self::get): one `now`, one source
    /// snapshot, and one data-block fetch per distinct block, however many keys
    /// of the batch land in it.
    pub(crate) fn multi_get(&self, keys: &[&[u8]], read_seq: u64) -> Vec<Result<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        self.point_reads
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        // One clock reading for the batch: two keys of one call must not
        // disagree about whether a TTL has expired.
        let now = coarse_now_nanos();
        let sources = self.batch_read_sources(keys);
        let mut cands: SmallVec<[PointReadCandidate; 8]> = (0..keys.len())
            .map(|_| PointReadCandidate::default())
            .collect();
        let mut errs: SmallVec<[Option<OndaError>; 8]> = (0..keys.len()).map(|_| None).collect();

        // Memtable sources, in `get`'s order, so a per-key replay of this batch
        // meets its sources in exactly the sequence `get` would have used.
        if let Some(u) = &self.ctx.unified {
            for (i, key) in keys.iter().enumerate() {
                crate::perf::bump(|p| p.memtable_probes += 1);
                cands[i].consider_memtable(u.get(self.id, key, read_seq, now));
            }
        }
        for (i, key) in keys.iter().enumerate() {
            crate::perf::bump(|p| p.memtable_probes += 1);
            cands[i].consider_memtable(sources.mem.get(key, read_seq, now));
        }
        for imm in sources.imms.iter().rev() {
            for (i, key) in keys.iter().enumerate() {
                crate::perf::bump(|p| p.memtable_probes += 1);
                cands[i].consider_memtable(imm.mem.get(key, read_seq, now));
            }
        }

        self.batch_covering_range_seqs(&sources, keys, read_seq, &mut cands, &mut errs);
        let mut scratch: SmallVec<[(usize, usize); 16]> = SmallVec::new();
        for (th, idxs) in &sources.tables {
            // Every key this table could answer is already resolved by a
            // version it cannot beat: skip it without even opening the reader.
            if idxs
                .iter()
                .all(|&i| Self::resolved_above(&cands[i], th.meta.max_seq))
            {
                continue;
            }
            match th.reader() {
                Ok(rd) => self.resolve_table_batch(
                    &rd,
                    th.meta.max_seq,
                    keys,
                    idxs,
                    read_seq,
                    now,
                    &mut cands,
                    &mut errs,
                    &mut scratch,
                ),
                // A table that cannot even be opened fails every key that had
                // not already been resolved by a newer source.
                Err(e) => {
                    for &i in idxs.iter() {
                        Self::record_read_error(i, &cands, &mut errs, &e);
                    }
                }
            }
        }

        // Merge post-pass: exactly the keys whose winning version is an operand
        // need their chain walked (see `get` for why that test is exact). The
        // batch keeps its one clock reading and one source snapshot; only those
        // keys give up the block dedup.
        let operator = self.opts.merge_operator.as_ref();
        cands
            .into_iter()
            .zip(errs)
            .enumerate()
            .map(|(i, (c, e))| match e {
                Some(e) => Err(e),
                None if c.kind == crate::format::KIND_MERGE => match operator {
                    Some(op) => self.fold_point_chain(
                        op,
                        &sources.mem,
                        &sources.imms,
                        sources
                            .tables
                            .iter()
                            .filter(|(_, idxs)| idxs.contains(&i))
                            .map(|(th, _)| th),
                        keys[i],
                        read_seq,
                        now,
                        c.range_floor,
                    ),
                    None => Err(OndaError::Corruption(format!(
                        "column family {:?} holds a merge operand for key {:?} \
                         but has no merge operator",
                        self.name,
                        String::from_utf8_lossy(keys[i])
                    ))),
                },
                None => c.finish(),
            })
            .collect()
    }

    /// Newest committed sequence for `user_key` across all sources (ignoring
    /// snapshots), or `0` if the key has never been written.  Used for
    /// write-write conflict detection.
    pub(crate) fn peek_seq(&self, user_key: &[u8]) -> Result<u64> {
        let now = coarse_now_nanos();
        let (mem, imms, tables) = {
            let s = self.state.read();
            let mut tables: SmallVec<[Arc<SstHandle>; 4]> = SmallVec::new();
            for th in &s.levels[0] {
                if Self::key_in_range(th, &self.cmp, user_key) {
                    tables.push(th.clone());
                }
            }
            for lvl in s.levels.iter().skip(1) {
                if let Some(i) = Self::find_overlapping(lvl, &self.cmp, user_key) {
                    tables.push(lvl[i].clone());
                }
            }
            (s.mem.clone(), s.imm.clone(), tables)
        };
        let mut best = 0u64;
        if let Some(u) = &self.ctx.unified {
            let r = u.get(self.id, user_key, u64::MAX, now);
            if r.found {
                best = best.max(r.seq);
            }
        }
        let r = mem.get(user_key, u64::MAX, now);
        if r.found {
            best = best.max(r.seq);
        }
        for imm in imms.iter().rev() {
            let r = imm.mem.get(user_key, u64::MAX, now);
            if r.found {
                best = best.max(r.seq);
            }
        }
        for th in &tables {
            let (_, seq, found, ..) = th.reader()?.get(user_key, u64::MAX, now)?;
            if found {
                best = best.max(seq);
            }
        }
        Ok(best)
    }

    /// Does `[meta.min_key, meta.max_key]` overlap the declared key bounds?
    fn sst_in_bounds(
        th: &SstHandle,
        cmp: &ComparatorRef,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
    ) -> bool {
        let above_lower = match bounds.0 {
            Bound::Unbounded => true,
            Bound::Included(l) => cmp.compare(&th.meta.max_key, l).is_ge(),
            Bound::Excluded(l) => cmp.compare(&th.meta.max_key, l).is_gt(),
        };
        let below_upper = match bounds.1 {
            Bound::Unbounded => true,
            Bound::Included(u) => cmp.compare(&th.meta.min_key, u).is_le(),
            Bound::Excluded(u) => cmp.compare(&th.meta.min_key, u).is_lt(),
        };
        above_lower && below_upper
    }

    fn append_memtable_children(
        &self,
        children: &mut Vec<ChildIter>,
        state: &CfState,
        extra: Option<Arc<Memtable>>,
    ) {
        if let Some(extra) = extra {
            children.push(ChildIter::Mem(extra.iter()));
        }
        // Unified mode: overlay this CF's slice of the shared memtable.
        if let Some(u) = &self.ctx.unified {
            if self.cmp.is_bytewise() {
                children.extend(
                    u.iterators_for_cf(self.id)
                        .into_iter()
                        .map(ChildIter::Unified),
                );
            } else {
                let entries = u.entries_for_cf(self.id);
                if !entries.is_empty() {
                    let overlay = Memtable::new(self.cmp.clone());
                    for e in entries {
                        overlay.put(&e.user_key, e.value, e.seq, e.ttl, e.kind);
                    }
                    children.push(ChildIter::Mem(overlay.iter()));
                }
            }
        }
        children.push(ChildIter::Mem(state.mem.iter()));
        for imm in state.imm.iter().rev() {
            children.push(ChildIter::Mem(imm.mem.iter()));
        }
    }

    fn append_l0_children(
        &self,
        children: &mut Vec<ChildIter>,
        level: &[Arc<SstHandle>],
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
    ) -> Result<()> {
        for th in level {
            if Self::sst_in_bounds(th, &self.cmp, bounds) {
                children.push(ChildIter::Sst(th.reader()?.iter()));
            }
        }
        Ok(())
    }

    fn sorted_level_start(&self, level: &[Arc<SstHandle>], lower: Bound<&[u8]>) -> usize {
        match lower {
            Bound::Unbounded => 0,
            Bound::Included(key) => {
                level.partition_point(|th| self.cmp.compare(&th.meta.max_key, key).is_lt())
            }
            Bound::Excluded(key) => {
                level.partition_point(|th| self.cmp.compare(&th.meta.max_key, key).is_le())
            }
        }
    }

    fn append_sorted_level_children(
        &self,
        children: &mut Vec<ChildIter>,
        level: &[Arc<SstHandle>],
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
    ) -> Result<()> {
        let start = self.sorted_level_start(level, bounds.0);
        for th in &level[start..] {
            if !key_is_below_upper(&self.cmp, &th.meta.min_key, bounds.1) {
                break;
            }
            children.push(ChildIter::Sst(th.reader()?.iter()));
        }
        Ok(())
    }

    fn iterator_children(
        &self,
        state: &CfState,
        extra: Option<Arc<Memtable>>,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
    ) -> Result<Vec<ChildIter>> {
        let mut children = Vec::new();
        self.append_memtable_children(&mut children, state, extra);
        self.append_l0_children(&mut children, &state.levels[0], bounds)?;
        // Levels >= 1 are sorted by key and disjoint, so the overlapping
        // tables form one contiguous run: binary-search its start, then walk
        // until the upper bound. Walking every table made an empty bounded scan
        // O(total tables), including reader opens after cache eviction.
        for level in state.levels.iter().skip(1) {
            self.append_sorted_level_children(&mut children, level, bounds)?;
        }
        Ok(children)
    }

    /// Collect the range-delete coverage a scan over `bounds` must honor.
    ///
    /// Every source that could hide a key inside the bounds contributes its
    /// fragments, **owned**: the cursors outlive any pinned data block, so
    /// invariant 8's key/value pin lifetimes are untouched.
    ///
    /// A table is consulted whenever its fragment span reaches into the
    /// bounds, which is a wider test than the point-bounds pruning
    /// `iterator_children` uses — a table whose points sort entirely below the
    /// scan can still own a fragment that reaches into it (the gap-owner rule,
    /// in its scan form).
    fn iterator_range_mask(
        &self,
        state: &CfState,
        extra: Option<&Arc<Memtable>>,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
    ) -> Result<crate::range_tombstone::RangeMask> {
        let mut mask = crate::range_tombstone::RangeMask::default();
        // Fragmentation clips to a half-open interval, so an inclusive upper
        // bound has to be widened to "everything at or below it" — which is
        // what `None` means here. Over-wide is harmless: a fragment outside the
        // bounds is never consulted, because the iterator stops at them.
        let lower = match bounds.0 {
            Bound::Unbounded => None,
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
        };
        let upper = match bounds.1 {
            Bound::Included(_) | Bound::Unbounded => None,
            Bound::Excluded(k) => Some(k),
        };
        if let Some(extra) = extra {
            if !extra.ranges().is_empty() {
                mask.push_snapshot(
                    extra.ranges().fragment_snapshot(),
                    &self.cmp,
                    lower,
                    upper,
                    None,
                );
            }
        }
        if let Some(u) = &self.ctx.unified {
            if u.has_ranges() {
                u.add_range_sources(
                    &mut mask,
                    self.id,
                    lower,
                    upper,
                    &self.ctx.range_fragment_registry,
                );
            }
        }
        for mem in std::iter::once(&state.mem).chain(state.imm.iter().map(|i| &i.mem)) {
            if !mem.ranges().is_empty() {
                mem.ranges()
                    .track_snapshots(&self.ctx.range_fragment_registry);
                mask.push_snapshot(
                    mem.ranges().fragment_snapshot(),
                    &self.cmp,
                    lower,
                    upper,
                    None,
                );
            }
        }
        for level in state.levels.iter() {
            for th in level {
                if !th.meta.has_ranges() || !self.span_in_bounds(&th.meta, bounds) {
                    continue;
                }
                mask.push_snapshot(
                    th.reader()?.range_fragment_snapshot(),
                    &self.cmp,
                    lower,
                    upper,
                    None,
                );
            }
        }
        Ok(mask)
    }

    /// Does this table's **span** (points plus fragments) reach into `bounds`?
    fn span_in_bounds(&self, meta: &SstMeta, bounds: &(Bound<&[u8]>, Bound<&[u8]>)) -> bool {
        let cmp = &self.cmp;
        let (lo, hi) = (meta.span_min(cmp), meta.span_max(cmp));
        let above_lower = match bounds.0 {
            Bound::Unbounded => true,
            Bound::Included(l) => cmp.compare(hi, l).is_ge(),
            Bound::Excluded(l) => cmp.compare(hi, l).is_gt(),
        };
        let below_upper = match bounds.1 {
            Bound::Unbounded => true,
            Bound::Included(u) => cmp.compare(lo, u).is_le(),
            Bound::Excluded(u) => cmp.compare(lo, u).is_lt(),
        };
        above_lower && below_upper
    }

    /// Build a snapshot iterator. `extra` is an optional transaction overlay
    /// memtable consulted as the newest source. `bounds` are the caller's
    /// declared key bounds: SSTables whose `[min_key, max_key]` lies entirely
    /// outside them are skipped (memtables are hash-sharded and cannot be
    /// pruned), and the returned iterator terminates at the bounds.
    pub(crate) fn new_iterator(
        &self,
        read_seq: u64,
        extra: Option<Arc<Memtable>>,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> Iterator {
        let s = self.state.read();
        // The mask is built from the SAME state snapshot as the children, so a
        // flush landing mid-construction cannot leave a scan reading points
        // whose covering fragments it never collected.
        let mask = match self.iterator_range_mask(&s, extra.as_ref(), &bounds) {
            Ok(mask) => mask,
            Err(error) => return Iterator::failed(self.cmp.clone(), error),
        };
        let children = match self.iterator_children(&s, extra, &bounds) {
            Ok(children) => children,
            // Omitting a table would return a short answer that looks
            // complete. Fail the iterator instead.
            Err(error) => return Iterator::failed(self.cmp.clone(), error),
        };
        drop(s);
        let owned = (bound_to_owned(bounds.0), bound_to_owned(bounds.1));
        Iterator::new(
            self.cmp.clone(),
            children,
            read_seq,
            coarse_now_nanos(),
            owned,
            mask,
        )
        .with_merge_operator(self.opts.merge_operator.clone())
    }

    /// Catalogued table metadata, level by level, in the order each level
    /// stores it (L0 newest-first; levels below sorted by `min_key`).
    ///
    /// The operator-facing view of what the catalog holds — including each
    /// table's range-tombstone summary
    /// ([`SstMeta::range_count`](crate::manifest::SstMeta::range_count) and
    /// friends), which is otherwise invisible from outside the engine.
    pub fn table_metadata(&self) -> Vec<Vec<SstMeta>> {
        let s = self.state.read();
        s.levels
            .iter()
            .map(|lvl| lvl.iter().map(|th| th.meta.clone()).collect())
            .collect()
    }

    pub(crate) fn range_cache_stats(&self) -> crate::range_tombstone::RangeCacheStats {
        let s = self.state.read();
        let mut stats = s.mem.ranges().stats();
        for imm in &s.imm {
            stats += imm.mem.ranges().stats();
        }
        stats
    }

    /// Whether any catalogued table of this family carries range-delete
    /// fragments (1.2) — the gate on the excise pre-pass being worth a wakeup.
    pub(crate) fn has_range_fragments(&self) -> bool {
        self.state
            .read()
            .levels
            .iter()
            .flatten()
            .any(|th| th.meta.has_ranges())
    }

    /// Markers the database-wide committed-span index currently holds.
    pub(crate) fn span_marker_count(&self) -> usize {
        self.ctx.span_index.len()
    }

    /// Range deletes committed to this column family since it was opened.
    pub(crate) fn range_deletes(&self) -> u64 {
        self.range_deletes.load(Ordering::Relaxed)
    }

    /// Count one committed range delete.
    #[inline]
    pub(crate) fn note_range_delete(&self, n: u64) {
        self.range_deletes.fetch_add(n, Ordering::Relaxed);
    }

    /// Tables retired by delete-only excise, and the bytes they held.
    pub(crate) fn excised(&self) -> (u64, u64) {
        (
            self.excised_tables.load(Ordering::Relaxed),
            self.excised_bytes.load(Ordering::Relaxed),
        )
    }

    /// Count one excise transaction's worth of retired tables.
    pub(crate) fn note_excised(&self, tables: u64, bytes: u64) {
        self.excised_tables.fetch_add(tables, Ordering::Relaxed);
        self.excised_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Snapshot the SSTable metadata for the manifest.
    pub(crate) fn snapshot_ssts(&self) -> Vec<SstMeta> {
        let s = self.state.read();
        let mut out = Vec::new();
        for lvl in &s.levels {
            for th in lvl {
                out.push(th.meta.clone());
            }
        }
        out
    }

    /// fsync this CF's active WAL (no-op when read-only / WAL-less).
    pub(crate) fn sync_wal(&self) -> Result<()> {
        let wal = self.state.read().wal.clone();
        match wal {
            Some(w) => w.sync(),
            None => Ok(()),
        }
    }

    /// Whether a flush is in progress.
    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::Relaxed)
    }
    /// Whether a compaction is in progress.
    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::Relaxed)
    }

    /// Close the active WAL and all open readers.
    pub(crate) fn close_resources(&self) {
        let mut s = self.state.write();
        s.mem.ranges().retire();
        for imm in &s.imm {
            imm.mem.ranges().retire();
        }
        if let Some(w) = s.wal.take() {
            let _ = w.close();
        }
        if self.ctx.shared_reads {
            return;
        }
        for lvl in &s.levels {
            for th in lvl {
                th.close();
            }
        }
    }

    // ---- accessors used by compaction (in compaction.rs) ----

    pub(crate) fn cmp(&self) -> ComparatorRef {
        self.cmp.clone()
    }

    pub(crate) fn dir(&self) -> &str {
        &self.dir
    }

    pub(crate) fn with_levels<R>(&self, f: impl FnOnce(&[Vec<Arc<SstHandle>>]) -> R) -> R {
        let s = self.state.read();
        f(&s.levels)
    }

    pub(crate) fn replace_levels(&self, levels: Vec<Vec<Arc<SstHandle>>>) {
        let mut s = self.state.write();
        s.levels = levels;
    }

    /// Rebuild the level set **atomically**: `f` sees the same state the result
    /// overwrites, under one exclusive lock.
    ///
    /// Compaction used to do this as `with_levels` (read lock, build, release)
    /// followed by `replace_levels` (write lock, wholesale overwrite). A flush
    /// completing in that window called [`Self::install_handles_l0`] and had
    /// its table inserted into L0 — and then the overwrite discarded it. The
    /// file stayed on disk (it was never a compaction input, so nothing deleted
    /// it) but vanished from the level set, and the manifest persisted right
    /// after recorded a database that did not contain it. Its data was
    /// unreachable, while the flush that produced it had already reclaimed its
    /// WAL on the strength of an earlier, correct manifest.
    ///
    /// That is silent committed-write loss, so the read and the write must not
    /// be separable. `f` must do in-memory work only and must not call back
    /// into anything that touches `state` — it would deadlock.
    pub(crate) fn update_levels(
        &self,
        f: impl FnOnce(&[Vec<Arc<SstHandle>>]) -> Vec<Vec<Arc<SstHandle>>>,
        _p: &crate::db::Publish,
    ) {
        let mut s = self.state.write();
        let next = f(&s.levels);
        s.levels = next;
    }

    pub(crate) fn l0_len(&self) -> usize {
        self.state.read().levels[0].len()
    }

    /// Per-level `(file_count, bytes)` plus totals, for stats and maintenance.
    pub(crate) fn level_summary(&self) -> Vec<(usize, u64)> {
        let s = self.state.read();
        s.levels
            .iter()
            .map(|lvl| {
                let bytes: u64 = lvl
                    .iter()
                    .map(|t| t.meta.klog_size + t.meta.vlog_size)
                    .sum();
                (lvl.len(), bytes)
            })
            .collect()
    }

    /// Total entries and tombstones across all SSTables.
    /// Entries currently in the active and sealed memtables.
    pub(crate) fn memtable_entries(&self) -> u64 {
        let s = self.state.read();
        let mut n = s.mem.num_entries().max(0) as u64;
        for imm in &s.imm {
            n += imm.mem.num_entries().max(0) as u64;
        }
        n
    }

    /// Approximate number of entries: per-SSTable entry counts (which still
    /// include not-yet-compacted old versions and tombstones) plus the active
    /// and sealed memtables. O(levels), no I/O. In unified-memtable mode,
    /// entries still in the shared memtable are not counted (they are only
    /// attributed to a CF at flush time).
    pub fn approximate_len(&self) -> u64 {
        let (entries, _) = self.entry_counts();
        entries + self.memtable_entries()
    }

    pub(crate) fn entry_counts(&self) -> (u64, u64) {
        let s = self.state.read();
        let mut entries = 0;
        let mut tombs = 0;
        for lvl in &s.levels {
            for t in lvl {
                entries += t.meta.num_entries;
                tombs += t.meta.num_tombstones;
            }
        }
        (entries, tombs)
    }

    /// FIFO eviction, selection half: the oldest L0 tables that put the CF back
    /// under `max_bytes`, plus any table whose klog file age exceeds `ttl`.
    ///
    /// Selects only — the level set is untouched. The removal half is
    /// [`remove_l0_tables`](Self::remove_l0_tables), which runs inside the
    /// eviction's `catalog_txn`; before 2.2 this function mutated the level set
    /// itself, which put the publication ahead of the durable edit.
    pub(crate) fn select_fifo_victims(
        &self,
        max_bytes: u64,
        ttl: std::time::Duration,
    ) -> Vec<Arc<SstHandle>> {
        let now = std::time::SystemTime::now();
        let s = self.state.read();
        // File ids are allocated monotonically: smallest id = oldest table.
        let mut by_age: Vec<Arc<SstHandle>> = s.levels[0].clone();
        by_age.sort_by_key(|t| t.meta.id);

        let mut victims: Vec<Arc<SstHandle>> = Vec::new();
        if !ttl.is_zero() {
            for t in &by_age {
                let path = self.klog_path(t.meta.id);
                let expired = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|mt| now.duration_since(mt).ok())
                    .is_some_and(|age| age > ttl);
                if expired {
                    victims.push(t.clone());
                }
            }
        }
        if max_bytes > 0 {
            let mut total: u64 = by_age
                .iter()
                .map(|t| t.meta.klog_size + t.meta.vlog_size)
                .sum();
            for t in &by_age {
                if total <= max_bytes {
                    break;
                }
                if !victims.iter().any(|v| Arc::ptr_eq(v, t)) {
                    victims.push(t.clone());
                }
                total -= t.meta.klog_size + t.meta.vlog_size;
            }
        }
        victims
    }

    /// Removal half of FIFO eviction: drop these ids from L0.
    ///
    /// A publication primitive: reachable only from a `catalog_txn` publish
    /// closure (see [`crate::db::Publish`]).
    pub(crate) fn remove_l0_tables(&self, ids: &[u64], _p: &crate::db::Publish) {
        let mut s = self.state.write();
        s.levels[0].retain(|t| !ids.contains(&t.meta.id));
    }

    /// Install pre-built level handles (used by clone).
    ///
    /// A publication primitive: reachable only from a `catalog_txn` publish
    /// closure (see [`crate::db::Publish`]).
    pub(crate) fn install_levels(&self, levels: Vec<Vec<Arc<SstHandle>>>, _p: &crate::db::Publish) {
        self.replace_levels(levels);
    }

    /// Build a handle for an already-on-disk SSTable id (used by clone).
    pub(crate) fn open_sst(&self, meta: SstMeta) -> Result<Arc<SstHandle>> {
        Ok(self.handle_for(meta))
    }

    /// Snapshot the live partition rules. Compaction takes one snapshot per run
    /// so a rule added mid-run cannot change that run's cut boundaries — it
    /// takes effect on the next bottom compaction.
    pub(crate) fn partition_rules_snapshot(&self) -> Vec<PartitionRule> {
        self.live_partition_rules.read().clone()
    }

    /// Snapshot whatever decides partitions for this CF — the live rule vector
    /// or the configured [`PartitionFn`](crate::PartitionFn).
    ///
    /// Same one-snapshot-per-run contract as
    /// [`partition_rules_snapshot`](Self::partition_rules_snapshot): the
    /// derived case is immutable for the life of the CF, so snapshotting it is
    /// just an `Arc` clone.
    pub(crate) fn partition_resolver_snapshot(&self) -> Result<crate::config::PartitionResolver> {
        match &self.opts.partition_scheme {
            crate::config::PartitionScheme::Derived(f) => {
                Ok(crate::config::PartitionResolver::Derived(f.clone()))
            }
            crate::config::PartitionScheme::Rules => Ok(crate::config::PartitionResolver::Rules(
                self.partition_rules_snapshot(),
            )),
            // `DB::open` resolves every persisted scheme before any CF becomes
            // reachable, so this is unreachable in a correctly opened database.
            // It is an error rather than a fallback to `Rules` because falling
            // back would cut every part written afterwards on the wrong
            // boundaries while appearing to succeed.
            crate::config::PartitionScheme::Unresolved(name) => {
                Err(OndaError::InvalidArgs(format!(
                    "column family {:?} uses derived partition scheme {name:?}, which was not \
                     registered in Options::partition_fns",
                    self.name()
                )))
            }
        }
    }

    /// The effective durable config: `opts` with its partition rules replaced by
    /// the live set. Every path that (re)encodes the config for persistence —
    /// the `CreateCf`/`SetCfConfig` ops `DbInner::catalog_txn` makes durable,
    /// the snapshot `DbInner::persist_manifest` rebuilds from them, `freeze_part`,
    /// CF copy/clear — goes through this so a live-added rule round-trips across
    /// reopen.
    pub(crate) fn effective_config(&self) -> ColumnFamilyConfig {
        let mut cfg = self.opts.clone();
        cfg.partition_rules = self.live_partition_rules.read().clone();
        cfg
    }

    /// Public read-only view of the effective durable config (see
    /// [`Self::effective_config`]); also reachable via
    /// [`crate::DB::column_family_config`].
    pub fn config(&self) -> ColumnFamilyConfig {
        self.effective_config()
    }

    /// Append `rule` to the live partition rules after validating the resulting
    /// set with the same check [`ColumnFamilyConfig::validate`] runs at create
    /// time (an exact-duplicate prefix is rejected). Validation and the append
    /// happen under one write-lock acquisition, so two concurrent adds serialize
    /// and the second observes the first (rejecting a duplicate). In-memory only;
    /// the caller persists the manifest.
    pub(crate) fn plan_partition_rule_addition(
        &self,
        rule: &PartitionRule,
    ) -> Result<ColumnFamilyConfig> {
        let rules = self.live_partition_rules.read();
        let mut candidate = self.opts.clone();
        candidate.partition_rules = rules.clone();
        candidate.partition_rules.push(rule.clone());
        candidate.validate().map_err(OndaError::InvalidArgs)?;
        Ok(candidate)
    }

    /// Publication half of [`plan_partition_rule_addition`]. A publication
    /// primitive: reachable only from a `catalog_txn` publish closure. Callers
    /// hold `DbInner::cf_lifecycle_mu` across plan and publish, which is what
    /// keeps two concurrent adds from both validating against the old set.
    pub(crate) fn append_partition_rule(&self, rule: PartitionRule, _p: &crate::db::Publish) {
        self.live_partition_rules.write().push(rule);
    }

    /// Remove the partition rule whose prefix exactly equals `prefix` from the
    /// live set, returning [`OndaError::NotFound`] if none matches. In-memory
    /// only; the caller persists the manifest. Symmetric with
    /// [`append_partition_rule`](Self::append_partition_rule): write-side-only,
    /// so already-materialized bottom parts keep their stamps until a later
    /// compaction rewrites them.
    pub(crate) fn plan_partition_rule_removal(&self, prefix: &[u8]) -> Result<ColumnFamilyConfig> {
        let rules = self.live_partition_rules.read();
        let mut candidate = self.opts.clone();
        candidate.partition_rules = rules.clone();
        candidate.partition_rules.retain(|r| r.prefix != prefix);
        if candidate.partition_rules.len() == rules.len() {
            return Err(OndaError::NotFound);
        }
        Ok(candidate)
    }

    /// Publication half of [`plan_partition_rule_removal`]. A publication
    /// primitive: reachable only from a `catalog_txn` publish closure.
    pub(crate) fn remove_partition_rule(&self, prefix: &[u8], _p: &crate::db::Publish) {
        self.live_partition_rules
            .write()
            .retain(|r| r.prefix != prefix);
    }

    // ---- part lifecycle support (used by parts.rs) ----

    /// Snapshot the bottom-level handles belonging to `partition` (the unit of
    /// DETACH / ATTACH / FREEZE). Only the last level is considered — upper
    /// levels are "young data" and never partition-clean.
    pub(crate) fn bottom_partition_handles(&self, partition: &str) -> Vec<Arc<SstHandle>> {
        let s = self.state.read();
        match s.levels.last() {
            Some(bottom) => bottom
                .iter()
                .filter(|h| h.meta.partition.as_deref() == Some(partition))
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// Remove the tables with these ids from the bottom level, under the state
    /// write-lock (the in-memory half of an atomic detach/move). Returns the
    /// number actually removed.
    pub(crate) fn remove_bottom_tables(&self, ids: &[u64], _p: &crate::db::Publish) -> usize {
        let mut s = self.state.write();
        let Some(bottom) = s.levels.last_mut() else {
            return 0;
        };
        let before = bottom.len();
        bottom.retain(|h| !ids.contains(&h.meta.id));
        before - bottom.len()
    }

    /// Remove the tables with these ids from **every** level, under the state
    /// write-lock. Returns the number actually removed.
    ///
    /// The level-agnostic sibling of
    /// [`remove_bottom_tables`](Self::remove_bottom_tables), which edits only
    /// `levels.last_mut()` because a part is by definition bottom-level. Delete-only
    /// excise (1.2) has no such restriction — a bulk range delete's most valuable
    /// targets are fully shadowed L0 and mid-level tables — so it needs a removal
    /// that walks the whole level set.
    ///
    /// `retain` is **stable**, so the per-level `min_key` ordering established at
    /// load survives; `find_overlapping`'s binary search, `bottom_overlaps` and
    /// `insert_bottom_sorted` all depend on it (pinned by
    /// `remove_tables_preserves_min_key_order`).
    ///
    /// A publication primitive: reachable only from a `catalog_txn` publish
    /// closure.
    pub(crate) fn remove_tables(&self, ids: &[u64], _p: &crate::db::Publish) -> usize {
        let mut s = self.state.write();
        let mut removed = 0;
        for lvl in s.levels.iter_mut() {
            let before = lvl.len();
            lvl.retain(|h| !ids.contains(&h.meta.id));
            removed += before - lvl.len();
        }
        removed
    }

    /// Whether `[span_min, span_max]` overlaps any live bottom-level table's
    /// **span**. An attached part with no overlap can slot straight into the
    /// bottom level; otherwise it must go to L0 (which permits overlapping
    /// tables).
    ///
    /// Spans, not point bounds, on both sides since 1.2: a table's range
    /// fragments may reach past its last point key, and two bottom tables whose
    /// spans overlap would break the level->=1 disjointness `find_overlapping`
    /// and the gap-owner rule depend on.
    pub(crate) fn bottom_overlaps(&self, span_min: &[u8], span_max: &[u8]) -> bool {
        let s = self.state.read();
        let Some(bottom) = s.levels.last() else {
            return false;
        };
        bottom.iter().any(|h| {
            self.cmp
                .compare(span_min, h.meta.span_max(&self.cmp))
                .is_le()
                && self
                    .cmp
                    .compare(h.meta.span_min(&self.cmp), span_max)
                    .is_le()
        })
    }

    /// Index of the bottom (last) level.
    pub(crate) fn bottom_level_index(&self) -> usize {
        self.state.read().levels.len().saturating_sub(1)
    }

    /// Insert `handle` into the bottom level, keeping it sorted by `min_key`
    /// (the invariant leveled reads rely on for binary search).
    pub(crate) fn insert_bottom_sorted(&self, handle: Arc<SstHandle>, _p: &crate::db::Publish) {
        let mut s = self.state.write();
        let cmp = self.cmp.clone();
        let bottom = s
            .levels
            .last_mut()
            .expect("at least one level always exists");
        bottom.push(handle);
        bottom.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
    }

    /// Replace the bottom-level tables with these ids by `replacements` (same
    /// ids, new handles/metas) under the state write-lock. Used by the tier
    /// mover to swap in relocated handles; in-flight reads finish on the old
    /// handles they already hold.
    pub(crate) fn swap_bottom_tables(
        &self,
        replacements: Vec<Arc<SstHandle>>,
        _p: &crate::db::Publish,
    ) {
        let mut s = self.state.write();
        let cmp = self.cmp.clone();
        let Some(bottom) = s.levels.last_mut() else {
            return;
        };
        let ids: std::collections::HashSet<u64> = replacements.iter().map(|h| h.meta.id).collect();
        bottom.retain(|h| !ids.contains(&h.meta.id));
        bottom.extend(replacements);
        bottom.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
    }

    /// This CF's storage-tier registry (path/backend resolution).
    pub(crate) fn tiers(&self) -> &Arc<TierRegistry> {
        &self.ctx.tiers
    }

    /// This CF's storage-tier placement rules (see
    /// [`ColumnFamilyConfig::tier_rules`]). Unlike partition rules these are not
    /// live-mutable, so the durable `opts` copy is authoritative.
    pub(crate) fn tier_rules(&self) -> &[crate::config::TierRule] {
        &self.opts.tier_rules
    }

    /// Summarize the bottom-level parts (one per distinct partition name) for the
    /// part mover: each part's smallest key, its current tier, and the age of its
    /// newest entry. A part whose tables straddle more than one tier (only
    /// possible after an interrupted move, before startup GC runs) is skipped so
    /// the mover never acts on an inconsistent set. `max_entry_time` is the max
    /// over the part's tables, or `None` if any table lacks a stamp (unknown age
    /// is conservatively ineligible). The implicit default partition (`None`) is
    /// not a mover part and is omitted.
    pub(crate) fn bottom_parts(&self) -> Vec<BottomPart> {
        let s = self.state.read();
        let Some(bottom) = s.levels.last() else {
            return Vec::new();
        };
        let mut groups: std::collections::HashMap<&str, Vec<&Arc<SstHandle>>> =
            std::collections::HashMap::new();
        for h in bottom {
            if let Some(p) = h.meta.partition.as_deref() {
                groups.entry(p).or_default().push(h);
            }
        }
        let mut out = Vec::with_capacity(groups.len());
        for (name, hs) in groups {
            let tier = hs[0].meta.tier.clone();
            if !hs.iter().all(|h| h.meta.tier == tier) {
                continue; // straddles tiers — leave for startup GC / next pass
            }
            let min_key = hs
                .iter()
                .map(|h| &h.meta.min_key)
                .min_by(|a, b| self.cmp.compare(a, b))
                .expect("group is non-empty")
                .clone();
            let max_entry_time = if hs.iter().all(|h| h.meta.max_entry_time.is_some()) {
                hs.iter().filter_map(|h| h.meta.max_entry_time).max()
            } else {
                None
            };
            out.push(BottomPart {
                partition: name.to_string(),
                min_key,
                tier,
                max_entry_time,
            });
        }
        out
    }
}

/// One bottom-level part as seen by the mover (see
/// [`ColumnFamily::bottom_parts`]).
pub(crate) struct BottomPart {
    /// Partition name (the mover moves whole named partitions).
    pub partition: String,
    /// Smallest user key in the part (used to resolve its tier rule).
    pub min_key: Vec<u8>,
    /// Tier the part currently lives on (`None` = the default tier).
    pub tier: Option<String>,
    /// Age of the part's newest entry, or `None` if unknown.
    pub max_entry_time: Option<i64>,
}

fn existing_wal_gens(dir: &str) -> Result<Vec<u64>> {
    let mut gens = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("wal-") {
            if let Some(num) = rest.strip_suffix(".log") {
                if let Ok(g) = num.parse::<u64>() {
                    gens.push(g);
                }
            }
        }
    }
    gens.sort_unstable();
    Ok(gens)
}

/// Convert a borrowed key bound to an owned one (for storage in the iterator).
/// Collapse a sorted `(table position, key index)` list into one entry per
/// table, preserving key order within each.
fn group_level_hits(out: &mut BatchTables, level: &[Arc<SstHandle>], sorted: &[(usize, usize)]) {
    let mut pos = 0;
    while pos < sorted.len() {
        let t = sorted[pos].0;
        let mut idxs: SmallVec<[usize; 8]> = SmallVec::new();
        while pos < sorted.len() && sorted[pos].0 == t {
            idxs.push(sorted[pos].1);
            pos += 1;
        }
        out.push((level[t].clone(), idxs));
    }
}

fn bound_to_owned(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k.to_vec()),
        Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
    }
}

fn key_is_below_upper(cmp: &ComparatorRef, key: &[u8], upper: Bound<&[u8]>) -> bool {
    match upper {
        Bound::Unbounded => true,
        Bound::Included(limit) => cmp.compare(key, limit).is_le(),
        Bound::Excluded(limit) => cmp.compare(key, limit).is_lt(),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use super::{key_is_below_upper, PointReadCandidate};
    use crate::comparator::{Bytewise, CaseInsensitive, ComparatorRef};
    use crate::error::OndaError;

    #[test]
    fn point_read_candidate_keeps_the_newest_visible_result() {
        let mut candidate = PointReadCandidate::default();

        candidate.consider(Some(b"new".to_vec()), 9, true, false, crate::format::KIND_PUT);
        candidate.consider(Some(b"old".to_vec()), 3, true, false, crate::format::KIND_PUT);
        candidate.consider(Some(b"same-sequence".to_vec()), 9, true, false, crate::format::KIND_PUT);
        candidate.consider(Some(b"absent".to_vec()), 12, false, false, crate::format::KIND_PUT);

        assert_eq!(candidate.finish().unwrap(), b"new");
    }

    #[test]
    fn point_read_candidate_newer_tombstone_hides_older_value() {
        let mut candidate = PointReadCandidate::default();

        candidate.consider(Some(b"value".to_vec()), 4, true, false, crate::format::KIND_PUT);
        candidate.consider(None, 5, true, true, crate::format::KIND_PUT);

        assert!(matches!(candidate.finish(), Err(OndaError::NotFound)));
    }

    #[test]
    fn sorted_level_upper_bound_uses_the_column_family_comparator() {
        let bytewise: ComparatorRef = Arc::new(Bytewise);
        let folded: ComparatorRef = Arc::new(CaseInsensitive);

        assert!(key_is_below_upper(&bytewise, b"B", Bound::Included(b"a")));
        assert!(!key_is_below_upper(&folded, b"B", Bound::Included(b"a")));
        assert!(key_is_below_upper(&folded, b"A", Bound::Included(b"a")));
        assert!(!key_is_below_upper(&folded, b"A", Bound::Excluded(b"a")));
    }

    /// Wait for background compaction to drain L0 into the levels below. A
    /// level only exists once compaction has created it, so a fixture that
    /// needs L1 has to let the worker run.
    fn wait_for_deep_level(cf: &Arc<super::ColumnFamily>) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let deep = cf.state.read().levels.iter().skip(1).any(|l| !l.is_empty());
            if deep {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "compaction never populated a level below L0"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// Open a database with one CF populated across L0 and the levels below,
    /// so the batch planner has to group per level as well as within L0.
    fn layered_cf(dir: &std::path::Path) -> (crate::DB, Arc<super::ColumnFamily>) {
        let db = crate::DB::open(crate::config::Options::new(dir.to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("layered", crate::config::ColumnFamilyConfig::default())
            .unwrap();
        // Six disjoint runs: past `l1_file_count_trigger` (4), so background
        // compaction pushes them down and a level >= 1 ends up holding several
        // tables that `find_overlapping` has to choose between per key.
        for run in 0..6u32 {
            for i in 0..24u32 {
                let k = format!("k{:03}", run * 24 + i);
                db.put(&cf, k.as_bytes(), b"deep", std::time::Duration::ZERO)
                    .unwrap();
            }
            db.flush_memtable(&cf).unwrap();
        }
        wait_for_deep_level(&cf);
        // The manual sweep can only push levels that exist, so it runs after
        // the background worker has created L1; it then empties L0.
        db.compact(&cf).unwrap();
        assert_eq!(cf.l0_file_count(), 0, "the sweep pushes all of L0 down");
        // ...plus two overlapping L0 tables on top — below the trigger, so they
        // stay where the test put them.
        for run in 0..2u32 {
            for i in 0..30u32 {
                let k = format!("k{:03}", i * 4 + run);
                db.put(&cf, k.as_bytes(), b"shallow", std::time::Duration::ZERO)
                    .unwrap();
            }
            db.flush_memtable(&cf).unwrap();
        }
        assert!(
            cf.l0_file_count() >= 2,
            "fixture needs overlapping L0 files"
        );
        (db, cf)
    }

    /// Drive `remove_tables` the only way it is reachable: through a catalog
    /// transaction, which is what mints the `Publish` token it demands.
    fn remove_via_txn(db: &crate::DB, cf: &Arc<super::ColumnFamily>, ids: &[u64]) -> usize {
        let edit =
            crate::manifest_edit::VersionEdit::new(vec![crate::manifest_edit::Op::RemoveTables {
                cf: cf.name().to_string(),
                ids: ids.to_vec(),
            }]);
        let mut removed = 0;
        db.inner
            .catalog_txn(edit, |p| removed = cf.remove_tables(ids, p))
            .unwrap();
        removed
    }

    #[test]
    fn remove_tables_removes_from_every_level() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = layered_cf(dir.path());
        // One victim from L0 and one from the deepest populated level, so a
        // bottom-only removal (`remove_bottom_tables`) could not do this.
        let (l0, deep) = cf.with_levels(|levels| {
            (
                levels[0][0].meta.id,
                levels
                    .iter()
                    .skip(1)
                    .rev()
                    .find_map(|l| l.first())
                    .expect("a level below L0 is populated")
                    .meta
                    .id,
            )
        });
        assert_ne!(l0, deep);

        assert_eq!(remove_via_txn(&db, &cf, &[l0, deep]), 2);
        let live: Vec<u64> =
            cf.with_levels(|levels| levels.iter().flatten().map(|th| th.meta.id).collect());
        assert!(!live.contains(&l0), "the L0 table is gone");
        assert!(!live.contains(&deep), "the deep table is gone");
        db.close().unwrap();
    }

    /// A column family whose deepest level holds several tables sorted by
    /// `min_key` — the ordering `find_overlapping`'s binary search,
    /// `bottom_overlaps` and `insert_bottom_sorted` all assume.
    fn sorted_deep_level(dir: &std::path::Path) -> (crate::DB, Arc<super::ColumnFamily>) {
        let db = crate::DB::open(crate::config::Options::new(dir.to_str().unwrap())).unwrap();
        let cfg = crate::config::ColumnFamilyConfig {
            // Small enough that one merge produces several output files.
            target_file_size: 4 << 10,
            ..Default::default()
        };
        let cf = db.create_column_family("sorted", cfg).unwrap();
        for run in 0..6u32 {
            for i in 0..40u32 {
                let k = format!("k{:04}", run * 40 + i);
                db.put(&cf, k.as_bytes(), &[b'v'; 64], std::time::Duration::ZERO)
                    .unwrap();
            }
            db.flush_memtable(&cf).unwrap();
        }
        wait_for_deep_level(&cf);
        db.compact(&cf).unwrap();
        (db, cf)
    }

    #[test]
    fn remove_tables_preserves_min_key_order() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = sorted_deep_level(dir.path());
        let cmp = cf.cmp();
        // A middle table of a sorted level: removing an end would leave the
        // level sorted whatever `retain` did.
        let (level, victim, probe) = cf
            .with_levels(|levels| {
                levels.iter().enumerate().skip(1).find_map(|(i, l)| {
                    (l.len() >= 3).then(|| (i, l[1].meta.id, l[2].meta.min_key.clone()))
                })
            })
            .expect("a sorted level with three tables");

        assert_eq!(remove_via_txn(&db, &cf, &[victim]), 1);
        cf.with_levels(|levels| {
            let l = &levels[level];
            assert!(
                l.windows(2)
                    .all(|w| cmp.compare(&w[0].meta.min_key, &w[1].meta.min_key).is_le()),
                "the level is still sorted by min_key"
            );
            // ...and the binary search that depends on it still lands right.
            let (hit, _) = super::ColumnFamily::find_overlapping_at(l, &cmp, &probe);
            assert_eq!(
                l[hit.expect("the probe key is inside a surviving table")]
                    .meta
                    .min_key,
                probe
            );
        });
        db.close().unwrap();
    }

    #[test]
    fn remove_tables_returns_removed_count() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = layered_cf(dir.path());
        let id = cf.with_levels(|levels| levels[0][0].meta.id);
        // An id the catalog does not hold contributes nothing to the count —
        // and the edit names only the ids that exist, since a `RemoveTables`
        // op naming an absent table is refused by the edit's own precondition.
        assert_eq!(remove_via_txn(&db, &cf, &[id]), 1);
        assert!(!cf.with_levels(|levels| levels.iter().flatten().any(|th| th.meta.id == id)));
        db.close().unwrap();
    }

    #[test]
    fn batch_read_sources_matches_per_key_sources() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = layered_cf(dir.path());

        let owned: Vec<Vec<u8>> = (0..140u32)
            .map(|i| format!("k{i:03}").into_bytes())
            .chain(std::iter::once(b"absent".to_vec()))
            .collect();
        let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();
        let batch = cf.batch_read_sources(&keys);

        // Every (table, key) pair the batch planner produced is a pair the
        // per-key planner produces, and vice versa — including the order in
        // which each key meets its tables.
        for (i, key) in keys.iter().enumerate() {
            let per_key = cf.point_read_sources(key);
            let from_batch: Vec<u64> = batch
                .tables
                .iter()
                .filter(|(_, idxs)| idxs.contains(&i))
                .map(|(th, _)| th.meta.id)
                .collect();
            let expected: Vec<u64> = per_key.tables.iter().map(|th| th.meta.id).collect();
            assert_eq!(
                from_batch,
                expected,
                "table set/order for {}",
                String::from_utf8_lossy(key)
            );
        }
        // No table is carried without a key that needs it.
        assert!(batch.tables.iter().all(|(_, idxs)| !idxs.is_empty()));
        db.close().unwrap();
    }

    #[test]
    fn batch_read_sources_takes_one_state_lock() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = layered_cf(dir.path());
        let owned: Vec<Vec<u8>> = (0..64u32)
            .map(|i| format!("k{i:03}").into_bytes())
            .collect();
        let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();

        let before = cf.point_state_reads();
        let _ = cf.batch_read_sources(&keys);
        assert_eq!(
            cf.point_state_reads() - before,
            1,
            "one snapshot for the whole batch"
        );

        // The per-key planner is what the batch replaces: 64 keys, 64 locks.
        let before = cf.point_state_reads();
        for key in &keys {
            let _ = cf.point_read_sources(key);
        }
        assert_eq!(cf.point_state_reads() - before, 64);
        db.close().unwrap();
    }

    /// The oracle the batch path is measured against: N sequential `get`s at
    /// one fixed read sequence.
    fn oracle_multi_get(
        cf: &super::ColumnFamily,
        keys: &[&[u8]],
        read_seq: u64,
    ) -> Vec<crate::error::Result<Vec<u8>>> {
        keys.iter().map(|k| cf.get(k, read_seq)).collect()
    }

    fn same_results(
        got: &[crate::error::Result<Vec<u8>>],
        want: &[crate::error::Result<Vec<u8>>],
    ) -> bool {
        got.len() == want.len()
            && got.iter().zip(want).all(|(g, w)| match (g, w) {
                (Ok(a), Ok(b)) => a == b,
                (Err(a), Err(b)) => std::mem::discriminant(a) == std::mem::discriminant(b),
                _ => false,
            })
    }

    #[test]
    fn oracle_matches_get() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = layered_cf(dir.path());
        // A quiescent database: every committed version is visible at MAX.
        let read_seq = u64::MAX;
        let owned: Vec<Vec<u8>> = ["k000", "k075", "k139", "absent", "k000"]
            .iter()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();

        let oracle = oracle_multi_get(&cf, &keys, read_seq);
        for (r, key) in oracle.iter().zip(&keys) {
            let direct = cf.get(key, read_seq);
            assert!(same_results(
                std::slice::from_ref(r),
                std::slice::from_ref(&direct)
            ));
        }
        db.close().unwrap();
    }

    #[test]
    fn multi_get_memtable_order_matches_get() {
        // The same key in four memtable-shaped sources with different
        // sequences: the batch must pick the winner `get` picks, whichever
        // source holds it.
        for unified in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = crate::DB::open(crate::config::Options {
                unified_memtable: unified,
                ..crate::config::Options::new(dir.path().to_str().unwrap())
            })
            .unwrap();
            let cf = db
                .create_column_family("m", crate::config::ColumnFamilyConfig::default())
                .unwrap();

            // Two sealed memtables, then the active one, each carrying a newer
            // version of the shared key plus one key of its own.
            for round in 0..3u32 {
                db.put(
                    &cf,
                    b"shared",
                    format!("v{round}").as_bytes(),
                    std::time::Duration::ZERO,
                )
                .unwrap();
                db.put(
                    &cf,
                    format!("own{round}").as_bytes(),
                    b"x",
                    std::time::Duration::ZERO,
                )
                .unwrap();
                if round < 2 {
                    cf.rotate_memtable(true);
                }
            }

            let owned: Vec<Vec<u8>> = ["shared", "own0", "own1", "own2", "nope", "shared"]
                .iter()
                .map(|k| k.as_bytes().to_vec())
                .collect();
            let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();
            let read_seq = u64::MAX;

            let batched = cf.multi_get(&keys, read_seq);
            let sequential = oracle_multi_get(&cf, &keys, read_seq);
            assert!(
                same_results(&batched, &sequential),
                "unified={unified}: {batched:?} vs {sequential:?}"
            );
            assert_eq!(batched[0].as_deref().unwrap(), b"v2", "newest wins");
            db.close().unwrap();
        }
    }

    /// Concatenating merge operator for the early-exit oracle.
    #[derive(Debug)]
    struct Concat;

    impl crate::config::MergeOperator for Concat {
        fn name(&self) -> &str {
            "test.early-exit.concat"
        }
        fn full_merge(
            &self,
            _key: &[u8],
            existing: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> std::result::Result<Vec<u8>, String> {
            let mut out = existing.map(<[u8]>::to_vec).unwrap_or_default();
            for op in operands {
                out.push(b'+');
                out.extend_from_slice(op);
            }
            Ok(out)
        }
    }

    /// The early exit (wavesdb 5ef39df) is an optimization, never a semantic:
    /// random puts, deletes, TTL writes, merge operands, range deletes,
    /// flushes, ingestions and compactions, in both layouts, read at the head
    /// and at pinned snapshots — `get`, `get_into` and `multi_get` must answer exactly what
    /// the exhaustive reference (every candidate table probed) answers.
    #[test]
    fn point_read_early_exit_matches_exhaustive() {
        use std::time::Duration;
        for unified in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = crate::config::Options::new(dir.path().to_str().unwrap());
            opts.unified_memtable = unified;
            opts.merge_fns = vec![Arc::new(Concat)];
            let db = crate::DB::open(opts).unwrap();
            db.enable_format_capabilities(crate::format::CAP_RANGE_DELETES)
                .unwrap();
            let cf = db
                .create_column_family(
                    "p",
                    crate::config::ColumnFamilyConfig {
                        l1_file_count_trigger: 6,
                        merge_operator_name: Some("test.early-exit.concat".into()),
                        ..crate::config::ColumnFamilyConfig::default()
                    },
                )
                .unwrap();
            // Deterministic LCG: the test must replay identically.
            let mut state = 0x5EED_u64 ^ u64::from(unified);
            let mut rng = move |n: u64| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (state >> 33) % n
            };
            const KEYS: u64 = 48;
            let key = |i: u64| format!("k{i:03}").into_bytes();
            let mut snaps: Vec<crate::Txn> = Vec::new();
            let check = |snaps: &[crate::Txn], when: &str| {
                let mut seqs = vec![db.inner.visible_seq()];
                seqs.extend(snaps.iter().map(|t| t.read_seq_for_tests()));
                let owned: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
                let keys: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
                for &seq in &seqs {
                    let batch = cf.multi_get(&keys, seq);
                    for (i, k) in keys.iter().enumerate() {
                        let want = cf.get_exhaustive(k, seq).map_err(|e| e.to_string());
                        let got = cf.get(k, seq).map_err(|e| e.to_string());
                        assert_eq!(
                            got,
                            want,
                            "{when}: unified={unified} key {:?} at seq {seq}: get",
                            String::from_utf8_lossy(k)
                        );
                        let mut buf = b"p".to_vec();
                        let got = cf
                            .get_into(k, seq, &mut buf)
                            .map(|n| {
                                assert_eq!(n, buf.len() - 1);
                                buf[1..].to_vec()
                            })
                            .map_err(|e| e.to_string());
                        assert_eq!(
                            got,
                            want,
                            "{when}: unified={unified} key {:?} at seq {seq}: get_into",
                            String::from_utf8_lossy(k)
                        );
                        if got.is_err() {
                            assert_eq!(buf, b"p", "a miss changed the caller's buffer");
                        }
                        let got = batch[i].as_ref().map(Vec::clone).map_err(|e| e.to_string());
                        assert_eq!(
                            got,
                            want,
                            "{when}: unified={unified} key {:?} at seq {seq}: multi_get",
                            String::from_utf8_lossy(k)
                        );
                    }
                }
            };
            for step in 0..600u64 {
                match rng(100) {
                    0..=39 => {
                        let ttl = match rng(6) {
                            0 => Duration::from_secs(3600),
                            1 => Duration::from_nanos(1), // expired when read
                            _ => Duration::ZERO,
                        };
                        db.put(&cf, &key(rng(KEYS)), format!("v{step}").as_bytes(), ttl)
                            .unwrap();
                    }
                    40..=51 => db.merge(&cf, &key(rng(KEYS)), format!("m{step}").as_bytes()).unwrap(),
                    52..=59 => db.delete(&cf, &key(rng(KEYS))).unwrap(),
                    60..=63 => {
                        let lo = rng(KEYS);
                        let hi = (lo + 1 + rng(6)).min(KEYS);
                        db.delete_range(&cf, &key(lo), &key(hi)).unwrap();
                    }
                    64..=73 => db.flush_memtable(&cf).unwrap(),
                    74..=79 => {
                        // Sorted batch through the ingest side door, whose
                        // sequence predates a put committed during the load.
                        let mut ing = db.start_ingestion(&cf).unwrap();
                        if rng(2) == 0 {
                            db.put(&cf, &key(rng(KEYS)), b"during-ingest", Duration::ZERO)
                                .unwrap();
                        }
                        let lo = rng(KEYS);
                        for i in lo..(lo + 8).min(KEYS) {
                            if rng(4) == 0 {
                                ing.write_tombstone(&key(i)).unwrap();
                            } else {
                                ing.write(&key(i), format!("ing{step}").as_bytes(), Duration::ZERO)
                                    .unwrap();
                            }
                        }
                        ing.finish().unwrap();
                    }
                    80..=85 => db.compact(&cf).unwrap(),
                    86..=92 => {
                        if snaps.len() < 4 {
                            snaps.push(db.begin_with_isolation(crate::IsolationLevel::Snapshot));
                        }
                    }
                    _ => {
                        if !snaps.is_empty() {
                            snaps.remove(0);
                        }
                    }
                }
                if step % 25 == 0 {
                    check(&snaps, &format!("step {step}"));
                }
            }
            check(&snaps, "end");
            drop(snaps);
            db.close().unwrap();
        }
    }
}
