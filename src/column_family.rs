//! Column family: an isolated, independently-configured key-value store within a
//! database, backed by its own memtable, WAL and LSM levels.
//!
//! read path, memtable rotation, flush,
//! recovery, adapted to Rust ownership: SSTable handles are reference-counted
//! with `Arc` (replacing the manual incref/decref), and backpressure uses a
//! `parking_lot` `Mutex`/`Condvar`.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use smallvec::SmallVec;
use crate::util::now_nanos;
use crate::wal::{self, Wal};

const DATA_BLOCK_SIZE: usize = 4 << 10;

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
    pub reader: Arc<Reader>,
}

/// An immutable (sealed) memtable awaiting flush.
#[derive(Debug)]
pub struct ImmMemtable {
    pub mem: Arc<Memtable>,
    pub wal_paths: Vec<String>,
}

/// Shared database context handed to each column family.
pub(crate) struct CfCtx {
    /// Storage-tier registry: resolves a table's tier to its root directory and
    /// [`Storage`](crate::storage::Storage) backend. The default tier is the DB
    /// directory, so untiered tables resolve exactly as before tiering existed.
    pub tiers: Arc<TierRegistry>,
    pub bc: Arc<BlockCache>,
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
    pub(crate) compacting: AtomicBool,
    /// Serializes compaction runs on this CF. Two concurrent runs each
    /// snapshot the level set, merge, and `replace_levels` — the loser's
    /// installed tables would be dropped while its inputs are already
    /// unlinked. Background workers and `DB::compact` can otherwise overlap
    /// (the compact queue may hold the same CF twice across two worker
    /// threads).
    pub(crate) compact_mu: Mutex<()>,
    commit_hook: Mutex<Option<CommitHookFn>>,
    compaction_filter: Mutex<Option<CompactionFilterFn>>,
    /// Mirrors `commit_hook.is_some()`; lets the commit path skip building hook
    /// payloads (a per-op key+value clone) with a relaxed load instead of a lock.
    hook_set: AtomicBool,

    pub(crate) flush_count: AtomicU64,
    pub(crate) compaction_count: AtomicU64,

    // Read-path counters (relaxed; observability only).
    pub(crate) point_reads: AtomicU64,
    pub(crate) bloom_skips: AtomicU64,
    pub(crate) sst_probes: AtomicU64,
}

impl std::fmt::Debug for ColumnFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamily")
            .field("name", &self.name)
            .finish()
    }
}

impl ColumnFamily {
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
        match meta.tier.as_deref() {
            None => self.klog_path(meta.id),
            Some(t) => format!(
                "{}/{}.klog",
                self.ctx.tiers.cf_dir(Some(t), &self.name),
                meta.id
            ),
        }
    }

    /// Open a reader for an already-on-disk table described by `meta`, using the
    /// [`Storage`](crate::storage::Storage) backend for its tier (so a no-mmap
    /// tier reads through the buffered path).
    pub(crate) fn open_reader_for(&self, meta: &SstMeta) -> Result<Arc<Reader>> {
        let storage = self.ctx.tiers.storage_for(meta.tier.as_deref());
        Reader::open(
            &self.klog_path_for(meta),
            storage,
            self.ctx.bc.clone(),
            meta.id,
            self.cmp.clone(),
        )
    }

    /// Create a fresh column family (directory + generation-0 WAL).
    pub(crate) fn create(
        ctx: Arc<CfCtx>,
        name: String,
        dir: String,
        opts: ColumnFamilyConfig,
        cmp: ComparatorRef,
    ) -> Result<Arc<ColumnFamily>> {
        std::fs::create_dir_all(&dir)?;
        let mem = Memtable::new(cmp.clone());
        let wal0 = format!("{dir}/wal-0.log");
        let wal = if ctx.read_only {
            None
        } else {
            let w = Wal::open(&wal0, opts.sync_mode, opts.sync_interval)?;
            w.set_poison(ctx.poison.clone());
            Some(Arc::new(w))
        };
        let live_partition_rules = RwLock::new(opts.partition_rules.clone());
        let cf = Arc::new(ColumnFamily {
            ctx,
            id: crate::unified::cf_id(&name),
            name,
            dir,
            opts,
            live_partition_rules,
            cmp,
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
            compacting: AtomicBool::new(false),
            compact_mu: Mutex::new(()),
            commit_hook: Mutex::new(None),
            compaction_filter: Mutex::new(None),
            hook_set: AtomicBool::new(false),
            flush_count: AtomicU64::new(0),
            compaction_count: AtomicU64::new(0),
            point_reads: AtomicU64::new(0),
            bloom_skips: AtomicU64::new(0),
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
        dir: String,
        opts: ColumnFamilyConfig,
        cmp: ComparatorRef,
        ssts: &[SstMeta],
    ) -> Result<(Arc<ColumnFamily>, u64)> {
        let mut max_level = 1usize;
        for s in ssts {
            max_level = max_level.max(s.level as usize + 1);
        }
        let mut levels: Vec<Vec<Arc<SstHandle>>> = vec![Vec::new(); max_level];
        for s in ssts {
            // Resolve the tier before opening so a bottom part on another mount
            // (and any no-mmap backend it carries) is read through the right
            // storage. `None` tier == the default path used before tiering.
            let storage = ctx.tiers.storage_for(s.tier.as_deref());
            let klog = match s.tier.as_deref() {
                None => format!("{dir}/{}.klog", s.id),
                Some(t) => format!("{}/{}.klog", ctx.tiers.cf_dir(Some(t), &name), s.id),
            };
            let reader = Reader::open(&klog, storage, ctx.bc.clone(), s.id, cmp.clone())?;
            levels[s.level as usize].push(Arc::new(SstHandle {
                meta: s.clone(),
                reader,
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
            let last = Wal::replay(&p, |r| {
                mem.put(&r.key, r.value, r.seq, r.ttl, r.tombstone, r.single_delete);
                Ok(())
            })?;
            max_seq = max_seq.max(last);
        }

        let next_gen = gens.last().map(|g| g + 1).unwrap_or(0);
        let (wal, pending) = if ctx.read_only {
            (None, replay_paths)
        } else {
            let p = format!("{dir}/wal-{next_gen}.log");
            let w = Wal::open(&p, opts.sync_mode, opts.sync_interval)?;
            w.set_poison(ctx.poison.clone());
            let w = Arc::new(w);
            let mut pend = replay_paths;
            pend.push(p);
            (Some(w), pend)
        };

        let live_partition_rules = RwLock::new(opts.partition_rules.clone());
        let cf = Arc::new(ColumnFamily {
            ctx,
            id: crate::unified::cf_id(&name),
            name,
            dir,
            opts,
            live_partition_rules,
            cmp,
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
            compacting: AtomicBool::new(false),
            compact_mu: Mutex::new(()),
            commit_hook: Mutex::new(None),
            compaction_filter: Mutex::new(None),
            hook_set: AtomicBool::new(false),
            flush_count: AtomicU64::new(0),
            compaction_count: AtomicU64::new(0),
            point_reads: AtomicU64::new(0),
            bloom_skips: AtomicU64::new(0),
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

    /// Apply a committed batch: append to the WAL and insert into the memtable,
    /// then rotate if the memtable is full. Records borrow the transaction's
    /// buffer; both the WAL and the memtable copy what they need.
    pub(crate) fn apply_commit(self: &Arc<Self>, recs: &[wal::RecordRef<'_>]) -> Result<()> {
        self.ctx.poison.check()?;
        let threshold = self.opts.l0_queue_stall_threshold as usize;
        {
            let mut g = self.rot.lock();
            loop {
                let stalled = g.rotating
                    || (self.state.read().imm.len() >= threshold
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
            if let Some(w) = &wal {
                w.append_batch(recs)?;
            } else {
                return Err(OndaError::ReadOnly("wal unavailable".into()));
            }
            mem.put_batch(recs);
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
                if s.mem.is_empty() {
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
                Wal::open(&new_path, self.opts.sync_mode, self.opts.sync_interval)
                    .ok()
                    .map(|w| {
                        w.set_poison(self.ctx.poison.clone());
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
        }
    }

    /// Flush an immutable memtable to a new L0 SSTable with id `file_id`.
    /// Returns the WAL paths that may now be deleted.
    pub(crate) fn flush_imm(&self, imm: &Arc<ImmMemtable>, file_id: u64) -> Result<Vec<String>> {
        self.flushing.store(true, Ordering::Relaxed);
        let result = self.flush_imm_inner(imm, file_id);
        self.flushing.store(false, Ordering::Relaxed);
        self.cond.notify_all();
        result
    }

    fn flush_imm_inner(&self, imm: &Arc<ImmMemtable>, file_id: u64) -> Result<Vec<String>> {
        // Fast path: stream entries straight out of the sealed memtable's arena
        // nodes through a k-way merge — no per-entry allocation, no sort.
        #[cfg(feature = "arena-memtable")]
        self.write_l0_streaming(&imm.mem, file_id)?;
        #[cfg(not(feature = "arena-memtable"))]
        {
            let entries = imm.mem.snapshot();
            self.write_l0(&entries, file_id)?;
        }
        // Remove the flushed immutable.
        {
            let mut s = self.state.write();
            if let Some(pos) = s.imm.iter().position(|i| Arc::ptr_eq(i, imm)) {
                s.imm.remove(pos);
            }
        }
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        Ok(imm.wal_paths.clone())
    }

    /// Finish `w` (fsync + footer) and open a reader, without installing it.
    pub(crate) fn finish_writer_to_handle(
        &self,
        w: Writer,
        file_id: u64,
    ) -> Result<Arc<SstHandle>> {
        // Flush/ingest output always lands on the default tier (L0), so
        // `meta.tier` is None and `open_reader_for` resolves the default path.
        let mut meta = w.finish()?.to_sst_meta(file_id, 0);
        // Stamp the write time as the table's max entry age: flush/ingest output
        // holds freshly committed data, so the file's finish time approximates
        // the newest entry's commit time (see `SstMeta::max_entry_time`).
        meta.max_entry_time = Some(now_nanos());
        let reader = self.open_reader_for(&meta)?;
        Ok(Arc::new(SstHandle { meta, reader }))
    }

    /// Register already-finished SSTables as the newest L0 files, atomically.
    pub(crate) fn install_handles_l0(&self, handles: Vec<Arc<SstHandle>>) {
        let mut s = self.state.write();
        for h in handles {
            s.levels[0].insert(0, h); // newest first
        }
    }

    /// Finish `w` and register the resulting SSTable as the newest L0 file.
    fn install_l0(&self, w: Writer, file_id: u64) -> Result<()> {
        let handle = self.finish_writer_to_handle(w, file_id)?;
        self.install_handles_l0(vec![handle]);
        Ok(())
    }

    /// Open a fresh SSTable writer for this CF (used by bulk ingestion).
    pub(crate) fn new_sst_writer(&self, file_id: u64, expected: usize) -> Result<Writer> {
        Writer::new(&self.klog_path(file_id), self.writer_opts(expected))
    }

    /// Stream a sealed memtable to a new L0 SSTable without materializing
    /// entries (keys/values borrowed from the arena through the merge).
    #[cfg(feature = "arena-memtable")]
    fn write_l0_streaming(&self, mem: &Memtable, file_id: u64) -> Result<()> {
        let mut m = mem.flush_merge();
        if !m.valid() {
            return Ok(());
        }
        let klog = self.klog_path(file_id);
        let mut w = Writer::new(&klog, self.writer_opts(mem.num_entries().max(0) as usize))?;
        while m.valid() {
            let c = m.top();
            w.add(
                c.user_key(),
                c.value(),
                c.seq(),
                c.ttl(),
                c.tombstone(),
                c.single_delete(),
            )?;
            m.advance();
        }
        self.install_l0(w, file_id)
    }

    /// Write `entries` (already in this CF's internal order) to a new L0 SSTable.
    fn write_l0(&self, entries: &[crate::memtable::Entry], file_id: u64) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let klog = self.klog_path(file_id);
        let mut w = Writer::new(&klog, self.writer_opts(entries.len()))?;
        for e in entries {
            w.add(
                &e.user_key,
                &e.value,
                e.seq,
                e.ttl,
                e.tombstone,
                e.single_delete,
            )?;
        }
        self.install_l0(w, file_id)
    }

    /// Ingest a CF's slice of a split unified memtable: sort by this CF's
    /// comparator, then write an L0 SSTable.
    pub(crate) fn ingest_l0(
        &self,
        mut entries: Vec<crate::memtable::Entry>,
        file_id: u64,
    ) -> Result<()> {
        entries.sort_by(|a, b| {
            self.cmp
                .compare(&a.user_key, &b.user_key)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        self.flushing.store(true, Ordering::Relaxed);
        let r = self.write_l0(&entries, file_id);
        self.flushing.store(false, Ordering::Relaxed);
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        r
    }

    /// Stable column-family id (used by unified-memtable key prefixing).
    pub fn id(&self) -> u64 {
        self.id
    }

    fn writer_opts(&self, expected: usize) -> WriterOptions {
        WriterOptions {
            // Flush and ingestion always write L0.
            compression: self.opts.compression_for_level(0),
            compression_rules: self.opts.compression_rules.clone(),
            cmp: self.cmp.clone(),
            enable_bloom: self.opts.enable_bloom_filter,
            bloom_fpr: self.opts.bloom_fpr,
            klog_value_threshold: self.opts.klog_value_threshold,
            block_size: DATA_BLOCK_SIZE,
            expected_entries: expected,
            use_btree: self.opts.use_btree,
            restart_interval: crate::sst::RESTART_INTERVAL,
        }
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

    /// Resolve `user_key` as of `read_seq`. Returns the value, or `NotFound`.
    pub(crate) fn get(&self, user_key: &[u8], read_seq: u64) -> Result<Vec<u8>> {
        self.point_reads.fetch_add(1, Ordering::Relaxed);
        let now = now_nanos();
        let (mem, imms, tables) = {
            let s = self.state.read();
            let mem = s.mem.clone();
            let imms: Vec<Arc<ImmMemtable>> = s.imm.clone();
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
            (mem, imms, tables)
        };

        let mut best_val: Option<Vec<u8>> = None;
        let mut best_seq = 0u64;
        let mut best_found = false;
        let mut best_deleted = false;

        // Unified-memtable mode: the shared store holds this CF's hot data.
        if let Some(u) = &self.ctx.unified {
            let r = u.get(self.id, user_key, read_seq, now);
            if r.found {
                best_found = true;
                best_seq = r.seq;
                best_deleted = r.deleted;
                best_val = if r.deleted { None } else { Some(r.value) };
            }
        }
        let r = mem.get(user_key, read_seq, now);
        if r.found && (!best_found || r.seq > best_seq) {
            best_found = true;
            best_seq = r.seq;
            best_deleted = r.deleted;
            best_val = if r.deleted { None } else { Some(r.value) };
        }
        for imm in imms.iter().rev() {
            let r = imm.mem.get(user_key, read_seq, now);
            if r.found && (!best_found || r.seq > best_seq) {
                best_found = true;
                best_seq = r.seq;
                best_deleted = r.deleted;
                best_val = if r.deleted { None } else { Some(r.value) };
            }
        }
        for th in &tables {
            // One bloom hash + one check per table; the probe below skips the
            // filter (it was just consulted).
            let h = th.reader.bloom_hash(user_key);
            if !th.reader.bloom_may_contain_hash(h) {
                self.bloom_skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.sst_probes.fetch_add(1, Ordering::Relaxed);
            let (v, seq, found, deleted) = th.reader.get_unfiltered(user_key, read_seq, now)?;
            if found && (!best_found || seq > best_seq) {
                best_found = true;
                best_seq = seq;
                best_deleted = deleted;
                best_val = v;
            }
        }

        if best_found && !best_deleted {
            Ok(best_val.unwrap_or_default())
        } else {
            Err(OndaError::NotFound)
        }
    }

    /// Newest committed sequence for `user_key` across all sources (ignoring
    /// snapshots), or `0` if the key has never been written.  Used for
    /// write-write conflict detection.
    pub(crate) fn peek_seq(&self, user_key: &[u8]) -> Result<u64> {
        let now = now_nanos();
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
            let (_, seq, found, _) = th.reader.get(user_key, u64::MAX, now)?;
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
        let mut children: Vec<ChildIter> = Vec::new();
        let s = self.state.read();
        if let Some(extra) = extra {
            children.push(ChildIter::Mem(extra.iter()));
        }
        // Unified mode: overlay this CF's slice of the shared memtable.
        if let Some(u) = &self.ctx.unified {
            let entries = u.entries_for_cf(self.id);
            if !entries.is_empty() {
                let overlay = Memtable::new(self.cmp.clone());
                for e in entries {
                    overlay.put(
                        &e.user_key,
                        e.value,
                        e.seq,
                        e.ttl,
                        e.tombstone,
                        e.single_delete,
                    );
                }
                children.push(ChildIter::Mem(overlay.iter()));
            }
        }
        children.push(ChildIter::Mem(s.mem.iter()));
        for imm in s.imm.iter().rev() {
            children.push(ChildIter::Mem(imm.mem.iter()));
        }
        for th in &s.levels[0] {
            if Self::sst_in_bounds(th, &self.cmp, &bounds) {
                children.push(ChildIter::Sst(th.reader.iter()));
            }
        }
        for lvl in s.levels.iter().skip(1) {
            for th in lvl {
                if Self::sst_in_bounds(th, &self.cmp, &bounds) {
                    children.push(ChildIter::Sst(th.reader.iter()));
                }
            }
        }
        drop(s);
        let owned = (
            bound_to_owned(bounds.0),
            bound_to_owned(bounds.1),
        );
        Iterator::new(self.cmp.clone(), children, read_seq, now_nanos(), owned)
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
        if let Some(w) = s.wal.take() {
            let _ = w.close();
        }
        for lvl in &s.levels {
            for th in lvl {
                th.reader.close();
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

    /// FIFO eviction: remove the oldest L0 tables until the CF is back under
    /// `max_bytes`, plus any table whose klog file age exceeds `ttl`. Returns
    /// the removed handles (caller persists the manifest, then deletes files).
    pub(crate) fn take_fifo_victims(
        &self,
        max_bytes: u64,
        ttl: std::time::Duration,
    ) -> Vec<Arc<SstHandle>> {
        let now = std::time::SystemTime::now();
        let mut s = self.state.write();
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
        if !victims.is_empty() {
            s.levels[0].retain(|t| !victims.iter().any(|v| Arc::ptr_eq(v, t)));
        }
        victims
    }

    /// Install pre-built level handles (used by clone).
    pub(crate) fn install_levels(&self, levels: Vec<Vec<Arc<SstHandle>>>) {
        self.replace_levels(levels);
    }

    /// Build a handle for an already-on-disk SSTable id (used by clone).
    pub(crate) fn open_sst(&self, meta: SstMeta) -> Result<Arc<SstHandle>> {
        let reader = self.open_reader_for(&meta)?;
        Ok(Arc::new(SstHandle { meta, reader }))
    }

    /// Snapshot the live partition rules. Compaction takes one snapshot per run
    /// so a rule added mid-run cannot change that run's cut boundaries — it
    /// takes effect on the next bottom compaction.
    pub(crate) fn partition_rules_snapshot(&self) -> Vec<PartitionRule> {
        self.live_partition_rules.read().clone()
    }

    /// The effective durable config: `opts` with its partition rules replaced by
    /// the live set. Every path that (re)encodes the config for persistence —
    /// `DbInner::persist_manifest`, `freeze_part`, CF copy/clear — goes through
    /// this so a live-added rule round-trips across reopen.
    pub(crate) fn effective_config(&self) -> ColumnFamilyConfig {
        let mut cfg = self.opts.clone();
        cfg.partition_rules = self.live_partition_rules.read().clone();
        cfg
    }

    /// Append `rule` to the live partition rules after validating the resulting
    /// set with the same check [`ColumnFamilyConfig::validate`] runs at create
    /// time (an exact-duplicate prefix is rejected). Validation and the append
    /// happen under one write-lock acquisition, so two concurrent adds serialize
    /// and the second observes the first (rejecting a duplicate). In-memory only;
    /// the caller persists the manifest.
    pub(crate) fn append_partition_rule(&self, rule: PartitionRule) -> Result<()> {
        let mut rules = self.live_partition_rules.write();
        let mut candidate = self.opts.clone();
        candidate.partition_rules = rules.clone();
        candidate.partition_rules.push(rule.clone());
        candidate.validate().map_err(OndaError::InvalidArgs)?;
        rules.push(rule);
        Ok(())
    }

    /// Remove the partition rule whose prefix exactly equals `prefix` from the
    /// live set, returning [`OndaError::NotFound`] if none matches. In-memory
    /// only; the caller persists the manifest. Symmetric with
    /// [`append_partition_rule`](Self::append_partition_rule): write-side-only,
    /// so already-materialized bottom parts keep their stamps until a later
    /// compaction rewrites them.
    pub(crate) fn remove_partition_rule(&self, prefix: &[u8]) -> Result<()> {
        let mut rules = self.live_partition_rules.write();
        let before = rules.len();
        rules.retain(|r| r.prefix != prefix);
        if rules.len() == before {
            return Err(OndaError::NotFound);
        }
        Ok(())
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
    pub(crate) fn remove_bottom_tables(&self, ids: &[u64]) -> usize {
        let mut s = self.state.write();
        let Some(bottom) = s.levels.last_mut() else {
            return 0;
        };
        let before = bottom.len();
        bottom.retain(|h| !ids.contains(&h.meta.id));
        before - bottom.len()
    }

    /// Whether `[min_key, max_key]` overlaps any live bottom-level table. An
    /// attached part with no overlap can slot straight into the bottom level;
    /// otherwise it must go to L0 (which permits overlapping tables).
    pub(crate) fn bottom_overlaps(&self, min_key: &[u8], max_key: &[u8]) -> bool {
        let s = self.state.read();
        let Some(bottom) = s.levels.last() else {
            return false;
        };
        bottom.iter().any(|h| {
            self.cmp.compare(min_key, &h.meta.max_key).is_le()
                && self.cmp.compare(&h.meta.min_key, max_key).is_le()
        })
    }

    /// Index of the bottom (last) level.
    pub(crate) fn bottom_level_index(&self) -> usize {
        self.state.read().levels.len().saturating_sub(1)
    }

    /// Insert `handle` into the bottom level, keeping it sorted by `min_key`
    /// (the invariant leveled reads rely on for binary search).
    pub(crate) fn insert_bottom_sorted(&self, handle: Arc<SstHandle>) {
        let mut s = self.state.write();
        let cmp = self.cmp.clone();
        let bottom = s.levels.last_mut().expect("at least one level always exists");
        bottom.push(handle);
        bottom.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
    }

    /// Replace the bottom-level tables with these ids by `replacements` (same
    /// ids, new handles/metas) under the state write-lock. Used by the tier
    /// mover to swap in relocated handles; in-flight reads finish on the old
    /// handles they already hold.
    pub(crate) fn swap_bottom_tables(&self, replacements: Vec<Arc<SstHandle>>) {
        let mut s = self.state.write();
        let cmp = self.cmp.clone();
        let Some(bottom) = s.levels.last_mut() else {
            return;
        };
        let ids: std::collections::HashSet<u64> =
            replacements.iter().map(|h| h.meta.id).collect();
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
fn bound_to_owned(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k.to_vec()),
        Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
    }
}
