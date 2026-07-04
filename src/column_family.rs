//! Column family: an isolated, independently-configured key-value store within a
//! database, backed by its own memtable, WAL and LSM levels.
//!
//! read path, memtable rotation, flush,
//! recovery, adapted to Rust ownership: SSTable handles are reference-counted
//! with `Arc` (replacing the manual incref/decref), and backpressure uses a
//! `parking_lot` `Mutex`/`Condvar`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_channel::Sender;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::cache::{BlockCache, FileCache};
use crate::comparator::ComparatorRef;
use crate::config::ColumnFamilyConfig;
use crate::error::{OndaError, Result};
use crate::iterator::{ChildIter, Iterator};
use crate::manifest::SstMeta;
use crate::memtable::Memtable;
use crate::sst::{Reader, Writer, WriterOptions};
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
    pub fc: Arc<FileCache>,
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
    cmp: ComparatorRef,

    state: RwLock<CfState>,
    rot: Mutex<RotState>,
    cond: Condvar,

    pub(crate) flushing: AtomicBool,
    pub(crate) compacting: AtomicBool,
    commit_hook: Mutex<Option<CommitHookFn>>,
    /// Mirrors `commit_hook.is_some()`; lets the commit path skip building hook
    /// payloads (a per-op key+value clone) with a relaxed load instead of a lock.
    hook_set: AtomicBool,

    pub(crate) flush_count: AtomicU64,
    pub(crate) compaction_count: AtomicU64,
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
        let cf = Arc::new(ColumnFamily {
            ctx,
            id: crate::unified::cf_id(&name),
            name,
            dir,
            opts,
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
            commit_hook: Mutex::new(None),
            hook_set: AtomicBool::new(false),
            flush_count: AtomicU64::new(0),
            compaction_count: AtomicU64::new(0),
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
            let klog = format!("{dir}/{}.klog", s.id);
            let reader = Reader::open(&klog, ctx.fc.clone(), ctx.bc.clone(), s.id, cmp.clone())?;
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

        let cf = Arc::new(ColumnFamily {
            ctx,
            id: crate::unified::cf_id(&name),
            name,
            dir,
            opts,
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
            commit_hook: Mutex::new(None),
            hook_set: AtomicBool::new(false),
            flush_count: AtomicU64::new(0),
            compaction_count: AtomicU64::new(0),
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
        #[cfg(feature = "unsafe-fastpath")]
        self.write_l0_streaming(&imm.mem, file_id)?;
        #[cfg(not(feature = "unsafe-fastpath"))]
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

    /// Finish `w` and register the resulting SSTable as the newest L0 file.
    fn install_l0(&self, w: Writer, file_id: u64) -> Result<()> {
        let klog = self.klog_path(file_id);
        let meta = w.finish()?.to_sst_meta(file_id, 0);
        let reader = Reader::open(
            &klog,
            self.ctx.fc.clone(),
            self.ctx.bc.clone(),
            file_id,
            self.cmp.clone(),
        )?;
        let handle = Arc::new(SstHandle { meta, reader });
        self.state.write().levels[0].insert(0, handle); // newest first
        Ok(())
    }

    /// Stream a sealed memtable to a new L0 SSTable without materializing
    /// entries (keys/values borrowed from the arena through the merge).
    #[cfg(feature = "unsafe-fastpath")]
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
            compression: self.opts.compression,
            cmp: self.cmp.clone(),
            enable_bloom: self.opts.enable_bloom_filter,
            bloom_fpr: self.opts.bloom_fpr,
            klog_value_threshold: self.opts.klog_value_threshold,
            block_size: DATA_BLOCK_SIZE,
            expected_entries: expected,
            use_btree: self.opts.use_btree,
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
        let now = now_nanos();
        let (mem, imms, tables) = {
            let s = self.state.read();
            let mem = s.mem.clone();
            let imms: Vec<Arc<ImmMemtable>> = s.imm.clone();
            let mut tables: Vec<Arc<SstHandle>> = Vec::new();
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
            let (v, seq, found, deleted) = th.reader.get(user_key, read_seq, now)?;
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
            let mut tables: Vec<Arc<SstHandle>> = Vec::new();
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

    /// Build a snapshot iterator. `extra` is an optional transaction overlay
    /// memtable consulted as the newest source.
    pub(crate) fn new_iterator(&self, read_seq: u64, extra: Option<Arc<Memtable>>) -> Iterator {
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
            children.push(ChildIter::Sst(th.reader.iter()));
        }
        for lvl in s.levels.iter().skip(1) {
            for th in lvl {
                children.push(ChildIter::Sst(th.reader.iter()));
            }
        }
        drop(s);
        Iterator::new(self.cmp.clone(), children, read_seq, now_nanos())
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

    /// Install pre-built level handles (used by clone).
    pub(crate) fn install_levels(&self, levels: Vec<Vec<Arc<SstHandle>>>) {
        self.replace_levels(levels);
    }

    /// Build a handle for an already-on-disk SSTable id (used by clone).
    pub(crate) fn open_sst(&self, meta: SstMeta) -> Result<Arc<SstHandle>> {
        let klog = self.klog_path(meta.id);
        let reader = Reader::open(
            &klog,
            self.ctx.fc.clone(),
            self.ctx.bc.clone(),
            meta.id,
            self.cmp.clone(),
        )?;
        Ok(Arc::new(SstHandle { meta, reader }))
    }

    pub(crate) fn config(&self) -> &ColumnFamilyConfig {
        &self.opts
    }
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
