//! Database: column-family registry, global commit-sequence management, snapshot
//! tracking, background flush/compaction workers, recovery, and the durable
//! manifest.
//!
//! the publish-sequence machinery
//! advances the visible sequence gap-free as concurrent commits complete, and
//! background work runs on std threads fed by crossbeam channels.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{unbounded, Receiver};
use parking_lot::{Mutex, RwLock};

use crate::cache::{BlockCache, FileCache};
use crate::column_family::{CfCtx, ColumnFamily, FlushJob};
use crate::compaction;
use crate::comparator::comparator_by_name;
use crate::config::{ColumnFamilyConfig, Options};
use crate::error::{OndaError, Result};
use crate::manifest::{manifest_path, CfManifest, Manifest};

const MAX_CF_NAME_LEN: usize = 128;
const WORKER_TICK: Duration = Duration::from_millis(50);

struct PublishState {
    cursor: u64,                  // next start sequence expected to publish
    completed: HashMap<u64, u64>, // start -> end of completed-but-unpublished ranges
}

/// Deferred-deletion control for consistent checkpoints/backups. While
/// `disabled > 0`, obsolete SSTable files are recorded in `pending` instead of
/// being unlinked, so a snapshot can copy a self-consistent file set even while
/// compaction runs.
#[derive(Default)]
struct FileDeletionState {
    disabled: u32,
    pending: Vec<String>,
}

/// Internal database state shared with workers and column families.
pub struct DbInner {
    pub(crate) opts: Options,
    pub(crate) dir: String,
    pub(crate) cfs: RwLock<HashMap<String, Arc<ColumnFamily>>>,
    /// CFs keyed by their stable id, for unified-memtable flush routing.
    pub(crate) cf_by_id: RwLock<HashMap<u64, Arc<ColumnFamily>>>,
    pub(crate) ctx: Arc<CfCtx>,
    pub(crate) unified: Option<Arc<crate::unified::UnifiedStore>>,

    next_seq: AtomicU64,
    visible: AtomicU64,
    publish: Mutex<PublishState>,
    snapshots: Mutex<BTreeMap<u64, usize>>,
    pub(crate) commit_mu: Mutex<()>,

    next_file_id: AtomicU64,
    pub(crate) closing: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    pub(crate) pending_flush: Arc<AtomicUsize>,

    /// Serializes manifest rebuild+write. Multiple flush workers, the compaction
    /// worker, and CF create/drop all call `persist_manifest` concurrently; without
    /// this they would race on the shared temp file and could publish a torn manifest.
    manifest_mu: Mutex<()>,

    file_deletion: Mutex<FileDeletionState>,

    workers: Mutex<Vec<JoinHandle<()>>>,

    /// Holds the OS advisory lock on `<dir>/LOCK` for the lifetime of the open
    /// database (exclusive for read-write, shared for read-only). Dropped — and
    /// thereby released — at the end of `close()`.
    lock_file: Mutex<Option<std::fs::File>>,

    /// Fail-stop flag: tripped by any durability failure (WAL fsync, background
    /// flush, manifest persist); checked at every write commit.
    pub(crate) poison: Arc<crate::util::Poison>,
}

/// RAII guard that pauses obsolete-SSTable deletion while held (see
/// [`DbInner::pause_deletions`]); deferred files are unlinked when the last guard
/// drops.
pub(crate) struct DeletionPause<'a> {
    inner: &'a DbInner,
}

impl Drop for DeletionPause<'_> {
    fn drop(&mut self) {
        self.inner.resume_deletions();
    }
}

impl std::fmt::Debug for DbInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbInner").field("dir", &self.dir).finish()
    }
}

/// A handle to an open ondaDB database.
#[derive(Debug, Clone)]
pub struct DB {
    pub(crate) inner: Arc<DbInner>,
}

impl DbInner {
    pub(crate) fn reserve_seq(&self, n: u64) -> u64 {
        self.next_seq.fetch_add(n, Ordering::SeqCst)
    }

    /// Mark `[start, end)` committed; advance the visible sequence gap-free.
    pub(crate) fn publish_range(&self, start: u64, end: u64) {
        let mut p = self.publish.lock();
        p.completed.insert(start, end);
        loop {
            let cursor = p.cursor;
            match p.completed.remove(&cursor) {
                Some(e) => p.cursor = e,
                None => break,
            }
        }
        let visible = p.cursor.saturating_sub(1);
        self.visible.store(visible, Ordering::SeqCst);
    }

    /// Highest fully-published (visible) sequence.
    pub(crate) fn visible_seq(&self) -> u64 {
        self.visible.load(Ordering::SeqCst)
    }

    pub(crate) fn acquire_snapshot(&self, seq: u64) -> u64 {
        *self.snapshots.lock().entry(seq).or_insert(0) += 1;
        seq
    }

    pub(crate) fn release_snapshot(&self, seq: u64) {
        let mut s = self.snapshots.lock();
        if let Some(c) = s.get_mut(&seq) {
            *c -= 1;
            if *c == 0 {
                s.remove(&seq);
            }
        }
    }

    /// The oldest live snapshot sequence, or the visible sequence if none.
    pub(crate) fn oldest_snapshot(&self) -> u64 {
        self.snapshots
            .lock()
            .keys()
            .next()
            .copied()
            .unwrap_or_else(|| self.visible_seq())
    }

    pub(crate) fn next_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::SeqCst)
    }

    fn observe_seq(&self, seq: u64) {
        if seq == 0 {
            return;
        }
        // Bump next_seq/visible/cursor past a recovered sequence.
        let mut cur = self.next_seq.load(Ordering::SeqCst);
        while seq + 1 > cur {
            match self.next_seq.compare_exchange_weak(
                cur,
                seq + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
        let mut p = self.publish.lock();
        if seq + 1 > p.cursor {
            p.cursor = seq + 1;
            self.visible.store(seq, Ordering::SeqCst);
        }
    }

    /// Rebuild and atomically persist the manifest.
    pub(crate) fn persist_manifest(&self) -> Result<()> {
        if self.opts.read_only {
            return Ok(());
        }
        // Serialize the whole rebuild+write so concurrent callers (flush workers,
        // the compaction worker, CF create/drop) can never race on the temp file
        // or publish an inconsistent manifest.
        let _mu = self.manifest_mu.lock();
        let cfs = self.cfs.read();
        let mut m = Manifest {
            next_file_id: self.next_file_id.load(Ordering::SeqCst),
            global_seq: self.visible_seq(),
            cfs: Vec::new(),
        };
        for cf in cfs.values() {
            m.cfs.push(CfManifest {
                name: cf.name().to_string(),
                config: cf.opts.encode(),
                sstables: cf.snapshot_ssts(),
            });
        }
        let res = m.save(manifest_path(&self.dir));
        if let Err(e) = &res {
            // A failed manifest write is a durability failure: fsync may have
            // dropped pages, and every caller's WAL-reclaim / file-delete step
            // depends on this succeeding. Fail-stop rather than limp on.
            self.poison.set(format!("manifest persist failed: {e}"));
        }
        res
    }

    pub(crate) fn cf_dir(&self, name: &str) -> String {
        format!("{}/cf-{}", self.dir, name)
    }

    /// Unlink an obsolete SSTable file, or defer it if deletions are paused (a
    /// checkpoint/backup is copying a consistent file set). Compaction routes all
    /// input-file removals through here.
    pub(crate) fn remove_sst_file(&self, path: &str) {
        let mut s = self.file_deletion.lock();
        if s.disabled > 0 {
            s.pending.push(path.to_string());
        } else {
            drop(s);
            let _ = std::fs::remove_file(path);
        }
    }

    /// Pause obsolete-file deletion for the lifetime of the returned guard. Nested
    /// pauses are counted; deferred files are unlinked when the last guard drops.
    pub(crate) fn pause_deletions(&self) -> DeletionPause<'_> {
        self.file_deletion.lock().disabled += 1;
        DeletionPause { inner: self }
    }

    fn resume_deletions(&self) {
        let drained = {
            let mut s = self.file_deletion.lock();
            s.disabled = s.disabled.saturating_sub(1);
            if s.disabled == 0 {
                std::mem::take(&mut s.pending)
            } else {
                Vec::new()
            }
        };
        for p in drained {
            let _ = std::fs::remove_file(p);
        }
    }
}

impl DB {
    /// Open (creating if needed) the database at `opts.path`.
    pub fn open(opts: Options) -> Result<DB> {
        if opts.path.is_empty() {
            return Err(OndaError::InvalidArgs("empty path".into()));
        }
        std::fs::create_dir_all(&opts.path)?;
        let dir = opts.path.clone();

        // Single-process guard: hold an advisory lock on <dir>/LOCK for the
        // lifetime of the DB. Read-write opens take it exclusive; read-only
        // opens take it shared so concurrent readers coexist but a writer is
        // excluded. The lock dies with the fd, so a crashed process never
        // leaves a stale lock behind.
        let lock_file = acquire_dir_lock(&dir, opts.read_only)?;

        let fc = Arc::new(FileCache::new(opts.max_open_sstables.max(1)));
        let bc = Arc::new(BlockCache::new(opts.block_cache_size as i64));

        let (flush_tx, flush_rx) = unbounded::<FlushJob>();
        let (compact_tx, compact_rx) = unbounded::<Arc<ColumnFamily>>();
        let poison = Arc::new(crate::util::Poison::new());
        let closing = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let pending_flush = Arc::new(AtomicUsize::new(0));

        // Unified memtable (shared across CFs), if enabled.
        let mut unified_max_seq = 0u64;
        let unified = if opts.unified_memtable {
            let (store, max_seq) = crate::unified::UnifiedStore::open(
                &dir,
                &opts,
                flush_tx.clone(),
                pending_flush.clone(),
                closing.clone(),
                poison.clone(),
            )?;
            unified_max_seq = max_seq;
            Some(store)
        } else {
            None
        };

        let ctx = Arc::new(CfCtx {
            fc,
            bc,
            flush_tx,
            compact_tx,
            closing: closing.clone(),
            read_only: opts.read_only,
            pending_flush: pending_flush.clone(),
            unified: unified.clone(),
            poison: poison.clone(),
        });

        let manifest = Manifest::load(manifest_path(&dir))?;

        let inner = Arc::new(DbInner {
            opts: opts.clone(),
            dir,
            cfs: RwLock::new(HashMap::new()),
            cf_by_id: RwLock::new(HashMap::new()),
            ctx,
            unified,
            next_seq: AtomicU64::new(manifest.global_seq + 1),
            visible: AtomicU64::new(manifest.global_seq),
            publish: Mutex::new(PublishState {
                cursor: manifest.global_seq + 1,
                completed: HashMap::new(),
            }),
            snapshots: Mutex::new(BTreeMap::new()),
            commit_mu: Mutex::new(()),
            next_file_id: AtomicU64::new(manifest.next_file_id.max(1)),
            closing,
            stop,
            pending_flush,
            manifest_mu: Mutex::new(()),
            file_deletion: Mutex::new(FileDeletionState::default()),
            workers: Mutex::new(Vec::new()),
            lock_file: Mutex::new(Some(lock_file)),
            poison,
        });
        inner.observe_seq(unified_max_seq);

        // Recover column families from the manifest.
        for cfm in &manifest.cfs {
            let cfg = ColumnFamilyConfig::decode(&cfm.config);
            let cmp = comparator_by_name(&cfg.comparator_name).ok_or_else(|| {
                OndaError::InvalidArgs(format!("unknown comparator {}", cfg.comparator_name))
            })?;
            let (cf, max_seq) = ColumnFamily::load(
                inner.ctx.clone(),
                cfm.name.clone(),
                inner.cf_dir(&cfm.name),
                cfg,
                cmp,
                &cfm.sstables,
            )?;
            inner.observe_seq(max_seq);
            inner.cf_by_id.write().insert(cf.id(), cf.clone());
            inner.cfs.write().insert(cfm.name.clone(), cf);
        }

        if !opts.read_only {
            spawn_workers(&inner, flush_rx, compact_rx);
        }
        Ok(DB { inner })
    }

    /// Create a new column family.
    pub fn create_column_family(
        &self,
        name: &str,
        config: ColumnFamilyConfig,
    ) -> Result<Arc<ColumnFamily>> {
        if name.is_empty() || name.len() > MAX_CF_NAME_LEN {
            return Err(OndaError::InvalidArgs("invalid column family name".into()));
        }
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        let cmp = comparator_by_name(&config.comparator_name).ok_or_else(|| {
            OndaError::InvalidArgs(format!("unknown comparator {}", config.comparator_name))
        })?;
        let mut cfs = self.inner.cfs.write();
        if cfs.contains_key(name) {
            return Err(OndaError::Exists(name.into()));
        }
        let cf = ColumnFamily::create(
            self.inner.ctx.clone(),
            name.to_string(),
            self.inner.cf_dir(name),
            config,
            cmp,
        )?;
        cfs.insert(name.to_string(), cf.clone());
        drop(cfs);
        self.inner.cf_by_id.write().insert(cf.id(), cf.clone());
        self.inner.persist_manifest()?;
        Ok(cf)
    }

    /// Look up a column family by name.
    pub fn get_column_family(&self, name: &str) -> Option<Arc<ColumnFamily>> {
        self.inner.cfs.read().get(name).cloned()
    }

    /// List column family names.
    pub fn list_column_families(&self) -> Vec<String> {
        self.inner.cfs.read().keys().cloned().collect()
    }

    /// Drop a column family and delete its files.
    pub fn drop_column_family(&self, name: &str) -> Result<()> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        let cf = {
            let mut cfs = self.inner.cfs.write();
            cfs.remove(name).ok_or(OndaError::NotFound)?
        };
        cf.close_resources();
        let _ = std::fs::remove_dir_all(self.inner.cf_dir(name));
        self.inner.persist_manifest()?;
        Ok(())
    }

    /// Flush a column family's active memtable to an SSTable (blocks until the
    /// flush is enqueued and drained).
    pub fn flush_memtable(&self, cf: &Arc<ColumnFamily>) -> Result<()> {
        cf.rotate_memtable(true);
        while self.inner.pending_flush.load(Ordering::SeqCst) > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    /// Trigger compaction on a column family and wait for it to settle.
    pub fn compact(&self, cf: &Arc<ColumnFamily>) -> Result<()> {
        compaction::run(&self.inner, cf)
    }

    /// Force an fsync of every write-ahead log (all column families plus the
    /// unified store, when enabled).
    ///
    /// Gives [`SyncMode::None`](crate::SyncMode::None) /
    /// [`SyncMode::Interval`](crate::SyncMode::Interval) users an explicit
    /// durability point: when this returns `Ok`, every write committed before
    /// the call is on disk. A failed sync fail-stops the database (see
    /// [`poisoned`](Self::poisoned)).
    pub fn sync_wal(&self) -> Result<()> {
        self.inner.poison.check()?;
        if let Some(u) = &self.inner.unified {
            u.sync_wal()?;
        }
        let cfs: Vec<Arc<ColumnFamily>> = self.inner.cfs.read().values().cloned().collect();
        for cf in &cfs {
            cf.sync_wal()?;
        }
        Ok(())
    }

    /// If the database has fail-stopped after a durability failure (failed
    /// fsync, background flush, or manifest persist), returns the reason.
    /// While poisoned, every write commit fails with
    /// [`OndaError::Poisoned`](crate::OndaError::Poisoned); reads keep working.
    /// The only recovery is to reopen the database.
    pub fn poisoned(&self) -> Option<String> {
        self.inner.poison.reason()
    }

    /// Close the database: flush all memtables, stop workers, fsync, persist.
    pub fn close(&self) -> Result<()> {
        if self.inner.closing.swap(true, Ordering::SeqCst) {
            return Ok(()); // already closing
        }
        // Enqueue final flushes for every column family (and the unified store).
        let cfs: Vec<Arc<ColumnFamily>> = self.inner.cfs.read().values().cloned().collect();
        if !self.inner.opts.read_only {
            for cf in &cfs {
                cf.rotate_memtable(true);
            }
            if let Some(u) = &self.inner.unified {
                u.rotate(true);
            }
            // Wait for the flush queue to drain.
            while self.inner.pending_flush.load(Ordering::SeqCst) > 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        self.inner.stop.store(true, Ordering::SeqCst);
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *self.inner.workers.lock());
        for h in handles {
            let _ = h.join();
        }
        let _ = self.inner.persist_manifest();
        if let Some(u) = &self.inner.unified {
            u.close();
        }
        for cf in &cfs {
            cf.close_resources();
        }
        // Release the directory lock last, once all state is durable, so a
        // concurrent open never sees a half-closed database.
        *self.inner.lock_file.lock() = None;
        Ok(())
    }
}

/// Acquire the advisory lock on `<dir>/LOCK` (exclusive unless `read_only`).
fn acquire_dir_lock(dir: &str, read_only: bool) -> Result<std::fs::File> {
    use std::fs::TryLockError;
    let path = std::path::Path::new(dir).join("LOCK");
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    let res = if read_only {
        f.try_lock_shared()
    } else {
        f.try_lock()
    };
    match res {
        Ok(()) => Ok(f),
        Err(TryLockError::WouldBlock) => Err(OndaError::Locked(format!(
            "database at {dir} is locked by another process or handle"
        ))),
        Err(TryLockError::Error(e)) => Err(e.into()),
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        // Close only when this is the last handle (workers hold no DB clone).
        if Arc::strong_count(&self.inner) == 1 {
            let _ = self.close();
        }
    }
}

fn spawn_workers(
    inner: &Arc<DbInner>,
    flush_rx: Receiver<FlushJob>,
    compact_rx: Receiver<Arc<ColumnFamily>>,
) {
    let n_flush = inner.opts.num_flush_threads.max(1);
    let mut handles = Vec::new();
    for _ in 0..n_flush {
        let db = inner.clone();
        let rx = flush_rx.clone();
        let stop = inner.stop.clone();
        handles.push(
            std::thread::Builder::new()
                .name("onda-flush".into())
                .spawn(move || flush_worker(db, rx, stop))
                .expect("spawn flush worker"),
        );
    }
    {
        let db = inner.clone();
        let rx = compact_rx;
        let stop = inner.stop.clone();
        handles.push(
            std::thread::Builder::new()
                .name("onda-compact".into())
                .spawn(move || compact_worker(db, rx, stop))
                .expect("spawn compaction worker"),
        );
    }
    *inner.workers.lock() = handles;
}

fn flush_worker(db: Arc<DbInner>, rx: Receiver<FlushJob>, stop: Arc<AtomicBool>) {
    loop {
        match rx.recv_timeout(WORKER_TICK) {
            Ok(FlushJob::PerCf { cf, imm }) => {
                let id = db.next_file_id();
                match cf.flush_imm(&imm, id) {
                    Ok(wal_paths) => {
                        // Only reclaim the WAL once the manifest that references the new
                        // SSTable is durable. If the manifest write fails, the SSTable is
                        // orphaned but the data is still in the WAL and recovers on reopen.
                        if db.persist_manifest().is_ok() {
                            for p in wal_paths {
                                crate::wal::remove_wal_files(p);
                            }
                        }
                        if !db.closing.load(Ordering::Relaxed)
                            && cf.l0_len() >= cf.opts.l1_file_count_trigger as usize
                        {
                            let _ = db.ctx.compact_tx.send(cf.clone());
                        }
                    }
                    Err(e) => {
                        // The data is still in the WAL, but a failed background
                        // flush (SST write/fsync) means durability can no longer
                        // be promised for new writes — fail-stop.
                        db.poison.set(format!("background flush failed: {e}"));
                    }
                }
                db.pending_flush.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(FlushJob::Unified { imm }) => {
                // Split the shared memtable by CF and flush each slice to L0.
                for (cf_id, entries) in crate::unified::split_by_cf(&imm) {
                    if let Some(cf) = db.cf_by_id.read().get(&cf_id).cloned() {
                        let file_id = db.next_file_id();
                        if let Err(e) = cf.ingest_l0(entries, file_id) {
                            db.poison.set(format!("unified flush failed: {e}"));
                        }
                        if !db.closing.load(Ordering::Relaxed)
                            && cf.l0_len() >= cf.opts.l1_file_count_trigger as usize
                        {
                            let _ = db.ctx.compact_tx.send(cf.clone());
                        }
                    }
                }
                // As above: only drop the WAL once the manifest is durable.
                if db.persist_manifest().is_ok() {
                    for p in &imm.wal_paths {
                        crate::wal::remove_wal_files(p);
                    }
                }
                if let Some(u) = &db.unified {
                    u.remove_imm(&imm);
                }
                db.pending_flush.fetch_sub(1, Ordering::SeqCst);
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn compact_worker(db: Arc<DbInner>, rx: Receiver<Arc<ColumnFamily>>, stop: Arc<AtomicBool>) {
    loop {
        match rx.recv_timeout(WORKER_TICK) {
            Ok(cf) => {
                let _ = compaction::run(&db, &cf);
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poisoned_db_rejects_writes_allows_reads() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        assert!(db.poisoned().is_none());

        db.inner.poison.set("simulated fsync failure".into());

        match db.put(&cf, b"k2", b"v", Duration::ZERO) {
            Err(OndaError::Poisoned(m)) => assert!(m.contains("simulated")),
            other => panic!("expected Poisoned, got {other:?}"),
        }
        assert_eq!(db.poisoned().as_deref(), Some("simulated fsync failure"));
        // Reads keep working on a poisoned DB; only new commits are refused.
        assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
        db.close().unwrap();
    }
}
