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
    /// Number of live [`DB`] handles.
    ///
    /// `Drop` cannot use `Arc::strong_count(&inner)` to decide whether it is
    /// the last handle: the flush and compaction workers each hold an
    /// `Arc<DbInner>` clone, so that count never reaches 1 while the database
    /// is running, and the close-on-drop path was therefore dead. Counting
    /// handles explicitly separates "the user still has a `DB`" from "a worker
    /// still holds the inner state".
    pub(crate) handles: Arc<std::sync::atomic::AtomicUsize>,
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
#[derive(Debug)]
pub struct DB {
    pub(crate) inner: Arc<DbInner>,
}

thread_local! {
    /// Highest sequence committed BY THIS THREAD, per DB instance.
    ///
    /// `visible_seq` advances gap-free: while an earlier-reserved commit
    /// from another thread is still in flight, a thread's OWN completed
    /// commit sits above the watermark and a read at `visible_seq` misses
    /// it — breaking read-your-own-writes for read-modify-write callers
    /// (found by marekvs's chaos suite: INCR under concurrent load silently
    /// lost ~2-6% of increments). ReadCommitted reads therefore use
    /// `max(visible_seq, own floor)`. Keyed by DbInner address; entries
    /// die with the thread.
    static THREAD_COMMIT_FLOOR: std::cell::RefCell<std::collections::HashMap<usize, u64>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

impl DbInner {
    pub(crate) fn reserve_seq(&self, n: u64) -> u64 {
        self.next_seq.fetch_add(n, Ordering::SeqCst)
    }

    fn db_key(&self) -> usize {
        self as *const DbInner as usize
    }

    /// Record that this thread committed up to `seq` (called post-publish).
    pub(crate) fn note_thread_commit(&self, seq: u64) {
        let k = self.db_key();
        THREAD_COMMIT_FLOOR.with(|f| {
            let mut m = f.borrow_mut();
            let e = m.entry(k).or_insert(0);
            if seq > *e {
                *e = seq;
            }
        });
    }

    /// Read sequence for ReadCommitted point reads: the published watermark,
    /// raised to this thread's own last commit (read-your-own-writes).
    /// Fixed-snapshot transactions keep `visible_seq` — the floor may sit
    /// inside a publication gap, which is fine for read-committed semantics
    /// but not for a repeatable snapshot.
    pub(crate) fn read_floor_seq(&self) -> u64 {
        let own = THREAD_COMMIT_FLOOR.with(|f| f.borrow().get(&self.db_key()).copied());
        self.visible_seq().max(own.unwrap_or(0))
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
                config: cf.effective_config().encode(),
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

        // Storage-tier registry: the default tier is the DB directory; every
        // configured tier gets its own LocalStorage over the shared file cache.
        // The default tier permits mmap (when the feature is built); a named tier
        // honors its `supports_mmap` flag so a slow/remote-style mount can force
        // the buffered pread path. The name "ssd" is reserved for the default.
        let default_storage = crate::storage::LocalStorage::new(fc.clone(), true);
        let mut extra_tiers: Vec<(String, String, Arc<dyn crate::storage::Storage>)> = Vec::new();
        for t in &opts.tiers {
            if t.name == "ssd" {
                continue;
            }
            let storage: Arc<dyn crate::storage::Storage> = match &t.backend {
                crate::config::TierBackend::Local => {
                    crate::storage::LocalStorage::new(fc.clone(), t.supports_mmap)
                }
                #[cfg(feature = "s3")]
                crate::config::TierBackend::S3(cfg) => crate::storage_s3::S3Storage::new(cfg)?,
                // A caller-provided backend is used verbatim (P8): the embedder
                // already built (and wrapped) it.
                crate::config::TierBackend::Custom(s) => s.clone(),
            };
            extra_tiers.push((t.name.clone(), t.root.clone(), storage));
        }
        let tiers = Arc::new(crate::storage::TierRegistry::new(
            dir.clone(),
            default_storage,
            extra_tiers,
        )?);

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
            tiers,
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
            handles: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            poison,
        });
        inner.observe_seq(unified_max_seq);

        // Recover column families from the manifest.
        for cfm in &manifest.cfs {
            let mut cfg = ColumnFamilyConfig::decode(&cfm.config);
            let cmp = comparator_by_name(&cfg.comparator_name).ok_or_else(|| {
                OndaError::InvalidArgs(format!("unknown comparator {}", cfg.comparator_name))
            })?;
            // A derived partitioner is persisted by name only; exchange it for
            // the registered implementation, exactly as the comparator above is
            // resolved. Failing here — rather than proceeding with rule-based
            // partitioning — is deliberate: the CF's existing parts were cut on
            // derived boundaries, and silently reverting would cut every part
            // written afterwards differently while every operation appeared to
            // succeed. The damage would surface much later, as parts that
            // detach, freeze and tier incorrectly.
            if let crate::config::PartitionScheme::Unresolved(name) = &cfg.partition_scheme {
                let found = opts
                    .partition_fns
                    .iter()
                    .find(|f| f.scheme_name() == name)
                    .cloned()
                    .ok_or_else(|| {
                        OndaError::InvalidArgs(format!(
                            "column family {:?} was written with derived partition scheme {name:?}, \
                             which is not registered in Options::partition_fns",
                            cfm.name
                        ))
                    })?;
                cfg.partition_scheme = crate::config::PartitionScheme::Derived(found);
            }
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
            // Sweep tier-move orphans left by a crash mid-move (a copy on the
            // target before the manifest flip, or a source after it). The
            // manifest — now recovered — is the single source of truth for where
            // each table lives; anything else is deleted. Runs before workers so
            // no background move races the sweep.
            sweep_move_orphans(&inner, &manifest);
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
        config.validate().map_err(OndaError::InvalidArgs)?;
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

    /// Atomically empty a column family, preserving its configuration.
    ///
    /// Implemented as drop + recreate under the registry lock, so concurrent
    /// `get_column_family` callers always see either the full old CF or the
    /// empty new one. Returns the fresh handle; previously obtained handles
    /// become stale (their writes fail), exactly as after
    /// [`drop_column_family`](Self::drop_column_family) + re-create.
    ///
    /// Not supported in unified-memtable mode: the shared memtable still holds
    /// the old entries under the same CF id, so they would resurface.
    pub fn clear_column_family(&self, name: &str) -> Result<Arc<ColumnFamily>> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        if self.inner.unified.is_some() {
            return Err(OndaError::InvalidArgs(
                "clear_column_family is not supported in unified-memtable mode".into(),
            ));
        }
        let mut cfs = self.inner.cfs.write();
        let old = cfs.remove(name).ok_or(OndaError::NotFound)?;
        let cfg = old.effective_config();
        let cmp = comparator_by_name(&cfg.comparator_name).ok_or_else(|| {
            OndaError::InvalidArgs(format!("unknown comparator {}", cfg.comparator_name))
        })?;
        old.close_resources();
        let _ = std::fs::remove_dir_all(self.inner.cf_dir(name));
        let cf = ColumnFamily::create(
            self.inner.ctx.clone(),
            name.to_string(),
            self.inner.cf_dir(name),
            cfg,
            cmp,
        )?;
        cfs.insert(name.to_string(), cf.clone());
        drop(cfs);
        // Same name => same stable id, so this replaces the old routing entry.
        self.inner.cf_by_id.write().insert(cf.id(), cf.clone());
        self.inner.persist_manifest()?;
        Ok(cf)
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

    /// Compact a column family and wait for it to settle: runs every
    /// triggered round, then sweeps all populated levels to the bottom so
    /// tombstones and shadowed versions are reclaimed even when no size
    /// trigger fires (e.g. a fully deleted CF).
    pub fn compact(&self, cf: &Arc<ColumnFamily>) -> Result<()> {
        compaction::run_manual(&self.inner, cf)
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

/// Delete storage-tier files orphaned by a crash mid-move. The recovered
/// `manifest` records, per table id, the tier its files durably live on. A crash
/// between the copy and the manifest flip leaves a copy on the *target* tier that
/// the manifest still attributes to the source; a crash between the flip and the
/// source delete leaves a copy on the *source* tier that the manifest now
/// attributes to the target. In both cases a `<id>.klog`/`<id>.vlog` file sits in
/// a tier directory that disagrees with the manifest's tier for that id — so we
/// delete exactly those. Files whose id the manifest does not know (in-flight
/// flush/compaction output, WALs) are left untouched; correctly-placed files
/// match and are kept.
fn sweep_move_orphans(inner: &Arc<DbInner>, manifest: &Manifest) {
    // Candidate tier locations: the default tier (`None`) plus every configured
    // named tier (the reserved "ssd" name aliases the default).
    let mut locations: Vec<Option<String>> = vec![None];
    for t in &inner.opts.tiers {
        if t.name != "ssd" {
            locations.push(Some(t.name.clone()));
        }
    }
    for cfm in &manifest.cfs {
        let mut tier_of: HashMap<u64, Option<String>> = HashMap::new();
        for s in &cfm.sstables {
            tier_of.insert(s.id, s.tier.clone());
        }
        for loc in &locations {
            let dir = inner.ctx.tiers.cf_dir(loc.as_deref(), &cfm.name);
            let entries = match std::fs::read_dir(&dir) {
                Ok(e) => e,
                Err(_) => continue, // tier dir may not exist yet — nothing to sweep
            };
            for entry in entries.flatten() {
                let fname = entry.file_name();
                let fname = fname.to_string_lossy();
                let Some(id) = parse_sst_file_id(&fname) else {
                    continue; // not an <id>.klog/.vlog (e.g. a WAL or subdir)
                };
                // Delete only when the manifest knows this id but places it on a
                // different tier than the directory we found it in.
                if let Some(manifest_tier) = tier_of.get(&id) {
                    if manifest_tier.as_deref() != loc.as_deref() {
                        let _ = std::fs::remove_file(format!("{dir}/{fname}"));
                    }
                }
            }
        }
    }
}

/// Parse the table id from an SSTable file name (`<id>.klog` or `<id>.vlog`),
/// or `None` for anything else.
fn parse_sst_file_id(name: &str) -> Option<u64> {
    let stem = name
        .strip_suffix(".klog")
        .or_else(|| name.strip_suffix(".vlog"))?;
    stem.parse::<u64>().ok()
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

impl Clone for DB {
    fn clone(&self) -> DB {
        self.inner.handles.fetch_add(1, Ordering::SeqCst);
        DB {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        // Close when the LAST `DB` handle goes away. Using
        // `Arc::strong_count(&self.inner)` here was wrong: the flush and
        // compaction workers hold `Arc<DbInner>` clones for the lifetime of
        // the database, so the count never fell to 1 and this path never ran
        // — leaving the `<dir>/LOCK` advisory lock held until the process
        // exited. Reopening a directory in the same process (restore, fork,
        // restart, or any test doing so) then failed with `Locked`.
        if self.inner.handles.fetch_sub(1, Ordering::SeqCst) == 1 {
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
                        // FIFO CFs enforce their size/age limit after every
                        // flush; leveled CFs wait for the L0 file trigger.
                        let fifo = cf.opts.compaction_style == crate::config::CompactionStyle::Fifo;
                        if !db.closing.load(Ordering::Relaxed)
                            && (fifo || cf.l0_len() >= cf.opts.l1_file_count_trigger as usize)
                        {
                            let _ = db.ctx.compact_tx.send(cf.clone());
                        }
                    }
                    Err(e) => {
                        // The data is still in the WAL, but a failed background
                        // flush (SST write/fsync) means durability can no longer
                        // be promised for new writes — fail-stop. Exception: if
                        // the CF was dropped or cleared while this job was in
                        // flight, the failure is expected (its directory is
                        // gone) and poisoning would take down a healthy DB.
                        let live = db
                            .cfs
                            .read()
                            .get(cf.name())
                            .is_some_and(|c| Arc::ptr_eq(c, &cf));
                        if live {
                            db.poison.set(format!("background flush failed: {e}"));
                        }
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
                        // FIFO CFs enforce their size/age limit after every
                        // flush; leveled CFs wait for the L0 file trigger.
                        let fifo = cf.opts.compaction_style == crate::config::CompactionStyle::Fifo;
                        if !db.closing.load(Ordering::Relaxed)
                            && (fifo || cf.l0_len() >= cf.opts.l1_file_count_trigger as usize)
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
    // The part mover shares this worker: between compaction jobs, once per
    // `part_mover_interval`, run a mover pass (a cheap no-op unless some CF has
    // tier rules and an aged, mis-placed part). ZERO disables the scheduled pass.
    let mover_interval = db.opts.part_mover_interval;
    let mut last_mover = std::time::Instant::now();
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
        if !mover_interval.is_zero()
            && !db.closing.load(Ordering::Relaxed)
            && last_mover.elapsed() >= mover_interval
        {
            last_mover = std::time::Instant::now();
            let _ = db.run_part_mover();
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

    /// A comparable engine's batch commit discarded the journal write error
    /// and published the batch to the memtable anyway, so a later successful
    /// sync falsely implied durability. The contract to pin: a commit that
    /// returns an error must not have published any of its writes.
    #[test]
    fn poisoned_txn_commit_does_not_publish() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&cf, b"pre", b"v", Duration::ZERO).unwrap();

        let mut t = db.begin();
        t.put(&cf, b"staged", b"v", Duration::ZERO).unwrap();
        // The durability failure lands between buffering and commit.
        db.inner.poison.set("simulated wal failure".into());
        match t.commit() {
            Err(OndaError::Poisoned(_)) => {}
            other => panic!("commit on a poisoned db must fail, got {other:?}"),
        }
        match db.get(&cf, b"staged") {
            Err(OndaError::NotFound) => {}
            other => panic!("failed commit must not publish its writes, got {other:?}"),
        }
        assert_eq!(db.get(&cf, b"pre").unwrap(), b"v");
        let _ = db.close(); // close may surface the poison; must not panic
    }
}
