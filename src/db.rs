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

use crossbeam_channel::{unbounded, Receiver, Sender};
use parking_lot::{Mutex, RwLock};

use crate::cache::{BlockCache, FileCache};
use crate::column_family::{CfCtx, ColumnFamily, FlushJob, ImmMemtable};
use crate::compaction;
use crate::comparator::comparator_by_name;
use crate::config::{ColumnFamilyConfig, Options};
use crate::error::{OndaError, Result};
use crate::manifest::{manifest_path, CfManifest, Manifest, WalLayout};

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
    /// Guards the scheduled part-mover pass so only one compaction worker runs
    /// it at a time.
    pub(crate) mover_running: AtomicBool,

    /// Serializes manifest rebuild+write. Multiple flush workers, the compaction
    /// worker, and CF create/drop all call `persist_manifest` concurrently; without
    /// this they would race on the shared temp file and could publish a torn manifest.
    manifest_mu: Mutex<()>,
    /// Durable database-wide WAL layout written by `persist_manifest`.
    wal_layout: Mutex<WalLayout>,
    /// Per-database nonce naming this instance's objects on shared tiers
    /// (A2, `SPADINO-A2.md`). Minted at open when a shared tier is configured
    /// and no nonce is recorded yet; `None` otherwise. Never re-minted:
    /// object names embed it.
    pub(crate) instance_nonce: Mutex<Option<u64>>,

    /// Count of successful manifest persists over this DB's lifetime. Cheap
    /// (a single relaxed increment on an already fsync-bound path); exists so
    /// batch operations can assert they collapse N per-item persists into one.
    manifest_persists: AtomicU64,

    file_deletion: Mutex<FileDeletionState>,

    workers: Mutex<Vec<JoinHandle<()>>>,

    /// Holds the OS advisory lock on `<dir>/LOCK` for the lifetime of the open
    /// database (exclusive for read-write, shared for read-only). Dropped — and
    /// thereby released — at the end of `close()`.
    lock_file: Mutex<Option<std::fs::File>>,

    /// Fail-stop flag: tripped by any durability failure (WAL fsync, background
    /// flush, manifest persist); checked at every write commit.
    pub(crate) poison: Arc<crate::util::Poison>,

    /// Count of successful physical WAL `sync_data` calls across every WAL this
    /// DB has opened (per-CF, unified, and post-rotation). Observability/test
    /// hook (see [`DB::wal_sync_count`]); relaxed increments on an fsync-bound path.
    pub(crate) wal_syncs: Arc<AtomicU64>,
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
        self.visible_seq().max(self.own_commit_floor())
    }

    /// Highest sequence THIS THREAD has committed on this DB, or 0.
    pub(crate) fn own_commit_floor(&self) -> u64 {
        THREAD_COMMIT_FLOOR
            .with(|f| f.borrow().get(&self.db_key()).copied())
            .unwrap_or(0)
    }

    /// Wait (bounded) until the published watermark reaches this thread's own
    /// commit floor.
    ///
    /// A fixed snapshot pinned BELOW the caller's own last commit is a trap:
    /// its write-write conflict check then refuses against the caller's OWN
    /// earlier, strictly-serial write (found live: a raft store's serial
    /// group commits — same thread, same hot HardState key — poisoned
    /// fail-stop whenever a slow commit on another thread straddled two of
    /// them and held the gap-free cursor down; spada S-158). Publication is
    /// guaranteed even for failed applies, so the gap closes as soon as the
    /// in-flight commit publishes — the wait is transient by construction.
    /// The timeout only guards a torn process (a thread that died between
    /// reserve and publish); on expiry the caller proceeds with the plain
    /// watermark, i.e. exactly the pre-fix behaviour.
    pub(crate) fn wait_visible_at_own_floor(&self) {
        let floor = self.own_commit_floor();
        if self.visible_seq() >= floor {
            return;
        }
        let start = std::time::Instant::now();
        while self.visible_seq() < floor && start.elapsed() < std::time::Duration::from_secs(1) {
            std::thread::yield_now();
        }
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

    pub(crate) fn observe_seq(&self, seq: u64) {
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
            wal_layout: *self.wal_layout.lock(),
            instance_nonce: self.instance_nonce.lock().to_owned(),
        };
        for cf in cfs.values() {
            m.cfs.push(CfManifest {
                name: cf.name().to_string(),
                config: cf.effective_config().encode(),
                sstables: cf.snapshot_ssts(),
            });
        }
        let res = m.save(manifest_path(&self.dir));
        match &res {
            Ok(()) => {
                self.manifest_persists.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                // A failed manifest write is a durability failure: fsync may have
                // dropped pages, and every caller's WAL-reclaim / file-delete step
                // depends on this succeeding. Fail-stop rather than limp on.
                self.poison.set(format!("manifest persist failed: {e}"));
            }
        }
        res
    }

    /// Number of successful manifest persists so far (see `manifest_persists`).
    /// A test/observability lever — batch operations assert they persist once.
    #[allow(dead_code)]
    pub(crate) fn manifest_persist_count(&self) -> u64 {
        self.manifest_persists.load(Ordering::Relaxed)
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

struct OpenResources {
    tiers: Arc<crate::storage::TierRegistry>,
    block_cache: Arc<BlockCache>,
    flush_tx: Sender<FlushJob>,
    flush_rx: Receiver<FlushJob>,
    compact_tx: Sender<Arc<ColumnFamily>>,
    compact_rx: Receiver<Arc<ColumnFamily>>,
    poison: Arc<crate::util::Poison>,
    wal_syncs: Arc<AtomicU64>,
    closing: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    pending_flush: Arc<AtomicUsize>,
}

impl OpenResources {
    fn new(opts: &Options, dir: &str) -> Result<Self> {
        let file_cache = Arc::new(FileCache::new(opts.max_open_sstables.max(1)));
        let tiers = build_tier_registry(opts, dir, file_cache)?;
        let (flush_tx, flush_rx) = unbounded::<FlushJob>();
        let (compact_tx, compact_rx) = unbounded::<Arc<ColumnFamily>>();
        Ok(Self {
            tiers,
            block_cache: Arc::new(BlockCache::new(opts.block_cache_size as i64)),
            flush_tx,
            flush_rx,
            compact_tx,
            compact_rx,
            poison: Arc::new(crate::util::Poison::new()),
            wal_syncs: Arc::new(AtomicU64::new(0)),
            closing: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            pending_flush: Arc::new(AtomicUsize::new(0)),
        })
    }
}

struct WorkerReceivers {
    flush: Receiver<FlushJob>,
    compact: Receiver<Arc<ColumnFamily>>,
}

fn build_tier_registry(
    opts: &Options,
    dir: &str,
    file_cache: Arc<FileCache>,
) -> Result<Arc<crate::storage::TierRegistry>> {
    // The default tier permits mmap when built; named local tiers honor their
    // flag so a slow mount can force positioned reads. "ssd" aliases default.
    let default = crate::storage::LocalStorage::new(file_cache.clone(), true);
    let mut extra: Vec<(String, String, Arc<dyn crate::storage::Storage>)> = Vec::new();
    for tier in &opts.tiers {
        if tier.name == "ssd" {
            continue;
        }
        let storage: Arc<dyn crate::storage::Storage> = match &tier.backend {
            crate::config::TierBackend::Local => {
                crate::storage::LocalStorage::new(file_cache.clone(), tier.supports_mmap)
            }
            #[cfg(feature = "s3")]
            crate::config::TierBackend::S3(config) => crate::storage_s3::S3Storage::new(config)?,
            // The embedder owns the construction and wrapping of custom stores.
            crate::config::TierBackend::Custom(storage) => storage.clone(),
        };
        extra.push((tier.name.clone(), tier.root.clone(), storage));
    }
    Ok(Arc::new(crate::storage::TierRegistry::new(
        dir.to_owned(),
        default,
        extra,
    )?))
}

fn requested_wal_layout(opts: &Options) -> WalLayout {
    if opts.unified_memtable {
        WalLayout::Unified
    } else {
        WalLayout::PerColumnFamily
    }
}

fn validate_wal_layout(manifest: &Manifest, requested: WalLayout) -> Result<()> {
    if manifest.cfs.is_empty() || manifest.wal_layout == requested {
        return Ok(());
    }
    Err(OndaError::InvalidArgs(format!(
        "WAL layout mismatch: database is {:?}, requested {:?}",
        manifest.wal_layout, requested
    )))
}

fn build_db_inner(
    opts: &Options,
    dir: String,
    manifest: &Manifest,
    requested_layout: WalLayout,
    lock_file: std::fs::File,
    resources: OpenResources,
) -> Result<(Arc<DbInner>, WorkerReceivers)> {
    let OpenResources {
        tiers,
        block_cache,
        flush_tx,
        flush_rx,
        compact_tx,
        compact_rx,
        poison,
        wal_syncs,
        closing,
        stop,
        pending_flush,
    } = resources;
    let (unified, unified_max_seq) = open_unified_store(
        opts,
        &dir,
        &flush_tx,
        &pending_flush,
        &closing,
        &poison,
        &wal_syncs,
    )?;
    let tables = Arc::new(crate::table_cache::TableCache::with_byte_budget(
        opts.max_open_readers,
        opts.max_open_reader_bytes,
    ));
    let ctx = Arc::new(CfCtx {
        tiers,
        bc: block_cache,
        tables,
        flush_tx,
        compact_tx,
        closing: closing.clone(),
        read_only: opts.read_only,
        pending_flush: pending_flush.clone(),
        unified: unified.clone(),
        poison: poison.clone(),
        wal_syncs: wal_syncs.clone(),
    });
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
        mover_running: AtomicBool::new(false),
        manifest_mu: Mutex::new(()),
        wal_layout: Mutex::new(requested_layout),
        instance_nonce: Mutex::new(manifest.instance_nonce),
        manifest_persists: AtomicU64::new(0),
        file_deletion: Mutex::new(FileDeletionState::default()),
        workers: Mutex::new(Vec::new()),
        lock_file: Mutex::new(Some(lock_file)),
        handles: Arc::new(AtomicUsize::new(1)),
        poison,
        wal_syncs,
    });
    inner.observe_seq(unified_max_seq);
    Ok((
        inner,
        WorkerReceivers {
            flush: flush_rx,
            compact: compact_rx,
        },
    ))
}

fn open_unified_store(
    opts: &Options,
    dir: &str,
    flush_tx: &Sender<FlushJob>,
    pending_flush: &Arc<AtomicUsize>,
    closing: &Arc<AtomicBool>,
    poison: &Arc<crate::util::Poison>,
    wal_syncs: &Arc<AtomicU64>,
) -> Result<(Option<Arc<crate::unified::UnifiedStore>>, u64)> {
    if !opts.unified_memtable {
        return Ok((None, 0));
    }
    let (store, max_seq) = crate::unified::UnifiedStore::open(
        dir,
        opts,
        flush_tx.clone(),
        pending_flush.clone(),
        closing.clone(),
        poison.clone(),
        wal_syncs.clone(),
    )?;
    Ok((Some(store), max_seq))
}

fn recover_column_families(
    inner: &Arc<DbInner>,
    manifest: &Manifest,
    opts: &Options,
) -> Result<()> {
    for persisted in &manifest.cfs {
        let mut config = ColumnFamilyConfig::decode(&persisted.config);
        let comparator = comparator_by_name(&config.comparator_name).ok_or_else(|| {
            OndaError::InvalidArgs(format!("unknown comparator {}", config.comparator_name))
        })?;
        resolve_partition_scheme(&mut config, opts, &persisted.name)?;
        let (cf, max_seq) = ColumnFamily::load(
            inner.ctx.clone(),
            persisted.name.clone(),
            inner.cf_dir(&persisted.name),
            config,
            comparator,
            &persisted.sstables,
        )?;
        inner.observe_seq(max_seq);
        inner.cf_by_id.write().insert(cf.id(), cf.clone());
        inner.cfs.write().insert(persisted.name.clone(), cf);
    }
    Ok(())
}

fn resolve_partition_scheme(
    config: &mut ColumnFamilyConfig,
    opts: &Options,
    cf_name: &str,
) -> Result<()> {
    let crate::config::PartitionScheme::Unresolved(name) = &config.partition_scheme else {
        return Ok(());
    };
    // Silently reverting to rules would cut all future parts on different
    // boundaries, with corruption surfacing only much later during movement.
    let found = opts
        .partition_fns
        .iter()
        .find(|partitioner| partitioner.scheme_name() == name)
        .cloned()
        .ok_or_else(|| {
            OndaError::InvalidArgs(format!(
                "column family {cf_name:?} was written with derived partition scheme {name:?}, \
                 which is not registered in Options::partition_fns"
            ))
        })?;
    config.partition_scheme = crate::config::PartitionScheme::Derived(found);
    Ok(())
}

fn finish_open(
    inner: &Arc<DbInner>,
    manifest: &Manifest,
    receivers: WorkerReceivers,
) -> Result<()> {
    if inner.opts.read_only {
        return Ok(());
    }
    ensure_instance_nonce(inner)?;
    // The sweep must run after recovery and before workers, so no move races
    // the manifest-as-source-of-truth cleanup.
    sweep_move_orphans(inner, manifest);
    spawn_workers(inner, receivers.flush, receivers.compact);
    Ok(())
}

fn ensure_instance_nonce(inner: &Arc<DbInner>) -> Result<()> {
    let has_shared = inner.opts.tiers.iter().any(|tier| tier.shared);
    if !has_shared || inner.instance_nonce.lock().is_some() {
        return Ok(());
    }
    *inner.instance_nonce.lock() = Some(mint_instance_nonce(&inner.dir));
    // Persist immediately: object names embed the nonce, so a crash before a
    // later manifest write must not allow a different nonce to be minted.
    inner.persist_manifest()
}

impl DB {
    /// Open (creating if needed) the database at `opts.path`.
    pub fn open(opts: Options) -> Result<DB> {
        if opts.path.is_empty() {
            return Err(OndaError::InvalidArgs("empty path".into()));
        }
        if opts.migrate_to_unified {
            if !opts.unified_memtable {
                return Err(OndaError::InvalidArgs(
                    "migrate_to_unified requires unified_memtable".into(),
                ));
            }
            if opts.read_only {
                return Err(OndaError::InvalidArgs(
                    "cannot migrate WAL layout in read-only mode".into(),
                ));
            }
            Self::migrate_to_unified(&opts)?;
        }
        Self::open_impl(opts)
    }

    /// Open without running layout migration. Kept separate so migration can
    /// recover and flush the legacy layout under the ordinary DB invariants.
    fn open_impl(opts: Options) -> Result<DB> {
        std::fs::create_dir_all(&opts.path)?;
        let dir = opts.path.clone();

        // Single-process guard: hold an advisory lock on <dir>/LOCK for the
        // lifetime of the DB. Read-write opens take it exclusive; read-only
        // opens take it shared so concurrent readers coexist but a writer is
        // excluded. The lock dies with the fd, so a crashed process never
        // leaves a stale lock behind.
        let lock_file = acquire_dir_lock(&dir, opts.read_only)?;
        let resources = OpenResources::new(&opts, &dir)?;

        // The WAL layout is a durable database-wide choice once the catalog
        // contains a column family. Opening under the other layout would make
        // recovery consult one set of WALs while new commits write another.
        let manifest = Manifest::load(manifest_path(&dir))?;
        let requested_layout = requested_wal_layout(&opts);
        validate_wal_layout(&manifest, requested_layout)?;
        let (inner, receivers) = build_db_inner(
            &opts,
            dir,
            &manifest,
            requested_layout,
            lock_file,
            resources,
        )?;
        recover_column_families(&inner, &manifest, &opts)?;
        finish_open(&inner, &manifest, receivers)?;
        Ok(DB { inner })
    }

    /// Crash-safe per-CF → unified migration. Until the manifest flip, reopen
    /// sees the legacy layout and can repeat recovery+flush. After the flip,
    /// every recovered byte is already in an SSTable and the next ordinary
    /// open creates/replays the unified WAL before accepting writes.
    fn migrate_to_unified(opts: &Options) -> Result<()> {
        let manifest = Manifest::load(manifest_path(&opts.path))?;
        if manifest.cfs.is_empty() || manifest.wal_layout == WalLayout::Unified {
            return Ok(());
        }

        let mut legacy_opts = opts.clone();
        legacy_opts.unified_memtable = false;
        legacy_opts.migrate_to_unified = false;
        let db = Self::open_impl(legacy_opts)?;

        let cfs: Vec<Arc<ColumnFamily>> = db.inner.cfs.read().values().cloned().collect();
        for cf in &cfs {
            db.flush_memtable(cf)?;
        }
        db.inner.poison.check()?;

        *db.inner.wal_layout.lock() = WalLayout::Unified;
        db.inner.persist_manifest()?;
        db.close()
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

    /// Create several column families in one shot, persisting the manifest
    /// **once** for the whole batch instead of once per family.
    ///
    /// This is semantically identical to calling [`create_column_family`] for
    /// each `(name, config)` in order — the returned handles are in input order
    /// — but avoids the per-family manifest fsync storm. The manifest is a full
    /// rebuild over every live CF, so a single persist after the batch records
    /// exactly what N sequential persists would have; on an SSD where each
    /// persist is a flush-to-medium (`F_FULLFSYNC` on macOS), collapsing 2N
    /// fsyncs (temp + directory per family) into 2 is the cold-boot win.
    ///
    /// The batch is atomic on its precondition: names are fully validated —
    /// including collisions against existing families and duplicates *within*
    /// the batch — before any filesystem work, so a conflicting batch creates
    /// nothing and never touches the manifest. (If a per-family filesystem
    /// create fails midway, already-created directories may linger exactly as a
    /// crash between single-CF create and manifest persist would leave them;
    /// they are unreferenced by the manifest and harmless.)
    ///
    /// Passing an empty slice is a no-op that returns an empty vector without
    /// persisting.
    pub fn create_column_families(
        &self,
        specs: &[(&str, ColumnFamilyConfig)],
    ) -> Result<Vec<Arc<ColumnFamily>>> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        if specs.is_empty() {
            return Ok(Vec::new());
        }
        // Validate every spec up front — name length, comparator, config, and
        // duplicates within the batch — so a bad batch fails before any
        // directory or WAL is created.
        let mut seen: HashMap<&str, ()> = HashMap::with_capacity(specs.len());
        for (name, config) in specs {
            if name.is_empty() || name.len() > MAX_CF_NAME_LEN {
                return Err(OndaError::InvalidArgs("invalid column family name".into()));
            }
            comparator_by_name(&config.comparator_name).ok_or_else(|| {
                OndaError::InvalidArgs(format!("unknown comparator {}", config.comparator_name))
            })?;
            config.validate().map_err(OndaError::InvalidArgs)?;
            if seen.insert(name, ()).is_some() {
                return Err(OndaError::Exists((*name).into()));
            }
        }

        // Hold the registry write lock across the whole batch: check every name
        // is free, then create and insert them, so a concurrent creator can
        // neither observe a half-built batch nor collide with one.
        let mut cfs = self.inner.cfs.write();
        for (name, _) in specs {
            if cfs.contains_key(*name) {
                return Err(OndaError::Exists((*name).into()));
            }
        }
        let mut created = Vec::with_capacity(specs.len());
        for (name, config) in specs {
            let cmp =
                comparator_by_name(&config.comparator_name).expect("comparator validated above");
            let cf = ColumnFamily::create(
                self.inner.ctx.clone(),
                (*name).to_string(),
                self.inner.cf_dir(name),
                config.clone(),
                cmp,
            )?;
            cfs.insert((*name).to_string(), cf.clone());
            created.push(cf);
        }
        drop(cfs);
        {
            let mut by_id = self.inner.cf_by_id.write();
            for cf in &created {
                by_id.insert(cf.id(), cf.clone());
            }
        }
        // One manifest persist for the entire batch.
        self.inner.persist_manifest()?;
        Ok(created)
    }

    /// Look up a column family by name.
    pub fn get_column_family(&self, name: &str) -> Option<Arc<ColumnFamily>> {
        self.inner.cfs.read().get(name).cloned()
    }

    /// The effective durable configuration of the column family `name` — what a
    /// reopen would restore, including live-added partition rules. Lets callers
    /// verify durability-critical settings (e.g. that a WAL-backed CF really
    /// records [`SyncMode::Full`](crate::config::SyncMode)) instead of assuming
    /// the config they opened with.
    pub fn column_family_config(&self, name: &str) -> Result<crate::config::ColumnFamilyConfig> {
        let cf = self.get_column_family(name).ok_or(OndaError::NotFound)?;
        Ok(cf.config())
    }

    /// Total successful physical WAL `sync_data` calls this DB has performed,
    /// across per-CF WALs, the unified WAL, and rotations. Under
    /// [`SyncMode::None`](crate::config::SyncMode) this never advances — which
    /// is exactly what a durability test should assert on.
    pub fn wal_sync_count(&self) -> u64 {
        self.inner.wal_syncs.load(Ordering::Relaxed)
    }

    /// List column family names.
    /// `(open readers, opens, hits, closes)` for the bounded reader cache.
    pub fn table_cache_stats(&self) -> (usize, u64, u64, u64) {
        self.inner.ctx.tables.stats()
    }

    /// `(resident, index, bloom, open readers, index entries)` bytes/counts held
    /// by the readers currently open.
    pub fn reader_memory(&self) -> (usize, usize, usize, usize, usize) {
        self.inner.ctx.tables.resident_breakdown()
    }

    /// `(resident bytes held by cached readers, byte budget)`. A budget of `0`
    /// means the byte bound is off. This is the counterpart of
    /// [`table_cache_stats`](Self::table_cache_stats) in the unit that actually
    /// bounds memory — see
    /// [`Options::max_open_reader_bytes`](crate::Options::max_open_reader_bytes).
    pub fn table_cache_bytes(&self) -> (usize, usize) {
        self.inner.ctx.tables.byte_stats()
    }

    /// Change the open-reader bound at runtime.
    pub fn set_max_open_readers(&self, n: usize) {
        self.inner.ctx.tables.set_max_open(n);
    }

    /// Change the open-reader **byte** budget at runtime; `0` disables it.
    /// Lowering it evicts immediately.
    pub fn set_max_open_reader_bytes(&self, n: usize) {
        self.inner.ctx.tables.set_max_bytes(n);
    }

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
        let result = compaction::run_manual(&self.inner, cf);
        if let Err(error) = &result {
            cf.record_compaction_failure(error);
        }
        result
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
/// delete exactly those. Unknown numeric SST files on the database-owned default
/// tier are also incomplete flush/compaction outputs and are removed. Unknown
/// named-tier files remain untouched because that storage may have external
/// ownership; correctly-placed manifest files are kept.
/// Mint the per-database instance nonce (A2): 8 bytes of SHA-256 over the
/// database path, the wall clock, and the pid. Not a cryptographic identity —
/// a collision needs two databases minting in the same nanosecond with the
/// same path and pid — but unique enough that object names never collide
/// under a shared tier root, which is all it exists for.
fn mint_instance_nonce(dir: &str) -> u64 {
    use sha2::Digest as _;
    let mut h = sha2::Sha256::new();
    h.update(dir.as_bytes());
    h.update(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos().to_be_bytes())
            .unwrap_or([0u8; 16]),
    );
    h.update(std::process::id().to_be_bytes());
    let out = h.finalize();
    u64::from_be_bytes(out[..8].try_into().expect("sha256 yields 32 bytes"))
}

fn sweep_move_orphans(inner: &Arc<DbInner>, manifest: &Manifest) {
    let locations = orphan_sweep_locations(&inner.opts);
    for cfm in &manifest.cfs {
        let tier_of = manifest_table_locations(cfm);
        for loc in &locations {
            sweep_cf_location(inner, &cfm.name, &tier_of, loc.as_deref());
        }
    }
}

fn orphan_sweep_locations(opts: &Options) -> Vec<Option<String>> {
    let mut locations = vec![None];
    for tier in &opts.tiers {
        // Never sweep a shared tier: its objects may belong to another database
        // whose table ids happen to coincide with ours (SPADINO-A2.md).
        if tier.name != "ssd" && !tier.shared {
            locations.push(Some(tier.name.clone()));
        }
    }
    locations
}

fn manifest_table_locations(cfm: &CfManifest) -> HashMap<u64, Option<String>> {
    cfm.sstables
        .iter()
        .map(|sst| (sst.id, sst.tier.clone()))
        .collect()
}

fn sweep_cf_location(
    inner: &Arc<DbInner>,
    cf_name: &str,
    manifest_tiers: &HashMap<u64, Option<String>>,
    location: Option<&str>,
) {
    let dir = inner.ctx.tiers.cf_dir(location, cf_name);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };
    for entry in entries.flatten() {
        sweep_sst_entry(&dir, &entry, manifest_tiers, location);
    }
}

fn sweep_sst_entry(
    dir: &str,
    entry: &std::fs::DirEntry,
    manifest_tiers: &HashMap<u64, Option<String>>,
    location: Option<&str>,
) {
    let file_name = entry.file_name();
    let file_name = file_name.to_string_lossy();
    let Some(id) = parse_sst_file_id(&file_name) else {
        return;
    };
    if sst_is_misplaced(manifest_tiers.get(&id), location) {
        let _ = std::fs::remove_file(format!("{dir}/{file_name}"));
    }
}

fn sst_is_misplaced(manifest_tier: Option<&Option<String>>, location: Option<&str>) -> bool {
    match manifest_tier {
        Some(tier) => tier.as_deref() != location,
        None => location.is_none(),
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
    let n_compact = inner.opts.num_compaction_threads.max(1);
    for worker in 0..n_compact {
        let db = inner.clone();
        let rx = compact_rx.clone();
        let stop = inner.stop.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("onda-compact-{worker}"))
                .spawn(move || compact_worker(db, rx, stop))
                .expect("spawn compaction worker"),
        );
    }
    *inner.workers.lock() = handles;
}

fn should_schedule_compaction(closing: bool, fifo: bool, l0_len: usize, trigger: usize) -> bool {
    !closing && (fifo || l0_len >= trigger)
}

fn schedule_compaction_after_flush(db: &DbInner, cf: &Arc<ColumnFamily>) {
    crate::compaction::refresh_compaction_debt(db, cf);
    let fifo = cf.opts.compaction_style == crate::config::CompactionStyle::Fifo;
    if should_schedule_compaction(
        db.closing.load(Ordering::Relaxed),
        fifo,
        cf.l0_len(),
        cf.opts.l1_file_count_trigger as usize,
    ) {
        let _ = db.ctx.compact_tx.send(cf.clone());
    }
}

fn flush_per_cf(db: &Arc<DbInner>, cf: Arc<ColumnFamily>, imm: Arc<ImmMemtable>) {
    match cf.flush_imm(&imm, db.next_file_id()) {
        Ok(wal_paths) => {
            // The SST is already synced by `flush_imm`. Reclaim its WAL only
            // after the manifest durably references that SST.
            if db.persist_manifest().is_ok() {
                for path in wal_paths {
                    crate::wal::remove_wal_files(path);
                }
            }
            schedule_compaction_after_flush(db, &cf);
        }
        Err(error) => {
            // A dropped/cleared CF may disappear while its queued job runs;
            // only a failure for the still-live instance fail-stops the DB.
            let live = db
                .cfs
                .read()
                .get(cf.name())
                .is_some_and(|current| Arc::ptr_eq(current, &cf));
            if live {
                db.poison.set(format!("background flush failed: {error}"));
            }
        }
    }
}

fn flush_unified(db: &Arc<DbInner>, imm: Arc<crate::unified::UnifiedImm>) {
    // Each CF slice lands in L0 before the single manifest publication.
    let mut all_slices_flushed = true;
    for (cf_id, entries) in crate::unified::split_by_cf(&imm) {
        let cf = db.cf_by_id.read().get(&cf_id).cloned();
        let Some(cf) = cf else {
            continue;
        };
        if let Err(error) = cf.ingest_l0(entries, db.next_file_id()) {
            db.poison.set(format!("unified flush failed: {error}"));
            all_slices_flushed = false;
        }
        schedule_compaction_after_flush(db, &cf);
    }
    // A shared WAL covers every CF slice. One failed slice must retain it even
    // if the manifest could persist the successful slices; recovery needs the
    // original atomic batch. Only full flush + manifest durability delete it.
    if all_slices_flushed && db.persist_manifest().is_ok() {
        for path in &imm.wal_paths {
            crate::wal::remove_wal_files(path);
        }
    }
    if let Some(unified) = &db.unified {
        unified.remove_imm(&imm);
    }
}

fn process_flush_job(db: &Arc<DbInner>, job: FlushJob) {
    match job {
        FlushJob::PerCf { cf, imm } => flush_per_cf(db, cf, imm),
        FlushJob::Unified { imm } => flush_unified(db, imm),
    }
    db.pending_flush.fetch_sub(1, Ordering::SeqCst);
}

fn flush_worker(db: Arc<DbInner>, rx: Receiver<FlushJob>, stop: Arc<AtomicBool>) {
    loop {
        match rx.recv_timeout(WORKER_TICK) {
            Ok(job) => process_flush_job(&db, job),
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
                // Check `stop` before starting, not only when the queue runs
                // dry. Testing it on timeout alone meant a closing database
                // drained every queued job first, which is what made
                // `finish_compactions_on_close` unobservable and turned close
                // into a 35-second wait after a large ingest. Leftover debt is
                // legal LSM state; the next open picks it up.
                if stop.load(Ordering::SeqCst) && !db.opts.finish_compactions_on_close {
                    break;
                }
                if let Err(error) = compaction::run(&db, &cf) {
                    cf.record_compaction_failure(&error);
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
        // One mover pass at a time. With several compaction workers, two could
        // otherwise scan concurrently and pick the same partition to relocate.
        if !mover_interval.is_zero()
            && !db.closing.load(Ordering::Relaxed)
            && last_mover.elapsed() >= mover_interval
            && db
                .mover_running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            last_mover = std::time::Instant::now();
            let _ = db.run_part_mover();
            db.mover_running.store(false, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_compaction_schedule_respects_style_trigger_and_close() {
        assert!(should_schedule_compaction(false, true, 0, 4));
        assert!(!should_schedule_compaction(false, false, 3, 4));
        assert!(should_schedule_compaction(false, false, 4, 4));
        assert!(!should_schedule_compaction(true, true, 8, 4));
    }

    #[test]
    fn wal_layout_validation_allows_empty_catalogs_and_matching_catalogs() {
        let empty = Manifest::default();
        assert!(validate_wal_layout(&empty, WalLayout::Unified).is_ok());

        let populated = Manifest {
            cfs: vec![CfManifest::default()],
            wal_layout: WalLayout::Unified,
            ..Manifest::default()
        };
        assert!(validate_wal_layout(&populated, WalLayout::Unified).is_ok());
    }

    #[test]
    fn wal_layout_validation_rejects_a_populated_mismatch() {
        let manifest = Manifest {
            cfs: vec![CfManifest::default()],
            wal_layout: WalLayout::PerColumnFamily,
            ..Manifest::default()
        };

        let err = validate_wal_layout(&manifest, WalLayout::Unified).unwrap_err();
        assert!(matches!(err, OndaError::InvalidArgs(message) if
            message == "WAL layout mismatch: database is PerColumnFamily, requested Unified"));
    }

    #[test]
    fn orphan_sweep_removes_only_known_tables_in_the_wrong_location() {
        let default = None;
        let cold = Some("cold".to_owned());

        assert!(sst_is_misplaced(None, None));
        assert!(!sst_is_misplaced(None, Some("cold")));
        assert!(!sst_is_misplaced(Some(&default), None));
        assert!(sst_is_misplaced(Some(&default), Some("cold")));
        assert!(!sst_is_misplaced(Some(&cold), Some("cold")));
        assert!(sst_is_misplaced(Some(&cold), None));
    }

    /// The two durability-inspection hooks: `column_family_config` reports the
    /// effective (reopen-faithful) config, and `wal_sync_count` counts only
    /// physical `sync_data` calls — zero under `SyncMode::None`, advancing per
    /// commit under `SyncMode::Full`.
    #[test]
    fn config_readback_and_physical_sync_count() {
        use crate::config::SyncMode;
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let lazy = db
            .create_column_family("lazy", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&lazy, b"k", b"v", Duration::ZERO).unwrap();
        assert_eq!(
            db.column_family_config("lazy").unwrap().sync_mode,
            SyncMode::None
        );
        assert!(matches!(
            db.column_family_config("missing"),
            Err(OndaError::NotFound)
        ));
        // A None-mode commit performs no physical sync.
        assert_eq!(db.wal_sync_count(), 0);

        let durable_cfg = ColumnFamilyConfig {
            sync_mode: SyncMode::Full,
            ..ColumnFamilyConfig::default()
        };
        let durable = db.create_column_family("durable", durable_cfg).unwrap();
        assert_eq!(
            db.column_family_config("durable").unwrap().sync_mode,
            SyncMode::Full
        );
        db.put(&durable, b"k", b"v", Duration::ZERO).unwrap();
        let after_one = db.wal_sync_count();
        assert!(after_one >= 1, "a Full-mode commit must sync_data");
        db.put(&durable, b"k2", b"v", Duration::ZERO).unwrap();
        assert!(db.wal_sync_count() > after_one);
        db.close().unwrap();
    }

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

    /// Batch CF creation must be semantically identical to N single creates —
    /// every family present, gettable, and recovered after reopen — while
    /// persisting the manifest exactly ONCE for the whole batch (the cold-boot
    /// fsync-storm fix: the manifest is a full rebuild over all CFs, so one
    /// persist after the batch records everything N per-CF persists would have).
    #[test]
    fn create_column_families_persists_manifest_once() {
        let dir = tempfile::tempdir().unwrap();
        let names = ["a", "b", "c", "d", "e"];
        {
            let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
            let before = db.inner.manifest_persist_count();
            let specs: Vec<(&str, ColumnFamilyConfig)> = names
                .iter()
                .map(|n| (*n, ColumnFamilyConfig::default()))
                .collect();
            let cfs = db.create_column_families(&specs).unwrap();
            assert_eq!(cfs.len(), names.len());
            // One persist for the whole batch, not one per CF.
            assert_eq!(db.inner.manifest_persist_count() - before, 1);
            // All families are live and usable immediately.
            for (i, n) in names.iter().enumerate() {
                assert!(db.get_column_family(n).is_some(), "missing {n}");
                db.put(&cfs[i], b"k", b"v", Duration::ZERO).unwrap();
            }
            db.close().unwrap();
        }
        // Reopen: every batched family recovered from the single manifest write.
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        for n in names {
            let cf = db
                .get_column_family(n)
                .unwrap_or_else(|| panic!("family {n} not recovered"));
            assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
        }
        db.close().unwrap();
    }

    /// Micro-benchmark (ignored; run with
    /// `cargo test --release create_column_families_bench -- --ignored --nocapture`):
    /// times creating 11 CFs one-by-one vs. in a single batch on fresh dirs,
    /// isolating the manifest-fsync cost from process startup. Illustrates the
    /// cold-boot fix; not a pass/fail gate (fsync latency is machine-dependent).
    #[test]
    #[ignore]
    fn create_column_families_bench() {
        use std::time::Instant;
        let names: Vec<String> = (0..11).map(|i| format!("cf{i}")).collect();
        let trials = 8;
        let mut per_cf = Vec::new();
        let mut batched = Vec::new();
        for _ in 0..trials {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
            let t = Instant::now();
            for n in &names {
                db.create_column_family(n, ColumnFamilyConfig::default())
                    .unwrap();
            }
            per_cf.push(t.elapsed().as_secs_f64() * 1e3);
            db.close().unwrap();

            let dir = tempfile::tempdir().unwrap();
            let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
            let specs: Vec<(&str, ColumnFamilyConfig)> = names
                .iter()
                .map(|n| (n.as_str(), ColumnFamilyConfig::default()))
                .collect();
            let t = Instant::now();
            db.create_column_families(&specs).unwrap();
            batched.push(t.elapsed().as_secs_f64() * 1e3);
            db.close().unwrap();
        }
        let med = |mut v: Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[v.len() / 2]
        };
        println!(
            "11 CFs: per-CF create median {:.1} ms, batched median {:.1} ms (n={trials})",
            med(per_cf),
            med(batched)
        );
    }

    /// A batch that names an existing family, or repeats a name within itself,
    /// is rejected as a whole and writes nothing — no partial creation, no
    /// manifest persist.
    #[test]
    fn create_column_families_rejects_conflicts_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        db.create_column_family("existing", ColumnFamilyConfig::default())
            .unwrap();

        let before = db.inner.manifest_persist_count();
        // Collides with an existing family.
        let specs = [
            ("new1", ColumnFamilyConfig::default()),
            ("existing", ColumnFamilyConfig::default()),
        ];
        assert!(matches!(
            db.create_column_families(&specs),
            Err(OndaError::Exists(_))
        ));
        // Duplicate within the batch.
        let specs = [
            ("dup", ColumnFamilyConfig::default()),
            ("dup", ColumnFamilyConfig::default()),
        ];
        assert!(matches!(
            db.create_column_families(&specs),
            Err(OndaError::Exists(_))
        ));
        // Nothing was created and the manifest was never touched.
        assert!(db.get_column_family("new1").is_none());
        assert!(db.get_column_family("dup").is_none());
        assert_eq!(db.inner.manifest_persist_count() - before, 0);
        db.close().unwrap();
    }
}
