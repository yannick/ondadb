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
static NEXT_DB_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

struct PublishState {
    cursor: u64,                  // next start sequence expected to publish
    completed: HashMap<u64, u64>, // start -> end of completed-but-unpublished ranges
}

/// Minimum charge for retiring one file, in bytes.
///
/// An unlink is metadata IO: it costs a directory update and an inode free even
/// when the file itself is empty, and a column family with no separated values
/// retires a zero-byte `<id>.vlog` at every compaction. Charging the literal
/// size would make a storm of those free, and a storm of tiny deletions is
/// precisely what saturates a device with metadata work. One filesystem block
/// is the documented floor.
pub const DELETE_METADATA_BYTES: u64 = 4096;

/// One file to retire, and what its removal is charged.
#[derive(Debug)]
struct DeleteTask {
    path: String,
    /// The file's size, floored at [`DELETE_METADATA_BYTES`].
    bytes: u64,
}

/// The thread that performs paced unlinks, present only when
/// `Options::obsolete_delete_bytes_per_second` is non-zero.
struct DeletionWorker {
    /// FIFO queue to the worker. `None` after [`FileDeletionState::drain`] has
    /// closed it (at `close`), from which point deletions unlink inline again —
    /// there is no thread left to hand them to.
    tx: Mutex<Option<Sender<DeleteTask>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    /// Admission control for the unlinks. Its own bucket at the deletion rate,
    /// unless the embedder injected a limiter — one supplied limiter means one
    /// device budget covering every class.
    limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>,
}

/// Deferred-deletion control for consistent checkpoints/backups, plus the
/// optional pacing worker.
///
/// While `paused.disabled > 0`, obsolete SSTable files are recorded in
/// `paused.pending` instead of being unlinked or queued, so a snapshot can copy
/// a self-consistent file set even while compaction runs. That path is
/// unchanged by pacing: the pause is what backup correctness rests on, and the
/// worker must not become a way around it.
struct FileDeletionState {
    paused: Mutex<PausedDeletions>,
    /// `None` — the default — means unlink inline on the caller's thread, with
    /// no channel and no thread, exactly as every release before 0.6.
    worker: Option<DeletionWorker>,
}

#[derive(Default)]
struct PausedDeletions {
    disabled: u32,
    pending: Vec<DeleteTask>,
}

impl FileDeletionState {
    /// Build the deletion state for `opts`, spawning the worker only when a
    /// rate is configured.
    fn new(opts: &Options) -> FileDeletionState {
        let rate = opts.obsolete_delete_bytes_per_second;
        let worker = (rate > 0).then(|| {
            // Same construction rule as the DB-wide limiter: an injected
            // limiter wins outright, otherwise a bucket at the deletion rate.
            let limiter = crate::ioctrl::limiter_for(
                rate,
                opts.background_io_burst_bytes,
                opts.io_limiter.clone(),
            );
            let (tx, rx) = unbounded::<DeleteTask>();
            let worker_limiter = limiter.clone();
            let handle = std::thread::Builder::new()
                .name("onda-delete".into())
                .spawn(move || deletion_worker(rx, worker_limiter))
                .expect("spawn deletion worker");
            DeletionWorker {
                tx: Mutex::new(Some(tx)),
                handle: Mutex::new(Some(handle)),
                limiter,
            }
        });
        FileDeletionState {
            paused: Mutex::new(PausedDeletions::default()),
            worker,
        }
    }

    /// Retire one file: hand it to the worker, or unlink it here.
    ///
    /// The inline case is not only the unpaced default — it is also the
    /// fallback once the queue has been closed at `close`, so a late deletion
    /// can never be silently dropped on the floor.
    fn dispatch(&self, task: DeleteTask) {
        let task = match &self.worker {
            Some(worker) => {
                let tx = worker.tx.lock();
                match tx.as_ref() {
                    Some(tx) => match tx.send(task) {
                        Ok(()) => return,
                        // The worker died; take the task back and do it here
                        // rather than lose the file.
                        Err(err) => err.into_inner(),
                    },
                    None => task,
                }
            }
            None => task,
        };
        let _ = std::fs::remove_file(&task.path);
    }

    /// Close the queue and join the worker, unlinking everything already
    /// queued. Idempotent.
    fn drain(&self) {
        let Some(worker) = &self.worker else {
            return;
        };
        // Dropping the only sender is what ends the worker's `for` loop — after
        // it has drained every task still in the channel, which is exactly the
        // ordering close needs.
        drop(worker.tx.lock().take());
        let handle = worker.handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    /// Release a worker parked on its bucket. See `DbInner::cancel_background_io`.
    fn cancel(&self) {
        if let Some(worker) = &self.worker {
            if let Some(limiter) = &worker.limiter {
                limiter.cancel();
            }
        }
    }
}

/// Unlink obsolete files in FIFO order, paced by `limiter`.
///
/// Order is not load-bearing — file ids are never reused, so no later task can
/// depend on an earlier one having run — but FIFO keeps the queue's depth a
/// function of the rate alone.
fn deletion_worker(rx: Receiver<DeleteTask>, limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>) {
    // Dedicated thread: set the class once, never restore it.
    crate::ioctrl::set_class(crate::ioctrl::IoClass::ObsoleteDelete);
    // Ends when the sender is dropped *and* the queue is empty: `drain` relies
    // on that to guarantee every queued file is gone before it returns.
    for task in rx {
        crate::ioctrl::charge(&limiter, task.bytes);
        let _ = std::fs::remove_file(&task.path);
    }
}

/// Internal database state shared with workers and column families.
pub struct DbInner {
    pub(crate) opts: Options,
    pub(crate) dir: String,
    pub(crate) instance_id: u64,
    pub(crate) cfs: RwLock<HashMap<String, Arc<ColumnFamily>>>,
    /// CFs keyed by their stable id, for unified-memtable flush routing.
    pub(crate) cf_by_id: RwLock<HashMap<u64, Arc<ColumnFamily>>>,
    pub(crate) ctx: Arc<CfCtx>,
    pub(crate) unified: Option<Arc<crate::unified::UnifiedStore>>,
    /// Bandwidth admission for background IO, or `None` when unlimited (the
    /// default). The same object the column families carry into their readers
    /// and writers; held here so the caller-thread background paths
    /// (`run_manual`, `flush_memtable`, ingest) can install it, and so close
    /// and fail-stop can cancel it.
    pub(crate) io_limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>,

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

    /// Admission for the **extra** threads a compaction job spawns to run its
    /// spans in parallel (0.8). See [`SpanPermits`].
    pub(crate) span_permits: SpanPermits,

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

    /// Format capabilities this database may **use right now** — the word every
    /// API entry point checks before writing a newer artifact.
    ///
    /// Split from `caps_durable` on purpose. `persist_manifest` encodes
    /// `caps_durable`, so `enable_capability` can stage a bit, make it durable,
    /// and only then flip `caps`. One word could not express that ordering: the
    /// encoder would either write a bit the database is already using
    /// (persist-after-use — a crash then leaves artifacts no reopen can read)
    /// or never write it at all.
    pub(crate) caps: AtomicU64,
    /// Capability word the manifest encoder writes — the durable intent, which
    /// leads `caps` for exactly the duration of one `persist_manifest`.
    pub(crate) caps_durable: AtomicU64,
    /// Serializes `enable_capability`, so N concurrent first-enables persist
    /// once and all observe the bit. Never held across `manifest_mu`'s
    /// acquisition order in the other direction: this lock is only ever taken
    /// first.
    enable_mu: Mutex<()>,

    /// Count of successful manifest persists over this DB's lifetime. Cheap
    /// (a single relaxed increment on an already fsync-bound path); exists so
    /// batch operations can assert they collapse N per-item persists into one.
    manifest_persists: AtomicU64,

    file_deletion: FileDeletionState,

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

/// DB-wide budget for parallel compaction **span workers** (0.8).
///
/// Sized independently of `num_compaction_threads`, and a coordinator consumes
/// nothing: it is an `onda-compact-{n}` thread that runs span 0 itself, and
/// `num_compaction_threads` already accounts for it. The naive alternative —
/// one pool of `num_compaction_threads` permits with the coordinator taking one
/// — silently no-ops at the defaults, because two concurrent jobs would consume
/// both permits as coordinators and no span worker could ever run.
///
/// Acquisition never blocks. A job takes what is free and degrades to fewer
/// spans (ultimately to one, which is exactly today's behavior), so no
/// background thread ever waits here while holding its range lock.
#[derive(Debug)]
pub(crate) struct SpanPermits {
    available: Mutex<usize>,
}

/// Permits held for the life of one compaction job; released when it joins its
/// workers, on the error path as much as the happy one.
#[derive(Debug)]
pub(crate) struct SpanPermitGuard<'a> {
    pool: &'a SpanPermits,
    held: usize,
}

impl SpanPermits {
    pub(crate) fn new(permits: usize) -> SpanPermits {
        SpanPermits {
            available: Mutex::new(permits),
        }
    }

    /// Take up to `want` permits, without waiting for any.
    pub(crate) fn take(&self, want: usize) -> SpanPermitGuard<'_> {
        let mut available = self.available.lock();
        let held = want.min(*available);
        *available -= held;
        SpanPermitGuard { pool: self, held }
    }

    /// Permits free right now — test observability only.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        *self.available.lock()
    }
}

impl SpanPermitGuard<'_> {
    pub(crate) fn granted(&self) -> usize {
        self.held
    }

    /// Give back everything past `keep`: the boundary planner may find fewer
    /// useful cuts than the permits allow, and holding the surplus for the
    /// length of the merge would starve a concurrent job for nothing.
    pub(crate) fn reduce_to(&mut self, keep: usize) {
        if keep >= self.held {
            return;
        }
        let released = self.held - keep;
        self.held = keep;
        *self.pool.available.lock() += released;
    }
}

impl Drop for SpanPermitGuard<'_> {
    fn drop(&mut self) {
        if self.held > 0 {
            *self.pool.available.lock() += self.held;
        }
    }
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
    /// `max(visible_seq, own floor)`. Keyed by a stable process-local database
    /// identity so allocator address reuse cannot transfer a closed DB's floor
    /// to a later instance. Entries die with the thread.
    static THREAD_COMMIT_FLOOR: std::cell::RefCell<std::collections::HashMap<u64, u64>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

impl DbInner {
    pub(crate) fn reserve_seq(&self, n: u64) -> u64 {
        self.next_seq.fetch_add(n, Ordering::SeqCst)
    }

    fn db_key(&self) -> u64 {
        self.instance_id
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
            // The staged word, not the active one: `enable_capability` must
            // make the bit durable BEFORE anything may write bytes using it.
            caps: self.caps_durable.load(Ordering::SeqCst),
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
                self.fail_stop(format!("manifest persist failed: {e}"));
            }
        }
        res
    }

    /// Format capabilities this database may use right now.
    pub(crate) fn caps(&self) -> u64 {
        self.caps.load(Ordering::SeqCst)
    }

    /// Durably enable `bits`, then start honoring them.
    ///
    /// The ordering is the whole contract — **persist before use**:
    ///
    /// 1. refuse a poisoned or read-only database, before taking any lock;
    /// 2. return `Ok` immediately if every bit is already active (idempotent —
    ///    a reopen of an enabled database re-enables nothing);
    /// 3. stage the bits into `caps_durable` and persist the manifest;
    /// 4. only then flip `caps`, which is what write paths check.
    ///
    /// A `persist_manifest` failure fail-stops the whole database (see
    /// [`persist_manifest`](Self::persist_manifest)), so a failed enable is not
    /// a recoverable no-op the caller can retry against the same handle: `caps`
    /// is untouched, the handle is dead, and a reopen sees the pre-enable state.
    /// The staged word is rolled back so no later persist can publish a bit
    /// this database never started using.
    pub(crate) fn enable_capability(&self, bits: u64) -> Result<()> {
        self.poison.check()?;
        if self.opts.read_only {
            return Err(OndaError::ReadOnly(
                "cannot enable format capabilities on a read-only database".into(),
            ));
        }
        if bits & !crate::format::KNOWN_CAPS != 0 {
            return Err(OndaError::InvalidArgs(format!(
                "capabilities {bits:#x} outside known mask {:#x}",
                crate::format::KNOWN_CAPS
            )));
        }
        let _mu = self.enable_mu.lock();
        let active = self.caps.load(Ordering::SeqCst);
        if active & bits == bits {
            return Ok(());
        }
        let previous = self.caps_durable.load(Ordering::SeqCst);
        self.caps_durable.store(previous | bits, Ordering::SeqCst);
        if let Err(e) = self.persist_manifest() {
            self.caps_durable.store(previous, Ordering::SeqCst);
            return Err(e);
        }
        self.caps.store(active | bits, Ordering::SeqCst);
        Ok(())
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

    /// Release every thread parked on the background IO limiter and stop
    /// delaying, permanently.
    ///
    /// A limiter wait is bounded by a refill that only keeps happening while
    /// the database is alive, so both terminal transitions — close and
    /// fail-stop — must cancel it. A no-op when background IO is unlimited.
    pub(crate) fn cancel_background_io(&self) {
        if let Some(limiter) = &self.io_limiter {
            limiter.cancel();
        }
        // The deletion worker parks on a bucket of its own (unless the
        // embedder injected one limiter for both), and a queue it can no longer
        // drain is a queue `close` would wait on forever.
        self.file_deletion.cancel();
    }

    /// Trip the fail-stop flag and release anything waiting on this database.
    ///
    /// Every production `poison.set` goes through here: after a durability
    /// failure the workers are on their way out, and a compaction thread parked
    /// on the IO limiter would otherwise sit out its full refill first.
    pub(crate) fn fail_stop(&self, why: String) {
        self.poison.set(why);
        self.cancel_background_io();
    }

    /// Retire an obsolete SSTable file, or defer it if deletions are paused (a
    /// checkpoint/backup is copying a consistent file set). Compaction, FIFO
    /// eviction and the part mover route all input-file removals through here.
    ///
    /// `bytes` is the file's size from its `SstMeta` — every caller already
    /// holds one — floored at [`DELETE_METADATA_BYTES`], and is what the unlink
    /// is charged when pacing is on. Unpaced (the default) it is ignored and
    /// the file is unlinked right here, on the caller's thread.
    pub(crate) fn remove_sst_file(&self, path: &str, bytes: u64) {
        let task = DeleteTask {
            path: path.to_string(),
            bytes: bytes.max(DELETE_METADATA_BYTES),
        };
        let mut paused = self.file_deletion.paused.lock();
        if paused.disabled > 0 {
            paused.pending.push(task);
            return;
        }
        drop(paused);
        self.file_deletion.dispatch(task);
    }

    /// Pause obsolete-file deletion for the lifetime of the returned guard. Nested
    /// pauses are counted; deferred files are retired when the last guard drops.
    pub(crate) fn pause_deletions(&self) -> DeletionPause<'_> {
        self.file_deletion.paused.lock().disabled += 1;
        DeletionPause { inner: self }
    }

    fn resume_deletions(&self) {
        let drained = {
            let mut paused = self.file_deletion.paused.lock();
            paused.disabled = paused.disabled.saturating_sub(1);
            if paused.disabled == 0 {
                std::mem::take(&mut paused.pending)
            } else {
                Vec::new()
            }
        };
        for task in drained {
            self.file_deletion.dispatch(task);
        }
    }

    /// Unlink everything queued for deletion and stop the worker.
    ///
    /// Called by `close` before the directory lock is released — the same
    /// obligation deferred deletes under a pause already had: nothing may
    /// outlive the open database that knows the file is obsolete, or the next
    /// open inherits a file no manifest names.
    pub(crate) fn drain_deletions(&self) {
        self.file_deletion.drain();
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
    // Built once and shared: `None` unless background IO is limited, so the
    // default configuration costs one nil check at each charge point.
    let io_limiter = crate::ioctrl::limiter_for(
        opts.background_io_bytes_per_second,
        opts.background_io_burst_bytes,
        opts.io_limiter.clone(),
    );
    let ctx = Arc::new(CfCtx {
        tiers,
        bc: block_cache,
        io_limiter: io_limiter.clone(),
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
        io_limiter,
        dir,
        instance_id: NEXT_DB_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
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
        span_permits: SpanPermits::new(match opts.max_subcompaction_workers {
            0 => opts.num_compaction_threads.max(1),
            n => n,
        }),
        manifest_mu: Mutex::new(()),
        wal_layout: Mutex::new(requested_layout),
        instance_nonce: Mutex::new(manifest.instance_nonce),
        // A capability recorded in the manifest is already durable, so both
        // words start from it: a reopen after a crash between persist and flip
        // simply sees an enabled database.
        caps: AtomicU64::new(manifest.caps),
        caps_durable: AtomicU64::new(manifest.caps),
        enable_mu: Mutex::new(()),
        manifest_persists: AtomicU64::new(0),
        file_deletion: FileDeletionState::new(opts),
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

    /// Create a [`TailingIterator`](crate::tailing::TailingIterator) over `cf`:
    /// a forward-only cursor that can be refreshed past its own end instead of
    /// being rebuilt per poll.
    ///
    /// **Not a change feed.** A refreshed tail observes only keys strictly
    /// greater than the last one it yielded; changes at or behind the cursor
    /// are never surfaced. See the type's documentation for the full contract.
    ///
    /// DB-level by construction: each segment reads at the read-committed floor,
    /// and refreshing a transaction's *fixed* snapshot would silently break that
    /// snapshot — so there is no `Txn` equivalent.
    pub fn new_tailing_iterator(&self, cf: &Arc<ColumnFamily>) -> crate::tailing::TailingIterator {
        crate::tailing::TailingIterator::new(self.inner.clone(), cf.clone())
    }

    /// Flush a column family's active memtable to an SSTable (blocks until the
    /// flush is enqueued and drained).
    pub fn flush_memtable(&self, cf: &Arc<ColumnFamily>) -> Result<()> {
        // Rotation and any work this thread does on the way to it are flush
        // work, not the caller's foreground work.
        let _io = crate::ioctrl::scoped(crate::ioctrl::IoClass::Flush);
        cf.rotate_memtable(true);
        let pending = || {
            if self.inner.unified.is_some() {
                self.inner.pending_flush.load(Ordering::SeqCst)
            } else {
                cf.pending_flushes.load(Ordering::SeqCst)
            }
        };
        while pending() > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    /// Durably enable one or more format capabilities
    /// ([`CAP_EXTENDED_RECORDS`](crate::format::CAP_EXTENDED_RECORDS) and
    /// friends), then start honoring them.
    ///
    /// A capability is a permission taken **once and durably, before the first
    /// byte using it exists**: the bit reaches the manifest — bumping it to
    /// VERSION 2, which older binaries refuse outright — before any write path
    /// may produce the newer artifact. Enabling is idempotent and safe to call
    /// on every open; a database that enables nothing keeps writing VERSION-1
    /// manifests and legacy artifacts forever.
    ///
    /// **This is one-way.** Once a capability is enabled, every reader of the
    /// database must understand it; there is no disable.
    ///
    /// Fails with `ReadOnly` on a read-only handle, `Poisoned` on a
    /// fail-stopped one, and `InvalidArgs` for a bit this binary does not
    /// implement. If the manifest write itself fails the database fail-stops
    /// (like any durability failure) and the capability is *not* enabled — a
    /// reopen sees the pre-enable state.
    pub fn enable_format_capabilities(&self, bits: u64) -> Result<()> {
        self.inner.enable_capability(bits)
    }

    /// Format capabilities this database has durably enabled (a mask of
    /// [`KNOWN_CAPS`](crate::format::KNOWN_CAPS) bits); `0` for a database that
    /// has enabled none.
    pub fn format_capabilities(&self) -> u64 {
        self.inner.caps()
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
        // Stop pacing background IO *before* anything waits on it. The final
        // flushes below are drained by a spin-wait, and a flush worker parked on
        // the limiter would make that wait as long as the configured rate says
        // its bytes take — unbounded, from close's point of view. The limiter
        // exists to protect foreground latency, and after close is called there
        // is no more foreground work to protect: the right thing is to let
        // shutdown durability run at full speed.
        self.inner.cancel_background_io();
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
        // Every obsolete file the run queued must be gone before the lock is:
        // the compaction and flush workers are joined above, so the queue is
        // now closed, and pacing was cancelled at the top of `close`, so this
        // drains at full speed rather than at the configured rate.
        self.inner.drain_deletions();
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
                db.fail_stop(format!("background flush failed: {error}"));
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
            db.fail_stop(format!("unified flush failed: {error}"));
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
        FlushJob::PerCf { cf, imm } => {
            flush_per_cf(db, cf.clone(), imm);
            cf.pending_flushes.fetch_sub(1, Ordering::SeqCst);
        }
        FlushJob::Unified { imm } => flush_unified(db, imm),
    }
    db.pending_flush.fetch_sub(1, Ordering::SeqCst);
}

fn flush_worker(db: Arc<DbInner>, rx: Receiver<FlushJob>, stop: Arc<AtomicBool>) {
    // Dedicated thread: set the class once, never restore it.
    crate::ioctrl::set_class(crate::ioctrl::IoClass::Flush);
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
    // Dedicated thread. The part mover shares it and inherits `Compaction`,
    // which is right: moving a part between tiers is background IO.
    crate::ioctrl::set_class(crate::ioctrl::IoClass::Compaction);
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
    fn no_limiter_object_when_disabled() {
        // The default configuration must not allocate a limiter at all: the
        // rollback position for this feature is "one nil check per charge
        // point", not "a bucket that happens to be infinite".
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        assert!(db.inner.io_limiter.is_none());
        assert!(db.inner.ctx.io_limiter.is_none());
        db.close().unwrap();

        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.background_io_bytes_per_second = 1 << 20;
        let db = DB::open(opts).unwrap();
        assert!(db.inner.io_limiter.is_some());
        assert!(db.inner.ctx.io_limiter.is_some());
        db.close().unwrap();
    }

    #[test]
    fn no_deletion_worker_when_unpaced() {
        // The rollback position for review B: at rate 0 there is no channel and
        // no thread, and `remove_sst_file` unlinks on the caller's thread
        // exactly as it did before the worker existed.
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        assert!(db.inner.file_deletion.worker.is_none());
        let victim = dir.path().join("1.klog");
        std::fs::write(&victim, b"x").unwrap();
        db.inner.remove_sst_file(victim.to_str().unwrap(), 1 << 20);
        assert!(!victim.exists(), "unpaced deletion must unlink inline");
        db.close().unwrap();
    }

    #[test]
    fn retire_charges_metadata_minimum_for_empty_vlog() {
        // A CF with no separated values retires a `<id>.vlog` of zero bytes at
        // every compaction. Charging the literal 0 would make a storm of tiny
        // deletions free, and a storm of tiny deletions is exactly the thing
        // that saturates a device with metadata IO.
        let dir = tempfile::tempdir().unwrap();
        let recorder = Arc::new(crate::ioctrl::RecordingLimiter::default());
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.obsolete_delete_bytes_per_second = 1 << 30; // paced, but not slow
        opts.io_limiter = Some(recorder.clone());
        let db = DB::open(opts).unwrap();
        let empty = dir.path().join("7.vlog");
        std::fs::write(&empty, b"").unwrap();
        db.inner.remove_sst_file(empty.to_str().unwrap(), 0);
        db.close().unwrap(); // drains the worker

        assert!(!empty.exists());
        let charges: Vec<u64> = recorder
            .charges()
            .into_iter()
            .filter(|(class, _)| *class == crate::ioctrl::IoClass::ObsoleteDelete)
            .map(|(_, bytes)| bytes)
            .collect();
        assert_eq!(
            charges,
            vec![DELETE_METADATA_BYTES],
            "a zero-byte retirement must still cost one metadata block"
        );
    }

    #[test]
    fn paused_deletion_defers_to_the_pending_list() {
        // Pause semantics are what checkpoint and backup rest on, and the
        // worker must not become a way around them: while a pause is held not
        // even a queued task may reach the filesystem.
        let dir = tempfile::tempdir().unwrap();
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.obsolete_delete_bytes_per_second = 1 << 30;
        let db = DB::open(opts).unwrap();
        let victim = dir.path().join("9.klog");
        std::fs::write(&victim, b"x").unwrap();
        {
            let _pause = db.inner.pause_deletions();
            db.inner.remove_sst_file(victim.to_str().unwrap(), 4096);
            std::thread::sleep(Duration::from_millis(50));
            assert!(victim.exists(), "a pause must defer even a paced deletion");
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while victim.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            !victim.exists(),
            "the last guard drop must drain the pending list"
        );
        db.close().unwrap();
    }

    #[test]
    fn poison_does_not_hang_the_deletion_worker() {
        // Lives in-module because tripping the fail-stop flag needs the
        // crate-private poison handle. A worker parked on a one-byte-per-second
        // bucket outlives any test — and, in production, would hold up close on
        // a database that has already given up on durability.
        let dir = tempfile::tempdir().unwrap();
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.obsolete_delete_bytes_per_second = 1; // ~4096 seconds per file
        let db = DB::open(opts).unwrap();
        let victim = dir.path().join("11.klog");
        std::fs::write(&victim, b"x").unwrap();
        db.inner.remove_sst_file(victim.to_str().unwrap(), 1 << 20);
        // Give the worker time to actually park on the bucket.
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            victim.exists(),
            "the worker should still be waiting on credit"
        );

        let started = std::time::Instant::now();
        db.inner.fail_stop("test-induced".to_string());
        db.inner.drain_deletions();
        let drained_in = started.elapsed();

        assert!(!victim.exists(), "poison must release the parked worker");
        assert!(
            drained_in < Duration::from_secs(30),
            "poison must wake the deletion worker; drain took {drained_in:?}"
        );
        db.close().unwrap();
    }

    #[test]
    fn database_instance_ids_are_monotonic_and_unique() {
        let dir = tempfile::tempdir().unwrap();
        let mut previous = 0;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..32 {
            let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
            let id = db.inner.instance_id;
            assert_ne!(id, 0);
            assert!(id > previous, "database identities must be monotonic");
            assert!(seen.insert(id), "database identity {id} was reused");
            previous = id;
            db.close().unwrap();
        }
    }

    #[test]
    fn flush_memtable_waits_only_for_target_cf() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let a = db
            .create_column_family("a", ColumnFamilyConfig::default())
            .unwrap();
        let _b = db
            .create_column_family("b", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&a, b"key", b"value", Duration::ZERO).unwrap();

        // Stand in for a queued flush belonging to B. The target-A wait must
        // complete without depending on this database-wide accounting entry.
        db.inner.pending_flush.fetch_add(1, Ordering::SeqCst);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                done_tx.send(db.flush_memtable(&a)).unwrap();
            });
            let completed = done_rx.recv_timeout(Duration::from_secs(1));
            // Always release the artificial work before asserting, so the old
            // implementation can exit and the scoped thread cannot deadlock.
            db.inner.pending_flush.fetch_sub(1, Ordering::SeqCst);
            assert!(
                completed.is_ok(),
                "flush_memtable(A) waited for unrelated CF B work"
            );
            completed.unwrap().unwrap();
        });
        db.close().unwrap();
    }

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

    /// The enable path refuses a fail-stopped database *before* taking any
    /// lock, matching `Txn::commit`'s gate: a poisoned handle's manifest write
    /// would be the very failure that poisoned it.
    #[test]
    fn enable_on_poisoned_db_is_poisoned_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.inner.poison.set("simulated fsync failure".into());

        let err = db
            .enable_format_capabilities(crate::format::CAP_EXTENDED_RECORDS)
            .expect_err("a poisoned database cannot take a capability");
        assert!(matches!(err, OndaError::Poisoned(_)), "{err:?}");
        assert_eq!(db.format_capabilities(), 0);
        assert_eq!(db.inner.caps_durable.load(Ordering::SeqCst), 0);
        drop(cf);
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
