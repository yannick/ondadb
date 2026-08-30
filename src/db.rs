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
use crate::column_family::{CfCtx, ColumnFamily, FlushJob, ImmMemtable, SstHandle};
use crate::compaction;
use crate::comparator::comparator_by_name;
use crate::config::{ColumnFamilyConfig, Options};
use crate::error::{OndaError, Result};
use crate::manifest::{manifest_path, CfManifest, Manifest, WalLayout};
use crate::manifest_edit::VersionEdit;

const MAX_CF_NAME_LEN: usize = 128;
const WORKER_TICK: Duration = Duration::from_millis(50);

/// [`DbInner::periodic_check`] value meaning "no column family enables periodic
/// compaction" — the default, and a single relaxed load's worth of cost.
pub(crate) const PERIODIC_DISABLED: u64 = u64::MAX;
/// Floor of the derived periodic-scan cadence: an interval of a few seconds
/// must not turn the compaction worker into a spin loop.
const PERIODIC_CHECK_MIN: Duration = Duration::from_secs(1);
/// Ceiling of the derived cadence. A month-long interval still gets looked at
/// four times an hour, so the reclaim lag stays bounded by `interval + check +
/// one job` as the acceptance criterion states.
const PERIODIC_CHECK_MAX: Duration = Duration::from_secs(15 * 60);

/// Derived scan cadence for one family's interval: a quarter of it, clamped.
pub(crate) fn periodic_check_interval(interval: Duration) -> Duration {
    (interval / 4).clamp(PERIODIC_CHECK_MIN, PERIODIC_CHECK_MAX)
}
static NEXT_DB_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

/// The database's view of its `MANIFEST-EDITS` cursor (2.2).
///
/// `next_edit_id` is always `applied_through + 1` at the moment a snapshot is
/// written; between snapshots the log holds ids `applied_through + 1 ..
/// next_edit_id - 1`. `log` is `None` until `CAP_MANIFEST_EDITS` is durably
/// enabled — a database without the capability keeps rewriting whole snapshots,
/// byte-for-byte as every release before 2.2 did.
#[derive(Debug)]
struct EditLogState {
    generation: u64,
    applied_through: u64,
    next_edit_id: u64,
    log: Option<crate::manifest_edit::EditLog>,
    /// Size of the last snapshot written, feeding the compaction trigger's
    /// "don't rewrite 40 MiB to reclaim 4" arm.
    snapshot_bytes: u64,
}

impl Default for EditLogState {
    fn default() -> EditLogState {
        EditLogState {
            generation: 0,
            applied_through: 0,
            next_edit_id: 1,
            log: None,
            snapshot_bytes: 0,
        }
    }
}

/// Proof that the holder is inside a catalog transaction's publish step (2.2).
///
/// Every function that installs a new level set — the six publication
/// primitives listed in `docs/architecture.md`, plus the CF-registry and
/// partition-rule publications — takes one of these by reference. The type has
/// a private field and no public constructor, so a token can only be minted in
/// this module, and this module mints one in exactly two places:
///
/// * [`DbInner::catalog_txn`], after the edit record's fsync returned `Ok`;
/// * [`DbInner::prepare_capability`], whose staging is published by a **full
///   snapshot rewrite** rather than an edit — it is the path that turns the
///   edit log on, so it cannot itself be an edit.
///
/// That is what makes "no catalog mutation happens outside `catalog_txn`" a
/// compile-time property of `column_family.rs`, `compaction.rs`, `parts.rs`,
/// `ingest.rs` and `maintenance.rs` rather than a convention.
#[derive(Debug)]
pub(crate) struct Publish(());

impl Publish {
    /// Minted by `catalog_txn` once the edit is durable.
    fn after_durable_edit() -> Publish {
        Publish(())
    }

    /// Minted by the capability-enable path, which publishes through a full
    /// snapshot (see the type documentation).
    fn for_snapshot_rewrite() -> Publish {
        Publish(())
    }
}

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
    /// Committed-span index (1.2), or `None` when the build never needs one.
    ///
    /// Its lock sits immediately **after** `commit_mu` and **before**
    /// `manifest_mu` (see `docs/concurrency-and-safety.md`). Always present, but
    /// only touched once `CAP_RANGE_DELETES` is active, so a database that
    /// never issues a range delete pays one relaxed capability load per commit.
    pub(crate) span_index: Arc<crate::span_index::SpanIndex>,

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
    /// Guards the scheduled periodic-compaction scan, mirroring
    /// [`mover_running`](Self::mover_running) field for field. With
    /// `num_compaction_threads` workers, an unguarded scan would have every one
    /// of them walk the same levels and enqueue the same column family each
    /// derived interval.
    pub(crate) periodic_running: AtomicBool,
    /// Derived periodic-scan cadence in nanoseconds — the minimum over every
    /// column family of `periodic_compaction_interval / 4`, clamped to
    /// `[1s, 15m]` — or [`PERIODIC_DISABLED`] when no family enables the
    /// feature.
    ///
    /// Cached as one atomic so the compaction worker's default path is a single
    /// relaxed load per tick rather than a lock on the CF map: a database that
    /// never sets the option must gain no per-tick work at all. Per-CF configs
    /// are immutable once opened, so this only ever moves when a family is
    /// created or opened.
    pub(crate) periodic_check: AtomicU64,

    /// Admission for the **extra** threads a compaction job spawns to run its
    /// spans in parallel (0.8). See [`SpanPermits`].
    pub(crate) span_permits: SpanPermits,

    /// Serializes manifest rebuild+write. Multiple flush workers, the compaction
    /// worker, and CF create/drop all call `persist_manifest` concurrently; without
    /// this they would race on the shared temp file and could publish a torn manifest.
    manifest_mu: Mutex<()>,
    /// Serializes the catalog-shape changes that must *validate* before they
    /// publish: CF create / create-many / drop / clear, and partition-rule
    /// add/remove (2.2 slice 9).
    ///
    /// Before the migration these held `cfs.write()` (or the CF's rule lock)
    /// across the validation and the insert, which is no longer possible: the
    /// publication now happens inside `catalog_txn`, and `catalog_txn` takes
    /// `manifest_mu` and then `cfs.read()` — taking `cfs.write()` first would
    /// deadlock the moment the snapshot-compaction trigger fired. This lock
    /// keeps the "a losing concurrent creator sees `Exists`" guarantee those
    /// call sites had. Always taken **before** `manifest_mu`, never after.
    pub(crate) cf_lifecycle_mu: Mutex<()>,
    /// Durable database-wide WAL layout written by `persist_manifest`.
    wal_layout: Mutex<WalLayout>,
    /// Edit-log cursor (2.2), guarded by `manifest_mu` in effect: it is only
    /// read and written inside the manifest critical section. Kept beside the
    /// catalog rather than derived at write time so a full rewrite carries the
    /// cursor forward instead of silently resetting it to "no log".
    edit_log: Mutex<EditLogState>,
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
    pub(crate) caps: Arc<AtomicU64>,
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

    /// Injectable wall clock, read **only** by periodic-compaction stamping and
    /// eligibility (see [`crate::util::Clock`]). Shared with every
    /// [`CfCtx`](crate::column_family::CfCtx) so flush output stamps from the
    /// same source the picker measures against.
    pub(crate) clock: Arc<crate::util::Clock>,

    /// Count of successful physical WAL `sync_data` calls across every WAL this
    /// DB has opened (per-CF, unified, and post-rotation). Observability/test
    /// hook (see [`DB::wal_sync_count`]); relaxed increments on an fsync-bound path.
    pub(crate) wal_syncs: Arc<AtomicU64>,

    /// Part operations (detach / attach / freeze / export / tier move) currently
    /// running anywhere in this database (1.2).
    ///
    /// Those operations move, hard-link and re-catalogue files in phases that
    /// their own range lock does not span end-to-end — `attach_part` copies
    /// bytes before it knows which range it will claim, and a tier move flips a
    /// manifest entry after its files have already been relocated. Delete-only
    /// excise is opportunistic, so it simply declines to run while any of that
    /// is in flight rather than reason about the interleavings.
    parts_in_flight: AtomicU64,
}

/// Marks a part operation in flight for the lifetime of the guard, so
/// [`crate::excise`] declines to run beside it. See
/// [`DbInner::parts_in_flight`].
#[derive(Debug)]
pub(crate) struct PartsOpGuard<'a> {
    inner: &'a DbInner,
}

impl Drop for PartsOpGuard<'_> {
    fn drop(&mut self) {
        self.inner.parts_in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Test-only rendezvous **inside** the `commit_mu` critical section (1.2).
///
/// The commit guard is a claim about a lock, and a lock is not directly
/// observable — so the claim is tested by *interleaving*: a commit that reaches
/// this point parks until the test releases it, and the test then checks
/// whether another commit that takes `commit_mu` can proceed meanwhile. The
/// park sits inside the guard's scope, so if the guard were not taken the
/// concurrent commit would complete and the test would fail.
///
/// Debug builds only, armed explicitly, fires once. Mirrors
/// [`crate::memtable::snapshot_calls`] as a test hook that lives in the code it
/// measures rather than in a parallel copy of it.
#[cfg(debug_assertions)]
pub mod commit_park {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc::{Receiver, SyncSender};

    /// Instance id of the database whose commits may park, or `0` for none.
    ///
    /// Scoped to one database because the hook is a process-global static and
    /// the test binary runs tests in parallel: without it, an unrelated test's
    /// range commit would trip the arm.
    static ARMED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    static CHANNELS: parking_lot::Mutex<Option<(SyncSender<()>, Receiver<()>)>> =
        parking_lot::Mutex::new(None);

    /// Arm the park for the next range commit on the database `instance`.
    ///
    /// Returns `(entered, release)`: `entered.recv()` blocks until a commit is
    /// parked inside `commit_mu`, and `release.send(())` lets it out.
    #[doc(hidden)]
    pub fn arm(instance: u64) -> (Receiver<()>, SyncSender<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *CHANNELS.lock() = Some((entered_tx, release_rx));
        ARMED.store(instance, Ordering::SeqCst);
        (entered_rx, release_tx)
    }

    /// Disarm, whether or not the park fired.
    #[doc(hidden)]
    pub fn disarm() {
        ARMED.store(0, Ordering::SeqCst);
        *CHANNELS.lock() = None;
    }

    /// Park if armed for `instance`. Called with `commit_mu` held.
    pub(crate) fn park_if_armed(instance: u64) {
        if ARMED.load(Ordering::SeqCst) != instance
            || ARMED
                .compare_exchange(instance, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return;
        }
        let taken = CHANNELS.lock().take();
        if let Some((entered, release)) = taken {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
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
    ///
    /// With `CAP_MANIFEST_EDITS` enabled this is a **snapshot compaction**: the
    /// four-step protocol that writes a new snapshot and restarts the edit log
    /// empty. Without the capability it is the full rewrite every release
    /// before 2.2 performed, byte-for-byte.
    pub(crate) fn persist_manifest(&self) -> Result<()> {
        if self.opts.read_only {
            return Ok(());
        }
        // Serialize the whole rebuild+write so concurrent callers (flush workers,
        // the compaction worker, CF create/drop) can never race on the temp file
        // or publish an inconsistent manifest.
        let _mu = self.manifest_mu.lock();
        let mut edits = self.edit_log.lock();
        self.write_snapshot(&mut edits)
    }

    /// One catalog transaction: make the edit durable, then publish it.
    ///
    /// The seven steps, in order:
    ///
    /// 1. the caller built `edit` and the candidate in-memory state, and has
    ///    published nothing;
    /// 2. every newly referenced file is already finished and fsynced
    ///    (`Writer::finish`), which this does not re-do;
    /// 3. under `manifest_mu`, the complete record is appended, flushed and
    ///    **fsynced** — this is the commit point, and the point WAL reclaim and
    ///    obsolete-input deletion key off (AGENTS.md invariant 1);
    /// 4. `publish` installs the candidate state;
    /// 5. the caller retires removed handles, after publication, through
    ///    `remove_sst_file` (invariant 6);
    /// 6. still under `manifest_mu`, the snapshot-compaction trigger is checked;
    /// 7. on failure nothing is published, the old state stays visible, and the
    ///    database fail-stops — the caller cleans up its never-installed files.
    ///
    /// Until `CAP_MANIFEST_EDITS` is enabled there is no log, and this falls
    /// back to the pre-2.2 order — publish, then rewrite the whole manifest —
    /// so enabling the capability is the only thing that changes behaviour.
    ///
    /// `publish` receives a [`Publish`] token: the six level-set primitives and
    /// the CF-registry / partition-rule publications all demand one, so they are
    /// unreachable from anywhere else.
    ///
    /// `next_file_id` and `global_seq` are deliberately **not** carried by every
    /// edit. They are reconciled at recovery from what the catalog actually
    /// references (`manifest_edit::recover_catalog`, rule r7), which is both
    /// smaller and safe under concurrency: two transactions serialize on
    /// `manifest_mu` in an order the file-id allocator does not, so a
    /// per-site `SetNextFileID` could land out of order and fail its own
    /// monotonicity precondition on replay.
    pub(crate) fn catalog_txn(
        &self,
        edit: VersionEdit,
        publish: impl FnOnce(&Publish),
    ) -> Result<()> {
        self.catalog_txn_with_rollback(edit, publish, |_| {})
    }

    /// [`catalog_txn`](Self::catalog_txn) for the one caller that has an
    /// in-memory undo: compaction, whose install swaps a whole level set.
    ///
    /// `rollback` runs in exactly one situation — the **pre-capability** path
    /// published (it has to: the snapshot is rebuilt from live state) and then
    /// the snapshot write failed. With the edit log on, a failed append
    /// publishes nothing and `rollback` is never called; and once the edit is
    /// durable it is never called either, because rolling back committed state
    /// is how a catalog comes to name files a later step deletes.
    pub(crate) fn catalog_txn_with_rollback(
        &self,
        edit: VersionEdit,
        publish: impl FnOnce(&Publish),
        rollback: impl FnOnce(&Publish),
    ) -> Result<()> {
        self.poison.check()?;
        if self.opts.read_only {
            // Exactly `persist_manifest`'s early return: nothing is made
            // durable. The in-memory publication still happens, so a read-only
            // handle's view stays consistent with what it just did.
            publish(&Publish::after_durable_edit());
            return Ok(());
        }
        let _mu = self.manifest_mu.lock();
        let mut st = self.edit_log.lock();
        if st.log.is_none() {
            // Pre-capability: today's publish-then-persist order, unchanged.
            publish(&Publish::after_durable_edit());
            let res = self.write_snapshot(&mut st);
            if res.is_err() {
                rollback(&Publish::after_durable_edit());
            }
            return res;
        }
        if edit.is_empty() {
            // An edit with no ops changes no catalog state, so there is nothing
            // to make durable and nothing a later replay could need. Appending
            // an empty record would only burn an id and an fsync. The in-memory
            // publication (a flush that produced no table still has to retire
            // its sealed memtable) still happens.
            publish(&Publish::after_durable_edit());
            return Ok(());
        }
        let edit_id = st.next_edit_id;
        let appended = st
            .log
            .as_mut()
            .expect("checked just above")
            .append(edit_id, &edit);
        if let Err(e) = appended {
            // Step 7. The candidate was never published, so the old state is
            // still the visible one; the failed fsync may have dropped pages,
            // so the database fail-stops exactly as a failed persist does.
            self.fail_stop(format!("manifest edit {edit_id} failed: {e}"));
            return Err(e);
        }
        st.next_edit_id = edit_id + 1;
        // Step 4: publication follows the durable edit, never precedes it.
        publish(&Publish::after_durable_edit());
        self.manifest_persists.fetch_add(1, Ordering::Relaxed);
        // Step 6, in the same critical section: a record appended between a
        // snapshot write and its log rename would be silently lost.
        let (bytes, count) = st
            .log
            .as_ref()
            .map(|l| (l.bytes(), l.count()))
            .unwrap_or((0, 0));
        if crate::manifest_edit::snapshot_due(bytes, count, st.snapshot_bytes) {
            // Deliberately not propagated. The edit is already durable, so this
            // transaction COMMITTED; returning `Err` here would tell the caller
            // to unlink files the catalog now permanently references. A failed
            // compaction has already fail-stopped the database (`write_snapshot`),
            // and every state it can leave behind is one recovery accepts: the
            // live log was never truncated in place, so the old log still
            // carries every record the new snapshot does not.
            let _ = self.write_snapshot(&mut st);
        }
        Ok(())
    }

    /// Write the catalog as a snapshot, holding `manifest_mu` and `st`.
    ///
    /// With a log active this runs the full four-step compaction and installs
    /// the fresh, empty log; without one it is a plain `Manifest::save`.
    fn write_snapshot(&self, st: &mut EditLogState) -> Result<()> {
        let edits_enabled =
            self.caps_durable.load(Ordering::SeqCst) & crate::format::CAP_MANIFEST_EDITS != 0;
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
            // Edit-log bookkeeping (2.2). A full rewrite is a snapshot that
            // contains everything appended so far, so it always carries the
            // current cursor forward; snapshot compaction advances it.
            generation: st.generation,
            applied_through: st.applied_through,
            next_edit_id: st.next_edit_id,
        };
        for cf in cfs.values() {
            m.cfs.push(CfManifest {
                name: cf.name().to_string(),
                config: cf.effective_config().encode(),
                sstables: cf.snapshot_ssts(),
            });
        }
        drop(cfs);
        let res = if edits_enabled {
            // Everything appended so far is in `m`, so the snapshot's
            // `applied_through` is the last id the log handed out. The old log
            // is replaced by a rename, never truncated in place.
            let applied_through = st.next_edit_id - 1;
            crate::manifest_edit::compact_snapshot(&self.dir, &mut m, applied_through).map(|log| {
                st.generation = m.generation;
                st.applied_through = applied_through;
                st.next_edit_id = applied_through + 1;
                st.log = Some(log);
            })
        } else {
            m.save(manifest_path(&self.dir))
        };
        if res.is_ok() {
            st.snapshot_bytes = std::fs::metadata(manifest_path(&self.dir))
                .map(|md| md.len())
                .unwrap_or(0);
        }
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
    /// The committed-span index, or `None` while range deletes are not enabled.
    ///
    /// Returning `None` rather than an empty index is what makes the gate
    /// *provable*: a caller cannot accidentally consult the index on a database
    /// that has never enabled the capability.
    #[inline]
    pub(crate) fn span_index(&self) -> Option<&crate::span_index::SpanIndex> {
        (self.caps() & crate::format::CAP_RANGE_DELETES != 0).then_some(&self.span_index)
    }

    /// Reserve `n` span-index slots ahead of `commit_mu`.
    ///
    /// `blocking` splits the two commit kinds, and the split is the whole
    /// policy: a **range** commit waits for room, because dropping one of its
    /// markers would blind every later point writer; a **point** commit never
    /// waits — the write path must not stall behind a bookkeeping structure —
    /// and gives up its marker instead, raising the index's overflow watermark
    /// so range writers stay conservative. `n == 0` is free either way.
    pub(crate) fn reserve_span_markers(
        &self,
        n: usize,
        blocking: bool,
        own_snapshot: Option<u64>,
    ) -> Result<Option<crate::span_index::SpanReservation<'_>>> {
        if !blocking {
            return Ok(self.span_index.try_reserve(n));
        }
        self.span_index.reserve(
            n,
            || self.oldest_snapshot(),
            || self.closing.load(Ordering::Relaxed),
            // Waiting is futile when the caller's OWN snapshot is the prune
            // floor: nothing it waits for can happen until it commits.
            || own_snapshot.is_some_and(|seq| self.oldest_snapshot() >= seq),
        )
    }

    /// Drop span markers no live transaction can still conflict against.
    pub(crate) fn prune_span_markers(&self) {
        self.span_index.prune(self.oldest_snapshot());
    }

    pub(crate) fn caps(&self) -> u64 {
        self.caps.load(Ordering::SeqCst)
    }

    /// Current reading of the injectable clock (0.3). Read only by periodic
    /// stamping and eligibility; see [`crate::util::Clock`].
    pub(crate) fn now(&self) -> i64 {
        self.clock.now()
    }

    /// Fold `cf`'s periodic interval into the cached scan cadence. Called from
    /// every site that registers a column family, so a family created after
    /// open starts the scan just as one recovered at open does.
    pub(crate) fn note_periodic_cf(&self, cf: &ColumnFamily) {
        let interval = cf.opts.periodic_compaction_interval;
        if interval.is_zero() {
            return;
        }
        let check = periodic_check_interval(interval).as_nanos() as u64;
        self.periodic_check.fetch_min(check, Ordering::Relaxed);
    }

    /// One pass of the periodic scan: enqueue every column family holding a
    /// table older than its interval.
    ///
    /// The pass only *sends*; [`compaction::pick_compaction`] re-derives
    /// eligibility under its normal locks, so a family that stops qualifying
    /// between the scan and the job simply produces no work. `try_send` on the
    /// unbounded channel cannot block, and a disconnected channel (a closing
    /// database) is a benign miss.
    pub(crate) fn run_periodic_scan(&self) -> usize {
        if self.opts.read_only || self.poison.check().is_err() {
            return 0;
        }
        if self.caps() & crate::format::CAP_PERIODIC_AGE == 0 {
            return 0;
        }
        let now = self.now();
        let cfs: Vec<Arc<ColumnFamily>> = self.cfs.read().values().cloned().collect();
        let mut queued = 0usize;
        for cf in &cfs {
            if cf.opts.periodic_compaction_interval.is_zero()
                || cf.opts.compaction_style == crate::config::CompactionStyle::Fifo
            {
                continue;
            }
            if compaction::periodic_candidate(self, cf, now).is_none() {
                continue;
            }
            if self.ctx.compact_tx.try_send(cf.clone()).is_ok() {
                queued += 1;
            }
        }
        queued
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
        // Implied capabilities: a range delete is written as a kind-bearing
        // envelope record, so enabling it without CAP_EXTENDED_RECORDS would
        // let a writer produce bytes the manifest does not authorize. Expanding
        // here keeps the caller's contract simple (`enable(CAP_RANGE_DELETES)`)
        // and the durability rule exact (both bits land in one manifest write).
        let bits = if bits & crate::format::CAP_RANGE_DELETES != 0 {
            bits | crate::format::CAP_EXTENDED_RECORDS
        } else {
            bits
        };
        let _mu = self.enable_mu.lock();
        let active = self.caps.load(Ordering::SeqCst);
        if active & bits == bits {
            return Ok(());
        }
        let previous = self.caps_durable.load(Ordering::SeqCst);
        self.caps_durable.store(previous | bits, Ordering::SeqCst);
        // Prepare hook: a capability whose enable transition carries a one-time
        // catalog change stages it HERE, so the change and the bit that
        // authorizes it reach disk in the same manifest write. Two writes would
        // leave a crash window in which one exists without the other.
        let undo = self.prepare_capability(bits);
        if let Err(e) = self.persist_manifest() {
            self.caps_durable.store(previous, Ordering::SeqCst);
            undo_capability_prepare(undo);
            return Err(e);
        }
        self.caps.store(active | bits, Ordering::SeqCst);
        Ok(())
    }

    /// Stage the in-memory catalog changes `bits` implies, returning what the
    /// levels looked like before so a failed manifest write can put them back.
    ///
    /// Only `CAP_PERIODIC_AGE` has one today: it stamps every local,
    /// non-mounted table whose age state is `None` with the enable time.
    ///
    /// The alternative — "eligible one interval after open" — is not
    /// restart-safe: open time is not durable, so a database restarted more
    /// often than its interval would never become eligible at all. Stamping at
    /// enable time makes the clock durable from the first moment the feature
    /// exists, at the cost of one catalog rewrite and no table IO.
    ///
    /// Foreign mounts are skipped: this database must never rewrite bytes it
    /// did not publish, so giving one an age would create a candidate the
    /// picker is obliged to veto. A table that already carries a stamp is left
    /// alone, which is what makes a re-enable a no-op.
    fn prepare_capability(&self, bits: u64) -> CapabilityPrepareUndo {
        if bits & crate::format::CAP_PERIODIC_AGE == 0 {
            return Vec::new();
        }
        let at = self.now();
        let cfs: Vec<Arc<ColumnFamily>> = self.cfs.read().values().cloned().collect();
        let mut undo = Vec::new();
        for cf in cfs {
            let stamped = restamp(&cf, |table| {
                (table.meta.last_compaction_time.is_none()
                    && !compaction::is_foreign_mount(self, &table.meta))
                .then_some(Some(at))
            });
            if !stamped.is_empty() {
                undo.push((cf.clone(), stamped));
            }
        }
        undo
    }

    /// Number of successful manifest persists so far (see `manifest_persists`).
    /// A test/observability lever — batch operations assert they persist once.
    #[allow(dead_code)]
    pub(crate) fn manifest_persist_count(&self) -> u64 {
        self.manifest_persists.load(Ordering::Relaxed)
    }

    /// Publish a newly built column family into the registries.
    ///
    /// A publication primitive: the `DbInner::cfs` / `cf_by_id` insert is a
    /// catalog mutation exactly as a level-set swap is, so it demands the same
    /// token (see [`Publish`]).
    pub(crate) fn register_cf(&self, cf: &Arc<ColumnFamily>, _p: &Publish) {
        self.cfs.write().insert(cf.name().to_string(), cf.clone());
        // Same name => same stable id, so this also replaces a cleared family's
        // routing entry.
        self.cf_by_id.write().insert(cf.id(), cf.clone());
        self.note_periodic_cf(cf);
    }

    /// Remove a column family from the registries, returning the old handle.
    /// The other half of [`register_cf`](Self::register_cf).
    pub(crate) fn unregister_cf(&self, name: &str, _p: &Publish) -> Option<Arc<ColumnFamily>> {
        let cf = self.cfs.write().remove(name);
        if let Some(cf) = &cf {
            self.cf_by_id.write().remove(&cf.id());
        }
        cf
    }

    /// Publish the minted instance nonce (A2). Object names embed it, so it is
    /// installed only once its `SetNonce` edit is durable.
    pub(crate) fn publish_instance_nonce(&self, nonce: u64, _p: &Publish) {
        *self.instance_nonce.lock() = Some(nonce);
    }

    /// Publish the database-wide WAL layout flip (per-CF -> unified, one way).
    pub(crate) fn publish_wal_layout(&self, layout: WalLayout, _p: &Publish) {
        *self.wal_layout.lock() = layout;
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

    /// Mark a part operation in flight until the returned guard drops. See
    /// [`parts_in_flight`](Self::parts_in_flight).
    pub(crate) fn begin_parts_op(&self) -> PartsOpGuard<'_> {
        self.parts_in_flight.fetch_add(1, Ordering::SeqCst);
        PartsOpGuard { inner: self }
    }

    /// Whether any part operation is currently running.
    pub(crate) fn parts_in_flight(&self) -> bool {
        self.parts_in_flight.load(Ordering::SeqCst) > 0
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
        // Crash simulation only (`util::fault::Call::Unlink`): leave the file
        // where it is, which is exactly the orphan a crash between the durable
        // catalog edit and this unlink produces. Free — and gone entirely from
        // release builds' behaviour — when no plan is installed.
        if crate::util::fault::check(crate::util::fault::Call::Unlink).is_err() {
            return;
        }
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

/// The ops that retire a column family: every table it holds, then the family.
///
/// `DropCF`'s precondition is that the same edit removed all of its tables, so
/// the two always travel together.
fn drop_cf_ops(name: &str, cf: &Arc<ColumnFamily>) -> Vec<crate::manifest_edit::Op> {
    let ids: Vec<u64> = cf.snapshot_ssts().into_iter().map(|meta| meta.id).collect();
    let mut ops = Vec::with_capacity(2);
    if !ids.is_empty() {
        ops.push(crate::manifest_edit::Op::RemoveTables {
            cf: name.to_string(),
            ids,
        });
    }
    ops.push(crate::manifest_edit::Op::DropCf {
        name: name.to_string(),
    });
    ops
}

/// Which tables [`DbInner::prepare_capability`] stamped, per column family, so
/// a failed manifest write can put exactly those back.
type CapabilityPrepareUndo = Vec<(Arc<ColumnFamily>, std::collections::HashSet<u64>)>;

/// Rewrite the age state of every table `pick` selects, atomically over one
/// level snapshot, returning the ids that changed.
///
/// `pick` returns the new value for a table it wants changed, or `None` to
/// leave it alone. Handles are rebuilt rather than mutated because `SstMeta`
/// lives inside an `Arc<SstHandle>`; rebuilding keeps the table's reader-cache
/// entry, which is keyed by file id, so this costs no reader re-open.
fn restamp(
    cf: &Arc<ColumnFamily>,
    pick: impl Fn(&SstHandle) -> Option<Option<i64>>,
) -> std::collections::HashSet<u64> {
    let mut changed = std::collections::HashSet::new();
    // The capability-enable path publishes through a full snapshot rewrite, not
    // an edit — it is the path that turns the edit log on, so it cannot itself
    // be an edit. See `Publish`.
    let token = Publish::for_snapshot_rewrite();
    cf.update_levels(
        |levels| {
            levels
                .iter()
                .map(|level| {
                    level
                        .iter()
                        .map(|table| match pick(table) {
                            Some(stamp) => {
                                changed.insert(table.meta.id);
                                let mut meta = table.meta.clone();
                                meta.last_compaction_time = stamp;
                                cf.handle_for(meta)
                            }
                            None => table.clone(),
                        })
                        .collect()
                })
                .collect()
        },
        &token,
    );
    changed
}

/// Undo a [`DbInner::prepare_capability`] staging after a failed manifest write.
///
/// The write failed, so nothing durable changed; leaving the stamps in memory
/// would let this handle behave as if the capability were on, and a later
/// persist could publish age state whose capability bit was rolled back.
///
/// Restores by id rather than by reinstating the saved level vectors: a flush
/// that installed into L0 in between must not be discarded (that wholesale
/// overwrite is a past data-loss bug — see `ColumnFamily::update_levels`).
fn undo_capability_prepare(undo: CapabilityPrepareUndo) {
    for (cf, ids) in undo {
        restamp(&cf, |table| ids.contains(&table.meta.id).then_some(None));
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
    // A writable database with the capability keeps its log open for append,
    // positioned past the last complete record recovery accepted; a read-only
    // one replays the log and never writes to it (2.2 recovery rule r8).
    //
    // `None` here with the capability set is the legal transition state: the
    // enable persisted the snapshot but crashed before the log's rename. It
    // heals on the next persist, which is a snapshot compaction and creates
    // one; until then `catalog_txn` takes its pre-capability path, which is
    // still correct — it just rewrites the whole manifest.
    let edit_log = if opts.read_only || manifest.caps & crate::format::CAP_MANIFEST_EDITS == 0 {
        None
    } else {
        crate::manifest_edit::open_log_for_append(&dir)?
    };
    let snapshot_bytes = std::fs::metadata(manifest_path(&dir))
        .map(|md| md.len())
        .unwrap_or(0);
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
    // One `Arc` each, shared with `DbInner` below: a flush stamping a table and
    // `enable_capability` flipping the bit must be looking at the same words.
    let caps: Arc<AtomicU64> = Arc::new(AtomicU64::new(manifest.caps));
    let clock = Arc::new(crate::util::Clock::new());
    // One index for the whole database, shared with every column family so
    // per-family stats can report its size (1.2).
    let span_index = Arc::new(crate::span_index::SpanIndex::new(opts.span_index_capacity));
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
        caps: caps.clone(),
        clock: clock.clone(),
        span_index: span_index.clone(),
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
        span_index: span_index.clone(),
        next_file_id: AtomicU64::new(manifest.next_file_id.max(1)),
        closing,
        stop,
        pending_flush,
        mover_running: AtomicBool::new(false),
        span_permits: SpanPermits::new(match opts.max_subcompaction_workers {
            0 => opts.num_compaction_threads.max(1),
            n => n,
        }),
        periodic_running: AtomicBool::new(false),
        periodic_check: AtomicU64::new(PERIODIC_DISABLED),
        manifest_mu: Mutex::new(()),
        cf_lifecycle_mu: Mutex::new(()),
        wal_layout: Mutex::new(requested_layout),
        edit_log: Mutex::new(EditLogState {
            generation: manifest.generation,
            applied_through: manifest.applied_through,
            next_edit_id: manifest.next_edit_id,
            log: edit_log,
            snapshot_bytes,
        }),
        instance_nonce: Mutex::new(manifest.instance_nonce),
        // A capability recorded in the manifest is already durable, so both
        // words start from it: a reopen after a crash between persist and flip
        // simply sees an enabled database.
        caps,
        caps_durable: AtomicU64::new(manifest.caps),
        enable_mu: Mutex::new(()),
        manifest_persists: AtomicU64::new(0),
        file_deletion: FileDeletionState::new(opts),
        workers: Mutex::new(Vec::new()),
        lock_file: Mutex::new(Some(lock_file)),
        handles: Arc::new(AtomicUsize::new(1)),
        poison,
        clock,
        wal_syncs,
        parts_in_flight: AtomicU64::new(0),
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
        resolve_merge_operator(&mut config, opts, &persisted.name)?;
        let (cf, max_seq) = ColumnFamily::load(
            inner.ctx.clone(),
            persisted.name.clone(),
            inner.cf_dir(&persisted.name),
            config,
            comparator,
            &persisted.sstables,
        )?;
        inner.observe_seq(max_seq);
        inner.note_periodic_cf(&cf);
        inner.cf_by_id.write().insert(cf.id(), cf.clone());
        inner.cfs.write().insert(persisted.name.clone(), cf);
    }
    Ok(())
}

/// Reject an `Options::merge_fns` list holding two operators with the same
/// [`MergeOperator::name`](crate::MergeOperator::name).
///
/// At open, not at first use: the engine would otherwise resolve a stored name
/// to whichever of the two came first in the vector and fold that family's
/// whole history with it, silently and non-reproducibly. The check is over the
/// registry alone, so a duplicate is refused even when no column family names
/// it yet — the ambiguity is the caller's bug either way.
fn check_merge_fn_names(opts: &Options) -> Result<()> {
    for (i, a) in opts.merge_fns.iter().enumerate() {
        if opts.merge_fns[..i].iter().any(|b| b.name() == a.name()) {
            return Err(OndaError::InvalidArgs(format!(
                "Options::merge_fns registers two merge operators named {:?}",
                a.name()
            )));
        }
    }
    Ok(())
}

/// Exchange a column family's stored merge-operator *name* for the
/// implementation registered in [`Options::merge_fns`](crate::Options::merge_fns).
///
/// The mirror of [`resolve_partition_scheme`], and for the same reason: the
/// manifest can only carry a name. The rules, in order:
///
/// 1. No stored name — nothing to resolve.
/// 2. A stored name with no registered implementation is
///    [`OndaError::InvalidArgs`], never a silent fallback to "no operator":
///    every operand already on disk would then read back as its own raw bytes,
///    which is a wrong answer rather than a missing one.
/// 3. Two registrations of one name are refused earlier, by
///    [`check_merge_fn_names`] at open.
/// 4. **The stored name wins.** This function is only ever handed the config
///    decoded from the manifest, and `create_column_family` on an existing
///    family returns `Exists`, so a caller has no path to rename a family's
///    operator — folding operands with a different operator than wrote them is
///    unreachable through the API.
fn resolve_merge_operator(
    config: &mut ColumnFamilyConfig,
    opts: &Options,
    cf_name: &str,
) -> Result<()> {
    let Some(name) = config.merge_operator_name.clone() else {
        config.merge_operator = None;
        return Ok(());
    };
    let found = opts
        .merge_fns
        .iter()
        .find(|operator| operator.name() == name)
        .cloned()
        .ok_or_else(|| {
            OndaError::InvalidArgs(format!(
                "column family {cf_name:?} was written with merge operator {name:?}, \
                 which is not registered in `Options::merge_fns`"
            ))
        })?;
    config.merge_operator = Some(found);
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
    // One `SetNonce` edit, durable before the nonce is installed: object names
    // embed it, so a crash must never leave objects named after a nonce a
    // reopen would re-mint differently.
    let nonce = mint_instance_nonce(&inner.dir);
    let edit = VersionEdit::new(vec![crate::manifest_edit::Op::SetNonce(nonce)]);
    inner.catalog_txn(edit, |p| inner.publish_instance_nonce(nonce, p))
}

impl DB {
    /// Open (creating if needed) the database at `opts.path`.
    pub fn open(opts: Options) -> Result<DB> {
        if opts.path.is_empty() {
            return Err(OndaError::InvalidArgs("empty path".into()));
        }
        check_merge_fn_names(&opts)?;
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

        // A leftover MANIFEST.tmp / MANIFEST-EDITS.tmp is a crash artifact, not
        // state: it is removed before anything is loaded, and never read. A
        // read-only open leaves them alone — it writes nothing, not even an
        // unlink.
        if !opts.read_only {
            crate::manifest_edit::sweep_manifest_temp_files(&dir)?;
        }
        // The WAL layout is a durable database-wide choice once the catalog
        // contains a column family. Opening under the other layout would make
        // recovery consult one set of WALs while new commits write another.
        let manifest = crate::manifest_edit::recover_catalog(&dir)?;
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

        let edit = VersionEdit::new(vec![crate::manifest_edit::Op::SetWalLayout(
            WalLayout::Unified,
        )]);
        db.inner
            .catalog_txn(edit, |p| db.inner.publish_wal_layout(WalLayout::Unified, p))?;
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
        let mut config = config;
        resolve_merge_operator(&mut config, &self.inner.opts, name)?;
        // Persist-before-use: the capability reaches the manifest before the
        // family that may write kind-4 records exists, so a binary too old to
        // fold operands refuses the database instead of reading them as values.
        if config.merge_operator_name.is_some() {
            self.inner
                .enable_capability(crate::format::CAPS_MERGE_WRITE)?;
        }
        // `cf_lifecycle_mu`, not `cfs.write()`, is what makes a losing racer see
        // `Exists` now: the registry insert happens inside the transaction, and
        // holding the registry's write lock across `catalog_txn` would deadlock
        // against the snapshot the trigger may take (see the field's docs).
        let _lifecycle = self.inner.cf_lifecycle_mu.lock();
        if self.inner.cfs.read().contains_key(name) {
            return Err(OndaError::Exists(name.into()));
        }
        let cf = ColumnFamily::create(
            self.inner.ctx.clone(),
            name.to_string(),
            self.inner.cf_dir(name),
            config,
            cmp,
        )?;
        let edit = VersionEdit::new(vec![crate::manifest_edit::Op::CreateCf {
            name: name.to_string(),
            config: cf.effective_config().encode(),
        }]);
        // A failed transaction leaves the directory and its empty WAL behind,
        // referenced by no catalog — exactly what a crash between the old
        // create and its manifest persist left, and what the open-time sweep
        // already treats as an artifact.
        self.inner
            .catalog_txn(edit, |p| self.inner.register_cf(&cf, p))?;
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
            resolve_merge_operator(&mut config.clone(), &self.inner.opts, name)?;
            if seen.insert(name, ()).is_some() {
                return Err(OndaError::Exists((*name).into()));
            }
        }
        if specs
            .iter()
            .any(|(_, config)| config.merge_operator_name.is_some())
        {
            self.inner
                .enable_capability(crate::format::CAPS_MERGE_WRITE)?;
        }

        // Hold `cf_lifecycle_mu` across the whole batch: check every name is
        // free, then build and publish them together, so a concurrent creator
        // can neither observe a half-built batch nor collide with one. (Before
        // 2.2 this was the registry's own write lock; it cannot be, now that the
        // insert happens inside `catalog_txn` — see the field's docs.)
        let _lifecycle = self.inner.cf_lifecycle_mu.lock();
        {
            let cfs = self.inner.cfs.read();
            for (name, _) in specs {
                if cfs.contains_key(*name) {
                    return Err(OndaError::Exists((*name).into()));
                }
            }
        }
        let mut created = Vec::with_capacity(specs.len());
        for (name, config) in specs {
            let cmp =
                comparator_by_name(&config.comparator_name).expect("comparator validated above");
            let mut config = config.clone();
            resolve_merge_operator(&mut config, &self.inner.opts, name)?;
            let cf = ColumnFamily::create(
                self.inner.ctx.clone(),
                (*name).to_string(),
                self.inner.cf_dir(name),
                config,
                cmp,
            )?;
            created.push(cf);
        }
        // ONE edit for the entire batch: N `CreateCF` ops in one record, one
        // append, one fsync — the same collapse the batch always promised, now
        // measured in edit bytes rather than whole-catalog rewrites.
        let edit = VersionEdit::new(
            created
                .iter()
                .map(|cf| crate::manifest_edit::Op::CreateCf {
                    name: cf.name().to_string(),
                    config: cf.effective_config().encode(),
                })
                .collect(),
        );
        let publish = created.clone();
        self.inner.catalog_txn(edit, |p| {
            for cf in &publish {
                self.inner.register_cf(cf, p);
            }
        })?;
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
        let _lifecycle = self.inner.cf_lifecycle_mu.lock();
        let cf = self
            .inner
            .cfs
            .read()
            .get(name)
            .cloned()
            .ok_or(OndaError::NotFound)?;
        // The `DropCF` edit is durable BEFORE any file is unlinked. Before 2.2
        // the directory went first, so a crash in that window left a manifest
        // naming a directory that no longer existed; this is the same shape as
        // invariant 1, applied to a drop.
        let edit = VersionEdit::new(drop_cf_ops(name, &cf));
        self.inner.catalog_txn(edit, |p| {
            self.inner.unregister_cf(name, p);
        })?;
        cf.close_resources();
        let _ = std::fs::remove_dir_all(self.inner.cf_dir(name));
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
        let _lifecycle = self.inner.cf_lifecycle_mu.lock();
        let old = self
            .inner
            .cfs
            .read()
            .get(name)
            .cloned()
            .ok_or(OndaError::NotFound)?;
        let cfg = old.effective_config();
        let cmp = comparator_by_name(&cfg.comparator_name).ok_or_else(|| {
            OndaError::InvalidArgs(format!("unknown comparator {}", cfg.comparator_name))
        })?;
        // ONE edit: every table removed, the family dropped, the same name
        // re-created empty. Durable before a single byte is unlinked.
        let mut ops = drop_cf_ops(name, &old);
        ops.push(crate::manifest_edit::Op::CreateCf {
            name: name.to_string(),
            config: cfg.encode(),
        });
        // The wipe-and-recreate runs *inside* the publish step so the registry
        // never shows a gap: a concurrent `get_column_family` sees either the
        // full old family or the empty new one, which is this method's contract.
        // `ColumnFamily::create` is the one fallible thing here; its error is
        // carried out rather than swallowed, and the durable edit already says
        // the family exists and is empty, so a reopen agrees with the catalog.
        let rebuilt: Mutex<Option<Result<Arc<ColumnFamily>>>> = Mutex::new(None);
        self.inner.catalog_txn(VersionEdit::new(ops), |p| {
            self.inner.unregister_cf(name, p);
            old.close_resources();
            let _ = std::fs::remove_dir_all(self.inner.cf_dir(name));
            let made = ColumnFamily::create(
                self.inner.ctx.clone(),
                name.to_string(),
                self.inner.cf_dir(name),
                cfg,
                cmp,
            );
            if let Ok(cf) = &made {
                self.inner.register_cf(cf, p);
            }
            *rebuilt.lock() = Some(made);
        })?;
        let cf = rebuilt
            .lock()
            .take()
            .expect("the publish step always records its outcome")?;
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

    /// This handle's process-unique database instance id.
    ///
    /// A test lever — it exists so the debug-only commit rendezvous can be
    /// scoped to one database while the test binary runs tests in parallel.
    #[doc(hidden)]
    pub fn instance_id(&self) -> u64 {
        self.inner.instance_id
    }

    /// Format capabilities this database has durably enabled (a mask of
    /// [`KNOWN_CAPS`](crate::format::KNOWN_CAPS) bits); `0` for a database that
    /// has enabled none.
    pub fn format_capabilities(&self) -> u64 {
        self.inner.caps()
    }

    /// Replace the clock periodic compaction (0.3) stamps and measures against.
    ///
    /// A test lever, not a supported knob — hence `doc(hidden)`. It exists
    /// because the whole feature is a *time* trigger: without it every
    /// eligibility test would have to sleep through a real interval, and the
    /// acceptance soak could not be run at all. The clock is read **only** by
    /// periodic stamping and the periodic picker; TTL evaluation, FIFO age
    /// eviction and `SstMeta::max_entry_time` keep reading the real clock, so
    /// injecting here cannot move tier placement or expiry.
    #[doc(hidden)]
    pub fn set_clock_for_tests(&self, clock: crate::util::ClockFn) {
        self.inner.clock.set(clock);
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
        // The final persist's result is the caller's: a silently dropped failure
        // means the next open replays more than it should, or fails outright.
        // Shutdown still runs to completion — resources are released and the
        // directory lock dropped — and the failure is returned at the end.
        let persist = self.inner.persist_manifest();
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
        persist
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

/// `excised` is the 1.2 arm: a flush that published range-delete fragments has
/// just made whole tables droppable by catalog edit alone, and that work is
/// worth waking the worker for even with L0 nowhere near its trigger — a bulk
/// delete followed by an idle database is exactly the case excise exists for,
/// and it produces no capacity pressure of its own.
fn should_schedule_compaction(
    closing: bool,
    fifo: bool,
    l0_len: usize,
    trigger: usize,
    ranges: bool,
) -> bool {
    !closing && (fifo || ranges || l0_len >= trigger)
}

fn schedule_compaction_after_flush(db: &DbInner, cf: &Arc<ColumnFamily>) {
    crate::compaction::refresh_compaction_debt(db, cf);
    let fifo = cf.opts.compaction_style == crate::config::CompactionStyle::Fifo;
    if should_schedule_compaction(
        db.closing.load(Ordering::Relaxed),
        fifo,
        cf.l0_len(),
        cf.opts.l1_file_count_trigger as usize,
        cf.has_range_fragments(),
    ) {
        let _ = db.ctx.compact_tx.send(cf.clone());
    }
}

fn flush_per_cf(db: &Arc<DbInner>, cf: Arc<ColumnFamily>, imm: Arc<ImmMemtable>) {
    match cf.flush_imm(&imm, db.next_file_id()) {
        Ok(out) => {
            // The SST is already synced by `flush_imm` (catalog_txn step 2).
            // Reclaim its WAL only after the edit record's fsync returned `Ok`
            // — AGENTS.md invariant 1. Note what the gate is *not*: a snapshot
            // write. Snapshot compaction is a space optimization and must never
            // be a durability precondition.
            let mut edit = VersionEdit::default();
            if let Some(handle) = &out.table {
                edit.push(crate::manifest_edit::Op::AddTable {
                    cf: cf.name().to_string(),
                    meta: handle.meta.clone(),
                });
            }
            let table = out.table.clone();
            if db
                .catalog_txn(edit, |p| cf.publish_flush(&imm, table, p))
                .is_ok()
            {
                for path in out.wal_paths {
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
    // Every CF slice is written and fsynced first; ONE edit then publishes them
    // all, so a shared WAL is never released against a partially recorded flush.
    let mut all_slices_flushed = true;
    let mut edit = VersionEdit::default();
    let mut staged: Vec<(Arc<ColumnFamily>, Arc<SstHandle>)> = Vec::new();
    // Range tombstones are split by the same cf-id prefix the point entries
    // carry. A family that has spans but no point entry in this memtable still
    // gets a table: `slices` is the union of both splits.
    let mut ranges = crate::unified::split_ranges_by_cf(&imm);
    let mut slices: Vec<(u64, Vec<crate::memtable::Entry>)> = crate::unified::split_by_cf(&imm);
    for (cf_id, _) in &ranges {
        if !slices.iter().any(|(id, _)| id == cf_id) {
            slices.push((*cf_id, Vec::new()));
        }
    }
    for (cf_id, entries) in slices {
        let cf = db.cf_by_id.read().get(&cf_id).cloned();
        let Some(cf) = cf else {
            continue;
        };
        let frags = ranges
            .iter_mut()
            .find(|(id, _)| *id == cf_id)
            .map(|(_, f)| std::mem::take(f))
            .unwrap_or_default();
        match cf.ingest_l0(entries, frags, db.next_file_id()) {
            Ok(Some(handle)) => {
                edit.push(crate::manifest_edit::Op::AddTable {
                    cf: cf.name().to_string(),
                    meta: handle.meta.clone(),
                });
                staged.push((cf.clone(), handle));
            }
            Ok(None) => {}
            Err(error) => {
                db.fail_stop(format!("unified flush failed: {error}"));
                all_slices_flushed = false;
            }
        }
    }
    // A shared WAL covers every CF slice. One failed slice must retain it even
    // if the manifest could record the successful slices; recovery needs the
    // original atomic batch. Only a full flush plus the edit's fsync delete it.
    if all_slices_flushed {
        let published: Vec<Arc<ColumnFamily>> = staged.iter().map(|(cf, _)| cf.clone()).collect();
        let install = staged;
        if db
            .catalog_txn(edit, |p| {
                for (cf, handle) in install {
                    cf.install_handles_l0(vec![handle], p);
                }
            })
            .is_ok()
        {
            for path in &imm.wal_paths {
                crate::wal::remove_wal_files(path);
            }
        }
        for cf in &published {
            schedule_compaction_after_flush(db, cf);
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
    // The periodic-compaction scan (0.3) shares the worker the same way, on its
    // own cadence and its own CAS. `PERIODIC_DISABLED` is the default, so a
    // database that never sets the option pays one relaxed load per tick.
    let mut last_periodic = std::time::Instant::now();
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
        // One periodic scan at a time, for exactly the reason the mover has its
        // own guard: with `num_compaction_threads` workers, every one of them
        // would otherwise walk the same levels and enqueue the same family each
        // interval. The cadence timer is per-worker, the exclusion is DB-wide.
        let check = db.periodic_check.load(Ordering::Relaxed);
        if check != PERIODIC_DISABLED
            && !db.closing.load(Ordering::Relaxed)
            && last_periodic.elapsed() >= Duration::from_nanos(check)
            && db
                .periodic_running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            last_periodic = std::time::Instant::now();
            db.run_periodic_scan();
            db.periodic_running.store(false, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod catalog_txn_tests {
    use super::*;
    use crate::manifest_edit::{recover_catalog, Op};
    use crate::util::fault;
    use std::sync::atomic::AtomicBool;

    /// A database with `CAP_MANIFEST_EDITS` enabled and one column family, so
    /// an `AddTable` edit has somewhere to land.
    fn edits_db(dir: &std::path::Path) -> DB {
        let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
        db.create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.enable_format_capabilities(crate::format::CAP_MANIFEST_EDITS)
            .unwrap();
        db
    }

    fn add_table(id: u64) -> VersionEdit {
        VersionEdit::new(vec![Op::AddTable {
            cf: "default".into(),
            meta: crate::manifest::SstMeta {
                id,
                level: 6,
                max_seq: id,
                ..Default::default()
            },
        }])
    }

    /// The fake sink the A rows publish into: it records whether publication
    /// happened, and when.
    #[derive(Default)]
    struct Sink {
        published: AtomicBool,
    }

    impl Sink {
        fn publish(&self) {
            self.published.store(true, Ordering::SeqCst);
        }
        fn published(&self) -> bool {
            self.published.load(Ordering::SeqCst)
        }
    }

    /// The edit ids the log on disk holds.
    fn log_ids(dir: &std::path::Path) -> Vec<u64> {
        let data = std::fs::read(crate::manifest_edit::edit_log_path(dir)).unwrap();
        crate::manifest_edit::decode_records(&data)
            .unwrap()
            .0
            .iter()
            .map(|r| r.edit_id)
            .collect()
    }

    /// A1/A3 — nothing is published until the record's fsync returns `Ok`. The
    /// injected failure is the fsync itself, so the write went to the page
    /// cache and the record may or may not be on disk: either way the candidate
    /// state stays invisible.
    #[test]
    fn txn_publishes_nothing_before_the_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        let sink = Sink::default();
        fault::fail_nth(fault::Call::Sync, 1);
        let err = db
            .inner
            .catalog_txn(add_table(41), |_| sink.publish())
            .expect_err("a failed fsync must fail the transaction");
        fault::clear();
        assert_eq!(err.kind(), "io", "{err:?}");
        assert!(
            !sink.published(),
            "publication must never precede the durable edit"
        );
    }

    #[test]
    fn txn_publishes_after_a_successful_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        let sink = Sink::default();
        db.inner
            .catalog_txn(add_table(41), |_| sink.publish())
            .unwrap();
        assert!(sink.published());
        assert_eq!(log_ids(dir.path()), vec![1]);
        db.close().unwrap();
    }

    /// A4 — a failed fsync fail-stops the database. Both outcomes are
    /// consistent: the record is either absent (Old) or complete (New), and the
    /// only recovery is a reopen, which reads whichever is on disk.
    #[test]
    fn txn_rolls_back_and_poisons_on_a_sync_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        fault::fail_nth(fault::Call::Sync, 1);
        let _ = db.inner.catalog_txn(add_table(41), |_| {});
        fault::clear();
        assert!(db.poisoned().is_some(), "a durability failure fail-stops");
        let err = db
            .inner
            .catalog_txn(add_table(42), |_| {})
            .expect_err("a poisoned database accepts no further transactions");
        assert!(matches!(err, OndaError::Poisoned(_)), "{err:?}");
    }

    /// A2 — a torn (failed) write leaves the old state visible and the old
    /// catalog on disk, with no record the next replay would apply.
    #[test]
    fn txn_leaves_old_state_visible_on_an_append_failure() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        let sink = Sink::default();
        fault::fail_nth(fault::Call::Write, 1);
        let err = db
            .inner
            .catalog_txn(add_table(41), |_| sink.publish())
            .expect_err("a failed write must fail the transaction");
        fault::clear();
        assert_eq!(err.kind(), "io");
        assert!(!sink.published());
        assert!(log_ids(dir.path()).is_empty(), "nothing reached the log");
        let back = recover_catalog(dir.path()).unwrap();
        assert!(
            back.cfs.iter().all(|cf| cf.sstables.is_empty()),
            "the old catalog is what a reopen sees"
        );
    }

    /// A5 — a crash after the fsync but before publication is a *committed*
    /// transaction: the next open replays the record and publishes it.
    #[test]
    fn txn_state_after_the_fsync_survives_a_crash_before_publish() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        // The empty publish closure *is* the crash: the record is fsynced and
        // the candidate state was never installed.
        db.inner.catalog_txn(add_table(41), |_| {}).unwrap();
        assert_eq!(log_ids(dir.path()), vec![1], "the record is durable");
        // What the next open would read, computed from the files as they stand.
        let back = recover_catalog(dir.path()).unwrap();
        let ids: Vec<u64> = back.cfs[0].sstables.iter().map(|s| s.id).collect();
        assert_eq!(ids, vec![41], "the durable edit is replayed at open");
        assert_eq!(back.applied_through, 1);
        drop(db);
    }

    /// A6 — retirement happens after publication, so a crash in between leaves
    /// files no catalog references (orphans the sweep collects), never a
    /// catalog referencing files that are gone.
    #[test]
    fn txn_retires_handles_only_after_publication() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        let victim = dir.path().join("9999.klog");
        std::fs::write(&victim, b"x").unwrap();
        let order: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
        db.inner
            .catalog_txn(add_table(41), |_| order.lock().push("publish"))
            .unwrap();
        db.inner.remove_sst_file(victim.to_str().unwrap(), 4096);
        order.lock().push("retire");
        assert_eq!(*order.lock(), vec!["publish", "retire"]);
        assert!(!victim.exists());
        db.close().unwrap();
    }

    #[test]
    fn txn_is_a_noop_in_read_only_mode() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = edits_db(dir.path());
            db.close().unwrap();
        }
        let before = std::fs::read(crate::manifest_edit::edit_log_path(dir.path())).unwrap();
        let mut o = Options::new(dir.path().to_str().unwrap());
        o.read_only = true;
        let db = DB::open(o).unwrap();
        let sink = Sink::default();
        db.inner
            .catalog_txn(add_table(41), |_| sink.publish())
            .expect("a read-only transaction is not an error");
        assert!(sink.published(), "the in-memory publication still happens");
        assert_eq!(
            std::fs::read(crate::manifest_edit::edit_log_path(dir.path())).unwrap(),
            before,
            "a read-only database writes nothing"
        );
        db.close().unwrap();
    }

    /// Without the capability the database behaves exactly as it did before
    /// 2.2: no log file, and the whole manifest rewritten.
    #[test]
    fn edits_are_written_only_after_the_capability() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        db.create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.inner.catalog_txn(add_table(41), |_| {}).unwrap();
        assert!(
            !crate::manifest_edit::edit_log_path(dir.path()).exists(),
            "no capability, no log"
        );
        assert_eq!(manifest_version(dir.path()), 1);

        db.enable_format_capabilities(crate::format::CAP_MANIFEST_EDITS)
            .unwrap();
        assert!(
            crate::manifest_edit::edit_log_path(dir.path()).exists(),
            "the capability is durable before the first append, and creates the log"
        );
        assert_eq!(manifest_version(dir.path()), 2);
        db.inner.catalog_txn(add_table(42), |_| {}).unwrap();
        assert_eq!(log_ids(dir.path()), vec![1]);
        db.close().unwrap();
    }

    #[test]
    fn capability_enable_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        let before = db.inner.manifest_persist_count();
        for _ in 0..3 {
            db.enable_format_capabilities(crate::format::CAP_MANIFEST_EDITS)
                .unwrap();
        }
        assert_eq!(
            db.inner.manifest_persist_count(),
            before,
            "re-enabling an active capability persists nothing"
        );
        db.close().unwrap();
    }

    /// A database written before the capability existed opens, upgrades, and
    /// keeps every table it had.
    #[test]
    fn a_pre_capability_database_opens_and_upgrades_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
            let cf = db
                .create_column_family("default", ColumnFamilyConfig::default())
                .unwrap();
            db.put(&cf, b"k", b"v", std::time::Duration::ZERO).unwrap();
            db.flush_memtable(&cf).unwrap();
            db.close().unwrap();
        }
        assert_eq!(manifest_version(dir.path()), 1);
        assert!(!crate::manifest_edit::edit_log_path(dir.path()).exists());
        {
            let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
            let cf = db.get_column_family("default").unwrap();
            assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
            db.enable_format_capabilities(crate::format::CAP_MANIFEST_EDITS)
                .unwrap();
            db.inner.catalog_txn(add_table(4_242), |_| {}).unwrap();
            db.close().unwrap();
        }
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.get_column_family("default").unwrap();
        assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
        assert_eq!(db.format_capabilities(), crate::format::CAP_MANIFEST_EDITS);
        db.close().unwrap();
    }

    /// The trigger runs inside the same critical section that appended, so a
    /// log that crosses the count threshold is compacted away and the snapshot
    /// carries everything.
    #[test]
    fn the_trigger_compacts_the_log_inside_the_append_section() {
        let dir = tempfile::tempdir().unwrap();
        let db = edits_db(dir.path());
        // One oversized edit crosses the byte threshold, which keeps the test
        // to a handful of fsyncs instead of the four thousand the count arm
        // would need.
        let mut fat = add_table(1_000);
        if let Op::AddTable { meta, .. } = &mut fat.ops[0] {
            meta.min_key = vec![7u8; (crate::manifest_edit::SNAPSHOT_MIN_EDIT_BYTES + 1) as usize];
        }
        db.inner.catalog_txn(fat, |_| {}).unwrap();
        assert!(
            log_ids(dir.path()).is_empty(),
            "the trigger fired inside the same section and restarted the log empty"
        );
        let back = recover_catalog(dir.path()).unwrap();
        assert!(back.generation >= 2, "a compaction advanced the generation");
        assert_eq!(back.applied_through, back.next_edit_id - 1);
        db.close().unwrap();
    }

    fn manifest_version(dir: &std::path::Path) -> u32 {
        let bytes = std::fs::read(manifest_path(dir.to_str().unwrap())).unwrap();
        u32::from_le_bytes(bytes[4..8].try_into().unwrap())
    }

    /// Slice 8's last row, which only becomes checkable once the call sites are
    /// migrated: publication is unreachable outside a catalog transaction.
    ///
    /// The guarantee itself is Rust's — `Publish` has a private field, so no
    /// other module can build one, and every primitive demands one by
    /// reference, so a stray publication does not compile. This test guards the
    /// two ways that could be quietly undone: dropping the parameter from a
    /// primitive, or adding a constructor another module can call. It reads the
    /// sources through `include_str!`, so it cannot go stale against a moved
    /// file or a renamed directory.
    #[test]
    fn no_publication_primitive_is_reachable_outside_txn() {
        const CF: &str = include_str!("column_family.rs");
        const DB: &str = include_str!("db.rs");

        /// The signature text of `fn <name>(`, up to the body brace.
        fn signature<'a>(src: &'a str, file: &str, name: &str) -> &'a str {
            let decl = format!("fn {name}(");
            let at = src
                .find(&decl)
                .unwrap_or_else(|| panic!("{name} is no longer declared in {file}"));
            let end = src[at..]
                .find(" {")
                .expect("a function signature ends at its body");
            &src[at..at + end]
        }

        // Every level-set, registry and rule publication demands the token.
        for name in [
            "install_handles_l0",
            "publish_flush",
            "update_levels",
            "install_levels",
            "remove_bottom_tables",
            "insert_bottom_sorted",
            "swap_bottom_tables",
            "remove_l0_tables",
            "append_partition_rule",
            "remove_partition_rule",
        ] {
            let sig = signature(CF, "column_family.rs", name);
            assert!(
                sig.contains("Publish"),
                "{name} publishes catalog state without a Publish token: {sig}"
            );
        }
        for name in [
            "register_cf",
            "unregister_cf",
            "publish_instance_nonce",
            "publish_wal_layout",
        ] {
            let sig = signature(DB, "db.rs", name);
            assert!(
                sig.contains("Publish"),
                "{name} publishes catalog state without a Publish token: {sig}"
            );
        }

        // The token's field stays private, so `Publish` is unconstructible
        // outside this module...
        assert!(
            DB.contains("pub(crate) struct Publish(());"),
            "Publish's field must stay private — a public field makes the token \
             forgeable from any module"
        );
        // ...and no other module names a constructor, directly or by path.
        for (file, src) in [
            ("column_family.rs", CF),
            ("compaction.rs", include_str!("compaction.rs")),
            ("parts.rs", include_str!("parts.rs")),
            ("ingest.rs", include_str!("ingest.rs")),
            ("maintenance.rs", include_str!("maintenance.rs")),
        ] {
            assert!(
                !src.contains("Publish(") && !src.contains("Publish::"),
                "{file} mints a Publish token; only db.rs may, and only in \
                 catalog_txn or the capability-enable snapshot rewrite"
            );
        }
        // Inside db.rs the mint sites are exactly the two documented ones. The
        // needle is assembled rather than written out, so this test's own
        // source does not count as a third one.
        let mint = format!("() -{} Publish {{", ">");
        assert_eq!(
            DB.matches(mint.as_str()).count(),
            2,
            "db.rs must mint the token in exactly two places: after a durable \
             edit, and for the capability-enable snapshot rewrite"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- 0.3: periodic compaction -----------------------------------------

    /// The injected clock must reach periodic age state and NOTHING else. If it
    /// leaked into `max_entry_time`, a test driving periodic time by days would
    /// silently move tier placement (`TierRule::min_age`) under itself.
    #[test]
    fn injected_clock_drives_periodic_eligibility_only() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", Default::default())
            .unwrap();
        db.enable_format_capabilities(crate::format::CAP_PERIODIC_AGE)
            .unwrap();

        // A fixed reading far enough from the real clock that the two can never
        // be confused for one another.
        const FAKE: i64 = 1_000_000_000;
        db.set_clock_for_tests(Arc::new(|| FAKE));
        let before = crate::util::now_nanos();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        let after = crate::util::now_nanos();

        let tables = cf.snapshot_ssts();
        assert_eq!(tables.len(), 1, "one flushed table");
        assert_eq!(
            tables[0].last_compaction_time,
            Some(FAKE),
            "periodic age state comes from the injected clock"
        );
        let entry_time = tables[0]
            .max_entry_time
            .expect("flush output always carries a mover age");
        assert!(
            entry_time >= before && entry_time <= after,
            "max_entry_time must still come from the real clock: {entry_time} \
             outside [{before}, {after}]"
        );
        assert_ne!(entry_time, FAKE);
        drop(cf);
        db.close().unwrap();
    }

    /// Without the capability there is no stamp at all: the manifest may not
    /// carry a field a reopen could not attribute to an enabled capability.
    #[test]
    fn flush_leaves_age_state_unset_without_the_capability() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", Default::default())
            .unwrap();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        assert!(cf
            .snapshot_ssts()
            .iter()
            .all(|t| t.last_compaction_time.is_none()));
        assert_eq!(db.format_capabilities(), 0);
        drop(cf);
        db.close().unwrap();
    }

    /// `num_compaction_threads` workers share one scan. Without this CAS every
    /// one of them would walk the same levels and enqueue the same family each
    /// derived interval — exactly the mistake the part mover's guard prevents.
    #[test]
    fn periodic_running_cas_admits_one_worker() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let inner = db.inner.clone();
        let entered = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(std::sync::Barrier::new(4));
        // Every thread has attempted the CAS before any winner releases, which
        // is what makes this test about exclusion rather than about timing.
        let attempted = Arc::new(std::sync::Barrier::new(4));

        let mut workers = Vec::new();
        for _ in 0..4 {
            let inner = inner.clone();
            let entered = entered.clone();
            let start = start.clone();
            let attempted = attempted.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                let won = inner
                    .periodic_running
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok();
                if won {
                    entered.fetch_add(1, Ordering::SeqCst);
                }
                attempted.wait();
                if won {
                    inner.periodic_running.store(false, Ordering::SeqCst);
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(
            entered.load(Ordering::SeqCst),
            1,
            "exactly one worker may run the pass at a time"
        );
        // And the guard is released, so the next interval can scan again.
        assert!(!inner.periodic_running.load(Ordering::SeqCst));
        db.close().unwrap();
    }

    /// Task 8: with `num_compaction_threads` workers racing the guarded block,
    /// one interval produces ONE enqueue, not one per worker.
    ///
    /// A unit test, because the observation is what `run_periodic_scan` puts on
    /// the compact channel and no integration test can see that. The interval
    /// is an hour, so the derived cadence is fifteen minutes and the real
    /// worker cannot scan underneath the race; eligibility comes from the fake
    /// clock instead.
    #[test]
    fn periodic_scan_enqueues_cf_once_per_interval() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.num_compaction_threads = 4;
        let db = DB::open(opts).unwrap();
        let cf = db
            .create_column_family(
                "default",
                crate::config::ColumnFamilyConfig {
                    periodic_compaction_interval: Duration::from_secs(3600),
                    // One L0 file never trips the capacity trigger, so the only
                    // thing that can enqueue this family is the periodic scan.
                    l1_file_count_trigger: 16,
                    ..Default::default()
                },
            )
            .unwrap();
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(1_000_000_000_000));
        let handle = clock.clone();
        db.set_clock_for_tests(Arc::new(move || handle.load(Ordering::SeqCst)));
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        db.enable_format_capabilities(crate::format::CAP_PERIODIC_AGE)
            .unwrap();
        // Two hours on: the flushed table is an interval past its stamp.
        clock.fetch_add(7_200_000_000_000, Ordering::SeqCst);

        // Four workers reach the guarded block at the same instant, exactly as
        // `compact_worker` runs it.
        let inner = db.inner.clone();
        let enqueued = Arc::new(AtomicUsize::new(0));
        let scans = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(std::sync::Barrier::new(4));
        let attempted = Arc::new(std::sync::Barrier::new(4));
        let mut workers = Vec::new();
        for _ in 0..4 {
            let inner = inner.clone();
            let enqueued = enqueued.clone();
            let scans = scans.clone();
            let start = start.clone();
            let attempted = attempted.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                let won = inner
                    .periodic_running
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok();
                if won {
                    scans.fetch_add(1, Ordering::SeqCst);
                    enqueued.fetch_add(inner.run_periodic_scan(), Ordering::SeqCst);
                }
                attempted.wait();
                if won {
                    inner.periodic_running.store(false, Ordering::SeqCst);
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(scans.load(Ordering::SeqCst), 1, "one scan, not four");
        assert_eq!(
            enqueued.load(Ordering::SeqCst),
            1,
            "one interval enqueues the family once, not once per worker"
        );
        drop(cf);
        db.close().unwrap();
    }

    /// Without the capability the scan enqueues nothing at all, whatever the
    /// interval says: no table can carry a stamp, so nothing is eligible.
    #[test]
    fn periodic_scan_enqueues_nothing_without_the_capability() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family(
                "default",
                crate::config::ColumnFamilyConfig {
                    periodic_compaction_interval: Duration::from_secs(3600),
                    l1_file_count_trigger: 16,
                    ..Default::default()
                },
            )
            .unwrap();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        db.set_clock_for_tests(Arc::new(|| i64::MAX / 2));
        assert_eq!(db.inner.run_periodic_scan(), 0);
        drop(cf);
        db.close().unwrap();
    }

    /// The derived cadence is `interval / 4`, clamped so a seconds-long
    /// interval cannot spin the worker and a month-long one still gets looked
    /// at often enough to bound the reclaim lag.
    #[test]
    fn periodic_check_interval_is_a_clamped_quarter() {
        assert_eq!(
            periodic_check_interval(Duration::from_secs(3600)),
            Duration::from_secs(900)
        );
        assert_eq!(
            periodic_check_interval(Duration::from_secs(600)),
            Duration::from_secs(150)
        );
        // Below the floor.
        assert_eq!(
            periodic_check_interval(Duration::from_secs(2)),
            Duration::from_secs(1)
        );
        assert_eq!(
            periodic_check_interval(Duration::from_millis(1)),
            Duration::from_secs(1)
        );
        // Above the ceiling.
        assert_eq!(
            periodic_check_interval(Duration::from_secs(30 * 24 * 3600)),
            Duration::from_secs(15 * 60)
        );
        assert_eq!(
            periodic_check_interval(Duration::from_secs(4 * 3600)),
            Duration::from_secs(15 * 60)
        );
    }

    /// The default costs one relaxed atomic load per worker tick and nothing
    /// else: no scan, no lock on the CF map.
    #[test]
    fn periodic_scan_is_disabled_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        assert_eq!(
            db.inner.periodic_check.load(Ordering::Relaxed),
            PERIODIC_DISABLED
        );
        let cf = db
            .create_column_family(
                "aged",
                crate::config::ColumnFamilyConfig {
                    periodic_compaction_interval: Duration::from_secs(600),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            db.inner.periodic_check.load(Ordering::Relaxed),
            Duration::from_secs(150).as_nanos() as u64,
            "creating a family arms the scan at its derived cadence"
        );
        drop(cf);
        db.close().unwrap();
    }

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
        assert!(should_schedule_compaction(false, true, 0, 4, false));
        assert!(!should_schedule_compaction(false, false, 3, 4, false));
        // A flush that published fragments wakes the worker for the excise
        // pre-pass even with L0 below its trigger (1.2).
        assert!(should_schedule_compaction(false, false, 3, 4, true));
        assert!(should_schedule_compaction(false, false, 4, 4, false));
        assert!(!should_schedule_compaction(true, true, 8, 4, true));
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
