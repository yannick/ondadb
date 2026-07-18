//! Regression tests for failure modes documented in the public bug trackers
//! of comparable production LSM engines (same architecture as ondaDB:
//! WAL/journal, memtables, leveled compaction, KV separation, MVCC
//! snapshots). Every scenario here caused real data loss, corruption, a
//! panic, or a hang in an engine of this shape — worth pinning down before
//! we hit it too.
//!
//! Covered failure classes:
//! - WAL recovery: lost committed writes after crash+reopen, length-field
//!   boundary values, corrupt/torn frames, batch atomicity across replay.
//! - CF lifecycle: clear/drop + crash resurrecting deleted data, concurrent
//!   clear vs ingestion, recreating a CF while stale handles live.
//! - Recovery visibility: recovered/ingested data invisible to transactions,
//!   permanent write stalls after recovery with a WAL backlog.
//! - Error paths: failed operations poisoning locks or losing later writes.
//! - Iterators/snapshots vs background work: iterate-during-flush/compaction
//!   deadlocks, snapshot reads going dirty under destructive compaction.
//! - Key semantics: prefix-confusion in point lookups (extra relevant here:
//!   ondaDB's merge paths use an 8-byte key-prefix compare shortcut),
//!   iterator bound handling with composite binary keys.
//! - Open robustness: stray files (.DS_Store etc.) in the data dir, per-CF
//!   config/comparator persistence across reopen.
//! - Shutdown: close() deadlocking under write pressure.
//! - Tombstone hygiene: fully-deleted CFs keeping tombstone debris forever.
//!
//! One companion test lives in-module (`poisoned_txn_commit_does_not_publish`
//! in src/db.rs) because it needs the crate-private poison handle.
//!
//! Deliberately not covered (and why):
//! - Network/exotic filesystem quirks (NFS, Btrfs) — not testable locally.
//! - Performance stalls — perf assertions are banned in this suite
//!   (thermally noisy machine, see docs/performance.md).
//! - PWD-dependent paths — chdir in a threaded test binary is flaky by
//!   construction.
//! - Idle-CF-pins-shared-journal: ondaDB WALs are per-CF, so the failure
//!   mode doesn't exist (unified-memtable mode shares a WAL and would be the
//!   analog; follow-up).
//! - FIFO snapshot dirty reads — ondaDB documents FIFO as cache semantics:
//!   "old data disappears by design, including from live snapshots"
//!   (`CompactionStyle::Fifo`). The leveled variant is tested.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, DB};

const ZERO: Duration = Duration::ZERO;

fn open(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = match db.get_column_family("default") {
        Some(cf) => cf,
        None => db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap(),
    };
    (db, cf)
}

/// Tiny buffers so flushes/rotations happen constantly (same knobs as
/// `concurrent_manifest_writes_survive_reopen` in tests/db.rs).
fn small_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        write_buffer_size: 32 * 1024,
        l1_file_count_trigger: 2,
        ..ColumnFamilyConfig::default()
    }
}

/// Deterministic value bytes so the parent process can verify content the
/// child wrote without sharing state.
fn val(i: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|j| (i as usize).wrapping_mul(31).wrapping_add(j * 7 + 13) as u8)
        .collect()
}

/// Deadlock guard: aborts the whole process if `done` isn't set within
/// `secs`. Abort (not panic) is deliberate — a deadlocked test would
/// otherwise hang the suite forever, and no in-process unwind can escape a
/// stuck lock. The guard disarms on drop, so ordinary assert failures are
/// unaffected.
fn watchdog(name: &'static str, secs: u64) -> impl Drop {
    struct Disarm(Arc<AtomicBool>);
    impl Drop for Disarm {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let done = Arc::new(AtomicBool::new(false));
    let d = done.clone();
    std::thread::spawn(move || {
        for _ in 0..secs * 10 {
            std::thread::sleep(Duration::from_millis(100));
            if d.load(Ordering::SeqCst) {
                return;
            }
        }
        eprintln!("watchdog: `{name}` did not finish within {secs}s — aborting (deadlock)");
        std::process::abort();
    });
    Disarm(done)
}

fn count_keys(db: &DB, cf: &Arc<ColumnFamily>) -> usize {
    let txn = db.begin();
    let mut it = txn.new_iterator(cf);
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        n += 1;
        it.next();
    }
    assert!(it.err().is_none(), "iteration error: {:?}", it.err());
    n
}

// --------------------------------------------------------------- crash helper
//
// A child process writes committed data and exits WITHOUT closing (process
// exit skips Drop), leaving live WAL files behind — the LOCK file makes
// in-process crash simulation impossible by design. `CRASH_MODE` selects the
// workload. Same pattern as tests/surrealkv_suite.rs.

const CRASH_DIR_ENV: &str = "ONDA_REGR_CRASH_DIR";
const CRASH_MODE_ENV: &str = "ONDA_REGR_CRASH_MODE";

const MULTI_CF_LIKES: u64 = 6000;
const MULTI_CF_UNLIKES: u64 = 1500;
const PAIR_COUNT: u64 = 300;
const BACKLOG_KEYS: u64 = 4000;

/// Not a real test: the child half of the crash simulation. Runs only when
/// the env vars are set; otherwise it's a no-op in normal suite runs.
#[test]
fn crash_helper() {
    let (Ok(dir), Ok(mode)) = (std::env::var(CRASH_DIR_ENV), std::env::var(CRASH_MODE_ENV)) else {
        return;
    };
    let dir = std::path::Path::new(&dir);
    match mode.as_str() {
        // Lost-writes shape: two CFs, sustained writes, flushes racing the WAL.
        "multi_cf" => {
            let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
            let likes = db.create_column_family("likes", small_cfg()).unwrap();
            let unlikes = db.create_column_family("unlikes", small_cfg()).unwrap();
            for i in 0..MULTI_CF_LIKES {
                db.put(&likes, format!("k{i:05}").as_bytes(), &val(i, 64), ZERO)
                    .unwrap();
                if i % 4 == 0 && i / 4 < MULTI_CF_UNLIKES {
                    let j = i / 4;
                    db.put(&unlikes, format!("u{j:05}").as_bytes(), &val(j, 64), ZERO)
                        .unwrap();
                }
            }
            db.sync_wal().unwrap();
        }
        // One frame per committed batch; pairs must recover
        // both-or-neither even after tail/middle corruption.
        "pairs" => {
            let (db, cf) = open(dir);
            for i in 0..PAIR_COUNT {
                let mut t = db.begin();
                t.put(&cf, format!("a{i:04}").as_bytes(), &val(i, 64), ZERO)
                    .unwrap();
                t.put(&cf, format!("b{i:04}").as_bytes(), &val(i, 64), ZERO)
                    .unwrap();
                t.commit().unwrap();
            }
            db.sync_wal().unwrap();
        }
        // Sizes around a u16 length-field limit, plus
        // a multi-MB value and an oversized key — all recovered from WAL only
        // (nothing is flushed before the crash).
        "boundary" => {
            let (db, cf) = open(dir);
            for (name, len) in [
                ("v64k-1", 65535usize),
                ("v64k", 65536),
                ("v64k+1", 65537),
                ("v4m", 4 << 20),
            ] {
                db.put(&cf, name.as_bytes(), &val(len as u64, len), ZERO)
                    .unwrap();
            }
            let big_key = val(7, 70_000);
            db.put(&cf, &big_key, b"big-key-value", ZERO).unwrap();
            db.sync_wal().unwrap();
        }
        // Clear, then crash before any flush.
        "clear" => {
            let (db, cf) = open(dir);
            for i in 0..500u64 {
                db.put(&cf, format!("old{i:04}").as_bytes(), &val(i, 32), ZERO)
                    .unwrap();
            }
            db.sync_wal().unwrap();
            let cf2 = db.clear_column_family("default").unwrap();
            drop(cf);
            for i in 0..100u64 {
                db.put(&cf2, format!("new{i:04}").as_bytes(), &val(i, 32), ZERO)
                    .unwrap();
            }
            db.sync_wal().unwrap();
        }
        // Die with flushes mid-flight and a WAL backlog.
        "backlog" => {
            let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
            let cfg = ColumnFamilyConfig {
                l0_queue_stall_threshold: 4,
                ..small_cfg()
            };
            let cf = db.create_column_family("default", cfg).unwrap();
            for i in 0..BACKLOG_KEYS {
                db.put(&cf, format!("k{i:05}").as_bytes(), &val(i, 200), ZERO)
                    .unwrap();
            }
            db.sync_wal().unwrap();
        }
        other => panic!("unknown crash mode {other}"),
    }
    // Simulated crash: no close(), no Drop.
    std::process::exit(0);
}

fn run_crash(dir: &std::path::Path, mode: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_helper", "--exact", "--nocapture"])
        .env(CRASH_DIR_ENV, dir.to_str().unwrap())
        .env(CRASH_MODE_ENV, mode)
        .status()
        .expect("spawn crash helper");
    assert!(status.success(), "crash helper child failed");
}

fn wal_files(dir: &std::path::Path, cf: &str) -> Vec<std::path::PathBuf> {
    let cf_dir = dir.join(format!("cf-{cf}"));
    let mut out = Vec::new();
    for e in std::fs::read_dir(cf_dir).unwrap().flatten() {
        if e.file_name().to_string_lossy().starts_with("wal-") {
            out.push(e.path());
        }
    }
    out.sort();
    assert!(!out.is_empty(), "crash left no WAL files behind");
    out
}

// ------------------------------------------------------- durability / recovery

/// A comparable engine lost committed writes after ~5M unique keys across
/// two partitions: journal truncation on recovery discarded valid frames.
/// Scaled down, but the same shape: two CFs, constant flush/WAL
/// churn from tiny buffers, then a crash and an exact-count recount.
#[test]
fn multi_cf_crash_reopen_loses_no_committed_writes() {
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path(), "multi_cf");

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let likes = db.get_column_family("likes").expect("likes CF recovered");
    let unlikes = db
        .get_column_family("unlikes")
        .expect("unlikes CF recovered");

    assert_eq!(
        count_keys(&db, &likes) as u64,
        MULTI_CF_LIKES,
        "likes lost committed writes after crash+reopen"
    );
    assert_eq!(
        count_keys(&db, &unlikes) as u64,
        MULTI_CF_UNLIKES,
        "unlikes lost committed writes after crash+reopen"
    );
    // Spot-check content, not just counts.
    for i in (0..MULTI_CF_LIKES).step_by(997) {
        assert_eq!(
            db.get(&likes, format!("k{i:05}").as_bytes()).unwrap(),
            val(i, 64)
        );
    }
    for j in (0..MULTI_CF_UNLIKES).step_by(211) {
        assert_eq!(
            db.get(&unlikes, format!("u{j:05}").as_bytes()).unwrap(),
            val(j, 64)
        );
    }
    db.close().unwrap();
}

/// A comparable engine encoded journal value lengths as u16 while its API
/// allowed 2^32, so any value >64 KiB silently corrupted the journal and
/// made the database unopenable. Pin ondaDB's WAL length encoding at exactly that
/// boundary (and well past it), recovered purely from the WAL.
#[test]
fn wal_boundary_sized_values_survive_crash_reopen() {
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path(), "boundary");

    let (db, cf) = open(dir.path());
    for (name, len) in [
        ("v64k-1", 65535usize),
        ("v64k", 65536),
        ("v64k+1", 65537),
        ("v4m", 4 << 20),
    ] {
        let got = db.get(&cf, name.as_bytes()).unwrap();
        assert_eq!(got.len(), len, "{name}: wrong length after WAL replay");
        assert_eq!(got, val(len as u64, len), "{name}: bytes mangled");
    }
    assert_eq!(db.get(&cf, &val(7, 70_000)).unwrap(), b"big-key-value");
    db.close().unwrap();
}

/// Recovery must
/// never panic, must keep every intact committed frame, and must never
/// surface half a batch. Three corruptions at once, in different places:
/// a plausible-but-bad frame appended to every WAL file, a garbage file with
/// a valid WAL name, and a bit-flip in the middle of the biggest WAL.
#[test]
fn wal_corruption_variants_never_panic_and_keep_batches_atomic() {
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path(), "pairs");

    // (a) Append a well-formed frame header with a wrong CRC to every file.
    for wal in wal_files(dir.path(), "default") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(wal).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(&16u32.to_le_bytes()); // payload_len
        frame.extend_from_slice(&0xdead_beefu32.to_le_bytes()); // bogus crc
        frame.extend_from_slice(&[0xab; 16]);
        f.write_all(&frame).unwrap();
    }
    // (b) A parseable WAL name holding pure garbage.
    std::fs::write(
        dir.path().join("cf-default").join("wal-99.log"),
        val(99, 3000),
    )
    .unwrap();
    // (c) Flip bytes a third of the way into the largest WAL file.
    let biggest = wal_files(dir.path(), "default")
        .into_iter()
        .max_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .unwrap();
    let mut bytes = std::fs::read(&biggest).unwrap();
    let at = bytes.len() / 3;
    let end = (at + 8).min(bytes.len());
    for b in &mut bytes[at..end] {
        *b ^= 0xff;
    }
    std::fs::write(&biggest, bytes).unwrap();

    // Open must succeed (truncate-at-corruption semantics), not panic.
    let (db, cf) = open(dir.path());
    let mut survivors = 0;
    for i in 0..PAIR_COUNT {
        let a = db.get(&cf, format!("a{i:04}").as_bytes());
        let b = db.get(&cf, format!("b{i:04}").as_bytes());
        match (a, b) {
            (Ok(va), Ok(vb)) => {
                assert_eq!(va, val(i, 64));
                assert_eq!(vb, val(i, 64));
                survivors += 1;
            }
            (Err(OndaError::NotFound), Err(OndaError::NotFound)) => {}
            (a, b) => panic!(
                "batch {i} half-applied after WAL corruption: a={:?} b={:?}",
                a.map(|v| v.len()),
                b.map(|v| v.len())
            ),
        }
    }
    assert!(survivors > 0, "corruption must not wipe every intact frame");
    // The store must still be writable after a damaged recovery.
    db.put(&cf, b"after-corruption", b"1", ZERO).unwrap();
    db.close().unwrap();
}

/// A comparable engine's `clear()` reset the on-disk tree but wrote no
/// journal marker, so recovery replayed all pre-clear entries — deleted data
/// came back after a restart. Clear, write new keys, crash, reopen: only post-clear keys may
/// exist.
#[test]
fn clear_then_crash_does_not_resurrect_cleared_data() {
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path(), "clear");

    let (db, cf) = open(dir.path());
    for i in 0..500u64 {
        match db.get(&cf, format!("old{i:04}").as_bytes()) {
            Err(OndaError::NotFound) => {}
            other => panic!("cleared key old{i:04} resurrected after crash: {other:?}"),
        }
    }
    for i in 0..100u64 {
        assert_eq!(
            db.get(&cf, format!("new{i:04}").as_bytes()).unwrap(),
            val(i, 32),
            "post-clear write new{i:04} lost"
        );
    }
    assert_eq!(count_keys(&db, &cf), 100);
    db.close().unwrap();
}

// --------------------------------------------------------- CF lifecycle races

/// Concurrent `.clear()` and the ingestion API crashed a comparable engine's
/// worker ("invalid table IDs") and left it leaking thousands of versions.
/// Stale-handle errors are fine; panics, poisoning, or a store that won't
/// reopen are not.
#[test]
fn concurrent_clear_and_ingestion_do_not_corrupt() {
    let _wd = watchdog("concurrent_clear_and_ingestion_do_not_corrupt", 300);
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.create_column_family("ing", small_cfg()).unwrap();

    std::thread::scope(|s| {
        s.spawn(|| {
            for _ in 0..30 {
                let _ = db.clear_column_family("ing"); // racing errors are OK
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        s.spawn(|| {
            for round in 0..30u64 {
                let Some(cf) = db.get_column_family("ing") else {
                    continue;
                };
                // Any step may fail against a just-cleared handle; that's the
                // documented stale-handle contract. It must not panic.
                let Ok(mut ing) = db.start_ingestion(&cf) else {
                    continue;
                };
                let mut ok = true;
                for i in 0..300u64 {
                    let key = format!("r{round:03}-k{i:05}");
                    if ing.write(key.as_bytes(), &val(i, 128), ZERO).is_err() {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    let _ = ing.finish();
                }
            }
        });
    });

    assert_eq!(
        db.poisoned(),
        None,
        "clear/ingestion race must not fail-stop the database"
    );
    // The store must survive the race: usable now and after reopen.
    let cf = db.get_column_family("ing").unwrap();
    db.put(&cf, b"sentinel", b"1", ZERO).unwrap();
    db.close().unwrap();
    drop(cf);
    drop(db);

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("ing").expect("CF must survive");
    let _ = count_keys(&db, &cf); // full scan must not error
    assert_eq!(db.get(&cf, b"sentinel").unwrap(), b"1");
    db.close().unwrap();
}

/// A comparable engine removed a deleted partition's folder only when the
/// last old handle dropped — deleting the *recreated* partition's files out
/// from under it. Recreate a CF while a stale handle is still alive,
/// then drop the stale handle and verify the new CF's data survives.
#[test]
fn drop_and_recreate_cf_with_live_stale_handle() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();

    let old = db.create_column_family("d", small_cfg()).unwrap();
    for i in 0..10u64 {
        db.put(&old, format!("old{i}").as_bytes(), b"x", ZERO)
            .unwrap();
    }
    db.flush_memtable(&old).unwrap();

    db.drop_column_family("d").unwrap(); // `old` is now a stale live handle
    let new = db.create_column_family("d", small_cfg()).unwrap();
    for i in 0..20u64 {
        db.put(&new, format!("new{i:02}").as_bytes(), &val(i, 32), ZERO)
            .unwrap();
    }
    db.flush_memtable(&new).unwrap();

    drop(old); // upstream, deferred cleanup nuked the new files right here

    for i in 0..20u64 {
        assert_eq!(
            db.get(&new, format!("new{i:02}").as_bytes()).unwrap(),
            val(i, 32),
            "stale handle drop damaged the recreated CF"
        );
    }
    for i in 0..10u64 {
        assert!(matches!(
            db.get(&new, format!("old{i}").as_bytes()),
            Err(OndaError::NotFound)
        ));
    }
    db.close().unwrap();
    drop(new);
    drop(db);

    // And the recreated CF must still be intact after a reopen.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("d").unwrap();
    assert_eq!(count_keys(&db, &cf), 20);
    assert_eq!(db.get(&cf, b"new07").unwrap(), val(7, 32));
    db.close().unwrap();
}

// ------------------------------------------------- recovery visibility / stall

/// In a comparable engine, ingested data was visible to plain reads after
/// reopen but invisible inside transactions — the recovered visible sequence
/// didn't cover the ingestion. Check all three persistence paths (WAL-only,
/// flushed, ingested) through a snapshot transaction after reopen.
#[test]
fn recovered_data_visible_in_snapshot_txn_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let walonly = db
            .create_column_family("walonly", ColumnFamilyConfig::default())
            .unwrap();
        let flushed = db
            .create_column_family("flushed", ColumnFamilyConfig::default())
            .unwrap();
        let ingested = db
            .create_column_family("ingested", ColumnFamilyConfig::default())
            .unwrap();

        for i in 0..50u64 {
            db.put(&walonly, format!("w{i:03}").as_bytes(), &val(i, 32), ZERO)
                .unwrap();
            db.put(&flushed, format!("f{i:03}").as_bytes(), &val(i, 32), ZERO)
                .unwrap();
        }
        db.flush_memtable(&flushed).unwrap();
        let mut ing = db.start_ingestion(&ingested).unwrap();
        for i in 0..50u64 {
            ing.write(format!("i{i:03}").as_bytes(), &val(i, 32), ZERO)
                .unwrap();
        }
        ing.finish().unwrap();
        db.close().unwrap();
    }

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let walonly = db.get_column_family("walonly").unwrap();
    let flushed = db.get_column_family("flushed").unwrap();
    let ingested = db.get_column_family("ingested").unwrap();

    // The upstream bug fired specifically for *transactional* reads: the
    // txn's pinned read_seq sat below the recovered data's sequence.
    let mut t = db.begin();
    for i in 0..50u64 {
        assert_eq!(
            t.get(&walonly, format!("w{i:03}").as_bytes()).unwrap(),
            val(i, 32),
            "WAL-recovered key invisible in snapshot txn"
        );
        assert_eq!(
            t.get(&flushed, format!("f{i:03}").as_bytes()).unwrap(),
            val(i, 32),
            "flushed key invisible in snapshot txn"
        );
        assert_eq!(
            t.get(&ingested, format!("i{i:03}").as_bytes()).unwrap(),
            val(i, 32),
            "ingested key invisible in snapshot txn"
        );
    }
    t.rollback().unwrap();
    assert_eq!(count_keys(&db, &ingested), 50);
    db.close().unwrap();
}

/// Killing a comparable engine mid-write left sealed memtables that were
/// recovered but never queued for flush — the next writes stalled forever on
/// "4+ sealed memtables queued up". After a crash with a WAL backlog and
/// flushes mid-flight, new writes must make progress in bounded time and a
/// forced flush must reclaim the recovered WAL generations.
#[test]
fn writes_progress_after_crash_recovery_with_wal_backlog() {
    let _wd = watchdog("writes_progress_after_crash_recovery_with_wal_backlog", 300);
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path(), "backlog");

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();

    // The stall-prone path: keep writing against the tiny recovered config
    // (write_buffer 32K, stall threshold 4) so rotations and the flush queue
    // are exercised immediately after recovery.
    for i in 0..5000u64 {
        db.put(&cf, format!("post{i:05}").as_bytes(), &val(i, 200), ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();

    // Nothing lost on either side of the crash.
    assert_eq!(count_keys(&db, &cf) as u64, BACKLOG_KEYS + 5000);
    // Recovered WAL generations must be reclaimed once their data is flushed
    // (pending_wals drained); only the current generation's files may remain.
    let live = wal_files(dir.path(), "default").len();
    assert!(
        live <= 4,
        "expected only the active WAL generation (≤4 stripe files) after \
         post-recovery flush, found {live}"
    );
    db.close().unwrap();
}

// ------------------------------------------------------ error-path resilience

/// In a comparable engine, a failed insert (invalid key) poisoned an
/// internal lock and the process aborted in the destructor. Every cheap error path here must
/// leave the DB fully usable, droppable, and reopenable.
#[test]
fn error_paths_leave_db_usable_and_reopenable() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    // Invalid CF names.
    assert!(matches!(
        db.create_column_family("", ColumnFamilyConfig::default()),
        Err(OndaError::InvalidArgs(_))
    ));
    let long = "x".repeat(300);
    assert!(matches!(
        db.create_column_family(&long, ColumnFamilyConfig::default()),
        Err(OndaError::InvalidArgs(_))
    ));
    // Duplicate CF.
    assert!(matches!(
        db.create_column_family("default", ColumnFamilyConfig::default()),
        Err(OndaError::Exists(_))
    ));
    // Unknown comparator.
    assert!(db
        .create_column_family(
            "badcmp",
            ColumnFamilyConfig {
                comparator_name: "no-such-comparator".into(),
                ..ColumnFamilyConfig::default()
            },
        )
        .is_err());
    // Missing key.
    assert!(matches!(db.get(&cf, b"nope"), Err(OndaError::NotFound)));
    // Write-write conflict abort.
    db.put(&cf, b"c", b"0", ZERO).unwrap();
    let mut t1 = db.begin();
    let mut t2 = db.begin();
    t1.put(&cf, b"c", b"1", ZERO).unwrap();
    t2.put(&cf, b"c", b"2", ZERO).unwrap();
    t1.commit().unwrap();
    assert!(matches!(t2.commit(), Err(OndaError::Conflict(_))));
    // Writes through a stale handle after clear() must fail, not panic.
    let stale = cf;
    let fresh = db.clear_column_family("default").unwrap();
    assert!(
        db.put(&stale, b"k", b"v", ZERO).is_err(),
        "write through a stale post-clear handle must fail"
    );
    drop(stale);

    // After all of the above the DB must still work, close cleanly (the
    // upstream abort fired in the destructor), and reopen.
    db.put(&fresh, b"alive", b"1", ZERO).unwrap();
    assert_eq!(db.get(&fresh, b"alive").unwrap(), b"1");
    assert_eq!(db.poisoned(), None);
    db.close().unwrap();
    drop(fresh);
    drop(db);

    let (db, cf) = open(dir.path());
    assert_eq!(db.get(&cf, b"alive").unwrap(), b"1");
    db.close().unwrap();
}

/// Found while writing this suite (the "commit error handled wrong" class):
/// a failed commit — e.g. a put through a dropped-CF handle —
/// reserved sequence numbers but returned before publishing them, leaving a
/// permanent hole in the gap-free publish cursor. `visible_seq` froze there
/// forever: other threads stopped seeing every later commit, the manifest
/// persisted the stalled watermark, and flushed data above the hole was
/// invisible (and its sequences reused) after reopen. This pins the fix.
#[test]
fn failed_commit_must_not_stall_visibility_or_lose_later_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    let dead = db
        .create_column_family("dead", ColumnFamilyConfig::default())
        .unwrap();
    db.drop_column_family("dead").unwrap();
    // This put reserves a sequence and then fails (stale handle, WAL gone).
    assert!(db.put(&dead, b"k", b"v", ZERO).is_err());
    drop(dead);

    db.put(&cf, b"alive", b"1", ZERO).unwrap();
    // Cross-thread visibility: the writing thread's read-your-own-writes
    // floor masks a stalled cursor, so the probe must come from another
    // thread.
    let ok = std::thread::scope(|s| s.spawn(|| db.get(&cf, b"alive").is_ok()).join().unwrap());
    assert!(
        ok,
        "commit after a failed commit is invisible to other threads \
         (publish cursor stalled)"
    );
    db.close().unwrap();
    drop(cf);
    drop(db);

    let (db, cf) = open(dir.path());
    assert_eq!(
        db.get(&cf, b"alive").unwrap(),
        b"1",
        "committed write lost across reopen after an earlier failed commit"
    );
    // And sequences must not be reused: a fresh write must supersede.
    db.put(&cf, b"alive", b"2", ZERO).unwrap();
    assert_eq!(db.get(&cf, b"alive").unwrap(), b"2");
    db.close().unwrap();
}

// ------------------------------------- iterators / snapshots vs background work

/// A comparable engine deadlocked when iterating while inserting, once the
/// memtable needed rotating (the iterator held read locks the rotation
/// needed). An open
/// snapshot iterator must survive concurrent rotations, flushes, compaction
/// (including SSTable unlinks), and still see exactly its snapshot.
#[test]
fn iterate_while_flush_and_compaction_run() {
    let _wd = watchdog("iterate_while_flush_and_compaction_run", 300);
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", small_cfg()).unwrap();

    for i in 0..3000u64 {
        db.put(&cf, format!("k{i:05}").as_bytes(), b"old", ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap(); // snapshot spans SSTs + memtable

    let t = db.begin(); // pins the snapshot
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();

    let mut seen = 0u64;
    std::thread::scope(|s| {
        s.spawn(|| {
            // Overwrite every key and add as many new ones: with 32K buffers
            // this forces many rotations + flushes, then a full compaction
            // that unlinks tables the iterator may still hold open.
            for i in 0..3000u64 {
                db.put(&cf, format!("k{i:05}").as_bytes(), b"new", ZERO)
                    .unwrap();
                db.put(&cf, format!("z{i:05}").as_bytes(), b"new", ZERO)
                    .unwrap();
            }
            db.compact(&cf).unwrap();
        });
        while it.valid() {
            assert!(
                it.key().starts_with(b"k"),
                "post-snapshot key leaked into the scan: {:?}",
                String::from_utf8_lossy(it.key())
            );
            assert_eq!(
                it.value(),
                b"old",
                "post-snapshot value leaked at {:?}",
                String::from_utf8_lossy(it.key())
            );
            seen += 1;
            // Give the writer real time to rotate/flush/compact mid-scan.
            if seen.is_multiple_of(256) {
                std::thread::sleep(Duration::from_millis(2));
            }
            it.next();
        }
        assert!(it.err().is_none(), "iteration error: {:?}", it.err());
    });
    assert_eq!(seen, 3000, "snapshot scan must see exactly its snapshot");
    drop(it);
    drop(t);
    db.close().unwrap();
}

/// In a comparable engine, snapshots didn't pin a tree version, so a
/// destructive compaction made snapshot reads dirty/non-repeatable. Re-reading the same
/// key inside one snapshot transaction must return the same value no matter
/// how many overwrites, flushes, and compactions land in between.
#[test]
fn snapshot_repeatable_read_during_destructive_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", small_cfg()).unwrap();

    db.put(&cf, b"K", b"v0", ZERO).unwrap();
    for i in 0..200u64 {
        db.put(&cf, format!("fill{i:04}").as_bytes(), &val(i, 256), ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();

    let mut t = db.begin();
    assert_eq!(t.get(&cf, b"K").unwrap(), b"v0");

    for round in 0..5u64 {
        db.put(&cf, b"K", format!("v{}", round + 1).as_bytes(), ZERO)
            .unwrap();
        for i in 0..200u64 {
            db.put(
                &cf,
                format!("fill{i:04}").as_bytes(),
                &val(i + round, 256),
                ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        assert_eq!(
            t.get(&cf, b"K").unwrap(),
            b"v0",
            "snapshot read went dirty after compaction round {round}"
        );
    }
    // Iterator through the same snapshot agrees.
    let mut it = t.new_iterator(&cf);
    it.seek(b"K");
    assert!(it.valid());
    assert_eq!(it.key(), b"K");
    assert_eq!(it.value(), b"v0");
    drop(it);
    t.rollback().unwrap();

    // Outside the snapshot, the newest value wins.
    assert_eq!(db.get(&cf, b"K").unwrap(), b"v5");
    db.close().unwrap();
}

// ------------------------------------------------------- key/bound semantics

/// A comparable engine's `get("hello-key-99999")` returned the value stored
/// under "hello-key-999991" — a prefix-confusion bug in point lookup. ondaDB's
/// merge paths additionally use an 8-byte key-prefix compare shortcut
/// (invariant 7 in AGENTS.md), so hammer exactly the inputs where a broken
/// shortcut would surface: keys sharing a full 8-byte window, keys that are
/// prefixes of other keys, zero-padding collisions, embedded NULs.
#[test]
fn prefix_adversarial_point_and_range_lookups() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    let keys: Vec<&[u8]> = vec![
        b"",
        b"\0",
        b"\0\0",
        b"a",
        b"a\0", // key_prefix8 zero-pads: collides with "a" in the u64 window
        b"a\0b",
        b"aaaaaaa",   // 7 a's
        b"aaaaaaaa",  // 8 a's — full window
        b"aaaaaaaab", // differs from "aaaaaaaa" only past the window
        b"aaaaaaab",
        b"hello-key-9999",
        b"hello-key-99999",
        b"hello-key-999991", // the upstream prefix-confusion pair
        b"hello-key-999992",
        b"12345678",
        b"123456789",
    ];
    // Split across an SSTable and the memtable so every lookup exercises the
    // merge path, not just one source.
    for (n, k) in keys.iter().enumerate() {
        if n % 2 == 0 {
            db.put(&cf, k, format!("val-{n}").as_bytes(), ZERO).unwrap();
        }
    }
    db.flush_memtable(&cf).unwrap();
    for (n, k) in keys.iter().enumerate() {
        if n % 2 == 1 {
            db.put(&cf, k, format!("val-{n}").as_bytes(), ZERO).unwrap();
        }
    }

    for (n, k) in keys.iter().enumerate() {
        assert_eq!(
            db.get(&cf, k).unwrap(),
            format!("val-{n}").as_bytes(),
            "wrong value for key {:?}",
            String::from_utf8_lossy(k)
        );
    }
    // Near misses — prefixes, extensions, and window-sharing neighbors of
    // stored keys — must be NotFound, never a prefix match.
    let misses: Vec<&[u8]> = vec![
        b"aa",
        b"aaaaaaaaa", // 9 a's
        b"a\0a",
        b"\0\0\0",
        b"b",
        b"hello-key",
        b"hello-key-999",
        b"hello-key-9999911",
        b"1234567",
        b"123456780",
    ];
    for k in &misses {
        assert!(
            matches!(db.get(&cf, k), Err(OndaError::NotFound)),
            "near-miss {:?} matched a stored key by prefix",
            String::from_utf8_lossy(k)
        );
    }
    // Full iteration must be exactly the bytewise-sorted key set.
    let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
    expected.sort();
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut got = Vec::new();
    while it.valid() {
        got.push(it.key().to_vec());
        it.next();
    }
    assert_eq!(got, expected, "iteration order broken on adversarial keys");
    drop(it);

    // Prefix scan over the shared-window group must return exactly its
    // members and stop at the first non-member.
    let mut it = txn.new_iterator_bounded(
        &cf,
        std::ops::Bound::Included(b"aaaaaaaa".as_slice()),
        std::ops::Bound::Unbounded,
    );
    it.seek_to_first();
    let mut scan = Vec::new();
    while it.valid() && it.key().starts_with(b"aaaaaaaa") {
        scan.push(it.key().to_vec());
        it.next();
    }
    assert_eq!(scan, vec![b"aaaaaaaa".to_vec(), b"aaaaaaaab".to_vec()]);
    drop(it);
    db.close().unwrap();
}

/// A user's inclusive range over composite `u64_be ++ suffix` keys silently
/// dropped the last key in a comparable engine. That case was user error (a bare
/// 8-byte inclusive upper bound sorts *before* any key extending it), but it
/// is exactly the kind of boundary semantics that must never drift. Pin all
/// four bound combinations, forward and backward.
#[test]
fn bounded_iterator_composite_key_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    let make = |ts: u64, suffix: &str| {
        let mut k = ts.to_be_bytes().to_vec();
        k.extend_from_slice(suffix.as_bytes());
        k
    };
    let k1 = make(1777062639, "C9KKYjtze5xMm4PKh8ixW5");
    let k2 = make(1777062640, "4RTTd706VuXGWksAe3lxlj");
    let k3 = make(1777062641, "AloW7vWFWpOTmiKXfu204F");
    db.put(&cf, &k1, b"1", ZERO).unwrap();
    db.put(&cf, &k2, b"2", ZERO).unwrap();
    db.put(&cf, &k3, b"3", ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    use std::ops::Bound::{Excluded, Included, Unbounded};
    let collect = |lower: std::ops::Bound<&[u8]>, upper: std::ops::Bound<&[u8]>| {
        let txn = db.begin();
        let mut it = txn.new_iterator_bounded(&cf, lower, upper);
        it.seek_to_first();
        let mut fwd = Vec::new();
        while it.valid() {
            fwd.push(it.value().to_vec());
            it.next();
        }
        // Backward pass over the same bounds must agree exactly.
        it.seek_to_last();
        let mut bwd = Vec::new();
        while it.valid() {
            bwd.push(it.value().to_vec());
            it.prev();
        }
        bwd.reverse();
        assert_eq!(
            fwd, bwd,
            "forward/backward disagree for {lower:?}..{upper:?}"
        );
        fwd
    };
    let vals = |s: &[&str]| -> Vec<Vec<u8>> { s.iter().map(|v| v.as_bytes().to_vec()).collect() };

    let lo = 1777062639u64.to_be_bytes();
    let hi = 1777062641u64.to_be_bytes();
    let past = 1777062642u64.to_be_bytes();

    // The confusing shape: Included(bare 8-byte upper) excludes k3, because
    // k3 extends the bound and therefore sorts after it.
    assert_eq!(collect(Included(&lo), Included(&hi)), vals(&["1", "2"]));
    // Bumping the upper prefix by one covers the whole last group.
    assert_eq!(
        collect(Included(&lo), Included(&past)),
        vals(&["1", "2", "3"])
    );
    assert_eq!(
        collect(Included(&lo), Excluded(&past)),
        vals(&["1", "2", "3"])
    );
    // Excluding a bare prefix never excludes its extensions.
    assert_eq!(collect(Excluded(&lo), Unbounded), vals(&["1", "2", "3"]));
    // Full-key bounds behave classically.
    assert_eq!(
        collect(Included(&k1), Included(&k3)),
        vals(&["1", "2", "3"])
    );
    assert_eq!(collect(Excluded(&k1), Excluded(&k3)), vals(&["2"]));
    assert_eq!(collect(Excluded(&k1), Included(&k3)), vals(&["2", "3"]));
    assert_eq!(collect(Unbounded, Excluded(&k2)), vals(&["1"]));
    db.close().unwrap();
}

// ------------------------------------------------ open robustness / config

/// macOS droppings (.DS_Store, AppleDouble `._*` files) in the data
/// directory broke a comparable engine's recovery. Junk anywhere in the tree —
/// including a *parseable-looking* orphan SSTable — must be ignored.
#[test]
fn stray_files_ignored_on_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = open(dir.path());
        for i in 0..100u64 {
            db.put(&cf, format!("k{i:03}").as_bytes(), &val(i, 32), ZERO)
                .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        // Leave some WAL-resident data too.
        for i in 100..120u64 {
            db.put(&cf, format!("k{i:03}").as_bytes(), &val(i, 32), ZERO)
                .unwrap();
        }
        db.close().unwrap();
    }

    let root = dir.path();
    let cf_dir = root.join("cf-default");
    std::fs::write(root.join(".DS_Store"), b"macos junk").unwrap();
    std::fs::write(root.join("garbage.tmp"), val(1, 100)).unwrap();
    std::fs::create_dir(root.join("cf-ghost")).unwrap(); // dir not in manifest
    std::fs::write(root.join("cf-ghost").join("wal-1.log"), val(2, 64)).unwrap();
    std::fs::write(cf_dir.join(".DS_Store"), b"macos junk").unwrap();
    std::fs::write(cf_dir.join("._wal-0001.log"), b"appledouble").unwrap();
    std::fs::write(cf_dir.join("wal-x.log"), val(3, 64)).unwrap(); // unparseable gen
    std::fs::write(cf_dir.join("99999.klog"), val(4, 512)).unwrap(); // orphan SST

    let (db, cf) = open(dir.path());
    assert_eq!(count_keys(&db, &cf), 120);
    for i in (0..120u64).step_by(17) {
        assert_eq!(
            db.get(&cf, format!("k{i:03}").as_bytes()).unwrap(),
            val(i, 32)
        );
    }
    db.put(&cf, b"post-junk", b"1", ZERO).unwrap();
    db.close().unwrap();
}

/// A comparable engine silently lost the level ratio passed at creation on
/// recovery. The comparator is the highest-stakes ondaDB analog (invariant
/// 7: a CF's comparator defines its on-disk order and is persisted by name).
/// `int64` orders negative keys first while bytewise orders them last, so a
/// reopen that dropped the comparator flips the iteration order.
#[test]
fn comparator_and_config_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let nums: Vec<i64> = vec![3, -1, 0, 2, -3, 1, -2];
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family(
                "nums",
                ColumnFamilyConfig {
                    comparator_name: "int64".into(),
                    klog_value_threshold: 64, // also persisted; exercises vlog
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        for n in &nums {
            db.put(&cf, &n.to_be_bytes(), &val(*n as u64, 128), ZERO)
                .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        // One memtable-resident key so reopen merges SST + WAL sources.
        db.put(&cf, &4i64.to_be_bytes(), &val(4, 128), ZERO)
            .unwrap();
        db.close().unwrap();
    }

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("nums").unwrap();
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut got = Vec::new();
    while it.valid() {
        got.push(i64::from_be_bytes(it.key().try_into().unwrap()));
        it.next();
    }
    assert_eq!(
        got,
        vec![-3, -2, -1, 0, 1, 2, 3, 4],
        "int64 comparator not restored from the manifest (bytewise order \
         would put negatives last)"
    );
    drop(it);
    for n in nums.iter().chain(&[4]) {
        assert_eq!(db.get(&cf, &n.to_be_bytes()).unwrap(), val(*n as u64, 128));
    }
    db.close().unwrap();
}

// ----------------------------------------------------------------- shutdown

/// A comparable engine's Drop sent Close messages through the same bounded
/// channel writers were filling — under write pressure nobody could make
/// progress and shutdown deadlocked. close() must terminate in bounded time
/// with writers still hammering, and the store must reopen intact.
#[test]
fn close_under_write_pressure_terminates() {
    let _wd = watchdog("close_under_write_pressure_terminates", 300);
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", small_cfg()).unwrap();

    std::thread::scope(|s| {
        for t in 0..4u64 {
            let (db, cf) = (&db, &cf);
            s.spawn(move || {
                for i in 0..100_000u64 {
                    let key = format!("t{t}-k{i:06}");
                    if db.put(cf, key.as_bytes(), &val(i, 64), ZERO).is_err() {
                        break; // writes failing during/after close is fine
                    }
                }
            });
        }
        // Close mid-flight, from a fifth thread.
        std::thread::sleep(Duration::from_millis(200));
        db.close().unwrap();
    });

    drop(cf);
    drop(db);
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    let n = count_keys(&db, &cf);
    assert!(n > 0, "no writes survived close-under-pressure");
    db.put(&cf, b"after", b"1", ZERO).unwrap();
    assert_eq!(db.get(&cf, b"after").unwrap(), b"1");
    db.close().unwrap();
}

// ------------------------------------------------------------ tombstone hygiene

/// In a comparable engine, a partition whose every key was deleted still
/// carried a 23 MB tombstone segment forever, and `is_empty()` burned CPU
/// walking it. After
/// delete-all + flush + compaction (no snapshots held), iteration must be
/// empty and the tombstones actually gone from the bottom level.
#[test]
fn delete_all_then_compact_leaves_no_debris() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", small_cfg()).unwrap();

    for i in 0..5000u64 {
        db.put(&cf, format!("k{i:05}").as_bytes(), &val(i, 64), ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let mut t = db.begin();
    for i in 0..5000u64 {
        t.delete(&cf, format!("k{i:05}").as_bytes()).unwrap();
    }
    t.commit().unwrap();
    db.flush_memtable(&cf).unwrap();

    // A few rounds so the tombstones reach (and are dropped at) the bottom.
    for _ in 0..5 {
        db.compact(&cf).unwrap();
        if cf.stats().num_tombstones == 0 {
            break;
        }
    }

    assert_eq!(count_keys(&db, &cf), 0);
    let stats = cf.stats();
    assert_eq!(
        stats.num_tombstones, 0,
        "tombstone debris survived full compaction: {stats:?}"
    );
    assert_eq!(
        stats.num_entries, 0,
        "phantom entries survived full compaction: {stats:?}"
    );
    db.close().unwrap();
}
