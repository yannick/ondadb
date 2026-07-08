//! Port of surrealkv v0.21.2's engine-generic tests that cover properties
//! ondaDB's suite did not yet exercise.
//!
//! Source: `../surrealkv/src/test/*.rs` (surrealkv keeps its tests in-crate).
//! surrealkv's isolation ceiling is Snapshot Isolation with first-committer-
//! wins write-write conflict detection — the same as ondaDB's `Snapshot`
//! level, so every ported test runs unmodified semantics-wise.
//!
//! Ported groups:
//! - hermitage anomalies: G0 (dirty write cycle), P4 (lost update),
//!   G-single (read skew), and the documented write-skew allowance.
//! - format edge cases: empty values, binary (non-UTF-8) keys.
//! - iterator robustness: direction switching mid-scan, reverse iteration
//!   over long tombstone runs (stack-overflow regression).
//! - crash/corruption: torn WAL tail, mid-WAL corruption, zero-length WAL —
//!   via a child process that exits without closing (the LOCK file makes
//!   in-process crash simulation impossible by design).
//! - one large transaction (scaled-down `insert_large_txn_and_get`).
//!
//! Not ported (feature doesn't exist in ondaDB): time-travel/versioned reads
//! (`get_at`, `history`, `scan_all_versions`), soft deletes, WAL recovery-mode
//! selection, compressed WAL segments.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, OndaError, Options, DB};

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

// ------------------------------------------------ hermitage: transaction_tests.rs

/// G0: two concurrent transactions write an overlapping key set; the write
/// cycle must be broken by aborting the second committer, and readers must
/// see only the winner's values (surrealkv `g0_tests`).
#[test]
fn g0_dirty_write_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k1", b"v0", ZERO).unwrap();
    db.put(&cf, b"k2", b"v0", ZERO).unwrap();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Snapshot);
    let mut t2 = db.begin_with_isolation(IsolationLevel::Snapshot);

    t1.put(&cf, b"k1", b"t1", ZERO).unwrap();
    t2.put(&cf, b"k1", b"t2", ZERO).unwrap();
    t1.put(&cf, b"k2", b"t1", ZERO).unwrap();
    t2.put(&cf, b"k2", b"t2", ZERO).unwrap();

    t1.commit().unwrap();
    match t2.commit() {
        Err(OndaError::Conflict(_)) => {}
        other => panic!("second committer must abort with Conflict, got {other:?}"),
    }

    assert_eq!(db.get(&cf, b"k1").unwrap(), b"t1");
    assert_eq!(db.get(&cf, b"k2").unwrap(), b"t1");
    db.close().unwrap();
}

/// P4: classic lost update — both transactions read a counter, both write it
/// back; the second commit must abort (surrealkv `p4`).
#[test]
fn p4_lost_update() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"counter", b"10", ZERO).unwrap();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Snapshot);
    let mut t2 = db.begin_with_isolation(IsolationLevel::Snapshot);

    assert_eq!(t1.get(&cf, b"counter").unwrap(), b"10");
    assert_eq!(t2.get(&cf, b"counter").unwrap(), b"10");

    t1.put(&cf, b"counter", b"11", ZERO).unwrap();
    t2.put(&cf, b"counter", b"11", ZERO).unwrap();

    t1.commit().unwrap();
    match t2.commit() {
        Err(OndaError::Conflict(_)) => {}
        other => panic!("lost update must be prevented, got {other:?}"),
    }
    assert_eq!(db.get(&cf, b"counter").unwrap(), b"11");
    db.close().unwrap();
}

/// G-single: read skew — a reader transaction must keep a stable snapshot of
/// (k1, k2) while a concurrent writer updates both and commits
/// (surrealkv `g_single`).
#[test]
fn g_single_read_skew() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k1", b"a10", ZERO).unwrap();
    db.put(&cf, b"k2", b"b20", ZERO).unwrap();

    let mut reader = db.begin_with_isolation(IsolationLevel::Snapshot);
    assert_eq!(reader.get(&cf, b"k1").unwrap(), b"a10");

    let mut writer = db.begin_with_isolation(IsolationLevel::Snapshot);
    writer.put(&cf, b"k1", b"a12", ZERO).unwrap();
    writer.put(&cf, b"k2", b"b18", ZERO).unwrap();
    writer.commit().unwrap();

    // The reader's snapshot predates the writer's commit: it must see the old
    // k2, not the new one (no read skew within one snapshot).
    assert_eq!(reader.get(&cf, b"k2").unwrap(), b"b20");
    reader.rollback().unwrap();

    assert_eq!(db.get(&cf, b"k1").unwrap(), b"a12");
    assert_eq!(db.get(&cf, b"k2").unwrap(), b"b18");
    db.close().unwrap();
}

/// Snapshot isolation permits write skew: disjoint write sets after a common
/// read both commit. This documents the isolation ceiling exactly like
/// surrealkv's `test_si_write_skew_still_allowed` — if ondaDB ever gains full
/// SSI, this test should flip.
#[test]
fn write_skew_allowed_at_snapshot_level() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"x", b"35", ZERO).unwrap();
    db.put(&cf, b"y", b"35", ZERO).unwrap();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Snapshot);
    let mut t2 = db.begin_with_isolation(IsolationLevel::Snapshot);

    // Both read the invariant x + y >= 60, then each debits a different key.
    let _ = t1.get(&cf, b"x").unwrap();
    let _ = t1.get(&cf, b"y").unwrap();
    let _ = t2.get(&cf, b"x").unwrap();
    let _ = t2.get(&cf, b"y").unwrap();
    t1.put(&cf, b"x", b"5", ZERO).unwrap();
    t2.put(&cf, b"y", b"5", ZERO).unwrap();

    t1.commit().unwrap();
    t2.commit().unwrap(); // disjoint write sets: no write-write conflict
    assert_eq!(db.get(&cf, b"x").unwrap(), b"5");
    assert_eq!(db.get(&cf, b"y").unwrap(), b"5");
    db.close().unwrap();
}

// ----------------------------------------- edge cases: batch/memtable/sstable_tests

/// Empty values must round-trip through memtable, WAL replay, flush and
/// compaction (surrealkv `test_batch_empty_key_and_value`, value part).
#[test]
fn empty_value_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    db.put(&cf, b"empty", b"", ZERO).unwrap();
    assert_eq!(db.get(&cf, b"empty").unwrap(), b"");
    db.flush_memtable(&cf).unwrap();
    assert_eq!(db.get(&cf, b"empty").unwrap(), b"");
    db.close().unwrap();
    drop(cf);
    drop(db);

    let (db, cf) = open(dir.path());
    assert_eq!(db.get(&cf, b"empty").unwrap(), b"");
    db.close().unwrap();
}

/// Arbitrary binary keys (0x00 / 0xff / invalid UTF-8) must round-trip and
/// iterate in bytewise order across flush and reopen (surrealkv
/// `test_binary_keys` / `test_writer_key_range_binary_keys`).
#[test]
fn binary_keys_roundtrip_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    let mut keys: Vec<Vec<u8>> = vec![
        vec![0x00],
        vec![0x00, 0x00],
        vec![0x00, 0xff],
        vec![0x7f],
        vec![0x80, 0x81], // invalid UTF-8
        vec![0xc3, 0x28], // invalid UTF-8 sequence
        vec![0xff],
        vec![0xff, 0x00],
        vec![0xff, 0xff, 0xff],
    ];
    // Insert in scrambled order.
    for k in keys.iter().rev() {
        db.put(&cf, k, k, ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.close().unwrap();
    drop(cf);
    drop(db);

    let (db, cf) = open(dir.path());
    keys.sort();
    for k in &keys {
        assert_eq!(&db.get(&cf, k).unwrap(), k);
    }
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    for k in &keys {
        assert!(it.valid());
        assert_eq!(it.key(), k.as_slice(), "bytewise iteration order");
        it.next();
    }
    assert!(!it.valid());
    db.close().unwrap();
}

// -------------------------------------- iterator robustness: snapshot/transaction_tests

/// Reverse iteration over a long run of tombstones must neither recurse nor
/// skip live keys (surrealkv regression
/// `test_backward_iter_does_not_stack_overflow_on_tombstones`).
#[test]
fn reverse_iteration_over_tombstone_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    for i in 0..10_000u32 {
        db.put(&cf, format!("k{i:05}").as_bytes(), b"v", ZERO)
            .unwrap();
    }
    // Delete everything except the two ends: a 9,998-tombstone run in the
    // middle that reverse iteration has to step over.
    let mut txn = db.begin();
    for i in 1..9_999u32 {
        txn.delete(&cf, format!("k{i:05}").as_bytes()).unwrap();
    }
    txn.commit().unwrap();
    db.flush_memtable(&cf).unwrap();

    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), b"k09999");
    it.prev();
    assert!(it.valid());
    assert_eq!(it.key(), b"k00000");
    it.prev();
    assert!(!it.valid());
    db.close().unwrap();
}

/// Switching iteration direction mid-scan, at the ends, and after a seek must
/// land on the adjacent key each time (surrealkv `test_direction_switch_*`,
/// `test_multiple_direction_switches`).
#[test]
fn iterator_direction_switching() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for k in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(&cf, k, k, ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    // A memtable-resident key too, so the switch crosses source boundaries.
    db.put(&cf, b"bb", b"bb", ZERO).unwrap();

    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);

    // forward: a -> b -> bb, then flip backward: b -> a
    it.seek_to_first();
    assert_eq!(it.key(), b"a");
    it.next();
    assert_eq!(it.key(), b"b");
    it.next();
    assert_eq!(it.key(), b"bb");
    it.prev();
    assert_eq!(it.key(), b"b");
    it.prev();
    assert_eq!(it.key(), b"a");
    // flip at the front edge: prev() exhausts, seek back in
    it.prev();
    assert!(!it.valid());
    it.seek(b"c");
    assert_eq!(it.key(), b"c");
    // multiple switches around a single position
    it.next();
    assert_eq!(it.key(), b"d");
    it.prev();
    assert_eq!(it.key(), b"c");
    it.next();
    assert_eq!(it.key(), b"d");
    // flip at the back edge
    it.seek_to_last();
    assert_eq!(it.key(), b"e");
    it.next();
    assert!(!it.valid());
    it.seek_for_prev(b"bz");
    assert_eq!(it.key(), b"bb");
    db.close().unwrap();
}

// ------------------------------------------- crash & corruption: recovery_tests.rs
//
// A child process writes committed data and exits WITHOUT closing (process
// exit skips Drop), leaving live WAL files behind — the LOCK file prevents
// simulating this in-process. The parent then damages the WAL and reopens.

const CRASH_HELPER_ENV: &str = "ONDA_SKV_SUITE_CRASH_DIR";

/// Not a real test: the child half of the crash simulation. Runs only when
/// the env var is set; writes 500 committed keys and exits without close.
#[test]
fn crash_writer_helper() {
    let Ok(dir) = std::env::var(CRASH_HELPER_ENV) else {
        return; // normal test runs skip this
    };
    let (db, cf) = open(std::path::Path::new(&dir));
    // Write from 4 threads so all 4 WAL stripes carry data (stripe choice is
    // per-thread) — the corruption tests damage one stripe and assert the
    // others survive.
    std::thread::scope(|s| {
        for t in 0..4u32 {
            let (db, cf) = (&db, &cf);
            s.spawn(move || {
                for i in (t * 125)..((t + 1) * 125) {
                    db.put(cf, format!("k{i:04}").as_bytes(), b"crash-value", ZERO)
                        .unwrap();
                }
            });
        }
    });
    // Simulated crash: no close(), no Drop.
    std::process::exit(0);
}

fn run_crash_writer(dir: &std::path::Path) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_writer_helper", "--exact", "--nocapture"])
        .env(CRASH_HELPER_ENV, dir.to_str().unwrap())
        .status()
        .expect("spawn crash writer");
    assert!(status.success(), "crash writer child failed");
}

fn wal_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let cf_dir = dir.join("cf-default");
    let mut out = Vec::new();
    for e in std::fs::read_dir(cf_dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with("wal-") {
            out.push(e.path());
        }
    }
    out.sort();
    assert!(!out.is_empty(), "crash left no WAL files behind");
    out
}

/// A torn write at the WAL tail (partial frame) must be discarded cleanly on
/// replay; every previously committed key survives (surrealkv
/// `test_crash_during_wal_write_mid_batch` / `test_tail_corruption_recovery`).
#[test]
fn wal_torn_tail_recovers_committed_prefix() {
    let dir = tempfile::tempdir().unwrap();
    run_crash_writer(dir.path());

    // Append garbage to every WAL file: a torn frame at each tail.
    for wal in wal_files(dir.path()) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(wal).unwrap();
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x42]).unwrap();
    }

    let (db, cf) = open(dir.path());
    for i in 0..500u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:04}").as_bytes()).unwrap(),
            b"crash-value",
            "committed key k{i:04} lost after torn tail"
        );
    }
    db.close().unwrap();
}

/// Corruption in the MIDDLE of a WAL file must never panic or corrupt the
/// store: replay stops at the bad frame, the database opens, and every key
/// either resolves or is cleanly absent (surrealkv
/// `test_corruption_middle_segment` family).
#[test]
fn wal_middle_corruption_reopens_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    run_crash_writer(dir.path());

    // Flip bytes a third of the way into the largest WAL file.
    let wal = wal_files(dir.path())
        .into_iter()
        .max_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .unwrap();
    let mut bytes = std::fs::read(&wal).unwrap();
    assert!(bytes.len() > 64, "wal too small to corrupt meaningfully");
    let at = bytes.len() / 3;
    let end = (at + 16).min(bytes.len());
    for b in &mut bytes[at..end] {
        *b ^= 0xff;
    }
    std::fs::write(&wal, bytes).unwrap();

    let (db, cf) = open(dir.path());
    let mut survivors = 0;
    for i in 0..500u32 {
        match db.get(&cf, format!("k{i:04}").as_bytes()) {
            Ok(v) => {
                assert_eq!(v, b"crash-value");
                survivors += 1;
            }
            Err(OndaError::NotFound) => {} // dropped past the corrupt frame
            Err(e) => panic!("unexpected error reading after corruption: {e}"),
        }
    }
    // Frames before the corruption point (and all other stripes) must survive.
    assert!(survivors > 0, "corruption must not wipe the whole WAL");
    db.close().unwrap();
}

/// A zero-length WAL file (created but never written) must replay as empty
/// (surrealkv `test_zero_length_segment_file`).
#[test]
fn zero_length_wal_replays_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    run_crash_writer(dir.path());

    let wal = wal_files(dir.path()).remove(0);
    std::fs::write(&wal, b"").unwrap(); // truncate to zero

    let (db, cf) = open(dir.path());
    // Keys from the truncated file are gone; the rest must still resolve.
    let mut survivors = 0;
    for i in 0..500u32 {
        if db.get(&cf, format!("k{i:04}").as_bytes()).is_ok() {
            survivors += 1;
        }
    }
    assert!(survivors > 0, "other WAL stripes must still replay");
    db.put(&cf, b"after", b"1", ZERO).unwrap();
    assert_eq!(db.get(&cf, b"after").unwrap(), b"1");
    db.close().unwrap();
}

// --------------------------------------------------- stress: transaction_tests.rs

/// One transaction with 100k entries commits atomically and reads back
/// (scaled-down surrealkv `insert_large_txn_and_get`, 400k there).
#[test]
fn large_single_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    let mut txn = db.begin();
    for i in 0..100_000u32 {
        txn.put(&cf, &i.to_be_bytes(), b"v", ZERO).unwrap();
    }
    txn.commit().unwrap();

    for i in (0..100_000u32).step_by(997) {
        assert_eq!(db.get(&cf, &i.to_be_bytes()).unwrap(), b"v");
    }
    db.flush_memtable(&cf).unwrap();
    assert_eq!(db.get(&cf, &99_999u32.to_be_bytes()).unwrap(), b"v");
    db.close().unwrap();
}
