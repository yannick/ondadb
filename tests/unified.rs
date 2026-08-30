//! Unified-memtable mode: one shared memtable + WAL across all column families.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, SyncMode, DB};

fn open_unified(path: &str) -> DB {
    let opts = Options {
        unified_memtable: true,
        unified_memtable_write_buffer_size: 64 * 1024, // small to force split flushes
        ..Options::new(path)
    };
    DB::open(opts).unwrap()
}

#[test]
fn unified_basic_multi_cf() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();

    // Same user key in two CFs must stay independent (prefixing by CF id).
    db.put(&a, b"k", b"va", Duration::ZERO).unwrap();
    db.put(&b, b"k", b"vb", Duration::ZERO).unwrap();
    assert_eq!(db.get(&a, b"k").unwrap(), b"va");
    assert_eq!(db.get(&b, b"k").unwrap(), b"vb");

    db.delete(&a, b"k").unwrap();
    assert!(db.get(&a, b"k").is_err());
    assert_eq!(db.get(&b, b"k").unwrap(), b"vb"); // b unaffected
    db.close().unwrap();
}

#[test]
fn unified_split_flush_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = db
            .create_column_family("a", ColumnFamilyConfig::default())
            .unwrap();
        let b = db
            .create_column_family("b", ColumnFamilyConfig::default())
            .unwrap();
        // Enough data across both CFs to overflow the shared memtable and force
        // split flushes into per-CF SSTables.
        for i in 0..5000u32 {
            db.put(&a, format!("a{i:06}").as_bytes(), b"VA", Duration::ZERO)
                .unwrap();
            db.put(&b, format!("b{i:06}").as_bytes(), b"VB", Duration::ZERO)
                .unwrap();
        }
        for i in (0..5000u32).step_by(50) {
            assert_eq!(db.get(&a, format!("a{i:06}").as_bytes()).unwrap(), b"VA");
            assert_eq!(db.get(&b, format!("b{i:06}").as_bytes()).unwrap(), b"VB");
        }
        db.close().unwrap();
    }
    // Reopen: data recovered from per-CF SSTables + the shared WAL replay.
    let db = open_unified(&path);
    let a = db.get_column_family("a").unwrap();
    let b = db.get_column_family("b").unwrap();
    for i in (0..5000u32).step_by(50) {
        assert_eq!(
            db.get(&a, format!("a{i:06}").as_bytes()).unwrap(),
            b"VA",
            "a{i}"
        );
        assert_eq!(
            db.get(&b, format!("b{i:06}").as_bytes()).unwrap(),
            b"VB",
            "b{i}"
        );
    }
    db.close().unwrap();
}

#[test]
fn unified_iteration() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let _b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    // Write to both, but iterate only "a": must see exactly a's keys, in order.
    for k in ["c", "a", "b", "e", "d"] {
        db.put(&a, k.as_bytes(), b"v", Duration::ZERO).unwrap();
        db.put(&_b, format!("z{k}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    let mut t = db.begin();
    let mut it = t.new_iterator(&a);
    let mut got = Vec::new();
    it.seek_to_first();
    while it.valid() {
        got.push(String::from_utf8(it.key().to_vec()).unwrap());
        it.next();
    }
    assert_eq!(got, vec!["a", "b", "c", "d", "e"]);

    it.seek(b"c");
    assert_eq!(it.key(), b"c");
    it.next();
    assert_eq!(it.key(), b"d");
    it.prev();
    assert_eq!(it.key(), b"c", "forward-to-backward switch must not skip");
    it.seek_for_prev(b"cc");
    assert_eq!(it.key(), b"c");
    it.prev();
    assert_eq!(it.key(), b"b");
    it.next();
    assert_eq!(it.key(), b"c", "backward-to-forward switch must not skip");
    drop(it);

    let mut bounded = t.new_iterator_bounded(
        &a,
        std::ops::Bound::Excluded(b"b".as_slice()),
        std::ops::Bound::Included(b"d".as_slice()),
    );
    bounded.seek_to_first();
    assert_eq!(bounded.key(), b"c");
    bounded.seek_to_last();
    assert_eq!(bounded.key(), b"d");
    bounded.next();
    assert!(!bounded.valid(), "upper bound must terminate iteration");
    drop(bounded);
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
#[ignore = "manual unified iterator construction timing probe"]
fn unified_iterator_construction_probe() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    db.put(&a, b"needle", b"value", Duration::ZERO).unwrap();
    for i in 0..2_000u32 {
        db.put(
            &b,
            format!("other/{i:06}").as_bytes(),
            b"value",
            Duration::ZERO,
        )
        .unwrap();
    }

    let txn = db.begin();
    let iterations = 200u32;
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let mut it = txn.new_iterator(&a);
        it.seek(b"needle");
        std::hint::black_box((it.key(), it.value()));
    }
    let elapsed = start.elapsed();
    eprintln!(
        "unified iterator construction: {} ns/iteration",
        elapsed.as_nanos() / u128::from(iterations)
    );
    drop(txn);
    db.close().unwrap();
}

#[test]
#[cfg(debug_assertions)]
fn unified_iteration_does_not_materialize_shared_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    db.put(&a, b"a", b"A", Duration::ZERO).unwrap();
    db.put(&a, b"b", b"B", Duration::ZERO).unwrap();
    db.put(&b, b"other", b"X", Duration::ZERO).unwrap();

    ondadb::memtable::reset_snapshot_calls();
    let txn = db.begin();
    let mut it = txn.new_iterator(&a);
    it.seek_to_first();
    while it.valid() {
        std::hint::black_box((it.key(), it.value()));
        it.next();
    }
    assert_eq!(
        ondadb::memtable::snapshot_calls(),
        0,
        "bytewise unified iteration must use lazy prefix cursors"
    );
    drop(it);
    drop(txn);
    db.close().unwrap();
}

#[test]
fn unified_concurrent_writers() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_unified(dir.path().to_str().unwrap()));
    let cfs: Vec<Arc<ColumnFamily>> = (0..4)
        .map(|i| {
            db.create_column_family(&format!("cf{i}"), ColumnFamilyConfig::default())
                .unwrap()
        })
        .collect();
    let mut handles = Vec::new();
    for (t, cf) in cfs.iter().enumerate() {
        let db = db.clone();
        let cf = cf.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..2000u32 {
                db.put(&cf, format!("k{t}-{i:05}").as_bytes(), b"v", Duration::ZERO)
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    for (t, cf) in cfs.iter().enumerate() {
        for i in 0..2000u32 {
            assert_eq!(db.get(cf, format!("k{t}-{i:05}").as_bytes()).unwrap(), b"v");
        }
    }
    db.close().unwrap();
}

#[test]
fn unified_sync_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    db.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    db.sync_wal().unwrap();
    assert_eq!(db.get(&a, b"k").unwrap(), b"v");
    db.close().unwrap();
}

#[test]
fn unified_database_refuses_per_cf_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    {
        let db = open_unified(path);
        let cf = db
            .create_column_family("data", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.close().unwrap();
    }

    let err = DB::open(Options::new(path)).expect_err("layout mismatch must fail");
    assert_eq!(err.kind(), "invalid_args");
}

#[test]
fn per_cf_database_refuses_unified_reopen_without_migration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    {
        let db = DB::open(Options::new(path)).unwrap();
        let cf = db
            .create_column_family("data", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.close().unwrap();
    }

    let err = DB::open(Options {
        unified_memtable: true,
        ..Options::new(path)
    })
    .expect_err("layout mismatch must fail");
    assert_eq!(err.kind(), "invalid_args");
}

#[test]
fn explicit_migration_preserves_per_cf_data_and_one_cross_cf_txn_syncs_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    {
        let db = DB::open(Options::new(path)).unwrap();
        let a = db
            .create_column_family("a", ColumnFamilyConfig::default())
            .unwrap();
        let b = db
            .create_column_family("b", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&a, b"old-a", b"A", Duration::ZERO).unwrap();
        db.put(&b, b"old-b", b"B", Duration::ZERO).unwrap();
        db.close().unwrap();
    }

    let db = DB::open(Options {
        unified_memtable: true,
        migrate_to_unified: true,
        unified_memtable_sync_mode: SyncMode::Full,
        ..Options::new(path)
    })
    .unwrap();
    let a = db.get_column_family("a").unwrap();
    let b = db.get_column_family("b").unwrap();
    assert_eq!(db.get(&a, b"old-a").unwrap(), b"A");
    assert_eq!(db.get(&b, b"old-b").unwrap(), b"B");

    let before = db.wal_sync_count();
    let mut tx = db.begin();
    tx.put(&a, b"log", b"entry", Duration::ZERO).unwrap();
    tx.put(&b, b"hs", b"hardstate", Duration::ZERO).unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.wal_sync_count() - before,
        1,
        "one unified Full WAL frame performs one physical sync"
    );
    db.close().unwrap();

    let db = DB::open(Options {
        unified_memtable: true,
        unified_memtable_sync_mode: SyncMode::Full,
        ..Options::new(path)
    })
    .unwrap();
    let a = db.get_column_family("a").unwrap();
    let b = db.get_column_family("b").unwrap();
    assert_eq!(db.get(&a, b"log").unwrap(), b"entry");
    assert_eq!(db.get(&b, b"hs").unwrap(), b"hardstate");
    db.close().unwrap();
}

/// The tail must work over the unified memtable layout too, where the shared
/// store is consulted through a CF-scoped overlay iterator.
///
/// Known cost, accepted for v1: the unified overlay path is rebuilt per
/// segment, so a tail that refreshes often pays for it more than the per-CF
/// layout does.
#[test]
fn tailing_iterator_works_in_unified_mode() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let cf = db
        .create_column_family("q", ColumnFamilyConfig::default())
        .unwrap();
    // A second CF sharing the memtable: its keys must never leak into the tail.
    let other = db
        .create_column_family("other", ColumnFamilyConfig::default())
        .unwrap();

    let key = |i: u32| format!("k{i:06}").into_bytes();
    for i in 0..40 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
        db.put(&other, &key(i), b"noise", Duration::ZERO).unwrap();
    }

    let mut tail = db.new_tailing_iterator(&cf);
    tail.seek_to_first();
    let mut observed: Vec<Vec<u8>> = Vec::new();
    while tail.valid() {
        assert_eq!(tail.value(), b"v");
        observed.push(tail.key().to_vec());
        tail.next();
    }
    assert_eq!(observed.len(), 40);

    // Append, then tail on: strictly greater keys only, and complete.
    for i in 40..80 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
        db.put(&other, &key(i), b"noise", Duration::ZERO).unwrap();
    }
    assert!(tail.refresh());
    while tail.valid() {
        observed.push(tail.key().to_vec());
        tail.next();
    }
    assert!(tail.err().is_none());
    assert_eq!(observed, (0..80).map(key).collect::<Vec<_>>());

    // A key behind the cursor stays invisible here as well.
    db.put(&cf, &key(7), b"rewritten", Duration::ZERO).unwrap();
    tail.refresh();
    assert!(!tail.valid());
    db.close().unwrap();
}

// ---- range deletes under schema 2 (1.2) -------------------------------------

/// Both bounds carry the 8-byte big-endian cf-id prefix, exactly as point keys
/// do — so one column family's span can never reach into another's keyspace,
/// even when the two use identical user keys.
#[test]
fn unified_range_record_prefixes_both_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();

    for cf in [&a, &b] {
        for k in [&b"k1"[..], b"k5", b"k9"] {
            db.put(cf, k, b"v", Duration::ZERO).unwrap();
        }
    }
    // A span that would swallow everything if the prefix were dropped.
    db.delete_range(&a, b"k0", b"z").unwrap();
    for k in [&b"k1"[..], b"k5", b"k9"] {
        assert!(db.get(&a, k).is_err(), "a/{:?} is deleted", k);
        assert_eq!(db.get(&b, k).unwrap(), b"v", "b/{:?} is untouched", k);
    }
    db.close().unwrap();
}

/// Replay carries the prefixed bounds through unchanged, so a recovered
/// database masks the same keys — and only those.
///
/// The `LOCK` file makes in-process crash simulation impossible by design, so
/// the writer half runs in a child process that exits without closing (the
/// pattern `tests/engine_regressions.rs` established).
const RANGE_CRASH_DIR_ENV: &str = "ONDA_UNIFIED_RANGE_CRASH_DIR";

/// Not a real test: the child half of the crash simulation above.
#[test]
fn unified_range_crash_helper() {
    let Ok(dir) = std::env::var(RANGE_CRASH_DIR_ENV) else {
        return;
    };
    let db = open_unified(&dir);
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    for cf in [&a, &b] {
        for i in 0..6u32 {
            db.put(cf, format!("k{i}").as_bytes(), b"v", Duration::ZERO)
                .unwrap();
        }
    }
    db.delete_range(&a, b"k2", b"k5").unwrap();
    // Simulated crash: no close(), no Drop, nothing flushed.
    std::process::exit(0);
}

#[test]
fn unified_range_replay_strips_prefix_from_both_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["unified_range_crash_helper", "--exact", "--nocapture"])
        .env(RANGE_CRASH_DIR_ENV, dir.path().to_str().unwrap())
        .status()
        .expect("spawn the crash helper");
    assert!(status.success(), "crash helper child failed");

    let db = open_unified(dir.path().to_str().unwrap());
    let a = db.get_column_family("a").expect("a recovered");
    let b = db.get_column_family("b").expect("b recovered");
    for i in 0..6u32 {
        let k = format!("k{i}");
        assert_eq!(
            db.get(&a, k.as_bytes()).is_ok(),
            !(2..5).contains(&i),
            "a/{k} after replay"
        );
        assert!(db.get(&b, k.as_bytes()).is_ok(), "b/{k} after replay");
    }
    // And the same after a flush, from the durable fragments alone.
    db.flush_memtable(&a).unwrap();
    for i in 0..6u32 {
        let k = format!("k{i}");
        assert_eq!(
            db.get(&a, k.as_bytes()).is_ok(),
            !(2..5).contains(&i),
            "{k}"
        );
    }
    db.close().unwrap();
}

/// A span can never cross a cf-id boundary: both bounds come from one
/// `delete_range` call on one column family, so they share a prefix by
/// construction. Asserted here for adversarial bounds — a start that IS a
/// neighbouring family's id, and a span that reaches to the end of the
/// keyspace.
#[test]
fn unified_range_span_never_crosses_cf_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let names = ["cf-a", "cf-b", "cf-c"];
    let cfs: Vec<_> = names
        .iter()
        .map(|n| {
            db.create_column_family(n, ColumnFamilyConfig::default())
                .unwrap()
        })
        .collect();
    // Keys chosen to look like cf-id prefixes if the prefix were mishandled.
    let keys: Vec<Vec<u8>> = (0..4u64)
        .map(|i| i.to_be_bytes().to_vec())
        .chain([vec![0xFF; 8], b"zzzzzzzzzzzz".to_vec()])
        .collect();
    for cf in &cfs {
        for k in &keys {
            db.put(cf, k, b"v", Duration::ZERO).unwrap();
        }
    }
    // The widest legal span in the middle family.
    db.delete_range(&cfs[1], &[0u8], &[0xFFu8; 16]).unwrap();
    for k in &keys {
        assert!(db.get(&cfs[0], k).is_ok(), "cf-a untouched");
        assert!(db.get(&cfs[1], k).is_err(), "cf-b fully deleted");
        assert!(db.get(&cfs[2], k).is_ok(), "cf-c untouched");
    }
    // Flush splits the shared memtable's spans back by cf id; the answer holds.
    for cf in &cfs {
        db.flush_memtable(cf).unwrap();
    }
    for k in &keys {
        assert!(db.get(&cfs[0], k).is_ok(), "cf-a after flush");
        assert!(db.get(&cfs[1], k).is_err(), "cf-b after flush");
        assert!(db.get(&cfs[2], k).is_ok(), "cf-c after flush");
    }
    db.close().unwrap();
}
