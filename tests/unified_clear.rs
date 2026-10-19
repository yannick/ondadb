//! Plan C F5′: `clear_column_family` under the **unified** WAL layout.
//!
//! The cleared family keeps its name but takes a fresh unified id, persisted in
//! the catalog; every entry the shared memtable and WAL still hold under the
//! old id is then owned by no family, and the unified flush discards it. The
//! regressions here are the bug wavesdb hit: a routing map keyed by
//! `hash(name)` instead of the family's stored id flushed the abandoned entries
//! straight back into the cleared family.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, SyncMode, DB};

const ZERO: Duration = Duration::ZERO;

fn opts(path: &std::path::Path) -> Options {
    let mut o = Options::new(path.to_str().unwrap());
    o.unified_memtable = true;
    o
}

fn scan(db: &DB, cf: &Arc<ColumnFamily>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let txn = db.begin();
    let mut it = txn.new_iterator(cf);
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    out
}

fn keys(prefix: &str, n: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            (
                format!("{prefix}{i:04}").into_bytes(),
                format!("v-{prefix}{i}").into_bytes(),
            )
        })
        .collect()
}

fn put_all(db: &DB, cf: &Arc<ColumnFamily>, kv: &[(Vec<u8>, Vec<u8>)]) {
    for (k, v) in kv {
        db.put(cf, k, v, ZERO).unwrap();
    }
}

/// Seal the shared memtable and wait for its split flush.
fn flush_unified(db: &DB) {
    db.rotate_unified_for_tests();
    while db.pending_flushes_for_tests() > 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Entries the family's tables hold — what a flush wrote into it.
fn table_entries(cf: &Arc<ColumnFamily>) -> u64 {
    cf.table_metadata()
        .iter()
        .flatten()
        .map(|m| m.num_entries)
        .sum()
}

#[test]
fn clear_under_unified_empties_the_family_and_spares_the_others() {
    for edit_log in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(opts(dir.path())).unwrap();
        if edit_log {
            db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
                .unwrap();
        }
        let a = db
            .create_column_family("a", ColumnFamilyConfig::default())
            .unwrap();
        let b = db
            .create_column_family("b", ColumnFamilyConfig::default())
            .unwrap();
        let old = keys("old", 300);
        let other = keys("b", 50);
        // Some of `a` flushed into tables, some only in the memtable and WAL.
        put_all(&db, &a, &old[..150]);
        put_all(&db, &b, &other);
        flush_unified(&db);
        put_all(&db, &a, &old[150..]);
        let old_id = a.id();

        let a2 = db.clear_column_family("a").unwrap();
        assert_ne!(a2.id(), old_id, "a cleared family takes a fresh unified id");
        assert_ne!(a2.id(), b.id());
        assert!(scan(&db, &a2).is_empty(), "cleared family reads empty");
        assert!(matches!(db.get(&a2, &old[200].0), Err(OndaError::NotFound)));
        assert_eq!(scan(&db, &b), other, "the other family is untouched");

        let new = keys("new", 40);
        put_all(&db, &a2, &new);
        assert_eq!(scan(&db, &a2), new);
        // The flush that follows must not route the abandoned entries (still
        // in the shared memtable under the old id) back into `a`.
        flush_unified(&db);
        assert_eq!(scan(&db, &a2), new);
        assert_eq!(
            table_entries(&a2),
            new.len() as u64,
            "abandoned entries flushed into a"
        );
        let cleared_id = a2.id();
        db.close().unwrap();

        let db = DB::open(opts(dir.path())).unwrap();
        let a3 = db.get_column_family("a").unwrap();
        assert_eq!(a3.id(), cleared_id, "the fresh id is durable");
        assert_eq!(scan(&db, &a3), new);
        assert_eq!(scan(&db, &db.get_column_family("b").unwrap()), other);
        db.close().unwrap();
    }
}

/// The wavesdb regression, at reopen: the abandoned entries are replayed from
/// the unified WAL into the shared memtable under the OLD id, and the catalog
/// names the NEW one. A registry keyed by `hash(name)` would hand the old
/// entries to the family again at the next flush.
#[test]
fn abandoned_entries_replayed_after_reopen_are_never_flushed_into_the_cleared_family() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.unified_memtable_sync_mode = SyncMode::Full;
    let new = keys("new", 25);
    {
        let db = DB::open(o.clone()).unwrap();
        let a = db
            .create_column_family("a", ColumnFamilyConfig::default())
            .unwrap();
        put_all(&db, &a, &keys("old", 200));
        let a2 = db.clear_column_family("a").unwrap();
        put_all(&db, &a2, &new);
        db.sync_wal().unwrap();
        // Leak the handle: no close, no final flush — the WAL still holds the
        // old id's entries, exactly what a crash leaves.
        std::mem::forget(db);
    }
    // A leaked handle still holds the LOCK in this process; reopen elsewhere.
    let copy = tempfile::tempdir().unwrap();
    copy_dir(dir.path(), copy.path());
    let db = DB::open(opts(copy.path())).unwrap();
    let a = db.get_column_family("a").unwrap();
    assert_eq!(scan(&db, &a), new, "after replay");
    flush_unified(&db);
    assert_eq!(scan(&db, &a), new, "after the replayed memtable's flush");
    assert_eq!(table_entries(&a), new.len() as u64);
    db.close().unwrap();
    let db = DB::open(opts(copy.path())).unwrap();
    assert_eq!(scan(&db, &db.get_column_family("a").unwrap()), new);
    db.close().unwrap();
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let to = dst.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to);
        } else if e.file_name() != "LOCK" {
            std::fs::copy(e.path(), &to).unwrap();
        }
    }
}

/// Clearing twice, and a drop + re-create of the same name, never resurrect
/// entries an earlier incarnation left in the shared memtable.
#[test]
fn repeated_clears_and_drop_recreate_never_resurrect() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(opts(dir.path())).unwrap();
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    put_all(&db, &a, &keys("gen0-", 30));
    let a = db.clear_column_family("a").unwrap();
    put_all(&db, &a, &keys("gen1-", 30));
    let a = db.clear_column_family("a").unwrap();
    assert!(scan(&db, &a).is_empty());
    put_all(&db, &a, &keys("gen2-", 30));
    assert_eq!(scan(&db, &a), keys("gen2-", 30));

    // Drop and re-create under the same name, nothing flushed in between:
    // the derived id's old entries are still in the shared memtable.
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    put_all(&db, &b, &keys("b-old", 20));
    db.drop_column_family("b").unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    assert!(
        scan(&db, &b).is_empty(),
        "drop + create resurrected old entries"
    );
    put_all(&db, &b, &keys("b-new", 5));

    flush_unified(&db);
    assert_eq!(scan(&db, &a), keys("gen2-", 30));
    assert_eq!(scan(&db, &b), keys("b-new", 5));
    db.close().unwrap();
    let db = DB::open(opts(dir.path())).unwrap();
    assert_eq!(
        scan(&db, &db.get_column_family("a").unwrap()),
        keys("gen2-", 30)
    );
    assert_eq!(
        scan(&db, &db.get_column_family("b").unwrap()),
        keys("b-new", 5)
    );
    db.close().unwrap();
}

/// A range tombstone and a merge-free point history under the old id are
/// abandoned too.
#[test]
fn range_tombstones_under_the_old_id_are_abandoned() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(opts(dir.path())).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    put_all(&db, &a, &keys("k", 20));
    db.delete_range(&a, b"k0000", b"k0010").unwrap();
    let a = db.clear_column_family("a").unwrap();
    // A key the old span covered is visible again in the new incarnation.
    db.put(&a, b"k0005", b"fresh", ZERO).unwrap();
    assert_eq!(db.get(&a, b"k0005").unwrap(), b"fresh");
    flush_unified(&db);
    assert_eq!(scan(&db, &a), vec![(b"k0005".to_vec(), b"fresh".to_vec())]);
    db.close().unwrap();
    let db = DB::open(opts(dir.path())).unwrap();
    let a = db.get_column_family("a").unwrap();
    assert_eq!(scan(&db, &a), vec![(b"k0005".to_vec(), b"fresh".to_vec())]);
    db.close().unwrap();
}

#[test]
fn a_prepared_transaction_on_the_family_blocks_the_clear() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(opts(dir.path())).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
        .unwrap();
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    let mut t = db.begin();
    t.put(&a, b"k", b"v", ZERO).unwrap();
    t.prepare(&[9u8; 16]).unwrap();
    assert_eq!(db.clear_column_family("a").err().unwrap().kind(), "busy");
    // Another family is not blocked.
    db.clear_column_family("b").unwrap();
    drop(b);
    db.commit_prepared(&[9u8; 16]).unwrap();
    assert_eq!(
        db.get(&db.get_column_family("a").unwrap(), b"k").unwrap(),
        b"v"
    );
    let a = db.clear_column_family("a").unwrap();
    assert!(scan(&db, &a).is_empty());
    db.close().unwrap();
}

// ---- crash: clear, then die before any flush -------------------------------------

const CRASH_DIR_ENV: &str = "ONDA_UNIFIED_CLEAR_CRASH_DIR";

/// Not a real test: the child half of the crash test.
#[test]
fn unified_clear_crash_helper() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    let mut o = Options::new(dir);
    o.unified_memtable = true;
    o.unified_memtable_sync_mode = SyncMode::Full;
    let db = DB::open(o).unwrap();
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    put_all(&db, &a, &keys("old", 500));
    put_all(&db, &b, &keys("b", 100));
    db.sync_wal().unwrap();
    let a2 = db.clear_column_family("a").unwrap();
    put_all(&db, &a2, &keys("new", 100));
    db.sync_wal().unwrap();
    // Simulated crash: no close, no Drop, nothing flushed.
    std::process::exit(0);
}

#[test]
fn clear_then_crash_before_flush_reopens_empty_with_later_writes_intact() {
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["unified_clear_crash_helper", "--exact", "--nocapture"])
        .env(CRASH_DIR_ENV, dir.path().to_str().unwrap())
        .status()
        .expect("spawn crash helper");
    assert!(status.success());
    for round in 0..2 {
        let db = DB::open(opts(dir.path())).unwrap();
        let a = db.get_column_family("a").unwrap();
        let b = db.get_column_family("b").unwrap();
        assert_eq!(scan(&db, &a), keys("new", 100), "round {round}");
        assert_eq!(scan(&db, &b), keys("b", 100), "round {round}");
        if round == 0 {
            flush_unified(&db);
            assert_eq!(table_entries(&a), 100, "abandoned entries flushed into a");
        }
        db.close().unwrap();
    }
}
