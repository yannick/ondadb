//! Unified-memtable mode: one shared memtable + WAL across all column families.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, DB};

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
    drop(it);
    t.rollback().unwrap();
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
