//! End-to-end database tests

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, Options, DB};

fn open(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    (db, cf)
}

#[test]
fn basic_put_get_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    db.put(&cf, b"b", b"2", Duration::ZERO).unwrap();
    assert_eq!(db.get(&cf, b"a").unwrap(), b"1");
    assert_eq!(db.get(&cf, b"b").unwrap(), b"2");
    assert!(db.get(&cf, b"missing").is_err());
    db.delete(&cf, b"a").unwrap();
    assert!(db.get(&cf, b"a").is_err());
    db.close().unwrap();
}

#[test]
fn overwrite_keeps_latest() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for i in 0..5 {
        db.put(&cf, b"k", format!("v{i}").as_bytes(), Duration::ZERO)
            .unwrap();
    }
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v4");
    db.close().unwrap();
}

#[test]
fn transaction_commit_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    let mut t = db.begin();
    t.put(&cf, b"x", b"1", Duration::ZERO).unwrap();
    t.put(&cf, b"y", b"2", Duration::ZERO).unwrap();
    assert_eq!(t.get(&cf, b"x").unwrap(), b"1"); // read-your-writes
    t.commit().unwrap();
    assert_eq!(db.get(&cf, b"x").unwrap(), b"1");

    let mut t = db.begin();
    t.put(&cf, b"z", b"3", Duration::ZERO).unwrap();
    t.rollback().unwrap();
    assert!(db.get(&cf, b"z").is_err());
    db.close().unwrap();
}

#[test]
fn snapshot_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k", b"old", Duration::ZERO).unwrap();

    let mut snap = db.begin(); // Snapshot at "old"
    db.put(&cf, b"k", b"new", Duration::ZERO).unwrap();
    assert_eq!(snap.get(&cf, b"k").unwrap(), b"old"); // snapshot still sees old
    assert_eq!(db.get(&cf, b"k").unwrap(), b"new"); // latest sees new
    snap.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn write_write_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k", b"0", Duration::ZERO).unwrap();

    let mut t1 = db.begin();
    t1.put(&cf, b"k", b"t1", Duration::ZERO).unwrap();
    // Another committed write after t1's snapshot.
    db.put(&cf, b"k", b"other", Duration::ZERO).unwrap();
    // t1 should conflict.
    assert!(matches!(t1.commit(), Err(e) if e.kind() == "conflict"));
    db.close().unwrap();
}

#[test]
fn serializable_validates_read_only_cf() {
    // A Serializable txn that READS a key in cf_a (which it never writes) and WRITES
    // to cf_b must abort if that read key changes under it. Previously the read-set
    // validation only checked CFs present in the write set, so this conflict was
    // silently missed.
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf_a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let cf_b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    db.put(&cf_a, b"r", b"0", Duration::ZERO).unwrap();

    let mut t = db.begin_with_isolation(IsolationLevel::Serializable);
    assert_eq!(t.get(&cf_a, b"r").unwrap(), b"0"); // read from cf_a (read-only for t)
    t.put(&cf_b, b"w", b"1", Duration::ZERO).unwrap(); // write only to cf_b

    // A concurrent committer changes the key t read.
    db.put(&cf_a, b"r", b"changed", Duration::ZERO).unwrap();

    // t must detect the read-set change and abort.
    assert!(
        matches!(t.commit(), Err(e) if e.kind() == "conflict"),
        "serializable txn must conflict on a changed read-only-CF key"
    );
    db.close().unwrap();
}

#[test]
fn ttl_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k", b"v", Duration::from_millis(50)).unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
    std::thread::sleep(Duration::from_millis(80));
    assert!(db.get(&cf, b"k").is_err(), "key should have expired");
    db.close().unwrap();
}

#[test]
fn iteration_forward_backward() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for k in ["a", "b", "c", "d", "e"] {
        db.put(&cf, k.as_bytes(), b"v", Duration::ZERO).unwrap();
    }
    let mut t = db.begin();
    let mut it = t.new_iterator(&cf);
    let mut fwd = Vec::new();
    it.seek_to_first();
    while it.valid() {
        fwd.push(String::from_utf8(it.key().to_vec()).unwrap());
        it.next();
    }
    assert_eq!(fwd, vec!["a", "b", "c", "d", "e"]);

    let mut bwd = Vec::new();
    it.seek_to_last();
    while it.valid() {
        bwd.push(String::from_utf8(it.key().to_vec()).unwrap());
        it.prev();
    }
    assert_eq!(bwd, vec!["e", "d", "c", "b", "a"]);
    drop(it);
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn iterator_snapshot_consistent_under_concurrent_writes() {
    // The lazy memtable iterator reads the live skip lists, so it can physically
    // observe entries inserted AFTER the snapshot. The public Iterator's read_seq
    // filter must hide every such entry. We overwrite existing keys and add new
    // ones (all at higher seqs) into the same unflushed memtable mid-scan and
    // assert the scan still sees exactly the pre-snapshot state.
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for i in 0..50u64 {
        db.put(&cf, format!("k{i:03}").as_bytes(), b"old", Duration::ZERO)
            .unwrap();
    }

    // Snapshot is pinned here (db.begin() == Snapshot isolation).
    let mut t = db.begin();
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();

    // Mutate the SAME active memtable at higher sequence numbers: overwrite every
    // existing key, then append 50 brand-new keys.
    for i in 0..100u64 {
        db.put(&cf, format!("k{i:03}").as_bytes(), b"new", Duration::ZERO)
            .unwrap();
    }

    let mut seen = Vec::new();
    while it.valid() {
        seen.push((
            String::from_utf8(it.key().to_vec()).unwrap(),
            String::from_utf8(it.value().to_vec()).unwrap(),
        ));
        it.next();
    }
    drop(it);

    let expected: Vec<(String, String)> = (0..50u64)
        .map(|i| (format!("k{i:03}"), "old".to_string()))
        .collect();
    assert_eq!(
        seen, expected,
        "iterator must see exactly the pre-snapshot keys/values, no seq>read_seq leakage"
    );
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn iterator_values_inline_and_vlog() {
    // Verify the merge iterator returns correct values after lazy-value capture,
    // for both inline (small) and vlog-separated (large) values, from SSTables.
    let dir = tempfile::tempdir().unwrap();
    let cfg = ColumnFamilyConfig {
        klog_value_threshold: 64, // values >= 64B go to the vlog
        ..ColumnFamilyConfig::default()
    };
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("v", cfg).unwrap();

    let small = b"small".to_vec();
    let big = vec![b'B'; 4096]; // forced into the vlog
    db.put(&cf, b"k-small", &small, Duration::ZERO).unwrap();
    db.put(&cf, b"k-big", &big, Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap(); // land in an SSTable (on-disk path)

    let mut t = db.begin();
    let mut it = t.new_iterator(&cf);
    let mut seen = std::collections::HashMap::new();
    it.seek_to_first();
    while it.valid() {
        seen.insert(it.key().to_vec(), it.value().to_vec());
        it.next();
    }
    assert!(it.err().is_none());
    assert_eq!(seen.get(b"k-small".as_slice()), Some(&small));
    assert_eq!(seen.get(b"k-big".as_slice()), Some(&big));
    drop(it);
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn multi_cf_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let a = db
        .create_column_family("a", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family("b", ColumnFamilyConfig::default())
        .unwrap();
    let mut t = db.begin();
    t.put(&a, b"k", b"va", Duration::ZERO).unwrap();
    t.put(&b, b"k", b"vb", Duration::ZERO).unwrap();
    t.commit().unwrap();
    assert_eq!(db.get(&a, b"k").unwrap(), b"va");
    assert_eq!(db.get(&b, b"k").unwrap(), b"vb");
    db.close().unwrap();
}

#[test]
fn savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    let mut t = db.begin();
    t.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    t.set_savepoint("sp").unwrap();
    t.put(&cf, b"b", b"2", Duration::ZERO).unwrap();
    t.rollback_to_savepoint("sp").unwrap();
    assert_eq!(t.get(&cf, b"a").unwrap(), b"1");
    assert!(t.get(&cf, b"b").is_err());
    t.commit().unwrap();
    assert_eq!(db.get(&cf, b"a").unwrap(), b"1");
    assert!(db.get(&cf, b"b").is_err());
    db.close().unwrap();
}

#[test]
fn persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = open(dir.path());
        for i in 0..1000u32 {
            db.put(
                &cf,
                format!("key{i:05}").as_bytes(),
                b"value",
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.close().unwrap();
    }
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf survives reopen");
    for i in 0..1000u32 {
        assert_eq!(
            db.get(&cf, format!("key{i:05}").as_bytes()).unwrap(),
            b"value",
            "key{i} after reopen"
        );
    }
    db.close().unwrap();
}

#[test]
fn flush_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ColumnFamilyConfig {
        write_buffer_size: 64 * 1024, // small to force flushes
        l1_file_count_trigger: 2,
        ..ColumnFamilyConfig::default()
    };
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", cfg).unwrap();

    let n = 20_000u32;
    for i in 0..n {
        db.put(
            &cf,
            format!("k{i:08}").as_bytes(),
            b"some-value-payload",
            Duration::ZERO,
        )
        .unwrap();
    }
    // Give background flush/compaction a moment.
    std::thread::sleep(Duration::from_millis(200));
    for i in 0..n {
        assert_eq!(
            db.get(&cf, format!("k{i:08}").as_bytes()).unwrap(),
            b"some-value-payload"
        );
    }
    db.close().unwrap();
}

#[test]
fn btree_column_family_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ColumnFamilyConfig {
        use_btree: true,
        write_buffer_size: 64 * 1024, // force flushes -> B+tree SSTables
        ..ColumnFamilyConfig::default()
    };
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("bt", cfg).unwrap();
        for i in 0..10_000u32 {
            db.put(&cf, format!("k{i:06}").as_bytes(), b"value", Duration::ZERO)
                .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        for i in (0..10_000u32).step_by(13) {
            assert_eq!(
                db.get(&cf, format!("k{i:06}").as_bytes()).unwrap(),
                b"value"
            );
        }
        db.close().unwrap();
    }
    // Reopen: B+tree SSTables must be readable from the manifest.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("bt").unwrap();
    assert_eq!(db.get(&cf, b"k005000").unwrap(), b"value");
    // Range scan returns everything in order.
    let mut t = db.begin();
    let mut it = t.new_iterator(&cf);
    let mut count = 0u32;
    it.seek_to_first();
    while it.valid() {
        count += 1;
        it.next();
    }
    assert_eq!(count, 10_000);
    drop(it);
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn concurrent_manifest_writes_survive_reopen() {
    // Drive many concurrent flushes + compactions across several CFs so multiple
    // flush workers and the compaction worker call persist_manifest at once. Before
    // manifest writes were serialized this raced on the shared temp file and could
    // publish a torn MANIFEST that fails its CRC on reopen (whole-DB loss). Here we
    // assert the DB reopens and every key survives.
    let dir = tempfile::tempdir().unwrap();
    let cfg = || ColumnFamilyConfig {
        write_buffer_size: 32 * 1024, // tiny -> frequent flushes
        l1_file_count_trigger: 2,     // frequent compactions
        ..ColumnFamilyConfig::default()
    };
    let cf_names = ["a", "b", "c", "d"];
    {
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.num_flush_threads = 4;
        let db = Arc::new(DB::open(opts).unwrap());
        let cfs: Vec<_> = cf_names
            .iter()
            .map(|n| db.create_column_family(n, cfg()).unwrap())
            .collect();

        let mut handles = Vec::new();
        for (ci, cf) in cfs.iter().cloned().enumerate() {
            let db = db.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..5000u32 {
                    let k = format!("cf{ci}-k{i:06}");
                    db.put(&cf, k.as_bytes(), b"payload-value", Duration::ZERO)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        db.close().unwrap();
    }

    // Reopen must succeed (manifest decodes) and all data must be present.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    for (ci, name) in cf_names.iter().enumerate() {
        let cf = db
            .get_column_family(name)
            .unwrap_or_else(|| panic!("cf {name} survives reopen"));
        for i in 0..5000u32 {
            let k = format!("cf{ci}-k{i:06}");
            assert_eq!(
                db.get(&cf, k.as_bytes()).unwrap(),
                b"payload-value",
                "missing {k} after reopen"
            );
        }
    }
    db.close().unwrap();
}

#[test]
fn concurrent_writers() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    let db = Arc::new(db);
    let mut handles = Vec::new();
    for t in 0..8u32 {
        let db = db.clone();
        let cf = cf.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..2000u32 {
                let k = format!("t{t}-k{i:05}");
                db.put(&cf, k.as_bytes(), b"v", Duration::ZERO).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    for t in 0..8u32 {
        for i in 0..2000u32 {
            let k = format!("t{t}-k{i:05}");
            assert_eq!(db.get(&cf, k.as_bytes()).unwrap(), b"v", "missing {k}");
        }
    }
    db.close().unwrap();
}

#[test]
fn lock_file_excludes_second_writer_but_shares_readers() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();

    // A second read-write open of a live database must fail with Locked.
    match DB::open(Options::new(dir.path().to_str().unwrap())) {
        Err(ondadb::OndaError::Locked(_)) => {}
        other => panic!("expected Locked, got {other:?}"),
    }
    db.close().unwrap();
    drop(cf);
    drop(db);

    // After a clean close the lock is released: read-only opens take a shared
    // lock, so two of them coexist...
    let ro = |ro: bool| {
        let mut o = Options::new(dir.path().to_str().unwrap());
        o.read_only = ro;
        DB::open(o)
    };
    let r1 = ro(true).unwrap();
    let r2 = ro(true).unwrap();
    assert_eq!(r1.get(&r1.get_column_family("default").unwrap(), b"k").unwrap(), b"v");

    // ...but a writer is excluded while any reader holds the shared lock.
    match ro(false) {
        Err(ondadb::OndaError::Locked(_)) => {}
        other => panic!("expected Locked while readers live, got {other:?}"),
    }

    r1.close().unwrap();
    r2.close().unwrap();
    drop(r1);
    drop(r2);

    // All handles released: a writer can open again.
    let db = ro(false).unwrap();
    db.close().unwrap();
}
