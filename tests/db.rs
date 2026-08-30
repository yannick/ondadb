//! End-to-end database tests

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, OndaError, Options, DB};

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
fn per_cf_multi_cf_commit_is_rejected_without_partial_apply() {
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
    let err = t
        .commit()
        .expect_err("per-CF WALs cannot commit atomically");
    assert!(matches!(
        err,
        OndaError::InvalidArgs(ref message)
            if message == "multi-column-family transactions require unified_memtable=true for atomic commit"
    ));
    assert!(matches!(db.get(&a, b"k"), Err(OndaError::NotFound)));
    assert!(matches!(db.get(&b, b"k"), Err(OndaError::NotFound)));
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
fn serializable_savepoint_rollback_discards_later_reads() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"later", b"old", Duration::ZERO).unwrap();

    let mut original = db.begin_with_isolation(IsolationLevel::Serializable);
    original.set_savepoint("before-read").unwrap();
    assert_eq!(original.get(&cf, b"later").unwrap(), b"old");
    original.rollback_to_savepoint("before-read").unwrap();

    db.put(&cf, b"later", b"new", Duration::ZERO).unwrap();
    original
        .put(&cf, b"unrelated", b"value", Duration::ZERO)
        .unwrap();
    original
        .commit()
        .expect("a rolled-back read must not remain in Serializable validation");
    assert_eq!(db.get(&cf, b"unrelated").unwrap(), b"value");
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
    assert_eq!(
        r1.get(&r1.get_column_family("default").unwrap(), b"k")
            .unwrap(),
        b"v"
    );

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

#[test]
fn sync_wal_durability_point() {
    // Default CFs run SyncMode::None; sync_wal() is the explicit fsync.
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    db.sync_wal().unwrap();
    db.sync_wal().unwrap(); // idempotent
    assert_eq!(db.get(&cf, b"a").unwrap(), b"1");
    db.close().unwrap();
    drop(cf);
    drop(db);

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"a").unwrap(), b"1");
    db.close().unwrap();
}

#[test]
fn clear_column_family_empties_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    // Data both flushed to SSTables and live in the memtable.
    for i in 0..100u32 {
        db.put(&cf, format!("k{i:03}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.put(&cf, b"in-memtable", b"v", Duration::ZERO).unwrap();

    let cf = db.clear_column_family("default").unwrap();
    assert!(db.get(&cf, b"k000").is_err());
    assert!(db.get(&cf, b"in-memtable").is_err());

    // The cleared CF is immediately writable and the clear is durable.
    db.put(&cf, b"after", b"1", Duration::ZERO).unwrap();
    db.close().unwrap();
    drop(cf);
    drop(db);

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf survives clear");
    assert!(db.get(&cf, b"k000").is_err());
    assert_eq!(db.get(&cf, b"after").unwrap(), b"1");
    assert!(matches!(
        db.clear_column_family("missing"),
        Err(ondadb::OndaError::NotFound)
    ));
    db.close().unwrap();
}

#[test]
fn bulk_ingestion_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());

    // Pre-existing data that ingestion must shadow / coexist with.
    db.put(&cf, b"k00500", b"old", Duration::ZERO).unwrap();
    db.put(&cf, b"pre", b"kept", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    let mut ing = db.start_ingestion(&cf).unwrap();
    for i in 0..10_000u32 {
        ing.write(format!("k{i:05}").as_bytes(), b"ingested", Duration::ZERO)
            .unwrap();
    }
    // Out-of-order write must be rejected without corrupting the stream.
    assert!(ing.write(b"k00000", b"dup", Duration::ZERO).is_err());
    assert_eq!(ing.finish().unwrap(), 10_000);

    // Ingested data visible, newer than the pre-existing version.
    assert_eq!(db.get(&cf, b"k00000").unwrap(), b"ingested");
    assert_eq!(db.get(&cf, b"k00500").unwrap(), b"ingested");
    assert_eq!(db.get(&cf, b"pre").unwrap(), b"kept");

    // Durable across reopen (no WAL involved — manifest only).
    db.close().unwrap();
    drop(cf);
    drop(db);
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"k09999").unwrap(), b"ingested");
    assert_eq!(db.get(&cf, b"k00500").unwrap(), b"ingested");

    // Iteration sees exactly 10_000 ingested keys + "pre".
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    let mut n = 0;
    it.seek_to_first();
    while it.valid() {
        n += 1;
        it.next();
    }
    assert_eq!(n, 10_001);
    db.close().unwrap();
}

#[test]
fn bulk_ingestion_abort_leaves_no_trace() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    {
        let mut ing = db.start_ingestion(&cf).unwrap();
        for i in 0..1000u32 {
            ing.write(format!("k{i:04}").as_bytes(), b"v", Duration::ZERO)
                .unwrap();
        }
        // dropped without finish
    }
    assert!(db.get(&cf, b"k0000").is_err());
    // Ingestion tombstones shadow existing keys.
    db.put(&cf, b"gone", b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    let mut ing = db.start_ingestion(&cf).unwrap();
    ing.write_tombstone(b"gone").unwrap();
    ing.finish().unwrap();
    assert!(db.get(&cf, b"gone").is_err());
    db.close().unwrap();
}

#[test]
fn per_level_compression_end_to_end() {
    use ondadb::Compression;
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    // L0 uncompressed, everything deeper Zstd.
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                compression_per_level: vec![Compression::None, Compression::Zstd],
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();

    // Compressible values so a Zstd level actually exercises the codec.
    let value = "abcdefgh".repeat(32);
    for i in 0..2000u32 {
        db.put(
            &cf,
            format!("k{i:05}").as_bytes(),
            value.as_bytes(),
            Duration::ZERO,
        )
        .unwrap();
    }
    db.flush_memtable(&cf).unwrap(); // L0 (None)
    db.compact(&cf).unwrap(); // pushes down => Zstd blocks

    for i in (0..2000u32).step_by(37) {
        assert_eq!(
            db.get(&cf, format!("k{i:05}").as_bytes()).unwrap(),
            value.as_bytes()
        );
    }
    db.close().unwrap();
    drop(cf);
    drop(db);

    // Policy is persisted in the manifest: after reopen the data reads back
    // and new writes keep working.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"k00000").unwrap(), value.as_bytes());
    db.put(&cf, b"new", b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(db.get(&cf, b"new").unwrap(), b"v");
    db.close().unwrap();
}

#[test]
fn bottom_level_compaction_cuts_at_partition_boundaries() {
    use ondadb::manifest::{manifest_path, Manifest};
    use ondadb::PartitionRule;

    let dir = tempfile::tempdir().unwrap();
    // Small write buffer so modest data forms several levels and forces a real
    // bottom-level compaction.
    let cfg = ColumnFamilyConfig {
        write_buffer_size: 8 << 10,
        partition_rules: vec![
            PartitionRule {
                prefix: b"a/".to_vec(),
                name: "alpha".into(),
            },
            PartitionRule {
                prefix: b"b/".to_vec(),
                name: "beta".into(),
            },
        ],
        ..ColumnFamilyConfig::default()
    };
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", cfg.clone()).unwrap();

    // Write across three partitions: a/ (alpha), b/ (beta), and c/ (un-ruled ->
    // implicit default partition, resolves to None). Interleave and flush often
    // so upper levels genuinely mix partitions before compaction separates them.
    let value = vec![b'v'; 256];
    for i in 0..300u32 {
        for p in ["a", "b", "c"] {
            db.put(
                &cf,
                format!("{p}/{i:05}").as_bytes(),
                &value,
                Duration::ZERO,
            )
            .unwrap();
        }
        if i % 40 == 39 {
            db.flush_memtable(&cf).unwrap();
        }
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    db.close().unwrap();
    drop(cf);
    drop(db);

    // Inspect the persisted catalog: every table at the bottom level must sit
    // entirely within one partition, and its stamped `partition` must match.
    let m = Manifest::load(manifest_path(dir.path())).unwrap();
    let cfm = m.cfs.iter().find(|c| c.name == "default").unwrap();
    let bottom = cfm.sstables.iter().map(|s| s.level).max().unwrap();
    assert!(
        bottom >= 1,
        "expected data pushed below L0, bottom = {bottom}"
    );

    let mut seen_alpha = false;
    let mut seen_beta = false;
    let mut seen_default = false;
    for s in cfm.sstables.iter().filter(|s| s.level == bottom) {
        let pmin = cfg.partition_of(&s.min_key).map(str::to_string);
        let pmax = cfg.partition_of(&s.max_key).map(str::to_string);
        assert_eq!(
            pmin, pmax,
            "bottom table {} spans a partition boundary: {:?}..{:?}",
            s.id, s.min_key, s.max_key
        );
        assert_eq!(
            s.partition, pmin,
            "bottom table {} has wrong stamped partition",
            s.id
        );
        match s.partition.as_deref() {
            Some("alpha") => seen_alpha = true,
            Some("beta") => seen_beta = true,
            None => seen_default = true,
            other => panic!("unexpected partition {other:?}"),
        }
    }
    assert!(
        seen_alpha && seen_beta && seen_default,
        "expected all three partitions represented at the bottom (alpha={seen_alpha}, beta={seen_beta}, default={seen_default})"
    );
}

#[test]
fn perf_memtable_only_get() {
    // A key that never left the active memtable must show exactly the memtable
    // probes the layout implies and no SSTable work at all.
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();

    let scope = ondadb::perf::enter();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
    let p = scope.finish();

    // Per-CF layout: one probe of the active memtable, no sealed memtables.
    assert_eq!(p.memtable_probes, 1);
    assert_eq!(p.bloom_probes, 0);
    assert_eq!(p.bloom_negatives, 0);
    assert_eq!(p.sstable_probes, 0);
    assert_eq!(p.index_seeks, 0);
    assert_eq!(p.block_cache_hits, 0);
    assert_eq!(p.block_misses, 0);
    assert_eq!(p.block_read_bytes, 0);
    db.close().unwrap();
}

/// A CF whose L0 files are never auto-compacted, so a test controls exactly how
/// many SSTables a point read must consider.
fn perf_cf(db: &DB, name: &str) -> Arc<ColumnFamily> {
    db.create_column_family(
        name,
        ColumnFamilyConfig {
            l1_file_count_trigger: 1 << 20,
            ..ColumnFamilyConfig::default()
        },
    )
    .unwrap()
}

#[test]
fn perf_get_missing_through_n_sstables() {
    const N: u64 = 4;
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = perf_cf(&db, "n");
    // Every table spans "aaa".."zzz", so all N are candidates for "mmm".
    for i in 0..N {
        db.put(&cf, b"aaa", format!("{i}").as_bytes(), Duration::ZERO)
            .unwrap();
        db.put(&cf, b"zzz", format!("{i}").as_bytes(), Duration::ZERO)
            .unwrap();
        db.flush_memtable(&cf).unwrap();
    }

    let scope = ondadb::perf::enter();
    assert!(db.get(&cf, b"mmm").is_err());
    let p = scope.finish();

    assert_eq!(p.bloom_probes, N, "every candidate table is filtered once");
    // Robust to the filter's false-positive rate: whatever the filter admits is
    // exactly what gets probed.
    assert_eq!(p.sstable_probes, p.bloom_probes - p.bloom_negatives);
    assert!(p.bloom_negatives <= N);
    // Block work happens only for the tables the filter let through.
    assert_eq!(p.index_seeks, p.sstable_probes);
    if p.sstable_probes == 0 {
        assert_eq!(p.block_misses, 0);
        assert_eq!(p.block_cache_hits, 0);
        assert_eq!(p.block_read_bytes, 0);
    }
    db.close().unwrap();
}

#[test]
fn perf_block_cache_hit_on_second_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = perf_cf(&db, "c");
    db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    let cold = ondadb::perf::enter();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
    let cold = cold.finish();
    let warm = ondadb::perf::enter();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
    let warm = warm.finish();

    assert_eq!(cold.sstable_probes, 1);
    assert!(cold.block_read_bytes > 0, "the block had to come from disk");
    if cfg!(feature = "mmap-reads") {
        // Uncompressed blocks are served as zero-copy views into the mmap: the
        // bytes are still counted, but the block cache is never consulted.
        assert_eq!(cold.block_misses, 0);
        assert_eq!(cold.block_cache_hits, 0);
        assert_eq!(warm.block_cache_hits, 0);
        assert_eq!(warm.block_misses, 0);
        assert_eq!(warm.block_read_bytes, cold.block_read_bytes);
    } else {
        assert_eq!(cold.block_misses, 1);
        assert_eq!(cold.block_cache_hits, 0);
        assert_eq!(warm.block_cache_hits, 1);
        assert_eq!(warm.block_misses, 0);
        assert_eq!(warm.block_read_bytes, 0, "a cache hit reads no bytes");
    }
    db.close().unwrap();
}

#[test]
fn perf_counts_vlog_reads_for_separated_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "v",
            ColumnFamilyConfig {
                klog_value_threshold: 64, // values >= 64B are separated
                l1_file_count_trigger: 1 << 20,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let big = vec![b'B'; 4096];
    db.put(&cf, b"k-big", &big, Duration::ZERO).unwrap();
    db.put(&cf, b"k-small", b"small", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    let scope = ondadb::perf::enter();
    assert_eq!(db.get(&cf, b"k-big").unwrap(), big);
    let separated = scope.finish();
    assert_eq!(separated.vlog_reads, 1);
    assert_eq!(separated.vlog_read_bytes, big.len() as u64);

    let scope = ondadb::perf::enter();
    assert_eq!(db.get(&cf, b"k-small").unwrap(), b"small");
    let inline = scope.finish();
    assert_eq!(inline.vlog_reads, 0, "an inline value touches no vlog");
    assert_eq!(inline.vlog_read_bytes, 0);
    db.close().unwrap();
}

#[test]
fn perf_iterator_walk_counts_steps() {
    const ENTRIES: u64 = 32;
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = perf_cf(&db, "i");
    for i in 0..ENTRIES {
        db.put(&cf, format!("k{i:04}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    // One deleted key: a tombstone group is resolved but never surfaced.
    db.put(&cf, b"k9999", b"v", Duration::ZERO).unwrap();
    db.delete(&cf, b"k9999").unwrap();
    db.flush_memtable(&cf).unwrap();

    let mut t = db.begin();
    let mut it = t.new_iterator(&cf);
    let scope = ondadb::perf::enter();
    let mut seen = 0u64;
    it.seek_to_first();
    while it.valid() {
        seen += 1;
        it.next();
    }
    assert!(it.err().is_none());
    let p = scope.finish();
    assert_eq!(seen, ENTRIES);
    assert_eq!(p.iterator_steps, ENTRIES, "one step per surfaced group");
    assert_eq!(p.iterator_seeks, 1, "`next` is not a seek");
    drop(it);
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn get_with_perf_matches_plain_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = perf_cf(&db, "p");
    db.put(&cf, b"hit", b"v", Duration::ZERO).unwrap();
    db.put(&cf, b"gone", b"v", Duration::ZERO).unwrap();
    db.delete(&cf, b"gone").unwrap();
    db.put(&cf, b"stale", b"v", Duration::from_millis(50))
        .unwrap();
    // Half the keys resolve from an SSTable, half from the memtable.
    db.flush_memtable(&cf).unwrap();
    db.put(&cf, b"hit2", b"v2", Duration::ZERO).unwrap();
    std::thread::sleep(Duration::from_millis(80));

    for key in [
        b"hit".as_slice(),
        b"hit2".as_slice(),
        b"missing".as_slice(),
        b"gone".as_slice(),
        b"stale".as_slice(),
    ] {
        let plain = db.get(&cf, key);
        let (measured, perf) = db.get_with_perf(&cf, key);
        match (&plain, &measured) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "value for {key:?}"),
            (Err(a), Err(b)) => assert_eq!(
                std::mem::discriminant(a),
                std::mem::discriminant(b),
                "error for {key:?}"
            ),
            _ => panic!("get and get_with_perf disagreed on {key:?}"),
        }
        assert!(perf.memtable_probes >= 1, "the read path was measured");

        // The transactional entry point behaves the same way.
        let mut t = db.begin();
        let (in_txn, txn_perf) = t.get_with_perf(&cf, key);
        assert_eq!(in_txn.is_ok(), plain.is_ok(), "txn result for {key:?}");
        if let (Ok(a), Ok(b)) = (&plain, &in_txn) {
            assert_eq!(a, b);
        }
        assert!(txn_perf.memtable_probes >= 1);
        t.rollback().unwrap();
    }
    db.close().unwrap();
}

#[test]
fn iterator_perf_scope_measures_a_walk() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for k in ["a", "b", "c"] {
        db.put(&cf, k.as_bytes(), b"v", Duration::ZERO).unwrap();
    }
    let mut t = db.begin();
    let mut it = t.new_iterator(&cf);
    let scope = it.perf_scope();
    it.seek_to_first();
    while it.valid() {
        it.next();
    }
    let p = scope.finish();
    assert_eq!(p.iterator_seeks, 1);
    assert_eq!(p.iterator_steps, 3);
    drop(it);
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn iterator_counters_are_thread_affine() {
    // `Iterator` is `Send`, and this test pins the documented consequence:
    // counters follow the *thread doing the work*, not the iterator. Moving a
    // walk to another thread is allowed; it just stops being measured by the
    // scope left open behind it.
    fn assert_send<T: Send>() {}
    assert_send::<ondadb::Iterator>();

    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for k in ["a", "b", "c", "d"] {
        db.put(&cf, k.as_bytes(), b"v", Duration::ZERO).unwrap();
    }
    let mut t = db.begin();
    let mut it = t.new_iterator(&cf);

    let scope = ondadb::perf::enter();
    it.seek_to_first(); // one seek plus the first group, on this thread
    assert!(it.valid());

    let walked = std::thread::spawn(move || {
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        n
    })
    .join()
    .unwrap();
    let p = scope.finish();

    assert_eq!(walked, 4, "the moved iterator kept working");
    assert_eq!(p.iterator_seeks, 1);
    assert_eq!(
        p.iterator_steps, 1,
        "work done on another thread must not grow this thread's context"
    );
    t.rollback().unwrap();
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// 0.9 — keyspace-tailing iterator
// ---------------------------------------------------------------------------

/// Format a tail test key so byte order matches numeric order.
fn tk(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Drain the current segment, appending every yielded key.
fn drain(tail: &mut ondadb::TailingIterator, out: &mut Vec<Vec<u8>>) {
    while tail.valid() {
        out.push(tail.key().to_vec());
        tail.next();
    }
}

#[test]
fn tailing_iterator_walks_a_static_keyspace() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    // Half the keyspace in SSTables, half in the memtable, so the tail has to
    // merge the same sources a normal snapshot iterator does.
    for i in 0..200 {
        db.put(&cf, &tk(i), format!("v{i}").as_bytes(), Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    for i in 200..300 {
        db.put(&cf, &tk(i), format!("v{i}").as_bytes(), Duration::ZERO)
            .unwrap();
    }

    let txn = db.begin();
    let mut reference = Vec::new();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    while it.valid() {
        reference.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    assert!(it.err().is_none());
    assert_eq!(reference.len(), 300);

    let mut tail = db.new_tailing_iterator(&cf);
    let mut observed = Vec::new();
    tail.seek_to_first();
    while tail.valid() {
        observed.push((tail.key().to_vec(), tail.value().to_vec()));
        tail.next();
    }
    assert!(tail.err().is_none());
    assert_eq!(observed, reference);
    // Nothing was written since the tail was built, so an exhausted tail has
    // nothing to refresh to.
    assert!(!tail.refresh());
    db.close().unwrap();
}

#[test]
fn refresh_yields_only_strictly_greater_keys() {
    const SEED: u32 = 50;
    const APPENDED: u32 = 500;

    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    let db = Arc::new(db);
    for i in 0..SEED {
        db.put(&cf, &tk(i), b"v", Duration::ZERO).unwrap();
    }

    let mut tail = db.new_tailing_iterator(&cf);
    tail.seek_to_first();

    let writer = {
        let db = db.clone();
        let cf = cf.clone();
        std::thread::spawn(move || {
            for i in SEED..SEED + APPENDED {
                db.put(&cf, &tk(i), b"v", Duration::ZERO).unwrap();
            }
        })
    };

    let total = (SEED + APPENDED) as usize;
    let mut observed: Vec<Vec<u8>> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        drain(&mut tail, &mut observed);
        assert!(tail.err().is_none(), "{:?}", tail.err());
        if observed.len() == total {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "tail stalled at {} of {total}",
            observed.len()
        );
        if !tail.refresh() {
            std::thread::yield_now();
        }
    }
    writer.join().unwrap();

    for pair in observed.windows(2) {
        assert!(
            pair[0] < pair[1],
            "tail went backwards or repeated: {:?} then {:?}",
            String::from_utf8_lossy(&pair[0]),
            String::from_utf8_lossy(&pair[1])
        );
    }
    let expected: Vec<Vec<u8>> = (0..SEED + APPENDED).map(tk).collect();
    assert_eq!(observed, expected);
    // The whole point: far fewer iterator constructions than yielded entries.
    assert!(
        tail.segments() < total as u64,
        "{} segments for {total} entries",
        tail.segments()
    );
    drop(tail);
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

#[test]
fn refresh_is_noop_mid_segment() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for i in 0..10 {
        db.put(&cf, &tk(i), b"v", Duration::ZERO).unwrap();
    }
    let mut tail = db.new_tailing_iterator(&cf);
    tail.seek_to_first();
    tail.next();
    assert!(tail.valid());

    // Advance the visible floor while entries remain unread.
    db.put(&cf, &tk(99), b"v", Duration::ZERO).unwrap();
    let before = tail.segments();
    assert!(!tail.refresh(), "mid-segment refresh must not rebuild");
    assert_eq!(tail.segments(), before);

    // The same segment continues from where it stood (k000001), at its own
    // snapshot: k000099 is not in it.
    let mut rest = Vec::new();
    drain(&mut tail, &mut rest);
    assert_eq!(rest, (1..10).map(tk).collect::<Vec<_>>());
    db.close().unwrap();
}

#[test]
fn refresh_is_noop_when_floor_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for i in 0..10 {
        db.put(&cf, &tk(i), b"v", Duration::ZERO).unwrap();
    }
    let mut tail = db.new_tailing_iterator(&cf);
    tail.seek_to_first();
    let mut all = Vec::new();
    drain(&mut tail, &mut all);
    assert_eq!(all.len(), 10);

    let before = tail.segments();
    assert!(!tail.refresh());
    assert!(!tail.refresh());
    assert_eq!(
        tail.segments(),
        before,
        "an unchanged floor must not rebuild"
    );
    db.close().unwrap();
}

#[test]
fn tail_never_observes_updates_behind_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for i in 0..5 {
        db.put(&cf, &tk(i), b"v", Duration::ZERO).unwrap();
    }
    let mut tail = db.new_tailing_iterator(&cf);
    tail.seek_to_first();
    let mut observed = Vec::new();
    drain(&mut tail, &mut observed);
    assert_eq!(observed.len(), 5);

    // Update a key the tail already passed: never re-yielded.
    db.put(&cf, &tk(2), b"updated", Duration::ZERO).unwrap();
    tail.refresh();
    drain(&mut tail, &mut observed);
    assert_eq!(observed.len(), 5, "an update behind the cursor re-surfaced");

    // Delete it: also never yielded (this is not a change feed).
    db.delete(&cf, &tk(2)).unwrap();
    tail.refresh();
    drain(&mut tail, &mut observed);
    assert_eq!(observed.len(), 5, "a delete behind the cursor surfaced");
    assert!(tail.err().is_none());

    // The change itself is real — a fresh reader sees it.
    assert!(db.get(&cf, &tk(2)).is_err());
    db.close().unwrap();
}

#[test]
fn tail_never_observes_inserts_behind_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for i in [1u32, 2, 4, 5] {
        db.put(&cf, &tk(i), b"v", Duration::ZERO).unwrap();
    }
    let mut tail = db.new_tailing_iterator(&cf);
    tail.seek_to_first();
    let mut observed = Vec::new();
    drain(&mut tail, &mut observed);
    assert_eq!(observed, [1u32, 2, 4, 5].map(tk).to_vec());

    db.put(&cf, &tk(3), b"late", Duration::ZERO).unwrap();
    tail.refresh();
    drain(&mut tail, &mut observed);
    assert_eq!(
        observed.len(),
        4,
        "a key inserted behind the cursor must stay invisible to this tail"
    );

    // Visible to anyone who starts fresh.
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut fresh = Vec::new();
    while it.valid() {
        fresh.push(it.key().to_vec());
        it.next();
    }
    assert_eq!(fresh, [1u32, 2, 3, 4, 5].map(tk).to_vec());
    db.close().unwrap();
}

#[test]
fn tail_surfaces_iterator_construction_failure() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    for i in 0..20 {
        db.put(&cf, &tk(i), b"v", Duration::ZERO).unwrap();
    }

    // Drain the first segment from the memtable alone, so no SSTable reader is
    // open when the table below is destroyed.
    let mut tail = db.new_tailing_iterator(&cf);
    tail.seek_to_first();
    let mut observed = Vec::new();
    drain(&mut tail, &mut observed);
    assert_eq!(observed.len(), 20);

    // k000999 sits above the cursor, so the flushed table cannot be pruned out
    // of the next segment by its lower bound.
    db.put(&cf, &tk(999), b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    // Destroy the table behind the manifest's back. Readers are opened lazily,
    // so the rebuild below is the first attempt to open this one.
    let klog = std::fs::read_dir(dir.path().join("cf-default"))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "klog"))
        .expect("flush created a klog");
    std::fs::write(&klog, b"not an sstable").unwrap();

    assert!(!tail.refresh(), "a failed rebuild is not a valid segment");
    assert!(!tail.valid());
    assert!(
        tail.err().is_some(),
        "a tail that cannot open a table must report it, not return a short answer"
    );
    let _ = db.close();
}

/// The vlog value cache limit is persisted per family (feature 0.5). A setting
/// that silently reverts on the first reopen is worse than no setting at all:
/// the tuning appears to hold for the life of the process and then vanishes.
#[test]
fn vlog_value_cache_limit_round_trips_through_reopen() {
    assert_eq!(
        ColumnFamilyConfig::default().max_cached_vlog_value_bytes,
        0,
        "the cache must be off unless a family opts in"
    );

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.create_column_family(
        "tuned",
        ColumnFamilyConfig {
            max_cached_vlog_value_bytes: 1 << 20,
            ..ColumnFamilyConfig::default()
        },
    )
    .unwrap();
    db.create_column_family("plain", ColumnFamilyConfig::default())
        .unwrap();
    drop(db);

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(
        db.column_family_config("tuned")
            .unwrap()
            .max_cached_vlog_value_bytes,
        1 << 20
    );
    assert_eq!(
        db.column_family_config("plain")
            .unwrap()
            .max_cached_vlog_value_bytes,
        0,
        "a family that never set the field must recover the default"
    );
}

// ---------------------------------------------------------------------------
// 0.4 — MultiGet batched point lookups
// ---------------------------------------------------------------------------

/// The oracle every batch assertion is measured against: N sequential `get`s at
/// one fixed read sequence. A `Snapshot` transaction pins that sequence, so the
/// comparison is exact even if another thread commits between the two shapes.
fn oracle_multi_get(
    t: &mut ondadb::Txn,
    cf: &Arc<ColumnFamily>,
    keys: &[&[u8]],
) -> Vec<ondadb::Result<Vec<u8>>> {
    keys.iter().map(|k| t.get(cf, k)).collect()
}

fn assert_same_results(
    got: &[ondadb::Result<Vec<u8>>],
    want: &[ondadb::Result<Vec<u8>>],
    keys: &[&[u8]],
    ctx: &str,
) {
    assert_eq!(got.len(), want.len(), "{ctx}: result count");
    assert_eq!(got.len(), keys.len(), "{ctx}: one result per key");
    for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        match (g, w) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "{ctx}: value for key {:?} at {i}", keys[i]),
            (Err(a), Err(b)) => assert_eq!(
                std::mem::discriminant(a),
                std::mem::discriminant(b),
                "{ctx}: error for key {:?} at {i}: {a:?} vs {b:?}",
                keys[i]
            ),
            _ => panic!(
                "{ctx}: batch and sequential disagreed on key {:?} at {i}: {g:?} vs {w:?}",
                keys[i]
            ),
        }
    }
}

/// A CF that separates large values, so a batch resolves both inline and
/// vlog-resident values.
fn multiget_cf(db: &DB, name: &str, comparator: &str) -> Arc<ColumnFamily> {
    db.create_column_family(
        name,
        ColumnFamilyConfig {
            comparator_name: comparator.into(),
            klog_value_threshold: 64, // exercise both inline and vlog values
            ..ColumnFamilyConfig::default()
        },
    )
    .unwrap()
}

/// Wait for background compaction to create a level below L0. A level only
/// exists once compaction has made it, so a fixture that wants L1 must let the
/// worker run before asking the manual sweep to push into it.
fn wait_for_deep_level(cf: &Arc<ColumnFamily>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while cf
        .stats()
        .levels
        .iter()
        .skip(1)
        .all(|(files, _)| *files == 0)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "compaction never populated a level below L0"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Spread `n` keys over the memtable, L0 and — when `deep` — L1, with
/// overwrites, tombstones, single-deletes, expired TTLs, and values on both
/// sides of the vlog threshold. Returns every key that was ever written.
///
/// `deep` is off in unified-memtable mode: there the shared store decides when
/// to split-flush, so a test cannot demand a level below L0 without racing it.
fn seed_mixed_layout(db: &DB, cf: &Arc<ColumnFamily>, n: usize, deep: bool) -> Vec<Vec<u8>> {
    let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("k{i:05}").into_bytes()).collect();
    let big = vec![b'B'; 512]; // above klog_value_threshold: lands in the vlog

    // Round 1 -> L1: every key, half of them with separated values, flushed in
    // enough chunks to pass `l1_file_count_trigger` so the background worker
    // creates L1; the manual sweep then empties L0 into it.
    let chunk = keys.len().div_ceil(5);
    for (i, k) in keys.iter().enumerate() {
        if i % 2 == 0 {
            db.put(cf, k, &big, Duration::ZERO).unwrap();
        } else {
            db.put(cf, k, format!("v1-{i}").as_bytes(), Duration::ZERO)
                .unwrap();
        }
        if (i + 1) % chunk == 0 {
            db.flush_memtable(cf).unwrap();
        }
    }
    db.flush_memtable(cf).unwrap();
    if deep {
        wait_for_deep_level(cf);
        db.compact(cf).unwrap();
        assert_eq!(cf.l0_file_count(), 0, "the sweep pushes all of L0 down");
    }

    // Round 2 -> L0: overwrites, deletes, single-deletes and short TTLs.
    for (i, k) in keys.iter().enumerate() {
        match i % 7 {
            0 => db
                .put(cf, k, format!("v2-{i}").as_bytes(), Duration::ZERO)
                .unwrap(),
            1 => db.delete(cf, k).unwrap(),
            2 => {
                let mut t = db.begin();
                t.single_delete(cf, k).unwrap();
                t.commit().unwrap();
            }
            3 => db
                .put(
                    cf,
                    k,
                    format!("ttl-{i}").as_bytes(),
                    Duration::from_millis(20),
                )
                .unwrap(),
            _ => {}
        }
    }
    db.flush_memtable(cf).unwrap();

    // Round 3 -> active memtable: a slice of the keys gets a newer version.
    for (i, k) in keys.iter().enumerate() {
        match i % 11 {
            0 => db
                .put(cf, k, format!("v3-{i}").as_bytes(), Duration::ZERO)
                .unwrap(),
            1 => db.delete(cf, k).unwrap(),
            _ => {}
        }
    }
    // Let every short TTL entry expire before anything reads, so a TTL result
    // cannot flip between the batch and the oracle.
    std::thread::sleep(Duration::from_millis(60));
    keys
}

/// Deterministic PRNG (SplitMix64). Local, so the fixture is reproducible
/// without pinning a `rand` version's stream behavior into the assertions.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn multi_get_oracle_case(unified: bool, comparator: &str) {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options {
        unified_memtable: unified,
        // Small enough that the shared store split-flushes into L0 rather than
        // holding the whole fixture in memory.
        unified_memtable_write_buffer_size: 32 * 1024,
        ..Options::new(dir.path().to_str().unwrap())
    })
    .unwrap();
    let cf = multiget_cf(&db, "m", comparator);
    let written = seed_mixed_layout(&db, &cf, 240, !unified);
    // Keys that were never written, so batches mix hits and misses.
    let missing: Vec<Vec<u8>> = (0..40).map(|i| format!("z{i:05}").into_bytes()).collect();

    let mut rng = Rng(0x0D4A_5EED);
    for round in 0..24 {
        let batch_len = 1 + rng.below(70);
        let mut owned: Vec<Vec<u8>> = Vec::with_capacity(batch_len);
        for j in 0..batch_len {
            // A third of the slots repeat an earlier slot verbatim, so every
            // batch carries duplicates that must keep their own positions.
            if j > 0 && rng.below(3) == 0 {
                owned.push(owned[rng.below(j)].clone());
            } else if rng.below(5) == 0 {
                owned.push(missing[rng.below(missing.len())].clone());
            } else {
                owned.push(written[rng.below(written.len())].clone());
            }
        }
        let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();

        let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
        let batched = t.multi_get(&cf, &keys);
        let sequential = oracle_multi_get(&mut t, &cf, &keys);
        t.rollback().unwrap();
        assert_same_results(
            &batched,
            &sequential,
            &keys,
            &format!("unified={unified} cmp={comparator} round={round}"),
        );
    }
    db.close().unwrap();
}

#[test]
fn multi_get_matches_sequential_gets() {
    for unified in [false, true] {
        for comparator in ["memcmp", "reverse"] {
            multi_get_oracle_case(unified, comparator);
        }
    }
}

#[test]
fn multi_get_empty_and_single_key_match_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = multiget_cf(&db, "s", "memcmp");
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.put(&cf, b"b", b"2", Duration::ZERO).unwrap();

    assert!(db.multi_get(&cf, &[]).is_empty(), "empty in, empty out");
    for key in [b"a".as_slice(), b"b".as_slice(), b"missing".as_slice()] {
        let one = db.multi_get(&cf, &[key]);
        assert_eq!(one.len(), 1);
        assert_same_results(&one, &[db.get(&cf, key)], &[key], "batch of one");
    }
    db.close().unwrap();
}

#[test]
fn multi_get_reads_each_block_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    // One big block, so all 32 keys land in it.
    let one_block = |name: &str| {
        let cf = db
            .create_column_family(
                name,
                ColumnFamilyConfig {
                    data_block_size: 1 << 16,
                    l1_file_count_trigger: 1 << 20,
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        for i in 0..32 {
            db.put(&cf, format!("k{i:04}").as_bytes(), b"v", Duration::ZERO)
                .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        assert_eq!(cf.l0_file_count(), 1, "one table for the whole key set");
        cf
    };
    // Two identical, equally cold column families: one measured as a batch, one
    // as a single `get`, so the comparison is not spoiled by a warm cache.
    let cf = one_block("b");
    let solo_cf = one_block("b-solo");
    let owned: Vec<Vec<u8>> = (0..32).map(|i| format!("k{i:04}").into_bytes()).collect();
    let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();

    let cold = ondadb::perf::enter();
    let got = db.multi_get(&cf, &keys);
    let cold = cold.finish();
    assert!(got.iter().all(|r| r.is_ok()), "every key resolves");

    // Whatever the config's block-read mechanism, exactly one data block was
    // materialized for 32 keys, and 31 of the 32 lookups rode along on it.
    assert_eq!(cold.multiget_blocks_deduped, 31);
    assert_eq!(cold.index_seeks, 32, "the index search is still per key");
    if cfg!(feature = "mmap-reads") {
        assert_eq!(cold.block_misses, 0, "uncompressed mmap blocks are views");
    } else {
        assert_eq!(cold.block_misses, 1, "one physical read for 32 keys");
        assert_eq!(cold.block_cache_hits, 0);
    }

    // The whole batch reads no more block bytes than a single `get` of one of
    // its keys — the definition of "one read per distinct block".
    let single = ondadb::perf::enter();
    assert!(db.get(&solo_cf, &owned[0]).is_ok());
    let single = single.finish();
    assert_eq!(
        cold.block_read_bytes, single.block_read_bytes,
        "a cold 32-key batch must read exactly the block bytes one cold get reads"
    );

    // The second batch is served entirely from the cache (or the mmap).
    let warm = ondadb::perf::enter();
    let got = db.multi_get(&cf, &keys);
    let warm = warm.finish();
    assert!(got.iter().all(|r| r.is_ok()));
    assert_eq!(warm.block_misses, 0, "the warm batch fetched nothing");
    if !cfg!(feature = "mmap-reads") {
        assert_eq!(warm.block_cache_hits, 1);
        assert_eq!(warm.block_read_bytes, 0);
    }
    db.close().unwrap();
}

#[test]
fn multiget_blocks_deduped_counts_savings() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    // Tiny blocks plus fat inline values: every key gets its own data block.
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                data_block_size: 64,
                klog_value_threshold: 1 << 20, // keep values inline, so blocks fill
                l1_file_count_trigger: 1 << 20,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let owned: Vec<Vec<u8>> = (0..8).map(|i| format!("k{i:04}").into_bytes()).collect();
    for k in &owned {
        db.put(&cf, k, &[b'v'; 200], Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();

    let scope = ondadb::perf::enter();
    let got = db.multi_get(&cf, &keys);
    let spread = scope.finish();
    assert!(got.iter().all(|r| r.is_ok()));
    assert_eq!(
        spread.multiget_blocks_deduped, 0,
        "N keys in N distinct blocks share nothing"
    );

    // The same key eight times is eight lookups into one block.
    let dup: Vec<&[u8]> = vec![keys[0]; 8];
    let scope = ondadb::perf::enter();
    let got = db.multi_get(&cf, &dup);
    let shared = scope.finish();
    assert!(got.iter().all(|r| r.is_ok()));
    assert_eq!(shared.multiget_blocks_deduped, 7);
    db.close().unwrap();
}

#[test]
fn multi_get_respects_bloom_negatives() {
    const TABLES: u64 = 3;
    const ABSENT: u64 = 16;
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = perf_cf(&db, "bn");
    // Every table spans the whole probed range, so all are position candidates
    // and only the bloom filter can rule them out.
    for i in 0..TABLES {
        db.put(&cf, b"aaa", format!("{i}").as_bytes(), Duration::ZERO)
            .unwrap();
        db.put(&cf, b"zzz", format!("{i}").as_bytes(), Duration::ZERO)
            .unwrap();
        db.flush_memtable(&cf).unwrap();
    }
    let owned: Vec<Vec<u8>> = (0..ABSENT)
        .map(|i| format!("mmm{i:04}").into_bytes())
        .collect();
    let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();

    let before = cf.stats().bloom_skips;
    let scope = ondadb::perf::enter();
    let got = db.multi_get(&cf, &keys);
    let p = scope.finish();
    let after = cf.stats().bloom_skips;
    assert!(got.iter().all(|r| r.is_err()), "no key exists");

    assert_eq!(
        p.bloom_probes,
        ABSENT * TABLES,
        "one filter consultation per (key, candidate table)"
    );
    assert_eq!(
        p.sstable_probes,
        p.bloom_probes - p.bloom_negatives,
        "exactly what the filter admits gets probed"
    );
    assert_eq!(
        after - before,
        p.bloom_negatives,
        "CfStats::bloom_skips grows by the batch's filter negatives"
    );
    assert!(
        p.bloom_negatives >= ABSENT,
        "a batch of absent keys must be mostly filtered out, saw {}",
        p.bloom_negatives
    );
    db.close().unwrap();
}

#[test]
fn multi_get_corrupt_table_errors_only_dependent_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = DB::open(Options::new(&path)).unwrap();
    let cf = perf_cf(&db, "corrupt");
    // One table holds every key...
    for i in 0..8 {
        db.put(&cf, format!("k{i}").as_bytes(), b"old", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let victim = std::fs::read_dir(dir.path().join("cf-corrupt"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|e| e == "klog"))
        .expect("the flushed table");
    db.close().unwrap();

    // ...and its data blocks are then corrupted on disk (the
    // `corrupt_vlog_value_is_detected` pattern, applied to the klog).
    let mut bytes = std::fs::read(&victim).unwrap();
    for b in bytes.iter_mut().take(256).skip(8) {
        *b ^= 0xFF;
    }
    std::fs::write(&victim, &bytes).unwrap();

    let db = DB::open(Options::new(&path)).unwrap();
    let cf = db.get_column_family("corrupt").unwrap();
    // Half the keys get a newer version in the memtable, which shadows the
    // corrupt table entirely.
    for i in 0..4 {
        db.put(&cf, format!("k{i}").as_bytes(), b"new", Duration::ZERO)
            .unwrap();
    }
    let owned: Vec<Vec<u8>> = (0..8).map(|i| format!("k{i}").into_bytes()).collect();
    let keys: Vec<&[u8]> = owned.iter().map(|k| k.as_slice()).collect();
    let got = db.multi_get(&cf, &keys);

    for (i, r) in got.iter().enumerate().take(4) {
        match r {
            Ok(v) => assert_eq!(v.as_slice(), b"new", "k{i}"),
            Err(e) => panic!("k{i} must resolve from the memtable, got {e:?}"),
        }
    }
    for (i, r) in got.iter().enumerate().skip(4) {
        assert!(
            matches!(r, Err(OndaError::Corruption(_))),
            "k{i} needs the corrupt table and must report it, got {r:?}"
        );
    }
    db.close().unwrap();
}

#[test]
fn multi_get_is_snapshot_fixed_during_flush_and_compaction() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(DB::open(Options::new(dir.path().to_str().unwrap())).unwrap());
    let cf = multiget_cf(&db, "snap", "memcmp");
    let written = seed_mixed_layout(&db, &cf, 200, true);
    let keys: Vec<&[u8]> = written.iter().map(|k| k.as_slice()).collect();

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, cf, stop) = (db.clone(), cf.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let k = format!("k{:05}", n % 200);
                db.put(
                    &cf,
                    k.as_bytes(),
                    format!("live-{n}").as_bytes(),
                    Duration::ZERO,
                )
                .unwrap();
                if n.is_multiple_of(64) {
                    db.flush_memtable(&cf).unwrap();
                    db.compact(&cf).unwrap();
                }
                n += 1;
            }
        })
    };

    for _ in 0..8 {
        let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
        let batched = t.multi_get(&cf, &keys);
        let sequential = oracle_multi_get(&mut t, &cf, &keys);
        t.rollback().unwrap();
        assert_same_results(&batched, &sequential, &keys, "concurrent flush/compaction");
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    db.close().unwrap();
}

#[test]
fn txn_multi_get_sees_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = multiget_cf(&db, "own", "memcmp");
    db.put(&cf, b"stored", b"disk", Duration::ZERO).unwrap();
    db.put(&cf, b"shadowed", b"disk", Duration::ZERO).unwrap();
    db.put(&cf, b"buried", b"disk", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    let mut t = db.begin();
    t.put(&cf, b"shadowed", b"first", Duration::ZERO).unwrap();
    t.put(&cf, b"shadowed", b"last", Duration::ZERO).unwrap(); // last write wins
    t.delete(&cf, b"buried").unwrap();
    t.put(&cf, b"fresh", b"only-in-txn", Duration::ZERO)
        .unwrap();

    let keys: Vec<&[u8]> = vec![b"stored", b"shadowed", b"buried", b"fresh", b"absent"];
    let got = t.multi_get(&cf, &keys);
    let want = oracle_multi_get(&mut t, &cf, &keys);
    assert_same_results(&got, &want, &keys, "txn overlay");
    assert_eq!(got[0].as_deref().unwrap(), b"disk");
    assert_eq!(got[1].as_deref().unwrap(), b"last");
    assert!(matches!(got[2], Err(OndaError::NotFound)), "{:?}", got[2]);
    assert_eq!(got[3].as_deref().unwrap(), b"only-in-txn");
    assert!(matches!(got[4], Err(OndaError::NotFound)));
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn txn_multi_get_records_same_read_set_as_n_gets() {
    // The read set is not observable directly, so it is asserted through the
    // conflict it causes: a Serializable txn that read a key must abort when a
    // concurrent writer changes it. Both shapes must abort identically.
    fn conflicts(batched: bool) -> bool {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = multiget_cf(&db, "rs", "memcmp");
        for k in ["a", "b", "c"] {
            db.put(&cf, k.as_bytes(), b"v0", Duration::ZERO).unwrap();
        }
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];

        let mut t = db.begin_with_isolation(IsolationLevel::Serializable);
        if batched {
            let r = t.multi_get(&cf, &keys);
            assert!(r.iter().all(|v| v.is_ok()));
        } else {
            for k in &keys {
                assert!(t.get(&cf, k).is_ok());
            }
        }
        // A concurrent writer touches one of the keys that was read.
        db.put(&cf, b"b", b"v1", Duration::ZERO).unwrap();
        t.put(&cf, b"unrelated", b"w", Duration::ZERO).unwrap();
        let outcome = t.commit();
        db.close().unwrap();
        matches!(outcome, Err(OndaError::Conflict(_)))
    }

    assert!(conflicts(false), "N gets must build a conflicting read set");
    assert!(
        conflicts(true),
        "one multi_get must record the same read set as N gets"
    );
}

#[test]
fn multi_get_with_perf_matches_multi_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = multiget_cf(&db, "wp", "memcmp");
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.put(&cf, b"b", b"2", Duration::ZERO).unwrap();
    let keys: Vec<&[u8]> = vec![b"a", b"b", b"missing"];

    let plain = db.multi_get(&cf, &keys);
    let (measured, perf) = db.multi_get_with_perf(&cf, &keys);
    assert_same_results(&measured, &plain, &keys, "multi_get_with_perf");
    assert!(perf.memtable_probes >= keys.len() as u64, "{perf:?}");
    db.close().unwrap();
}
