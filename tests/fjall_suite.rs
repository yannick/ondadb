//! Port of fjall v3.1.5's engine-generic integration tests to ondaDB.
//!
//! Source: `../fjall/tests/*.rs`. Each test keeps its fjall name so the two
//! suites can be diffed side by side. API mapping:
//!
//! | fjall                          | ondaDB                                   |
//! |--------------------------------|------------------------------------------|
//! | `Database::builder(p).open()`  | `DB::open(Options::new(p))`              |
//! | `db.keyspace(name, opts)`      | `create_column_family` / `get_column_family` |
//! | `db.batch()` / `db.write_tx()` | `db.begin()` (`Txn`)                     |
//! | `db.snapshot()` / `read_tx()`  | `db.begin()` (Snapshot isolation)        |
//! | `tree.clear()`                 | `drop_column_family` + recreate          |
//! | KV separation (blob)           | vlog (`klog_value_threshold`)            |
//! | `rotate_memtable_and_wait()`   | `flush_memtable`                         |
//!
//! Not ported (feature absent or fjall-internal — see the gap report):
//! - `keyspace_visible_seqno`, `keyspace_torn_read`, `seqno_recovery`,
//!   `write_buffer_size`: poke fjall's seqno/write-buffer internals.
//! - `ingest_recovery`: ondaDB has no bulk-ingestion API.
//! - `keyspace_recover`, `compaction_filter`, `fifo_dirty_read`: fjall
//!   config-persistence internals, compaction filters and FIFO compaction.
//! - `keyspace_v1/v2_load_fixture`, `recovery_*_mac`,
//!   `recover_from_different_folder`: fjall on-disk format / platform quirks.
//! - `clear_dirty_read`'s tail (snapshot reads across a concurrent `clear()`):
//!   dropping a CF mid-snapshot has no ondaDB equivalent.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, Options, DB};

const ZERO: Duration = Duration::ZERO;

fn open(dir: &std::path::Path) -> DB {
    DB::open(Options::new(dir.to_str().unwrap())).unwrap()
}

/// fjall's `db.keyspace(name, ...)` is get-or-create.
fn keyspace(db: &DB, name: &str) -> Arc<ColumnFamily> {
    keyspace_cfg(db, name, ColumnFamilyConfig::default())
}

fn keyspace_cfg(db: &DB, name: &str, cfg: ColumnFamilyConfig) -> Arc<ColumnFamily> {
    match db.get_column_family(name) {
        Some(cf) => cf,
        None => db.create_column_family(name, cfg).unwrap(),
    }
}

/// fjall's `tree.len()` — ondaDB has no O(1) len, so count via a fresh snapshot.
fn len(db: &DB, cf: &Arc<ColumnFamily>) -> usize {
    let txn = db.begin();
    let mut it = txn.new_iterator(cf);
    let mut n = 0;
    it.seek_to_first();
    while it.valid() {
        n += 1;
        it.next();
    }
    n
}

fn contains(db: &DB, cf: &Arc<ColumnFamily>, key: &[u8]) -> bool {
    db.get(cf, key).is_ok()
}

// ---------------------------------------------------------------- batch.rs

#[test]
fn batch_simple() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    let mut batch = db.begin();
    assert_eq!(len(&db, &tree), 0);
    batch.put(&tree, b"1", b"abc", ZERO).unwrap();
    batch.put(&tree, b"3", b"abc", ZERO).unwrap();
    batch.put(&tree, b"5", b"abc", ZERO).unwrap();
    assert_eq!(len(&db, &tree), 0); // not visible before commit

    batch.commit().unwrap();
    assert_eq!(len(&db, &tree), 3);
    db.close().unwrap();
}

#[test]
fn blob_batch_simple() {
    // fjall: KV separation. ondaDB separates values > klog_value_threshold
    // (default 512 B) into the vlog automatically.
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    let blob = "oxygen".repeat(128_000);

    let mut batch = db.begin();
    assert_eq!(len(&db, &tree), 0);
    batch.put(&tree, b"1", blob.as_bytes(), ZERO).unwrap();
    batch.put(&tree, b"3", b"abc", ZERO).unwrap();
    batch.put(&tree, b"5", b"abc", ZERO).unwrap();
    assert_eq!(len(&db, &tree), 0);

    batch.commit().unwrap();
    assert_eq!(len(&db, &tree), 3);
    assert_eq!(db.get(&tree, b"1").unwrap(), blob.as_bytes());

    // Also exercise the vlog read path, not just the memtable.
    db.flush_memtable(&tree).unwrap();
    assert_eq!(db.get(&tree, b"1").unwrap(), blob.as_bytes());
    db.close().unwrap();
}

#[test]
fn batch_multi_keys() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    let mut batch = db.begin();
    batch.put(&tree, b"1", b"abc", ZERO).unwrap();
    batch.put(&tree, b"1", b"def", ZERO).unwrap();
    batch.put(&tree, b"1", b"ghi", ZERO).unwrap();
    batch.commit().unwrap();

    assert_eq!(len(&db, &tree), 1);
    assert_eq!(db.get(&tree, b"1").unwrap(), b"ghi");
    db.close().unwrap();
}

// ------------------------------------------------------- batch_recovery.rs

#[test]
fn batch_recovery() {
    let folder = tempfile::tempdir().unwrap();

    for i in 0_u128..25 {
        let db = open(folder.path());
        let tree = keyspace(&db, "default");
        let tree2 = keyspace(&db, "default2");

        let mut batch = db.begin();
        batch
            .put(&tree, &i.to_be_bytes(), &i.to_be_bytes(), ZERO)
            .unwrap();
        batch
            .put(&tree2, &i.to_be_bytes(), &i.to_be_bytes(), ZERO)
            .unwrap();
        batch.commit().unwrap();

        // Every previously committed batch must have survived the reopen in
        // both column families.
        for j in 0_u128..=i {
            assert_eq!(db.get(&tree, &j.to_be_bytes()).unwrap(), j.to_be_bytes());
            assert_eq!(db.get(&tree2, &j.to_be_bytes()).unwrap(), j.to_be_bytes());
        }
        db.close().unwrap();
    }
}

// -------------------------------------------------------------- db_open.rs

#[test]
fn db_open() {
    let folder = tempfile::tempdir().unwrap();
    {
        let db = open(folder.path());
        db.close().unwrap();
    }
    // DB should not be locked after a clean close.
    {
        let db = open(folder.path());
        db.close().unwrap();
    }
}

#[test]
fn db_open_with_keyspace() {
    let folder = tempfile::tempdir().unwrap();
    {
        let db = open(folder.path());
        let _tree = keyspace(&db, "hello");
        db.close().unwrap();
    }
    {
        let db = open(folder.path());
        assert!(db.get_column_family("hello").is_some());
        db.close().unwrap();
    }
}

// -------------------------------------------------------------- db_lock.rs

#[test]
fn db_lock() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    db.put(&tree, b"asd", b"def", ZERO).unwrap();
    db.put(&tree, b"efg", b"hgf", ZERO).unwrap();
    db.put(&tree, b"hij", b"wer", ZERO).unwrap();

    // fjall: a second open of a live DB must fail with Error::Locked.
    assert!(
        DB::open(Options::new(folder.path().to_str().unwrap())).is_err(),
        "second open of a live database must fail"
    );
    db.close().unwrap();
}

// ------------------------------------------------------- keyspace_clear.rs
// fjall's `tree.clear()` has no ondaDB equivalent; the closest durable
// operation is drop_column_family + recreate.

#[test]
fn clear_recover() {
    let folder = tempfile::tempdir().unwrap();
    {
        let db = open(folder.path());
        let tree = keyspace(&db, "default");
        assert_eq!(len(&db, &tree), 0);

        db.put(&tree, b"a", b"a", ZERO).unwrap();
        assert!(contains(&db, &tree, b"a"));

        db.drop_column_family("default").unwrap();
        let tree = keyspace(&db, "default");
        assert_eq!(len(&db, &tree), 0);
        db.close().unwrap();
    }
    {
        let db = open(folder.path());
        let tree = keyspace(&db, "default");
        assert_eq!(len(&db, &tree), 0);
        db.close().unwrap();
    }
}

#[test]
fn clear_recover_multi_tree() {
    let folder = tempfile::tempdir().unwrap();
    {
        let db = open(folder.path());
        let tree = keyspace(&db, "default");
        let _other = keyspace(&db, "other");

        db.put(&tree, b"a", b"a", ZERO).unwrap();
        db.drop_column_family("default").unwrap();
        let tree = keyspace(&db, "default");
        assert_eq!(len(&db, &tree), 0);

        let other = db.get_column_family("other").unwrap();
        db.put(&other, b"a", b"z", ZERO).unwrap();
        db.close().unwrap();
    }
    {
        let db = open(folder.path());
        let tree = keyspace(&db, "default");
        assert_eq!(len(&db, &tree), 0);

        db.drop_column_family("other").unwrap();
        let other = keyspace(&db, "other");
        assert_eq!(len(&db, &other), 0);

        db.put(&tree, b"a", b"a", ZERO).unwrap();
        db.close().unwrap();
    }
    {
        let db = open(folder.path());
        let tree = db.get_column_family("default").unwrap();
        let other = db.get_column_family("other").unwrap();
        assert!(contains(&db, &tree, b"a"));
        assert_eq!(len(&db, &other), 0);
        db.close().unwrap();
    }
}

// ---------------------------------------------------- keyspace_snapshot.rs

#[test]
fn keyspace_iter_dirty_read() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    db.put(&tree, b"a#1", b"a", ZERO).unwrap();
    db.put(&tree, b"a#2", b"b", ZERO).unwrap();
    db.put(&tree, b"a#3", b"c", ZERO).unwrap();

    db.flush_memtable(&tree).unwrap();

    // Iterator taken now must not see the later write (no dirty read).
    let txn = db.begin();
    let mut it = txn.new_iterator(&tree);

    db.put(&tree, b"a#4", b"d", ZERO).unwrap();

    let mut n = 0;
    it.seek_to_first();
    while it.valid() {
        n += 1;
        it.next();
    }
    assert_eq!(n, 3);
    db.close().unwrap();
}

#[test]
fn keyspace_snapshot_read() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");
    let tree2 = keyspace(&db, "default2");

    db.put(&tree, b"a#1", b"a", ZERO).unwrap();
    db.put(&tree, b"a#2", b"b", ZERO).unwrap();
    db.put(&tree, b"a#3", b"c", ZERO).unwrap();
    db.put(&tree2, b"b#1", b"5", ZERO).unwrap();

    // A snapshot transaction spans both column families.
    let snapshot = db.begin();

    db.flush_memtable(&tree).unwrap();

    db.put(&tree, b"a#4", b"d", ZERO).unwrap();
    db.put(&tree2, b"b#2", b"6", ZERO).unwrap();

    for _ in 0..2 {
        let mut it = snapshot.new_iterator(&tree);
        let mut n = 0;
        it.seek_to_first();
        while it.valid() {
            n += 1;
            it.next();
        }
        assert_eq!(n, 3);
    }
    let mut it = snapshot.new_iterator(&tree2);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.key(), b"b#1");
    it.next();
    assert!(!it.valid());
    db.close().unwrap();
}

// ---------------------------------------------------- memtable_recover.rs

#[test]
fn reload_with_memtable() {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir().unwrap();
    {
        let db = open(folder.path());
        let tree = keyspace(&db, "default");

        let mut batch = db.begin();
        for x in 0..(ITEM_COUNT * 2) as u64 {
            let value = format!("val{x}");
            batch
                .put(&tree, &x.to_be_bytes(), value.as_bytes(), ZERO)
                .unwrap();
        }
        batch.commit().unwrap();

        assert_eq!(len(&db, &tree), ITEM_COUNT * 2);
        db.close().unwrap();
    }

    for _ in 0..5 {
        let db = open(folder.path());
        let tree = db.get_column_family("default").unwrap();

        // Forward and backward full iteration both see every entry.
        let txn = db.begin();
        let mut it = txn.new_iterator(&tree);
        let mut fwd = 0;
        it.seek_to_first();
        while it.valid() {
            fwd += 1;
            it.next();
        }
        let mut bwd = 0;
        it.seek_to_last();
        while it.valid() {
            bwd += 1;
            it.prev();
        }
        assert_eq!(fwd, ITEM_COUNT * 2);
        assert_eq!(bwd, ITEM_COUNT * 2);
        db.close().unwrap();
    }
}

// ------------------------------------------------------ prefix_complex.rs

#[test]
fn keyspace_prefix_carl() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    // 4 KiB values exceed klog_value_threshold (512) => vlog-separated.
    db.put(&tree, b"a#1", "a".repeat(4096).as_bytes(), ZERO)
        .unwrap();
    db.put(&tree, b"a#2", "b".repeat(4096).as_bytes(), ZERO)
        .unwrap();
    db.put(&tree, b"a#3", "c".repeat(4096).as_bytes(), ZERO)
        .unwrap();
    // Keys outside the prefix, so the scan has something to stop at.
    db.put(&tree, b"a!", b"x", ZERO).unwrap();
    db.put(&tree, b"b#1", b"y", ZERO).unwrap();

    db.flush_memtable(&tree).unwrap();

    // ondaDB prefix scan = seek(prefix) + iterate while the prefix holds.
    let txn = db.begin();
    let mut it = txn.new_iterator(&tree);
    it.seek(b"a#");

    assert!(it.valid());
    assert_eq!(it.key(), b"a#1");

    it.next();
    assert!(it.valid());
    assert_eq!(it.key(), b"a#2");
    assert_eq!(it.value(), "b".repeat(4096).as_bytes());

    it.next();
    assert!(it.valid());
    assert_eq!(it.key(), b"a#3");
    assert_eq!(it.value(), "c".repeat(4096).as_bytes());

    it.next();
    assert!(!it.valid() || !it.key().starts_with(b"a#"));
    db.close().unwrap();
}

// ---------------------------------------------------- write_during_read.rs

#[test]
fn write_during_read() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace_cfg(
        &db,
        "default",
        ColumnFamilyConfig {
            write_buffer_size: 128_000, // force flushes mid-fill, like fjall
            ..ColumnFamilyConfig::default()
        },
    );

    let mut batch = db.begin();
    for x in 0u64..50_000 {
        batch
            .put(&tree, &x.to_be_bytes(), &x.to_be_bytes(), ZERO)
            .unwrap();
        if x % 1_000 == 999 {
            batch.commit().unwrap();
            batch = db.begin();
        }
    }
    batch.commit().unwrap();
    db.flush_memtable(&tree).unwrap();

    // Re-insert every entry while a full scan is in flight.
    let txn = db.begin();
    let mut it = txn.new_iterator(&tree);
    let mut n = 0u64;
    it.seek_to_first();
    while it.valid() {
        let (k, v) = (it.key().to_vec(), it.value().to_vec());
        db.put(&tree, &k, &v.repeat(4), ZERO).unwrap();
        n += 1;
        it.next();
    }
    assert_eq!(n, 50_000); // snapshot scan unaffected by concurrent writes
    db.close().unwrap();
}

// ------------------------------------------------------------ write_tx.rs

#[test]
fn write_tx_multi_keys() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    let mut wtx = db.begin();
    wtx.put(&tree, b"1", b"abc", ZERO).unwrap();
    wtx.put(&tree, b"1", b"def", ZERO).unwrap();
    wtx.put(&tree, b"1", b"ghi", ZERO).unwrap();
    assert_eq!(wtx.get(&tree, b"1").unwrap(), b"ghi");
    wtx.commit().unwrap();
    assert_eq!(db.get(&tree, b"1").unwrap(), b"ghi");
    db.close().unwrap();
}

// ------------------------------------------------------------- tx_ryow.rs

#[test]
fn tx_ryow() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    let mut tx = db.begin();

    assert!(tx.get(&tree, b"a").is_err());

    tx.put(&tree, b"a", b"a", ZERO).unwrap();
    assert_eq!(tx.get(&tree, b"a").unwrap(), b"a");

    tx.delete(&tree, b"a").unwrap();
    assert!(tx.get(&tree, b"a").is_err());

    tx.put(&tree, b"a", b"a", ZERO).unwrap();
    tx.put(&tree, b"a", b"c", ZERO).unwrap();
    assert_eq!(tx.get(&tree, b"a").unwrap(), b"c");

    tx.delete(&tree, b"a").unwrap();
    assert!(tx.get(&tree, b"a").is_err());

    tx.rollback().unwrap();
    db.close().unwrap();
}

// -------------------------------------------------- tx_ryow_snapshot.rs

#[test]
fn tx_ryow_snapshot() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    let mut tx = db.begin();

    // An iterator taken before the writes must not see them...
    let mut before = tx.new_iterator(&tree);
    for x in 0u64..100 {
        tx.put(&tree, &x.to_be_bytes(), b"a", ZERO).unwrap();
    }
    let mut n = 0;
    before.seek_to_first();
    while before.valid() {
        n += 1;
        before.next();
    }
    assert_eq!(n, 0);

    // ...but a fresh iterator sees the transaction's own writes.
    let mut after = tx.new_iterator(&tree);
    let mut n = 0;
    after.seek_to_first();
    while after.valid() {
        n += 1;
        after.next();
    }
    assert_eq!(n, 100);
    db.close().unwrap();
}

#[test]
fn tx_ryow_snapshot_ssi() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    let mut tx = db.begin_with_isolation(IsolationLevel::Snapshot);

    let mut before = tx.new_iterator(&tree);
    for x in 0u64..100 {
        tx.put(&tree, &x.to_be_bytes(), b"a", ZERO).unwrap();
    }
    let mut n = 0;
    before.seek_to_first();
    while before.valid() {
        n += 1;
        before.next();
    }
    assert_eq!(n, 0);

    let count = |tx: &ondadb::Txn| {
        let mut it = tx.new_iterator(&tree);
        let mut n = 0;
        it.seek_to_first();
        while it.valid() {
            n += 1;
            it.next();
        }
        n
    };
    assert_eq!(count(&tx), 100);

    // Concurrent committed writes must stay invisible to the pinned snapshot.
    db.put(&tree, b"2", b"2", ZERO).unwrap();
    assert_eq!(count(&tx), 100);

    let mut it = tx.new_iterator(&tree);
    db.put(&tree, b"3", b"3", ZERO).unwrap();
    let mut n = 0;
    it.seek_to_first();
    while it.valid() {
        n += 1;
        it.next();
    }
    assert_eq!(n, 100);

    tx.rollback().unwrap();
    db.close().unwrap();
}

// ----------------------------------------------- journal_large_value.rs
// Regression test for fjall issue #68: a large value written just before
// shutdown must survive WAL replay.

#[test]
fn journal_recover_large_value() {
    let folder = tempfile::tempdir().unwrap();
    let large_value = "a".repeat(128_000);

    {
        let db = open(folder.path());
        // Inline path: threshold above the value size keeps it in the klog.
        let tree = keyspace_cfg(
            &db,
            "default",
            ColumnFamilyConfig {
                klog_value_threshold: 1 << 20,
                ..ColumnFamilyConfig::default()
            },
        );
        db.put(&tree, b"a", large_value.as_bytes(), ZERO).unwrap();
        db.put(&tree, b"b", b"b", ZERO).unwrap();
        db.close().unwrap();
    }
    {
        let db = open(folder.path());
        let tree = db.get_column_family("default").unwrap();
        assert_eq!(db.get(&tree, b"a").unwrap(), large_value.as_bytes());
        assert_eq!(db.get(&tree, b"b").unwrap(), b"b");
        db.close().unwrap();
    }
}

#[test]
fn journal_recover_large_value_blob() {
    let folder = tempfile::tempdir().unwrap();
    let large_value = "a".repeat(128_000);

    {
        let db = open(folder.path());
        // Default config: 128 kB value goes through the vlog on flush.
        let tree = keyspace(&db, "default");
        db.put(&tree, b"a", large_value.as_bytes(), ZERO).unwrap();
        db.put(&tree, b"b", b"b", ZERO).unwrap();
        db.close().unwrap();
    }
    {
        let db = open(folder.path());
        let tree = db.get_column_family("default").unwrap();
        assert_eq!(db.get(&tree, b"a").unwrap(), large_value.as_bytes());
        assert_eq!(db.get(&tree, b"b").unwrap(), b"b");
        // Force the vlog path too, then read it back.
        db.flush_memtable(&tree).unwrap();
        assert_eq!(db.get(&tree, b"a").unwrap(), large_value.as_bytes());
        db.close().unwrap();
    }
}

// ------------------------------------------------ keyspace_iter_lifetime.rs

#[test]
fn keyspace_iter_lifetime() {
    let folder = tempfile::tempdir().unwrap();
    let db = open(folder.path());
    let tree = keyspace(&db, "default");

    db.put(&tree, b"asd", b"def", ZERO).unwrap();
    db.put(&tree, b"efg", b"hgf", ZERO).unwrap();
    db.put(&tree, b"hij", b"wer", ZERO).unwrap();

    // The iterator owns its snapshot; it can be moved into helpers freely.
    struct Counter {
        iter: ondadb::Iterator,
    }
    impl Counter {
        fn execute(mut self) -> usize {
            let mut n = 0;
            self.iter.seek_to_first();
            while self.iter.valid() {
                n += 1;
                self.iter.next();
            }
            n
        }
    }

    let txn = db.begin();
    let counter = Counter {
        iter: txn.new_iterator(&tree),
    };
    assert_eq!(3, counter.execute());
    db.close().unwrap();
}
