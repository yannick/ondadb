//! Standalone refcounted read snapshots (`DB::snapshot`, wavesdb
//! `SnapshotHandle`).

use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, DB};

fn open(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("c", ColumnFamilyConfig::default())
        .unwrap();
    (db, cf)
}

/// Entries the catalog holds across all tables. The retention test writes only
/// `k`, so this is the number of its versions still stored.
fn stored_entries(cf: &ColumnFamily) -> u64 {
    cf.table_metadata()
        .iter()
        .flatten()
        .map(|t| t.num_entries)
        .sum()
}

#[test]
fn reads_stay_at_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    db.put(&cf, b"k", b"old", Duration::ZERO).unwrap();
    let snap = db.snapshot();
    db.put(&cf, b"k", b"new", Duration::ZERO).unwrap();
    db.delete(&cf, b"a").unwrap();
    db.put(&cf, b"z", b"later", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    assert_eq!(snap.get(&cf, b"k").unwrap(), b"old");
    assert_eq!(db.get_at(&cf, b"k", &snap).unwrap(), b"old");
    assert_eq!(snap.get(&cf, b"a").unwrap(), b"1");
    assert!(matches!(snap.get(&cf, b"z"), Err(OndaError::NotFound)));
    assert_eq!(db.get(&cf, b"k").unwrap(), b"new");
    let batch = snap.multi_get(&cf, &[b"a", b"k", b"z"]);
    assert_eq!(batch[0].as_deref().unwrap(), b"1");
    assert_eq!(batch[1].as_deref().unwrap(), b"old");
    assert!(matches!(batch[2], Err(OndaError::NotFound)));

    let mut it = db.new_iterator_at(&cf, &snap, Bound::Unbounded, Bound::Unbounded);
    it.seek_to_first();
    let mut seen = Vec::new();
    while it.valid() {
        seen.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    assert!(it.err().is_none());
    assert_eq!(
        seen,
        vec![(b"a".to_vec(), b"1".to_vec()), (b"k".to_vec(), b"old".to_vec())]
    );
    drop(snap);
    db.close().unwrap();
}

#[test]
fn snapshot_sees_this_threads_own_last_write() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k", b"mine", Duration::ZERO).unwrap();
    assert_eq!(db.snapshot().get(&cf, b"k").unwrap(), b"mine");
    db.close().unwrap();
}

/// The point of the handle: while it lives, compaction keeps the version it
/// can see; once it is released, the next compaction collects it.
#[test]
fn compaction_retains_versions_until_release() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put(&cf, b"k", b"v1", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    let snap = db.snapshot();
    db.put(&cf, b"k", b"v2", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    db.compact(&cf).unwrap();
    assert_eq!(stored_entries(&cf), 2, "the snapshot's version was collected");
    assert_eq!(snap.get(&cf, b"k").unwrap(), b"v1");
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v2");

    // A clone shares the pin: dropping the original keeps it.
    let clone = snap.clone();
    let seq = snap.seq();
    snap.release();
    assert_eq!(db.oldest_snapshot_for_tests(), seq);
    db.compact(&cf).unwrap();
    assert_eq!(clone.get(&cf, b"k").unwrap(), b"v1");

    drop(clone);
    assert!(db.oldest_snapshot_for_tests() > seq, "last clone did not release the pin");
    // Compaction only rewrites what a trigger or a newer write gives it, so add
    // a table to make the manual sweep merge again.
    db.put(&cf, b"k", b"v3", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(stored_entries(&cf), 1, "released versions were retained");
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v3");
    db.close().unwrap();
}

#[test]
fn a_handle_from_another_database_is_refused() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let (db1, _) = open(d1.path());
    let (db2, cf2) = open(d2.path());
    let foreign = db1.snapshot();
    assert!(matches!(
        db2.get_at(&cf2, b"k", &foreign),
        Err(OndaError::InvalidArgs(_))
    ));
    let it = db2.new_iterator_at(&cf2, &foreign, Bound::Unbounded, Bound::Unbounded);
    assert!(!it.valid());
    assert!(matches!(it.err(), Some(OndaError::InvalidArgs(_))));
    drop(foreign);
    db1.close().unwrap();
    db2.close().unwrap();
}
