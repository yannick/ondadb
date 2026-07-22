//! Dropping the last `DB` handle must release the directory lock.
//!
//! Regression: `Drop` gated on `Arc::strong_count(&inner) == 1`, but the flush
//! and compaction workers each hold an `Arc<DbInner>`, so the count never fell
//! to 1 and the close-on-drop path never ran. The `<dir>/LOCK` advisory lock
//! stayed held until process exit, so reopening a directory in the same
//! process — restore, fork, restart, or a test doing any of those — failed
//! with `Locked`.

#[test]
fn dropping_the_last_handle_releases_the_lock() {
    let dir = std::env::temp_dir().join(format!("onda-drop-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let p = dir.to_string_lossy().to_string();

    {
        let db = ondadb::DB::open(ondadb::Options::new(p.clone())).unwrap();
        let cf = db.create_column_family("t", Default::default()).unwrap();
        let mut txn = db.begin();
        txn.put(&cf, b"k", b"v", std::time::Duration::ZERO).unwrap();
        txn.commit().unwrap();
    }

    let db = ondadb::DB::open(ondadb::Options::new(p.clone()))
        .expect("reopen after drop must succeed — the lock should be released");
    let cf = db.get_column_family("t").expect("column family survived");
    let mut txn = db.begin();
    assert_eq!(
        txn.get(&cf, b"k").unwrap(),
        b"v",
        "data survived the reopen"
    );
}

/// A clone keeps the database open; only the last handle closes it.
#[test]
fn a_surviving_clone_keeps_the_database_open() {
    let dir = std::env::temp_dir().join(format!("onda-clone-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let p = dir.to_string_lossy().to_string();

    let db = ondadb::DB::open(ondadb::Options::new(p.clone())).unwrap();
    let cf = db.create_column_family("t", Default::default()).unwrap();
    let clone = db.clone();
    drop(db); // not the last handle

    // The clone must still work — dropping one handle must not close the DB.
    let mut txn = clone.begin();
    txn.put(&cf, b"k", b"v", std::time::Duration::ZERO).unwrap();
    txn.commit().unwrap();

    drop(clone); // now the last handle
    ondadb::DB::open(ondadb::Options::new(p)).expect("reopen after the last handle drops");
}
