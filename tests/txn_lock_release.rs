//! The 3.3 lock release funnel: every terminal path of a pessimistic
//! transaction frees the point locks it held.
//!
//! `Txn::release` is that funnel, and the reason release lives there rather
//! than at the call sites is the poison path — `commit` returns from its
//! fail-stop check *without* calling `release`, relying on `Drop`. A release
//! wired only into the explicit paths would miss it, so each of them gets a
//! test here and the poison one is pinned by name.
//!
//! Not to be confused with `tests/lock_release_on_drop.rs`, which is about the
//! `<dir>/LOCK` directory advisory lock and has nothing to do with
//! transactions. The distinct name is deliberate.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, DB};

fn open(dir: &std::path::Path) -> DB {
    DB::open(Options::new(dir.to_str().unwrap())).unwrap()
}

fn cf(db: &DB, name: &str) -> Arc<ColumnFamily> {
    db.create_column_family(name, ColumnFamilyConfig::default())
        .unwrap()
}

/// The observable that says "the lock is free": a *younger* transaction takes
/// it without waiting. Younger is the point — wait-die would have killed it
/// outright had the lock still been held, so this discriminates release from
/// hand-off in a single-threaded test.
fn assert_key_is_free(db: &DB, c: &Arc<ColumnFamily>, key: &[u8]) {
    assert!(
        db.txn_locks_idle_for_tests(),
        "a transaction still holds a lock"
    );
    let mut t = db.begin_pessimistic();
    if let Err(e) = t.get_for_update(c, key) {
        assert_eq!(e.kind(), "not_found", "the key is still locked: {e}");
    }
    t.rollback().unwrap();
}

#[test]
fn locks_release_on_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let mut t = db.begin_pessimistic();
    t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
    t.commit().unwrap();
    assert_key_is_free(&db, &c, b"k");
    db.close().unwrap();
}

#[test]
fn locks_release_on_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let mut t = db.begin_pessimistic();
    t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
    t.rollback().unwrap();
    assert_key_is_free(&db, &c, b"k");
    db.close().unwrap();
}

#[test]
fn locks_release_on_reset() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let mut t = db.begin_pessimistic();
    t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
    // `reset` releases through `rollback`, and the reset transaction is a new,
    // younger one — so it must be able to retake the key it just freed.
    t.reset(ondadb::IsolationLevel::Snapshot).unwrap();
    assert!(db.txn_locks_idle_for_tests());
    t.get_for_update(&c, b"k").unwrap_err();
    t.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn locks_release_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    {
        let mut t = db.begin_pessimistic();
        t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
        // No commit, no rollback: `Drop` is the only path that runs.
    }
    assert_key_is_free(&db, &c, b"k");
    db.close().unwrap();
}

#[test]
fn locks_release_on_poisoned_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let mut t = db.begin_pessimistic();
    t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
    db.fail_stop_for_tests("simulated durability failure");
    // `commit` returns at its fail-stop check WITHOUT calling `release` — the
    // transaction stays usable for rollback, and only `Drop` frees the lock.
    assert_eq!(t.commit().unwrap_err().kind(), "poisoned");
    assert!(
        !db.txn_locks_idle_for_tests(),
        "the poison check returns before the release funnel, by design"
    );
    drop(t);
    assert!(
        db.txn_locks_idle_for_tests(),
        "Drop is the release path the poison check relies on"
    );
    // And a second transaction takes the key without waiting. It still cannot
    // commit — the database is fail-stopped — but the lock is not what stops it.
    let mut second = db.begin_pessimistic();
    second.get_for_update(&c, b"k").unwrap_err();
    assert_eq!(
        second
            .put(&c, b"k", b"w", Duration::ZERO)
            .and_then(|()| second.commit())
            .unwrap_err()
            .kind(),
        "poisoned"
    );
    let _ = db.close();
}

#[test]
fn locks_release_when_txn_dropped_on_another_thread() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    let mut t = db.begin_pessimistic();
    t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
    // `Txn` is `Send`, so the releasing thread need not be the acquiring one.
    std::thread::spawn(move || drop(t)).join().unwrap();
    assert_key_is_free(&db, &c, b"k");
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

#[test]
fn locks_release_on_panic_unwind() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut t = db.begin_pessimistic();
        t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
        panic!("unwind with the lock held");
    }));
    assert!(result.is_err(), "the closure must have panicked");
    assert_key_is_free(&db, &c, b"k");
    db.close().unwrap();
}

#[test]
fn close_with_locks_held_wakes_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    db.put(&c, b"k", b"v0", Duration::ZERO).unwrap();

    // The waiter's transaction is created first, so wait-die makes it the
    // older one and it waits rather than dying; the holder takes the lock
    // before the waiter asks for it. Both orderings are pinned by barriers,
    // because either one going the other way silently tests something else.
    let begun = Arc::new(std::sync::Barrier::new(2));
    let held = Arc::new(std::sync::Barrier::new(2));
    let waiter = {
        let db = db.clone();
        let c = c.clone();
        let begun = begun.clone();
        let held = held.clone();
        std::thread::spawn(move || {
            let mut t = db.begin_pessimistic();
            begun.wait();
            held.wait();
            let err = t.get_for_update(&c, b"k").unwrap_err();
            // Whatever the holder does, the waiter must come back.
            assert!(
                matches!(err.kind(), "invalid_db" | "conflict"),
                "unexpected wake-up error: {err}"
            );
        })
    };
    begun.wait();
    let mut holder = db.begin_pessimistic();
    holder.put(&c, b"k", b"v1", Duration::ZERO).unwrap();
    held.wait();
    while db.txn_lock_waiters_for_tests(&c, b"k") == 0 {
        std::thread::yield_now();
    }
    // Close with the lock held and a waiter parked on it: nothing may hang.
    let _ = db.close();
    waiter.join().unwrap();
    drop(holder);
}
