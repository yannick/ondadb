//! Flushes publish in seal order.
//!
//! Several flush workers write tables concurrently. If a newer memtable's
//! table is published before an older one's, a compaction in between can push
//! the newer table to the bottom and drop a tombstone it holds; the older
//! table then lands in L0 above the output and a deleted key comes back —
//! durably, because the edit log replays the same order. A flush therefore
//! publishes only once no older sealed memtable is left (`await_flush_turn`).
//!
//! The older flush is parked before it writes (`FlushBeforeWrite`), so the
//! newer one is the first to finish writing. Both layouts are covered: the
//! per-family queue and the unified store's.
#![cfg(debug_assertions)]

use std::ops::Bound::Unbounded;
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::db::cf_park::{self, Point};
use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, DB};

const ZERO: Duration = Duration::ZERO;
const DEADLINE: Duration = Duration::from_secs(20);

fn open(path: &Path, unified: bool) -> DB {
    let mut o = Options::new(path.to_str().unwrap());
    o.unified_memtable = unified;
    assert!(
        o.num_flush_threads >= 2,
        "needs two flush workers to reorder"
    );
    DB::open(o).unwrap()
}

fn wait_until(what: &str, f: impl Fn() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < DEADLINE, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Seal the current memtable and queue its flush, on another thread: the
/// per-family `flush_memtable` blocks until every queued flush is done.
fn seal(db: &Arc<DB>, cf: &Arc<ColumnFamily>, unified: bool) -> std::thread::JoinHandle<()> {
    let (db, cf) = (db.clone(), cf.clone());
    std::thread::spawn(move || {
        if unified {
            db.rotate_unified_for_tests();
        } else {
            let _ = db.flush_memtable(&cf);
        }
    })
}

struct Parked {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    release: SyncSender<()>,
    sealers: Vec<std::thread::JoinHandle<()>>,
}

/// Memtable A puts `k` and its flush parks before writing; memtable B deletes
/// `k` and its flush writes its table. Returns once B's table is written.
fn older_flush_parked(path: &Path, unified: bool) -> Parked {
    let db = Arc::new(open(path, unified));
    let cf = db
        .create_column_family("c", ColumnFamilyConfig::default())
        .unwrap();

    db.put(&cf, b"k", b"v-old", ZERO).unwrap();
    let (entered, release): (Receiver<()>, SyncSender<()>) =
        cf_park::arm(&cf, Point::FlushBeforeWrite);
    let a = seal(&db, &cf, unified);
    entered
        .recv_timeout(DEADLINE)
        .expect("flush A never parked");

    db.delete(&cf, b"k").unwrap();
    let b = seal(&db, &cf, unified);
    // A is parked before its write, so one written table is B's.
    wait_until("B's table written", || cf.stats().flush_count == 1);
    Parked {
        db,
        cf,
        release,
        sealers: vec![a, b],
    }
}

fn get(db: &DB, cf: &Arc<ColumnFamily>) -> Result<Vec<u8>, OndaError> {
    db.get(cf, b"k")
}

fn deleted_key_stays_deleted(unified: bool) {
    let dir = tempfile::tempdir().unwrap();
    let p = older_flush_parked(dir.path(), unified);
    let Parked {
        db,
        cf,
        release,
        sealers,
    } = p;

    // B is written but must not be published ahead of A. Give it the time it
    // used to need to publish: with the bug it did, within microseconds.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        cf.l0_file_count(),
        0,
        "B published while the older A was still unflushed"
    );

    // The window the bug needed: a compaction while A is outstanding. With B
    // held back it has nothing to push to the bottom.
    db.compact_range(&cf, Unbounded, Unbounded).unwrap();

    release.send(()).unwrap();
    wait_until("both flushes published", || {
        db.pending_flushes_for_tests() == 0
    });
    for s in sealers {
        s.join().unwrap();
    }
    cf_park::disarm(&cf);
    assert_eq!(cf.l0_file_count(), 2, "both tables in L0");
    assert!(
        matches!(get(&db, &cf), Err(OndaError::NotFound)),
        "deleted key visible after the flushes"
    );

    // Compaction now sees B above A and drops the put with the tombstone.
    db.compact_range(&cf, Unbounded, Unbounded).unwrap();
    assert!(
        matches!(get(&db, &cf), Err(OndaError::NotFound)),
        "deleted key visible after compaction"
    );

    // And the edit log replays the same order.
    let Ok(db) = Arc::try_unwrap(db) else {
        panic!("another handle still holds the database");
    };
    db.close().unwrap();
    let db = open(dir.path(), unified);
    let cf = db.get_column_family("c").unwrap();
    assert!(
        matches!(get(&db, &cf), Err(OndaError::NotFound)),
        "deleted key visible after reopen"
    );
}

#[test]
fn per_cf_flushes_publish_in_seal_order() {
    deleted_key_stays_deleted(false);
}

#[test]
fn unified_flushes_publish_in_seal_order() {
    deleted_key_stays_deleted(true);
}

/// A fail-stop while a newer flush waits for its turn releases it without
/// publishing: an older flush that failed must not be overtaken.
fn fail_stop_releases_the_waiting_flush(unified: bool) {
    let dir = tempfile::tempdir().unwrap();
    let Parked {
        db,
        cf,
        release,
        sealers,
    } = older_flush_parked(dir.path(), unified);

    db.fail_stop_for_tests("test: an older flush failed");
    // Only A's job is left: B's gave up its turn.
    wait_until("B's flush to give up", || {
        db.pending_flushes_for_tests() == 1
    });
    assert_eq!(cf.l0_file_count(), 0, "B published past a fail-stop");

    release.send(()).unwrap();
    wait_until("A's flush to return", || {
        db.pending_flushes_for_tests() == 0
    });
    for s in sealers {
        s.join().unwrap();
    }
    cf_park::disarm(&cf);
    assert_eq!(
        cf.l0_file_count(),
        0,
        "a fail-stopped database published a flush"
    );
}

#[test]
fn per_cf_fail_stop_releases_the_waiting_flush() {
    fail_stop_releases_the_waiting_flush(false);
}

#[test]
fn unified_fail_stop_releases_the_waiting_flush() {
    fail_stop_releases_the_waiting_flush(true);
}

/// Dropping the family while a newer flush waits for its turn completes: the
/// waiter holds a flush claim the drop waits for, and the older flush, finding
/// the family retiring, never publishes the memtable it was waiting behind.
#[test]
fn per_cf_drop_releases_the_waiting_flush() {
    let dir = tempfile::tempdir().unwrap();
    let Parked {
        db,
        cf,
        release,
        sealers,
    } = older_flush_parked(dir.path(), false);

    let dropper = {
        let db = db.clone();
        std::thread::spawn(move || db.drop_column_family("c"))
    };
    // The drop waits for A's parked flush; B must give up rather than hold it.
    wait_until("the drop to wait for the flushes", || {
        cf_park::retire_waiting(&cf)
    });
    release.send(()).unwrap();
    dropper
        .join()
        .unwrap()
        .expect("drop with a flush waiting for its turn");
    wait_until("flushes to return", || db.pending_flushes_for_tests() == 0);
    for s in sealers {
        s.join().unwrap();
    }
    cf_park::disarm(&cf);
    assert!(
        db.get_column_family("c").is_none(),
        "family still registered after the drop"
    );
}
