//! A memtable rotation or a flush in flight across `drop_column_family` /
//! `clear_column_family`.
//!
//! wavesdb 63e084f: a rotation could still be under way when a drop removed the
//! family's directory, open its next WAL there and poison the database. ondaDB
//! had the same window, with worse outcomes when the name was reused: the
//! rotation's WAL landed in the *re-created* family's directory (writes through
//! the stale handle then succeeded and replayed into the new family), and the
//! dropped memtable's flush wrote its table there and appended an `AddTable`
//! naming the new family — old data resurrected on the next open. Under the
//! unified layout a flush slice of a family dropped mid-flush failed and
//! fail-stopped the whole database. A compaction (or any other table edit)
//! racing a drop appended its edit after the `DropCF`, which the next open
//! refused as a failed precondition.
//!
//! Each test holds the background path at a debug-only park point
//! (`ondadb::db::cf_park`) while the drop runs, so both orderings are forced
//! without sleeps: the drop either runs straight past the parked path (the old
//! code) or blocks waiting for it (`cf_park::retire_waiting`).
#![cfg(debug_assertions)]

use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ondadb::db::cf_park::{self, Point};
use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, DB};

const ZERO: Duration = Duration::ZERO;
const DEADLINE: Duration = Duration::from_secs(20);

fn opts(path: &Path, unified: bool) -> Options {
    let mut o = Options::new(path.to_str().unwrap());
    o.unified_memtable = unified;
    o
}

/// The `MANIFEST-EDITS` log on: that is where a stray `AddTable` would land.
fn open(path: &Path, unified: bool) -> DB {
    let db = DB::open(opts(path, unified)).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
        .unwrap();
    db
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dst = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &dst);
        } else {
            std::fs::copy(entry.path(), &dst).unwrap();
        }
    }
}

fn old_key(i: u32) -> Vec<u8> {
    format!("old{i:03}").into_bytes()
}

fn assert_absent(db: &DB, cf: &Arc<ColumnFamily>, key: &[u8], what: &str) {
    match db.get(cf, key) {
        Err(OndaError::NotFound) => {}
        other => panic!(
            "{what}: key {:?} of the dropped family is visible in the re-created one: {other:?}",
            String::from_utf8_lossy(key)
        ),
    }
}

/// Block until the drop either finished or is waiting for the parked path.
fn wait_drop_done_or_waiting<T>(dropper: &JoinHandle<T>, cf: &Arc<ColumnFamily>) {
    let start = Instant::now();
    while !dropper.is_finished() && !cf_park::retire_waiting(cf) {
        assert!(
            start.elapsed() < DEADLINE,
            "the drop neither finished nor waited for the parked path"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_flushes(db: &DB) {
    let start = Instant::now();
    while db.pending_flushes_for_tests() > 0 {
        assert!(start.elapsed() < DEADLINE, "flush queue never drained");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Drop,
    DropAndRecreate,
    Clear,
}

/// Hold `point` of family `victim` in flight while `op` runs, then check every
/// outcome the race could damage.
fn held_across(point: Point, op: Op, unified: bool) {
    let dir = tempfile::tempdir().unwrap();
    let crash = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path(), unified));
    let keeper = db
        .create_column_family("keeper", ColumnFamilyConfig::default())
        .unwrap();
    let victim = db
        .create_column_family("victim", ColumnFamilyConfig::default())
        .unwrap();
    for i in 0..32 {
        db.put(&victim, &old_key(i), b"old", ZERO).unwrap();
    }
    db.put(&keeper, b"k1", b"1", ZERO).unwrap();

    let (entered, release) = cf_park::arm(&victim, point);
    // What starts the background path: a forced per-CF rotation (whose job the
    // flush worker then runs), or a unified rotation whose flush slices it.
    let starter = {
        let (db, victim) = (db.clone(), victim.clone());
        std::thread::spawn(move || {
            if unified {
                db.rotate_unified_for_tests();
                Ok(())
            } else {
                db.flush_memtable(&victim)
            }
        })
    };
    entered
        .recv_timeout(DEADLINE)
        .expect("the background path never reached its park point");

    let dropper = {
        let db = db.clone();
        std::thread::spawn(move || match op {
            Op::Drop | Op::DropAndRecreate => db.drop_column_family("victim").map(|_| None),
            Op::Clear => db.clear_column_family("victim").map(Some),
        })
    };
    wait_drop_done_or_waiting(&dropper, &victim);
    // The old code ran straight past the parked path. Re-creating the name
    // while it is still parked is the worst case: the path then resumes into
    // the new family's directory.
    let finish_op = |dropper: JoinHandle<ondadb::Result<Option<Arc<ColumnFamily>>>>| {
        let made = dropper.join().unwrap().unwrap();
        if op == Op::DropAndRecreate {
            Some(
                db.create_column_family("victim", ColumnFamilyConfig::default())
                    .unwrap(),
            )
        } else {
            made
        }
    };
    let (replacement, started) = if dropper.is_finished() {
        let replacement = finish_op(dropper);
        release.send(()).unwrap();
        (replacement, starter.join().unwrap())
    } else {
        release.send(()).unwrap();
        let started = starter.join().unwrap();
        (finish_op(dropper), started)
    };
    cf_park::disarm(&victim);
    wait_flushes(&db);

    // 1. Nothing poisoned the database, and the other family still works.
    assert_eq!(db.poisoned(), None, "the race poisoned the database");
    db.put(&keeper, b"k2", b"2", ZERO)
        .expect("an unrelated family must keep accepting writes");
    assert_eq!(db.get(&keeper, b"k1").unwrap(), b"1");

    // 2. The stale handle cannot write: no WAL may have been installed after
    // the drop closed the family's resources.
    if !unified {
        assert!(
            db.put(&victim, b"stale", b"x", ZERO).is_err(),
            "a write through the dropped family's handle succeeded"
        );
    }

    // 3. The family's directory is gone, or belongs to the replacement alone.
    let cf_dir = dir.path().join(ondadb::format::cf_dir_name("victim"));
    match &replacement {
        None => assert!(
            !cf_dir.exists(),
            "the dropped family's directory was recreated: {:?}",
            std::fs::read_dir(&cf_dir)
                .map(|d| d.map(|e| e.unwrap().file_name()).collect::<Vec<_>>())
        ),
        Some(new) => {
            for i in 0..32 {
                assert_absent(&db, new, &old_key(i), "live");
            }
            db.put(new, b"fresh", b"1", ZERO).unwrap();
        }
    }

    // 4. A crash image (the directory as it stands, no clean close) reopens and
    // shows the replacement empty of the old family's data.
    copy_tree(dir.path(), crash.path());
    {
        let img = DB::open(opts(crash.path(), unified))
            .expect("a crash image taken after the race must reopen");
        assert_eq!(img.poisoned(), None);
        match (&replacement, img.get_column_family("victim")) {
            (None, Some(_)) => panic!("the dropped family came back in the crash image"),
            (Some(_), None) => panic!("the replacement family is missing from the crash image"),
            (Some(_), Some(new)) => {
                for i in 0..32 {
                    assert_absent(&img, &new, &old_key(i), "crash image");
                }
                assert_absent(&img, &new, b"stale", "crash image");
            }
            (None, None) => {}
        }
        img.close().unwrap();
    }

    // 5. A flush through the dropped handle is NotFound, not a silent no-op (only
    // the per-CF starter flushed through it). Checked after the crash image:
    // on the old code this flush itself wrote into the replacement.
    if !unified && point == Point::RotationBeforeWalOpen {
        assert!(
            matches!(started, Err(OndaError::NotFound)),
            "flush_memtable on a family dropped under it returned {started:?}"
        );
    }
    assert!(
        matches!(db.flush_memtable(&victim), Err(OndaError::NotFound)),
        "flush_memtable through a dropped family's handle must be NotFound"
    );

    // 6. A clean close and reopen agree.
    drop(victim);
    drop(replacement);
    let Ok(db) = Arc::try_unwrap(db) else {
        panic!("every thread joined, so this is the last handle");
    };
    db.close().unwrap();
    drop(db);
    let db = DB::open(opts(dir.path(), unified)).unwrap();
    let keeper = db.get_column_family("keeper").unwrap();
    assert_eq!(db.get(&keeper, b"k1").unwrap(), b"1");
    assert_eq!(db.get(&keeper, b"k2").unwrap(), b"2");
    match (op, db.get_column_family("victim")) {
        (Op::Drop, None) => {}
        (Op::Drop, Some(_)) => panic!("the dropped family came back on reopen"),
        (_, None) => panic!("the replacement family is missing on reopen"),
        (_, Some(new)) => {
            for i in 0..32 {
                assert_absent(&db, &new, &old_key(i), "reopen");
            }
            assert_absent(&db, &new, b"stale", "reopen");
            assert_eq!(db.get(&new, b"fresh").unwrap(), b"1");
        }
    }
    db.close().unwrap();
}

#[test]
fn rotation_held_across_drop() {
    held_across(Point::RotationBeforeWalOpen, Op::Drop, false);
}

#[test]
fn rotation_held_across_drop_and_recreate() {
    held_across(Point::RotationBeforeWalOpen, Op::DropAndRecreate, false);
}

#[test]
fn rotation_held_across_clear() {
    held_across(Point::RotationBeforeWalOpen, Op::Clear, false);
}

#[test]
fn flush_held_across_drop() {
    held_across(Point::FlushBeforeWrite, Op::Drop, false);
}

#[test]
fn flush_held_across_drop_and_recreate() {
    held_across(Point::FlushBeforeWrite, Op::DropAndRecreate, false);
}

#[test]
fn flush_held_across_clear() {
    held_across(Point::FlushBeforeWrite, Op::Clear, false);
}

#[test]
fn unified_flush_held_across_drop() {
    held_across(Point::FlushBeforeWrite, Op::Drop, true);
}

#[test]
fn unified_flush_held_across_drop_and_recreate() {
    held_across(Point::FlushBeforeWrite, Op::DropAndRecreate, true);
}

#[test]
fn unified_flush_held_across_clear() {
    held_across(Point::FlushBeforeWrite, Op::Clear, true);
}

/// No race at all: a flush through a handle whose family was dropped (and the
/// name re-created) is NotFound and leaves the new family alone.
#[test]
fn flush_through_a_dropped_handle_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let crash = tempfile::tempdir().unwrap();
    let db = open(dir.path(), false);
    let old = db
        .create_column_family("victim", ColumnFamilyConfig::default())
        .unwrap();
    for i in 0..32 {
        db.put(&old, &old_key(i), b"old", ZERO).unwrap();
    }
    db.drop_column_family("victim").unwrap();
    let new = db
        .create_column_family("victim", ColumnFamilyConfig::default())
        .unwrap();
    let res = db.flush_memtable(&old);
    assert!(
        matches!(res, Err(OndaError::NotFound)),
        "flush_memtable through a dropped handle returned {res:?}"
    );
    wait_flushes(&db);
    assert_eq!(db.poisoned(), None);
    for i in 0..32 {
        assert_absent(&db, &new, &old_key(i), "live");
    }
    copy_tree(dir.path(), crash.path());
    let img = DB::open(opts(crash.path(), false)).unwrap();
    let new_img = img.get_column_family("victim").unwrap();
    for i in 0..32 {
        assert_absent(&img, &new_img, &old_key(i), "crash image");
    }
    img.close().unwrap();
    db.close().unwrap();
}

/// A compaction whose outputs are written but not yet installed, across a
/// drop or clear. The old code appended its `RemoveTables`/`AddTable` after
/// the `DropCF`, and the next open failed the record's precondition
/// (`RemoveTables: table 3 absent from "victim"`): an unopenable database.
fn compaction_held_across(op: Op) {
    let dir = tempfile::tempdir().unwrap();
    let crash = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path(), false));
    let keeper = db
        .create_column_family("keeper", ColumnFamilyConfig::default())
        .unwrap();
    let victim = db
        .create_column_family("victim", ColumnFamilyConfig::default())
        .unwrap();
    for round in 0..3u32 {
        for i in 0..32 {
            db.put(&victim, &old_key(i), format!("r{round}").as_bytes(), ZERO)
                .unwrap();
        }
        db.flush_memtable(&victim).unwrap();
    }
    db.put(&keeper, b"k1", b"1", ZERO).unwrap();
    let (entered, release) = cf_park::arm(&victim, Point::CompactionBeforeInstall);
    let starter = {
        let (db, victim) = (db.clone(), victim.clone());
        std::thread::spawn(move || db.compact(&victim))
    };
    entered
        .recv_timeout(DEADLINE)
        .expect("the compaction never reached its park point");
    let dropper = {
        let db = db.clone();
        std::thread::spawn(move || match op {
            Op::Drop | Op::DropAndRecreate => db.drop_column_family("victim").map(|_| None),
            Op::Clear => db.clear_column_family("victim").map(Some),
        })
    };
    wait_drop_done_or_waiting(&dropper, &victim);
    // The drop does not wait for a compaction: the compaction's edit is the one
    // refused. Either way the compaction itself may fail; only the catalog
    // matters here.
    let mut replacement = if dropper.is_finished() {
        let made = dropper.join().unwrap().unwrap();
        release.send(()).unwrap();
        let _ = starter.join().unwrap();
        made
    } else {
        release.send(()).unwrap();
        let _ = starter.join().unwrap();
        dropper.join().unwrap().unwrap()
    };
    if op == Op::DropAndRecreate {
        replacement = Some(
            db.create_column_family("victim", ColumnFamilyConfig::default())
                .unwrap(),
        );
    }
    cf_park::disarm(&victim);
    wait_flushes(&db);
    assert_eq!(db.poisoned(), None, "the race poisoned the database");
    db.put(&keeper, b"k2", b"2", ZERO).unwrap();
    copy_tree(dir.path(), crash.path());
    {
        let img = DB::open(opts(crash.path(), false))
            .expect("a crash image taken after the race must reopen");
        if let Some(new) = img.get_column_family("victim") {
            for i in 0..32 {
                assert_absent(&img, &new, &old_key(i), "crash image");
            }
        }
        img.close().unwrap();
    }
    drop(victim);
    drop(replacement);
    let Ok(db) = Arc::try_unwrap(db) else {
        panic!("every thread joined, so this is the last handle");
    };
    db.close().unwrap();
}

#[test]
fn compaction_held_across_drop() {
    compaction_held_across(Op::Drop);
}

#[test]
fn compaction_held_across_drop_and_recreate() {
    compaction_held_across(Op::DropAndRecreate);
}

#[test]
fn compaction_held_across_clear() {
    compaction_held_across(Op::Clear);
}
