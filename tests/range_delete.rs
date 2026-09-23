//! Range tombstones (1.2): API validation, conflict outcomes, read masking,
//! flush fragmentation and compaction rules.
//!
//! The centerpiece is the **reference model**. A range delete is a claim about
//! what a read returns, so most of what follows compares the engine against a
//! brute-force oracle built from the same history — point `get`, forward and
//! reverse iteration, and the two against each other. A bug in fragmentation,
//! clipping or the gap-owner rule shows up there as a disagreement rather than
//! as a hand-written expectation someone has to keep in step.

use std::sync::Arc;
use std::time::Duration;

use ondadb::format::{CAP_EXTENDED_RECORDS, CAP_MANIFEST_EDITS, CAP_RANGE_DELETES};
use ondadb::manifest::SstMeta;
use ondadb::{
    ColumnFamily, ColumnFamilyConfig, IsolationLevel, OndaError, Options, PartitionRule, DB,
};

// ---- fixtures ---------------------------------------------------------------

fn open_with(opts: Options, cfg: ColumnFamilyConfig) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(opts).unwrap();
    let cf = db.create_column_family("default", cfg).unwrap();
    (db, cf)
}

/// A database with range deletes enabled.
fn open_ranged(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let (db, cf) = open_with(
        Options::new(dir.to_str().unwrap()),
        ColumnFamilyConfig::default(),
    );
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    (db, cf)
}

/// A database that has NOT enabled range deletes.
fn open_plain(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    open_with(
        Options::new(dir.to_str().unwrap()),
        ColumnFamilyConfig::default(),
    )
}

/// Reopen `dir`. `DB::open` restores the catalogued column families, so the
/// handle is fetched rather than created.
fn reopen(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .get_column_family("default")
        .expect("default is catalogued");
    (db, cf)
}

/// Reopen `dir` read-only. Excise returns early on a read-only handle, so this
/// is how a test inspects a catalog without a background pre-pass changing it
/// underneath the assertion.
fn reopen_read_only(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.read_only = true;
    let db = DB::open(opts).unwrap();
    let cf = db
        .get_column_family("default")
        .expect("default is catalogued");
    (db, cf)
}

fn part(prefix: &[u8], name: &str) -> PartitionRule {
    PartitionRule {
        prefix: prefix.to_vec(),
        name: name.to_string(),
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn tables(cf: &Arc<ColumnFamily>) -> Vec<SstMeta> {
    cf.table_metadata().into_iter().flatten().collect()
}

fn total_fragments(cf: &Arc<ColumnFamily>) -> u64 {
    tables(cf).iter().map(|m| m.range_count).sum()
}

// ---- task 2: API and validation --------------------------------------------

#[test]
fn delete_range_requires_capability() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_plain(dir.path());
    let e = db.delete_range(&cf, b"a", b"z").unwrap_err();
    assert_eq!(e.kind(), "invalid_args");
    assert!(
        e.to_string().contains("CAP_RANGE_DELETES"),
        "the error must name the capability: {e}"
    );
    // Enabling it also takes CAP_EXTENDED_RECORDS: a range delete is written as
    // a kind-bearing envelope record, so the two are one permission.
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    assert_eq!(
        db.format_capabilities() & (CAP_RANGE_DELETES | CAP_EXTENDED_RECORDS),
        CAP_RANGE_DELETES | CAP_EXTENDED_RECORDS
    );
    db.delete_range(&cf, b"a", b"z").unwrap();
    db.close().unwrap();
}

#[test]
fn delete_range_rejects_reversed_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for (start, end) in [(&b"z"[..], &b"a"[..]), (b"m", b"m")] {
        let e = db.delete_range(&cf, start, end).unwrap_err();
        assert_eq!(e.kind(), "invalid_args", "{start:?}..{end:?}");
        assert!(e.to_string().contains("strictly before"), "{e}");
    }
    // Half-open and non-empty is fine, however tight.
    db.delete_range(&cf, b"m", b"m\0").unwrap();
    db.close().unwrap();
}

#[test]
fn delete_range_rejects_empty_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for (start, end) in [(&b""[..], &b"z"[..]), (b"a", b""), (b"", b"")] {
        let e = db.delete_range(&cf, start, end).unwrap_err();
        assert_eq!(e.kind(), "invalid_args", "{start:?}..{end:?}");
        assert!(e.to_string().contains("non-empty"), "{e}");
    }
    db.close().unwrap();
}

#[test]
fn own_write_inside_own_range_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    let mut t = db.begin();
    t.put(&cf, b"kmid", b"v", Duration::ZERO).unwrap();
    t.delete_range(&cf, b"k", b"l").unwrap();
    let e = t.commit().unwrap_err();
    assert_eq!(e.kind(), "invalid_args");
    let msg = e.to_string();
    // The error must name BOTH the key and the span, so the caller can see
    // which of its own writes collided with which of its own deletes.
    assert!(
        msg.contains("107, 109, 105, 100"),
        "must name the key: {msg}"
    );
    assert!(msg.contains("[107]"), "must name the span start: {msg}");
    assert!(msg.contains("[108]"), "must name the span end: {msg}");

    // The order of the two calls does not matter: the check is over the whole
    // write set, not over a prefix of it.
    let mut t = db.begin();
    t.delete_range(&cf, b"k", b"l").unwrap();
    t.put(&cf, b"kmid", b"v", Duration::ZERO).unwrap();
    assert_eq!(t.commit().unwrap_err().kind(), "invalid_args");
    db.close().unwrap();
}

#[test]
fn own_write_outside_own_range_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    // The precision case: both kinds in one transaction, no containment.
    let mut t = db.begin();
    t.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    t.delete_range(&cf, b"m", b"z").unwrap();
    t.commit()
        .expect("a disjoint put and range delete must commit");
    assert_eq!(db.get(&cf, b"a").unwrap(), b"1");

    // And the boundary: `end` is exclusive, so a write AT `end` is outside.
    let mut t = db.begin();
    t.put(&cf, b"z", b"2", Duration::ZERO).unwrap();
    t.delete_range(&cf, b"m", b"z").unwrap();
    t.commit()
        .expect("end is exclusive, so a write at end is outside");
    assert_eq!(db.get(&cf, b"z").unwrap(), b"2");
    db.close().unwrap();
}

#[test]
fn range_delete_read_your_writes_inside_the_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    db.put(&cf, b"m1", b"v", Duration::ZERO).unwrap();
    db.put(&cf, b"z1", b"v", Duration::ZERO).unwrap();
    let mut t = db.begin();
    assert_eq!(t.get(&cf, b"m1").unwrap(), b"v");
    t.delete_range(&cf, b"m", b"n").unwrap();
    assert!(matches!(t.get(&cf, b"m1"), Err(OndaError::NotFound)));
    assert_eq!(t.get(&cf, b"z1").unwrap(), b"v");
    // multi_get agrees with get, key for key.
    let got = t.multi_get(&cf, &[b"m1", b"z1"]);
    assert!(got[0].is_err());
    assert_eq!(got[1].as_deref().unwrap(), b"v");
    t.rollback().unwrap();
    // Rolled back: the store is untouched.
    assert_eq!(db.get(&cf, b"m1").unwrap(), b"v");
    db.close().unwrap();
}

// ---- task 5: span index and the commit guard --------------------------------

#[test]
fn range_commit_conflicts_with_overlapping_point_write() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    db.put(&cf, b"m1", b"old", Duration::ZERO).unwrap();

    // Snapshot taken BEFORE the concurrent point write.
    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    t.get(&cf, b"m1").unwrap();
    db.put(&cf, b"m5", b"new", Duration::ZERO).unwrap();
    t.delete_range(&cf, b"m", b"n").unwrap();
    let e = t.commit().unwrap_err();
    assert_eq!(e.kind(), "conflict");
    assert!(e.to_string().contains("overlaps a write"), "{e}");
    // The point write survives: the range commit never applied.
    assert_eq!(db.get(&cf, b"m5").unwrap(), b"new");

    // A DISJOINT span commits happily against the same marker.
    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    t.delete_range(&cf, b"p", b"q").unwrap();
    t.commit().expect("a disjoint span must not conflict");
    db.close().unwrap();
}

#[test]
fn point_write_conflicts_with_newer_covering_range() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    db.put(&cf, b"m1", b"old", Duration::ZERO).unwrap();

    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    t.get(&cf, b"m1").unwrap();
    // A range delete lands after the snapshot, covering the key t writes.
    db.delete_range(&cf, b"m", b"n").unwrap();
    t.put(&cf, b"m5", b"v", Duration::ZERO).unwrap();
    let e = t.commit().unwrap_err();
    assert_eq!(e.kind(), "conflict");
    assert!(e.to_string().contains("covered by a range delete"), "{e}");

    // Outside the span: no conflict.
    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    t.put(&cf, b"z5", b"v", Duration::ZERO).unwrap();
    t.commit()
        .expect("a key outside the span must not conflict");
    db.close().unwrap();
}

/// The five isolation levels, against the same interleaving: a range delete
/// commits between another transaction's snapshot and its commit.
///
/// Only the two conflict-checking levels refuse. `ReadCommitted` and
/// `ReadUncommitted` have no snapshot to violate; `RepeatableRead` pins a read
/// sequence but performs no write-write check (its documented behavior), so it
/// is unchanged by 1.2 as well.
#[test]
fn conflict_outcomes_per_isolation_level() {
    for (level, want_conflict) in [
        (IsolationLevel::ReadUncommitted, false),
        (IsolationLevel::ReadCommitted, false),
        (IsolationLevel::RepeatableRead, false),
        (IsolationLevel::Snapshot, true),
        (IsolationLevel::Serializable, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open_ranged(dir.path());
        db.put(&cf, b"m1", b"old", Duration::ZERO).unwrap();
        let mut t = db.begin_with_isolation(level);
        t.get(&cf, b"m1").unwrap();
        db.delete_range(&cf, b"m", b"n").unwrap();
        t.put(&cf, b"m5", b"v", Duration::ZERO).unwrap();
        let got = t.commit();
        assert_eq!(
            got.is_err(),
            want_conflict,
            "{level:?}: {:?}",
            got.err().map(|e| e.to_string())
        );
        db.close().unwrap();
    }
}

/// A range delete over a key a Serializable transaction **read** invalidates
/// that read exactly as a point overwrite would, even when every write of the
/// transaction lies outside the span.
///
/// This is a point conflict, not phantom protection: the key was read, and the
/// value that read returned is no longer current. `peek_seq` cannot see it —
/// the delete leaves no point version at the key — so the check has to ask the
/// span index. Snapshot does not validate reads, so it commits.
#[test]
fn range_delete_over_a_serializable_read_conflicts() {
    for (level, want_conflict) in [
        (IsolationLevel::Snapshot, false),
        (IsolationLevel::Serializable, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open_ranged(dir.path());
        db.put(&cf, b"m1", b"old", Duration::ZERO).unwrap();
        let mut t = db.begin_with_isolation(level);
        assert_eq!(t.get(&cf, b"m1").unwrap(), b"old");
        db.delete_range(&cf, b"a", b"z").unwrap();
        // Outside the span: the write itself conflicts with nothing.
        t.put(&cf, b"zz", b"v", Duration::ZERO).unwrap();
        let got = t.commit();
        assert_eq!(
            got.is_err(),
            want_conflict,
            "{level:?}: {:?}",
            got.as_ref().err().map(|e| e.to_string())
        );
        if want_conflict {
            assert!(
                matches!(got, Err(OndaError::Conflict(_))),
                "{level:?}: {got:?}"
            );
        }
        db.close().unwrap();
    }

    // A span that just covers the read key conflicts the same way.
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    db.put(&cf, b"m1", b"old", Duration::ZERO).unwrap();
    let mut t = db.begin_with_isolation(IsolationLevel::Serializable);
    t.get(&cf, b"m1").unwrap();
    db.delete_range(&cf, b"m", b"n").unwrap();
    t.put(&cf, b"other", b"v", Duration::ZERO).unwrap();
    assert!(matches!(t.commit(), Err(OndaError::Conflict(_))));

    // A range delete that does NOT cover the read key is not a conflict.
    db.put(&cf, b"m2", b"v2", Duration::ZERO).unwrap();
    let mut t = db.begin_with_isolation(IsolationLevel::Serializable);
    t.get(&cf, b"m2").unwrap();
    db.delete_range(&cf, b"p", b"q").unwrap();
    t.put(&cf, b"other", b"v", Duration::ZERO).unwrap();
    t.commit().unwrap();
    db.close().unwrap();
}

/// A ReadCommitted commit **containing a range delete** takes `commit_mu`,
/// which point-only ReadCommitted commits do not.
///
/// Observed by interleaving, not by inspecting the lock. The engine carries a
/// debug-only rendezvous *inside* the guard's scope
/// (`ondadb::db::commit_park`): a range commit parks there, and the test then
/// checks what a concurrent commit can do. If the ReadCommitted range path did
/// not take `commit_mu`, the Snapshot commit below — which does take it — would
/// complete while the range writer was parked, and the assertion would fail.
#[test]
#[cfg(debug_assertions)]
fn read_committed_range_commit_takes_commit_mu() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    let db = Arc::new(db);
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();

    let (entered, release) = ondadb::db::commit_park::arm(db.instance_id());
    let (d2, c2) = (db.clone(), cf.clone());
    let range_writer = std::thread::spawn(move || {
        let mut t = d2.begin_with_isolation(IsolationLevel::ReadCommitted);
        t.delete_range(&c2, b"m", b"n").unwrap();
        t.commit()
    });
    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("the range commit must reach the park");

    // A point commit that also takes `commit_mu`. It must NOT finish while the
    // range writer is parked inside the guard.
    let done = Arc::new(AtomicBool::new(false));
    let (d3, c3, f3) = (db.clone(), cf.clone(), done.clone());
    let point_writer = std::thread::spawn(move || {
        let mut t = d3.begin_with_isolation(IsolationLevel::Snapshot);
        t.put(&c3, b"z", b"1", Duration::ZERO).unwrap();
        let r = t.commit();
        f3.store(true, Ordering::SeqCst);
        r
    });
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !done.load(Ordering::SeqCst),
        "a Snapshot commit completed while a ReadCommitted RANGE commit was          inside the guard — the range path did not take commit_mu"
    );

    release.send(()).unwrap();
    range_writer.join().unwrap().unwrap();
    point_writer.join().unwrap().unwrap();
    ondadb::db::commit_park::disarm();

    // A point-only ReadCommitted commit, by contrast, never reaches the park at
    // all: arming it and committing one leaves the arm untouched.
    let (entered2, release2) = ondadb::db::commit_park::arm(db.instance_id());
    db.put(&cf, b"b", b"1", Duration::ZERO).unwrap();
    assert!(
        entered2.recv_timeout(Duration::from_millis(200)).is_err(),
        "a point-only ReadCommitted commit must not enter the range guard"
    );
    ondadb::db::commit_park::disarm();
    drop(release2);
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

#[test]
fn span_markers_prune_below_oldest_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    // A live snapshot holds the prune floor down.
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    for i in 0..8 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, b"k0000", b"k0004").unwrap();
    let with_snapshot = cf.stats().span_markers;
    assert!(
        with_snapshot > 0,
        "markers must accumulate while a snapshot is live"
    );
    drop(held);
    // The next range commit prunes: nothing below the (now advanced) oldest
    // snapshot can be the newer side of any conflict.
    db.delete_range(&cf, b"z0000", b"z0004").unwrap();
    let after = cf.stats().span_markers;
    assert!(
        after < with_snapshot + 2,
        "pruning must reclaim the markers the released snapshot pinned \
         (before={with_snapshot} after={after})"
    );
    db.close().unwrap();
}

#[test]
fn span_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = open_ranged(dir.path());
        db.put(&cf, b"m1", b"v", Duration::ZERO).unwrap();
        // A live snapshot pins the markers; without one they are pruned the
        // moment the range commit finishes, which is itself the point.
        let held = db.begin_with_isolation(IsolationLevel::Snapshot);
        db.delete_range(&cf, b"m", b"n").unwrap();
        assert!(cf.stats().span_markers > 0);
        drop(held);
        db.close().unwrap();
    }
    let (db, cf) = reopen(dir.path());
    // The index is in-memory conflict state, not durable data: a reopen with no
    // active transactions starts empty, and the DELETE is still in effect.
    assert_eq!(cf.stats().span_markers, 0);
    assert!(db.get(&cf, b"m1").is_err(), "the range delete survived");
    db.close().unwrap();
}

/// A range writer blocked on span-index capacity must not block anyone else.
///
/// The reservation is taken **before** `commit_mu`, so a waiting range writer
/// holds no lock: an ordinary Snapshot commit — which does take `commit_mu` —
/// completes while it waits. (Were the wait performed under the guard, the
/// point commit below would hang until the snapshot is released.)
#[test]
fn span_index_capacity_waits_before_commit_mu() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    // Two markers: enough to fill the index with point writes, not enough to
    // leave room for a range commit while a snapshot pins them.
    opts.span_index_capacity = 2;
    let db = Arc::new(DB::open(opts).unwrap());
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    // Fill the index and pin it with a live snapshot.
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    db.put(&cf, b"b", b"1", Duration::ZERO).unwrap();
    assert_eq!(cf.stats().span_markers, 2, "the index is full");

    let waiting = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let (d2, c2, w2, f2) = (db.clone(), cf.clone(), waiting.clone(), done.clone());
    let range_writer = std::thread::spawn(move || {
        let mut t = d2.begin_with_isolation(IsolationLevel::ReadCommitted);
        t.delete_range(&c2, b"m", b"n").unwrap();
        w2.store(true, Ordering::SeqCst);
        let r = t.commit();
        f2.store(true, Ordering::SeqCst);
        r
    });
    while !waiting.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        !done.load(Ordering::SeqCst),
        "the range commit must be waiting for capacity"
    );

    // THE ASSERTION: the blocked range writer holds no `commit_mu`, so a
    // Snapshot point commit — which does take it — completes anyway. It also
    // finds the index full, and takes the overflow path rather than waiting.
    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    t.put(&cf, b"c", b"1", Duration::ZERO).unwrap();
    t.commit()
        .expect("a point commit must not stall behind a capacity wait");
    assert_eq!(db.get(&cf, b"c").unwrap(), b"1");

    // Releasing the snapshot lets pruning free the slots.
    drop(held);
    range_writer
        .join()
        .unwrap()
        .expect("the range commit completes once capacity frees up");
    assert!(db.get(&cf, b"m5").is_err());
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

/// A point commit that cannot be indexed raises the overflow watermark, and a
/// range writer reading below it conflicts rather than trusting an index that
/// is missing a marker.
#[test]
fn span_index_overflow_makes_range_writers_conservative() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.span_index_capacity = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    // The reader's snapshot predates the overflow — and is what pins the index
    // full, so the writes below cannot be indexed.
    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    // The index is now full; this one overflows — and commits anyway, because a
    // point write must never stall behind a bookkeeping structure.
    db.put(&cf, b"far-away", b"1", Duration::ZERO).unwrap();
    assert_eq!(db.get(&cf, b"far-away").unwrap(), b"1");

    // A Snapshot range delete over a span containing NEITHER key still
    // conflicts: the index cannot prove nothing changed in it.
    t.delete_range(&cf, b"m", b"n").unwrap();
    assert_eq!(
        t.commit().unwrap_err().kind(),
        "conflict",
        "a lost marker must be reported conservatively"
    );
    db.close().unwrap();
}

// ---- task 7: point-read masking ---------------------------------------------

#[test]
fn point_read_masked_by_memtable_range() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..10 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(3), &key(7)).unwrap();
    for i in 0..10 {
        let got = db.get(&cf, &key(i));
        assert_eq!(
            got.is_ok(),
            !(3..7).contains(&i),
            "k{i:04} (end is exclusive)"
        );
    }
    // A write AFTER the range delete is visible again — a range tombstone is a
    // sequence, not a permanent hole.
    db.put(&cf, &key(4), b"again", Duration::ZERO).unwrap();
    assert_eq!(db.get(&cf, &key(4)).unwrap(), b"again");
    // multi_get must agree with get, key for key.
    let keys: Vec<Vec<u8>> = (0..10).map(key).collect();
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    for (i, got) in db.multi_get(&cf, &refs).into_iter().enumerate() {
        assert_eq!(got.is_ok(), db.get(&cf, refs[i]).is_ok(), "k{i:04}");
    }
    db.close().unwrap();
}

#[test]
fn point_read_masked_by_sst_fragment() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..10 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(3), &key(7)).unwrap();
    db.flush_memtable(&cf).unwrap();
    assert!(
        total_fragments(&cf) > 0,
        "the flush must have published fragments"
    );
    for i in 0..10 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(3..7).contains(&i),
            "k{i:04}"
        );
    }
    // And after a reopen, from the durable bytes alone.
    db.close().unwrap();
    let (db, cf) = reopen(dir.path());
    for i in 0..10 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(3..7).contains(&i),
            "k{i:04}"
        );
    }
    db.close().unwrap();
}

/// The gap-owner rule: a level->=1 table whose *point* bounds exclude the key
/// still owns the fragment covering it.
///
/// Built by deleting a whole contiguous run of keys so the surviving points
/// leave a gap wider than any one table's point range, then compacting so the
/// fragments are clipped to output intervals. Reading a key inside the gap must
/// find the tombstone through the table to its LEFT.
#[test]
fn gap_owner_table_is_consulted() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    // Small files, so one compaction produces several outputs and the gap
    // between two of them is a real interval someone has to own.
    let cfg = ColumnFamilyConfig {
        target_file_size: 4 << 10,
        ..Default::default()
    };
    let (db, cf) = open_with(opts, cfg);
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    for i in 0..400 {
        db.put(&cf, &key(i), &[b'v'; 64], Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    // A snapshot taken BEFORE the delete keeps both the covered points and the
    // tombstone alive through the bottom compaction — otherwise the compaction
    // reclaims both and there is no fragment left to consult.
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.delete_range(&cf, &key(100), &key(300)).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    let metas = tables(&cf);
    assert!(
        metas.iter().any(|m| m.range_count > 0),
        "compaction must carry the fragments forward: {metas:?}"
    );
    // At least one table must own a fragment reaching past its own point keys —
    // otherwise the gap-owner rule is not under test.
    assert!(
        metas.iter().any(|m| m
            .range_max_key
            .as_deref()
            .is_some_and(|e| e > m.max_key.as_slice())),
        "no table owns a fragment past its last point key: {:?}",
        metas
            .iter()
            .map(|m| (
                String::from_utf8_lossy(&m.min_key).to_string(),
                String::from_utf8_lossy(&m.max_key).to_string(),
                m.range_min_key
                    .as_deref()
                    .map(|k| String::from_utf8_lossy(k).to_string()),
                m.range_max_key
                    .as_deref()
                    .map(|k| String::from_utf8_lossy(k).to_string()),
            ))
            .collect::<Vec<_>>()
    );
    for i in 0..400 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(100..300).contains(&i),
            "k{i:04} after compaction"
        );
    }
    // The pinned snapshot still sees everything: the tombstone is newer than it.
    let t = held;
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        n += 1;
        it.next();
    }
    assert_eq!(n, 400, "the pinned snapshot predates the range delete");
    drop(t);
    db.close().unwrap();
}

/// A column family that never issues a range delete must not allocate for the
/// feature on the read path.
///
/// Proved with a counter, not by inspection: every source reports emptiness
/// through a relaxed load or a `range_count == 0` comparison, so the mask is
/// never built and no fragment vector is ever created.
#[test]
#[cfg(debug_assertions)]
fn no_range_cf_allocates_nothing_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_plain(dir.path());
    for i in 0..200 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    for i in 0..200 {
        db.put(&cf, &key(i), b"v2", Duration::ZERO).unwrap();
    }

    let stats = cf.stats();
    assert_eq!(stats.range_deletes, 0);
    assert_eq!(stats.range_fragments, 0);
    assert_eq!(stats.span_markers, 0, "the span index stays inert");
    for m in tables(&cf) {
        assert_eq!(m.range_count, 0, "table {} carries no fragments", m.id);
        assert!(m.range_min_key.is_none());
    }

    ondadb::range_tombstone::reset_mask_sources();
    for i in 0..200 {
        db.get(&cf, &key(i)).unwrap();
    }
    assert_eq!(scan_forward(&db, &cf).len(), 200);
    assert_eq!(scan_backward(&db, &cf).len(), 200);
    let keys: Vec<Vec<u8>> = (0..200).map(key).collect();
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    assert_eq!(db.multi_get(&cf, &refs).len(), 200);
    let materialized = ondadb::range_tombstone::mask_sources();
    assert_eq!(
        materialized, 0,
        "a point-only column family materialized {materialized} range-mask sources"
    );
    db.close().unwrap();
}

// ---- task 8: iterator masking -----------------------------------------------

fn scan_forward(db: &DB, cf: &Arc<ColumnFamily>) -> Vec<Vec<u8>> {
    let t = db.begin();
    let mut it = t.new_iterator(cf);
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
        out.push(it.key().to_vec());
        it.next();
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    out
}

fn scan_backward(db: &DB, cf: &Arc<ColumnFamily>) -> Vec<Vec<u8>> {
    let t = db.begin();
    let mut it = t.new_iterator(cf);
    it.seek_to_last();
    let mut out = Vec::new();
    while it.valid() {
        out.push(it.key().to_vec());
        it.prev();
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    out.reverse();
    out
}

#[test]
fn forward_iter_skips_covered_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..10 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(3), &key(7)).unwrap();
    let want: Vec<Vec<u8>> = (0..10).filter(|i| !(3..7).contains(i)).map(key).collect();
    assert_eq!(scan_forward(&db, &cf), want);
    // Same answer from durable fragments.
    db.flush_memtable(&cf).unwrap();
    assert_eq!(scan_forward(&db, &cf), want);
    db.close().unwrap();
}

#[test]
fn reverse_iter_skips_covered_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..10 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(3), &key(7)).unwrap();
    let want: Vec<Vec<u8>> = (0..10).filter(|i| !(3..7).contains(i)).map(key).collect();
    assert_eq!(scan_backward(&db, &cf), want);
    db.flush_memtable(&cf).unwrap();
    assert_eq!(scan_backward(&db, &cf), want);
    db.close().unwrap();
}

#[test]
fn bounded_iter_respects_range_and_bounds() {
    use std::ops::Bound;
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..20 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(5), &key(15)).unwrap();
    db.flush_memtable(&cf).unwrap();

    let (lo, hi) = (key(3), key(18));
    let t = db.begin();
    let mut it = t.new_iterator_bounded(
        &cf,
        Bound::Included(lo.as_slice()),
        Bound::Excluded(hi.as_slice()),
    );
    it.seek_to_first();
    let mut got = Vec::new();
    while it.valid() {
        got.push(it.key().to_vec());
        it.next();
    }
    let want: Vec<Vec<u8>> = (3..18).filter(|i| !(5..15).contains(i)).map(key).collect();
    assert_eq!(got, want);

    // The reverse walk over the same bounds yields the same set.
    let mut it = t.new_iterator_bounded(
        &cf,
        Bound::Included(lo.as_slice()),
        Bound::Excluded(hi.as_slice()),
    );
    it.seek_to_last();
    let mut back = Vec::new();
    while it.valid() {
        back.push(it.key().to_vec());
        it.prev();
    }
    back.reverse();
    assert_eq!(back, want);
    db.close().unwrap();
}

// ---- the reference model ----------------------------------------------------

/// One step of a generated history.
#[derive(Debug, Clone)]
enum Step {
    Put(u32),
    Delete(u32),
    DeleteRange(u32, u32),
    Flush,
    Compact,
}

/// The oracle: replay a history over a plain map, applying range deletes by
/// erasing the covered keys at the moment they commit. Legal because every step
/// commits before the next begins, so there is exactly one sequence order and a
/// tombstone's effect is total over what precedes it.
fn oracle(steps: &[Step], span: u32) -> Vec<u32> {
    let mut live = vec![false; span as usize];
    for step in steps {
        match *step {
            Step::Put(k) => live[k as usize] = true,
            Step::Delete(k) => live[k as usize] = false,
            Step::DeleteRange(a, b) => {
                for slot in live.iter_mut().take(b as usize).skip(a as usize) {
                    *slot = false;
                }
            }
            Step::Flush | Step::Compact => {}
        }
    }
    (0..span).filter(|i| live[*i as usize]).collect()
}

fn run_history(db: &DB, cf: &Arc<ColumnFamily>, steps: &[Step]) {
    for step in steps {
        match *step {
            Step::Put(k) => db.put(cf, &key(k), b"v", Duration::ZERO).unwrap(),
            Step::Delete(k) => db.delete(cf, &key(k)).unwrap(),
            Step::DeleteRange(a, b) => db.delete_range(cf, &key(a), &key(b)).unwrap(),
            Step::Flush => db.flush_memtable(cf).unwrap(),
            Step::Compact => db.compact(cf).unwrap(),
        }
    }
}

fn histories(span: u32, rounds: usize) -> Vec<Vec<Step>> {
    let mut rng = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    (0..rounds)
        .map(|_| {
            let n = 8 + (next() % 24) as usize;
            (0..n)
                .map(|_| match next() % 10 {
                    0..=4 => Step::Put((next() % u64::from(span)) as u32),
                    5 => Step::Delete((next() % u64::from(span)) as u32),
                    6..=7 => {
                        let a = (next() % u64::from(span - 1)) as u32;
                        let b = a + 1 + (next() % u64::from(span - a - 1)) as u32;
                        Step::DeleteRange(a, b)
                    }
                    8 => Step::Flush,
                    _ => Step::Compact,
                })
                .collect()
        })
        .collect()
}

/// The centerpiece: random histories of puts, point deletes, range deletes,
/// flushes and compactions, checked against the oracle through **every** read
/// surface — `get`, `multi_get`, forward iteration and reverse iteration — and
/// again after a reopen, which reads the durable bytes alone.
#[test]
fn iter_range_mask_matches_point_get_oracle() {
    const SPAN: u32 = 24;
    for (round, steps) in histories(SPAN, 40).into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open_ranged(dir.path());
        run_history(&db, &cf, &steps);

        let want: Vec<Vec<u8>> = oracle(&steps, SPAN).into_iter().map(key).collect();
        let ctx = || format!("round {round}: {steps:?}");

        let got: Vec<Vec<u8>> = (0..SPAN)
            .filter(|i| db.get(&cf, &key(*i)).is_ok())
            .map(key)
            .collect();
        assert_eq!(got, want, "point get disagrees — {}", ctx());

        let keys: Vec<Vec<u8>> = (0..SPAN).map(key).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let batched: Vec<Vec<u8>> = db
            .multi_get(&cf, &refs)
            .into_iter()
            .zip(&keys)
            .filter(|(r, _)| r.is_ok())
            .map(|(_, k)| k.clone())
            .collect();
        assert_eq!(batched, want, "multi_get disagrees — {}", ctx());
        assert_eq!(scan_forward(&db, &cf), want, "forward scan — {}", ctx());
        assert_eq!(scan_backward(&db, &cf), want, "reverse scan — {}", ctx());

        db.close().unwrap();
        let (db, cf) = reopen(dir.path());
        assert_eq!(scan_forward(&db, &cf), want, "after reopen — {}", ctx());
        db.close().unwrap();
    }
}

// ---- task 9: flush fragmentation --------------------------------------------

#[test]
fn flush_emits_fragments() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..10 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(2), &key(5)).unwrap();
    db.delete_range(&cf, &key(4), &key(8)).unwrap();
    db.flush_memtable(&cf).unwrap();

    let metas = tables(&cf);
    assert_eq!(metas.len(), 1, "one flush, one table");
    let m = &metas[0];
    // Two overlapping spans fragment into three intervals; the middle one
    // carries both sequences.
    assert_eq!(m.range_count, 3, "{m:?}");
    assert_eq!(m.range_min_key.as_deref(), Some(key(2).as_slice()));
    assert_eq!(m.range_max_key.as_deref(), Some(key(8).as_slice()));
    assert!(m.range_min_seq > 0 && m.range_max_seq >= m.range_min_seq);
    assert_eq!(cf.stats().range_fragments, 3);
    assert_eq!(cf.stats().range_deletes, 2);
    db.close().unwrap();
}

/// A memtable holding **only** range tombstones must still flush: dropping it
/// as "empty" would lose the deletes.
#[test]
fn flush_of_range_only_memtable_produces_a_table() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..10 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.delete_range(&cf, &key(2), &key(5)).unwrap();
    db.flush_memtable(&cf).unwrap();
    assert!(total_fragments(&cf) > 0, "the span reached a table");
    for i in 0..10 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(2..5).contains(&i),
            "k{i:04}"
        );
    }
    db.close().unwrap();
}

/// Both flush builds — the streaming arena merge (`unsafe-fastpath`) and the
/// snapshot-and-write one — must publish the same fragments. Whichever build is
/// running, the invariant is that the fragments describe the memtable's spans
/// exactly, so the two are compared through their observable result.
#[test]
fn streaming_flush_matches_batch_flush_fragments() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..40 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(5), &key(12)).unwrap();
    db.delete_range(&cf, &key(10), &key(20)).unwrap();
    db.delete_range(&cf, &key(30), &key(33)).unwrap();
    let before = scan_forward(&db, &cf);
    db.flush_memtable(&cf).unwrap();
    let after = scan_forward(&db, &cf);
    assert_eq!(
        before, after,
        "the flush must not change what the scan returns"
    );
    let m = &tables(&cf)[0];
    // Boundaries: 5, 10, 12, 20, 30, 33 -> [5,10) [10,12) [12,20) [30,33).
    assert_eq!(m.range_count, 4, "{m:?}");
    db.close().unwrap();
}

#[test]
fn ingest_l0_refuses_range_fragments() {
    // Externally ingested files are point-only, and a database that has not
    // enabled the capability may not receive fragments at all: the table would
    // have to be extended, and a reopen could not attribute it.
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_plain(dir.path());
    for i in 0..10 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    for m in tables(&cf) {
        assert_eq!(m.range_count, 0);
    }
    let e = db.delete_range(&cf, b"a", b"z").unwrap_err();
    assert_eq!(e.kind(), "invalid_args");
    db.close().unwrap();
}

// ---- task 10: compaction ----------------------------------------------------

/// The binding rule: fragments are clipped to the interval each output owns,
/// `[o_i.min_key, o_{i+1}.min_key)`, with the first extended down to the job
/// span and the last up to it. The intervals must tile the span and be
/// disjoint, because level->=1 point disjointness — and therefore
/// `find_overlapping` — depends on it.
#[test]
fn fragments_clipped_to_output_intervals() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let cfg = ColumnFamilyConfig {
        target_file_size: 4 << 10,
        ..Default::default()
    };
    let (db, cf) = open_with(opts, cfg);
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    for i in 0..300 {
        db.put(&cf, &key(i), &[b'v'; 64], Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    // Spans reaching across many output boundaries, but not covering
    // everything: the surviving points force several outputs.
    db.delete_range(&cf, &key(50), &key(90)).unwrap();
    db.delete_range(&cf, &key(200), &key(240)).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    // Every level: a shallow family's bottom IS L0, and after a bottom
    // compaction its outputs are disjoint and key-ordered like any other level.
    let mut sorted: Vec<SstMeta> = cf
        .table_metadata()
        .into_iter()
        .flatten()
        .filter(|m| !m.min_key.is_empty())
        .collect();
    sorted.sort_by(|a, b| a.min_key.cmp(&b.min_key));
    assert!(sorted.len() > 1, "the job must produce several outputs");
    for pair in sorted.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        assert!(a.max_key < b.min_key, "point bounds must stay disjoint");
        if let Some(end) = &a.range_max_key {
            assert!(
                end.as_slice() <= b.min_key.as_slice(),
                "output {} owns [{:?}) past the next output's min_key {:?}",
                a.id,
                String::from_utf8_lossy(end),
                String::from_utf8_lossy(&b.min_key),
            );
        }
        if let Some(start) = &b.range_min_key {
            assert!(
                start.as_slice() >= a.max_key.as_slice(),
                "output {} owns a fragment starting inside its predecessor",
                b.id
            );
        }
    }
    assert!(
        sorted.iter().any(|m| m.range_count > 0),
        "the clipped fragments must survive the compaction"
    );
    // The masking still holds after all that clipping.
    for i in 0..300 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(50..90).contains(&i) && !(200..240).contains(&i),
            "k{i:04}"
        );
    }
    drop(held);
    db.close().unwrap();
}

/// A compaction's span must be the union of its inputs' **span** bounds, not
/// their point bounds: an input fragment reaching past the last point key would
/// otherwise fall outside the job and be lost when its owner is dropped.
#[test]
fn gather_target_expands_to_input_span_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    // A table whose fragment reaches far past its own last point key.
    db.put(&cf, &key(1), b"v", Duration::ZERO).unwrap();
    db.delete_range(&cf, &key(1), &key(900)).unwrap();
    db.flush_memtable(&cf).unwrap();
    // Older data that the span covers, in a separate table.
    db.put(&cf, &key(500), b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    // `k0500` was written AFTER the tombstone, so it survives.
    assert_eq!(db.get(&cf, &key(500)).unwrap(), b"v");

    db.compact(&cf).unwrap();
    assert_eq!(
        db.get(&cf, &key(500)).unwrap(),
        b"v",
        "a key written after the tombstone must survive compaction"
    );
    assert!(db.get(&cf, &key(1)).is_err(), "k0001 stays deleted");
    db.close().unwrap();
}

/// A fragment survives every non-bottom compaction and is dropped only at the
/// bottom, once nothing older can resurface.
#[test]
fn fragment_survives_until_bottom() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    for i in 0..40 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.delete_range(&cf, &key(10), &key(30)).unwrap();
    db.flush_memtable(&cf).unwrap();
    assert!(total_fragments(&cf) > 0, "fragments start out durable");

    db.compact(&cf).unwrap();
    // Everything reached the bottom, no snapshot is live, and no mount is in
    // the way: both the covered points and the tombstone that hid them are
    // reclaimed. What must NOT change is the answer.
    for i in 0..40 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(10..30).contains(&i),
            "k{i:04}"
        );
    }
    assert_eq!(
        total_fragments(&cf),
        0,
        "a fully collapsed bottom drops the tombstone with the data it hid"
    );
    // ... and the data really is gone, not merely masked.
    let stats = cf.stats();
    assert!(
        stats.num_entries <= 20,
        "the covered points were reclaimed, not just hidden: {stats:?}"
    );
    db.close().unwrap();
}

/// A live snapshot keeps the fragment alive through the bottom compaction: a
/// reader at the older sequence must still see the tombstone.
#[test]
fn fragment_not_dropped_under_a_live_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    for i in 0..40 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    // Taken BEFORE the delete: this reader must still see the covered keys, so
    // neither they nor the tombstone that hides them from newer readers may be
    // reclaimed.
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.delete_range(&cf, &key(10), &key(30)).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert!(
        total_fragments(&cf) > 0,
        "a live snapshot pins the tombstone"
    );
    drop(held);
    for i in 0..40 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(10..30).contains(&i),
            "k{i:04}"
        );
    }
    db.close().unwrap();
}

/// Level->=1 disjointness, as a property over random compactions.
///
/// This is the invariant the whole read path rests on: `find_overlapping` does
/// a binary search on point bounds and returns at most one table per level, so
/// two level->=1 tables must never both own the same key — for points *or* for
/// fragments. Consulting the gap owner is what covers the one table the search
/// can miss; that is only sound while the intervals stay disjoint.
#[test]
fn level1_span_intervals_stay_disjoint() {
    const SPAN: u32 = 60;
    for (round, steps) in histories(SPAN, 12).into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.num_compaction_threads = 1;
        let cfg = ColumnFamilyConfig {
            target_file_size: 2 << 10,
            ..Default::default()
        };
        let (db, cf) = open_with(opts, cfg);
        db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
        run_history(&db, &cf, &steps);
        db.compact(&cf).unwrap();

        for (level, metas) in cf.table_metadata().into_iter().enumerate().skip(1) {
            let mut sorted = metas;
            sorted.sort_by(|a, b| a.min_key.cmp(&b.min_key));
            for pair in sorted.windows(2) {
                let (a, b) = (&pair[0], &pair[1]);
                assert!(
                    a.max_key < b.min_key,
                    "round {round} level {level}: point bounds overlap"
                );
                if let (Some(ae), Some(bs)) = (&a.range_max_key, &b.range_min_key) {
                    assert!(
                        ae <= bs || ae.as_slice() <= b.min_key.as_slice(),
                        "round {round} level {level}: fragment intervals overlap \
                         ({:?} vs {:?})",
                        String::from_utf8_lossy(ae),
                        String::from_utf8_lossy(bs)
                    );
                }
            }
        }
        db.close().unwrap();
    }
}

/// Partition-cut outputs never contain a fragment crossing a partition.
///
/// The clipping rule delivers this for free: a bottom output is already cut at
/// every partition boundary, and fragments are clipped to the interval each
/// output owns. The test pins the consequence rather than the mechanism.
#[test]
fn flush_fragments_cut_at_partition_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let cfg = ColumnFamilyConfig {
        partition_rules: vec![part(b"a/", "a"), part(b"b/", "b"), part(b"c/", "c")],
        ..Default::default()
    };
    let (db, cf) = open_with(opts, cfg);
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    for p in [&b"a/"[..], b"b/", b"c/"] {
        for i in 0..20u32 {
            let mut k = p.to_vec();
            k.extend_from_slice(format!("{i:03}").as_bytes());
            db.put(&cf, &k, b"v", Duration::ZERO).unwrap();
        }
    }
    db.flush_memtable(&cf).unwrap();
    // One span crossing all three partitions.
    db.delete_range(&cf, b"a/005", b"c/005").unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    for metas in cf.table_metadata().into_iter().skip(1) {
        for m in metas {
            let Some(part) = &m.partition else { continue };
            let (Some(lo), Some(hi)) = (&m.range_min_key, &m.range_max_key) else {
                continue;
            };
            let prefix = format!("{part}/");
            assert!(
                lo.starts_with(prefix.as_bytes()) || hi.starts_with(prefix.as_bytes()),
                "table {} in partition {part} owns a fragment [{:?}, {:?}) outside it",
                m.id,
                String::from_utf8_lossy(lo),
                String::from_utf8_lossy(hi),
            );
        }
    }
    // And the deletion is still correct across the boundaries.
    for (p, lo) in [(&b"a/"[..], 5u32), (b"b/", 0), (b"c/", 5)] {
        for i in 0..20u32 {
            let mut k = p.to_vec();
            k.extend_from_slice(format!("{i:03}").as_bytes());
            let covered = if p == b"c/" { i < lo } else { i >= lo };
            assert_eq!(
                db.get(&cf, &k).is_ok(),
                !covered,
                "{}",
                String::from_utf8_lossy(&k)
            );
        }
    }
    db.close().unwrap();
}

// ---- slice 11: surfaces ------------------------------------------------------

/// Detach and re-attach a part carrying range fragments: the summary is
/// re-derived from the table's own aux section, so the tombstones keep masking.
///
/// Both attach paths rebuild `SstMeta` from the reader rather than trusting a
/// foreign catalog. Before slice 11 that meant a table's `range_count` came back
/// as zero while the file still held its fragments, and the read path — which
/// gates on exactly that field — stopped masking, resurrecting the deleted data;
/// attaching such a table was refused outright. `adopt_range_summary` closes it
/// by running the same `summarize` the writer used.
#[test]
fn attach_adopts_range_fragments() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let cfg = ColumnFamilyConfig {
        partition_rules: vec![part(b"p/", "p")],
        ..Default::default()
    };
    let (db, cf) = open_with(opts, cfg);
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();

    let k = |i: u32| format!("p/{i:04}").into_bytes();
    for i in 0..200u32 {
        db.put(&cf, &k(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    // A snapshot taken before the delete keeps the fragment alive through the
    // compaction that pushes everything to the bottom — and keeps the excise
    // pre-pass from reclaiming the covered table before the part is detached.
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.delete_range(&cf, b"p/0050", b"p/0150").unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    let before = total_fragments(&cf);
    assert!(before > 0, "the part must carry fragments to mean anything");

    let detached = db.detach_part(&cf, "p").unwrap();
    assert!(!detached.table_ids.is_empty(), "the part held tables");
    assert_eq!(total_fragments(&cf), 0, "the detach took the fragments out");
    assert!(
        db.get(&cf, &k(10)).is_err(),
        "a detached part's live keys are gone for new reads too"
    );

    db.attach_part(&cf, &detached.dir).unwrap();
    assert_eq!(
        total_fragments(&cf),
        before,
        "every fragment came back through the re-derived summary"
    );
    for i in 0..200u32 {
        assert_eq!(
            db.get(&cf, &k(i)).is_ok(),
            !(50..150).contains(&i),
            "p/{i:04} after re-attach"
        );
    }
    drop(held);
    db.close().unwrap();
}

/// A checkpoint links the klog whole — aux block included — and copies the
/// catalog's `range_*` fields, so the copy masks exactly what the source did.
#[test]
fn checkpoint_preserves_range_masking() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.delete_range(&cf, &key(10), &key(30)).unwrap();
    db.flush_memtable(&cf).unwrap();
    assert!(total_fragments(&cf) > 0);

    let copy = tempfile::tempdir().unwrap();
    db.checkpoint(copy.path()).unwrap();
    let backup = tempfile::tempdir().unwrap();
    db.backup(backup.path()).unwrap();
    drop(held);
    db.close().unwrap();

    for (what, at) in [("checkpoint", copy.path()), ("backup", backup.path())] {
        let (db2, cf2) = reopen(at);
        assert!(total_fragments(&cf2) > 0, "{what} kept the fragments");
        for i in 0..40u32 {
            assert_eq!(
                db2.get(&cf2, &key(i)).is_ok(),
                !(10..30).contains(&i),
                "{what} k{i:04}"
            );
        }
        db2.close().unwrap();
    }
}

/// `clone_column_family` hard-links the same klogs under fresh ids and copies
/// each `SstMeta` verbatim, so the clone masks what the source masked.
#[test]
fn clone_column_family_preserves_range_masking() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.delete_range(&cf, &key(10), &key(30)).unwrap();
    db.flush_memtable(&cf).unwrap();

    let clone = db.clone_column_family("default", "copy").unwrap();
    assert_eq!(
        total_fragments(&clone),
        total_fragments(&cf),
        "the clone carries the same fragments"
    );
    for i in 0..40u32 {
        assert_eq!(
            db.get(&clone, &key(i)).is_ok(),
            !(10..30).contains(&i),
            "clone k{i:04}"
        );
    }
    drop(held);
    db.close().unwrap();
}

// ---- slice 10: delete-only excise -------------------------------------------

/// Tables of `cf` carrying no fragments of their own — the excise candidates.
fn covered_tables(cf: &Arc<ColumnFamily>) -> Vec<SstMeta> {
    tables(cf)
        .into_iter()
        .filter(|m| m.range_count == 0)
        .collect()
}

/// Build a database whose first table is fully shadowed by a durable fragment
/// owned by a second table, then **close and reopen** it.
///
/// The reopen is what makes the fixture deterministic. Publishing the fragment
/// wakes the compaction worker — that is the point of the 1.2 arm of
/// `should_schedule_compaction` — so a test that wants to observe *which* actor
/// reclaimed the table cannot leave that wakeup outstanding. A snapshot held
/// across the build makes the pre-pass that wakeup triggers a no-op, and the
/// reopened database's worker has never been signalled at all: two L0 files are
/// nowhere near `l1_file_count_trigger`. Returns the covered table's bytes.
fn covered_table_fixture(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>, u64) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    // The edit log too, so `catalog_txn` runs its 2.2 shape: the append's fsync
    // is the commit point and nothing is published before it. Without the log
    // the pre-capability path publishes first and a later snapshot write makes
    // the removal durable anyway (`detach_part`'s precedent) — a consistent
    // catalog either way, but not one whose commit point a test can pin.
    db.enable_format_capabilities(CAP_RANGE_DELETES | CAP_MANIFEST_EDITS)
        .unwrap();
    // Taken before anything is written, so the floor sits below every sequence
    // the build produces and no fragment can justify a drop while it lives.
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    for i in 0..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    // Half-open and strictly past the table's inclusive max_key.
    db.delete_range(&cf, &key(0), &key(40)).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.close().unwrap();
    drop(held);
    drop(cf);
    drop(db);

    let (db, cf) = reopen(dir);
    let covered = covered_tables(&cf);
    assert_eq!(covered.len(), 1, "one fully covered point-only table");
    let bytes = covered[0].klog_size + covered[0].vlog_size;
    assert!(bytes > 0, "the covered table holds bytes to reclaim");
    (db, cf, bytes)
}

#[test]
fn excise_drops_fully_covered_table() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf, _) = covered_table_fixture(dir.path());
    assert_eq!(db.excise_covered(&cf).unwrap(), 1);
    assert!(covered_tables(&cf).is_empty(), "the catalog lost it");
    // The answer is unchanged: the fragment that justified the drop is still
    // there, and every key still reads as deleted.
    for i in 0..40u32 {
        assert!(db.get(&cf, &key(i)).is_err(), "k{i:04}");
    }
    assert!(total_fragments(&cf) > 0, "the owner survived");
    db.close().unwrap();

    let (db, cf) = reopen(dir.path());
    for i in 0..40u32 {
        assert!(db.get(&cf, &key(i)).is_err(), "k{i:04} after reopen");
    }
    db.close().unwrap();
}

/// The stats an operator watches: a table reclaimed with no read and no
/// rewrite is counted apart from compaction.
#[test]
fn excise_counts_reclaimed_tables_and_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf, bytes) = covered_table_fixture(dir.path());
    assert_eq!(db.excise_covered(&cf).unwrap(), 1);
    let stats = cf.stats();
    assert_eq!(stats.excised_tables, 1, "one table retired by catalog edit");
    assert_eq!(stats.excised_bytes, bytes, "and the bytes it held");
    db.close().unwrap();
}

/// A live snapshot below the tombstone's sequence is a veto: the reader can
/// still see the data the fragment deletes.
#[test]
fn excise_refuses_under_a_live_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    for i in 0..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let held = db.begin_with_isolation(IsolationLevel::Snapshot);
    db.delete_range(&cf, &key(0), &key(40)).unwrap();
    db.flush_memtable(&cf).unwrap();

    assert_eq!(db.excise_covered(&cf).unwrap(), 0, "the snapshot vetoes it");
    assert_eq!(covered_tables(&cf).len(), 1, "the covered table stays");
    drop(held);
    assert_eq!(db.excise_covered(&cf).unwrap(), 1, "and goes once it lifts");
    db.close().unwrap();
}

/// Never drop the only durable owner of the tombstone that justifies the drop.
#[test]
fn excise_refuses_when_table_owns_its_own_covering_fragment() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    // Points and the covering span in ONE memtable, so one table carries both.
    for i in 0..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(0), &key(40)).unwrap();
    db.flush_memtable(&cf).unwrap();
    assert_eq!(tables(&cf).len(), 1, "one table holding both kinds");

    assert_eq!(db.excise_covered(&cf).unwrap(), 0);
    assert_eq!(
        tables(&cf).len(),
        1,
        "dropping it would destroy the evidence"
    );
    for i in 0..40u32 {
        assert!(db.get(&cf, &key(i)).is_err(), "k{i:04} still deleted");
    }
    db.close().unwrap();
}

/// The contiguous-set rule: two tables each owning part of the union that
/// covers the other. Dropping the set would destroy exactly the fragments that
/// justified dropping it, so the set is refused.
#[test]
fn excise_refuses_contiguous_set_owning_its_union() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    // Table 1 holds k0000..k0019 and the span covering k0020..k0039;
    // table 2 holds k0020..k0039 and the span covering k0000..k0019.
    for i in 0..20u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(20), &key(40)).unwrap();
    db.flush_memtable(&cf).unwrap();
    for i in 20..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, &key(0), &key(20)).unwrap();
    db.flush_memtable(&cf).unwrap();
    assert_eq!(tables(&cf).len(), 2);

    assert_eq!(db.excise_covered(&cf).unwrap(), 0);
    assert_eq!(tables(&cf).len(), 2, "neither member may go");
    db.close().unwrap();
}

/// A shared tier is a delete-free publication other databases may reference,
/// and obsolete-file deletion resolves default-tier paths only — so a table
/// that has left the default tier is never excised, however dead its keys are.
#[test]
fn excise_refuses_shared_tier_table() {
    for shared in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let mut opts = Options::new(dir.path().to_str().unwrap());
        let def = ondadb::TierDef::new("cold", tier.path().to_str().unwrap());
        opts.tiers = vec![if shared { def.shared() } else { def }];
        opts.num_compaction_threads = 1;
        let cfg = ColumnFamilyConfig {
            partition_rules: vec![part(b"k", "all")],
            ..Default::default()
        };
        let (db, cf) = open_with(opts, cfg);
        db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
        for i in 0..40u32 {
            db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        db.move_part_to_tier(&cf, "all", "cold").unwrap();
        assert!(
            tables(&cf)
                .iter()
                .all(|m| m.tier.as_deref() == Some("cold")),
            "shared={shared}: the part moved"
        );
        db.delete_range(&cf, &key(0), &key(40)).unwrap();
        db.flush_memtable(&cf).unwrap();

        assert_eq!(
            db.excise_covered(&cf).unwrap(),
            0,
            "shared={shared}: an off-default-tier table is never excised"
        );
        assert_eq!(
            tables(&cf)
                .iter()
                .filter(|m| m.tier.as_deref() == Some("cold"))
                .count(),
            1,
            "shared={shared}: the tiered table stays catalogued"
        );
        db.close().unwrap();
    }
}

/// The picker's pre-pass reclaims a covered table without an operator call.
#[test]
fn excise_runs_as_a_picker_pre_pass() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    for i in 0..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.delete_range(&cf, &key(0), &key(40)).unwrap();
    db.flush_memtable(&cf).unwrap();
    // Built without a snapshot hold, unlike `covered_table_fixture`: the flush
    // that published the fragment signalled the compaction worker, and the
    // pre-pass runs ahead of its capacity scoring.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !covered_tables(&cf).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the pre-pass never reclaimed the covered table"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    for i in 0..40u32 {
        assert!(db.get(&cf, &key(i)).is_err(), "k{i:04}");
    }
    db.close().unwrap();
}

// ---- failure matrix ---------------------------------------------------------

//
// The `LOCK` file makes in-process crash simulation impossible by design: a
// handle that is `forget`ten keeps the directory locked, so a reopen in the
// same process fails with `Locked`. The established pattern (see
// `tests/engine_regressions.rs`) is a child process that exits without closing.

const CRASH_DIR_ENV: &str = "ONDA_RANGE_CRASH_DIR";

/// Not a real test: the child half of the crash simulation. A no-op unless the
/// environment variable is set, so it costs nothing in a normal suite run.
#[test]
fn range_crash_helper() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    let dir = std::path::Path::new(&dir);
    let (db, cf) = open_ranged(dir);
    for i in 0..20 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    // Durable half: flushed into a table with its fragments.
    db.delete_range(&cf, &key(3), &key(7)).unwrap();
    db.flush_memtable(&cf).unwrap();
    assert!(total_fragments(&cf) > 0, "the flush published fragments");
    // WAL-only half: committed, never flushed.
    db.delete_range(&cf, &key(12), &key(16)).unwrap();
    // Simulated crash: no close(), no Drop.
    std::process::exit(0);
}

fn run_crash(dir: &std::path::Path) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["range_crash_helper", "--exact", "--nocapture"])
        .env(CRASH_DIR_ENV, dir.to_str().unwrap())
        .status()
        .expect("spawn the crash helper");
    assert!(status.success(), "crash helper child failed");
}

/// A crash with range deletes still only in the WAL: replay restores them, and
/// range deletes already flushed into a table are unaffected.
///
/// The manifest-rename half of the failure-matrix row is generic, not
/// range-specific: `flush_memtable` hands the work to a background worker, so a
/// thread-local fault cannot reach it, and the orphan sweep it exercises is the
/// default-tier sweep every flush already goes through (`tests/maintenance.rs`).
/// What IS specific to 1.2 — and asserted here — is that an unflushed range
/// delete survives a crash at all, and that a flushed one is not disturbed by
/// the replay of a later one.
#[test]
fn range_flush_crash() {
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path());
    let (db, cf) = reopen(dir.path());
    for i in 0..20 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(3..7).contains(&i) && !(12..16).contains(&i),
            "k{i:04} after replay"
        );
    }
    // And the replayed span becomes durable in its turn.
    db.flush_memtable(&cf).unwrap();
    for i in 0..20 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(3..7).contains(&i) && !(12..16).contains(&i),
            "k{i:04} after the replayed span is flushed"
        );
    }
    db.close().unwrap();
}

/// A crash during compaction before the manifest is persisted: the outputs are
/// orphaned, the inputs stay catalogued, and the answer is unchanged.
#[test]
fn range_compact_crash() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.num_compaction_threads = 1;
    let (db, cf) = open_with(opts, ColumnFamilyConfig::default());
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    for i in 0..40 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.delete_range(&cf, &key(10), &key(30)).unwrap();
    db.flush_memtable(&cf).unwrap();
    let before = scan_forward(&db, &cf);

    ondadb::util::fault::fail_nth(ondadb::util::fault::Call::Rename, 1);
    let compacted = db.compact(&cf);
    ondadb::util::fault::clear();
    assert!(compacted.is_err(), "the injected rename must fail");

    // The inputs are intact, so the answer is the same as before the attempt.
    assert_eq!(scan_forward(&db, &cf), before);
    for i in 0..40 {
        assert_eq!(
            db.get(&cf, &key(i)).is_ok(),
            !(10..30).contains(&i),
            "k{i:04}"
        );
    }
    // Dropped rather than `forget`ten: the handle owns the directory's advisory
    // lock, and the point of the test is the catalog state, which the failed
    // compaction already rolled back.
    drop(cf);
    drop(db);

    let (db, cf) = reopen(dir.path());
    assert_eq!(scan_forward(&db, &cf), before, "after reopen");
    db.close().unwrap();
}

// ---- excise failure matrix --------------------------------------------------

const EXCISE_CRASH_DIR_ENV: &str = "ONDA_EXCISE_CRASH_DIR";
const EXCISE_CRASH_WHERE_ENV: &str = "ONDA_EXCISE_CRASH_WHERE";

/// Not a real test: the child half of the excise crash simulations. A no-op
/// unless the environment variables are set, so a normal suite run pays
/// nothing. `WHERE` selects which side of the catalog edit the crash lands on.
#[test]
fn excise_crash_helper() {
    let Ok(dir) = std::env::var(EXCISE_CRASH_DIR_ENV) else {
        return;
    };
    let where_ = std::env::var(EXCISE_CRASH_WHERE_ENV).unwrap_or_default();
    let (db, cf, _) = covered_table_fixture(std::path::Path::new(&dir));
    match where_.as_str() {
        // The catalog edit is durable, the file is not yet unlinked: an
        // uncatalogued table left on disk, which the default-tier orphan sweep
        // collects on the next open.
        "unlink" => {
            ondadb::util::fault::fail_nth(ondadb::util::fault::Call::Unlink, 1);
            let dropped = db.excise_covered(&cf).unwrap();
            ondadb::util::fault::clear();
            assert_eq!(dropped, 1, "the catalog edit committed");
        }
        // The edit record's write fails: nothing is published, the database
        // fail-stops, and a reopen sees the pre-excise catalog.
        //
        // `Write`, not `Sync`: a failed fsync says the bytes are not *durable*,
        // not that they are absent — the record may already sit in the file,
        // and recovery that finds a CRC-valid record is right to replay it. The
        // write is the injection point at which "the catalog edit never
        // happened" is a claim the filesystem actually supports.
        "persist" => {
            ondadb::util::fault::fail_nth(ondadb::util::fault::Call::Write, 1);
            let failed = db.excise_covered(&cf);
            ondadb::util::fault::clear();
            assert!(failed.is_err(), "the injected write must fail the excise");
        }
        other => panic!("unknown crash point {other:?}"),
    }
    // Simulated crash: no close(), no Drop.
    std::process::exit(0);
}

fn run_excise_crash(dir: &std::path::Path, where_: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["excise_crash_helper", "--exact", "--nocapture"])
        .env(EXCISE_CRASH_DIR_ENV, dir.to_str().unwrap())
        .env(EXCISE_CRASH_WHERE_ENV, where_)
        .status()
        .expect("spawn the excise crash helper");
    assert!(status.success(), "excise crash helper child failed");
}

/// Crash after the catalog edit, before the unlink: the table is on disk but
/// out of the catalog. Reads are unaffected — the catalog is the source of
/// truth — and the file is an orphan, never a route to stale data.
#[test]
fn excise_crash_before_unlink() {
    let dir = tempfile::tempdir().unwrap();
    run_excise_crash(dir.path(), "unlink");

    let (db, cf) = reopen(dir.path());
    assert_eq!(
        covered_tables(&cf).len(),
        0,
        "the excised table stays out of the catalog across the crash"
    );
    assert!(total_fragments(&cf) > 0, "its covering fragment survived");
    for i in 0..40u32 {
        assert!(
            db.get(&cf, &key(i)).is_err(),
            "k{i:04} must not resurrect from the orphan"
        );
    }
    db.close().unwrap();
}

/// Crash before the catalog edit is durable: the old catalog is what a reopen
/// finds, the table is still there, and the tombstone still masks it.
#[test]
fn excise_crash_before_persist() {
    let dir = tempfile::tempdir().unwrap();
    run_excise_crash(dir.path(), "persist");

    let (db, cf) = reopen_read_only(dir.path());
    assert_eq!(
        covered_tables(&cf).len(),
        1,
        "the pre-excise catalog is what survived"
    );
    for i in 0..40u32 {
        assert!(db.get(&cf, &key(i)).is_err(), "k{i:04} still masked");
    }
    db.close().unwrap();

    // And a writable reopen completes the excise the crash never committed —
    // by the pre-pass or by the operator call, whichever gets there first.
    let (db, cf) = reopen(dir.path());
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !covered_tables(&cf).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the reopened database never completed the excise"
        );
        let _ = db.excise_covered(&cf).unwrap();
        std::thread::sleep(Duration::from_millis(20));
    }
    for i in 0..40u32 {
        assert!(db.get(&cf, &key(i)).is_err(), "k{i:04} after the retry");
    }
    db.close().unwrap();
}

/// A failed catalog transaction **poisons** the database. The in-memory view
/// was already the post-excise one when the edit's fsync failed, so there is
/// nothing to leave untouched; restoring the handles would be a new capability
/// with its own races. This is `detach_part`'s precedent, unchanged.
#[test]
fn excise_persist_fails_poisons() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf, _) = covered_table_fixture(dir.path());

    ondadb::util::fault::fail_nth(ondadb::util::fault::Call::Write, 1);
    let failed = db.excise_covered(&cf);
    ondadb::util::fault::clear();
    assert!(failed.is_err(), "the injected write must fail the excise");

    // Fail-stopped: every subsequent write is refused, on this handle and for
    // every column family.
    let after = db.put(&cf, b"anything", b"v", Duration::ZERO);
    assert!(after.is_err(), "a poisoned database accepts no writes");
    assert!(
        db.excise_covered(&cf).is_err(),
        "and no further catalog transactions"
    );

    drop(cf);
    drop(db);
    // A reopen sees the pre-excise catalog. Read-only, because a writable
    // reopen is entitled to run the pre-pass again and excise the table for
    // real — which is correct, and would hide what is being asserted here.
    let (db, cf) = reopen_read_only(dir.path());
    assert_eq!(
        covered_tables(&cf).len(),
        1,
        "the failed transaction published nothing durable"
    );
    for i in 0..40u32 {
        assert!(db.get(&cf, &key(i)).is_err(), "k{i:04} still masked");
    }
    db.close().unwrap();
}

// ---- slice 12: PerfContext ---------------------------------------------------

/// `range_sources` / `range_masked` answer "why was *this* read slow" for the
/// 1.2 feature the way `bloom_probes` does for filters, and stay at zero for a
/// family that never issued a range delete.
#[test]
fn perf_context_counts_range_sources_and_masked_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_ranged(dir.path());
    for i in 0..40u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.delete_range(&cf, &key(10), &key(30)).unwrap();
    db.flush_memtable(&cf).unwrap();

    let scope = ondadb::perf::enter();
    assert!(db.get(&cf, &key(20)).is_err(), "inside the span");
    let masked = scope.finish();
    assert!(
        masked.range_sources > 0,
        "the covering fragment's table was consulted"
    );
    assert_eq!(masked.range_masked, 1, "one key hidden by a range delete");

    let scope = ondadb::perf::enter();
    assert!(db.get(&cf, &key(5)).is_ok(), "outside the span");
    let live = scope.finish();
    assert!(live.range_sources > 0, "the sources were still consulted");
    assert_eq!(live.range_masked, 0, "nothing was hidden");

    // A scan counts one source per fragment list it materialized, and one
    // `range_masked` per key it skipped.
    let scope = ondadb::perf::enter();
    let seen = scan_forward(&db, &cf).len();
    let scan = scope.finish();
    assert_eq!(seen, 20, "twenty keys survive the delete");
    assert!(scan.range_sources > 0);
    assert_eq!(scan.range_masked, 20, "twenty keys skipped");
    db.close().unwrap();

    // A family that never enabled range deletes pays nothing and reports zero.
    let plain_dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_plain(plain_dir.path());
    for i in 0..10u32 {
        db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let scope = ondadb::perf::enter();
    assert!(db.get(&cf, &key(5)).is_ok());
    let _ = scan_forward(&db, &cf);
    let none = scope.finish();
    assert_eq!(none.range_sources, 0, "no source to consult");
    assert_eq!(none.range_masked, 0);
    db.close().unwrap();
}
