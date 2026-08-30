//! Durable prepared transactions (3.2): `Txn::prepare`, `DB::commit_prepared`,
//! `DB::abort_prepared`, `DB::list_prepared`, and the control-plane matrix.
//!
//! Recovery, the crash matrix and the WAL generation pins live in
//! `tests/prepared_recovery.rs`; the Hermitage anomaly table lives in
//! `tests/prepared_hermitage.rs`.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, Options, DB};

/// Every prepared-transaction test runs against the unified layout — the only
/// one 2PC supports — with a memtable large enough that no test rotates by
/// accident.
pub fn unified_opts(path: &str) -> Options {
    Options {
        unified_memtable: true,
        ..Options::new(path)
    }
}

fn open_unified(path: &str) -> DB {
    let db = DB::open(unified_opts(path)).unwrap();
    enable_2pc(&db);
    db
}

/// Take the durable permission to write kinds 16-18. One-way, and implied
/// `CAP_EXTENDED_RECORDS` comes with it; without this every `prepare` is
/// refused with `InvalidArgs`, which is what
/// `prepare_requires_cap_txn_decisions` pins.
pub fn enable_2pc(db: &DB) {
    db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
        .unwrap();
}

fn cf(db: &DB, name: &str) -> Arc<ColumnFamily> {
    db.create_column_family(name, ColumnFamilyConfig::default())
        .unwrap()
}

fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

/// The five levels, so every "at every isolation level" claim is a loop rather
/// than a sentence.
pub const LEVELS: [IsolationLevel; 5] = [
    IsolationLevel::ReadUncommitted,
    IsolationLevel::ReadCommitted,
    IsolationLevel::RepeatableRead,
    IsolationLevel::Snapshot,
    IsolationLevel::Serializable,
];

// ---- the reservation check on the ordinary commit path (rule 5) ------------

/// Rule 5: **every** commit checks the reservation registry, at every isolation
/// level — including the three that took no commit lock at all before 3.2.
#[test]
fn ordinary_commit_conflicts_with_reservation_at_every_level() {
    for level in LEVELS {
        let dir = tempfile::tempdir().unwrap();
        let db = open_unified(dir.path().to_str().unwrap());
        let a = cf(&db, "a");

        let mut t = db.begin();
        t.put(&a, b"reserved", b"prepared-value", Duration::ZERO)
            .unwrap();
        t.prepare(&id(1)).unwrap();

        let mut other = db.begin_with_isolation(level);
        other.put(&a, b"reserved", b"other", Duration::ZERO).unwrap();
        let err = other
            .commit()
            .unwrap_err_at(level, "a reserved key must refuse every writer");
        assert_eq!(err, "conflict", "{level:?} must see the reservation");

        // An untouched key is unaffected: the reservation is per key, not
        // per database.
        let mut free = db.begin_with_isolation(level);
        free.put(&a, b"free", b"v", Duration::ZERO).unwrap();
        free.commit().unwrap();

        db.abort_prepared(&id(1)).unwrap();
        drop(db);
    }
}

/// The single-op `DB::put`/`DB::delete` path begins a `ReadCommitted`
/// transaction and took no lock before 3.2. It performs the check too.
#[test]
fn single_op_put_conflicts_with_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");

    let mut t = db.begin();
    t.put(&a, b"k", b"prepared", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    assert_eq!(
        db.put(&a, b"k", b"v", Duration::ZERO).unwrap_err().kind(),
        "conflict"
    );
    assert_eq!(db.delete(&a, b"k").unwrap_err().kind(), "conflict");
    // Another key still commits through the same path.
    db.put(&a, b"other", b"v", Duration::ZERO).unwrap();

    db.abort_prepared(&id(1)).unwrap();
    db.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    assert_eq!(db.get(&a, b"k").unwrap(), b"v");
    db.close().unwrap();
}

// ---- Txn::prepare ---------------------------------------------------------

/// Per-CF WALs cannot atomically establish a record across independent logs, so
/// `prepare` refuses in that layout however many families the writeset touches
/// — mirroring `commit`'s multi-CF refusal variant and wording shape.
#[test]
fn prepare_refuses_per_cf_layout() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    // The capability is on, so the refusal under test is the *layout* one.
    enable_2pc(&db);
    let a = cf(&db, "a");
    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();

    let err = t.prepare(&id(1)).unwrap_err();
    assert_eq!(err.kind(), "invalid_args");
    let message = err.to_string();
    assert!(
        message.contains("unified_memtable=true"),
        "the refusal must name the option that fixes it: {message}"
    );
    db.close().unwrap();
}

/// Kinds 16–18 may only be written once the manifest durably says so. Without
/// `CAP_TXN_DECISIONS` the whole feature is inert on disk — which is what makes
/// the rollback story true — and `prepare` says which call turns it on.
#[test]
fn prepare_requires_cap_txn_decisions() {
    let dir = tempfile::tempdir().unwrap();
    // Deliberately NOT `open_unified`: that enables the capability.
    let db = DB::open(unified_opts(dir.path().to_str().unwrap())).unwrap();
    let a = cf(&db, "a");
    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();

    let err = t.prepare(&id(1)).unwrap_err();
    assert_eq!(err.kind(), "invalid_args");
    assert!(
        err.to_string().contains("CAP_TXN_DECISIONS"),
        "the refusal must name the capability: {err}"
    );
    // Nothing was registered and no frame was written.
    assert!(db.list_prepared().is_empty());

    // Enabling it also takes CAP_EXTENDED_RECORDS: a control record *is* a
    // kind-bearing envelope record, so the two are one permission.
    enable_2pc(&db);
    let caps = db.format_capabilities();
    assert_ne!(caps & ondadb::format::CAP_TXN_DECISIONS, 0);
    assert_ne!(
        caps & ondadb::format::CAP_EXTENDED_RECORDS,
        0,
        "CAP_TXN_DECISIONS must imply CAP_EXTENDED_RECORDS"
    );

    let mut t2 = db.begin();
    t2.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t2.prepare(&id(1)).expect("enabled: the prepare goes through");
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

/// The capability is durable, so a reopen does not have to re-enable it.
#[test]
fn cap_txn_decisions_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        cf(&db, "a");
        db.close().unwrap();
    }
    let db = DB::open(unified_opts(&path)).unwrap();
    assert_ne!(
        db.format_capabilities() & ondadb::format::CAP_TXN_DECISIONS,
        0
    );
    let a = db.get_column_family("a").unwrap();
    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).expect("the capability is durable");
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

#[test]
fn prepare_refuses_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        cf(&db, "a");
        db.close().unwrap();
    }
    let db = DB::open(Options {
        read_only: true,
        ..unified_opts(&path)
    })
    .unwrap();
    let a = db.get_column_family("a").unwrap();
    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    assert_eq!(t.prepare(&id(1)).unwrap_err().kind(), "readonly");
    assert_eq!(db.commit_prepared(&id(1)).unwrap_err().kind(), "readonly");
    assert_eq!(db.abort_prepared(&id(1)).unwrap_err().kind(), "readonly");
    // ...but listing works: an operator opening read-only must still be able to
    // see what is outstanding.
    assert!(db.list_prepared().is_empty());
}

#[test]
fn prepare_refuses_poisoned() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    db.fail_stop_for_tests("simulated durability failure");

    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    assert_eq!(t.prepare(&id(1)).unwrap_err().kind(), "poisoned");
    // Reads and listing keep working on a poisoned database.
    assert!(db.list_prepared().is_empty());
    let _ = db.close();
}

#[test]
fn prepare_duplicate_id_returns_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");

    let mut t = db.begin();
    t.put(&a, b"k1", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    let mut t2 = db.begin();
    t2.put(&a, b"k2", b"v", Duration::ZERO).unwrap();
    assert_eq!(t2.prepare(&id(1)).unwrap_err().kind(), "exists");

    // A different id on the same (disjoint) keys is fine.
    let mut t3 = db.begin();
    t3.put(&a, b"k2", b"v", Duration::ZERO).unwrap();
    t3.prepare(&id(2)).unwrap();

    db.abort_prepared(&id(1)).unwrap();
    db.abort_prepared(&id(2)).unwrap();
    db.close().unwrap();
}

/// A prepare publishes nothing and reserves no sequence: the watermark is
/// unchanged, and the next ordinary commit gets the sequence the prepare would
/// have taken if it had reserved one.
#[test]
fn prepare_does_not_publish_or_reserve() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();

    let before = db.visible_seq_for_tests();
    let mut t = db.begin();
    for i in 0..4u32 {
        t.put(&a, format!("p{i}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    t.prepare(&id(1)).unwrap();
    assert_eq!(
        db.visible_seq_for_tests(),
        before,
        "a prepare must not publish"
    );

    // The next ordinary commit takes the very next sequence — the prepare's
    // four records reserved none of them.
    db.put(&a, b"after", b"v", Duration::ZERO).unwrap();
    assert_eq!(
        db.visible_seq_for_tests(),
        before + 1,
        "a prepare must not reserve a sequence"
    );
    // And nothing prepared is readable.
    assert!(db.get(&a, b"p0").is_err());

    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

/// `prepare` MOVES the transaction's arena into the registry rather than
/// copying it, and the buffer never re-enters the thread-local pool — pinning
/// an up-to-32-MiB buffer there for an unbounded prepare lifetime is exactly
/// what `BUF_POOL`'s caps exist to prevent. Two 1-MiB prepares must therefore
/// remain independently readable.
#[test]
fn prepare_buffer_does_not_return_to_pool() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    let big_a = vec![0xAAu8; 1 << 20];
    let big_b = vec![0xBBu8; 1 << 20];

    let mut t1 = db.begin();
    t1.put(&a, b"one", &big_a, Duration::ZERO).unwrap();
    t1.prepare(&id(1)).unwrap();
    let mut t2 = db.begin();
    t2.put(&a, b"two", &big_b, Duration::ZERO).unwrap();
    t2.prepare(&id(2)).unwrap();

    // If the first arena had been recycled, the second prepare would have
    // written over it.
    db.commit_prepared(&id(1)).unwrap();
    db.commit_prepared(&id(2)).unwrap();
    assert_eq!(db.get(&a, b"one").unwrap(), big_a);
    assert_eq!(db.get(&a, b"two").unwrap(), big_b);
    db.close().unwrap();
}

#[test]
fn prepare_byte_cap_rejects_with_too_large() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options {
        max_prepared_bytes: 4096,
        ..unified_opts(dir.path().to_str().unwrap())
    })
    .unwrap();
    enable_2pc(&db);
    let a = cf(&db, "a");

    let mut t = db.begin();
    t.put(&a, b"big", &vec![7u8; 8192], Duration::ZERO).unwrap();
    assert_eq!(t.prepare(&id(1)).unwrap_err().kind(), "too_large");
    // Nothing was registered, so nothing is reserved.
    assert!(db.list_prepared().is_empty());
    db.put(&a, b"big", b"v", Duration::ZERO).unwrap();
    db.close().unwrap();
}

/// The prepare record carries point writes only (kinds 1/2/3), so a
/// transaction holding a range delete is refused rather than silently losing
/// the span.
#[test]
fn prepare_refuses_a_range_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let a = cf(&db, "a");
    let mut t = db.begin();
    t.delete_range(&a, b"m", b"z").unwrap();
    let err = t.prepare(&id(1)).unwrap_err();
    assert_eq!(err.kind(), "invalid_args");
    assert!(err.to_string().contains("range delete"));
    db.close().unwrap();
}

/// A range delete covering a reserved key is another writer changing it, so it
/// is refused exactly as a point write to that key would be. A hash probe
/// cannot answer a span, so this is the arm the point path never reaches.
#[test]
fn range_delete_covering_a_reservation_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let a = cf(&db, "a");
    let b = cf(&db, "b");

    let mut t = db.begin();
    t.put(&a, b"mmm", b"prepared", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    assert_eq!(
        db.delete_range(&a, b"a", b"z").unwrap_err().kind(),
        "conflict",
        "a span covering a reserved key must be refused"
    );
    // A span that misses it commits, and so does the same span in another
    // column family: the reservation is per (cf, key).
    db.delete_range(&a, b"n", b"z").unwrap();
    db.delete_range(&b, b"a", b"z").unwrap();

    db.abort_prepared(&id(1)).unwrap();
    db.delete_range(&a, b"a", b"z")
        .expect("the reservation is gone");
    db.close().unwrap();
}

/// A prepare validates the span index at the levels that validate anything, so
/// a point write already covered by a newer range delete loses its conflict at
/// `prepare` rather than being promised a commit that would have to refuse it.
#[test]
fn prepare_conflicts_with_a_newer_covering_range_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let a = cf(&db, "a");
    db.put(&a, b"mmm", b"v0", Duration::ZERO).unwrap();

    // Snapshot taken BEFORE the range delete commits.
    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    assert_eq!(t.get(&a, b"mmm").unwrap(), b"v0");
    db.delete_range(&a, b"a", b"z").unwrap();
    t.put(&a, b"mmm", b"v1", Duration::ZERO).unwrap();

    assert_eq!(
        t.prepare(&id(1)).unwrap_err().kind(),
        "conflict",
        "the prepare must lose the conflict its commit would have lost"
    );
    assert!(db.list_prepared().is_empty());
    db.close().unwrap();
}

// ---- commit_prepared ------------------------------------------------------

#[test]
fn commit_prepared_applies_and_publishes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    let b = cf(&db, "b");

    let mut t = db.begin();
    t.put(&a, b"k", b"va", Duration::ZERO).unwrap();
    t.put(&b, b"k", b"vb", Duration::ZERO).unwrap();
    t.delete(&a, b"gone").unwrap();
    let prepared = t.prepare(&id(1)).unwrap();
    assert_eq!(prepared.id, id(1));
    assert!(db.get(&a, b"k").is_err(), "nothing prepared is visible");

    let before = db.visible_seq_for_tests();
    db.commit_prepared(&id(1)).unwrap();
    assert_eq!(
        db.visible_seq_for_tests(),
        before + 3,
        "the whole writeset publishes as one block"
    );
    assert_eq!(db.get(&a, b"k").unwrap(), b"va");
    assert_eq!(db.get(&b, b"k").unwrap(), b"vb");
    assert!(db.list_prepared().is_empty());
    db.close().unwrap();
}

/// A coordinator retry after an acknowledged commit must be a no-op, not a
/// second apply: the value's sequence is what proves it.
#[test]
fn commit_prepared_is_idempotent_on_retry() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");

    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();
    db.commit_prepared(&id(1)).unwrap();
    let after_first = db.visible_seq_for_tests();

    db.commit_prepared(&id(1)).expect("a retry returns Ok");
    assert_eq!(
        db.visible_seq_for_tests(),
        after_first,
        "a retry must reserve and publish nothing"
    );
    assert_eq!(db.get(&a, b"k").unwrap(), b"v");
    db.close().unwrap();
}

#[test]
fn commit_prepared_unknown_id_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    cf(&db, "a");
    assert_eq!(db.commit_prepared(&id(9)).unwrap_err().kind(), "not_found");
    assert_eq!(db.abort_prepared(&id(9)).unwrap_err().kind(), "not_found");
    db.close().unwrap();
}

/// Every exit path after `reserve_seq` publishes its reserved range, or the
/// gap-free cursor freezes forever (AGENTS.md invariant 5). Inject a WAL
/// failure and assert `visible_seq` still advances past the block and a later
/// ordinary commit becomes visible.
#[test]
fn prep_commit_failure_publishes_reserved_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    // A committed record, so the rotation below has something to seal: a
    // rotation of an empty memtable returns without opening a fresh WAL.
    db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.put(&a, b"k1", b"v", Duration::ZERO).unwrap();
    t.put(&a, b"k2", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    let before = db.visible_seq_for_tests();
    // Close the unified WAL under the commit: the decision append then fails,
    // which is the "decision write or fsync fails (no crash)" crash-matrix row.
    db.close_unified_wal_for_tests();
    let err = db.commit_prepared(&id(1)).unwrap_err();
    assert_eq!(err.kind(), "invalid_db");

    assert_eq!(
        db.visible_seq_for_tests(),
        before + 2,
        "the reserved block is published even though the commit failed"
    );
    // The watermark keeps advancing: a later ordinary commit is visible, which
    // it would not be if the cursor had frozen.
    assert!(db.get(&a, b"k1").is_err(), "the failed apply is invisible");
    db.rotate_unified_for_tests();
    db.put(&a, b"later", b"v", Duration::ZERO).unwrap();
    assert_eq!(db.get(&a, b"later").unwrap(), b"v");
    assert_eq!(db.visible_seq_for_tests(), before + 3);
    let _ = db.close();
}

#[test]
fn commit_prepared_runs_commit_hooks() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    /// `(key, tombstone)` pairs the hook observed.
    type HookLog = Arc<parking_lot::Mutex<Vec<(Vec<u8>, bool)>>>;
    let seen: HookLog = Arc::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = seen.clone();
    let counter = calls.clone();
    a.set_commit_hook(Some(Arc::new(move |_seq, ops: &[ondadb::CommitOp]| {
        counter.fetch_add(1, Ordering::SeqCst);
        let mut g = sink.lock();
        for op in ops {
            g.push((op.key.clone(), op.tombstone));
        }
    })));

    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.delete(&a, b"d").unwrap();
    t.prepare(&id(1)).unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a prepare delivers no hook: nothing has committed"
    );

    db.commit_prepared(&id(1)).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let got = seen.lock().clone();
    assert!(got.contains(&(b"k".to_vec(), false)));
    assert!(got.contains(&(b"d".to_vec(), true)));
    db.close().unwrap();
}

// ---- abort_prepared / list_prepared ---------------------------------------

#[test]
fn abort_prepared_releases_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");

    let mut t = db.begin();
    t.put(&a, b"k", b"prepared", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();
    assert_eq!(
        db.put(&a, b"k", b"v", Duration::ZERO).unwrap_err().kind(),
        "conflict"
    );

    db.abort_prepared(&id(1)).unwrap();
    db.put(&a, b"k", b"v", Duration::ZERO)
        .expect("the reservation is gone");
    assert_eq!(db.get(&a, b"k").unwrap(), b"v");
    assert!(db.list_prepared().is_empty());
    db.close().unwrap();
}

/// An abort reserves no sequence, so it opens no gap: the watermark before and
/// after are the same, and the next commit takes the very next sequence.
#[test]
fn abort_prepared_reserves_no_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();

    let mut t = db.begin();
    for i in 0..5u32 {
        t.put(&a, format!("k{i}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    t.prepare(&id(1)).unwrap();
    let before = db.visible_seq_for_tests();
    db.abort_prepared(&id(1)).unwrap();
    assert_eq!(db.visible_seq_for_tests(), before);

    db.put(&a, b"after", b"v", Duration::ZERO).unwrap();
    assert_eq!(db.visible_seq_for_tests(), before + 1);
    db.close().unwrap();
}

/// An abort must write a durable decision, and a fail-stopped database cannot.
/// So it refuses and the prepare stays on disk, resolvable after a reopen —
/// rather than being dropped from a registry a coordinator is still tracking.
#[test]
fn abort_prepared_on_poisoned_db_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    db.fail_stop_for_tests("simulated durability failure");
    assert_eq!(db.abort_prepared(&id(1)).unwrap_err().kind(), "poisoned");
    assert_eq!(db.commit_prepared(&id(1)).unwrap_err().kind(), "poisoned");
    // The durable prepared state is never discarded by poisoning.
    assert_eq!(db.list_prepared().len(), 1);
    let _ = db.close();
}

#[test]
fn list_prepared_reports_id_age_and_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    let b = cf(&db, "b");

    let mut t = db.begin();
    t.put(&a, b"k", &vec![1u8; 1000], Duration::ZERO).unwrap();
    t.put(&b, b"k", &vec![1u8; 1000], Duration::ZERO).unwrap();
    t.prepare(&id(3)).unwrap();
    let mut t2 = db.begin();
    t2.put(&a, b"other", b"v", Duration::ZERO).unwrap();
    t2.prepare(&id(1)).unwrap();

    let list = db.list_prepared();
    assert_eq!(list.len(), 2);
    assert_eq!(
        list.iter().map(|i| i.id).collect::<Vec<_>>(),
        vec![id(1), id(3)],
        "the listing is sorted by id, so two calls agree"
    );
    let big = &list[1];
    assert!(
        big.bytes >= 2000,
        "bytes must account for the arena: {}",
        big.bytes
    );
    let mut cfs = big.cf_ids.clone();
    cfs.sort_unstable();
    let mut want = vec![a.id(), b.id()];
    want.sort_unstable();
    assert_eq!(cfs, want);
    assert!(big.age < Duration::from_secs(60));

    db.abort_prepared(&id(1)).unwrap();
    db.abort_prepared(&id(3)).unwrap();
    db.close().unwrap();
}

#[test]
fn list_prepared_works_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        let mut t = db.begin();
        t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        // `close` refuses; drop leaves the prepared state on disk.
        assert_eq!(db.close().unwrap_err().kind(), "busy");
        drop(db);
    }
    let db = DB::open(Options {
        read_only: true,
        ..unified_opts(&path)
    })
    .unwrap();
    let list = db.list_prepared();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, id(1));
}

// ---- control plane --------------------------------------------------------

#[test]
fn close_with_unresolved_prepare_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    let err = db.close().unwrap_err();
    assert_eq!(err.kind(), "busy");
    assert!(
        err.to_string().contains('1'),
        "the refusal names the count: {err}"
    );
    // The refusal happens BEFORE anything closes, so the handle still works.
    db.put(&a, b"other", b"v", Duration::ZERO).unwrap();
    assert_eq!(db.get(&a, b"other").unwrap(), b"v");
    db.abort_prepared(&id(1)).unwrap();
    db.close().expect("the refusal clears once resolved");
}

/// `Drop` cannot return an error, so it closes anyway: workers stop, the
/// manifest persists, and — the part `tests/lock_release_on_drop.rs` depends on
/// — the directory lock is released. The pinned WAL files are left on disk.
#[test]
fn drop_with_pins_releases_dir_lock_and_keeps_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        let mut t = db.begin();
        t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        drop(db);
    }
    assert!(
        unified_wal_files(&path) > 0,
        "the pinned WAL generation must survive the drop"
    );
    // The lock was released: reopening in the same process succeeds.
    let db = open_unified(&path);
    assert_eq!(db.list_prepared().len(), 1);
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

#[test]
fn drop_cf_untouched_by_unrelated_prepare_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    let _b = cf(&db, "b");

    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    // In unified mode the WAL is database-wide, but a prepare on `a` must not
    // block dropping `b`.
    db.drop_column_family("b").expect("unrelated family");
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

#[test]
fn drop_cf_named_by_prepare_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    let err = db.drop_column_family("a").unwrap_err();
    assert_eq!(err.kind(), "busy");
    db.abort_prepared(&id(1)).unwrap();
    db.drop_column_family("a").expect("resolved: the drop is free");
    db.close().unwrap();
}

/// `snapshot_to` links SSTables and the manifest and never copies WAL files, so
/// a checkpoint contains no prepared state at all. Refusing the checkpoint
/// would deny an operator a backup at exactly the moment an abandoned prepare
/// makes one most useful.
#[test]
fn prep_checkpoint_has_no_prepared_state() {
    let dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let a = cf(&db, "a");
    db.put(&a, b"committed", b"v", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.put(&a, b"prepared", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();

    // Seal and flush the shared memtable first. `snapshot_to` flushes the
    // *per-CF* memtables only, so in unified mode a checkpoint taken without
    // this carries no recently-committed data — a pre-existing gap this test
    // works around rather than asserts, since the claim under test is about
    // prepared state.
    db.rotate_unified_for_tests();
    wait_for_flush(&db);

    let copy = out.path().join("checkpoint");
    db.checkpoint(&copy).expect("a checkpoint must still succeed");

    let restored = DB::open(unified_opts(copy.to_str().unwrap())).unwrap();
    assert!(
        restored.list_prepared().is_empty(),
        "a checkpoint links SSTables and the manifest, never WAL files, so it \
         carries no prepared state"
    );
    let ra = restored.get_column_family("a").unwrap();
    assert_eq!(restored.get(&ra, b"committed").unwrap(), b"v");
    assert!(
        restored.get(&ra, b"prepared").is_err(),
        "nothing prepared was ever applied"
    );
    restored.close().unwrap();

    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

// ---- helpers --------------------------------------------------------------

/// Block until every queued flush has completed.
pub fn wait_for_flush(db: &DB) {
    for _ in 0..2000 {
        if db.pending_flushes_for_tests() == 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("the flush queue did not drain");
}

/// Count the unified WAL stripe files under `path`.
pub fn unified_wal_files(path: &str) -> usize {
    std::fs::read_dir(path)
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    e.file_name()
                        .to_string_lossy()
                        .starts_with("unified-wal-")
                })
                .count()
        })
        .unwrap_or(0)
}

/// `Result::unwrap_err` that names the isolation level in its panic message,
/// so a parameterised failure says which case failed.
trait UnwrapErrAt<T> {
    fn unwrap_err_at(self, level: IsolationLevel, why: &str) -> &'static str;
}

impl<T: std::fmt::Debug> UnwrapErrAt<T> for ondadb::Result<T> {
    fn unwrap_err_at(self, level: IsolationLevel, why: &str) -> &'static str {
        match self {
            Ok(v) => panic!("{why} ({level:?}): got Ok({v:?})"),
            Err(e) => e.kind(),
        }
    }
}
