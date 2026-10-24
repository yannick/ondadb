//! Cross-feature composition: merge operators (1.1) against range tombstones
//! (1.2) and prepared transactions (3.2).
//!
//! Each feature's own test file exercises it alone. These are the rules that
//! only exist where two of them meet, and that no single feature's author was
//! in a position to write:
//!
//!   * a range delete is, for one key, a **deleted base at its sequence** —
//!     operands above the span fold onto nothing, operands at or below it and
//!     the base under them are masked;
//!   * a merge operand is a one-key write, so a **prepare frame carries it**
//!     exactly as it carries a put; a range delete, with two keys and no value,
//!     still cannot be prepared.
//!
//! The three read paths (point, batch and both scan directions) must agree on
//! every one of them, before and after a flush and a compaction, because each
//! resolves a chain with different code.

use std::sync::Arc;
use std::time::Duration;

use ondadb::format::CAP_RANGE_DELETES;
use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, Options, DB};

/// Appends operands to the base, `|`-separated; a missing base renders as the
/// empty string, so "masked base" and "no base" are the same observable and a
/// dropped operand shows up as a missing field rather than a silent zero.
#[derive(Debug)]
struct Concat;

impl MergeOperator for Concat {
    fn name(&self) -> &str {
        "test.concat.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let mut out = String::from_utf8(existing.unwrap_or(b"").to_vec())
            .map_err(|e| format!("base is not utf-8: {e}"))?;
        for operand in operands {
            out.push('|');
            out.push_str(&String::from_utf8(operand.to_vec()).map_err(|e| e.to_string())?);
        }
        Ok(out.into_bytes())
    }
}

fn open_merge_ranged(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "m",
            ColumnFamilyConfig {
                merge_operator_name: Some(Concat.name().to_string()),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    (db, cf)
}

/// What every read path says about `key`, as `Option<Vec<u8>>`. They must
/// agree; the assertion is here rather than at each call site so a divergence
/// names itself.
fn read_all(db: &DB, cf: &Arc<ColumnFamily>, key: &[u8]) -> Option<Vec<u8>> {
    let point = db.get(cf, key).ok();
    let batch = db.multi_get(cf, &[key]).pop().unwrap().ok();
    assert_eq!(point, batch, "point and batch reads disagree on {key:?}");

    for backward in [false, true] {
        let t = db.begin();
        let mut it = t.new_iterator(cf);
        let mut found = None;
        if backward {
            it.seek_to_last();
            while it.valid() {
                if it.key() == key {
                    found = Some(it.value().to_vec());
                }
                it.prev();
            }
        } else {
            it.seek_to_first();
            while it.valid() {
                if it.key() == key {
                    found = Some(it.value().to_vec());
                }
                it.next();
            }
        }
        assert!(it.err().is_none(), "{:?}", it.err());
        assert_eq!(
            found, point,
            "scan (backward={backward}) disagrees with the point read on {key:?}"
        );
    }
    point
}

/// Operands written *after* a span are above it: the span masks the base they
/// would otherwise have folded onto, and nothing else.
#[test]
fn operands_above_a_span_fold_onto_no_base() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"old").unwrap();
    db.delete_range(&cf, b"k", b"l").unwrap();
    db.merge(&cf, b"k", b"new1").unwrap();
    db.merge(&cf, b"k", b"new2").unwrap();

    // `base` and `old` are both at or below the span; only the two operands
    // above it survive, folded onto an absent base.
    assert_eq!(read_all(&db, &cf, b"k").as_deref(), Some(&b"|new1|new2"[..]));
    db.flush_memtable(&cf).unwrap();
    assert_eq!(
        read_all(&db, &cf, b"k").as_deref(),
        Some(&b"|new1|new2"[..]),
        "the answer must not change when the chain and the fragment move to an SSTable"
    );
    db.compact(&cf).unwrap();
    assert_eq!(
        read_all(&db, &cf, b"k").as_deref(),
        Some(&b"|new1|new2"[..]),
        "nor when compaction folds and re-fragments them"
    );
    db.close().unwrap();
}

/// A span above the whole chain hides it, exactly as a point tombstone would.
#[test]
fn a_span_above_the_chain_hides_it() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();
    db.delete_range(&cf, b"k", b"l").unwrap();

    assert_eq!(read_all(&db, &cf, b"k"), None);
    db.flush_memtable(&cf).unwrap();
    assert_eq!(read_all(&db, &cf, b"k"), None);
    db.compact(&cf).unwrap();
    assert_eq!(
        read_all(&db, &cf, b"k"),
        None,
        "compaction must not resurrect a masked chain"
    );
    db.close().unwrap();
}

/// A chain with no base at all under a span that predates it: the span changes
/// nothing, because there was nothing for it to mask.
#[test]
fn a_span_below_a_baseless_chain_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.delete_range(&cf, b"k", b"l").unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();

    assert_eq!(read_all(&db, &cf, b"k").as_deref(), Some(&b"|a|b"[..]));
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(read_all(&db, &cf, b"k").as_deref(), Some(&b"|a|b"[..]));
    db.close().unwrap();
}

/// The span covers a *range*, so a key beside the chain is untouched and the
/// scan still surfaces exactly the surviving keys in order.
#[test]
fn a_span_masks_only_the_keys_it_covers() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    for key in [&b"a"[..], b"m", b"z"] {
        db.put(&cf, key, b"base", Duration::ZERO).unwrap();
        db.merge(&cf, key, b"1").unwrap();
    }
    db.delete_range(&cf, b"b", b"n").unwrap();
    db.merge(&cf, b"m", b"2").unwrap();

    assert_eq!(read_all(&db, &cf, b"a").as_deref(), Some(&b"base|1"[..]));
    assert_eq!(read_all(&db, &cf, b"m").as_deref(), Some(&b"|2"[..]));
    assert_eq!(read_all(&db, &cf, b"z").as_deref(), Some(&b"base|1"[..]));

    let t = db.begin();
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    let mut keys = Vec::new();
    while it.valid() {
        keys.push(it.key().to_vec());
        it.next();
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    assert_eq!(keys, vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]);
    db.close().unwrap();
}

/// Fully masked keys disappear from the scan rather than surfacing as an empty
/// fold — the case that a `merged` group without a sequence of its own would
/// get wrong in the opposite direction.
#[test]
fn a_fully_masked_chain_is_absent_from_the_scan() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"a", b"keep", Duration::ZERO).unwrap();
    db.put(&cf, b"m", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"m", b"1").unwrap();
    db.delete_range(&cf, b"b", b"n").unwrap();

    for stage in ["memtable", "flushed", "compacted"] {
        let t = db.begin();
        let mut it = t.new_iterator(&cf);
        it.seek_to_first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(it.key().to_vec());
            it.next();
        }
        assert!(it.err().is_none(), "{:?}", it.err());
        assert_eq!(keys, vec![b"a".to_vec()], "at stage {stage}");
        drop(it);
        drop(t);
        match stage {
            "memtable" => db.flush_memtable(&cf).unwrap(),
            "flushed" => db.compact(&cf).unwrap(),
            _ => {}
        }
    }
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// 1.1 composed with 3.2: a prepared transaction holding a merge operand
// ---------------------------------------------------------------------------

/// A merge operand is a one-key, one-value write, so a prepare frame carries it
/// exactly as it carries a put. Nothing else in the tree covers this pair, and
/// getting it wrong is not a wrong answer but an **unreadable WAL**: the writer
/// would emit kind 4 inside a control frame that replay refuses as corruption.
#[test]
fn a_prepared_merge_operand_survives_a_crash_and_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let id = [7u8; 16];
    {
        let mut opts = Options::new(&path);
        opts.unified_memtable = true;
        opts.merge_fns = vec![Arc::new(Concat)];
        let db = DB::open(opts).unwrap();
        db.enable_format_capabilities(
            ondadb::format::CAP_TXN_DECISIONS | CAP_RANGE_DELETES,
        )
        .unwrap();
        let cf = db
            .create_column_family(
                "m",
                ColumnFamilyConfig {
                    merge_operator_name: Some(Concat.name().to_string()),
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
        let mut t = db.begin();
        t.merge(&cf, b"k", b"prepared").unwrap();
        t.prepare(&id).unwrap();
        // Uncommitted: the operand must not be visible yet.
        assert_eq!(db.get(&cf, b"k").unwrap(), b"base");
        drop(db); // crash
    }

    let mut opts = Options::new(&path);
    opts.unified_memtable = true;
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("m").unwrap();
    assert_eq!(
        db.list_prepared().len(),
        1,
        "the reservation must survive the crash"
    );
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base");
    db.commit_prepared(&id).unwrap();
    assert_eq!(
        db.get(&cf, b"k").unwrap(),
        b"base|prepared",
        "the recovered operand must fold, not land as a plain put"
    );
    db.close().unwrap();
}

/// A range delete still cannot be prepared: two keys and no value have no shape
/// in the frame, and the refusal is at the API rather than at replay.
#[test]
fn a_prepared_range_delete_is_still_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.unified_memtable = true;
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS | CAP_RANGE_DELETES)
        .unwrap();
    let cf = db
        .create_column_family("d", ColumnFamilyConfig::default())
        .unwrap();
    let mut t = db.begin();
    t.delete_range(&cf, b"a", b"z").unwrap();
    let err = t.prepare(&[9u8; 16]).expect_err("a range cannot be prepared");
    assert_eq!(err.kind(), "invalid_args", "{err}");
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// 3.3 composed with 3.2: in-memory locks become durable reservations
// ---------------------------------------------------------------------------

/// A pessimistic database with 2PC enabled, and one column family.
fn open_pessimistic_2pc(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.unified_memtable = true;
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
        .unwrap();
    let cf = db
        .create_column_family("d", ColumnFamilyConfig::default())
        .unwrap();
    (db, cf)
}

/// At `prepare`, a point lock (3.3) becomes a reservation (3.2) on the same
/// `(cf.id(), key)` — and every waiter is woken with `Conflict` rather than
/// granted a lock whose commit is guaranteed to fail against the reservation
/// it cannot see. The reservation is the authority from that instant.
#[test]
fn waiter_on_prepared_owner_sees_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_pessimistic_2pc(dir.path());
    let db = Arc::new(db);
    db.put(&cf, b"k", b"v0", Duration::ZERO).unwrap();

    // The waiter's transaction is begun first, so wait-die lets it wait; the
    // holder takes the lock before the waiter asks. Both pinned by barriers.
    let begun = Arc::new(std::sync::Barrier::new(2));
    let held = Arc::new(std::sync::Barrier::new(2));
    let waiter = {
        let (db, cf) = (db.clone(), cf.clone());
        let (begun, held) = (begun.clone(), held.clone());
        std::thread::spawn(move || {
            let mut t = db.begin_pessimistic();
            begun.wait();
            held.wait();
            let err = t.get_for_update(&cf, b"k").unwrap_err();
            assert_eq!(err.kind(), "conflict", "{err}");
            t.rollback().unwrap();
        })
    };
    begun.wait();
    let mut holder = db.begin_pessimistic();
    holder.put(&cf, b"k", b"prepared", Duration::ZERO).unwrap();
    held.wait();
    while db.txn_lock_waiters_for_tests(&cf, b"k") == 0 {
        std::thread::yield_now();
    }
    holder.prepare(&[11u8; 16]).unwrap();
    waiter.join().unwrap();
    assert!(
        db.txn_locks_idle_for_tests(),
        "the prepare handed its keys to the registry"
    );
    db.abort_prepared(&[11u8; 16]).unwrap();
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

/// After recovery only the **reservation** exists: in-memory locks are
/// volatile. A new pessimistic transaction takes the lock (nothing holds it)
/// and its commit is refused against the recovered reservation until a
/// coordinator resolves the prepare — so a waiter never hangs on a crashed
/// owner, it is simply told no.
#[test]
fn recovered_reservation_reblocks_lock_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let id = [12u8; 16];
    {
        let (db, cf) = open_pessimistic_2pc(dir.path());
        let mut t = db.begin_pessimistic();
        t.put(&cf, b"k", b"prepared", Duration::ZERO).unwrap();
        t.prepare(&id).unwrap();
        drop(db); // crash
    }
    let mut opts = Options::new(&path);
    opts.unified_memtable = true;
    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("d").unwrap();
    assert_eq!(db.list_prepared().len(), 1);

    let mut t = db.begin_pessimistic();
    // The lock is free — nothing survived the crash to hold it.
    t.put(&cf, b"k", b"other", Duration::ZERO).unwrap();
    let err = t.commit().unwrap_err();
    assert_eq!(
        err.kind(),
        "conflict",
        "the recovered reservation must refuse the commit: {err}"
    );
    db.abort_prepared(&id).unwrap();
    db.close().unwrap();
}

/// The coordinator commits: the reservation clears, and the next pessimistic
/// transaction commits without `Conflict` — after its refresh, which is what
/// carries its snapshot over the sequences `commit_prepared` just assigned.
#[test]
fn commit_prepared_unblocks_pessimistic_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_pessimistic_2pc(dir.path());
    db.put(&cf, b"k", b"v0", Duration::ZERO).unwrap();
    let mut t = db.begin_pessimistic();
    t.put(&cf, b"k", b"prepared", Duration::ZERO).unwrap();
    t.prepare(&[13u8; 16]).unwrap();
    db.commit_prepared(&[13u8; 16]).unwrap();

    let mut next = db.begin_pessimistic();
    assert_eq!(next.get_for_update(&cf, b"k").unwrap(), b"prepared");
    next.put(&cf, b"k", b"after", Duration::ZERO).unwrap();
    next.commit().expect("the reservation is gone");
    assert_eq!(db.get(&cf, b"k").unwrap(), b"after");
    db.close().unwrap();
}

/// The coordinator aborts: same unblocking, and the aborted writeset is not
/// visible — the next transaction reads what was there before the prepare.
#[test]
fn abort_prepared_unblocks_pessimistic_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_pessimistic_2pc(dir.path());
    db.put(&cf, b"k", b"v0", Duration::ZERO).unwrap();
    let mut t = db.begin_pessimistic();
    t.put(&cf, b"k", b"prepared", Duration::ZERO).unwrap();
    t.prepare(&[14u8; 16]).unwrap();
    db.abort_prepared(&[14u8; 16]).unwrap();

    let mut next = db.begin_pessimistic();
    assert_eq!(
        next.get_for_update(&cf, b"k").unwrap(),
        b"v0",
        "an aborted writeset is not visible"
    );
    next.put(&cf, b"k", b"after", Duration::ZERO).unwrap();
    next.commit().expect("the reservation is gone");
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// 3.3 composed with 1.1 and 1.2: what a pessimistic transaction locks
// ---------------------------------------------------------------------------

/// A **merge** operand takes the key's lock, because in this engine a merge is
/// a write for conflict purposes: `validate_write_conflicts` walks the write
/// order without looking at kinds, so an unlocked merge on a hot key would
/// abort at commit exactly as an unlocked put would. Exempting it would hand
/// the caller a pessimistic transaction that still loses the race it opted in
/// to avoid.
#[test]
fn a_pessimistic_merge_takes_the_keys_lock() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();

    let mut older = db.begin_pessimistic();
    let mut younger = db.begin_pessimistic();
    older.merge(&cf, b"k", b"op").unwrap();
    let err = younger
        .get_for_update(&cf, b"k")
        .expect_err("a merged key is a locked key");
    assert_eq!(err.kind(), "conflict", "{err}");
    // ...and so is the reverse: a merge onto a key someone else holds.
    let err = younger.merge(&cf, b"k", b"other").unwrap_err();
    assert_eq!(err.kind(), "conflict", "{err}");
    younger.rollback().unwrap();
    older.commit().unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|op");
    db.close().unwrap();
}

/// A **range delete** takes no lock, even in a pessimistic transaction. What it
/// would need is a lock on the *interval*, and span locks are v2; a point lock
/// on the start bound would protect one key while reading as if it protected
/// the span. So for its span a pessimistic transaction is an ordinary
/// optimistic writer, and the span index decides its conflicts at commit
/// exactly as it always has.
#[test]
fn a_pessimistic_range_delete_takes_no_point_lock() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"b", b"v", Duration::ZERO).unwrap();

    let mut older = db.begin_pessimistic();
    let mut younger = db.begin_pessimistic();
    older.delete_range(&cf, b"a", b"z").unwrap();
    assert!(
        db.txn_locks_idle_for_tests(),
        "a range delete must not take a point lock"
    );
    // Neither the start bound nor a covered key is locked.
    younger.get_for_update(&cf, b"a").unwrap_err();
    assert_eq!(younger.get_for_update(&cf, b"b").unwrap(), b"v");
    younger.rollback().unwrap();
    older.commit().unwrap();
    db.close().unwrap();
}

/// Prepared transactions × merge operators: a WAL generation a prepare pins is
/// replayed whole on every open, records an earlier flush already made durable
/// included. A put replayed over its own flushed copy is the same version
/// twice, but a merge operand must still count once — or every open of a
/// database with an undecided prepare appends the flushed operands again.
#[test]
fn pinned_generation_replay_counts_a_flushed_operand_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let open = || {
        let mut opts = Options::new(&path);
        opts.unified_memtable = true;
        opts.merge_fns = vec![Arc::new(Concat)];
        DB::open(opts).unwrap()
    };
    {
        let db = open();
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
            .unwrap();
        let m = db
            .create_column_family(
                "m",
                ColumnFamilyConfig {
                    merge_operator_name: Some(Concat.name().to_string()),
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        db.put(&m, b"k", b"base", Duration::ZERO).unwrap();
        db.merge(&m, b"k", b"x").unwrap();
        // Undecided: it pins this generation past the flush below.
        let mut t = db.begin();
        t.put(&m, b"held", b"v", Duration::ZERO).unwrap();
        t.prepare(&[7; 16]).unwrap();
        db.rotate_unified_for_tests();
        while db.pending_flushes_for_tests() > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        db.merge(&m, b"k", b"y").unwrap();
        db.sync_wal().unwrap();
        drop(db);
    }
    for round in 0..2 {
        let db = open();
        let m = db.get_column_family("m").unwrap();
        assert_eq!(db.get(&m, b"k").unwrap(), b"base|x|y", "open {round}");
        let txn = db.begin();
        let mut it = txn.new_iterator(&m);
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!((it.key(), it.value()), (&b"k"[..], &b"base|x|y"[..]), "open {round}");
        drop(it);
        drop(txn);
        drop(db);
    }
    // The replayed memtable flushed again: two tables hold the operand at one
    // sequence, and compaction must fold it in once.
    let db = open();
    let m = db.get_column_family("m").unwrap();
    db.rotate_unified_for_tests();
    while db.pending_flushes_for_tests() > 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(db.get(&m, b"k").unwrap(), b"base|x|y", "two tables");
    db.compact(&m).unwrap();
    assert_eq!(db.get(&m, b"k").unwrap(), b"base|x|y", "compacted");
    db.abort_prepared(&[7; 16]).unwrap();
    db.close().unwrap();
    let db = open();
    let m = db.get_column_family("m").unwrap();
    assert_eq!(db.get(&m, b"k").unwrap(), b"base|x|y", "after the abort");
    db.close().unwrap();
}

/// The same replay, with the chain compacted before the reopen: compaction
/// folds `base|x` into one put at `x`'s sequence, so the replayed operand `x`
/// meets a put copy of itself. One sequence is one version whatever the copy's
/// kind — the put must not become the operand's base, or a forward scan reads
/// `base|x|x` while a point read and a reverse scan read `base|x`.
#[test]
fn pinned_generation_replay_never_bases_an_operand_on_its_folded_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let open = || {
        let mut opts = Options::new(&path);
        opts.unified_memtable = true;
        opts.merge_fns = vec![Arc::new(Concat)];
        DB::open(opts).unwrap()
    };
    {
        let db = open();
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
            .unwrap();
        let m = db
            .create_column_family(
                "m",
                ColumnFamilyConfig {
                    merge_operator_name: Some(Concat.name().to_string()),
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        db.put(&m, b"j", b"base", Duration::ZERO).unwrap();
        db.merge(&m, b"j", b"x").unwrap();
        db.merge(&m, b"k", b"a").unwrap();
        db.merge(&m, b"k", b"b").unwrap();
        let mut t = db.begin();
        t.put(&m, b"held", b"v", Duration::ZERO).unwrap();
        t.prepare(&[7; 16]).unwrap();
        db.rotate_unified_for_tests();
        while db.pending_flushes_for_tests() > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        db.compact(&m).unwrap();
        db.sync_wal().unwrap();
        drop(db);
    }
    let want: [(&[u8], &[u8]); 2] = [(b"j", b"base|x"), (b"k", b"|a|b")];
    let check = |db: &DB, stage: &str| {
        let m = db.get_column_family("m").unwrap();
        for (k, v) in want {
            assert_eq!(db.get(&m, k).unwrap(), v, "{stage}: get");
        }
        let txn = db.begin();
        let mut it = txn.new_iterator(&m);
        it.seek_to_first();
        for (k, v) in want {
            assert!(it.valid(), "{stage}: forward");
            assert_eq!((it.key(), it.value()), (k, v), "{stage}: forward");
            it.next();
        }
        it.seek_to_last();
        for (k, v) in want.iter().rev() {
            assert!(it.valid(), "{stage}: reverse");
            assert_eq!((it.key(), it.value()), (*k, *v), "{stage}: reverse");
            it.prev();
        }
    };
    let db = open();
    check(&db, "open");
    let m = db.get_column_family("m").unwrap();
    db.rotate_unified_for_tests();
    while db.pending_flushes_for_tests() > 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    check(&db, "two tables");
    db.compact(&m).unwrap();
    check(&db, "compacted");
    db.abort_prepared(&[7; 16]).unwrap();
    db.close().unwrap();
}

/// Flush the shared memtable and wait for it (unified layout).
fn flush_unified(db: &DB) {
    db.rotate_unified_for_tests();
    while db.pending_flushes_for_tests() > 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn open_unified(path: &str) -> DB {
    let mut opts = Options::new(path);
    opts.unified_memtable = true;
    opts.merge_fns = vec![Arc::new(Concat)];
    DB::open(opts).unwrap()
}

/// The replay floor (`CAP_WAL_FLOOR`). A prepare keeps its generation on disk
/// past that generation's flush; replaying it whole put the flushed put back,
/// and once compaction had dropped it together with the later tombstone —
/// whose generation was unlinked — nothing deleted it any more.
#[test]
fn pinned_generation_replay_never_resurrects_a_compacted_delete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS).unwrap();
        assert_ne!(db.format_capabilities() & ondadb::format::CAP_WAL_FLOOR, 0, "implied");
        let c = db.create_column_family("c", ColumnFamilyConfig::default()).unwrap();
        db.put(&c, b"k", b"old", Duration::ZERO).unwrap();
        let mut t = db.begin();
        t.put(&c, b"held", b"v", Duration::ZERO).unwrap();
        t.prepare(&[7; 16]).unwrap();
        flush_unified(&db); // flushed, but the prepare keeps it on disk
        db.delete(&c, b"k").unwrap();
        flush_unified(&db); // flushed and unlinked
        db.compact(&c).unwrap(); // the put and its tombstone are dropped
        assert!(db.get(&c, b"k").is_err(), "before the reopen");
        db.sync_wal().unwrap();
        drop(db);
    }
    let floor = ondadb::manifest_edit::recover_catalog(&path).unwrap().wal_floor;
    assert!(floor >= 2, "two flushes raised the floor past the pinned generation: {floor}");
    for round in 0..2 {
        let db = open_unified(&path);
        let c = db.get_column_family("c").unwrap();
        assert!(db.get(&c, b"k").is_err(), "open {round}: the deleted key came back");
        let txn = db.begin();
        let mut it = txn.new_iterator(&c);
        it.seek_to_first();
        assert!(!it.valid(), "open {round}: a scan shows the deleted key");
        drop(it);
        drop(txn);
        assert_eq!(db.list_prepared().len(), 1, "open {round}: the prepare is still recovered");
        drop(db);
    }
    let db = open_unified(&path);
    db.commit_prepared(&[7; 16]).unwrap();
    let c = db.get_column_family("c").unwrap();
    assert_eq!(db.get(&c, b"held").unwrap(), b"v");
    assert!(db.get(&c, b"k").is_err());
    db.close().unwrap();
}

/// The same with a merge chain: the base and an operand sat in the pinned
/// generation, a delete above them in an unlinked one, and compaction dropped
/// all three. A whole replay brought the chain back as `base|x`.
#[test]
fn pinned_generation_replay_never_resurrects_a_compacted_operand() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS).unwrap();
        let m = db
            .create_column_family(
                "m",
                ColumnFamilyConfig {
                    merge_operator_name: Some(Concat.name().to_string()),
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        db.put(&m, b"k", b"base", Duration::ZERO).unwrap();
        db.merge(&m, b"k", b"x").unwrap();
        let mut t = db.begin();
        t.put(&m, b"held", b"v", Duration::ZERO).unwrap();
        t.prepare(&[8; 16]).unwrap();
        flush_unified(&db);
        db.delete(&m, b"k").unwrap();
        flush_unified(&db);
        db.compact(&m).unwrap();
        assert!(db.get(&m, b"k").is_err());
        db.sync_wal().unwrap();
        drop(db);
    }
    let db = open_unified(&path);
    let m = db.get_column_family("m").unwrap();
    let got = db.get(&m, b"k");
    assert!(got.is_err(), "the flushed chain came back: {got:?}");
    db.abort_prepared(&[8; 16]).unwrap();
    db.close().unwrap();
}

/// A committed prepare whose decision lies in a flushed generation that
/// another prepare keeps: its records are in a table, so a reopen must not
/// install them again — here, over a delete compaction has since dropped
/// together with them.
#[test]
fn a_commit_decision_below_the_floor_installs_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS).unwrap();
        let c = db.create_column_family("c", ColumnFamilyConfig::default()).unwrap();
        let mut keep = db.begin();
        keep.put(&c, b"held", b"v", Duration::ZERO).unwrap();
        keep.prepare(&[1; 16]).unwrap();
        let mut t = db.begin();
        t.put(&c, b"k", b"committed", Duration::ZERO).unwrap();
        t.prepare(&[2; 16]).unwrap();
        db.commit_prepared(&[2; 16]).unwrap();
        flush_unified(&db); // the decision's generation, kept by [1; 16]
        db.delete(&c, b"k").unwrap();
        flush_unified(&db);
        db.compact(&c).unwrap();
        assert!(db.get(&c, b"k").is_err());
        db.sync_wal().unwrap();
        drop(db);
    }
    let db = open_unified(&path);
    let c = db.get_column_family("c").unwrap();
    assert!(db.get(&c, b"k").is_err(), "the committed prepare was installed again");
    assert_eq!(db.list_prepared().len(), 1);
    db.abort_prepared(&[1; 16]).unwrap();
    db.close().unwrap();
}

/// Above the floor nothing changes: a crash before the flush still replays
/// the committed prepare and the ordinary writes of the unflushed generation.
#[test]
fn generations_above_the_floor_replay_whole() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS).unwrap();
        let c = db.create_column_family("c", ColumnFamilyConfig::default()).unwrap();
        db.put(&c, b"a", b"1", Duration::ZERO).unwrap();
        flush_unified(&db);
        db.put(&c, b"b", b"2", Duration::ZERO).unwrap();
        let mut t = db.begin();
        t.put(&c, b"p", b"3", Duration::ZERO).unwrap();
        t.prepare(&[3; 16]).unwrap();
        db.commit_prepared(&[3; 16]).unwrap();
        db.sync_wal().unwrap();
        drop(db);
    }
    let db = open_unified(&path);
    let c = db.get_column_family("c").unwrap();
    for (k, v) in [(&b"a"[..], &b"1"[..]), (b"b", b"2"), (b"p", b"3")] {
        assert_eq!(db.get(&c, k).unwrap(), v);
    }
    db.close().unwrap();
}

/// A crash: `drop` alone is a close, whose final flush would put the WAL's
/// contents in a table. Poisoned, that flush makes nothing durable — no table
/// in the catalog, no floor, no unlinked segment — so the next open has only
/// replay to go on, as after a real crash.
fn crash(db: DB) {
    db.fail_stop_for_tests("simulated crash");
    drop(db);
}

/// An open appends at the floor or above, never below it: a directory can
/// hold a floor and no segment at all (every generation flushed and unlinked
/// by a clean close, or a checkpoint, which copies the catalog but no WAL).
/// Restarting the numbering at 0 there put the next writes below the floor,
/// and a crash then dropped them at replay as already flushed.
#[test]
fn an_open_never_appends_below_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS).unwrap();
        let c = db.create_column_family("c", ColumnFamilyConfig::default()).unwrap();
        for i in 0..3u8 {
            db.put(&c, &[b'f', i], b"v", Duration::ZERO).unwrap();
            flush_unified(&db);
        }
        db.close().unwrap();
    }
    let floor = ondadb::manifest_edit::recover_catalog(&path).unwrap().wal_floor;
    assert!(floor > 0, "the flushes raised the floor");
    for round in 0..2u8 {
        {
            let db = open_unified(&path);
            let c = db.get_column_family("c").unwrap();
            db.put(&c, &[b'k', round], b"after", Duration::ZERO).unwrap();
            db.sync_wal().unwrap();
            crash(db);
        }
        let db = open_unified(&path);
        let c = db.get_column_family("c").unwrap();
        for r in 0..=round {
            assert_eq!(
                db.get(&c, &[b'k', r]).unwrap(),
                b"after",
                "round {round}: a write after a reopen fell below the floor {floor}"
            );
        }
        db.close().unwrap();
    }
}

/// The copy case: a checkpoint carries the catalog, floor included, and no
/// WAL segment, so its first open finds no generation to continue from.
#[test]
fn a_checkpoint_appends_at_its_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let copy = dir.path().join("copy").to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS).unwrap();
        let c = db.create_column_family("c", ColumnFamilyConfig::default()).unwrap();
        for i in 0..3u8 {
            db.put(&c, &[b'f', i], b"v", Duration::ZERO).unwrap();
            flush_unified(&db);
        }
        db.checkpoint(&copy).unwrap();
        db.close().unwrap();
    }
    let floor = ondadb::manifest_edit::recover_catalog(&copy).unwrap().wal_floor;
    assert!(floor > 0, "the checkpoint carries the floor");
    {
        let db = open_unified(&copy);
        let c = db.get_column_family("c").unwrap();
        db.put(&c, b"k", b"after", Duration::ZERO).unwrap();
        db.sync_wal().unwrap();
        crash(db);
    }
    let db = open_unified(&copy);
    let c = db.get_column_family("c").unwrap();
    assert_eq!(db.get(&c, b"k").unwrap(), b"after", "the write fell below the floor {floor}");
    assert_eq!(db.get(&c, &[b'f', 2]).unwrap(), b"v");
    db.close().unwrap();
}
