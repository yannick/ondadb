//! Pessimistic transaction locking (3.3): `DB::begin_pessimistic`,
//! `Txn::get_for_update`, wait-die, and the snapshot refresh on lock grant.
//!
//! The release funnel has its own file (`tests/txn_lock_release.rs`); the
//! rules that only exist where 3.3 meets another feature — the lock-to-
//! reservation conversion with 3.2, merge operands, range deletes — live in
//! `tests/composition.rs` with the other cross-feature rules.
//!
//! **Every threaded test here pins two orderings with barriers**: which
//! transaction is *older* (begun first, and therefore the one wait-die lets
//! wait) and which one holds the lock first. Let either float and the test
//! silently measures something else — usually the younger requester dying,
//! which is a different assertion.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, OndaError, Options, DB};

fn open(dir: &std::path::Path) -> DB {
    DB::open(Options::new(dir.to_str().unwrap())).unwrap()
}

fn cf(db: &DB, name: &str) -> Arc<ColumnFamily> {
    db.create_column_family(name, ColumnFamilyConfig::default())
        .unwrap()
}

const LEVELS: [IsolationLevel; 5] = [
    IsolationLevel::ReadUncommitted,
    IsolationLevel::ReadCommitted,
    IsolationLevel::RepeatableRead,
    IsolationLevel::Snapshot,
    IsolationLevel::Serializable,
];

/// Spin until `f` holds. Used instead of a sleep so no test encodes a guess
/// about scheduling.
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "timed out waiting for {what}"
        );
        std::thread::yield_now();
    }
}

fn value(db: &DB, c: &Arc<ColumnFamily>, key: &[u8]) -> Option<u64> {
    match db.get(c, key) {
        Ok(v) => Some(u64::from_be_bytes(v.try_into().expect("8-byte counter"))),
        Err(OndaError::NotFound) => None,
        Err(e) => panic!("read failed: {e}"),
    }
}

// ---- acquisition points ---------------------------------------------------

/// The holder takes the lock through `get_for_update`; a second, *older*
/// reader waits for it rather than reading around it.
#[test]
fn get_for_update_blocks_second_reader() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    db.put(&c, b"k", b"v0", Duration::ZERO).unwrap();

    let begun = Arc::new(Barrier::new(2));
    let held = Arc::new(Barrier::new(2));
    let read = Arc::new(AtomicU64::new(0));
    let waiter = {
        let (db, c) = (db.clone(), c.clone());
        let (begun, held, read) = (begun.clone(), held.clone(), read.clone());
        std::thread::spawn(move || {
            let mut t = db.begin_pessimistic();
            begun.wait();
            held.wait();
            let v = t.get_for_update(&c, b"k").unwrap();
            read.store(1, Ordering::SeqCst);
            // The grant refreshed the snapshot, so this is the holder's value,
            // not the one the transaction began at.
            assert_eq!(v, b"v1", "the waiter must see what it waited for");
            t.commit().unwrap();
        })
    };
    begun.wait();
    let mut holder = db.begin_pessimistic();
    holder.get_for_update(&c, b"k").unwrap();
    holder.put(&c, b"k", b"v1", Duration::ZERO).unwrap();
    held.wait();

    wait_until("the second reader to park", || {
        db.txn_lock_waiters_for_tests(&c, b"k") == 1
    });
    assert_eq!(
        read.load(Ordering::SeqCst),
        0,
        "the second reader read through a held lock"
    );
    holder.commit().unwrap();
    waiter.join().unwrap();
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

/// `get_for_update` acquires **before** `get`'s buffered-write scan, so a
/// transaction cannot read a key it wrote without holding that key's lock.
#[test]
fn get_for_update_locks_key_the_txn_already_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let mut t = db.begin_pessimistic();
    // A younger transaction, created now, so the assertion below is about the
    // lock and not about who is older.
    let mut younger = db.begin_pessimistic();

    t.put(&c, b"k", b"v", Duration::ZERO).unwrap();
    // Re-entrant: the key is already this transaction's, and asking again
    // returns the buffered value rather than deadlocking against itself.
    assert_eq!(t.get_for_update(&c, b"k").unwrap(), b"v");
    assert_eq!(
        younger.get_for_update(&c, b"k").unwrap_err().kind(),
        "conflict",
        "the key is still locked after the owner read its own write"
    );
    t.rollback().unwrap();
    younger.rollback().unwrap();
    db.close().unwrap();
}

/// The feature is opt-in on both sides: an optimistic transaction neither
/// takes locks nor waits for them.
#[test]
fn optimistic_txn_never_waits() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    db.put(&c, b"k", b"v0", Duration::ZERO).unwrap();

    let mut holder = db.begin_pessimistic();
    holder.put(&c, b"k", b"held", Duration::ZERO).unwrap();

    let start = Instant::now();
    let mut optimistic = db.begin();
    optimistic.put(&c, b"k", b"free", Duration::ZERO).unwrap();
    optimistic
        .commit()
        .expect("nothing has committed to the key since this snapshot");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "an optimistic commit waited for a lock"
    );
    assert_eq!(db.txn_lock_waiters_for_tests(&c, b"k"), 0);

    // And the other half of "locks are advisory": the lock holder loses to the
    // writer that ignored it, exactly as first-committer-wins says.
    assert_eq!(holder.commit().unwrap_err().kind(), "conflict");
    db.close().unwrap();
}

#[test]
fn get_for_update_requires_a_pessimistic_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let mut t = db.begin();
    assert_eq!(
        t.get_for_update(&c, b"k").unwrap_err().kind(),
        "invalid_args",
        "silently degrading to `get` would hand back an unprotected value"
    );
    db.close().unwrap();
}

// ---- fallible buffering ---------------------------------------------------

/// A wait-die abort now surfaces from `put`, not only from `commit`.
#[test]
fn wait_die_abort_surfaces_from_put() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    let mut older = db.begin_pessimistic();
    let mut younger = db.begin_pessimistic();
    older.put(&c, b"k", b"a", Duration::ZERO).unwrap();

    let err = younger.put(&c, b"k", b"b", Duration::ZERO).unwrap_err();
    assert_eq!(err.kind(), "conflict", "{err}");
    // The transaction is still usable — nothing was buffered — and rolling it
    // back is the caller's contract, exactly as after a commit conflict.
    younger.rollback().unwrap();
    older.commit().unwrap();
    db.close().unwrap();
}

/// Upgrade on write: a buffered write takes the key's lock at buffer time, so
/// an older reader parks on a transaction that has only buffered.
#[test]
fn upgrade_on_write_acquires_lock_at_buffer_time() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    db.put(&c, b"k", b"v0", Duration::ZERO).unwrap();

    let begun = Arc::new(Barrier::new(2));
    let buffered = Arc::new(Barrier::new(2));
    let waiter = {
        let (db, c) = (db.clone(), c.clone());
        let (begun, buffered) = (begun.clone(), buffered.clone());
        std::thread::spawn(move || {
            let mut t = db.begin_pessimistic();
            begun.wait();
            buffered.wait();
            assert_eq!(t.get_for_update(&c, b"k").unwrap(), b"v1");
            t.rollback().unwrap();
        })
    };
    begun.wait();
    let mut holder = db.begin_pessimistic();
    // Only a buffered put — no commit, no lock call of any other kind.
    holder.put(&c, b"k", b"v1", Duration::ZERO).unwrap();
    buffered.wait();
    wait_until("the reader to park on a merely buffered write", || {
        db.txn_lock_waiters_for_tests(&c, b"k") == 1
    });
    holder.commit().unwrap();
    waiter.join().unwrap();
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

// ---- the snapshot refresh -------------------------------------------------

/// One round of increment-under-contention. Returns `(commits, commit
/// conflicts, acquisition conflicts)`.
///
/// Acquisition conflicts and commit conflicts are counted **separately**, and
/// the distinction is the whole point of the measurement: wait-die kills a
/// younger requester by design, and the caller retries exactly as it would
/// after any conflict. What the feature promises is that a transaction which
/// *gets* the lock does not then lose at commit — a zero in the middle column.
fn contend(db: &Arc<DB>, c: &Arc<ColumnFamily>, pessimistic: bool, rounds: usize) -> (u64, u64, u64) {
    let commits = Arc::new(AtomicU64::new(0));
    let commit_conflicts = Arc::new(AtomicU64::new(0));
    let acquire_conflicts = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let (db, c) = (db.clone(), c.clone());
        let (commits, commit_conflicts, acquire_conflicts) = (
            commits.clone(),
            commit_conflicts.clone(),
            acquire_conflicts.clone(),
        );
        threads.push(std::thread::spawn(move || {
            for _ in 0..rounds {
                loop {
                    let mut t = if pessimistic {
                        db.begin_pessimistic()
                    } else {
                        db.begin()
                    };
                    let read = if pessimistic {
                        t.get_for_update(&c, b"hot")
                    } else {
                        t.get(&c, b"hot")
                    };
                    let current = match read {
                        Ok(v) => u64::from_be_bytes(v.try_into().expect("8-byte counter")),
                        Err(OndaError::NotFound) => 0,
                        Err(e) => {
                            assert_eq!(e.kind(), "conflict", "{e}");
                            acquire_conflicts.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    if let Err(e) = t.put(&c, b"hot", &(current + 1).to_be_bytes(), Duration::ZERO)
                    {
                        assert_eq!(e.kind(), "conflict", "{e}");
                        acquire_conflicts.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    match t.commit() {
                        Ok(()) => {
                            commits.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(e) => {
                            assert_eq!(e.kind(), "conflict", "{e}");
                            commit_conflicts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    (
        commits.load(Ordering::Relaxed),
        commit_conflicts.load(Ordering::Relaxed),
        acquire_conflicts.load(Ordering::Relaxed),
    )
}

/// **The feature.** Two threads, one key, 1000 rounds each at `Snapshot`: a
/// transaction that holds the lock never loses its commit.
///
/// This is what the snapshot refresh buys. Without it the grant would hand the
/// lock over and the commit would still find `peek_seq(key) > read_seq` — the
/// predecessor's write, committed while this transaction waited for it — and
/// abort on the very thing it waited for.
#[test]
fn pessimistic_hot_key_serializes_without_conflict() {
    const ROUNDS: usize = 1000;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    let (commits, commit_conflicts, _acquire) = contend(&db, &c, true, ROUNDS);
    assert_eq!(
        commit_conflicts, 0,
        "a lock holder must not lose its commit"
    );
    assert_eq!(commits, 2 * ROUNDS as u64);
    assert_eq!(
        value(&db, &c, b"hot"),
        Some(2 * ROUNDS as u64),
        "no increment was lost"
    );
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

/// The control: the same workload optimistically aborts at commit, repeatedly.
#[test]
fn optimistic_hot_key_aborts() {
    const ROUNDS: usize = 1000;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    let (commits, commit_conflicts, acquire_conflicts) = contend(&db, &c, false, ROUNDS);
    assert!(
        commit_conflicts > 0,
        "the optimistic control saw no conflicts at all — the contrast is the feature"
    );
    assert_eq!(acquire_conflicts, 0, "an optimistic txn takes no lock");
    assert_eq!(commits, 2 * ROUNDS as u64);
    assert_eq!(value(&db, &c, b"hot"), Some(2 * ROUNDS as u64));
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

/// At `Serializable` the refresh revalidates the read set against the OLD
/// snapshot first, so a stale read aborts here rather than validating silently
/// under an adopted snapshot.
#[test]
fn serializable_refresh_aborts_on_changed_read_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    db.put(&c, b"r", b"0", Duration::ZERO).unwrap();
    db.put(&c, b"k", b"0", Duration::ZERO).unwrap();

    let mut t = db.begin_pessimistic_with_isolation(IsolationLevel::Serializable);
    assert_eq!(t.get(&c, b"r").unwrap(), b"0");
    // Somebody else changes what this transaction read.
    db.put(&c, b"r", b"1", Duration::ZERO).unwrap();

    let err = t.get_for_update(&c, b"k").unwrap_err();
    assert_eq!(
        err.kind(),
        "conflict",
        "the refresh must not adopt a snapshot under which a stale read validates"
    );
    t.rollback().unwrap();
    db.close().unwrap();
}

/// `RepeatableRead` never refreshes: its whole contract is a snapshot that
/// does not move, and a lock grant does not get to break it.
#[test]
fn repeatable_read_does_not_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    db.put(&c, b"k", b"v0", Duration::ZERO).unwrap();

    let mut t = db.begin_pessimistic_with_isolation(IsolationLevel::RepeatableRead);
    let before = t.read_seq_for_tests();
    db.put(&c, b"k", b"v1", Duration::ZERO).unwrap();
    assert_eq!(
        t.get_for_update(&c, b"k").unwrap(),
        b"v0",
        "RepeatableRead reads its own snapshot, grant or no grant"
    );
    assert_eq!(
        t.read_seq_for_tests(),
        before,
        "a grant moved a RepeatableRead snapshot"
    );
    t.rollback().unwrap();
    db.close().unwrap();
}

/// The refresh acquires the new snapshot before releasing the old one, so the
/// GC floor never transiently jumps forward past a version the transaction
/// still needs.
#[test]
fn refresh_never_advances_oldest_snapshot_early() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    db.put(&c, b"k", b"v", Duration::ZERO).unwrap();

    // ONE long-lived transaction refreshing over and over, so the snapshot map
    // is never empty for a reason other than the swap. (Between two separate
    // transactions the map does empty, `oldest_snapshot()` falls back to
    // `visible_seq()`, and a later transaction pinning a lower sequence makes
    // it fall again — a legitimate decrease that says nothing about the swap.)
    let mut t = db.begin_pessimistic();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // With release-before-acquire the map momentarily empties and
    // `oldest_snapshot()` jumps to `visible_seq()` — which the writer below
    // keeps pushing up — before coming back DOWN to the adopted snapshot. A
    // decrease is that bug and nothing else.
    //
    // The window is a few instructions wide, so this is a probabilistic net,
    // not a proof: measured against a deliberately inverted swap it caught the
    // inversion at 40,000 rounds and missed it at 2,000. The round count is
    // therefore load-bearing — do not lower it.
    let sampler = {
        let (db, stop) = (db.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut last = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let now = db.oldest_snapshot_for_tests();
                assert!(
                    now >= last,
                    "oldest_snapshot went backwards across a refresh: {last} -> {now}"
                );
                last = now;
            }
        })
    };
    let writer = {
        let (db, c, stop) = (db.clone(), c.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                db.put(&c, b"other", b"v", Duration::ZERO).unwrap();
            }
        })
    };
    for i in 0..40_000u64 {
        // A fresh key each round, so every acquisition is a real grant and
        // therefore a real refresh.
        t.get_for_update(&c, &i.to_be_bytes()).unwrap_err();
    }
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    writer.join().unwrap();
    assert!(
        t.read_seq_for_tests() > 0,
        "the refreshes should have moved the snapshot"
    );
    t.rollback().unwrap();
    Arc::try_unwrap(db).unwrap().close().unwrap();
}

/// The documented cost of the refresh: a pessimistic `Snapshot` transaction is
/// no longer snapshot-isolated across a lock grant. Read skew (G-single)
/// becomes possible where snapshot isolation prevented it.
///
/// Pinned as a test rather than only as a sentence in the docs, because it is
/// a real behaviour change and a future reader needs to see it fail if it ever
/// silently goes away.
#[test]
fn snapshot_refresh_admits_read_skew() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = cf(&db, "d");
    db.put(&c, b"x", b"0", Duration::ZERO).unwrap();
    db.put(&c, b"y", b"0", Duration::ZERO).unwrap();

    let mut t = db.begin_pessimistic();
    assert_eq!(t.get(&c, b"x").unwrap(), b"0");
    // A concurrent transaction moves both halves of the invariant.
    let mut other = db.begin();
    other.put(&c, b"x", b"1", Duration::ZERO).unwrap();
    other.put(&c, b"y", b"1", Duration::ZERO).unwrap();
    other.commit().unwrap();

    // The grant refreshes the snapshot, so `y` is read at the NEW one while
    // `x` was read at the old: the pair is inconsistent.
    assert_eq!(t.get_for_update(&c, b"y").unwrap(), b"1");
    assert_eq!(
        t.get(&c, b"x").unwrap(),
        b"1",
        "reads after the grant are all at the new snapshot"
    );
    t.rollback().unwrap();
    db.close().unwrap();
}

// ---- the isolation matrix -------------------------------------------------

/// Serialize a read-modify-write on one key through two pessimistic
/// transactions at `level`, with the waiter older than the holder so wait-die
/// makes it wait. Returns the counter's final value.
fn serialized_increment(level: IsolationLevel) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    db.put(&c, b"n", &0u64.to_be_bytes(), Duration::ZERO)
        .unwrap();

    let begun = Arc::new(Barrier::new(2));
    let held = Arc::new(Barrier::new(2));
    let waiter = {
        let (db, c) = (db.clone(), c.clone());
        let (begun, held) = (begun.clone(), held.clone());
        std::thread::spawn(move || {
            let mut t = db.begin_pessimistic_with_isolation(level);
            begun.wait();
            held.wait();
            let n = u64::from_be_bytes(
                t.get_for_update(&c, b"n").unwrap().try_into().unwrap(),
            );
            t.put(&c, b"n", &(n + 1).to_be_bytes(), Duration::ZERO)
                .unwrap();
            t.commit().unwrap_or_else(|e| panic!("{level:?}: {e}"));
        })
    };
    begun.wait();
    let mut holder = db.begin_pessimistic_with_isolation(level);
    let n = u64::from_be_bytes(holder.get_for_update(&c, b"n").unwrap().try_into().unwrap());
    holder
        .put(&c, b"n", &(n + 1).to_be_bytes(), Duration::ZERO)
        .unwrap();
    held.wait();
    wait_until("the older transaction to park", || {
        db.txn_lock_waiters_for_tests(&c, b"n") == 1
    });
    holder.commit().unwrap_or_else(|e| panic!("{level:?}: {e}"));
    waiter.join().unwrap();
    let final_value = value(&db, &c, b"n").unwrap();
    Arc::try_unwrap(db).unwrap().close().unwrap();
    final_value
}

/// G0 (dirty write) and P4 (lost update) at the four levels whose reads can see
/// the holder's committed value: locks add **ordering**, and at every one of
/// these levels that ordering is enough to prevent both.
///
/// `RepeatableRead` is the exception and has its own test below — not because
/// locking fails there, but because its snapshot deliberately does not move.
#[test]
fn pessimistic_level_contract_prevents_lost_update() {
    for level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::Snapshot,
        IsolationLevel::Serializable,
    ] {
        assert_eq!(
            serialized_increment(level),
            2,
            "{level:?} lost an update under lock serialization"
        );
    }
}

/// The one cell of the plan's Hermitage table that reality contradicts.
///
/// `RepeatableRead` does not refresh on a grant — that is the level's contract
/// and the plan says so explicitly — so the waiter reads the value its own
/// snapshot pins, which is the value from *before* the holder committed. It
/// then writes `old + 1` and, running no validation, commits. The lock made
/// the two writes ordered (G0 is prevented) but it cannot make a stale read
/// fresh, so P4 survives. Locks add ordering, not isolation.
#[test]
fn repeatable_read_keeps_its_snapshot_and_can_lose_an_update() {
    assert_eq!(
        serialized_increment(IsolationLevel::RepeatableRead),
        1,
        "RepeatableRead refreshed its snapshot on a lock grant"
    );
}

/// Every level takes locks: the feature is a property of the transaction, not
/// of its isolation level.
#[test]
fn pessimistic_level_contract_locks_at_every_level() {
    for level in LEVELS {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        let c = cf(&db, "d");
        let mut older = db.begin_pessimistic_with_isolation(level);
        let mut younger = db.begin_pessimistic_with_isolation(level);
        older.put(&c, b"k", b"v", Duration::ZERO).unwrap();
        assert_eq!(
            younger.put(&c, b"k", b"w", Duration::ZERO).unwrap_err().kind(),
            "conflict",
            "{level:?} took no lock"
        );
        younger.rollback().unwrap();
        older.rollback().unwrap();
        // And an optimistic transaction at the same level takes none.
        let mut a = db.begin_with_isolation(level);
        let mut b = db.begin_with_isolation(level);
        a.put(&c, b"k", b"v", Duration::ZERO).unwrap();
        b.put(&c, b"k", b"w", Duration::ZERO).unwrap();
        a.rollback().unwrap();
        b.rollback().unwrap();
        db.close().unwrap();
    }
}

/// G2-item write skew is prevented at `Serializable` and possible below it,
/// exactly as in optimistic mode: locking one key says nothing about the other
/// key the decision read.
#[test]
fn write_skew_still_needs_serializable() {
    for (level, prevented) in [
        (IsolationLevel::Snapshot, false),
        (IsolationLevel::Serializable, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        let c = cf(&db, "d");
        db.put(&c, b"x", b"1", Duration::ZERO).unwrap();
        db.put(&c, b"y", b"1", Duration::ZERO).unwrap();

        // Each transaction reads the other's key and writes its own; the locks
        // never meet, so ordering buys nothing.
        let mut t1 = db.begin_pessimistic_with_isolation(level);
        let mut t2 = db.begin_pessimistic_with_isolation(level);
        t1.get(&c, b"y").unwrap();
        t2.get(&c, b"x").unwrap();
        t1.put(&c, b"x", b"0", Duration::ZERO).unwrap();
        t2.put(&c, b"y", b"0", Duration::ZERO).unwrap();
        t1.commit().unwrap();
        let second = t2.commit();
        assert_eq!(
            second.is_err(),
            prevented,
            "{level:?}: write skew prevented = {prevented}"
        );
        db.close().unwrap();
    }
}

// ---- stress ---------------------------------------------------------------

/// Randomized multi-key contention through the whole transaction path: mixed
/// ages, overlapping key sets, both feature configurations. A deadlock shows
/// up as a join that never returns, so the harness timeout is the assertion;
/// the counter invariant proves it did real work and lost nothing.
#[test]
fn pessimistic_multi_key_stress_never_hangs() {
    const THREADS: usize = 6;
    const ROUNDS: usize = 150;
    const KEYS: u64 = 8;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open(dir.path()));
    let c = cf(&db, "d");
    for k in 0..KEYS {
        db.put(&c, &k.to_be_bytes(), &0u64.to_be_bytes(), Duration::ZERO)
            .unwrap();
    }
    let commits = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();
    for t in 0..THREADS {
        let (db, c, commits) = (db.clone(), c.clone(), commits.clone());
        threads.push(std::thread::spawn(move || {
            let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64).wrapping_mul(0x1234_5678);
            let mut next = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            for _ in 0..ROUNDS {
                // Two keys, always in the same order: wait-die does not need a
                // lock ordering, but a workload with one is still a workload.
                let mut keys = [next() % KEYS, next() % KEYS];
                keys.sort_unstable();
                loop {
                    let mut txn = db.begin_pessimistic();
                    let mut ok = true;
                    for k in keys {
                        let read = txn.get_for_update(&c, &k.to_be_bytes());
                        let n = match read {
                            Ok(v) => u64::from_be_bytes(v.try_into().unwrap()),
                            Err(e) => {
                                assert_eq!(e.kind(), "conflict", "{e}");
                                ok = false;
                                break;
                            }
                        };
                        if txn
                            .put(&c, &k.to_be_bytes(), &(n + 1).to_be_bytes(), Duration::ZERO)
                            .is_err()
                        {
                            ok = false;
                            break;
                        }
                    }
                    if !ok {
                        txn.rollback().unwrap();
                        continue;
                    }
                    match txn.commit() {
                        Ok(()) => {
                            commits.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(e) => assert_eq!(e.kind(), "conflict", "{e}"),
                    }
                }
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    // Each committed transaction incremented one or two distinct keys.
    let total: u64 = (0..KEYS)
        .map(|k| value(&db, &c, &k.to_be_bytes()).unwrap())
        .sum();
    assert_eq!(
        commits.load(Ordering::Relaxed),
        (THREADS * ROUNDS) as u64,
        "every round must eventually commit"
    );
    assert!(total > 0 && total <= 2 * (THREADS * ROUNDS) as u64);
    Arc::try_unwrap(db).unwrap().close().unwrap();
}
