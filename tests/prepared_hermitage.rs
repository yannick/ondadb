//! The Hermitage anomaly table for prepared transactions (3.2).
//!
//! Rows are the Hermitage tests; the column under test is a transaction that
//! went through `prepare` → `commit_prepared` against a concurrent **ordinary**
//! writer. The expected outcome is filled per isolation level, so "all five
//! levels" is a passing table rather than a sentence.
//!
//! | Anomaly | RU | RC | RR | Snapshot | Serializable | Prepared (any level) |
//! | --- | --- | --- | --- | --- | --- | --- |
//! | G0 dirty write | A | A | A | P | P | **P** — the reservation blocks the other writer at every level |
//! | G1a dirty read (aborted) | P | P | P | P | P | **P** — prepared records are never applied and never published |
//! | G1b intermediate read | P | P | P | P | P | **P** — same reason |
//! | G1c circular information flow | P | P | P | P | P | **P** |
//! | P4 lost update | A | A | A | P | P | **P** — reservation, not validation, is what prevents it |
//! | G-single read skew | A | A | P | P | P | as the txn's own level |
//! | G2-item write skew | A | A | A | A | P | as the txn's own level |
//! | G2 phantom | A | A | A | A | **A** | as the txn's own level (documented non-goal) |
//!
//! "P" = prevented, "A" = anomaly possible. G1a–G1c are prevented at *every*
//! level because ondaDB has no dirty-read path at all: `ReadUncommitted` reads
//! the live published watermark, not uncommitted state. The prepared column's
//! G0 and P4 entries are the feature — they hold at `ReadUncommitted` too, and
//! that is only true because phase rule 5 puts the reservation check on every
//! commit path.
//!
//! The last three rows are "as the txn's own level" and are covered by the
//! existing suite (`tests/snapshot_self_conflict.rs`, `tests/db.rs`); the two
//! that a prepare could plausibly change are asserted here anyway.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, Options, DB};

fn open_unified(path: &str) -> DB {
    let db = DB::open(Options {
        unified_memtable: true,
        ..Options::new(path)
    })
    .unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
        .unwrap();
    db
}

fn cf(db: &DB, name: &str) -> Arc<ColumnFamily> {
    db.create_column_family(name, ColumnFamilyConfig::default())
        .unwrap()
}

fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

/// One test bed: an open unified database with a seeded column family.
struct Bed {
    _dir: tempfile::TempDir,
    db: DB,
    cf: Arc<ColumnFamily>,
}

fn bed() -> Bed {
    let dir = tempfile::tempdir().unwrap();
    let db = open_unified(dir.path().to_str().unwrap());
    let cf = cf(&db, "t");
    db.put(&cf, b"1", b"10", Duration::ZERO).unwrap();
    db.put(&cf, b"2", b"20", Duration::ZERO).unwrap();
    Bed {
        _dir: dir,
        db,
        cf,
    }
}

/// Generate one test per (anomaly, level) cell of the prepared column.
macro_rules! hermitage {
    ($body:ident: $($name:ident => $level:expr),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                $body($level);
            }
        )+
    };
}

// ---- G0: dirty write ------------------------------------------------------

/// G0 is prevented at **every** level for a prepared transaction, including the
/// three that perform no validation of their own. The mechanism is the
/// reservation, not the validation: an ordinary writer touching a reserved key
/// is refused before it can interleave its writes with the prepared ones.
fn g0_dirty_write(level: IsolationLevel) {
    let b = bed();
    let (db, t) = (&b.db, &b.cf);

    // T1 prepares a two-key write.
    let mut t1 = db.begin_with_isolation(level);
    t1.put(t, b"1", b"11", Duration::ZERO).unwrap();
    t1.put(t, b"2", b"21", Duration::ZERO).unwrap();
    t1.prepare(&id(1)).unwrap();

    // T2 tries to interleave its own writes to the same keys.
    let mut t2 = db.begin_with_isolation(level);
    t2.put(t, b"1", b"12", Duration::ZERO).unwrap();
    t2.put(t, b"2", b"22", Duration::ZERO).unwrap();
    assert_eq!(
        t2.commit().unwrap_err().kind(),
        "conflict",
        "G0 must be prevented for a prepared writeset at {level:?}"
    );

    db.commit_prepared(&id(1)).unwrap();
    // No interleaving happened: both keys carry T1's values.
    assert_eq!(db.get(t, b"1").unwrap(), b"11");
    assert_eq!(db.get(t, b"2").unwrap(), b"21");
    db.close().unwrap();
}

hermitage!(g0_dirty_write:
    hermitage_g0_prepared_read_uncommitted => IsolationLevel::ReadUncommitted,
    hermitage_g0_prepared_read_committed => IsolationLevel::ReadCommitted,
    hermitage_g0_prepared_repeatable_read => IsolationLevel::RepeatableRead,
    hermitage_g0_prepared_snapshot => IsolationLevel::Snapshot,
    hermitage_g0_prepared_serializable => IsolationLevel::Serializable,
);

// ---- G1a: dirty read (aborted transaction) --------------------------------

/// A prepared writeset is never applied to the memtable and never published, so
/// no reader at any level can see it — and after an abort there is nothing to
/// see at all.
fn g1a_dirty_read(level: IsolationLevel) {
    let b = bed();
    let (db, t) = (&b.db, &b.cf);

    let mut t1 = db.begin_with_isolation(level);
    t1.put(t, b"1", b"101", Duration::ZERO).unwrap();
    t1.prepare(&id(1)).unwrap();

    // T2 reads while T1 is prepared but unresolved: it must see 10.
    let mut t2 = db.begin_with_isolation(level);
    assert_eq!(
        t2.get(t, b"1").unwrap(),
        b"10",
        "a prepared value must not be readable at {level:?}"
    );
    db.abort_prepared(&id(1)).unwrap();
    assert_eq!(t2.get(t, b"1").unwrap(), b"10");
    t2.rollback().unwrap();
    // ...and after the abort it is still 10 for a fresh reader.
    assert_eq!(db.get(t, b"1").unwrap(), b"10");
    db.close().unwrap();
}

hermitage!(g1a_dirty_read:
    hermitage_g1a_prepared_read_uncommitted => IsolationLevel::ReadUncommitted,
    hermitage_g1a_prepared_read_committed => IsolationLevel::ReadCommitted,
    hermitage_g1a_prepared_repeatable_read => IsolationLevel::RepeatableRead,
    hermitage_g1a_prepared_snapshot => IsolationLevel::Snapshot,
    hermitage_g1a_prepared_serializable => IsolationLevel::Serializable,
);

// ---- G1b: intermediate read -----------------------------------------------

/// A prepare publishes its whole writeset at once at `commit_prepared`, so no
/// reader ever observes a value the transaction later overwrote — the
/// intermediate value never reaches the store at all.
fn g1b_intermediate_read(level: IsolationLevel) {
    let b = bed();
    let (db, t) = (&b.db, &b.cf);

    let mut t1 = db.begin_with_isolation(level);
    t1.put(t, b"1", b"101", Duration::ZERO).unwrap(); // intermediate
    t1.put(t, b"1", b"11", Duration::ZERO).unwrap(); // final
    t1.prepare(&id(1)).unwrap();

    let mut t2 = db.begin_with_isolation(level);
    assert_eq!(t2.get(t, b"1").unwrap(), b"10", "{level:?}");
    t2.rollback().unwrap();

    db.commit_prepared(&id(1)).unwrap();
    // Only the final value is ever observable; the intermediate one never
    // existed outside the transaction's own arena.
    assert_eq!(db.get(t, b"1").unwrap(), b"11");
    db.close().unwrap();
}

hermitage!(g1b_intermediate_read:
    hermitage_g1b_prepared_read_uncommitted => IsolationLevel::ReadUncommitted,
    hermitage_g1b_prepared_read_committed => IsolationLevel::ReadCommitted,
    hermitage_g1b_prepared_repeatable_read => IsolationLevel::RepeatableRead,
    hermitage_g1b_prepared_snapshot => IsolationLevel::Snapshot,
    hermitage_g1b_prepared_serializable => IsolationLevel::Serializable,
);

// ---- G1c: circular information flow ---------------------------------------

/// Two prepared transactions cannot read each other's unresolved writes, so no
/// cycle of information can form between them.
fn g1c_circular_information_flow(level: IsolationLevel) {
    let b = bed();
    let (db, t) = (&b.db, &b.cf);

    let mut t1 = db.begin_with_isolation(level);
    t1.put(t, b"1", b"11", Duration::ZERO).unwrap();
    let mut t2 = db.begin_with_isolation(level);
    t2.put(t, b"2", b"22", Duration::ZERO).unwrap();

    // Each reads the key the *other* is about to change, before either
    // prepares...
    assert_eq!(t1.get(t, b"2").unwrap(), b"20", "{level:?}");
    assert_eq!(t2.get(t, b"1").unwrap(), b"10", "{level:?}");
    t1.prepare(&id(1)).unwrap();
    t2.prepare(&id(2)).unwrap();

    // ...and a third reader sees neither until both resolve.
    let mut t3 = db.begin_with_isolation(level);
    assert_eq!(t3.get(t, b"1").unwrap(), b"10");
    assert_eq!(t3.get(t, b"2").unwrap(), b"20");
    t3.rollback().unwrap();

    db.commit_prepared(&id(1)).unwrap();
    db.commit_prepared(&id(2)).unwrap();
    assert_eq!(db.get(t, b"1").unwrap(), b"11");
    assert_eq!(db.get(t, b"2").unwrap(), b"22");
    db.close().unwrap();
}

hermitage!(g1c_circular_information_flow:
    hermitage_g1c_prepared_read_uncommitted => IsolationLevel::ReadUncommitted,
    hermitage_g1c_prepared_read_committed => IsolationLevel::ReadCommitted,
    hermitage_g1c_prepared_repeatable_read => IsolationLevel::RepeatableRead,
    hermitage_g1c_prepared_snapshot => IsolationLevel::Snapshot,
    hermitage_g1c_prepared_serializable => IsolationLevel::Serializable,
);

// ---- P4: lost update ------------------------------------------------------

/// P4 is prevented at every level for a prepared transaction. The mechanism is
/// again the **reservation** rather than validation: at `ReadUncommitted` there
/// is no write-write check to catch the second writer, and the reservation is
/// what refuses it instead.
fn p4_lost_update(level: IsolationLevel) {
    let b = bed();
    let (db, t) = (&b.db, &b.cf);

    let mut t1 = db.begin_with_isolation(level);
    let read = t1.get(t, b"1").unwrap();
    assert_eq!(read, b"10");
    t1.put(t, b"1", b"11", Duration::ZERO).unwrap();
    t1.prepare(&id(1)).unwrap();

    // T2 read the same value and now tries to write back its own increment.
    let mut t2 = db.begin_with_isolation(level);
    assert_eq!(t2.get(t, b"1").unwrap(), b"10");
    t2.put(t, b"1", b"11", Duration::ZERO).unwrap();
    assert_eq!(
        t2.commit().unwrap_err().kind(),
        "conflict",
        "P4 must be prevented for a prepared writeset at {level:?}"
    );

    db.commit_prepared(&id(1)).unwrap();
    assert_eq!(db.get(t, b"1").unwrap(), b"11");
    db.close().unwrap();
}

hermitage!(p4_lost_update:
    hermitage_p4_prepared_read_uncommitted => IsolationLevel::ReadUncommitted,
    hermitage_p4_prepared_read_committed => IsolationLevel::ReadCommitted,
    hermitage_p4_prepared_repeatable_read => IsolationLevel::RepeatableRead,
    hermitage_p4_prepared_snapshot => IsolationLevel::Snapshot,
    hermitage_p4_prepared_serializable => IsolationLevel::Serializable,
);

// ---- the "as the txn's own level" rows -------------------------------------

/// G-single (read skew) is a property of the *reader's* level, and a prepare
/// does not change it: a fixed-snapshot level pins its read sequence at
/// `begin`, and a prepared commit landing after that is invisible to it.
#[test]
fn hermitage_g_single_prepared_follows_the_txns_own_level() {
    for (level, sees_update) in [
        (IsolationLevel::ReadCommitted, true),
        (IsolationLevel::RepeatableRead, false),
        (IsolationLevel::Snapshot, false),
        (IsolationLevel::Serializable, false),
    ] {
        let b = bed();
        let (db, t) = (&b.db, &b.cf);

        let mut reader = db.begin_with_isolation(level);
        assert_eq!(reader.get(t, b"1").unwrap(), b"10");

        let mut writer = db.begin();
        writer.put(t, b"1", b"12", Duration::ZERO).unwrap();
        writer.put(t, b"2", b"18", Duration::ZERO).unwrap();
        writer.prepare(&id(1)).unwrap();
        db.commit_prepared(&id(1)).unwrap();

        let second = reader.get(t, b"2").unwrap();
        if sees_update {
            assert_eq!(second, b"18", "{level:?} floats on the read floor");
        } else {
            assert_eq!(
                second, b"20",
                "{level:?} pins a snapshot, so a prepared commit after `begin` is invisible"
            );
        }
        reader.rollback().unwrap();
        db.close().unwrap();
    }
}

/// G2-item (write skew) is prevented at `Serializable` by read-set validation
/// and possible below it — and a prepared transaction is validated at
/// `prepare`, so the outcome is the same, just decided earlier.
#[test]
fn hermitage_g2_item_prepared_follows_the_txns_own_level() {
    for (level, prevented) in [
        (IsolationLevel::Snapshot, false),
        (IsolationLevel::Serializable, true),
    ] {
        let b = bed();
        let (db, t) = (&b.db, &b.cf);

        // Both transactions read both keys, then each writes the other's.
        let mut t1 = db.begin_with_isolation(level);
        let mut t2 = db.begin_with_isolation(level);
        assert_eq!(t1.get(t, b"1").unwrap(), b"10");
        assert_eq!(t1.get(t, b"2").unwrap(), b"20");
        assert_eq!(t2.get(t, b"1").unwrap(), b"10");
        assert_eq!(t2.get(t, b"2").unwrap(), b"20");

        t1.put(t, b"1", b"11", Duration::ZERO).unwrap();
        t2.put(t, b"2", b"21", Duration::ZERO).unwrap();
        t1.prepare(&id(1)).unwrap();
        db.commit_prepared(&id(1)).unwrap();

        // T2's prepare runs the same validation its commit would have.
        let result = t2.prepare(&id(2));
        if prevented {
            assert_eq!(
                result.unwrap_err().kind(),
                "conflict",
                "{level:?} validates the read set, at prepare rather than at commit"
            );
        } else {
            result.expect("Snapshot does not track reads: write skew is possible");
            db.commit_prepared(&id(2)).unwrap();
            assert_eq!(db.get(t, b"2").unwrap(), b"21");
        }
        db.close().unwrap();
    }
}
