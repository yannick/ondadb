//! The open-reader count must stay inside `max_open_readers`.
//!
//! Opening an SSTable loads its whole block index and bloom filter, and both
//! stay resident while the reader does. Before this bound existed,
//! `ColumnFamily::open` opened *every* table in the manifest and never closed
//! one — resident memory tracked total stored bytes, not the working set, and it
//! was paid at startup. Measured on a real 48 GiB store of 14,051 tables: 6.6 GB
//! twelve seconds into startup, 12 GB at twenty-five and still opening.
//!
//! Two properties matter and both are asserted: the bound is **enforced**, and
//! closing a reader **cannot change an answer**. The second is what makes the
//! first safe — a reader is a pure, re-derivable view of an immutable file.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, DB};

fn opts(dir: &std::path::Path, max_open: usize) -> Options {
    Options {
        path: dir.to_string_lossy().into_owned(),
        max_open_readers: max_open,
        ..Options::default()
    }
}

/// Write `tables` L0 tables without letting compaction merge them away.
fn write_tables(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, tables: usize, per: usize) {
    for t in 0..tables {
        let mut ing = db.start_ingestion(cf).expect("start ingestion");
        for i in 0..per {
            let key = format!("{t:04}/{i:08}");
            ing.write(key.as_bytes(), b"value", Duration::ZERO)
                .expect("write");
        }
        ing.finish().expect("finish");
    }
}

/// Reading every table must not leave every reader open.
#[test]
fn reading_many_tables_respects_the_open_reader_bound() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 40;
    const MAX_OPEN: usize = 6;

    let db = DB::open(opts(dir.path(), MAX_OPEN)).expect("open");
    let cf = db
        .create_column_family(
            "t",
            ColumnFamilyConfig {
                // High trigger: keep the tables separate so there is something
                // to bound. Compaction merging them away would make this
                // vacuous.
                l1_file_count_trigger: 10_000,
                ..ColumnFamilyConfig::default()
            },
        )
        .expect("create cf");
    write_tables(&db, &cf, TABLES, 50);
    assert_eq!(
        cf.l0_file_count(),
        TABLES,
        "the fixture needs {TABLES} separate tables to bound; compaction \
         merged them, so this test would prove nothing"
    );

    // Touch every table.
    let mut txn = db.begin();
    for t in 0..TABLES {
        let key = format!("{t:04}/{:08}", 0);
        txn.get(&cf, key.as_bytes()).expect("get");
    }
    txn.rollback().expect("rollback");

    let (open, opens, _hits, closes) = db.table_cache_stats();
    assert!(
        open <= MAX_OPEN,
        "{open} readers open with max_open_readers = {MAX_OPEN} — the bound is \
         not being enforced, which is the whole point of the cache"
    );
    assert!(
        closes > 0,
        "no reader was ever closed across {TABLES} tables under a bound of \
         {MAX_OPEN} (opens={opens}); either the fixture is not touching distinct \
         tables or eviction is not running, and both make the assertion above \
         vacuous"
    );
    db.close().expect("close");
}

/// **Closing a reader must not change an answer.** A reader is a re-derivable
/// view of an immutable file, so a bound costs re-opens and nothing else.
#[test]
fn every_key_is_readable_under_a_bound_of_one() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 25;
    const PER: usize = 40;

    // A bound of one: every access to a different table evicts the previous.
    let db = DB::open(opts(dir.path(), 1)).expect("open");
    let cf = db
        .create_column_family(
            "t",
            ColumnFamilyConfig {
                l1_file_count_trigger: 10_000,
                ..ColumnFamilyConfig::default()
            },
        )
        .expect("create cf");
    write_tables(&db, &cf, TABLES, PER);

    let mut txn = db.begin();
    for t in 0..TABLES {
        for i in 0..PER {
            let key = format!("{t:04}/{i:08}");
            let got = txn
                .get(&cf, key.as_bytes())
                .unwrap_or_else(|e| panic!("{key} unreadable under a bound of 1: {e}"));
            assert_eq!(got, b"value", "{key} has the wrong value");
        }
    }
    txn.rollback().expect("rollback");

    // And an iterator, which opens every table at once rather than one at a
    // time — the case a per-access bound is least likely to survive.
    let mut it = txn_iter(&db, &cf);
    let mut seen = 0usize;
    while it.valid() {
        seen += 1;
        it.next();
    }
    assert!(it.err().is_none(), "iteration failed: {:?}", it.err());
    assert_eq!(
        seen,
        TABLES * PER,
        "a full scan under a bound of 1 saw {seen} of {} keys",
        TABLES * PER
    );
    db.close().expect("close");
}

fn txn_iter(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>) -> ondadb::Iterator {
    let txn = db.begin();
    let mut it = txn.new_iterator_bounded(
        cf,
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
    );
    it.seek_to_first();
    it
}

/// The runtime setter must actually change the bound.
///
/// `Options::max_open_readers` is set at open; `DB::set_max_open_readers`
/// changes it afterwards, and that is the path spada's configuration uses. It
/// was plumbed and appeared to do nothing in a live experiment — both arms of a
/// 512-vs-20,000 comparison reported 512 open readers — so the setter itself is
/// pinned here rather than trusted.
#[test]
fn the_runtime_setter_changes_the_bound() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 30;

    // Open with a SMALL bound, then raise it well above the table count.
    let db = DB::open(opts(dir.path(), 4)).expect("open");
    let cf = db
        .create_column_family(
            "t",
            ColumnFamilyConfig {
                l1_file_count_trigger: 10_000,
                ..ColumnFamilyConfig::default()
            },
        )
        .expect("create cf");
    write_tables(&db, &cf, TABLES, 30);

    db.set_max_open_readers(1000);

    let mut txn = db.begin();
    for t in 0..TABLES {
        txn.get(&cf, format!("{t:04}/{:08}", 0).as_bytes()).expect("get");
    }
    txn.rollback().expect("rollback");

    let (open, opens, _hits, closes) = db.table_cache_stats();
    assert_eq!(
        open, TABLES,
        "after raising the bound to 1000, all {TABLES} readers should be open; \
         {open} are (opens={opens} closes={closes}) — the runtime setter is not \
         taking effect, which is what spada's storage.max_open_readers relies on"
    );
    assert_eq!(
        closes, 0,
        "nothing should have been evicted under a bound of 1000 with {TABLES} \
         tables, but {closes} readers were closed"
    );

    // And lowering it must evict immediately, not lazily on the next insert.
    db.set_max_open_readers(5);
    let (open_after, _, _, closes_after) = db.table_cache_stats();
    assert!(
        open_after <= 5,
        "lowering the bound to 5 left {open_after} readers open — set_max_open \
         must evict on the spot, or a memory limit does not take hold until the \
         next read"
    );
    assert!(closes_after > 0, "no eviction recorded after lowering the bound");
    db.close().expect("close");
}
