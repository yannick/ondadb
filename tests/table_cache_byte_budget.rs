//! The open-reader cache must stay inside a **byte** budget, not just a count.
//!
//! `max_open_readers` bounds how many readers are open, which is only a memory
//! bound if readers cost the same. They do not. Per-reader resident cost tracks
//! the table's key count, and on spada's staging cluster (2026-08-09) it varied
//! about 30x with segment size alone: under a megabyte per reader at
//! 256-document segments, ~20 MB at 8192-document segments. The count stayed at
//! 4096 the whole time; the memory it implied went from a few hundred megabytes
//! to roughly 10 GiB, and nodes OOMed. `max_open_reader_bytes` is the bound in
//! the unit that was actually running out.
//!
//! These tests deliberately do not hard-code a per-reader size. They measure it
//! from the fixture and derive the budget, so a change to the block size, bloom
//! bits or key length cannot quietly turn a budget into "larger than everything"
//! and leave the assertions passing while proving nothing.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, DB};

/// The value every fixture key carries; big enough that a table spans several
/// blocks, so its index has more than one entry to account for.
const VALUE: [u8; 64] = [b'v'; 64];

fn opts(dir: &std::path::Path, max_open: usize, max_bytes: usize) -> Options {
    Options {
        path: dir.to_string_lossy().into_owned(),
        max_open_readers: max_open,
        max_open_reader_bytes: max_bytes,
        ..Options::default()
    }
}

/// Write `tables` L0 tables without letting compaction merge them away.
fn write_tables(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, tables: usize, per: usize) {
    for t in 0..tables {
        let mut ing = db.start_ingestion(cf).expect("start ingestion");
        for i in 0..per {
            let key = format!("{t:04}/{i:08}");
            ing.write(key.as_bytes(), &VALUE, Duration::ZERO)
                .expect("write");
        }
        ing.finish().expect("finish");
    }
}

fn cf_of(db: &DB) -> std::sync::Arc<ondadb::ColumnFamily> {
    db.create_column_family(
        "t",
        ColumnFamilyConfig {
            // High trigger: keep the tables separate so there is something to
            // bound. Compaction merging them would make these tests vacuous.
            l1_file_count_trigger: 10_000,
            ..ColumnFamilyConfig::default()
        },
    )
    .expect("create cf")
}

/// Touch the first key of every table, so every reader gets opened.
fn touch_all(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, tables: usize) {
    let mut txn = db.begin();
    for t in 0..tables {
        txn.get(&cf.clone(), format!("{t:04}/{:08}", 0).as_bytes())
            .expect("get");
    }
    txn.rollback().expect("rollback");
}

/// Lowering the byte budget must evict until the bytes fit — and only until.
#[test]
fn a_byte_budget_evicts_until_the_resident_bytes_fit() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 40;
    const PER: usize = 200;

    // Count bound far above the table count and the byte bound off, so the
    // starting state is "everything open" and each bound can be tested alone.
    let db = DB::open(opts(dir.path(), 10_000, 0)).expect("open");
    let cf = cf_of(&db);
    write_tables(&db, &cf, TABLES, PER);
    assert_eq!(
        cf.l0_file_count(),
        TABLES,
        "the fixture needs {TABLES} separate tables; compaction merged them"
    );

    touch_all(&db, &cf, TABLES);
    let (open, _, _, _) = db.table_cache_stats();
    let (bytes, budget) = db.table_cache_bytes();
    assert_eq!(open, TABLES, "expected every reader open before bounding");
    assert_eq!(budget, 0, "this arm starts with the byte bound off");
    assert!(
        bytes > 0,
        "{TABLES} open readers report 0 resident bytes — the accounting is not \
         wired up, so every byte assertion below would pass vacuously"
    );

    // Derive a budget of about four readers from the measured cost.
    let per_reader = bytes / TABLES;
    assert!(
        per_reader > 0,
        "per-reader cost measured as 0 from {bytes} bytes over {TABLES} readers"
    );
    let target = per_reader * 4;

    db.set_max_open_reader_bytes(target);

    let (open_after, _, _, closes_after) = db.table_cache_stats();
    let (bytes_after, budget_after) = db.table_cache_bytes();
    assert_eq!(budget_after, target, "the budget did not take");
    assert!(
        bytes_after <= target,
        "{bytes_after} bytes resident under a {target}-byte budget — the byte \
         bound is not being enforced, which is the whole point of S-161"
    );
    assert!(
        closes_after > 0,
        "nothing was evicted while going from {bytes} bytes to a {target}-byte \
         budget"
    );
    // Evicting *until* the bytes fit, not evicting everything: the cache should
    // still hold roughly the readers the budget pays for.
    assert!(
        open_after > 1,
        "a budget worth ~4 readers left {open_after} open — eviction overshot, \
         and an over-evicting cache re-opens on every access"
    );

    // The count bound was never the binding one here.
    assert!(
        open_after < TABLES,
        "no reader was dropped at all ({open_after} of {TABLES} still open)"
    );

    // And closing readers cannot change an answer.
    let mut txn = db.begin();
    for t in 0..TABLES {
        for i in (0..PER).step_by(37) {
            let key = format!("{t:04}/{i:08}");
            let got = txn
                .get(&cf, key.as_bytes())
                .unwrap_or_else(|e| panic!("{key} unreadable under a byte budget: {e}"));
            assert_eq!(got, VALUE, "{key} has the wrong value");
        }
    }
    txn.rollback().expect("rollback");
    db.close().expect("close");
}

/// The count bound must still bind on its own when bytes are unlimited.
#[test]
fn the_count_bound_binds_independently_of_the_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 40;
    const MAX_OPEN: usize = 6;

    let db = DB::open(opts(dir.path(), MAX_OPEN, 0)).expect("open");
    let cf = cf_of(&db);
    write_tables(&db, &cf, TABLES, 50);
    assert_eq!(cf.l0_file_count(), TABLES, "compaction merged the fixture");

    touch_all(&db, &cf, TABLES);

    let (open, _, _, closes) = db.table_cache_stats();
    assert!(
        open <= MAX_OPEN,
        "{open} readers open with max_open_readers = {MAX_OPEN} and no byte \
         bound — adding the byte budget must not have weakened the count bound"
    );
    assert!(closes > 0, "no reader was ever closed; the bound never engaged");
    db.close().expect("close");
}

/// A zero byte budget is exactly the pre-S-161 behaviour: count only.
#[test]
fn a_zero_byte_budget_leaves_the_old_behaviour_untouched() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 30;

    let db = DB::open(opts(dir.path(), 1000, 0)).expect("open");
    let cf = cf_of(&db);
    write_tables(&db, &cf, TABLES, 50);
    touch_all(&db, &cf, TABLES);

    let (open, _, _, closes) = db.table_cache_stats();
    let (bytes, budget) = db.table_cache_bytes();
    assert_eq!(
        open, TABLES,
        "with the byte bound off and a count bound of 1000, all {TABLES} \
         readers should be open; {open} are"
    );
    assert_eq!(
        closes, 0,
        "{closes} readers were evicted with both bounds slack — a byte budget \
         of 0 must mean no byte bound, not a bound of zero"
    );
    assert_eq!(budget, 0, "budget should read back as 0 (disabled)");
    assert!(
        bytes > 0,
        "occupancy is still reported when the bound is off — it is a \
         measurement, not a consequence of the bound"
    );
    db.close().expect("close");
}

/// The byte occupancy must be visible to an operator, and must agree with the
/// per-reader breakdown it is derived from.
///
/// Two accountings of the same memory that disagree are worse than one: the
/// operator cannot tell which to trust. `table_cache_bytes` is a running sum
/// maintained on insert and eviction; `reader_memory` recomputes it by walking
/// every index. They must land on the same number.
#[test]
fn the_byte_occupancy_is_visible_and_agrees_with_the_breakdown() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 20;

    let db = DB::open(opts(dir.path(), 1000, 0)).expect("open");
    let cf = cf_of(&db);
    write_tables(&db, &cf, TABLES, 120);
    touch_all(&db, &cf, TABLES);

    let (bytes, _) = db.table_cache_bytes();
    let (resident, index, bloom, readers, entries) = db.reader_memory();
    assert_eq!(
        bytes, resident,
        "the cache's running byte total ({bytes}) disagrees with the walked \
         breakdown ({resident}) — index={index} bloom={bloom} \
         readers={readers} entries={entries}"
    );
    assert!(bytes > 0, "no resident bytes reported for {readers} readers");

    // And it must fall when readers are evicted, not merely exist.
    db.set_max_open_readers(3);
    let (after, _) = db.table_cache_bytes();
    let (resident_after, ..) = db.reader_memory();
    assert!(
        after < bytes,
        "occupancy did not fall ({bytes} -> {after}) after evicting down to 3 \
         readers — the total is not being decremented on eviction, so it would \
         drift upward forever and never let a budget be satisfied"
    );
    assert_eq!(
        after, resident_after,
        "the two accountings diverged after eviction"
    );
    db.close().expect("close");
}

/// An in-flight reader survives eviction under the byte bound.
///
/// Eviction drops the *cache's* reference; a caller mid-scan holds its own
/// `Arc`, so the reader lives until that caller is done. This is the same
/// contract the count bound has, and it is what makes a hard budget safe to
/// apply during an incident rather than only at open.
#[test]
fn an_in_flight_scan_survives_a_hard_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    const TABLES: usize = 25;
    const PER: usize = 40;

    let db = DB::open(opts(dir.path(), 10_000, 0)).expect("open");
    let cf = cf_of(&db);
    write_tables(&db, &cf, TABLES, PER);

    // An iterator opens every table at once and holds those readers for its
    // whole life — the case a budget applied mid-flight is least likely to
    // survive.
    let txn = db.begin();
    let mut it =
        txn.new_iterator_bounded(&cf, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
    it.seek_to_first();

    let mut seen = 0usize;
    while it.valid() && seen < 10 {
        seen += 1;
        it.next();
    }
    assert_eq!(seen, 10, "the scan did not start");

    // Now squeeze the budget to a single byte mid-scan: every cached reader is
    // evicted down to the one the cache always keeps.
    db.set_max_open_reader_bytes(1);
    let (open_mid, _, _, closes_mid) = db.table_cache_stats();
    assert!(
        closes_mid > 0,
        "a 1-byte budget evicted nothing from {open_mid} open readers"
    );

    // The scan must still complete, and see every key.
    while it.valid() {
        seen += 1;
        it.next();
    }
    assert!(it.err().is_none(), "iteration failed: {:?}", it.err());
    assert_eq!(
        seen,
        TABLES * PER,
        "a scan that was in flight when the byte budget was slammed shut saw \
         {seen} of {} keys — eviction invalidated a live reader, which it must \
         never do",
        TABLES * PER
    );

    // A fresh read under the same budget still answers correctly; the cache is
    // re-opening rather than failing.
    let mut txn2 = db.begin();
    for t in 0..TABLES {
        let key = format!("{t:04}/{:08}", 0);
        let got = txn2
            .get(&cf, key.as_bytes())
            .unwrap_or_else(|e| panic!("{key} unreadable under a 1-byte budget: {e}"));
        assert_eq!(got, VALUE, "{key} has the wrong value");
    }
    txn2.rollback().expect("rollback");
    db.close().expect("close");
}

/// The shipped default is bounded, and that is a deliberate behaviour change.
///
/// Before S-161 the byte axis was unlimited; a store that silently relied on
/// that now gets a 1 GiB ceiling. Pinning the default here makes the change
/// visible in the test suite rather than only in a release note.
#[test]
fn the_default_options_carry_a_byte_budget() {
    assert_eq!(
        Options::default().max_open_reader_bytes,
        1 << 30,
        "the default byte budget changed; that is an operator-visible change \
         and needs a release note, not a silent edit"
    );
}
