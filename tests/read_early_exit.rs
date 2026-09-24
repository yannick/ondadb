//! Point reads stop probing tables once they hold a version newer than every
//! remaining candidate's `max_seq` (wavesdb `5ef39df`).
//!
//! The counters come from [`ondadb::perf`]: `bloom_probes` is one per
//! candidate table the read actually consulted, so a skipped table is visible
//! as a probe that did not happen. The answer-equivalence half of the feature
//! (early exit never changes a result) is the randomized oracle in
//! `column_family.rs`, which can call the exhaustive reference directly.

use std::sync::Arc;
use std::time::Duration;

use ondadb::format::CAP_RANGE_DELETES;
use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, DB};

/// A CF whose L0 files are never auto-compacted, so each flush adds exactly one
/// candidate table.
fn l0_cf(db: &DB) -> Arc<ColumnFamily> {
    db.create_column_family(
        "p",
        ColumnFamilyConfig {
            l1_file_count_trigger: 1 << 20,
            ..ColumnFamilyConfig::default()
        },
    )
    .unwrap()
}

/// `tables` flushed L0 tables, each spanning `aaa..zzz` and each holding its
/// own version of `k`.
fn layered(db: &DB, cf: &Arc<ColumnFamily>, tables: usize) {
    for i in 0..tables {
        db.put(cf, b"aaa", b"x", Duration::ZERO).unwrap();
        db.put(cf, b"k", format!("t{i}").as_bytes(), Duration::ZERO)
            .unwrap();
        db.put(cf, b"zzz", b"x", Duration::ZERO).unwrap();
        db.flush_memtable(cf).unwrap();
    }
    assert_eq!(cf.table_metadata()[0].len(), tables, "fixture: one L0 table per flush");
}

#[test]
fn memtable_hit_probes_no_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = l0_cf(&db);
    layered(&db, &cf, 3);
    db.put(&cf, b"k", b"mem", Duration::ZERO).unwrap();

    let (v, p) = db.get_with_perf(&cf, b"k");
    assert_eq!(v.unwrap(), b"mem");
    assert!(p.memtable_probes >= 1);
    assert_eq!(p.bloom_probes, 0, "memtable hit still filtered tables: {p:?}");
    assert_eq!(p.sstable_probes, 0);
    assert_eq!(p.block_misses + p.block_cache_hits, 0);
    db.close().unwrap();
}

#[test]
fn newest_l0_hit_probes_nothing_older() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = l0_cf(&db);
    layered(&db, &cf, 4);

    let (v, p) = db.get_with_perf(&cf, b"k");
    assert_eq!(v.unwrap(), b"t3");
    assert_eq!(p.bloom_probes, 1, "an L0 hit probed past the newest table: {p:?}");
    assert_eq!(p.sstable_probes, 1);

    // A miss still has to ask every candidate.
    let (v, p) = db.get_with_perf(&cf, b"kz");
    assert!(matches!(v, Err(OndaError::NotFound)));
    assert_eq!(p.bloom_probes, 4, "a miss must consult every table: {p:?}");
    db.close().unwrap();
}

#[test]
fn deleted_key_stops_at_its_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = l0_cf(&db);
    layered(&db, &cf, 3);
    db.delete(&cf, b"k").unwrap();
    db.put(&cf, b"aaa", b"x", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    // The tombstone is the newest version and lives in the newest table.
    let (v, p) = db.get_with_perf(&cf, b"k");
    assert!(matches!(v, Err(OndaError::NotFound)));
    assert_eq!(p.bloom_probes, 1, "a tombstone hit probed older tables: {p:?}");
    db.close().unwrap();
}

#[test]
fn range_delete_in_memtable_skips_every_older_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    let cf = l0_cf(&db);
    layered(&db, &cf, 3);
    db.delete_range(&cf, b"j", b"l").unwrap();

    let (v, p) = db.get_with_perf(&cf, b"k");
    assert!(matches!(v, Err(OndaError::NotFound)));
    assert_eq!(p.range_masked, 1);
    assert_eq!(p.bloom_probes, 0, "a covering span newer than every table still probed: {p:?}");
    db.close().unwrap();
}

#[test]
fn multi_get_skips_tables_older_than_the_resolved_version() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = l0_cf(&db);
    layered(&db, &cf, 3);
    db.put(&cf, b"k", b"mem", Duration::ZERO).unwrap();

    // `k` resolves in the memtable; `aaa` in the newest table; `kz` nowhere.
    let (vals, p) = db.multi_get_with_perf(&cf, &[b"k", b"aaa", b"kz"]);
    assert_eq!(vals[0].as_deref().unwrap(), b"mem");
    assert_eq!(vals[1].as_deref().unwrap(), b"x");
    assert!(matches!(vals[2], Err(OndaError::NotFound)));
    // k: 0 tables; aaa: newest only; kz: all three.
    assert_eq!(p.bloom_probes, 1 + 3, "{p:?}");
    db.close().unwrap();
}

/// The case "stop at the first hit" gets wrong. An ingestion carries the
/// sequence reserved at its START, so a put that commits after that start but
/// shares a memtable with an older put of the same key leaves the memtable
/// holding a version OLDER than the ingested table's, and after a flush the
/// older version sits in a table whose `max_seq` is higher. The read must still
/// answer with the ingested (newer) version at every stage.
#[test]
fn ingest_newer_below_older_is_still_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = l0_cf(&db);
    db.put(&cf, b"k", b"v1", Duration::ZERO).unwrap(); // seq s
    let mut ing = db.start_ingestion(&cf).unwrap(); // reserves s+1
    db.put(&cf, b"zz", b"later", Duration::ZERO).unwrap(); // s+2, same memtable
    ing.write(b"k", b"v2", Duration::ZERO).unwrap();
    ing.finish().unwrap();

    // The memtable's hit is older than the ingested table's.
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v2");
    assert_eq!(db.multi_get(&cf, &[b"k"])[0].as_deref().unwrap(), b"v2");

    db.flush_memtable(&cf).unwrap();
    let l0 = &cf.table_metadata()[0];
    assert_eq!(l0.len(), 2);
    assert!(
        l0.iter().map(|t| t.max_seq).max() > l0.iter().map(|t| t.max_seq).min(),
        "fixture: the flushed table outranks the ingested one by max_seq"
    );
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v2");
    assert_eq!(db.multi_get(&cf, &[b"k"])[0].as_deref().unwrap(), b"v2");
    db.close().unwrap();
}
