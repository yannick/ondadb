//! Opt-in database-wide read profiling (plan C F13, wavesdb
//! `EnableReadProfiling`/`ReadStats`).

use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, ReadStats, DB};

fn open(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("c", ColumnFamilyConfig::default())
        .unwrap();
    for i in 0..10u8 {
        db.put(&cf, &[b'k', i], b"v", Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    (db, cf)
}

#[test]
fn off_by_default_and_counts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    assert!(!db.read_profiling_enabled());
    db.get(&cf, b"k\x01").unwrap();
    let _ = db.multi_get(&cf, &[b"k\x01", b"k\x02"]);
    let mut it = db.begin().new_iterator(&cf);
    it.seek_to_first();
    assert_eq!(db.read_stats(), ReadStats::default());
    db.close().unwrap();
}

#[test]
fn counts_point_batch_and_iterator_reads() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.enable_read_profiling(true);

    db.get(&cf, b"k\x01").unwrap();
    let mut buf = Vec::new();
    db.get_into(&cf, b"k\x02", &mut buf).unwrap();
    let s = db.read_stats();
    assert_eq!(s.point_reads, 2);
    assert!(s.perf.memtable_probes >= 2, "{s:?}");
    assert!(s.perf.sstable_probes >= 2, "{s:?}");
    assert!(s.perf.bloom_probes >= 2, "{s:?}");
    // Every hit read a data block. Under `unsafe-fastpath` an uncompressed
    // block is a zero-copy mmap view that is neither a cache hit nor a miss,
    // and shows only as bytes read.
    assert!(
        s.perf.block_cache_hits + s.perf.block_misses >= 2 || s.perf.block_read_bytes > 0,
        "every hit read a data block: {s:?}"
    );

    let batch = db.multi_get(&cf, &[b"k\x03", b"k\x04", b"missing"]);
    assert_eq!(batch.len(), 3);
    let s = db.read_stats();
    assert_eq!((s.multi_get_calls, s.multi_get_keys), (1, 3));
    assert_eq!(s.point_reads, 2, "a batch is not counted as point reads");

    let txn = db.begin();
    let mut it = txn.new_iterator_bounded(&cf, Bound::Unbounded, Bound::Unbounded);
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        n += 1;
        it.next();
    }
    assert_eq!(n, 10);
    let s = db.read_stats();
    assert_eq!(s.iterator_ops, 11, "one seek and ten nexts");
    assert!(s.perf.iterator_steps >= 10, "{s:?}");
    assert_eq!(s.perf.iterator_seeks, 1, "{s:?}");
    db.close().unwrap();
}

/// Profiling measures through a private scope; a caller's own `PerfContext`
/// must still see the whole operation.
#[test]
fn does_not_hide_work_from_a_callers_perf_context() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    let (_, without) = db.get_with_perf(&cf, b"k\x05");
    db.enable_read_profiling(true);
    let (_, with) = db.get_with_perf(&cf, b"k\x05");
    assert_eq!(with.sstable_probes, without.sstable_probes);
    assert_eq!(with.memtable_probes, without.memtable_probes);
    assert_eq!(with.bloom_probes, without.bloom_probes);
    assert!(with.sstable_probes > 0);
    assert_eq!(db.read_stats().perf.sstable_probes, with.sstable_probes);
    db.close().unwrap();
}

#[test]
fn disable_freezes_and_enable_resets() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.enable_read_profiling(true);
    db.get(&cf, b"k\x01").unwrap();
    db.enable_read_profiling(false);
    let frozen = db.read_stats();
    assert_eq!(frozen.point_reads, 1);
    db.get(&cf, b"k\x01").unwrap();
    assert_eq!(db.read_stats(), frozen, "counted while off");
    db.enable_read_profiling(true);
    assert_eq!(db.read_stats(), ReadStats::default(), "enable must reset");
    db.close().unwrap();
}
