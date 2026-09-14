//! Deterministic cache reuse and MVCC regressions, plus the idle scan fixture.
use ondadb::{ColumnFamilyConfig, IsolationLevel, Options, DB};
use std::{
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};

fn open(unified: bool) -> (tempfile::TempDir, DB, Arc<ondadb::ColumnFamily>) {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.unified_memtable = unified;
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let cf = db
        .create_column_family("data", ColumnFamilyConfig::default())
        .unwrap();
    (dir, db, cf)
}

#[test]
fn unchanged_iterators_build_once_in_both_layouts() {
    for unified in [false, true] {
        let (_dir, db, cf) = open(unified);
        db.delete_range(&cf, b"a", b"z").unwrap();
        assert_eq!(db.stats().range_memtable_spans, 1);
        assert!(db.stats().range_memtable_bytes > 0);
        assert_eq!(db.stats().range_fragment_cache_bytes, 0);
        for _ in 0..100 {
            let txn = db.begin();
            let mut it = txn.new_iterator(&cf);
            it.seek_to_first();
            assert!(!it.valid());
        }
        let stats = db.stats();
        assert_eq!(stats.range_fragment_cache_builds, 1);
        assert_eq!(stats.range_fragment_cache_hits, 99);
        assert!(stats.range_fragment_cache_bytes > 0);
        assert_eq!(stats.range_fragment_retained_bytes, 0);
    }
}

#[test]
fn snapshots_reinsertions_bounds_and_direction_changes() {
    for unified in [false, true] {
        let (_dir, db, cf) = open(unified);
        for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
            db.put(&cf, key, b"old", Duration::ZERO).unwrap();
        }
        let before = db.begin_with_isolation(IsolationLevel::Snapshot);
        db.delete_range(&cf, b"b", b"d").unwrap();
        let equal = db.begin_with_isolation(IsolationLevel::Snapshot);
        let mut old = equal.new_iterator(&cf);
        old.seek_to_first();
        let bytes = db.stats().range_fragment_cache_bytes;
        db.delete_range(&cf, b"c", b"f").unwrap();
        assert_eq!(db.stats().range_fragment_retained_bytes, bytes);
        db.put(&cf, b"c", b"new", Duration::ZERO).unwrap();
        let after = db.begin_with_isolation(IsolationLevel::Snapshot);
        for (txn, want) in [
            (&before, b"abcdef".as_slice()),
            (&equal, b"adef"),
            (&after, b"acf"),
        ] {
            let mut it = txn.new_iterator(&cf);
            it.seek_to_first();
            let mut got = Vec::new();
            while it.valid() {
                got.extend_from_slice(it.key());
                it.next();
            }
            assert_eq!(got, want);
            it.seek_to_last();
            let mut back = Vec::new();
            while it.valid() {
                back.extend_from_slice(it.key());
                it.prev();
            }
            back.reverse();
            assert_eq!(back, want);
        }
        let mut got = Vec::new();
        while old.valid() {
            got.extend_from_slice(old.key());
            old.next();
        }
        assert_eq!(got, b"adef");
        for (lo, hi, want) in [
            (
                Bound::Included(b"a".as_slice()),
                Bound::Included(b"f".as_slice()),
                b"acf".as_slice(),
            ),
            (
                Bound::Excluded(b"a".as_slice()),
                Bound::Excluded(b"f".as_slice()),
                b"c".as_slice(),
            ),
        ] {
            let mut it = after.new_iterator_bounded(&cf, lo, hi);
            it.seek_to_first();
            let mut got = Vec::new();
            while it.valid() {
                got.extend_from_slice(it.key());
                it.next();
            }
            assert_eq!(got, want);
            it.seek(b"b");
            assert!(it.valid());
            assert_eq!(it.key(), b"c");
            it.seek_for_prev(b"d");
            assert!(it.valid());
            assert_eq!(it.key(), b"c");
            it.seek_to_last();
            assert!(it.valid());
            assert_eq!(it.key(), &want[want.len() - 1..]);
        }
    }
}

#[test]
fn unified_fragments_do_not_cross_family_prefixes() {
    let (_dir, db, cf) = open(true);
    let other = db
        .create_column_family("other", ColumnFamilyConfig::default())
        .unwrap();
    db.put(&cf, b"m", b"v", Duration::ZERO).unwrap();
    db.put(&other, b"m", b"v", Duration::ZERO).unwrap();
    db.delete_range(&cf, b"a", b"z").unwrap();
    for _ in 0..10 {
        let txn = db.begin();
        let mut a = txn.new_iterator(&cf);
        let mut b = txn.new_iterator(&other);
        a.seek_to_first();
        b.seek_to_first();
        assert!(!a.valid());
        assert!(b.valid());
        assert_eq!(b.key(), b"m");
    }
    assert_eq!(db.stats().range_memtable_spans, 1);
    assert_eq!(db.stats().range_fragment_cache_builds, 1);
}

#[test]
fn ordinary_reverse_comparator_preserves_cached_interval_order() {
    let (_dir, db, _) = open(false);
    let cf = db
        .create_column_family(
            "reverse",
            ColumnFamilyConfig {
                comparator_name: "reverse".into(),
                ..Default::default()
            },
        )
        .unwrap();
    for key in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(&cf, key, b"v", Duration::ZERO).unwrap();
    }
    db.delete_range(&cf, b"d", b"b").unwrap();
    for _ in 0..10 {
        let txn = db.begin();
        let mut it = txn.new_iterator_bounded(&cf, Bound::Included(b"e"), Bound::Included(b"a"));
        it.seek_to_first();
        let mut got = Vec::new();
        while it.valid() {
            got.extend_from_slice(it.key());
            it.next();
        }
        assert_eq!(got, b"eba");
        it.seek_to_last();
        let mut got = Vec::new();
        while it.valid() {
            got.extend_from_slice(it.key());
            it.prev();
        }
        assert_eq!(got, b"abe");
    }
    assert_eq!(db.stats().range_fragment_cache_builds, 1);
}

#[test]
fn concurrent_cold_readers_publish_one_snapshot() {
    for unified in [false, true] {
        let (_dir, db, cf) = open(unified);
        db.delete_range(&cf, b"a", b"z").unwrap();
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    barrier.wait();
                    let txn = db.begin();
                    let mut it = txn.new_iterator(&cf);
                    it.seek_to_first();
                    assert!(!it.valid());
                });
            }
        });
        assert_eq!(db.stats().range_fragment_cache_builds, 1);
        assert_eq!(db.stats().range_fragment_cache_hits, 7);
    }
}

#[test]
#[ignore = "synthetic idle CPU fixture; run in release with --ignored --nocapture"]
fn synthetic_idle_range_scan_benchmark() {
    for unified in [false, true] {
        for (n, unique) in [(8_331usize, 24usize), (18_117, 466)] {
            for run in 0..5 {
                let (_dir, db, cf) = open(unified);
                for i in 0..n {
                    let p = (i % unique) as u16;
                    db.delete_range(&cf, &p.to_be_bytes(), &(p + 1).to_be_bytes())
                        .unwrap();
                }
                let txn = db.begin();
                let cold = Instant::now();
                let held = txn.new_iterator(&cf);
                let cold_us = cold.elapsed().as_micros();
                let start = Instant::now();
                for _ in 0..100 {
                    let mut it = txn.new_iterator(&cf);
                    it.seek_to_first();
                    assert!(!it.valid());
                    std::hint::black_box(it);
                }
                let warm_ns = start.elapsed().as_nanos() / 100;
                let stats = db.stats();
                assert_eq!(stats.range_fragment_cache_builds, 1);
                assert_eq!(stats.range_fragment_cache_hits, 100);
                db.delete_range(&cf, b"xx", b"zz").unwrap();
                let retained = db.stats().range_fragment_retained_bytes;
                assert_eq!(retained, stats.range_fragment_cache_bytes);
                drop(held);
                assert_eq!(db.stats().range_fragment_retained_bytes, 0);
                drop(txn);
                db.flush_memtable(&cf).unwrap();
                let start = Instant::now();
                for _ in 0..100 {
                    let txn = db.begin();
                    let mut it = txn.new_iterator(&cf);
                    it.seek_to_first();
                    assert!(!it.valid());
                    std::hint::black_box(it);
                }
                println!("unified={unified} n={n} unique={unique} run={run} cold_us={cold_us} warm_ns={warm_ns} after_flush_request_ns={} span_bytes={} cache_bytes={} retained_bytes={retained} builds={} hits={}", start.elapsed().as_nanos()/100, stats.range_memtable_bytes, stats.range_fragment_cache_bytes, stats.range_fragment_cache_builds, stats.range_fragment_cache_hits);
            }
        }
    }
}

#[test]
fn bounded_scan_consults_sst_ranges_beyond_point_bounds() {
    for unified in [false] {
        let (_dir, db, cf) = open(unified);
        db.put(&cf, b"m", b"old", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        let held = db.begin_with_isolation(IsolationLevel::Snapshot);
        db.put(&cf, b"a", b"outside", Duration::ZERO).unwrap();
        db.delete_range(&cf, b"b", b"z").unwrap();
        db.flush_memtable(&cf).unwrap();
        assert!(cf
            .table_metadata()
            .iter()
            .flatten()
            .any(|meta| meta.range_count != 0
                && meta.range_max_key.as_deref().unwrap() > meta.max_key.as_slice()));
        for _ in 0..10 {
            let txn = db.begin();
            let mut it =
                txn.new_iterator_bounded(&cf, Bound::Included(b"m"), Bound::Included(b"m"));
            it.seek_to_first();
            assert!(!it.valid());
            it.seek_to_last();
            assert!(!it.valid());
            let mut old =
                held.new_iterator_bounded(&cf, Bound::Included(b"m"), Bound::Included(b"m"));
            old.seek_to_first();
            assert!(old.valid());
            assert_eq!(old.key(), b"m");
        }
    }
}

#[test]
fn reader_retention_is_accounted_after_memtable_retirement() {
    let (_dir, db, cf) = open(false);
    db.delete_range(&cf, b"a", b"z").unwrap();
    let txn = db.begin();
    let held = txn.new_iterator(&cf);
    let bytes = db.stats().range_fragment_cache_bytes;
    assert!(bytes > 0);
    db.flush_memtable(&cf).unwrap();
    let stats = db.stats();
    assert_eq!(stats.range_memtable_spans, 0);
    assert_eq!(stats.range_fragment_cache_bytes, 0);
    assert_eq!(stats.range_fragment_retained_bytes, bytes);
    drop(held);
    assert_eq!(db.stats().range_fragment_retained_bytes, 0);
}

#[test]
fn dropped_family_keeps_only_reader_owned_fragment_memory() {
    let (_dir, db, cf) = open(false);
    db.delete_range(&cf, b"a", b"z").unwrap();
    let txn = db.begin();
    let held = txn.new_iterator(&cf);
    let bytes = db.stats().range_fragment_cache_bytes;
    db.drop_column_family("data").unwrap();
    assert_eq!(db.stats().range_fragment_cache_bytes, 0);
    assert_eq!(db.stats().range_fragment_retained_bytes, bytes);
    drop(held);
    assert_eq!(db.stats().range_fragment_retained_bytes, 0);
    // The stale CF handle is intentionally still alive.
    drop(cf);
}
