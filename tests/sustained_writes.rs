//! Sustained-write behaviour introduced in 0.8.0: bounded compaction jobs,
//! debt-aware write pacing, and close semantics.
//!
//! The regression these guard against is not a crash but a *lie*: before 0.8.0
//! ingest ran at memtable speed however far compaction had fallen behind, so
//! the write rate an application measured was one the engine could not sustain,
//! and the deferred work surfaced as a multi-second `close()`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamilyConfig, IsolationLevel, Options, DB};

#[test]
fn configured_compaction_workers_run_disjoint_cfs_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path().to_str().unwrap());
    options.num_compaction_threads = 2;
    let db = DB::open(options).unwrap();
    let config = ColumnFamilyConfig {
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    };
    let a = db.create_column_family("a", config.clone()).unwrap();
    let b = db.create_column_family("b", config).unwrap();

    let (entered_tx, entered_rx) = crossbeam_channel::unbounded::<&'static str>();
    let (release_tx, release_rx) = crossbeam_channel::unbounded::<()>();
    for (name, cf) in [("a", &a), ("b", &b)] {
        let entered = entered_tx.clone();
        let release = release_rx.clone();
        let first = Arc::new(AtomicBool::new(true));
        cf.set_compaction_filter(Some(Arc::new(move |_, _| {
            if first.swap(false, Ordering::Relaxed) {
                entered.send(name).unwrap();
                release
                    .recv_timeout(Duration::from_secs(5))
                    .expect("test releases blocked compaction");
            }
            ondadb::FilterDecision::Keep
        })));
    }

    db.put(&a, b"key-a", b"value", Duration::ZERO).unwrap();
    db.flush_memtable(&a).unwrap();
    db.put(&b, b"key-b", b"value", Duration::ZERO).unwrap();
    db.flush_memtable(&b).unwrap();

    let first = entered_rx.recv_timeout(Duration::from_secs(2));
    let second = entered_rx.recv_timeout(Duration::from_millis(500));
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();

    let first = first.expect("one compaction should enter its filter");
    let second = second.expect("two configured workers should compact concurrently");
    assert_ne!(first, second);
    db.close().unwrap();
}

/// Small geometry so the tests build a multi-level tree from a modest number of
/// records; the ratios, not the absolute sizes, are what is under test.
fn small_geometry() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        write_buffer_size: 256 << 10, // 256 KiB memtable
        target_file_size: 64 << 10,   // 64 KiB output files
        l1_base_bytes: 512 << 10,     // => ~8 files in L1
        level_size_ratio: 4,
        ..ColumnFamilyConfig::default()
    }
}

fn put_range(db: &DB, cf: &Arc<ondadb::ColumnFamily>, lo: usize, hi: usize, val: &[u8]) {
    let mut i = lo;
    while i < hi {
        let end = (i + 500).min(hi);
        let mut txn = db.begin_with_isolation(IsolationLevel::ReadCommitted);
        for k in i..end {
            txn.put(cf, format!("key{k:012}").as_bytes(), val, Duration::ZERO)
                .unwrap();
        }
        txn.commit().unwrap();
        i = end;
    }
}

/// The geometry fix: a level must hold *many* files, or partial compaction is
/// impossible — one file's range covers everything below it, so every push-down
/// degenerates into rewriting the whole level (the 0.7.x behaviour).
#[test]
fn levels_hold_many_files_so_compaction_can_be_partial() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("bench", small_geometry()).unwrap();

    put_range(&db, &cf, 0, 40_000, &[b'v'; 100]);
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    let stats = cf.stats();
    let populated: Vec<(usize, (usize, u64))> = stats
        .levels
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, (n, _))| *n > 0)
        .collect();
    assert!(!populated.is_empty(), "nothing was written");

    // The property under test is that output is cut at `target_file_size`
    // (64 KiB here) rather than at `write_buffer_size` (256 KiB). Cutting at
    // the memtable size is what gave 0.7.x a single-file L1 whose range covered
    // everything below it, forcing every push-down to rewrite the whole level.
    let target = small_geometry().target_file_size as u64;
    for (lvl, (files, bytes)) in &populated {
        let avg = bytes / *files as u64;
        assert!(
            avg <= target * 2,
            "level {lvl} averages {avg} B/file against a {target} B target \
             ({files} files, {bytes} B) — output is not being cut at \
             target_file_size; levels = {:?}",
            stats.levels
        );
    }
    // And the level holding the bulk of the data must be many files, not one.
    let (_, (biggest_files, _)) = populated
        .iter()
        .max_by_key(|(_, (_, bytes))| *bytes)
        .copied()
        .unwrap();
    assert!(
        biggest_files >= 4,
        "the largest level holds {biggest_files} file(s); a level sized to one \
         file cannot be compacted partially. levels = {:?}",
        stats.levels
    );
    db.close().unwrap();
}

/// Every key survives partial-level compaction. Bounded jobs rewrite subsets of
/// a level rather than the whole thing, so a mistake in input selection or in
/// the level re-install loses records silently.
#[test]
fn all_keys_readable_after_partial_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("bench", small_geometry()).unwrap();

    const N: usize = 30_000;
    put_range(&db, &cf, 0, N, &[b'v'; 100]);
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    for k in 0..N {
        let got = db
            .get(&cf, format!("key{k:012}").as_bytes())
            .unwrap_or_else(|e| panic!("key {k} lost after compaction: {e:?}"));
        assert_eq!(got.len(), 100, "key {k} has the wrong value length");
    }

    // And again through the iterator, which reads the level structure rather
    // than point-looking-up each key.
    let mut txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    let mut count = 0usize;
    it.seek_to_first();
    while it.valid() {
        count += 1;
        it.next();
    }
    assert_eq!(count, N, "iteration lost records");
    drop(it);
    let _ = txn.rollback();
    db.close().unwrap();
}

/// Pacing must keep compaction debt *bounded* under sustained ingest.
///
/// Note what the hard threshold is and is not. It is the point at which
/// `apply_commit` blocks; it does not cap the gauge. When debt crosses it,
/// work already in flight still lands — in the worst case every sealed
/// memtable the flush pipeline is allowed to hold, which is
/// `l0_queue_stall_threshold * write_buffer_size`. An assertion of
/// `debt <= hard` is therefore unsound, and was: with a 4 MiB ceiling and a
/// 5 MiB flush pipeline this test failed roughly one run in three under
/// full-suite load. The ceiling is sized above that pipeline here, and the
/// bound allows exactly the overshoot the implementation permits.
#[test]
fn sustained_ingest_keeps_compaction_debt_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let base = small_geometry();
    const HARD: u64 = 16 << 20;
    let cfg = ColumnFamilyConfig {
        soft_pending_compaction_bytes: 2 << 20,
        hard_pending_compaction_bytes: HARD,
        ..base.clone()
    };
    let cf = db.create_column_family("bench", cfg).unwrap();

    // Sample as we go: the end-of-run value alone could miss an excursion.
    let mut peak = 0u64;
    for chunk in 0..12 {
        put_range(&db, &cf, chunk * 5_000, (chunk + 1) * 5_000, &[b'v'; 100]);
        peak = peak.max(cf.stats().compaction_debt);
    }

    let in_flight = base.l0_queue_stall_threshold as u64 * base.write_buffer_size as u64;
    let bound = HARD + in_flight;
    assert!(
        peak <= bound,
        "peak debt {peak} exceeded {bound} (ceiling {HARD} + {in_flight} in-flight); \
         pacing is not bounding the backlog"
    );
    db.close().unwrap();
}

/// Closing must not drain the compaction queue by default. Before 0.8.0 the
/// worker only checked its stop flag when the queue ran dry, so close paid off
/// the entire backlog — 35 seconds after a 20M-record ingest.
#[test]
fn close_does_not_drain_compaction_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = Options::new(dir.path().to_str().unwrap());
    o.finish_compactions_on_close = false;
    let db = DB::open(o).unwrap();
    let cf = db.create_column_family("bench", small_geometry()).unwrap();

    put_range(&db, &cf, 0, 40_000, &[b'v'; 100]);

    let t = Instant::now();
    db.close().unwrap();
    let elapsed = t.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "close took {elapsed:?}: it appears to have drained the compaction queue"
    );

    // Whatever debt was abandoned is legal state: the data must all reopen.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("bench").expect("cf after reopen");
    for k in (0..40_000).step_by(97) {
        assert!(
            db.get(&cf, format!("key{k:012}").as_bytes()).is_ok(),
            "key {k} lost across a close that abandoned compaction"
        );
    }
    db.close().unwrap();
}

/// The option is wired up in both directions — it was declared but read by
/// nothing before 0.8.0, so a test that only covers the default would not have
/// noticed.
#[test]
fn close_finishes_compactions_when_asked() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = Options::new(dir.path().to_str().unwrap());
    o.finish_compactions_on_close = true;
    let db = DB::open(o).unwrap();
    let cf = db.create_column_family("bench", small_geometry()).unwrap();

    put_range(&db, &cf, 0, 20_000, &[b'v'; 100]);
    db.close().unwrap();

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("bench").expect("cf after reopen");
    for k in (0..20_000).step_by(53) {
        assert!(db.get(&cf, format!("key{k:012}").as_bytes()).is_ok());
    }
    db.close().unwrap();
}

/// Compaction and the parts/tiers operations exclude each other through the
/// range locks now, not through `compact_mu`. Writing hard while repeatedly
/// compacting must not lose data or deadlock.
#[test]
fn concurrent_writes_and_manual_compaction_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(DB::open(Options::new(dir.path().to_str().unwrap())).unwrap());
    let cf = db.create_column_family("bench", small_geometry()).unwrap();

    const N: usize = 20_000;
    let stop = Arc::new(AtomicBool::new(false));

    std::thread::scope(|s| {
        let db2 = Arc::clone(&db);
        let cf2 = Arc::clone(&cf);
        let stop2 = Arc::clone(&stop);
        s.spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                let _ = db2.compact(&cf2);
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        put_range(&db, &cf, 0, N, &[b'v'; 100]);
        stop.store(true, Ordering::Relaxed);
    });

    db.flush_memtable(&cf).unwrap();
    for k in 0..N {
        assert!(
            db.get(&cf, format!("key{k:012}").as_bytes()).is_ok(),
            "key {k} lost to a concurrent compaction"
        );
    }
    Arc::try_unwrap(db).ok().unwrap().close().unwrap();
}
