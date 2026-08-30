//! The `range-delete` acceptance harness for 1.2.
//!
//! `#[ignore]`d, like every other benchmark in this repo: it is run by hand and
//! its output is committed under `bench-results/1.2/`. Run with
//!
//! ```sh
//! cargo test --release --features unsafe-fastpath --test range_delete_bench \
//!   -- --ignored --nocapture
//! ```
//!
//! Four measurements, matching the feature's acceptance list:
//!
//! 1. **Coverage** — bytes read and rewritten by a compaction whose input is
//!    fully covered by one range delete, against the same workload deleted key
//!    by key. This is the categorical claim the feature is for.
//! 2. **Delete latency** p50/p99, with and without concurrent point writers —
//!    the baseline for deferred review item **M3** (`commit_mu` latency), which
//!    a range commit makes measurably worse by taking the lock at every
//!    isolation level.
//! 3. **Read p99 vs fragment count** — point and scan latency against tables
//!    carrying 0, 10 and 1000 fragments. The 0-fragment column is the
//!    regression gate: a database that never uses the feature must not pay for
//!    it.
//! 4. **Time to space reclaim** — how long a bulk delete takes to actually free
//!    the bytes.
//! 5. **Excise vs compaction** (slices 10-12) — the same fully-covering bulk
//!    delete reclaimed by catalog edit and by rewrite, wall clock and bytes.
//!
//! This machine is thermally noisy (±15–20% run to run; see
//! `docs/performance.md`), so every figure below is the median of `RUNS`
//! repetitions and only same-run ratios are interpreted.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::format::CAP_RANGE_DELETES;
use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, DB};

const RUNS: usize = 5;
const KEYS: u32 = 60_000;
const VALUE_LEN: usize = 100;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

fn open(dir: &std::path::Path, ranges: bool) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.num_compaction_threads = 2;
    let cfg = ColumnFamilyConfig {
        target_file_size: 2 << 20,
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();
    let cf = db.create_column_family("default", cfg).unwrap();
    if ranges {
        db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    }
    (db, cf)
}

fn fill(db: &DB, cf: &Arc<ColumnFamily>, n: u32) {
    let value = vec![b'v'; VALUE_LEN];
    for i in 0..n {
        db.put(cf, &key(i), &value, Duration::ZERO).unwrap();
    }
    db.flush_memtable(cf).unwrap();
}

/// Bytes of SSTable currently catalogued for `cf`.
fn resident_bytes(cf: &Arc<ColumnFamily>) -> u64 {
    cf.table_metadata()
        .into_iter()
        .flatten()
        .map(|m| m.klog_size + m.vlog_size)
        .sum()
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn median_dur(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

// ---- 1. coverage: bytes read and rewritten ---------------------------------

/// Delete every key of a populated family two ways and compare what compaction
/// has to move.
#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn range_delete_coverage_bytes() {
    println!("\n== 1. bulk delete: bytes to reclaim ==");
    println!("(60k keys, 100-byte values; median of {RUNS} runs)");

    let mut point_ms = Vec::new();
    let mut range_ms = Vec::new();
    let mut point_after = Vec::new();
    let mut range_after = Vec::new();
    let mut point_records = Vec::new();
    let mut range_records = Vec::new();

    for _ in 0..RUNS {
        // --- key-by-key deletes (today's only option) ---
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), false);
        fill(&db, &cf, KEYS);
        let before = resident_bytes(&cf);
        let t0 = Instant::now();
        for i in 0..KEYS {
            db.delete(&cf, &key(i)).unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        let written = resident_bytes(&cf) - before;
        db.compact(&cf).unwrap();
        point_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        point_after.push(resident_bytes(&cf));
        point_records.push(written);
        db.close().unwrap();

        // --- one range delete ---
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), true);
        fill(&db, &cf, KEYS);
        let before = resident_bytes(&cf);
        let t0 = Instant::now();
        db.delete_range(&cf, &key(0), b"l").unwrap();
        db.flush_memtable(&cf).unwrap();
        let written = resident_bytes(&cf) - before;
        db.compact(&cf).unwrap();
        range_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        range_after.push(resident_bytes(&cf));
        range_records.push(written);
        db.close().unwrap();
    }

    let pm = median(point_ms);
    let rm = median(range_ms);
    let pw = median(point_records.iter().map(|b| *b as f64).collect());
    let rw = median(range_records.iter().map(|b| *b as f64).collect());
    println!(
        "  delete + flush + compact   point {pm:>9.1} ms   range {rm:>9.1} ms   ({:.1}x)",
        pm / rm.max(1e-9)
    );
    println!(
        "  bytes the delete WROTE     point {pw:>9.0} B    range {rw:>9.0} B    ({:.0}x)",
        pw / rw.max(1.0)
    );
    println!(
        "  bytes resident afterwards  point {:>9.0} B    range {:>9.0} B",
        median(point_after.iter().map(|b| *b as f64).collect()),
        median(range_after.iter().map(|b| *b as f64).collect()),
    );
}

// ---- 2. delete latency, with and without concurrent point writers ----------

#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn range_delete_commit_latency() {
    println!("\n== 2. delete-commit latency (the RV-M3 baseline) ==");
    println!("  200 commits each; median of {RUNS} runs");

    for writers in [0usize, 4] {
        let mut p50 = Vec::new();
        let mut p99 = Vec::new();
        let mut point_p99 = Vec::new();
        for _ in 0..RUNS {
            let dir = tempfile::tempdir().unwrap();
            let (db, cf) = open(dir.path(), true);
            fill(&db, &cf, 20_000);
            let db = Arc::new(db);

            let stop = Arc::new(AtomicBool::new(false));
            let mut handles = Vec::new();
            for w in 0..writers {
                let (d, c, s) = (db.clone(), cf.clone(), stop.clone());
                handles.push(std::thread::spawn(move || {
                    let value = vec![b'v'; VALUE_LEN];
                    let mut i = 0u32;
                    while !s.load(Ordering::Relaxed) {
                        let k = format!("w{w}-{i:08}");
                        let _ = d.put(&c, k.as_bytes(), &value, Duration::ZERO);
                        i += 1;
                    }
                }));
            }

            let mut range_lat = Vec::new();
            for i in 0..200u32 {
                let lo = format!("r{i:06}-a");
                let hi = format!("r{i:06}-z");
                let t = Instant::now();
                db.delete_range(&cf, lo.as_bytes(), hi.as_bytes()).unwrap();
                range_lat.push(t.elapsed());
            }
            // The same number of ordinary point commits, for scale.
            let mut point_lat = Vec::new();
            let value = vec![b'v'; VALUE_LEN];
            for i in 0..200u32 {
                let k = format!("p{i:08}");
                let t = Instant::now();
                db.put(&cf, k.as_bytes(), &value, Duration::ZERO).unwrap();
                point_lat.push(t.elapsed());
            }
            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }
            range_lat.sort();
            point_lat.sort();
            p50.push(percentile(&range_lat, 0.50));
            p99.push(percentile(&range_lat, 0.99));
            point_p99.push(percentile(&point_lat, 0.99));
            Arc::try_unwrap(db).unwrap().close().unwrap();
        }
        println!(
            "  {writers} concurrent point writers: range p50 {:>8.1} us  p99 {:>8.1} us   (point commit p99 {:>8.1} us)",
            median_dur(p50).as_secs_f64() * 1e6,
            median_dur(p99).as_secs_f64() * 1e6,
            median_dur(point_p99).as_secs_f64() * 1e6,
        );
    }
}

// ---- 3. read latency vs fragment count -------------------------------------

#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn range_delete_read_latency_by_fragment_count() {
    println!("\n== 3. read p99 vs published fragment count ==");
    println!("  40k keys; the deleted spans are disjoint and narrow, so the");
    println!("  surviving key set is the same in every column; median of {RUNS} runs");

    for fragments in [0usize, 10, 1000] {
        let mut get_p99 = Vec::new();
        let mut scan_ms = Vec::new();
        for _ in 0..RUNS {
            let dir = tempfile::tempdir().unwrap();
            let (db, cf) = open(dir.path(), fragments > 0);
            fill(&db, &cf, 40_000);
            // Spans over a keyspace the data does not use, so the answer set is
            // identical across columns and only the fragment count varies.
            for f in 0..fragments {
                let lo = format!("z{f:06}-a");
                let hi = format!("z{f:06}-z");
                db.delete_range(&cf, lo.as_bytes(), hi.as_bytes()).unwrap();
            }
            db.flush_memtable(&cf).unwrap();
            let published: u64 = cf
                .table_metadata()
                .into_iter()
                .flatten()
                .map(|m| m.range_count)
                .sum();
            assert!(
                published >= fragments as u64 || fragments == 0,
                "expected {fragments} fragments, published {published}"
            );

            let mut lat = Vec::with_capacity(20_000);
            for i in 0..20_000u32 {
                let k = key(i * 2);
                let t = Instant::now();
                let _ = db.get(&cf, &k);
                lat.push(t.elapsed());
            }
            lat.sort();
            get_p99.push(percentile(&lat, 0.99));

            let t = Instant::now();
            let txn = db.begin();
            let mut it = txn.new_iterator(&cf);
            it.seek_to_first();
            let mut n = 0u64;
            while it.valid() {
                n += 1;
                it.next();
            }
            assert_eq!(n, 40_000, "the surviving key set must not vary");
            drop(it);
            drop(txn);
            scan_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            db.close().unwrap();
        }
        println!(
            "  {fragments:>4} fragments:  get p99 {:>7.2} us   full scan {:>8.1} ms",
            median_dur(get_p99).as_secs_f64() * 1e6,
            median(scan_ms),
        );
    }
}

// ---- 4. time to space reclaim ----------------------------------------------

#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn range_delete_time_to_space_reclaim() {
    println!("\n== 4. time to space reclaim ==");
    let mut point = Vec::new();
    let mut range = Vec::new();
    for _ in 0..RUNS {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), false);
        fill(&db, &cf, KEYS);
        let t = Instant::now();
        for i in 0..KEYS {
            db.delete(&cf, &key(i)).unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        point.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(resident_bytes(&cf), 0, "point deletes reclaimed everything");
        db.close().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), true);
        fill(&db, &cf, KEYS);
        let t = Instant::now();
        db.delete_range(&cf, &key(0), b"l").unwrap();
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        range.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(
            resident_bytes(&cf),
            0,
            "the range delete reclaimed everything"
        );
        db.close().unwrap();
    }
    println!(
        "  delete -> zero resident bytes:  point {:>9.1} ms   range {:>9.1} ms",
        median(point),
        median(range),
    );
}

// ---- 5. time to space reclaim: excise vs compaction (1.2 slices 10-12) ------

/// The same fully-covering bulk delete reclaimed two ways.
///
/// **A (excise)** publishes the tombstone and then retires every fully shadowed
/// table by catalog edit — no block is read, no byte is rewritten, and the work
/// is one `RemoveTables` record plus N unlinks.
///
/// **B (compaction)** is the pre-1.2 path: the same tombstone, reclaimed by
/// `DB::compact`, which merge-reads every input and writes the survivors.
/// `run_manual` carries no excise pre-pass, and while it holds the whole-keyspace
/// range lock a background pre-pass is vetoed — so B measures compaction. If the
/// background worker nonetheless wins the race between the flush and the
/// `compact` call, it makes B *faster*, which understates A's margin rather than
/// inflating it.
///
/// A leaves the fragment owner behind by design (dropping it would destroy the
/// evidence), so the arms are compared on **covered bytes reclaimed per
/// millisecond**, not on reaching zero.
#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn range_delete_excise_vs_compaction_reclaim() {
    println!("\n== 5. time to space reclaim: excise vs compaction ==");
    let mut excise_ms = Vec::new();
    let mut compact_ms = Vec::new();
    let mut excise_bytes = Vec::new();
    let mut compact_bytes = Vec::new();

    for _ in 0..RUNS {
        // A: excise.
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), true);
        fill(&db, &cf, KEYS);
        let before = resident_bytes(&cf);
        let t = Instant::now();
        db.delete_range(&cf, &key(0), b"l").unwrap();
        db.flush_memtable(&cf).unwrap();
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let n = db.excise_covered(&cf).unwrap();
            let left = cf
                .table_metadata()
                .into_iter()
                .flatten()
                .filter(|m| m.range_count == 0)
                .count();
            if left == 0 {
                break;
            }
            assert!(Instant::now() < deadline, "excise never converged");
            if n == 0 {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        excise_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        excise_bytes.push((before - resident_bytes(&cf)) as f64);
        db.close().unwrap();

        // B: compaction.
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), true);
        fill(&db, &cf, KEYS);
        let before = resident_bytes(&cf);
        let t = Instant::now();
        db.delete_range(&cf, &key(0), b"l").unwrap();
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        compact_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        compact_bytes.push((before - resident_bytes(&cf)) as f64);
        db.close().unwrap();
    }

    let (a_ms, b_ms) = (median(excise_ms), median(compact_ms));
    let (a_by, b_by) = (median(excise_bytes), median(compact_bytes));
    println!(
        "  excise      {:>9.1} ms   reclaimed {:>12.0} B   {:>10.0} B/ms",
        a_ms,
        a_by,
        a_by / a_ms.max(f64::MIN_POSITIVE)
    );
    println!(
        "  compaction  {:>9.1} ms   reclaimed {:>12.0} B   {:>10.0} B/ms",
        b_ms,
        b_by,
        b_by / b_ms.max(f64::MIN_POSITIVE)
    );
    println!("  speedup (wall clock, same coverage): {:.1}x", b_ms / a_ms);
}
