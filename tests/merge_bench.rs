//! Feature 1.1 acceptance benchmark: merge operands versus the read-modify-write
//! they replace, chain length over time with folding on and off, and the
//! no-regression measurement for a column family that configures no operator.
//!
//! `#[ignore]`d — it is a measurement, not a gate. Run it with:
//!
//! ```sh
//! BENCH_DATE=$(date +%F) cargo test --release --features unsafe-fastpath \
//!     --test merge_bench -- --ignored --nocapture
//! ```
//!
//! Results land in `bench-results/1.1/<date>/`. Per AGENTS.md this machine is
//! thermally noisy (±15-20 % run to run), so every phase is measured `RUNS`
//! times and every claim is a **same-run ratio**, never an absolute number
//! compared across sessions.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, Options, SyncMode, DB};

const RUNS: usize = 5;
/// Counter operations per phase. Small enough that five runs of every phase fit
/// in a few minutes, large enough to swamp per-call overhead.
const OPS: usize = 20_000;
/// Keys the counter workload spreads over: a hot-ish set, which is what a
/// counter or an HLL sketch actually looks like.
const COUNTER_KEYS: usize = 500;
/// Rows for the no-operator regression phases.
const SCAN_ROWS: usize = 200_000;

/// Little-endian i64 counter; operands are deltas.
#[derive(Debug)]
struct Counter;

impl MergeOperator for Counter {
    fn name(&self) -> &str {
        "bench.counter.i64.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let read = |b: &[u8]| -> Result<i64, String> {
            b.try_into()
                .map(i64::from_le_bytes)
                .map_err(|_| "short operand".to_string())
        };
        let mut acc = match existing {
            Some(b) => read(b)?,
            None => 0,
        };
        for operand in operands {
            acc = acc.wrapping_add(read(operand)?);
        }
        Ok(acc.to_le_bytes().to_vec())
    }
}

fn key_of(i: usize) -> Vec<u8> {
    format!("counter/{:08}", i % COUNTER_KEYS).into_bytes()
}

/// Every phase runs at the same durability: `SyncMode::None`, so the number
/// being compared is engine CPU plus the conflict window, not the fsync the
/// two paths would pay identically.
fn options(dir: &std::path::Path, operators: Vec<Arc<dyn MergeOperator>>) -> Options {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = operators;
    opts
}

fn cf_config(operator: Option<&str>) -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        merge_operator_name: operator.map(str::to_string),
        sync_mode: SyncMode::None,
        ..ColumnFamilyConfig::default()
    }
}

/// Operand entries still on disk for the counter keyspace — the "chain length"
/// the folding phases report.
fn operands_on_disk(dir: &std::path::Path) -> usize {
    use ondadb::cache::{BlockCache, FileCache};
    use ondadb::sst::Reader;
    use ondadb::LocalStorage;

    let mut klogs: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("cf-c"))
        .expect("the column family directory exists")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "klog"))
        .collect();
    klogs.sort();
    let mut operands = 0usize;
    for (i, klog) in klogs.iter().enumerate() {
        let reader = Reader::open(
            klog.to_str().unwrap(),
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            i as u64 + 1,
            ondadb::comparator::default_comparator(),
            0,
        )
        .unwrap();
        let mut it = reader.iter();
        it.seek_to_first();
        while it.valid() {
            if it.kind() == ondadb::format::KIND_MERGE {
                operands += 1;
            }
            it.next();
        }
    }
    operands
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn out_dir() -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("bench-results/1.1")
        .join(std::env::var("BENCH_DATE").unwrap_or_else(|_| "latest".into()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// Phase 1: counter workload — merge versus Get+Put at equal durability
// ---------------------------------------------------------------------------

/// One counter increment through `merge`: one append, no read, no conflict
/// window.
fn merge_chunk(db: &DB, cf: &Arc<ColumnFamily>, range: std::ops::Range<usize>) {
    let one = 1i64.to_le_bytes();
    for i in range {
        db.merge(cf, &key_of(i), &one).unwrap();
    }
}

/// The read-modify-write it replaces, at Snapshot isolation — which is what
/// makes a counter correct without merge, and what pays the conflict window.
fn rmw_chunk(db: &DB, cf: &Arc<ColumnFamily>, range: std::ops::Range<usize>) {
    for i in range {
        let key = key_of(i);
        loop {
            let mut t = db.begin();
            let current = match t.get(cf, &key) {
                Ok(v) => i64::from_le_bytes(v.try_into().unwrap()),
                Err(_) => 0,
            };
            t.put(cf, &key, &(current + 1).to_le_bytes(), Duration::ZERO)
                .unwrap();
            if t.commit().is_ok() {
                break;
            }
        }
    }
}

/// A plain `put` of the same shape: the floor both counter paths sit on.
fn put_chunk(db: &DB, cf: &Arc<ColumnFamily>, range: std::ops::Range<usize>) {
    let one = 1i64.to_le_bytes();
    for i in range {
        db.put(cf, &key_of(i), &one, Duration::ZERO).unwrap();
    }
}

/// Run the three counter paths **interleaved in small chunks**, each against
/// its own database, accumulating their times separately.
///
/// Interleaving is not cosmetic: this machine is shared and thermally noisy, so
/// three phases run back to back are three different machines. Alternating
/// chunks puts the same drift into all three accumulators, which is what makes
/// their ratio mean something when their absolute numbers do not.
fn interleaved_counter_run() -> (f64, f64, f64) {
    const CHUNK: usize = 500;
    let (dp, dm, dr) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let put_db = DB::open(Options::new(dp.path().to_str().unwrap())).unwrap();
    let put_cf = put_db.create_column_family("c", cf_config(None)).unwrap();
    let merge_db = DB::open(options(dm.path(), vec![Arc::new(Counter)])).unwrap();
    let merge_cf = merge_db
        .create_column_family("c", cf_config(Some(Counter.name())))
        .unwrap();
    let rmw_db = DB::open(Options::new(dr.path().to_str().unwrap())).unwrap();
    let rmw_cf = rmw_db.create_column_family("c", cf_config(None)).unwrap();

    let (mut put_ns, mut merge_ns, mut rmw_ns) = (0u128, 0u128, 0u128);
    let mut at = 0usize;
    while at < OPS {
        let range = at..(at + CHUNK).min(OPS);
        let t = Instant::now();
        put_chunk(&put_db, &put_cf, range.clone());
        put_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        merge_chunk(&merge_db, &merge_cf, range.clone());
        merge_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        rmw_chunk(&rmw_db, &rmw_cf, range.clone());
        rmw_ns += t.elapsed().as_nanos();
        at = range.end;
    }
    put_db.close().unwrap();
    merge_db.close().unwrap();
    rmw_db.close().unwrap();
    let n = OPS as f64;
    (put_ns as f64 / n, merge_ns as f64 / n, rmw_ns as f64 / n)
}

#[test]
#[ignore = "benchmark; run manually and record the output"]
fn counter_merge_versus_read_modify_write() {
    let mut csv = String::from("run,put_ns_per_op,merge_ns_per_op,get_put_ns_per_op,speedup\n");
    let mut speedups = Vec::new();
    for run in 0..RUNS {
        let (put_ns, merge_ns, rmw_ns) = interleaved_counter_run();
        let speedup = rmw_ns / merge_ns;
        speedups.push(speedup);
        csv.push_str(&format!(
            "{run},{put_ns:.1},{merge_ns:.1},{rmw_ns:.1},{speedup:.3}\n"
        ));
        println!(
            "run {run}: put {put_ns:.0} ns/op, merge {merge_ns:.0} ns/op, \
             get+put {rmw_ns:.0} ns/op, x{speedup:.2}"
        );
    }
    println!("median speedup x{:.2}", median(speedups.clone()));
    std::fs::write(out_dir().join("counter.csv"), csv).unwrap();
}

/// Phase 1b: the same counter under **contention**, which is where the feature's
/// claim actually lives.
///
/// A single-threaded read-modify-write never conflicts, so it only pays the
/// `get`. With N threads on a small key set it also pays retries, and the retry
/// count is the one number on this machine that is not a timing: it is a
/// property of the workload, not of how loaded the box is.
#[test]
#[ignore = "benchmark; run manually and record the output"]
fn contended_counter_merge_versus_read_modify_write() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 1_500;
    const HOT_KEYS: usize = 16;

    let hot = |i: usize| format!("hot/{:04}", i % HOT_KEYS).into_bytes();
    let mut csv = String::from("run,merge_ns_per_op,get_put_ns_per_op,get_put_retries,speedup\n");
    let mut speedups = Vec::new();
    for run in 0..RUNS {
        // Merge.
        let dm = tempfile::tempdir().unwrap();
        let merge_db = Arc::new(DB::open(options(dm.path(), vec![Arc::new(Counter)])).unwrap());
        let merge_cf = merge_db
            .create_column_family("c", cf_config(Some(Counter.name())))
            .unwrap();
        let start = Instant::now();
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let (db, cf) = (merge_db.clone(), merge_cf.clone());
                scope.spawn(move || {
                    let one = 1i64.to_le_bytes();
                    for i in 0..PER_THREAD {
                        db.merge(&cf, &hot(i), &one).unwrap();
                    }
                });
            }
        });
        let merge_ns = start.elapsed().as_nanos() as f64 / (THREADS * PER_THREAD) as f64;
        Arc::try_unwrap(merge_db).unwrap().close().unwrap();

        // Read-modify-write at Snapshot isolation.
        let dr = tempfile::tempdir().unwrap();
        let rmw_db = Arc::new(DB::open(Options::new(dr.path().to_str().unwrap())).unwrap());
        let rmw_cf = rmw_db.create_column_family("c", cf_config(None)).unwrap();
        let retries = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let start = Instant::now();
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let (db, cf, retries) = (rmw_db.clone(), rmw_cf.clone(), retries.clone());
                scope.spawn(move || {
                    for i in 0..PER_THREAD {
                        let key = hot(i);
                        loop {
                            let mut t = db.begin();
                            let current = match t.get(&cf, &key) {
                                Ok(v) => i64::from_le_bytes(v.try_into().unwrap()),
                                Err(_) => 0,
                            };
                            t.put(&cf, &key, &(current + 1).to_le_bytes(), Duration::ZERO)
                                .unwrap();
                            if t.commit().is_ok() {
                                break;
                            }
                            retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        let rmw_ns = start.elapsed().as_nanos() as f64 / (THREADS * PER_THREAD) as f64;
        let retried = retries.load(std::sync::atomic::Ordering::Relaxed);
        drop(rmw_cf);
        Arc::try_unwrap(rmw_db).unwrap().close().unwrap();

        let speedup = rmw_ns / merge_ns;
        speedups.push(speedup);
        csv.push_str(&format!(
            "{run},{merge_ns:.1},{rmw_ns:.1},{retried},{speedup:.3}\n"
        ));
        println!(
            "run {run}: merge {merge_ns:.0} ns/op, get+put {rmw_ns:.0} ns/op \
             ({retried} retries), x{speedup:.2}"
        );
    }
    println!("median speedup x{:.2}", median(speedups.clone()));
    std::fs::write(out_dir().join("counter-contended.csv"), csv).unwrap();
}

// ---------------------------------------------------------------------------
// Phase 2: chain length over time, folding on and off
// ---------------------------------------------------------------------------

fn chain_length_series(dir: &std::path::Path, folding: bool) -> Vec<(usize, usize, f64)> {
    let mut opts = options(dir, vec![Arc::new(Counter)]);
    opts.enable_merge_folding = folding;
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family("c", cf_config(Some(Counter.name())))
        .unwrap();
    let one = 1i64.to_le_bytes();
    let mut series = Vec::new();
    for round in 1..=8usize {
        for i in 0..OPS {
            db.merge(&cf, &key_of(i), &one).unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        let operands = operands_on_disk(dir);
        // Point-read cost at this chain length: the thing chain growth actually
        // costs a reader.
        let start = Instant::now();
        for i in 0..COUNTER_KEYS {
            std::hint::black_box(db.get(&cf, &key_of(i)).unwrap());
        }
        let read_ns = start.elapsed().as_nanos() as f64 / COUNTER_KEYS as f64;
        series.push((round, operands, read_ns));
    }
    db.close().unwrap();
    series
}

#[test]
#[ignore = "benchmark; run manually and record the output"]
fn chain_length_with_folding_on_and_off() {
    let mut csv = String::from("folding,round,operands_on_disk,point_read_ns\n");
    for folding in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        for (round, operands, read_ns) in chain_length_series(dir.path(), folding) {
            csv.push_str(&format!("{folding},{round},{operands},{read_ns:.1}\n"));
            println!("folding={folding} round={round} operands={operands} read={read_ns:.0} ns");
        }
    }
    std::fs::write(out_dir().join("chain-length.csv"), csv).unwrap();
}

// ---------------------------------------------------------------------------
// Phase 3: the gate — no measurable regression for a family with NO operator
// ---------------------------------------------------------------------------

/// Fill a family with `SCAN_ROWS` puts and flush, then measure a full scan and
/// a point-read sweep. `operator` selects the family shape being measured; the
/// *workload* is identical in both, so the ratio isolates 1.1's cost.
fn no_operator_phase(dir: &std::path::Path, operator: Option<&str>) -> (f64, f64) {
    let db = DB::open(options(dir, vec![Arc::new(Counter)])).unwrap();
    let cf = db.create_column_family("c", cf_config(operator)).unwrap();
    let value = vec![b'v'; 48];
    for i in 0..SCAN_ROWS {
        db.put(&cf, format!("row/{i:010}").as_bytes(), &value, Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    scan_and_point_read(&db, &cf)
}

fn scan_and_point_read(db: &DB, cf: &Arc<ColumnFamily>) -> (f64, f64) {
    // Warm the caches so the numbers are decode CPU, not I/O.
    for _ in 0..2 {
        let t = db.begin();
        let mut it = t.new_iterator(cf);
        it.seek_to_first();
        while it.valid() {
            std::hint::black_box(it.value());
            it.next();
        }
    }

    let t = db.begin();
    let mut it = t.new_iterator(cf);
    let start = Instant::now();
    let mut entries = 0u64;
    it.seek_to_first();
    while it.valid() {
        std::hint::black_box(it.key());
        std::hint::black_box(it.value());
        entries += 1;
        it.next();
    }
    let scan_ns = start.elapsed().as_nanos() as f64 / entries.max(1) as f64;

    const PROBES: usize = 20_000;
    let start = Instant::now();
    for i in 0..PROBES {
        let k = format!("row/{:010}", (i * 7919) % SCAN_ROWS);
        std::hint::black_box(db.get(cf, k.as_bytes()).unwrap());
    }
    let point_ns = start.elapsed().as_nanos() as f64 / PROBES as f64;
    (scan_ns, point_ns)
}

#[test]
#[ignore = "benchmark; run manually and record the output"]
fn no_operator_family_shows_no_regression() {
    let mut csv = String::from(
        "run,no_operator_scan_ns,no_operator_point_ns,\
         operator_scan_ns,operator_point_ns,scan_ratio,point_ratio\n",
    );
    let (mut scan_ratios, mut point_ratios) = (Vec::new(), Vec::new());
    for run in 0..RUNS {
        // Same-run pairs only: the two families are built and measured back to
        // back so a thermal drift hits both halves of the ratio.
        let a = tempfile::tempdir().unwrap();
        let (plain_scan, plain_point) = no_operator_phase(a.path(), None);
        let b = tempfile::tempdir().unwrap();
        let (op_scan, op_point) = no_operator_phase(b.path(), Some(Counter.name()));
        let scan_ratio = op_scan / plain_scan;
        let point_ratio = op_point / plain_point;
        scan_ratios.push(scan_ratio);
        point_ratios.push(point_ratio);
        csv.push_str(&format!(
            "{run},{plain_scan:.2},{plain_point:.1},{op_scan:.2},{op_point:.1},\
             {scan_ratio:.4},{point_ratio:.4}\n"
        ));
        println!(
            "run {run}: scan {plain_scan:.1} -> {op_scan:.1} ns/entry (x{scan_ratio:.3}), \
             point {plain_point:.0} -> {op_point:.0} ns (x{point_ratio:.3})"
        );
    }
    println!(
        "median scan ratio x{:.3}, median point ratio x{:.3}",
        median(scan_ratios),
        median(point_ratios)
    );
    std::fs::write(out_dir().join("no-operator.csv"), csv).unwrap();
}
