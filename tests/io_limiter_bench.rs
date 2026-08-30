//! 0.6 acceptance harness: foreground read latency during a forced compaction,
//! unlimited versus limited.
//!
//! Ignored by default — it is a measurement, not an assertion, and it takes
//! minutes. Run it with:
//!
//! ```sh
//! cargo test --release --test io_limiter_bench -- --ignored --nocapture
//! ```
//!
//! It writes one JSONL record per phase per run to `ONDADB_BENCH_OUT` (default
//! `bench-results/0.6/latest.jsonl`). Charged bytes are published alongside
//! fsync latency on purpose: charged bytes are a *proxy* for device pressure,
//! not a measurement of it, and the fsync number is what keeps the proxy
//! honest.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::ioctrl::{IoClass, IoLimiter, TokenBucket};
use ondadb::{ColumnFamilyConfig, Options, DB};

/// Records what it admits, then delegates. Wraps either a real token bucket or
/// nothing, so both arms of the comparison report charged bytes identically.
#[derive(Debug)]
struct Meter {
    inner: Option<TokenBucket>,
    background_bytes: AtomicU64,
    foreground_bytes: AtomicU64,
}

impl Meter {
    fn new(rate: u64) -> Arc<Meter> {
        Arc::new(Meter {
            inner: (rate > 0).then(|| TokenBucket::new(rate, rate)),
            background_bytes: AtomicU64::new(0),
            foreground_bytes: AtomicU64::new(0),
        })
    }
}

impl IoLimiter for Meter {
    fn charge(&self, class: IoClass, bytes: u64) {
        if class == IoClass::Foreground {
            self.foreground_bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.background_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        if let Some(b) = &self.inner {
            b.charge(class, bytes);
        }
    }
    fn cancel(&self) {
        if let Some(b) = &self.inner {
            b.cancel();
        }
    }
}

const KEYS: u32 = 40_000;
const GENERATIONS: u32 = 5;
const VALUE_LEN: usize = 400;
/// Long enough that a run's p99 is not one unlucky sample.
const READ_SECONDS: u64 = 6;

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

struct Phase {
    name: &'static str,
    reads: usize,
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
    max_us: u64,
    fsync_p99_us: u64,
    background_bytes: u64,
    foreground_bytes: u64,
    compaction_secs: f64,
}

/// Build a multi-level CF with enough real data that a sweep has work to do.
fn build(db: &DB) -> Arc<ondadb::ColumnFamily> {
    let cf = db
        .create_column_family(
            "bench",
            ColumnFamilyConfig {
                l1_file_count_trigger: 4,
                write_buffer_size: 4 << 20,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let value = vec![b'v'; VALUE_LEN];
    for generation in 0..GENERATIONS {
        for i in 0..KEYS {
            db.put(
                &cf,
                format!("key{i:08}").as_bytes(),
                &[&generation.to_le_bytes()[..], &value[..]].concat(),
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
    }
    cf
}

/// Read `key` in a loop for `READ_SECONDS`, optionally with a compaction
/// running concurrently, and return the latency distribution.
fn measure(name: &'static str, dir: &std::path::Path, rate: u64, compact: bool) -> Phase {
    let meter = Meter::new(rate);
    let limiter: Arc<dyn IoLimiter> = meter.clone();
    let mut options = Options::new(dir.to_str().unwrap());
    options.io_limiter = Some(limiter);
    // A small block cache so foreground reads keep reaching the device rather
    // than answering everything out of RAM — otherwise the phase measures the
    // cache, not the contention the feature is about.
    options.block_cache_size = 4 << 20;
    let db = DB::open(options).unwrap();
    let cf = build(&db);
    // Settle whatever the flushes armed, so the phase measures only what this
    // phase starts.
    std::thread::sleep(Duration::from_secs(2));
    meter.background_bytes.store(0, Ordering::Relaxed);
    meter.foreground_bytes.store(0, Ordering::Relaxed);

    let stop = Arc::new(AtomicBool::new(false));
    let compaction_secs = Arc::new(parking_lot_lite::Cell::new(0.0));

    let (reads, latencies, fsyncs) = std::thread::scope(|scope| {
        let compactor = compact.then(|| {
            let db = &db;
            let cf = cf.clone();
            let stop = stop.clone();
            let compaction_secs = compaction_secs.clone();
            scope.spawn(move || {
                let t0 = Instant::now();
                // Keep the device busy for the whole read window.
                while !stop.load(Ordering::SeqCst) {
                    db.compact(&cf).unwrap();
                }
                compaction_secs.set(t0.elapsed().as_secs_f64());
            })
        });

        let mut latencies: Vec<u64> = Vec::with_capacity(1 << 20);
        let mut fsyncs: Vec<u64> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(READ_SECONDS);
        let mut i = 0u32;
        let mut next_fsync = Instant::now();
        while Instant::now() < deadline {
            let key = format!("key{:08}", (i.wrapping_mul(2_654_435_761)) % KEYS);
            let t0 = Instant::now();
            let _ = db.get(&cf, key.as_bytes());
            latencies.push(t0.elapsed().as_micros() as u64);
            i = i.wrapping_add(1);
            // Sample fsync latency alongside: charged bytes are a proxy for
            // device pressure, this is a measurement of it.
            if Instant::now() >= next_fsync {
                let t0 = Instant::now();
                db.sync_wal().unwrap();
                fsyncs.push(t0.elapsed().as_micros() as u64);
                next_fsync = Instant::now() + Duration::from_millis(100);
            }
        }
        stop.store(true, Ordering::SeqCst);
        if let Some(c) = compactor {
            c.join().unwrap();
        }
        (latencies.len(), latencies, fsyncs)
    });

    let mut sorted = latencies;
    sorted.sort_unstable();
    let mut fs = fsyncs;
    fs.sort_unstable();
    let phase = Phase {
        name,
        reads,
        p50_us: percentile(&sorted, 0.50),
        p99_us: percentile(&sorted, 0.99),
        p999_us: percentile(&sorted, 0.999),
        max_us: sorted.last().copied().unwrap_or(0),
        fsync_p99_us: percentile(&fs, 0.99),
        background_bytes: meter.background_bytes.load(Ordering::Relaxed),
        foreground_bytes: meter.foreground_bytes.load(Ordering::Relaxed),
        compaction_secs: compaction_secs.get(),
    };
    db.close().unwrap();
    phase
}

/// A `Cell<f64>` shareable across a scoped thread without pulling in a dep.
mod parking_lot_lite {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[derive(Debug, Default)]
    pub struct Cell(AtomicU64);

    impl Cell {
        pub fn new(v: f64) -> Arc<Cell> {
            Arc::new(Cell(AtomicU64::new(v.to_bits())))
        }
        pub fn set(&self, v: f64) {
            self.0.store(v.to_bits(), Ordering::SeqCst);
        }
        pub fn get(&self) -> f64 {
            f64::from_bits(self.0.load(Ordering::SeqCst))
        }
    }
}

#[test]
#[ignore = "measurement harness; run explicitly with --ignored"]
fn foreground_reads_during_forced_compaction() {
    let runs: usize = std::env::var("RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let rate: u64 = std::env::var("ONDADB_BENCH_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16 << 20); // 16 MiB/s of background bandwidth
    let out = std::env::var("ONDADB_BENCH_OUT")
        .unwrap_or_else(|_| "bench-results/0.6/latest.jsonl".to_string());
    if let Some(parent) = std::path::Path::new(&out).parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut sink = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out)
        .unwrap();
    use std::io::Write;

    for run in 0..runs {
        for (name, phase_rate, compact) in [
            ("baseline_no_compaction", 0u64, false),
            ("compaction_unlimited", 0, true),
            ("compaction_limited", rate, true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let p = measure(name, dir.path(), phase_rate, compact);
            let line = format!(
                r#"{{"run":{run},"phase":"{}","rate_bytes_per_sec":{phase_rate},"reads":{},"p50_us":{},"p99_us":{},"p999_us":{},"max_us":{},"fsync_p99_us":{},"background_bytes":{},"foreground_bytes":{},"compaction_secs":{:.3}}}"#,
                p.name,
                p.reads,
                p.p50_us,
                p.p99_us,
                p.p999_us,
                p.max_us,
                p.fsync_p99_us,
                p.background_bytes,
                p.foreground_bytes,
                p.compaction_secs,
            );
            println!("{line}");
            writeln!(sink, "{line}").unwrap();
            sink.flush().unwrap();
        }
    }
}
