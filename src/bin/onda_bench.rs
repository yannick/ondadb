//! Standalone ondaDB benchmark, .
//!  16-byte keys, 100-byte values, random keys, 8
//! worker threads by default; Put / cold Get / Delete run as batched
//! transactions (1000 ops/txn) partitioned across threads, Forward/Backward
//! scans run `threads` concurrent full iterations.
//!
//! Output lines match the Go bench format exactly (`<phase> … <ops/sec> ops/sec`)
//! so `bench/bench_graphs.sh`'s `parse_go` awk handles them verbatim with the
//! engine label `ondadb`.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamilyConfig, Compression, IsolationLevel, Options, DB};

struct Args {
    ops: usize,
    key_size: usize,
    value_size: usize,
    threads: usize,
    pattern: String,
    compression: String,
    batch: usize,
    db_path: String,
    keep: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        ops: 1_000_000,
        key_size: 16,
        value_size: 100,
        threads: 8,
        pattern: "random".into(),
        compression: "none".into(),
        batch: 1000,
        db_path: String::new(),
        keep: false,
    };
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        let flag = argv[i].as_str();
        let mut val = || {
            i += 1;
            argv.get(i).cloned().unwrap_or_default()
        };
        match flag {
            "-ops" => a.ops = val().parse().unwrap_or(a.ops),
            "-key_size" => a.key_size = val().parse().unwrap_or(a.key_size),
            "-value_size" => a.value_size = val().parse().unwrap_or(a.value_size),
            "-threads" => a.threads = val().parse().unwrap_or(a.threads),
            "-pattern" => a.pattern = val(),
            "-compression" => a.compression = val(),
            "-batch" => a.batch = val().parse().unwrap_or(a.batch),
            "-db" => a.db_path = val(),
            "-keep" => a.keep = true,
            "-engine" => {
                let _ = val(); // accepted for CLI compatibility
            }
            _ => {}
        }
        i += 1;
    }
    a.threads = a.threads.max(1);
    a.batch = a.batch.max(1);
    a
}

fn gen_keys(a: &Args) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(a.ops);
    // Simple deterministic xorshift PRNG (no external rng needed in the binary).
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for idx in 0..a.ops {
        let mut k = vec![0u8; a.key_size];
        if a.pattern == "sequential" {
            let be = (idx as u64).to_be_bytes();
            let n = be.len().min(a.key_size);
            k[..n].copy_from_slice(&be[..n]);
        } else {
            for b in k.iter_mut() {
                *b = next() as u8;
            }
        }
        keys.push(k);
    }
    keys
}

fn compression(name: &str) -> Compression {
    Compression::parse(name).unwrap_or(Compression::None)
}

/// Run `n` items across `threads`, calling `f(lo, hi)` per partition.
fn run_threaded<F>(n: usize, threads: usize, f: F)
where
    F: Fn(usize, usize) + Sync,
{
    let per = n.div_ceil(threads);
    thread::scope(|s| {
        for t in 0..threads {
            let lo = t * per;
            let hi = ((t + 1) * per).min(n);
            if lo >= hi {
                continue;
            }
            let f = &f;
            s.spawn(move || f(lo, hi));
        }
    });
}

fn report(phase: &str, ops: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64().max(1e-9);
    let ops_per_sec = ops as f64 / secs;
    println!(
        "{:<28} {} ops    {:.2} ms    {:.0} ops/sec",
        phase,
        ops,
        elapsed.as_secs_f64() * 1000.0,
        ops_per_sec
    );
}

fn main() {
    let a = parse_args();
    let db_path = if a.db_path.is_empty() {
        "ondadb_bench_data".to_string()
    } else {
        a.db_path.clone()
    };
    let _ = std::fs::remove_dir_all(&db_path);

    eprintln!(
        "ondadb benchmark: ops={} threads={} key={} value={} pattern={} batch={} compression={}",
        a.ops, a.threads, a.key_size, a.value_size, a.pattern, a.batch, a.compression
    );

    let keys = gen_keys(&a);
    let value = vec![b'v'; a.value_size];

    let cfg = ColumnFamilyConfig {
        compression: compression(&a.compression),
        ..ColumnFamilyConfig::default()
    };

    // ---- Put -----------------------------------------------------------------
    let db = Arc::new(DB::open(Options::new(&db_path)).expect("open"));
    let cf = db
        .create_column_family("bench", cfg.clone())
        .expect("create cf");
    let start = Instant::now();
    {
        let db = &db;
        let cf = &cf;
        let keys = &keys;
        let value = &value;
        run_threaded(a.ops, a.threads, |lo, hi| {
            let mut i = lo;
            while i < hi {
                let be = (i + a.batch).min(hi);
                let mut txn = db.begin_with_isolation(IsolationLevel::ReadCommitted);
                for key in &keys[i..be] {
                    txn.put(cf, key, value, Duration::ZERO).unwrap();
                }
                txn.commit().unwrap();
                i = be;
            }
        });
    }
    report("Put", a.ops, start.elapsed());

    // Close and reopen so cold Get reads from SSTables.
    db.close().expect("close");
    drop(cf);
    drop(db);

    let db = Arc::new(DB::open(Options::new(&db_path)).expect("reopen"));
    let cf = db.get_column_family("bench").expect("cf after reopen");

    // ---- Get (cold) ----------------------------------------------------------
    let start = Instant::now();
    {
        let db = &db;
        let cf = &cf;
        let keys = &keys;
        let vsize = a.value_size;
        run_threaded(a.ops, a.threads, |lo, hi| {
            for k in &keys[lo..hi] {
                match db.get(cf, k) {
                    Ok(v) if v.len() == vsize => {}
                    _ => { /* count silently; random keys may collide/miss */ }
                }
            }
        });
    }
    report("Get (cold)", a.ops, start.elapsed());

    // ---- Forward / Backward scan (threads concurrent full iterations) --------
    // Run BEFORE Delete and straight after the reopen, matching the Go/C harness
    // phase order — so scans read from SSTables (the on-disk path), not a hot
    // memtable.
    let scan = |reverse: bool| -> Duration {
        let start = Instant::now();
        {
            let db = &db;
            let cf = &cf;
            thread::scope(|s| {
                for _ in 0..a.threads {
                    s.spawn(move || {
                        let mut txn = db.begin();
                        let mut it = txn.new_iterator(cf);
                        let mut count = 0u64;
                        if reverse {
                            it.seek_to_last();
                            while it.valid() {
                                count += 1;
                                it.prev();
                            }
                        } else {
                            it.seek_to_first();
                            while it.valid() {
                                count += 1;
                                it.next();
                            }
                        }
                        std::hint::black_box(count);
                        let _ = txn.rollback();
                    });
                }
            });
        }
        start.elapsed()
    };
    report("Forward Scan", a.ops, scan(false));
    report("Backward Scan", a.ops, scan(true));

    // ---- Delete --------------------------------------------------------------
    let start = Instant::now();
    {
        let db = &db;
        let cf = &cf;
        let keys = &keys;
        run_threaded(a.ops, a.threads, |lo, hi| {
            let mut i = lo;
            while i < hi {
                let be = (i + a.batch).min(hi);
                let mut txn = db.begin_with_isolation(IsolationLevel::ReadCommitted);
                for key in &keys[i..be] {
                    txn.delete(cf, key).unwrap();
                }
                txn.commit().unwrap();
                i = be;
            }
        });
    }
    report("Delete", a.ops, start.elapsed());

    db.close().expect("final close");
    if !a.keep {
        let _ = std::fs::remove_dir_all(&db_path);
    }
}
