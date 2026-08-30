//! Nil-path microbenchmark for 0.10 (`PerfContext`).
//!
//! `#[ignore]`d: this is a measurement, not an assertion. The whole-process
//! harness (`onda_bench -perf_scope off|thread`) is too noisy on this machine to
//! resolve a few percent — run-to-run spread on the Get phase is ±20% and
//! bimodal. This runs both arms **in one process, over one warm database,
//! alternating trial by trial**, so page cache, allocator state and clock domain
//! are shared and only the open scope differs.
//!
//! ```sh
//! cargo test --release --features unsafe-fastpath --test perf_nilpath \
//!     -- --ignored --nocapture
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamilyConfig, Options, DB};

const KEYS: usize = 200_000;
const TRIALS: usize = 11;

fn keys() -> Vec<Vec<u8>> {
    // Deterministic xorshift, matching the bench binary's key shape (16 bytes).
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    (0..KEYS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let mut k = vec![0u8; 16];
            k[..8].copy_from_slice(&state.to_le_bytes());
            k[8..].copy_from_slice(&state.rotate_left(24).to_le_bytes());
            k
        })
        .collect()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn nil_path_scope_overhead() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("bench", ColumnFamilyConfig::default())
        .unwrap();
    let ks = keys();
    let value = vec![b'v'; 100];

    let mut txn = db.begin();
    for (i, k) in ks.iter().enumerate() {
        txn.put(&cf, k, &value, Duration::ZERO).unwrap();
        if i % 1000 == 999 {
            txn.commit().unwrap();
            txn = db.begin();
        }
    }
    txn.commit().unwrap();
    db.flush_memtable(&cf).unwrap();

    // Warm the readers, the block cache and the page cache before timing.
    for _ in 0..2 {
        for k in &ks {
            black_box(db.get(&cf, k).ok());
        }
    }

    let pass = |scope_open: bool| -> f64 {
        let scope = scope_open.then(ondadb::perf::enter);
        let t0 = Instant::now();
        for k in &ks {
            black_box(db.get(&cf, k).ok());
        }
        let elapsed = t0.elapsed().as_secs_f64() * 1e3;
        drop(scope);
        elapsed
    };

    // Evidence that the timed arm really is doing counted work — a scope that
    // counted nothing would trivially cost nothing.
    let (_, sample) = db.get_with_perf(&cf, &ks[0]);
    println!("per-get context: {sample:?}");
    assert!(sample.memtable_probes > 0 && sample.bloom_probes > 0);

    let mut off = Vec::with_capacity(TRIALS);
    let mut on = Vec::with_capacity(TRIALS);
    for t in 0..TRIALS {
        // Alternate the order every trial so a systematic first/second-position
        // effect cancels instead of accruing to one arm.
        if t % 2 == 0 {
            off.push(pass(false));
            on.push(pass(true));
        } else {
            on.push(pass(true));
            off.push(pass(false));
        }
        println!(
            "trial {t}: no_scope={:.2} ms  scope={:.2} ms  ratio={:.4}",
            off[t],
            on[t],
            on[t] / off[t]
        );
    }

    let ratios: Vec<f64> = (0..TRIALS).map(|i| on[i] / off[i]).collect();
    let p50_off = median(off.clone());
    let p50_on = median(on.clone());
    println!("keys={KEYS} trials={TRIALS}");
    println!("p50 no_scope = {p50_off:.2} ms");
    println!("p50 scope    = {p50_on:.2} ms");
    println!(
        "p50 delta    = {:+.2}%   (median of per-trial ratios: {:+.2}%)",
        (p50_on / p50_off - 1.0) * 100.0,
        (median(ratios) - 1.0) * 100.0
    );
    db.close().unwrap();
}
