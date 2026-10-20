//! A transaction iterator's overlay versus the size of the transaction's
//! buffer (wavesdb f5a5dc4, mstep MV-662).
//!
//! `#[ignore]`d — it is a measurement, not a gate. Run it with:
//!
//! ```sh
//! cargo test --release --features unsafe-fastpath \
//!     --test txn_overlay_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! Three shapes, each measured `RUNS` times (median reported, per AGENTS.md:
//! this machine is thermally noisy, so compare within a run):
//!
//! * **write-then-scan** — N buffered puts in one transaction, each followed
//!   by a one-key bounded scan of the key just written. The workload that
//!   motivated the wavesdb change.
//! * **creation cost** — a transaction holding B buffered writes builds 1,000
//!   one-key bounded iterators; ns per iterator as B grows. Flat means the
//!   overlay is bounded, linear means it copies the whole buffer.
//! * **merge write-then-scan** — the write-then-scan shape on a family with a
//!   merge operator, whose overlay path pre-folds every merged key.

use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, Options, SyncMode, DB};

const RUNS: usize = 5;

#[derive(Debug)]
struct Concat;

impl MergeOperator for Concat {
    fn name(&self) -> &str {
        "bench.concat.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let mut out = existing.map(<[u8]>::to_vec).unwrap_or_default();
        for operand in operands {
            out.extend_from_slice(operand);
        }
        Ok(out)
    }
}

fn open(dir: &std::path::Path, merge: bool) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                merge_operator_name: merge.then(|| "bench.concat.v1".to_string()),
                sync_mode: SyncMode::None,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    // A committed base so every scan has a store to merge with.
    for i in 0..10_000usize {
        db.put(&cf, &key(i), b"committed", Duration::ZERO).unwrap();
    }
    (db, cf)
}

fn key(i: usize) -> Vec<u8> {
    format!("k/{i:08}").into_bytes()
}

/// The exclusive successor of `k`: `[k, succ(k))` holds exactly `k`.
fn succ(k: &[u8]) -> Vec<u8> {
    let mut s = k.to_vec();
    s.push(0);
    s
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// N writes, each followed by a one-key bounded scan of what was just written.
fn write_then_scan(db: &DB, cf: &Arc<ColumnFamily>, n: usize, merge: bool) -> f64 {
    let mut t = db.begin();
    let start = Instant::now();
    for i in 0..n {
        let k = key(i);
        if merge {
            t.merge(cf, &k, b"+").unwrap();
        } else {
            t.put(cf, &k, b"buffered", Duration::ZERO).unwrap();
        }
        let hi = succ(&k);
        let mut it = t.new_iterator_bounded(cf, Bound::Included(&k), Bound::Excluded(&hi));
        it.seek_to_first();
        assert!(it.valid(), "the key just written is in range");
        assert_eq!(it.key(), &k[..]);
        it.next();
        assert!(!it.valid(), "one key in range");
    }
    let secs = start.elapsed().as_secs_f64();
    t.rollback().unwrap();
    secs
}

/// ns per one-key bounded iterator built over a buffer of `b` writes.
fn creation_ns(db: &DB, cf: &Arc<ColumnFamily>, b: usize) -> f64 {
    const ITERS: usize = 1_000;
    let mut t = db.begin();
    for i in 0..b {
        t.put(cf, &key(i), b"buffered", Duration::ZERO).unwrap();
    }
    let probe = key(b / 2);
    let hi = succ(&probe);
    let start = Instant::now();
    for _ in 0..ITERS {
        let mut it = t.new_iterator_bounded(cf, Bound::Included(&probe), Bound::Excluded(&hi));
        it.seek_to_first();
        assert!(it.valid());
    }
    let ns = start.elapsed().as_nanos() as f64 / ITERS as f64;
    t.rollback().unwrap();
    ns
}

#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn txn_overlay_bench() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), false);
    for n in [1_000usize, 10_000] {
        let runs: Vec<f64> = (0..RUNS).map(|_| write_then_scan(&db, &cf, n, false)).collect();
        println!(
            "write-then-scan        N={n:>6}: median {:.4} s  runs {:?}",
            median(runs.clone()),
            runs.iter().map(|s| format!("{s:.4}")).collect::<Vec<_>>()
        );
    }
    for b in [0usize, 100, 1_000, 10_000, 100_000] {
        let runs: Vec<f64> = (0..RUNS).map(|_| creation_ns(&db, &cf, b)).collect();
        println!(
            "iterator creation      B={b:>6}: median {:>10.0} ns/iter",
            median(runs)
        );
    }
    drop(cf);
    db.close().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), true);
    for n in [1_000usize, 3_000] {
        let runs: Vec<f64> = (0..RUNS).map(|_| write_then_scan(&db, &cf, n, true)).collect();
        println!(
            "merge write-then-scan  N={n:>6}: median {:.4} s  runs {:?}",
            median(runs.clone()),
            runs.iter().map(|s| format!("{s:.4}")).collect::<Vec<_>>()
        );
    }
    drop(cf);
    db.close().unwrap();
}
