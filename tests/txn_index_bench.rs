//! Read-your-own-writes cost inside one transaction: N buffered writes, then N
//! reads of those same keys, for growing N. Before 0.10.1 every read scanned
//! the whole buffer, so the read phase grew as N²; the buffered-write index
//! makes it linear. Also times many tiny transactions (1-8 writes, each read
//! back), the path the index must not slow down.
//!
//! `#[ignore]`d — it is a measurement, not a gate. Run it with:
//!
//! ```sh
//! cargo test --release --features unsafe-fastpath --test txn_index_bench \
//!     -- --ignored --nocapture --test-threads 1
//! ```
//!
//! This machine is thermally noisy (±15-20 % run to run), so every phase is
//! measured `RUNS` times and the median is reported.

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
        for o in operands {
            out.extend_from_slice(o);
        }
        Ok(out)
    }
}

fn open(dir: &std::path::Path, merge: bool) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let cf = db
        .create_column_family(
            "c",
            ColumnFamilyConfig {
                merge_operator_name: merge.then(|| Concat.name().to_string()),
                sync_mode: SyncMode::None,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    (db, cf)
}

fn key(i: usize) -> Vec<u8> {
    format!("key/{i:010}").into_bytes()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// Puts, read back with `get`.
    Get,
    /// Puts, read back with one `multi_get` per 64 keys.
    MultiGet,
    /// Puts plus one range delete over keys nobody wrote, read back with
    /// `get` — every read also checks the buffered ranges.
    GetWithRange,
    /// Merge operands on a merge family, read back with `get` (the buffered
    /// chain path).
    MergeGet,
}

/// One transaction: `n` writes, then `n` reads. Returns `(write_ms, read_ms)`.
fn one_run(shape: Shape, n: usize) -> (f64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), matches!(shape, Shape::MergeGet));
    let keys: Vec<Vec<u8>> = (0..n).map(key).collect();
    let mut t = db.begin();
    let w = Instant::now();
    if matches!(shape, Shape::GetWithRange) {
        t.delete_range(&cf, b"zz/0", b"zz/9").unwrap();
    }
    for k in &keys {
        match shape {
            Shape::MergeGet => t.merge(&cf, k, b"v").unwrap(),
            _ => t.put(&cf, k, b"value", Duration::ZERO).unwrap(),
        }
    }
    let write_ms = w.elapsed().as_secs_f64() * 1e3;
    let r = Instant::now();
    match shape {
        Shape::MultiGet => {
            for chunk in keys.chunks(64) {
                let refs: Vec<&[u8]> = chunk.iter().map(Vec::as_slice).collect();
                for v in t.multi_get(&cf, &refs) {
                    assert!(v.is_ok());
                }
            }
        }
        _ => {
            for k in &keys {
                assert!(!t.get(&cf, k).unwrap().is_empty());
            }
        }
    }
    let read_ms = r.elapsed().as_secs_f64() * 1e3;
    t.rollback().unwrap();
    (write_ms, read_ms)
}

#[test]
#[ignore]
fn own_writes_read_back() {
    for shape in [
        Shape::Get,
        Shape::MultiGet,
        Shape::GetWithRange,
        Shape::MergeGet,
    ] {
        let sizes: &[usize] = match std::env::var("TXN_INDEX_BENCH_SIZES") {
            Ok(s) => &*s
                .split(',')
                .map(|n| n.parse().unwrap())
                .collect::<Vec<_>>()
                .leak(),
            Err(_) => &[1_000, 10_000, 50_000],
        };
        for &n in sizes {
            let mut ws = Vec::new();
            let mut rs = Vec::new();
            for _ in 0..RUNS {
                let (w, r) = one_run(shape, n);
                ws.push(w);
                rs.push(r);
            }
            let read = median(rs.clone());
            println!(
                "{shape:?}\tn={n}\twrite_ms={:.2}\tread_ms={read:.2}\tns/read={:.0}\truns={rs:.2?}",
                median(ws),
                read * 1e6 / n as f64,
            );
        }
    }
}

/// Many tiny transactions: `size` puts then `size` gets each. The index never
/// builds below its threshold, so this must not move.
#[test]
#[ignore]
fn small_txns_unchanged() {
    const TXNS: usize = 50_000;
    for size in [1usize, 4, 8] {
        let mut per_txn = Vec::new();
        for _ in 0..RUNS {
            let dir = tempfile::tempdir().unwrap();
            let (db, cf) = open(dir.path(), false);
            let keys: Vec<Vec<u8>> = (0..size).map(key).collect();
            let t0 = Instant::now();
            for _ in 0..TXNS {
                let mut t = db.begin();
                for k in &keys {
                    t.put(&cf, k, b"value", Duration::ZERO).unwrap();
                }
                for k in &keys {
                    assert!(t.get(&cf, k).is_ok());
                }
                t.rollback().unwrap();
            }
            per_txn.push(t0.elapsed().as_secs_f64() * 1e9 / TXNS as f64);
        }
        println!(
            "small txn size={size}\tns/txn={:.0}\truns={per_txn:.0?}",
            median(per_txn.clone())
        );
    }
}
