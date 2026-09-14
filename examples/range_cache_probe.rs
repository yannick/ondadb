//! Comparable pre/post benchmark: copy this file into the baseline checkout.
//! cargo run --release --example range_cache_probe
use ondadb::{ColumnFamilyConfig, Options, DB};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
fn main() {
    for unified in [false, true] {
        for (n, unique) in [(0usize, 0usize), (1024, 1024), (8331, 24), (18117, 466)] {
            for live in [false, true] {
                for run in 0..5 {
                    let dir = tempfile::tempdir().unwrap();
                    let mut opts = Options::new(dir.path().to_str().unwrap());
                    opts.unified_memtable = unified;
                    let db = DB::open(opts).unwrap();
                    let cf = db
                        .create_column_family("data", ColumnFamilyConfig::default())
                        .unwrap();
                    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
                        .unwrap();
                    if live {
                        for key in 0..256u16 {
                            let mut k = vec![255];
                            k.extend_from_slice(&key.to_be_bytes());
                            db.put(&cf, &k, b"live", Duration::ZERO).unwrap();
                        }
                    }
                    for i in 0..n {
                        let p = (i % unique) as u16;
                        db.delete_range(&cf, &p.to_be_bytes(), &(p + 1).to_be_bytes())
                            .unwrap();
                    }
                    let scan = || {
                        let txn = db.begin();
                        let mut it = txn.new_iterator(&cf);
                        it.seek_to_first();
                        let mut count = 0;
                        while it.valid() {
                            count += 1;
                            black_box(it.key());
                            it.next();
                        }
                        assert_eq!(count, if live { 256 } else { 0 });
                    };
                    let start = Instant::now();
                    scan();
                    let cold_us = start.elapsed().as_micros();
                    let start = Instant::now();
                    for _ in 0..10 {
                        scan();
                    }
                    let warm_ns = start.elapsed().as_nanos() / 10;
                    db.flush_memtable(&cf).unwrap();
                    let start = Instant::now();
                    for _ in 0..10 {
                        scan();
                    }
                    let after_flush_request_ns = start.elapsed().as_nanos() / 10;
                    println!("unified={unified} n={n} unique={unique} live={live} run={run} cold_us={cold_us} warm_ns={warm_ns} after_flush_request_ns={after_flush_request_ns}");
                }
            }
        }
    }
}
