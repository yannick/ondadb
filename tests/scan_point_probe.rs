//! A deliberately minimal scan + point-read probe that compiles unchanged
//! against **both** the 1.1 tree and its 0.8.2 baseline (`roadmap/wave-a`).
//!
//! It uses no 1.1 API at all, so the only difference between the two builds is
//! the engine underneath it — which is exactly the "no measurable regression for
//! a column family with no operator configured" question.
//!
//! One process = one measurement, so the two builds can be **alternated at the
//! process level** and share this machine's thermal drift.

use std::time::{Duration, Instant};

use ondadb::{ColumnFamilyConfig, Options, SyncMode, DB};

const ROWS: usize = 60_000;
const PROBES: usize = 20_000;

#[test]
#[ignore = "probe; driven by the 1.1 no-regression comparison"]
fn scan_and_point_read_probe() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "c",
            ColumnFamilyConfig {
                sync_mode: SyncMode::None,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let value = vec![b'v'; 48];
    for i in 0..ROWS {
        db.put(&cf, format!("row/{i:010}").as_bytes(), &value, Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    for _ in 0..2 {
        let t = db.begin();
        let mut it = t.new_iterator(&cf);
        it.seek_to_first();
        while it.valid() {
            std::hint::black_box(it.value());
            it.next();
        }
    }

    let t = db.begin();
    let mut it = t.new_iterator(&cf);
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

    let start = Instant::now();
    for i in 0..PROBES {
        let k = format!("row/{:010}", (i * 7919) % ROWS);
        std::hint::black_box(db.get(&cf, k.as_bytes()).unwrap());
    }
    let point_ns = start.elapsed().as_nanos() as f64 / PROBES as f64;
    println!("PROBE scan_ns_per_entry={scan_ns:.3} point_ns={point_ns:.1}");
    db.close().unwrap();
}
