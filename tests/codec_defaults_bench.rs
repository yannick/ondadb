//! Plan C P10 benchmark: the graduated default codecs (`[None, Lz4, Zstd]`)
//! against the pre-0.10 default (uniform `None`).
//!
//! Both arms load the same semi-compressible data (text-like values: a random
//! word salad from a small vocabulary plus a random suffix), compact it to the
//! bottom, and report on-disk bytes, load+compaction time, and point-read and
//! full-scan throughput after a reopen. Ignored by default; run it with
//!
//! ```sh
//! cargo test --release --features unsafe-fastpath --test codec_defaults_bench -- --ignored --nocapture
//! ```
//!
//! Arms alternate in one process. The machine is thermally noisy (see
//! `docs/performance.md`): compare same-invocation ratios, never absolute
//! numbers across sessions, and run it several times.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamily, ColumnFamilyConfig, Compression, Options, DB};

const KEYS: u32 = 400_000;
const READS: u32 = 100_000;

const WORDS: [&str; 16] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
    "kilo", "lima", "mike", "november", "oscar", "papa",
];

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn value(i: u32) -> Vec<u8> {
    let mut r = Rng(u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut v = Vec::with_capacity(160);
    while v.len() < 120 {
        v.extend_from_slice(WORDS[(r.next() % 16) as usize].as_bytes());
        v.push(b' ');
    }
    for _ in 0..16 {
        v.push(b'a' + (r.next() % 26) as u8);
    }
    v
}

fn dir_bytes(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .unwrap()
        .flatten()
        .map(|e| {
            let m = e.metadata().unwrap();
            if m.is_dir() {
                dir_bytes(&e.path())
            } else if e.path().extension().is_some_and(|x| x == "klog" || x == "vlog") {
                m.len()
            } else {
                0
            }
        })
        .sum()
}

/// Small geometry, so ~60 MB of data spreads over L0-L2 and the bottom is a
/// Zstd level under the graduated default — the shape of a real tree, scaled.
fn geometry() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        write_buffer_size: 4 << 20,
        target_file_size: 2 << 20,
        l1_base_bytes: 8 << 20,
        ..ColumnFamilyConfig::default()
    }
}

fn arm(name: &str, cfg: ColumnFamilyConfig) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let t0 = Instant::now();
    {
        let db = DB::open(Options::new(&path)).unwrap();
        let cf = db.create_column_family("b", cfg).unwrap();
        for i in 0..KEYS {
            db.put(&cf, format!("key{i:012}").as_bytes(), &value(i), Duration::ZERO)
                .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.compact_range(&cf, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
            .unwrap();
        println!("{name:>10}: levels {:?}", cf.stats().levels);
        db.close().unwrap();
    }
    let load = t0.elapsed();
    let bytes = dir_bytes(dir.path());
    let db = DB::open(Options::new(&path)).unwrap();
    let cf: Arc<ColumnFamily> = db.get_column_family("b").unwrap();
    let mut r = Rng(42);
    let t1 = Instant::now();
    for _ in 0..READS {
        let i = (r.next() % u64::from(KEYS)) as u32;
        let _ = db.get(&cf, format!("key{i:012}").as_bytes()).unwrap();
    }
    let gets = f64::from(READS) / t1.elapsed().as_secs_f64();
    let t2 = Instant::now();
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut n = 0u64;
    while it.valid() {
        n += 1;
        it.next();
    }
    assert_eq!(n, u64::from(KEYS));
    let scan = n as f64 / t2.elapsed().as_secs_f64();
    drop(it);
    drop(txn);
    db.close().unwrap();
    println!(
        "{name:>10}: sst bytes {:>10}  load+compact {:>6.2}s  get {:>9.0}/s  scan {:>10.0}/s",
        bytes,
        load.as_secs_f64(),
        gets,
        scan
    );
}

#[test]
#[ignore = "benchmark; run manually with --ignored --nocapture"]
fn graduated_codecs_vs_uniform_none() {
    for _ in 0..2 {
        arm(
            "none",
            ColumnFamilyConfig {
                compression: Compression::None,
                compression_per_level: Vec::new(),
                ..geometry()
            },
        );
        arm("graduated", geometry());
    }
}
