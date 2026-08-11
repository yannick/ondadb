//! Repeat-read throughput of vlog (large) values.
//!
//! `#[ignore]`d: this is a measurement harness, not a gate — it prints numbers
//! and asserts nothing about them, and it writes tens of megabytes. Run it
//! explicitly, in both feature configurations, because the two exercise
//! different read paths (mmap vs buffered `pread`):
//!
//! ```sh
//! cargo test --release --test vlog_read_bench -- --ignored --nocapture
//! cargo test --release --features unsafe-fastpath --test vlog_read_bench -- --ignored --nocapture
//! ```
//!
//! Thermal noise on this machine is ±15–20% (see `docs/performance.md`), so
//! compare A/B runs minutes apart on the same build, never absolute numbers
//! across sessions.

use std::sync::Arc;
use std::time::Instant;

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::default_comparator;
use ondadb::config::Compression;
use ondadb::sst::{Reader, Writer, WriterOptions};
use ondadb::storage::LocalStorage;

fn opts(n: usize) -> WriterOptions {
    WriterOptions {
        compression: Compression::None,
        compression_rules: Vec::new(),
        cmp: default_comparator(),
        enable_bloom: true,
        bloom_fpr: 0.01,
        klog_value_threshold: 512,
        block_size: 4096,
        expected_entries: n,
        use_btree: false,
        restart_interval: 8,
    }
}

/// Build an SSTable of `n` values of `val_size` bytes, all in the vlog, and
/// return an open reader plus the keys.
fn build(dir: &std::path::Path, n: usize, val_size: usize) -> (Arc<Reader>, Vec<String>) {
    let klog = dir.join("bench.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(n)).unwrap();
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        // Incompressible-ish, and distinct per key so nothing dedups.
        let mut val = vec![0u8; val_size];
        for (j, b) in val.iter_mut().enumerate() {
            *b = (j.wrapping_mul(31).wrapping_add(i)) as u8;
        }
        let k = format!("key{i:06}");
        w.add(k.as_bytes(), &val, (i + 1) as u64, 0, false, false)
            .unwrap();
        keys.push(k);
    }
    w.finish().unwrap();
    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(64 << 20)); // the default cache size
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        1,
        default_comparator(),
    )
    .unwrap();
    (r, keys)
}

/// Read every key `rounds` times and report throughput. The first pass over the
/// keys is a warm-up (page-cache fill, first CRC) and is excluded.
fn measure(label: &str, n: usize, val_size: usize, rounds: usize) {
    let dir = tempfile::tempdir().unwrap();
    let (r, keys) = build(dir.path(), n, val_size);

    for k in &keys {
        let (v, _, found, _) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
        assert!(found);
        assert_eq!(v.unwrap().len(), val_size);
    }

    let mut sink = 0u64;
    let t0 = Instant::now();
    for _ in 0..rounds {
        for k in &keys {
            let (v, _, _, _) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
            let v = v.unwrap();
            sink = sink
                .wrapping_add(v[0] as u64)
                .wrapping_add(v[val_size - 1] as u64);
        }
    }
    let el = t0.elapsed();
    let reads = (rounds * n) as f64;
    let bytes = reads * val_size as f64;
    println!(
        "{label:<28} {:>5} reads of {:>7} KiB  {:>8.1} us/read  {:>6.2} GB/s  (sink {sink})",
        reads as u64,
        val_size / 1024,
        el.as_secs_f64() * 1e6 / reads,
        bytes / el.as_secs_f64() / 1e9,
    );
}

#[test]
#[ignore]
fn vlog_repeat_read_400k() {
    measure("vlog repeat-read 400 KiB", 16, 400 * 1024, 40);
}

#[test]
#[ignore]
fn vlog_repeat_read_5m() {
    measure("vlog repeat-read 5 MiB", 4, 5 * 1024 * 1024, 20);
}
