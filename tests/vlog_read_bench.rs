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
        0,
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

// ---------------------------------------------------------------------------
// Hot/cold large-value point reads with and without the vlog value cache (0.5)
// ---------------------------------------------------------------------------

/// One phase's measurements. Rates are `hits / (hits + misses)`.
struct Phase {
    hot_us: f64,
    hot_gbs: f64,
    klog_hit_rate: f64,
    vlog_hit_rate: f64,
    vlog_bytes: i64,
}

/// Xorshift64*, so the read order is reproducible without a dependency.
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// A table with a large klog (many small inline values) and a set of large
/// separated values. Both compete for one block cache, which is the whole point
/// of the measurement: what vlog admission costs klog residency.
fn build_mixed(dir: &std::path::Path, n_small: usize, n_big: usize, big_size: usize) -> String {
    let klog = dir.join("mixed.klog");
    let klog = klog.to_str().unwrap().to_string();
    let mut w = Writer::new(&klog, opts(n_small + n_big)).unwrap();
    let small = vec![b's'; 64];
    for i in 0..n_small {
        w.add(
            format!("s{i:07}").as_bytes(),
            &small,
            (i + 1) as u64,
            0,
            false,
            false,
        )
        .unwrap();
    }
    for i in 0..n_big {
        let mut val = vec![0u8; big_size];
        for (j, b) in val.iter_mut().enumerate() {
            *b = (j.wrapping_mul(31).wrapping_add(i)) as u8;
        }
        w.add(
            format!("v{i:07}").as_bytes(),
            &val,
            (n_small + i + 1) as u64,
            0,
            false,
            false,
        )
        .unwrap();
    }
    w.finish().unwrap();
    klog
}

#[allow(clippy::too_many_arguments)]
fn run_phase(
    klog: &str,
    cache_bytes: i64,
    limit: usize,
    n_small: usize,
    hot_big: usize,
    big_size: usize,
    rounds: usize,
    small_per_big: usize,
) -> Phase {
    let bc = Arc::new(BlockCache::new(cache_bytes));
    let r = Reader::open(
        klog,
        LocalStorage::new(Arc::new(FileCache::new(16)), cfg!(feature = "mmap-reads")),
        Arc::clone(&bc),
        1,
        default_comparator(),
        limit,
    )
    .unwrap();

    // Warm-up round, excluded: page cache fill, first CRC of every frame, and
    // the first admission of every block.
    let mut rng = 0x243F_6A88_85A3_08D3u64;
    for i in 0..hot_big {
        let _ = r.get(format!("v{i:07}").as_bytes(), u64::MAX, 0).unwrap();
        for _ in 0..small_per_big {
            let j = (next_rand(&mut rng) as usize) % n_small;
            let _ = r.get(format!("s{j:07}").as_bytes(), u64::MAX, 0).unwrap();
        }
    }

    let base = bc.stats();
    let mut sink = 0u64;
    let mut hot_nanos = 0u128;
    for _ in 0..rounds {
        for i in 0..hot_big {
            let key = format!("v{i:07}");
            let t0 = Instant::now();
            let (v, _, _, _) = r.get(key.as_bytes(), u64::MAX, 0).unwrap();
            hot_nanos += t0.elapsed().as_nanos();
            let v = v.unwrap();
            sink = sink
                .wrapping_add(v[0] as u64)
                .wrapping_add(v[big_size - 1] as u64);
            // Interleaved klog pressure: this is what vlog admission evicts.
            for _ in 0..small_per_big {
                let j = (next_rand(&mut rng) as usize) % n_small;
                let (v, _, _, _) = r.get(format!("s{j:07}").as_bytes(), u64::MAX, 0).unwrap();
                sink = sink.wrapping_add(v.unwrap()[0] as u64);
            }
        }
    }
    std::hint::black_box(sink);

    let st = bc.stats();
    let rate = |h: u64, m: u64| {
        let t = h + m;
        if t == 0 {
            f64::NAN
        } else {
            h as f64 / t as f64
        }
    };
    let hot_reads = (rounds * hot_big) as f64;
    let secs = hot_nanos as f64 / 1e9;
    Phase {
        hot_us: secs * 1e6 / hot_reads,
        hot_gbs: hot_reads * big_size as f64 / secs / 1e9,
        klog_hit_rate: rate(st.hits - base.hits, st.misses - base.misses),
        vlog_hit_rate: rate(
            st.vlog_hits - base.vlog_hits,
            st.vlog_misses - base.vlog_misses,
        ),
        vlog_bytes: st.vlog_bytes,
    }
}

/// Prints the acceptance evidence for feature 0.5: hot large-value point-read
/// latency with the cache off vs on, the vlog cache hit rate, and the klog
/// block hit-rate delta the admission costs.
///
/// Two scenarios, because the feature's worth is entirely a function of whether
/// the hot set fits: a **fits** case (the hot values are a fraction of the
/// cache) and a **thrashes** case (they are most of it). The second is the
/// "vlog admission evicts klog blocks" risk row made measurable, and it is why
/// the option defaults to 0.
///
/// The klog hit rate is **only meaningful on the default build**: under
/// `mmap-reads` an uncompressed klog block is served straight from the mapping
/// by `read_data_block_local` and never enters the block cache, so there is no
/// klog residency for vlog admission to displace.
#[test]
#[ignore]
fn vlog_value_cache_hot_cold_point_reads() {
    const N_SMALL: usize = 120_000;
    const N_BIG: usize = 96;
    const BIG: usize = 64 << 10;
    const CACHE: i64 = 8 << 20;
    const ROUNDS: usize = 12;
    const SMALL_PER_BIG: usize = 40;

    let dir = tempfile::tempdir().unwrap();
    let klog = build_mixed(dir.path(), N_SMALL, N_BIG, BIG);

    let config = if cfg!(feature = "mmap-reads") {
        "unsafe-fastpath (mmap)"
    } else {
        "default (buffered)"
    };
    println!("\n== vlog value cache, hot/cold large-value point reads [{config}] ==");
    println!(
        "   {} KiB values, {N_SMALL} small keys, {SMALL_PER_BIG} small reads per large read, \
         {} MiB cache, {ROUNDS} rounds",
        BIG / 1024,
        CACHE >> 20
    );

    for (scenario, hot_big) in [("fits", 16usize), ("thrashes", N_BIG)] {
        let phase = |limit: usize| {
            run_phase(
                &klog,
                CACHE,
                limit,
                N_SMALL,
                hot_big,
                BIG,
                ROUNDS,
                SMALL_PER_BIG,
            )
        };
        // Off, on, off: alternating the arms makes thermal drift over the run
        // visible instead of silently biasing one of them.
        let off_a = phase(0);
        let on = phase(1 << 20);
        let off_b = phase(0);

        println!(
            "\n   -- hot set {scenario}: {hot_big} hot values = {:.1} MiB of a {} MiB cache --",
            (hot_big * BIG) as f64 / (1 << 20) as f64,
            CACHE >> 20
        );
        for (name, p) in [("off (A)", &off_a), ("on  1MiB", &on), ("off (B)", &off_b)] {
            println!(
                "   {name:<9} hot {:>7.1} us/read {:>6.2} GB/s   klog hit {:>6.2}%   vlog hit \
                 {:>7}   vlog resident {:>5.1} MiB",
                p.hot_us,
                p.hot_gbs,
                p.klog_hit_rate * 100.0,
                if p.vlog_hit_rate.is_nan() {
                    "n/a".to_string()
                } else {
                    format!("{:.2}%", p.vlog_hit_rate * 100.0)
                },
                p.vlog_bytes as f64 / (1 << 20) as f64,
            );
        }
        let off_us = (off_a.hot_us + off_b.hot_us) / 2.0;
        let off_klog = (off_a.klog_hit_rate + off_b.klog_hit_rate) / 2.0;
        println!(
            "   => hot latency {:.2}x ({:.1} -> {:.1} us), klog hit-rate delta {:+.2} pp",
            off_us / on.hot_us,
            off_us,
            on.hot_us,
            (on.klog_hit_rate - off_klog) * 100.0,
        );
    }
    if cfg!(feature = "mmap-reads") {
        println!(
            "   (klog hit rate is not meaningful in this config: uncompressed klog blocks are \
             served from the mmap and never enter the cache)"
        );
    }
}
