//! Feature 2.1 acceptance benchmark: `block_restart_interval` crossed with
//! `data_block_size`, legacy twin against delta.
//!
//! `#[ignore]`d — it is a measurement, not a gate. Run it with:
//!
//! ```sh
//! cargo test --release --features unsafe-fastpath \
//!     --test prefix_delta_bench -- --ignored --nocapture
//! ```
//!
//! Results are written to `bench-results/2.1/<date>/` as CSV plus a summary.
//! Per AGENTS.md the machine is thermally noisy (±15-20 % run to run), so every
//! cell is measured `RUNS` times and the report carries the **same-run ratio**
//! delta/legacy, never an absolute number to be compared across sessions.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::default_comparator;
use ondadb::config::Compression;
use ondadb::format::{CAP_EXTENDED_RECORDS, CAP_PREFIX_DELTA};
use ondadb::sst::{Reader, Writer, WriterOptions};
use ondadb::storage::LocalStorage;
use ondadb::{ColumnFamilyConfig, Options, DB};

const RUNS: usize = 5;
const ENTRIES: usize = 120_000;
const INTERVALS: [usize; 4] = [4, 8, 16, 32];
const BLOCK_SIZES: [usize; 4] = [4 << 10, 8 << 10, 16 << 10, 64 << 10];

/// The documented spada key shape: `tenant/cluster/segment`, deeply shared.
fn prefix_heavy_keys(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            format!(
                "tenant/{:04}/cluster/{:04}/segment/{:010}",
                i / 20_000,
                (i / 500) % 40,
                i
            )
            .into_bytes()
        })
        .collect()
}

/// Keys with no exploitable prefix: the phase that must show no more than the
/// +1 byte/entry floor cost.
fn random_keys(n: usize) -> Vec<Vec<u8>> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut v: Vec<Vec<u8>> = (0..n)
        .map(|_| {
            let mut k = Vec::with_capacity(36);
            for _ in 0..5 {
                k.extend_from_slice(&next().to_be_bytes());
            }
            k.truncate(36);
            k
        })
        .collect();
    v.sort();
    v.dedup();
    v
}

fn writer_opts(interval: usize, block_size: usize, delta: bool) -> WriterOptions {
    WriterOptions {
        // Compressed, because the honest hypothesis is that block compression
        // already recovers most redundancy on disk — the on-disk column has to
        // be measured against a compressed baseline, not a raw one.
        compression: Compression::Lz4,
        compression_rules: Vec::new(),
        cmp: default_comparator(),
        enable_bloom: true,
        bloom_fpr: Some(0.01),
        klog_value_threshold: 1 << 20, // values stay inline: klog-only measurement
        block_size,
        expected_entries: ENTRIES,
        use_btree: false,
        restart_interval: interval,
        extended_entries: false,
        prefix_delta: delta,
    }
}

fn build(path: &str, keys: &[Vec<u8>], o: WriterOptions) {
    let mut w = Writer::new(path, o).unwrap();
    let value = vec![b'v'; 24];
    for (i, k) in keys.iter().enumerate() {
        w.add(k, &value, i as u64 + 1, 0, ondadb::format::KIND_PUT).unwrap();
    }
    w.finish().unwrap();
}

/// Bytes a table costs the *decompressed* block cache if every block is
/// resident, plus its on-disk (compressed) data bytes and its index block size.
/// Parsed straight from the file: `[alg u8][comp_len u32][raw_len u32][crc u32]`
/// per block, data blocks running from offset 0 to the first meta block.
struct TableBytes {
    decompressed: u64,
    on_disk_data: u64,
    index: u64,
    blocks: u64,
    file: u64,
}

fn table_bytes(path: &str) -> TableBytes {
    const HEADER: usize = 13;
    const FOOTER: usize = 64;
    let bytes = std::fs::read(path).unwrap();
    let f = bytes.len() - FOOTER;
    let index_off = u64::from_le_bytes(bytes[f..f + 8].try_into().unwrap());
    let index_len = u64::from_le_bytes(bytes[f + 8..f + 16].try_into().unwrap());
    let bloom_off = u64::from_le_bytes(bytes[f + 16..f + 24].try_into().unwrap());
    // Data blocks end where the first meta block starts.
    let data_end = if bloom_off > 0 { bloom_off } else { index_off } as usize;
    let (mut off, mut decompressed, mut blocks) = (0usize, 0u64, 0u64);
    while off < data_end {
        let comp_len = u32::from_le_bytes(bytes[off + 1..off + 5].try_into().unwrap()) as usize;
        let raw_len = u32::from_le_bytes(bytes[off + 5..off + 9].try_into().unwrap()) as u64;
        decompressed += raw_len;
        blocks += 1;
        off += HEADER + comp_len;
    }
    TableBytes {
        decompressed,
        on_disk_data: data_end as u64,
        index: index_len,
        blocks,
        file: bytes.len() as u64,
    }
}

fn open(path: &str, cache: Arc<BlockCache>) -> Arc<Reader> {
    Reader::open(
        path,
        LocalStorage::new(Arc::new(FileCache::new(8)), cfg!(feature = "mmap-reads")),
        cache,
        1,
        default_comparator(),
        0,
    )
    .unwrap()
}

#[derive(Default, Clone, Copy)]
struct Timing {
    forward_ns: u64,
    reverse_ns: u64,
    reverse_p99_ns: u64,
    entries: u64,
    cache_hits: u64,
    cache_misses: u64,
}

/// One warm forward scan and one warm reverse scan, timed. The cache is warmed
/// first so the numbers are decode CPU, not I/O.
fn time_scans(r: &Arc<Reader>) -> Timing {
    // Warm.
    let mut it = r.iter();
    it.seek_to_first();
    while it.valid() {
        std::hint::black_box(it.user_key());
        it.next();
    }

    let scope = ondadb::perf::enter();
    let start = Instant::now();
    let mut it = r.iter();
    let mut entries = 0u64;
    it.seek_to_first();
    while it.valid() {
        std::hint::black_box(it.user_key());
        std::hint::black_box(it.seq());
        entries += 1;
        it.next();
    }
    let forward_ns = start.elapsed().as_nanos() as u64;

    // Reverse, with a per-step sample so p99 can be published.
    let mut steps: Vec<u64> = Vec::with_capacity(entries as usize);
    let start = Instant::now();
    let mut it = r.iter();
    it.seek_to_last();
    while it.valid() {
        let t = Instant::now();
        std::hint::black_box(it.user_key());
        it.prev();
        steps.push(t.elapsed().as_nanos() as u64);
    }
    let reverse_ns = start.elapsed().as_nanos() as u64;
    let ctx = scope.finish();
    steps.sort_unstable();
    let p99 = steps
        .get(steps.len().saturating_sub(1).min(steps.len() * 99 / 100))
        .copied()
        .unwrap_or(0);
    Timing {
        forward_ns,
        reverse_ns,
        reverse_p99_ns: p99,
        entries,
        cache_hits: ctx.block_cache_hits,
        cache_misses: ctx.block_misses,
    }
}

/// Merge-scan throughput through the public iterator: four overlapping L0
/// tables, so every group forces `capture_group_key`. Legacy children serve a
/// pinned key; delta children serve a buffered copy — this is the cost the
/// acceptance criteria require published, not argued.
fn merge_scan_ns(delta: bool, keys: &[Vec<u8>]) -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    if delta {
        db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
            .unwrap();
    }
    let cf = db
        .create_column_family(
            "b",
            ColumnFamilyConfig {
                enable_prefix_delta_keys: delta,
                block_restart_interval: 8,
                compression: Compression::Lz4,
                l1_file_count_trigger: 1 << 20, // keep the children separate
                write_buffer_size: 1 << 30,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let value = vec![b'v'; 24];
    for _round in 0..4 {
        for k in keys {
            db.put(&cf, k, &value, Duration::ZERO).unwrap();
        }
        db.flush_memtable(&cf).unwrap();
    }
    // Warm, then measure.
    for pass in 0..2 {
        let mut txn = db.begin();
        let mut it = txn.new_iterator(&cf);
        let start = Instant::now();
        it.seek_to_first();
        let mut n = 0u64;
        while it.valid() {
            std::hint::black_box(it.key());
            std::hint::black_box(it.value());
            n += 1;
            it.next();
        }
        let ns = start.elapsed().as_nanos() as u64;
        drop(it);
        txn.rollback().unwrap();
        if pass == 1 {
            db.close().unwrap();
            return (ns, n);
        }
    }
    unreachable!()
}

struct Cell {
    shape: &'static str,
    interval: usize,
    block_size: usize,
    delta: bool,
    bytes: TableBytes,
    timing: Timing,
}

fn measure(shape: &'static str, keys: &[Vec<u8>], interval: usize, block_size: usize) -> Vec<Cell> {
    let dir = tempfile::tempdir().unwrap();
    let mut out = Vec::new();
    for delta in [false, true] {
        let path = dir
            .path()
            .join(if delta { "d.klog" } else { "l.klog" })
            .to_str()
            .unwrap()
            .to_string();
        build(&path, keys, writer_opts(interval, block_size, delta));
        let bytes = table_bytes(&path);
        let r = open(&path, Arc::new(BlockCache::new(512 << 20)));
        let timing = time_scans(&r);
        out.push(Cell {
            shape,
            interval,
            block_size,
            delta,
            bytes,
            timing,
        });
    }
    out
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[test]
#[ignore = "benchmark; run manually and record the output"]
fn prefix_delta_interval_by_block_size_sweep() {
    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("bench-results/2.1")
        .join(std::env::var("BENCH_DATE").unwrap_or_else(|_| "latest".into()));
    std::fs::create_dir_all(&out_dir).unwrap();

    let corpora: Vec<(&'static str, Vec<Vec<u8>>)> = vec![
        ("prefix-heavy", prefix_heavy_keys(ENTRIES)),
        ("random", random_keys(ENTRIES)),
    ];

    let mut csv = String::from(
        "shape,interval,block_size,format,run,blocks,decompressed_bytes,\
         on_disk_data_bytes,index_bytes,file_bytes,entries,forward_ns_per_entry,\
         reverse_ns_per_entry,reverse_p99_ns,cache_hits,cache_misses\n",
    );
    // shape -> (interval, block) -> per-run ratios
    let mut summary: Vec<String> = Vec::new();

    for (shape, keys) in &corpora {
        summary.push(format!("\n### {shape} ({} keys)\n", keys.len()));
        summary.push(
            "| interval | block | decompressed | on-disk | index | fwd ns/entry | \
             rev ns/entry | rev p99 ns |\n\
             |---:|---:|---:|---:|---:|---:|---:|---:|\n"
                .into(),
        );
        for interval in INTERVALS {
            for block_size in BLOCK_SIZES {
                let mut ratios: Vec<[f64; 6]> = Vec::new();
                let mut p99s: Vec<[f64; 2]> = Vec::new();
                for run in 0..RUNS {
                    let cells = measure(shape, keys, interval, block_size);
                    let (l, d) = (&cells[0], &cells[1]);
                    for c in [l, d] {
                        csv.push_str(&format!(
                            "{},{},{},{},{},{},{},{},{},{},{},{:.2},{:.2},{},{},{}\n",
                            c.shape,
                            c.interval,
                            c.block_size,
                            if c.delta { "delta" } else { "legacy" },
                            run,
                            c.bytes.blocks,
                            c.bytes.decompressed,
                            c.bytes.on_disk_data,
                            c.bytes.index,
                            c.bytes.file,
                            c.timing.entries,
                            c.timing.forward_ns as f64 / c.timing.entries as f64,
                            c.timing.reverse_ns as f64 / c.timing.entries as f64,
                            c.timing.reverse_p99_ns,
                            c.timing.cache_hits,
                            c.timing.cache_misses,
                        ));
                    }
                    ratios.push([
                        d.bytes.decompressed as f64 / l.bytes.decompressed as f64,
                        d.bytes.on_disk_data as f64 / l.bytes.on_disk_data as f64,
                        d.bytes.index as f64 / l.bytes.index as f64,
                        d.timing.forward_ns as f64 / l.timing.forward_ns as f64,
                        d.timing.reverse_ns as f64 / l.timing.reverse_ns as f64,
                        d.bytes.blocks as f64 / l.bytes.blocks as f64,
                    ]);
                    p99s.push([
                        l.timing.reverse_p99_ns as f64,
                        d.timing.reverse_p99_ns as f64,
                    ]);
                }
                let m = |i: usize| median(ratios.iter().map(|r| r[i]).collect());
                let p99_l = median(p99s.iter().map(|p| p[0]).collect());
                let p99_d = median(p99s.iter().map(|p| p[1]).collect());
                summary.push(format!(
                    "| {interval} | {} KiB | {:.3}x | {:.3}x | {:.3}x | {:.3}x | {:.3}x | \
                     {p99_l:.0} -> {p99_d:.0} |\n",
                    block_size >> 10,
                    m(0),
                    m(1),
                    m(2),
                    m(3),
                    m(4),
                ));
                println!(
                    "{shape} r={interval} b={}KiB  decompressed {:.3}x  on-disk {:.3}x  \
                     index {:.3}x  fwd {:.3}x  rev {:.3}x  blocks {:.3}x",
                    block_size >> 10,
                    m(0),
                    m(1),
                    m(2),
                    m(3),
                    m(4),
                    m(5),
                );
            }
        }
    }

    // Merge-scan (buffered vs pinned key) comparison, the acceptance item that
    // cannot be argued away.
    summary.push("\n### merge scan: pinned (legacy) vs buffered (delta) key\n\n".into());
    for (shape, keys) in &corpora {
        let sample: Vec<Vec<u8>> = keys.iter().take(60_000).cloned().collect();
        let mut ratios = Vec::new();
        let (mut ln, mut dn) = (0u64, 0u64);
        for _ in 0..RUNS {
            let (l, n) = merge_scan_ns(false, &sample);
            let (d, _) = merge_scan_ns(true, &sample);
            ln = l;
            dn = d;
            ratios.push(d as f64 / l as f64);
            let _ = n;
        }
        let r = median(ratios.clone());
        summary.push(format!(
            "- **{shape}**: delta/legacy merge-scan time **{r:.3}x** \
             (median of {RUNS}; last pair {ln} ns vs {dn} ns)\n"
        ));
        println!("{shape} merge scan delta/legacy = {r:.3}x");
    }

    std::fs::write(out_dir.join("sweep.csv"), &csv).unwrap();
    std::fs::write(out_dir.join("sweep-summary.md"), summary.concat()).unwrap();
    println!("wrote {}", out_dir.display());
}
