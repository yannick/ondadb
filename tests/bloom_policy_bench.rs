//! 0.1 acceptance benchmark: per-level Bloom policy.
//!
//! Three arms over identical data, differing only in the filter policy:
//!
//! - `uniform` — today's behaviour: one `bloom_fpr` for every level.
//! - `per_level` — a strong filter for the small upper levels, a weak one for
//!   the bottom.
//! - `hits` — `per_level` plus `optimize_filters_for_hits`, so bottom
//!   compaction output carries no filter at all.
//!
//! Each arm runs a **miss-heavy** and a **hit-heavy** point-read phase and
//! reports p50/p99 latency beside `DB::reader_memory()`'s resident filter bytes
//! and the 0.10 PerfContext counters that explain the number. Ignored by
//! default; run it with
//!
//! ```sh
//! cargo test --release --test bloom_policy_bench -- --ignored --nocapture
//! ```
//!
//! All three arms run in one process over freshly built databases, so page
//! cache, allocator state and clock domain are shared. The machine is
//! thermally noisy (see `docs/performance.md`): compare same-invocation ratios
//! between arms, never absolute numbers across sessions.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, DB};

/// Distinct keys written. Even indices are stored, so the odd ones between them
/// are absent probes that still fall inside every table's key span.
const KEYS: u32 = 200_000;
/// Probes per phase.
const PROBES: u32 = 50_000;
const VALUE_LEN: usize = 96;

fn key(i: u32) -> Vec<u8> {
    format!("key{i:013}").into_bytes()
}

fn base_config() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        // The data has to actually spread over three levels, or a per-level
        // policy has nothing to be per-level about. Small output files are what
        // makes level 1 *retain* some: background compaction pushes one file at
        // a time and stops as soon as the level fits its capacity, so a level
        // whose whole content is one file always empties itself.
        target_file_size: 2 << 20,
        l1_base_bytes: 10 << 20,
        l1_file_count_trigger: 4,
        ..ColumnFamilyConfig::default()
    }
}

fn wait_quiescent(cf: &Arc<ColumnFamily>) {
    let deadline = Instant::now() + Duration::from_secs(300);
    while Instant::now() < deadline {
        let stats = cf.stats();
        if stats.levels[0].0 == 0 && !cf.is_compacting() && stats.compaction_debt == 0 {
            // Confirm it is stable, not merely between jobs.
            std::thread::sleep(Duration::from_millis(200));
            let again = cf.stats();
            if again.levels[0].0 == 0 && !cf.is_compacting() {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("compaction never settled");
}

fn build(dir: &std::path::Path, config: ColumnFamilyConfig) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db.create_column_family("bench", config).unwrap();
    let value = vec![b'v'; VALUE_LEN];
    const BATCH: u32 = 25_000;
    let mut i = 0;
    while i < KEYS {
        for j in i..(i + BATCH).min(KEYS) {
            db.put(&cf, &key(j * 2), &value, Duration::ZERO).unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        i += BATCH;
    }
    wait_quiescent(&cf);

    // Overlay pass. Without it every level ends up owning a *disjoint* slice of
    // the keyspace, so a lookup has exactly one candidate table and a per-level
    // policy has no cascade to shorten — an artifact of writing once and
    // compacting once, not the steady state. Rewriting a scattered 5% spanning
    // the whole range puts a thin, full-width level above the bottom, which is
    // what continuous ingestion produces.
    for pass in 0..4u32 {
        let mut j = pass;
        while j < KEYS {
            db.put(&cf, &key(j * 2), &value, Duration::ZERO).unwrap();
            j += 80;
        }
        db.flush_memtable(&cf).unwrap();
    }
    wait_quiescent(&cf);
    (db, cf)
}

/// `(p50 ns, p99 ns, PerfContext)` over `PROBES` lookups.
fn phase(db: &DB, cf: &Arc<ColumnFamily>, hit: bool) -> (u64, u64, ondadb::perf::PerfContext) {
    let step = (KEYS * 2 / PROBES).max(1);
    let mut samples: Vec<u64> = Vec::with_capacity(PROBES as usize);
    let scope = ondadb::perf::enter();
    for n in 0..PROBES {
        // Hit-heavy probes an even (stored) key; miss-heavy the odd key beside
        // it, which is inside every candidate table's span but absent.
        let k = key(n * step + u32::from(!hit));
        let started = Instant::now();
        let found = db.get(cf, &k).is_ok();
        samples.push(started.elapsed().as_nanos() as u64);
        assert_eq!(found, hit, "probe {n} disagreed with the phase");
    }
    let counters = scope.finish();
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p99 = samples[samples.len() * 99 / 100];
    (p50, p99, counters)
}

fn arm(name: &str, config: ColumnFamilyConfig) {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = build(dir.path(), config);
    let levels: Vec<usize> = cf.stats().levels.iter().map(|(n, _)| *n).collect();

    // Warm the reader cache so the residency figure below covers every table
    // the probes actually touch.
    let (_, _, _) = phase(&db, &cf, false);

    let (miss_p50, miss_p99, miss) = phase(&db, &cf, false);
    let (hit_p50, hit_p99, hits) = phase(&db, &cf, true);
    let (resident, index_bytes, filter_bytes, readers, _entries) = db.reader_memory();

    println!(
        "arm={name} levels={levels:?} readers={readers} resident_bytes={resident} \
         index_bytes={index_bytes} filter_bytes={filter_bytes}"
    );
    println!(
        "  miss p50={miss_p50}ns p99={miss_p99}ns probes={} negatives={} sstable_probes={} \
         block_misses={} block_cache_hits={}",
        miss.bloom_probes,
        miss.bloom_negatives,
        miss.sstable_probes,
        miss.block_misses,
        miss.block_cache_hits
    );
    println!(
        "  hit  p50={hit_p50}ns p99={hit_p99}ns probes={} negatives={} sstable_probes={} \
         block_misses={} block_cache_hits={}",
        hits.bloom_probes,
        hits.bloom_negatives,
        hits.sstable_probes,
        hits.block_misses,
        hits.block_cache_hits
    );
    db.close().unwrap();
}

#[test]
#[ignore = "acceptance benchmark; run explicitly with --ignored --nocapture"]
fn bloom_policy_miss_and_hit_phases() {
    arm("uniform", base_config());
    // Aimed at the SAME resident filter bytes as `uniform`: bits per key go as
    // -ln(fpr)/ln(2)^2, so buying 10x the strength in the small upper levels is
    // paid for by relaxing the bottom, where nearly all the keys are. This is
    // the arm the miss-heavy acceptance bar is about.
    arm(
        "per_level_equal",
        ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.001, 0.02],
            ..base_config()
        },
    );
    // The other direction: spend fewer bytes overall and accept a worse
    // aggregate FP rate.
    arm(
        "per_level_lean",
        ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.005, 0.05],
            ..base_config()
        },
    );
    arm(
        "hits",
        ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.001, 0.02],
            optimize_filters_for_hits: true,
            ..base_config()
        },
    );
}
