//! Bloom auto-allocation (`ColumnFamilyConfig::bloom_auto_allocate`, wavesdb
//! `BloomAutoAllocate`, plan C P7): tables written into upper levels get
//! geometrically stronger filters, the bottom level gets `bloom_fpr`.
//!
//! Every assertion is on the filter a table actually carries on disk — its bit
//! count — against the exact size the writer computes for its entry count at
//! the rate the policy names.

use std::sync::Arc;
use std::time::Duration;

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::default_comparator;
use ondadb::config::bloom_auto_fpr;
use ondadb::sst::Reader;
use ondadb::{ColumnFamily, ColumnFamilyConfig, LocalStorage, Options, DB};

const BASE: f64 = 0.01;
const RATIO: u64 = 4;

/// The writer's sizing rule (`bloom::bloom_params`): the bit count for `n`
/// keys at rate `fpr`.
fn expected_bits(n: u64, fpr: f64) -> u64 {
    let ln2 = std::f64::consts::LN_2;
    let mf = -(n.max(1) as f64) * fpr.ln() / (ln2 * ln2);
    (mf as u64 + 1).clamp(64, u64::from(u32::MAX))
}

fn cfg(auto: bool) -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        bloom_fpr: BASE,
        bloom_auto_allocate: auto,
        level_size_ratio: RATIO,
        write_buffer_size: 64 << 10,
        target_file_size: 32 << 10,
        l1_base_bytes: 96 << 10,
        l1_file_count_trigger: 2,
        soft_pending_compaction_bytes: 0,
        hard_pending_compaction_bytes: 0,
        ..ColumnFamilyConfig::default()
    }
}

/// `(level, bloom bits, entries)` of every live table of `cf`.
fn filters(db: &DB, dir: &std::path::Path, cf: &str) -> Vec<(u32, u64, u64)> {
    let cache = Arc::new(BlockCache::new(0));
    db.live_sstables()
        .into_iter()
        .filter(|t| t.cf == cf)
        .map(|t| {
            let path = dir
                .join(ondadb::format::cf_dir_name(cf))
                .join(format!("{}.klog", t.id));
            let r = Reader::open(
                path.to_str().unwrap(),
                LocalStorage::new(Arc::new(FileCache::new(8)), false),
                cache.clone(),
                t.id,
                default_comparator(),
                0,
            )
            .unwrap();
            (t.level, r.bloom_bits().expect("a filter"), r.num_entries())
        })
        .collect()
}

fn deepest(db: &DB, cf: &str) -> u32 {
    db.live_sstables()
        .iter()
        .filter(|t| t.cf == cf)
        .map(|t| t.level)
        .max()
        .unwrap_or(0)
}

fn wait_quiet(db: &DB, cf: &Arc<ColumnFamily>) {
    for _ in 0..4000 {
        if !cf.is_compacting() && !cf.is_flushing() && db.pending_flushes_for_tests() == 0 {
            std::thread::sleep(Duration::from_millis(20));
            if !cf.is_compacting() {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("compaction never settled");
}

/// Write `batches` flushed tables of unique keys (so a table's entry count is
/// the key count its filter was sized for).
fn fill(db: &DB, cf: &Arc<ColumnFamily>, from: &mut u64, batches: usize) {
    let value = vec![0x5A; 100];
    for _ in 0..batches {
        for _ in 0..400 {
            db.put(
                cf,
                format!("k{:08}", *from).as_bytes(),
                &value,
                Duration::ZERO,
            )
            .unwrap();
            *from += 1;
        }
        db.flush_memtable(cf).unwrap();
    }
}

#[test]
fn levels_get_geometric_filters() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("auto", cfg(true)).unwrap();
    let mut next = 0;
    // Grow until at least three levels exist (L0 and two below it).
    for _ in 0..40 {
        fill(&db, &cf, &mut next, 4);
        wait_quiet(&db, &cf);
        if deepest(&db, "auto") >= 2 {
            break;
        }
    }
    let bottom = deepest(&db, "auto");
    assert!(bottom >= 2, "the family never deepened (bottom L{bottom})");

    // One more flush with the depth now known: its L0 table's rate is fixed.
    fill(&db, &cf, &mut next, 1);
    let tables = filters(&db, dir.path(), "auto");
    let l0: Vec<_> = tables.iter().filter(|t| t.0 == 0).collect();
    assert!(!l0.is_empty(), "the last flush was compacted away");
    let want_l0 = bloom_auto_fpr(BASE, RATIO, 0, bottom);
    assert!((want_l0 - BASE / (RATIO as f64).powi(bottom as i32)).abs() < 1e-15);
    assert!(
        l0.iter()
            .any(|&&(_, bits, n)| bits == expected_bits(n, want_l0)),
        "no L0 table carries the {want_l0} filter: {l0:?}"
    );

    let mut saw_upper = false;
    for &(level, bits, n) in &tables {
        if level == bottom {
            // The bottom is always written as bottom output: `bloom_fpr`.
            assert_eq!(bits, expected_bits(n, BASE), "bottom L{level}, {n} keys");
            continue;
        }
        // An upper table was written when the family was at most as deep as
        // it is now: its rate is `BASE × RATIO^(level − d)` for the depth `d`
        // it saw, which is never below its own level.
        let ok = (level..=bottom)
            .any(|d| bits == expected_bits(n, bloom_auto_fpr(BASE, RATIO, level, d)));
        assert!(ok, "L{level} table: {bits} bits for {n} keys fits no depth");
        // And it is strictly stronger than a uniform filter whenever the
        // family was deeper than this level when it was written.
        if bits > expected_bits(n, BASE) {
            saw_upper = true;
        }
    }
    assert!(
        saw_upper,
        "no upper-level table got a stronger filter: {tables:?}"
    );

    // Bits per key, the Monkey shape: L0 > bottom.
    let per_key = |level: u32| {
        let (b, n) = tables
            .iter()
            .filter(|t| t.0 == level)
            .fold((0u64, 0u64), |(b, n), t| (b + t.1, n + t.2));
        b as f64 / n.max(1) as f64
    };
    assert!(per_key(0) > per_key(bottom) * 1.3, "{tables:?}");

    // Reopen: the policy is persisted (config TLV tag 33).
    db.close().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert!(db.column_family_config("auto").unwrap().bloom_auto_allocate);
}

#[test]
fn uniform_policy_is_unchanged_without_auto() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("flat", cfg(false)).unwrap();
    let mut next = 0;
    for _ in 0..40 {
        fill(&db, &cf, &mut next, 4);
        wait_quiet(&db, &cf);
        if deepest(&db, "flat") >= 2 {
            break;
        }
    }
    fill(&db, &cf, &mut next, 1);
    for (level, bits, n) in filters(&db, dir.path(), "flat") {
        assert_eq!(bits, expected_bits(n, BASE), "L{level}, {n} keys");
    }
}

#[test]
fn auto_with_a_vector_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let both = ColumnFamilyConfig {
        bloom_fpr_per_level: vec![0.001, 0.01],
        ..cfg(true)
    };
    assert!(db.create_column_family("both", both).is_err());
}
