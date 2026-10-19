//! Background reads do not admit into the block cache (wavesdb `ac16c8a`,
//! plan C P2).
//!
//! A compaction reads every block of its inputs exactly once. Admitting those
//! blocks can only evict ones a foreground reader wants, so by default
//! compaction reads *through* the cache without inserting. The fixture: a small
//! hot family whose blocks a foreground reader has just loaded, and a cold
//! family many times the cache's size that is then compacted. The hot set must
//! come out of the compaction still resident — measured with the cache's own
//! counters, not with timing.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Compression, Options, DB};

const CACHE_BYTES: usize = 256 << 10;
const HOT_KEYS: u32 = 200;
const COLD_KEYS: u32 = 20_000;

fn cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        // Compressed on purpose: under `mmap-reads` an uncompressed block is
        // served straight from the mapping and never touches the block cache,
        // so only compressed blocks exercise the admission policy in every
        // build configuration.
        compression: Compression::Lz4,
        compression_per_level: Vec::new(),
        // No background compaction: the manual one below must be the only
        // background reader, or it races the warm-up measurement.
        l1_file_count_trigger: 64,
        ..ColumnFamilyConfig::default()
    }
}

/// A value that does not compress away, so the cold family's decompressed
/// blocks really are many times the cache.
fn value(i: u32) -> Vec<u8> {
    let mut x = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..100)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn read_hot(db: &DB, hot: &Arc<ColumnFamily>) {
    for i in 0..HOT_KEYS {
        let got = db.get(hot, format!("hot{i:05}").as_bytes()).unwrap();
        assert_eq!(got, value(i));
    }
}

/// Foreground misses and cache evictions caused by compacting the cold
/// family, measured around a re-read of the (already loaded) hot set.
fn hot_set_damage(admit: bool) -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.block_cache_size = CACHE_BYTES;
    opts.admit_background_scan_blocks = admit;
    let db = DB::open(opts).unwrap();
    let hot = db.create_column_family("hot", cfg()).unwrap();
    let cold = db.create_column_family("cold", cfg()).unwrap();

    for i in 0..HOT_KEYS {
        db.put(&hot, format!("hot{i:05}").as_bytes(), &value(i), Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&hot).unwrap();
    // Several overlapping L0 tables, so the manual compaction merges them.
    for round in 0..4u32 {
        for i in (round..COLD_KEYS).step_by(4) {
            db.put(&cold, format!("cold{i:07}").as_bytes(), &value(i), Duration::ZERO)
                .unwrap();
        }
        db.flush_memtable(&cold).unwrap();
    }

    // Load the hot set, twice: the second pass must be all hits, which proves
    // it fits the cache and makes the measurement below meaningful.
    read_hot(&db, &hot);
    let before = db.stats();
    read_hot(&db, &hot);
    let warm = db.stats();
    assert_eq!(
        warm.block_cache_misses, before.block_cache_misses,
        "the hot set must fit the cache for this test to mean anything"
    );

    db.compact(&cold).unwrap();
    let after_compaction = db.stats();
    read_hot(&db, &hot);
    let after = db.stats();
    db.close().unwrap();
    (
        after.block_cache_misses - after_compaction.block_cache_misses,
        after_compaction.block_cache_evictions - warm.block_cache_evictions,
    )
}

#[test]
fn compaction_does_not_evict_the_foreground_hot_set() {
    let (misses, evictions) = hot_set_damage(false);
    assert_eq!(evictions, 0, "compaction admitted blocks into the cache");
    assert_eq!(misses, 0, "the hot set lost blocks to a compaction");
}

/// The control: with admission switched back on, the same compaction flushes
/// the hot set. Without this the test above could pass because the fixture
/// never pressured the cache at all.
#[test]
fn admitting_background_reads_evicts_the_hot_set() {
    let (misses, evictions) = hot_set_damage(true);
    assert!(evictions > 0, "the cold compaction never pressured the cache");
    assert!(misses > 0, "the hot set survived a cache-cycling compaction");
}
