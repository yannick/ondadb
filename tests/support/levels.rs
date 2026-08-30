//! Deterministic overlapping-level fixture generator (feature 0.2).
//!
//! One definition of "a column family whose levels overlap like *this*", so
//! the picker's benchmark and the level-shaped tests share a workload instead
//! of each inventing one. 0.1's multi-level benchmark and 0.8's
//! large-bounded-job phase are the other intended consumers.
//!
//! Everything here is a pure function of [`LevelGeometry`], including the
//! seed: the same geometry twice produces the same `SstMeta` sequence (ids,
//! key spans, sizes), which is what `level_fixture_is_deterministic` pins.
//!
//! Shared verbatim between `tests/` and the in-crate benchmark in
//! `src/compaction.rs`, so it names everything through the crate's public
//! path.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use ondadb::manifest::{manifest_path, Manifest, SstMeta};
use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, DB};

/// Deterministic 64-bit generator. `rand`'s stream is not a stable contract
/// across versions and this fixture's whole point is reproducibility.
#[derive(Debug)]
pub struct Lcg(u64);

impl Lcg {
    pub fn new(seed: u64) -> Lcg {
        Lcg(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

/// The shape of the fixture: how many tables, how wide each one's key window
/// is, and how big its entries are.
#[derive(Debug, Clone)]
pub struct LevelGeometry {
    pub seed: u64,
    /// Flushed tables to produce — one per batch.
    pub batches: usize,
    pub keys_per_batch: usize,
    pub value_len: usize,
    /// How narrow each batch's key window is: the window spans
    /// `key_space >> overlap_skew` keys. `0` means every batch spans the whole
    /// keyspace, so every push-down rewrites the level below it whole — the
    /// worst case for a first-fit picker, and the case where a ratio picker
    /// has nothing cheaper to find either. Larger values give mostly-disjoint
    /// windows with a few wide ones, which is where the two pickers diverge.
    pub overlap_skew: u32,
    pub key_space: u64,
    /// How much larger a record in the top quarter of the keyspace is than one
    /// elsewhere. `1` gives uniform record sizes. See [`value_len_for`].
    ///
    /// [`value_len_for`]: LevelGeometry::value_len_for
    pub heavy_value_ratio: usize,
}

impl Default for LevelGeometry {
    fn default() -> Self {
        LevelGeometry {
            seed: 0xC0FF_EE00_1234_5678,
            batches: 40,
            keys_per_batch: 20_000,
            value_len: 96,
            overlap_skew: 3,
            key_space: 4_000_000,
            heavy_value_ratio: 8,
        }
    }
}

impl LevelGeometry {
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// The key batches, in write order. Each batch draws `keys_per_batch` keys
    /// from a seed-placed window whose width is `key_space >> overlap_skew`
    /// narrowed by a seed-chosen factor, so windows overlap partially and
    /// unevenly — the geometry that makes one source table cheap to push down
    /// and its neighbour expensive.
    pub fn batches(&self) -> Vec<Vec<Vec<u8>>> {
        let widest = (self.key_space >> self.overlap_skew).max(1);
        let mut rng = Lcg::new(self.seed);
        let mut out = Vec::with_capacity(self.batches);
        for _ in 0..self.batches {
            // Window widths span three orders of magnitude, so the keyspace
            // ends up with dense stretches beside sparse ones. A
            // uniform-width fixture gives every candidate the same overlap
            // ratio and leaves the picker nothing to choose between — that
            // measures the absence of the effect, not its size.
            let width = (widest >> (rng.below(4) * 3)).max(1);
            let base = rng.below(self.key_space.saturating_sub(width).max(1));
            let mut keys = Vec::with_capacity(self.keys_per_batch);
            for _ in 0..self.keys_per_batch {
                let k = base + rng.below(width);
                keys.push(format!("k{k:012}").into_bytes());
            }
            out.push(keys);
        }
        out
    }

    /// Value length for `key`: records in the top quarter of the keyspace are
    /// `heavy_value_ratio` times larger than the rest.
    ///
    /// This is what actually spreads the overlap ratios apart. Key counts
    /// alone equalize across a settled tree, so every candidate ends up with
    /// roughly `level_size_ratio` bytes below it per byte of its own. Making
    /// *bytes per key* vary by region means a source table in the light region
    /// pushes down over a thin target span while its neighbour in the heavy
    /// one sits over several times as many bytes — a difference only a
    /// byte-weighted ratio can see. Variable-size records are also the normal
    /// case in a store that separates values.
    pub fn value_len_for(&self, key: &[u8]) -> usize {
        let n: u64 = std::str::from_utf8(&key[1..])
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if n * 4 >= self.key_space * 3 {
            self.value_len * self.heavy_value_ratio
        } else {
            self.value_len
        }
    }

    pub fn value_for(&self, key: &[u8]) -> Vec<u8> {
        vec![b'v'; self.value_len_for(key)]
    }

    /// User bytes handed to the engine — the denominator of write
    /// amplification.
    pub fn ingested_bytes(&self) -> u64 {
        self.batches()
            .iter()
            .flatten()
            .map(|k| (k.len() + self.value_len_for(k)) as u64)
            .sum()
    }

    /// A config that never triggers background compaction, so the fixture's
    /// flushed tables survive exactly as written. Used by the determinism
    /// test, and as the "no compaction" control.
    pub fn quiescent_config(&self) -> ColumnFamilyConfig {
        ColumnFamilyConfig {
            write_buffer_size: 512 << 20,
            l1_file_count_trigger: 1 << 20,
            l1_base_bytes: 1 << 60,
            ..ColumnFamilyConfig::default()
        }
    }

    /// A config with a real multi-level geometry: small tables, a small L1 and
    /// a shallow level ratio, so L1..L3 are all populated and levels >= 1 —
    /// the only branch 0.2 changes — do most of the compaction. L1 holds
    /// `l1_base_bytes / target_file_size` = 8 tables, enough for the picker to
    /// have candidates to choose between.
    pub fn compacting_config(&self) -> ColumnFamilyConfig {
        ColumnFamilyConfig {
            write_buffer_size: 4 << 20,
            target_file_size: 1 << 20,
            l1_base_bytes: 8 << 20,
            level_size_ratio: 4,
            l1_file_count_trigger: 4,
            // The benchmark measures the picker, not the pacer: let ingest run
            // and let the debt be paid at close.
            soft_pending_compaction_bytes: 0,
            hard_pending_compaction_bytes: 0,
            ..ColumnFamilyConfig::default()
        }
    }

    /// Write every batch, flushing after each so one batch becomes one table.
    pub fn write(&self, db: &DB, cf: &Arc<ColumnFamily>) {
        for batch in self.batches() {
            for key in &batch {
                db.put(cf, key, &self.value_for(key), Duration::ZERO)
                    .unwrap();
            }
            db.flush_memtable(cf).unwrap();
        }
    }

    /// Build the fixture in `dir` with compaction disabled and return the
    /// tables it produced, in manifest order.
    pub fn materialize_quiescent(&self, dir: &std::path::Path) -> Vec<SstMeta> {
        {
            let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
            let cf = db
                .create_column_family("fixture", self.quiescent_config())
                .unwrap();
            self.write(&db, &cf);
            db.close().unwrap();
        }
        tables(dir, "fixture")
    }
}

/// The tables the manifest at `dir` records for column family `cf`.
pub fn tables(dir: &std::path::Path, cf: &str) -> Vec<SstMeta> {
    Manifest::load(manifest_path(dir))
        .unwrap()
        .cfs
        .into_iter()
        .find(|c| c.name == cf)
        .map(|c| c.sstables)
        .unwrap_or_default()
}

/// One table's identity, as a determinism check compares it:
/// `(id, level, min_key, max_key, klog_size, vlog_size)`.
pub type TableFingerprint = (u64, u32, Vec<u8>, Vec<u8>, u64, u64);

/// The identity a determinism check compares: everything about a table that a
/// deterministic generator must reproduce. `max_entry_time` is deliberately
/// excluded — it is wall-clock, and no generator can hold it fixed.
pub fn fingerprint(metas: &[SstMeta]) -> Vec<TableFingerprint> {
    metas
        .iter()
        .map(|m| {
            (
                m.id,
                m.level,
                m.min_key.clone(),
                m.max_key.clone(),
                m.klog_size,
                m.vlog_size,
            )
        })
        .collect()
}
