//! Configuration types for ondaDB.
//!


use std::time::Duration;

/// Compression algorithm applied per SSTable block (never to the WAL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Snappy = 1,
    Lz4 = 2,
    Zstd = 3,
    Lz4Fast = 4,
    Flate = 5,
}

impl Compression {
    /// Parse the lowercase names used by the benchmark harness / config files.
    pub fn parse(name: &str) -> Option<Compression> {
        Some(match name.to_ascii_lowercase().as_str() {
            "none" => Compression::None,
            "snappy" => Compression::Snappy,
            "lz4" => Compression::Lz4,
            "zstd" => Compression::Zstd,
            "lz4fast" | "lz4_fast" => Compression::Lz4Fast,
            "flate" | "deflate" => Compression::Flate,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
            Compression::Lz4Fast => "lz4fast",
            Compression::Flate => "flate",
        }
    }

    pub fn from_u8(v: u8) -> Option<Compression> {
        Some(match v {
            0 => Compression::None,
            1 => Compression::Snappy,
            2 => Compression::Lz4,
            3 => Compression::Zstd,
            4 => Compression::Lz4Fast,
            5 => Compression::Flate,
            _ => return None,
        })
    }
}

/// WAL durability mode (mirrors `TDB_SYNC_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Rely on the OS page cache (fastest, least durable).
    None,
    /// `fsync` after every commit.
    Full,
    /// Background `fsync` on a fixed interval.
    Interval,
}

impl SyncMode {
    pub fn from_u8(v: u8) -> Option<SyncMode> {
        Some(match v {
            0 => SyncMode::None,
            1 => SyncMode::Full,
            2 => SyncMode::Interval,
            _ => return None,
        })
    }
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Reads observe the latest committed sequence at read time; no conflict
    /// detection on commit.
    ReadUncommitted,
    /// Reads observe the latest committed sequence at read time; no conflict
    /// detection on commit. (The default for the single-op helpers.)
    ReadCommitted,
    /// Reads are pinned to a snapshot taken at `begin`; no conflict detection on
    /// commit.
    RepeatableRead,
    /// Snapshot isolation: reads are pinned to the `begin` snapshot and commit
    /// aborts with [`Conflict`](crate::OndaError::Conflict) on a write-write
    /// conflict (first-committer-wins). Permits write skew.
    Snapshot,
    /// Snapshot isolation plus validation, on commit, that every key the
    /// transaction *read by point lookup* is unchanged since its snapshot.
    ///
    /// **Not full serializability.** Range/iterator reads are not tracked, so
    /// phantoms (rows inserted into a scanned range by a concurrent committer) are
    /// not detected. Use only point `get`s if you rely on the conflict check.
    /// (TODO:  implement full SSI with rw-antidependency)
    Serializable,
}

/// Logging verbosity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
    None,
}

/// Database-wide configuration. `Options::new(path)` then tweak fields.
#[derive(Debug, Clone)]
pub struct Options {
    pub path: String,
    pub num_flush_threads: usize,
    pub num_compaction_threads: usize,
    pub log_level: LogLevel,
    pub block_cache_size: usize,
    pub max_open_sstables: usize,
    pub max_memory_usage: u64,
    pub read_only: bool,
    pub finish_compactions_on_close: bool,
    pub max_concurrent_flushes: usize,
    pub unified_memtable: bool,
    pub unified_memtable_write_buffer_size: usize,
    pub unified_memtable_skip_list_max_level: u32,
    pub unified_memtable_skip_list_probability: f64,
    pub unified_memtable_sync_mode: SyncMode,
    pub unified_memtable_sync_interval: Duration,
}

impl Options {
    pub fn new(path: impl Into<String>) -> Self {
        Options {
            path: path.into(),
            ..Default::default()
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Options {
            path: String::new(),
            num_flush_threads: 2,
            num_compaction_threads: 2,
            log_level: LogLevel::None,
            block_cache_size: 64 << 20, // 64 MiB
            max_open_sstables: 256,
            max_memory_usage: 0, // 0 => auto (≈75% system memory)
            read_only: false,
            finish_compactions_on_close: false,
            max_concurrent_flushes: 0, // 0 => == num_flush_threads
            unified_memtable: false,
            unified_memtable_write_buffer_size: 64 << 20,
            unified_memtable_skip_list_max_level: 12,
            unified_memtable_skip_list_probability: 0.25,
            unified_memtable_sync_mode: SyncMode::None,
            unified_memtable_sync_interval: Duration::from_micros(128_000),
        }
    }
}

/// Per-column-family configuration.
#[derive(Debug, Clone)]
pub struct ColumnFamilyConfig {
    pub write_buffer_size: usize,
    pub level_size_ratio: u64,
    pub min_levels: u32,
    pub dividing_level_offset: i32,
    pub klog_value_threshold: usize,
    pub compression: Compression,
    /// Per-level override of `compression`. Empty = use `compression` for
    /// every level. Otherwise level L uses `compression_per_level[min(L,
    /// len-1)]` — the last entry repeats for all deeper levels (so
    /// `[None, None, Zstd]` = hot L0/L1 uncompressed, everything below Zstd).
    pub compression_per_level: Vec<Compression>,
    pub enable_bloom_filter: bool,
    pub bloom_fpr: f64,
    pub enable_block_indexes: bool,
    pub index_sample_ratio: u32,
    pub block_index_prefix_len: usize,
    pub sync_mode: SyncMode,
    pub sync_interval: Duration,
    pub comparator_name: String,
    pub comparator_ctx_str: String,
    pub skip_list_max_level: u32,
    pub skip_list_probability: f64,
    pub default_isolation_level: IsolationLevel,
    pub min_disk_space: u64,
    pub l1_file_count_trigger: u32,
    pub l0_queue_stall_threshold: u32,
    pub tombstone_density_trigger: f64,
    pub tombstone_density_min_entries: u64,
    pub use_btree: bool,
}

impl Default for ColumnFamilyConfig {
    fn default() -> Self {
        ColumnFamilyConfig {
            write_buffer_size: 64 << 20, // 64 MiB
            level_size_ratio: 10,
            min_levels: 1,
            dividing_level_offset: 1,
            klog_value_threshold: 512, // WiscKey separation threshold
            compression: Compression::None,
            compression_per_level: Vec::new(),
            enable_bloom_filter: true,
            bloom_fpr: 0.01,
            enable_block_indexes: true,
            index_sample_ratio: 1,
            block_index_prefix_len: 16,
            sync_mode: SyncMode::None,
            sync_interval: Duration::from_micros(128_000),
            comparator_name: "memcmp".to_string(),
            comparator_ctx_str: String::new(),
            skip_list_max_level: 12,
            skip_list_probability: 0.25,
            default_isolation_level: IsolationLevel::ReadCommitted,
            min_disk_space: 100 << 20, // 100 MiB
            l1_file_count_trigger: 4,
            l0_queue_stall_threshold: 20,
            tombstone_density_trigger: 0.0, // disabled
            tombstone_density_min_entries: 0,
            use_btree: false,
        }
    }
}

impl ColumnFamilyConfig {
    /// Compression algorithm for SSTables written at `level` (see
    /// `compression_per_level`).
    pub fn compression_for_level(&self, level: u32) -> Compression {
        match self.compression_per_level.as_slice() {
            [] => self.compression,
            v => v[(level as usize).min(v.len() - 1)],
        }
    }

    /// Serialize the durable subset of the config for the manifest blob.
    pub fn encode(&self) -> Vec<u8> {
        use crate::encoding::{append_u32, append_u64, append_uvarint};
        let mut b = Vec::new();
        append_uvarint(&mut b, self.comparator_name.len() as u64);
        b.extend_from_slice(self.comparator_name.as_bytes());
        b.push(self.compression as u8);
        append_u64(&mut b, self.write_buffer_size as u64);
        append_u64(&mut b, self.level_size_ratio);
        append_u64(&mut b, self.klog_value_threshold as u64);
        b.push(u8::from(self.enable_bloom_filter));
        append_u64(&mut b, self.bloom_fpr.to_bits());
        append_u32(&mut b, self.l1_file_count_trigger);
        append_u32(&mut b, self.l0_queue_stall_threshold);
        b.push(u8::from(self.use_btree));
        // Appended after the original durable subset: manifests written by older
        // versions lack these trailing bytes, so `decode_into` (which stops early
        // on a short blob via `?`) reconstructs them as the struct defaults —
        // backward compatible in both directions.
        b.push(self.sync_mode as u8);
        append_u64(&mut b, self.sync_interval.as_micros() as u64);
        b.push(self.compression_per_level.len().min(255) as u8);
        for c in self.compression_per_level.iter().take(255) {
            b.push(*c as u8);
        }
        b
    }

    /// Reconstruct a config from a manifest blob; unknown/short blobs fall back
    /// to defaults (preserving at least the comparator name when present).
    pub fn decode(blob: &[u8]) -> ColumnFamilyConfig {
        let mut cfg = ColumnFamilyConfig::default();
        decode_into(blob, &mut cfg);
        cfg
    }
}

fn decode_into(mut p: &[u8], cfg: &mut ColumnFamilyConfig) -> Option<()> {
    use crate::encoding::{read_u32, read_u64, uvarint};
    let (nlen, n) = uvarint(p)?;
    p = &p[n..];
    let nlen = nlen as usize;
    if p.len() < nlen {
        return None;
    }
    cfg.comparator_name = String::from_utf8_lossy(&p[..nlen]).into_owned();
    p = &p[nlen..];

    let byte = |p: &mut &[u8]| -> Option<u8> {
        let b = *p.first()?;
        *p = &p[1..];
        Some(b)
    };
    let u64v = |p: &mut &[u8]| -> Option<u64> {
        if p.len() < 8 {
            return None;
        }
        let v = read_u64(p);
        *p = &p[8..];
        Some(v)
    };
    let u32v = |p: &mut &[u8]| -> Option<u32> {
        if p.len() < 4 {
            return None;
        }
        let v = read_u32(p);
        *p = &p[4..];
        Some(v)
    };

    if let Some(c) = Compression::from_u8(byte(&mut p)?) {
        cfg.compression = c;
    }
    cfg.write_buffer_size = u64v(&mut p)? as usize;
    cfg.level_size_ratio = u64v(&mut p)?;
    cfg.klog_value_threshold = u64v(&mut p)? as usize;
    cfg.enable_bloom_filter = byte(&mut p)? != 0;
    cfg.bloom_fpr = f64::from_bits(u64v(&mut p)?);
    cfg.l1_file_count_trigger = u32v(&mut p)?;
    cfg.l0_queue_stall_threshold = u32v(&mut p)?;
    cfg.use_btree = byte(&mut p)? != 0;
    // Trailing fields added later; an older manifest ends here and keeps the
    // struct defaults for these (the `?` returns before overwriting them).
    if let Some(sm) = SyncMode::from_u8(byte(&mut p)?) {
        cfg.sync_mode = sm;
    }
    cfg.sync_interval = std::time::Duration::from_micros(u64v(&mut p)?);
    let n_levels = byte(&mut p)?;
    let mut per_level = Vec::with_capacity(n_levels as usize);
    for _ in 0..n_levels {
        per_level.push(Compression::from_u8(byte(&mut p)?)?);
    }
    cfg.compression_per_level = per_level;
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cf_config_encode_decode() {
        let c = ColumnFamilyConfig {
            comparator_name: "uint64".into(),
            compression: Compression::Zstd,
            write_buffer_size: 123456,
            enable_bloom_filter: false,
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&c.encode());
        assert_eq!(d.comparator_name, "uint64");
        assert!(d.compression_per_level.is_empty());
        assert_eq!(d.compression, Compression::Zstd);
        assert_eq!(d.write_buffer_size, 123456);
        assert!(!d.enable_bloom_filter);
    }

    #[test]
    fn cf_defaults() {
        let c = ColumnFamilyConfig::default();
        assert_eq!(c.write_buffer_size, 64 << 20);
        assert_eq!(c.level_size_ratio, 10);
        assert_eq!(c.klog_value_threshold, 512);
        assert_eq!(c.bloom_fpr, 0.01);
        assert_eq!(c.skip_list_max_level, 12);
        assert_eq!(c.skip_list_probability, 0.25);
        assert_eq!(c.l1_file_count_trigger, 4);
        assert_eq!(c.comparator_name, "memcmp");
    }

    #[test]
    fn db_defaults() {
        let o = Options::new("/tmp/x");
        assert_eq!(o.path, "/tmp/x");
        assert_eq!(o.block_cache_size, 64 << 20);
        assert_eq!(o.max_open_sstables, 256);
        assert_eq!(o.num_flush_threads, 2);
    }

    #[test]
    fn compression_roundtrip() {
        for c in [
            Compression::None,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
            Compression::Lz4Fast,
            Compression::Flate,
        ] {
            assert_eq!(Compression::parse(c.as_str()), Some(c));
            assert_eq!(Compression::from_u8(c as u8), Some(c));
        }
    }

    #[test]
    fn sync_mode_roundtrip() {
        for sm in [SyncMode::None, SyncMode::Full, SyncMode::Interval] {
            assert_eq!(SyncMode::from_u8(sm as u8), Some(sm));
        }
    }

    #[test]
    fn cf_config_persists_sync_mode_and_interval() {
        for sm in [SyncMode::Full, SyncMode::Interval] {
            let c = ColumnFamilyConfig {
                sync_mode: sm,
                sync_interval: Duration::from_micros(250_000),
                ..ColumnFamilyConfig::default()
            };
            let d = ColumnFamilyConfig::decode(&c.encode());
            assert_eq!(d.sync_mode, sm, "sync_mode must survive a manifest round-trip");
            assert_eq!(d.sync_interval, Duration::from_micros(250_000));
        }
    }

    #[test]
    fn legacy_blob_without_sync_fields_decodes_to_defaults() {
        // Simulate a manifest written before sync_mode/sync_interval (and the
        // later compression_per_level count) were persisted: encode, then
        // truncate off the trailing fields (1 byte sync_mode + 8 bytes
        // sync_interval + 1 byte per-level count).
        let c = ColumnFamilyConfig {
            sync_mode: SyncMode::Full,
            comparator_name: "uint64".into(),
            ..ColumnFamilyConfig::default()
        };
        let full = c.encode();
        let legacy = &full[..full.len() - 10];
        let d = ColumnFamilyConfig::decode(legacy);
        // Older fields still decode; the missing sync fields fall back to default.
        assert_eq!(d.comparator_name, "uint64");
        assert_eq!(d.sync_mode, SyncMode::None);
        assert_eq!(d.sync_interval, ColumnFamilyConfig::default().sync_interval);
    }
}

#[cfg(test)]
mod per_level_tests {
    use super::*;

    #[test]
    fn compression_per_level_roundtrip_and_selection() {
        let c = ColumnFamilyConfig {
            compression: Compression::Snappy,
            compression_per_level: vec![
                Compression::None,
                Compression::None,
                Compression::Zstd,
            ],
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&c.encode());
        assert_eq!(d.compression_per_level, c.compression_per_level);
        assert_eq!(d.compression_for_level(0), Compression::None);
        assert_eq!(d.compression_for_level(1), Compression::None);
        assert_eq!(d.compression_for_level(2), Compression::Zstd);
        assert_eq!(d.compression_for_level(9), Compression::Zstd); // last repeats

        // Empty policy falls back to the uniform setting.
        let u = ColumnFamilyConfig {
            compression: Compression::Lz4,
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(u.compression_for_level(0), Compression::Lz4);
        assert_eq!(u.compression_for_level(5), Compression::Lz4);
    }
}
