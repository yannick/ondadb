//! Configuration types for ondaDB.
//!

use std::sync::Arc;
use std::time::Duration;

use crate::storage::Storage;

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

/// How a column family reclaims space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompactionStyle {
    /// Classic leveled compaction (the default).
    #[default]
    Leveled = 0,
    /// FIFO: data stays in L0 and is never merged. Once the CF exceeds
    /// `fifo_max_bytes` (and/or a table's file age exceeds `fifo_ttl`), the
    /// **oldest tables are deleted whole** — cache semantics, not a KV store:
    /// old data disappears by design, including from live snapshots. Age is
    /// taken from the klog file's modification time (approximate; a restore
    /// that rewrites files resets it).
    Fifo = 1,
}

impl CompactionStyle {
    pub fn from_u8(v: u8) -> Option<CompactionStyle> {
        match v {
            0 => Some(CompactionStyle::Leveled),
            1 => Some(CompactionStyle::Fifo),
            _ => None,
        }
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
    /// Named storage tiers, in addition to the implicit `"ssd"` tier (the
    /// database directory). A bottom-level part may be moved to a tier; its
    /// files then live under `<tier.root>/cf-<name>/`. WAL and upper levels
    /// always stay on the default tier. Empty by default. See
    /// [`TierDef`]. (The keyspace→tier policy and the background mover are a
    /// later milestone; this release ships the storage substrate.)
    pub tiers: Vec<TierDef>,
    /// How often the background part mover scans for bottom-level parts to
    /// relocate per their column family's
    /// [`tier_rules`](ColumnFamilyConfig::tier_rules). The pass runs on the
    /// compaction worker. `Duration::ZERO` disables the scheduled pass entirely
    /// (a manual [`DB::run_part_mover`](crate::DB::run_part_mover) still works).
    /// Defaults to 30s; the pass is a cheap no-op when no CF has tier rules.
    pub part_mover_interval: Duration,
}

/// A named storage location — for now, a directory on some mount (ssd, hdd,
/// nfs). A later milestone adds an S3-backed tier behind the same
/// [`Storage`](crate::storage::Storage) trait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierDef {
    /// Tier name, referenced by [`SstMeta::tier`](crate::manifest::SstMeta::tier).
    /// The name `"ssd"` is reserved for the implicit default tier (the DB dir).
    pub name: String,
    /// Root for this tier. For a local tier it is a filesystem directory; for an
    /// S3 tier it is the in-bucket key prefix. Either way per-CF files live under
    /// `<root>/cf-<name>/`.
    pub root: String,
    /// Whether readers may mmap files on this tier. Local disks set this `true`;
    /// slow/remote-style mounts set it `false` so reads always use the buffered
    /// `pread` path plus the block cache (which matters more there). Defaults to
    /// `true` via [`TierDef::new`]. An S3 tier is always `false`.
    pub supports_mmap: bool,
    /// The storage backend for this tier. Defaults to [`TierBackend::Local`]; an
    /// S3-backed tier is built with [`TierDef::s3`] (requires the `s3` feature).
    pub backend: TierBackend,
}

/// Which storage backend implements a [`TierDef`]. A local tier is a directory on
/// some mount; an S3 tier lives in an S3-compatible object store; a `Custom` tier
/// hands the engine a caller-built [`Storage`] so an embedder can interpose its
/// own decorator (e.g. a read-through cache in front of an S3 tier — ayu's foyer
/// layer, P8).
#[derive(Debug, Clone)]
pub enum TierBackend {
    /// A directory on a local (or NFS/SMB-mounted) filesystem.
    Local,
    /// An S3-compatible object store (feature-gated behind `s3`).
    #[cfg(feature = "s3")]
    S3(S3Config),
    /// A caller-provided [`Storage`] used verbatim for this tier. The engine
    /// treats it opaquely (no mmap: [`TierDef::custom`] forces the buffered path),
    /// so an embedder can wrap another backend — the intended seam for a
    /// read-through cache in front of a remote tier.
    Custom(Arc<dyn Storage>),
}

// `Arc<dyn Storage>` has no structural equality, so `TierBackend` cannot derive
// `PartialEq`/`Eq`. Two `Custom` backends are equal iff they are the *same* Arc
// (identity — a decorator has no meaningful value equality); `Local`/`S3` keep
// their value semantics.
impl PartialEq for TierBackend {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TierBackend::Local, TierBackend::Local) => true,
            #[cfg(feature = "s3")]
            (TierBackend::S3(a), TierBackend::S3(b)) => a == b,
            (TierBackend::Custom(a), TierBackend::Custom(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for TierBackend {}

/// Connection parameters for an [`S3-backed tier`](TierBackend::S3). Credentials,
/// endpoint, bucket and region come straight from `Options`. Use `path_style` for
/// MinIO and other endpoints that address buckets by path rather than subdomain.
#[cfg(feature = "s3")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    /// Bucket name the tier's objects live in.
    pub bucket: String,
    /// Region name (e.g. `"us-east-1"`); any string the endpoint accepts.
    pub region: String,
    /// Endpoint URL, e.g. `http://192.168.65.11:9000` for a local MinIO.
    pub endpoint: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
    /// Path-style addressing (`endpoint/bucket/key`). Required by MinIO.
    pub path_style: bool,
}

impl TierDef {
    /// A local tier at `root` with mmap reads enabled.
    pub fn new(name: impl Into<String>, root: impl Into<String>) -> Self {
        TierDef {
            name: name.into(),
            root: root.into(),
            supports_mmap: true,
            backend: TierBackend::Local,
        }
    }

    /// Disable mmap reads for this tier (route reads through the buffered
    /// `pread` path + block cache, as a remote tier would).
    pub fn without_mmap(mut self) -> Self {
        self.supports_mmap = false;
        self
    }

    /// An S3-backed tier: objects live under the in-bucket prefix `root` and are
    /// read via HTTP range GETs (never mmap'd). See [`S3Config`].
    #[cfg(feature = "s3")]
    pub fn s3(name: impl Into<String>, root: impl Into<String>, config: S3Config) -> Self {
        TierDef {
            name: name.into(),
            root: root.into(),
            supports_mmap: false,
            backend: TierBackend::S3(config),
        }
    }

    /// A tier backed by a caller-provided [`Storage`] (P8). Reads never mmap (the
    /// buffered `pread` path + block cache is used, as for any remote-style tier),
    /// so an embedder can wrap a slow/remote backend with its own read-through
    /// cache and hand the wrapper here. `root` is still the in-backend key/dir
    /// prefix the [`TierRegistry`](crate::storage::TierRegistry) prepends.
    pub fn custom(
        name: impl Into<String>,
        root: impl Into<String>,
        storage: Arc<dyn Storage>,
    ) -> Self {
        TierDef {
            name: name.into(),
            root: root.into(),
            supports_mmap: false,
            backend: TierBackend::Custom(storage),
        }
    }
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
            num_flush_threads: 4,
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
            tiers: Vec::new(),
            part_mover_interval: Duration::from_secs(30),
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
    /// Per-key-prefix override of the level compression. The **longest**
    /// matching prefix wins; keys matching no rule use
    /// [`compression_for_level`](Self::compression_for_level). Applied per
    /// vlog value and per klog data block (the writer cuts a block early when
    /// the next key's rule differs, so blocks never mix algorithms). Purely a
    /// write-side policy — SSTable blocks and vlog frames are self-describing,
    /// so rules can change at any time without rewriting existing data.
    pub compression_rules: Vec<CompressionRule>,
    /// Prefix rules that carve the keyspace into named **partitions**. The
    /// **longest** matching prefix wins (so rules may nest: `img/` and
    /// `img/thumb/` are both legal and a key under `img/thumb/` resolves to the
    /// latter); keys matching no rule live in the implicit default partition
    /// (`partition_of` returns `None`). Exact-duplicate prefixes are rejected by
    /// [`validate`](Self::validate).
    ///
    /// Partitions are the unit of the parts/tiers machinery: bottom-level
    /// compaction cuts its output files at partition boundaries so that no
    /// bottom SSTable ever spans two partitions (see
    /// [`SstMeta::partition`](crate::manifest::SstMeta::partition)). Upper
    /// levels are left mixed. Purely a write-side policy — changing the rules
    /// only affects files written afterward; existing files keep whatever
    /// partition they were cut into.
    pub partition_rules: Vec<PartitionRule>,
    /// Prefix rules that pin a partition's bottom-level part to a storage
    /// **tier** (see [`TierDef`]). The **longest** matching prefix wins, exactly
    /// like [`partition_rules`](Self::partition_rules) and
    /// [`compression_rules`](Self::compression_rules); a part matching no rule
    /// stays on the tier it was written to (the default `"ssd"` tier).
    ///
    /// The background **part mover** (`DB::run_part_mover`, and a scheduled
    /// cadence) reads these: for each bottom-level part it resolves the target
    /// tier by the part's key prefix and, once the part's newest entry is older
    /// than [`TierRule::min_age`], relocates the part there (copy → fsync →
    /// one-record manifest flip → delete source). Purely a placement policy —
    /// changing the rules only affects where the mover *next* places a part;
    /// data already on a tier is not rewritten until it qualifies for a move.
    /// Exact-duplicate prefixes are rejected by [`validate`](Self::validate).
    pub tier_rules: Vec<TierRule>,
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
    pub compaction_style: CompactionStyle,
    /// FIFO only: evict oldest tables once the CF's total bytes exceed this
    /// (0 = no size limit).
    pub fifo_max_bytes: u64,
    /// FIFO only: evict tables whose klog file is older than this
    /// (zero = no age limit).
    pub fifo_ttl: Duration,
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
            compression_rules: Vec::new(),
            partition_rules: Vec::new(),
            tier_rules: Vec::new(),
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
            compaction_style: CompactionStyle::Leveled,
            fifo_max_bytes: 0,
            fifo_ttl: Duration::ZERO,
        }
    }
}

/// One per-key-prefix compression rule (see
/// [`ColumnFamilyConfig::compression_rules`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionRule {
    /// Keys starting with this byte prefix use `compression`.
    pub prefix: Vec<u8>,
    pub compression: Compression,
}

/// Resolve `user_key` against prefix rules: longest matching prefix wins.
/// `None` when no rule matches.
pub(crate) fn compression_for_key(
    rules: &[CompressionRule],
    user_key: &[u8],
) -> Option<Compression> {
    rules
        .iter()
        .filter(|r| user_key.starts_with(&r.prefix))
        .max_by_key(|r| r.prefix.len())
        .map(|r| r.compression)
}

/// One prefix → partition-name rule (see
/// [`ColumnFamilyConfig::partition_rules`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRule {
    /// Keys starting with this byte prefix belong to partition `name`.
    pub prefix: Vec<u8>,
    /// Partition name, recorded on bottom-level SSTables cut on this boundary.
    pub name: String,
}

/// Resolve `user_key` to a partition name: longest matching prefix wins.
/// `None` (the implicit default partition) when no rule matches.
pub(crate) fn partition_of<'a>(rules: &'a [PartitionRule], user_key: &[u8]) -> Option<&'a str> {
    rules
        .iter()
        .filter(|r| user_key.starts_with(&r.prefix))
        .max_by_key(|r| r.prefix.len())
        .map(|r| r.name.as_str())
}

/// One prefix → storage-tier rule (see
/// [`ColumnFamilyConfig::tier_rules`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierRule {
    /// A part is targeted by this rule when its keys start with this prefix.
    pub prefix: Vec<u8>,
    /// Target tier name (a [`TierDef::name`], or the reserved `"ssd"` for the
    /// default tier).
    pub tier: String,
    /// Move a part only once its newest entry
    /// ([`SstMeta::max_entry_time`](crate::manifest::SstMeta::max_entry_time)) is
    /// older than this — a part whose freshest data is younger stays put.
    pub min_age: Duration,
}

/// Resolve `user_key` to a tier rule: longest matching prefix wins. `None` when
/// no rule matches (the part keeps its current tier).
pub(crate) fn tier_for_key<'a>(rules: &'a [TierRule], user_key: &[u8]) -> Option<&'a TierRule> {
    rules
        .iter()
        .filter(|r| user_key.starts_with(&r.prefix))
        .max_by_key(|r| r.prefix.len())
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

    /// Compression algorithm for `user_key` written at `level`: the longest
    /// matching entry in `compression_rules`, falling back to
    /// [`compression_for_level`](Self::compression_for_level).
    pub fn compression_for_key(&self, user_key: &[u8], level: u32) -> Compression {
        compression_for_key(&self.compression_rules, user_key)
            .unwrap_or_else(|| self.compression_for_level(level))
    }

    /// Partition name for `user_key`: the longest matching entry in
    /// [`partition_rules`](Self::partition_rules), or `None` for the implicit
    /// default partition.
    pub fn partition_of(&self, user_key: &[u8]) -> Option<&str> {
        partition_of(&self.partition_rules, user_key)
    }

    /// The [`TierRule`] governing `user_key`: the longest matching entry in
    /// [`tier_rules`](Self::tier_rules), or `None` if no rule applies.
    pub fn tier_for_key(&self, user_key: &[u8]) -> Option<&TierRule> {
        tier_for_key(&self.tier_rules, user_key)
    }

    /// Reject structurally invalid configuration. Currently: exact-duplicate
    /// partition prefixes (two rules with the same `prefix`). Nested prefixes
    /// are legal — longest-prefix-wins resolves them deterministically — so
    /// only an exact collision (which would make resolution order-dependent) is
    /// an error.
    pub fn validate(&self) -> Result<(), String> {
        for (i, a) in self.partition_rules.iter().enumerate() {
            for b in &self.partition_rules[i + 1..] {
                if a.prefix == b.prefix {
                    return Err(format!(
                        "duplicate partition prefix {:?} (rules {:?} and {:?})",
                        String::from_utf8_lossy(&a.prefix),
                        a.name,
                        b.name
                    ));
                }
            }
        }
        // Two tier rules with the same prefix would make longest-prefix
        // resolution order-dependent (like duplicate partition prefixes above).
        for (i, a) in self.tier_rules.iter().enumerate() {
            for b in &self.tier_rules[i + 1..] {
                if a.prefix == b.prefix {
                    return Err(format!(
                        "duplicate tier prefix {:?} (tiers {:?} and {:?})",
                        String::from_utf8_lossy(&a.prefix),
                        a.tier,
                        b.tier
                    ));
                }
            }
        }
        Ok(())
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
        let n_levels = self.compression_per_level.len().min(u8::MAX as usize);
        b.push(n_levels as u8);
        for c in self.compression_per_level.iter().take(n_levels) {
            b.push(*c as u8);
        }
        b.push(self.compaction_style as u8);
        append_u64(&mut b, self.fifo_max_bytes);
        append_u64(&mut b, self.fifo_ttl.as_micros() as u64);
        let n_rules = self.compression_rules.len().min(u8::MAX as usize);
        b.push(n_rules as u8);
        for r in self.compression_rules.iter().take(n_rules) {
            append_uvarint(&mut b, r.prefix.len() as u64);
            b.extend_from_slice(&r.prefix);
            b.push(r.compression as u8);
        }
        // Appended tail (same backward/forward-compatible scheme as above): an
        // older manifest ends before this count, so `decode_into` returns via
        // `?` and leaves `partition_rules` empty.
        let n_parts = self.partition_rules.len().min(u8::MAX as usize);
        b.push(n_parts as u8);
        for r in self.partition_rules.iter().take(n_parts) {
            append_uvarint(&mut b, r.prefix.len() as u64);
            b.extend_from_slice(&r.prefix);
            append_uvarint(&mut b, r.name.len() as u64);
            b.extend_from_slice(r.name.as_bytes());
        }
        // Appended tail (same backward/forward-compatible scheme): storage-tier
        // rules. An older manifest ends before this count byte, so `decode_into`
        // returns via `?` and leaves `tier_rules` empty.
        let n_tiers = self.tier_rules.len().min(u8::MAX as usize);
        b.push(n_tiers as u8);
        for r in self.tier_rules.iter().take(n_tiers) {
            append_uvarint(&mut b, r.prefix.len() as u64);
            b.extend_from_slice(&r.prefix);
            append_uvarint(&mut b, r.tier.len() as u64);
            b.extend_from_slice(r.tier.as_bytes());
            append_u64(&mut b, r.min_age.as_micros() as u64);
        }

        // Preserve the released 0.3.0 representation above byte-for-byte for
        // every count it could represent. Extra entries live in a tagged tail:
        // old readers ignore it, while new readers append it to the first 255.
        // This avoids the ambiguity between a legacy u8 count >= 128 and the
        // first byte of a LEB128 count.
        if self.compression_per_level.len() > u8::MAX as usize
            || self.compression_rules.len() > u8::MAX as usize
            || self.partition_rules.len() > u8::MAX as usize
            || self.tier_rules.len() > u8::MAX as usize
        {
            b.extend_from_slice(CONFIG_OVERFLOW_MAGIC);

            append_uvarint(
                &mut b,
                self.compression_per_level.len().saturating_sub(n_levels) as u64,
            );
            for c in self.compression_per_level.iter().skip(n_levels) {
                b.push(*c as u8);
            }

            append_uvarint(
                &mut b,
                self.compression_rules.len().saturating_sub(n_rules) as u64,
            );
            for r in self.compression_rules.iter().skip(n_rules) {
                append_uvarint(&mut b, r.prefix.len() as u64);
                b.extend_from_slice(&r.prefix);
                b.push(r.compression as u8);
            }

            append_uvarint(
                &mut b,
                self.partition_rules.len().saturating_sub(n_parts) as u64,
            );
            for r in self.partition_rules.iter().skip(n_parts) {
                append_uvarint(&mut b, r.prefix.len() as u64);
                b.extend_from_slice(&r.prefix);
                append_uvarint(&mut b, r.name.len() as u64);
                b.extend_from_slice(r.name.as_bytes());
            }

            append_uvarint(&mut b, self.tier_rules.len().saturating_sub(n_tiers) as u64);
            for r in self.tier_rules.iter().skip(n_tiers) {
                append_uvarint(&mut b, r.prefix.len() as u64);
                b.extend_from_slice(&r.prefix);
                append_uvarint(&mut b, r.tier.len() as u64);
                b.extend_from_slice(r.tier.as_bytes());
                append_u64(&mut b, r.min_age.as_micros() as u64);
            }
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

const CONFIG_OVERFLOW_MAGIC: &[u8; 8] = b"ONDAOVF1";

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
    let uvar = |p: &mut &[u8]| -> Option<u64> {
        let (v, n) = uvarint(p)?;
        *p = &p[n..];
        Some(v)
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
    if let Some(style) = CompactionStyle::from_u8(byte(&mut p)?) {
        cfg.compaction_style = style;
    }
    cfg.fifo_max_bytes = u64v(&mut p)?;
    cfg.fifo_ttl = std::time::Duration::from_micros(u64v(&mut p)?);
    let n_rules = byte(&mut p)?;
    let mut rules = Vec::with_capacity(n_rules as usize);
    for _ in 0..n_rules {
        let (plen, n) = uvarint(p)?;
        p = &p[n..];
        let plen = plen as usize;
        if p.len() < plen {
            return None;
        }
        let prefix = p[..plen].to_vec();
        p = &p[plen..];
        rules.push(CompressionRule {
            prefix,
            compression: Compression::from_u8(byte(&mut p)?)?,
        });
    }
    cfg.compression_rules = rules;
    // Appended-tail partition rules; an older manifest ends above and keeps the
    // default (empty) list.
    let n_parts = byte(&mut p)?;
    let mut parts = Vec::with_capacity(n_parts as usize);
    for _ in 0..n_parts {
        let (plen, n) = uvarint(p)?;
        p = &p[n..];
        let plen = plen as usize;
        if p.len() < plen {
            return None;
        }
        let prefix = p[..plen].to_vec();
        p = &p[plen..];
        let (nlen, n) = uvarint(p)?;
        p = &p[n..];
        let nlen = nlen as usize;
        if p.len() < nlen {
            return None;
        }
        let name = String::from_utf8_lossy(&p[..nlen]).into_owned();
        p = &p[nlen..];
        parts.push(PartitionRule { prefix, name });
    }
    cfg.partition_rules = parts;
    // Appended-tail tier rules; an older manifest ends above and keeps the
    // default (empty) list.
    let n_tiers = byte(&mut p)?;
    let mut tiers = Vec::with_capacity(n_tiers as usize);
    for _ in 0..n_tiers {
        let (plen, n) = uvarint(p)?;
        p = &p[n..];
        let plen = plen as usize;
        if p.len() < plen {
            return None;
        }
        let prefix = p[..plen].to_vec();
        p = &p[plen..];
        let (tlen, n) = uvarint(p)?;
        p = &p[n..];
        let tlen = tlen as usize;
        if p.len() < tlen {
            return None;
        }
        let tier = String::from_utf8_lossy(&p[..tlen]).into_owned();
        p = &p[tlen..];
        let min_age = std::time::Duration::from_micros(u64v(&mut p)?);
        tiers.push(TierRule {
            prefix,
            tier,
            min_age,
        });
    }
    cfg.tier_rules = tiers;

    let Some(rest) = p.strip_prefix(CONFIG_OVERFLOW_MAGIC) else {
        return Some(());
    };
    p = rest;

    let extra_levels = uvar(&mut p)?;
    cfg.compression_per_level
        .reserve(extra_levels.min(p.len() as u64) as usize);
    for _ in 0..extra_levels {
        cfg.compression_per_level
            .push(Compression::from_u8(byte(&mut p)?)?);
    }

    let extra_rules = uvar(&mut p)?;
    cfg.compression_rules
        .reserve(extra_rules.min(p.len() as u64) as usize);
    for _ in 0..extra_rules {
        let (plen, n) = uvarint(p)?;
        p = &p[n..];
        let plen = plen as usize;
        if p.len() < plen {
            return None;
        }
        let prefix = p[..plen].to_vec();
        p = &p[plen..];
        cfg.compression_rules.push(CompressionRule {
            prefix,
            compression: Compression::from_u8(byte(&mut p)?)?,
        });
    }

    let extra_parts = uvar(&mut p)?;
    cfg.partition_rules
        .reserve(extra_parts.min(p.len() as u64) as usize);
    for _ in 0..extra_parts {
        let (plen, n) = uvarint(p)?;
        p = &p[n..];
        let plen = plen as usize;
        if p.len() < plen {
            return None;
        }
        let prefix = p[..plen].to_vec();
        p = &p[plen..];
        let (nlen, n) = uvarint(p)?;
        p = &p[n..];
        let nlen = nlen as usize;
        if p.len() < nlen {
            return None;
        }
        let name = String::from_utf8_lossy(&p[..nlen]).into_owned();
        p = &p[nlen..];
        cfg.partition_rules.push(PartitionRule { prefix, name });
    }

    let extra_tiers = uvar(&mut p)?;
    cfg.tier_rules
        .reserve(extra_tiers.min(p.len() as u64) as usize);
    for _ in 0..extra_tiers {
        let (plen, n) = uvarint(p)?;
        p = &p[n..];
        let plen = plen as usize;
        if p.len() < plen {
            return None;
        }
        let prefix = p[..plen].to_vec();
        p = &p[plen..];
        let (tlen, n) = uvarint(p)?;
        p = &p[n..];
        let tlen = tlen as usize;
        if p.len() < tlen {
            return None;
        }
        let tier = String::from_utf8_lossy(&p[..tlen]).into_owned();
        p = &p[tlen..];
        let min_age = std::time::Duration::from_micros(u64v(&mut p)?);
        cfg.tier_rules.push(TierRule {
            prefix,
            tier,
            min_age,
        });
    }
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
            compression_rules: vec![
                CompressionRule {
                    prefix: b"img/".to_vec(),
                    compression: Compression::Zstd,
                },
                CompressionRule {
                    prefix: b"hot/".to_vec(),
                    compression: Compression::None,
                },
            ],
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&c.encode());
        assert_eq!(d.comparator_name, "uint64");
        assert!(d.compression_per_level.is_empty());
        assert_eq!(d.compression, Compression::Zstd);
        assert_eq!(d.write_buffer_size, 123456);
        assert!(!d.enable_bloom_filter);
        assert_eq!(d.compression_rules, c.compression_rules);
    }

    #[test]
    fn compression_rule_resolution() {
        let rules = vec![
            CompressionRule {
                prefix: b"a".to_vec(),
                compression: Compression::Lz4,
            },
            CompressionRule {
                prefix: b"az".to_vec(),
                compression: Compression::Zstd,
            },
        ];
        // Longest prefix wins regardless of rule order.
        assert_eq!(
            compression_for_key(&rules, b"az123"),
            Some(Compression::Zstd)
        );
        assert_eq!(compression_for_key(&rules, b"ab"), Some(Compression::Lz4));
        assert_eq!(compression_for_key(&rules, b"zz"), None);
        let cfg = ColumnFamilyConfig {
            compression: Compression::Snappy,
            compression_rules: rules,
            ..Default::default()
        };
        assert_eq!(cfg.compression_for_key(b"az1", 0), Compression::Zstd);
        assert_eq!(cfg.compression_for_key(b"zz", 3), Compression::Snappy);
    }

    #[test]
    fn partition_rule_resolution() {
        let rules = vec![
            PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            },
            PartitionRule {
                prefix: b"img/thumb/".to_vec(),
                name: "thumb".into(),
            },
        ];
        // Longest prefix wins; nesting is legal.
        assert_eq!(partition_of(&rules, b"img/thumb/1.jpg"), Some("thumb"));
        assert_eq!(partition_of(&rules, b"img/full/1.jpg"), Some("img"));
        // Un-ruled keys fall into the implicit default partition.
        assert_eq!(partition_of(&rules, b"logs/2026"), None);
        let cfg = ColumnFamilyConfig {
            partition_rules: rules,
            ..Default::default()
        };
        assert_eq!(cfg.partition_of(b"img/thumb/x"), Some("thumb"));
        assert_eq!(cfg.partition_of(b"other"), None);
    }

    #[test]
    fn partition_rules_survive_manifest_round_trip() {
        let c = ColumnFamilyConfig {
            partition_rules: vec![
                PartitionRule {
                    prefix: b"a/".to_vec(),
                    name: "alpha".into(),
                },
                PartitionRule {
                    prefix: b"b/".to_vec(),
                    name: "beta".into(),
                },
            ],
            // Coexists with compression_rules (both are appended tails).
            compression_rules: vec![CompressionRule {
                prefix: b"a/".to_vec(),
                compression: Compression::Zstd,
            }],
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&c.encode());
        assert_eq!(d.partition_rules, c.partition_rules);
        assert_eq!(d.compression_rules, c.compression_rules);
    }

    #[test]
    fn legacy_config_without_partition_tail_decodes_to_empty() {
        // A config encoded before partition_rules / tier_rules existed ends right
        // after the compression_rules section. The encoding now appends a 1-byte
        // partition-count then a 1-byte tier-count; dropping both trailing count
        // bytes simulates that older, shorter blob and both lists fall back empty.
        let c = ColumnFamilyConfig {
            comparator_name: "uint64".into(),
            ..ColumnFamilyConfig::default()
        };
        let full = c.encode();
        let legacy = &full[..full.len() - 2];
        let d = ColumnFamilyConfig::decode(legacy);
        assert_eq!(d.comparator_name, "uint64");
        assert!(d.partition_rules.is_empty());
        assert!(d.tier_rules.is_empty());
    }

    #[test]
    fn tier_rules_survive_manifest_round_trip() {
        let c = ColumnFamilyConfig {
            tier_rules: vec![
                TierRule {
                    prefix: b"img/".to_vec(),
                    tier: "hdd".into(),
                    min_age: Duration::from_secs(30 * 24 * 3600),
                },
                TierRule {
                    prefix: b"log/".to_vec(),
                    tier: "cold".into(),
                    min_age: Duration::from_secs(3600),
                },
            ],
            // Coexists with partition_rules (both are appended tails).
            partition_rules: vec![PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            }],
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&c.encode());
        assert_eq!(d.tier_rules, c.tier_rules);
        assert_eq!(d.partition_rules, c.partition_rules);
    }

    #[test]
    fn legacy_config_with_partition_but_no_tier_tail_decodes_tiers_empty() {
        // A P1-era blob carried the partition tail but no tier tail. Encode with
        // a partition rule, drop only the trailing tier-count byte, and confirm
        // the partition rule still decodes while tier_rules falls back empty.
        let c = ColumnFamilyConfig {
            partition_rules: vec![PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            }],
            ..ColumnFamilyConfig::default()
        };
        let full = c.encode();
        let legacy = &full[..full.len() - 1];
        let d = ColumnFamilyConfig::decode(legacy);
        assert_eq!(d.partition_rules, c.partition_rules);
        assert!(d.tier_rules.is_empty());
    }

    #[test]
    fn tier_rule_resolution() {
        let rules = vec![
            TierRule {
                prefix: b"img/".to_vec(),
                tier: "hdd".into(),
                min_age: Duration::from_secs(1),
            },
            TierRule {
                prefix: b"img/thumb/".to_vec(),
                tier: "ssd".into(),
                min_age: Duration::from_secs(2),
            },
        ];
        // Longest prefix wins regardless of order; unmatched keys resolve to None.
        assert_eq!(tier_for_key(&rules, b"img/thumb/1").unwrap().tier, "ssd");
        assert_eq!(tier_for_key(&rules, b"img/full/1").unwrap().tier, "hdd");
        assert!(tier_for_key(&rules, b"log/2026").is_none());
        let cfg = ColumnFamilyConfig {
            tier_rules: rules,
            ..Default::default()
        };
        assert_eq!(cfg.tier_for_key(b"img/thumb/x").unwrap().tier, "ssd");
        assert!(cfg.tier_for_key(b"other").is_none());
    }

    #[test]
    fn validate_rejects_duplicate_tier_prefix() {
        let dup = ColumnFamilyConfig {
            tier_rules: vec![
                TierRule {
                    prefix: b"x/".to_vec(),
                    tier: "hdd".into(),
                    min_age: Duration::ZERO,
                },
                TierRule {
                    prefix: b"x/".to_vec(),
                    tier: "cold".into(),
                    min_age: Duration::ZERO,
                },
            ],
            ..ColumnFamilyConfig::default()
        };
        assert!(dup.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_partition_prefix() {
        let dup = ColumnFamilyConfig {
            partition_rules: vec![
                PartitionRule {
                    prefix: b"x/".to_vec(),
                    name: "one".into(),
                },
                PartitionRule {
                    prefix: b"x/".to_vec(),
                    name: "two".into(),
                },
            ],
            ..ColumnFamilyConfig::default()
        };
        assert!(dup.validate().is_err());

        // Nested (non-equal) prefixes are legal — longest-prefix-wins.
        let nested = ColumnFamilyConfig {
            partition_rules: vec![
                PartitionRule {
                    prefix: b"x/".to_vec(),
                    name: "one".into(),
                },
                PartitionRule {
                    prefix: b"x/y/".to_vec(),
                    name: "two".into(),
                },
            ],
            ..ColumnFamilyConfig::default()
        };
        assert!(nested.validate().is_ok());
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
        assert_eq!(o.num_flush_threads, 4);
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
            assert_eq!(
                d.sync_mode, sm,
                "sync_mode must survive a manifest round-trip"
            );
            assert_eq!(d.sync_interval, Duration::from_micros(250_000));
        }
    }

    #[test]
    fn legacy_blob_without_sync_fields_decodes_to_defaults() {
        // Simulate a manifest written before the appended-tail fields
        // (sync_mode/sync_interval, compression_per_level, FIFO settings,
        // compression_rules, partition_rules, tier_rules) were persisted:
        // encode, then truncate the whole tail (9 bytes sync + 1 byte per-level
        // count + 17 bytes FIFO + 1 byte compression-rules count + 1 byte
        // partition-rules count + 1 byte tier-rules count).
        let c = ColumnFamilyConfig {
            sync_mode: SyncMode::Full,
            comparator_name: "uint64".into(),
            ..ColumnFamilyConfig::default()
        };
        let full = c.encode();
        let legacy = &full[..full.len() - 30];
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
            compression_per_level: vec![Compression::None, Compression::None, Compression::Zstd],
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

    /// Counts above 255 use the compatible overflow tail instead of silently
    /// dropping policies. This is realistic for prefix-per-tenant layouts.
    #[test]
    fn rule_counts_are_not_truncated() {
        let n = 1000;
        let c = ColumnFamilyConfig {
            compression_per_level: vec![Compression::Zstd; n],
            partition_rules: (0..n)
                .map(|i| PartitionRule {
                    prefix: format!("ns{i:04}/").into_bytes(),
                    name: format!("p{i:04}"),
                })
                .collect(),
            tier_rules: (0..n)
                .map(|i| TierRule {
                    prefix: format!("ns{i:04}/").into_bytes(),
                    tier: format!("t{i:04}"),
                    min_age: Duration::from_secs(i as u64),
                })
                .collect(),
            compression_rules: (0..n)
                .map(|i| CompressionRule {
                    prefix: format!("ns{i:04}/").into_bytes(),
                    compression: Compression::Zstd,
                })
                .collect(),
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&c.encode());
        assert_eq!(d.compression_per_level.len(), n, "level policy truncated");
        assert_eq!(d.partition_rules.len(), n, "partition rules truncated");
        assert_eq!(d.tier_rules.len(), n, "tier rules truncated");
        assert_eq!(d.compression_rules.len(), n, "compression rules truncated");
        assert_eq!(d.partition_rules[999].name, "p0999");
        assert_eq!(d.tier_rules[999].tier, "t0999");
        assert_eq!(d.compression_per_level[999], Compression::Zstd);
    }

    #[test]
    fn representable_rule_counts_keep_the_legacy_encoding() {
        let c = ColumnFamilyConfig {
            compression_per_level: vec![Compression::Zstd; 255],
            partition_rules: (0..255)
                .map(|i| PartitionRule {
                    prefix: format!("p{i}/").into_bytes(),
                    name: format!("p{i}"),
                })
                .collect(),
            ..ColumnFamilyConfig::default()
        };
        let encoded = c.encode();
        assert!(!encoded
            .windows(CONFIG_OVERFLOW_MAGIC.len())
            .any(|w| w == CONFIG_OVERFLOW_MAGIC));
        let d = ColumnFamilyConfig::decode(&encoded);
        assert_eq!(d.compression_per_level.len(), 255);
        assert_eq!(d.partition_rules.len(), 255);
    }

    #[test]
    fn old_reader_can_ignore_the_overflow_tail() {
        let c = ColumnFamilyConfig {
            partition_rules: (0..300)
                .map(|i| PartitionRule {
                    prefix: format!("p{i}/").into_bytes(),
                    name: format!("p{i}"),
                })
                .collect(),
            ..ColumnFamilyConfig::default()
        };
        let encoded = c.encode();
        let tail = encoded
            .windows(CONFIG_OVERFLOW_MAGIC.len())
            .position(|w| w == CONFIG_OVERFLOW_MAGIC)
            .expect("oversized policy must have an overflow tail");

        // A 0.3.0 reader ignores bytes after its four base lists. Decoding the
        // base alone models that behavior and must preserve its first 255 rules.
        let old_view = ColumnFamilyConfig::decode(&encoded[..tail]);
        assert_eq!(old_view.partition_rules.len(), 255);
        assert_eq!(old_view.partition_rules[254].name, "p254");
    }

    #[test]
    fn legacy_u8_count_128_decodes_without_losing_policy() {
        use crate::encoding::uvarint;

        let c127 = ColumnFamilyConfig {
            compression_per_level: vec![Compression::Zstd; 127],
            ..ColumnFamilyConfig::default()
        };
        let mut legacy = c127.encode();

        // Locate the first variable-count field after the fixed config prefix.
        let (name_len, name_len_bytes) = uvarint(&legacy).unwrap();
        let count_offset = name_len_bytes
            + name_len as usize
            + 1 // compression
            + 8 // write_buffer_size
            + 8 // level_size_ratio
            + 8 // klog_value_threshold
            + 1 // enable_bloom_filter
            + 8 // bloom_fpr
            + 4 // l1_file_count_trigger
            + 4 // l0_queue_stall_threshold
            + 1 // use_btree
            + 1 // sync_mode
            + 8; // sync_interval

        // Counts through 127 have always been byte-identical. Turn that blob
        // into the exact 0.3.0 representation of 128 entries: one count byte
        // followed immediately by all 128 compression bytes.
        assert_eq!(legacy[count_offset], 127);
        legacy[count_offset] = 128;
        legacy.insert(count_offset + 1, Compression::Zstd as u8);

        let decoded = ColumnFamilyConfig::decode(&legacy);
        assert_eq!(decoded.compression_per_level, vec![Compression::Zstd; 128]);
    }
}
