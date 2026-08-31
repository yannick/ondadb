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
///
/// Each variant describes an **optimistic** transaction, the default. A
/// pessimistic one ([`DB::begin_pessimistic`](crate::DB::begin_pessimistic),
/// 3.3) takes point locks and waits for them, which changes two of the five
/// levels; each variant below says how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Reads observe the latest committed sequence at read time; no conflict
    /// detection on commit.
    ///
    /// Pessimistic mode adds lock ordering and nothing else — there is no
    /// snapshot to refresh, and reads already float.
    ReadUncommitted,
    /// Reads observe the latest committed sequence at read time; no conflict
    /// detection on commit. (The default for the single-op helpers.)
    ///
    /// Pessimistic mode adds lock ordering and nothing else — there is no
    /// snapshot to refresh, and reads already float.
    ReadCommitted,
    /// Reads are pinned to a snapshot taken at `begin`; no conflict detection on
    /// commit.
    ///
    /// **Pessimistic mode does not refresh this snapshot**, because not moving
    /// is the one thing this level promises. Locks therefore order the writes
    /// (a dirty write is impossible) but cannot make a stale read fresh: a
    /// read-modify-write that reads before the grant can still lose its update.
    /// Use [`Snapshot`](Self::Snapshot) if you want the lock to prevent that.
    RepeatableRead,
    /// Snapshot isolation: reads are pinned to the `begin` snapshot and commit
    /// aborts with [`Conflict`](crate::OndaError::Conflict) on a write-write
    /// conflict (first-committer-wins). Permits write skew.
    ///
    /// **In pessimistic mode a lock grant refreshes the snapshot**, so a
    /// transaction that waited for a key commits against what it waited for
    /// instead of aborting on it. The price is that reads taken before an
    /// acquisition may be older than reads taken after it: read skew
    /// (G-single) becomes possible where snapshot isolation prevented it. That
    /// is the documented semantic of pessimistic `Snapshot`, not a bug.
    Snapshot,
    /// Snapshot isolation plus validation, on commit, that every key the
    /// transaction *read by point lookup* is unchanged since its snapshot.
    ///
    /// **Not full serializability.** Range/iterator reads are not tracked, so
    /// phantoms (rows inserted into a scanned range by a concurrent committer) are
    /// not detected. Use only point `get`s if you rely on the conflict check.
    /// (TODO:  implement full SSI with rw-antidependency)
    ///
    /// **In pessimistic mode a lock grant refreshes the snapshot**, after
    /// revalidating the read set against the old one — so a changed read-set
    /// key aborts at the acquisition rather than at the commit. Pessimistic
    /// `Serializable` is therefore not conflict-free in general, only for
    /// write-write contention with an unchanged read set.
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
    /// Number of background flush workers. At least one is always spawned.
    pub num_flush_threads: usize,
    /// Number of background compaction workers. At least one is always spawned.
    pub num_compaction_threads: usize,
    /// Reserved for a future logging subsystem; currently ignored.
    pub log_level: LogLevel,
    pub block_cache_size: usize,
    /// Maximum SSTable readers held open at once — the `max_open_files`
    /// analogue.
    ///
    /// Opening a table loads its whole block index and bloom filter, and both
    /// stay resident while the reader does. Without a bound, opening every table
    /// in the manifest made resident memory proportional to **total stored
    /// bytes**: 6.6 GB twelve seconds into startup on a 48 GiB store of 14,051
    /// tables, 12 GB at twenty-five and still going. Closing a reader costs a
    /// re-open and cannot change an answer, so this bounds memory by count
    /// rather than by corpus size. There is no "unlimited" value.
    ///
    /// A count alone is not the real constraint — see
    /// [`max_open_reader_bytes`](Self::max_open_reader_bytes), which bounds the
    /// same memory in the unit an operator actually has.
    pub max_open_readers: usize,
    /// Maximum **resident bytes** (block index + bloom) across open SSTable
    /// readers. `0` disables the byte bound; default 1 GiB.
    ///
    /// [`max_open_readers`](Self::max_open_readers) assumes readers cost roughly
    /// the same, and they do not: per-reader cost scales with the table's key
    /// count. Measured on spada's staging cluster (2026-08-09) it varied about
    /// **30x with segment size alone** — under a megabyte per reader at
    /// 256-document segments, about 20 MB at 8192-document segments. The count
    /// stayed at spada's configured 4096; the memory it implied went from a few
    /// hundred megabytes to roughly 10 GiB, and nodes OOMed repeatedly. Nothing
    /// in the count expressed that, because the count is not the constraint —
    /// bytes are.
    ///
    /// Both bounds are enforced: the cache evicts least-recently-used readers
    /// until the open count is within `max_open_readers` **and** their resident
    /// bytes are within this. Setting either to `0`/unlimited leaves the other
    /// in force. Read the occupancy back with
    /// [`DB::table_cache_bytes`](crate::DB::table_cache_bytes).
    ///
    /// The bound covers what the cache holds. A reader being read right now is
    /// kept alive by its caller past eviction, so peak memory is this budget
    /// plus the concurrent in-flight readers.
    pub max_open_reader_bytes: usize,
    pub max_open_sstables: usize,
    /// Reserved for a future database-wide memory governor; currently ignored.
    pub max_memory_usage: u64,
    pub read_only: bool,
    /// Whether [`DB::close`](crate::DB::close) drains queued compaction work
    /// before returning.
    ///
    /// Default `false`: close abandons whatever is still queued. Leftover debt
    /// is legal LSM state that the next open resumes from, so the only cost is
    /// that the reopened database starts with more to compact.
    ///
    /// Declared since the option existed but read by nothing until 0.8.0 — the
    /// compaction worker only tested its stop flag when its queue ran dry, so
    /// close drained the queue regardless of this setting. On a database that
    /// had just ingested 20M records that made `close()` take 35 seconds.
    pub finish_compactions_on_close: bool,
    /// Reserved for separate flush admission control; currently ignored.
    pub max_concurrent_flushes: usize,
    pub unified_memtable: bool,
    pub unified_memtable_write_buffer_size: usize,
    /// Reserved unified skip-list tuning; the shared store currently uses the
    /// ordinary memtable constants and ignores this field.
    pub unified_memtable_skip_list_max_level: u32,
    /// Reserved unified skip-list tuning; currently ignored.
    pub unified_memtable_skip_list_probability: f64,
    pub unified_memtable_sync_mode: SyncMode,
    pub unified_memtable_sync_interval: Duration,
    /// Maximum number of sealed unified memtables awaiting flush before new
    /// writers stall. Bounds recovery/WAL-backed memory when flush falls
    /// behind; one flush completion wakes the blocked writers.
    pub unified_memtable_stall_threshold: usize,
    /// Maximum number of markers the committed-span index (1.2) holds at once.
    ///
    /// The index records recent commits so a range delete can ask "did anything
    /// in `[start, end)` change since my snapshot" — a question the LSM cannot
    /// answer without scanning the span. Markers are pruned below the oldest
    /// live snapshot; when a long-lived snapshot keeps them alive and the index
    /// fills, further **range** commits *wait* rather than silently dropping a
    /// marker (a dropped marker would blind every later point writer), and they
    /// wait *before* taking `commit_mu` so nothing else stalls behind them.
    ///
    /// Point commits never wait — the write path must not stall behind a
    /// bookkeeping structure. A point commit that finds the index full gives up
    /// its marker and raises an overflow watermark instead; range writers
    /// reading at or below that watermark then conflict unconditionally, which
    /// can refuse a range commit that would have been safe but never admits one
    /// that would not.
    ///
    /// Unused until [`CAP_RANGE_DELETES`](crate::format::CAP_RANGE_DELETES) is
    /// enabled; no allocation happens before the first marker.
    pub span_index_capacity: usize,
    /// Explicitly migrate an existing per-column-family database to the
    /// unified WAL layout during open. The migration flushes all recovered
    /// per-CF WAL state before atomically flipping the manifest layout.
    pub migrate_to_unified: bool,
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
    /// Derived partitioners available to this database, resolved by
    /// [`PartitionFn::scheme_name`](crate::PartitionFn::scheme_name).
    ///
    /// A column family that was created with
    /// [`PartitionScheme::Derived`] records only its scheme *name* in the
    /// manifest, because a boxed function is not serializable. On open the
    /// engine looks the name up here; if it is absent the open fails rather
    /// than silently reverting the column family to
    /// [`partition_rules`](ColumnFamilyConfig::partition_rules), which would
    /// mis-cut every part written afterwards.
    ///
    /// This is the same name-and-registry indirection used for comparators,
    /// made extensible because partitioners are consumer-defined.
    pub partition_fns: Vec<Arc<dyn PartitionFn>>,
    /// Merge operators available to this database, resolved by
    /// [`MergeOperator::name`].
    ///
    /// A column family created with
    /// [`merge_operator_name`](ColumnFamilyConfig::merge_operator_name)
    /// records only that *name* in the manifest, because a boxed function is
    /// not serializable. On open the engine looks the name up here; if it is
    /// absent the open fails rather than silently reverting the family to
    /// "no operator", which would make every stored operand unreadable and
    /// every folded value wrong.
    ///
    /// Registering two operators reporting the same `name()` is an error at
    /// open: the engine would otherwise pick one arbitrarily and fold history
    /// with it.
    pub merge_fns: Vec<Arc<dyn MergeOperator>>,
    /// Let compaction collapse an operand chain into the value it folds to
    /// (default `true`).
    ///
    /// A pure space-and-read optimization: without it every operand a family
    /// ever writes stays on disk forever and every read of that key folds the
    /// whole chain again. Turning it off changes no answer — folding is only
    /// correct when it does not — so it exists as a rollout switch: a fold bug
    /// silently rewrites history, and being able to stop new folds without
    /// reverting the binary is worth one boolean.
    ///
    /// **Not persisted.** It is a policy of this process, not of the stored
    /// data, and already-folded entries stay folded either way.
    pub enable_merge_folding: bool,
    /// Bandwidth ceiling for *background* IO — flush, compaction and the part
    /// mover — in bytes per second. `0` (the default) is unlimited, and costs
    /// exactly one nil check per read/write: no limiter object is built.
    ///
    /// Foreground reads and write commits are never delayed by this, and the
    /// WAL is never charged at all: it is foreground durability. Only the
    /// bytes background work moves through SSTable blocks and vlog frames are
    /// paced. See [`crate::ioctrl`].
    ///
    /// **Not persisted.** It describes this host and device, not the stored
    /// data, so it is re-read from `Options` at every open — the same rule
    /// `part_mover_interval` follows.
    pub background_io_bytes_per_second: u64,
    /// Credit a background worker may accrue while idle and then spend at
    /// once, in bytes. `0` derives one second of
    /// [`background_io_bytes_per_second`](Self::background_io_bytes_per_second),
    /// the smallest burst that lets a steady producer actually reach the
    /// configured rate. Ignored when the rate is `0`. Not persisted.
    pub background_io_burst_bytes: u64,
    /// Rate at which obsolete SSTable files are unlinked, in bytes per second.
    ///
    /// `0` (the default) unlinks them inline on the thread that obsoleted them,
    /// which is what every release before 0.6 did: no channel, no worker
    /// thread, no added latency. Any other value spawns a single deletion
    /// worker that unlinks in FIFO order, charging each file's size (at least
    /// [`crate::DELETE_METADATA_BYTES`], since an unlink costs metadata IO even
    /// for an empty file) under [`IoClass::ObsoleteDelete`](crate::ioctrl::IoClass::ObsoleteDelete).
    ///
    /// It is a separate rate from
    /// [`background_io_bytes_per_second`](Self::background_io_bytes_per_second)
    /// because deletion is a metadata storm rather than a bandwidth one: a
    /// compaction that retires two hundred files moves almost no bytes and can
    /// still stall a device. When
    /// [`io_limiter`](Self::io_limiter) is set it governs both — an embedder
    /// supplying its own limiter is describing one device budget.
    ///
    /// Deletion *order* is never load-bearing (file ids are never reused), and
    /// a pause ([`DB::backup`](crate::DB::backup),
    /// [`DB::checkpoint`](crate::DB::checkpoint)) still defers every unlink
    /// regardless of this setting. Not persisted.
    pub obsolete_delete_bytes_per_second: u64,
    /// Maximum number of parallel **spans** one compaction job may split its
    /// user-key range into (0.8). `0` and `1` both mean today's behavior: one
    /// span, one merge, no extra thread. Default `1`.
    ///
    /// A job that qualifies partitions its key range at comparator-aware
    /// boundaries — partition cuts where it writes bottom output, target-table
    /// `min_key`s otherwise — and merges the pieces concurrently into one
    /// atomic install. Every version of a user key stays in one span, so the
    /// result is logically identical to the single-span merge; the *files* are
    /// not, since their boundaries and ids differ.
    ///
    /// It applies to capacity-triggered level >= 1 jobs and L0 -> L1
    /// oldest-window jobs. [`DB::compact`](crate::DB::compact)'s whole-level
    /// sweep, the in-place bottom rewrite, FIFO eviction and any column family
    /// with a compaction filter installed stay single-span — the filter because
    /// it is written against a single-threaded, key-ordered traversal.
    ///
    /// The actual span count is
    /// `min(max_subcompactions, useful boundaries + 1, free span permits + 1)`,
    /// reported per job as [`CfStats::span_count`](crate::CfStats::span_count).
    ///
    /// **Not persisted.** It describes this host, not the stored data — the
    /// same rule [`background_io_bytes_per_second`](Self::background_io_bytes_per_second)
    /// follows.
    pub max_subcompactions: usize,
    /// Size of the DB-wide pool of span-worker threads
    /// ([`max_subcompactions`](Self::max_subcompactions)). `0` (the default)
    /// derives [`num_compaction_threads`](Self::num_compaction_threads).
    ///
    /// The pool is deliberately sized *independently* of the compaction worker
    /// count, and a job's coordinator consumes nothing from it: the coordinator
    /// runs span 0 on the compaction thread it already occupies, which
    /// `num_compaction_threads` already accounts for. Permits gate only the
    /// extra threads. Sizing one shared pool for both instead makes the feature
    /// a no-op at the defaults — two concurrent jobs would take both permits as
    /// coordinators and no span worker could ever run.
    ///
    /// Acquisition never blocks: a job takes what is free and runs fewer spans
    /// if it cannot have them all. Not persisted.
    pub max_subcompaction_workers: usize,
    /// Replace the built-in token bucket with a caller-supplied limiter.
    ///
    /// Set, this overrides
    /// [`background_io_bytes_per_second`](Self::background_io_bytes_per_second)
    /// entirely. It exists so tests can observe what the engine charges, and so
    /// an embedder can enforce a policy the engine has no view of (a
    /// device-wide budget shared with another process, say). Not persisted.
    pub io_limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>,
    /// Total bytes unresolved prepared transactions (3.2) may hold in memory,
    /// summed across every prepare this database has not yet resolved.
    ///
    /// A **byte** cap, not a count, because that is what the resource actually
    /// is: `Txn::prepare` moves the transaction's whole write arena into the
    /// registry and holds it until a coordinator resolves the transaction, for
    /// an unbounded time. A prepare that would take the database past this
    /// fails with [`OndaError::TooLarge`](crate::OndaError::TooLarge) and
    /// registers nothing.
    ///
    /// The cap governs **new** prepares only. Prepared state recovered from the
    /// WAL is admitted whatever its size — refusing to open a database because
    /// of durable state on disk would leave an operator no way to see, let
    /// alone abort, what is holding it. Use
    /// [`DB::list_prepared`](crate::DB::list_prepared) for that.
    ///
    /// **Not persisted.** It describes this host's memory, not the stored data
    /// — the same rule [`max_subcompactions`](Self::max_subcompactions)
    /// follows.
    pub max_prepared_bytes: usize,
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
    /// Whether this tier's root is SHARED between databases (A2,
    /// `SPADINO-A2.md`). On a shared tier: part moves name their objects
    /// `cf-{cf}/{instance:016x}-{id}` (collision-free across databases),
    /// [`attach_part_by_ref`](crate::DB::attach_part_by_ref) may mount other
    /// databases' immutable parts without copying, and this engine NEVER
    /// deletes an object (reclaim belongs to the layer above — one sharer's
    /// hygiene must not be another's data loss). Defaults to `false`: a
    /// non-shared tier behaves exactly as before A2 existed, byte-for-byte.
    pub shared: bool,
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
            shared: false,
            backend: TierBackend::Local,
        }
    }

    /// Disable mmap reads for this tier (route reads through the buffered
    /// `pread` path + block cache, as a remote tier would).
    pub fn without_mmap(mut self) -> Self {
        self.supports_mmap = false;
        self
    }

    /// Declare this tier's root SHARED between databases (A2): part moves get
    /// collision-free object names, `attach_part_by_ref` may mount other
    /// databases' parts, and this engine never deletes an object on the tier.
    /// See [`TierDef::shared`](Self::shared) (the field) for the contract.
    pub fn shared(mut self) -> Self {
        self.shared = true;
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
            shared: false,
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
            shared: false,
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
            max_open_readers: crate::table_cache::DEFAULT_MAX_OPEN_READERS,
            max_open_reader_bytes: crate::table_cache::DEFAULT_MAX_OPEN_READER_BYTES,
            max_open_sstables: 256,
            max_memory_usage: 0, // reserved; currently ignored
            read_only: false,
            finish_compactions_on_close: false,
            max_concurrent_flushes: 0, // reserved; currently ignored
            unified_memtable: false,
            unified_memtable_write_buffer_size: 64 << 20,
            unified_memtable_skip_list_max_level: 12,
            unified_memtable_skip_list_probability: 0.25,
            unified_memtable_sync_mode: SyncMode::None,
            unified_memtable_sync_interval: Duration::from_micros(128_000),
            unified_memtable_stall_threshold: 6,
            span_index_capacity: 16 << 10,
            migrate_to_unified: false,
            tiers: Vec::new(),
            part_mover_interval: Duration::from_secs(30),
            partition_fns: Vec::new(),
            merge_fns: Vec::new(),
            enable_merge_folding: true,
            background_io_bytes_per_second: 0, // unlimited: no limiter object
            background_io_burst_bytes: 0,
            obsolete_delete_bytes_per_second: 0, // unlink inline: no worker thread
            max_subcompactions: 1,               // one span: today's behavior, no extra thread
            max_subcompaction_workers: 0,        // derive num_compaction_threads
            io_limiter: None,
            max_prepared_bytes: 64 << 20,
        }
    }
}

/// Per-column-family configuration.
#[derive(Debug, Clone)]
pub struct ColumnFamilyConfig {
    pub write_buffer_size: usize,
    pub level_size_ratio: u64,
    /// Reserved level-geometry knob; level count is currently data-derived.
    pub min_levels: u32,
    /// Reserved level-geometry knob; currently ignored.
    pub dividing_level_offset: i32,
    pub klog_value_threshold: usize,
    /// Target size of an SSTable data block, in bytes. Default 4 KiB.
    ///
    /// Blocks are self-describing, so this is a per-family write policy rather
    /// than a format choice. Larger blocks improve compression windows at the
    /// cost of decompressing more bytes for a point read.
    pub data_block_size: usize,
    /// Store each data-block key as the bytes it does not share with its
    /// predecessor (2.1). Default `false`: legacy full-key blocks.
    ///
    /// A write policy, never a read one — tables already written keep their own
    /// encoding, which is recorded in their footer, and both formats coexist in
    /// any level, part, checkpoint or attach. Turning it on is inert until
    /// [`CAP_PREFIX_DELTA`](crate::format::CAP_PREFIX_DELTA) is durably
    /// enabled: a table this binary writes must be refused outright by a binary
    /// too old to decode it, and the capability word in the manifest is what
    /// makes that refusal happen.
    pub enable_prefix_delta_keys: bool,
    /// Entries per in-block restart point. Default
    /// [`RESTART_INTERVAL`](crate::sst::RESTART_INTERVAL) (8); valid range
    /// `[1, 1024]`.
    ///
    /// A restart anchor is self-contained, so the interval trades index density
    /// (bounded seek and reverse-step cost, `4` trailer bytes per anchor)
    /// against how many full keys a block repeats. It applies whether or not
    /// `enable_prefix_delta_keys` is set.
    ///
    /// `0` is **not** accepted even though `WriterOptions::restart_interval`
    /// takes it: there it means "emit no restart trailer at all", the legacy
    /// block shape, which stays reachable only by constructing `WriterOptions`
    /// directly. Mapping a config `0` to the default would make the config
    /// value mean the opposite of the writer value it feeds.
    pub block_restart_interval: usize,
    /// Byte ceiling on a decoded vlog value the block cache may hold. `0`
    /// (the default) disables vlog value caching entirely.
    ///
    /// A separated value costs a positional read plus a CRC verify plus (v2) a
    /// decompression on **every** access — `Reader::vlog_verified` memoizes
    /// the checksum, never the bytes. Caching the decoded bytes removes all
    /// three for hot values, at the cost of sharing the block cache's fixed
    /// capacity with klog data blocks. Off by default because that trade is
    /// workload-specific; a practical starting value is 1 MiB.
    ///
    /// Must be either 0 or at least `klog_value_threshold` — nothing shorter
    /// than the threshold ever reaches the vlog.
    pub max_cached_vlog_value_bytes: usize,
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
    /// How partitions are decided: the
    /// [`partition_rules`](Self::partition_rules) vector (the default, and the
    /// behaviour of every earlier release) or a computed
    /// [`PartitionFn`](crate::PartitionFn).
    ///
    /// Setting [`PartitionScheme::Derived`] makes `partition_rules` inert. The
    /// scheme is *policy*, like the rules it replaces: changing it rewrites
    /// nothing, and bottom SSTables keep whatever partition they were cut with
    /// until a later compaction rewrites them.
    ///
    /// Only the [`PartitionFn::scheme_name`](crate::PartitionFn::scheme_name)
    /// is persisted; register the implementation in
    /// [`Options::partition_fns`] so reopening can resolve it.
    pub partition_scheme: PartitionScheme,
    /// Name of this family's merge operator, or `None` (the default) for a
    /// family that has none. Persisted in the manifest blob.
    ///
    /// Setting it on `create_column_family` durably enables
    /// [`CAPS_MERGE_WRITE`](crate::format::CAPS_MERGE_WRITE) and requires an
    /// implementation of the same name in [`Options::merge_fns`]. Reopening a
    /// family that stored a name without registering that implementation is an
    /// error — see [`MergeOperator`]. The **stored** name always wins: a
    /// different name supplied at reopen cannot rename a family's operator,
    /// because that would fold already-written operands with the wrong
    /// function.
    pub merge_operator_name: Option<String>,
    /// The implementation behind [`merge_operator_name`](Self::merge_operator_name),
    /// resolved from [`Options::merge_fns`] at create/open. Never persisted.
    ///
    /// `pub` only because [`ColumnFamilyConfig`] is built with struct-update
    /// syntax; the engine overwrites whatever a caller leaves here from the
    /// stored name, so setting it by hand changes nothing.
    #[doc(hidden)]
    pub merge_operator: Option<Arc<dyn MergeOperator>>,
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
    /// Per-level override of [`bloom_fpr`](Self::bloom_fpr). Empty (the
    /// default) = the uniform `bloom_fpr` at every level. Otherwise a table
    /// written into level L is filtered at `bloom_fpr_per_level[min(L,
    /// len-1)]` — the last entry repeats for all deeper levels, exactly like
    /// [`compression_per_level`](Self::compression_per_level).
    ///
    /// The intended shape is a *strong* (small) FPR for the small upper levels,
    /// where a filter costs little memory, and a weaker one for the huge bottom
    /// level, where it costs the most. Filters are sized from the keys a table
    /// actually holds, so the setting is honest per table.
    ///
    /// Every entry must be finite and strictly inside `(0, 1)`
    /// ([`validate`](Self::validate) rejects the rest). Purely a write-side
    /// policy: changing it rewrites nothing, and existing tables keep the
    /// filter they were written with until a later compaction rewrites them.
    pub bloom_fpr_per_level: Vec<f64>,
    /// Omit the bloom filter entirely on **compaction** output written into the
    /// bottom level (`false` by default).
    ///
    /// The bottom level holds most of the data and therefore most of the filter
    /// bytes. A workload whose point reads almost always hit pays for those
    /// bytes and gets nothing: the filter is consulted, admits the key, and the
    /// table is read anyway. Dropping it trades negative-lookup speed for
    /// resident memory. It is the wrong trade for a miss-heavy workload — a
    /// miss that reaches the bottom level now always reads a block.
    ///
    /// Flush and ingestion output is never affected: those always write L0, and
    /// in a young column family L0 *is* the bottom level, so honouring the
    /// option there would strip the filter from every table the family has.
    ///
    /// **The degradation is one-way.** "Bottom" is dynamic: a table written
    /// filterless while level N was bottom keeps no filter once a deeper level
    /// appears, and nothing retro-fits one. Reads stay correct (a missing
    /// filter means "may contain"), but negative lookups against that table
    /// stay degraded until it is recompacted. Enabling this is a decision about
    /// the data already in the bottom level as much as about future writes.
    /// The converse is guaranteed: any compaction whose output target is *not*
    /// bottom writes a filter, whatever its inputs carried.
    pub optimize_filters_for_hits: bool,
    /// Reserved sampled-index policy; indexes are currently exhaustive.
    pub enable_block_indexes: bool,
    /// Reserved sampled-index policy; currently ignored.
    pub index_sample_ratio: u32,
    /// Reserved sampled-index policy; currently ignored.
    pub block_index_prefix_len: usize,
    pub sync_mode: SyncMode,
    pub sync_interval: Duration,
    pub comparator_name: String,
    /// Reserved comparator context; built-in comparators currently take none.
    pub comparator_ctx_str: String,
    /// Reserved memtable tuning; the implementation uses fixed constants.
    pub skip_list_max_level: u32,
    /// Reserved memtable tuning; the implementation uses a fixed probability.
    pub skip_list_probability: f64,
    /// Reserved per-CF default; [`DB::begin`](crate::DB::begin) currently uses
    /// Snapshot and explicit callers choose via `begin_with_isolation`.
    pub default_isolation_level: IsolationLevel,
    /// Reserved for a future disk-space admission guard; currently ignored.
    pub min_disk_space: u64,
    pub l1_file_count_trigger: u32,
    pub l0_queue_stall_threshold: u32,
    /// Reserved tombstone-density trigger; currently ignored.
    pub tombstone_density_trigger: f64,
    /// Reserved tombstone-density trigger; currently ignored.
    pub tombstone_density_min_entries: u64,
    pub use_btree: bool,
    pub compaction_style: CompactionStyle,
    /// FIFO only: evict oldest tables once the CF's total bytes exceed this
    /// (0 = no size limit).
    pub fifo_max_bytes: u64,
    /// FIFO only: evict tables whose klog file is older than this
    /// (zero = no age limit).
    pub fifo_ttl: Duration,
    /// Revisit a table this long after the compaction that wrote it, even when
    /// no size or file-count trigger fires (feature 0.3). `Duration::ZERO` —
    /// the default — disables periodic compaction entirely.
    ///
    /// The point is an **idle** database: today reclamation only happens inside
    /// a compaction, and background compactions are triggered by L0 file count
    /// or level bytes, so a family that stops taking writes keeps its expired
    /// TTL entries, tombstones and shadowed versions forever (short of a manual
    /// [`DB::compact`](crate::DB::compact)). With this set, the compaction
    /// worker scans every `interval / 4` (clamped to `[1s, 15m]`) and enqueues
    /// a family holding a table whose
    /// [`last_compaction_time`](crate::manifest::SstMeta::last_compaction_time)
    /// is at least `interval` old.
    ///
    /// Requires [`CAP_PERIODIC_AGE`](crate::format::CAP_PERIODIC_AGE): without
    /// it no table carries the stamp and nothing is ever eligible. Invalid on a
    /// [`CompactionStyle::Fifo`] family, which has its own age eviction
    /// ([`fifo_ttl`](Self::fifo_ttl)).
    ///
    /// Periodic work is strictly the **lowest** priority: it is picked only
    /// when no level is over capacity, and it adds **no new retention rule** —
    /// the same snapshot/TTL/tombstone logic every other compaction uses
    /// decides what a periodic rewrite may drop.
    pub periodic_compaction_interval: Duration,
    /// Size at which compaction cuts an output SSTable.
    ///
    /// This is what makes a compaction's work *bounded*. Compaction picks one
    /// input file and merges it with the target-level files its key range
    /// overlaps, so the cost of a single job is roughly
    /// `target_file_size * (1 + level_size_ratio)` — independent of how large
    /// the level has grown. Before 0.8.0 output was cut at
    /// [`write_buffer_size`](Self::write_buffer_size) and the L1 cap was the
    /// same value, so L1 held exactly one file spanning the whole keyspace and
    /// every push-down rewrote the entire level below: work per compaction grew
    /// with the dataset, and sustained ingest accumulated unbounded debt.
    ///
    /// Smaller values make compaction finer-grained (and more parallelizable)
    /// at the cost of more files, each holding a block index and bloom filter
    /// while open — see [`Options::max_open_reader_bytes`].
    pub target_file_size: usize,
    /// Byte capacity of L1; deeper levels are this times
    /// [`level_size_ratio`](Self::level_size_ratio) per level.
    ///
    /// Held separately from [`write_buffer_size`](Self::write_buffer_size) so
    /// the number of files per level (`l1_base_bytes / target_file_size`) can be
    /// chosen independently of memtable size. A level that holds only one file
    /// cannot be compacted partially, because that file's range covers
    /// everything below it.
    pub l1_base_bytes: u64,
    /// Estimated pending-compaction bytes above which each commit is delayed in
    /// proportion to the excess, slowing writers smoothly as compaction falls
    /// behind. `0` disables pacing.
    ///
    /// Without this, ingest runs at memtable speed no matter how far compaction
    /// lags: the write returns quickly, the debt is paid later at close or by
    /// whoever reads next, and reported throughput is a rate the engine cannot
    /// actually sustain.
    pub soft_pending_compaction_bytes: u64,
    /// Estimated pending-compaction bytes above which commits block until a
    /// compaction completes. `0` disables the hard stop.
    ///
    /// This is the ceiling that bounds debt (and therefore disk footprint and
    /// read amplification) when ingest simply outruns compaction. Must be >=
    /// [`soft_pending_compaction_bytes`](Self::soft_pending_compaction_bytes);
    /// [`validate`](Self::validate) rejects the inversion.
    pub hard_pending_compaction_bytes: u64,
}

impl Default for ColumnFamilyConfig {
    fn default() -> Self {
        ColumnFamilyConfig {
            write_buffer_size: 64 << 20, // 64 MiB
            level_size_ratio: 10,
            min_levels: 1,
            dividing_level_offset: 1,
            klog_value_threshold: 512, // WiscKey separation threshold
            data_block_size: crate::column_family::DEFAULT_DATA_BLOCK_SIZE,
            enable_prefix_delta_keys: false, // legacy full-key blocks
            block_restart_interval: crate::sst::RESTART_INTERVAL,
            max_cached_vlog_value_bytes: 0, // vlog value caching off
            compression: Compression::None,
            compression_per_level: Vec::new(),
            compression_rules: Vec::new(),
            partition_rules: Vec::new(),
            partition_scheme: PartitionScheme::Rules,
            merge_operator_name: None,
            merge_operator: None,
            tier_rules: Vec::new(),
            enable_bloom_filter: true,
            bloom_fpr: 0.01,
            bloom_fpr_per_level: Vec::new(),
            optimize_filters_for_hits: false,
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
            periodic_compaction_interval: Duration::ZERO,
            target_file_size: 16 << 20,             // 16 MiB
            l1_base_bytes: 256 << 20,               // 256 MiB => ~16 files in L1
            soft_pending_compaction_bytes: 2 << 30, // 2 GiB
            hard_pending_compaction_bytes: 8 << 30, // 8 GiB
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

/// A partitioner that **computes** a key's partition instead of looking it up
/// in a rule vector.
///
/// [`partition_rules`](ColumnFamilyConfig::partition_rules) enumerates
/// partitions, which is the right shape when there are a handful of them and
/// the wrong shape when the partition is a function of the key. A consumer
/// keying by `(tenant, time-bucket)` needs one partition per pair — thousands
/// per column family — which no enumeration can carry (the durable rule vector
/// is bounded, and resolution is a longest-prefix scan of it for *every key* a
/// bottom compaction writes).
///
/// A `PartitionFn` replaces the vector with two functions of the key. The
/// engine's cutting mechanism is unchanged: a part is still a contiguous key
/// range, still cut only at the bottom level, still write-side-only policy.
///
/// # Contract
///
/// Implementations MUST satisfy all of the following. The engine does not (and
/// cannot cheaply) verify them, and violating them produces parts that are not
/// contiguous key ranges — which breaks detach/attach, freeze, and tiering.
///
/// 1. **Prefix-determined.** `boundary_len(k)` MUST be `<= k.len()`, and any
///    two keys sharing the prefix `k[..boundary_len(k)]` MUST yield the same
///    boundary length and the same [`name`](Self::name).
/// 2. **Order-compatible.** Under the column family's comparator, keys of one
///    partition MUST form a contiguous range. Deriving the boundary from a
///    leading, order-significant portion of the key satisfies this; hashing
///    does not.
/// 3. **Pure and stable.** Same key ⇒ same answer, for the life of the data.
/// 4. **Cheap.** Called once per key written by a bottom compaction.
///
/// # Example
///
/// ```
/// use ondadb::{PartitionFn, PartitionScheme};
/// use std::sync::Arc;
///
/// /// Keys are `<name>\0\0<8-byte bucket><rest>`; a partition is `(name, bucket)`.
/// #[derive(Debug)]
/// struct NameAndBucket;
///
/// impl PartitionFn for NameAndBucket {
///     fn boundary_len(&self, key: &[u8]) -> usize {
///         match key.windows(2).position(|w| w == [0, 0]) {
///             // terminator + the 8-byte bucket that follows it
///             Some(end) => (end + 2 + 8).min(key.len()),
///             None => key.len(),
///         }
///     }
///     fn name(&self, key: &[u8]) -> String {
///         hex(&key[..self.boundary_len(key)])
///     }
///     fn scheme_name(&self) -> &str {
///         "example.name-and-bucket.v1"
///     }
/// }
/// # fn hex(b: &[u8]) -> String { b.iter().map(|x| format!("{x:02x}")).collect() }
/// let scheme = PartitionScheme::Derived(Arc::new(NameAndBucket));
/// ```
pub trait PartitionFn: Send + Sync + std::fmt::Debug {
    /// Length of the prefix of `key` that determines its partition.
    ///
    /// Bottom compaction cuts its output whenever `key[..boundary_len(key)]`
    /// changes, so this is the function that decides part boundaries.
    fn boundary_len(&self, key: &[u8]) -> usize;

    /// Stable, unique name for the partition containing `key`.
    ///
    /// Recorded on the bottom-level SSTables cut on this boundary and used to
    /// address the part in `detach_part` / `freeze_part` / `move_part_to_tier`,
    /// so it must be filesystem-safe: it becomes a directory component.
    fn name(&self, key: &[u8]) -> String;

    /// Stable identifier for *this partitioner*, persisted in the column
    /// family's manifest blob.
    ///
    /// A boxed function cannot be serialized, so the manifest records this name
    /// and the caller re-supplies the implementation through
    /// [`Options::partition_fns`] when reopening — the same
    /// name-plus-registry indirection the engine already uses for comparators.
    /// Reopening with a different or missing implementation is an error rather
    /// than a fallback, because silently resolving keys with the wrong
    /// partitioner would mis-cut every part written afterwards.
    ///
    /// Change it whenever the boundary or naming behaviour changes.
    fn scheme_name(&self) -> &str;
}

/// How a column family decides which partition a key belongs to.
///
/// Defaults to [`Rules`](Self::Rules), which is the behaviour of every release
/// before derived partitioning existed.
#[derive(Clone, Default)]
pub enum PartitionScheme {
    /// Longest-matching-prefix over
    /// [`partition_rules`](ColumnFamilyConfig::partition_rules).
    #[default]
    Rules,
    /// Boundaries computed from the key by a [`PartitionFn`].
    ///
    /// [`partition_rules`](ColumnFamilyConfig::partition_rules) is ignored
    /// while this is set.
    Derived(Arc<dyn PartitionFn>),
    /// A derived scheme read from the manifest whose implementation has not
    /// been supplied yet.
    ///
    /// Only `ColumnFamilyConfig::decode` produces this: the manifest carries
    /// the scheme *name*, and `DB::open` exchanges it for the registered
    /// implementation in [`Options::partition_fns`], failing if there is none.
    /// Encoding a config in this state preserves the name, so a column family
    /// cannot be silently demoted to rule-based partitioning by a round trip
    /// through a reader that could not resolve it.
    Unresolved(String),
}

impl std::fmt::Debug for PartitionScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionScheme::Rules => f.write_str("Rules"),
            PartitionScheme::Derived(p) => {
                write!(f, "Derived({})", p.scheme_name())
            }
            PartitionScheme::Unresolved(n) => write!(f, "Unresolved({n})"),
        }
    }
}

/// Deterministic per-column-family fold of merge operands (feature 1.1).
///
/// A `merge` writes an *operand* — a value that is not yet the record. Reads,
/// compaction and flush fold the operands of one key against the value below
/// them by calling [`full_merge`](Self::full_merge). This removes the
/// read-modify-write round trip a counter or a set-union otherwise pays: under
/// MVCC that round trip costs a snapshot `get` **plus** the write-write
/// conflict window.
///
/// # Contract
///
/// 1. **Deterministic and pure.** The same `(key, existing, operands)` must
///    always produce the same bytes. Compaction folds a prefix of the chain at
///    unpredictable times and may re-fold the result later, so a non-pure
///    operator makes the stored value depend on when compaction ran.
/// 2. **Associative over the chain.** Folding `[a, b, c]` against `E` must
///    equal folding `[c]` against `full_merge(k, E, [a, b])` — that identity is
///    exactly what compaction folding relies on.
/// 3. **Total.** An operand this operator cannot interpret must return `Err`,
///    which surfaces as [`OndaError::Corruption`](crate::OndaError) naming the
///    key. It must not panic and must not silently invent a value.
/// 4. **Stable name.** [`name`](Self::name) is persisted in the column
///    family's manifest blob and re-resolved from [`Options::merge_fns`] at
///    every open. Change it whenever the fold's meaning changes; reopening
///    without a registered implementation of the stored name is an error, not
///    a fallback.
///
/// Operands carry **no TTL** (v1): a per-operand expiry would resurrect the
/// base it was folded into. `Options::merge_fns` is where implementations are
/// registered.
///
/// # Example
///
/// ```
/// use ondadb::MergeOperator;
///
/// /// Little-endian i64 counter: operands are deltas.
/// #[derive(Debug)]
/// struct Counter;
///
/// impl MergeOperator for Counter {
///     fn name(&self) -> &str {
///         "example.counter.i64.v1"
///     }
///     fn full_merge(
///         &self,
///         _key: &[u8],
///         existing: Option<&[u8]>,
///         operands: &[&[u8]],
///     ) -> Result<Vec<u8>, String> {
///         let read = |b: &[u8]| -> Result<i64, String> {
///             b.try_into()
///                 .map(i64::from_le_bytes)
///                 .map_err(|_| format!("operand is {} bytes, want 8", b.len()))
///         };
///         let mut acc = match existing {
///             Some(b) => read(b)?,
///             None => 0,
///         };
///         for operand in operands {
///             acc = acc.wrapping_add(read(operand)?);
///         }
///         Ok(acc.to_le_bytes().to_vec())
///     }
/// }
/// ```
pub trait MergeOperator: Send + Sync + std::fmt::Debug {
    /// Stable identifier for this operator, persisted in the column family's
    /// manifest blob (see the contract above).
    fn name(&self) -> &str;

    /// Fold `operands` (**oldest first**) onto `existing`.
    ///
    /// `existing` is `None` when no visible put backs the chain — either the
    /// base is a delete, or the chain reaches the end of history without one.
    /// A real zero-length value is `Some(b"")`, and the distinction is
    /// deliberate: it is the same found/deleted split the point-read path
    /// already carries.
    ///
    /// An `Err` message is surfaced verbatim inside
    /// [`OndaError::Corruption`](crate::OndaError), together with the key.
    fn full_merge(
        &self,
        key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String>;
}

/// A partition resolver snapshotted for the duration of one compaction run.
///
/// Compaction takes one of these at the start of a run so that a concurrent
/// `add_partition_rule` cannot move this run's cut boundaries — the same
/// guarantee the rule-vector snapshot has always given, extended to the
/// derived case.
#[derive(Debug, Clone)]
pub(crate) enum PartitionResolver {
    Rules(Vec<PartitionRule>),
    Derived(Arc<dyn PartitionFn>),
}

impl PartitionResolver {
    /// The partition name for `user_key`, or `None` for the implicit default
    /// partition (rules only — a derived scheme names every key).
    pub(crate) fn name_of(&self, user_key: &[u8]) -> Option<String> {
        match self {
            PartitionResolver::Rules(rules) => partition_of(rules, user_key).map(str::to_string),
            PartitionResolver::Derived(f) => Some(f.name(user_key)),
        }
    }

    /// The bytes that must stay constant within one part.
    ///
    /// For a derived scheme this is `key[..boundary_len(key)]`, which is a
    /// finer and more direct test than comparing names: it depends only on
    /// [`PartitionFn::boundary_len`], so a part is a contiguous key range even
    /// if an implementation's [`name`](PartitionFn::name) collides. For rules
    /// the boundary is the matched prefix.
    pub(crate) fn boundary<'k>(&self, user_key: &'k [u8]) -> Option<&'k [u8]> {
        match self {
            PartitionResolver::Rules(rules) => rules
                .iter()
                .filter(|r| user_key.starts_with(&r.prefix))
                .max_by_key(|r| r.prefix.len())
                .map(|r| &user_key[..r.prefix.len()]),
            PartitionResolver::Derived(f) => {
                let n = f.boundary_len(user_key).min(user_key.len());
                Some(&user_key[..n])
            }
        }
    }
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

    /// Bloom false-positive rate for a table written into `level`, or `None`
    /// when no filter block should be written at all.
    ///
    /// `bottom` is a *compaction* input: it says this output lands in the
    /// deepest populated level (see `compaction::is_bottom_target`). Flush and
    /// ingestion pass `false` unconditionally — see
    /// [`optimize_filters_for_hits`](Self::optimize_filters_for_hits).
    ///
    /// The decision is made from `level` and `bottom` alone and is never
    /// inherited from a compaction's inputs, which is what makes the
    /// re-filter-on-promotion guarantee hold: a filterless table compacted into
    /// a non-bottom target comes back out with a filter.
    pub fn bloom_fpr_for_level(&self, level: u32, bottom: bool) -> Option<f64> {
        if self.optimize_filters_for_hits && bottom {
            return None; // write no filter block at all
        }
        match self.bloom_fpr_per_level.as_slice() {
            // The empty arm is what keeps `v.len() - 1` below off an empty
            // slice: that subtraction is a `usize` underflow, not a fallback.
            [] => Some(self.bloom_fpr),
            v => Some(v[(level as usize).min(v.len() - 1)]),
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
        if self.target_file_size == 0 {
            return Err("target_file_size must be non-zero".to_string());
        }
        if self.data_block_size == 0 {
            return Err("data_block_size must be non-zero".to_string());
        }
        if !(1..=1024).contains(&self.block_restart_interval) {
            return Err(format!(
                "block_restart_interval ({}) must be in [1, 1024]; 0 is not \
                 'the default', it is the writer-level 'no restart trailer'",
                self.block_restart_interval
            ));
        }
        // A limit under the separation threshold is indistinguishable from
        // "off" at runtime but reads as "on" in the config — reject it rather
        // than let it look like a tuning that did nothing.
        if self.max_cached_vlog_value_bytes != 0
            && self.max_cached_vlog_value_bytes < self.klog_value_threshold
        {
            return Err(format!(
                "max_cached_vlog_value_bytes ({}) is below klog_value_threshold ({}), \
                 so no vlog value could ever be cached",
                self.max_cached_vlog_value_bytes, self.klog_value_threshold
            ));
        }
        if self.l1_base_bytes == 0 {
            return Err("l1_base_bytes must be non-zero".to_string());
        }
        // A rate outside (0, 1) has no filter that realizes it: `Bloom::new`
        // would derive a non-positive or zero-length bit array from it, and NaN
        // would propagate silently into the sizing arithmetic. Refuse at
        // configuration time, where the operator can still see why.
        for (level, fpr) in self.bloom_fpr_per_level.iter().enumerate() {
            if !fpr.is_finite() || *fpr <= 0.0 || *fpr >= 1.0 {
                return Err(format!(
                    "bloom_fpr_per_level[{level}] is {fpr}; every entry must be \
                     finite and strictly between 0 and 1"
                ));
            }
        }
        // Pacing that starts after the hard stop can never run, and the
        // inversion reads as a tuning success until debt is already unbounded.
        if self.soft_pending_compaction_bytes != 0
            && self.hard_pending_compaction_bytes != 0
            && self.soft_pending_compaction_bytes > self.hard_pending_compaction_bytes
        {
            return Err(format!(
                "soft_pending_compaction_bytes ({}) exceeds \
                 hard_pending_compaction_bytes ({})",
                self.soft_pending_compaction_bytes, self.hard_pending_compaction_bytes
            ));
        }
        // FIFO never merges — it evicts whole tables by size and file age
        // (`fifo_ttl`). A periodic *rewrite* has nothing to do there, and
        // accepting the option would silently do nothing, reading as a tuning
        // that took effect.
        if !self.periodic_compaction_interval.is_zero()
            && self.compaction_style == CompactionStyle::Fifo
        {
            return Err(
                "periodic_compaction_interval is invalid for CompactionStyle::Fifo; \
                 FIFO evicts by age through fifo_ttl"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Serialize the durable subset of the config for the manifest blob.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        encode_base_config(&mut b, self);
        let counts = encode_legacy_policies(&mut b, self);
        encode_overflow_policies(&mut b, self, counts);
        encode_partition_scheme(&mut b, self);
        encode_compaction_geometry(&mut b, self);
        encode_block_size(&mut b, self);
        encode_vlog_cache(&mut b, self);
        encode_bloom_policy(&mut b, self);
        encode_periodic_interval(&mut b, self);
        encode_prefix_delta(&mut b, self);
        encode_merge_operator(&mut b, self);
        b
    }

    /// The scheme name to persist: from a live derived partitioner, or the one
    /// read from the manifest if it has not been resolved yet.
    ///
    /// The unresolved case matters because a read-only or not-yet-resolved
    /// config must not *lose* the marker when it is re-encoded — dropping it
    /// would silently demote the column family to rule-based partitioning.
    fn derived_scheme_name(&self) -> Option<&str> {
        match &self.partition_scheme {
            PartitionScheme::Derived(f) => Some(f.scheme_name()),
            PartitionScheme::Unresolved(n) => Some(n.as_str()),
            PartitionScheme::Rules => None,
        }
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
/// Tag introducing the derived-partitioner tail (scheme name only).
const CONFIG_PARTITION_FN_MAGIC: &[u8; 8] = b"ONDAPFN1";
/// Tag introducing the 0.8.0 compaction-geometry tail.
const CONFIG_COMPACTION_MAGIC: &[u8; 8] = b"ONDACMP1";
/// Tag introducing the 0.8.1 per-family data-block-size tail.
const CONFIG_BLOCK_SIZE_MAGIC: &[u8; 8] = b"ONDABLK1";
/// Tag introducing the vlog-value-cache tail (feature 0.5).
const CONFIG_VLOG_CACHE_MAGIC: &[u8; 8] = b"ONDAVVC1";
/// Tag introducing the per-level bloom-policy tail (0.1).
const CONFIG_BLOOM_POLICY_MAGIC: &[u8; 8] = b"ONDABLM1";
/// Tag introducing the periodic-compaction interval tail (0.3).
const CONFIG_PERIODIC_MAGIC: &[u8; 8] = b"ONDAPRD1";
/// Tag introducing the prefix-delta key-encoding tail (2.1).
const CONFIG_PREFIX_DELTA_MAGIC: &[u8; 8] = b"ONDAPFX1";
/// Tag introducing the merge-operator-name tail (1.1).
const CONFIG_MERGE_OP_MAGIC: &[u8; 8] = b"ONDAMRG1";
/// Reserved for a future geometric (Monkey-style) auto-allocation policy. It is
/// mutually exclusive with the explicit `bloom_fpr_per_level` vector, so the tag
/// is claimed here to keep the two from ever sharing one; nothing writes or
/// reads it yet.
#[allow(dead_code)]
const CONFIG_BLOOM_AUTO_MAGIC: &[u8; 8] = b"ONDABLM2";

#[derive(Clone, Copy)]
struct LegacyPolicyCounts {
    levels: usize,
    compression: usize,
    partitions: usize,
    tiers: usize,
}

fn encode_base_config(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::{append_u32, append_u64, append_uvarint};

    append_uvarint(b, cfg.comparator_name.len() as u64);
    b.extend_from_slice(cfg.comparator_name.as_bytes());
    b.push(cfg.compression as u8);
    append_u64(b, cfg.write_buffer_size as u64);
    append_u64(b, cfg.level_size_ratio);
    append_u64(b, cfg.klog_value_threshold as u64);
    b.push(u8::from(cfg.enable_bloom_filter));
    append_u64(b, cfg.bloom_fpr.to_bits());
    append_u32(b, cfg.l1_file_count_trigger);
    append_u32(b, cfg.l0_queue_stall_threshold);
    b.push(u8::from(cfg.use_btree));

    // This is the first append-tolerant tail. A legacy blob ending above keeps
    // the defaults because decoding stops before assigning these fields.
    b.push(cfg.sync_mode as u8);
    append_u64(b, cfg.sync_interval.as_micros() as u64);
}

fn encode_legacy_policies(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) -> LegacyPolicyCounts {
    use crate::encoding::{append_u64, append_uvarint};

    let counts = LegacyPolicyCounts {
        levels: cfg.compression_per_level.len().min(u8::MAX as usize),
        compression: cfg.compression_rules.len().min(u8::MAX as usize),
        partitions: cfg.partition_rules.len().min(u8::MAX as usize),
        tiers: cfg.tier_rules.len().min(u8::MAX as usize),
    };
    b.push(counts.levels as u8);
    b.extend(
        cfg.compression_per_level
            .iter()
            .take(counts.levels)
            .map(|c| *c as u8),
    );
    b.push(cfg.compaction_style as u8);
    append_u64(b, cfg.fifo_max_bytes);
    append_u64(b, cfg.fifo_ttl.as_micros() as u64);

    b.push(counts.compression as u8);
    for rule in cfg.compression_rules.iter().take(counts.compression) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        b.push(rule.compression as u8);
    }
    b.push(counts.partitions as u8);
    for rule in cfg.partition_rules.iter().take(counts.partitions) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.name.len() as u64);
        b.extend_from_slice(rule.name.as_bytes());
    }
    b.push(counts.tiers as u8);
    for rule in cfg.tier_rules.iter().take(counts.tiers) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.tier.len() as u64);
        b.extend_from_slice(rule.tier.as_bytes());
        append_u64(b, rule.min_age.as_micros() as u64);
    }
    counts
}

fn has_overflow_policies(cfg: &ColumnFamilyConfig) -> bool {
    cfg.compression_per_level.len() > u8::MAX as usize
        || cfg.compression_rules.len() > u8::MAX as usize
        || cfg.partition_rules.len() > u8::MAX as usize
        || cfg.tier_rules.len() > u8::MAX as usize
}

fn encode_overflow_policies(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig, counts: LegacyPolicyCounts) {
    use crate::encoding::{append_u64, append_uvarint};

    if !has_overflow_policies(cfg) {
        return;
    }
    b.extend_from_slice(CONFIG_OVERFLOW_MAGIC);
    append_uvarint(
        b,
        cfg.compression_per_level
            .len()
            .saturating_sub(counts.levels) as u64,
    );
    b.extend(
        cfg.compression_per_level
            .iter()
            .skip(counts.levels)
            .map(|c| *c as u8),
    );

    append_uvarint(
        b,
        cfg.compression_rules
            .len()
            .saturating_sub(counts.compression) as u64,
    );
    for rule in cfg.compression_rules.iter().skip(counts.compression) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        b.push(rule.compression as u8);
    }
    append_uvarint(
        b,
        cfg.partition_rules.len().saturating_sub(counts.partitions) as u64,
    );
    for rule in cfg.partition_rules.iter().skip(counts.partitions) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.name.len() as u64);
        b.extend_from_slice(rule.name.as_bytes());
    }
    append_uvarint(b, cfg.tier_rules.len().saturating_sub(counts.tiers) as u64);
    for rule in cfg.tier_rules.iter().skip(counts.tiers) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.tier.len() as u64);
        b.extend_from_slice(rule.tier.as_bytes());
        append_u64(b, rule.min_age.as_micros() as u64);
    }
}

fn encode_partition_scheme(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_uvarint;

    let Some(name) = cfg.derived_scheme_name() else {
        return;
    };
    b.extend_from_slice(CONFIG_PARTITION_FN_MAGIC);
    append_uvarint(b, name.len() as u64);
    b.extend_from_slice(name.as_bytes());
}

fn encode_compaction_geometry(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    let defaults = ColumnFamilyConfig::default();
    if cfg.target_file_size == defaults.target_file_size
        && cfg.l1_base_bytes == defaults.l1_base_bytes
        && cfg.soft_pending_compaction_bytes == defaults.soft_pending_compaction_bytes
        && cfg.hard_pending_compaction_bytes == defaults.hard_pending_compaction_bytes
    {
        return;
    }
    b.extend_from_slice(CONFIG_COMPACTION_MAGIC);
    append_u64(b, cfg.target_file_size as u64);
    append_u64(b, cfg.l1_base_bytes);
    append_u64(b, cfg.soft_pending_compaction_bytes);
    append_u64(b, cfg.hard_pending_compaction_bytes);
}

fn encode_block_size(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    if cfg.data_block_size == ColumnFamilyConfig::default().data_block_size {
        return;
    }
    b.extend_from_slice(CONFIG_BLOCK_SIZE_MAGIC);
    append_u64(b, cfg.data_block_size as u64);
}

fn encode_vlog_cache(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    // Eliding the default keeps an untouched family's blob byte-identical to
    // what a pre-0.5 binary wrote.
    if cfg.max_cached_vlog_value_bytes == ColumnFamilyConfig::default().max_cached_vlog_value_bytes
    {
        return;
    }
    b.extend_from_slice(CONFIG_VLOG_CACHE_MAGIC);
    append_u64(b, cfg.max_cached_vlog_value_bytes as u64);
}

/// The per-level bloom-policy tail: `count` levels of IEEE-754 bits, then the
/// `optimize_filters_for_hits` byte. Elided at the defaults so a family that
/// never touches the policy encodes byte-for-byte as earlier releases wrote it.
fn encode_bloom_policy(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    if cfg.bloom_fpr_per_level.is_empty() && !cfg.optimize_filters_for_hits {
        return;
    }
    b.extend_from_slice(CONFIG_BLOOM_POLICY_MAGIC);
    // A fixed-width count, matching the other tails: the vector holds one entry
    // per level and is never large, but a varint here would buy nothing and
    // make the truncation check below less obvious.
    append_u64(b, cfg.bloom_fpr_per_level.len() as u64);
    for fpr in &cfg.bloom_fpr_per_level {
        append_u64(b, fpr.to_bits());
    }
    b.push(u8::from(cfg.optimize_filters_for_hits));
}

#[derive(Clone, Copy)]
struct ConfigCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> ConfigCursor<'a> {
    fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(crate::encoding::read_u32(self.bytes(4)?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(crate::encoding::read_u64(self.bytes(8)?))
    }

    fn uvar(&mut self) -> Option<u64> {
        let (value, used) = crate::encoding::uvarint(self.remaining)?;
        self.remaining = &self.remaining[used..];
        Some(value)
    }

    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.remaining.len() < len {
            return None;
        }
        let (value, remaining) = self.remaining.split_at(len);
        self.remaining = remaining;
        Some(value)
    }

    fn consume_prefix(&mut self, prefix: &[u8]) -> bool {
        let Some(remaining) = self.remaining.strip_prefix(prefix) else {
            return false;
        };
        self.remaining = remaining;
        true
    }

    fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    fn into_remaining(self) -> &'a [u8] {
        self.remaining
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

fn decode_into(p: &[u8], cfg: &mut ColumnFamilyConfig) -> Option<()> {
    let mut cursor = ConfigCursor::new(p);
    decode_base_config(&mut cursor, cfg)?;
    decode_legacy_policies(&mut cursor, cfg)?;
    if cursor.consume_prefix(CONFIG_OVERFLOW_MAGIC) {
        decode_overflow_policies(&mut cursor, cfg)?;
    }
    let p = read_partition_fn_tail(cursor.into_remaining(), cfg);
    let p = read_compaction_tail(p, cfg);
    let p = read_block_size_tail(p, cfg);
    let p = read_vlog_cache_tail(p, cfg);
    let p = read_bloom_policy_tail(p, cfg);
    let p = read_periodic_interval_tail(p, cfg);
    let p = read_prefix_delta_tail(p, cfg);
    read_merge_operator_tail(p, cfg);
    Some(())
}

fn decode_base_config(cursor: &mut ConfigCursor<'_>, cfg: &mut ColumnFamilyConfig) -> Option<()> {
    let name_len = cursor.uvar()? as usize;
    cfg.comparator_name = String::from_utf8_lossy(cursor.bytes(name_len)?).into_owned();
    if let Some(compression) = Compression::from_u8(cursor.byte()?) {
        cfg.compression = compression;
    }
    cfg.write_buffer_size = cursor.u64()? as usize;
    cfg.level_size_ratio = cursor.u64()?;
    cfg.klog_value_threshold = cursor.u64()? as usize;
    cfg.enable_bloom_filter = cursor.byte()? != 0;
    cfg.bloom_fpr = f64::from_bits(cursor.u64()?);
    cfg.l1_file_count_trigger = cursor.u32()?;
    cfg.l0_queue_stall_threshold = cursor.u32()?;
    cfg.use_btree = cursor.byte()? != 0;

    // All remaining fields were appended after the original durable subset.
    // A short legacy blob returns here and leaves their defaults in place.
    if let Some(sync_mode) = SyncMode::from_u8(cursor.byte()?) {
        cfg.sync_mode = sync_mode;
    }
    cfg.sync_interval = std::time::Duration::from_micros(cursor.u64()?);
    Some(())
}

fn decode_legacy_policies(
    cursor: &mut ConfigCursor<'_>,
    cfg: &mut ColumnFamilyConfig,
) -> Option<()> {
    let level_count = cursor.byte()? as usize;
    cfg.compression_per_level = decode_compression_levels(cursor, level_count)?;
    if let Some(style) = CompactionStyle::from_u8(cursor.byte()?) {
        cfg.compaction_style = style;
    }
    cfg.fifo_max_bytes = cursor.u64()?;
    cfg.fifo_ttl = std::time::Duration::from_micros(cursor.u64()?);

    let compression_count = cursor.byte()? as usize;
    cfg.compression_rules = decode_compression_rules(cursor, compression_count)?;
    let partition_count = cursor.byte()? as usize;
    cfg.partition_rules = decode_partition_rules(cursor, partition_count)?;
    let tier_count = cursor.byte()? as usize;
    cfg.tier_rules = decode_tier_rules(cursor, tier_count)?;
    Some(())
}

fn decode_compression_levels(
    cursor: &mut ConfigCursor<'_>,
    count: usize,
) -> Option<Vec<Compression>> {
    let mut levels = Vec::with_capacity(count);
    for _ in 0..count {
        levels.push(Compression::from_u8(cursor.byte()?)?);
    }
    Some(levels)
}

fn decode_compression_rules(
    cursor: &mut ConfigCursor<'_>,
    count: usize,
) -> Option<Vec<CompressionRule>> {
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        rules.push(decode_compression_rule(cursor)?);
    }
    Some(rules)
}

fn decode_compression_rule(cursor: &mut ConfigCursor<'_>) -> Option<CompressionRule> {
    let prefix_len = cursor.uvar()? as usize;
    let prefix = cursor.bytes(prefix_len)?.to_vec();
    let compression = Compression::from_u8(cursor.byte()?)?;
    Some(CompressionRule {
        prefix,
        compression,
    })
}

fn decode_partition_rules(
    cursor: &mut ConfigCursor<'_>,
    count: usize,
) -> Option<Vec<PartitionRule>> {
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        rules.push(decode_partition_rule(cursor)?);
    }
    Some(rules)
}

fn decode_partition_rule(cursor: &mut ConfigCursor<'_>) -> Option<PartitionRule> {
    let prefix_len = cursor.uvar()? as usize;
    let prefix = cursor.bytes(prefix_len)?.to_vec();
    let name_len = cursor.uvar()? as usize;
    let name = String::from_utf8_lossy(cursor.bytes(name_len)?).into_owned();
    Some(PartitionRule { prefix, name })
}

fn decode_tier_rules(cursor: &mut ConfigCursor<'_>, count: usize) -> Option<Vec<TierRule>> {
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        rules.push(decode_tier_rule(cursor)?);
    }
    Some(rules)
}

fn decode_tier_rule(cursor: &mut ConfigCursor<'_>) -> Option<TierRule> {
    let prefix_len = cursor.uvar()? as usize;
    let prefix = cursor.bytes(prefix_len)?.to_vec();
    let tier_len = cursor.uvar()? as usize;
    let tier = String::from_utf8_lossy(cursor.bytes(tier_len)?).into_owned();
    let min_age = std::time::Duration::from_micros(cursor.u64()?);
    Some(TierRule {
        prefix,
        tier,
        min_age,
    })
}

fn decode_overflow_policies(
    cursor: &mut ConfigCursor<'_>,
    cfg: &mut ColumnFamilyConfig,
) -> Option<()> {
    let extra_levels = cursor.uvar()? as usize;
    cfg.compression_per_level
        .reserve(extra_levels.min(cursor.remaining_len()));
    for _ in 0..extra_levels {
        cfg.compression_per_level
            .push(Compression::from_u8(cursor.byte()?)?);
    }

    let extra_compression = cursor.uvar()? as usize;
    cfg.compression_rules
        .reserve(extra_compression.min(cursor.remaining_len()));
    for _ in 0..extra_compression {
        cfg.compression_rules.push(decode_compression_rule(cursor)?);
    }

    let extra_partitions = cursor.uvar()? as usize;
    cfg.partition_rules
        .reserve(extra_partitions.min(cursor.remaining_len()));
    for _ in 0..extra_partitions {
        cfg.partition_rules.push(decode_partition_rule(cursor)?);
    }

    let extra_tiers = cursor.uvar()? as usize;
    cfg.tier_rules
        .reserve(extra_tiers.min(cursor.remaining_len()));
    for _ in 0..extra_tiers {
        cfg.tier_rules.push(decode_tier_rule(cursor)?);
    }
    Some(())
}

/// Read the optional derived-partitioner tail, recording the scheme name for
/// `DB::open` to resolve.
///
/// Absent tail ⇒ rule-based partitioning, which is what every config written
/// before derived schemes existed decodes to. A malformed tail is ignored
/// rather than fatal, matching how the rest of this decoder treats a truncated
/// blob; the consequence is a column family that opens as rule-partitioned,
/// and `DB::open` cannot then mis-resolve it because there is no name to
/// resolve.
/// Consume the derived-partitioner tail if present, returning what follows it
/// so later tails can be read in turn.
fn read_partition_fn_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::uvarint;
    let Some(rest) = p.strip_prefix(CONFIG_PARTITION_FN_MAGIC) else {
        return p;
    };
    let Some((len, n)) = uvarint(rest) else {
        return p;
    };
    let rest = &rest[n..];
    let len = len as usize;
    if rest.len() < len {
        return p;
    }
    cfg.partition_scheme =
        PartitionScheme::Unresolved(String::from_utf8_lossy(&rest[..len]).into_owned());
    &rest[len..]
}

/// Consume the 0.8.0 compaction-geometry tail if present. Absent (every
/// manifest written before 0.8.0, and any config left at the defaults), the
/// struct defaults stand.
fn read_compaction_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::read_u64;
    let Some(mut rest) = p.strip_prefix(CONFIG_COMPACTION_MAGIC) else {
        return p;
    };
    let mut next = || -> Option<u64> {
        if rest.len() < 8 {
            return None;
        }
        let v = read_u64(rest);
        rest = &rest[8..];
        Some(v)
    };
    // All four or none: a truncated tail leaves every field at its default
    // rather than applying a half-read geometry.
    let (Some(tfs), Some(l1), Some(soft), Some(hard)) = (next(), next(), next(), next()) else {
        return p;
    };
    cfg.target_file_size = tfs as usize;
    cfg.l1_base_bytes = l1;
    cfg.soft_pending_compaction_bytes = soft;
    cfg.hard_pending_compaction_bytes = hard;
    rest
}

fn read_block_size_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_BLOCK_SIZE_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    let value = crate::encoding::read_u64(rest) as usize;
    if value != 0 {
        cfg.data_block_size = value;
    }
    &rest[8..]
}

fn read_vlog_cache_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_VLOG_CACHE_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    // Unlike the block size, 0 is a meaningful value here (disabled) — but the
    // encoder elides it, so a stored 0 can only come from a truncated or
    // hand-edited blob. Take it at face value: it is also the default.
    cfg.max_cached_vlog_value_bytes = crate::encoding::read_u64(rest) as usize;
    &rest[8..]
}

/// Consume the per-level bloom-policy tail if present. Absent (every manifest
/// written before 0.1, and any family left at the defaults), the struct
/// defaults stand — an empty vector and `optimize_filters_for_hits == false`,
/// which is exactly the uniform behaviour of earlier releases.
///
/// All-or-nothing, like the compaction tail: a truncated tail leaves both
/// fields at their defaults rather than applying a half-read policy that would
/// silently filter some levels and not others.
///
/// Returns the unconsumed remainder so later tails can be chained behind it. A
/// rejected (absent, short or invalid) tail returns `p` untouched — the next
/// reader then fails its own `strip_prefix` and also falls back to defaults,
/// which is the intended all-or-nothing behaviour for a damaged blob.
fn read_bloom_policy_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::read_u64;

    let Some(rest) = p.strip_prefix(CONFIG_BLOOM_POLICY_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    let count = read_u64(rest) as usize;
    let rest = &rest[8..];
    // `count` comes off disk, so the size it implies is computed with checked
    // arithmetic — a lying count must fail the bounds check, not wrap past it —
    // and the bytes must actually be present before anything is reserved.
    let Some(needed) = count.checked_mul(8).and_then(|n| n.checked_add(1)) else {
        return p;
    };
    if rest.len() < needed {
        return p;
    }
    let mut per_level = Vec::with_capacity(count);
    for i in 0..count {
        let fpr = f64::from_bits(read_u64(&rest[i * 8..]));
        // A blob whose rates would not `validate` is not made valid by having
        // been written: fall back to uniform rather than hand a NaN to the
        // filter sizer.
        if !fpr.is_finite() || fpr <= 0.0 || fpr >= 1.0 {
            return p;
        }
        per_level.push(fpr);
    }
    cfg.bloom_fpr_per_level = per_level;
    cfg.optimize_filters_for_hits = rest[count * 8] != 0;
    &rest[needed..]
}

/// The 0.3 periodic-compaction interval tail: one `u64` of microseconds.
/// Elided at the default (zero, disabled) so a family that never sets it
/// encodes byte-for-byte as earlier releases wrote it.
fn encode_periodic_interval(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    if cfg.periodic_compaction_interval.is_zero() {
        return;
    }
    b.extend_from_slice(CONFIG_PERIODIC_MAGIC);
    append_u64(b, cfg.periodic_compaction_interval.as_micros() as u64);
}

/// Consume the periodic-compaction tail if present. Absent (every blob written
/// before 0.3, and any family that left the option at zero), the default stands
/// — `Duration::ZERO`, which disables the trigger.
/// Returns the unconsumed remainder so later tails can be chained behind it.
fn read_periodic_interval_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_PERIODIC_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    cfg.periodic_compaction_interval =
        std::time::Duration::from_micros(crate::encoding::read_u64(rest));
    &rest[8..]
}

/// The 2.1 prefix-delta tail: `enabled u8 | block_restart_interval u64 LE`.
/// Elided when both fields are at their defaults, so a family that never sets
/// them encodes byte-for-byte as earlier releases wrote it.
fn encode_prefix_delta(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    let default = ColumnFamilyConfig::default();
    if !cfg.enable_prefix_delta_keys && cfg.block_restart_interval == default.block_restart_interval
    {
        return;
    }
    b.extend_from_slice(CONFIG_PREFIX_DELTA_MAGIC);
    b.push(u8::from(cfg.enable_prefix_delta_keys));
    append_u64(b, cfg.block_restart_interval as u64);
}

/// Consume the prefix-delta tail if present. All-or-nothing, like the tails
/// before it: a truncated or out-of-range tail leaves both fields at their
/// defaults rather than applying half a policy — and an interval outside
/// `[1, 1024]` would not survive `validate`, so it is not made valid by having
/// been written.
fn read_prefix_delta_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_PREFIX_DELTA_MAGIC) else {
        return p;
    };
    if rest.len() < 9 {
        return p;
    }
    let interval = crate::encoding::read_u64(&rest[1..]) as usize;
    if !(1..=1024).contains(&interval) {
        return p;
    }
    cfg.enable_prefix_delta_keys = rest[0] != 0;
    cfg.block_restart_interval = interval;
    &rest[9..]
}

/// The 1.1 merge-operator tail: `name_len uvarint | name`. Elided entirely for
/// a family with no operator, so a family that never sets one encodes
/// byte-for-byte as earlier releases wrote it.
fn encode_merge_operator(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_uvarint;

    let Some(name) = cfg.merge_operator_name.as_deref() else {
        return;
    };
    b.extend_from_slice(CONFIG_MERGE_OP_MAGIC);
    append_uvarint(b, name.len() as u64);
    b.extend_from_slice(name.as_bytes());
}

/// Consume the merge-operator tail if present. All-or-nothing, like the tails
/// before it: a truncated tail leaves `merge_operator_name` at `None`, which is
/// how a pre-1.1 blob decodes and is the only safe default — the resolver then
/// simply has nothing to look up.
fn read_merge_operator_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::uvarint;
    let Some(rest) = p.strip_prefix(CONFIG_MERGE_OP_MAGIC) else {
        return p;
    };
    let Some((len, n)) = uvarint(rest) else {
        return p;
    };
    let rest = &rest[n..];
    let len = len as usize;
    if rest.len() < len {
        return p;
    }
    cfg.merge_operator_name = Some(String::from_utf8_lossy(&rest[..len]).into_owned());
    &rest[len..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_config_emits_no_prefix_delta_tail() {
        let blob = ColumnFamilyConfig::default().encode();
        assert!(
            !blob
                .windows(CONFIG_PREFIX_DELTA_MAGIC.len())
                .any(|w| w == CONFIG_PREFIX_DELTA_MAGIC),
            "a family at the defaults must encode as earlier releases wrote it"
        );
        let decoded = ColumnFamilyConfig::decode(&blob);
        assert!(!decoded.enable_prefix_delta_keys);
        assert_eq!(decoded.block_restart_interval, crate::sst::RESTART_INTERVAL);
    }

    #[test]
    fn prefix_delta_settings_round_trip() {
        for (enabled, interval) in [(true, 8usize), (false, 32), (true, 1), (true, 1024)] {
            let config = ColumnFamilyConfig {
                enable_prefix_delta_keys: enabled,
                block_restart_interval: interval,
                ..Default::default()
            };
            config.validate().unwrap();
            let decoded = ColumnFamilyConfig::decode(&config.encode());
            assert_eq!(decoded.enable_prefix_delta_keys, enabled);
            assert_eq!(decoded.block_restart_interval, interval);
        }
    }

    /// The blob tails are positional, so the new one must survive behind every
    /// tail that already existed — including the two it directly follows.
    #[test]
    fn the_prefix_delta_tail_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            data_block_size: 16 << 10,
            max_cached_vlog_value_bytes: 1 << 20,
            bloom_fpr_per_level: vec![0.02, 0.05],
            optimize_filters_for_hits: true,
            periodic_compaction_interval: std::time::Duration::from_secs(3600),
            enable_prefix_delta_keys: true,
            block_restart_interval: 16,
            ..Default::default()
        };
        config.validate().unwrap();
        let blob = config.encode();
        let block_at = blob
            .windows(8)
            .position(|w| w == CONFIG_BLOCK_SIZE_MAGIC)
            .expect("block-size tail");
        let delta_at = blob
            .windows(8)
            .position(|w| w == CONFIG_PREFIX_DELTA_MAGIC)
            .expect("prefix-delta tail");
        assert!(block_at < delta_at, "the new tail must come last");
        let decoded = ColumnFamilyConfig::decode(&blob);
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.max_cached_vlog_value_bytes, 1 << 20);
        assert_eq!(decoded.bloom_fpr_per_level, vec![0.02, 0.05]);
        assert!(decoded.optimize_filters_for_hits);
        assert_eq!(
            decoded.periodic_compaction_interval,
            std::time::Duration::from_secs(3600)
        );
        assert!(decoded.enable_prefix_delta_keys);
        assert_eq!(decoded.block_restart_interval, 16);
    }

    /// `0` is the writer-level "no restart trailer at all", which a config
    /// value must not be able to mean.
    #[test]
    fn a_zero_restart_interval_is_rejected() {
        let error = ColumnFamilyConfig {
            block_restart_interval: 0,
            ..Default::default()
        }
        .validate()
        .expect_err("zero must be rejected");
        assert!(error.contains("block_restart_interval"), "{error}");
    }

    #[test]
    fn a_restart_interval_above_1024_is_rejected() {
        let error = ColumnFamilyConfig {
            block_restart_interval: 1025,
            ..Default::default()
        }
        .validate()
        .expect_err("above the bound must be rejected");
        assert!(error.contains("block_restart_interval"), "{error}");
        // The bound itself is accepted.
        ColumnFamilyConfig {
            block_restart_interval: 1024,
            ..Default::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn config_cursor_reads_checked_little_endian_values() {
        let bytes = [
            0x7f, 0x78, 0x56, 0x34, 0x12, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xac,
            0x02, b'o', b'k',
        ];
        let mut cursor = ConfigCursor::new(&bytes);

        assert_eq!(cursor.byte(), Some(0x7f));
        assert_eq!(cursor.u32(), Some(0x1234_5678));
        assert_eq!(cursor.u64(), Some(0x0102_0304_0506_0708));
        assert_eq!(cursor.uvar(), Some(300));
        assert_eq!(cursor.bytes(2), Some(&b"ok"[..]));
        assert!(cursor.is_empty());
    }

    #[test]
    fn config_cursor_does_not_advance_after_a_short_fixed_width_read() {
        let mut cursor = ConfigCursor::new(&[1, 2, 3]);

        assert_eq!(cursor.u64(), None);
        assert_eq!(cursor.bytes(3), Some(&[1, 2, 3][..]));
        assert!(cursor.is_empty());
    }

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

    /// The 0.8.0 geometry survives a manifest round-trip, and a config left at
    /// the defaults still encodes exactly as earlier releases wrote it.
    #[test]
    fn compaction_geometry_roundtrip_and_default_is_byte_identical() {
        let tuned = ColumnFamilyConfig {
            target_file_size: 4 << 20,
            l1_base_bytes: 1 << 30,
            soft_pending_compaction_bytes: 7 << 30,
            hard_pending_compaction_bytes: 9 << 30,
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&tuned.encode());
        assert_eq!(d.target_file_size, 4 << 20);
        assert_eq!(d.l1_base_bytes, 1 << 30);
        assert_eq!(d.soft_pending_compaction_bytes, 7 << 30);
        assert_eq!(d.hard_pending_compaction_bytes, 9 << 30);

        // Defaults carry no tail at all.
        let base = ColumnFamilyConfig::default().encode();
        assert!(
            !base
                .windows(CONFIG_COMPACTION_MAGIC.len())
                .any(|w| w == CONFIG_COMPACTION_MAGIC),
            "a default config must not emit the compaction tail"
        );
    }

    /// A pre-0.8.0 manifest (no compaction tail) decodes to the new defaults
    /// rather than to zeroes, which would divide by zero when sizing levels.
    #[test]
    fn pre_080_manifest_decodes_to_compaction_defaults() {
        let legacy = ColumnFamilyConfig {
            compression: Compression::Zstd,
            ..ColumnFamilyConfig::default()
        }
        .encode();
        let d = ColumnFamilyConfig::decode(&legacy);
        let def = ColumnFamilyConfig::default();
        assert_eq!(d.target_file_size, def.target_file_size);
        assert_eq!(d.l1_base_bytes, def.l1_base_bytes);
        assert_eq!(
            d.hard_pending_compaction_bytes,
            def.hard_pending_compaction_bytes
        );
    }

    /// The geometry tail must survive alongside the tails that precede it.
    #[test]
    fn compaction_tail_coexists_with_partition_fn_tail() {
        let cfg = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            ..ColumnFamilyConfig::default()
        };
        let d = ColumnFamilyConfig::decode(&cfg.encode());
        assert_eq!(d.target_file_size, 2 << 20);
        match d.partition_scheme {
            PartitionScheme::Unresolved(n) => assert_eq!(n, "byhash"),
            other => panic!("partition scheme lost: {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_inverted_compaction_thresholds() {
        let bad = ColumnFamilyConfig {
            soft_pending_compaction_bytes: 9 << 30,
            hard_pending_compaction_bytes: 1 << 30,
            ..ColumnFamilyConfig::default()
        };
        assert!(bad.validate().is_err());
    }
}

#[cfg(test)]
mod block_size_tests {
    use super::*;

    #[test]
    fn a_default_config_emits_no_block_size_tail() {
        let blob = ColumnFamilyConfig {
            compression: Compression::Zstd,
            ..ColumnFamilyConfig::default()
        }
        .encode();
        assert!(!blob
            .windows(CONFIG_BLOCK_SIZE_MAGIC.len())
            .any(|window| window == CONFIG_BLOCK_SIZE_MAGIC));
    }

    #[test]
    fn a_set_block_size_round_trips() {
        let config = ColumnFamilyConfig {
            data_block_size: 64 << 10,
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(
            ColumnFamilyConfig::decode(&config.encode()).data_block_size,
            64 << 10
        );
    }

    #[test]
    fn the_block_size_tail_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            data_block_size: 16 << 10,
            ..ColumnFamilyConfig::default()
        };
        let decoded = ColumnFamilyConfig::decode(&config.encode());
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.target_file_size, 2 << 20);
        assert!(matches!(
            decoded.partition_scheme,
            PartitionScheme::Unresolved(ref name) if name == "byhash"
        ));
    }

    #[test]
    fn vlog_cache_blob_omits_default() {
        // The default (0, disabled) must add no bytes: old readers decode new
        // blobs, and an untouched family's blob does not change shape.
        let blob = ColumnFamilyConfig {
            compression: Compression::Zstd,
            data_block_size: 16 << 10,
            ..ColumnFamilyConfig::default()
        }
        .encode();
        assert!(!blob
            .windows(CONFIG_VLOG_CACHE_MAGIC.len())
            .any(|window| window == CONFIG_VLOG_CACHE_MAGIC));
    }

    #[test]
    fn a_set_vlog_cache_limit_round_trips() {
        let config = ColumnFamilyConfig {
            max_cached_vlog_value_bytes: 1 << 20,
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(
            ColumnFamilyConfig::decode(&config.encode()).max_cached_vlog_value_bytes,
            1 << 20
        );
    }

    #[test]
    fn the_vlog_cache_tail_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            data_block_size: 16 << 10,
            max_cached_vlog_value_bytes: 4 << 20,
            ..ColumnFamilyConfig::default()
        };
        let decoded = ColumnFamilyConfig::decode(&config.encode());
        assert_eq!(decoded.max_cached_vlog_value_bytes, 4 << 20);
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.target_file_size, 2 << 20);
        assert!(matches!(
            decoded.partition_scheme,
            PartitionScheme::Unresolved(ref name) if name == "byhash"
        ));
    }

    #[test]
    fn a_vlog_cache_limit_below_the_separation_threshold_is_rejected() {
        // No value shorter than `klog_value_threshold` ever reaches the vlog,
        // so such a limit reads as "on" while admitting nothing.
        let config = ColumnFamilyConfig {
            klog_value_threshold: 512,
            max_cached_vlog_value_bytes: 511,
            ..ColumnFamilyConfig::default()
        };
        let error = config.validate().expect_err("must not validate");
        assert!(error.contains("max_cached_vlog_value_bytes"), "{error}");

        // Exactly at the threshold is the smallest useful limit.
        assert!(ColumnFamilyConfig {
            klog_value_threshold: 512,
            max_cached_vlog_value_bytes: 512,
            ..ColumnFamilyConfig::default()
        }
        .validate()
        .is_ok());
        // ...and 0 (disabled) is always fine.
        assert!(ColumnFamilyConfig::default().validate().is_ok());
    }

    #[test]
    fn a_zero_block_size_is_rejected() {
        let config = ColumnFamilyConfig {
            data_block_size: 0,
            ..ColumnFamilyConfig::default()
        };
        let error = config.validate().expect_err("zero must not validate");
        assert!(error.contains("data_block_size"), "{error}");
    }
}

#[cfg(test)]
mod bloom_policy_tests {
    use super::*;

    #[test]
    fn bloom_fpr_for_level_repeats_last_element() {
        // Empty vector: uniform `bloom_fpr` at every level. This also pins the
        // underflow guard — `v.len() - 1` on an empty slice would panic.
        let uniform = ColumnFamilyConfig::default();
        assert_eq!(uniform.bloom_fpr_for_level(0, false), Some(0.01));
        assert_eq!(uniform.bloom_fpr_for_level(3, false), Some(0.01));

        let tiered = ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.01],
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(tiered.bloom_fpr_for_level(0, false), Some(0.001));
        assert_eq!(tiered.bloom_fpr_for_level(1, false), Some(0.01));
        assert_eq!(tiered.bloom_fpr_for_level(2, false), Some(0.01));
        assert_eq!(tiered.bloom_fpr_for_level(7, false), Some(0.01));

        // `optimize_filters_for_hits` fires only for bottom output.
        let hits = ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.01],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(hits.bloom_fpr_for_level(1, true), None);
        assert_eq!(hits.bloom_fpr_for_level(1, false), Some(0.01));
        assert_eq!(hits.bloom_fpr_for_level(0, true), None);
        // Off by default: bottom output keeps its filter.
        assert_eq!(tiered.bloom_fpr_for_level(1, true), Some(0.01));
    }

    #[test]
    fn validate_rejects_bloom_fpr_out_of_range() {
        for bad in [0.0, 1.0, f64::NAN, f64::INFINITY, -0.5, 1.5] {
            let cfg = ColumnFamilyConfig {
                bloom_fpr_per_level: vec![0.01, bad],
                ..ColumnFamilyConfig::default()
            };
            let error = cfg
                .validate()
                .expect_err("an out-of-range per-level FPR must not validate");
            assert!(error.contains("bloom_fpr_per_level"), "{error}");
        }
        let good = ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.01, 0.05],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        good.validate().expect("in-range FPRs validate");
    }

    #[test]
    fn bloom_policy_blob_omits_defaults() {
        // A config that differs only elsewhere must encode exactly as it did
        // before this tail existed, so an older binary keeps decoding it.
        let blob = ColumnFamilyConfig {
            compression: Compression::Zstd,
            data_block_size: 16 << 10,
            ..ColumnFamilyConfig::default()
        }
        .encode();
        assert!(!blob
            .windows(CONFIG_BLOOM_POLICY_MAGIC.len())
            .any(|window| window == CONFIG_BLOOM_POLICY_MAGIC));
    }

    #[test]
    fn a_set_bloom_policy_round_trips_and_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            data_block_size: 16 << 10,
            bloom_fpr_per_level: vec![0.001, 0.01, 0.05],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        let decoded = ColumnFamilyConfig::decode(&config.encode());
        assert_eq!(decoded.bloom_fpr_per_level, vec![0.001, 0.01, 0.05]);
        assert!(decoded.optimize_filters_for_hits);
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.target_file_size, 2 << 20);
        assert!(matches!(
            decoded.partition_scheme,
            PartitionScheme::Unresolved(ref name) if name == "byhash"
        ));
    }

    /// A truncated tail leaves both fields at their defaults rather than
    /// applying a half-read policy (the compaction tail's rule).
    #[test]
    fn a_truncated_bloom_policy_tail_is_ignored() {
        let config = ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.01],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        let mut blob = config.encode();
        blob.truncate(blob.len() - 4);
        let decoded = ColumnFamilyConfig::decode(&blob);
        assert!(decoded.bloom_fpr_per_level.is_empty());
        assert!(!decoded.optimize_filters_for_hits);
    }
}

#[cfg(test)]
mod periodic_tests {
    use super::*;

    /// FIFO evicts by age already (`fifo_ttl`); a periodic *rewrite* has no
    /// meaning there, so the combination is refused rather than silently
    /// ignored.
    #[test]
    fn periodic_refuses_fifo() {
        let cfg = ColumnFamilyConfig {
            compaction_style: CompactionStyle::Fifo,
            periodic_compaction_interval: Duration::from_secs(3600),
            ..ColumnFamilyConfig::default()
        };
        let error = cfg
            .validate()
            .expect_err("periodic compaction is invalid on a FIFO family");
        assert!(error.contains("periodic_compaction_interval"), "{error}");

        // Zero is the default and stays legal on FIFO...
        ColumnFamilyConfig {
            compaction_style: CompactionStyle::Fifo,
            ..ColumnFamilyConfig::default()
        }
        .validate()
        .expect("a FIFO family that leaves the option alone validates");
        // ...and a leveled family accepts the interval.
        ColumnFamilyConfig {
            periodic_compaction_interval: Duration::from_secs(3600),
            ..ColumnFamilyConfig::default()
        }
        .validate()
        .expect("periodic compaction is a leveled-family option");
    }

    /// 1.1: the operator name is the durable half of the merge feature, and it
    /// is decoded from the remainder of the prefix-delta tail — so the two must
    /// chain, in both orders of being set.
    #[test]
    fn merge_operator_name_round_trips() {
        let cfg = ColumnFamilyConfig {
            merge_operator_name: Some("example.counter.i64.v1".to_string()),
            ..ColumnFamilyConfig::default()
        };
        let decoded = ColumnFamilyConfig::decode(&cfg.encode());
        assert_eq!(
            decoded.merge_operator_name.as_deref(),
            Some("example.counter.i64.v1")
        );
        // The resolved implementation is not persisted; only the name is.
        assert!(decoded.merge_operator.is_none());

        // Behind every other tail this release writes.
        let chained = ColumnFamilyConfig {
            merge_operator_name: Some("m".to_string()),
            enable_prefix_delta_keys: true,
            block_restart_interval: 16,
            periodic_compaction_interval: Duration::from_secs(60),
            bloom_fpr_per_level: vec![0.001, 0.01],
            data_block_size: 8192,
            ..ColumnFamilyConfig::default()
        };
        let decoded = ColumnFamilyConfig::decode(&chained.encode());
        assert_eq!(decoded.merge_operator_name.as_deref(), Some("m"));
        assert!(decoded.enable_prefix_delta_keys);
        assert_eq!(decoded.block_restart_interval, 16);
        assert_eq!(decoded.data_block_size, 8192);
    }

    /// A blob written before 1.1 has no merge tail, so the family decodes as
    /// having no operator rather than reading garbage off the end — and a
    /// family that sets none must still encode byte-for-byte as 0.8.2 wrote it.
    #[test]
    fn config_blob_without_operator_decodes_none() {
        let default = ColumnFamilyConfig::default();
        let blob = default.encode();
        assert!(
            !blob
                .windows(CONFIG_MERGE_OP_MAGIC.len())
                .any(|w| w == CONFIG_MERGE_OP_MAGIC),
            "a family with no operator must stay byte-identical to a pre-1.1 blob"
        );
        assert!(ColumnFamilyConfig::decode(&blob).merge_operator_name.is_none());

        // A truncated tail is all-or-nothing: no name rather than half a name.
        let cfg = ColumnFamilyConfig {
            merge_operator_name: Some("truncated".to_string()),
            ..ColumnFamilyConfig::default()
        };
        let full = cfg.encode();
        let cut = &full[..full.len() - 3];
        assert!(ColumnFamilyConfig::decode(cut).merge_operator_name.is_none());
    }

    #[test]
    fn periodic_interval_blob_omits_default_and_round_trips() {
        let default = ColumnFamilyConfig::default();
        assert!(
            !default
                .encode()
                .windows(CONFIG_PERIODIC_MAGIC.len())
                .any(|w| w == CONFIG_PERIODIC_MAGIC),
            "the default must stay byte-identical to a pre-0.3 blob"
        );

        let cfg = ColumnFamilyConfig {
            periodic_compaction_interval: Duration::from_secs(7 * 24 * 3600),
            // Set alongside the bloom tail so the two chain correctly: the
            // periodic tail is decoded from the bloom tail's remainder.
            bloom_fpr_per_level: vec![0.001, 0.01],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        let decoded = ColumnFamilyConfig::decode(&cfg.encode());
        assert_eq!(
            decoded.periodic_compaction_interval,
            Duration::from_secs(7 * 24 * 3600)
        );
        assert_eq!(decoded.bloom_fpr_per_level, vec![0.001, 0.01]);
        assert!(decoded.optimize_filters_for_hits);

        // And without the bloom tail ahead of it.
        let alone = ColumnFamilyConfig {
            periodic_compaction_interval: Duration::from_secs(60),
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(
            ColumnFamilyConfig::decode(&alone.encode()).periodic_compaction_interval,
            Duration::from_secs(60)
        );
    }

    /// A blob written before 0.3 has no tail, so the option decodes to its
    /// disabled default rather than to garbage read off the end.
    #[test]
    fn legacy_blob_decodes_periodic_interval_as_disabled() {
        let legacy = ColumnFamilyConfig {
            write_buffer_size: 7 << 20,
            ..ColumnFamilyConfig::default()
        };
        let blob = legacy.encode();
        let decoded = ColumnFamilyConfig::decode(&blob);
        assert_eq!(decoded.write_buffer_size, 7 << 20);
        assert!(decoded.periodic_compaction_interval.is_zero());
    }
}
