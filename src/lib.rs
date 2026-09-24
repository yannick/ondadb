//! # ondaDB
//!
//! A safe, performance-focused Rust key/value LSM storage engine: column
//! families, MVCC transactions with five isolation levels, savepoints, TTL,
//! WiscKey value separation, Bloom filters, leveled compaction, a group-commit
//! WAL, block and file caches, partitioned parts with storage tiers, and
//! optional S3-backed bottom levels — staying in safe Rust wherever that does
//! not cost measurable performance. (`#![deny(unsafe_code)]` by default; the
//! `unsafe-fastpath` feature lifts it for exactly two localized paths.)
//!
//! Beyond the ordinary key/value surface:
//!
//! * [`DB::merge`](crate::DB::merge) appends a **merge operand** resolved at
//!   read time by the family's [`MergeOperator`] — a read-modify-write with no
//!   read and no conflict window.
//! * [`DB::delete_range`](crate::DB::delete_range) records the deletion of a
//!   whole comparator interval as **one** record at one sequence.
//! * [`DB::multi_get`](crate::DB::multi_get) resolves many keys in one
//!   snapshot-consistent pass, fetching each distinct block once.
//! * [`Txn::prepare`](crate::Txn::prepare) durably prepares a transaction for
//!   two-phase commit, resolvable by external id across a restart.
//! * [`DB::new_tailing_iterator`](crate::DB::new_tailing_iterator) follows an
//!   append-only keyspace without rebuilding a cursor per poll.
//! * [`PerfContext`] attributes one operation's cost to a mechanism.
//!
//! **Formats are capability-gated.** Every stored byte that is not 0.8.2's sits
//! behind a `CAP_*` bit in [`mod@format`], enabled explicitly and one-way by
//! [`DB::enable_format_capabilities`](crate::DB::enable_format_capabilities).
//! Nothing is on by default, so an upgraded database keeps writing bytes an
//! older binary can read until an operator decides otherwise.

// The default build is safe Rust.  The optional `mmap-reads` and
// `arena-memtable` features each lift this to allow the localized `unsafe` in,
// respectively, the mmap zero-copy reader and the arena-backed memtable;
// everything else stays safe.  `unsafe-fastpath` enables both.
//
// `deny`, not `forbid`, because `forbid` cannot be lifted anywhere in the
// crate — not even by an audited `#[allow(unsafe_code)]` on a single function.
// That made the default build **fail to compile on Linux**, where
// `util::coarse_now_nanos` calls `clock_gettime(CLOCK_REALTIME_COARSE)`
// (E0453: `allow(unsafe_code)` incompatible with previous forbid). `deny`
// still hard-errors on every `unsafe` that is not individually annotated, so
// new unsafe cannot appear by accident; the exceptions are greppable via
// `#[allow(unsafe_code)]`.
#![cfg_attr(
    not(any(feature = "mmap-reads", feature = "arena-memtable")),
    deny(unsafe_code)
)]
#![warn(missing_debug_implementations)]

// `tests/support/levels.rs` (the 0.2 overlapping-level fixture generator) is
// shared verbatim between the integration tests and the in-crate compaction
// benchmark, so it names types through the crate's PUBLIC path. This alias is
// what lets that one file compile in both positions instead of being copied.
// Test builds only; it adds nothing to a released binary.
#[cfg(test)]
extern crate self as ondadb;

pub mod block;
pub mod bloom;
pub mod cache;
pub mod checkpoint;
pub mod column_family;
pub mod compaction;
pub mod comparator;
pub mod compress;
pub mod config;
pub mod db;
pub mod encoding;
pub mod error;
pub(crate) mod excise;
pub mod format;
pub mod ingest;
pub mod ioctrl;
pub mod iterator;
pub mod maintenance;
pub mod manifest;
pub mod manifest_edit;
pub mod memtable;
#[cfg(feature = "arena-memtable")]
pub mod memtable_arena;
pub mod parts;
pub mod perf;
pub mod prepared;
pub(crate) mod range_lock;
pub mod range_tombstone;
pub mod read_resources;
pub(crate) mod span_index;
pub mod snapshot;
pub mod sst;
pub mod storage;
#[cfg(feature = "s3")]
pub mod storage_s3;
pub mod table_cache;
pub mod tailing;
pub mod txn;
pub(crate) mod txn_lock;
pub mod unified;
pub mod util;
pub mod wal;

pub use checkpoint::{
    open_remote_checkpoint, restore_from_object_store, CheckpointTable, ObjectCheckpoint,
    ObjectCheckpointOptions, ObjectReceipt, TableSetDiff,
};
pub use column_family::{ColumnFamily, CommitHookFn, CommitOp, CompactionFilterFn, FilterDecision};
pub use comparator::{Comparator, ComparatorRef};
#[cfg(feature = "s3")]
pub use config::{S3Config, S3CredentialSource};
pub use config::{
    ColumnFamilyConfig, CompactionStyle, Compression, CompressionRule, IsolationLevel, LogLevel,
    MergeOperator, Options, PartitionFn, PartitionRule, PartitionScheme, SyncMode, TierBackend,
    TierDef, TierRule,
};
pub use db::{DB, DELETE_METADATA_BYTES};
pub use error::{OndaError, Result};
pub use ingest::Ingestion;
pub use iterator::Iterator;
pub use maintenance::{CfStats, DbStats};
pub use parts::{
    DetachedPart, MovePhase, MovePhaseEvent, MovePhaseObserver, PartManifest, PartTable,
    PartitionInfo,
};
pub use perf::PerfContext;
pub use prepared::PreparedInfo;
pub use read_resources::{ReadResourceOptions, ReadResourceStats, ReadResources};
pub use snapshot::SnapshotHandle;
pub use storage::{CreateOutcome, LocalStorage, ObjectInfo, PrefixPage, Storage};
#[cfg(feature = "s3")]
pub use storage_s3::S3Storage;
pub use tailing::TailingIterator;
pub use txn::{PreparedTxn, Txn};
