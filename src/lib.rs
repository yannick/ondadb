//! # ondaDB
//!
//! A safe, performance-focused Rust key/value
//! LSM storage engine.  ondaDB targets feature parity with the C engine — column
//! families, MVCC transactions with five isolation levels, savepoints, TTL,
//! WiscKey value separation, bloom filters, leveled ("Spooky") compaction, a
//! group-commit WAL, block/file caches, and object-store replication — while
//! staying in safe Rust wherever it does not cost measurable performance.
//!
//! The crate is built bottom-up; modules are added phase by phase. See the
//! implementation plan for the full roadmap.

// The default build is 100% safe Rust.  The optional `mmap-reads` and
// `arena-memtable` features each lift this to allow the localized `unsafe` in,
// respectively, the mmap zero-copy reader and the arena-backed memtable;
// everything else stays safe.  `unsafe-fastpath` enables both.
#![cfg_attr(
    not(any(feature = "mmap-reads", feature = "arena-memtable")),
    forbid(unsafe_code)
)]
#![warn(missing_debug_implementations)]

pub mod block;
pub mod bloom;
pub mod cache;
pub mod column_family;
pub mod compaction;
pub mod comparator;
pub mod compress;
pub mod config;
pub mod db;
pub mod encoding;
pub mod error;
pub mod format;
pub mod ingest;
pub mod iterator;
pub mod maintenance;
pub mod manifest;
pub mod memtable;
pub mod parts;
#[cfg(feature = "arena-memtable")]
pub mod memtable_arena;
pub mod sst;
pub mod storage;
pub mod txn;
pub mod unified;
pub mod util;
pub mod wal;

pub use column_family::{ColumnFamily, CommitHookFn, CommitOp, CompactionFilterFn, FilterDecision};
pub use comparator::{Comparator, ComparatorRef};
pub use config::{
    ColumnFamilyConfig, CompactionStyle, Compression, CompressionRule, IsolationLevel, LogLevel,
    Options, PartitionRule, SyncMode, TierDef, TierRule,
};
pub use db::DB;
pub use error::{OndaError, Result};
pub use ingest::Ingestion;
pub use iterator::Iterator;
pub use maintenance::{CfStats, DbStats};
pub use parts::DetachedPart;
pub use storage::{LocalStorage, Storage};
pub use txn::Txn;
