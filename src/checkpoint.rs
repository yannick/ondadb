//! Changed-table diffs behind incremental backups (F6), and — built on them —
//! object-store checkpoints.
//!
//! SSTables are immutable and their ids are never reused (one database-wide
//! counter), so a table set is fully described by its `(cf, id)` pairs, and the
//! difference between two sets is exactly what an incremental backup has to
//! ship (`added`) and may forget (`removed`).

use std::collections::HashSet;

use crate::db::DB;

/// One SSTable in a database's (or a checkpoint's) table set. Everything a
/// caller's backup catalog needs to name and size the table's objects.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CheckpointTable {
    /// Column family the table belongs to.
    pub cf: String,
    /// Table id (unique database-wide, never reused).
    pub id: u64,
    /// Level the table sits on.
    pub level: u32,
    /// Largest sequence number the table holds.
    pub max_seq: u64,
    /// Bytes in the table's `.klog`.
    pub klog_size: u64,
    /// Bytes in the table's `.vlog` (0 = no value log).
    pub vlog_size: u64,
}

impl CheckpointTable {
    fn from_meta(cf: &str, meta: &crate::manifest::SstMeta) -> CheckpointTable {
        CheckpointTable {
            cf: cf.to_string(),
            id: meta.id,
            level: meta.level,
            max_seq: meta.max_seq,
            klog_size: meta.klog_size,
            vlog_size: meta.vlog_size,
        }
    }
}

/// The difference between a prior table set and the live one
/// ([`DB::sstables_diff`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableSetDiff {
    /// Live tables the prior set did not have — what an incremental must ship.
    pub added: Vec<CheckpointTable>,
    /// Prior tables no longer live (compacted away, excised, detached, or their
    /// family dropped) — what a restore of the new set no longer needs.
    pub removed: Vec<CheckpointTable>,
}

impl DB {
    /// Every live SSTable of every column family, ordered by `(cf, id)`.
    ///
    /// Each family's list is one consistent snapshot of its levels; families
    /// are read one after another, so a compaction finishing in between can be
    /// seen in one family and not another. For a set that must match a restore
    /// point exactly, use the table list an object-store checkpoint returns.
    pub fn live_sstables(&self) -> Vec<CheckpointTable> {
        let cfs: Vec<_> = self.inner.cfs.read().values().cloned().collect();
        let mut out: Vec<CheckpointTable> = cfs
            .iter()
            .flat_map(|cf| {
                cf.snapshot_ssts()
                    .into_iter()
                    .map(|meta| CheckpointTable::from_meta(cf.name(), &meta))
                    .collect::<Vec<_>>()
            })
            .collect();
        out.sort_by(|a, b| (&a.cf, a.id).cmp(&(&b.cf, b.id)));
        out
    }

    /// Live SSTables whose `max_seq` is greater than `seq` (wavesdb
    /// `SSTablesSince`): pass a prior backup's sequence to find the tables
    /// holding writes made since.
    ///
    /// This answers "which tables hold new *data*", not "which tables are new":
    /// a compaction that rewrites only old data produces a new table whose
    /// `max_seq` is still `<= seq`, and this call does not report it. A backup
    /// that must restore the *current* table set — whose old inputs that
    /// compaction just retired — needs [`sstables_diff`](Self::sstables_diff).
    pub fn sstables_since(&self, seq: u64) -> Vec<CheckpointTable> {
        self.live_sstables()
            .into_iter()
            .filter(|t| t.max_seq > seq)
            .collect()
    }

    /// The live table set relative to `prior` (a table list an earlier
    /// checkpoint or [`live_sstables`](Self::live_sstables) returned): tables
    /// added since, and tables of `prior` that are gone. Tables are matched by
    /// `(cf, id)`; ids are never reused, so an id present in both is the same
    /// immutable bytes.
    pub fn sstables_diff(&self, prior: &[CheckpointTable]) -> TableSetDiff {
        let live = self.live_sstables();
        let prior_ids: HashSet<(&str, u64)> = prior.iter().map(|t| (t.cf.as_str(), t.id)).collect();
        let live_ids: HashSet<(&str, u64)> = live.iter().map(|t| (t.cf.as_str(), t.id)).collect();
        let removed = prior
            .iter()
            .filter(|t| !live_ids.contains(&(t.cf.as_str(), t.id)))
            .cloned()
            .collect();
        let added = live
            .iter()
            .filter(|t| !prior_ids.contains(&(t.cf.as_str(), t.id)))
            .cloned()
            .collect();
        TableSetDiff { added, removed }
    }
}
