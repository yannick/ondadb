//! Maintenance operations: checkpoint, backup, column-family clone, and stats.
//!

use std::path::Path;
use std::sync::Arc;

use crate::column_family::{ColumnFamily, SstHandle};
use crate::db::DB;
use crate::error::{OndaError, Result};
use crate::manifest::{manifest_path, Manifest, SstMeta};

/// Per-column-family statistics.
#[derive(Debug, Clone, Default)]
pub struct CfStats {
    pub name: String,
    pub num_levels: usize,
    /// `(file_count, bytes)` per level.
    pub levels: Vec<(usize, u64)>,
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub flush_count: u64,
    pub compaction_count: u64,
}

/// Database-wide statistics.
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub num_column_families: usize,
    pub total_sstables: usize,
    pub total_bytes: u64,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
}

impl ColumnFamily {
    /// Snapshot statistics for this column family.
    pub fn stats(&self) -> CfStats {
        let levels = self.level_summary();
        let (entries, tombs) = self.entry_counts();
        CfStats {
            name: self.name().to_string(),
            num_levels: levels.len(),
            num_entries: entries,
            num_tombstones: tombs,
            flush_count: self.flush_count.load(std::sync::atomic::Ordering::Relaxed),
            compaction_count: self
                .compaction_count
                .load(std::sync::atomic::Ordering::Relaxed),
            levels,
        }
    }
}

impl DB {
    /// Database-wide statistics.
    pub fn stats(&self) -> DbStats {
        let cfs: Vec<Arc<ColumnFamily>> = self.inner.cfs.read().values().cloned().collect();
        let mut total_sstables = 0;
        let mut total_bytes = 0;
        for cf in &cfs {
            for (count, bytes) in cf.level_summary() {
                total_sstables += count;
                total_bytes += bytes;
            }
        }
        let bc = self.inner.ctx.bc.stats();
        DbStats {
            num_column_families: cfs.len(),
            total_sstables,
            total_bytes,
            block_cache_hits: bc.hits,
            block_cache_misses: bc.misses,
        }
    }

    /// Flush all column families and create a checkpoint: a directory of
    /// hard-linked SSTables plus a copy of the manifest.
    pub fn checkpoint(&self, dir: impl AsRef<Path>) -> Result<()> {
        self.snapshot_to(dir.as_ref(), true)
    }

    /// Like [`checkpoint`](Self::checkpoint) but copies file bytes instead of
    /// hard-linking, producing a standalone backup.
    pub fn backup(&self, dir: impl AsRef<Path>) -> Result<()> {
        self.snapshot_to(dir.as_ref(), false)
    }

    fn snapshot_to(&self, dir: &Path, hard_link: bool) -> Result<()> {
        // Pause obsolete-file deletion so a concurrent compaction cannot unlink an
        // SSTable that the snapshot's manifest still references. Held until return.
        let _pause = self.inner.pause_deletions();

        let cfs: Vec<Arc<ColumnFamily>> = self.inner.cfs.read().values().cloned().collect();
        // Flush memtables so all data lives in SSTables, then persist manifest.
        for cf in &cfs {
            self.flush_memtable(cf)?;
        }
        self.inner.persist_manifest()?;

        std::fs::create_dir_all(dir)?;
        // Load the manifest and link exactly the files it references. With deletions
        // paused, every file any persisted manifest lists still exists on disk, so
        // the copied catalog and the copied files are guaranteed consistent — even if
        // a compaction rewrote the live manifest after our persist above.
        let src_manifest = manifest_path(&self.inner.dir);
        let manifest = Manifest::load(&src_manifest)?;
        for cfm in &manifest.cfs {
            let cf_dir = dir.join(format!("cf-{}", cfm.name));
            std::fs::create_dir_all(&cf_dir)?;
            for sst in &cfm.sstables {
                for ext in ["klog", "vlog"] {
                    let src = format!("{}/{}.{ext}", self.inner.cf_dir(&cfm.name), sst.id);
                    if !Path::new(&src).exists() {
                        continue; // vlog absent when the SSTable has no large values
                    }
                    let dst = cf_dir.join(format!("{}.{ext}", sst.id));
                    let _ = std::fs::remove_file(&dst);
                    if hard_link {
                        std::fs::hard_link(&src, &dst)?;
                    } else {
                        std::fs::copy(&src, &dst)?;
                    }
                }
            }
        }
        // Persist the same manifest we linked against, so the backup catalog matches
        // its files exactly.
        manifest.save(dir.join("MANIFEST"))?;
        Ok(())
    }

    /// Clone a column family: create `dst` sharing `src`'s current SSTables via
    /// hard links.  Future writes to either are independent.
    pub fn clone_column_family(&self, src: &str, dst: &str) -> Result<Arc<ColumnFamily>> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        let src_cf = self.get_column_family(src).ok_or(OndaError::NotFound)?;
        self.flush_memtable(&src_cf)?;

        // Keep src's SSTables from being compacted away while we hard-link them.
        let _pause = self.inner.pause_deletions();

        let dst_cf = self.create_column_family(dst, src_cf.config().clone())?;

        // Hard-link each src SSTable into dst under a fresh id and register it.
        let src_metas: Vec<SstMeta> = src_cf.snapshot_ssts();
        let mut by_level: Vec<Vec<Arc<SstHandle>>> = Vec::new();
        for meta in src_metas {
            let new_id = self.inner.next_file_id();
            for ext in ["klog", "vlog"] {
                let s = format!("{}/{}.{ext}", src_cf.dir(), meta.id);
                if Path::new(&s).exists() {
                    let d = format!("{}/{new_id}.{ext}", dst_cf.dir());
                    std::fs::hard_link(&s, &d)?;
                }
            }
            let level = meta.level as usize;
            let mut new_meta = meta;
            new_meta.id = new_id;
            while by_level.len() <= level {
                by_level.push(Vec::new());
            }
            by_level[level].push(dst_cf.open_sst(new_meta)?);
        }
        if by_level.is_empty() {
            by_level.push(Vec::new());
        }
        dst_cf.install_levels(by_level);
        self.inner.persist_manifest()?;
        Ok(dst_cf)
    }
}
