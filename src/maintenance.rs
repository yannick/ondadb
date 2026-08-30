//! Maintenance operations: checkpoint, backup, column-family clone, and stats.
//!

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::column_family::{ColumnFamily, SstHandle};
use crate::db::DB;
use crate::error::{OndaError, Result};
use crate::manifest::{manifest_path, Manifest, SstMeta};
use crate::storage::Storage;
use crate::util::sync_parent_dir;

fn copy_storage_file(storage: &dyn Storage, src: &str, dst: &Path) -> Result<()> {
    let reader = storage.open_read(src)?;
    let size = reader.size()?;
    let mut file = std::fs::File::create(dst)?;
    let mut offset = 0u64;
    let mut buffer = vec![0u8; 256 << 10];
    while offset < size {
        let len = usize::try_from((size - offset).min(buffer.len() as u64))
            .expect("bounded copy chunk fits usize");
        reader.read_exact_at(&mut buffer[..len], offset)?;
        file.write_all(&buffer[..len])?;
        offset += len as u64;
    }
    file.sync_all()?;
    sync_parent_dir(dst)
}

fn place_storage_file(
    storage: &dyn Storage,
    src: &str,
    dst: &Path,
    prefer_hard_link: bool,
) -> Result<()> {
    match std::fs::remove_file(dst) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    if prefer_hard_link && Path::new(src).exists() && std::fs::hard_link(src, dst).is_ok() {
        return sync_parent_dir(dst);
    }
    copy_storage_file(storage, src, dst)
}

/// Per-column-family statistics.
#[derive(Debug, Clone, Default)]
pub struct CfStats {
    pub name: String,
    pub num_levels: usize,
    /// `(file_count, bytes)` per level.
    pub levels: Vec<(usize, u64)>,
    pub num_entries: u64,
    pub num_tombstones: u64,
    /// Entries in the active + sealed memtables.
    pub memtable_entries: u64,
    /// `num_entries + memtable_entries` (see `ColumnFamily::approximate_len`).
    pub approximate_len: u64,
    pub flush_count: u64,
    pub compaction_count: u64,
    /// Number of manual or background compaction attempts that returned an
    /// error since this column family was opened.
    pub compaction_failures: u64,
    /// Most recently observed compaction error, if any.
    pub last_compaction_error: Option<String>,
    /// Point lookups served by this CF.
    pub point_reads: u64,
    /// SSTable probes skipped by a bloom-filter negative.
    pub bloom_skips: u64,
    /// SSTable probes actually issued.
    pub sst_probes: u64,
    /// Bytes by which the levels exceed their capacities — the backlog
    /// compaction still owes. Writers pace against this once it passes
    /// `soft_pending_compaction_bytes` and block at
    /// `hard_pending_compaction_bytes`, so a value pinned near the hard limit
    /// means ingest is outrunning compaction.
    pub compaction_debt: u64,
}

/// Database-wide statistics.
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub num_column_families: usize,
    pub total_sstables: usize,
    pub total_bytes: u64,
    /// Klog data-block cache hits/misses. Vlog values share the same cache but
    /// are counted separately, so these keep meaning what they always did.
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    /// Decoded vlog values served from the block cache
    /// (`max_cached_vlog_value_bytes`; always 0 when no family enables it).
    pub vlog_cache_hits: u64,
    pub vlog_cache_misses: u64,
    /// Bytes of the block cache currently held by decoded vlog values — the
    /// capacity vlog admission is taking from klog data blocks.
    pub vlog_cache_bytes: i64,
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
            memtable_entries: self.memtable_entries(),
            approximate_len: entries + self.memtable_entries(),
            flush_count: self.flush_count.load(std::sync::atomic::Ordering::Relaxed),
            compaction_count: self
                .compaction_count
                .load(std::sync::atomic::Ordering::Relaxed),
            compaction_failures: self
                .compaction_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            last_compaction_error: self.last_compaction_error.lock().clone(),
            point_reads: self.point_reads.load(std::sync::atomic::Ordering::Relaxed),
            bloom_skips: self.bloom_skips.load(std::sync::atomic::Ordering::Relaxed),
            sst_probes: self.sst_probes.load(std::sync::atomic::Ordering::Relaxed),
            compaction_debt: self
                .compaction_debt
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
            vlog_cache_hits: bc.vlog_hits,
            vlog_cache_misses: bc.vlog_misses,
            vlog_cache_bytes: bc.vlog_bytes,
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
        let mut manifest = Manifest::load(&src_manifest)?;
        for cfm in &mut manifest.cfs {
            let source_cf = cfs
                .iter()
                .find(|cf| cf.name() == cfm.name)
                .ok_or(OndaError::NotFound)?;
            let cf_dir = dir.join(format!("cf-{}", cfm.name));
            std::fs::create_dir_all(&cf_dir)?;
            for sst in &mut cfm.sstables {
                let storage = source_cf.tiers().storage_for(sst.tier.as_deref());
                let src_klog = source_cf.klog_path_for(sst);
                for (ext, src, size) in [
                    ("klog", src_klog.clone(), sst.klog_size),
                    ("vlog", crate::sst::vlog_path_for(&src_klog), sst.vlog_size),
                ] {
                    if ext == "vlog" && size == 0 {
                        continue;
                    }
                    let dst = cf_dir.join(format!("{}.{ext}", sst.id));
                    place_storage_file(storage.as_ref(), &src, &dst, hard_link)?;
                }
                sst.tier = None;
                sst.object = None;
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

        let dst_cf = self.create_column_family(dst, src_cf.effective_config())?;

        // Hard-link each src SSTable into dst under a fresh id and register it.
        let src_metas: Vec<SstMeta> = src_cf.snapshot_ssts();
        let mut by_level: Vec<Vec<Arc<SstHandle>>> = Vec::new();
        for meta in src_metas {
            let new_id = self.inner.next_file_id();
            let storage = src_cf.tiers().storage_for(meta.tier.as_deref());
            let src_klog = src_cf.klog_path_for(&meta);
            for (ext, source, size) in [
                ("klog", src_klog.clone(), meta.klog_size),
                ("vlog", crate::sst::vlog_path_for(&src_klog), meta.vlog_size),
            ] {
                if ext == "vlog" && size == 0 {
                    continue;
                }
                let destination =
                    std::path::PathBuf::from(format!("{}/{new_id}.{ext}", dst_cf.dir()));
                place_storage_file(storage.as_ref(), &source, &destination, true)?;
            }
            let level = meta.level as usize;
            let mut new_meta = meta;
            new_meta.id = new_id;
            new_meta.tier = None;
            new_meta.object = None;
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
