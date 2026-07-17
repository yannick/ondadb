//! Part lifecycle: DETACH / ATTACH / FREEZE and cross-tier moves for the
//! bottom-level parts introduced by [`partition_rules`](crate::ColumnFamilyConfig::partition_rules).
//!
//! A **part** is one partition's set of bottom-level SSTable file pairs (see the
//! parts & tiers design). These operations, like ClickHouse's, treat a part as
//! the unit of backup, retention, and tiering:
//!
//! - [`DB::detach_part`] removes a part from the catalog and moves its files
//!   aside; [`DB::attach_part`] brings one back in.
//! - [`DB::freeze_part`] hard-links a part into a standalone, independently
//!   openable directory.
//! - [`DB::move_part_to_tier`] relocates a part's files to another storage tier
//!   and flips its `tier` in the manifest (the substrate the later part-mover
//!   milestone drives from policy).
//!
//! **Not snapshot-consistent.** Like a ClickHouse `DETACH`, [`detach_part`] makes
//! the part's data vanish for *new* reads regardless of their snapshot sequence.
//! Iterators opened before the detach keep working: an iterator pins the part's
//! `Arc<SstHandle>` and its currently-loaded data block. Under `mmap-reads` the
//! reader's mmap keeps the moved file's pages valid indefinitely; on the default
//! buffered path, already-loaded blocks stay served from the pinned block, so a
//! scan in flight is unaffected — this is the same property compaction already
//! relies on when it unlinks input files out from under open iterators.
//!
//! All manifest-touching steps go through [`DbInner::persist_manifest`], which
//! rewrites the whole manifest atomically (temp file + fsync + rename), so the
//! removal/insertion of a part's table ids is a single crash-atomic record. The
//! manifest is the source of truth: a crash mid-operation can only leave orphan
//! files, never route a reader to a file that is not durably in place.

use std::path::Path;
use std::sync::Arc;

use crate::column_family::{ColumnFamily, SstHandle};
use crate::db::DB;
use crate::error::{OndaError, Result};
use crate::manifest::{manifest_path, CfManifest, Manifest, SstMeta};
use crate::sst::vlog_path_for;

/// The result of a [`DB::detach_part`]: where the part's files now live and
/// which table ids were removed from the catalog. Pass [`DetachedPart::dir`] to
/// [`DB::attach_part`] to bring the part back.
#[derive(Debug, Clone)]
pub struct DetachedPart {
    /// The partition that was detached.
    pub partition: String,
    /// Directory now holding the detached file pairs
    /// (`<cf-dir>/detached/<partition>`).
    pub dir: String,
    /// Table ids removed from the catalog.
    pub table_ids: Vec<u64>,
    /// Absolute paths of every file moved into [`dir`](Self::dir).
    pub files: Vec<String>,
}

impl DB {
    /// Detach the bottom-level part for `partition`: remove its tables from the
    /// catalog in one atomic manifest record and move their file pairs to
    /// `<cf-dir>/detached/<partition>`. New reads no longer see the range;
    /// iterators opened beforehand are unaffected (see the [module
    /// docs](crate::parts) — this is **not** snapshot-consistent).
    ///
    /// Errors with [`NotFound`](OndaError::NotFound) if the partition has no
    /// materialized bottom-level tables (its data may still be in upper levels
    /// or the memtable; flush + compact first to fully materialize a part).
    pub fn detach_part(&self, cf: &Arc<ColumnFamily>, partition: &str) -> Result<DetachedPart> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.inner.poison.check()?;
        // Serialize against compaction so the bottom level cannot be rewritten
        // out from under us between snapshot and removal.
        let _mu = cf.compact_mu.lock();

        let handles = cf.bottom_partition_handles(partition);
        if handles.is_empty() {
            return Err(OndaError::NotFound);
        }
        let ids: Vec<u64> = handles.iter().map(|h| h.meta.id).collect();

        // 1. Drop the tables from the in-memory level set (new reads stop seeing
        //    them immediately), then 2. persist the manifest — the atomic commit
        //    point. A crash after this leaves the files in place but out of the
        //    catalog: harmless orphans, and a clean reopen.
        cf.remove_bottom_tables(&ids);
        self.inner.persist_manifest()?;

        // 3. Move the file pairs aside. Existing readers hold their own open
        //    descriptors/mmaps, so the moves don't disturb them.
        let dest_dir = format!("{}/detached/{}", cf.dir(), partition);
        std::fs::create_dir_all(&dest_dir)?;
        let mut files = Vec::new();
        for h in &handles {
            h.reader.close(); // drop cached fds for the old paths
            let src_klog = cf.klog_path_for(&h.meta);
            let src_vlog = vlog_path_for(&src_klog);
            let dst_klog = format!("{dest_dir}/{}.klog", h.meta.id);
            let dst_vlog = format!("{dest_dir}/{}.vlog", h.meta.id);
            move_file(&src_klog, &dst_klog)?;
            files.push(dst_klog);
            if Path::new(&src_vlog).exists() {
                move_file(&src_vlog, &dst_vlog)?;
                files.push(dst_vlog);
            }
        }
        Ok(DetachedPart {
            partition: partition.to_string(),
            dir: dest_dir,
            table_ids: ids,
            files,
        })
    }

    /// Attach the part whose file pairs live in `dir` (e.g. a
    /// [`DetachedPart::dir`] or a [`freeze_part`](Self::freeze_part) slice).
    ///
    /// Every file is validated (footer magic + block CRCs, via the reader open)
    /// and must be **same-lineage**: its `max_seq` at or below the current
    /// visible sequence. Files whose sequences exceed the watermark — a foreign
    /// database's tables — are rejected (cross-database restore with sequence
    /// remapping is a later milestone). A part slots into the bottom level when
    /// its key range does not overlap a live bottom table, else into L0; either
    /// way it is copied in under fresh file ids (so the block cache, keyed by id,
    /// can never collide with an evicted file's blocks).
    ///
    /// Validation and copy happen for the whole directory before anything is
    /// installed: if any file is rejected, no partial state is published and the
    /// copies made so far are cleaned up.
    pub fn attach_part(&self, cf: &Arc<ColumnFamily>, dir: impl AsRef<Path>) -> Result<()> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.inner.poison.check()?;
        let _mu = cf.compact_mu.lock();

        let dir = dir.as_ref();
        let mut klogs: Vec<std::path::PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "klog"))
            .collect();
        klogs.sort();
        if klogs.is_empty() {
            return Err(OndaError::InvalidArgs("no .klog files to attach".into()));
        }

        let visible = self.inner.visible_seq();
        // (handle, at_bottom). Built and validated before anything is installed.
        let mut staged: Vec<(Arc<SstHandle>, bool)> = Vec::new();
        // dest paths copied so far, for cleanup on any rejection.
        let mut copied: Vec<String> = Vec::new();

        let result = (|| -> Result<()> {
            for src_klog in &klogs {
                let src_klog = src_klog.to_string_lossy().into_owned();
                let src_vlog = vlog_path_for(&src_klog);

                let new_id = self.inner.next_file_id();
                let dst_klog = cf.klog_path(new_id);
                let dst_vlog = vlog_path_for(&dst_klog);
                std::fs::copy(&src_klog, &dst_klog)?;
                copied.push(dst_klog.clone());
                let has_vlog = Path::new(&src_vlog).exists();
                if has_vlog {
                    std::fs::copy(&src_vlog, &dst_vlog)?;
                    copied.push(dst_vlog.clone());
                }

                // Open on the default tier: this validates footer magic + the
                // index/bloom CRCs, and gives us the key range and max_seq.
                let mut meta = SstMeta {
                    id: new_id,
                    ..Default::default()
                };
                let reader = cf.open_reader_for(&meta)?;
                if reader.max_seq() > visible {
                    return Err(OndaError::InvalidArgs(format!(
                        "attach rejected: table max_seq {} exceeds visible sequence {} \
                         (foreign lineage; cross-database attach is not yet supported)",
                        reader.max_seq(),
                        visible
                    )));
                }

                let min_key = reader.min_key().to_vec();
                let max_key = reader.max_key().to_vec();
                let at_bottom = !cf.bottom_overlaps(&min_key, &max_key);
                meta.level = if at_bottom {
                    cf.bottom_level_index() as u32
                } else {
                    0
                };
                meta.num_entries = reader.num_entries();
                meta.max_seq = reader.max_seq();
                meta.klog_size = file_len(&dst_klog);
                meta.vlog_size = if has_vlog { file_len(&dst_vlog) } else { 0 };
                meta.min_key = min_key;
                meta.max_key = max_key;
                // A bottom part is partition-clean: recover its partition tag
                // from the rules. L0 is never partition-clean, so leave None.
                meta.partition = if at_bottom {
                    crate::config::partition_of(&cf.partition_rules_snapshot(), &meta.min_key)
                        .map(str::to_string)
                } else {
                    None
                };
                staged.push((Arc::new(SstHandle { meta, reader }), at_bottom));
            }
            Ok(())
        })();

        if let Err(e) = result {
            // Roll back: nothing was installed, so unlink the copies we made.
            for p in &copied {
                let _ = std::fs::remove_file(p);
            }
            return Err(e);
        }

        for (handle, at_bottom) in staged {
            if at_bottom {
                cf.insert_bottom_sorted(handle);
            } else {
                cf.install_handles_l0(vec![handle]);
            }
        }
        self.inner.persist_manifest()?;
        Ok(())
    }

    /// Freeze the bottom-level part for `partition` into `dir`: hard-link its
    /// files and write a one-part manifest slice, producing a standalone,
    /// independently-openable database directory (mirrors
    /// [`checkpoint`](Self::checkpoint)'s deletion-pause discipline so a
    /// concurrent compaction cannot unlink a file mid-freeze).
    pub fn freeze_part(
        &self,
        cf: &Arc<ColumnFamily>,
        partition: &str,
        dir: impl AsRef<Path>,
    ) -> Result<()> {
        self.inner.poison.check()?;
        // Keep the part's files from being unlinked by a concurrent compaction
        // while we hard-link them (deferred deletion, same as checkpoint()).
        let _pause = self.inner.pause_deletions();

        let handles = cf.bottom_partition_handles(partition);
        if handles.is_empty() {
            return Err(OndaError::NotFound);
        }
        let dir = dir.as_ref();
        let cf_dir = dir.join(format!("cf-{}", cf.name()));
        std::fs::create_dir_all(&cf_dir)?;

        let mut metas: Vec<SstMeta> = Vec::new();
        let mut max_id = 0u64;
        for h in &handles {
            let src_klog = cf.klog_path_for(&h.meta);
            let src_vlog = vlog_path_for(&src_klog);
            let dst_klog = cf_dir.join(format!("{}.klog", h.meta.id));
            let dst_vlog = cf_dir.join(format!("{}.vlog", h.meta.id));
            let _ = std::fs::remove_file(&dst_klog);
            std::fs::hard_link(&src_klog, &dst_klog)?;
            if Path::new(&src_vlog).exists() {
                let _ = std::fs::remove_file(&dst_vlog);
                std::fs::hard_link(&src_vlog, &dst_vlog)?;
            }
            // The frozen copy is a self-contained default-tier snapshot, so drop
            // any tier annotation (the files live at the default location here).
            let mut meta = h.meta.clone();
            meta.tier = None;
            max_id = max_id.max(meta.id);
            metas.push(meta);
        }

        let manifest = Manifest {
            next_file_id: max_id + 1,
            global_seq: self.inner.visible_seq(),
            cfs: vec![CfManifest {
                name: cf.name().to_string(),
                config: cf.effective_config().encode(),
                sstables: metas,
            }],
        };
        manifest.save(manifest_path(dir))?;
        Ok(())
    }

    /// Move the bottom-level part for `partition` to storage tier `tier`
    /// (which must be configured in [`Options::tiers`](crate::Options::tiers)).
    ///
    /// Copy → fsync → flip the manifest `tier` in one record → delete the
    /// source (the plan's mover protocol). Reads are uninterrupted: the flip
    /// swaps the handles under the state write-lock, and in-flight reads finish
    /// on the old handles. This is the storage substrate for the policy-driven
    /// part mover of a later milestone; here it is the manual lever that proves
    /// cross-tier reads work.
    pub fn move_part_to_tier(
        &self,
        cf: &Arc<ColumnFamily>,
        partition: &str,
        tier: &str,
    ) -> Result<()> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.inner.poison.check()?;
        if !cf.tiers().is_known(Some(tier)) {
            return Err(OndaError::InvalidArgs(format!("unknown tier {tier:?}")));
        }
        let _mu = cf.compact_mu.lock();

        let handles = cf.bottom_partition_handles(partition);
        if handles.is_empty() {
            return Err(OndaError::NotFound);
        }

        let dest_cf_dir = cf.tiers().cf_dir(Some(tier), cf.name());
        std::fs::create_dir_all(&dest_cf_dir)?;

        // Copy every file to the target tier and open new handles there, before
        // touching the manifest — the part stays fully live on its current tier
        // until the flip.
        let mut new_handles: Vec<Arc<SstHandle>> = Vec::new();
        for h in &handles {
            let src_klog = cf.klog_path_for(&h.meta);
            let src_vlog = vlog_path_for(&src_klog);
            let dst_klog = format!("{dest_cf_dir}/{}.klog", h.meta.id);
            let dst_vlog = format!("{dest_cf_dir}/{}.vlog", h.meta.id);
            copy_and_sync(&src_klog, &dst_klog)?;
            if Path::new(&src_vlog).exists() {
                copy_and_sync(&src_vlog, &dst_vlog)?;
            }
            let mut meta = h.meta.clone();
            meta.tier = Some(tier.to_string());
            new_handles.push(Arc::new(SstHandle {
                meta: meta.clone(),
                reader: cf.open_reader_for(&meta)?,
            }));
        }

        // Flip: swap the handles in memory, then persist the manifest (the
        // durable commit point that records tier=<tier> for these ids).
        cf.swap_bottom_tables(new_handles);
        self.inner.persist_manifest()?;

        // Delete the now-obsolete source files (default-tier copies). Crash
        // before this leaves harmless orphans on the source tier; the manifest
        // already points readers at the new tier.
        for h in &handles {
            h.reader.close();
            let src_klog = cf.klog_path_for(&h.meta);
            let src_vlog = vlog_path_for(&src_klog);
            self.inner.remove_sst_file(&src_klog);
            if Path::new(&src_vlog).exists() {
                self.inner.remove_sst_file(&src_vlog);
            }
        }
        Ok(())
    }

    /// Add a partition rule to a **live** column family, carving out a new named
    /// partition of the keyspace (see
    /// [`ColumnFamilyConfig::partition_rules`](crate::ColumnFamilyConfig::partition_rules)).
    ///
    /// **Write-side-only semantics.** The rule affects only *future* bottom-level
    /// compactions: the next compaction that reaches the bottom level cuts its
    /// output files on the new boundary. No existing data is rewritten — bottom
    /// SSTables already on disk keep whatever partition stamp they were cut with
    /// until a later compaction happens to touch them. So a freshly added rule
    /// does not immediately make its partition detachable/tierable; flush +
    /// compact first to materialize a clean part on the boundary.
    ///
    /// The rule is validated with the same check applied at CF creation
    /// ([`ColumnFamilyConfig::validate`](crate::ColumnFamilyConfig::validate)): an
    /// exact-duplicate prefix is rejected with [`OndaError::InvalidArgs`]. Nested
    /// prefixes are legal (longest-prefix-wins). The new rule set is persisted via
    /// the standard manifest rewrite ([`DbInner::persist_manifest`]), so it
    /// survives reopen; a compaction already in flight finishes on the rules it
    /// snapshotted at its start.
    ///
    /// Concurrent adds are safe: validation and the in-memory append happen under
    /// one lock, so a duplicate racing add is rejected rather than both landing.
    pub fn add_partition_rule(
        &self,
        cf: &Arc<ColumnFamily>,
        rule: crate::config::PartitionRule,
    ) -> Result<()> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.inner.poison.check()?;
        // Validate + append to the live rules under the CF's rule lock, then
        // persist the manifest (its own `manifest_mu` serializes the rewrite).
        // The lock is released before persist because `persist_manifest` re-reads
        // the live rules through `effective_config`.
        cf.append_partition_rule(rule)?;
        self.inner.persist_manifest()?;
        Ok(())
    }

    /// Remove the partition rule whose prefix exactly equals `prefix` from a live
    /// column family, then persist. Errors with [`OndaError::NotFound`] if no
    /// rule has that exact prefix.
    ///
    /// Symmetric with [`add_partition_rule`](Self::add_partition_rule) and equally
    /// write-side-only: future bottom compactions stop cutting on the boundary,
    /// but bottom parts already stamped with the removed partition keep those
    /// stamps (and stay detachable by name) until a later compaction merges them
    /// back into their neighbors.
    pub fn remove_partition_rule(&self, cf: &Arc<ColumnFamily>, prefix: &[u8]) -> Result<()> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.inner.poison.check()?;
        cf.remove_partition_rule(prefix)?;
        self.inner.persist_manifest()?;
        Ok(())
    }
}

/// Move `from` to `to`, falling back to copy + delete across filesystems.
fn move_file(from: &str, to: &str) -> Result<()> {
    match std::fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(_) => {
            std::fs::copy(from, to)?;
            std::fs::remove_file(from)?;
            Ok(())
        }
    }
}

/// Copy `from` to `to`, fsyncing the destination file and its directory so the
/// copy is durable before the manifest flip references it.
fn copy_and_sync(from: &str, to: &str) -> Result<()> {
    std::fs::copy(from, to)?;
    if let Ok(f) = std::fs::File::open(to) {
        let _ = f.sync_all();
    }
    if let Some(parent) = Path::new(to).parent() {
        if let Ok(d) = std::fs::File::open(parent) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

fn file_len(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}
