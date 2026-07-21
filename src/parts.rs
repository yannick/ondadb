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

/// A semantic boundary in the crash-safe part-move protocol.
///
/// These events are emitted only by [`DB::move_part_to_tier_observed`]. The
/// ordinary mover does not allocate events or invoke a callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MovePhase {
    /// One complete destination object has received all source bytes, but its
    /// [`StorageWriter`](crate::storage::StorageWriter) has not been finished.
    CopyComplete {
        /// One-based object number within this part move.
        object_index: usize,
        /// Total destination objects in this part move.
        object_count: usize,
    },
    /// Every destination writer finished, so all copied objects are durable.
    DestinationSynced,
    /// The crash-atomic manifest durably names the destination tier.
    ManifestFlipped,
    /// Every source deletion was issued. A nonzero count means checkpoint or
    /// backup pinning deferred physical unlink; catalog authority has already
    /// moved to the destination.
    SourceDeleteFinished { remaining_files: usize },
}

impl MovePhase {
    /// Whether two observations name the same semantic boundary, ignoring its
    /// per-run counters. Useful for deterministic fault selection.
    pub fn same_kind(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// Run-bound identity accompanying one observed move boundary.
#[derive(Clone, Debug)]
pub struct MovePhaseEvent<'a> {
    pub cf_name: &'a str,
    pub partition: &'a str,
    pub destination_tier: &'a str,
    pub phase: MovePhase,
}

/// Synchronous observer for deterministic crash/fault harnesses.
///
/// The callback runs while the column-family compaction mutex is held. It may
/// block to coordinate a subprocess kill. Returning an error before
/// [`MovePhase::ManifestFlipped`] interrupts the move. At and after that durable
/// commit point the engine reports the event but deliberately ignores callback
/// errors: a diagnostic hook cannot turn a committed move into an apparent
/// failure. Production placement policy should use the unobserved mover APIs.
pub trait MovePhaseObserver: Send + Sync {
    fn observe(&self, event: &MovePhaseEvent<'_>) -> Result<()>;
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
                // The attached data is being brought in now; give it a defined
                // age so the mover treats it as freshly written (it must age
                // `min_age` again before qualifying for a tier move).
                meta.max_entry_time = Some(crate::util::now_nanos());
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
        self.inner.relocate_part(cf, partition, tier, None)
    }

    /// Move one part while reporting every durability boundary to `observer`.
    ///
    /// This is the deterministic fault-harness form of
    /// [`move_part_to_tier`](Self::move_part_to_tier). It executes the exact
    /// same persistence helper and ordering; the observer cannot replace or
    /// acknowledge an engine mutation. Observer errors before the durable
    /// manifest flip are returned; errors at or after it are ignored so a
    /// committed move is never reported as failed.
    pub fn move_part_to_tier_observed(
        &self,
        cf: &Arc<ColumnFamily>,
        partition: &str,
        tier: &str,
        observer: &dyn MovePhaseObserver,
    ) -> Result<()> {
        self.inner
            .relocate_part(cf, partition, tier, Some(observer))
    }

    /// Run one full pass of the background part mover across every column family
    /// and return the number of parts relocated.
    ///
    /// For each bottom-level part the mover resolves the target tier from the
    /// CF's [`tier_rules`](crate::ColumnFamilyConfig::tier_rules) (longest prefix
    /// wins) and moves the part there when it is not already on that tier and its
    /// newest entry is older than the rule's
    /// [`min_age`](crate::config::TierRule::min_age). Each move uses the same
    /// crash-safe copy → fsync → manifest-flip → delete protocol as
    /// [`move_part_to_tier`](Self::move_part_to_tier) and is idempotent: a
    /// re-run once a part is placed is a no-op.
    ///
    /// This is the manual trigger (used by tests and callers that want a mover
    /// pass on demand); the same pass also runs on a background cadence
    /// ([`Options::part_mover_interval`](crate::Options::part_mover_interval))
    /// on the compaction worker.
    pub fn run_part_mover(&self) -> Result<usize> {
        self.inner.run_part_mover()
    }
}

impl crate::db::DbInner {
    /// The crash-safe cross-tier part move (the mover protocol, §10 of the parts
    /// & tiers plan). Shared by the manual
    /// [`DB::move_part_to_tier`](crate::DB::move_part_to_tier) lever and the
    /// policy-driven [`run_part_mover`](Self::run_part_mover).
    pub(crate) fn relocate_part(
        &self,
        cf: &Arc<ColumnFamily>,
        partition: &str,
        tier: &str,
        observer: Option<&dyn MovePhaseObserver>,
    ) -> Result<()> {
        if self.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.poison.check()?;
        if !cf.tiers().is_known(Some(tier)) {
            return Err(OndaError::InvalidArgs(format!("unknown tier {tier:?}")));
        }
        let _mu = cf.compact_mu.lock();

        let handles = cf.bottom_partition_handles(partition);
        if handles.is_empty() {
            return Err(OndaError::NotFound);
        }
        // A caller can lose the response after the manifest commit and retry
        // the same move. In that case the handles already resolve to `tier`;
        // copying them onto themselves would truncate the source when the
        // destination writer is created.
        if handles
            .iter()
            .all(|handle| handle.meta.tier.as_deref() == Some(tier))
        {
            return Ok(());
        }

        // The destination backend may be local or remote (S3); route all writes
        // through it so the same mover protocol serves both — only the `Storage`
        // impl differs (a local copy+fsync vs. a buffered single-shot PUT).
        let dest_storage = cf.tiers().storage_for(Some(tier));
        let dest_cf_dir = cf.tiers().cf_dir(Some(tier), cf.name());
        dest_storage.ensure_dir(&dest_cf_dir)?;

        // Copy every file to the target tier and open new handles there, before
        // touching the manifest — the part stays fully live on its current tier
        // until the flip.
        let object_count = handles
            .iter()
            .map(|handle| {
                let source_klog = cf.klog_path_for(&handle.meta);
                1 + usize::from(Path::new(&vlog_path_for(&source_klog)).exists())
            })
            .sum();
        let mut object_index = 0;
        let mut new_handles: Vec<Arc<SstHandle>> = Vec::new();
        for h in &handles {
            let src_klog = cf.klog_path_for(&h.meta);
            let src_vlog = vlog_path_for(&src_klog);
            let dst_klog = format!("{dest_cf_dir}/{}.klog", h.meta.id);
            let dst_vlog = format!("{dest_cf_dir}/{}.vlog", h.meta.id);
            object_index += 1;
            copy_to_storage(&src_klog, &dst_klog, &dest_storage, || {
                observe_move(
                    observer,
                    cf.name(),
                    partition,
                    tier,
                    MovePhase::CopyComplete {
                        object_index,
                        object_count,
                    },
                )
            })?;
            if Path::new(&src_vlog).exists() {
                object_index += 1;
                copy_to_storage(&src_vlog, &dst_vlog, &dest_storage, || {
                    observe_move(
                        observer,
                        cf.name(),
                        partition,
                        tier,
                        MovePhase::CopyComplete {
                            object_index,
                            object_count,
                        },
                    )
                })?;
            }
            let mut meta = h.meta.clone();
            meta.tier = Some(tier.to_string());
            new_handles.push(Arc::new(SstHandle {
                meta: meta.clone(),
                reader: cf.open_reader_for(&meta)?,
            }));
        }
        observe_move(
            observer,
            cf.name(),
            partition,
            tier,
            MovePhase::DestinationSynced,
        )?;

        // Flip: swap the handles in memory, then persist the manifest (the
        // durable commit point that records tier=<tier> for these ids).
        cf.swap_bottom_tables(new_handles);
        self.persist_manifest()?;
        observe_committed_move(
            observer,
            cf.name(),
            partition,
            tier,
            MovePhase::ManifestFlipped,
        );

        // Delete the now-obsolete source files (default-tier copies). Crash
        // before this leaves harmless orphans on the source tier; the manifest
        // already points readers at the new tier.
        for h in &handles {
            h.reader.close();
            let src_klog = cf.klog_path_for(&h.meta);
            let src_vlog = vlog_path_for(&src_klog);
            self.remove_sst_file(&src_klog);
            if Path::new(&src_vlog).exists() {
                self.remove_sst_file(&src_vlog);
            }
        }
        let remaining_files = handles
            .iter()
            .flat_map(|handle| {
                let klog = cf.klog_path_for(&handle.meta);
                let vlog = vlog_path_for(&klog);
                [klog, vlog]
            })
            .filter(|path| Path::new(path).exists())
            .count();
        observe_committed_move(
            observer,
            cf.name(),
            partition,
            tier,
            MovePhase::SourceDeleteFinished { remaining_files },
        );
        Ok(())
    }

    /// One full pass of the part mover; see
    /// [`DB::run_part_mover`](crate::DB::run_part_mover).
    pub(crate) fn run_part_mover(&self) -> Result<usize> {
        if self.opts.read_only {
            return Ok(0);
        }
        self.poison.check()?;
        let cfs: Vec<Arc<ColumnFamily>> = self.cfs.read().values().cloned().collect();
        let now = crate::util::now_nanos();
        let mut moved = 0usize;
        for cf in &cfs {
            let rules = cf.tier_rules();
            if rules.is_empty() {
                continue;
            }
            // Snapshot the bottom-level parts once, then act on each: the
            // relocate below re-snapshots the part's handles under the CF's
            // compaction lock, so a concurrent compaction between snapshot and
            // move can only make a part vanish (relocate then finds nothing and
            // is a no-op), never move stale data.
            for part in cf.bottom_parts() {
                let Some(rule) = crate::config::tier_for_key(rules, &part.min_key) else {
                    continue;
                };
                // The reserved name "ssd" denotes the default tier (`None`).
                let target: Option<&str> = if rule.tier == "ssd" {
                    None
                } else {
                    Some(rule.tier.as_str())
                };
                // Already on the target tier → nothing to do (idempotent).
                if target == part.tier.as_deref() {
                    continue;
                }
                // P4 relocates onto named local tiers only; moving a part back to
                // the default tier is out of scope (there is no copy target).
                let Some(target) = target else { continue };
                // Age gate: only move once the part's newest entry is older than
                // the rule's min_age. An unknown age (`None`) is never eligible.
                let Some(newest) = part.max_entry_time else {
                    continue;
                };
                if now.saturating_sub(newest) <= rule.min_age.as_nanos() as i64 {
                    continue;
                }
                match self.relocate_part(cf, &part.partition, target, None) {
                    Ok(()) => moved += 1,
                    // A part that vanished (compacted/detached) between snapshot
                    // and move is a benign miss; a genuine durability failure has
                    // already poisoned the DB via persist_manifest.
                    Err(OndaError::NotFound) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(moved)
    }
}

impl DB {
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

/// Copy the local file `from` to `to` on `storage`, durably committing the
/// destination before the manifest flip references it. For a local tier the
/// [`StorageWriter`](crate::storage::StorageWriter) streams and fsyncs (file +
/// parent dir); for an S3 tier it buffers and single-shot PUTs on finish. The
/// source is always on a local tier (the mover only moves *onto* named tiers), so
/// it is read with a plain file.
fn copy_to_storage(
    from: &str,
    to: &str,
    storage: &Arc<dyn crate::storage::Storage>,
    copied: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let mut src = std::fs::File::open(from)?;
    let mut dst = storage.create(to)?;
    std::io::copy(&mut src, &mut *dst)?;
    copied()?;
    dst.finish()?;
    Ok(())
}

fn observe_move(
    observer: Option<&dyn MovePhaseObserver>,
    cf_name: &str,
    partition: &str,
    destination_tier: &str,
    phase: MovePhase,
) -> Result<()> {
    match observer {
        Some(observer) => observer.observe(&MovePhaseEvent {
            cf_name,
            partition,
            destination_tier,
            phase,
        }),
        None => Ok(()),
    }
}

fn observe_committed_move(
    observer: Option<&dyn MovePhaseObserver>,
    cf_name: &str,
    partition: &str,
    destination_tier: &str,
    phase: MovePhase,
) {
    // The manifest already durably names the destination. Returning a hook
    // error now would tell callers the move failed even though retry/recovery
    // must treat it as committed.
    let _ = observe_move(observer, cf_name, partition, destination_tier, phase);
}

fn file_len(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}
