//! Maintenance operations: checkpoint, backup, column-family clone, and stats.
//!

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::column_family::{ColumnFamily, SstHandle};
use crate::db::DB;
use crate::error::{OndaError, Result};
use crate::manifest::SstMeta;
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

/// One file a snapshot copies: its place in the snapshot (`cf-<cf>/<id>.<ext>`)
/// and where its bytes are read from now.
pub(crate) struct SnapshotFile {
    pub(crate) cf: String,
    pub(crate) id: u64,
    pub(crate) ext: &'static str,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) src: String,
    pub(crate) size: u64,
}

/// See [`DB::plan_snapshot`].
pub(crate) struct SnapshotPlan {
    pub(crate) manifest: crate::manifest::Manifest,
    pub(crate) files: Vec<SnapshotFile>,
    pub(crate) cfs: Vec<Arc<ColumnFamily>>,
}

impl SnapshotPlan {
    /// Make the manifest snapshot-only before it is saved into a destination:
    /// a fresh generation, nothing applied, and no `MANIFEST-EDITS` beside it.
    /// It grows a log the first time it is opened writable and mutated.
    /// Copying the source's cursor instead would describe a log the
    /// destination does not have.
    pub(crate) fn finalize_manifest(&mut self) {
        self.manifest.generation = 1;
        self.manifest.applied_through = 0;
        self.manifest.next_edit_id = 1;
    }
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
    /// Subset of `compaction_count` that the periodic (age) trigger picked
    /// rather than a capacity trigger — see
    /// [`ColumnFamilyConfig::periodic_compaction_interval`](crate::config::ColumnFamilyConfig::periodic_compaction_interval).
    ///
    /// Zero for every database that leaves the option at its default, and the
    /// number an operator watches to tell idle reclamation from ingest-driven
    /// compaction. Counts completed jobs only: a job that failed leaves its
    /// input's stamp untouched, so the table stays eligible and is retried.
    pub periodic_compactions: u64,
    /// Subset of `compaction_count` picked by the tombstone-density trigger
    /// ([`ColumnFamilyConfig::tombstone_density_trigger`](crate::config::ColumnFamilyConfig::tombstone_density_trigger)).
    /// Completed jobs only; zero while the trigger is off.
    pub tombstone_density_compactions: u64,
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
    /// Range-delete records (1.2) committed to this family since it was
    /// opened. Zero for every family that never calls `delete_range`.
    pub range_deletes: u64,
    /// Range-tombstone *fragments* across every catalogued table of this
    /// family — the durable cost of the deletes above, after flush and
    /// compaction have merged and clipped them.
    pub range_fragments: u64,
    /// Tables delete-only excise (1.2) has retired since this family was
    /// opened, and the klog+vlog bytes they held.
    ///
    /// Space reclaimed **without reading or rewriting** the data — a table every
    /// one of whose keys a durable range tombstone already deleted is removed by
    /// catalog edit alone. Compare against `compaction_count`: a bulk
    /// `delete_range` whose reclamation shows up here cost no read or write
    /// amplification at all, where the same delete expressed as point tombstones
    /// would have been paid for in full by compaction.
    pub excised_tables: u64,
    /// Bytes those tables held (`klog_size + vlog_size`).
    pub excised_bytes: u64,
    /// Markers the database-wide committed-span index currently holds.
    ///
    /// A database-wide number reported per family because that is where an
    /// operator looks: it grows with commit rate while a long-lived snapshot
    /// holds the prune floor down, and range commits wait when it reaches
    /// [`Options::span_index_capacity`](crate::config::Options::span_index_capacity).
    pub span_markers: usize,
    /// Bytes by which the levels exceed their capacities — the backlog
    /// compaction still owes. Writers pace against this once it passes
    /// `soft_pending_compaction_bytes` and block at
    /// `hard_pending_compaction_bytes`, so a value pinned near the hard limit
    /// means ingest is outrunning compaction.
    pub compaction_debt: u64,
    /// Spans the most recent **bounded** compaction job on this family ran
    /// (0.8). `1` means it ran as one merge — the default, and what a family
    /// excluded from spanning always reports. The single-span job classes
    /// ([`DB::compact`](crate::DB::compact)'s whole-level sweep, FIFO eviction)
    /// leave it alone rather than overwriting what the bounded jobs did.
    /// See [`Options::max_subcompactions`](crate::Options::max_subcompactions).
    pub span_count: u64,
    /// `max_span_bytes - min_span_bytes` for that job: how unevenly the
    /// boundary planner divided the work. Near zero is the goal; a large value
    /// on a multi-span job means the span boundaries did not track the input
    /// bytes, and the job took as long as its widest span.
    pub span_imbalance_bytes: u64,
}

/// Database-wide statistics.
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    /// Range spans and estimated owned bytes in active/sealed memtables (unified counted once).
    pub range_memtable_spans: u64,
    pub range_memtable_bytes: u64,
    /// Resident memtable fragment snapshots, excluding allocator headers.
    /// SST-reader fragments and transaction overlays are not included.
    pub range_fragment_cache_bytes: u64,
    /// Memtable snapshots retained by readers after mutation or retirement.
    pub range_fragment_retained_bytes: u64,
    /// Builds/hits for currently live memtable sets; these reset as sets retire.
    pub range_fragment_cache_builds: u64,
    pub range_fragment_cache_hits: u64,
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
    /// Entries (either domain) the block cache evicted to stay under
    /// capacity. With background admission off (the default,
    /// [`Options::admit_background_scan_blocks`](crate::Options::admit_background_scan_blocks))
    /// only foreground reads insert, so compaction cannot drive this up.
    pub block_cache_evictions: u64,
    /// Bytes the block cache currently holds, both domains.
    pub block_cache_bytes: i64,
}

impl ColumnFamily {
    /// Snapshot statistics for this column family.
    pub fn stats(&self) -> CfStats {
        let levels = self.level_summary();
        let (entries, tombs) = self.entry_counts();
        let (excised_tables, excised_bytes) = self.excised();
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
            periodic_compactions: self
                .periodic_compactions
                .load(std::sync::atomic::Ordering::Relaxed),
            tombstone_density_compactions: self
                .tombstone_density_compactions
                .load(std::sync::atomic::Ordering::Relaxed),
            compaction_failures: self
                .compaction_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            last_compaction_error: self.last_compaction_error.lock().clone(),
            point_reads: self.point_reads.load(std::sync::atomic::Ordering::Relaxed),
            bloom_skips: self.bloom_skips.load(std::sync::atomic::Ordering::Relaxed),
            sst_probes: self.sst_probes.load(std::sync::atomic::Ordering::Relaxed),
            range_deletes: self.range_deletes(),
            range_fragments: self
                .table_metadata()
                .iter()
                .flatten()
                .map(|m| m.range_count)
                .sum(),
            excised_tables,
            excised_bytes,
            span_markers: self.span_marker_count(),
            compaction_debt: self
                .compaction_debt
                .load(std::sync::atomic::Ordering::Relaxed),
            span_count: self.span_count.load(std::sync::atomic::Ordering::Relaxed),
            span_imbalance_bytes: self
                .span_imbalance_bytes
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
        let mut ranges = crate::range_tombstone::RangeCacheStats::default();
        for cf in &cfs {
            ranges += cf.range_cache_stats();
        }
        if let Some(u) = &self.inner.unified {
            ranges += u.range_cache_stats();
        }
        let (cache_bytes, retained_bytes) = self.inner.ctx.range_fragment_registry.stats();
        DbStats {
            range_memtable_spans: ranges.spans,
            range_memtable_bytes: ranges.span_bytes,
            range_fragment_cache_bytes: cache_bytes,
            range_fragment_retained_bytes: retained_bytes,
            range_fragment_cache_builds: ranges.builds,
            range_fragment_cache_hits: ranges.hits,
            num_column_families: cfs.len(),
            total_sstables,
            total_bytes,
            block_cache_hits: bc.hits,
            block_cache_misses: bc.misses,
            vlog_cache_hits: bc.vlog_hits,
            vlog_cache_misses: bc.vlog_misses,
            vlog_cache_bytes: bc.vlog_bytes,
            block_cache_evictions: bc.evictions,
            block_cache_bytes: bc.bytes,
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
        let mut plan = self.plan_snapshot()?;

        std::fs::create_dir_all(dir)?;
        for cfm in &plan.manifest.cfs {
            std::fs::create_dir_all(dir.join(crate::format::cf_dir_name(&cfm.name)))?;
        }
        for f in &plan.files {
            let dst = dir.join(crate::format::cf_dir_name(&f.cf)).join(format!("{}.{}", f.id, f.ext));
            place_storage_file(f.storage.as_ref(), &f.src, &dst, hard_link)?;
        }
        if self.inner.opts.read_only {
            self.write_sealed_memtables(dir, &plan.cfs, &mut plan.manifest)?;
        }
        plan.finalize_manifest();
        plan.manifest.save(dir.join("MANIFEST"))?;
        Ok(())
    }

    /// Everything a snapshot copies, gathered under the caller's
    /// `pause_deletions` guard (which must outlive every use of the plan: it is
    /// what keeps the listed source files on disk).
    ///
    /// Flushes every family (on a read-only source this only seals the
    /// replayed memtables — the caller writes them with
    /// `write_sealed_memtables`), persists the catalog, and loads it back with
    /// `recover_catalog`. The returned manifest already names every table at
    /// its snapshot-relative place (tier and object cleared), and `files` lists
    /// each `.klog` / non-empty `.vlog` with the backend and path it is read
    /// from — so a local checkpoint and an object-store checkpoint copy the
    /// exact same set.
    pub(crate) fn plan_snapshot(&self) -> Result<SnapshotPlan> {
        let cfs: Vec<Arc<ColumnFamily>> = self.inner.cfs.read().values().cloned().collect();
        // Flush memtables so all data lives in SSTables, then persist manifest.
        for cf in &cfs {
            self.flush_memtable(cf)?;
        }
        let read_only = self.inner.opts.read_only;
        if read_only {
            // Same effect as the per-family rotations above: seal the unified
            // store's replayed data so `write_sealed_memtables` sees all of it.
            if let Some(u) = &self.inner.unified {
                u.rotate(true);
            }
        }
        self.inner.persist_manifest()?;

        // Load the manifest and link exactly the files it references. With deletions
        // paused, every file any persisted manifest lists still exists on disk, so
        // the copied catalog and the copied files are guaranteed consistent — even if
        // a compaction rewrote the live manifest after our persist above.
        // `recover_catalog`, not `Manifest::load`: with 2.2's edit log the
        // snapshot on disk is only the whole catalog once the log has been
        // replayed into it. A read-only source cannot force a compaction
        // (`persist_manifest` returns early), so loading the bare snapshot there
        // would silently drop every edit since the last one — which is what
        // would make the "read-only-capable backup" claim false.
        let mut manifest = crate::manifest_edit::recover_catalog(&self.inner.dir)?;
        let mut files = Vec::new();
        for cfm in &mut manifest.cfs {
            let source_cf = cfs
                .iter()
                .find(|cf| cf.name() == cfm.name)
                .ok_or(OndaError::NotFound)?;
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
                    files.push(SnapshotFile {
                        cf: cfm.name.clone(),
                        id: sst.id,
                        ext,
                        storage: storage.clone(),
                        src,
                        size,
                    });
                }
                sst.tier = None;
                sst.object = None;
            }
        }
        Ok(SnapshotPlan {
            manifest,
            files,
            cfs,
        })
    }

    /// Carry a read-only database's memtable data into a snapshot.
    ///
    /// A read-only open replays the WAL into memtables but runs no flush
    /// worker, so `flush_memtable` above only sealed that data — no table was
    /// written and the catalog does not know it. The snapshot has no WAL, so
    /// copying the catalog alone would silently drop every write that was only
    /// in the WAL. Instead each sealed memtable is written, **into the
    /// destination only**, as the L0 table a flush would have produced, and
    /// added to the destination catalog as the newest L0 entry of its family.
    /// The source directory is never written.
    ///
    /// The tables use the same writer and capability gates as a flush, so the
    /// destination needs nothing the source catalog did not already declare;
    /// ids come from this handle's counter, which starts above every id the
    /// source catalog holds, and `global_seq`/`next_file_id` are raised to
    /// cover what was written.
    pub(crate) fn write_sealed_memtables(
        &self,
        dir: &Path,
        cfs: &[Arc<ColumnFamily>],
        manifest: &mut crate::manifest::Manifest,
    ) -> Result<()> {
        // Per family, the new tables oldest first; prepended in reverse so the
        // catalog's L0 stays newest first.
        let mut added: Vec<(String, Vec<SstMeta>)> = Vec::new();
        let mut write = |cf: &ColumnFamily,
                         entries: &[crate::memtable::Entry],
                         fragments: Vec<crate::range_tombstone::Fragment>|
         -> Result<()> {
            let id = self.inner.next_file_id();
            let klog = dir
                .join(crate::format::cf_dir_name(cf.name()))
                .join(format!("{id}.klog"));
            let klog = klog.to_str().ok_or_else(|| {
                OndaError::InvalidArgs(format!("snapshot path {klog:?} is not UTF-8"))
            })?;
            if let Some(meta) = cf.write_detached_l0(klog, entries, fragments, id)? {
                match added.iter_mut().find(|(name, _)| name == cf.name()) {
                    Some((_, metas)) => metas.push(meta),
                    None => added.push((cf.name().to_string(), vec![meta])),
                }
            }
            Ok(())
        };
        // Per-family memtables hold data older than the unified store's (a
        // family only has them before a migration to the unified layout).
        for cf in cfs {
            for (entries, fragments) in cf.sealed_contents() {
                write(cf, &entries, fragments)?;
            }
        }
        if let Some(u) = &self.inner.unified {
            for imm in u.sealed() {
                let mut ranges = crate::unified::split_ranges_by_cf(&imm);
                let mut slices = crate::unified::split_by_cf(&imm);
                for (cf_id, _) in &ranges {
                    if !slices.iter().any(|(id, _)| id == cf_id) {
                        slices.push((*cf_id, Vec::new()));
                    }
                }
                for (cf_id, mut entries) in slices {
                    let Some(cf) = cfs.iter().find(|cf| cf.id() == cf_id) else {
                        continue;
                    };
                    let fragments = ranges
                        .iter_mut()
                        .find(|(id, _)| *id == cf_id)
                        .map(|(_, f)| std::mem::take(f))
                        .unwrap_or_default();
                    cf.sort_internal(&mut entries);
                    write(cf, &entries, fragments)?;
                }
            }
        }
        for (name, metas) in added {
            let cfm = manifest
                .cfs
                .iter_mut()
                .find(|cfm| cfm.name == name)
                .ok_or(OndaError::NotFound)?;
            for meta in metas {
                manifest.global_seq = manifest.global_seq.max(meta.max_seq);
                manifest.next_file_id = manifest.next_file_id.max(meta.id + 1);
                cfm.sstables.insert(0, meta);
            }
        }
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

        // The destination family is created *and* populated by ONE edit
        // (`CreateCF` + `AddTable` x N), so it never exists on disk as an empty
        // catalog entry a crash could strand. That rules out routing through
        // `create_column_family`, which is a transaction of its own.
        let _lifecycle = self.inner.cf_lifecycle_mu.lock();
        if self.inner.cfs.read().contains_key(dst) {
            return Err(OndaError::Exists(dst.into()));
        }
        let config = src_cf.effective_config();
        let comparator = crate::comparator::comparator_by_name(&config.comparator_name)
            .ok_or_else(|| {
                OndaError::InvalidArgs(format!("unknown comparator {}", config.comparator_name))
            })?;
        let dst_cf = crate::column_family::ColumnFamily::create(
            self.inner.ctx.clone(),
            dst.to_string(),
            self.inner.cf_dir(dst),
            config,
            comparator,
        )?;

        // Hard-link each src SSTable into dst under a fresh id.
        let src_metas: Vec<SstMeta> = src_cf.snapshot_ssts();
        let mut by_level: Vec<Vec<Arc<SstHandle>>> = Vec::new();
        let mut linked: Vec<std::path::PathBuf> = Vec::new();
        let mut new_metas: Vec<SstMeta> = Vec::new();
        let staging = (|| -> Result<()> {
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
                    linked.push(destination);
                }
                let level = meta.level as usize;
                let mut new_meta = meta;
                new_meta.id = new_id;
                new_meta.tier = None;
                new_meta.object = None;
                while by_level.len() <= level {
                    by_level.push(Vec::new());
                }
                by_level[level].push(dst_cf.open_sst(new_meta.clone())?);
                new_metas.push(new_meta);
            }
            Ok(())
        })();
        if let Err(e) = staging {
            for path in &linked {
                let _ = std::fs::remove_file(path);
            }
            return Err(e);
        }
        if by_level.is_empty() {
            by_level.push(Vec::new());
        }
        let mut ops = vec![crate::manifest_edit::Op::CreateCf {
            name: dst.to_string(),
            config: dst_cf.effective_config().encode(),
        }];
        for meta in new_metas {
            ops.push(crate::manifest_edit::Op::AddTable {
                cf: dst.to_string(),
                meta,
            });
        }
        let published = dst_cf.clone();
        if let Err(e) = self
            .inner
            .catalog_txn(crate::manifest_edit::VersionEdit::new(ops), |p| {
                self.inner.register_cf(&published, p);
                published.install_levels(by_level, p);
            })
        {
            // Nothing was published; the hard links this call made are the only
            // trace, and they go.
            for path in &linked {
                let _ = std::fs::remove_file(path);
            }
            return Err(e);
        }
        Ok(dst_cf)
    }
}
