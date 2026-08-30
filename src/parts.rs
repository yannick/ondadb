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

use crate::column_family::{BottomPart, ColumnFamily, SstHandle};
use crate::config::TierRule;
use crate::db::DB;
use crate::error::{OndaError, Result};
use crate::manifest::{manifest_path, CfManifest, Manifest, SstMeta, WalLayout};
use crate::range_lock::{KeyRange, RangeGuard};
use crate::sst::vlog_path_for;

fn staged_range_overlaps(
    cf: &ColumnFamily,
    staged_bottom: &[(Vec<u8>, Vec<u8>)],
    min_key: &[u8],
    max_key: &[u8],
) -> bool {
    let cmp = cf.cmp();
    staged_bottom.iter().any(|(staged_min, staged_max)| {
        cmp.compare(min_key, staged_max).is_le() && cmp.compare(staged_min, max_key).is_le()
    })
}

/// Lock the key span covering `partition`'s bottom-level tables, so compaction
/// cannot rewrite them between the caller's snapshot and its removal.
///
/// Before 0.8.0 that exclusion came from `cf.compact_mu`, which compaction held
/// for a whole run. Compaction now holds only the range it rewrites, so these
/// operations have to name a range too, or they lose the guarantee entirely —
/// the failure being a tier move and a compaction rewriting the same bottom
/// tables, one of them installing tables whose inputs the other has unlinked.
///
/// The span is re-read under the lock: a compaction that finished between the
/// first read and the acquire may have widened the partition's extent. If it
/// did, widen and retry. Falls back to locking the whole keyspace, which is
/// always correct and is what the old `compact_mu` effectively did.
fn lock_partition_span(cf: &Arc<ColumnFamily>, partition: &str) -> RangeGuard {
    let cmp = cf.cmp();
    let span_of = |handles: &[Arc<SstHandle>]| -> Option<KeyRange> {
        let spans: Vec<(&[u8], &[u8])> = handles
            .iter()
            .map(|h| (h.meta.min_key.as_slice(), h.meta.max_key.as_slice()))
            .collect();
        KeyRange::union(spans, &cmp)
    };
    let within = |handles: &[Arc<SstHandle>], r: &KeyRange| -> bool {
        handles.iter().all(|h| {
            let lo_ok = r
                .min
                .as_ref()
                .is_none_or(|m| cmp.compare(&h.meta.min_key, m).is_ge());
            let hi_ok = r
                .max
                .as_ref()
                .is_none_or(|m| cmp.compare(&h.meta.max_key, m).is_le());
            lo_ok && hi_ok
        })
    };

    for _ in 0..4 {
        let Some(range) = span_of(&cf.bottom_partition_handles(partition)) else {
            // Nothing materialized: the caller will report NotFound, but it
            // must still do so under a lock, or a compaction could materialize
            // the part underneath the check.
            return cf.range_locks.acquire_blocking(KeyRange::all());
        };
        let guard = cf.range_locks.acquire_blocking(range.clone());
        if within(&cf.bottom_partition_handles(partition), &range) {
            return guard;
        }
        drop(guard); // extent grew under us; re-derive and try again
    }
    cf.range_locks.acquire_blocking(KeyRange::all())
}

/// One SSTable of an exported part, described independently of this database.
///
/// Everything here except [`content`](Self::content) is copied from the
/// catalog. `content` is read from the bytes, which is what makes the
/// description an *identity* rather than a summary: two databases can compare
/// parts without trusting each other's metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartTable {
    /// The table's file id **in the exporting database**. Local to that
    /// database and not an identity — see [`PartManifest`].
    pub id: u64,
    /// Smallest user key in the table.
    pub min_key: Vec<u8>,
    /// Largest user key in the table.
    pub max_key: Vec<u8>,
    /// Highest sequence number in the table.
    pub max_seq: u64,
    /// Live entries (including tombstones).
    pub num_entries: u64,
    /// Size of the key log in bytes.
    pub klog_size: u64,
    /// Size of the value log in bytes; `0` when the table has none.
    pub vlog_size: u64,
    /// Tier the bytes currently live on, or `None` for the default tier.
    pub tier: Option<String>,
    /// Tier-root-relative object path on a SHARED tier (A2), the name
    /// [`attach_part_by_ref`](crate::DB::attach_part_by_ref) mounts. `None`
    /// for default-tier and non-shared-tier tables — those are not mountable
    /// by reference. Deliberately excluded from the part digest: the digest
    /// identifies bytes, and a rename is not a rewrite.
    pub object: Option<String>,
    /// SHA-256 over the klog bytes followed by the vlog bytes — the table's
    /// content identity.
    pub content: [u8; 32],
}

/// A part, described so another database (or another process entirely) can
/// recognise it: its tables, their key ranges, and a digest over their bytes.
///
/// # Why this exists
///
/// A part is identified inside a database by its tables' **file ids**, which
/// are local counters — id 7 in one database has nothing to do with id 7 in
/// another. A consumer coordinating parts across machines (replicating them,
/// placing them in a shared object store, proving two replicas hold the same
/// bytes) needs an identity that travels, and until now had to maintain that
/// mapping itself, outside the engine that owns the facts.
///
/// [`digest`](Self::digest) is that identity. It covers every table's content
/// hash, key range and sequence bound, in a fixed order.
///
/// # It is a PHYSICAL identity, not a logical one
///
/// Read this before using the digest to decide anything.
///
/// The digest is independent of file ids, of file paths, of which tier the
/// bytes sit on, and of which database instance produced it: two databases
/// that performed the same writes produce the same digest, which is what makes
/// it usable across machines at all.
///
/// It is **not** independent of a database's write history. SSTable entries
/// carry sequence numbers, and those are database-global counters, so
/// unrelated earlier writes shift them and change the bytes. Two parts holding
/// logically identical data that arrived by different routes hash
/// *differently*.
///
/// So:
///
/// - **Right question:** "I am about to ship this part — does the peer already
///   have exactly these bytes?" The digest answers it, and answers it without
///   trusting either side's metadata.
/// - **Wrong question:** "Did two replicas independently rebuild the same
///   data?" The digest will say no even when they did. That comparison needs a
///   hash over *logical* content, which is the consumer's business, not the
///   engine's — only the consumer knows which parts of an entry are meaningful
///   to it.
///
/// Both tests for this live in `tests/parts.rs`, including one that asserts the
/// limitation, so a change that made the digest logical cannot land without
/// updating this paragraph.
///
/// # Cost
///
/// Exporting **reads every byte of the part** to hash it. That is the price of
/// an identity rather than an assertion, and it is deliberately not optional:
/// a metadata-only digest would compare catalog entries, which is precisely
/// the thing a consumer cannot afford to trust when it is checking whether two
/// machines agree. A part on a remote tier is hashed where it lives, through
/// that tier's backend, rather than being brought local first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartManifest {
    /// Column family the part belongs to.
    pub cf: String,
    /// The partition this part materialises.
    pub partition: String,
    /// The part's tables, ordered by `min_key` then `id` so the digest is
    /// independent of catalog iteration order.
    pub tables: Vec<PartTable>,
    /// SHA-256 over the canonical encoding of [`tables`](Self::tables).
    pub digest: [u8; 32],
}

impl PartManifest {
    /// Total bytes across every table of the part.
    pub fn size_bytes(&self) -> u64 {
        self.tables.iter().map(|t| t.klog_size + t.vlog_size).sum()
    }

    /// The digest as lowercase hex — the form a consumer names objects with.
    pub fn digest_hex(&self) -> String {
        self.digest.iter().map(|b| format!("{b:02x}")).collect()
    }
}

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
        // out from under us between snapshot and removal. Scoped to this
        // partition's span, so compaction elsewhere in the CF continues.
        let _range = lock_partition_span(cf, partition);

        let handles = cf.bottom_partition_handles(partition);
        if handles.is_empty() {
            return Err(OndaError::NotFound);
        }
        // A shared-tier table is an immutable publication other databases may
        // reference; unlinking or hard-linking it locally is either data loss
        // for a sharer or nonsense for a remote object (A2 — shared tiers are
        // delete-free and fs-free).
        if let Some(h) = handles.iter().find(|h| {
            h.meta.tier.as_deref().is_some_and(|t| {
                self.inner
                    .opts
                    .tiers
                    .iter()
                    .any(|d| d.name == t && d.shared)
            })
        }) {
            return Err(OndaError::InvalidArgs(format!(
                "part {partition:?} has table id {} on a shared tier — \
                 shared publications cannot be detached or frozen",
                h.meta.id
            )));
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
            h.close(); // drop cached fds for the old paths
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
        // Whole keyspace: the incoming tables' extent is not known until the
        // directory has been read and validated, and the `bottom_overlaps`
        // placement decision below has to be stable against compaction. An
        // attach is a rare administrative operation, so this costs no steady-
        // state concurrency — and it is what `compact_mu` already did here.
        let _range = cf.range_locks.acquire_blocking(KeyRange::all());

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
        let default_storage = cf.tiers().storage_for(None);
        // (handle, at_bottom). Built and validated before anything is installed.
        let mut staged: Vec<(Arc<SstHandle>, bool)> = Vec::new();
        let mut staged_bottom: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        // dest paths copied so far, for cleanup on any rejection.
        let mut copied: Vec<String> = Vec::new();

        let result = (|| -> Result<()> {
            for src_klog in &klogs {
                let src_klog = src_klog.to_string_lossy().into_owned();
                let src_vlog = vlog_path_for(&src_klog);

                let new_id = self.inner.next_file_id();
                let dst_klog = cf.klog_path(new_id);
                let dst_vlog = vlog_path_for(&dst_klog);
                copied.push(dst_klog.clone());
                copy_into_storage(Path::new(&src_klog), &dst_klog, &default_storage)?;
                let has_vlog = Path::new(&src_vlog).exists();
                if has_vlog {
                    copied.push(dst_vlog.clone());
                    copy_into_storage(Path::new(&src_vlog), &dst_vlog, &default_storage)?;
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
                let at_bottom = !cf.bottom_overlaps(&min_key, &max_key)
                    && !staged_range_overlaps(cf, &staged_bottom, &min_key, &max_key);
                if at_bottom {
                    staged_bottom.push((min_key.clone(), max_key.clone()));
                }
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
                // A bottom part is partition-clean, so its tag is recoverable
                // from any key it holds — resolved through the CF's scheme so a
                // derived partitioner tags attached parts the same way
                // compaction would have. L0 is never partition-clean, so leave
                // None.
                meta.partition = if at_bottom {
                    cf.partition_resolver_snapshot()?.name_of(&meta.min_key)
                } else {
                    None
                };
                // The attached data is being brought in now; give it a defined
                // age so the mover treats it as freshly written (it must age
                // `min_age` again before qualifying for a tier move).
                meta.max_entry_time = Some(crate::util::now_nanos());
                // `last_compaction_time` is deliberately left None (0.3): this
                // table was written by ANOTHER database, whose compaction
                // history this one does not own, so its age here is unknown —
                // and unknown is never eligible for a periodic rewrite.
                debug_assert!(meta.last_compaction_time.is_none());
                staged.push((cf.handle_for(meta), at_bottom));
            }
            Ok(())
        })();

        if let Err(e) = result {
            // Roll back: nothing was installed, so unlink the copies we made.
            for p in &copied {
                let _ = default_storage.delete(p);
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

    /// Mount another database's part from a SHARED tier — by reference, zero
    /// bytes copied (A2, `SPADINO-A2.md`).
    ///
    /// Every table of `part` must carry an [`object`](PartTable::object) name
    /// (i.e. the part was published onto a shared tier); `tier` must name a
    /// tier declared [`shared`](crate::TierDef::shared) on THIS database whose
    /// root is the same location the exporter's tier pointed at. Each table is
    /// registered in the catalog under a fresh local id whose paths resolve to
    /// the shared objects; the block cache stays collision-free because it is
    /// keyed by the per-process local id, which is never reused.
    ///
    /// # Lineage
    ///
    /// Unlike [`attach_part`](Self::attach_part), a foreign sequence lineage
    /// is ACCEPTED: the database adopts a sequence floor past the tables'
    /// `max_seq` (the recovery path's own mechanism), making the mounted
    /// entries visible to every snapshot taken after the attach. This is
    /// sound for the intended topology — immutable, single-writer parts,
    /// read-only sharers — and that topology is a CONTRACT: mounting a part
    /// whose writer still appends to it is unsupported.
    ///
    /// # Validation
    ///
    /// Each table's footer, index and bloom are opened and CRC-verified
    /// through the tier's backend (bounded reads — for an S3 tier a handful
    /// of range GETs, never a download), and the reader's own
    /// `num_entries`/`max_seq`/key range are cross-checked against the
    /// manifest's claims; any mismatch rejects the whole part with nothing
    /// installed. Byte-level identity is NOT re-verified here — that is
    /// [`export_part`](Self::export_part)'s job, priced honestly.
    pub fn attach_part_by_ref(
        &self,
        cf: &Arc<ColumnFamily>,
        part: &PartManifest,
        tier: &str,
    ) -> Result<()> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.inner.poison.check()?;
        let is_shared = self
            .inner
            .opts
            .tiers
            .iter()
            .any(|t| t.name == tier && t.shared);
        if !is_shared {
            return Err(OndaError::InvalidArgs(format!(
                "attach_part_by_ref: tier {tier:?} is not declared shared \
                 (TierDef::shared) on this database"
            )));
        }
        if part.tables.is_empty() {
            return Err(OndaError::InvalidArgs("empty part manifest".into()));
        }
        if let Some(t) = part.tables.iter().find(|t| t.object.is_none()) {
            return Err(OndaError::InvalidArgs(format!(
                "attach_part_by_ref: table id {} carries no object name — the \
                 part was not published onto a shared tier",
                t.id
            )));
        }
        // Whole keyspace, for the same reason as `attach_part`.
        let _range = cf.range_locks.acquire_blocking(KeyRange::all());

        // Stage and validate every table before installing anything.
        let mut staged: Vec<(Arc<SstHandle>, bool)> = Vec::new();
        let mut staged_bottom: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for t in &part.tables {
            let new_id = self.inner.next_file_id();
            let mut meta = SstMeta {
                id: new_id,
                tier: Some(tier.to_string()),
                object: t.object.clone(),
                ..Default::default()
            };
            // Opens through the tier's backend and CRC-verifies footer,
            // index and bloom — the same validation every reader open does.
            let reader = cf.open_reader_for(&meta)?;
            if reader.num_entries() != t.num_entries
                || reader.max_seq() != t.max_seq
                || reader.min_key() != t.min_key.as_slice()
                || reader.max_key() != t.max_key.as_slice()
            {
                return Err(OndaError::InvalidArgs(format!(
                    "attach_part_by_ref: object {:?} disagrees with the part \
                     manifest (entries {} vs {}, max_seq {} vs {}) — wrong or \
                     stale manifest",
                    t.object.as_deref().unwrap_or(""),
                    reader.num_entries(),
                    t.num_entries,
                    reader.max_seq(),
                    t.max_seq,
                )));
            }
            let at_bottom = !cf.bottom_overlaps(&t.min_key, &t.max_key)
                && !staged_range_overlaps(cf, &staged_bottom, &t.min_key, &t.max_key);
            if at_bottom {
                staged_bottom.push((t.min_key.clone(), t.max_key.clone()));
            }
            meta.level = if at_bottom {
                cf.bottom_level_index() as u32
            } else {
                0
            };
            meta.num_entries = t.num_entries;
            meta.max_seq = t.max_seq;
            meta.klog_size = t.klog_size;
            meta.vlog_size = t.vlog_size;
            meta.min_key = t.min_key.clone();
            meta.max_key = t.max_key.clone();
            meta.partition = if at_bottom {
                cf.partition_resolver_snapshot()?.name_of(&meta.min_key)
            } else {
                None
            };
            // Freshly mounted: the mover must not immediately re-move it, and
            // a shared-tier part is never moved by a sharer anyway (the mover
            // skips off-default parts by the tier filter).
            meta.max_entry_time = Some(crate::util::now_nanos());
            // No periodic age state (0.3), for the same reason a foreign mount
            // has none: this database neither wrote nor may rewrite it.
            debug_assert!(meta.last_compaction_time.is_none());
            staged.push((cf.handle_for(meta), at_bottom));
        }

        // Adopt the foreign lineage BEFORE the tables become visible, so no
        // read can see an entry above the visible sequence.
        let max_seq = part.tables.iter().map(|t| t.max_seq).max().unwrap_or(0);
        self.inner.observe_seq(max_seq);

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

    /// Describe the bottom-level part for `partition` as a
    /// [`PartManifest`] — its tables, their key ranges, and a digest over
    /// their bytes.
    ///
    /// Unlike [`freeze_part`](Self::freeze_part), which produces an openable
    /// *database directory*, this produces a *description*: nothing is
    /// written, nothing is linked, and the caller gets an identity it can send
    /// somewhere else. Use it to name a part in an external catalog, or to
    /// check that two databases hold the same part without shipping either.
    ///
    /// Deletions are paused for the duration, exactly as `freeze_part` and
    /// `checkpoint` do, so a concurrent compaction cannot unlink a file
    /// half-way through hashing it.
    ///
    /// Errors with [`NotFound`](OndaError::NotFound) if the partition has no
    /// materialized bottom-level tables (flush + compact first).
    ///
    /// **Reads the whole part.** See [`PartManifest`] for why that is the
    /// point rather than an oversight.
    pub fn export_part(&self, cf: &Arc<ColumnFamily>, partition: &str) -> Result<PartManifest> {
        self.inner.poison.check()?;
        let _pause = self.inner.pause_deletions();

        let mut handles = cf.bottom_partition_handles(partition);
        if handles.is_empty() {
            return Err(OndaError::NotFound);
        }
        // A fixed order, derived from the data rather than from however the
        // level happened to be laid out, so the digest is reproducible.
        handles.sort_by(|a, b| {
            a.meta
                .min_key
                .cmp(&b.meta.min_key)
                .then(a.meta.id.cmp(&b.meta.id))
        });

        let mut tables = Vec::with_capacity(handles.len());
        for h in &handles {
            let klog = cf.klog_path_for(&h.meta);
            let vlog = vlog_path_for(&klog);
            // Through the tier's own backend, so a part that lives on a remote
            // tier exports without being brought local first.
            let storage = cf.ctx.tiers.storage_for(h.meta.tier.as_deref());
            let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
            hash_file(storage.as_ref(), &klog, h.meta.klog_size, &mut hasher)?;
            if h.meta.vlog_size > 0 {
                hash_file(storage.as_ref(), &vlog, h.meta.vlog_size, &mut hasher)?;
            }
            let content: [u8; 32] = <sha2::Sha256 as sha2::Digest>::finalize(hasher).into();
            tables.push(PartTable {
                id: h.meta.id,
                min_key: h.meta.min_key.clone(),
                max_key: h.meta.max_key.clone(),
                max_seq: h.meta.max_seq,
                num_entries: h.meta.num_entries,
                klog_size: h.meta.klog_size,
                vlog_size: h.meta.vlog_size,
                tier: h.meta.tier.clone(),
                object: h.meta.object.clone(),
                content,
            });
        }

        Ok(PartManifest {
            cf: cf.name().to_string(),
            partition: partition.to_string(),
            digest: part_digest(&tables),
            tables,
        })
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
        // A shared-tier table is an immutable publication other databases may
        // reference; unlinking or hard-linking it locally is either data loss
        // for a sharer or nonsense for a remote object (A2 — shared tiers are
        // delete-free and fs-free).
        if let Some(h) = handles.iter().find(|h| {
            h.meta.tier.as_deref().is_some_and(|t| {
                self.inner
                    .opts
                    .tiers
                    .iter()
                    .any(|d| d.name == t && d.shared)
            })
        }) {
            return Err(OndaError::InvalidArgs(format!(
                "part {partition:?} has table id {} on a shared tier — \
                 shared publications cannot be detached or frozen",
                h.meta.id
            )));
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
            // A frozen slice is a standalone database with no shared tiers;
            // it mints its own nonce if it ever configures one.
            instance_nonce: None,
            // A frozen slice carries whatever capabilities its source database
            // had durably enabled: the tables it references were written under
            // them.
            caps: self
                .inner
                .caps_durable
                .load(std::sync::atomic::Ordering::SeqCst),
            wal_layout: if self.inner.opts.unified_memtable {
                WalLayout::Unified
            } else {
                WalLayout::PerColumnFamily
            },
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
        // Scoped to this partition, so the background mover no longer stops
        // compaction across the whole column family while it copies.
        let _range = lock_partition_span(cf, partition);

        let handles = cf.bottom_partition_handles(partition);
        if handles.is_empty() {
            return Err(OndaError::NotFound);
        }
        // A retry may find all or part of the partition already on `tier`.
        // Reuse those handles in place: their source and destination paths are
        // identical, so passing them through `Storage::create` would truncate
        // live data. Moving only the remaining handles also heals a mixed-tier
        // part produced by a disjoint attach or partial bottom compaction.
        let shared_tier_names: Vec<String> = self
            .opts
            .tiers
            .iter()
            .filter(|t| t.shared)
            .map(|t| t.name.clone())
            .collect();
        let handles: Vec<_> = handles
            .into_iter()
            .filter(|handle| handle.meta.tier.as_deref() != Some(tier))
            // A table on a SHARED tier is an immutable publication: moving it
            // would delete a source object another database may reference
            // (A2 — shared tiers are delete-free). Re-placement is the layer
            // above's job, by publishing anew.
            .filter(|handle| {
                handle
                    .meta
                    .tier
                    .as_deref()
                    .is_none_or(|t| !shared_tier_names.iter().any(|s| s == t))
            })
            .collect();
        if handles.is_empty() {
            return Ok(());
        }

        // The destination backend may be local or remote (S3); route all writes
        // through it so the same mover protocol serves both — only the `Storage`
        // impl differs (a local copy+fsync vs. a buffered single-shot PUT).
        let dest_storage = cf.tiers().storage_for(Some(tier));
        let dest_cf_dir = cf.tiers().cf_dir(Some(tier), cf.name());
        dest_storage.ensure_dir(&dest_cf_dir)?;
        // A2: on a SHARED tier, objects are named by the per-database instance
        // nonce so two databases pointed at one root cannot collide. On a
        // non-shared tier the legacy id-derived path is kept byte-for-byte.
        let shared = self.opts.tiers.iter().any(|t| t.name == tier && t.shared);
        let nonce = if shared {
            let n = *self.instance_nonce.lock();
            Some(n.expect("a shared tier always mints the instance nonce at open"))
        } else {
            None
        };
        let object_for = |id: u64| -> Option<String> {
            nonce.map(|n| format!("cf-{}/{n:016x}-{id}", cf.name()))
        };

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
            let object = object_for(h.meta.id);
            let (dst_klog, dst_vlog) = match &object {
                Some(o) => {
                    let root = cf.tiers().root_for(Some(tier));
                    (format!("{root}/{o}.klog"), format!("{root}/{o}.vlog"))
                }
                None => (
                    format!("{dest_cf_dir}/{}.klog", h.meta.id),
                    format!("{dest_cf_dir}/{}.vlog", h.meta.id),
                ),
            };
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
            meta.object = object;
            new_handles.push(cf.handle_for(meta.clone()));
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
            h.close();
            let src_klog = cf.klog_path_for(&h.meta);
            let src_vlog = vlog_path_for(&src_klog);
            self.remove_sst_file(&src_klog, h.meta.klog_size);
            if Path::new(&src_vlog).exists() {
                self.remove_sst_file(&src_vlog, h.meta.vlog_size);
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
                let Some(target) = eligible_part_target(rules, &part, now) else {
                    continue;
                };
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

fn eligible_part_target<'a>(rules: &'a [TierRule], part: &BottomPart, now: i64) -> Option<&'a str> {
    let rule = crate::config::tier_for_key(rules, &part.min_key)?;
    // "ssd" denotes the default tier, and moving back to that tier has no copy
    // target in the P4 mover protocol.
    let target = (rule.tier != "ssd").then_some(rule.tier.as_str())?;
    if part.tier.as_deref() == Some(target) {
        return None;
    }
    let newest = part.max_entry_time?;
    (now.saturating_sub(newest) > rule.min_age.as_nanos() as i64).then_some(target)
}

#[cfg(test)]
mod attach_copy_tests {
    use std::io::Write;

    use super::copy_into_writer;
    use crate::error::OndaError;
    use crate::storage::StorageWriter;

    struct FailingFinishWriter(Vec<u8>);

    impl Write for FailingFinishWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl StorageWriter for FailingFinishWriter {
        fn finish(self: Box<Self>) -> crate::Result<()> {
            Err(std::io::Error::other("injected finish failure").into())
        }
    }

    #[test]
    fn attach_copy_propagates_storage_finish_failure() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.klog");
        std::fs::write(&src, b"sst bytes").unwrap();

        let err = copy_into_writer(&src, Box::new(FailingFinishWriter(Vec::new())))
            .expect_err("attach copy must not publish an unfinished object");
        assert!(matches!(err, OndaError::Io(_)));
    }
}

#[cfg(test)]
mod mover_policy_tests {
    use std::time::Duration;

    use super::eligible_part_target;
    use crate::column_family::BottomPart;
    use crate::config::TierRule;

    fn part(tier: Option<&str>, newest: Option<i64>) -> BottomPart {
        BottomPart {
            partition: "part".into(),
            min_key: b"logs/2026".to_vec(),
            tier: tier.map(str::to_owned),
            max_entry_time: newest,
        }
    }

    #[test]
    fn mover_policy_requires_a_named_different_and_old_enough_target() {
        let cold = TierRule {
            prefix: b"logs/".to_vec(),
            tier: "cold".into(),
            min_age: Duration::from_nanos(10),
        };
        let default = TierRule {
            tier: "ssd".into(),
            ..cold.clone()
        };

        assert_eq!(
            eligible_part_target(std::slice::from_ref(&cold), &part(None, Some(80)), 100),
            Some("cold")
        );
        assert_eq!(
            eligible_part_target(std::slice::from_ref(&cold), &part(None, Some(90)), 100),
            None
        );
        assert_eq!(
            eligible_part_target(std::slice::from_ref(&cold), &part(None, None), 100),
            None
        );
        assert_eq!(
            eligible_part_target(&[cold], &part(Some("cold"), Some(0)), 100),
            None
        );
        assert_eq!(
            eligible_part_target(&[default], &part(Some("cold"), Some(0)), 100),
            None
        );
    }
}

/// One materialized bottom-level partition, as reported by
/// [`DB::list_partitions`].
///
/// A partition appears here only once bottom compaction has cut a clean part on
/// its boundary. A rule or derived boundary that has been *configured* but whose
/// keys still live in upper (uncut) levels is not yet listed — which is exactly
/// what a consumer wants to verify: that its partitioner actually produced the
/// physical separation it intended, not merely that it was declared.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionInfo {
    /// The partition name — the same string [`detach_part`](DB::detach_part),
    /// [`freeze_part`](DB::freeze_part) and
    /// [`move_part_to_tier`](DB::move_part_to_tier) address.
    pub partition: String,
    /// The smallest user key currently in the part.
    pub min_key: Vec<u8>,
    /// The storage tier the part lives on, or `None` for the default tier.
    pub tier: Option<String>,
}

impl DB {
    /// List the materialized bottom-level partitions of `cf`, read-only.
    ///
    /// This is how a consumer confirms its partitioner — rule-based or derived —
    /// actually cut the parts it intended: that no bottom part spans two of its
    /// logical partitions, and that the boundaries it expects exist. Without it
    /// a consumer can declare a `PartitionScheme` but has no way to check the
    /// result, which for a correctness-driven partitioner (a time bucket that
    /// must be independently droppable) is the property that actually matters.
    ///
    /// Only **bottom-level** parts are listed, because only bottom compaction
    /// cuts on partition boundaries; keys still in upper levels are not yet
    /// partition-clean and are deliberately excluded. A part that straddles
    /// tiers mid-move is omitted until the move settles, matching what the mover
    /// itself sees. Ordered by partition name for a stable listing.
    #[must_use]
    pub fn list_partitions(&self, cf: &Arc<ColumnFamily>) -> Vec<PartitionInfo> {
        let mut out: Vec<PartitionInfo> = cf
            .bottom_parts()
            .into_iter()
            .map(|p| PartitionInfo {
                partition: p.partition,
                min_key: p.min_key,
                tier: p.tier,
            })
            .collect();
        out.sort_by(|a, b| a.partition.cmp(&b.partition));
        out
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

fn copy_into_storage(
    from: &Path,
    to: &str,
    storage: &Arc<dyn crate::storage::Storage>,
) -> Result<()> {
    copy_into_writer(from, storage.create(to)?)
}

fn copy_into_writer(from: &Path, mut writer: Box<dyn crate::storage::StorageWriter>) -> Result<()> {
    let mut src = std::fs::File::open(from)?;
    std::io::copy(&mut src, &mut *writer)?;
    writer.finish()
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

/// Chunk size for [`hash_file`]. Large enough that a remote tier issues few
/// range GETs, small enough that hashing a multi-gigabyte part does not
/// allocate a multi-gigabyte buffer.
const HASH_CHUNK: usize = 1 << 20;

/// Feed `len` bytes of `path` into `hasher`, reading through `storage` so a
/// tier-resident file is hashed where it lives rather than being copied local.
fn hash_file(
    storage: &dyn crate::storage::Storage,
    path: &str,
    len: u64,
    hasher: &mut sha2::Sha256,
) -> Result<()> {
    use sha2::Digest as _;
    let handle = storage.open_read(path)?;
    let mut buf = vec![0u8; HASH_CHUNK.min(len.max(1) as usize)];
    let mut off = 0u64;
    while off < len {
        let n = HASH_CHUNK.min((len - off) as usize);
        let chunk = &mut buf[..n];
        handle.read_exact_at(chunk, off)?;
        hasher.update(&*chunk);
        off += n as u64;
    }
    Ok(())
}

/// SHA-256 over a canonical encoding of a part's tables.
///
/// Length-prefixed, fixed field order, big-endian lengths: two different table
/// lists cannot produce the same byte stream by shifting a boundary, which is
/// the classic way a naive concatenation becomes forgeable. The file **id** is
/// deliberately excluded — it is local to the exporting database, and
/// including it would make the same bytes hash differently on two machines,
/// defeating the entire purpose.
fn part_digest(tables: &[PartTable]) -> [u8; 32] {
    use sha2::Digest as _;
    let mut h = sha2::Sha256::new();
    h.update(b"ondadb/part/v1");
    h.update((tables.len() as u64).to_be_bytes());
    for t in tables {
        h.update(t.content);
        h.update(t.max_seq.to_be_bytes());
        h.update(t.num_entries.to_be_bytes());
        h.update(t.klog_size.to_be_bytes());
        h.update(t.vlog_size.to_be_bytes());
        h.update((t.min_key.len() as u64).to_be_bytes());
        h.update(&t.min_key);
        h.update((t.max_key.len() as u64).to_be_bytes());
        h.update(&t.max_key);
    }
    h.finalize().into()
}

#[cfg(test)]
mod derived_partition_tests {
    //! Structural properties of derived partitioning (A5).
    //!
    //! These live in-crate because they assert on `ColumnFamily::bottom_parts`,
    //! which is crate-private: the properties under test are about how
    //! compaction *cuts* files, which the public API deliberately does not
    //! expose.

    use std::sync::Arc;
    use std::time::Duration;

    use crate::{ColumnFamilyConfig, Options, PartitionFn, PartitionScheme, DB};

    /// Keys are `<name>\0\0<8-byte bucket><rest>`; a partition is one
    /// `(name, bucket)` pair — the shape this feature was asked for.
    #[derive(Debug)]
    struct NameAndBucket;

    impl NameAndBucket {
        fn key(name: &str, bucket: u64, rest: &str) -> Vec<u8> {
            let mut k = Vec::new();
            k.extend_from_slice(name.as_bytes());
            k.extend_from_slice(&[0, 0]);
            k.extend_from_slice(&bucket.to_be_bytes());
            k.extend_from_slice(rest.as_bytes());
            k
        }
    }

    impl PartitionFn for NameAndBucket {
        fn boundary_len(&self, key: &[u8]) -> usize {
            match key.windows(2).position(|w| w == [0, 0]) {
                Some(end) => (end + 2 + 8).min(key.len()),
                None => key.len(),
            }
        }
        fn name(&self, key: &[u8]) -> String {
            key[..self.boundary_len(key)]
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect()
        }
        fn scheme_name(&self) -> &str {
            "test.name-and-bucket.v1"
        }
    }

    fn open(dir: &tempfile::TempDir) -> DB {
        let mut o = Options::new(dir.path().to_str().unwrap());
        o.partition_fns = vec![Arc::new(NameAndBucket)];
        DB::open(o).unwrap()
    }

    fn derived_cfg() -> ColumnFamilyConfig {
        ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Derived(Arc::new(NameAndBucket)),
            l1_file_count_trigger: 1,
            ..ColumnFamilyConfig::default()
        }
    }

    /// Bottom compaction cuts a new file whenever the derived boundary changes.
    #[test]
    fn boundary_changes_cut_parts() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let cf = db.create_column_family("default", derived_cfg()).unwrap();

        for tenant in ["alpha", "beta", "gamma"] {
            for bucket in [1u64, 2] {
                for i in 0..4u32 {
                    db.put(
                        &cf,
                        &NameAndBucket::key(tenant, bucket, &format!("{i:03}")),
                        b"v",
                        Duration::ZERO,
                    )
                    .unwrap();
                }
            }
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();

        assert_eq!(
            cf.bottom_parts().len(),
            6,
            "each (tenant, bucket) pair is its own part"
        );
    }

    /// Every bottom part is a contiguous key range belonging to one partition.
    ///
    /// This is the property detach/attach, freeze and tiering all rest on: a
    /// part that spanned a boundary would move data belonging to another
    /// partition along with it.
    #[test]
    fn each_part_is_a_contiguous_key_range() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let cf = db.create_column_family("default", derived_cfg()).unwrap();

        for tenant in ["a", "b", "c"] {
            for bucket in [7u64, 9] {
                for i in 0..6u32 {
                    db.put(
                        &cf,
                        &NameAndBucket::key(tenant, bucket, &format!("{i:03}")),
                        b"v",
                        Duration::ZERO,
                    )
                    .unwrap();
                }
            }
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();

        let f = NameAndBucket;
        let parts = cf.bottom_parts();
        assert_eq!(parts.len(), 6);
        for p in &parts {
            // Check every SSTable in the part, not just the aggregate: a file
            // whose min and max keys resolve to different partitions has
            // spanned a boundary, which is exactly what must not happen.
            for h in cf.bottom_partition_handles(&p.partition) {
                assert_eq!(
                    f.name(&h.meta.min_key),
                    p.partition,
                    "min_key outside its part"
                );
                assert_eq!(
                    f.name(&h.meta.max_key),
                    p.partition,
                    "max_key outside its part — the file spans a boundary"
                );
            }
        }
    }

    /// Partition count is not bounded by the durable rule-vector ceiling.
    ///
    /// This is the whole reason the feature exists: 300 partitions is already
    /// past the 255-entry limit that constrains enumerated rules.
    #[test]
    fn partition_count_is_unbounded() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let cf = db.create_column_family("default", derived_cfg()).unwrap();

        for t in 0..300u32 {
            db.put(
                &cf,
                &NameAndBucket::key(&format!("t{t:04}"), 1, "x"),
                b"v",
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();

        assert_eq!(
            cf.bottom_parts().len(),
            300,
            "derived partitioning must not inherit the 255-rule ceiling"
        );
    }

    /// A column family whose scheme could not be resolved refuses to compact
    /// rather than silently cutting on rule boundaries.
    #[test]
    fn an_unresolved_scheme_refuses_to_partition() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cfg = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("test.absent.v1".into()),
            l1_file_count_trigger: 1,
            ..ColumnFamilyConfig::default()
        };
        let cf = db.create_column_family("default", cfg).unwrap();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();

        let err = db.compact(&cf).expect_err("must not partition blindly");
        assert!(format!("{err:?}").contains("test.absent.v1"));
    }

    /// A `PartitionFn` that violates the prefix-determined contract: the key
    /// `xy` gets a 2-byte boundary while every other key under the `x` prefix
    /// gets 1, so the boundary `x` is finalized when we cross into `xy` and then
    /// reappears at `xz`. A correct (prefix-determined) partitioner cannot do
    /// this under sorted keys; this one is deliberately wrong.
    #[derive(Debug)]
    struct NonPrefixDetermined;

    impl PartitionFn for NonPrefixDetermined {
        fn boundary_len(&self, key: &[u8]) -> usize {
            if key == b"xy" {
                2
            } else {
                1.min(key.len())
            }
        }
        fn name(&self, key: &[u8]) -> String {
            key[..self.boundary_len(key)]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        }
        fn scheme_name(&self) -> &str {
            "test.non-prefix-determined.v1"
        }
    }

    /// The debug-only guard (Q2) catches a partitioner that reopens a finalized
    /// boundary — the misimplementation that would silently produce a bottom
    /// SSTable spanning two partitions. Release builds do not pay for this, so
    /// the test only asserts the behaviour under `debug_assertions`.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "not order-compatible")]
    fn a_non_order_compatible_partitioner_is_caught_in_debug() {
        let dir = tempfile::tempdir().unwrap();
        let mut o = Options::new(dir.path().to_str().unwrap());
        o.partition_fns = vec![Arc::new(NonPrefixDetermined)];
        let db = DB::open(o).unwrap();
        let cfg = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Derived(Arc::new(NonPrefixDetermined)),
            l1_file_count_trigger: 1,
            ..ColumnFamilyConfig::default()
        };
        let cf = db.create_column_family("default", cfg).unwrap();
        // Sorted order is x < xy < xz; xy's odd 2-byte boundary makes `x`
        // reappear at xz.
        for k in [b"x".as_slice(), b"xy".as_slice(), b"xz".as_slice()] {
            db.put(&cf, k, b"v", Duration::ZERO).unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        let _ = db.compact(&cf);
    }
}
