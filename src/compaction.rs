//! Leveled compaction.
//!
//! L0 is compacted into L1 when its file count reaches `l1_file_count_trigger`;
//! a level `i >= 1` is compacted into `i+1` when its byte size exceeds the
//! level's capacity (`write_buffer_size * level_size_ratio^(i-1)`).  Inputs are
//! merge-iterated in internal order; for each user key the newest version is
//! kept, plus every version newer than the oldest live snapshot, and tombstones
//! are dropped once they reach the bottom level.
//!
//! This is standard leveled compaction; the C
//! engine's three-mode "Spooky" merge is a future refinement.

use std::sync::Arc;

use crate::column_family::{ColumnFamily, SstHandle};
use crate::comparator::ComparatorRef;
use crate::db::DbInner;
use crate::error::Result;
use crate::manifest::SstMeta;
use crate::sst::{SstIterator, Writer};
use crate::util::now_nanos;

/// Manual compaction (`DB::compact`): run the triggered rounds, then sweep
/// every populated level down to the bottom once. The sweep is what lets an
/// explicit compact() reclaim tombstone debris from a quiescent CF — a fully
/// deleted CF sits below every size trigger, so `run` alone would keep its
/// tombstones forever (a comparable engine kept 23 MB of tombstones in an
/// "empty" partition this way). Background workers keep using `run`; only
/// the user-invoked path pays for the full sweep.
pub(crate) fn run_manual(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<()> {
    run(db, cf)?;
    if cf.opts.compaction_style == crate::config::CompactionStyle::Fifo {
        return Ok(()); // FIFO never merges; eviction already ran above
    }
    let _mu = cf.compact_mu.lock();
    cf.compacting
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let res = (|| {
        let n = cf.with_levels(|levels| levels.len());
        for level in 0..n.saturating_sub(1) {
            compact_level(db, cf, level)?;
            cf.compaction_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // Rewrite the last level in place. Push-down merges only rewrite
        // bottom tables that overlap incoming data, so a bottom table that
        // never overlaps anything again would otherwise keep its tombstones
        // and never see the compaction filter, no matter how often compact()
        // is called.
        let last = cf.with_levels(|levels| levels.len()).saturating_sub(1);
        compact_into(db, cf, last, last)?;
        cf.compaction_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    })();
    cf.compacting
        .store(false, std::sync::atomic::Ordering::Relaxed);
    res
}

/// Run compaction on `cf` until no level is over its trigger.
pub(crate) fn run(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<()> {
    if cf.opts.compaction_style == crate::config::CompactionStyle::Fifo {
        return run_fifo(db, cf);
    }
    let _mu = cf.compact_mu.lock();
    cf.compacting
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let res = (|| {
        while let Some(level) = pick_level(db, cf) {
            compact_level(db, cf, level)?;
            cf.compaction_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    })();
    cf.compacting
        .store(false, std::sync::atomic::Ordering::Relaxed);
    res
}

/// FIFO "compaction": never merges — evicts the oldest L0 tables past the
/// CF's size/age limits. Manifest is persisted before any file is unlinked
/// (the same ordering the merge path uses).
fn run_fifo(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<()> {
    let _mu = cf.compact_mu.lock();
    cf.compacting
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let res = (|| {
        let victims = cf.take_fifo_victims(cf.opts.fifo_max_bytes, cf.opts.fifo_ttl);
        if victims.is_empty() {
            return Ok(());
        }
        db.persist_manifest()?;
        for t in &victims {
            db.remove_sst_file(&cf.klog_path(t.meta.id));
            db.remove_sst_file(&format!("{}/{}.vlog", cf.dir(), t.meta.id));
        }
        cf.compaction_count
            .fetch_add(victims.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    })();
    cf.compacting
        .store(false, std::sync::atomic::Ordering::Relaxed);
    res
}

/// A table mounted from another database's publication
/// (`attach_part_by_ref`): its shared-tier object name carries a FOREIGN
/// instance nonce. Such tables are read-only mounts — the sharer must never
/// rewrite bytes it did not publish (`SPADINO-A2.md`'s safety argument; the
/// original "and cannot" claim missed local re-materialization by
/// background compaction, which silently rebuilt whole mounted parts as
/// local tables). They are excluded from compaction triggers and inputs.
fn is_foreign_mount(db: &DbInner, meta: &crate::manifest::SstMeta) -> bool {
    let Some(object) = &meta.object else {
        return false;
    };
    // Object names are `cf-{cf}/{nonce:016x}-{id}.klog`; the nonce is the
    // 16-hex prefix of the final path component.
    let stem = object.rsplit('/').next().unwrap_or(object);
    let Some((hex, _)) = stem.split_once('-') else {
        return true; // unparseable foreign-shaped name: treat as mounted
    };
    let Ok(nonce) = u64::from_str_radix(hex, 16) else {
        return true;
    };
    match *db.instance_nonce.lock() {
        Some(own) => nonce != own,
        // No nonce minted: this database never published to a shared tier,
        // so ANY object-named table was mounted from elsewhere.
        None => true,
    }
}

/// Choose a level to compact, or `None` if nothing is triggered.
///
/// Foreign mounts are invisible to the triggers: a level full of mounted
/// tables must not re-trigger a compaction that would then exclude them all
/// (a busy loop), and their bytes are another publisher's, not this
/// database's write debt.
fn pick_level(db: &DbInner, cf: &Arc<ColumnFamily>) -> Option<usize> {
    let trigger = cf.opts.l1_file_count_trigger as usize;
    let ratio = cf.opts.level_size_ratio.max(2);
    let wbs = cf.opts.write_buffer_size as u64;
    cf.with_levels(|levels| {
        if levels[0]
            .iter()
            .filter(|t| !is_foreign_mount(db, &t.meta))
            .count()
            >= trigger
        {
            return Some(0);
        }
        for (i, lvl) in levels.iter().enumerate().skip(1) {
            let bytes: u64 = lvl
                .iter()
                .filter(|t| !is_foreign_mount(db, &t.meta))
                .map(|t| t.meta.klog_size + t.meta.vlog_size)
                .sum();
            let cap = wbs.saturating_mul(ratio.saturating_pow(i as u32 - 1));
            if bytes > cap {
                return Some(i);
            }
        }
        None
    })
}

/// Compact every table in `level` plus overlapping tables in `level+1` into
/// `level+1`.
fn compact_level(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>, level: usize) -> Result<()> {
    compact_into(db, cf, level, level + 1)
}

/// Compact `level` into `target` (either `level + 1`, or `level` itself for
/// the in-place bottom rewrite manual compaction does — the only way tables
/// in the last level that overlap no incoming data ever see the compaction
/// filter or drop their tombstones again).
fn compact_into(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    level: usize,
    target: usize,
) -> Result<()> {
    let cmp = cf.cmp();
    debug_assert!(target == level || target == level + 1);

    // Snapshot the input handles. An in-place rewrite takes the whole level;
    // a push-down merges the level with overlapping tables below it.
    // `retained_next` is deliberately NOT captured here: it would be a snapshot
    // of the target level taken before the compaction ran, and re-installing it
    // afterwards would drop any table added meanwhile. It is re-derived from
    // live state inside `update_levels` below, as `levels[target]` minus the
    // inputs — which is the same set, plus concurrent arrivals.
    let (inputs, num_levels): (Vec<Arc<SstHandle>>, usize) = cf.with_levels(|levels| {
        // Foreign mounts (attach_part_by_ref) never compact: not as the
        // level's own inputs, and a push-down that would overlap one in the
        // target is skipped whole — merging around a read-only mount would
        // leave overlapping tables in one level.
        let mut inputs: Vec<Arc<SstHandle>> = levels[level]
            .iter()
            .filter(|t| !is_foreign_mount(db, &t.meta))
            .cloned()
            .collect();
        if inputs.is_empty() {
            return (Vec::new(), levels.len());
        }
        let (min_key, max_key) = key_span(&inputs, &cmp);
        if target != level && target < levels.len() {
            for th in &levels[target] {
                if ranges_overlap(&cmp, &th.meta.min_key, &th.meta.max_key, &min_key, &max_key) {
                    if is_foreign_mount(db, &th.meta) {
                        return (Vec::new(), levels.len());
                    }
                    inputs.push(th.clone());
                }
            }
        }
        (inputs, levels.len())
    });

    if inputs.is_empty() {
        return Ok(());
    }

    let bottom = target >= num_levels - 1 && {
        // bottom only if no level beyond target holds data
        cf.with_levels(|levels| levels.iter().skip(target + 1).all(|l| l.is_empty()))
    };
    let oldest_snapshot = db.oldest_snapshot();
    let now = now_nanos();
    let filter = cf.compaction_filter();
    // Snapshot the partition rules once for the whole run. A rule added
    // concurrently (via `DB::add_partition_rule`) must not change this run's cut
    // boundaries — it takes effect on the next bottom compaction. Only bottom
    // output is cut on partitions, so upper-level runs need no snapshot.
    // Snapshotted once per run for both schemes; see
    // `ColumnFamily::partition_resolver_snapshot`.
    let partitioner = if bottom {
        Some(cf.partition_resolver_snapshot()?)
    } else {
        None
    };

    // Merge-iterate all inputs and write new output SSTables.
    let mut its: Vec<SstIterator> = inputs
        .iter()
        .map(|t| t.reader().map(|r| r.iter()))
        .collect::<Result<_>>()?;
    for it in its.iter_mut() {
        it.seek_to_first();
    }

    let target_bytes = (cf.opts.write_buffer_size as u64).max(1);
    let mut outputs: Vec<SstMeta> = Vec::new();
    // (writer, klog, id, bytes, partition). `partition` is the partition every
    // key in the current output file belongs to — only meaningful at the bottom
    // level, where output is cut on partition boundaries; `None` elsewhere.
    let mut writer: Option<(Writer, String, u64, u64, Option<String>)> = None;

    // Age carried onto every output: the maximum `max_entry_time` over the
    // inputs. Carrying it forward (rather than stamping "now") means compaction
    // rewriting cold data does not reset its age, so a bottom part keeps
    // qualifying for a tier move; an input that predates timestamps contributes
    // nothing, and if no input has one the output's age stays unknown (`None`).
    let carry_entry_time: Option<i64> = inputs.iter().filter_map(|h| h.meta.max_entry_time).max();

    // Finish `writer`, stamping the accumulated partition and carried age onto
    // its manifest record, and push it to `outputs`.
    let finish_output = |writer: &mut Option<(Writer, String, u64, u64, Option<String>)>,
                         outputs: &mut Vec<SstMeta>|
     -> Result<()> {
        if let Some((wr, _klog, id, _bytes, part)) = writer.take() {
            let mut meta = wr.finish()?.to_sst_meta(id, target as u32);
            meta.partition = part;
            meta.max_entry_time = carry_entry_time;
            outputs.push(meta);
        }
        Ok(())
    };

    let mut last_key: Option<Vec<u8>> = None;
    let mut emitted_le_for_key = false;
    // Boundary bytes of the key currently being written, so a change can be
    // detected without re-resolving the previous key.
    let mut last_boundary: Option<Vec<u8>> = None;
    // Debug-only guard against a misimplemented consumer `PartitionFn`. The
    // documented contract is that boundaries are prefix-determined and
    // order-compatible: keys arrive in ascending user-key order, so once
    // compaction leaves a boundary it must never see it again. A partitioner
    // that violates this reopens a finalized part, producing a bottom SSTable
    // that spans two partitions — precisely the corruption partitioning exists
    // to prevent, and one every operation would report as success. Cheap to
    // catch here, invisible in release builds.
    #[cfg(debug_assertions)]
    let mut finalized_boundaries: std::collections::HashSet<Vec<u8>> =
        std::collections::HashSet::new();

    loop {
        // pick the smallest (user_key asc, seq desc) across iterators
        let mut best: Option<usize> = None;
        for (i, it) in its.iter().enumerate() {
            if !it.valid() {
                continue;
            }
            match best {
                None => best = Some(i),
                Some(b) => {
                    let bi = &its[b];
                    let ord = cmp
                        .compare(it.user_key(), bi.user_key())
                        .then_with(|| bi.seq().cmp(&it.seq()));
                    if ord.is_lt() {
                        best = Some(i);
                    }
                }
            }
        }
        let Some(bi) = best else { break };

        let (uk, seq, tomb, ttl) = {
            let it = &its[bi];
            (
                it.user_key().to_vec(),
                it.seq(),
                it.is_tombstone(),
                it.ttl(),
            )
        };

        // Per-key version-collapse decision.
        let new_key = last_key.as_deref() != Some(uk.as_slice());
        if new_key {
            last_key = Some(uk.clone());
            emitted_le_for_key = false;
        }
        let mut keep = true;
        if seq > oldest_snapshot {
            keep = true; // a snapshot above may need this version
        } else if !emitted_le_for_key {
            emitted_le_for_key = true;
            if tomb && bottom {
                keep = false; // tombstone with nothing below: drop the key
            }
        } else {
            keep = false; // older than the version visible to the oldest snapshot
        }
        // Expired entries can also be dropped at the bottom.
        if keep && bottom && ttl != 0 && ttl <= now && !tomb {
            keep = false;
        }

        if keep {
            let value = its[bi].value()?;
            // Compaction filter: only the newest surviving non-tombstone
            // version at or below the oldest snapshot is eligible (newer
            // versions stay protected; older ones were dropped above).
            let mut write_tomb = tomb;
            if !tomb && seq <= oldest_snapshot && (ttl == 0 || ttl > now) {
                if let Some(f) = &filter {
                    if f(&uk, &value) == crate::column_family::FilterDecision::Remove {
                        if bottom {
                            keep = false; // nothing below can resurface
                        } else {
                            // Emit a tombstone so versions in lower levels
                            // stay shadowed until they compact away.
                            write_tomb = true;
                        }
                    }
                }
            }
            if keep {
                // Bottom-level output is cut at partition boundaries so no
                // bottom SSTable spans two partitions. Keys arrive in ascending
                // user-key order, so a change in `partition_of` means we have
                // crossed into a different partition: finish the current file
                // (stamped with its partition) before opening the next. Upper
                // levels leave `part = None`, so this never cuts there.
                let part = match &partitioner {
                    Some(p) => p.name_of(&uk),
                    None => None,
                };
                // Cut on a change in the *boundary bytes*, not the name. For
                // rules the two are equivalent (a name is a function of the
                // matched prefix). For a derived scheme the boundary is the
                // stronger test: it keeps a part a contiguous key range even
                // if an implementation's `name` collides across boundaries,
                // which is what detach/attach, freeze and tiering rely on.
                if let Some((_, _, _, _, cur)) = writer.as_ref() {
                    let crossed = match (&partitioner, last_boundary.as_deref()) {
                        (Some(p), Some(prev)) => p.boundary(&uk) != Some(prev),
                        (Some(p), None) => p.boundary(&uk).is_some(),
                        (None, _) => false,
                    };
                    if crossed || *cur != part {
                        // The boundary we are leaving is now sealed into a part.
                        // Re-entering it later would mean the partitioner is not
                        // order-compatible (see `finalized_boundaries`).
                        #[cfg(debug_assertions)]
                        if crossed {
                            if let Some(prev) = &last_boundary {
                                finalized_boundaries.insert(prev.clone());
                            }
                            if let Some(next) = partitioner.as_ref().and_then(|p| p.boundary(&uk)) {
                                debug_assert!(
                                    !finalized_boundaries.contains(next),
                                    "PartitionFn is not order-compatible: boundary {next:?} \
                                     reappeared after its part was finalized, which would make a \
                                     bottom SSTable span two partitions"
                                );
                            }
                        }
                        finish_output(&mut writer, &mut outputs)?;
                    }
                }
                last_boundary = partitioner
                    .as_ref()
                    .and_then(|p| p.boundary(&uk))
                    .map(<[u8]>::to_vec);
                if writer.is_none() {
                    let id = db.next_file_id();
                    let klog = cf.klog_path(id);
                    let w = Writer::new(&klog, cf_writer_opts(cf, &cmp, target as u32))?;
                    writer = Some((w, klog, id, 0, part));
                }
                let w = writer.as_mut().unwrap();
                w.0.add(
                    &uk,
                    &value,
                    seq,
                    ttl,
                    write_tomb,
                    its[bi].is_single_delete(),
                )?;
                w.3 += (uk.len() + value.len()) as u64;
                if w.3 >= target_bytes {
                    finish_output(&mut writer, &mut outputs)?;
                }
            }
        }

        its[bi].next();
    }
    finish_output(&mut writer, &mut outputs)?;

    // Handles for the new tables. Readers are NOT opened here: compaction
    // output is often not read for a while, and opening it would put its index
    // and bloom in memory on the writer's behalf.
    let mut new_handles = Vec::new();
    for meta in &outputs {
        new_handles.push(cf.handle_for(meta.clone()));
    }

    // Build the new level set.
    let input_ids: std::collections::HashSet<u64> = inputs.iter().map(|t| t.meta.id).collect();
    cf.update_levels(|levels| {
        let mut out: Vec<Vec<Arc<SstHandle>>> = Vec::new();
        let needed = (target + 1).max(levels.len());
        for i in 0..needed {
            if i == level {
                // Drop all inputs that came from this level. (Tables added
                // concurrently — e.g. a flush landing in L0 — are kept.)
                let mut kept: Vec<Arc<SstHandle>> = levels
                    .get(i)
                    .map(|l| {
                        l.iter()
                            .filter(|t| !input_ids.contains(&t.meta.id))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                if level == target {
                    // In-place rewrite: the outputs replace the inputs here.
                    kept.extend(new_handles.iter().cloned());
                    kept.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
                }
                out.push(kept);
            } else if i == target {
                // Live state minus the inputs — never a pre-compaction
                // snapshot, so a table that arrived while this compaction ran
                // survives instead of being overwritten.
                let mut lvl: Vec<Arc<SstHandle>> = levels
                    .get(i)
                    .map(|l| {
                        l.iter()
                            .filter(|t| !input_ids.contains(&t.meta.id))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                lvl.extend(new_handles.iter().cloned());
                lvl.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
                out.push(lvl);
            } else {
                out.push(levels.get(i).cloned().unwrap_or_default());
            }
        }
        // Silent loss is the failure mode this rebuild had, so make it loud:
        // every table present before must either be a compaction input or
        // still be here. Debug-only — it is O(tables) and the invariant is
        // structural, not data-dependent.
        debug_assert!(
            {
                let before: std::collections::HashSet<u64> =
                    levels.iter().flatten().map(|t| t.meta.id).collect();
                let after: std::collections::HashSet<u64> =
                    out.iter().flatten().map(|t| t.meta.id).collect();
                before.difference(&after).all(|id| input_ids.contains(id))
            },
            "compaction dropped a table that was not one of its inputs — that \
             is committed data becoming unreachable (level={level} target={target})"
        );
        out
    });

    // Persist the manifest before deleting old files.
    db.persist_manifest()?;

    // Delete and evict the obsolete input files (deferred if a checkpoint/backup
    // has paused deletions, so it can copy a consistent file set).
    for th in &inputs {
        th.close();
        let klog = cf.klog_path(th.meta.id);
        let vlog = format!("{}/{}.vlog", cf.dir(), th.meta.id);
        db.remove_sst_file(&klog);
        db.remove_sst_file(&vlog);
    }
    Ok(())
}

fn cf_writer_opts(
    cf: &Arc<ColumnFamily>,
    cmp: &ComparatorRef,
    target_level: u32,
) -> crate::sst::WriterOptions {
    crate::sst::WriterOptions {
        compression: cf.opts.compression_for_level(target_level),
        compression_rules: cf.opts.compression_rules.clone(),
        cmp: cmp.clone(),
        enable_bloom: cf.opts.enable_bloom_filter,
        bloom_fpr: cf.opts.bloom_fpr,
        klog_value_threshold: cf.opts.klog_value_threshold,
        block_size: 4 << 10,
        // Capacity hint for the writer's bloom-hash buffer ONLY. It used to
        // size the filter itself, which is why every compacted table carried a
        // filter built for 4,096 keys while holding a million — saturated, and
        // skipping nothing. The writer now sizes the filter from the keys it
        // actually wrote (`tests/bloom_survives_compaction.rs`), so a wrong
        // hint costs a few reallocs and nothing else.
        expected_entries: 4096,
        use_btree: cf.opts.use_btree,
        restart_interval: crate::sst::RESTART_INTERVAL,
    }
}

fn key_span(tables: &[Arc<SstHandle>], cmp: &ComparatorRef) -> (Vec<u8>, Vec<u8>) {
    let mut min: Option<&[u8]> = None;
    let mut max: Option<&[u8]> = None;
    for t in tables {
        min = Some(match min {
            Some(m) if cmp.compare(m, &t.meta.min_key).is_le() => m,
            _ => &t.meta.min_key,
        });
        max = Some(match max {
            Some(m) if cmp.compare(m, &t.meta.max_key).is_ge() => m,
            _ => &t.meta.max_key,
        });
    }
    (
        min.map(|s| s.to_vec()).unwrap_or_default(),
        max.map(|s| s.to_vec()).unwrap_or_default(),
    )
}

fn ranges_overlap(cmp: &ComparatorRef, amin: &[u8], amax: &[u8], bmin: &[u8], bmax: &[u8]) -> bool {
    cmp.compare(amin, bmax).is_le() && cmp.compare(bmin, amax).is_le()
}
