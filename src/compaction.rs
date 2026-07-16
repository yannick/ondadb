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
use crate::sst::{Reader, SstIterator, Writer};
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
        while let Some(level) = pick_level(cf) {
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

/// Choose a level to compact, or `None` if nothing is triggered.
fn pick_level(cf: &Arc<ColumnFamily>) -> Option<usize> {
    let trigger = cf.opts.l1_file_count_trigger as usize;
    let ratio = cf.opts.level_size_ratio.max(2);
    let wbs = cf.opts.write_buffer_size as u64;
    cf.with_levels(|levels| {
        if levels[0].len() >= trigger {
            return Some(0);
        }
        for (i, lvl) in levels.iter().enumerate().skip(1) {
            let bytes: u64 = lvl
                .iter()
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
fn compact_into(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>, level: usize, target: usize) -> Result<()> {
    let cmp = cf.cmp();
    debug_assert!(target == level || target == level + 1);

    // Snapshot the input handles. An in-place rewrite takes the whole level;
    // a push-down merges the level with overlapping tables below it.
    let (inputs, retained_next, num_levels): (Vec<Arc<SstHandle>>, Vec<Arc<SstHandle>>, usize) = cf
        .with_levels(|levels| {
            let mut inputs: Vec<Arc<SstHandle>> = levels[level].clone();
            let (min_key, max_key) = key_span(&inputs, &cmp);
            let mut retained_next = Vec::new();
            if target != level && target < levels.len() {
                for th in &levels[target] {
                    if ranges_overlap(&cmp, &th.meta.min_key, &th.meta.max_key, &min_key, &max_key)
                    {
                        inputs.push(th.clone());
                    } else {
                        retained_next.push(th.clone());
                    }
                }
            }
            (inputs, retained_next, levels.len())
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

    // Merge-iterate all inputs and write new output SSTables.
    let mut its: Vec<SstIterator> = inputs.iter().map(|t| t.reader.iter()).collect();
    for it in its.iter_mut() {
        it.seek_to_first();
    }

    let target_bytes = (cf.opts.write_buffer_size as u64).max(1);
    let mut outputs: Vec<SstMeta> = Vec::new();
    // (writer, klog, id, bytes, partition). `partition` is the partition every
    // key in the current output file belongs to — only meaningful at the bottom
    // level, where output is cut on partition boundaries; `None` elsewhere.
    let mut writer: Option<(Writer, String, u64, u64, Option<String>)> = None;

    // Finish `writer`, stamping the accumulated partition onto its manifest
    // record, and push it to `outputs`.
    let finish_output = |writer: &mut Option<(Writer, String, u64, u64, Option<String>)>,
                         outputs: &mut Vec<SstMeta>|
     -> Result<()> {
        if let Some((wr, _klog, id, _bytes, part)) = writer.take() {
            let mut meta = wr.finish()?.to_sst_meta(id, target as u32);
            meta.partition = part;
            outputs.push(meta);
        }
        Ok(())
    };

    let mut last_key: Option<Vec<u8>> = None;
    let mut emitted_le_for_key = false;

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
                let part = if bottom {
                    cf.opts.partition_of(&uk).map(str::to_string)
                } else {
                    None
                };
                if let Some((_, _, _, _, cur)) = writer.as_ref() {
                    if *cur != part {
                        finish_output(&mut writer, &mut outputs)?;
                    }
                }
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

    // Open readers for the new tables.
    let mut new_handles = Vec::new();
    for meta in &outputs {
        let klog = cf.klog_path(meta.id);
        let reader = Reader::open(
            &klog,
            cf.ctx.fc.clone(),
            cf.ctx.bc.clone(),
            meta.id,
            cmp.clone(),
        )?;
        new_handles.push(Arc::new(SstHandle {
            meta: meta.clone(),
            reader,
        }));
    }

    // Build the new level set.
    let input_ids: std::collections::HashSet<u64> = inputs.iter().map(|t| t.meta.id).collect();
    let new_levels = cf.with_levels(|levels| {
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
                let mut lvl = retained_next.clone();
                lvl.extend(new_handles.iter().cloned());
                lvl.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
                out.push(lvl);
            } else {
                out.push(levels.get(i).cloned().unwrap_or_default());
            }
        }
        out
    });
    cf.replace_levels(new_levels);

    // Persist the manifest before deleting old files.
    db.persist_manifest()?;

    // Delete and evict the obsolete input files (deferred if a checkpoint/backup
    // has paused deletions, so it can copy a consistent file set).
    for th in &inputs {
        th.reader.close();
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
