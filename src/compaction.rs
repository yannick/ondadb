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

/// Run compaction on `cf` until no level is over its trigger.
pub(crate) fn run(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<()> {
    if cf.opts.compaction_style == crate::config::CompactionStyle::Fifo {
        return run_fifo(db, cf);
    }
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
    let cmp = cf.cmp();
    let target = level + 1;

    // Snapshot the input handles.
    let (inputs, retained_next, num_levels): (Vec<Arc<SstHandle>>, Vec<Arc<SstHandle>>, usize) = cf
        .with_levels(|levels| {
            let mut inputs: Vec<Arc<SstHandle>> = levels[level].clone();
            let (min_key, max_key) = key_span(&inputs, &cmp);
            let mut retained_next = Vec::new();
            if target < levels.len() {
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
    let mut writer: Option<(Writer, String, u64, u64)> = None; // (writer, klog, id, bytes)

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
                if writer.is_none() {
                    let id = db.next_file_id();
                    let klog = cf.klog_path(id);
                    let w = Writer::new(&klog, cf_writer_opts(cf, &cmp, target as u32))?;
                    writer = Some((w, klog, id, 0));
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
                    let (wr, _klog, id, _bytes) = writer.take().unwrap();
                    outputs.push(wr.finish()?.to_sst_meta(id, target as u32));
                }
            }
        }

        its[bi].next();
    }
    if let Some((wr, _klog, id, _bytes)) = writer.take() {
        outputs.push(wr.finish()?.to_sst_meta(id, target as u32));
    }

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
                // Drop all inputs that came from this level.
                let kept: Vec<Arc<SstHandle>> = levels
                    .get(i)
                    .map(|l| {
                        l.iter()
                            .filter(|t| !input_ids.contains(&t.meta.id))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
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
        cmp: cmp.clone(),
        enable_bloom: cf.opts.enable_bloom_filter,
        bloom_fpr: cf.opts.bloom_fpr,
        klog_value_threshold: cf.opts.klog_value_threshold,
        block_size: 4 << 10,
        expected_entries: 4096,
        use_btree: cf.opts.use_btree,
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
