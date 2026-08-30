//! Leveled compaction.
//!
//! L0 is compacted into L1 when its file count reaches `l1_file_count_trigger`;
//! a level `i >= 1` is compacted into `i+1` when its byte size exceeds the
//! level's capacity (`l1_base_bytes * level_size_ratio^(i-1)`).  Inputs are
//! merge-iterated in internal order; for each user key the newest version is
//! kept, plus every version newer than the oldest live snapshot, and tombstones
//! are dropped once they reach the bottom level.
//!
//! # Bounded jobs (0.8.0)
//!
//! Compaction picks **one** file from the source level and merges it with only
//! the target-level files its key range overlaps, so a job costs about
//! `target_file_size * (1 + level_size_ratio)` regardless of how large the
//! level has grown. Before 0.8.0 a job took the *whole* source level plus every
//! target file it overlapped; since L0 files span nearly the entire keyspace
//! under random keys, that rewrote all of L1 every time, and all of L2 below
//! that. Work per compaction therefore grew with the dataset, and sustained
//! ingest built debt faster than it could be paid: measured on a 24-core M2
//! Ultra, draining that backlog at close took 2.5 s after 5M inserts and 35 s
//! after 20M — while the reported write rate stayed flat at ~4.6M ops/s,
//! because nothing in the write path was aware of the debt at all.
//!
//! Two things follow from bounded jobs. Compactions on disjoint key ranges no
//! longer share inputs, so they run concurrently — see [`crate::range_lock`],
//! which is also what excludes them from the parts/tiers operations. And debt
//! becomes measurable ([`pending_compaction_bytes`]), which is what the write
//! pacing in `ColumnFamily::apply_commit` throttles against.
//!
//! L0 is bounded differently. Its files overlap each other, so an *arbitrary*
//! subset cannot be merged without reordering versions of a key — but the
//! **oldest** files can be, because `levels[0]` is newest-first and the read
//! path walks it in that order, so a version left behind in a newer L0 file
//! still shadows the copy pushed down to L1. A job therefore takes the oldest
//! `l1_file_count_trigger` files.
//!
//! # Minimum overlap ratio (0.2)
//!
//! Which candidate a level's sweep takes first is a cost choice, and bounded
//! jobs made it a live one: two same-sized files push down for very different
//! prices depending on how many bytes of the next level their spans cover. The
//! sweep therefore visits candidates in ascending
//! `overlap_bytes / (klog_size + vlog_size)` ([`rank_candidates`]) instead of
//! in cursor order. It remains an *ordering* — the loop is still a full sweep
//! that wraps, because the cheapest candidate may be vetoed or already locked
//! — and it applies to levels >= 1 only: L0's oldest-first window is a
//! correctness invariant, not a cost choice.

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
    // A whole-level sweep is the largest burst the engine produces, and it runs
    // on the *caller's* thread. Classifying only at worker spawn would leave
    // every byte of it labelled `Foreground` and therefore unpaced.
    let _io = crate::ioctrl::scoped(crate::ioctrl::IoClass::Compaction);
    run(db, cf)?;
    if cf.opts.compaction_style == crate::config::CompactionStyle::Fifo {
        return Ok(()); // FIFO never merges; eviction already ran above
    }
    let _mu = cf.compact_mu.lock();
    // The sweep rewrites every level, so it takes the whole keyspace: this is
    // what excludes it from background jobs and from the parts/tiers
    // operations, which hold ranges rather than this mutex.
    let _range = cf
        .range_locks
        .acquire_blocking(crate::range_lock::KeyRange::all());
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

/// Background compaction: run bounded jobs until nothing is triggered.
///
/// Unlike [`run_manual`] this takes no CF-wide lock. Each job holds only the
/// key range it rewrites, so several workers compact one column family at once
/// as long as their ranges are disjoint — and a tier move or `detach_part`
/// blocks only the range it touches.
///
/// `stop` lets a closing database abandon queued work between jobs instead of
/// draining it (see `Options::finish_compactions_on_close`).
pub(crate) fn run(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<()> {
    if cf.opts.compaction_style == crate::config::CompactionStyle::Fifo {
        return run_fifo(db, cf);
    }
    while let Some((job, guard)) = pick_compaction(db, cf) {
        cf.compacting
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let res = compact_inputs(db, cf, job.level, job.target, job.inputs);
        cf.compacting
            .store(false, std::sync::atomic::Ordering::Relaxed);
        drop(guard);
        res?;
        cf.compaction_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        refresh_compaction_debt(db, cf);
        // A closing DB stops between jobs; the debt it leaves is legal LSM
        // state that the next open recovers from.
        if db.closing.load(std::sync::atomic::Ordering::Relaxed)
            && !db.opts.finish_compactions_on_close
        {
            break;
        }
    }
    Ok(())
}

/// One unit of compaction work: a bounded input set and the span it covers.
pub(crate) struct CompactionJob {
    pub(crate) level: usize,
    pub(crate) target: usize,
    pub(crate) inputs: Vec<Arc<SstHandle>>,
}

/// Byte capacity of `level` (>= 1). Held apart from `write_buffer_size` since
/// 0.8.0: a level sized to one file cannot be compacted a piece at a time,
/// because that file's range spans everything below it.
fn level_capacity(cf: &Arc<ColumnFamily>, level: usize) -> u64 {
    let ratio = cf.opts.level_size_ratio.max(2);
    cf.opts
        .l1_base_bytes
        .saturating_mul(ratio.saturating_pow(level.saturating_sub(1) as u32))
}

/// Non-mounted bytes held in `level`.
fn level_bytes(db: &DbInner, cf: &Arc<ColumnFamily>, level: usize) -> u64 {
    cf.with_levels(|levels| {
        levels
            .get(level)
            .map(|l| {
                l.iter()
                    .filter(|t| !is_foreign_mount(db, &t.meta))
                    .map(|t| t.meta.klog_size + t.meta.vlog_size)
                    .sum()
            })
            .unwrap_or(0)
    })
}

/// How far past its capacity each level sits, summed — the engine's compaction
/// debt. Drives write pacing (`ColumnFamilyConfig::soft_pending_compaction_bytes`).
pub(crate) fn pending_compaction_bytes(db: &DbInner, cf: &Arc<ColumnFamily>) -> u64 {
    let n = cf.with_levels(|levels| levels.len());
    let mut debt: u64 = 0;
    // L0 is counted by file count, not capacity: every file there must be
    // rewritten into L1 regardless of size.
    let trigger = cf.opts.l1_file_count_trigger.max(1) as u64;
    let l0_files = cf.with_levels(|levels| {
        levels
            .first()
            .map(|l| l.iter().filter(|t| !is_foreign_mount(db, &t.meta)).count())
            .unwrap_or(0)
    }) as u64;
    if l0_files > trigger {
        debt = debt.saturating_add(level_bytes(db, cf, 0));
    }
    for i in 1..n {
        let bytes = level_bytes(db, cf, i);
        debt = debt.saturating_add(bytes.saturating_sub(level_capacity(cf, i)));
    }
    debt
}

/// Recompute the cached debt gauge writers pace against.
///
/// Called by whatever changes level sizes — a flush landing in L0, a compaction
/// completing. Doing it here rather than on the write path keeps `apply_commit`
/// an atomic load instead of a walk over every level's file list.
pub(crate) fn refresh_compaction_debt(db: &DbInner, cf: &Arc<ColumnFamily>) {
    let debt = pending_compaction_bytes(db, cf);
    cf.compaction_debt
        .store(debt, std::sync::atomic::Ordering::Relaxed);
    // A writer parked on the hard limit is waiting for exactly this number to
    // come down; nothing else will wake it.
    cf.notify_debt_waiters();
}

/// Choose the next bounded compaction job and lock its key range, or `None`
/// when nothing is triggered or every triggered candidate is already held.
///
/// Levels are considered most-overfull first (`bytes / capacity`, or
/// `files / trigger` for L0) so the worst backlog is worked down first.
fn pick_compaction(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
) -> Option<(CompactionJob, crate::range_lock::RangeGuard)> {
    let n = cf.with_levels(|levels| levels.len());
    let mut scored: Vec<(f64, usize)> = Vec::new();

    let trigger = cf.opts.l1_file_count_trigger.max(1) as f64;
    let l0_files = cf.with_levels(|levels| {
        levels
            .first()
            .map(|l| l.iter().filter(|t| !is_foreign_mount(db, &t.meta)).count())
            .unwrap_or(0)
    }) as f64;
    if l0_files >= trigger {
        scored.push((l0_files / trigger, 0));
    }
    for i in 1..n {
        let cap = level_capacity(cf, i).max(1) as f64;
        let bytes = level_bytes(db, cf, i) as f64;
        if bytes > cap {
            scored.push((bytes / cap, i));
        }
    }
    // Highest score first; ties by shallower level, which unblocks the levels
    // above it soonest.
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.cmp(&b.1))
    });

    for (_, level) in scored {
        if let Some(job) = build_job(db, cf, level) {
            return Some(job);
        }
    }
    None
}

/// Assemble a job for `level`, or `None` if every candidate there is blocked
/// (range already held, or a foreign mount in the way).
fn build_job(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    level: usize,
) -> Option<(CompactionJob, crate::range_lock::RangeGuard)> {
    let cmp = cf.cmp();
    let target = level + 1;

    if level == 0 {
        // L0 files overlap each other, so an arbitrary subset cannot be
        // compacted — it would reorder versions of the same key. The OLDEST
        // files can be, though: `levels[0]` is newest-first and the read path
        // walks it in that order, so a version left behind in a newer L0 file
        // still shadows the copy this pushes down to L1.
        //
        // Taking the oldest `l1_file_count_trigger` bounds the job. Taking all
        // of L0 bounded it only by `l0_queue_stall_threshold` — 20 memtables,
        // over a gigabyte — and an ingest that happened to stop on a full L0
        // then paid for that whole merge inside `close()`, which is how an
        // otherwise ~200 ms close occasionally became 18 s.
        let take = cf.opts.l1_file_count_trigger.max(1) as usize;
        let inputs: Vec<Arc<SstHandle>> = cf.with_levels(|levels| {
            let Some(l0) = levels.first() else {
                return Vec::new();
            };
            let live: Vec<Arc<SstHandle>> = l0
                .iter()
                .filter(|t| !is_foreign_mount(db, &t.meta))
                .cloned()
                .collect();
            // Oldest-first, then the oldest `take` of them.
            live.into_iter().rev().take(take).collect()
        });
        if inputs.is_empty() {
            return None;
        }
        let (min_key, max_key) = key_span(&inputs, &cmp);
        let with_target = gather_target(db, cf, target, &min_key, &max_key, inputs)?;
        return lock_job(cf, level, target, with_target);
    }

    // Levels >= 1 are sorted by key and disjoint, so one file can be taken on
    // its own. Sweep from the cursor so successive jobs advance across the
    // keyspace rather than re-picking the head of the level.
    let candidates: Vec<Arc<SstHandle>> = cf.with_levels(|levels| {
        levels
            .get(level)
            .map(|l| {
                l.iter()
                    .filter(|t| !is_foreign_mount(db, &t.meta))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    });
    if candidates.is_empty() {
        return None;
    }
    let cursor = cf.compact_cursor.lock().get(&level).cloned();
    let start = match &cursor {
        Some(c) => candidates
            .iter()
            .position(|t| cmp.compare(&t.meta.min_key, c).is_ge())
            .unwrap_or(0),
        None => 0,
    };

    // One full sweep from the cursor, wrapping once, so a blocked candidate
    // never wedges the level — visited cheapest-first (0.2): the file that
    // rewrites the least target-level data per source byte goes first. This is
    // an ORDERING, not a selection. The minimum-score candidate can be
    // unusable, either because `gather_target` vetoes it (a foreign mount
    // overlaps its TARGET span, which the candidate filter above cannot see —
    // it only screens the source table) or because `lock_job` finds the range
    // already held. Picking the minimum and stopping would wedge the level on
    // either; the sweep survives both.
    let order = cf.with_levels(|levels| rank_candidates(levels, &cmp, target, &candidates, start));
    for idx in order {
        let pick = candidates[idx].clone();
        let (min_key, max_key) = key_span(std::slice::from_ref(&pick), &cmp);
        let Some(inputs) = gather_target(db, cf, target, &min_key, &max_key, vec![pick.clone()])
        else {
            continue; // a foreign mount overlaps: try the next file
        };
        if let Some(job) = lock_job(cf, level, target, inputs) {
            // Next job starts after this file.
            cf.compact_cursor
                .lock()
                .insert(level, pick.meta.max_key.clone());
            return Some(job);
        }
    }
    None
}

/// Add the tables in `target` overlapping `[min_key, max_key]` to `inputs`.
/// `None` if any of them is a foreign mount — merging around a read-only mount
/// would leave overlapping tables in one level.
fn gather_target(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    target: usize,
    min_key: &[u8],
    max_key: &[u8],
    mut inputs: Vec<Arc<SstHandle>>,
) -> Option<Vec<Arc<SstHandle>>> {
    let cmp = cf.cmp();
    let blocked = cf.with_levels(|levels| {
        let Some(lvl) = levels.get(target) else {
            return false;
        };
        for th in lvl {
            if ranges_overlap(&cmp, &th.meta.min_key, &th.meta.max_key, min_key, max_key) {
                if is_foreign_mount(db, &th.meta) {
                    return true;
                }
                inputs.push(th.clone());
            }
        }
        false
    });
    if blocked {
        return None;
    }
    Some(inputs)
}

/// Test-only override restoring the pre-0.2 first-fit cursor sweep, so the
/// write-amplification benchmark can measure both pickers inside one test
/// binary without a second build. Compiled out entirely otherwise.
///
/// It is process-global and the benchmark that flips it is `#[ignore]`d, so a
/// plain `cargo test` never runs it concurrently with the ordering tests.
#[cfg(test)]
static FIRST_FIT_ORDER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Bytes of SSTable written by compaction since the last reset. See the
/// accounting hook in [`compact_inputs`].
#[cfg(test)]
static COMPACTION_OUTPUT_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Bytes of `levels[target]` a compaction over `[min_key, max_key]` would have
/// to rewrite: the **whole** size of every target table the span intersects.
///
/// Whole tables, not the geometric fraction of them the span covers, because
/// that is what a job actually rewrites. Purely advisory: `gather_target` is
/// what builds the real input set, and it may still veto a span scored here
/// (a foreign mount below it). Scoring a span that is later vetoed costs
/// nothing — the try-loop moves on to the next candidate.
///
/// A linear scan rather than the two-pointer pass sortedness would allow: it
/// is correct for any level layout (no disjointness precondition to violate),
/// and at a few hundred files per level the comparisons are lost next to the
/// compaction this schedules.
fn overlap_bytes(
    levels: &[Vec<Arc<SstHandle>>],
    cmp: &ComparatorRef,
    target: usize,
    min_key: &[u8],
    max_key: &[u8],
) -> u64 {
    let Some(lvl) = levels.get(target) else {
        return 0; // nothing below the deepest level
    };
    lvl.iter()
        .filter(|t| ranges_overlap(cmp, &t.meta.min_key, &t.meta.max_key, min_key, max_key))
        .fold(0u64, |sum, t| {
            sum.saturating_add(t.meta.klog_size.saturating_add(t.meta.vlog_size))
        })
}

/// Order `candidates` (indices into it) by minimum overlap ratio:
/// `overlap_bytes(c) / max(1, c.klog_size + c.vlog_size)` ascending — the file
/// that rewrites the least of the level below per byte it contributes.
///
/// Ties break by cyclic distance from `start`, which keeps the cursor sweep's
/// fairness intact when scores are equal (a fresh, evenly-shaped level scores
/// uniformly, and visiting it in cursor order is what advances jobs across the
/// keyspace instead of re-picking its head). `meta.id` is a determinism
/// backstop below that; two distinct indices cannot share a cyclic distance,
/// so it never actually decides, but it makes the order a total one.
///
/// Ratios are compared by cross-multiplication in `u128`: exact for every
/// `u64` size, no float rounding to make two different ratios compare equal,
/// and a product of two `u64`s cannot overflow a `u128`.
fn rank_candidates(
    levels: &[Vec<Arc<SstHandle>>],
    cmp: &ComparatorRef,
    target: usize,
    candidates: &[Arc<SstHandle>],
    start: usize,
) -> Vec<usize> {
    let n = candidates.len();
    if n == 0 {
        return Vec::new();
    }

    #[cfg(test)]
    if FIRST_FIT_ORDER.load(std::sync::atomic::Ordering::Relaxed) {
        return (0..n).map(|k| (start + k) % n).collect();
    }

    // (overlap bytes, source bytes) per candidate. The source floor of 1 keeps
    // a zero-sized table from making every ratio compare equal to it.
    let scores: Vec<(u128, u128)> = candidates
        .iter()
        .map(|c| {
            let over = overlap_bytes(levels, cmp, target, &c.meta.min_key, &c.meta.max_key);
            let src = c.meta.klog_size.saturating_add(c.meta.vlog_size).max(1);
            (over as u128, src as u128)
        })
        .collect();

    let start = start % n;
    let cyclic = |i: usize| (i + n - start) % n;

    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| {
        (scores[a].0 * scores[b].1)
            .cmp(&(scores[b].0 * scores[a].1))
            .then_with(|| cyclic(a).cmp(&cyclic(b)))
            .then_with(|| candidates[a].meta.id.cmp(&candidates[b].meta.id))
    });
    order
}

/// Take the range lock covering every input, or `None` if it is already held.
fn lock_job(
    cf: &Arc<ColumnFamily>,
    level: usize,
    target: usize,
    inputs: Vec<Arc<SstHandle>>,
) -> Option<(CompactionJob, crate::range_lock::RangeGuard)> {
    let cmp = cf.cmp();
    let spans: Vec<(&[u8], &[u8])> = inputs
        .iter()
        .map(|t| (t.meta.min_key.as_slice(), t.meta.max_key.as_slice()))
        .collect();
    let range = crate::range_lock::KeyRange::union(spans, &cmp)?;
    let guard = cf.range_locks.try_acquire(range)?;
    Some((
        CompactionJob {
            level,
            target,
            inputs,
        },
        guard,
    ))
}

/// FIFO "compaction": never merges — evicts the oldest L0 tables past the
/// CF's size/age limits. Manifest is persisted before any file is unlinked
/// (the same ordering the merge path uses).
fn run_fifo(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<()> {
    let _mu = cf.compact_mu.lock();
    // Eviction unlinks whole tables, so it takes the same exclusion the merge
    // path does — otherwise a concurrent parts/tiers operation could be holding
    // a table this is about to delete.
    let _range = cf
        .range_locks
        .acquire_blocking(crate::range_lock::KeyRange::all());
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

/// Compact every table in `level` plus overlapping tables in `level+1` into
/// `level+1`.
fn compact_level(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>, level: usize) -> Result<()> {
    compact_into(db, cf, level, level + 1)
}

/// Compact `level` into `target` (either `level + 1`, or `level` itself for
/// the in-place bottom rewrite manual compaction does — the only way tables
/// in the last level that overlap no incoming data ever see the compaction
/// filter or drop their tombstones again).
/// Whole-level compaction, used by the manual [`run_manual`] sweep. Background
/// compaction goes through [`pick_compaction`] instead, which selects a bounded
/// subset; taking a whole level here is deliberate, since the sweep's job is to
/// push every populated level down once.
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
    let inputs: Vec<Arc<SstHandle>> = cf.with_levels(|levels| {
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
            return Vec::new();
        }
        let (min_key, max_key) = key_span(&inputs, &cmp);
        if target != level && target < levels.len() {
            for th in &levels[target] {
                if ranges_overlap(&cmp, &th.meta.min_key, &th.meta.max_key, &min_key, &max_key) {
                    if is_foreign_mount(db, &th.meta) {
                        return Vec::new();
                    }
                    inputs.push(th.clone());
                }
            }
        }
        inputs
    });

    if inputs.is_empty() {
        return Ok(());
    }
    compact_inputs(db, cf, level, target, inputs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retention {
    Drop,
    Keep { filter_eligible: bool },
}

struct VersionRetention {
    bottom: bool,
    oldest_snapshot: u64,
    now: i64,
    cmp: ComparatorRef,
    last_key: Option<Vec<u8>>,
    emitted_at_or_below_snapshot: bool,
}

impl VersionRetention {
    fn new(bottom: bool, oldest_snapshot: u64, now: i64, cmp: ComparatorRef) -> Self {
        Self {
            bottom,
            oldest_snapshot,
            now,
            cmp,
            last_key: None,
            emitted_at_or_below_snapshot: false,
        }
    }

    fn decide(&mut self, key: &[u8], seq: u64, tombstone: bool, ttl: i64) -> Retention {
        let new_key = self
            .last_key
            .as_deref()
            .is_none_or(|last| !self.cmp.compare(last, key).is_eq());
        if new_key {
            self.last_key = Some(key.to_vec());
            self.emitted_at_or_below_snapshot = false;
        }
        if seq <= self.oldest_snapshot {
            if self.emitted_at_or_below_snapshot {
                return Retention::Drop;
            }
            self.emitted_at_or_below_snapshot = true;
            if tombstone && self.bottom {
                return Retention::Drop;
            }
        }
        if self.bottom && !tombstone && ttl != 0 && ttl <= self.now {
            return Retention::Drop;
        }
        Retention::Keep {
            filter_eligible: !tombstone
                && seq <= self.oldest_snapshot
                && (ttl == 0 || ttl > self.now),
        }
    }
}

struct CurrentOutput {
    writer: Writer,
    klog: String,
    id: u64,
    bytes: u64,
    partition: Option<String>,
}

struct CompactionOutputBuilder<'a> {
    db: &'a Arc<DbInner>,
    cf: &'a Arc<ColumnFamily>,
    cmp: &'a ComparatorRef,
    target: usize,
    target_bytes: u64,
    carry_entry_time: Option<i64>,
    partitioner: Option<crate::config::PartitionResolver>,
    current: Option<CurrentOutput>,
    outputs: Vec<SstMeta>,
    last_boundary: Option<Vec<u8>>,
    #[cfg(debug_assertions)]
    finalized_boundaries: std::collections::HashSet<Vec<u8>>,
    finished: bool,
}

impl<'a> CompactionOutputBuilder<'a> {
    fn new(
        db: &'a Arc<DbInner>,
        cf: &'a Arc<ColumnFamily>,
        cmp: &'a ComparatorRef,
        target: usize,
        inputs: &[Arc<SstHandle>],
        partitioner: Option<crate::config::PartitionResolver>,
    ) -> Self {
        Self {
            db,
            cf,
            cmp,
            target,
            target_bytes: (cf.opts.target_file_size as u64).max(1),
            carry_entry_time: inputs
                .iter()
                .filter_map(|handle| handle.meta.max_entry_time)
                .max(),
            partitioner,
            current: None,
            outputs: Vec::new(),
            last_boundary: None,
            #[cfg(debug_assertions)]
            finalized_boundaries: std::collections::HashSet::new(),
            finished: false,
        }
    }

    fn finish_current(&mut self) -> Result<()> {
        let Some(current) = self.current.take() else {
            return Ok(());
        };
        let CurrentOutput {
            writer,
            klog,
            id,
            partition,
            ..
        } = current;
        let file_meta = match writer.finish() {
            Ok(meta) => meta,
            Err(error) => {
                let _ = std::fs::remove_file(&klog);
                let _ = std::fs::remove_file(crate::sst::vlog_path_for(&klog));
                return Err(error);
            }
        };
        let mut meta = file_meta.to_sst_meta(id, self.target as u32);
        meta.partition = partition;
        meta.max_entry_time = self.carry_entry_time;
        self.outputs.push(meta);
        Ok(())
    }

    fn output_boundary_change(&self, key: &[u8], partition: &Option<String>) -> (bool, bool) {
        let Some(current) = &self.current else {
            return (false, false);
        };
        let boundary_changed = match (&self.partitioner, self.last_boundary.as_deref()) {
            (Some(resolver), Some(previous)) => resolver.boundary(key) != Some(previous),
            (Some(resolver), None) => resolver.boundary(key).is_some(),
            (None, _) => false,
        };
        (
            boundary_changed || current.partition != *partition,
            boundary_changed,
        )
    }

    #[cfg(debug_assertions)]
    fn record_boundary_crossing(&mut self, key: &[u8]) {
        if let Some(previous) = &self.last_boundary {
            self.finalized_boundaries.insert(previous.clone());
        }
        if let Some(next) = self.partitioner.as_ref().and_then(|p| p.boundary(key)) {
            debug_assert!(
                !self.finalized_boundaries.contains(next),
                "PartitionFn is not order-compatible: boundary {next:?} reappeared after its \
                 part was finalized, which would make a bottom SSTable span two partitions"
            );
        }
    }

    fn open_output(&mut self, partition: Option<String>) -> Result<()> {
        let id = self.db.next_file_id();
        let klog = self.cf.klog_path(id);
        let writer = match Writer::new(&klog, cf_writer_opts(self.cf, self.cmp, self.target as u32))
        .map(|w| w.with_limiter(self.cf.ctx.io_limiter.clone()))
        {
            Ok(writer) => writer,
            Err(error) => {
                let _ = std::fs::remove_file(&klog);
                let _ = std::fs::remove_file(crate::sst::vlog_path_for(&klog));
                return Err(error);
            }
        };
        self.current = Some(CurrentOutput {
            writer,
            klog,
            id,
            bytes: 0,
            partition,
        });
        Ok(())
    }

    fn write(
        &mut self,
        key: &[u8],
        value: &[u8],
        seq: u64,
        ttl: i64,
        tombstone: bool,
        single_delete: bool,
    ) -> Result<()> {
        let partition = self.partitioner.as_ref().and_then(|p| p.name_of(key));
        let (cut, _boundary_changed) = self.output_boundary_change(key, &partition);
        if cut {
            #[cfg(debug_assertions)]
            if _boundary_changed {
                self.record_boundary_crossing(key);
            }
            self.finish_current()?;
        }
        self.last_boundary = self
            .partitioner
            .as_ref()
            .and_then(|p| p.boundary(key))
            .map(<[u8]>::to_vec);
        if self.current.is_none() {
            self.open_output(partition)?;
        }
        let current = self.current.as_mut().expect("output opened above");
        current
            .writer
            .add(key, value, seq, ttl, tombstone, single_delete)?;
        current.bytes += (key.len() + value.len()) as u64;
        if current.bytes >= self.target_bytes {
            self.finish_current()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<SstMeta>> {
        self.finish_current()?;
        self.finished = true;
        Ok(std::mem::take(&mut self.outputs))
    }
}

impl Drop for CompactionOutputBuilder<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(current) = self.current.take() {
            current.writer.abort();
        }
        for meta in &self.outputs {
            let klog = self.cf.klog_path(meta.id);
            let _ = std::fs::remove_file(&klog);
            let _ = std::fs::remove_file(crate::sst::vlog_path_for(&klog));
        }
    }
}

fn smallest_input(its: &[SstIterator], cmp: &ComparatorRef) -> Option<usize> {
    let mut best = None;
    for (index, iterator) in its.iter().enumerate() {
        if !iterator.valid() {
            continue;
        }
        match best {
            None => best = Some(index),
            Some(previous) => {
                let previous = &its[previous];
                let order = cmp
                    .compare(iterator.user_key(), previous.user_key())
                    .then_with(|| previous.seq().cmp(&iterator.seq()));
                if order.is_lt() {
                    best = Some(index);
                }
            }
        }
    }
    best
}

struct CompactionMerge<'a> {
    db: &'a Arc<DbInner>,
    cf: &'a Arc<ColumnFamily>,
    cmp: &'a ComparatorRef,
    target: usize,
    inputs: &'a [Arc<SstHandle>],
    bottom: bool,
    oldest_snapshot: u64,
    now: i64,
    filter: Option<crate::column_family::CompactionFilterFn>,
    partitioner: Option<crate::config::PartitionResolver>,
}

impl CompactionMerge<'_> {
    fn run(self) -> Result<Vec<SstMeta>> {
        let mut iterators: Vec<SstIterator> = self
            .inputs
            .iter()
            .map(|table| table.reader().map(|reader| reader.iter()))
            .collect::<Result<_>>()?;
        for iterator in &mut iterators {
            iterator.seek_to_first();
        }
        let mut retention = VersionRetention::new(
            self.bottom,
            self.oldest_snapshot,
            self.now,
            self.cmp.clone(),
        );
        let mut outputs = CompactionOutputBuilder::new(
            self.db,
            self.cf,
            self.cmp,
            self.target,
            self.inputs,
            self.partitioner,
        );
        while let Some(index) = smallest_input(&iterators, self.cmp) {
            let (key, seq, tombstone, ttl, single_delete) = {
                let iterator = &iterators[index];
                (
                    iterator.user_key().to_vec(),
                    iterator.seq(),
                    iterator.is_tombstone(),
                    iterator.ttl(),
                    iterator.is_single_delete(),
                )
            };
            if let Retention::Keep { filter_eligible } = retention.decide(&key, seq, tombstone, ttl)
            {
                let value = iterators[index].value()?;
                let filter_removes = filter_eligible
                    && self.filter.as_ref().is_some_and(|filter| {
                        filter(&key, &value) == crate::column_family::FilterDecision::Remove
                    });
                if !(filter_removes && self.bottom) {
                    outputs.write(
                        &key,
                        &value,
                        seq,
                        ttl,
                        tombstone || filter_removes,
                        single_delete,
                    )?;
                }
            }
            iterators[index].next();
        }
        outputs.finish()
    }
}

fn install_compaction_outputs(
    cf: &Arc<ColumnFamily>,
    cmp: &ComparatorRef,
    level: usize,
    target: usize,
    inputs: &[Arc<SstHandle>],
    outputs: &[SstMeta],
) {
    let new_handles: Vec<Arc<SstHandle>> = outputs
        .iter()
        .map(|meta| cf.handle_for(meta.clone()))
        .collect();
    let input_ids: std::collections::HashSet<u64> =
        inputs.iter().map(|table| table.meta.id).collect();
    cf.update_levels(|levels| {
        let needed = (target + 1).max(levels.len());
        let mut updated = Vec::with_capacity(needed);
        for index in 0..needed {
            let mut tables: Vec<Arc<SstHandle>> = levels
                .get(index)
                .map(|tables| {
                    tables
                        .iter()
                        .filter(|table| !input_ids.contains(&table.meta.id))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            if index == target {
                tables.extend(new_handles.iter().cloned());
                tables.sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
            }
            updated.push(tables);
        }
        debug_assert!(
            {
                let before: std::collections::HashSet<u64> =
                    levels.iter().flatten().map(|table| table.meta.id).collect();
                let after: std::collections::HashSet<u64> = updated
                    .iter()
                    .flatten()
                    .map(|table| table.meta.id)
                    .collect();
                before.difference(&after).all(|id| input_ids.contains(id))
            },
            "compaction dropped a table that was not one of its inputs — that \
             is committed data becoming unreachable (level={level} target={target})"
        );
        updated
    });
}

fn remove_compaction_inputs(db: &DbInner, cf: &ColumnFamily, inputs: &[Arc<SstHandle>]) {
    for table in inputs {
        table.close();
        db.remove_sst_file(&cf.klog_path(table.meta.id));
        db.remove_sst_file(&format!("{}/{}.vlog", cf.dir(), table.meta.id));
    }
}

/// Merge `inputs` from `level` into `target` and install the result.
///
/// The caller owns input selection *and* the range lock covering every input —
/// this function assumes exclusive ownership of that span and does not check.
pub(crate) fn compact_inputs(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    level: usize,
    target: usize,
    inputs: Vec<Arc<SstHandle>>,
) -> Result<()> {
    let cmp = cf.cmp();
    debug_assert!(target == level || target == level + 1);
    if inputs.is_empty() {
        return Ok(());
    }

    let num_levels = cf.with_levels(|levels| levels.len()).max(target + 1);
    let bottom = target >= num_levels - 1
        && cf.with_levels(|levels| levels.iter().skip(target + 1).all(|level| level.is_empty()));
    let oldest_snapshot = db.oldest_snapshot();
    let now = now_nanos();
    let filter = cf.compaction_filter();
    // Only bottom output is partition-cut. Snapshot the resolver once so a
    // concurrent rule addition cannot change boundaries during this run.
    let partitioner = if bottom {
        Some(cf.partition_resolver_snapshot()?)
    } else {
        None
    };

    let outputs = CompactionMerge {
        db,
        cf,
        cmp: &cmp,
        target,
        inputs: &inputs,
        bottom,
        oldest_snapshot,
        now,
        filter,
        partitioner,
    }
    .run()?;
    // Benchmark accounting only (`overlap_ratio_write_amp_benchmark`): the
    // numerator of compaction write amplification. Compiled out of every
    // non-test build — write-amp statistics are a documented non-goal of the
    // public API, and this exists so the 0.2 picker could be measured, not to
    // become one.
    #[cfg(test)]
    COMPACTION_OUTPUT_BYTES.fetch_add(
        outputs
            .iter()
            .map(|o| o.klog_size.saturating_add(o.vlog_size))
            .sum::<u64>(),
        std::sync::atomic::Ordering::Relaxed,
    );
    install_compaction_outputs(cf, &cmp, level, target, &inputs, &outputs);

    // Writer::finish has synced every output and its parent directory. Publish
    // that new level set durably before any obsolete input can be unlinked.
    db.persist_manifest()?;
    remove_compaction_inputs(db, cf, &inputs);
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
        block_size: cf.opts.data_block_size,
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

/// The 0.2 overlapping-level fixture generator, shared with the integration
/// tests rather than copied (see `tests/support/levels.rs`).
#[cfg(test)]
#[path = "../tests/support/levels.rs"]
mod fixture;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        build_job, gather_target, key_span, overlap_bytes, rank_candidates, Retention,
        VersionRetention, COMPACTION_OUTPUT_BYTES, FIRST_FIT_ORDER,
    };
    use crate::comparator::{default_comparator, CaseInsensitive, ComparatorRef};

    #[test]
    fn version_retention_preserves_snapshots_and_reclaims_bottom_debris() {
        let mut bottom = VersionRetention::new(true, 10, 100, default_comparator());

        assert_eq!(
            bottom.decide(b"a", 12, true, 0),
            Retention::Keep {
                filter_eligible: false
            }
        );
        assert_eq!(
            bottom.decide(b"a", 10, false, 0),
            Retention::Keep {
                filter_eligible: true
            }
        );
        assert_eq!(bottom.decide(b"a", 9, false, 0), Retention::Drop);
        assert_eq!(bottom.decide(b"b", 8, true, 0), Retention::Drop);
        assert_eq!(bottom.decide(b"c", 8, false, 99), Retention::Drop);

        let mut upper = VersionRetention::new(false, 10, 100, default_comparator());
        assert_eq!(
            upper.decide(b"a", 10, true, 0),
            Retention::Keep {
                filter_eligible: false
            }
        );
        assert_eq!(
            upper.decide(b"b", 10, false, 99),
            Retention::Keep {
                filter_eligible: false
            }
        );

        let folded: ComparatorRef = Arc::new(CaseInsensitive);
        let mut custom = VersionRetention::new(false, 10, 100, folded);
        assert!(matches!(
            custom.decide(b"A", 10, false, 0),
            Retention::Keep { .. }
        ));
        assert_eq!(custom.decide(b"a", 9, false, 0), Retention::Drop);
    }

    // ---- 0.2: minimum-overlap-ratio picking -------------------------------

    /// Open a scratch database with one column family, so the picker tests can
    /// install hand-built level sets through `replace_levels`. Nothing here
    /// ever opens a reader — the picker reads `SstMeta` only — so the handles
    /// may name files that do not exist.
    fn picker_db(
        dir: &tempfile::TempDir,
        cfg: crate::config::ColumnFamilyConfig,
    ) -> (crate::DB, Arc<crate::column_family::ColumnFamily>) {
        let db =
            crate::DB::open(crate::config::Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("default", cfg).unwrap();
        (db, cf)
    }

    fn handle(
        cf: &Arc<crate::column_family::ColumnFamily>,
        id: u64,
        level: u32,
        min: &[u8],
        max: &[u8],
        klog: u64,
        vlog: u64,
    ) -> Arc<crate::column_family::SstHandle> {
        cf.handle_for(crate::manifest::SstMeta {
            id,
            level,
            klog_size: klog,
            vlog_size: vlog,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            ..crate::manifest::SstMeta::default()
        })
    }

    /// A table `is_foreign_mount` rejects: it carries a shared-tier object
    /// name, and a database that never published to a shared tier (no minted
    /// instance nonce) treats every object-named table as mounted elsewhere.
    fn foreign_handle(
        cf: &Arc<crate::column_family::ColumnFamily>,
        id: u64,
        level: u32,
        min: &[u8],
        max: &[u8],
        klog: u64,
        vlog: u64,
    ) -> Arc<crate::column_family::SstHandle> {
        cf.handle_for(crate::manifest::SstMeta {
            id,
            level,
            klog_size: klog,
            vlog_size: vlog,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            object: Some(format!("cf-default/{:016x}-{id}", 0xfeedu64)),
            ..crate::manifest::SstMeta::default()
        })
    }

    #[test]
    fn overlap_bytes_sums_intersecting_target_tables() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        let levels = vec![
            Vec::new(),
            Vec::new(),
            vec![
                handle(&cf, 1, 2, b"c", b"e", 10, 20),     // 30
                handle(&cf, 2, 2, b"g", b"i", 100, 200),   // 300
                handle(&cf, 3, 2, b"k", b"m", 1000, 2000), // 3000
            ],
        ];

        // Disjoint span: nothing to rewrite.
        assert_eq!(overlap_bytes(&levels, &cmp, 2, b"a", b"b"), 0);
        // Contained in one table's span: that whole table, klog + vlog.
        assert_eq!(overlap_bytes(&levels, &cmp, 2, b"cc", b"dd"), 30);
        // Straddling two: both, in full — a job rewrites whole tables, not the
        // fraction of them the span covers.
        assert_eq!(overlap_bytes(&levels, &cmp, 2, b"d", b"h"), 330);
        assert_eq!(overlap_bytes(&levels, &cmp, 2, b"d", b"l"), 3330);
        // Inclusive boundaries: a target table whose max_key equals the span's
        // min_key overlaps it, and is rewritten.
        assert_eq!(overlap_bytes(&levels, &cmp, 2, b"e", b"f"), 30);
        assert_eq!(overlap_bytes(&levels, &cmp, 2, b"b", b"c"), 30);
        // An absent target level scores zero rather than panicking: the
        // deepest level has nothing below it.
        assert_eq!(overlap_bytes(&levels, &cmp, 9, b"a", b"z"), 0);
    }

    #[test]
    fn rank_candidates_orders_by_overlap_ratio() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        // Equal-sized sources; the geometrically first one sits over the
        // biggest target table and the last over almost nothing, so cursor
        // order and ratio order disagree completely.
        let candidates = vec![
            handle(&cf, 10, 1, b"a", b"c", 100, 0),
            handle(&cf, 11, 1, b"e", b"g", 100, 0),
            handle(&cf, 12, 1, b"i", b"k", 100, 0),
        ];
        let levels = vec![
            Vec::new(),
            candidates.clone(),
            vec![
                handle(&cf, 20, 2, b"a", b"c", 10_000, 0),
                handle(&cf, 21, 2, b"e", b"g", 5_000, 0),
                handle(&cf, 22, 2, b"i", b"k", 10, 0),
            ],
        ];

        assert_eq!(
            rank_candidates(&levels, &cmp, 2, &candidates, 0),
            vec![2, 1, 0]
        );
        // The cursor moves the tie-break, never the ratio order.
        assert_eq!(
            rank_candidates(&levels, &cmp, 2, &candidates, 1),
            vec![2, 1, 0]
        );
    }

    #[test]
    fn rank_candidates_breaks_ties_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        // Three candidates with identical ratios: the sweep's fairness is the
        // tie-break, so they are visited in cursor order, wrapping once. (The
        // `meta.id` tie-break below it is a determinism backstop only — two
        // distinct indices can never share a cyclic distance.)
        let candidates = vec![
            handle(&cf, 30, 1, b"a", b"c", 100, 0),
            handle(&cf, 31, 1, b"e", b"g", 100, 0),
            handle(&cf, 32, 1, b"i", b"k", 100, 0),
        ];
        let levels = vec![
            Vec::new(),
            candidates.clone(),
            vec![
                handle(&cf, 40, 2, b"a", b"c", 200, 0),
                handle(&cf, 41, 2, b"e", b"g", 200, 0),
                handle(&cf, 42, 2, b"i", b"k", 200, 0),
            ],
        ];

        assert_eq!(
            rank_candidates(&levels, &cmp, 2, &candidates, 2),
            vec![2, 0, 1]
        );
        // Repeatable: the order is a total order, not a hash iteration.
        for _ in 0..4 {
            assert_eq!(
                rank_candidates(&levels, &cmp, 2, &candidates, 1),
                vec![1, 2, 0]
            );
        }
    }

    #[test]
    fn rank_candidates_uses_u128_not_float() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        // Two ratios that differ only in the last integer bit. Both round to
        // exactly 1.0 in f64, so a float comparison would tie them and fall
        // through to cursor order — which here puts the WORSE one first.
        let big = 1u64 << 62;
        let candidates = vec![
            handle(&cf, 50, 1, b"e", b"g", big, 0), // worse ratio, cursor-first
            handle(&cf, 51, 1, b"a", b"c", big, 0), // better by one byte
        ];
        let levels = vec![
            Vec::new(),
            candidates.clone(),
            vec![
                handle(&cf, 60, 2, b"a", b"c", big + 1, 0),
                handle(&cf, 61, 2, b"e", b"g", big + 2, 0),
            ],
        ];
        assert_eq!(rank_candidates(&levels, &cmp, 2, &candidates, 0), vec![1, 0]);

        // Saturating sizes must not wrap or panic: klog + vlog saturates at
        // u64::MAX and the cross-multiplication stays inside u128.
        let huge = vec![
            handle(&cf, 70, 1, b"a", b"c", u64::MAX, u64::MAX),
            handle(&cf, 71, 1, b"e", b"g", u64::MAX, u64::MAX),
        ];
        let huge_levels = vec![
            Vec::new(),
            huge.clone(),
            vec![
                handle(&cf, 80, 2, b"a", b"c", u64::MAX, u64::MAX),
                handle(&cf, 81, 2, b"e", b"g", 1, 0),
            ],
        ];
        assert_eq!(rank_candidates(&huge_levels, &cmp, 2, &huge, 0), vec![1, 0]);
    }

    #[test]
    fn build_job_skips_unusable_minimum_score_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        // `a` has by far the lowest ratio, but a foreign mount covers its
        // target span — a veto the candidate filter cannot see, because it
        // only screens the SOURCE table. Stopping at the minimum would wedge
        // the level; the sweep must fall through to `b`.
        let candidates = vec![
            handle(&cf, 100, 1, b"a000", b"a099", 1000, 0),
            handle(&cf, 101, 1, b"b000", b"b099", 1000, 0),
        ];
        cf.replace_levels(vec![
            Vec::new(),
            candidates.clone(),
            vec![
                foreign_handle(&cf, 200, 2, b"a000", b"a099", 10, 0),
                handle(&cf, 201, 2, b"b000", b"b099", 100_000, 0),
            ],
        ]);

        let (job, _guard) =
            build_job(&db.inner, &cf, 1).expect("the next-best candidate is usable");
        assert_eq!((job.level, job.target), (1, 2));
        let ids: Vec<u64> = job.inputs.iter().map(|t| t.meta.id).collect();
        assert_eq!(ids, vec![101, 201], "picked the vetoed minimum, or gave up");
    }

    #[test]
    fn build_job_l0_branch_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(
            &dir,
            crate::config::ColumnFamilyConfig {
                l1_file_count_trigger: 2,
                ..crate::config::ColumnFamilyConfig::default()
            },
        );
        // `levels[0]` is newest-first. The L0 branch takes the OLDEST
        // `l1_file_count_trigger` files regardless of how much of L1 they
        // overlap — newest-first shadowing is a correctness invariant, so
        // scoring must never touch this path.
        cf.replace_levels(vec![
            vec![
                handle(&cf, 3, 0, b"x", b"z", 10, 0), // newest, cheapest overlap
                handle(&cf, 2, 0, b"a", b"z", 10, 0),
                handle(&cf, 1, 0, b"a", b"z", 10, 0), // oldest
            ],
            vec![
                handle(&cf, 10, 1, b"a", b"c", 100_000, 0),
                handle(&cf, 11, 1, b"x", b"z", 10, 0),
            ],
            Vec::new(),
        ]);

        let (job, _guard) = build_job(&db.inner, &cf, 0).expect("L0 is compactable");
        assert_eq!((job.level, job.target), (0, 1));
        let ids: Vec<u64> = job.inputs.iter().map(|t| t.meta.id).collect();
        assert_eq!(ids, vec![1, 2, 10, 11], "L0 input selection changed");
        // The L0 branch never touches the level cursor.
        assert!(cf.compact_cursor.lock().get(&0).is_none());
    }

    #[test]
    fn cursor_advances_only_on_usable_pick() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        // Every candidate's target span is covered by a foreign mount, so no
        // pick is usable at all.
        cf.replace_levels(vec![
            Vec::new(),
            vec![
                handle(&cf, 100, 1, b"a000", b"a099", 1000, 0),
                handle(&cf, 101, 1, b"b000", b"b099", 1000, 0),
            ],
            vec![foreign_handle(&cf, 200, 2, b"a000", b"b099", 10, 0)],
        ]);
        assert!(build_job(&db.inner, &cf, 1).is_none());
        assert!(
            cf.compact_cursor.lock().get(&1).is_none(),
            "the cursor advanced past a level that produced no job"
        );

        // Free the target level: the pick succeeds and the cursor lands on the
        // picked table's max_key.
        cf.replace_levels(vec![
            Vec::new(),
            vec![
                handle(&cf, 100, 1, b"a000", b"a099", 1000, 0),
                handle(&cf, 101, 1, b"b000", b"b099", 1000, 0),
            ],
            vec![
                handle(&cf, 200, 2, b"a000", b"a099", 100_000, 0),
                handle(&cf, 201, 2, b"b000", b"b099", 10, 0),
            ],
        ]);
        let (job, _guard) = build_job(&db.inner, &cf, 1).expect("a usable candidate exists");
        assert_eq!(job.inputs[0].meta.id, 101);
        assert_eq!(
            cf.compact_cursor.lock().get(&1).cloned(),
            Some(b"b099".to_vec())
        );
    }

    /// Deterministic 64-bit generator: the property test must reproduce
    /// exactly from its seed, and `rand`'s stream is not a stable contract.
    struct Lcg(u64);

    impl Lcg {
        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 11
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next_u64() % n.max(1)
        }
    }

    #[test]
    fn picked_candidate_minimizes_score_among_usable() {
        for seed in 0..48u64 {
            let dir = tempfile::tempdir().unwrap();
            let (db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
            let cmp = cf.cmp();
            let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);

            // Random disjoint, sorted geometry for both levels, with foreign
            // mounts sprinkled into the target so some candidates are vetoed.
            let n_src = 2 + rng.below(6) as usize;
            let mut source = Vec::new();
            let mut key = 0u64;
            for i in 0..n_src {
                let lo = key + rng.below(3);
                let hi = lo + 1 + rng.below(9);
                key = hi + 1;
                source.push(handle(
                    &cf,
                    1000 + i as u64,
                    1,
                    format!("{lo:06}").as_bytes(),
                    format!("{hi:06}").as_bytes(),
                    1 + rng.below(4096),
                    rng.below(4096),
                ));
            }
            let n_tgt = 1 + rng.below(10) as usize;
            let mut target = Vec::new();
            let mut key = 0u64;
            for i in 0..n_tgt {
                let lo = key + rng.below(3);
                let hi = lo + 1 + rng.below(6);
                key = hi + 1;
                let id = 2000 + i as u64;
                let (lo_k, hi_k) = (format!("{lo:06}"), format!("{hi:06}"));
                let (klog, vlog) = (1 + rng.below(65536), rng.below(65536));
                target.push(if rng.below(5) == 0 {
                    foreign_handle(&cf, id, 2, lo_k.as_bytes(), hi_k.as_bytes(), klog, vlog)
                } else {
                    handle(&cf, id, 2, lo_k.as_bytes(), hi_k.as_bytes(), klog, vlog)
                });
            }
            let levels = vec![Vec::new(), source.clone(), target];
            cf.replace_levels(levels.clone());

            // Score every candidate `gather_target` would accept. Nothing else
            // holds a range lock here, so acceptance is the only usability
            // constraint.
            let usable: Vec<(u64, u128, u128)> = source
                .iter()
                .filter(|c| {
                    gather_target(
                        &db.inner,
                        &cf,
                        2,
                        &c.meta.min_key,
                        &c.meta.max_key,
                        vec![(*c).clone()],
                    )
                    .is_some()
                })
                .map(|c| {
                    let over =
                        overlap_bytes(&levels, &cmp, 2, &c.meta.min_key, &c.meta.max_key) as u128;
                    let src = c.meta.klog_size.saturating_add(c.meta.vlog_size).max(1) as u128;
                    (c.meta.id, over, src)
                })
                .collect();

            match build_job(&db.inner, &cf, 1) {
                None => assert!(usable.is_empty(), "seed {seed}: gave up on a usable level"),
                Some((job, guard)) => {
                    let picked = job.inputs[0].meta.id;
                    let me = usable
                        .iter()
                        .find(|(id, _, _)| *id == picked)
                        .unwrap_or_else(|| panic!("seed {seed}: picked an unusable candidate"));
                    for other in &usable {
                        assert!(
                            me.1 * other.2 <= other.1 * me.2,
                            "seed {seed}: picked {picked} over cheaper usable {}",
                            other.0
                        );
                    }
                    // The job's range lock covers the union span of its
                    // inputs: while the guard lives that exact span cannot be
                    // taken again, and it frees on drop.
                    let (min_key, max_key) = key_span(&job.inputs, &cmp);
                    let span =
                        crate::range_lock::KeyRange::new(min_key.clone(), max_key.clone());
                    assert!(
                        cf.range_locks.try_acquire(span).is_none(),
                        "seed {seed}: the job's span is not locked"
                    );
                    drop(guard);
                    assert!(cf
                        .range_locks
                        .try_acquire(crate::range_lock::KeyRange::new(min_key, max_key))
                        .is_some());
                }
            }
        }
    }

    /// Compaction write amplification of the 0.2 ratio picker against the
    /// first-fit sweep it replaces, on the shared overlapping-level fixture.
    ///
    /// `#[ignore]`d: it writes hundreds of megabytes and takes minutes, and it
    /// flips the process-global [`FIRST_FIT_ORDER`], which the ordering tests
    /// above read. A plain `cargo test` runs neither, so they never collide.
    ///
    /// ```sh
    /// cargo test --release --lib -- --ignored --nocapture write_amp
    /// ```
    ///
    /// Arms alternate so thermal drift lands on both. One CSV line per run:
    /// `picker,run,ingested_bytes,compaction_bytes,ratio`.
    #[test]
    #[ignore]
    fn overlap_ratio_write_amp_benchmark() {
        use std::sync::atomic::Ordering::Relaxed;

        let runs: usize = std::env::var("ONDADB_BENCH_RUNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let geometry = super::fixture::LevelGeometry::default();
        let ingested = geometry.ingested_bytes();

        /// One ingest of the whole fixture, drained at close so deferred
        /// compaction is counted rather than abandoned. Returns the bytes
        /// compaction wrote.
        fn one_run(geometry: &super::fixture::LevelGeometry, first_fit: bool) -> u64 {
            let dir = tempfile::tempdir().unwrap();
            FIRST_FIT_ORDER.store(first_fit, Relaxed);
            COMPACTION_OUTPUT_BYTES.store(0, Relaxed);
            let mut opts = crate::config::Options::new(dir.path().to_str().unwrap());
            // Otherwise the backlog is abandoned at close and the arm that
            // deferred the most work would look like the cheapest one.
            opts.finish_compactions_on_close = true;
            let db = crate::DB::open(opts).unwrap();
            let cf = db
                .create_column_family("fixture", geometry.compacting_config())
                .unwrap();
            geometry.write(&db, &cf);
            db.close().unwrap();
            FIRST_FIT_ORDER.store(false, Relaxed);
            COMPACTION_OUTPUT_BYTES.load(Relaxed)
        }

        let mut baseline = Vec::new();
        let mut candidate = Vec::new();
        println!("picker,run,ingested_bytes,compaction_bytes,ratio");
        for run in 0..runs {
            for (name, first_fit, out) in [
                ("first-fit", true, &mut baseline),
                ("min-overlap-ratio", false, &mut candidate),
            ] {
                let bytes = one_run(&geometry, first_fit);
                let ratio = bytes as f64 / ingested as f64;
                println!("{name},{run},{ingested},{bytes},{ratio:.4}");
                out.push(ratio);
            }
        }

        let summarize = |name: &str, mut v: Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median = v[v.len() / 2];
            println!(
                "{name}: median {median:.4}  min {:.4}  max {:.4}  spread {:.4}",
                v[0],
                v[v.len() - 1],
                v[v.len() - 1] - v[0]
            );
            (median, v[v.len() - 1] - v[0])
        };
        let (base_median, base_spread) = summarize("first-fit", baseline);
        let (cand_median, _) = summarize("min-overlap-ratio", candidate);
        // The acceptance gate: the candidate's median must beat the baseline's
        // by MORE than the baseline's own min-max spread, or the difference is
        // indistinguishable from this machine's run-to-run noise.
        println!(
            "gate: improvement {:.4} vs baseline spread {base_spread:.4} => {}",
            base_median - cand_median,
            if base_median - cand_median > base_spread {
                "MET"
            } else {
                "NOT MET"
            }
        );
    }
}
