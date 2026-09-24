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
//!
//! # Parallel spans within one job (0.8)
//!
//! Bounded jobs cap the *size* of a job but not its *duration*: an L0 push-down
//! that rewrites all of L1 is one merge on one thread, however many compaction
//! workers are configured, because the concurrency above is between jobs on
//! disjoint ranges and this is one job. [`Options::max_subcompactions`]
//! (default 1, off) lets such a job partition its user-key range into half-open
//! **spans** ([`plan_spans`]), merge them concurrently ([`run_span`] per span,
//! span 0 inline on the coordinator's thread), and install every output with
//! ONE `install_compaction_outputs` inside one `DbInner::catalog_txn` —
//! the catalog commit point since 2.2, whose edit-record fsync is what
//! obsolete-input deletion keys off (AGENTS.md invariant 1).
//!
//! The split is invisible to a reader. Boundaries are user keys and spans are
//! half-open, so every version of one user key lands in exactly one span, which
//! is what makes each span's own [`VersionRetention`] correct. Job-wide
//! decisions are frozen once in [`FrozenJob`] and shared by reference, so no two
//! spans can disagree about `bottom`, the snapshot horizon or the partitioner.
//! The output *files* differ from a single-span run — different boundaries,
//! different ids — so the oracle is logical scan equality at every snapshot,
//! not byte equality.
//!
//! [`Options::max_subcompactions`]: crate::config::Options::max_subcompactions

use std::ops::Bound;
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
    // The excise pre-pass (1.2), ahead of any capacity work: a table every one
    // of whose keys a durable range tombstone already deletes is free to drop,
    // and rewriting it first would be paying to move bytes that are about to be
    // unlinked. One relaxed capability load for a family that never issued a
    // range delete.
    crate::excise::pre_pass(db, cf)?;
    let mut periodic_left = PERIODIC_BURST;
    while let Some((job, guard)) = pick_compaction_with(db, cf, periodic_left > 0) {
        cf.compacting
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let reason = job.reason;
        let res = compact_inputs_spanned(db, cf, job.level, job.target, job.inputs);
        cf.compacting
            .store(false, std::sync::atomic::Ordering::Relaxed);
        drop(guard);
        // A failed job is not counted, for either reason code: the table it
        // would have rewritten keeps its old stamp and stays eligible.
        res?;
        cf.compaction_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if reason == CompactionReason::Periodic {
            cf.periodic_compactions
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            periodic_left -= 1;
            if periodic_left == 0 && periodic_candidate(db, cf, db.now()).is_some() {
                // Burst spent with age work still due: go to the back of the
                // compaction queue instead of draining the rest here, so other
                // families' jobs interleave with this backlog. Capacity work
                // for this family is still picked below — it always outranks
                // age work. A failed send (closing database) just leaves the
                // rest to the next periodic scan.
                let _ = cf.ctx.compact_tx.try_send(cf.clone());
            }
        }
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

/// Why a job was picked. Capacity work and age work are the same merge with
/// the same retention rules; the code exists so an operator can tell them apart
/// in [`CfStats`](crate::maintenance::CfStats) rather than infer periodic
/// activity from a compaction count that never stops rising.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionReason {
    /// A level is over its byte capacity, or L0 over its file-count trigger.
    Capacity,
    /// A table has not been rewritten within
    /// [`periodic_compaction_interval`](crate::config::ColumnFamilyConfig::periodic_compaction_interval).
    Periodic,
}

/// One unit of compaction work: a bounded input set and the span it covers.
pub(crate) struct CompactionJob {
    pub(crate) level: usize,
    pub(crate) target: usize,
    pub(crate) inputs: Vec<Arc<SstHandle>>,
    pub(crate) reason: CompactionReason,
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
/// Periodic (age) jobs one [`run`] pass may take before it yields.
///
/// An interval that elapses for a whole database at once — the typical case,
/// since every table written in one ingest burst ages together — would
/// otherwise be drained back-to-back by a single pass, holding a compaction
/// worker for the entire backlog while every other family's work queues
/// behind it. After this many age jobs the pass re-enqueues its family and
/// stops taking age work; capacity work is unaffected. Not persisted and not
/// an option: it changes only scheduling order, never what a job does.
pub(crate) const PERIODIC_BURST: u32 = 4;

#[cfg(test)]
fn pick_compaction(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
) -> Option<(CompactionJob, crate::range_lock::RangeGuard)> {
    pick_compaction_with(db, cf, true)
}

/// The next job for `cf`: capacity work first, then — when `allow_periodic`
/// — age work.
fn pick_compaction_with(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    allow_periodic: bool,
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
        if let Some(job) = build_job(db, cf, level, CompactionReason::Capacity) {
            return Some(job);
        }
    }

    // Age work is the LOWEST priority: it is consulted only once every level is
    // within capacity and every triggered candidate was unusable. A level over
    // capacity is a backlog that grows; a table past its interval is stale
    // space that does not, so capacity must never wait behind it.
    if !allow_periodic {
        return None;
    }
    periodic_pick(db, cf)
}

/// The periodic pre-pass: pick the oldest eligible table and shape a job for it.
///
/// Gated on the capability as well as the interval. Without `CAP_PERIODIC_AGE`
/// no table carries a stamp and the walk would find nothing anyway — the check
/// is here to state that the trigger is off, rather than to rely on the absence
/// of data for it.
fn periodic_pick(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
) -> Option<(CompactionJob, crate::range_lock::RangeGuard)> {
    if cf.opts.periodic_compaction_interval.is_zero()
        || db.caps() & crate::format::CAP_PERIODIC_AGE == 0
    {
        return None;
    }
    let (level, pick) = periodic_candidate(db, cf, db.now())?;
    build_periodic_job(db, cf, level, pick)
}

/// The oldest table past its family's periodic interval, with the level holding
/// it, or `None` when nothing qualifies.
///
/// Pure over the level snapshot it takes. Three exclusions, all of them
/// "unknown, therefore ineligible":
///
/// * `last_compaction_time == None` — a legacy table, a table written before
///   the capability was enabled, or a part attached from another database
///   (`attach_part`/`attach_part_by_ref`), whose compaction history this
///   database does not own;
/// * a foreign mount, which this database must never rewrite at all;
/// * a stamp in the future (`now < stamp`), which is clock skew: the age is not
///   negative, the table is simply not eligible yet.
pub(crate) fn periodic_candidate(
    db: &DbInner,
    cf: &Arc<ColumnFamily>,
    now: i64,
) -> Option<(usize, Arc<SstHandle>)> {
    let interval = cf.opts.periodic_compaction_interval;
    if interval.is_zero() {
        return None;
    }
    // A configured interval is always positive here, so a non-positive age can
    // never pass the gate below — which is what makes skew safe.
    let interval = i64::try_from(interval.as_nanos()).unwrap_or(i64::MAX);
    cf.with_levels(|levels| oldest_eligible_table(db, levels, interval, now))
}

/// [`periodic_candidate`] over an explicit level snapshot.
fn oldest_eligible_table(
    db: &DbInner,
    levels: &[Vec<Arc<SstHandle>>],
    interval: i64,
    now: i64,
) -> Option<(usize, Arc<SstHandle>)> {
    let mut best: Option<(usize, Arc<SstHandle>, i64)> = None;
    // Top-down, so a tie between two equally old tables resolves to the
    // shallower level — the one whose rewrite also unblocks the levels above.
    for (level, tables) in levels.iter().enumerate() {
        for table in tables {
            let Some(stamp) = table.meta.last_compaction_time else {
                continue;
            };
            if is_foreign_mount(db, &table.meta) {
                continue;
            }
            // Saturating, and compared against a strictly positive interval:
            // a reading behind the stamp yields an age of at most zero and is
            // simply not eligible. No panic, no negative age.
            if now.saturating_sub(stamp) < interval {
                continue;
            }
            if best.as_ref().is_none_or(|(_, _, oldest)| stamp < *oldest) {
                best = Some((level, table.clone(), stamp));
            }
        }
    }
    best.map(|(level, table, _)| (level, table))
}

/// Shape a job around one age-eligible table.
///
/// Non-bottom is an ordinary bounded push-down through
/// [`gather_target`]/[`lock_job`], with the foreign-mount and range-lock vetoes
/// as usual. Bottom is an **in-place rewrite** — the `compact_into(last, last)`
/// shape [`run_manual`] uses, which is the only way a bottom table that
/// overlaps no incoming data ever sees the compaction filter or drops its
/// tombstones again. A deeper level is never created for age reasons alone.
fn build_periodic_job(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    level: usize,
    pick: Arc<SstHandle>,
) -> Option<(CompactionJob, crate::range_lock::RangeGuard)> {
    if !is_bottom_target(cf, level) {
        // L0's files overlap each other, so periodic may NOT push down an
        // arbitrary one — that would reorder versions of a key. Defer to the
        // oldest-first window `build_job` already enforces and relabel the
        // reason; the eligible table is in L0, so the window covers it.
        if level == 0 {
            return build_job(db, cf, 0, CompactionReason::Periodic);
        }
        let cmp = cf.cmp();
        let (min_key, max_key) = key_span(std::slice::from_ref(&pick), &cmp);
        let inputs = gather_target(db, cf, level + 1, &min_key, &max_key, vec![pick])?;
        return lock_job(cf, level, level + 1, inputs, CompactionReason::Periodic);
    }

    let inputs = if level == 0 {
        // A one-level family: its bottom IS L0, whose tables overlap, so the
        // in-place rewrite must take the whole level exactly as
        // `compact_into(0, 0)` does. That is what makes the outputs disjoint,
        // and therefore what makes `install_compaction_outputs`' sort of the
        // rebuilt level correct.
        cf.with_levels(|levels| {
            levels
                .first()
                .map(|l| {
                    l.iter()
                        .filter(|t| !is_foreign_mount(db, &t.meta))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
    } else {
        // Levels >= 1 are key-sorted and disjoint, so one table is rewritten on
        // its own. Taking the whole bottom level here instead would make the
        // single largest job in the engine an untriggered background one.
        vec![pick]
    };
    if inputs.is_empty() {
        return None;
    }
    lock_job(cf, level, level, inputs, CompactionReason::Periodic)
}

/// Assemble a job for `level`, or `None` if every candidate there is blocked
/// (range already held, or a foreign mount in the way).
fn build_job(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    level: usize,
    reason: CompactionReason,
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
        return lock_job(cf, level, target, with_target, reason);
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
        if let Some(job) = lock_job(cf, level, target, inputs, reason) {
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
            // Span bounds on both sides: a target table whose *fragments*
            // reach into the job must be an input, or the job's outputs would
            // sit under a tombstone the job never saw.
            if ranges_overlap(
                &cmp,
                th.meta.span_min(&cmp),
                th.meta.span_max(&cmp),
                min_key,
                max_key,
            ) {
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
static COMPACTION_OUTPUT_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
    reason: CompactionReason,
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
            reason,
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
        let victims = cf.select_fifo_victims(cf.opts.fifo_max_bytes, cf.opts.fifo_ttl);
        if victims.is_empty() {
            return Ok(());
        }
        // Selection no longer mutates the level set: the eviction is published
        // by the transaction, after its edit record's fsync, and only then may
        // a file be unlinked (AGENTS.md invariant 1).
        let ids: Vec<u64> = victims.iter().map(|t| t.meta.id).collect();
        let edit =
            crate::manifest_edit::VersionEdit::new(vec![crate::manifest_edit::Op::RemoveTables {
                cf: cf.name().to_string(),
                ids: ids.clone(),
            }]);
        db.catalog_txn(edit, |p| cf.remove_l0_tables(&ids, p))?;
        for t in &victims {
            db.remove_sst_file(&cf.klog_path(t.meta.id), t.meta.klog_size);
            db.remove_sst_file(
                &format!("{}/{}.vlog", cf.dir(), t.meta.id),
                t.meta.vlog_size,
            );
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
pub(crate) fn is_foreign_mount(db: &DbInner, meta: &crate::manifest::SstMeta) -> bool {
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
    /// Whether a merge operand of the current key has been kept since the last
    /// base. Versions arrive newest-first, so every operand of a chain is seen
    /// before the base that terminates it — which is what lets the base's
    /// bottom-level drops consult this.
    live_operands: bool,
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
            live_operands: false,
        }
    }

    /// Decide whether one version survives this compaction.
    ///
    /// `kind` is not decoration. The "keep exactly one version at or below
    /// `oldest_snapshot`" rule is correct only for *point* kinds, each of which
    /// replaces everything older. A merge operand does not: it composes with
    /// what is below it, so applying the rule unchanged would silently truncate
    /// an operand chain to its newest link and lose history with no folding bug
    /// at all. Hence the three merge carve-outs below, all of which had to land
    /// in the same change that first let kind 4 reach compaction.
    fn decide(&mut self, key: &[u8], seq: u64, kind: u64, tombstone: bool, ttl: i64) -> Retention {
        let merge = kind == crate::format::KIND_MERGE;
        let new_key = self
            .last_key
            .as_deref()
            .is_none_or(|last| !self.cmp.compare(last, key).is_eq());
        if new_key {
            self.last_key = Some(key.to_vec());
            self.emitted_at_or_below_snapshot = false;
            self.live_operands = false;
        }
        if seq <= self.oldest_snapshot {
            // Everything below the base that terminates the visible chain is
            // dead history, operands included — no reader ever walks past a
            // base — so the "already emitted" drop applies to every kind.
            if self.emitted_at_or_below_snapshot {
                return Retention::Drop;
            }
        }
        if seq <= self.oldest_snapshot && !merge {
            // Carve-out 1: an operand does not consume the one-version slot and
            // does not set the flag; only the base that terminates the chain
            // does. That is what keeps a whole chain alive below the snapshot
            // while still collapsing everything older than its base.
            self.emitted_at_or_below_snapshot = true;
            // Carve-out 2: a delete that terminates a chain whose operands are
            // still live is that chain's base, and dropping it would leave the
            // operands reading against whatever a later compaction leaves
            // below. Conservative — at the bottom the older versions are
            // dropped anyway — but it is also what compaction folding needs to
            // see, and a stray bottom tombstone costs one entry.
            if tombstone && self.bottom && !self.live_operands {
                return Retention::Drop;
            }
        }
        // Carve-out 3: operands carry no TTL in v1, so the bottom TTL drop
        // cannot apply to one. (`ttl` is 0 for every operand a writer produces;
        // the guard makes a hand-edited byte harmless rather than lossy.)
        if self.bottom && !merge && !tombstone && ttl != 0 && ttl <= self.now {
            return Retention::Drop;
        }
        self.live_operands = merge;
        Retention::Keep {
            // A merge operand is not filter-eligible. The filter answers "is
            // there a version of this key", and the key is already contributed
            // by the operand's base or by the newest operand; letting an
            // operand answer it would also hand a user filter an operand where
            // it expects a value.
            filter_eligible: !merge
                && !tombstone
                && seq <= self.oldest_snapshot
                && (ttl == 0 || ttl > self.now),
        }
    }
}

#[cfg(test)]
impl VersionRetention {
    /// [`decide`](Self::decide) for a point kind, which is what every
    /// pre-1.1 case is. Keeps the existing table of cases readable now that
    /// `decide` also has to be told which kind it is looking at.
    fn decide_point(&mut self, key: &[u8], seq: u64, tombstone: bool, ttl: i64) -> Retention {
        self.decide(
            key,
            seq,
            crate::format::point_kind(tombstone, false),
            tombstone,
            ttl,
        )
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
    /// The job's one snapshot of `is_bottom_target`, carried rather than
    /// re-derived per output file so every table this job writes agrees with
    /// the retention and partition decisions made from the same snapshot.
    bottom: bool,
    target_bytes: u64,
    carry_entry_time: Option<i64>,
    /// One clock reading taken when the job froze, so every output file of one
    /// compaction shares a stamp. `None` unless `CAP_PERIODIC_AGE` is active —
    /// the manifest may not carry age state a reopen could not attribute.
    ///
    /// Deliberately NOT `carry_entry_time`'s max-over-inputs: carrying the
    /// oldest input's age forward would leave the output instantly eligible
    /// again, which is the loop the design calls out.
    last_compaction_time: Option<i64>,
    partitioner: Option<crate::config::PartitionResolver>,
    current: Option<CurrentOutput>,
    outputs: Vec<SstMeta>,
    last_boundary: Option<Vec<u8>>,
    #[cfg(debug_assertions)]
    finalized_boundaries: std::collections::HashSet<Vec<u8>>,
    finished: bool,
    /// The span's retained range fragments, sorted and disjoint, before
    /// clipping. Each output takes the slice that falls in the interval it
    /// owns.
    fragments: Vec<crate::range_tombstone::Fragment>,
    /// Lower edge of the interval the current output owns; `None` means the
    /// span's own lower edge.
    ///
    /// **This is the clipping rule.** Output `o_i` owns
    /// `[o_i.min_key, o_{i+1}.min_key)`, with the first extended down to the
    /// span's lower edge and the last up to its upper edge. The intervals are
    /// disjoint and ordered by `min_key`, which is exactly what level->=1 point
    /// disjointness already guarantees — so `find_overlapping`'s binary search,
    /// `bottom_overlaps` and `insert_bottom_sorted` all stay valid. Unclipped
    /// bounds would let two adjacent level->=1 tables both cover a key, and the
    /// search returns at most one of them: a covering tombstone would be missed
    /// and deleted data would resurrect.
    interval_lower: Option<Vec<u8>>,
    /// The span's upper edge, closing the last output's interval.
    span_upper: Option<Vec<u8>>,
    /// A size-triggered cut waiting for the next key.
    ///
    /// Deferred on purpose: an output's interval ends where the *next* output
    /// begins, so the cut cannot be finalized until that key is in hand. Before
    /// 1.2 the size cut closed the file immediately, which is equivalent for a
    /// point-only table and wrong for a fragment.
    pending_cut: bool,
    /// The user key whose write armed `pending_cut`. The cut waits until a
    /// *different* user key arrives: cutting between two versions of one key
    /// would leave it in two adjacent tables of a level >= 1, and a point read
    /// probes only the first — every version in the second one (the older
    /// versions a live snapshot may still need) would be invisible.
    cut_after: Vec<u8>,
}

impl<'a> CompactionOutputBuilder<'a> {
    fn new(
        db: &'a Arc<DbInner>,
        cf: &'a Arc<ColumnFamily>,
        cmp: &'a ComparatorRef,
        target: usize,
        bottom: bool,
        carry_entry_time: Option<i64>,
        partitioner: Option<crate::config::PartitionResolver>,
    ) -> Self {
        Self {
            db,
            cf,
            cmp,
            target,
            bottom,
            target_bytes: (cf.opts.target_file_size as u64).max(1),
            carry_entry_time,
            last_compaction_time: (db.caps() & crate::format::CAP_PERIODIC_AGE != 0)
                .then(|| db.now()),
            partitioner,
            current: None,
            outputs: Vec::new(),
            last_boundary: None,
            #[cfg(debug_assertions)]
            finalized_boundaries: std::collections::HashSet::new(),
            finished: false,
            fragments: Vec::new(),
            interval_lower: None,
            span_upper: None,
            pending_cut: false,
            cut_after: Vec::new(),
        }
    }

    /// Install this span's retained fragments and its upper edge.
    fn with_fragments(
        mut self,
        fragments: Vec<crate::range_tombstone::Fragment>,
        span_upper: Option<Vec<u8>>,
    ) -> Self {
        self.fragments = fragments;
        self.span_upper = span_upper;
        self
    }

    /// Close the current output, whose owned interval ends at `upper`
    /// (`None` = the span's upper edge).
    fn finish_current(&mut self, upper: Option<&[u8]>) -> Result<()> {
        let Some(current) = self.current.take() else {
            return Ok(());
        };
        let CurrentOutput {
            mut writer,
            klog,
            id,
            partition,
            ..
        } = current;
        if !self.fragments.is_empty() {
            let hi = upper.or(self.span_upper.as_deref());
            let clipped = clip_fragments(
                self.cmp,
                &self.fragments,
                self.interval_lower.as_deref(),
                hi,
            );
            writer.set_range_fragments(clipped);
        }
        self.interval_lower = upper.map(<[u8]>::to_vec);
        let file_meta = match writer.finish() {
            Ok(meta) => meta,
            Err(error) => {
                let _ = std::fs::remove_file(&klog);
                let _ = std::fs::remove_file(crate::sst::vlog_path_for(&klog));
                return Err(error);
            }
        };
        let mut meta = file_meta.to_sst_meta(id, self.target as u32);
        // A table with fragments but no point entry (a span covered entirely by
        // a range delete) has empty point bounds, which `find_overlapping`'s
        // binary search cannot order. Give it the fragments' own bounds: it has
        // no point versions, so being selected costs one lookup that misses.
        if meta.num_entries == 0 && meta.range_count > 0 {
            meta.min_key = meta.range_min_key.clone().unwrap_or_default();
            meta.max_key = meta.range_max_key.clone().unwrap_or_default();
        }
        meta.partition = partition;
        meta.max_entry_time = self.carry_entry_time;
        meta.last_compaction_time = self.last_compaction_time;
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
        let writer = match Writer::new(
            &klog,
            cf_writer_opts(self.cf, self.cmp, self.target as u32, self.bottom),
        )
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

    fn write(&mut self, key: &[u8], value: &[u8], seq: u64, ttl: i64, kind: u64) -> Result<()> {
        let partition = self.partitioner.as_ref().and_then(|p| p.name_of(key));
        let (cut, _boundary_changed) = self.output_boundary_change(key, &partition);
        let size_cut = self.pending_cut
            && self.current.is_some()
            && !self.cmp.compare(key, &self.cut_after).is_eq();
        if cut || size_cut {
            #[cfg(debug_assertions)]
            if _boundary_changed {
                self.record_boundary_crossing(key);
            }
            // This key opens the next output, so it closes the current one's
            // interval — including the gap between the previous output's last
            // point key and this one.
            self.finish_current(Some(key))?;
            self.pending_cut = false;
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
        current.writer.add(key, value, seq, ttl, kind)?;
        current.bytes += (key.len() + value.len()) as u64;
        if current.bytes >= self.target_bytes && !self.pending_cut {
            // Deferred: the interval's upper edge is the next key.
            self.pending_cut = true;
            self.cut_after.clear();
            self.cut_after.extend_from_slice(key);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<SstMeta>> {
        // A span whose whole content is a range delete produces no point entry
        // and therefore no output — but the fragments still have to land
        // somewhere, or the delete is lost the moment its inputs are unlinked.
        if self.current.is_none() && self.outputs.is_empty() && !self.fragments.is_empty() {
            self.open_output(None)?;
        }
        self.finish_current(None)?;
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

/// Every input table's fragments, exploded into one span per covering
/// sequence.
///
/// Opening a reader per input is free here: the merge is about to open all of
/// them anyway, and a table with no fragments contributes nothing but the
/// `range_count == 0` test.
fn collect_input_ranges(inputs: &[Arc<SstHandle>]) -> Result<Vec<crate::range_tombstone::Span>> {
    if !inputs.iter().any(|t| t.meta.has_ranges()) {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for table in inputs {
        if !table.meta.has_ranges() {
            continue;
        }
        out.extend(crate::range_tombstone::spans_of_fragments(
            table.reader()?.range_fragments(),
        ));
    }
    Ok(out)
}

/// Which sequences of one fragment stack survive this job.
///
/// Mirrors [`VersionRetention`] for the point stream, and for the same reason:
/// every sequence above `oldest_snapshot` is still needed by some live reader,
/// and exactly one at or below it is needed to keep masking older data. That
/// last one is dropped only at the bottom, where nothing older can resurface —
/// and never over a foreign mount, whose bytes this database did not publish.
fn retained_fragment_seqs(seqs: &[u64], oldest_snapshot: u64, droppable: bool) -> Vec<u64> {
    let mut out: Vec<u64> = seqs
        .iter()
        .copied()
        .filter(|s| *s > oldest_snapshot)
        .collect();
    if let Some(&newest_old) = seqs.iter().find(|s| **s <= oldest_snapshot) {
        if !droppable {
            out.push(newest_old);
        }
    }
    out
}

/// Clip already-disjoint, sorted fragments to `[lower, upper)`.
///
/// No re-fragmentation: the input is disjoint, so intersecting each fragment
/// with the interval preserves both properties.
fn clip_fragments(
    cmp: &ComparatorRef,
    frags: &[crate::range_tombstone::Fragment],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> Vec<crate::range_tombstone::Fragment> {
    let mut out = Vec::new();
    for f in frags {
        let start = match lower {
            Some(l) if cmp.compare(&f.start, l).is_lt() => l.to_vec(),
            _ => f.start.clone(),
        };
        let end = match upper {
            Some(u) if cmp.compare(&f.end, u).is_gt() => u.to_vec(),
            _ => f.end.clone(),
        };
        if cmp.compare(&start, &end).is_ge() {
            continue;
        }
        out.push(crate::range_tombstone::Fragment {
            start,
            end,
            seqs: f.seqs.clone(),
        });
    }
    out
}

/// A span's fragment-clipping edges as `(lower, upper)` user keys.
///
/// Fragmentation works on half-open intervals, so an `Excluded` upper bound is
/// the interval's own edge and an `Included` one (or `Unbounded`) means "no
/// clip at the top" — the job span already bounds what the inputs contributed.
fn span_edges<'a>(
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
) -> (Option<&'a [u8]>, Option<&'a [u8]>) {
    let lo = match lower {
        Bound::Unbounded => None,
        Bound::Included(k) | Bound::Excluded(k) => Some(k),
    };
    let hi = match upper {
        Bound::Excluded(k) => Some(k),
        Bound::Included(_) | Bound::Unbounded => None,
    };
    (lo, hi)
}

/// The input whose current entry sorts first, or `None` once every input is
/// exhausted.
///
/// An input that went invalid because a block failed its checksum or could not
/// be read looks exactly like an exhausted one, so it is asked for its error
/// here, on every step: skipping it would merge the rest of the job without
/// that table's remaining entries, and the catalog edit would then retire the
/// only copy of them.
fn smallest_input(its: &[SstIterator], cmp: &ComparatorRef) -> Result<Option<usize>> {
    let mut best = None;
    for (index, iterator) in its.iter().enumerate() {
        if !iterator.valid() {
            if let Some(error) = iterator.err() {
                return Err(error.duplicate());
            }
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
    Ok(best)
}

/// Every decision a compaction job makes **once**, before any merge work
/// starts, and then hands to each span by shared reference.
///
/// Freezing them is what makes several spans of one job agree with each other:
/// two workers deriving `bottom` or the partition resolver independently could
/// see a concurrent compaction or a rule addition in between and cut different
/// boundaries out of the same input set. It is also why a single-span job is
/// unchanged by this refactor — the same values were already captured once in
/// `compact_inputs`, just closer to the merge.
struct FrozenJob {
    /// The job's one snapshot of `is_bottom_target`.
    bottom: bool,
    oldest_snapshot: u64,
    now: i64,
    /// `max_entry_time` carried onto every output, from the inputs.
    carry_entry_time: Option<i64>,
    partitioner: Option<crate::config::PartitionResolver>,
    filter: Option<crate::column_family::CompactionFilterFn>,
    /// The family's merge operator when this job may fold operand chains with
    /// it: `None` when the family has none, when folding is switched off
    /// ([`Options::enable_merge_folding`](crate::Options::enable_merge_folding)),
    /// or when an input is a foreign mount.
    ///
    /// The foreign-mount case cannot arise today — both input-selection paths
    /// filter mounts out, because merging around a read-only mount would leave
    /// overlapping tables in one level — but folding is the one thing here that
    /// rewrites history, so it re-checks rather than inheriting the invariant.
    merge_fold: Option<Arc<dyn crate::config::MergeOperator>>,
    /// Set by the first span that fails; every span polls it once per entry so
    /// siblings stop instead of finishing megabytes of doomed output. Lives
    /// here because it is the one piece of per-job state every span shares,
    /// and `run_span` already takes the job by reference.
    cancel: std::sync::atomic::AtomicBool,
    /// Every range tombstone the inputs carry, exploded back into one span per
    /// `(interval, sequence)` pair so the whole job can be re-fragmented over
    /// its own boundaries. Empty for every job whose inputs are point-only.
    ranges: Vec<crate::range_tombstone::Span>,
    /// Key ranges of foreign mounts in this column family.
    ///
    /// A fragment overlapping one may never be dropped, however old: this
    /// database did not publish the mounted bytes and cannot prove that
    /// nothing under the tombstone survives there.
    foreign_spans: Vec<(Vec<u8>, Vec<u8>)>,
}

impl FrozenJob {
    fn new(
        db: &Arc<DbInner>,
        cf: &Arc<ColumnFamily>,
        target: usize,
        inputs: &[Arc<SstHandle>],
    ) -> Result<FrozenJob> {
        // One snapshot of the predicate for the whole job: retention, partition
        // cutting and the output filter policy must all agree on whether this
        // output is bottom, even if a concurrent compaction changes the shape.
        let bottom = is_bottom_target(cf, target);
        Ok(FrozenJob {
            bottom,
            oldest_snapshot: db.oldest_snapshot(),
            now: now_nanos(),
            carry_entry_time: inputs
                .iter()
                .filter_map(|handle| handle.meta.max_entry_time)
                .max(),
            // Only bottom output is partition-cut. Snapshot the resolver once
            // so a concurrent rule addition cannot change boundaries during
            // this run.
            partitioner: if bottom {
                Some(cf.partition_resolver_snapshot()?)
            } else {
                None
            },
            filter: cf.compaction_filter(),
            merge_fold: cf
                .merge_op()
                .filter(|_| db.opts.enable_merge_folding)
                .filter(|_| !inputs.iter().any(|t| is_foreign_mount(db, &t.meta)))
                .cloned(),
            cancel: std::sync::atomic::AtomicBool::new(false),
            ranges: collect_input_ranges(inputs)?,
            foreign_spans: cf.with_levels(|levels| {
                levels
                    .iter()
                    .flatten()
                    .filter(|t| is_foreign_mount(db, &t.meta))
                    .map(|t| {
                        let cmp = cf.cmp();
                        (
                            t.meta.span_min(&cmp).to_vec(),
                            t.meta.span_max(&cmp).to_vec(),
                        )
                    })
                    .collect()
            }),
        })
    }

    /// Does a foreign mount overlap `[start, end)`?
    fn mount_overlaps(&self, cmp: &ComparatorRef, start: &[u8], end: &[u8]) -> bool {
        self.foreign_spans
            .iter()
            .any(|(lo, hi)| cmp.compare(lo, end).is_lt() && cmp.compare(start, hi).is_le())
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn cancel(&self) {
        self.cancel
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Merge the half-open user-key span `[lower, upper)` of `inputs` into tables
/// for `target`, and return their metadata. The tables are on disk and synced
/// when this returns; nothing is installed and nothing is persisted.
///
/// A whole job is `run_span(.., Unbounded, Unbounded, ..)`; several spans of
/// one job partition the keyspace between them. Because the bounds are **user**
/// keys and the spans are half-open, every version of one user key lands in
/// exactly one span, which is what makes the per-span [`VersionRetention`]
/// correct: it never sees a partial version chain.
#[allow(clippy::too_many_arguments)]
fn run_span(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    cmp: &ComparatorRef,
    target: usize,
    inputs: &[Arc<SstHandle>],
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    frozen: &FrozenJob,
) -> Result<Vec<SstMeta>> {
    let mut iterators: Vec<SstIterator> = inputs
        .iter()
        .map(|table| table.reader().map(|reader| reader.iter()))
        .collect::<Result<_>>()?;
    for iterator in &mut iterators {
        match lower {
            Bound::Unbounded => iterator.seek_to_first(),
            // Entries are ordered user key ascending, sequence DESCENDING, so
            // `(key, u64::MAX)` is the first version of `key`.
            Bound::Included(key) => iterator.seek(key, u64::MAX),
            Bound::Excluded(key) => {
                // `(key, 0)` is the LAST version of `key`; one step past it
                // leaves the whole excluded key behind.
                iterator.seek(key, 0);
                if iterator.valid() && cmp.compare(iterator.user_key(), key).is_eq() {
                    iterator.next();
                }
            }
        }
    }
    let past_upper = |key: &[u8]| match upper {
        Bound::Unbounded => false,
        Bound::Excluded(end) => cmp.compare(key, end).is_ge(),
        Bound::Included(end) => cmp.compare(key, end).is_gt(),
    };

    let mut retention = VersionRetention::new(
        frozen.bottom,
        frozen.oldest_snapshot,
        frozen.now,
        cmp.clone(),
    );
    // Re-fragment every input's range tombstones over THIS span's bounds, then
    // decide what survives. Two passes over the same data:
    //
    //  * `covering` (pre-drop) filters the point stream — a point hidden by a
    //    tombstone at or below `oldest_snapshot` is dropped *before*
    //    `VersionRetention::decide` sees it, so it is never counted as "the one
    //    version at or below the snapshot";
    //  * `retained` is what is written back out, clipped per output.
    let (span_lo, span_hi) = span_edges(lower, upper);
    let covering = if frozen.ranges.is_empty() {
        Vec::new()
    } else {
        crate::range_tombstone::fragment_spans(cmp, &frozen.ranges, span_lo, span_hi)
    };
    let retained: Vec<crate::range_tombstone::Fragment> = covering
        .iter()
        .filter_map(|f| {
            let droppable = frozen.bottom && !frozen.mount_overlaps(cmp, &f.start, &f.end);
            let seqs = retained_fragment_seqs(&f.seqs, frozen.oldest_snapshot, droppable);
            (!seqs.is_empty()).then(|| crate::range_tombstone::Fragment {
                start: f.start.clone(),
                end: f.end.clone(),
                seqs,
            })
        })
        .collect();

    let mut outputs = CompactionOutputBuilder::new(
        db,
        cf,
        cmp,
        target,
        frozen.bottom,
        frozen.carry_entry_time,
        frozen.partitioner.clone(),
    )
    .with_fragments(retained, span_hi.map(<[u8]>::to_vec));
    // The operand suffix being collected for the current key, or `None` —
    // which is every iteration for a family with no operator, and every
    // iteration of a merge family's non-merge keys.
    let mut pending: Option<PendingFold> = None;
    while let Some(index) = smallest_input(&iterators, cmp)? {
        let (key, seq, tombstone, ttl, kind) = {
            let iterator = &iterators[index];
            (
                iterator.user_key().to_vec(),
                iterator.seq(),
                iterator.is_tombstone(),
                iterator.ttl(),
                iterator.kind(),
            )
        };
        if past_upper(&key) {
            break;
        }
        if frozen.cancelled() {
            // A sibling already failed; this span's outputs are removed by
            // `CompactionOutputBuilder`'s abort-on-drop.
            return Err(crate::error::OndaError::Unknown(
                "compaction span cancelled by a failing sibling span".to_string(),
            ));
        }
        // A new user key ends any pending fold: its suffix had no base among
        // these inputs. Ahead of the range-delete drop below, which `continue`s
        // without ending the key — leaving the previous key's suffix pending
        // would attach it to whatever key surfaced next.
        if pending
            .as_ref()
            .is_some_and(|p| !cmp.compare(&p.key, &key).is_eq())
        {
            flush_pending(&mut pending, &mut outputs, frozen)?;
        }
        // Range-delete masking, ahead of `decide`. Only a tombstone at or
        // below `oldest_snapshot` may drop a point: a newer one still has to
        // let snapshots between the two sequences see the value, and those
        // snapshots read the fragment, not the absence of the point.
        //
        // An operand is dropped by the same rule, and correctly: the read path
        // treats a covering span as a deleted base, so operands at or below it
        // contribute nothing. Ones above it survive and fold onto no base.
        if !covering.is_empty()
            && crate::range_tombstone::covering_seq_in(cmp, &covering, &key, frozen.oldest_snapshot)
                .is_some_and(|c| c > seq)
        {
            iterators[index].next();
            continue;
        }
        if let Retention::Keep { filter_eligible } =
            retention.decide(&key, seq, kind, tombstone, ttl)
        {
            let value = iterators[index].value()?;
            // Fold only a contiguous suffix wholly at or below
            // `oldest_snapshot`: above it a live snapshot could sit between two
            // operands, and folding across one would change what that snapshot
            // reads.
            let foldable = frozen.merge_fold.is_some() && seq <= frozen.oldest_snapshot;
            if foldable && kind == crate::format::KIND_MERGE {
                pending
                    .get_or_insert_with(|| PendingFold {
                        key: key.clone(),
                        newest_seq: seq,
                        operands: Vec::new(),
                    })
                    .operands
                    .push((seq, value));
                iterators[index].next();
                continue;
            }
            if let Some(p) = pending.take() {
                // This version terminates the suffix. A base with a live TTL is
                // not foldable: the fold would produce a value whose expiry
                // would later delete the operands' contribution too, where the
                // unfolded chain resolves to `existing = None` plus the
                // operands. Emit the operands unchanged instead.
                if foldable && ttl == 0 {
                    let base = if tombstone { None } else { Some(value) };
                    emit_fold(p, base, &mut outputs, frozen)?;
                    iterators[index].next();
                    continue;
                }
                emit_operands(p, &mut outputs)?;
            }
            let filter_removes = filter_eligible
                && frozen.filter.as_ref().is_some_and(|filter| {
                    filter(&key, &value) == crate::column_family::FilterDecision::Remove
                });
            if !(filter_removes && frozen.bottom) {
                // A filtered-out version is replaced by an ordinary tombstone
                // above the bottom; `filter_eligible` is false for an operand,
                // so this can never rewrite a merge operand's kind.
                let out_kind = if filter_removes {
                    crate::format::KIND_DELETE
                } else {
                    kind
                };
                outputs.write(&key, &value, seq, ttl, out_kind)?;
            }
        }
        iterators[index].next();
    }
    flush_pending(&mut pending, &mut outputs, frozen)?;
    outputs.finish()
}

/// An operand suffix of one key, collected at or below `oldest_snapshot` and
/// waiting for the base that terminates it.
struct PendingFold {
    key: Vec<u8>,
    /// Newest sequence in the suffix — the sequence the folded entry keeps, so
    /// the collapsed record still shadows exactly what the chain shadowed.
    newest_seq: u64,
    /// `(seq, operand)` newest first, the order compaction sees them in.
    operands: Vec<(u64, Vec<u8>)>,
}

/// Emit a pending suffix that never met its base among this job's inputs.
///
/// At the bottom there is nothing below, so "no base" really is
/// `existing = None` and the suffix folds. Above the bottom the base may live
/// in a deeper level, and folding against `None` would invent history — the
/// operands are written back unchanged instead.
fn flush_pending(
    pending: &mut Option<PendingFold>,
    outputs: &mut CompactionOutputBuilder<'_>,
    frozen: &FrozenJob,
) -> Result<()> {
    let Some(p) = pending.take() else {
        return Ok(());
    };
    if frozen.bottom {
        return emit_fold(p, None, outputs, frozen);
    }
    emit_operands(p, outputs)
}

/// Write a suffix's operands back out unchanged, in the order they arrived.
fn emit_operands(p: PendingFold, outputs: &mut CompactionOutputBuilder<'_>) -> Result<()> {
    for (seq, operand) in &p.operands {
        outputs.write(&p.key, operand, *seq, 0, crate::format::KIND_MERGE)?;
    }
    Ok(())
}

/// Collapse a suffix and its base into the single value they fold to.
fn emit_fold(
    p: PendingFold,
    base: Option<Vec<u8>>,
    outputs: &mut CompactionOutputBuilder<'_>,
    frozen: &FrozenJob,
) -> Result<()> {
    let op = frozen
        .merge_fold
        .as_ref()
        .expect("a pending fold exists only when the job may fold");
    // Collected newest first; the operator is handed oldest first.
    let mut operands: Vec<&[u8]> = p.operands.iter().map(|(_, o)| o.as_slice()).collect();
    operands.reverse();
    let folded = op
        .full_merge(&p.key, base.as_deref(), &operands)
        .map_err(|e| {
            crate::error::OndaError::Corruption(format!(
                "merge operator {:?} failed for key {:?}: {e}",
                op.name(),
                String::from_utf8_lossy(&p.key)
            ))
        })?;
    // The folded entry is the surviving version at or below the snapshot, so it
    // is exactly what the compaction filter is meant to see — and it sees a
    // value, never an operand.
    let filter_removes = frozen.filter.as_ref().is_some_and(|filter| {
        filter(&p.key, &folded) == crate::column_family::FilterDecision::Remove
    });
    if filter_removes && frozen.bottom {
        return Ok(());
    }
    let (value, kind) = if filter_removes {
        (&[][..], crate::format::KIND_DELETE)
    } else {
        (folded.as_slice(), crate::format::KIND_PUT)
    };
    outputs.write(&p.key, value, p.newest_seq, 0, kind)
}

/// Extra span workers this job may ask the DB-wide pool for: one fewer than
/// the span count, because the coordinator runs span 0 inline on the
/// `onda-compact-{n}` thread it already occupies.
///
/// Returns 0 — a single span, today's behavior — for every excluded job class.
fn span_budget(db: &Arc<DbInner>, frozen: &FrozenJob, spans_allowed: bool) -> usize {
    if !spans_allowed {
        return 0;
    }
    // A user compaction filter is written against a single-threaded, key-ordered
    // traversal, and its documented "not snapshot-consistent" caveat would
    // become dependent on the span count on top of that. Thread safety is not
    // the reason — the type is already `Send + Sync`.
    if frozen.filter.is_some() {
        return 0;
    }
    db.opts.max_subcompactions.max(1).saturating_sub(1)
}

/// Split the job's user-key range into at most `max_spans` half-open spans,
/// returned as their lower bounds: element 0 is always `Unbounded` and span `i`
/// runs from `plan[i]` up to (excluding) `plan[i + 1]`, the last one to
/// `Unbounded`. Pure over table metadata — it opens nothing.
///
/// Candidates, in the order the design prefers them:
///
/// 1. **Partition cuts, when the job writes bottom output.** These are the
///    ideal split points, because [`CompactionOutputBuilder`] already refuses
///    to let one output table span two partitions, so a span boundary that is
///    also a partition boundary costs nothing in extra files. A partition's
///    boundary is a key *prefix* ([`crate::config::PartitionResolver::boundary`]),
///    and a prefix is exactly the first key of its partition under a bytewise
///    comparator — every key starting with `b` sorts at or after `b`, and no
///    key sorting before `b` can start with it. That argument is bytewise-only,
///    so a partitioned job under a custom comparator stays single-span rather
///    than risk cutting a partition in half.
/// 2. Otherwise the **target tables' `min_key`s**: free, already sorted, and
///    each one is a point where the output would have started a new file
///    anyway.
/// 3. Failing that (an empty target level — an L0 -> L1 job into a fresh
///    level), the **input tables' `min_key`s**.
///
/// Candidates are then deduplicated *under the comparator*, filtered down to
/// the ones that actually have input data on both sides, and — if more remain
/// than `max_spans - 1` — sampled by cumulative target bytes so the spans carry
/// comparable amounts of work rather than comparable amounts of keyspace (see
/// the weighting note below for why the target level and not the whole input
/// set).
fn plan_spans(
    cmp: &ComparatorRef,
    inputs: &[Arc<SstHandle>],
    target_tables: &[Arc<SstHandle>],
    partitioner: Option<&crate::config::PartitionResolver>,
    max_spans: usize,
) -> Vec<Bound<Vec<u8>>> {
    let single = vec![Bound::Unbounded];
    if max_spans <= 1 || inputs.is_empty() {
        return single;
    }

    // A resolver that names no boundary anywhere in this job's key range does
    // not partition it — an empty rule set, or rules that match nothing here —
    // so it constrains nothing. Without this the common case (a bottom job on a
    // family that never declared a partition rule) would refuse to split.
    let partitioner = partitioner.filter(|resolver| {
        inputs.iter().any(|table| {
            resolver.boundary(&table.meta.min_key).is_some()
                || resolver.boundary(&table.meta.max_key).is_some()
        })
    });

    let mut candidates: Vec<Vec<u8>> = Vec::new();
    if let Some(partitioner) = partitioner {
        if !cmp.is_bytewise() {
            return single;
        }
        // Mandatory candidates first: a partition's own start key.
        for table in inputs {
            for key in [&table.meta.min_key, &table.meta.max_key] {
                if let Some(boundary) = partitioner.boundary(key) {
                    candidates.push(boundary.to_vec());
                }
            }
        }
    }
    candidates.extend(target_tables.iter().map(|t| t.meta.min_key.clone()));
    if candidates.is_empty() {
        candidates.extend(inputs.iter().map(|t| t.meta.min_key.clone()));
    }
    if let Some(partitioner) = partitioner {
        // A candidate that is not the first key of its partition would cut one
        // in half. `boundary(c) == Some(c)` says `c` starts a partition;
        // `None` says it is in no named partition at all, where a cut costs
        // nothing — the partitions on either side still force their own cuts.
        // (Every mandatory candidate above passes this by construction; it is
        // the target `min_key`s mixed in with them that need screening.)
        candidates.retain(|c| {
            partitioner
                .boundary(c)
                .is_none_or(|boundary| boundary == c.as_slice())
        });
    }

    candidates.sort_by(|a, b| cmp.compare(a, b));
    candidates.dedup_by(|a, b| cmp.compare(a, b).is_eq());

    // A boundary at or below the job's first key opens with an empty span, and
    // one past its last key closes with one.
    let (job_min, job_max) = key_span(inputs, cmp);
    candidates.retain(|c| cmp.compare(c, &job_min).is_gt() && cmp.compare(c, &job_max).is_le());
    if candidates.is_empty() {
        return single;
    }

    // Bytes lying below each candidate — the work the spans before it would do.
    //
    // Measured over the TARGET tables when there are any, not over the whole
    // input set. Target tables are disjoint and tile the job's key range, so
    // "bytes in tables entirely below `c`" tracks how much of the *keyspace*
    // sits below `c`. Source tables do not: an L0 table spans nearly everything
    // under random keys, so every candidate would count its bytes in full and
    // the weights would come out nearly flat — which is how a 4-span job of a
    // perfectly even fixture ended up two thirds skewed. Splitting the target
    // evenly splits the sources that overlay it evenly too.
    let table_bytes =
        |t: &Arc<SstHandle>| -> u64 { t.meta.klog_size.saturating_add(t.meta.vlog_size) };
    let weighed: &[Arc<SstHandle>] = if target_tables.is_empty() {
        inputs
    } else {
        target_tables
    };
    let below = |candidate: &[u8]| -> u64 {
        weighed
            .iter()
            .filter(|t| cmp.compare(&t.meta.max_key, candidate).is_lt())
            .fold(0u64, |sum, t| sum.saturating_add(table_bytes(t)))
    };
    let total: u64 = weighed
        .iter()
        .fold(0u64, |sum, t| sum.saturating_add(table_bytes(t)));

    let wanted = max_spans - 1;
    if candidates.len() > wanted {
        // Keep the candidate nearest each even byte fraction, in order. Byte
        // weight rather than keyspace: a job's inputs are rarely uniform, and
        // the imbalance stat is what this is trying to keep small.
        let weights: Vec<u64> = candidates.iter().map(|c| below(c)).collect();
        let mut chosen: Vec<usize> = Vec::with_capacity(wanted);
        for k in 1..=wanted {
            let goal = (total / (wanted as u64 + 1)).saturating_mul(k as u64);
            let pick = (0..candidates.len())
                .filter(|i| !chosen.contains(i))
                .min_by_key(|&i| weights[i].abs_diff(goal));
            if let Some(pick) = pick {
                chosen.push(pick);
            }
        }
        chosen.sort_unstable();
        candidates = chosen.into_iter().map(|i| candidates[i].clone()).collect();
    }

    // Drop boundaries that would open an empty span: no input table holds keys
    // in `[previous, candidate)`.
    let mut bounds: Vec<Bound<Vec<u8>>> = vec![Bound::Unbounded];
    let mut previous: Option<Vec<u8>> = None;
    for candidate in candidates {
        let occupied = inputs.iter().any(|t| {
            cmp.compare(&t.meta.min_key, &candidate).is_lt()
                && previous
                    .as_deref()
                    .is_none_or(|lo| cmp.compare(&t.meta.max_key, lo).is_ge())
        });
        if !occupied {
            continue;
        }
        previous = Some(candidate.clone());
        bounds.push(Bound::Included(candidate));
    }
    bounds
}

/// Run `plan`'s spans and return each one's outputs, in span order.
///
/// Span 0 runs inline on the coordinator's own thread — it is an
/// `onda-compact-{n}` worker that is going to do a share of the merge itself,
/// and it is already accounted for by `num_compaction_threads`. The rest run on
/// scoped threads, so every one of them is joined before this returns, on the
/// error path as much as the happy one.
fn run_spans(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    cmp: &ComparatorRef,
    target: usize,
    inputs: &[Arc<SstHandle>],
    plan: &[Bound<Vec<u8>>],
    frozen: &FrozenJob,
) -> Result<Vec<Vec<SstMeta>>> {
    if plan.len() <= 1 {
        return Ok(vec![run_span(
            db,
            cf,
            cmp,
            target,
            inputs,
            Bound::Unbounded,
            Bound::Unbounded,
            frozen,
        )?]);
    }

    fn bound_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
        match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(key.as_slice()),
            Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
        }
    }
    /// The next span's INCLUSIVE lower bound is this one's exclusive upper
    /// bound: the spans are half-open and share no key.
    fn upper_of(plan: &[Bound<Vec<u8>>], index: usize) -> Bound<&[u8]> {
        match plan.get(index + 1) {
            None => Bound::Unbounded,
            Some(Bound::Included(key)) => Bound::Excluded(key.as_slice()),
            Some(other) => bound_ref(other),
        }
    }

    let mut results: Vec<Result<Vec<SstMeta>>> = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(plan.len() - 1);
        let mut spawn_failure: Option<crate::error::OndaError> = None;
        for index in 1..plan.len() {
            let (lower, upper) = (bound_ref(&plan[index]), upper_of(plan, index));
            let worker = std::thread::Builder::new()
                .name(format!("onda-span-{index}"))
                .spawn_scoped(scope, move || {
                    // A fresh thread defaults to `Foreground`; without this the
                    // bytes a span moves would escape background pacing (0.6).
                    let _io = crate::ioctrl::scoped(crate::ioctrl::IoClass::Compaction);
                    let outcome = run_span(db, cf, cmp, target, inputs, lower, upper, frozen);
                    if outcome.is_err() {
                        frozen.cancel();
                    }
                    outcome
                });
            match worker {
                Ok(handle) => workers.push(handle),
                Err(error) => {
                    // Could not spawn: cancel the siblings already running and
                    // let this job fail rather than silently drop a span's
                    // share of the keyspace.
                    frozen.cancel();
                    spawn_failure = Some(error.into());
                }
            }
        }
        let mut results = vec![{
            let outcome = run_span(
                db,
                cf,
                cmp,
                target,
                inputs,
                bound_ref(&plan[0]),
                upper_of(plan, 0),
                frozen,
            );
            if outcome.is_err() {
                frozen.cancel();
            }
            outcome
        }];
        for worker in workers {
            results.push(worker.join().unwrap_or_else(|_| {
                Err(crate::error::OndaError::Unknown(
                    "compaction span worker panicked".to_string(),
                ))
            }));
        }
        if let Some(error) = spawn_failure {
            results.push(Err(error));
        }
        results
    });

    // Report the FIRST real failure rather than a cancellation caused by it.
    if let Some(position) = results.iter().position(|r| r.is_err()) {
        let mut first = position;
        for (index, result) in results.iter().enumerate() {
            if let Err(error) = result {
                if !is_span_cancellation(error) {
                    first = index;
                    break;
                }
            }
        }
        // A span that FINISHED before its sibling failed has already handed its
        // tables back, so `CompactionOutputBuilder`'s abort-on-drop no longer
        // covers them. Nothing here ever reached the manifest, so they are
        // removed outright — leaving them would leak a file per surviving span,
        // per failed job.
        for outputs in results.iter().flatten() {
            for meta in outputs {
                let klog = cf.klog_path(meta.id);
                db.remove_sst_file(&crate::sst::vlog_path_for(&klog), meta.vlog_size);
                db.remove_sst_file(&klog, meta.klog_size);
            }
        }
        return Err(results.swap_remove(first).unwrap_err());
    }
    Ok(results
        .into_iter()
        .map(|r| r.expect("checked above"))
        .collect())
}

fn is_span_cancellation(error: &crate::error::OndaError) -> bool {
    matches!(error, crate::error::OndaError::Unknown(message)
        if message.starts_with("compaction span cancelled"))
}

fn install_compaction_outputs(
    cf: &Arc<ColumnFamily>,
    cmp: &ComparatorRef,
    level: usize,
    target: usize,
    inputs: &[Arc<SstHandle>],
    new_handles: &[Arc<SstHandle>],
    p: &crate::db::Publish,
) {
    let input_ids: std::collections::HashSet<u64> =
        inputs.iter().map(|table| table.meta.id).collect();
    cf.update_levels(
        |levels| {
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
        },
        p,
    );
}

/// Undo [`install_compaction_outputs`] after a failed snapshot write.
///
/// Reachable only as `catalog_txn`'s rollback closure, and therefore only on the
/// pre-`CAP_MANIFEST_EDITS` path, where the publication necessarily precedes the
/// write. The manifest still names the inputs, so in-memory state must go back
/// to naming them too before the output files are unlinked — otherwise a reader
/// between the two steps sees tables whose bytes are about to disappear. The
/// inputs are re-inserted at their own recorded level rather than restored from
/// a level-set snapshot taken before the install, because a snapshot would also
/// undo whatever a concurrent flush added meanwhile.
fn rollback_compaction_outputs(
    cf: &Arc<ColumnFamily>,
    cmp: &ComparatorRef,
    inputs: &[Arc<SstHandle>],
    installed: &[Arc<SstHandle>],
    p: &crate::db::Publish,
) {
    let output_ids: std::collections::HashSet<u64> =
        installed.iter().map(|table| table.meta.id).collect();
    cf.update_levels(
        |levels| {
            let mut updated: Vec<Vec<Arc<SstHandle>>> = levels
                .iter()
                .map(|tables| {
                    tables
                        .iter()
                        .filter(|table| !output_ids.contains(&table.meta.id))
                        .cloned()
                        .collect()
                })
                .collect();
            for input in inputs {
                let level = input.meta.level as usize;
                if level >= updated.len() {
                    updated.resize(level + 1, Vec::new());
                }
                if updated[level]
                    .iter()
                    .any(|table| table.meta.id == input.meta.id)
                {
                    continue;
                }
                updated[level].push(input.clone());
                // L0 is newest-first and the inputs were its OLDEST files, so
                // appending restores the order; every deeper level is sorted.
                if level > 0 {
                    updated[level].sort_by(|a, b| cmp.compare(&a.meta.min_key, &b.meta.min_key));
                }
            }
            updated
        },
        p,
    );
    for table in installed {
        table.close();
    }
}

/// Retire tables the catalog no longer references: drop their cached
/// descriptors, then hand both file halves to [`DbInner::remove_sst_file`]
/// (invariant 6 — defer-aware, so a checkpoint in progress keeps the bytes, and
/// paced when 0.6-B pacing is configured).
///
/// Called after publication, by compaction with its inputs and by delete-only
/// excise (1.2) with the tables it dropped. Default-tier paths only, exactly as
/// compaction's input removal has always been (AGENTS.md); excise refuses a
/// non-default-tier candidate for that reason.
pub(crate) fn retire_tables(db: &DbInner, cf: &ColumnFamily, inputs: &[Arc<SstHandle>]) {
    for table in inputs {
        table.close();
        db.remove_sst_file(&cf.klog_path(table.meta.id), table.meta.klog_size);
        db.remove_sst_file(
            &format!("{}/{}.vlog", cf.dir(), table.meta.id),
            table.meta.vlog_size,
        );
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
    compact_inputs_inner(db, cf, level, target, inputs, false)
}

/// [`compact_inputs`] for the job classes 0.8 allows to run in parallel spans:
/// capacity-triggered level >= 1 jobs and L0 -> L1 oldest-window jobs, both of
/// which [`pick_compaction`] produces. Everything else — the manual sweep, the
/// in-place bottom rewrite, FIFO — goes through [`compact_inputs`] and stays
/// single-span.
pub(crate) fn compact_inputs_spanned(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    level: usize,
    target: usize,
    inputs: Vec<Arc<SstHandle>>,
) -> Result<()> {
    compact_inputs_inner(db, cf, level, target, inputs, true)
}

fn compact_inputs_inner(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    level: usize,
    target: usize,
    inputs: Vec<Arc<SstHandle>>,
    spans_allowed: bool,
) -> Result<()> {
    let cmp = cf.cmp();
    debug_assert!(target == level || target == level + 1);
    if inputs.is_empty() {
        return Ok(());
    }

    let frozen = FrozenJob::new(db, cf, target, &inputs)?;
    // Spans, permits and boundaries are all settled before any merge starts;
    // nothing below re-reads configuration or level shape.
    let mut permits = db
        .span_permits
        .take(span_budget(db, &frozen, spans_allowed));
    let target_tables: Vec<Arc<SstHandle>> = inputs
        .iter()
        .filter(|table| table.meta.level as usize == target)
        .cloned()
        .collect();
    let plan = plan_spans(
        &cmp,
        &inputs,
        &target_tables,
        frozen.partitioner.as_ref(),
        permits.granted() + 1,
    );
    permits.reduce_to(plan.len().saturating_sub(1));
    let outputs = run_spans(db, cf, &cmp, target, &inputs, &plan, &frozen);
    // Every span worker has been joined by now, on the error path as much as
    // the happy one, so the permits go back before the manifest fsync below
    // rather than after it.
    drop(permits);
    let outputs = outputs?;
    if spans_allowed {
        // Only the bounded job classes report span statistics: the excluded
        // ones are single-span by construction, and letting the whole-level
        // sweep that follows `DB::compact`'s bounded rounds overwrite the
        // numbers would hide what the rounds actually did.
        record_span_stats(cf, &outputs);
    }
    let outputs: Vec<SstMeta> = outputs.into_iter().flatten().collect();

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
    debug_assert!(
        outputs_are_sorted_and_disjoint(&cmp, &outputs),
        "spans produced overlapping or unsorted output — the span bounds are \
         not a partition of the keyspace (level={level} target={target})"
    );
    // Writer::finish has synced every output and its parent directory
    // (catalog_txn step 2). ONE edit retires every input and adds every output;
    // a partially applied compaction is not representable.
    let new_handles: Vec<Arc<SstHandle>> = outputs
        .iter()
        .map(|meta| cf.handle_for(meta.clone()))
        .collect();
    let mut edit = crate::manifest_edit::VersionEdit::default();
    edit.push(crate::manifest_edit::Op::RemoveTables {
        cf: cf.name().to_string(),
        ids: inputs.iter().map(|table| table.meta.id).collect(),
    });
    for meta in &outputs {
        edit.push(crate::manifest_edit::Op::AddTable {
            cf: cf.name().to_string(),
            meta: meta.clone(),
        });
    }
    let publish_handles = new_handles.clone();
    let rollback_handles = new_handles;
    if let Err(error) = db.catalog_txn_with_rollback(
        edit,
        |p| install_compaction_outputs(cf, &cmp, level, target, &inputs, &publish_handles, p),
        |p| rollback_compaction_outputs(cf, &cmp, &inputs, &rollback_handles, p),
    ) {
        // Nothing durable references the outputs: either the append failed and
        // they were never published, or the pre-capability snapshot failed and
        // the rollback above put the inputs back. Either way the file set and
        // the level set agree again once these are gone.
        for meta in &outputs {
            let klog = cf.klog_path(meta.id);
            db.remove_sst_file(&klog, meta.klog_size);
            db.remove_sst_file(&crate::sst::vlog_path_for(&klog), meta.vlog_size);
        }
        return Err(error);
    }
    // Step 5: obsolete inputs are retired only after the edit's fsync.
    retire_tables(db, cf, &inputs);
    Ok(())
}

/// Outputs across every span, concatenated in span order, must be sorted and
/// non-overlapping — the observable half of "the spans partitioned the
/// keyspace". Debug-only: it is O(n) over a handful of tables, but it asserts a
/// property the merge already guarantees rather than checking user input.
fn outputs_are_sorted_and_disjoint(cmp: &ComparatorRef, outputs: &[SstMeta]) -> bool {
    outputs
        .windows(2)
        .all(|pair| cmp.compare(&pair[0].max_key, &pair[1].min_key).is_lt())
}

/// Publish the per-job span statistics (`CfStats::span_count`,
/// `CfStats::span_imbalance_bytes`).
fn record_span_stats(cf: &Arc<ColumnFamily>, per_span: &[Vec<SstMeta>]) {
    use std::sync::atomic::Ordering::Relaxed;
    let bytes: Vec<u64> = per_span
        .iter()
        .map(|span| {
            span.iter()
                .map(|meta| meta.klog_size.saturating_add(meta.vlog_size))
                .sum()
        })
        .collect();
    let imbalance = match (bytes.iter().max(), bytes.iter().min()) {
        (Some(max), Some(min)) => max - min,
        _ => 0,
    };
    cf.span_count.store(per_span.len() as u64, Relaxed);
    cf.span_imbalance_bytes.store(imbalance, Relaxed);
}

/// Does compaction output written into `target` land in the bottom level?
///
/// Lifted verbatim out of [`compact_inputs`], which is the point: the retention
/// and partition-cutting decisions there and the output filter policy in
/// [`cf_writer_opts`] must agree, and re-deriving "bottom" at the second site
/// is how they would drift. It is deliberately not `levels.len() - 1` either —
/// a push-down may create a level beyond the current vector, hence the clamp up
/// to `target + 1`.
pub(crate) fn is_bottom_target(cf: &Arc<ColumnFamily>, target: usize) -> bool {
    cf.with_levels(|levels| target_is_bottom(levels, target))
}

/// The predicate itself, over the level shape alone. Generic so a test can pin
/// it against a shape without standing up a column family; only each level's
/// emptiness is read.
///
/// Both clauses are kept as `compact_inputs` had them. Over the single snapshot
/// this now takes, the second is implied by the first — `target >= num_levels -
/// 1` already puts `target` at or past the last index, so `skip(target + 1)`
/// skips everything — but it is the clause that states the intent, and the
/// original read the level set twice, where it was not implied. So a level that
/// merely *exists* below the target, empty or not, makes the target non-bottom;
/// the levels vector never shrinks, so that is the durable signal.
pub(crate) fn target_is_bottom<T>(levels: &[Vec<T>], target: usize) -> bool {
    let num_levels = levels.len().max(target + 1);
    target >= num_levels - 1 && levels.iter().skip(target + 1).all(|level| level.is_empty())
}

pub(crate) fn cf_writer_opts(
    cf: &Arc<ColumnFamily>,
    cmp: &ComparatorRef,
    target_level: u32,
    bottom: bool,
) -> crate::sst::WriterOptions {
    // Same gate as flush/ingest: the option alone never produces a delta
    // table, the durably-enabled capability is what authorizes it.
    let prefix_delta = cf.opts.enable_prefix_delta_keys
        && cf.ctx.caps.load(std::sync::atomic::Ordering::SeqCst)
            & crate::format::CAPS_PREFIX_DELTA_WRITE
            == crate::format::CAPS_PREFIX_DELTA_WRITE;
    crate::sst::WriterOptions {
        compression: cf.opts.compression_for_level(target_level),
        compression_rules: cf.opts.compression_rules.clone(),
        cmp: cmp.clone(),
        enable_bloom: cf.opts.enable_bloom_filter,
        // The filter policy is decided from the OUTPUT level and the bottom
        // predicate, never inherited from the inputs — which is what makes a
        // filterless table compacted into a non-bottom target regain a filter.
        // Auto allocation measures against the deepest level as the writer
        // is created — or the target itself, when this job creates it.
        bloom_fpr: cf.opts.bloom_fpr_in_shape(
            target_level,
            bottom,
            cf.with_levels(|levels| levels.len().saturating_sub(1))
                .max(target_level as usize) as u32,
        ),
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
        restart_interval: cf.opts.block_restart_interval,
        // Extended (kind-bearing) entries are a table-level property fixed at
        // writer construction, and the aux block that carries range fragments
        // only exists on an extended table. A database holding
        // CAP_RANGE_DELETES therefore writes every new table extended, whether
        // or not this particular one ends up with a fragment — the alternative
        // is knowing the answer before the merge has run. A merge family needs
        // the same layout for kind 4, and compaction is where an operand chain
        // spends most of its life; same gate as flush.
        extended_entries: cf.range_deletes_enabled() || cf.merge_writes_enabled(),
        prefix_delta,
    }
}

fn key_span(tables: &[Arc<SstHandle>], cmp: &ComparatorRef) -> (Vec<u8>, Vec<u8>) {
    let mut min: Option<&[u8]> = None;
    let mut max: Option<&[u8]> = None;
    for t in tables {
        // SPAN bounds, not point bounds: a table's range fragments can reach
        // past its first and last point key (they are clipped to the *output
        // interval*, which extends into the gaps between outputs). A job span
        // built from point bounds alone would leave an input fragment outside
        // it, and that fragment would be lost the moment its owner is dropped.
        let (tmin, tmax) = (t.meta.span_min(cmp), t.meta.span_max(cmp));
        min = Some(match min {
            Some(m) if cmp.compare(m, tmin).is_le() => m,
            _ => tmin,
        });
        max = Some(match max {
            Some(m) if cmp.compare(m, tmax).is_ge() => m,
            _ => tmax,
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

    use std::ops::Bound;

    use super::{
        build_job, gather_target, key_span, oldest_eligible_table, overlap_bytes,
        periodic_candidate, plan_spans, rank_candidates, target_is_bottom, CompactionReason,
        FrozenJob, Retention, VersionRetention, COMPACTION_OUTPUT_BYTES, FIRST_FIT_ORDER,
    };
    use crate::comparator::{default_comparator, CaseInsensitive, ComparatorRef};

    /// A level shape: `levels[i]` stands in for level `i`'s file list, and only
    /// its emptiness matters to the predicate.
    fn shape(counts: &[usize]) -> Vec<Vec<u8>> {
        counts.iter().map(|n| vec![0u8; *n]).collect()
    }

    #[test]
    fn is_bottom_target_requires_deeper_levels_empty() {
        // A deeper level below the target — populated OR merely present and
        // empty — makes the target non-bottom. The levels vector never shrinks,
        // so "a level exists below me" is the durable signal, and this is the
        // case a `levels.len() - 1` rederivation would have gotten wrong in the
        // other direction.
        assert!(!target_is_bottom(&shape(&[0, 3, 2]), 1));
        assert!(!target_is_bottom(&shape(&[0, 3, 0]), 1));
        assert!(!target_is_bottom(&shape(&[0, 3, 0, 0]), 1));
        assert!(!target_is_bottom(&shape(&[2, 0, 0, 1]), 0));
        // The last index is bottom, populated below it or not.
        assert!(target_is_bottom(&shape(&[0, 3, 2]), 2));
        assert!(target_is_bottom(&shape(&[0, 3, 0]), 2));
        assert!(target_is_bottom(&shape(&[0, 0, 3, 0]), 3));
        // A target past the end of the vector is bottom too: `num_levels`
        // clamps up to `target + 1`, so a push-down that creates the level
        // lands in the bottom.
        assert!(target_is_bottom(&shape(&[4]), 1));
        assert!(target_is_bottom(&shape(&[4]), 7));
        // A fresh family is one level, and L0 is its bottom — which is exactly
        // why flush passes `bottom = false` instead of asking.
        assert!(target_is_bottom(&shape(&[0]), 0));
    }

    #[test]
    fn version_retention_preserves_snapshots_and_reclaims_bottom_debris() {
        let mut bottom = VersionRetention::new(true, 10, 100, default_comparator());

        assert_eq!(
            bottom.decide_point(b"a", 12, true, 0),
            Retention::Keep {
                filter_eligible: false
            }
        );
        assert_eq!(
            bottom.decide_point(b"a", 10, false, 0),
            Retention::Keep {
                filter_eligible: true
            }
        );
        assert_eq!(bottom.decide_point(b"a", 9, false, 0), Retention::Drop);
        assert_eq!(bottom.decide_point(b"b", 8, true, 0), Retention::Drop);
        assert_eq!(bottom.decide_point(b"c", 8, false, 99), Retention::Drop);

        let mut upper = VersionRetention::new(false, 10, 100, default_comparator());
        assert_eq!(
            upper.decide_point(b"a", 10, true, 0),
            Retention::Keep {
                filter_eligible: false
            }
        );
        assert_eq!(
            upper.decide_point(b"b", 10, false, 99),
            Retention::Keep {
                filter_eligible: false
            }
        );

        let folded: ComparatorRef = Arc::new(CaseInsensitive);
        let mut custom = VersionRetention::new(false, 10, 100, folded);
        assert!(matches!(
            custom.decide_point(b"A", 10, false, 0),
            Retention::Keep { .. }
        ));
        assert_eq!(custom.decide_point(b"a", 9, false, 0), Retention::Drop);
    }

    // ---- 1.1: kind-aware retention ----------------------------------------

    /// A merge operand at or below `oldest_snapshot` is not "the one version"
    /// the retention rule keeps: run unmodified, the pre-1.1 rule would keep
    /// the newest operand and drop every older one, truncating the chain with
    /// no folding bug at all.
    #[test]
    fn decide_keeps_every_operand_below_snapshot() {
        let merge = crate::format::KIND_MERGE;
        for bottom in [false, true] {
            let mut r = VersionRetention::new(bottom, 100, 0, default_comparator());
            for seq in (1..=6).rev() {
                assert!(
                    matches!(r.decide(b"k", seq, merge, false, 0), Retention::Keep { .. }),
                    "operand at seq {seq} dropped (bottom = {bottom})"
                );
            }
            // The base that terminates the chain is kept once...
            assert!(matches!(
                r.decide_point(b"k", 0, false, 0),
                Retention::Keep { .. }
            ));
        }
    }

    /// Everything *older* than the base still collapses exactly as before: the
    /// base, not the operands, is what sets `emitted_at_or_below_snapshot`.
    #[test]
    fn decide_drops_versions_older_than_the_base() {
        let merge = crate::format::KIND_MERGE;
        let mut r = VersionRetention::new(false, 100, 0, default_comparator());
        assert!(matches!(
            r.decide(b"k", 9, merge, false, 0),
            Retention::Keep { .. }
        ));
        assert!(matches!(
            r.decide_point(b"k", 8, false, 0),
            Retention::Keep { .. }
        ));
        assert_eq!(r.decide_point(b"k", 7, false, 0), Retention::Drop);
        assert_eq!(r.decide(b"k", 6, merge, false, 0), Retention::Drop);
    }

    /// The pre-1.1 behaviour for point kinds, unchanged: a second put at or
    /// below the snapshot is dropped.
    #[test]
    fn decide_drops_second_put_below_snapshot() {
        let mut r = VersionRetention::new(false, 100, 0, default_comparator());
        assert!(matches!(
            r.decide_point(b"k", 9, false, 0),
            Retention::Keep { .. }
        ));
        assert_eq!(r.decide_point(b"k", 8, false, 0), Retention::Drop);
    }

    /// A delete terminating a chain whose operands are still live is that
    /// chain's base; the bottom level must not reclaim it out from under them.
    #[test]
    fn decide_keeps_terminating_delete_while_operands_live() {
        let merge = crate::format::KIND_MERGE;
        let mut r = VersionRetention::new(true, 100, 0, default_comparator());
        assert!(matches!(
            r.decide(b"k", 9, merge, false, 0),
            Retention::Keep { .. }
        ));
        assert!(
            matches!(r.decide_point(b"k", 8, true, 0), Retention::Keep { .. }),
            "the base of a live chain is not bottom debris"
        );
        // A delete with no operands above it is still reclaimed at the bottom.
        assert_eq!(r.decide_point(b"other", 8, true, 0), Retention::Drop);
        // ...and so is one whose chain was terminated by an intervening base.
        let mut r = VersionRetention::new(true, 100, 0, default_comparator());
        assert!(matches!(
            r.decide(b"k", 9, merge, false, 0),
            Retention::Keep { .. }
        ));
        assert!(matches!(
            r.decide_point(b"k", 8, false, 0),
            Retention::Keep { .. }
        ));
        assert_eq!(r.decide_point(b"k", 7, true, 0), Retention::Drop);
    }

    /// Operands carry no TTL in v1, so the bottom TTL drop cannot apply to one
    /// even if a hand-edited byte claimed an expiry.
    #[test]
    fn decide_never_ttl_drops_an_operand() {
        let merge = crate::format::KIND_MERGE;
        let mut r = VersionRetention::new(true, 100, 1_000, default_comparator());
        assert!(matches!(
            r.decide(b"k", 9, merge, false, 1),
            Retention::Keep { .. }
        ));
        // The same expiry on a point kind is reclaimed, as it always was.
        assert_eq!(r.decide_point(b"other", 9, false, 1), Retention::Drop);
    }

    /// A merge operand is not filter-eligible: the bloom answers "is there a
    /// version of this key", which the operand's base (or the newest operand)
    /// already contributes, and a user compaction filter expects a value where
    /// an operand is only half of one.
    #[test]
    fn operand_is_not_filter_eligible() {
        let merge = crate::format::KIND_MERGE;
        let mut r = VersionRetention::new(false, 100, 0, default_comparator());
        assert_eq!(
            r.decide(b"k", 9, merge, false, 0),
            Retention::Keep {
                filter_eligible: false
            }
        );
        assert_eq!(
            r.decide_point(b"k", 8, false, 0),
            Retention::Keep {
                filter_eligible: true
            }
        );
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

    // ---- 0.3: periodic-compaction eligibility -----------------------------

    /// One hour, the interval every eligibility test below measures against.
    const HOUR: i64 = 3_600_000_000_000;

    /// A table carrying periodic age state.
    fn aged_handle(
        cf: &Arc<crate::column_family::ColumnFamily>,
        id: u64,
        level: u32,
        min: &[u8],
        max: &[u8],
        stamp: Option<i64>,
    ) -> Arc<crate::column_family::SstHandle> {
        cf.handle_for(crate::manifest::SstMeta {
            id,
            level,
            klog_size: 1024,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            last_compaction_time: stamp,
            ..crate::manifest::SstMeta::default()
        })
    }

    /// [`aged_handle`] with an explicit klog size, for the picker tests that
    /// need a level to be over or under its capacity.
    fn aged_handle_sized(
        cf: &Arc<crate::column_family::ColumnFamily>,
        id: u64,
        level: u32,
        min: &[u8],
        max: &[u8],
        klog: u64,
        stamp: Option<i64>,
    ) -> Arc<crate::column_family::SstHandle> {
        cf.handle_for(crate::manifest::SstMeta {
            id,
            level,
            klog_size: klog,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            last_compaction_time: stamp,
            ..crate::manifest::SstMeta::default()
        })
    }

    /// A foreign mount that is also old enough to qualify on age alone — the
    /// combination the veto exists for.
    fn aged_foreign_handle(
        cf: &Arc<crate::column_family::ColumnFamily>,
        id: u64,
        level: u32,
        min: &[u8],
        max: &[u8],
        stamp: i64,
    ) -> Arc<crate::column_family::SstHandle> {
        cf.handle_for(crate::manifest::SstMeta {
            id,
            level,
            klog_size: 1024,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            object: Some(format!("cf-default/{:016x}-{id}", 0xfeedu64)),
            last_compaction_time: Some(stamp),
            ..crate::manifest::SstMeta::default()
        })
    }

    /// [`oldest_eligible_table`] over a live column family's levels.
    fn oldest(
        db: &crate::DB,
        cf: &Arc<crate::column_family::ColumnFamily>,
        interval: i64,
        now: i64,
    ) -> Option<(usize, Arc<crate::column_family::SstHandle>)> {
        cf.with_levels(|levels| oldest_eligible_table(&db.inner, levels, interval, now))
    }

    #[test]
    fn periodic_candidate_picks_oldest_eligible() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let now = 100 * HOUR;
        cf.replace_levels(vec![
            vec![
                // Fresh: half an interval old.
                aged_handle(&cf, 1, 0, b"a", b"b", Some(now - HOUR / 2)),
                // Unknown age (legacy table / attached part): never eligible,
                // however long ago it might actually have been written.
                aged_handle(&cf, 2, 0, b"c", b"d", None),
            ],
            vec![
                // Eligible, but not the oldest.
                aged_handle(&cf, 3, 1, b"a", b"b", Some(now - 3 * HOUR)),
                // The oldest LOCAL table — the expected pick.
                aged_handle(&cf, 4, 1, b"c", b"d", Some(now - 9 * HOUR)),
                // Older still, but a foreign mount: this database may not
                // rewrite bytes it did not publish.
                aged_foreign_handle(&cf, 5, 1, b"e", b"f", now - 50 * HOUR),
            ],
        ]);

        let (level, pick) = oldest(&db, &cf, HOUR, now).expect("eligible");
        assert_eq!(level, 1);
        assert_eq!(pick.meta.id, 4);

        // Rewind to just before the third table qualifies and the pick moves to
        // the only remaining eligible one.
        let (level, pick) =
            oldest(&db, &cf, 5 * HOUR, now).expect("one table is still older than five hours");
        assert_eq!((level, pick.meta.id), (1, 4));
        // With an interval nothing has reached, there is no candidate at all.
        assert!(oldest(&db, &cf, 20 * HOUR, now).is_none());
    }

    /// A tie between two levels resolves to the shallower one, whose rewrite
    /// also unblocks what sits above it.
    #[test]
    fn periodic_candidate_breaks_level_ties_top_down() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let now = 100 * HOUR;
        cf.replace_levels(vec![
            Vec::new(),
            vec![aged_handle(&cf, 1, 1, b"a", b"b", Some(now - 4 * HOUR))],
            vec![aged_handle(&cf, 2, 2, b"c", b"d", Some(now - 4 * HOUR))],
        ]);
        let (level, pick) = oldest(&db, &cf, HOUR, now).expect("eligible");
        assert_eq!((level, pick.meta.id), (1, 1));
    }

    /// Failure-matrix row: `clock() < stamp`. A clock that steps backwards must
    /// yield no candidate, no panic, and no negative age treated as huge.
    #[test]
    fn periodic_clock_skew() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let stamp = 100 * HOUR;
        cf.replace_levels(vec![
            Vec::new(),
            vec![aged_handle(&cf, 1, 1, b"a", b"b", Some(stamp))],
        ]);

        // A reading one whole interval BEFORE the stamp.
        assert!(
            oldest(&db, &cf, HOUR, stamp - HOUR).is_none(),
            "a stamp in the future is not eligible"
        );
        // And the extreme: saturating arithmetic, not a wrap into a huge age.
        assert!(
            oldest(&db, &cf, HOUR, i64::MIN).is_none(),
            "an absurdly skewed clock must not wrap into eligibility"
        );
        // Exactly at the stamp is an age of zero, still short of the interval.
        assert!(oldest(&db, &cf, HOUR, stamp).is_none());
        // The boundary itself is inclusive.
        assert!(oldest(&db, &cf, HOUR, stamp + HOUR).is_some());
    }

    /// Zero is the documented "disabled" value, and it disables the walk
    /// entirely — even for a table stamped at the epoch.
    #[test]
    fn periodic_candidate_none_when_interval_zero() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        assert!(
            cf.opts.periodic_compaction_interval.is_zero(),
            "the default"
        );
        cf.replace_levels(vec![
            Vec::new(),
            vec![aged_handle(&cf, 1, 1, b"a", b"b", Some(0))],
        ]);
        assert!(periodic_candidate(&db.inner, &cf, 1_000 * HOUR).is_none());

        // The same levels under a configured interval do yield a candidate, so
        // the assertion above is about the option and not about the fixture.
        let dir2 = tempfile::tempdir().unwrap();
        let (db2, cf2) = picker_db(
            &dir2,
            crate::config::ColumnFamilyConfig {
                periodic_compaction_interval: std::time::Duration::from_secs(3600),
                ..crate::config::ColumnFamilyConfig::default()
            },
        );
        cf2.replace_levels(vec![
            Vec::new(),
            vec![aged_handle(&cf2, 1, 1, b"a", b"b", Some(0))],
        ]);
        assert!(periodic_candidate(&db2.inner, &cf2, 1_000 * HOUR).is_some());
    }

    /// Task 7: age work is the lowest priority. With a level over capacity AND
    /// an eligible old table, the capacity job is the one that runs — a backlog
    /// grows, stale space does not.
    ///
    /// A picker test rather than an end-to-end one: `pick_compaction`'s choice
    /// is the whole claim, and observing it through the background worker would
    /// be a race between two jobs that are both going to happen.
    #[test]
    fn periodic_does_not_preempt_capacity_work() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(
            &dir,
            crate::config::ColumnFamilyConfig {
                periodic_compaction_interval: std::time::Duration::from_secs(3600),
                l1_base_bytes: 1 << 10,
                ..crate::config::ColumnFamilyConfig::default()
            },
        );
        // Enable BEFORE installing the fixture, so the enable-time stamping
        // cannot overwrite the stamps this test depends on.
        db.enable_format_capabilities(crate::format::CAP_PERIODIC_AGE)
            .unwrap();
        let now = 100 * HOUR;
        db.set_clock_for_tests(Arc::new(move || now));

        // L1 is far over its 1 KiB capacity; L2's table is a day past its
        // interval and would be the periodic pick if nothing else were due.
        cf.replace_levels(vec![
            Vec::new(),
            vec![aged_handle_sized(
                &cf,
                1,
                1,
                b"a",
                b"m",
                1 << 20,
                Some(now - HOUR / 2),
            )],
            vec![aged_handle_sized(
                &cf,
                2,
                2,
                b"n",
                b"z",
                16,
                Some(now - 24 * HOUR),
            )],
        ]);

        let (job, guard) = super::pick_compaction(&db.inner, &cf).expect("a job is due");
        assert_eq!(job.reason, CompactionReason::Capacity);
        assert_eq!((job.level, job.target), (1, 2));
        assert_eq!(job.inputs[0].meta.id, 1);
        drop(guard);

        // Bring L1 back inside its capacity and the same call now returns the
        // age-triggered job — so the assertion above is about priority, not
        // about the periodic path being unreachable.
        cf.replace_levels(vec![
            Vec::new(),
            vec![aged_handle_sized(
                &cf,
                1,
                1,
                b"a",
                b"m",
                16,
                Some(now - HOUR / 2),
            )],
            vec![aged_handle_sized(
                &cf,
                2,
                2,
                b"n",
                b"z",
                16,
                Some(now - 24 * HOUR),
            )],
        ]);
        let (job, guard) = super::pick_compaction(&db.inner, &cf).expect("the age job is due");
        assert_eq!(job.reason, CompactionReason::Periodic);
        assert_eq!(
            (job.level, job.target),
            (2, 2),
            "a bottom candidate is rewritten in place, never into a new level"
        );
        assert_eq!(job.inputs.len(), 1);
        assert_eq!(job.inputs[0].meta.id, 2);
        drop(guard);
    }

    /// Once a pass has spent its periodic burst, the picker stops offering age
    /// work but keeps offering capacity work — the burst limit only reorders
    /// age work, it never delays a growing backlog.
    #[test]
    fn periodic_burst_spent_withholds_only_age_work() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(
            &dir,
            crate::config::ColumnFamilyConfig {
                periodic_compaction_interval: std::time::Duration::from_secs(3600),
                l1_base_bytes: 1 << 10,
                ..crate::config::ColumnFamilyConfig::default()
            },
        );
        db.enable_format_capabilities(crate::format::CAP_PERIODIC_AGE)
            .unwrap();
        let now = 100 * HOUR;
        db.set_clock_for_tests(Arc::new(move || now));
        let fixture = |l1_bytes: u64| {
            vec![
                Vec::new(),
                vec![aged_handle_sized(&cf, 1, 1, b"a", b"m", l1_bytes, Some(now))],
                vec![aged_handle_sized(&cf, 2, 2, b"n", b"z", 16, Some(now - 24 * HOUR))],
            ]
        };

        // Only age work due.
        cf.replace_levels(fixture(16));
        assert!(super::pick_compaction_with(&db.inner, &cf, false).is_none());
        let (job, guard) =
            super::pick_compaction_with(&db.inner, &cf, true).expect("the age job is due");
        assert_eq!(job.reason, CompactionReason::Periodic);
        drop(guard);

        // Capacity work is offered whatever the burst state.
        cf.replace_levels(fixture(1 << 20));
        let (job, guard) =
            super::pick_compaction_with(&db.inner, &cf, false).expect("capacity work is due");
        assert_eq!(job.reason, CompactionReason::Capacity);
        drop(guard);
    }

    /// A non-bottom age candidate is an ordinary bounded push-down: source plus
    /// the target tables it overlaps, exactly as capacity work would build it.
    #[test]
    fn periodic_non_bottom_candidate_is_a_bounded_push_down() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = picker_db(
            &dir,
            crate::config::ColumnFamilyConfig {
                periodic_compaction_interval: std::time::Duration::from_secs(3600),
                ..crate::config::ColumnFamilyConfig::default()
            },
        );
        db.enable_format_capabilities(crate::format::CAP_PERIODIC_AGE)
            .unwrap();
        let now = 100 * HOUR;
        db.set_clock_for_tests(Arc::new(move || now));
        cf.replace_levels(vec![
            Vec::new(),
            vec![aged_handle_sized(
                &cf,
                1,
                1,
                b"c",
                b"f",
                16,
                Some(now - 9 * HOUR),
            )],
            vec![
                aged_handle_sized(&cf, 2, 2, b"a", b"d", 16, Some(now)),
                aged_handle_sized(&cf, 3, 2, b"e", b"g", 16, Some(now)),
                // Disjoint from the source span: must NOT be pulled in.
                aged_handle_sized(&cf, 4, 2, b"x", b"z", 16, Some(now)),
            ],
        ]);
        let (job, guard) = super::pick_compaction(&db.inner, &cf).expect("the age job is due");
        assert_eq!(job.reason, CompactionReason::Periodic);
        assert_eq!((job.level, job.target), (1, 2));
        let mut ids: Vec<u64> = job.inputs.iter().map(|t| t.meta.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3], "only the overlapping target tables");
        drop(guard);
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
        assert_eq!(
            rank_candidates(&levels, &cmp, 2, &candidates, 0),
            vec![1, 0]
        );

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

        let (job, _guard) = build_job(&db.inner, &cf, 1, CompactionReason::Capacity)
            .expect("the next-best candidate is usable");
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

        let (job, _guard) =
            build_job(&db.inner, &cf, 0, CompactionReason::Capacity).expect("L0 is compactable");
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
        assert!(build_job(&db.inner, &cf, 1, CompactionReason::Capacity).is_none());
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
        let (job, _guard) = build_job(&db.inner, &cf, 1, CompactionReason::Capacity)
            .expect("a usable candidate exists");
        assert_eq!(job.inputs[0].meta.id, 101);
        assert_eq!(
            cf.compact_cursor.lock().get(&1).cloned(),
            Some(b"b099".to_vec())
        );
    }

    // ---- 0.8: frozen job decisions and span boundary planning --------------

    /// Lower bounds as plain byte vectors, for readable assertions.
    fn lowers(plan: &[Bound<Vec<u8>>]) -> Vec<Option<Vec<u8>>> {
        plan.iter()
            .map(|bound| match bound {
                Bound::Unbounded => None,
                Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
            })
            .collect()
    }

    fn rules(prefixes: &[&str]) -> crate::config::PartitionResolver {
        crate::config::PartitionResolver::Rules(
            prefixes
                .iter()
                .map(|p| crate::config::PartitionRule {
                    prefix: p.as_bytes().to_vec(),
                    name: p.trim_end_matches('/').to_string(),
                })
                .collect(),
        )
    }

    /// A job freezes `bottom`, the snapshot horizon, `now`, the carried entry
    /// time, the partition resolver and the compaction filter ONCE, before any
    /// span opens a reader. Adding a partition rule afterwards must not move
    /// the boundaries this job cuts on — the guarantee
    /// `partition_resolver_snapshot` has always given, pinned at the new seam
    /// where several spans share the snapshot by reference.
    #[test]
    fn frozen_job_is_captured_before_the_merge() {
        let dir = tempfile::tempdir().unwrap();
        let db =
            crate::DB::open(crate::config::Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family(
                "default",
                crate::config::ColumnFamilyConfig {
                    partition_rules: vec![crate::config::PartitionRule {
                        prefix: b"a/".to_vec(),
                        name: "a".to_string(),
                    }],
                    ..crate::config::ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        // Level 0 is this family's bottom, so the job is partition-cutting.
        let inputs = vec![handle(&cf, 1, 0, b"a/1", b"b/9", 100, 0)];
        let frozen = FrozenJob::new(&db.inner, &cf, 0, &inputs).unwrap();
        let partitioner = frozen
            .partitioner
            .clone()
            .expect("a bottom job snapshots the resolver");
        assert_eq!(partitioner.boundary(b"a/1"), Some(&b"a/"[..]));
        assert_eq!(partitioner.boundary(b"b/9"), None);

        db.add_partition_rule(
            &cf,
            crate::config::PartitionRule {
                prefix: b"b/".to_vec(),
                name: "b".to_string(),
            },
        )
        .unwrap();
        // The live resolver sees the new rule; the frozen one must not, or two
        // spans of one job could cut on different boundaries.
        assert_eq!(
            cf.partition_resolver_snapshot().unwrap().boundary(b"b/9"),
            Some(&b"b/"[..])
        );
        assert_eq!(frozen.partitioner.as_ref().unwrap().boundary(b"b/9"), None);
        let cmp = cf.cmp();
        assert_eq!(
            lowers(&plan_spans(
                &cmp,
                &inputs,
                &inputs,
                frozen.partitioner.as_ref(),
                4
            )),
            vec![None],
            "the frozen resolver knows only the `a/` rule, whose boundary is \
             below the job's first key"
        );
    }

    #[test]
    fn plan_spans_uses_partition_boundaries_when_bottom() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        let partitioner = rules(&["a/", "b/", "c/"]);
        // One source table straddling all three partitions, over three
        // partition-clean target tables.
        let inputs = vec![
            handle(&cf, 1, 1, b"a/0", b"c/9", 300, 0),
            handle(&cf, 2, 2, b"a/0", b"a/9", 100, 0),
            handle(&cf, 3, 2, b"b/0", b"b/9", 100, 0),
            handle(&cf, 4, 2, b"c/0", b"c/9", 100, 0),
        ];
        let target: Vec<_> = inputs[1..].to_vec();

        let plan = plan_spans(&cmp, &inputs, &target, Some(&partitioner), 8);
        assert_eq!(
            lowers(&plan),
            vec![None, Some(b"b/".to_vec()), Some(b"c/".to_vec())],
            "cuts must be the partition boundaries themselves"
        );
        // `a/` is the job's first key's boundary and would open an empty span,
        // and no cut ever lands INSIDE a partition.
        for bound in &plan[1..] {
            let Bound::Included(key) = bound else {
                panic!("span lower bounds are inclusive user keys")
            };
            assert_eq!(
                partitioner.boundary(key).map(<[u8]>::to_vec),
                Some(key.clone()),
                "cut {key:?} is not the first key of its partition"
            );
        }
    }

    /// A partitioned job under a non-bytewise comparator stays single-span: the
    /// "a boundary prefix is the first key of its partition" argument is a
    /// bytewise one, and cutting on it under another order could split a part.
    #[test]
    fn plan_spans_will_not_cut_partitions_under_a_custom_comparator() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let folded: ComparatorRef = Arc::new(CaseInsensitive);
        let inputs = vec![
            handle(&cf, 1, 1, b"a/0", b"c/9", 300, 0),
            handle(&cf, 2, 2, b"a/0", b"a/9", 100, 0),
            handle(&cf, 3, 2, b"b/0", b"c/9", 100, 0),
        ];
        assert_eq!(
            lowers(&plan_spans(
                &folded,
                &inputs,
                &inputs[1..],
                Some(&rules(&["a/", "b/", "c/"])),
                8
            )),
            vec![None]
        );
    }

    #[test]
    fn plan_spans_falls_back_to_target_min_keys() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        let inputs = vec![
            handle(&cf, 1, 1, b"a", b"z", 400, 0),
            handle(&cf, 2, 2, b"a", b"f", 100, 0),
            handle(&cf, 3, 2, b"g", b"m", 100, 0),
            handle(&cf, 4, 2, b"n", b"z", 100, 0),
        ];
        let target: Vec<_> = inputs[1..].to_vec();
        assert_eq!(
            lowers(&plan_spans(&cmp, &inputs, &target, None, 8)),
            vec![None, Some(b"g".to_vec()), Some(b"n".to_vec())],
            "the target tables' min_keys are free, sorted split points"
        );

        // With no target tables at all (an L0 -> L1 job into a fresh level) the
        // inputs' own min_keys are the only metadata left to cut on.
        let l0 = vec![
            handle(&cf, 10, 0, b"a", b"m", 100, 0),
            handle(&cf, 11, 0, b"h", b"z", 100, 0),
        ];
        assert_eq!(
            lowers(&plan_spans(&cmp, &l0, &[], None, 8)),
            vec![None, Some(b"h".to_vec())]
        );
    }

    #[test]
    fn plan_spans_dedupes_comparator_equal_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let folded: ComparatorRef = Arc::new(CaseInsensitive);
        // `M` and `m` are distinct byte strings that the CF's comparator calls
        // equal. Two spans divided at both would make the middle one empty and
        // — worse — would put versions of one user key on either side of a
        // boundary the merge does not believe in.
        let inputs = vec![
            handle(&cf, 1, 1, b"a", b"z", 400, 0),
            handle(&cf, 2, 2, b"a", b"f", 100, 0),
            handle(&cf, 3, 2, b"M", b"q", 100, 0),
            handle(&cf, 4, 2, b"m", b"z", 100, 0),
        ];
        let plan = plan_spans(&folded, &inputs, &inputs[1..], None, 8);
        assert_eq!(plan.len(), 2, "M and m must collapse to one boundary");
        let Bound::Included(cut) = &plan[1] else {
            panic!("expected an inclusive cut")
        };
        assert!(folded.compare(cut, b"m").is_eq());
    }

    #[test]
    fn plan_spans_drops_empty_spans() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        // The source table covers `a..f` only; the target tables at `s` and `w`
        // are below the job's span because a wide target table pulled them in.
        // Cutting there would produce spans no input can fill.
        let inputs = vec![
            handle(&cf, 1, 1, b"a", b"f", 400, 0),
            handle(&cf, 2, 2, b"a", b"f", 100, 0),
        ];
        let target = vec![
            inputs[1].clone(),
            handle(&cf, 3, 2, b"s", b"t", 100, 0),
            handle(&cf, 4, 2, b"w", b"z", 100, 0),
        ];
        assert_eq!(
            lowers(&plan_spans(&cmp, &inputs, &target, None, 8)),
            vec![None],
            "no cut has input data on both sides"
        );

        // A boundary equal to the job's first key would open with an empty
        // span too, and one past its last key would close with one.
        let two = vec![
            handle(&cf, 5, 1, b"a", b"z", 400, 0),
            handle(&cf, 6, 2, b"a", b"c", 100, 0),
            handle(&cf, 7, 2, b"d", b"z", 100, 0),
        ];
        assert_eq!(
            lowers(&plan_spans(&cmp, &two, &two[1..], None, 8)),
            vec![None, Some(b"d".to_vec())]
        );
    }

    #[test]
    fn plan_spans_never_exceeds_max() {
        let dir = tempfile::tempdir().unwrap();
        let (_db, cf) = picker_db(&dir, crate::config::ColumnFamilyConfig::default());
        let cmp = cf.cmp();
        let mut inputs = vec![handle(&cf, 1, 1, b"k00", b"k63", 6400, 0)];
        for i in 0..64u64 {
            inputs.push(handle(
                &cf,
                100 + i,
                2,
                format!("k{i:02}").as_bytes(),
                format!("k{i:02}z").as_bytes(),
                100,
                0,
            ));
        }
        let target: Vec<_> = inputs[1..].to_vec();
        for max in [1usize, 2, 3, 4, 8, 16] {
            let plan = plan_spans(&cmp, &inputs, &target, None, max);
            assert!(plan.len() <= max, "max {max} produced {} spans", plan.len());
            assert!(matches!(plan[0], Bound::Unbounded));
            // Strictly ascending, so the spans are a partition of the keyspace.
            for pair in plan.windows(2) {
                let (Bound::Included(a) | Bound::Excluded(a)) = &pair[1] else {
                    panic!("only the first bound is unbounded")
                };
                if let Bound::Included(b) | Bound::Excluded(b) = &pair[0] {
                    assert!(cmp.compare(b, a).is_lt());
                }
            }
        }
        // Zero and one both mean "today's behavior".
        assert_eq!(plan_spans(&cmp, &inputs, &target, None, 0).len(), 1);
    }

    // ---- 0.8: running a job in parallel spans -----------------------------
    //
    // These build their tree by hand and call `compact_inputs_spanned`
    // directly. The alternative — letting the size triggers schedule the job —
    // would make every assertion below a race with whatever the flush-armed
    // background worker picked up first, and "which job was the last one" is
    // not a property any of this should depend on. The configuration therefore
    // triggers nothing at all (`l1_file_count_trigger` and `l1_base_bytes` gate
    // both `should_schedule_compaction` and `pick_compaction`), so the only
    // compaction that ever runs is the one the test asks for.

    fn quiet_span_config() -> crate::config::ColumnFamilyConfig {
        crate::config::ColumnFamilyConfig {
            write_buffer_size: 4 << 20,
            // Small enough that L1 ends up holding many tables, which is what
            // gives the boundary planner real target `min_key`s to cut on.
            target_file_size: 8 << 10,
            l1_file_count_trigger: 1 << 20,
            l1_base_bytes: 1 << 60,
            // Most values land in the vlog, so a span reads two files per input
            // and the oracle covers separated values.
            klog_value_threshold: 48,
            ..crate::config::ColumnFamilyConfig::default()
        }
    }

    fn span_options(dir: &tempfile::TempDir, spans: usize) -> crate::config::Options {
        let mut options = crate::config::Options::new(dir.path().to_str().unwrap());
        options.max_subcompactions = spans;
        // Past what any test asks for, so a test that wants N spans measures
        // the planner rather than the pool.
        options.max_subcompaction_workers = 8;
        options
    }

    fn span_value(round: u32, i: u32) -> Vec<u8> {
        let mut value = format!("v{round:02}-{i:06}-").into_bytes();
        value.resize(64 + (i as usize % 23), b'x');
        value
    }

    /// One flushed L0 table per round, each spanning the whole key range.
    ///
    /// Batched into transactions of `WRITE_BATCH` keys rather than one
    /// auto-committed `put` each: a `put` is a commit, and a commit is a WAL
    /// append, which is where a profile of the span benchmark spent 98% of its
    /// time building the fixture rather than merging it. Each key still gets its
    /// own sequence, so the version chains the merge sees are unchanged.
    const WRITE_BATCH: u32 = 2048;

    fn write_rounds(
        db: &crate::DB,
        cf: &Arc<crate::column_family::ColumnFamily>,
        rounds: std::ops::Range<u32>,
        keys: u32,
    ) {
        for round in rounds {
            let mut written = 0u32;
            while written < keys {
                let end = (written + WRITE_BATCH).min(keys);
                let mut txn = db.begin_with_isolation(crate::config::IsolationLevel::ReadCommitted);
                for i in written..end {
                    txn.put(
                        cf,
                        format!("k{i:06}").as_bytes(),
                        &span_value(round, i),
                        std::time::Duration::ZERO,
                    )
                    .unwrap();
                }
                txn.commit().unwrap();
                written = end;
            }
            db.flush_memtable(cf).unwrap();
        }
    }

    const SPAN_KEYS: u32 = 1200;

    /// A two-level tree with a big L0 -> L1 job pending: three flushed L0
    /// tables merged into a many-table L1 by one ordinary single-span job,
    /// then three more L0 tables left on top of it.
    fn span_fixture(
        dir: &tempfile::TempDir,
        spans: usize,
    ) -> (crate::DB, Arc<crate::column_family::ColumnFamily>) {
        let db = crate::DB::open(span_options(dir, spans)).unwrap();
        let cf = span_fixture_cf(&db, "default");
        (db, cf)
    }

    /// [`span_fixture`]'s column family on its own, for the tests that need two
    /// of them in one database — the span permit pool is DB-wide.
    fn span_fixture_cf(db: &crate::DB, name: &str) -> Arc<crate::column_family::ColumnFamily> {
        let cf = db.create_column_family(name, quiet_span_config()).unwrap();
        write_rounds(db, &cf, 0..3, SPAN_KEYS);
        let l0 = cf.with_levels(|levels| levels[0].clone());
        super::compact_inputs(&db.inner, &cf, 0, 1, l0).unwrap();
        assert!(
            cf.with_levels(|levels| levels[1].len()) >= 4,
            "the fixture's L1 must hold several tables or there is nothing to cut on"
        );
        write_rounds(db, &cf, 3..6, SPAN_KEYS);
        cf
    }

    /// Every table the pending job merges: all of L0 plus all of L1.
    fn pending_inputs(
        cf: &Arc<crate::column_family::ColumnFamily>,
    ) -> Vec<Arc<crate::column_family::SstHandle>> {
        cf.with_levels(|levels| {
            let mut inputs = levels[0].clone();
            inputs.extend(levels.get(1).into_iter().flatten().cloned());
            inputs
        })
    }

    fn scan(
        txn: &crate::Txn,
        cf: &Arc<crate::column_family::ColumnFamily>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut iterator = txn.new_iterator(cf);
        iterator.seek_to_first();
        let mut out = Vec::new();
        while iterator.valid() {
            out.push((iterator.key().to_vec(), iterator.value().to_vec()));
            iterator.next();
        }
        assert!(
            iterator.err().is_none(),
            "scan failed: {:?}",
            iterator.err()
        );
        out
    }

    /// Klog and vlog file names under `dir`'s column family, sorted.
    fn sst_files(dir: &tempfile::TempDir) -> Vec<String> {
        let mut files: Vec<String> = std::fs::read_dir(dir.path().join("cf-default"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".klog") || name.ends_with(".vlog"))
            .collect();
        files.sort();
        files
    }

    fn level_ids(cf: &Arc<crate::column_family::ColumnFamily>) -> Vec<Vec<u64>> {
        cf.with_levels(|levels| {
            levels
                .iter()
                .map(|tables| tables.iter().map(|t| t.meta.id).collect())
                .collect()
        })
    }

    /// What one arm of a 1-vs-N comparison produced.
    struct SpanArm {
        at_snapshot: Vec<(Vec<u8>, Vec<u8>)>,
        at_visible: Vec<(Vec<u8>, Vec<u8>)>,
        span_count: u64,
        span_imbalance_bytes: u64,
        total_output_bytes: u64,
        target: Vec<(Vec<u8>, Vec<u8>)>,
    }

    /// Build the fixture, run its pending job at `spans` spans, and report the
    /// scans taken through a snapshot held across the whole compaction and
    /// afterwards at the visible sequence.
    fn run_arm(spans: usize) -> SpanArm {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = span_fixture(&dir, spans);
        // Held across the compaction: this is the job's `oldest_snapshot`, and
        // every version at or above it must survive the merge whatever the span
        // count.
        let pinned = db.begin();
        let before = scan(&pinned, &cf);

        let inputs = pending_inputs(&cf);
        super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();

        let at_snapshot = scan(&pinned, &cf);
        assert_eq!(
            before, at_snapshot,
            "a pinned snapshot's scan changed across a {spans}-span compaction"
        );
        let visible = db.begin();
        let at_visible = scan(&visible, &cf);
        let stats = cf.stats();
        let target = cf.with_levels(|levels| {
            levels[1]
                .iter()
                .map(|t| (t.meta.min_key.clone(), t.meta.max_key.clone()))
                .collect()
        });
        let total_output_bytes = cf.with_levels(|levels| {
            levels[1]
                .iter()
                .fold(0u64, |sum, t| sum + t.meta.klog_size + t.meta.vlog_size)
        });
        drop(pinned);
        drop(visible);
        db.close().unwrap();
        SpanArm {
            at_snapshot,
            at_visible,
            span_count: stats.span_count,
            span_imbalance_bytes: stats.span_imbalance_bytes,
            total_output_bytes,
            target,
        }
    }

    /// Output tables must be sorted and disjoint, and cover the job's whole
    /// range — the observable half of "the spans partitioned the keyspace".
    fn assert_sorted_and_disjoint(tables: &[(Vec<u8>, Vec<u8>)]) {
        for pair in tables.windows(2) {
            assert!(
                pair[0].1 < pair[1].0,
                "output tables overlap or are unsorted: {:?} then {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn two_span_compaction_matches_one_span() {
        let one = run_arm(1);
        let two = run_arm(2);

        assert_eq!(one.span_count, 1, "one span was asked for");
        assert_eq!(two.span_count, 2, "the job did not split");
        // The output FILES cannot match — boundaries and ids differ — but what
        // the engine returns must, at the pinned snapshot and at the visible
        // sequence alike.
        assert_eq!(one.at_snapshot, two.at_snapshot);
        assert_eq!(one.at_visible, two.at_visible);
        assert_sorted_and_disjoint(&one.target);
        assert_sorted_and_disjoint(&two.target);
        assert_eq!(
            (
                one.target.first().map(|t| t.0.clone()),
                one.target.last().map(|t| t.1.clone())
            ),
            (
                two.target.first().map(|t| t.0.clone()),
                two.target.last().map(|t| t.1.clone())
            ),
            "the spans together covered a different key range"
        );
    }

    /// Span bounds are user keys and the spans are half-open, so every version
    /// of one key lands in exactly one span and therefore in one output table.
    /// The observable form is the target level staying disjoint — a boundary
    /// falling between two versions of a key would leave two tables claiming it
    /// — plus a snapshot pinned across the merge still reading the version it
    /// was pinned to.
    #[test]
    fn all_versions_of_a_key_land_in_one_span() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = span_fixture(&dir, 4);
        // Six versions of every key are live across the inputs; pin the third
        // generation so the merge cannot collapse them to the newest.
        let mut pinned = db.begin();
        let before: Vec<Vec<u8>> = (0..SPAN_KEYS)
            .map(|i| pinned.get(&cf, format!("k{i:06}").as_bytes()).unwrap())
            .collect();

        let inputs = pending_inputs(&cf);
        super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();
        assert!(cf.stats().span_count > 1, "the job ran in one span");

        for (i, expected) in before.iter().enumerate() {
            let key = format!("k{i:06}");
            assert_eq!(
                &pinned.get(&cf, key.as_bytes()).unwrap(),
                expected,
                "the version the snapshot pinned was lost for {key}"
            );
            // Exactly one table in the target level can claim the key.
            let claimants = cf.with_levels(|levels| {
                levels[1]
                    .iter()
                    .filter(|t| {
                        t.meta.min_key.as_slice() <= key.as_bytes()
                            && key.as_bytes() <= t.meta.max_key.as_slice()
                    })
                    .count()
            });
            assert_eq!(claimants, 1, "{key} is claimed by {claimants} tables");
        }
        drop(pinned);
        db.close().unwrap();
    }

    /// The centerpiece. 1, 2 and 4 spans over the same inputs must be
    /// logically indistinguishable at the oldest snapshot and at the visible
    /// sequence, over every entry kind the engine has, under a custom
    /// comparator, and in both memtable layouts.
    #[test]
    fn span_oracle_1_vs_n() {
        /// Puts, deletes, single-deletes, TTL entries and vlog-separated
        /// values, keyed so that a case-insensitive comparator sees ONE user
        /// key where the bytes differ — which is exactly the version chain a
        /// boundary must not cut.
        fn build(
            dir: &tempfile::TempDir,
            spans: usize,
            unified: bool,
        ) -> (crate::DB, Arc<crate::column_family::ColumnFamily>) {
            let mut options = span_options(dir, spans);
            options.unified_memtable = unified;
            // The unified store has no per-CF `flush_memtable`: it rotates on
            // size. Make that size small enough that each generation below
            // overflows it, so both layouts reach the same shape — several
            // flushed L0 tables over a many-table L1.
            options.unified_memtable_write_buffer_size = 32 << 10;
            let db = crate::DB::open(options).unwrap();
            let cf = db
                .create_column_family(
                    "default",
                    crate::config::ColumnFamilyConfig {
                        comparator_name: "case_insensitive".to_string(),
                        ..quiet_span_config()
                    },
                )
                .unwrap();
            let mut round = 0u32;
            let mut generation = |db: &crate::DB, cf: &Arc<crate::column_family::ColumnFamily>| {
                let mut txn = db.begin();
                for i in 0..800u32 {
                    let key = if round.is_multiple_of(2) {
                        format!("K{i:06}")
                    } else {
                        format!("k{i:06}")
                    };
                    match (round + i) % 5 {
                        0 => txn.delete(cf, key.as_bytes()).unwrap(),
                        1 if round > 0 => txn.single_delete(cf, key.as_bytes()).unwrap(),
                        2 => txn
                            .put(
                                cf,
                                key.as_bytes(),
                                &span_value(round, i),
                                std::time::Duration::from_secs(3600),
                            )
                            .unwrap(),
                        _ => txn
                            .put(
                                cf,
                                key.as_bytes(),
                                &span_value(round, i),
                                std::time::Duration::ZERO,
                            )
                            .unwrap(),
                    }
                }
                txn.commit().unwrap();
                db.flush_memtable(cf).unwrap();
                round += 1;
            };
            for _ in 0..3 {
                generation(&db, &cf);
            }
            let l0 = cf.with_levels(|levels| levels[0].clone());
            assert!(
                !l0.is_empty(),
                "the fixture flushed nothing (unified={unified})"
            );
            super::compact_inputs(&db.inner, &cf, 0, 1, l0).unwrap();
            assert!(
                cf.with_levels(|levels| levels[1].len()) >= 3,
                "L1 must hold several tables or there is nothing to cut on"
            );
            for _ in 0..3 {
                generation(&db, &cf);
            }
            (db, cf)
        }

        for unified in [false, true] {
            let mut arms = Vec::new();
            for spans in [1usize, 2, 4] {
                let dir = tempfile::tempdir().unwrap();
                let (db, cf) = build(&dir, spans, unified);
                let pinned = db.begin();
                let before = scan(&pinned, &cf);
                let inputs = pending_inputs(&cf);
                super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();
                let at_snapshot = scan(&pinned, &cf);
                assert_eq!(before, at_snapshot, "unified={unified} spans={spans}");
                let visible = db.begin();
                let at_visible = scan(&visible, &cf);
                let count = cf.stats().span_count;
                drop(pinned);
                drop(visible);
                db.close().unwrap();
                arms.push((spans, count, at_snapshot, at_visible));
            }
            assert_eq!(arms[0].1, 1);
            assert!(arms[1].1 > 1, "the 2-span arm ran {} span(s)", arms[1].1);
            assert!(arms[2].1 > 1, "the 4-span arm ran {} span(s)", arms[2].1);
            assert!(arms[2].1 <= 4);
            for arm in &arms[1..] {
                assert_eq!(
                    arms[0].2, arm.2,
                    "snapshot scan differs at {} spans (unified={unified})",
                    arm.0
                );
                assert_eq!(
                    arms[0].3, arm.3,
                    "visible scan differs at {} spans (unified={unified})",
                    arm.0
                );
            }
        }
    }

    /// A span that fails takes the whole job with it: nothing installed,
    /// nothing persisted, and no output file left behind. Injected at reader
    /// open (a truncated klog) and at value read (a corrupted vlog frame) —
    /// two points a span fails before it ever reaches `Writer::finish` — and
    /// the siblings merging the other spans must be cancelled and cleaned up
    /// with it.
    #[test]
    fn span_failure_leaves_no_partial_install() {
        for truncate in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let (db, cf) = span_fixture(&dir, 4);
            let before_levels = level_ids(&cf);
            let before_files = sst_files(&dir);
            let manifest_before =
                std::fs::read(crate::manifest::manifest_path(dir.path())).unwrap();

            // Break one L1 input. Whichever span owns its key range fails; the
            // rest must stop and remove what they had already written.
            // The NEWEST L0 table: its versions are the ones the merge
            // retains, so its vlog frames are actually read.
            let klog = cf.with_levels(|levels| cf.klog_path(levels[0][0].meta.id));
            if truncate {
                std::fs::write(&klog, b"").unwrap();
            } else {
                // Values are vlog-separated here, so this is the frame a span
                // reads when it asks the merge for a retained value.
                let vlog = crate::sst::vlog_path_for(&klog);
                let mut bytes = std::fs::read(&vlog).unwrap();
                let middle = bytes.len() / 2;
                for byte in &mut bytes[middle..middle + 64] {
                    *byte ^= 0xff;
                }
                std::fs::write(&vlog, bytes).unwrap();
            }
            // The reader may already be open with its index resident; evict
            // every reader so the damage is read from disk rather than served
            // from memory.
            db.set_max_open_readers(0);
            db.set_max_open_readers(64);

            let inputs = pending_inputs(&cf);
            let error = super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs)
                .expect_err("a broken input must fail the job");
            assert!(
                !format!("{error}").is_empty(),
                "truncate={truncate}: no error text"
            );

            assert_eq!(
                level_ids(&cf),
                before_levels,
                "truncate={truncate}: the level set changed after a failed job"
            );
            assert_eq!(
                std::fs::read(crate::manifest::manifest_path(dir.path())).unwrap(),
                manifest_before,
                "truncate={truncate}: the manifest changed after a failed job"
            );
            assert_eq!(
                sst_files(&dir),
                before_files,
                "truncate={truncate}: a failed job left output files behind"
            );
            drop(db);
        }
    }

    /// A catalog-transaction failure after a successful multi-span merge rolls
    /// the in-memory install back — the manifest still names the inputs, so the
    /// level set has to name them too — and removes the outputs it wrote.
    #[test]
    fn span_manifest_persist_failure_rolls_back() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = span_fixture(&dir, 4);
        let before_levels = level_ids(&cf);
        let before_files = sst_files(&dir);
        let manifest_before = std::fs::read(crate::manifest::manifest_path(dir.path())).unwrap();

        // `Manifest::save` writes `MANIFEST.tmp` and renames it. A directory of
        // that name makes the create fail, and nothing else.
        std::fs::create_dir(dir.path().join("MANIFEST.tmp")).unwrap();
        let inputs = pending_inputs(&cf);
        super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs)
            .expect_err("the manifest write must fail");
        std::fs::remove_dir(dir.path().join("MANIFEST.tmp")).unwrap();

        assert_eq!(
            level_ids(&cf),
            before_levels,
            "the in-memory install was not rolled back"
        );
        assert_eq!(
            std::fs::read(crate::manifest::manifest_path(dir.path())).unwrap(),
            manifest_before
        );
        assert_eq!(
            sst_files(&dir),
            before_files,
            "the outputs of the rolled-back install were left on disk"
        );
        // Every input is still readable through the rolled-back level set.
        let txn = db.begin();
        assert_eq!(scan(&txn, &cf).len(), SPAN_KEYS as usize);
        drop(txn);
        drop(db); // the database is poisoned; `close` would surface the same error
    }

    /// Span workers are fresh threads, and a fresh thread defaults to
    /// `Foreground`. Without a scope guard at span entry every byte a span
    /// moves would escape 0.6's background pacing.
    #[test]
    fn span_workers_charge_as_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = Arc::new(crate::ioctrl::RecordingLimiter::default());
        let mut options = span_options(&dir, 4);
        options.io_limiter = Some(recorder.clone());
        let db = crate::DB::open(options).unwrap();
        let cf = db
            .create_column_family("default", quiet_span_config())
            .unwrap();
        write_rounds(&db, &cf, 0..3, SPAN_KEYS);
        let l0 = cf.with_levels(|levels| levels[0].clone());
        super::compact_inputs(&db.inner, &cf, 0, 1, l0).unwrap();
        write_rounds(&db, &cf, 3..6, SPAN_KEYS);

        recorder.clear();
        let inputs = pending_inputs(&cf);
        // The caller thread is a test thread, i.e. `Foreground`; a real
        // coordinator is an `onda-compact-{n}` worker that already carries the
        // class. Scope it here so only the SPAWNED spans are under test.
        {
            let _io = crate::ioctrl::scoped(crate::ioctrl::IoClass::Compaction);
            super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();
        }
        let spans = cf.stats().span_count;
        let compaction = recorder.bytes_for(crate::ioctrl::IoClass::Compaction);
        let foreground = recorder.count_for(crate::ioctrl::IoClass::Foreground);
        db.close().unwrap();

        assert!(spans > 1, "the job ran in one span");
        assert!(compaction > 0, "a multi-span job charged nothing");
        assert_eq!(
            foreground, 0,
            "a span worker escaped its IoClass::Compaction scope"
        );
    }

    /// The imbalance stat is what makes span skew visible: a single span
    /// reports none, and the fixture's even geometry — every round writes every
    /// key — must divide into spans of comparable size.
    #[test]
    fn span_imbalance_is_reported() {
        let one = run_arm(1);
        assert_eq!(one.span_count, 1);
        assert_eq!(
            one.span_imbalance_bytes, 0,
            "a single span cannot be imbalanced"
        );

        // The fixture writes every key in every round, so its spans carry
        // comparable work and the spread stays a fraction of the job.
        let four = run_arm(4);
        assert!(four.span_count > 1);
        assert!(
            four.span_imbalance_bytes < four.total_output_bytes / 2,
            "an even geometry reported {} of {} bytes of skew",
            four.span_imbalance_bytes,
            four.total_output_bytes
        );

        // A skewed geometry: one span's worth of keyspace holds most of the
        // bytes, because only the low keys were ever written more than once.
        let dir = tempfile::tempdir().unwrap();
        let db = crate::DB::open(span_options(&dir, 2)).unwrap();
        let cf = db
            .create_column_family("default", quiet_span_config())
            .unwrap();
        write_rounds(&db, &cf, 0..3, SPAN_KEYS);
        let l0 = cf.with_levels(|levels| levels[0].clone());
        super::compact_inputs(&db.inner, &cf, 0, 1, l0).unwrap();
        // Rewrite only the bottom eighth of the keyspace, with fat values.
        for i in 0..SPAN_KEYS / 8 {
            db.put(
                &cf,
                format!("k{i:06}").as_bytes(),
                &vec![b'w'; 2048],
                std::time::Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        let inputs = pending_inputs(&cf);
        super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();
        let skewed = cf.stats();
        db.close().unwrap();
        assert_eq!(skewed.span_count, 2);
        assert!(
            skewed.span_imbalance_bytes > 0,
            "a deliberately skewed job reported a perfectly even split"
        );
    }

    /// The excluded job classes stay single-span even when the option asks for
    /// eight: a compaction filter (per-key ordering semantics), the whole-level
    /// sweep and in-place bottom rewrite `DB::compact` runs, and FIFO, which
    /// never merges at all.
    #[test]
    fn excluded_jobs_run_single_span() {
        // A compaction filter: eligible job class, excluded family.
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = span_fixture(&dir, 8);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        cf.set_compaction_filter(Some(Arc::new(move |_k: &[u8], _v: &[u8]| {
            seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            crate::column_family::FilterDecision::Keep
        })));
        let inputs = pending_inputs(&cf);
        super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();
        assert_eq!(
            cf.stats().span_count,
            1,
            "a filtered column family must stay single-span"
        );
        assert!(
            calls.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "the filter never ran, so the exclusion was not exercised"
        );
        db.close().unwrap();

        // `DB::compact`: its bounded rounds may span, but the whole-level sweep
        // and the in-place bottom rewrite that follow are a single-span class
        // and never report otherwise.
        let dir = tempfile::tempdir().unwrap();
        let db = crate::DB::open(span_options(&dir, 8)).unwrap();
        let cf = db
            .create_column_family("default", quiet_span_config())
            .unwrap();
        write_rounds(&db, &cf, 0..3, 400);
        db.compact(&cf).unwrap();
        assert_eq!(
            cf.stats().span_count,
            1,
            "the manual sweep must stay single-span"
        );
        db.close().unwrap();

        // FIFO evicts and never merges.
        let dir = tempfile::tempdir().unwrap();
        let db = crate::DB::open(span_options(&dir, 8)).unwrap();
        let cf = db
            .create_column_family(
                "default",
                crate::config::ColumnFamilyConfig {
                    compaction_style: crate::config::CompactionStyle::Fifo,
                    fifo_max_bytes: 8 << 10,
                    write_buffer_size: 8 << 10,
                    ..crate::config::ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        write_rounds(&db, &cf, 0..4, 200);
        db.compact(&cf).unwrap();
        assert_eq!(cf.stats().span_count, 1, "FIFO must stay single-span");
        db.close().unwrap();
    }

    /// Point reads run throughout a 4-span job with the reader cache squeezed,
    /// so readers are evicted and reopened underneath them. Every read must be
    /// correct: the install is one `update_levels` swap, so a reader sees
    /// either all the inputs or all the outputs and never a partial set.
    #[test]
    fn point_reads_during_multi_span_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = span_fixture(&dir, 4);
        db.set_max_open_readers(2);
        let expected: Vec<Vec<u8>> = (0..SPAN_KEYS)
            .map(|i| db.get(&cf, format!("k{i:06}").as_bytes()).unwrap())
            .collect();

        let stop = std::sync::atomic::AtomicBool::new(false);
        let reads = std::sync::atomic::AtomicU64::new(0);
        let inputs = pending_inputs(&cf);
        std::thread::scope(|scope| {
            for _ in 0..3 {
                let (db, cf, stop, reads, expected) = (&db, &cf, &stop, &reads, &expected);
                scope.spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                        for (i, want) in expected.iter().enumerate() {
                            let key = format!("k{i:06}");
                            assert_eq!(
                                &db.get(cf, key.as_bytes()).unwrap(),
                                want,
                                "a read during compaction observed a partial install at {key}"
                            );
                            reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                                break;
                            }
                        }
                    }
                });
            }
            super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let spans = cf.stats().span_count;
        let served = reads.load(std::sync::atomic::Ordering::Relaxed);
        db.close().unwrap();
        assert!(spans > 1, "the job ran in one span");
        assert!(served > 0, "no read ran during the compaction");
    }

    /// The F8.3 regression pin. The span pool is sized independently of the
    /// compaction worker count and a coordinator takes nothing from it: it runs
    /// span 0 on the `onda-compact-{n}` thread it already occupies, which
    /// `num_compaction_threads` already accounts for.
    ///
    /// One shared pool with the coordinator taking a permit silently no-ops at
    /// the defaults — two concurrent jobs would consume both permits as
    /// coordinators and no span worker could ever run. Two concurrent jobs here
    /// each ask for exactly one span worker out of a pool of two, so under that
    /// design one of them would run single-span.
    #[test]
    fn coordinators_do_not_consume_span_permits() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = span_options(&dir, 2);
        options.num_compaction_threads = 2;
        options.max_subcompaction_workers = 0; // derive num_compaction_threads
        let db = crate::DB::open(options).unwrap();
        assert_eq!(
            db.inner.span_permits.available(),
            2,
            "the pool must default to num_compaction_threads"
        );
        let left = span_fixture_cf(&db, "left");
        let right = span_fixture_cf(&db, "right");
        let left_inputs = pending_inputs(&left);
        let right_inputs = pending_inputs(&right);

        let gate = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let db = &db;
            let gate = &gate;
            let job = |cf: &Arc<crate::column_family::ColumnFamily>,
                       inputs: Vec<Arc<crate::column_family::SstHandle>>| {
                gate.wait();
                super::compact_inputs_spanned(&db.inner, cf, 0, 1, inputs).unwrap();
            };
            let right = right.clone();
            let other = scope.spawn(move || job(&right, right_inputs));
            job(&left, left_inputs);
            other.join().unwrap();
        });

        let (left_spans, right_spans) = (left.stats().span_count, right.stats().span_count);
        assert_eq!(
            db.inner.span_permits.available(),
            2,
            "permits were not released when the jobs joined"
        );
        db.close().unwrap();
        assert_eq!(
            (left_spans, right_spans),
            (2, 2),
            "a coordinator consumed a permit: two concurrent jobs each wanted \
             ONE span worker from a pool of two"
        );
    }

    /// A job that cannot have every span it asked for runs fewer, and completes
    /// correctly — permits are never waited on, because that wait would happen
    /// under the job's own range lock.
    #[test]
    fn span_count_degrades_under_permit_pressure() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = span_options(&dir, 4);
        options.max_subcompaction_workers = 1; // one extra thread, DB-wide
        let db = crate::DB::open(options).unwrap();
        let cf = span_fixture_cf(&db, "default");
        let pinned = db.begin();
        let before = scan(&pinned, &cf);

        let inputs = pending_inputs(&cf);
        super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();

        let stats = cf.stats();
        assert_eq!(
            stats.span_count, 2,
            "a job asking for 4 spans with one permit must run 2"
        );
        assert_eq!(
            before,
            scan(&pinned, &cf),
            "the degraded job did not produce the same data"
        );
        assert_eq!(db.inner.span_permits.available(), 1);
        drop(pinned);
        db.close().unwrap();
    }

    /// Wall time of one deliberately large bounded job at 1, 2 and 4 spans, at
    /// a fixed thread and IO budget.
    ///
    /// `#[ignore]`d: it writes and rewrites hundreds of megabytes. Arms
    /// alternate so thermal drift lands on all three, and the median of each is
    /// what is compared — this machine's run-to-run spread is 15-20%.
    ///
    /// ```sh
    /// cargo test --release --lib -- --ignored --nocapture span_scaling
    /// ```
    ///
    /// One CSV line per run: `spans,run,seconds,span_count,imbalance_bytes`.
    #[test]
    #[ignore]
    fn subcompaction_span_scaling_benchmark() {
        let runs: usize = std::env::var("ONDADB_BENCH_RUNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let keys: u32 = std::env::var("ONDADB_BENCH_KEYS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);

        /// One job, timed. The tree is built the same way every time, so the
        /// only difference between arms is how the merge is executed.
        fn one_run(spans: usize, keys: u32) -> (f64, u64, u64) {
            let dir = tempfile::tempdir().unwrap();
            let mut options = span_options(&dir, spans);
            // Fixed budget for every arm: the point is spans, not threads.
            options.num_compaction_threads = 2;
            options.max_subcompaction_workers = 4;
            let db = crate::DB::open(options).unwrap();
            let cf = db
                .create_column_family(
                    "default",
                    crate::config::ColumnFamilyConfig {
                        // ~12 tables per level at this size, so the planner
                        // has boundaries to give four spans.
                        target_file_size: 1 << 20,
                        ..quiet_span_config()
                    },
                )
                .unwrap();
            write_rounds(&db, &cf, 0..3, keys);
            let l0 = cf.with_levels(|levels| levels[0].clone());
            super::compact_inputs(&db.inner, &cf, 0, 1, l0).unwrap();
            write_rounds(&db, &cf, 3..6, keys);

            let inputs = pending_inputs(&cf);
            let started = std::time::Instant::now();
            super::compact_inputs_spanned(&db.inner, &cf, 0, 1, inputs).unwrap();
            let elapsed = started.elapsed().as_secs_f64();
            let stats = cf.stats();
            db.close().unwrap();
            (elapsed, stats.span_count, stats.span_imbalance_bytes)
        }

        let mut by_arm: std::collections::BTreeMap<usize, Vec<f64>> =
            std::collections::BTreeMap::new();
        println!("spans,run,seconds,span_count,imbalance_bytes");
        for run in 0..runs {
            for spans in [1usize, 2, 4] {
                let (seconds, count, imbalance) = one_run(spans, keys);
                println!("{spans},{run},{seconds:.3},{count},{imbalance}");
                by_arm.entry(spans).or_default().push(seconds);
            }
        }
        let median = |mut v: Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            (v[v.len() / 2], v[0], v[v.len() - 1])
        };
        let (base, base_min, base_max) = median(by_arm[&1].clone());
        println!("1 span: median {base:.3}s  min {base_min:.3}  max {base_max:.3}");
        for spans in [2usize, 4] {
            let (m, lo, hi) = median(by_arm[&spans].clone());
            println!(
                "{spans} spans: median {m:.3}s  min {lo:.3}  max {hi:.3}  speedup {:.2}x => {}",
                base / m,
                // The gate: the speedup must exceed the single-span arm's own
                // min-max spread, or it is indistinguishable from noise.
                if base - m > base_max - base_min {
                    "MET"
                } else {
                    "NOT MET"
                }
            );
        }
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

            match build_job(&db.inner, &cf, 1, CompactionReason::Capacity) {
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
                    let span = crate::range_lock::KeyRange::new(min_key.clone(), max_key.clone());
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
