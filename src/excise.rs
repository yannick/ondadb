//! Delete-only excise (1.2): retire a whole SSTable by catalog edit, without
//! reading a byte of it, when durable range-tombstone fragments prove every key
//! it holds is already deleted.
//!
//! This is the sub-part granularity counterpart to
//! [`DB::detach_part`](crate::DB::detach_part). `detach_part` drops a
//! *partition*'s bottom-level tables because an operator named the partition;
//! excise drops *any* table, at *any* level, because the data speaks for
//! itself. Neither reads the tables it removes.
//!
//! # Preconditions
//!
//! A candidate table `T` is excisable when durable fragments — fragments
//! installed in an SSTable's aux section, never a memtable's live set — satisfy
//! all of:
//!
//! ```text
//! fragment.start <= T.min_key   AND   T.max_key < fragment.end   (per fragment)
//! T.max_seq < fragment.seq <= oldest_snapshot                    (per fragment)
//! the union of qualifying fragments is gap-free over [T.min_key, T.max_key]
//! every OWNER of that union lies outside the dropped set
//! ```
//!
//! The first line generalizes to a union: no single fragment has to span `T`,
//! only the union has to, gap-free. The last line is the contiguous-set rule:
//! for one table it reads "never drop the only durable owner of the tombstone
//! that justifies the drop"; for a set where each member owns part of the
//! covering union, dropping the whole set would destroy exactly the evidence.
//! Owners are computed first and the offending members are dropped from the
//! candidate set until the intersection is empty.
//!
//! **v1 additionally refuses a candidate that carries fragments of its own.**
//! The precondition set above establishes that `T`'s *points* are dead; it says
//! nothing about the tables `T`'s *own* fragments mask. A fragment at level `L`
//! shadows data at every level below `L`, so unlinking `T` could resurrect a
//! third table's data that `T` — and only `T` — was hiding. Refusing such a
//! candidate makes the owner rule hold by construction (an owner carries
//! fragments and is therefore never itself a candidate) and costs nothing in
//! the case excise exists for: a bulk `delete_range` lands its fragments in a
//! shallow level and the fully shadowed point-only tables below it are exactly
//! what this reclaims.
//!
//! # Vetoes
//!
//! Foreign mounts (this database must never unlink another's publication, and
//! must not reason about one's contents), tables not on the default tier
//! (obsolete-file deletion is default-tier-only — AGENTS.md — so excising an
//! S3-resident table would leak the object), shared-tier tables specifically
//! (delete-free publications), an overlapping range-lock holder, and any
//! in-flight parts operation.
//!
//! # Transaction
//!
//! 1. plan, lock-free, over a level snapshot;
//! 2. acquire the candidates' key span through
//!    [`parts::try_lock_key_span`](crate::parts::try_lock_key_span) — the key-span
//!    generalization of `lock_partition_span` — and give up rather than block;
//! 3. **revalidate** the plan under that lock;
//! 4. one `RemoveTables` edit through [`DbInner::catalog_txn`], whose fsync is
//!    the commit point, publishing through
//!    [`ColumnFamily::remove_tables`](crate::column_family::ColumnFamily::remove_tables);
//! 5. retire the files through [`DbInner::remove_sst_file`] (invariant 6:
//!    defer-aware and 0.6-B paced — never a bare `fs::remove_file`).
//!
//! A failed step 4 **fail-stops the database**: the level set was already the
//! candidate one by the time the publish closure ran, so there is nothing to
//! "leave untouched", and a reopen reads the pre-excise catalog. This is
//! `detach_part`'s precedent unchanged.

use std::collections::HashSet;
use std::sync::Arc;

use crate::column_family::{ColumnFamily, SstHandle};
use crate::comparator::ComparatorRef;
use crate::db::DbInner;
use crate::error::Result;
use crate::range_lock::KeyRange;

/// One durable fragment, flattened to the interval and the single sequence
/// that matters for excise, plus the table that owns it.
struct Cover {
    start: Vec<u8>,
    end: Vec<u8>,
    /// Greatest sequence of the fragment's stack that is at or below the oldest
    /// live snapshot. A stack entry above that watermark describes a delete
    /// some live reader cannot see yet, and must not justify unlinking bytes.
    seq: u64,
    owner: u64,
}

/// A table the plan proposes to drop, with the owners of its covering union.
struct Candidate {
    handle: Arc<SstHandle>,
    owners: Vec<u64>,
}

/// Collect every durable fragment of `handles`, flattened to [`Cover`]s and
/// sorted by `start`.
///
/// Fragments whose whole stack sits above `oldest` contribute nothing and are
/// dropped here rather than filtered at every use.
fn covers(cmp: &ComparatorRef, handles: &[Arc<SstHandle>], oldest: u64) -> Result<Vec<Cover>> {
    let mut out = Vec::new();
    for th in handles {
        if !th.meta.has_ranges() {
            continue;
        }
        for frag in th.reader()?.range_fragments() {
            let Some(seq) = frag.seqs.iter().copied().filter(|s| *s <= oldest).max() else {
                continue;
            };
            out.push(Cover {
                start: frag.start.clone(),
                end: frag.end.clone(),
                seq,
                owner: th.meta.id,
            });
        }
    }
    out.sort_by(|a, b| cmp.compare(&a.start, &b.start));
    Ok(out)
}

/// The owners of a gap-free covering union of `[min_key, max_key]`, or `None`
/// when no such union exists.
///
/// `covers` is sorted by `start`, which is what makes one forward pass both
/// sufficient and decisive: once a gap opens, no later interval can start
/// earlier, so the answer is settled.
fn covering_owners(
    cmp: &ComparatorRef,
    covers: &[Cover],
    min_key: &[u8],
    max_key: &[u8],
    table_max_seq: u64,
) -> Option<Vec<u64>> {
    let mut owners: Vec<u64> = Vec::new();
    // Exclusive end covered so far; `None` until the first interval lands.
    let mut reach: Option<&[u8]> = None;
    for c in covers {
        // `table.max_seq < fragment.seq <= oldest_snapshot`. The upper half was
        // applied when the cover was built.
        if c.seq <= table_max_seq {
            continue;
        }
        if cmp.compare(&c.end, min_key).is_le() {
            continue; // ends before the table begins
        }
        let need = reach.unwrap_or(min_key);
        if cmp.compare(&c.start, need).is_gt() {
            return None; // gap, and starts only grow from here
        }
        if reach.is_none_or(|r| cmp.compare(&c.end, r).is_gt()) {
            reach = Some(&c.end);
            owners.push(c.owner);
        }
        // `max_key` is inclusive and `end` exclusive, so covering it means
        // `max_key < end`.
        if cmp.compare(max_key, reach.expect("just set")).is_lt() {
            owners.sort_unstable();
            owners.dedup();
            return Some(owners);
        }
    }
    None
}

/// Whether `meta` is one this database may never unlink or reason about.
fn vetoed(db: &DbInner, cf: &ColumnFamily, meta: &crate::manifest::SstMeta) -> bool {
    if crate::compaction::is_foreign_mount(db, meta) {
        return true;
    }
    // Shared tiers are delete-free publications (A2); named non-shared tiers
    // are refused too, because obsolete-file deletion resolves default-tier
    // paths only and would leak the object rather than reclaim it.
    if meta.tier.is_some() {
        return true;
    }
    // A foreign mount overlapping the candidate's span means part of this key
    // range is another database's publication; this database's fragments say
    // nothing about it, so it does not get to decide the range is dead.
    let cmp = cf.cmp();
    let (lo, hi) = (meta.span_min(&cmp), meta.span_max(&cmp));
    cf.with_levels(|levels| {
        levels.iter().flatten().any(|th| {
            crate::compaction::is_foreign_mount(db, &th.meta)
                && cmp.compare(lo, th.meta.span_max(&cmp)).is_le()
                && cmp.compare(th.meta.span_min(&cmp), hi).is_le()
        })
    })
}

/// Plan the excisable set over one level snapshot. Takes no lock the caller
/// does not already hold and mutates nothing.
fn plan(db: &DbInner, cf: &Arc<ColumnFamily>) -> Result<Vec<Candidate>> {
    let oldest = db.oldest_snapshot();
    let handles: Vec<Arc<SstHandle>> =
        cf.with_levels(|levels| levels.iter().flatten().cloned().collect());
    if !handles.iter().any(|th| th.meta.has_ranges()) {
        return Ok(Vec::new());
    }
    let cmp = cf.cmp();
    let covers = covers(&cmp, &handles, oldest)?;
    if covers.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates: Vec<Candidate> = Vec::new();
    for th in &handles {
        // A table carrying fragments is never a candidate — see the module
        // docs. This is also what makes the owner rule below hold by
        // construction rather than by luck.
        if th.meta.has_ranges() || vetoed(db, cf, &th.meta) {
            continue;
        }
        let Some(owners) = covering_owners(
            &cmp,
            &covers,
            &th.meta.min_key,
            &th.meta.max_key,
            th.meta.max_seq,
        ) else {
            continue;
        };
        candidates.push(Candidate {
            handle: th.clone(),
            owners,
        });
    }

    // The contiguous-set rule: no member of the dropped set may own part of
    // the union justifying any member's drop. Removing an offender can only
    // shrink the offender set, so the fixpoint is reached in at most one pass
    // per candidate.
    loop {
        let dropped: HashSet<u64> = candidates.iter().map(|c| c.handle.meta.id).collect();
        let offenders: HashSet<u64> = candidates
            .iter()
            .flat_map(|c| c.owners.iter().copied())
            .filter(|id| dropped.contains(id))
            .collect();
        if offenders.is_empty() {
            return Ok(candidates);
        }
        candidates.retain(|c| !offenders.contains(&c.handle.meta.id));
    }
}

/// The key span an excise of `candidates` must hold against compaction and the
/// part operations.
fn span_of(cf: &ColumnFamily, candidates: &[Candidate]) -> Option<KeyRange> {
    let cmp = cf.cmp();
    let spans: Vec<(&[u8], &[u8])> = candidates
        .iter()
        .map(|c| (c.handle.meta.span_min(&cmp), c.handle.meta.span_max(&cmp)))
        .collect();
    KeyRange::union(spans, &cmp)
}

/// Drop every table whose keys durable range tombstones already delete.
///
/// Returns the number of tables removed from the catalog. `Ok(0)` is the
/// ordinary answer and covers every veto: excise is opportunistic and never
/// reports "nothing to do" as an error.
pub(crate) fn excise_covered(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<usize> {
    if db.opts.read_only {
        return Ok(0);
    }
    db.poison.check()?;
    // One relaxed load skips the whole feature for a family that never enabled
    // range deletes.
    if !cf.range_deletes_enabled() {
        return Ok(0);
    }
    // A part operation is mid-flight somewhere in this database: its tables are
    // being moved, hard-linked or re-catalogued, and its own range lock does
    // not cover every phase of that. Try again later.
    if db.parts_in_flight() {
        return Ok(0);
    }
    let candidates = plan(db, cf)?;
    if candidates.is_empty() {
        return Ok(0);
    }
    let Some(range) = span_of(cf, &candidates) else {
        return Ok(0);
    };
    // Non-blocking on purpose: an overlapping range-lock holder is a veto, not
    // something to wait behind. Compaction and the part operations are both
    // free to rewrite what excise would have dropped.
    let Some(_guard) = crate::parts::try_lock_key_span(cf, range) else {
        return Ok(0);
    };

    // Revalidate under the lock. Everything the plan read — the level set, the
    // snapshot floor, the mount state — could have changed while the lock was
    // being taken, so the set that is actually dropped is the intersection.
    let revalidated = plan(db, cf)?;
    let confirmed: HashSet<u64> = revalidated.iter().map(|c| c.handle.meta.id).collect();
    let doomed: Vec<Arc<SstHandle>> = candidates
        .into_iter()
        .filter(|c| confirmed.contains(&c.handle.meta.id))
        .map(|c| c.handle)
        .collect();
    if doomed.is_empty() {
        return Ok(0);
    }
    let ids: Vec<u64> = doomed.iter().map(|h| h.meta.id).collect();

    // ONE edit retires every table; its fsync is the commit point. A crash
    // after it leaves uncatalogued files the default-tier orphan sweep
    // collects; a crash before it leaves the tables catalogued and the
    // tombstones still masking them.
    let edit =
        crate::manifest_edit::VersionEdit::new(vec![crate::manifest_edit::Op::RemoveTables {
            cf: cf.name().to_string(),
            ids: ids.clone(),
        }]);
    db.catalog_txn(edit, |p| {
        cf.remove_tables(&ids, p);
    })?;

    let bytes: u64 = doomed
        .iter()
        .map(|h| h.meta.klog_size + h.meta.vlog_size)
        .sum();
    // Step 5, after publication: defer-aware and paced (invariant 6).
    crate::compaction::retire_tables(db, cf, &doomed);
    cf.note_excised(doomed.len() as u64, bytes);
    crate::compaction::refresh_compaction_debt(db, cf);
    Ok(doomed.len())
}

/// The picker's excise pre-pass: covered tables are free to drop, so they go
/// before any capacity work that would otherwise pay to rewrite them.
pub(crate) fn pre_pass(db: &Arc<DbInner>, cf: &Arc<ColumnFamily>) -> Result<()> {
    excise_covered(db, cf).map(|_| ())
}

impl crate::db::DB {
    /// Retire every table of `cf` whose keys durable range tombstones already
    /// delete, by catalog edit — without reading one of their blocks.
    ///
    /// Returns the number of tables removed. Background compaction runs the same
    /// pass before it considers capacity work, so this exists for operators who
    /// want the space back *now* (and for tests). It is opportunistic: `Ok(0)` is
    /// the answer whenever nothing qualifies or a veto applies (a concurrent
    /// compaction holding an overlapping range, an in-flight part operation, a
    /// foreign mount, a table off the default tier), never an error.
    ///
    /// A long-lived snapshot pins the floor this pass measures against, so a
    /// database that always has one open reclaims nothing until it closes — the
    /// same rule compaction's tombstone dropping already obeys.
    ///
    /// # Errors
    ///
    /// Propagates a poisoned or failed catalog transaction. A failed one has
    /// **fail-stopped the database** (the level set was already the post-excise
    /// one when the edit's fsync failed); a reopen reads the pre-excise catalog.
    pub fn excise_covered(&self, cf: &Arc<ColumnFamily>) -> Result<usize> {
        excise_covered(&self.inner, cf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::Bytewise;

    fn cmp() -> ComparatorRef {
        Arc::new(Bytewise)
    }

    fn cover(start: &str, end: &str, seq: u64, owner: u64) -> Cover {
        Cover {
            start: start.as_bytes().to_vec(),
            end: end.as_bytes().to_vec(),
            seq,
            owner,
        }
    }

    #[test]
    fn a_single_spanning_fragment_covers_the_table() {
        let c = [cover("a", "z", 50, 7)];
        assert_eq!(
            covering_owners(&cmp(), &c, b"b", b"y", 10),
            Some(vec![7]),
            "one fragment spanning the whole table"
        );
    }

    #[test]
    fn an_older_fragment_does_not_cover() {
        let c = [cover("a", "z", 5, 7)];
        assert_eq!(
            covering_owners(&cmp(), &c, b"b", b"y", 10),
            None,
            "fragment.seq must exceed the table's max_seq"
        );
    }

    #[test]
    fn adjacent_fragments_union_gap_free() {
        let c = [cover("a", "m", 50, 7), cover("m", "z", 50, 8)];
        assert_eq!(
            covering_owners(&cmp(), &c, b"b", b"y", 10),
            Some(vec![7, 8]),
            "a half-open pair meeting at m leaves no gap"
        );
    }

    #[test]
    fn a_gap_in_the_union_refuses() {
        let c = [cover("a", "m", 50, 7), cover("n", "z", 50, 8)];
        assert_eq!(
            covering_owners(&cmp(), &c, b"b", b"y", 10),
            None,
            "m..n is uncovered"
        );
    }

    #[test]
    fn the_end_bound_must_pass_the_inclusive_max_key() {
        let c = [cover("a", "y", 50, 7)];
        assert_eq!(
            covering_owners(&cmp(), &c, b"b", b"y", 10),
            None,
            "ends are exclusive, so max_key == end is NOT covered"
        );
    }

    #[test]
    fn a_redundant_fragment_contributes_no_owner() {
        let c = [cover("a", "z", 50, 7), cover("b", "c", 50, 9)];
        assert_eq!(
            covering_owners(&cmp(), &c, b"b", b"y", 10),
            Some(vec![7]),
            "the inner fragment extends nothing, so table 9 is not an owner"
        );
    }
}
