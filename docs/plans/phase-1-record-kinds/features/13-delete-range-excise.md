# 1.2 — Range tombstones and delete-only excise

**Readiness:** architectural. **Prerequisites:** 1.0 (`CAP_RANGE_DELETES`,
kind 5, the aux block, the `ReplayRecord` enum). Deliver range semantics
first; excise is a separate review. **Effort:** 8–13 dev-weeks.
**Baseline:** ondaDB 0.8.2 (`3afc3c1`). **wavesdb counterpart:** 1.2 — the
flagship; both corrected decisions adopted (no start-key sharding; excise's
full precondition set).

RV-F2 (attach disjointness) is **not** a prerequisite: it landed in 0.8.2
(`250cb78`, tests `attach_mutually_overlapping_staged_tables_uses_l0`,
`attach_by_ref_mutually_overlapping_tables_uses_l0`). What remains is a
verification item in slice 12 — the 0.8.2 staging-overlap check must be
extended to compare *span* bounds, not only point bounds, once a table can
carry range fragments.

## Goal

Represent deletion of comparator interval `[start, end)` once; every read and
transaction path honors its sequence; compaction fragments spans safely; then
remove fully shadowed tables by catalog edit without reading them.

## Why ondaDB wants this specifically

Today a bulk delete writes one tombstone per key (write amp + tombstone
debris), and dropping a range without rewriting requires partition rules
configured *in advance*. ondaDB's part machinery is already a metadata-only
range drop at **partition** granularity (`detach_part` removes tables from the
catalog and moves files, manifest-atomic — `parts.rs:333–339`); excise fills
the **sub-part** granularity, the combination wavesdb's gap analysis calls out
as stronger than Pebble's. Partition cuts are mandatory fragment boundaries;
bottom SSTs already never span partitions (`CompactionOutputBuilder`).

## API and v1 restrictions

```rust
impl Txn { pub fn delete_range(&mut self, cf: &Arc<ColumnFamily>, start: &[u8], end: &[u8]) -> Result<()>; }
impl DB  { pub fn delete_range(&self,    cf: &Arc<ColumnFamily>, start: &[u8], end: &[u8]) -> Result<()>; }
```

- Bounds retained copies, CF comparator, start inclusive / end exclusive
  (half-open; `end` never covered — pin with boundary tests). Nil bounds
  rejected; `start >= end` under the CF comparator is `InvalidArgs`; both ≤ the
  existing key size limits.
- Range tombstones carry no TTL and never use the vlog.
- Overlapping range deletes in one transaction may be unioned at one sequence.
- Persist `CAP_RANGE_DELETES` before first use.

**Own-write overlap is rejected in v1 — because of write *ordering*, not ties.**
Sequences inside one commit are distinct by construction: `apply_prepared`
(`txn.rs:473–502`) assigns `let seq = start + slot as u64;` per slot of
`prepared.order`, and different transactions draw disjoint ranges from
`reserve_seq`. Equal sequences therefore cannot arise, and
`covering seq > point seq` is always well-defined. The real hazard is
`deduplicated_write_order` (`txn.rs:405–429`), which gives a key the slot of
its **first** insertion while taking its **last** value:

```rust
Entry::Vacant(entry)   => { entry.insert(order.len()); order.push(index); }
Entry::Occupied(entry) => { order[*entry.get()] = index; }
```

(pinned by `prepared_write_order_is_last_write_wins_in_first_key_order`). So
`put(k,v1); delete_range(k..z); put(k,v2)` would give the *put* slot 0 (seq
`start`) and the range slot 1 (seq `start+1`) — the range delete would mask
`v2`, which the caller wrote *after* it. That is the reason for the
restriction. (The earlier citation of RV-L1 was stale twice over: RV-L1 was
about transaction-overlay entries tying with committed entries at
`seq == read_seq` in the merge iterator, and it was fixed in `7739691`.)

**The check is precise, not conservative:** reject when a buffered point
write's key falls inside any of the transaction's own range spans **under the
CF comparator** (`start ≤ key < end`), not merely when both kinds are present
in the same transaction. Error: `InvalidArgs` naming the key and the span.

## Representation

**Not** sharded by start key into the point memtable — a lookup for `k` must
find every covering span. Beside the point shards, per `Memtable` (and
`UnifiedStore`, prefixed):

```rust
// new, memtable.rs
impl RangeTombstoneSet {
    fn add(&self, start: &[u8], end: &[u8], seq: u64);
    fn covering_seq(&self, key: &[u8], read_seq: u64) -> Option<u64>;
    fn fragments(&self, lower: &[u8], upper: &[u8]) -> FragmentIter;
    fn is_empty(&self) -> bool;   // the zero-cost gate below
}
```

Ordered by `start` (a `crossbeam_skiplist::SkipMap` or a small sorted
structure — the arena build composes its own); `covering_seq` scans backwards
from the greatest `start ≤ key` while candidates can still cover, bounded by
the set's longest span (tracked as a max on `add`).

**Fragmentation happens at flush and compaction only**: boundaries = sorted
unique starts/ends ∪ partition cuts ∪ output-table clip points; each
consecutive pair emits the seq stack of covering tombstones (newest→oldest,
snapshot-visible versions retained); adjacent fragments with identical stacks
merge.

## Wire format

### Range record in the WAL envelope (kind 5)

Uses 1.0's generic two-slot record with no value:

```
kind = 5 | modifiers = 0 | alen uvarint | blen uvarint | seq uvarint
       | a bytes (= start) | b bytes (= end)
```

No `ttl` (modifiers are 0). `alen`/`blen` are the same slots a put uses for
key/value, so 1.0's exact frame-size precompute needs no change.

**Schema 1 (per-CF WAL):** `start`/`end` are user keys.
**Schema 2 (unified WAL):** **both** bounds carry the 8-byte big-endian cf-id
prefix, applied at encode and stripped at replay exactly as point keys are
(`unified.rs:311–315`, `prefixed`, `split_by_cf`). A span can never cross a
cf-id boundary: both bounds come from one `delete_range` call on one CF, so
they share a prefix by construction — asserted at encode.

Replay delivers `ReplayRecord::RangeDelete { start, end, seq }` (1.0's enum) to
both callbacks: `ColumnFamily::load` (`column_family.rs:491`) routes it to the
CF memtable's `RangeTombstoneSet`; `UnifiedStore::open` (`unified.rs:228`)
strips the prefix from both bounds and routes it to the unified set keyed by
cf id. `Wal::replay`'s `last_seq` accounting includes range records.

### Range-fragment section (aux-block section tag 1)

1.0 defines the aux block (16-byte extended-footer prefix →
`block.rs`-framed block → tagged section list). 1.2 defines section tag `1`:

```
payload := count uvarint | fragment x count            (sorted by start)
fragment := slen uvarint | start | elen uvarint | end
          | nseq uvarint | seq uvarint x nseq          (newest -> oldest)
```

The section is CRC-covered by the enclosing block frame (invariant 4). Legacy
tables and tables with no fragments carry no aux block (`aux_off = aux_len = 0`).

### `SstMeta` range fields — tagged tail `ONDARNG1`

`SstMeta` gains `range_count: u64`, `range_min_seq: u64`, `range_max_seq: u64`,
`range_min_key: Option<Vec<u8>>`, `range_max_key: Option<Vec<u8>>` (all
**new**). They are carried as a **tagged tail**, never appended to the
positional `SstMeta` body — `decode_sstable` (`manifest.rs:359–377`)
initializes optional fields to `None` and relies on tails to fill them, and
appending to the body would break both VERSION-1 readers and the
append-tolerant decode.

```
tag bytes : "ONDARNG1"
payload   : per CF in manifest order:
              count uvarint
              { table_index uvarint | range_count uvarint
                | range_min_seq uvarint | range_max_seq uvarint
                | min_key bytes | max_key bytes } x count
```

Position in tag order: **after `ONDACAP1`, before `ONDAWAL1`**, decoded by
1.0's dispatch loop. `ManifestTailPresence` gains `range: bool` (set when any
table has `range_count > 0`) which **must** be included in `tagged()` — the
same positional-decoder hazard 1.0 documents for `ONDACAP1`: without it the
range bytes are decoded as a partition name section.

## Fragment clipping and table selection

**Binding rule: fragments are clipped to output-table bounds at write time**
(the RocksDB rule), so level-≥1 point-key disjointness — which
`find_overlapping` (`column_family.rs:948–967`) binary-searches on
`meta.max_key`/`meta.min_key`, and which `bottom_overlaps`,
`insert_bottom_sorted` and the attach staging check also rely on — is
preserved. Unclipped range bounds would let two adjacent level-≥1 tables both
cover a key, and the binary search returns at most one of them arbitrarily: a
covering tombstone would be missed and deleted data would resurrect.

Precisely, given a job's outputs `o_1 … o_n` in key order, output `o_i` owns
the half-open interval

```
[ o_i.min_key , o_{i+1}.min_key )     for i < n
[ o_n.min_key , job.span_max ]        for i = n
[ job.span_min , o_2.min_key )        for i = 1   (lower end extended to the job span)
```

intersected with the job span, and every fragment is clipped to the owning
output's interval. The intervals are disjoint and ordered by `min_key`, so
level-≥1 ordering is intact. Consequences to implement explicitly:

- `range_min_key` may be **below** `min_key` for `o_1`, and `range_max_key`
  **above** `max_key` for `o_n` (the gap between an output's last point key
  and the next output's first point key belongs to the earlier output). Both
  stay inside the job span.
- `gather_target` (`compaction.rs:334`) expands the job span to the union of
  the input tables' **span** bounds (`min(min_key, range_min_key)` …
  `max(max_key, range_max_key)`), not only their point bounds. Without this an
  input fragment could reach past the job span and be lost when its owning
  input is dropped.
- **Selection rule at level ≥ 1** (this is the change to
  `point_read_sources`, `column_family.rs:969–987`): `find_overlapping` is
  unchanged and yields the point candidate `i` (or `None`). For range
  coverage the walk additionally consults the **gap owner** — the table at
  `i-1`, or when `find_overlapping` returns `None` the last table with
  `max_key < user_key` — and only when that table has `range_count > 0` and
  `user_key < range_max_key`. At most two tables per level, one binary search,
  and one extra `range_count == 0` branch for every legacy/point-only table.
  L0 is unchanged: every table whose **span** contains the key.

## Read semantics

For one key at `read_seq`:

1. Resolve the newest visible **point** candidate + its seq (existing rules).
2. Resolve the max visible **covering range seq** across: txn overlay,
   unified/per-CF memtables + imms (their `RangeTombstoneSet`s), every L0 table
   whose span contains the key, and per level ≥ 1 the point candidate plus the
   gap owner (selection rule above).
3. Deleted iff `covering seq > point seq`, or there is no point candidate.
   Sequences within a commit are distinct by construction (`apply_prepared`),
   and own-write overlap is rejected, so the comparison is total.

**Zero-cost gate:** every source reports `is_empty` / `range_count == 0`; a CF
that never uses range deletes pays one branch per source (prove
allocation-free with a counter assertion in the test, not by inspection).

Iterators: a per-source fragment cursor (positioned at the greatest
`start ≤ current key`) maintained monotonically forward / retreated backward;
skip a point iff `max covering seq > point seq`. Merge across sources is
max-over-sources (no heap — only the max matters). Fragment cursors hold
**owned** bounds copied out of the source, so invariant 8's pinned-block
lifetimes are untouched. Bounds, snapshots, TTL, point tombstones and custom
comparators compose; block-level elision is a later optimization.

## Transaction conflict model

ondaDB's serialization point is coarser than wavesdb's stripes: `commit_mu`
already guards Snapshot/Serializable check→apply — `txn.rs:588–592`,
`let _guard = if needs_check || self.isolation == IsolationLevel::Serializable
{ Some(db.commit_mu.lock()) } else { None };` where `needs_check` covers
`Snapshot | Serializable`. **ReadCommitted does not take `commit_mu` today.**

v1 guard: commits **containing a range delete** take `commit_mu` — at
Snapshot/Serializable they would anyway; ReadCommitted range-commits are
extended to take it. Point-only commits keep today's behavior exactly.

*This makes the deferred review item RV-M3 (`commit_mu` latency) measurably
worse for range commits*, and that is accepted: range commits are the rare,
bulk operation, and the alternative is a per-key conflict domain the engine
does not have. The `range-delete` harness phase measures it (delete latency
p50/p99 with and without concurrent point writers) so M3's eventual fix has a
baseline.

**Committed-span index** (per DB, **new**): point markers + range intervals
newer than the oldest active snapshot. A range writer conflicts with any
overlapping marker with `seq > its read_seq`; a point writer additionally
checks newer covering range markers (`peek_seq`, `column_family.rs:1035`,
stays for points — it sees max seq regardless of kind). Markers insert only
after full installation; pruning removes everything below
`DbInner::oldest_snapshot()` (`db.rs:254`) when no active txn can reference
them.

**Lock order (add to `docs/concurrency-and-safety.md`'s inventory in the same
change):** `DbInner::span_index` (Mutex, **new**) sits immediately **after**
`DbInner::commit_mu` and **before** `DbInner::manifest_mu`. It is acquired
while `commit_mu` is held (marker insert) and alone (pruning); it is never
acquired before `commit_mu` on the commit path.

**Capacity wait happens before `commit_mu`.** The memory cap waits range
writers rather than dropping markers, so the wait must not be performed under
`commit_mu` — that would stall every Snapshot/Serializable commit
database-wide and could convoy against the pruner. A range commit calls
`span_index.reserve(n)` (blocking, **no other lock held**) before acquiring
`commit_mu`; the reservation is consumed at insert and released on every
commit exit path including failure.

Serializable makes no predicate-locking claims beyond this (phantom caveat
unchanged, documented on `IsolationLevel::Serializable`).

## Compaction and GC

Merge point entries + fragment stacks over the job span. A fragment drops only
when: its seq ≤ `oldest_snapshot`, bottom treatment proves nothing older
resurfaces (`compact_inputs`' `bottom` flag, `compaction.rs:928`), and **no
foreign mount overlaps its span** (`is_foreign_mount`, `compaction.rs:426` — a
private module `fn` today; excise and the read path need it `pub(crate)`).
Otherwise emit clipped fragments per the clipping rule above.

Range sections and point outputs install atomically
(`install_compaction_outputs`, `compaction.rs:850`); a partition-cut output
never contains a fragment crossing a partition; vlog pointers pass through
untouched. `VersionRetention::decide` is unaffected by range fragments — they
live in the aux section, not the point stream — but the point stream *is*
filtered against the fragment stack before `decide` sees it, so a point
shadowed by a droppable fragment is dropped without being counted as the
"one version at or below `oldest_snapshot`".

`FlushMerge` (`memtable.rs:1049`, driven by `write_l0_streaming`,
`column_family.rs:861`) and `write_l0` (`:884`) both emit the flushing
memtable's `RangeTombstoneSet` as fragments; `ingest_l0` (`:905`) refuses
range fragments in v1 (external files are point-only) with `InvalidArgs`.

## Excise deliverable

Only durable fragments installed in an SST justify a metadata drop. For a
candidate table (or contiguous set):

```text
fragment.start ≤ table.min_key   AND   table.max_key < fragment.end
table.max_seq < fragment.seq ≤ oldest_snapshot
union of covering fragments gap-free over [table.min_key, table.max_key]
every owner of that union lies OUTSIDE the dropped set
```

The last line is the contiguous-set rule: for a single table it reduces to
"`owner(fragment) ≠ the table itself` — never drop the only durable owner",
but for a set where each member owns part of the covering union, dropping the
whole set would destroy the tombstones that justify the drop. Compute the
union's owner ids first and require `owners ∩ dropped = ∅`.

Vetoes: foreign mounts (`is_foreign_mount`), overlapping `range_locks`
holders, in-flight parts operations, **shared-tier tables** (ondaDB-specific:
shared tiers are delete-free — the S3 publication rule beside the
foreign-mount veto; note that `remove_compaction_inputs` is default-tier-only,
AGENTS.md).

The transaction:

1. acquire the range lock (`lock_partition_span`, `parts.rs:67`, generalized to
   a key span — it reduces to `cf.range_locks.acquire_blocking(KeyRange)`,
   `parts.rs:95,97`);
2. revalidate coverage / snapshot / mount state under the current locks;
3. `cf.remove_tables(&ids)` (**new** — see below);
4. `persist_manifest` while the fragment owners remain catalogued;
5. publish;
6. `remove_sst_file` per file (`db.rs:344`; **never** a bare
   `fs::remove_file` — invariant 6), defer-aware and 0.6-paced.

**Persist failure fail-stops (poisons) the DB.** The in-memory view was
already mutated at step 3, so it cannot be "left untouched"; restoring the
handles would be a new capability with its own races. This matches the
existing precedent exactly — `detach_part` does `cf.remove_bottom_tables(&ids);
self.inner.persist_manifest()?;` (`parts.rs:338–339`) and relies on
`persist_manifest` poisoning. The failure-matrix row is reworded accordingly.

**`remove_tables` is new and level-agnostic.** `remove_bottom_tables`
(`column_family.rs:1534–1542`) edits **only** `levels.last_mut()`, but fully
shadowed tables live in L0 and intermediate levels too — indeed a bulk range
delete's most valuable excise targets are mid-level tables.

```rust
// new, column_family.rs — level-agnostic; `remove_bottom_tables` stays for parts.rs
pub(crate) fn remove_tables(&self, ids: &[u64]) -> usize {
    let mut s = self.state.write();
    let mut removed = 0;
    for lvl in s.levels.iter_mut() {
        let before = lvl.len();
        lvl.retain(|h| !ids.contains(&h.meta.id));   // stable: preserves min_key order
        removed += before - lvl.len();
    }
    removed
}
```

`retain` is stable, so the per-level `min_key` sort established at load
(`column_family.rs:477–479`) — and depended on by `find_overlapping`'s binary
search, `bottom_overlaps` and `insert_bottom_sorted` — survives. Pin that with
a test rather than an inline assertion.

Run excise as a **pre-pass in the picker** (before capacity work — covered
tables are free to drop) and as `DB::excise_covered(cf) -> Result<usize>`
(**new**) for operators and tests. V1 does no partial table split.

## Slices

1. Fragment structure (`RangeTombstoneSet`, `FragmentIter`) + reference-model
   property tests.
2. Option/capability enable; API validation incl. the precise own-write
   overlap check.
3. Envelope kind 5 in the WAL (both schemas) + memtable sets + replay through
   `ReplayRecord::RangeDelete`, incl. the unified cf-prefix on both bounds.
4. Span index + capacity reservation + commit guard; conflict tests at all five
   isolation levels; `concurrency-and-safety.md` inventory update.
5. Aux-section writer/reader + footer/`SstMeta`/`ONDARNG1` tail;
   detached-reader tests.
6. Point `get` masking incl. the gap-owner selection rule.
7. Forward/reverse iterator masking (oracle tests).
8. Flush fragmentation (`FlushMerge`, `write_l0`, `write_l0_streaming`) incl.
   partition cuts; `ingest_l0` refusal.
9. Compaction propagation / clip / drop / mount rules + `gather_target` span
   expansion + crash tests.
10. Excise picker pre-pass + transaction + `remove_tables` + fault injection.
11. Checkpoint/backup/clone/parts/S3 paths or explicit refusal;
    `attach_part`/`attach_part_by_ref` validate incoming range sections and
    compare **span** bounds in the staging-overlap check.
12. Stats (`CfStats::range_deletes/fragments/span_markers/excised_*`),
    PerfContext, docs (`formats.md` tail + aux block, `parts-and-tiers.md`),
    harness `range-delete` phase.

## Implementation tasks

One commit per task after the 4-command gate; tests written first.

1. **`RangeTombstoneSet` + fragments.** New in `memtable.rs`.
   Tests first (in-module): `covering_seq_finds_longest_span()`,
   `covering_seq_respects_read_seq()`, `end_bound_is_exclusive()`,
   `fragments_split_at_unique_boundaries()`,
   `adjacent_fragments_with_equal_stacks_merge()`,
   `empty_set_reports_is_empty()`.
   Property test: random spans vs a brute-force oracle over a small key space.

2. **API + validation.** `Txn::delete_range`, `DB::delete_range`,
   `ColumnFamilyConfig` gate.
   Tests first in `tests/db.rs`: `delete_range_rejects_reversed_bounds()`,
   `delete_range_rejects_empty_bounds()`,
   `delete_range_requires_capability()`,
   `own_write_inside_own_range_is_rejected()` (assert the error names key and
   span), `own_write_outside_own_range_is_allowed()` (the precision case —
   `put(a)` + `delete_range(m..z)` in one txn must succeed).

3. **WAL kind 5, schema 1.** Tests first in `wal.rs`:
   `range_record_round_trips()`, `range_record_golden_bytes()` (new fixture
   `tests/fixtures/phase1/wal_v2_range_schema1.bin`),
   `range_torn_frame()` (torn frame containing a range record → whole frame
   dropped, `Ok`).

4. **WAL kind 5, schema 2 + replay.** Tests first in `tests/unified.rs`:
   `unified_range_record_prefixes_both_bounds()`,
   `unified_range_replay_strips_prefix_from_both_bounds()`,
   `unified_range_span_never_crosses_cf_prefix()` (assert the encode-side
   assertion holds for adversarial bounds).
   Then wire `ReplayRecord::RangeDelete` through `column_family.rs:491` and
   `unified.rs:228`.

5. **Span index + commit guard.** New `DbInner::span_index`.
   Tests first in `tests/db.rs`:
   `range_commit_conflicts_with_overlapping_point_write()`,
   `point_write_conflicts_with_newer_covering_range()`,
   `read_committed_range_commit_takes_commit_mu()` (observable via a
   deterministic interleaving test, not by inspecting the lock),
   `span_index_capacity_waits_before_commit_mu()` (a range writer blocked on
   capacity must not block a concurrent point commit — assert the point commit
   completes while the range writer waits),
   `span_markers_prune_below_oldest_snapshot()`,
   `span_reopen()` (reopen with no active txns → index empty).
   Then update `docs/concurrency-and-safety.md`'s lock inventory.

6. **Aux section + `SstMeta` tail.** Tests first in `tests/sst.rs` and
   `manifest.rs`: `range_section_round_trips()`,
   `range_section_golden_bytes()`, `range_section_bad_crc_is_corruption()`,
   `sst_meta_range_tail_round_trips()`,
   `range_tail_only_manifest_emits_all_positional_sections()` (the
   `ManifestTailPresence::tagged()` hazard, mirroring 1.0's caps test),
   `legacy_table_reports_range_count_zero()`.

7. **Point read masking.** Tests first in `tests/db.rs`:
   `point_read_masked_by_memtable_range()`,
   `point_read_masked_by_sst_fragment()`,
   `gap_owner_table_is_consulted()` — construct a level-1 pair where the
   covering fragment lives in the table *left* of the key and
   `find_overlapping` returns `None`; assert the key reads as deleted,
   `no_range_cf_allocates_nothing_on_read()` (counter assertion).

8. **Iterator masking.** Tests first in `tests/db.rs`:
   `forward_iter_skips_covered_keys()`, `reverse_iter_skips_covered_keys()`,
   `iter_range_mask_matches_point_get_oracle()` (random histories, both
   directions, both feature configs), `bounded_iter_respects_range_and_bounds()`.

9. **Flush fragmentation.** Tests first: `flush_emits_fragments()`,
   `flush_fragments_cut_at_partition_boundaries()`,
   `streaming_flush_matches_batch_flush_fragments()` (`FlushMerge` vs
   `write_l0`), `ingest_l0_refuses_range_fragments()`.

10. **Compaction.** Tests first: `fragments_clipped_to_output_intervals()`
    (assert `range_min_key`/`range_max_key` against the interval rule, incl.
    the `o_1` lower and `o_n` upper extensions),
    `gather_target_expands_to_input_span_bounds()`,
    `fragment_survives_until_bottom()`,
    `fragment_not_dropped_over_foreign_mount()`,
    `level1_span_intervals_stay_disjoint()` (property test over random
    compactions — the disjointness invariant the read path depends on),
    `range_flush_crash()`, `range_compact_crash()`.

11. **`remove_tables`.** Tests first in `column_family.rs`:
    `remove_tables_removes_from_every_level()`,
    `remove_tables_preserves_min_key_order()` (assert the level is still sorted
    and `find_overlapping` still finds the right table),
    `remove_tables_returns_removed_count()`.

12. **Excise.** Tests first in `tests/maintenance.rs`:
    `excise_drops_fully_covered_table()`,
    `excise_refuses_when_table_owns_its_own_covering_fragment()`,
    `excise_refuses_contiguous_set_owning_its_union()` (the new
    contiguous-set rule), `excise_refuses_shared_tier_table()`,
    `excise_refuses_foreign_mount()`,
    `excise_crash_before_unlink()`, `excise_crash_before_persist()`,
    `excise_persist_fails_poisons()`.

13. **Surfaces pass.** Checkpoint/backup/clone/parts/S3, stats, PerfContext,
    docs, harness phase — per the phase-plan checklist.

## Failure matrix (selected)

| Point | Behavior | Test |
| --- | --- | --- |
| torn range frame in WAL | whole frame dropped (frame CRC covers the payload — invariant 3) | `range_torn_frame` |
| flush w/ range section, crash before manifest | orphan klog swept (default-tier sweep, `76cdae4`); WAL replays ranges | `range_flush_crash` |
| compaction crash before manifest | outputs orphaned, inputs intact | `range_compact_crash` |
| excise: crash after manifest, before unlink | table present, uncatalogued → default-tier orphan sweep (named-tier/S3 gaps stay open, AGENTS.md) | `excise_crash_before_unlink` |
| excise: crash before manifest | old catalog; tombstone still masks | `excise_crash_before_persist` |
| excise: persist error | **DB poisoned and fail-stopped**; the mutated in-memory view is moot; a reopen sees the pre-excise catalog | `excise_persist_fails_poisons` |
| reopen with no active txns | span index empty (correct) | `span_reopen` |

## Acceptance

`range-delete` harness phase (bulk deletes fully/partially covering tables):
categorical reduction in read/rewrite bytes on full coverage; delete latency
p50/p99 with and without concurrent point writers (the RV-M3 baseline);
point/scan p99 with 0/10/1000 fragments published; time-to-space-reclaim. Zero
failed invariant/fault tests. Range semantics may ship opt-in before excise.

Both feature configurations green (`unsafe-fastpath` compiles a separate arena
`RangeTombstoneSet` and the mmap read path).

## Rollback

Once range sections exist, disabling new calls does not clear the capability or
remove the need for range-aware readers (documented).
