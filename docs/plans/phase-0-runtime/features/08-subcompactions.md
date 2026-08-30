# 0.8 — Parallel spans within one compaction

**Readiness:** design settled. **Its only remaining gate is 0.6's IO classes**,
so spans cannot oversubscribe the device. The old multi-worker gate is closed:
at 0.8.2 `spawn_workers` already spawns `num_compaction_threads.max(1)`
consumers (default **2**), each named `onda-compact-{worker}` and sharing a
cloned `Receiver` — the compact channel is multi-consumer today.
**Effort:** 3–5 dev-weeks. **wavesdb counterpart:** 0.8.

## Goal

Cut the wall time of one large bounded job by partitioning its user-key range
into independent half-open spans processed concurrently, with one atomic
install. The output cannot be byte-identical (file boundaries and ids change);
the oracle is **logical scan equality at every relevant snapshot** plus
disjoint, sorted output ranges.

## Baseline (verified at 0.8.2)

- `compact_inputs` runs one single-threaded merge: `CompactionMerge::run` over
  `Vec<SstIterator>` with `smallest_input` selection and `VersionRetention`,
  emitting into one `CompactionOutputBuilder`.
- `CompactionOutputBuilder` already freezes job-wide decisions before the merge
  starts: `oldest_snapshot`, `now`, `carry_entry_time`, and the partitioner —
  `cf.partition_resolver_snapshot()` is taken once, with the comment "Snapshot
  the resolver once so a concurrent rule addition cannot change boundaries
  during this run". It cuts at partition boundaries and `target_file_size`, and
  a `debug_assertions` `finalized_boundaries` set enforces that no partition is
  reopened.
- `install_compaction_outputs` does the atomic `update_levels` swap, sorting
  the target level by `min_key` and carrying a debug-assert that "compaction
  dropped a table that was not one of its inputs".
- `compact_inputs` then calls `db.persist_manifest()?` **before**
  `remove_compaction_inputs` — the durability ordering in AGENTS.md invariant 1.
- `lock_job` holds one range lock per job, covering every input.
- `impl Drop for CompactionOutputBuilder` gives abort-on-drop; `Writer::abort`
  removes a partial file.
- `db.next_file_id()` is an atomic `fetch_add`, so span workers can mint their
  own ids concurrently and safely.

## v1 scope

Supported: capacity-triggered level ≥ 1 jobs, and L0→L1 oldest-window jobs.

Excluded (single span):

- `run_manual`'s whole-range sweep and `compact_into(last, last)` in-place
  bottom rewrites — both take a whole-keyspace range lock and `compact_mu`.
- FIFO (`run_fifo` never merges).
- Jobs with a `CompactionFilterFn`. Note the *reason*: the type is
  `Arc<dyn Fn(&[u8], &[u8]) -> FilterDecision + Send + Sync>`, so thread safety
  is not the issue. The exclusion is about **per-key ordering semantics** — a
  user filter written against a single-threaded, key-ordered traversal would
  start seeing keys in an order that depends on span count, and its documented
  "not snapshot-consistent" caveat would become non-deterministic on top. Lift
  the exclusion only with an explicit filter contract.
- Jobs whose inputs or target span overlap a foreign mount (already vetoed by
  `gather_target`).

## Configuration and resource control

```rust
// Options (DB level), NOT persisted — these describe the current host and
// device, exactly like 0.6's knobs.
pub max_subcompactions: usize,          // 0/1 = current behavior (default 1)
pub max_subcompaction_workers: usize,   // new; 0 => num_compaction_threads
```

Actual spans for a job = `min(max_subcompactions, useful boundaries + 1,
available span permits + 1)`.

**The span pool is sized independently of the coordinator count, and
coordinators consume no permit.** The naive alternative — one DB-wide semaphore
of `num_compaction_threads` permits with the coordinator taking one — silently
no-ops at defaults: with `num_compaction_threads = 2`, two concurrent
compaction jobs consume both permits as coordinators and no span worker can
ever run, so the feature would measure nothing. The coordinator is a
`onda-compact-{n}` thread that is going to do a share of the merge itself; it
is already accounted for by `num_compaction_threads`. Permits gate only the
*extra* threads a job spawns:

- `DbInner::span_permits` (**new**): a semaphore of `max_subcompaction_workers`
  (defaulting to `num_compaction_threads`) permits.
- A job takes `spans - 1` permits (the coordinator runs span 0 inline),
  degrading to fewer spans, and ultimately to one, if permits are unavailable.
  Permits are released when the job's workers join, including on the error
  path.

## Boundary construction (comparator-aware, never a bytewise midpoint)

1. **Bottom partition-cut jobs: partition boundaries are mandatory
   candidates.** ondaDB's partition cuts are the ideal split points, and
   `CompactionOutputBuilder` already refuses to reopen a finalized partition. A
   span may contain several whole partitions and must never split one.
2. Otherwise: target-table `min_key`s first (free and comparator-sorted), then
   input index keys sampled with weight proportional to input bytes, until the
   configured count is reached.
3. Deduplicate comparator-equal boundaries and drop empty spans. Boundaries are
   **user keys**, so every version of one user key lands in exactly one span —
   which is what makes `VersionRetention` correct per span.

## Coordinator and failure handling

- Pick inputs and take one range lock exactly as today. Freeze the job-wide
  decisions once (`oldest_snapshot`, `now`, `carry_entry_time`, partitioner
  snapshot) **before** any worker starts, and pass the frozen set by value.
- Each span worker builds its own `Vec<SstIterator>` over the same immutable
  input handles, seeks to `lower`, stops at `upper`, and owns its
  `CompactionOutputBuilder` (own file ids, own vlog sinks). Reader pins are per
  worker; `Arc<SstHandle>` and the table cache already support concurrent
  positional reads.
- First error sets a shared `AtomicBool` cancel flag, checked once per entry
  block; all workers join; every finished or partial output is removed
  directly, since none reached the manifest — reuse `CompactionOutputBuilder`'s
  abort-on-drop discipline.
- Only after **all** spans succeed: outputs are sorted, checked disjoint and
  partition-clean, installed via **one** `install_compaction_outputs` call, then
  one `persist_manifest`, then `remove_compaction_inputs`. Persist failure
  rolls the in-memory install back before removing outputs — the current
  compaction discipline, unchanged.
- Each span worker sets `IoClass::Compaction` (0.6) at entry via
  `ioctrl::scoped`, since it is a fresh thread and would otherwise default to
  `Foreground`.

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

1. **Extract a single-span merge.** `compaction.rs`: refactor the body of
   `compact_inputs` into `fn run_span(db, cf, cmp, target, inputs, lower:
   Bound<&[u8]>, upper: Bound<&[u8]>, frozen: &FrozenJob) -> Result<Vec<SstMeta>>`
   (**new**), where `FrozenJob` (**new**) carries `bottom`, `oldest_snapshot`,
   `now`, `carry_entry_time`, `partitioner`, and `filter`. `compact_inputs`
   becomes: freeze → `run_span(.., Unbounded, Unbounded, ..)` → install →
   persist → remove. **Zero behavior change.**
   Test first: no new test — the whole existing suite is the assertion, in
   both feature configs. Add
   `compaction.rs::frozen_job_is_captured_before_the_merge` — a unit test that
   mutating partition rules after `FrozenJob` construction does not change the
   boundaries the job cuts on (pinning the existing `partition_resolver_snapshot`
   guarantee at the new seam).
2. **Boundary planning.** `compaction.rs::plan_spans(cmp, inputs, target_tables,
   partitioner, max_spans) -> Vec<Bound<Vec<u8>>>` (**new**), pure over
   metadata.
   Test first: `compaction.rs::plan_spans_uses_partition_boundaries_when_bottom`
   — a partitioned bottom job yields exactly the partition cuts (never a cut
   inside one); `compaction.rs::plan_spans_falls_back_to_target_min_keys`;
   `compaction.rs::plan_spans_dedupes_comparator_equal_boundaries` under a
   custom comparator that equates keys the bytewise comparator does not;
   `compaction.rs::plan_spans_drops_empty_spans`;
   `compaction.rs::plan_spans_never_exceeds_max`.
3. **Two-span execution.** Coordinator that runs span 0 inline and one worker,
   under a deterministic test scheduler; collect outputs and install once.
   Test first: `tests/maintenance.rs::two_span_compaction_matches_one_span` —
   build a CF via 0.2's fixture generator, snapshot a full scan at
   `oldest_snapshot` and at the visible sequence, compact with 1 span and with
   2 spans on identical inputs, assert scan equality and that outputs are
   sorted, disjoint, and cover the same key range.
   Plus `tests/maintenance.rs::all_versions_of_a_key_land_in_one_span` — a key
   with many versions straddling a candidate boundary; assert one output file
   holds them all.
4. **Cancellation and cleanup.** Cancel flag, join-all, abort-on-drop.
   Test first: `tests/maintenance.rs::span_failure_leaves_no_partial_install` —
   fault injection at reader open, mid-block read, writer finish, and sibling
   cancel; after each, assert the level set is unchanged, the manifest is
   unchanged, and no output file remains on disk (the orphan discipline).
   Plus `tests/maintenance.rs::span_manifest_persist_failure_rolls_back` —
   persist fails after a successful multi-span merge; assert the in-memory
   install is rolled back and inputs are still present.
5. **Permits.** `DbInner::span_permits`, `Options::max_subcompactions`,
   `Options::max_subcompaction_workers`.
   Test first (this is the F8.3 regression pin):
   `db.rs::coordinators_do_not_consume_span_permits` — with
   `num_compaction_threads = 2` and `max_subcompactions = 4`, two concurrent
   jobs both obtain span workers (assert the observed span count per job is > 1
   for at least one job); and `db.rs::span_count_degrades_under_permit_pressure`
   — with `max_subcompaction_workers = 1`, a job asking for 4 spans runs 2 and
   completes correctly.
6. **IO class in span workers.** `ioctrl::scoped(IoClass::Compaction)` at span
   entry.
   Test first: `tests/maintenance.rs::span_workers_charge_as_compaction` —
   with 0.6's recording limiter, a 4-span job produces only `Compaction`
   charges.
7. **Stats.** `CfStats::span_count` and `CfStats::span_imbalance_bytes`
   (**new**) — the latter is `max_span_bytes - min_span_bytes` for the last
   job.
   Test first: `tests/maintenance.rs::span_imbalance_is_reported` — a
   deliberately skewed geometry reports a non-zero imbalance; an even one
   reports near zero.
8. **Exclusion enforcement.** Test first:
   `tests/maintenance.rs::excluded_jobs_run_single_span` — a CF with a
   `CompactionFilterFn`, a FIFO CF, `DB::compact`'s manual sweep, and a
   foreign-mount-overlapping job each report `span_count == 1` even with
   `max_subcompactions = 8`.
9. **Full logical oracle.** Test first:
   `tests/maintenance.rs::span_oracle_1_vs_n` — puts, deletes, single-deletes,
   TTL entries, separated vlog values, a custom comparator, both memtable
   layouts, and live snapshots; assert scan equality at `oldest_snapshot` and
   at the visible sequence for 1, 2, and 4 spans, in **both** feature configs.
10. **Race coverage.** Test first:
    `tests/maintenance.rs::point_reads_during_multi_span_compaction` — table
    cache eviction plus concurrent point reads throughout a 4-span job; assert
    every read is correct and the install is atomic (no read ever observes a
    partial output set).
11. **Harness.** One deliberately large bounded job built from 0.2's fixture
    generator; per-job wall time at 1/2/4 spans at fixed thread and IO limits;
    logical write amplification and foreground p99 published alongside.

## Tests (summary)

- Logical scan equality 1-vs-N spans across every value/entry kind, both
  layouts, both feature configs, at multiple snapshots.
- All versions of one user key in one span; partition boundaries never split.
- Faults at every stage: no partial install, no file leak.
- Permits: coordinators consume none; graceful degradation under pressure.
- Excluded job classes stay single-span.
- Span workers are IO-classified.

## Acceptance

Job wall time improves materially at 2 and 4 spans without increasing logical
write amplification or foreground p99 beyond a published bound, at fixed
`num_compaction_threads` and a fixed 0.6 IO limit. Default stays 1.

## Rollback

`max_subcompactions = 1` selects the extracted single-span coordinator —
`run_span` over the whole range, which is task 1's behavior-neutral refactor.
Outputs already written are ordinary SSTables. Nothing persisted changes.
