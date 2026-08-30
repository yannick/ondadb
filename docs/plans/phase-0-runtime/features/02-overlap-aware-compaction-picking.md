# 0.2 — Minimum-overlap-ratio compaction picking

**Readiness:** ready. **Effort:** 1–2 dev-weeks. **wavesdb counterpart:** 0.2
(design transfers as-is; deltas below).

## Goal

When level `i ≥ 1` is over capacity, prefer the source table that rewrites the
least target-level data per source byte, while preserving sweep-cursor
fairness, the foreign-mount veto, and the property that a blocked candidate
never wedges the level. RocksDB's MinOverlappingRatio has been its default
picker since 2019 with write amp "dropped by more than half" at Facebook scale
— motivation, not a gate.

## Baseline (verified at 0.8.2)

- `pick_compaction` scores levels most-overfull-first, then calls
  `build_job(db, cf, level)` per level until one returns a job.
- `build_job`'s level ≥ 1 branch: candidates are `levels[level]` filtered of
  foreign mounts (`is_foreign_mount`); `start` is derived from
  `cf.compact_cursor` (a `Mutex<HashMap<usize, Vec<u8>>>` on `ColumnFamily`) by
  finding the first candidate whose `min_key` is `>=` the stored cursor; then a
  single wrapping sweep takes the **first** candidate whose `gather_target`
  succeeds *and* whose `lock_job` acquires. On success the cursor advances:
  `cf.compact_cursor.lock().insert(level, pick.meta.max_key.clone())`. Its own
  comment states the intent: "One full sweep from the cursor, wrapping once, so
  a blocked candidate never wedges the level."
- `gather_target(db, cf, target, min_key, max_key, inputs)` collects exactly
  the `levels[target]` tables intersecting `[min_key, max_key]` **and** applies
  the foreign-mount veto in the same `with_levels` closure: it returns `None`
  if any overlapping target table is a foreign mount.
- `SstMeta` carries `klog_size` and `vlog_size`, so overlap bytes are one sum.
- Levels ≥ 1 are comparator-sorted and disjoint (`build_job`'s own comment), so
  a two-pointer pass over (candidates, target) is sound.
- The L0 branch takes the oldest `l1_file_count_trigger` window; that ordering
  is a correctness invariant (newest-first shadowing) — **excluded** from
  scoring.

## Design

For each non-mounted candidate `c` in `levels[level]`:

```text
overlap_bytes(c) = Σ (klog_size + vlog_size) over levels[target] tables
                   intersecting [c.min_key, c.max_key]
score(c)         = overlap_bytes(c) / max(1, c.klog_size + c.vlog_size)
```

- Whole-intersecting-table counting is deliberate: the job rewrites the whole
  target table, not a geometric fraction of its span.
- Compare ratios by integer cross-multiplication in `u128` — no float
  instability, no overflow.
- **Ordering, not selection.** Sort candidates ascending by score; ties break
  by position in the current cyclic order (cursor-relative), then by lowest
  `meta.id` for deterministic fixtures. Then run **today's try-loop over that
  order**: for each candidate call `gather_target`, `continue` on `None`, then
  `lock_job`, `continue` on `None`, and on success insert the cursor and
  return.

  Picking the minimum and stopping would wedge the level: the minimum-score
  candidate can be unusable either because `gather_target` vetoes it (a foreign
  mount overlaps the *target* span — a condition the candidate filter does not
  screen, since it only checks the source table) or because `lock_job` finds
  the range already held by a concurrent job. The current sweep exists
  precisely to survive both. The change is which order the sweep visits
  candidates in; the loop's structure and its cursor discipline are unchanged.
- Cursor semantics unchanged: it advances only after a usable pick, and still
  stores the picked table's `max_key`.
- Score the whole compactable level in one two-pointer pass (both lists are
  comparator-sorted). If that ever shows up in profiles, fall back to scoring a
  bounded window after the cursor — the try-loop then covers the rest.

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

1. **Pure overlap helper.** `compaction.rs`: add
   `fn overlap_bytes(levels: &[Vec<Arc<SstHandle>>], cmp: &ComparatorRef,
   target: usize, min_key: &[u8], max_key: &[u8]) -> u64` (**new**) — a pure
   function over an already-borrowed level slice, so it can run inside the same
   `with_levels` closure as the scoring pass. **The foreign-mount veto does not
   move**: it stays fused into `gather_target`'s closure, which is what
   actually builds the job's input set. `overlap_bytes` is advisory scoring
   only, and may report bytes for a span `gather_target` will later veto —
   that is fine, the try-loop handles it.
   Test first: `compaction.rs::overlap_bytes_sums_intersecting_target_tables` —
   hand-built `levels` with known `klog_size`/`vlog_size`; assert zero for a
   disjoint span, the single-table sum for a contained span, the multi-table
   sum for a straddling span, and inclusive-boundary behavior (a target table
   whose `max_key` equals the span's `min_key` counts).
2. **Score + ordering helper.** `compaction.rs`: add
   `fn rank_candidates(levels, cmp, level, target, candidates: &[Arc<SstHandle>],
   start: usize) -> Vec<usize>` (**new**) returning candidate indices ordered by
   `(score asc, cyclic position from start asc, meta.id asc)`, comparing scores
   by `u128` cross-multiplication.
   Test first: `compaction.rs::rank_candidates_orders_by_overlap_ratio` — a
   level where the geometrically-first candidate has the largest overlap and a
   later one has almost none; assert the low-overlap candidate ranks first.
   Plus `compaction.rs::rank_candidates_breaks_ties_deterministically` — two
   candidates with identical scores and identical cyclic distance rank by
   `meta.id`; and `rank_candidates_uses_u128_not_float` — sizes near `u64::MAX`
   rank without panic or wraparound.
3. **Swap the sweep order in.** `compaction.rs::build_job`, level ≥ 1 branch
   only: replace `for k in 0..candidates.len() { let idx = (start + k) % .. }`
   with `for idx in rank_candidates(..)`. Body unchanged
   (`gather_target` → `continue`, `lock_job` → `continue`, cursor insert,
   return).
   Test first: `compaction.rs::build_job_skips_unusable_minimum_score_candidate`
   — construct a level whose minimum-score candidate's target span is covered
   by a foreign mount; assert `build_job` returns a job for the *next*-best
   candidate rather than `None`. Plus
   `compaction.rs::build_job_l0_branch_is_unchanged` — pin the L0 path's chosen
   input set against the pre-change expectation.
4. **Cursor invariants.** No new code.
   Test first: `compaction.rs::cursor_advances_only_on_usable_pick` — when
   every candidate is vetoed, `build_job` returns `None` and
   `cf.compact_cursor` is unchanged; when one succeeds, the cursor holds that
   table's `max_key`.
5. **Fixture generator (shared asset).** `tests/` support module (**new**,
   e.g. `tests/support/levels.rs`) plus the `../bench` phase: build a CF with a
   configurable overlapping-level geometry (level sizes, overlap skew, key
   distribution) deterministically from a seed. 0.1's multi-level benchmark and
   0.8's large-bounded-job phase both consume this.
   Test first: `tests/maintenance.rs::level_fixture_is_deterministic` — same
   seed twice produces identical `SstMeta` id/min_key/max_key/size sequences.
6. **Property test.** Test first:
   `compaction.rs::picked_candidate_minimizes_score_among_usable` — random
   level geometries (seeded), assert the returned job's source table has a
   score `<=` every candidate that `gather_target` would have accepted and
   `lock_job` could have taken; and that the job's range lock covers the union
   span of its inputs.
7. **Regression + benchmark.** Run `tests/ingest_arms_compaction.rs` and
   `tests/sustained_writes.rs`; then the fixture benchmark: compaction bytes
   written per ingested byte, ≥5 runs, 1-vs-N levels. Retain the raw output.

## Tests (summary)

- Deterministic pick assertions on hand-built levels with skewed overlap.
- Foreign-mount veto still skips candidates (source *and* target side); cursor
  still advances only on usable picks; `refresh_compaction_debt` semantics
  unchanged.
- L0 branch behavior pinned byte-for-byte in input selection.
- Fuzz: random geometries; picked file minimizes score among usable
  candidates; job stays range-locked over its true span.
- Both feature configs.

## Acceptance

On the fixture workload, compaction write amplification (output bytes /
ingested bytes) improves by more than the baseline's own min–max spread across
≥5 runs; no regression in `tests/ingest_arms_compaction.rs` or the
sustained-writes tests.

## Rollback

One call site: `build_job` reverts to the `(start + k) % len` sweep. The helpers
become dead code and are deleted with it.
