# Phase 0 — runtime and observability track

**Baseline:** ondaDB 0.8.2 (`Cargo.toml` version `0.8.2`, commit `3afc3c1`).
Wave 0 of the August 2026 code review has landed in full
(`docs/code-review-2026-08-resolution.md`); the only open review debt is M3
(`commit_mu` latency, deferred) and M5 (manifest rewrite cost, folded into
feature 2.2). No phase-0 item is gated on a review fix any more.

No item in this phase changes the SSTable, WAL, or manifest **record** format.
0.1 and 0.5 add persisted `ColumnFamilyConfig` fields, which append new tags to
the per-CF config blob — old blobs still decode, because the encoder elides
defaults (`encode_block_size` is the pattern). 0.3 does change the manifest: it
adds an `SstMeta` field, which is why it waits for the 1.0 capability
framework. 0.6 and 0.8's knobs are non-persisted `Options` fields — they
describe the current host, not the stored data.

| # | Feature | Readiness | Effort | Hard dependencies |
| --- | --- | --- | ---: | --- |
| [0.1](features/01-per-level-bloom-fpr.md) | Per-level Bloom policy | ready after 0.10 counters | 1–2 wks | 0.10 for acceptance evidence |
| [0.2](features/02-overlap-aware-compaction-picking.md) | Minimum-overlap-ratio picker | ready | 1–2 wks | overlapping-level fixture generator (built here) |
| [0.3](features/03-periodic-compaction.md) | Periodic compaction | deferred to the capability wave | 2–3 wks | 1.0 (`CAP_PERIODIC_AGE`, new `SstMeta` field) |
| [0.4](features/04-multiget.md) | MultiGet | design settled (sequential v1) | 2–4 wks | 0.10 counters |
| [0.5](features/05-vlog-value-cache.md) | Vlog value cache | ready; needs a block-cache key-domain change | 2–3 wks | 0.10 counters (soft) |
| [0.6](features/06-io-rate-limiter-paced-deletes.md) | IO limiter and paced deletion | design settled | 3–5 wks | none |
| [0.7](features/07-global-memory-budget.md) | Global memtable budget | **rejected** — see the decision record | — | — |
| [0.8](features/08-subcompactions.md) | Parallel spans within one compaction | design settled | 3–5 wks | 0.6's IO classes |
| [0.9](features/09-tailing-iterators.md) | Keyspace-tailing iterator | ready, narrow semantics | 1–2 wks | none |
| [0.10](features/10-perf-context.md) | Per-operation observability | ready, land first | 1–2 wks | none |

**Effort envelope.** Summing the per-feature ranges: the wave-B subset
(0.1, 0.2, 0.4, 0.5, 0.6, 0.8, 0.9, 0.10) is **14–25 dev-weeks**; 0.3 adds
**2–3** once 1.0 lands, for **16–28 dev-weeks** across the whole phase. These
are serialized-implementer numbers, and the maxima are additive — a narrower
budget means dropping items, not assuming the maxima cancel.

## Required order

1. **0.10 first.** Every later acceptance section quotes PerfContext deltas;
   without it the evidence is aggregate CF counters and guesswork.
2. **0.2 and 0.5** next, as separate changes. 0.2 builds the prebuilt
   overlapping-level fixture generator that 0.1's and 0.8's benchmarks reuse.
   0.5 may start once 0.10 task 1 exists (it needs the counter scope, not the
   full wiring).
3. **0.6** in two reviews: (A) IO classes plus the limiter, (B) the deletion
   worker and pacing.
4. **0.4** after 0.10; **0.9** in any free slot; **0.1** last of the reversible
   set (it wants 0.10's counters and 0.2's fixture).
5. **0.8** after 0.6's classes exist, so spans cannot oversubscribe the device.
   Multiple compaction workers already exist at 0.8.2
   (`num_compaction_threads`, default 2, `spawn_workers` in `db.rs`), so that
   half of the old gate is closed.
6. **0.3** only after 1.0 can persist `last_compaction_time` behind a
   capability bit.

0.7 is not scheduled: it is a rejected-with-reasons decision record.

## Phase-wide rules

- New options default to current behavior; zero/negative semantics are
  documented and enforced in `ColumnFamilyConfig::validate` (which returns
  `Result<(), String>`).
- A DB-wide resource feature covers **both** per-CF and unified layouts, or
  refuses the unsupported layout at open.
- **Background-wait rule.** No background wait may hold `commit_mu`,
  `rot`/`state`, or WAL file mutexes. **Explicit exception:** a compaction
  job's own range lock (`lock_job`'s `RangeGuard`) may be held across an
  `IoLimiter::charge` wait. The range lock *is* that job's unit of exclusion —
  it exists to keep other jobs off the same span, and no foreground read or
  write path acquires it. Waiting under it delays only work that was already
  excluded. Every other lock in the inventory
  (`docs/concurrency-and-safety.md`) remains off-limits to a background wait.
- Close, poison, and `stop` wake every new waiter.
- Tests use controllable clocks/limiters; wall-clock sleeps only in a small
  end-to-end smoke test.
- **Identifier contract.** Any Rust name used in a feature document must exist
  in the 0.8.2 tree, or be marked **new** at first use. Names verified for this
  phase and used freely: `pick_compaction`, `build_job`, `gather_target`,
  `lock_job`, `compact_inputs`, `cf_writer_opts`, `writer_opts`,
  `finish_writer_to_handle`, `point_read_sources`, `PointReadSources`,
  `PointReadCandidate::{consider, consider_memtable}`, `consider_sstables`,
  `find_overlapping`, `ColumnFamily::new_iterator`, `Reader::{read_data_block,
  read_data_block_local, find_block, bloom_hash, bloom_may_contain_hash,
  get_unfiltered, read_vlog, read_vlog_into, split_block}`,
  `BlockCache::{get, put, shard_for, enabled, stats}`,
  `DbInner::{remove_sst_file, pause_deletions, resume_deletions, next_file_id,
  mover_running, oldest_snapshot, read_floor_seq}`, `FileDeletionState`,
  `CfStats::{compaction_failures, last_compaction_error}`,
  `record_compaction_failure`, `run_manual`, `run_fifo`, `compact_into`,
  `CompactionOutputBuilder`, `CompactionMerge`, `VersionRetention`,
  `install_compaction_outputs`, `remove_compaction_inputs`, `is_foreign_mount`,
  `refresh_compaction_debt`, `pace_for_compaction_debt`, `DB::reader_memory`.

## Gate (every implementation task, without exception)

```sh
cargo test
cargo test --features unsafe-fastpath
cargo clippy --all-targets
cargo clippy --all-targets --features unsafe-fastpath
```

Check each test binary for the presence of `test result: ok`; never pipe
through `tail` (AGENTS.md). One commit per feature on `roadmap/wave-a`, after
the four commands are green.

## Harness additions (one mode per feature, not speculative)

| Feature | Addition |
| --- | --- |
| 0.1 / 0.5 | miss-heavy and hit-heavy point-read phases; filter/vlog residency output |
| 0.2 | prebuilt overlapping-level fixture generator (reused by 0.1 and 0.8); per-level input/output bytes |
| 0.3 | TTL write phase + idle soak |
| 0.4 | batch size, duplicate ratio, hit ratio knobs |
| 0.6 | concurrent foreground reads during forced compaction; delete storms |
| 0.8 | one deliberately large bounded job; per-job wall time |
| 0.9 | append-only keyspace-tail comparison (explicitly not CDC) |

## Exit criteria

- Every selected feature has focused tests, both feature configs green, docs,
  config wiring, and retained benchmark output.
- Defaults produce no new thread, ticker, allocation, or disk artifact except
  where a feature document explicitly permits a nil-check-only hook.
- Features that miss their gate stay off by default or are rejected with a
  note (0.7 is the worked example).

## Risk register

| Risk | Feature | Mitigation |
| --- | --- | --- |
| Reader-pool eviction shifts Bloom cost | 0.1 | publish resident filter bytes with latency |
| Filter omission is one-way: a table written filterless at a level that later stops being bottom never regains a filter | 0.1 | documented degradation + the re-filter-on-promotion rule in the design |
| Ratio picker starves large-overlap tables | 0.2 | level-debt assertions; rollback is one call site |
| Minimum-score candidate is unusable (foreign-mount veto or held range lock), wedging the level | 0.2 | score-ordered try-loop, not score-then-stop |
| Block-cache key aliasing between klog blocks and vlog frames | 0.5 | `BlockDomain` tag on `BlockKey`; aliasing regression test |
| Vlog admission evicts klog blocks | 0.5 | default 0; publish klog hit-rate delta (default build only — the mmap config serves uncompressed klog blocks outside the cache) |
| Charging bytes ≠ device pressure | 0.6 | measure fsync latency alongside |
| Caller-thread background work (manual compaction, ingest) escapes classification | 0.6 | scope guards at `run_manual`/`Ingestion::finish` entry, not only at worker spawn |
| Span permits starved by coordinators | 0.8 | span pool sized independently; coordinators consume no permit |
| Span skew | 0.8 | sampling fallback; imbalance stat |
| Misuse as CDC | 0.9 | API docs and negative tests |
| Multiple compaction workers each schedule the same periodic CF | 0.3 | `periodic_running` CAS, mirroring `mover_running` |
