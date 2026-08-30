# 0.3 — Periodic compaction with durable age state

**Readiness:** deferred until 1.0 provides the `CAP_PERIODIC_AGE` capability
bit (`1 << 5`) and the manifest can carry a new `SstMeta` field behind it.
**Effort:** 2–3 dev-weeks after 1.0. **wavesdb counterpart:** 0.3, whose two
corrections are adopted verbatim because ondaDB has the identical traps.

## Goal

Revisit tables after a configured interval so an otherwise **idle** database
reclaims expired TTL entries, tombstones, and obsolete versions. Today ondaDB
reclaims only inside a compaction, and background compactions are triggered
only by L0 file count (`schedule_compaction_after_flush`, whose
`should_schedule_compaction` also forces a schedule for `CompactionStyle::Fifo`)
or by level bytes (`pick_compaction`). An idle DB never reclaims anything short
of a manual `DB::compact`.

## Corrected metadata model (the two traps)

1. `SstMeta.max_entry_time` **cannot** be the periodic clock. Its own doc
   comment says compaction "carries forward the maximum over its inputs so
   re-compacting cold data does not make it look freshly written. Drives the
   age gate of the part mover (`TierRule::min_age`)". The carry-forward is
   `CompactionOutputBuilder::carry_entry_time`; the consumer is
   `parts.rs::eligible_part_target` via `TierRule::min_age`. Reusing it would
   either make a periodic output instantly re-eligible (a loop) or reset cold
   data's age (breaking tier placement).
2. "Eligible one interval after open" is **not restart-safe**: open time is not
   durable, so a frequently-restarted DB never becomes eligible. Instead the
   capability-enable transition stamps every local table whose field is `None`
   with the enable time, **inside the same manifest write** that persists
   `CAP_PERIODIC_AGE` (one catalog rewrite, no table IO — via 1.0's
   `enable_capability` `prepare` hook). Foreign mounts (`is_foreign_mount`) are
   never stamped and never eligible.

New capability-gated metadata: `SstMeta::last_compaction_time: Option<i64>`
(**new**; unix nanos, `None` = unknown/legacy).

**Stamping sites — all of them:**

| Site | Stamp |
| --- | --- |
| `ColumnFamily::finish_writer_to_handle` (flush + ingest) | `Some(clock())` — set it beside the existing `meta.max_entry_time = Some(now_nanos())` |
| every compaction output (`CompactionOutputBuilder`) | `Some(clock())` at job freeze time, so all outputs of one job share a stamp |
| `parts.rs::relocate_part` (move/copy) | unchanged — the meta is cloned, the stamp rides along |
| `DB::attach_part` / `DB::attach_part_by_ref` | left `None` **and never eligible**, exactly like foreign mounts: an attached part was written by another database, whose compaction history this one does not own |
| capability-enable rewrite | `None` → enable time, local non-mounted tables only |

`None` is therefore never "eligible now": it means unknown, and unknown is
ineligible. That is the same convention `max_entry_time` already uses for the
part mover.

## Clock

`now_nanos()` is a free function in `util.rs`. Add (**new**) an injectable
clock on `DbInner` — `clock: Arc<dyn Fn() -> i64 + Send + Sync>` defaulting to
`now_nanos` — read **only** by periodic stamping and eligibility. Existing
`max_entry_time` stamps keep calling `now_nanos` directly; changing them is out
of scope. Deterministic tests inject a fake.

## Configuration

```rust
// ColumnFamilyConfig (persisted)
pub periodic_compaction_interval: Duration,   // 0 disables; invalid for Fifo
```

## Scheduling — under its own CAS

Piggyback on the existing compaction-worker cadence: `compact_worker` already
runs a part-mover pass between jobs, every `part_mover_interval`. **But
`compact_worker` is N threads** (`num_compaction_threads`, default 2), which is
exactly why the mover pass is guarded:

```rust
// db.rs — One mover pass at a time. With several compaction workers, two could
// otherwise scan concurrently and pick the same partition to relocate.
&& db.mover_running.compare_exchange(false, true, SeqCst, SeqCst).is_ok()
```

A periodic scan bolted beside it without the same guard means every worker
independently scans and enqueues the same CF each derived interval. Add
`DbInner::periodic_running: AtomicBool` (**new**), mirroring `mover_running`
field-for-field: the same `compare_exchange(false, true, SeqCst, SeqCst)`
before the pass, the same `store(false, SeqCst)` after, and its own
`last_periodic: Instant` local in the worker loop.

Derived check interval = `interval / 4` clamped to `[1s, 15m]`. The pass only
sends on the compact channel (non-blocking `try_send`/`send` on the existing
unbounded channel); the picker rechecks eligibility under its normal locks.
Read-only opens never schedule; a poisoned DB exits the pass.

## Picker

Selection runs **after** capacity work (lowest priority), as a pre-pass in
`pick_compaction` that is consulted only when the scored-levels loop yields no
job:

1. `periodic_compaction_interval == 0` → no periodic pick.
2. Walk levels top-down; among local, non-foreign tables with
   `last_compaction_time == Some(t)` and `clock() - t >= interval`, remember
   the oldest `t`.
3. Non-bottom candidate: an ordinary bounded job (source + `gather_target`
   overlap), with the foreign-mount and `lock_job` vetoes as usual.
4. Bottom candidate: in-place rewrite only, via exactly the
   `compact_into(last, last)` shape `run_manual` already uses (its doc calls it
   "the in-place bottom rewrite manual compaction does — the only way tables in
   the last level that overlap no incoming data ever see the compaction filter
   or drop their tombstones again"). Never create a deeper level for age
   reasons alone. Output is stamped `clock()`, so it is not immediately
   re-picked.

Job reason code `periodic` (**new**) alongside the capacity reason, surfaced as
`CfStats::periodic_compactions: u64` (**new**) so the work is distinguishable
from capacity-driven compaction.

## Failure matrix

| Point | State | Behavior | Test |
| --- | --- | --- | --- |
| crash during enable-time stamping | old manifest | capability absent on reopen; no stamps; enable retries cleanly | `periodic_enable_crash` |
| crash after stamp, before any job | v2 manifest with stamps | eligible one interval after the stamp; restart-safe | `periodic_stamp_survives_reopen` |
| periodic job persist fails | old view | existing compaction rollback; table stays eligible | `periodic_job_rollback` |
| clock skew (`clock() < stamp`) | — | not eligible; no panic, no negative age | `periodic_clock_skew` |
| `CompactionStyle::Fifo` CF sets the option | — | `validate` returns `Err` (FIFO has its own age eviction) | `periodic_refuses_fifo` |
| attached part (`attach_part*`) | `None` stamp | never eligible, never stamped | `periodic_ignores_attached_parts` |

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`. Tasks 1–2 depend on
1.0 having landed.

1. **Metadata field.** `manifest.rs`: `SstMeta::last_compaction_time:
   Option<i64>`, encoded behind `CAP_PERIODIC_AGE`.
   Test first: `manifest.rs::last_compaction_time_round_trips` — save/load a
   manifest with `Some(t)` and with `None`; and
   `manifest.rs::legacy_manifest_decodes_last_compaction_time_as_none`.
2. **Capability enable hook.** 1.0's `enable_capability` `prepare` hook stamps
   `None` → enable time for local non-mounted tables in the same manifest
   write.
   Test first: `tests/maintenance.rs::periodic_enable_stamps_local_tables_once`
   — enable, assert every local table has the enable stamp and every foreign
   mount is still `None`; re-enable is a no-op. Then
   `tests/maintenance.rs::periodic_enable_crash` — inject a manifest-save
   failure and assert reopen shows neither the capability nor any stamp.
3. **Injectable clock.** `db.rs`: `DbInner::clock` (**new**) defaulting to
   `now_nanos`, plus a test-only setter.
   Test first: `db.rs::injected_clock_drives_periodic_eligibility_only` —
   inject a fake clock, assert `max_entry_time` stamps still come from
   `now_nanos` (i.e. the fake does not leak into tier placement).
4. **Stamping sites.** `column_family.rs::finish_writer_to_handle` and
   `compaction.rs::CompactionOutputBuilder`.
   Test first: `tests/db.rs::flush_and_compaction_stamp_last_compaction_time` —
   flush, assert `Some`; compact, assert all outputs of one job share one
   stamp and it is `>=` the inputs'. Plus
   `tests/parts.rs::periodic_ignores_attached_parts`.
5. **Config.** `config.rs`: `periodic_compaction_interval`, `Default` (zero),
   `validate` (`Err` when non-zero on a FIFO CF), config-blob tag.
   Test first: `config.rs::periodic_refuses_fifo` and
   `tests/db.rs::periodic_interval_round_trips_through_reopen`.
6. **Eligibility helper.** `compaction.rs`: `fn periodic_candidate(db, cf,
   now: i64) -> Option<(usize, Arc<SstHandle>)>` (**new**) — pure over a level
   snapshot.
   Test first: `compaction.rs::periodic_candidate_picks_oldest_eligible` —
   mixed stamps; assert the oldest eligible non-mounted local table wins;
   `None` stamps and foreign mounts are skipped;
   `compaction.rs::periodic_clock_skew` — `now < stamp` yields no candidate and
   no panic; `compaction.rs::periodic_candidate_none_when_interval_zero`.
7. **Picker integration.** `compaction.rs::pick_compaction` — consult
   `periodic_candidate` only after the capacity loop yields nothing; non-bottom
   → bounded job through `gather_target`/`lock_job`; bottom →
   `compact_into(last, last)`.
   Test first: `tests/maintenance.rs::periodic_rewrites_bottom_in_place` —
   assert the rewrite happens and `levels.len()` does not grow; and
   `tests/maintenance.rs::periodic_does_not_preempt_capacity_work` — with a
   level over capacity *and* an eligible old table, the capacity job is chosen.
8. **Scheduler + CAS.** `db.rs::compact_worker`: `periodic_running` CAS,
   `last_periodic` cadence, derived interval clamp; `DbInner::periodic_running`
   field and initializer.
   Test first: `db.rs::periodic_running_cas_admits_one_worker` — spawn two
   threads through the guarded block, assert exactly one enters. Plus
   `tests/maintenance.rs::periodic_scan_enqueues_cf_once_per_interval` with
   `num_compaction_threads = 4` and a fake clock: assert one enqueue per
   interval, not four.
9. **Stats.** `maintenance.rs`: `CfStats::periodic_compactions` (**new**) and
   the `periodic` reason code.
   Test first: `tests/maintenance.rs::periodic_compactions_counter_increments`
   — capacity work leaves it at zero; one periodic job takes it to one.
10. **Idle-reclaim end-to-end.** Test first:
    `tests/maintenance.rs::idle_ttl_database_reclaims_without_writes` — fake
    clock, TTL data, no writes after the initial load; advance past the
    interval and assert on-disk bytes drop and no repeated immediate job loop
    occurs (bounded compaction count over a long soak). Plus
    `tests/maintenance.rs::snapshots_retain_hidden_data_under_periodic` —
    `VersionRetention` unchanged: the trigger adds **no new drop rule**.
11. **Harness.** TTL write phase + idle soak; publish reclaimed bytes and idle
    CPU with the interval at 0 versus set.

## Acceptance

TTL-write + idle-soak phase: stale space reclaimed within
`interval + check + one job`; no repeated immediate job loop; negligible idle
CPU when nothing is eligible (measured with the option at 0 as the control).

## Rollback

Option to 0 stops scheduling; persisted stamps stay readable and are simply
never consulted.
