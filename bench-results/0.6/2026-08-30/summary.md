# 0.6-A — background IO limiter: acceptance measurement

**Date:** 2026-08-30 · **Branch:** `worktree-agent-a5610933c0c9a568b` (on
`roadmap/wave-a`) · **Host:** macOS (darwin 25.5.0), Apple silicon
**Harness:** `tests/io_limiter_bench.rs`, `#[ignore]`d; re-run with

```sh
RUNS=5 ONDADB_BENCH_RATE=16777216 \
ONDADB_BENCH_OUT=bench-results/0.6/2026-08-30/raw.jsonl \
cargo test --release --test io_limiter_bench -- --ignored --nocapture
```

Raw per-phase records: `raw.jsonl` (15 records = 5 runs x 3 phases), retained.

## What was measured

Three phases per run, each on a fresh database of 40,000 keys x 5 overwritten
generations (~80 MB logical, ~400-byte values), block cache 4 MiB:

| Phase | Foreground | Background |
|---|---|---|
| `baseline_no_compaction` | point reads, 6 s | none |
| `compaction_unlimited` | point reads, 6 s | `DB::compact` in a loop, unlimited |
| `compaction_limited` | point reads, 6 s | `DB::compact` in a loop, 16 MiB/s |

Foreground read latency percentiles are in microseconds. `fsync_p99` is
`DB::sync_wal` latency sampled every 100 ms — published alongside charged bytes
deliberately, because **charged bytes are a proxy for device pressure, not a
measurement of it** (the risk row for 0.6 in `docs/plans/phase-0-runtime/plan.md`).

## Results (median of 5 runs)

| Phase | p50 | p99 | p99.9 | max | fsync p99 | background MB | reads | compact s |
|---|---|---|---|---|---|---|---|---|
| baseline_no_compaction | 2 | 7 | 37 | 12944 | 299649 | 0.0 | 1446585 | — |
| compaction_unlimited | 2 | 7 | 69 | 10158 | 41149 | 364.6 | 1521671 | 6.14 |
| compaction_limited | 2 | **11** | 74 | 10014 | 223661 | **123.6** | 1405042 | 6.95 |

Per-run read p99 (us), sorted:

- unlimited: 6, 6, 7, 11, 16
- limited: 5, 7, 11, 31, 116

Per-run background bytes (MB):

- unlimited: 334.2, 335.5, 704.2, 463.0, 364.6
- limited: 123.6, 123.6, 178.0, 247.2, 122.9

## Finding 1 — the rate limit works, precisely

Background throughput in the limited phase, per run, against the 16.78 MB/s
configured ceiling:

| run | background MB | compaction s | MB/s |
|---|---|---|---|
| 0 | 123.6 | 6.95 | 17.79 |
| 1 | 123.6 | 6.73 | 18.38 |
| 2 | 178.0 | 10.66 | 16.71 |
| 3 | 247.2 | 14.91 | 16.58 |
| 4 | 122.9 | 6.39 | 19.23 |

16.6–19.2 MB/s against a 16.78 MB/s limit. The overshoot is the initial full
burst, which the bucket hands out free at open: subtracting one burst from run 0
gives (123.6 − 16.8) / 6.95 = **15.4 MB/s**, so the steady-state rate brackets
the configured value from both sides. Against unlimited compaction (median
364.6 MB) the limiter cut background bytes **2.95x**. The mechanism does what it
says it does.

## Finding 2 — foreground read p99 is NOT protected. Acceptance is not met.

The acceptance bar was: *"p99 read latency under a limited compaction stays
within a documented bound of the no-compaction baseline, while unlimited
compaction measurably degrades it — that delta is the feature."*

Neither half holds here:

- **Unlimited compaction does not degrade read p99.** Median p99 is 7 us both
  with and without compaction running — identical to the baseline. There is no
  degradation for a limiter to recover.
- **The limit makes p99 worse, not better**, in 3 of 5 runs (11, 31, 116 us
  against an unlimited median of 7 us).

## Why the measurement cannot support the claim

Two defects in the experiment, both mine, both recorded so the re-run does not
repeat them:

1. **The workload never reaches the device.** p50 is 2 us in every phase,
   including the baseline — these reads are served from the block cache and the
   OS page cache. An ~80 MB dataset on a host with many GB of RAM cannot
   generate read-versus-compaction contention at the device queue, which is the
   only place the limiter can help. The dataset must exceed RAM.
2. **The host was not quiet.** Five or more sibling agents were running
   `cargo build` / `cargo test` against a shared build directory throughout.
   Read counts in the limited phase span **84,269 to 1,810,512 — a 21.5x
   spread** within one configuration; the baseline phase spans 3.9x. External
   load of that magnitude swamps a few-microsecond p99 delta entirely. The two
   worst limited runs (2 and 3) are also the two with anomalous compaction
   durations (10.7 s and 14.9 s against ~6.4 s elsewhere): they were measuring
   the machine, not the feature. `fsync_p99` is likewise pure noise here —
   baseline 300 ms is *higher* than unlimited-compaction 41 ms, which is
   physically backwards.

`AGENTS.md` already warns this machine is thermally noisy at +/-15-20 %
run-to-run. What was seen here is an order of magnitude beyond that.

## Gate decision

**Acceptance NOT met on this evidence. Do not enable by default.**

That is the shipped state and needs no further action: all the options default
to `0`, `ioctrl::limiter_for` returns `None`, no limiter object is allocated,
and every charge point is one nil check — today's behaviour exactly, pinned by
`db.rs::no_limiter_object_when_disabled`. The rollback described in the feature
doc *is* the default configuration.

What is verified and can be relied on:

- the bandwidth ceiling is enforced to within the burst allowance (Finding 1);
- background work is correctly classified, including the three caller-thread
  paths (`tests/maintenance.rs::manual_compaction_is_charged_as_background`,
  `worker_threads_report_their_io_class`,
  `tests/ingest_arms_compaction.rs::ingest_finish_is_charged_as_flush`);
- foreground never waits, asserted in simulated time
  (`limited_compaction_stretches_over_fake_clock`,
  `ioctrl.rs::foreground_never_waits`);
- close and fail-stop wake every waiter
  (`close_wakes_a_blocked_background_charge`,
  `ioctrl.rs::cancel_wakes_a_blocked_waiter`).

What must happen before any non-zero default is recommended:

1. Re-run on a **quiet** host — no sibling builds — and report the read-count
   spread as a validity check; reject the run if it exceeds ~1.3x.
2. Use a dataset **larger than RAM** (tens of GB), so foreground reads actually
   issue device IO and p50 leaves the microsecond range. Until reads touch the
   device this experiment cannot detect the effect the feature exists to
   produce, in either direction.
3. Only then judge the p99 bound.
