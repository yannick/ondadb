# 0.3 — periodic compaction: TTL-write + idle-soak

Date: 2026-08-30. Machine: 24-core M2 Ultra, macOS 25.5.
Harness: `tests/periodic_soak_bench.rs::periodic_ttl_write_then_idle_soak`
(`#[ignore]`d; run with
`cargo test --release --test periodic_soak_bench -- --ignored --nocapture`).
Raw output: `raw-runs.txt`.

## What is measured

One fixture, two arms, in one process on the same build:

| | control | enabled |
|---|---|---|
| `periodic_compaction_interval` | `0` (the default — today's behaviour) | `4s` |
| `CAP_PERIODIC_AGE` | enabled | enabled |

Both arms take the capability, so **the option is the only difference**. The
fixture is 20,000 entries carrying a 5 s TTL plus 2,000 without, 512 B values,
flushed to a single L0 file with `l1_file_count_trigger = 64` — a file count no
capacity trigger can reach. After the flush the database takes **no writes at
all**.

TTL expiry runs on the real clock (`now_nanos`), so the harness genuinely waits
its TTL out; the periodic trigger runs on the injected clock, so the soak does
not have to last a real interval. Idle CPU is the process's own `ps` cputime
delta across the 20 s soak window (centisecond resolution on macOS and Linux).

## Result

```
interval = 0 (control) bytes   12221393 ->   12221393 (  0.0% reclaimed)  entries  22000 ->  22000  periodic_compactions   0  idle CPU   0.04s over 20s
interval = 4s          bytes   12221393 ->    1097990 ( 91.0% reclaimed)  entries  22000 ->   2000  periodic_compactions   1  idle CPU   0.07s over 20s

reclaimed delta: 91.0 pp   idle CPU delta: +0.03s
```

## Against the acceptance criteria

| Criterion | Result |
|---|---|
| stale space reclaimed within `interval + check + one job` | **met** — 11.1 MB of 12.2 MB (91.0%) reclaimed, 20,000 expired entries gone, well inside the 20 s soak (interval 4 s, derived check 1 s, one job) |
| no repeated immediate job loop | **met** — `periodic_compactions = 1` over the whole soak. The rewrite stamps its output at the current reading, so what it produced is not eligible again. Pinned independently by `tests/maintenance.rs::idle_ttl_database_reclaims_without_writes`, which asserts the counter is unchanged over a further 5 s |
| negligible idle CPU when nothing is eligible, with the option at 0 as the control | **met** — 0.04 s over 20 s in the control against 0.07 s enabled, a delta of +0.03 s (~0.15% of one core). The control number is the pre-existing compaction-worker tick, not this feature |
| the control must reclaim nothing | **met** — 0.0%, `periodic_compactions = 0`. This is the gap the feature exists to close: with no writes, no size trigger is ever due |

All four are asserted in the harness, not eyeballed, so the numbers cannot
silently rot.

## Honest reading of the numbers

- **The 91% is fixture-shaped, not a headline.** It is the fraction of the
  fixture written to expire; a workload with a smaller expired fraction
  reclaims proportionally less. What generalizes is the *control* column:
  without this feature an idle database reclaims **nothing**, whatever the
  expired fraction.
- **`periodic_compactions = 1` is this fixture's shape too.** One flushed L0
  file is one eligible table, so one job drains it. A family with many bottom
  tables does one job per eligible table, each re-stamping its own output.
- **The idle-CPU delta is at the edge of what `ps` resolves.** 0.03 s over 20 s
  is a handful of ticks, and it moved between two runs of the same build
  (+0.02 s, then +0.03 s). The claim it supports is bounded — the scan does not
  show up above measurement noise — not that it costs a specific number of milliseconds. The
  structural argument is the stronger one: at the default the compaction
  worker's added work is a single relaxed load of `DbInner::periodic_check` per
  tick, and `run_periodic_scan` is never entered.
- **Benchmarks on this machine are thermally noisy (±15–20%, see
  `docs/performance.md`).** That does not affect the byte and entry counts,
  which are exact; it does affect the CPU figures, which is why the comparison
  is a same-run A/B rather than absolute numbers.
- Two runs of the same build, minutes apart. The byte and entry results were
  identical in both; the CPU figures were not, and should be re-measured rather
  than quoted across sessions.

## Gate decision

**Pass.** The feature meets every acceptance criterion, defaults to off, and at
its default adds one atomic load per compaction-worker tick and no new thread,
ticker, or disk artifact. Rollback is setting the option back to `0`: scheduling
stops and the persisted stamps stay readable and simply unconsulted.
