# 0.8 — parallel spans within one compaction, 2026-08-30

Wall time of **one deliberately large bounded job** at 1, 2 and 4 spans, at a
fixed thread and IO budget. Harness:
`src/compaction.rs::subcompaction_span_scaling_benchmark` (`#[ignore]`d).

```sh
ONDADB_BENCH_RUNS=5 ONDADB_BENCH_KEYS=200000 \
  cargo test --release --lib -- --ignored --nocapture span_scaling
```

## What is measured

One `compact_inputs_spanned` call, timed on its own — not a whole sweep, and not
a rate over an ingest. The tree is built identically for every arm: three
flushed L0 tables merged into L1 by an ordinary single-span job, then three more
L0 tables on top. The timed job merges all six L0 tables with the whole of L1
(`target_file_size = 1 MiB`, so L1 holds ~12 tables and the boundary planner has
real cuts to choose from). 200,000 keys × ~80 B values, six generations.

Fixed for every arm: `num_compaction_threads = 2`,
`max_subcompaction_workers = 4`, no IO limiter, one process, arms alternating
within each run so thermal drift lands on all three.

## Result

Raw rows in `span-scaling.csv` (`spans,run,seconds,span_count,imbalance_bytes`).

| Spans | Median | Min | Max | Speedup vs 1 span |
|---|---:|---:|---:|---:|
| 1 | 2.926 s | 2.600 | 3.660 | — |
| 2 | 1.948 s | 1.634 | 3.143 | 1.50× |
| 4 | 1.956 s | 1.477 | 2.192 | 1.50× |

**Gate: NOT MET.** The gate the harness prints is "the median improvement must
exceed the single-span arm's own min–max spread, or it is indistinguishable from
this machine's noise". The improvement is 0.98 s at both 2 and 4 spans; the
single-span arm's spread on this run was 1.06 s. The measurement does not
separate the effect from the noise, so it is not evidence.

It is also not evidence of *absence*: every arm's median moved in the expected
direction, and the 4-span arm's worst run (2.192 s) was faster than the 1-span
arm's median. The honest reading is "plausibly ~1.5×, not demonstrated here".

Two things are worth recording about how this run was taken, because they bound
what it can say:

- **The machine was not quiet.** Another build and test suite ran concurrently
  for the whole benchmark, which is the most likely source of the 1.06 s spread
  on the baseline arm (the documented run-to-run figure for this machine is
  15–20%; this was 36%). A re-run on an idle machine is the way to settle it.
- **4 spans is not faster than 2** on a job this size — the medians are within
  0.5% of each other. Per-span fixed costs are the plausible reason: each span
  opens its own reader over every input and seeks it, and each writes its own
  output files, each of which costs a `sync_all` plus a parent-directory fsync
  in `Writer::finish`. At ~24 MB of output those are a real fraction of the job.
  A job an order of magnitude larger is where 4 spans would have room.

## Span balance

`span_imbalance_bytes` (widest span minus narrowest, in output bytes) was
**492,549 B at 2 spans and 492,552 B at 4 spans**, identical across all five
runs — the planner is deterministic over metadata, as intended. Against roughly
24 MB of output that is about **2% skew**, so the job is not being held up by one
oversized span. The number is stable enough to be used as a regression signal.

## Decision

**The default stays `max_subcompactions = 1`**, as it would have regardless of
this result. Nothing changes for a database that does not set the option: no
extra thread, no permit pool traffic, and `plan_spans` returns a single
unbounded span before it looks at anything.

The write-amplification side is unchanged by construction rather than by
measurement: spans partition the key range, so the same entries are read and the
same entries are written; only the file boundaries differ. Foreground p99 is not
reported here — this harness times one background job in isolation and has no
foreground load to measure. The 0.6 limiter is what bounds a span's effect on
foreground latency, and span workers are inside it
(`span_workers_charge_as_compaction`).
