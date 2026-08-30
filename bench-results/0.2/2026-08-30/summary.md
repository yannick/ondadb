# 0.2 — minimum-overlap-ratio compaction picking, write-amp evidence

**Date:** 2026-08-30. **Base:** 0.8.2 (`3afc3c1`). **Host:** macOS (darwin
25.5.0), Apple silicon. **Build:** `cargo test --release --lib`, default
feature set.

## What was measured

Compaction **bytes written per ingested byte** — the numerator counted at the
one place compaction installs output (`compact_inputs`, behind a `#[cfg(test)]`
counter, so no write-amp statistic enters the public API), the denominator the
user bytes the fixture hands to `put`.

Command (raw output in `raw-write-amp.txt`):

```sh
ONDADB_BENCH_RUNS=5 cargo test --release --lib -- --ignored --nocapture write_amp
```

**Baseline reachability.** The pre-0.2 first-fit sweep is kept alive behind
`FIRST_FIT_ORDER`, a `#[cfg(test)]` atomic that makes `rank_candidates` return
the old `(start + k) % n` cursor order. That was chosen over stashing the
function so both arms run in **one binary, interleaved, on one machine state** —
on a host with ±15–20% thermal drift (AGENTS.md), two separately-built runs
minutes apart are not comparable. It costs nothing: the flag and the counter are
compiled out of every non-test build, and the benchmark that flips it is
`#[ignore]`d, so a plain `cargo test` neither runs it nor sees the flag move.

## Fixture

`tests/support/levels.rs`, `LevelGeometry::default()` — 40 flushed batches x
20,000 keys drawn from seed-placed windows of varying width, over a 4M-key
space; records in the top quarter of the keyspace are 8x larger than the rest.
195,450,464 ingested bytes. `compacting_config()`: 4 MiB memtable, 1 MiB target
file, 8 MiB L1, `level_size_ratio` 4, so L1..L3 are all populated and levels
>= 1 — the only branch 0.2 changes — do most of the compaction. Pacing disabled
and `finish_compactions_on_close = true`, so neither arm can look cheap by
deferring work past the timer.

The skewed record size is load-bearing and worth stating plainly. On a
**uniform** fixture the same code measured 2.4276 -> 2.4232 (0.2%): once a tree
settles, every candidate sits over roughly `level_size_ratio` bytes per byte of
its own, all ratios are equal, and there is nothing for the picker to choose
between. The effect needs the bytes-per-key density to vary across the
keyspace — which is what variable-size records give, and what a store with
value separation sees normally.

## Results — compaction bytes / ingested byte, 5 runs each, interleaved

| run | first-fit (baseline) | min-overlap-ratio |
| ---: | ---: | ---: |
| 0 | 2.7680 | 2.4818 |
| 1 | 2.8284 | 2.4313 |
| 2 | 2.8356 | 2.4469 |
| 3 | 2.7680 | 2.4523 |
| 4 | 2.7680 | 2.4523 |

|  | median | min | max | spread |
| --- | ---: | ---: | ---: | ---: |
| first-fit | 2.7680 | 2.7680 | 2.8356 | 0.0676 |
| min-overlap-ratio | 2.4523 | 2.4313 | 2.4818 | 0.0505 |

## Gate

> Candidate median must beat baseline median by **more than** the baseline's own
> min-max spread.

Improvement **0.3157** vs baseline spread **0.0676** — **MET**, by 4.7x the
noise band. Every candidate run is below every baseline run, so the arms do not
overlap at all. In relative terms compaction wrote **11.4% fewer bytes** per
ingested byte.

## Regressions

`tests/ingest_arms_compaction.rs` and `tests/sustained_writes.rs` pass in both
feature configurations, along with the rest of the suite (four-command gate).

## Honest caveats

- One host, one fixture shape. The magnitude is a property of *this* density
  skew; a uniform workload gains ~0.2% (measured above) and loses nothing.
- The measurement counts compaction output bytes, not device writes: flush
  output is excluded on both arms, so this is the compaction component of write
  amplification, which is exactly what the picker can move.
- `finish_compactions_on_close = true` means both arms are drained, but neither
  is driven to a fully settled tree; a longer soak could shift the absolute
  ratios (not, on this evidence, their ordering).
