# 0.10 PerfContext — nil-path microbenchmark

**Date:** 2026-08-30. **Host:** darwin 25.5.0 (the machine `docs/performance.md`
describes as thermally noisy). **Build:** `--release`, both feature configs.

**Acceptance (`docs/plans/phase-0-runtime/features/10-perf-context.md`):** hot
point `get`, scope open versus absent, p50 delta within ±2% over ≥5 runs.

**Verdict: met.** Median p50 delta is **+0.74%** in the default config and
**−1.02%** under `unsafe-fastpath`, over 5 invocations × 11 paired trials each.
The per-invocation spread (±5%) straddles zero in both directions in both
configs, which is the signature of a difference below this machine's noise
floor rather than a small real cost.

## What was measured

Two arms, identical work, differing only in whether a `perf::Scope` is open on
the measuring thread:

- **no_scope** — every `perf::bump` in the read path is the nil check: one
  thread-local load of a `Cell<usize>` depth and a compare.
- **scope** — a scope is open, so every bump borrows the innermost frame and
  increments. A single `get` here does 5 bumps (see the per-get context below).

Per-get context sampled from the same database, so the "scope" arm is provably
doing counted work:

```
default:         bloom_probes 1, memtable_probes 1, sstable_probes 1,
                 index_seeks 1, block_cache_hits 1
unsafe-fastpath: bloom_probes 1, memtable_probes 1, sstable_probes 1,
                 index_seeks 1, block_read_bytes 4168   (zero-copy mmap block)
```

## Method

`tests/perf_nilpath.rs::nil_path_scope_overhead` (`#[ignore]`d):

```sh
cargo test --release --test perf_nilpath -- --ignored --nocapture
cargo test --release --features unsafe-fastpath --test perf_nilpath -- --ignored --nocapture
```

200,000 keys (16 B) × 100 B values, flushed to an SSTable, two warm-up passes,
then 11 trials. Each trial times one full pass per arm; the arm order alternates
every trial so a first/second-position effect cancels. Both arms run **in one
process over one warm database**, so page cache, allocator state and clock
domain are shared.

Raw output: `in-process-ab.txt` (10 invocations, 110 paired trials).

## Results — p50 delta (scope vs no scope), per invocation

| Invocation | default | unsafe-fastpath |
| ---: | ---: | ---: |
| 1 | +0.21% | −0.45% |
| 2 | +0.74% | −5.37% |
| 3 | −3.18% | −1.02% |
| 4 | +2.24% | −3.83% |
| 5 | +1.14% | +0.61% |
| **median** | **+0.74%** | **−1.02%** |

Median across all ten invocations: **−0.12%**.

## Why not the whole-process harness

`raw-runs.txt` holds the first attempt: `onda_bench -ops 1000000 -threads 8
-phases get` with `-perf_scope off|thread|op`, plus a baseline binary built from
`3afc3c1` (v0.8.2, no PerfContext), 9 rounds interleaved. It is **not** usable
evidence at this resolution and is kept only for completeness: the Get phase
went bimodal mid-session (rounds 1–5 clustered at 1.1–1.4 s, rounds 6–9 at
0.65–0.87 s), so the medians order the arms incoherently — `op`
(`get_with_perf` per operation, strictly more work) came out *faster* than
`off`. Same-run ratios do land inside the bar (median `off`/baseline +0.7%,
median `thread`/`off` −1.2%), but with a per-round spread of 0.51–1.21 they
cannot resolve 2%. Hence the in-process A/B above.

The `-perf_scope off|thread|op` flag added to `onda_bench` stays: it is the way
to exercise the in-scope read path under the full concurrent harness, which is
what later features' acceptance runs will want.

## Implementation note

The hot path is deliberately split across two thread-locals. `HOT` holds the
depth counter and the innermost frame; neither field owns anything, so that
thread-local has **no destructor** and its access lowers to a plain TLS load
rather than a lazy-init-plus-registration call. The `Vec` of enclosing frames
lives in a separate thread-local touched only by `enter`/`finish`. An earlier
version kept both in one `RefCell<Vec<PerfContext>>`; the process-level harness
suggested that shape cost ~5% with a scope open, which is what prompted the
split — though on this machine's noise floor that number was never solid.
