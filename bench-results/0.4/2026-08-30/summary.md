# 0.4 MultiGet — benchmark evidence, 2026-08-30

**Gate decision: PASS** for batch sizes 16–256 on local flash, and PASS for the
batch-size-1 no-regression clause. The S3 arm was **not run** (see below).

## Method

- Binary: `onda_bench` (release, `--features unsafe-fastpath`), built from this
  branch. macOS, local flash, 8 worker threads.
- 200 000 keys, 16-byte keys, 100-byte values, no compression. The database is
  populated and **reopened** before the phase, so the keys live in SSTables.
- The `multiget` phase (new, opt-in via `-phases multiget`) builds **one**
  deterministic key plan and replays it twice: once as sequential `DB::get`s,
  once as `DB::multi_get` batches. Both passes run once un-timed first, so the
  measurement compares batching **at equal cache state**, not who paid for the
  first read.
- 5 runs per cell; the tables below report the **median** of the 5 per-run
  µs/op figures and the ratio of those medians. This machine is thermally
  noisy (±15–20% run to run, `docs/performance.md`), so only same-run ratios
  are meaningful — which is exactly what the paired passes produce.
- Raw data: `raw.csv` (batch-size sweep), `knobs.csv` (duplicate/hit-ratio
  sweeps). Both carry every individual run.

## Batch-size sweep (`raw.csv`)

`ratio` is sequential µs/op ÷ batched µs/op — above 1.0 means the batch is
faster. `dedup/keys` is `PerfContext::multiget_blocks_deduped` over the number
of lookups in the sampled sub-run.

| pattern | batch | seq µs/op | batch µs/op | ratio | spd min | spd max | dedup/keys |
|---|---:|---:|---:|---:|---:|---:|---:|
| random | 1 | 0.402 | 0.400 | **1.00** | 0.65 | 1.08 | 0/256 |
| random | 16 | 0.447 | 0.158 | 2.83 | 1.96 | 3.22 | 5/4096 |
| random | 32 | 0.409 | 0.131 | 3.12 | 2.27 | 3.31 | 20/8192 |
| random | 64 | 0.404 | 0.169 | 2.39 | 2.05 | 3.19 | 85/16384 |
| random | 128 | 0.397 | 0.117 | 3.39 | 2.76 | 3.65 | 339/32768 |
| random | 256 | 0.426 | 0.146 | 2.92 | 2.16 | 3.63 | 1390/65536 |
| sequential | 1 | 0.412 | 0.417 | **0.99** | 0.95 | 1.28 | 0/256 |
| sequential | 16 | 0.461 | 0.149 | 3.09 | 2.66 | 3.21 | 5/4096 |
| sequential | 32 | 0.405 | 0.125 | 3.24 | 2.95 | 3.46 | 24/8192 |
| sequential | 64 | 0.437 | 0.136 | 3.21 | 2.81 | 3.41 | 98/16384 |
| sequential | 128 | 0.415 | 0.134 | 3.10 | 2.83 | 3.38 | 340/32768 |
| sequential | 256 | 0.424 | 0.148 | 2.86 | 1.84 | 3.80 | 1369/65536 |

**16–256: 2.4×–3.4× median, and the *worst* single run at every one of those
cells is still >= 1.84×** — an order of magnitude outside the ±15–20% baseline
spread. The acceptance criterion ("p50/op improves beyond baseline spread
versus sequential `get` at equal cache state") is met.

**Batch size 1: 1.00 and 0.99** — no regression, within noise in both
directions.

### The batch-1 regression that was found and fixed

The first pass of this matrix measured batch 1 at **0.92 (random) / 0.91
(sequential)** — a consistent ~8–9% regression, inside the machine's noise band
but reproducibly on the wrong side of 1.0 across all 10 runs. The cause was
allocation, not algorithm: `multi_get` heap-allocated four vectors (candidates,
per-key errors, the candidate-table list, and the per-table block scratch) where
`get` uses `SmallVec` and allocates nothing. Giving all four the same inline
budgets `PointReadSources` already uses removed it; the numbers above are the
re-run after that fix. Nothing else changed between the two matrices.

## Duplicate-ratio and hit-ratio knobs (`knobs.csv`, batch 64)

| knob | value | seq µs/op | batch µs/op | ratio | dedup/keys |
|---|---:|---:|---:|---:|---:|
| dup_ratio | 0% | 0.478 | 0.172 | 2.78 | 85/16384 |
| dup_ratio | 25% | 0.407 | 0.143 | 2.85 | 4111/16384 |
| dup_ratio | 50% | 0.446 | 0.131 | 3.40 | 8069/16384 |
| dup_ratio | 75% | 0.388 | 0.105 | 3.70 | 12071/16384 |
| hit_ratio | 100% | 0.418 | 0.132 | 3.17 | 85/16384 |
| hit_ratio | 75% | 0.334 | 0.105 | 3.18 | 48/16384 |
| hit_ratio | 50% | 0.281 | 0.074 | 3.80 | 20/16384 |
| hit_ratio | 25% | 0.257 | 0.039 | 6.59 | 4/16384 |

Duplicate keys are absorbed exactly as designed: the deduplication counter rises
monotonically with the duplicate ratio (85 -> 12 071 of 16 384 lookups) and the
speedup rises with it. Miss-heavy batches get faster still, because a bloom
negative costs a hash and nothing else once the table has been opened once for
the whole batch.

## Honest reading of where the win comes from

**The measured 2.4–3.4× at 0% duplicates is not mostly block deduplication.**
At batch 64 with random keys only 85 of 16 384 lookups rode along on a block
another key had already fetched — the keys are spread across a large table set,
so they rarely share a block. The dominant term is the work the batch does
*once per batch* instead of once per key: one `state` read-lock acquisition and
one clone of the candidate-handle set (`batch_read_sources`), and one
`SstHandle::reader()` resolution per candidate table rather than per (key,
table) pair. With 8 threads that lock and those clones are the per-`get` cost
the batch amortizes.

Block deduplication is real and is what the counter measures, but it only
dominates when a batch's keys cluster into the same blocks — the duplicate-ratio
sweep above, and the unit test `multi_get_reads_each_block_once`, which pins 32
keys in one block to exactly one physical block read (`block_misses == 1`,
`multiget_blocks_deduped == 31`) and the same block bytes a single cold `get`
reads. On a high-latency tier, where one cold block is one range GET, that term
is the one that matters — which is precisely the case this evidence does *not*
cover.

## S3 phase: not run

`ONDADB_S3_ENDPOINT` is not set in this environment and no object-store endpoint
was available, so the S3 arm of the acceptance criterion (`range_gets` per batch
dropping by the deduplication factor) is **unmeasured**. The harness's S3 arm was
also not implemented: adding `S3Metrics.range_gets` reporting that could never be
executed here would have shipped untested network code. This is the one part of
0.4's acceptance section that remains open, and it should be closed before the
S3 motivation in the feature document is claimed as demonstrated.
