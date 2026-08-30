# 0.5 vlog value cache - hot/cold large-value point reads

**Date:** 2026-08-30. **Host:** darwin 25.5.0 (the machine `docs/performance.md`
describes as thermally noisy, +/-15-20% run to run). **Build:** `--release`,
both feature configurations.

**Acceptance (`docs/plans/phase-0-runtime/features/05-vlog-value-cache.md`):**
"Hot/cold large-value point-read phase: hot-frame latency improves beyond the
>=5-run baseline spread. The klog block hit-rate loss must be published, and it
is measured on the default build only."

**Verdict: met on the default build when the hot set fits the cache; not met
otherwise, and the option therefore stays 0 by default (it already does).**

| build / hot set | hot latency (median of 5) | range | vlog hit rate | klog hit-rate delta |
|---|---|---|---|---|
| default, hot set **fits** (1 MiB of 8) | **2.61x** | 2.34-4.14x | 100% | **-5.35 pp** |
| default, hot set **thrashes** (6 MiB of 8) | **0.55x** | 0.50-0.59x | 0% | **-26.18 pp** |
| `unsafe-fastpath`, hot set fits | **0.83x** | 0.78-0.88x | 100% | not measurable |
| `unsafe-fastpath`, hot set thrashes | **0.83x** | 0.71-0.86x | 100% | not measurable |

A ratio above 1.00x is a speed-up. The 2.61x median in the winning row is far
outside this machine's +/-20% noise floor, and the arms were alternated
off/on/off inside every invocation, so it is not a thermal artifact - the
vlog-hit-rate and klog-hit-rate columns are the mechanism behind it, not a
story about one.

**Caveat on the absolute latencies:** the host was not quiet during these runs
(other builds were competing for it), so the microsecond figures in
`raw-runs*.txt` are noisier than a dedicated run would give. Two things make
the conclusion hold anyway. The arms alternate inside one process, so
contention lands on all three roughly equally, and the two `off` arms bracket
the `on` arm in every run (e.g. 8.3 / 15.3 / 8.5 us) rather than drifting one
way. And the hit-rate columns are not timings at all: `-5.35 pp` and
`-26.18 pp` came out bit-identical in all five default-build runs, which is
what a deterministic cache-behaviour difference looks like. Only the ratios
carry the noise, and the winning ratio's whole range (2.34-4.14x) sits above
the noise floor.

## What was measured

`tests/vlog_read_bench.rs::vlog_value_cache_hot_cold_point_reads` (`#[ignore]`d):

```sh
cargo test --release --test vlog_read_bench -- --ignored --nocapture \
  vlog_value_cache_hot_cold_point_reads
cargo test --release --features unsafe-fastpath --test vlog_read_bench -- \
  --ignored --nocapture vlog_value_cache_hot_cold_point_reads
```

One SSTable holding both populations, so the two cache domains genuinely compete
for one capacity:

- 120,000 small keys with 64-byte inline values (the klog working set),
- 96 separated 64 KiB values in the vlog,
- an **8 MiB** block cache shared by both domains,
- 40 random small point reads interleaved per large read - the klog pressure
  that vlog admission has to evict something to make room for,
- 12 rounds, a full warm-up round excluded (page-cache fill, first CRC of every
  frame, first admission of every block).

Two hot-set sizes against the same table:

- **fits** - 16 hot values = 1 MiB of the 8 MiB cache,
- **thrashes** - 96 hot values = 6 MiB of the 8 MiB cache.

Three arms per scenario, in order **off (A) -> on (1 MiB limit) -> off (B)**,
each with its own fresh `BlockCache` and `Reader`. Alternating the arms inside
one invocation is what makes thermal drift over the run visible instead of
silently biasing one arm; the two `off` measurements are averaged for the ratio.

Only the large-value `get` calls are timed. Hit rates come from
`BlockCache::stats()` deltas taken after the warm-up, so the warm-up's
compulsory misses do not flatter either arm.

## Reading the three regimes

**Default build, hot set fits - the case the feature exists for.** The read
collapses to a shard-locked memcpy out of an `Arc`: no `pread`, no CRC32-C over
64 KiB, no decompression. 100% vlog hit rate. The klog pays **5.35 points** of
hit rate for the 1 MiB the values occupy, which is the honest cost side of the
"vlog admission evicts klog blocks" risk row.

**Default build, hot set thrashes - a regression, published deliberately.** At
6 MiB of hot values in an 8 MiB cache shared with the klog, nothing survives to
be re-read: the vlog hit rate is **0%** while 2 MiB stays resident. Every read
pays a 64 KiB `Arc::from` copy plus an eviction sweep whose entry is never used,
*and* the klog loses **26.18 points** of hit rate to house it. Worse in both
terms at once, for a net 1.8x slowdown. v1 has no adaptive admission, so this is
the operator's judgement to make, and 0 is the safe default.

**`unsafe-fastpath` - a regression regardless of hit rate.** This reproduces the
finding already recorded in `docs/performance.md`: the caller wants an owned
`Vec`, so a cached value is memcpy'd out of an `Arc` rather than copied straight
from the page-cache-resident mapping. The cache adds a copy and a shard lock and
removes no work, so it loses (0.83x) even at a **100%** hit rate. The option
should not be enabled on a build that mmaps its tables.

Note that under `mmap-reads` the "thrashes" scenario still reaches a 100% vlog
hit rate with 6 MiB resident: uncompressed klog blocks are served from the
mapping and never enter the cache, so the vlog has all 8 MiB to itself. That is
also why the klog hit-rate delta prints as `NaN` there - there is no klog
residency to displace, and a number measured in that config would be measuring
nothing. This is exactly the scoping the acceptance section requires.

## Not measured here

`tests/s3_tier.rs::warm_vlog_value_issues_no_range_get` asserts that a warm
large value on an S3-resident part issues **zero** range GETs. It is gated on
`ONDADB_S3_ENDPOINT` and no MinIO was reachable on this host, so it is written
and compiled but skipped - its result is not claimed above.

## Raw output

- `raw-runs.txt` - 5 invocations, default build.
- `raw-runs-fastpath.txt` - 5 invocations, `--features unsafe-fastpath`.
