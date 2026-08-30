# Performance

How ondaDB gets its numbers, how to measure honestly, and which lessons are
load-bearing. Baseline hardware in all figures: Apple M-series, 12 cores,
8 benchmark threads, `--features unsafe-fastpath` release build.

## Current standing

16 B keys / 100 B values, 1M ops, sync off, no compression, mean of runs:
Put ~3.3–3.6M ops/s. Get ~2.7–2.9M (≈1.1×), Delete
~3.7–4.2M (≈6–7×), forward scan ~8.5–9.5M (≈1.0×), backward ~6.7–7.1M
(≈1.1–1.2×). Across the size matrix (see `../bench/bench_matrix.html`):
ondaDB wins Put and Delete at every size, dominates ≥1 KiB-value workloads and trails on
reads with 1–2 KiB keys (0.5–0.7× — few entries per 4 KiB block; their B+tree
klog pays off there). That large-key read gap is the known open item.

## The fast paths (what makes it fast — don't break these)

Write path:
- `Txn` arena buffering: zero per-op allocations; commit builds `RecordRef`s
  borrowing the arena; hook payloads only when a hook exists.
- `Wal::append_batch`: one frame per batch (one CRC+header per 1000 ops, not
  per record), encoded in the committing thread, appended to a sticky
  per-thread stripe — no leader bottleneck, no file-mutex convoy.
- `Memtable::put_batch`: counting-sort → per-shard runs → one shard-lock
  acquisition per batch; nodes prebuilt outside locks; shared counters
  updated once per batch (they are contended cache lines).
- `ArenaShard`: one allocation per node (`key‖!seq‖value`), inline
  `kprefix`/`nseq` so probes resolve from the node's own cache line,
  `MAX_HEIGHT = 8`.
- Rotation: losers return instead of queueing; next WAL pre-opened during the
  writer drain.

Read/scan path:
- `ChildIter` enum instead of `Box<dyn>` — inlined per-entry accessors.
- Prefix-first comparisons (`key_prefix8`) in the merge heap, group-boundary
  check, and flush merge — most comparisons never touch the key slice.
- Pinned-block borrowed keys/values — no per-entry memcpy; pins refresh per
  block transition, not per entry.
- CRC-once-per-block bitmap in `Reader` — N scanning threads don't re-verify
  the same immutable block N times.
- CRC-once-per-vlog-frame set in `Reader` — the same rule for large values, on
  both the mmap and buffered paths (see "Vlog reads" below). Opt-in caching of
  the *decoded* value on top of it (`max_cached_vlog_value_bytes`, default 0).
- `uvarint` single-byte inline fast path; mmap + `madvise(WillNeed)` prefault.

Flush path:
- `write_l0_streaming` + `FlushMerge`/`ShardCursor`: zero-materialization —
  no `Vec<Entry>` (was 2 allocs + 2 copies per entry) and no sort; borrowed
  slices flow straight into `Writer::add`.

Open path:
- `create_column_families` (0.4.1): one manifest persist per **batch**, not
  per CF. Each persist is a temp-file `sync_all()` (`F_FULLFSYNC` on macOS,
  tens to hundreds of ms on Apple SSDs) + a directory fsync, and the
  manifest is a full rebuild over all CFs — so N sequential creations pay
  ~2N fsyncs for the information content of 2. Measured for an 11-CF boot
  layout: 209.8 ms per-CF vs 22.5 ms batched (median of 8, ~9.3×). If a
  consumer opens a fixed CF layout at boot, use the batch API. Deliberately
  NOT pursued: lazy WAL materialization — `Wal::open` performs no fsync in
  any sync mode, so WAL creation was never the cost, and deferring it would
  push new failure modes into the crash-consistency-critical
  commit/rotation path to save ~22 ms of file creates.

## Measurement methodology

```sh
# full 4-engine suite (single config, 3-run mean, HTML+CSV):
cd ../bench && RUNS=3 ./bench_graphs.sh
# key/value size matrix (5 configs, grouped-bar report):
cd ../bench && ./bench_matrix.sh
# onda alone, quick A/B:
for i in 1 2 3 4 5; do ./target/release/onda_bench -ops 1000000 -threads 8; done
```

Rules learned the hard way:
1. **Thermal noise is ±15–20%**, and worse after sustained benching — single
   runs are meaningless, and even 3-run means drift between sessions. When one
   engine's numbers move, check whether *all* engines moved (machine state)
   before believing a code effect. Same-run cross-engine ratios are the
   trustworthy signal; cool the machine before "final" numbers.
2. **A/B on the same build, minutes apart.** `git stash` the change to
   re-measure baseline if in doubt; both regressions caught in this repo's
   history were found this way.
3. **Profile before optimizing**: run `onda_bench -ops 8000000` in the
   background and `sample <pid> 2` (macOS) during the phase you care about.
   Every optimization above targeted a top-of-profile entry; the ones that
   didn't (early lazy-value attempt) regressed.
4. **Time the close.** See below — this one cost us three releases of a write
   number that was not real.

## Measuring a change (the evidence format)

Every phase-0 feature's acceptance section quotes numbers. This is the shape
they have to be in to count, and the shape a reviewer should ask for.

**Where it lives.** Raw, unedited tool output under
`bench-results/<feature>/<date>/`, plus a `summary.md` next to it that states
the feature's acceptance bar, the verdict, the method, and the numbers. The
summary interprets; the raw files are the record. Keep runs that came out
inconclusive and say why — `bench-results/0.10/2026-08-30/` keeps a whole
process-level attempt that could not resolve its 2% bar, because the reason it
failed is the useful part.

**What to report.** At least 5 runs per arm, and the **p50**, not the mean — one
thermal excursion drags a mean and leaves a median alone. Report the per-run
spread too: a delta smaller than the spread is a non-result, however clean the
median looks. State the feature configuration (`default` and/or
`unsafe-fastpath`) for every number; the two builds run different reader code.

**Same-run ratios, never cross-session absolutes.** Interleave the arms inside
each round so thermal drift hits them equally, and compare the arms against each
other within a round. Absolute ops/sec from one session cannot be compared with
another session's — see the thermal rules above.

**Attribute the change to a mechanism.** Wall time says a change helped; it does
not say why, and a plausible story about why is not evidence. Open a
[`PerfContext`](../src/perf.rs) scope (`DB::get_with_perf`, `Txn::get_with_perf`,
`Iterator::perf_scope`) around a representative operation and quote the counter
that moved — `bloom_negatives` for a filter change, `block_misses` for a cache
change, `vlog_reads`/`vlog_cache_hits` for a value-separation or vlog-cache
change. A latency win with no counter movement behind it is a measurement
artifact until proven otherwise.
Counters are per-operation and thread-affine, so they cost nothing to the
threads that are not measuring: the nil path is a thread-local depth check
(measured at 0.10; see `bench-results/0.10/2026-08-30/summary.md`).

**When the process-level harness is too coarse.** `onda_bench` re-populates and
re-opens a database per run, so its Get phase carries the session's page-cache
and thermal state and can go bimodal within one sitting. For deltas of a few
percent, use an in-process A/B instead — both arms in one process over one warm
database, alternating arm order per trial — as
`tests/perf_nilpath.rs::nil_path_scope_overhead` does. Mark such a test
`#[ignore]` so the gate compiles it without running it.

## Deferred work is not free work

A benchmark that stops its timer before the engine has finished the work
measures the buffer, not the system. `onda_bench`, like the Go and C harnesses
it mirrors, reports Put and then closes the database *outside* the timer. That
convention is fine only if closing is cheap — and until 0.8.0 it was not.

Measured on a 24-core M2 Ultra, 16 B keys / 100 B values, 8 threads:

| Records | Put as reported | close() | Put counting close |
|---|---|---|---|
| 5M  | ~4.6M ops/s | 2.5 s  | 1.36M ops/s |
| 10M | ~4.6M ops/s | 10.8 s | 0.77M ops/s |
| 20M | ~4.6M ops/s | 35 s   | 0.49M ops/s |

The reported column is flat. The real one halves every time the data doubles.
Nothing about the write path was slow; compaction was falling behind and the
debt was paid at close, where no one was looking.

**How to check.** Split the close into its parts before blaming any of them.
`DB::flush_memtable` drains the flush queue only, so timing it separately from
`close()` separates flush backlog from compaction backlog — which is how this
was diagnosed: flush drained in ~130 ms at every dataset size, and everything
else was compaction.

```rust
let t = Instant::now(); db.flush_memtable(&cf)?;  let flush = t.elapsed();
let t = Instant::now(); db.close()?;              let rest  = t.elapsed();
```

**Rules that follow:**

- Quote a write rate as `ops / (ingest + close)` unless you are explicitly
  measuring buffered ingest, and say which you mean.
- Run at more than one dataset size. A single size cannot show the *shape*, and
  the shape is where this class of bug lives — a rate that decays with size
  looks like a healthy rate at any one point.
- Watch `cf.stats().compaction_debt`. A run that ends with debt near
  `hard_pending_compaction_bytes` has deferred work its throughput number does
  not include.
- Cross-engine comparisons must apply this to *every* engine. RocksDB's close
  after the same workload is 31–45 ms — it does its flush work inline — so
  comparing its Put against a competitor's buffered Put is not one measurement.

Also beware the inverse. Moving work *out* of close does not delete it: with
`finish_compactions_on_close = false` the tree is less merged when reads begin,
and cold Get right after reopening measures ~0.78M ops/s against ~1.41M on a
settled tree. Neither number is wrong; they answer different questions. See
`docs/compaction-and-write-pacing.md` § Closing, and reads right after opening.


## Regression history (why the code looks the way it does)

- **Per-entry block pinning (reverted)**: handing the merge an owned `Block`
  per entry cloned the shared `Arc<Mmap>` refcount per entry across 8 threads
  → 3× scan regression. The shipped design pins per child per block
  transition. Never reintroduce per-entry `Arc` traffic on shared blocks.
- **Group commit for non-Full sync (removed)**: without an fsync to amortize,
  the queue/leader/wakeup machinery was pure overhead vs direct striped
  writes.
- `#[cold]` on multi-byte `uvarint` decode would pessimize sequence decoding
  (seqs are 3–4 byte varints) — only the 1-byte path is the fast path.
- **Bloom filters sized from a caller's guess (fixed)**: `Bloom::new` allocates
  a fixed bit array that cannot grow, and the writer sized it from
  `WriterOptions::expected_entries` before seeing a single key. Compaction
  passed a hardcoded `4096` for every table it produced, so a compacted table
  holding a million entries carried a filter designed for four thousand — every
  bit set, every key admitted. Because a leveled LSM keeps nearly all its data
  in compacted levels, **blooms were effectively off for the whole steady-state
  database**: measured on a consumer store with 33.5M entries in one CF,
  400,000 point reads for keys that provably did not exist produced **zero**
  bloom skips, while still paying to build, store, load and hash against the
  filter. Bulk ingestion had a milder form of the same bug
  (`roll_bytes / 64`, an assumed 64-byte entry).

  The writer now buffers one hash per key and builds the filter in `finish()`
  from the count it actually wrote, so no caller can size a filter wrongly.
  Identical data through ingest / put / compact now measures 99.0 % skips on
  all three paths (was 100 % / 99 % / **0 %**), and the filters get *smaller*:
  9.59 bits/key, against the 9.6 the 0.01 target implies, where the old L0
  tables were over-provisioned at 17.4. Cost: 8 bytes per entry buffered until
  the table closes — bounded by the roll target, ~26 MB at the default 64 MiB
  target and 21-byte entries, one writer at a time. Pinned by
  `tests/bloom_survives_compaction.rs`, whose non-vacuity guard asserts the
  filter works *before* compaction so the test cannot pass for the wrong
  reason.

  Measured end to end on a real 3.7 GB consumer store (8,947,487 entries in the
  probed CF), same data and same process with one compaction between the arms,
  so only the filter changes:

  | | before | after |
  |---|---|---|
  | skip rate on absent keys | 400,000 probed, **0 skipped (0.0 %)** | 1 probed, 199,999 skipped (**100.0 %**) |
  | absent-key lookup | 875 ns | **201 ns** (4.4×) |
  | present-key lookup | 901 ns | 569 ns |
  | resident reader bytes | 9.6 MB | 3.3 MB |

  What that does **not** show: the compaction which rewrote the filters also
  merged the CF's nine tables into one, so the present-key figure and part of
  the reader-byte drop are compaction shape, not the filter. The skip rate is
  unconfounded — 0 % to 100 % is the filter alone, and it is what makes the
  absent-key number move.

  **Existing SSTables keep their broken filters until rewritten** — the fix
  applies to newly written tables, and a full compaction migrates the rest.

## Per-level Bloom policy: why it is opt-in (0.1)

`ColumnFamilyConfig::bloom_fpr_per_level` and `optimize_filters_for_hits` are
both **off by default**, and the measurements are the reason. Harness:
`tests/bloom_policy_bench.rs` (`#[ignore]`d), full numbers in
`bench-results/0.1/2026-08-30/`.

- **Per-level rates at equal resident bytes are a wash.** Monkey's premise is
  that a negative lookup probes *every* level, so bits are worth more in the
  small upper levels. ondaDB's read path only probes tables whose `[min, max]`
  covers the key, and leveled compaction makes each level's tables disjoint —
  the measured candidate count is **1.59 tables per lookup**, not one per level.
  Redistributing bits across a cascade that short buys little: at matched
  resident bytes a `[0.001, 0.001, 0.02]` vector returned **−7.4 %** miss-path
  `sstable_probes` (754 → 698) with no movement in p50 at all, and the opposite
  trade (`[0.001, 0.005, 0.05]`, −18 % filter bytes) cost **+163 %**.
  That 1.59 needs a workload that keeps writing: after a bulk load and one
  compaction every level owns a *disjoint* slice of the keyspace, the count is
  exactly 1.00, and there is no cascade at all — so a benchmark without a
  scattered overlay pass will always show per-level rates losing, which is a
  fixture artifact and not a result.
- **`optimize_filters_for_hits` is a real, large, one-directional trade.**
  Resident filter bytes fall **50 %** (246,800 → 123,104 B) and hit-heavy p99
  improves **~22 %**, because the hit path stops hashing against a filter that
  was going to admit the key anyway. Miss-heavy `sstable_probes` rise **46×**
  (754 → 34,404) and miss p50 **+150 %**: a bottom table with no filter is read
  on every negative lookup. It is the right setting only for a workload that is
  known to hit.
- The degradation is **one-way**: "bottom" is dynamic, and a table written
  filterless never regains a filter until a compaction rewrites it into a
  non-bottom target. See `ColumnFamilyConfig::optimize_filters_for_hits`.

## Vlog reads: CRC-once, and the opt-in value cache

Large values (`>= klog_value_threshold`) live in the vlog, and every read of one
used to re-checksum the whole stored payload — a klog data block was verified
once per open reader, a vlog frame every single time. On a value big enough to
matter that checksum is most of the read: CRC32-C runs at about 6.3 GB/s here,
so a 5 MB value cost roughly 800 µs of pure re-verification per read, and
spada's S-208 probe measured vlog reads at 6.96 GB/s against 11.3 GB/s for
cached klog frames.

`Reader::verify_vlog_frame` now checks a frame at most once per open reader.
Repeat-read throughput, `tests/vlog_read_bench.rs`, same build with the arm
selected at runtime, median of 3 runs, `--test-threads=1`:

| | 400 KiB value | 5 MiB value |
|---|---|---|
| mmap (`unsafe-fastpath`), before | 6.97 GB/s | 6.52 GB/s |
| mmap, **CRC-once** | **45.1 GB/s** (6.5×) | **47.3 GB/s** (7.2×) |
| mmap, CRC-once + block cache | 30.8 GB/s | (above the cap — uncached) |
| buffered `pread`, before | 4.85 GB/s | 3.87 GB/s |
| buffered, **CRC-once** | **9.29 GB/s** (1.9×) | **6.47 GB/s** (1.7×) |
| buffered, CRC-once + block cache | 29.0 GB/s (3.1×) | (above the cap — uncached) |

**Vlog values are cacheable, and off by default** (feature 0.5). The table above
is why it is a per-family opt-in
(`ColumnFamilyConfig::max_cached_vlog_value_bytes`, default 0) and not a
default: caching is a win in exactly one of the three regimes below. Two things
had to be true before it could exist at all:

- The block cache needed a **key domain**. A `Reader` owns a klog and a vlog
  under one `file_id`, and both offset spaces start at zero, so `(file_id, 0)`
  named both the first data block and the first vlog frame. `BlockKey` now
  carries a `BlockDomain` (`Klog` | `Vlog`);
  `tests/sst.rs::klog_block_and_vlog_frame_at_same_offset_do_not_alias` pins it.
  That is a correctness fix, not part of the opt-in surface.
- The lookup had to sit **before** the mmap attempt, not after, or the
  `mmap-reads` config would keep paying the v2 decompression on every read.

### What it costs and buys (0.5 acceptance, `bench-results/0.5/2026-08-30/`)

`tests/vlog_read_bench.rs::vlog_value_cache_hot_cold_point_reads`, 5 invocations
per config, arms alternated off/on/off inside each run. 64 KiB values, an 8 MiB
block cache shared with 120k small keys, 40 small point reads interleaved per
large read. Both domains share one capacity, which is the whole measurement.

| build / hot set | hot latency (median of 5) | vlog hit rate | klog hit-rate delta |
|---|---|---|---|
| default, hot set **fits** (1 MiB of 8) | **2.61× faster** (range 2.34–4.14×) | 100% | **−5.35 pp** |
| default, hot set **thrashes** (6 MiB of 8) | **0.55× — a 1.8× regression** | 0% | **−26.18 pp** |
| `unsafe-fastpath`, either hot set | **0.83× — a 1.2× regression** | 100% | not measurable |

Three regimes, and only the first is a win.

**Default build, hot set fits.** The read becomes a shard-locked memcpy out of
an `Arc` instead of a `pread` plus a CRC, and the klog pays about five points of
hit rate for it. This is the case the feature exists for.

**Default build, hot set thrashes.** Every admission is a 64 KiB copy plus an
eviction sweep that is never reused, *and* it evicts a quarter of the klog
residency — strictly worse than not caching, in both terms at once. There is no
adaptive admission policy in v1, so the operator owns that judgement, and the
default (0) is the safe side of it.

**`unsafe-fastpath`, always.** Unchanged from the original finding above: the
caller wants an owned `Vec`, so a cached value is memcpy'd out of an `Arc`
instead of straight from the page-cache-resident mapping. The cache adds a copy
and a shard lock and removes no work, and it loses even at a 100% hit rate. Do
not enable the option on a build that mmaps its tables.

The klog delta is quoted from the **default build only**, and reported as `NaN`
under `mmap-reads` on purpose: an uncompressed klog block is served straight
from the mapping by `read_data_block_local` and never enters the block cache, so
there is no klog residency there for vlog admission to displace.

The case with no such trade-off is a **remote tier** (`s3`), where a miss is an
HTTP range GET rather than a page-cache hit and the arithmetic is not close:
`tests/s3_tier.rs::warm_vlog_value_issues_no_range_get` asserts a warm large
value costs zero requests. It is `ONDADB_S3_ENDPOINT`-gated and so is *not* part
of the numbers above — this repo does not publish unmeasured claims.

`vlog_verified` is kept, not replaced: it still serves bypassed, evicted and
oversized frames, and it is what keeps a cache miss cheap on the second read of
an oversized value.

Scope, honestly: this is once per *open reader*, not once per process — closing
and re-opening a table re-verifies, which is the same guarantee the klog bitmap
has always given. The first read of every frame still verifies, and a frame that
fails verification is never marked, so corruption is reported on every read
(`tests/sst.rs::corrupt_vlog_value_is_detected_on_every_read`).

## Open performance items

1. Reads with 1–2 KiB keys (0.5–0.7× of C): block-level entry-offset restarts
   for in-block binary search, index prefix compression, or leaning on the
   existing `use_btree` klog for large-key CFs.
2. Put tail latency: rotation drain still gates all writers ~per-64 MiB;
   epoch-based memtable handoff would remove the stall.
3. Compaction runs concurrently with the benchmark's scan phases (visible in
   profiles) — a rate limiter or scan-priority scheduling would steady scan
   numbers.
4. `Serializable` phantom tracking and Spooky/DCA compaction are correctness/
   feature items with performance implications; see `AGENTS.md` non-goals.
