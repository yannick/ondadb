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
