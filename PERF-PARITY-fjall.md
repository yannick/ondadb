# ondaDB → fjall performance-parity plan

Baseline: the M2 Ultra 10-workload sweep (rust-storage-bench, `add-ondadb`), onda-lsm /
onda-btree vs fjall 3.1.6, 60 s runs, 1 M × 200 B unless noted. All code refs are on
ondaDB branch `perf/read-write-gaps`.

## The gaps we are closing (M2 baseline)

| Workload | ondaDB (lsm) | fjall3 | Gap | Kind |
|---|---|---|---|---|
| queue peek (`range_first`) | 14 k/s | 810 k/s | **58×** | scan (memtable-resident) |
| feed (prefix scan) | 8 k/s | 63 k/s | **8×** | scan (SST-resident) |
| event-log (range scan) | 7 k/s | 40 k/s | **6×** | scan (SST-resident) |
| read-write random, read | 86 k/s | 525 k/s | **6×** | point read (mixed) |
| YCSB-A read | — | — | **2.7×** | point read |
| YCSB-B read | — | — | **2.3×** | point read |
| time-series range | 2 k/s | 4 k/s | **2×** | scan |
| webtable disk / write-amp | 6.1–6.5 GB / 4.7 | 3.0 GB / 3.0 | **~2×** | disk / large value |

## Root-cause summary

The single most important finding: **the benchmark measured ondaDB on its deliberately-slow
safe path with a mis-sized cache**, not the configuration the engine is designed and documented
to run in. Two harness facts, both in rust-storage-bench:

1. **`unsafe-fastpath` is OFF.** `Cargo.toml:35` pulls `ondadb` with no `features`, so
   `#![forbid(unsafe_code)]` holds and the **mmap zero-copy SSTable reader + arena memtable are
   compiled out**. `docs/performance.md:5` states every published ondaDB figure was measured
   *with* `--features unsafe-fastpath`. We benchmarked the opposite build.
2. **`--cache-size 512 MB` never reaches ondaDB.** `src/db/builder.rs` (OndaLsm arm) sets only
   `use_btree`, `compression`, `sync_mode`, `write_buffer_size` — never `block_cache_size`. Every
   other engine gets `args.cache_size`; ondaDB stays at its **64 MiB default** (`config.rs:177`),
   so a random working set thrashes while fjall serves from a 512 MiB-equivalent mapped set.

Everything else is genuine engine work, ranked below by leverage.

---

## Phase 0 — Fair-footing rebench — DONE, MEASURED (M2, 2026-07-09)

Ran three configs on the M2 (safe+64 MiB baseline, safe+512 MB, fastpath+512 MB), 10 workloads
× 3 engines each. The two levers behave **oppositely** and must be treated separately:

| Lever | Point reads | Scans | Verdict |
|---|---|---|---|
| **0a. Cache 64 MiB → 512 MB** | +13–25% uniform | no change | **Pure win — landed** |
| **0b. mmap SST reads** (fastpath) | rw-random +64% (gap 2.7×→1.6×), rw-seq +38% | (would help SST scans) | Win, but bundled ↓ |
| **arena memtable** (bundled in same flag) | ~0 | **50–1700× worse** | **Landmine** |

Measured onda-lsm throughput (cache64 / cache512 / fastpath vs fjall512):
- rw-random read: 170k / 212k / **348k** vs 572k → gap 3.4× → 2.7× → **1.6×**
- ycsb-c read: 2.38M / 2.84M / 2.90M vs 2.15M → onda **wins** at 512 MB+
- queue peek: 14k / 13k / **52** range-ops/s vs 89k → fastpath **destroys** it
- event-log write: 252k / 251k / **5k** → arena scans drag mixed write+scan workloads down too

**0a. Cache size — landed.** `builder.rs` OndaLsm/OndaBtree arm now sets
`opts.block_cache_size = args.cache_size` (64 MiB → 512 MB). Pure upside, no scan regression.
This is the committed benchmark config.

**0b. mmap reads — blocked on ondaDB, do NOT enable `unsafe-fastpath` as-is.** The single feature
gates mmap zero-copy reads (good) *and* the arena memtable, whose read iterator materializes+sorts
the entire memtable per scan (`memtable.rs:480-483`) — scans go O(n·log n) per call and collapse
50–1700× as the memtable fills (verified: rate decays within a run, e.g. event-log 396→43/s).
**Prerequisite:** split the feature into `mmap-reads` and `arena-memtable` (or fix the arena read
iterator, which is Phase 1a anyway). Then `mmap-reads` + cache gives the rw-random 1.6× *without*
the scan regression. Until then the bench stays on the safe build.

**Net after 0a:** point-read gaps narrowed (rw-random 3.4×→2.7×, ycsb-c now a win); **scan gaps and
webtable disk are entirely config-independent** — they are the real engine work below. Best-measured
safe-config residual gaps: queue peek 6.6×, feed 6.3×, timeseries 1.9×, event-log 1.5×,
rw-random 2.7×, webtable disk 5.1 GB vs 3.4 GB.

---

## Phase 1 — Scans (the 6–58× gaps): iterator construction cost

The merge *hot loop* is already well optimized (8-byte key-prefix compare, block-granularity value
pinning). The cost is in **constructing** an iterator, and it is paid per scan call.

- **1a. Fast single-key head-peek for `range_first` (fixes the 58× queue peek).**
  Root cause: the memtable is **256 independent skip-lists** (`memtable.rs:46`). Every iterator
  builds a `ShardCur` for all 256 shards and `seek_to_first` does 256 × `front()` + a 256-way
  heapify (`memtable.rs:646-661, 723-729`) — a fixed ~256× cost to return **one** element. That is
  14 k/s @ 106 µs vs fjall's single-skiplist 810 k/s @ 2 µs.
  Options (pick one): (i) a dedicated `first()/last()` path that scans 256 shard fronts once and
  returns the min without allocating a full `LazyMemIter`/heap; (ii) reduce read-side shard count
  (a tunable, or a merged read view); (iii) cache/reuse the merge iterator across peeks.
- **1b. Range-prune SSTables in `new_iterator` (fixes feed / event-log / time-series).**
  `column_family.rs:818-829` pushes one child iterator per **every** SSTable in **every** level with
  **no `[min_key,max_key]` overlap check** — a narrow-range scan still builds and seeks an
  `SstIterator` for every table, each doing a full block read + full in-block offset decode. The
  point-`get` path already prunes by key range (`column_family.rs:643-661`); **reuse that pruning in
  the iterator path.** This is the primary driver of the prefix/range-scan gaps.
- **1c. (After 0a) confirm block-fetch cost is gone.** With mmap on, the per-block
  allocate-copy-decompress-CRC path (`reader.rs:351-358, 610-615`) and the per-block-transition
  block-cache mutex disappear for the uncompressed default. Verify in profiles; if scans still show
  cache-lock time, apply #3 below.

---

## Phase 2 — Point reads (2–6×): after Phase 0, the residual code work

Assuming 0a/0b land, the algorithm is already sound (bloom → binary-searched block index → one data
block). Residual targets, in order:

- **2a. Lock-free / read-mostly block cache.** Every cache *hit* takes an exclusive per-shard
  `parking_lot::Mutex` to bump LRU recency (`cache/block.rs:97-102`), serializing 8 reader threads
  across 16 shards. Replace the LRU-on-read with a read-mostly policy (sharded CLOCK / S3-FIFO, or
  `try_lock`-with-recency-skip) so hits don't serialize. fjall's mapped reads take no per-read lock.
- **2b. Trim per-get allocations.** `s.imm.clone()` allocates a `Vec<Arc<ImmMemtable>>` every get
  (`column_family.rs:675`); the safe-build memtable boxes an `IKey` probe per get
  (`memtable.rs:71-73` — resolved by 0a's arena path); value copied via `.to_vec()`
  (`reader.rs:517`); `read_floor_seq` does a thread-local `HashMap` lookup per get (`db.rs:150-152`).
  Individually small, collectively real at 500 k+ ops/s, and they compound under mixed read-write
  where `imm`/L0 are non-empty.
- **2c. Mixed-workload contention (the 6× read-write-random).** Concurrent flush/compaction both
  evict the (now 512 MiB) cache and compete for CPU/IO; `docs/performance.md:94-96` already flags
  this. Lower priority — measure after 0b + 2a; consider compaction rate-limiting or read-path
  cache pinning if it persists.

---

## Phase 3 — Large-value disk & write-amp (webtable ~2×)

Independent of the read/scan work; `unsafe-fastpath` does nothing here. Two structural fixes:

- **3a. Compress vlog values (biggest disk win, ~2×).** WiscKey-separated values (≥ 512 B) go to
  the vlog **uncompressed** — `write_vlog` frames `[crc32c u32][raw value]` with no compression
  (`sst/writer.rs:217-234`), so `--compression lz4` never touches the 8 KB HTML values (they'd
  compress ~3–4×). fjall compresses its separated blobs. Fix: run `compress(alg, value)` in
  `write_vlog` wired to `compression_for_level`/`opts.compression`, frame like a data block
  (store-raw-on-non-shrink as `block.rs:26-35` already does), and decompress in `sst/reader.rs`
  `read_vlog`. This alone accounts for most of the 6.1 → 3.0 GB gap.
- **3b. Decouple vlog from per-SST compaction + add vlog GC (biggest write-amp win, 4.7 → ~3.0).**
  Today each SSTable owns a paired `<id>.vlog`, and **compaction physically rewrites every value
  into a fresh vlog** — `compact_level` reads `its[bi].value()` and re-`add`s it, calling
  `write_vlog` again per level move (`compaction.rs:196, 222-230`), with **no vlog GC**. So each
  8 KB payload is copied in full L0→L1→L2… fjall keeps blobs in shared 128 MB blob files with
  pointers that survive tree compaction and a dedicated space-amp/staleness GC
  (`gc_with_space_amp_target(2.0)` / `gc_with_staleness_threshold(0.5)`), so large values are not
  re-copied per level. Adopt the shared-blob + pointer + GC model. (Secondary effect: the oversized
  raw vlog also inflates `pick_level` sizing at `compaction.rs:78-82`, pulling compactions earlier.)
- **3c. Minor:** default `compression_per_level = [None, Lz4, …]` to skip hot-L0 CPU (matches
  fjall's `[None, Lz4]` policy), and raise `DEFAULT_BLOCK_SIZE` (`sst/mod.rs:49`, hardcoded
  `compaction.rs:318`) from 4 KiB toward 16–32 KiB to improve LZ4 ratio for inlined/klog data.
  Small for pure-vlog webtable, helpful for mixed value sizes.

---

## Sequencing & expected outcome

1. **Phase 0** (bench Cargo.toml + builder arm, ~1 h) → rebench → establishes the true baseline.
   Expect point-read and SST-scan gaps to shrink to near-parity or better; queue-peek and webtable
   unchanged.
2. **Phase 1a + 1b** (memtable head-peek + iterator SST range-pruning) → the queue-peek 58× and the
   prefix/range-scan gaps. Highest-value engine work.
3. **Phase 3a + 3b** (vlog compression + vlog GC) → webtable disk & write-amp to parity.
4. **Phase 2a + 2b** (lock-free cache + per-get alloc trim) → close any residual point-read gap and
   improve multi-thread scaling.

Phases 1, 2, 3 are independent and can proceed in parallel once Phase 0 fixes the measurement.
Re-run the full 10-workload M2 sweep after each phase and diff against this baseline.

---

## Phase 1 — DONE, MEASURED (M2, 2026-07-09, ondadb 9b47504)

Shipped: feature split (`mmap-reads` / `arena-memtable`, `d97374b`), lazy arena read
iterator (`4291883`), `NUM_SHARDS` 256→16 (`5444451`), bounded iterators + SST pruning
(`0db814d`), bidirectional lazy arena merge (`9b47504`). Bench config: full
`unsafe-fastpath` + 512 MB cache.

| workload | orig gap | NOW | note |
|---|---|---|---|
| queue peek | 6.5× | **parity** (89k vs 88k) | shard fan-out was 70% of reader CPU |
| feed rev-prefix | 6.2× | **1.3×** (142k vs 183k, p99 43µs vs 29µs) | arena reverse = O(log n) steps now |
| timeseries | 2.1× | **win 1.3×** | |
| event-log | 1.5× | 1.2× | unbounded prefix — merge-throughput bound |
| ycsb-c read | 0.9× | **win 1.4×** (2.81M vs 2.03M) | |
| rw-seq read | 1.1× | **win 1.3×** | |
| rw-random read | 3.4× | **1.8×** (332k vs 585k) | → Phase 2 |
| ycsb-b read | 1.8× | 1.6× | → Phase 2 |
| queue/webtable/tseries writes | up to 5.8× | **parity or win** | |
| webtable disk | 2.1× | 2.2× (8.0 GB vs 3.7 GB, wamp 4.55 vs 3.03) | untouched → Phase 3 |

Remaining work, in value order:
1. **Phase 3 (vlog compression + GC)** — the only ≥2× gap left (webtable disk/write-amp).
2. **Phase 2 (lock-free block cache, per-get allocs)** — rw-random 1.8×, ycsb-b 1.6×.
3. event-log/feed residuals are merge-throughput and write-path parity, low urgency.

Lesson recorded: the feed decay was diagnosed twice — the first "fix" (bounded
materialization, `0db814d`) attacked the right code path with the wrong asymptotics
(O(range) per read still decays as hot timelines grow); only O(log n)-per-step backward
iteration (`9b47504`) flattened the curve. Profile-late-in-run was the decisive tool.

---

## Phase 3a — DONE, MEASURED (M2, 2026-07-09, ondadb bf0d2bf)

Vlog compression + per-key-prefix compression rules. Webtable (8 KB pages, lz4):

| | disk | write-amp | writes/s |
|---|---|---|---|
| onda orig | 6,145 MB | 4.70 | 252k |
| **onda +3a** | **2,966 MB** | **2.35** | **286k** |
| fjall3 (same run) | 3,062 MB | 2.99 | 270k |

The last ≥2× gap is closed — ondaDB now **beats fjall on disk, write-amp, and
write throughput** for the large-value workload. All Phase 1/2 gains held
exactly (queue PAR, timeseries/ycsb-c/rw-seq wins, feed 1.3×); zero regressions
across the 30-run sweep.

**Final residual gaps (all < 2×):** ycsb-b read 1.6×, rw-random read 1.7×,
ycsb-a 1.3×, feed 1.3×, event-log 1.2× → Phase 2 (lock-free block cache,
per-get allocations) is the remaining lever.
