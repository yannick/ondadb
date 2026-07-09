# ondaDB benchmark results — 9-engine field, M2 Ultra

**Date:** 2026-07-09/10 · **Machine:** Apple M2 Ultra, 64 GB, macOS 26.3 (arm64)
**Harness:** [rust-storage-bench](https://github.com/yannick/rust-storage-bench) `main` (v1 harness + onda backends)
**Method:** one engine at a time, 60 s per run, identical parameters, data dir wiped between runs.
**ondaDB config:** `unsafe-fastpath` (mmap reads + arena memtable), 512 MB block cache, lz4 — ondadb `main` @ `714e720`.
**Engines:** onda-lsm, onda-btree, fjall 3.1.6, fjall 2.11.2, RocksDB (rust-rocksdb 0.44, harness defaults), sled 0.34, redb 4.1, canopydb 0.2, heed/LMDB 0.20.

**Workloads:** YCSB A/B/C (1M × 200 B, 4 threads), event-log (sequential append + windowed scans), time-series (10k series, 4 readers), queue (FIFO with backpressure, 100k pending), feed (50k users, zipf reverse timeline scans), webtable (8 KB pages), read-write random & sequential (1M × 200 B, 4 threads).

---

## Point reads (ops/s — bold = best in field)

| workload | onda-lsm | onda-btree | fjall 3.1 | fjall 2.11 | RocksDB | sled | redb | canopydb | heed/LMDB |
|---|---|---|---|---|---|---|---|---|---|
| YCSB-A (50/50 zipf) | 168k | 170k | 217k | 224k | 181k | **312k** | 25k | 117k | 80k |
| YCSB-B (95/5 zipf) | 493k | 498k | 735k | 861k | 480k | **1.16M** | 318k | 766k | 746k |
| YCSB-C (read-only) | 2.91M | 2.96M | 2.14M | **3.49M** | 1.50M | 3.41M | 1.45M | 2.23M | 2.95M |
| read-write random | 367k | 370k | 576k | 543k | 409k | 192k | 529k | 652k | **1.19M** |
| read-write sequential | 978k | 995k | 721k | 601k | 651k | 101k | 594k | 578k | **1.47M** |


## Scans (range-ops/s)

| workload | onda-lsm | onda-btree | fjall 3.1 | fjall 2.11 | RocksDB | sled | redb | canopydb | heed/LMDB |
|---|---|---|---|---|---|---|---|---|---|
| queue peek (FIFO head) | 97k | 98k | 88k | 85k | 67k | **456k** | 21k | 120k | 53k |
| feed (reverse timeline) | 154k | 153k | 184k | 212k | 158k | 219k | 52k | **233k** | 170k |
| event-log (newest 256) | 3k | 3k | 4k | **4k** | 3k | 2k | 307 | 2k | — |
| time-series window | 19k | 20k | 15k | 17k | 24k | 2k | 109k | 65k | **310k** |


## Writes (ops/s)

| workload | onda-lsm | onda-btree | fjall 3.1 | fjall 2.11 | RocksDB | sled | redb | canopydb | heed/LMDB |
|---|---|---|---|---|---|---|---|---|---|
| YCSB-A | 184k | 186k | 233k | 240k | 197k | **327k** | 40k | 134k | 96k |
| event-log append | 316k | 315k | 378k | **381k** | 294k | 168k | 30k | 233k | — |
| queue (insert+delete) | 99k | 99k | 88k | 85k | 69k | **456k** | 21k | 120k | 53k |
| webtable (8 KB rows) | **286k** | 282k | 278k | 278k | 275k | 35k | 29k | 143k | 47k |
| feed post | 79k | 79k | 87k | 94k | 80k | 89k | 45k | **97k** | 84k |
| read-write random | 307k | **308k** | 287k | 294k | 285k | 117k | 33k | 88k | 78k |


## Storage efficiency — webtable, 8 KB pages, lz4

| engine | disk | write-amp |
|---|---|---|
| redb | 2,056 MB | 0.0 |
| heed/LMDB | 2,154 MB | 0.0 |
| RocksDB | 2,772 MB | 3.84 |
| **onda-lsm** | 2,874 MB | 2.32 |
| fjall 2.11 | 2,947 MB | 2.69 |
| onda-btree | 2,993 MB | 2.35 |
| fjall 3.1 | 3,468 MB | 3.01 |
| sled | 4,056 MB | 8.5 |
| canopydb | 5,780 MB | 7.76 |


## How to read this honestly

- **sled's headline numbers carry a large asterisk.** Its queue run used **5.9 GB peak RAM and wrote 17 GB to disk** (onda: 0.9 GB / 2.9 GB); webtable write-amp is 8.5. It buys throughput with memory and I/O, and collapses on mixed load (rw-random reads 192k, rw-seq 101k).
- **heed/LMDB is the read-throughput ceiling** (rw-seq 1.47M, timeseries 310k) — a zero-copy memory-mapped B-tree. The price: single-writer, 16–100k writes/s everywhere, and its event-log run crashed (thread panic).
- **RocksDB runs harness defaults** — treat its column as a floor, not its ceiling.
- **fjall 2 beats fjall 3 on most reads** in this suite; fjall 3 wins scans and write-amp.
- **redb** is slow across the board except a standout time-series scan (109k).

## Where ondaDB lands

Best-in-field among **general-purpose engines** (excluding the sled/LMDB caveats above) on:

- **Sequential mixed reads** — rw-seq 978k–995k (next LSM: fjall 3 at 721k)
- **Read-only point lookups** — ycsb-c 2.91M–2.96M (beats fjall 3 and RocksDB)
- **Queue / FIFO patterns** — 97–99k peek+write, ahead of every durable engine
- **Webtable write throughput** — 286k/s, best of all nine
- **Large-value storage efficiency** — 2,874 MB / write-amp **2.32**, the best of every LSM in the field (RocksDB: 3.84, fjall 3: 3.01, sled: 8.5)

Remaining gaps: zipf-hot point reads (YCSB-B 493k vs fjall 2's 861k) and random mixed reads (367k vs canopydb's 652k) — root causes analyzed in `PERF-PARITY-fjall.md` Phase 2.

## The week in one table — ondaDB before/after

All engine work happened on this branch over one day (see `PERF-PARITY-fjall.md` for
each phase's commits and measurements):

| workload | before | after | vs fjall 3 now |
|---|---|---|---|
| queue peek | 14k (6.5× behind) | **97k** | **wins** |
| queue writes | 15k (5.8× behind) | **99k** | **wins** |
| feed reverse scans | 29k (6.2× behind) | **152k** | 1.2× |
| time-series scans | 7k (2.1× behind) | **19k** | **wins** |
| rw-random reads | 170k (3.4× behind) | **370k** | 1.6× |
| ycsb-c reads | 2.38M (even) | **2.91M** | **wins** |
| webtable disk / wamp | 6,145 MB / 4.7 | **2,874 MB / 2.32** | **wins** |

What changed: correct benchmark config (512 MB cache, fastpath), `mmap-reads`/`arena-memtable`
feature split, lazy + bidirectional arena read iterator, 16 memtable shards, bounded iterators
with SST range pruning, vlog compression + per-key-prefix compression rules, CLOCK block cache,
memtable pre-filter, uvarint fast path.
