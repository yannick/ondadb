# Changelog

## 0.7.3

**Committed writes could be silently lost.** This fixes the durability bug
recorded as a known issue in 0.7.1, and it was never a WAL bug.

- **A compaction could discard a table a concurrent flush had just installed.**
  The level-set swap was a non-atomic read-modify-write: compaction read the
  levels under a read lock, built a replacement, dropped the lock, and then
  overwrote the whole vector. A flush finishing in that window inserted its new
  L0 table and had it thrown away by the overwrite.

  Nothing deleted the orphaned file — it had never been a compaction input — so
  the bytes stayed on disk while the level set, and the manifest persisted
  immediately afterwards, no longer referenced them. Worse, the flush that
  produced that table had already reclaimed its WAL, correctly, against an
  earlier manifest that *did* contain it. So the data existed in exactly one
  place that nothing would ever read.

  The rebuild now happens inside a single exclusive lock (`update_levels`), and
  the target level is re-derived from live state rather than from a
  pre-compaction snapshot, so a table arriving mid-compaction survives instead
  of being overwritten. A debug assertion makes the invariant loud: a table may
  leave the level set only if it was one of that compaction's inputs.

  **Measured:** `writes_progress_after_crash_recovery_with_wal_backlog` failed
  2 runs in 8 before and 0 in 16 after. The signature was always the same —
  one contiguous run of ~122 keys, a single memtable generation, gone. Whole
  generations vanishing rather than scattered keys is what pointed at a table,
  not at frames.

  Found by instrumenting the swap: it reported dropping non-input table 10, and
  the first missing key was then located inside `10.klog`, on disk, absent from
  the manifest.

## 0.7.2

Read-path concurrency and cost. Everything in 0.7.1 plus the work that had been
sitting on the `parts-and-tiers` branch — which is what the consumer actually
compiled against, so this release makes the tag match reality.

- **The table cache stops serializing reads.** It was one process-global
  `Mutex`, and its *hit* path wrote a recency tick — so a cache hit took an
  exclusive lock, shared by every column family. Measured on an 8-core box,
  point-read throughput through the engine was **flat from 1 to 8 threads**
  with SSTables present, and scaled 4.6x with none: the difference was this
  lock. It is now sixteen shards with second-chance (CLOCK) replacement; a hit
  takes a shard **read** lock and one relaxed bit store. The `max_open` bound
  stays **global and exact** via a shared open count with rotating cross-shard
  eviction — it is a memory contract (0.7.0), so sharding it would have been a
  regression dressed as a speedup.
- **TTL checks use `CLOCK_REALTIME_COARSE`.** A precise `SystemTime::now` per
  get measured ~2 % of warm reads.
- **`uvarint` gains 3- and 4-byte fast paths.** Real sequence numbers made
  every block decode fall into the general byte loop.
- **`new_iterator` binary-searches sorted levels** for the overlapping run
  instead of testing every table in the column family — O(log n + overlap) per
  level.

No format change and no API change.

## 0.7.1

One bug fix, and it is a large one: **bloom filters were effectively off for the
whole steady-state database.**

- **A filter is sized from the keys actually written, not from a caller's
  guess.** `Bloom::new` allocates a fixed bit array that cannot grow, and the
  SSTable writer sized it from `WriterOptions::expected_entries` *before seeing
  a single key* — so every caller had to guess, and both guessed wrong:
  compaction passed a hardcoded `4096` for every table it produced, and bulk
  ingestion divided a byte target by an assumed 64-byte entry.

  A compacted table holding a million entries therefore carried a filter
  designed for four thousand. Every bit ends up set, so `may_contain` answers
  "maybe" to everything — strictly worse than no filter at all, because the
  bytes are still built, written, loaded into memory and hashed against on
  every lookup, and nothing is ever skipped. Because a leveled LSM keeps nearly
  all of its data in compacted levels, this meant blooms did nothing for the
  steady state. Measured on a real consumer store with 33.5M entries in one
  column family: **400,000 point reads for keys that provably did not exist
  produced zero bloom skips.**

  The fix removes the guess rather than improving it. The writer buffers one
  hash per key and builds the filter in `finish()` from the count it actually
  wrote, so no caller can size a filter wrongly; `expected_entries` survives
  only as a capacity hint for that buffer. Identical data through
  ingest / put / compact now measures **99.0 % skips on all three paths**
  (was 100 % / 99 % / **0 %**), and the filters get *smaller*: 9.59 bits/key
  against the 9.6 the 0.01 target implies, where the old L0 tables were
  over-provisioned at 17.4.

  **Cost:** 8 bytes per entry buffered until `finish`, bounded by the writer's
  roll target rather than by the store — at a 64 MiB target and the smallest
  entries a real consumer writes (~21 bytes), a full table is ~3.2M entries and
  the buffer peaks near 26 MB, for one writer at a time.

  No format change: a filter written by 0.7.0 is still readable, it was simply
  useless. Existing tables get correct filters as compaction rewrites them.
  Pinned by `tests/bloom_survives_compaction.rs`.

**Known issue, pre-existing and not fixed here.**
`writes_progress_after_crash_recovery_with_wal_backlog` fails intermittently:
after a crash with a WAL backlog, a reopened database counted **8,878 of 9,000**
keys — 122 writes lost across the crash boundary. Measured at **2 failures in 8
runs, and the same 2 in 8 at v0.7.0**, so this release neither introduces nor
worsens it; it was already present in 0.7.0 as shipped. It is recorded here
because an intermittent *durability* failure is the wrong thing to leave as an
unexplained red test, and because the rate means roughly one CI run in four will
show it. Not diagnosed: whether the loss is in WAL replay or in the flush of
recovered generations.

## 0.7.0

Memory and compaction. Two behaviour changes and one changed default, all
driven by a consumer that reached **10 GB resident and was killed by the kernel
opening a 48 GiB store**, before serving a single request.

- **Bounded open readers (`Options::max_open_readers`, default 512).** Opening
  an SSTable eagerly loads its whole block index and bloom filter, and both stay
  resident while the reader does. `ColumnFamily::open` opened *every* table named
  in the manifest and never closed one, so resident memory was proportional to
  **total stored bytes** rather than to the working set — paid at startup,
  whether or not a table was ever read.

  This is RocksDB's mechanism, which this engine had omitted while copying its
  eager per-table load: *"If `cache_index_and_filter_blocks` is false (which is
  default), the number of index/filter blocks is controlled by option
  `max_open_files`."* `SstHandle` no longer owns a reader; it holds the
  information to re-open one and asks a shared least-recently-used `TableCache`.

  Closing is safe because a reader is a pure, re-derivable view of an immutable
  file: it costs a re-open and cannot change an answer. **Memory is bounded by
  `max_open` plus concurrent in-flight readers**, not by `max_open` alone — an
  in-flight caller holds an `Arc`, so eviction drops only the cache's reference.
  Measured on the store above: 6.6 GB at ten seconds into startup became 784 MB,
  and flat rather than climbing.

  **`Iterator` behaviour change:** `new_iterator` cannot return a `Result`, so a
  reader that fails to open now yields an iterator that is invalid and carries
  the error on `err()`. **Callers that walk `while it.valid()` without checking
  `err()` will read a failed scan as an empty one.** Omitting the table instead
  would return a short answer that looks complete, which is worse; but the
  contract is now load-bearing where it previously could not fire.

- **Bulk ingest arms compaction.** `Ingestion::finish` installed its L0 tables,
  persisted the manifest, and omitted the `compact_tx` send that a memtable
  flush performs — so a bulk-loaded store grew one permanent L0 table per
  ingestion and nothing ever asked the compactor to look. A real store reached
  **14,051 L0 tables for a million documents**; rebuilt with this fix, the same
  corpus shape produced **58**. Verified by removal: with the send deleted, the
  test reports "after 12 bulk-ingested tables, L0 still holds 12 files".

- **`DEFAULT_BLOCK_SIZE` 4 KiB → 16 KiB.** Every data block costs one resident
  index entry. Measured afterwards at ~3 % of the resident total (the bloom
  filter dominates an un-compacted store by 32×), so this is a small win
  honestly labelled: it scales the index down by a constant and bounds nothing.
  Existing files are unaffected — block size is a property of the file that
  wrote it. Callers doing many small random point reads should set
  `SstOptions::block_size` down.

- **New accounting:** `Reader::resident_bytes`/`resident_breakdown`,
  `ColumnFamily::resident_reader_bytes`, `ColumnFamily::l0_file_count`,
  `DB::table_cache_stats`, `DB::reader_memory`, `DB::set_max_open_readers`.
  Added *before* optimising, because this memory grew to dominate while being
  invisible, and two successive diagnoses were wrong without it.

**Not in this release:** the vlog value cache. It is a latency fix that *adds*
memory, and it should not land before resident memory is settled.


## 0.6.0

Two additive changes, no API or format break. Minor bump for the new public
surface: `DB::export_part` (plus the `PartManifest`/`PartTable` types) and
unified-mode's `unified_memtable_stall_threshold` and `migrate_to_unified`.

- **Part export (`DB::export_part`)** — new method returning a `PartManifest`:
  a part's tables (key ranges, sequence bounds, sizes, tier) plus a SHA-256
  content digest, computed by reading the bytes. Where `freeze_part` produces
  an openable *database directory*, this produces a *description* a caller can
  send elsewhere. Motivation: inside a database a part is identified by its
  tables' file ids, which are local counters, so a consumer coordinating parts
  across machines had to maintain a content→part mapping itself, outside the
  engine that owns the facts.

  The digest is a **physical** identity, and the documentation says so
  prominently because the stronger reading is the natural one to assume: it is
  independent of file ids, paths, tier and database instance (two databases
  that did the same writes agree), but *not* of write history, since SSTable
  entries carry database-global sequence numbers. It answers "does the peer
  already have exactly these bytes?" and deliberately does not answer "did two
  replicas independently rebuild the same data?" — that needs a hash over
  logical content, which only the consumer can compute. Both properties have
  tests, including one asserting the limitation, so a change that made the
  digest logical cannot land without updating the docs with it.

  A remote-tier part is hashed through its own tier backend rather than being
  pulled local; a tier move leaves the digest unchanged. Deletions are paused
  for the duration, the same discipline as `checkpoint`/`freeze_part`.

- **Unified WAL durability hardening** — unified memtable mode now has bounded
  immutable backpressure (`unified_memtable_stall_threshold`, default 6),
  persists its WAL layout in the manifest, rejects accidental
  per-CF/unified reopen mismatches, and supports an explicit crash-safe per-CF
  migration via `migrate_to_unified`. A cross-CF transaction in unified
  `SyncMode::Full` remains one checksummed WAL frame and now has a regression
  test proving it performs exactly one physical WAL sync and survives reopen.

## 0.5.0

Three additive changes, no API or format break. Minor bump: two new public
`DB` methods (the durability-inspection hooks).

- **Durability-inspection hooks** — new `DB::column_family_config(name)`
  returns a column family's *effective* durable configuration (what a reopen
  restores, including live-added partition rules), and
  `DB::wal_sync_count()` counts successful physical `sync_data()` calls
  across every WAL the DB opens (per-CF, unified, and post-rotation; wired
  like the poison flag, so rotation never resets it). Motivation: a consumer
  whose correctness depends on `SyncMode::Full` (spada's raft O1 boundary)
  can now *verify* the recorded mode instead of assuming the config it
  opened with, and tests can assert on real physical syncs — under
  `SyncMode::None` the counter never advances, which is exactly what a
  durability test should pin. Added by the spada raft-correctness
  remediation (its S-077/S-082 decisions).

- **Batch column-family creation** — new
  `DB::create_column_families(&[(&str, ColumnFamilyConfig)])` creates a batch
  of CFs and persists the manifest **once** for the whole batch instead of
  once per CF. Semantics are identical to N sequential `create_column_family`
  calls (handles returned in input order); all names are validated up front
  (length, comparator, config, collisions against existing CFs, duplicates
  within the batch), so a conflicting batch creates nothing and never
  persists, and the registry write lock is held across the batch so a
  concurrent creator can never observe it half-built. Motivation: each
  per-CF creation ran a full manifest rebuild + persist — a temp-file
  `sync_all()` (`F_FULLFSYNC` on macOS) plus a directory fsync — and the
  manifest is a full rebuild over all CFs, so after N creations the last
  write already contained everything the first N−1 wrote. A consumer opening
  11 CFs at boot paid ~22 fsyncs for the information content of 2; measured
  209.8 ms per-CF vs **22.5 ms batched** for 11 CFs (median of 8, real
  `F_FULLFSYNC`, ~9.3×). The single-CF path is untouched. Deliberately *not*
  done: lazy WAL materialization — `Wal::open` performs no fsync in any sync
  mode (WAL creation was never the cost), and deferring it would complicate
  the crash-consistency-critical commit/rotation path to save ~22 ms of file
  creates.
- **S3 backend: bounded retry on transport errors** (feature `s3`) —
  every request the backend issues is wrapped in a bounded retry: up to 4
  attempts, 25/50/100 ms backoff, retrying **only** transport-level
  `S3Error::Hyper`/`S3Error::Io`. This closes the hyper 0.14 keep-alive
  reuse race (hyperium/hyper#2136): rust-s3 0.35's tokio backend drives a
  raw `hyper::Client` with a 90 s idle pool and no retry, so a store (or a
  NAT in front of it) that drops a pooled idle connection first kills the
  next request mid-flight with `IncompleteMessage` — a bodied PUT is the
  most exposed because hyper will not replay it. Retrying is sound because
  every operation this backend performs is idempotent by construction:
  part objects use unique never-reused ids and are written whole (single
  PUT), and reads/HEAD/COPY/DELETE/LIST are idempotent by nature. HTTP
  status failures surface as `Ok` with a non-2xx code and can never
  trigger a retry; nothing non-idempotent exists to be retried.

## 0.4.0

Derived-partitioning milestone (A5). Additive — no manifest or API break; a
rules-only column family encodes byte-for-byte as before.

- Partitions may be computed from the key by a consumer-supplied `PartitionFn`
  (`PartitionScheme::Derived`) rather than enumerated in `partition_rules`. The
  partition becomes a *function* of the key, which is what a consumer keying by
  `(namespace, cluster_key)` needs — its partition count is a function of the
  data, not a list anyone can write out in advance. Persisted by scheme name and
  resolved on open through `Options::partition_fns`, mirroring comparators; a
  missing or mismatched implementation is a hard error, never a silent fallback
  to rule-based cutting.
- `DB::list_partitions` returns the materialized bottom-level partitions
  (`PartitionInfo`), read-only, so a consumer can verify its partitioner
  produced the physical separation it intended.
- Debug-only guard in bottom compaction against a `PartitionFn` that is not
  order-compatible — the misimplementation that would otherwise produce a bottom
  SSTable spanning two partitions. No release-build cost.

## 0.3.2

- Derived partitioning (A5) landed (see 0.4.0 for the consumer-facing
  additions and hardening that finalized it).
- Zero-materialization flush merge.

## 0.3.1

- Make part-tier move retries safe after a lost post-commit response, including
  partitions containing a mix of already-moved and off-target SSTables.
- Add deterministic move-phase observation for crash and durability testing.
- Preserve more than 255 per-level compression, compression-rule, partition,
  and tier policies without breaking manifests written by ondaDB 0.3.0.
- Release the database directory lock when the final public `DB` handle drops.
