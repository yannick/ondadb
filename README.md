# ondaDB

A safe, performance-focused **Rust** key/value LSM storage engine.
Single crate, no async runtime, `#![forbid(unsafe_code)]` by default.

- **100% safe Rust by default** (`#![forbid(unsafe_code)]`). Two opt-in fast-path
  features (`mmap-reads`, `arena-memtable`) each lift that forbid for one module
  only, in small, documented `unsafe` regions — see [Builds](#builds).
- **Durable by construction** — every stored byte is checksummed, flushes fsync
  in a fixed order, and any fsync/manifest failure fail-stops the database rather
  than silently continuing. See the invariants in
  [`docs/concurrency-and-safety.md`](docs/concurrency-and-safety.md).

Full release history is in [`CHANGELOG.md`](CHANGELOG.md).

## Features

### Core engine

- **Column families** — isolated, independently configured key/value stores, each
  with its own memtable, WAL and LSM levels.
- **MVCC transactions** with five isolation levels (`ReadUncommitted`,
  `ReadCommitted`, `RepeatableRead`, `Snapshot`, `Serializable`), write-write
  conflict detection (Snapshot/Serializable) and read-set validation
  (Serializable). See [`docs/concurrency-and-safety.md`](docs/concurrency-and-safety.md).
- **Savepoints** (partial rollback) and **commit hooks** (post-commit callbacks).
- **Bidirectional, snapshot-consistent iterators** (`seek`, `seek_for_prev`,
  forward/backward).
- **TTL** per key — lazy expiry on read, dropped during compaction.

### Storage & format

- **WiscKey value separation** — values ≥ `klog_value_threshold` go to a separate
  value log (vlog), keeping the key log dense and scans cheap.
- **Bloom filters** (dense + sparse encodings) sized from the keys actually
  written, plus a per-block index.
- **Compression** — none / snappy / lz4 / zstd / lz4fast / flate, per column
  family and per SSTable block, with an optional **per-level policy**
  (`compression_per_level`, e.g. `[None, Zstd]` = hot L0 uncompressed, everything
  below Zstd).
- **B+tree hybrid klog** (`use_btree`) — a large-scale index format that keeps the
  on-disk index shallow as SSTables grow. [Details below](#btree-hybrid-klog-columnfamilyconfiguse_btree).
- **Six comparators** (`memcmp`, `reverse`, `lexicographic`, `uint64`, `int64`,
  `case_insensitive`) plus custom comparators, persisted by name in the manifest.
- On-disk format documented byte-for-byte in [`docs/formats.md`](docs/formats.md).

### Write path & durability

- **Group-commit WAL** — one `write`/`fsync` per batch of concurrent committers,
  three sync modes (`None` / `Full` / `Interval`), crash-safe replay.
- **Fail-stop durability (poisoning)** — any fsync/flush/manifest failure
  fail-stops the database: writes are rejected with `OndaError::Poisoned` (reads
  keep working, `DB::poisoned()` reports why) instead of retrying after the kernel
  may have dropped dirty pages.
- **`sync_wal()`** — an explicit durability point for `SyncMode::None` / `Interval`:
  fsyncs every WAL; on `Ok`, everything committed before the call is on disk.
- **Single-process lock** — a `LOCK` file (exclusive for read-write, shared for
  read-only opens) makes a second open fail with `OndaError::Locked`.
- **Bulk ingestion** — `DB::start_ingestion(&cf)` streams pre-sorted entries
  straight into L0 SSTables (no WAL, no memtable), rolls files at
  `write_buffer_size`, installs them atomically at `finish()`, and arms the
  compactor so a bulk-loaded store does not accumulate permanent L0 tables.

### Compaction

- **Leveled compaction** (L0→L1 by file count, Li→Li+1 by size), snapshot-aware
  version collapse and tombstone GC.
- **Compaction filters** — `cf.set_compaction_filter(|key, value| ...)` drops (or
  tombstones) entries during compaction for custom GC/expiry.
- **FIFO compaction style** — `compaction_style: Fifo` with `fifo_max_bytes` /
  `fifo_ttl`: never merges, evicts the oldest tables whole (cache semantics).

### Memory & caching

- **Bounded open readers** — opening an SSTable eagerly loads its block index and
  bloom filter, and both stay resident while the reader lives. A shared,
  least-recently-used **table cache** bounds that resident memory by
  **both a reader count** (`max_open_readers`, default 512) **and a byte budget**
  (`max_open_reader_bytes`, default 1 GiB) — the `max_open_files` equivalent.
  Closing a reader is safe: it is a pure, re-derivable view of an immutable file.
- **Non-serializing read path** — the table cache is sharded with second-chance
  (CLOCK) replacement, so a cache hit takes a shard read lock instead of one
  process-global mutex; point-read throughput now scales across cores.
- **Block cache** (sharded, byte-bounded LRU) and **file-handle cache**.

### Partitions, parts & storage tiers

See [`docs/parts-and-tiers.md`](docs/parts-and-tiers.md) for the full guide —
concepts, worked examples, S3 setup and operational notes.

- **Partitions** — prefix rules carve a CF's keyspace into named partitions;
  bottom-level compaction cuts its output at the boundaries, so each partition's
  bottom data is a clean, addressable **part**. Rules can be added/removed on a
  live CF (`add_partition_rule`). Partitions may also be **derived from the key**
  by a caller-supplied `PartitionFn` (`PartitionScheme::Derived`) when the
  partition set is a function of the data rather than a list.
- **Part lifecycle** — ClickHouse-style `detach_part` / `attach_part` /
  `freeze_part`: drop a part from the catalog atomically, re-attach a same-lineage
  part, or export one as a standalone openable database.
- **Part export** — `DB::export_part` returns a `PartManifest` (a part's tables,
  key ranges, sequence bounds, sizes, tier, plus a SHA-256 **content digest**) so
  a part's physical identity can leave the database and be coordinated across
  machines, without copying bytes.
- **Storage tiers + part mover** — named tiers (`Options::tiers`): a second disk,
  a no-mmap NFS-style mount, S3, or a caller-built `Storage` backend
  (`TierDef::custom`). Per-CF `tier_rules` (prefix + `min_age`) drive a background
  mover that relocates aged bottom parts crash-safely
  (copy → fsync → atomic manifest flip → delete source).
- **S3 tier** (cargo feature `s3`, MinIO-tested) — cold parts live in an
  S3-compatible object store: block reads become bounded HTTP range GETs fronted
  by the block cache (cold block = 1 GET, warm = 0), writes are single-shot PUTs,
  and no async runtime bleeds into the engine. Every request carries a bounded
  retry (4 attempts, 25/50/100 ms backoff) on transport-level errors only — sound
  because every operation the backend issues is idempotent.
- **Attach-by-reference over shared tiers** — `TierDef::shared()` plus
  `attach_part_by_ref` let N reader databases **mount the same object-store parts
  with zero bytes copied**. Objects on a shared tier are named by a per-database
  instance nonce so writers sharing a root never collide, each attached table is
  CRC-verified through the tier backend on attach, and shared tiers are delete-free
  (reclamation is the coordinating layer's job). This expresses the
  one-writer / many-disposable-reader topology directly.

### Operations & observability

- **Batch CF creation** — `create_column_families(&[(name, config)])` creates many
  CFs with **one** manifest persist instead of one per CF (each persist is an
  `F_FULLFSYNC` + directory fsync on macOS). All-or-nothing on validation, so a
  conflicting batch creates nothing. Measured ~9× faster for 11 CFs at boot.
- **`clear_column_family()`** — atomically empty a CF, preserving its configuration.
- **Durability inspection** — `DB::wal_sync_count()` counts successful physical
  WAL syncs (so a `SyncMode::Full` consumer can *verify*, not assume) and
  `DB::column_family_config(name)` returns a CF's effective durable configuration.
- **Observability** — `approximate_len()`, per-CF read counters (point reads,
  bloom-filter skips, SSTable probes), cache hit/miss stats, `DB::reader_memory`
  and `DB::table_cache_bytes()`.
- **Maintenance** — checkpoint (hard-link), backup (copy), column-family clone,
  per-CF and database stats. See [`docs/architecture.md`](docs/architecture.md).

### Modes

- **Unified memtable** (`unified_memtable`) — the whole database shares one
  memtable and one WAL, reducing per-CF overhead for many small/idle CFs.
  [Details below](#unified-memtable-optionsunified_memtable).

Not implemented / out of scope: read replicas, range compaction, Serializable
phantom protection (point-read validation only). See the non-goals in
[`AGENTS.md`](AGENTS.md).

## Builds

ondaDB ships a safe default build and two opt-in fast-path features:

| Feature | `unsafe` | What it adds |
|---------|----------|--------------|
| **default** | none (`#![forbid(unsafe_code)]`) | lock-free `crossbeam-skiplist` memtable, group-commit WAL, LRU caches, zero-allocation iterator |
| **`mmap-reads`** | one contained region | `mmap` zero-copy SSTable/vlog reads (helps point reads and SST-resident scans) |
| **`arena-memtable`** | one contained region | arena-backed skip-list memtable (chunked arena, one writer per shard, lock-free readers) |
| **`unsafe-fastpath`** | both regions | back-compat alias enabling `mmap-reads` + `arena-memtable` together |

```sh
cargo build                              # safe build (default)
cargo build --features unsafe-fastpath   # both fast paths
cargo test                               # full suite, safe build
cargo test --features unsafe-fastpath    # same suite over the fast paths
```

Both configurations must stay green — the two builds compile different
memtable/reader code. See [`AGENTS.md`](AGENTS.md) for the CI-equivalent gate.

## Documentation

| Doc | Covers |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Module map, write/read/flush/compaction/recovery data flow, partitions, storage tiers, part lifecycle & mover, S3 tier |
| [`docs/formats.md`](docs/formats.md) | Every on-disk byte: WAL frames, SSTable klog/vlog, manifest (incl. the append-tolerant tail and shared-tier sections), internal keys |
| [`docs/concurrency-and-safety.md`](docs/concurrency-and-safety.md) | Lock inventory & ordering, MVCC, rotation protocol, S3 runtime contract, every `unsafe` contract |
| [`docs/parts-and-tiers.md`](docs/parts-and-tiers.md) | User guide to partitions, parts and tiers — worked examples, S3 setup, attach-by-reference, operational notes |
| [`docs/performance.md`](docs/performance.md) | Fast paths, benchmark methodology, known measurement artifacts |

## Usage

```rust
use std::time::Duration;
use ondadb::{DB, Options, ColumnFamilyConfig, IsolationLevel};

let db = DB::open(Options::new("/tmp/onda"))?;
let cf = db.create_column_family("default", ColumnFamilyConfig::default())?;

// Opening a fixed multi-CF layout? Create them as ONE batch — one manifest
// persist (two fsyncs) for the lot instead of one per CF:
let cfs = db.create_column_families(&[
    ("events", ColumnFamilyConfig::default()),
    ("index",  ColumnFamilyConfig::default()),
])?;
let _ = cfs; // handles in input order

// Single-op API (auto-committed at ReadCommitted).
db.put(&cf, b"key", b"value", Duration::ZERO)?;
assert_eq!(db.get(&cf, b"key")?, b"value");
db.delete(&cf, b"key")?;

// Transactions.
let mut txn = db.begin();                          // Snapshot isolation
txn.put(&cf, b"a", b"1", Duration::ZERO)?;
txn.set_savepoint("sp")?;
txn.put(&cf, b"b", b"2", Duration::ZERO)?;
txn.rollback_to_savepoint("sp")?;                  // drops "b"
txn.commit()?;

// Iteration (bidirectional, snapshot-consistent).
let mut txn = db.begin();
let mut it = txn.new_iterator(&cf);
it.seek_to_first();
while it.valid() {
    let (k, v) = (it.key().to_vec(), it.value().to_vec());
    let _ = (k, v);
    it.next();
}
drop(it);
txn.rollback()?;

db.close()?;
# Ok::<(), ondadb::OndaError>(())
```

## B+tree hybrid klog (`ColumnFamilyConfig::use_btree`)

By default an SSTable's klog is sorted data blocks + a flat single-level index.
With `use_btree = true`, the index is written as a **B+tree** on disk: leaf nodes
point at data blocks, internal nodes at leaves, and the root carries the min key
(`FOOTER_BTREE` flag). It is a per-column-family, opt-in on-disk format. The
data-block format is unchanged, so it composes with compression, bloom filters
and WiscKey.

```rust
let cfg = ColumnFamilyConfig { use_btree: true, ..Default::default() };
let cf = db.create_column_family("bt", cfg)?;
# Ok::<(), ondadb::OndaError>(())
```

The reader walks the tree from the root to its leaves to load the index; at
typical SSTable sizes the in-memory index stays flat (a flat index is already
cache-friendly), so the B-tree is the on-disk format and the benefit grows with
SSTable size. Implemented in [`src/sst/`](src/sst).

### When to use `use_btree`

**Use it when:**

- **SSTables get very large** (hundreds of MB to GB). A large `write_buffer_size`,
  deep levels, or large inline values (`klog_value_threshold` high) all produce
  big runs whose index a tiered B+tree keeps shallow and cache-friendly.
- **Seek-heavy / point-read-heavy** workloads on those large runs, where index
  navigation is on the hot path.

**Avoid it (keep the default flat index) when:**

- **SSTables are small/typical** (the usual case). The flat single-level index is
  already small and cache-resident; the B+tree only adds internal-node blocks —
  slightly more on-disk space and a little extra work at flush/compaction — with
  **no read win at these sizes**.
- **Write-heavy** workloads sensitive to flush/compaction cost: building the extra
  index levels is pure overhead if reads don't benefit.
- You want the **smallest possible SSTable footprint**.

Rule of thumb: leave it **off** unless you have measured large SSTables and a
read-bound workload; it is a large-scale tuning knob, not a general default.
`use_btree` is per column family and applies only to **newly written** SSTables —
flipping it does not rewrite existing files (the format is recorded per file in
the footer), and a full compaction migrates older files over time.

## Unified memtable (`Options::unified_memtable`)

Normally each column family has its own memtable + WAL. In unified mode the whole
database shares **one** memtable and **one** WAL; every entry's key is prefixed
with a stable 8-byte column-family id, so a single bytewise memtable holds all
CFs grouped by id. When it fills, the flush **splits by CF** into per-CF L0
SSTables (the LSM levels stay per-CF); recovery replays the single WAL and routes
each record back to its CF. This reduces per-CF overhead for workloads with many
small/idle column families.

```rust
let opts = Options { unified_memtable: true, ..Options::new("/tmp/onda-unified") };
let db = DB::open(opts)?;
# Ok::<(), ondadb::OndaError>(())
```

The layout is persisted once the database has column families; reopening it with
the other layout is rejected. To convert an existing per-CF database, open once
with both `unified_memtable: true` and `migrate_to_unified: true`. Migration
recovers and flushes every legacy WAL before atomically recording the unified
layout. `unified_memtable_stall_threshold` (default 6) bounds sealed memtables
when flush falls behind.

Point reads work under any per-CF comparator (exact prefixed-key lookup); ordered
iteration and flush re-sort a CF's slice with that CF's comparator. Implemented in
[`src/unified.rs`](src/unified.rs).

## Architecture / module map

Full data-flow walkthrough in [`docs/architecture.md`](docs/architecture.md).

```
src/
  config.rs        Options / ColumnFamilyConfig (+ defaults, manifest blob codec)
  error.rs         OndaError
  encoding.rs      varints, fixed ints, CRC32-C, xxHash32
  compress.rs      none/snappy/lz4/zstd/lz4fast/flate codecs
  bloom.rs         bloom filter (dense + sparse)
  comparator.rs    6 built-ins + custom comparators
  format.rs        flag bits + MVCC internal-key trailer
  block.rs         SSTable block framing (compress + checksum)
  cache/           byte-bounded LRU block cache + file-handle cache
  wal.rs           group-commit WAL
  manifest.rs      durable catalog (atomic rewrite)
  memtable.rs      sharded MVCC memtable (crossbeam, or arena under fastpath)
  memtable_arena.rs  arena skip-list shard (arena-memtable feature only)
  sst/             SSTable writer/reader/iterator (+ B+tree hybrid klog)
  table_cache.rs   sharded LRU of open SSTable readers (count + byte budget)
  storage.rs       Storage trait + LocalStorage + tier registry
  storage_s3.rs    S3 tier backend (feature "s3")
  parts.rs         part lifecycle (detach/attach/freeze/export) + part mover
  column_family.rs read path, rotation, flush, levels
  compaction.rs    leveled compaction (+ filters, partition cuts) and FIFO eviction
  ingest.rs        bulk ingestion (sorted stream -> L0, no WAL/memtable)
  flush.rs / db.rs DB lifecycle, workers, sequence/snapshot mgmt, recovery
  txn.rs           transactions + single-op API
  iterator.rs      merging MVCC iterator
  maintenance.rs   checkpoint / backup / clone / stats
  unified.rs       unified-memtable mode
  bin/onda_bench.rs  standalone benchmark (used by ../bench)
```

## Testing & quality

```sh
cargo test                                  # default (safe) build
cargo test --features unsafe-fastpath       # fast path
cargo clippy --all-targets                  # clean
cargo clippy --features unsafe-fastpath --all-targets
cargo fmt --check
```

Corruption and crash regressions get integration tests in `tests/` (e.g.
`corrupt_vlog_value_is_detected`, `concurrent_manifest_writes_survive_reopen`,
`backup_consistent_during_compaction`). The integration suite also includes an
engine-generic compatibility suite ([`tests/fjall_suite.rs`](tests/fjall_suite.rs))
covering batch atomicity, recovery loops, snapshot isolation, prefix scans,
large-value WAL replay and DB locking.

## Benchmarks

ondaDB's standalone benchmark binary lives at
[`src/bin/onda_bench.rs`](src/bin/onda_bench.rs) and is driven by the shared
harness in the sibling [`../bench`](../bench) workspace:

```sh
cargo build --release --features unsafe-fastpath --bin onda_bench
./target/release/onda_bench -ops 1000000 -threads 8
```

Benchmark results on developer hardware are thermally noisy (±15–20% run-to-run);
compare same-run ratios, never absolute numbers across sessions. See
[`docs/performance.md`](docs/performance.md) for the methodology, fast paths and
known measurement artifacts.
</content>
</invoke>
