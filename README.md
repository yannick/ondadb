# ondaDB

A safe, performance-focused **Rust** key/value LSM storage engine.
Single crate, synchronous API, std threads + crossbeam (no async runtime),
unsafe code denied by default, with one audited Linux coarse-clock call.

## Highlights

- **Safe Rust data structures by default** (`#![deny(unsafe_code)]`); one audited
  Linux `clock_gettime` call serves coarse TTL checks. Two opt-in fast-path
  features add documented `unsafe` regions for mmap reads and an arena memtable
  — see [Builds](#builds).
- **Column families** — isolated, independently configured key/value stores, each
  with its own memtable, WAL and LSM levels.
- **MVCC transactions** — five isolation levels, savepoints, post-commit hooks,
  and bidirectional snapshot-consistent iterators, with write-conflict detection
  on Snapshot/Serializable. Cross-column-family atomic commits require unified
  memtable mode; per-CF WAL mode rejects them rather than exposing partial state.
- **Tiered storage, including S3** — hot data lives on local SSD; aged bottom
  parts are pushed to a second disk, an NFS-style mount, or an S3-compatible
  object store (cargo feature `s3`) and read back through bounded HTTP range GETs
  fronted by the block cache (a cold block is one GET, a warm one none). N
  disposable reader databases can mount the same object-store parts with **zero
  bytes copied** (attach-by-reference). The engine API stays synchronous
  throughout — the S3 backend's own runtime never surfaces. See
  [Storage tiers & S3](#storage-tiers--s3).
- **Classic LSM core** — leveled compaction (plus a FIFO style), WiscKey value
  separation, bloom filters, six comparators, and six compression codecs
  (optionally per level).
- **Bounded, predictable memory** — a byte-budgeted, sharded table cache caps
  resident index + bloom memory (the `max_open_files` equivalent), so opening a
  large store never loads all of it at once.
- **Durable by construction** — every stored byte is checksummed, flush fsync
  ordering is fixed, and any fsync/manifest failure fail-stops the database
  rather than silently continuing. See
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
- On-disk format: **yoloDB format epoch 1** (CRC32-C everywhere, versioned
  magics on every artifact), documented byte-for-byte in
  [`docs/formats.md`](docs/formats.md) with every number registered in
  [`docs/format-registry.md`](docs/format-registry.md). ondaDB 0.9.x
  directories open read-only through `legacy_onda` (default-on `legacy-onda`
  feature).

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
- **Bounded compaction jobs** — a job takes one file from the source level plus
  only the target files it overlaps, so its cost is
  `target_file_size * (1 + level_size_ratio)` no matter how large the level is.
  Jobs on disjoint key ranges compact concurrently.
- **Debt-aware write pacing** — writers are delayed in proportion to pending
  compaction bytes past `soft_pending_compaction_bytes` and block at
  `hard_pending_compaction_bytes`, so sustained ingest reports a rate the engine
  can actually hold. Read the backlog back with `CfStats::compaction_debt`.
- **`finish_compactions_on_close`** (default `false`) — close abandons queued
  compaction, leaving debt the next open resumes from. Set it `true` if you
  load a dataset, close, and reopen to serve point reads immediately: an
  abandoned backlog leaves L0 deeper, and since L0 files overlap, every point
  read probes all of them until compaction catches up.
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
  mover that relocates aged bottom parts crash-safely.
- **S3 tier** (cargo feature `s3`) — cold parts live in an S3-compatible object
  store, read through bounded range GETs fronted by the block cache.
- **Attach-by-reference over shared tiers** — `TierDef::shared()` +
  `attach_part_by_ref` let N reader databases mount the same object-store parts
  with zero bytes copied.

  See [Storage tiers & S3](#storage-tiers--s3) for the tier model, an S3 setup
  example and attach-by-reference.

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
  bloom-filter skips, SSTable probes), compaction failure count/latest error,
  cache hit/miss stats, `DB::reader_memory` and `DB::table_cache_bytes()`.
- **Maintenance** — checkpoint, backup, and column-family clone resolve tiered
  tables and materialize self-contained default-tier copies, plus per-CF and
  database stats. See [`docs/architecture.md`](docs/architecture.md).

### Modes

- **Unified memtable** (`unified_memtable`) — the whole database shares one
  memtable and one WAL, reducing per-CF overhead for many small/idle CFs.
  [Details below](#unified-memtable-optionsunified_memtable).

Not implemented / out of scope: read replicas, range compaction, Serializable
phantom protection (point-read validation only). See the non-goals in
[`AGENTS.md`](AGENTS.md).

## Builds

ondaDB ships a safe default build, two opt-in fast-path features, and an
independent object-store feature:

| Feature | `unsafe` | What it adds |
|---------|----------|--------------|
| **default** | one audited Linux clock call (`#![deny(unsafe_code)]`) | lock-free `crossbeam-skiplist` memtable, group-commit WAL, LRU caches, zero-allocation iterator |
| **`mmap-reads`** | one contained region | `mmap` zero-copy SSTable/vlog reads (helps point reads and SST-resident scans) |
| **`arena-memtable`** | one contained region | arena-backed skip-list memtable (chunked arena, one writer per shard, lock-free readers) |
| **`unsafe-fastpath`** | both regions | back-compat alias enabling `mmap-reads` + `arena-memtable` together |
| **`s3`** | none | S3-compatible object-store tier (range-GET reads, single-PUT writes); adds a tokio runtime used **only inside** the S3 backend |

```sh
cargo build                              # safe build (default)
cargo build --features unsafe-fastpath   # both fast paths
cargo build --features s3                # object-store tier support
cargo test                               # full suite, safe build
cargo test --features unsafe-fastpath    # same suite over the fast paths
```

The `s3` feature is orthogonal to the fast-path features and composes with any
build; it stays behind a flag so the core build pulls in no network dependencies.
Both fast-path configurations must stay green — the two builds compile different
memtable/reader code. See [`AGENTS.md`](AGENTS.md) for the CI-equivalent gate.

## Documentation

| Doc | Covers |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Module map, write/read/flush/compaction/recovery data flow, partitions, storage tiers, part lifecycle & mover, S3 tier |
| [`docs/formats.md`](docs/formats.md) | Every on-disk byte (yoloDB epoch 1): WAL segments, SSTable klog/vlog, manifest sections, config TLV, edit log, internal keys |
| [`docs/format-registry.md`](docs/format-registry.md) | The yoloDB registry: magics, versions, flags, capability bits, kinds, codec ids, config tags |
| [`docs/concurrency-and-safety.md`](docs/concurrency-and-safety.md) | Lock inventory & ordering, MVCC, rotation protocol, S3 runtime contract, every `unsafe` contract |
| [`docs/parts-and-tiers.md`](docs/parts-and-tiers.md) | User guide to partitions, parts and tiers — worked examples, S3 setup, attach-by-reference, operational notes |
| [`docs/compaction-and-write-pacing.md`](docs/compaction-and-write-pacing.md) | User guide to compaction geometry, bounded jobs, debt-based write pacing, close semantics, tuning by symptom |
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

// A transaction spanning multiple CFs is atomic only in unified-memtable
// mode. Per-CF WAL mode rejects such a commit with InvalidArgs.

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
(the footer's `FLAG_BTREE`). It is a per-column-family, opt-in on-disk format. The
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
iteration over bytewise CFs uses lazy prefix-bounded cursors without cloning the
shared memtable. Custom-comparator CFs materialize and re-sort their slice to restore
that comparator's order. Split flushes likewise re-sort per CF. Implemented in
[`src/unified.rs`](src/unified.rs).

## Storage tiers & S3

A **tier** is a named storage backend (`Options::tiers`). By default all data
lives in the database directory; a tier lets a column family's **aged bottom
parts** live somewhere else — a second disk, a no-mmap NFS-style mount, an
S3-compatible object store, or a caller-built `Storage` backend
(`TierDef::custom`). Per-CF `tier_rules` (a key prefix + a `min_age`) drive a
background **part mover** that relocates a part crash-safely once its newest
entry is old enough: copy → fsync → atomic manifest flip → delete source. WAL and
upper levels always stay local; only bottom-level parts move. Reads are
transparent — a moved part is served through its tier backend, and the manifest
remains the source of truth. Full guide, worked examples and operational notes:
[`docs/parts-and-tiers.md`](docs/parts-and-tiers.md).

**S3 tier** (cargo feature `s3`, MinIO-tested). Cold parts become objects in an
S3-compatible bucket. Block reads are bounded HTTP range GETs fronted by the
block cache (a cold data block is one GET, a warm one none); writes are
single-shot PUTs of whole, never-appended objects; every request carries a
bounded transport-level retry. No async runtime bleeds into the engine — the S3
backend owns a contained tokio runtime and the public API stays synchronous.
Because the block cache fully fronts a remote tier, size it up for S3 workloads.

```rust
use std::time::Duration;
use ondadb::{ColumnFamilyConfig, Options, S3Config, TierDef, TierRule, DB};

let s3 = S3Config {
    bucket: "archive".into(),
    region: "us-east-1".into(),
    endpoint: "https://s3.example.com".into(),   // e.g. a MinIO endpoint
    access_key: "…".into(),
    secret_key: "…".into(),
    path_style: true,                            // required by MinIO
};

let mut opts = Options::new("/data/onda");
// The tier root is an in-bucket KEY PREFIX, not a filesystem path.
opts.tiers = vec![TierDef::s3("s3", "onda-prod", s3)];
opts.block_cache_size = 512 << 20;               // S3 tiers lean on the cache

let db = DB::open(opts)?;
let cf = db.create_column_family("default", ColumnFamilyConfig {
    // Once an img/ part's newest entry is 30 days old, the mover puts its files
    // to the bucket; db.get(&cf, b"img/…") keeps working via range GETs.
    tier_rules: vec![TierRule {
        prefix: b"img/".to_vec(),
        tier: "s3".into(),
        min_age: Duration::from_secs(30 * 24 * 3600),
    }],
    ..ColumnFamilyConfig::default()
})?;
# let _ = cf;
# Ok::<(), ondadb::OndaError>(())
```

**Attach-by-reference over shared tiers.** Declaring a tier
`TierDef::s3(…).shared()` lets N reader databases mount the *same* object-store
parts with **zero bytes copied** (`DB::attach_part_by_ref`), for a
one-writer / many-disposable-reader topology (e.g. a search index over immutable
segments). Objects on a shared tier are named by a persisted per-database nonce
so writers sharing a root never collide, each attached table is CRC-verified on
attach, and shared tiers are delete-free — reclaiming objects is the coordinating
layer's job. See [`docs/parts-and-tiers.md`](docs/parts-and-tiers.md).

## Architecture / module map

Full data-flow walkthrough in [`docs/architecture.md`](docs/architecture.md).

```
src/
  config.rs        Options / ColumnFamilyConfig (+ defaults)
  config_blob.rs   the YOLODBCF config TLV (unknown tags preserved)
  error.rs         OndaError
  encoding.rs      varints, fixed ints, CRC32-C, xxHash32
  compress.rs      none/snappy/lz4/zstd/lz4fast/flate codecs
  bloom.rs         bloom filter (xxh3, leading hash tag)
  comparator.rs    6 built-ins + custom comparators
  format.rs        every on-disk number (epoch-1 magics, flags, caps, kinds, codecs, tags) + MVCC trailer
  block.rs         SSTable block framing (compress + checksum)
  cache/           byte-bounded LRU block cache + file-handle cache
  wal.rs           group-commit WAL (YOLODBWL segment headers)
  manifest.rs      durable catalog (YOLODBMF, flagged sections, atomic rewrite)
  legacy_onda/     frozen read-only 0.9.x decoders (feature "legacy-onda")
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

`just` is the discoverable command index for the quality, metrics, benchmark,
and setup recipes (`just --list`). The explicit Cargo commands below remain the
authoritative CI-equivalent reference. See [`metrics/README.md`](metrics/README.md)
for metric definitions, ratchets, baseline and history policy, and report paths.

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

Use `just --list` to discover standalone and sibling-harness benchmark recipes;
[`metrics/README.md`](metrics/README.md) documents their parameters, report
paths, and interpretation.

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
