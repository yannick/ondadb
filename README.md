# ondaDB

A safe, performance-focused **Rust** key/value
LSM storage engine. 
`#![forbid(unsafe_code)]`.


## Highlights

- **100% safe Rust by default** (`#![forbid(unsafe_code)]`); an opt-in
  `unsafe-fastpath` feature adds two small, documented `unsafe` regions for
  C-class performance (see [Builds](#builds)).

## Features

- **Column families** — isolated, independently configured key/value stores, each
  with its own memtable, WAL and LSM levels.
- **MVCC transactions** with five isolation levels (`ReadUncommitted`,
  `ReadCommitted`, `RepeatableRead`, `Snapshot`, `Serializable`), write-write
  conflict detection (Snapshot/Serializable) and read-set validation
  (Serializable).
- **Savepoints** (partial rollback) and **commit hooks** (post-commit callbacks).
- **TTL** per key (lazy expiry on read, dropped during compaction).
- **WiscKey value separation** — values ≥ `klog_value_threshold` go to a vlog.
- **Bloom filters** (dense + sparse encodings) and a per-block index.
- **Leveled compaction** (L0→L1 by file count, Li→Li+1 by size), snapshot-aware
  version collapse and tombstone GC.
- **Group-commit WAL** (one `write`/`fsync` per batch of concurrent committers),
  three sync modes (`None`/`Full`/`Interval`), crash-safe replay.
- **Block cache** (sharded, byte-bounded LRU) and **file-handle cache**.
- **Bidirectional, snapshot-consistent iterators** (`seek`, `seek_for_prev`,
  forward/backward).
- **Maintenance**: checkpoint (hard-link), backup (copy), column-family clone,
  per-CF and database stats.
- **Six comparators** (`memcmp`, `reverse`, `lexicographic`, `uint64`, `int64`,
  `case_insensitive`) plus custom comparators.
- **B+tree hybrid klog** (`use_btree`) — see below.
- **Unified memtable** (`unified_memtable`) — see below.
- **Compression**: none / snappy / lz4 / zstd / lz4fast / flate (per column
  family, per SSTable block), with an optional **per-level policy**
  (`compression_per_level`, e.g. `[None, Zstd]` = hot L0 uncompressed,
  everything below Zstd).
- **Fail-stop durability (poisoning)** — any fsync/flush/manifest failure
  fail-stops the database: writes are rejected with `OndaError::Poisoned`
  (reads keep working, `DB::poisoned()` reports why) instead of silently
  retrying after the kernel may have dropped dirty pages.
- **Single-process lock** — a `LOCK` file (exclusive for read-write, shared
  for read-only opens) makes a second open fail with `OndaError::Locked`.
- **`sync_wal()`** — an explicit durability point for `SyncMode::None` /
  `Interval`: fsyncs every WAL; on `Ok`, everything committed before the call
  is on disk.
- **Bulk ingestion** — `DB::start_ingestion(&cf)` streams pre-sorted entries
  straight into L0 SSTables (no WAL, no memtable), rolls files at
  `write_buffer_size`, and installs them atomically at `finish()`.
- **Compaction filters** — `cf.set_compaction_filter(|key, value| ...)`
  drops (or tombstones) entries during compaction for custom GC/expiry.
- **FIFO compaction style** — `compaction_style: Fifo` with `fifo_max_bytes`
  / `fifo_ttl`: never merges, evicts the oldest tables whole (cache
  semantics, RocksDB-FIFO-style).
- **`clear_column_family()`** — atomically empty a CF, preserving its
  configuration.
- **Batch CF creation** (0.4.1) — `create_column_families(&[(name, config)])`
  creates many CFs with **one** manifest persist instead of one per CF
  (each persist is an `F_FULLFSYNC` + directory fsync on macOS). All-or-
  nothing on validation: a conflicting batch creates nothing. 11 CFs:
  209.8 ms per-CF → 22.5 ms batched. Use it whenever a consumer opens a
  fixed CF layout at boot.
- **Observability** — `approximate_len()` plus per-CF read counters
  (point reads, bloom-filter skips, SSTable probes) and cache hit/miss stats.
- **Partitions** (0.3.0) — prefix rules carve a CF's keyspace into named
  partitions; bottom-level compaction cuts its output at the boundaries, so
  each partition's bottom data is a clean, addressable **part**. Rules can be
  added/removed on a live CF (`add_partition_rule`).
- **Part lifecycle** (0.3.0) — ClickHouse-style `detach_part` / `attach_part`
  / `freeze_part`: drop a part from the catalog atomically, re-attach a
  same-lineage part, or export one as a standalone openable database.
- **Storage tiers + part mover** (0.3.0) — named tiers (`Options::tiers`):
  second disk, no-mmap NFS-style mounts, S3, or a caller-built `Storage`
  backend (`TierDef::custom`). Per-CF `tier_rules` (prefix + `min_age`) drive
  a background mover that relocates aged bottom parts crash-safely
  (copy → fsync → atomic manifest flip → delete source).
- **S3 tier** (0.3.0, cargo feature `s3`) — cold parts live in an
  S3-compatible object store (MinIO-tested): block reads become bounded HTTP
  range GETs fronted by the block cache (cold block = 1 GET, warm = 0),
  writes are single-shot PUTs; no async runtime bleeds into the engine.
  Since 0.4.1 every request carries a bounded retry (4 attempts, 25/50/100 ms
  backoff) on **transport-level** errors only — sound because every operation
  the backend issues is idempotent — closing the hyper keep-alive reuse race
  that intermittently killed requests with "connection closed before message
  completed".

See **[docs/parts-and-tiers.md](docs/parts-and-tiers.md)** for the full guide
to the 0.3.0 features (concepts, worked examples, S3 setup, operational
notes).

Not implemented: read replicas (intentionally out of scope).

## Benchmarks

**Live report: [yannick.github.io/ondadb](https://yannick.github.io/ondadb/)** —
interactive chartsets from an 8-engine head-to-head suite (TidesDB, RocksDB,
BadgerDB, fjall, surrealkv, wavesdb, Wildcat, ondaDB) across seven
key/value-size configurations: cross-config scoreboard, logical-MB/s tables and
per-phase charts with engine filtering. Two datasets:
[Linux 6.19 / xfs on NVMe](https://yannick.github.io/ondadb/) and
[macOS / Apple M2 Ultra](https://yannick.github.io/ondadb/m2ultra.html); raw
CSVs are linked on each page. Generated by the `bench/` suite in the sibling
`storage-engines` workspace.

## Builds

ondaDB ships two configurations:

| Build | `unsafe` | What it adds |
|-------|----------|--------------|
| **default** | none (`#![forbid(unsafe_code)]`) | lock-free `crossbeam-skiplist` memtable, group-commit WAL, LRU caches, zero-*allocation* iterator |
| **`--features unsafe-fastpath`** | two contained, documented regions | `mmap` zero-copy SSTable reads (klog/vlog) + an **arena-backed skip-list memtable** (chunked arena, one writer per shard, lock-free readers via `Release`/`Acquire`) |

```sh
cargo build                              # safe build
cargo build --features unsafe-fastpath   # performance build
cargo test                               # 137 tests, safe build
cargo test --features unsafe-fastpath    # same suite over the fast path
```


## Documentation

| Doc | Covers |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Module map, write/read/flush/compaction/recovery data flow, partitions, storage tiers, part lifecycle & mover, S3 tier |
| [docs/formats.md](docs/formats.md) | Every on-disk byte: WAL frames, SSTable klog/vlog, manifest (incl. the 0.3.0 append-tolerant tail), internal keys |
| [docs/concurrency-and-safety.md](docs/concurrency-and-safety.md) | Lock inventory & ordering, MVCC, rotation protocol, S3 runtime contract, `unsafe` contracts |
| [docs/parts-and-tiers.md](docs/parts-and-tiers.md) | User guide to partitions, parts and tiers — worked examples, S3 setup, operational notes |
| [docs/performance.md](docs/performance.md) | Fast paths, benchmark methodology, known measurement artifacts |

## Usage

```rust
use std::time::Duration;
use ondadb::{DB, Options, ColumnFamilyConfig, IsolationLevel};

let db = DB::open(Options::new("/tmp/onda"))?;
let cf = db.create_column_family("default", ColumnFamilyConfig::default())?;

// Opening a fixed multi-CF layout? Create them as ONE batch — one manifest
// persist (two fsyncs) for the lot instead of two per CF:
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
- You expected an immediate *memory*/seek speedup at small sizes: ondaDB currently
  rebuilds a flat in-memory index from the tree on open, so today the win is the
  on-disk format and it materializes only as SSTables grow (lazy per-seek descent
  is future work).

**Notes:**

- `use_btree` is **per column family** and applies only to **newly written**
  SSTables. Flipping it does not rewrite existing files — they keep their format
  and stay readable (the format is recorded per file in the footer). A full
  compaction will migrate older files to the new format over time.
- The data-block format is identical either way, so it composes freely with
  compression, bloom filters, WiscKey value separation and all comparators.
- Rule of thumb: leave it **off** unless you have measured large SSTables and a
  read-bound workload; it is a large-scale tuning knob, not a general default.

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

Point reads work under any per-CF comparator (exact prefixed-key lookup); ordered
iteration and flush re-sort a CF's slice with that CF's comparator. Implemented in
[`src/unified.rs`](src/unified.rs).

## Architecture / module map

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
  cache/           CLOCK-ish LRU block cache + file-handle cache
  wal.rs           group-commit WAL
  manifest.rs      durable catalog (atomic rewrite)
  memtable.rs      sharded MVCC memtable (crossbeam, or arena under fastpath)
  memtable_arena.rs  arena skip-list shard (unsafe-fastpath only)
  sst/             SSTable writer/reader/iterator (+ B+tree hybrid klog)
  storage.rs       Storage trait + LocalStorage + tier registry
  storage_s3.rs    S3 tier backend (feature "s3")
  parts.rs         part lifecycle (detach/attach/freeze) + part mover
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

The integration suite includes a port of [fjall](https://github.com/fjall-rs/fjall)'s
engine-generic tests ([`tests/fjall_suite.rs`](tests/fjall_suite.rs)) — batch
atomicity, recovery loops, snapshot isolation, prefix scans, large-value WAL
replay, DB locking — kept name-for-name so the two suites diff side by side.


## Benchmarks

ondaDB is wired into the shared harness at [`../bench`](../bench) (its standalone
binary mirrors the C harness and the Go output format). Run
`./bench_graphs.sh` there to regenerate the four-engine charts.

