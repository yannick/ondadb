# ondaDB — Agent Guide

ondaDB is a safe-Rust LSM key/value engine. 
Single crate, ~9k lines, no async runtime — std threads +
crossbeam channels. 

Deep documentation (read the one that matches your task):

| Doc | Covers |
|---|---|
| `docs/architecture.md` | Module map, write/read/flush/compaction/recovery data flow |
| `docs/formats.md` | Every on-disk byte: WAL frames, SSTable klog/vlog, manifest, internal keys |
| `docs/concurrency-and-safety.md` | Lock inventory & ordering, MVCC, rotation protocol, all `unsafe` contracts |
| `docs/performance.md` | Fast paths, benchmark methodology, known measurement artifacts |

## Build, test, verify

```sh
cargo build                                    # default: #![forbid(unsafe_code)]
cargo build --features unsafe-fastpath         # mmap reads + arena memtable
cargo test                                     # must pass in BOTH configs
cargo test --features unsafe-fastpath
cargo clippy --all-targets                     # must be clean in BOTH configs
cargo clippy --all-targets --features unsafe-fastpath
```

**Every change must keep both feature configurations green** — the two builds
compile different memtable/reader code (`memtable_arena.rs` and the mmap paths
exist only under `unsafe-fastpath`). CI-equivalent = 4 commands above.

Benchmarks (harness lives in `../bench`, compares 4 engines):

```sh
cd ../bench && ./run_bench.sh                  # one-shot side-by-side table
cd ../bench && RUNS=3 ./bench_graphs.sh        # single-config report + CSV
cd ../bench && ./bench_matrix.sh               # 5 key/value-size configs + HTML report
./target/release/onda_bench -ops 1000000 -threads 8   # onda alone (build with
   # cargo build --release --features unsafe-fastpath --bin onda_bench)
```

Benchmark results are **thermally noisy** on this machine (±15–20% run-to-run;
worse after sustained load). Never conclude from one run; compare same-run
ratios between engines, not absolute numbers across sessions. See
`docs/performance.md` for the full methodology and known artifacts.

## Critical invariants (violating any of these is a data-loss bug)

1. **Durability ordering on flush**: SSTable written → `sync_all` on klog+vlog →
   parent-dir fsync (all inside `Writer::finish`) → manifest persisted
   (`DbInner::persist_manifest`) → only if that returned `Ok` may WAL files be
   deleted (`wal::remove_wal_files`). Same ordering for compaction: manifest
   before input-file deletion.
2. **Manifest writes are serialized** by `DbInner::manifest_mu`; `Manifest::save`
   is temp-file + fsync + rename + dir-fsync. Never write the manifest outside
   `persist_manifest`.
3. **WAL batch atomicity**: one frame per committed batch. Replay must never
   surface a partial batch (frame CRC covers the whole payload).
4. **Every stored byte is checksummed**: WAL frames (CRC32-C), SSTable blocks
   (CRC32-C, verified at least once per open reader), vlog values (per-value
   CRC32-C prefix), manifest (whole-file CRC32-C). Adding a new persisted
   structure without a checksum is a regression.
5. **Sequence visibility is gap-free**: readers only see `visible_seq()`;
   `publish_range` advances it only when every lower range has completed. Never
   read at `next_seq`.
6. **Obsolete SSTable deletion goes through `DbInner::remove_sst_file`** so
   checkpoint/backup can pin the file set (`pause_deletions`). A bare
   `fs::remove_file` on an SST is a bug.
7. **Comparator stability**: a CF's comparator defines its on-disk order and is
   persisted by name in the manifest. The 8-byte **key-prefix compare trick**
   (used in the memtable, merge iterator, and flush merge) is only valid when
   `Comparator::is_bytewise()` — every prefix shortcut must fall through to the
   full comparison on prefix equality and must be gated on `bytewise`.
8. **Pinned-block lifetime**: the merge iterator returns `key()`/`value()`
   slices borrowed from per-child pinned `Block`s (`pinned_key`/`pinned_val` in
   `iterator.rs`). Key and value pins are separate arrays — the winning value
   may live in a later block than the group key; sharing a pin slot would
   invalidate one of them. Pins are refreshed only on block transitions —
   per-entry `Arc` clones of shared mmaps caused a measured 3× scan regression
   (see `docs/performance.md`).
9. **Rotation protocol**: writers hold `active_writers` for their entire
   `apply_commit`; rotation waits for drain before swapping the memtable. A
   sealed (imm) memtable is immutable — the zero-materialization flush cursors
   depend on it.

## Conventions

- All on-disk integers little-endian; varints are LEB128 (`encoding.rs`).
- Tests live in-module (`#[cfg(test)]`) plus integration tests in `tests/`
  (`db.rs`, `sst.rs`, `maintenance.rs`, `unified.rs`). Corruption/crash
  regressions get integration tests (see `corrupt_vlog_value_is_detected`,
  `concurrent_manifest_writes_survive_reopen`,
  `backup_consistent_during_compaction` for the pattern).
- Comments explain *why*; keep density similar to surrounding code.
- Performance work: profile first (`sample <pid>` on macOS during a bench
  phase), change one thing, re-measure ≥5 runs, and revert honestly if it
  regresses. Both previous regressions in this repo's history were caught
  this way.

## Known non-goals / out of scope (documented, not missing by accident)

Read replicas, Spooky compaction + Dynamic Capacity Adaptation (classic leveled
is implemented), range compaction, `Serializable` phantom protection (point-read
validation only — documented on `IsolationLevel::Serializable`),
rename/hot-reconfig of column families, write-amp statistics.

**S3 tiering (P7, behind the `s3` feature)** is implemented: `S3Storage`
(`storage_s3.rs`) is a no-mmap `Storage` backend that reads SSTable blocks with
HTTP range GETs (fronted by the block cache — a cold block is one GET, a warm one
none) and writes objects with single-shot PUTs. A `TierDef::s3(...)` tier plugs
into the existing part-mover/flip protocol unchanged. Known gaps (future work):
the crash-mid-move orphan sweep (`sweep_move_orphans`) and compaction's
obsolete-input delete are still local-path only, so an S3 object orphaned by a
crash-during-move or by compacting an S3-resident part is not GC'd (the manifest
stays the source of truth, so reads are never affected — only storage leaks).
ondaDB needs **no internal object CAS**: part objects use unique, never-reused ids
(one writer per key) and the commit point is the *local* manifest's fsync+rename,
not an S3 object — CAS on a shared S3 pointer is ayu's layer, not the engine's.
S3 tests are gated by `ONDADB_S3_ENDPOINT` (see `storage_s3.rs` / `tests/s3_tier.rs`).
