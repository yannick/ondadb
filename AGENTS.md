# ondaDB — Agent Guide

ondaDB is a safe-Rust LSM key/value engine. 
Single crate, ~9k lines, no async runtime — std threads +
crossbeam channels. 

Deep documentation (read the one that matches your task):

| Doc | Covers |
|---|---|
| `docs/architecture.md` | Module map, write/read/flush/compaction/recovery data flow |
| `docs/formats.md` | Every on-disk byte (yoloDB format epoch 1): WAL segments and frames, SSTable klog/vlog, manifest + `MANIFEST-EDITS`, config TLV, internal keys; appendix on the 0.9 formats |
| `docs/format-registry.md` | The yoloDB registry: every magic, version, flag, capability bit, kind, codec id, TLV tag and edit op |
| `docs/concurrency-and-safety.md` | Lock inventory & ordering, MVCC, rotation protocol, S3 runtime/blocking contract, all `unsafe` contracts |
| `docs/parts-and-tiers.md` | User-facing guide to partitions, parts and storage tiers (0.3.0): concepts, worked examples, S3 setup, operational notes |
| `docs/performance.md` | Fast paths, benchmark methodology, known measurement artifacts |

## Build, test, verify

```sh
cargo build                                    # default: deny unsafe; one audited Linux clock call
cargo build --features unsafe-fastpath         # mmap reads + arena memtable
cargo test                                     # must pass in ALL THREE configs
cargo test --features unsafe-fastpath
cargo test --no-default-features               # without legacy-onda (the 0.9 decoders)
cargo clippy --all-targets -- -D warnings      # must be clean in ALL THREE configs
cargo clippy --all-targets --features unsafe-fastpath -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
cargo build --features s3                      # the S3 tier compiles
```

**Every change must keep all three feature configurations green** — the builds
compile different code: `memtable_arena.rs` and the mmap paths exist only under
`unsafe-fastpath`, and `src/legacy_onda/` (read-only 0.9.x decoders, default-on
feature `legacy-onda`) exists only with default features. CI-equivalent = the
seven commands above.

When scripting the gate, check each test binary for the *presence of*
`test result: ok`, not the *absence of* `FAILED`, and never pipe `cargo test`
through `tail` — it shows only the last binary's result and silently hides a
failure in any earlier one. A known intermittent `unsafe-fastpath` failure
(`read_your_writes`, see `docs/concurrency-and-safety.md`) was masked this way.

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
   parent-dir fsync (all inside `Writer::finish`) → **the catalog edit appended
   and fsynced to `MANIFEST-EDITS`** (`DbInner::catalog_txn`) → only if that
   returned `Ok` may WAL files be deleted (`wal::remove_wal_files`). Same
   ordering for compaction: edit fsync before input-file deletion.
   Snapshot compaction is a **space optimization and never a durability
   precondition** — nothing keys off a snapshot write. Without
   `CAP_MANIFEST_EDITS` there is no log and `catalog_txn` falls back to the
   pre-2.2 full rewrite (`DbInner::persist_manifest`), which is then the gate;
   the ordering is otherwise identical.
   Every catalog mutation goes through `catalog_txn`, which publishes **after**
   the durable edit and hands the publish closure a `db::Publish` token. The six
   level-set primitives (`install_handles_l0`, `update_levels`, `install_levels`,
   `remove_bottom_tables`, `insert_bottom_sorted`, `swap_bottom_tables`), the
   `remove_l0_tables`/`publish_flush` halves, the CF-registry insert/remove and
   the partition-rule append/remove all demand that token, so publishing outside
   a transaction does not compile. The one documented exception is
   `DbInner::prepare_capability`, which publishes through a full snapshot
   rewrite because it is the path that turns the edit log on.
2. **Manifest writes are serialized** by `DbInner::manifest_mu`; `Manifest::save`
   is temp-file + fsync + rename + dir-fsync (the dir fsync propagates its
   error). Never write the manifest outside `persist_manifest`. Under 2.2's
   `CAP_MANIFEST_EDITS` the same lock covers edit-log appends *and* snapshot
   compaction, in one critical section: compaction renames a fresh
   `MANIFEST-EDITS` over the live one, so a record appended in between would be
   silently lost. `DbInner::catalog_txn` is the only thing that may append.
3. **WAL batch atomicity**: one frame per committed batch. Replay must never
   surface a partial batch (frame CRC covers the whole payload).
4. **Every stored byte is checksummed**, with **CRC32-C** (Castagnoli,
   `encoding::checksum` via the `crc32c` crate — pinned by the check value
   `"123456789"` → `0xE3069283`): WAL segment headers and frames, SSTable
   blocks and the 96-byte footer, vlog headers and values (per-value CRC
   prefix), manifest (whole-file), edit-log header and every edit record.
   (ondaDB 0.9.x claimed CRC32-C but computed CRC-32/IEEE, and left its SST
   footer unchecksummed; IEEE now exists only in `legacy_onda` to read 0.9
   files.) Blocks, vlog headers and vlog frames are verified **at least once per open
   reader** — never fewer (the first read always checks, and a frame that fails
   is never marked verified), and re-verified on re-open. Adding a new persisted
   structure without a checksum is a regression.
5. **Sequence visibility is gap-free**: readers only see `visible_seq()`;
   `publish_range` advances it only when every lower range has completed. Never
   read at `next_seq`.
6. **Obsolete SSTable deletion goes through `DbInner::remove_sst_file`** so
   checkpoint/backup can pin the file set (`pause_deletions`). A bare
   `fs::remove_file` on an SST is a bug.
7. **Comparator stability**: a CF's comparator defines its on-disk order and is
   persisted by name in the CF config blob (TLV tag 1). The 8-byte **key-prefix compare trick**
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
10. **Format-upgrade swap** (`upgrade.rs`): the 0.9 source directory is
   **never written before the swap** (only its `LOCK` is taken, exclusively,
   for the whole run); the rebuild's `MANIFEST` is written last; the swap
   journal is written only **after** verification passed, and it is the only
   thing that may authorize renaming the source. Every crash-recovery branch
   either rolls a *complete* rebuild forward or renames the untouched source
   back — none deletes the source or the backup (only `format_upgrade_keep_backup
   = false` does, after the upgraded database opened). A read-only open writes
   neither a journal nor a 0.9 directory.

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

## On-disk format: yoloDB epoch 1

Since 0.10 ondaDB writes **yoloDB format epoch 1** (plan C,
`docs/plans/phase-c-yolodb-convergence/plan.md`): the magics `YOLOST01` (SST
footer), `YOLODBMF` (manifest), `YOLODBED` (edit log), `YOLODBWL` (WAL segment
header), `YOLODBVL` (vlog header) and `YOLODBCF` (config TLV), CRC32-C
everywhere, restart trailers on every data block, codec id 6 for LZ4 (2 and 4
burned), a leading bloom hash tag and the correct FNV-1a-64 basis for unified
CF ids. `src/format.rs` is the single home for every number, each pinned by a
`const` assertion and a golden test (`tests/epoch1_golden.rs` over
`tests/fixtures/epoch1/`); `docs/format-registry.md` is the registry. Fail
closed: malformed bytes are `Corruption`; an unknown version, flag, capability
bit, codec, kind or config enum value is `UnsupportedFormat`.

A 0.9.x directory is **not** readable by the epoch-1 engine. The frozen 0.9
decoders live in `src/legacy_onda/` (default-on feature `legacy-onda`,
decode-only, pinned by `tests/fixtures/legacy-onda/`), and
`legacy_onda::open_read_only` opens a 0.9 database read-only through the
engine. `DB::open` **upgrades a 0.9 directory automatically** (plan C §1.3,
`upgrade.rs`, `Options::format_upgrade` = `Auto` | `Forbid` |
`ReadOnlyLegacy`): a one-for-one transcode into a sibling directory, verified,
then swapped in under a journal that the next open resolves after a crash;
`yolodb upgrade <path>` runs it offline. Never add a 0.9 *encoder* outside a
`#[cfg(test)]` fixture builder.

## Optional format capabilities

Seven `CAP_*` bits in the manifest's capability word gate the optional
artifacts. Each is **opt-in and one-way**: `DB::enable_format_capabilities`
persists the bit before the first byte using it exists, and from then on the
database is unreadable by a binary that does not implement it. Each SSTable also
declares the subset its bytes use in its footer's capability word.

| Bit | Capability | Turns on |
|---|---|---|
| `1<<0` | `CAP_EXTENDED_RECORDS` | Kind-bearing WAL envelopes and SSTable entries (1.0) |
| `1<<1` | `CAP_MERGE_OPERANDS` | Kind 4, merge operands (1.1); taken automatically when a family with a merge operator is created |
| `1<<2` | `CAP_RANGE_DELETES` | Kind 5, range tombstones and the SSTable aux fragment section (1.2); implies `CAP_EXTENDED_RECORDS` |
| `1<<3` | `CAP_PREFIX_DELTA` | Prefix-delta data blocks (2.1) |
| `1<<4` | `CAP_MANIFEST_EDITS` | The `MANIFEST-EDITS` log (2.2) |
| `1<<5` | `CAP_PERIODIC_AGE` | Durable `last_compaction_time` (0.3) |
| `1<<6` | `CAP_TXN_DECISIONS` | Kinds 16-18, prepared transactions (3.2); implies `CAP_EXTENDED_RECORDS` |

The values are **pinned for wavesdb compatibility** and never reused, as are
the record kinds (1 put, 2 delete, 3 single-delete, 4 merge, 5 range delete,
6-15 reserved data kinds, 16-31 transaction control, 32-63 reserved,
`> 63` never assigned). A kind above 63 is `Corruption` — no writer of any
vintage produces it; an assigned kind this binary does not implement is
`UnsupportedFormat`. That split is the whole point of the reservation, and
`src/format.rs` is where it is enforced.

## Where the newer features live

| Feature | Module | Notes |
|---|---|---|
| Merge operators (1.1) | `column_family.rs` (`fold_point_chain`), `iterator.rs` (`resolve_merging_group`), `compaction.rs` (`PendingFold`) | Folding is a space/read optimization, never a correctness requirement; `Options::enable_merge_folding` is its rollout switch |
| Range tombstones (1.2) | `range_tombstone.rs`, `span_index.rs` | A commit holding a span takes `commit_mu` at every isolation level |
| Delete-only excise (1.2) | `excise.rs` | Retires a whole table by catalog edit without reading it |
| Prepared transactions (3.2) | `prepared.rs`, `txn.rs`, `db.rs` | Unified layout only |
| Pessimistic locking (3.3) | `txn_lock.rs`, `txn.rs`, `db.rs` | Opt-in per transaction (`DB::begin_pessimistic`); point locks only, wait-die, and a snapshot refresh on grant that costs `Snapshot` its read-skew guarantee |
| Prefix-delta blocks (2.1) | `sst/mod.rs` (`encode_entry_delta`) | |
| Manifest edit log (2.2) | `manifest_edit.rs` | See invariants 1 and 2 |
| PerfContext (0.10) | `perf.rs` | |
| Read profiling (F13) | `read_profile.rs` | DB-wide opt-in aggregate of `PerfContext` counters; off = one relaxed load per read |
| IO classes / rate limiter (0.6) | `ioctrl.rs` | |
| Tailing iterators (0.9) | `tailing.rs` | |
| Clear under the unified layout (F5′) | `db.rs` (`clear_column_family`, `DbInner::choose_unified_id`), `unified.rs` (`holds_cf`), `manifest_edit.rs` (`SetCfUnifiedId`) | Every name→id lookup uses the family's **stored** id (`ColumnFamily::id`, `cf_by_id`); `unified::cf_id(name)` is only the default. Tests: `tests/unified_clear.rs` |
| 0.9 → epoch-1 upgrade (plan C §1.3) | `upgrade.rs`, `legacy_onda/`, `src/bin/yolodb.rs` | See invariant 10; crash matrix in `tests/format_upgrade.rs` (fault hook: `UpgradeObserver`) |
| Wide-column entities (F11) | `entity.rs` | Value-level frame shared with wavesdb (`WVE1`); no engine change |

**Cross-feature rules live in `tests/composition.rs`**, not in either feature's
own file: a range delete is, for one key, a *deleted base at its sequence* (so a
merge chain's operands above the span survive and fold onto nothing), and a
prepare frame carries a merge operand but never a range delete. Adding a
feature that interacts with an existing one belongs there.

## Known non-goals / out of scope (documented, not missing by accident)

Read replicas, Spooky compaction + Dynamic Capacity Adaptation (classic leveled
is implemented), range compaction, `Serializable` phantom protection (point-read
validation only — documented on `IsolationLevel::Serializable`),
rename/hot-reconfig of column families, write-amp statistics. Also: coordinator
election, consensus and automatic abort for prepared transactions (3.2 is a
*participant* only), **span locks** and phantom protection for pessimistic
transactions (3.3 is point locks only — a range delete in a pessimistic
transaction takes no lock at all), managed sequence mode, large-transaction
private spill, and tiered/lazy-leveling compaction.

**Two-phase commit (3.2) is implemented** and is no longer a non-goal:
`Txn::prepare` / `DB::commit_prepared` / `abort_prepared` / `list_prepared`,
behind `CAP_TXN_DECISIONS` (which implies `CAP_EXTENDED_RECORDS`), unified
layout only. ondaDB is a **participant**,
never a coordinator — it holds durable prepared state and resolves it by a
stable external id; election, consensus and timeout decisions are ayu's layer,
and **nothing is ever aborted automatically**. Two consequences bind every
change to the write path: every commit now takes `commit_mu` and probes the
reservation registry (phase rule 5 — including the single-op `put`/`delete`
path, which took no lock before), and a unified WAL generation may not be
unlinked while a prepare or a not-yet-durable decision lives in it. See
`docs/concurrency-and-safety.md` § Prepared transactions.

**Known cost, measured:** rule 5 is free for a single writer but costs **~3×
write throughput at 8 concurrent writers** (`bench-results/3.2/2026-08-30/`),
because `commit_mu` is held across the whole apply and the three low isolation
levels never took it before. Do not "fix" this by moving the reservation check
outside the lock — that is the TOCTOU the rule exists to close. Shrinking the
critical section is RV-M3's job.

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
Full feature guide: `docs/parts-and-tiers.md`.
