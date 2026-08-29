# ondaDB 0.8.2 Code-Review Corrective Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the validated correctness, durability, robustness, and low-severity defects from the August 2026 code review and cut a verified local `v0.8.2` release.

**Architecture:** Preserve the existing LSM, WAL, and manifest formats except for merging the already-released `v0.8.1` block-size tail. Close data-loss windows at existing storage/durability choke points, reject unsupported per-CF multi-CF atomic commits before mutation, and add bounded counters/wrappers rather than new subsystems. Findings requiring new commit or manifest protocols remain explicitly deferred.

**Tech Stack:** Rust 2021, std threads/filesystem, crossbeam channels/skip lists, parking_lot, in-module and integration tests, Cargo.

**Spec:** `docs/superpowers/specs/2026-08-29-code-review-corrective-release-design.md`

## Global Constraints

- Preserve all nine critical invariants in `AGENTS.md`.
- Every production behavior change begins with a failing automated test and an observed expected failure.
- Both default and `unsafe-fastpath` configurations must remain green.
- Never delete an obsolete SST outside `DbInner::remove_sst_file`; startup orphan cleanup is the explicit pre-worker exception for files absent from the manifest.
- Do not change the manifest, WAL, SSTable, or internal-key formats except by carrying the tagged `v0.8.1` `ONDABLK1` tail forward.
- Keep `docs/code-review-2026-08.md` intact; record dispositions separately.
- Do not push, publish a crate, or create a hosted release.

## File structure

- `src/maintenance.rs`: tier-aware durable snapshot and clone copying.
- `src/parts.rs`: staged-overlap placement and durable physical attach copying.
- `src/txn.rs`: per-CF multi-CF rejection, savepoint read rollback, reset floor.
- `src/wal.rs`, `src/util.rs`: WAL-create parent-directory durability.
- `src/unified.rs`, `src/iterator.rs`, `src/memtable.rs`, `src/column_family.rs`: lazy unified bytewise iterator children and explicit merge ties.
- `src/db.rs`: stable DB IDs, worker count, orphan sweep, per-CF flush completion, recorded compaction errors.
- `src/compaction.rs`, `src/maintenance.rs`: compaction error statistics.
- `src/config.rs`, `src/compaction.rs`, `src/column_family.rs`: `v0.8.1` per-CF data block size.
- `tests/maintenance.rs`, `tests/parts.rs`, `tests/db.rs`, `tests/unified.rs`, `tests/engine_regressions.rs`, `tests/data_block_size.rs`: public regression coverage.
- `README.md`, `AGENTS.md`, `docs/architecture.md`, `docs/concurrency-and-safety.md`, `docs/formats.md`, `docs/parts-and-tiers.md`: corrected contracts.
- `docs/code-review-2026-08-resolution.md`: finding-by-finding evidence and disposition.
- `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`: release metadata.

---

### Task 1: Carry `v0.8.1` block-size behavior onto `main`

**Files:**
- Create: `tests/data_block_size.rs`
- Modify: `src/config.rs`
- Modify: `src/column_family.rs`
- Modify: `src/compaction.rs`
- Add: `SPADINO-A10.md`

**Interfaces:**
- Produces: `ColumnFamilyConfig::data_block_size: usize`, default `4 << 10`, encoded in the `ONDABLK1` config tail.
- Consumed by: `ColumnFamily::writer_opts` and compaction writer options.

- [ ] **Step 1: Add the released tests without its implementation**

Use `git show v0.8.1:tests/data_block_size.rs` as the exact source and add that file with `apply_patch`. Add `SPADINO-A10.md` from the same tag unchanged.

- [ ] **Step 2: Run the test to verify RED**

Run: `cargo test --test data_block_size`

Expected: compilation fails because `ColumnFamilyConfig` has no `data_block_size` field.

- [ ] **Step 3: Port the minimal implementation into the refactored mainline code**

Add this field and default:

```rust
pub data_block_size: usize,
// Default:
data_block_size: 4 << 10,
```

Add `CONFIG_BLOCK_SIZE_MAGIC: &[u8; 8] = b"ONDABLK1"`; append the tagged tail only when the value differs from `4 << 10`; decode it append-tolerantly after existing config tails; reject zero in `ColumnFamilyConfig::validate`.

Replace the two hardcoded block-size sites with:

```rust
block_size: self.opts.data_block_size,
// and
block_size: cf.opts.data_block_size,
```

- [ ] **Step 4: Verify GREEN in both configurations**

Run: `cargo test --test data_block_size`

Run: `cargo test --features unsafe-fastpath --test data_block_size`

Expected: all data-block-size tests pass.

- [ ] **Step 5: Commit the port and record the released ancestry**

Commit the tested mainline port first:

```bash
git add SPADINO-A10.md tests/data_block_size.rs src/config.rs src/column_family.rs src/compaction.rs
git commit -m "feat: carry per-family block size onto main"
```

Then join the already-published sibling history without replaying its
pre-refactor files over main's quality-sweep structure:

```bash
git merge -s ours --no-ff v0.8.1 -m "merge: carry v0.8.1 release ancestry"
```

### Task 2: Make checkpoints, backups, and clones self-contained across tiers

**Files:**
- Modify: `tests/maintenance.rs`
- Modify: `src/maintenance.rs`

**Interfaces:**
- Consumes: `ColumnFamily::klog_path_for`, `TierRegistry::storage_for`, `ReadHandle::{size,read_exact_at}`.
- Produces: private `copy_storage_file(storage, src, dst) -> Result<()>` and `snapshot_source(cf, meta, ext) -> (Arc<dyn Storage>, String)` helpers.

- [ ] **Step 1: Write failing tiered snapshot and clone tests**

Add `tiered_snapshot_and_clone_are_default_tier_self_contained`. Its fixture creates an `img/` partition, moves it to a local `hdd` tier, runs `checkpoint`, `backup`, and `clone_column_family`, closes the source, renames the tier root out of reach, and opens both snapshots with plain `Options::new`. Assert `img/000` and a default-tier key are readable, and assert the clone remains readable after the source tier root is unavailable.

The production mutation this catches is resolving a manifest table through `cf_dir` instead of its `SstMeta::tier`/`object`.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test maintenance tiered_snapshot_and_clone_are_default_tier_self_contained -- --exact --nocapture`

Expected: snapshot reopen or clone read fails with `Io(NotFound)` for the tiered table.

- [ ] **Step 3: Implement tier-aware durable copying**

For each metadata item, obtain the live CF by name, compute klog/vlog paths from `klog_path_for`, and read through `storage_for(meta.tier.as_deref())`. Copy in fixed-size chunks using `read_exact_at`; `sync_all` each destination file and sync its CF directory before saving the destination manifest.

For checkpoints, attempt `hard_link` only when the source exists as a local path; on `CrossesDevices` or a non-local source, use the storage copy helper. Backups always copy. Treat a missing klog as an error; skip only a vlog whose metadata size is zero.

Before `manifest.save`, mutate the copied manifest's tables:

```rust
for table in &mut cfm.sstables {
    table.tier = None;
    table.object = None;
}
```

For clones, use fresh IDs, copy or link from `src_cf.klog_path_for(&meta)`, clear `tier`/`object`, and open the fresh default-tier metadata.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test maintenance tiered_snapshot_and_clone_are_default_tier_self_contained -- --exact --nocapture`

Run: `cargo test --test maintenance`

Expected: all maintenance tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/maintenance.rs tests/maintenance.rs
git commit -m "fix: make tiered snapshots self-contained"
```

### Task 3: Keep mutually overlapping attached tables out of the bottom level

**Files:**
- Modify: `tests/parts.rs`
- Modify: `src/parts.rs`

**Interfaces:**
- Produces: private comparator-aware `ranges_overlap(cmp, a_min, a_max, b_min, b_max) -> bool` and staged placement tracking shared by physical and by-reference attach.

- [ ] **Step 1: Write the failing physical-attach regression**

Add `attach_mutually_overlapping_staged_tables_uses_l0`. Detach one materialized partition, duplicate its klog and optional vlog under a second numeric filename in the detached directory, then attach the directory once. Assert exactly one of the duplicate ranges can increase the bottom-level count and at least one table lands in L0; verify every key reads correctly.

The production mutation this catches is checking only `bottom_overlaps` and forgetting earlier staged extents.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test parts attach_mutually_overlapping_staged_tables_uses_l0 -- --exact --nocapture`

Expected: both staged tables enter the bottom level, so the L0/count assertion fails.

- [ ] **Step 3: Implement staged overlap classification in both attach paths**

Maintain `bottom_extents: Vec<(Vec<u8>, Vec<u8>)>` for tables provisionally assigned to bottom. A table is `at_bottom` only when `!cf.bottom_overlaps(...)` and it overlaps no extent in `bottom_extents`; append its extent only when assigned bottom. Reuse the same helper in `attach_part_by_ref`.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test parts attach_mutually_overlapping_staged_tables_uses_l0 -- --exact --nocapture`

Run: `cargo test --test parts attach_overlapping_range_goes_to_l0 -- --exact`

Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add src/parts.rs tests/parts.rs
git commit -m "fix: validate staged attach overlap"
```

### Task 4: Durably finish physical attach copies before manifest publication

**Files:**
- Modify: `src/parts.rs`
- Modify: in-module tests in `src/parts.rs`

**Interfaces:**
- Produces: `copy_into_storage(src: &Path, storage: Arc<dyn Storage>, dst: &str) -> Result<()>` that always calls `StorageWriter::finish`.

- [ ] **Step 1: Write a failing finish-propagation test**

Add an in-module fake `StorageWriter` whose `finish` returns `OndaError::Io(Error::other("finish failed"))`. Call the copy helper with a valid temporary source and assert that exact error is returned. The production mutation this catches is replacing the helper with `std::fs::copy` or omitting `finish`.

- [ ] **Step 2: Verify RED**

Run: `cargo test parts::tests::attach_copy_propagates_storage_finish_failure -- --exact`

Expected: compilation fails because `copy_into_storage` does not exist.

- [ ] **Step 3: Implement durable attach copy**

Open the source with `File::open`, create the destination through `cf.tiers().storage_for(None).create`, stream with `std::io::copy`, and call `finish`. Use it for both klog and vlog before opening readers or staging handles.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test parts::tests::attach_copy_propagates_storage_finish_failure -- --exact`

Run: `cargo test --test parts detach_hides_then_attach_restores_and_preexisting_iterator_unaffected -- --exact`

Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add src/parts.rs
git commit -m "fix: finish attach copies before manifest flip"
```

### Task 5: Reject non-atomic per-CF multi-CF transactions

**Files:**
- Modify: `tests/db.rs`
- Modify: `src/txn.rs`

**Interfaces:**
- Produces: commit-time `InvalidArgs` for more than one distinct CF when `DbInner::unified` is `None`.

- [ ] **Step 1: Write failing public behavior tests**

Add `per_cf_multi_cf_commit_is_rejected_without_partial_apply`: buffer one put in each of two CFs, call commit, assert `invalid_args`, and assert both keys remain `NotFound`. Add or retain a unified counterpart that commits, reopens, and reads both keys.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test db per_cf_multi_cf_commit_is_rejected_without_partial_apply -- --exact`

Expected: commit returns `Ok` and both writes become visible.

- [ ] **Step 3: Implement pre-apply rejection**

After `prepare_commit` and before locking, validation, sequence reservation, or `apply_prepared`, derive distinct CF pointer IDs from `prepared.order`. If `self.db.unified.is_none()` and more than one exists, release the transaction and return:

```rust
OndaError::InvalidArgs(
    "multi-column-family transactions require unified_memtable=true for atomic commit".into()
)
```

- [ ] **Step 4: Verify GREEN and unified behavior**

Run: `cargo test --test db per_cf_multi_cf_commit_is_rejected_without_partial_apply -- --exact`

Run: `cargo test --test unified explicit_migration_preserves_per_cf_data_and_one_cross_cf_txn_syncs_once -- --exact`

Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add src/txn.rs tests/db.rs
git commit -m "fix: reject non-atomic per-CF cross-family commits"
```

### Task 6: Fsync newly created WAL directory entries

**Files:**
- Modify: `src/util.rs`
- Modify: `src/wal.rs`
- Modify: in-module tests in `src/wal.rs`

**Interfaces:**
- Produces: `util::sync_parent_dir(path: &Path) -> Result<()>`; `Wal::open_inner(..., sync_parent)` test seam.

- [ ] **Step 1: Write a failing newly-created-WAL sync test**

Add `new_wal_creation_propagates_parent_sync_failure`. Call `Wal::open_inner` with a closure that increments an atomic and returns an injected error. Assert the open fails and the closure ran once. Add `existing_wal_does_not_require_creation_sync` asserting the closure is not called on reopen.

- [ ] **Step 2: Verify RED**

Run: `cargo test wal::tests::new_wal_creation_propagates_parent_sync_failure -- --exact`

Expected: compilation fails because `open_inner`/the sync seam does not exist.

- [ ] **Step 3: Implement creation detection and shared directory sync**

For every stripe path, record `created |= !stripe.exists()` before `OpenOptions`. After all stripes open successfully, call the injected sync function once when `created`. Public `Wal::open` passes `crate::util::sync_parent_dir`. Hoist equivalent code from `sst/writer.rs` to `util.rs` and use the shared helper there too.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test wal::tests::`

Run: `cargo test wal::tests`

Expected: WAL tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/util.rs src/wal.rs src/sst/writer.rs
git commit -m "fix: sync WAL parent on file creation"
```

### Task 7: Add a zero-materialization unified iterator for bytewise CFs

**Files:**
- Modify: `src/memtable.rs`
- Modify: `src/unified.rs`
- Modify: `src/iterator.rs`
- Modify: `src/column_family.rs`
- Modify: `tests/unified.rs`

**Interfaces:**
- Produces: `UnifiedMemIter`, matching the `MemIter` accessor/seek surface while stripping one fixed CF prefix; `UnifiedStore::iterators_for_cf(id) -> Vec<UnifiedMemIter>`.
- Consumed by: new `ChildIter::Unified` variant.

- [ ] **Step 1: Write failing behavior and materialization tests**

Extend unified iteration coverage to forward/backward direction switches, `seek`, `seek_for_prev`, and declared bounds. Add a test-only `SNAPSHOT_CALLS` counter in `Memtable::snapshot`; reset it, construct and walk a bytewise unified iterator containing entries in two CFs, and assert the counter stays zero.

Also add an ignored `unified_iterator_construction_probe` that constructs and
seeks a one-record iterator repeatedly with a populated second CF and prints
elapsed nanoseconds. Run it five times now, before production changes:

```bash
cargo test --release --test unified unified_iterator_construction_probe -- --ignored --exact --nocapture
```

- [ ] **Step 2: Verify RED**

Run: `cargo test --test unified unified_iteration_does_not_materialize_shared_memtable -- --exact`

Expected: snapshot counter is nonzero because `entries_for_cf` calls `snapshot`.

- [ ] **Step 3: Implement `UnifiedMemIter`**

Wrap an owned `MemIter` plus `[u8; 8]`. `seek_to_first` seeks to `(prefix, u64::MAX)`; `seek_to_last` seeks immediately before the next big-endian prefix (or from the end for `u64::MAX`); user-key seeks prepend the prefix; `next`/`prev` invalidate when the current key no longer starts with the prefix. Accessors proxy sequence, TTL, tombstone, and value while `user_key()` returns `inner.user_key()[8..]`.

Add `ChildIter::Unified` dispatch. In `append_memtable_children`, use lazy unified children only when `self.cmp.is_bytewise()`; retain `entries_for_cf` materialization for custom comparators.

- [ ] **Step 4: Verify GREEN in both memtable builds**

Run: `cargo test --test unified`

Run: `cargo test --features unsafe-fastpath --test unified`

Expected: all unified tests pass and snapshot count stays zero for bytewise CFs.

- [ ] **Step 5: Measure five post-change release-mode construction runs**

Run the ignored construction probe five times after the implementation using:

```bash
cargo test --release --test unified unified_iterator_construction_probe -- --ignored --exact --nocapture
```

Record medians and same-run ratios in `docs/code-review-2026-08-resolution.md`; revert the fast path if it regresses.

- [ ] **Step 6: Commit**

```bash
git add src/memtable.rs src/unified.rs src/iterator.rs src/column_family.rs tests/unified.rs
git commit -m "perf: iterate unified bytewise slices lazily"
```

### Task 8: Expose compaction failures in CF statistics

**Files:**
- Modify: `src/column_family.rs`
- Modify: `src/compaction.rs`
- Modify: `src/db.rs`
- Modify: `src/maintenance.rs`
- Modify: `tests/maintenance.rs`

**Interfaces:**
- Produces: `CfStats::compaction_failures: u64`, `CfStats::last_compaction_error: Option<String>`, and `ColumnFamily::record_compaction_failure`.

- [ ] **Step 1: Write a failing corruption-to-stats test**

Write a large vlog-separated value, flush it, corrupt its vlog bytes, call `DB::compact`, assert compaction returns corruption, then assert failure count is one and last error contains `checksum` or `corrupt`.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test maintenance compaction_failure_is_reported_in_stats -- --exact --nocapture`

Expected: compilation fails because the stats fields do not exist.

- [ ] **Step 3: Implement shared failure recording**

Add `AtomicU64` and `Mutex<Option<String>>` to `ColumnFamily`, initialize them in create/load, and expose them through `stats`. Wrap manual and background `compaction::run` calls so every returned error is recorded exactly once; background remains non-fatal and manual returns the same error.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test maintenance compaction_failure_is_reported_in_stats -- --exact --nocapture`

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/column_family.rs src/compaction.rs src/db.rs src/maintenance.rs tests/maintenance.rs
git commit -m "feat: report compaction failures in stats"
```

### Task 9: Collect unknown default-tier crash orphans on open

**Files:**
- Modify: `src/db.rs`
- Modify: `tests/engine_regressions.rs`

**Interfaces:**
- Changes: `sst_is_misplaced(None, None)` becomes true only for the default location; unknown named-tier IDs remain untouched.

- [ ] **Step 1: Write failing reopen cleanup test**

Create and close a DB with one known SST. Add `999999.klog` and `999999.vlog` to its CF directory, reopen, and assert the unknown pair is gone while the manifest-referenced table and its data remain.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test engine_regressions unknown_default_tier_sst_orphans_are_removed_on_open -- --exact`

Expected: unknown files still exist.

- [ ] **Step 3: Implement default-only unknown cleanup**

Change the predicate to:

```rust
match manifest_tier {
    Some(tier) => tier.as_deref() != location,
    None => location.is_none(),
}
```

Keep shared and named-tier sweep ownership unchanged.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test engine_regressions unknown_default_tier_sst_orphans_are_removed_on_open -- --exact`

Run: `cargo test db::tests::orphan_sweep_removes_only_known_tables_in_the_wrong_location`

Expected: both pass after updating the predicate unit expectations.

- [ ] **Step 5: Commit**

```bash
git add src/db.rs tests/engine_regressions.rs
git commit -m "fix: sweep default-tier crash orphans"
```

### Task 10: Honor `num_compaction_threads`

**Files:**
- Modify: `src/db.rs`
- Modify: `tests/sustained_writes.rs`

**Interfaces:**
- Changes: compaction receiver is cloned to `num_compaction_threads.max(1)` named worker threads.

- [ ] **Step 1: Write a failing concurrency test**

Configure two compaction workers and two CFs with trigger one. Install a compaction filter on each that sends its CF name to a channel and waits on a release channel. Flush both CFs and assert two distinct entry messages arrive before either filter is released. On a single worker only one message arrives within the timeout. Release both and close cleanly.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test sustained_writes configured_compaction_workers_run_disjoint_cfs_concurrently -- --exact --nocapture`

Expected: second entry message times out.

- [ ] **Step 3: Spawn configured consumers**

Replace the single compaction-worker block with a loop over `inner.opts.num_compaction_threads.max(1)`, cloning `compact_rx`, `inner`, and `stop` for every spawn.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test sustained_writes configured_compaction_workers_run_disjoint_cfs_concurrently -- --exact --nocapture`

Expected: pass without deadlock.

- [ ] **Step 5: Commit**

```bash
git add src/db.rs tests/sustained_writes.rs
git commit -m "fix: honor configured compaction workers"
```

### Task 11: Wait only for the requested CF's flush

**Files:**
- Modify: `src/column_family.rs`
- Modify: `src/db.rs`
- Modify: in-module tests in `src/db.rs`

**Interfaces:**
- Produces: `ColumnFamily::pending_flushes: AtomicUsize`; per-CF `FlushJob` completion decrements it.

- [ ] **Step 1: Write the failing wait-isolation test**

In a `db.rs` unit test, open two CFs, artificially increment the DB-global pending count to represent unrelated CF B work, then call `flush_memtable(A)` on a thread. Assert it completes before the artificial global count is released; always release the count afterward so the test terminates. The old global wait misses the deadline.

- [ ] **Step 2: Verify RED**

Run: `cargo test db::tests::flush_memtable_waits_only_for_target_cf -- --exact --nocapture`

Expected: deadline assertion fails.

- [ ] **Step 3: Add per-CF accounting**

Increment both global and `self.pending_flushes` immediately before sending `FlushJob::PerCf`; undo both on send failure. In `process_flush_job`, preserve the CF handle long enough to decrement its counter after `flush_per_cf` returns. `DB::flush_memtable` waits on the per-CF counter when `unified.is_none()` and the global counter otherwise. `close` remains global.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test db::tests::flush_memtable_waits_only_for_target_cf -- --exact --nocapture`

Run: `cargo test --test maintenance backup_consistent_during_compaction -- --exact`

Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add src/column_family.rs src/db.rs
git commit -m "fix: scope flush waits to their column family"
```

### Task 12: Reject impossible manifest levels before allocation

**Files:**
- Modify: `src/column_family.rs`
- Modify: `tests/engine_regressions.rs`

**Interfaces:**
- Produces: `MAX_MANIFEST_LEVEL: u32 = 64` validation in `ColumnFamily::load`.

- [ ] **Step 1: Write failing CRC-valid manifest test**

Create/flush/close a DB, load its `Manifest`, set one `SstMeta.level = 65`, save it through `Manifest::save` so the checksum is valid, and assert `DB::open` returns `OndaError::Corruption` mentioning level 65.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test engine_regressions manifest_level_above_limit_is_corruption -- --exact`

Expected: open succeeds with 66 allocated levels or fails for a reason other than level validation.

- [ ] **Step 3: Validate before `max_level` arithmetic**

Before scanning for the maximum, reject the first metadata item with `level > 64` using a corruption error naming the CF, table ID, level, and supported maximum.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test engine_regressions manifest_level_above_limit_is_corruption -- --exact`

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/column_family.rs tests/engine_regressions.rs
git commit -m "fix: bound manifest levels before allocation"
```

### Task 13: Make exact merge ties explicitly overlay-first

**Files:**
- Modify: `src/iterator.rs`

**Interfaces:**
- Changes: `MergingIter::before` is a strict total order using child index after key/sequence equality.

- [ ] **Step 1: Write failing tie-order unit test**

Build two one-entry memtables with identical key and sequence but values `overlay` and `committed`; place overlay first, construct an `Iterator`, and assert forward and backward positioning both return `overlay`. Add a direct `before(0, 1)` / `before(1, 0)` assertion so the test fails under the current accidental equality.

- [ ] **Step 2: Verify RED**

Run: `cargo test iterator::tests::exact_ties_prefer_earlier_child_in_both_directions -- --exact`

Expected: direct ordering assertion fails.

- [ ] **Step 3: Add explicit child priority**

After key and sequence compare equal, return `i < j` before applying direction reversal. Earlier child priority is direction-independent because `VisibleVersion` is first-wins for an exact tie.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test iterator::tests`

Expected: iterator unit tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/iterator.rs
git commit -m "fix: make transaction overlay tie priority explicit"
```

### Task 14: Key thread commit floors by stable DB identity

**Files:**
- Modify: `src/db.rs`

**Interfaces:**
- Produces: `DbInner::instance_id: u64` from static `NEXT_DB_INSTANCE_ID: AtomicU64`; thread-local map becomes `HashMap<u64, u64>`.

- [ ] **Step 1: Write a failing identity test**

Construct, close, and reopen database instances repeatedly in the same thread. Assert every internal `instance_id` is nonzero and unique. This first fails to compile because the identity does not exist.

- [ ] **Step 2: Verify RED**

Run: `cargo test db::tests::database_instance_ids_are_monotonic_and_unique -- --exact`

Expected: compilation fails for missing `instance_id`.

- [ ] **Step 3: Implement monotonic identities**

Add a process-global atomic initialized to one, mint with `fetch_add(1, Relaxed)` in `build_db_inner`, and replace `db_key()` pointer conversion with the field. Use `u64` thread-local keys.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test db::tests::database_instance_ids_are_monotonic_and_unique -- --exact`

Run: `cargo test --test read_your_writes`

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/db.rs
git commit -m "fix: give commit floors stable database identities"
```

### Task 15: Roll Serializable read tracking back with savepoints

**Files:**
- Modify: `src/txn.rs`
- Modify: `tests/db.rs`

**Interfaces:**
- Produces: `Txn::read_log: Vec<(usize, Vec<u8>)>` and savepoint tuple `(name, writes_len, buf_len, read_log_len)`.

- [ ] **Step 1: Write failing Serializable savepoint test**

Begin Serializable, set savepoint, read key `later`, roll back to savepoint, update `later` in another transaction, buffer an unrelated write in the original transaction, and assert its commit succeeds. The old retained read set returns `Conflict`.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test db serializable_savepoint_rollback_discards_later_reads -- --exact`

Expected: conflict error.

- [ ] **Step 3: Implement ordered read rollback**

When `read_set.insert` returns true, push the same `(cf_id, key)` to `read_log`. Record its length in each savepoint. On rollback, remove every truncated log entry from `read_set`, truncate the log, and retain `read_cfs` only for IDs still present in `read_set`. Clear the log on reset/rollback/commit cleanup.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test db serializable_savepoint_rollback_discards_later_reads -- --exact`

Run: `cargo test --test db savepoint_rollback -- --exact`

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/txn.rs tests/db.rs
git commit -m "fix: roll back serializable reads at savepoints"
```

### Task 16: Make reset honor the thread's commit floor

**Files:**
- Modify: `src/txn.rs`
- Modify: in-module tests in `src/txn.rs`

**Interfaces:**
- Changes: fixed-level `Txn::reset` calls `wait_visible_at_own_floor` before reading `visible_seq`.

- [ ] **Step 1: Write a failing forced-gap test**

Create a ReadCommitted transaction, reserve a sequence on its DB and record it as this thread's commit floor without publishing. Spawn a helper that publishes the reserved range after synchronization. Reset the transaction to Snapshot and assert its private `read_seq` is at least the reserved sequence. The old reset pins the stale visible watermark.

- [ ] **Step 2: Verify RED**

Run: `cargo test txn::tests::reset_fixed_snapshot_waits_for_own_commit_floor -- --exact`

Expected: stale `read_seq` assertion fails.

- [ ] **Step 3: Mirror begin's fixed-snapshot sequence selection**

For fixed levels call `self.db.wait_visible_at_own_floor()` before `let read_seq = self.db.visible_seq()`; non-fixed levels use `read_floor_seq` so reset matches begin semantics completely.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test txn::tests::reset_fixed_snapshot_waits_for_own_commit_floor -- --exact`

Run: `cargo test --test snapshot_self_conflict`

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/txn.rs
git commit -m "fix: preserve own commit floor on transaction reset"
```

### Task 17: Resolve documentation drift and record every finding

**Files:**
- Modify: `AGENTS.md`
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/concurrency-and-safety.md`
- Modify: `docs/formats.md`
- Modify: `docs/parts-and-tiers.md`
- Modify: public field comments in `src/config.rs`
- Modify: reader comments in `src/sst/reader.rs`
- Modify: `tests/maintenance.rs`
- Create: `docs/code-review-2026-08-resolution.md`

**Interfaces:**
- Produces: one review-resolution table mapping F1-F5, M1-M10, L1-L6, dead surface, documentation drift, and known limitations to test/commit/defer rationale.

- [ ] **Step 1: Run the documentation fact checks**

Run targeted `rg` queries for `256 shards`, `forbid(unsafe_code)`, part operations under `compact_mu`, `16 KiB`, cross-CF transaction wording, and reader bloom filtering. Capture every stale location in the resolution document.

For L6, add an ignored `fifo_ttl_selection_probe` to `tests/maintenance.rs`.
It creates 100 FIFO L0 tables with a nonzero TTL, times `db.compact(&cf)`,
and prints elapsed microseconds and victim count. Run this exact command five
times in release mode and record all five observations:

```bash
cargo test --release --test maintenance fifo_ttl_selection_probe -- --ignored --exact --nocapture
```

This establishes whether metadata calls under the state lock are material at
the reviewed scale. Because 0.8.2 makes no L6 code change, do not manufacture a
post-change comparison; record the baseline and the decision to defer.

- [ ] **Step 2: Correct contracts**

Document 16 shards; `deny` with the audited Linux exception; range-lock ownership; `data_block_size`; unified-only atomic multi-CF commits; bytewise-only lazy unified iteration; compaction failure stats; and default-tier orphan cleanup. Mark remaining no-op public options as reserved/currently ignored without removing fields.

- [ ] **Step 3: Record no-change findings**

For M3, M5, M10, L5, L6, dead API/format surface, missing features, and known limitations, copy the exact rationale and evidence from the approved spec. For L6, record five baseline measurements and state whether no code change was justified.

- [ ] **Step 4: Verify docs and diff hygiene**

Run: `git diff --check`

Run the same `rg` fact checks and confirm stale claims are absent except where quoted historically in the review.

- [ ] **Step 5: Commit**

```bash
git add AGENTS.md README.md docs/architecture.md docs/concurrency-and-safety.md docs/formats.md docs/parts-and-tiers.md docs/code-review-2026-08.md docs/code-review-2026-08-resolution.md src/config.rs src/sst/reader.rs tests/maintenance.rs
git commit -m "docs: resolve August 2026 code review"
```

### Task 18: Full verification, release metadata, and local tag

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Produces: package version `0.8.2`, release notes, release commit, annotated local tag `v0.8.2`.

- [ ] **Step 1: Run focused tests and inspect the complete diff**

Run all focused commands named above once more. Run `git diff --check`, `git status --short`, and inspect `git diff v0.8.1..HEAD` for unrelated changes, bare SST deletion, manifest writes outside `persist_manifest`, and ungated comparator prefix shortcuts.

- [ ] **Step 2: Run the complete dual-configuration gate**

Run each command independently and preserve full output:

```bash
cargo build
cargo build --features unsafe-fastpath
cargo test
cargo test --features unsafe-fastpath
cargo clippy --all-targets
cargo clippy --all-targets --features unsafe-fastpath
cargo check --features s3
```

For both `cargo test` outputs, verify every test binary contains `test result: ok`; do not pipe through `tail`.

- [ ] **Step 3: Update release metadata**

Set `Cargo.toml` package version to `0.8.2`, update the root ondadb package version in `Cargo.lock`, and prepend a `## 0.8.2` changelog entry covering the five high findings, bounded robustness fixes, low-severity correctness fixes, `v0.8.1` ancestry, compatibility restriction, and explicit deferrals.

- [ ] **Step 4: Verify the exact release tree again**

Run all seven gate commands again after metadata changes. Confirm `cargo metadata --no-deps` reports `0.8.2` and `git diff --check` is clean.

- [ ] **Step 5: Commit release metadata**

```bash
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "release: ondadb 0.8.2 corrective release"
```

- [ ] **Step 6: Create and verify the local annotated tag**

```bash
git tag -a v0.8.2 -m "v0.8.2 — August 2026 code-review corrective release"
git show --no-patch --decorate v0.8.2
git status --short --branch
```

Expected: tag points at the release commit; only intentionally uncommitted user files remain; nothing has been pushed.
