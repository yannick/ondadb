# Aggressive Code-Quality Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a trustworthy pre-sweep baseline, then reduce every actionable production complexity exception and every function over 200 lines while preserving ondaDB's behavior, durability, concurrency, formats, and performance.

**Architecture:** Refactor in four risk-ordered waves. Each change first characterizes the existing decision or failure path, then extracts a cohesive private phase, state, or policy abstraction. Deterministic baselines only shrink after the anchor commit. Safe and `unsafe-fastpath` configurations are verified after every coherent unit, and hot-path changes are retained only after five paired benchmark runs satisfy the approved gate.

**Tech Stack:** Rust 2021, Python standard-library metric tooling, Just, BCA 2.1.0, cargo-geiger 0.13.0, cargo-llvm-cov 0.8.7, cargo-bloat 0.12.1.

## Global Constraints

- Preserve every critical invariant in `AGENTS.md` and the approved design.
- Do not change public APIs, persisted bytes, comparator semantics, MVCC visibility, lock ordering, manifest ordering, or intended behavior.
- Do not add a production dependency.
- Do not introduce allocation, cloning, locking, or materialization on read, iterator, write, or compaction hot paths without measured justification.
- A helper must own a coherent decision or phase; do not split branches solely to manipulate metrics.
- Never regenerate a baseline to accept a regression. Before publishing a smaller baseline, independently prove that every remaining exception is one of the three reviewed `OndaError` mappings.
- Run both feature configurations for every focused and full gate.
- Capture Cargo test output without `tail`; confirm every emitted test binary contains `test result: ok`.
- Use `apply_patch` for source and documentation edits.

## Task 1: Create the Isolated Sweep and Anchor the Before Baseline

**Files:**

- Modify: `.bca-baseline.toml`
- Modify: `metrics/baseline.json`
- Create: `metrics/history/<timestamp>-before-quality-sweep.json`
- Generated only: `target/quality-sweep/before/**`

- [ ] **Step 1: Create the isolated worktree**

From the primary checkout, create branch `quality/aggressive-sweep` at the approved plan commit and worktree `.worktrees/aggressive-quality-sweep`. Verify the worktree is ignored and clean before edits.

- [ ] **Step 2: Verify the pinned toolchain**

Run:

```sh
just tools-check
just --fmt --check
cargo fmt --check
```

Expected: exact pinned versions, a valid Justfile, and no formatting diff.

- [ ] **Step 3: Run the pre-refactor correctness gate**

Run and retain full logs under `target/quality-sweep/before/`:

```sh
cargo test
cargo test --features unsafe-fastpath
cargo clippy --all-targets
cargo clippy --all-targets --features unsafe-fastpath
```

Expected: every test binary reports `test result: ok`; both Clippy runs exit zero.

- [ ] **Step 4: Refresh deterministic baselines on the clean revision**

Run:

```sh
just metrics-baseline
just metrics-check
```

Inspect the documents and assert exactly 26 BCA entries, three long functions, 63 unsafe expressions, and three unsafe impls. Confirm the generated baseline records a clean Git state and current commit.

- [ ] **Step 5: Capture coverage and before history**

Run:

```sh
just coverage
just metrics-record before-quality-sweep
```

Copy the raw metrics and coverage artifacts into `target/quality-sweep/before/`. Record line, function, and region coverage for the final 0.5 percentage-point gate.

- [ ] **Step 6: Capture standalone before benchmarks**

Run the full five-run workload:

```sh
just bench-onda 5 1000000 8 16 100 random none 1000 unsafe-fastpath
```

Retain the raw JSON/CSV report in `target/quality-sweep/before/`. Treat it as context only; the final performance decision uses alternating paired checkouts.

- [ ] **Step 7: Commit the anchor**

Review the baseline and history diff, then commit only deterministic baselines and the explicit history snapshot:

```sh
git commit -m "chore: anchor quality sweep baseline"
```

## Task 2: Refactor Configuration Decoding and Database Open

**Files:**

- Modify: `src/config.rs`
- Modify: `src/db.rs`
- Test: in-module tests in `src/config.rs` and `src/db.rs`

- [ ] **Step 1: Add configuration codec characterization tests**

Add tests that assert byte-for-byte encoding stability for the default and a fully populated `ColumnFamilyConfig`, decode every legacy prefix length, reject/truncate malformed optional tails without panicking, and preserve unknown/registered derived-partitioner behavior. Run the named tests in both configurations and observe the new seam-oriented tests fail before helpers exist.

- [ ] **Step 2: Introduce a checked configuration decoder**

Add private `ConfigDecoder<'a> { remaining: &'a [u8] }` methods for bytes, fixed-width integers, varints, byte strings, and UTF-8 strings. Move legacy fields and optional tail sections into named phase methods. Keep `ColumnFamilyConfig::decode`'s forgiving compatibility behavior and every encoded byte unchanged.

- [ ] **Step 3: Extract coherent configuration encoder sections**

Extract private base, legacy-collection, and optional-tail encoders. Return any counts needed by later sections explicitly rather than recomputing policy. Verify encode/decode tests in both configurations.

- [ ] **Step 4: Characterize database open phases**

Add focused tests for WAL-layout mismatch, unknown comparator, missing derived partitioner registration, read-only open behavior, shared-tier nonce initialization, and reopen recovery. Use existing failure hooks where available; do not add a public hook.

- [ ] **Step 5: Extract private open phases**

Extract cohesive helpers for tier-registry construction, manifest/layout validation, `DbInner` construction, column-family recovery, and writable-open finalization. Keep the top-level `open_impl` as the visible ordered orchestration. The helper that finalizes a writable open must preserve nonce persistence, orphan sweep, and worker start ordering.

- [ ] **Step 6: Verify and commit**

Run focused tests in both feature configurations, then:

```sh
just metrics-check
cargo fmt --check
cargo clippy --all-targets
cargo clippy --all-targets --features unsafe-fastpath
```

Assert `decode_into` and `DB::open_impl` are no longer over thresholds and no new BCA offender appears. Commit:

```sh
git commit -m "refactor: clarify configuration and open phases"
```

## Task 3: Refactor Manifest Codecs, Orphan Sweep, and Error Policy

**Files:**

- Modify: `src/manifest.rs`
- Modify: `src/db.rs`
- Modify only to document policy if needed: `src/error.rs`
- Test: in-module tests in those files and relevant integration corruption tests

- [ ] **Step 1: Add manifest compatibility and corruption tests**

Assert byte identity for representative legacy/current manifests, decode success at every compatible optional-tail boundary, rejection for every truncation inside a fixed or variable field, checksum failure behavior, WAL-layout preservation, tier metadata preservation, and instance-nonce preservation.

- [ ] **Step 2: Introduce a checked manifest decoder and named encoder sections**

Use a private cursor type for checked reads and private section methods for header, column families, SST metadata, and optional tails. Split encoding into equally explicit sections. Keep CRC placement and every serialized byte unchanged.

- [ ] **Step 3: Characterize and simplify move-orphan decisions**

Add a pure decision test matrix covering manifest-resident source/target, missing files, local versus shared tiers, per-CF directories, and pre/post-manifest-flip leftovers. Extract manifest tier-location indexing and per-location sweep helpers. Continue treating the manifest as the only source of truth and never delete shared-tier objects.

- [ ] **Step 4: Review the exhaustive error mappings**

Prove `OndaError::{code,fmt,kind}` remain flat exhaustive matches at cyclomatic 16 and cognitive 1. Add or retain an exhaustiveness test where practical, and document why they are the only acceptable exceptions. Do not split them.

- [ ] **Step 5: Verify wave 1 and shrink its debt**

Run focused tests in both configurations, full safe and fast suites, both Clippy configurations, and `just metrics-check`. Generate a candidate BCA baseline in scratch, assert it contains no wave-1 entry except the three error mappings, then update the committed baseline by removing resolved entries only. Commit:

```sh
git commit -m "refactor: make persisted format phases explicit"
```

## Task 4: Refactor Column-Family Point Reads and Iterator Construction

**Files:**

- Modify: `src/column_family.rs`
- Test: in-module tests and `tests/db.rs`, `tests/unified.rs`

- [ ] **Step 1: Characterize point-read source precedence**

Add a decision matrix across active/immutable memtables, L0 newest-first, sorted levels, tombstones, TTL expiry, snapshot sequence, unified versus per-CF layouts, and non-bytewise comparators. Include equal eight-byte-prefix keys to prove full-comparator fallback.

- [ ] **Step 2: Extract a stack-only point-candidate policy**

Introduce a private candidate/result type that records the best visible sequence and terminal value/tombstone without allocating. Extract source traversal helpers while preserving lookup order, early exits, cache behavior, and exact comparator gating.

- [ ] **Step 3: Characterize iterator child construction**

Add tests for active/immutable/unified sources, overlapping L0, non-overlapping sorted levels, bounds, forward/backward direction, snapshots, and comparator equality.

- [ ] **Step 4: Extract iterator source builders**

Create named helpers for memtable children, L0 children, and sorted-level children. Preserve child ordering, pin ownership, and the absence of per-entry `Arc` clones.

- [ ] **Step 5: Verify, benchmark, and commit**

Run focused tests in both configurations, `just metrics-check`, both Clippy runs, then five Get/Forward/Backward runs against the unchanged base checkout. If the gate fails, revert or redesign. Remove only resolved baseline entries and commit:

```sh
git commit -m "refactor: clarify column-family read planning"
```

## Task 5: Refactor Merge Iteration, Table Cache, and SST Reads

**Files:**

- Modify: `src/iterator.rs`
- Modify: `src/table_cache.rs`
- Modify: `src/sst/reader.rs`
- Test: in-module tests, `tests/sst.rs`, and iterator/read integration tests

- [ ] **Step 1: Characterize forward/backward merge groups**

Add tests for duplicate internal keys, tombstones, expired values, bounds, direction switches, block transitions, and a case where the winning key and value come from different child blocks. Assert separate key/value pins remain valid.

- [ ] **Step 2: Extract group-resolution operations**

Move the coherent forward and backward group decisions into private operations with explicit outputs. Preserve pin refresh only on block transitions and avoid per-entry cloning or materialization.

- [ ] **Step 3: Characterize cache eviction**

Cover byte/count budgets, pinned readers, recently used entries, shard wraparound, and inability to reach the target because entries are in use. Extract a per-shard eviction attempt with explicit progress reporting.

- [ ] **Step 4: Characterize SST lookup and vlog validation**

Cover restart selection, internal-key visibility, tombstone/TTL decisions, block checksum first-read behavior, failed-frame non-verification, reopen re-verification, mmap and positioned-read paths, short reads, CRC mismatch, and bounds overflow.

- [ ] **Step 5: Extract SST decision and I/O phases**

Separate restart scan selection, visible-entry interpretation, mmap vlog validation, and file vlog validation into private helpers sharing the same checksum policy. Do not add a cache lookup, allocation, syscall, or mapping clone to the hot path.

- [ ] **Step 6: Verify, benchmark, and commit**

Run focused and full tests in both configurations, both Clippy runs, `just metrics-check`, and five paired Get/Forward/Backward runs. Shrink resolved baseline entries only. Commit:

```sh
git commit -m "refactor: isolate read-path decisions"
```

## Task 6: Refactor WAL Sync, Flush, and Part Movement

**Files:**

- Modify: `src/wal.rs`
- Modify: `src/db.rs`
- Modify: `src/parts.rs`
- Test: in-module tests and `tests/maintenance.rs`, `tests/unified.rs`

- [ ] **Step 1: Characterize interval-sync state transitions**

Cover idle intervals, dirty generations, concurrent appends, sync success/failure, stop notification, poison propagation, and final flush. Extract the snapshot/sync/report phase without changing lock scope or sync timing.

- [ ] **Step 2: Characterize flush-job paths and failure ordering**

Cover per-CF and unified jobs, closing/stop, poison, pending counters, compaction scheduling, WAL cleanup, and injected failures at SST finish and manifest persistence. Explicitly assert SST sync plus directory fsync precede manifest persistence and WAL removal happens only after success.

- [ ] **Step 3: Extract flush job phases**

Keep `flush_worker` as orchestration over private per-CF/unified job handlers and a compaction-scheduling operation. Preserve active-writer drain and sealed-memtable immutability.

- [ ] **Step 4: Characterize part-move eligibility and cleanup**

Cover age/size/tier eligibility, shared-tier rules, successful manifest flip, pre-commit copy failure cleanup, post-commit source deletion failure, and concurrent mover exclusion. Extract a pure target decision and explicit pre-commit/copy/commit/post-commit phases.

- [ ] **Step 5: Verify, benchmark, and commit**

Run focused and full suites in both configurations, both Clippy runs, `just metrics-check`, and five paired Put/Delete runs. Shrink resolved baseline entries only. Commit:

```sh
git commit -m "refactor: expose maintenance phase boundaries"
```

## Task 7: Refactor Transaction Commit

**Files:**

- Modify: `src/txn.rs`
- Test: in-module tests and transaction integration tests

- [ ] **Step 1: Characterize commit decisions and cleanup**

Cover empty commits, last-write-wins deduplication, write/write conflicts, serializable read conflicts, cross-CF batches, unified/per-CF WAL selection, reserve failure, WAL failure, apply failure, poison, and successful visibility. Assert every reserved sequence range is published gap-free even when apply fails, while failed commits never surface a partial batch.

- [ ] **Step 2: Introduce a private prepared-commit representation**

Extract write-order deduplication, conflict validation, read-set validation, and per-store grouping into private operations. Use borrowed records where possible and preserve order and one-frame batch atomicity.

- [ ] **Step 3: Extract the commit application phase**

Make reservation, WAL append, active-writer lifetime, memtable apply, publication, and cleanup visually explicit. Prefer an internal guard only if it makes unconditional publication/cleanup provable and does not change lock duration.

- [ ] **Step 4: Verify, benchmark, and commit**

Run focused and full suites in both configurations, both Clippy runs, `just metrics-check`, and five paired Put/Delete runs. Inspect lock acquisition order in the diff. Shrink resolved baseline entries only. Commit:

```sh
git commit -m "refactor: make transaction commit phases explicit"
```

## Task 8: Refactor Compaction Last

**Files:**

- Modify: `src/compaction.rs`
- Test: in-module tests and `tests/maintenance.rs`, `tests/db.rs`

- [ ] **Step 1: Characterize version-retention policy**

Add a table-driven pure-policy test for newest/older versions, oldest snapshots, tombstones, TTL expiry, bottom versus non-bottom levels, merge/filter outcomes, and equal user keys under bytewise and custom comparators.

- [ ] **Step 2: Characterize output boundaries and failure ordering**

Cover target file size, partition boundaries, tier selection, empty output, writer finish failure, manifest persistence failure, checkpoint-pinned inputs, and successful obsolete-input removal. Assert incomplete outputs are removed before commit and inputs are never removed before durable manifest publication.

- [ ] **Step 3: Introduce cohesive compaction state**

Create private state for version retention and output construction. The retention operation decides emit/drop/transform; the output builder owns current writer, boundary state, completed metadata, and pre-commit cleanup. Keep merge selection and comparator decisions allocation-neutral.

- [ ] **Step 4: Make commit ordering explicit**

Keep `compact_inputs` as a short ordered orchestration: build iterators, merge/retain, finish and sync outputs, persist the manifest through `DbInner::persist_manifest`, then remove inputs only through `DbInner::remove_sst_file`. Ensure every early return has the correct cleanup behavior.

- [ ] **Step 5: Verify, benchmark, and commit**

Run all compaction, snapshot, backup, corruption, partition, and tier tests in both configurations; then full suites, both Clippy runs, `just metrics-check`, and all five paired benchmark phases. Reject or redesign any change that crosses the performance gate. Shrink resolved baseline entries only. Commit:

```sh
git commit -m "refactor: make compaction policy and phases explicit"
```

## Task 9: Prove Final Targets and Publish the After Report

**Files:**

- Modify: `.bca-baseline.toml`
- Modify: `metrics/baseline.json`
- Create: `metrics/history/<timestamp>-after-quality-sweep.json`
- Create: `metrics/quality-sweep-2026-08-15.md`
- Generated only: `target/quality-sweep/after/**`, `target/quality-sweep/paired/**`

- [ ] **Step 1: Prove the raw complexity target before baseline publication**

Generate an unsuppressed BCA report into scratch. Assert:

- zero cognitive values above 20;
- zero cyclomatic values above 15 except exactly `OndaError::{code,fmt,kind}` at 16/cognitive 1;
- zero function spans above 200;
- no new production path or metric violation.

Only after that proof, generate a candidate baseline and replace the committed BCA baseline if it contains exactly those three entries. Refresh the long-function/unsafe baseline only after proving unsafe counts did not rise.

- [ ] **Step 2: Run final coverage and dependency/size comparison**

Run:

```sh
just coverage
just metrics
just metrics-record after-quality-sweep
```

Compare against the before snapshot. Fail if line, function, or region coverage falls by more than 0.5 percentage points, direct dependencies increase, duplicates are unexplained, or unsafe surface rises.

- [ ] **Step 3: Run the alternating paired performance gate**

Build the pre-refactor primary checkout and the sweep worktree into separate target directories. For each of five pairs, run Put, Get, Forward, Backward, and Delete once in alternating A/B then B/A order with identical settings and retained raw reports. Compute per-pair throughput ratios and medians. A phase fails if after loses at least four pairs and median throughput is more than 10 percent lower. Rerun thermally ambiguous results before deciding.

- [ ] **Step 4: Run the complete CI-equivalent gate with auditable logs**

Run:

```sh
cargo fmt --check
cargo test
cargo test --features unsafe-fastpath
cargo clippy --all-targets
cargo clippy --all-targets --features unsafe-fastpath
just metrics-check
git diff --check
```

Count `test result: ok` lines and inspect each test binary. Do not claim success from process exit alone.

- [ ] **Step 5: Write and commit the final report**

The report must include before/after complexity maxima and distributions, exception and long-function counts, unsafe categories, coverage percentages, production/test lines, direct/transitive/duplicate dependencies, binary sizes, all paired benchmark results, rejected or reverted experiments, and the exact final verification evidence. Commit:

```sh
git commit -m "docs: publish quality sweep results"
```

## Task 10: Review, Merge to Main, and Remove the Worktree

- [ ] **Step 1: Review the full branch diff and history**

Compare the branch with the approved plan base. Audit every critical invariant, hot-path ownership/lifetime change, baseline deletion, generated artifact, and production dependency change. Resolve all Critical and Important findings, then rerun affected and final gates.

- [ ] **Step 2: Merge locally to main**

Verify the primary checkout is clean and still at the approved base plus plan. Merge `quality/aggressive-sweep` with a non-interactive merge commit or fast-forward as appropriate.

- [ ] **Step 3: Verify the merged checkout**

From `main`, rerun formatting, both full test suites, both Clippy configurations, `just metrics-check`, and `git diff --check`. Inspect every emitted test summary again.

- [ ] **Step 4: Remove the worktree and branch**

Remove only the resolved `.worktrees/aggressive-quality-sweep` worktree after confirming it is clean and merged. Delete the merged feature branch, prune worktree metadata, and report the final main commit, tests, metrics delta, coverage delta, and paired benchmark result.
