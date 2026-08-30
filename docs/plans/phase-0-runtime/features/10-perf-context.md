# 0.10 — Per-operation PerfContext

**Readiness:** ready; **land first**. Every later feature's acceptance section
quotes PerfContext deltas. **Effort:** 1–2 dev-weeks. **wavesdb counterpart:**
0.10.

## Goal

Caller-owned counters for the lifetime of one `get` / `multi_get` / iterator
walk, without contending on DB-wide atomics, so a performance claim can be
attributed to a mechanism rather than inferred from wall time.

The RV-M2 compaction-failure work that earlier drafts bundled here **is already
in the tree at 0.8.2** and is not part of this feature:
`compact_worker` does `if let Err(error) = compaction::run(&db, &cf) {
cf.record_compaction_failure(&error); }`, `DB::compact` does the same, and
`CfStats` carries `compaction_failures: u64` and
`last_compaction_error: Option<String>`. Nothing to add.

The multi-level benchmark fixture generator that earlier drafts asked this
feature to commit belongs to **0.2**, which is where it is described and built.

## Baseline (verified at 0.8.2)

- Aggregate-only counters today: `ColumnFamily::{point_reads, bloom_skips,
  sst_probes}` (all `fetch_add(1, Relaxed)` inside `get` /
  `consider_sstables`), `BlockCache::stats`, `TableCache::stats`,
  `DbInner::wal_syncs`.
- The read chain is shallow: `ColumnFamily::get` → `point_read_sources` →
  memtable `get`s → `consider_sstables` → `Reader::get_unfiltered` →
  `find_block` / `read_data_block_local` / `split_block` /
  `restart_scan_offset` / `scan_point_entry`, plus `read_vlog_into` →
  `read_vlog_from_mmap` / `read_vlog_from_file`.
- Precedent for thread-scoped state: `THREAD_COMMIT_FLOOR` (`db.rs`) and
  `wal::my_stripe` — a thread-local scope guard is idiomatic here.
- **`ondadb::Iterator` is `Send`.** Compile-verified against this tree
  (`fn assert_send<T: Send>() {}` / `assert_send::<ondadb::Iterator>()`
  builds). This is structural: every field is a `Vec`, `Arc`, or primitive —
  `MergingIter`, `Vec<Option<Block>>` where `Block` is
  `Owned(Arc<[u8]>)` / `Mapped { mmap: Arc<Mmap>, .. }`, `SstIterator { r:
  Arc<Reader>, .. }`, `LazyMemIter { cell }`. Any claim that it is not `Send`
  is false, and adding a `PhantomData<*const ()>` to make it so would be a
  breaking change for users who move iterators between threads today.

## Design

The default build denies `unsafe`, so the context cannot be a
`&'static mut` or a raw pointer parked in a thread-local. Own it instead:

```rust
// new module perf.rs
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PerfContext {                 // all u64
    pub bloom_probes: u64, pub bloom_negatives: u64,
    pub memtable_probes: u64, pub sstable_probes: u64, pub index_seeks: u64,
    pub block_cache_hits: u64, pub block_misses: u64,
    pub block_read_bytes: u64, pub bytes_decompressed: u64,
    pub vlog_reads: u64, pub vlog_read_bytes: u64,
    pub iterator_seeks: u64, pub iterator_steps: u64,
    // added by their features: vlog_cache_hits (0.5),
    // multiget_blocks_deduped (0.4)
}

thread_local! {
    static STACK: RefCell<Vec<PerfContext>> = const { RefCell::new(Vec::new()) };
}

pub struct Scope(());                       // new
pub fn enter() -> Scope;                    // pushes a zeroed context
impl Scope { pub fn finish(self) -> PerfContext; }  // pops and returns it
impl Drop for Scope { /* pops and discards if finish() was not called */ }

#[inline] pub(crate) fn bump(f: impl FnOnce(&mut PerfContext));  // empty stack => no-op
```

- Nesting is a stack, and an inner scope's counters do **not** roll up into the
  outer one — `bump` writes to the innermost frame only. That is the simplest
  rule to reason about and the one the nesting test pins.
- Public surface: `DB::get_with_perf(cf, key) -> (Result<Vec<u8>>,
  PerfContext)`, `Txn::get_with_perf`, `DB::multi_get_with_perf` (once 0.4
  lands), and `Iterator::perf_scope(&mut self) -> Scope` for a walk. Each
  opens a `perf::enter` scope and hands back the finished context; deep layers
  call `perf::bump` and no signature changes.
- **Nil path** is one thread-local borrow and an `is_empty()` check. Measure it
  (acceptance below); if the `RefCell` borrow shows up, back the stack with a
  `Cell<usize>` depth counter checked first.
- **Counters are thread-affine and best-effort.** They accumulate on the thread
  that does the work, into that thread's innermost scope. An `Iterator` moved
  to another thread keeps working — it is `Send` — but its counters then
  accumulate into whatever scope (if any) is open on the new thread. Document
  that plainly on `PerfContext`; do **not** try to prevent it.
- `multi_get` (0.4) accumulates into one context on the calling thread, since
  v1 is sequential. If parallelism ever lands, workers use local contexts
  merged after join.

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

1. **`perf.rs`.** The struct, the thread-local stack, `enter`, `Scope`,
   `finish`, `Drop`, `bump`.
   Test first: `perf.rs::bump_outside_a_scope_is_a_noop` — `bump` with an
   empty stack does not panic and records nothing;
   `perf.rs::finish_returns_only_this_scopes_counters` — bump, finish, assert
   the values; `perf.rs::nested_scopes_do_not_leak` — outer scope, inner scope
   with bumps, inner `finish`, more outer bumps; assert the outer context holds
   only its own bumps; `perf.rs::dropped_scope_pops_the_stack` — a `Scope`
   dropped without `finish` leaves the stack at its prior depth, including when
   dropped during a panic (`catch_unwind`).
2. **Read-chain bumps, memtable side.** `column_family.rs::get` and the
   memtable lookups: `memtable_probes`.
   Test first: `tests/db.rs::perf_memtable_only_get` — a key resolved from the
   active memtable reports `memtable_probes == 1` (plus the unified probe when
   that layout is on) and every bloom/SST counter at 0.
3. **Read-chain bumps, SST side.** `consider_sstables`: `bloom_probes`,
   `bloom_negatives`, `sstable_probes`. `Reader`: `index_seeks` in
   `find_block`; `block_cache_hits` / `block_misses` / `block_read_bytes` in
   `read_data_block`; `bytes_decompressed` at the decompress calls.
   Test first: `tests/db.rs::perf_get_missing_through_n_sstables` — build a CF
   with N tables all covering the key range; a missing key reports
   `bloom_probes == N`, `bloom_negatives == N - (bloom false positives)`, and
   block counters only for the survivors. Assert the invariant
   `sstable_probes == bloom_probes - bloom_negatives`, which is robust to the
   filter's false-positive rate.
   Plus `tests/db.rs::perf_block_cache_hit_on_second_get` — cold then warm;
   `block_misses` then `block_cache_hits`. Runs in **both** feature configs;
   under `mmap-reads` an uncompressed block is served from the mmap, so that
   config asserts `block_read_bytes` without cache traffic — spell the
   difference out in the test rather than skipping it.
4. **Vlog bumps.** `read_vlog_into` / `read_vlog_from_file` /
   `read_vlog_from_mmap`: `vlog_reads`, `vlog_read_bytes`.
   Test first: `tests/db.rs::perf_counts_vlog_reads_for_separated_values` — a
   value above `klog_value_threshold` reports one vlog read of the logical
   length; a value below it reports none.
5. **Iterator bumps.** `iterator.rs`: `iterator_seeks` on `seek*`,
   `iterator_steps` on each yielded group.
   Test first: `tests/db.rs::perf_iterator_walk_counts_steps` — a full walk of
   a known table reports `iterator_steps` equal to the live entry count and
   `iterator_seeks == 1`.
6. **Public entry points.** `DB::get_with_perf`, `Txn::get_with_perf`,
   `Iterator::perf_scope`.
   Test first: `tests/db.rs::get_with_perf_matches_plain_get` — same value,
   same error, for hits, misses, tombstones, and expired TTL entries.
7. **Thread-affinity documentation + test.** Test first:
   `tests/db.rs::iterator_counters_are_thread_affine` — build an iterator under
   a scope on thread A, move it to thread B (this compiles, because `Iterator`
   is `Send`), walk it there with no scope open, and assert thread A's finished
   context did not grow. The test's purpose is to pin the documented
   best-effort semantics, not to forbid the move.
8. **Nil-path microbenchmark.** `../bench`: hot point `get` with and without an
   open scope, ≥5 runs each.
   Test first: none (measurement task). Gate still runs.
9. **Docs.** `docs/performance.md` gains a "measuring a change" section
   describing the evidence format every later feature's acceptance section
   refers to (raw runs under `bench-results/<feature>/<date>/`, same-run
   ratios, ≥5 runs, thermal caveat).

## Tests (summary)

- Scope mechanics: no-op outside, nesting isolation, drop-pops (including on
  panic).
- Memtable-only `get`; missing `get` through N SSTables; warm cache hit.
- Vlog reads counted for separated values only.
- Iterator seeks/steps match the entry count.
- `*_with_perf` results identical to the plain entry points.
- Thread affinity pinned as best-effort, with `Iterator: Send` preserved.
- Both feature configs.

## Acceptance

Nil-path microbenchmark (hot point `get`, scope open versus absent): p50 delta
within noise (±2% — the `coarse_now_nanos` precedent is the bar), over ≥5 runs.
Raw runs under `bench-results/0.10/<date>/`.

## Rollback

Delete `perf.rs` and the public entry points; the internal `perf::bump` calls
are one-line removals. No disk state, no format change.
