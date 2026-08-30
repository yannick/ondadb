# 0.4 — MultiGet batched point lookups

**Readiness:** design settled — **v1 is sequential**; parallel IO is a later
spike. **Effort:** 2–4 dev-weeks. **wavesdb counterpart:** 0.4, with their
corrected premise adopted: bloom membership and index position are per key; the
win is **deduplicating block fetches** (one read + decode per distinct block),
not coalescing bloom/index work. RocksDB's 2.5× on high-latency flash is
motivation — and ondaDB's S3 tier is exactly high-latency-flash-shaped, since
every cold block there is one range GET.

## Public API

```rust
impl DB {
    pub fn multi_get(&self, cf: &Arc<ColumnFamily>, keys: &[&[u8]])
        -> Vec<Result<Vec<u8>>>;
}
impl Txn {
    pub fn multi_get(&mut self, cf: &Arc<ColumnFamily>, keys: &[&[u8]])
        -> Vec<Result<Vec<u8>>>;
}
```

`values.len() == keys.len()`; duplicate keys keep their positions and share
internal work. Empty input → empty output. The `Txn` variant resolves its own
buffered writes first (last-write-wins scan of `writes`, exactly as
`Txn::get`) and records Serializable read-set entries identically to N `get`s.

## Baseline (verified at 0.8.2)

- `ColumnFamily::point_read_sources(user_key)` takes `self.state.read()` and
  clones, **per call**: `s.mem`, `s.imm`, and the covering handles — L0 tables
  passing `key_in_range`, plus at most one table per level ≥ 1 via
  `find_overlapping` (which returns `Option<usize>`). It returns
  `PointReadSources { mem, imms, tables }` — **three fields**. The unified
  store is *not* in it: `ColumnFamily::get` consults `self.ctx.unified`
  separately, outside the state lock.
- `ColumnFamily::get` resolves through `PointReadCandidate` with **two** entry
  points, in this order: `consider_memtable(lookup)` for the unified store, the
  active memtable, and each immutable (newest-first, `imms.iter().rev()`); then
  `consider_sstables` → `PointReadCandidate::consider(value, seq, found,
  deleted)` per table. `consider` keeps the entry with the highest `seq` among
  `found` results, so relative order is a tie-break only when seqs are
  equal — but the batch path must preserve it anyway, because that is the
  contract the oracle test asserts.
- Per-table resolution is `Reader::get_unfiltered(user_key, read_seq, now)`,
  strictly one key at a time:
  `find_block` → `read_data_block_local` → `split_block` →
  `restart_scan_offset` → `scan_point_entry`.
  `find_block` and `split_block` are `pub(crate)`; `restart_scan_offset` and
  `scan_point_entry` are private, and `PointResult` is a private type alias.
- `bloom_hash(user_key) -> Option<u64>` and `bloom_may_contain_hash(h)` are
  `pub(crate)`; `consider_sstables` already does one hash + one check per
  table.
- `DbInner::read_floor_seq()` is the read-committed floor.

### Block-read API, per feature config

Both configs use **`read_data_block_local(i) -> BlockRef<'_>`**, not
`read_data_block`:

- default build: `read_data_block_local` delegates to `read_data_block` and
  wraps the result as `BlockRef::Owned(Arc<[u8]>)` — cache-checked, one
  positional read per miss.
- `mmap-reads` (`unsafe-fastpath`): an *uncompressed* block returns
  `BlockRef::Mapped(&[u8])` straight from the mmap with no cache traffic; a
  compressed one falls through to `read_data_block`, which decompresses once
  and caches the owned result.

`read_data_block` is the wrong choice here: it bumps the mmap `Arc` refcount on
the fast path, which is precisely the per-entry clone that
`docs/performance.md` records as a 3× scan regression. The batch scan consumes
each block within the `Arc<Reader>` borrow it already holds, so the borrowed
form is both correct and cheaper. This mirrors what `get_unfiltered` already
does.

## Algorithm

1. Capture **one** `read_seq` (DB: `read_floor_seq()`; `Txn` fixed: `read_seq`)
   and one `now` (`coarse_now_nanos()`).
2. Take **one** state snapshot for the whole batch. New helper
   `ColumnFamily::batch_read_sources(keys: &[&[u8]]) -> BatchReadSources`
   (**new**), one `self.state.read()`:

   ```rust
   struct BatchReadSources {                       // new
       mem: Arc<Memtable>,
       imms: Vec<Arc<ImmMemtable>>,
       /// Each covering table, with the result indices whose key it covers,
       /// in the same source order `point_read_sources` produces:
       /// L0 newest-first, then one table per level >= 1.
       tables: Vec<(Arc<SstHandle>, SmallVec<[usize; 8]>)>,
   }
   ```

   The existing single-key `point_read_sources` and `PointReadSources` stay —
   `get` is the hot path and must not grow a vector allocation.
3. Probe the memtables per key, in `get`'s exact order:
   `self.ctx.unified` (outside the state lock, as today) → `sources.mem` →
   `sources.imms.iter().rev()`, each through
   `PointReadCandidate::consider_memtable`.
4. Per table, in source order: `bloom_hash` + `bloom_may_contain_hash` for each
   of its assigned keys (bloom negatives increment `bloom_skips` and drop out);
   `find_block` each survivor; **group survivors by block index**.
5. Per distinct block index: one `read_data_block_local`, one `split_block`,
   then for each target key in comparator order one `restart_scan_offset` +
   `scan_point_entry`. The restart search is per key (it is a binary search
   over restart offsets keyed on `(user_key, seq)`, and each key has a
   different target), but the **block read and decompression happen once** —
   which is the entire win. Feed each result through
   `PointReadCandidate::consider`.
6. `finish()` each candidate into `Result<Vec<u8>>`.

No arbitrary key-count limit; plan in bounded chunks against a fixed scratch
buffer so a million-key call does not allocate proportionally.

**Required visibility changes:** promote `Reader::restart_scan_offset` and
`Reader::scan_point_entry` from private to `pub(crate)`, and the
`PointResult` type alias with them, so the per-block loop can live in
`column_family.rs` next to the planner that owns the grouping.

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

1. **Reader visibility.** `sst/reader.rs`: `restart_scan_offset`,
   `scan_point_entry`, `PointResult` → `pub(crate)`. No behavior change.
   Test first: `sst/reader.rs::get_unfiltered_matches_manual_block_walk` — for
   a built table, drive `find_block` → `read_data_block_local` → `split_block`
   → `restart_scan_offset` → `scan_point_entry` by hand and assert the result
   equals `get_unfiltered` for present keys, absent keys, tombstones, and an
   expired-TTL key.
2. **Batch source snapshot.** `column_family.rs`: `BatchReadSources` and
   `batch_read_sources`.
   Test first: `column_family.rs::batch_read_sources_matches_per_key_sources` —
   for a random key set over a CF with L0 overlap and levels 1–3, assert the
   per-table index sets are exactly `{i : point_read_sources(keys[i]).tables
   contains that table}`, and that the table order matches
   `point_read_sources`' order. Plus
   `column_family.rs::batch_read_sources_takes_one_state_lock` — a counting
   wrapper (or a `state` read-guard counter under `#[cfg(test)]`) asserts one
   acquisition for a 64-key batch.
3. **Sequential oracle helper.** Test-only: `fn oracle_multi_get(cf, keys,
   read_seq, now) -> Vec<Result<Vec<u8>>>` replaying N `ColumnFamily::get`s at
   one fixed `read_seq`.
   Test first: `column_family.rs::oracle_matches_get` — the trivial identity,
   so the oracle itself is pinned before anything depends on it.
4. **Batch resolution, memtables only.** `ColumnFamily::multi_get(keys,
   read_seq) -> Vec<Result<Vec<u8>>>` (**new**, `pub(crate)`) implementing
   steps 1–3 and 6, falling back to `consider_sstables` per key for the SST
   half.
   Test first: `column_family.rs::multi_get_memtable_order_matches_get` —
   a key present in the unified store, the active memtable, and two immutables
   with different seqs; assert the batch picks the same winner as `get` in all
   orderings, including equal-seq ties.
5. **Table-grouped, block-deduped SST resolution.** Replace the per-key
   fallback with steps 4–5.
   Test first: `tests/db.rs::multi_get_reads_each_block_once` — 32 keys known
   to share one data block; with PerfContext (0.10) assert
   `block_cache_misses` (cold) and `block_read_bytes` correspond to **one**
   block, and that a second batch is a pure cache hit. Plus
   `tests/db.rs::multi_get_respects_bloom_negatives` — assert
   `CfStats::bloom_skips` grows by the number of absent keys per table.
6. **Oracle property test.** Test first:
   `tests/db.rs::multi_get_matches_sequential_gets` — seeded random key sets
   with duplicates, missing keys, tombstones, single-deletes, TTL entries, and
   values above/below `klog_value_threshold`; over memtable/L0/L1 mixes, a
   custom comparator, and unified mode; asserted equal to
   `oracle_multi_get` at one fixed `read_seq`. Runs in **both** feature
   configs.
7. **Error isolation.** Test first:
   `tests/db.rs::multi_get_corrupt_table_errors_only_dependent_keys` — corrupt
   one table's block (the `corrupt_vlog_value_is_detected` pattern); assert
   keys resolved definitively by a newer source still return `Ok`, and only the
   keys whose resolution needed the corrupt table return `Err(Corruption)`.
8. **Snapshot fixedness under concurrency.** Test first:
   `tests/db.rs::multi_get_is_snapshot_fixed_during_flush_and_compaction` — a
   writer thread plus forced flush/compaction during a large batch; assert the
   batch's results equal the oracle at the captured `read_seq`.
9. **Public entry points.** `db.rs::DB::multi_get`, `txn.rs::Txn::multi_get`
   (overlay first, then `ColumnFamily::multi_get`, then Serializable read-set
   registration).
   Test first: `tests/db.rs::txn_multi_get_sees_own_writes` — buffered puts and
   deletes shadow the store, last-write-wins within the buffer; and
   `tests/db.rs::txn_multi_get_records_same_read_set_as_n_gets` — a
   Serializable txn's read set after one `multi_get` equals the read set after
   N `get`s of the same keys (assert via the conflict outcome: a concurrent
   write to any of the keys aborts the txn in both shapes).
10. **PerfContext counter.** 0.10's context gains `multiget_blocks_deduped`
    (**new**).
    Test first: `tests/db.rs::multiget_blocks_deduped_counts_savings` — N keys
    in one block yields `N - 1`; N keys in N blocks yields 0.
11. **Harness.** Batch size / duplicate ratio / hit ratio knobs; run on local
    flash and, env-gated, on the S3 tier (`ONDADB_S3_ENDPOINT`), publishing
    `S3Metrics.range_gets` per batch beside p50/op.

## Tests (summary)

- Oracle equality over random key sets in both feature configs and both
  layouts.
- One physical block read for many keys in that block; warm second batch.
- Bloom negatives skip tables; source order preserved across both `consider`
  entry points.
- Corrupt table errors only the dependent keys.
- Snapshot fixedness during concurrent writers, flush, and compaction.
- Batch size 1 equals `get` (no regression path).

## Acceptance

Batch phase at 16–256 keys on the S3 tier and on local flash: p50/op improves
beyond baseline spread versus sequential `get` at equal cache state; on S3,
`range_gets` per batch drops by the deduplication factor. No regression at
batch size 1.

## Rollback

New API only. Delete `DB::multi_get`, `Txn::multi_get`,
`ColumnFamily::multi_get`, `BatchReadSources`, and revert the two reader
visibility promotions.
