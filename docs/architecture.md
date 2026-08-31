# Architecture

ondaDB is a leveled-compaction LSM engine with WiscKey value separation,
per-column-family storage, MVCC transactions, and a striped, batch-framed WAL.
This file maps the modules and traces every major data path. Anchors are
type/function names — grep for them; line numbers rot.

## Module map

| Module | Role |
|---|---|
| `db.rs` | `DB`/`DbInner`: CF registry, global sequence + publish machinery, snapshot refcounts, background flush/compaction workers, recovery, manifest persistence, deferred SST deletion, `LOCK` file, fail-stop poisoning |
| `column_family.rs` | `ColumnFamily`: per-CF memtable + WAL + LSM levels; commit application, memtable rotation, flush to L0, point reads (incl. merge-chain folding), iterator construction |
| `txn.rs` | `Txn`: arena-buffered writes, five isolation levels, conflict detection, savepoints; also the `DB::put/get/delete` single-op helpers |
| `txn_lock.rs` | Pessimistic point locks (3.3): the `LockTable`, FIFO grant order with wait-die on `Txn::txn_id`, and the hand-off that denies waiters younger than the new holder. Acquired **outside** `commit_mu` and never held across it |
| `prepared.rs` | Prepared-transaction registry (3.2): the durable writeset a `prepare` reserves, its key reservations, and the WAL-generation pins that keep an unresolved prepare recoverable across reopen |
| `perf.rs` | `PerfContext` (0.10): caller-owned, per-operation read-path counters on a thread-local scope stack; the nil path is one `Cell` load and a compare |
| `memtable.rs` | Sharded (16) MVCC write buffer; `put_batch` shard-grouped inserts; `snapshot()`/`MemIterator`; `FlushMerge` (fastpath); the per-memtable `RangeTombstoneSet` |
| `range_tombstone.rs` | Range deletes (1.2): the live `RangeTombstoneSet` beside the point shards, the durable `Fragment` form, the aux-section codec, and the `RangeMask` cursors the read paths apply |
| `span_index.rs` | Committed-span index (1.2): the conflict domain a range delete needs, bounded by `Options::span_index_capacity` and pruned at the oldest live snapshot |
| `excise.rs` | Delete-only excise (1.2): the picker pre-pass and `DB::excise_covered` that retire whole tables by catalog edit when durable fragments prove every key they hold is deleted |
| `memtable_arena.rs` | *(unsafe-fastpath only)* arena skip-list shard: single-allocation nodes with inline key prefix + seq; `ShardCursor` for zero-copy flush |
| `wal.rs` | Striped write-ahead log: batch frames, group commit (Full mode), replay |
| `sst/` | SSTable `writer.rs` (klog/vlog/bloom/index/footer), `reader.rs` (point get, block reads, CRC-once bitmap, mmap fastpath), `iter.rs` (bidirectional iterator, cached key prefix), `mod.rs` (formats, `Block`) |
| `table_cache.rs` | `TableCache`: sharded (CLOCK) LRU of open SSTable readers, bounding resident index+bloom memory by reader count (`max_open_readers`) and byte budget (`max_open_reader_bytes`); the `max_open_files` equivalent |
| `iterator.rs` | `ChildIter` enum (Mem/Sst), heap `MergingIter`, public `Iterator` with MVCC collapse, pinned-block borrowed keys/values and the merge-operand arena (1.1) |
| `tailing.rs` | `TailingIterator`: forward-only keyspace tail that refreshes past its own end (not a change feed) |
| `compaction.rs` | Leveled compaction: pick level, k-way merge, kind-aware version collapse, tombstone/TTL GC, merge-operand folding (1.1), compaction filters; bottom-level output cut at partition boundaries; FIFO style (oldest-table eviction) |
| `ingest.rs` | Bulk ingestion: pre-sorted stream → L0 SSTables directly (no WAL/memtable); atomic install at `finish()` |
| `manifest.rs` | Durable catalog (`MANIFEST`): next file id, global seq, per-CF config blob + SST set (incl. per-table partition/tier/max-entry-time via the append-tolerant tail); crash-atomic save |
| `manifest_edit.rs` | Numbered catalog edits (2.2): `VersionEdit`/`Op` codec, all-or-nothing `apply_edit` with per-op preconditions, the `MANIFEST-EDITS` log writer, the four-step snapshot-compaction protocol and `recover_catalog` |
| `storage.rs` | `Storage`/`ReadHandle`/`StorageWriter` traits, `LocalStorage`, `TierRegistry` — the choke point all SSTable file access flows through so parts can live on multiple tiers |
| `storage_s3.rs` | *(feature `s3`)* `S3Storage`: object-store backend — range-GET reads, single-PUT writes, own tokio runtime |
| `parts.rs` | Part lifecycle: `detach_part`/`attach_part`/`freeze_part`, `move_part_to_tier`, the policy-driven part mover, live partition-rule add/remove |
| `unified.rs` | Optional shared memtable+WAL across CFs (8-byte CF-id key prefix); split flush |
| `ioctrl.rs` | Background IO classes (`IoClass` in a thread-local, `scoped` guards) and the `IoLimiter` trait with a work-conserving `TokenBucket` on an injectable `Clock`; bounds flush/compaction bandwidth so it cannot inflate foreground p99 |
| `block.rs` | Block framing: `[alg][comp_len][raw_len][crc]payload`, compress-if-shrinks |
| `bloom.rs`, `cache/`, `compress.rs`, `comparator.rs`, `encoding.rs`, `format.rs`, `error.rs`, `maintenance.rs` | Support: bloom filters, block/file LRU caches, codecs, key ordering, varints/CRC, flag bits + internal keys, error codes, checkpoint/backup/clone/stats |

## On-disk layout of a database directory

```
<db>/
  MANIFEST                 # catalog (see formats.md); rewritten atomically
  cf-<name>/               # one directory per column family
    wal-<gen>.log          # WAL stripe 0 (generation marker)
    wal-<gen>.log.s1..s3   # WAL stripes 1..3 (non-Full sync modes)
    <fileid>.klog          # SSTable keys + inline values + bloom + index
    <fileid>.vlog          # SSTable large values (only if any value >= threshold)
    detached/<partition>/  # file pairs moved aside by DB::detach_part
  unified-wal-<gen>.log[.sN]  # unified-memtable mode only

<tier root>/               # per named tier (Options::tiers) — dir or S3 prefix
  cf-<name>/
    <fileid>.klog          # bottom-level part files moved to this tier
    <fileid>.vlog
```

One WAL *generation* corresponds to one memtable lifetime; rotation bumps the
generation. SST file ids come from the manifest's `next_file_id` counter and
are unique db-wide (also across tiers — a moved part keeps its ids, so the
block cache, keyed by id, never collides). WAL and upper levels always live
in the database directory; only bottom-level parts may live on a named tier
(`SstMeta.tier` in the manifest records where).

## Write path (`Txn::commit`)

1. Buffered writes live in a per-txn byte arena (`Txn::buf`) with
   `WriteEntry { key: (off,len), value: (off,len), … }` ranges — zero
   per-op allocations at the API boundary.
2. Dedup: last write per (cf, key) wins, sequenced in first-write order
   (`slot_of` map hashing key slices with xxh3; single-write txns skip it).
   A transaction holding a **merge operand** (1.1) takes the merge-aware
   variant instead: operands compose rather than replace, so a key commits the
   run of writes starting at its last *base* — identical to the collapsed order
   whenever that run has length one, which is every key of every merge-free
   transaction.
3. A commit holding a range delete first rejects any of its **own** point
   writes that fall inside one of its own spans (v1 restriction — see
   `Txn::check_own_range_overlap`), then reserves span-index capacity **with no
   lock held**.
4. **Every** commit takes `DbInner::commit_mu` (3.2 phase rule 5) and first
   probes the prepared-transaction reservation registry: a key another
   transaction has prepared refuses this commit with `Conflict`, at every
   isolation level, including the single-op `DB::put`/`DB::delete` path. With
   no prepared transaction outstanding that probe is one relaxed load. Then
   Snapshot/Serializable run the write-write
   conflict check via `ColumnFamily::peek_seq`; Serializable additionally
   validates the point-read set (`read_cfs`). Snapshot/Serializable also ask
   the committed-span index: a range writer conflicts with any overlapping
   marker newer than its snapshot, a point writer with any newer *covering
   range* marker.
5. Reserve a contiguous seq block: `DbInner::reserve_seq(n)`. Range deletes
   take their own slot in the dedup order — never collapsed into a point write
   whose key equals their start bound — so their sequence is distinct by
   construction.
6. Build per-CF `Vec<RecordRef>` **borrowing the txn arena** (`CfGroup`), plus a
   `Vec<RangeRef>` of that CF's range deletes; `CommitOp` hook payloads are
   built only if `cf.has_commit_hook()` (range deletes are not surfaced to
   hooks in v1 — `CommitOp` describes one key and one value).
7. `ColumnFamily::apply_commit_with_ranges(&recs, &ranges)`:
   - gate: wait while rotating or imm-queue ≥ `l0_queue_stall_threshold`;
     increment `active_writers`
   - `Wal::append_batch(&recs)` — encodes ONE frame in this thread, writes it
     to this thread's WAL stripe (see `docs/formats.md`). A batch that carries
     anything the legacy record cannot spell — a 1.1 merge operand, whose kind
     has no flags-byte encoding, or a 1.2 range delete — instead writes one
     **envelope** frame carrying every kind together (`append_batch_envelope`,
     or `append_batch_enveloped` for a point-only batch). One frame either way,
     because batch atomicity is per frame. The test is on the batch, not the
     family, so every ordinary commit keeps its 0.8.2 frame bytes, including on
     a family that merely *has* a merge operator
   - `Memtable::put_batch(&recs)` — counting-sorts into per-shard runs, one
     shard lock per batch, nodes prebuilt outside locks, counters updated once
   - `Memtable::add_range` per range delete, into the `RangeTombstoneSet`
   - decrement `active_writers`; if memtable ≥ `write_buffer_size`, call
     `rotate_memtable(false)`
8. `DbInner::publish_range(start, end)` — advances `visible` gap-free.
9. Span markers are inserted, still under `commit_mu`, only after every record
   is installed.
10. Run commit hooks (outside `commit_mu`).

Unified-memtable mode replaces step 6 with `UnifiedStore::apply` (records get
an 8-byte big-endian CF-id key prefix, one shared WAL + memtable). Its sealed
queue is bounded by `unified_memtable_stall_threshold` (default 6).

In per-CF WAL mode, commit rejects a transaction touching more than one column
family: independent WAL frames cannot provide crash or partial-I/O atomicity.
Unified mode encodes every touched CF in one shared WAL frame and is the
supported atomic cross-CF layout.

### Prepared transactions (`prepared.rs`, 3.2)

Unified layout only, for the same reason cross-CF commits are.

`Txn::prepare(id)` consumes the transaction, validates it at its own isolation
level, reserves every key it writes, appends **one** prepare frame (kind 16,
the id + cf ids + the whole writeset, every record at `seq = 0`) and fsyncs
**the handle it appended to** — a rotation can replace the store's current WAL,
so `UnifiedStore::sync_wal` would miss the frame. Nothing is applied, nothing is
published, and **no sequence is reserved**: a prepare that never commits leaves
no gap. The arena is *moved* into the registry, never recycled into
`txn::BUF_POOL`.

`DB::commit_prepared(id)` is the seven-step second phase, and the order is the
whole correctness argument — decision **first**, then the apply, because
recovery must never find applied data without a durable decision naming the
sequences it was applied at:

1. `commit_mu`, then the registry lock (held across the fsync, so a failed
   decision cannot leave a retry answering `Ok`);
2. reserve the sequence block;
3. append + fsync the kind-17 decision on the **captured** handle, recording the
   generation it landed in from the same `state.read()`;
4. `UnifiedStore::apply_memtable_only` — a path that writes the memtable and
   **not** the WAL; the decision is already this batch's durable record, and a
   second copy of the writeset in the log would leave recovery reconciling it
   against the decision. The same path recovery pass 2 uses;
5. `publish_range`, **unconditionally**, on every exit path after the
   reservation — a failed decision, fsync or apply still publishes its block, or
   the gap-free cursor freezes permanently (invariant 5);
6. resolve the registration and record the generation pair;
7. drop `commit_mu`, run commit hooks, sweep retirable WAL generations.

`DB::abort_prepared(id)` writes and fsyncs a kind-18 decision, then drops the
registration. No sequence is reserved and none is published.

`DB::list_prepared()` reports id, age, bytes and cf ids. Nothing is ever aborted
automatically. See `docs/concurrency-and-safety.md` for the registry's lock
position, the WAL generation pins and their retirement predicate, and the
RV-M3 latency contract this makes worse.

## Read path

Point get (`ColumnFamily::get`): consult, newest-wins by seq —
unified store (if enabled) → active memtable → immutable memtables (newest
first) → L0 tables whose [min,max] covers the key (all of them; L0 overlaps) →
one binary-searched table per level ≥ 1.

**Range-delete masking (1.2)** runs beside that walk and is resolved against it
at the end: the maximum *covering* sequence at or below `read_seq` is taken
across the memtable sets, the unified set, every L0 table whose **span**
contains the key, and per level ≥ 1 the point candidate plus its **gap owner**
— the table to its left, consulted only when that table has `range_count > 0`
and the key sorts below its `range_max_key`. The key is deleted iff the covering
sequence is above the surviving point version's, or there is no point version.
At most two tables per level, one binary search, and one `range_count == 0`
branch for every legacy or point-only table; a column family that never issues a
range delete allocates nothing (pinned by
`no_range_cf_allocates_nothing_on_read`).

Iterators apply the same rule per surfaced group, through a monotonic cursor per
source that walks with the scan in either direction. SSTable get: bloom filter →
`find_block` binary search on the in-memory index → linear entry scan inside
the data block → inline value or vlog read (CRC-verified).

### Merge operands (1.1)

A column family with a merge operator takes a parallel resolution path, entered
by a single `Option::is_none()` at the top of `get` — the no-operator family
runs exactly the code it ran before 1.1.

`get_with_merge` asks each source for a *chain* rather than a winner:
`Memtable::chain` / `UnifiedStore::chain` walk a key's versions newest-first,
and `collect_table_chain` drives an `SstIterator` seeked to `(key, read_seq)`
(the iterator, not the point-read block walk, because a key's versions are
contiguous in internal order but may straddle a block boundary). Each source
contributes its own run — every operand it holds plus the base that terminates
them — and `fold_chain` orders the runs by sequence, collects operands down to
the first base, and calls `full_merge` with them oldest-first. `multi_get` keeps
its single snapshot and single clock reading but resolves each key's chain
separately, so it does not dedupe blocks across a merge batch.

In the merge iterator, `resolve_current_group` accumulates operands into a
**per-iterator arena** (`operands: Vec<u8>` plus `operand_spans`), entered
lazily on the first kind-4 entry of a group and `clear()`ed rather than shrunk,
so steady-state scanning allocates nothing after warm-up. Copying is what
invariant 8 leaves available: operands of one group can come from several
children and several blocks, so they cannot all be pinned, and per-entry `Arc`
clones of shared mmaps are the measured 3x scan regression. The folded value is
published as `CurVal::Buffered`; no pin slot changes hands. Forward iteration
sees the group newest-first and reverses the operands before folding; backward
sees it oldest-first, and each base clears the operands it superseded.

A transaction's own buffered operands are resolved against the buffer first
(`Txn::buffered_chain`), and only a chain with no buffered base reaches the
store — which is then a genuine read and is recorded in the read set. The
overlay memtable a scan builds **pre-folds** merged keys: every buffered write
lands at the same sequence, which is what makes last-write-wins work for puts,
and operands cannot share a sequence that way because they compose. Pre-folding
costs one point read per merged key and keeps the merge iterator free of any
overlay special case.

**Composed with a range delete (1.2), a span is a deleted base at its own
sequence.** That one sentence is the whole rule, and it is implemented as that
sentence in all three read paths rather than as a second set of conditions:
`fold_point_chain` pushes the covering sequence into the chain as a
`ChainVersion { merge: false, value: None }` and lets the existing "stop at the
first base" ordering do the rest, so operands above the span fold onto nothing
and operands at or below it — with the base under them — vanish. The merge
iterator drops operands at or below the span, nulls a base at or below it, and
re-seats the folded group's sequence at its **newest surviving operand**, which
is what stops `masked_by_range` from hiding a chain that sits entirely above the
span. Compaction ends a pending fold on a key change *before* the range-mask
drop, so a fully masked key cannot strand the previous key's operand suffix.
`tests/composition.rs` requires the point, batch and both scan directions to
agree on all of it, in the memtable, after a flush and after a compaction.

**Composed with a prepared transaction (3.2)**, an operand is an ordinary
one-key write: a prepare frame carries kind 4 in its writeset and recovery folds
it on `commit_prepared`. A range delete cannot be prepared — two keys and no
value have no shape in that frame — and `Txn::prepare` refuses it at the API
rather than letting replay discover it.

**The block cache holds two key domains.** A `Reader` owns two files under one
`file_id` — the klog and the vlog — and their offset spaces are independent and
both start at zero, so `(file_id, 0)` names both the first data block and the
first vlog frame. `BlockKey` therefore carries a `BlockDomain` (`Klog` |
`Vlog`), mixed into `shard_for` so the two domains do not share a shard in
lockstep, and every `get`/`put`/`remove` names one. Decompressed klog data
blocks are always admitted; decoded vlog values are admitted only when the
family sets `max_cached_vlog_value_bytes` (default 0 = off) and only up to that
size. Both domains share one capacity, so `CacheStats` reports the vlog share
separately (`vlog_hits`, `vlog_misses`, `vlog_entries`, `vlog_bytes`) and
`hits`/`misses` stay klog-only.

A vlog value is admitted at one place — the join point of the mmap and buffered
decode paths in `Reader::read_vlog_into`, after a complete decode — so nothing
cancelled, truncated or CRC-failed can ever enter the cache, and both feature
configs share one admission rule. The lookup sits *before* the mmap attempt,
because a v2 frame is decompressed on every mmap read (`vlog_verified`
memoizes the checksum, never the bytes), so the cache is the only thing that
removes that cost under `mmap-reads`.

Retiring a table (compaction, part move) evicts nothing: `next_file_id` never
reuses an id, so a dead table's entries can never be mistaken for a live one's
and simply age out under CLOCK. Explicit eviction there would be hygiene, not
correctness, and is not done.

Batched point get (`DB::multi_get` / `Txn::multi_get` →
`ColumnFamily::multi_get`): the same sources in the same order, resolved for N
keys in one pass. It captures **one** read sequence, **one** `coarse_now_nanos`
and **one** state snapshot for the batch (`batch_read_sources`, a single
`state.read()` that groups each candidate table with the result indices whose
keys it covers), so a flush or compaction landing mid-batch cannot make two keys
of the same call see different sources. Per table it bloom-filters and
`find_block`s each assigned key — both are inherently per key — then groups the
survivors by block index and does **one** `read_data_block_local` per distinct
block, running `restart_scan_offset` + `scan_point_entry` per key against it.
`PerfContext::multiget_blocks_deduped` counts the fetches that saved. Results go
through the same `PointReadCandidate::consider`/`consider_memtable` entry points
as `get`, so newest-wins (and equal-seq ties) resolve identically. One
divergence from N `get`s, deliberate: a failing source errors only the keys
whose resolution needed it — a key a strictly newer source already resolved
keeps its value, where `get` propagates the error.

A table's filter strength is chosen when it is **written**, from its output
level: `ColumnFamilyConfig::bloom_fpr_for_level(level, bottom)` returns the
rate (`bloom_fpr_per_level`, last entry repeating, or the uniform `bloom_fpr`)
or `None` for "write no filter block" when `optimize_filters_for_hits` is set
and the output lands in the bottom level (`compaction::is_bottom_target`).
Flush and ingest always pass `bottom = false`; only compaction can omit a
filter. A table without one is read exactly as before — `bloom_may_contain_hash`
answers `true` — so the omission costs negative-lookup speed, never
correctness. See `ColumnFamilyConfig::optimize_filters_for_hits` for the
one-way degradation this implies.

Every SSTable touched by a read or scan resolves its reader through the shared
`TableCache` (`table_cache.rs`), not by opening the file directly: `SstHandle`
holds only the information to re-open a reader and asks the cache for one. A
reader carries the table's whole block index and bloom filter resident, so the
cache bounds that memory by **both** an open-reader count (`max_open_readers`,
default 512) **and** a byte budget (`max_open_reader_bytes`, default 1 GiB),
evicting least-recently-used readers past either bound — the `max_open_files`
equivalent. Closing a reader is sound because it is a pure, re-derivable view of
an immutable file (an eviction only drops the cache's `Arc`; a caller mid-read
keeps its own until done). The cache is sharded with second-chance (CLOCK)
replacement, so a hit takes a shard read lock rather than a process-global
mutex, and point reads scale across cores.

Iterator (`ColumnFamily::new_iterator`): builds `ChildIter`s over the txn
overlay (optional), unified slice (optional), the active memtable, the imms, and
every SST, then heap-merges in internal order collapsing MVCC versions
(`Iterator::advance_forward/backward`). Keys and values are returned as
borrowed slices from per-child pinned blocks where possible; see
`docs/concurrency-and-safety.md` § Pinned blocks.

### Keyspace-tailing iterator (`tailing.rs`, 0.9)

`DB::new_tailing_iterator(cf)` returns a `TailingIterator`: a forward-only
cursor over an append-only ordered keyspace that can be advanced *past its own
end* instead of being rebuilt per poll. It holds one ordinary `Iterator`
("segment") at a time, plus the last key it yielded. `refresh()` is non-blocking
and rebuilds only when the segment is exhausted **and** `read_floor_seq()` has
advanced since the segment was built; the new segment is
`cf.new_iterator(floor, None, (Bound::Excluded(last_yielded), Bound::Unbounded))`
followed by `seek_to_first()`. Both no-op paths cost a `valid()` check and one
atomic load. The target workload is the queue peek in `memtable.rs`'s header,
where per-poll iterator construction is the dominant cost.

**This is not a change feed.** A refreshed tail may observe only keys that
compare strictly greater than its last yielded key; an insert, update or delete
at or behind the cursor is never observed, and an already-yielded key is never
re-yielded. There is deliberately no `prev` and no `seek_for_prev`.

Each segment reads at `DbInner::read_floor_seq()` — the read-committed floor,
captured afresh per segment, never a pinned snapshot. Refreshing a *fixed*
snapshot would silently break that snapshot's guarantees, which is why the tail
is a `DB`-level API with no `Txn` equivalent (a `Txn` passes its buffered writes
as the `extra` overlay; the tail always passes `None`). `now` for TTL expiry
comes from `new_iterator`'s own `coarse_now_nanos()` call, so a long-lived tail
re-evaluates expiry per segment rather than against its construction time. A
segment whose sources cannot all be opened is an `Iterator::failed(..)`, so
`err()` must be checked after a walk goes invalid — otherwise a missing table
reads as "no more entries". `segments()` reports iterator constructions, which
is what the acceptance benchmark divides by yielded entries.

Known cost, accepted for v1: in unified-memtable mode the CF-scoped overlay is
rebuilt per segment, so a tail that refreshes often pays more there than under
the per-CF layout.

### Lazy memtable iterator (`LazyMemIter`, default build)

The memtable `ChildIter::Mem` is **lazy**. It does *not* materialize the
memtable. `LazyMemIter` (`memtable.rs`) runs a bidirectional k-way merge
(`MemMerge`) directly over the 16 shard skip lists — one persistent
`crossbeam_skiplist::map::Entry` cursor per shard, which is an `O(1)`
forward/backward cursor (`move_next`/`move_prev`) plus `lower_bound`/`upper_bound`
for (re-)seeks. So **constructing/positioning a memtable iterator is `O(shards)`,
not `O(entries)`** — the fix for a measured pathology where a prefix scan reading
a single record cost 1.3 ms at 2k entries and 5.1 ms at ~15k (growing linearly
with memtable size) because the old path cloned and sorted every entry into a
`Vec<Entry>` on *every* iterator construction.

`LazyMemIter` owns the `Arc<Memtable>` and, in the same struct, holds cursors
borrowing from inside it. That self-reference is expressed safely with the
`self_cell` crate (macro-only; its `unsafe` is contained in that crate), so the
default build keeps unsafe denied. The only default-build exception is the
audited Linux `clock_gettime(CLOCK_REALTIME_COARSE)` call used for TTL checks.

For a bytewise CF in unified mode, `UnifiedMemIter` lazily bounds the shared
memtable cursor to that CF's contiguous eight-byte id prefix and strips it from
reported keys. Custom-comparator CFs retain the materialize-and-reinsert path,
because the shared memtable's bytewise order cannot represent their ordering.

Bidirectionality follows LevelDB's merging-iterator scheme: a `dir` flag selects
a min-heap (forward) or max-heap (backward); reversing direction repositions
every non-top shard cursor relative to the current key
(`flip_to_forward`/`flip_to_backward`), so arbitrary `next`/`prev` interleaving
matches a random-access cursor over the sorted sequence — behaviourally identical
to the old materialized `Vec`. crossbeam range cursors are double-ended, which is
what makes the reverse direction cheap.

**Snapshot consistency.** The cursors read the live, lock-free skip lists, so
they can physically observe entries inserted *after* the iterator was built.
That is harmless: sequence numbers are monotonic and become visible only through
the gap-free `visible` cursor, so a reader's `read_seq` implies every seq
`<= read_seq` was already published — hence already in the memtable — before
`read_seq` was observed. Any later insert therefore has `seq > read_seq`.
`MemMerge` itself does no seq filtering (it yields every version, exactly as the
snapshot path did); the public `Iterator` drops `seq > read_seq` during version
collapse. So a later insert is either skipped (a phantom user key whose only
versions are `> read_seq`) or shadowed by the visible older version — the visible
result equals a point-in-time snapshot at `read_seq`. Imm memtables are sealed
(no writer, ever), so only the active memtable can grow under an iterator.

The materialized `Memtable::snapshot()` path is **retained for flush**
(`flush_imm` on the safe build). Under `--features unsafe-fastpath` the arena
shard cursor is forward-only, so the *read* iterator keeps the snapshot path
there (the default build is what ships); the fast path's flush still uses the
zero-copy `FlushMerge`/`ShardCursor`.

Internal order everywhere: `(user_key ascending via comparator, seq
descending)`. Internal key encoding: `user_key || big_endian(!seq)`
(`format.rs`), so newer versions sort first.

## Memtable rotation & flush

`rotate_memtable(force)`:
- losers of the race return immediately (only `force` callers wait)
- winner: set `rotating`, **pre-open the next WAL generation while in-flight
  writers drain**, wait `active_writers == 0`, swap memtable + WAL under the
  state write lock, enqueue `FlushJob::PerCf { imm }`.

Flush worker (`db.rs::flush_worker`):
- `ColumnFamily::flush_imm` → under fastpath `write_l0_streaming`: a 16-way
  `FlushMerge` over borrowing `ShardCursor`s feeds `Writer::add` directly —
  no `Vec<Entry>` materialization, no sort. Safe build: `snapshot()` + sort.
  Nothing is published: `flush_imm` returns a `FlushOutput` holding the finished
  handle (`None` for an empty memtable) and the WAL paths.
- The sealed memtable's `RangeTombstoneSet` is fragmented over the whole
  keyspace and handed to the writer (`set_range_fragments`) — one L0 file, one
  owned interval, so nothing is clipped. A memtable holding **only** range
  tombstones still produces a table: dropping it as "empty" would lose the
  deletes.
- `Writer::finish` fsyncs klog+vlog and the CF directory.
- `catalog_txn(AddTable, publish_flush)`: the record is appended and **fsynced**,
  and only then is the table installed in L0 and the sealed memtable retired,
  under one state write-lock. **Only on `Ok`** are the imm's WAL files deleted
  (`wal::remove_wal_files` unlinks all stripes) — invariant 1's gate is the edit
  fsync, never a snapshot write.
- If L0 count ≥ `l1_file_count_trigger`, enqueue compaction.

## Compaction (`compaction.rs`)

Classic leveled: L0→L1 on file count, Li→Li+1 when level bytes exceed
`l1_base_bytes * level_size_ratio^(i-1)`. Merge-iterates inputs plus
overlapping next-level tables; keeps the newest version per key plus every
version newer than `DbInner::oldest_snapshot()`; drops tombstones and expired
TTL entries only at the bottom level. Output SSTs are split at
`target_file_size` and, at the bottom level, additionally **cut at partition
boundaries** (see § Partitions).

**Range fragments (1.2)** are merged over the job span and re-fragmented over
this span's boundaries, then each output takes the slice of them that falls in
the interval it owns — `[o_i.min_key, o_{i+1}.min_key)`, with the first extended
down to the job span and the last up to it. Clipping is what preserves level-≥1
point disjointness, which `find_overlapping` and the read path's gap-owner rule
depend on (`docs/formats.md` § Aux block). Consequences implemented explicitly:
`gather_target` and `key_span` expand the job to the union of the inputs'
**span** bounds, not their point bounds, so an input fragment reaching past the
last point key is never left outside the job; the point stream is filtered
against the pre-drop fragment stack **before** `VersionRetention::decide` sees
it, so a shadowed point is dropped without being counted as the one version at
or below `oldest_snapshot`; and a fragment sequence is itself dropped only at
the bottom, only at or below `oldest_snapshot`, and never when a foreign mount
overlaps its span. Because outputs are already cut at partition boundaries,
clipping also gives "no fragment crosses a partition" for free. Every output
carries
`max_entry_time = max` over its inputs' stamps, so re-compacting cold data
does not reset its age for the part mover. Ordering: outputs written and fsynced → ONE edit
(`RemoveTables` for every input plus `AddTable` for every output) appended and
fsynced → new levels installed → inputs deleted via `DbInner::remove_sst_file`
(defer-aware, and paced when `obsolete_delete_bytes_per_second` is set — see
§ Paced obsolete-file deletion). Input deletion resolves **default-tier paths only** — a
compacted input that lived on a named tier is not unlinked there (a storage
leak, never a correctness issue; see `docs/parts-and-tiers.md` § Known gaps).

### Merge-operand retention and folding (1.1)

`VersionRetention::decide` takes the record **kind**, because the "keep exactly
one version at or below `oldest_snapshot`" rule is correct only for point
kinds — each of which replaces everything older. A merge operand composes, so:
it neither consumes that slot nor sets `emitted_at_or_below_snapshot` (only the
base terminating the chain does); a bottom-level tombstone that terminates a
chain whose operands are still live is not reclaimed; the bottom TTL drop never
applies to an operand; and an operand is not bloom-filter-eligible. Everything
*older* than a chain's base is still dropped exactly as before — the drop that
follows from the flag applies to every kind.

On top of that, `run_span` may **fold**: it collects a key's operand suffix
(`PendingFold`) and, on reaching the base, writes one kind-1 entry carrying the
suffix's newest sequence. Folding is a space-and-read optimization and never a
correctness or durability requirement; it is fenced by `oldest_snapshot` (only
a suffix wholly at or below it folds), by the bottom predicate (a suffix that
met no base folds only where there is nothing below), by a live TTL on the base,
and by `Options::enable_merge_folding` — a rollout switch, because a fold bug
silently rewrites history. `tests/merge.rs::fold_oracle` is the guard: pre- and
post-compaction reads must be identical at every snapshot.

### Delete-only excise (1.2)

`excise.rs` retires a whole SSTable **by catalog edit, without reading it**,
when durable range-tombstone fragments prove every key it holds is already
deleted. It is the sub-part-granularity counterpart to `detach_part`: that one
drops a partition's bottom tables because an operator named the partition; this
one drops any table at any level because the data says so.

A candidate `T` qualifies when qualifying fragments — installed in some table's
aux section, never a memtable's live set — satisfy

```text
fragment.start <= T.min_key   AND   T.max_key < fragment.end
T.max_seq < fragment.seq <= oldest_snapshot
the union of them is gap-free over [T.min_key, T.max_key]
every OWNER of that union lies outside the dropped set
```

The first line generalizes to the union; no single fragment has to span `T`.
The last is the contiguous-set rule — for one table it reads "never drop the
only durable owner of the tombstone that justifies the drop", and offending
members are removed from the candidate set until the intersection is empty.
v1 additionally refuses a candidate carrying fragments of **its own**: the
preconditions establish that `T`'s points are dead, but say nothing about the
tables `T`'s fragments mask, and a fragment at level `L` shadows every level
below it. That restriction is also what makes the owner rule hold by
construction, since an owner carries fragments and is therefore never a
candidate.

Vetoes: foreign mounts (this database neither unlinks nor reasons about another
database's publication), any table off the **default tier** (obsolete-file
deletion resolves default-tier paths only, so an S3-resident excise would leak
the object rather than reclaim it — this subsumes the shared-tier rule), an
overlapping range-lock holder, and any in-flight part operation
(`DbInner::parts_in_flight`).

The transaction: plan lock-free over a level snapshot → `parts::try_lock_key_span`
(the key-span generalization of `lock_partition_span`, non-blocking, so a
compaction holding the range is a veto rather than a queue) → **revalidate**
under that lock → ONE `RemoveTables` edit through `catalog_txn`, publishing via
`ColumnFamily::remove_tables` → retire files through `DbInner::remove_sst_file`
(invariant 6). A failed transaction fail-stops the database, exactly as
`detach_part`'s identical shape does.

It runs as a **pre-pass in the picker** (`compaction::run`, ahead of capacity
scoring — a covered table is free to drop and rewriting it first pays to move
bytes about to be unlinked) and as `DB::excise_covered(cf)` for operators. A
flush that published fragments therefore wakes the compaction worker even with
L0 far below its trigger: a bulk delete followed by an idle database is the
case excise exists for, and it produces no capacity pressure of its own.
`CfStats::excised_tables` / `excised_bytes` report what it reclaimed.

### Bounded jobs and backpressure (0.8.0)

> User-facing guide with the tuning knobs and worked symptoms:
> [`compaction-and-write-pacing.md`](compaction-and-write-pacing.md).

A job takes **one** file from the source level plus only the target-level files
its range overlaps, so it costs about
`target_file_size * (1 + level_size_ratio)` however large the level is. A
per-level cursor sweeps the keyspace so successive jobs advance rather than
re-picking the head of the level. L0 is the exception twice over: its files
overlap each other, so a job takes the **oldest** `l1_file_count_trigger` of
them — safe because `levels[0]` is newest-first and reads walk it in that
order, so a version left in a newer L0 file still shadows the copy pushed down.

### Which file the sweep takes first (0.2)

Within a level `i >= 1`, candidates are visited in ascending **overlap ratio**
— `overlap_bytes(c) / max(1, c.klog_size + c.vlog_size)`, where
`overlap_bytes` is the full size of every `levels[i+1]` table the candidate's
span intersects (whole tables: a job rewrites them end to end, not the fraction
of them the span covers). Ratios are compared by cross-multiplication in
`u128`, so two ratios that differ by one byte never compare equal the way f64
would round them together. Ties fall back to cyclic distance from the cursor,
which keeps the sweep's fairness where scores are uniform.

It is an **ordering, not a selection**. The try-loop is still a full sweep that
wraps: the minimum-score candidate may be unusable, either because
`gather_target` vetoes it (a foreign mount overlaps its *target* span — the
candidate filter cannot see that, since it only screens the source table) or
because `lock_job` finds the range held by a running job. Stopping at the
minimum would wedge the level on either. The cursor still advances only after a
usable pick, and still stores the picked table's `max_key`. **L0 is excluded**:
its oldest-`l1_file_count_trigger` window is a correctness invariant, not a
cost choice.

Measured on the overlapping-level fixture (`tests/support/levels.rs`, skewed
record sizes so overlap ratios actually differ): compaction bytes per ingested
byte fell from 2.77 to 2.48, roughly 10%. Raw runs in `bench-results/0.2/`.

Before 0.8.0 a job took the whole source level plus every overlapping target
file. Under random keys an L0 file spans nearly the entire keyspace, so each
push-down rewrote all of the level below, and the work in one job grew with the
dataset. `target_file_size` and `l1_base_bytes` did not exist: both were
`write_buffer_size`, which meant L1 held exactly one file whose range covered
everything beneath it — partial compaction was not merely unimplemented, the
geometry made it impossible.

Two consequences. Jobs on disjoint ranges share no inputs, so they run
concurrently (§ Range locks). And debt is measurable —
`compaction::pending_compaction_bytes`, cached on the CF and refreshed by flush
and compaction — which is what `ColumnFamily::apply_commit` paces against:
proportional delay past `soft_pending_compaction_bytes`, blocking at
`hard_pending_compaction_bytes`. Without that, ingest ran at memtable speed no
matter how far compaction lagged, since `l0_queue_stall_threshold` gates on
sealed memtables awaiting *flush* and flush was never the bottleneck.

### Range locks (`range_lock.rs`)

Compaction and the parts/tiers operations exclude each other by **key range**
rather than by a CF-wide mutex. Compaction `try_acquire`s and picks different
work when a range is held; `detach_part`, `attach_part`, `attach_part_by_ref`
and `relocate_part` `acquire_blocking` because they are user-initiated and must
not fail spuriously. `cf.compact_mu` now guards only whole-CF operations (the
manual `DB::compact` sweep and FIFO eviction), both of which additionally take
the whole keyspace. Lock order is always `compact_mu` → range lock.

## Partitions (`ColumnFamilyConfig::partition_rules`)

A **partition** is a named slice of the keyspace declared by prefix rules
(`PartitionRule { prefix, name }`, `config.rs`). Resolution is
longest-matching-prefix (`partition_of`), so rules may nest (`img/` and
`img/thumb/` coexist; only an exact-duplicate prefix is rejected by
`ColumnFamilyConfig::validate`). Keys matching no rule belong to the implicit
default partition (`None`).

Partitions materialize only at the **bottom level**: when a compaction's
target is the bottom, it snapshots the rules once for the run
(`partition_rules_snapshot`) and, since keys arrive in ascending user-key
order, finishes the current output file whenever `partition_of` changes —
so no bottom SSTable ever spans two partitions, and each is stamped with
its partition in `SstMeta.partition`. Upper levels stay mixed (`None`).

Why bottom-only: upper levels are young, transient data that L0 overlap and
push-down merges churn constantly — cutting them would multiply file counts
for boundaries that the next merge erases anyway. The bottom level holds the
durable bulk and is the only level where a partition's file set is stable
and *partition-clean*, which is exactly what the part machinery (detach /
freeze / tier moves) needs as its unit.

Rules are write-side-only policy: changing them (including live, via
`DB::add_partition_rule` / `remove_partition_rule`, `parts.rs`) affects only
files written by future bottom compactions; existing files keep their stamps
until a later compaction re-cuts them. A compaction already in flight
finishes on the rules it snapshotted. Live-added rules are persisted through
`effective_config()` in the manifest config blob (the durable `opts` copy is
otherwise immutable), so they survive reopen.

## Storage tiers (`storage.rs`)

All SSTable file access flows through the `Storage` trait — the seam that
lets a column family keep bottom-level parts on more than one location:

- `open_read(path) → Arc<dyn ReadHandle>` — positional reads
  (`read_exact_at`, `size`); local backends wrap a `FileCache`-shared `File`,
  S3 issues one HTTP range GET per read.
- `create(path) → Box<dyn StorageWriter>` — a `Write` sink committed by
  `finish()` (fsync file + parent dir locally; single-shot PUT on S3).
- `ensure_dir` / `delete` / `rename` / `list` / `release` — namespace ops
  (no-op or emulated on object stores).
- `supports_mmap()` — whether readers may mmap files on this backend.

The `TierRegistry` maps a tier name (`None` = the implicit default tier, the
DB directory; the name `"ssd"` is reserved as its alias) to a root plus a
`Storage`. `DB::open` builds it from `Options::tiers`: each `TierDef`
resolves to a `LocalStorage` (honoring `supports_mmap`), an `S3Storage`
(`TierBackend::S3`, feature `s3`), or a caller-provided backend used
verbatim (`TierBackend::Custom` — the P8 injection seam; an embedder wraps a
remote backend with e.g. a read-through cache and hands the wrapper in via
`TierDef::custom`). An unknown tier name degrades to the default root rather
than losing the file — the manifest stays the source of truth.

**Read dispatch.** `ColumnFamily::open_reader_for(meta)` resolves
`meta.tier` through the registry to a path (`klog_path_for`) and backend,
and hands both to `sst::Reader::open`. Under the `mmap-reads` feature the
reader mmaps the klog **only if** `storage.supports_mmap()`; otherwise —
no-mmap local tiers (NFS-style mounts), S3, custom backends, or the default
safe build — every block read goes block cache → miss →
`ReadHandle::read_exact_at` for exactly one framed block. That is why the
block cache fully fronts a remote tier: a cold block is one bounded range
GET, a warm one is free.

## Part lifecycle (`parts.rs`)

A **part** is one partition's set of bottom-level SSTable file pairs. Like
ClickHouse's parts, it is the unit of backup, retention and tiering:

- `DB::detach_part(cf, partition) → DetachedPart` — removes the part's
  tables from the catalog in one atomic manifest record, then moves the file
  pairs to `<cf-dir>/detached/<partition>`. **Not snapshot-consistent**: new
  reads stop seeing the range regardless of their snapshot seq. Iterators
  opened *before* the detach keep working — they pin the part's
  `Arc<SstHandle>` and loaded blocks (the same property compaction relies on
  when unlinking inputs under open iterators).
- `DB::attach_part(cf, dir)` — validates every `.klog` in `dir` (footer
  magic + CRCs via a reader open) and requires **same lineage**: a table's
  `max_seq` must not exceed the current visible sequence (foreign databases
  are rejected; cross-DB attach with seq remapping is future work). Files
  are copied in under fresh ids; a part whose range does not overlap a live
  bottom table slots into the bottom level only when it also avoids every table
  staged earlier in the same attach; overlapping input is routed to L0.
  All-or-nothing:
  any rejection cleans up the copies before anything is installed.
- `DB::freeze_part(cf, partition, dir)` — hard-links the part's files and
  writes a one-part manifest slice, producing a standalone, independently
  openable database directory; runs under `pause_deletions` (checkpoint's
  discipline) so compaction cannot unlink a file mid-freeze. The live part
  is untouched.

All three serialize against compaction by taking a **range lock** over the span
they touch (freeze uses the deletion pause instead) — `detach` and `relocate`
over their partition's span, `attach` over the whole keyspace since the incoming
extent is not known until the files are validated. Before 0.8.0 this was
`cf.compact_mu`; when compaction stopped taking that mutex, these operations had
to name a range or lose the guarantee entirely, the failure being a tier move
and a compaction rewriting the same bottom tables with one unlinking the other's
inputs. Every catalog change is one crash-atomic manifest
rewrite, so a crash can only leave orphan files, never route a reader to a
file that is not durably in place. Detach/attach/freeze move files with
`std::fs`, so they operate on **default-tier (local) parts**; move a part
back off a remote tier before detaching or freezing it.

## Part mover (`tier_rules` + `run_part_mover`)

`ColumnFamilyConfig::tier_rules` (`TierRule { prefix, tier, min_age }`) pin
partitions to tiers by longest-prefix, with an age gate: a part qualifies
once the newest entry across its tables (`SstMeta.max_entry_time` — stamped
"now" at flush/ingest, carried forward as the max over inputs by compaction)
is older than `min_age`. Unknown age (`None`, e.g. legacy manifests) is
conservatively ineligible.

One pass (`DbInner::run_part_mover`) snapshots each CF's bottom parts
(`bottom_parts` — one per distinct partition name; a part straddling tiers
mid-interrupted-move is skipped) and relocates each eligible, mis-placed
part via the crash-safe protocol of `relocate_part`:

```
copy every file pair to the target tier (StorageWriter::finish = durable)
→ one edit of UpdateTable{Tier,Object} per id, appended + fsynced
                              # the commit point: records tier=<t> for the ids
→ swap the in-memory handles (reads flip; in-flight reads finish on old handles)
→ delete the source files (remove_sst_file, defer-aware and paceable)
```

The flip is one record, so a partially applied move is not representable.

A crash before the flip leaves target-side copies the manifest does not
know about; after it, source-side leftovers. Both are cleaned by
`sweep_move_orphans` at the next `DB::open`: for every table id the manifest
knows, any copy sitting in a tier directory that disagrees with the
manifest's tier is deleted (unknown ids — in-flight flush output, WALs —
are untouched). The sweep walks directories with `std::fs`, so it covers
local tiers only; an S3 orphan survives as a storage leak (see
`docs/parts-and-tiers.md`).

The pass runs on the compaction worker every `Options::part_mover_interval`
(default 30 s; `Duration::ZERO` disables the cadence) and manually via
`DB::run_part_mover() → Result<usize>`. Moves are idempotent — a re-run on a
placed part is a no-op. Moving a part *back* to the default tier is out of
scope for the mover (a rule targeting `"ssd"` only stops future moves);
`DB::move_part_to_tier` is the manual per-part lever behind the same
protocol.

## S3 tier (`storage_s3.rs`, feature `s3`)

`S3Storage` implements `Storage` over an S3-compatible object store
(developed against MinIO). Shape:

- **Reads**: `supports_mmap()` is always false, so the reader takes the
  buffered path and each block-cache miss becomes exactly one HTTP range GET
  of that block's framed bytes (`S3ReadHandle::read_exact_at`); `size()` is
  one HEAD, cached. No read ever downloads a whole file. `S3Metrics`
  (range_gets / range_get_bytes / puts / heads, via `S3Storage::metrics()`)
  makes this observable and testable.
- **Writes**: a part file is produced whole (one compaction output or one
  mover copy, never appended), so `create` buffers in memory and
  `finish()` issues a single-shot PUT — matching S3's write-once object
  model. There is deliberately **no internal object CAS**: part objects use
  unique never-reused ids (one writer per key) and the commit point is the
  *local* manifest's fsync+rename, never an S3 object.
- **Runtime**: rust-s3 is async and ondaDB runs no async runtime, so the
  backend owns a small multi-thread tokio runtime and `block_on`s each
  request; engine worker threads call in synchronously (see
  `docs/concurrency-and-safety.md` § S3Storage).
- **Transport retry** (0.4.1): every request is wrapped in `with_retry` —
  up to 4 attempts, 25/50/100 ms backoff, retrying **only** transport-level
  `S3Error::Hyper`/`S3Error::Io`. This absorbs the hyper 0.14 keep-alive
  reuse race (hyperium/hyper#2136: the store or a NAT drops a pooled idle
  connection, the next request reusing that socket dies with
  `IncompleteMessage`; a bodied PUT is the most exposed because hyper never
  replays it). Retrying is sound here because every operation this backend
  issues is idempotent by construction — part objects use unique
  never-reused ids written whole in a single PUT (see "no internal object
  CAS" above), and GET/HEAD/COPY/DELETE/LIST are idempotent by nature. An
  HTTP status failure surfaces as `Ok` with a non-2xx `status_code()` and
  can never trigger a retry.

## Catalog persistence (`db.rs`, `manifest_edit.rs`)

Every structural change (flush, compaction, ingest, part move, CF lifecycle)
ends by making the catalog durable. Two shapes exist, chosen by one durable bit:

- **without `CAP_MANIFEST_EDITS`** (the default): `DbInner::persist_manifest`
  rebuilds the whole `Manifest` from the live CFs under `manifest_mu` and
  `Manifest::save`s it — the pre-2.2 behaviour, byte-for-byte. Cost is
  O(catalog) per change: 12.4 MiB re-encoded and fsynced per persist at 100k
  parts.
- **with the capability**: `DbInner::catalog_txn(edit, publish)` appends one
  CRC-framed record to `MANIFEST-EDITS` and fsyncs it — **the commit point** —
  and only then runs `publish`. `persist_manifest` becomes a snapshot
  compaction: the four-step protocol that writes a new `MANIFEST` and restarts
  the log empty (`docs/formats.md` § Snapshot compaction).

`catalog_txn`'s seven steps, in order: build the edit and the candidate state
without publishing → ensure every newly referenced file is already fsynced
(`Writer::finish`) → **under `manifest_mu`, append + fsync the record** →
publish the candidate → retire removed handles through
`DbInner::remove_sst_file` → still under `manifest_mu`, check the
snapshot-compaction trigger → on failure publish nothing, leave the old state
visible, and fail-stop.

This **inverts** the pre-2.2 order at every mutating site, which published
before persisting. Appends and snapshot compaction both hold `manifest_mu`, and
that is not optional: compaction renames a fresh log over the live one, so a
record appended between the snapshot write and the rename would be silently
lost.

`next_file_id` and `global_seq` ride no edit. Recovery reconciles both from what
the catalog actually references (rule r7), which is smaller *and* safe under
concurrency: transactions serialize on `manifest_mu` in an order the file-id
allocator does not, so a per-site `SetNextFileID` could land out of order and
fail its own monotonicity precondition on replay.

An `AddTable` at level 0 replays to the **front** of the CF's table list, which
is where `install_handles_l0` puts the handle: L0 is read newest-first and
`ColumnFamily::load` preserves the manifest's within-level order, so appending
would reopen a family with its newest L0 table treated as its oldest.

### The publication token

`publish` receives a `db::Publish`, a zero-sized token with a private field and
no constructor outside `db.rs`. Every function that installs catalog state takes
one by reference:

| Primitive | Where | Publishes |
| --- | --- | --- |
| `install_handles_l0` | `column_family.rs` | new L0 tables (flush, ingest, attach) |
| `publish_flush` | `column_family.rs` | an L0 table + retiring its sealed memtable |
| `update_levels` | `column_family.rs` | a whole level set (compaction install/rollback) |
| `install_levels` | `column_family.rs` | a pre-built level set (clone) |
| `remove_bottom_tables` | `column_family.rs` | detach |
| `remove_tables` | `column_family.rs` | delete-only excise (level-agnostic) |
| `insert_bottom_sorted` | `column_family.rs` | attach into the bottom level |
| `swap_bottom_tables` | `column_family.rs` | the part-mover flip |
| `remove_l0_tables` | `column_family.rs` | FIFO eviction |
| `append_partition_rule` / `remove_partition_rule` | `column_family.rs` | live partition rules |
| `register_cf` / `unregister_cf` | `db.rs` | the CF registries |
| `publish_instance_nonce` / `publish_wal_layout` | `db.rs` | the two scalar catalog fields |

`db.rs` mints a token in exactly two places: `catalog_txn`, after the fsync, and
`prepare_capability`, which publishes through a full snapshot rewrite because it
is the path that turns the edit log on. Publishing from `compaction.rs`,
`parts.rs`, `ingest.rs` or `maintenance.rs` outside a transaction therefore does
not compile.

`catalog_txn` publishes nothing for an edit with no ops (an empty flush): there
is no catalog change to make durable, so no id and no fsync are spent.

Compaction is the one site with an in-memory undo, and uses
`catalog_txn_with_rollback`. Its rollback runs in exactly one situation — the
pre-capability path published (it must: the snapshot is rebuilt from live state)
and then the snapshot write failed. With the log on, a failed append publishes
nothing; and once the edit is durable nothing is rolled back, because rolling
back committed state is how a catalog comes to name files a later step deletes.

`cf_lifecycle_mu` serializes the catalog-shape changes that validate before they
publish — CF create / create-many / drop / clear, and partition-rule add/remove.
Those held `cfs.write()` across validation and insert before 2.2; they cannot
now, because the insert happens inside `catalog_txn`, which takes `manifest_mu`
and then `cfs.read()`. Lock order: `cf_lifecycle_mu` → `manifest_mu` → `cfs` →
`cf.state`.

The capability is taken through `DB::enable_format_capabilities`, which persists
the bit *before* the first append (`enable_capability`'s persist-before-use
ordering), and that persist is the compaction that creates the log. A database
that never takes it never grows a log file.

**Migration status:** every catalog-mutating site now goes through
`catalog_txn`. Three `persist_manifest` callers remain, and all three are
deliberately snapshot writers, not mutations: `enable_capability` (the path that
creates the log), `snapshot_to` (checkpoint/backup force a snapshot on the
source, then write a snapshot-only destination with no log), and `close` (a
final snapshot compaction whose failure is the caller's — a silently dropped one
would make the next open replay more than it should).

## Recovery (`DB::open`)

0. Read-write opens only: `sweep_manifest_temp_files` unlinks any leftover
   `MANIFEST.tmp` / `MANIFEST-EDITS.tmp` before anything is loaded. They are
   crash artifacts of a snapshot compaction, never state, and are never read.
1. `recover_catalog` — `Manifest::load` (missing file = empty DB; CRC failure =
   hard error), then, if `CAP_MANIFEST_EDITS` is set, replay `MANIFEST-EDITS`
   into it: accept the log iff `header.base_applied_through <=
   snapshot.applied_through`, skip ids at or below `applied_through`, apply the
   rest with contiguous ids, and reconcile `next_file_id`/`global_seq`. See
   `docs/formats.md` § Edit-log tail tag for the full recovery rules. A writable
   open then reopens the log for append, truncating any torn tail away; a
   read-only open replays it and writes nothing. The
   persisted WAL layout must match the requested mode. An explicit
   per-CF→unified migration first recovers and flushes the legacy layout, then
   atomically flips the manifest; an implicit layout change is rejected.
2. Per CF: open every SST listed (levels rebuilt, level ≥1 sorted by min key),
   then replay every WAL generation found on disk (`existing_wal_gens` scans
   `wal-<gen>.log` stripe-0 names; `Wal::replay` reads all stripes of each
   generation). Replay is order-independent across stripes because sequence
   numbers define visibility; each frame (= one committed batch) applies
   atomically; a torn/corrupt tail cleanly ends that stripe.
3. `observe_seq` bumps `next_seq`/`visible` past the highest replayed seq.
   Control frames (3.2) contribute nothing to that mark: a prepared record
   carries the `seq = 0` sentinel, because it has not committed and may yet be
   aborted.
4. **Prepared-transaction recovery, two passes, order-free** (3.2). The unified
   WAL is four-striped in every mode but `SyncMode::Full`, and `prepare` and
   `commit_prepared` are separate API calls that commonly run on different
   threads, so a prepare frame and its decision have no recoverable relative
   order. **Pass 1** runs inside `UnifiedStore::open` alongside the replay above
   and only *collects* — every kind-16 prepare and every kind-17/18 decision,
   with the generation each landed in, and nothing inserted into the memtable.
   Two unresolved prepares sharing an id are `Corruption`. `UnifiedStore::open`
   returns them as its third element, and `build_db_inner` moves them into
   `DbInner::prepared`. **Pass 2** is `DbInner::resolve_recovered_prepares`,
   called immediately after step 3 — the first point at which `DbInner` exists
   and `observe_seq` is callable, and still before any worker is spawned. Per
   prepare: a commit decision raises the watermark to `commit_seq + count - 1`
   **first** and then applies the writeset at `commit_seq + slot` through
   `apply_memtable_only` (watermark before data, so a decision whose records
   fail to apply has still reserved its sequences); an abort decision drops it;
   no decision leaves it registered, its keys reserved and its generation
   pinned. A decision for an unknown id is a no-op — the pair was already
   retired, and the retirement ordering guarantees its records are durable in
   L0. Recovery invokes no application code and repeated opens are idempotent.
5. Fresh WAL generation opened; replayed WALs stay on disk until their
   memtable flushes (they are listed in `pending_wals`) — and, if a prepared
   transaction pins one, until the sweep releases it.
6. Read-write opens only: `sweep_move_orphans` deletes unreferenced default-tier
   SST output and local tier-move residue a crash left behind (see § Part
   mover), then the workers start — so no background move races the sweep.

## Maintenance (`maintenance.rs`)

`checkpoint` / `backup`: flush all CFs, persist the manifest, then — under
`DbInner::pause_deletions` so compaction cannot unlink anything — resolve
**exactly the files the freshly-loaded manifest references** through their
storage tiers and durably materialize them into the target's default tier. The
target manifest clears tier/object metadata and is self-contained.
`clone_column_family` applies the same rule to one CF under fresh file ids,
also under a deletion pause.

## Background IO limiter (`ioctrl.rs`)

Compaction debt already paces *writers* by how much work is owed; this bounds
how fast background work is allowed to consume the device. Two independent
pieces:

**The class** is a thread-local `IoClass`, `const`-initialized so a foreground
read pays one TLS load. Workers set it once at spawn (`onda-flush` → `Flush`,
`onda-compact-{n}` → `Compaction`, inherited by the part mover). The three paths
that run background-sized IO on the caller's thread — `compaction::run_manual`
(a whole-level sweep, the largest burst the engine produces), `DB::flush_memtable`,
and `Ingestion::{add, finish}` — install it with an `ioctrl::scoped` guard that
restores in LIFO order, including while unwinding. A spawn-time default alone
would leave all three labelled `Foreground` and unpaced.

**The limiter** is DB-scoped, not thread-local: `Option<Arc<dyn IoLimiter>>` on
`DbInner` and `CfCtx`, carried into every `Reader` (`open_with_limiter`) and
`Writer` (`with_limiter`). Two databases in one process therefore pace
independently, and the default — `Options::background_io_bytes_per_second == 0`
— builds no limiter at all, leaving one nil check per charge point.

`TokenBucket` is work-conserving: a charge spends whatever tokens are present
and waits only for the remainder, so a value larger than the whole burst still
completes rather than deadlocking, and the device is never left idle waiting for
a large charge to be payable in one piece. `background_io_burst_bytes` of 0
derives one second of rate. Charges are split into 1 MiB chunks.

Charge points are the paths that actually issue device IO: a block-cache miss
or the first mmap touch of a block (`Reader::read_data_block`,
`read_data_block_local`), `read_vlog_into`, and the writer's `flush_block`,
`write_meta_block` and `write_vlog` — always *before* the IO is issued, so a
cancelled job never consumes bandwidth it queued for. Cache hits are free
because they cost no device IO. **The WAL is never charged**: it is foreground
durability, not background bandwidth. `IoClass::Foreground` never waits.

Close and fail-stop (`DbInner::fail_stop`, which every production `poison.set`
now routes through) call `cancel_background_io`, which wakes every waiter and
makes all later charges free. `close` does this *first*, before the flush-drain
spin-wait, so shutdown durability runs at full speed. The blocking contract and
the compaction range-lock exception are in `docs/concurrency-and-safety.md`.

### Paced obsolete-file deletion (0.6-B)

`DbInner::remove_sst_file(path, bytes)` is the single retirement point for an
obsolete SSTable file (invariant 6). `bytes` is the file's size from the
`SstMeta` every caller already holds — `klog_size` for the klog, `vlog_size` for
the vlog — floored at `DELETE_METADATA_BYTES` (4096, one filesystem block),
because an unlink costs metadata IO even for a file of zero bytes and a CF with
no separated values retires an empty `<id>.vlog` at every compaction.

`FileDeletionState` owns two things: the pause counter plus its pending list
(unchanged — a pause still defers every unlink, which is what makes
checkpoint/backup self-consistent), and, when
`Options::obsolete_delete_bytes_per_second` is non-zero, a `DeletionWorker`: an
unbounded crossbeam channel, one `onda-delete` thread, and its own `TokenBucket`
at the deletion rate (an injected `Options::io_limiter` overrides it — one
supplied limiter describes one device budget). The worker sets
`IoClass::ObsoleteDelete` once at spawn and charges each file before unlinking
it, in FIFO order. Deletion *order* is never load-bearing: file ids are never
reused, so no task depends on an earlier one.

At the default rate of 0 there is no channel, no thread and no queue: the
caller unlinks inline, exactly as every release before 0.6. Resuming from a
pause hands the deferred tasks to the worker if there is one, and unlinks them
inline otherwise.

`close` cancels the pacing (via `cancel_background_io`, first thing) and then,
after the flush and compaction workers are joined, calls `drain_deletions`:
drop the sender, join the worker: the `for task in rx` loop only ends once the
queue is empty, so every queued file is unlinked **before** the `LOCK` file is
released. After that the sender is `None` and any late retirement unlinks
inline. `fail_stop` cancels the pacing too, so a poisoned database never leaves
the worker parked on credit it will never be granted.
