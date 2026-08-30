# Architecture

ondaDB is a leveled-compaction LSM engine with WiscKey value separation,
per-column-family storage, MVCC transactions, and a striped, batch-framed WAL.
This file maps the modules and traces every major data path. Anchors are
type/function names — grep for them; line numbers rot.

## Module map

| Module | Role |
|---|---|
| `db.rs` | `DB`/`DbInner`: CF registry, global sequence + publish machinery, snapshot refcounts, background flush/compaction workers, recovery, manifest persistence, deferred SST deletion, `LOCK` file, fail-stop poisoning |
| `column_family.rs` | `ColumnFamily`: per-CF memtable + WAL + LSM levels; commit application, memtable rotation, flush to L0, point reads, iterator construction |
| `txn.rs` | `Txn`: arena-buffered writes, five isolation levels, conflict detection, savepoints; also the `DB::put/get/delete` single-op helpers |
| `memtable.rs` | Sharded (16) MVCC write buffer; `put_batch` shard-grouped inserts; `snapshot()`/`MemIterator`; `FlushMerge` (fastpath) |
| `memtable_arena.rs` | *(unsafe-fastpath only)* arena skip-list shard: single-allocation nodes with inline key prefix + seq; `ShardCursor` for zero-copy flush |
| `wal.rs` | Striped write-ahead log: batch frames, group commit (Full mode), replay |
| `sst/` | SSTable `writer.rs` (klog/vlog/bloom/index/footer), `reader.rs` (point get, block reads, CRC-once bitmap, mmap fastpath), `iter.rs` (bidirectional iterator, cached key prefix), `mod.rs` (formats, `Block`) |
| `table_cache.rs` | `TableCache`: sharded (CLOCK) LRU of open SSTable readers, bounding resident index+bloom memory by reader count (`max_open_readers`) and byte budget (`max_open_reader_bytes`); the `max_open_files` equivalent |
| `iterator.rs` | `ChildIter` enum (Mem/Sst), heap `MergingIter`, public `Iterator` with MVCC collapse and pinned-block borrowed keys/values |
| `tailing.rs` | `TailingIterator`: forward-only keyspace tail that refreshes past its own end (not a change feed) |
| `compaction.rs` | Leveled compaction: pick level, k-way merge, version collapse, tombstone/TTL GC, compaction filters; bottom-level output cut at partition boundaries; FIFO style (oldest-table eviction) |
| `ingest.rs` | Bulk ingestion: pre-sorted stream → L0 SSTables directly (no WAL/memtable); atomic install at `finish()` |
| `manifest.rs` | Durable catalog (`MANIFEST`): next file id, global seq, per-CF config blob + SST set (incl. per-table partition/tier/max-entry-time via the append-tolerant tail); crash-atomic save |
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
3. Snapshot/Serializable only: take `DbInner::commit_mu`, run write-write
   conflict check via `ColumnFamily::peek_seq`; Serializable additionally
   validates the point-read set (`read_cfs`).
4. Reserve a contiguous seq block: `DbInner::reserve_seq(n)`.
5. Build per-CF `Vec<RecordRef>` **borrowing the txn arena** (`CfGroup`);
   `CommitOp` hook payloads are built only if `cf.has_commit_hook()`.
6. `ColumnFamily::apply_commit(&recs)`:
   - gate: wait while rotating or imm-queue ≥ `l0_queue_stall_threshold`;
     increment `active_writers`
   - `Wal::append_batch(&recs)` — encodes ONE frame in this thread, writes it
     to this thread's WAL stripe (see `docs/formats.md`)
   - `Memtable::put_batch(&recs)` — counting-sorts into per-shard runs, one
     shard lock per batch, nodes prebuilt outside locks, counters updated once
   - decrement `active_writers`; if memtable ≥ `write_buffer_size`, call
     `rotate_memtable(false)`
7. `DbInner::publish_range(start, end)` — advances `visible` gap-free.
8. Run commit hooks (outside `commit_mu`).

Unified-memtable mode replaces step 6 with `UnifiedStore::apply` (records get
an 8-byte big-endian CF-id key prefix, one shared WAL + memtable). Its sealed
queue is bounded by `unified_memtable_stall_threshold` (default 6).

In per-CF WAL mode, commit rejects a transaction touching more than one column
family: independent WAL frames cannot provide crash or partial-I/O atomicity.
Unified mode encodes every touched CF in one shared WAL frame and is the
supported atomic cross-CF layout.

## Read path

Point get (`ColumnFamily::get`): consult, newest-wins by seq —
unified store (if enabled) → active memtable → immutable memtables (newest
first) → L0 tables whose [min,max] covers the key (all of them; L0 overlaps) →
one binary-searched table per level ≥ 1. SSTable get: bloom filter →
`find_block` binary search on the in-memory index → linear entry scan inside
the data block → inline value or vlog read (CRC-verified).

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
- `Writer::finish` fsyncs klog+vlog and the CF directory.
- `persist_manifest()`; **only on `Ok`** delete the imm's WAL files
  (`wal::remove_wal_files` unlinks all stripes).
- If L0 count ≥ `l1_file_count_trigger`, enqueue compaction.

## Compaction (`compaction.rs`)

Classic leveled: L0→L1 on file count, Li→Li+1 when level bytes exceed
`l1_base_bytes * level_size_ratio^(i-1)`. Merge-iterates inputs plus
overlapping next-level tables; keeps the newest version per key plus every
version newer than `DbInner::oldest_snapshot()`; drops tombstones and expired
TTL entries only at the bottom level. Output SSTs are split at
`target_file_size` and, at the bottom level, additionally **cut at partition
boundaries** (see § Partitions). Every output carries
`max_entry_time = max` over its inputs' stamps, so re-compacting cold data
does not reset its age for the part mover. Ordering: new levels installed →
`persist_manifest()?` → inputs deleted via `DbInner::remove_sst_file`
(defer-aware). Input deletion resolves **default-tier paths only** — a
compacted input that lived on a named tier is not unlinked there (a storage
leak, never a correctness issue; see `docs/parts-and-tiers.md` § Known gaps).

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
→ swap the in-memory handles (reads flip; in-flight reads finish on old handles)
→ persist_manifest()          # the commit point: records tier=<t> for the ids
→ delete the source files (remove_sst_file, defer-aware)
```

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

## Recovery (`DB::open`)

1. `Manifest::load` — missing file = empty DB; CRC failure = hard error. The
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
4. Fresh WAL generation opened; replayed WALs stay on disk until their
   memtable flushes (they are listed in `pending_wals`).
5. Read-write opens only: `sweep_move_orphans` deletes unreferenced default-tier
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

Not yet done (review B of this feature): pacing obsolete-SSTable deletion.
`IoClass::ObsoleteDelete` exists and is unused; `remove_sst_file` still unlinks
inline.
