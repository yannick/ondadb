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
| `memtable.rs` | Sharded (256) MVCC write buffer; `put_batch` shard-grouped inserts; `snapshot()`/`MemIterator`; `FlushMerge` (fastpath) |
| `memtable_arena.rs` | *(unsafe-fastpath only)* arena skip-list shard: single-allocation nodes with inline key prefix + seq; `ShardCursor` for zero-copy flush |
| `wal.rs` | Striped write-ahead log: batch frames, group commit (Full mode), replay |
| `sst/` | SSTable `writer.rs` (klog/vlog/bloom/index/footer), `reader.rs` (point get, block reads, CRC-once bitmap, mmap fastpath), `iter.rs` (bidirectional iterator, cached key prefix), `mod.rs` (formats, `Block`) |
| `iterator.rs` | `ChildIter` enum (Mem/Sst), heap `MergingIter`, public `Iterator` with MVCC collapse and pinned-block borrowed keys/values |
| `compaction.rs` | Leveled compaction: pick level, k-way merge, version collapse, tombstone/TTL GC, compaction filters; FIFO style (oldest-table eviction) |
| `ingest.rs` | Bulk ingestion: pre-sorted stream → L0 SSTables directly (no WAL/memtable); atomic install at `finish()` |
| `manifest.rs` | Durable catalog (`MANIFEST`): next file id, global seq, per-CF config blob + SST set; crash-atomic save |
| `unified.rs` | Optional shared memtable+WAL across CFs (8-byte CF-id key prefix); split flush |
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
  unified-wal-<gen>.log[.sN]  # unified-memtable mode only
```

One WAL *generation* corresponds to one memtable lifetime; rotation bumps the
generation. SST file ids come from the manifest's `next_file_id` counter and
are unique db-wide.

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
an 8-byte big-endian CF-id key prefix, one shared WAL + memtable).

## Read path

Point get (`ColumnFamily::get`): consult, newest-wins by seq —
unified store (if enabled) → active memtable → immutable memtables (newest
first) → L0 tables whose [min,max] covers the key (all of them; L0 overlaps) →
one binary-searched table per level ≥ 1. SSTable get: bloom filter →
`find_block` binary search on the in-memory index → linear entry scan inside
the 4 KiB block → inline value or vlog read (CRC-verified).

Iterator (`ColumnFamily::new_iterator`): builds `ChildIter`s over the txn
overlay (optional), unified slice (optional), the active memtable, the imms, and
every SST, then heap-merges in internal order collapsing MVCC versions
(`Iterator::advance_forward/backward`). Keys and values are returned as
borrowed slices from per-child pinned blocks where possible; see
`docs/concurrency-and-safety.md` § Pinned blocks.

### Lazy memtable iterator (`LazyMemIter`, default build)

The memtable `ChildIter::Mem` is **lazy**. It does *not* materialize the
memtable. `LazyMemIter` (`memtable.rs`) runs a bidirectional k-way merge
(`MemMerge`) directly over the 256 shard skip lists — one persistent
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
default build stays `#![forbid(unsafe_code)]`.

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
- `ColumnFamily::flush_imm` → under fastpath `write_l0_streaming`: a 256-way
  `FlushMerge` over borrowing `ShardCursor`s feeds `Writer::add` directly —
  no `Vec<Entry>` materialization, no sort. Safe build: `snapshot()` + sort.
- `Writer::finish` fsyncs klog+vlog and the CF directory.
- `persist_manifest()`; **only on `Ok`** delete the imm's WAL files
  (`wal::remove_wal_files` unlinks all stripes).
- If L0 count ≥ `l1_file_count_trigger`, enqueue compaction.

## Compaction (`compaction.rs`)

Classic leveled: L0→L1 on file count, Li→Li+1 when level bytes exceed
`write_buffer_size * level_size_ratio^(i-1)`. Merge-iterates inputs plus
overlapping next-level tables; keeps the newest version per key plus every
version newer than `DbInner::oldest_snapshot()`; drops tombstones and expired
TTL entries only at the bottom level. Output SSTs are split at
`write_buffer_size`. Ordering: new levels installed → `persist_manifest()?` →
inputs deleted via `DbInner::remove_sst_file` (defer-aware).

## Recovery (`DB::open`)

1. `Manifest::load` — missing file = empty DB; CRC failure = hard error.
2. Per CF: open every SST listed (levels rebuilt, level ≥1 sorted by min key),
   then replay every WAL generation found on disk (`existing_wal_gens` scans
   `wal-<gen>.log` stripe-0 names; `Wal::replay` reads all stripes of each
   generation). Replay is order-independent across stripes because sequence
   numbers define visibility; each frame (= one committed batch) applies
   atomically; a torn/corrupt tail cleanly ends that stripe.
3. `observe_seq` bumps `next_seq`/`visible` past the highest replayed seq.
4. Fresh WAL generation opened; replayed WALs stay on disk until their
   memtable flushes (they are listed in `pending_wals`).

## Maintenance (`maintenance.rs`)

`checkpoint` (hard links) / `backup` (copies): flush all CFs, persist the
manifest, then — under `DbInner::pause_deletions` so compaction cannot unlink
anything — link/copy **exactly the files the freshly-loaded manifest
references** and write that same manifest into the target. `clone_column_family`
hard-links a CF's SSTs under new file ids, also under a deletion pause.
