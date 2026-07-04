# Concurrency & safety

The default build is `#![forbid(unsafe_code)]`. The `unsafe-fastpath` feature
lifts that for exactly two modules — `memtable_arena.rs` and the mmap paths in
`sst/reader.rs` / `sst/mod.rs` — whose contracts are spelled out below. Adding
`unsafe` anywhere else needs a documented contract here and a strong measured
justification.

## Fail-stop poisoning (`util::Poison`)

After a failed fsync the kernel may have dropped the dirty pages it could not
persist, so retrying can silently lose already-acknowledged data. Any
durability failure — WAL fsync (group commit, interval thread, manual
`sync_wal`), a background flush, or a manifest persist — trips one DB-wide
flag. From then on every `Txn::commit` / `apply_commit` fails with
`OndaError::Poisoned` (reads keep working); `DB::poisoned()` reports the first
failure's reason. The only recovery is reopening the database. Exception: a
flush that fails because its CF was dropped/cleared mid-flight does not poison
(the failure is expected — its directory is gone).

## MVCC

- `DbInner::next_seq` (AtomicU64): commit reserves `[start, start+n)` via
  `reserve_seq`.
- `DbInner::visible` + `PublishState { cursor, completed }`: `publish_range`
  records completed ranges and advances `visible` **gap-free** — a commit's
  records become readable only when every earlier sequence has also completed.
  Readers snapshot `visible_seq()`; nothing ever reads at `next_seq`.
- Snapshots: `snapshots: Mutex<BTreeMap<seq, refcount>>`;
  Repeatable-Read/Snapshot/Serializable txns pin their `read_seq` via
  `acquire_snapshot`/`release_snapshot`. Compaction's version GC keeps every
  version newer than `oldest_snapshot()`.
- Isolation (`txn.rs`): ReadUncommitted/ReadCommitted read live `visible_seq`;
  the pinned levels read their snapshot. Snapshot+Serializable serialize
  commit-time validation under `commit_mu` and abort with `Conflict` on
  write-write conflicts (first-committer-wins). **Serializable validates point
  reads only** (`read_set`/`read_cfs`) — range scans are not tracked; phantoms
  are possible and this is documented API behavior, not a bug to "fix"
  silently.

## Lock inventory (order within = acquisition order; never invert)

| Lock | Guards | Held across |
|---|---|---|
| `DbInner::commit_mu` | Snapshot/Serializable validation + apply | conflict check → apply → publish |
| `DbInner::manifest_mu` | manifest rebuild + save | whole `persist_manifest` |
| `DbInner::publish` (Mutex) | publish cursor | short |
| `DbInner::file_deletion` | deferred-SST-delete state | short; `pause_deletions` returns an RAII guard |
| `ColumnFamily::rot` (Mutex+Condvar) | `active_writers`, `rotating` | gate checks, rotation drain |
| `ColumnFamily::state` (RwLock) | memtable/WAL handles, imm queue, levels | read: clone handles; write: swap/install — keep short |
| `Wal::qstate` / per-stripe file mutexes | group-commit queue / file appends | one frame write |
| `ArenaShard::arena` (Mutex) | skip-list structure per shard | one batch group's inserts |
| `commit_hook` (Mutex) | hook fn | hook invocation |
| `<dir>/LOCK` (OS advisory file lock) | whole DB directory against other processes/handles | entire open→close lifetime; exclusive for read-write, shared for read-only opens; second open fails with `OndaError::Locked` |

Safe patterns used: `create/drop_column_family` release the `cfs` write lock
before `persist_manifest`; rotation drops `rot` while opening the next WAL
file; commit runs hooks after dropping `commit_mu`.

## Rotation protocol (`ColumnFamily::rotate_memtable`)

Writers: under `rot`, wait while `rotating || imm.len() >= stall_threshold`,
then `active_writers += 1`; clone `(wal, mem)` under `state.read()`; do WAL +
memtable work with **no CF locks held**; then `active_writers -= 1` + notify.

Rotator: if a rotation is already in flight, size-triggered callers return
(only `force` callers wait). The winner sets `rotating`, **opens the next WAL
generation with `rot` released** (the syscall overlaps the writer drain), then
waits `active_writers == 0`, swaps memtable+WAL under `state.write()`, closes
the old WAL, clears `rotating`, notifies, and enqueues the flush job.

Consequences agents rely on:
- An imm memtable has no writers, ever. `FlushMerge`/`ShardCursor` and
  `Memtable::snapshot` assume this.
- A record's WAL write and memtable insert happen under one `active_writers`
  span, so rotation can never split a batch across memtables (its WAL
  generation always covers its memtable).

## Memtable

256 shards (`NUM_SHARDS`), routed by `xxh3(user_key)`. Default build: one
`crossbeam_skiplist::SkipMap<IKey, Val>` per shard (lock-free); `IKey` avoids
a comparator `Arc` clone and virtual calls for the byte-wise default.
`put_batch` counting-sorts a committed batch into per-shard runs and updates
the shared `approx_size`/`num_entries`/`max_seq` atomics **once per batch**.

### `ArenaShard` (unsafe-fastpath) — the unsafe contract

- Nodes live in chunked arenas (`Box<[MaybeUninit<Node>; CHUNK]>`) that never
  move or free individual nodes; all nodes drop with the shard.
- **Single writer per shard**: the `arena` Mutex serializes structural
  changes; `put_group` inserts a whole per-shard run under one acquisition,
  with nodes fully constructed *before* the lock.
- Publication: a node is fully initialized, then linked bottom-up with
  `Release` stores; readers traverse with `Acquire` loads — they can never see
  a partially built node. Following any loaded pointer is sound because nodes
  are never freed while the shard lives.
- `Node` packs `user_key || !seq || value` in ONE allocation (`data`,
  split at `klen`), plus an inline `kprefix: u64` (zero-padded big-endian
  first 8 key bytes) and `nseq: u64` so most probes never touch `data`'s
  cache line. `MAX_HEIGHT = 8` (shards are bounded by
  `write_buffer_size / 256`).
- `ShardCursor` hands out `&'a [u8]` borrows of node data tied to the shard
  borrow — only valid because flush runs on sealed memtables (no writer) and
  nodes are immortal until drop.

### The 8-byte prefix-compare trick (used in 4 places)

`key_prefix8(k)` = first `min(8, len)` key bytes, zero-padded, as a big-endian
u64. If two prefixes differ, their comparison equals the byte-wise key
comparison; if equal, you MUST fall through to the full comparison (zero
padding makes short keys prefix-equal to their extensions). **Only valid when
`Comparator::is_bytewise()`** — every use is gated: `ArenaShard::cmp_node`,
`MergingIter::before`, `Iterator::top_in_group`, `FlushMerge::before`.

## Merge iterator pinning (`iterator.rs`)

`Iterator::key()/value()` return borrowed slices. Sources:
- Memtable entries: copied into the reused `key`/`val` buffers (`Buffered`).
- Inline SSTable entries: borrowed from a pinned `Block` held in
  `pinned_key[child]` / `pinned_val[child]`.

Rules (each encodes a bug we actually hit):
1. **Separate key and value pin arrays.** The group key is captured from the
   newest entry; the *visible* value may come from an older version in a later
   block of the same child. One shared slot would evict the key's block.
2. **Per-child slots, refreshed only on block transition**
   (`Block::same_backing`: `Arc::ptr_eq`, plus the window offset for mmaps).
   Cloning the shared mmap `Arc` per *entry* caused cross-thread refcount
   contention and a measured 3× scan regression — never reintroduce per-entry
   pin churn.
3. Pins are only mutated inside `capture_group_key`/`capture_value`, which run
   during `advance_*`; between advances the returned slices are stable.

## SSTable reader (unsafe-fastpath mmap contract)

`Reader::open` mmaps the klog (and lazily the vlog). Sound because a finished
SSTable is immutable: ondaDB never writes to it after `Writer::finish`, and
compaction/deferred deletion only *unlink* it — pages stay valid while the
mmap holds the inode. `Block::Mapped` views carry the `Arc<Mmap>` so they
outlive the reader if needed.

CRC-once bitmap: `Reader::verified` (one bit per data block, AtomicU64 words).
First reader of a block verifies its CRC (`block_payload`), sets the bit with
`AcqRel`; later readers use `block_payload_preverified`. Immutability of the
file makes this sound; the bit is only set after a successful verify.

## WAL concurrency

Non-Full modes: each committing thread encodes its frame locally and appends
it to its sticky stripe under that stripe's file mutex — no cross-thread
coordination. Full mode: single stripe + group commit (leader drains
`qstate.queue`, one write + one `sync_data`, wakes followers over bounded
channels). `Wal::close` is idempotent and `&self` (callable through `Arc`).

## Background workers

`spawn_workers`: `num_flush_threads` flush workers + 1 compaction worker, fed
by unbounded crossbeam channels, polling with 50 ms tick to observe `stop`.
`DB::close`: set `closing` → rotate every CF (+unified) with `force` → spin
until `pending_flush == 0` → set `stop`, join workers → final
`persist_manifest` → close WALs/readers. `DB::drop` closes only when it holds
the last `Arc` (workers hold no `DB` clone).
