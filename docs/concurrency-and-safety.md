# Concurrency & safety

The default build is `#![deny(unsafe_code)]`, with one localized audited
exception for Linux `clock_gettime(CLOCK_REALTIME_COARSE)` in `util.rs`. The
`unsafe-fastpath` feature additionally permits exactly two implementation areas
— `memtable_arena.rs` and the mmap paths in `sst/reader.rs` / `sst/mod.rs` —
whose contracts are spelled out below. Adding unsafe anywhere else needs a
documented contract here and a strong measured justification.

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
- **Read-your-own-writes floor** (`de50da9`): `visible_seq` advances gap-free,
  so while another thread's earlier-reserved commit is in flight, a thread's
  own completed commit sits *above* the watermark and a ReadCommitted `get()`
  right after `put()` returned the PREVIOUS value. `THREAD_COMMIT_FLOOR` (a
  thread-local keyed by a process-monotonic `DbInner::instance_id`) gives ReadCommitted point reads and
  iterators `max(visible_seq, own_floor)`. Fixed-snapshot levels keep the
  gap-free watermark on purpose — the floor may sit inside a publication gap,
  which is acceptable for read-committed but not for repeatable reads.

### Known defect: the floor does not fully hold under `unsafe-fastpath`

`tests/read_your_writes.rs::get_sees_own_put_under_concurrent_writes` fails
intermittently, **but only with `--features unsafe-fastpath`**. The failure is
not the lost-write assertion the test was written for: it panics on the `Err`
arm with **`get failed: NotFound`** — a `get` returns NotFound for a key the
same thread just successfully `put`.

Measured with 8 concurrent copies of the test binary: **2/48 on v0.6.0, 1/48 on
v0.5.0**. Never reproduced running the test alone (0/8 isolated, 0/24 under
synthetic CPU load) — it needs real multi-process contention. Failures land
within 0.06–0.20 s, i.e. in the first handful of the 50,000 iterations, which
points at an early/rotation window rather than slow drift.

**Pre-existing, not a 0.6.0 regression** — v0.5.0 reproduces it. Recorded here
rather than silently carried: the default (safe) build is unaffected, and no
consumer that builds with default features is exposed.

**Narrowed 2026-07-28 by two measured experiments** (the diagnostics were
throwaway; the reproduction lives in `tests/read_your_writes.rs`):

1. **It is transient, not a lost write.** Re-reading the key immediately after
   the failure returns the correct value, and so does a read after a
   `yield_now`. So nothing is dropped or overwritten — a read momentarily fails
   to see data that is present the whole time. This is a read-visibility
   defect, not a durability one.
2. **Rotation, flush, compaction and the SSTable path are all excluded.** With
   `write_buffer_size` raised to 2 GiB, so the memtable can never rotate and no
   flush or L0 install can occur, the failure still reproduces. It is therefore
   inside the arena memtable itself — `ArenaShard::get` / `find_ge` /
   `descend` / `insert_node` — and not in any interaction with the rest of the
   engine. The unified store is likewise not involved: `unified_memtable`
   defaults to `false`.

What has been checked and found sound: the arena is chunked with boxed
fixed-size chunks, so node pointers never move; inserts are serialized by the
arena lock; the link loop publishes each level with `Release` against the
reader's `Acquire`; `ColumnFamily::get` takes one consistent
`(mem, imms, tables)` snapshot under the state read lock; rotation swaps the
memtable and pushes the imm under a single `state.write()`; and both flush
paths install the SSTable *before* removing the immutable.

Still unexplained, and the place to look next: `self.height` is stored and
loaded `Relaxed` and is raised *before* the new node's levels are linked. Each
individual interleaving traced by hand so far comes out correct, so either the
argument has a hole or the race is elsewhere in the traversal — `cmp_node`'s
8-byte prefix shortcut and the shard selection used by `put` versus `get` are
the two unexamined candidates.

## Lock inventory (order within = acquisition order; never invert)

| Lock | Guards | Held across |
|---|---|---|
| `DbInner::commit_mu` | Snapshot/Serializable validation + apply | conflict check → apply → publish |
| `DbInner::manifest_mu` | manifest rebuild + save | whole `persist_manifest` |
| `DbInner::publish` (Mutex) | publish cursor | short |
| `DbInner::file_deletion.paused` | pause counter + deferred-delete list | short; `pause_deletions` returns an RAII guard. Never held across the unlink or the channel send |
| `DbInner::file_deletion.worker.{tx,handle}` | deletion-queue sender / join handle | one unbounded `send` (never blocks) or one `take`; the join in `drain_deletions` happens with neither held |
| `ColumnFamily::rot` (Mutex+Condvar) | `active_writers`, `rotating` | gate checks, rotation drain |
| `ColumnFamily::state` (RwLock) | memtable/WAL handles, imm queue, levels | read: clone handles; write: swap/install — keep short |
| `ColumnFamily::compact_mu` (Mutex) | whole-CF compaction operations | manual compaction sweep and FIFO eviction; acquired before the whole-keyspace range lock |
| `ColumnFamily::range_locks` | key ranges being rewritten | bounded compaction jobs use non-blocking acquisition; attach takes the whole keyspace; detach and part moves block on the affected partition span, including copy + manifest flip |
| `ColumnFamily::live_partition_rules` (RwLock) | the live partition-rule set | `append_partition_rule` validates + appends under one write acquisition (concurrent duplicate adds: exactly one wins); released **before** `persist_manifest`, which re-reads the rules via `effective_config` |
| `Wal::qstate` / per-stripe file mutexes | group-commit queue / file appends | one frame write |
| `ArenaShard::arena` (Mutex) | skip-list structure per shard | one batch group's inserts |
| `commit_hook` (Mutex) | hook fn | hook invocation |
| `<dir>/LOCK` (OS advisory file lock) | whole DB directory against other processes/handles | entire open→close lifetime; exclusive for read-write, shared for read-only opens; second open fails with `OndaError::Locked` |

Safe patterns used: `create/drop_column_family` release the `cfs` write lock
before `persist_manifest`; rotation drops `rot` while opening the next WAL
file; commit runs hooks after dropping `commit_mu`.

### Background-wait rule and its one exception (0.6)

`ioctrl::charge` can **block** a background thread waiting for IO credit (see
"Background IO limiter" below). No background wait may hold `commit_mu`,
`rot`/`state`, or a WAL file mutex — those are on the foreground write path, and
parking a compaction under one would convert a bandwidth limit into a write
stall.

**Explicit exception: a compaction job's own range lock.** `lock_job` returns a
`RangeGuard` held for the whole job (`compact_inputs`: "the caller owns input
selection *and* the range lock covering every input"), so every `charge()` in a
compaction read or write blocks with that lock held. This is deliberate and
safe: the range lock *is* that job's unit of exclusion — it exists to keep other
compaction jobs off the same span, and **no foreground read or write path
acquires it**. Waiting under it delays only work that was already excluded.
`run_manual` and `run_fifo` additionally hold `cf.compact_mu` and a
whole-keyspace range lock for the same reason and with the same justification.
Every other lock in the inventory above stays off-limits to a background wait.

### Background IO limiter (`ioctrl.rs`, 0.6)

`IoClass` lives in a `const`-initialized thread-local `Cell`, so the foreground
fast path is one TLS load. Workers set their class once at spawn
(`onda-flush` → `Flush`, `onda-compact-{n}` → `Compaction`; the part mover
shares the compaction worker and correctly inherits `Compaction`). The three
paths that run background-sized IO on the *caller's* thread — `run_manual`,
`DB::flush_memtable`, and `Ingestion::{add, finish}` — install the class with an
`ioctrl::scoped` guard, which restores in LIFO order and restores while
unwinding.

The limiter object is **not** thread-local: it is DB-scoped, carried by
`DbInner`, `CfCtx`, every `Reader` and every `Writer`, so two databases in one
process pace independently and the default (`None`) is one nil check.

Blocking points, all of them `TokenBucket::charge`:

| Site | Charged |
|---|---|
| `Reader::read_data_block` / `read_data_block_local` | framed block length, on a block-cache miss or the first mmap touch of a block (the `verified` bit); a cache hit and a re-read cost nothing |
| `Reader::read_vlog_into` | frame length (value + v2 header), at the funnel both the mmap and buffered paths pass through |
| `Writer::flush_block`, `write_meta_block` | framed block length, before the write is issued |
| `Writer::write_vlog` | frame length, before the write is issued |

`IoClass::Foreground` never waits — `charge` returns immediately for it — and
the WAL is never charged at all: it is foreground durability, not background
bandwidth. Charges are split into `MAX_CHARGE_CHUNK` (1 MiB) pieces so a value
larger than the bucket capacity still completes and cancellation stays
observable.

**Both terminal transitions wake every waiter.** `DbInner::fail_stop` (which all
production `poison.set` calls now route through) and `DB::close` call
`cancel_background_io`, which sets the bucket's `cancelled` flag and wakes its
condvar; every subsequent charge is free. `close` cancels **first**, before the
`pending_flush` spin-wait, because that wait would otherwise last as long as the
configured rate says the final flush's bytes take — and after close is called
there is no foreground latency left to protect. `SystemClock::wait` additionally
caps a single park at 50 ms, so even a lost wakeup cannot hang a waiter.

### Deletion worker (`db.rs`, 0.6-B)

A fourth blocking point: the `onda-delete` thread charges
`max(file size, DELETE_METADATA_BYTES)` under `IoClass::ObsoleteDelete` before
each unlink. It exists only when
`Options::obsolete_delete_bytes_per_second` is non-zero; at the default of 0
`remove_sst_file` unlinks on the caller's thread with no channel and no thread,
exactly as before 0.6.

The worker **holds no engine lock while it waits**, which is why it needs no
exception to the background-wait rule: it is handed a path and a byte count and
never touches CF state, the manifest, or the levels. `remove_sst_file` releases
the `paused` mutex before dispatching, so a paced queue can never delay a
compaction that is retiring files, nor a `pause_deletions` taken concurrently.

Ordering obligations:

- **A pause still wins.** While `paused.disabled > 0` a retirement goes to the
  pending list and is *not* queued; the last guard drop dispatches it. Backup
  and checkpoint correctness rests on this and is unchanged by pacing.
- **`close` drains before releasing `LOCK`.** `cancel_background_io` (first
  thing in `close`) frees the worker's bucket, then — after the flush and
  compaction workers are joined, so nothing can enqueue more —
  `drain_deletions` drops the sender and joins the thread. The worker's
  `for task in rx` loop ends only when the queue is empty, so every queued file
  is gone before the directory lock is. Late retirements after that point
  unlink inline; nothing is dropped.
- **Poison does not hang it.** `fail_stop` routes through
  `cancel_background_io`, so a worker parked on credit a dying database will
  never grant is released immediately.

FIFO is the queue's discipline but not a correctness requirement: file ids are
never reused, so no retirement can depend on an earlier one having run.

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

Unified rotation uses the same `rot → state` lock order. Writers additionally
wait while the sealed unified queue is at
`Options::unified_memtable_stall_threshold`; `remove_imm` takes `rot` before
removing from `state` and notifying, so a completion cannot be lost between a
writer's predicate check and its condition-variable wait.

## Memtable

16 shards (`NUM_SHARDS`), routed by `xxh3(user_key)`. Default build: one
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
  `write_buffer_size / 16`).
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

The WAL layout is persisted in the manifest. Explicit per-CF→unified migration
recovers and flushes all legacy memtables while the manifest still says
per-CF, then flips the manifest to unified before the first unified open accepts
writes. A crash before the flip repeats legacy recovery; a crash after it opens
the unified layout over already-durable SSTables.

## Part lifecycle & the part mover (`parts.rs`)

Ordering all part operations follow: in-memory swap under `state.write()` →
`persist_manifest` (the crash-atomic commit point) → only then touch files.
In-flight reads are never interrupted — they finish on the `Arc<SstHandle>`s
(and pinned blocks / mmaps) they already hold, the same lifetime argument
compaction uses when unlinking inputs. `detach_part` is therefore **not
snapshot-consistent** by design: new reads lose the range immediately,
whatever their snapshot seq; pre-existing iterators keep it.

The mover pass (`DbInner::run_part_mover`) snapshots `bottom_parts()` under
`state.read()`, then `lock_partition_span` re-reads and locks the selected
partition's current key range before each move. A compaction that wins the race
first changes the re-snapshot; a move that wins blocks overlapping compaction
for copy + manifest flip. Disjoint ranges remain concurrent. `mover_running`
serializes scheduled/manual mover passes.

`DB::move_part_to_tier_observed` is the deterministic crash-test form of the
same mover. Its synchronous `MovePhaseObserver` runs under the partition range lock at four
semantic boundaries: copied bytes before each destination writer finishes, all
destination objects durable, manifest flip durable, and source cleanup issued.
An observer may block for an external subprocess kill. An injected error before
the manifest phase aborts and reopen selects the source. Callback errors at or
after `ManifestFlipped` are ignored because the destination is already durably
committed; cleanup continues and the API reports success. A lost response is
safe to retry: live handles already on the requested tier are reused without
copying, while any remaining handles are moved. This also heals a mixed-tier
part created by attaching a disjoint bottom table. Never install an observer on
an ordinary latency-sensitive production move.

## S3Storage runtime & blocking contract (`storage_s3.rs`, feature `s3`)

ondaDB has no async runtime; rust-s3 is async. Each `S3Storage` owns a
dedicated **multi-thread tokio runtime** (2 worker threads) and drives every
request with `Runtime::block_on` from whatever engine thread calls in —
point-read threads on a block-cache miss, the compaction worker (mover pass,
compaction reads of S3-resident inputs), and `DB::open` (reader opens).
Contract:

- **Concurrent `block_on` from many engine threads is supported** — that is
  precisely what a multi-thread runtime permits (a `current_thread` runtime
  would deadlock here; do not "simplify" to one).
- Engine threads **block** for the full network round-trip. No engine lock
  is held across a *read* (`read_exact_at` is called from the reader's
  block-miss path, outside all locks), but a part move holds its partition range
  lock across the copy loop — S3 PUT latency stalls overlapping compaction,
  accepted because moves are rare and background.
- Never call `S3Storage` methods from inside the tokio runtime's own worker
  context (`block_on` would panic); nothing in the engine does — all callers
  are plain std threads.
- **Retries stay inside the backend** (0.4.1): each `block_on` is wrapped in
  a bounded retry (4 attempts, 25/50/100 ms backoff) that fires **only** on
  transport-level `S3Error::Hyper`/`S3Error::Io` — never on an HTTP status,
  which this backend reports as `Ok` with a non-2xx code. The engine thread
  simply blocks a little longer on a retried request; no engine-visible
  state changes between attempts, and every request the backend issues is
  idempotent (unique never-reused object ids, whole-object single PUTs), so
  a retry after an ambiguous transport failure can at worst overwrite an
  identical object. Callers must not add their own retry layers on top —
  the worst-case added latency under total outage is bounded (~175 ms of
  backoff plus the request timeouts) and already accounted for in the
  part-move range-lock stall analysis above.
- `S3ReadHandle` holds no OS resource (`release` is a no-op); handles are
  cheap to construct and never go through the `FileCache`, so the
  `max_open_sstables` bound does not apply to S3-resident tables.
- The runtime lives as long as its `S3Storage` (shared `Arc` into every
  handle/writer), i.e. as long as the `TierRegistry` — dropped only when the
  DB closes.

## Background workers

`spawn_workers`: `num_flush_threads.max(1)` flush workers plus
`num_compaction_threads.max(1)` compaction workers, fed
by unbounded crossbeam channels, polling with 50 ms tick to observe `stop`.
`DB::close`: set `closing` → **cancel the background IO limiter** (0.6: so the
drain below cannot wait on a rate) → rotate every CF (+unified) with `force` →
spin until `pending_flush == 0` → set `stop`, join workers → final
`persist_manifest` → close WALs/readers. `DB::clone` increments an explicit
public-handle count; `DB::drop` closes only when that count reaches zero.
Worker-held `Arc<DbInner>` references therefore cannot keep the directory lock
after the final public `DB` handle is dropped.
