# 3.2 — Durable prepared transactions (two-phase commit)

**Readiness:** architectural. A coordinator consumer (ayu/spada) exists, so
this is **no longer product-gated**. **Prerequisites:** 1.0B — both
`CAP_EXTENDED_RECORDS` (the WAL envelope; once enabled *every* frame is an
envelope) and `CAP_TXN_DECISIONS` — plus the unified WAL layout. Wave-0 review
fixes are already released in 0.8.2; do not gate on RV-F4/RV-F5.
**Effort:** 6–10 dev-weeks. **wavesdb counterpart:** 3.2.

## Scope

A storage-engine **participant** for an external coordinator: durably prepare
a transaction, then resolve it by a stable external id after process restart.
No coordinator election, consensus, or timeout decisions. ondaDB's stance (CAS
and coordination are ayu's layer) makes this exactly the right shape: the
engine holds the durable state, the layer above drives it.

**Unified layout only.** Per-CF WALs cannot atomically establish a prepare
record across independent logs. `Txn::commit` already refuses a multi-CF commit
when `db.unified.is_none()` (`src/txn.rs:572-586`, `InvalidArgs`), before
`reserve_seq`; `prepare` mirrors that variant and refuses in the per-CF layout
regardless of how many CFs the writeset touches (one layout, tested — the
phase rule).

**The central guarantee:** a `prepare` that returns `Ok` cannot subsequently
lose a conflict. Validation and reservation both happen at `prepare`, under one
`commit_mu` acquisition; from that moment the reserved keys are protected from
every other writer (phase rule 5). `commit_prepared` can therefore fail only on
durability (poison, fsync, closed WAL) — never with `Conflict`. That is what a
coordinator needs from a participant, and it is why the reservation check must
cover *all* isolation levels.

## API

```rust
impl Txn {
    /// Durably prepare. Consumes the transaction; validates and reserves the
    /// writeset's keys under `commit_mu`. Refused in the per-CF layout, on a
    /// read-only handle, on a poisoned DB, and for an already-finished txn.
    pub fn prepare(self, id: &[u8; 16]) -> Result<PreparedTxn>;   // new
}
pub struct PreparedTxn { pub id: [u8; 16] }                        // new
impl DB {
    pub fn commit_prepared(&self, id: &[u8; 16]) -> Result<()>;    // new
    pub fn abort_prepared(&self, id: &[u8; 16]) -> Result<()>;     // new
    pub fn list_prepared(&self) -> Vec<PreparedInfo>;              // new
}
pub struct PreparedInfo {                                          // new
    pub id: [u8; 16],
    pub age: std::time::Duration,   // from `util::now_nanos` at prepare
    pub bytes: usize,               // arena + record overhead
    pub cf_ids: Vec<u64>,           // `ColumnFamily::id()` values
}
```

`Txn::done: bool` (`src/txn.rs:81`) becomes the three-state enum from the phase
plan. After `prepare`, `put`/`delete`/`single_delete`/`set_savepoint`/
`rollback_to_savepoint`/`release_savepoint`/`reset`/`commit` on the (consumed)
transaction are unreachable by ownership; the refusals that matter are the
ones on the `DB` methods.

Ids are supplied by the coordinator. `prepare` with an id already live in the
registry returns `Exists`.

## Wire format

Under 1.0B's WAL envelope, which 3.2 extends rather than reshapes. Frame
framing is unchanged (`[payload_len u32 LE][crc32c u32 LE][payload]`,
`src/wal.rs:8`), and so is the record layout:

```text
payload : 0xFF | schema uvarint | count uvarint | record x count
record  : kind uvarint | modifiers uvarint | alen uvarint | blen uvarint
        | seq uvarint | ttl varint (iff modifiers & HAS_TTL) | a | b
```

`schema = 2` (unified): the 8-byte big-endian CF id stays **inside** the key
slot, exactly as `UnifiedStore::apply` builds it (`src/unified.rs:311-314`);
there is no separate cf-id field. 1.0's generic `(a, b)` slot naming is what
lets the control kinds reuse the layout unchanged. For WAL records the only
modifier bit is `HAS_TTL` (0x02) — `TOMBSTONE` and `SINGLE_DELETE` are kinds
2 and 3, not modifiers.

**`KIND_PREPARE` = 16.** One frame, `count = 1 + N`:

| # | kind | modifiers | alen | blen | seq | a | b |
|---|---|---|---|---|---|---|---|
| 0 | 16 | 0 | 16 | `8*C` | 0 | the 16-byte id | `C` CF ids, `u64` LE each |
| 1..N | 1 / 2 / 3 | `HAS_TTL`? | `8+len(uk)` | value len | 0 | `cf_id` BE ‖ user key | value |

**`KIND_COMMIT_DECISION` = 17.** One frame, `count = 1`:

| # | kind | modifiers | alen | blen | seq | a | b |
|---|---|---|---|---|---|---|---|
| 0 | 17 | 0 | 16 | 16 | 0 | id | `commit_seq u64 LE` ‖ `count u64 LE` |

`commit_seq` is the **first** sequence of the reserved block (the local named
`start` in `Txn::commit`, `src/txn.rs:599`), and `count` is the record count.
Carrying both makes the decision self-sufficient: replay can raise the
watermark to `commit_seq + count - 1` without reading the prepare frame.

**`KIND_ABORT_DECISION` = 18.** One frame, `count = 1`: kind 18, `alen = 16`
(id), `blen = 0`.

### Frame-shape rules, enforced at decode

A control frame is decoded as a unit, so recovery never has to infer grouping
from callback adjacency:

- A frame whose first record is kind 16 contains exactly one kind-16 record
  followed by records of kinds 1/2/3 only, every record with `seq == 0`.
  Anything else is `Corruption`.
- A frame whose first record is kind 17 or 18 has `count == 1` and `seq == 0`.
- Kinds 16–18 in a binary without 3.2 are an *assigned but unimplemented* kind
  → `UnsupportedFormat` (1.0's taxonomy); so is opening a database whose caps
  word has `CAP_TXN_DECISIONS` set.

3.2 therefore extends 1.0's replay enum rather than adding a parallel one:

```rust
// new arms on wal::ReplayRecord (1.0 owns the enum)
Prepare  { id: [u8; 16], cf_ids: Vec<u64>, records: Vec<Record> },
Decision { id: [u8; 16], commit: Option<(u64 /*commit_seq*/, u64 /*count*/)> },
```

`commit: None` is the abort decision. Both callers of `Wal::replay` match
exhaustively; `ColumnFamily::load` (`src/column_family.rs:491`, the per-CF
layout) rejects both arms with `InvalidArgs` — 2PC is unified-only.

### Why every control record carries `seq = 0`

`Wal::replay_file` derives the returned high-water mark from record `seq`
fields (`src/wal.rs:516-518`), and that value becomes `unified_max_seq` →
`inner.observe_seq(unified_max_seq)` (`src/db.rs:546`). A prepare frame must
**never** raise the watermark: its records are not committed and may yet be
aborted. `seq = 0` is an unambiguous sentinel — a real record can never carry
it (`next_seq` starts at `manifest.global_seq + 1 ≥ 1`, `src/db.rs:522`) and
`observe_seq` already returns early on `seq == 0` (`src/db.rs:267-270`).

The watermark for a committed prepare comes from the **decision** instead, in
pass 2 (below), via an explicit `observe_seq(commit_seq + count - 1)`. Without
that call, a crash between the decision fsync and the memtable apply would
leave no WAL record carrying those sequences, `next_seq` would restart below
`commit_seq`, and the next ordinary commit would **reuse** them — a direct
violation of AGENTS.md invariant 5.

Golden bytes for all three frames are pinned in a fixture test, alongside 1.0's
envelope corpus.

## Visibility and conflict

- Prepared records are **not** applied to the memtable and **not** published.
  Nothing uncommitted becomes visible. (`poisoned_txn_commit_does_not_publish`,
  `src/db.rs:1566-1588`, is the precedent for the *visibility* half only — it
  poisons before `commit`, so it returns at `src/txn.rs:556` before
  `reserve_seq` and says nothing about reserved sequences. The reserved-range
  contract gets its own test — the
  `prep_commit_failure_publishes_reserved_range` row of the crash matrix.)
- Sequence numbers are reserved only at `commit_prepared`. A prepare that never
  commits leaves no sequence gap, because it never reserved one.
- **Reservation registry** (**new**, `DbInner::prepared: Mutex<PreparedRegistry>`):
  two maps — `by_id: HashMap<[u8;16], PreparedEntry>` and
  `by_key: HashMap<(u64 /*cf.id()*/, Vec<u8> /*user key*/), [u8;16]>`. The key
  identity is `(cf.id(), key)` — the durable FNV id (`src/column_family.rs:923`,
  `src/unified.rs:28-37`), **not** `cf_id(&Arc<ColumnFamily>)`'s pointer
  identity (`src/txn.rs:94-96`), which a dropped-and-recreated CF reuses. This
  is the same key 3.3's lock manager uses, and the same lesson as review finding
  L2.
- `prepare` takes `commit_mu`, runs the transaction's own level validation
  (`validate_commit`, `src/txn.rs:463-471`: write-write for Snapshot and
  Serializable, read-set for Serializable), then checks `by_key` for overlap,
  then registers — all under one acquisition. First preparer wins; a
  second overlapping `prepare` returns `Conflict`.
- **Every** ordinary commit checks `by_key` before applying, under `commit_mu`
  (phase rule 5), and returns `Conflict` on overlap. This extends `commit_mu` to
  `ReadUncommitted`/`ReadCommitted`/`RepeatableRead`, which take no lock today
  (`src/txn.rs:588-592`) — including every single-op `DB::put`/`DB::delete`
  (`src/txn.rs:196`, `:209`). Measure that cost and publish it (phase plan exit
  criteria).

### Memory bound and the buffer pool

`Txn::prepare(mut self)` moves the arena into the registry with
`std::mem::take(&mut self.buf)`. The subsequent `Drop` (`src/txn.rs:694-700`)
then calls `put_buf` on an empty `Vec`, which returns immediately
(`src/txn.rs:109-112`, `capacity() == 0`) — so the arena never re-enters
`BUF_POOL`. That bypass is required, not incidental: `BUF_POOL_MAX_CAP` is
32 MiB and `BUF_POOL_MAX_LEN` is 4, and pinning an up-to-32-MiB buffer in a
thread-local for an unbounded prepare lifetime is exactly what those caps
exist to prevent. On resolve the buffer is **dropped**, never recycled — the
resolving thread is usually not the preparing thread.

The registry cap is therefore a **byte** cap, not a count:
`Options::max_prepared_bytes` (**new**, default 64 MiB), charged as
`buf.len()` plus per-write bookkeeping, summed across all unresolved prepares.
Exceeding it fails the `prepare` with `TooLarge` and registers nothing.
(`MemoryLimit` also exists and is dead; `TooLarge`'s doc comment — "value or
request exceeds a hard size limit" — is the right fit. Leave `MemoryLimit`
dead.) `max_prepared_bytes` is a host-describing, **non-persisted** `Options`
field, like 0.8's `max_subcompactions`: no config-blob tail, no reopen test for
the value itself.

On resolve, the entry's `buf` and `writes` are dropped immediately (freeing the
byte budget) and a small tombstone `{ id, prepare_gen, decision_gen, … }`
survives until both generations are unlinked (see pins).

## `commit_prepared` ordering

Decision frame **first**, then the apply: recovery must never find applied
data without a durable decision naming the sequences it was applied at.

```text
commit_prepared(id):
  0. poison.check()?;  read-only → ReadOnly;  unknown id → NotFound;
     already-resolved id → Ok (idempotent coordinator retry).
  1. take commit_mu.
  2. n = entry.writes.len();  start = db.reserve_seq(n).
  3. capture wal = unified.state.read().wal.clone()   // the Arc used below
     wal.append_batch(&[decision{id, start, n}])?;  wal.sync()?;
  4. unified.apply_memtable_only(&items, start)      // NO WAL re-append
  5. db.publish_range(start, start + n)              // UNCONDITIONAL
  6. db.note_thread_commit(start + n - 1);  registry.resolve(id, decision_gen)
  7. drop commit_mu;  run commit hooks;  sweep retirable WAL generations.
```

**Structure the body like `Txn::commit` (`src/txn.rs:601-615`)**: compute the
result of steps 3–4 into a local, run step 5 unconditionally, then return the
error. Every exit path after `reserve_seq` — decision write failure, fsync
failure, apply failure, poison — must publish its reserved range, or
`visible_seq` freezes permanently (AGENTS.md invariant 5). Test:
`prep_commit_failure_publishes_reserved_range`.

Notes on individual steps:

- **Step 3 syncs the captured handle**, per phase rule 2. `UnifiedStore::sync_wal`
  (`src/unified.rs:471-477`) re-clones the *current* WAL and would miss a frame
  that a concurrent rotation has just closed out (`src/unified.rs:444-446`).
  `decision_gen` is read from the same `state.read()` acquisition as the handle
  (`s.wal_gen`), so the pin bookkeeping names the generation the frame actually
  landed in.
- **Step 4 is a new path.** `UnifiedStore::apply` writes the WAL *and* the
  memtable (`src/unified.rs:325-328`); replaying a prepared writeset through it
  would append a second full copy of the writeset to the WAL, and recovery would
  then have to reconcile a decision against an ordinary frame. Add
  `UnifiedStore::apply_memtable_only(&self, items: &[(u64, RecordRef<'_>)],
  start: u64) -> Result<()>` (**new**; the decision doc's "`apply_recovered`-style
  path"). It does the same rotation-gate/`active_writers` bookkeeping as `apply`
  (AGENTS.md invariant 9), builds the same id-prefixed scratch keys, calls
  `mem.put_batch(&recs)`, and skips the `append_batch` call. It is the *only*
  apply used by `commit_prepared` and by recovery pass 2 — one code path, one
  set of tests.
- **Step 7 runs hooks after `commit_mu` is dropped**, matching `Txn::commit`
  (`src/txn.rs:619-621`). The `CommitOp` values are rebuilt from the registry's
  records for CFs where `has_commit_hook()`; dropping hook delivery for prepared
  commits silently would be a regression.

### `abort_prepared`

`poison.check()` first: an abort must write a durable decision, so on a
poisoned DB it returns `Poisoned` and the prepare stays on disk, resolvable
after reopen. Otherwise: append + fsync the kind-18 frame on the captured
handle, drop the registration and its reserved keys, record `decision_gen`,
sweep. **No sequence is reserved and none is published** — the prepare never
reserved one.

### Latency contract (RV-M3, stated not hidden)

`commit_mu` is already held across `reserve_seq` → `apply_prepared` →
`publish_range` → `note_thread_commit` (`src/txn.rs:588-617`); RV-M3 recorded
that as deferred. 3.2 makes it worse in two ways and both are accepted
deliberately:

1. `commit_prepared` holds `commit_mu` across an **fsync**, inside a
   reserved-but-unpublished window. `visible_seq` cannot advance past the
   reserved block until the fsync returns.
2. Meanwhile every concurrent fixed-isolation `begin`/`reset` spins in
   `wait_visible_at_own_floor` (`src/db.rs:207-218`) — `yield_now` in a loop,
   bounded at one second, after which it silently falls back to the plain
   watermark.

So a slow decision fsync burns CPU in every concurrent transaction's `begin`.
Publish p50/p99 for `begin` and for the single-op write path under a workload
with a concurrent `commit_prepared` stream. If the numbers are unacceptable,
the fix is RV-M3 (write intents or a second publication protocol), not a
weakening of the ordering above.

## WAL generation pins

The prepare frame lives in unified WAL generation `P`; the decision lands in
whatever generation `D ≥ P` is current at resolve time. Pinning only `P` is
**wrong** and produces a data bug:

1. `prepare` writes into generation `P`; pin(P).
2. Rotation seals `P` into an immutable and opens `P+1`
   (`src/unified.rs:423-446`).
3. `commit_prepared` writes the decision into `D = P+1` and applies; pin(P)
   released.
4. `D`'s immutable flushes first, manifest persists, its WAL files are deleted
   (`src/db.rs:1317-1321`).
5. Crash. `UnifiedStore::open` (`src/unified.rs:210-233`) enumerates the
   `unified-wal-*.log` files that still exist and replays them: it sees the
   **prepare** and no decision, re-registers a reservation for a transaction
   that already committed, and a coordinator retry re-applies the whole
   writeset at fresh sequence numbers — a silent duplicate write.

Generations retire independently (each sealed immutable carries its own
`wal_paths`, `src/unified.rs:423-426`, and rotation resets
`s.pending_wals = vec![new_path]`, `:442`), and with
`num_flush_threads > 1` they can retire out of generation order. The pin must
therefore cover the **pair** `(P, D)`.

### Exact retirement predicate

State on `DbInner` (**new**): `wal_gens: Mutex<WalGenState>` holding
`flushed: BTreeMap<u64 /*gen*/, String /*base path*/>` — generations whose
flush has persisted the manifest but whose files are withheld — and the
registry's per-entry `{ prepare_gen, decision_gen, prepare_deleted,
decision_deleted }`.

`flush_unified` (`src/db.rs:1300-1325`) changes its deletion loop from

```rust
for path in &imm.wal_paths { crate::wal::remove_wal_files(path); }
```

to `db.retire_wal_paths(&imm.wal_paths)` (**new**), which marks each path's
generation flushed and then runs the sweep. **Per path, not per imm**:
`imm.wal_paths` is a `Vec<String>`, and the first immutable after an open
carries *every replayed path plus the new one* (`src/unified.rs:252-253`,
`let mut pend = replay_paths; pend.push(p);`), so one imm routinely bundles a
pinned generation with unpinned ones. The generation number is parsed from the
file name with the same rule `UnifiedStore::open` uses
(`unified-wal-<gen>.log`, `src/unified.rs:210-218`); factor it into
`wal_gen_of_path` (**new**).

Sweep — a fixpoint loop over `flushed`, deleting `G` when all hold:

- **(a)** no *unresolved* prepare has `prepare_gen == G`;
- **(b)** every tombstone with `prepare_gen == G` has its decision flushed
  (`decision_gen` present and already in `flushed` or already deleted);
- **(c)** every tombstone with `decision_gen == G` **and** `prepare_gen != G`
  has `prepare_deleted == true`.

On deleting `G`: `wal::remove_wal_files(path)`, set `prepare_deleted` /
`decision_deleted` on the tombstones naming it, drop tombstones with both set,
and re-run the loop. The sweep also runs after every resolve and after every
abort, so a generation withheld by a flush is unlinked as soon as its pin
clears.

Condition (c) is the ordering rule: **the prepare is unlinked before its
decision, never the other way round.** A crash after unlinking `P` leaves a
decision for an unknown id, which recovery treats as a no-op — safe, because
`P` was only unlinked once the decision's own generation was flushed, i.e. once
the applied records were durable in L0. A crash after unlinking `D` first would
leave the phantom prepare of the failure sequence above. Condition (c) excludes
`prepare_gen == G` because a prepare and its decision in the *same* generation
retire atomically with the file; without that exclusion the predicate is
circular and the generation never retires.

### Costs the pin imposes

- A pinned generation is **fully re-replayed** on the next open. Replay has no
  sequence floor, no generation floor and no manifest cross-check: file absence
  *is* the flushed marker (`src/unified.rs:210-233`, established by
  `flush_unified`'s `remove_wal_files` after `persist_manifest` — invariant 1).
  Re-inserting records already durable in L0 is read-correct but re-inflates
  the memtable accounting through `after_insert(added, seq)`
  (`src/memtable.rs:320`), causing an immediate redundant rotation and flush
  after a pinned reopen. Expected, documented, asserted in a test.
- Re-insertion is **observably** idempotent, not structurally idempotent, and
  the two memtable backends differ. Default build: `SkipMap::insert` on
  `IKey::new(user_key, seq, &self.cmp)` replaces
  (`src/memtable.rs:310-317`). `arena-memtable` (i.e. `unsafe-fastpath`):
  `ArenaShard::put` → `insert_node` always links a **new node**
  (`src/memtable_arena.rs:209-215`, `:237`), so a repeat at the same
  `(key, seq)` leaves a duplicate node that shadows identically. Reads agree;
  memory and `approx_size` do not. Every idempotency test runs in **both**
  feature configurations.
- An abandoned prepare pins its generation forever. That is the intended
  operator-visible failure mode: `list_prepared` reports id, age and bytes;
  nothing is ever auto-aborted.

The pin registry and `DeletionPause` (`src/db.rs:356`) stay **separate**
mechanisms and do not unify: `DeletionPause` guards SSTable unlinking for
checkpoint/backup (AGENTS.md invariant 6, routed through `remove_sst_file`),
while WAL files are deleted by a bare `crate::wal::remove_wal_files` on both
flush paths (`src/db.rs:1280`, `:1319`) and have never gone through it.
Merging them would put an unbounded, coordinator-driven hold on SST deletion.

## Recovery — two passes, order-free

The unified WAL is **4-striped** in every mode except `SyncMode::Full`
(`src/wal.rs:231-235`, `WAL_STRIPES = 4` at `:152`), and the default unified
sync mode is `SyncMode::None` (`src/config.rs:434`), so the default
configuration has four stripes. Stripe choice is per-thread
(`my_stripe`, `src/wal.rs:335`) and `Wal::replay`'s own doc comment
(`src/wal.rs:471-478`) states the consequence: *"record order across stripes is
not meaningful — sequence numbers define visibility."* Since `prepare` and
`commit_prepared` are separate API calls that commonly run on different
threads, the prepare frame and its decision land in different stripe files with
no recoverable relative order. **"Replay frames in order" is not available**,
and requiring `SyncMode::Full` to buy it would be a silent, expensive
configuration constraint. Recovery is order-free instead.

**Pass 1** — inside `UnifiedStore::open`, over every generation (ascending)
and every stripe, exactly as today plus two new match arms in the replay
closure (`src/unified.rs:228-231`):

- `ReplayRecord::Point` → `mem.put(...)` as today; `max_seq` unchanged.
- `ReplayRecord::Prepare` → collect `{ id, cf_ids, records, prepare_gen }` into
  `RecoveredPrepares`; **nothing is inserted into the memtable**.
- `ReplayRecord::Decision` → collect `{ id, commit, decision_gen }`.
- two *unresolved* prepares with the same id → `Corruption` naming the id (ids
  may be reused only after the previous instance is resolved and retired;
  `prepare` enforces the live half with `Exists`).

`UnifiedStore::open` returns `(Arc<UnifiedStore>, u64, RecoveredPrepares)`;
`build_db_inner` moves the third element into `DbInner::prepared` in the same
literal that initialises `next_seq` (`src/db.rs:513-545`) — phase rule 3.

**Pass 2** — `DbInner::resolve_recovered_prepares` (**new**), called in
`build_db_inner` immediately after the existing
`inner.observe_seq(unified_max_seq)` (`src/db.rs:546`), i.e. after `DbInner`
exists (so `observe_seq` is callable) and before workers are spawned or
`DB::open` returns:

- **commit decision present** → `observe_seq(commit_seq + count - 1)` **first**,
  then apply the prepare's records at `commit_seq + slot` through
  `UnifiedStore::apply_memtable_only` — the same path `commit_prepared` uses.
  Watermark before data, so a decision whose records fail to apply still
  reserves its sequences.
- **abort decision present** → drop the registration.
- **decision for an unknown id** → no-op (the pair was already retired; its
  data is durable in L0 by the retirement ordering). Call `observe_seq` anyway;
  it is a no-op below the recovered watermark.
- **no decision** → keep as unresolved: reservation restored, records held in
  memory against `max_prepared_bytes`, `prepare_gen` pinned. If the recovered
  set alone exceeds the cap, open still succeeds — the cap governs new
  prepares, never recovery; refusing to open a database because of durable
  state on disk is the wrong failure mode. Log it and let `list_prepared` show
  it.

Recovery invokes no application code and repeated opens are idempotent —
asserted across ≥3 opens, both feature configs.

## Control-plane behaviour

| Operation | With an unresolved prepare |
| --- | --- |
| `DB::close` (`src/db.rs:1031`) | Returns `Busy` naming the count of unresolved prepares, **before** setting `closing`. The WAL and its pinned generations survive; reopen recovers them. |
| `DB::drop` (`src/db.rs:1208-1221`) | Cannot return an error. Last handle: log at the poison/reason surface, skip the WAL deletion the pins forbid, still stop workers, persist the manifest, and **release the directory lock** — `tests/lock_release_on_drop.rs` (`dropping_the_last_handle_releases_the_lock`) must keep passing. Pinned WAL files are simply left on disk. |
| `drop_column_family` | `Busy` **only if** the prepare's `cf_ids` (which the kind-16 record carries) include this CF's `id()`. In unified mode the WAL is DB-wide, so a prepare on CF `a` must not block dropping CF `b`. |
| `clear_column_family` | Already `InvalidArgs` in unified mode (`src/db.rs:944-948`); unchanged. |
| `checkpoint` / `backup` | **Succeed.** `snapshot_to` (`src/maintenance.rs:158-204`) pauses deletions, flushes memtables, persists the manifest, and links only SSTables plus `MANIFEST` — it never copies WAL files. So the copy contains **no prepared state**, and `list_prepared` on a database opened from it is empty. Refusing would deny an operator a backup at exactly the moment an abandoned prepare makes one most useful. Documented on both methods; test `prep_checkpoint_has_no_prepared_state`. |
| Read-only open | `UnifiedStore::open` sets `wal: None` under `opts.read_only` (`src/unified.rs:240-241`) and `sync_wal` becomes a no-op (`:471-477`). `prepare`, `commit_prepared` and `abort_prepared` all return `ReadOnly`; `list_prepared` **works** and reports what pass 1 recovered. |
| Poisoned DB | `prepare`/`commit_prepared`/`abort_prepared` return `Poisoned` (existing `poison.check()` shape, `src/txn.rs:556`). Reads and `list_prepared` keep working. Durable prepared state is never discarded by poisoning. |

## Crash matrix

| Point | On-disk | Recovery | Test |
| --- | --- | --- | --- |
| crash after prepare fsync, before `prepare` returns | prepare frame durable | reservation restored; records invisible; no sequence reserved | `prep_crash_after_sync` |
| crash mid decision write (torn frame) | decision absent (frame CRC fails, stripe ends cleanly) | still prepared; coordinator retries `commit_prepared` | `prep_torn_decision` |
| crash after decision fsync, before apply | decision durable, memtable never touched | pass 2 raises the watermark to `commit_seq + count - 1`, then applies the prepare's records | `prep_crash_before_apply` |
| crash mid apply | decision durable, memtable partially populated (and volatile) | memtable content is lost with the process; pass 2 re-applies the whole writeset from the prepare frame at `commit_seq + slot`. Idempotency is *observable*, not structural — see the arena note above | `prep_crash_mid_apply` |
| decision write or fsync fails (no crash) | decision may or may not be durable | `commit_prepared` publishes `[start, start+n)` and returns the error; `visible_seq` keeps advancing; DB is poisoned by `Wal::sync` as usual | `prep_commit_failure_publishes_reserved_range` |
| abort after a crash-recovered prepare | prepare frame + abort decision | registration dropped; no gap (no sequence was ever reserved) | `prep_abort_after_restart` |
| flush would retire a pinned generation | WAL files withheld, manifest persisted | files survive; deleted by the sweep once the pin clears | `prep_wal_pin_withholds_generation` |
| crash between unlinking `P` and unlinking `D` | decision without prepare | no-op; data already durable in L0 | `prep_decision_without_prepare_is_noop` |
| repeated opens with an unresolved prepare | prepare frame retained each time | identical registry after 1, 2 and 3 opens | `prep_recovery_idempotent_across_opens` |

## Hermitage anomaly table (prepared mode)

Rows are the Hermitage tests; columns are ondaDB's five levels
(`src/config.rs:108-129`). "P" = prevented, "A" = anomaly possible. The
*prepared* column is the outcome for a transaction that went through
`prepare` → `commit_prepared`, against a concurrent ordinary writer.

| Anomaly | RU | RC | RR | Snapshot | Serializable | Prepared (any level) |
| --- | --- | --- | --- | --- | --- | --- |
| G0 dirty write | A | A | A | P (write-write validation) | P | **P** — the reservation blocks the other writer at every level (phase rule 5) |
| G1a dirty read (aborted) | P | P | P | P | P | **P** — prepared records are never applied and never published |
| G1b intermediate read | P | P | P | P | P | **P** — same reason |
| G1c circular info flow | P | P | P | P | P | **P** |
| P4 lost update | A | A | A | P | P | **P** — reservation, not validation, is what prevents it |
| G-single read skew | A | A | P (pinned snapshot) | P | P | as the txn's own level |
| G2-item write skew | A | A | A | A | P (read-set validation) | as the txn's own level |
| G2 phantom / anti-dependency | A | A | A | A | **A** | as the txn's own level — documented non-goal: `Serializable` validates point reads only (`src/txn.rs:451-461`, AGENTS.md) |

G1a–G1c are prevented at *all* levels because ondaDB has no dirty-read path:
`ReadUncommitted` reads the live published watermark, not uncommitted state.
The prepared column's G0/P4 entries are the feature: they hold at
`ReadUncommitted` too, which is only true because rule 5 puts the reservation
check on every commit path.

## Slices

1. Envelope kinds 16/17/18 + golden bytes; `UnsupportedFormat` refusal without
   `CAP_TXN_DECISIONS`.
2. Reservation registry + the all-levels commit check + conflict tests
   (optimistic × prepared × ordinary), with the `commit_mu` measurement.
3. `prepare`: validation, registration, forced sync, arena hand-off, byte cap,
   refusals (per-CF layout, read-only, poison, duplicate id).
4. WAL generation pins: `retire_wal_paths`, the sweep, `flush_unified` change.
5. `commit_prepared` / `abort_prepared`: ordering, `apply_memtable_only`,
   unconditional publish, hooks.
6. Two-pass recovery + the crash matrix.
7. Control-plane matrix; `list_prepared`; docs (`concurrency-and-safety.md`
   lock inventory row, `formats.md` frame layouts).

## Implementation tasks

Ordered. One at a time, test first, both feature configurations green before
moving on. The gate after **every** task is the four AGENTS.md commands
(`cargo test`, `cargo test --features unsafe-fastpath`,
`cargo clippy --all-targets`, `… --features unsafe-fastpath`), checking each
test binary for the presence of `test result: ok`.

1. **Frame encode/decode + golden bytes.** `src/format.rs`: kind constants
   `KIND_PREPARE = 16`, `KIND_COMMIT_DECISION = 17`, `KIND_ABORT_DECISION = 18`
   (1.0 reserves 16..31; 3.2 assigns three of them). `src/wal.rs`: the two new
   `ReplayRecord` arms, the frame-shape rules, and encoders. Tests first,
   in-module: `prepare_frame_golden_bytes`, `commit_decision_golden_bytes`,
   `abort_decision_golden_bytes` — each pins the exact byte vector from the
   Wire format tables; `control_frame_seq_is_zero` asserts every decoded
   record's `seq == 0`; `control_frame_roundtrip` asserts decode∘encode is the
   identity for a 3-CF, 5-record prepare;
   `prepare_frame_with_nonzero_seq_is_corruption` and
   `prepare_frame_with_foreign_kind_is_corruption` pin the shape rules;
   `txn_kinds_without_feature_are_unsupported_format` pins the 1.0 taxonomy.
2. **Registry types, no API yet.** `src/txn.rs` or a new `src/prepared.rs`:
   `PreparedRegistry`, `PreparedEntry`, `RecoveredPrepares`, `PreparedInfo`.
   Tests: `registry_key_uses_durable_cf_id` (two CFs whose `Arc` addresses are
   reused still map to distinct keys), `registry_byte_cap_rejects_with_too_large`
   (asserts `err.kind() == "too_large"`), `registry_resolve_frees_bytes`.
3. **`DbInner::prepared` + the all-levels commit check.** Wire the registry into
   `DbInner`; move the `commit_mu` acquisition in `Txn::commit`
   (`src/txn.rs:588-592`) to unconditional. Tests in `tests/db.rs`:
   `ordinary_commit_conflicts_with_reservation_at_every_level` (parameterised
   over all five `IsolationLevel`s; asserts `Conflict`),
   `single_op_put_conflicts_with_reservation` (via `DB::put`). Then run
   `onda_bench` before/after and record the numbers under
   `bench-results/3.2/<date>/` per the implementation plan's evidence format.
4. **`Txn::prepare`.** State enum, validation + reservation under one
   `commit_mu`, arena hand-off with `std::mem::take`, frame append + `sync` on
   the captured handle, `prepare_gen` recorded. Tests in a new
   `tests/prepared_txn.rs`: `prepare_refuses_per_cf_layout` (asserts
   `invalid_args` and the multi-CF wording shape),
   `prepare_refuses_read_only` (`readonly`), `prepare_refuses_poisoned`
   (`poisoned`), `prepare_duplicate_id_returns_exists`,
   `prepare_does_not_publish_or_reserve` (asserts `visible_seq` and the next
   commit's start sequence are both unchanged across a `prepare`),
   `prepare_buffer_does_not_return_to_pool` (two prepares of 1 MiB each remain
   independently readable — the arena was moved, not recycled).
5. **`UnifiedStore::apply_memtable_only`.** `src/unified.rs`. Tests in-module:
   `apply_memtable_only_writes_no_wal` (WAL file size unchanged across the
   call), `apply_memtable_only_holds_active_writers` (a concurrent `rotate`
   blocks until it returns — the invariant-9 check),
   `apply_memtable_only_is_visible_after_publish`.
6. **`commit_prepared`.** The seven-step protocol; failure funnel modelled on
   `src/txn.rs:601-615`. Tests: `commit_prepared_applies_and_publishes`,
   `commit_prepared_is_idempotent_on_retry` (second call returns `Ok`, no second
   apply — assert the value's sequence is unchanged),
   `commit_prepared_unknown_id_is_not_found`,
   `prep_commit_failure_publishes_reserved_range` (inject a WAL failure; assert
   `visible_seq` still advances past the block and a subsequent ordinary commit
   becomes visible), `commit_prepared_runs_commit_hooks`.
7. **`abort_prepared` + `list_prepared`.** Tests:
   `abort_prepared_releases_reservation` (a previously-conflicting ordinary
   commit now succeeds), `abort_prepared_reserves_no_sequence`,
   `abort_prepared_on_poisoned_db_is_refused`,
   `list_prepared_reports_id_age_and_bytes`, `list_prepared_works_read_only`.
8. **Pins and the sweep.** `DbInner::wal_gens`, `retire_wal_paths`,
   `wal_gen_of_path`, the fixpoint sweep; change `flush_unified`
   (`src/db.rs:1317-1321`). Tests in `tests/unified.rs`:
   `prep_wal_pin_withholds_generation` (force a rotation and flush with an
   unresolved prepare; assert the generation's files still exist),
   `pin_releases_after_decision_and_flush` (assert **zero** `unified-wal-*.log`
   files remain after resolve + flush + close),
   `prepare_and_decision_in_same_generation_retire_together` (the (c)-exclusion
   edge case), `decision_generation_not_unlinked_before_prepare` (a
   `MovePhaseObserver`-style ordering assertion on the unlink calls, or a
   direct assertion on the sweep's returned order).
9. **Pass 1 recovery.** `UnifiedStore::open` kind dispatch and the third return
   element; `build_db_inner` plumbing. Tests: `prep_crash_after_sync`,
   `prep_torn_decision`, `duplicate_unresolved_prepare_id_is_corruption`,
   `prepare_frame_does_not_raise_max_seq` (assert `next_seq` after reopen equals
   what it was before the prepare).
10. **Pass 2 recovery.** `DbInner::resolve_recovered_prepares`. Tests:
    `prep_crash_before_apply` (assert the value is readable **and** that the
    next commit's sequence is above `commit_seq + count - 1`),
    `prep_crash_mid_apply`, `prep_abort_after_restart`,
    `prep_decision_without_prepare_is_noop`,
    `prep_recovery_idempotent_across_opens` (three opens, identical
    `list_prepared` and identical reads),
    `pinned_generation_replay_is_idempotent_in_both_backends` (the arena vs
    SkipMap difference; runs in both configs).
11. **Control-plane matrix.** `close`, `Drop`, `drop_column_family`,
    `checkpoint`/`backup`, read-only. Tests:
    `close_with_unresolved_prepare_is_busy`,
    `drop_with_pins_releases_dir_lock_and_keeps_wal`,
    `drop_cf_untouched_by_unrelated_prepare_succeeds`,
    `drop_cf_named_by_prepare_is_busy`, `prep_checkpoint_has_no_prepared_state`.
12. **Hermitage table as tests.** One test per filled cell of the prepared
    column, named `hermitage_<anomaly>_prepared_<level>`; the ordinary-level
    columns are covered by the existing suite and asserted, not re-derived.
13. **Docs.** `docs/formats.md`: the three frame layouts.
    `docs/concurrency-and-safety.md`: a lock-inventory row for the registry
    (`DbInner::prepared`, taken **inside** `commit_mu`), the pin/sweep
    protocol, and the rule-5 change to `commit_mu` scope. `docs/architecture.md`:
    the two-pass recovery step. AGENTS.md: 2PC is no longer a non-goal.

## Acceptance

- Hermitage table above filled by passing tests, per level.
- Recovery idempotent across ≥3 repeated opens, both feature configs.
- WAL pins provably released: zero `unified-wal-*.log` files after
  resolve + flush + close, and the withheld map empty.
- `read_your_writes`-style concurrency tests extended to prepared commits.
- Rule 5's `commit_mu` extension measured and published under
  `bench-results/3.2/<date>/` (≥5 runs; `docs/performance.md` methodology).
- Both feature configurations green on all four commands.

## Rollback

With no prepared state on disk the code path is inert: no frame is written
until `CAP_TXN_DECISIONS` is enabled, and the registry check on an empty
registry is a hash probe. The one irreversible piece is rule 5's `commit_mu`
extension — reverting it re-opens the TOCTOU, so it reverts only together with
the whole feature. With prepared state on disk, readers must stay kind-aware;
an operator lists orphans with `list_prepared` and aborts them explicitly via
`abort_prepared`. Nothing is ever aborted automatically.
