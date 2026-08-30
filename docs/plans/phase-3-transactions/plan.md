# Phase 3 — transaction modes track

Both modes are opt-in; the optimistic snapshot transaction stays the default
and its behaviour is unchanged. 3.2 is **no longer product-gated** — a
coordinator consumer (ayu/spada) exists — so it ships. 3.3 ships after it:
point locks first, span locks only after 1.2's interval representation.

**Baseline:** ondaDB 0.8.2 (`3afc3c1`). The 2026-08 review's Wave-0 items are
landed and released (`docs/code-review-2026-08-resolution.md`: F1–F5, M1, M2,
M4, M6–M9, L1–L4). Nothing in this phase gates on them; do not cite them as
prerequisites. Two review items remain open and both bear on 3.2:

- **RV-M3** — `commit_mu` is held across fsync and rotation stalls. Deferred
  in 0.8.2 as "a performance design item". 3.2 makes it materially worse
  (decision fsync inside the commit critical section) and rule 5 below extends
  `commit_mu` to every isolation level. 3.2 owns the latency contract and the
  measurement; it does not own the fix.
- **RV-M5** — manifest rewrite cost, addressed by feature 2.2. Unrelated to
  correctness here.

| # | Feature | Readiness | Effort | Depends on |
| --- | --- | --- | ---: | --- |
| [3.2](features/32-two-phase-commit.md) | durable prepare/decision | architectural | 6–10 wks | 1.0 `CAP_TXN_DECISIONS` + WAL record envelopes; unified layout |
| [3.3](features/33-pessimistic-locking.md) | point (then span) pessimistic locks | design required | 4–7 wks | new lock manager; 3.2 for the prepared×pessimistic slice; spans need 1.2 |

## Shared state machine

```text
active -> committed | rolled_back
active -> prepared -> committed | aborted
```

`Txn::done: bool` (`src/txn.rs:81`, checked at `:247`, `:258`, `:271`, `:549`,
`:634`, `:650`) becomes a three-state enum (**new**) — `Active`, `Prepared`,
`Finished`. After `prepared`, ordinary mutation, savepoint creation, and
`reset` are refused with `InvalidArgs`. All terminal paths release snapshot
registrations, locks, WAL generation pins, and sequence reservations exactly
once.

## Shared correctness rules

### 1. The full reserved range is always published

`Txn::commit` publishes the **entire** reserved range on every path that
reserved, successful or not (`src/txn.rs:601-615`):

```rust
let application = self.apply_prepared(&prepared, start);
self.db.publish_range(start, start + n);
if let Some(error) = application.error { … return Err(error); }
```

There is no empty-range publish anywhere in the crate and there must not be
one. `publish_range` (`src/db.rs:219-231`) inserts `[start, end)` into
`p.completed` and walks `p.cursor` forward through contiguous entries; an
empty range inserts `start -> start`, the walk sets `p.cursor = start`, and
the cursor **does not advance**. "Close the gap with an empty publish" would
freeze `visible_seq` at exactly the point invariant 5 protects.

The rule for every new code path that reserves: *a failed apply publishes a
range that is empty of data but non-empty of sequence*. Publishing a failed
range is safe — its records never reached the WAL or memtable, so nothing
unapplied becomes visible.

A commit with no writes never reserves at all (`src/txn.rs:563-570` returns
before `reserve_seq`), so it has no gap to close.

### 2. Durable operations fsync explicitly, on the handle they appended to

`Wal::append_batch` (`src/wal.rs:304`) has no `force_sync` argument and no
per-call durability override; `Wal::sync` (`src/wal.rs:424-441`) fsyncs every
stripe regardless of `SyncMode` and poisons the DB on failure. The pair is the
mechanism, with three consequences that belong in every design that uses it:

- The pair is **not atomic**. Another thread's `Wal::sync` or the
  `SyncMode::Interval` background thread can interleave; the sync may cover
  unrelated frames. Harmless for durability, relevant to the latency budget.
- `Wal::sync` clears the whole-WAL `dirty` flag (`src/wal.rs:425`), so a
  forced sync also cancels the interval thread's next scheduled sync. 2PC
  therefore perturbs the `SyncMode::Interval` timing envelope.
- Sync **the handle you appended to**. `UnifiedStore::sync_wal`
  (`src/unified.rs:471-477`) clones the *current* `Arc<Wal>` under the state
  read lock; rotation replaces it and closes the old one
  (`src/unified.rs:444-446`). A frame written just before a rotation cannot be
  synced through `sync_wal`. Capture the `Arc<Wal>` used for the append and
  call `sync()` on that clone.

### 3. Recovery reconstructs prepared state before serving, and hands it across the open boundary explicitly

Unified recovery runs entirely inside `UnifiedStore::open`
(`src/unified.rs:198-282`), which `build_db_inner` calls at `src/db.rs:487-495`
— *before* the `DbInner` literal at `src/db.rs:513`. So the rule is
satisfiable, but the recovered registry needs a named home:

`UnifiedStore::open` returns `(Arc<UnifiedStore>, u64 /*max_seq*/,
RecoveredPrepares)` (**new** third element); `build_db_inner` moves it into a
**new** `DbInner::prepared: Mutex<PreparedRegistry>` field constructed in the
same literal that already initialises `next_seq` from
`manifest.global_seq + 1`. Nothing reads the registry before `DB::open`
returns. Recovery invokes no application code, and repeated opens are
idempotent.

### 4. Cross-CF prepare requires the unified layout

Per-CF WALs cannot atomically establish a record across independent logs.
`Txn::commit` already rejects a multi-CF commit outright when
`db.unified.is_none()` (`src/txn.rs:572-586`), **before** `reserve_seq`, with
`InvalidArgs("multi-column-family transactions require unified_memtable=true
for atomic commit")`. `prepare` mirrors that variant and that wording shape.

### 5. Every commit checks the reservation registry, under `commit_mu`

Three of the five isolation levels take no commit lock and run no validation
today. `src/txn.rs:558-561`:

```rust
let needs_check = matches!(self.isolation,
    IsolationLevel::Snapshot | IsolationLevel::Serializable);
```

and `src/txn.rs:588-592` takes `commit_mu` only when `needs_check ||
isolation == Serializable`. So `ReadUncommitted`, `ReadCommitted` and
`RepeatableRead` reserve → apply → publish entirely unserialized — and that
includes every single-op `DB::put` / `DB::delete`, which begin a
`ReadCommitted` transaction (`src/txn.rs:196`, `:209`).

**Decision:** from 3.2 onward *all* commits perform the reservation check, and
the check is under `commit_mu`. Putting it outside is a TOCTOU: releasing a
registry lock before taking `commit_mu` leaves a window in which a concurrent
`prepare` registers between an ordinary commit's check and its apply, and
first-preparer-wins stops meaning anything. The validation-to-apply exclusion
`commit_mu` provides is exactly what RV-M3's resolution says cannot be
weakened without write intents or a second publication protocol.

Consequences, to be accepted knowingly and **measured**, not assumed:

- The single-op `put`/`delete` fast path takes `commit_mu` where it previously
  took nothing. Benchmark `onda_bench` single-op throughput and p99 before and
  after, per `docs/performance.md` (≥5 runs, same-run ratios).
- The check itself is a hash probe against a registry that is empty in every
  database with no prepared transactions — the intended steady state. Keep it
  non-blocking (no waiting on prepared owners) so an abandoned prepare degrades
  one key range, not the whole write path.

### 6. `Txn` is `Send` today; the cross-thread contract is explicit

`Txn` (`src/txn.rs:61-82`) holds only `Arc<DbInner>`, `Arc<ColumnFamily>`,
`Vec`s, `HashSet`, `HashMap`, `bool`, `u64`. There is no
`PhantomData<*const ()>`, no negative impl, no `unsafe impl` — it is auto-`Send
+ Sync` and can legally be moved to another thread mid-transaction. "`Txn` is
not thread-safe" is a convention, not a type-level fact, and no phase-3 work
adds a `!Send` marker (that would be a breaking change for existing consumers
for no correctness gain).

The contract instead:

- Lock release (3.3) and pin release (3.2) must be correct when the releasing
  thread is not the acquiring thread. `Txn::drop` may run anywhere.
- `THREAD_COMMIT_FLOOR` is a `thread_local!` (`src/db.rs:140-154`), so a moved
  `Txn` already loses its own read-your-writes floor
  (`own_commit_floor`, `src/db.rs:187-191`). Pre-existing; documented, not
  fixed here.
- `DB::commit_prepared(&self, id)` is a `DB` method and will normally run on a
  different thread from `prepare`. `note_thread_commit` (`src/db.rs:166-176`)
  records the floor on the **committing** thread only; the preparing thread
  does not read its own prepared write back through `read_floor_seq`. That is
  accepted and documented on the API.

### 7. No control-plane operation silently abandons durable prepared state

`DB::close`, `DB::drop`, `drop_column_family`, `clear_column_family`,
`checkpoint`, `backup` and read-only opens each define their behaviour with an
unresolved prepare outstanding. `Drop` cannot return an error
(`src/db.rs:1208-1221`), so its behaviour is stated, not refused. 3.2 owns the
matrix.

## Required test matrix (select applicable cells)

- five isolation levels × per-CF vs unified layout (supported, or explicit
  refusal with the variant named) — after rule 5, all five have a commit-time
  serialization point;
- single- and multi-CF batches; savepoint/reset/rollback/close;
- `SyncMode::None`/`Interval`/`Full` on the unified WAL (prepared frames sync
  regardless; note `Full` collapses the WAL to one stripe,
  `src/wal.rs:231-235`, and the other modes use four);
- crash before/inside/after each durable frame and each publication step;
- poison/fail-stop and coordinator retry;
- concurrent optimistic, pessimistic, prepared, and ordinary readers;
- flush / unified WAL rotation / compaction while transaction state is live;
- checkpoint, backup, and read-only open with prepared state outstanding.

## Exit criteria

- Default-mode isolation behaviour unchanged (full regression suite plus
  `tests/snapshot_self_conflict.rs` and `tests/read_your_writes.rs`).
- New anomaly/prevention pairs stated explicitly, per level, with expected
  outcomes. "All five levels" is not accepted without a filled table.
- Recovery idempotent across ≥3 repeated opens, in both feature
  configurations.
- WAL generation pins provably released (counter returns to zero); no
  `unified-wal-*.log` survives a clean resolve-then-flush cycle.
- `list_prepared` exposes id, age and byte size for operator action. No
  automatic abort of abandoned prepares, ever.
- Rule 5's `commit_mu` extension has published before/after numbers for the
  single-op write path.
