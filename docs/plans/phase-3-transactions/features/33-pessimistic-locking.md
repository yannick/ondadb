# 3.3 — Pessimistic transaction locking

**Readiness:** design required; point locks precede span locks.
**Prerequisites:** none for slices 1–4. Slice 5 (composition with prepared
transactions) needs 3.2 landed; slice 6 (span locks) needs 1.2's interval
representation. **Effort:** 4–7 dev-weeks. **wavesdb counterpart:** 3.3.

## Goal

Let selected transactions **wait** for ownership of keys (v1) or spans (v2)
instead of abort-retrying through optimistic validation. Optimistic
transactions remain the default and never wait: at commit validation they
return `Conflict` immediately if a held lock or a prepared reservation overlaps
their writes.

The feature only pays for itself if lock-serialized transactions actually stop
aborting. Locks alone do not achieve that — see **Snapshot refresh** below,
which is the substance of this feature, not a detail.

## Non-goals

Every ordinary write waiting behind application locks. Reusing
`range_locks`: that is the compaction/parts span lock
(`src/column_family.rs:248`, constructed at `:409`/`:538`), used only by
compaction (`src/compaction.rs:64`, `:376`, `:396`) and parts
(`src/parts.rs:95-103`), with no per-owner identity and background waiters
(`src/range_lock.rs:1-30`). The transaction lock manager is **new** and
separate. Its drop-guard style is worth copying (`src/range_lock.rs:27-29`:
guards release on drop, including on panic and on `?` early returns); the type
is not.

Phantom protection is unchanged and remains out of scope: `Serializable`
validates point reads only (`src/txn.rs:451-461`).

## API

```rust
impl DB {
    pub fn begin_pessimistic(&self) -> Txn;                                    // new
    pub fn begin_pessimistic_with_isolation(&self, level: IsolationLevel) -> Txn; // new
}
impl Txn {
    /// Take (or wait for) the lock on `key`, then read. At Snapshot and
    /// Serializable the grant refreshes the transaction's snapshot (below).
    /// Returns `Conflict` if wait-die kills this transaction.
    pub fn get_for_update(&mut self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<Vec<u8>>; // new
}
```

`begin_pessimistic*` mirror the shape of `begin`/`begin_with_isolation`
(`src/txn.rs:144-186`) and set a **new** `Txn::pessimistic: bool`.

## Transaction identity (**new**)

Wait-die needs a total order on transaction age, and `Txn` (`src/txn.rs:61-82`)
has neither an id nor a start stamp today. `read_seq` cannot serve: it is
`visible_seq()` for fixed levels but `read_floor_seq()` for
`ReadCommitted`/`ReadUncommitted` (`src/txn.rs:163-168`) — two different
clocks; it is **not unique** (many concurrent transactions pin the same
watermark, which is why `acquire_snapshot` is refcounted,
`src/db.rs:238-241`); and `reset` reassigns it (`src/txn.rs:659-669`), so a
reused long-lived `Txn` gets *younger* over time.

So: **new** `DbInner::txn_ids: AtomicU64` and **new** `Txn::txn_id: u64`,
assigned with `fetch_add(1)` in `begin_with_isolation` /
`begin_pessimistic_with_isolation` and **re-assigned** in `reset`
(`src/txn.rs:649-684`) — a reset transaction is a new transaction and must be
younger, or a reused handle would starve everyone else forever. Lower id =
older. The counter is per-`DbInner`, distinct from `next_seq` (reusing
`next_seq` would burn sequence numbers on transactions that never commit).

**Wait-die.** A requester older than the holder (`self.txn_id <
holder.txn_id`) waits; a younger requester dies immediately with `Conflict`.
Ids come from one atomic counter, so a genuine tie is impossible; equal ids
mean the holder *is* the requester — a re-entrant acquisition of a lock the
transaction already holds, which is granted immediately and is not a deadlock.
That is the whole tie-break rule.

No cycle detection, no lock-ordering discipline imposed on callers. The
contract is documented, including the caveat in "Fallible buffering" below.

## Snapshot refresh on lock grant — the semantic

Without this, the headline acceptance test cannot pass.
`validate_write_conflicts` (`src/txn.rs:437-449`) is

```rust
if write.cf.peek_seq(key)? > self.read_seq {
    return Err(OndaError::Conflict(...));
}
```

Holding a lock does not move `read_seq`. Thread B waits politely for A's lock
on a hot key; A commits at seq 100; B acquires the lock, commits, and
`peek_seq(key)` returns 100 > B's snapshot — B aborts anyway. So at
`Snapshot`/`Serializable` a lock buys nothing without a snapshot move.

**The semantic (option b):** a successful lock acquisition at `Snapshot` or
`Serializable` refreshes the transaction's `read_seq`.

1. Before releasing a lock, the outgoing owner stamps the `LockEntry` with the
   sequence it committed at (`LockEntry::last_commit_seq`, **new**; written
   from `Txn::release` out of the **new** `Txn::committed_at` field — see
   "Release — one funnel"). A rolled-back owner stamps nothing.
2. On grant, the new owner waits (bounded, `yield_now`, one second — the exact
   shape of `wait_visible_at_own_floor`, `src/db.rs:207-218`) until
   `visible_seq() >= entry.last_commit_seq`. Publication is gap-free
   (invariant 5), so this is transient by construction; the bound only guards a
   torn process.
3. At `Serializable`, re-run `validate_read_conflicts` (`src/txn.rs:451-461`)
   against the **old** `read_seq` first. If a read-set key changed, return
   `Conflict` now rather than adopting a snapshot under which the stale read
   would validate silently. This is what keeps the refresh sound: the reads are
   proven unchanged at the new snapshot, so it is as if they had all happened
   there.
4. Adopt `new_seq = visible_seq()`: `acquire_snapshot(new_seq)` **before**
   `release_snapshot(old_seq)` (`src/db.rs:238-249`), so `oldest_snapshot()`
   never transiently jumps forward and lets compaction GC a version this
   transaction still needs.

`RepeatableRead` does **not** refresh: it runs no validation, so it has no
conflict to avoid, and refreshing would break the one thing its contract
promises. `ReadCommitted`/`ReadUncommitted` hold no snapshot at all.

What this costs, stated plainly:

- **A pessimistic `Snapshot` transaction is no longer snapshot-isolated across
  lock grants.** Reads taken before an acquisition may be older than reads
  taken after it — read skew (G-single) becomes possible where it was
  prevented. That is the price of Conflict-free serialization, and it is the
  documented semantic of pessimistic mode, not a bug.
- At `Serializable`, step 3 converts the same situation into an **earlier**
  abort instead of an anomaly. Pessimistic `Serializable` is therefore *not*
  Conflict-free in general — only for write-write contention with an unchanged
  read set. The acceptance test is scoped accordingly.
- The Conflict-free guarantee holds **between transactions that both take the
  lock**. `peek_seq` reads at `u64::MAX` and can observe an in-flight,
  not-yet-published write (review finding L5, resolved "no change"), so an
  ordinary optimistic writer ignoring the lock can still make a pessimistic
  transaction abort. Locks are advisory *pre-commit* coordination; MVCC remains
  the authority.

## Lock manager (**new**)

Per-DB `DbInner::txn_locks: Mutex<HashMap<(u64, Vec<u8>), LockEntry>>` with
FIFO waiters on per-entry condvar handles. Shard only if contention
measurements demand it.

- The key is `(cf.id(), key)` — the durable FNV id
  (`src/column_family.rs:923`, `src/unified.rs:28-37`), **not**
  `cf_id(&Arc<ColumnFamily>)`'s pointer identity (`src/txn.rs:94-96`). A CF
  dropped and recreated reuses the allocation; that is exactly the bug L2 fixed
  for `THREAD_COMMIT_FLOOR` ("commit floors are keyed by process-monotonic
  database instance ids rather than reusable allocation addresses"). It is also
  the key 3.2's reservation registry uses, so the two subsystems index the same
  space.
- The owned `Vec<u8>` key means every acquisition allocates. That is a real
  departure from `deduplicated_write_order`'s explicit design note — "allocates
  only the slot map and order vector, never key/value copies"
  (`src/txn.rs:409-410`) — and is accepted for v1 because pessimistic mode is
  opt-in and its transactions are, by definition, not latency-critical enough
  to abort-retry. Measure it; do not pretend it is free.
- `LockEntry { owner: u64 /*txn_id*/, last_commit_seq: u64, waiters: VecDeque<..> }`.
- `DB::close` and poisoning wake every waiter with an error. Nothing in this
  feature may hang a caller past `close` (phase rule 7: no control-plane
  operation silently abandons transaction state).

## Acquisition points

- `get_for_update` acquires **before** the buffered-write scan. `Txn::get`
  (`src/txn.rs:281-305`) scans the write set first and returns early for keys
  the transaction already wrote; acquiring after that early return would let a
  transaction read a key it wrote without holding its lock.
- **Upgrade on write:** any buffered write to an unlocked key acquires its lock
  at buffer time, inside `Txn::buffer` (`src/txn.rs:216-237`).

### Fallible buffering

`Txn::buffer` is infallible today (returns `()`). It becomes
`fn buffer(&mut self, …) -> Result<()>` (**new** signature) and `put`,
`delete` and `single_delete` propagate with `?` — they already return `Result`
(`src/txn.rs:240`, `:257`, `:270`), so no public signature changes.

Two behaviour changes that belong in the release notes, not just the code:

- **A wait-die abort now surfaces from `put`**, not only from `commit`.
- **`put` can now block.** It is a pure arena memcpy today. A caller holding
  another lock (its own, or a foreign one such as a `parking_lot` mutex) across
  `put` gains a new deadlock edge that wait-die does not cover — wait-die orders
  transactions, not foreign locks. Documented caveat.

Both apply only when `pessimistic` is set; an optimistic `Txn` never touches
the manager.

## Release — one funnel

`Txn::release` (`src/txn.rs:686-691`) is snapshot-registration-only today.
Lock release folds **into it**:

```rust
fn release(&mut self) {
    if self.snapshot_held { self.db.release_snapshot(self.read_seq); … }
    if self.pessimistic {
        // `committed_at` is a **new** `Option<u64>` field, set on the success
        // path of `commit` next to `note_thread_commit` (src/txn.rs:616) and
        // left `None` on every abort path. It becomes the outgoing owner's
        // `LockEntry::last_commit_seq` stamp; `None` leaves the stamp alone,
        // because a rolled-back owner published nothing to wait for.
        self.db.release_txn_locks(self.txn_id, self.committed_at);
    }
}
```

so every terminal path inherits it. This matters because `commit` has five
distinct returns that each call `release()` separately (`src/txn.rs:568`,
`:580`, `:594`, `:613`, `:628`), `rollback` calls it (`:644`), `reset` calls it
via `rollback` (`:650-652`), `Drop` calls it (`:694-700`) — and the poison
check at `src/txn.rs:556` returns **without** calling it at all, relying on
`Drop`. A lock release wired only into the explicit paths would miss the poison
path. Folding it into `release()` covers all of them, including the
`Drop`-only one; a test pins the poison path specifically.

Release must be correct when the releasing thread is not the acquiring thread:
`Txn` is auto-`Send` (phase rule 6) and may be dropped anywhere.

## Composition with prepared transactions (3.2)

Its own slice, after 3.2 lands. In-memory locks are volatile; reservations are
durable. The conversion:

- **At `prepare`**, the transaction's locks become 3.2 reservations on the same
  `(cf.id(), key)` pairs, inside the same `commit_mu` acquisition that
  registers the prepare. `Txn::release` then drops the in-memory locks — and
  every waiter on those keys is woken with `Conflict` rather than granted a
  lock that is guaranteed to fail its commit against the reservation. The
  reservation is the authority from that instant.
- **After recovery**, only the reservation exists. A new pessimistic
  transaction can acquire the lock (nothing holds it), and its commit returns
  `Conflict` against the recovered reservation until the coordinator resolves
  the prepare. Waiters never hang on a crashed owner.

| Scenario | Waiter's outcome | Test |
| --- | --- | --- |
| owner prepares while a waiter is queued | woken with `Conflict` at the wait point | `waiter_on_prepared_owner_sees_conflict` |
| owner prepares, process crashes, DB reopens | new txn takes the lock; its commit is `Conflict` against the recovered reservation | `recovered_reservation_reblocks_lock_waiters` |
| coordinator commits the prepare | reservation clears; the next pessimistic txn commits without `Conflict` (after its refresh) | `commit_prepared_unblocks_pessimistic_waiter` |
| coordinator aborts the prepare | same, and the aborted writeset is not visible | `abort_prepared_unblocks_pessimistic_waiter` |

## Hermitage anomaly table (pessimistic mode)

Both parties pessimistic, contending on the same key. "P" = prevented,
"A" = anomaly possible. Compare against 3.2's table for the optimistic
baseline.

| Anomaly | RU | RC | RR | Snapshot | Serializable |
| --- | --- | --- | --- | --- | --- |
| G0 dirty write | P | P | P | P | P |
| G1a/G1b/G1c dirty reads | P | P | P | P | P |
| P4 lost update | P | P | P | P | P |
| G-single read skew | A | A | P (no refresh) | **A** (refresh moves the snapshot) | P (refresh revalidates the read set) |
| G2-item write skew | A | A | A | A | P |
| G2 phantom | A | A | A | A | A (documented non-goal) |

G0/P4 become P at *every* level, which is the feature: lock serialization, not
validation, is what prevents them, so it works at levels that run no
validation. The Snapshot/G-single cell is the documented regression from the
refresh semantic. Against a non-locking optimistic writer, every cell reverts
to that level's ordinary outcome — locks are advisory.

## Slices

1. `txn_id` plumbing (`DbInner::txn_ids`, assignment in `begin*`, reassignment
   in `reset`) — no lock manager yet.
2. Lock manager unit + concurrency tests (FIFO, wait-die, release-on-drop,
   poison/close wake).
3. `begin_pessimistic*` + `get_for_update` + fallible buffer-time acquisition.
4. Snapshot refresh + the optimistic×pessimistic interplay matrix at each
   isolation level; the hot-key acceptance measurement.
5. Composition with 3.2: lock→reservation conversion and the recovery rows.
6. Docs: when to prefer pessimistic (hot-key abort storms), the wait-die
   contract, the refresh semantic and its Snapshot cost, no interaction with
   `range_locks`.
7. (v2, after 1.2) span locks `lock_range(cf, start, end)` on the interval
   representation.

## Implementation tasks

Ordered, test-first. The gate after **every** task is the four AGENTS.md
commands (`cargo test`, `cargo test --features unsafe-fastpath`,
`cargo clippy --all-targets`, `… --features unsafe-fastpath`), checking each
test binary for the presence of `test result: ok`.

1. **`txn_id`.** `src/db.rs`: `DbInner::txn_ids: AtomicU64`. `src/txn.rs`:
   `Txn::txn_id`, assigned in `begin_with_isolation` (`:149`) and reassigned in
   `reset` (`:649`). Tests in-module: `txn_ids_are_monotonic_and_unique` (1000
   `begin`s across 8 threads, all distinct and increasing — mirrors
   `database_instance_ids_are_monotonic_and_unique`, `src/db.rs:1403`),
   `reset_assigns_a_younger_txn_id`.
2. **Lock manager, standalone.** New `src/txn_lock.rs`: `LockEntry`,
   `LockTable`, `acquire(txn_id, key) -> Result<LockGuard>`, `release_all`,
   `wake_all_with_error`. Tests in-module, no `DB` involved:
   `fifo_grant_order`, `wait_die_younger_requester_dies` (assert
   `err.kind() == "conflict"`), `wait_die_older_requester_waits`,
   `reentrant_acquire_by_same_txn_id_is_granted`,
   `wake_all_with_error_never_hangs` (a waiter parked on a condvar returns an
   error within a bounded time).
3. **Wire into `DbInner`; release funnel.** `DbInner::txn_locks`,
   `release_txn_locks`; fold into `Txn::release` (`src/txn.rs:686-691`). New
   test file **`tests/txn_lock_release.rs`** — note the existing
   `tests/lock_release_on_drop.rs` is about the `<dir>/LOCK` *directory*
   advisory lock (`dropping_the_last_handle_releases_the_lock`,
   `a_surviving_clone_keeps_the_database_open`) and is unrelated; the distinct
   name is deliberate. Tests: `locks_release_on_commit`,
   `locks_release_on_rollback`, `locks_release_on_drop`,
   `locks_release_on_poisoned_commit` (poison the DB, call `commit`, assert it
   returns at `src/txn.rs:556` **and** that a second transaction acquires the
   key without waiting), `locks_release_when_txn_dropped_on_another_thread`,
   `locks_release_on_panic_unwind`.
4. **`begin_pessimistic*` + `get_for_update`.** Acquisition placed before the
   buffered-write scan in `Txn::get`'s shape. Tests in a new
   `tests/pessimistic.rs`: `get_for_update_blocks_second_reader`,
   `get_for_update_locks_key_the_txn_already_wrote`,
   `optimistic_txn_never_waits` (assert an optimistic commit against a locked
   key returns immediately, with `Conflict` or `Ok` per its level — never
   blocking).
5. **Fallible `buffer`.** Signature change plus `?` in `put`/`delete`/
   `single_delete`. Tests: `wait_die_abort_surfaces_from_put` (assert the
   `Conflict` comes out of `put`, not `commit`),
   `upgrade_on_write_acquires_lock_at_buffer_time` (a second txn's
   `get_for_update` on that key blocks while the first has only buffered).
6. **Snapshot refresh.** `LockEntry::last_commit_seq`, the bounded wait, the
   Serializable revalidation, acquire-before-release snapshot swap. Tests:
   `pessimistic_hot_key_serializes_without_conflict` (two threads, one key,
   1000 rounds at `Snapshot`; assert **zero** `Conflict`s) and its control
   `optimistic_hot_key_aborts` (same workload optimistically; assert > 0);
   `serializable_refresh_aborts_on_changed_read_set`;
   `refresh_waits_out_publication_gap` (a third thread holds an earlier
   unpublished range; assert no `Conflict` and no hang);
   `repeatable_read_does_not_refresh` (assert `read_seq` is unchanged across a
   grant); `refresh_never_advances_oldest_snapshot_early` (assert
   `oldest_snapshot()` is monotonic across the swap).
7. **Isolation matrix + stress.** `pessimistic_level_contract_<level>` for all
   five levels (locks add ordering, not isolation), the Hermitage cells above,
   and a randomized multi-key stress (≥10⁶ acquisitions, random key sets, mixed
   ages) asserting no deadlock and no hang under both feature configs.
8. **Composition with 3.2.** The four rows of the composition table, in
   `tests/pessimistic.rs`, gated on 3.2 being present.
9. **Docs.** `docs/concurrency-and-safety.md`: a lock-inventory row for
   `DbInner::txn_locks` (acquired **outside** `commit_mu`; never held across
   `commit_mu`), the wait-die contract, and the refresh semantic with its
   Snapshot cost. `IsolationLevel` doc comments gain the pessimistic-mode
   deviation.

## Acceptance

- Hot-key contention phase: abort-retry storm (optimistic) vs wait-serialize
  (pessimistic), with retry-corrected throughput and p99 published under
  `bench-results/3.3/<date>/` per the implementation plan's evidence format
  (≥5 runs; `docs/performance.md` thermal-noise rule).
- Zero `Conflict`s in the two-thread hot-key `Snapshot` test; a non-zero count
  in the optimistic control. That contrast is the feature.
- Wait-die: younger dies, older waits, no deadlock under randomized multi-key
  stress.
- Locks released on commit / rollback / drop / cross-thread drop / panic
  unwind / poisoned commit; `DB::close` with locks held wakes waiters with an
  error and never hangs.
- Feature is opt-in and off by default regardless of the numbers.
- Both feature configurations green on all four commands.

## Rollback

New entry points plus one module (`src/txn_lock.rs`); delete them and the
feature is gone. No disk state, no format change, nothing to recover. The two
changes that outlive a revert are `Txn::txn_id` (harmless — an unused counter)
and the fallible `Txn::buffer` signature, which is private and can stay.
