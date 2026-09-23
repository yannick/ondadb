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
| `DbInner::commit_mu` | **every** commit, at every isolation level (3.2 phase rule 5); before that, only Snapshot/Serializable validation + apply and any commit containing a range delete (1.2) | reservation check → conflict check → apply → publish → span-marker insert. In `commit_prepared` it additionally spans the **decision fsync** |
| `DbInner::prepared` (Mutex) | the prepared-transaction reservation registry (3.2) | one hash probe on the commit path; the whole of `Txn::prepare`'s validate-and-register; the whole of `commit_prepared`/`abort_prepared` including their decision fsync. Taken **inside** `commit_mu`, never outside it on a write path, and never while `wal_gens` is held |
| `DbInner::txn_locks` (Mutex + per-entry Condvars) | point locks held by pessimistic transactions (3.3) | one acquire, one release, or one mass wake. A **leaf** on the acquisition path: `acquire` parks holding nothing else, and a parked waiter holds the table mutex only between wake-ups. The one nesting is `commit_mu` → `txn_locks` (`Txn::prepare`'s lock-to-reservation conversion, and `commit`'s early returns, which call `release` under the guard); **never** the reverse |
| `DbInner::wal_gens` (Mutex) | unified WAL generation pins and the retirement sweep (3.2) | one pin, one release, or one sweep. A **leaf**: flush workers take it holding nothing, and the unlinking itself happens after it is dropped |
| `DbInner::span_index` (Mutex+Condvar) | committed-span markers (1.2) | one check, one insert, or one prune; **never held across IO**. Also taken alone, ahead of `commit_mu`, for the capacity reservation |
| `DbInner::cf_lifecycle_mu` (2.2) | the catalog-shape changes that validate before they publish: CF create / create-many / drop / clear, partition-rule add/remove | validation → `catalog_txn` → publish. Taken **before** `manifest_mu`, never after |
| `DbInner::manifest_mu` | manifest rebuild + save; under `CAP_MANIFEST_EDITS` also the edit append, the publish step and the snapshot-compaction trigger | whole `persist_manifest`, or a whole `catalog_txn` |
| `DbInner::publish` (Mutex) | publish cursor | short |
| `DbInner::file_deletion.paused` | pause counter + deferred-delete list | short; `pause_deletions` returns an RAII guard. Never held across the unlink or the channel send |
| `DbInner::file_deletion.worker.{tx,handle}` | deletion-queue sender / join handle | one unbounded `send` (never blocks) or one `take`; the join in `drain_deletions` happens with neither held |
| `ColumnFamily::rot` (Mutex+Condvar) | `active_writers`, `rotating` | gate checks, rotation drain |
| `ColumnFamily::state` (RwLock) | memtable/WAL handles, imm queue, levels | read: clone handles; write: swap/install — keep short |
| `ColumnFamily::compact_mu` (Mutex) | whole-CF compaction operations | manual compaction sweep and FIFO eviction; acquired before the whole-keyspace range lock |
| `ColumnFamily::range_locks` | key ranges being rewritten | bounded compaction jobs use non-blocking acquisition; attach takes the whole keyspace; detach and part moves block on the affected partition span, including copy + manifest flip; delete-only excise (1.2) uses `parts::try_lock_key_span` — non-blocking over the union of its candidates' **span** bounds, held across revalidation, the catalog transaction and file retirement |
| `ColumnFamily::live_partition_rules` (RwLock) | the live partition-rule set | one read acquisition to validate a candidate set, one write acquisition to append/remove it inside the transaction's publish step. Exclusion between concurrent duplicate adds comes from `cf_lifecycle_mu`, which spans both (exactly one wins) |
| `Wal::qstate` / per-stripe file mutexes | group-commit queue / file appends | one frame write |
| `ArenaShard::arena` (Mutex) | skip-list structure per shard | one batch group's inserts |
| `commit_hook` (Mutex) | hook fn | hook invocation |
| `DbInner::span_permits` (Mutex&lt;usize&gt;) | count of free compaction **span workers** (0.8) | one non-blocking take/release; never held across IO |
| `<dir>/LOCK` (OS advisory file lock) | whole DB directory against other processes/handles | entire open→close lifetime; exclusive for read-write, shared for read-only opens; second open fails with `OndaError::Locked` |

Order among the four that meet: `cf_lifecycle_mu` → `manifest_mu` → `cfs` →
`ColumnFamily::state`. `catalog_txn` runs its publish closure while holding
`manifest_mu`, and `write_snapshot` takes `cfs.read()` under the same lock, so
**no caller may hold `cfs.write()` across a catalog transaction** — that is why
`create_column_family` and friends serialize on `cf_lifecycle_mu` instead of on
the registry lock they used before 2.2.

Other safe patterns used: rotation drops `rot` while opening the next WAL file;
commit runs hooks after dropping `commit_mu`, and so does `commit_prepared`.

### Prepared transactions: the registry and the pins (3.2)

Two pieces of state, two locks, and they **never nest**. No path holds one while
acquiring the other; `sweep_wal_gens` takes `wal_gens`, drops it, and only then
takes `prepared` to release retired ids.

**`DbInner::prepared` — the reservation registry.** `Txn::prepare` validates at
the transaction's own isolation level, checks `by_key` for overlap, checks the
id, checks the byte cap, appends and fsyncs the prepare frame, and registers —
all under **one** `commit_mu` acquisition. From that moment the reserved keys are
protected from every other writer, which is what makes the central guarantee
true: *a `prepare` that returns `Ok` cannot subsequently lose a conflict*, so
`commit_prepared` can only fail on durability.

Phase rule 5 is the other half: **every** commit checks the registry, under
`commit_mu`. That extends `commit_mu` to `ReadUncommitted`, `ReadCommitted` and
`RepeatableRead`, which took no lock before 3.2 — including every single-op
`DB::put`/`DB::delete`, which begins a `ReadCommitted` transaction. Putting the
check outside the lock would be a TOCTOU: releasing the registry lock before
taking `commit_mu` leaves a window in which a concurrent `prepare` registers
between an ordinary commit's check and its apply, and first-preparer-wins stops
meaning anything.

The steady state — no prepared transaction anywhere — costs one relaxed load:
`DbInner::prepared_live` is written only under `commit_mu`, so a commit reading
it under the same lock cannot see a stale zero, and the registry mutex is never
touched. The check is also **non-blocking**: it never waits on a prepared owner,
so an abandoned prepare degrades one key range rather than the whole write path.

**Measured cost** (`bench-results/3.2/2026-08-30/`), and it has two faces:

- **Uncontended**: below the measurement floor — median per-op p50 moved 0.3%,
  against a 41–54% run-to-run spread within each arm.
- **8 concurrent writers**: a **~3× throughput regression** (2,206 → 730
  ops/sec median) and 4.1× worse p99. Real, not noise: the base arm varies +92%
  (machine-limited) while the 3.2 arm varies +27% (lock-limited), and the two
  distributions do not overlap.

The regression is *not* the registry probe — that is one relaxed load when no
prepare is outstanding. It is that `commit_mu` is held across the whole apply,
so eight `ReadCommitted` writers that previously ran their WAL append and
memtable insert concurrently now serialize all of it. That is the price rule 5
charges, and it cannot be lowered by moving the check: outside the lock it is a
TOCTOU. Lowering it means shrinking the critical section — write intents or a
second publication protocol — which is RV-M3's job, not 3.2's. This measurement
is what turns RV-M3 from a deferred theoretical item into one with a price.

**Latency (RV-M3, stated not hidden).** `commit_prepared` holds `commit_mu`
across an fsync, inside a reserved-but-unpublished window, so `visible_seq`
cannot advance past the reserved block until the fsync returns — and every
concurrent fixed-isolation `begin`/`reset` meanwhile spins in
`wait_visible_at_own_floor` (`yield_now` in a loop, bounded at one second). This
makes RV-M3 worse, deliberately. The fix, if the numbers demand one, is RV-M3
itself (write intents, or a second publication protocol), not a weakening of the
ordering: the validation-to-apply exclusion `commit_mu` provides is exactly what
the reservation depends on. `commit_prepared` also holds `prepared` across that
fsync, so `list_prepared` blocks for its duration; a resolve-then-write order
would instead let a same-process retry answer `Ok` for a transaction whose
decision never reached disk.

**`DbInner::wal_gens` — the generation pins.** The prepare frame lives in unified
WAL generation `P` and the decision lands in whatever generation `D ≥ P` is
current at resolve time. Pinning only `P` is a **data bug**: `D`'s immutable can
flush and unlink first, and a crash then leaves a prepare with no decision, so
recovery re-registers a reservation for a transaction that already committed and
a coordinator retry applies the whole writeset again at fresh sequences.

So the pin covers the pair. `flush_unified` hands its paths to
`DbInner::retire_wal_paths` instead of unlinking them — **per path, not per
immutable**, because the first immutable after an open carries every replayed
generation plus the new one, and routinely bundles a pinned generation with
unpinned ones. A withheld generation `G` retires when all of:

- **(a)** no *unresolved* prepare has `prepare_gen == G`;
- **(b)** every resolved pair with `prepare_gen == G` has its decision's
  generation flushed or already deleted;
- **(c)** every resolved pair with `decision_gen == G` **and**
  `prepare_gen != G` has had its prepare unlinked already.

Condition (c) is the ordering rule: **the prepare is unlinked before its
decision, never the other way round.** A crash after unlinking `P` leaves a
decision for an unknown id, which recovery treats as a no-op — safe, because `P`
only went once `D` had flushed, i.e. once the applied records were durable in
L0. The `prepare_gen != G` exclusion keeps the predicate from being circular
when a pair shares one generation; those two retire atomically with the file.

The sweep runs after every flush, every resolve and every abort, and returns its
unlink list **in order**, which the caller then performs outside the lock.

A resolved pair's id stays *retiring* — unusable for a new prepare — until both
its generations are unlinked. Reusing it earlier would put two prepare frames
with one id in the replayed set, which recovery cannot disambiguate (and rejects
as `Corruption`).

**Costs the pin imposes,** accepted and documented rather than worked around: a
pinned generation is fully re-replayed on the next open (file absence *is* the
flushed marker — replay has no sequence floor, no generation floor and no
manifest cross-check), which re-inflates the memtable accounting and causes a
redundant rotation and flush after a pinned reopen. Re-insertion is
**observably** idempotent, not structurally so, and the two backends differ:
`SkipMap::insert` replaces at `(key, seq)`, while `ArenaShard::put` always links
a new node that shadows identically. Reads agree; memory and `approx_size` do
not. Every idempotency test therefore runs in **both** feature configurations.

An abandoned prepare pins its generation forever. That is the intended
operator-visible failure mode: `DB::list_prepared` reports id, age and bytes, and
nothing is ever auto-aborted.

**The pin registry and `DeletionPause` stay separate** and do not unify.
`DeletionPause` guards SSTable unlinking for checkpoint/backup (invariant 6,
routed through `remove_sst_file`), while WAL files have always been deleted by a
bare `wal::remove_wal_files` on both flush paths. Merging them would put an
unbounded, coordinator-driven hold on SST deletion.

**Recovery is a two-pass, order-free scan.** The unified WAL is four-striped in
every mode but `SyncMode::Full`, stripe choice is per-thread, and `prepare` and
`commit_prepared` are separate API calls that commonly run on different threads
— so a prepare frame and its decision land in different stripe files with no
recoverable relative order. "Replay frames in order" is not available, and
requiring `SyncMode::Full` to buy it would be a silent, expensive configuration
constraint. Pass 1 (inside `UnifiedStore::open`) collects every prepare and every
decision and inserts **nothing** into the memtable; pass 2
(`DbInner::resolve_recovered_prepares`, called from `build_db_inner` right after
`observe_seq(unified_max_seq)`) matches them. A commit decision raises the
watermark to `commit_seq + count - 1` **before** applying, so a decision whose
records fail to apply has still reserved its sequences.

### Pessimistic transaction locks (3.3)

`DbInner::txn_locks` (`src/txn_lock.rs`) is a per-database table of **point
locks**, taken only by transactions begun with `DB::begin_pessimistic` /
`begin_pessimistic_with_isolation`. An optimistic transaction never touches it:
it takes no lock, waits for none, and validates at commit exactly as before.
The table is empty and untouched in every database that never opts in.

It is **not** `ColumnFamily::range_locks`. That is the compaction/parts span
lock: no per-owner identity, background waiters, span-shaped. This one has an
owner (the transaction id), a deadlock rule, and a hand-off order. The two
never meet.

**Key.** `(cf.id(), key)` — the durable column-family id, not
`txn::cf_id`'s pointer identity. A family dropped and recreated reuses the
allocation (the bug L2 fixed for `THREAD_COMMIT_FLOOR`), and this is also the
key 3.2's reservation registry uses, so the two subsystems index one space.

**Wait-die.** Ids come from one `DbInner::txn_ids` counter, so lower is
strictly older and a tie is impossible. A requester **older** than the holder
waits; a **younger** one dies at once with `Conflict`. Equal ids mean the
holder *is* the requester — a re-entrant acquisition, granted immediately.
Waits therefore always point older→younger, a cycle would need the id order to
cycle, and there is no cycle detection and no lock ordering imposed on callers.
`Txn::reset` re-mints the id: a reset transaction is a new transaction, and a
reused handle that stayed the oldest would win every race forever.

FIFO hand-off costs one extra rule. Waiters queue in arrival order, not age
order, so handing the lock to the front of the queue can leave a *younger*
waiter behind an older new holder — the one edge wait-die forbids, and enough
to deadlock against a second key (holder T5, waiters T2 then T4: T5 releases to
T2, and T4 would now wait on T2 while T2 may already be waiting on T4
elsewhere). A hand-off therefore **denies** every remaining waiter younger than
the new holder, with the same `Conflict` it would have got had it arrived an
instant later. FIFO order survives among the rest.

**Two consequences for callers**, both in the API docs and the release notes: a
conflict can now surface from `put`/`merge`/`get_for_update` rather than only
from `commit`, and **`put` can block** — a caller holding a foreign lock across
one gains a deadlock edge wait-die does not cover, because wait-die orders
transactions, not foreign locks.

**Snapshot refresh on grant — the semantic.** Holding a lock does not move
`read_seq`, so without this the feature buys nothing at the fixed levels: a
transaction waits politely for the hot key, is granted it, and
`validate_write_conflicts` then finds `peek_seq(key) > read_seq` — the
predecessor's write, committed while it waited — and aborts on the very thing
it waited for. Measured: with the refresh disabled the two-thread hot-key test
takes 85 commit conflicts over 2,000 rounds; with it, zero.

So at `Snapshot` and `Serializable` a successful acquisition refreshes the
transaction's snapshot, in this order:

1. The outgoing owner stamps its entry with the sequence it committed at
   (`LockEntry::last_commit_seq`, from `Txn::committed_at`); a rolled-back owner
   stamps nothing. A stamped entry outlives its owner's release while
   publication has not reached it, because the *next* holder of that key needs
   it whether or not it was queued at the time.
2. On grant the new owner waits (bounded, `yield_now`, one second — the shape of
   `wait_visible_at_own_floor`) for `visible_seq() >= last_commit_seq`.
   Publication is gap-free (invariant 5), so this is transient by construction
   and the bound only guards a torn process.
3. At `Serializable`, `validate_read_conflicts` re-runs against the **old**
   `read_seq` first. That is what makes the refresh sound: the reads are proven
   unchanged at the new snapshot, so it is as if they had all happened there.
4. `acquire_snapshot(new)` **before** `release_snapshot(old)`, so
   `oldest_snapshot()` never transiently jumps forward and lets compaction GC a
   version this transaction still needs.

What it costs, stated plainly:

- **A pessimistic `Snapshot` transaction is no longer snapshot-isolated across
  lock grants.** Read skew (G-single) becomes possible where snapshot isolation
  prevented it. That is the documented semantic, not a bug
  (`snapshot_refresh_admits_read_skew` pins it).
- At `Serializable`, step 3 turns the same situation into an *earlier* abort.
  Pessimistic `Serializable` is therefore not conflict-free in general — only
  for write-write contention with an unchanged read set.
- `RepeatableRead` does **not** refresh: it runs no validation, so it has no
  conflict to avoid, and refreshing would break the one thing its contract
  promises. The consequence is that a `RepeatableRead` transaction can still
  lose an update it read before the grant — the lock made the writes ordered,
  but it cannot make a stale read fresh. Locks add ordering, not isolation
  (`repeatable_read_keeps_its_snapshot_and_can_lose_an_update`).
- The conflict-free guarantee holds **between transactions that both take the
  lock**. `peek_seq` reads at `u64::MAX` and can see an in-flight, unpublished
  write, so an ordinary optimistic writer ignoring the lock can still make a
  pessimistic transaction abort. Locks are advisory *pre-commit* coordination;
  MVCC remains the authority.

**Composition with prepared transactions (3.2).** In-memory locks are volatile;
reservations are durable. At `prepare` — inside the same `commit_mu`
acquisition that registers it — the transaction's locks are dropped and every
waiter on them is woken with `Conflict` rather than granted a lock whose commit
is guaranteed to fail against the reservation it cannot see. After a crash only
the reservation exists: a new pessimistic transaction takes the lock and its
*commit* is refused until a coordinator resolves the prepare, so a waiter is
told no rather than left hanging on an owner that is never coming back.

**Release.** `Txn::release` is the one funnel: commit's five returns, rollback,
reset, `Drop`, and the poison check that returns *without* calling release and
relies on `Drop`. Release must be correct when the releasing thread is not the
acquiring one — `Txn` is `Send`. `DB::close` shuts the table down (waiters woken
with `InvalidDb`, later acquisitions refused); fail-stop wakes waiters with
`Poisoned` but leaves the table usable, because a poisoned database still lets a
transaction buffer writes and fail at `commit` and 3.3 does not move that
failure earlier.

**Measured cost, and it is the uncomfortable one**
(`bench-results/3.3/2026-08-31/`). The conflict-free guarantee holds everywhere
— zero commit conflicts in all 20 benchmark runs against thousands optimistic —
but on retry-corrected *throughput* a tight begin-acquire-commit loop on one key
is **0.61x** at two threads and **0.17x** at eight. The reason is wait-die's
direction: it kills the **younger** requester, ids increase, and a tight-loop
requester is essentially always the younger party, so the "older waits" arm
almost never runs and the mode degenerates into spin-abort-retry. Give the
transaction some life before it asks for the lock (sixteen unrelated reads) and
ages mix, deaths per successful round fall from ~33 to ~5.6, throughput reaches
parity and p99 improves 3.8x. **So: use pessimistic mode for transactions that
do real work around a contended key, not for a one-key increment loop** — and
do not "fix" the loop case by tuning. The two things that would change it are
design changes, both out of scope: retaining a restarted transaction's timestamp
(the textbook wait-die formulation, which the plan explicitly rejected for
`reset`), or switching to wound-wait.

**Not in v1:** span locks (`lock_range`), and therefore range deletes take no
lock at all — a point lock on a start bound would protect one key while reading
as if it protected the span. Phantom protection is unchanged and still out of
scope.

### Range deletes and the committed-span index (1.2)

The index answers the one question a range delete needs and `peek_seq` cannot:
*did anything in `[start, end)` change since my snapshot?* It is **inert until
`CAP_RANGE_DELETES` is enabled** — `DbInner::span_index()` returns `None`, and
the commit path pays one relaxed load of the capability word.

Three rules, in the order they matter:

1. **`span_index` sits after `commit_mu` and before `manifest_mu`.** It is
   acquired while `commit_mu` is held (the conflict check and the marker
   insert, which is what makes them atomic against each other) and alone
   (pruning, and the capacity reservation). It is never acquired before
   `commit_mu` *on the commit path*.

2. **The capacity wait happens BEFORE `commit_mu`, with no other lock held.**
   Waiting under `commit_mu` would stall every Snapshot/Serializable commit in
   the database behind one range writer, and could convoy against the pruner.
   The reservation is an RAII guard: unconsumed slots go back on every exit
   path, including a conflict, an apply failure and a panic.

3. **A commit containing a range delete takes `commit_mu` even at
   ReadCommitted.** Point-only commits keep today's behavior exactly. This
   makes deferred review item **M3** (`commit_mu` latency) measurably worse for
   range commits, and that is accepted: range commits are the rare, bulk
   operation, and the alternative is a per-key conflict domain the engine does
   not have. `bench-results/1.2/` carries the baseline M3's eventual fix will
   be measured against.

**Bounded, never blocking a point write.** Capacity is freed by pruning, pruning
is driven by the oldest live snapshot, and nothing guarantees that snapshot ever
advances — a Snapshot transaction can even be its own prune floor. So the wait
is bounded three ways: it is skipped outright when the caller's own snapshot is
the floor, it expires after `CAPACITY_WAIT`, and a **point** commit never waits
at all. A commit that cannot be indexed raises an *overflow watermark* instead:
both conflict checks report a conflict for any reader at or below it. That is
conservative in exactly one direction — it can refuse a commit that would have
been safe, never admit one that would not.

**Serializable read validation asks the index too.** A range delete leaves no
point version at the keys it covers, so `peek_seq` over the read set cannot see
it; `validate_read_conflicts` therefore also runs `point_conflict` for every key
the transaction read. That is a point check over keys actually read, not
phantom protection, and it inherits the overflow watermark: a Serializable
transaction reading below it conflicts, like any point writer would. The check
runs wherever read validation does — at commit, at prepare, and on a
pessimistic lock grant (there without `commit_mu`, taking the index mutex
alone, which rule 1 permits).

The index holds no durable state. A reopen with no active transactions starts
empty, which is correct: every marker described a window that no live
transaction can still be reading in.

### Range fragments and the read paths

`RangeTombstoneSet` (`range_tombstone.rs`) sits beside the point shards in each
`Memtable`, under its own `RwLock`, and is **never sharded by start key** — a
lookup for `k` must find every covering span, and a hash on `start` scatters
exactly the spans that could cover it. `is_empty()` is one relaxed load, which
is the gate every read path checks first.

Scan cursors (`RangeMask`) hold **owned** bounds copied out of their source, so
invariant 8's pinned-block lifetimes are untouched: a fragment cursor never
borrows from a `Block`. The mask is built from the **same** `state` snapshot as
the iterator's children, so a flush landing mid-construction cannot leave a scan
reading points whose covering fragments it never collected.

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
### Compaction span workers and their permit pool (0.8)

`Options::max_subcompactions` (default `1`) lets **one** compaction job split
its user-key range into half-open spans and merge them concurrently. The shape
is deliberately narrow:

- **Boundaries are user keys and the spans are half-open**, so every version of
  one user key lands in exactly one span. That is what makes each span's own
  `VersionRetention` correct: it never sees a partial version chain.
  `plan_spans` is pure over table metadata — partition cuts where the job writes
  bottom output, target-table `min_key`s otherwise — and is comparator-aware
  throughout. A partitioned job under a non-bytewise comparator stays
  single-span, because "a partition boundary prefix is the first key of its
  partition" is a bytewise argument.
- **Job-wide decisions are frozen once**, before any span starts, in `FrozenJob`
  (`bottom`, `oldest_snapshot`, `now`, `carry_entry_time`, the partition
  resolver, the compaction filter). Two spans re-deriving `bottom` or the
  resolver independently could straddle a concurrent change and cut differently
  out of one input set.
- **Span 0 runs inline on the coordinator's own thread**; the rest run on
  `std::thread::scope` threads named `onda-span-{n}`, so every one of them is
  joined before the job returns — on the error path as much as the happy one.
  Each sets `IoClass::Compaction` with an `ioctrl::scoped` guard at entry: a
  fresh thread defaults to `Foreground`, and without the guard every byte a span
  moved would escape the 0.6 limiter.
- **One shared cancel flag** in `FrozenJob`, polled once per entry. The first
  failure sets it; siblings stop rather than finish megabytes of doomed output.
- **One install.** Spans hold no lock of their own — the job's single
  `RangeGuard` already excludes everyone else from the whole span — and nothing
  reaches the level set until every span has succeeded: outputs are concatenated
  in span order, debug-asserted sorted and disjoint, installed with **one**
  `install_compaction_outputs`, then persisted with one `persist_manifest`.
  A reader therefore sees either all of the inputs or all of the outputs.
  Failure anywhere removes every output file directly (none reached the
  manifest): a partial span through `CompactionOutputBuilder`'s abort-on-drop, a
  *finished* sibling's tables explicitly, since `finish()` has already taken
  them out of the builder's reach.
- **Persist failure rolls the install back** (`rollback_compaction_outputs`)
  before the outputs are unlinked, so the level set goes back to naming the
  inputs the manifest still names. Inputs are re-inserted at their own recorded
  level rather than from a pre-install snapshot, which would also undo whatever
  a concurrent flush added meanwhile.

**The permit pool is sized independently of the compaction worker count, and a
coordinator consumes nothing from it.** `DbInner::span_permits` holds
`max_subcompaction_workers` permits (default: `num_compaction_threads`), and a
job takes `spans - 1` of them. The coordinator is an `onda-compact-{n}` thread
that is going to do a share of the merge itself, and `num_compaction_threads`
already accounts for it. Sizing one pool for both silently no-ops at the
defaults: with two compaction threads, two concurrent jobs would consume both
permits as coordinators and no span worker could ever run
(`coordinators_do_not_consume_span_permits` pins this).

Acquisition **never blocks**. A job takes what is free and degrades to fewer
spans, ultimately to one, so the background-wait rule above is not stretched any
further: no thread ever parks on this pool while holding its range lock.
Permits are released when the job joins its workers, including on the error
path, and the surplus is released early when the boundary planner finds fewer
useful cuts than the permits allow.

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

## Delete-only excise (`excise.rs`, 1.2)

Excise removes catalogued tables without reading them, so its whole safety
argument is about what it is allowed to observe and when. Ordering: plan over a
level snapshot with no lock held → `parts::try_lock_key_span` over the union of
the candidates' **span** bounds (`meta.span_min`/`span_max`, so a fragment
reaching past a point bound is inside the lock) → **replan under that lock and
drop to the intersection** → one `RemoveTables` edit through `catalog_txn` →
`compaction::retire_tables`, which is `DbInner::remove_sst_file` per file half
(invariant 6: defer-aware, so a checkpoint in progress keeps the bytes).

The lock is taken non-blocking on purpose. An overlapping holder is a
compaction or a part operation that may well rewrite or delete the same bytes
itself, and excise is opportunistic: `Ok(0)` is a normal answer. The same is
true of the `parts_in_flight` gate, of a foreign mount overlapping the
candidate's span, and of a live snapshot below the tombstone's sequence.

In-flight reads are unaffected for the reason compaction already relies on:
they finish on the `Arc<SstHandle>`s and pinned blocks they already hold. Excise
is therefore **not snapshot-consistent** in the same sense `detach_part` is not
— but unlike `detach_part` it never changes an answer, because it only removes
tables every one of whose keys a visible tombstone already deletes.

A failed catalog transaction **fail-stops the database**. With the edit log on,
nothing was published and a reopen reads the pre-excise catalog; without it, the
pre-capability path published before the snapshot write (it has to — the
snapshot is rebuilt from live state), so the in-memory view is already the
post-excise one and a later snapshot may make it durable. Both states are
consistent catalogs whose files were never unlinked, and both are exactly what
`detach_part` has always produced.

## Part lifecycle & the part mover (`parts.rs`)

Ordering all part operations follow: ONE `VersionEdit` appended and fsynced by
`DbInner::catalog_txn` (the crash-atomic commit point since 2.2; before it, the
whole-manifest rewrite `persist_manifest` still performs when
`CAP_MANIFEST_EDITS` is off) → the in-memory swap under `state.write()`, inside
that transaction's publish closure → only then touch files.

Every part operation also holds a `DbInner::begin_parts_op` guard for its whole
duration, bumping an `AtomicU64`. It is not a lock and excludes nothing among
the part operations themselves — their range locks do that. It exists so
delete-only excise (1.2) can decline to run beside phases those range locks do
not span end to end: `attach_part` copies bytes before it knows which range it
will claim, and a tier move flips a manifest entry after its files have already
been relocated.
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

The periodic-compaction scan (0.3, `DbInner::run_periodic_scan`) shares the same
worker and the same shape: `periodic_running` is an `AtomicBool` guarded by
`compare_exchange(false, true, SeqCst, SeqCst)` before the pass and
`store(false, SeqCst)` after, mirroring `mover_running` field for field. Without
it every compaction worker would independently walk the same levels and enqueue
the same column family each derived interval. The pass holds no lock of its own:
it reads level snapshots through `with_levels` and only *sends* on the compact
channel, so the picker re-derives eligibility under its normal locks. The
`DbInner::clock` it reads is a `Mutex<ClockFn>` whose closure is cloned out
before it is called, so a clock cannot deadlock on its own lock.

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
A compaction worker may additionally spawn scoped `onda-span-{n}` threads for
the length of one job (0.8, off by default) — they are joined inside the job,
so they never outlive it and `close` has nothing extra to wait for.
`DB::close`: set `closing` → **cancel the background IO limiter** (0.6: so the
drain below cannot wait on a rate) → rotate every CF (+unified) with `force` →
spin until `pending_flush == 0` → set `stop`, join workers → final
`persist_manifest` → close WALs/readers. `DB::clone` increments an explicit
public-handle count; `DB::drop` closes only when that count reaches zero.
Worker-held `Arc<DbInner>` references therefore cannot keep the directory lock
after the final public `DB` handle is dropped.
