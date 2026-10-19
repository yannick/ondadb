# 3.4 — Reversing the 3.2 and 3.3 commit-path regressions

**Readiness:** design required; every option below needs a measurement before it
is chosen, and two of them need a correctness proof this document only sketches.
**Prerequisites:** 3.2 and 3.3 landed (0.9.0). **Effort:** 1–2 dev-weeks for
option A1 alone; 4–8 for the full set. **wavesdb counterpart:** none — this is
ondaDB's own debt.

## Why this document exists

0.9.0 shipped two features whose measured cost is worse than their plan
predicted, and both were shipped knowingly rather than quietly:

| | measured | where |
|---|---|---|
| 3.2 | 8-thread commit throughput **2,206 → 730 ops/sec** (~3×), p99 **20 → 83 ms**; free at 1 thread | `bench-results/3.2/2026-08-30/` |
| 3.3 | 8-thread hot-key retry-corrected throughput **0.17×**; ~33–54 wait-die deaths per successful round | `bench-results/3.3/2026-08-31/` |

Both are *design* consequences, not tuning mistakes, and neither is fixed by a
knob. This document records what would actually reverse them, ordered by
cost-to-benefit, so the next person does not have to re-derive it.

Neither regression makes a database incorrect, and both are opt-in at the level
that matters: 3.3 requires `begin_pessimistic`, and 3.2's cost — see A1 — is
paid by every writer only because the current implementation cannot tell that
nothing is prepared.

---

## A. The 3.2 regression: `commit_mu` on every commit

### What actually costs

The reservation check itself is **already free**: with nothing prepared it is a
single relaxed load of `DbInner::prepared_live` and never touches the registry
mutex (measured, `bench-results/3.2/2026-08-30/README.md`). The entire
regression is that phase rule 5 extended `commit_mu` to `ReadUncommitted`,
`ReadCommitted` and `RepeatableRead` — which took **no** commit lock before 3.2
— and that the lock is held across the whole apply, WAL append and memtable
insert included. Eight threads that previously ran those concurrently now
serialize on one mutex.

That framing matters, because it means the cheapest fix does not touch the
validation protocol at all.

### A1. Do not take the lock when nothing is prepared *(recommended first)*

If no prepared transaction exists, there is no reservation to check, so a
`ReadCommitted` commit needs no reservation check and therefore no `commit_mu`
— it can take the byte-for-byte pre-3.2 path. The whole problem is the race: a
`prepare` could land between a committer's decision to skip the lock and its
apply.

Resolve it the way this engine already resolves the same shape. Invariant 9's
rotation protocol has writers hold `active_writers` for the whole of
`apply_commit` while rotation *waits for the drain* before swapping the
memtable. Apply that asymmetry here:

- a lock-free committer increments an `active_committers` counter, re-checks
  `prepared_live`, applies, decrements;
- `Txn::prepare` takes `commit_mu` **and** waits for `active_committers` to
  reach zero before it registers its reservation.

Prepares are rare and committers are hot, so the cost lands entirely on the rare
side. The re-check after the increment is what closes the window: a prepare that
has not yet drained cannot have registered, and one that has registered is
visible to the re-check.

**Recovers:** the whole regression for every database with no live prepare —
which is every database that does not use 3.2 at all, i.e. the population that
is paying for it today for nothing.
**Does not recover:** anything for a database actively running 2PC. That is B.
**Risk:** the drain protocol must be got exactly right; a missed re-check is a
lost conflict, which is the failure class this engine cares most about. It needs
the same treatment as the rotation protocol — a written proof in
`docs/concurrency-and-safety.md` and a dedicated stress test, not just passing
tests.
**Effort:** 1–2 weeks including the proof.

### A2. A lock-free reservation probe, for databases that do use 2PC

With live prepares, A1 degrades to today's behaviour. Make the registry a
sharded lock-free set keyed by `(cf_id, key)` with a generation counter, probe
it *before* taking `commit_mu`, and re-validate inside only when the generation
moved. Commits whose keys miss every reservation — the overwhelming majority
even under active 2PC — never contend with each other.

**Risk:** the generation counter is a global write point and becomes the new
contention if prepares are frequent. Shard it too, or accept that a
prepare-heavy workload serializes.
**Effort:** 2–3 weeks.

### A3. Shrink the critical section: validate under the lock, apply outside

This is **RV-M3 proper**, open since the August 2026 review and still the only
option that helps a workload with genuine write-write contention. The apply
(WAL append, memtable insert) is the expensive half; validation is cheap.
Moving apply outside the lock requires that a later validator can see an
*in-flight* commit's writes, which is the write-intent design the review's
disposition already names: on reserving its sequence range a commit publishes
its key set into a sharded intent table, validation consults committed state ∪
intents, and publish removes them.

**Risk:** the intent table is a new concurrent structure on the hot path, and
"validation consults intents" has to be exhaustive or a conflict is lost. This
is a protocol change, not a refactor.
**Effort:** 4–6 weeks. Do A1 first and re-measure; A1 may make A3 unnecessary
for every workload anyone actually runs.

### A4. Shard `commit_mu` — *considered and not recommended*

Per-CF sharding is trivial and buys nothing for the measured case, which is a
single column family. Per-key-range sharding with an ordered multi-acquire is
deadlock-free and would help disjoint-key workloads, but the probe that found
this regression is `DB::put` across 8 threads on one family, and Serializable's
read-set validation is not partitionable by key range anyway. Recorded so it is
not re-proposed.

---

## B. The 3.3 regression: wait-die kills the requester that should wait

### Why it happens

Wait-die kills the *younger* requester. `txn_id`s increase monotonically and are
re-minted on `reset`, so a freshly-begun transaction is younger than any holder
that has been alive long enough to hold a lock. In a tight
begin → acquire → commit loop **every** requester is the younger party, the
"older waits" arm essentially never runs, and pessimistic mode degenerates into
spin-abort-retry: 33 deaths per successful round at two threads, 54 at eight.

The correctness criterion still holds — zero *commit* conflicts, against
thousands optimistically — because the deaths are acquisition conflicts. The
feature does what it claims; it just costs more than it saves in that shape.

### B1. A transaction holding no locks may always wait *(recommended first)*

A wait-for edge runs from a waiter to the holder it waits on. A transaction that
holds **no** locks has no incoming edges, so it cannot be part of a cycle, so
letting it block is deadlock-free **regardless of age**. Wait-die's death is
unnecessary there.

So: the **first** lock acquisition of a transaction always waits in FIFO order;
wait-die applies only from the second acquisition onward, when the transaction
can actually close a cycle.

In the measured tight loop every requester holds exactly zero locks at its one
acquisition, so this takes acquisition deaths to **zero** and leaves the shape
the plan predicted. It also covers the common `get_for_update(k); … ; commit`
transaction, which only ever holds one lock.

**Verify before implementing** — this is the claim the whole option rests on:
- The proof assumes wait-for edges are created *only* by lock acquisition. If a
  transaction can be blocked by anything else another transaction holds (a
  prepared reservation, the span-index capacity wait, `commit_mu` itself), that
  edge has to be in the graph too, and the argument needs redoing over the union.
- It interacts with the hand-off rule 3.3's implementation added — a hand-off
  denies remaining waiters younger than the new holder, precisely to prevent a
  younger-waits-on-older edge. Under B1 that edge is *legal* for a zero-lock
  waiter, so the hand-off rule needs a matching exemption, and the two rules must
  be shown consistent rather than assumed to be.

**Effort:** 1 week plus the proof and a randomized multi-key deadlock stress
(3.3 already has one at 10⁶ acquisitions; extend it to mixed zero-lock and
multi-lock requesters).

### B2. Retain the age of a *retrying* transaction

Textbook wait-die requires a restarted transaction to keep its original
timestamp; without that it is livelock-prone by construction, which is exactly
what was measured. The 3.3 plan rejected age retention for `reset` for a sound
reason — a reused long-lived handle would get younger forever and starve
everyone — but that conflates **handle reuse** with **retry after abort**.

Make it explicit and opt-in: `DB::begin_retry_of(prior_id)` (or
`Txn::inherit_age`), so a caller retrying a killed transaction keeps its age and
a caller merely reusing a handle does not. This restores wait-die's forward-
progress guarantee: a repeatedly-killed transaction eventually becomes the
oldest live one and cannot be killed again.

**Complements B1 rather than replacing it.** B1 removes the deaths that should
never have happened; B2 bounds how long a transaction that *does* legitimately
die can keep dying.
**Effort:** 1–2 weeks. The API is small; the starvation-freedom argument is the
work.

### B3. Wound-wait instead of wait-die

Invert the rule: an older requester **wounds** (aborts) the younger holder and
takes the lock; a younger requester waits. The waiting party becomes the young,
fresh transaction — the tight loop's shape — so deaths move to the rarer
old-requester case. Also deadlock-free.

**Cost:** wounding needs a cancellation path the engine does not have. A wounded
holder may already have buffered writes and be mid-API-call; it has to be marked
and made to fail at its next call, and every acquisition and commit site has to
check that mark. That is a larger change than B1 and B2 together, and it changes
the caller contract (a transaction can now be aborted by someone else, where
today only its own operations can fail).
**Effort:** 3–5 weeks. Measure B1+B2 first; wound-wait may buy nothing over them.

### B4. Bounded wait before dying — *considered, with a caveat*

Let a younger requester wait up to a short deadline before dying. Cheap and
mechanical, but it downgrades the guarantee from "deadlocks never form" to
"deadlocks resolve within the deadline", and a deadline long enough to help is
long enough to hide a real deadlock. Named so the trade is explicit; not
recommended ahead of B1.

---

## Ordering and acceptance

Do **A1** and **B1** first: they are the cheapest, they target the exact
measured shapes, and each is expected to recover most of its regression on its
own. Re-measure before starting anything else — A3 and B3 are multi-week
protocol changes that may turn out to be unnecessary.

Acceptance, in both feature configurations, on a **quiet** host (0.9.0's numbers
were taken with sibling builds running; see `docs/performance.md`):

- **A1** — 8-thread `DB::put` throughput within noise of `41b4d84` (pre-3.2)
  with no prepared transaction outstanding; unchanged 3.2 semantics with one
  outstanding, proven by the existing `tests/prepared_*.rs` suites plus a new
  test that races `prepare` against in-flight lock-free committers.
- **B1** — zero acquisition deaths in the two-thread hot-key probe; retry-
  corrected throughput ≥ 1.0× the optimistic arm in the tight loop (today
  0.61×/0.17×); the randomized multi-key stress still shows no deadlock and no
  hang.
- Both — every existing invariant test green, and the concurrency proof for
  whichever of A1/B1 lands written into `docs/concurrency-and-safety.md`
  alongside the rotation protocol it is modelled on.

If A1 lands, RV-M3's disposition in
[`../../../code-review-2026-08-resolution.md`](../../../code-review-2026-08-resolution.md)
should be updated: the finding stays open for genuinely contended writers, but
the population paying for it shrinks to those actually running 2PC.
