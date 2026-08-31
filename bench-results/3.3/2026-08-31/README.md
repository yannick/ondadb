# 3.3 — hot-key contention: optimistic abort-retry vs pessimistic wait-serialize

Date 2026-08-31. Host `Darwin 25.5.0 arm64`, 12 cores, release build with
`unsafe-fastpath`. Probe source: `hotkey-probe.rs.txt`. Raw data:
`raw-2thread.txt`, `raw-8thread.txt`, `raw-8thread-yield.txt`,
`raw-4thread-delayed.txt`.

**Read the headline before the tables, because it is not the one the feature
plan predicted.**

- **The correctness contrast is absolute and holds in every configuration
  measured.** A pessimistic transaction that is granted the lock never loses its
  commit: **zero** commit conflicts in all 20 pessimistic runs, against
  2,482-30,317 in every optimistic run. That contrast — not a timing number —
  is what the feature is for, and it is also asserted unconditionally by
  `pessimistic_hot_key_serializes_without_conflict` and its control
  `optimistic_hot_key_aborts`.
- **Throughput does not follow.** In the plan's own shape — a tight
  begin/acquire/commit loop on one key — pessimistic mode is **0.61x** at two
  threads and **0.17x** at eight. In a realistic transaction shape (sixteen
  unrelated reads between `begin` and the acquisition) it is **1.25x** at four
  threads, but the two distributions overlap, so that gain is *not* decisive on
  this machine.
- **The tail is the real win, where there is one.** p99 improves **3.2x** at two
  threads and **3.8x** in the realistic shape, with non-overlapping ranges in
  both. It is 3x *worse* in the eight-thread tight loop.

## Why the tight loop loses, and it is a design consequence, not a bug

Wait-die kills a requester **younger** than the current holder. Transaction ids
increase monotonically, and `Txn::reset` re-mints a *younger* one by design
(feature plan, "Transaction identity"), so in a tight begin-acquire-commit loop
the requester is essentially always the younger party. The "older waits" arm of
wait-die therefore almost never runs, and pessimistic mode degenerates into a
**spin-abort-retry** loop that is strictly worse than the optimistic
abort-retry it was meant to replace — same aborts, plus a lock table.

The numbers say exactly that. Acquisition deaths **per successful round**:

| Shape | deaths/round |
|---|---:|
| 2 threads, tight | ~33 |
| 8 threads, tight | ~54 |
| 8 threads, tight, `yield_now` on death | ~26 |
| 4 threads, 16 pre-reads | ~5.6 |

The realistic shape lets a transaction age before it asks for the lock, ages
mix, the wait arm runs, and deaths drop by an order of magnitude — which is
where the p99 win and the throughput parity come from.

**What would change this, stated so nobody has to rediscover it:** the textbook
formulation of wait-die has a restarted transaction **keep its original
timestamp**, so it grows older relative to everyone else and eventually wins
rather than dying forever. This implementation deliberately does the opposite
(the plan: "a reset transaction is a new transaction and must be younger, or a
reused handle would starve everyone else forever"). The alternative worth
measuring is **wound-wait** — younger waits, older preempts — under which the
tight-loop requester waits instead of dying and the plan's predicted result
would plausibly appear. Neither is a tuning knob; both are design changes, and
neither was in scope here.

## What is measured

Both arms are the **same binary against the same engine**; the only difference
is `db.begin()` versus `db.begin_pessimistic()` (and `get` versus
`get_for_update`). Runs alternate optimistic/pessimistic/optimistic/... so
thermal drift and the other agents on this host are shared between the arms
rather than attributed to one.

- **Retry-corrected throughput** — *successful* rounds per second. Counting
  attempts would flatter the optimistic arm, which does its aborting fast.
- **Retry-corrected latency** — per round, from the first attempt to the
  successful commit, retries included. Per-attempt latency hides the storm.
- **Aborts, split two ways.** Commit conflicts are what the feature removes.
  Acquisition conflicts are wait-die killing a younger requester — a real cost
  the caller pays as a retry, and sweeping the two into one bucket would be the
  whole finding lost.

`SyncMode::None` (the default) and a 1 GiB write buffer, for the reason 3.2's
probe used them: an fsync per commit measured 2.6 ms per operation against a
commit path whose median is single-digit microseconds, and flush backpressure
puts background IO in the tail.

## Results — 2 threads, tight loop (20,000 rounds/thread, 5 pairs)

| Run | opt ops/s | pess ops/s | opt p99 (µs) | pess p99 (µs) | opt commit conflicts | pess commit conflicts |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 34,949 | 86,501 | 251 | 66 | 2,482 | 0 |
| 2 | 151,428 | 114,375 | 172 | 52 | 7,878 | 0 |
| 3 | 160,011 | 96,870 | 169 | 49 | 5,530 | 0 |
| 4 | 168,117 | 103,599 | 135 | 42 | 4,561 | 0 |
| 5 | 170,924 | 93,917 | 102 | 55 | 4,278 | 0 |
| **median** | **160,011** | **96,870** | **169** | **52** | **4,561** | **0** |

Pessimistic is **0.61x** on throughput and **3.25x better** at p99. Run 1's
optimistic figure (34,949) is a cold-start outlier — it is the first arm of the
first pair, and every later optimistic run is 4-5x higher; it is left in the
table rather than deleted, and the median is taken over all five.

## Results — 8 threads, tight loop (5,000 rounds/thread, 5 pairs)

| Run | opt ops/s | pess ops/s | opt p99 (ms) | pess p99 (ms) |
|---:|---:|---:|---:|---:|
| 1 | 125,244 | 21,511 | 1.48 | 4.11 |
| 2 | 146,617 | 20,574 | 1.22 | 4.16 |
| 3 | 107,997 | 21,061 | 1.37 | 4.14 |
| 4 | 156,448 | 21,659 | 1.05 | 4.10 |
| 5 | 99,180 | 21,301 | 2.21 | 4.24 |
| **median** | **125,244** | **21,301** | **1.37** | **4.14** |

**Pessimistic is 5.9x slower and 3.0x worse at p99.** This is not noise: the
pessimistic arm varies by +5% across its five runs (it is limited by the death
loop, which is indifferent to what else the machine is doing) while the
optimistic arm varies by +58% (machine-limited). The two ranges do not come
close to overlapping. Commit conflicts: 3,434-13,780 optimistic, **0**
pessimistic, in every run.

With `yield_now()` on each wait-die death (`raw-8thread-yield.txt`) the
pessimistic median rises to 27,601 ops/s — a 30% improvement, and still **4.0x
below** that run set's optimistic median of 111,002. p99 is unchanged at 3.0x
worse (4.08 ms against 1.36 ms). Backing off is worth doing and does not change
the conclusion.

## Results — 4 threads, realistic shape: 16 pre-reads (5,000 rounds/thread, 5 pairs)

| Run | opt ops/s | pess ops/s | opt p99 (µs) | pess p99 (µs) | opt commit conflicts |
|---:|---:|---:|---:|---:|---:|
| 1 | 62,839 | 78,787 | 770 | 201 | 27,551 |
| 2 | 59,056 | 85,174 | 815 | 140 | 26,200 |
| 3 | 80,043 | 85,352 | 373 | 149 | 30,317 |
| 4 | 74,028 | 65,913 | 503 | 322 | 29,167 |
| 5 | 59,950 | 60,070 | 909 | 337 | 26,411 |
| **median** | **62,839** | **78,787** | **770** | **201** | **27,551** |

- **Throughput 1.25x, and NOT decisive.** The ranges overlap (optimistic
  59,056-80,043 against pessimistic 60,070-85,352), which on a machine with this
  noise floor means "no measured difference", not "25% faster". A valid claim
  would need the re-run described below.
- **p99 3.8x better, and decisive.** The ranges do not overlap (optimistic
  373-909 µs against pessimistic 140-337 µs), and the direction is the same in
  all five pairs.
- Commit conflicts: 26,200-30,317 optimistic against **0** pessimistic, in every
  pair.

## What a valid re-measurement would need

The throughput comparisons in the two-thread and four-thread tables are the ones
this host cannot settle. Ten or more A/B pairs on an otherwise idle machine
(this run shared the host with roughly ten sibling agents), interleaved as here,
reporting medians with the full min-max range, and discarding the first pair as
warm-up rather than carrying it as an outlier. The eight-thread result and every
conflict count are decisive as they stand and do not need re-running.

## Acceptance, judged honestly

| Criterion | Verdict |
|---|---|
| Zero `Conflict`s in the two-thread hot-key `Snapshot` test, non-zero in the optimistic control | **Met**, unconditionally, in the test and in all 20 benchmark runs |
| Wait-die: younger dies, older waits, no deadlock under randomized multi-key stress | **Met** (`randomized_multi_key_stress_never_deadlocks`, 10^6 acquisitions; `pessimistic_multi_key_stress_never_hangs`) |
| Locks released on commit / rollback / drop / cross-thread drop / panic unwind / poisoned commit; `close` wakes waiters | **Met** (`tests/txn_lock_release.rs`) |
| Feature opt-in and off by default regardless of the numbers | **Met** — nothing changes for a caller that does not call `begin_pessimistic` |
| Hot-key phase shows wait-serialize beating abort-retry on retry-corrected throughput | **NOT met.** Decisively lost in the tight loop (0.17x at eight threads); at parity, not decisively better, in the realistic shape. The cause is understood and stated above |
| ...and on p99 | **Met in two of the three workload shapes** — 3.25x at two threads, 3.8x in the realistic shape, both with non-overlapping ranges — and **lost in the eight-thread tight loop**, 3.0x worse with or without a yield on death |
