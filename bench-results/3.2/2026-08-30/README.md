# 3.2 — the cost of rule 5's `commit_mu` extension on the ordinary commit path

Date 2026-08-30. Host `Darwin 25.5.0 arm64`, release build with
`unsafe-fastpath`. Raw data: `rule5-1thread.txt`, `rule5-8thread.txt`. Probe
source: `rule5-probe.rs.txt`.

Phase rule 5 says every commit checks the prepared-transaction reservation
registry, under `commit_mu`. That extends `commit_mu` to `ReadUncommitted`,
`ReadCommitted` and `RepeatableRead` — which took **no** commit lock before 3.2
— and therefore to every single-op `DB::put` / `DB::delete`, since those begin a
`ReadCommitted` transaction. The phase plan's exit criteria require that cost to
be measured and published rather than assumed. This is that measurement.

**Headline, and it is not the comfortable one.** The cost has two very different
faces:

- **Uncontended (1 thread): free, or near enough.** Median per-op p50 differs by
  0.3% between the arms, while each arm's own six runs vary by 41–54%. The
  effect is below this machine's noise floor.
- **Contended (8 threads): a ~3× throughput regression, and it is real.** Median
  throughput falls from 2,206 to 730 ops/sec and median p99 rises from 20 ms to
  83 ms. This is not noise — see the variance argument below — and it is the
  single most important number in this document.

The reason is that `commit_mu` is held across the whole apply, not just the
check. Before 3.2 a `ReadCommitted` commit took no lock and ran its WAL append
and memtable insert concurrently with every other writer; now eight threads
serialize all of that on one mutex. Rule 5 buys the reservation guarantee at
exactly that price, knowingly (`docs/plans/phase-3-transactions/plan.md` rule 5:
"Consequences, to be accepted knowingly and **measured**, not assumed").

## What is being compared

- **A (base)** — `roadmap/wave-a`, `41b4d84`, the commit this feature branched
  from. A `ReadCommitted` commit takes no lock and runs no validation.
- **B (3.2)** — this branch. Every commit takes `commit_mu`; inside it, the
  reservation check runs. With no prepared transaction outstanding that check is
  a single relaxed load of `DbInner::prepared_live` and never touches the
  registry mutex.

Both arms are the same probe binary compiled against the two engine trees, each
with its **own** `CARGO_TARGET_DIR`. That detail is load-bearing: this host sets
`CARGO_TARGET_DIR` globally, so a first attempt had both builds writing to one
directory and silently overwriting each other. The two binaries are byte-different.

## Why a purpose-built probe and not `onda_bench`

`onda_bench` populates with `SyncMode::Full` at the column-family default, which
puts an fsync on every commit: it measured 1,526 ops/sec, i.e. ~2.6 ms per
operation against a commit path whose median is ~8 µs. Over 99% of each
operation was the fsync, and the lock would have been invisible underneath it.
The question here is the cost of the *commit path*, so the probe uses
`SyncMode::None`.

The probe (`rule5-probe.rs.txt`) does exactly one thing: N single-op
`DB::put` calls across T threads, timing each individually, and reports
throughput plus per-op p50/p99/p999. It also sets `write_buffer_size` to 1 GiB
so the run never rotates — at the default buffer, flush backpressure produced a
p50 of 12.8 µs against a p99 of 24 ms, i.e. the tail was background-IO latency
and had nothing to do with the commit path.

Runs alternate A/B/A/B so drift and external load are shared between the arms
rather than attributed to one.

## Results — 1 thread (uncontended lock)

20,000 ops, 6 runs per arm. Per-op p50, nanoseconds:

| Run | A (base) | B (3.2) |
|---:|---:|---:|
| 1 | 7,125 | 7,250 |
| 2 | 7,542 | 8,375 |
| 3 | 10,042 | 8,959 |
| 4 | 8,167 | 8,041 |
| 5 | 8,333 | 10,750 |
| 6 | 8,292 | 7,000 |
| **median** | **8,230** | **8,208** |
| min–max | 7,125–10,042 (+41%) | 7,000–10,750 (+54%) |

B's median is **0.3% below** A's — that is, the two are indistinguishable, and
the sign is meaningless. Each arm's own spread is more than a hundred times the
difference between them.

This is the expected shape. What rule 5 adds to an uncontended commit is one
`parking_lot::Mutex` acquire/release plus one relaxed atomic load: on this class
of hardware roughly 20–25 ns, against a p50 commit of ~8,200 ns. That is ~0.3%
in principle, and ~0.3% is precisely what cannot be measured here.

## Results — 8 threads (contended lock)

40,000 ops across 8 threads, 3 runs per arm (see "Why only three pairs" below):

| Run | A ops/sec | B ops/sec | A p99 (ms) | B p99 (ms) |
|---:|---:|---:|---:|---:|
| 1 | 1,654 | 730 | 26.9 | 82.8 |
| 2 | 2,206 | 764 | 20.1 | 80.0 |
| 3 | 3,171 | 601 | 18.8 | 90.8 |
| **median** | **2,206** | **730** | **20.1** | **82.8** |
| min–max | 1,654–3,171 (+92%) | 601–764 (+27%) | | |

**B is 3.0× slower in throughput and 4.1× worse at p99.**

### Why this is a real effect and not noise

The variances point in opposite directions, and that is the argument:

- **A varies by +92%** (max/min) across its three runs — it is *machine-limited*, so it
  inherits every scheduling accident from the ten other agents on this host.
- **B varies by +27%** — it is *lock-limited*, so its throughput is pinned by
  the duration of one serialized critical section and is largely indifferent to
  what else the machine is doing.

A confound would have to inflate B and *stabilise* it at the same time, run
after run, while leaving A both faster and wildly variable. Serialization
explains that shape exactly; external load does not. B's slowest run (601) is
still 2.7× below A's slowest (1,654), so the two distributions do not overlap.

### Why only three pairs

The 8-thread pass was cut at three A/B pairs so the final `cargo test` gate
could run without its compile load landing inside one arm's runs and
manufacturing the very result being reported. Three non-overlapping pairs with
opposite variance signatures is a sound basis for "3×, real"; it is not a basis
for "3.02×", and no such precision is claimed. The 1-thread pass, which the
phase plan's exit criterion actually names (the single-op write path), has the
full six runs per arm.

### What this means

For a single-threaded or lightly concurrent writer — the shape most embedded
users have — rule 5 costs nothing measurable. For a database with many
concurrent writers at `ReadUncommitted`/`ReadCommitted`/`RepeatableRead`, it
costs most of the write concurrency, because those levels never took the lock
before.

That is the trade the feature makes, and it is not negotiable in isolation: the
reservation check must be under `commit_mu` or first-preparer-wins stops meaning
anything (the TOCTOU is spelled out in the phase plan and in
`docs/concurrency-and-safety.md`). The lever that *is* available is RV-M3 — the
deferred review item this measurement makes urgent rather than theoretical.
Shrinking the critical section so the apply happens outside it needs write
intents or a second publication protocol; that is RV-M3's job, not 3.2's, and
3.2's rollback note already records that rule 5 reverts only together with the
whole feature.

## Honesty about this machine

`ps` showed **ten concurrent `claude` processes** during these runs — other
agents in the same session, several of them compiling. The effect is visible in
the raw data: wall time for the same binary and the same workload ranged from
9.7 s to 28.8 s, and p99 from 8.3 ms to 15.5 ms, entirely from external load.
AGENTS.md already warns that this host is thermally noisy (±15–20%, worse after
sustained load); ten concurrent agents is well past that.

So:

- **p50 is the reported metric for the 1-thread pass.** It is the only
  percentile there that survives an unrelated process stealing the CPU for
  10 ms.
- **Absolute p99 and p999 values are not attributable.** Every p99 here is
  milliseconds on a code path whose median is microseconds; that gap is the
  other agents, not ondaDB. The 8-thread table uses p99 only as a *ratio between
  the two arms measured alternately in one session*, where the shared load
  cancels — not as a latency figure for ondaDB.
- **The 1-thread result should be repeated on an idle host** before anyone
  treats 0.3% as *the* number. What it establishes is an upper bound on
  plausibility: a 10% uncontended regression would be visible even through this
  noise, and it is not.
- **The 8-thread result does not need an idle host.** Its evidence is the
  opposing variance signature and the non-overlapping distributions, both of
  which the noise strengthens rather than manufactures: external load makes A
  *slower and more variable*, which can only shrink the measured gap.

## The second RV-M3 consequence, which this does *not* measure

The 8-thread number above is the cost with **no prepared transaction anywhere** —
pure lock serialization, the registry never even touched. There is a second,
independent cost that only appears once prepared transactions are actually in
flight, and it is not measured here.

`DB::commit_prepared` holds `commit_mu` **across an fsync**, inside a
reserved-but-unpublished window. So `visible_seq` cannot advance past the
reserved block until that fsync returns, and every concurrent fixed-isolation
`begin`/`reset` meanwhile spins in `wait_visible_at_own_floor` — `yield_now` in
a loop, bounded at one second — burning CPU rather than sleeping. A slow decision
fsync therefore taxes every concurrent transaction's `begin`, not just the
committing thread.

That is RV-M3 made worse a second time, deliberately, with the reason recorded in
`docs/concurrency-and-safety.md` § Prepared transactions. It is not measured
because it needs a sustained concurrent `commit_prepared` stream to manifest, and
this host cannot resolve a latency claim of that shape while ten agents are
running. It is stated rather than hidden.

Taken together, the two costs make RV-M3 an item with a measured price rather
than a theoretical one. Neither is fixable by relaxing the ordering: the
validation-to-apply exclusion `commit_mu` provides is precisely what the
reservation guarantee rests on.

## Reproducing

```sh
# One target dir per arm; this host sets CARGO_TARGET_DIR globally.
git archive roadmap/wave-a | tar -x -C /tmp/base
# Point a copy of rule5-probe.rs.txt at each tree via a path dependency, then:
CARGO_TARGET_DIR=/tmp/t-base cargo build --release   # arm A
CARGO_TARGET_DIR=/tmp/t-new  cargo build --release   # arm B
# Alternate the arms; 6 runs each.
./rule5 <ops> <threads> <db-dir>
```
