# Compaction & write pacing — feature guide

How ondaDB decides what to compact, how it keeps a write burst from outrunning
that, and which knobs to turn when it does. New in 0.8.0.

Internals live in `docs/architecture.md` (§ Compaction, § Bounded jobs and
backpressure, § Range locks); locking contracts in
`docs/concurrency-and-safety.md`. This file is the user-facing guide: the model,
the knobs, and the operational notes. All names are real API names.

## The model in one paragraph

Data lands in a memtable, is flushed to L0 as an SSTable, and is merged
downwards level by level. L0 files **overlap** each other; every level below it
is sorted and disjoint, so a point read probes at most one file per level there
but **every** file in L0. Level `i >= 1` holds up to
`l1_base_bytes * level_size_ratio^(i-1)` bytes and is compacted into `i+1` when
it exceeds that. L0 is compacted into L1 when it reaches
`l1_file_count_trigger` files.

## Jobs are bounded

A compaction takes **one** file from the source level plus only the files in the
next level whose key ranges overlap it. The cost of a single job is therefore
about:

```
target_file_size * (1 + level_size_ratio)
```

which does not depend on how large the level has grown. A per-level cursor
sweeps the keyspace so successive jobs advance across it rather than repeatedly
picking the same file.

L0 is the exception, twice. Its files overlap, so an arbitrary subset cannot be
merged — that would reorder versions of a key. The **oldest** files can be,
because L0 is kept newest-first and reads walk it in that order, so a version
left behind in a newer L0 file still shadows the copy pushed down to L1. A job
takes the oldest `l1_file_count_trigger` files.

> **Why this matters.** Before 0.8.0 a job took the *whole* source level plus
> every overlapping target file. Under random keys an L0 file spans nearly the
> entire keyspace, so each push-down rewrote all of the level below it, and the
> work in one job grew with the dataset. See § What 0.7.x did wrong.

## Geometry: sizing levels and files

Three fields interact, and the ratio between them is what matters:

| Field | Default | What it controls |
|---|---|---|
| `write_buffer_size` | 64 MiB | Memtable size — how much is buffered before a flush |
| `target_file_size` | 16 MiB | Size at which compaction cuts an output SSTable |
| `l1_base_bytes` | 256 MiB | Byte capacity of L1; deeper levels multiply by `level_size_ratio` |

`l1_base_bytes / target_file_size` is **the number of files a level holds**, and
it is the number that decides whether partial compaction is possible at all. A
level holding one file cannot be compacted a piece at a time, because that
file's range covers everything below it. The defaults give L1 about 16 files.

Smaller `target_file_size` means finer-grained, more parallelizable compaction,
but more files — each holding a block index and bloom filter while open, bounded
by `Options::max_open_reader_bytes`. Going below a few MiB is rarely worth it.

## Write pacing

Ingest that outruns compaction has to be slowed down, or the debt grows without
limit and the write rate you measure is one the engine cannot sustain. Two
thresholds, both per column family:

| Field | Default | Effect |
|---|---|---|
| `soft_pending_compaction_bytes` | 2 GiB | Each commit is delayed in proportion to the excess |
| `hard_pending_compaction_bytes` | 8 GiB | Commits block until a compaction completes |

`0` disables either. The soft delay is capped at 1 ms per commit — it shapes the
ingest rate rather than stopping it, and leaves the stopping to the hard
ceiling. `validate()` rejects a soft threshold above the hard one, since pacing
that starts after the stop can never run.

Read the backlog back at any time:

```rust
let debt = cf.stats().compaction_debt;   // bytes
```

Debt is the sum over levels of how far each sits past its capacity. It is a
cached gauge refreshed by flush and by compaction, not recomputed per write, so
reading it is cheap. A value pinned near the hard ceiling means ingest is
outrunning compaction and the sustained rate is whatever the pacing allows —
not what a short burst reported.

Note this is **separate** from `l0_queue_stall_threshold`, which stalls writers
when sealed memtables pile up awaiting *flush*. Flush and compaction fall behind
for different reasons and are bounded separately; before 0.8.0 only the flush
side existed, which is why a compaction backlog could grow unnoticed.

## Closing, and reads right after opening

`Options::finish_compactions_on_close` (default `false`) decides whether
`close()` drains queued compaction before returning. Leftover debt is legal LSM
state that the next open resumes from, so abandoning it is safe — but it is not
free, and the cost lands on whoever reads next.

An abandoned backlog leaves L0 deeper. Since L0 files overlap, a point read
probes every one of them, so read cost is **linear in L0 depth** until
compaction catches up. Measured on 5M records, reading immediately after
reopening:

| | L0 files when reads begin | cold Get |
|---|---|---|
| `false` (default) | 6 | ~0.78M ops/s |
| `true` | 2 | ~1.41M ops/s |

On a *settled* tree there is no difference worth naming — 1.41M ops/s, the same
as 0.7.8 — because levels below L0 are disjoint and binary-searched, so the
smaller files 0.8.0 writes cost nothing on the read path.

**Set `finish_compactions_on_close = true`** if you load a dataset, close, and
reopen to serve point reads immediately. It costs a longer close (~3.2 s after
5M records, against ~1.1 s abandoning) and buys a fully merged tree. For a
long-running database, compaction keeps up and the distinction does not arise.

## Concurrency

Jobs on disjoint key ranges share no inputs and no outputs, so they run at once;
`Options::num_compaction_threads` (default 2) is what limits them. Exclusion is
by key range (`range_lock.rs`), not by a column-family-wide mutex, and the
parts/tiers operations participate in the same protocol: `detach_part` and
`relocate_part` lock their partition's span, `attach_part` and
`attach_part_by_ref` the whole keyspace, since their extent is not known until
the incoming files are validated.

Two consequences worth knowing operationally:

- The background part mover blocks only the partition it is relocating, not the
  whole column family.
- A part mounted by `attach_part_by_ref` is never rewritten, and now blocks only
  the ranges that actually overlap it rather than its entire level.

`DB::compact` (the manual sweep) still takes the column family whole — it
rewrites every level by design.

## Tuning by symptom

| Symptom | Look at |
|---|---|
| Write throughput collapses over a long ingest | `compaction_debt` — if pinned near the hard ceiling, compaction cannot keep up; raise `num_compaction_threads`, or accept the paced rate as the real one |
| `close()` takes seconds | Expected with `finish_compactions_on_close = true`; otherwise check debt at close |
| Point reads slow right after opening | L0 depth. `cf.stats().levels[0]` — see § Closing |
| Point reads slow in steady state | Level count and bloom settings, not this document — see `docs/performance.md` |
| Compaction never seems to run on a mostly-idle CF | Size triggers do not fire below capacity; `DB::compact` sweeps explicitly (this is what reclaims tombstones from a fully deleted CF) |

## What 0.7.x did wrong

Recorded because the shape of the bug is more instructive than the fix, and
because it is easy to reintroduce.

Compaction took the whole source level plus every overlapping target file, so
work per job grew with the dataset. Nothing in the write path noticed:
`l0_queue_stall_threshold` gates on flush backlog, and flush was never the
bottleneck — isolating the phases showed the flush queue draining in ~130 ms
whether 5M or 20M records had been written. Ingest therefore ran at memtable
speed however far compaction had fallen behind.

On a 24-core M2 Ultra (16 B keys, 100 B values, 8 threads) the reported write
rate sat flat at ~4.6M ops/s from 5M through 20M records, while the close that
followed went 2.5 s → 10.8 s → 35 s. Counting that close, the rate at which
records actually became durable SSTables was 1.36M → 0.77M → 0.49M ops/s: it
**halved every time the data doubled**, and no measurement of the write path
alone would ever have shown it.

Underneath sat a geometry bug that made the fix impossible until it was
addressed. Output was cut at `write_buffer_size` and L1's capacity *was*
`write_buffer_size`, so L1 held exactly one file whose range covered everything
beneath it. Partial compaction was not merely unimplemented — the geometry ruled
it out. Hence `target_file_size` and `l1_base_bytes` as separate fields.

The lesson generalizes past this engine: **a benchmark that stops its timer
before the engine has finished the work measures the buffer, not the system.**
See `docs/performance.md` § Deferred work is not free work.
