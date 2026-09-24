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

Which file the sweep takes first is a cost choice. Since 0.2 the level's
candidates are visited **cheapest first**: the file that rewrites the least
data in the level below per byte of its own — its *overlap ratio*. Two files
of the same size cost very different amounts to push down when one sits over a
dense stretch of the next level and the other over a sparse one, and taking the
cheap one first means fewer bytes rewritten for the same amount of debt paid.
On a fixture with skewed record sizes this cut compaction bytes per ingested
byte by about 10%; on a perfectly uniform workload every candidate scores the
same and the order is the old cursor order, so there is nothing to gain and
nothing to lose. There is no knob: it is not a behavior change you can observe
except in bytes written.

Ordering is all it is. If the cheapest candidate cannot actually be compacted —
a read-only mounted part overlaps what it would rewrite, or another job already
holds that key range — the sweep moves on to the next-cheapest rather than
giving up, so one blocked file never wedges the level.

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

## Periodic compaction (0.3) — the age trigger

Both triggers above are about **size**: L0 file count, or a level over its byte
capacity. Neither fires on a database that has stopped taking writes, so an idle
column family keeps its expired TTL entries, its tombstones and its shadowed
versions indefinitely — reclamation only ever happened inside a compaction, and
short of a manual `DB::compact` no compaction was due.

`ColumnFamilyConfig::periodic_compaction_interval` adds a third trigger:
revisit a table this long after the compaction that wrote it. `Duration::ZERO`
— the default — disables it entirely, and a database that leaves it alone gains
no thread, no ticker and no disk artifact: the compaction worker's default path
is one relaxed atomic load per tick.

**Durable age state.** Eligibility is measured against
`SstMeta::last_compaction_time`, a manifest field written only behind the
`CAP_PERIODIC_AGE` format capability. It is emphatically **not**
`max_entry_time`: that field carries the maximum forward over a compaction's
inputs so cold data does not look freshly written, which is what the part
mover's `TierRule::min_age` gate needs. Carrying it forward here would make a
just-rewritten table instantly re-eligible; resetting it would break tier
placement. `None` means *unknown*, and unknown is never eligible — a legacy
table, a table written before the capability was taken, a foreign mount, or a
part attached from another database.

Enabling the capability stamps every local, non-mounted table that has no age
state yet with the enable time, **inside the same manifest write** that persists
the bit — one catalog rewrite, no table IO. The alternative ("eligible one
interval after open") is not restart-safe: open time is not durable, so a
database restarted more often than its interval would never become eligible at
all.

**Scheduling.** The scan piggybacks on the compaction worker, like the part
mover, on a derived cadence of `interval / 4` clamped to `[1s, 15m]`, and under
its own `periodic_running` CAS. The guard is not optional: with
`num_compaction_threads` workers, an unguarded scan would have every one of them
walk the same levels and enqueue the same family each interval. The pass only
*sends* on the compact channel; the picker rechecks eligibility under its normal
locks.

**Picking.** Age work is the **lowest** priority — `pick_compaction` consults it
only after the scored capacity levels yield no job. A level over capacity is a
backlog that grows; a stale table is space that does not, so capacity never
waits behind age. Among eligible tables the oldest stamp wins, ties resolving to
the shallower level. Then:

- **non-bottom**: an ordinary bounded push-down, source plus the target tables
  it overlaps, with the foreign-mount and range-lock vetoes as usual. An
  eligible L0 table defers to L0's oldest-first window, which is a correctness
  invariant rather than a cost choice;
- **bottom**: an **in-place rewrite** (`target == level`) — the only way a
  bottom table that overlaps no incoming data ever sees the compaction filter or
  drops its tombstones again. A deeper level is never created for age reasons
  alone. When the bottom *is* L0 (a one-level family) the rewrite takes the
  whole level, because L0's files overlap and only a whole-level merge leaves
  the outputs disjoint.

The rewrite stamps its outputs with the current reading, so the trigger settles
instead of looping on its own output.

**Burst limit.** Tables written together age together, so an interval tends to
make a whole backlog eligible at once. One `run` pass takes at most
`PERIODIC_BURST` (4) age jobs; if age work is still due it then re-enqueues its
family at the back of the compact channel and stops taking age work (capacity
work is still picked). Other families' jobs therefore interleave with a large
backlog instead of waiting behind all of it. Scheduling order only — nothing is
persisted and no job changes.

**No new drop rule.** A periodic job is an ordinary compaction: the same
snapshot, TTL and tombstone retention decides what it may drop. Data hidden
behind a live snapshot survives a periodic rewrite exactly as it survives a
capacity one.

**Observability.** `CfStats::periodic_compactions` counts the subset of
`compaction_count` that the age trigger picked — zero for every database that
leaves the option at its default, and the number to watch to tell idle
reclamation from ingest-driven work. Only completed jobs count: a failed job
leaves its input's stamp untouched, so the table stays eligible and is retried.

**Rollback** is setting the option back to `0`. Scheduling stops; the persisted
stamps stay readable and are simply never consulted.

The option is invalid on a `CompactionStyle::Fifo` family (`validate` returns
`Err`), which evicts by age through `fifo_ttl` and never merges at all.

## Tombstone density (plan C P4) — the delete trigger

A family whose writes are mostly deletes may never reach a size trigger, so
its tombstones — and the puts they shadow — sit in the tree, costing reads a
walk over dead versions and holding space. `ColumnFamilyConfig::
tombstone_density_trigger` (default `0.0`, off; persisted as TLV tag 34)
compacts a table whose `num_tombstones / num_entries` (from its manifest
entry) is **at least** the trigger; `tombstone_density_min_entries` (tag 35)
ignores tables too small to be worth a job. A value above `1.0` never fires.

- **Priority.** Below capacity work, above periodic (age) work; the densest
  eligible table first (ties to the shallower level). A candidate whose range
  another job holds is skipped for the next one, never waited on.
- **Shape.** Non-bottom: the ordinary bounded push-down (an L0 table through
  L0's oldest-first window). Bottom: an in-place rewrite — but only once every
  version in the table is older than the oldest live snapshot, because until
  then the snapshot keeps every tombstone and the rewrite would produce the
  same table. A table written by the current `run` pass is not rewritten in
  place again by it, so a tombstone that must survive (a merge chain's
  terminating delete) cannot loop the worker.
- **Scheduling.** A flush that leaves a dense table wakes the worker even with
  L0 below its file-count trigger, exactly as a flush carrying range-delete
  fragments does.
- **Observability.** `CfStats::tombstone_density_compactions`.

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

## Compacting a key span: `DB::compact_range` (plan C F3)

`db.compact_range(&cf, lower, upper)` compacts the tables whose key span
reaches into `[lower, upper]` down to the bottom level and returns when the
result is installed. Bounds are `std::ops::Bound<&[u8]>` under the family's
comparator, exactly as for `new_iterator_bounded`; `(Unbounded, Unbounded)`
is the whole family.

- **Whole tables.** A table is taken when its span (point keys plus range
  fragments) intersects the bounds, so neighbouring keys that share a table
  are rewritten too. In L0 the oldest-first window up to and including the
  newest in-span file moves — an older overlapping file may never stay above a
  newer one that moved down.
- **Level by level.** `L -> L+1` pushes (each an ordinary job whose target set
  covers the source's whole key span), then an in-place rewrite of the in-span
  bottom tables no push produced. A one-level family pushes its window into a
  new L1. One multi-level merge into the deepest level would be cheaper but is
  only correct if the input set is closed under overlap at every intermediate
  level; the chain of ordinary pushes is correct by construction.
- **Retention and partitions** are the ordinary job's: tombstones and expired
  TTL entries that reach the bottom are dropped unless a live snapshot still
  reads what they shadow; bottom output is cut at partition boundaries.
- **Exclusion**, as for `DB::compact`: the family's whole key range for the
  duration (the pushed tables extend past the span). Writes and flushes go on.
  A foreign mount skips only the push that would merge around it. FIFO
  families run their eviction pass instead.

## Splitting one job across threads

`Options::num_compaction_threads` limits how many *jobs* run at once, and jobs
only run at once when their key ranges are disjoint. One large job — an L0
push-down that rewrites all of L1 — is a single merge on a single thread however
many workers are configured, and it is usually the longest job the engine
produces.

Since 0.8, `Options::max_subcompactions` lets that one job split its key range
into **spans** and merge them concurrently into a single atomic install:

| Field | Default | What it controls |
|---|---|---|
| `max_subcompactions` | 1 | Maximum spans one job splits into. `0` and `1` both mean today's behavior |
| `max_subcompaction_workers` | 0 | Size of the database-wide pool of span threads; `0` derives `num_compaction_threads` |

Neither is persisted: like the IO limiter's knobs they describe the host, not the
stored data, and are re-read at every open.

The split is **logically invisible**. Boundaries are user keys and spans are
half-open, so every version of a key stays in one span; a scan returns the same
thing at every snapshot whether the job ran in one span or four. What does
change is the *files*: their boundaries and ids differ from a single-span run,
which is why the tests compare scans and not bytes.

Where the cuts land, in order of preference:

1. **Partition boundaries**, when the job writes bottom output. A part is never
   split across spans, and a bottom SSTable never spans two partitions anyway,
   so these cuts are free. A partition boundary is a key *prefix*, and "a prefix
   is the first key of its partition" is a bytewise fact — so a **partitioned**
   family with a custom comparator stays single-span rather than risk cutting a
   part in half. A custom comparator on its own does not: cuts then come from
   real table keys, compared with that comparator.
2. **Target-table `min_key`s** otherwise — already sorted, and points where the
   output would have started a new file regardless.
3. The **input tables' `min_key`s**, when the target level is empty.

More candidates than spans are sampled by cumulative target bytes, so the spans
carry comparable amounts of work rather than comparable amounts of keyspace.
Read the result back per job:

```rust
let s = cf.stats();
s.span_count;             // spans the last bounded job ran (1 = single merge)
s.span_imbalance_bytes;   // widest span minus narrowest, in output bytes
```

A `span_imbalance_bytes` close to the job's whole output means the boundaries
did not track the data, and the job took as long as its widest span.

**What stays single-span**, whatever the setting: `DB::compact`'s whole-level
sweep and its in-place bottom rewrite, FIFO (which never merges), and any column
family with a compaction filter installed. The filter is excluded for a
semantic reason rather than a thread-safety one — `CompactionFilterFn` is
already `Send + Sync`, but it is written against a single-threaded, key-ordered
traversal, and spans would make the order depend on the span count.

**Sizing.** The span pool is separate from `num_compaction_threads` on purpose,
and a job's coordinator takes nothing from it — it runs the first span on the
compaction thread it already occupies. `max_subcompactions = 4` with the default
two compaction threads therefore peaks at 2 coordinators + 2 span workers, not
8. Nothing ever waits for a permit: a job that cannot have all its spans runs
fewer.

The default is `1`, and stays there: the gain is real only when one job is large
relative to the device's spare bandwidth, and a machine whose compaction is
already IO-bound gets nothing from splitting it (the 0.6 limiter still caps the
total either way). Raise it when `compaction_debt` is driven by a few big jobs
rather than by many small ones, and measure.

## Tuning by symptom

| Symptom | Look at |
|---|---|
| Write throughput collapses over a long ingest | `compaction_debt` — if pinned near the hard ceiling, compaction cannot keep up; raise `num_compaction_threads`, or accept the paced rate as the real one |
| Debt is driven by a few very large jobs rather than many small ones | `max_subcompactions` — see § Splitting one job across threads; check `span_imbalance_bytes` afterwards |
| `close()` takes seconds | Expected with `finish_compactions_on_close = true`; otherwise check debt at close |
| Point reads slow right after opening | L0 depth. `cf.stats().levels[0]` — see § Closing |
| Point reads slow in steady state | Level count and bloom settings, not this document — see `docs/performance.md` |
| Compaction never seems to run on a mostly-idle CF | Size triggers do not fire below capacity; `DB::compact` sweeps explicitly (this is what reclaims tombstones from a fully deleted CF), or set `periodic_compaction_interval` so the engine revisits stale tables on its own — see § Periodic compaction |
| Space is not reclaimed although TTLs have expired | Same cause: nothing was due. `periodic_compaction_interval` plus `CAP_PERIODIC_AGE`; watch `CfStats::periodic_compactions` |
| Deletes never free space although no snapshot is open | Size triggers do not fire for a delete-heavy family; set `tombstone_density_trigger` (e.g. `0.5`) — see § Tombstone density |
| `periodic_compactions` stays at zero with the option set | The capability is not enabled (`DB::enable_format_capabilities(CAP_PERIODIC_AGE)`), so no table carries age state |

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
