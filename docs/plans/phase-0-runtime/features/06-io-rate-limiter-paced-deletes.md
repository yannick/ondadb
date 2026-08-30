# 0.6 — Background IO limiter and paced deletion

**Readiness:** design settled (thread-local IO class with explicit scope guards
— see below). **Effort:** 3–5 dev-weeks, two reviews: (A) classes and limiter,
(B) deletion worker and pacing. **wavesdb counterpart:** 0.6 (SILK framing
adopted).

## Goal

Bound background *bandwidth* so flush, compaction, and obsolete-file cleanup
don't monopolize the device and inflate foreground p99. ondaDB currently paces
writers by compaction-**debt bytes** (`ColumnFamily::pace_for_compaction_debt`,
called from `apply_commit` before any lock is taken — "the point is to slow
this writer down, not to hold anyone else up while it waits") — how much work
is owed, with no dimension for how fast background work consumes IO.
`DbInner::remove_sst_file` unlinks immediately outside a deletion pause.

## The ondaDB-specific simplification: thread-local IO classes

wavesdb had to thread an IO class through `context.Context` because goroutines
are migratory. ondaDB's *scheduled* background IO runs on dedicated, stable
worker threads (`onda-flush`, `onda-compact-{n}`), so a thread-local default
class costs no signature churn:

```rust
// new module ioctrl.rs
pub enum IoClass { Foreground, Flush, Compaction, ObsoleteDelete }
thread_local! { static CLASS: Cell<IoClass> = const { Cell::new(IoClass::Foreground) } }
pub fn set_class(c: IoClass) -> IoClass;        // returns the previous value
pub struct ClassGuard(IoClass);                 // new: restores on drop
pub fn scoped(c: IoClass) -> ClassGuard;        // new
#[inline] pub fn current() -> IoClass;

pub trait IoLimiter: Send + Sync {
    /// Blocks until `bytes` of `class` may proceed. Called before issuing
    /// known-size IO so a cancelled job never consumes bandwidth.
    fn charge(&self, class: IoClass, bytes: u64);
}
```

### The hole a spawn-time default leaves, and the fix

**Not all background IO runs on a worker thread.** Three paths run the
engine's largest IO bursts on the *caller's* thread, which under a spawn-time
default would be `IoClass::Foreground` and never charged:

| Path | Entry point | Guard |
| --- | --- | --- |
| manual compaction (a whole-level sweep, the largest burst the engine produces) | `DB::compact` → `compaction::run_manual` | `let _c = ioctrl::scoped(IoClass::Compaction);` at the top of `run_manual` |
| caller-side flush rotate + spin-wait | `DB::flush_memtable` | `ioctrl::scoped(IoClass::Flush)` around the body |
| bulk ingest | `ingest.rs::Ingestion::finish` | `ioctrl::scoped(IoClass::Flush)` (ingest output is L0, like a flush) |

Set the class with an explicit scope guard at those entry points, **not only at
worker spawn**. `set_class` already returns the previous value, which is what
`ClassGuard` restores — nesting is therefore safe, and a caller thread that
happens to be a worker thread is left as it was.

- Workers set their class once at spawn: `onda-flush` → `Flush`,
  `onda-compact-{n}` → `Compaction`. The part mover shares the compaction
  worker and inherits `Compaction`, which is correct: it is background IO.
- Production limiter: work-conserving token bucket with an injectable clock
  (`Arc<dyn Fn() -> Instant + Send + Sync>` for tests). Disabled is
  `Option<Arc<dyn IoLimiter>>` = `None` — a nil check, no allocation, no
  thread.
- **Foreground never waits**: `charge` returns immediately for
  `IoClass::Foreground`.
- Charge points:
  - Reads: `Reader::read_data_block` (miss path only), `read_vlog_from_file`,
    and the S3 `Storage::read_exact_at` implementation — charge the requested
    framed length under `current()`. Cache hits are free, which is correct:
    they cost no device IO.
  - Writes: `Writer::flush_block` and `write_meta_block` charge the framed
    block bytes; `write_vlog` charges frame bytes. Large operations charge in
    bounded chunks so no single charge can exceed bucket capacity forever.
- WAL write/fsync is foreground durability — out of scope, never charged.

### Lock-order note (phase rule exception)

`lock_job` returns a `RangeGuard` held for the whole compaction job
(`compact_inputs`' doc: "The caller owns input selection *and* the range lock
covering every input"), so every `charge()` inside compaction read/write blocks
while that range lock is held. This is the **documented exception** in
`../plan.md`'s background-wait rule: the job's own range lock is its unit of
exclusion, no foreground path acquires it, and waiting under it delays only
work that was already excluded. `run_manual` and `run_fifo` additionally hold
`cf.compact_mu` and a whole-keyspace range lock — same reasoning, same
exception. Record it in `docs/concurrency-and-safety.md` alongside the lock
inventory. Every other lock in the inventory stays off-limits to a background
wait.

## Deletion worker and pacing (review B)

Extend `FileDeletionState` (`db.rs`, today a pause counter plus a pending list)
into an owned worker.

`DbInner::remove_sst_file(&self, path: &str)` keeps its name and gains a byte
count: `remove_sst_file(&self, path: &str, bytes: u64)` (**new** shape). Under
an active pause, tasks queue exactly as today — pause semantics are unchanged,
because checkpoint/backup correctness depends on them (`pause_deletions` /
`resume_deletions`, which already drains on last-guard drop). Otherwise tasks
go to the worker's channel.

**All six call sites, and where the bytes come from:**

| Call site | Bytes |
| --- | --- |
| `compaction.rs` `run_fifo` eviction, klog | `t.meta.klog_size` |
| `compaction.rs` `run_fifo` eviction, vlog | `t.meta.vlog_size` |
| `compaction.rs` `remove_compaction_inputs`, klog | `table.meta.klog_size` |
| `compaction.rs` `remove_compaction_inputs`, vlog | `table.meta.vlog_size` |
| `parts.rs` post-move source klog | `h.meta.klog_size` |
| `parts.rs` post-move source vlog | `h.meta.vlog_size` |

Every site already holds the `SstMeta`, so no signature above `remove_sst_file`
changes. A `vlog_size` of 0 (no separated values) still charges
`DELETE_METADATA_BYTES` (**new**, `4096` — one filesystem block, the documented
minimum for an unlink's metadata cost), so a storm of tiny deletions is still
paced.

- The worker unlinks at `obsolete_delete_bytes_per_second` (0 = immediate,
  today's behavior), charging under `IoClass::ObsoleteDelete`. FIFO order;
  **correctness never depends on deletion order**, because file ids never
  reuse.
- `close()` drains the queue and joins the worker before releasing the
  directory lock — the same ordering deferred deletes under a pause already
  obey.
- Optional later slice (ties to the S3 orphan gap in AGENTS.md): a trash
  directory for tier-root files the orphan sweep deliberately leaves
  (`orphan_sweep_locations` skips shared tiers) — rename-then-slow-unlink,
  swept at open. Separate review; only if tier usage grows.

## Configuration

DB-level `Options`, 0 = disabled, **not persisted** (they describe the current
host and device, not the stored data — the same rule 0.8's
`max_subcompactions` follows):

```rust
pub background_io_bytes_per_second: u64,
pub background_io_burst_bytes: u64,      // 0 derives one second of rate, bounded
pub obsolete_delete_bytes_per_second: u64,
```

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

### Review A — classes and limiter

1. **`ioctrl.rs`.** `IoClass`, the thread-local, `set_class`, `current`,
   `ClassGuard`, `scoped`, and the `IoLimiter` trait.
   Test first: `ioctrl.rs::scoped_restores_previous_class` — nested `scoped`
   calls restore in LIFO order, and a panic inside the scope still restores
   (assert via `catch_unwind`); `ioctrl.rs::default_class_is_foreground` on a
   fresh thread.
2. **Token bucket.** `ioctrl.rs::TokenBucket` (**new**) implementing
   `IoLimiter` with an injectable clock.
   Test first: `ioctrl.rs::bucket_refills_at_configured_rate` — fake clock;
   charging `rate` bytes then `rate` more takes exactly one simulated second;
   `ioctrl.rs::bucket_is_work_conserving` — after an idle period the
   accumulated burst is spendable up to `burst_bytes` and no further;
   `ioctrl.rs::foreground_never_waits` — a `charge(Foreground, huge)` on an
   exhausted bucket returns without advancing the clock;
   `ioctrl.rs::zero_rate_is_unlimited` — rate 0 never blocks.
3. **Wire the limiter into `DbInner`.** `Option<Arc<dyn IoLimiter>>`
   constructed from the three options; `None` when the rate is 0.
   Test first: `db.rs::no_limiter_object_when_disabled` — default `Options`
   yields `None` (no allocation, no thread).
4. **Worker class at spawn.** `db.rs::spawn_workers` — `set_class` at the top
   of `flush_worker` and `compact_worker`.
   Test first: `tests/maintenance.rs::worker_threads_report_their_io_class` —
   a test `IoLimiter` records `(class, bytes)`; force a flush and a compaction
   and assert `Flush` and `Compaction` charges appear.
5. **Caller-thread scope guards.** `compaction.rs::run_manual`,
   `db.rs::DB::flush_memtable`, `ingest.rs::Ingestion::finish`.
   Test first (this is the F6.1 regression pin):
   `tests/maintenance.rs::manual_compaction_is_charged_as_background` — with
   the recording limiter, `DB::compact` on a multi-level CF produces
   `Compaction` charges and **zero** `Foreground` charges; and
   `tests/ingest_arms_compaction.rs::ingest_finish_is_charged_as_flush`.
6. **Read charge points.** `sst/reader.rs::read_data_block` (miss path only)
   and `read_vlog_from_file`; the S3 backend's `read_exact_at`.
   Test first: `tests/sst.rs::cached_block_reads_are_not_charged` — read a
   block twice with the recording limiter; exactly one charge, of the framed
   length.
7. **Write charge points.** `sst/writer.rs::flush_block`, `write_meta_block`,
   `write_vlog`, in bounded chunks.
   Test first: `tests/sst.rs::written_bytes_are_charged_once` — build a table
   with a known block count and assert charged bytes equal the file's framed
   size within the metadata allowance; and
   `sst/writer.rs::large_write_charges_in_bounded_chunks` — a value larger than
   the bucket capacity is charged in chunks and completes.
8. **End-to-end pacing.** Test first:
   `tests/maintenance.rs::limited_compaction_stretches_over_fake_clock` — a
   compaction under a tight limit consumes the expected simulated time while
   concurrent point reads issue zero limiter waits (assert via PerfContext
   timings from 0.10 and the recording limiter's class breakdown).
9. **Lock-order documentation.** `docs/concurrency-and-safety.md`: the
   range-lock exception above. No test; the gate still runs.

### Review B — deletion worker

10. **Signature + call sites.** `db.rs::remove_sst_file(path, bytes)`,
    `DELETE_METADATA_BYTES`, and all six call sites from the table above.
    Test first: `db.rs::retire_charges_metadata_minimum_for_empty_vlog` — a
    zero-byte deletion charges `DELETE_METADATA_BYTES`, not 0.
11. **Worker.** `FileDeletionState` gains a channel, a thread, and a join
    handle; unpaced default unlinks immediately (no worker spawned when
    `obsolete_delete_bytes_per_second == 0`).
    Test first: `tests/maintenance.rs::unpaced_deletion_is_immediate` — with
    the option at 0, files are gone when `remove_compaction_inputs` returns
    (today's observable behavior, pinned); and
    `tests/maintenance.rs::paced_deletion_spreads_over_fake_clock` — with a
    rate set, a hundred obsolete files are unlinked over the expected
    simulated interval and all are eventually gone.
12. **Pause interaction.** No new code; the pause path must be untouched.
    Test first: `tests/maintenance.rs::paused_deletion_still_defers_with_worker`
    — `pause_deletions` during a paced-deletion storm still defers every
    unlink, and the guard drop drains them; run the existing
    `backup_consistent_during_compaction` unchanged.
13. **Shutdown.** Test first:
    `tests/maintenance.rs::close_drains_deletion_queue_before_lock_release` —
    queue deletions, `close()`, assert the files are gone and the `LOCK` file
    is released after (reopen succeeds and finds no orphans); and
    `tests/maintenance.rs::poison_does_not_hang_the_deletion_worker`.
14. **Harness.** Concurrent foreground reads during a forced compaction; delete
    storms. Retain raw JSONL.

## Tests (summary)

- Fake-clock limiter: refill rate, work conservation, foreground never blocks,
  zero rate unlimited, scope-guard restore including on panic.
- Caller-thread background work is classified (manual compaction, ingest,
  caller-side flush) — the regression pin for the spawn-time-only hole.
- Cached reads uncharged; written bytes charged once; large writes chunked.
- Deletion: immediate at 0, paced when set, pause still defers, close drains,
  poison does not hang.
- Both feature configs.

## Acceptance

Foreground-reads-during-forced-compaction phase: p99 read latency under a
limited compaction stays within a documented bound of the no-compaction
baseline, while unlimited compaction measurably degrades it — that delta is the
feature. Publish fsync latency alongside charged bytes, since charged bytes are
a proxy for device pressure, not a measurement of it. Raw JSONL retained.

## Rollback

All three options to 0: the limiter is `None`, the deletion worker is never
spawned and unlinks happen inline — today's behavior exactly. The `ioctrl`
module and the `remove_sst_file` byte parameter remain (inert); no disk state.
