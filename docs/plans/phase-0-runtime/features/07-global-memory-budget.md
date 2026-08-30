# 0.7 — Global memtable budget — **REJECTED**

**Status:** rejected for this roadmap (decision taken 2026-08-30, baseline
0.8.2). `Options::max_memory_usage` stays a documented-reserved field. No
implementation work is scheduled; this document is the decision record.

## What was proposed

Bound approximate active + immutable memtable bytes across all CFs and the
unified store, with a soft trigger at `7/8 × max_memory_usage` (rotate the
largest active buffer) and a hard wait at the limit (backpressure commits until
a flush completes). Effort was estimated at 3–4 dev-weeks.

## Why it is rejected

1. **The field is not "dead and undecided" — 0.8.2 already decided.**
   `max_memory_usage` has exactly two occurrences in `src/`, both declarations:
   the field on `Options` and its initializer, `max_memory_usage: 0, //
   reserved; currently ignored`. Its doc comment reads "Reserved for a future
   database-wide memory governor; currently ignored." The earlier, misleading
   "0 => auto (≈75% system memory)" comment was corrected in the 0.8.2
   corrective release, and `docs/code-review-2026-08-resolution.md` records the
   accompanying policy: the review-listed inactive fields "remain public for
   source compatibility and are explicitly documented as reserved/ignored:
   `max_concurrent_flushes`, `max_memory_usage`, `log_level`, …". Adopting the
   feature would reverse a decision made three weeks ago; deleting the field
   would be a breaking API change for no benefit.

2. **The hard wait cannot be placed where the design claimed.** The design put
   the wait "before the CF `rot` gate in `apply_commit` (and
   `UnifiedStore::apply`) so no CF lock is held while waiting". That is true of
   `rot` and false of `commit_mu`: `Txn::commit` takes `db.commit_mu` for every
   conflict-checked or `Serializable` transaction and holds it across
   `apply_prepared` → `apply_per_cf_groups` → `ColumnFamily::apply_commit`. A
   hard wait inside `apply_commit` therefore parks a writer **under
   `commit_mu`**, serializing every validated commit in the database behind one
   flush. That violates the phase-wide background-wait rule, and it collides
   with open review item **M3** (`commit_mu` latency), which is deferred
   precisely because that lock is already the write path's contention point.

   The fix — hoisting the wait into `Txn::commit` before the `commit_mu`
   acquisition, mirroring what `pace_for_compaction_debt` already does before
   `apply_commit` takes `rot` — is possible, but it means the budget check
   lives in the transaction layer while the accounting lives in the memtable
   layer, and it must be duplicated on every commit path (including the
   single-op fast paths that never take `commit_mu`). That is a materially
   larger and more invasive change than the estimate covered.

3. **No consumer.** Nothing in the engine, in `../bench`, or in the known
   downstream (ayu/spada) asks for a database-wide memtable bound. The existing
   per-CF controls — `l0_queue_stall_threshold`, the `rot` gate,
   `soft`/`hard_pending_compaction_bytes` and `pace_for_compaction_debt` —
   already bound memtable growth per CF, and the many-CF skew workload that
   would justify a global bound does not exist as a real deployment shape here.

4. **The contract is weak enough to mislead.** The design's own caveat is that
   this is not an RSS cap: block and table caches, reader metadata, txn arenas,
   compaction buffers, and allocator overhead all sit outside it. A knob named
   `max_memory_usage` that bounds one of six contributors is a support burden.

## What stays

- `Options::max_memory_usage` remains, public, `0`, and documented as reserved
  and ignored. Do not delete it (source compatibility) and do not re-document
  it as active.
- No `DbInner::memory_budget`, no per-memtable budget handle, no new atomics on
  the write path.

## What would reopen this

Any one of:

- A named consumer with a many-CF workload that demonstrably overruns memory
  under the existing per-CF controls, with a profile showing memtables (not
  caches or reader metadata) as the dominant term.
- Review item **M3** resolved such that `commit_mu` is no longer held across
  apply — which removes objection 2 and makes the wait placement a local
  decision again.
- A decision to make the field mean something narrower and honest (for example
  `max_memtable_bytes`), accepting the rename as a breaking change.

If reopened, the design sketch above is a sound starting point: the soft/hard
shape, the `Condvar` wake modelled on `notify_debt_waiters` (which takes `rot`
to make the wake race-free), the `Arc<Memtable>`-`Drop` decrement hook, and the
explicit not-an-RSS-cap contract all survive the objections. Only the wait
placement and the estimate need redoing.
