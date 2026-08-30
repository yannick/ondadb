# ondaDB forward roadmap

These documents adapt the wavesdb forward roadmap (audited 2026-08-29 at
`1a052a4`) to **ondaDB 0.8.2** (`3afc3c1`). They are design plans, not release
promises: a feature moves to execution only after its prerequisite design
decisions and benchmark harness exist.

Wave 0 — the August 2026 code review's corrective work — **landed in 0.8.2**
([`../code-review-2026-08-resolution.md`](../code-review-2026-08-resolution.md)).
Only M3 (`commit_mu` latency, deferred) and M5 (manifest rewrite cost, fixed by
feature 2.2) remain open, and neither gates entry to a wave.

Start with:

- [`implementation-plan.md`](implementation-plan.md) — dependency waves, the
  pinned capability/kind registry, the identifier contract, effort, release
  gates, and the execution protocol for agentic workers.
- [`../wavesdb-feature-assessment.md`](../wavesdb-feature-assessment.md) — why
  each wavesdb feature was adopted, adapted, or skipped for ondaDB.
- [`../code-review-2026-08-resolution.md`](../code-review-2026-08-resolution.md)
  — what 0.8.2 fixed, and the two items it deliberately deferred.

| Area | Directory | Scope | Compatibility impact |
| --- | --- | --- | --- |
| Runtime and observability | [`phase-0-runtime/`](phase-0-runtime/plan.md) | Nine runtime features (0.7 rejected); periodic compaction needs the capability framework | none, except 0.1/0.5's appended `ONDA*` config-blob tails (old blobs still decode) and 0.3's new `SstMeta` field behind `CAP_PERIODIC_AGE` |
| Record semantics | [`phase-1-record-kinds/`](phase-1-record-kinds/plan.md) | strict decoding, capability-gated manifest v2, merge operands, range tombstones and excise | manifest v2 + `FORMAT_CAPS_TAG`; WAL record envelopes; `FOOTER_EXTENDED_BLOCK` (table-level); `CAP_MERGE_OPERANDS`, `CAP_RANGE_DELETES` |
| Storage formats | [`phase-2-formats/`](phase-2-formats/plan.md) | prefix-delta data blocks and the manifest edit log | `FOOTER_PREFIX_DELTA` (table-level) under `CAP_PREFIX_DELTA`; a second manifest artifact (`ONDE` edit log) under `CAP_MANIFEST_EDITS` |
| Transactions | [`phase-3-transactions/`](phase-3-transactions/plan.md) | durable 2PC (unified layout) and pessimistic locks | WAL control records (kinds 16–31) under `CAP_TXN_DECISIONS`; 2PC requires the unified WAL layout. 3.3 is in-memory only |

Every capability bit, record kind, and footer flag above has a pinned value in
[`implementation-plan.md` § Capability and kind registry](implementation-plan.md#capability-and-kind-registry);
wavesdb reconciles to those values.

Feature numbers intentionally match the wavesdb roadmap (0.x runtime, 1.x
record kinds, 2.x formats, 3.x transactions) so the two plans and the
assessment cross-reference cleanly. Where a wavesdb plan transfers nearly
as-is, the ondaDB document cites it and records only the deltas.

**Rejected (decision recorded, not scheduled):** 0.7 global memtable budget —
`Options::max_memory_usage` stays a documented-reserved field. The budget check
would sit inside the `commit_mu` window that review item M3 already indicts,
and no consumer is asking for it.
[`phase-0-runtime/features/07-global-memory-budget.md`](phase-0-runtime/features/07-global-memory-budget.md)
records the reasons and what would reopen it.

**Deferred (no feature doc; revisit when the named prerequisite exists):**
managed sequence mode (needs an external coordinator consumer for sequence
allocation specifically — 3.2's coordinator does not supply it), large-txn
private spill (needs 3.2's decision records), tiered/lazy-leveling compaction
(needs 2.2 run identity; spike-only), wide-column entities (value-level API, no
engine change — take wavesdb 4.2 as-is if wanted), WAL failover (needs a fault
model), trace/replay and io_uring (self-contained spikes; see assessment §4).

3.2 (durable 2PC) and 3.3 (pessimistic locking) are **no longer product-gated**
— a coordinator consumer exists, so both are scheduled in Wave D.
