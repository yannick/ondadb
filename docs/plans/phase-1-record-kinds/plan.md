# Phase 1 — durable record-semantics track

Not one release: strict decoding ships alone, the v2 framework ships with no
capabilities enabled, and merge/range capabilities enable independently.

**Baseline:** ondaDB 0.8.2 (`3afc3c1`). Wave 0 landed in 0.8.2 (see
`docs/code-review-2026-08-resolution.md`); the only outstanding review items
are **M3** (`commit_mu` latency — deferred, and 1.2 makes it measurably worse
for range commits; acknowledged there) and **M5** (manifest rewrite cost —
feature 2.2). No RV item gates any phase-1 feature. Branch `roadmap/wave-a`,
one commit per implementation task after the 4-command gate.

| # | Feature | Readiness | Effort | Depends on |
| --- | --- | --- | ---: | --- |
| [1.0](features/11-strict-decoding-manifest-capabilities.md) | strict decoding, manifest v2 capabilities, record envelopes | design ready | 3–5 wks | frozen legacy corpus (its own first task) |
| [1.2](features/13-delete-range-excise.md) | range tombstones, then excise | architectural | 8–13 wks | 1.0 (`CAP_RANGE_DELETES`, kind 5, aux block, `ReplayRecord`) |
| [1.1](features/12-merge-operators.md) | merge operands | design required | 5–8 wks | 1.0 (`CAP_MERGE_OPERANDS`, kind 4), 1.2 (shared read/compaction surfaces) |

## Why flag-bit extensibility is rejected

ondaDB's entry flags byte uses `0x01 TOMBSTONE, 0x02 HAS_TTL, 0x04 HAS_VLOG,
0x08 DELTA_SEQ (never written), 0x10 SINGLE_DELETE` (`format.rs:10–21`) — five
bits conceptually consumed, three remaining, while the roadmap needs merge,
range-delete, and transaction-control kinds. Same wall wavesdb hit; same
answer: an explicit record `kind` in versioned envelopes plus a manifest
capability word. Flags stay modifiers, never an extensibility mechanism.

## Pinned numbering

Capability bits and record kinds are owned by `format.rs` and pinned by 1.0
(golden fixtures assert the literal values; wavesdb is reconciled to them):

```
CAP_EXTENDED_RECORDS 1<<0   CAP_MERGE_OPERANDS 1<<1   CAP_RANGE_DELETES 1<<2
CAP_PREFIX_DELTA     1<<3   CAP_MANIFEST_EDITS 1<<4   CAP_PERIODIC_AGE   1<<5
CAP_TXN_DECISIONS    1<<6   KNOWN_CAPS = 0x7F

kinds: 1 put, 2 delete, 3 single_delete, 4 merge, 5 range_delete,
       6..15 reserved, 16..31 txn control, 32..63 reserved,
       >= 64 never assigned (Corruption, not UnsupportedFormat)
```

## Record-kind flow checklist

Merge operands and range tombstones must account for every surface; a feature
may explicitly refuse one, but never silently treat a new kind as a put:

1. API validation and option/capability enablement.
2. Transaction overlay, savepoints, conflict sets, commit hooks, reset.
3. Per-CF and unified WAL encode/replay (`ReplayRecord` in both callbacks).
4. Per-CF and unified memtable/flush paths — `FlushMerge` (`memtable.rs:1049`,
   the arena/streaming flush build, a merge path of its own), `write_l0`,
   `write_l0_streaming`, `ingest_l0`, `split_by_cf`.
5. SST writer/reader, table metadata, cache identity, mixed legacy/new levels.
6. Point reads, forward/reverse iterators, snapshots, all five isolation levels.
7. Compaction collapse (`VersionRetention`), bottom treatment, output rollback,
   vlog pointer handling.
8. Ingestion, parts freeze/export/attach/attach_by_ref, foreign-mount rules.
9. Manifest, checkpoint/backup, clone, S3 tiers.
10. Stats, PerfContext, docs, config wiring, compatibility fixtures.

## Phase execution order

| Step | Deliverable | Review checkpoint |
| --- | --- | --- |
| 1 | **1.0A** strict masks (`UnsupportedFormat`, frozen corpus, encode normalization, decode strictness, `decode_record` → `Result`, tag dispatch loop) | golden corpus unchanged; fuzz seeds green |
| 2 | **1.0B** manifest v2 caps word, envelopes, extended block layout, enable helper | golden bytes; frozen-decoder refusal; crash matrix |
| 3 | **1.2** storage + reads + conflicts | model-based reads; conflict outcomes per isolation level; lock-inventory update |
| 4 | **1.2** compaction rules (clipping, span expansion, mount vetoes) | level-≥1 span disjointness property test; crash matrix |
| 5 | **1.2** excise (`remove_tables`, picker pre-pass, poison policy) | fault injection; manifest-before-unlink |
| 6 | **1.1** kind-4 pass-through **+ kind-aware retention**, then read resolution | operand chains survive compaction *before* reads ship; reference-model reads |
| 7 | **1.1** folding | fold oracle at every snapshot |

Steps 6 and 7 must not be split differently: retention becomes operand-aware in
the same change that first lets kind 4 reach compaction, or background
compaction silently truncates chains in the window before folding lands.

## Exit criteria

- A legacy-only database keeps writing VERSION-1 manifests (the existing
  lowest-version discipline in `encode_positional_tails`).
- Unknown legacy flags and invalid combinations fail closed as `Corruption`;
  unknown capability bits, unknown footer bits, unknown record kinds `< 64` and
  unknown aux sections fail closed as `UnsupportedFormat` (code `-16`) —
  distinguishable at the API and over the numeric-code boundary.
- Capability persistence provably precedes WAL/SST use under injected crashes,
  and a failed enable is documented as fail-stopping the handle (not a
  retryable no-op).
- Every new tagged manifest tail (`ONDACAP1`, `ONDARNG1`) is wired into
  `ManifestTailPresence::tagged()`, with a golden fixture that carries the tag
  and **no** positional data — the case the positional decoder gets wrong.
- Merge/range fixtures round-trip through both WAL layouts, flush, compaction,
  reopen, checkpoint/backup, and the S3 tier where supported.
- Old-binary refusal proven with a frozen decoder (`tests/frozen_decoder.rs`)
  or a real previous-release fixture, not by changing a constant.
- Both feature configurations green for every task
  (`cargo test`, `cargo test --features unsafe-fastpath`,
  `cargo clippy --all-targets`, `… --features unsafe-fastpath`).
