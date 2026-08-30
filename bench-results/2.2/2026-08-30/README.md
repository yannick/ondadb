# 2.2 — numbered manifest version edits: model measurement

Date 2026-08-30. Host `Darwin 25.5.0 arm64`. Raw data:
`edit-log-vs-full-snapshot.txt` (5 runs, release build).

## What was measured

`tests/manifest_edits.rs::manifest_edit_scale_probe`, the model test the feature
doc calls for: a **10k-table catalog** — shaped like a real parts/tiers
deployment, every table carrying a partition, a tier, an object name and an age
stamp — subjected to **4096 structural changes**, each one an
`UpdateTable{Tier, Object}` (a part-mover flip).

Two modes, same catalog, same changes, in the same process:

| | durability per change |
| --- | --- |
| `full` | one `Manifest::save`: whole catalog re-encoded, temp write + fsync + rename + dir fsync |
| `edits` | one appended `MANIFEST-EDITS` record: write + flush + fsync |

`replay` is the cost of reconstructing the catalog at open: `Manifest::load` for
`full`, `recover_catalog` (snapshot + 4096 replayed records) for `edits`.

## Result (5 runs)

| metric | full | edits | ratio |
| --- | ---: | ---: | ---: |
| bytes made durable | 5,181,034,496 | 273,202 | **18,964×** less |
| fsyncs | 8,192 | 4,096 | **2×** fewer |
| write wall time | 128–371 s | 41–118 s | **2.8–5.0×** faster |
| replay at open | 2.7–6.5 ms | 9.5–24.8 ms | 1.8–4.5× slower |

Byte and fsync counts are exact and identical across all five runs — they are
counted, not timed. Wall times are thermally noisy on this machine (AGENTS.md:
±15–20 %, worse under sustained load), which is why only same-run ratios are
quoted; the spread inside `full` alone is 2.9×.

One structural change costs **67 bytes** of durable log against a **1,264,901
byte** snapshot rewrite. That is the acceptance criterion — append bytes are
O(edit), not O(catalog) — and it is what
`ten_thousand_tables_replay_within_the_bound` asserts as a gate, expressed as a
ratio rather than an absolute so it does not become a flake generator.

## Gate decision: **pass**, with one honest caveat

- **Bytes and fsyncs: decisive.** Four orders of magnitude on bytes, an exact
  halving of fsyncs, both measured rather than timed. This is the RV-M5 evidence:
  the 12.4 MiB-per-persist figure at 100k parts is the *before* number; the
  *after* number is 67 bytes per change, independent of catalog size.
- **Write time: real but smaller than the byte ratio.** 2.8–5.0×, not 18,964×,
  because both modes pay one fsync per change and an fsync on this device costs
  far more than the bytes it carries. The extra factor comes from `full`'s second
  (directory) fsync and from re-encoding 1.2 MB each time. The honest claim is
  "fewer fsyncs and vastly fewer bytes", not "18,000× faster".
- **Replay is slower, by design, and bounded.** Reconstructing from a snapshot
  plus 4096 records costs 9.5–24.8 ms against 2.7–6.5 ms for a bare load. That
  is the price of not rewriting the catalog 4096 times, it is paid once per open,
  and the compaction trigger (`> 4 MiB` or `> 4096` records) caps it. Not a
  regression worth trading the write side for.

### A measured regression this probe caught, and the fix

The first run of this probe reported **1.35 s** of replay, not 15 ms.
`apply_edit` builds its precondition index from the manifest on every call, so
replaying N records against a C-table catalog was O(C×N) — 41 M hash inserts
here. `CatalogReplayer` now builds that index once and reuses it across the
records of one recovery; `apply_edit` keeps its strictly all-or-nothing
single-edit semantics, and
`the_replayer_matches_repeated_apply_edit` pins the two to the same result.
87× on replay, and the property the acceptance criterion actually asks for —
*bounded* replay — only holds because of it.

## Not measured here — since measured

The flush/compaction p99 evidence at the 10k-table fixture could not be produced
at the time of this record: the twenty `persist_manifest` call sites had **not**
been migrated onto `catalog_txn`, so a running database still paid a full rewrite
per flush whether or not the capability was enabled, and any benchmark would have
measured the unchanged path.

It landed with the migration commit (2.2 slice 9):
`bench-results/2.2-slice9/2026-08-30/`. Summary of that record — flush p50 89.1 →
64.3 ms and p99 150.6 → 115.4 ms at a 270 KB catalog (4/5 paired runs each),
compaction p50 63.5 → 47.5 ms (5/5); compaction p99 **inconclusive** at 25
samples per run and reported as such.
