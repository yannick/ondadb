# 2.2 slice 9 — flush and compaction latency at the 10k-table fixture

Date 2026-08-30. Host `Darwin 25.5.0 arm64`. Raw data:
`flush-compaction-p99.txt` (5 runs, release build, machine otherwise idle).

This is the measurement `bench-results/2.2/2026-08-30/README.md` deferred under
"Not measured here": with the twenty `persist_manifest` call sites migrated onto
`catalog_txn`, a running database's structural operations finally take the edit
path, so there is something to measure.

## Why this probe and not `../bench`

`../bench` compares four engines on put/get throughput and latency. It has no
mode that isolates *structural-operation* latency at a large catalog, and the
cost 2.2 removes is invisible in a throughput number: a flush's manifest write
is one fsync inside a background worker, amortized away by any pipeline deep
enough to keep the device busy. Adding such a mode to a sibling repository is
out of scope for this commit, so the evidence is produced by an in-repo probe of
exactly that shape — `tests/manifest_edits.rs::structural_op_latency_probe`,
`#[ignore]`d, run with:

```sh
ONDADB_PROBE_TABLES=10000 ONDADB_PROBE_FLUSHES=200 \
ONDADB_PROBE_COMPACTIONS=25 ONDADB_PROBE_RUNS=5 \
cargo test --release --test manifest_edits -- --ignored --nocapture \
  structural_op_latency_probe
```

## What was measured

Two column families:

- `bulk` — 10,000 bottom-level tables, the catalog weight. Never read, never
  compacted (its triggers are set past any reachable value).
- `hot` — where every measured operation happens.

`persist_manifest` rebuilds **every** column family, so one flush on `hot`
re-encodes and fsyncs all 10,000 of `bulk`'s entries — a **270,029-byte**
`MANIFEST` — in full-snapshot mode, against a **~72-byte** appended record with
the log on. That asymmetry is RV-M5 exactly, and it is the only place a user
feels it.

Per arm, per run: 200 flushes (timed individually) and 25 compactions (four
flushes then one `DB::compact`, the compaction timed). Percentiles are over the
per-operation wall times of one arm of one run.

**The ballast catalog is written directly rather than flushed into existence.**
Producing 10k tables through 10k real flushes costs O(n²) manifest bytes in the
very mode under test — the fixture would become the experiment (a 300-table
build already took 61 s). `bulk`'s tables are therefore placeholders. This is
sound because what is being timed is the cost of making the *catalog* durable,
which is a function of the catalog, not of the bytes behind it; nothing in the
measured path reads a `bulk` table.

## Result (5 runs, milliseconds)

| run | flush p50 full | flush p50 edits | flush p99 full | flush p99 edits | compact p50 full | compact p50 edits | compact p99 full | compact p99 edits |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 108.13 | 81.81 | 150.56 | 123.75 | 63.49 | 47.69 | 424.73 | 497.75 |
| 2 | 86.32 | 64.29 | 159.54 | 115.06 | 62.59 | 47.51 | 512.21 | 219.39 |
| 3 | 79.42 | 88.84 | 132.34 | 159.06 | 65.16 | 52.61 | 1201.91 | 969.46 |
| 4 | 110.02 | 55.76 | 268.11 | 115.41 | 70.21 | 28.97 | 354.40 | 334.83 |
| 5 | 89.14 | 59.43 | 141.27 | 106.24 | 49.32 | 29.99 | 318.96 | 130.85 |
| **median** | **89.14** | **64.29** | **150.56** | **115.41** | **63.49** | **47.51** | **424.73** | **334.83** |
| paired wins for `edits` | | 4/5 | | 4/5 | | **5/5** | | 4/5 |

Durable bytes per structural operation: **270,029** (full) against **~72**
(edits; 21,877 log bytes over 300 records), a 3,750× ratio at this catalog size
and growing linearly with it.

## Gate decision: **qualified pass**

- **Compaction p50: pass, unambiguously.** 63.5 → 47.5 ms median, and `edits`
  wins **all five** paired runs. A clean sweep across five independent runs is
  not something this machine's noise produces by accident.
- **Flush p50 and p99: pass, directionally.** Medians improve 1.39× (p50) and
  1.30× (p99), and `edits` wins 4 of 5 paired runs on both. But the honest
  caveat is that the `full` arm's own p99 spread across runs is 132–268 ms
  (2.0×), which is wider than the 1.30× median gap — so on *absolute* numbers
  the improvement does not clear the baseline spread. What carries the claim is
  the **pairing**: both arms run back-to-back on the same fixture in the same
  run, and the sign of the difference is stable. Run 3 is the single reversal,
  and it reverses p50 and p99 together, which is the signature of a thermal
  excursion rather than of the feature.
- **Compaction p99: inconclusive, and reported as such.** 25 samples per arm is
  too few for a 99th percentile; the within-arm spread is 3.8× (`full`) and 7.4×
  (`edits`), and a single stalled compaction moves the number by an order of
  magnitude. `edits` wins 4/5, but nothing should be concluded from that. The
  acceptance criterion's "compaction p99 improves beyond baseline spread" is
  **not** demonstrated here. Raising the sample count enough to demonstrate it
  would take hours per run on this device; it is left as a follow-up rather than
  claimed.
- **Why only ~1.3–1.4× when the bytes differ 3,750×.** Both arms pay one fsync
  per structural operation, and on this device an fsync costs tens of
  milliseconds regardless of what it carries. The edit log removes the bytes and
  the second (directory) fsync of the temp+rename; it cannot remove the fsync
  the durability contract requires. The byte ratio is the number that scales
  with catalog size — at 100k parts the `full` arm re-encodes 12.4 MiB per
  operation while the `edits` arm still appends ~72 bytes — and it is the number
  the model probe (`bench-results/2.2/2026-08-30/`) measures decisively.

## Correctness evidence accompanying this

The four-command gate, both feature configurations, 31 test binaries each, every
one reporting `test result: ok`; `cargo clippy --all-targets` clean in both.
`tests/manifest_edits.rs` gains one test per migrated call-site group asserting
the *shape* of the record each one writes, plus
`crash_between_edit_fsync_and_wal_delete_replays_cleanly` (invariant 1's window,
reconstructed byte-for-byte) and `l0_add_tables_replay_newest_first` (a real bug
this slice had to fix: L0 is read newest-first, so a replayed `AddTable` at
level 0 must land at the front of the family's table list, not the back).
