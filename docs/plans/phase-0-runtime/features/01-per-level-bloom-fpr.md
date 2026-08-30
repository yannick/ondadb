# 0.1 — Per-level Bloom policy

**Readiness:** ready once 0.10 provides counters. **Effort:** 1–2 dev-weeks.
**wavesdb counterpart:** 0.1 (with their *corrected* claim: skipping the
bottom filter helps only hit-heavy workloads — the old "bottom filter is least
useful for negatives" argument was backwards).

## Goal

Choose bloom FPR by output level: small upper levels get strong (cheap)
filters; the huge bottom level makes an explicit memory/IO trade. Inspired by
Monkey, but an engineering approximation — no constrained optimizer, no
promised latency gains.

## Baseline (verified at 0.8.2)

- Two writer-option constructors pass `opts.bloom_fpr` uniformly:
  `ColumnFamily::writer_opts(expected)` (flush and ingest — its own comment is
  "Flush and ingestion always write L0") and `compaction::cf_writer_opts(cf,
  cmp, target_level)`, which already receives the output level and already
  varies `compression` by it (`compression_for_level`).
- Filters are sized from the **actual** distinct-key count: the writer buffers
  `bloom_hashes: Option<Vec<u64>>` and builds `Bloom::new(hashes.len().max(1),
  self.opts.bloom_fpr)` at finish. An FPR change is therefore honest per table.
  `cf_writer_opts`' own comment records why (`expected_entries: 4096` is a
  capacity hint only).
- `enable_bloom_filter: bool` and `bloom_fpr: f64` already exist on
  `ColumnFamilyConfig`.
- A missing bloom block already means "may contain":
  `Reader::bloom_may_contain_hash` returns `true` whenever `self.bloom` is
  `None`. Omission is reader-compatible forever.
- `compression_for_level` is the semantics to mirror: `[] => self.compression`,
  otherwise `v[(level as usize).min(v.len() - 1)]` — last element repeats.
- `DB::reader_memory()` exists (a 5-tuple from
  `TableCache::resident_breakdown`) and is the resident-filter-bytes source for
  acceptance.
- `WriterOptions` already carries `enable_bloom: bool` beside `bloom_fpr`, so
  there are two ways to end up with no filter block. Keep them distinct:
  `enable_bloom == false` is the CF-wide "never build filters" switch;
  `bloom_fpr == None` (below) is the per-output-level decision.

### The real `bottom` predicate

`compact_inputs` does **not** use `levels.len() - 1`:

```rust
let num_levels = cf.with_levels(|levels| levels.len()).max(target + 1);
let bottom = target >= num_levels - 1
    && cf.with_levels(|levels| levels.iter().skip(target + 1).all(|level| level.is_empty()));
```

It additionally requires every deeper level to be empty, and clamps
`num_levels` up to `target + 1`. Re-deriving `levels.len() - 1` would strip
filters from tables `compact_inputs` does not consider bottom. The predicate
must be **extracted once** and shared.

## Design

New persisted CF options:

```rust
pub bloom_fpr_per_level: Vec<f64>,   // empty: uniform bloom_fpr (default)
pub optimize_filters_for_hits: bool, // false; omit the filter on COMPACTION
                                     // output written into a bottom level
```

- `validate`: every element finite and strictly in `(0, 1)`; last element
  repeats for deeper levels.
- Resolution helper (**new**, `config.rs` beside `compression_for_level`):

  ```rust
  pub fn bloom_fpr_for_level(&self, level: u32, bottom: bool) -> Option<f64> {
      if self.optimize_filters_for_hits && bottom {
          return None;                       // write no filter block
      }
      match self.bloom_fpr_per_level.as_slice() {
          [] => Some(self.bloom_fpr),        // guards the underflow: no len-1
          v  => Some(v[(level as usize).min(v.len() - 1)]),
      }
  }
  ```

  The empty-slice arm exists so `v.len() - 1` is never evaluated on an empty
  vector (a `usize` underflow, not a fallback).
- `WriterOptions::bloom_fpr: f64` becomes `bloom_fpr: Option<f64>` (**new**
  shape) — `None` means "write no bloom block". `Writer::finish` already has
  the `Option<Vec<u64>>` buffer to skip.
- **`bottom` is a compaction-only input.** Extract the predicate above into
  `compaction::is_bottom_target(cf, target) -> bool` (**new**) and call it from
  both `compact_inputs` and `cf_writer_opts`. `writer_opts` (flush/ingest)
  passes `bottom = false` unconditionally: it always writes L0, and a fresh CF's
  `levels` is `vec![Vec::new()]`, so L0 *is* bottom by the predicate —
  honouring `optimize_filters_for_hits` there would strip the filter from every
  table in a young CF's only level. That is not the trade the option offers.
- Deliberately **not** adopting wavesdb's `BloomAutoAllocate` (geometric
  Monkey-style allocation) in v1: it is mutually exclusive with the explicit
  vector and adds a policy without a consumer. Reserve the config tail tag,
  don't implement.

### Filter omission is a one-way degradation

"Bottom" is dynamic. A table written filterless while level N was bottom keeps
no filter after a deeper level appears; negative lookups against it stay
degraded with no repair path short of recompacting it. Reader compatibility is
never at risk (`bloom_may_contain_hash` → `true`), but the *performance*
contract is. Two things follow, both required:

1. Document the degradation on the option: "tables written filterless are not
   retro-fitted; enabling this option is a decision about the data already in
   the bottom level as much as about future writes."
2. **Re-filter on promotion:** any compaction whose output target is not
   bottom writes a filter, whatever the inputs carried. This falls out of the
   design for free (the decision is made per output from `target` + `bottom`,
   never inherited from inputs) — state it explicitly so a later refactor
   cannot start inheriting.

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

1. **Option wiring.** `config.rs`: add both fields to `ColumnFamilyConfig`,
   `Default`, `validate`, and a new config-blob tag pair
   (`encode_bloom_policy` / its cursor arm, modelled on `encode_block_size` —
   elide when equal to default so old blobs still decode).
   Test first: `tests/db.rs::bloom_policy_round_trips_through_reopen` — set
   `bloom_fpr_per_level = vec![0.001, 0.01, 0.05]` and
   `optimize_filters_for_hits = true`, reopen, assert both fields survive; and
   `config.rs` unit tests `validate_rejects_bloom_fpr_out_of_range` (0.0, 1.0,
   `f64::NAN`, negative → `Err`) and
   `bloom_policy_blob_omits_defaults` (a default config's blob is byte-equal to
   today's).
2. **Resolution helper.** `config.rs::bloom_fpr_for_level`. Test first:
   `config.rs::bloom_fpr_for_level_repeats_last_element` — empty vector at
   levels 0/3 → `Some(bloom_fpr)` (asserting no panic on empty, i.e. the
   underflow guard); `[0.001, 0.01]` at levels 0/1/2/7 → `0.001, 0.01, 0.01,
   0.01`; `optimize_filters_for_hits` with `bottom = true` → `None`, with
   `bottom = false` → `Some(..)`.
3. **Extract the bottom predicate.** `compaction.rs`: lift the two-line
   `num_levels`/`bottom` computation out of `compact_inputs` into
   `is_bottom_target(cf, target) -> bool` and call it from `compact_inputs`.
   No behavior change. Test first:
   `compaction.rs::is_bottom_target_requires_deeper_levels_empty` — a CF whose
   `levels` has a populated level below `target` is not bottom, one whose
   deeper levels are all empty is, and `target >= levels.len()` is.
4. **Writer plumbing.** `sst/mod.rs` (`WriterOptions::bloom_fpr` →
   `Option<f64>`), `sst/writer.rs` (skip the bloom block when `None`; keep
   sizing from `hashes.len().max(1)` when `Some`). Update both constructors to
   compile: `writer_opts` passes `self.opts.bloom_fpr_for_level(0, false)`,
   `cf_writer_opts` passes
   `cf.opts.bloom_fpr_for_level(target_level, is_bottom_target(cf, target_level))`.
   Test first: `tests/sst.rs::writer_omits_bloom_block_when_fpr_is_none` —
   write a table with `bloom_fpr: None`, reopen the reader, assert
   `bloom_may_contain_hash` returns `true` for a key that was never written
   (i.e. the filter is absent, not empty), and that every written key still
   reads back.
5. **End-to-end level policy.** No new code; wiring verification.
   Test first: `tests/db.rs::per_level_bloom_fpr_is_applied_by_output_level` —
   a CF with `[0.001, 0.05]`, force flush + compaction to L1, probe ~10k absent
   keys per level and assert the measured false-positive rate at L1 is
   materially above L0's and each is within a generous factor of its
   configured target (the assertion is *ordering plus order-of-magnitude*, not
   an exact rate — filter sizing is per table).
6. **Bottom omission.** Test first:
   `tests/db.rs::optimize_filters_for_hits_omits_only_compaction_bottom_output`
   — with the option on: a flushed L0 table in a fresh CF still carries a
   filter (task 4's `bottom = false` for flush), a compacted bottom table does
   not, and after a deeper level is created the previously-bottom table still
   reads correctly. Plus
   `tests/db.rs::non_bottom_compaction_output_regains_a_filter` — compact a
   filterless bottom table upward into a non-bottom target and assert the
   output has a filter.
7. **Legacy coexistence.** Test first:
   `tests/db.rs::mixed_filter_tables_in_one_level_read_correctly` — build one
   level holding tables written under a uniform FPR and under the per-level
   vector (and one filterless), assert every key resolves and scans are
   complete.
8. **Harness + decision note.** `../bench`: miss-heavy and hit-heavy
   point-read phases; publish `DB::reader_memory()` resident filter bytes beside
   latency. Reuse 0.2's overlapping-level fixture generator to build the
   multi-level state. Record the acceptance decision in the doc. No new tests;
   the gate still runs.

## Tests (summary)

- Round-trip of both options through manifest/config reopen.
- `validate` rejects out-of-range, NaN, and non-finite FPRs.
- Resolution helper: empty vector, repeat-last, bottom omission.
- Per-level FP-rate ordering on real tables; bottom omission applies to
  compaction output only; non-bottom output always filtered.
- Legacy uniform-FPR tables coexist in one level and read correctly.
- Both feature configs (the mmap path shares `Reader::bloom_*` unchanged, so
  this is a compile-and-run check, not a second code path).

## Acceptance

Miss-heavy phase: negative-lookup probes to upper levels drop measurably at
equal resident bytes, or the feature stays opt-in documentation. Hit-heavy
phase with bottom omission: `DB::reader_memory()` filter bytes drop without p99
regression beyond the ≥5-run spread. Publish both, with the one-way
degradation restated in the note.

## Rollback

Empty vector + `false` = current behavior; already-written tables are ordinary
SSTables (a filterless one stays filterless until recompacted — that is the
documented one-way part).
