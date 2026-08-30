# 0.1 Per-level Bloom policy — acceptance measurements

**Date:** 2026-08-30. **Host:** darwin 25.5.0 (the machine `docs/performance.md`
describes as thermally noisy). **Build:** `--release`, both feature configs, 5
invocations each. **Harness:** `tests/bloom_policy_bench.rs` (`#[ignore]`d):

```sh
cargo test --release --test bloom_policy_bench -- --ignored --nocapture
cargo test --release --features unsafe-fastpath --test bloom_policy_bench -- --ignored --nocapture
```

Raw output: `raw-runs.txt` (10 invocations × 4 arms).

**Acceptance bar** (`docs/plans/phase-0-runtime/features/01-per-level-bloom-fpr.md`):

> Miss-heavy phase: negative-lookup probes to upper levels drop measurably at
> equal resident bytes, or the feature stays opt-in documentation. Hit-heavy
> phase with bottom omission: `DB::reader_memory()` filter bytes drop without
> p99 regression beyond the ≥5-run spread. Publish both, with the one-way
> degradation restated in the note.

## Verdict

**Miss-heavy bar: NOT met — the feature ships opt-in, exactly as the bar's own
fallback clause provides for.** At matched resident bytes the per-level vector
moves miss-path `sstable_probes` by −7.4% (754 → 698), which is real and
repeatable (the counters are deterministic across all 5 runs) but nowhere near
"measurable" against a p50 that does not move at all. The default stays an
empty vector.

**Hit-heavy bar: MET.** `optimize_filters_for_hits` cuts resident filter bytes
by **50.1%** (246,800 → 123,104 B; total resident reader bytes −20.1%) and hit
p99 *improves* — −22.6% default, −20.8% `unsafe-fastpath` — well outside a
5-run spread that is itself ±15%. There is no p99 regression to weigh against
it. It still ships off by default because of the miss-path cost below, which is
the trade the option exists to offer, not a defect.

## Results (median of 5 runs per arm; counters are deterministic)

Fixture: 200,000 keys × 96 B values, levels `[L0=0, L1=4, L2=8]` files, 50,000
probes per phase. Arms differ **only** in the filter policy.

| arm | `bloom_fpr_per_level` | `optimize_filters_for_hits` |
| --- | --- | --- |
| `uniform` | *(empty — 0.01 everywhere)* | false |
| `per_level_equal` | `[0.001, 0.001, 0.02]` | false |
| `per_level_lean` | `[0.001, 0.005, 0.05]` | false |
| `hits` | `[0.001, 0.001, 0.02]` | **true** |

### default config

| arm | filter bytes | resident bytes | miss p50 | miss p99 | hit p50 | hit p99 | miss `sstable_probes` | miss `bloom_negatives` | hit `block_misses` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| uniform | 246,800 | 616,588 | 167 ns | 916 ns | 458 ns | 3,875 ns | 754 | 78,924 | 5,290 |
| per_level_equal | 263,048 | 632,836 | 167 ns | 1,000 ns | 458 ns | 3,750 ns | **698** | 78,980 | 5,220 |
| per_level_lean | 201,600 | 571,388 | 167 ns | 1,250 ns | 458 ns | 3,459 ns | 1,984 | 77,694 | 4,271 |
| hits | **123,104** | 492,892 | 417 ns | 875 ns | 458 ns | **3,000 ns** | 34,404 | 45,274 | 1,914 |

### unsafe-fastpath

| arm | filter bytes | resident bytes | miss p50 | miss p99 | hit p50 | hit p99 | miss `sstable_probes` | miss `bloom_negatives` | hit `block_misses` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| uniform | 246,800 | 616,588 | 167 ns | 916 ns | 458 ns | 1,209 ns | 754 | 78,924 | 0 |
| per_level_equal | 263,048 | 632,836 | 167 ns | 875 ns | 416 ns | 1,084 ns | **698** | 78,980 | 0 |
| per_level_lean | 201,600 | 571,388 | 167 ns | 1,125 ns | 417 ns | 1,125 ns | 1,984 | 77,694 | 0 |
| hits | **123,104** | 492,892 | 375 ns | 708 ns | 375 ns | **958 ns** | 34,404 | 45,274 | 0 |

`block_misses` is 0 throughout `unsafe-fastpath` because that config serves
uncompressed klog blocks from the mmap without going through the block cache.

## Reading the numbers

**Why the per-level vector barely moves.** Monkey's premise is that a negative
lookup probes *every* level, so a bit spent on a small upper level buys more
than a bit spent on the huge bottom one. ondaDB only probes tables whose
`[min, max]` covers the key, and leveled compaction makes each level's tables
disjoint — so the candidate count per lookup here is 1.59 (79,678 bloom probes
for 50,000 lookups), not one per level. Redistributing bits across a cascade
that short cannot buy much: `per_level_equal` at +6.6% filter bytes returns
−7.4% miss probes, and `per_level_lean` shows the honest cost of the other
direction — −18.3% filter bytes for **+163%** miss probes.

That 1.59 is itself only reachable because the harness writes a scattered
overlay pass after the bulk load. Without it, every level owns a *disjoint*
slice of the keyspace, the candidate count is exactly 1.00, and a per-level
policy has no cascade at all to shorten. A benchmark that loads once and
compacts once will therefore always show per-level rates losing; that is a
fixture artifact, and it is recorded here so the next reader does not take it
for a result.

**Why `optimize_filters_for_hits` is a genuine trade, in both directions.**
Miss-path `sstable_probes` go 754 → 34,404 (**46×**) and miss p50 167 ns →
417 ns (**+150%**): a bottom table with no filter is read on every negative
lookup, so misses that used to end at the filter now reach a data block. In
exchange half the filter bytes go away and the hit path gets *faster* — fewer
resident bytes, no filter hash, and (in the default config) a warmer block
cache, since the filterless bottom table is read on every probe. Enable it only
for a workload known to hit.

## One-way degradation (restated, as the acceptance section requires)

"Bottom" is dynamic. A table written filterless while level N was bottom keeps
no filter once a deeper level appears, and **nothing retro-fits one** — the
negative-lookup cost above becomes permanent for that table until a compaction
rewrites it. Reads stay correct throughout: a missing filter means "may
contain" (`Reader::bloom_may_contain_hash` returns `true`), so only performance
degrades, never an answer.

The converse is guaranteed and tested: any compaction whose output target is
not bottom writes a filter, whatever its inputs carried
(`tests/db.rs::non_bottom_compaction_output_regains_a_filter`, verified
non-vacuous by forcing `bottom = true` and watching it fail).

## Rollback

An empty `bloom_fpr_per_level` plus `optimize_filters_for_hits: false` — the
defaults — is byte-for-byte the previous behaviour, including the config blob,
which elides the `ONDABLM1` tail entirely at the defaults. Tables already
written are ordinary SSTables; a filterless one stays filterless until
recompacted, which is the documented one-way part.
