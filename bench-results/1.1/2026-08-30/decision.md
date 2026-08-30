# 1.1 merge operators — acceptance measurement and gate decision

**Decision: ship the feature; the gate is met on point reads and *not* met on
scan.** A column family with no merge operator scans ~5 % slower than the 0.8.2
baseline. That is below a single run's noise floor, but its **sign is consistent
across 20 process-level alternations in three sessions**, so it is a real
regression and is reported as one rather than rounded away. Point reads,
merge-vs-read-modify-write and folding all land where the feature promised.

## The machine, and why every number here is a ratio

This box is shared with ~28 other agent sessions and is thermally noisy well
past the ±15–20 % AGENTS.md documents: a plain `db.put` measured anywhere from
95 µs to 357 µs per op *within one test run*, and the scan baseline moved
41–50 ns/entry between sessions. Absolute numbers here are worthless across
sessions. Every claim below is a **same-run** or **same-alternation** ratio, and
the counter phases interleave their variants in 500-op chunks so one drift lands
in all of them.

## How this was measured

`tests/merge_bench.rs` (all `#[ignore]`d), plus `tests/scan_point_probe.rs`,
which is the only file here that compiles unchanged against **both** trees:

```sh
BENCH_DATE=2026-08-30 cargo test --release --features unsafe-fastpath \
    --test merge_bench -- --ignored --nocapture --test-threads=1
```

The no-regression comparison needs a 0.8.2 binary, so `roadmap/wave-a` was
extracted with `git archive`, given the same probe file, built with the same
flags, and the two binaries were **alternated at the process level** — one
baseline run, one 1.1 run, seven times — so both halves of every ratio see the
same drift. `no-operator-vs-0.8.2.csv` is that run.

## 1. Counter workload: merge versus Get+Put at equal durability

Both paths at `SyncMode::None`, so the number is engine CPU plus the conflict
window, not an fsync both would pay identically. A plain `put` of the same shape
is measured alongside as the floor.

| phase | median merge | median get+put | **speedup** |
|---|---:|---:|---:|
| single-threaded, 500 keys (`counter.csv`) | 213 µs/op | 246 µs/op | **×1.15** |
| 8 threads, 16 hot keys (`counter-contended.csv`) | 81 µs/op | 244 µs/op | **×3.02** |

The single-threaded row is the honest floor of the claim: one thread never
conflicts, so the only saving is the `get`. The contended row is where the
feature actually lives, and it carries a number that is **not** a timing and so
does not care how loaded this machine is:

> Read-modify-write took **1,231–2,278 commit retries** per run to land 12,000
> increments. `merge` took **0**, by construction — it never reads, so it has
> nothing to validate.

## 2. Chain length over time, folding on and off

Eight rounds of 20,000 operands over 500 keys, each round flushed and compacted;
`operands_on_disk` counts kind-4 entries in the family's SSTables
(`chain-length.csv`).

| round | folding on: operands / point read | folding off: operands / point read |
|---:|---|---|
| 1 | 0 / 2.14 µs | 20,000 / 4.43 µs |
| 4 | 0 / 1.97 µs | 80,000 / 15.4 µs |
| 8 | 0 / 1.97 µs | 160,000 / 17.3 µs |

Folding on: the chain collapses to one entry every compaction and read cost is
**flat**. Folding off: operands accumulate linearly and the point read degrades
**8.8×** over eight rounds. This is what `Options::enable_merge_folding = false`
costs, and it is why the switch is a rollout control rather than a tuning knob.

## 3. The gate: a column family with NO operator configured

### 3a. Against the 0.8.2 baseline (`no-operator-vs-0.8.2.csv`)

Seven alternations, 60,000 rows, warm caches, `unsafe-fastpath`. Run 7 of the
final session hit a machine stall (302 ns/entry against a 48 ns neighbour) and is
excluded as an outlier; it is left in the CSV rather than deleted.

| metric | median 1.1 / 0.8.2 | spread | verdict |
|---|---:|---|---|
| full-scan decode CPU per entry | **×1.052** | 0.96–1.10 | consistent small regression |
| point read | **×0.995** | 0.77–1.19 | no detectable change |

Three sessions of this comparison gave scan medians of **1.050, 1.087 and
1.052**, and 18 of 20 alternations were above 1.0. One run cannot see a 5 %
effect through this machine's noise; twenty alternations with a consistent sign
can, and calling that "no measurable regression" would be dishonest.

**What it is not.** Two hypotheses were tested and both are ruled out:

* *`DecEntry` grew.* It is returned by value once per decoded entry, so its size
  is a scan cost. The record kind is stored as a `u8` beside `flags`, and
  `sst::size_probe::dec_entry_stays_the_size_it_was` pins the struct at the 72
  bytes it was before 1.1. Narrowing it from `u64` did not move the ratio.
* *A per-entry merge branch in the iterator.* `resolve_current_group` was split
  so a no-operator family runs the pre-1.1 loop with the operand check hoisted
  into one `is_some()` per group. That did not move the ratio either.

Both changes are kept — smaller struct, cleaner shape, neither regressed — but
neither explains the 5 %. What remains on that path is small and unprofiled: one
`point_kind()` (two compares) plus a byte store per legacy entry decode, and an
`Iterator` struct 56 bytes larger, which can move hot fields across a cache
line. AGENTS.md says profile first; that was not done here, and the two attempts
above were guesses. **Left as known, measured, unexplained work** rather than a
third guess — the profile (`sample` during the scan phase) is the next step for
whoever picks it up.

### 3b. Operator-configured versus not, same binary (`no-operator.csv`)

This one bounds the cost of *having* an operator, on a family holding no operands
at all, and it is what drove the read-path design:

| metric | before the fast path | after |
|---|---:|---:|
| scan | ×1.072 | ×1.072 |
| point read | **×1.814** | **×1.075** |

The first column is the naive shape: a merge family always walked the whole
version chain through an `SstIterator` seek instead of the tuned point-read block
walk. The fix is exact, not an approximation — the fold rule reads a key's
versions newest-first and stops at the first base, so if the winning version
across all sources is not an operand, the chain has no operand above its base and
the ordinary answer *is* the folded one. `get` and `multi_get` therefore run the
normal candidate pass and only fold the keys whose winner is kind 4. `multi_get`
keeps its single snapshot and single clock reading; only those keys give up the
cross-key block dedup.

## Acceptance summary

| criterion | result |
|---|---|
| counter ops/s vs Get+Put at equal durability | ×1.15 uncontended, **×3.02** contended, 0 retries vs 1,231–2,278 |
| chain length over time, folding on/off | flat vs linear growth; **8.8×** read degradation with folding off |
| no regression, scan, no-operator CF | **not met**: ×1.05, consistent sign over 20 alternations |
| no regression, point read, no-operator CF | met: ×0.995 |
| both feature configurations green | yes |
