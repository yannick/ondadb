# Quality metrics operations

`just` is the command index for these operations. Use `just --list` to see
the complete task list; `README.md` retains the explicit Cargo commands that
are authoritative for the CI-equivalent gate.

Generated reports owned by ondaDB remain under `target/` and are ignored by
Git. The narrow inherited-harness exception is the explicitly delegated legacy
sibling harness in `../bench`: it owns and writes its CSV/HTML beside itself.
Do not move or commit either kind of generated output merely to preserve a
measurement.

## Setup and command roles

Run `just tools-check` before a metrics or coverage operation. It verifies the
pinned producers and prints the matching installation command; `just
tools-install` installs all four:

| Producer | Pinned version | Used for |
| --- | --- | --- |
| `bca` | 2.1.0 | Complexity, long-function, source-line, and baseline checks |
| `cargo-geiger` | 0.13.0 | Unsafe-surface counts |
| `cargo-bloat` | 0.12.1 | Release benchmark-binary sizes |
| `cargo-llvm-cov` | 0.8.7 | Merged safe/`unsafe-fastpath` coverage |

| Command | Purpose | Writes |
| --- | --- | --- |
| `just metrics-check` | Deterministic CI ratchets: BCA baseline, unsafe counts, and long functions | `target/metrics/raw/` and the isolated Geiger target |
| `just metrics` | Collect the normalized current report; it is informational, not a gate | `target/metrics/current.json` and `target/metrics/raw/` |
| `just metrics-record LABEL` | Append one observed current report to history | `metrics/history/YYYYMMDDTHHMMSSZ-<short-sha>-<sanitized-label>.json` |
| `just metrics-baseline` | Deliberately refresh accepted deterministic debt | `metrics/baseline.json` and `.bca-baseline.toml` |
| `just coverage` | Freshly collect normalized metrics, merge coverage from both feature configurations into that same-provenance report, and atomically replace the current snapshot | `target/metrics/raw/coverage.json`, `target/metrics/coverage/html/`, and `target/metrics/current.json` |
| `just coverage-open` | Run `coverage`, then open its HTML index when the platform has an opener | the same coverage paths |
| `just hotspots` | Render a version-control churn/complexity report | `target/metrics/hotspots.html` |

`metrics-baseline` is a reviewed debt acceptance: inspect the changed baseline
and explain why a new unsafe count or long function is acceptable before
committing it. `metrics-record LABEL` is observational history, not acceptance;
it never changes a ratchet and exclusively creates a timestamped snapshot. Use
a descriptive ASCII-alphanumeric label such as `before-compaction-tuning`.

## Metric catalogue

`just metrics` writes the versioned `ondadb.metrics.v1` document to
`target/metrics/current.json`. It records collection time, Git revision and
dirty state, host details, and producer versions alongside these values.

| Metric | Definition | Better direction | Gate or report | Producer and generated path | Baseline / reduction policy |
| --- | --- | --- | --- | --- | --- |
| Cyclomatic complexity | Function complexity distribution: `max`, `median`, and `p90` | Lower | Report; BCA's committed baseline also gates its configured rules | `bca 2.1.0`; current report and `target/metrics/raw/bca-*.json` | `.bca-baseline.toml` is reviewed debt. Reduce real branching; do not game the metric by splitting one meaningless function into fragments. |
| Cognitive complexity | Function cognitive-complexity distribution: `max`, `median`, and `p90` | Lower | Report | `bca 2.1.0`; current report and BCA raw paths | Trend it; reduce nested control flow only when it makes code easier to understand. |
| Function source-span length | Per-function source span (`end_line - start_line + 1`) distribution: `max`, `median`, and `p90` | Lower | Report; functions over 200 source-span lines are ratcheted | `bca 2.1.0`; current report and BCA raw paths | `metrics/baseline.json` owns accepted over-200-line functions. Refactor coherent units, not artificial wrappers or meaningless function splitting. |
| Complexity baseline exceptions | Number of entries accepted by BCA's baseline | Lower | Report, with BCA enforcing the underlying baseline | `bca 2.1.0`; `target/metrics/current.json` | `.bca-baseline.toml`; remove exceptions by addressing the rule violation, and add one only through reviewed acceptance. |
| Unsafe surface | Counts of unsafe functions, expressions, impls, traits, and methods, collected with `unsafe-fastpath` | Lower | Ratcheted | `cargo-geiger 0.13.0`; current report and `target/metrics/raw/geiger.json` | `metrics/baseline.json`; any increase requires explicit, reviewed debt acceptance. Prefer eliminating unsafe code or shrinking its audited boundary. |
| Long functions | Fully qualified production functions whose source-span length exceeds 200, with their lengths | Lower | Ratcheted | `bca 2.1.0`; current report and BCA raw paths | `metrics/baseline.json`; each newly long function fails the gate. Reduce genuine responsibility, preserving clear control flow. |
| Source lines | Production Rust source lines under `src/`, plus integration-test Rust source lines directly collected below `tests/` (inline `#[cfg(test)]` modules under `src/` are not included in the test total) | Context only | Report | `bca 2.1.0`; `target/metrics/current.json` | No baseline; use it to interpret other trends, never as a target by itself. |
| Dependencies | Active direct and transitive Cargo packages, plus extra versions of duplicate package names | Lower | Report | Cargo metadata/tree; current report, `target/metrics/raw/cargo-metadata.json`, and `target/metrics/raw/cargo-tree-duplicates.txt` | No baseline; remove a dependency or duplicate only when it preserves required behavior and maintenance clarity. |
| Benchmark binary size | Whole-file and `.text` bytes for release `onda_bench`, in safe and `unsafe-fastpath` builds | Lower, subject to performance and functionality | Report | `cargo-bloat 0.12.1`; current report and `target/metrics/raw/cargo-bloat-*.json` | No baseline; investigate material growth rather than optimizing bytes at the cost of correctness or speed. |
| Coverage | Merged line, function, and region `count`, `covered`, and percentage for safe plus `unsafe-fastpath` | Higher | Report | `cargo-llvm-cov 0.8.7`; `target/metrics/raw/coverage.json`, `target/metrics/coverage/html/index.html`, and updated current report | No baseline. Run the long coverage task when coverage itself is being assessed; it is intentionally outside the fast gate. |
| Churn hotspots | Version-control churn combined with complexity for prioritization | Lower is generally preferable, but interpret in context | Report | `bca 2.1.0`; `target/metrics/hotspots.html` | No baseline; use it to choose review/refactoring candidates, not as a standalone quality verdict. |

Only the deterministic BCA rules, unsafe counts, and long-function list are
checked by `just metrics-check`. The other measurements are deliberately
reported for trend interpretation. Refresh a baseline only after reviewing a
failure or intentional debt change; do not refresh it to make an unexplained
regression disappear. Current reports are replaceable scratch output; history
snapshots are the committed comparison record when a maintainer chooses to add
them.

Unsafe collection copies the current checkout into a fresh temporary project
mirror before running Geiger. The copy includes current tracked, modified, and
untracked project source, but excludes repository-root administration and
generated trees such as `.git`, `.worktrees`, `.superpowers`, and `target*` so a
nested checkout cannot be counted as unused source. Geiger uses the mirror's
explicit `Cargo.toml`; its reusable Cargo target remains outside the mirror, and
the mirror is removed after both successful and failed producer runs.

## Benchmark operations and interpretation

The standalone runner defaults to five runs. Run the complete standalone suite
with:

```sh
just bench-onda
```

Run a selected phase with `just bench-phase PHASE`, or the phase shortcuts:
`just bench-put`, `just bench-get`, `just bench-forward`, `just
bench-backward`, and `just bench-delete`. The valid phases are `put`, `get`,
`forward`, `backward`, and `delete`.

Every selected phase has its required setup. A phase-only post-Put invocation
(`get`, `forward`, `backward`, or `delete` without `put`) first populates the
database untimed, then closes and reopens it before timing the selected phase.
The selected reads, scans, and deletes therefore operate on SST-resident data,
not the initial memtable, and that population plus close/reopen work is outside
the phase timer. A selected `put` phase times the population itself; a
full-suite run follows the same close/reopen boundary before its post-Put
phases.

All standalone recipes accept these positional overrides, in this order:

```text
runs ops threads key_size value_size pattern compression batch features
```

Their defaults are `5 1000000 8 16 100 random none 1000 unsafe-fastpath`.
`runs`, `ops`, `threads`, `key_size`, `value_size`, and `batch` must be positive;
`pattern` is `random` or `sequential`; `compression` is `none`, `snappy`, or
`zstd`; and `features` defaults to `unsafe-fastpath` (use `safe` for no Cargo
feature). For example, `just bench-put 5 1000000 8 16 100 random none 1000
unsafe-fastpath` runs the standard Put arm.

The runner builds the requested release `onda_bench` binary, creates a separate
database for every repetition, and writes `ondadb.benchmark.v1` to
`target/benchmarks/onda-latest.json` plus flat per-run rows to
`target/benchmarks/onda-latest.csv`. The JSON preserves workload, host, Git,
Rust compiler provenance, captured output, samples, and each phase's median,
minimum, maximum, and relative spread. Every CSV row repeats its schema,
collection time, Git, host, Rust compiler, complete workload, feature set, and
requested phases alongside the raw run measurement, so the CSV remains
self-describing when detached from the JSON. The median is the headline number;
use the spread to decide whether a difference is credible.

For cross-engine work, use the explicitly delegated legacy sibling harness:

```sh
just bench-suite
just bench-graphs
just bench-matrix
```

These commands require the sibling directory in `ONDADB_BENCH_DIR` (default
`../bench`) and respectively require `run_bench.sh`, `bench_graphs.sh`, and
`bench_matrix.sh` to exist and be executable. Their overrides are:

| Recipe | Overrides (in order) |
| --- | --- |
| `bench-suite` | `ops key_size value_size threads pattern compression` |
| `bench-graphs` | `runs ops key_size value_size threads pattern compression` |
| `bench-matrix` | `runs threads pattern compression` |

The sibling harness owns its CSV/HTML beside `../bench`; that narrow inherited
exception does not change ondaDB's rule that its own generated reports stay in
`target/`.

Benchmark hardware is thermally noisy by **±15–20%** run-to-run (and more after
sustained load). Five runs are the standalone default; do not treat a single
sample as evidence. Use A/B measurements on the same build, minutes apart, and
for cross-engine comparisons use same-run ratios rather than absolute numbers
from separate sessions. Cool the machine before recording results. See
[`../docs/performance.md`](../docs/performance.md) for the full methodology,
including deferred-work and close-time caveats.
