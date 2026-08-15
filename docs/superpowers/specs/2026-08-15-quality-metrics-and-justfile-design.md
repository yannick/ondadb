# Quality Metrics and Justfile Design

## Purpose

ondaDB will gain a repository-native quality measurement system and a single
task-running interface. The system must expose concrete engineering signals,
ratchet deterministic regressions, preserve the engine's two-configuration
quality gate, and keep noisy performance data observational.

The implementation must not introduce a composite quality score. Individual
metrics remain visible so maintainers can distinguish code complexity, test
coverage, unsafe surface, dependency growth, binary growth, and runtime
performance.

## Principles

- Keep configuration, baselines, and intentionally recorded history in Git.
- Put disposable reports under `target/`.
- Pin external analysis tools and make installation explicit.
- Fail on deterministic regressions; report noisy or immature signals.
- Make baseline replacement an explicit command, separate from measurement.
- Preserve benchmark comparability and never use cross-machine absolute
  performance as a gate.
- Keep the `justfile` declarative; Python scripts own normalization and
  comparison logic.

## Metric Model

### Hard ratchets

The following checks fail when new code exceeds a threshold, or when a
baseline offender becomes worse:

| Metric | Initial threshold | Scope |
|---|---:|---|
| Cyclomatic complexity | 15 | Per function in production Rust source |
| Cognitive complexity | 20 | Per function in production Rust source |
| Logical function length | 200 | Per function in production Rust source |
| Unsafe surface | No increase | Project-owned unsafe functions, expressions, impls, traits, and methods |

Existing complexity offenders are recorded in `.bca-baseline.toml`. Existing
unsafe use is recorded in `metrics/baseline.json`. Updating either baseline is
an explicit maintainer action with a reviewable diff.

The current format, Clippy, and test expectations also remain hard gates:

```sh
cargo fmt --check
cargo test
cargo test --features unsafe-fastpath
cargo clippy --all-targets
cargo clippy --all-targets --features unsafe-fastpath
```

Test runs must preserve full output and their exit status. Automation must not
infer success from the absence of `FAILED`, truncate output with `tail`, or
otherwise hide an earlier test binary's result.

### Tracked trends

The first release records but does not gate:

- line, function, and region coverage merged across the default and
  `unsafe-fastpath` configurations;
- production and test source lines;
- maximum, median, and p90 per-function cyclomatic and cognitive complexity;
- the number of baseline complexity exceptions;
- unsafe surface by category in fast-path builds;
- direct and transitive dependency counts and duplicate dependency versions;
- release file size and `.text` size for safe and fast-path `onda_bench`;
- source-code complexity multiplied by Git churn, to identify maintenance
  hotspots; and
- benchmark median and dispersion with Git revision, features, workload, and
  machine metadata.

Coverage, binary size, and performance can become ratchets only after their
collection is stable and representative baselines have accumulated.

### Deliberate non-metrics

Composite quality scores, comment percentage, raw test count, Halstead
estimates of bugs or implementation time, and absolute cross-machine
performance thresholds are not optimization targets. They are too indirect or
too easy to improve without improving the engine.

## Tooling

The initial external tool set is pinned:

| Tool | Version | Responsibility |
|---|---:|---|
| `big-code-analysis-cli` (`bca`) | 2.1.0 | Per-function metrics, thresholds, baselines, and churn hotspots |
| `cargo-llvm-cov` | 0.8.7 | Merged source-based coverage |
| `cargo-geiger` | 0.13.0 | Project-owned unsafe surface |
| `cargo-bloat` | 0.12.1 | Release `.text` size and contributors |
| `just` | >= 1.43.1 | Task discovery and orchestration |

The `tools-install` recipe is opt-in. All other recipes check prerequisites and
fail with a command that installs the missing pinned version. Measurement must
never modify a developer's global toolchain implicitly.

`bca` is selected because it supports committed baselines and regression-only
gates directly. `cargo-llvm-cov` accumulates raw coverage from multiple feature
configurations before producing one report. `cargo-geiger` supplies semantic
unsafe counts instead of textual keyword matching. `cargo metadata
--format-version 1` and `cargo tree --duplicates` provide dependency data
without another tool.

References:

- <https://dekobon.github.io/big-code-analysis/recipes/baselines.html>
- <https://github.com/taiki-e/cargo-llvm-cov>
- <https://github.com/geiger-rs/cargo-geiger>
- <https://github.com/RazrFalcon/cargo-bloat>
- <https://github.com/casey/just>

## Repository Layout

The implementation creates or modifies these units:

- `justfile`: public grouped command interface and colorful default help.
- `bca.toml`: analyzed paths, exclusions, thresholds, and baseline location.
- `.bca-baseline.toml`: current complexity debt.
- `metrics/baseline.json`: unsafe ratchet and trend reference.
- `metrics/history/*.json`: explicitly recorded, append-only snapshots.
- `metrics/README.md`: definitions, interpretation, tool versions, and baseline
  update policy.
- `tools/metrics.py`: collection, normalization, comparison, and terminal
  summaries using only the Python standard library.
- `tools/benchmark.py`: repeated execution, benchmark parsing, statistics, and
  machine-readable output using only the Python standard library.
- `src/bin/onda_bench.rs`: benchmark phase selection with correct prerequisite
  setup.
- tests for metric comparison and benchmark phase behavior.

Disposable raw and rendered files live in `target/metrics/` and
`target/benchmarks/` and are covered by the existing `/target` ignore rule.

## Just Interface

Running bare `just` prints an ANSI-colored, grouped help screen containing each
public recipe's description, parameters, and defaults. Private helper recipes
start with `_` and do not appear in help.

The public recipes are grouped as follows:

### Quality

- `fmt`
- `lint-safe`
- `lint-fast`
- `test-safe`
- `test-fast`
- `check`

`check` runs format, both Clippy configurations, both test configurations, and
the deterministic metric ratchets. It fails at the first failing gate while
leaving the failing command's complete output visible.

### Metrics

- `metrics`: generate raw reports and a human summary without gating.
- `metrics-check`: run complexity and unsafe ratchets.
- `metrics-record LABEL`: write a new history snapshot with a sanitized label,
  UTC timestamp, and Git revision; never overwrite an existing snapshot.
- `metrics-baseline`: deliberately regenerate both committed baselines.
- `coverage`: collect merged JSON and HTML coverage.
- `coverage-open`: collect coverage and open its HTML index when the platform
  supports it.
- `hotspots`: show complexity-by-churn rankings.

### Benchmarks

- `bench-onda`: run the full standalone ondaDB workload repeatedly.
- `bench-phase PHASE`: run one of `put`, `get`, `forward`, `backward`, or
  `delete` repeatedly.
- `bench-put`, `bench-get`, `bench-forward`, `bench-backward`, and
  `bench-delete`: discoverable convenience recipes.
- `bench-vlog`: run the ignored vlog repeat-read benchmarks.
- `bench-cf`: run the ignored column-family creation benchmark.
- `bench-suite`: run the sibling harness's one-shot comparison.
- `bench-graphs`: run its repeated single-configuration report.
- `bench-matrix`: run its multi-size report.

The onda-only recipes default to five measured runs. Workload size, thread
count, key size, value size, pattern, compression, batch size, feature set, and
run count remain overridable without editing the `justfile`. Sibling-harness
recipes pass through that harness's supported environment variables.

### Setup

- `tools-check`: report every required tool and expected version.
- `tools-install`: explicitly install the pinned Cargo tools; it does not
  install `just` because the recipe cannot run without it.

## Metrics Data Flow

`tools/metrics.py collect` invokes each producer, validates its exit status and
schema, and writes raw output under `target/metrics/raw/`. It then creates one
normalized snapshot containing:

- schema version;
- UTC collection time and Git revision;
- tool versions;
- feature configurations;
- normalized metric values; and
- enough host information to interpret platform-dependent binary metrics.

Human output is derived from that normalized document. `check` compares only
the deterministic fields against committed baselines. `record` copies the
normalized document into history using exclusive creation so an existing
snapshot cannot be replaced accidentally.

Coverage collection performs this exact sequence:

1. Clean the coverage workspace once.
2. Run default tests with `--no-report`.
3. Run `unsafe-fastpath` tests with `--no-report` without cleaning accumulated
   profiles.
4. Generate merged JSON and HTML reports.
5. Parse line, function, and region totals into the normalized snapshot.

This covers both compile-time implementations without pretending an
`--all-features` run is equivalent to the project's required matrix.

## Benchmark Phase Semantics

`onda_bench` gains `-phases` with a comma-separated subset of `put`, `get`,
`forward`, `backward`, and `delete`. The default remains all phases in the
existing order and retains the current output labels consumed by the sibling
harness.

Every requested phase produces valid state independently:

- `put` creates an empty database and times population.
- `get`, `forward`, `backward`, and `delete` populate the database first when
  `put` is not measured.
- disk-read phases close and reopen after population before their timer starts.
- `delete` starts with all requested keys present.
- prerequisite population, close, reopen, and cleanup are never charged to the
  selected phase.

Unknown or empty phase selections fail before deleting or creating benchmark
data. Existing CLI output stays parseable by `bench_graphs.sh`.

`tools/benchmark.py` runs the configured number of repetitions, parses each
requested phase, rejects missing or duplicate phase lines, and reports median,
minimum, maximum, and relative spread. JSON and CSV include the full workload,
feature set, host facts, Git revision, and raw per-run values.

Performance is never a quality gate. ondaDB-only results support close-in-time
A/B work; multi-engine results must be interpreted as same-run ratios. The
documentation repeats the project's ±15–20% thermal-noise warning.

## Error Handling and Safety

- Missing tools produce actionable, version-pinned installation messages.
- Tool errors and metric-gate failures remain distinct exit statuses where the
  upstream tool distinguishes them.
- Malformed or partial JSON fails collection; it is never recorded as zero.
- Snapshot writes use temporary files plus atomic replacement for disposable
  current reports and exclusive creation for committed history.
- Baseline generation prints the paths changed and requires the explicitly
  named recipe.
- Benchmark phase and numeric arguments are validated before the database path
  is touched.
- Sibling benchmark recipes fail clearly when `../bench` is absent.
- Benchmark automation does not suppress build or benchmark failures.

## Verification

Focused tests cover:

- default and grouped `just` help, including ANSI color and descriptions;
- missing-tool diagnostics;
- normalization and comparison against JSON fixtures;
- detection of unsafe-surface increases;
- exclusive history recording and label sanitization;
- phase parsing, invalid phases, prerequisite setup, and output compatibility;
- benchmark result parsing and median/spread calculation; and
- delegation to sibling harness commands without executing long benchmarks.

Before completion, run the project gate in both configurations exactly as
documented in `AGENTS.md`, plus `cargo fmt --check`, the focused Python tests,
`just --list`, bare `just`, `just tools-check`, `just metrics-check`, and a
small-operation smoke run for every individual benchmark phase.

Long benchmarks, coverage collection, and multi-engine matrix runs are exposed
and smoke-checked but are not required in the ordinary fast verification pass.
