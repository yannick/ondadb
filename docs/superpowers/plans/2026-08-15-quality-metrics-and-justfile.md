# Quality Metrics and Justfile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add repository-native quality metrics, hybrid ratchet gates, individually runnable benchmark phases and suites, and a grouped colorful `just` task interface.

**Architecture:** Pinned upstream analyzers remain the sources of raw measurements. Two standard-library Python CLIs validate and normalize metric and benchmark output into versioned JSON/CSV, while committed BCA and unsafe baselines implement deterministic ratchets. `just` is a thin, discoverable orchestration layer over Cargo, the Python CLIs, and the existing sibling benchmark harness.

**Tech Stack:** Rust 2021, Python 3 standard library, just 1.43.1+, big-code-analysis-cli 2.1.0, cargo-llvm-cov 0.8.7, cargo-geiger 0.13.0, cargo-bloat 0.12.1.

## Global Constraints

- Both default and `unsafe-fastpath` configurations must remain green.
- Never hide or truncate Cargo test output; propagate every command's exit status.
- Complexity thresholds are cyclomatic 15, cognitive 20, and logical function length 200.
- Existing complexity and unsafe debt is baselined; new or worsened debt fails.
- Coverage, source size, dependencies, binary size, hotspots, and performance are observational in the first release.
- Benchmark results never gate and must preserve the documented ±15–20% thermal-noise warning.
- All generated reports live below `target/`; only explicit snapshots and baselines are committed.
- External tool installation is explicit and version-pinned.
- Do not add third-party Python dependencies.

---

## File Map

- Modify `src/bin/onda_bench.rs`: validated phase selection and untimed prerequisite population.
- Create `tools/__init__.py`: make repository tooling importable by standard `unittest`.
- Create `tools/benchmark.py`: repeated standalone benchmark runner and JSON/CSV summarizer.
- Create `tools/metrics.py`: tool checks, normalized metric collection, unsafe ratchet, coverage, history, and baselines.
- Create `tools/tests/test_benchmark.py`: benchmark parser/statistics/CLI unit tests.
- Create `tools/tests/test_metrics.py`: schema, unsafe comparison, history, and tool diagnostics tests.
- Create `bca.toml`: BCA input, threshold, and baseline configuration.
- Create `.bca-baseline.toml`: current per-function cyclomatic/cognitive debt.
- Create `metrics/baseline.json`: current unsafe-surface and long-function debt.
- Create `metrics/history/.gitkeep`: preserve the intentionally empty history directory.
- Create `metrics/README.md`: metric meanings, commands, interpretation, and ratchet policy.
- Create `justfile`: grouped public recipes and hidden helpers.
- Modify `README.md`: point contributors to `just` and the quality-metrics guide.

---

### Task 1: Select Individual `onda_bench` Phases Safely

**Files:**
- Modify: `src/bin/onda_bench.rs`

**Interfaces:**
- Consumes: existing CLI flags and output labels used by `../bench/bench_graphs.sh`.
- Produces: `-phases put,get,forward,backward,delete`; omitted means all phases.
- Produces: `PhaseSet::parse_list(&str) -> Result<PhaseSet, String>` and `parse_args_from<I, S>(I) -> Result<Args, String>` for focused tests.

- [ ] **Step 1: Add failing unit tests for phase parsing and argument validation**

Add a `#[cfg(test)] mod tests` in `src/bin/onda_bench.rs` covering:

```rust
#[test]
fn phases_default_to_all() {
    let args = parse_args_from(["onda_bench"]).unwrap();
    assert!(Phase::ALL.into_iter().all(|phase| args.phases.contains(phase)));
}

#[test]
fn phases_accept_a_comma_separated_subset() {
    let args = parse_args_from(["onda_bench", "-phases", "get,forward"]).unwrap();
    assert!(args.phases.contains(Phase::Get));
    assert!(args.phases.contains(Phase::Forward));
    assert!(!args.phases.contains(Phase::Put));
}

#[test]
fn phases_reject_unknown_and_empty_values() {
    assert!(parse_args_from(["onda_bench", "-phases", "bogus"]).is_err());
    assert!(parse_args_from(["onda_bench", "-phases", ""]).is_err());
}

#[test]
fn numeric_arguments_reject_invalid_values() {
    assert!(parse_args_from(["onda_bench", "-ops", "zero"]).is_err());
    assert!(parse_args_from(["onda_bench", "-threads", "0"]).is_err());
}
```

- [ ] **Step 2: Run the focused tests and confirm they fail**

Run: `cargo test --bin onda_bench phases_ -- --nocapture`

Expected: compilation failure because `parse_args_from`, `Phase`, and the phase set do not exist.

- [ ] **Step 3: Implement typed phases and fallible argument parsing**

Add:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase { Put, Get, Forward, Backward, Delete }

impl Phase {
    const ALL: [Self; 5] = [Self::Put, Self::Get, Self::Forward, Self::Backward, Self::Delete];
    fn parse(name: &str) -> Option<Self> {
        match name {
            "put" => Some(Self::Put),
            "get" => Some(Self::Get),
            "forward" => Some(Self::Forward),
            "backward" => Some(Self::Backward),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PhaseSet(u8);

impl PhaseSet {
    fn all() -> Self;
    fn parse_list(value: &str) -> Result<Self, String>;
    fn contains(self, phase: Phase) -> bool;
    fn needs_population(self) -> bool;
    fn needs_reopen(self) -> bool;
}
```

Refactor `parse_args` into generic `parse_args_from<I, S>(argv: I) -> Result<Args, String>` and make `main` print `onda_bench: <message>` and exit 2 on validation errors. Preserve `-engine` compatibility and existing defaults. Validate `ops`, sizes, threads, and batch as positive integers before filesystem work.

- [ ] **Step 4: Run parser tests and confirm they pass**

Run: `cargo test --bin onda_bench phases_ numeric_arguments -- --nocapture`

Expected: all new parser tests pass.

- [ ] **Step 5: Add failing state-transition tests for phase prerequisites**

Test the phase-set decisions that drive untimed setup:

```rust
#[test]
fn post_put_phases_require_population_and_reopen() {
    for name in ["get", "forward", "backward", "delete"] {
        let phases = PhaseSet::parse_list(name).unwrap();
        assert!(phases.needs_population());
        assert!(phases.needs_reopen());
    }
}

#[test]
fn put_only_does_not_require_reopen() {
    let phases = PhaseSet::parse_list("put").unwrap();
    assert!(phases.needs_population());
    assert!(!phases.needs_reopen());
}
```

- [ ] **Step 6: Refactor population and timed phase execution**

Extract `populate(&DB, &ColumnFamily, &[Vec<u8>], &[u8], &Args) -> Duration`. Always populate when any requested phase needs data; report its duration only when `Put` is selected. Close/reopen before post-put phases so phase-only runs have the same SST-resident shape as the full suite. Run selected phases in canonical order, emit only selected labels, keep prerequisites outside timers, and delete the database only after argument validation.

- [ ] **Step 7: Verify every phase in both feature configurations**

Run:

```sh
cargo test --bin onda_bench
cargo test --features unsafe-fastpath --bin onda_bench
cargo run --release --bin onda_bench -- -ops 100 -threads 2 -phases put
cargo run --release --bin onda_bench -- -ops 100 -threads 2 -phases get
cargo run --release --bin onda_bench -- -ops 100 -threads 2 -phases forward
cargo run --release --bin onda_bench -- -ops 100 -threads 2 -phases backward
cargo run --release --bin onda_bench -- -ops 100 -threads 2 -phases delete
```

Expected: each smoke run emits exactly its selected phase and exits successfully.

- [ ] **Step 8: Commit the phase-selection deliverable**

```sh
git add src/bin/onda_bench.rs
git commit -m "feat: run benchmark phases independently"
```

---

### Task 2: Add a Repeated Benchmark Runner

**Files:**
- Create: `tools/__init__.py`
- Create: `tools/benchmark.py`
- Create: `tools/tests/__init__.py`
- Create: `tools/tests/test_benchmark.py`

**Interfaces:**
- Consumes: `onda_bench` output lines matching `<phase> <ops> ops <ms> ms <ops/sec> ops/sec`.
- Produces: `python3 tools/benchmark.py onda [options]`.
- Produces: schema `ondadb.benchmark.v1` JSON and flat CSV under `target/benchmarks/`.

- [ ] **Step 1: Write failing parser and statistics tests**

Create tests for exact phase-name normalization, missing and duplicate output,
median, min, max, relative spread, and subprocess behavior. The subprocess test
uses a temporary executable script that emits one Get line; a second test uses
a script that exits 2 before touching its supplied DB path, proving errors are
propagated without filesystem mutation:

```python
class ParseOutputTests(unittest.TestCase):
    def test_parses_existing_harness_labels(self):
        parsed = benchmark.parse_output(
            "Put                          100 ops    2.00 ms    50000 ops/sec\n"
            "Get (cold)                   100 ops    1.00 ms    100000 ops/sec\n"
        )
        self.assertEqual(parsed["put"].ops_per_second, 50000)
        self.assertEqual(parsed["get"].milliseconds, 1.0)

    def test_rejects_duplicate_phase_lines(self):
        with self.assertRaisesRegex(ValueError, "duplicate phase"):
            benchmark.parse_output(PUT_LINE + PUT_LINE)

class SummaryTests(unittest.TestCase):
    def test_reports_median_and_relative_spread(self):
        summary = benchmark.summarize([100.0, 120.0, 80.0])
        self.assertEqual(summary["median"], 100.0)
        self.assertEqual(summary["relative_spread_percent"], 40.0)
```

- [ ] **Step 2: Run tests and verify they fail**

Run: `python3 -m unittest tools.tests.test_benchmark -v`

Expected: import or attribute failures because the implementation does not exist.

- [ ] **Step 3: Implement pure parsing and summarization functions**

Implement immutable `PhaseResult`, a compiled anchored regular expression,
`parse_output(text) -> dict[str, PhaseResult]`, and
`summarize(values) -> dict[str, float]`. Reject non-positive operation counts,
missing throughput, duplicates, and non-finite values.

- [ ] **Step 4: Run pure-function tests and confirm they pass**

Run: `python3 -m unittest tools.tests.test_benchmark -v`

Expected: parser and summary tests pass.

- [ ] **Step 5: Write failing command-construction and output tests**

Test that `build_command()` includes all workload flags, maps `features="unsafe-fastpath"` to the Cargo build, uses an explicit DB path, and passes `-phases`. Test `write_reports()` against a temporary directory and assert JSON schema/version, raw runs, workload, Git/host fields, and CSV columns.

- [ ] **Step 6: Implement the `onda` CLI**

Use `argparse` with validated positive integers and choices for phases, pattern, and compression. Defaults: 5 runs, 1,000,000 ops, 8 threads, 16-byte keys, 100-byte values, batch 1,000, random keys, no compression, and `unsafe-fastpath`. Build once, run the binary repeatedly, stream captured stdout/stderr after each run, reject missing requested phases, and atomically write:

```text
target/benchmarks/onda-latest.json
target/benchmarks/onda-latest.csv
```

Include schema, UTC timestamp, Git revision/dirty flag, `rustc -Vv`, OS/architecture/CPU count, full workload, selected features, raw measurements, and per-phase summaries.

- [ ] **Step 7: Verify the runner with a small smoke workload**

Run: `python3 tools/benchmark.py onda --runs 2 --ops 100 --threads 2 --phases get`

Expected: two successful Get runs and both report files with one phase summary.

- [ ] **Step 8: Commit the benchmark runner**

```sh
git add tools/__init__.py tools/benchmark.py tools/tests/__init__.py tools/tests/test_benchmark.py
git commit -m "feat: record repeatable benchmark summaries"
```

---

### Task 3: Add Metric Collection and Hybrid Ratchets

**Files:**
- Create: `tools/metrics.py`
- Create: `tools/tests/test_metrics.py`
- Create: `bca.toml`
- Create: `.bca-baseline.toml`
- Create: `metrics/baseline.json`
- Create: `metrics/history/.gitkeep`

**Interfaces:**
- Consumes: BCA JSON/check output, cargo-geiger JSON, cargo metadata JSON, `cargo tree --duplicates`, cargo-bloat JSON, and cargo-llvm-cov JSON.
- Produces: `target/metrics/current.json` with schema `ondadb.metrics.v1`.
- Produces: `check`, `collect`, `record`, `baseline`, `coverage`, `tools-check`, and `tools-install-command` subcommands.

- [ ] **Step 1: Write failing schema and unsafe-ratchet tests**

Create fixture dictionaries in the test module and cover project-package selection, category normalization, missing schema fields, no-regression success, added unsafe failure, and decreased-count success:

```python
class UnsafeRatchetTests(unittest.TestCase):
    def test_added_unsafe_expression_fails(self):
        baseline = {"unsafe": {"functions": 1, "expressions": 2, "impls": 0,
                               "traits": 0, "methods": 0}}
        current = {"unsafe": {**baseline["unsafe"], "expressions": 3}}
        self.assertEqual(
            metrics.unsafe_regressions(current, baseline),
            ["expressions: 2 -> 3"],
        )

    def test_debt_reduction_passes(self):
        self.assertEqual(metrics.unsafe_regressions(CURRENT_LOWER, BASELINE), [])
```

- [ ] **Step 2: Run tests and verify they fail**

Run: `python3 -m unittest tools.tests.test_metrics -v`

Expected: import or attribute failures.

- [ ] **Step 3: Implement command execution, schema validation, and unsafe comparison**

Implement `run_json(command, output_path=None)`, `atomic_write_json`,
`load_json`, `normalize_geiger(document, package="ondadb")`, and
`unsafe_regressions(current, baseline)`. Never map malformed/missing values to
zero. Keep tool errors distinct from ratchet errors with exit codes 1 and 2.

- [ ] **Step 4: Write and pass tool-version diagnostic tests**

Represent required tools in one constant:

```python
TOOLS = {
    "bca": ("2.1.0", "cargo install --locked big-code-analysis-cli@2.1.0"),
    "cargo-llvm-cov": ("0.8.7", "cargo install --locked cargo-llvm-cov@0.8.7"),
    "cargo-geiger": ("0.13.0", "cargo install --locked cargo-geiger@0.13.0"),
    "cargo-bloat": ("0.12.1", "cargo install --locked cargo-bloat@0.12.1"),
}
```

Mock `shutil.which` and subprocess version output to verify missing and wrong
versions produce exact actionable messages and `tools-check` never installs.

- [ ] **Step 5: Configure BCA and bootstrap its baseline**

Create:

```toml
paths = ["src"]
exclude_tests = true
cyclomatic_count_try = false

[check]
baseline = ".bca-baseline.toml"
exclude = ["./src/bin/**"]

[thresholds]
cyclomatic = 15
cognitive = 20
```

Run `bca check --no-fail` to inspect current distributions, then
`bca check --write-baseline`. Confirm `bca check` exits 0 with the generated
baseline. Use BCA metric output spans in `metrics.py` to calculate logical
function length; compare functions over 200 lines through the normalized
baseline because BCA's built-in `loc.*` gate is file-scoped.

- [ ] **Step 6: Write failing normalization tests for trends**

Cover percentile interpolation for p50/p90, BCA function extraction, Cargo
metadata direct/transitive counts, duplicate-tree counting, cargo-bloat totals,
and LLVM coverage totals. Each malformed fixture must raise a descriptive
`MetricsError`.

- [ ] **Step 7: Implement `collect` and normalized snapshot output**

Collect BCA metrics for production and test paths, geiger with
`--features unsafe-fastpath --output-format Json --quiet`, Cargo metadata
format 1, Cargo duplicate tree, and safe/fast cargo-bloat reports. Normalize
production/test line totals along with the other values into:

```json
{
  "schema": "ondadb.metrics.v1",
  "collected_at": "2026-08-15T12:00:00Z",
  "git": {"revision": "0123456789abcdef0123456789abcdef01234567", "dirty": true},
  "host": {"os": "Darwin", "architecture": "arm64", "cpu_count": 24},
  "tools": {"bca": "2.1.0"},
  "complexity": {"cyclomatic": {"max": 0, "median": 0, "p90": 0},
                   "cognitive": {"max": 0, "median": 0, "p90": 0},
                   "function_lloc": {"max": 0, "median": 0, "p90": 0},
                   "baseline_exceptions": 0},
  "unsafe": {"functions": 0, "expressions": 0, "impls": 0,
              "traits": 0, "methods": 0},
  "long_functions": {},
  "source": {"production_lines": 0, "test_lines": 0},
  "dependencies": {"direct": 0, "transitive": 0, "duplicate_versions": 0},
  "binary": {"safe": {"file_bytes": 0, "text_bytes": 0},
              "unsafe_fastpath": {"file_bytes": 0, "text_bytes": 0}},
  "coverage": null
}
```

Raw producer documents go to `target/metrics/raw`; `current.json` is replaced
atomically. Print a compact table from the normalized data.

- [ ] **Step 8: Implement check, baseline, and append-only record**

`check` runs `bca check`, collects current unsafe/function-length values, and
compares only those deterministic values. `baseline` writes BCA's baseline and
`metrics/baseline.json` atomically. `record LABEL` requires an existing current
snapshot, sanitizes the label to lowercase `[a-z0-9-]`, and exclusively creates
`metrics/history/YYYYMMDDTHHMMSSZ-<short-sha>-<label>.json`.

- [ ] **Step 9: Implement merged coverage collection**

Run exactly:

```sh
cargo llvm-cov clean --workspace
cargo llvm-cov --no-report
cargo llvm-cov --no-report --features unsafe-fastpath
cargo llvm-cov report --json --output-path target/metrics/raw/coverage.json
cargo llvm-cov report --html --output-dir target/metrics/coverage
```

Parse line, function, and region count/covered/percent totals into
`target/metrics/current.json`. `coverage --open` opens the generated index with
`open` on macOS, `xdg-open` on Linux, and otherwise prints the path.

- [ ] **Step 10: Install pinned tools explicitly and generate real baselines**

Run the printed installation commands, verify exact versions, generate the BCA
and unsafe baselines, inspect them for project-only paths/packages, and run
`python3 tools/metrics.py check` twice to prove determinism.

- [ ] **Step 11: Run all metric unit tests and smoke collection**

Run:

```sh
python3 -m unittest tools.tests.test_metrics -v
python3 tools/metrics.py tools-check
python3 tools/metrics.py collect
python3 tools/metrics.py check
```

Expected: tests pass, tools match pins, collection emits a valid snapshot, and
both ratchets pass.

- [ ] **Step 12: Commit metric collection and baselines**

```sh
git add tools/metrics.py tools/tests/test_metrics.py bca.toml .bca-baseline.toml metrics/baseline.json metrics/history/.gitkeep
git commit -m "feat: track repository quality metrics"
```

---

### Task 4: Introduce the Grouped `just` Task Interface

**Files:**
- Create: `justfile`
- Create: `tools/tests/test_justfile.py`

**Interfaces:**
- Consumes: Cargo commands, `tools/metrics.py`, `tools/benchmark.py`, and sibling `../bench` scripts.
- Produces: grouped recipes in Quality, Metrics, Benchmarks, and Setup.

- [ ] **Step 1: Write failing help-surface tests**

Create subprocess tests that run `just --color always` and `just --list` from
the repo root. Assert ANSI escape bytes, the four group headings, recipe
descriptions, visible parameters, and absence of private `_` helpers. Also
assert `just --summary` contains every public recipe named in the design.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `python3 -m unittest tools.tests.test_justfile -v`

Expected: failure because no `justfile` exists.

- [ ] **Step 3: Implement the default help and quality/setup recipes**

Use `set shell := ["bash", "-euo", "pipefail", "-c"]`, `[default]`, recipe
doc comments, and `[group('Quality')]`/`[group('Setup')]`. Bare `just` invokes
`just --color always --list`. Implement exact Cargo commands from `AGENTS.md`;
`check` depends on format, both Clippy configurations, both test
configurations, and `metrics-check`. `tools-install` runs the four pinned Cargo
install commands and `tools-check` delegates to `metrics.py`.

- [ ] **Step 4: Implement metrics recipes**

Under `[group('Metrics')]`, delegate `metrics`, `metrics-check`,
`metrics-record label`, `metrics-baseline`, `coverage`, and `coverage-open` to
the Python CLI. Implement `hotspots` with BCA's VCS ranking/report command and
write any generated report below `target/metrics/`.

- [ ] **Step 5: Implement standalone and subsystem benchmark recipes**

Under `[group('Benchmarks')]`, make `bench-onda` and `bench-phase phase` accept
overridable parameters with the approved defaults and delegate to
`tools/benchmark.py`. Convenience phase recipes delegate to `bench-phase`.
`bench-vlog` runs both safe and fast ignored tests with `--test-threads=1`;
`bench-cf` runs the ignored release microbenchmark. Preserve full output.

- [ ] **Step 6: Implement sibling harness recipes with validation**

Private `_require-bench-harness` verifies `../bench` and the target script.
`bench-suite`, `bench-graphs`, and `bench-matrix` change into `../bench` and
pass supported values through environment variables. Do not use `|| true` or
suppress harness failures in the `justfile`.

- [ ] **Step 7: Run help and recipe wiring tests**

Run:

```sh
python3 -m unittest tools.tests.test_justfile -v
just --fmt --check
just --list
just --color always
just tools-check
just bench-phase get 1 100 2
```

Expected: all help tests pass and the small benchmark recipe records one Get
run.

- [ ] **Step 8: Commit the task interface**

```sh
git add justfile tools/tests/test_justfile.py
git commit -m "build: add discoverable just tasks"
```

---

### Task 5: Document Operations and Run the Full Gate

**Files:**
- Create: `metrics/README.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: the completed recipes and normalized schemas.
- Produces: maintainer guidance for interpreting, recording, and ratcheting metrics.

- [ ] **Step 1: Write the metrics operations guide**

Document every metric's definition, gate/report status, tool/version, generated
path, `just` command, baseline ownership, history naming, and reduction policy.
Include a table that says lower/higher is better and cautions against reducing
complexity through meaningless function splitting. Explain that
`metrics-baseline` is reviewed debt acceptance, while `metrics-record LABEL` is
observational history.

- [ ] **Step 2: Document benchmark interpretation**

List full-suite and phase recipes, all override parameters, prerequisite setup
semantics, medians/spread, A/B usage, and sibling harness delegation. Repeat
that results are thermally noisy by ±15–20%, that five runs are the standalone
default, and that cross-engine comparisons use same-run ratios.

- [ ] **Step 3: Update the README contributor entry point**

In `README.md`'s Testing & quality and Benchmarks sections, add bare `just` as
the discoverable command index and link `metrics/README.md`. Keep the explicit
Cargo commands as the authoritative CI-equivalent reference.

- [ ] **Step 4: Run focused tests and static checks**

Run:

```sh
python3 -m unittest discover -s tools/tests -v
python3 -m py_compile tools/metrics.py tools/benchmark.py
just --fmt --check
cargo fmt --check
git diff --check
```

Expected: every command succeeds.

- [ ] **Step 5: Run the complete ondaDB gate with untruncated output**

Run separately and inspect each exit code:

```sh
cargo test
cargo test --features unsafe-fastpath
cargo clippy --all-targets
cargo clippy --all-targets --features unsafe-fastpath
```

Expected: each test invocation contains `test result: ok` for every emitted test
binary and exits 0; both Clippy commands exit 0 with no warnings.

- [ ] **Step 6: Run metric and benchmark smoke verification**

Run:

```sh
just metrics-check
just metrics
just bench-put 1 100 2
just bench-get 1 100 2
just bench-forward 1 100 2
just bench-backward 1 100 2
just bench-delete 1 100 2
```

Validate `target/metrics/current.json`, `target/benchmarks/onda-latest.json`,
and `target/benchmarks/onda-latest.csv` against their schema fields. Do not run
long coverage or multi-engine matrix jobs as part of the fast gate.

- [ ] **Step 7: Inspect repository changes and commit documentation**

Run `git status --short`, `git diff --stat HEAD~4`, and `git diff --check`.
Confirm no generated `target/` data or accidental history snapshot is staged.

```sh
git add README.md metrics/README.md
git commit -m "docs: explain quality metrics and benchmark tasks"
```

- [ ] **Step 8: Perform completion verification**

Re-run `git status --short`, the focused unit suite, `just metrics-check`, and
the four required Cargo commands from fresh command invocations. Record exact
passing output and any intentionally unrun long jobs in the final handoff.
