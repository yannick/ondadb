set shell := ["bash", "-euo", "pipefail", "-c"]

bench_dir := env_var_or_default("ONDADB_BENCH_DIR", "../bench")

[default]
[private]
_default:
    @just --color always --list

# Format Rust source.
[group('Quality')]
fmt:
    cargo fmt --check

# Run Clippy for the safe configuration.
[group('Quality')]
lint-safe:
    cargo clippy --all-targets

# Run Clippy for the unsafe fast-path configuration.
[group('Quality')]
lint-fast:
    cargo clippy --all-targets --features unsafe-fastpath

# Run tests for the safe configuration.
[group('Quality')]
test-safe:
    cargo test

# Run tests for the unsafe fast-path configuration.
[group('Quality')]
test-fast:
    cargo test --features unsafe-fastpath

# Run every deterministic quality gate.
[group('Quality')]
check: fmt lint-safe lint-fast test-safe test-fast metrics-check

# Generate quality metrics without gating.
[group('Metrics')]
metrics:
    python3 tools/metrics.py collect

# Check deterministic complexity and unsafe-surface ratchets.
[group('Metrics')]
metrics-check:
    python3 tools/metrics.py check

# Record the current metrics snapshot under label.
[group('Metrics')]
metrics-record label:
    python3 tools/metrics.py record '{{ label }}'

# Refresh the committed metric baselines deliberately.
[group('Metrics')]
metrics-baseline:
    python3 tools/metrics.py baseline

# Collect merged safe and fast-path coverage reports.
[group('Metrics')]
coverage:
    python3 tools/metrics.py coverage

# Collect and open the merged HTML coverage report.
[group('Metrics')]
coverage-open:
    python3 tools/metrics.py coverage --open

# Rank complexity hotspots by version-control churn.
[group('Metrics')]
hotspots:
    mkdir -p target/metrics
    python3 tools/metrics.py tools-check bca
    bca vcs --format html --output target/metrics/hotspots.html

# Run the standalone ondaDB benchmark.
[group('Benchmarks')]
bench-onda runs="5" ops="1000000" threads="8" key_size="16" value_size="100" pattern="random" compression="none" batch="1000" features="unsafe-fastpath":
    python3 tools/benchmark.py onda --runs '{{ runs }}' --ops '{{ ops }}' --threads '{{ threads }}' --key-size '{{ key_size }}' --value-size '{{ value_size }}' --pattern '{{ pattern }}' --compression '{{ compression }}' --batch '{{ batch }}' --features '{{ features }}'

# Run one standalone ondaDB benchmark phase.
[group('Benchmarks')]
bench-phase phase runs="5" ops="1000000" threads="8" key_size="16" value_size="100" pattern="random" compression="none" batch="1000" features="unsafe-fastpath":
    python3 tools/benchmark.py onda --runs '{{ runs }}' --ops '{{ ops }}' --threads '{{ threads }}' --key-size '{{ key_size }}' --value-size '{{ value_size }}' --pattern '{{ pattern }}' --compression '{{ compression }}' --batch '{{ batch }}' --features '{{ features }}' --phases '{{ phase }}'

# Run the standalone Put benchmark phase.
[group('Benchmarks')]
bench-put runs="5" ops="1000000" threads="8" key_size="16" value_size="100" pattern="random" compression="none" batch="1000" features="unsafe-fastpath":
    @just bench-phase put '{{ runs }}' '{{ ops }}' '{{ threads }}' '{{ key_size }}' '{{ value_size }}' '{{ pattern }}' '{{ compression }}' '{{ batch }}' '{{ features }}'

# Run the standalone Get benchmark phase.
[group('Benchmarks')]
bench-get runs="5" ops="1000000" threads="8" key_size="16" value_size="100" pattern="random" compression="none" batch="1000" features="unsafe-fastpath":
    @just bench-phase get '{{ runs }}' '{{ ops }}' '{{ threads }}' '{{ key_size }}' '{{ value_size }}' '{{ pattern }}' '{{ compression }}' '{{ batch }}' '{{ features }}'

# Run the standalone forward-scan benchmark phase.
[group('Benchmarks')]
bench-forward runs="5" ops="1000000" threads="8" key_size="16" value_size="100" pattern="random" compression="none" batch="1000" features="unsafe-fastpath":
    @just bench-phase forward '{{ runs }}' '{{ ops }}' '{{ threads }}' '{{ key_size }}' '{{ value_size }}' '{{ pattern }}' '{{ compression }}' '{{ batch }}' '{{ features }}'

# Run the standalone backward-scan benchmark phase.
[group('Benchmarks')]
bench-backward runs="5" ops="1000000" threads="8" key_size="16" value_size="100" pattern="random" compression="none" batch="1000" features="unsafe-fastpath":
    @just bench-phase backward '{{ runs }}' '{{ ops }}' '{{ threads }}' '{{ key_size }}' '{{ value_size }}' '{{ pattern }}' '{{ compression }}' '{{ batch }}' '{{ features }}'

# Run the standalone Delete benchmark phase.
[group('Benchmarks')]
bench-delete runs="5" ops="1000000" threads="8" key_size="16" value_size="100" pattern="random" compression="none" batch="1000" features="unsafe-fastpath":
    @just bench-phase delete '{{ runs }}' '{{ ops }}' '{{ threads }}' '{{ key_size }}' '{{ value_size }}' '{{ pattern }}' '{{ compression }}' '{{ batch }}' '{{ features }}'

# Run ignored vlog repeat-read benchmarks in both configurations.
[group('Benchmarks')]
bench-vlog:
    cargo test --release --test vlog_read_bench -- --ignored --nocapture --test-threads=1
    cargo test --release --features unsafe-fastpath --test vlog_read_bench -- --ignored --nocapture --test-threads=1

# Run the ignored release column-family creation microbenchmark.
[group('Benchmarks')]
bench-cf:
    cargo test --release create_column_families_bench -- --ignored --nocapture --test-threads=1

[private]
_require-bench-harness script:
    if [[ ! -d "{{ bench_dir }}" ]]; then echo "benchmark harness is missing: {{ bench_dir }}" >&2; exit 1; fi
    if [[ ! -x "{{ bench_dir }}/{{ script }}" ]]; then echo "benchmark harness script is missing or not executable: {{ bench_dir }}/{{ script }}" >&2; exit 1; fi

# Run the sibling harness's one-shot comparison.
[group('Benchmarks')]
bench-suite ops="1000000" key_size="16" value_size="100" threads="8" pattern="random" compression="none": (_require-bench-harness "run_bench.sh")
    cd "{{ bench_dir }}" && OPS='{{ ops }}' KEY_SIZE='{{ key_size }}' VALUE_SIZE='{{ value_size }}' THREADS='{{ threads }}' PATTERN='{{ pattern }}' COMPRESSION='{{ compression }}' ./run_bench.sh

# Run the sibling harness's repeated benchmark report.
[group('Benchmarks')]
bench-graphs runs="3" ops="1000000" key_size="16" value_size="100" threads="8" pattern="random" compression="none": (_require-bench-harness "bench_graphs.sh")
    cd "{{ bench_dir }}" && RUNS='{{ runs }}' OPS='{{ ops }}' KEY_SIZE='{{ key_size }}' VALUE_SIZE='{{ value_size }}' THREADS='{{ threads }}' PATTERN='{{ pattern }}' COMPRESSION='{{ compression }}' ./bench_graphs.sh

# Run the sibling harness's multi-size benchmark matrix.
[group('Benchmarks')]
bench-matrix runs="2" threads="8" pattern="random" compression="none": (_require-bench-harness "bench_matrix.sh")
    cd "{{ bench_dir }}" && RUNS='{{ runs }}' THREADS='{{ threads }}' PATTERN='{{ pattern }}' COMPRESSION='{{ compression }}' ./bench_matrix.sh

# Report required tool versions and installation commands.
[group('Setup')]
tools-check:
    python3 tools/metrics.py tools-check

# Install the pinned Cargo quality tools explicitly.
[group('Setup')]
tools-install:
    cargo install --locked big-code-analysis-cli@2.1.0
    cargo install --locked cargo-llvm-cov@0.8.7
    cargo install --locked cargo-geiger@0.13.0
    cargo install --locked cargo-bloat@0.12.1
