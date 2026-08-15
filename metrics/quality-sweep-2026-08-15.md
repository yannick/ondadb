# Aggressive quality sweep — 2026-08-15

The sweep compared the clean pre-refactor revision `9dcb1ae51c9f` with the
clean post-refactor revision `6836364369aa`. The machine-readable snapshots
are committed beside this report in `metrics/history/`.

## Outcome

| Metric | Before | After | Change |
|---|---:|---:|---:|
| BCA baseline exceptions | 26 | 3 | -23 (-88.5%) |
| Cyclomatic complexity, max / median / p90 | 46 / 1 / 5 | 16 / 1 / 5 | max -65.2% |
| Cognitive complexity, max / median / p90 | 107 / 0 / 5 | 20 / 0 / 5 | max -81.3% |
| Function span, max / median / p90 | 321 / 5 / 23 | 182 / 5 / 23 | max -43.3% |
| Functions over 200 lines | 3 | 0 | -3 |
| Production source lines | 16,748 | 17,269 | +521 (+3.1%) |
| Test source lines | 8,219 | 8,219 | unchanged |

The three remaining BCA exceptions are the deliberately flat, exhaustive
`OndaError::{code, fmt, kind}` mappings, each at cyclomatic complexity 16.
They are the only entries in `.bca-baseline.toml`.

## Safety, dependencies, coverage, and size

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Unsafe expressions / impls | 63 / 3 | 63 / 3 | unchanged |
| Unsafe functions / traits / methods | 0 / 0 / 0 | 0 / 0 / 0 | unchanged |
| Direct / transitive dependencies | 18 / 62 | 18 / 62 | unchanged |
| Duplicate dependency versions | 2 | 2 | unchanged |
| Line coverage | 90.25% | 90.93% | +0.67 pp |
| Function coverage | 90.38% | 91.26% | +0.88 pp |
| Region coverage | 89.24% | 89.93% | +0.68 pp |
| Safe binary file / text bytes | 1,813,544 / 1,338,204 | 1,796,216 / 1,331,720 | -0.96% / -0.48% |
| Fast-path binary file / text bytes | 1,794,248 / 1,332,868 | 1,793,368 / 1,325,720 | -0.05% / -0.54% |

Coverage is the merged `cargo-llvm-cov` result for the safe and
`unsafe-fastpath` configurations. All three coverage measures improved, so
the 0.5 percentage-point regression limit passed. No dependency or unsafe
surface was added.

## Performance

The final benchmark used five alternating before/after pairs, one million
operations, eight threads, 16-byte keys, 100-byte values, batch size 1,000,
no compression, and the `unsafe-fastpath` feature. Ratios are after/before
throughput from each same-run pair.

| Phase | Pair ratios | Median | After losses |
|---|---|---:|---:|
| Put | 1.060, 0.298, 1.180, 1.124, 1.125 | 1.124 (+12.4%) | 1/5 |
| Get | 0.963, 0.995, 1.112, 0.915, 1.117 | 0.995 (-0.5%) | 3/5 |
| Forward scan | 1.039, 0.785, 1.186, 0.994, 1.178 | 1.039 (+3.9%) | 2/5 |
| Backward scan | 0.943, 1.050, 1.105, 0.947, 0.980 | 0.980 (-2.0%) | 3/5 |
| Delete | 1.029, 0.875, 1.081, 0.806, 0.969 | 0.969 (-3.1%) | 3/5 |

No phase met the rejection rule of losing at least four pairs while its
median throughput was more than 10% lower. The large second-pair Put outlier
is retained rather than discarded; same-run ratios and the acceptance rule
absorb this machine's documented thermal noise. Earlier wave-specific paired
runs also passed. No refactoring experiment required a performance revert.

## Material improvements

- Persisted configuration and manifest parsing now use explicit checked
  phases. An exhaustive truncation regression found and fixed a panic on a
  checksummed but truncated manifest.
- Column-family point reads and iterator construction now expose their source
  precedence and selection decisions without heap materialization on the hot
  path.
- Merge iteration retains separate winning-key and winning-value pins, table
  cache eviction is a named per-shard CLOCK operation, and mmap/file vlog
  validation share an explicit checksum policy.
- WAL interval syncing now records each physical sync consistently. Unified
  flush keeps a shared WAL until every column-family slice is durably flushed
  and the manifest is persisted.
- Maintenance, transaction, and part-movement code now separate decision,
  execution, publication, and cleanup phases while preserving manifest and
  SST deletion ordering.
- Compaction separates version retention, output construction, installation,
  manifest publication, and obsolete-input removal. Comparator-equal keys use
  the configured comparator for MVCC grouping, and partial output files are
  cleaned on pre-publication errors.

## Verification evidence

- The final safe and `unsafe-fastpath` test gates each emitted 23
  `test result: ok` summaries with no failed summary.
- The final merged coverage rerun emitted 44 successful test-binary summaries
  with no failed summary.
- `cargo clippy --all-targets -- -D warnings` and the corresponding
  `unsafe-fastpath` command completed without warnings.
- `cargo fmt --check`, `git diff --check`, pinned-tool checks, the explicit
  no-suppression BCA gate, and `just metrics-check` passed.
- The documented intermittent `unsafe-fastpath` `read_your_writes` test failed
  once during an initial coverage attempt. Five isolated exact reruns passed,
  followed by the complete successful coverage rerun; the test was neither
  changed nor ignored.
- Every sweep wave ran focused tests, both full feature configurations, both
  Clippy configurations, and the metric ratchets before commit.
