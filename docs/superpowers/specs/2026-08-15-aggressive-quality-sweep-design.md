# Aggressive Code-Quality Sweep Design

## Goal

Establish a trustworthy current metrics baseline, then eliminate every
actionable production-complexity exception and every function over 200 source
lines without changing ondaDB's public API, on-disk formats, concurrency
contracts, durability semantics, or intended performance.

The sweep is successful when production cognitive complexity is at most 20,
production cyclomatic complexity is at most 15 except for a tiny reviewed set
of mechanically exhaustive mappings, and no production function spans more
than 200 source lines. Unsafe code may shrink only when the change is
benchmark-neutral; it may never increase.

## Starting Evidence

The pre-sweep generated report records:

- 26 BCA baseline exceptions;
- three functions over 200 source lines:
  `compact_inputs` at 321, `decode_into` at 225, and `DB::open_impl` at 202;
- maximum cyclomatic complexity 46 and maximum cognitive complexity 107;
- 63 unsafe expressions and three unsafe impls;
- 16,748 production source lines and 8,219 integration-test source lines;
- 18 direct and 62 transitive dependencies, with two extra duplicate versions.

The highest combined complexity/churn targets are compaction, database open and
flush, configuration, column-family reads, SST reads, transactions, iterators,
WAL, and part movement. The committed deterministic baseline has stale Git
provenance, so the first implementation deliverable is a clean baseline-only
commit before any production refactoring.

## Baseline Deliverable

The isolated sweep branch begins by:

1. refreshing `.bca-baseline.toml` and `metrics/baseline.json` at the clean
   branch revision;
2. running merged safe/`unsafe-fastpath` coverage;
3. recording a committed `before-quality-sweep` metrics history snapshot;
4. capturing five-run standalone benchmark evidence for every phase;
5. retaining the raw before-sweep reports in the plan's ignored execution
   workspace for paired comparisons.

The deterministic baselines are reviewed debt acceptance. After this anchor
commit they may only shrink. They are never regenerated to absorb a refactoring
regression. Observational snapshots record coverage, source size, dependency
counts, binary size, and producer/Git/host provenance without creating new
gates.

## Risk-Layered Sweep

Refactoring proceeds in four independently reviewable waves, ordered from
lowest semantic risk to highest:

1. **Formats and configuration:** configuration encode/decode, manifest
   encode/decode, orphan sweep, and the mechanically exhaustive error mappings.
2. **Read paths:** iterators, table-cache eviction, column-family point and
   iterator reads, and SST lookup/value-log reads.
3. **Write and maintenance paths:** interval WAL sync, flush worker, part mover,
   and transaction commit.
4. **Compaction:** `compact_inputs`, isolated last because it combines the
   largest complexity debt with the most data-loss-sensitive ordering.

Each wave must remove its targeted baseline entries before the next starts.
Simple offenders receive focused control-flow cleanup. The transaction and
compaction functions may receive cohesive internal state, phase, or policy
types when those types make invariants explicit. The sweep does not impose a
blanket module split.

## Refactoring Policy

Every target begins with characterization tests for its existing decisions,
error paths, and ordering. A valid extraction creates a named operation with a
coherent contract that can be understood and tested independently. Moving a
branch into a meaningless one-line helper merely to lower BCA output is not an
acceptable improvement.

A refactor is rejected if it:

- obscures the orchestration flow;
- duplicates policy or cleanup logic;
- introduces allocation, cloning, locking, or materialization on a hot path;
- weakens failure-path or invariant coverage;
- changes a public interface, persisted representation, or intended behavior;
- adds a production dependency.

The only expected remaining BCA exceptions are the three exhaustive
`OndaError::{code,fmt,kind}` mappings if their cyclomatic value remains 16 and
cognitive value remains 1. They are retained because splitting flat exhaustive
matches would reduce the metric while worsening the code. Any other remaining
exception requires a new design decision rather than silent acceptance.

## Critical Safety Contracts

### Formats and configuration

- Preserve byte-for-byte legacy encodings and optional-tail compatibility.
- Preserve corruption and truncation detection.
- Keep every persisted integer and checksum contract unchanged.

### Reads and iteration

- Preserve comparator gating and full-compare fallback behavior.
- Preserve gap-free MVCC visibility.
- Preserve separate key/value block pins and refresh them only on block
  transitions.
- Preserve cold/warm cache behavior and avoid per-entry shared-mapping clones.

### WAL, flush, parts, and transactions

- Preserve one-frame committed-batch atomicity.
- Preserve active-writer draining and sealed-memtable immutability.
- Preserve gap-free sequence publication.
- Preserve SST sync and parent-directory fsync before manifest publication,
  then WAL/input deletion only after successful manifest persistence.
- Keep manifest writes behind `manifest_mu` and obsolete SST deletion behind
  `remove_sst_file`.
- Preserve part-move manifest commit points and all pre/post-commit cleanup
  distinctions.

### Compaction

- Preserve manifest-before-input-deletion ordering and checkpoint pinning.
- Preserve comparator order, internal-key version order, snapshot visibility,
  tombstone retention/elision, TTL behavior, and partition/tier boundaries.
- Delete incomplete outputs on every pre-commit failure and never delete
  inputs before a durable manifest commit.

### Unsafe fast paths

- Preserve both default and `unsafe-fastpath` feature configurations.
- Reduce unsafe boundaries only when the contract becomes smaller and more
  auditable.
- Retain a reduction only when paired benchmarks show no material regression.

## Per-Change and Per-Wave Loop

Each coherent refactor follows this sequence:

1. add focused characterization or regression tests and observe the expected
   failure when the new seam does not yet exist;
2. make one behavior-preserving refactor;
3. run focused tests in both feature configurations;
4. run `just metrics-check` and confirm targeted debt shrinks without any new
   offender;
5. run formatting and both Clippy configurations;
6. inspect the diff for invariant and hot-path changes;
7. commit the coherent unit.

At the end of every wave, run the full safe and `unsafe-fastpath` test suites,
recollect metrics, and compare against the initial snapshot. A wave touching a
hot path also runs the paired performance gate before completion.

## Performance Gate

Performance uses five paired before/after standalone runs with identical
workloads and a cooled machine. A refactor is reverted or redesigned when a
phase loses at least four of five pairs and its median throughput regresses by
more than 10 percent. Smaller or directionally inconsistent differences are
treated as thermally ambiguous and rerun; absolute results from separate
thermal sessions are never compared.

The gate covers Put, cold Get, Forward scan, Backward scan, and Delete. The
existing phase runner keeps prerequisite population and reopen work outside the
selected phase timer. Cross-engine matrix runs remain observational and are not
required for each internal refactor.

## Final Acceptance

Shipping requires all of the following:

- zero production functions above cognitive complexity 20;
- zero actionable production functions above cyclomatic complexity 15;
- at most the three reviewed exhaustive error mappings at cyclomatic 16;
- zero production functions above 200 source-span lines;
- BCA baseline entries reduced from 26 to at most three;
- unsafe functions/expressions/impls/traits/methods at or below the initial
  baseline, with performance-neutral reductions retained;
- no decrease greater than 0.5 percentage points in line, function, or region
  coverage;
- no new production dependency and no unexplained duplicate dependency;
- clean formatting and Clippy in both feature configurations;
- every emitted test binary reporting `test result: ok` in both full suites;
- passing metric ratchets and five-run paired performance evidence for affected
  hot paths;
- a committed after-sweep history snapshot and a before/after report covering
  complexity distributions, exception count, long functions, unsafe surface,
  coverage, dependency counts, binary size, and benchmark evidence.

Long coverage runs and benchmark evidence are retained as generated artifacts;
only explicit metrics history snapshots, deterministic baselines, design/plan,
and the final summary are committed.

## Failure Handling

Tool-version mismatches, malformed metric documents, partial unsafe scans,
thermal ambiguity, and intermittent tests are investigated as failures of
evidence. They are not converted into accepted debt. Any refactor that cannot
preserve a critical invariant or the performance gate is reverted while its
tests and diagnostic findings may be retained if independently useful.
