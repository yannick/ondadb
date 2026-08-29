# ondaDB 0.8.2 Code-Review Corrective Release Design

## Goal

Ship a test-backed `v0.8.2` release that closes the correctness and durability
defects in `docs/code-review-2026-08.md`, repairs the actionable bounded
robustness defects, and records explicit dispositions for findings that are
either already fixed, not reproducible, deliberately conservative, or too
architectural for a corrective release.

## Release base and compatibility

`main` and the tagged `v0.8.1` commit are sibling branches from `v0.8.0`.
The release history must remain monotonic, so the `v0.8.1` block-size work will
be merged into `main` before `v0.8.2` is tagged. Its per-family
`data_block_size` setting resolves M6 and its tests travel with the merge.

This is a source-compatible corrective release except for one intentional
behavioral restriction: a transaction touching more than one column family is
rejected at commit time when the database uses per-CF WALs. Such a transaction
was never atomic; accepting it exposed partial writes after an I/O failure or
crash. Unified-memtable mode remains the supported atomic multi-CF layout.

The remaining public no-op configuration fields stay present in 0.8.2 to avoid
a source-breaking release. Their documentation will identify them as reserved
or currently ignored. Removing them belongs in a future major release.

## Correctness and durability changes

### Self-contained snapshots and clones (F1)

Snapshot code will resolve each table through its column family's tier-aware
metadata and `Storage` backend. Checkpoints may hard-link an accessible local
file, but must fall back to copying through `ReadHandle` for remote storage or
cross-device links. Backups always copy. Destination files and directories are
made durable before the destination manifest is published.

The copied manifest rewrites every copied `SstMeta` to the destination's
default tier by clearing `tier` and `object`. A restored checkpoint or backup
therefore needs no access to the source tier registry. Column-family cloning
uses the same tier-aware source resolution; default-tier local files can remain
hard-linked, while tiered or remote files are durably copied under fresh IDs.
Cloned metadata likewise clears `tier` and `object`.

### Attach placement and durability (F2, F4)

Both `attach_part` and `attach_part_by_ref` will classify staged tables against
the live bottom level and against tables staged earlier in the same operation.
Any table overlapping either set goes to L0; only mutually disjoint staged
ranges may enter the bottom level. Comparator calls, not bytewise assumptions,
define overlap.

Physical attach copies will use the default `Storage::create` /
`StorageWriter::finish` path. The manifest cannot name a copied table until
both klog and optional vlog have completed the backend's durability operation.
Rejection cleanup remains pre-publication and best effort.

### Transaction atomicity contract (F3)

Commit preparation will count distinct column families before sequence
reservation or WAL application. Per-CF mode rejects a count greater than one
with `InvalidArgs`, leaving all writes unapplied and invisible. Unified mode
continues to encode all CF records in one WAL frame and remains atomic across
the touched families. Documentation will state this distinction at the public
transaction API, architecture write path, concurrency guide, README, and
release notes.

### WAL creation durability (F5)

The existing parent-directory fsync logic will move to a shared utility.
`Wal::open` will detect whether any stripe was newly created and sync the WAL
parent directory after opening those files. Creating a new CF therefore makes
the generation-zero WAL entry durable before a Full-mode commit can be
acknowledged; rotations receive the same guarantee for every new generation.

## Bounded robustness and operational changes

### Unified iterator fast path (M1)

For bytewise column families, unified-mode iterators will wrap lazy memtable
iterators over the contiguous `cf_id || user_key` prefix range. The wrapper
seeks directly to the prefix, strips the eight-byte CF id from keys, and stops
at the end of the prefix in both directions. Active and immutable unified
memtables each contribute one lazy child; no entry or value is materialized.

A custom comparator cannot consume the bytewise ordering of the shared
memtable directly. Those CFs retain the current materialize-and-reinsert path,
which restores their comparator order. The documentation will make that
fallback explicit. Repeated measurements will validate the bytewise fast path
before and after the change.

### Compaction failure observability (M2)

Each CF will track a monotonic compaction-failure count and the latest error
string. Background compaction records an error instead of discarding it, while
remaining non-fatal. `CfStats` exposes both fields. Manual compaction continues
to return its error directly and also records it for consistent statistics.

### Default-tier orphan collection (M4)

The startup sweep runs after recovery and before workers while the database
LOCK excludes another process. In the default CF directory it will delete
numeric klog/vlog files whose ID is absent from the loaded manifest, as well as
known files in the wrong location. Unknown files in named or shared tiers keep
the existing conservative behavior because they may belong to storage with
different ownership or GC rules.

### Configuration-backed compaction workers (M7)

`spawn_workers` will create `num_compaction_threads.max(1)` consumers of the
existing multi-consumer channel. The existing range locks exclude overlapping
jobs, and `mover_running` continues to serialize mover passes.

### Per-CF flush waiting (M8)

Per-CF mode will increment a pending counter on the CF when its immutable
memtable is queued and decrement it when that exact job completes.
`flush_memtable(cf)` waits for that CF only. Unified mode keeps the global wait
because one shared immutable contains slices for multiple CFs. `close()` keeps
the global drain in both layouts.

### Manifest level validation (M9)

Opening a CF will reject an SST metadata level greater than 64 with
`OndaError::Corruption` before calculating an allocation size. A CRC-valid
manifest can therefore neither force an enormous allocation nor silently alter
bottom-level semantics.

## Low-severity correctness changes

### Explicit overlay precedence (L1)

Merge ordering will use the child index as an explicit final tie-breaker after
key and sequence. Earlier children win exact ties in both iteration directions;
the transaction overlay is intentionally child zero. This preserves current
behavior while making it contractual and testable.

### Stable database identity (L2)

Every `DbInner` receives a process-monotonic `u64` identity from a global
atomic. The thread-local commit-floor map keys on this identity rather than a
reusable allocation address.

### Serializable savepoint reads (L3)

Serializable point reads will maintain an insertion-order log in addition to
the deduplicating set. A savepoint records the read-log length. Rolling back
removes reads first observed after that savepoint and rebuilds or prunes the CF
lookup map, preventing conflicts on logically rolled-back reads.

### Reset snapshot floor (L4)

Resetting to a fixed-snapshot isolation level will call
`wait_visible_at_own_floor` before reading and pinning `visible_seq`, matching
`begin_with_isolation`.

## Findings without a 0.8.2 code change

- M3 is real DB-wide latency coupling, but `commit_mu` currently makes conflict
  validation and application indivisible. Releasing it around fsync without
  write intents or revalidation would break Snapshot semantics. That protocol
  redesign requires a separate measured design.
- M5 is a real scale ceiling. An incremental manifest changes the durable
  catalog protocol, recovery, compatibility, and compaction of the manifest
  itself; it is a separate feature rather than a corrective patch.
- M10's `seek` unwrap is reached only after `load_block(full=true)` has decoded
  every offset against the same immutable block. Malformed entries set the
  iterator error and return before the binary search. The internal-key helpers
  accept engine-owned keys, not persisted untrusted slices. No reproducing path
  exists under the current interfaces.
- L5 deliberately over-checks in-flight memtable entries and can only cause a
  spurious conflict. Weakening it without a publication-aware lookup risks an
  under-check, so behavior remains unchanged.
- L6 is performance-only. Metadata collection will be profiled and measured at
  least five times; it changes only if evidence shows a worthwhile regression-
  free improvement.
- The missing-feature inventory and already-documented limitations are not
  regressions and are not silently presented as 0.8.2 deliverables.

## Documentation changes

The release updates the shard count from 256 to 16, describes `deny` rather
than `forbid` for default unsafe-code policy, fixes the range-lock inventory,
uses the merged `data_block_size` setting as the single block-size truth,
documents the per-CF/unified transaction distinction, and clarifies bloom
pre-filtering at the reader boundary. Public no-op configuration fields are
described honestly as reserved/currently ignored.

The original review remains intact. A resolution document or appendix will map
every detailed finding to its test, change, or explicit no-change rationale.

## Test strategy

Each behavioral fix starts with a focused failing test and an observed expected
failure before production code changes:

- tiered checkpoint, backup, and clone restore with the source tiers absent;
- mutually overlapping staged tables never both enter the bottom level;
- per-CF multi-CF commit rejects without publishing either write, while unified
  multi-CF commit remains durable;
- attach copy finalization and WAL parent-sync failure propagation;
- unified bytewise iterator construction avoids materialization and preserves
  forward, backward, bounded, and seek behavior in both memtable builds;
- background compaction errors appear in CF statistics;
- default-tier unknown SST files disappear on reopen while known files remain;
- configured compaction workers can enter two disjoint jobs concurrently;
- flushing one CF does not wait for another CF's queued job;
- a CRC-valid level-65 manifest is rejected without allocation;
- exact merge ties prefer child zero;
- DB identities do not repeat in a constructed lifetime sequence;
- Serializable reads after a savepoint no longer conflict after rollback;
- reset waits through a forced publication gap.

Durability properties that cannot be reproduced with a real crash portably use
small internal seams that propagate injected `finish`/directory-sync failures.
Tests assert observable errors and publication ordering, not source text.

Final verification runs, with complete output checked for every test binary:

1. `cargo build`
2. `cargo build --features unsafe-fastpath`
3. `cargo test`
4. `cargo test --features unsafe-fastpath`
5. `cargo clippy --all-targets`
6. `cargo clippy --all-targets --features unsafe-fastpath`
7. An S3-feature compile check because snapshot copying crosses the `Storage`
   abstraction.

## Release operation

After verification, update `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` for
`0.8.2`; commit the implementation and audit resolution; and create a local
annotated `v0.8.2` tag. No remote push, hosted release, or crate publication is
performed without additional authorization.
