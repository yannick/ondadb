# Changelog

## 0.5.0

Three additive changes, no API or format break. Minor bump: two new public
`DB` methods (the durability-inspection hooks).

- **Durability-inspection hooks** — new `DB::column_family_config(name)`
  returns a column family's *effective* durable configuration (what a reopen
  restores, including live-added partition rules), and
  `DB::wal_sync_count()` counts successful physical `sync_data()` calls
  across every WAL the DB opens (per-CF, unified, and post-rotation; wired
  like the poison flag, so rotation never resets it). Motivation: a consumer
  whose correctness depends on `SyncMode::Full` (spada's raft O1 boundary)
  can now *verify* the recorded mode instead of assuming the config it
  opened with, and tests can assert on real physical syncs — under
  `SyncMode::None` the counter never advances, which is exactly what a
  durability test should pin. Added by the spada raft-correctness
  remediation (its S-077/S-082 decisions).

- **Batch column-family creation** — new
  `DB::create_column_families(&[(&str, ColumnFamilyConfig)])` creates a batch
  of CFs and persists the manifest **once** for the whole batch instead of
  once per CF. Semantics are identical to N sequential `create_column_family`
  calls (handles returned in input order); all names are validated up front
  (length, comparator, config, collisions against existing CFs, duplicates
  within the batch), so a conflicting batch creates nothing and never
  persists, and the registry write lock is held across the batch so a
  concurrent creator can never observe it half-built. Motivation: each
  per-CF creation ran a full manifest rebuild + persist — a temp-file
  `sync_all()` (`F_FULLFSYNC` on macOS) plus a directory fsync — and the
  manifest is a full rebuild over all CFs, so after N creations the last
  write already contained everything the first N−1 wrote. A consumer opening
  11 CFs at boot paid ~22 fsyncs for the information content of 2; measured
  209.8 ms per-CF vs **22.5 ms batched** for 11 CFs (median of 8, real
  `F_FULLFSYNC`, ~9.3×). The single-CF path is untouched. Deliberately *not*
  done: lazy WAL materialization — `Wal::open` performs no fsync in any sync
  mode (WAL creation was never the cost), and deferring it would complicate
  the crash-consistency-critical commit/rotation path to save ~22 ms of file
  creates.
- **S3 backend: bounded retry on transport errors** (feature `s3`) —
  every request the backend issues is wrapped in a bounded retry: up to 4
  attempts, 25/50/100 ms backoff, retrying **only** transport-level
  `S3Error::Hyper`/`S3Error::Io`. This closes the hyper 0.14 keep-alive
  reuse race (hyperium/hyper#2136): rust-s3 0.35's tokio backend drives a
  raw `hyper::Client` with a 90 s idle pool and no retry, so a store (or a
  NAT in front of it) that drops a pooled idle connection first kills the
  next request mid-flight with `IncompleteMessage` — a bodied PUT is the
  most exposed because hyper will not replay it. Retrying is sound because
  every operation this backend performs is idempotent by construction:
  part objects use unique never-reused ids and are written whole (single
  PUT), and reads/HEAD/COPY/DELETE/LIST are idempotent by nature. HTTP
  status failures surface as `Ok` with a non-2xx code and can never
  trigger a retry; nothing non-idempotent exists to be retried.

## 0.4.0

Derived-partitioning milestone (A5). Additive — no manifest or API break; a
rules-only column family encodes byte-for-byte as before.

- Partitions may be computed from the key by a consumer-supplied `PartitionFn`
  (`PartitionScheme::Derived`) rather than enumerated in `partition_rules`. The
  partition becomes a *function* of the key, which is what a consumer keying by
  `(namespace, cluster_key)` needs — its partition count is a function of the
  data, not a list anyone can write out in advance. Persisted by scheme name and
  resolved on open through `Options::partition_fns`, mirroring comparators; a
  missing or mismatched implementation is a hard error, never a silent fallback
  to rule-based cutting.
- `DB::list_partitions` returns the materialized bottom-level partitions
  (`PartitionInfo`), read-only, so a consumer can verify its partitioner
  produced the physical separation it intended.
- Debug-only guard in bottom compaction against a `PartitionFn` that is not
  order-compatible — the misimplementation that would otherwise produce a bottom
  SSTable spanning two partitions. No release-build cost.

## 0.3.2

- Derived partitioning (A5) landed (see 0.4.0 for the consumer-facing
  additions and hardening that finalized it).
- Zero-materialization flush merge.

## 0.3.1

- Make part-tier move retries safe after a lost post-commit response, including
  partitions containing a mix of already-moved and off-target SSTables.
- Add deterministic move-phase observation for crash and durability testing.
- Preserve more than 255 per-level compression, compression-rule, partition,
  and tier policies without breaking manifests written by ondaDB 0.3.0.
- Release the database directory lock when the final public `DB` handle drops.
