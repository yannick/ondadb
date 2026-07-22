# Changelog

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
