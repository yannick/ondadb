# August 2026 code-review resolution

This document records the disposition of every item in
`docs/code-review-2026-08.md`. The corrective work is released as 0.8.2. For
each implemented bug, its regression test was first observed failing against
the reviewed code and passing after the fix.

## Correctness and durability findings

| Finding | Resolution | Validation |
|---|---|---|
| F1 | Fixed in `f306b2b`. Backup, checkpoint, and CF clone resolve the source table's tier, durably copy it into the destination default tier, and clear tier/object metadata. | `tiered_backup_is_default_tier_self_contained`, `tiered_checkpoint_is_default_tier_self_contained`, `clone_of_tiered_cf_copies_data_to_the_default_tier` |
| F2 | Fixed in `250cb78`. Both classic and by-reference attach test a candidate range against live bottom tables and tables staged earlier in the same operation; overlapping input goes to L0. | `attach_mutually_overlapping_staged_tables_uses_l0`, `attach_by_ref_mutually_overlapping_tables_uses_l0` |
| F3 | Fixed in `33a7b0e`. Per-CF WAL mode rejects a transaction touching multiple CFs before reserving sequence numbers or applying data. Unified WAL mode remains the atomic cross-CF layout. | `per_cf_multi_cf_commit_is_rejected_without_partial_apply` plus the existing unified multi-CF/recovery suite |
| F4 | Fixed in `c6c9471`. Attach copies through `StorageWriter` and calls `finish` before the manifest commit. | `attach_copy_propagates_storage_finish_failure` injects a finish failure and verifies nothing is installed |
| F5 | Fixed in `04592de`. Creating a WAL stripe fsyncs its parent directory; reopening an existing stripe does not impose that creation sync. | `new_wal_creation_propagates_parent_sync_failure`, `existing_wal_does_not_require_creation_sync` |

## Medium findings

| Finding | Resolution | Validation or rationale |
|---|---|---|
| M1 | Fixed in `3319299`. Bytewise unified CF iterators now use lazy prefix-bounded cursors over the shared memtable; custom comparators retain materialization because shared bytewise order cannot represent their order. | `unified_iteration_does_not_materialize_shared_memtable` passes in default and unsafe-fastpath builds. Five release-mode probes improved iterator construction from a 51,439 ns median to 1,864 ns, about 27.6x. |
| M2 | Fixed in `62cf05d`. `CfStats` exposes `compaction_failures` and `last_compaction_error` for manual and background failures. | `compaction_failure_is_reported_in_stats` |
| M3 | Deferred. The latency coupling is real, but dropping `commit_mu` around WAL/apply would break the validation-to-apply exclusion unless the engine first gains write intents or a second validation/publication protocol. That protocol change is too broad for a corrective release. | No correctness failure exists under the current locking; this remains a performance design item. |
| M4 | Fixed for the reported default-tier case in `76cdae4`. Open removes manifest-unreferenced default-tier SST files while retaining the separately documented named-tier/S3 gaps. | `unknown_default_tier_sst_orphans_are_removed_on_open` and unit coverage of the sweep predicate |
| M5 | Deferred. The measured full-manifest rewrite cost is real; an edit log plus periodic snapshot compaction is a format and recovery feature, not a contained corrective patch. | The existing ignored `manifest_encoded_size_at_scale` probe remains the sizing evidence. |
| M6 | Fixed in `ef496d1` (with published 0.8.1 ancestry retained by `2fd2b39`). `ColumnFamilyConfig::data_block_size`, default 4 KiB, is persisted in `ONDABLK1` and used by flush, ingest, and compaction. | Config-tail tests and `tests/data_block_size.rs` cover defaults, validation, reopen, cross-policy reads, and actual block-policy effect. |
| M7 | Fixed in `0f721be`. Startup now spawns `num_compaction_threads.max(1)` consumers. | `configured_compaction_workers_run_disjoint_cfs_concurrently` |
| M8 | Fixed in `0ac1dc8`. Each CF tracks its own pending flushes; `flush_memtable(cf)` waits only for that CF. Database close still drains globally. | `flush_memtable_waits_only_for_target_cf` |
| M9 | Fixed in `ae1c533`. A manifest level above 64 is rejected before allocation. | `manifest_level_above_limit_is_corruption` |
| M10 | No code change. Full block decoding validates every entry and offset before an iterator can call its infallible internal seek; the internal-key helpers receive engine-created internal keys. The alleged panic is not reachable through a CRC-valid reader under current interfaces. | Existing corruption integration tests exercise malformed blocks, WALs, vlogs, and manifests without panic. |

## Low findings

| Finding | Resolution | Validation or rationale |
|---|---|---|
| L1 | Fixed in `7739691`. Merge ordering explicitly prefers the earlier child on exact key/sequence ties in both directions; the transaction overlay is intentionally child zero. | `exact_ties_prefer_earlier_child_in_both_directions` |
| L2 | Fixed in `510b258`. Commit floors are keyed by process-monotonic database instance ids rather than reusable allocation addresses. | `database_instance_ids_are_monotonic_and_unique` and the read-your-writes suite |
| L3 | Fixed in `5701813`. Savepoints record Serializable read-set lengths and rollback truncates later reads. | `serializable_savepoint_rollback_discards_later_reads` |
| L4 | Fixed in `cf6d09d`. Reset waits through the transaction's own publication floor before taking a new fixed snapshot. | `reset_fixed_snapshot_waits_for_own_commit_floor` forces a publication gap |
| L5 | No change. Seeing an in-flight higher sequence can only cause a conservative conflict abort. Restricting the probe without a publication-aware replacement risks missing a real conflict. | Retained as a documented conservative behavior. |
| L6 | No change after measurement. A 100-table, zero-victim FIFO TTL selection pass took 285, 319, 305, 284, and 296 microseconds in release mode (median 296 microseconds). Moving metadata I/O out of the state lock adds coordination complexity without evidence of a material bottleneck. | Ignored `fifo_ttl_selection_probe` preserves the reproducible probe. FIFO tables remain default-tier L0 data. |

## Configuration and API surface

`num_compaction_threads` is now active (M7). The other review-listed inactive
fields remain public for source compatibility and are explicitly documented as
reserved/ignored: `max_concurrent_flushes`, `max_memory_usage`, `log_level`, the
unified and per-CF skip-list knobs, `default_isolation_level`, `min_levels`,
`dividing_level_offset`, tombstone-density knobs, sampled-index knobs,
`comparator_ctx_str`, and `min_disk_space`.

The dead format/API surface is retained compatibly. `DELTA_SEQ`, `Busy`,
`MemoryLimit`, sparse bloom helpers, and `Wal::size` do not create correctness
issues. `single_delete` documentation now states its actual conservative
tombstone behavior; no compaction optimization is promised.

## Scope decisions and documentation audit

Compaction I/O rate limiting, incremental manifests, per-range size queries,
multi-process/replica support, and sequence-remapping cross-database attach are
features rather than corrective fixes. S3 orphan collection and obsolete-input
deletion remain the documented storage-leak-only gaps; the manifest remains
authoritative.

The documentation drift identified by the review was corrected: the engine has
16 memtable shards, the default build denies unsafe with one audited Linux clock
exception, part lifecycle operations use range locks, the active default block
target is 4 KiB, cross-CF atomicity depends on unified WAL mode, and reader-level
bloom behavior is described accurately.
