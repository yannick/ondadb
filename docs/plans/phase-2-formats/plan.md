# Phase 2 — storage-format track

Baseline **ondaDB 0.8.2** (`3afc3c1`). Independent features sharing only the
1.0 capability framework. Mixed-format levels are normal: readers decode legacy
and new blocks indefinitely; writers select formats from persisted CF options.

| # | Feature | Readiness | Effort | Depends on |
| --- | --- | --- | ---: | --- |
| [2.1](features/21-prefix-delta-key-encoding.md) | restart-based prefix-delta data blocks | design ready (ondaDB already has restarts) | 4–6 wks | 1.0 (incl. Change A's strict footer mask), golden block corpus |
| [2.2](features/22-manifest-edit-log.md) | snapshot plus numbered version edits | architectural; **local protocol only** | 5–8 wks | 1.0; the two prerequisite fixes below |

**Delivery order:** 2.2 first (RV-M5 severity — the parts/tiers scaling wall),
then 2.1. RV-M5 is the one review item explicitly deferred as "a format and
recovery feature, not a contained corrective patch"
(`docs/code-review-2026-08-resolution.md`, M5 row), and its 12.4 MiB-per-persist
sizing probe remains the standing evidence. 2.2's local two-file protocol is the
whole scope for ondaDB: the wavesdb object-store/replica/promotion slices do not
apply (assessment §5 — ondaDB's manifest is local by design, object CAS is
ayu's layer).

**Prerequisites for 2.2**, small and independently valuable (2.2 §Prerequisites):

- `Manifest::save`'s directory fsync discards both its errors
  (`src/manifest.rs:145-152`) — convert to `util::sync_parent_dir`.
- `close` discards its persist result (`src/db.rs:1054`) — propagate it.

## Format rules

- Self-description lives at the artifact that needs it: a detached/frozen/
  mounted table must identify its own encoding without consulting the DB
  manifest (ondaDB `freeze_part`/`attach_part`/`attach_part_by_ref` all produce
  standalone table sets). In practice this means **footer flags**, which is
  where `FOOTER_RESTARTS`/`FOOTER_VLOG_V2`/`FOOTER_BTREE`/`FOOTER_HAS_BLOOM`
  already live (`src/sst/reader.rs:230-238`) — not a per-cached-block tag, for
  which `BlockCache` has no slot (it is keyed `(file_id, off)` and stores bytes
  only, `src/cache/block.rs:22-32`).
- Unknown complete encodings fail. Only explicitly defined partial EOF tails
  may be ignored.
- A config option controls new writes, never whether old data can be read.
- Golden bytes pin magic, version, capability numbers, field order,
  endianness, checksums, restart layout, edit IDs, generation headers.
- Fuzzers cover decoders before new writers become default-capable.
- New persisted CF options land complete in one change: `Default` + `validate`
  + config-blob tail + `CONFIG_*_MAGIC` + reopen test.

## Checkpoints

1. **P2-0 — corpus frozen.** Legacy block golden corpus and VERSION-1 manifest
   corpus committed and passing against the untouched decoders. Must precede
   2.1's block-size accounting change, which shifts block boundaries.
2. **P2-2·0 — 2.2 prerequisites.** The two fixes above, each with its own test.
3. **P2-1 — 2.2 codec + recovery.** Ops, framing, v2 snapshot fields,
   `Recover`, all R* rows, and the catalog-inventory guard. The guard must name
   the opaque `CfManifest.config` blob as the carrier for partition rules and
   tier definitions (`src/db.rs:311`) — they are not manifest fields, and
   without that the guard reports a false failure. No call site migrated.
4. **P2-2 — 2.2 crash protocol.** Snapshot compaction and `catalog_txn`
   against a fault shim; S*/A* rows.
5. **P2-3 — 2.2 call-site migration + enable.** All 20 sites, in order: flush
   and unified flush → **ingest** (`ingest.rs::Ingestion::finish`, an
   `AddTable` producer that belongs with flush) → compaction (merge, then FIFO)
   → mover/parts → maintenance/CF lifecycle → open paths. AGENTS.md invariant 1
   is restated in the flush commit: WAL reclaim keys off the **edit fsync**, not
   a snapshot write.
6. **P2-4 — 2.1 delta reader.** Point seek, iterator `seek`/`seek_to_last`/
   `prev`, merge-iterator buffered-key fallback, against legacy/delta twins;
   corruption matrix.
7. **P2-5 — 2.1 writer + wiring.** Options, capability, mixed tables, the
   restart-interval × `data_block_size` sweep. Opt-in default stays legacy
   unless the sweep is decisive.

## Exit criteria

- Every existing fixture plus legacy/new/mixed twins opens and scans
  identically, in both feature configurations. 2.1 additionally runs its reader
  tests in **all four** configurations (default, `mmap-reads`,
  `arena-memtable`, `unsafe-fastpath`) at least once per slice: `Block::Mapped`
  exists only under `mmap-reads` (`src/sst/mod.rs:129`), and `unsafe-fastpath`
  is the alias for both (`Cargo.toml:32`). AGENTS.md's 4-command two-config
  gate remains the per-commit CI equivalent.
- New format options can be disabled for future output without stranding
  written tables or edit logs.
- Corruption tests distinguish truncated tails from bad checksums and
  semantic-invalid transitions.
- Benchmarks include decode CPU, resident cache bytes, metadata bytes, fsyncs,
  and open/replay time — not only final disk size. Per AGENTS.md: ≥5 runs,
  same-run ratios, never absolute numbers across sessions.
