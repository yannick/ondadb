# yoloDB format registry

ondaDB and wavesdb are converging on **one on-disk format family, yoloDB**
(plan C, `docs/plans/phase-c-yolodb-convergence/plan.md`). Each engine may keep
specialities as optional capabilities, but every number either engine writes
into a shared artifact comes from this page. The failure this registry exists
to prevent is not a missing feature but a **silent misread** — the same number
meaning two different things in two engines that read each other's files.

This page describes **format epoch 1**, which ondaDB 0.10 writes. The numbers
are enforced, not merely documented: `src/format.rs` pins each one with a
`const _: () = assert!(...)` (a collision fails the **build**) and with the
golden tests `format::tests::epoch1_identifiers_are_pinned` and
`tests/epoch1_golden.rs` (a renumbering fails a test).

> **Rule going forward:** assign from this page, then implement. A number that is
> written down here and unimplemented costs nothing; a number implemented and not
> written down here is the failure mode above. Plan C step 2 moves this page to a
> shared `yolodb-format` repository.

## Checksum

Every epoch-1 checksum is **CRC32-C** (Castagnoli, reflected polynomial
`0x82F63B78`; check value `"123456789"` → `0xE3069283`). ondaDB 0.9 documented
CRC32-C but computed CRC-32/IEEE (`0xCBF43926`); that polynomial is used only by
the read-only `legacy_onda` decoders.

## Magics and versions

Every magic is exactly 8 printable ASCII bytes filling a `u64` slot, followed by
a version that counts revisions **within** the epoch. A new magic is minted only
for a future epoch break (`YOLOST02`, …); plan C step 2 lands as epoch 1,
version 2.

| Artifact | Magic | Version | Where | Replaces (0.9) |
| --- | --- | ---: | --- | --- |
| SSTable footer | `YOLOST01` | `format_version` u32 = 1 | last 8 bytes of `.klog` | `WAVESST1` (u64) |
| `MANIFEST` | `YOLODBMF` | u32 = 1 | offset 0 | `WVMF` (u32), v1/v2 |
| `MANIFEST-EDITS` | `YOLODBED` | schema u32 = 1 | offset 0 | `ONDE` (u32) |
| WAL segment | `YOLODBWL` | u32 = 1 | offset 0 of every stripe | *(no header)* |
| Value log (`.vlog`) | `YOLODBVL` | u32 = 1 | offset 0 | *(no header)* |
| CF config blob | `YOLODBCF` | u32 = 1 | offset 0 of the blob | positional + `ONDA*` tails |

An unknown version is `UnsupportedFormat`. A 0.9 magic in an epoch-1 position
is `UnsupportedFormat` naming the upgrade path; any other foreign magic is
`Corruption` (a WAL segment without a header is `UnsupportedFormat`, refused at
byte 0).

## SSTable footer (96 bytes)

| Off | Field | Notes |
| ---: | --- | --- |
| 0 | index handle: `off u64 \| len u64` | |
| 16 | bloom handle: `off u64 \| len u64` | `(0, 0)` iff no `FLAG_BLOOM` |
| 32 | `num_entries u64` | |
| 40 | `max_seq u64` | |
| 48 | `flags u32` | see below; strict mask |
| 52 | `format_version u32` = 1 | |
| 56 | capability word `u64` | the table's subset, see below |
| 64 | aux handle: `off u64 \| len u64` | `(0, 0)` when no aux block |
| 80 | `crc32c u32` over bytes 0..80 | |
| 84 | reserved `u32` = 0 | non-zero is `Corruption` |
| 88 | magic `YOLOST01` | |

### Footer flags (shared allocation)

| Bit | Meaning |
| --- | --- |
| `0x01` | a bloom block is present |
| `0x02` | the index is a B+tree root |
| others | unassigned — `UnsupportedFormat` |

Footer flags carry **no format meaning** in epoch 1. 0.9's `0x04` (restart
trailers) became unconditional, its `0x08` (vlog v2 frames) became the only vlog
frame, and its `0x10`/`0x20` (extended entries, prefix-delta) moved into the
capability word — which also retires the 0.9/wavesdb collision where `0x04` and
`0x08` meant different things in the two engines.

### Table capability word

A table declares, in its footer, the capability bits a reader needs to decode
it: a subset of `TABLE_CAPS = CAP_EXTENDED_RECORDS | CAP_MERGE_OPERANDS |
CAP_RANGE_DELETES | CAP_PREFIX_DELTA`. A bit outside `KNOWN_CAPS` is
`UnsupportedFormat`; a known database-level bit, a table bit without
`CAP_EXTENDED_RECORDS`, or `CAP_RANGE_DELETES` disagreeing with the aux block's
range section is `Corruption`.

## Capability bits (manifest header word, table footer word)

| Bit | Meaning | ondaDB | wavesdb |
| --- | --- | --- | --- |
| 0 | extended (kind-bearing) records | `CAP_EXTENDED_RECORDS` | `CapExtendedRecords` |
| 1 | merge operands | `CAP_MERGE_OPERANDS` | `CapMergeOperands` |
| 2 | range deletes | `CAP_RANGE_DELETES` | `CapRangeDeletes` |
| 3 | prefix-delta blocks | `CAP_PREFIX_DELTA` | `CapPrefixDeltaBlocks` |
| 4 | manifest edit log | `CAP_MANIFEST_EDITS` | `CapManifestEdits` |
| 5 | periodic age stamps | `CAP_PERIODIC_AGE` | `CapPeriodicAge` |
| 6 | transaction decisions | `CAP_TXN_DECISIONS` | `CapTxnDecisions` |
| 7 | managed sequence mode | *reserved to wavesdb* | `CapManagedMode` |
| 8 | vlog compression grouping | *reserved to wavesdb* | `CapVlogGrouping` |

Bits 0–6 were assigned identically by both engines independently. 7 and 8 are
wavesdb specialities; ondaDB reserves them so it cannot assign them to anything
else.

## Manifest sections

The manifest's optional fields are **flagged sections** in ascending bit order,
each under a strict mask (unknown bit: `UnsupportedFormat`). Layout:
`docs/formats.md` § MANIFEST.

| Word | Bit | Section | Payload |
| --- | --- | --- | --- |
| db (u32) | `0x1` | unified WAL layout | none |
| db | `0x2` | instance nonce | `u64` |
| db | `0x4` | edit-log cursor | `generation u64 \| applied_through u64 \| next_edit_id u64` |
| cf (uvarint) | `0x1` | unified CF id | `u64`, only when ≠ FNV-1a-64(name) |
| sst (uvarint) | `0x01` | partition | string |
| sst | `0x02` | tier | string |
| sst | `0x04` | object stem | string |
| sst | `0x08` | `max_entry_time` | uvarint (`i64` as `u64`) |
| sst | `0x10` | `last_compaction_time` | uvarint (requires `CAP_PERIODIC_AGE`) |
| sst | `0x20` | range summary | `count \| min_seq \| max_seq \| min_key* \| max_key*` (requires `CAP_RANGE_DELETES`) |

## WAL segment layouts

| Byte (segment header offset 12) | Layout | Envelope schema |
| ---: | --- | ---: |
| 1 | one WAL per column family | 1 |
| 2 | unified (cf-id-prefixed keys) | 2 |

## Record kinds (WAL envelope, extended SST entries)

| Kind | Meaning | ondaDB | wavesdb |
| ---: | --- | --- | --- |
| 1–5 | put, delete, single-delete, merge, range delete | yes | yes |
| 6–15 | reserved for future data kinds | — | — |
| 16–18 | prepare, commit decision, abort decision | yes | yes |
| 19 | large-transaction spill descriptor | *reserved to wavesdb* | `KindSpillDescriptor` |
| 20–31 | reserved for further transaction control | — | — |
| 32 | managed-sequence ownership | *reserved to wavesdb* | `KindManagedOwnership` |
| 33 | managed-sequence discard floor | *reserved to wavesdb* | `KindManagedDiscardFloor` |
| 34–63 | reserved | — | — |
| > 63 | never assigned — `Corruption`, not `UnsupportedFormat` | — | — |

## Entry flags (the flags byte, and the extended modifier word)

| Bit | Meaning | Status |
| --- | --- | --- |
| `0x01` | tombstone | shared |
| `0x02` | has TTL | shared (also a modifier) |
| `0x04` | value in the vlog | shared (also a modifier) |
| `0x08` | wavesdb `FlagVlogGrouped` | **reserved to wavesdb**; ondaDB rejects it |
| `0x10` | single-delete | shared |
| `0x20`–`0x80` | unassigned — do not assign; use kinds | — |

ondaDB once named `0x08` `DELTA_SEQ`, an encoding no writer ever produced;
wavesdb independently assigned it to grouped vlog pointers and writes it, so the
bit is wavesdb's. ondaDB's rejection is correct and must stay: it cannot
decompress a group, and failing closed beats misreading a pointer.

## Compression codec ids

Stored per block frame and per vlog frame.

| Id | Codec | Epoch 1 |
| ---: | --- | --- |
| 0 | none | written and read |
| 1 | snappy | written and read |
| **2** | ***burned*** — LZ4 in ondaDB 0.9, zstd in wavesdb | never written; `UnsupportedFormat` |
| 3 | zstd | written and read |
| **4** | ***burned*** — LZ4-fast in ondaDB 0.9, zstd in wavesdb | never written; `UnsupportedFormat` |
| 5 | raw deflate | written and read |
| 6 | raw LZ4 block (`lz4_flex`) | written and read (ondaDB `Lz4` and `Lz4Fast`) |
| 7 | zstd with a per-file dictionary | reserved until plan C step 2 |
| 8 | brotli | reserved (plan C row S) |

Ids 2 and 4 are permanently unassigned: each meant a different codec in each
engine, so neither can ever reuse them. Tag-6 bytes are identical to ondaDB
0.9's tag-2 bytes, which is why moving LZ4 cost one constant.

## Bloom filter hash ids

The hash id **leads** the bloom block: `hash u8 | m uvarint | k uvarint | words
u64 LE`, with `1 ≤ m < 2^32` and `1 ≤ k ≤ 30`.

| Id | Hash | Epoch 1 |
| ---: | --- | --- |
| 0 | ondaDB 0.9 FNV-1a (truncated offset basis) | never written; `UnsupportedFormat` |
| 1 | xxh3-64, seed 0, over the user key | written and read |

## Value-log header flags

`flags u32` at offset 12 of the vlog header: **no bit is assigned in epoch 1**
(any bit is `UnsupportedFormat`). Reserved for per-file dictionaries and
compression groups (plan C step 2).

## Unified column-family id

FNV-1a-64 of the CF name with the standard offset basis `14695981039346656037`
and prime `1099511628211` — the same function wavesdb uses. A manifest may
override it per family (the CF section above). ondaDB 0.9 used the basis
`1469598103934665603` (one digit short); that survives only in `legacy_onda`.

## CF config TLV tags

`YOLODBCF | version u32 | (tag uvarint, len uvarint, value)*`, tags strictly
ascending, defaults elided, **unknown tags preserved** verbatim across a
decode→encode round trip. Durations are **nanoseconds**. Scalars are one
minimal uvarint; booleans one byte `0`/`1`; codec values are codec ids.

| Tag | Field | Value |
| ---: | --- | --- |
| 1 | `comparator_name` | UTF-8 |
| 2 | `compression` | codec id (u8) |
| 3 | `write_buffer_size` | uvarint |
| 4 | `level_size_ratio` | uvarint |
| 5 | `klog_value_threshold` | uvarint |
| 6 | `enable_bloom_filter` | bool |
| 7 | `bloom_fpr` | f64 bits (u64 LE) |
| 8 | `l1_file_count_trigger` | uvarint |
| 9 | `l0_queue_stall_threshold` | uvarint |
| 10 | `use_btree` | bool |
| 11 | `sync_mode` | u8: 0 none, 1 full, 2 interval |
| 12 | `sync_interval` | uvarint ns |
| 13 | `compression_per_level` | codec id per level. **Never elided when non-empty; absent = empty** (uniform tag 2), whatever the in-memory default — see below |
| 14 | `compaction_style` | u8: 0 leveled, 1 FIFO |
| 15 | `fifo_max_bytes` | uvarint |
| 16 | `fifo_ttl` | uvarint ns |
| 17 | `compression_rules` | `count \| (prefix* \| codec u8)*` |
| 18 | `partition_rules` | `count \| (prefix* \| name*)*` |
| 19 | `tier_rules` | `count \| (prefix* \| tier* \| min_age ns uvarint)*` |
| 20 | `partition_scheme` (derived-scheme name) | UTF-8 |
| 21 | `target_file_size` | uvarint |
| 22 | `l1_base_bytes` | uvarint |
| 23 | `soft_pending_compaction_bytes` | uvarint |
| 24 | `hard_pending_compaction_bytes` | uvarint |
| 25 | `data_block_size` | uvarint, non-zero |
| 26 | `max_cached_vlog_value_bytes` | uvarint |
| 27 | `bloom_fpr_per_level` | f64 bits (u64 LE) per level, each in (0, 1) |
| 28 | `optimize_filters_for_hits` | bool |
| 29 | `periodic_compaction_interval` | uvarint ns |
| 30 | `enable_prefix_delta_keys` | bool |
| 31 | `block_restart_interval` | uvarint in [1, 1024] |
| 32 | `merge_operator_name` | UTF-8 |
| 33 | *reserved*: bloom auto-allocation (plan C P7) | — |
| 34 | `tombstone_density_trigger` | f64 bits (u64 LE), finite and ≥ 0 |
| 35 | `tombstone_density_min_entries` | uvarint |

Tag 13 is the one exception to "defaults elided": its default moved from the
empty list to `[None, LZ4, Zstd]` (plan C P10), so a blob written before the
change (tag absent) must keep meaning *empty*. Encoders therefore write tag 13
whenever the list is non-empty — the default included — and decoders read an
absent tag 13 as an empty list. The 0.9 legacy decoder applies the same rule.

Unknown and reserved tags are preserved **in tag order**, merged among the
known ones on re-encode — a reserved tag (33) sits below known tags (34, 35),
so appending it would produce a blob the decoder refuses as out of order.

## Edit-log op codes

| Code | Op | Code | Op |
| ---: | --- | ---: | --- |
| 1 | `AddTable` | 7 | `SetNextFileID` |
| 2 | `RemoveTable` | 8 | `SetGlobalSeq` |
| 3 | `UpdateTable` | 9 | `SetWalLayout` |
| 4 | `CreateCF` | 10 | `SetNonce` |
| 5 | `DropCF` | 11 | `SetCapability` |
| 6 | `SetCFConfig` | 12 | `RemoveTables` |

13–63 are unassigned (`Corruption` naming the op index); ≥ 64 never assigned.
`UpdateTable` field-mask bits: `0x01` level, `0x02` tier, `0x04` object, `0x08`
partition, `0x10` max_entry_time, `0x20` last_compaction_time, present values in
ascending bit order. Plan C step 2 row I merges this table with wavesdb's
18-op `WDME` table.

## Aux-block section tags

| Tag | Section |
| ---: | --- |
| 1 | range-delete fragments |
| 2.. | unassigned — `UnsupportedFormat` at open |
