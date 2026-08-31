# Cross-engine format registry

ondaDB and wavesdb are format siblings: the same WAL frame, the same SST block
frame, the same entry-flag byte, and a mounting model that lets each engine read
objects the other wrote (`attach_part_by_ref` here, mounts there). That makes
every assigned number a **shared** namespace, and the failure it can produce is
not a missing feature but a silent misread — the same mask meaning two different
things in two engines that mount each other's files.

This page is the registry. It is the answer to §7 of
[`wavesdb-feature-assessment.md`](wavesdb-feature-assessment.md), which asked for
one "before first write, not reconciled after". The reconciliation below is
*after* first write in one case, and that case is why the page exists.

The numbers here are also enforced, not merely documented: `src/format.rs`
carries `const _: () = assert!(...)` for each reservation, so an ondaDB constant
that collides with a wavesdb assignment fails the **build**. See
`format::wavesdb_reserved`.

## Entry flags (the legacy flags byte, and the extended modifier word)

| Bit | ondaDB | wavesdb | Status |
| --- | --- | --- | --- |
| `0x01` | `TOMBSTONE` | `FlagTombstone` | agreed |
| `0x02` | `HAS_TTL` | `FlagHasTTL` | agreed |
| `0x04` | `HAS_VLOG` | `FlagHasVlog` | agreed |
| `0x08` | *reserved to wavesdb* | `FlagVlogGrouped` — **live** | **resolved in wavesdb's favour** |
| `0x10` | `SINGLE_DELETE` | `FlagSingleDelete` | agreed |
| `0x20`–`0x80` | unassigned | unassigned | do not assign; use kinds |

### The `0x08` case, which is the reason for this page

ondaDB once named `0x08` `DELTA_SEQ`, an encoding **no writer ever produced**.
0.9.0 removed it and made the bit reserved-unknown, so ondaDB now rejects it.
wavesdb independently assigned the same bit to `FlagVlogGrouped` and *does*
write it: the value's 16-byte vlog pointer addresses a shared compression GROUP
frame rather than a frame of its own, with a uvarint in-group offset following.

The bit stays wavesdb's, for two reasons. ondaDB never wrote it, so no ondaDB
artifact carries it and nothing has to be migrated; and wavesdb writes it today,
so moving it would break files that already exist. ondaDB's rejection is the
**correct** behaviour and must stay — it cannot decompress a group it does not
implement, and failing closed beats misreading a pointer. What the reservation
adds is that ondaDB can never *reclaim* the bit and start decoding wavesdb's
grouped pointers as some future feature of its own.

wavesdb, for its side, gated the encoding behind capability bit 8
(`CapVlogGrouping`) so the incompatibility is declared in the manifest and a
reader learns it at **open** rather than mid-scan. A wavesdb database that never
groups declares nothing and stays readable here.

## Capability bits (manifest `ONDACAP1` / wavesdb manifest v4)

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

Bits 0–6 were assigned identically by both engines independently, which is the
part of the contract that worked. 7 and 8 are wavesdb-only features; ondaDB
reserves them so it cannot assign them to something else.

## Record kinds (extended envelope)

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
| > 63 | never assigned to anything, by either engine | — | — |

## Compression codec ids

Stored per block and per blob frame, so this byte is read from every file
either engine opens.

| Id | Codec | ondaDB | wavesdb |
| ---: | --- | --- | --- |
| 0–5 | none, snappy, lz4, zstd, lz4-fast, flate | yes | yes |
| 6 | LZ4 native block format | *reserved to wavesdb* | `LZ4Native` |
| 7 | zstd with a per-blob-file trained dictionary | *reserved to wavesdb* | `ZstdDict` |
| 8 | brotli | *reserved to wavesdb* | `Brotli` |

0–5 agree by independent assignment, as the capability bits did.
`Compression::from_u8` returns `None` for 6–8, so ondaDB already fails closed
on a wavesdb block using one — the entry here is to keep those numbers from
being handed to a different codec later, which is the failure that would not
fail closed.

The practical consequence is the same as for grouping: a database whose blocks
or blob frames use a wavesdb-only codec is not readable by ondaDB. Both engines
default to codecs in the shared range.

## The rule going forward

Assign from this page, then implement. A number that is written down here and
unimplemented costs nothing; a number implemented and not written down here is
the failure mode above. When either engine assigns a new bit or kind, add the
row and add the matching `const _: () = assert!(...)` reservation on the other
side.
