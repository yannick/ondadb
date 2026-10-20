# wavesdb 1a052a4 → v0.8.6: feature inventory against ondaDB 0.9.0

Research date 2026-09-23. Read-only; nothing in either repo was modified.

- wavesdb range: `git log 1a052a4..v0.8.6`, 292 commits (2026-08-30 to 2026-09-17).
  Tags: v0.8.0 = eec88bf (08-31), v0.8.1 = bc98349, v0.8.2 = 8232492 (09-01), v0.8.3 = c3521db (09-15),
  v0.8.4 = b08d2d0 (09-16), v0.8.5 = 8eaa73a, v0.8.6 = a26fb4c (09-17).
  CHANGELOG covers 0.8.0–0.8.3. **v0.8.4–0.8.6 have no CHANGELOG entry** ("Unreleased: Nothing yet");
  they are three `objstore` commits, summarized here from their commit messages.
- ondaDB: 0.9.0 (`d07ee76`) + `411c0c7` (wavesdb number reservations) + `8afa066` (range-fragment cache
  port of wavesdb ea30eb9). ondaDB's 0.9.0 is itself "the wavesdb roadmap, phases 0-3", so most of
  wavesdb 0.8.0's features already exist in ondaDB. They were implemented independently, though, and
  are **not byte-compatible** (see §0).

Status legend: **present** / **partial** / **absent** / **rejected** (a recorded decision) / **deferred**
(recorded, not scheduled) / **n/a** (does not apply to ondaDB's architecture).
"Likely shared bug" means I read the code and the defect pattern is there, but I did **not** write a
repro test.

---

## 0. Headline for the binary-compatibility goal (reading wavesdb 0.8.6 checkpoints)

The shared registry (`docs/format-registry.md`) lines up **capability bit numbers, record-kind numbers,
entry-flag bits and codec ids**. The container formats that hold them do **not** match. ondaDB cannot
open any wavesdb table or catalog today, and that has nothing to do with the post-1a052a4 features:

| Surface | wavesdb 0.8.6 | ondaDB 0.9.0 | Consequence |
|---|---|---|---|
| SST footer magic | `0x5741564553535432` "WAVESST2" (`internal/sst/sst.go:80`; since 037ea41, 2026-07-14, tag v0.1.0) | `0x…5431` "WAVESST1" (`src/sst/mod.rs:43`), checked by equality (`src/sst/reader.rs:289`) → `Corruption` | **Every** wavesdb table is rejected at open |
| Footer flag bits | 0x01 bloom, 0x02 btree, 0x04 **Extended**, 0x08 **RangeBlock** (`sst.go:83-98`); per-table capability word in footer bytes 49..55 | 0x04 RESTARTS, 0x08 VLOG_V2, 0x10 EXTENDED_BLOCK, 0x20 PREFIX_DELTA (`src/sst/mod.rs:45-82`); bytes 49..56 unused | Same bit, different meaning. **Not in the registry.** |
| Extended/delta encoding | per-block: marker 0xff, block header, keyEncoding 1 = delta, restart trailer (`sst.go:24-37`) | table-level footer flag; entries carry the kind (`src/sst/mod.rs:58-82`) | Different layouts |
| Range-delete block | 16-byte handle before footer + flag 0x08 | 16-byte aux handle before footer + flag 0x10 | Different flag |
| Value log | shared **blob files** per CF, klog pointer `blobID u64 + blobOff u64`, blob-file format 2 with header/dict/group frames (`sst.go:21-55`) | per-SSTable vlog next to the klog | Every blob-bearing wavesdb table is unreadable without a blob-file reader |
| Bloom | xxh3, tagged (`internal/sst/bloom.go:54`) | xxh3 (`src/bloom.rs:66`) | Probably compatible; not verified bit for bit |
| Manifest | magic "WVMF", **versions 2/3/4**; v3 flag-word sections, v4 uvarint capability word, `sstFlagCaps`, `dbFlagEditLog` (`internal/manifest/manifest.go:24-100`) | magic "WVMF", **versions 1/2**; 8-byte tagged tails `ONDACAP1`, `ONDAAGE1`, `ONDARNG1`, `ONDAMED1` … (`src/manifest.rs:20-50`) | **Same magic, and version 2 means a different layout in each engine.** Neither can read the other's catalog |
| Edit log | magic `0x57444d45` "WDME" | `0x4F4E4445` "ONDE"; a "WD…" magic is deliberately rejected (`src/manifest_edit.rs:36-40, 2165`) | Separate on purpose |
| Object-store checkpoint | `<prefix>/MANIFEST` (wavesdb binary manifest) + `cf_<name>/<id>.klog` + `<id>.blob`; `CheckpointReceipt` with SHA-256 per object (`checkpoint_objstore.go:108-146`); `OpenRemoteCheckpoint` (`remote_checkpoint.go:26`) | none (ondaDB checkpoints are local only, `src/maintenance.rs:250`) | No reader |

So to read a wavesdb 0.8.6 checkpoint, ondaDB needs, at minimum: a wavesdb manifest v2/v3/v4 decoder
(including the CF-options blob, `UnifiedID`, managed-state and edit-log blocks), a WAVESST2 footer and
flag decoder, the per-block extended and prefix-delta decoders, a shared-blob-file reader (format 1 and 2,
and ZstdDict frames if codec 7 is used), the wavesdb range-block layout, and the checkpoint object-key
layout. The format registry's premise that "a mounting model … lets each engine read objects the other
wrote" does not hold at the SST level. The registry should **also** pin footer magic, footer flags,
manifest version numbers and blob-file format, or state explicitly that those surfaces are not shared.

A wavesdb checkpoint can carry these post-audit bytes, and each would have to be handled:
- manifest v4 plus `CapManifestEdits` (the checkpoint itself is a plain snapshot, but the bit is set);
- `CapPeriodicAge` (`sstFlagLastCompactionTime`);
- `CapRangeDeletes` range blocks;
- `CapPrefixDeltaBlocks` blocks;
- `CapMergeOperands` operands (the operator name is persisted, and ondaDB would need a same-named
  operator registered);
- **`CapManagedMode`** (a managed-state block in the manifest; its WAL kinds 32/33 do not reach a
  checkpoint, but the discard floor does);
- **`CapVlogGrouping`** (flag 0x08 group pointers, which ondaDB fails closed on by design);
- codecs 6/7/8 (reserved, and `from_u8` returns None).

`CapTxnDecisions` and the spill descriptor live only in the WAL, and "Checkpoint and Backup carry no WAL".
While a prepare is unresolved, wavesdb refuses to take a checkpoint at all.

---

## 1. Format-affecting features (landed after 1a052a4)

| wavesdb feature (commit) | Format change | ondaDB status | Evidence (ondaDB) |
|---|---|---|---|
| Manifest v4 + capability word + enable protocol (d7e3c92, 3482526, 5b00109) | manifest v4, footer capability word (bytes 49..55) | **present, different encoding** (manifest v2 + `ONDACAP1`; no footer capability word) | `src/manifest.rs:29,35`; `src/db.rs:2647` `enable_format_capabilities` |
| Strict decoders fail closed (234d6c7) | none (rejects invalid bytes) | **present** (1.0-A) | CHANGELOG 0.9.0 "Breaking changes"; `src/sst/mod.rs:88` `KNOWN_FOOTER_FLAGS` |
| Extended WAL/SST envelopes, kinds 1-5 (d7e3c92) | `CapExtendedRecords` bit 0 | **present** (WAL tag 0xFF matches; SST envelope differs, §0) | `src/format.rs:120`; `src/wal.rs:247` |
| Merge operators (6afb1c5) | `CapMergeOperands` bit 1, kind 4 | **present** | `src/config.rs:751`; `column_family.rs` `fold_point_chain` |
| Range deletes, all slices (5a5f664, f676e95, 13b4495, 435d610, 9169114) | `CapRangeDeletes` bit 2, kind 5, range block | **present** (different block flag/handle, §0) | `src/txn.rs:486` `delete_range`; `src/range_tombstone.rs` |
| Excise (208eb93, 165e409, 22a149a) | none (catalog edit) | **present** | `src/excise.rs` (re-plans under lock: `:296-307`; mount veto over span, all levels: `:169-190`) |
| Prefix-delta blocks (a549348) | `CapPrefixDeltaBlocks` bit 3 | **present** (table-level flag 0x20, not wavesdb's per-block key encoding) | `src/config.rs:668,683`; `src/sst/mod.rs:82` |
| Manifest edit log, local part (950bb3c, efbeb6e, 9e27aa9, b390cef) | `CapManifestEdits` bit 4, `MANIFEST-EDITS` | **present** (own magic "ONDE") | `src/manifest_edit.rs:40`; `DbInner::catalog_txn` |
| Edit log object-store publication (generation objects + CAS pointer `MANIFEST-CURRENT`) | pointer magic `0x57444d43` | **rejected** (engine has no object CAS; that is ayu's layer) | assessment §5; AGENTS.md "S3 tiering" |
| Periodic compaction + durable age (69829af) | `CapPeriodicAge` bit 5, `LastCompactionTime` per table | **present** | `src/config.rs:873`; `src/manifest.rs:38` `ONDAAGE1` |
| Two-phase commit (efb5268) | `CapTxnDecisions` bit 6, kinds 16-18 | **present** (unified only, like wavesdb) | `src/prepared.rs`; `DB::commit_prepared` |
| Managed sequence mode (05a3cff) | `CapManagedMode` bit 7, kinds 32/33, manifest `dbFlagManagedState` | **deferred** (bit/kinds reserved) | `src/format.rs:226-236`; plans/README "Deferred" |
| Large-txn spill (c3e22c1, 605139a, 057a29a, 63e1fc6, 6d6c40c) | kind 19 spill descriptor, private chunk files | **deferred** (kind reserved) | `src/format.rs:232` |
| Vlog compression grouping gated (87c329b) | `CapVlogGrouping` bit 8, entry flag 0x08 | **rejected / fail-closed** (bit, flag and codecs reserved to wavesdb) | `src/format.rs:30-31,228-231`; `docs/format-registry.md` |
| Rename/clear CF under unified layout via persisted family id (9e56f67) | manifest v3 `dbFlagCFUnifiedIDs`, `CFManifest.UnifiedID` | **absent**. clear is refused in unified mode; rename is a non-goal | `src/db.rs:2535-2541`; AGENTS.md non-goals |
| Wide-column entities (340cd87) | none (opaque value frame, a public spec) | **deferred** ("take wavesdb 4.2 as-is") | no `entity` API in `src/` |
| Per-level Bloom policy (a5bab39) | persisted CF options only | **present** (no `BloomAutoAllocate`) | `src/config.rs:792,815` |
| Compaction picker option (0f91435) | persisted CF option | **present as behaviour** (min-overlap always on; no option) | CHANGELOG 0.9.0 "Compaction and IO"; no `CompactionPicker` symbol in `src/` |
| MaxSubcompactions (716842b etc.) | persisted CF option | **present** (DB-wide `Options::max_subcompactions`) | `src/config.rs:398` |

All wavesdb options that are persisted per CF are encoded in wavesdb's CF-options blob. ondaDB's config
blob (`ONDA*` tails, `src/config.rs:1451-1540`) is a different encoding, and it would have to be
translated when a checkpoint is read.

## 2. API / runtime features

| wavesdb feature (commit) | ondaDB status | Evidence / note |
|---|---|---|
| PerfContext (6a21c8e) | **present** | `src/perf.rs:21` |
| MultiGet (e9a29b4, 79e6524) | **present**. I found no `MaxConcurrentBlockReads` / bounded-parallel-IO knob | `src/txn.rs:441,964` |
| Tailing iterator (eeb3ff4) | **present** | `src/tailing.rs:65` |
| Blob/vlog value cache (9b82302, 1ea9c6a) | **present** as a per-SST vlog cache | `src/config.rs:696` |
| Background IO limiter (7eed02a, 306c588) | **present** | `src/config.rs:345-351`; `src/ioctrl.rs` |
| Paced obsolete-file deletion (8cdb038) | **present** (0.6-B) | CHANGELOG 0.9.0 |
| Pessimistic locking (80c2224) | **partial**: point locks + wait-die only. No `LockRange`, no FIFO no-detection mode, and optimistic txns never probe the lock table | `src/txn.rs:288`; `src/txn_lock.rs:1-35` |
| DefaultIsolation made DB-wide (d789912) | **absent** (the per-CF field is documented "reserved"; `begin()` hard-codes Snapshot) | `src/config.rs:831-833`; `src/txn.rs:274-276` |
| Soft global memtable budget `MaxMemoryUsage` (ee8e1aa) | **rejected** | `src/config.rs:613` "reserved; currently ignored"; plans/README |
| Point reads stop early by table MaxSeq (5ef39df) | **absent** (perf only). `get` probes every candidate table | `src/column_family.rs:1681-1700, 1544-1571` |
| Background scans read through the block cache without admitting (ac16c8a) | **absent**. Compaction readers `put` every block | `src/sst/reader.rs:652,675` |
| O_DIRECT compaction reads (7289c76) | **absent** (spike-only in assessment §4) | no `O_DIRECT` in `src/` |
| Block-cache eviction indexed per file; namespaced identity at every open | **n/a / absent**. ondaDB never evicts retired tables' blocks (unique ids, so LRU ages them out); there is no namespace concept | `src/cache/` has only `file.rs:60 evict` |
| CF options immutable snapshot (7cc2f9c) | **n/a** (Rust ownership; hot reconfig is a non-goal) | AGENTS.md non-goals |
| UpdateConfig wakes the compactor on geometry change | **n/a** (no hot reconfig) | — |
| InjectReadLatency, bench harness phases, `wavesbench compare` | **absent** (tooling) | — |
| Demote parts to the default tier / off a store tier (4fa392c) | **absent (likely)**. `relocate_part` validates `Some(tier)`; the mover treats "ssd" as "no move" | `src/parts.rs:985-994, 1219-1221` |
| Periodic compaction burst bound (887f8ad) | **absent**. The picker loop drains every due table back-to-back | `src/compaction.rs:151` (`while let Some(..) = pick_compaction`); `src/db.rs:1253-1276` |
| Subcompactions stay single-span when inputs carry range tombstones (c94588d) | **different by design**: ondaDB re-fragments and clips fragments per span. I did not verify this against wavesdb's "drop rule quantified over job span" concern | `src/compaction.rs:1517-1553` |
| WAL failover 4.3 (2f24c16…9b120b8) | **deferred**. wavesdb has only an unwired prototype (`internal/walfailover`) | — |
| Tiered/lazy leveling 4.1, trace/replay 4.4 | dropped in wavesdb itself (406237c, 017d15b) | nothing to port |

## 3. Object-store / replication

| wavesdb feature (commit, release) | ondaDB status | Evidence / note |
|---|---|---|
| Object-store local **disk** block cache (`LocalCachePath`, `blockcache/disk`) (f28aecc) | **absent**. S3 reads are fronted only by the in-memory block cache | `src/storage_s3.rs` |
| Checkpoint of a materialized no-lock read-only DB (bc98349, v0.8.1) | ondaDB has no checkpoint-to-object-store. See the shared-bug entry in §4 | — |
| Verified S3 upload checksum receipts retained (e0ed123, v0.8.2) | **absent** (no checksums on PUT, no receipts) | no sha256/checksum in `src/storage_s3.rs` |
| S3 credential chain (env, shared file/`AWS_PROFILE`, IAM), live refresh, **session tokens**, `ReadOnly` (no bucket HEAD/create, local write refusal) (b08d2d0, v0.8.4) | **absent**. Static key pair only, and the session token is hard-coded `None` (the defect wavesdb fixed). No request is made at construction, so the "connecting is a write" half does not apply | `src/storage_s3.rs:169-176`; `src/config.rs:517-530` |
| `PrefixLister` (immediate child prefixes via delimiter, StartAfter pagination) (8eaa73a, v0.8.5) | **absent**. `list` sends delimiter "/" but keeps only `contents`, so common prefixes are discarded | `src/storage_s3.rs:361-381` |
| `Anonymous` (unsigned) and named `Profile` (a26fb4c, v0.8.6) | **absent** | same |
| Replicas, promotion, catalog mirror, monotonic replica word (5b00109) | **non-goal** | AGENTS.md "Read replicas" |

## 4. Fixes: does ondaDB have the same bug?

| wavesdb fix | ondaDB | Evidence |
|---|---|---|
| **Compaction input read error silently truncates the merge** (eacc833): an iterator that errors looks exhausted, the output is installed, and the inputs are deleted | **Likely shared, severe.** `SstIterator::valid()` is `valid && err.is_none()` (`src/sst/iter.rs:318-320`). `smallest_input` skips invalid iterators (`src/compaction.rs:1329-1334`). The merge loop ends with `flush_pending` + `outputs.finish()` without calling `.err()` on any input (`src/compaction.rs:1655-1658`). The only `.err()` calls in `compaction.rs` are in tests (`:3712`). A block CRC or IO failure mid-merge would drop the rest of that input, and the catalog edit would then retire it | code reading |
| **Compaction output cut inside one key's version chain** (fc32015). Two adjacent L≥1 tables both hold key K; the point read probes only the first; a snapshot read of an older version misses it | **Likely shared.** `CompactionOutputBuilder::write` sets `pending_cut` on size and honours it on the **next** write with no same-user-key check (`src/compaction.rs:1174-1201`). `VersionRetention` keeps every version above `oldest_snapshot` (`:887-905`), so multiple versions of K do reach `write`. `find_overlapping_at` returns the first table with `max_key >= K` (`src/column_family.rs:1427-1443`) | code reading |
| **DeleteRange does not conflict with a Serializable read set** (3591223) | **Likely shared.** `validate_read_conflicts` uses `peek_seq`, which reads only point versions (`src/txn.rs:1313-1322`; `src/column_family.rs:2091-2130`). `validate_span_conflicts` checks only the transaction's **writes** (`src/txn.rs:1333-1356`). Nobody tests read-k → concurrent `delete_range` over k → commit a write outside the span (`tests/range_delete.rs:264-290` writes inside the span) | code reading |
| **Read-only checkpoint drops WAL-only records** (v0.8.0 guard; v0.8.1 refined it) | **Likely shared.** A read-only open replays the WAL into memtables (`src/column_family.rs:772-773`) and spawns no workers (`src/db.rs:2156-2158`). `snapshot_to` calls `flush_memtable`, whose `flush_tx.send` fails when no receiver exists, so the job is silently dropped (`src/column_family.rs:1085-1098`). The checkpoint/backup then copies a catalog without those records (`src/maintenance.rs:260-270`). There is no read-only guard | code reading |
| Periodic backlog runs as one burst (887f8ad) | **Likely shared** (latency, not correctness) | `src/compaction.rs:151` |
| Merge chain folded across a range tombstone in point reads (9e525c0) | **not shared**. `fold_point_chain` receives `candidate.range_floor`, and the rule is pinned in `tests/composition.rs` | `src/column_family.rs:1699-1709` |
| Prepared DeleteRange reserved only its start key (b419355) | **n/a**. ondaDB refuses a range delete in a prepare | CHANGELOG 0.9.0 "Cross-feature rules" |
| Prepare ignored / released pessimistic locks (b335a0a) | **n/a by design**. Locks convert to reservations at prepare and waiters are woken with Conflict. An optimistic prepare can still take a key a pessimistic txn holds, and the holder then gets Conflict at commit. That is documented behaviour, not wavesdb's bug | `src/txn_lock.rs:1-6`; CHANGELOG 0.9.0 |
| Foreign mount's fragments justify an excise (22a149a); stale owners (165e409); mount veto below L0 | **not shared**. The veto uses `span_min/span_max`, which include fragments, over **all** levels, and `plan()` is re-run under the span lock | `src/excise.rs:169-190, 296-307` |
| Table MaxSeq ignored range fragments, so a checkpoint cut below its own tombstone | **not shared**. "`Reader::max_seq` covers fragments as well as entries" | `src/parts.rs:55-60`; `src/sst/writer.rs:726` |
| Blob-GC rewrite dropped the range block | **n/a** (no shared blob files / blob GC) | assessment §5 |
| ioctrl slept a stale duration (f2f3fe8) | **not shared**. The bucket re-checks in a loop with a bounded park and never sleeps against a pre-taken reservation | `src/ioctrl.rs:300-325` |
| Range fragments rebuilt per scan (ea30eb9, v0.8.3) | **already mirrored** | ondaDB `8afa066` |
| Capability word shrinking after a failed object publish | **n/a** (no catalog mirror) | — |
| Env list-valued config; wavesbench double flag registration | **n/a** (Go tooling) | — |
| DefaultIsolation persisted but ignored (d789912) | **same situation, but documented** as reserved in ondaDB | `src/config.rs:831-833` |

## 5. Branches

| Branch | Merged into main? | Notes |
|---|---|---|
| `feat/yolosearch-immutable-checkpoint` | **yes**. Tip bc98349 = v0.8.1 | post-audit; the read-only checkpoint fix |
| `origin/feat/blob-compression-dictionaries` | **yes**, pre-audit (tip bccf5b8, 08-20, ancestor of 1a052a4) | Codec 7 ZstdDict + blob-file format 2 header. Needed for checkpoint reading |
| `origin/feat/ondadb-parity-port` | **yes**, pre-audit (edb4987, 08-15) | — |
| `origin/feat/yolosearch-m0-storage` | **yes**, pre-audit (d4aa2b1, 08-15) | — |
| `origin/fix/ondadb-parity-audit` | **yes**, pre-audit (68ecbb7, 08-12) | — |
| `feat/subcompactions-2` | **no**. 6 commits ahead and 87 behind, forked at 20b465f | An alternative 0.8 implementation (`compaction_spans.go`, 1,181 lines; claims 3.08x at 4 spans). The merged `feat/subcompactions` (716842b…b6a0573) supersedes it. Runtime only, no format |
| `module-path-github` / `origin/module-path-github` | **no** (1 commit, 07-14, 407 behind) | Go module rename only |
| `origin-roadmap` | a remote with no listed branches | — |

## 6. wavesdb features that sit in ondaDB's non-goals or deferred lists

| Feature | ondaDB stance | Would the checkpoint-reading goal force a revisit? |
|---|---|---|
| Read replicas / promotion / catalog mirror | non-goal (AGENTS.md) | **No** for reading a static checkpoint. `OpenRemoteCheckpoint`'s behaviour (read-only, range GETs) is a subset of what ondaDB's S3 tier already does |
| Managed sequence mode (`CapManagedMode`, kinds 32/33) | deferred; bit and kinds reserved | **Yes, the read side only.** A managed wavesdb database's checkpoint has bit 7 set and a managed-state block in the manifest. ondaDB must at least parse and admit it (its reads are ordinary; the discard floor is a retention hint). Full managed-mode writes are not needed |
| Large-txn spill (kind 19) | deferred | **No.** WAL/private files only; never in a checkpoint |
| Vlog compression grouping (`CapVlogGrouping`, flag 0x08) | rejected, fail-closed | **Yes, if** the producing wavesdb sets `BlobCompressionGroupBytes`. Otherwise require it unset (wavesdb's own release notes recommend that) |
| Codecs 6/7/8 (LZ4Native, ZstdDict, Brotli) | reserved, fail-closed | **Yes, if** the producer uses them. ZstdDict needs the blob-file dictionary frame |
| Shared blob files / blob GC | "do not copy" (assessment §5) | **Yes, the read side.** Any wavesdb table with separated values points into `<id>.blob` files |
| Global memtable budget | rejected | No |
| Subcompactions | implemented in ondaDB (no longer a non-goal) | No |
| Rename / hot-reconfig of CFs; clear under unified | non-goal / refused | Only if checkpoints come from a wavesdb that renamed or cleared a family: the manifest v3 `UnifiedID` must be parsed. It does not matter for SST reads |
| Tiered/lazy leveling, trace/replay, io_uring, O_DIRECT, WAL failover | spike/deferred | No |
| Span locks for pessimistic txns | non-goal | No |

## 7. Suggested follow-ups (not done)

1. Write repro tests for the four likely-shared bugs in §4: compaction input error, version-chain cut,
   Serializable vs `delete_range`, and read-only checkpoint. The first two can lose or hide data.
2. Extend `docs/format-registry.md` to cover footer magic, footer flags, manifest version numbers, and
   the blob-file and range-block layouts, or state that the SST/manifest containers are not shared. As
   written, the registry implies mount interoperability that the magic check rules out.
3. If reading wavesdb checkpoints is a real goal, scope it as a separate "wavesdb import/reader" module:
   manifest v2-4 decoder, WAVESST2 reader, blob-file reader, and CF-options translation. Do not try to
   reach byte compatibility of ondaDB's own writer.
