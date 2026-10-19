# Opening a wavesdb v0.8.6 checkpoint with ondaDB: catalog and directory layout

Scope: what ondaDB (Rust, `/Volumes/HOME/code/storage-engines/ondadb`, HEAD `0f4ebc6`) would need so it can open,
at least read-only, a directory or object prefix written by wavesdb v0.8.6
(`/Volumes/HOME/code/storage-engines/wavesdb`, tag `v0.8.6` = `a26fb4c`). This covers the **catalog** (MANIFEST,
MANIFEST-EDITS, per-CF config blob) and the **layout** (file and object names). The SST/blob byte formats are out of
scope. Some SST-level blockers surfaced along the way. They are listed in §0 because they decide whether catalog
work alone is enough.

All citations are `file:line`. wavesdb paths are relative to the wavesdb repo; ondaDB paths are relative to the ondadb repo.

---

## 0. Headline findings (read first)

1. **The CRC polynomials differ, so every checksum in both engines is incompatible.** This contradicts the docs.
   * wavesdb: `internal/encoding/encoding.go:15-19` uses `crc32.MakeTable(crc32.Castagnoli)` (CRC32-C), and has since the
     first commit `2fbd823`.
   * ondaDB: `src/encoding.rs:11-17` says "CRC32-C (Castagnoli)" in its doc comment, but the code is
     `crc32fast::Hasher`. crc32fast is **CRC-32/IEEE** (poly `0xEDB88320`; the crate's README says "CRC32 (IEEE)";
     `~/.cargo/registry/.../crc32fast-1.5.0/src/combine.rs:1`).
   * Verified empirically:
     * wavesdb fixture `internal/manifest/testdata/v4_caps.bin` has trailer `0x5ee55fb6`. That equals CRC32-C of the
       body; the IEEE value is `0xc09620de`.
     * All 8 ondaDB fixtures `tests/fixtures/phase1/manifest_v*.bin` carry the **IEEE** CRC.
   * Consequences:
     * The claims of byte-identical frames in `docs/format-registry.md` ("same WAL frame, the same SST block frame")
       and `docs/wavesdb-feature-assessment.md:14-15` are false at the checksum level.
     * `Manifest::decode` rejects every wavesdb MANIFEST at `verified_manifest_body` (`src/manifest.rs:536-545`)
       before looking at anything else. The same goes for every wavesdb SST block (`src/block.rs:54-61`), WAL frame,
       edit-log record and pointer.
   * Fix: add a CRC32-C implementation, e.g. the `crc32c` crate, or `crc-fast`, which is already in the registry
     cache. Choose the polynomial per artifact origin. Changing ondaDB's own polynomial would break every existing
     ondaDB database, so it is not an option.
   * Useful side effect: the polynomial is a free, reliable **engine discriminator** for a MANIFEST. Both engines use
     magic `WVMF`, and "version 2" means different things in each (see §1), so the CRC is what can tell them apart.
2. **The magic is the same, but the version numbers collide.** Both write magic `0x57564D46` "WVMF":
   * wavesdb: `internal/manifest/manifest.go:25`
   * ondaDB: `src/manifest.rs:20`

   The version words disagree:

   | Version | wavesdb | ondaDB |
   |---|---|---|
   | 1 | rejected | legacy |
   | 2 | body with blob refs | legacy body + `ONDACAP1`/`ONDAMED1` tags |
   | 3 | flag-word section | rejected |
   | 4 | v3 + capabilities | rejected |

   ondaDB accepts only 1 and 2 (`src/manifest.rs:560`). Even with the CRC fixed, a wavesdb **v2** manifest would get
   past ondaDB's version check and then be mis-parsed positionally, because the per-table blob map and per-CF blob list
   are not expected.
3. **Different per-CF directory name**: wavesdb uses `cf_<name>` (`db.go:2069`, `checkpoint_objstore.go:108-110`) and
   ondaDB uses `cf-<name>` (`src/db.rs:1424`, `src/storage.rs:288`, `src/maintenance.rs:289`).
4. **The CF config blob is JSON in wavesdb and a positional binary with `ONDA*` tails in ondaDB.** ondaDB's decoder is
   lenient ("unknown/short blobs fall back to defaults", `src/config.rs:1480-1486`):
   * It reads `{` (0x7B = 123) as a uvarint length and takes 123 bytes of JSON as the comparator name.
   * Every open path then fails with `InvalidArgs("unknown comparator {...")` (`src/db.rs:2284-2285`, `2364-2365`),
     which does fail closed.
   * A dedicated JSON decoder is needed.
5. **No per-table vlog in wavesdb 0.8.x (SST-level, out of scope but blocking):**
   * Large values go to CF-shared blob files `<cfdir>/<id>.blob` (`internal/sst/blob.go:17-29`).
   * The klog entry stores a **16-byte** pointer (blob id u64 + offset u64, plus an in-group uvarint when grouped;
     `internal/sst/sst.go:494-520`).
   * ondaDB stores an **8-byte** `vlog_off` into `<id>.vlog` beside the klog (`src/sst/mod.rs:16`, `366`,
     `174-180`).
   * The catalog side of this is the `BlobBytes` per table and `Blobs` per CF (see §1). The reader side is a new value
     resolver.
   * Also, by default wavesdb compresses deep levels with Zstd (`options.go:1600-1605`) and trains blob dictionaries
     (`DisableBlobDictionary` defaults to false). ondaDB rejects codec id 7 (`ZstdDict`) per
     `docs/format-registry.md` "Compression codec ids". So a default wavesdb database with large values is unreadable
     without zstd-dict support.
6. **What a local wavesdb 0.8.6 `Checkpoint` contains** (`maintenance.go:441-532`):
   * A flushed, **WAL-free** copy: `MANIFEST` plus `cf_<name>/<id>.klog` and `cf_<name>/<id>.blob`, hard-linked or
     copied.
   * **No `MANIFEST-EDITS`, no WAL segments, no LOCK file**, and the instance nonce is stripped.
   * `GlobalSeq` is lowered to the maximum table `MaxSeq`.
   * The manifest version follows the lowest-version rule:
     * **v2** by default;
     * **v3** if the database is unified-layout, partitioned, has tier rules, or has a CF whose unified ID is not the
       hash of its name (it was renamed or cleared);
     * **v4** if any capability is enabled.

   This is the *easiest* wavesdb artifact to open: no WAL replay and no edit log. Only MANIFEST decode, the config
   blob, directory names, CRC32-C, and SST/blob reading are needed.

---

## 1. Manifest (snapshot) encoding: field-by-field

### 1.1 wavesdb MANIFEST layout (all LE, varints = Go `binary.Uvarint`)

Source: `internal/manifest/manifest.go`.

```
magic u32 = 0x57564D46 "WVMF"                                          :25, :594
version u32 ∈ {2,3,4}   (lowest-version rule, Version() :431-439)       :595, :1012
NextFileID u64                                                          :596
GlobalSeq  u64                                                          :597
ncf uvarint                                                             :598
per CF:
  name   bytes(uvarint len)                                             :600
  config bytes(uvarint len)   -- JSON, see §5                           :601
  nsst uvarint
  per SST:
    ID, Level, NumEntries, NumTombstones, MaxSeq, KlogSize, VlogSize  (7 uvarints)  :604-610
    MinKey bytes, MaxKey bytes                                          :611-612
    nblob uvarint, then (blobID uvarint, rawBytes uvarint) sorted by id  :613-617   <-- v2 addition
  nblobs uvarint, then per blob: ID, Size, LiveBytes (uvarints)         :619-624   <-- v2 addition
[v3/v4 section, iff version>=3]                                         :626-628, appendV3 :646-733
crc32c u32 over everything above                                        :629-630
```

v3/v4 section (`appendV3` :646-733, `decodeV3` :741-957):

```
dbFlags uvarint  (bits: 0 WALLayoutUnified, 1 InstanceNonce, 2 CFUnifiedIDs,
                  3 Capabilities[v4], 4 ManagedState[v4], 5 EditLog[v4])   :62-88
[bit1] InstanceNonce u64 (fixed)                                        :670-672
[bit2] one u64 (fixed) UnifiedID per CF, in body order; 0 = derive from name   :677-681
[bit3] FormatCapabilities uvarint (nonzero; unknown bit -> UnsupportedError)   :682-684, :959-980
[bit4] DiscardSeq uvarint, LastManagedCommit uvarint (requires CapManagedMode) :685-688, :793-809
[bit5] SnapshotGeneration, AppliedThroughEditID, NextEditID (uvarints)         :689-695, :810-832
       (bit5 present  <=>  caps has CapManifestEdits; gen!=0; next >= applied+1)
per CF (same order): nsst uvarint (must equal body count -- cross-check)  :696-697, :839
  per SST: sstFlags uvarint (bits 0 Partition,1 Tier,2 Object,3 MaxEntryTime,
                             4 Caps[v4],5 LastCompactionTime[v4],6 Merge[v4],7 Range[v4])  :92-108
    [0] Partition bytes  [1] Tier bytes  [2] Object bytes  [3] MaxEntryTime uvarint(u64(int64))
    [4] Caps uvarint (subset of DB word)  [5] LastCompactionTime uvarint (!=0, needs CapPeriodicAge)
    [6] MergeCount, MergeChainMax uvarints
    [7] RangeCount(!=0), RangeMinSeq, RangeMaxSeq uvarints, RangeMinKey bytes, RangeMaxKey bytes
trailing bytes after section -> errBad                                  :947-955
```

The section uses strict flag masks. Under v3 the v4 bits are refused (`dbFlagV3Known`, `sstFlagV3Known` at :87, :108).

### 1.2 ondaDB MANIFEST layout

Source: `src/manifest.rs`.

```
magic u32 = 0x57564D46                                    :20, :409
version u32 ∈ {1,2}  (2 iff caps!=0 or edit tail)         :25-29, :412-419, :560
next_file_id u64, global_seq u64, ncf uvarint             :420-422
per CF: name bytes, config bytes, nsst, per SST:
   id, level, num_entries, num_tombstones, max_seq, klog_size, vlog_size (uvarints), min_key, max_key
   -- NO blob map, NO per-CF blob list                    :425-442, decode :600-622
positional tails (present iff anything later is present) :444-457, :624-635
   partition name-section, tier name-section, max_entry_time u64-section
   (name-section = per CF: count uvarint, then (table_index uvarint, bytes)*)   :750-791
tagged tails, 8-byte ASCII tags, any order, each at most once   :459-492, :667-738
   ONDAOBJ1  object name-section
   ONDAINS1  instance nonce u64
   ONDACAP1  caps u64 (fixed)
   ONDARNG1  range summary section                        :836-902
   ONDAAGE1  last_compaction_time u64-section
   ONDAMED1  generation u64, applied_through u64, next_edit_id u64 (fixed)
   ONDAWAL1  1 byte (1 = Unified)
crc32 (IEEE!) u32                                         :286-287, :536-545
```

Post-decode rules (:298-331):
* caps or edits require v2;
* `ONDAAGE1` requires `CAP_PERIODIC_AGE`;
* `ONDARNG1` requires `CAP_RANGE_DELETES`;
* `check_caps`;
* `next_edit_id == applied_through + 1` must hold exactly.

### 1.3 Field-by-field comparison

Key: **same** = same semantics and bytes are directly mappable; **diff** = same concept, different encoding or
semantics; **missing** = ondaDB has no field or concept.

| Field | wavesdb (enc / cite) | ondaDB (enc / cite) | Status / what ondaDB needs |
|---|---|---|---|
| Magic | `WVMF` u32 LE :25 | `WVMF` u32 LE :20 | **same** (collision, so it cannot be used to tell the engines apart) |
| Checksum | CRC32-C trailer over body, `encoding.go:15-19`, `manifest.go:629, 998` | CRC32 **IEEE** trailer, `encoding.rs:13-17`, `manifest.rs:541` | **diff**: need CRC32-C. Also usable as the engine discriminator |
| Version | 2/3/4, strict equality, unknown → `UnsupportedError` (:1012-1017) | 1/2 (:560); unknown → Corruption | **diff**: needs a wavesdb-mode decoder for v2/v3/v4. Note: wavesdb v2 ≠ ondaDB v2 |
| NextFileID | u64 fixed (:596) | u64 (:420) | **same**. The wavesdb value also covers blob ids (blob ids come from the same allocator, `unified_flush.go:330` `sst.NewBlobWriterWith(..., cf.db.nextID, ...)`) |
| GlobalSeq | u64 (:597). Checkpoint lowers it to max `MaxSeq` (`maintenance.go:593-611`). Managed mode: highest admitted commit | u64 (:421). Open sets `visible = global_seq`, `next = global_seq+1` (`db.rs:1798-1801`) | **same** |
| CF count / order | uvarint; order = map iteration (Checkpoint) or name-sorted (`checkpoint_objstore.go:164`) | uvarint | **same** |
| CF name | bytes | bytes, UTF-8 checked (:584) | **same**. Both cap names at 128 (`options.go:228`, `db.rs:27`) |
| CF config | opaque JSON (§5) | opaque positional binary + `ONDA*` tails | **diff**: needs a JSON → `ColumnFamilyConfig` mapper |
| SST id, level, num_entries, num_tombstones, max_seq, klog_size, vlog_size | 7 uvarints | 7 uvarints | **same** layout. `VlogSize` in wavesdb = raw bytes the table references in blob files (`internal/sst/writer.go:751`), not a vlog file size. ondaDB `snapshot_to` skips the vlog when size is 0 (`maintenance.rs:300-302`) but would otherwise look for `<id>.vlog` |
| min_key / max_key | bytes | bytes | **same** |
| SST `BlobBytes` map | uvarint n + (id, bytes)* inside the body (:613-617) | — | **missing**: must parse (or skip) and keep it for blob resolution |
| CF `Blobs` list (ID, Size, LiveBytes) | per CF after the tables (:619-624) | — | **missing**: needed to know which `.blob` files exist (Restore uses it: `restore_objstore.go:49-57`) |
| Partition | v3 sstFlag bit 0, bytes | positional name-section #1 (`Option<String>`) | **diff** encoding, same meaning. Empty = default in wavesdb, `None` in ondaDB |
| Tier | sstFlag bit 1 | positional name-section #2 | **diff** encoding. A local checkpoint never carries one (refused, `maintenance.go:36-52`, :461-465). An object-store checkpoint may (§3.3) |
| Object stem | sstFlag bit 2, e.g. `cf_<cf>/<nonce:016x>-<id>` (`nonce.go:96`) | `ONDAOBJ1` name-section, `cf-<cf>/<nonce:016x>-<id>` (`parts.rs:1052`) | **diff**: encoding and `cf_` vs `cf-` |
| MaxEntryTime | sstFlag bit 3, uvarint(u64(int64)) | positional u64-section | **diff** encoding. wavesdb stamps it only for partitioned CFs (`flush.go:355-366`) |
| Table Caps | sstFlag bit 4 (v4), uvarint, ⊆ DB word | — | **missing**: can be ignored for reading. Must stay ⊆ the DB word |
| LastCompactionTime | sstFlag bit 5 (v4), needs `CapPeriodicAge` | `ONDAAGE1` u64-section, needs `CAP_PERIODIC_AGE` | **diff** encoding, same meaning and gate |
| MergeCount / MergeChainMax | sstFlag bit 6 (v4) | — | **missing**: advisory (the picker uses it). Ignorable for read-only |
| Range summary (count, min/max seq, min/max key) | sstFlag bit 7 (v4) | `ONDARNG1` section | **diff** encoding, **same** fields and validity rules (count≠0, keys non-empty, lo≤hi: :917-944 vs :890) |
| WAL layout | dbFlag bit 0 (v3) | `ONDAWAL1` tag, 1 byte | **diff** encoding, same meaning. ondaDB validates against `opts.unified_memtable` (`db.rs:2224-2225`) |
| Instance nonce | dbFlag bit 1, u64 fixed | `ONDAINS1` u64 | **diff** encoding. Stripped from wavesdb checkpoints (`db.go:1798-1800`) |
| Per-CF UnifiedID | dbFlag bit 2: one u64 per CF, 0 = derive `fnv1a64(name)` (`unified.go:53-60`) | — (always `cf_id(name)` = FNV-1a64, `unified.rs:27-28`) | **missing**. Same hash function. Only matters for WAL/memtable prefixes (SST keys are unprefixed, `unified_flush.go:300-330`), so a WAL-free checkpoint can ignore it. Reading a live wavesdb unified WAL after a rename/clear would need it |
| Format capability word | dbFlag bit 3 (v4), **uvarint** | `ONDACAP1` **u64 fixed** | **diff** encoding. Bits 0-6 are identical (`docs/format-registry.md`). Bits 7 (`CapManagedMode`) and 8 (`CapVlogGrouping`) are reserved in ondaDB and rejected by `check_caps` (`format.rs:155-162`, `228-231`) → `UnsupportedFormat` |
| Managed state (DiscardSeq, LastManagedCommit) | dbFlag bit 4 (v4) | — | **missing**. Only present with bit 7, which ondaDB refuses anyway. A read-only reader could honour `DiscardSeq` as a history floor |
| Edit-log watermark | dbFlag bit 5 (v4), 3 **uvarints**, `next >= applied+1` (:829) | `ONDAMED1`, 3 **u64**, `next == applied+1` (:329) | **diff** encoding and a stricter rule. A wavesdb checkpoint always writes the "fresh" state (`fillEditWatermark` :393-403 via :690-691), so equality holds. A live wavesdb DB can have `next > applied+1` while its log holds the rest |
| Trailing-bytes policy | error (:947-955) | tagged loop errors on an unknown tag (:733-734) | same philosophy |

**Implementation shape.** Add `manifest_wavesdb.rs`, a pure decoder for WVMF v2/v3/v4 with CRC32-C that produces
ondaDB's `Manifest`, plus side tables for blob metadata. Dispatch on the CRC polynomial, or try the ondaDB decode
first and fall back on a CRC mismatch. Read-only means no encoder is needed. Effort: **~2-3 days** including golden
tests against `internal/manifest/testdata/v4_caps.bin`, plus fixtures generated by wavesdb for v2 and v3.

### 1.4 MANIFEST-EDITS

This does **not** apply to a checkpoint: neither engine writes an edit log into a checkpoint.
* wavesdb: `Checkpoint` only `man.Save(...)`s (`maintenance.go:531`).
* ondaDB: `snapshot_to` resets the watermark (`maintenance.rs:314-317`).

It matters only for opening a *live* wavesdb dir or a primary object-store mirror.

| | wavesdb (`internal/manifest/editlog.go`, `edit.go`) | ondaDB (`src/manifest_edit.rs`) |
|---|---|---|
| Name | `MANIFEST-EDITS` (:38) | `MANIFEST-EDITS` (:53-55) |
| Header | 25 B: magic u32 `WDME` 0x57444d45, **u8** schema=1, u64 BaseAppliedThrough, u64 SnapshotGeneration, crc32c u32 over 0..21 (:21-26, :50-57) | 28 B: magic u32 `ONDE` 0x4F4E4445, **u32** schema=1, u64 base_applied_through, u64 snapshot_generation, crc32(IEEE) over 0..24 (:41-45, :676-719) |
| Record frame | u32 len(id+payload), u32 crc32c(id+payload), u64 id, payload (:27-31, :77-86) | u32 len(payload), u32 crc(payload), payload = u64 id + … (:390-411) → **the same frame shape** |
| Payload | uvarint opCount, then ops. **Canonical-form check**: re-encode must match the input bytes (edit.go:179-186, :403-420) | uvarint op_count, then ops |
| Op code | 1 **byte** (edit.go:189) | **uvarint** (the same bytes for codes <128) |
| Op numbering | 1 AddTable, 2 RemoveTable, 3 UpdateTable, 4 AddBlob, 5 UpdateBlob, 6 RemoveBlob, 7 CreateCF, 8 DropCF, 9 RenameCF, 10 SetCFConfig, 11 SetNextFileID, 12 SetGlobalSeq, 13 SetWALLayout, 14 SetNonce, 15 SetCapability, 16 SetManagedState, 17-18 reserved (edit.go:35-64) | 1 AddTable, 2 RemoveTable, 3 UpdateTable, 4 CreateCf, 5 DropCf, 6 SetCfConfig, 7 SetNextFileId, 8 SetGlobalSeq, 9 SetWalLayout, 10 SetNonce, 11 SetCapability, 12 RemoveTables (:71-86) → **only 1-3 agree** |
| Table encoding in AddTable | v2 body fields + blob map + v3-style sstFlags word + flagged fields (edit.go:255-298) | fixed field order with presence bytes, range fields unconditional (:267-303) → **diff** |
| UpdateTable mask | 22 bits (Level=0…RangeMaxKey=21, edit.go:110-127) | 6 bits (LEVEL 1, TIER 2, OBJECT 4, PARTITION 8, MAX_ENTRY_TIME 0x10, LCT 0x20, :90-99) → **diff** |
| SetNonce / SetCapability value | uvarint | u64 fixed |
| CF ops | CreateCF carries mask + optional UnifiedID; RenameCF exists | no rename, no unified id |

Supporting it would need a separate wavesdb edit decoder and applier: Add/Update/Remove Blob, RenameCF, UnifiedID and
ManagedState ops, plus wavesdb's recovery predicate (`internal/manifest/recover.go`). Effort: **~3-4 days**. Not
needed for checkpoints.

Object-store publication under `CapManifestEdits` (live primary mirror only, `internal/manifest/pointer.go:11-33`):

```
MANIFEST-CURRENT                    91-byte pointer: u32 "WDMC" 0x57444d43 | u8 schema 1 | u64 generation |
                                    u64 appliedThrough | 32B sha256(snapshot) | 32B sha256(edits) | crc32c u32
manifests/<generation>/snapshot     snapshot bytes
manifests/<generation>/edits-<through>   edit-log bytes through <through>
```

Without the edit log, the primary mirror publishes a single `MANIFEST` key by CAS or Put
(`objstore_mode.go:257-313`). ondaDB has no equivalent; by design it keeps its manifest local-only (AGENTS.md).

---

## 2. Directory layout

| Artifact | wavesdb v0.8.6 | ondaDB | Same? |
|---|---|---|---|
| Catalog snapshot | `<db>/MANIFEST` (`manifest.go:475`), temp `MANIFEST.tmp` (:532) | `<db>/MANIFEST` (`manifest.rs:238-240`), temp `MANIFEST.tmp` (`with_extension("tmp")`, :257) | same |
| Edit log | `<db>/MANIFEST-EDITS` (`editlog.go:38`) | `<db>/MANIFEST-EDITS`, `.tmp` (`manifest_edit.rs:53-61`) | name same, bytes differ |
| CURRENT file | none | none | — |
| LOCK | `<db>/LOCK`, `flock` EX/SH NB (`lock_unix.go:19-35`) | `<db>/LOCK`, `File::try_lock`/`try_lock_shared` (flock on unix) (`db.rs:3264-3284`) | same, and the two interoperate. ondaDB creates `LOCK` even on a read-only open (`db.rs:2206, 2214`), so the checkpoint dir must be writable, or a no-lock mode is needed |
| Per-CF dir | `<db>/cf_<name>` (`db.go:2069`) | `<db>/cf-<name>` (`db.rs:1424`) | **differs** |
| Klog | `cf_<name>/<id>.klog` (`columnfamily.go:351`) | `cf-<name>/<id>.klog` (`column_family.rs:549`, :709) | differs through the dir only |
| Value log | **none per table**. CF-shared `cf_<name>/<blobID>.blob` (`internal/sst/blob.go:17-29`). On a shared tier: `<root>/<objectStem>-<blobID>.blob` (`tier.go:440-475`) | per table `cf-<name>/<id>.vlog` (`sst/mod.rs:174-180`) | **differs** (different model) |
| Per-CF WAL | `cf_<name>/wal-<gen>.log` (`columnfamily.go:346`) | `cf-<name>/wal-<gen>.log` (`column_family.rs:545`, :742) | differs through the dir; frame CRC also differs |
| Unified WAL | `<db>/unified-wal-<gen>.log` (`unified.go:257-260`) | `<db>/unified-wal-<gen>.log` (`unified.rs:198`) | name same |
| Tier (unshared) | `<tierRoot>/cf_<name>/<id>.klog` (`tier.go:399`) | `<tierRoot>/cf-<name>/<id>.klog` (`storage.rs:288`, `column_family.rs:713-715`) | differs through the dir |
| Tier (shared, object stem) | `<tierRoot>/<object>.klog`, object = `cf_<cf>/<nonce:016x>-<id>` (`tier.go:397`, `nonce.go:96`) | `<tierRoot>/<object>.klog`, object = `cf-<cf>/<nonce:016x>-<id>` (`column_family.rs:711`, `parts.rs:1052`) | the resolution rule is **the same** (`root/object.klog`); only the stem text differs. ondaDB would resolve a wavesdb stem verbatim, since the stem is data |
| Detached part | `cf_<name>/detached/<partition>/` (`parts.go:30, 219`) | `cf-<name>/detached/<partition>/` (`parts.rs:403`) | differs through the dir |
| Frozen part | `<dir>/cf_<name>/<id>.klog` + MANIFEST (`parts_freeze.go:417, 445`) | `<dir>/cf-<name>/…` (`parts.rs:861`) | differs through the dir |
| Other | `txnspill`, `walfailover` (optional) | — | ignorable for a checkpoint |

The layout fix is small. Make the CF dir name come from a function of the catalog origin: `cf_` for wavesdb catalogs,
`cf-` for ondaDB. It is built in about six places (`db.rs:1424`, `storage.rs:288`, `maintenance.rs:289`,
`parts.rs:403/861/1052`). Effort: **~0.5 day**. Opening a checkpoint needs only the read path.

---

## 3. wavesdb checkpoints

### 3.1 Local `Checkpoint(dir)` (`maintenance.go:441-532`)

1. `checkpointSourceReady`, :547-585. A writable DB is admitted. A read-only DB is admitted only if it was opened with
   `OpenReadOnlyNoLock` (`immutableNoLock`) **and** every memtable, both the per-CF and the unified one, is empty
   (nothing was replayed from the WAL). Everything else is refused with `ErrReadOnly`. This is what `bc98349`
   ("checkpoint materialized immutable databases", in v0.8.1+) added. Before it, every read-only source was refused.
2. Further refusals:
   * **unresolved prepared transactions** (`refuseUnresolvedPrepares` :68, called at :458);
   * **any table on a non-default tier** (`refuseTieredFamily` :36-52, :461-465).
3. **Flush** every CF (writable only, :467-473). **No WAL segments are copied.**
4. `pauseDeletions` for the whole copy (:482).
5. Build the header with `manifestHeaderLocked(forCopy=true)` (`db.go:1764-1802`). It carries:
   * `NextFileID`;
   * `GlobalSeq = visibleSeqForManifest()`;
   * `WALLayoutUnified`;
   * `FormatCapabilities`;
   * managed `DiscardSeq` / `LastManagedCommit`.

   It strips the `InstanceNonce`.
6. Per live CF:
   * `mkdir cf_<name>`;
   * hard-link or copy (`linkOrCopy`) every `<id>.klog`, plus every blob file in the CF's blob tracker, as
     `<id>.blob`;
   * fsync the dir;
   * `addCFToManifestLocked` under the same `cf.mu` (:490-527). This writes the JSON config, the tables (with Caps,
     merge and range stats, LastCompactionTime), `Blobs`, and the UnifiedID when it is not the name hash
     (`columnfamily.go:953-971`).
7. `stampCutSeq`: `GlobalSeq = min(GlobalSeq, max MaxSeq)` (:593-611).
8. `man.Save(dir/MANIFEST)`: temp file, fsync, rename, dir fsync. Under `CapManifestEdits` the snapshot carries the
   **fresh** watermark (gen≥1, next=applied+1) and **no MANIFEST-EDITS file is written**.

The resulting directory:

```
<ckpt>/MANIFEST                 (WVMF v2 | v3 | v4, crc32c)
<ckpt>/cf_<name>/<id>.klog      (hard links)
<ckpt>/cf_<name>/<blobID>.blob
```

Manifest version in practice:
* **v2** when the CFs are plain, per-CF WAL, no capabilities. This is the 0.8.x default: `ManifestEditLog` and
  `UnifiedMemtable` both default to false (`options.go:442, 664`).
* **v3** with unified layout, partition rules/scheme or tier rules (`partition.go:257-259` → `ForceV3`), or a
  renamed/cleared CF.
* **v4** as soon as any capability is on: merge operator, range delete, prefix delta, edit log, periodic age, 2PC,
  managed mode, or vlog grouping.

`Backup(dir)` (:613+) differs: it copies bytes, and on a read-only source it copies live WALs
(`cf_<name>/wal-<g>.log`, `unified-wal-<g>.log`, :660-720). So a *backup* can need WAL replay; a *checkpoint* never
does.

### 3.2 Branch `feat/yolosearch-immutable-checkpoint`

* `git log main..feat/yolosearch-immutable-checkpoint` is **empty**. The branch head is `bc98349` itself, which is an
  ancestor of `main` (tags v0.8.1-v0.8.6 all contain it). The worktree `.worktrees/yolosearch-v081` sits at `bc98349`
  with a clean `git status`.
* So the branch **adds nothing** beyond what 0.8.6 already has.
* `bc98349` itself (+225/-21 across `CHANGELOG.md`, `checkpoint_objstore.go`, `checkpoint_readonly_test.go`,
  `maintenance.go`) replaced the flat `if db.opts.ReadOnly { refuse }` in both `Checkpoint` and
  `checkpointToObjectStore` with `checkpointSourceReady`. The point was to publish "fully materialized immutable
  database trees" opened with `OpenReadOnlyNoLock`.

### 3.3 `CheckpointToObjectStore` / `…WithReceipts` (`checkpoint_objstore.go:123-292`)

Object keys (`objKey` :108-110, `path.Join(prefix, "cf_"+cf, "<id>.<ext>")`):

```
<keyPrefix>/MANIFEST                  same WVMF v2|v3|v4 bytes as a local checkpoint (staged via temp file, :268-283)
<keyPrefix>/cf_<cf>/<id>.klog
<keyPrefix>/cf_<cf>/<blobID>.blob
```

With `objstore.S3`, `S3Config.Prefix` is prepended as `prefix + "/" + key` (`objstore/s3.go:185-189`).

Behaviour:
* There are **no pointer or generation objects** for checkpoints. `MANIFEST-CURRENT` and `manifests/<gen>/…` belong
  only to the live primary mirror with the edit log (§1.4).
* The MANIFEST is uploaded **last**, after every table and blob (:262-283), so its presence is the commit marker.
* The source rules are the same as for `Checkpoint`, including the flush and `checkpointSourceReady`. But it does
  **not** call `refuseTieredFamily` or `refuseUnresolvedPrepares`. So an object checkpoint's MANIFEST can still carry
  `Tier`/`Object` stamps (and the upload of a tiered blob would likely fail). A reader should overwrite or ignore
  Tier/Object, as `OpenRemoteCheckpoint` does (`remote_checkpoint.go:98-101`).
* Incrementals (`parent != nil`) reuse the prefix. The MANIFEST is rewritten with the cumulative table set, and
  objects already in `parent` are skipped (:210-253). Only the latest checkpoint per prefix can be restored.
* `WithReceipts`:
  * requires a `ConditionalObjectStore`;
  * uses create-if-absent `Create`, and accepts an existing object only if its size and sha256 match (:356-446);
  * returns per-object `{Key, Size, SHA256, StoreChecksum, StoreVerified}`, sorted MANIFEST first (:457-505);
  * forbids a `parent`;
  * uses 4 concurrent uploads (:24).
* The public `CheckpointManifest` return value (GlobalSeq, NextFileID, Tables{CF, ID, MaxSeq, KlogSize, VlogSize,
  BlobIDs}) is **not** persisted in the store. It exists for the caller's own backup catalog.

`RestoreFromObjectStore(ctx, store, keyPrefix, dir)` (`restore_objstore.go:20-60`):
* downloads `MANIFEST`;
* `manifest.Load`s it;
* downloads each `cf_<cf>/<id>.klog` and each `cf.Blobs` entry to `dir/cf_<cf>/…`.

The result is an ordinary local checkpoint dir.

`OpenRemoteCheckpoint(ctx, opts{ReadOnly}, RemoteCheckpoint{Store, KeyPrefix})` (`remote_checkpoint.go:26-120`) is a
lazy open:
1. Download the MANIFEST only.
2. Create empty `cf_<name>` dirs.
3. Rewrite every table's `Tier = "__wavesdb_remote_checkpoint__"` and `Object = ""`, and save the local MANIFEST.
4. Register a shared tier `{Root: prefix, Store}`.
5. Normal `Open`.

Tables then resolve to `<prefix>/cf_<name>/<id>.klog` and blobs to `<prefix>/cf_<name>/<blobID>.blob` through
`tierLocationFor` / `tierBlobLocationFor` (`tier.go:385-399, 440-475`). The store must implement `RangeGetter`.
Commit `c148bc0` "remote: trust manifest table sizes" avoids a HEAD per table.

### 3.4 objstore changes in v0.8.3..v0.8.6 (`git log -p c3521db..a26fb4c`)

Files touched: `objstore.go`, `objstore/s3.go`, `objstore/prefixlist.go` (new), `objstore/contract/prefix.go` (new),
and tests. +1318/-13.

* **v0.8.4 `b08d2d0`: credential chain + read-only.**
  * `S3Config` gains `SessionToken` (`objstore/s3.go:57`) and `ReadOnly` (:65).
  * `s3Credentials` (:115):
    * with an explicit key pair, it passes them through as static V4 credentials together with the session token.
      Before this, the token was hardcoded `""`, which broke SSO, IRSA, EC2/ECS role and assumed-role credentials;
    * otherwise it builds a live `NewChainCredentials` over EnvAWS, FileAWSCredentials (honours `AWS_PROFILE`) and
      IAM, with a 5 s metadata timeout.
  * `ReadOnly` skips the `BucketExists` probe and the bucket create (a 403 for a GetObject/ListBucket-only
    principal).
  * `Put`, `Delete`, `Create` and `CompareAndSwap` fail locally with `objstore.ErrReadOnly` (:44).
  * An empty region is passed through unchanged.
* **v0.8.5 `8eaa73a`: delimiter listing.**
  * New optional capability `PrefixLister.ListPrefixes(ctx, prefix, token, limit) (PrefixPage{Prefixes, NextToken})`
    (`objstore/contract/prefix.go`; aliased in `objstore.go`). It lists the children one level down, with delimiter
    "/", and is implemented for FS and S3 (`objstore/prefixlist.go:68, 141`).
  * The continuation token is the last common prefix with its trailing `/` replaced by `0`, used as `StartAfter`,
    because minio-go exposes no continuation token.
  * It cancels its own context so the minio producer goroutine does not leak.
  * `List` is documented as recursive.
* **v0.8.6 `a26fb4c`: anonymous signing + named profile.**
  * `S3Config.Anonymous` (:79) produces `credentials.NewStatic("","","",SignatureAnonymous)`, is checked first, and
    beats everything else.
  * `S3Config.Profile` (:70) produces `NewFileAWSCredentials("", profile)` and does **not** fall through to the chain.
  * An explicit key pair still beats both.

Relevance: a consumer reading someone else's published checkpoint bucket needs exactly these (read-only, no bucket
probe, anonymous, STS or profile credentials, cheap listing of checkpoint prefixes). ondaDB's S3 backend has none of
them today (§4).

---

## 4. What ondaDB can reuse to read a wavesdb object-store checkpoint

ondaDB side:
* `DB::checkpoint` / `backup` (`src/maintenance.rs:250-321`) → `snapshot_to`:
  * flushes;
  * uses `recover_catalog`;
  * links or copies `cf-<name>/<id>.klog`, plus `.vlog` when `vlog_size > 0`;
  * clears tier/object;
  * writes a snapshot-only MANIFEST (gen 1, applied 0, next 1).

  It keeps the instance nonce. This is the mirror image of wavesdb's `Checkpoint` and suggests nothing new.
* There is **no object-store checkpoint or restore in ondaDB**.
* `S3Storage` (`src/storage_s3.rs`, feature `s3`, rust-s3 0.35 + private tokio runtime):
  * `open_read` gives a lazy handle; `read_exact_at` = one ranged GET with retry (:199-240); `size()` = one HEAD
    (:242-257);
  * `create` = buffered single PUT;
  * `list(dir)` = delimiter "/" listing of *object keys* (:360-380; common prefixes are ignored);
  * `rename` = copy + delete;
  * no mmap.

**Directly reusable.** The whole remote *read* path:
* `TierRegistry` + `S3Storage` + table cache + block cache.
* ondaDB's per-table resolution rule `(Some(tier), Some(object)) → {root}/{object}.klog` (`column_family.rs:706-716`)
  already reproduces wavesdb's shared-tier rule, so a wavesdb object checkpoint can be mounted with **no directory
  renames**:
  1. Download `<prefix>/MANIFEST` (one GET).
  2. Decode it with the wavesdb decoder (§1).
  3. For each table set `tier = Some("<remote>")` and `object = Some(format!("cf_{cf}/{id}"))`.
  4. Register `TierDef { name: "<remote>", root: "<prefix>", backend: S3(..), shared: true }`.
  5. Build the CFs.

  This is the `OpenRemoteCheckpoint` pattern. The alternative is to set `object = None` and fix `cf_dir` to use `cf_`.

**Must add:**
1. The S3 auth surface wavesdb gained in 0.8.4-0.8.6. `S3Config` (`src/config.rs:517-530`) has only static
   `access_key`/`secret_key` and passes `None` for the session token (`storage_s3.rs:165-172`). rust-s3 already offers
   `Credentials::anonymous()`, `from_env`, `from_profile(Some(name))`, `from_sts_env` and `from_instance_metadata`,
   and the session-token slot. Then:
   * add a `read_only` flag. `S3Storage::new` already makes no network call, so that part is fine; the missing piece
     is refusing writes locally;
   * **HEAD avoidance**: seed `size` from the manifest's `klog_size`, like wavesdb's `c148bc0`.

   Effort **~1 day**.
2. A blob-file reader for `cf_<cf>/<blobID>.blob`. The table's blob map and the CF blob list come from the MANIFEST.
   The value pointer is `(blobID, offset[, inGroupOff])`, and the frames use wavesdb framing and codecs (zstd-dict,
   grouping). This is SST/blob scope, the biggest item: **~1-2 weeks** depending on dict/group support.
3. A download-based restore (`RestoreFromObjectStore` equivalent), if a lazy mount is not wanted: GET MANIFEST, then
   GET each klog and each blob into a local `cf_<cf>/…` dir. With `S3Storage` + `LocalStorage` this is **~0.5 day**
   (plus the dir-name fix).
4. Optionally, a delimiter child-prefix lister to discover checkpoint prefixes (wavesdb `PrefixLister`). rust-s3's
   `list(prefix, Some("/"))` already returns `common_prefixes`; they are just dropped at `storage_s3.rs:370-378`.
   **~0.25 day**.

---

## 5. Per-CF config blob

### 5.1 wavesdb: JSON (`config.go:8-65`, marshal :179-247, unmarshal :249-317)

`encoding/json`: unknown fields are ignored, `[]byte` values are base64, durations are int64 **nanoseconds**.

| JSON key | Meaning | ondaDB counterpart (binary blob, `src/config.rs:1451-1465`) | Mapping note |
|---|---|---|---|
| `comparator` | comparator name. Unknown → **silently bytewise** (:254-257) | `comparator_name` (base, uvarint-len string) | Names are identical in both: `memcmp`, `reverse`, `lexicographic`, `uint64`, `int64`, `case_insensitive` (`comparator.go:33-57, 130`; `comparator.rs:174-184`). ondaDB errors on unknown, which is the correct fail-closed choice |
| `merge_operator` | stable operator name, resolved against `Options.MergeOperators` | `ONDAMRG1` tail → `merge_operator_name`, resolved by `resolve_merge_operator` | Names are user-chosen and must be registered in ondaDB under the same string. Required to read a CF holding merge operands |
| `compression` (u8), `compression_per_level` ([]u8), `compression_rules` ([{prefix b64, compression}]) | codecs | base byte, per-level count+bytes, rules | ids 0-5 identical, 6-8 wavesdb-only (`Compression::from_u8` → None) |
| `zstd_level`, `blob_zstd_level`, `disable_blob_dictionary`, `blob_dictionary_size`, `blob_dictionary_train_bytes`, `blob_compression_group_bytes`, `max_cached_blob_value_bytes`, `blob_gc_staleness` | blob-file policy | — (`max_cached_vlog_value_bytes` ≈ `max_cached_blob_value_bytes`, `ONDAVVC1`) | missing. Not needed to read |
| `write_buffer_size`, `level_size_ratio`, `min_levels`, `target_file_size`, `l1_base_bytes`, `max_subcompactions`, `l1_file_count_trigger`, `l0_stall_threshold`, `soft/hard_pending_compaction_bytes`, `tombstone_compaction_threshold`, `compaction_picker`, `merge_chain_compaction_trigger` | LSM shape | base fields + `ONDACMP1` (target, l1_base, soft, hard) | partial. Irrelevant for read-only |
| `compaction_style` (u8), `fifo_max_bytes`, `fifo_ttl_ns` | FIFO | base (`compaction_style`, `fifo_max_bytes`, `fifo_ttl` **µs**) | enum values match (Leveled 0, Fifo 1). Unit: ns → µs |
| `klog_value_threshold` | inline/separated cutoff | base | same meaning (the separated target differs) |
| `block_size` | data block size | `ONDABLK1` | same |
| `enable_bloom_filter`, `bloom_fpr`, `bloom_fpr_per_level`, `optimize_filters_for_hits`, `bloom_auto_allocate` | bloom | base + `ONDABLM1` (`ONDABLM2` reserved for auto) | auto-allocate missing |
| `enable_block_index`, `index_sample_ratio`, `block_index_prefix_len`, `use_btree` | index | `use_btree` only | missing. The reader must be driven by footer flags |
| `sync` (u8), `sync_interval_ns` | WAL sync | `sync_mode` (u8), `sync_interval` **µs** | enums match (None 0, Full 1, Interval 2). ns → µs |
| `wal_write_buffer_size`, `skip_list_max_level`, `skip_list_probability`, `default_isolation` | runtime | — | ignore |
| `periodic_compaction_interval_ns` | periodic | `ONDAPRD1` (µs) | ns → µs |
| `partition_rules` ([{prefix b64, name}]), `partition_scheme` (name) | partitioning | legacy rules section + `ONDAPFN1` scheme name | same concepts. The scheme function must be registered by name in both engines. wavesdb fails open if the scheme is unregistered (`partition.go:261-275`) |
| `tier_rules` ([{prefix b64, tier, min_age_ns}]) | tiering | tier rules (min_age µs) | ns → µs. Tier names must exist in `Options` |
| `enable_prefix_delta_keys`, `block_restart_interval` | 2.1 | `ONDAPFX1` | same |
| `enable_range_deletes` | per-CF switch | — (capability only) | ignore |

For a **read-only open** only these matter: `comparator` (hard), `merge_operator` (hard if there are operands), and
`partition_scheme` (ondaDB may refuse to open an unresolved derived scheme; check `PartitionScheme::Unresolved`
handling). Codecs and block formats are self-described per block or footer.

Effort for `wavesdb_cfconfig::from_json(&[u8]) -> ColumnFamilyConfig` with `serde_json` (a new dependency) and unit
conversions: **~0.5-1 day**. Dispatch rule: a wavesdb blob always starts with `{`. ondaDB's never does in practice,
but the reliable signal is the manifest origin (the CRC polynomial), so the choice should not be made per blob.

---

## 6. CF identity and sequence numbers

* **Identity.** Both are **name-keyed** in the catalog.
  * No numeric CF id is persisted, except wavesdb's optional UnifiedID override (v3 dbFlag bit 2), which exists
    because wavesdb supports `RenameColumnFamily` (`maintenance.go:802`) and Clear (which mints a new id). ondaDB has
    no rename (a documented non-goal) and always derives `cf_id = fnv1a64(name)` (`unified.rs:27-28`), the same
    function as wavesdb's `cfID` (`unified.go:53-60`).
  * The unified id only prefixes memtable/WAL keys, never SST keys, so a WAL-free checkpoint is unaffected.
* **Default CF.** Neither engine has an implicit one. Both open whatever the catalog lists. Tests in both create
  `"default"` explicitly.
* **Sequence numbers.**
  * Both persist a single `GlobalSeq`/`global_seq` u64 in the fixed header, plus per-table `MaxSeq` (and range
    min/max seq).
  * A wavesdb checkpoint sets `GlobalSeq = max(MaxSeq)` (`maintenance.go:593-611`). ondaDB's open treats it as the
    visible cut (`db.rs:1798-1801`), so this is compatible as-is.
  * wavesdb managed mode adds `DiscardSeq`/`LastManagedCommit`, but that needs cap bit 7, which ondaDB refuses.
  * **Sequence encoding inside keys and WAL is SST/WAL scope**: the entry `seq` uvarint is the same as far as seen.

---

## 7. Effort summary (catalog + layout only; rough, one engineer)

| # | Item | Needed for | Effort |
|---|---|---|---|
| 1 | CRC32-C implementation + per-origin selection (manifest, blocks, WAL, blob frames) | everything | 0.5-1 d (wiring through the SST/WAL readers is extra) |
| 2 | WVMF v2/v3/v4 decoder (blob map, blob list, flag section, uvarint caps, watermark semantics) → `Manifest` + blob side table | any open | 2-3 d |
| 3 | CF dir naming `cf_` vs `cf-` by origin (read path) | local ckpt / restore | 0.5 d |
| 4 | JSON config blob → `ColumnFamilyConfig` (comparator, merge op, partition scheme, units) | any open | 0.5-1 d |
| 5 | Capability handling: accept bits 0-6; refuse 7/8 (already); ignore table caps and merge stats | any open | ~0 (falls out of #2) |
| 6 | Read-only open without writing LOCK (or tolerate a read-only dir) | immutable ckpt on RO media | 0.5 d |
| 7 | Remote mount: MANIFEST GET + rewrite tier/object + shared S3 tier; seed sizes from the manifest | object ckpt, lazy | 1 d |
| 8 | S3 auth parity: session token, env/profile/IMDS chain, anonymous, read-only guard; common-prefix listing | object ckpt on foreign bucket | 1 d |
| 9 | Restore-by-download | object ckpt, eager | 0.5 d |
| 10 | wavesdb MANIFEST-EDITS decoder/applier + MANIFEST-CURRENT pointer | live dir or live mirror only (not checkpoints) | 3-4 d |
| — | *Out of scope but blocking:* shared blob files + 16-B pointer, `.blob` framing, zstd-dict (7), vlog grouping, and per-block format parity | reading any separated value | 1-2 wk+ |

The catalog/layout work for a **local or object checkpoint** (#1-#9) is about **6-9 working days**. It is not enough
without the blob-file value path.
