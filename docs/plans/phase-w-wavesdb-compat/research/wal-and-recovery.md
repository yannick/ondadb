# ondaDB ← wavesdb v0.8.6: WAL replay and directory-open compatibility

Scope: what ondaDB (Rust, `/Volumes/HOME/code/storage-engines/ondadb`, HEAD of main)
needs in order to replay WAL segments written by wavesdb v0.8.6
(`/Volumes/HOME/code/storage-engines/wavesdb`, tag `v0.8.6` = HEAD) and to open a
wavesdb directory read-only or read-write. Read-only research; no file in either repo was
modified.

Citation convention: `W:` = wavesdb path, `O:` = ondaDB path, `file:line`.

---

## 0. Bottom line

| Area | Status |
|---|---|
| Frame `[len u32 LE][crc32c u32 LE][payload]` | **identical** |
| Legacy (non-envelope) payload | **identical, byte for byte** |
| Extended envelope (`0xff …`) | **incompatible**: different header *and* different record field order; both engines use `schema = 1` for different layouts |
| Control frames 16/17/18 | **incompatible**: different frame shape, id type, body, and commit-sequence semantics |
| Kind 19 (spill descriptor) | ondaDB refuses (`UnsupportedFormat`). Reading it also means reading the `txn-spill/` chunk files |
| Kinds 32/33 (managed) | **never written to the WAL** by wavesdb v0.8.6. The wavesdb decoder rejects them there too. Managed state lives in the manifest |
| File naming, generations | same names (`wal-<gen>.log`, `unified-wal-<gen>.log`) but **CF directory differs: `cf_<name>` (wavesdb) vs `cf-<name>` (ondaDB)** |
| Stripes (`.s1..s3`) | ondaDB only. wavesdb writes none, and ondaDB replay tolerates their absence |
| Segment header / recycling / preallocation | neither engine has any |
| Torn-tail semantics | same (clean stop at a short read or CRC failure; CRC-valid but undecodable → corruption) |
| Unified CF-id prefix | same 8-byte big-endian FNV-1a **by default**. wavesdb then persists a per-family id that can diverge from `hash(name)` after rename/clear/clone. ondaDB always recomputes `hash(name)` |
| Checkpoints | **contain no WAL segments** (flush first). For the checkpoint use case, WAL compatibility is optional |

In practice, **any wavesdb database with `CapExtendedRecords` set (bit 0) has a WAL ondaDB
cannot replay today**. The bit gets set on the first `Merge`, `DeleteRange`, `Prepare`, or
explicit `EnableFormatCapabilities`. From then on *every* frame wavesdb writes is an
envelope, including plain puts (W:internal/wal/wal.go:555-572). ondaDB currently fails such
an open with `UnsupportedFormat("record kind 0 …")` (walkthrough in §1.4). It fails closed and
does not misread.

---

## 1. WAL, field by field

### 1.1 File naming and placement

| | wavesdb | ondaDB |
|---|---|---|
| Per-CF segment | `<db>/cf_<name>/wal-<gen>.log` — W:db.go:2069 (`"cf_"+name`), W:columnfamily.go:345-347 | `<db>/cf-<name>/wal-<gen>.log` — O:src/db.rs:1423-1425 (`"{}/cf-{}"`), O:src/column_family.rs:544-546 |
| Unified segment | `<db>/unified-wal-<gen>.log` — W:unified.go:257-261 | `<db>/unified-wal-<gen>.log` — O:src/unified.rs:197-199 |
| Stripes | none | `wal-<gen>.log.s1..s3` for `SyncMode::None/Interval` — O:src/wal.rs:798-808, 877-891 |
| Generation scan | parse `wal-*.log`, sort ascending — W:columnfamily.go:884-900; W:unified.go:265-283 | same — O:src/column_family.rs:2969-2984; O:src/unified.rs:226-238 |
| Next gen on open | `last+1`, never append to a replayed segment — W:columnfamily.go:860-876; W:unified.go:429-446 | same — O:src/column_family.rs:772-786; O:src/unified.rs:289-307 |
| Per-CF segments under unified layout | **none created** (`openCFWAL` returns nil) — W:columnfamily.go:386-418 | always creates `wal-0.log` / `wal-<next>.log` per CF, in unified mode too — O:src/column_family.rs:617-625, 772-786 |
| Layout recorded | manifest `WALLayoutUnified` (db flag bit 0), checked at open and refused on mismatch — W:internal/manifest/manifest.go:316-323; W:migrate.go:53-67; W:db.go:1018 | `WalLayout` in its own manifest (O:src/db.rs:1418-1421) |

Consequences:
- Because of the directory-name difference, ondaDB's per-CF path resolution would find
  **no WALs and no SSTables** in a wavesdb directory. It needs a directory-naming shim (for
  example, `cf_` when the manifest came from wavesdb). This is a layout issue, not a WAL-byte
  issue, but it blocks everything else.
- Extra empty per-CF segments that ondaDB creates in a unified-layout wavesdb directory are
  harmless if wavesdb reopens it (it replays empty per-CF segments,
  W:columnfamily.go:824-852). They are still clutter, and wavesdb's own design calls them
  ambiguous (W:columnfamily.go:390-403).

### 1.2 Segment header, preallocation, recycling, write buffer, failover

- **No segment header** in either engine. wavesdb: "A WAL file writes no header (see wal.Open)"
  (W:migrate.go:76-78); `wal.Open` is `O_CREATE|O_WRONLY|O_APPEND` (W:internal/wal/wal.go:304-334).
  ondaDB: `OpenOptions::create(true).append(true)` (O:src/wal.rs:885-891).
- **No recycling or preallocation** in either engine. A grep of wavesdb for
  `Recycl|Prealloc|fallocate` finds nothing in non-test code. Zero-length segments are
  therefore legal and replay empty in both.
- **wavesdb write buffer** (`WALWriteBufferSize`, `UnifiedWALWriteBufferSize`):
  user-space coalescing, ignored under `SyncFull` (W:internal/wal/wal.go:181-190, 314-332,
  647-657). This is not a format change. It only widens what a *process* crash can lose, and
  a crash leaves an ordinary torn tail at a frame boundary or mid-frame. ondaDB has no write
  buffer and needs nothing for it.
- **Group commit**: both concatenate pre-encoded frames in one `write` (W:wal.go:542-687;
  O:src/wal.rs:1050-1142). Not a format concern.
- **Striping**: ondaDB only (O:src/wal.rs:794-808). When replaying a wavesdb dir, stripes
  `.s1..s3` are simply missing, and `replay_file` treats a missing file as empty
  (O:src/wal.rs:1209-1213). The reverse direction is a hazard: wavesdb would never read
  ondaDB's `.sN` stripes (W:columnfamily.go:891 only parses `wal-<n>.log`).
- **WAL failover** (`internal/walfailover`): a prototype that no engine code imports
  (`grep walfailover` outside the package finds nothing). No on-disk impact in v0.8.6.

### 1.3 Frame and legacy payload — identical

wavesdb (W:internal/wal/wal.go:5-11, 64, 359-405):
```
[payloadLen u32 LE][crc32c(payload) u32 LE][payload]
payload = record*   record = flags(1) | keyLen uvarint | valLen uvarint | seq uvarint
                             | ttl varint (iff HAS_TTL) | key | value
```
ondaDB (O:src/wal.rs:8-12, 42, 653-675, 749-780): the same order and widths.

| Field | wavesdb | ondaDB | Compatible |
|---|---|---|---|
| payloadLen | u32 LE (W:wal.go:402) | u32 LE (O:wal.rs:777) | yes |
| crc | CRC32-C of payload (W:wal.go:403) | CRC32-C of payload (O:wal.rs:776-778) | yes (same Castagnoli; both engines "same WAL frame" per O:docs/format-registry.md:3-4) |
| flags | mask `0x01\|0x02\|0x10`, SINGLE_DELETE needs TOMBSTONE (W:internal/format/format.go:37,45-50) | mask `0x17` (also admits `HAS_VLOG 0x04`) (O:src/format.rs:37-38, 90-107) | yes. ondaDB is a superset. wavesdb never writes `0x04` in the WAL |
| keyLen, valLen, seq | uvarint | uvarint | yes |
| ttl | zig-zag varint iff `0x02`, absolute Unix-nanos | same | yes |
| kind mapping | `KindOf(tomb, single)` (W:format/capability.go:276-286) | `point_kind(...)` (O:src/wal.rs:692-698) | yes |
| empty payload (`len=0`) | legacy decode → 0 records, OK (W:wal.go:898-913) | skipped (O:docs/formats.md "payload_len == 0 is legitimate") | yes |
| multiple records per frame | yes, one frame per batch | yes | yes |

Under the unified layout, legacy frames carry the 8-byte big-endian CF-id prefix inside
`key` in both engines (W:unified.go:62-77; O:src/unified.rs:39-44), and a range delete cannot
be legacy-encoded in either. **A wavesdb database that never enabled `CapExtendedRecords`
has a WAL that ondaDB's current decoder replays correctly**, subject to the directory-name
and CF-id caveats.

### 1.4 Extended envelope — incompatible

wavesdb (W:internal/wal/wal.go:13-27, 483-521, 915-1041; W:docs/format-compatibility.md:288-299):
```
0xff | schema uvarint (=1) | count uvarint (>0) | envelopeFlags uvarint (=0) |
  ( kind uvarint | modifiers uvarint | cfID uvarint | keyLen uvarint | key |
    valLen uvarint | value | seq uvarint | ttl varint if ModHasTTL )*
```
ondaDB (O:src/wal.rs:242-256, 291-386, 406-449, 606-650; O:docs/formats.md:168-212):
```
0xFF | schema uvarint (1 = per-CF, 2 = unified) | count uvarint |
  ( kind uvarint | modifiers uvarint | alen uvarint | blen uvarint | seq uvarint |
    ttl varint if HAS_TTL | a | b )*
```

| Field | wavesdb | ondaDB | Compatible |
|---|---|---|---|
| marker | `0xff` (W:format/capability.go:337) | `0xFF` (O:wal.rs:247) | yes |
| schema | always `1`. Layout **not** encoded in schema; `schema != 1` → corrupt (W:wal.go:921-924; format/capability.go:351-353) | `1` = per-CF, `2` = unified; others → `UnsupportedFormat` (O:wal.rs:249-256, 614-619) | **no**. `1` means a different layout in each engine |
| count | uvarint, must be > 0 | uvarint (0 allowed for data) | mostly |
| envelopeFlags | uvarint, must be 0 (W:wal.go:931-934) | **absent** | **no** |
| per-record field order | kind, mods, **cfID**, keyLen, **key**, valLen, **value**, seq, ttl? | kind, mods, alen, blen, seq, ttl?, a, b | **no** |
| cfID | uvarint unified-family id (0 per-CF). Replay cross-checks against the key prefix (W:unified.go:386-393) | absent (the id is only in the 8-byte key prefix) | **no** |
| modifiers | WAL mask = `HasTTL` only (W:format/capability.go:346-347) | `HAS_TTL`; `HAS_VLOG` explicitly refused in WAL (O:wal.rs:426-432) | yes |
| kind 4 merge + TTL | refused (W:wal.go:984-987) | not checked in the WAL decoder | superset |
| kind 5 range delete | key = start, value = end; mods must be 0; both non-empty; under unified both bounds prefixed (W:wal.go:988-992, 1027-1049; W:unified.go:394-398) | a = start, b = end; same rules (O:wal.rs:465-488) | semantics agree, bytes do not |
| trailing bytes / count mismatch | corrupt (W:wal.go:955-960) | corrupt (O:wal.rs:637-649) | yes |
| unknown kind | `!Decodable && !IsControl` → corrupt (W:wal.go:977-979) | `> 63` → Corruption; assigned-but-unknown → `UnsupportedFormat` (O:src/format.rs:258-278) | different error class, both fail closed |
| admission vs manifest word | frame needing bits the word lacks → `NotAdmittedError` (W:wal.go:852-854, 874-885) | none; ondaDB never checks frame vs manifest caps | ondaDB is more permissive |

**What ondaDB does today with a wavesdb envelope frame.** After `0xFF`, `schema = 1` passes
(O:wal.rs:614-619) and `count` is read. The peeked "first kind" is actually wavesdb's
`envelopeFlags = 0`, which is not a control kind (O:wal.rs:627-636). `decode_envelope_record`
→ `decode_raw_record` → `check_kind(0)` → `UnsupportedFormat("record kind 0 is not
implemented …")` (O:src/format.rs:258-278). The whole open fails. It fails closed, with a
confusing message.

**Reverse direction.** wavesdb with an ondaDB schema-2 frame → corrupt (schema check). With
an ondaDB schema-1 frame, wavesdb reads the first record's kind (≥1) as `envelopeFlags` →
corrupt. Also fails closed.

**Discriminator you can build on.** ondaDB never writes kind 0 (O:src/format.rs:258-278),
and wavesdb always writes `envelopeFlags = 0` (W:wal.go:513) and rejects anything else. So in
a `0xFF | 1 | count | X …` frame, `X == 0` means wavesdb layout and `X >= 1` means ondaDB
schema-1 layout. This is unambiguous today. It depends on wavesdb never defining envelope
flags, so it should be written into `docs/format-registry.md` as a pinned rule (the registry
currently has no row for WAL envelope schemas; see O:docs/format-registry.md:52-82).
Alternatively, choose the decoder per database from the manifest's origin. The byte rule is
safer because it also covers a planted segment. Note also that ondaDB's feature assessment
calls the WAL frame "identical" (O:docs/wavesdb-feature-assessment.md:14). That is true for
the outer frame and the legacy payload only.

### 1.5 Transaction-control kinds (16–19) and managed kinds (32–33)

| Kind | wavesdb meaning | wavesdb frame shape | ondaDB |
|---|---|---|---|
| 16 prepare | `KindTxnPrepare` | **one record** (count = 1), `cfID = 0`, `key` = id string (1..128 B, no NUL), `value` = versioned batch body, `seq = 0` (W:prepared_api.go:186; W:prepared.go:22-50, 136-176; W:wal.go:452-465, 945-951) | frame = kind-16 record (a = 16-byte id, b = CF ids u64 LE) **followed by N data records** with seq 0 in the same frame (O:src/wal.rs:985-1012, 521-576) |
| 17 commit | `KindTxnCommit` | one record, `key` = id, `value` empty, **`seq = commitSeq` = LAST sequence of the span** (W:prepared_api.go:379-396; W:prepared_recovery.go:143) | a = 16-byte id, b = `commit_seq u64 LE ‖ count u64 LE` where commit_seq is the **FIRST** sequence, record `seq = 0` (O:src/wal.rs:1024-1047, 591-599) |
| 18 abort | `KindTxnAbort` | one record, `key` = id, `seq = 0` (W:prepared_api.go:476) | a = id, b empty (O:wal.rs:585-590) |
| 19 spill descriptor | `KindSpillDescriptor` (3.4), control, under `CapTxnDecisions` | one record, `key` = id, `value` = descriptor body (W:spill_prepare.go:104; W:spill_descriptor.go:77-120+) | reserved; `check_kind(19)` → `UnsupportedFormat` (O:src/format.rs:219-238, 258-278) |
| 32 / 33 managed | `KindManagedOwnership` / `KindManagedDiscardFloor` | **Never written to the WAL.** Only referenced in `internal/format`. `IsControl()` excludes them (W:format/capability.go:224-230), so the WAL decoder rejects them as corrupt (W:wal.go:977-979). wavesdb's docs table claims "control; admitted under CapManagedMode" (W:docs/format-compatibility.md:410,415), which contradicts the code | reserved; `UnsupportedFormat` |

wavesdb's control-frame rule is "exactly one record per control frame", enforced both on
write (`ErrControlFrameShape`, W:wal.go:452-465) and on read (W:wal.go:945-951). ondaDB's
prepare frame is the opposite: 1+N records in one frame. Neither engine can read the other's
control frames. Both replay decisions into a registry: wavesdb in
W:prepared_recovery.go:26-92 plus `finishPreparedRecovery` (W:prepared_recovery.go:108-196);
ondaDB via the two-pass `RecoveredPrepares` (O:src/unified.rs:247-278; O:src/db.rs:850-907).

**Important recovery consequence.** In wavesdb, a committed prepared transaction's data is
**not** re-logged as data frames. Commit writes the decision frame and installs into the
memtable (W:unified_prepared.go:98-118). After a crash, the data exists in the WAL only as
the prepare body plus the decision. So even a *resolved, committed* prepared transaction
whose segment has not been flushed yet requires decoding kinds 16 and 17 to recover. Skipping
control frames would silently lose committed data. The only acceptable outcomes for ondaDB are
"decode them" or "refuse the open".

**Sequence semantics differ.** wavesdb replay's `lastSeq` includes the commit decision's
`seq` (it is `max(rec.Seq)` over every record, control records included, W:wal.go:863-866).
The decision seq is the last sequence of the span, and install uses
`firstSeq = commitSeq - span + 1` with op seq = `firstSeq + ordinal`
(W:prepared_recovery.go:143; W:prepared_api.go:493-520). ondaDB control records contribute 0
and raise the watermark explicitly to `commit_seq + count - 1` (O:src/wal.rs:176-189;
O:src/db.rs:855-878). A translator must map wavesdb `(commitSeq, span)` to ondaDB
`(commitSeq - span + 1, span)` and apply ordinals, not "count".

### 1.6 Torn-tail and corruption semantics

| Case | wavesdb (W:wal.go:800-872) | ondaDB (O:src/wal.rs:1205-1261) | Same? |
|---|---|---|---|
| EOF or partial header | clean end | clean end | yes |
| `plen > bytes remaining in file` | clean end, **checked before allocating** (W:wal.go:827-831) | allocates `vec![0; plen]` (up to 4 GiB) and then hits short read → clean end (O:wal.rs:1221-1226) | same result. ondaDB has a memory-spike hazard on a garbage length |
| short payload | clean end | clean end | yes |
| CRC mismatch (anywhere, not only at the true tail) | clean end of segment, later frames ignored | clean end of stripe | yes |
| CRC-valid but undecodable | `ErrCorrupt` with offset | `Corruption` | yes |
| CRC-valid, needs caps not in manifest | `NotAdmittedError` → `ErrUnsupportedFormat` | not checked | ondaDB more permissive |
| Frame atomicity | decode whole frame, then apply (W:wal.go:840-870, 887-913) | legacy frames decode record by record *while applying* (O:wal.rs:1251-1259); envelope applies per record after its own validation | see note |

Note on atomicity: for legacy frames, ondaDB calls `f(rec)` per record before decoding the
rest of the frame (O:wal.rs:1251-1259). A CRC-valid frame with a malformed second record
therefore applies record 1 and then returns `Corruption`. The open fails anyway, so no
partially replayed DB is ever served, but it is weaker than wavesdb's stage-then-apply. The
envelope path has the same shape (O:wal.rs:637-645). Not a compatibility blocker.

### 1.7 Compatibility summary table

| Item | Verdict | Work for ondaDB to read wavesdb |
|---|---|---|
| Outer frame | identical | none |
| Legacy record | identical | none |
| Per-CF dir name | `cf_` vs `cf-` | naming shim keyed on manifest origin |
| Unified CF id | FNV-1a by default; wavesdb persists `UnifiedID` (manifest v3 `dbFlagCFUnifiedIDs`) that diverges after rename/clear/clone (W:columnfamily.go:502-560, 758-760; W:manifest.go:297-307) | route WAL prefixes by the manifest id, not `cf_id(name)` (O:src/unified.rs:28-37) |
| Envelope header and record layout | incompatible | second decoder, selected by the `envelopeFlags == 0` discriminator; honour `cfID` (cross-check it against the prefix, as wavesdb does) |
| Control 16/17/18 | incompatible | decode the wavesdb body (§2); support variable-length ids; range deletes inside prepares; convert sequence semantics |
| Kind 19 | unsupported | refuse (current behaviour is correct) or implement the spill reader (§3.2) |
| Kinds 32/33 | never in WAL | keep refusing |
| Stripes | ondaDB-only | none for reading; hazard for round-trip |
| Recycling/prealloc/header | none on either side | none |

---

## 2. Prepared-transaction body: not byte-identical

The wavesdb prepare body (value of the kind-16 record) is encoded by `encodePreparedBatch`
(W:prepared.go:128-176), decoded with strict checks (W:prepared.go:178-328), and pinned by
`TestPreparedBatchGolden` (W:prepared_codec_test.go:12-52; layout doc
W:docs/format-compatibility.md:220-239):
```
version uvarint (=1) | flags uvarint (bit0 = unified) | isolation uvarint |
readSeq uvarint | span uvarint | cfCount uvarint | (nameLen uvarint | name)* |
opCount uvarint |
(cfIdx uvarint | kind uvarint | ordinal uvarint | keyLen uvarint | key |
 valLen uvarint | value | ttl varint)* |
crc32c u32 LE   (over everything before it)
```
Golden (sample: unified, Snapshot, readSeq 7, span 2, cfs [a,b], 4 ops including a merge
and a TTL'd put): `0101030702020161016204000100026b3102763100000401026b31026f7000010200026b320000010100026b33027633a41362a0a3c0`
(W:prepared_codec_test.go:31-33).

ondaDB's prepare (O:src/wal.rs:985-1012; O:docs/formats.md:247-285) has **no body at all**.
The writeset is ordinary envelope records in the same frame:
```
record0: kind 16 | mods 0 | alen 16 | blen 8*C | seq 0 | id[16] | C × cf_id u64 LE
record1..N: kind 1/2/3/4 | mods | alen 8+len(uk) | blen | seq 0 | ttl? | cf_id BE ‖ user key | value
```

| Aspect | wavesdb | ondaDB |
|---|---|---|
| Frame record count | 1 | 1 + N |
| Transaction id | caller string, 1..128 bytes, no NUL (W:prepared.go:22-50) | fixed 16 bytes (O:wal.rs:504-512) |
| Families | **names**, indexed by `cfIdx` (so they survive a rename) | ids (FNV-1a, O:src/prepared.rs:28-40), u64 **LE** list |
| Keys | user keys (prefix re-added at install from the current id, W:prepared_api.go:493-520) | prefixed keys (8-byte BE CF id) |
| Isolation, readSeq | persisted | not persisted |
| span / ordinal (same key written and merged) | persisted, `ordinal < span` validated (W:prepared.go:320-326) | none (count-based) |
| TTL | always present (varint, 0 = none) | only if `HAS_TTL` modifier |
| Range delete in a prepare | **allowed** (`Decodable` includes kind 5; spans reserved at prepare, W:prepared.go:277; W:prepared_api.go:171-183) | **refused** (O:wal.rs:559-568) |
| Inner checksum | trailing CRC32-C over the body | none (frame CRC only) |
| Versioning | body `version`; unknown → `ErrUnsupportedFormat` | none |

**Verdict: not byte-identical, and not even structurally equivalent.** Reading wavesdb
prepares means a new decoder, a variable-length id in ondaDB's `PreparedRegistry` and public
API (today `[u8; 16]`, O:src/prepared.rs:43-58), name→id resolution through the manifest's
`UnifiedID`, span/ordinal sequence assignment, and range-delete support in prepared install.

Pairing and retention rules also differ. wavesdb treats "decision without prepare" as
corruption (W:prepared_recovery.go:81-84) and pins the prepare's segment until the decision's
segment retires (W:prepared_registry.go:310-335; W:unified.go:756-774). The decision and the
installed data always land in the same generation (W:unified_prepared.go:14-27). ondaDB's
decisions are self-sufficient, and its stripes lose relative order (O:src/unified.rs:201-212).
When ondaDB opens a wavesdb directory, the wavesdb invariants (a decision is always preceded
in generation order by its prepare) hold, so the stricter check is available.

---

## 3. Managed sequence mode and large-transaction spill

### 3.1 Managed mode (`CapManagedMode`, bit 7)

What is persisted: **manifest only**. Manifest v4 db flag bit 4 announces `DiscardSeq` (the
durable history floor, 0 = none) and `LastManagedCommit` (advisory)
(W:docs/format-compatibility.md:241-253). The capability can only be enabled at creation of
an empty DB (W:managed.go:43-70). Every open must pass `Options.ManagedMode` matching the
manifest, checked **before** WAL replay (W:managed.go:49-58; W:db.go:1010).

In the WAL: no new kinds. Commits carry externally assigned sequences, which are strictly
increasing per segment but gappy, anywhere in `(0, 2^64-1)` (W:managed.go:96-130). Replay
enforces monotonicity for non-control records per segment (`managedReplayGuard`,
W:managed.go:337-355). A managed commit decision uses `firstSeq = commitSeq = managedSeq`
(W:prepared_api.go:383-394).

What ondaDB needs:
- **Read/refuse correctly.** Bit 7 is outside ondaDB's `KNOWN_CAPS` (`0x7F`,
  O:src/format.rs:145-162), so the open is refused with `UnsupportedFormat`. That is correct.
- **To allow read-only:** accept bit 7 in RO mode only, and treat `DiscardSeq` as a read
  floor. Compaction may already have dropped shadowed versions at or below it
  (W:managed.go:36-38), so snapshot reads below it must be refused (wavesdb returns
  `ErrHistoryDiscarded`). Varints already cover large sequences. Optionally add the
  monotonic replay check.
- **Read-write:** refuse. ondaDB has no `CommitAt`/`BeginAt` admission, and engine-allocated
  sequences would break the external sequencer's contract. Kinds 32/33: keep refusing (they
  never appear in the WAL).

### 3.2 Large-transaction spill (kind 19, feature 3.4)

What is persisted:
- A **spill descriptor** as the prepare frame: one control record, key = id, value = body
  `version(=1) | flags(bit0 unified) | isolation | readSeq | span | txnID | cfCount |
  (cfID, nameLen, name)* | chunkCount | (ordinal, bytes, sha256[32], firstOrdinal,
  lastOrdinal, nameLen, name, rangeCount, (cfID, minLen, min, maxLen, max)*)* | … | crc`
  (W:spill_descriptor.go:11-120; W:spill_prepare.go:104).
- **Chunk files** under the spill root `txn-spill/txn-<016x>/NNNNNNNN.chunk`, format
  `WVSPILL1`, a 36-byte header, a sparse index, a bloom filter, and a trailer
  (W:internal/txnspill/chunk.go:28-44; W:internal/txnspill/spill.go:91-135, 209). The spill
  root defaults to `<db>/txn-spill` but **can be moved out of the database directory** with
  `Options.TxnSpillDir` (W:spill_txn.go:28-47). In that case the directory is not
  self-contained while such a transaction is pending.
- The decision is an ordinary kind 17/18. A committed spill is installed by streaming the
  chunks at `commitSeq` (W:prepared_recovery.go:123-141; W:spill_prepare.go:205-262). An
  undecided one re-reserves key spans from the descriptor without opening chunks
  (W:prepared_recovery.go:160-177). Orphan spill directories are swept at RW open
  (W:spill_txn.go:49-66).

What ondaDB needs:
- **Minimum:** keep refusing kind 19 (the current `UnsupportedFormat` is correct). Make the
  error say that the source must resolve its spilled prepares and flush. Never sweep or
  delete `txn-spill/`.
- **Full support:** the descriptor decoder, the chunk reader (sha256 digest verification),
  span reservations, and streamed install. This is a multi-week feature. Only worth it if a
  live-open of wavesdb dirs with in-flight 3.4 transactions is a real requirement.

---

## 4. Recovery semantics when ondaDB opens a wavesdb directory

1. **Last-seq derivation.** wavesdb starts `seq = nextSeq = manifest.GlobalSeq`
   (W:db.go:1029-1030) and then raises it with `observeSeq(max replayed seq)` per CF
   (W:columnfamily.go:854) and for unified (W:unified.go:405-426), including the max
   decision seq from `finishPreparedRecovery`. ondaDB does the same: `next = global_seq + 1`,
   `visible = global_seq` (O:src/db.rs:1798-1801), then `observe_seq(replay max)`
   (O:src/db.rs:1853, 2056). This is compatible. The wavesdb manifest's `GlobalSeq` is the
   visible cursor or the pending jump target (W:db.go:1659-1693), and a checkpoint lowers it
   to the max table seq (`stampCutSeq`, W:maintenance.go:579-603). The one trap: wavesdb
   **control** records feed `lastSeq` (the commit decision's seq), while ondaDB's control
   records contribute 0. A translator must raise the watermark from the decision explicitly.
2. **WAL deletion vs manifest.** Per-CF flush: the table is installed and published in
   memory, **then** `persistManifest()`, then `os.Remove(walPaths)`, and only if persist
   succeeded (W:flush.go:381-418). Unified flush: `persistManifest()` (W:unified_flush.go:251),
   then `noteFlushed` deletes segments unless a prepare pins them (W:unified.go:740-775).
   The durable ordering (manifest before WAL unlink) matches ondaDB invariant 1. Unlike
   ondaDB, wavesdb publishes before the durable edit, but that does not change the files
   ondaDB finds. Implication: a WAL segment found at open may overlap SSTables (a crash
   between persist and unlink). Replay must be idempotent by (key, seq). Both engines'
   memtables replace (key, seq) in place (W:prepared_recovery.go:100-104).
3. **Pinned segments.** A segment holding an unresolved prepare survives flushes
   (W:unified.go:756-774). ondaDB's `WalGenState` (O:src/prepared.rs) must adopt the same
   pins, or refuse, for any wavesdb segment containing kind 16/19.
4. **Instance nonce.** wavesdb mints a nonce at a RW open that declares any shared tier. It
   names published object files and is cleared on every copy (checkpoint, backup, clone)
   (W:internal/manifest/manifest.go:324-333; W:docs/format-compatibility.md:99-104). It has
   **no WAL role**. For ondaDB it only matters when publishing to a tier shared with wavesdb.
   ondaDB tracks its own (`instance_nonce`, O:src/db.rs:1415).
5. **Poisoned state.** wavesdb's `Poison` is in-memory only (W:poison.go:9-22): "recovery is
   a reopen". Nothing poisoned is persisted, and the directory shows only its effects (a WAL
   left unflushed, possibly a torn tail). Nothing is needed.
6. **Layout and mode gates before replay.** wavesdb refuses a WAL-layout mismatch
   (W:migrate.go:53-67) and a managed-mode mismatch (W:managed.go:49-58) before replay.
   ondaDB should mirror both: choose the per-CF or unified path from the manifest's
   `WALLayoutUnified`, never from options. A database mid-migration may still hold
   pre-switch per-CF segments that the unified layout replays too (W:columnfamily.go:386-403,
   865-876).
7. **Clean shutdown.** `Close` rotates every CF memtable and the unified memtable and drains
   the flush workers (W:db.go:1939-1996). After a clean close, the WAL files left are
   normally the freshly opened, empty next-generation segments, plus any segment pinned by
   an unresolved prepare or left behind by a dropped or failed flush job. This is inferred
   from the code; it was not tested here. So for a **cleanly closed wavesdb database with no
   unresolved prepares**, ondaDB's replay will usually see only zero-length files. Those
   replay fine regardless of envelope format.
8. **Prerequisite outside the WAL.** All of the above assumes ondaDB can parse the wavesdb
   manifest (v2/v3/v4 flag blocks, `UnifiedID`s, `WALLayoutUnified`, the capability word,
   `MANIFEST-EDITS`). ondaDB's manifest has its own layout (`ONDACAP1` tail,
   O:src/format.rs:109-119). That is a separate workstream and not covered here.

---

## 5. Do wavesdb checkpoints contain WAL segments?

**No.** `DB.Checkpoint` (W:maintenance.go:427-545):
- W:maintenance.go:431-433: "A checkpoint copies no WAL segments: it relies on the flush
  above to have moved everything into SSTables first."
- It refuses if any prepared transaction is unresolved (W:maintenance.go:458,
  `refuseUnresolvedPrepares`: "a copy cannot carry their WAL frames", W:maintenance.go:68-78).
- It refuses tiered families.
- On a writable source it calls `FlushMemtable` for every CF. That call also drains the
  unified memtable through `unified.flushAllAndWait()` (W:stats.go:930-936).
- It hard-links only klogs and blob files into `cf_<name>/` and writes a manifest with
  `GlobalSeq` lowered to the max table seq (`stampCutSeq`, W:maintenance.go:579-603).
- It refuses an ordinary read-only source. The narrow exception is `OpenReadOnlyNoLock`, and
  only when it replayed no mutable WAL state (`checkpointSourceReady`,
  W:maintenance.go:547-577).
- The object-store checkpoint path has the same shape: flush, then upload tables, no WAL
  (W:checkpoint_objstore.go:146-200).

**Backup** is different. On a *read-only* source it copies live WAL segments: per-CF
`wal-<g>.log` (W:maintenance.go:680-694) and unified `unified-wal-<g>.log` into the backup
root (W:maintenance.go:705-725). On a writable source it flushes and copies no WAL.

**Therefore:** if the use case is "ondaDB opens a wavesdb **checkpoint**" (local or
object-store), **WAL compatibility is optional**. There are no WAL segments and no prepared
state. The only WAL files ondaDB might meet are the empty ones a later wavesdb open created.
The blockers become the manifest and SSTable formats plus the `cf_` directory name. WAL
compatibility is required only for (a) opening a live or crashed wavesdb directory in place,
and (b) opening a wavesdb **Backup taken from a read-only source**.

---

## 6. What to build, with effort (rough)

| # | Item | Effort |
|---|---|---|
| 0 | Prerequisites: `cf_` directory naming, manifest parse, `UnifiedID` routing, layout/mode gates | separate workstream (manifest agent) |
| 1 | Guard `plen` against remaining file size before allocating (robustness, both formats) | < 0.5 day |
| 2 | wavesdb envelope decoder for data kinds 1–5 (discriminate on `envelopeFlags == 0` after `schema = 1, count`; honour `cfID`, cross-check it against the key prefix under unified; per-CF expects 0) + golden fixtures produced by the wavesdb binary + registry row pinning the discriminator | 1–2 days |
| 3 | Admission check of frame kinds vs the manifest word (parity with wavesdb `NotAdmittedError`) | 0.5 day |
| 4 | Refuse-correctly path for control frames: on any wavesdb kind 16/17/18/19 in a segment being replayed, fail the open with an actionable error ("resolve prepared transactions and flush/close cleanly in wavesdb first"). Required because committed-but-unflushed prepares exist only as prepare + decision | 0.5 day |
| 5 | Full wavesdb 3.2 support: body v1 decoder with CRC, variable-length ids through registry and API, name→id resolution, span/ordinal sequencing, range deletes in prepared install, generation pins | 1–2 weeks |
| 6 | Managed mode RO-only acceptance (`DiscardSeq` read floor, monotonic replay check); RW refused | 2–3 days |
| 7 | Kind 19 spill support (descriptor + `WVSPILL1` chunk reader + streamed install + external `TxnSpillDir`) | 2–4 weeks; recommend refusing instead |

Recommended minimum for "open a wavesdb dir": items 1–4 (plus 0). That gives correct replay
for every wavesdb WAL that contains no transaction-control frames, and a correct refusal for
the rest. For "open a wavesdb checkpoint", none of the WAL items are needed.
