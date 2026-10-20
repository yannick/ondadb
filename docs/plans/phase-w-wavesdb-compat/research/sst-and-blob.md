# What ondaDB needs to read wavesdb v0.8.6 SSTables and blob files

Research date 2026-09-23. wavesdb at `v0.8.6` (= `main` HEAD `a26fb4c`), ondaDB at `main` `0f4ebc6`.
Paths: `W:` = `/Volumes/HOME/code/storage-engines/wavesdb`, `O:` = `/Volumes/HOME/code/storage-engines/ondadb`.
Everything below comes from the code. Doc claims were checked against the code, and five claims were
also checked by running scratch probes (throwaway scratch programs, not committed).

> **Citations updated 2026-10-20.** The findings describe wavesdb v0.8.6, but `file:line` citations into code that the plan C 2-pre layout refactor moved (`options.go`, `checkpoint_objstore.go`, `objstore_mode.go`) now point at wavesdb `5459080` (main after 2-pre). Every other wavesdb citation is unchanged and still reads against v0.8.6.

---

## 0. Headline findings

1. **The checksum polynomials differ, and this is the biggest problem.** ondaDB's `checksum()` says it computes
   "CRC32-C (Castagnoli)" but calls `crc32fast`, which computes **CRC-32/IEEE**.
   - `O:src/encoding.rs:11-17`:
     ```rust
     /// CRC32-C (Castagnoli) checksum — hardware-accelerated on x86-64/aarch64.
     pub fn checksum(b: &[u8]) -> u32 { let mut h = crc32fast::Hasher::new(); ... }
     ```
     `O:Cargo.toml:43` `crc32fast = "1.4"` (the lockfile has 1.5.0).
   - wavesdb uses real Castagnoli: `W:internal/encoding/encoding.go:13-20`
     `var crcTable = crc32.MakeTable(crc32.Castagnoli)`.
   - Probe: `crc32fast("123456789") = cbf43926`, which is the IEEE check value. Go's
     `crc32.Castagnoli("123456789") = e3069283`.
   - Consequence: **every** wavesdb block frame, vlog/blob frame, blob header and dict frame fails
     ondaDB's CRC check. The same applies to WAL and manifest bytes.
   - The ondaDB claims "every stored byte is CRC32-C" (AGENTS.md invariant 4, docs) and "same WAL frame, same SST
     block frame" (`O:docs/format-registry.md:3-4`) are false at the byte level.
   - Fixing ondaDB's *own* format is a migration. Every existing ondaDB file is IEEE-checksummed.
2. **ondaDB cannot open any wavesdb v0.8.6 table today.** The footer magic differs:
   - ondaDB `WAVESST1` = `0x5741_5645_5353_5431` (`O:src/sst/mod.rs:43`), enforced at `O:src/sst/reader.rs:289`.
   - wavesdb `WAVESST2` = `0x5741564553535432` (`W:internal/sst/sst.go:80`). "format 2 switches bloom
     filters to tagged xxh3 hashing".
   - The footer flag bits 0x04 and 0x08 also mean different things in the two engines (§1.3).
3. **The codec-id registry row "0–5 agree" (`O:docs/format-registry.md:91,96`) is wrong for ids 2 and 4.**
   - In wavesdb, ids 2 and 4 always *decode as Zstd* and are never written (`W:internal/compress/compress.go:4-15, 79-86, 210-213`).
   - In ondaDB, ids 2 and 4 are raw LZ4 blocks (`O:src/compress.rs:28,57`).
   - So a wavesdb pre-v0.4.0 block tagged 2 or 4 fails in ondaDB as an LZ4 error. An ondaDB LZ4 block fails in wavesdb as a zstd error.
   - The registry's other claim, "Both engines default to codecs in the shared range" (`:103-104`), is also false:
     - wavesdb v0.8.6 defaults to `{None, LZ4→tag 6, Zstd}` per level (`W:options.go:1605`).
     - Blob values use Zstd, and a trained dictionary once one exists (tag 7).
4. **Blob storage has a different model.**
   - wavesdb puts a 16-byte `(blobID u64, blobOff u64)` pointer in the klog entry, plus a uvarint if grouped. The value lives in shared `<id>.blob` files with a v2 32-byte header, per-file zstd dictionaries and group frames.
   - ondaDB puts an 8-byte offset in the entry, pointing into the table's own `.vlog`.
   - The **value frame itself is byte-identical** (`crc|alg|compLen|payload`), apart from the CRC polynomial.
5. **What is identical:**
   - Block frame layout (not the checksum).
   - Legacy entry-flag byte, varints, zig-zag TTL (absolute Unix nanoseconds), sequence as a separate uvarint.
   - Flat index and B+tree index encodings.
   - Bloom bit math and the xxh3-64 hash (verified identical).
   - Range-fragment body, comparator name "memcmp", LZ4 block format (verified interoperable), snappy, zstd and deflate.

---

## 1. SST (`.klog`) compatibility table

### 1.1 Block frame

| Field | wavesdb | ondaDB | Status |
|---|---|---|---|
| Layout | `alg u8 | compLen u32 LE | rawLen u32 LE | crc u32 LE | comp` (13 B). `W:sst.go:81,176-193`. The package doc at `sst.go:16` omits `alg`, which is a stale comment. | identical, `O:src/block.rs:7,19,24-42` | **identical** |
| CRC scope | crc over compressed bytes, `W:sst.go:190,209` | same, `O:block.rs:39,60` | identical scope |
| CRC polynomial | **CRC32-C**, `W:internal/encoding/encoding.go:15-20` | **CRC-32 IEEE** (crc32fast), `O:src/encoding.rs:13-17` | **DIFFERS, fatal** |
| Store-raw fallback | `len(comp) >= len(payload)` → alg 0, `W:sst.go:182-185` | `c.len() < raw.len()` else None, `O:block.rs:30-34` | same semantics |
| rawLen bomb guard | `rawLen <= compLen*1000 + 1 MiB`, `W:sst.go:220` | none | ondaDB should add it |
| Unknown alg | error | `Corruption("bad algorithm")`, `O:block.rs:50-51` | should be `UnsupportedFormat` |

### 1.2 Data block encodings

wavesdb picks the encoding **per block** from the block's first byte, `W:sst.go:39-43,656-680` and `delta.go:71-96`.
ondaDB picks it **per table** from the footer, `O:src/sst/mod.rs:58-92` and `reader.rs:304-326`.

| Field | wavesdb | ondaDB | Status |
|---|---|---|---|
| Legacy entry | `flags(1) | klen uv | vlen uv | seq uv | ttl zz-varint? | key | value \| ptr`, `W:sst.go:497-532` | same prefix, `O:src/sst/mod.rs:14-17,321-370` | identical except the pointer |
| Flags byte | 0x01 tomb, 0x02 ttl, 0x04 vlog, **0x08 VlogGrouped**, 0x10 single-del. Mask and combination rules at `W:internal/format/format.go:16-67` | 0x01/0x02/0x04/0x10. 0x08 is rejected, `O:src/format.rs:16-36` | ondaDB must accept 0x08 on the wavesdb path |
| Vlog pointer | **16 B** `blobID u64 LE | blobOff u64 LE`, plus `inGroupOff uvarint` if grouped. `W:sst.go:522-527,757-773` | **8 B** `vlog_off u64 LE`, `O:mod.rs:365-367,438-443` | **DIFFERS** |
| valLen for blob values | RAW value length, `W:sst.go:494-496` | logical length, `O:mod.rs:258` | same meaning |
| Legacy restart trailer | none; wavesdb legacy/extended blocks never carry restarts | optional table-level `restart_off u32 × R | R u32` (FOOTER_RESTARTS 0x04), `O:writer.rs:485-496` | ondaDB can read without it (legacy path) |
| Extended block | per-block header `0xff | schema=1 | keyEnc=0 | count` (count is a 5-byte padded uvarint), then entries `kind uv | mods uv | klen | vlen | seq | ttl? | key | val\|ptr`. `W:sst.go:29-31,536-594`; `W:internal/format/capability.go:337,354-357` | **no block header**; table-level FOOTER_EXTENDED_BLOCK 0x10; same entry fields, `O:mod.rs:349-356,393-417` | entry identical, **header differs** (ondaDB must skip 8 B and dispatch on `0xff`) |
| Modifier bits | ttl 0x02, vlog 0x04, grouped 0x08, `W:capability.go:344-349` | 0x02/0x04 | ondaDB lacks 0x08 |
| Kinds in SST | 1-4 in the point stream; 5 only in the range block; `Decodable` = 1..5, `W:capability.go:188-194` | same, `O:src/format.rs:297-310` | identical |
| Prefix-delta block | header `0xff | 1 | keyEnc=1 | count`. Entry order: **`shared | suffixLen | suffix | kind | mods | valLen | seq | ttl? | val\|ptr`**. Trailer `restart u32 × R | R u32 | magic "WDBL" u32` (`0x5744424c` LE). Default interval **16**. `W:internal/sst/delta.go:15-52,106-118,137-168` | no header. Entry order: **`kind | mods | shared | suffixLen | valLen | seq | ttl? | suffix | val\|ptr`**. Trailer `restart u32 × R | R u32`, **no magic**. Default interval **8**. `O:mod.rs:489-539`, `writer.rs:485-496`, `mod.rs:102` | **DIFFERS in 3 ways**: field order, WDBL magic, header. ondaDB does not use "WDBL" anywhere (grep is empty). |

### 1.3 Footer (last 64 bytes)

| Bytes | wavesdb (`W:writer.go:675-697`, `reader.go:376-423`) | ondaDB (`O:writer.rs:680-701`, `reader.rs:287-366`) | Status |
|---|---|---|---|
| 0..16 | index handle off,len u64 LE | same | identical |
| 16..32 | bloom handle | same | identical |
| 32..40 | numEntries | same | identical |
| 40..48 | maxSeq | same | identical |
| 48 flags 0x01 | HasBloom | FOOTER_HAS_BLOOM | identical |
| 48 flags 0x02 | BTree index | FOOTER_BTREE | identical |
| 48 flags 0x04 | **Extended** (≥1 extended/delta block; must equal `caps & CapExtendedRecords`), `sst.go:90` | **RESTARTS** (blocks carry a restart trailer), `mod.rs:52` | **CONFLICT** |
| 48 flags 0x08 | **RangeBlock** (16-byte handle precedes the footer), `sst.go:98` | **VLOG_V2** (vlog frames have an alg byte), `mod.rs:57` | **CONFLICT** |
| 48 flags 0x10/0x20 | unassigned (rejected) | EXTENDED_BLOCK / PREFIX_DELTA | ondaDB-only |
| 49..56 | **table capability word**, uvarint (≤7 B), trailing bytes must be 0. `sst.go:101-106`, `writer.go:683-686`, `reader.go:389-402`. Carries CapExtendedRecords, CapPrefixDeltaBlocks, CapMergeOperands, CapVlogGrouping, CapRangeDeletes (`writer.go:621-638`) | unused/zero (`mod.rs:97-99`); ondaDB never reads it | **missing in ondaDB** |
| 56..64 | magic `WAVESST2` | magic `WAVESST1` | **DIFFERS; ondaDB rejects** |
| Pre-footer 16 B | only when flag 0x08: `rangeOff u64 | rangeLen u64`; len must be > 0 (`writer.go:690-696`, `reader.go:406-423`) | whenever EXTENDED (0x10): `aux_off | aux_len`, both may be 0 (`writer.rs:691-700`, `reader.rs:324-343`) | **DIFFERS in trigger** |

A default wavesdb table has footer flags `0x01` and caps 0. Even with the magic check relaxed, ondaDB
would treat it as "no restarts, legacy vlog frames". That is wrong: every wavesdb value frame is
v2-shaped, and the pointer is 16 bytes. **The magic must be the discriminator that selects a whole
wavesdb decoding profile.**

### 1.4 Meta blocks

| Item | wavesdb | ondaDB | Status |
|---|---|---|---|
| Flat index | `minKeyLen uv | minKey | n uv | (klen uv | key | seq uv | off uv | len uv)×n`, `W:writer.go:487-500` | same, `O:writer.rs:540-554` | **identical** |
| Index compression | **compressed with the table's data codec** (e.g. tag 6 at L1, zstd at L2+), `W:writer.go:652` | always None, `O:writer.rs:527-529`; the reader decompresses generically, `reader.rs:1151-1160` | readable once tags 2/4/6 map correctly |
| Separators | shortest separator with seq 0 when shortened, bytewise only, `W:writer.go:429-467` | same rule, `O:writer.rs:278-296` | identical |
| B+tree | leaf `1 | n | (klen|key|seq|off|len)×n`; internal `0 | [minKey if root] | n | (seplen|sep|off|len)×n`; fanout 256. `W:writer.go:511-581`, `sst.go:113` | same, `O:writer.rs:562-615` | **identical** (nodes compressed with the data codec in wavesdb) |
| Bloom encoding | **`tag=1 (leading) | m uv | k uv | words u64 LE`**; k ≤ 30; m < 2^32. `W:bloom.go:57,95-141`; stored with alg None (`writer.go:611`) | **`m uv | k uv | words | tag (trailing)`**; missing tag means FNV. `O:src/bloom.rs:140-176,231-248` | **DIFFERS**: tag position. ondaDB would parse tag 0x01 as m=1, which gives garbage or false negatives. |
| Bloom hash | xxh3-64, seed 0 (`zeebo/xxh3` v1.1.0), over the **user key**, `W:bloom.go:62`, `internal/hash/hash.go:11` | `xxhash_rust::xxh3::xxh3_64` over the user key, `O:bloom.rs:65-67,93` | **identical** (probe: 14 lengths 0..5000, all equal) |
| Bloom probing | `h1=uint32(h)`, `h2=uint32(h>>32)`, `bit=(h1+i*h2) % uint32(m)`, bits LSB-first in u64 words, `W:bloom.go:68-91` | same, `O:bloom.rs:99-104` | **identical** |
| Range-delete block | `0xff | schema uv=1 | count uv | (slen|start|elen|end|nseq|seq…)`, minimal uvarints, `W:rangedel.go:36-38,66-120` | aux container `nsections uv | tag=1 | len uv | payload`, where payload = `count | (slen|start|elen|end|nseq|seq…)`, `O:src/sst/mod.rs:764-822`, `range_tombstone.rs:556-580` | **body identical, envelope differs** |
| Properties / metadata block | none; stats live in the footer and the manifest (`SSTMeta.Caps`) | none | n/a |

### 1.5 Keys, sequence, TTL, comparator, unified prefix

| Item | wavesdb | ondaDB | Status |
|---|---|---|---|
| Key in SST | plain user key; seq is a separate uvarint field. No inverted-seq trailer in the SST. `W:sst.go:18-22`. (`format.MakeInternalKey` = `userKey || BE(^seq)` exists for memtable/iterator use only, `W:internal/format/format.go:93-124`) | same, `O:mod.rs:12-17` | identical |
| Order | user key ascending (comparator), seq descending | same (`cmp_internal`, `O:mod.rs:825-833`) | identical |
| Comparator names | "memcmp" default, "reverse", … (`W:comparator.go:33-36`) | "memcmp", "reverse", "lexicographic", "uint64" (`O:src/comparator.rs:40-93`) | names match for the built-ins |
| TTL | zig-zag varint (`binary.AppendVarint`), absolute **Unix nanoseconds**, expired iff `ttl != 0 && ttl <= now`. `W:internal/encoding/encoding.go:29-31`, `format.go:74-91`, `db.go:960` | zig-zag, `now_nanos`, same predicate. `O:src/encoding.rs:112-115`, `iterator.rs:443-445`, `util.rs:108` | identical |
| Unified CF prefix | 8-byte **big-endian FNV-1a-64** of the CF name, offset basis `14695981039346656037`. Used in the shared memtable/WAL only; the flush **strips** it, so SSTs hold plain user keys. `W:unified.go:30-33,53-77,217` | 8-byte BE `cf_id`, stripped at flush, `O:src/unified.rs:1-43`. **But `OFFSET = 1469598103934665603` is missing its last digit**, so ondaDB's `cf_id` is *not* FNV-1a and ≠ wavesdb's `cfID` | SSTs unaffected. **Unified WAL cross-reading would misroute every record** (out of scope here, but a real bug) |

---

## 2. Values: wavesdb blob files vs ondaDB per-table vlog

### 2.1 Model

| Item | wavesdb | ondaDB |
|---|---|---|
| Location | shared per-CF files `<cfdir>/<id>.blob`; tier variant `<stem><id>.blob`, e.g. `<object>-<blobID>.blob`. `W:internal/sst/blob.go:17-29`, `tier.go:472-479` | `<table>.vlog` next to the `.klog`, `O:mod.rs:175-181` |
| Written by | job-scoped `BlobWriter`, rolls at 128 MiB (`blob.go:123`) | the table writer |
| Separation threshold | `KlogValueThreshold` default **512 B** (`W:options.go:193`) | CF config |
| Pointer | `blobID u64 | blobOff u64` (absolute offset in that file) + `inGroupOff uvarint` if `FlagVlogGrouped`/`ModVlogGrouped` | `vlog_off u64` |

### 2.2 Blob file layout (format 2)

`W:internal/sst/sst.go:45-56, 312-486`; writer at `blob.go:355-640`.

```
header (32 B, at offset 0):
  magic u32 **BE** "WVB2" (0x57564232) | version u8 = 2 | flags u8 (0x01 HasDict, 0x02 Grouped) | reserved u16 |
  dictOff u64 LE | dictLen u32 LE | dictID u32 LE | crc32c(bytes 0..24) u32 LE | 4 B pad (zero)
value frame:  crc32c(payload) u32 | alg u8 | compLen u32 | payload            (9 B hdr, sst.go:232,254-270)
dict frame:   crc32c(dict) u32 | dictLen u32 | dict bytes (raw)              (sst.go:396-419)
group frame:  crc32c(payload) u32 | alg u8 | compLen u32 | rawLen u32 | payload   (13 B hdr, sst.go:427-486)
```

- **Format 1 blob files have no header.** Frames start at offset 0. The absolute pointer offset makes
  detection unnecessary, and a v1 file can hold neither ZstdDict nor grouped frames (`sst.go:53-56,312-320`).
- The header is written at creation with flags 0 and patched at seal if a dict or group exists (`blob.go:366,621-640`).
- The header's magic is **big-endian**, the only BE integer in these files besides the unified CF prefix.

Frame compatibility with ondaDB's vlog frame:

| Aspect | wavesdb value frame | ondaDB FOOTER_VLOG_V2 frame (`O:writer.rs:444-479`, `mod.rs:110-111`) | Status |
|---|---|---|---|
| Header | `crc u32 | alg u8 | compLen u32` | `crc u32 | alg u8 | comp_len u32` | **identical** |
| CRC | CRC32-C of stored payload | IEEE CRC of stored payload | **DIFFERS** (polynomial) |
| Store-raw rule | yes; `compLen <= rawLen` enforced (`sst.go:286`) | yes | identical |
| Per-value CRC verified at least once per handle | yes (`verifiedFrames`, `blob.go:61-119`) | yes (`vlog_verified`, `reader.rs:91-97,914-947`) | equivalent |
| Grouped frames (`FlagVlogGrouped` 0x08, `CapVlogGrouping` = `1<<8`, `W:capability.go:48`) | off by default (`BlobCompressionGroupBytes` = 0). The manifest declares the capability; the table caps word also carries it (`writer.go:631-633`) | not implemented; 0x08 rejected | **missing** |
| Dictionaries (alg 7) | **on by default** whenever the blob codec is Zstd (`W:flush.go:252-283`). Train after ≥2 MiB of samples, ≥8 values, file ≥ 16 × 64 KiB = 1 MiB (`blob.go:123-180,403-436`). Frames written after training are tagged 7. | not implemented | **missing; hit by default data** |

`origin/feat/blob-compression-dictionaries` (`bccf5b8`, 2026-08-20) **is merged into main**: merge `6ce44e1`,
`git merge-base --is-ancestor` = true. It ships in v0.8.6.

### 2.3 What ondaDB must do to resolve one wavesdb blob pointer

1. Decode the 16-byte pointer (+ uvarint when grouped) from any of the three block encodings.
2. Map `blobID` → path. `<cfdir>/<blobID>.blob` normally, or `<stem><blobID>.blob` for tier-published
   tables (`W:reader.go:265,1079`). This needs a blob-file registry, since blob files are shared across
   tables and outlive any one of them. wavesdb's manifest tracks them, and a table's `BlobBytes` map
   lists the ids it references.
3. Ungrouped: speculative pread of `rawLen + 9` at `blobOff`, then check the alg tag:
   - alg 7 → load the dictionary first (step 5);
   - verify CRC32-C, decompress, and check the result is `rawLen` bytes (`W:reader.go:1219-1300`, `sst.go:279-310`).
4. Grouped:
   - Read the 13-byte group header at `blobOff`.
   - Bound `rawLen ≤ MaxBlobGroupBytes` (64 MiB, `blob.go:180`) and require `compLen ≤ rawLen`.
   - Read `compLen`, verify CRC32-C and decompress (alg may be 7).
   - Slice `[inGroupOff, inGroupOff + valueRawLen)` (`W:reader.go:1328-1395`).
   - Cache the decompressed group, otherwise each member read pays the whole group.
5. Dictionary (alg 7):
   - Read the 32-byte header. Check magic, version and header CRC, and require `flags & 0x01`.
   - Read the dict frame at `dictOff`. Its length must equal the header's `dictLen` and be ≤ 64 MiB. Verify the dict CRC32-C.
   - Build a zstd decoder with that dictionary, e.g. `zstd::bulk::Decompressor::with_dictionary`.
   - The dictionary is a standard zstd dictionary built by klauspost `zstd.BuildDict` with ID 1 and repeat offsets {1,4,8} (`W:internal/compress/dict.go:93-104,162-201,217-231`). libzstd should accept it. **Needs a golden-file test.**
   - Keep one decoder per open blob file.

---

## 3. Compression codec ids: the truth from code

### wavesdb v0.8.6

From `W:internal/compress/compress.go`.

| id | Write | Read |
|---|---|---|
| 0 None | raw | raw |
| 1 Snappy | `s2.EncodeSnappy` (standard snappy block) | `s2.Decode` |
| 2 "LZ4" | **never written**; `Canonical` maps option 2 → 6 (`:79-86`) | **Zstd** (`:210-213`) |
| 3 Zstd | klauspost `EncodeAll` (standard frame) | zstd |
| 4 "LZ4Fast" | **never written**; `Canonical` 4 → 6 | **Zstd** |
| 5 Flate | Go `compress/flate` (raw DEFLATE, no zlib header) | raw inflate |
| 6 LZ4Native | `pierrec/lz4/v4` `Compressor.CompressBlock` = **raw LZ4 block, no size prefix, no frame** (`:128-154`) | `lz4.UncompressBlock(src, out[rawLen])` (`:221-227`) |
| 7 ZstdDict | zstd frame against the per-blob-file dict (blob values/groups only) | needs the dict (`dict.go:290-330`) |
| 8 Brotli | `andybalholm/brotli`, window 2^24, quality 6 by default (`dict.go:332-372`) | brotli stream |

- History: before v0.4.0 (`ebc7b7e`, 2026-08-15, "native LZ4 block codec — new tags, legacy lz4-tagged zstd
  still decodes"), wavesdb had no LZ4 dependency and wrote **Zstd frames under ids 2 and 4**.
- The pre-change source reads `LZ4 Algorithm = 2 // mapped to Zstd`.

### ondaDB

`O:src/config.rs:12-19,46-56`, `O:src/compress.rs:22-67`.

| id | Codec |
|---|---|
| 0 | none |
| 1 | snap::raw (standard snappy) |
| 2 | **lz4_flex::compress** = raw LZ4 block, no size prefix |
| 3 | zstd (libzstd) level 3 |
| 4 | same as 2 |
| 5 | flate2 DeflateEncoder (raw DEFLATE) |
| 6+ | `from_u8` returns None → `Corruption("bad algorithm")` |

### Verdict on `O:docs/format-registry.md:84-104`

- The row "0–5 … agree" is **wrong for ids 2 and 4**.
  - The *name* matches, but the *on-disk meaning* does not: wavesdb reads 2/4 as Zstd, ondaDB as LZ4.
  - Both directions misread: they fail with a decode error rather than returning silent garbage, because zstd frames have a magic and LZ4 decoding of a zstd frame fails.
  - Only 0, 1, 3 and 5 truly agree.
- The row "6 = LZ4 native, reserved to wavesdb" is correct as a reservation. Its implication is missed: **ondaDB's own
  id 2 bytes are exactly wavesdb's id 6 bytes.**
  - Probe: pierrec `UncompressBlock` decoded an `lz4_flex::compress` block, and `lz4_flex::decompress` decoded a pierrec `CompressBlock` block. Both round-tripped exactly.
  - `lz4_flex::compress_prepend_size` would add a 4-byte length; ondaDB does not use it.
- "Both engines default to codecs in the shared range" is **false** for wavesdb v0.8.6:
  - L0 None, L1 **tag 6**, L2+ Zstd (3), per `W:options.go:1600-1605`.
  - Klog index and B+tree blocks are compressed with the same per-level codec.
  - Blob values use the deepest codec, Zstd, and switch to **tag 7** once a dict trains. `BlobZstdLevel` defaults to 7 (`options.go:1606`).
  - Note: `W:docs/ondadb-comparison.md:175` says the default is `{None, LZ4}`. That is stale; the code has a third Zstd entry.

What ondaDB needs, per tag, when reading a wavesdb file:

- 0, 1, 3, 5: already fine.
- 2 and 4: **Zstd, in a wavesdb-origin file only**. The mapping must be chosen by file origin (magic), not globally.
- 6: `lz4_flex::decompress(src, rawLen)`, which works as-is.
- 7: zstd with the dictionary; blob files only. It could in principle appear in klog blocks, but `frameBlock`
  passes a nil dict, so klog blocks never carry it (`W:sst.go:177`, `compress.go:176-182`).
- 8: needs a brotli decoder crate. Only appears if a user configured it.

---

## 4. Checksums and hashes summary

| Use | wavesdb | ondaDB |
|---|---|---|
| Block frames, vlog/blob value frames, group frames, dict frame, blob header | CRC32-C | CRC-32 IEEE (mislabelled as CRC32-C) |
| Bloom | xxh3-64 seed 0 (tag 1, leading) | xxh3-64 seed 0 (tag 1, trailing) or legacy FNV (with a non-standard offset constant) |
| Blob-frame memo slot hash | Fibonacci multiply (in memory only) | n/a |
| xxh32 | not in SST | `encoding::xxhash32` exists (`O:encoding.rs:19-23`), not used in SST |

xxh3 appears only in blooms. No blob or frame structure uses xxh3 in either engine.

---

## 5. Work items and rough effort (wavesdb-read path only)

| # | Item | Effort |
|---|---|---|
| 1 | CRC32-C for wavesdb-origin files, e.g. via the `crc32c` crate, selected per file profile. Separately decide whether to fix ondaDB's own IEEE-vs-C mismatch. That needs a capability bit or a per-file flag plus a dual-verify migration, and it touches WAL, manifest, SST and vlog. | 0.5 d for the read path; 3-5 d for ondaDB's own format fix |
| 2 | Magic-dispatched "wavesdb profile" in `Reader::open`: WAVESST2, footer bits 0x04 = extended and 0x08 = range, caps word at 49..56 (reject unknown bits), range pre-footer only when 0x08 | 1 d |
| 3 | Per-block dispatch on `0xff` header (schema 1, keyEnc 0/1, padded count); legacy/extended entry decode with 16 B pointer + grouped uvarint; accept flag/mod 0x08 | 1.5-2 d (DecEntry grows; watch the ~5 % scan cost noted at `O:mod.rs:266-273`) |
| 4 | wavesdb prefix-delta decoder: different field order, `WDBL` trailer magic, interval-agnostic | 1-1.5 d |
| 5 | Bloom decoder with leading tag; range block `0xff|1|count` envelope | 0.5 d |
| 6 | Codec map for wavesdb files: 2/4 → zstd, 6 → lz4_flex, 8 → brotli (new dependency); rawLen bomb guard | 0.5 d |
| 7 | Blob resolution: id → path registry (plain and stem layouts), v2 header, v1 headerless, value frames, group frames with a group cache, per-file zstd dictionary (alg 7) | 3-4 d |
| 8 | Golden fixtures generated by wavesdb v0.8.6: legacy/extended/delta/btree/range tables, L1 lz4 tag 6, L2 zstd, blob v2 with dict, grouped blob; plus a tags 2/4 legacy file from pre-v0.4 | 1.5-2 d |

Total ≈ 9-12 engineer-days for read-only SST+blob compatibility. That excludes manifest/catalog
reading of a wavesdb DB directory (blob registry, CF ids, caps word v4) and excludes ondaDB's own
CRC migration.

---

## 6. Doc corrections this implies (not made; read-only task)

- `O:docs/format-registry.md`:
  - Lines 3-4, "same WAL frame, same SST block frame": false because of the CRC polynomial.
  - Line 91, "0–5 agree": false for ids 2 and 4.
  - Lines 103-104, "both default to codecs in the shared range": false.
- `O:src/encoding.rs:2-11` and AGENTS.md invariant 4: the code computes CRC-32/IEEE, not CRC32-C.
- `O:src/sst/mod.rs:42` says the magic is "WAVESST1-derived". wavesdb has since moved to WAVESST2 and rejects WAVESST1 at open (`W:sst.go:77-80`).
- `O:src/unified.rs:28` and `O:src/bloom.rs:48`: the FNV offset basis `1469598103934665603` should be `14695981039346656037`. Changing it now is itself a format change for ondaDB's unified WAL and FNV blooms.
- `W:internal/sst/sst.go:16`: the frame comment omits the leading `alg` byte.
- `W:docs/ondadb-comparison.md:175`: the default per-level list omits Zstd.
