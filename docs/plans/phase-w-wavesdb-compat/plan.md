# Phase W — reading wavesdb 0.8.6 databases and checkpoints

> **For agentic workers:** each numbered item (W0.1, W2.3, …) is the unit of
> work. Derive a task-by-task plan from it, write the failing test first, and
> keep both feature configurations green (AGENTS.md "Build, test, verify").
> The `research/` directory holds the byte-level evidence this plan was built
> from; cite it, don't re-derive it.

**Baseline:** ondaDB `0f4ebc6` (Cargo `0.9.0`). wavesdb `v0.8.6` = `a26fb4c`
(2026-09-17). Research date 2026-09-23.

**Goal:** ondaDB opens a wavesdb 0.8.6 **checkpoint**, both the local
`Checkpoint(dir)` output and the `CheckpointToObjectStore` output, and serves
the same reads wavesdb would. Stretch goals: import one into a native ondaDB
database, then open a *live* wavesdb directory.

**Evidence:** the four reports under [`research/`](research/) carry `file:line`
citations on both sides:

- [`sst-and-blob.md`](research/sst-and-blob.md): klog, blocks, footer, bloom, blob files, codecs, checksums
- [`catalog-and-checkpoints.md`](research/catalog-and-checkpoints.md): MANIFEST v2/v3/v4, MANIFEST-EDITS, directory layout, JSON CF config, checkpoints, object store
- [`wal-and-recovery.md`](research/wal-and-recovery.md): WAL frames, envelopes, control kinds, recovery semantics
- [`feature-inventory.md`](research/feature-inventory.md): every wavesdb change `1a052a4..v0.8.6` and its ondaDB status

---

## 1. What the research found

### 1.1 Today ondaDB reads no wavesdb byte

The cross-engine registry ([`../../format-registry.md`](../../format-registry.md))
aligned the **numbers**: capability bits 0–6, record kinds, entry flags and
codec ids 0–5. The **containers** those numbers live in were never compared.
The list below is ordered by how early each item blocks a read.

| # | Incompatibility | Where | Consequence |
|---|---|---|---|
| 1 | **Checksum polynomial.** ondaDB's `encoding::checksum` is documented as CRC32-C but calls `crc32fast`, which computes **CRC-32/IEEE**. wavesdb uses Castagnoli. | `src/encoding.rs:11-17`; W `internal/encoding/encoding.go:15` | Every wavesdb manifest, block, WAL frame, blob frame and edit record fails ondaDB's CRC check. Verified: the check value for `"123456789"` is `cbf43926` (IEEE), not `e3069283`. Every ondaDB fixture carries IEEE CRCs. |
| 2 | **Manifest.** Both engines use magic `WVMF`, but the version numbers collide. wavesdb has v2, v3 and v4 (flag-word sections, uvarint caps, blob maps). ondaDB has v1 and v2 (`ONDA*` tagged tails). | `src/manifest.rs:560`; W `internal/manifest/manifest.go:594-630` | A wavesdb v2 manifest would pass ondaDB's version check and then be misparsed. Only the CRC mismatch stops it today. |
| 3 | **Per-CF config blob.** wavesdb stores JSON; ondaDB stores positional binary. | W `config.go:8-65`; `src/config.rs:1451` | The open fails with "unknown comparator {…". This fails closed, by accident. |
| 4 | **CF directory name.** wavesdb uses `cf_<name>`, ondaDB uses `cf-<name>`. | W `db.go:2069`; `src/db.rs:1424` and about 5 other sites | ondaDB finds no tables. |
| 5 | **SST footer magic.** wavesdb writes `WAVESST2`; ondaDB expects `WAVESST1`. | `src/sst/mod.rs:43`, `reader.rs:289`; W `internal/sst/sst.go:80` | ondaDB rejects every wavesdb table. |
| 6 | **Footer flag bits collide.** In wavesdb `0x04` = Extended and `0x08` = RangeBlock. In ondaDB `0x04` = Restarts and `0x08` = VlogV2. wavesdb also has a capability word at footer bytes 49–55. | `research/sst-and-blob.md` §1.3 | Once the magic check is relaxed, ondaDB would read the flags with the wrong meanings. |
| 7 | **Block encodings.** wavesdb chooses the encoding **per block**, from a `0xff\|schema\|keyEnc\|count` header. ondaDB chooses it per table, from the footer. wavesdb prefix-delta entries use a different field order and end with a `WDBL` trailer magic. | §1.2 | Extended and delta blocks are unreadable. |
| 8 | **Value storage.** wavesdb puts a 16-byte `(blobID, off)` pointer into **shared** `<id>.blob` files. Those files have a v2 header, per-file zstd dictionaries and optional group frames. ondaDB stores an 8-byte offset into a per-table `.vlog`. | §2 | Every value above 512 B (the wavesdb default threshold) is unreadable. |
| 9 | **Codec ids.** In wavesdb, ids 2 and 4 **decode as Zstd** (legacy tags, never written). ondaDB writes LZ4 under them. wavesdb's defaults are L1 = tag 6 (native LZ4) and L2+ = Zstd. Blob values use tag 7 (zstd with a trained dictionary) **by default**. | §3 | A default wavesdb database needs tags 3, 6 and 7. ondaDB's registry row "0–5 agree" is wrong. |
| 10 | **Bloom filter.** The hash math is identical (xxh3-64, same probing). The tag byte **leads** in wavesdb and **trails** in ondaDB. | §1.4 | If misparsed, a read can get false negatives, i.e. miss a key that exists. |
| 11 | **Range-delete block.** The fragment body is identical; the envelope differs (`0xff\|1\|count` vs ondaDB's aux-section container). | §1.4 | Small shim. |
| 12 | **WAL envelope.** Schema 1 has a different layout in each engine. wavesdb has an `envelopeFlags` field and a per-record `cfID`. Control frames and prepare bodies differ entirely. | `research/wal-and-recovery.md` | Affects only live directories; **checkpoints contain no WAL**. |
| 13 | **MANIFEST-EDITS.** Headers differ (`WDME` vs `ONDE`), and only op codes 1–3 mean the same in both engines. | catalog §1.4 | Affects only live directories; checkpoints carry no edit log. |

What is already identical:
- the block-frame layout (the CRC aside);
- the legacy entry encoding and the flag byte (apart from `0x08`);
- the sequence number as a separate uvarint;
- zig-zag TTL in absolute Unix nanoseconds;
- the flat and B+tree index encodings;
- the value-frame header `crc|alg|compLen`;
- comparator names;
- snappy, zstd and deflate;
- the raw LZ4 block format (ondaDB tag 2 bytes are wavesdb tag 6 bytes; both directions were cross-decoded);
- `GlobalSeq` semantics;
- name-keyed column families;
- the object-store resolution rule `root/<object>.klog`.

### 1.2 What a wavesdb 0.8.6 checkpoint contains (the target)

`Checkpoint` (W `maintenance.go:441-532`) flushes first. It copies **no WAL, no
MANIFEST-EDITS and no LOCK file**, strips the instance nonce, and lowers
`GlobalSeq` to the highest table `MaxSeq`. It refuses to run while prepared
transactions are unresolved or while any table sits on a non-default tier.

```
<ckpt>/MANIFEST                 WVMF v2 (default) | v3 (unified/partitions/tier rules) | v4 (any capability), CRC32-C
<ckpt>/cf_<name>/<id>.klog      WAVESST2 tables
<ckpt>/cf_<name>/<blobID>.blob  shared blob files (header "WVB2" v2, or headerless v1)
```

`CheckpointToObjectStore` writes the same bytes to
`<prefix>/cf_<cf>/<id>.{klog,blob}` and uploads `<prefix>/MANIFEST` **last** as
the commit marker. It creates no pointer or generation objects. Incremental
checkpoints rewrite the MANIFEST in place. v0.8.1 (`bc98349`) also admits
checkpoints taken from an `OpenReadOnlyNoLock` database that has fully
materialised its data.

A checkpoint is therefore the smallest useful target. It needs the catalog,
the tables and the blob files, and none of the WAL, edit-log or prepared-state
machinery. The waves below are ordered on that basis.

### 1.3 Shared bugs found along the way (independent of compatibility)

The feature inventory checked each wavesdb fix after `1a052a4` against ondaDB.
One suspected bug is **confirmed by reading the code**: in
`src/compaction.rs`, `smallest_input` (`:1329`) skips any `!valid()` iterator.
A `SstIterator` that hits an error reports `!valid()` (`src/sst/iter.rs:318`),
and nothing checks `err()` before `outputs.finish()` (`:1655`). A CRC or I/O
error in the middle of a merge therefore makes that input look exhausted. The
truncated output is installed and the inputs are deleted, which is **data
loss**. The other items are suspected until a repro test exists. See wave S.

---

## 2. Decisions

| # | Decision | Recommendation | Why |
|---|---|---|---|
| D1 | Should ondaDB **read wavesdb's format as a foreign profile**, or **converge its own native format** onto wavesdb's (CRC32-C, WAVESST2, 16-byte pointers, shared blob files)? | **Read-only foreign profile.** Convergence is a separate, later decision (W8). | The goal is to read wavesdb's files. Convergence rewrites ondaDB's write path, blob model and GC, and still needs a migration for every existing ondaDB database. A profile touches only reader code, and the footer magic and manifest CRC already discriminate unambiguously. |
| D2 | ondaDB's checksum mislabel | **Fix the documentation now** (AGENTS.md invariant 4, `encoding.rs` doc comment, `formats.md`, `format-registry.md`). **Do not** change the native polynomial. Add `checksum_crc32c` beside it. | Changing `checksum()` would make every existing ondaDB file unreadable. IEEE is not a durability weakness; only the claim is wrong. |
| D3 | Unit of dispatch | **Per artifact, from self-description.** Tables dispatch on the footer magic, manifests on which CRC verifies, blob files on the `WVB2` header (or on being reached through a wavesdb table). Never dispatch on a global "this is a wavesdb DB" flag alone. | This follows the existing format rule: "self-description lives at the artifact that needs it" (`phase-2-formats/plan.md` § Format rules). Mounting and attaching mix origins. |
| D4 | Write access to a wavesdb catalog | **Read-only until W6.** A read-write open of a wavesdb directory is refused with `UnsupportedFormat`. | The alternative is writing a wavesdb manifest (a v2/v3/v4 encoder plus blob GC) or silently converting the catalog. Both are one-way and out of scope. |
| D5 | wavesdb-only capabilities | Bit 8 (grouping): **read** (W3.4, small). Bit 7 (managed mode): **read-only**, honouring `DiscardSeq` as a read floor (W4.6). Kind 19 (spill): **refuse**. Codec 8 (brotli): **read**, via a pure-Rust decoder. | Grouping, dictionaries and brotli are reader-only and small. Spill would need a whole transaction subsystem that no checkpoint contains. |
| D6 | Fixture source | **Generate from wavesdb v0.8.6 itself**, with a pinned Go generator committed under `tools/wavesdb-fixtures/`. Hand-built bytes are allowed only for corruption rows. | Only the real writer shows us the defaults: trained dictionaries, per-level codecs, padded counts. |

---

## 3. Waves

Effort is for one engineer and approximate. **W0–W5 make up the checkpoint
goal: about 5–6 weeks.** Each wave is a separate review and a separate revert.

```
S (shared fixes) ─────────────────────────────────── independent, do first
W0 truth + corpus ─► W1 primitives ─► W2 SST profile ─┐
                                   └► W3 blob files ──┼─► W4 catalog + open ─► W5 object store
                                                      │                     └─► W6 import
                                                      └──────────────────────► W7 live dirs (optional)
W8 convergence: decision only
```

### Wave S — shared correctness fixes (≈1 wk, independent)

Every item starts with a failing regression test in `tests/` and follows the
`engine_regressions.rs` pattern.

- **S1 (confirmed, data loss): a compaction input error must fail the job.**
  After the merge loop, and in `smallest_input`'s caller, check `err()` on
  every input iterator. Abort with the error before `outputs.finish()` and
  before `catalog_txn`. Audit every other `SstIterator` consumer for the same
  pattern: flush-merge, excise, the ingest verify, parts. The test injects a
  corrupt block into an input table of a compaction, then asserts the job
  errors, the inputs survive and a reopen reads every key. wavesdb reference:
  `eacc833`.
- **S2 (suspected): a size-triggered output cut splits one key's versions
  across two tables.** Point reads check only the first table
  (`column_family.rs:1427`), so a snapshot read of an older version can miss
  it. Reproduce with a small `target_file_size` and many versions of one key
  under a held snapshot. Fix: never cut between versions of one user key.
  wavesdb reference: `fc32015`.
- **S3 (suspected): a range delete does not conflict with a Serializable
  transaction's point reads** (`txn.rs:1313`). Repro, then fix or document it
  under the existing Serializable caveat. wavesdb reference: `3591223`.
- **S4 (suspected): a checkpoint or backup of a read-only database silently
  drops WAL-only data**, because a read-only open starts no flush worker
  (`maintenance.rs:260`). Fix: refuse, as wavesdb's `checkpointSourceReady`
  does, unless the memtables are empty.
- **S5 (perf, optional):** periodic compaction runs its whole backlog at once
  (`compaction.rs:151`). Add a burst limit. wavesdb reference: `887f8ad`.

### Wave W0 — documentation truth and the golden corpus (≈4 d)

- **W0.1 Correct the documents.** These documents are wrong today:
  - AGENTS.md invariant 4 and `src/encoding.rs:1-11` both say "CRC32-C". The code computes CRC-32/IEEE.
  - `docs/formats.md`: the same checksum wording wherever it appears.
  - `docs/format-registry.md`:
    - "same WAL frame, the same SST block frame" is false (see §1.1).
    - The codec table says "0–5 agree". Split the row: 0, 1, 3 and 5 agree; wavesdb decodes 2 and 4 as Zstd; wavesdb tag 6 is byte-identical to ondaDB tag 2.
    - "both engines default to shared-range codecs" is false.
    - Add the collisions the registry never covered: footer magic, footer flags `0x04`/`0x08`, manifest version numbers, the per-block header, the prefix-delta field order, the bloom tag position, and the blob-file format.
  - `docs/wavesdb-feature-assessment.md:14-15`.

  Also record the FNV offset constant typo (`1469598103934665603`, missing
  its last digit) in `src/unified.rs:29` and `src/bloom.rs:48`. It is
  persisted in unified WAL key prefixes and legacy FNV blooms, so it is **a
  format value now and must not be "fixed"**. For wavesdb WAL routing (W7),
  use the real FNV basis or the manifest `UnifiedID`.
- **W0.2 Fixture generator.** Add `tools/wavesdb-fixtures/`: a Go module
  whose `go.mod` has `replace github.com/yannick/wavesdb => ../../../wavesdb`,
  with a `go:generate` guard that asserts the source is at `v0.8.6`
  (`a26fb4c`). It emits
  `tests/fixtures/wavesdb-0.8.6/<scenario>/{ckpt/…, expected.jsonl, meta.json}`.
  `expected.jsonl` is the oracle: every visible `(cf, key, value|tombstone,
  ttl)` read back through wavesdb's own iterator at the checkpoint sequence.
  `meta.json` records the manifest version, the capability word, and the
  codec tags and flags per table, so ondaDB tests can assert that the
  scenario actually covers what it claims to.
- **W0.3 Scenarios.** Each scenario must be large enough to trigger the
  feature it names:

  | Scenario | Exercises |
  |---|---|
  | `default-small` | v2 manifest, inline values, L0 uncompressed |
  | `default-leveled` | forced compaction to L2: tag 6 at L1, zstd at L2, compressed index blocks |
  | `blobs-dict` | values ≥ 512 B, > 2 MiB of samples, a blob file ≥ 1 MiB: header v2, tag 7, dict frame |
  | `blobs-v1` | a headerless blob file (dictionary disabled, or an older-format path) |
  | `blobs-grouped` | `BlobCompressionGroupBytes` > 0: capability bit 8, flag `0x08`, group frames |
  | `btree-index` | B+tree index blocks |
  | `ttl-singledelete` | TTL and single-delete entries |
  | `comparators` | reverse, uint64 and int64 families |
  | `merge` | capability bits 0 and 1, extended blocks, kind 4, merge statistics |
  | `range-delete` | capability bit 2, footer `0x08`, pre-footer handle, range block |
  | `prefix-delta` | capability bit 3, delta blocks, `WDBL` trailer |
  | `periodic-age` | capability bit 5, per-table `LastCompactionTime` |
  | `edit-log` | capability bit 4 checkpoint (the fresh watermark) |
  | `unified` | v3 manifest, the WAL layout flag, a renamed CF with an explicit `UnifiedID` |
  | `partitions` | v3 manifest, partition rules and `MaxEntryTime` |
  | `managed` | capability bit 7, `DiscardSeq` |
  | `brotli` | a codec-8 family |
  | `objstore` | `CheckpointToObjectStore` onto the FS object store: a byte-identical tree with a key prefix |
  | `immutable-ro` | a checkpoint taken from `OpenReadOnlyNoLock` |

  Commit the corpus only if it stays small (< 20 MiB total). Otherwise
  commit the generator and a checksummed manifest of the fixtures, and let CI
  generate them. Tests skip with a loud message when Go is absent, never
  silently (the same rule as `ONDADB_S3_ENDPOINT`).
- **W0.4 Corruption corpus.** From each generated fixture, derive bit-flip,
  truncation and wrong-magic variants for the manifest, block, blob header,
  dict frame and group frame. Every one must be rejected, never misread.

### Wave W1 — primitives (≈2 d)

- **W1.1 CRC32-C.** Add `encoding::checksum_crc32c`, using the `crc32c` crate
  (hardware-accelerated on aarch64 and x86-64). Pin it with check-value
  tests: `e3069283` for C and `cbf43926` for IEEE.
- **W1.2 `FormatProfile { Onda, Wavesdb }`.** A small `Copy` enum that
  carries the per-origin choices: the checksum function, the codec map, the
  footer-flag interpretation, the pointer width, the bloom tag position and
  the delta layout. Branch on it once per table or file, **never per entry**
  (invariant 8's scan budget; see `docs/performance.md`).
- **W1.3 Codec map by profile.**
  - Under `Wavesdb`: 2 and 4 → zstd decode; 6 → `lz4_flex::decompress(src, raw_len)`; 7 → zstd with a dictionary (a handle supplied by W3); 8 → `brotli-decompressor`.
  - Under `Onda`: unchanged.
  - Add wavesdb's decompression-bomb guard (`raw_len ≤ comp_len·1000 + 1 MiB`) to both profiles.
  - Change an unknown codec from `Corruption` to `UnsupportedFormat` where the id is registry-reserved.
  - New dependencies (`crc32c`, `brotli-decompressor`, `serde_json`) go behind a default-on `wavesdb-compat` cargo feature, so a minimal build can drop them. **Both feature configurations must build with and without it.**

### Wave W2 — WAVESST2 table reader (≈1–1.5 wk)

Everything here is read-only and is selected in `Reader::open` by the footer
magic.

- **W2.1 Footer.** Accept `WAVESST2` → `FormatProfile::Wavesdb`. Decode the
  flags with wavesdb meanings: `0x01` bloom, `0x02` btree, `0x04` extended,
  `0x08` range block. Decode the uvarint capability word at bytes 49–55,
  enforce zero padding, refuse unknown bits with `UnsupportedFormat`, and
  check that `0x04 ⇔ CapExtendedRecords`. Read the 16-byte pre-footer handle
  **only** when `0x08` is set, and require its length to be > 0. `WAVESST1`
  keeps its ondaDB meaning; there is no ambiguity, because wavesdb rejects
  `WAVESST1`.
- **W2.2 Per-block dispatch.** In the wavesdb profile, a data block whose
  first byte is `0xff` carries the header `schema(=1) | keyEnc(0|1) |
  count(padded or minimal uvarint)`. Otherwise it is a legacy block. Entry
  decoding:
  - Legacy and full-key extended entries reuse ondaDB's entry decoder, with
    a 16-byte pointer (plus a uvarint in-group offset when flag or modifier
    `0x08` is set).
  - `DecEntry` gains a `ValueRef` variant, `Blob { id, off, in_group }`.
    Measure the effect of `DecEntry` growth on the scan benchmark (≥5 runs)
    before and after. The ~5% scan-cost note at `src/sst/mod.rs:266-273` is
    the precedent.
- **W2.3 wavesdb prefix-delta decoder.** Field order `shared | suffixLen |
  suffix | kind | mods | valLen | seq | ttl? | val|ptr`, a restart trailer
  that ends in the `WDBL` magic, and any restart interval. Port every
  validation row from W `docs/format-compatibility.md` § Prefix-delta, and
  seed a fuzzer (`cargo fuzz` target or a proptest) with the W0 corpus.
- **W2.4 Bloom with a leading tag.** Only the parse location changes; the
  hash and probing are verified identical. A regression test must assert
  **no false negatives** over every key in the fixture.
- **W2.5 Range block.** Parse the `0xff | 1 | count` envelope into the
  existing `range_tombstone::Fragment` list; the body parser is shared.
  Kind 5 in a data block stays `Corruption`.
- **W2.6 Index and B+tree blocks** are decoded through the per-level codec
  (W1.3); ondaDB's generic decompress path already covers them.
- **W2.7 Verification rule (invariant 4).** Every block is CRC32-C-verified
  at least once per open reader, through the same verified-block machinery
  and with the same "a frame that fails is never marked verified" rule.

### Wave W3 — blob files (≈1 wk)

- **W3.1 `BlobSet` per column family.** A map from blob id to a lazily opened
  handle (local path `cf_<name>/<id>.blob`, or tier stem
  `<object>-<id>.blob`). It is built from the catalog's per-CF `Blobs` list
  and per-table `BlobBytes` maps (W4). Handles go through the table cache
  bounds (`max_open_readers`, `max_reader_bytes`). Cache keys: blob ids come
  from the same wavesdb allocator as table ids, so they are unique within a
  database, but a mounted or foreign namespace must still get a distinct
  cache-id space.
- **W3.2 Header.**
  - v2: `WVB2` **big-endian** magic, version 2, flags (`0x01` dict, `0x02` grouped), dict offset/length/id, and a header CRC32-C over bytes 0..24.
  - v1: headerless; frames start at offset 0.
  - Every mismatch is `Corruption`; an unknown version is `UnsupportedFormat`.
- **W3.3 Ungrouped value read.** Read the frame at `off`: `crc | alg |
  compLen | payload`. Verify CRC32-C (per-handle verified-frame memo, as
  `vlog_verified` does), decompress through W1.3, and check the result
  length equals the entry's `valLen`.
- **W3.4 Grouped read (capability bit 8).** Read the 13-byte group header,
  bound `rawLen` to 64 MiB and require `compLen ≤ rawLen`. Verify,
  decompress, slice `[in_group, in_group + valLen)`. Cache decompressed
  groups in a small per-`BlobSet` LRU charged to the block cache, or member
  reads cost O(group).
- **W3.5 Dictionaries (tag 7).** Load the dict frame lazily at the first tag-7
  read: bounded to 64 MiB, CRC32-C-verified, length equal to the header's.
  Build one `zstd::bulk::Decompressor::with_dictionary` per open blob file.
  **Open risk:** the dictionary is built by klauspost `zstd.BuildDict` (ID 1,
  repeat offsets {1,4,8}). libzstd should accept it; the `blobs-dict` fixture
  is the gate. If it fails, fall back to a raw-content dictionary via
  `ZSTD_dictContentType_e::Raw`, with a test for both.
- **W3.6 Value cache.** Reuse the 0.5 vlog value cache (`ONDAVVC1` semantics)
  for resolved blob values.

### Wave W4 — catalog and read-only open (≈1–1.5 wk)

- **W4.1 `manifest_wavesdb.rs`.** A pure decoder for WVMF v2/v3/v4 with
  CRC32-C. Dispatch: verify the CRC as ondaDB (IEEE) first, then as wavesdb
  (C); exactly one can verify. It covers:
  - the body, including per-table `BlobBytes` maps and per-CF `Blobs` lists;
  - the v3/v4 flag section, with strict masks, ascending-bit order and the per-CF table-count cross-check;
  - the uvarint capability word;
  - the uvarint edit watermark (`next ≥ applied+1`);
  - managed state;
  - per-table caps, merge statistics and range summaries.

  Its output is ondaDB's `Manifest` plus a `WavesdbCatalogExt` side table
  (blob maps, UnifiedIDs, managed floor). Golden tests: W
  `internal/manifest/testdata/v4_caps.bin` plus the W0 corpus. The
  decoder-accepts-only-what-the-writer-produces rule applies.
- **W4.2 JSON CF config → `ColumnFamilyConfig`.** `serde_json`, unknown
  fields ignored. Hard fields:
  - `comparator` (unknown → `InvalidArgs`, not wavesdb's silent fallback to bytewise);
  - `merge_operator` (must be registered under the same name);
  - `partition_scheme`.

  Convert nanoseconds to microseconds where ondaDB stores µs. Everything
  else is advisory for a read-only open.
- **W4.3 Directory naming by origin.** Route `cf_dir` (`db.rs:1424`,
  `storage.rs:288`, `maintenance.rs:289`, `parts.rs:403/861/1052`) through
  one function of the catalog origin, so the `cf-` vs `cf_` choice lives in
  one place. The read path is the only consumer in this phase.
- **W4.4 `DB::open_wavesdb(path, WavesdbOpenOptions)`.**
  - Read-only and no-lock (a checkpoint may sit on read-only media; ondaDB's read-only open currently creates `LOCK`, `db.rs:2206`). Make the lock optional for this entry point only.
  - No WAL replay: refuse if any `wal-*.log` or `unified-wal-*.log` holding frames is present, until W7.
  - Refuse if `MANIFEST-EDITS` exists (W7).
  - Refuse capability bits the build does not implement.
  - Install `FormatProfile::Wavesdb` tables and `BlobSet`s, and set `visible_seq = GlobalSeq`.
  - Writes, flush, compaction and every catalog mutation return `ReadOnly`.
  - Iterators, `get`, `multi_get`, snapshots and `PerfContext` work unchanged.
- **W4.5 Capabilities at open.**
  - Bits 0–6: accepted.
  - Bit 8: accepted when W3.4 has landed.
  - Bit 7: W4.6.
  - Unknown bits: `UnsupportedFormat` naming the mask.
- **W4.6 Managed mode, read-only.** Honour `DiscardSeq` as the read floor.
  Snapshots below it are refused, matching wavesdb's semantics. Write paths
  stay refused.
- **W4.7 Acceptance.** For every W0 scenario, `open_wavesdb` + a full scan +
  point `get`s of every expected key + range scans with every bound
  combination equal `expected.jsonl`, in **both** feature configurations.
  Every W0.4 corruption variant is rejected, and a clean error names the
  artifact.

### Wave W5 — object-store checkpoints (≈1 wk; `s3` feature)

- **W5.1 Lazy remote open** (the pattern of wavesdb's `OpenRemoteCheckpoint`).
  1. GET `<prefix>/MANIFEST` and decode it (W4.1).
  2. Register a shared S3 tier rooted at `<prefix>`.
  3. Set each table's `tier` to that tier and its `object` to `cf_<cf>/<id>`; blob files resolve to `cf_<cf>/<blobID>.blob` through the same rule. ondaDB's `root/<object>.klog` rule already matches (`column_family.rs:706-716`).
  4. Ignore the tier and object stamps carried in the manifest (W `remote_checkpoint.go:98-101`).
  5. Seed object sizes from the manifest's `klog_size` instead of issuing a HEAD per table (wavesdb `c148bc0`).
- **W5.2 S3 authentication parity** (wavesdb v0.8.4–v0.8.6).
  - Session token: today it is hard-coded `None` at `storage_s3.rs:169`.
  - Credential chain: env, then shared file / `AWS_PROFILE`, then instance metadata / STS.
  - A named `profile` that does **not** fall back to the chain.
  - `anonymous`, which wins over everything.
  - `read_only`: no bucket probe or create, and writes are refused locally.

  rust-s3 already provides the credential constructors.
- **W5.3 Restore by download.** `restore_wavesdb_checkpoint(store, prefix,
  dir)`: GET the MANIFEST, then every klog and blob, into a local
  `cf_<cf>/…` tree, then `open_wavesdb`. The manifest is downloaded first
  but written into `dir` **last**, so an interrupted restore never looks
  complete.
- **W5.4 Child-prefix listing.** Keep the common prefixes that
  `storage_s3.rs:370-378` currently drops, and expose them so a consumer can
  enumerate checkpoint prefixes.
- **W5.5 Tests.** Run the W0 `objstore` scenario against MinIO, gated on
  `ONDADB_S3_ENDPOINT` as `tests/s3_tier.rs` already is. Cover lazy and
  restored opens for equality with the local open, and verify that a missing
  MANIFEST is reported as "no checkpoint", not as corruption.

**End of the checkpoint goal.**

### Wave W6 — import into a native ondaDB database (≈1 wk)

`DB::import_wavesdb(src, dst, opts)` streams each column family of an
`open_wavesdb` handle into native ondaDB tables. It reuses the ingest/SST
writer (and so `ONDA`-format manifests and IEEE CRCs) and preserves sequences,
TTLs, tombstones (at the bottom level they can be dropped), merge operands (or
folds them when an operator is registered), range tombstones and partition
stamps. The catalog becomes durable through `catalog_txn`, which honours
invariant 1. The result is an ordinary ondaDB database. This is the supported
path to **writing** data that started in wavesdb. An in-place "adopt" mode,
where wavesdb tables stay in the catalog next to native ones, is **not**
proposed: it would make ondaDB's manifest carry blob-file ownership, which is
the shared-blob GC model the assessment (§5) rejected.

### Wave W7 — live wavesdb directories (optional, ≈3–4 wk)

This wave is only for a consumer that needs to open a wavesdb directory that
was not checkpointed, for example one that crashed.

- **W7.1 WAL replay of data frames.** Accept a wavesdb-schema envelope only
  where it is unambiguous. The discriminator is that after `schema=1,
  count`, a `0` can only be wavesdb's `envelopeFlags`, because ondaDB never
  writes kind 0; pin that in the registry. Cross-check the per-record
  `cfID` against the prefix. Route by the manifest `UnifiedID`, else by the
  **correct** FNV-1a basis (not ondaDB's typo'd constant). Bound the frame
  length by the remaining file size before allocating; ondaDB currently
  allocates on a garbage length, and that should be fixed for the native
  profile too.
- **W7.2 Edit-log replay.** Parse the `WDME` header and ops 1–18, apply them
  on top of the W4.1 snapshot, and implement wavesdb's recovery predicate
  (`internal/manifest/recover.go`). Read-only: never compacted by ondaDB.
- **W7.3 Prepared state (kinds 16–18).** Decode wavesdb's prepare body (the
  `TestPreparedBatchGolden` layout) and its 1–128-byte ids. A **committed but
  unflushed** prepare exists only as body + decision, so skipping these
  frames would lose committed data. Until this is implemented, a wavesdb WAL
  holding any control kind is refused at open.
- **W7.4 Kind 19 (spill)** stays refused.

### Wave W8 — format convergence (decision record only)

W8 would make wavesdb able to read ondaDB. That requires ondaDB to write
CRC32-C, `WAVESST2`, wavesdb footer flags, per-block headers, 16-byte blob
pointers into shared blob files, and a JSON config. Equivalently, it means
adopting wavesdb's blob model with its GC. **Not scheduled.** Reopen only if
a consumer needs wavesdb to mount ondaDB-written parts (`attach_part_by_ref`
in the other direction). If it is ever reopened, the path is a new capability
bit (e.g. "CRC32-C native") taken from the shared registry first. No existing
ondaDB file changes meaning.

---

## 4. Invariants this phase must not bend

- **Invariant 1, 2 and 6 (durability ordering, manifest serialisation,
  deletion through `remove_sst_file`).** W1–W5 write no catalog and delete
  nothing. `open_wavesdb` must refuse every mutating API at the boundary. A
  test calls each public mutator and expects `ReadOnly`.
- **Invariant 4 (checksums).** Every wavesdb byte ondaDB interprets is
  CRC32-C-verified at least once per open reader: manifest, block, blob
  header, dict frame, value and group frame. The native profile keeps IEEE.
  The profile decides the polynomial; nothing guesses per frame, except the
  manifest dispatch in W4.1, where exactly one polynomial can verify.
- **Invariant 7 (comparators).** wavesdb comparator names map one-to-one.
  The prefix-compare trick stays gated on `is_bytewise()`, independent of
  the profile.
- **Invariant 8 (pinned blocks).** The wavesdb delta decoder produces the
  same pinned-block slices. A value resolved from a blob file is owned or
  cache-pinned, never borrowed from a transient decompress buffer.
- **Fail closed.** Unknown version, capability, codec or kind →
  `UnsupportedFormat`. Malformed bytes → `Corruption`. Nothing is read as a
  plain put "just in case" (W `docs/format-compatibility.md`).

## 5. Release gate for the checkpoint goal (W0–W5)

1. The CI-equivalent four commands are green, with and without `wavesdb-compat`.
2. Every W0 scenario matches its oracle in both feature configurations, locally and, where `ONDADB_S3_ENDPOINT` is set, over S3.
3. Every W0.4 corruption variant is rejected, and a fuzzer runs clean for ≥ 10 minutes per decoder: manifest, delta block, blob header, group frame.
4. Native-profile scan and point-get benchmarks show no regression beyond noise (≥ 5 runs, same-run ratios; `docs/performance.md`).
5. The docs are updated:
   - `docs/formats.md` gains a "wavesdb profile" section;
   - `docs/format-registry.md` covers the container-level collisions;
   - AGENTS.md gains a "Where the newer features live" row and the corrected invariant 4;
   - the wavesdb side gets a note in its `docs/ondadb-comparison.md` (a separate commit in that repo, with its owner's consent).

## 6. Risks

| Risk | Mitigation |
|---|---|
| libzstd rejects klauspost dictionaries | The W3.5 fixture gates it; fall back to a raw-content dictionary; worst case, a pure-Rust zstd decoder (`ruzstd`) that supports dictionaries, for tag 7 only. |
| wavesdb moves again (0.8.7+) while this is in flight | Pin the generator to `a26fb4c`. Any later wavesdb tag re-runs W0 and adds scenarios; a diff in `meta.json` flags new flags, codecs and capabilities. |
| `DecEntry` growth costs native scans | Keep `ValueRef` compact (an enum with niche optimisation), measure in W2.2, and revert or redesign if it exceeds noise. |
| Corpus size | Commit the generator plus hashes, not the bytes, when above 20 MiB. |
| Scope creep into W6/W7 | Each is a separate decision. The checkpoint goal closes at W5. |
