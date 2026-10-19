# Plan C — yoloDB: one format for ondaDB and wavesdb

> **Status:** proposal, 2026-09-23. Baseline ondaDB `0f4ebc6` (0.9.0), wavesdb
> `v0.8.6` (`a26fb4c`). Evidence for every divergence named here is in
> [`../phase-w-wavesdb-compat/research/`](../phase-w-wavesdb-compat/research/).
> Plan C **may break compatibility**. Plan W (read-only wavesdb 0.8.6 profile)
> becomes the "legacy wavesdb reader" of step 2 and is needed only if 0.8.6
> checkpoints must be read before wavesdb itself moves to the yoloDB format.
>
> **Update 2026-10-19:**
> - **Step 1 is built:** ondaDB 0.10.0, PR #5. It adds a preflight guard that
>   refuses to auto-upgrade a mount-point or symlinked database directory.
> - **marekvs 0.3.5** moved its database to `<data_dir>/db` for the same
>   reason.
> - **wavesdb is at v0.8.9 with no on-disk format change since `a26fb4c`,** so
>   the step-2 table below still holds. Its three runtime changes are taken as
>   0.10.x follow-ups (§1.6).
> - **Step 2 gains a precondition:** the wavesdb layout refactor (§2.3,
>   step 2-pre).

**Goal:** ondaDB and wavesdb converge on **one on-disk format family,
"yoloDB"**. Each engine may keep specialities as **optional capabilities**:
every engine reads every capability it claims to support and refuses the rest
at open. An engine may run in:

- **native mode**: the writer uses its own specialities, for example wavesdb's
  shared blob files or ondaDB's WAL stripes;
- **compat mode**: the writer restricts itself to the common profile, so any
  yoloDB engine can open the directory and continue writing it.

**Two steps:**

1. **Step 1 (ondaDB only):** move ondaDB onto a fresh yoloDB format epoch,
   covering new magics and versions and CRC32-C. Then port every wavesdb
   feature that adds function or speed.
2. **Step 2 (both engines):** define the rest of the shared format, and list
   what each engine must change to reach full compatibility.

---

## Step 1 — ondaDB becomes yoloDB format epoch 1

### 1.0 Decisions to confirm before coding

| # | Decision | Recommendation |
|---|---|---|
| C-D1 | Footer magic | **Decided (2026-09-23): `YOLOST01`.** Exactly 8 ASCII bytes, filling the `u64` slot with no padding. The `01` names the format epoch. The footer's `format_version u32` counts revisions *within* the epoch (step 2 lands as epoch 1, version 2), so a new magic (`YOLOST02`) is minted only for a future epoch break. |
| C-D2 | Rename the crate/project (`ondadb` → `yolodb`)? | **Decided (2026-09-23): later, as its own release.** Step 1 changes only the on-disk format; the crate stays `ondadb`. |
| C-D3 | Existing ondaDB 0.9.x databases | **Decided (2026-09-23): automatic upgrade on open** (1.3). A binary with the epoch-1 format that opens a 0.9.x directory rebuilds it into epoch 1 in a **new sibling directory**, then swaps it into place, unless `Options::format_upgrade` forbids it. Any failure leaves the 0.9.x directory untouched and usable by a 0.9.x binary. The legacy decoders therefore live in a **default-on** cargo feature `legacy-onda`; turning it off makes a 0.9.x directory a hard `UnsupportedFormat` refusal. |
| C-D4 | How much convergence goes into epoch 1? | Beyond the magic and CRC swap, include **only choices that cost nothing now and would otherwise force another break later** (1.1.3). Everything that is a real design trade-off (value storage, the manifest encoding, the WAL envelope) waits for step 2 and lands as epoch-1 **format version 2**, not as a new magic. |

### 1.1 New constants, magics and versions (≈2 wk)

#### 1.1.1 Every persisted identifier gets a yoloDB value

| Artifact | Today (ondaDB) | Epoch 1 | Notes |
|---|---|---|---|
| SST footer magic | `WAVESST1` (u64) | `YOLOST01` (u64) + `format_version u32 = 1` | See the new footer below |
| Manifest magic and version | `WVMF` (u32), version 1 or 2 | `YOLODBMF` (u64) + `version u32 = 1` | This removes the `WVMF` collision, where both engines use the same magic and version 2 means different layouts (research: catalog §1) |
| Manifest edit log | `ONDE`, u32 schema | `YOLODBED` (u64) + `schema u32 = 1` | Same framing, new header |
| WAL segment | **no header** | a new 32-byte segment header: `YOLODBWL` + `version u32` + `layout u8` (per-CF or unified) + `generation u64` + `crc32c` | Today nothing identifies a WAL file's format or engine. A header lets replay refuse a foreign or old segment at byte 0 instead of mid-frame |
| Value log (`.vlog`) | no header; footer flag `VLOG_V2` | a 32-byte header: `YOLODBVL` + `version u32` + `flags u32` (reserved for dictionary/grouping, step 2) + `crc32c` | The same header shape wavesdb's `WVB2` blob header proved out. It is ready for per-file dictionaries without another break |
| Config blob | positional fields + `ONDA*` 8-byte tagged tails | a tagged TLV: `YOLODBCF` + `version` + `(tag uvarint, len uvarint, bytes)*` | Positional decoding is what made wavesdb's JSON look like a comparator name. Unknown tags are **preserved** on rewrite, which step 2 needs so one engine never strips the other's options |
| Manifest tagged tails | `ONDACAP1`, `ONDAMED1`, `ONDARNG1`, `ONDAAGE1`, `ONDAOBJ1`, `ONDAINS1`, `ONDAWAL1` | Fold these into the epoch-1 manifest body as **flagged sections** in ascending bit order (the wavesdb v3/v4 discipline), with a strict flag mask and the capability word as a fixed u64 | One manifest encoding instead of a positional body with tails |
| Unified CF id | FNV-1a with a **wrong offset basis** `1469598103934665603` (`src/unified.rs:29`) | The correct FNV-1a-64 basis `14695981039346656037`, the same as wavesdb | Fixing it is free in a new epoch. Keep the old constant only under `legacy-onda` |
| Legacy FNV bloom | readable; basis constant also wrong (`src/bloom.rs:48`) | Not written; readable only under `legacy-onda` | Epoch 1 blooms are xxh3 only |
| Directory names | `cf-<name>` | **decided in step 2** (wavesdb uses `cf_`); keep `cf-` in epoch 1 behind one `cf_dir()` function | Doing the plumbing now (6 call sites) makes the step-2 switch a one-liner |

**New SST footer (96 bytes).** A clean break allows the footer to carry what
the 64-byte one could not:

```
 0  index handle      off u64 | len u64
16  bloom handle      off u64 | len u64
32  num_entries u64
40  max_seq u64
48  flags u32                (shared registry allocation, 1.1.3)
52  format_version u32 = 1
56  capability word u64      (the table's subset; replaces wavesdb's uvarint-in-bytes-49..55 trick)
64  aux handle        off u64 | len u64   (range block and future sections; zero if none;
                                            replaces the "16 bytes before the footer" trick)
80  crc32c u32 over bytes 0..80           (today's footer is NOT checksummed; invariant 4 gap)
84  reserved u32 = 0         (must be zero)
88  magic "YOLOST01" u64   (8 ASCII bytes)
```

#### 1.1.2 Where the change lands

- `src/format.rs`: the single home for magics, versions, footer flags,
  capability bits, kinds and codec ids, each with a `const _: () = assert!`
  pin.
- `src/sst/{mod,writer,reader}.rs`: the new footer.
- `src/manifest.rs`, `src/manifest_edit.rs`: the new headers and sectioned
  body.
- `src/wal.rs`: the segment header.
- `src/config.rs`: the TLV blob.
- `src/unified.rs`, `src/bloom.rs`: the correct FNV basis.
- The old encoders are **deleted**. The old decoders move to
  `src/legacy_onda/` behind `legacy-onda`.
- Golden corpus: regenerate `tests/fixtures/` for epoch 1. Move the existing
  fixtures to `tests/fixtures/legacy-onda/`, where they become the
  auto-upgrade test inputs.

#### 1.1.3 Zero-cost convergence choices taken in epoch 1

Each of these is a free choice while the format is being broken anyway. Each
matches wavesdb where wavesdb's choice is not worse:

| Item | Epoch-1 choice | Why |
|---|---|---|
| Codec ids | 0 none, 1 snappy, **2 and 4 permanently unassigned** ("burned"), 3 zstd, 5 raw deflate, **6 raw LZ4 block**, 7 zstd-with-dictionary (reserved until step 2), 8 brotli (reserved) | Ids 2 and 4 mean LZ4 in ondaDB and Zstd in wavesdb, so neither engine can ever reuse them. Tag-6 bytes are identical to ondaDB's current tag-2 bytes (research: sst §3), so the writer change is one constant. |
| Bloom encoding | The tag byte **leads** (wavesdb) | The hash and probing are already identical. Only the parse position differed. |
| Footer flags | Allocate from the shared registry: `0x01` bloom, `0x02` btree. Everything else is a capability bit in the footer word, not a flag. | The flag byte collision (`0x04`/`0x08` meaning different things) disappears because the flags stop carrying format meaning. Restarts become unconditional (next row), and vlog v2 is the only vlog frame format. |
| In-block restarts | **Every** data block carries a restart trailer | ondaDB's point-read and seek advantage. Step 2 asks wavesdb to adopt it (its legacy full-key blocks have no restarts). |
| Unknown bits | `UnsupportedFormat` for unknown version, capability, codec or kind. `Corruption` for malformed bytes. Nothing is ever read as a plain put. | Both engines already agree on this. |
| Registry | `docs/format-registry.md` becomes the **yoloDB registry**, covering magics, versions, footer layout, flags, capabilities, kinds, codecs, TLV config tags, edit ops and WAL layouts. Move it to a shared location in step 2. | The failure so far was containers nobody registered. |

### 1.2 CRC32-C (≈3–4 d, part of the epoch)

- `encoding::checksum` → CRC32-C via the `crc32c` crate, which uses hardware
  CRC on aarch64 and SSE4.2 on x86-64. Pin it with the check-value test:
  `"123456789"` → `e3069283`.
- It applies to every checksum in epoch 1: WAL frames, block frames, vlog
  frames and headers, the manifest, edit records, the new footer, and the WAL
  segment header.
- The IEEE implementation (`crc32fast`) moves to `legacy-onda`.
- **Measure** the change in `onda_bench` fill and scan, ≥5 runs with
  same-run ratios (`docs/performance.md`). A regression beyond noise on
  either architecture must be explained before merging. CRC throughput is
  not expected to matter next to decompression.
- Fix the AGENTS.md invariant 4 wording and the `encoding.rs` doc comment
  in the same change. They claim CRC32-C today and become true.

### 1.3 Automatic upgrade of 0.9.x databases (≈1.5–2 wk)

**Behaviour.** `DB::open` on a directory whose `MANIFEST` is a 0.9.x one
(magic `WVMF`, IEEE CRC, version 1 or 2) does the following:

| `Options::format_upgrade` | Result |
|---|---|
| `Auto` (**default**) | Rebuild into epoch 1 (below), swap, then open the new directory normally |
| `Forbid` | `UnsupportedFormat("ondaDB 0.9 format; format upgrade forbidden by Options::format_upgrade")`. Nothing is written |
| `ReadOnlyLegacy` | Open the 0.9 directory read-only through the legacy decoders, with no rebuild. Useful to inspect or export without committing to the upgrade |

A read-only open (`Options::read_only`) never upgrades. It behaves as
`ReadOnlyLegacy` when `Auto` is set, and refuses when `Forbid` is set.
`Options` is `#[non_exhaustive]`-safe here only because the epoch release is
already a breaking release.

**Rebuild protocol.** The source directory is never modified before the swap.
Every step before the swap can fail and leave the old database intact.

1. Take the source's `LOCK` exclusively and hold it throughout, so no 0.9.x
   process can write concurrently. If another process holds it, fail with
   the normal lock error.
2. Preflight:
   - **Free space:** require ≥ the source's live bytes plus a margin on the
     destination filesystem; otherwise fail before writing.
   - **Tables on non-default tiers or object stores:** refuse with guidance
     (`FormatUpgradeUnsupported`) in this step. Rewriting remote objects is
     step-2 work.
   - **Unresolved prepared transactions:** refuse. Committing or aborting
     them is the operator's call, because nothing is ever aborted
     automatically.
3. Create `<parent>/.<name>.yolo-upgrade-<nonce>/`, on the same filesystem so
   the swap is a rename. Write an `UPGRADE-IN-PROGRESS` marker file into it
   first.
4. Open the source read-only through `legacy-onda`, with WAL replay into
   memtables, which is the path wave S4 made complete. Write every column
   family through the ingest writer into epoch-1 tables, preserving:
   - sequences and TTLs
   - tombstones and single deletes
   - merge operands (unfolded)
   - range tombstones
   - partition and `max_entry_time` stamps
   - the `CAP_*` word
   - per-CF config (re-encoded as TLV)

   Write the epoch-1 manifest **last**, then fsync every file and the
   directory.
5. **Verify** before the swap. Reopen the new directory read-only and
   compare with the source: per-CF entry counts, max sequence, and a
   streaming full-scan equality check. On a large database the scan can be
   disabled with `format_upgrade_verify: Scan | Counts`; the default is
   `Scan`.
6. **Swap**, crash-safe, with two renames and a durable journal:
   1. Write `<parent>/.<name>.yolo-upgrade.journal` (`from`, `to`, `backup`,
      state = `swapping`) and fsync the parent directory.
   2. Rename `<path>` → `<parent>/.<name>.pre-yolo-<nonce>` (the backup).
   3. Rename the upgrade directory → `<path>` and fsync the parent.
   4. Set the journal state to `done`, remove the marker, fsync.

   A crash anywhere in step 6 is resolved at the next open, which reads the
   journal first. If `<path>` is missing and the upgrade directory is
   complete, it finishes the swap. If the upgrade directory is incomplete, it
   rolls back: it renames the backup back and deletes the partial directory.
   macOS `renamex_np(RENAME_SWAP)` and Linux `renameat2(RENAME_EXCHANGE)`
   would make the swap atomic, but both need `unsafe` FFI under
   `#![deny(unsafe_code)]`, and the journal makes two plain renames
   sufficient.
7. Backup retention: `Options::format_upgrade_keep_backup` (default `true`).
   The old 0.9 directory stays at `.<name>.pre-yolo-<nonce>` until the
   operator deletes it, which keeps the upgrade reversible. `false` deletes
   it after a successful open.
8. Report the outcome through the log and a returned `UpgradeReport` on
   `DB::last_format_upgrade()`: old and new byte counts, duration, and the
   backup path.

A failure in steps 2–5 removes the upgrade directory, releases the lock and
returns the error. The 0.9.x directory is byte-identical afterwards, and a
test asserts it.

**Also shipped:** `yolodb upgrade <path>`, a binary target that runs the same
protocol offline with progress output, for operators who want to upgrade
outside the application.

**Tests** (crash tests follow the fault-shim pattern in
`tests/engine_regressions.rs`):
- every `tests/fixtures/legacy-onda/` fixture auto-upgrades, and the result
  is scan-equal to the source;
- `Forbid` and `ReadOnlyLegacy` behave as specified;
- a crash injected after each numbered step either completes or rolls back
  on the next open;
- a preflight failure (no space, tiered tables, pending prepares) leaves the
  source byte-identical;
- a concurrent 0.9.x holder of `LOCK` blocks the upgrade.

### 1.4 Port wavesdb features that add function or speed

Scope is everything in
[`feature-inventory.md`](../phase-w-wavesdb-compat/research/feature-inventory.md)
and wavesdb's `docs/ondadb-comparison.md` ("present in wavesdb, absent from
ondaDB") that is **absent or partial** in ondaDB, is not a correctness fix
(those are wave S), and does not depend on a step-2 format decision. Each
item is its own change, tests first, with both feature configurations green.
Speed items need ≥5-run before/after evidence and are reverted if they do not
pay.

**Speed**

| # | Feature (wavesdb ref) | What | Effort |
|---|---|---|---|
| P1 | Point reads stop early by table `MaxSeq` (`5ef39df`) | `get` stops probing older tables once it holds a version newer than every remaining candidate's `max_seq`. Today it probes every candidate (`column_family.rs:1544-1571, 1681-1700`) | 2–3 d |
| P2 | Background scans don't admit into the block cache (`ac16c8a`) | Compaction, backup and verify readers look up the cache but never insert (`sst/reader.rs:652,675`), so a big compaction no longer flushes the hot set | 2 d |
| P3 | Periodic-compaction burst bound (`887f8ad`) | Covered by wave S5; listed here only if S5 was skipped | 1 d |
| P4 | Wired tombstone-density trigger | `tombstone_density_trigger` / `_min_entries` are declared but never read (wavesdb's comparison, "Not worth porting"). Wire them into the picker | 3 d |
| P5 | MultiGet bounded parallel block reads (`MaxConcurrentBlockReads`) | Parallel cold-block I/O for batched gets; the benefit is on S3 tiers | 3–5 d |
| P6 | User-space WAL write buffer | Coalesces single-threaded commit streams into fewer `write` syscalls. The durability contract is unchanged: a sync still flushes the buffer | 3–5 d |
| P7 | Bloom auto-allocation (`BloomAutoAllocate`, Monkey-style per-level bits) | Fills the reserved `ONDABLM2` slot (a TLV tag in epoch 1) | 1 wk |
| P8 | Local disk cache for object-store reads (`LocalCachePath`, `f28aecc`) | A second-tier block cache on local SSD in front of S3 range GETs | 1–1.5 wk |
| P9 | O_DIRECT (Linux) / `F_NOCACHE` (macOS) compaction reads (`7289c76`) | Keeps compaction I/O out of the page cache. Aligned buffers need either a documented `unsafe` contract or an aligned-alloc crate, so this is behind `unsafe-fastpath`, measured, and spike-first | 1 wk |
| P10 | Graduated default codecs | Defaults `{None, LZ4 (tag 6), Zstd}` per level, like wavesdb. A **defaults** change: measure ratio and speed and document it | 1 d + bench |

**Function**

| # | Feature (wavesdb ref) | What | Effort |
|---|---|---|---|
| F1 | Database-wide default isolation (`d789912`) | `Options::default_isolation`, honoured by `begin()`. Today the field is documented as reserved and `begin` hard-codes Snapshot | 1–2 d |
| F2 | `SnapshotHandle` | A standalone, refcounted read snapshot outside a transaction; pins `oldest_snapshot` like a txn does | 2–3 d |
| F3 | `compact_range(cf, start, end)` | Manual compaction of a key span through the normal picker and job path (range lock, `catalog_txn`) | 3–5 d |
| F4 | `purge()` / `purge_column_family()` | Drop expired and obsolete data eagerly (wavesdb semantics) | 2–3 d |
| F5′ | Clear under the unified layout, **no rename** (from `9e56f67`) | Persist a per-CF unified id in the epoch-1 manifest (defaults to FNV of the name, so it is omitted until it diverges). `clear_column_family` in unified mode takes a fresh id, so the old entries become unowned and are dropped at flush, instead of today's refusal (`src/db.rs:2540`). Rename stays a non-goal (decided 2026-09-23), but the id makes it cheap if reopened | 3–5 d |
| F6 | `sstables_since(seq/generation)` | The changed-table diff behind incremental backups | 2 d |
| F7 | Object-store checkpoints | `checkpoint_to_object_store` (MANIFEST uploaded last as the commit marker, per-object SHA-256 receipts, create-if-absent), `restore_from_object_store`, and `open_remote_checkpoint` (lazy, range GETs through the existing S3 tier). Carry the S4 fix's read-only-source rule | 2 wk |
| F8 | S3 parity (v0.8.2–v0.8.6) | Session token (hard-coded `None` today, `storage_s3.rs:169`), a credential chain (env, then profile file, then IMDS/STS), named `profile` without chain fallback, `anonymous`, `read_only` (no bucket probe or create, writes refused locally), a child-prefix lister (the common prefixes `storage_s3.rs:370-378` drops), and verified upload checksums | 3–5 d |
| F9 | Demote a part to the default tier (`4fa392c`) | `relocate_part(.., None)` moves off a tier back to local | 3 d |
| F10 | `get_into(&mut Vec<u8>)` | Caller-buffer point read with no allocation | 1 d |
| F11 | Wide-column entities (wavesdb 4.2, `340cd87`) | A value-level API over an opaque, specified value frame. No engine change; take the wavesdb spec as-is so values interoperate | 1 wk |
| F13 | Observability | Deletion-pause stats (`deletions_paused`, `deletions_queued`) and read profiling (`ReadStats`) | 2–3 d |
| F14 | Shared read resources (`ReadResources`, wavesdb `23648c8`, `f6b3def`) | A process-wide `ReadResources { block_cache_bytes, max_open_files, max_open_readers, max_reader_bytes }` that many **read-only** opens lease from, so N immutable databases share one block cache and one file/reader budget instead of N. `close` stops new leases and frees the caches when the last database closes. Stats: hits, misses, evictions, entries, bytes, open files, leases. yolosearch depends on this | 1 wk |

**Deferred to step 2** (each needs a shared format decision first, or is a
wavesdb speciality whose encoding both sides must agree on):

| Feature | Why it waits |
|---|---|
| Shared blob files, blob GC, staleness accounting, blob exclusivity for parts | The value-storage decision (2.2 row V) |
| Per-file zstd dictionaries (codec 7), compression grouping (cap bit 8) | Belong to whichever value container step 2 defines. The epoch-1 vlog header already reserves the flags |
| Managed sequence mode (cap bit 7, kinds 32/33, manifest block) | Capability semantics must be jointly specified. It also needs a consumer |
| Large-transaction spill (kind 19, `txn-spill/` chunk files) | Needs the step-2 prepare/control-frame layout |
| Replicas, promotion, catalog mirror, `MANIFEST-CURRENT` CAS pointer | A recorded ondaDB non-goal. Step 2 defines the format so wavesdb can keep it as a native-mode speciality |
| WAL failover (wavesdb 4.3, an unwired prototype) | Unfinished upstream |
| YAML/env configuration, `InjectReadLatency`, bench compare tooling | Go tooling, not engine surface |
| Global memory budget | Rejected in ondaDB (decision record 0.7), unchanged |
| CF rename (from F5) and pessimistic range locks, FIFO mode, optimistic lock-table probe (F12, `80c2224`) | **Left out of step 1 (decided 2026-09-23).** Both stay AGENTS.md non-goals. No consumer needs them (yolosearch uses neither), and F12 adds per-commit work to the path 3.4 is trying to shrink. F5′ keeps rename cheap if it is reopened |

### 1.5 Step-1 release gate

1. The four CI commands (AGENTS.md) are green, and also green without
   `legacy-onda`. `cargo build --features s3` compiles.
2. The new golden corpus is committed, and every magic, version, flag,
   capability, codec, TLV tag and edit op is pinned by a golden test and a
   `const` assertion.
3. Every legacy fixture auto-upgrades with a scan-equal result, and the 1.3 crash matrix passes.
4. Benchmarks show no regression beyond noise versus 0.9.0 on
   `onda_bench` fill, get and scan (≥5 runs), apart from the explained CRC
   delta.
5. `docs/formats.md` is rewritten for epoch 1, `format-registry.md` has
   become the yoloDB registry, AGENTS.md invariants are updated, and the
   CHANGELOG has a "Breaking: format epoch" section with migration steps.

**Step-1 size (one engineer, rough):**
- Epoch, CRC and legacy/migrate: about 4 weeks.
- Speed ports: about 6–7 weeks.
- Function ports: about 6 weeks (F5′ and F14 in, F5 rename and F12 out).

The items are independent after the epoch lands, so they parallelize well.

### 1.6 Step-1 follow-ups (0.10.x, no format change)

wavesdb v0.8.7–v0.8.9 (`ce19021`, `f5a5dc4`, `63e084f`) changed runtime code
only. Each item below is validated in ondaDB before it is fixed:

| Item | wavesdb | ondaDB suspicion | Fix if confirmed |
|---|---|---|---|
| Read-your-own-writes index | `ce19021`: per-family index of buffered point ops, built once a family holds ≥ 32 ops | `Txn` point reads and `multi_get` scan `writes` linearly per read (`txn.rs` `plan_point_read`), so N writes plus N reads is O(N²) | A per-family index, dropped on savepoint rollback and reset. Benchmark: N writes then N reads |
| Bounded txn iterator overlay | `f5a5dc4`: the overlay holds only writes inside `[lower, upper)` | `new_iterator_bounded` copies every buffered write of the family | Filter by bounds; keep range deletes that can cover a key in range |
| Drop CF racing a rotation | `63e084f`: a rotation opened its WAL in a dropped family's directory and poisoned the DB | `drop_column_family` removes the directory without draining an in-flight rotation, and the rotation has no "retiring" check | First a regression test that holds a rotation in flight across the drop; then a retiring flag, a drain in the drop, and `NotFound` from flush on a dropped family |

---

## Step 2 — full convergence

### 2.1 The model: one format, a common profile, native specialities

- **One registry, one spec, one corpus.** Create a third repository, e.g.
  `yolodb-format`, holding:
  - the byte specification;
  - the registry (every number, both engines);
  - a golden corpus;
  - a **cross-engine CI harness**, in which each engine writes every
    scenario and the other engine opens, scans and compares it against an
    oracle. The pair is compatible exactly when that matrix is green.
- **Capabilities are the negotiation mechanism.** Every optional artifact
  kind is a capability bit in the manifest word, as today. An engine opening
  a directory refuses (`UnsupportedFormat`) any bit it does not implement,
  before touching a table or a WAL.
- **Common profile** = the set of bits **every** conforming engine must
  implement for read and write. A database written in compat mode uses only
  common-profile bits, so either engine can open it read-write.
- **Native specialities** = bits only one engine writes. The other engine
  should at least **read** them (read-compat), so checkpoints are portable.
  Where reading is impractical, the bit stays refused and the directory is
  single-engine. That is the documented cost of native mode.
- The manifest records which engine last wrote it and in which mode. This is
  advisory; the capability word is the authority.

### 2.2 Divergences to resolve

Legend for "Proposal": **C** = common-profile rule both engines implement;
**N** = native speciality behind a capability bit, with the reader
requirement stated.

| Row | Area | ondaDB today | wavesdb today | Proposal | ondaDB changes | wavesdb changes |
|---|---|---|---|---|---|---|
| A | Magics, versions, footer, WAL segment header, CRC32-C | done in step 1 | `WAVESST2`, `WVMF` v2/3/4, `WDME`, `WVB2`; CRC32-C already | **C:** epoch 1 as defined in 1.1 | none | Adopt all epoch-1 containers. Keep the WAVESST2/WVMF readers as a **legacy-wavesdb** compat mode plus a migrator (mirror of 1.3) |
| B | Block encoding declaration | per table (footer) | per block (`0xff` header) | **C:** a per-block header byte (self-describing; lets one table mix encodings, needed by mounts and prefix-delta rollout) | Emit and parse the per-block header; drop the table-level encoding flags | Adopt epoch-1 header values |
| C | Restart points | every block (step 1) | only in prefix-delta blocks | **C:** every data block carries restarts | none | Write restart trailers in full-key blocks; binary-search inside blocks (a speed win for wavesdb point reads) |
| D | Prefix-delta entry layout | `kind\|mods\|shared\|suffixLen\|valLen\|seq\|ttl?\|suffix\|val` | `shared\|suffixLen\|suffix\|kind\|mods\|valLen\|seq\|ttl?\|val` + `WDBL` trailer | **C:** pick one by microbenchmark (default: wavesdb's, since it is fuzzed and documented); the trailer magic becomes a yolo value or is dropped, since the block CRC already covers it | Port the decoder and writer if wavesdb's layout wins | Port if ondaDB's layout wins |
| E | Range-delete block | aux section container | `0xff\|1\|count` block | **C:** one envelope inside the epoch-1 aux handle; the fragment body is already identical | Small | Small |
| **V** | **Value storage** | per-table `.vlog`, 8-byte offset; compaction rewrites values | shared `<id>.blob`, 16-byte `(id,off)` pointer; compaction passes pointers; blob GC | **Both, as capabilities.** `CAP_VALUES_PER_TABLE` (common profile: every engine reads and writes it) and `CAP_VALUES_SHARED_BLOBS` (N: wavesdb's native write mode; **every engine must read it**, and writing needs blob GC). The pointer carries a 1-byte container tag, so both kinds can coexist in one table. Chosen per CF by `value_storage = PerTable \| SharedBlobs` | Reader for shared blob files (the header, frames, dictionaries, groups: Plan W's W3, re-targeted to epoch-1 headers). Optional later: a SharedBlobs writer with GC | Reader and writer for per-table value files; write them in compat mode. Blob GC stays for native mode |
| V2 | Value compression extras | none | per-file dictionaries (codec 7), grouping (cap bit 8) | **N → C over time:** define dictionary and group frames for **both** containers via the epoch-1 value-file header flags; readers must implement them | Implement dictionary and group read (and write for per-table files, a compression-ratio win) | Emit them in the epoch-1 header layout |
| F | Manifest body | step-1 sectioned body | v2/3/4 flag sections | **C:** the epoch-1 manifest carries the **union**: blob maps and per-CF blob lists (present only with the shared-blob bit), unified ids, managed state, nonce, caps, edit watermark, partition/tier/object, max entry time, LCT, merge stats, range summaries, per-table caps | Add the blob and managed sections as parse-and-preserve even when not acted on | Adopt the encoding |
| G | CF config | TLV (step 1) | JSON | **C:** TLV with registered tags; **unknown tags preserved** on rewrite; unit rule: every duration in ns | Register all wavesdb option tags; preserve the ones it ignores | Replace JSON with TLV (keep a JSON reader for legacy) |
| H | Directory and object names | `cf-<name>`; stems `cf-<cf>/<nonce>-<id>` | `cf_<name>`; stems `cf_<cf>/…`; blobs `<stem>-<id>.blob` | **C:** pick one (`cf_`, which keeps wavesdb's published object-store checkpoints path-stable) | Flip `cf_dir()` (plumbed in step 1) | none |
| I | Edit log ops | 12 ops (ONDE) | 18 ops (WDME) | **C:** one op table (the union; wavesdb's numbering as the base since it is the superset); canonical-form re-encode check on both sides | Adopt the op table; implement blob, rename and managed ops as preserve-or-apply | Adopt the epoch-1 header |
| J | WAL envelope | `0xFF\|schema(1\|2)\|count\|(kind\|mods\|alen\|blen\|seq\|ttl?\|a\|b)*` | `0xff\|schema=1\|count\|envFlags\|(kind\|mods\|cfID\|klen\|key\|vlen\|val\|seq\|ttl?)*` | **C:** one envelope, versioned by the step-1 segment header; per-record `cfID` kept (it lets replay cross-check the prefix) | Adopt | Adopt |
| K | Unified CF id | true FNV (step 1) | true FNV + a manifest override after rename/clear | **C:** manifest id if present, otherwise FNV of the name | Already done by F5′ | none |
| L | Control frames and 2PC | 16-byte ids; prepare = 1+N records; commit seq = **first** of span; no range delete in a prepare | 1–128-byte string ids; one record per control frame + a versioned CRC'd prepare body; commit seq = **last** of span; range delete allowed | **C:** wavesdb's shape (a strict superset: variable ids, a self-contained body, one record per frame); commit seq semantics fixed to one definition | Rewrite prepare/decision encoding; allow range deletes in prepares (reserve the span) | Adopt the epoch-1 kinds and body version |
| M | WAL stripes (`.s1`–`.s3`) | ondaDB speciality | none | **N:** `CAP_WAL_STRIPES`; wavesdb must at least **replay** striped segments to open a crashed native-onda directory | none | Stripe-aware replay (read only) |
| N | Managed sequence mode | reserved | bit 7, kinds 32/33, manifest block | **N:** wavesdb speciality; ondaDB reads it read-only (the discard floor is a read floor) and refuses writes | Read-only support | none |
| O | Large-transaction spill | reserved | kind 19 + chunk files | **N:** refused by engines without it; a checkpoint never carries it | none (refuse) | none |
| P | Parts: digest, detach order, blob exclusivity | digest = klog ++ vlog; commit-then-move | digest = klog ++ owned blobs; link-then-commit + incomplete marker | **C:** digest = klog ++ each value file (per-table or owned blobs) in id order; **link-then-commit** (it is crash-safer: research, wavesdb comparison table) | Switch the detach order and digest preamble | Include per-table value files in the digest |
| Q | Object-store checkpoint layout | none before step 1 F7 | `<prefix>/MANIFEST` last + `cf_<cf>/<id>.{klog,blob}` + receipts | **C:** wavesdb's layout with epoch-1 files; F7 already follows it | Align F7 to the spec | Adopt epoch-1 files |
| R | Catalog mirror, `MANIFEST-CURRENT`, replicas, promotion | non-goal | yes | **N:** specified in the format so the bytes are registered; ondaDB **ignores** mirror objects and never writes them | none | Re-encode the pointer with epoch-1 magics |
| S | Codec 8 (brotli) | reserved (step 1) | yes | **C** for read (a pure-Rust decoder); writing optional | Brotli decode | none |
| T | Semantics beyond bytes | — | — | **C:** TTL (absolute ns, `ttl <= now` expired), comparator names, merge-operator names, sequence visibility, tombstone and single-delete rules, and the range-delete-vs-merge rule from `tests/composition.rs` become **spec text**, each with a cross-engine scenario | Spec + scenarios | Spec + scenarios |
| U | Write-pacing defaults, isolation defaults, WAL-sync counting | on at 2/8 GiB; —; interval syncs not counted | off; honoured; counted | Not format. **Documented differences**, no convergence needed | optional | optional |

### 2.3 Order of work

0. **2-pre — wavesdb layout refactor (≈1–2 wk, wavesdb, before 2b).** Step 2b
   rewrites wavesdb's format code (`internal/sst`, `manifest`, `wal`,
   `format`). Restructuring that code while it is being rewritten would make
   every 2b change a conflict, so the restructuring happens first, as its own
   series. Rules:
   - **No behaviour and no byte change.** Every refactor PR passes the
     unchanged test suite. `go test ./...` and the bench smoke run are its gate,
     and a byte-for-byte diff of a fixture checkpoint written before and after
     must be empty.
   - **The public surface stays importable.** That is the root package
     `github.com/yannick/wavesdb`, `objstore`, `objstore/contract` and
     `blockcache`. Consumers:
     - yolosearch (root, `objstore`, `blockcache`, `contract`; pinned v0.8.6);
     - waveSpan (root, `objstore`; pinned v0.8.4);
     - `../bench` and `testing-waves`, both via `replace ../wavesdb`, so they
       break immediately.

     Exported types that move into a subpackage stay reachable through root
     type aliases until a major version drops them.
   - **`internal/` may be restructured freely.** Go forbids outside imports of
     it.
   - **Go's one-directory-one-package rule bounds the scope.** The root package
     (79 source and 171 test files) cannot be split into folders by moving
     files. Each extracted subsystem becomes a package behind a small
     interface:
     - candidates: entity, ingest/excise, object-store checkpoints, bloom
       policy;
     - not candidates: DB, column family, flush and compaction, which share
       unexported state. They are reorganised **within** the root package
       (file naming, consolidating tests).
   - **Before starting:** land or rebase the open branches
     (`feat/subcompactions-2`, `module-path-github`,
     `feat/yolosearch-immutable-checkpoint`).
   - **After the refactor:** update the wavesdb file and line citations in this
     plan and in `../phase-w-wavesdb-compat/research/`.
1. **2a — spec and harness (≈3 wk, both teams):** `yolodb-format` repository,
   the registry, rows A–T written as spec text, the golden corpus from
   ondaDB epoch 1, and the cross-engine CI harness (ondaDB write → wavesdb
   read and the reverse, oracle-checked).
2. **2b — wavesdb adopts epoch 1 (≈6–8 wk, wavesdb):** row A, and its legacy
   reader and migrator. After this, both engines speak the same containers,
   and the common profile is still ondaDB-shaped.
3. **2c — read-compat for specialities (≈6–8 wk, split):**
   - ondaDB: rows V/V2 (shared-blob reader, dictionaries, groups), N, S.
   - wavesdb: rows V (per-table value files, read and write), C, M (read).

   After this, **any checkpoint from either engine opens in the other**.
4. **2d — write-compat, the common profile (≈6–10 wk, split):** rows B, D,
   E, F, G, H, I, J, K, L, P, Q. After this, a compat-mode directory can be
   opened read-write by either engine, WAL and edit log included.
5. **2e — retire legacy readers** (`legacy-onda`, `legacy-wavesdb`) after a
   published deprecation window.

### 2.4 What breaks, per engine

**ondaDB**
- Step 1 breaks every 0.9.x database. The mitigation is
  automatic upgrade on open (1.3), which keeps a backup of the 0.9 directory.
- The crate rename (C-D2) is deferred to a later release.
- Step 2 changes, each within the epoch (format version 2):
  - the per-block header;
  - the `cf_` directory switch;
  - the WAL envelope;
  - the prepare/decision encoding;
  - the detach order;
  - the part digest.

  Parts detached or exported before step 2 need the migrator or a digest
  re-stamp.
- Semantics:
  - commit-seq semantics for prepared transactions change (row L);
  - range deletes become allowed in prepares.

**wavesdb**
- Every WAVESST2/WVMF database and every published object-store checkpoint
  moves to legacy-wavesdb. The mitigation is its reader plus a migrator.
  Consumers that read published checkpoints must upgrade before producers
  switch.
- The JSON config goes away (a legacy reader stays).
- Ids 2 and 4 stay burned (no change).
- Full-key blocks gain restart trailers (writer change).
- Compat mode requires the per-table value-file writer. Native mode keeps
  shared blobs and GC.

**Neither engine gives up a speciality:**
- ondaDB keeps per-table value files (read-optimised) and WAL stripes.
- wavesdb keeps shared blob files with GC (write-amplification-optimised),
  managed mode, spill, and replicas/mirror.

Each speciality is a capability bit. The only cost of using one is that the
other engine may refuse the **directory** (never misread it).

### 2.5 Performance expectation

The format is not where the two engines' performance difference comes from;
the six-engine matrix (`../bench/BENCHMARKS.md`) mostly measures the
implementations. The format-level trade-offs are:

- **Value storage:** per-table files favour scans and space reclamation;
  shared blobs favour large-value write amplification. This is why row V
  keeps **both**, per CF.
- **Restarts in every block:** helps point reads and seeks; it costs about 4
  bytes per 8 entries.
- **CRC32-C:** neutral on arm64, and marginally faster on x86 thanks to the
  SSE4.2 instruction.

Every step-2 row that changes a hot path (B, C, D, V) carries a ≥5-run
before/after gate in **both** engines.

## Relationship to other plans

- **Wave S** (shared correctness fixes) shipped in 0.9.1, **before** the epoch,
  so the upgrade starts from fixed code.
- **Plan W** (read wavesdb 0.8.6): its W2–W4 decoders become the ondaDB side
  of `legacy-wavesdb` and row V's blob reader. Run it before step 2 only if
  0.8.6 checkpoints must be read in the meantime.
- **Phase 0–3 roadmap:** unaffected. Any item still open lands in epoch-1
  encodings.
