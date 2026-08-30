# On-disk formats

Every persisted byte, exactly. All fixed-width integers are **little-endian**;
varints are unsigned LEB128 (`uvarint`) or zig-zag LEB128 (`varint`) — see
`encoding.rs`. The framing checksum everywhere is **CRC32-C** (`checksum()`,
crc32fast). Format changes require updating this file, the round-trip tests,
and a release note (no cross-version compat machinery exists yet — v0).

## Internal keys (`format.rs`)

```
internal_key = user_key || big_endian(!seq)        (TRAILER_SIZE = 8)
```

Complementing the sequence makes **higher seqs sort first** within a user key
under plain byte-wise comparison of the trailer, so a forward seek to
`(user_key, !read_seq)` lands on the newest visible version. Internal order
everywhere is `(user_key asc via the CF comparator, seq desc)` —
`sst::cmp_internal` is the reference implementation.

Entry flag bits (`format::flags`, shared by WAL + SSTable):
`TOMBSTONE=0x01, HAS_TTL=0x02, HAS_VLOG=0x04, SINGLE_DELETE=0x10`.
`KNOWN_ENTRY_FLAGS = 0x17` is the whole mask; `0x08` is **reserved-unknown**
(it named a `DELTA_SEQ` encoding no writer ever produced). Flags are modifiers,
never an extensibility mechanism — new record semantics get a record *kind*,
not a spare bit.

**Strict decoding** (`format::check_entry_flags`, called by both
`wal::decode_record` and `sst::decode_entry`). An entry is rejected as
`Corruption` when it sets a bit outside `KNOWN_ENTRY_FLAGS`, or carries a
combination no writer can produce:

| Rejected | Why unproducible |
|---|---|
| `flags & !0x17` | no writer ever sets those bits |
| `SINGLE_DELETE` without `TOMBSTONE` | a single-delete *is* a tombstone |
| `TOMBSTONE` with `HAS_VLOG` | `Writer::add` separates a value only when `!tombstone` |

The three encode sites — `memtable::flag_bits`, `wal::encode_record_body`,
`sst::encode_entry` — all build their byte through
`format::normalized_entry_flags`, which repairs both invariants (and
debug-asserts them first). That normalization is what makes the decode-side
strictness safe: `wal::RecordRef` is public, so a caller outside the crate can
construct `{ tombstone: false, single_delete: true }`, and writing bytes we then
refuse to read would turn a caller's mistake into an unopenable database.

### Error taxonomy

| Error | Meaning | Examples |
|---|---|---|
| `Corruption` (code `-5`) | the bytes contradict a format this binary *does* implement | unknown entry-flag bit, `SINGLE_DELETE` without `TOMBSTONE`, unknown or duplicated manifest tail tag, a record that fails to decode inside a CRC-valid WAL frame |
| `UnsupportedFormat` (code `-16`) | the bytes are well-formed but name a feature this binary does not implement | a footer flag bit outside `KNOWN_FOOTER_FLAGS`, a manifest capability bit outside `KNOWN_CAPS`, an assigned-but-unimplemented record kind (`< 64`), an unknown WAL envelope schema, an unknown SST aux-section tag |

A record kind **≥ 64 is `Corruption`, not `UnsupportedFormat`**: that range is
never assigned to anything, so those bytes cannot have come from a newer writer.

A **torn tail** is neither: a short header, a short payload or a frame CRC
mismatch is the expected residue of a crash mid-write and ends that WAL stripe
cleanly (`Ok`).

### Format capabilities (`format.rs`)

A capability is the durable *permission* to write a newer artifact, taken once —
before the first byte using it exists — through
`DB::enable_format_capabilities`. The word lives in the manifest's `ONDACAP1`
tail and bumps the manifest to VERSION 2, which pre-1.0 binaries refuse outright.

| Bit | Symbol | Owner feature |
|---:|---|---|
| `1 << 0` | `CAP_EXTENDED_RECORDS` | 1.0 — kind-bearing record envelopes |
| `1 << 1` | `CAP_MERGE_OPERANDS` | 1.1 |
| `1 << 2` | `CAP_RANGE_DELETES` | 1.2 |
| `1 << 3` | `CAP_PREFIX_DELTA` | 2.1 |
| `1 << 4` | `CAP_MANIFEST_EDITS` | 2.2 |
| `1 << 5` | `CAP_PERIODIC_AGE` | 0.3 |
| `1 << 6` | `CAP_TXN_DECISIONS` | 3.2 |

`KNOWN_CAPS = 0x7F`. The values are an interoperability contract with wavesdb:
a bit is never renumbered, only retired. Enabling is **one-way and idempotent**;
a database that enables nothing keeps writing VERSION-1 manifests and legacy
artifacts forever.

The enable protocol (`DbInner::enable_capability`) is persist-before-use:
refuse a poisoned or read-only database → return `Ok` if the bits are already
active → stage them into `caps_durable` and `persist_manifest` → only then flip
the `caps` word write paths check. The two words exist precisely so the encoder
never writes a bit the database is already using, and never omits one it is.
A failed persist fail-stops the database, leaves `caps` untouched, and rolls the
staged word back: a reopen sees the pre-enable state.

### Record kinds and modifiers (`format.rs`)

The legacy flags byte is nearly exhausted (five of eight bits), so extended
records carry a **kind** instead:

| Kind | Meaning | Owner |
|---:|---|---|
| 1 | put | 1.0 |
| 2 | delete | 1.0 |
| 3 | single_delete | 1.0 |
| 4 | merge operand | 1.1 |
| 5 | range delete | 1.2 |
| 6–15 | reserved (data kinds) | — |
| 16–31 | transaction control | 3.2 |
| 32–63 | reserved | — |
| ≥ 64 | **never assigned** | — |

Modifiers keep the legacy bit values so an extended entry and a legacy entry
describe the same thing with the same numbers: `HAS_TTL = 0x02`,
`HAS_VLOG = 0x04` (SSTable only), `modifiers::KNOWN = 0x06`. `TOMBSTONE` and
`SINGLE_DELETE` are *not* modifiers — they are kinds 2 and 3. An unknown
modifier bit is `Corruption`: modifiers are not capability-gated, so no writer
of any vintage may set one.

Every legacy byte pattern these rules must keep accepting is pinned by the
frozen corpus in `tests/fixtures/phase1/` (see
`tests/fixtures_phase1.rs::legacy_corpus_decodes_unchanged`). Those files were
produced by the 0.8.2 encoders and are regenerated only by an explicit,
reviewed format change — the `#[ignore]`d `regenerate_phase1_fixtures` test.

## WAL (`wal.rs`)

File set per generation (a generation = one memtable lifetime):

```
wal-<gen>.log            stripe 0 — its presence marks the generation
wal-<gen>.log.s1 .. .s3  stripes 1..3   (only for SyncMode::None/Interval)
```

`SyncMode::Full` uses a single stripe so group commit can amortize the fsync.
Committing threads own a sticky stripe (`my_stripe`), eliminating file-mutex
convoys. Replay reads all stripes; cross-stripe order is immaterial (seq
decides visibility). Deletion must use `wal::remove_wal_files(base)`.

Frame — **one frame per committed batch** (atomic replay unit):

```
[payload_len u32][crc32c(payload) u32][payload]
```

Payload = records back-to-back, each:

```
flags u8 | key_len uvarint | val_len uvarint | seq uvarint
| ttl varint (only if HAS_TTL) | key bytes | value bytes
```

Replay (`Wal::replay`) splits the two tail cases:

- short/torn header, short payload, or CRC mismatch ⇒ **clean end of that
  stripe** (`Ok(last_seq)`) — the expected crash residue;
- a record that fails to decode *inside* a CRC-verified frame ⇒
  **`Err(Corruption)`** propagated out of `Wal::replay`. Those bytes reached
  disk intact and still contradict the format, so swallowing them would hide
  real corruption behind the crash-recovery path.

A frame with `payload_len == 0` is legitimate (`Wal::append_batch(&[])` is
public API) and is skipped; replay continues with the frames behind it. A frame
is applied all-or-nothing. WAL bytes are never compressed.

Replay callbacks receive a `wal::ReplayRecord`, not a bare `Record`: later
record kinds are not all point writes, so callers match on the kind rather than
assume one.

### Envelope payload (`CAP_EXTENDED_RECORDS`)

The frame is unchanged; the **first payload byte** selects the form:

```
payload[0] == 0xFF  →  envelope
otherwise           →  legacy record stream (strictly masked flags byte)
```

`0xFF` is safe as a discriminator: a legacy payload starts with a flags byte and
no writer produces one above `KNOWN_ENTRY_FLAGS` (`0x17`).

```
envelope := 0xFF | schema uvarint | count uvarint | record × count

schema 1 = per-CF layout   (keys are user keys)
schema 2 = unified layout  (keys carry the 8-byte big-endian CF-id prefix
                            INSIDE the key, exactly as the legacy unified
                            layout writes them — there is no separate cf-id
                            field: a LEB128 id would cost more than the fixed
                            8 bytes already there)

record := kind uvarint | modifiers uvarint
        | alen uvarint          ← legacy klen slot
        | blen uvarint          ← legacy vlen slot
        | seq uvarint
        | ttl varint            (only if modifiers & HAS_TTL)
        | a bytes               ← legacy key slot
        | b bytes               ← legacy value slot
```

The legacy field order is preserved deliberately, so `append_batch`'s exact
frame-size precompute survives as a one-line variation. The slots are named
generically because kind 5 puts a range's `(start, end)` in them rather than
`(key, value)`; for kinds 1–4 they are `(key, value)`, and a delete has
`blen = 0`. A point record costs **+1 byte** versus legacy (the kind uvarint;
modifiers replace the flags byte), plus 3 bytes of envelope header per frame.

Writer rule: envelopes are emitted iff `caps & CAP_EXTENDED_RECORDS != 0` — a
per-database decision, never a per-frame one — and **replay accepts both forms
forever**, so a file written across an enable replays whole. Decode errors:
unknown `schema` → `UnsupportedFormat`; `kind > 63` → `Corruption`; an assigned
but unimplemented kind → `UnsupportedFormat`; unknown modifier bit →
`Corruption`; a `count` that disagrees with the payload in either direction →
`Corruption`, so an envelope frame can never deliver a partial batch.

As of 1.0-B the engine enables no capability by default and therefore writes no
envelopes; `Wal::append_batch_enveloped` is the codec entry point.

## SSTable (`sst/`)

Two files: `<id>.klog` (always) and `<id>.vlog` (created lazily on the first
value with `len >= klog_value_threshold`, default 512 — WiscKey separation).

### klog layout

```
[data block 0] … [data block N-1] [bloom block?] [index block(s)] [footer 64B]
```

Every block (data/bloom/index) is framed by `block.rs`:

```
[alg u8][comp_len u32][raw_len u32][crc32c(payload) u32][payload]
```

`alg` is the `Compression` enum; if compression does not shrink a block it is
stored with `alg = None`. The CRC covers the compressed payload. Data blocks
target `ColumnFamilyConfig::data_block_size` raw bytes (default 4 KiB). Block
handles make each file self-describing, so changing the policy does not affect
reads of existing tables.

Data-block entry (`sst::encode_entry` / `decode_entry`):

```
flags u8 | key_len uvarint | val_len uvarint | seq uvarint
| ttl varint (if HAS_TTL) | key bytes
| value bytes            (inline; if !HAS_VLOG)
| vlog_off u64           (if HAS_VLOG; val_len = logical value length)
```

Entries are appended in internal order; each block's index separator is the
block's **last** `(user_key, seq)`.

#### Extended entry layout (`FOOTER_EXTENDED_BLOCK = 0x10`)

When the footer sets `FOOTER_EXTENDED_BLOCK`, **every** data-block entry in the
table uses the kind-bearing layout instead:

```
kind uvarint | modifiers uvarint | key_len uvarint | val_len uvarint
| seq uvarint | ttl varint (if modifiers & HAS_TTL) | key bytes
| value bytes | vlog_off u64   (as above, on modifiers & HAS_VLOG)
```

The flag is **table-level**, not per-block: a block carries no flag byte of its
own, so a per-block decision would be its own format change. `Reader::open`
resolves the layout once from `footer[48]` and threads it to every
`decode_entry`. Block framing, compression, the restart trailer and the index
are untouched — entry boundaries still come from `decode_entry`'s returned
`next`, so restart binary search, the B+tree index and the block CRC all keep
working unchanged.

**Extended footer prefix.** The 64-byte footer is full, so the aux-block handle
lives in the 16 bytes immediately preceding it:

```
[size-80 .. size-72)  aux_off u64   (0 when absent)
[size-72 .. size-64)  aux_len u64   (0 when absent)
[size-64 .. size)     the fixed 64-byte footer
```

Read at open and bounds-checked against the file before any allocation. The aux
block is `block.rs`-framed like every other block (so it is CRC-covered) and its
payload is a tagged section list:

```
aux payload := section_count uvarint | section × count
section     := tag u8 | len uvarint | payload[len]
tag 1 = range-delete fragments (defined by 1.2)
tag 2..  reserved
```

An unknown section tag is `UnsupportedFormat`, raised at `Reader::open` rather
than surfacing later as a silently missing section. 1.0 defines the container
and writes `aux_off = aux_len = 0`; 1.2 is the first producer. A legacy table
has no prefix at all (`Reader::aux_block_handle()` returns `None`).

### vlog layout

Concatenated per-value frames, addressed by `vlog_off` (frame start). Two frame
layouts exist; which one a table uses is a footer flag (`FOOTER_VLOG_V2`), not a
per-frame tag:

```
v1: [crc32c(value) u32][value bytes]                        (VLOG_CRC_LEN = 4)
v2: [crc32c(stored) u32][alg u8][stored_len u32][stored]    (VLOG_V2_HDR_LEN = 9)
```

In v2 the payload is the value compressed with `alg`, or the raw value with
`alg = None` when compression would not shrink it — so **the stored length never
exceeds the logical value length** (`val_len` in the klog entry), and a frame
claiming otherwise is corrupt. `stored_len` is a `u32`: the writer refuses a
value whose stored form reaches 4 GiB (`OndaError::TooLarge`) rather than
truncate the field, which would leave the CRC covering bytes no reader reads and
the next frame's offset pointing inside this one.

The CRC covers the stored bytes and is verified **once per frame per open
reader** (`Reader::verify_vlog_frame`), on both the file and mmap paths — the
same "immutable file, check it once" rule the klog's per-block `verified` bitmap
uses, and the same limit: a frame is re-verified when the table is re-opened, not
when it is re-read. A frame that fails is never marked, so corruption keeps being
reported on every subsequent read.

Older builds wrote unframed vlogs — no migration exists.

### Index

Flat (default): one entry per data block —

```
min_key_len uvarint | min_key | count uvarint |
{ sep_key_len uvarint | sep_key | seq uvarint | offset uvarint | length uvarint } × count
```

B+tree (`use_btree = true`, "hybrid klog"): bottom-up tree of meta blocks,
fanout 256 (`BTREE_FANOUT`). Node: `node_type u8 (1=leaf, 0=internal)` |
*(root only)* `min_key_len uvarint | min_key` | `count uvarint` | entries
(leaf: separator+seq+data-block handle; internal: separator+child handle).
The reader walks the tree at open and rebuilds the flat in-memory index —
`use_btree` changes the on-disk index layout only, not the engine.

### Footer (fixed 64 bytes at EOF)

```
offset  field
0..8    index handle offset      (u64)
8..16   index handle length
16..24  bloom handle offset      (0 if none)
24..32  bloom handle length
32..40  num_entries
40..48  max_seq
48      flags: FOOTER_HAS_BLOOM=0x01, FOOTER_BTREE=0x02,
               FOOTER_RESTARTS=0x04, FOOTER_VLOG_V2=0x08,
               FOOTER_EXTENDED_BLOCK=0x10
49..56  unused
56..64  FOOTER_MAGIC = 0x5741_5645_5353_5431
```

`KNOWN_FOOTER_FLAGS = 0x1F`. A bit outside that mask was written by a newer
binary and names a feature this one does not implement, so `Reader::open`
refuses the file with `OndaError::UnsupportedFormat` (code `-16`) rather than
`Corruption` — the file is intact, this binary is simply too old.

### Bloom filter (`bloom.rs`)

Classic k-hash (double hashing from one FNV-1a), sized from expected entries ×
`bloom_fpr` (default 0.01). Serialized dense or sparse (non-zero words only);
stored as a meta block, referenced by the footer.

## MANIFEST (`manifest.rs`)

Whole file, CRC32-C over everything before the trailing 4-byte CRC:

```
magic u32 = 0x5756_4D46 ("WVMF") | version u32 ∈ {1, 2}
| next_file_id u64 | global_seq u64 | cf_count uvarint
| per CF: name bytes* | config blob bytes* | sst_count uvarint
  | per SST: id, level, num_entries, num_tombstones, max_seq,
             klog_size, vlog_size (all uvarint) | min_key* | max_key*
| append-tolerant tail (0–4 sections, see below)
| crc32c u32
```

(`*` = uvarint length prefix + bytes.) The config blob is the
`ColumnFamilyConfig::encode` durable subset (comparator name, use_btree,
compression, sync mode, per-level/per-prefix compression, FIFO settings,
partition rules, tier rules — see § Config blob below). `Manifest::save` is
crash-atomic: write `MANIFEST.tmp` → `sync_all` → rename over `MANIFEST` →
parent-dir fsync. The temp path is fixed, so all saves MUST be serialized by
`DbInner::manifest_mu` (a past data-loss bug). A CRC-invalid manifest fails
`DB::open` (no partial recovery); a missing one is an empty database.

### Append-tolerant tail (SST metadata and WAL layout)

The per-SST record list above is a flat sequential encoding with no framing,
so optional per-record fields cannot be added in place without breaking older
readers. Instead they live in a tail between the last CF's records and the
CRC (the CRC covers the tail). Its four original sections are always in this
order; later tagged A2 sections are described below:

```
1. partition section   per CF, in manifest CF order:
                         count uvarint
                         { table_index uvarint | name* } × count
2. tier section        same shape as 1 (payload = tier name)
3. max-entry-time section
                         count uvarint
                         { table_index uvarint | value uvarint } × count
4. unified WAL layout    "ONDAWAL1" | layout u8 (1 = unified)
```

`table_index` is the table's position in that CF's `sst_count` list.
`value` in section 3 is `SstMeta::max_entry_time` cast to `u64` (nanoseconds
since the Unix epoch); tables not listed in a section decode that field as
`None`.

**Emission rules** (`Manifest::encode`) keep every earlier on-disk format
byte-identical — a later section is emitted only when all earlier ones
precede it, even if those are all-empty counts:

| Fields set anywhere in the manifest | Tail emitted |
|---|---|
| none                                | no tail at all (legacy layout, byte-identical to pre-0.3.0) |
| only `partition`                    | section 1 only (P1 layout) |
| any `tier`, no `max_entry_time`     | sections 1 + 2 (P3 layout) |
| any `max_entry_time`                | sections 1 + 2 + 3 |
| unified WAL layout                  | sections 1 + 2 + 3 + tagged section 4 |
| any tagged section (`ONDAOBJ1`, `ONDAINS1`, `ONDACAP1`, `ONDAAGE1`) | sections 1 + 2 + 3 (possibly all-empty) + the tags |

**Decoding** is positional for sections 1–3: after the CF loop, if bytes remain
before the CRC the first section is the partition section, the next (if bytes
remain) the tier section, the next the time section. Everything after that is
**tagged**, and is decoded by a dispatch loop (`decode_tagged_tails`) that reads
an 8-byte tag, hands the remainder to that tag's decoder, and repeats:

- a tag this binary does not know ⇒ `Corruption` (the loop's default arm);
- a residual shorter than 8 bytes, or a tag payload shorter than the tag
  requires ⇒ `Corruption`;
- a repeated tag ⇒ `Corruption` (the encoder emits each at most once, and a
  second copy would silently overwrite the first);
- `ONDAWAL1` accepts only layout byte `1`.

This rejects exactly what the previous fixed sequence rejected — an unknown
trailing tag was already `Corruption`, because the old `decode_wal_layout`
demanded an exact 9-byte residual. The loop shape is what lets a future tail
section be one more arm instead of another positional hazard.

A manifest that stops before the layout tag decodes as `PerColumnFamily`; every
legacy layout therefore decodes cleanly. Unified manifests emit the preceding
three sections even when empty so the tag is unambiguous.

**Compatibility rules:**

- *Old reader, new manifest*: pre-tail decoders ignored trailing body bytes
  (the CRC still validates — it covers the whole body), so a pre-0.3.0
  binary opens a 0.3.0 manifest without error. **But** it reconstructs every
  `SstMeta` without `partition`/`tier`/`max_entry_time`, and its next
  manifest rewrite (any flush/compaction) re-encodes without the tail —
  the metadata is silently and permanently stripped. For an untiered
  database that only loses partition stamps (re-derivable by the next
  bottom compaction); for a database with parts on a **named tier** it is
  fatal-on-reopen: the stripped `tier` makes the engine resolve those
  tables at the default-tier path, where the files do not exist. Do not
  downgrade a tiered database (see `docs/parts-and-tiers.md` § Downgrade).
- *New reader, old manifest*: a legacy (no-tail) manifest decodes with all
  three fields `None` on every table — the pre-partitioning semantics.

### Config blob (`ColumnFamilyConfig::encode`)

The per-CF config blob uses the same append-tolerant idea *inside* the blob:
a fixed prefix (comparator name`*`, compression u8, write_buffer_size u64,
level_size_ratio u64, klog_value_threshold u64, enable_bloom u8, bloom_fpr
f64-bits, l1_file_count_trigger u32, l0_queue_stall_threshold u32, use_btree
u8 — all little-endian fixed width unless marked) followed by appended tails
in this order: sync_mode u8 + sync_interval u64 (µs); compression_per_level
(count u8 + algs); compaction_style u8 + fifo_max_bytes u64 + fifo_ttl u64
(µs); compression_rules (count u8 + `{prefix* | alg u8}`); **partition_rules**
(count u8 + `{prefix* | name*}`); **tier_rules** (count u8 + `{prefix* |
tier_name* | min_age u64 (µs)}`). `decode_into` stops early on a short blob
via `?`, so a blob from any older version reconstructs the missing trailing
fields as struct defaults (empty rule lists).

Version 0.3.1 retains those four `u8` counts and their first 255 entries
byte-for-byte. If any list is longer, the normal config blob is followed by
`ONDAOVF1`, then four overflow lists in the same order. Each overflow list is
`extra_count uvarint` followed by the entries beyond index 254, using the same
entry encoding shown above. A 0.3.0 reader ignores this tagged tail and keeps
the first 255 entries; a 0.3.1 reader appends every overflow entry. The tag is
inside the manifest body and is therefore covered by the manifest CRC32-C.

Later tagged config tails follow in this fixed order:

```
ONDAPFN1 | scheme_name*                         derived partition function
ONDACMP1 | target_file_size u64 | l1_base_bytes u64
          | soft_pending_compaction_bytes u64
          | hard_pending_compaction_bytes u64   compaction geometry
ONDABLK1 | data_block_size u64                  per-CF block target
ONDAVVC1 | max_cached_vlog_value_bytes u64      per-CF vlog value cache limit
ONDABLM1 | count u64 | fpr f64-bits x count
          | optimize_filters_for_hits u8        per-level bloom policy
ONDAPRD1 | periodic_compaction_interval u64     microseconds; 0 = disabled (0.3)
```

`ONDAPRD1` is elided at the default (zero, disabled), so a family that never
sets it encodes byte-for-byte as earlier releases wrote it; it is refused by
`ColumnFamilyConfig::validate` on a `CompactionStyle::Fifo` family, which evicts
by age through `fifo_ttl` instead.

`ONDABLM1` is all-or-nothing: a truncated tail, or one holding a rate outside
`(0, 1)`, leaves both fields at their defaults (empty vector, `false`) rather
than applying half a policy. `ONDABLM2` is **reserved** for a future geometric
(Monkey-style) auto-allocation policy, which would be mutually exclusive with
the explicit vector; nothing writes or reads it yet.

Each tag is omitted when its setting is absent or equal to the release default.
Decoders consume only tags they recognize and leave missing or truncated tails
at defaults; all bytes remain covered by the enclosing manifest checksum.

## Unified-memtable WAL (`unified.rs`)

Same WAL format; file names `unified-wal-<gen>.log[.sN]`; record keys carry an
8-byte big-endian CF-id prefix (`cf_id = fnv64(cf_name)`). Split flush strips
the prefix and re-sorts each CF's slice with that CF's comparator. The manifest
tag above prevents reopening a non-empty database under a different WAL layout.

## A2 tail tags (0.7.8)

Two tagged manifest-tail sections follow the positional
(partition/tier/max-entry-time) sections, in fixed order, each self-identifying
by an 8-byte magic (the `ONDAWAL1` precedent):

- `ONDAOBJ1` — per-CF `(count, (table_index, object)...)` name section: the
  tier-root-relative object path of each shared-tier table
  (`SstMeta::object`). Emitted only when some table carries one.
- `ONDAINS1` — 8-byte per-database instance nonce naming this database's
  objects on shared tiers. Emitted once minted.

When any tagged section is present the encoder emits ALL positional sections
first (possibly all-empty), which is what lets the positional decoder consume
greedily without misreading a tag. Manifests carrying neither tag are
byte-identical to pre-A2 encodings.

## Capability tail tag (1.0)

```
ONDACAP1 | caps u64        (16 bytes total)
```

Decoded **after `ONDAINS1`, before `ONDAWAL1`** — the full emitted tail order is

```
[positional: partition | tier | max_entry_time]
[ONDAOBJ1 …] [ONDAINS1 nonce] [ONDACAP1 caps] [ONDAWAL1 layout]
[crc32c u32]
```

`ManifestTailPresence::tagged()` includes `caps`, and this wiring is
**load-bearing**: the positional decoder is gated only on non-emptiness, so a
caps tag emitted without the three positional sections ahead of it would be read
as a partition name section — silent corruption rather than rejection.

Version coupling, both directions:

- the header carries `2` **iff** `caps != 0`, and `1` otherwise (the same
  lowest-version discipline the positional tails follow), so a legacy-only
  database keeps writing bytes every previous binary can read;
- `ONDACAP1` in a VERSION-1 manifest is `Corruption` (the encoder bumps the
  version exactly when it emits the tag, so those bytes contradict themselves);
- `caps & !KNOWN_CAPS != 0` is `UnsupportedFormat`;
- a duplicate `ONDACAP1` is `Corruption`, like every other repeated tag.

A pre-1.0 binary checks the version by exact equality against `1` and therefore
refuses a v2 manifest outright. That refusal is proven by
`tests/frozen_decoder.rs`, which vendors a copy of the 0.8.2 header decode path
rather than trusting a constant this repository still owns.

## Periodic-age tail tag (0.3)

```
ONDAAGE1 | per CF, in manifest CF order:
             count uvarint
             { table_index uvarint | value uvarint } × count
```

The same `(table_index, u64)` section shape as the positional max-entry-time
section, carrying `SstMeta::last_compaction_time` — the wall-clock nanoseconds
at which a table was last *written by a compaction*. Tables not listed decode
that field as `None`, which the picker reads as **unknown, therefore never
eligible**.

Deliberately a **new** field rather than a reuse of `max_entry_time`. That field
carries the maximum forward over a compaction's inputs so re-compacting cold
data does not make it look freshly written — which is exactly what the part
mover's `TierRule::min_age` gate needs, and exactly what a periodic trigger must
not have: carrying it forward would leave a just-rewritten table instantly
re-eligible (a loop), and resetting it would break tier placement.

Decoded **after `ONDACAP1`, before `ONDAWAL1`**; the full emitted tail order is

```
[positional: partition | tier | max_entry_time]
[ONDAOBJ1 …] [ONDAINS1 nonce] [ONDACAP1 caps] [ONDAAGE1 …] [ONDAWAL1 layout]
[crc32c u32]
```

Order in the byte stream is a convention, not a requirement: the tag dispatch
loop is order-independent. `ManifestTailPresence::tagged()` includes this
section, for the same load-bearing reason `ONDACAP1` does.

Capability coupling, both directions:

- the section is emitted **iff** `caps & CAP_PERIODIC_AGE != 0` *and* some table
  carries a stamp, so a database that has not taken the capability writes the
  same bytes it always did (and, transitively, still writes VERSION 1);
- `ONDAAGE1` without `CAP_PERIODIC_AGE` in the same manifest is `Corruption`:
  nothing stamps a table before the bit is durable, so those bytes were
  truncated, hand-edited, or written by something that skipped the enable;
- a duplicate `ONDAAGE1` is `Corruption`, like every other repeated tag.

Who sets the field:

| Site | Stamp |
|---|---|
| `ColumnFamily::finish_writer_to_handle` (flush + ingest) | the injectable clock's reading |
| every output of one compaction (`CompactionOutputBuilder`) | one reading taken at job freeze, shared by all outputs |
| `parts.rs::relocate_part` (tier move/copy) | unchanged — the meta is cloned, the stamp rides along |
| `DB::attach_part` / `attach_part_by_ref` | left `None`, and therefore never eligible |
| the `CAP_PERIODIC_AGE` enable transition | `None` → the enable time, for local non-mounted tables, **in the same manifest write** that persists the capability |

The enable-time stamping is what makes the trigger restart-safe. "Eligible one
interval after open" is not: open time is not durable, so a database restarted
more often than its interval would never become eligible at all.
## Edit-log tail tag and `MANIFEST-EDITS` (2.2)

A full `MANIFEST` rewrite costs O(catalog) bytes and one fsync per structural
change — 12.4 MiB per persist at 100k parts, paid by every flush. With
`CAP_MANIFEST_EDITS` the durable catalog becomes a **periodic snapshot**
(`MANIFEST`, unchanged in shape) plus an **append-only log of numbered edits**
(`MANIFEST-EDITS`). Without the capability nothing changes: no log file is
created and the manifest is still rewritten in full, byte-for-byte as before.

### Snapshot tail tag

```
ONDAMED1 | generation u64 | applied_through u64 | next_edit_id u64   (32 bytes)
```

Decoded **after `ONDACAP1`, before `ONDAWAL1`**, so the full emitted tail order is

```
[positional: partition | tier | max_entry_time]
[ONDAOBJ1 …] [ONDAINS1 nonce] [ONDACAP1 caps] [ONDAMED1 …] [ONDAWAL1 layout]
[crc32c u32]
```

`ManifestTailPresence::tagged()` includes `edits`, for the same load-bearing
reason `caps` is in it. The tag is emitted only when the triple differs from
`(0, 0, 1)` — a database that has never had a log writes exactly the bytes it
wrote before 2.2 — and, like `ONDACAP1`, it forces header version 2 and is
`Corruption` inside a VERSION-1 manifest. `next_edit_id != applied_through + 1`
is `Corruption`: no writer produces that pairing. The whole-file CRC already
covers the tail, so no new checksum is introduced on the snapshot side.

`applied_through` is the highest edit id the snapshot already contains;
`generation` is **informational only** (see Recovery below).

### `MANIFEST-EDITS` header (fixed 28 bytes, at offset 0)

```
off  len  field
  0    4  magic  u32 LE = 0x4F4E_4445 ("ONDE"; on disk: 45 44 4E 4F)
  4    4  schema u32 LE = 1
  8    8  base_applied_through u64   — no record in this file has id <= this
 16    8  snapshot_generation  u64   — informational only
 24    4  crc32c u32 over bytes [0, 24)
```

The magic is ondaDB-namespaced on purpose: `"WD…"` is the wavesdb namespace and
the two engines are expected to share tiers, so a wavesdb-looking magic here
would invite cross-engine mount confusion. A file shorter than 28 bytes, a bad
header CRC, or an unknown magic/schema is `Corruption` — never a torn tail. The
header is written once, by snapshot compaction, and fsynced before any record.

### Record framing (records begin at offset 28, contiguous)

```
off  len   field
  0    4   len u32 LE    — payload byte count; the record occupies 8 + len bytes
  4    4   crc32c u32    — over the payload
  8  len   payload

payload:  edit_id u64 LE | op_count uvarint | op × op_count
op:       op_code uvarint | op payload
```

`len` is capped at `MAX_EDIT_RECORD_BYTES` = 64 MiB, checked **before any
allocation**.

The shape matches the WAL's `[len][crc][payload]` but the torn-tail contract is
the **opposite**, and the WAL's helpers must not be reused here: `wal::replay`
treats any unreadable trailing frame as a clean tail, whereas here only an
**EOF-truncated** frame header or payload ends replay cleanly. A complete record
with a bad CRC, an unknown op code, an out-of-sequence id or a failed
precondition is `Corruption`.

### Op payloads

`opt<T>` is `0x00` (None) or `0x01` followed by `T`; `str` is a length-prefixed
byte string, UTF-8 validated on decode. `sst_meta` mirrors the `SstMeta`
declaration order exactly, so the inventory guard reads as a field-by-field walk:

```
sst_meta: id, level, num_entries, num_tombstones, max_seq, klog_size,
          vlog_size (all uvarint) | min_key* | max_key*
        | partition opt<str> | tier opt<str>
        | max_entry_time opt<varint> | object opt<str>
```

`UpdateTable`'s field mask is a uvarint bitset whose present values follow in
**ascending bit order**; a mask of 0, or a bit above `0x10`, is `Corruption`:

```
0x01 Level uvarint | 0x02 Tier opt<str> | 0x04 Object opt<str>
0x08 Partition opt<str> | 0x10 MaxEntryTime opt<varint>
```

| code | op | payload | precondition |
| ---: | --- | --- | --- |
| 1 | `AddTable` | cf str, sst_meta | cf exists; id absent from it |
| 2 | `RemoveTable` | cf str, id, expected_level | id present at that level |
| 3 | `UpdateTable` | cf str, id, mask, values | id present in cf |
| 4 | `CreateCF` | name str, config bytes | name absent |
| 5 | `DropCF` | name str | name present; the same edit removed all its tables |
| 6 | `SetCFConfig` | name str, config bytes | name present |
| 7 | `SetNextFileID` | value uvarint | `value >= current` |
| 8 | `SetGlobalSeq` | value uvarint | `value >= current` |
| 9 | `SetWalLayout` | unified u8 (0/1) | one-way, per-CF → unified |
| 10 | `SetNonce` | nonce u64 | not yet minted |
| 11 | `SetCapability` | bits u64 | `KNOWN_CAPS` only |
| 12 | `RemoveTables` | cf str, count, id × count | every id present in cf |

Codes 13..63 are unassigned and reject as `Corruption` naming the code and the
op index; codes ≥ 64 are never assigned, mirroring the WAL's kind rule.

Column families are **name-keyed**: `CfManifest.name` is a CF's only catalog
identity (`unified::cf_id` is a WAL routing hash and is never a catalog
identity). Partition rules and tier rules are **not** manifest fields — they
travel inside the opaque `CfManifest.config` blob, so `CreateCF`/`SetCFConfig`
carry them.

Applying an edit is **all-or-nothing**: `apply_edit` validates every op against
a candidate that costs O(edit), then mutates. A failed precondition leaves the
manifest untouched.

### Snapshot compaction

Entirely under `manifest_mu`, with `N` = the last durable edit id:

1. write `MANIFEST.tmp` {generation `G+1`, `applied_through = N`,
   `next_edit_id = N+1`, full catalog}, fsync;
2. rename over `MANIFEST`, fsync the directory;
3. write `MANIFEST-EDITS.tmp` {header with `base_applied_through = N`}, fsync;
4. rename over `MANIFEST-EDITS`, fsync the directory again.

The live log is **never truncated in place**. A crash between steps 2 and 4
leaves the new snapshot beside the old log, which is consistent because replay
skips ids at or below `applied_through`. Both temp files are unlinked at open,
before anything is loaded, and are never read — a leftover temp is a crash
artifact, not state. The trigger, checked after each append in the same critical
section: `edit_bytes > max(4 MiB, snapshot_bytes)` or `edit_count > 4096`.

### Recovery

1. load and CRC-verify `MANIFEST` (a CRC-invalid one fails `DB::open`; a missing
   one is an empty database);
2. a log present without `CAP_MANIFEST_EDITS` in the snapshot is `Corruption` —
   a newer binary wrote state this one cannot interpret;
3. verify the log header and accept **iff
   `header.base_applied_through <= snapshot.applied_through`**. That is the
   whole predicate. `snapshot_generation` carries no decision power: the legal
   crash-between-steps-2-and-4 state has the snapshot at `G+1` while the
   surviving log still says `G`, so requiring equality would reject a consistent
   database;
4. record ids must be contiguous from `base_applied_through + 1`; ids at or
   below `applied_through` are skipped, the rest applied. A gap or a duplicate
   is `Corruption`;
5. an EOF-truncated frame ends replay cleanly; everything else is `Corruption`
   (see the framing note above). Reopening for append truncates the partial
   frame away, so the next record is reachable rather than stranded behind it;
6. a missing log is valid — it is the state before the first append, and the
   state a fresh backup/checkpoint destination is handed;
7. reconcile `next_file_id >= max table id + 1` and `global_seq >= max max_seq`;
8. read-only opens replay the log but never compact it and never write to it —
   not even the temp-file sweep.
