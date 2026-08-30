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
| `UnsupportedFormat` (code `-16`) | the bytes are well-formed but name a feature this binary does not implement | a footer flag bit outside `KNOWN_FOOTER_FLAGS` |

A **torn tail** is neither: a short header, a short payload or a frame CRC
mismatch is the expected residue of a crash mid-write and ends that WAL stripe
cleanly (`Ok`).

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
               FOOTER_RESTARTS=0x04, FOOTER_VLOG_V2=0x08
49..56  unused
56..64  FOOTER_MAGIC = 0x5741_5645_5353_5431
```

`KNOWN_FOOTER_FLAGS = 0x0F`. A bit outside that mask was written by a newer
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
magic u32 = 0x5756_4D46 ("WVMF") | version u32 = 1
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
```

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
