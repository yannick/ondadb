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
`TOMBSTONE=0x01, HAS_TTL=0x02, HAS_VLOG=0x04, DELTA_SEQ=0x08 (reserved),
SINGLE_DELETE=0x10`.

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

Replay (`Wal::replay`): short/torn header or payload, CRC mismatch, or a
record that fails to decode ⇒ clean end of that stripe (expected crash
residue). A frame is applied all-or-nothing. WAL bytes are never compressed.

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
target 4 KiB raw (`DATA_BLOCK_SIZE`).

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

Concatenated per-value frames, addressed by `vlog_off` (frame start):

```
[crc32c(value) u32][value bytes]        (VLOG_CRC_LEN = 4)
```

The CRC is verified on every read (`Reader::read_vlog_into`), both file and
mmap paths. Older builds wrote unframed vlogs — no migration exists.

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
48      flags: FOOTER_HAS_BLOOM=0x01, FOOTER_BTREE=0x02
56..64  FOOTER_MAGIC = 0x5741_5645_5353_5431
```

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
| crc32c u32
```

(`*` = uvarint length prefix + bytes.) The config blob is the
`ColumnFamilyConfig::encode` durable subset (comparator name, use_btree,
compression, …). `Manifest::save` is crash-atomic: write `MANIFEST.tmp` →
`sync_all` → rename over `MANIFEST` → parent-dir fsync. The temp path is
fixed, so all saves MUST be serialized by `DbInner::manifest_mu` (a past
data-loss bug). A CRC-invalid manifest fails `DB::open` (no partial
recovery); a missing one is an empty database.

## Unified-memtable WAL (`unified.rs`)

Same WAL format; file names `unified-wal-<gen>.log[.sN]`; record keys carry an
8-byte big-endian CF-id prefix (`cf_id = fnv64(cf_name)`). Split flush strips
the prefix and re-sorts each CF's slice with that CF's comparator.
