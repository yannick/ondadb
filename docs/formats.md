# On-disk formats — yoloDB format epoch 1

Every persisted byte, exactly. All fixed-width integers are **little-endian**;
varints are unsigned LEB128 (`uvarint`) or zig-zag LEB128 (`varint`) — see
`encoding.rs`. The integrity checksum everywhere is **CRC32-C** (Castagnoli;
`encoding::checksum`, the `crc32c` crate, check value `"123456789"` →
`0xE3069283`).

This is **format epoch 1** of the yoloDB family (plan C,
`docs/plans/phase-c-yolodb-convergence/plan.md`), which ondaDB writes from
0.10. Every magic, version, flag, capability bit, kind, codec id and config
tag named here is registered in [`format-registry.md`](format-registry.md) and
pinned in `src/format.rs` by a `const` assertion and a golden test; the byte
corpus is `tests/fixtures/epoch1/` (`tests/epoch1_golden.rs`). The ondaDB 0.9
formats this epoch replaced are summarized in the appendix; they are read only
by `src/legacy_onda/`.

A format change requires updating this file, the registry, the golden corpus
and a release note.

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
| `Corruption` (code `-5`) | the bytes contradict a format this binary *does* implement | unknown entry-flag bit, `SINGLE_DELETE` without `TOMBSTONE`, a manifest section a writer cannot produce, a record that fails to decode inside a CRC-valid WAL frame, any checksum mismatch outside a torn tail |
| `UnsupportedFormat` (code `-16`) | the bytes are well-formed but name a feature this binary does not implement | an unknown artifact version, a footer flag outside `0x03`, a capability bit outside `KNOWN_CAPS`, an unknown manifest section flag, a burned or unimplemented codec id, an assigned-but-unimplemented record kind (`< 64`), an unknown WAL envelope schema or segment layout, an unknown SST aux-section tag, an unknown config enum value, and any 0.9 artifact (read through `legacy_onda`) |

A record kind **≥ 64 is `Corruption`, not `UnsupportedFormat`**: that range is
never assigned to anything, so those bytes cannot have come from a newer writer.

A **torn tail** is neither: a short header, a short payload or a frame CRC
mismatch is the expected residue of a crash mid-write and ends that WAL stripe
cleanly (`Ok`).

### Format capabilities (`format.rs`)

A capability is the durable *permission* to write a newer artifact, taken once —
before the first byte using it exists — through
`DB::enable_format_capabilities`. The database's word is the fixed `caps u64`
field of the manifest header; each SSTable additionally declares the subset its
own bytes use in its footer's capability word (§ Footer).

| Bit | Symbol | Owner feature |
|---:|---|---|
| `1 << 0` | `CAP_EXTENDED_RECORDS` | 1.0 — kind-bearing record envelopes |
| `1 << 1` | `CAP_MERGE_OPERANDS` | 1.1 |
| `1 << 2` | `CAP_RANGE_DELETES` | 1.2 |
| `1 << 3` | `CAP_PREFIX_DELTA` | 2.1 |
| `1 << 4` | `CAP_MANIFEST_EDITS` | 2.2 |
| `1 << 5` | `CAP_PERIODIC_AGE` | 0.3 |
| `1 << 6` | `CAP_TXN_DECISIONS` | 3.2 — prepare/decision records; implies `CAP_EXTENDED_RECORDS` |

`KNOWN_CAPS = 0x7F`. The values are an interoperability contract with wavesdb:
a bit is never renumbered, only retired. Enabling is **one-way and idempotent**.
A bit outside `KNOWN_CAPS` in a manifest or a footer is `UnsupportedFormat`.

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
| 16 | prepare | 3.2 |
| 17 | commit decision | 3.2 |
| 18 | abort decision | 3.2 |
| 19–31 | reserved (transaction control) | — |
| 32–63 | reserved | — |
| ≥ 64 | **never assigned** | — |

Modifiers keep the legacy bit values so an extended entry and a legacy entry
describe the same thing with the same numbers: `HAS_TTL = 0x02`,
`HAS_VLOG = 0x04` (SSTable only), `modifiers::KNOWN = 0x06`. `TOMBSTONE` and
`SINGLE_DELETE` are *not* modifiers — they are kinds 2 and 3. An unknown
modifier bit is `Corruption`: modifiers are not capability-gated, so no writer
of any vintage may set one.

The byte patterns these rules accept are pinned by the epoch-1 golden corpus in
`tests/fixtures/epoch1/` (`tests/epoch1_golden.rs`), which the live encoders
must reproduce byte for byte and which the tests decode by hand. Those files
change only by an explicit, reviewed regeneration (the `#[ignore]`d
`regenerate_epoch1_fixtures`).

## WAL (`wal.rs`)

File set per generation (a generation = one memtable lifetime; its files are a
*segment*):

```
wal-<gen>.log            stripe 0 — its presence marks the generation
wal-<gen>.log.s1 .. .s3  stripes 1..3   (only for SyncMode::None/Interval)
```

`SyncMode::Full` uses a single stripe so group commit can amortize the fsync.
Committing threads own a sticky stripe (`my_stripe`), eliminating file-mutex
convoys. Replay reads all stripes; cross-stripe order is immaterial (seq
decides visibility). Deletion must use `wal::remove_wal_files(base)`.

### Segment header (32 bytes, offset 0 of every stripe file)

```
off len field
  0   8 magic "YOLODBWL"
  8   4 version u32 = 1
 12   1 layout u8          1 = per column family (envelope schema 1)
                           2 = unified            (envelope schema 2)
 13   3 reserved = 0
 16   8 generation u64     must equal the <gen> in the file name
 24   4 reserved = 0
 28   4 crc32c u32 over bytes 0..28
```

Frames start at offset 32. `Wal::open(path, mode, interval, SegmentId)` writes
the header into every new stripe and **fsyncs it before the open returns**, so
no frame is ever appended ahead of a durable header. That ordering is what makes
the torn-header rule safe:

| First bytes of a stripe | Replay | Reopen for append |
|---|---|---|
| zero-length file (created, never written) | empty | header written |
| a short prefix of the magic, or all zero, shorter than 32 bytes | empty (torn header) | truncated, header rewritten |
| exactly 32 bytes, CRC mismatch or all zero | empty (torn header) | truncated, header rewritten |
| anything else without the magic — a 0.9 WAL is frames from byte 0 | `UnsupportedFormat` at byte 0 | `UnsupportedFormat` |
| unknown version, unknown layout byte | `UnsupportedFormat` | same |
| CRC mismatch with frames behind it; reserved bytes set; a layout or generation other than the file's name and place say | `Corruption` | same |

A torn header can only sit on a file holding no frame, because the header was
durable before the first frame was written — so treating it as empty loses
nothing, exactly as a torn tail loses nothing.

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

A database enables no capability by default and therefore writes no envelopes.
Emission is decided **per batch, not per database**: `ColumnFamily::apply_commit`
(and `UnifiedStore::apply`) writes an envelope frame iff the batch carries
something the legacy record cannot spell — a record whose kind is not a point
kind (1.1's merge operand) or a range delete (1.2). `Wal::append_batch_enveloped`
is the codec entry point for a point-only batch, `append_batch_envelope` for one
mixing points and ranges. Checking the batch rather than the database keeps every
ordinary commit byte-identical to what 0.8.2 wrote, including on a family that
merely *has* a merge operator.

#### Kind 5 — range delete (1.2)

```
record := 5 | 0 (no modifiers) | alen uvarint | blen uvarint | seq uvarint
        | a bytes (= start) | b bytes (= end)
```

No TTL and no vlog: a range tombstone has no value to expire or separate, so a
non-zero modifier word here is `Corruption`, as is an empty bound (an empty
`end` would delete nothing and an empty `start` is indistinguishable from
absent). The interval is half-open — `end` is never itself deleted.

**Schema 1** carries user keys. **Schema 2** carries the 8-byte big-endian
cf-id prefix on **both** bounds, exactly as it does on a point key; a span can
never cross a cf-id boundary, because both bounds come from one `delete_range`
call on one column family, and that is asserted at encode rather than assumed.

A commit holding a range delete writes **one** envelope frame carrying both
kinds. One frame, because WAL batch atomicity (invariant 3) is per frame — two
frames could replay half a commit.

#### Kinds 16–18 — transaction control (3.2)

Durable prepared transactions write three frame shapes, all **schema 2**
(unified layout only: per-CF WALs cannot atomically establish a record across
independent logs). They are gated by `CAP_TXN_DECISIONS`, which
`enable_format_capabilities` expands to include `CAP_EXTENDED_RECORDS` — a
control record *is* a kind-bearing envelope record, so enabling one without the
other would authorize bytes the manifest does not describe. Both land in one
manifest write. With the bit unset, `Txn::prepare` is refused with
`InvalidArgs` naming the call that turns it on, and **no control frame is ever
written**: that is what makes the feature inert on disk before it is enabled.

**Every control record carries `seq = 0`.** `Wal::replay_file` derives its
high-water mark from record sequences, and that value becomes
`inner.observe_seq(unified_max_seq)`. A prepare frame must never raise the
watermark: its records are not committed and may yet be aborted. `0` is an
unambiguous sentinel — a real record can never carry it (`next_seq` starts at
`manifest.global_seq + 1 ≥ 1`) and `observe_seq` already returns early on it.
The watermark for a *committed* prepare comes from the decision instead, in
recovery pass 2, via an explicit `observe_seq(commit_seq + count - 1)`.

**Kind 16 — prepare.** One frame, `count = 1 + N`:

| # | kind | modifiers | alen | blen | seq | a | b |
|---|---|---|---|---|---|---|---|
| 0 | 16 | 0 | 16 | `8*C` | 0 | the 16-byte transaction id | `C` CF ids, `u64` **LE** each |
| 1..N | 1 / 2 / 3 | `HAS_TTL`? | `8+len(uk)` | value len | 0 | `cf_id` **BE** ‖ user key | value |

One frame for the id and the whole writeset, because a prepare is atomic exactly
as a commit is: a torn tail must drop both together, never leave a registered
reservation with half a writeset behind it. The cf-id list is little-endian
because it is a *payload* integer list; the prefix inside each key stays
big-endian because that is what orders the unified memtable.

**Kind 17 — commit decision.** One frame, `count = 1`:

| # | kind | modifiers | alen | blen | seq | a | b |
|---|---|---|---|---|---|---|---|
| 0 | 17 | 0 | 16 | 16 | 0 | id | `commit_seq u64 LE` ‖ `count u64 LE` |

`commit_seq` is the **first** sequence of the reserved block and `count` the
record count. Carrying both makes the decision self-sufficient: replay can raise
the watermark to `commit_seq + count - 1` without having found the prepare
frame. Without that, a crash between the decision fsync and the memtable apply
would leave no record carrying those sequences, `next_seq` would restart below
`commit_seq`, and the next ordinary commit would **reuse** them — a direct
violation of invariant 5.

**Kind 18 — abort decision.** One frame, `count = 1`: kind 18, `alen = 16`
(the id), `blen = 0`.

**Frame-shape rules, enforced at decode.** A control frame is decoded as a
*unit*, so recovery never infers grouping from callback adjacency:

- a frame whose first record is kind 16 holds exactly one kind-16 record
  followed by records of kinds 1/2/3 only, every record with `seq == 0` — any
  other kind, or any non-zero sequence, is `Corruption`;
- a frame whose first record is kind 17 or 18 has `count == 1` and `seq == 0`,
  and its `b` slot is exactly 16 bytes (17) or empty (18);
- a control kind appearing anywhere but as a frame's **first** record is
  `Corruption` — it names a placement no writer produces;
- an id that is not 16 bytes, or a cf-id list whose length is not a multiple of
  8, is `Corruption`.

The dispatch costs one uvarint peek per frame on the data path: the decoder
reads the first record's kind and only then chooses the control path, rather
than materializing every frame's records before classifying it.

## SSTable (`sst/`)

Two files: `<id>.klog` (always) and `<id>.vlog` (created lazily on the first
value with `len >= klog_value_threshold`, default 512 — WiscKey separation).

### klog layout

```
[data block 0] … [data block N-1] [bloom block?] [aux block?] [index block(s)] [footer 96B]
```

Every block (data/bloom/aux/index) is framed by `block.rs`:

```
[codec u8][comp_len u32][raw_len u32][crc32c(payload) u32][payload]
```

`codec` is an epoch-1 codec id (§ Codec ids); if compression does not shrink a
block it is stored with codec `0`. The CRC covers the stored payload, and it is
checked **before** the codec byte is interpreted, so a flipped codec byte is
`Corruption` and only an intact frame naming an unimplemented codec is
`UnsupportedFormat`. Data blocks target `ColumnFamilyConfig::data_block_size` raw
bytes (default 4 KiB), counting the restart trailer the block will carry — it
rides inside the framed payload, so a block cut on its entries alone overshoots
the target by `4 * R + 4`. Block handles make each file self-describing, so
changing the policy does not affect reads of existing tables.

**Every data block carries a restart trailer** (0.9 made it optional behind a
footer flag):

```
entries … | restart_off u32 × R | R u32
```

one anchor per `restart_interval` entries (`ColumnFamilyConfig::
block_restart_interval`, default 8, in `[1, 1024]`; `WriterOptions::
restart_interval = 0` — 0.9's "no trailer" — is refused). The first anchor is
at offset 0, anchors strictly increase, and point reads and seeks binary-search
them.

The entry layout is table-level and read from the footer's **capability word**:
without `CAP_EXTENDED_RECORDS` the *base* layout below; with it the extended
layout; with `CAP_PREFIX_DELTA` as well, the prefix-delta layout.

Base data-block entry (`sst::encode_entry` / `decode_entry`):

```
flags u8 | key_len uvarint | val_len uvarint | seq uvarint
| ttl varint (if HAS_TTL) | key bytes
| value bytes            (inline; if !HAS_VLOG)
| vlog_off u64           (if HAS_VLOG; val_len = logical value length)
```

Entries are appended in internal order; each block's index separator is the
block's **last** `(user_key, seq)` (shortened under a bytewise comparator).

#### Extended entry layout (`CAP_EXTENDED_RECORDS` in the footer word)

When the footer declares `CAP_EXTENDED_RECORDS`, **every** data-block entry in
the table uses the kind-bearing layout instead:

```
kind uvarint | modifiers uvarint | key_len uvarint | val_len uvarint
| seq uvarint | ttl varint (if modifiers & HAS_TTL) | key bytes
| value bytes | vlog_off u64   (as above, on modifiers & HAS_VLOG)
```

The choice is **table-level**, not per-block: a block carries no header byte of
its own, so a per-block decision would be its own format change (plan C step 2,
row B). `Reader::open` resolves the layout once from the footer and threads it
to every `decode_entry`. Block framing, compression, the restart trailer and the
index are untouched — entry boundaries still come from `decode_entry`'s returned
`next`.

#### Merge operands (kind 4, `CAP_MERGE_OPERANDS`)

A **merge operand** is a record that does not replace older versions of its key:
it composes with them. It is written as an ordinary extended entry with
`kind = 4`, its operand bytes in the value slot, and no TTL modifier — v1 puts
no expiry on an operand, because a per-operand expiry would resurrect the base
it was folded into.

Writer rule, the same shape as prefix-delta's: a family emits kind 4 only when
it has a merge operator **and** the database durably holds
`CAPS_MERGE_WRITE = CAP_EXTENDED_RECORDS | CAP_MERGE_OPERANDS`. Because the kind
exists only inside the extended entry, a family with an operator writes extended
tables from flush, ingestion and compaction alike; `Writer::add` refuses a
non-point kind on a base-layout table rather than emitting bytes that say
something else. A table that holds an operand declares `CAP_MERGE_OPERANDS` in
its footer word.

**The fold rule.** For key `k` at `read_seq`, over the versions of `k`
newest-first, considering only `seq <= read_seq`:

1. collect kind-4 entries into a list as they are seen;
2. the **base** is the first put, delete or single-delete met: a put gives
   `existing = Some(value)`, a delete (or a TTL-expired put) gives
   `existing = None`; versions older than the base are ignored;
3. sources exhausted without a base is also `existing = None`;
4. reverse the collected operands to oldest-first and return
   `full_merge(k, existing, &operands)`;
5. a group with **no** operand resolves exactly as it did before 1.1 — no
   operator call and no allocation.

`existing = None` and `Some(b"")` are a real distinction, and the same one the
point-read path already carries as found/deleted.

The operator is identified by *name*, persisted in the column family's config
blob (TLV tag 32, `merge_operator_name`) and re-resolved from
`Options::merge_fns` at every open. A stored name with no registered
implementation fails the open — never a silent fallback, which would read every
stored operand back as its own raw bytes. The stored name always wins, and
`create_column_family` on an existing family returns `Exists`, so folding with a
different operator than wrote the operands is unreachable through the API.

**Compaction folding.** Compaction may collapse a chain into the value it folds
to, writing one kind-1 entry that carries the **newest sequence the folded
suffix represented**. It is a space-and-read optimization, never a durability or
correctness requirement, and it is fenced by four rules:

* only a contiguous suffix wholly **at or below `oldest_snapshot`** folds — a
  live snapshot between two operands would otherwise read a different value;
* a suffix that meets no base among the job's inputs folds only at the
  **bottom** level, where "no base" really is `existing = None`; above it, the
  base may live deeper and the operands are written back unchanged;
* a base carrying a **live TTL** is not a fold target: the folded value would
  either inherit an expiry that also deletes the operands' contribution or lose
  one that must apply;
* a **foreign mount** among the inputs disables folding for the job (structurally
  impossible today — both input-selection paths filter mounts out — but folding
  is the one thing here that rewrites history, so it re-checks).

Retention is kind-aware in the same code that lets kind 4 reach compaction
(`VersionRetention::decide`): the "keep exactly one version at or below
`oldest_snapshot`" rule holds only for point kinds, so an operand neither
consumes that slot nor sets the flag, a bottom-level tombstone terminating a
live chain is not reclaimed, the bottom TTL drop never applies to an operand,
and an operand is not bloom-filter-eligible. Everything *older* than a chain's
base is still dropped exactly as before.

#### Prefix-delta entry layout (`CAP_PREFIX_DELTA` in the footer word)

When the footer declares `CAP_PREFIX_DELTA`, **every** data-block entry stores
only the key bytes it does not share with its predecessor
(`sst::encode_entry_delta` / `decode_entry_delta`):

```
kind uvarint | modifiers uvarint | shared_len uvarint | suffix_len uvarint
| val_len uvarint | seq uvarint | ttl varint (if modifiers & HAS_TTL)
| suffix bytes
| value bytes | vlog_off u64   (as above, on modifiers & HAS_VLOG)
```

The user key is `prev_key[..shared_len] ++ suffix`. This is the extended layout
with `key_len` split into `shared_len | suffix_len`; nothing else moves, and
that order is load-bearing twice: `kind`/`modifiers` stay first so `HAS_TTL` is
resolved before the `ttl` slot, and every varint precedes every
variable-length field so a decoder can bounds-check `shared_len`, `suffix_len`
and `val_len` **before** any memcpy.

`prev_key` is reset to empty at the start of every block *and* at every restart
anchor, so **every offset the restart array names decodes with
`shared_len = 0`** and is self-contained. That is what keeps the anchor binary
search free of materialization, and what makes bidirectional iteration
possible at all. Sharing never crosses a block boundary or an anchor.

The bit requires `CAP_EXTENDED_RECORDS` (the layout is defined only over the
extended entry); a footer declaring one without the other is `Corruption` — no
writer can produce it. Every block has anchors in epoch 1, so the 0.9 rule
"prefix-delta requires the restart flag" is now structural. Nothing else
changes: the restart trailer, the index block, the B+tree index, the bloom
block, block framing and the vlog are untouched.

Decoder validation rules, each with its own corruption test
(`tests/prefix_delta.rs`):

1. `shared_len <= prev_key.len()`, checked before the reconstruction memcpy.
2. `shared_len == 0` at every offset the restart array names.
3. A non-empty block has at least one anchor; the first is at offset 0; the
   offsets strictly increase and stay inside the entries region.
4. `suffix_len` and `val_len` fit the remaining entries-region bytes.
5. A restart run's walk lands exactly on the next anchor — and the last run's
   on the end of the entries region, so nothing sits between the last entry and
   the trailer.
6. `CAP_PREFIX_DELTA` without `CAP_EXTENDED_RECORDS` in the footer word.
7. Reconstructed keys are non-decreasing within a block. Exact under byte-wise
   ordering, which is where it is enforced; a custom comparator defines its own
   order, and threading a `ComparatorRef` vtable call into the per-entry decode
   would cost every scan (AGENTS.md invariant 7).

**Reading a delta table.** A key exists contiguously nowhere in the block, so
`SstIterator::key_block_ref` returns `None` and the merge iterator copies the
key into `CurKey::Buffered` — the path memtable children already take. Values
are unaffected: an inline value is still one contiguous run of block bytes and
keeps its pin (AGENTS.md invariant 8). Positioning enters through the restart
array (`restart_lower_bound`) and one materialized run; forward stepping needs
only a running previous-key buffer, since an anchor's `shared_len = 0` truncates
it away by itself.

Writing is gated on **both** `CAP_EXTENDED_RECORDS` and `CAP_PREFIX_DELTA` plus
`ColumnFamilyConfig::enable_prefix_delta_keys`. Turning the option off stops new
delta blocks; tables already written stay readable, in any level, part,
checkpoint or attach — the footer's capability word is read from the artifact,
never from the manifest.

### vlog layout

A 32-byte header, then concatenated per-value frames addressed by `vlog_off`
(the frame's absolute file offset, so every offset is ≥ 32):

```
header:  0 magic "YOLODBVL" | 8 version u32 = 1 | 12 flags u32 = 0
        | 16 reserved [12] = 0 | 28 crc32c u32 over bytes 0..28
frame:   [crc32c(stored) u32][codec u8][stored_len u32][stored]     (9-byte header)
```

The header is written when the writer creates the file (on its first large
value) and validated by the reader on its first vlog access — magic and CRC
`Corruption`, version and flags `UnsupportedFormat`, reserved bytes `Corruption`
— once per open reader, like the block and frame checksums. A frame offset below
32 is `Corruption`. The `flags` word is reserved for per-file dictionaries and
compression groups (plan C step 2); no bit is assigned in epoch 1.

The payload is the value compressed with `codec`, or the raw value with codec
`0` when compression would not shrink it — so **the stored length never exceeds
the logical value length** (`val_len` in the klog entry), and a frame claiming
otherwise is corrupt. `stored_len` is a `u32`: the writer refuses a value whose
stored form reaches 4 GiB (`OndaError::TooLarge`) rather than truncate the
field, which would leave the CRC covering bytes no reader reads and the next
frame's offset pointing inside this one.

The frame CRC covers the stored bytes and is verified **once per frame per open
reader** (`Reader::verify_vlog_frame`), on both the file and mmap paths — the
same "immutable file, check it once" rule the klog's per-block `verified` bitmap
uses, and the same limit: a frame is re-verified when the table is re-opened, not
when it is re-read. A frame that fails is never marked, so corruption keeps being
reported on every subsequent read.

Epoch 1 has exactly one frame format (0.9 had two, selected by a footer flag).

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

### Footer (fixed 96 bytes at EOF)

```
offset  field
 0..16  index handle        off u64 | len u64
16..32  bloom handle        off u64 | len u64     (0, 0 iff no FLAG_BLOOM)
32..40  num_entries u64
40..48  max_seq u64
48..52  flags u32           FLAG_BLOOM = 0x01, FLAG_BTREE = 0x02
52..56  format_version u32 = 1
56..64  capability word u64 (the table's subset of TABLE_CAPS)
64..80  aux handle          off u64 | len u64     (0, 0 when no aux block)
80..84  crc32c u32 over bytes 0..80
84..88  reserved u32 = 0
88..96  magic "YOLOST01"
```

`sst::decode_footer` checks, in order, and fails closed:

| Check | Error |
|---|---|
| magic is 0.9's `WAVESST1` | `UnsupportedFormat` (read it through `legacy_onda`) |
| magic is anything else | `Corruption` |
| `format_version != 1` | `UnsupportedFormat` — the CRC position is the version's to define |
| CRC32-C over bytes 0..80 | `Corruption` |
| reserved word non-zero | `Corruption` |
| a flag outside `0x03` | `UnsupportedFormat` |
| a capability bit outside `KNOWN_CAPS` | `UnsupportedFormat` |
| a database-level bit (`CAP_MANIFEST_EDITS`, `CAP_PERIODIC_AGE`, `CAP_TXN_DECISIONS`), or a table bit without `CAP_EXTENDED_RECORDS` | `Corruption` |
| any handle past the footer, `FLAG_BLOOM` disagreeing with the bloom handle, an empty aux handle with an offset | `Corruption` |
| `CAP_RANGE_DELETES` disagreeing with the aux block's range section (checked at open) | `Corruption` |

The flags carry no format meaning (plan C §1.1.3): restarts became
unconditional, one vlog frame format remains, and the extended/prefix-delta/
range meaning moved into the capability word. The footer is checksummed — 0.9's
was not.

### Aux block and the range-fragment section (tag 1, 1.2)

Every table's footer carries the aux-block handle (0.9 put it in 16 bytes ahead
of the footer, and only for extended tables). The block it addresses is
`block.rs`-framed like every other block, so its bytes are CRC-covered
(invariant 4), and its payload is a tagged section list:

```
aux payload := section_count uvarint | section × count
section     := tag u8 | len uvarint | payload[len]
tag 1 = range-delete fragments (1.2)
tag 2.. reserved
```

An unknown section tag fails the **open** with `UnsupportedFormat`: the block is
intact and names a feature this binary does not implement, and a silently
skipped section would be a silently missing range delete. A table carrying a
range section declares `CAP_RANGE_DELETES` in its footer word, and only such a
table does.

Section 1's payload:

```
payload  := count uvarint | fragment × count            (sorted by start)
fragment := slen uvarint | start | elen uvarint | end
          | nseq uvarint | seq uvarint × nseq           (newest → oldest)
```

Fragments of one table are **disjoint, sorted by `start`, and non-empty**, with
strictly descending sequence stacks — a file claiming otherwise is `Corruption`,
because the read path's binary search and its monotonic scan cursor both depend
on those properties. Tables with no fragments carry no aux block at all
(`aux_off = aux_len = 0` in the footer).

**Where fragments come from.** A flush emits its memtable's range tombstones
fragmented over the whole keyspace — one L0 file, one owned interval. A
compaction merges its inputs' fragments over the job span and **clips** each
output's copy to the interval that output owns:

```
[ job.span_min , o_2.min_key )        for i = 1   (extended down to the job span)
[ o_i.min_key  , o_{i+1}.min_key )    for 1 < i < n
[ o_n.min_key  , job.span_max ]       for i = n   (extended up to the job span)
```

Clipping is not an optimization. Level-≥1 point disjointness is what lets
`find_overlapping` binary-search `max_key`/`min_key` and return **at most one**
table per level; unclipped fragment bounds would let two adjacent tables both
cover a key, and the search would return one of them arbitrarily — a covering
tombstone would be missed and deleted data would resurrect. Because outputs are
already cut at partition boundaries, clipping also gives "no fragment crosses a
partition" for free.

The consequence to keep in mind when reading `SstMeta`: `range_min_key` may sort
**below** `min_key` and `range_max_key` **above** `max_key` — the gap between an
output's last point key and the next output's first belongs to the earlier
output. Both stay inside the job span.

### Bloom filter (`bloom.rs`)

Classic k-hash (double hashing from one xxh3-64 of the user key, seed 0), sized
from the keys a table actually holds × the level's false-positive rate. Stored
as a meta block, referenced by the footer:

```
hash u8 = 1 (xxh3-64) | m uvarint | k uvarint | words u64 LE × ceil(m / 64)
```

The hash id **leads** (wavesdb's layout; 0.9 trailed it). Probing is
`bit_i = (h1 + i·h2) mod m` with `h1 = h as u32`, `h2 = (h >> 32) as u32`, bits
LSB-first in u64 words — identical in both engines. `Bloom::decode` refuses a
hash id other than 1 as `UnsupportedFormat` (0 is 0.9's FNV) and `m` outside
`[1, 2^32)`, `k` outside `[1, 30]`, a short word array or trailing bytes as
`Corruption`.

### Codec ids

| Id | Codec |
|---:|---|
| 0 | none |
| 1 | snappy |
| 2 | **burned** (0.9 LZ4 / wavesdb zstd) — `UnsupportedFormat` |
| 3 | zstd |
| 4 | **burned** (0.9 LZ4-fast / wavesdb zstd) — `UnsupportedFormat` |
| 5 | raw deflate |
| 6 | raw LZ4 block — `Compression::Lz4` and `Lz4Fast` both write it |
| 7 | zstd with a per-file dictionary — reserved (step 2) |
| 8 | brotli — reserved |

`Compression` is an API enum; `Compression::codec_id` / `from_codec_id` are the
only mapping to and from the byte.

## MANIFEST (`manifest.rs`)

Whole file, CRC32-C over everything before the trailing 4-byte CRC:

```
off  field
  0  magic "YOLODBMF"
  8  version u32 = 1
 12  caps u64                       the database's capability word
 20  db_flags u32                   sections present (strict mask)
 24  next_file_id u64
 32  global_seq u64
 40  [0x2 DB_INSTANCE_NONCE]  nonce u64
     [0x4 DB_EDIT_LOG]        generation u64 | applied_through u64 | next_edit_id u64
     cf_count uvarint
     per CF:  cf_flags uvarint | name* | config* | [0x1 CF_UNIFIED_ID] id u64
              | sst_count uvarint
       per SST: id, level, num_entries, num_tombstones, max_seq, klog_size,
                vlog_size (all uvarint) | min_key* | max_key*
                | sst_flags uvarint
                | [0x01 SST_PARTITION]            name*
                | [0x02 SST_TIER]                 name*
                | [0x04 SST_OBJECT]               stem*
                | [0x08 SST_MAX_ENTRY_TIME]       uvarint (i64 as u64)
                | [0x10 SST_LAST_COMPACTION_TIME] uvarint (i64 as u64)
                | [0x20 SST_RANGE]                range_count | range_min_seq
                                                  | range_max_seq | min_key* | max_key*
     crc32c u32
```

(`*` = uvarint length + bytes.) `DB_UNIFIED_WAL` (`0x1`) has no payload: its
presence is the unified WAL layout. Every optional field is a **flagged
section**, present in ascending bit order exactly when its value differs from
the empty default — the discipline wavesdb's v3/v4 manifest proved out,
replacing 0.9's positional tails and `ONDA*` 8-byte tagged tails (whose
"tagged sections force the positional ones" rule was load-bearing and a
silent-corruption hazard). `Manifest::save` is crash-atomic: write
`MANIFEST.tmp` → `sync_all` → rename over `MANIFEST` → parent-dir fsync. The
temp path is fixed, so all saves MUST be serialized by `DbInner::manifest_mu`
(a past data-loss bug). A missing manifest is an empty database.

**Decoding** checks the whole-file CRC first — any flipped bit is `Corruption`
— then fails closed:

| Condition | Error |
|---|---|
| 0.9 magic `WVMF` | `UnsupportedFormat` (read through `legacy_onda`) |
| any other foreign magic, CRC mismatch, truncation, trailing bytes after the last CF | `Corruption` |
| version ≠ 1; a capability bit outside `KNOWN_CAPS`; an unknown db / cf / sst flag bit | `UnsupportedFormat` |
| `DB_EDIT_LOG` at the empty default `(0, 0, 1)`, or with `next_edit_id ≠ applied_through + 1` | `Corruption` |
| `SST_LAST_COMPACTION_TIME` without `CAP_PERIODIC_AGE`; `SST_RANGE` without `CAP_RANGE_DELETES` | `Corruption` — nothing stamps a table before the bit is durable |
| a range summary naming zero fragments, an empty bound, or `min_seq > max_seq` | `Corruption` |
| `CF_UNIFIED_ID` equal to FNV-1a-64 of the name | `Corruption` — the derived id is never written |
| a table level above `u32`, invalid UTF-8 in a name | `Corruption` |

The config blob is the `ColumnFamilyConfig::encode` TLV (§ Config blob);
partition and tier rules travel inside it.

### Per-table fields

`partition` is set for bottom-level files compaction cut on a partition
boundary; `tier` names the storage tier holding a part (`None` = the database
directory); `object` is the tier-root-relative stem of a table on a **shared**
tier (`cf-{cf}/{instance:016x}-{id}`, or adopted verbatim by
`attach_part_by_ref`); `max_entry_time` is the approximate wall-clock age the
part mover's `TierRule::min_age` gate reads (carried as the maximum over a
compaction's inputs).

`last_compaction_time` is the wall-clock time a table was last *written by a
compaction* — deliberately not `max_entry_time`, which carries forward and would
leave a just-rewritten table instantly re-eligible for periodic compaction. It
is persisted only under `CAP_PERIODIC_AGE`; unknown (`None`) is never eligible.

| Site | Stamp |
|---|---|
| `ColumnFamily::finish_writer_to_handle` (flush + ingest) | the injectable clock's reading |
| every output of one compaction (`CompactionOutputBuilder`) | one reading taken at job freeze, shared by all outputs |
| `parts.rs::relocate_part` (tier move/copy) | unchanged — the meta is cloned, the stamp rides along |
| `DB::attach_part` / `attach_part_by_ref` | left `None`, and therefore never eligible |
| the `CAP_PERIODIC_AGE` enable transition | `None` → the enable time, for local non-mounted tables, **in the same manifest write** that persists the capability |

The **range summary** is the catalog's view of a table's aux range section: how
many fragments it holds, the lowest and highest sequence in any stack, and the
lowest `start` / highest `end` it owns. The read path's gap-owner rule needs
`range_count` and `range_max_key` before any reader is opened, `gather_target`
needs the span bounds to size a compaction job, and delete-only excise reads
`range_count`; `attach_part` and `attach_part_by_ref` re-derive all five fields
from the incoming table's decoded aux section rather than trusting a foreign
catalog. `range_min_key` may sort **below** `min_key` and `range_max_key`
**above** `max_key` — fragments are clipped to the output *interval*, which
reaches past a table's first and last point key.

### The unified column-family id (`CF_UNIFIED_ID`)

Under the unified WAL layout every key in the shared WAL and memtable carries an
8-byte big-endian column-family id. It defaults to **FNV-1a-64 of the name**
(the standard offset basis `14695981039346656037`, as wavesdb computes it); a
family whose id diverges from that stores it here. Epoch-1 writers never produce
a divergent id yet — the section is the ground plan C F5′ (clearing a family
under the unified layout by giving it a fresh id) builds on, and it is how a
0.9 directory opened through `legacy_onda` keeps the truncated-basis ids its WAL
keys carry.

### Config blob (`ColumnFamilyConfig::encode`, `config_blob.rs`)

```
blob  := magic "YOLODBCF" | version u32 = 1 | entry*
entry := tag uvarint | len uvarint | value[len]
```

One tag per durable `ColumnFamilyConfig` field (the table is in
[`format-registry.md`](format-registry.md#cf-config-tlv-tags)). Entries are in
**strictly ascending** tag order; a field at its default is **elided** —
except `compression_per_level` (tag 13), written whenever non-empty because
its default moved in 0.10 and an absent tag 13 must keep meaning *empty* — so a
default family's blob is the 12-byte header plus tag 13; durations are **nanoseconds**.
Scalars are exactly one minimal uvarint, booleans one byte `0`/`1`, codec
fields epoch-1 codec ids, lists `count uvarint | item*` with no bytes left over.

A tag this binary does not know is **preserved**: `decode` keeps it on
`ColumnFamilyConfig::unknown_config_tags` and the next `encode` writes it back
verbatim, merged into tag order (an unknown tag can sit *below* a known one — the
reserved tag 33 under the known 34 and 35), so rewriting a family's config never strips an option a newer binary
— or another yoloDB engine — stored there (plan C step 2 row G). Everything else
is strict, because the blob sits inside a CRC-verified manifest or edit record:
a wrong magic, a short entry, a tag out of order or repeated, tag 0, or a known
tag whose value does not parse is `Corruption`; an unknown version or a known
tag naming an enum value this binary lacks (a codec, a sync mode, a compaction
style) is `UnsupportedFormat`. This replaces 0.9's lenient positional decoder,
which fell back to defaults on anything it did not understand — and read
wavesdb's JSON config as a 123-byte comparator name.

The merge operator and the derived partition scheme are persisted by **name**
(tags 32 and 20) and re-resolved from `Options::merge_fns` /
`Options::partition_fns` at every open. A stored name with no registered
implementation fails the open — never a silent fallback, which would read every
stored operand back as its own raw bytes. The stored name always wins.

## Unified-memtable WAL (`unified.rs`)

Same WAL format with layout byte 2; file names `unified-wal-<gen>.log[.sN]`;
record keys carry the 8-byte big-endian CF-id prefix (§ The unified column-family
id). Split flush strips the prefix and re-sorts each CF's slice with that CF's
comparator. The manifest's `DB_UNIFIED_WAL` flag prevents reopening a non-empty
database under a different WAL layout.

## `MANIFEST-EDITS` (2.2)

A full `MANIFEST` rewrite costs O(catalog) bytes and one fsync per structural
change — 12.4 MiB per persist at 100k parts, paid by every flush. With
`CAP_MANIFEST_EDITS` the durable catalog becomes a **periodic snapshot**
(`MANIFEST`) plus an **append-only log of numbered edits** (`MANIFEST-EDITS`).
Without the capability no log file is created and the manifest is rewritten in
full.

### Snapshot cursor (`DB_EDIT_LOG`)

The snapshot's `DB_EDIT_LOG` section — `generation | applied_through |
next_edit_id` — is present only when the triple differs from `(0, 0, 1)` (a
checkpoint or backup destination is stamped generation 1 with nothing applied). `applied_through` is the highest edit id the
snapshot already contains; `generation` is **informational only** (see
Recovery). `next_edit_id != applied_through + 1` is `Corruption`.

### `MANIFEST-EDITS` header (fixed 32 bytes, at offset 0)

```
off  len  field
  0    8  magic "YOLODBED"
  8    4  schema u32 = 1
 12    8  base_applied_through u64   — no record in this file has id <= this
 20    8  snapshot_generation  u64   — informational only
 28    4  crc32c u32 over bytes [0, 28)
```

A file shorter than 32 bytes, a foreign magic or a bad header CRC is
`Corruption`; an unknown schema, and 0.9's `ONDE` log, are `UnsupportedFormat` —
never a torn tail. The header is written once, by snapshot compaction, and
fsynced before any record.

### Record framing (records begin at offset 32, contiguous)


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

## Appendix: the ondaDB 0.9 formats (read only by `legacy_onda`)

Epoch 1 replaced every 0.9 container; the 0.9 decoders live, frozen and
decode-only, in `src/legacy_onda/` behind the default-on `legacy-onda` cargo
feature, pinned against `tests/fixtures/legacy-onda/` (byte corpus plus three
database directories written by 0.9.1). `legacy_onda::open_read_only` opens a
0.9 directory read-only through the engine; building without the feature makes
every 0.9 artifact a hard `UnsupportedFormat`.

| Artifact | 0.9 | Epoch 1 |
|---|---|---|
| checksum | CRC-32/**IEEE** (documented as CRC32-C) | CRC32-C |
| SST footer | 64 B, magic `WAVESST1` u64, unchecksummed, flag byte `0x01` bloom `0x02` btree `0x04` restarts `0x08` vlog-v2 `0x10` extended `0x20` prefix-delta; aux handle in the 16 bytes before it (extended tables only) | 96 B, `YOLOST01`, CRC32-C, flags bloom/btree only, capability word, aux handle inside |
| restart trailer | optional (flag `0x04`) | every block |
| vlog | no header; v1 `crc \| value` or v2 frames | 32-byte header; v2 frames only |
| bloom | `m \| k \| words \| [tag]`, absent tag = FNV (basis `1469598103934665603`) | `tag \| m \| k \| words`, xxh3 only |
| codec ids | 0–5, `2`/`4` = LZ4 | 0, 1, 3, 5, `6` = LZ4; 2/4 burned |
| MANIFEST | `WVMF` u32, v1/v2 by capability, positional body + positional tails + `ONDA*` tagged tails | `YOLODBMF`, one version, flagged sections |
| MANIFEST-EDITS | 28-byte `ONDE` u32 header | 32-byte `YOLODBED` header, same record framing |
| WAL | no header | 32-byte `YOLODBWL` segment header |
| config blob | positional fields + `ONDA*` tails, µs durations, lenient | `YOLODBCF` TLV, ns durations, strict, unknown tags preserved |
| unified CF id | FNV-1a with the truncated basis | FNV-1a-64 (correct basis), overridable per CF |
