# 1.0 — Strict decoding, manifest-v2 capabilities, and record envelopes

**Readiness:** design ready; split into two independently reviewable changes:
legacy hardening first (**1.0A**), v2 scaffolding second (**1.0B**, with no
capability enabled). **Effort:** 3–5 dev-weeks. **Baseline:** ondaDB 0.8.2
(`3afc3c1`); branch `roadmap/wave-a`. **wavesdb counterpart:** 1.0 — the
flag-exhaustion argument transfers verbatim (ondaDB: five of eight entry-flag
bits consumed, `DELTA_SEQ` dead; three remain; the roadmap needs merge,
range-delete, and transaction-control kinds).

## Goal

Fail closed on bytes the engine does not understand, and create the extensible
capability/kind scheme **before** any feature writes a new artifact.

## Baseline (verified against 0.8.2)

- Entry flags (`format.rs:10–21`): `TOMBSTONE 0x01, HAS_TTL 0x02, HAS_VLOG
  0x04, DELTA_SEQ 0x08 (never written by any writer), SINGLE_DELETE 0x10`.
  `wal::decode_record` (`wal.rs:103–129`) tests only `TOMBSTONE`,
  `SINGLE_DELETE`, `HAS_TTL`; `sst::decode_entry` (`sst/mod.rs:250–292`) tests
  only `HAS_TTL`, `HAS_VLOG`. Both **ignore unknown bits and invalid
  combinations**.
- Writer-produced combinations are exactly `{}`, `{TOMBSTONE}`,
  `{TOMBSTONE|SINGLE_DELETE}`, `{HAS_TTL}`, `{HAS_VLOG}`, `{HAS_VLOG|HAS_TTL}`.
  There are three flag-producing sites: `memtable::flag_bits` (`memtable.rs:255`),
  `wal::encode_record_body` (`wal.rs:79–98`), `sst::encode_entry`
  (`sst/mod.rs:210–247`) via `Writer::add` (`sst/writer.rs:218`).
- `Writer::add` sets `has_vlog` only when `!tombstone`
  (`sst/writer.rs:249–254`), so `TOMBSTONE|HAS_VLOG` is unproducible.
  **`flag_bits` does *not* imply `TOMBSTONE` from `single_delete`** — it ORs
  the two booleans independently. The `SINGLE_DELETE ⇒ TOMBSTONE` invariant
  holds only by caller convention (`Txn::single_delete` passes
  `tombstone = true` itself), and `wal::RecordRef` is `pub` (`lib.rs` exports
  `pub mod wal` with `pub fn append_batch`), so
  `RecordRef { tombstone: false, single_delete: true }` is constructible
  outside the crate. This is why Change A normalizes at every *encode* site,
  not only at decode.
- Footer flags (`sst/mod.rs:39–53`): `FOOTER_HAS_BLOOM 0x01, FOOTER_BTREE 0x02,
  FOOTER_RESTARTS 0x04, FOOTER_VLOG_V2 0x08`. `Reader::open`
  (`sst/reader.rs:230–238`) extracts known bits from `footer[48]` and ignores
  the rest. Footer bytes `49..56` are unused; `56..64` hold `FOOTER_MAGIC`.
- Manifest (`manifest.rs:20–24`): `MAGIC 0x5756_4D46`, `VERSION = 1` checked by
  **exact equality** (`:329`) — any version bump is automatically a fail-closed
  boundary for old binaries. Positional tails (partition → tier →
  max_entry_time, `:378–390`) then tagged tails (`OBJECT_TAG` → `INSTANCE_TAG`,
  `:391–404`) then an exact `WAL_LAYOUT_TAG` residual (`:406–419`).
  **An unknown trailing tag is already a hard `Corruption`** — `decode_wal_layout`
  demands an exact 9-byte residual; nothing is "consumed by accident". The real
  gap is the absence of per-tag *dispatch*, which is what 1.0B needs.
- `MAX_MANIFEST_LEVEL = 64` (`column_family.rs:31`) is enforced before
  allocation (`:437–443`, test `manifest_level_above_limit_is_corruption`).
  **This bound is not changed by this feature** — lowering it would reject
  existing manifests for no stated benefit.
- A WAL frame with `payload_len == 0` is **benign and skipped**: `read_full`
  (`wal.rs:532`) returns `Ok(Some(()))` for an empty buffer, `checksum(&[])`
  matches, and the record loop `while !p.is_empty()` (`wal.rs:510`) does
  nothing. `Wal::append_batch(&[])` is public API and produces exactly such a
  frame, so an externally driven WAL can legitimately hold one.
- `OndaError` (`error.rs:14–46`) is `#[non_exhaustive]`; there is no
  `UnsupportedFormat` variant. `Busy(String)` (code `-14`) exists but is
  **never constructed outside `error.rs`** — a dead variant. Do not repurpose
  it; it is listed here only so a reader does not assume it is live.

## Error taxonomy (pinned by this feature)

| Error | Meaning | Examples |
| --- | --- | --- |
| `Corruption` | bytes contradict a format this binary *does* implement | unknown entry-flag bit, `SINGLE_DELETE` without `TOMBSTONE`, unknown manifest tag magic, torn structure inside a CRC-valid payload |
| `UnsupportedFormat` (**new**, code `-16`) | bytes are well-formed but name a feature this binary does not implement | unknown footer flag bit, unknown capability bit in the manifest caps word, unknown record kind `< 64`, unknown aux-block section tag |

Kind `≥ 64` is `Corruption`, not `UnsupportedFormat`: those values are never
assigned, so they cannot come from a newer writer.

## Change A — strict legacy masks (1.0A, ships alone)

1. `KNOWN_ENTRY_FLAGS = TOMBSTONE|HAS_TTL|HAS_VLOG|SINGLE_DELETE` (`0x17`).
   `flags::DELTA_SEQ` is **deleted**; `0x08` becomes reserved-unknown.
   `wal::decode_record` and `sst::decode_entry` reject `fl & !KNOWN_ENTRY_FLAGS`
   with `Corruption`. The memtable-side flag consumers are `memtable.rs:304,
   334, 430` and `memtable_arena.rs:225` (the latter compiled only under
   `unsafe-fastpath`) — they consume `flag_bits` output, so they need the
   *encode*-side normalization of item 3, not a decode check. (The plan
   previously said "`MemFilter`-side readers"; `MemFilter` is a presence
   filter and decodes no flags.)
2. Invalid combinations rejected at decode, exhaustive from the writer
   contract: `TOMBSTONE|HAS_VLOG`, and `SINGLE_DELETE` without `TOMBSTONE`.
3. **Encode-site normalization** (this is what makes item 2 safe): `flag_bits`,
   `wal::encode_record_body` and `sst::encode_entry` each assert the invariant
   in debug and normalize in release — `single_delete ⇒ tombstone`,
   `tombstone ⇒ !has_vlog`. Without this, strictness converts a latent internal
   or downstream bug into an unopenable database.
4. Footer: `flags & !KNOWN_FOOTER_FLAGS` → `UnsupportedFormat` naming the mask.
5. Manifest: replace the fixed `decode_tagged_tails` sequence + exact
   `decode_wal_layout` residual with a **dispatch loop** whose default arm is
   `Corruption` (see Wire format below). This adds no rejection that does not
   already exist; it makes the tail set extensible.
6. `decode_record` becomes `Result<(Record, usize)>` (from `Option`), and
   `replay_file` (`wal.rs:483–522`) splits the two tail cases:
   - torn header / torn payload / CRC mismatch → `Ok(last_seq)` (clean stop —
     the expected crash outcome; 1.2's `range_torn_frame` row depends on it),
   - CRC-valid but undecodable record → `Err(Corruption)` propagated out of
     `Wal::replay`.
   Both replay callers are audited: `ColumnFamily::load` (`column_family.rs:491`)
   and `UnifiedStore::open` (`unified.rs:228`).
7. **Empty frames stay skipped.** `payload_len == 0` is *not* rejected (see
   Baseline). A frozen fixture pins the behavior either way.
8. `MAX_MANIFEST_LEVEL` is **unchanged** at 64.
9. `OndaError::UnsupportedFormat(String)` lands here (task 0 below) so items 4
   and 1.0B share one taxonomy from the start.

**Frozen-corpus-first ordering is binding**: the golden corpus (task 1) is
generated by the *current* decoders and byte-pinned before a single strictness
check is written. Change A must not alter any outcome for valid legacy bytes.

## Change B — manifest v2, capability word, record envelopes (1.0B)

**Manifest v2.** `VERSION = 2`, accepted alongside 1. The encoder writes 2 iff
`caps != 0`, otherwise 1 — the existing lowest-version discipline
(`encode_positional_tails`). `FORMAT_CAPS_TAG` in a VERSION-1 manifest is
`Corruption`. Unknown capability bits reject the open with `UnsupportedFormat`
naming the mask.

**Capability registry** — single owner `format.rs`, values pinned here and
golden-pinned in fixtures. wavesdb is reconciled to these values; that
reconciliation is a note, not a gate.

```rust
// new, format.rs
pub const CAP_EXTENDED_RECORDS: u64 = 1 << 0;   // kind-bearing envelopes (1.0)
pub const CAP_MERGE_OPERANDS:   u64 = 1 << 1;   // 1.1
pub const CAP_RANGE_DELETES:    u64 = 1 << 2;   // 1.2
pub const CAP_PREFIX_DELTA:     u64 = 1 << 3;   // 2.1
pub const CAP_MANIFEST_EDITS:   u64 = 1 << 4;   // 2.2
pub const CAP_PERIODIC_AGE:     u64 = 1 << 5;   // 0.3
pub const CAP_TXN_DECISIONS:    u64 = 1 << 6;   // 3.2
pub const KNOWN_CAPS: u64 = CAP_EXTENDED_RECORDS | CAP_MERGE_OPERANDS
    | CAP_RANGE_DELETES | CAP_PREFIX_DELTA | CAP_MANIFEST_EDITS
    | CAP_PERIODIC_AGE | CAP_TXN_DECISIONS;                 // = 0x7F
```

**Record kinds** — single owner `format.rs`, pinned:

```rust
pub const KIND_PUT: u64 = 1;
pub const KIND_DELETE: u64 = 2;
pub const KIND_SINGLE_DELETE: u64 = 3;
pub const KIND_MERGE: u64 = 4;          // 1.1
pub const KIND_RANGE_DELETE: u64 = 5;   // 1.2
// 6..15   reserved for future data kinds
// 16..31  transaction control (3.2)
// 32..63  reserved
// >= 64   never assigned -> Corruption
pub const MAX_ASSIGNABLE_KIND: u64 = 63;
```

**Modifiers** keep the legacy bit values so an extended entry and a legacy
entry describe the same thing with the same numbers:

```rust
pub mod modifiers {
    pub const HAS_TTL:  u64 = 0x02;   // == flags::HAS_TTL
    pub const HAS_VLOG: u64 = 0x04;   // == flags::HAS_VLOG, SST only
    pub const KNOWN:    u64 = 0x06;
}
```

`TOMBSTONE`/`SINGLE_DELETE` are **not** modifiers in the envelope — they are
kinds 2 and 3. Unknown modifier bits → `Corruption` (they are not
capability-gated).

**Enable protocol.** `enable_capability(bits)`: check
`db.poison.check()` and read-only first, then persist the bit under
`manifest_mu` via `persist_manifest`, then flip the in-memory `DbInner::caps`
(**new**) word that API entry points check. A public
`DB::enable_format_capabilities(bits) -> Result<()>` exposes it for
tests/operators.

A `persist_manifest` failure **fail-stops the whole DB** (`db.rs:315–326`:
`self.poison.set(format!("manifest persist failed: {e}"))`; the
`parts.rs:1069` comment records the same policy), so a failed enable is not a
recoverable no-op — the caller cannot simply retry against the same handle.
Documented as such; the enable path refuses on an already-poisoned DB *before*
taking `manifest_mu`, matching `Txn::commit`'s `self.db.poison.check()?` gate.

Recovery never consults options to decode a detached artifact — the envelope
and the footer are self-describing (the `freeze_part`/`attach_part`
standalone-table rule). This is why the WAL envelope carries a `schema` id
rather than a per-record "unified?" field.

## Wire format

All integers little-endian; `uvarint`/`varint` are LEB128 (`encoding.rs`).

### 1. Manifest `FORMAT_CAPS_TAG` tail

```
tag bytes : "ONDACAP1"   (8 bytes, b"\x4F\x4E\x44\x41\x43\x41\x50\x31")
payload   : caps u64 LE  (8 bytes)
total     : 16 bytes
```

Position in tag order: **after `INSTANCE_TAG`, before `WAL_LAYOUT_TAG`**.
Full emitted order is therefore

```
[positional: partition | tier | max_entry_time]
[ONDAOBJ1 ...] [ONDAINS1 nonce] [ONDACAP1 caps] [ONDAWAL1 layout]
[crc32c u32 LE]
```

`ManifestTailPresence` (`manifest.rs:190–216`) gains `caps: bool`, set by
`detect` from `manifest.caps != 0`, and `tagged()` becomes
`self.object || self.nonce || self.caps`. **This wiring is load-bearing**: the
positional decoder (`decode_positional_tails`, `:378`) is gated only on
non-emptiness, so a caps tag emitted without the three positional sections in
front of it is decoded as a *partition name section* — silent corruption, not
rejection. `Manifest` gains `caps: u64` (**new**), defaulting to 0.

Tag decoding becomes a dispatch loop replacing `decode_tagged_tails` +
`decode_wal_layout`:

```rust
// manifest.rs, replaces :391-419
let mut layout = WalLayout::PerColumnFamily;
let mut nonce = None;
let mut caps = 0u64;
while !p.is_empty() {
    if p.len() < TAG_LEN { return Err(corrupt_manifest()); }
    let (tag, rest) = p.split_at(TAG_LEN);
    p = match tag {
        t if t == OBJECT_TAG      => decode_name_section(rest, cfs, |s, n| s.object = Some(n))?,
        t if t == INSTANCE_TAG    => { nonce = Some(read_u64(take(rest, 8)?)); &rest[8..] }
        t if t == FORMAT_CAPS_TAG => { caps  = read_u64(take(rest, 8)?);       &rest[8..] }
        t if t == WAL_LAYOUT_TAG  => { layout = decode_layout_byte(rest)?;     &rest[1..] }
        _ => return Err(corrupt_manifest()),      // default arm
    };
}
```

The default arm reproduces today's behavior exactly (an unknown residual is
already `Corruption`); the loop is what makes 1.2's `ONDARNG1` and 2.2's
edit-log tags addable without another positional hazard. `decode_layout_byte`
rejects any byte other than `1`. Duplicate tags are `Corruption`.

Version gate: `decode_manifest_header` accepts `VERSION ∈ {1, 2}`; after the
loop, `caps != 0 && version == 1` → `Corruption`, and
`caps & !KNOWN_CAPS != 0` → `UnsupportedFormat`.

### 2. WAL envelope

Frame is unchanged: `[payload_len u32 LE][crc32c(payload) u32 LE][payload]`
(`wal.rs:8`). The discriminator is the first payload byte:

```
payload[0] == 0xFF  -> envelope (v2)
otherwise           -> legacy record stream (strict-masked by Change A)
```

`0xFF` is safe: a legacy first byte is a flags byte, and writer-produced flag
bytes are at most `0x17`; Change A rejects anything above `KNOWN_ENTRY_FLAGS`
anyway.

```
envelope := 0xFF | schema uvarint | count uvarint | record x count

schema 1 = per-CF WAL layout        (keys are user keys)
schema 2 = unified WAL layout       (keys carry the 8-byte big-endian cf-id
                                     prefix INSIDE the key, exactly as the
                                     legacy unified layout does today —
                                     unified.rs:311-315; there is NO separate
                                     cf-id field)

record := kind uvarint
        | modifiers uvarint
        | alen uvarint          <- legacy klen slot
        | blen uvarint          <- legacy vlen slot
        | seq uvarint
        | ttl varint            (present iff modifiers & HAS_TTL)
        | a bytes               <- legacy key slot
        | b bytes               <- legacy value slot
```

The **legacy field order is preserved deliberately** (`alen, blen, seq, ttl?,
a, b`) so `append_batch`'s exact frame-size precompute (`wal.rs:309–320`,
whose comment records that growth reallocation "dominated large-value
commits") survives with a one-line change:

```rust
let head = 1 + uvarint_len(schema) + uvarint_len(recs.len() as u64);
let body: usize = recs.iter().map(|r| {
    uvarint_len(kind_of(r)) + uvarint_len(mods_of(r))
      + uvarint_len(r.a.len() as u64) + uvarint_len(r.b.len() as u64)
      + uvarint_len(r.seq) + if r.ttl != 0 { 10 } else { 0 }
      + r.a.len() + r.b.len()
}).sum();
```

Slot meaning per kind: for kinds 1–4, `(a, b) = (key, value)` — a delete
(kind 2/3) has `blen = 0`. For kind 5 (1.2), `(a, b) = (start, end)` and there
is no value; this is why the slots are named generically. Ordinary
`put`/`delete` therefore cost `+1` byte versus legacy (the kind uvarint) and
`+1` for `modifiers`, replacing the 1-byte flags byte: net `+1` byte per
record. Unified records are unchanged in size — the prefixed key is kept, so
there is no LEB128-encoded FNV-1a id (which would have been ~10 bytes against
today's fixed 8).

Writer rule: the envelope is emitted iff `caps & CAP_EXTENDED_RECORDS != 0`;
once enabled, **all** frames from that DB are envelopes (no per-frame
decision). Replay accepts both forms forever.

Decode errors: unknown `schema` → `UnsupportedFormat`; `kind > 63` →
`Corruption`; unknown assigned kind (e.g. 5 in a binary without 1.2) →
`UnsupportedFormat`; `modifiers & !modifiers::KNOWN` → `Corruption`; `count`
disagreeing with the payload length → `Corruption`.

Replay signature (**new**), needed because kind 5 has two keys and no value:

```rust
// new, wal.rs — replaces `FnMut(Record) -> Result<()>`
pub enum ReplayRecord {
    Point(Record),                                        // kinds 1..4
    RangeDelete { start: Vec<u8>, end: Vec<u8>, seq: u64 }, // kind 5 (1.2)
}
```

`Wal::replay`'s callback takes `ReplayRecord`; both callers
(`column_family.rs:491`, `unified.rs:228`) match on it. 1.0 lands the enum with
only the `Point` arm reachable; 1.2 fills in the second.

### 3. SST extended entry layout (`FOOTER_EXTENDED_BLOCK = 0x10`)

```rust
// new, sst/mod.rs
pub(crate) const FOOTER_EXTENDED_BLOCK: u8 = 0x10;  // next free bit after VLOG_V2 0x08
pub(crate) const KNOWN_FOOTER_FLAGS: u8 =
    FOOTER_HAS_BLOOM | FOOTER_BTREE | FOOTER_RESTARTS | FOOTER_VLOG_V2
    | FOOTER_EXTENDED_BLOCK;                         // = 0x1F
```

The flag is **table-level**: when set, *every* data-block entry in the table
uses the extended layout. There is no per-block flag byte today (a block is
`[alg u8][comp_len u32 LE][raw_len u32 LE][crc32c u32 LE][payload]` plus an
optional restart trailer, `block.rs`), and adding one would be its own format
change; the earlier "a table mixes legacy and extended blocks freely" wording
is withdrawn.

```
legacy entry   : flags(1) | klen uv | vlen uv | seq uv | ttl var? | key
                 | (value | vlog_off u64 LE if HAS_VLOG)

extended entry : kind uv | modifiers uv | klen uv | vlen uv | seq uv
                 | ttl var? (iff modifiers & HAS_TTL) | key
                 | (value | vlog_off u64 LE if modifiers & HAS_VLOG)
```

Block framing, compression, restart trailer and index are untouched — entry
boundaries still come from `decode_entry`'s returned `next`, so
`FOOTER_RESTARTS` binary search, the B+tree index and the block CRC
(invariant 4) all keep working unchanged. `decode_entry` gains a layout
parameter resolved once at `Reader::open` from `footer[48]`.

**Extended footer prefix.** The 64-byte footer is full (`0..48` fields, `48`
flags, `49..56` unused, `56..64` magic) — 7 free bytes cannot hold a block
handle. When `FOOTER_EXTENDED_BLOCK` is set, the **16 bytes immediately
preceding the footer** are an aux-block handle:

```
[size-80 .. size-72)  aux_off u64 LE   (0 when absent)
[size-72 .. size-64)  aux_len u64 LE   (0 when absent)
[size-64 .. size)     the fixed 64-byte footer
```

The aux block is `block.rs`-framed like every other block (so it is
CRC-covered — invariant 4) and its payload is a tagged section list:

```
aux payload := section_count uvarint | section x count
section     := tag u8 | len uvarint | payload[len]
tag 1 = range-delete fragments (defined by 1.2)
tag 2..  reserved
```

Unknown section tag → `UnsupportedFormat`. 1.0 defines the container and
writes `aux_off = aux_len = 0`; 1.2 is the first producer.

### 4. `OndaError::UnsupportedFormat` (code `-16`)

Five hand-maintained sites in `error.rs` plus a test — all must be updated
together, because `from_code` is load-bearing for WAL group-commit followers
(`wal.rs::code_to_result`, `:545`), which reconstruct an error from an integer
across threads:

| Site | Line (0.8.2) | Addition |
| --- | --- | --- |
| enum variant | `error.rs:14–46` | `UnsupportedFormat(String)` |
| `code()` | `:53–70` | `UnsupportedFormat(_) => -16` |
| `from_code()` | `:76–93` | `-16 => UnsupportedFormat(String::new())` |
| `kind()` | `:97–113` | `=> "unsupported_format"` |
| `Display` | `:118–139` | `write!(f, "unsupported format: {m}")` |
| `codes()` unit test | `:166` | assert round-trip of `-16` |

`OndaError` is `#[non_exhaustive]` (`error.rs:13`), so adding the variant is
source-compatible for downstream matches.

## Golden fixtures

Location: `tests/fixtures/phase1/` (**new** directory, committed to git).

Generation: one ignored test acts as the generator —

```rust
// tests/fixtures_phase1.rs
#[test] #[ignore = "regenerates committed fixtures; run manually"]
fn regenerate_phase1_fixtures() { /* writes tests/fixtures/phase1/*.bin */ }
```

It is run **once**, on 0.8.2 semantics, before any strictness lands; the output
is committed and never regenerated except by an explicit, reviewed format
change. Every live test only *reads* the files. Each fixture is pinned three
ways: the exact bytes (committed), a decoded-value assertion, and
`encode(decode(bytes)) == bytes` where the structure is re-encodable.

| Fixture | Content |
| --- | --- |
| `wal_legacy_all_flags.bin` | one frame per writer-produced flag combination |
| `wal_legacy_empty_frame.bin` | `append_batch(&[])` then a normal frame — pins "skipped, replay continues" |
| `wal_legacy_torn_tail.bin` | valid frame + truncated payload → `Ok(last_seq)` |
| `wal_legacy_crc_valid_undecodable.bin` | CRC-valid frame whose record body is malformed → `Err(Corruption)` after 1.0A |
| `klog_legacy_{flat,btree}_{restarts,norestarts}_{bloom,nobloom}.klog` (+ paired `.vlog` where values are separated) | all entry-flag combos, both index shapes |
| `manifest_v1_notail.bin` | no tail at all (pre-0.3.0 byte layout) |
| `manifest_v1_partition.bin`, `_tier.bin`, `_time.bin`, `_unified.bin`, `_object.bin`, `_nonce.bin` | every tail combination in emission order |
| `manifest_v2_caps_only.bin` | `caps != 0`, **no** partition/tier/time data — the exact case the positional decoder gets wrong if `tagged()` is not extended |
| `wal_v2_envelope_schema1.bin`, `_schema2.bin` | 1.0B; kinds 1–3 only |
| `klog_extended.klog` | 1.0B; `FOOTER_EXTENDED_BLOCK` set, aux handle `0/0` |

**Frozen decoder.** `tests/frozen_decoder.rs` (**new**) vendors a *copy* of the
0.8.2 `Manifest::decode` VERSION-1 path (not a re-export, not a changed
constant) and asserts it rejects `manifest_v2_caps_only.bin`. This is the
old-binary-refusal proof.

## Crash matrix (persist-before-use)

| Point | State | Recovery | Test |
| --- | --- | --- | --- |
| crash before capability persist | no new-format bytes exist | reopen: API refused until enabled again | `caps_crash_before_persist` |
| crash after persist, before in-memory flip | bit durable, no artifacts | reopen sees the bit; enabling is idempotent | `caps_crash_after_persist` |
| race: first enable vs concurrent writers | writers of the new kind must see the bit | entry points check `caps` after the durable write; race N first-calls | `caps_race_first_enable` |
| `persist_manifest` fails during enable | DB poisoned, `caps` untouched | handle is fail-stopped; a reopen sees the pre-enable state | `caps_persist_failure_poisons` |

## Slices

**1.0A**

1. `UnsupportedFormat` + code `-16` wiring (no format change).
2. Frozen legacy corpus + generator (gates everything after it).
3. Encode-site normalization (`flag_bits`, `encode_record_body`,
   `encode_entry`).
4. Strict masks: entry flags, invalid combinations, footer bits.
5. `decode_record` → `Result` + `replay_file` torn/undecodable split +
   `ReplayRecord` enum.
6. Manifest tag dispatch loop (behavior-identical; extensibility only).
7. Fuzz corpus per decoder (`decode_record`, `decode_entry`, manifest tails,
   footer).

**1.0B**

8. `Manifest::caps` + `FORMAT_CAPS_TAG` + `ManifestTailPresence::caps` +
   VERSION 2 acceptance + `manifest_v2_caps_only` golden + frozen-decoder
   refusal.
9. Capability + kind registries in `format.rs`; `DbInner::caps`.
10. WAL envelope encode/decode (both schemas) + golden bytes.
11. SST `FOOTER_EXTENDED_BLOCK` + extended entry layout + extended footer
    prefix + empty aux block + golden bytes.
12. `enable_capability` + `DB::enable_format_capabilities` + crash matrix.

## Implementation tasks

Each task is one commit after the 4-command gate
(`cargo test` / `cargo test --features unsafe-fastpath` /
`cargo clippy --all-targets` / `cargo clippy --all-targets --features
unsafe-fastpath`). Tests are written **before** the implementation.

1. **`UnsupportedFormat` error variant.**
   Test first, in `src/error.rs` `#[cfg(test)]`: extend `codes()` with
   `assert_eq!(OndaError::UnsupportedFormat("m".into()).code(), -16)`,
   `assert_eq!(OndaError::from_code(-16).kind(), "unsupported_format")`,
   and `assert!(format!("{}", OndaError::UnsupportedFormat("m".into()))
   .starts_with("unsupported format"))`.
   Then add the variant and the five match arms (table above).

2. **Fixture generator + legacy corpus.**
   New `tests/fixtures_phase1.rs` with `#[ignore]`
   `regenerate_phase1_fixtures()` writing every `wal_legacy_*`,
   `klog_legacy_*` and `manifest_v1_*` file listed above via the *current*
   encoders. Run it once; commit `tests/fixtures/phase1/`.
   Then a live test `legacy_corpus_decodes_unchanged()` asserting, per fixture,
   the decoded value table and (where re-encodable) byte round-trip. This test
   must stay green through every later task — it is the "strictness changes no
   legacy outcome" gate.

3. **Encode-site normalization.**
   Tests first: `flag_bits_normalizes_single_delete_to_tombstone()`
   (`memtable.rs`), `wal_encode_normalizes_single_delete()` (`wal.rs`),
   `sst_encode_never_sets_vlog_on_tombstone()` (`sst/mod.rs`) — each asserts
   the produced flags byte, and a `#[should_panic]` debug-assert twin.
   Then implement in the three functions.

4. **Strict entry-flag masks.**
   Tests first in `tests/sst.rs` + `wal.rs` unit tests:
   `decode_record_rejects_unknown_flag_bit()`,
   `decode_record_rejects_single_delete_without_tombstone()`,
   `decode_entry_rejects_tombstone_with_vlog()`,
   `decode_entry_rejects_unknown_flag_bit()` — each asserts
   `err.kind() == "corruption"`.
   Then add `KNOWN_ENTRY_FLAGS`, delete `flags::DELTA_SEQ`, add the checks.
   Re-run task 2's corpus test.

5. **Strict footer mask.**
   Test first: `footer_unknown_flag_bit_is_unsupported_format()` in
   `tests/sst.rs` — take `klog_legacy_flat_restarts_bloom.klog`, flip bit
   `0x20` in `footer[48]`, assert `Reader::open` returns
   `kind() == "unsupported_format"`.
   Then add `KNOWN_FOOTER_FLAGS` and the check in `Reader::open`.

6. **`decode_record` → `Result`; replay split; `ReplayRecord`.**
   Tests first in `wal.rs` unit tests: `torn_payload_stops_replay_cleanly()`
   (fixture `wal_legacy_torn_tail.bin` → `Ok`, records before the tear
   delivered), `crc_valid_undecodable_record_is_corruption()` (fixture
   `wal_legacy_crc_valid_undecodable.bin` → `Err`, `kind() == "corruption"`),
   `empty_frame_is_skipped_and_replay_continues()` (fixture
   `wal_legacy_empty_frame.bin` → `Ok`, the following frame's record is
   delivered).
   Then change the signature, introduce `ReplayRecord` (only `Point`
   constructed), and update `column_family.rs:491` and `unified.rs:228`.

7. **Manifest tag dispatch loop.**
   Tests first in `manifest.rs` unit tests:
   `unknown_tag_is_corruption()` (append `b"ONDAXXX1"` + 8 bytes to
   `manifest_v1_object.bin`), `duplicate_tag_is_corruption()`,
   `short_residual_is_corruption()` (7 trailing bytes),
   `all_v1_tail_fixtures_decode_identically()` over every `manifest_v1_*`
   fixture.
   Then replace `decode_tagged_tails` + `decode_wal_layout` with the loop.

8. **Fuzz corpora.** A loop-driven runner per decoder seeded from
   `tests/fixtures/phase1/`, asserting no panic and no non-`Result` exit.
   *End of 1.0A.*

9. **`Manifest::caps` + `FORMAT_CAPS_TAG` + VERSION 2.**
   Tests first in `manifest.rs`:
   `caps_tail_round_trips()`;
   `caps_only_manifest_emits_all_positional_sections()` — build a manifest with
   `caps = CAP_EXTENDED_RECORDS` and **no** partition/tier/time data, encode,
   and assert the first three positional sections are present (all-empty
   counts) before `ONDACAP1`; then decode and assert `caps` and that no
   `SstMeta.partition` was populated;
   `unknown_caps_bit_is_unsupported_format()`;
   `caps_tag_under_version_1_is_corruption()`;
   `zero_caps_writes_version_1()`.
   Then add the `caps` field, the tag, the `ManifestTailPresence::caps` +
   `tagged()` wiring, the VERSION `{1,2}` gate, and regenerate *only*
   `manifest_v2_caps_only.bin`.

10. **Frozen-decoder refusal.** New `tests/frozen_decoder.rs` vendoring the
    0.8.2 VERSION-1 decode path; test
    `frozen_v1_decoder_refuses_v2_manifest()` asserting it errors on
    `manifest_v2_caps_only.bin`.

11. **Registries.** `format.rs`: capability consts, `KNOWN_CAPS`, kind consts,
    `MAX_ASSIGNABLE_KIND`, `modifiers`. Tests: `known_caps_is_0x7f()`,
    `capability_bits_are_pinned()` (assert each literal value — this is the
    wavesdb-compatibility pin), `modifier_bits_match_legacy_flags()`.

12. **WAL envelope.**
    Tests first in `wal.rs` + `tests/unified.rs`:
    `envelope_schema1_round_trips_all_point_kinds()`,
    `envelope_schema2_keeps_cf_prefix_in_key()` (assert the decoded key's first
    8 bytes are the BE cf-id and that replay feeds `mem.put` the prefixed key
    unchanged), `envelope_golden_bytes()` (byte-compare against
    `wal_v2_envelope_schema1.bin`), `legacy_and_envelope_frames_interleave()`
    (one file containing both forms replays fully),
    `envelope_unknown_kind_is_unsupported_format()`,
    `envelope_kind_above_63_is_corruption()`,
    `envelope_unknown_modifier_is_corruption()`,
    `envelope_frame_size_precompute_is_exact()` (assert
    `buf.len() == HEADER_SIZE + predicted`).
    Then implement encode/decode and the `append_batch` precompute change.

13. **SST extended layout.**
    Tests first in `tests/sst.rs`:
    `extended_table_round_trips()`, `extended_footer_prefix_is_16_bytes()`
    (assert `aux_off`/`aux_len` positions and zero values),
    `extended_table_restart_search_matches_scan()`,
    `extended_golden_bytes()` against `klog_extended.klog`,
    `legacy_table_still_decodes_with_extended_support_compiled()`.
    Then add `FOOTER_EXTENDED_BLOCK`, the layout parameter on `decode_entry`,
    the extended `encode_entry`, and the aux handle write/read.
    **Both feature configs matter here** — the mmap read path in
    `sst/reader.rs` is compiled only under `unsafe-fastpath`.

14. **`enable_capability` + public API + crash matrix.**
    Tests first in `tests/db.rs`: the four crash-matrix rows by name, plus
    `enable_on_readonly_is_readonly_error()`,
    `enable_on_poisoned_db_is_poisoned_error()`,
    `enable_is_idempotent()`.
    Then implement `DbInner::caps`, `enable_capability`,
    `DB::enable_format_capabilities`.

## Acceptance

```sh
cargo test && cargo test --features unsafe-fastpath   # both configs, per-binary ok
cargo test legacy_corpus_ decode_ caps_ envelope_ extended_ frozen_
```

- Every Change-A strictness check is proven against the frozen corpus with
  **zero** outcome changes for valid legacy bytes.
- Legacy-only databases keep writing VERSION-1 manifests; v2 appears only when
  a capability is enabled.
- Old-binary refusal proven by `tests/frozen_decoder.rs`, not by changing a
  constant.

## Rollback

Change A is permanent (strictness is not un-learnable). Change B with no
capabilities enabled writes v1 and is revertible; after any capability is
enabled, readers must stay v2-aware (documented — same stance as wavesdb).
