# 2.1 — Restart-based prefix-delta data blocks

**Readiness:** design ready — ondaDB starts **ahead** of wavesdb here: the
restart machinery this feature needs already exists (`FOOTER_RESTARTS`,
`RESTART_INTERVAL = 8`, the restart-offset trailer, `Reader::restart_scan_offset`).
Only the shared-prefix encoding is new. **Effort:** 4–6 dev-weeks — revised up
from 3–5 because the key cannot stay pinned (§Merge-iterator fallback) and
because four reader entry points, not one, need delta treatment (§Reader
algorithms). **wavesdb counterpart:** 2.1, with their open-decision answers
adopted.

## Goal

Cut repeated user-key bytes in raw/decompressed data blocks while keeping
bounded point-seek and bidirectional iteration. Honest hypothesis (wavesdb's
correction): block compression already recovers most redundancy *on disk*; the
expected wins are **decompressed cache residency** and **decode bandwidth** on
prefix-heavy keyspaces (spada's `tenant/cluster/segment` keys are the
documented shape).

### The hypothesis at 4 KiB blocks

RV-M6 is **settled** (fixed `ef496d1`): blocks are 4 KiB by default, per-CF,
persisted — `ColumnFamilyConfig::data_block_size` (`src/config.rs:460`,
default `column_family::DEFAULT_DATA_BLOCK_SIZE` = `sst::DEFAULT_BLOCK_SIZE`
= `4 << 10`), consumed by flush (`src/column_family.rs:936`), ingest (same
`writer_opts`) and compaction (`src/compaction.rs:975`).

The verification report argued the smaller block weakens the hypothesis
("~4× fewer entries per block, so proportionally more full-key restart
anchors"). **That reasoning is wrong and is corrected here.** Restart anchors
are placed by `cur_entries.is_multiple_of(restart_interval)` with `cur_entries`
reset per block (`src/sst/writer.rs:256-259,348-349`), so the anchor *fraction* is
`1/interval` regardless of block size — 12.5 % at the default 8. Block size
changes only the partial last run: at ~55 B/entry a 4 KiB block holds ~74
entries → 10 anchors (13.5 %); a 16 KiB block holds ~296 → 37 anchors
(12.5 %). One percentage point, not 4×.

What 4 KiB actually changes, and the direction it pushes:

- **Compression window.** zstd/lz4 see 4 KiB at a time instead of 16 KiB, so
  block compression recovers *less* cross-entry prefix redundancy on its own.
  This makes the delta encoding's on-disk contribution **larger** at 4 KiB, not
  smaller — the opposite of the report's conclusion.
- **Trailer overhead.** `4·R + 4` bytes per block: ~44 B of 4096 (1.1 %) at
  R = 10 vs ~152 B of 16384 (0.9 %). Delta encoding raises entry density, so
  R per block rises with it — a second-order cost the benchmark must show, not
  argue away.
- **Per-entry floor cost.** A delta entry carries one extra uvarint
  (`shared_len`) versus the extended layout. On keys with no shared prefix that
  is **+1 byte per entry, unconditionally** — ~1.8 % on a 55 B entry. This is
  the random-key regression the acceptance gate must bound.
- **Index block.** 4× more blocks means 4× more index separators. Independent
  of 2.1, but delta encoding shrinks blocks and therefore *adds* blocks per
  table at a fixed `data_block_size` — another reason the sweep must be crossed
  with `data_block_size` rather than run at one point.

Conclusion: the hypothesis survives at 4 KiB, with a different balance
(compression overlap down, per-entry floor cost relatively up). It is not
settled by argument — the sweep in §Benchmark decides, and the default stays
opt-in unless it is decisive.

## Dependencies on 1.0

Hard, in this order:

- `FOOTER_EXTENDED_BLOCK = 0x10` (**new**, 1.0 Change B) — **table-level** per
  the binding decision: every data block of a table uses the extended entry
  layout, no per-block mixing. Delta blocks are extended blocks.
- `[kind uvarint][modifiers uvarint]` replaces the legacy `flags u8`
  (**new**, 1.0 Change B). `HAS_TTL` / `HAS_VLOG` / `TOMBSTONE` /
  `SINGLE_DELETE` become modifier bits; `kind` carries put/delete/
  single_delete/merge/range_delete.
- `CAP_PREFIX_DELTA = 1 << 3` (**new**, 1.0 Change B) and the shared
  `enable_capability` helper.
- **1.0 Change A's strict footer-flag mask.** At 0.8.2 the reader ignores
  unknown footer bits (`src/sst/reader.rs:230-238` reads `footer[48]` and tests
  only the four bits it knows). A pre-1.0 binary handed a `FOOTER_PREFIX_DELTA`
  table would therefore not refuse it — it would run `decode_entry` over delta
  bytes and produce garbage keys, sometimes without hitting a bounds error.
  Strict masking must land, and be released, **before** any writer emits the
  bit. Binaries older than 1.0 cannot be fixed retroactively; that is why this
  format is capability-gated and opt-in, and it belongs in the release notes.

Named here so an implementer does not have to infer it: nothing in 2.1 changes
the index block, the B+tree index (`FOOTER_BTREE`), the bloom block, the block
framing (`src/block.rs`), or the vlog.

## Current entry layout (0.8.2, verified)

`src/sst/mod.rs:210-247` (`encode_entry`), decoded at `:250-302`
(`decode_entry`), documented at `src/sst/mod.rs:14-17`:

```text
flags     u8                 TOMBSTONE 0x01 | HAS_TTL 0x02 | HAS_VLOG 0x04
                             | DELTA_SEQ 0x08 (never written) | SINGLE_DELETE 0x10
key_len   uvarint            LEB128
val_len   uvarint            logical value length — ALSO set for vlog values
seq       uvarint
ttl       varint             present iff flags & HAS_TTL
key       key_len bytes      USER key; no 8-byte !seq trailer (unlike format.rs
                             internal keys)
then exactly one of:
  vlog_off u64 LE (8 bytes)  iff flags & HAS_VLOG
  value    val_len bytes     otherwise
```

Facts an implementer needs before touching this:

- The stored key is the **user key**. The sequence is its own uvarint field,
  not the `user_key || BE(!seq)` trailer used for memtable/internal keys
  (`src/format.rs:22-33`). Prefix sharing over "user-key bytes" is therefore
  well defined here, unlike a LevelDB-style block that shares over internal
  keys.
- With `HAS_VLOG`, `DecEntry.val_start` points at the 8 offset bytes while
  `val_len` is the logical value length (`src/sst/mod.rs:276-282`) — so
  `val_start`/`val_len` are *not* a valid slice in that case. Only
  `inline_value()` callers use them, guarded by `has_vlog()`.
- Block trailer, present iff footer flag `FOOTER_RESTARTS` (`src/sst/mod.rs:48`):
  `entries… | restart_off u32 LE × R | R u32 LE`. Appended to the raw block
  before framing (`src/sst/writer.rs:336-347`), split off by
  `Reader::split_block` (`src/sst/reader.rs:503-518`).
- The trailer rides **inside** the framed/compressed payload, so the existing
  block CRC covers it (`src/block.rs:38`) — AGENTS.md invariant 4 is satisfied
  with no new checksum.
- Block-size accounting today **excludes** the trailer: `src/sst/writer.rs:285`
  tests `self.cur_block.len() >= self.opts.block_size` before the trailer is
  appended.

## New delta entry layout

Selected by footer flags, table-level (§Table-level selection). Field order:

```text
kind        uvarint          1.0 record kind
modifiers   uvarint          1.0 modifier bits (HAS_TTL, HAS_VLOG, …)
shared_len  uvarint          bytes shared with the previous entry's user key
suffix_len  uvarint          bytes stored here; user key = prev[..shared] ++ suffix
val_len     uvarint          logical value length (unchanged semantics)
seq         uvarint
ttl         varint           present iff modifiers & HAS_TTL
suffix      suffix_len bytes
then exactly one of:
  vlog_off  u64 LE           iff modifiers & HAS_VLOG
  value     val_len bytes    otherwise
```

**Why this order.** It is the extended layout with `key_len` split into
`shared_len | suffix_len` — nothing else moves:

1. `kind`/`modifiers` stay first, so `HAS_TTL` presence is resolved before the
   decoder reaches the `ttl` slot. Putting the key suffix first (the shape the
   previous revision of this doc proposed) would force the decoder to read the
   variable-length key before it knows the field list, which is decodable but
   removes the property below.
2. Every fixed-width/varint field precedes every variable-length field, so a
   decoder can bounds-check `shared_len ≤ prev_key.len()`, `suffix_len`, and
   `val_len` against the remaining block bytes **before** any memcpy. This is
   what makes the fuzz target cheap to satisfy.
3. `restart_off` values in the trailer keep pointing at entry starts, so the
   existing trailer shape, `split_block`, and the binary search over it are
   untouched.

**Restart entries** (entry 0 of each block and every `restart_interval`-th
after) have `shared_len = 0` and `suffix_len = full key length`: they are
self-contained and decodable without any predecessor. The anchor placement
logic in `src/sst/writer.rs:256-259` needs no change to guarantee this.

`prev` is reset to empty at the start of every block *and* at every restart
anchor. Sharing never crosses a block boundary or an anchor.

**Prefix sharing is comparator-agnostic**: it is a representation choice; order
still comes from `Comparator`, and nothing here uses the 8-byte key-prefix
compare trick for ordering. AGENTS.md invariant 7 is untouched — but the test
in §Tests exercises a non-bytewise comparator anyway.

## Table-level selection (answers "where does the tag live")

Two footer flag bits, both **new**, in the existing `footer[48]` flag byte
(`src/sst/writer.rs:508`, `src/sst/reader.rs:230`), which has 0x01/0x02/0x04/
0x08 taken:

```text
FOOTER_EXTENDED_BLOCK = 0x10   (1.0) extended [kind][modifiers] entry layout
FOOTER_PREFIX_DELTA   = 0x20   (2.1) entries are prefix-delta encoded
```

Invariants, all checked at `Reader::open`, each a `Corruption`:

- `FOOTER_PREFIX_DELTA` requires `FOOTER_EXTENDED_BLOCK` (the delta layout is
  defined only over the extended entry).
- `FOOTER_PREFIX_DELTA` requires `FOOTER_RESTARTS`. Without a restart trailer a
  delta block is decodable only from offset 0 — no seek, no reverse. The writer
  refuses the combination as well (`WriterOptions::restart_interval == 0` with
  delta enabled is `InvalidArgs`).

The previous revision proposed "one `block_encoding` tag per cached block".
That is not implementable as written: `BlockCache` is keyed `(file_id, off)`
and stores bytes only (`src/cache/block.rs:22-32`) — there is no per-block
metadata slot, and a cache hit returns bytes with no envelope to re-parse.
Footer flags are how `has_restarts` and `vlog_v2` are already handled
(`src/sst/reader.rs:231-232`), they are read once at open, and they make a
detached/frozen/mounted table self-describing without the DB manifest — the
`freeze_part`/`attach_part`/`attach_part_by_ref` rule. Table-level also matches
the binding decision that `FOOTER_EXTENDED_BLOCK` is table-level.

## Configuration, capability, and wiring

```rust
// ColumnFamilyConfig — both fields new
pub enable_prefix_delta_keys: bool,   // default false: legacy full-key blocks
pub block_restart_interval: usize,    // default RESTART_INTERVAL (8)
```

`validate`: `block_restart_interval` in **`[1, 1024]`** — not `[0, 1024]`.
`0` is already taken in `WriterOptions`, where it means *emit no restart
trailer at all* (`src/sst/writer.rs:256`, `:336`, `:478-479`). Mapping config-0
to 8 would make the config value mean the opposite of the writer value it
feeds, and would delete the only way to express legacy no-trailer output — which
the golden corpus needs. Legacy no-trailer output stays reachable by
constructing `WriterOptions` directly (test-only), exactly as today; the sibling
option `data_block_size` likewise validates non-zero (`src/config.rs:946-947`).
`enable_prefix_delta_keys = false` with any interval is accepted and the
interval still applies (it is the restart interval, delta or not).

Config→writer mapping, stated once: `WriterOptions::restart_interval =
cfg.block_restart_interval` and `WriterOptions::prefix_delta` (**new**) =
`cfg.enable_prefix_delta_keys`.

**Both engine write paths hard-wire the interval today** and must be threaded:

- `ColumnFamily::writer_opts` (`src/column_family.rs:939`) —
  `restart_interval: crate::sst::RESTART_INTERVAL`. Serves flush *and* bulk
  ingest (`new_sst_writer`, `src/column_family.rs:855`).
- `cf_writer_opts` (`src/compaction.rs:984`) — same hard-wire.

Per the global option constraint, the two config fields land in one change with
`Default` + `validate` + config-blob tail + `CONFIG_PREFIX_DELTA_MAGIC`
(**new**, `b"ONDAPFX1"`) + reopen test. The blob tails are positional and
`read_block_size_tail` (`src/config.rs:1461`) currently returns `()`, so it must
be changed to return the unconsumed remainder before a new tail can follow it.
Emit the tail only when non-default, mirroring `encode_block_size`
(`src/config.rs:1170-1178`).

Enabling persists `CAP_PREFIX_DELTA` before any writer emits a delta block.
Legacy and delta tables coexist in any level, part, checkpoint, or attach.

## Reader algorithms

Four entry points need delta treatment, not one. The previous revision named
only `prev`, and described `restart_scan_offset` as covering "point seek" — it
covers `Reader::get*` only (its single caller is `src/sst/reader.rs:650` inside
`get_unfiltered`). `SstIterator::seek` does not use restarts at all: it calls
`load_block(bi, /*full=*/true)`, which decodes **every** entry offset into
`self.offsets`, then binary-searches those offsets
(`src/sst/iter.rs:171-205`). An offset alone cannot reconstruct a delta key, so
the offsets vector is insufficient wherever it is used.

**Shared primitive** (**new**, free function in `src/sst/mod.rs` so both the
reader and the iterator use one implementation):

```text
restart_lower_bound(raw, restarts, cmp, key, seq) -> anchor_offset
```
Binary search over the restart array, decoding the anchor entry at each probe.
Anchors are self-contained (`shared_len = 0`), so this works with no
materialization at all — it is today's `Reader::restart_scan_offset` body
(`src/sst/reader.rs:571-602`) with the anchor decode routed through the delta
decoder. `restart_scan_offset` becomes a thin caller.

**Run materialization** (**new**, `RunCursor` in `src/sst/iter.rs`): decode one
restart run forward into a reusable arena — `keys: Vec<u8>` plus
`Vec<(entry_off, key_off, key_len)>` — bounded by
`max(64 KiB, 2 × WriterOptions::block_size)`. A run whose reconstructed keys
exceed the bound is not an error: fall back to re-decode-from-anchor
(O(interval) per positioning step) rather than growing the arena. Buffers are
cleared and reused across seeks and across blocks, never reallocated per entry.

Per entry point:

- **`Reader::get_unfiltered` (point read)** — `restart_lower_bound`, then
  `scan_point_entry` forward from the anchor with a running `prev_key: Vec<u8>`
  scratch. No arena; the scan is already bounded by one restart interval plus
  the walk to the target. `scan_point_entry` returns owned values today
  (`src/sst/reader.rs:605-635`), so buffering the key costs it nothing new.
- **`SstIterator::seek` / `seek_for_prev`** — replace
  `load_block(bi, true)` + binary search over `self.offsets` with
  `restart_lower_bound` + materialize the containing run + linear scan within
  it. This is strictly *less* work than today's full-block decode.
- **`SstIterator::seek_to_last`** — last anchor → materialize the final run →
  position on its last entry. Today it uses `load_block(last, true)` and takes
  `offsets.len() - 1` (`src/sst/iter.rs:156-168`).
- **`SstIterator::prev`** — today `self.pos -= 1; decode_at(self.offsets[pos])`
  is random access into a pre-built offsets vector, with
  `load_block(block_idx - 1, true)` on block underflow (`:251-272`). (The
  previous revision called this an "offset walk"; the concern was right, the
  mechanism description was not.) For delta blocks: step back inside the
  materialized run; at run start, materialize the previous run; at block start,
  load the previous block and materialize its **last** run.
- **`SstIterator::next` and `load_block(.., full=false)`** — the cheap path
  stays cheap: forward stepping needs only the running previous-key buffer, no
  arena. `load_block(.., full=true)` is not called for delta blocks at all; the
  `full` parameter becomes "build the offsets vector (legacy blocks only)".

`key_prefix8`/`cur_pfx` keep working — `decode_at` recomputes `cur_pfx` from
the entry's user key (`src/sst/iter.rs:116-122`), which for delta blocks is the
reconstructed key. Merge-hot comparisons keep their fast path.

`value_block_ref` is **unaffected**: inline value bytes are still contiguous in
the block (`src/sst/iter.rs:308-317`), so values stay pinned. Only the key
changes.

## Merge-iterator fallback: `key_block_ref` returns `None`

The previous revision claimed the `key_block_ref` contract was unchanged. It
cannot be. `SstIterator::key_block_ref` returns a contiguous slice inside the
block bytes (`src/sst/iter.rs:320-325`); `Iterator::capture_group_key` pins that
`Block` into `pinned_key[idx]` and serves the key as `CurKey::Pinned { child,
start, len }` (`src/iterator.rs:550-563`). **A prefix-delta key does not exist
contiguously anywhere in the block.** Serving a borrow into it would violate
AGENTS.md invariant 8.

Design:

- `SstIterator` gains a `key_buf: Vec<u8>` (**new**) holding the reconstructed
  current key; `user_key()` returns `&self.key_buf` for delta tables and the
  block slice for legacy tables.
- `key_block_ref` returns `None` for delta tables. `capture_group_key` already
  has that branch: it copies `child.user_key()` into `Iterator::key` and sets
  `CurKey::Buffered` (`src/iterator.rs:564-568`) — the path memtable children
  take today. No new variant, no new lifetime.
- The copy is **necessary, not merely convenient**: the merge iterator advances
  the child past the group key while still serving it, so a borrow of the
  child's own buffer would dangle. A `CurKey::ChildBuf` variant is not a valid
  optimization — do not attempt it.

**Cost and measurement.** This adds one key memcpy per merged entry for delta
children, on top of the per-entry reconstruction memcpy inside `SstIterator`.
`docs/performance.md` records that per-entry `Arc` clones in this exact path
cost a measured **3× scan regression**, so this is precisely the class of change
that needs its own before/after evidence rather than an argument. Two things
bound the risk: the copy is of key bytes only (values stay pinned), and
memtable children already pay it, so a memtable-only scan is an existing
in-repo baseline. Acceptance requires a published legacy-SST (pinned) vs
delta-SST (buffered) scan-throughput comparison on identical data — see
§Acceptance.

## Writer changes

- `Writer` gains `prev_key: Vec<u8>` (**new**), cleared at every block start and
  every restart anchor.
- `Writer::add` (`src/sst/writer.rs:218-288`): after the existing anchor test,
  compute `shared_len` against `prev_key` (0 at an anchor), then call
  `encode_entry_delta` (**new**, `src/sst/mod.rs`). `encode_entry` stays for
  legacy output.
- Block-size accounting must include the eventual trailer: the cut test at
  `src/sst/writer.rs:285` becomes
  `self.cur_block.len() + 4 * self.cur_restarts.len() + 4 >= self.opts.block_size`.
  This shifts block boundaries and therefore invalidates any pre-existing golden
  block bytes — freeze the legacy corpus **before** this change (P2-0).
- `finish` sets `FOOTER_EXTENDED_BLOCK | FOOTER_PREFIX_DELTA` when delta output
  is enabled (`src/sst/writer.rs:478-479`).

## Decoder validation rules (each gets a corruption test)

1. `shared_len ≤ prev_key.len()` — checked before the reconstruction memcpy.
2. `shared_len == 0` at every offset listed in the restart array.
3. Restart count non-zero for a non-empty block; offsets strictly increasing
   and `< entries_len`; offset 0 present as the first anchor.
4. `suffix_len` and `val_len` fit the remaining entries-region bytes.
5. Entry decode consumes the entries region exactly — no bytes between the last
   entry and the trailer.
6. `FOOTER_PREFIX_DELTA` without `FOOTER_EXTENDED_BLOCK` or without
   `FOOTER_RESTARTS` → `Corruption` at open.
7. Reconstructed keys are non-decreasing under the table's comparator within a
   block (cheap: compare against `prev_key` during forward decode).

## Tests

- **Golden corpus.** Legacy twins (frozen at P2-0, before the accounting
  change) and delta twins, byte-pinned: footer flags, restart trailer,
  `shared_len`/`suffix_len` fields, entry count.
- **Round-trip equivalence.** Identical logical results — point get, forward
  scan, reverse scan, `seek`, `seek_for_prev`, `seek_to_last` — from a delta
  table and its legacy twin, at several snapshots. Key shapes: prefix-heavy
  (`tenant/cluster/segment`), random, and adversarial (long shared prefixes,
  `0xFF`-heavy, keys shorter than 8 bytes so `key_prefix8` padding is
  exercised, keys differing only in the last byte).
- **Reverse stress** across run and block boundaries; assert the arena bound is
  respected and the re-decode-from-anchor fallback is taken (counter or test
  hook).
- **Comparator-agnosticism.** The same round-trip under a reverse/custom
  comparator.
- **Mixed levels.** Legacy and delta tables in one level; compaction reading
  legacy inputs and writing delta output, and the reverse.
- **Standalone self-description.** `freeze_part` / `attach_part` /
  `attach_part_by_ref` round-trip a delta table with no manifest consulted.
- **Fuzz** the delta decoder before any writer becomes default-capable.
- **Config.** Blob round-trip, default emits no tail, tail coexists with the
  `ONDABLK1` tail, `[1,1024]` bounds rejected outside.

**Test matrix is four configurations, not two.** `Block::Mapped` is gated on
`mmap-reads` (`src/sst/mod.rs:129`); `unsafe-fastpath` is the alias for
`["mmap-reads", "arena-memtable"]` (`Cargo.toml:32`). Delta reconstruction reads
`Block::bytes()` identically either way, but the claim must be tested: run
default, `--features mmap-reads`, `--features arena-memtable`, and
`--features unsafe-fastpath` at least once per slice. AGENTS.md's 4-command
two-config gate remains the per-commit CI equivalent.

## Benchmark

Sweep `block_restart_interval` ∈ {4, 8, 16, 32} **crossed with**
`data_block_size` ∈ {4, 8, 16, 64} KiB — `data_block_size` is a first-class
persisted option now, and §Goal shows the two interact through compression
window and trailer size. Report per cell:

- decompressed cache bytes per table and block-cache hit rate
- decode CPU per scanned entry (forward and reverse separately)
- on-disk klog bytes (compressed) and index-block bytes
- scan throughput: legacy-SST pinned key vs delta-SST buffered key
- **reverse-scan p99**, published unconditionally

Per AGENTS.md: ≥5 runs, same-run ratios between configurations, never absolute
numbers across sessions.

## Implementation tasks

Ordered; each is one TDD step — write the named test first, watch it fail, then
implement. Gate after **every** task: `cargo test`, `cargo test --features
unsafe-fastpath`, `cargo clippy --all-targets`, `cargo clippy --all-targets
--features unsafe-fastpath`. Check each test binary for the presence of
`test result: ok`; never pipe through `tail`.

1. **Freeze the legacy golden corpus.** New `tests/golden_blocks.rs`. Build a
   fixed table with `WriterOptions { restart_interval: 8, block_size: 4096, .. }`
   and a second with `restart_interval: 0`, commit both byte-for-byte as
   fixtures. Tests: `legacy_restart_block_bytes_are_frozen`,
   `legacy_no_trailer_block_bytes_are_frozen` — assert the exact klog bytes and
   that `Reader::open` + full scan reproduce the expected entries. Nothing else
   in this task. This must land before task 6 changes block boundaries.
2. **Footer flag constants and open-time invariants.** `src/sst/mod.rs`: add
   `FOOTER_PREFIX_DELTA = 0x20` (`FOOTER_EXTENDED_BLOCK = 0x10` comes from 1.0;
   if 1.0 has not landed, define it here and hand ownership over on merge).
   `src/sst/reader.rs`: parse both into `r.extended` / `r.prefix_delta`; reject
   the two illegal combinations. Tests in `src/sst/reader.rs` `#[cfg(test)]`:
   `prefix_delta_without_extended_is_corruption`,
   `prefix_delta_without_restarts_is_corruption`.
3. **Delta entry codec.** `src/sst/mod.rs`: `encode_entry_delta(dst, prev_key,
   user_key, …) -> shared_len` and `decode_entry_delta(raw, off, prev_key,
   out_key) -> Result<(DecEntry, usize)>` (**new**). Tests in-module:
   `delta_entry_round_trips_with_shared_prefix`,
   `delta_entry_round_trips_with_zero_shared`,
   `delta_entry_rejects_shared_longer_than_prev`,
   `delta_entry_rejects_truncated_suffix`,
   `delta_entry_rejects_truncated_value`,
   `delta_vlog_entry_carries_eight_offset_bytes`.
4. **Fuzz target for the delta decoder.** Corpus-driven loop test
   `delta_decoder_never_panics_on_arbitrary_bytes` (in-module, seeded PRNG plus
   the golden bytes with single-byte mutations). Must be green before task 6.
5. **`restart_lower_bound` extraction.** Move the body of
   `Reader::restart_scan_offset` (`src/sst/reader.rs:571-602`) into a free
   function in `src/sst/mod.rs` taking an entry-decode closure; `restart_scan_offset`
   calls it. Pure refactor — test `restart_lower_bound_matches_legacy_scan_offset`
   asserts identical offsets over the task-1 fixture.
6. **Writer: delta output.** `WriterOptions::prefix_delta` (**new**),
   `Writer::prev_key`, `Writer::add` delta path, trailer-inclusive block-size
   accounting, footer flags in `finish`. Refuse `prefix_delta && restart_interval
   == 0` with `InvalidArgs`. Tests in `src/sst/writer.rs` `#[cfg(test)]`:
   `delta_writer_emits_zero_shared_at_every_restart`,
   `delta_writer_refuses_zero_restart_interval`,
   `block_cut_accounts_for_the_restart_trailer`, and in
   `tests/golden_blocks.rs`: `delta_block_bytes_are_frozen`.
7. **Reader point path.** `scan_point_entry` + `get_unfiltered` over delta
   blocks with a `prev_key` scratch. Tests in `tests/prefix_delta.rs` (**new**):
   `delta_point_get_matches_legacy_twin`,
   `delta_point_get_finds_key_at_every_run_position`,
   `delta_point_get_misses_between_keys`.
8. **`RunCursor` + iterator `seek`/`seek_to_last`.** `src/sst/iter.rs`:
   `key_buf`, `RunCursor` with the `max(64 KiB, 2 × block_size)` arena bound and
   the re-decode fallback; rewire `seek`, `seek_for_prev`, `seek_to_last`.
   Tests: `delta_seek_matches_legacy_twin_at_every_key`,
   `delta_seek_past_block_end_advances_to_next_block`,
   `delta_seek_to_last_matches_legacy_twin`,
   `delta_run_arena_bound_is_respected` (asserts the fallback counter fires on
   1 KiB keys sharing 1023 bytes).
9. **Iterator `next`/`prev`.** Forward stepping on the running buffer; `prev`
   across run and block boundaries. Tests:
   `delta_forward_scan_matches_legacy_twin`,
   `delta_reverse_scan_matches_legacy_twin`,
   `delta_reverse_scan_crosses_run_and_block_boundaries`,
   `delta_alternating_next_prev_is_stable`.
10. **Merge-iterator fallback.** `key_block_ref` → `None` for delta tables;
    confirm `capture_group_key` takes `CurKey::Buffered`; confirm
    `value_block_ref` still pins. Tests in `tests/prefix_delta.rs`:
    `delta_child_serves_buffered_key`, `delta_child_still_pins_inline_value`,
    `delta_and_legacy_children_merge_identically`,
    `delta_scan_survives_child_advance_past_group_key`.
11. **Corruption matrix.** One test per §Decoder validation rule, in
    `tests/prefix_delta.rs`, each mutating a golden delta block:
    `delta_rejects_shared_longer_than_prev_key`,
    `delta_rejects_nonzero_shared_at_restart`,
    `delta_rejects_unsorted_restart_offsets`,
    `delta_rejects_zero_restart_count_on_nonempty_block`,
    `delta_rejects_trailing_bytes_before_trailer`,
    `delta_rejects_out_of_order_reconstructed_keys`.
12. **Config fields + blob tail.** `enable_prefix_delta_keys`,
    `block_restart_interval`, `Default`, `validate`, `CONFIG_PREFIX_DELTA_MAGIC`
    tail (after the `ONDABLK1` tail — change `read_block_size_tail` to return
    the remainder first). Tests in `src/config.rs`:
    `a_default_config_emits_no_prefix_delta_tail`,
    `prefix_delta_settings_round_trip`,
    `the_prefix_delta_tail_coexists_with_preceding_tails`,
    `a_zero_restart_interval_is_rejected`,
    `a_restart_interval_above_1024_is_rejected`.
13. **Thread the options.** `ColumnFamily::writer_opts`
    (`src/column_family.rs:927-941`) and `cf_writer_opts`
    (`src/compaction.rs:963-986`) take the two fields from `cf.opts`. Tests in
    `tests/prefix_delta.rs`: `flush_honours_the_prefix_delta_option`,
    `ingest_honours_the_prefix_delta_option`,
    `compaction_output_honours_the_prefix_delta_option`,
    `option_survives_reopen`.
14. **Capability gate.** Persist `CAP_PREFIX_DELTA` before the first delta
    write (1.0's `enable_capability`). Tests:
    `delta_writer_requires_the_capability`,
    `capability_persists_before_the_first_delta_table`.
15. **Mixed and standalone.** Tests in `tests/prefix_delta.rs`:
    `mixed_legacy_and_delta_tables_in_one_level_scan_identically`,
    `compaction_reads_legacy_writes_delta`,
    `compaction_reads_delta_writes_legacy`,
    `frozen_delta_part_reopens_standalone`,
    `attached_delta_part_reads_without_the_source_manifest`.
16. **Four-config sweep.** Run tasks 7–15's tests under all four feature
    configurations; record the command lines in the slice note.
17. **Benchmark + default decision.** Run §Benchmark, ≥5 runs per cell, publish
    the table including reverse p99 and the buffered-key scan comparison. Write
    the decision note. Default stays `false` unless the sweep is decisive.

## Slices

1. Footer flags + delta codec + fuzz (tasks 1–4).
2. Shared `restart_lower_bound` + writer (tasks 5–6).
3. Reader point path + iterator seek/reverse + merge fallback (tasks 7–10).
4. Corruption matrix (task 11).
5. Config + wiring + capability + mixed/standalone (tasks 12–15).
6. Four-config sweep + interval × block-size benchmark + default note
   (tasks 16–17).

## Acceptance

Prefix-heavy phase: decompressed cache bytes per table and decode CPU per
scanned entry drop beyond baseline spread at the 4 KiB default.
**Reverse-scan p99 published** (the known regression risk — stays opt-in if it
fails the gate). **Buffered-key scan cost published**: legacy-SST vs delta-SST
merge-scan throughput on identical data; a regression beyond baseline spread
keeps the option off by default and is reported honestly rather than absorbed.
Random-key phase: no meaningful regression beyond the +1 byte/entry floor.

## Rollback

Option off stops new delta blocks; written tables remain readable (and must
be). Downgrading below the 1.0 capability floor while delta tables exist is not
supported — pre-1.0 binaries do not refuse the footer bit (§Dependencies).
