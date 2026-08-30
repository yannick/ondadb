# 2.2 — Numbered manifest version edits

**Readiness:** architectural; **local protocol only** (assessment §5 — the
wavesdb object-store/replica/promotion slices do not apply to ondaDB; the
manifest's commit point stays the local fsync+rename). **Effort:** 5–8
dev-weeks — revised up from 4–7: the call-site list is 20, not ≈ 15, and every
one of them inverts today's publish/persist order. **wavesdb counterpart:**
2.2 — the protocol, recovery rules, and crash matrix transfer; ondaDB's smaller
model reduces the codec work, not the migration work.

## Goal

Replace O(catalog) full `MANIFEST` rewrites on every structural change with one
fsynced, CRC-framed catalog edit, keeping periodic atomic snapshots. RV-M5 is
the one code-review item explicitly deferred as "a format and recovery feature,
not a contained corrective patch" (`docs/code-review-2026-08-resolution.md`,
M5 row); its evidence is ondaDB's own sizing probe —
**12.4 MiB re-encoded and fsynced per persist at 100k parts**, paid by every
flush (`docs/code-review-2026-08.md:402-419`, the ignored
`manifest_encoded_size_at_scale` probe).

## Baseline (verified against 0.8.2 / `3afc3c1`)

`DbInner::persist_manifest` (`src/db.rs:292-328`) takes `manifest_mu`, rebuilds
the **whole** `Manifest` from live CFs, and `Manifest::save`s it. It returns
`Ok(())` immediately in read-only mode (`:293-295`) and poisons the DB on write
failure (`:320-325`).

Model — complete, nothing omitted:

```text
Manifest    { next_file_id u64, global_seq u64, cfs, wal_layout, instance_nonce }
                                                    src/manifest.rs:89-101
CfManifest  { name String, config Vec<u8> (opaque blob), sstables }
                                                    src/manifest.rs:81-85
SstMeta     { id, level, num_entries, num_tombstones, max_seq, klog_size,
              vlog_size, min_key, max_key, partition, tier, max_entry_time,
              object }                              src/manifest.rs:38-77
```

Three facts the inventory guard (slice 1) depends on:

- **Partition rules and tier definitions are not manifest fields.** They live
  inside the opaque `CfManifest.config` blob (`src/db.rs:311`
  `config: cf.effective_config().encode()`; blob layout in
  `docs/formats.md` § Config blob). `SetCFConfig` (op 6) therefore covers
  `add_partition_rule` / `remove_partition_rule` / `TierDef` changes. Without
  this stated, the guard reports them as uncovered.
- **CFs are name-keyed; there is no manifest CF id.** `CfManifest.name` is the
  only identity and `DbInner::cfs` is a name-keyed map. `unified::cf_id`
  (FNV-1a) is a WAL/unified-store routing hash — never a catalog identity, and
  must not be conflated with one.
- **There is no `last_compaction_time` field.** An earlier revision of this doc
  listed one in `SstMeta` and in the `UpdateTable` mask. It does not exist
  (`src/manifest.rs:38-77`; `grep -rn last_compaction_time src/` → no hits).
  Dropped from the mask below.
- There is no capability/format-version word in `Manifest` today. Op 11
  `SetCapability` presumes 1.0's v2 header and is gated on it.

### The 20 `persist_manifest` call sites

`grep -rn persist_manifest src/`, minus the definition and comments:

| # | Site | Enclosing fn | Becomes |
| ---: | --- | --- | --- |
| 1 | `db.rs:655` | `ensure_instance_nonce` | `SetNonce` |
| 2 | `db.rs:735` | `migrate_to_unified` | `SetWalLayout(true)` |
| 3 | `db.rs:769` | `create_column_family` | `CreateCF` |
| 4 | `db.rs:852` | `create_column_families` | N × `CreateCF`, **one** edit |
| 5 | `db.rs:926` | `drop_column_family` | `DropCF` |
| 6 | `db.rs:968` | `clear_column_family` | `DropCF` + `CreateCF`, one edit |
| 7 | `db.rs:1054` | `close` (result discarded, `let _ =`) | final snapshot compaction |
| 8 | `db.rs:1278` | `flush_per_cf` | N × `AddTable` |
| 9 | `db.rs:1317` | `flush_unified` | N × `AddTable` across CFs, one edit |
| 10 | `compaction.rs:405` | `run_fifo` | `RemoveTables` |
| 11 | `compaction.rs:958` | `compact_inputs` | `RemoveTables` + N × `AddTable`, one edit |
| 12 | `parts.rs:339` | `detach_part` | `RemoveTables` |
| 13 | `parts.rs:496` | `attach_part` | N × `AddTable` |
| 14 | `parts.rs:635` | `attach_part_by_ref` | N × `AddTable` (each with `object`) |
| 15 | `parts.rs:1001` | part-mover flip in `relocate_part` | N × `UpdateTable{Tier,Object}` |
| 16 | `parts.rs:1267` | `add_partition_rule` | `SetCFConfig` |
| 17 | `parts.rs:1286` | `remove_partition_rule` | `SetCFConfig` |
| 18 | `maintenance.rs:168` | `snapshot_to` (checkpoint + backup) | forced snapshot compaction, no edit |
| 19 | `maintenance.rs:253` | `clone_column_family` | `CreateCF` + N × `AddTable`, one edit |
| 20 | `ingest.rs:135` | `Ingestion::finish` | N × `AddTable` |

`freeze_part` and `export_part` correctly do **not** persist (no
`persist_manifest` in `src/parts.rs:658-780`), so freeze keeps writing its
one-part slice as a plain snapshot with no log.

### The six publication primitives

`update_levels` is **not** "the" publication primitive — it has exactly one
caller outside its module (`src/compaction.rs:864`). Six functions take
`self.state.write()` and publish a new level set:

| Primitive | `src/column_family.rs` | Callers |
| --- | ---: | --- |
| `install_handles_l0` | 839 | flush (`install_l0`:849), ingest:134, attach:493/632 |
| `update_levels` | 1307 | compaction:864 |
| `install_levels` | 1419 | `clone_column_family`:252 |
| `remove_bottom_tables` | 1534 | `detach_part`:338 |
| `insert_bottom_sorted` | 1565 | attach:491/630 |
| `swap_bottom_tables` | 1580 | mover flip:1000 |

`catalog_txn` subsumes **all six**: they become reachable only from the
transaction's publish closure, which is what makes "no catalog mutation outside
`catalog_txn`" true rather than aspirational. Three further publications are not
level-set swaps and must be inside the same closure:

- the `DbInner::cfs` / `cf_by_id` registry insert/remove (sites 3–6, 19),
- `DbInner::wal_layout` (site 2) and `DbInner::instance_nonce` (site 1),
- `ColumnFamily::append_partition_rule` / `remove_partition_rule` (sites 16–17),
  whose effect reaches the manifest through `effective_config()`.

Nothing else may mutate catalog state. `take_fifo_victims`
(`src/compaction.rs:401`) mutates the level set inside itself and must be split
into "select victims" + "publish removal" for site 10.

## Prerequisites (land before any 2.2 slice)

1. **`Manifest::save`'s directory fsync must propagate.**
   `src/manifest.rs:145-152`:
   ```rust
   std::fs::rename(&tmp, path)?;
   if let Some(dir) = path.parent() {
       if let Ok(d) = std::fs::File::open(dir) {
           let _ = d.sync_all();          // <- error discarded
       }
   }
   ```
   Both the open failure and the `sync_all` error are dropped. Every other
   durability path in the crate uses `util::sync_parent_dir`
   (`src/util.rs:14-22`), which propagates. Under 2.2 the snapshot-compaction
   protocol performs **two** renames whose durability is load-bearing, so
   convert `Manifest::save` to `util::sync_parent_dir`. This gap is not in the
   review's F-list and will otherwise be missed. (RV-F4 and RV-F5 — attach and
   WAL directory fsyncs — already landed, `c6c9471` / `04592de`; this is the
   remaining one.)
2. **`close` must stop discarding its persist result.** `src/db.rs:1054` is
   `let _ = self.inner.persist_manifest();` inside a function that returns
   `Result`. Under 2.2 that call becomes the final snapshot compaction; a
   silently dropped failure means the next open replays a longer log than it
   should, or fails. Propagate it.

Both are small, independently testable, and independently valuable.

## Files

```text
MANIFEST         v2 snapshot (1.0) + the three edit-log fields below
MANIFEST-EDITS   header + append-only numbered records
```

Snapshot side (1.0's v2 tagged tail — **new** tag `MANIFEST_EDITS_TAG =
b"ONDAMED1"`, placed in 1.0's tagged-tail dispatch loop after `INSTANCE_TAG`
and `FORMAT_CAPS_TAG`, before `WAL_LAYOUT_TAG`):

```text
ONDAMED1 | generation u64 LE | applied_through u64 LE | next_edit_id u64 LE
```

The whole-file CRC32 already covers the tail (`src/manifest.rs:161-163`), so no
new checksum is introduced on the snapshot side — AGENTS.md invariant 4 holds
by construction.

## Wire format

All integers little-endian; `uvarint` is LEB128 and `varint` is zig-zag LEB128
(`src/encoding.rs`); `checksum` is `crate::encoding::checksum`, the same CRC the
WAL, blocks, and `MANIFEST` use.

Primitive encodings used below:

```text
uvarint      LEB128, 1..=10 bytes
varint       zig-zag LEB128 (signed)
bytes        uvarint len | len raw bytes
str          bytes, UTF-8 validated on decode (invalid UTF-8 => Corruption)
opt<T>       0x00 = None | 0x01 followed by T
u64le        8 bytes little-endian
```

### Header (fixed 28 bytes, at offset 0)

```text
off  len  field
  0    4  magic  u32 LE = 0x4F4E_4445 ("ONDE"; on disk: 45 44 4E 4F)
  4    4  schema u32 LE = 1
  8    8  base_applied_through u64 LE   — no record in this file has id <= this
 16    8  snapshot_generation  u64 LE   — informational only (see Recovery r3)
 24    4  crc32 u32 LE over bytes [0, 24)
```

The magic is ondaDB-namespaced. The earlier `0x5744_4D45` "WDME" is rejected:
"WD…" is the wavesdb namespace, and the two engines are expected to share
tiers — a wavesdb-looking magic on an ondaDB file invites exactly the
cross-engine mount confusion this plan's own risk register warns about.
ondaDB's manifest magic is `0x5756_4D46` "WVMF" (`src/manifest.rs:20`), stored
little-endian the same way.

A file shorter than 28 bytes, or with a bad header CRC or unknown magic/schema,
is `Corruption` — never a torn tail. A header is written once, by
snapshot compaction, and fsynced before any record is appended.

### Record framing (records begin at offset 28, contiguous)

```text
off  len      field
  0    4      len u32 LE     — payload byte count; record occupies 8 + len bytes
  4    4      crc32 u32 LE   — over payload bytes [8, 8 + len)
  8  len      payload
```

```text
payload:  edit_id u64 LE | op_count uvarint | op × op_count
op:       op_code uvarint | op payload
```

`len` is capped at `MAX_EDIT_RECORD_BYTES = 64 MiB` (**new** constant); a
larger value is `Corruption`, checked before any allocation.

**Do not reuse the WAL's framing helpers.** The shape matches
`[len u32][crc32 u32][payload]` (`docs/formats.md` § WAL) but the torn-tail
contract is the opposite: `wal::replay` treats a bad trailing frame as a clean
tail; here only an *EOF-truncated* header or payload is a clean tail, and a
complete record with a bad CRC is `Corruption` (Recovery r5).

### Op payloads

`sst_meta` (field order mirrors the `SstMeta` declaration exactly, so the
inventory guard can be a field-by-field walk):

```text
sst_meta:
  id             uvarint
  level          uvarint
  num_entries    uvarint
  num_tombstones uvarint
  max_seq        uvarint
  klog_size      uvarint
  vlog_size      uvarint
  min_key        bytes
  max_key        bytes
  partition      opt<str>
  tier           opt<str>
  max_entry_time opt<varint>      (i64, Unix nanoseconds)
  object         opt<str>
```

`UpdateTable` field mask — `uvarint` bitset; present values follow in
**ascending bit order**:

```text
0x01 Level         uvarint
0x02 Tier          opt<str>
0x04 Object        opt<str>
0x08 Partition     opt<str>
0x10 MaxEntryTime  opt<varint>
```

A mask of 0, or a bit above 0x10, is `Corruption`.

```text
 1  AddTable      cf str | sst_meta
 2  RemoveTable   cf str | id uvarint | expected_level uvarint
 3  UpdateTable   cf str | id uvarint | mask uvarint | values (ascending bit order)
 4  CreateCF      name str | config bytes
 5  DropCF        name str
 6  SetCFConfig   name str | config bytes
 7  SetNextFileID value uvarint
 8  SetGlobalSeq  value uvarint
 9  SetWalLayout  unified u8 (0 | 1; any other byte is Corruption)
10  SetNonce      nonce u64le
11  SetCapability bits u64le
12  RemoveTables  cf str | count uvarint | id uvarint × count
```

Op codes 13..63 are unassigned and reject as `Corruption` naming the code and
the op index; codes ≥ 64 are never assigned (mirroring the WAL-kind rule).

**Preconditions**, checked during `Apply`; a failure is `Corruption` naming the
op index:

```text
 1  AddTable      cf exists; id absent from that cf
 2  RemoveTable   id present in cf at expected_level
 3  UpdateTable   id present in cf
 4  CreateCF      name absent
 5  DropCF        name present; all of its tables removed by the same edit
 6  SetCFConfig   name present
 7  SetNextFileID value >= current
 8  SetGlobalSeq  value >= current
 9  SetWalLayout  one-way false -> true
10  SetNonce      not yet set
11  SetCapability known bits only (1.0's KNOWN_CAPS)
12  RemoveTables  every id present in cf
```

Applying an edit is all-or-nothing: build the candidate catalog, run every
precondition, then swap. Edit ids are strictly `previous + 1`; a duplicate or a
gap is `Corruption`.

AddTable's "id absent" precondition **holds against every current producer**,
verified: `attach_part_by_ref` mints a fresh id (`src/parts.rs:569`) and
validates the carried object name against the part manifest (`:576-592`);
`attach_part` stamps fresh metadata on a newly copied file (`:417-431`);
`clone_column_family` assigns `new_meta.id = new_id`
(`src/maintenance.rs:241`). `attach_part_by_ref` always carries
`object: Some(..)` — it rejects a part whose table has no object name
(`:554-560`). `relocate_part` keeps the id and mutates tier **and** object, so a
move onto a shared tier is `UpdateTable{Tier, Object}`, not `UpdateTable{Tier}`.

## `catalog_txn` — the one structural-operation shape

One **new** `DbInner::catalog_txn(edit, publish)` helper owns this ordering;
after migration, no catalog mutation happens outside it. 1.2's excise and the
part mover plug into the same helper.

1. Build the `VersionEdit` and the candidate in-memory catalog. Publish
   nothing.
2. Ensure every newly referenced file is finished and fsynced
   (`Writer::finish` / `StorageWriter::finish`) — this preserves the RV-F4 fix
   (`c6c9471`), it does not re-do it.
3. Under `manifest_mu`: append the complete record, flush, **fsync
   `MANIFEST-EDITS`**. This is the commit point.
4. Publish the candidate state (the six primitives plus the three non-level
   publications above).
5. Retire removed handles after publication, through
   `DbInner::remove_sst_file` (`src/db.rs:344-352`) so checkpoint/backup can pin
   the file set — AGENTS.md invariant 6.
6. Still under `manifest_mu`, check the snapshot-compaction trigger and run it
   if it fires (§Snapshot compaction).
7. On append/fsync failure: discard the candidate, leave the old state visible,
   clean never-installed outputs, and run the existing poison policy
   (`src/db.rs:320-325`).

**Appends hold `manifest_mu`** (step 3) and snapshot compaction runs inside the
same critical section (step 6). This is not optional: snapshot compaction writes
a fresh `MANIFEST-EDITS.tmp` with `base = N` and renames it over the live file,
so any record appended to the old file between the snapshot write and the rename
would be **silently lost**. `persist_manifest` already takes `manifest_mu`
(`src/db.rs:299`), so this preserves today's serialization rather than adding
one.

**Read-only.** `catalog_txn` returns early exactly as `persist_manifest` does
(`src/db.rs:293-295`) — but callers that *read* the catalog from disk must not
rely on that. See §Checkpoint, backup, freeze.

### The publish/persist inversion

Step 4 publishes **after** the durable edit. Today ondaDB publishes **before**
persisting at every mutating site — this is universal, not a six-site quirk:

- flush: `install_l0` → `install_handles_l0` (`src/column_family.rs:849,839`)
  then `db.persist_manifest()` (`src/db.rs:1278`); same for `flush_unified` via
  `ingest_l0` (`:1317`);
- compaction: `install_compaction_outputs` → `update_levels`
  (`src/compaction.rs:850` → `update_levels` (`:864`)) then persist (`:958`);
- FIFO: `take_fifo_victims` mutates the level set, then persist
  (`src/compaction.rs:401-405`).

Six of the sites additionally assert or depend on that order in code, and get
their own migration note below:

| Site | Today | Migration note |
| --- | --- | --- |
| `detach_part` `parts.rs:333-339` | in-code comment names `remove_bottom_tables` → `persist_manifest` as "the atomic commit point" | The comment must be **rewritten**, not silently invalidated: the commit point moves to the edit fsync. Post-migration crash semantics are unchanged in effect (files stay, catalog does not reference them, reopen is clean) but the window shifts. |
| `relocate_part` flip `parts.rs:1000-1001` | `swap_bottom_tables` then persist, with a comment naming persist as "the durable commit point" | Same comment rewrite. The flip must be one `catalog_txn` carrying every `UpdateTable`; a partially applied flip is not representable. |
| `attach_part` `parts.rs:491-496` | stages handles, publishes, persists; an existing rollback path unlinks copied files on failure (`:482-489`) | Extend that rollback to cover append/fsync failure: the copies are unlinked and nothing is published. This is the model for the whole attach/ingest/clone family. |
| `attach_part_by_ref` `parts.rs:630-635` | same shape, zero-copy | No copies to unlink; rollback is "discard staged handles". |
| `clone_column_family` `maintenance.rs:252-253` | `install_levels` then persist | The destination CF is created *and* populated in one edit (`CreateCF` + `AddTable`×N); on failure the hard links it created must be unlinked. |
| `ingest.rs:134-135` | `install_handles_l0` then persist | The finished tables are already fsynced; on failure they are unlinked and never published. Ingest is an `AddTable` producer and belongs with flush in the migration order. |

Two sites get a **correctness improvement** from the inversion, worth calling
out so it is not mistaken for a regression: `drop_column_family`
(`src/db.rs:924-927`) and `clear_column_family` (`:955-968`) today
`remove_dir_all` the CF **before** persisting. A crash in that window leaves a
manifest referencing a directory that no longer exists. Under `catalog_txn` the
`DropCF` edit is durable before any file is unlinked — the same shape as
invariant 1.

### WAL reclaim (AGENTS.md invariant 1)

The single highest-consequence line of this migration. Today the gate is "only
after a successful `persist_manifest`":

```rust
// src/db.rs:1278
if db.persist_manifest().is_ok() { for path in wal_paths { wal::remove_wal_files(path); } }
// src/db.rs:1317
if all_slices_flushed && db.persist_manifest().is_ok() { … }
```

Under 2.2 the gate becomes **"only after the edit record's fsync returned
`Ok`"** — i.e. after `catalog_txn` step 3, not after a snapshot write. Snapshot
compaction is a space optimization and must never be a durability
precondition. Invariant 1 is restated as:

> SSTable written → `sync_all` on klog+vlog → parent-dir fsync (all inside
> `Writer::finish`) → **edit appended and fsynced to `MANIFEST-EDITS`** → only
> if that returned `Ok` may WAL files be deleted (`wal::remove_wal_files`).
> Same ordering for compaction: edit fsync before input-file deletion.

AGENTS.md must be updated in the same commit that flips the enable switch.

## Snapshot compaction protocol

`N` = last durable edit id. Entirely under `manifest_mu`:

1. Write `MANIFEST.tmp` {generation `G+1`, `applied_through = N`,
   `next_edit_id = N+1`, full catalog}; fsync. Reuse `Manifest::save` — it
   already does temp + `sync_all` + rename + dir fsync
   (`src/manifest.rs:132-152`, with the prerequisite fix).
2. Rename over `MANIFEST`; fsync the DB directory.
3. Write `MANIFEST-EDITS.tmp` {header with `base_applied_through = N`,
   `snapshot_generation = G+1`, no records}; fsync.
4. Rename over `MANIFEST-EDITS`; fsync the DB directory again.

**Never truncate the live edit file in place.** A crash between 2 and 4 leaves
the old log in place; recovery skips records with id ≤ `applied_through`
(Recovery r4).

Trigger, checked after each append inside the same `manifest_mu` section:
`edit_bytes > max(4 MiB, snapshot_bytes)` **or** `edit_count > 4096`.
Constants, not options, in v1.

**Temp-file paths.** `Manifest::save` uses `path.with_extension("tmp")`
(`src/manifest.rs:136`) → `<dir>/MANIFEST.tmp`, which is exactly the path step 1
names; reusing `Manifest::save` is the right call and removes the collision
question. The edit log's temp is `<dir>/MANIFEST-EDITS.tmp`. Both are unlinked
at open **before** anything is loaded and are never read — a leftover temp is a
crash artifact, not state.

## Recovery rules

r1. Load and CRC-verify `MANIFEST`. A CRC-invalid manifest fails `DB::open`
    (today's behavior, `src/manifest.rs:317-325`); a missing one is an empty
    database.

r2. Require `CAP_MANIFEST_EDITS` in the snapshot's capability word before
    consulting `MANIFEST-EDITS`. A log file present without the capability is
    `Corruption` — it means a newer binary wrote state this one cannot
    interpret.

r3. Verify the log header: magic, schema, header CRC. Accept iff
    **`header.base_applied_through ≤ snapshot.applied_through`**. That is the
    whole predicate. `snapshot_generation` is **informational only** — it is
    logged and golden-pinned, and carries no decision power: the legal
    crash-between-steps-2-and-4 state has `MANIFEST.generation = G+1` while the
    surviving log says `snapshot_generation = G`, so requiring equality would
    reject a legal state. Do not invent a second check; two implementers writing
    two different predicates is exactly what this paragraph exists to prevent.

r4. Replay records with strictly increasing ids, skipping ids ≤
    `snapshot.applied_through` and applying the rest. The first applied id must
    be `applied_through + 1`; every subsequent id must be `previous + 1`. A gap
    or a duplicate is `Corruption`.

r5. **An EOF-truncated record header or payload is a clean torn tail and ends
    replay. A complete record with a bad CRC, an unknown op code, a wrong id, or
    a failed precondition is `Corruption` — never silently a tail.**

r6. A missing edit file is valid only for the fresh-capability transition state
    the snapshot explicitly represents (`applied_through == next_edit_id - 1`
    and no capability-history marker demanding a log).

r7. Reconcile `next_file_id ≥ max table id + 1` and `global_seq ≥ max
    max_seq`, then verify that every referenced local file exists before
    serving. (`SetGlobalSeq value ≥ current` is safe because
    `persist_manifest` writes `global_seq: self.visible_seq()`
    (`src/db.rs:303`), monotone by AGENTS.md invariant 5.)

r8. Read-only opens replay the log but never compact it and never write.

## Checkpoint, backup, freeze

**Both `checkpoint` and `backup` materialize a fresh v2 snapshot with an empty
log.** The earlier "backup copies `MANIFEST` plus the edit bytes through the
last durable id — a byte prefix, consistent by construction" design is
**rejected**: it reintroduces the bug RV-F1 fixed (`f306b2b`). Since that fix,
`snapshot_to` **rewrites** the destination catalog per table
(`src/maintenance.rs:186-197`): files are placed into the destination's
default-tier layout (`cf-<name>/<id>.klog`) and `sst.tier` / `sst.object` are
cleared so the copy is self-contained. A byte-identical manifest plus an edit-log
prefix would reference tiered ids and object names whose bytes now live at
default-tier paths.

So:

- `checkpoint` / `backup` (`snapshot_to`, site 18): pause deletions (already
  done, `src/maintenance.rs:158-161`), flush, force one snapshot compaction on the
  **source**, then build the destination `Manifest` in memory, rewrite
  `tier`/`object` per table as today, and `Manifest::save` it into the
  destination with `generation = 1`, `applied_through = 0`, `next_edit_id = 1`
  and **no `MANIFEST-EDITS` file**. The destination is a snapshot-only database;
  it grows a log the first time it is opened writable and mutated.
- **Read-only source.** `snapshot_to` on a read-only DB cannot force a snapshot
  compaction (`catalog_txn`, like `persist_manifest`, returns early) and today
  silently uses whatever `MANIFEST` is on disk. Under 2.2 that would drop every
  edit since the last compaction. `snapshot_to` must therefore load the
  snapshot **and replay the edit log** into the in-memory `Manifest` before
  rewriting it. Without this the "read-only-capable backup" claim is false.
- `freeze_part` writes its one-part slice as a plain snapshot with no log
  (unchanged — it does not call `persist_manifest` today).

## Slices

1. **Catalog inventory test.** Construct a `Manifest` with every field of
   `Manifest`/`CfManifest`/`SstMeta` set to a distinguishable value, round-trip
   it through an edit sequence, and fail if any field is not covered by an op.
   Manual field enumeration in the test — Rust has no reflection, so the list
   *is* the guard. Must name the config blob as the carrier for partition rules
   and tier defs.
2. **Op codec + `Apply` + preconditions + fuzz.**
3. **Log codec**: header, record framing, torn-tail vs bad-CRC, golden bytes.
4. **v2 snapshot fields** (`ONDAMED1` tail) + golden bytes.
5. **`Recover`** + every R* row.
6. **Prerequisites**: `Manifest::save` → `util::sync_parent_dir`; `close`
   propagates its result.
7. **Fault-injecting sync shim** (a `Storage`-style wrapper or a test-only file
   hook) + the S* and A* rows.
8. **`catalog_txn`** + A* tests against a fake sink; the six publication
   primitives become transaction-private.
9. **Migrate call sites**, in this order: flush + unified flush → **ingest** →
   compaction (merge, then FIFO) → mover/parts → maintenance/CF lifecycle →
   open paths (open may keep writing snapshots).
10. **Backup/checkpoint/freeze semantics** + enable switch + close-time
    compaction.
11. **10k-table model test + bench**: edits vs full snapshot bytes, replay
    time, append fsync count, plus `../bench` structural-op p99 evidence.

## Crash matrix (drive with the shim; wavesdb numbering kept)

A1 before write · A2 torn record · A3 unsynced-complete (legal New) ·
A4 sync-error (poison; Old-or-New both consistent) · A5 after sync before
publish · A6 after publish before retire → swept as orphans. **RV-M4's
default-tier orphan sweep landed** (`76cdae4`), so A6 is a statement, not a
conditional — but the named-tier/S3 gap remains: `sweep_move_orphans` and
compaction's obsolete-input delete are still local-path only (AGENTS.md § S3
tiering), so an S3 object orphaned this way leaks storage without affecting
reads.

S1–S7 snapshot/reset windows (crash after each of the four protocol steps, plus
the two "old log survives" states and the trigger-fires-during-append case).

R1 bad header · R2 complete-record bad CRC **is corruption** · R3 id gap ·
R4 invalid transition (precondition failure) · R5 log without capability ·
R6 previous-release refusal, driven by a frozen VERSION-1 decoder compiled into
the test rather than a changed constant · R7 `base_applied_through >
applied_through` → corruption · R8 leftover `MANIFEST.tmp` /
`MANIFEST-EDITS.tmp` at open are unlinked and ignored.

## Implementation tasks

Ordered; each is one TDD step — write the named test first, watch it fail, then
implement. Gate after **every** task: `cargo test`, `cargo test --features
unsafe-fastpath`, `cargo clippy --all-targets`, `cargo clippy --all-targets
--features unsafe-fastpath`. Check each test binary for the presence of
`test result: ok`; never pipe through `tail`.

1. **Prerequisite: propagating dir fsync.** `src/manifest.rs:145-152` →
   `crate::util::sync_parent_dir(path)?`. Test in `src/manifest.rs`
   `#[cfg(test)]`: `save_propagates_a_directory_fsync_failure` (save into a
   path whose parent is removed between write and rename, or via the shim from
   task 9 if that ordering is easier) and
   `save_still_round_trips_after_the_fsync_change`.
2. **Prerequisite: `close` propagates.** `src/db.rs:1054` drops the `let _ =`.
   Test in `tests/db.rs`: `close_reports_a_failed_final_persist`.
3. **Catalog inventory guard.** New `tests/manifest_edits.rs`. Test
   `every_manifest_field_is_covered_by_an_op` — a manual enumeration of all 5
   `Manifest`, 3 `CfManifest`, and 13 `SstMeta` fields, each set to a
   distinguishable value, asserting a round-trip through ops reproduces it.
   Include `partition_rules_and_tier_defs_travel_in_the_config_blob`.
   Write this before the codec so the codec is built to satisfy it.
4. **Op codec.** New `src/manifest_edit.rs`: `VersionEdit`, `Op`,
   `encode_op`/`decode_op`, `sst_meta` codec, the `UpdateTable` mask. Tests
   in-module: `every_op_round_trips`, `sst_meta_round_trips_with_all_options_set`,
   `sst_meta_round_trips_with_all_options_none`,
   `update_mask_values_decode_in_ascending_bit_order`,
   `unknown_op_code_is_corruption_naming_the_index`,
   `zero_update_mask_is_corruption`, `invalid_utf8_cf_name_is_corruption`,
   `set_wal_layout_rejects_bytes_other_than_zero_and_one`.
5. **`Apply` + preconditions.** `apply_edit(&mut Manifest, &VersionEdit)`,
   all-or-nothing. Tests in-module, one per precondition:
   `add_table_rejects_a_duplicate_id`, `remove_table_rejects_a_wrong_level`,
   `update_table_rejects_a_missing_id`, `create_cf_rejects_an_existing_name`,
   `drop_cf_rejects_leftover_tables_in_the_same_edit`,
   `set_next_file_id_rejects_a_decrease`, `set_global_seq_rejects_a_decrease`,
   `set_wal_layout_is_one_way`, `set_nonce_rejects_a_second_mint`,
   `set_capability_rejects_unknown_bits`,
   `remove_tables_rejects_an_absent_id`,
   `a_failed_precondition_leaves_the_manifest_untouched`.
6. **Fuzz the op decoder.** `op_decoder_never_panics_on_arbitrary_bytes`
   (seeded PRNG + single-byte mutations of the golden ops). Green before
   task 8.
7. **Log codec.** Header + record framing + `MAX_EDIT_RECORD_BYTES`. Tests
   in-module and in `tests/manifest_edits.rs`:
   `header_bytes_are_frozen` (byte-exact 28 bytes, magic on disk
   `45 44 4E 4F`), `record_bytes_are_frozen`,
   `short_file_is_corruption`, `bad_header_crc_is_corruption`,
   `unknown_magic_is_corruption`, `unknown_schema_is_corruption`,
   `truncated_record_header_is_a_clean_tail`,
   `truncated_record_payload_is_a_clean_tail`,
   `complete_record_with_bad_crc_is_corruption`,
   `oversized_record_length_is_corruption_before_allocation`.
8. **v2 snapshot tail.** `ONDAMED1` in 1.0's tagged-tail dispatch. Tests in
   `src/manifest.rs`: `edits_tail_round_trips`,
   `edits_tail_bytes_are_frozen`, `edits_tail_coexists_with_object_and_nonce_tags`,
   `a_manifest_without_the_edits_tail_decodes_as_generation_zero`.
9. **Fault-injecting sync shim.** Test-only wrapper that can fail `write`,
   `flush`, `sync_all`, or `rename` at the Nth call. Tests:
   `shim_fails_the_selected_call_and_no_other`.
10. **Recovery.** `recover_catalog(dir) -> Result<Manifest>`. Tests in
    `tests/manifest_edits.rs`, one per R row:
    `recovery_applies_records_after_applied_through`,
    `recovery_skips_records_at_or_below_applied_through`,
    `recovery_rejects_a_base_ahead_of_applied_through`,
    `recovery_rejects_an_id_gap`, `recovery_rejects_a_failed_precondition`,
    `recovery_rejects_a_log_without_the_capability`,
    `recovery_accepts_a_missing_log_for_the_transition_state`,
    `recovery_reconciles_next_file_id_and_global_seq`,
    `recovery_unlinks_leftover_tmp_files`,
    `a_frozen_version_one_decoder_refuses_a_v2_snapshot`.
11. **Snapshot compaction.** The four-step protocol under `manifest_mu`, plus
    the trigger. Tests driven by the shim, one per S row:
    `crash_after_snapshot_write_replays_the_old_log`,
    `crash_after_snapshot_rename_replays_the_old_log`,
    `crash_after_new_log_write_replays_the_old_log`,
    `crash_after_new_log_rename_starts_clean`,
    `compaction_never_truncates_the_live_log`,
    `trigger_fires_on_byte_threshold`, `trigger_fires_on_count_threshold`.
12. **`catalog_txn`.** The seven-step helper; the six publication primitives
    become transaction-private. Tests against a fake sink, one per A row:
    `txn_publishes_nothing_before_the_fsync`,
    `txn_publishes_after_a_successful_fsync`,
    `txn_rolls_back_and_poisons_on_a_sync_error`,
    `txn_leaves_old_state_visible_on_an_append_failure`,
    `txn_retires_handles_only_after_publication`,
    `txn_is_a_noop_in_read_only_mode`,
    `no_publication_primitive_is_reachable_outside_txn` (compile-time via
    visibility; assert with a `#[deny]`-style module test or a grep test).
13. **Migrate flush + unified flush** (sites 8, 9) and restate invariant 1.
    Tests in `tests/db.rs` / `tests/unified.rs`:
    `flush_reclaims_wal_only_after_the_edit_fsync`,
    `unified_flush_writes_one_edit_for_every_cf_slice`,
    `crash_between_edit_fsync_and_wal_delete_replays_cleanly`.
    Update AGENTS.md invariant 1 in this commit.
14. **Migrate ingest** (site 20). Tests in `tests/db.rs`:
    `ingest_finish_writes_one_edit_for_all_tables`,
    `ingest_unlinks_its_tables_when_the_append_fails`.
15. **Migrate compaction** (sites 10, 11); split `take_fifo_victims`. Tests:
    `compaction_writes_one_edit_with_removes_and_adds`,
    `compaction_deletes_inputs_only_after_the_edit_fsync`,
    `fifo_publishes_eviction_only_after_the_edit_fsync`.
16. **Migrate mover and parts** (sites 12–17); rewrite the two commit-point
    comments. Tests in `tests/maintenance.rs`:
    `detach_writes_one_remove_tables_edit`,
    `relocate_flip_is_one_edit_of_tier_and_object_updates`,
    `attach_rolls_back_its_copies_when_the_append_fails`,
    `attach_by_ref_carries_an_object_in_every_add_table`,
    `partition_rule_changes_write_a_set_cf_config_edit`.
17. **Migrate maintenance and CF lifecycle** (sites 1–6, 19). Tests:
    `create_column_families_writes_one_edit_for_the_batch`,
    `drop_cf_makes_the_edit_durable_before_deleting_files`,
    `clear_cf_is_one_drop_plus_create_edit`,
    `clone_cf_is_one_create_plus_add_table_edit`,
    `nonce_and_wal_layout_edits_apply_once`.
18. **Checkpoint/backup/freeze** (site 18) + `close` compaction (site 7).
    Tests in `tests/maintenance.rs`:
    `checkpoint_writes_a_fresh_snapshot_with_no_edit_log`,
    `backup_writes_a_fresh_snapshot_with_no_edit_log`,
    `backup_from_a_read_only_db_includes_uncompacted_edits`,
    `backup_clears_tier_and_object_on_every_table` (guards RV-F1),
    `frozen_part_has_no_edit_log`,
    `close_compacts_the_log_and_reports_failure`.
19. **Enable switch.** `CAP_MANIFEST_EDITS` persisted before the first append;
    a database without it keeps writing full snapshots. Tests:
    `edits_are_written_only_after_the_capability`,
    `capability_enable_is_idempotent`,
    `a_pre_capability_database_opens_and_upgrades_cleanly`.
20. **Scale test + benchmark.** `tests/manifest_edits.rs`
    `ten_thousand_tables_replay_within_the_bound` (model test: build a 10k-table
    catalog, apply 4096 edits, assert replay time and append bytes are O(edit)),
    plus `../bench` flush/compaction p99 at the 10k-table fixture, ≥5 runs.

## Acceptance

O(edit) append bytes between snapshots; bounded replay; every crash-matrix row
green; `../bench` flush and compaction p99 improve at the 10k-table fixture
beyond baseline spread. The 12.4 MiB-per-persist figure at 100k parts is the
before number; the after number is published next to it.

## Rollback

A maintenance compaction to a single v2 snapshot with an empty log is always
possible, so the log can be disabled for future writes without stranding state.
Downgrading below v2 while capability history remains is not supported.
