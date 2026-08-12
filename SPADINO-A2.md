# A2 — Attach-by-reference (shared tiers)

For the ondaDB owner. Branch `feat/attach-by-reference`, based on `07aee3b`
(0.7.7). Requested by spadino; the consuming design is
`spadino/docs/milestones/M2-cas.md` §1 and decisions SP-005/SP-007.

## The ask

spadino's topology is one writer sealing immutable parts into an object store
and N disposable query nodes mounting them. Today that second step cannot be
expressed:

1. **Tier object paths derive from the local file id** —
   `{tier_root}/cf-{name}/{id}.klog`. File ids are per-database counters, so
   two databases pointed at one tier root collide: both eventually move *their*
   id 7, and the second PUT overwrites the first database's object.
2. **`attach_part` copies bytes.** It validates and copies file pairs into the
   target's own directory under fresh ids. For an S3-resident part that is a
   full download per attaching node — the opposite of "local SSD is only a
   cache".
3. **`attach_part` rejects foreign lineage.** A table whose `max_seq` exceeds
   the target's visible sequence is refused, so a fresh (empty) database can
   attach nothing.

## What this adds

Everything is **opt-in per tier** via `TierDef::shared()`. Databases that never
declare a shared tier are byte-identical to 0.7.7 in every persisted structure
and every path.

```rust
// Declaring a tier shared:
TierDef::new("cas", "/mnt/shared/onda").shared()      // or TierDef::s3(...).shared()

// SstMeta gains a tier-root-relative object path, None for every table
// written before A2 or on a non-shared tier:
pub struct SstMeta { ..., pub object: Option<String> }

// PartTable mirrors it, so an exported description names the shared objects:
pub struct PartTable { ..., pub object: Option<String> }

// The new API:
pub fn attach_part_by_ref(&self, cf: &Arc<ColumnFamily>, part: &PartManifest,
                          tier: &str) -> Result<()>
```

- **Object naming.** A move onto a *shared* tier names each file
  `cf-{cf}/{instance:016x}-{id}.klog` relative to the tier root, where
  `instance` is a per-database nonce minted once and persisted in the
  manifest. Two databases sharing a root can no longer collide, and the layout
  under the root keeps its `cf-*/` shape. Moves onto non-shared tiers keep the
  legacy id-derived path exactly.
- **`attach_part_by_ref`** registers the part's tables in the catalog with
  fresh local ids pointing at the shared objects — zero bytes copied. Each
  table's footer/index/bloom are opened and CRC-verified through the tier's
  backend (bounded reads, the same validation `Reader::open` always does), and
  the manifest's `num_entries`/`max_seq` claims are cross-checked against the
  footer; a mismatch rejects the whole part, nothing installed. The target
  adopts the part's sequence lineage by bumping its sequence floor past the
  tables' `max_seq` (the recovery path's `observe_seq`, reused) — the sealed
  entries become visible, and snapshots opened before the attach do not see
  them (the same semantics as attach and detach today).
- **Shared tiers are delete-free.** The engine never deletes an object on a
  shared tier: the mover's source-delete only touches origin-local files, the
  startup orphan sweep skips shared tiers, and compaction's obsolete-input
  deletion already resolves default-tier paths only. Reclaiming shared objects
  is the explicitly the layer above's job (spadino's mark→quarantine GC), same
  as the "no internal object CAS" rule. Without this rule one sharer's
  hygiene would be another sharer's data loss.

## Safety argument

Sharing is sound because shared parts are **immutable and single-writer**: a
part is produced by exactly one compaction in exactly one database, moved once,
and never appended. The read-only sharers hold catalog entries and block-cache
blocks keyed by their own fresh local ids (per-process, never reused), so no
cache aliasing is possible. **Mutable sharing is out of scope and unsupported**:
a sharer must never compact, detach, freeze, or re-tier an attached-by-ref
part's objects — and cannot, since all of those either operate on default-tier
files or are delete-free on shared tiers.

`attach_part_by_ref` trusts the `PartManifest`'s key ranges the way the catalog
trusts its own manifest (they steer placement and query routing, and the
per-table footer cross-check bounds the damage of a wrong claim to a wrong
routing, caught by block CRCs on read). A caller that requires byte-level
certainty runs `export_part` afterwards and compares digests — the existing
"does the peer hold exactly these bytes" tool.

## Persistence

Two new tagged manifest-tail sections, following the `ONDAWAL1` precedent, in
fixed order after the positional (partition, tier, max-entry-time) sections:

- `ONDAOBJ1` — per-CF `(count, (table_index, object)...)` name section,
  emitted only when some table carries an object.
- `ONDAINS1` — the 8-byte instance nonce, emitted once minted (first open
  under an A2 binary).

A pre-A2 manifest decodes with every `object = None` and no nonce. A pre-A2
binary refuses an A2 manifest (checksummed unknown tail → corruption error) —
fail-stop, the same downgrade posture as the 0.3.0 tier tail, documented in
`docs/parts-and-tiers.md`. A database that never declares a shared tier never
mints objects; its manifests stay pre-A2-readable until the nonce is minted,
and the nonce is only minted when the first shared tier is configured.

## What this deliberately does not do

- No sharing of mutable state: WAL, memtable, upper levels, non-shared tiers.
- No cross-database coordination: who attaches what, when objects die, and how
  a sharer learns a part exists are the consumer's catalog concerns (spadino's
  manifest tree), not the engine's.
- No digest re-verification on attach (that is `export_part`'s job, priced
  honestly).
- No change to `attach_part` (the copying form remains for same-lineage local
  restore).
