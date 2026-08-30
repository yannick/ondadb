# 1.1 — Merge operators

**Readiness:** design required; scheduled **after 1.2** (shared surfaces, and
range deletes are higher value). This ordering is binding and is what the phase
plan and the implementation plan's Wave C both state. **Effort:** 5–8
dev-weeks. **Baseline:** ondaDB 0.8.2 (`3afc3c1`). **wavesdb counterpart:**
1.1 — including its corrected fold rule (below), which is the part naive
implementations get wrong.

## Goal

Append operands without a caller-side Get/Put round trip and resolve them
through a deterministic per-CF operator. In ondaDB the round trip is
especially expensive: under MVCC, a read-modify-write costs a snapshot `get`
(the `Txn` overlay scan or a full source walk) *plus* the conflict-window
cost — exactly what counters/HLL pay today.

## Public API and registry

The right precedent is **`PartitionScheme::Unresolved` + `Options::partition_fns`**
(`config.rs:764–800`, `:254`), resolved at open by `resolve_partition_scheme`
(`db.rs:606–629`). `comparator_by_name` (`comparator.rs:174`) is *not* a model
for this: it is a **closed match** returning `None` for unknown names, not an
extensible registry.

```rust
// new, config.rs
pub trait MergeOperator: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &str;
    /// Operands oldest→newest. `existing = None` means no visible put base;
    /// a real empty value stays `Some(b"")`.
    fn full_merge(&self, key: &[u8], existing: Option<&[u8]>,
                  operands: &[&[u8]]) -> Result<Vec<u8>, String>;
}

// new, Options
pub merge_fns: Vec<Arc<dyn MergeOperator>>,

impl Txn { pub fn merge(&mut self, cf: &Arc<ColumnFamily>, key: &[u8], operand: &[u8]) -> Result<()>; }
impl DB  { pub fn merge(&self,    cf: &Arc<ColumnFamily>, key: &[u8], operand: &[u8]) -> Result<()>; }
```

- `ColumnFamilyConfig::merge_operator_name: Option<String>` (**new**;
  persisted in the config blob, `config.rs::encode`/`decode`).
- Enabling persists `CAP_MERGE_OPERANDS` and the operator name.

**Open-time operator check is a NEW explicit check.** Nothing today diffs a
caller-supplied config against the stored one: `recover_column_families`
(`db.rs:580–600`) decodes the *persisted* blob and resolves the comparator from
it, so a differing `merge_operator_name` supplied at reopen would simply be
ignored, not rejected. Add `resolve_merge_operator`, a mirror of
`resolve_partition_scheme`, called from the same place:

```rust
// new, db.rs — called from recover_column_families, beside resolve_partition_scheme
fn resolve_merge_operator(config: &mut ColumnFamilyConfig, opts: &Options, cf_name: &str)
    -> Result<()>
```

Rules, in order:

1. Stored name `None` → nothing to resolve.
2. Stored name `Some(n)` with no `opts.merge_fns` entry whose `name() == n` →
   `InvalidArgs` naming the CF and `n` ("…was written with merge operator
   {n:?}, which is not registered in `Options::merge_fns`"), never a silent
   fallback — the `PartitionScheme::Unresolved` stance.
3. Two registered operators with the same `name()` → `InvalidArgs` at open.
4. **The stored name always wins.** A caller cannot rename a CF's operator by
   passing a different config; `create_column_family` on an existing CF already
   returns `Exists` (`db.rs:756–758`), so there is no legitimate re-supply path.
   Renaming/hot-reconfiguring a CF is an explicit non-goal (AGENTS.md).

Folding with a *different* operator than wrote the operands is therefore
unreachable through the API; if it is ever reached (a hand-edited config blob)
it is `Corruption` at read, naming the key.

## Semantics

**Fold rule.** For key `k` at `read_seq`, over the versions of `k` in
newest→oldest order, considering only versions with `seq ≤ read_seq`:

1. Collect kind-4 (`KIND_MERGE`) entries into a list as they are seen.
2. The **base** is the first `Put`, `Delete` or `SingleDelete` encountered:
   `Put` ⇒ `existing = Some(value)`; `Delete`/`SingleDelete` ⇒
   `existing = None`. Versions older than the base are ignored.
3. If the sources are exhausted without a base, `existing = None`.
4. Reverse the collected operands to oldest→newest and return
   `full_merge(k, existing, &operands)`.
5. A group with no operand resolves exactly as today (no operator call, no
   allocation).

Point `get`, iterators and the `Txn::get` overlay all resolve identically;
overlay merges append to the buffered chain ahead of the committed ones.

`existing = None` vs `Some(b"")` is a real distinction and matches
`PointReadCandidate`'s found/deleted split (`column_family.rs::consider_sstables`
→ `candidate.consider(value, seq, found, deleted)`); pin it with a test in the
read-resolution slice.

No TTL on merge operands in v1 (per-operand expiry creates base-resurrection
semantics — rejected). A merge on `k` conflicts like a write on `k`:
`peek_seq` (`column_family.rs:1035–1078`) reads every source at `u64::MAX` and
takes the max `seq`, ignoring `tombstone`/kind entirely, so it needs no change.

**Compaction folding** (second deliverable) fold only a contiguous suffix of
the chain wholly at or below `oldest_snapshot`, reusing `VersionRetention`'s
`emitted_at_or_below_snapshot` machinery; the folded entry preserves the
**newest sequence represented** by the suffix. Stop folding at bases, deletes,
foreign mounts (`is_foreign_mount`, `compaction.rs:426`) and snapshot
boundaries. At the bottom level `decide` may drop the terminating `Delete`, so
**the fold must run inside retention, not after it**. Folding is an
optimization: a bug here silently changes history — hence the oracle below.

## Retention must be kind-aware in the SAME slice that lets kind 4 reach compaction

`VersionRetention::decide` (`compaction.rs:537–563`) is **not** a
pass-through:

```rust
if new_key { self.last_key = Some(key.to_vec()); self.emitted_at_or_below_snapshot = false; }
if seq <= self.oldest_snapshot {
    if self.emitted_at_or_below_snapshot { return Retention::Drop; }
    self.emitted_at_or_below_snapshot = true;
    if tombstone && self.bottom { return Retention::Drop; }
}
```

It keeps exactly **one** version at or below `oldest_snapshot` per user key and
drops every older one. Run unmodified over a merge chain, the second and
subsequent operands below the snapshot are dropped: the chain is silently
truncated and history is lost — with no folding bug at all, and no oracle in
the read-resolution slice that would catch it (the fold oracle arrives later).
"Ship read resolution first, folding later" is only safe if retention becomes
operand-aware *before* kind 4 can reach compaction.

Required, in the pass-through slice:

- `decide` gains a `kind` parameter:
  `fn decide(&mut self, key: &[u8], seq: u64, kind: u64, tombstone: bool, ttl: i64)
  -> Retention`. It has none today, so threading one through touches every call
  site — an unlisted change in the previous plan.
- **Retain every merge operand down to its base.** `emitted_at_or_below_snapshot`
  is not set by a kind-4 entry, and a kind-4 entry below the snapshot is never
  dropped by the "one version" rule; the flag is set only by the base that
  terminates the chain.
- The bottom-level tombstone drop is guarded: a `Delete` that terminates a
  chain whose operands are still live must not be dropped at the bottom until
  the whole chain is folded.
- The bottom-level TTL drop (`self.bottom && !tombstone && ttl != 0 && …`)
  never applies to kind 4 (operands carry no TTL).
- `Retention::Keep { filter_eligible }` computes
  `!tombstone && seq <= oldest_snapshot && (ttl == 0 || ttl > now)`. A merge
  operand is **not** filter-eligible: the bloom filter answers "is there a
  version of this key", and the operand's key is already contributed by its
  base or by the newest operand. Pin the chosen answer with a test either way —
  a wrong choice here is a silent false-negative bloom.

The same rule applies to `FlushMerge` (`memtable.rs:1049`, driven by
`write_l0_streaming`) — a separate merge path that must also stop collapsing
operand chains.

## Iterator operand accumulation (invariant 8)

`Iterator::resolve_current_group` (`iterator.rs:614–630`) drains the whole
user-key group but keeps only the newest visible version, via
`VisibleVersion::consider` (`:427–440`):

```rust
if seq > read_seq || (self.found && seq <= self.seq) { return VersionDecision::Ignore; }
```

and `capture_value()` (`:593`) overwrites `cur_val` on each
`VersionDecision::Value`. Merge resolution needs **all** operands of the group.
The heap yields newest-first, so the collected list is reversed before
`full_merge`.

This collides with **invariant 8**: `key()`/`value()` return slices borrowed
from per-child pinned `Block`s (`pinned_key`/`pinned_val`, `iterator.rs:383–385`),
and operands of one group can come from several children and several blocks —
they cannot all be pinned. Per-entry `Arc` clones of shared mmaps are not an
option either: that is the measured **3× scan regression** recorded in
`docs/performance.md`.

**Copy strategy (chosen):**

- A per-`Iterator` operand arena: `operands: Vec<u8>` plus
  `operand_spans: Vec<(usize, usize)>`, both `clear()`ed per group and never
  shrunk, so steady-state scanning allocates nothing after warm-up.
- The arena is entered **lazily**: the group is resolved by today's pinned,
  zero-copy path until the first kind-4 entry is seen. On that transition, the
  already-captured value (if any) is copied into the arena and `cur_val`
  switches to `CurVal::Buffered` — the same fallback shape 2.1 uses for
  `CurKey::Buffered` on delta blocks.
- A group with no kind-4 entry therefore executes exactly today's code, and a
  CF with no operator configured never enters the branch at all (the check is
  a single `Option::is_none()` on a field cached at `Iterator::new`).
- The folded result is written into `self.val` and exposed as
  `CurVal::Buffered`; no new pin slots, no change to `pinned_key`/`pinned_val`
  lifetimes.

**Measurement.** The previous acceptance claim — "no regression when the
option is unset (zero branch on the read path — the kind check rides the
existing flags dispatch)" — is **false on both counts**: under 1.0's extended
entry layout the kind does not ride the flags byte (it replaces it), so there
is at minimum a per-entry decode difference; and accumulation is new work.
Restate the gate as:

> **No measurable regression on scan and point-read throughput for a CF with
> no operator configured**, measured per `docs/performance.md`: profile first,
> change one thing, ≥5 runs, compare same-run ratios (the machine is
> thermally noisy, ±15–20%). Report the extended-layout decode cost separately
> from the accumulation cost, since the former is 1.0's and applies to every
> extended table.

## Slices

1. Trait + `Options::merge_fns` + `ColumnFamilyConfig::merge_operator_name` +
   `resolve_merge_operator` + reopen tests.
2. Envelope kind 4 through WAL / memtable / flush (`FlushMerge`, `write_l0`,
   `write_l0_streaming`, `ingest_l0`) / compaction writer, **together with the
   kind-aware `VersionRetention::decide`** — these ship as one change.
3. Read resolution in `get` / iterators / `Txn` overlay + reference-model
   tests + the operand-arena measurement.
4. Conflict model (`peek_seq` unchanged) + isolation-level tests.
5. Folding inside `VersionRetention` + fold oracle; an enable switch for a
   folding-off rollout.
6. Surfaces pass over the phase-1 checklist (parts / attach / backup / stats /
   docs).

## Implementation tasks

One commit per task after the 4-command gate; tests written first.

1. **Trait + registry + config.**
   Tests first in `config.rs` in-module: `merge_operator_name_round_trips()`
   (encode/decode of the config blob), `config_blob_without_operator_decodes_none()`
   (backward compatibility against a 0.8.2-shaped blob).
   Then add the trait, `Options::merge_fns`, the config field and its
   encode/decode.

2. **Open-time resolution.**
   Tests first in `tests/db.rs`:
   `reopen_without_registered_operator_is_error()` (assert the message names
   the CF and the operator name),
   `reopen_with_mismatched_operator_name_is_error()` (register an operator
   whose `name()` differs from the stored name),
   `duplicate_operator_names_in_options_is_error()`,
   `reopen_with_matching_operator_succeeds()`.
   Then add `resolve_merge_operator` and call it from
   `recover_column_families` beside `resolve_partition_scheme`.

3. **Kind 4 through the write path + kind-aware retention (one change).**
   Tests first:
   - `tests/db.rs`: `merge_operand_survives_flush()`,
     `merge_operand_survives_compaction()`,
     `operand_chain_below_snapshot_is_not_truncated()` — write N operands and a
     base, take no snapshot, force a full compaction, and assert all N operands
     are still present via a raw table scan (this is the regression the old
     slice ordering would have shipped);
   - `compaction.rs` in-module: `decide_keeps_every_operand_below_snapshot()`,
     `decide_keeps_terminating_delete_while_operands_live()`,
     `decide_drops_second_put_below_snapshot()` (the existing behavior must be
     unchanged for point kinds), `operand_is_not_filter_eligible()`;
   - `memtable.rs` in-module: `flush_merge_passes_operands_through()`.
   Then thread `kind` through `decide` and every call site, add the guards, and
   add kind 4 to `Txn::merge`/`DB::merge`, the WAL envelope, the memtable, the
   flush paths and the compaction writer.

4. **Read resolution.**
   Tests first in `tests/db.rs`:
   `get_folds_operands_oldest_to_newest()`,
   `delete_base_gives_existing_none()`,
   `empty_put_base_gives_existing_some_empty()`,
   `no_base_gives_existing_none()`,
   `operator_error_is_corruption_naming_the_key()`,
   `merge_reference_model()` — random Put/Delete/Merge histories replayed at
   many `read_seq`s against a sequential-application oracle, across TTL, point
   tombstones, custom comparators, both WAL layouts, reopen, and snapshots
   older and newer than the operands.
   Then implement resolution in `column_family.rs::consider_sstables` /
   `PointReadCandidate` and in `Iterator::resolve_current_group` with the
   operand arena.

5. **Accumulation measurement.** No new test; a recorded benchmark run per
   `docs/performance.md` (≥5 runs, same-run ratios) for: scan throughput on a
   no-operator CF, point-read throughput on a no-operator CF, and the same two
   on an operator CF with chain lengths 0/1/8/64. Revert honestly if the
   no-operator numbers move.

6. **Conflict model.** Tests first in `tests/db.rs`:
   `merge_conflicts_like_a_write_at_snapshot()`,
   `merge_conflicts_like_a_write_at_serializable()`,
   `read_committed_merge_does_not_conflict()`.
   `peek_seq` needs no change; the tests pin that.

7. **Folding.** Tests first:
   `fold_oracle()` — at every snapshot, pre- and post-compaction reads are
   identical; `fold_never_crosses_a_base()` (mixed chains: operand, base,
   operand); `fold_preserves_newest_sequence_of_the_suffix()`;
   `fold_stops_at_foreign_mount()`;
   `fold_does_not_run_above_oldest_snapshot()`;
   `folding_off_switch_disables_folding()`.
   Then implement folding inside `VersionRetention`.

8. **Surfaces pass.** parts/attach/backup/checkpoint/clone/stats/PerfContext/
   docs, per the phase-plan checklist; deterministic-operator contract
   documented on the trait (compaction may re-fold the same key repeatedly at
   unpredictable times).

## Acceptance

Counter-style workload phase: ops/s versus Get+Put at equal durability; chain
length over time with folding on and off. No measurable regression on scan or
point-read throughput for a CF with no operator configured, measured as
described above. Both feature configurations green.

## Rollback

Disabling new `merge` calls stops new operands; existing operands remain
readable (and must be — readers stay kind-aware).
