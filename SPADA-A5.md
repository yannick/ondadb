# A5 — Derived partitioning

For the ondaDB owner. Branch `spada/a5-derived-partitioning`, based on
`v0.3.1`. Requested by spada; the normative write-up is
`spada/design/stage0/A0-1-physical-data-layout.md` §6.

## The ask

spada partitions by `(namespace, cluster_key)` — a tenant and a time bucket.
At its stated scale that is roughly **7,200 partitions per column family**
(600 namespaces × 12 retained buckets).

`partition_rules` cannot express that, for two independent reasons:

1. **The durable count is bounded.** `partition_rules` persists behind a `u8`
   count. 0.3.1's overflow tail (`ONDAOVF1`) removed the silent truncation,
   but the shape is still an enumeration — one durable entry per partition,
   growing the config blob linearly.
2. **Resolution is a scan.** `partition_of` is a longest-prefix scan of the
   rule vector, run **once per key** written by a bottom compaction. At 7,200
   rules that is 7,200 prefix comparisons per key.

Neither is fixed by a wider count. The partition here is a *function of the
key*, not a set of names someone can enumerate in advance.

## What this adds

```rust
pub trait PartitionFn: Send + Sync + Debug {
    fn boundary_len(&self, key: &[u8]) -> usize;  // prefix that decides the partition
    fn name(&self, key: &[u8]) -> String;         // partition name for that key
    fn scheme_name(&self) -> &str;                // stable id, persisted
}

pub enum PartitionScheme {
    Rules,                        // default; today's behaviour
    Derived(Arc<dyn PartitionFn>),
    Unresolved(String),           // read from a manifest, not yet resolved
}
```

`ColumnFamilyConfig::partition_scheme` selects between them; `Options::partition_fns`
is the registry that resolves a persisted scheme name on open.

**Deviation from the requested shape, for your ruling.** A0-1 asked for
`Rules(Vec<PartitionRule>)`. I used a unit `Rules` variant that defers to the
existing `partition_rules` field instead, because putting the vector in the
enum would either (a) require removing the public `partition_rules` field —
a breaking change for every consumer — or (b) duplicate it, and the
compaction path would then clone the vector per run. `Unresolved` is an
addition; §"Persistence" explains why it is a variant rather than a side
channel.

`scheme_name` is also an addition to the requested trait: the per-key `name`
identifies a *partition*, and persistence needs to identify the
*partitioner*.

## Persistence

A boxed function is not serializable, so:

- The manifest records only `scheme_name()`, in its own tagged tail
  (`ONDAPFN1`), written **only** when a derived scheme is set.
- `DB::open` exchanges that name for the registered implementation in
  `Options::partition_fns`. This mirrors how `comparator_name` is already
  resolved via `comparator_by_name` — the same name-and-registry
  indirection, made extensible because partitioners are consumer-defined.
- **A missing or mismatched implementation is an error, not a fallback.**
  Reverting to rule-based partitioning would cut every part written
  afterwards on different boundaries while every operation reported success;
  the damage would surface much later as parts that detach, freeze and tier
  incorrectly. `Unresolved` exists so that a config which round-trips through
  a reader that could not resolve it **keeps the marker** rather than
  silently losing it.

An unresolved scheme that somehow reaches compaction returns an error rather
than partitioning blindly (`partition_resolver_snapshot`). `DB::open` makes
that unreachable; it is defence in depth, and it is tested.

## What is *not* changed, and the evidence

- **Cutting mechanism.** Bottom compaction still finishes the current output
  file when the boundary changes. A part is still a contiguous key range,
  still bottom-level only, still write-side-only policy.
- **Part lifecycle, tiering, mover, manifest tail.** Untouched. They address
  parts by name, and a derived scheme supplies names the same way rules do.
  `attach_part` now resolves through the scheme so an attached part is tagged
  the way compaction would have tagged it.
- **The rules path.** `rules_config_encoding_is_byte_identical` asserts a
  rules-only config carries no `ONDAPFN1` tail at all, so every existing
  manifest encodes and decodes exactly as before.
  `rules_path_cuts_the_same_parts` asserts the `img`/`log` cuts are unchanged.

One deliberate refinement: for a derived scheme the cut compares the
**boundary bytes**, not the resolved name. For rules the two are equivalent.
For a derived scheme the boundary is stronger — it depends only on
`boundary_len`, so a part stays a contiguous key range even if an
implementation's `name` collides across two boundaries.

## Item skipped, deliberately

A0-1 also asked to widen the `u8` rule counts to uvarint. **Already solved,
and better**: 0.3.1's `0150e97` added the `ONDAOVF1` overflow tail, which
preserves the 0.3.0 byte representation exactly and avoids the ambiguity
between a legacy `u8` count ≥ 128 and the first byte of a LEB128 count. A0-1
is stale on this point; nothing to do.

## Tests

In-crate (`src/parts.rs`, needs `bottom_parts`): boundary changes cut parts;
every SSTable in a part has both `min_key` and `max_key` inside that
partition; 300 partitions materialize (past the 255 ceiling); an unresolved
scheme refuses to compact.

Integration (`tests/derived_partitioning.rs`): rules-only encoding carries no
marker; rules cuts unchanged; reopen with the same scheme works; reopen
without the scheme errors naming the scheme and `partition_fns`; reopen with
a *different* scheme errors; an unresolved scheme survives re-encoding;
detach → attach preserves derived tags.

Both build configurations, `clippy --all-targets -D warnings`, `fmt --check`.

## Open questions for you

1. **The `Rules` variant shape** — unit variant deferring to
   `partition_rules` (chosen, non-breaking) vs `Rules(Vec<PartitionRule>)` as
   requested (breaking). Happy to change it if you would rather take the
   break now.
2. **Should `PartitionFn` be validated?** The contract (prefix-determined,
   order-compatible, pure) is documented but unchecked. A debug-only
   assertion that consecutive keys in a compaction never move *backwards*
   across boundaries would catch a misimplemented partitioner cheaply.
3. **Rules → Derived on an existing CF** is currently allowed (it is
   write-side-only policy, like `add_partition_rule` on a live CF). Derived →
   Rules is *not* — the marker persists and reopening without the function
   fails. Is that asymmetry the one you want?
4. **`bottom_parts` visibility.** It is `pub(crate)`, so consumers cannot
   enumerate parts; my structural tests had to live in-crate. A read-only
   public listing would help consumers verify their own partitioners. Out of
   scope here — flagging it.

---

## Owner rulings (0.4.0)

Reviewed against `v0.3.2` (the derived-partitioning merge) with a full test run
and the rules-path additivity assertions confirmed. The design is accepted as
merged; the four questions are resolved as follows.

1. **`Rules` variant shape — keep the unit variant.** The argument against
   `Rules(Vec<PartitionRule>)` is decisive on its own terms: it would either
   delete the public `partition_rules` field (breaking every consumer) or
   duplicate it and clone the vector per compaction run. The unit variant that
   defers to the existing field is the better design *independent of version*,
   so there is no reason to take the break even though 0.4.0 would permit it.
   A0-1's requested shape is declined.

2. **Validate `PartitionFn` — done, debug-only.** A misimplemented partitioner
   that is not order-compatible reopens a finalized boundary and produces a
   bottom SSTable spanning two partitions, which every operation would report as
   success. A `debug_assertions`-only guard in the bottom-compaction cut now
   catches exactly that (a boundary reappearing after its part was finalized),
   with no release-build cost. Note that a genuinely *prefix-determined*
   partitioner cannot trip it under sorted keys — which is the point: the guard
   fires only on the contract violation, and a test drives a deliberately
   non-prefix-determined partitioner to prove it does.

3. **Rules→Derived allowed, Derived→Rules stranding — keep the asymmetry.** It
   is the safe direction. Rules→Derived is write-side-only policy (future
   compactions cut differently; existing parts keep their stamp), exactly like
   adding a live rule. Derived→Rules would strand parts cut on derived
   boundaries and is correctly refused by the persisted marker plus fail-closed
   open. No change.

4. **`bottom_parts` visibility — added, minimally, read-only.** A consumer whose
   partitioner is correctness-driven (a time bucket that must be independently
   droppable) needs to verify the physical cut, and there was no public way.
   `DB::list_partitions` now returns `PartitionInfo { partition, min_key, tier }`
   for the materialized bottom parts, name-sorted. Read-only, no new mutation
   surface, and it reports only bottom-cut parts — a rule declared but not yet
   compacted is honestly absent.

Not breaking: everything added in 0.4.0 is additive (`PartitionInfo`,
`list_partitions`, an internal debug guard). 0.4.0 marks the derived-partitioning
milestone rather than a compatibility break.
