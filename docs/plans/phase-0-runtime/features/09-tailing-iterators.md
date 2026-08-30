# 0.9 — Keyspace-tailing iterator

**Readiness:** ready, deliberately narrow semantics. **Effort:** 1–2
dev-weeks. **wavesdb counterpart:** 0.9 (semantics correction adopted
verbatim).

## Goal

Avoid reconstructing an iterator per poll of an append-only ordered keyspace —
ondaDB's documented queue-peek workload (`memtable.rs` header). **This is not
CDC**: a refreshed tail may observe only keys that compare strictly *greater*
than its last yielded key. A later insert, update, or delete **at or behind**
the cursor is never observed; already-yielded keys are never re-yielded. The
iterator-construction cost that motivated `LazyMemIter` (1.3 ms per scan at 2k
entries) is exactly what this avoids on the tail path.

## Baseline (verified at 0.8.2)

- **There is no `DB`-level iterator API.** `Txn::new_iterator(cf)` and
  `Txn::new_iterator_bounded(cf, lower, upper)` are the only public entry
  points; `new_iterator_bounded` is a `Txn` method, not a `DB` one.
- The primitive to build on is
  `ColumnFamily::new_iterator(read_seq: u64, extra: Option<Arc<Memtable>>,
  bounds: (Bound<&[u8]>, Bound<&[u8]>)) -> Iterator` — `pub(crate)`, already
  taking exactly the bounds shape this feature needs. `Txn` passes its buffered
  writes as `extra`; a DB-level tail passes **`None`**, which is the
  read-only-transaction path `new_iterator_bounded` already takes when
  `self.writes.is_empty()`.
- `Iterator` is snapshot-fixed at the `read_seq` it was constructed with, owns
  `lower`/`upper` as `Bound<Vec<u8>>`, and terminates at the declared bounds
  (`past_upper` / `below_lower`). There is no refresh mechanism.
- `DbInner::read_floor_seq()` is `visible_seq().max(own_commit_floor())` — the
  read-committed floor, including this thread's own writes.
- `ColumnFamily::new_iterator` calls `coarse_now_nanos()` itself and passes it
  to `Iterator::new`.
- `Txn` fixed-snapshot levels must not silently refresh — hence DB-level only.

## Design

```rust
// new type, tailing.rs
impl DB {
    pub fn new_tailing_iterator(&self, cf: &Arc<ColumnFamily>) -> TailingIterator;
}
pub struct TailingIterator {                // new
    /* one Iterator at a time, plus the CF handle and the last yielded key */
}
impl TailingIterator {
    pub fn seek_to_first(&mut self);
    pub fn seek(&mut self, key: &[u8]);
    pub fn next(&mut self);
    /// Non-blocking. Rebuilds only when the current segment is exhausted AND
    /// the visible floor advanced; the new segment starts strictly after the
    /// last yielded key. Returns whether the new segment is immediately valid.
    pub fn refresh(&mut self) -> bool;
    pub fn valid(&self) -> bool;
    pub fn key(&self) -> &[u8];
    pub fn value(&self) -> &[u8];
    pub fn err(&self) -> Option<&OndaError>;
}
```

- Read sequence per segment: `DbInner::read_floor_seq()` — the read-committed
  floor, **not** a pinned snapshot. Refreshing a fixed snapshot is exactly what
  must not happen.
- `refresh` copies the last yielded key into an owned `Vec<u8>`, drops the old
  `Iterator`, captures a fresh `read_floor_seq()`, and calls
  `cf.new_iterator(floor, None, (Bound::Excluded(&last), Bound::Unbounded))`,
  then `seek_to_first()` on the result. If nothing has been yielded yet the
  lower bound is `Bound::Unbounded`.
- **`now` comes from the constructor.** `ColumnFamily::new_iterator` already
  calls `coarse_now_nanos()` internally and hands it to `Iterator::new`; the
  tail accepts that and does **not** pass its own. No signature change to
  `new_iterator`, and TTL expiry is evaluated per segment against a fresh
  clock read — which is the behavior a long-lived tail wants.
- `refresh` returns `false` without rebuilding when the current segment is
  still valid (mid-iteration) or when the floor has not advanced. Both are
  cheap: one atomic load and one `valid()` check.
- No `prev`, no `seek_for_prev`. The type simply does not expose them, so
  backward use is a compile error rather than a runtime surprise — the shape
  wavesdb's correction settled on.
- `err()` surfaces the underlying `Iterator::err()`, including the
  `Iterator::failed(..)` construction path (`new_iterator` returns a failed
  iterator rather than omitting a table it could not open).

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

1. **Type skeleton.** `src/tailing.rs` (**new**), exported from `lib.rs`:
   `TailingIterator` owning `Arc<ColumnFamily>`, `Arc<DbInner>`, the current
   `Iterator`, the last yielded key (`Vec<u8>`), and the floor the current
   segment was built at. `DB::new_tailing_iterator` builds the first segment
   with `(Bound::Unbounded, Bound::Unbounded)`.
   Test first: `tests/db.rs::tailing_iterator_walks_a_static_keyspace` — over a
   CF that is not being written, `seek_to_first` + repeated `next` yields
   exactly the same key/value sequence as `Txn::new_iterator`, and `refresh`
   at exhaustion returns `false`.
2. **Cursor tracking.** `next`/`seek` record the yielded key.
   Test first: `tailing.rs::cursor_tracks_last_yielded_key` — after N `next`
   calls the recorded cursor equals the last key; after an exhausting `next` it
   is unchanged (exhaustion does not clear the cursor).
3. **`refresh`.** Floor check, rebuild with `Bound::Excluded(last)`, seek.
   Test first: `tests/db.rs::refresh_yields_only_strictly_greater_keys` —
   append keys concurrently while tailing; assert the full observed sequence is
   strictly increasing, contains no duplicates, and eventually contains every
   key appended after the tail started.
   Plus `tests/db.rs::refresh_is_noop_mid_segment` — with unread entries
   remaining, `refresh` returns `false` and the next `next()` continues the
   same segment (assert the segment was not rebuilt via a
   `#[cfg(test)]` rebuild counter).
   Plus `tests/db.rs::refresh_is_noop_when_floor_unchanged` — no writes since
   the last rebuild → `false`, no rebuild.
4. **Non-CDC contract (negative tests).** No new code; these pin the semantics.
   Test first: `tests/db.rs::tail_never_observes_updates_behind_the_cursor` —
   yield key `k`, then update and then delete `k`; assert no subsequent
   `refresh`/`next` ever yields `k` again, in either state.
   Plus `tests/db.rs::tail_never_observes_inserts_behind_the_cursor` — yield
   `k5`, insert `k3`, assert `k3` is invisible to this tail but visible to a
   freshly constructed iterator.
5. **Error propagation.** Test first:
   `tests/db.rs::tail_surfaces_iterator_construction_failure` — make a table
   unopenable (the corruption-test pattern), assert `valid() == false` and
   `err()` is `Some` after the rebuild, and that the tail does not silently
   return a short answer.
6. **Unified mode.** Test first:
   `tests/unified.rs::tailing_iterator_works_in_unified_mode` — same
   append-and-tail assertions with the unified memtable layout. Document the
   known cost: the unified overlay path rebuilds per segment; accept it for v1
   and do not fix it here.
7. **Both configs + docs.** Run the gate; document the non-CDC contract on
   `TailingIterator` itself and in `docs/architecture.md`'s iterator section,
   including the "not a change feed" sentence verbatim.
8. **Harness.** Queue-peek phase: tail-with-refresh versus rebuild-per-poll,
   ≥5 runs, publishing iterator constructions per yielded entry.

## Tests (summary)

- Static-keyspace equivalence with `Txn::new_iterator`.
- Append-only + concurrent writer: strictly greater, in order, no duplicates,
  eventually complete.
- Negative tests pinning the non-CDC contract (update, delete, behind-cursor
  insert).
- `refresh` is a no-op mid-segment and when the floor has not advanced.
- Construction failure surfaces through `err()`.
- Unified mode; both feature configs.

## Acceptance

Queue-peek phase: iterator construction amortizes to roughly one rebuild per
refreshed batch rather than one per poll, and throughput improves beyond the
≥5-run baseline spread versus rebuild-per-poll.

## Rollback

New type and one new `DB` method; delete both. Nothing else changes —
`ColumnFamily::new_iterator` is used exactly as `Txn` already uses it.
