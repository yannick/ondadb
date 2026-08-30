# 0.5 — Vlog value cache

**Readiness:** ready. **Effort:** 2–3 dev-weeks (revised: the block cache needs
a key-domain change — see below). **wavesdb counterpart:** 0.5 (blob value
cache; ondaDB's vlog is per-SSTable so all blob-GC plumbing drops out — only
the admission/caching core transfers).

## Goal

Cache decoded vlog values. Today every access to a separated value performs a
positional read + CRC verify + (v2) decompress: `Reader::read_vlog_into` on
every call. `vlog_verified` memoizes the **checksum**, never the bytes. With
the default `klog_value_threshold = 512` every non-trivial value lives in the
vlog, so hot large values pay disk + decompress forever. On the S3 tier each
uncached read is a range GET, which `S3Metrics.range_gets` makes directly
observable.

## Baseline (verified at 0.8.2)

- `BlockCache` (`cache/block.rs`): sharded CLOCK, `BlockKey { file_id: u64,
  off: u64 }`, values `Arc<[u8]>`. `shard_for(&BlockKey)` hashes
  `file_id.wrapping_mul(1099511628211) ^ off`. Public surface:
  `enabled()`, `get(file_id, off)`, `put(file_id, off, val)`, `stats()`.
- Exactly four call sites, all in `sst/reader.rs::read_data_block`: two `get`,
  two `put`, all keyed on the **klog** block's `h.offset`.
- Vlog read entry points: `read_vlog(off, length) -> Vec<u8>` (point gets,
  `SstIterator::value`) and `read_vlog_into(off, length, out)` (iterator
  `value_into`). `read_vlog_into` tries `read_vlog_from_mmap` first under
  `mmap-reads`, then `read_vlog_from_file`; both verify the CRC frame and
  decompress v2 frames via `append_vlog_payload`.
- `vlog_verified` / `vlog_verified_slot` memoize per-frame CRC verification
  (`VLOG_VERIFIED_SLOTS = 1024`).
- `next_file_id` is a monotonic `fetch_add(1, SeqCst)`, persisted — file ids
  are db-wide-unique and never reused.
- The point-read path knows the logical `val_len` from the klog entry.

### The key domain is NOT free — this is the design's load-bearing correction

A `Reader` owns **two files under one `file_id`**: `klog_path` and
`vlog_path: vlog_path_for(klog_path)`. Their offset spaces are independent and
both start at zero — `Writer` initialises `klog_off: 0, vlog_off: 0`, then
`flush_block` advances `klog_off` by the framed block length while `write_vlog`
advances `vlog_off` by `VLOG_V2_HDR_LEN + stored.len()`.

So for any table with a vlog, `(file_id, 0)` names **both** the first klog data
block and the first vlog frame, with further aliases following. Admitting
decoded vlog values under `(file_id, vlog_off)` would return a data block where
a value is expected and vice versa. A length check on the hit does not rescue
this: it would raise `Corruption` on healthy data and evict live blocks. There
is no "no new key domain needed" version of this feature.

**Adopted fix — a 1-bit domain tag on `BlockKey`:**

```rust
// cache/block.rs
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlockDomain { Klog, Vlog }          // new

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct BlockKey { file_id: u64, off: u64, domain: BlockDomain }   // field new
```

Threaded through `shard_for` (mix the domain into the hash so the two domains
do not correlate their shard placement), `get(file_id, off, domain)`, and
`put(file_id, off, domain, val)`. All four existing call sites pass
`BlockDomain::Klog`; the new vlog admission passes `BlockDomain::Vlog`. Both
domains share one capacity — that is deliberate, and it is exactly what the
"vlog admission evicts klog blocks" risk row and the acceptance gate measure.

The two rejected alternatives, recorded so they are not re-litigated: biasing
vlog offsets by the reader's klog size (fragile — it depends on a size the
cache cannot see and breaks if either file is ever rewritten), and setting the
high bit of `file_id` (silently caps the id space and corrupts the moment
`next_file_id` crosses `1 << 63`).

## Design

- New persisted CF option `max_cached_vlog_value_bytes: usize` — 0 disables
  (the default); a practical starting value is 1 MiB. Wired per the standard
  config checklist: field, `Default`, `validate`, config-blob tag that elides
  the default, reopen test.
- **Lookup**, at the top of `read_vlog_into`, before the mmap attempt: when the
  option is non-zero and `self.bc.enabled()`, try
  `bc.get(file_id, off, BlockDomain::Vlog)`; on a hit, extend `out` from the
  cached `Arc<[u8]>` and return. Checking before the mmap path matters: a v2
  compressed frame is decompressed on every mmap read, so the cache is the only
  thing that removes that cost in the `unsafe-fastpath` config.
- **Admission**, after a complete successful decode (CRC verified,
  decompressed, correct logical length) and only when `decoded.len() <= limit`:
  `bc.put(file_id, off, BlockDomain::Vlog, decoded)`. Never admit a cancelled,
  truncated, or corrupt result. Oversized values bypass without a temporary
  copy.
- The domain tag makes the hit unambiguous, so **no length check is needed for
  correctness**. Keep a `debug_assert_eq!(cached.len(), len)` as a
  cheap invariant tripwire; a mismatch in release is treated as
  `Corruption` and the entry evicted (a new `BlockCache::remove(file_id, off,
  domain)` — **new**).
- Keep `vlog_verified`: it still serves bypassed, evicted, and oversized
  frames, and it is what makes a cache miss cheap on the second read of a
  large value.
- Iterator lifetime contract unchanged: `value_into` extends `out` from an
  immutable `Arc<[u8]>` slice — valid until the next movement, same as today's
  buffered path.
- No singleflight in v1: concurrent misses may decode twice; `put` already
  keeps the existing entry on a duplicate insert, and values are immutable.
- Defensive eviction on `remove_compaction_inputs` / table retire is optional —
  ids never reuse, so it is hygiene, not correctness. Skipped in v1; noted.

## Implementation tasks

Gate for every task: the four-command gate in `../plan.md`.

1. **Key domain.** `cache/block.rs`: add `BlockDomain`, the `domain` field,
   mix it into `shard_for`, and extend `get`/`put` signatures. Update the four
   `sst/reader.rs` call sites to pass `BlockDomain::Klog`. Behavior-neutral.
   Test first: `cache/block.rs::domains_do_not_alias` — `put(7, 0, Klog, a)`
   then `put(7, 0, Vlog, b)`; assert both `get`s return their own value, that
   `stats()` reports two entries, and that neither insert evicted the other at
   ample capacity. Plus `cache/block.rs::shard_for_separates_domains` — over
   many `(file_id, off)` pairs the Klog and Vlog keys do not land in the same
   shard with degenerate frequency (assert a non-trivial spread, not an exact
   distribution).
2. **Regression pin for the aliasing bug.** Test first:
   `tests/sst.rs::klog_block_and_vlog_frame_at_same_offset_do_not_alias` —
   build a table whose first data block and first vlog frame both sit at offset
   0 (any table with a value above `klog_value_threshold` does); read the value
   and then the block, in both orders, in both feature configs, and assert both
   are correct. This test must fail if anyone later drops the domain tag.
3. **Removal primitive.** `cache/block.rs::remove(file_id, off, domain)`
   (**new**) — unlink from `map` and leave the `ring` entry to be reaped by the
   sweep (the sweep already tolerates ring entries with no map entry; if it
   does not, make it, and pin that with a test).
   Test first: `cache/block.rs::remove_drops_entry_and_survives_sweep` —
   insert, remove, force an eviction sweep past capacity, assert no panic and
   correct `used` accounting.
4. **Option wiring.** `config.rs`: `max_cached_vlog_value_bytes`, `Default`
   (0), `validate`, config-blob tag.
   Test first: `tests/db.rs::vlog_value_cache_limit_round_trips_through_reopen`
   and `config.rs::vlog_cache_blob_omits_default`.
5. **Read path.** `sst/reader.rs::read_vlog_into` — lookup before the mmap
   attempt, admission after a successful decode in both
   `read_vlog_from_mmap` and `read_vlog_from_file` (admit at the single join
   point in `read_vlog_into`, not in each branch, so there is one admission
   rule). The limit must reach the `Reader`: thread it through
   `WriterOptions`' sibling reader-open path (the same route
   `klog_value_threshold` takes from `ColumnFamilyConfig` into the reader), or
   store it on the `Reader` at open.
   Test first: `tests/sst.rs::hot_vlog_frame_is_served_from_cache` — read a
   large value twice; with PerfContext (0.10) assert the second read performs
   no vlog read and no decompression (`vlog_reads` unchanged,
   `vlog_cache_hits` +1). Runs in **both** feature configs — under
   `mmap-reads` this is the assertion that the cache is consulted before the
   mmap path.
6. **Admission boundary.** Test first:
   `tests/sst.rs::vlog_admission_respects_limit` — values of exactly
   `limit - 1`, `limit`, and `limit + 1` decoded bytes; the first two are
   admitted, the third is not (assert via `BlockCache::stats()` entry counts);
   and with the limit at 0 nothing is ever admitted.
7. **Corruption never admitted.** Test first:
   `tests/sst.rs::corrupt_vlog_frame_is_not_admitted` — corrupt a frame (the
   `corrupt_vlog_value_is_detected` pattern), read → `Err(Corruption)`, assert
   the cache holds no entry for it; repair the file, reopen, read → `Ok` and
   now admitted.
8. **Concurrency.** Test first:
   `tests/sst.rs::concurrent_vlog_misses_both_return_correct_bytes` — two
   threads read the same cold frame; both get correct bytes, the cache ends
   with exactly one entry.
9. **Legacy frames.** Test first:
   `tests/sst.rs::legacy_v1_vlog_frames_are_cached` — a table written with
   `vlog_v2 == false` caches and serves identically.
10. **PerfContext counter.** 0.10's context gains `vlog_cache_hits` (**new**)
    beside the existing `vlog_reads` / `vlog_read_bytes`.
11. **S3 assertion.** Test first (env-gated on `ONDADB_S3_ENDPOINT`, the
    `tests/s3_tier.rs` pattern):
    `tests/s3_tier.rs::warm_vlog_value_issues_no_range_get` — read a large
    value on an S3-resident part twice; assert `S3Metrics.range_gets` is
    unchanged by the second read.
12. **Harness + klog-hit-rate publication.** Hot/cold large-value point-read
    phase; publish vlog cache hit rate, hot-frame latency, and the klog block
    hit-rate delta.

## Tests (summary)

- Domain separation and the offset-aliasing regression pin (both configs).
- Two reads of a hot frame: second does no read/decompress; on S3 no range GET.
- Admission boundary at `limit ± 1`; disabled cache admits nothing.
- Corrupt first read never admitted; corrected re-read works and is admitted.
- Concurrent misses; legacy v1 frames; `unsafe-fastpath` mmap path caches
  identically.
- `BlockCache::remove` accounting survives an eviction sweep.

## Acceptance

Hot/cold large-value point-read phase: hot-frame latency improves beyond the
≥5-run baseline spread. **The klog block hit-rate loss must be published, and
it is measured on the default build only** — under `mmap-reads` uncompressed
klog blocks are served straight from the mmap by `read_data_block_local` and
never enter the block cache, so a klog hit-rate delta measured there is
meaningless. If admission materially degrades ordinary point reads at the
recommended limit, the default stays 0 (it already does).

## Rollback

Set the option to 0; existing entries age out via CLOCK. The `BlockDomain`
tag stays — it is a correctness fix, not part of the feature's opt-in surface.
No disk state changes.
