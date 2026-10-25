# SSTable index search over 8-byte key prefixes

Baseline **ondaDB 0.20.x** (`22fe48b`). An in-memory change to one function,
`Reader::find_block`. **No format change**: no byte on disk, no capability
bit, no registry number moves. It came out of the bbtree B+tree analysis, and
an A/B experiment (below) cleared the bar set before it was run.

| | |
| --- | --- |
| Readiness | measured, design ready |
| Effort | 1–2 days including tests |
| Risk | low. Read path only; old behaviour is one branch away |
| Invariants touched | 7 (prefix compare tricks), 8 (pinned blocks: not affected, see below) |

## Motivation

`Reader::find_block` (`src/sst/reader.rs`) binary-searches `Vec<IndexEntry>`.
`IndexEntry::user_key` is a heap `Vec<u8>`, so each of the ~15 steps loads the
48-byte entry, follows a pointer to the key's heap allocation, and calls the
comparator through `dyn Comparator` (`cmp_internal`). Once the index is no
longer cache-resident, each step costs up to two dependent cache misses. When it
is resident, the virtual call and `memcmp` still cost ~14 ns per step.

The change keeps a parallel `Vec<u64>` of `key_prefix8(separator)` and
binary-searches that instead. A step whose prefix differs from the probe's is
decided by one integer compare, with no pointer chase and no virtual call.
Only an equal prefix falls back to the full `cmp_internal`.

### Measured (2026-10-25, Apple M2 Max: 16 MiB L2 per P-cluster, ~48 MiB SLC)

Method: `docs/performance.md`. Both arms ran in **one process over one warm
database**, switched at runtime by a test-only atomic. Arm order alternated
every trial, 9 trials per row, and the release build used `--features
unsafe-fastpath` unless the row says otherwise. Ranges are min–max over the
trials. Keys were 16 bytes with the first 8 spread uniformly over the whole
u64 range plus a random tail (loaded in sorted order, so compaction did not
rewrite them). Probes were random keys known to be present. Values were
16 bytes and `data_block_size` 512. The small block size was chosen to make
the index large without a 50 GB database.

**End-to-end `DB::get`, ns/get:**

| Case | Data | Legacy p50 [min–max] | Prefix p50 [min–max] | Δ p50 |
| --- | --- | ---: | ---: | ---: |
| (a) cache-resident: 400k keys, 1 table, ~2 MiB index | 16.7 MB | 571.4 [557.9–591.6] | 444.4 [422.3–491.0] | **−22.2%** |
| (a) same, default features (no mmap) | 16.7 MB | 742.2 [729.4–823.3] | 621.3 [595.0–690.8] | −16.3% |
| (b) cold: 80M keys, 153 tables in L2, ~240 MiB index¹ | 1.92 GB | 3609.0 [3549.0–3708.0] | 2595.2 [2518.3–2704.2] | **−28.1%** |
| 1 KiB keys sharing a 20-byte prefix, 200k keys, 13 tables, block 4096 | 212 MB | 1233.6 [1217.5–1256.7] | 1234.6 [1202.3–1275.8] | +0.1% |

¹ Estimated as ~3.75M data blocks × (48-byte `IndexEntry` + 16-byte key
allocation). That is about 5× the SLC. The prefix array for it is ~30 MiB.

**`find_block` alone, ns/call** (in-crate microbench over many `Reader`s, one
separator per block, random present keys):

| Case | Legacy p50 [min–max] | Prefix p50 [min–max] | Δ |
| --- | ---: | ---: | ---: |
| 16 tables, 16k entries, ~1 MiB index | 144.5 [143.4–154.6] | 30.1 [29.8–31.3] | −79% |
| 800 tables, 6.55M entries, ~400 MiB index | 1592.9 [1515.3–1838.1] | 382.9 [369.8–400.2] | −76% |
| 1 KiB shared-prefix keys, **no all-tied gate** | 210.0 [206.9–215.7] | 222.3 [217.5–230.2] | +5.8% |

**Mechanism counter** (full `cmp_internal` calls per `find_block`): legacy
14.85 (case a), 15.37 (case b), 10.0–13.0 (microbench). Prefix: **0.00** in
every 16-byte case. That is lower than the ~1–2 expected, because the writer
shortens separators (`Writer::settle_pending_index`), so a probe almost never
shares a separator's full 8-byte prefix. With 1 KiB shared-prefix keys both arms
make 11.94: every prefix ties. The last microbench row shows that the tie path
then costs about 6% more than today. That result led to the **all-tied gate**
below, and with the gate the end-to-end row is +0.1%.

Decision rule set before measuring: case (b) p50 must improve by ≥10% with
min/max ranges that do not overlap, and case (a) must not regress by more than
3%. Result: case (b) improved by 28.1% and the ranges do not overlap (prefix
max 2704 < legacy min 3549). Case (a) improved by 22.2%. **Passed.**

## Design

### Data layout

```rust
pub struct Reader {
    pub(crate) index: Vec<IndexEntry>,
    /// `key_prefix8(index[i].user_key)` for every i, or empty when the
    /// prefix order is not the comparator's order or would decide nothing.
    index_prefix: Vec<u64>,
    ...
}
```

The array is built once in `Reader::open_with_limiter`, right after the
`load_btree`/`decode_index` branch, so it covers the flat index and the hybrid
B+tree klog the same way. It is allocated with `Vec::with_capacity(index.len())`
and filled with one pass of `sst::key_prefix8` (the existing zero-padded
big-endian packing, `src/sst/iter.rs`). It is immutable after open, like
`index`, so no lock or cache interaction is needed.

### Gating: invariant 7

The array is built only when **both** of these hold:

1. `self.bytewise` (= `cmp.is_bytewise()`, already resolved at open). For a
   non-bytewise comparator the prefix order says nothing about the comparator
   order. `index_prefix` stays empty and `find_block` takes today's loop
   unchanged.
2. `prefix[0] != prefix[last]` (the **all-tied gate**). If the first and last
   separators share 8 bytes, the array is sorted, so *every* entry shares them.
   Every step would tie, and the array would only add a load per step (+5.8%
   measured). The array is left empty and the table searches as today.

### Search and tie resolution

```rust
let pre = key_prefix8(user_key);
while lo < hi {
    let mid = (lo + hi) / 2;
    let p = self.index_prefix[mid];
    let lt = if p != pre {
        p < pre                       // strict prefix order decides
    } else {
        let e = &self.index[mid];     // tie: full internal-key order
        cmp_internal(&self.cmp, &e.user_key, e.seq, user_key, seq).is_lt()
    };
    if lt { lo = mid + 1 } else { hi = mid }
}
```

Why this is exact under a bytewise comparator: zero-padding is monotone. If
`prefix(a) < prefix(b)` then `a < b` bytewise, and if `prefix(a) > prefix(b)`
then `a > b`. Separator `(k, s)` is `< (user_key, seq)` in internal order
exactly when `k < user_key`, or when `k == user_key` and `s > seq`. A strict
prefix inequality decides `k` vs `user_key`, so the seq never matters there.
Equal prefixes, which include `k == user_key`, separators that differ only by
seq, and `"a"` vs `"a\0"` (both pad to the same word), always fall through to
the full comparison, as rule 7 requires. The loop is one binary search with a
per-step fallback, not a two-phase "prefix range, then full search": it does the
same number of steps and needs no extra bounds bookkeeping.

`perf::bump(index_seeks)` stays at the top. `find_block`'s contract (*the first
block whose separator is `>= (user_key, seq)`*) is unchanged, so its callers
need no change: `get_unfiltered` / `get_unfiltered_into`, the batch point-read
planner in `column_family.rs`, and `SstIterator::seek`.

### Memory cost

8 bytes per index entry, i.e. per data block. Per table: `8 × data_block_count`
bytes. At the 4 KiB default block size that is ~0.2% of data size, or 2 KiB per
1,000 blocks. Against today's ~64 B per entry (48-byte `IndexEntry` plus a
≥16-byte key allocation) it is +12.5% of index memory. The 48 GiB / 14,051-table
example in `resident_bytes`'s doc (~12M entries) gains ~92 MiB. In the
experiment's case (b) the array is ~30 MiB against ~240 MiB of index.
**`resident_bytes` and `resident_breakdown` must count it**
(`index_prefix.capacity() * 8`), so `max_open_reader_bytes` and the
table-cache budget see it.

### Interactions

- **Block cache / mmap:** none. `find_block` returns an index into `index`, and
  which block is then read (mmap `read_data_block_local` or the cached `pread`
  path) is unchanged. Checksum verification (invariant 4) happens at the block
  read, which the change does not touch.
- **Table cache:** the array lives and dies with the `Reader`. Evicting and
  reopening a table rebuilds it, which costs the same order as the existing
  `decode_index` key copies.
- **Pinned blocks (invariant 8):** not involved. The merge iterator's pins live
  in blocks, and the change touches only the in-memory index.
- **`FormatProfile::Waves08` and `Onda09` readers:** both go through the same
  `Reader::open_with_limiter`, after their index is decoded, so they get the
  array under the same gates. Correctness depends only on `index` being sorted
  under the table's comparator, which every profile already requires
  (`find_block` assumes it today). `waves08/block.rs`'s separator test
  (`assert_eq!(r.find_block(&key, d.seq), i)`) exercises it, as do
  `tests/wavesdb_epoch1_fixtures.rs` and `tests/legacy_onda.rs`.
- **Non-bytewise comparators** (`ReverseBytewise`, `be_u64`-style, user
  registered): array empty, behaviour byte-identical to today.

## Test plan

1. **Property test** (`src/sst/reader.rs` tests, or a sibling `#[cfg(test)]`
   module) comparing `find_block` against a reference
   `index.partition_point(|e| cmp_internal(..).is_lt())`. Run it over several
   table shapes at block sizes 64, 256 and 1024:
   - random 16-byte keys;
   - long keys sharing a 20-byte prefix (all tied: asserts the array is
     **not** built);
   - the same with keys differing inside the first 20 bytes mixed in (ties
     with an array present);
   - short keys of 0–10 bytes drawn from `{0x00, 0x01, 'a', 0xff}`
     (zero-padding ties);
   - a few user keys with 60 versions each (separators equal by user key,
     differing by seq).

   Probes: each separator at `seq ∈ {0, s−1, s, s+1, u64::MAX}`, the separator
   with a trailing `0x00` byte, the separator truncated by one byte, every third
   stored `(key, seq)`, 2,000 random keys of 0–29 bytes, `""` and `0xff×40`.
   Also add a `ReverseBytewise` table asserting `index_prefix.is_empty()` and
   unchanged results. The experiment's version of this test passed in both
   default and `unsafe-fastpath` builds.
2. **Existing suites** in all three feature configurations plus clippy: the
   seven-command gate in `AGENTS.md`. The waves08 and legacy-onda fixture tests
   are the cross-profile check.
3. **Perf test kept `#[ignore]`.** Add `tests/perf_sst_index.rs` in the style of
   `tests/perf_nilpath.rs`, minimal: environment-sized key count, block size and
   long-key switch, then sorted load, `compact`, interleaved arms, and a printed
   p50 and min/max. It needs a `#[doc(hidden)] pub` runtime arm switch. Keep
   that switch only if the lead accepts one relaxed atomic load per
   `find_block`, which is unmeasurable next to the work above. Otherwise drop
   both the switch and the perf test, and record the numbers above in
   `docs/performance.md`. Do **not** ship the experiment's comparison
   counters: they were an untimed mechanism check.
4. Add a row to `docs/performance.md` § fast paths ("index search compares
   8-byte prefixes, bytewise only, all-tied tables excluded") so a future
   change does not undo it unknowingly.

## Follow-ups (not in this change)

- **Skip the shared separator prefix for long keys.** Compute the common
  prefix length `c` of the first and last separators. Every separator shares
  it, so store `key_prefix8(sep[c..])` instead. The search would compare
  `user_key[..c]` against the shared prefix once, then use the prefix array.
  This turns the 1 KiB / 20-byte-shared case from "gated off" into a win. It
  costs a per-table `c` and a probe-side slice. Measure it separately.
- **Eytzinger (BFS) layout** of the prefix array, for prefetch-friendly
  descent in case (b). At ~4 MiB of prefixes per large table, the remaining
  ~380 ns/`find_block` is mostly the ~15 dependent misses that are left.
- Shrinking `IndexEntry` itself (one arena of separator bytes plus offsets
  instead of a `Vec<u8>` per entry) would also reduce the tie path's cost and
  the resident memory that `resident_bytes` already reports.

## Rollback

Remove the field and the branch. `find_block` goes back to the plain loop.
Nothing on disk, in the manifest or in any other process depends on it, so
rollback has no compatibility consequence and needs no upgrade step.
