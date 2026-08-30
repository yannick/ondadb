# 2.1 prefix-delta data blocks — acceptance measurement and default decision

**Decision: the option stays opt-in (`enable_prefix_delta_keys = false`).**
The sweep is not decisive. It is a clear space-for-CPU trade that is a large win
on prefix-heavy keys and a pure loss on random ones, and a default has to serve
both.

## How this was measured

`tests/prefix_delta_bench.rs::prefix_delta_interval_by_block_size_sweep`
(`#[ignore]`d), `cargo test --release --features unsafe-fastpath`, 5 runs per
cell, `block_restart_interval ∈ {4, 8, 16, 32}` crossed with `data_block_size ∈
{4, 8, 16, 64} KiB`, 120,000 entries, 24-byte inline values, **lz4** blocks (the
honest baseline: the hypothesis is that block compression already recovers most
of the redundancy on disk).

Every table is written twice from the same entries — legacy and delta — and every
number below is the **same-run ratio** delta/legacy, median of 5. Per AGENTS.md
this machine is thermally noisy (±15–20 % run to run), so absolutes are in
`sweep.csv` and are not to be compared across sessions.

- Cache-residency bytes = the sum of `raw_len` over the file's data blocks, i.e.
  what a fully-resident decompressed block cache would hold.
- Decode CPU per scanned entry = a **warm** full scan (cache pre-warmed) divided
  by entries, forward and reverse timed separately.
- Reverse p99 = per-`prev()` step latency, published unconditionally.
- Merge scan = the public iterator over four overlapping L0 tables, so every
  group runs `capture_group_key`: legacy children serve a **pinned** key, delta
  children a **buffered** copy. This is the end-to-end number.

## At the 4 KiB default, interval 8

| metric | prefix-heavy | random |
|---|---:|---:|
| decompressed cache bytes | **0.53×** (8.81 → 4.70 MB) | 1.010× |
| on-disk data bytes (lz4) | **0.75×** (1.32 → 0.99 MB) | 0.986× |
| index-block bytes | **0.52×** (110.8 → 57.4 KB) | 1.017× |
| data blocks per table | **0.53×** (2138 → 1139) | 1.016× |
| forward decode CPU / entry | 1.46× (24.0 → 34.9 ns) | 2.22× (19.4 → 38.3 ns) |
| reverse decode CPU / entry | **0.92×** (114.5 → 105.5 ns) | 1.46× (65.1 → 92.0 ns) |
| reverse-scan p99 | **708 → 375 ns** | 750 → 417 ns |
| merge-scan wall time | **1.152×** | **1.137×** |

Full grid in `sweep-summary.md`; raw per-run rows in `sweep.csv`.

## What the sweep actually shows

**The size hypothesis holds, and is bigger than the doc's pessimistic case.**
On prefix-heavy keys the decompressed footprint halves, and — the part the
argument-only version of this got wrong — so does the *index*, because denser
entries mean **fewer** blocks per table at a fixed `data_block_size`, not more.
lz4 does not already recover it: on-disk data still drops 25 % at interval 8 and
37 % at interval 32.

**Interval dominates block size.** Every size ratio moves monotonically with
`block_restart_interval` (0.60× → 0.47× decompressed from 4 to 32) and is flat
across `data_block_size` to three decimals. The anchor fraction is `1/interval`
regardless of block size, exactly as predicted; the cross with `data_block_size`
was worth running and its answer is "no interaction worth tuning for".

**The random-key floor cost is real but small in bytes**: +1.0 % decompressed,
and on-disk actually −1.4 % (the extra `shared_len` uvarint is nearly always 0 and
lz4 eats it). That half of the acceptance criterion passes.

**Decode CPU is where it fails.** The prefix-heavy acceptance line asks for
decode CPU per scanned entry to *drop*; it rises 1.46× forward. Reverse improves
(0.92×) and reverse p99 nearly halves at 4 KiB — legacy `prev` pays a full-block
offset rebuild on every block underflow, and the run cursor is cheaper than that
— but forward is the common case. On random keys forward decode roughly doubles
for no size benefit at all.

**The buffered-key cost is 14–15 %, and is reported, not absorbed.** A delta
child cannot serve `key_block_ref` (a delta key exists contiguously nowhere in
the block, AGENTS.md invariant 8), so the merge iterator copies it into
`CurKey::Buffered`. End to end that is +15 % on a four-child merge scan — the
same class of change as the per-entry `Arc` clone that once cost 3× here, and an
order of magnitude smaller, but not free. The 1.5–2.2× per-entry decode ratios
above are a *pure decode loop* and overstate what a real workload sees; 1.14× is
the number to quote.

**Reverse p99 degrades with interval, not with block size**: 375 ns at interval
8 against 916 ns at interval 32 (legacy 708–750 ns throughout). A long run costs
a long materialization on every backward block crossing. Anyone enabling the
option for reverse-heavy scans should stay at interval 8 or below.

## Recommendation

- Default: **off**. Unchanged.
- Worth enabling: prefix-heavy keyspaces (the `tenant/cluster/segment` shape)
  where decompressed cache residency or index memory is the binding constraint —
  it buys ~2× the tables per byte of block cache and halves index residency, the
  dominant per-reader memory term.
- Not worth enabling: random or already-short keys, and forward-scan-bound
  workloads.
- If enabled, `block_restart_interval = 8` is the balanced point: 16 and 32 buy
  another 4–8 % of size and cost reverse p99 2.4×.
