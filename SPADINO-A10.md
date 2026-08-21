# A10 — Configurable data block size

For the ondaDB owner. Requested by spadino. Small: one `ColumnFamilyConfig`
field and two call sites. The case is measured, not argued — the numbers are
`spadino/docs/benchmarks/2026-08-density.md`, section "The real corpus".

## The ask

`ColumnFamilyConfig` cannot set the SSTable data block size. It is a private
constant:

```rust
// src/column_family.rs:29
const DATA_BLOCK_SIZE: usize = 4 << 10;
```

used at exactly two places — the flush/ingest writer
(`ColumnFamily::writer_opts`, `block_size: DATA_BLOCK_SIZE`) and the compaction
writer (`compaction.rs::cf_writer_opts`, which repeats the literal
`block_size: 4 << 10`). `WriterOptions::block_size` already exists and is
already honoured; nothing else is missing.

```rust
pub struct ColumnFamilyConfig {
    ...
    /// Target size of an SSTable data block. The writer cuts a block once it
    /// *exceeds* this (`sst/writer.rs:285`), so a single value larger than the
    /// target gets a block to itself.
    pub data_block_size: usize,   // default: 4 << 10, i.e. today's behaviour
}
```

Both writer sites read `cf.opts.data_block_size`. Validation: reject 0 in
`ColumnFamilyConfig::validate` (the writer's own `0 → DEFAULT_BLOCK_SIZE`
fallback would otherwise silently ignore the setting).

Purely a **write-side policy**, like `compression_rules` and `partition_rules`:
blocks are self-describing and carry their own length, so changing the field
rewrites nothing and existing tables keep whatever they were written with. No
format bump, no migration, no manifest change.

## Why spadino wants it

Every value spadino stores is larger than 4 KiB or close to it, so the block
target is not a target — it is a guarantee of **one value per block**, and
therefore of a compression window of one value. A 5.6 KB document fills a block
by itself. So does a position lane.

Measured on 99,996 real news articles (434.8 MB of text), zstd, one arm per
block size, everything else identical:

| lane | 4 KiB | 16 KiB | 64 KiB | 256 KiB |
|---|---|---|---|---|
| d_doc | 184.1 MB | 172.5 | 161.3 | 149.4 |
| t_pos | 169.0 MB | 151.8 | 147.6 | 145.7 |
| t_post | 118.5 MB | 104.9 | 104.5 | 104.1 |
| s_struct | 47.7 MB | 43.7 | 41.3 | 38.7 |
| s_pass | 32.7 MB | 28.6 | 27.7 | 27.2 |
| **whole store** | **562.0 MB** | **509.5** | **490.2** | **472.9** |
| | — | **−9.3%** | **−12.8%** | −15.9% |

It is not one lane's problem. A 4 KiB window is too small for everything in
this store, and the effect is 11–15% on each of the five lanes that matter.

The write path gets **faster**, not slower — fewer, larger compression calls:

| | 4 KiB | 64 KiB |
|---|---|---|
| seal (5 × 20k-doc segments) | 39.7 s | 35.9 s |
| full compaction | 23.8 s | 15.8 s |

The read cost is real but small, and it is the reason this is a **per-family**
field rather than a global default change. Same store, 50 reps per term,
`bm25pff`, warm, local NVMe:

| term | df | p50 @ 4 KiB | p50 @ 64 KiB |
|---|---|---|---|
| die | 14,926 | 10,577 µs | 10,870 µs (+2.8%) |
| polizei | 1,365 | 509 µs | 546 µs (+7.3%) |
| berlin | 604 | 659 µs | 667 µs (+1.2%) |
| klimawandel | 126 | 155 µs | 158 µs (+1.9%) |

A rare term reads few blocks, so a wider block is decompression it did not
need. Per-family is what lets a consumer take the bytes on the lanes it scans
and keep 4 KiB on the lanes it point-reads — which is precisely what a global
constant forbids today.

## What it does not need

No new object, no new key, no addressing change, no interaction with
`klog_value_threshold` (these values are already inline — spadino raises the
threshold to 16 KiB deliberately, so the block cache covers them), and no
interaction with `compression_rules` beyond the existing rule that the writer
cuts a block early when the next key's rule differs.

## Suggested acceptance

- `ColumnFamilyConfig::data_block_size` defaults to `4 << 10`; an existing
  database that never sets it writes byte-identical tables.
- `validate()` rejects 0.
- Both writer sites read it; the literal in `compaction.rs` is gone.
- A test that writes the same keys at two block sizes and asserts the reader
  returns identical values from both (blocks are self-describing, so a table
  written at 64 KiB must open in a database configured for 4 KiB).
