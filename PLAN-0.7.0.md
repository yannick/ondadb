# ondadb 0.7.0 — the value log grows a cache, and the defaults stop lying

> **Framing.** Every item here was found by profiling spada, ondadb's primary
> consumer, against a real 20,000-document corpus. None of it is speculative
> tuning. Three are outright bugs; the rest are defaults calibrated for a
> workload ondadb does not actually serve.
>
> The owner has explicitly waived the additive-only constraint (D-191) for this
> release: **defaults may change.** That makes this a behaviour-affecting minor
> release, and the changelog must say so at the top rather than bury it.

---

## The finding that motivates the release

WiscKey value separation exists to cut write amplification: an LSM rewrites
data as it merges levels, so keeping large values out of the merge path saves
copying them repeatedly. ondadb implements the separation
(`sst/writer.rs:205`) with a 512-byte default (`config.rs:465`).

**ondadb does not realise the payoff.** Compaction merge-iterates with
`SstIterator` and writes each entry into a fresh SSTable, and
`SstIterator::value()` (`sst/iter.rs:295-298`) does:

```rust
if e.has_vlog() { self.r.read_vlog(e.vlog_off, e.val_len as u64) }
```

It reads the value back out of the vlog and hands an owned `Vec` to the writer,
which writes it again. **Compaction fully rewrites vlog values.** The write
amplification WiscKey separation is supposed to avoid is paid anyway.

Meanwhile the cost *is* real. `read_vlog_into` (`sst/reader.rs:544`) contains
no cache lookup; the block cache covers **data blocks only** (`reader.rs:20`).
So every vlog access is a `pread` plus a CRC32 over the whole value, every
time, forever — with no cache in front of it at any size.

So today's separation trades a benefit ondadb doesn't deliver for a cost it
does. Two things follow, and **item 1 matters far more than item 2**:

1. **Cache vlog reads.** Removes the cliff for every caller at any threshold.
2. **Raise the threshold**, because 512 is aggressive even given a cache.

Measured consequence in spada: a ~10 KiB position lane re-read and
re-checksummed once per scored document. Raising the threshold at the caller
gave **1.5× on high-frequency queries** with no other change (spada S-109).

---

## 1. Vlog value cache — the headline

**Problem.** `Reader::read_vlog_into` (`sst/reader.rs:544-625`) goes straight
to mmap or a pair of `pread`s plus `checksum()`, with no cache consulted at any
point. A value read a thousand times is read from the filesystem a thousand
times and checksummed a thousand times.

**Design.** Reuse `cache::block::BlockCache` unchanged in structure — it is
already the right shape: sharded, byte-bounded, CLOCK eviction, `Arc<[u8]>`
values, and **non-serializing reads** (a hit takes the shard `RwLock` in read
mode and sets an atomic bit). Its own module doc records why that matters:

> *"the previous `Mutex<LruCache>` made every cache hit take an exclusive lock,
> which showed up as reader serialization on point-read-heavy multi-threaded
> workloads"*

That property is exactly what a vlog cache needs.

**Key collision is the one real design question.** `BlockKey` is
`(file_id, off)` (`cache/block.rs:23-26`), and a klog block offset and a vlog
frame offset can collide for the same `file_id`. Options:

- **(a)** Add a `kind: u8` discriminator to `BlockKey`. One byte, one cache,
  one budget, shared eviction pressure across both populations.
- **(b)** A second `BlockCache` instance with its own byte budget.

**Recommend (a).** One budget is easier to reason about and lets a
value-heavy workload spend the cache where it needs it, rather than stranding
half of it. It also avoids a second config knob.

**Store decompressed, post-CRC bytes.** The CRC and any decompression then
happen once per value, not once per read — which is most of the win, not just
the I/O.

**Scope.** Add a `cache_vlog_values: bool` (default `true`) so a caller with a
genuinely read-once large-value workload can opt out rather than have its cache
evicted by values it will never re-read.

**Expected effect.** In spada this removes the entire `read_vlog_into` +
`crc32fast` cost under `SegmentReader::positions` — ~203 profile samples out of
a `positions` node that is 63% of query time. It also makes item 2 much less
load-bearing.

---

## 2. Default changes

The waiver applies here. Each row says whether the number is **measured** or
**needs a sweep before it lands** — I will not ship an unmeasured default.

| Setting | Current | Proposed | Basis |
|---|---|---|---|
| `klog_value_threshold` | 512 (`config.rs:465`) | **4096** | Judgement + spada's 1.5× at 16 KiB. **Sweep 512/1k/4k/16k/64k required.** |
| `block_cache_size` | 64 MiB (`config.rs:344`) | **256 MiB** | ondadb's own `BENCHMARK-RESULTS.md` uses 512 MiB. 64 MiB is not a serious default for a storage engine; 512 MiB is too much to impose. **Needs an owner decision, not a measurement.** |
| `max_open_sstables` | 256 (`config.rs:345`) | **1024** | Straight bug-adjacent: spada had 308 SSTables against a 256 cap, so the file cache thrashed. Any default below a realistic table count is wrong. |
| `compression` | `None` (`config.rs:466`) | **unchanged**; add `compression_per_level` guidance | Changing this default alters every consumer's disk format *and* CPU profile. Recommend documenting `[None, Lz4, Zstd]` and letting callers opt in. **Open question — see below.** |

**On `klog_value_threshold` specifically:** with item 1 landed, the threshold
governs write-time layout and cache granularity rather than read cost, so the
pressure to get it exactly right drops a lot. 4096 is defensible because a
value below one page costs a whole extra seek to save under 4 KiB of block
space. Above that the trade genuinely turns.

**Format impact:** none of these change on-disk *formats*. `klog_value_threshold`
changes on-disk *layout* for newly written tables only; blocks and vlog frames
are self-describing (`config.rs:384-386`), so old and new tables interoperate
and no migration exists or is needed. Say this explicitly in the changelog —
it is the first question a consumer will ask.

---

## 3. BUG — compaction is never scheduled for bulk-ingested column families

**Confirmed.** `Ingestion::finish` (`ingest.rs:129-137`) installs tables into L0
and persists the manifest:

```rust
self.cf.install_handles_l0(std::mem::take(&mut self.done));
self.db.persist_manifest()?;
```

It **never sends to `compact_tx`**. The only two senders in the engine are in
the flush worker (`db.rs:1012`, `db.rs:1048`), both gated on a memtable flush
having occurred. A consumer that writes exclusively through `start_ingestion`
— which is exactly what spada does for all its data families — therefore
**never compacts at all**.

Observed: 308 SSTables for 20,000 documents, all in L0, growing without bound.
Every iterator construction linearly scans all of them for bounds
(`column_family.rs:917-928`).

**Fix.** Mirror the flush worker's trigger at the end of `Ingestion::finish`:

```rust
if !db.closing.load(Ordering::Relaxed)
    && (fifo || cf.l0_len() >= cf.opts.l1_file_count_trigger as usize)
{
    let _ = db.ctx.compact_tx.send(cf.clone());
}
```

**This is the highest-severity item in the release** — an engine that silently
never compacts for a whole class of writer is broken, not slow. It deserves its
own changelog paragraph and a regression test asserting `l0_len()` falls after
ingestion.

⚠️ **Consumer note:** spada must land its posting-list cursor *before* taking
this fix, because compaction fills posting frames from ~4 entries toward 128,
which multiplies spada's current per-frame re-decode cost. Coordinate the
version bump with spada's plan ordering.

---

## 4. BUG — `Memtable::get`'s empty fast path races the counter it tests

**Confirmed by reading.** `memtable.rs:439`:

```rust
if self.num_entries.load(AtOrd::Relaxed) == 0 { return Lookup::default(); }
```

but `put` (`memtable.rs:275-303`) publishes in the opposite order:

```rust
self.filter.get_or_init(MemFilter::new).insert(h);   // 1
shard.insert(...) / shard.put(...);                  // 2  node fully linked and visible
self.after_insert(added, seq);                       // 3  num_entries += 1  ← LAST
```

A `get` landing between (2) and (3) on a previously-empty memtable reads `0`,
returns not-found, and `ColumnFamily::get` (`column_family.rs:773-788`) falls
through to the SSTables and returns `NotFound` **for a key that is fully linked
and visible**. `put_batch` has the identical shape (`memtable.rs:401-425`).

The pre-filter check one line down (`memtable.rs:443-448`) is a second
unsynchronised early return: a `Relaxed` load against a `Relaxed` `fetch_or`,
with no ordering to the shard publish.

**Fix.** Increment `num_entries` and set the filter **before** the shard
insert, or delete the fast path. Incrementing first is safe: an over-count
merely costs a descend that finds nothing.

**Honest scoping.** This is in `memtable.rs`, compiled into **both** feature
builds, so it does not by itself explain the arena-only `unsafe-fastpath`
NotFound symptom that has been under investigation. All three previously
suspected arena causes were **ruled out** by reading — the `Relaxed` height
store before level linking, `cmp_node`'s 8-byte prefix shortcut, and shard
selection divergence between `put` and `get` are each sound. This is a real bug
worth fixing regardless; **it is not yet established to be that bug.** Fix it,
then re-run the stress reproduction and see whether the symptom survives.

---

## 5. Grouped multi-CF ingestion — one manifest persist, not N

**Confirmed.** `Ingestion::finish` calls `db.persist_manifest()` (`ingest.rs:135`),
and `persist_manifest` (`db.rs:250-274`) rewrites the **whole** manifest:
every CF, every level, every `SstMeta` via `snapshot_ssts`
(`column_family.rs:935-944`), then temp-write → `sync_all` → rename →
`sync_all` the parent dir (`manifest.rs:113-134`).

spada writes five data lanes per segment seal, so a single seal triggers
**five full manifest rewrites**. Worse, it is quadratic in table count: each
rewrite re-serialises every previously written table's metadata.

**Fix.** Add `DB::start_ingestion_multi(&[&str]) -> MultiIngestion` that
installs every CF's tables and persists the manifest **once**. Keep
`start_ingestion` as-is.

**Effect:** ~5× fewer manifest rewrites and ~10 fewer `F_FULLFSYNC` barriers
per spada seal. Note the durability invariant is unchanged — tables are still
fsynced before the manifest that references them.

---

## 6. Documentation — the omission that made this release necessary

`docs/formats.md:58-59` currently says:

> *"Two files: `<id>.klog` (always) and `<id>.vlog` (created lazily on the first
> value with `len >= klog_value_threshold`, default 512 — WiscKey separation)."*

That is the entire guidance. It does not say that vlog reads bypass the block
cache, which is the single most important operational fact about the setting.
I found it by profiling a consumer, not by reading the docs.

**Add** to `docs/formats.md` and `docs/architecture.md`:

- Vlog values are read through the vlog cache (after item 1) / were previously
  uncached; state which, per version.
- Compaction **rewrites** vlog values — so separation does *not* reduce write
  amplification in ondadb as implemented. Anyone reasoning from the WiscKey
  paper will assume otherwise.
- Guidance: raise the threshold when values are small-ish and read repeatedly
  within one logical operation; lower it when values are large and read once.
- ondadb's own benchmark configuration (512 MiB cache, lz4, `unsafe-fastpath`)
  belongs in the README next to the numbers, since consumers are otherwise
  comparing against defaults ondadb does not itself benchmark with.

---

## 7. Deferred to 0.8.0 — named so they are not forgotten

- **A9 incremental manifest.** The whole manifest is rewritten per persist;
  ondadb's own sizing probe (`manifest.rs:658-711`) records 12.4 MiB at 100k
  parts and comments *"untenable"*. Item 5 reduces the *frequency*; only an
  edit log fixes the *cost*. Required before spada's full-corpus stage.
- **SSTable shared-prefix key compression.** `sst/mod.rs:237` writes the full
  key verbatim per entry; the `restart_interval` machinery exists for in-block
  binary search but the LevelDB-style prefix delta that normally accompanies it
  is not implemented. spada keys share a ~27-byte invariant prefix across
  millions of entries.
- **Honour `sync_mode` in the SST writer.** `Writer::finish` `sync_all`s
  regardless of the CF's configured `SyncMode::None`.

---

## Release mechanics

1. Land items 1, 3, 4, 5 with tests; then 2 behind its sweep; then 6.
2. `cargo test`, `cargo test --features unsafe-fastpath`,
   `cargo clippy --all-targets`, `cargo clippy --all-targets --features unsafe-fastpath`
   — the four-command, two-config gate.
3. `CHANGELOG.md`: **lead with the fact that defaults changed and that D-191's
   additive-only rule was waived by decision.** Then the compaction bug, then
   the vlog cache. A consumer skimming must not miss either.
4. Version **0.7.0** (minor: new public surface, changed defaults, no format
   break). Tag `v0.7.0`.
5. Push to **`teixos` first** — spada's CI fetches `ONDADB_REF` as a tarball
   from that instance, so a tag on `origin` alone leaves CI red — then `origin`.
   Verify with `git branch -r --contains v0.7.0` naming both.
6. Bump spada's `ONDADB_REF` and re-run spada's df sweep and ingest benchmark
   to quantify what the release bought, **coordinated with the cursor-first
   ordering in item 3's consumer note.**

## Verification

- **Item 1:** a test asserting the second read of a vlog value performs no
  `pread` (instrument `LocalReadHandle`, or assert on `CacheStats`
  (`cache/block.rs:76`), which already exists and is currently uninstrumented
  by consumers). Plus a benchmark: repeated reads of a 10 KiB value.
- **Item 3:** ingest enough tables to exceed `l1_file_count_trigger`; assert
  `l0_len()` falls. This test fails on `main` today.
- **Item 4:** a stress test hammering `get` against a memtable transitioning
  from empty, asserting no false `NotFound`. Then re-run the `unsafe-fastpath`
  reproduction and report honestly whether the symptom survives.
- **Item 5:** assert one manifest persist for an N-CF grouped ingestion.
- **Item 2:** the threshold sweep, recorded in `BENCHMARK-RESULTS.md` with the
  hardware and the workload shape.
