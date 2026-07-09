# Parts & storage tiers — design plan

ClickHouse-style detachable data parts for ondaDB, with a storage-policy layer that
moves keyspaces (by prefix) across tiers (ssd → hdd → s3). This reshapes the old
"Phase 3b" (shared blob files + GC, fjall's model) — that design is **rejected** here
because shared blob files create cross-part references and kill detachability.

Status of the disk/write-amp motivation: vlog compression (3a, `bf0d2bf`) already took
webtable from 2,875 MB / wamp 4.8 to **1,172 MB / wamp 2.46** (fjall: 2,540 MB / 2.54).
The write-amp emergency is over; parts are now driven by **operability** (backup,
tiering, retention) with vlog GC as a secondary win.

## 1. Concepts

- **Partition** — a key range defined by prefix rules (same machinery as
  `compression_rules`: longest prefix wins, `partition_of(key) -> PartitionId`).
  Un-ruled keys live in the implicit default partition.
- **Part** — one partition's set of SSTable file pairs (`<id>.klog` + `<id>.vlog`) at
  the bottom level. Parts are the unit of DETACH/ATTACH/FREEZE and of tier movement.
  Upper levels (L0..L(n-1)) are "young data" and never detachable.
- **Tier** — a named storage location (`ssd` = default DB dir, `hdd` = another mount,
  later `s3`). Every SSTable has a location; today that is implicitly the CF dir.

## 2. What already exists (foundations)

| Need | Existing code |
|---|---|
| Part files | every SSTable is already a discrete `<id>.klog`/`<id>.vlog` pair (`column_family.rs::klog_path`) |
| Catalog | `MANIFEST` + `SstMeta` (`manifest.rs`) records id/level/sizes/min/max keys |
| FREEZE | `DB::checkpoint(dir)` / `backup(dir)` — hard-link / copy snapshot (`maintenance.rs:93-103`) |
| ATTACH foundation | `DB::start_ingestion` writes + registers external SSTables (`ingest.rs`) |
| Prefix rules | `CompressionRule` resolution (`config.rs::compression_for_key`) — reuse shape for `PartitionRule`, `TierRule` |
| S3 dep | `s3` feature (`rust-s3`, `tokio`) declared, currently unused — the future remote backend |

## 3. Partitioning (prerequisite for everything below)

**Change:** compaction into the bottom level cuts output files at partition
boundaries (today `compact_level` cuts on size only). One hook:

```rust
pub struct PartitionRule { pub prefix: Vec<u8>, pub name: String }
// ColumnFamilyConfig::partition_rules: Vec<PartitionRule>   (manifest-persisted,
// same append-tolerant tail as compression_rules)
```

- Writer side: `compact_level` tracks `partition_of(current_key)`; when it changes,
  finish the current output file and start a new one. Upper levels are untouched
  (parts only need to be clean at the bottom).
- `SstMeta` gains `partition: Option<String>` (set only for bottom-level files cut on
  a boundary). Older manifests decode to `None` — the whole CF is then one part.
- Effort: small. The file-cutting loop and manifest plumbing are localized.

## 4. Part lifecycle: DETACH / ATTACH / FREEZE

```text
ACTIVE ──detach──▶ DETACHED (files in cf-dir/detached/, out of manifest)
  ▲                     │
  └──────attach─────────┘        FREEZE = hard-link snapshot (exists: checkpoint)
```

- **`cf.detach_part(partition)`** — under the state write-lock: remove the part's
  tables from `levels[last]` + MANIFEST (one atomic manifest record listing the
  detached ids), move the file pairs to `detached/`. Open iterators keep working —
  readers hold `Arc<SstHandle>`; files are moved, not deleted (fd-based reads are
  unaffected on unix; the mmap stays valid). New reads no longer see the range.
  Caveat (documented): like a ClickHouse DETACH, this is **not snapshot-consistent**
  — data vanishes for new readers regardless of their snapshot seq.
- **`cf.attach_part(path)`** — validate footer magic + CRC per file, then:
  - *Same-lineage* files (our own detached parts): seqs are already below
    `visible_seq` → insert directly at the bottom level if the key range doesn't
    overlap live bottom tables; else L0.
  - *Foreign* files: seqs may collide/exceed — either remap seqs by rewriting (slow
    path, reuse `Ingestion`) or record a per-table `seq_floor` translation in
    `SstMeta` (fast path, RocksDB-style "global seqno"). Start with the rewrite path;
    add `seq_floor` when needed.
- **FREEZE** — `checkpoint()` already does hard-link snapshots of the whole DB; add a
  per-partition variant that links only one part + a manifest slice.

## 5. Partition-scoped vlogs (the reshaped GC, now optional)

Keep WiscKey vlogs **per part**, never shared across partitions:

- Compaction within a partition may emit klogs that *reference the inputs' vlog
  files* instead of rewriting values (`SstMeta` gains `vlog_refs: Vec<u64>`;
  vlog files become refcounted within the partition).
- A staleness GC (per partition): when `live_bytes / vlog_bytes < 0.5`, rewrite the
  vlog. Bounded, local, and a part remains a closed set of files → detachable.
- Priority: **low** since 3a (compressed rewrites already cut wamp to 2.46, better
  than fjall). Implement only if profiling shows vlog rewrite cost matters on real
  workloads.

## 6. Storage tiers — the substrate (this plan), not yet S3 (later)

### 6.1 Tier registry

```rust
pub struct TierDef { pub name: String, pub root: PathBuf }   // DB-level Options
// Options::tiers: Vec<TierDef>  — "ssd" (default) is implicit: the DB dir.
```

A tier is, for now, **a directory on some mount** (ssd, hdd, nfs). The S3 tier later
implements the same `Storage` trait behind the existing `s3` feature.

### 6.2 Storage abstraction (the substrate)

All SSTable file access today goes through `FileCache::acquire(path)` + `read_exact_at`
(+ optional mmap). Introduce a minimal trait *at that choke point*:

```rust
pub trait Storage: Send + Sync {
    fn open_read(&self, rel: &str) -> Result<FileHandle>;   // pread-able handle
    fn create(&self, rel: &str) -> Result<Box<dyn Write>>;  // writer (flush/compact)
    fn delete(&self, rel: &str) -> Result<()>;
    fn rename(&self, from: &str, to: &str) -> Result<()>;
    fn list(&self, dir: &str) -> Result<Vec<String>>;
    fn supports_mmap(&self) -> bool;                        // false for remote tiers
}
```

- `LocalStorage` (per tier root) is the only implementation in this phase.
- `Reader` gains "no-mmap" operation for tiers where `supports_mmap() == false`
  (the safe-build pread path already exists — remote tiers just always use it, plus
  the block cache, which becomes *more* important for slow tiers).
- WAL and L0 always live on the default tier — only bottom-level parts move.

### 6.3 Manifest: file location

`SstMeta` gains `tier: Option<String>` (default = primary dir; append-tolerant
decode). `SstHandle` resolution (`klog_path`) consults the tier registry.

### 6.4 Storage policies

```rust
pub struct TierRule {
    pub prefix: Vec<u8>,       // keyspace selector (same shape as the other rules)
    pub tier: String,          // target tier for the part
    pub min_age: Duration,     // move only parts whose newest entry is older
}
// ColumnFamilyConfig::tier_rules: Vec<TierRule>
```

A background **part mover** (new maintenance job, same worker pool as compaction):

1. Scan bottom-level parts; for each, resolve `TierRule` by partition prefix.
2. If the part's tier ≠ target and `now - max_entry_time > min_age`:
   copy files to the target tier → fsync → flip `tier` in one manifest record →
   delete source files (crash-safe order; a crash between copy and flip leaves an
   orphan copy that startup GC removes).
3. Reads are uninterrupted: the flip swaps the `SstHandle`'s storage under the state
   write-lock; in-flight reads finish on the old handle.

Age tracking needs `max_entry_time` per table — add to `SstMeta` (writers stamp it
from commit time; approximate is fine for tiering).

### 6.5 What S3 needs later (explicitly out of scope now)

- `S3Storage` implementing `Storage` via `rust-s3` (feature-gated, sync wrappers) —
  no mmap, aggressive block-cache use, range GETs for blocks.
- Part-mover retry/ratelimit hardening, credentials in `Options`.
- Optional local read-through cache directory for hot remote parts.

## 7. Milestones

| # | Deliverable | Size | Unblocks |
|---|---|---|---|
| P1 | `partition_rules` + boundary-cutting compaction + `SstMeta.partition` | S | everything |
| P2 | `detach_part` / `attach_part` (same-lineage) + per-part FREEZE | M | backups |
| P3 | `Storage` trait + `LocalStorage` + `SstMeta.tier` + tier registry | M | tiering |
| P4 | `tier_rules` + part mover + `max_entry_time` | M | ssd→hdd policies |
| P5 | Foreign-file ATTACH (seq remap / seq_floor) | M | cross-DB restore |
| P6 | Partition-scoped vlog refs + staleness GC | M | write-amp (optional) |
| P7 | `S3Storage` behind `s3` feature | L | cold tier |

P1→P4 give the requested substrate: prefix-scoped storage policies over detachable
parts on local tiers, with S3 as a drop-in `Storage` later.

## 8. Invariants & risks

- **Detach is not snapshot-consistent** (documented; same as ClickHouse).
- **Seq safety on attach:** same-lineage only in P2; foreign attach is its own
  milestone because seq collisions corrupt MVCC if mishandled.
- **Crash-safety of moves:** copy → fsync → manifest flip → delete; startup sweeps
  orphans on both sides (manifest is the single source of truth).
- **Cache keying:** block cache is keyed by `file_id` — ids are never reused across
  attach (attach assigns fresh ids), so no stale-cache hazard.
- **WAL/L0 never tiered** — tier movement applies to sealed bottom-level parts only.
- **Compression rules & tier rules compose:** e.g. `img/` → zstd + hdd after 30 days;
  both resolve by longest prefix, independently.
