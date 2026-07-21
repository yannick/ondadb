# Parts & tiers — feature guide

How to carve a column family's keyspace into named **partitions**, treat each
partition's bottom-level data as a **part** (detach / attach / freeze it like a
ClickHouse part), and place parts on storage **tiers** — a second disk, an NFS
mount, or an S3 bucket — by an age-based policy or by hand. New in 0.3.0.

Internals live in `docs/architecture.md` (§ Partitions, § Storage tiers,
§ Part lifecycle, § Part mover, § S3 tier); on-disk encodings in
`docs/formats.md` (§ Append-tolerant tail); locking/blocking contracts in
`docs/concurrency-and-safety.md`. This file is the user-facing guide: concepts,
worked examples, operational notes. All snippets use real API names verified
against the code; they are illustrative (`no_run`-style) — they need real
directories/endpoints to execute.

## Concepts: partition → part → tier

| Term | What it is | Declared / recorded by |
|---|---|---|
| **partition** | A named slice of the keyspace, by key prefix; longest matching prefix wins, un-ruled keys form the implicit default partition | `ColumnFamilyConfig::partition_rules` (`PartitionRule { prefix, name }`) |
| **part** | One partition's set of **bottom-level** SSTable file pairs. Bottom compaction cuts its output at partition boundaries, so a part is always partition-clean; upper levels and the memtable stay mixed | `SstMeta::partition`, stamped by bottom compaction |
| **tier** | A named storage location parts can live on: a directory on some mount, an S3 prefix, or a caller-built backend. The DB directory is the implicit default tier, reserved name `"ssd"` | `Options::tiers` (`TierDef`); per-table placement in `SstMeta::tier` |

Three consequences to internalize before using any of it:

- **Partition rules are write-side-only policy.** They shape files written by
  *future* bottom compactions; nothing is rewritten when rules change. A part
  only exists once flush + compaction have materialized bottom files on the
  boundary.
- **Only the bottom level is partitioned.** Fresh writes sit in the
  memtable/L0/upper levels regardless of partition; detach/freeze/move act on
  the bottom part only. `detach_part` errors `NotFound` if a partition has no
  bottom tables yet — flush + compact first.
- **The local manifest is the commit point** for every catalog change (detach,
  attach, tier flip). It is rewritten crash-atomically (temp file + fsync +
  rename); a crash mid-operation leaves at worst orphan files, never a reader
  routed to a missing file.

## Defining partitions

```rust
use std::time::Duration;
use ondadb::{ColumnFamilyConfig, Options, PartitionRule, DB};

let db = DB::open(Options::new("/data/onda"))?;
let cf = db.create_column_family(
    "default",
    ColumnFamilyConfig {
        partition_rules: vec![
            PartitionRule { prefix: b"img/".to_vec(), name: "img".into() },
            // Nesting is legal — longest prefix wins, so img/thumb/* resolves
            // to "thumb", every other img/* to "img". Only an exact-duplicate
            // prefix is rejected (ColumnFamilyConfig::validate).
            PartitionRule { prefix: b"img/thumb/".to_vec(), name: "thumb".into() },
            PartitionRule { prefix: b"log/".to_vec(), name: "log".into() },
        ],
        ..ColumnFamilyConfig::default()
    },
)?;

// Keys route by prefix; anything else lands in the implicit default partition.
db.put(&cf, b"img/2026/07/cat.jpg", b"...", Duration::ZERO)?;
db.put(&cf, b"img/thumb/cat.jpg", b"...", Duration::ZERO)?;
db.put(&cf, b"log/2026-07-18", b"...", Duration::ZERO)?;
db.put(&cf, b"users/42", b"...", Duration::ZERO)?;   // default partition

// A part materializes when the data reaches the bottom level:
db.flush_memtable(&cf)?;
db.compact(&cf)?;
// Now cf has partition-clean bottom parts "img", "thumb", "log" (plus the
// default part) — each detachable / freezable / tierable by name.
# Ok::<(), ondadb::OndaError>(())
```

`partition_rules` are persisted in the manifest's per-CF config blob, so a
reopened DB does not need them re-supplied.

## Adding (and removing) partition rules live

`DB::add_partition_rule` carves a new partition out of a **running** column
family — no reopen, no rewrite:

```rust
use ondadb::PartitionRule;

// Take effect on the NEXT bottom compaction (write-side-only): existing
// bottom files keep their stamps until a compaction re-cuts them.
db.add_partition_rule(&cf, PartitionRule {
    prefix: b"metrics/".to_vec(),
    name: "metrics".into(),
})?;

// Even with metrics/ data already at the bottom, no file is stamped
// "metrics" until a re-cut:
assert!(db.detach_part(&cf, "metrics").is_err());

db.put(&cf, b"metrics/cpu", b"0.7", std::time::Duration::ZERO)?;
db.flush_memtable(&cf)?;
db.compact(&cf)?;                // re-cuts the bottom on the new boundary
db.detach_part(&cf, "metrics")?; // now it is a part
# Ok::<(), ondadb::OndaError>(())
```

- An exact-duplicate prefix fails with `OndaError::InvalidArgs` ("duplicate");
  concurrent adds are race-free (validation + append under one lock — exactly
  one of two racing duplicate adds wins).
- The new rule is persisted immediately (manifest rewrite) and survives
  reopen; a compaction already in flight finishes on the rules it snapshotted.
- `DB::remove_partition_rule(&cf, b"metrics/")` is the symmetric inverse:
  future bottom compactions stop cutting on the boundary, but already-stamped
  parts keep their names (and stay detachable) until a later compaction merges
  them back.

## Part lifecycle: detach, attach, freeze

```rust
use ondadb::DetachedPart;

// DETACH: drop the part from the catalog (one atomic manifest record) and
// move its file pairs to <cf-dir>/detached/img.
let d: DetachedPart = db.detach_part(&cf, "img")?;
println!(
    "partition {} → {} ({} tables, files: {:?})",
    d.partition, d.dir, d.table_ids.len(), d.files
);
assert!(db.get(&cf, b"img/2026/07/cat.jpg").is_err()); // hidden for new reads

// ATTACH it elsewhere. The target must be the SAME LINEAGE: attach validates
// every file (footer magic + CRCs) and rejects tables whose max_seq exceeds
// the target's visible sequence — so "another DB" means a checkpoint, backup
// or clone of this one, not an arbitrary foreign database.
db.checkpoint("/data/onda-copy")?;
let db2 = DB::open(ondadb::Options::new("/data/onda-copy"))?;
let cf2 = db2.get_column_family("default").unwrap();
db2.attach_part(&cf2, &d.dir)?;      // validated, copied in under fresh ids
assert_eq!(db2.get(&cf2, b"img/2026/07/cat.jpg")?, b"...");

// ...or simply re-attach into the source DB:
db.attach_part(&cf, &d.dir)?;
# Ok::<(), ondadb::OndaError>(())
```

Attach placement: a part whose key range does not overlap any live bottom
table slots straight into the bottom level; an overlapping one goes to L0 (L0
tolerates overlap) and merges down normally. Validation and copy are
all-or-nothing — one bad file rejects the whole directory and removes the
copies already made.

**Detach is not snapshot-consistent** (deliberately, like ClickHouse's
`DETACH`): the part vanishes for every *new* read regardless of its snapshot
sequence. Iterators opened *before* the detach are unaffected — they pin the
part's SSTable handles and blocks, so an in-flight scan completes normally.

```rust
// FREEZE: hard-link the part + a one-part manifest slice into `dir`,
// producing a standalone, independently openable database. The live part is
// untouched (freeze is the cheap "export a partition" primitive).
db.freeze_part(&cf, "log", "/backups/log-2026-07-18")?;
let frozen = DB::open(ondadb::Options::new("/backups/log-2026-07-18"))?;
let fcf = frozen.get_column_family("default").unwrap();
assert_eq!(fcf.name(), "default"); // only the log part's data is inside
# Ok::<(), ondadb::OndaError>(())
```

`freeze_part` doubles as a non-destructive probe: it returns
`OndaError::NotFound` when no bottom part carries that partition name.

Limitation: detach/attach/freeze move and link files with `std::fs`, so they
operate on parts resident on the **default tier**. A part already moved to a
remote tier cannot be detached or frozen in 0.3.0.

## Local multi-tier setup + age policy

Declare tiers in `Options`, pin partitions to them with per-CF `tier_rules`,
and let the background part mover do the placement:

```rust
use std::time::Duration;
use ondadb::{ColumnFamilyConfig, Options, PartitionRule, TierDef, TierRule, DB};

let mut opts = Options::new("/data/onda");
opts.tiers = vec![
    // A second local disk; mmap reads allowed (the default for TierDef::new).
    TierDef::new("hdd", "/mnt/hdd/onda"),
    // An NFS-style mount: without_mmap() forces the buffered pread path +
    // block cache, which is what you want on a slow/remote filesystem.
    TierDef::new("cold", "/mnt/nfs/onda").without_mmap(),
];
// The mover pass runs on the compaction worker at this cadence (default 30s;
// Duration::ZERO disables the schedule — DB::run_part_mover still works).
opts.part_mover_interval = Duration::from_secs(60);

let db = DB::open(opts)?;
let cf = db.create_column_family(
    "default",
    ColumnFamilyConfig {
        partition_rules: vec![
            PartitionRule { prefix: b"img/".to_vec(), name: "img".into() },
            PartitionRule { prefix: b"log/".to_vec(), name: "log".into() },
        ],
        tier_rules: vec![
            // Longest matching prefix wins, exactly like partition_rules.
            // A part moves only once its newest entry (SstMeta::max_entry_time)
            // is older than min_age; compaction carries the stamp forward, so
            // rewriting cold data does not reset its age.
            TierRule { prefix: b"img/".to_vec(), tier: "hdd".into(),
                       min_age: Duration::from_secs(7 * 24 * 3600) },
            TierRule { prefix: b"log/".to_vec(), tier: "cold".into(),
                       min_age: Duration::from_secs(30 * 24 * 3600) },
        ],
        ..ColumnFamilyConfig::default()
    },
)?;

// Manual levers (also what tests use):
let moved: usize = db.run_part_mover()?;          // one full pass, all CFs
db.move_part_to_tier(&cf, "img", "hdd")?;         // move one part now
# Ok::<(), ondadb::OndaError>(())
```

Deterministic crash harnesses can call `DB::move_part_to_tier_observed` with a
`MovePhaseObserver`. It executes the same mover and reports, synchronously,
each object-copy completion before `StorageWriter::finish`, completion of all
destination syncs, the durable manifest flip, and completion/deferment of
source deletion. Blocking at a boundary lets a subprocess controller kill the
process without timing sleeps. Returning an error stops the move only before
`ManifestFlipped`; at and after that durable commit point hook errors are
ignored, cleanup continues, and the call reports success. Retrying a move after
a lost response is idempotent when the part already names the target tier. This
API is an observation/fault seam only: it cannot replace the manifest commit,
and normal policy/manual moves pay no callback or event-allocation cost.

Notes:

- Both rule sets persist in the manifest; a reopened DB re-applies them. The
  `Options::tiers` list itself is **not** persisted — supply the same
  `TierDef`s on every open (the manifest records *which* tier each table is
  on; `Options` records where each tier *is*).
- Moves are crash-safe (copy → durable finish → manifest flip → delete
  source) and idempotent; local orphans from a crash mid-move are swept at
  the next open. Reads never block on a move — the flip swaps handles and
  in-flight reads finish on the old ones.
- The mover only moves parts **onto** named tiers. A rule with
  `tier: "ssd".into()` (the reserved default-tier name) stops future moves
  but does not move a part back; there is no automatic demotion in 0.3.0.
- A partition with no tier rule stays wherever it was written (the default
  tier — compaction output always lands there).

## S3 tier end-to-end (feature `s3`)

Enable the feature (it pulls network deps; the core build stays lean):

```toml
[dependencies]
ondadb = { version = "0.3", features = ["s3"] }
```

Then define the tier — `S3Config` + `TierDef::s3`. For MinIO (and any
endpoint that addresses buckets by path, not subdomain) set `path_style`:

```rust
use std::time::Duration;
use ondadb::{ColumnFamilyConfig, Options, PartitionRule, S3Config, TierDef, TierRule, DB};

let s3cfg = S3Config {
    bucket: "archive".into(),
    region: "us-east-1".into(),               // any string MinIO accepts
    endpoint: "http://192.168.65.11:9000".into(),
    access_key: "minioadmin".into(),
    secret_key: "minioadmin".into(),
    path_style: true,                         // required by MinIO
};

let mut opts = Options::new("/data/onda");
// The tier root is an IN-BUCKET KEY PREFIX, not a filesystem path: this
// part's files become objects archive:/onda-prod/cf-default/<id>.klog.
// mmap is forced off for S3 tiers.
opts.tiers = vec![TierDef::s3("s3", "onda-prod", s3cfg)];
// S3 tiers lean hard on the block cache (below) — size it up.
opts.block_cache_size = 512 << 20;

let db = DB::open(opts)?;
let cf = db.create_column_family(
    "default",
    ColumnFamilyConfig {
        partition_rules: vec![
            PartitionRule { prefix: b"img/".to_vec(), name: "img".into() },
        ],
        tier_rules: vec![
            TierRule { prefix: b"img/".to_vec(), tier: "s3".into(),
                       min_age: Duration::from_secs(30 * 24 * 3600) },
        ],
        ..ColumnFamilyConfig::default()
    },
)?;
// From here everything is the ordinary flow: once an img/ part's newest entry
// is 30 days old, the mover PUTs its files to the bucket, flips the manifest,
// deletes the local copies — and db.get(&cf, b"img/...") keeps working,
// served by range GETs through the block cache.
# Ok::<(), ondadb::OndaError>(())
```

What actually happens on the wire:

- **Writes** (a part move onto the tier): each file is streamed into a
  buffering `StorageWriter` and committed as **one single-shot PUT** on
  `finish()`. Objects are written whole and never appended — a part file is
  produced by exactly one compaction or copy.
- **Reads**: opening a reader costs one HEAD (object size, cached) plus
  range GETs for footer/index/bloom; after that, every block-cache miss on a
  data block is **exactly one HTTP range GET** of that ~4 KiB framed block,
  and a warm block is zero network. No query ever downloads a whole file.
  `S3Storage::metrics()` (`S3Metrics { range_gets, range_get_bytes, puts,
  heads }`) exposes the counters.
- **Block-cache sizing**: on a local tier the cache saves a pread; on S3 it
  saves a network round-trip, so it is the entire read-latency story. Budget
  the cache against the *working set of S3-resident blocks* (indexes and
  blooms are held by the reader; data blocks compete for the cache). If S3
  reads matter to you, hundreds of MB (`Options::block_cache_size`, default
  64 MiB) is money well spent; watch `range_gets` to confirm your hit rate.

## Custom tier backends (`TierBackend::Custom`)

`TierDef::custom` injects a caller-built `Storage` for a tier — the engine
uses it verbatim (and forces the no-mmap read path). This is the seam for
interposing a decorator in front of a remote backend, e.g. a read-through
block/object cache like ayu's foyer layer:

```rust
use std::sync::Arc;
use ondadb::storage::{ReadHandle, Storage, StorageWriter};
use ondadb::{Result, S3Config, S3Storage, TierDef};

/// Sketch: a read-through cache over any inner Storage. Writes pass straight
/// through; open_read wraps the inner handle with one that consults a local
/// cache before delegating a range read.
#[derive(Debug)]
struct CachingStorage {
    inner: Arc<dyn Storage>,
    // + your cache handle (disk/LRU/foyer/...)
}

impl Storage for CachingStorage {
    fn open_read(&self, path: &str) -> Result<Arc<dyn ReadHandle>> {
        let inner = self.inner.open_read(path)?;
        Ok(Arc::new(CachingReadHandle { inner /*, cache key = path */ }))
    }
    fn create(&self, path: &str) -> Result<Box<dyn StorageWriter>> {
        self.inner.create(path)               // write-through
    }
    fn ensure_dir(&self, dir: &str) -> Result<()> { self.inner.ensure_dir(dir) }
    fn delete(&self, path: &str) -> Result<()> { self.inner.delete(path) }   // + invalidate
    fn rename(&self, from: &str, to: &str) -> Result<()> { self.inner.rename(from, to) }
    fn list(&self, dir: &str) -> Result<Vec<String>> { self.inner.list(dir) }
    fn supports_mmap(&self) -> bool { false } // Custom tiers never mmap anyway
    fn release(&self, path: &str) { self.inner.release(path) }
}

struct CachingReadHandle { inner: Arc<dyn ReadHandle> }
impl ReadHandle for CachingReadHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        // hit? copy from cache : { self.inner.read_exact_at(buf, offset)?; insert }
        self.inner.read_exact_at(buf, offset)
    }
    fn size(&self) -> Result<u64> { self.inner.size() }
}

// Wire it in: wrap the real S3 backend and hand the wrapper to the tier.
fn s3_cached_tier(s3cfg: &S3Config) -> ondadb::Result<TierDef> {
    let s3 = S3Storage::new(s3cfg)?;             // Arc<S3Storage>
    let cached: Arc<dyn Storage> = Arc::new(CachingStorage { inner: s3 });
    Ok(TierDef::custom("s3-cached", "onda-prod", cached))
}
// opts.tiers = vec![s3_cached_tier(&s3cfg)?];
```

Contract for implementors: all methods take absolute paths/keys built by the
engine's `TierRegistry` (`<tier root>/cf-<name>/<id>.klog`); `read_exact_at`
is positional and a short read is an error; `create(...).finish()` must make
the object durable (it runs *before* the manifest flip that publishes it);
`delete` of a missing object must succeed; implementations must be
`Send + Sync` — engine threads call concurrently.

## Operational notes

**Durability model.** The commit point for every part/tier operation is the
*local* manifest (crash-atomic temp-file + fsync + rename), never a remote
object. ondaDB deliberately has no S3 object CAS: part objects have unique,
never-reused ids with a single writer each, so plain PUTs are safe, and any
shared-pointer coordination (multi-writer S3 pointer CAS) belongs to the
layer above the engine. Consequence: an S3 bucket alone is not a backup — a
usable copy of a tiered database is the DB directory (manifest + WAL + hot
parts) *plus* the tier objects.

**Crash residue & the S3 leak gap.** A crash mid-move strands either a copy
on the target tier (before the manifest flip) or the source files (after
it). On **local** tiers, `DB::open` sweeps both cases automatically (it
deletes any manifest-known table id found on the wrong tier). The sweep
walks directories with `std::fs`, so it does **not** cover S3; likewise,
compaction's obsolete-input deletion resolves default-tier paths, so
compacting or re-cutting an S3-resident part strands its old objects.
Both gaps leak *storage only* — the manifest is the source of truth, reads
are never affected. Mitigation until in-engine GC lands: periodically audit
the bucket, listing `<root>/cf-<name>/` and deleting any `<id>.klog`/`.vlog`
whose id the current manifest does not place on that tier (the startup
sweep's rule, applied externally). Do not run the audit against a manifest
older than the bucket listing.

**Downgrade caveat.** Pre-0.3.0 binaries open a 0.3.0 database fine but
re-encode the manifest **without** the partition/tier/max-entry-time tail on
their first flush or compaction (see `docs/formats.md`). Partition stamps
regrow on the next bottom compaction; the **tier column does not** — after a
downgrade-then-upgrade, tier-resident parts resolve to default-tier paths
where no file exists, and the DB fails to open its readers. Never run a
pre-0.3.0 binary against a database with parts on a named tier; for
untiered databases a downgrade merely forgets partition stamps temporarily.

**Testing against MinIO.** The S3 suite compiles under `--features s3` and
no-ops unless an endpoint is configured, so plain `cargo test --features s3`
stays green offline. Against a real MinIO:

```sh
ONDADB_S3_ENDPOINT=http://192.168.65.11:9000 \
ONDADB_S3_KEY=<access-key> ONDADB_S3_SECRET=<secret> ONDADB_S3_BUCKET=<bucket> \
  cargo test --features s3 --test s3_tier -- --nocapture --test-threads=1
```

(`ONDADB_S3_REGION` optional, default `us-east-1`. The same variables drive
the in-module tests in `storage_s3.rs`: `cargo test --features s3 s3_`.)
The suite exercises the full loop: mover pass onto the tier, read-back via
bounded range GETs, idempotent re-run, reopen persistence — and asserts via
`S3Metrics` that a point read fetches individual blocks, never whole files.
