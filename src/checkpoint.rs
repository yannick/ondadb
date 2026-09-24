//! Changed-table diffs behind incremental backups (F6), and — built on them —
//! object-store checkpoints.
//!
//! SSTables are immutable and their ids are never reused (one database-wide
//! counter), so a table set is fully described by its `(cf, id)` pairs, and the
//! difference between two sets is exactly what an incremental backup has to
//! ship (`added`) and may forget (`removed`).

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::cache::FileCache;
use crate::db::DB;
use crate::error::{OndaError, Result};
use crate::storage::{CreateOutcome, LocalStorage, Storage};

/// One SSTable in a database's (or a checkpoint's) table set. Everything a
/// caller's backup catalog needs to name and size the table's objects.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CheckpointTable {
    /// Column family the table belongs to.
    pub cf: String,
    /// Table id (unique database-wide, never reused).
    pub id: u64,
    /// Level the table sits on.
    pub level: u32,
    /// Largest sequence number the table holds.
    pub max_seq: u64,
    /// Bytes in the table's `.klog`.
    pub klog_size: u64,
    /// Bytes in the table's `.vlog` (0 = no value log).
    pub vlog_size: u64,
}

impl CheckpointTable {
    fn from_meta(cf: &str, meta: &crate::manifest::SstMeta) -> CheckpointTable {
        CheckpointTable {
            cf: cf.to_string(),
            id: meta.id,
            level: meta.level,
            max_seq: meta.max_seq,
            klog_size: meta.klog_size,
            vlog_size: meta.vlog_size,
        }
    }
}

/// The difference between a prior table set and the live one
/// ([`DB::sstables_diff`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableSetDiff {
    /// Live tables the prior set did not have — what an incremental must ship.
    pub added: Vec<CheckpointTable>,
    /// Prior tables no longer live (compacted away, excised, detached, or their
    /// family dropped) — what a restore of the new set no longer needs.
    pub removed: Vec<CheckpointTable>,
}

impl DB {
    /// Every live SSTable of every column family, ordered by `(cf, id)`.
    ///
    /// Each family's list is one consistent snapshot of its levels; families
    /// are read one after another, so a compaction finishing in between can be
    /// seen in one family and not another. For a set that must match a restore
    /// point exactly, use the table list an object-store checkpoint returns.
    pub fn live_sstables(&self) -> Vec<CheckpointTable> {
        let cfs: Vec<_> = self.inner.cfs.read().values().cloned().collect();
        let mut out: Vec<CheckpointTable> = cfs
            .iter()
            .flat_map(|cf| {
                cf.snapshot_ssts()
                    .into_iter()
                    .map(|meta| CheckpointTable::from_meta(cf.name(), &meta))
                    .collect::<Vec<_>>()
            })
            .collect();
        out.sort_by(|a, b| (&a.cf, a.id).cmp(&(&b.cf, b.id)));
        out
    }

    /// Live SSTables whose `max_seq` is greater than `seq` (wavesdb
    /// `SSTablesSince`): pass a prior backup's sequence to find the tables
    /// holding writes made since.
    ///
    /// This answers "which tables hold new *data*", not "which tables are new":
    /// a compaction that rewrites only old data produces a new table whose
    /// `max_seq` is still `<= seq`, and this call does not report it. A backup
    /// that must restore the *current* table set — whose old inputs that
    /// compaction just retired — needs [`sstables_diff`](Self::sstables_diff).
    pub fn sstables_since(&self, seq: u64) -> Vec<CheckpointTable> {
        self.live_sstables()
            .into_iter()
            .filter(|t| t.max_seq > seq)
            .collect()
    }

    /// The live table set relative to `prior` (a table list an earlier
    /// checkpoint or [`live_sstables`](Self::live_sstables) returned): tables
    /// added since, and tables of `prior` that are gone. Tables are matched by
    /// `(cf, id)`; ids are never reused, so an id present in both is the same
    /// immutable bytes.
    pub fn sstables_diff(&self, prior: &[CheckpointTable]) -> TableSetDiff {
        let live = self.live_sstables();
        let prior_ids: HashSet<(&str, u64)> = prior.iter().map(|t| (t.cf.as_str(), t.id)).collect();
        let live_ids: HashSet<(&str, u64)> = live.iter().map(|t| (t.cf.as_str(), t.id)).collect();
        let removed = prior
            .iter()
            .filter(|t| !live_ids.contains(&(t.cf.as_str(), t.id)))
            .cloned()
            .collect();
        let added = live
            .iter()
            .filter(|t| !prior_ids.contains(&(t.cf.as_str(), t.id)))
            .cloned()
            .collect();
        TableSetDiff { added, removed }
    }
}

// ---- object-store checkpoints (F7) ----------------------------------------

/// Name of the tier [`open_remote_checkpoint`] registers for the checkpoint's
/// objects. Reserved: an `Options::tiers` entry by this name is refused.
pub const REMOTE_CHECKPOINT_TIER: &str = "__ondadb_remote_checkpoint__";

/// Uploads in flight at once. A checkpoint is many independent immutable
/// objects: enough concurrency to fill a remote link, bounded so one checkpoint
/// cannot hold unbounded buffers or connections.
const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;

/// Chunk size for streaming a file between backends.
const COPY_CHUNK: usize = 1 << 20;

/// Options for [`DB::checkpoint_to_object_store`].
#[derive(Debug, Clone, Default)]
pub struct ObjectCheckpointOptions {
    /// Take an **incremental** into the same prefix: objects for tables this
    /// earlier checkpoint (of the same database, into the same prefix) already
    /// uploaded are skipped. The MANIFEST is rewritten with the full cumulative
    /// table set, so only the latest checkpoint in a prefix is restorable.
    pub parent: Option<ObjectCheckpoint>,
    /// Publish with **create-if-absent** semantics and return a receipt (size +
    /// SHA-256) per object. An object already present is accepted only if its
    /// size and SHA-256 match exactly; anything else is refused with
    /// [`OndaError::Exists`]. Needs a store implementing
    /// [`Storage::create_if_absent`]; incompatible with `parent`, because every
    /// receipt must name a complete, independently restorable object set.
    pub receipts: bool,
    /// Concurrent uploads (0 = the default, 4).
    pub upload_concurrency: usize,
}

/// The exact bytes published at one key ([`ObjectCheckpoint::receipts`]).
///
/// `sha256` is the digest of the bytes this process sent (or, for an object
/// that already existed, of the bytes read back and found equal).
/// `store_verified` says the store itself checked what it received against a
/// checksum; see [`ObjectInfo`](crate::storage::ObjectInfo).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReceipt {
    /// Full object key (`<prefix>/MANIFEST`, `<prefix>/cf-<name>/<id>.klog`, …).
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// SHA-256 of the object's bytes.
    pub sha256: [u8; 32],
    /// The store's own checksum token, when it reported one.
    pub store_checksum: Option<String>,
    /// Whether the store verified the bytes on arrival.
    pub store_verified: bool,
}

/// What an object-store checkpoint contains — the caller's record for its own
/// backup catalog, and the `parent` of the next incremental. Nothing here is
/// stored in the object store beyond what the MANIFEST already says.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectCheckpoint {
    /// The checkpoint's sequence cut (the catalog's `global_seq`).
    pub global_seq: u64,
    /// The catalog's next file id: every table created after this checkpoint
    /// has an id at or above it.
    pub next_file_id: u64,
    /// The full table set the MANIFEST names, ordered by `(cf, id)`.
    pub tables: Vec<CheckpointTable>,
    /// One receipt per object, MANIFEST first, when
    /// [`receipts`](ObjectCheckpointOptions::receipts) was requested.
    pub receipts: Vec<ObjectReceipt>,
}

/// Object key of `cf`'s table `id` with extension `ext` under `prefix`. Keeps
/// ondaDB's `cf-<name>` directory naming, so a restored checkpoint is an
/// ordinary database directory.
fn table_key(prefix: &str, cf: &str, id: u64, ext: &str) -> String {
    join_key(prefix, &format!("cf-{cf}/{id}.{ext}"))
}

fn join_key(prefix: &str, rest: &str) -> String {
    let p = prefix.trim_end_matches('/');
    if p.is_empty() {
        rest.to_string()
    } else {
        format!("{p}/{rest}")
    }
}

/// One object an upload publishes: where its bytes come from and its key.
struct Upload {
    key: String,
    storage: Arc<dyn Storage>,
    src: String,
    size: u64,
}

/// Read all `size` bytes of `path` from `storage`.
fn read_all(storage: &dyn Storage, path: &str, size: Option<u64>) -> Result<Vec<u8>> {
    let handle = storage.open_read(path)?;
    let size = match size {
        Some(s) => s,
        None => handle.size()?,
    };
    let len =
        usize::try_from(size).map_err(|_| OndaError::TooLarge(format!("{path}: {size} bytes")))?;
    let mut buf = vec![0u8; len];
    let mut off = 0usize;
    while off < len {
        let n = COPY_CHUNK.min(len - off);
        handle.read_exact_at(&mut buf[off..off + n], off as u64)?;
        off += n;
    }
    Ok(buf)
}

/// Publish one object. Plain mode overwrites; receipts mode creates only if
/// absent and accepts an existing object only when size and SHA-256 match.
fn upload_one(store: &dyn Storage, up: &Upload, receipts: bool) -> Result<Option<ObjectReceipt>> {
    let data = read_all(up.storage.as_ref(), &up.src, Some(up.size))?;
    if !receipts {
        store.put_object(&up.key, &data)?;
        return Ok(None);
    }
    let sha256 = crate::storage::sha256_of(&data);
    match store.create_if_absent(&up.key, &data)? {
        CreateOutcome::Created(info) => {
            if info.size != data.len() as u64 || info.sha256 != sha256 {
                return Err(OndaError::Corruption(format!(
                    "checkpoint upload {}: store reported {} bytes / a different digest",
                    up.key, info.size
                )));
            }
            Ok(Some(ObjectReceipt {
                key: up.key.clone(),
                size: info.size,
                sha256,
                store_checksum: info.store_checksum,
                store_verified: info.store_verified,
            }))
        }
        CreateOutcome::AlreadyExists => {
            // Written by an earlier attempt — or by someone else. Only a
            // complete read that matches exactly may stand in for our upload.
            let existing = store.open_read(&up.key)?;
            let size = existing.size()?;
            if size != data.len() as u64 {
                return Err(OndaError::Exists(format!(
                    "checkpoint object {} differs in size ({size} bytes, want {})",
                    up.key,
                    data.len()
                )));
            }
            let theirs = read_all(store, &up.key, Some(size))?;
            if crate::storage::sha256_of(&theirs) != sha256 {
                return Err(OndaError::Exists(format!(
                    "checkpoint object {} differs in content",
                    up.key
                )));
            }
            Ok(Some(ObjectReceipt {
                key: up.key.clone(),
                size,
                sha256,
                store_checksum: None,
                store_verified: false,
            }))
        }
    }
}

/// Run `uploads` on up to `workers` threads; stop handing out work at the first
/// failure and return it.
fn upload_all(
    store: &dyn Storage,
    uploads: &[Upload],
    receipts: bool,
    workers: usize,
) -> Result<Vec<ObjectReceipt>> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let results: parking_lot::Mutex<Vec<ObjectReceipt>> = parking_lot::Mutex::new(Vec::new());
    let first_err: parking_lot::Mutex<Option<OndaError>> = parking_lot::Mutex::new(None);
    let workers = workers.clamp(1, uploads.len().max(1));
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| loop {
                if failed.load(Ordering::Relaxed) {
                    return;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(up) = uploads.get(i) else {
                    return;
                };
                match upload_one(store, up, receipts) {
                    Ok(Some(r)) => results.lock().push(r),
                    Ok(None) => {}
                    Err(e) => {
                        failed.store(true, Ordering::Relaxed);
                        first_err.lock().get_or_insert(e);
                        return;
                    }
                }
            });
        }
    });
    match first_err.into_inner() {
        Some(e) => Err(e),
        None => Ok(results.into_inner()),
    }
}

/// Receipt order: MANIFEST first, then by family, `.klog` before `.vlog`, id.
fn receipt_order(prefix: &str, key: &str) -> (bool, String, u8, u64) {
    let rel = key
        .strip_prefix(prefix.trim_end_matches('/'))
        .unwrap_or(key)
        .trim_start_matches('/');
    if rel == "MANIFEST" {
        return (false, String::new(), 0, 0);
    }
    let (dir, file) = rel.rsplit_once('/').unwrap_or(("", rel));
    let (stem, ext) = file.rsplit_once('.').unwrap_or((file, ""));
    (
        true,
        dir.trim_start_matches("cf-").to_string(),
        u8::from(ext == "vlog"),
        stem.parse().unwrap_or(0),
    )
}

/// A unique scratch directory under the system temp dir, for bytes that must be
/// staged locally but may not touch the source database directory (a
/// read-only source is never written). Removed on drop.
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn new(tag: &str) -> Result<ScratchDir> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ondadb-{tag}-{}-{}-{seq}",
            std::process::id(),
            crate::util::now_nanos()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(ScratchDir(path))
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        // Scratch only: never part of any catalog, so a plain removal is right.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn utf8(p: &Path) -> Result<&str> {
    p.to_str()
        .ok_or_else(|| OndaError::InvalidArgs(format!("path {p:?} is not UTF-8")))
}

impl DB {
    /// Write a consistent checkpoint of this database to `store` under
    /// `prefix`: every live table's `.klog` (and `.vlog`, when it has one) as
    /// `<prefix>/cf-<name>/<id>.{klog,vlog}`, then `<prefix>/MANIFEST` **last**.
    ///
    /// The MANIFEST is the commit marker: a prefix without one holds no
    /// checkpoint (an interrupted upload leaves only table objects, which a
    /// restore never looks at), and a prefix with one names only objects that
    /// were fully uploaded before it. The MANIFEST is the same snapshot-only
    /// catalog a local [`checkpoint`](DB::checkpoint) writes (tier placement
    /// cleared, no edit log), so a restored prefix is an ordinary database
    /// directory.
    ///
    /// Memtables are flushed first and obsolete-file deletion is paused for the
    /// whole upload, so no table the catalog names can vanish mid-copy. A
    /// table on any tier — local or S3 — is read where it lives. A read-only
    /// source is accepted: its WAL-replayed memtables are written, into a local
    /// scratch directory only, as the newest L0 tables of the checkpoint (the
    /// same path a local checkpoint of a read-only database takes).
    ///
    /// See [`ObjectCheckpointOptions`] for incrementals and receipts.
    pub fn checkpoint_to_object_store(
        &self,
        store: &dyn Storage,
        prefix: &str,
        opts: &ObjectCheckpointOptions,
    ) -> Result<ObjectCheckpoint> {
        if store.is_read_only() {
            return Err(OndaError::ReadOnly(
                "checkpoint_to_object_store: the destination store is read-only".into(),
            ));
        }
        if opts.receipts && opts.parent.is_some() {
            return Err(OndaError::InvalidArgs(
                "checkpoint receipts name a complete, independently restorable object set; \
                 an incremental parent is not supported with receipts"
                    .into(),
            ));
        }
        let _pause = self.inner.pause_deletions();
        let mut plan = self.plan_snapshot()?;
        let scratch = ScratchDir::new("objckpt")?;
        let staged_before: HashSet<(String, u64)> = plan
            .manifest
            .cfs
            .iter()
            .flat_map(|cfm| cfm.sstables.iter().map(|m| (cfm.name.clone(), m.id)))
            .collect();
        if self.inner.opts.read_only {
            for cfm in &plan.manifest.cfs {
                std::fs::create_dir_all(scratch.0.join(format!("cf-{}", cfm.name)))?;
            }
            self.write_sealed_memtables(&scratch.0, &plan.cfs, &mut plan.manifest)?;
        }
        plan.finalize_manifest();

        let skip: HashSet<(&str, u64)> = opts
            .parent
            .as_ref()
            .map(|p| p.tables.iter().map(|t| (t.cf.as_str(), t.id)).collect())
            .unwrap_or_default();
        let mut uploads = Vec::new();
        for f in &plan.files {
            if skip.contains(&(f.cf.as_str(), f.id)) {
                continue;
            }
            uploads.push(Upload {
                key: table_key(prefix, &f.cf, f.id, f.ext),
                storage: f.storage.clone(),
                src: f.src.clone(),
                size: f.size,
            });
        }
        // Tables written from sealed memtables live in the scratch directory.
        let local: Arc<dyn Storage> = LocalStorage::new(Arc::new(FileCache::new(16)), false);
        for cfm in &plan.manifest.cfs {
            for m in &cfm.sstables {
                if staged_before.contains(&(cfm.name.clone(), m.id)) {
                    continue;
                }
                for (ext, size) in [("klog", m.klog_size), ("vlog", m.vlog_size)] {
                    if ext == "vlog" && size == 0 {
                        continue;
                    }
                    let src = scratch.0.join(format!("cf-{}/{}.{ext}", cfm.name, m.id));
                    uploads.push(Upload {
                        key: table_key(prefix, &cfm.name, m.id, ext),
                        storage: local.clone(),
                        src: utf8(&src)?.to_string(),
                        size,
                    });
                }
            }
        }
        let workers = if opts.upload_concurrency == 0 {
            DEFAULT_UPLOAD_CONCURRENCY
        } else {
            opts.upload_concurrency
        };
        let mut receipts = upload_all(store, &uploads, opts.receipts, workers)?;

        // Only now, with every table object in place, the commit marker.
        let manifest_file = scratch.0.join("MANIFEST");
        plan.manifest.save(&manifest_file)?;
        let manifest_upload = Upload {
            key: join_key(prefix, "MANIFEST"),
            storage: local.clone(),
            src: utf8(&manifest_file)?.to_string(),
            size: std::fs::metadata(&manifest_file)?.len(),
        };
        if let Some(r) = upload_one(store, &manifest_upload, opts.receipts)? {
            receipts.push(r);
        }
        receipts.sort_by_cached_key(|r| receipt_order(prefix, &r.key));

        let mut tables: Vec<CheckpointTable> = plan
            .manifest
            .cfs
            .iter()
            .flat_map(|cfm| {
                cfm.sstables
                    .iter()
                    .map(|m| CheckpointTable::from_meta(&cfm.name, m))
            })
            .collect();
        tables.sort_by(|a, b| (&a.cf, a.id).cmp(&(&b.cf, b.id)));
        Ok(ObjectCheckpoint {
            global_seq: plan.manifest.global_seq,
            next_file_id: plan.manifest.next_file_id,
            tables,
            receipts,
        })
    }
}

/// Download the checkpoint MANIFEST under `prefix` into `dst` (a local file,
/// written temp + fsync + rename) and decode it. A prefix with no MANIFEST is
/// [`OndaError::NotFound`]: no checkpoint was ever committed there.
fn fetch_checkpoint_manifest(
    store: &dyn Storage,
    prefix: &str,
    dst: &Path,
) -> Result<crate::manifest::Manifest> {
    let key = join_key(prefix, "MANIFEST");
    let bytes = match read_all(store, &key, None) {
        Ok(b) => b,
        Err(e) if crate::storage::is_not_found(&e) => return Err(OndaError::NotFound),
        Err(e) => return Err(e),
    };
    write_file_durably(dst, &bytes)?;
    // `Manifest::load` verifies the whole-file CRC: a torn or foreign object is
    // Corruption, never a silently empty catalog (it is non-empty by now).
    crate::manifest::Manifest::load(dst)
}

/// Write `data` to `path` via a sibling temp file, fsync, rename, dir fsync.
fn write_file_durably(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("download");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    crate::util::sync_parent_dir(path)
}

/// Download a checkpoint written by [`DB::checkpoint_to_object_store`] into
/// `dir`, producing an ordinary database directory ([`DB::open`] it).
///
/// The MANIFEST is fetched **first** — it says which objects make up the
/// checkpoint, and a prefix without one is [`OndaError::NotFound`] ("no
/// checkpoint here", not corruption) — but written into `dir` **last**, after
/// every table is durably in place, so an interrupted restore leaves a
/// directory with no MANIFEST rather than one naming missing tables. Each
/// downloaded table is checked against the size the MANIFEST records. `dir`
/// must not already contain a MANIFEST.
pub fn restore_from_object_store(
    store: &dyn Storage,
    prefix: &str,
    dir: impl AsRef<Path>,
) -> Result<()> {
    let dir = dir.as_ref();
    let final_manifest = crate::manifest::manifest_path(dir);
    if final_manifest.exists() {
        return Err(OndaError::Exists(format!(
            "{} already holds a database",
            dir.display()
        )));
    }
    std::fs::create_dir_all(dir)?;
    let staged = dir.join(".restore-MANIFEST");
    let result = (|| -> Result<()> {
        let mut manifest = fetch_checkpoint_manifest(store, prefix, &staged)?;
        let mut rewrite = false;
        for cfm in &mut manifest.cfs {
            let cf_dir = dir.join(format!("cf-{}", cfm.name));
            std::fs::create_dir_all(&cf_dir)?;
            for m in &mut cfm.sstables {
                for (ext, size) in [("klog", m.klog_size), ("vlog", m.vlog_size)] {
                    if ext == "vlog" && size == 0 {
                        continue;
                    }
                    let key = table_key(prefix, &cfm.name, m.id, ext);
                    let data = read_all(store, &key, None)?;
                    if data.len() as u64 != size {
                        return Err(OndaError::Corruption(format!(
                            "checkpoint object {key} holds {} bytes, the MANIFEST says {size}",
                            data.len()
                        )));
                    }
                    write_file_durably(&cf_dir.join(format!("{}.{ext}", m.id)), &data)?;
                }
                // Our checkpoints never carry placement, but a table that
                // names a tier here would resolve somewhere this directory
                // does not have.
                if m.tier.is_some() || m.object.is_some() {
                    m.tier = None;
                    m.object = None;
                    rewrite = true;
                }
            }
        }
        if rewrite {
            manifest.save(&final_manifest)?;
            let _ = std::fs::remove_file(&staged);
        } else {
            std::fs::rename(&staged, &final_manifest)?;
            crate::util::sync_parent_dir(&final_manifest)?;
        }
        Ok(())
    })();
    if result.is_err() {
        // This call's own scratch file; no database is open on `dir`.
        let _ = std::fs::remove_file(&staged);
    }
    result
}

/// A [`Storage`] that answers `size()` for known objects from a table of sizes
/// instead of asking the backend. A remote checkpoint's MANIFEST already records
/// every table's exact klog/vlog size, so opening a reader need not cost a HEAD
/// per table (wavesdb `c148bc0`). Everything else delegates.
#[derive(Debug)]
struct SeededSizeStorage {
    inner: Arc<dyn Storage>,
    sizes: HashMap<String, u64>,
}

struct SizedHandle {
    inner: Arc<dyn crate::storage::ReadHandle>,
    size: u64,
}

impl crate::storage::ReadHandle for SizedHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn size(&self) -> Result<u64> {
        Ok(self.size)
    }
}

impl Storage for SeededSizeStorage {
    fn open_read(&self, path: &str) -> Result<Arc<dyn crate::storage::ReadHandle>> {
        let handle = self.inner.open_read(path)?;
        Ok(match self.sizes.get(path) {
            Some(&size) => Arc::new(SizedHandle {
                inner: handle,
                size,
            }),
            None => handle,
        })
    }
    fn create(&self, path: &str) -> Result<Box<dyn crate::storage::StorageWriter>> {
        Err(OndaError::ReadOnly(format!(
            "remote checkpoint objects are immutable ({path})"
        )))
    }
    fn ensure_dir(&self, _dir: &str) -> Result<()> {
        Ok(())
    }
    fn delete(&self, path: &str) -> Result<()> {
        Err(OndaError::ReadOnly(format!(
            "remote checkpoint objects are immutable ({path})"
        )))
    }
    fn rename(&self, from: &str, _to: &str) -> Result<()> {
        Err(OndaError::ReadOnly(format!(
            "remote checkpoint objects are immutable ({from})"
        )))
    }
    fn list(&self, dir: &str) -> Result<Vec<String>> {
        self.inner.list(dir)
    }
    fn supports_mmap(&self) -> bool {
        false
    }
    fn release(&self, path: &str) {
        self.inner.release(path)
    }
    fn is_read_only(&self) -> bool {
        true
    }
}

/// Open the checkpoint under `prefix` **lazily and read-only**: download only
/// its MANIFEST, and serve every table straight from `store` with range reads
/// through the block cache (a cold block is one read, a warm one none).
///
/// `opts.path` is a caller-owned local directory that receives derived metadata
/// only — a rewritten MANIFEST and empty `cf-<name>` directories — and must not
/// already hold a database. `opts.read_only` must be set. Every table is placed
/// on a shared tier named [`REMOTE_CHECKPOINT_TIER`] rooted at `prefix` (object
/// stem `cf-<cf>/<id>`), and each reader's object size is seeded from the
/// MANIFEST, so opening costs one GET, not a HEAD per table. A prefix without a
/// MANIFEST is [`OndaError::NotFound`].
pub fn open_remote_checkpoint(
    store: Arc<dyn Storage>,
    prefix: &str,
    mut opts: crate::config::Options,
) -> Result<DB> {
    if !opts.read_only {
        return Err(OndaError::InvalidArgs(
            "open_remote_checkpoint requires Options::read_only".into(),
        ));
    }
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        return Err(OndaError::InvalidArgs(
            "open_remote_checkpoint: the checkpoint prefix is required".into(),
        ));
    }
    if opts.tiers.iter().any(|t| t.name == REMOTE_CHECKPOINT_TIER) {
        return Err(OndaError::InvalidArgs(format!(
            "tier name {REMOTE_CHECKPOINT_TIER:?} is reserved by open_remote_checkpoint"
        )));
    }
    let dir = std::path::PathBuf::from(&opts.path);
    let manifest_file = crate::manifest::manifest_path(&dir);
    if manifest_file.exists() {
        return Err(OndaError::Exists(format!(
            "{} already holds a database",
            dir.display()
        )));
    }
    std::fs::create_dir_all(&dir)?;
    let staged = dir.join(".remote-MANIFEST");
    let mut created_dirs = Vec::new();
    let cleanup = |created_dirs: &[std::path::PathBuf]| {
        // This call's own derived files, in a directory that held no database
        // and has no database open on it: never catalogued SSTables.
        let _ = std::fs::remove_file(&staged);
        let _ = std::fs::remove_file(&manifest_file);
        for d in created_dirs {
            let _ = std::fs::remove_dir(d);
        }
    };
    let prepared = (|| -> Result<HashMap<String, u64>> {
        let mut manifest = fetch_checkpoint_manifest(store.as_ref(), prefix, &staged)?;
        let mut sizes = HashMap::new();
        for cfm in &mut manifest.cfs {
            let cf_dir = dir.join(format!("cf-{}", cfm.name));
            if !cf_dir.exists() {
                std::fs::create_dir_all(&cf_dir)?;
                created_dirs.push(cf_dir);
            }
            for m in &mut cfm.sstables {
                let stem = format!("cf-{}/{}", cfm.name, m.id);
                sizes.insert(format!("{prefix}/{stem}.klog"), m.klog_size);
                if m.vlog_size > 0 {
                    sizes.insert(format!("{prefix}/{stem}.vlog"), m.vlog_size);
                }
                m.tier = Some(REMOTE_CHECKPOINT_TIER.to_string());
                m.object = Some(stem);
            }
        }
        manifest.save(&manifest_file)?;
        let _ = std::fs::remove_file(&staged);
        Ok(sizes)
    })();
    let sizes = match prepared {
        Ok(sizes) => sizes,
        Err(e) => {
            cleanup(&created_dirs);
            return Err(e);
        }
    };
    let seeded: Arc<dyn Storage> = Arc::new(SeededSizeStorage {
        inner: store,
        sizes,
    });
    opts.tiers
        .push(crate::config::TierDef::custom(REMOTE_CHECKPOINT_TIER, prefix, seeded).shared());
    match DB::open(opts) {
        Ok(db) => Ok(db),
        Err(e) => {
            cleanup(&created_dirs);
            Err(e)
        }
    }
}
