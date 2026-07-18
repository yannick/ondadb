//! Storage substrate: the choke point through which all SSTable file access
//! flows, so a column family can hold its parts on more than one location.
//!
//! Today the only backend is [`LocalStorage`] — a directory on some mount, its
//! open file descriptors bounded by the shared [`FileCache`]. The trait is the
//! seam an object-store (S3) tier slots into later, behind the existing `s3`
//! feature: no mmap, aggressive block-cache use, range reads. All paths handed
//! to a [`Storage`] are absolute; the [`TierRegistry`] owns the mapping from a
//! tier name to its root directory.

use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use crate::cache::FileCache;
use crate::error::Result;

/// A random-access, read-only handle to one stored object. Local backends wrap a
/// shared [`File`] (positional `pread`); the S3 backend issues one HTTP range GET
/// per read. This is the seam that lets the SSTable
/// [`Reader`](crate::sst::reader::Reader) fetch blocks without knowing whether the
/// bytes live on a local mount or in an object store.
pub trait ReadHandle: Send + Sync {
    /// Read exactly `buf.len()` bytes starting at `offset` (positional; no shared
    /// cursor). A short read is an error. On the S3 backend this is a single range
    /// GET of `buf.len()` bytes — because the reader fronts these with its block
    /// cache, a cold data block costs exactly one such GET and a warm one none.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;
    /// Total size of the object in bytes.
    fn size(&self) -> Result<u64>;
    /// The underlying local file, when this handle is backed by one — used only
    /// to mmap it. Object-store handles return `None`, which is exactly why a
    /// remote tier reports `supports_mmap() == false` and never reaches here.
    #[cfg(feature = "mmap-reads")]
    fn as_file(&self) -> Option<&File> {
        None
    }
}

/// A write sink for one stored object, committed by
/// [`finish`](StorageWriter::finish). Local backends stream to a file and fsync on
/// finish; the S3 backend buffers and issues a single-shot PUT on finish (S3
/// objects are written whole — a part file is produced by one compaction or copy
/// and never appended to afterward).
pub trait StorageWriter: Write + Send {
    /// Durably commit the object: fsync file + parent dir (local) or PUT (S3).
    fn finish(self: Box<Self>) -> Result<()>;
}

/// A place SSTable files live and are read/written. Every method takes an
/// absolute path (the [`TierRegistry`] builds them from the tier root).
pub trait Storage: Send + Sync + std::fmt::Debug {
    /// Open `path` for positional reads, returning a shared handle. Local backends
    /// route this through the [`FileCache`] so the open-fd count stays bounded and
    /// one handle serves all concurrent readers; the S3 backend returns a cheap
    /// handle that range-GETs on demand (no network call to open).
    fn open_read(&self, path: &str) -> Result<Arc<dyn ReadHandle>>;
    /// Create (truncating/overwriting) `path` for writing, returning a sink
    /// finalized by [`StorageWriter::finish`].
    fn create(&self, path: &str) -> Result<Box<dyn StorageWriter>>;
    /// Ensure the directory (local) or key namespace (object store) `dir` exists.
    /// Object stores have no directories, so this is a no-op there.
    fn ensure_dir(&self, dir: &str) -> Result<()>;
    /// Remove `path`. A missing file/object is not an error.
    fn delete(&self, path: &str) -> Result<()>;
    /// Rename `from` to `to` within this backend.
    fn rename(&self, from: &str, to: &str) -> Result<()>;
    /// List entry names (not full paths) directly under `dir`.
    fn list(&self, dir: &str) -> Result<Vec<String>>;
    /// Whether readers may mmap files on this backend. `false` forces the
    /// buffered `pread` path (used for slow/remote tiers).
    fn supports_mmap(&self) -> bool;
    /// Drop any cached descriptor for `path` (called when a file is obsoleted or
    /// moved). Readers still holding a handle keep the file open until they drop.
    fn release(&self, path: &str);
}

/// A tier backed by a local filesystem. Open descriptors are bounded by the
/// shared [`FileCache`]; `mmap` records whether this tier permits zero-copy
/// mmap reads (see [`Storage::supports_mmap`]).
pub struct LocalStorage {
    fc: Arc<FileCache>,
    mmap: bool,
}

impl std::fmt::Debug for LocalStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalStorage")
            .field("mmap", &self.mmap)
            .finish()
    }
}

impl LocalStorage {
    /// A local tier sharing `fc`, with mmap reads `enabled`.
    pub fn new(fc: Arc<FileCache>, mmap: bool) -> Arc<LocalStorage> {
        Arc::new(LocalStorage { fc, mmap })
    }
}

/// Positional-read handle over a locally-open file shared via the [`FileCache`].
struct LocalReadHandle(Arc<File>);

impl ReadHandle for LocalReadHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.0.read_exact_at(buf, offset)?;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    #[cfg(feature = "mmap-reads")]
    fn as_file(&self) -> Option<&File> {
        Some(&self.0)
    }
}

/// A file writer that fsyncs the file and its parent directory on finish — the
/// durability contract the part mover relies on before the manifest flip.
struct LocalStorageWriter {
    file: File,
    dir: Option<std::path::PathBuf>,
}

impl Write for LocalStorageWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl StorageWriter for LocalStorageWriter {
    fn finish(self: Box<Self>) -> Result<()> {
        let this = *self;
        this.file.sync_all()?;
        if let Some(dir) = this.dir {
            if let Ok(d) = File::open(&dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }
}

impl Storage for LocalStorage {
    fn open_read(&self, path: &str) -> Result<Arc<dyn ReadHandle>> {
        Ok(Arc::new(LocalReadHandle(self.fc.acquire(path)?)))
    }

    fn create(&self, path: &str) -> Result<Box<dyn StorageWriter>> {
        let p = std::path::Path::new(path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        Ok(Box::new(LocalStorageWriter {
            file,
            dir: p.parent().map(|d| d.to_path_buf()),
        }))
    }

    fn ensure_dir(&self, dir: &str) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        std::fs::rename(from, to)?;
        Ok(())
    }

    fn list(&self, dir: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            out.push(entry.file_name().to_string_lossy().into_owned());
        }
        Ok(out)
    }

    fn supports_mmap(&self) -> bool {
        self.mmap
    }

    fn release(&self, path: &str) {
        self.fc.evict(path);
    }
}

/// One resolved tier: its name, filesystem root, and backend.
struct TierEntry {
    name: String,
    root: String,
    storage: Arc<dyn Storage>,
}

/// Maps a tier name (or `None` = the implicit default tier) to its root
/// directory and [`Storage`] backend. The default tier is the database
/// directory itself, so a table with `tier == None` resolves exactly to the
/// pre-tiering path (`<db_dir>/cf-<name>/<id>.klog`).
#[derive(Debug)]
pub(crate) struct TierRegistry {
    default_root: String,
    default_storage: Arc<dyn Storage>,
    tiers: Vec<TierEntry>,
}

impl std::fmt::Debug for TierEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierEntry")
            .field("name", &self.name)
            .field("root", &self.root)
            .finish()
    }
}

impl TierRegistry {
    /// Build the registry. The default tier is rooted at `default_root` with
    /// `default_storage`; each `(name, root, storage)` is an extra named tier.
    /// The extra tiers' root directories are created up front.
    pub(crate) fn new(
        default_root: String,
        default_storage: Arc<dyn Storage>,
        extra: Vec<(String, String, Arc<dyn Storage>)>,
    ) -> Result<TierRegistry> {
        let mut tiers = Vec::with_capacity(extra.len());
        for (name, root, storage) in extra {
            // Local tiers create their root dir; an object-store tier has no
            // directories, so `ensure_dir` is a no-op there.
            storage.ensure_dir(&root)?;
            tiers.push(TierEntry {
                name,
                root,
                storage,
            });
        }
        Ok(TierRegistry {
            default_root,
            default_storage,
            tiers,
        })
    }

    fn entry(&self, tier: Option<&str>) -> Option<&TierEntry> {
        let name = tier?;
        self.tiers.iter().find(|t| t.name == name)
    }

    /// Root directory for `tier` (the default-tier root when `tier` is `None`
    /// or unknown — an unknown tier degrades to the default rather than losing
    /// the file, and the manifest is the source of truth for where files are).
    pub(crate) fn root_for(&self, tier: Option<&str>) -> &str {
        match self.entry(tier) {
            Some(t) => &t.root,
            None => &self.default_root,
        }
    }

    /// Backend for `tier` (the default backend when `tier` is `None`/unknown).
    pub(crate) fn storage_for(&self, tier: Option<&str>) -> Arc<dyn Storage> {
        match self.entry(tier) {
            Some(t) => t.storage.clone(),
            None => self.default_storage.clone(),
        }
    }

    /// Whether the named tier exists (the default tier, `None`, always does).
    pub(crate) fn is_known(&self, tier: Option<&str>) -> bool {
        tier.is_none() || self.entry(tier).is_some()
    }

    /// The per-CF directory for `tier`: `<root>/cf-<cf_name>`.
    pub(crate) fn cf_dir(&self, tier: Option<&str>, cf_name: &str) -> String {
        format!("{}/cf-{}", self.root_for(tier), cf_name)
    }
}
