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
use std::sync::Arc;

use crate::cache::FileCache;
use crate::error::Result;

/// A place SSTable files live and are read/written. Every method takes an
/// absolute path (the [`TierRegistry`] builds them from the tier root).
pub trait Storage: Send + Sync + std::fmt::Debug {
    /// Open `path` for `pread`-style reads, returning a shared handle. Local
    /// backends route this through the [`FileCache`] so the open-fd count stays
    /// bounded and one handle serves all concurrent readers.
    fn open_read(&self, path: &str) -> Result<Arc<File>>;
    /// Create (truncating) `path` for writing.
    fn create(&self, path: &str) -> Result<File>;
    /// Remove `path`. A missing file is not an error.
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

impl Storage for LocalStorage {
    fn open_read(&self, path: &str) -> Result<Arc<File>> {
        self.fc.acquire(path)
    }

    fn create(&self, path: &str) -> Result<File> {
        Ok(File::create(path)?)
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
            std::fs::create_dir_all(&root)?;
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
