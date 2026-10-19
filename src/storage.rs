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

/// What a completed whole-object write left behind.
///
/// `sha256` is the digest of the bytes **this process sent**. `store_verified`
/// is the stronger statement: the backend checked what it received against a
/// checksum and would have refused the write had they disagreed, so a caller
/// can treat the object as confirmed without reading it back. `store_checksum`
/// is whatever token the backend reported (an S3 base64 SHA-256 echo), kept so
/// a caller can record it in its own catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    /// Object size in bytes.
    pub size: u64,
    /// SHA-256 of the bytes sent.
    pub sha256: [u8; 32],
    /// The backend's own checksum token for the object, when it returned one.
    pub store_checksum: Option<String>,
    /// Whether the backend verified the received bytes against a checksum.
    pub store_verified: bool,
}

/// The result of [`Storage::create_if_absent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOutcome {
    /// The object did not exist and now holds exactly the bytes sent.
    Created(ObjectInfo),
    /// An object already existed under that name and was left untouched. Its
    /// contents are unknown: the caller decides whether it is acceptable.
    AlreadyExists,
}

/// One page of immediate child "directories" returned by
/// [`Storage::list_prefixes`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrefixPage {
    /// Immediate children of the listed prefix, in lexicographic order. Each is
    /// the listed prefix joined with the child name, with no trailing `/`: a
    /// child `b` of `a` reads `a/b`.
    pub prefixes: Vec<String>,
    /// Resumes the listing where this page stopped; opaque. `None` exactly when
    /// the listing is complete — stop on `None`, never on an empty page (an
    /// object store may return a page of plain objects and no prefixes).
    pub next_token: Option<String>,
}

/// SHA-256 of `data` (used by every default write path to fill
/// [`ObjectInfo::sha256`]).
pub(crate) fn sha256_of(data: &[u8]) -> [u8; 32] {
    use sha2::Digest as _;
    sha2::Sha256::digest(data).into()
}

/// Whether `e` means "no such file/object" on any backend. Local backends say
/// it with `io::ErrorKind::NotFound`; the S3 backend maps a 404 to the same
/// kind so callers need one test, not one per backend.
pub fn is_not_found(e: &crate::error::OndaError) -> bool {
    match e {
        crate::error::OndaError::NotFound => true,
        crate::error::OndaError::Io(io) => io.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

/// A place SSTable files live and are read/written. Every method takes an
/// absolute path (the [`TierRegistry`] builds them from the tier root).
///
/// The methods after [`release`](Storage::release) are optional capabilities
/// with conservative defaults, so a caller-built backend
/// ([`TierDef::custom`](crate::TierDef::custom)) keeps compiling unchanged.
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

    /// Write `data` as the whole object `path` (overwriting) and report what was
    /// stored. The default goes through [`create`](Storage::create), so the
    /// digest is of the bytes sent and nothing is store-verified; the S3 backend
    /// overrides it to have the store check a SHA-256 on arrival.
    fn put_object(&self, path: &str, data: &[u8]) -> Result<ObjectInfo> {
        let mut w = self.create(path)?;
        w.write_all(data)?;
        w.finish()?;
        Ok(ObjectInfo {
            size: data.len() as u64,
            sha256: sha256_of(data),
            store_checksum: None,
            store_verified: false,
        })
    }

    /// Write `data` as `path` only if nothing exists there yet, atomically:
    /// implementations must never emulate this with an unlocked existence check
    /// followed by a write. The default refuses with `InvalidArgs` — a backend
    /// that cannot do it atomically must say so rather than race.
    fn create_if_absent(&self, path: &str, data: &[u8]) -> Result<CreateOutcome> {
        let _ = data;
        Err(crate::error::OndaError::InvalidArgs(format!(
            "storage backend does not support create-if-absent ({path})"
        )))
    }

    /// List the immediate child prefixes ("directories") of `prefix`, one page
    /// at a time. `token` is a [`PrefixPage::next_token`] from an earlier page
    /// or `None` to start; `limit` bounds the page (0 = the backend default).
    ///
    /// [`list`](Storage::list) names the *objects* directly under a directory;
    /// this names the *subtrees*, without walking them — which is what lets a
    /// caller discover checkpoint prefixes in a bucket cheaply. The default
    /// refuses with `InvalidArgs`.
    fn list_prefixes(&self, prefix: &str, token: Option<&str>, limit: usize) -> Result<PrefixPage> {
        let _ = (token, limit);
        Err(crate::error::OndaError::InvalidArgs(format!(
            "storage backend does not support prefix listing ({prefix})"
        )))
    }

    /// Whether this backend refuses every write locally
    /// (`S3Config::read_only` on the S3 backend). Defaults to `false`.
    fn is_read_only(&self) -> bool {
        false
    }
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

    fn create_if_absent(&self, path: &str, data: &[u8]) -> Result<CreateOutcome> {
        // Stage the bytes durably under a unique name, then `link(2)` them into
        // place: link fails with EEXIST atomically when the target exists,
        // which is the create-if-absent the contract demands, and the target is
        // never visible half-written.
        let p = std::path::Path::new(path);
        let parent = p.parent().ok_or_else(|| {
            crate::error::OndaError::InvalidArgs(format!("path {path:?} has no parent"))
        })?;
        std::fs::create_dir_all(parent)?;
        static STAGE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = STAGE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let staged = format!("{path}.create-{}-{seq}", std::process::id());
        let result = (|| -> Result<CreateOutcome> {
            let mut file = File::create(&staged)?;
            file.write_all(data)?;
            file.sync_all()?;
            match std::fs::hard_link(&staged, path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Ok(CreateOutcome::AlreadyExists)
                }
                Err(e) => return Err(e.into()),
            }
            Ok(CreateOutcome::Created(ObjectInfo {
                size: data.len() as u64,
                sha256: sha256_of(data),
                store_checksum: None,
                store_verified: false,
            }))
        })();
        let _ = std::fs::remove_file(&staged);
        if matches!(result, Ok(CreateOutcome::Created(_))) {
            crate::util::sync_parent_dir(p)?;
        }
        result
    }

    fn list_prefixes(&self, prefix: &str, token: Option<&str>, limit: usize) -> Result<PrefixPage> {
        let base = prefix.trim_end_matches('/');
        let mut names = Vec::new();
        match std::fs::read_dir(if base.is_empty() { "/" } else { base }) {
            Ok(rd) => {
                for entry in rd {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        names.push(entry.file_name().to_string_lossy().into_owned());
                    }
                }
            }
            // A prefix with nothing under it lists empty, as on an object store.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        names.sort();
        // The token is the last child returned: resume strictly after it, the
        // same "start after" rule an object store's listing uses.
        let start = match token {
            Some(t) => names.partition_point(|n| n.as_str() <= t),
            None => 0,
        };
        let limit = if limit == 0 { 1000 } else { limit };
        let end = (start + limit).min(names.len());
        let page = &names[start..end];
        Ok(PrefixPage {
            prefixes: page.iter().map(|n| format!("{base}/{n}")).collect(),
            next_token: if end < names.len() {
                page.last().cloned()
            } else {
                None
            },
        })
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
        format!(
            "{}/{}",
            self.root_for(tier),
            crate::format::cf_dir_name(cf_name)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local() -> Arc<LocalStorage> {
        LocalStorage::new(Arc::new(FileCache::new(16)), false)
    }

    #[test]
    fn local_create_if_absent_never_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let s = local();
        let path = format!("{}/sub/obj", dir.path().to_str().unwrap());
        match s.create_if_absent(&path, b"first").unwrap() {
            CreateOutcome::Created(info) => {
                assert_eq!(info.size, 5);
                assert_eq!(info.sha256, sha256_of(b"first"));
            }
            other => panic!("expected Created, got {other:?}"),
        }
        assert_eq!(
            s.create_if_absent(&path, b"second").unwrap(),
            CreateOutcome::AlreadyExists
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"first");
        // No staging file is left behind either way.
        let names = s
            .list(&format!("{}/sub", dir.path().to_str().unwrap()))
            .unwrap();
        assert_eq!(names, vec!["obj".to_string()]);
    }

    #[test]
    fn local_list_prefixes_pages_directories_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        for c in ["c", "a", "e", "b", "d"] {
            std::fs::create_dir_all(format!("{root}/{c}/deeper")).unwrap();
        }
        std::fs::write(format!("{root}/loose"), b"x").unwrap();
        let s = local();
        let mut seen = Vec::new();
        let mut token = None;
        loop {
            let page = s.list_prefixes(root, token.as_deref(), 2).unwrap();
            assert!(page.prefixes.len() <= 2);
            seen.extend(page.prefixes);
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        let want: Vec<String> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|c| format!("{root}/{c}"))
            .collect();
        assert_eq!(seen, want);
        let missing = s
            .list_prefixes(&format!("{root}/nothing"), None, 0)
            .unwrap();
        assert!(missing.prefixes.is_empty() && missing.next_token.is_none());
    }

    #[test]
    fn missing_objects_are_not_found_on_the_local_backend() {
        let dir = tempfile::tempdir().unwrap();
        let s = local();
        let e = s
            .open_read(&format!("{}/nope", dir.path().to_str().unwrap()))
            .err()
            .unwrap();
        assert!(is_not_found(&e), "{e}");
    }
}
