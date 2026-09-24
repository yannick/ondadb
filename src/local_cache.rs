//! Local disk cache for object-store reads (plan C P8, wavesdb `LocalCachePath`,
//! `f28aecc`).
//!
//! A second cache tier on local disk **below** the in-memory block cache and
//! **in front of** a remote tier's range reads: [`CachedStorage`] wraps a tier's
//! [`Storage`] and serves `read_exact_at` of SSTable objects (`.klog` / `.vlog`)
//! from files under [`Options::local_cache_path`], so a block evicted from
//! memory — or read after a restart — costs a local file read, not a range GET.
//!
//! It is a **cache and nothing more**. Every entry is reconstructible from the
//! object it came from, so every failure here — a missing file, a short read, a
//! full disk, a torn or corrupt entry — degrades to a miss, never to an error.
//! The one thing it must never do is hand back the wrong bytes:
//!
//! - **Every entry is checksummed.** An entry file is
//!   `version u32 | key_len u32 | data_len u64 | key | data | crc32c u32`, the
//!   CRC32-C covering every byte before it. A lookup verifies the length, the
//!   version, the stored key (byte-equal to the one asked for — so even a file
//!   name collision cannot serve another extent) and the CRC; any mismatch drops
//!   the entry and reads through. A torn write (crash between the temp-file
//!   write and the rename, or a rename that survived a power cut without its
//!   data) is exactly such a mismatch. Entries are written to a temp file and
//!   renamed into place, never fsynced: losing one costs a re-fetch.
//! - **Keys name the database incarnation.** A key is `(namespace, object path,
//!   offset, length)`. The namespace is [`Options::read_cache_namespace`] when
//!   set (the caller's promise that every database under that name holds
//!   byte-identical tables under the same ids, as for `ReadResources`), and
//!   otherwise the database directory's canonical path **plus the identity of
//!   its `LOCK` file** (inode and birth time). The LOCK file is created with the
//!   database and never replaced, so a directory wiped and re-created — whose
//!   table ids restart at 1 and may overwrite old objects at the same keys —
//!   is a different namespace.
//! - **Only immutable objects are cached.** Table objects are written once
//!   under never-reused ids and never modified; everything else a tier holds
//!   (a remote checkpoint's `MANIFEST`, say) can be rewritten in place, so it
//!   always reads through.
//!
//! The byte bound ([`Options::local_cache_max_bytes`], `0` = unbounded) counts
//! whole entry files and is enforced by evicting least-recently-used entries
//! after each admission. Bookkeeping lives in memory and is rebuilt at open by
//! listing the directory (a two-level hex tree); anything that is not an entry
//! is removed. One [`DiskCache`] serves every database of the process that
//! names the same directory (namespaces keep them apart); two *processes*
//! sharing one directory stay correct — every entry is verified on read — but
//! each accounts only for what it has seen.
//!
//! [`Options::local_cache_path`]: crate::Options::local_cache_path
//! [`Options::local_cache_max_bytes`]: crate::Options::local_cache_max_bytes
//! [`Options::read_cache_namespace`]: crate::Options::read_cache_namespace

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::encoding::checksum;
use crate::error::Result;
use crate::storage::{CreateOutcome, ObjectInfo, PrefixPage, ReadHandle, Storage, StorageWriter};

/// Version of the entry-file layout. Engine-local: the cache lives outside
/// every database directory and is never part of a database's format.
const ENTRY_VERSION: u32 = 1;
/// `version u32 | key_len u32 | data_len u64`.
const ENTRY_HEADER: usize = 16;
/// Trailing CRC32-C.
const ENTRY_TRAILER: usize = 4;
/// Reads larger than this are never admitted: a whole-object copy (a part
/// demotion, a checkpoint) would otherwise flush the working set of blocks.
pub const MAX_ENTRY_BYTES: usize = 4 << 20;

/// What a [`DiskCache`] holds and has done since it was opened in this process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalCacheStats {
    /// Entry files currently accounted.
    pub entries: u64,
    /// Bytes of those files (headers and checksums included).
    pub bytes: u64,
    /// The configured bound (`0` = unbounded).
    pub max_bytes: u64,
    /// Reads served from disk.
    pub hits: u64,
    /// Cacheable reads that went to the backend.
    pub misses: u64,
    /// Entries written.
    pub admits: u64,
    /// Entries removed to honour the bound.
    pub evictions: u64,
    /// Entries found torn or corrupt on lookup (each became a miss).
    pub corrupt: u64,
}

struct Entry {
    size: u64,
    used: u64,
}

#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    /// `used` clock → entry name: the eviction order. A counter, not a time:
    /// it only has to order uses, and it cannot run backwards.
    lru: BTreeMap<u64, String>,
    bytes: u64,
    clock: u64,
}

impl State {
    fn touch(&mut self, name: &str) -> bool {
        self.clock += 1;
        let clock = self.clock;
        match self.entries.get_mut(name) {
            Some(e) => {
                self.lru.remove(&e.used);
                e.used = clock;
                self.lru.insert(clock, name.to_string());
                true
            }
            None => false,
        }
    }

    fn insert(&mut self, name: String, size: u64) {
        self.remove(&name);
        self.clock += 1;
        self.lru.insert(self.clock, name.clone());
        self.entries.insert(
            name,
            Entry {
                size,
                used: self.clock,
            },
        );
        self.bytes += size;
    }

    fn remove(&mut self, name: &str) -> bool {
        match self.entries.remove(name) {
            Some(e) => {
                self.lru.remove(&e.used);
                self.bytes -= e.size;
                true
            }
            None => false,
        }
    }
}

/// A bounded, verified, on-disk cache of object extents. See the module docs.
pub struct DiskCache {
    root: PathBuf,
    max_bytes: AtomicU64,
    state: Mutex<State>,
    hits: AtomicU64,
    misses: AtomicU64,
    admits: AtomicU64,
    evictions: AtomicU64,
    corrupt: AtomicU64,
    tmp_seq: AtomicU64,
}

impl std::fmt::Debug for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("root", &self.root)
            .field("stats", &self.stats())
            .finish()
    }
}

/// Process-wide registry: one `DiskCache` per canonical directory, so two
/// databases naming the same path share one bound and one bookkeeping.
fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<DiskCache>>> {
    static R: std::sync::OnceLock<Mutex<HashMap<PathBuf, Weak<DiskCache>>>> =
        std::sync::OnceLock::new();
    R.get_or_init(Default::default)
}

impl DiskCache {
    /// Open (creating if needed) the cache at `dir`, bounded to `max_bytes`
    /// (`0` = unbounded). An existing directory is adopted: its entries are
    /// still valid, since a key names the database incarnation and the exact
    /// extent. If this process already has the directory open, that instance
    /// is returned with its bound set to `max_bytes`.
    pub fn open(dir: impl AsRef<Path>, max_bytes: u64) -> Result<Arc<DiskCache>> {
        std::fs::create_dir_all(dir.as_ref())?;
        let root = std::fs::canonicalize(dir.as_ref())?;
        let mut reg = registry().lock();
        if let Some(existing) = reg.get(&root).and_then(Weak::upgrade) {
            existing.max_bytes.store(max_bytes, Ordering::Relaxed);
            existing.evict_to_bound();
            return Ok(existing);
        }
        let cache = Arc::new(DiskCache {
            root: root.clone(),
            max_bytes: AtomicU64::new(max_bytes),
            state: Mutex::new(State::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            admits: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            corrupt: AtomicU64::new(0),
            tmp_seq: AtomicU64::new(0),
        });
        cache.scan();
        cache.evict_to_bound();
        reg.retain(|_, w| w.strong_count() > 0);
        reg.insert(root, Arc::downgrade(&cache));
        Ok(cache)
    }

    /// Current counters.
    pub fn stats(&self) -> LocalCacheStats {
        let s = self.state.lock();
        LocalCacheStats {
            entries: s.entries.len() as u64,
            bytes: s.bytes,
            max_bytes: self.max_bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            admits: self.admits.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            corrupt: self.corrupt.load(Ordering::Relaxed),
        }
    }

    /// The directory this cache lives in (canonical).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Rebuild the bookkeeping from the directory. Anything that is not an
    /// entry — a temp file a crash left, a stray file — is removed: it can
    /// never be served and would occupy space the bound does not know about.
    fn scan(&self) {
        let Ok(shards) = std::fs::read_dir(&self.root) else {
            return;
        };
        let mut st = self.state.lock();
        for shard in shards.flatten() {
            let shard_name = shard.file_name().to_string_lossy().into_owned();
            let is_shard = shard_name.len() == 2 && is_hex(&shard_name);
            if !shard.file_type().map(|t| t.is_dir()).unwrap_or(false) || !is_shard {
                continue;
            }
            let Ok(files) = std::fs::read_dir(shard.path()) else {
                continue;
            };
            for f in files.flatten() {
                let file_name = f.file_name().to_string_lossy().into_owned();
                let name = format!("{shard_name}{file_name}");
                let size = f.metadata().map(|m| m.len()).unwrap_or(0);
                if name.len() != 64
                    || !is_hex(&file_name)
                    || size < (ENTRY_HEADER + ENTRY_TRAILER) as u64
                {
                    let _ = std::fs::remove_file(f.path());
                    continue;
                }
                st.insert(name, size);
            }
        }
    }

    fn path_of(&self, name: &str) -> PathBuf {
        self.root.join(&name[..2]).join(&name[2..])
    }

    /// Fill `buf` from the entry for `key`, if one exists and verifies.
    fn lookup(&self, name: &str, key: &[u8], buf: &mut [u8]) -> bool {
        if !self.state.lock().touch(name) {
            return false;
        }
        let bytes = match std::fs::read(self.path_of(name)) {
            Ok(b) => b,
            Err(_) => {
                self.drop_entry(name, true);
                return false;
            }
        };
        if !verify_entry(&bytes, key, buf.len()) {
            self.drop_entry(name, true);
            return false;
        }
        let at = ENTRY_HEADER + key.len();
        buf.copy_from_slice(&bytes[at..at + buf.len()]);
        true
    }

    /// Store `data` as the entry for `key`. Best effort: any IO failure leaves
    /// the cache without the entry and the read unaffected.
    fn admit(&self, name: &str, key: &[u8], data: &[u8]) {
        let dst = self.path_of(name);
        let Some(dir) = dst.parent() else { return };
        if std::fs::create_dir_all(dir).is_err() {
            return;
        }
        let seq = self.tmp_seq.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!("{}.tmp-{}-{seq}", &name[2..], std::process::id()));
        let bytes = encode_entry(key, data);
        let written = std::fs::File::create(&tmp).and_then(|mut f| f.write_all(&bytes));
        // No fsync: a lost or torn entry is a miss, verified away on lookup.
        if written.is_err() || std::fs::rename(&tmp, &dst).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        self.state
            .lock()
            .insert(name.to_string(), bytes.len() as u64);
        self.admits.fetch_add(1, Ordering::Relaxed);
        self.evict_to_bound();
    }

    fn drop_entry(&self, name: &str, corrupt: bool) {
        self.state.lock().remove(name);
        let _ = std::fs::remove_file(self.path_of(name));
        if corrupt {
            self.corrupt.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Evict least-recently-used entries until the bound holds. O(log n) per
    /// victim (the `lru` map), and the unlink happens outside the lock.
    fn evict_to_bound(&self) {
        let max = self.max_bytes.load(Ordering::Relaxed);
        if max == 0 {
            return;
        }
        loop {
            let victim = {
                let mut st = self.state.lock();
                if st.bytes <= max {
                    return;
                }
                let Some((_, name)) = st.lru.pop_first() else {
                    return;
                };
                if let Some(e) = st.entries.remove(&name) {
                    st.bytes -= e.size;
                }
                name
            };
            let _ = std::fs::remove_file(self.path_of(&victim));
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn is_hex(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn encode_entry(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(ENTRY_HEADER + key.len() + data.len() + ENTRY_TRAILER);
    b.extend_from_slice(&ENTRY_VERSION.to_le_bytes());
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(&(data.len() as u64).to_le_bytes());
    b.extend_from_slice(key);
    b.extend_from_slice(data);
    let crc = checksum(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

/// Whether `bytes` is an intact entry for exactly `key` holding `len` bytes.
fn verify_entry(bytes: &[u8], key: &[u8], len: usize) -> bool {
    let want = ENTRY_HEADER + key.len() + len + ENTRY_TRAILER;
    if bytes.len() != want {
        return false;
    }
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    if u32_at(0) != ENTRY_VERSION
        || u32_at(4) as usize != key.len()
        || u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize != len
        || &bytes[ENTRY_HEADER..ENTRY_HEADER + key.len()] != key
    {
        return false;
    }
    let body = want - ENTRY_TRAILER;
    checksum(&bytes[..body]) == u32_at(body)
}

/// `(namespace, path, offset, length)` as the byte key stored in the entry, and
/// the entry's file name (hex SHA-256 of the key).
fn entry_key(ns: &str, path: &str, off: u64, len: usize) -> (Vec<u8>, String) {
    let mut k = Vec::with_capacity(24 + ns.len() + path.len());
    k.extend_from_slice(&(ns.len() as u32).to_le_bytes());
    k.extend_from_slice(ns.as_bytes());
    k.extend_from_slice(&(path.len() as u32).to_le_bytes());
    k.extend_from_slice(path.as_bytes());
    k.extend_from_slice(&off.to_le_bytes());
    k.extend_from_slice(&(len as u64).to_le_bytes());
    let digest = crate::storage::sha256_of(&k);
    let mut name = String::with_capacity(64);
    for b in digest {
        name.push_str(&format!("{b:02x}"));
    }
    (k, name)
}

/// Whether reads of `path` may be cached: SSTable objects only, the one kind
/// of object that is immutable once written.
fn cacheable(path: &str) -> bool {
    path.ends_with(".klog") || path.ends_with(".vlog")
}

/// A [`Storage`] that serves table-object range reads through a [`DiskCache`]
/// and forwards everything else to the wrapped backend unchanged.
pub struct CachedStorage {
    inner: Arc<dyn Storage>,
    cache: Arc<DiskCache>,
    namespace: String,
}

impl std::fmt::Debug for CachedStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedStorage")
            .field("inner", &self.inner)
            .field("cache", &self.cache.root)
            .field("namespace", &self.namespace)
            .finish()
    }
}

impl CachedStorage {
    /// Wrap `inner`, keying every entry under `namespace` (see the module docs
    /// for what a namespace must identify).
    pub fn new(
        inner: Arc<dyn Storage>,
        cache: Arc<DiskCache>,
        namespace: impl Into<String>,
    ) -> Arc<CachedStorage> {
        Arc::new(CachedStorage {
            inner,
            cache,
            namespace: namespace.into(),
        })
    }
}

struct CachedHandle {
    inner: Arc<dyn ReadHandle>,
    cache: Arc<DiskCache>,
    namespace: Arc<str>,
    path: String,
}

impl ReadHandle for CachedHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        if buf.len() > MAX_ENTRY_BYTES {
            return self.inner.read_exact_at(buf, offset);
        }
        let (key, name) = entry_key(&self.namespace, &self.path, offset, buf.len());
        if self.cache.lookup(&name, &key, buf) {
            self.cache.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.cache.misses.fetch_add(1, Ordering::Relaxed);
        // The backend's error is the read's error; only a successful read is
        // admitted, so a failure can never be cached.
        self.inner.read_exact_at(buf, offset)?;
        self.cache.admit(&name, &key, buf);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
}

impl Storage for CachedStorage {
    fn open_read(&self, path: &str) -> Result<Arc<dyn ReadHandle>> {
        let inner = self.inner.open_read(path)?;
        if !cacheable(path) {
            return Ok(inner);
        }
        Ok(Arc::new(CachedHandle {
            inner,
            cache: self.cache.clone(),
            namespace: Arc::from(self.namespace.as_str()),
            path: path.to_string(),
        }))
    }
    fn create(&self, path: &str) -> Result<Box<dyn StorageWriter>> {
        self.inner.create(path)
    }
    fn ensure_dir(&self, dir: &str) -> Result<()> {
        self.inner.ensure_dir(dir)
    }
    fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path)
    }
    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.inner.rename(from, to)
    }
    fn list(&self, dir: &str) -> Result<Vec<String>> {
        self.inner.list(dir)
    }
    fn supports_mmap(&self) -> bool {
        self.inner.supports_mmap()
    }
    fn release(&self, path: &str) {
        self.inner.release(path)
    }
    fn put_object(&self, path: &str, data: &[u8]) -> Result<ObjectInfo> {
        self.inner.put_object(path, data)
    }
    fn create_if_absent(&self, path: &str, data: &[u8]) -> Result<CreateOutcome> {
        self.inner.create_if_absent(path, data)
    }
    fn list_prefixes(&self, prefix: &str, token: Option<&str>, limit: usize) -> Result<PrefixPage> {
        self.inner.list_prefixes(prefix, token, limit)
    }
    fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }
}

/// The namespace a database's entries are keyed under: the caller's
/// `read_cache_namespace`, or the directory's canonical path plus the identity
/// of its `LOCK` file (see the module docs for why the path alone is not
/// enough). Falls back to a per-process random component if the LOCK file's
/// identity cannot be read, which keeps the cache correct and merely
/// unshared across restarts.
pub(crate) fn namespace_for(explicit: Option<&str>, dir: &str) -> Result<String> {
    if let Some(name) = explicit {
        return Ok(format!("name:{name}"));
    }
    let canonical = std::fs::canonicalize(dir)?;
    let lock = canonical.join("LOCK");
    let identity = std::fs::metadata(&lock).ok().and_then(|m| {
        use std::os::unix::fs::MetadataExt as _;
        let born = m
            .created()
            .or_else(|_| m.modified())
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos();
        Some(format!("{}:{}:{born}", m.dev(), m.ino()))
    });
    let identity = identity.unwrap_or_else(|| {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        format!(
            "ephemeral:{}:{}:{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            crate::util::now_nanos()
        )
    });
    Ok(format!("path:{}#{identity}", canonical.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// An in-memory backend that counts the reads that reach it.
    #[derive(Debug, Default)]
    struct MemStorage {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        reads: AtomicUsize,
        fail: Mutex<bool>,
    }

    struct MemHandle {
        store: Arc<MemStorage>,
        path: String,
    }

    impl ReadHandle for MemHandle {
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
            if *self.store.fail.lock() {
                return Err(std::io::Error::other("backend down").into());
            }
            self.store.reads.fetch_add(1, Ordering::SeqCst);
            let objs = self.store.objects.lock();
            let o = objs
                .get(&self.path)
                .ok_or(crate::error::OndaError::NotFound)?;
            let at = offset as usize;
            buf.copy_from_slice(&o[at..at + buf.len()]);
            Ok(())
        }
        fn size(&self) -> Result<u64> {
            Ok(self.store.objects.lock()[&self.path].len() as u64)
        }
    }

    /// `Storage` needs `open_read(&self)` to hand out handles that outlive the
    /// call, so the test store is used through this `Arc` wrapper.
    #[derive(Debug)]
    struct Mem(Arc<MemStorage>);

    impl Storage for Mem {
        fn open_read(&self, path: &str) -> Result<Arc<dyn ReadHandle>> {
            Ok(Arc::new(MemHandle {
                store: self.0.clone(),
                path: path.to_string(),
            }))
        }
        fn create(&self, _: &str) -> Result<Box<dyn StorageWriter>> {
            unimplemented!()
        }
        fn ensure_dir(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn delete(&self, p: &str) -> Result<()> {
            self.0.objects.lock().remove(p);
            Ok(())
        }
        fn rename(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn list(&self, _: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn supports_mmap(&self) -> bool {
            false
        }
        fn release(&self, _: &str) {}
    }

    fn object(n: usize, seed: u8) -> Vec<u8> {
        (0..n)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    fn setup(max: u64) -> (tempfile::TempDir, Arc<MemStorage>, Arc<DiskCache>) {
        let dir = tempfile::tempdir().unwrap();
        let mem = Arc::new(MemStorage::default());
        mem.objects
            .lock()
            .insert("b/1.klog".into(), object(64 << 10, 1));
        mem.objects
            .lock()
            .insert("b/1.vlog".into(), object(64 << 10, 2));
        mem.objects
            .lock()
            .insert("b/MANIFEST".into(), object(4096, 3));
        let cache = DiskCache::open(dir.path().join("c"), max).unwrap();
        (dir, mem, cache)
    }

    fn read(s: &CachedStorage, path: &str, off: u64, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        s.open_read(path)
            .unwrap()
            .read_exact_at(&mut buf, off)
            .unwrap();
        buf
    }

    fn entry_files(cache: &DiskCache) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for shard in std::fs::read_dir(cache.root()).unwrap().flatten() {
            for f in std::fs::read_dir(shard.path()).unwrap().flatten() {
                out.push(f.path());
            }
        }
        out
    }

    #[test]
    fn a_repeat_read_is_served_from_disk() {
        let (_d, mem, cache) = setup(0);
        let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
        let want = object(64 << 10, 1)[4096..8192].to_vec();
        assert_eq!(read(&s, "b/1.klog", 4096, 4096), want);
        assert_eq!(mem.reads.load(Ordering::SeqCst), 1);
        for _ in 0..3 {
            assert_eq!(read(&s, "b/1.klog", 4096, 4096), want);
        }
        assert_eq!(
            mem.reads.load(Ordering::SeqCst),
            1,
            "hits went to the backend"
        );
        let st = cache.stats();
        assert_eq!((st.hits, st.misses, st.admits, st.entries), (3, 1, 1, 1));
        // A different extent of the same object is a different entry.
        read(&s, "b/1.klog", 4096, 100);
        assert_eq!(mem.reads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn non_table_objects_always_read_through() {
        let (_d, mem, cache) = setup(0);
        let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
        for _ in 0..3 {
            read(&s, "b/MANIFEST", 0, 512);
        }
        assert_eq!(mem.reads.load(Ordering::SeqCst), 3);
        assert_eq!(cache.stats().entries, 0);
    }

    #[test]
    fn entries_survive_a_reopen() {
        let (_d, mem, cache) = setup(0);
        let root = cache.root().to_path_buf();
        {
            let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
            read(&s, "b/1.vlog", 0, 1000);
        }
        drop(cache);
        let cache = DiskCache::open(&root, 0).unwrap();
        assert_eq!(cache.stats().entries, 1, "the scan found the entry");
        let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
        assert_eq!(read(&s, "b/1.vlog", 0, 1000), object(64 << 10, 2)[..1000]);
        assert_eq!(mem.reads.load(Ordering::SeqCst), 1, "served after reopen");
    }

    /// A torn or corrupt entry — cut at any byte, or with any byte flipped — is
    /// a miss that reads through and heals, never wrong bytes.
    #[test]
    fn torn_or_corrupt_entries_are_never_served() {
        let (_d, mem, cache) = setup(0);
        let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
        let want = object(64 << 10, 1)[..300].to_vec();
        read(&s, "b/1.klog", 0, 300);
        let file = entry_files(&cache).pop().unwrap();
        let good = std::fs::read(&file).unwrap();
        let mut reads = mem.reads.load(Ordering::SeqCst);
        let mut damaged: Vec<Vec<u8>> = (0..good.len()).map(|cut| good[..cut].to_vec()).collect();
        for i in (0..good.len()).step_by(7) {
            let mut b = good.clone();
            b[i] ^= 0x40;
            damaged.push(b);
        }
        for bytes in damaged {
            std::fs::write(&file, &bytes).unwrap();
            assert_eq!(read(&s, "b/1.klog", 0, 300), want);
            reads += 1;
            assert_eq!(
                mem.reads.load(Ordering::SeqCst),
                reads,
                "damaged entry served"
            );
            // Re-admitted intact by that read.
            assert_eq!(std::fs::read(&file).unwrap(), good);
        }
        assert!(cache.stats().corrupt > 0);
    }

    #[test]
    fn namespaces_never_alias() {
        let (_d, mem, cache) = setup(0);
        let a = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "db-a");
        let a_bytes = read(&a, "b/1.klog", 0, 256);
        // The same path now holds different bytes (another database's object).
        mem.objects
            .lock()
            .insert("b/1.klog".into(), object(64 << 10, 9));
        let b = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "db-b");
        assert_eq!(read(&b, "b/1.klog", 0, 256), object(64 << 10, 9)[..256]);
        assert_eq!(
            read(&a, "b/1.klog", 0, 256),
            a_bytes,
            "a keeps its own entry"
        );
        assert_eq!(cache.stats().entries, 2);
    }

    #[test]
    fn the_byte_bound_evicts_least_recently_used() {
        let entry = (ENTRY_HEADER + ENTRY_TRAILER + 1000) as u64;
        let (_d, mem, cache) = setup(0);
        let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
        let key_len = |off: u64| entry_key("ns", "b/1.klog", off, 1000).0.len() as u64;
        let per = entry + key_len(0);
        // Room for exactly three entries.
        let cache = DiskCache::open(cache.root(), 3 * per).unwrap();
        for i in 0..3u64 {
            read(&s, "b/1.klog", i * 1000, 1000);
        }
        read(&s, "b/1.klog", 0, 1000); // touch the oldest: now most recent
        read(&s, "b/1.klog", 3000, 1000); // evicts offset 1000
        let st = cache.stats();
        assert_eq!(st.entries, 3);
        assert!(st.bytes <= 3 * per, "{st:?}");
        assert_eq!(st.evictions, 1);
        assert_eq!(entry_files(&cache).len(), 3);
        let before = mem.reads.load(Ordering::SeqCst);
        read(&s, "b/1.klog", 0, 1000);
        assert_eq!(
            mem.reads.load(Ordering::SeqCst),
            before,
            "recently used survived"
        );
        read(&s, "b/1.klog", 1000, 1000);
        assert_eq!(
            mem.reads.load(Ordering::SeqCst),
            before + 1,
            "LRU victim was offset 1000"
        );
        // Shrinking the bound on reopen applies at once.
        drop(s);
        let cache = DiskCache::open(cache.root(), per).unwrap();
        assert_eq!(cache.stats().entries, 1);
    }

    #[test]
    fn a_failed_backend_read_is_not_cached() {
        let (_d, mem, cache) = setup(0);
        let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
        *mem.fail.lock() = true;
        let mut buf = vec![0u8; 64];
        assert!(s
            .open_read("b/1.klog")
            .unwrap()
            .read_exact_at(&mut buf, 0)
            .is_err());
        assert_eq!(cache.stats().entries, 0);
        *mem.fail.lock() = false;
        assert_eq!(read(&s, "b/1.klog", 0, 64), object(64 << 10, 1)[..64]);
    }

    #[test]
    fn oversized_reads_bypass_the_cache() {
        let (_d, mem, cache) = setup(0);
        mem.objects
            .lock()
            .insert("b/2.klog".into(), object(MAX_ENTRY_BYTES + 10, 4));
        let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
        read(&s, "b/2.klog", 0, MAX_ENTRY_BYTES + 1);
        assert_eq!(cache.stats().entries, 0);
    }

    #[test]
    fn the_scan_removes_stray_and_temp_files() {
        let (_d, mem, cache) = setup(0);
        let root = cache.root().to_path_buf();
        {
            let s = CachedStorage::new(Arc::new(Mem(mem.clone())), cache.clone(), "ns");
            read(&s, "b/1.klog", 0, 10);
        }
        let shard = entry_files(&cache)
            .pop()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::write(shard.join("abc.tmp-1-2"), b"torn").unwrap();
        std::fs::write(shard.join("zz"), b"stray").unwrap();
        drop(cache);
        let cache = DiskCache::open(&root, 0).unwrap();
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(entry_files(&cache).len(), 1);
    }

    #[test]
    fn one_directory_is_one_cache_per_process() {
        let (_d, _mem, cache) = setup(0);
        let again = DiskCache::open(cache.root(), 0).unwrap();
        assert!(Arc::ptr_eq(&cache, &again));
    }

    #[test]
    fn the_namespace_names_the_incarnation() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().to_str().unwrap();
        std::fs::write(dir.path().join("LOCK"), b"").unwrap();
        let a = namespace_for(None, d).unwrap();
        assert_eq!(a, namespace_for(None, d).unwrap(), "stable across opens");
        // A wiped and re-created database is another incarnation.
        std::fs::remove_file(dir.path().join("LOCK")).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        std::fs::write(dir.path().join("LOCK"), b"").unwrap();
        assert_ne!(a, namespace_for(None, d).unwrap());
        assert_eq!(namespace_for(Some("pub-1"), d).unwrap(), "name:pub-1");
    }
}
