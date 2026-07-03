//! LRU cache bounding the number of open SSTable file descriptors.
//!
//! Handles are shared as `Arc<File>`; `File::read_at` maps to `pread`, which is
//! safe for concurrent use, so one handle serves all readers.  A handle still
//! held by a reader stays open even if evicted from the cache — the OS file
//! closes when the last `Arc` drops — so eviction never breaks an in-flight
//! read.

use std::fs::File;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::error::Result;

/// Bounds idle open file handles with an LRU policy.
pub struct FileCache {
    inner: Mutex<LruCache<String, Arc<File>>>,
}

impl std::fmt::Debug for FileCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCache").finish()
    }
}

impl FileCache {
    /// Create a cache holding up to `max` handles (clamped to at least 1).
    pub fn new(max: usize) -> FileCache {
        let cap = NonZeroUsize::new(max.max(1)).unwrap();
        FileCache {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Return a shared handle for `path`, opening and caching it on a miss.
    pub fn acquire(&self, path: &str) -> Result<Arc<File>> {
        {
            let mut c = self.inner.lock();
            if let Some(h) = c.get(path) {
                return Ok(h.clone());
            }
        }
        // Open outside the lock to avoid holding it across a syscall.
        let f = Arc::new(File::open(Path::new(path))?);
        let mut c = self.inner.lock();
        if let Some(h) = c.get(path) {
            // Lost a race; reuse the cached handle and drop ours.
            return Ok(h.clone());
        }
        c.put(path.to_string(), f.clone());
        Ok(f)
    }

    /// Drop any cached handle for `path` (used after compaction deletes a file).
    /// Readers still holding an `Arc` keep the file open until they release it.
    pub fn evict(&self, path: &str) {
        self.inner.lock().pop(path);
    }

    /// Number of currently cached handles.
    pub fn num_open(&self) -> usize {
        self.inner.lock().len()
    }

    /// Drop all cached handles.
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::FileExt;

    fn write_file(dir: &Path, name: &str, data: &[u8]) -> String {
        let p = dir.join(name);
        let mut f = File::create(&p).unwrap();
        f.write_all(data).unwrap();
        p.to_str().unwrap().to_string()
    }

    #[test]
    fn acquire_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "a.dat", b"hello world");
        let c = FileCache::new(4);
        let h = c.acquire(&p).unwrap();
        let mut buf = [0u8; 5];
        h.read_at(&mut buf, 6).unwrap();
        assert_eq!(&buf, b"world");
        // Second acquire returns the same underlying handle.
        let h2 = c.acquire(&p).unwrap();
        assert!(Arc::ptr_eq(&h, &h2));
        assert_eq!(c.num_open(), 1);
    }

    #[test]
    fn bounds_open_handles() {
        let dir = tempfile::tempdir().unwrap();
        let c = FileCache::new(2);
        let paths: Vec<String> = (0..5)
            .map(|i| write_file(dir.path(), &format!("f{i}.dat"), b"x"))
            .collect();
        for p in &paths {
            let _ = c.acquire(p).unwrap();
        }
        assert!(c.num_open() <= 2, "num_open={}", c.num_open());
    }

    #[test]
    fn evicted_handle_stays_open_for_reader() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "x.dat", b"abcdef");
        let c = FileCache::new(1);
        let h = c.acquire(&p).unwrap();
        c.evict(&p);
        assert_eq!(c.num_open(), 0);
        // Reader still works via its own Arc.
        let mut buf = [0u8; 3];
        h.read_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"abc");
    }
}
