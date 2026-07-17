//! S3-backed [`Storage`]: a cold tier whose SSTable parts live in an
//! S3-compatible object store (developed and tested against MinIO).
//!
//! Two properties shape the implementation:
//!
//! - **No mmap; every read is an HTTP range GET.** [`supports_mmap`] is always
//!   `false`, so the SSTable reader takes its buffered `pread` path and, on a
//!   cache miss, calls [`ReadHandle::read_exact_at`] for exactly the bytes of one
//!   data block. That single `read_exact_at` becomes one range GET. The reader's
//!   block cache therefore fronts S3: a cold block is one GET, a warm block is
//!   free, and no query ever downloads a whole file.
//! - **Objects are written whole.** A part file is produced in full by one
//!   compaction output or one part-mover copy and never appended to afterward, so
//!   [`create`] buffers the bytes and issues a single-shot PUT on
//!   [`StorageWriter::finish`], matching S3's write-once object model.
//!
//! rust-s3's API is async and ondaDB runs no async runtime, so this module owns a
//! small multi-thread tokio runtime and `block_on`s each request. The engine's
//! own worker threads (compaction, part mover, point reads) call in
//! synchronously; concurrent `block_on` from several of them is supported by the
//! multi-thread runtime.
//!
//! [`supports_mmap`]: Storage::supports_mmap
//! [`create`]: Storage::create

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use s3::creds::Credentials;
use s3::region::Region;
use s3::Bucket;
use tokio::runtime::Runtime;

use crate::config::S3Config;
use crate::error::{OndaError, Result};
use crate::storage::{ReadHandle, Storage, StorageWriter};

/// Wrap any S3/runtime failure as an I/O error (ondaDB's error taxonomy has no
/// dedicated network variant; the message preserves the operation and cause).
fn s3_err(op: &str, e: impl std::fmt::Display) -> OndaError {
    OndaError::Io(std::io::Error::other(format!("s3 {op}: {e}")))
}

/// True for the 2xx success range (200 OK, 204 No Content, 206 Partial Content).
fn is_ok(code: u16) -> bool {
    (200..300).contains(&code)
}

/// Map a tier-relative path to an S3 object key. Keys never carry a leading `/`
/// (some stores treat `/key` and `key` as distinct); the tier's `root` is already
/// baked into the path by the [`TierRegistry`](crate::storage::TierRegistry).
fn object_key(path: &str) -> String {
    path.strip_prefix('/').unwrap_or(path).to_string()
}

/// Request counters for an [`S3Storage`], shared with every handle and writer it
/// hands out. Cheap atomics — useful for observability of a remote tier, and they
/// let a test assert that a query fetches individual blocks (bounded range GETs)
/// rather than whole files.
#[derive(Debug, Default)]
pub struct S3Metrics {
    /// Number of range GETs issued (one per cold block read).
    pub range_gets: AtomicU64,
    /// Total bytes requested across all range GETs.
    pub range_get_bytes: AtomicU64,
    /// Number of single-shot object PUTs.
    pub puts: AtomicU64,
    /// Number of HEAD requests (one per reader open, for the object size).
    pub heads: AtomicU64,
}

/// A [`Storage`] backend over an S3-compatible object store.
pub struct S3Storage {
    bucket: Arc<Bucket>,
    rt: Arc<Runtime>,
    metrics: Arc<S3Metrics>,
}

impl std::fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.bucket.name())
            .finish()
    }
}

impl S3Storage {
    /// Build an S3 backend from `cfg`. Constructs the bucket client and the
    /// dedicated tokio runtime used to drive its async calls. No network request
    /// is made here — connectivity is exercised on the first read/write.
    pub fn new(cfg: &S3Config) -> Result<Arc<S3Storage>> {
        let region = Region::Custom {
            region: cfg.region.clone(),
            endpoint: cfg.endpoint.clone(),
        };
        let creds = Credentials::new(
            Some(&cfg.access_key),
            Some(&cfg.secret_key),
            None,
            None,
            None,
        )
        .map_err(|e| s3_err("credentials", e))?;
        let bucket = Bucket::new(&cfg.bucket, region, creds).map_err(|e| s3_err("bucket", e))?;
        let bucket = if cfg.path_style {
            bucket.with_path_style()
        } else {
            bucket
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| s3_err("runtime", e))?;
        Ok(Arc::new(S3Storage {
            bucket: Arc::new(*bucket),
            rt: Arc::new(rt),
            metrics: Arc::new(S3Metrics::default()),
        }))
    }

    /// Shared request counters for this backend (see [`S3Metrics`]).
    pub fn metrics(&self) -> Arc<S3Metrics> {
        self.metrics.clone()
    }
}

/// A read handle over one S3 object: cheap to construct (no network), it range-
/// GETs on demand and caches the object size after the first `size()` HEAD.
struct S3ReadHandle {
    bucket: Arc<Bucket>,
    rt: Arc<Runtime>,
    metrics: Arc<S3Metrics>,
    key: String,
    size: Mutex<Option<u64>>,
}

impl ReadHandle for S3ReadHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let want = buf.len();
        if want == 0 {
            return Ok(());
        }
        self.metrics.range_gets.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .range_get_bytes
            .fetch_add(want as u64, Ordering::Relaxed);
        // HTTP byte ranges are inclusive on both ends, and rust-s3 asserts
        // `start < end`, so a 1-byte read would trip the assert; request one
        // extra byte in that case. S3 clamps an over-long range to the object
        // size, so requesting past EOF never fails — we just truncate to `want`.
        let end = offset + (want.max(2) as u64) - 1;
        let data = self
            .rt
            .block_on(self.bucket.get_object_range(&self.key, offset, Some(end)))
            .map_err(|e| s3_err("get_range", e))?;
        if !is_ok(data.status_code()) {
            return Err(s3_err(
                "get_range",
                format!("status {} for {}", data.status_code(), self.key),
            ));
        }
        let bytes = data.as_slice();
        if bytes.len() < want {
            return Err(s3_err(
                "get_range",
                format!("short read on {}: wanted {want} got {}", self.key, bytes.len()),
            ));
        }
        buf.copy_from_slice(&bytes[..want]);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        if let Some(s) = *self.size.lock() {
            return Ok(s);
        }
        self.metrics.heads.fetch_add(1, Ordering::Relaxed);
        let (head, code) = self
            .rt
            .block_on(self.bucket.head_object(&self.key))
            .map_err(|e| s3_err("head", e))?;
        if !is_ok(code) {
            return Err(s3_err("head", format!("status {code} for {}", self.key)));
        }
        let len = head.content_length.unwrap_or(0).max(0) as u64;
        *self.size.lock() = Some(len);
        Ok(len)
    }
}

/// Buffers all writes in memory and PUTs the whole object on [`finish`].
///
/// [`finish`]: StorageWriter::finish
struct S3StorageWriter {
    bucket: Arc<Bucket>,
    rt: Arc<Runtime>,
    metrics: Arc<S3Metrics>,
    key: String,
    buf: Vec<u8>,
}

impl Write for S3StorageWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl StorageWriter for S3StorageWriter {
    fn finish(self: Box<Self>) -> Result<()> {
        let this = *self;
        this.metrics.puts.fetch_add(1, Ordering::Relaxed);
        let resp = this
            .rt
            .block_on(this.bucket.put_object(&this.key, &this.buf))
            .map_err(|e| s3_err("put", e))?;
        if !is_ok(resp.status_code()) {
            return Err(s3_err(
                "put",
                format!("status {} for {}", resp.status_code(), this.key),
            ));
        }
        Ok(())
    }
}

impl Storage for S3Storage {
    fn open_read(&self, path: &str) -> Result<Arc<dyn ReadHandle>> {
        Ok(Arc::new(S3ReadHandle {
            bucket: self.bucket.clone(),
            rt: self.rt.clone(),
            metrics: self.metrics.clone(),
            key: object_key(path),
            size: Mutex::new(None),
        }))
    }

    fn create(&self, path: &str) -> Result<Box<dyn StorageWriter>> {
        Ok(Box::new(S3StorageWriter {
            bucket: self.bucket.clone(),
            rt: self.rt.clone(),
            metrics: self.metrics.clone(),
            key: object_key(path),
            buf: Vec::new(),
        }))
    }

    fn ensure_dir(&self, _dir: &str) -> Result<()> {
        // Object stores have no directories; keys carry their own prefix.
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<()> {
        let resp = self
            .rt
            .block_on(self.bucket.delete_object(object_key(path)))
            .map_err(|e| s3_err("delete", e))?;
        let code = resp.status_code();
        // A missing object (404) is not an error, matching LocalStorage::delete.
        if code == 404 || is_ok(code) {
            Ok(())
        } else {
            Err(s3_err("delete", format!("status {code} for {path}")))
        }
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        // S3 has no rename: server-side copy, then delete the source.
        let code = self
            .rt
            .block_on(
                self.bucket
                    .copy_object_internal(object_key(from), object_key(to)),
            )
            .map_err(|e| s3_err("copy", e))?;
        if !is_ok(code) {
            return Err(s3_err("copy", format!("status {code} for {from} -> {to}")));
        }
        self.delete(from)
    }

    fn list(&self, dir: &str) -> Result<Vec<String>> {
        let mut prefix = object_key(dir);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let results = self
            .rt
            .block_on(self.bucket.list(prefix.clone(), Some("/".to_string())))
            .map_err(|e| s3_err("list", e))?;
        let mut out = Vec::new();
        for page in results {
            for obj in page.contents {
                if let Some(name) = obj.key.strip_prefix(&prefix) {
                    if !name.is_empty() {
                        out.push(name.to_string());
                    }
                }
            }
        }
        Ok(out)
    }

    fn supports_mmap(&self) -> bool {
        false
    }

    fn release(&self, _path: &str) {
        // Nothing to release: handles hold no OS file descriptor.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    use crate::cache::BlockCache;
    use crate::comparator::default_comparator;
    use crate::config::Compression;
    use crate::sst::{Reader, Writer, WriterOptions};
    use crate::storage::Storage;

    /// Build an [`S3Config`] from the environment, or `None` to skip when MinIO
    /// is not configured (so `cargo test --features s3` stays green offline).
    ///
    /// Run against MinIO with:
    /// ```sh
    /// ONDADB_S3_ENDPOINT=http://192.168.65.11:9000 \
    /// ONDADB_S3_KEY=ayu ONDADB_S3_SECRET=ayudevsecret ONDADB_S3_BUCKET=ayu \
    ///   cargo test --features s3 s3_ -- --nocapture --test-threads=1
    /// ```
    fn env_config() -> Option<S3Config> {
        let endpoint = std::env::var("ONDADB_S3_ENDPOINT").ok()?;
        Some(S3Config {
            bucket: std::env::var("ONDADB_S3_BUCKET").unwrap_or_else(|_| "ayu".into()),
            region: std::env::var("ONDADB_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            endpoint,
            access_key: std::env::var("ONDADB_S3_KEY").unwrap_or_else(|_| "ayu".into()),
            secret_key: std::env::var("ONDADB_S3_SECRET").unwrap_or_else(|_| "ayudevsecret".into()),
            path_style: true,
        })
    }

    /// A key prefix unique to this run so parallel/repeated tests never collide.
    fn unique_prefix(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("ondadb-test/{tag}-{nanos}")
    }

    #[test]
    fn s3_roundtrip_create_read_list_rename_delete() {
        let Some(cfg) = env_config() else {
            eprintln!("skipping s3_roundtrip: ONDADB_S3_ENDPOINT not set");
            return;
        };
        let s3 = S3Storage::new(&cfg).unwrap();
        let prefix = unique_prefix("roundtrip");
        let key = format!("{prefix}/obj-a.bin");

        // create -> PUT.
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let mut w = s3.create(&key).unwrap();
        w.write_all(&payload).unwrap();
        w.finish().unwrap();

        // size via HEAD.
        let h = s3.open_read(&key).unwrap();
        assert_eq!(h.size().unwrap(), payload.len() as u64);

        // range GET in the middle returns exactly those bytes.
        let mut mid = vec![0u8; 100];
        h.read_exact_at(&mut mid, 1000).unwrap();
        assert_eq!(mid, &payload[1000..1100]);

        // a 1-byte read (exercises the inclusive-range / assert workaround).
        let mut one = [0u8; 1];
        h.read_exact_at(&mut one, 42).unwrap();
        assert_eq!(one[0], payload[42]);

        // list sees the object under its prefix.
        let names = s3.list(&prefix).unwrap();
        assert!(names.contains(&"obj-a.bin".to_string()), "list: {names:?}");

        // rename (server-side copy + delete).
        let key2 = format!("{prefix}/obj-b.bin");
        s3.rename(&key, &key2).unwrap();
        assert!(s3.open_read(&key).unwrap().size().is_err(), "source gone");
        let hb = s3.open_read(&key2).unwrap();
        assert_eq!(hb.size().unwrap(), payload.len() as u64);

        // delete (and a second delete of a missing object is not an error).
        s3.delete(&key2).unwrap();
        s3.delete(&key2).unwrap();
    }

    #[test]
    fn s3_reader_serves_blocks_via_bounded_range_gets() {
        let Some(cfg) = env_config() else {
            eprintln!("skipping s3_reader: ONDADB_S3_ENDPOINT not set");
            return;
        };
        let s3 = S3Storage::new(&cfg).unwrap();
        let metrics = s3.metrics();
        let prefix = unique_prefix("reader");
        let key = format!("{prefix}/t.klog");

        // Write a multi-block SSTable locally (small blocks so many keys span
        // many blocks), then upload the klog to S3.
        let dir = tempfile::tempdir().unwrap();
        let local_klog = dir.path().join("t.klog");
        let local_klog = local_klog.to_str().unwrap();
        let n = 2000usize;
        let mut writer = Writer::new(
            local_klog,
            WriterOptions {
                compression: Compression::None,
                compression_rules: Vec::new(),
                cmp: default_comparator(),
                enable_bloom: true,
                bloom_fpr: 0.01,
                klog_value_threshold: 512, // inline values -> no vlog
                block_size: 256,
                expected_entries: n,
                use_btree: false,
                restart_interval: 8,
            },
        )
        .unwrap();
        for i in 0..n {
            let k = format!("key{i:06}");
            writer
                .add(k.as_bytes(), b"value", (i + 1) as u64, 0, false, false)
                .unwrap();
        }
        writer.finish().unwrap();

        let bytes = std::fs::read(local_klog).unwrap();
        let file_size = bytes.len() as u64;
        assert!(
            file_size > 16 * 1024,
            "want a large multi-block file for the bound to be meaningful, got {file_size}"
        );

        // Upload via the S3 create/PUT path.
        let mut up = s3.create(&key).unwrap();
        up.write_all(&bytes).unwrap();
        up.finish().unwrap();

        // Open a Reader backed by S3 (mmap off, block cache fronting range GETs).
        let bc = Arc::new(BlockCache::new(1 << 20));
        let reader = Reader::open(&key, s3.clone(), bc.clone(), 7, default_comparator()).unwrap();

        // A single point get must not download the whole file: it costs a HEAD
        // (on open) + a handful of range GETs (footer, index, bloom, one data
        // block), each far smaller than the file.
        let (v, _, found, deleted) = reader.get(b"key001000", u64::MAX, 0).unwrap();
        assert!(found && !deleted);
        assert_eq!(v.unwrap(), b"value");

        let gets = metrics.range_gets.load(Ordering::Relaxed);
        let got_bytes = metrics.range_get_bytes.load(Ordering::Relaxed);
        assert!(gets >= 1, "expected at least one range GET");
        assert!(
            got_bytes < file_size,
            "reader fetched {got_bytes} bytes >= whole file {file_size}: not block-bounded"
        );
        // Reads must be block-sized: the largest single request is the index/bloom
        // block, all far below the file size. A crude ceiling catches a regression
        // to whole-file GETs.
        assert!(
            got_bytes < file_size / 2,
            "range GETs summed to {got_bytes}, more than half the file {file_size}"
        );

        // A warm re-read of the same key hits the block cache: no new range GET.
        let before = metrics.range_gets.load(Ordering::Relaxed);
        let (v2, _, _, _) = reader.get(b"key001000", u64::MAX, 0).unwrap();
        assert_eq!(v2.unwrap(), b"value");
        assert_eq!(
            metrics.range_gets.load(Ordering::Relaxed),
            before,
            "a cached block must not trigger another range GET"
        );

        // Clean up the uploaded object.
        s3.delete(&key).unwrap();
    }
}
