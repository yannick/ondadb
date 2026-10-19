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
use s3::error::S3Error;
use s3::region::Region;
use s3::Bucket;
use tokio::runtime::Runtime;

use crate::config::{S3Config, S3CredentialSource};
use crate::error::{OndaError, Result};
use crate::storage::{CreateOutcome, ObjectInfo, PrefixPage, ReadHandle, Storage, StorageWriter};

/// Wrap any S3/runtime failure as an I/O error (ondaDB's error taxonomy has no
/// dedicated network variant; the message preserves the operation and cause).
fn s3_err(op: &str, e: impl std::fmt::Display) -> OndaError {
    OndaError::Io(std::io::Error::other(format!("s3 {op}: {e}")))
}

/// True for the 2xx success range (200 OK, 204 No Content, 206 Partial Content).
fn is_ok(code: u16) -> bool {
    (200..300).contains(&code)
}

/// Maximum attempts for one idempotent S3 request (1 try + up to 3 retries).
///
/// rust-s3 0.35's tokio backend drives a raw `hyper::Client` (hyper 0.14) with
/// the default keep-alive connection pool and **no retry**. When the store —
/// MinIO here, or the NAT in front of it — closes a pooled idle connection
/// before hyper's own 90 s idle timeout, the next request that reuses that
/// connection dies mid-flight with `hyper::Error(IncompleteMessage)`, whose
/// message is the classic "connection closed before message completed"
/// (hyperium/hyper#2136). A bodied PUT is the most exposed request because
/// hyper 0.14 will not silently replay it. rust-s3 0.35 exposes no hook to tune
/// the pool, so a bounded retry at this layer is the available lever — and it is
/// safe here because **every** request this backend issues is idempotent
/// (whole-object PUT, range GET, HEAD, server-side COPY, DELETE, prefix LIST),
/// so re-issuing a request that never completed cannot double-apply anything.
const S3_MAX_ATTEMPTS: u32 = 4;

/// Backoff before the retry that follows failed `attempt` (1-based): 25, 50,
/// 100 ms. Deterministic and short — the race is a stale-socket reconnect, not
/// server overload, so a brief pause to let a fresh connection open is enough;
/// total added latency across all retries is bounded below ~200 ms.
fn backoff_delay(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(25u64 << (attempt.clamp(1, 3) - 1))
}

/// A transport-level failure that is safe to retry for an **idempotent**
/// request. hyper surfaces the keep-alive reuse race (server dropped a pooled
/// idle connection) as [`S3Error::Hyper`] — "connection closed before message
/// completed"; a reset / broken pipe mid-request arrives as [`S3Error::Io`].
/// Both mean the request did not complete against the store.
///
/// HTTP status failures are deliberately **not** retried here: under rust-s3's
/// `fail-on-err` feature a 4xx/5xx arrives as [`S3Error::HttpFailWithBody`],
/// which this classifier rejects, so a 412 or 404 is answered, not replayed.
/// Credential, region, and XML-decode errors are not transient and fall
/// through to `false`.
fn is_transient(e: &S3Error) -> bool {
    matches!(e, S3Error::Hyper(_) | S3Error::Io(_))
}

/// Bounded-retry driver, factored out from [`with_retry`] so the control flow is
/// testable without a live endpoint. Calls `op` up to `max_attempts` times,
/// retrying only while `is_transient` holds for the returned error, invoking
/// `sleep(attempt)` between a failed attempt and the next one. Returns the last
/// error when attempts are exhausted or the error is not transient.
fn retry_loop<T, E>(
    max_attempts: u32,
    mut op: impl FnMut() -> std::result::Result<T, E>,
    mut is_transient: impl FnMut(&E) -> bool,
    mut sleep: impl FnMut(u32),
) -> std::result::Result<T, E> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match op() {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_attempts && is_transient(&e) => sleep(attempt),
            Err(e) => return Err(e),
        }
    }
}

/// Drive an idempotent S3 request to completion, retrying the hyper keep-alive
/// reuse race up to [`S3_MAX_ATTEMPTS`] times (see [`is_transient`]). `call`
/// must be safe to run more than once — every caller in this module is.
fn with_retry<T>(op: &str, call: impl FnMut() -> std::result::Result<T, S3Error>) -> Result<T> {
    retry_loop(S3_MAX_ATTEMPTS, call, is_transient, |attempt| {
        std::thread::sleep(backoff_delay(attempt))
    })
    .map_err(|e| s3_err(op, e))
}

/// Map a tier-relative path to an S3 object key. Keys never carry a leading `/`
/// (some stores treat `/key` and `key` as distinct); the tier's `root` is already
/// baked into the path by the [`TierRegistry`](crate::storage::TierRegistry).
fn object_key(path: &str) -> String {
    path.strip_prefix('/').unwrap_or(path).to_string()
}

/// The HTTP status carried by an S3 failure, when it was an HTTP status failure.
/// With rust-s3's `fail-on-err` feature a non-2xx response arrives as
/// [`S3Error::HttpFailWithBody`]; older call sites also see it as `Ok(resp)`
/// with a non-2xx code, which they check themselves.
fn http_status(e: &S3Error) -> Option<u16> {
    match e {
        S3Error::HttpFailWithBody(code, _) => Some(*code),
        _ => None,
    }
}

/// An error for a non-2xx `status` on `key`. A 404 becomes
/// `io::ErrorKind::NotFound` — the kind a local backend reports for a missing
/// file — so [`is_not_found`](crate::storage::is_not_found) answers the same on
/// every backend: "no such object" is a normal state (a prefix with no
/// checkpoint yet), not an outage.
fn status_err(op: &str, key: &str, status: u16) -> OndaError {
    if status == 404 {
        OndaError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("s3 {op}: no such object {key}"),
        ))
    } else {
        s3_err(op, format!("status {status} for {key}"))
    }
}

/// `with_retry`, but a 404 answer comes back as an `io::ErrorKind::NotFound`.
fn with_retry_nf<T>(
    op: &str,
    key: &str,
    call: impl FnMut() -> std::result::Result<T, S3Error>,
) -> Result<T> {
    retry_loop(S3_MAX_ATTEMPTS, call, is_transient, |attempt| {
        std::thread::sleep(backoff_delay(attempt))
    })
    .map_err(|e| match http_status(&e) {
        Some(status) => status_err(op, key, status),
        None => s3_err(op, e),
    })
}

/// Standard base64 (RFC 4648, padded) — the encoding S3 uses for
/// `x-amz-checksum-sha256`. Twelve lines here beat a dependency.
pub(crate) fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

// ---- credentials ---------------------------------------------------------

/// Where credential resolution looks things up. Abstracted so the precedence
/// rules are testable without mutating the process environment (which races
/// every other test thread) or touching the network.
pub(crate) trait CredEnv {
    /// An environment variable, `None` when unset **or empty** (an exported
    /// empty variable configures nothing).
    fn var(&self, name: &str) -> Option<String>;
    /// The user's home directory, for `~/.aws/credentials`.
    fn home_dir(&self) -> Option<std::path::PathBuf>;
}

/// The real process environment.
struct ProcessEnv;

impl CredEnv for ProcessEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.is_empty())
    }
    fn home_dir(&self) -> Option<std::path::PathBuf> {
        std::env::var_os("HOME").map(std::path::PathBuf::from)
    }
}

/// Which step produced a credential — reported so tests (and debugging) can see
/// that precedence did what it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CredOrigin {
    Static,
    Anonymous,
    Profile(String),
    Env,
    SharedFile(String),
    WebIdentity,
    InstanceMetadata,
}

fn static_creds(access: String, secret: String, token: Option<String>) -> Credentials {
    Credentials {
        access_key: Some(access),
        secret_key: Some(secret),
        security_token: None,
        session_token: token,
        expiration: None,
    }
}

/// The shared credentials file: `AWS_SHARED_CREDENTIALS_FILE`, else
/// `~/.aws/credentials`.
fn shared_credentials_path(env: &dyn CredEnv) -> Option<std::path::PathBuf> {
    env.var("AWS_SHARED_CREDENTIALS_FILE")
        .map(std::path::PathBuf::from)
        .or_else(|| env.home_dir().map(|h| h.join(".aws").join("credentials")))
}

/// Read one `[section]` of the shared credentials file. `Ok(None)` when the file
/// or the section is absent — the caller decides whether that is an error (a
/// named profile) or a reason to try the next source (the chain).
fn shared_file_creds(env: &dyn CredEnv, section: &str) -> Result<Option<Credentials>> {
    let Some(path) = shared_credentials_path(env) else {
        return Ok(None);
    };
    let conf = match ini::Ini::load_from_file(&path) {
        Ok(conf) => conf,
        Err(ini::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(OndaError::InvalidArgs(format!(
                "s3 credentials: cannot parse {}: {e}",
                path.display()
            )))
        }
    };
    let Some(data) = conf.section(Some(section)) else {
        return Ok(None);
    };
    let (Some(access), Some(secret)) = (
        data.get("aws_access_key_id").filter(|v| !v.is_empty()),
        data.get("aws_secret_access_key").filter(|v| !v.is_empty()),
    ) else {
        return Err(OndaError::InvalidArgs(format!(
            "s3 credentials: profile [{section}] in {} lacks aws_access_key_id/aws_secret_access_key",
            path.display()
        )));
    };
    let token = data
        .get("aws_session_token")
        .or_else(|| data.get("aws_security_token"))
        .filter(|v| !v.is_empty())
        .map(str::to_string);
    Ok(Some(static_creds(
        access.to_string(),
        secret.to_string(),
        token,
    )))
}

/// Resolve the sources that need no network: explicit keys, anonymous, and a
/// named profile. `Ok(None)` means "the default chain" — deferred to first use.
pub(crate) fn resolve_local_credentials(
    source: &S3CredentialSource,
    env: &dyn CredEnv,
) -> Result<Option<(Credentials, CredOrigin)>> {
    Ok(Some(match source {
        S3CredentialSource::Static {
            access_key,
            secret_key,
            session_token,
        } => (
            static_creds(
                access_key.clone(),
                secret_key.clone(),
                session_token.clone(),
            ),
            CredOrigin::Static,
        ),
        S3CredentialSource::Anonymous => (
            Credentials::anonymous().map_err(|e| s3_err("credentials", e))?,
            CredOrigin::Anonymous,
        ),
        S3CredentialSource::Profile(name) => match shared_file_creds(env, name)? {
            Some(c) => (c, CredOrigin::Profile(name.clone())),
            // No fallback, by design: the caller asked for one identity.
            None => {
                return Err(OndaError::InvalidArgs(format!(
                    "s3 credentials: profile {name:?} not found in {}",
                    shared_credentials_path(env)
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "<no credentials file>".into())
                )))
            }
        },
        S3CredentialSource::DefaultChain => return Ok(None),
    }))
}

/// The non-network half of the default chain: environment, then the shared
/// credentials file. Split out so it is testable offline.
pub(crate) fn chain_local_credentials(
    env: &dyn CredEnv,
) -> Result<Option<(Credentials, CredOrigin)>> {
    if let (Some(access), Some(secret)) = (
        env.var("AWS_ACCESS_KEY_ID"),
        env.var("AWS_SECRET_ACCESS_KEY"),
    ) {
        let token = env
            .var("AWS_SESSION_TOKEN")
            .or_else(|| env.var("AWS_SECURITY_TOKEN"));
        return Ok(Some((static_creds(access, secret, token), CredOrigin::Env)));
    }
    let section = env.var("AWS_PROFILE").unwrap_or_else(|| "default".into());
    if let Some(c) = shared_file_creds(env, &section)? {
        return Ok(Some((c, CredOrigin::SharedFile(section))));
    }
    Ok(None)
}

/// The full default chain. The last two steps are network calls (STS and the
/// instance metadata service); aws-creds bounds them with its own request
/// timeout and skips IMDS outright off EC2/ECS.
fn chain_credentials(env: &dyn CredEnv) -> Result<(Credentials, CredOrigin)> {
    if let Some(found) = chain_local_credentials(env)? {
        return Ok(found);
    }
    if let (Some(role), Some(token_file)) = (
        env.var("AWS_ROLE_ARN"),
        env.var("AWS_WEB_IDENTITY_TOKEN_FILE"),
    ) {
        let token = std::fs::read_to_string(&token_file)?;
        let session = env
            .var("AWS_ROLE_SESSION_NAME")
            .unwrap_or_else(|| "ondadb".into());
        let c = Credentials::from_sts(&role, &session, token.trim())
            .map_err(|e| s3_err("credentials (web identity)", e))?;
        return Ok((c, CredOrigin::WebIdentity));
    }
    match Credentials::from_instance_metadata_v2()
        .or_else(|_| Credentials::from_instance_metadata())
    {
        Ok(c) => Ok((c, CredOrigin::InstanceMetadata)),
        Err(e) => Err(s3_err(
            "credentials",
            format!(
                "no credentials configured and none found in the environment, the shared \
                 credentials file, web identity or instance metadata ({e})"
            ),
        )),
    }
}

// ---- the backend ---------------------------------------------------------

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
    /// Number of LIST requests (object and prefix listings, one per page).
    pub lists: AtomicU64,
}

/// State shared by an [`S3Storage`] and every handle/writer it hands out.
struct S3Inner {
    cfg: S3Config,
    source: S3CredentialSource,
    /// Built on first use when the credential source is the default chain, so
    /// construction never blocks on (or fails for want of) a metadata service.
    /// A failed resolution is not cached: the next request tries again.
    bucket: Mutex<Option<Arc<Bucket>>>,
    rt: Runtime,
    metrics: Arc<S3Metrics>,
}

impl S3Inner {
    fn build_bucket(&self, creds: Credentials) -> Result<Arc<Bucket>> {
        let region = Region::Custom {
            region: self.cfg.region.clone(),
            endpoint: self.cfg.endpoint.clone(),
        };
        let bucket =
            Bucket::new(&self.cfg.bucket, region, creds).map_err(|e| s3_err("bucket", e))?;
        let bucket = if self.cfg.path_style {
            bucket.with_path_style()
        } else {
            bucket
        };
        Ok(Arc::new(*bucket))
    }

    fn bucket(&self) -> Result<Arc<Bucket>> {
        let mut slot = self.bucket.lock();
        if let Some(b) = slot.as_ref() {
            return Ok(b.clone());
        }
        let (creds, _origin) = match resolve_local_credentials(&self.source, &ProcessEnv)? {
            Some(found) => found,
            None => chain_credentials(&ProcessEnv)?,
        };
        let b = self.build_bucket(creds)?;
        *slot = Some(b.clone());
        Ok(b)
    }

    fn refuse_write(&self, op: &str, path: &str) -> Result<()> {
        if self.cfg.read_only {
            return Err(OndaError::ReadOnly(format!(
                "s3 {op} {path}: bucket {} is configured read_only",
                self.cfg.bucket
            )));
        }
        Ok(())
    }

    /// PUT `data` at `key` with a SHA-256 the store checks on arrival.
    ///
    /// `x-amz-checksum-sha256` makes S3 hash the bytes that actually arrived and
    /// refuse the write if the digest disagrees, so a successful PUT is the
    /// store's own statement that it holds exactly these bytes — the guarantee a
    /// read-back would give, for none of the transfer. rust-s3 also sends the
    /// payload's hex SHA-256 as the signed `x-amz-content-sha256` and a
    /// `Content-MD5`, both of which S3 verifies too; the checksum header is what
    /// makes the store *echo* a digest we can confirm.
    ///
    /// `if_none_match` adds `If-None-Match: *` (create-if-absent). `Ok(None)`
    /// then means the object already existed (HTTP 412, or 409 from a store
    /// that reports a concurrent conditional write as a conflict).
    fn put_checked(
        &self,
        key: &str,
        data: &[u8],
        if_none_match: bool,
    ) -> Result<Option<ObjectInfo>> {
        let sha256 = crate::storage::sha256_of(data);
        let encoded = base64_encode(&sha256);
        let mut bucket = (*self.bucket()?).clone();
        let headers = bucket.extra_headers_mut();
        headers.insert(
            "x-amz-checksum-sha256",
            encoded.parse().map_err(|e| s3_err("put", e))?,
        );
        if if_none_match {
            headers.insert("if-none-match", "*".parse().map_err(|e| s3_err("put", e))?);
        }
        self.metrics.puts.fetch_add(1, Ordering::Relaxed);
        let resp = retry_loop(
            S3_MAX_ATTEMPTS,
            || self.rt.block_on(bucket.put_object(key, data)),
            is_transient,
            |attempt| std::thread::sleep(backoff_delay(attempt)),
        );
        let resp = match resp {
            Ok(resp) if is_ok(resp.status_code()) => resp,
            Ok(resp) if if_none_match && matches!(resp.status_code(), 409 | 412) => {
                return Ok(None)
            }
            Err(e) if if_none_match && matches!(http_status(&e), Some(409 | 412)) => {
                return Ok(None)
            }
            Ok(resp) => {
                return Err(s3_err(
                    "put",
                    format!("status {} for {key}", resp.status_code()),
                ))
            }
            Err(e) => return Err(s3_err("put", e)),
        };
        // S3 echoes the digest it computed. Disagreeing means the object it
        // stored is not the object that was sent.
        let echoed = resp.headers().get("x-amz-checksum-sha256").cloned();
        if let Some(echo) = &echoed {
            if echo != &encoded {
                return Err(OndaError::Corruption(format!(
                    "s3 put {key}: store recorded checksum {echo}, sent {encoded}"
                )));
            }
        }
        Ok(Some(ObjectInfo {
            size: data.len() as u64,
            sha256,
            store_verified: echoed.is_some(),
            store_checksum: echoed,
        }))
    }
}

/// A [`Storage`] backend over an S3-compatible object store.
pub struct S3Storage {
    inner: Arc<S3Inner>,
}

impl std::fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.inner.cfg.bucket)
            .field("read_only", &self.inner.cfg.read_only)
            .finish()
    }
}

impl S3Storage {
    /// Build an S3 backend from `cfg`. Constructs the dedicated tokio runtime
    /// used to drive rust-s3's async calls and resolves the credentials that
    /// need no network (explicit keys, anonymous, a named profile — so a
    /// misspelled profile fails here, not on the first read). **No network
    /// request is made**: the bucket is never probed or created, and the
    /// default credential chain is resolved on the first request.
    pub fn new(cfg: &S3Config) -> Result<Arc<S3Storage>> {
        let source = cfg.credential_source()?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| s3_err("runtime", e))?;
        let inner = S3Inner {
            cfg: cfg.clone(),
            source,
            bucket: Mutex::new(None),
            rt,
            metrics: Arc::new(S3Metrics::default()),
        };
        if let Some((creds, _)) = resolve_local_credentials(&inner.source, &ProcessEnv)? {
            let bucket = inner.build_bucket(creds)?;
            *inner.bucket.lock() = Some(bucket);
        }
        Ok(Arc::new(S3Storage {
            inner: Arc::new(inner),
        }))
    }

    /// Shared request counters for this backend (see [`S3Metrics`]).
    pub fn metrics(&self) -> Arc<S3Metrics> {
        self.inner.metrics.clone()
    }

    /// Whether this backend was configured [`read_only`](S3Config::read_only).
    pub fn read_only(&self) -> bool {
        self.inner.cfg.read_only
    }
}

/// A read handle over one S3 object: cheap to construct (no network), it range-
/// GETs on demand and caches the object size after the first `size()` HEAD.
struct S3ReadHandle {
    inner: Arc<S3Inner>,
    key: String,
    size: Mutex<Option<u64>>,
}

impl ReadHandle for S3ReadHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let want = buf.len();
        if want == 0 {
            return Ok(());
        }
        let bucket = self.inner.bucket()?;
        let metrics = &self.inner.metrics;
        metrics.range_gets.fetch_add(1, Ordering::Relaxed);
        metrics
            .range_get_bytes
            .fetch_add(want as u64, Ordering::Relaxed);
        // HTTP byte ranges are inclusive on both ends, and rust-s3 asserts
        // `start < end`, so a 1-byte read would trip the assert; request one
        // extra byte in that case. S3 clamps an over-long range to the object
        // size, so requesting past EOF never fails — we just truncate to `want`.
        let end = offset + (want.max(2) as u64) - 1;
        let data = with_retry_nf("get_range", &self.key, || {
            self.inner
                .rt
                .block_on(bucket.get_object_range(&self.key, offset, Some(end)))
        })?;
        if !is_ok(data.status_code()) {
            return Err(status_err("get_range", &self.key, data.status_code()));
        }
        let bytes = data.as_slice();
        if bytes.len() < want {
            return Err(s3_err(
                "get_range",
                format!(
                    "short read on {}: wanted {want} got {}",
                    self.key,
                    bytes.len()
                ),
            ));
        }
        buf.copy_from_slice(&bytes[..want]);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        if let Some(s) = *self.size.lock() {
            return Ok(s);
        }
        let bucket = self.inner.bucket()?;
        self.inner.metrics.heads.fetch_add(1, Ordering::Relaxed);
        let (head, code) = with_retry_nf("head", &self.key, || {
            self.inner.rt.block_on(bucket.head_object(&self.key))
        })?;
        if !is_ok(code) {
            return Err(status_err("head", &self.key, code));
        }
        let len = head.content_length.unwrap_or(0).max(0) as u64;
        *self.size.lock() = Some(len);
        Ok(len)
    }
}

/// Buffers all writes in memory and PUTs the whole object on [`finish`], with a
/// store-verified SHA-256 (see `S3Inner::put_checked`).
///
/// [`finish`]: StorageWriter::finish
struct S3StorageWriter {
    inner: Arc<S3Inner>,
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
        this.inner.put_checked(&this.key, &this.buf, false)?;
        Ok(())
    }
}

impl Storage for S3Storage {
    fn open_read(&self, path: &str) -> Result<Arc<dyn ReadHandle>> {
        Ok(Arc::new(S3ReadHandle {
            inner: self.inner.clone(),
            key: object_key(path),
            size: Mutex::new(None),
        }))
    }

    fn create(&self, path: &str) -> Result<Box<dyn StorageWriter>> {
        // Refused here, before a byte is buffered: a store that accepted the
        // write and failed at the network would report a configuration mistake
        // as an outage.
        self.inner.refuse_write("create", path)?;
        Ok(Box::new(S3StorageWriter {
            inner: self.inner.clone(),
            key: object_key(path),
            buf: Vec::new(),
        }))
    }

    fn ensure_dir(&self, _dir: &str) -> Result<()> {
        // Object stores have no directories; keys carry their own prefix.
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.inner.refuse_write("delete", path)?;
        let bucket = self.inner.bucket()?;
        let key = object_key(path);
        let resp = retry_loop(
            S3_MAX_ATTEMPTS,
            || self.inner.rt.block_on(bucket.delete_object(&key)),
            is_transient,
            |attempt| std::thread::sleep(backoff_delay(attempt)),
        );
        let code = match resp {
            Ok(resp) => resp.status_code(),
            Err(e) => match http_status(&e) {
                Some(code) => code,
                None => return Err(s3_err("delete", e)),
            },
        };
        // A missing object (404) is not an error, matching LocalStorage::delete.
        if code == 404 || is_ok(code) {
            Ok(())
        } else {
            Err(s3_err("delete", format!("status {code} for {path}")))
        }
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.inner.refuse_write("rename", from)?;
        let bucket = self.inner.bucket()?;
        // S3 has no rename: server-side copy, then delete the source.
        let (from_key, to_key) = (object_key(from), object_key(to));
        let code = with_retry_nf("copy", &from_key, || {
            self.inner
                .rt
                .block_on(bucket.copy_object_internal(&from_key, &to_key))
        })?;
        if !is_ok(code) {
            return Err(s3_err("copy", format!("status {code} for {from} -> {to}")));
        }
        self.delete(from)
    }

    fn list(&self, dir: &str) -> Result<Vec<String>> {
        let bucket = self.inner.bucket()?;
        let mut prefix = object_key(dir);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        self.inner.metrics.lists.fetch_add(1, Ordering::Relaxed);
        let results = with_retry("list", || {
            self.inner
                .rt
                .block_on(bucket.list(prefix.clone(), Some("/".to_string())))
        })?;
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

    fn put_object(&self, path: &str, data: &[u8]) -> Result<ObjectInfo> {
        self.inner.refuse_write("put", path)?;
        let key = object_key(path);
        self.inner
            .put_checked(&key, data, false)?
            .ok_or_else(|| s3_err("put", format!("unexpected precondition failure for {key}")))
    }

    fn create_if_absent(&self, path: &str, data: &[u8]) -> Result<CreateOutcome> {
        self.inner.refuse_write("create_if_absent", path)?;
        let key = object_key(path);
        Ok(match self.inner.put_checked(&key, data, true)? {
            Some(info) => CreateOutcome::Created(info),
            None => CreateOutcome::AlreadyExists,
        })
    }

    fn list_prefixes(&self, prefix: &str, token: Option<&str>, limit: usize) -> Result<PrefixPage> {
        let bucket = self.inner.bucket()?;
        // Listing is a read, so a read-only store still lists.
        let base = prefix.trim_end_matches('/');
        let mut listed = object_key(base);
        if !listed.is_empty() {
            listed.push('/');
        }
        self.inner.metrics.lists.fetch_add(1, Ordering::Relaxed);
        let (page, code) = with_retry("list_prefixes", || {
            self.inner.rt.block_on(bucket.list_page(
                listed.clone(),
                Some("/".to_string()),
                token.map(str::to_string),
                None,
                (limit > 0).then_some(limit),
            ))
        })?;
        if !is_ok(code) {
            return Err(s3_err(
                "list_prefixes",
                format!("status {code} for {prefix}"),
            ));
        }
        let mut prefixes: Vec<String> = page
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|cp| {
                let child = cp.prefix.strip_prefix(&listed)?.trim_end_matches('/');
                (!child.is_empty()).then(|| {
                    if base.is_empty() {
                        child.to_string()
                    } else {
                        format!("{base}/{child}")
                    }
                })
            })
            .collect();
        prefixes.sort();
        // ListObjectsV2 hands back a real continuation token (v1 stores a
        // marker there); either resumes exactly where this page stopped.
        let next_token = if page.is_truncated {
            page.next_continuation_token
        } else {
            None
        };
        Ok(PrefixPage {
            prefixes,
            next_token,
        })
    }

    fn is_read_only(&self) -> bool {
        self.inner.cfg.read_only
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
            ..S3Config::default()
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
                bloom_fpr: Some(0.01),
                klog_value_threshold: 512, // inline values -> no vlog
                block_size: 256,
                expected_entries: n,
                use_btree: false,
                restart_interval: 8,
                extended_entries: false,
                prefix_delta: false,
            },
        )
        .unwrap();
        for i in 0..n {
            let k = format!("key{i:06}");
            writer
                .add(
                    k.as_bytes(),
                    b"value",
                    (i + 1) as u64,
                    0,
                    crate::format::KIND_PUT,
                )
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
        let reader =
            Reader::open(&key, s3.clone(), bc.clone(), 7, default_comparator(), 0).unwrap();

        // A single point get must not download the whole file: it costs a HEAD
        // (on open) + a handful of range GETs (footer, index, bloom, one data
        // block), each far smaller than the file.
        let (v, _, found, deleted, _) = reader.get(b"key001000", u64::MAX, 0).unwrap();
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
        let (v2, _, _, _, _) = reader.get(b"key001000", u64::MAX, 0).unwrap();
        assert_eq!(v2.unwrap(), b"value");
        assert_eq!(
            metrics.range_gets.load(Ordering::Relaxed),
            before,
            "a cached block must not trigger another range GET"
        );

        // Clean up the uploaded object.
        s3.delete(&key).unwrap();
    }

    /// Canary for the keep-alive reuse race the bounded retry guards
    /// ([`with_retry`]): drive many sequential PUT/HEAD/GET/DELETE requests over
    /// the shared bucket client so hyper's connection pool is reused across each.
    /// A pooled connection the store closes between requests would surface as
    /// "connection closed before message completed"; the retry absorbs it, so
    /// this must stay green. Env-gated like the other MinIO tests.
    #[test]
    fn s3_repeated_requests_reuse_connections() {
        let Some(cfg) = env_config() else {
            eprintln!("skipping s3_repeated_requests: ONDADB_S3_ENDPOINT not set");
            return;
        };
        let s3 = S3Storage::new(&cfg).unwrap();
        let prefix = unique_prefix("reuse");
        let payload: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();
        for i in 0..40 {
            let key = format!("{prefix}/obj-{i:03}.bin");
            let mut w = s3.create(&key).unwrap();
            w.write_all(&payload).unwrap();
            w.finish().unwrap(); // PUT
            let h = s3.open_read(&key).unwrap();
            assert_eq!(h.size().unwrap(), payload.len() as u64); // HEAD
            let mut got = vec![0u8; 64];
            h.read_exact_at(&mut got, 100).unwrap(); // range GET
            assert_eq!(got, &payload[100..164]);
            s3.delete(&key).unwrap(); // DELETE
        }
    }

    /// F8: child-prefix listing keeps the common prefixes `list` drops, pages
    /// with a real continuation token, and never descends into a subtree.
    #[test]
    fn s3_list_prefixes_pages_children_one_level_down() {
        let Some(cfg) = env_config() else {
            eprintln!("skipping s3_list_prefixes: ONDADB_S3_ENDPOINT not set");
            return;
        };
        let s3 = S3Storage::new(&cfg).unwrap();
        let prefix = unique_prefix("prefixes");
        let children = ["alpha", "beta", "delta", "gamma", "omega"];
        for c in children {
            // Two levels deep: the lister must name `c` once, not its contents.
            s3.put_object(&format!("{prefix}/{c}/deep/obj.bin"), b"x")
                .unwrap();
            s3.put_object(&format!("{prefix}/{c}/MANIFEST"), b"m")
                .unwrap();
        }
        // A plain object beside the children is not a prefix.
        s3.put_object(&format!("{prefix}/loose.bin"), b"y").unwrap();

        let mut seen = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0;
        loop {
            let page = s3.list_prefixes(&prefix, token.as_deref(), 2).unwrap();
            pages += 1;
            assert!(page.prefixes.len() <= 2, "page over limit: {page:?}");
            seen.extend(page.prefixes);
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
            assert!(pages < 20, "pagination does not terminate");
        }
        let want: Vec<String> = children.iter().map(|c| format!("{prefix}/{c}")).collect();
        assert_eq!(seen, want);
        assert!(pages >= 3, "limit 2 over 5 children must page, got {pages}");

        // A trailing slash on the prefix changes nothing; an empty subtree lists empty.
        let all = s3.list_prefixes(&format!("{prefix}/"), None, 0).unwrap();
        assert_eq!(all.prefixes, want);
        assert_eq!(all.next_token, None);
        let none = s3
            .list_prefixes(&format!("{prefix}/nothing"), None, 0)
            .unwrap();
        assert!(none.prefixes.is_empty() && none.next_token.is_none());

        for c in children {
            s3.delete(&format!("{prefix}/{c}/deep/obj.bin")).unwrap();
            s3.delete(&format!("{prefix}/{c}/MANIFEST")).unwrap();
        }
        s3.delete(&format!("{prefix}/loose.bin")).unwrap();
    }

    /// F8: uploads carry a SHA-256 the store checks and echoes; create-if-absent
    /// refuses to overwrite; a missing object is `NotFound`, not an outage.
    #[test]
    fn s3_verified_put_and_create_if_absent() {
        let Some(cfg) = env_config() else {
            eprintln!("skipping s3_verified_put: ONDADB_S3_ENDPOINT not set");
            return;
        };
        let s3 = S3Storage::new(&cfg).unwrap();
        let prefix = unique_prefix("verified");
        let key = format!("{prefix}/a.bin");
        let payload = b"verified payload bytes";

        let info = s3.put_object(&key, payload).unwrap();
        assert_eq!(info.size, payload.len() as u64);
        assert_eq!(info.sha256, crate::storage::sha256_of(payload));
        assert!(
            info.store_verified,
            "store must echo the checksum: {info:?}"
        );
        assert_eq!(
            info.store_checksum.as_deref(),
            Some(base64_encode(&info.sha256).as_str())
        );

        let fresh = format!("{prefix}/b.bin");
        match s3.create_if_absent(&fresh, b"first").unwrap() {
            CreateOutcome::Created(i) => assert!(i.store_verified),
            other => panic!("expected Created, got {other:?}"),
        }
        assert_eq!(
            s3.create_if_absent(&fresh, b"second!").unwrap(),
            CreateOutcome::AlreadyExists
        );
        let h = s3.open_read(&fresh).unwrap();
        assert_eq!(h.size().unwrap(), 5, "the first write must survive");

        let missing = s3.open_read(&format!("{prefix}/missing")).unwrap();
        let e = missing.size().unwrap_err();
        assert!(crate::storage::is_not_found(&e), "want NotFound, got {e}");
        let mut b = [0u8; 4];
        let e = missing.read_exact_at(&mut b, 0).unwrap_err();
        assert!(crate::storage::is_not_found(&e), "want NotFound, got {e}");

        // A read-only view of the same bucket reads and lists but never writes.
        let ro = S3Storage::new(&S3Config {
            read_only: true,
            ..cfg.clone()
        })
        .unwrap();
        assert_eq!(
            ro.open_read(&key).unwrap().size().unwrap(),
            payload.len() as u64
        );
        assert!(ro.list_prefixes(&prefix, None, 0).is_ok());
        assert!(matches!(
            ro.put_object(&key, b"x"),
            Err(OndaError::ReadOnly(_))
        ));

        s3.delete(&key).unwrap();
        s3.delete(&fresh).unwrap();
    }

    // --- Hermetic credential / read-only tests (no network) ---------------

    /// A fake environment: variables and a home directory, nothing global.
    #[derive(Default)]
    struct FakeEnv {
        vars: std::collections::HashMap<String, String>,
        home: Option<std::path::PathBuf>,
    }

    impl FakeEnv {
        fn with(mut self, k: &str, v: &str) -> Self {
            self.vars.insert(k.into(), v.into());
            self
        }
    }

    impl CredEnv for FakeEnv {
        fn var(&self, name: &str) -> Option<String> {
            self.vars.get(name).cloned().filter(|v| !v.is_empty())
        }
        fn home_dir(&self) -> Option<std::path::PathBuf> {
            self.home.clone()
        }
    }

    fn creds_file(dir: &tempfile::TempDir) -> String {
        let path = dir.path().join("credentials");
        std::fs::write(
            &path,
            "[default]\naws_access_key_id = DEF\naws_secret_access_key = defsecret\n\n\
             [work]\naws_access_key_id = WORK\naws_secret_access_key = worksecret\n\
             aws_session_token = worktoken\n\n\
             [broken]\naws_access_key_id = ONLYKEY\n",
        )
        .unwrap();
        path.to_str().unwrap().to_string()
    }

    fn cfg() -> S3Config {
        S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            // Unroutable on purpose: nothing in these tests may touch it.
            endpoint: "http://127.0.0.1:9".into(),
            path_style: true,
            ..S3Config::default()
        }
    }

    #[test]
    fn credential_source_precedence() {
        let keys = S3Config {
            access_key: "AK".into(),
            secret_key: "SK".into(),
            session_token: Some("TOK".into()),
            anonymous: true,
            profile: Some("work".into()),
            ..cfg()
        };
        // Explicit keys beat anonymous and a profile, and carry the token.
        assert_eq!(
            keys.credential_source().unwrap(),
            S3CredentialSource::Static {
                access_key: "AK".into(),
                secret_key: "SK".into(),
                session_token: Some("TOK".into()),
            }
        );
        // Anonymous beats a profile (and therefore the chain).
        let anon = S3Config {
            anonymous: true,
            profile: Some("work".into()),
            ..cfg()
        };
        assert_eq!(
            anon.credential_source().unwrap(),
            S3CredentialSource::Anonymous
        );
        let prof = S3Config {
            profile: Some("work".into()),
            ..cfg()
        };
        assert_eq!(
            prof.credential_source().unwrap(),
            S3CredentialSource::Profile("work".into())
        );
        assert_eq!(
            cfg().credential_source().unwrap(),
            S3CredentialSource::DefaultChain
        );
        // An empty profile name configures nothing.
        let empty_profile = S3Config {
            profile: Some(String::new()),
            ..cfg()
        };
        assert_eq!(
            empty_profile.credential_source().unwrap(),
            S3CredentialSource::DefaultChain
        );
        // Half a key pair, or a token with no keys, is refused, not guessed at.
        for bad in [
            S3Config {
                access_key: "AK".into(),
                ..cfg()
            },
            S3Config {
                secret_key: "SK".into(),
                ..cfg()
            },
            S3Config {
                session_token: Some("TOK".into()),
                ..cfg()
            },
        ] {
            assert!(matches!(
                bad.credential_source(),
                Err(OndaError::InvalidArgs(_))
            ));
        }
    }

    #[test]
    fn static_keys_carry_the_session_token() {
        let src = S3CredentialSource::Static {
            access_key: "AK".into(),
            secret_key: "SK".into(),
            session_token: Some("TOK".into()),
        };
        let (c, origin) = resolve_local_credentials(&src, &FakeEnv::default())
            .unwrap()
            .unwrap();
        assert_eq!(origin, CredOrigin::Static);
        assert_eq!(c.session_token.as_deref(), Some("TOK"));
        assert_eq!(c.access_key.as_deref(), Some("AK"));
    }

    #[test]
    fn anonymous_signs_nothing() {
        let (c, origin) =
            resolve_local_credentials(&S3CredentialSource::Anonymous, &FakeEnv::default())
                .unwrap()
                .unwrap();
        assert_eq!(origin, CredOrigin::Anonymous);
        // rust-s3 omits the Authorization header exactly when there is no secret.
        assert!(c.access_key.is_none() && c.secret_key.is_none());
    }

    #[test]
    fn named_profile_reads_its_section_and_never_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let file = creds_file(&dir);
        let env = FakeEnv::default()
            .with("AWS_SHARED_CREDENTIALS_FILE", &file)
            // Ambient credentials that must NOT answer for a named profile.
            .with("AWS_ACCESS_KEY_ID", "ENVKEY")
            .with("AWS_SECRET_ACCESS_KEY", "envsecret")
            .with("AWS_PROFILE", "default");
        let (c, origin) =
            resolve_local_credentials(&S3CredentialSource::Profile("work".into()), &env)
                .unwrap()
                .unwrap();
        assert_eq!(origin, CredOrigin::Profile("work".into()));
        assert_eq!(c.access_key.as_deref(), Some("WORK"));
        assert_eq!(c.session_token.as_deref(), Some("worktoken"));

        // Missing profile: an error, even though the environment holds keys.
        let e = resolve_local_credentials(&S3CredentialSource::Profile("typo".into()), &env)
            .unwrap_err();
        assert!(e.to_string().contains("typo"), "{e}");
        // A profile with half a key pair is an error too.
        assert!(
            resolve_local_credentials(&S3CredentialSource::Profile("broken".into()), &env).is_err()
        );
        // No credentials file at all: still no fallback.
        let nofile = FakeEnv::default()
            .with(
                "AWS_SHARED_CREDENTIALS_FILE",
                dir.path().join("nope").to_str().unwrap(),
            )
            .with("AWS_ACCESS_KEY_ID", "ENVKEY")
            .with("AWS_SECRET_ACCESS_KEY", "envsecret");
        assert!(
            resolve_local_credentials(&S3CredentialSource::Profile("work".into()), &nofile)
                .is_err()
        );
        // The chain is deferred to first use, never resolved eagerly.
        assert!(
            resolve_local_credentials(&S3CredentialSource::DefaultChain, &env)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn default_chain_order_env_then_shared_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = creds_file(&dir);

        // 1. Environment beats the file.
        let env = FakeEnv::default()
            .with("AWS_ACCESS_KEY_ID", "ENVKEY")
            .with("AWS_SECRET_ACCESS_KEY", "envsecret")
            .with("AWS_SESSION_TOKEN", "envtoken")
            .with("AWS_SHARED_CREDENTIALS_FILE", &file);
        let (c, origin) = chain_local_credentials(&env).unwrap().unwrap();
        assert_eq!(origin, CredOrigin::Env);
        assert_eq!(c.access_key.as_deref(), Some("ENVKEY"));
        assert_eq!(c.session_token.as_deref(), Some("envtoken"));

        // Half an env pair does not count; the file answers instead.
        let half = FakeEnv::default()
            .with("AWS_ACCESS_KEY_ID", "ENVKEY")
            .with("AWS_SHARED_CREDENTIALS_FILE", &file);
        let (_, origin) = chain_local_credentials(&half).unwrap().unwrap();
        assert_eq!(origin, CredOrigin::SharedFile("default".into()));

        // 2. The file, section AWS_PROFILE...
        let prof = FakeEnv::default()
            .with("AWS_SHARED_CREDENTIALS_FILE", &file)
            .with("AWS_PROFILE", "work");
        let (c, origin) = chain_local_credentials(&prof).unwrap().unwrap();
        assert_eq!(origin, CredOrigin::SharedFile("work".into()));
        assert_eq!(c.access_key.as_deref(), Some("WORK"));

        // ...found through $HOME/.aws/credentials when no file is named.
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".aws")).unwrap();
        std::fs::copy(&file, home.path().join(".aws/credentials")).unwrap();
        let via_home = FakeEnv {
            home: Some(home.path().to_path_buf()),
            ..FakeEnv::default()
        };
        let (c, origin) = chain_local_credentials(&via_home).unwrap().unwrap();
        assert_eq!(origin, CredOrigin::SharedFile("default".into()));
        assert_eq!(c.access_key.as_deref(), Some("DEF"));

        // 3. Nothing local: the network steps (STS, IMDS) are next, not here.
        let empty = FakeEnv {
            home: Some(dir.path().join("empty-home")),
            ..FakeEnv::default()
        };
        assert!(chain_local_credentials(&empty).unwrap().is_none());
    }

    #[test]
    fn construction_makes_no_request() {
        // Default chain + unroutable endpoint: construction must still succeed,
        // because neither the bucket nor the chain is touched until first use.
        let s3 = S3Storage::new(&cfg()).unwrap();
        assert_eq!(s3.metrics().puts.load(Ordering::Relaxed), 0);
        // Anonymous resolves offline.
        S3Storage::new(&S3Config {
            anonymous: true,
            ..cfg()
        })
        .unwrap();
        // Half a key pair is refused at construction.
        assert!(S3Storage::new(&S3Config {
            access_key: "AK".into(),
            ..cfg()
        })
        .is_err());
    }

    #[test]
    fn read_only_refuses_every_write_locally() {
        let s3 = S3Storage::new(&S3Config {
            anonymous: true,
            read_only: true,
            ..cfg()
        })
        .unwrap();
        assert!(s3.is_read_only());
        assert!(matches!(s3.create("k"), Err(OndaError::ReadOnly(_))));
        assert!(matches!(
            s3.put_object("k", b"v"),
            Err(OndaError::ReadOnly(_))
        ));
        assert!(matches!(
            s3.create_if_absent("k", b"v"),
            Err(OndaError::ReadOnly(_))
        ));
        assert!(matches!(s3.delete("k"), Err(OndaError::ReadOnly(_))));
        assert!(matches!(s3.rename("k", "j"), Err(OndaError::ReadOnly(_))));
        // Refused before any request: the endpoint is unroutable, and a request
        // would have surfaced as an I/O error (after retries), not ReadOnly.
        assert_eq!(s3.metrics().puts.load(Ordering::Relaxed), 0);
        // Opening for read is still allowed (it makes no request by itself).
        assert!(s3.open_read("k").is_ok());
    }

    #[test]
    fn base64_matches_rfc4648_vectors() {
        for (input, want) in [
            (&b""[..], ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"fooba", "Zm9vYmE="),
            (b"foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64_encode(input), want);
        }
    }

    // --- Hermetic retry-logic tests (no network) --------------------------

    /// A synthetic error to drive [`retry_loop`] without constructing an
    /// `S3Error` (hyper's error has no public constructor).
    #[derive(Debug, PartialEq)]
    enum FakeErr {
        Transient,
        Fatal,
    }

    fn is_fake_transient(e: &FakeErr) -> bool {
        matches!(e, FakeErr::Transient)
    }

    #[test]
    fn retry_loop_succeeds_after_transient_failures() {
        let mut calls = 0u32;
        let mut slept: Vec<u32> = Vec::new();
        let out: std::result::Result<&str, FakeErr> = retry_loop(
            S3_MAX_ATTEMPTS,
            || {
                calls += 1;
                if calls < 3 {
                    Err(FakeErr::Transient)
                } else {
                    Ok("ok")
                }
            },
            is_fake_transient,
            |attempt| slept.push(attempt),
        );
        assert_eq!(out, Ok("ok"));
        assert_eq!(calls, 3, "two failures then success");
        assert_eq!(slept, vec![1, 2], "slept once per retried failure");
    }

    #[test]
    fn retry_loop_does_not_retry_fatal() {
        let mut calls = 0u32;
        let mut slept = 0u32;
        let out: std::result::Result<(), FakeErr> = retry_loop(
            S3_MAX_ATTEMPTS,
            || {
                calls += 1;
                Err(FakeErr::Fatal)
            },
            is_fake_transient,
            |_| slept += 1,
        );
        assert_eq!(out, Err(FakeErr::Fatal));
        assert_eq!(calls, 1, "a non-transient error must not be retried");
        assert_eq!(slept, 0);
    }

    #[test]
    fn retry_loop_exhausts_bounded_attempts() {
        let mut calls = 0u32;
        let mut slept: Vec<u32> = Vec::new();
        let out: std::result::Result<(), FakeErr> = retry_loop(
            S3_MAX_ATTEMPTS,
            || {
                calls += 1;
                Err(FakeErr::Transient)
            },
            is_fake_transient,
            |attempt| slept.push(attempt),
        );
        assert_eq!(out, Err(FakeErr::Transient));
        assert_eq!(calls, S3_MAX_ATTEMPTS, "exactly max_attempts calls");
        assert_eq!(
            slept,
            vec![1, 2, 3],
            "sleeps between attempts, none after the last"
        );
    }

    #[test]
    fn is_transient_classifies_transport_errors() {
        // A connection reset / broken pipe mid-request lands in Io -> retry.
        assert!(is_transient(&S3Error::Io(std::io::Error::other("reset"))));
        // An HTTP status failure is surfaced as Ok(resp) elsewhere; if it ever
        // arrives as an error it is NOT a transport race and must not retry.
        assert!(!is_transient(&S3Error::HttpFailWithBody(
            500,
            "server".into()
        )));
    }
}
