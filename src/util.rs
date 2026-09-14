//! Small shared helpers.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::error::{OndaError, Result};

/// Fsync the parent directory of `path`, making a newly-created directory
/// entry durable. A path without a parent is already relative to the caller's
/// current directory and needs no additional handling here.
pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Fault injection for the catalog's durability calls.
///
/// The crash matrix of the manifest edit log (2.2) is a statement about *which*
/// call failed and *when*, and neither can be produced by manipulating a
/// directory's permission bits: "the second `sync_all` fails, the first
/// succeeded" has no filesystem-level equivalent. So the four catalog calls —
/// `write`, `flush`, `sync_all`, `rename` — consult this shim.
///
/// It is compiled unconditionally rather than behind `cfg(test)`, because the
/// integration tests that drive the matrix are a separate crate and cannot see
/// a `cfg(test)` item. The cost is one thread-local read per catalog write —
/// a path that is already fsync-bound — and the plan is `None` unless a test
/// installs one, so no production write can ever be failed by it. The plan is
/// **thread-local**: a test installs it on its own thread and a background
/// worker is unaffected.
#[doc(hidden)]
pub mod fault {
    use std::cell::Cell;

    /// The calls whose failure the crash matrix distinguishes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Call {
        Write,
        Flush,
        Sync,
        Rename,
        /// Retirement of an obsolete SSTable file
        /// (`DbInner::remove_sst_file`). Unlike the four above this does not
        /// surface an error anywhere — the unlink is best-effort by design —
        /// so the injection simply **skips** it, which is precisely the state a
        /// crash between the durable catalog edit and the unlink leaves behind
        /// (`excise_crash_before_unlink`).
        Unlink,
    }

    thread_local! {
        static PLAN: Cell<Option<(Call, u32)>> = const { Cell::new(None) };
        static SEEN: Cell<u32> = const { Cell::new(0) };
    }

    /// Fail the `nth` (1-based) occurrence of `call` on this thread, and no
    /// other call. Replaces any previous plan.
    pub fn fail_nth(call: Call, nth: u32) {
        assert!(nth >= 1, "occurrences are 1-based");
        PLAN.with(|p| p.set(Some((call, nth))));
        SEEN.with(|s| s.set(0));
    }

    /// Remove any installed plan. Always call this once the failing operation
    /// has been driven, so the rest of the test runs on a healthy filesystem.
    pub fn clear() {
        PLAN.with(|p| p.set(None));
        SEEN.with(|s| s.set(0));
    }

    /// Whether a plan is installed and has not fired yet.
    pub fn armed() -> bool {
        PLAN.with(|p| p.get()).is_some()
    }

    /// Consult the plan for one call site. `Ok(())` unless this is exactly the
    /// occurrence the test asked to fail.
    pub(crate) fn check(call: Call) -> std::io::Result<()> {
        let Some((want, nth)) = PLAN.with(|p| p.get()) else {
            return Ok(());
        };
        if want != call {
            return Ok(());
        }
        let seen = SEEN.with(|s| {
            let n = s.get() + 1;
            s.set(n);
            n
        });
        if seen == nth {
            return Err(std::io::Error::other(format!(
                "injected {call:?} failure (occurrence {nth})"
            )));
        }
        Ok(())
    }
}

/// Current wall-clock time in Unix nanoseconds (used for TTL evaluation).
pub fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Report a recovery-time anomaly the caller cannot be handed as an error.
///
/// The engine has no logging framework and deliberately does not grow one: this
/// is the single case where an open *succeeds* with state the operator must
/// know about — prepared transactions recovered past
/// [`Options::max_prepared_bytes`](crate::Options::max_prepared_bytes).
/// Refusing the open instead would leave no way to inspect, let alone resolve,
/// the durable state causing it. At most one line per open.
pub(crate) fn log_warn(message: &str) {
    eprintln!("ondadb: {message}");
}

/// A source of Unix-nanosecond readings, so a test can drive time instead of
/// waiting for it.
pub type ClockFn = std::sync::Arc<dyn Fn() -> i64 + Send + Sync>;

/// The database's injectable wall clock (0.3).
///
/// Read by **periodic-compaction stamping and eligibility only**. Every other
/// timestamp in the engine — `max_entry_time`, TTL evaluation, FIFO age
/// eviction — keeps calling [`now_nanos`] / [`coarse_now_nanos`] directly, so
/// injecting a fake here cannot move tier placement or expiry out from under a
/// test that was not asking for it.
pub(crate) struct Clock {
    f: Mutex<ClockFn>,
}

impl Clock {
    pub(crate) fn new() -> Self {
        Self {
            f: Mutex::new(std::sync::Arc::new(now_nanos)),
        }
    }

    /// Current reading. The closure is cloned out before it is called, so a
    /// clock that reaches back into the database cannot deadlock on this lock.
    pub(crate) fn now(&self) -> i64 {
        let f = self.f.lock().clone();
        f()
    }

    pub(crate) fn set(&self, f: ClockFn) {
        *self.f.lock() = f;
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Clock").finish()
    }
}

/// Coarse wall-clock nanos for the READ path's TTL checks.
///
/// `now_nanos()` is a precise clock read on every `get`/`peek_seq`/iterator —
/// measured at ~2 % of a warm point-get workload. TTL expiry does not need
/// nanosecond precision: a boundary that moves by a few milliseconds changes
/// when an already-expired entry stops being served, not whether. On Linux
/// this is `CLOCK_REALTIME_COARSE` (vDSO, tick resolution); elsewhere it
/// falls back to the precise clock.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)] // the crate's single audited exception; see lib.rs
#[allow(clippy::unnecessary_cast)] // timespec field widths depend on the Linux target
pub fn coarse_now_nanos() -> i64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: a valid timespec pointer; COARSE is supported on every Linux
    // this crate targets. On the impossible failure, fall back to precise.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME_COARSE, &mut ts) };
    if rc == 0 {
        (ts.tv_sec as i64) * 1_000_000_000 + ts.tv_nsec as i64
    } else {
        now_nanos()
    }
}

#[cfg(not(target_os = "linux"))]
pub fn coarse_now_nanos() -> i64 {
    now_nanos()
}

/// Fail-stop flag shared across the DB, its WALs, and background workers.
///
/// After a failed fsync the kernel may have dropped the dirty pages it could
/// not persist, so retrying can silently lose already-acknowledged data. Any
/// durability failure therefore sets this flag once (first reason wins) and
/// every subsequent write commit is rejected with [`OndaError::Poisoned`].
/// Reads stay available; the only recovery is reopening the database, which
/// re-establishes state from what is actually on disk.
#[derive(Default)]
pub(crate) struct Poison {
    flag: AtomicBool,
    reason: Mutex<String>,
}

impl Poison {
    pub fn new() -> Poison {
        Poison::default()
    }

    /// Trip the flag. The first caller's reason is kept.
    pub fn set(&self, why: String) {
        if !self.flag.swap(true, Ordering::SeqCst) {
            *self.reason.lock() = why;
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub fn reason(&self) -> Option<String> {
        if self.is_poisoned() {
            Some(self.reason.lock().clone())
        } else {
            None
        }
    }

    /// `Err(Poisoned)` if tripped, for use at write entry points.
    pub fn check(&self) -> Result<()> {
        if self.is_poisoned() {
            Err(OndaError::Poisoned(self.reason.lock().clone()))
        } else {
            Ok(())
        }
    }
}

/// Path of one frozen phase-1 fixture (`tests/fixtures/phase1/`, committed to
/// git). Unit tests in the decoder modules read the corpus through this.
#[cfg(test)]
pub(crate) fn phase1_fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/phase1")
        .join(name)
}

/// Deterministic xorshift64 PRNG driving the fuzz-corpus tests. A real PRNG
/// dependency would be a new crate for four lines of arithmetic, and a fixed
/// seed keeps a failure reproducible.
#[cfg(test)]
pub(crate) struct FuzzRng(u64);

#[cfg(test)]
impl FuzzRng {
    pub(crate) fn new(seed: u64) -> FuzzRng {
        FuzzRng(seed | 1)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    pub(crate) fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }
}

/// Derive one fuzz case from `seed`: a handful of random byte pokes, plus an
/// occasional truncation (the shape that finds missing length checks).
#[cfg(test)]
pub(crate) fn fuzz_mutate(rng: &mut FuzzRng, seed: &[u8]) -> Vec<u8> {
    let mut out = seed.to_vec();
    if out.is_empty() {
        return out;
    }
    let pokes = 1 + rng.below(4);
    for _ in 0..pokes {
        let at = rng.below(out.len());
        out[at] = rng.next_u64() as u8;
    }
    if rng.below(4) == 0 {
        let keep = rng.below(out.len());
        out.truncate(keep);
    }
    out
}
