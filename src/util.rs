//! Small shared helpers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::error::{OndaError, Result};

/// Current wall-clock time in Unix nanoseconds (used for TTL evaluation).
pub fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
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
