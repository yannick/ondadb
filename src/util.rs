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
