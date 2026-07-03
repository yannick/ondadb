//! Small shared helpers.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time in Unix nanoseconds (used for TTL evaluation).
pub fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
