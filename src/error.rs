//! Error type for ondaDB.
//!
//! `OndaError::code()` returns the same integer a C caller would see.

use std::fmt;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, OndaError>;

/// Errors returned by ondaDB operations.
///
#[derive(Debug)]
#[non_exhaustive]
pub enum OndaError {
    /// Out-of-memory / allocation failure.
    Memory(String),
    /// Invalid arguments passed to an API.
    InvalidArgs(String),
    /// Key (or other entity) not found.
    NotFound,
    /// Underlying I/O failure.
    Io(std::io::Error),
    /// On-disk data failed an integrity check (checksum / magic / framing).
    Corruption(String),
    /// Entity already exists (e.g. column family).
    Exists(String),
    /// Transaction conflict (serialization / write-write).
    Conflict(String),
    /// Value or request exceeds a hard size limit.
    TooLarge(String),
    /// A configured memory limit was reached.
    MemoryLimit(String),
    /// The database handle is invalid or closed.
    InvalidDb(String),
    /// A resource is locked.
    Locked(String),
    /// The database (or column family) is read-only.
    ReadOnly(String),
    /// A resource is busy (e.g. compaction in progress).
    Busy(String),
    /// The database hit an unrecoverable durability failure (a failed fsync or
    /// background flush) and fail-stopped: writes are rejected until the
    /// database is reopened. Reads keep working.
    Poisoned(String),
    /// Unclassified error.
    Unknown(String),
}

impl OndaError {
    /// Numeric error code
    pub fn code(&self) -> i32 {
        match self {
            OndaError::Memory(_) => -1,
            OndaError::InvalidArgs(_) => -2,
            OndaError::NotFound => -3,
            OndaError::Io(_) => -4,
            OndaError::Corruption(_) => -5,
            OndaError::Exists(_) => -6,
            OndaError::Conflict(_) => -7,
            OndaError::TooLarge(_) => -8,
            OndaError::MemoryLimit(_) => -9,
            OndaError::InvalidDb(_) => -10,
            OndaError::Unknown(_) => -11,
            OndaError::Locked(_) => -12,
            OndaError::ReadOnly(_) => -13,
            OndaError::Busy(_) => -14,
            OndaError::Poisoned(_) => -15,
        }
    }

    /// Reconstruct an error from a numeric code (used when a code is passed
    /// across threads, e.g. WAL group-commit follower results).  Detail is lost;
    /// `0` maps to [`OndaError::Unknown`] since it is not an error code.
    pub fn from_code(code: i32) -> OndaError {
        match code {
            -1 => OndaError::Memory(String::new()),
            -2 => OndaError::InvalidArgs(String::new()),
            -3 => OndaError::NotFound,
            -4 => OndaError::Io(std::io::Error::other("io error")),
            -5 => OndaError::Corruption(String::new()),
            -6 => OndaError::Exists(String::new()),
            -7 => OndaError::Conflict(String::new()),
            -8 => OndaError::TooLarge(String::new()),
            -9 => OndaError::MemoryLimit(String::new()),
            -10 => OndaError::InvalidDb(String::new()),
            -12 => OndaError::Locked(String::new()),
            -13 => OndaError::ReadOnly(String::new()),
            -14 => OndaError::Busy(String::new()),
            -15 => OndaError::Poisoned(String::new()),
            _ => OndaError::Unknown(format!("code {code}")),
        }
    }

    /// Short stable kind string, useful in tests and logs.
    pub fn kind(&self) -> &'static str {
        match self {
            OndaError::Memory(_) => "memory",
            OndaError::InvalidArgs(_) => "invalid_args",
            OndaError::NotFound => "not_found",
            OndaError::Io(_) => "io",
            OndaError::Corruption(_) => "corruption",
            OndaError::Exists(_) => "exists",
            OndaError::Conflict(_) => "conflict",
            OndaError::TooLarge(_) => "too_large",
            OndaError::MemoryLimit(_) => "memory_limit",
            OndaError::InvalidDb(_) => "invalid_db",
            OndaError::Locked(_) => "locked",
            OndaError::ReadOnly(_) => "readonly",
            OndaError::Busy(_) => "busy",
            OndaError::Poisoned(_) => "poisoned",
            OndaError::Unknown(_) => "unknown",
        }
    }
}

impl fmt::Display for OndaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OndaError::Memory(m) => write!(f, "memory error: {m}"),
            OndaError::InvalidArgs(m) => write!(f, "invalid arguments: {m}"),
            OndaError::NotFound => write!(f, "not found"),
            OndaError::Io(e) => write!(f, "io error: {e}"),
            OndaError::Corruption(m) => write!(f, "corruption: {m}"),
            OndaError::Exists(m) => write!(f, "already exists: {m}"),
            OndaError::Conflict(m) => write!(f, "transaction conflict: {m}"),
            OndaError::TooLarge(m) => write!(f, "too large: {m}"),
            OndaError::MemoryLimit(m) => write!(f, "memory limit reached: {m}"),
            OndaError::InvalidDb(m) => write!(f, "invalid database: {m}"),
            OndaError::Locked(m) => write!(f, "locked: {m}"),
            OndaError::ReadOnly(m) => write!(f, "read-only: {m}"),
            OndaError::Busy(m) => write!(f, "busy: {m}"),
            OndaError::Poisoned(m) => write!(f, "poisoned (fail-stop after durability failure): {m}"),
            OndaError::Unknown(m) => write!(f, "unknown error: {m}"),
        }
    }
}

impl std::error::Error for OndaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OndaError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for OndaError {
    fn from(e: std::io::Error) -> Self {
        OndaError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes() {
        assert_eq!(OndaError::Memory(String::new()).code(), -1);
        assert_eq!(OndaError::InvalidArgs(String::new()).code(), -2);
        assert_eq!(OndaError::NotFound.code(), -3);
        assert_eq!(OndaError::Corruption(String::new()).code(), -5);
        assert_eq!(OndaError::Conflict(String::new()).code(), -7);
        assert_eq!(OndaError::Busy(String::new()).code(), -14);
    }

    #[test]
    fn io_error_converts() {
        let io = std::io::Error::other("boom");
        let e: OndaError = io.into();
        assert_eq!(e.code(), -4);
        assert_eq!(e.kind(), "io");
    }
}
