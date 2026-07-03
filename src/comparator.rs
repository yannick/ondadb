//! Key comparators.
//!
//! A [`Comparator`] defines a total ordering over user keys.  The ordering must
//! be deterministic for the lifetime of a column family, since the on-disk order
//! of SSTables depends on it.  The comparator's [`name`](Comparator::name) is
//! persisted with the column family so a mismatch can be detected on reopen.
//!
//! Six built-ins mirror: `memcmp`, `reverse`, `lexicographic`,
//! `uint64`, `int64`, `case_insensitive`.  Custom comparators are supported via
//! [`FnComparator`].

use std::cmp::Ordering;
use std::sync::Arc;

/// A total ordering over byte-string keys.
pub trait Comparator: Send + Sync + std::fmt::Debug {
    /// Order `a` relative to `b`.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;
    /// Stable name, persisted with the column family.
    fn name(&self) -> &str;
    /// `true` when this ordering is exactly plain byte-wise comparison, letting
    /// hot paths (merge iteration) use an inlined slice compare instead of a
    /// virtual call. Only override to return `true` if `compare` is identical
    /// to `<[u8]>::cmp` for all inputs.
    fn is_bytewise(&self) -> bool {
        false
    }
}

/// Shared comparator handle.
pub type ComparatorRef = Arc<dyn Comparator>;

/// Byte-wise unsigned comparison (`memcmp`). The default.
#[derive(Debug, Default, Clone, Copy)]
pub struct Bytewise;
impl Comparator for Bytewise {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
    fn name(&self) -> &str {
        "memcmp"
    }
    fn is_bytewise(&self) -> bool {
        true
    }
}

/// Byte-wise comparison, reversed.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReverseBytewise;
impl Comparator for ReverseBytewise {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
    fn name(&self) -> &str {
        "reverse"
    }
}

/// Lexicographic — identical to `memcmp` for raw bytes, kept distinct for parity.
#[derive(Debug, Default, Clone, Copy)]
pub struct Lexicographic;
impl Comparator for Lexicographic {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
    fn name(&self) -> &str {
        "lexicographic"
    }
    fn is_bytewise(&self) -> bool {
        true
    }
}

fn be_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[..8].try_into().unwrap())
}

/// Big-endian unsigned 64-bit integer keys (falls back to byte-wise unless both
/// keys are exactly 8 bytes).
#[derive(Debug, Default, Clone, Copy)]
pub struct Uint64Cmp;
impl Comparator for Uint64Cmp {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() == 8 && b.len() == 8 {
            be_u64(a).cmp(&be_u64(b))
        } else {
            a.cmp(b)
        }
    }
    fn name(&self) -> &str {
        "uint64"
    }
}

/// Big-endian signed 64-bit integer keys (falls back to byte-wise unless both
/// keys are exactly 8 bytes).
#[derive(Debug, Default, Clone, Copy)]
pub struct Int64Cmp;
impl Comparator for Int64Cmp {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() == 8 && b.len() == 8 {
            (be_u64(a) as i64).cmp(&(be_u64(b) as i64))
        } else {
            a.cmp(b)
        }
    }
    fn name(&self) -> &str {
        "int64"
    }
}

/// ASCII case-insensitive comparison.
#[derive(Debug, Default, Clone, Copy)]
pub struct CaseInsensitive;
impl Comparator for CaseInsensitive {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let n = a.len().min(b.len());
        for i in 0..n {
            let ca = a[i].to_ascii_lowercase();
            let cb = b[i].to_ascii_lowercase();
            match ca.cmp(&cb) {
                Ordering::Equal => continue,
                non_eq => return non_eq,
            }
        }
        a.len().cmp(&b.len())
    }
    fn name(&self) -> &str {
        "case_insensitive"
    }
}

/// Boxed comparison closure used by [`FnComparator`].
type CompareFn = Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>;

/// A comparator backed by a user-supplied closure (custom comparators).
pub struct FnComparator {
    name: String,
    f: CompareFn,
}

impl FnComparator {
    pub fn new(
        name: impl Into<String>,
        f: impl Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
    ) -> Self {
        FnComparator {
            name: name.into(),
            f: Box::new(f),
        }
    }
}

impl std::fmt::Debug for FnComparator {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("FnComparator")
            .field("name", &self.name)
            .finish()
    }
}

impl Comparator for FnComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        (self.f)(a, b)
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Resolve a built-in comparator by its persisted name. `""` and `"memcmp"`
/// both map to [`Bytewise`].
pub fn comparator_by_name(name: &str) -> Option<ComparatorRef> {
    Some(match name {
        "" | "memcmp" => Arc::new(Bytewise),
        "reverse" => Arc::new(ReverseBytewise),
        "lexicographic" => Arc::new(Lexicographic),
        "uint64" => Arc::new(Uint64Cmp),
        "int64" => Arc::new(Int64Cmp),
        "case_insensitive" => Arc::new(CaseInsensitive),
        _ => return None,
    })
}

/// The default comparator ([`Bytewise`]).
pub fn default_comparator() -> ComparatorRef {
    Arc::new(Bytewise)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytewise() {
        assert_eq!(Bytewise.compare(b"a", b"b"), Ordering::Less);
        assert_eq!(Bytewise.compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(ReverseBytewise.compare(b"a", b"b"), Ordering::Greater);
    }

    fn mk(v: u64) -> [u8; 8] {
        v.to_be_bytes()
    }

    #[test]
    fn uint64() {
        assert_eq!(Uint64Cmp.compare(&mk(5), &mk(200)), Ordering::Less);
        assert_eq!(Uint64Cmp.compare(&mk(1 << 63), &mk(1)), Ordering::Greater);
    }

    #[test]
    fn int64_negatives() {
        let mki = |v: i64| (v as u64).to_be_bytes();
        assert_eq!(Int64Cmp.compare(&mki(-5), &mki(3)), Ordering::Less);
        assert_eq!(Int64Cmp.compare(&mki(-5), &mki(-10)), Ordering::Greater);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(CaseInsensitive.compare(b"Hello", b"hello"), Ordering::Equal);
        assert_eq!(CaseInsensitive.compare(b"abc", b"abd"), Ordering::Less);
        assert_eq!(CaseInsensitive.compare(b"ab", b"abc"), Ordering::Less);
    }

    #[test]
    fn sort_consistency() {
        let mut keys: Vec<&[u8]> = vec![b"banana", b"apple", b"cherry", b"apple"];
        keys.sort_by(|a, b| Bytewise.compare(a, b));
        for w in keys.windows(2) {
            assert_ne!(Bytewise.compare(w[0], w[1]), Ordering::Greater);
        }
    }

    #[test]
    fn by_name() {
        for name in [
            "",
            "memcmp",
            "reverse",
            "lexicographic",
            "uint64",
            "int64",
            "case_insensitive",
        ] {
            let c = comparator_by_name(name).expect("resolve");
            // round-trip the resolved name (except the "" alias)
            if !name.is_empty() {
                assert_eq!(c.name(), name);
            }
        }
        assert!(comparator_by_name("nope").is_none());
    }

    #[test]
    fn custom_fn_comparator() {
        let c = FnComparator::new("by_len", |a, b| a.len().cmp(&b.len()));
        assert_eq!(c.compare(b"a", b"bb"), Ordering::Less);
        assert_eq!(c.name(), "by_len");
    }
}
