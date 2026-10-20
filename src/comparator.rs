//! Key comparators.
//!
//! A [`Comparator`] defines a total ordering over user keys.  The ordering must
//! be deterministic for the lifetime of a column family, since the on-disk order
//! of SSTables depends on it.  The comparator's [`name`](Comparator::name) is
//! persisted with the column family so a mismatch can be detected on reopen.
//!
//! **A comparator may return [`Ordering::Equal`] only for byte-identical
//! keys.** Point reads match keys by their bytes (the bloom filter hashes the
//! raw key; memtable, table and transaction-buffer lookups check byte
//! equality), while iteration groups keys by the comparator. A comparator that
//! calls two different byte strings equal would make `get` and a scan disagree
//! about whether they are one key. An ordering that ignores some difference
//! (letter case, say) must still break the tie on the raw bytes, as
//! [`CaseInsensitive`] does.
//!
//! Six built-ins mirror: `memcmp`, `reverse`, `lexicographic`,
//! `uint64`, `int64`, `case_insensitive`.  Custom comparators are supported via
//! [`FnComparator`] (or any [`Comparator`] implementation), registered by name
//! in [`Options::comparators`](crate::Options::comparators).

use std::cmp::Ordering;
use std::sync::Arc;

/// A total ordering over byte-string keys.
pub trait Comparator: Send + Sync + std::fmt::Debug {
    /// Order `a` relative to `b`. Must be a total order, and `Equal` only when
    /// `a == b` byte for byte (see the module docs).
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

/// Big-endian signed 64-bit integer keys. Two 8-byte keys compare as signed
/// integers; every other pair compares byte-wise, except that a **negative**
/// 8-byte key sorts just below the 8-byte zero key (`00…00`).
///
/// That exception is what makes this a total order. Comparing 8-byte pairs as
/// integers and everything else as bytes (ondaDB ≤ 0.10.0) is not transitive
/// once lengths mix: `1 < "z"` and `"z" < -1` byte-wise, yet `-1 < 1`. Placing
/// the negatives below zero keeps every other key exactly where byte order put
/// it, so a family without negative 8-byte keys, or with only 8-byte keys,
/// orders exactly as before.
#[derive(Debug, Default, Clone, Copy)]
pub struct Int64Cmp;

/// The sort key behind [`Int64Cmp`]: a negative 8-byte key ranks as
/// `(00…00, 0, value)`, just below the zero key, and every other key as
/// `(bytes, 1, 0)`. Ordering these tuples is a total order, and two keys tie
/// only when they are byte-identical.
fn int64_rank(k: &[u8]) -> (&[u8], u8, i64) {
    const ZERO: [u8; 8] = [0; 8];
    if k.len() == 8 && k[0] & 0x80 != 0 {
        (&ZERO, 0, be_u64(k) as i64)
    } else {
        (k, 1, 0)
    }
}

impl Comparator for Int64Cmp {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() == 8 && b.len() == 8 {
            return (be_u64(a) as i64).cmp(&(be_u64(b) as i64));
        }
        int64_rank(a).cmp(&int64_rank(b))
    }
    fn name(&self) -> &str {
        "int64"
    }
}

/// ASCII case-insensitive ordering: keys sort by their lowercased bytes, and
/// keys that differ only in letter case are distinct, ordered by their raw
/// bytes (`"K10" < "k10"`). Before 0.10.1 such keys compared `Equal`, which
/// made a point read for one spelling miss the other while a scan merged them.
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
        // Same letters ignoring case: the raw bytes decide, so `Equal` still
        // means byte-identical (see the module docs).
        a.len().cmp(&b.len()).then_with(|| a.cmp(b))
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

/// Resolve a persisted comparator name: a built-in first, then a comparator
/// registered in [`Options::comparators`](crate::Options::comparators). `None`
/// when neither answers it.
pub(crate) fn resolve_comparator(name: &str, registry: &[ComparatorRef]) -> Option<ComparatorRef> {
    comparator_by_name(name).or_else(|| registry.iter().find(|c| c.name() == name).cloned())
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

    /// Every built-in is a total order whose `Equal` means byte-identical,
    /// checked exhaustively over keys that stress each "ignore this" rule.
    #[test]
    fn builtins_are_total_orders_with_byte_identity() {
        let mut keys: Vec<Vec<u8>> = vec![
            vec![], b"z".to_vec(), b"Z".to_vec(), b"\x00".to_vec(), b"k10".to_vec(),
            b"K10".to_vec(), b"1234567".to_vec(), b"123456789".to_vec(),
            vec![0x80; 9], vec![0xff; 7],
        ];
        for n in [0i64, 1, -1, 255, -256, i64::MIN, i64::MAX] {
            keys.push(n.to_be_bytes().to_vec());
        }
        for c in [
            &Bytewise as &dyn Comparator, &ReverseBytewise, &Lexicographic, &Uint64Cmp,
            &Int64Cmp, &CaseInsensitive,
        ] {
            for a in &keys {
                for b in &keys {
                    let ab = c.compare(a, b);
                    assert_eq!(ab, c.compare(b, a).reverse(), "{}: antisymmetry", c.name());
                    assert_eq!(ab == Ordering::Equal, a == b, "{}: Equal iff identical", c.name());
                    for x in &keys {
                        if ab != Ordering::Greater && c.compare(b, x) != Ordering::Greater {
                            assert_ne!(
                                c.compare(a, x),
                                Ordering::Greater,
                                "{}: not transitive: {a:?} <= {b:?} <= {x:?}",
                                c.name()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn int64_orders_negatives_below_zero_and_keeps_byte_order_elsewhere() {
        let i = |v: i64| v.to_be_bytes().to_vec();
        let mut keys = vec![b"z".to_vec(), i(1), i(-1), i(i64::MIN), i(0), b"\x01".to_vec()];
        keys.sort_by(|a, b| Int64Cmp.compare(a, b));
        assert_eq!(
            keys,
            vec![i(i64::MIN), i(-1), i(0), i(1), b"\x01".to_vec(), b"z".to_vec()]
        );
    }

    #[test]
    fn case_insensitive() {
        // Case is ignored for ordering, but differing bytes are never Equal.
        assert_eq!(CaseInsensitive.compare(b"Hello", b"hello"), Ordering::Less);
        assert_eq!(CaseInsensitive.compare(b"hello", b"Hello"), Ordering::Greater);
        assert_eq!(CaseInsensitive.compare(b"hello", b"hello"), Ordering::Equal);
        assert_eq!(CaseInsensitive.compare(b"Hb", b"ha"), Ordering::Greater);
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
