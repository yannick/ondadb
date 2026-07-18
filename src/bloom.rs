//! Bloom filter for SSTable negative lookups.
//!
//! Classic Bloom filter with `k` hash functions over `m` bits using double
//! hashing derived from a single 64-bit FNV-1a key hash.  It
//! is built during SSTable construction and consulted before reading data
//! blocks.  Two serializations are provided: a dense form and a sparse form
//! (only non-zero words, with indices)

use crate::encoding::{append_u64, append_uvarint, read_u64, uvarint};
use crate::error::{OndaError, Result};

/// Which hash function a filter's bits were built with. Legacy filters
/// (encoded without a trailing hash tag) use byte-at-a-time FNV-1a; new
/// filters use xxh3, which processes the key in wide lanes and is several
/// times cheaper for long keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashKind {
    Fnv,
    Xxh3,
}

/// A built or loaded Bloom filter.
#[derive(Debug, Clone)]
pub struct Bloom {
    bits: Vec<u64>,
    m: u64, // number of bits
    k: u32, // number of hash functions
    hash: HashKind,
}

/// Compute `(m, k)` for `n` expected entries at false-positive rate `fpr`.
fn bloom_params(n: usize, fpr: f64) -> (u64, u32) {
    let n = n.max(1) as f64;
    let fpr = if fpr <= 0.0 || fpr >= 1.0 { 0.01 } else { fpr };
    const LN2: f64 = std::f64::consts::LN_2;
    let mf = -n * fpr.ln() / (LN2 * LN2);
    let mut m = mf as u64 + 1;
    if m < 64 {
        m = 64;
    }
    let kf = (mf / n) * LN2;
    let k = ((kf + 0.5) as u32).clamp(1, 30);
    (m, k)
}

/// 64-bit FNV-1a hash; stable across processes and platforms.
fn hash_key(key: &[u8]) -> u64 {
    const OFFSET: u64 = 1469598103934665603;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for &c in key {
        h ^= u64::from(c);
        h = h.wrapping_mul(PRIME);
    }
    h
}

impl Bloom {
    /// Build an empty filter sized for `n` entries at `fpr`.
    pub fn new(n: usize, fpr: f64) -> Bloom {
        let (m, k) = bloom_params(n, fpr);
        let words = m.div_ceil(64);
        Bloom {
            bits: vec![0u64; words as usize],
            m,
            k,
            hash: HashKind::Xxh3,
        }
    }

    /// Hash `key` with this filter's hash function. Callers that consult the
    /// filter and then probe the SSTable can compute this once and reuse it.
    #[inline]
    pub fn hash_of(&self, key: &[u8]) -> u64 {
        match self.hash {
            HashKind::Fnv => hash_key(key),
            HashKind::Xxh3 => xxhash_rust::xxh3::xxh3_64(key),
        }
    }

    /// Bit positions derived from a precomputed key hash `h` (no borrow of
    /// `self.bits`, so callers may mutate it while iterating).
    fn positions(m: u64, k: u32, h: u64) -> impl Iterator<Item = usize> {
        let h1 = h as u32;
        let h2 = (h >> 32) as u32;
        let m = m as u32;
        (0..k).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize)
    }

    /// Insert `key`.
    pub fn add(&mut self, key: &[u8]) {
        self.add_hash(self.hash_of(key));
    }

    /// Insert a key by its precomputed [`hash_of`](Bloom::hash_of) value.
    pub fn add_hash(&mut self, h: u64) {
        for bit in Bloom::positions(self.m, self.k, h) {
            self.bits[bit / 64] |= 1 << (bit % 64);
        }
    }

    /// Return `true` if `key` may be present (false positives possible), `false`
    /// if it is definitely absent.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.may_contain_hash(self.hash_of(key))
    }

    /// [`may_contain`](Bloom::may_contain) by a precomputed
    /// [`hash_of`](Bloom::hash_of) value.
    pub fn may_contain_hash(&self, h: u64) -> bool {
        Bloom::positions(self.m, self.k, h).all(|bit| self.bits[bit / 64] & (1 << (bit % 64)) != 0)
    }

    /// Number of hash functions.
    pub fn k(&self) -> u32 {
        self.k
    }

    /// Number of bits.
    pub fn bits(&self) -> u64 {
        self.m
    }

    /// Dense serialization: `m(uvarint) | k(uvarint) | words(LE u64...) |
    /// hash_id(u8)`. The trailing hash tag is absent in legacy encodings, which
    /// implies FNV; decoders that predate it ignore trailing bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut dst = Vec::with_capacity(17 + self.bits.len() * 8);
        append_uvarint(&mut dst, self.m);
        append_uvarint(&mut dst, u64::from(self.k));
        for &w in &self.bits {
            append_u64(&mut dst, w);
        }
        dst.push(hash_id(self.hash));
        dst
    }

    /// Decode a dense-encoded filter.
    pub fn decode(mut p: &[u8]) -> Result<Bloom> {
        let corrupt = || OndaError::Corruption("bloom: truncated".into());
        let (m, n) = uvarint(p).ok_or_else(corrupt)?;
        p = &p[n..];
        let (k, n) = uvarint(p).ok_or_else(corrupt)?;
        p = &p[n..];
        let words = m.div_ceil(64) as usize;
        if p.len() < words * 8 {
            return Err(corrupt());
        }
        let mut bits = vec![0u64; words];
        for (i, slot) in bits.iter_mut().enumerate() {
            *slot = read_u64(&p[i * 8..]);
        }
        let hash = hash_kind(p.get(words * 8).copied())?;
        Ok(Bloom {
            bits,
            m,
            k: k as u32,
            hash,
        })
    }

    /// Sparse serialization: only non-zero words are written,
    /// each preceded by its index.  Far smaller for mostly-empty filters.
    ///
    /// Format: `m(uvarint) | k(uvarint) | total_words(uvarint) |
    /// nonzero_count(uvarint) | [idx(uvarint) word(LE u64)]... | hash_id(u8)`
    /// (the trailing hash tag is absent in legacy encodings, implying FNV).
    pub fn encode_sparse(&self) -> Vec<u8> {
        let nonzero: Vec<(usize, u64)> = self
            .bits
            .iter()
            .enumerate()
            .filter(|(_, &w)| w != 0)
            .map(|(i, &w)| (i, w))
            .collect();
        let mut dst = Vec::with_capacity(24 + nonzero.len() * 10);
        append_uvarint(&mut dst, self.m);
        append_uvarint(&mut dst, u64::from(self.k));
        append_uvarint(&mut dst, self.bits.len() as u64);
        append_uvarint(&mut dst, nonzero.len() as u64);
        for (i, w) in nonzero {
            append_uvarint(&mut dst, i as u64);
            append_u64(&mut dst, w);
        }
        dst.push(hash_id(self.hash));
        dst
    }

    /// Decode a sparse-encoded filter.
    pub fn decode_sparse(mut p: &[u8]) -> Result<Bloom> {
        let corrupt = || OndaError::Corruption("bloom: truncated (sparse)".into());
        let take = |p: &mut &[u8]| -> Result<u64> {
            let (v, n) = uvarint(p).ok_or_else(corrupt)?;
            *p = &p[n..];
            Ok(v)
        };
        let m = take(&mut p)?;
        let k = take(&mut p)? as u32;
        let total_words = take(&mut p)? as usize;
        let nonzero = take(&mut p)? as usize;
        let mut bits = vec![0u64; total_words];
        for _ in 0..nonzero {
            let idx = take(&mut p)? as usize;
            if p.len() < 8 || idx >= total_words {
                return Err(corrupt());
            }
            bits[idx] = read_u64(p);
            p = &p[8..];
        }
        let hash = hash_kind(p.first().copied())?;
        Ok(Bloom { bits, m, k, hash })
    }
}

fn hash_id(h: HashKind) -> u8 {
    match h {
        HashKind::Fnv => 0,
        HashKind::Xxh3 => 1,
    }
}

/// Map an encoded hash tag back to a [`HashKind`]; `None` (no trailing byte)
/// is the legacy FNV encoding.
fn hash_kind(id: Option<u8>) -> Result<HashKind> {
    match id {
        None | Some(0) => Ok(HashKind::Fnv),
        Some(1) => Ok(HashKind::Xxh3),
        Some(other) => Err(OndaError::Corruption(format!(
            "bloom: unknown hash id {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut b = Bloom::new(1000, 0.01);
        for i in 0..1000u32 {
            b.add(&i.to_be_bytes());
        }
        for i in 0..1000u32 {
            assert!(b.may_contain(&i.to_be_bytes()), "missing {i}");
        }
    }

    #[test]
    fn false_positive_rate_within_bound() {
        let n = 10_000usize;
        let fpr = 0.01;
        let mut b = Bloom::new(n, fpr);
        for i in 0..n as u64 {
            b.add(&i.to_be_bytes());
        }
        let mut fp = 0;
        let trials = 20_000u64;
        for i in n as u64..n as u64 + trials {
            if b.may_contain(&i.to_be_bytes()) {
                fp += 1;
            }
        }
        let observed = fp as f64 / trials as f64;
        // Allow generous slack (5x) for statistical noise; must be in the ballpark.
        assert!(observed < fpr * 5.0, "observed fpr {observed} too high");
    }

    #[test]
    fn dense_round_trip() {
        let mut b = Bloom::new(500, 0.01);
        for i in 0..500u32 {
            b.add(&i.to_le_bytes());
        }
        let enc = b.encode();
        let d = Bloom::decode(&enc).unwrap();
        assert_eq!(d.m, b.m);
        assert_eq!(d.k, b.k);
        for i in 0..500u32 {
            assert!(d.may_contain(&i.to_le_bytes()));
        }
    }

    #[test]
    fn sparse_round_trip() {
        let mut b = Bloom::new(100_000, 0.01); // big filter, few entries => sparse wins
        for i in 0..50u32 {
            b.add(&i.to_le_bytes());
        }
        let sparse = b.encode_sparse();
        let dense = b.encode();
        assert!(sparse.len() < dense.len(), "sparse should be smaller");
        let d = Bloom::decode_sparse(&sparse).unwrap();
        assert_eq!(d.m, b.m);
        assert_eq!(d.k, b.k);
        for i in 0..50u32 {
            assert!(d.may_contain(&i.to_le_bytes()));
        }
    }

    #[test]
    fn decode_rejects_truncation() {
        let b = Bloom::new(64, 0.01);
        let enc = b.encode();
        // Truncating into the bit words is corruption...
        assert!(Bloom::decode(&enc[..enc.len() - 9]).is_err());
        // ...but stripping only the trailing hash tag is a valid LEGACY (FNV)
        // encoding, by construction.
        let legacy = Bloom::decode(&enc[..enc.len() - 1]).unwrap();
        assert_eq!(legacy.hash, HashKind::Fnv);
    }

    #[test]
    fn legacy_decode_uses_fnv() {
        // Build under FNV (as a pre-tag writer would have), encode WITHOUT the
        // tag byte, and verify decode finds every key — no false negatives.
        let mut b = Bloom::new(1000, 0.01);
        b.hash = HashKind::Fnv;
        for i in 0..1000u32 {
            b.add(&i.to_be_bytes());
        }
        let mut enc = b.encode();
        enc.pop(); // strip the hash tag -> legacy format
        let d = Bloom::decode(&enc).unwrap();
        assert_eq!(d.hash, HashKind::Fnv);
        for i in 0..1000u32 {
            assert!(d.may_contain(&i.to_be_bytes()), "missing {i}");
        }
    }

    #[test]
    fn hash_once_api() {
        let mut b = Bloom::new(100, 0.01);
        for i in 0..100u32 {
            b.add(&i.to_be_bytes());
        }
        for i in 0..200u32 {
            let key = i.to_be_bytes();
            let h = b.hash_of(&key);
            assert_eq!(b.may_contain_hash(h), b.may_contain(&key));
        }
    }

    #[test]
    fn sparse_tag_round_trip() {
        let mut b = Bloom::new(100_000, 0.01);
        for i in 0..50u32 {
            b.add(&i.to_le_bytes());
        }
        let d = Bloom::decode_sparse(&b.encode_sparse()).unwrap();
        assert_eq!(d.hash, HashKind::Xxh3);
        for i in 0..50u32 {
            assert!(d.may_contain(&i.to_le_bytes()));
        }
    }
}
