//! Bloom filter for SSTable negative lookups.
//!
//! Classic Bloom filter with `k` hash functions over `m` bits using double
//! hashing derived from a single 64-bit FNV-1a key hash.  It
//! is built during SSTable construction and consulted before reading data
//! blocks.  Two serializations are provided: a dense form and a sparse form
//! (only non-zero words, with indices) 

use crate::encoding::{append_u64, append_uvarint, read_u64, uvarint};
use crate::error::{OndaError, Result};

/// A built or loaded Bloom filter.
#[derive(Debug, Clone)]
pub struct Bloom {
    bits: Vec<u64>,
    m: u64, // number of bits
    k: u32, // number of hash functions
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
        }
    }

    /// Bit positions touched by `key`, computed from `m`/`k` only (no borrow of
    /// `self.bits`, so callers may mutate it while iterating).
    fn positions(m: u64, k: u32, key: &[u8]) -> impl Iterator<Item = usize> {
        let h = hash_key(key);
        let h1 = h as u32;
        let h2 = (h >> 32) as u32;
        let m = m as u32;
        (0..k).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize)
    }

    /// Insert `key`.
    pub fn add(&mut self, key: &[u8]) {
        for bit in Bloom::positions(self.m, self.k, key) {
            self.bits[bit / 64] |= 1 << (bit % 64);
        }
    }

    /// Return `true` if `key` may be present (false positives possible), `false`
    /// if it is definitely absent.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        Bloom::positions(self.m, self.k, key)
            .all(|bit| self.bits[bit / 64] & (1 << (bit % 64)) != 0)
    }

    /// Number of hash functions.
    pub fn k(&self) -> u32 {
        self.k
    }

    /// Number of bits.
    pub fn bits(&self) -> u64 {
        self.m
    }

    /// Dense serialization: `m(uvarint) | k(uvarint) | words(LE u64...)`.
    pub fn encode(&self) -> Vec<u8> {
        let mut dst = Vec::with_capacity(16 + self.bits.len() * 8);
        append_uvarint(&mut dst, self.m);
        append_uvarint(&mut dst, u64::from(self.k));
        for &w in &self.bits {
            append_u64(&mut dst, w);
        }
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
        Ok(Bloom {
            bits,
            m,
            k: k as u32,
        })
    }

    /// Sparse serialization: only non-zero words are written,
    /// each preceded by its index.  Far smaller for mostly-empty filters.
    ///
    /// Format: `m(uvarint) | k(uvarint) | total_words(uvarint) |
    /// nonzero_count(uvarint) | [idx(uvarint) word(LE u64)]...`
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
        Ok(Bloom { bits, m, k })
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
        assert!(Bloom::decode(&enc[..enc.len() - 1]).is_err());
    }
}
