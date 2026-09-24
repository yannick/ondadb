//! Bloom filter for SSTable negative lookups.
//!
//! Classic Bloom filter with `k` hash functions over `m` bits using double
//! hashing derived from a single 64-bit xxh3 key hash. It is built during
//! SSTable construction and consulted before reading data blocks.
//!
//! Epoch-1 encoding (a meta block, referenced by the footer):
//!
//! ```text
//! hash u8 = 1 (xxh3-64) | m uvarint | k uvarint | words u64 LE × ceil(m / 64)
//! ```
//!
//! The hash byte **leads** — wavesdb's layout, which epoch 1 adopted; 0.9 put a
//! tag at the end and read its absence as FNV. The hash, the probing and the bit
//! order were already identical in both engines. A 0.9 filter is decoded only by
//! `legacy_onda::sst::decode_bloom`.

use crate::encoding::{append_u64, append_uvarint, read_u64, uvarint};
use crate::error::{OndaError, Result};
use crate::format::BLOOM_HASH_XXH3;

/// Which hash function a filter's bits were built with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashKind {
    /// xxh3-64, seed 0 — the only hash epoch 1 writes.
    Xxh3,
    /// 0.9's FNV-1a with the truncated offset basis, readable only through
    /// `legacy_onda` ([`crate::legacy_onda::fnv1a64_09`]).
    #[cfg(feature = "legacy-onda")]
    Fnv09,
}

/// A built or loaded Bloom filter.
#[derive(Debug, Clone)]
pub struct Bloom {
    bits: Vec<u64>,
    m: u64, // number of bits
    k: u32, // number of hash functions
    hash: HashKind,
}

/// Largest `k` a writer produces, and a decoder accepts.
const MAX_K: u64 = 30;

/// Compute `(m, k)` for `n` expected entries at false-positive rate `fpr`.
fn bloom_params(n: usize, fpr: f64) -> (u64, u32) {
    let n = n.max(1) as f64;
    let fpr = if fpr <= 0.0 || fpr >= 1.0 { 0.01 } else { fpr };
    const LN2: f64 = std::f64::consts::LN_2;
    let mf = -n * fpr.ln() / (LN2 * LN2);
    // Probing reduces modulo `m` as a u32, so a filter never has 2^32 bits or
    // more (512 MiB of filter — far past any table's).
    let m = (mf as u64 + 1).clamp(64, u64::from(u32::MAX));
    let kf = (mf / n) * LN2;
    let k = ((kf + 0.5) as u32).clamp(1, MAX_K as u32);
    (m, k)
}

/// The hash a filter built by [`Bloom::new`] will use.
///
/// A writer cannot size a filter until it knows how many keys it wrote, so it
/// buffers each key's hash and constructs the filter at `finish()`. Those
/// hashes must be computed with the same function the finished filter uses, and
/// nothing in the type system enforces that — so it lives here, next to
/// [`Bloom::new`], with `hash_matches_new` pinning the agreement.
pub fn hash_for_new(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

impl Bloom {
    /// Heap bytes this filter holds. For memory accounting only.
    pub(crate) fn resident_bytes(&self) -> usize {
        self.bits.len() * std::mem::size_of::<u64>()
    }

    /// Assemble a decoded filter. For decoders outside this module (the 0.9
    /// reader); `m` and `k` must already be validated against `bits`.
    #[cfg(feature = "legacy-onda")]
    pub(crate) fn from_parts(bits: Vec<u64>, m: u64, k: u32, hash: HashKind) -> Bloom {
        debug_assert_eq!(bits.len() as u64, m.div_ceil(64));
        Bloom { bits, m, k, hash }
    }

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
            HashKind::Xxh3 => xxhash_rust::xxh3::xxh3_64(key),
            #[cfg(feature = "legacy-onda")]
            HashKind::Fnv09 => crate::legacy_onda::fnv1a64_09(key),
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

    /// The epoch-1 encoding: `hash u8 | m uvarint | k uvarint | words u64 LE`.
    pub fn encode(&self) -> Vec<u8> {
        debug_assert_eq!(
            self.hash,
            HashKind::Xxh3,
            "epoch 1 writes xxh3 filters only"
        );
        let mut dst = Vec::with_capacity(17 + self.bits.len() * 8);
        dst.push(BLOOM_HASH_XXH3);
        append_uvarint(&mut dst, self.m);
        append_uvarint(&mut dst, u64::from(self.k));
        for &w in &self.bits {
            append_u64(&mut dst, w);
        }
        dst
    }

    /// Decode an epoch-1 filter.
    ///
    /// Strict: a hash id other than xxh3 is `UnsupportedFormat` (0.9's FNV id
    /// included — such a filter belongs to a 0.9 table), and `m` outside
    /// `[1, 2^32)`, `k` outside `[1, 30]`, missing words or trailing bytes are
    /// `Corruption`.
    pub fn decode(p: &[u8]) -> Result<Bloom> {
        let corrupt = |what: &str| OndaError::Corruption(format!("bloom: {what}"));
        let (&hash, mut p) = p.split_first().ok_or_else(|| corrupt("empty block"))?;
        if hash != BLOOM_HASH_XXH3 {
            return Err(OndaError::UnsupportedFormat(format!(
                "bloom hash id {hash} is not implemented by this binary"
            )));
        }
        let (m, n) = uvarint(p).ok_or_else(|| corrupt("truncated m"))?;
        p = &p[n..];
        let (k, n) = uvarint(p).ok_or_else(|| corrupt("truncated k"))?;
        p = &p[n..];
        if m == 0 || m > u64::from(u32::MAX) {
            return Err(corrupt("bit count outside [1, 2^32)"));
        }
        if k == 0 || k > MAX_K {
            return Err(corrupt("hash count outside [1, 30]"));
        }
        let words = m.div_ceil(64) as usize;
        if p.len() != words * 8 {
            return Err(corrupt("word array length disagrees with m"));
        }
        let bits = (0..words).map(|i| read_u64(&p[i * 8..])).collect();
        Ok(Bloom {
            bits,
            m,
            k: k as u32,
            hash: HashKind::Xxh3,
        })
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
    fn round_trip() {
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

    /// The encoding, byte for byte: the hash id leads.
    #[test]
    fn golden_bytes() {
        let mut b = Bloom::new(1, 0.01);
        b.add(b"k");
        let enc = b.encode();
        assert_eq!(enc[0], 1, "xxh3 id leads");
        assert_eq!(enc[1], 64, "m = 64 (one word)");
        assert_eq!(enc[2], 7, "k");
        assert_eq!(enc.len(), 3 + 8);
        let mut word = 0u64;
        let h = xxhash_rust::xxh3::xxh3_64(b"k");
        let (h1, h2) = (h as u32, (h >> 32) as u32);
        for i in 0..7u32 {
            word |= 1 << (h1.wrapping_add(i.wrapping_mul(h2)) % 64);
        }
        assert_eq!(enc[3..11], word.to_le_bytes());
    }

    #[test]
    fn decode_rows_fail_closed() {
        let mut b = Bloom::new(64, 0.01);
        b.add(b"x");
        let enc = b.encode();
        // Truncated anywhere.
        for n in 0..enc.len() {
            assert!(Bloom::decode(&enc[..n]).is_err(), "truncated to {n}");
        }
        // Trailing bytes — including 0.9's trailing tag.
        let mut t = enc.clone();
        t.push(1);
        assert_eq!(Bloom::decode(&t).unwrap_err().kind(), "corruption");
        // Hash id 0 (0.9 FNV) and an unknown id: a format this binary lacks.
        for id in [0u8, 2, 255] {
            let mut t = enc.clone();
            t[0] = id;
            assert_eq!(Bloom::decode(&t).unwrap_err().kind(), "unsupported_format");
        }
        // k = 0 and k = 31.
        for k in [0u8, 31] {
            let mut t = enc.clone();
            t[2] = k;
            assert_eq!(Bloom::decode(&t).unwrap_err().kind(), "corruption");
        }
        // m = 0.
        let mut t = vec![1u8, 0, 1];
        t.extend_from_slice(&[0; 8]);
        assert_eq!(Bloom::decode(&t).unwrap_err().kind(), "corruption");
        // m = 2^32.
        let mut t = vec![1u8];
        append_uvarint(&mut t, 1 << 32);
        append_uvarint(&mut t, 3);
        assert_eq!(Bloom::decode(&t).unwrap_err().kind(), "corruption");
    }

    #[test]
    fn fuzz_decode_never_panics() {
        let mut b = Bloom::new(200, 0.01);
        for i in 0..200u32 {
            b.add(&i.to_le_bytes());
        }
        let seed = b.encode();
        let mut rng = crate::util::FuzzRng::new(0xB100_F1E7_0000_0001);
        for _ in 0..5000 {
            let case = crate::util::fuzz_mutate(&mut rng, &seed);
            let _ = Bloom::decode(&case);
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

    /// `hash_for_new` must agree with what `Bloom::new` actually uses, or a
    /// writer that buffers hashes builds a filter over a different hash space
    /// than the reader consults — which silently loses every key.
    #[test]
    fn hash_matches_new() {
        let b = Bloom::new(1000, 0.01);
        for k in [b"".as_slice(), b"a", b"abcdefghijklmnop", &[0xFF; 257]] {
            assert_eq!(
                b.hash_of(k),
                super::hash_for_new(k),
                "hash_for_new disagrees with Bloom::new's hash function"
            );
        }
    }

    /// A filter built from the entries actually written must filter, whatever
    /// a caller guessed. This is the unit-level statement of the compaction
    /// regression in `tests/bloom_survives_compaction.rs`.
    #[test]
    fn a_filter_sized_for_its_real_load_still_rejects() {
        const N: usize = 200_000;
        let mut b = Bloom::new(N, 0.01);
        for i in 0..N as u64 {
            b.add_hash(super::hash_for_new(&(2 * i).to_be_bytes()));
        }
        let admitted = (0..10_000u64)
            .filter(|i| b.may_contain_hash(super::hash_for_new(&(2 * i + 1).to_be_bytes())))
            .count();
        assert!(
            admitted < 500,
            "{admitted}/10000 absent keys admitted at fpr 0.01 — the filter is \
             saturated, which is what sizing from a guess produces"
        );
    }

    /// And the failure mode itself, so the cost of guessing is on the record:
    /// size for 4,096 (what compaction used to pass) and load 200,000.
    #[test]
    fn a_filter_sized_by_a_guess_admits_everything() {
        const N: usize = 200_000;
        let mut b = Bloom::new(4096, 0.01);
        for i in 0..N as u64 {
            b.add_hash(super::hash_for_new(&(2 * i).to_be_bytes()));
        }
        let admitted = (0..10_000u64)
            .filter(|i| b.may_contain_hash(super::hash_for_new(&(2 * i + 1).to_be_bytes())))
            .count();
        assert_eq!(
            admitted, 10_000,
            "a 49x-overloaded filter should admit everything; if this ever \
             fails the sizing math changed and the regression test above is \
             measuring something else"
        );
    }
}
