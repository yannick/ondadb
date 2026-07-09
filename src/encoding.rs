//! Low-level serialization primitives shared across ondaDB: little-endian
//! fixed-width integers, LEB128 varints (plain + zig-zag), and block checksums
//! (CRC32-C and xxHash32).
//!
//! All on-disk integers are little-endian so database files are portable across
//! architectures . CRC32-C is the
//! framing checksum used by the WAL and SSTable blocks;

use xxhash_rust::xxh32::xxh32;

/// CRC32-C (Castagnoli) checksum — hardware-accelerated on x86-64/aarch64.
#[inline]
pub fn checksum(b: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(b);
    h.finalize()
}

/// xxHash32 of `b` with the given seed.
#[inline]
pub fn xxhash32(b: &[u8], seed: u32) -> u32 {
    xxh32(b, seed)
}

// ---- varints (LEB128) -------------------------------------------------------

/// Append the unsigned LEB128 (uvarint) encoding of `x` to `dst`.
pub fn append_uvarint(dst: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        dst.push((x as u8) | 0x80);
        x >>= 7;
    }
    dst.push(x as u8);
}

/// Encoded length of `x` as a uvarint (1..=10 bytes).
#[inline]
pub fn uvarint_len(mut x: u64) -> usize {
    let mut n = 1;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}

/// Decode a uvarint from the front of `b`.
///
/// Returns `Some((value, bytes_consumed))`, or `None` if the buffer is too short
/// or the encoding overflows 64 bits (mirrors Go's `binary.Uvarint` returning
/// `n <= 0`).
#[inline]
pub fn uvarint(b: &[u8]) -> Option<(u64, usize)> {
    // One- and two-byte fast paths: lengths are almost always 1 byte, and
    // sequence numbers 2-3 bytes, in the hot block-decode loops.
    match b.first() {
        Some(&byte) if byte < 0x80 => return Some((u64::from(byte), 1)),
        None => return None,
        _ => {}
    }
    if let Some(&b1) = b.get(1) {
        if b1 < 0x80 {
            return Some((u64::from(b[0] & 0x7f) | (u64::from(b1) << 7), 2));
        }
    }
    uvarint_slow(b)
}

fn uvarint_slow(b: &[u8]) -> Option<(u64, usize)> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &byte) in b.iter().enumerate() {
        if i == 10 {
            return None; // overflow: more than 10 bytes
        }
        if byte < 0x80 {
            if i == 9 && byte > 1 {
                return None; // overflow in the 10th byte
            }
            return Some((x | (u64::from(byte) << s), i + 1));
        }
        x |= u64::from(byte & 0x7f) << s;
        s += 7;
    }
    None // buffer too small
}

/// Append the zig-zag LEB128 (signed varint) encoding of `x` to `dst`.
pub fn append_varint(dst: &mut Vec<u8>, x: i64) {
    let ux = ((x << 1) ^ (x >> 63)) as u64; // zig-zag
    append_uvarint(dst, ux);
}

/// Decode a zig-zag signed varint from the front of `b`.
pub fn varint(b: &[u8]) -> Option<(i64, usize)> {
    let (ux, n) = uvarint(b)?;
    let x = ((ux >> 1) as i64) ^ -((ux & 1) as i64); // un-zig-zag
    Some((x, n))
}

// ---- fixed-width little-endian ---------------------------------------------

/// Write `x` to `b[..4]` in little-endian order. Panics if `b.len() < 4`.
#[inline]
pub fn put_u32(b: &mut [u8], x: u32) {
    b[..4].copy_from_slice(&x.to_le_bytes());
}

/// Write `x` to `b[..8]` in little-endian order. Panics if `b.len() < 8`.
#[inline]
pub fn put_u64(b: &mut [u8], x: u64) {
    b[..8].copy_from_slice(&x.to_le_bytes());
}

/// Read a little-endian `u32` from `b[..4]`. Panics if `b.len() < 4`.
#[inline]
pub fn read_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(b[..4].try_into().unwrap())
}

/// Read a little-endian `u64` from `b[..8]`. Panics if `b.len() < 8`.
#[inline]
pub fn read_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

/// Append `x` to `dst` in little-endian order.
#[inline]
pub fn append_u32(dst: &mut Vec<u8>, x: u32) {
    dst.extend_from_slice(&x.to_le_bytes());
}

/// Append `x` to `dst` in little-endian order.
#[inline]
pub fn append_u64(dst: &mut Vec<u8>, x: u64) {
    dst.extend_from_slice(&x.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_round_trip() {
        let vals = [0u64, 1, 127, 128, 300, 1 << 16, 1 << 32, u64::MAX];
        for v in vals {
            let mut buf = Vec::new();
            append_uvarint(&mut buf, v);
            let (got, n) = uvarint(&buf).expect("decode");
            assert!(n > 0);
            assert_eq!(got, v, "value {v}");
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn varint_round_trip() {
        let vals = [
            0i64,
            -1,
            1,
            -128,
            127,
            -(1 << 40),
            1 << 40,
            i64::MIN,
            i64::MAX,
        ];
        for v in vals {
            let mut buf = Vec::new();
            append_varint(&mut buf, v);
            let (got, n) = varint(&buf).expect("decode");
            assert!(n > 0);
            assert_eq!(got, v, "value {v}");
        }
    }

    #[test]
    fn uvarint_truncated_and_overflow() {
        assert_eq!(uvarint(&[]), None);
        assert_eq!(uvarint(&[0x80]), None); // continuation but no more bytes
                                            // 11 continuation bytes => overflow
        assert_eq!(uvarint(&[0x80; 11]), None);
    }

    #[test]
    fn fixed_width() {
        let mut b = [0u8; 8];
        put_u32(&mut b, 0xDEAD_BEEF);
        assert_eq!(read_u32(&b), 0xDEAD_BEEF);
        put_u64(&mut b, 0x0123_4567_89AB_CDEF);
        assert_eq!(read_u64(&b), 0x0123_4567_89AB_CDEF);

        let mut v = Vec::new();
        append_u32(&mut v, 7);
        assert_eq!(v, vec![7, 0, 0, 0]); // little-endian
    }

    #[test]
    fn checksum_stable() {
        let a = checksum(b"hello world");
        assert_eq!(a, checksum(b"hello world"));
        assert_ne!(a, checksum(b"hello worle"));
    }

    #[test]
    fn xxhash_stable() {
        let a = xxhash32(b"hello world", 0);
        assert_eq!(a, xxhash32(b"hello world", 0));
        assert_ne!(a, xxhash32(b"hello world", 1));
    }
}
