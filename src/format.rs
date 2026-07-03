//! On-disk constants shared by the memtable, WAL and SSTable layers: per-entry
//! flag bits and the MVCC internal-key trailer.
//!
//! An *internal key* is `user_key` followed by an 8-byte big-endian trailer
//! holding the bitwise complement of the sequence number.  Complementing makes
//! higher sequence numbers sort *first* within the same user key, so a forward
//! seek to `(user_key, !read_seq)` lands on the newest version visible at
//! `read_seq`.

/// Per-entry flag bits, persisted in WAL and SSTable klog entries.
pub mod flags {
    /// Entry is a delete marker.
    pub const TOMBSTONE: u8 = 0x01;
    /// A TTL field follows.
    pub const HAS_TTL: u8 = 0x02;
    /// Value lives in the vlog; klog holds an 8-byte offset.
    pub const HAS_VLOG: u8 = 0x04;
    /// Sequence is delta-encoded from the previous entry.
    pub const DELTA_SEQ: u8 = 0x08;
    /// Single-delete tombstone (set together with [`TOMBSTONE`]).
    pub const SINGLE_DELETE: u8 = 0x10;
}

/// Width of the internal-key sequence trailer.
pub const TRAILER_SIZE: usize = 8;

/// Return `user_key || big_endian(!seq)`.
pub fn make_internal_key(user_key: &[u8], seq: u64) -> Vec<u8> {
    let mut ik = Vec::with_capacity(user_key.len() + TRAILER_SIZE);
    ik.extend_from_slice(user_key);
    ik.extend_from_slice(&(!seq).to_be_bytes());
    ik
}

/// Append `user_key || big_endian(!seq)` to `dst`.
pub fn append_internal_key(dst: &mut Vec<u8>, user_key: &[u8], seq: u64) {
    dst.extend_from_slice(user_key);
    dst.extend_from_slice(&(!seq).to_be_bytes());
}

/// User-key portion of an internal key.
pub fn user_key(ik: &[u8]) -> &[u8] {
    &ik[..ik.len() - TRAILER_SIZE]
}

/// Sequence number encoded in an internal key.
pub fn seq(ik: &[u8]) -> u64 {
    let n = ik.len() - TRAILER_SIZE;
    !u64::from_be_bytes(ik[n..].try_into().unwrap())
}

/// Split an internal key into `(user_key, seq)`.
pub fn split_internal_key(ik: &[u8]) -> (&[u8], u64) {
    let n = ik.len() - TRAILER_SIZE;
    (&ik[..n], !u64::from_be_bytes(ik[n..].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_key_round_trip() {
        let ik = make_internal_key(b"hello", 42);
        assert_eq!(user_key(&ik), b"hello");
        assert_eq!(seq(&ik), 42);
        let (uk, s) = split_internal_key(&ik);
        assert_eq!(uk, b"hello");
        assert_eq!(s, 42);
    }

    #[test]
    fn higher_seq_sorts_first() {
        // Same user key: newer (higher seq) internal key must compare LESS.
        let older = make_internal_key(b"k", 1);
        let newer = make_internal_key(b"k", 9);
        assert!(newer < older);
    }

    #[test]
    fn user_key_ordering_dominates() {
        let a = make_internal_key(b"a", 100);
        let b = make_internal_key(b"b", 1);
        assert!(a < b);
    }

    #[test]
    fn append_matches_make() {
        let mut dst = Vec::new();
        append_internal_key(&mut dst, b"xyz", 7);
        assert_eq!(dst, make_internal_key(b"xyz", 7));
    }
}
