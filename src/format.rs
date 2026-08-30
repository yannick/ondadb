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
    /// Single-delete tombstone (set together with [`TOMBSTONE`]).
    pub const SINGLE_DELETE: u8 = 0x10;
}

/// Mask of every entry-flag bit this binary implements (`0x17`).
///
/// `0x08` is deliberately absent: it named a `DELTA_SEQ` encoding no writer
/// ever produced, so it is reserved-unknown and the decoders reject it. Record
/// extensibility comes from kinds, not from the remaining flag bits.
pub const KNOWN_ENTRY_FLAGS: u8 =
    flags::TOMBSTONE | flags::HAS_TTL | flags::HAS_VLOG | flags::SINGLE_DELETE;

/// Build the flags byte of one entry, normalizing the two invariants the
/// decoders enforce: a single-delete *is* a tombstone, and a tombstone never
/// carries a vlog pointer (it has no value to separate).
///
/// Normalizing rather than trusting the caller is what makes strict decoding
/// safe: [`RecordRef`](crate::wal::RecordRef) is public and `wal::append_batch`
/// is exported, so `{ tombstone: false, single_delete: true }` is constructible
/// outside the crate. Writing those bytes and then refusing to read them back
/// would turn a caller's mistake into an unopenable database. Debug builds
/// still trip [`debug_check_entry_flags`] at each encode site, so an internal
/// bug is loud where being loud is free.
pub fn normalized_entry_flags(
    tombstone: bool,
    single_delete: bool,
    has_ttl: bool,
    has_vlog: bool,
) -> u8 {
    let tombstone = tombstone || single_delete;
    let mut fl = 0u8;
    if tombstone {
        fl |= flags::TOMBSTONE;
    }
    if single_delete {
        fl |= flags::SINGLE_DELETE;
    }
    if has_ttl {
        fl |= flags::HAS_TTL;
    }
    if has_vlog && !tombstone {
        fl |= flags::HAS_VLOG;
    }
    fl
}

/// Debug-only guard for the invariants [`normalized_entry_flags`] repairs.
#[inline]
pub(crate) fn debug_check_entry_flags(tombstone: bool, single_delete: bool, has_vlog: bool) {
    debug_assert!(
        !single_delete || tombstone,
        "SINGLE_DELETE without TOMBSTONE"
    );
    debug_assert!(!(tombstone && has_vlog), "TOMBSTONE with HAS_VLOG");
}

/// Reject an entry-flag byte this binary cannot honor.
///
/// Two classes, both `Corruption` (the bytes contradict a format this binary
/// *does* implement, rather than naming one it does not):
/// unknown bits outside [`KNOWN_ENTRY_FLAGS`], and combinations no writer can
/// produce — `SINGLE_DELETE` without `TOMBSTONE`, `TOMBSTONE` with `HAS_VLOG`.
pub fn check_entry_flags(fl: u8) -> crate::error::Result<()> {
    if fl & !KNOWN_ENTRY_FLAGS != 0 {
        return Err(crate::error::OndaError::Corruption(format!(
            "entry flags {fl:#04x} outside known mask {KNOWN_ENTRY_FLAGS:#04x}"
        )));
    }
    if fl & flags::SINGLE_DELETE != 0 && fl & flags::TOMBSTONE == 0 {
        return Err(crate::error::OndaError::Corruption(
            "entry flags: SINGLE_DELETE without TOMBSTONE".into(),
        ));
    }
    if fl & flags::TOMBSTONE != 0 && fl & flags::HAS_VLOG != 0 {
        return Err(crate::error::OndaError::Corruption(
            "entry flags: TOMBSTONE with HAS_VLOG".into(),
        ));
    }
    Ok(())
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
