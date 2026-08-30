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

/// Format capabilities a database may enable, persisted as a single `u64` word
/// in the manifest's `ONDACAP1` tail (see [`crate::manifest`]).
///
/// A capability is the *permission* to write a newer artifact, taken once and
/// durably, before the first byte using it exists. The values are pinned here
/// and golden-pinned in `tests/fixtures/phase1/`: they are the interoperability
/// contract with wavesdb, so a bit is never renumbered, only retired.
///
/// A manifest naming a bit outside [`KNOWN_CAPS`] was written by a newer binary:
/// the bytes are well-formed and describe a feature this one does not implement,
/// so the open fails with [`OndaError::UnsupportedFormat`](crate::OndaError).
pub const CAP_EXTENDED_RECORDS: u64 = 1 << 0; // kind-bearing envelopes (1.0)
/// Merge operands (1.1).
pub const CAP_MERGE_OPERANDS: u64 = 1 << 1;
/// Range deletes (1.2).
pub const CAP_RANGE_DELETES: u64 = 1 << 2;
/// Prefix-delta key encoding (2.1).
pub const CAP_PREFIX_DELTA: u64 = 1 << 3;
/// Incremental (edit-log) manifest (2.2).
pub const CAP_MANIFEST_EDITS: u64 = 1 << 4;
/// Periodic-age compaction stamps (0.3).
pub const CAP_PERIODIC_AGE: u64 = 1 << 5;
/// Persisted transaction decisions (3.2).
pub const CAP_TXN_DECISIONS: u64 = 1 << 6;
/// Capabilities a writer must hold before it may emit a prefix-delta table
/// (2.1). Both, not just [`CAP_PREFIX_DELTA`]: a delta block *is* an extended
/// block — the layout is defined only over the kind-bearing envelope — and
/// [`CAP_EXTENDED_RECORDS`] is the permission for that envelope.
pub const CAPS_PREFIX_DELTA_WRITE: u64 = CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA;

/// Capabilities a writer must hold before it may emit a merge operand (1.1).
/// Both, for the same reason [`CAPS_PREFIX_DELTA_WRITE`] needs both: a
/// [`KIND_MERGE`] record exists only inside the kind-bearing envelope, and
/// [`CAP_EXTENDED_RECORDS`] is the permission for that envelope.
pub const CAPS_MERGE_WRITE: u64 = CAP_EXTENDED_RECORDS | CAP_MERGE_OPERANDS;

/// Mask of every capability bit this roadmap has assigned (`0x7F`).
pub const KNOWN_CAPS: u64 = CAP_EXTENDED_RECORDS
    | CAP_MERGE_OPERANDS
    | CAP_RANGE_DELETES
    | CAP_PREFIX_DELTA
    | CAP_MANIFEST_EDITS
    | CAP_PERIODIC_AGE
    | CAP_TXN_DECISIONS;

/// Reject a capability word naming a bit this binary does not implement.
pub fn check_caps(caps: u64) -> crate::error::Result<()> {
    if caps & !KNOWN_CAPS != 0 {
        return Err(crate::error::OndaError::UnsupportedFormat(format!(
            "manifest capabilities {caps:#x} outside known mask {KNOWN_CAPS:#x}"
        )));
    }
    Ok(())
}

/// Record kind, the leading field of an extended (envelope) record.
///
/// Kinds replace the exhausted entry-flag bits as the extension point: the
/// legacy flags byte has three free bits, while the roadmap needs merge, range
/// delete and transaction-control records. Values are pinned for wavesdb
/// compatibility exactly as the capability bits are.
pub const KIND_PUT: u64 = 1;
/// Delete tombstone.
pub const KIND_DELETE: u64 = 2;
/// Single-delete tombstone.
pub const KIND_SINGLE_DELETE: u64 = 3;
/// Merge operand (1.1).
pub const KIND_MERGE: u64 = 4;
/// Range delete (1.2).
pub const KIND_RANGE_DELETE: u64 = 5;
// 6..15   reserved for future data kinds
/// Durable prepare record (3.2): the transaction id, its column families, and
/// the writeset that follows it in the same frame.
pub const KIND_PREPARE: u64 = 16;
/// Commit decision for a prepared transaction (3.2).
pub const KIND_COMMIT_DECISION: u64 = 17;
/// Abort decision for a prepared transaction (3.2).
pub const KIND_ABORT_DECISION: u64 = 18;
// 19..31  reserved for further transaction control
// 32..63  reserved
/// Highest value that may ever be assigned a meaning. Anything above this is
/// never written by any writer, so it cannot have come from a newer binary.
pub const MAX_ASSIGNABLE_KIND: u64 = 63;

/// Per-record modifiers of an extended record.
///
/// The bit values deliberately match [`flags`], so an extended entry and a
/// legacy entry describe the same thing with the same numbers. `TOMBSTONE` and
/// `SINGLE_DELETE` are *not* modifiers — they are kinds 2 and 3.
pub mod modifiers {
    /// A TTL field follows (`== flags::HAS_TTL`).
    pub const HAS_TTL: u64 = 0x02;
    /// Value lives in the vlog (`== flags::HAS_VLOG`); SSTable entries only.
    pub const HAS_VLOG: u64 = 0x04;
    /// Mask of every modifier bit this binary implements.
    pub const KNOWN: u64 = HAS_TTL | HAS_VLOG;
}

/// Reject a record kind this binary cannot honor.
///
/// Two classes, and the split is the whole point of the `>= 64` reservation:
/// a kind above [`MAX_ASSIGNABLE_KIND`] is never assigned to anything, so it
/// cannot have been written by a newer binary and is `Corruption`; an assigned
/// but unimplemented kind (merge, range delete, transaction control) is
/// [`OndaError::UnsupportedFormat`](crate::OndaError) — those bytes are intact,
/// this binary is simply too old to read them.
pub fn check_kind(kind: u64) -> crate::error::Result<()> {
    if kind > MAX_ASSIGNABLE_KIND {
        return Err(crate::error::OndaError::Corruption(format!(
            "record kind {kind} is above the never-assigned bound {MAX_ASSIGNABLE_KIND}"
        )));
    }
    match kind {
        KIND_PUT
        | KIND_DELETE
        | KIND_SINGLE_DELETE
        | KIND_MERGE
        | KIND_RANGE_DELETE
        | KIND_PREPARE
        | KIND_COMMIT_DECISION
        | KIND_ABORT_DECISION => Ok(()),
        _ => Err(crate::error::OndaError::UnsupportedFormat(format!(
            "record kind {kind} is not implemented by this binary"
        ))),
    }
}

/// Whether `kind` is a transaction-control kind (3.2), i.e. one that may only
/// appear in a control frame and never in a data stream.
pub fn is_control_kind(kind: u64) -> bool {
    matches!(
        kind,
        KIND_PREPARE | KIND_COMMIT_DECISION | KIND_ABORT_DECISION
    )
}

/// Reject a record kind that may not appear in a data-block **point** stream.
///
/// Range deletes (kind 5) live in the aux block's fragment section, never
/// between two point entries: a fragment has two keys and no value, so a
/// decoder that accepted one here would have to invent a value slot. The
/// transaction-control kinds (16–18) never reach an SSTable at all — a prepare
/// is applied through its decision, and only the resulting point records are
/// flushed. The bytes are intact and name a placement no writer produces, so
/// this is `Corruption` rather than `UnsupportedFormat`.
pub fn check_point_kind(kind: u64) -> crate::error::Result<()> {
    check_kind(kind)?;
    if kind == KIND_RANGE_DELETE {
        return Err(crate::error::OndaError::Corruption(
            "sst: range-delete kind in the point-entry stream".into(),
        ));
    }
    if is_control_kind(kind) {
        return Err(crate::error::OndaError::Corruption(format!(
            "sst: transaction-control kind {kind} in the point-entry stream"
        )));
    }
    Ok(())
}

/// Reject a modifier word naming a bit this binary does not implement.
///
/// `Corruption`, not `UnsupportedFormat`: modifiers are not capability-gated,
/// so no writer — of any vintage — may set a bit outside [`modifiers::KNOWN`].
pub fn check_modifiers(mods: u64) -> crate::error::Result<()> {
    if mods & !modifiers::KNOWN != 0 {
        return Err(crate::error::OndaError::Corruption(format!(
            "record modifiers {mods:#x} outside known mask {:#x}",
            modifiers::KNOWN
        )));
    }
    Ok(())
}

/// Whether `kind` names a *point* record — one that replaces every older
/// version of its key, rather than composing with them.
///
/// [`KIND_MERGE`] is the first kind that is not a point: it is an operand that
/// only becomes a value once folded against everything below it, which is why
/// compaction retention, the iterator's group resolution and the point-read
/// candidate all have to ask this question rather than reading `tombstone`.
#[inline]
pub fn is_point_kind(kind: u64) -> bool {
    matches!(kind, KIND_PUT | KIND_DELETE | KIND_SINGLE_DELETE)
}

/// The kind naming a point record's `(tombstone, single_delete)` pair.
pub fn point_kind(tombstone: bool, single_delete: bool) -> u64 {
    if single_delete {
        KIND_SINGLE_DELETE
    } else if tombstone {
        KIND_DELETE
    } else {
        KIND_PUT
    }
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

    /// The capability word is a cross-engine contract (wavesdb is reconciled to
    /// these numbers), so every literal is asserted rather than derived.
    #[test]
    fn capability_bits_are_pinned() {
        assert_eq!(CAP_EXTENDED_RECORDS, 0x01);
        assert_eq!(CAP_MERGE_OPERANDS, 0x02);
        assert_eq!(CAP_RANGE_DELETES, 0x04);
        assert_eq!(CAP_PREFIX_DELTA, 0x08);
        assert_eq!(CAP_MANIFEST_EDITS, 0x10);
        assert_eq!(CAP_PERIODIC_AGE, 0x20);
        assert_eq!(CAP_TXN_DECISIONS, 0x40);
    }

    #[test]
    fn known_caps_is_0x7f() {
        assert_eq!(KNOWN_CAPS, 0x7F);
        assert!(check_caps(KNOWN_CAPS).is_ok());
        let err = check_caps(1 << 7).expect_err("an unassigned capability bit must fail closed");
        assert_eq!(err.kind(), "unsupported_format");
    }

    #[test]
    fn record_kinds_are_pinned() {
        assert_eq!(KIND_PUT, 1);
        assert_eq!(KIND_DELETE, 2);
        assert_eq!(KIND_SINGLE_DELETE, 3);
        assert_eq!(KIND_MERGE, 4);
        assert_eq!(KIND_RANGE_DELETE, 5);
        assert_eq!(KIND_PREPARE, 16);
        assert_eq!(KIND_COMMIT_DECISION, 17);
        assert_eq!(KIND_ABORT_DECISION, 18);
        assert_eq!(MAX_ASSIGNABLE_KIND, 63);
        assert_eq!(point_kind(false, false), KIND_PUT);
        assert_eq!(point_kind(true, false), KIND_DELETE);
        assert_eq!(point_kind(true, true), KIND_SINGLE_DELETE);
        assert!(is_point_kind(KIND_PUT));
        assert!(is_point_kind(KIND_DELETE));
        assert!(is_point_kind(KIND_SINGLE_DELETE));
        assert!(!is_point_kind(KIND_MERGE));
        assert_eq!(CAPS_MERGE_WRITE, CAP_EXTENDED_RECORDS | CAP_MERGE_OPERANDS);
    }

    /// An assigned-but-unimplemented kind names a real feature (`UnsupportedFormat`);
    /// a kind in the never-assigned range cannot have come from any writer
    /// (`Corruption`).
    #[test]
    fn kind_check_splits_unsupported_from_corruption() {
        for k in [
            KIND_PUT,
            KIND_DELETE,
            KIND_SINGLE_DELETE,
            KIND_MERGE,
            KIND_RANGE_DELETE,
            KIND_PREPARE,
            KIND_COMMIT_DECISION,
            KIND_ABORT_DECISION,
        ] {
            assert!(check_kind(k).is_ok());
        }
        for k in [0, 6, 19, 63] {
            assert_eq!(check_kind(k).unwrap_err().kind(), "unsupported_format");
        }
        for k in [64u64, 1000] {
            assert_eq!(check_kind(k).unwrap_err().kind(), "corruption");
        }
    }

    /// 1.0 reserved 16..31 for transaction control; 3.2 assigns three of them
    /// and leaves the rest *assigned-but-unimplemented*, which stays
    /// `UnsupportedFormat` — a binary without those kinds is the one at fault,
    /// not the bytes. The taxonomy is what this pins, in both directions.
    #[test]
    fn txn_kinds_without_feature_are_unsupported_format() {
        for k in [KIND_PREPARE, KIND_COMMIT_DECISION, KIND_ABORT_DECISION] {
            assert!(is_control_kind(k));
            assert!(check_kind(k).is_ok(), "3.2 implements kind {k}");
            // Implemented in the WAL envelope only: an SSTable point entry
            // naming one is a placement no writer produces.
            assert_eq!(check_point_kind(k).unwrap_err().kind(), "corruption");
        }
        for k in 19..=31u64 {
            assert!(!is_control_kind(k));
            assert_eq!(
                check_kind(k).unwrap_err().kind(),
                "unsupported_format",
                "reserved transaction kind {k}"
            );
        }
    }

    /// Kind 5 is implemented, but only in the WAL envelope and the aux section:
    /// a data-block point entry naming it is corruption, not a newer format.
    #[test]
    fn range_delete_kind_is_refused_in_the_point_stream() {
        // 1.1's merge operand is not a *point* kind, but it is still a
        // one-key data-block entry: only the two-key fragment is refused here.
        for k in [KIND_PUT, KIND_DELETE, KIND_SINGLE_DELETE, KIND_MERGE] {
            assert!(check_point_kind(k).is_ok());
        }
        assert_eq!(
            check_point_kind(KIND_RANGE_DELETE).unwrap_err().kind(),
            "corruption"
        );
    }

    /// An extended entry and a legacy entry must describe the same thing with
    /// the same numbers, so the modifier bits mirror the flag bits.
    #[test]
    fn modifier_bits_match_legacy_flags() {
        assert_eq!(modifiers::HAS_TTL, u64::from(flags::HAS_TTL));
        assert_eq!(modifiers::HAS_VLOG, u64::from(flags::HAS_VLOG));
        assert_eq!(modifiers::KNOWN, 0x06);
        assert!(check_modifiers(modifiers::KNOWN).is_ok());
        // TOMBSTONE/SINGLE_DELETE are kinds, never modifiers.
        for m in [u64::from(flags::TOMBSTONE), u64::from(flags::SINGLE_DELETE)] {
            assert_eq!(check_modifiers(m).unwrap_err().kind(), "corruption");
        }
    }

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
