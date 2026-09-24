//! Every on-disk number of **yoloDB format epoch 1** — the magics and versions
//! of each artifact, the SST footer layout and flags, capability bits, record
//! kinds, codec ids, manifest section bits and config TLV tags — plus the
//! per-entry flag bits and the MVCC internal-key trailer. Each identifier is
//! pinned by a `const` assertion and a golden test.
//!
//! Every number assigned here is shared with wavesdb, the format sibling whose
//! objects this engine can mount. `docs/format-registry.md` is the registry;
//! [`wavesdb_reserved`] pins the numbers wavesdb owns, with build-time
//! assertions so ondaDB cannot claim one.
//!
//! An *internal key* is `user_key` followed by an 8-byte big-endian trailer
//! holding the bitwise complement of the sequence number.  Complementing makes
//! higher sequence numbers sort *first* within the same user key, so a forward
//! seek to `(user_key, !read_seq)` lands on the newest version visible at
//! `read_seq`.

/// Which on-disk format family a reader decodes.
///
/// Every artifact this binary **writes** is [`FormatProfile::Epoch1`]. The
/// other variant exists only to read an ondaDB 0.9.x directory through the
/// frozen decoders in `legacy_onda`, and only when that feature is compiled
/// in; it selects whole containers (footer, checksums, codec ids, bloom
/// encoding), never individual fields, so an epoch-1 reader can never be
/// talked into accepting a 0.9 byte by one field that happens to parse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FormatProfile {
    /// yoloDB format epoch 1: the only format this binary writes.
    #[default]
    Epoch1,
    /// ondaDB 0.9.x, read-only (`legacy_onda`).
    #[cfg(feature = "legacy-onda")]
    Onda09,
}

impl FormatProfile {
    /// The integrity checksum of this family's frames (blocks, vlog frames).
    #[inline]
    pub fn checksum(self, b: &[u8]) -> u32 {
        match self {
            FormatProfile::Epoch1 => crate::encoding::checksum(b),
            #[cfg(feature = "legacy-onda")]
            FormatProfile::Onda09 => crate::legacy_onda::checksum_ieee(b),
        }
    }

    /// The codec a block or vlog frame names by `id` in this family.
    #[inline]
    pub fn codec(self, id: u8) -> crate::error::Result<crate::config::Compression> {
        match self {
            FormatProfile::Epoch1 => crate::config::Compression::from_codec_id(id),
            #[cfg(feature = "legacy-onda")]
            FormatProfile::Onda09 => crate::legacy_onda::codec(id).ok_or_else(|| {
                crate::error::OndaError::Corruption(format!("0.9: unknown codec id {id}"))
            }),
        }
    }
}

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
/// `0x08` is deliberately absent. It once named a `DELTA_SEQ` encoding no
/// writer ever produced; it is now permanently assigned to wavesdb's
/// [`wavesdb_reserved::FLAG_VLOG_GROUPED`] and must never be reused here.
/// The decoders reject it, which is the correct outcome — a grouped vlog
/// pointer addresses a compression group this binary cannot decompress — but
/// they must keep rejecting it as *someone else's feature* rather than
/// reclaiming it. Record extensibility comes from kinds, not from the
/// remaining flag bits.
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

/// Format capabilities a database may enable, persisted as a fixed `u64` word
/// in the epoch-1 manifest header (see [`crate::manifest`]); a table declares
/// the subset its bytes use in its footer ([`sst_footer::TABLE_CAPS`]).
///
/// A capability is the *permission* to write a newer artifact, taken once and
/// durably, before the first byte using it exists. The values are pinned here
/// and golden-pinned in `tests/fixtures/epoch1/`: they are the interoperability
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

/// Bits, modifiers and kinds permanently assigned to **wavesdb**, the format
/// sibling these files are shared with (`attach_part_by_ref` here, mounts
/// there). They are listed as constants rather than prose so that the
/// compile-time assertions below can hold ondaDB to them: the failure this
/// prevents is not a missing feature but a *silent misread* — the same mask
/// meaning two different things in two engines that mount each other's
/// objects.
///
/// ondaDB implements none of these. Encountering one is
/// [`OndaError::UnsupportedFormat`](crate::OndaError), which is correct and
/// must stay correct; the reservation is what stops a future ondaDB feature
/// from claiming a number and decoding wavesdb's bytes as its own.
pub mod wavesdb_reserved {
    /// Entry flag `0x08`: the vlog pointer addresses a shared compression
    /// GROUP frame, with a uvarint in-group offset following the 16-byte
    /// pointer. wavesdb gates it behind [`CAP_VLOG_GROUPING`].
    pub const FLAG_VLOG_GROUPED: u8 = 0x08;
    /// The same encoding named in an extended record's modifier word.
    pub const MOD_VLOG_GROUPED: u64 = 0x08;
    /// Capability bit 7: wavesdb managed-sequence mode — persisted sequence
    /// ownership and a durable discard floor.
    pub const CAP_MANAGED_MODE: u64 = 1 << 7;
    /// Capability bit 8: wavesdb vlog compression grouping, the permission
    /// for [`FLAG_VLOG_GROUPED`].
    pub const CAP_VLOG_GROUPING: u64 = 1 << 8;
    /// Kind 19: wavesdb large-transaction spill descriptor.
    pub const KIND_SPILL_DESCRIPTOR: u64 = 19;
    /// Kind 32: wavesdb managed-sequence ownership record.
    pub const KIND_MANAGED_OWNERSHIP: u64 = 32;
    /// Kind 33: wavesdb managed-sequence discard floor.
    pub const KIND_MANAGED_DISCARD_FLOOR: u64 = 33;
}

// ondaDB must never assign a number wavesdb already writes. These fail the
// BUILD, not a test, because the damage is done at the moment such a constant
// is written down — every artifact produced afterwards carries the collision.
const _: () = assert!(KNOWN_ENTRY_FLAGS & wavesdb_reserved::FLAG_VLOG_GROUPED == 0);
const _: () = assert!(modifiers::KNOWN & wavesdb_reserved::MOD_VLOG_GROUPED == 0);
const _: () = assert!(
    KNOWN_CAPS & (wavesdb_reserved::CAP_MANAGED_MODE | wavesdb_reserved::CAP_VLOG_GROUPING) == 0
);
const _: () = assert!(wavesdb_reserved::KIND_MANAGED_DISCARD_FLOOR <= MAX_ASSIGNABLE_KIND);

// ===========================================================================
// yoloDB format epoch 1: every persisted identifier
// ===========================================================================
//
// Each artifact starts (or, for the SST footer, ends) with an 8-byte ASCII
// magic filling a `u64` slot exactly, followed by a version counting revisions
// *within* the epoch. A new magic is minted only for a future epoch break.
// `docs/format-registry.md` is the registry; every number below is pinned both
// by a `const` assertion (a collision fails the build) and by
// `tests::epoch1_identifiers_are_pinned` (a renumbering fails a test).

/// `true` iff every byte of `m` is printable ASCII: magics are readable in a
/// hex dump, which is the point of spelling them.
const fn is_printable_ascii(m: &[u8; 8]) -> bool {
    let mut i = 0;
    while i < 8 {
        if m[i] < 0x20 || m[i] > 0x7E {
            return false;
        }
        i += 1;
    }
    true
}

/// The SSTable footer (96 bytes, at the end of every `.klog`).
///
/// ```text
///  0  index handle      off u64 | len u64
/// 16  bloom handle      off u64 | len u64      (0, 0 when no filter)
/// 32  num_entries u64
/// 40  max_seq u64
/// 48  flags u32                                 (FLAG_BLOOM | FLAG_BTREE)
/// 52  format_version u32 = 1
/// 56  capability word u64                       (the table's subset of TABLE_CAPS)
/// 64  aux handle        off u64 | len u64      (0, 0 when no aux block)
/// 80  crc32c u32 over bytes 0..80
/// 84  reserved u32 = 0
/// 88  magic "YOLOST01"
/// ```
pub mod sst_footer {
    /// `YOLOST01`: the `01` names the format epoch.
    pub const MAGIC: [u8; 8] = *b"YOLOST01";
    /// Revision of the footer layout within epoch 1.
    pub const FORMAT_VERSION: u32 = 1;
    /// Fixed footer width.
    pub const SIZE: usize = 96;
    /// Offsets of the footer fields.
    pub const INDEX: usize = 0;
    pub const BLOOM: usize = 16;
    pub const NUM_ENTRIES: usize = 32;
    pub const MAX_SEQ: usize = 40;
    pub const FLAGS: usize = 48;
    pub const VERSION: usize = 52;
    pub const CAPS: usize = 56;
    pub const AUX: usize = 64;
    pub const CRC: usize = 80;
    pub const RESERVED: usize = 84;
    pub const MAGIC_AT: usize = 88;
    /// A bloom block is present (shared registry allocation).
    pub const FLAG_BLOOM: u32 = 0x01;
    /// The index block is a B+tree root (shared registry allocation).
    pub const FLAG_BTREE: u32 = 0x02;
    /// Every footer flag epoch 1 assigns. Format meaning lives in the
    /// capability word, never here: a bit outside this mask is
    /// `UnsupportedFormat`.
    pub const KNOWN_FLAGS: u32 = FLAG_BLOOM | FLAG_BTREE;
    /// The capability bits a table may declare in its footer word: the
    /// kind-bearing entry layout, merge operands, range-delete fragments in the
    /// aux block, and prefix-delta blocks. Database-level bits (edit log,
    /// periodic age, transaction decisions) describe no table byte and are
    /// `Corruption` in a footer.
    pub const TABLE_CAPS: u64 = super::CAP_EXTENDED_RECORDS
        | super::CAP_MERGE_OPERANDS
        | super::CAP_RANGE_DELETES
        | super::CAP_PREFIX_DELTA;
}

const _: () = assert!(is_printable_ascii(&sst_footer::MAGIC));
const _: () = assert!(sst_footer::SIZE == 96);
const _: () = assert!(sst_footer::CRC == 80 && sst_footer::RESERVED == 84);
const _: () = assert!(sst_footer::MAGIC_AT + 8 == sst_footer::SIZE);
const _: () = assert!(sst_footer::AUX + 16 == sst_footer::CRC);
const _: () = assert!(sst_footer::KNOWN_FLAGS == 0x03);
const _: () = assert!(sst_footer::TABLE_CAPS & !KNOWN_CAPS == 0);

/// The `MANIFEST` snapshot.
///
/// ```text
///  0  magic "YOLODBMF" | 8 version u32 = 1 | 12 caps u64 | 20 db_flags u32
/// 24  next_file_id u64 | 32 global_seq u64
/// 40  db sections, ascending bit order (see DB_*)
///     cf_count uvarint
///     per CF: cf_flags uvarint | name* | config* | cf sections (CF_*)
///             | sst_count uvarint
///       per SST: id, level, num_entries, num_tombstones, max_seq, klog_size,
///                vlog_size (uvarint) | min_key* | max_key*
///                | sst_flags uvarint | sst sections (SST_*)
///     crc32c u32 over everything before it
/// ```
pub mod manifest_file {
    pub const MAGIC: [u8; 8] = *b"YOLODBMF";
    pub const VERSION: u32 = 1;
    /// Fixed header width, before the first db section.
    pub const HEADER_LEN: usize = 40;
    /// Database flag: the unified WAL layout (no payload).
    pub const DB_UNIFIED_WAL: u32 = 1 << 0;
    /// Database flag: the shared-tier instance nonce, `u64`.
    pub const DB_INSTANCE_NONCE: u32 = 1 << 1;
    /// Database flag: edit-log cursor, `generation u64 | applied_through u64 |
    /// next_edit_id u64`.
    pub const DB_EDIT_LOG: u32 = 1 << 2;
    pub const DB_KNOWN: u32 = DB_UNIFIED_WAL | DB_INSTANCE_NONCE | DB_EDIT_LOG;
    /// CF flag: the unified-layout CF id, `u64` — present only when it is not
    /// FNV-1a-64 of the name (plan C, F5′).
    pub const CF_UNIFIED_ID: u64 = 1 << 0;
    pub const CF_KNOWN: u64 = CF_UNIFIED_ID;
    /// SST flag: partition name.
    pub const SST_PARTITION: u64 = 1 << 0;
    /// SST flag: tier name.
    pub const SST_TIER: u64 = 1 << 1;
    /// SST flag: shared-tier object stem.
    pub const SST_OBJECT: u64 = 1 << 2;
    /// SST flag: `max_entry_time`, uvarint of the `i64` as `u64`.
    pub const SST_MAX_ENTRY_TIME: u64 = 1 << 3;
    /// SST flag: `last_compaction_time` (requires `CAP_PERIODIC_AGE`).
    pub const SST_LAST_COMPACTION_TIME: u64 = 1 << 4;
    /// SST flag: range summary, `count | min_seq | max_seq | min_key* |
    /// max_key*` (requires `CAP_RANGE_DELETES`).
    pub const SST_RANGE: u64 = 1 << 5;
    pub const SST_KNOWN: u64 = SST_PARTITION
        | SST_TIER
        | SST_OBJECT
        | SST_MAX_ENTRY_TIME
        | SST_LAST_COMPACTION_TIME
        | SST_RANGE;
}

const _: () = assert!(is_printable_ascii(&manifest_file::MAGIC));
const _: () = assert!(manifest_file::DB_KNOWN == 0x07);
const _: () = assert!(manifest_file::CF_KNOWN == 0x01);
const _: () = assert!(manifest_file::SST_KNOWN == 0x3F);

/// The `MANIFEST-EDITS` header (32 bytes). Records follow it with the same
/// framing 0.9 used, under CRC32-C: `len u32 | crc32c u32 | payload`.
///
/// ```text
///  0 magic "YOLODBED" | 8 schema u32 = 1 | 12 base_applied_through u64
/// 20 snapshot_generation u64 | 28 crc32c u32 over bytes 0..28
/// ```
pub mod edit_log {
    pub const MAGIC: [u8; 8] = *b"YOLODBED";
    pub const SCHEMA: u32 = 1;
    pub const HEADER_BYTES: usize = 32;
}

const _: () = assert!(is_printable_ascii(&edit_log::MAGIC));
const _: () = assert!(edit_log::HEADER_BYTES == 8 + 4 + 8 + 8 + 4);

/// The WAL segment header: 32 bytes at offset 0 of every WAL stripe file,
/// written and fsynced before the first frame.
///
/// ```text
///  0 magic "YOLODBWL" | 8 version u32 = 1 | 12 layout u8 | 13 reserved [3] = 0
/// 16 generation u64 | 24 reserved u32 = 0 | 28 crc32c u32 over bytes 0..28
/// ```
pub mod wal_segment {
    pub const MAGIC: [u8; 8] = *b"YOLODBWL";
    pub const VERSION: u32 = 1;
    pub const HEADER_LEN: usize = 32;
    /// One WAL per column family; envelope schema 1.
    pub const LAYOUT_PER_CF: u8 = 1;
    /// One database-wide WAL with cf-id-prefixed keys; envelope schema 2.
    pub const LAYOUT_UNIFIED: u8 = 2;
}

const _: () = assert!(is_printable_ascii(&wal_segment::MAGIC));
const _: () = assert!(wal_segment::HEADER_LEN == 32);
const _: () = assert!(wal_segment::LAYOUT_PER_CF != wal_segment::LAYOUT_UNIFIED);

/// The value-log header: 32 bytes at offset 0 of every `.vlog`. Frame offsets
/// in klog entries are absolute, so the first frame is at [`HEADER_LEN`].
///
/// ```text
///  0 magic "YOLODBVL" | 8 version u32 = 1 | 12 flags u32 (reserved, 0)
/// 16 reserved [12] = 0 | 28 crc32c u32 over bytes 0..28
/// ```
///
/// [`HEADER_LEN`]: vlog_header::HEADER_LEN
pub mod vlog_header {
    pub const MAGIC: [u8; 8] = *b"YOLODBVL";
    pub const VERSION: u32 = 1;
    pub const HEADER_LEN: usize = 32;
    /// No flag is assigned in epoch 1; per-file dictionaries and compression
    /// groups (plan C step 2) will take bits here.
    pub const KNOWN_FLAGS: u32 = 0;
}

const _: () = assert!(is_printable_ascii(&vlog_header::MAGIC));
const _: () = assert!(vlog_header::HEADER_LEN == 32);

/// The format-upgrade swap journal, `<parent>/.<name>.yolo-upgrade.journal`
/// (plan C §1.3): `magic | version u32 | state u8 | (len u32, utf8)×3 —
/// database, upgrade-dir and backup-dir names | crc32c u32` over everything
/// before it. It lives **beside** the database, never inside it, because the
/// swap it journals renames the database directory itself.
pub mod upgrade_journal {
    pub const MAGIC: [u8; 8] = *b"YOLODBUJ";
    pub const VERSION: u32 = 1;
    /// Both renames may be in flight; the next open resolves it.
    pub const STATE_SWAPPING: u8 = 1;
    /// Both renames are durable; only cleanup remains.
    pub const STATE_DONE: u8 = 2;
}

const _: () = assert!(is_printable_ascii(&upgrade_journal::MAGIC));
const _: () = assert!(upgrade_journal::STATE_SWAPPING != upgrade_journal::STATE_DONE);

/// The column-family config blob: `magic | version u32 | (tag uvarint, len
/// uvarint, bytes)*`, tags strictly ascending. Unknown tags are preserved on a
/// decode→encode round trip. Durations are nanoseconds.
pub mod cf_config {
    pub const MAGIC: [u8; 8] = *b"YOLODBCF";
    pub const VERSION: u32 = 1;
    pub const HEADER_LEN: usize = 12;

    /// Stable TLV tag numbers, one per durable `ColumnFamilyConfig` field.
    /// Registered in `docs/format-registry.md`; never renumbered, only retired.
    pub mod tag {
        pub const COMPARATOR_NAME: u64 = 1;
        pub const COMPRESSION: u64 = 2;
        pub const WRITE_BUFFER_SIZE: u64 = 3;
        pub const LEVEL_SIZE_RATIO: u64 = 4;
        pub const KLOG_VALUE_THRESHOLD: u64 = 5;
        pub const ENABLE_BLOOM_FILTER: u64 = 6;
        pub const BLOOM_FPR: u64 = 7;
        pub const L1_FILE_COUNT_TRIGGER: u64 = 8;
        pub const L0_QUEUE_STALL_THRESHOLD: u64 = 9;
        pub const USE_BTREE: u64 = 10;
        pub const SYNC_MODE: u64 = 11;
        pub const SYNC_INTERVAL: u64 = 12;
        pub const COMPRESSION_PER_LEVEL: u64 = 13;
        pub const COMPACTION_STYLE: u64 = 14;
        pub const FIFO_MAX_BYTES: u64 = 15;
        pub const FIFO_TTL: u64 = 16;
        pub const COMPRESSION_RULES: u64 = 17;
        pub const PARTITION_RULES: u64 = 18;
        pub const TIER_RULES: u64 = 19;
        pub const PARTITION_SCHEME: u64 = 20;
        pub const TARGET_FILE_SIZE: u64 = 21;
        pub const L1_BASE_BYTES: u64 = 22;
        pub const SOFT_PENDING_COMPACTION_BYTES: u64 = 23;
        pub const HARD_PENDING_COMPACTION_BYTES: u64 = 24;
        pub const DATA_BLOCK_SIZE: u64 = 25;
        pub const MAX_CACHED_VLOG_VALUE_BYTES: u64 = 26;
        pub const BLOOM_FPR_PER_LEVEL: u64 = 27;
        pub const OPTIMIZE_FILTERS_FOR_HITS: u64 = 28;
        pub const PERIODIC_COMPACTION_INTERVAL: u64 = 29;
        pub const ENABLE_PREFIX_DELTA_KEYS: u64 = 30;
        pub const BLOCK_RESTART_INTERVAL: u64 = 31;
        pub const MERGE_OPERATOR_NAME: u64 = 32;
        /// `bloom_auto_allocate` (plan C P7, wavesdb `BloomAutoAllocate`):
        /// bool. The successor of 0.9's reserved `ONDABLM2` tail.
        pub const BLOOM_AUTO_ALLOCATE: u64 = 33;
        /// The name tag 33 had while it was reserved.
        pub const RESERVED_BLOOM_AUTO_ALLOCATE: u64 = BLOOM_AUTO_ALLOCATE;
        /// Highest tag epoch 1 assigns a meaning to.
        pub const MAX_KNOWN: u64 = BLOOM_AUTO_ALLOCATE;
    }
}

const _: () = assert!(is_printable_ascii(&cf_config::MAGIC));
const _: () = assert!(cf_config::tag::MAX_KNOWN == 33);
const _: () = assert!(cf_config::tag::BLOOM_AUTO_ALLOCATE == 33);

/// Compression codec ids — the `alg` byte of every block frame and vlog frame.
///
/// Ids 2 and 4 are **burned**: they meant LZ4 in ondaDB 0.9 and zstd in
/// wavesdb, so neither engine may ever assign them again, and an epoch-1 file
/// naming one is `UnsupportedFormat`. LZ4 moved to 6, whose bytes are
/// identical to 0.9's id-2 bytes (a raw LZ4 block).
pub mod codec {
    pub const NONE: u8 = 0;
    pub const SNAPPY: u8 = 1;
    /// Burned (0.9 LZ4 / wavesdb zstd). Never written; refused.
    pub const BURNED_2: u8 = 2;
    pub const ZSTD: u8 = 3;
    /// Burned (0.9 LZ4-fast / wavesdb zstd). Never written; refused.
    pub const BURNED_4: u8 = 4;
    /// Raw deflate.
    pub const DEFLATE: u8 = 5;
    /// A raw LZ4 block (`lz4_flex::compress`).
    pub const LZ4: u8 = 6;
    /// Zstd with a per-file dictionary — reserved until plan C step 2.
    pub const RESERVED_ZSTD_DICT: u8 = 7;
    /// Brotli — reserved (plan C row S).
    pub const RESERVED_BROTLI: u8 = 8;
}

const _: () = assert!(codec::LZ4 != codec::BURNED_2 && codec::LZ4 != codec::BURNED_4);
const _: () = assert!(codec::NONE == 0 && codec::SNAPPY == 1 && codec::ZSTD == 3);
const _: () = assert!(codec::DEFLATE == 5 && codec::LZ4 == 6);

/// Bloom-filter hash id: the **leading** byte of an epoch-1 bloom block,
/// `hash u8 | m uvarint | k uvarint | words u64 LE`. Only xxh3-64 is written.
pub const BLOOM_HASH_XXH3: u8 = 1;
/// The id 0.9 used for its FNV filters. Registered so it is never reassigned;
/// an epoch-1 file naming it is `UnsupportedFormat`.
pub const BLOOM_HASH_FNV_09: u8 = 0;

/// FNV-1a-64 offset basis — the **correct** one. 0.9 used
/// `1469598103934665603` (a digit short), which survives only in `legacy_onda`.
pub const FNV1A64_OFFSET_BASIS: u64 = 14695981039346656037;
/// FNV-1a-64 prime.
pub const FNV1A64_PRIME: u64 = 1099511628211;

const _: () = assert!(FNV1A64_OFFSET_BASIS == 0xcbf2_9ce4_8422_2325);

/// Prefix of every column family's directory (and object-store key stem).
pub const CF_DIR_PREFIX: &str = "cf-";

/// The directory (and shared-tier object prefix) of column family `name`:
/// `cf-<name>`.
///
/// The only place the spelling lives. wavesdb writes `cf_<name>`, and plan C
/// step 2 (row H) picks one for both engines; routing every path through this
/// function is what makes that switch a one-line change.
pub fn cf_dir_name(name: &str) -> String {
    format!("{CF_DIR_PREFIX}{name}")
}

/// FNV-1a-64 of `bytes`: the unified-layout column-family id of a name.
pub const fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = FNV1A64_OFFSET_BASIS;
    let mut i = 0;
    while i < bytes.len() {
        h ^= bytes[i] as u64;
        h = h.wrapping_mul(FNV1A64_PRIME);
        i += 1;
    }
    h
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
        KIND_PUT | KIND_DELETE | KIND_SINGLE_DELETE | KIND_MERGE | KIND_RANGE_DELETE
        | KIND_PREPARE | KIND_COMMIT_DECISION | KIND_ABORT_DECISION => Ok(()),
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

    /// Every epoch-1 identifier, as the literal bytes and numbers on disk. A
    /// renumbering here is a format break and must be deliberate.
    #[test]
    fn epoch1_identifiers_are_pinned() {
        assert_eq!(&sst_footer::MAGIC, b"YOLOST01");
        assert_eq!(
            sst_footer::MAGIC,
            [0x59, 0x4F, 0x4C, 0x4F, 0x53, 0x54, 0x30, 0x31]
        );
        assert_eq!(sst_footer::FORMAT_VERSION, 1);
        assert_eq!(sst_footer::SIZE, 96);
        assert_eq!(
            (sst_footer::FLAG_BLOOM, sst_footer::FLAG_BTREE),
            (0x01, 0x02)
        );
        assert_eq!(sst_footer::TABLE_CAPS, 0x0F);
        assert_eq!(&manifest_file::MAGIC, b"YOLODBMF");
        assert_eq!(manifest_file::VERSION, 1);
        assert_eq!(manifest_file::HEADER_LEN, 40);
        assert_eq!(&edit_log::MAGIC, b"YOLODBED");
        assert_eq!(edit_log::SCHEMA, 1);
        assert_eq!(edit_log::HEADER_BYTES, 32);
        assert_eq!(&wal_segment::MAGIC, b"YOLODBWL");
        assert_eq!(wal_segment::VERSION, 1);
        assert_eq!(
            (wal_segment::LAYOUT_PER_CF, wal_segment::LAYOUT_UNIFIED),
            (1, 2)
        );
        assert_eq!(&vlog_header::MAGIC, b"YOLODBVL");
        assert_eq!(vlog_header::VERSION, 1);
        assert_eq!(&upgrade_journal::MAGIC, b"YOLODBUJ");
        assert_eq!(upgrade_journal::VERSION, 1);
        assert_eq!(
            (upgrade_journal::STATE_SWAPPING, upgrade_journal::STATE_DONE),
            (1, 2)
        );
        assert_eq!(&cf_config::MAGIC, b"YOLODBCF");
        assert_eq!(cf_config::VERSION, 1);
        assert_eq!(
            [
                codec::NONE,
                codec::SNAPPY,
                codec::ZSTD,
                codec::DEFLATE,
                codec::LZ4
            ],
            [0, 1, 3, 5, 6]
        );
        assert_eq!(
            [codec::BURNED_2, codec::BURNED_4],
            [2, 4],
            "burned ids stay burned"
        );
        assert_eq!([codec::RESERVED_ZSTD_DICT, codec::RESERVED_BROTLI], [7, 8]);
        assert_eq!((BLOOM_HASH_FNV_09, BLOOM_HASH_XXH3), (0, 1));
        assert_eq!(FNV1A64_OFFSET_BASIS, 14695981039346656037);
        // The FNV-1a-64 reference vectors.
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    /// The manifest section bits and the config tags, one literal each.
    #[test]
    fn epoch1_section_bits_and_config_tags_are_pinned() {
        use cf_config::tag::*;
        use manifest_file::*;
        assert_eq!(
            [DB_UNIFIED_WAL, DB_INSTANCE_NONCE, DB_EDIT_LOG],
            [0x1, 0x2, 0x4]
        );
        assert_eq!(CF_UNIFIED_ID, 0x1);
        assert_eq!(
            [
                SST_PARTITION,
                SST_TIER,
                SST_OBJECT,
                SST_MAX_ENTRY_TIME,
                SST_LAST_COMPACTION_TIME,
                SST_RANGE
            ],
            [0x01, 0x02, 0x04, 0x08, 0x10, 0x20]
        );
        let tags = [
            COMPARATOR_NAME,
            COMPRESSION,
            WRITE_BUFFER_SIZE,
            LEVEL_SIZE_RATIO,
            KLOG_VALUE_THRESHOLD,
            ENABLE_BLOOM_FILTER,
            BLOOM_FPR,
            L1_FILE_COUNT_TRIGGER,
            L0_QUEUE_STALL_THRESHOLD,
            USE_BTREE,
            SYNC_MODE,
            SYNC_INTERVAL,
            COMPRESSION_PER_LEVEL,
            COMPACTION_STYLE,
            FIFO_MAX_BYTES,
            FIFO_TTL,
            COMPRESSION_RULES,
            PARTITION_RULES,
            TIER_RULES,
            PARTITION_SCHEME,
            TARGET_FILE_SIZE,
            L1_BASE_BYTES,
            SOFT_PENDING_COMPACTION_BYTES,
            HARD_PENDING_COMPACTION_BYTES,
            DATA_BLOCK_SIZE,
            MAX_CACHED_VLOG_VALUE_BYTES,
            BLOOM_FPR_PER_LEVEL,
            OPTIMIZE_FILTERS_FOR_HITS,
            PERIODIC_COMPACTION_INTERVAL,
            ENABLE_PREFIX_DELTA_KEYS,
            BLOCK_RESTART_INTERVAL,
            MERGE_OPERATOR_NAME,
            BLOOM_AUTO_ALLOCATE,
        ];
        assert_eq!(tags.to_vec(), (1..=33).collect::<Vec<u64>>());
        assert_eq!(BLOOM_AUTO_ALLOCATE, 33);
        assert_eq!(MAX_KNOWN, 33);
    }

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
