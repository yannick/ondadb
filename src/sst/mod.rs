//! ondaDB SSTables: immutable sorted runs produced by memtable flushes and
//! compaction.
//!
//! An SSTable is a klog file (keys + small values + metadata) and an optional
//! vlog file (large values — WiscKey key/value separation). yoloDB epoch-1
//! klog layout:
//!
//! ```text
//! [data block 0] .. [data block N-1] [bloom?] [aux?] [index] [footer 96B]
//! ```
//!
//! Blocks are framed by [`crate::block`]. Every data block carries a restart
//! trailer. Data-block entries are in internal order (user key ascending,
//! sequence descending), in one of three table-level layouts the footer's
//! capability word selects:
//!
//! ```text
//! base      flags(1) | key_len | val_len | seq | ttl? | key | value|vlog_off
//! extended  kind | modifiers | key_len | val_len | seq | ttl? | key | value|vlog_off
//! delta     kind | modifiers | shared | suffix_len | val_len | seq | ttl? | suffix | …
//! ```
//!
//! The footer layout is [`crate::format::sst_footer`]; it is CRC32-C
//! checksummed and names its format version and the table's capability
//! subset. A 0.9 table (`WAVESST1`) is decoded only through `legacy_onda`.

mod iter;
mod reader;
mod writer;

// The arena memtable caches key prefixes on its own nodes, so this re-export
// is only consumed by the default build's memtable merge.
#[cfg_attr(feature = "arena-memtable", allow(unused_imports))]
pub(crate) use iter::key_prefix8;
pub use iter::SstIterator;
pub use reader::Reader;
pub use writer::{Writer, WriterOptions};

use crate::config::Compression;
use crate::encoding::{
    append_u64, append_uvarint, append_varint, put_u32, put_u64, read_u32, read_u64, uvarint,
    varint,
};
use crate::error::{OndaError, Result};
use crate::format::{flags, sst_footer};

/// Fixed footer size in bytes (epoch 1).
pub(crate) const FOOTER_SIZE: usize = sst_footer::SIZE;
/// Entries per restart interval written by default.
pub(crate) const RESTART_INTERVAL: usize = 8;
/// Default target data-block size used by low-level writers when their option
/// is zero. Engine write paths pass `ColumnFamilyConfig::data_block_size`
/// explicitly; its default is the same 4 KiB value. Existing files are
/// unaffected because block boundaries are self-describing.
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4 << 10;
/// Length of the value-log frame header: crc32c(4) + alg(1) + stored_len(4).
/// Epoch 1 has one frame format — 0.9's "v2" frame.
pub(crate) const VLOG_FRAME_HDR_LEN: usize = 9;
/// Length of the value-log file header ([`crate::format::vlog_header`]); the
/// first frame starts here.
pub(crate) const VLOG_HEADER_LEN: usize = crate::format::vlog_header::HEADER_LEN;

/// How a table's value-log frames are laid out. Epoch 1 has exactly one
/// shape; the other two exist only to read 0.9 tables through `legacy_onda`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VlogFrames {
    /// A 32-byte file header, then `crc32c | alg | stored_len | stored`.
    Epoch1,
    /// 0.9 "v2": no file header, `crc | alg | stored_len | stored` (IEEE).
    #[cfg(feature = "legacy-onda")]
    Onda09V2,
    /// 0.9 "v1": no file header, `crc | raw value` (IEEE).
    #[cfg(feature = "legacy-onda")]
    Onda09V1,
}

/// A decoded footer, normalized over both format families. [`Reader::open`]
/// works from this alone, so the family-specific parsing stays in
/// [`decode_footer`] and `legacy_onda::sst`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TableFooter {
    pub index: BlockHandle,
    /// `None` when the table has no filter.
    pub bloom: Option<BlockHandle>,
    pub num_entries: u64,
    pub max_seq: u64,
    pub btree: bool,
    /// The table's capability subset: which entry layout its blocks use and
    /// which aux sections it may carry.
    pub caps: u64,
    /// Data blocks carry a restart trailer. Always true in epoch 1.
    pub restarts: bool,
    pub vlog: VlogFrames,
    /// The aux-block handle; `Some((0, 0))`-shaped when the table has none.
    /// `None` only for a 0.9 table without the extended layout, which had no
    /// place to put one.
    pub aux: Option<BlockHandle>,
}

impl TableFooter {
    /// The entry layout the capability word selects.
    pub(crate) fn entry_layout(&self) -> EntryLayout {
        if self.caps & crate::format::CAP_EXTENDED_RECORDS != 0 {
            EntryLayout::Extended
        } else {
            EntryLayout::Base
        }
    }

    pub(crate) fn prefix_delta(&self) -> bool {
        self.caps & crate::format::CAP_PREFIX_DELTA != 0
    }
}

/// The fields a writer puts in an epoch-1 footer.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FooterFields {
    pub index: BlockHandle,
    pub bloom: Option<BlockHandle>,
    pub num_entries: u64,
    pub max_seq: u64,
    pub btree: bool,
    pub caps: u64,
    pub aux: Option<BlockHandle>,
}

/// Encode the 96-byte epoch-1 footer ([`crate::format::sst_footer`]).
pub(crate) fn encode_footer(f: &FooterFields) -> [u8; FOOTER_SIZE] {
    use sst_footer::*;
    let mut b = [0u8; FOOTER_SIZE];
    put_u64(&mut b[INDEX..], f.index.offset);
    put_u64(&mut b[INDEX + 8..], f.index.length);
    let bloom = f.bloom.unwrap_or_default();
    put_u64(&mut b[BLOOM..], bloom.offset);
    put_u64(&mut b[BLOOM + 8..], bloom.length);
    put_u64(&mut b[NUM_ENTRIES..], f.num_entries);
    put_u64(&mut b[MAX_SEQ..], f.max_seq);
    let mut flags = 0u32;
    if f.bloom.is_some() {
        flags |= FLAG_BLOOM;
    }
    if f.btree {
        flags |= FLAG_BTREE;
    }
    put_u32(&mut b[FLAGS..], flags);
    put_u32(&mut b[VERSION..], FORMAT_VERSION);
    put_u64(&mut b[CAPS..], f.caps);
    let aux = f.aux.unwrap_or_default();
    put_u64(&mut b[AUX..], aux.offset);
    put_u64(&mut b[AUX + 8..], aux.length);
    let crc = crate::encoding::checksum(&b[..CRC]);
    put_u32(&mut b[CRC..], crc);
    // RESERVED stays zero.
    b[MAGIC_AT..].copy_from_slice(&MAGIC);
    b
}

/// The last eight bytes of a 0.9 klog, as that binary stored its footer magic.
const ONDA09_FOOTER_MAGIC: u64 = 0x5741_5645_5353_5431;

/// Decode the epoch-1 footer from `tail`, the last [`FOOTER_SIZE`] bytes of a
/// klog of `file_len` bytes.
///
/// Fail-closed, in this order: a foreign magic is `Corruption` (a 0.9
/// `WAVESST1` magic is `UnsupportedFormat` naming the upgrade path); a format
/// version other than 1 is `UnsupportedFormat` — its CRC position is not ours
/// to know; then the CRC32-C over bytes 0..80, the reserved word, the flag
/// mask (`UnsupportedFormat`), the capability word (unknown bit:
/// `UnsupportedFormat`; a database-level or dependency-violating bit:
/// `Corruption`) and every handle's bounds (`Corruption`).
pub(crate) fn decode_footer(tail: &[u8], file_len: u64) -> Result<TableFooter> {
    use sst_footer::*;
    let corrupt = |what: &str| OndaError::Corruption(format!("sst footer: {what}"));
    if tail.len() < 8 || file_len < tail.len() as u64 {
        return Err(corrupt("file shorter than a footer"));
    }
    let last8 = &tail[tail.len() - 8..];
    if last8 != MAGIC {
        if read_u64(last8) == ONDA09_FOOTER_MAGIC {
            return Err(OndaError::UnsupportedFormat(
                "sst: an ondaDB 0.9 table (WAVESST1 footer); it is readable only through \
                 legacy_onda, and the database must be upgraded to yoloDB epoch 1"
                    .into(),
            ));
        }
        return Err(corrupt("magic is not YOLOST01"));
    }
    if tail.len() < SIZE {
        return Err(corrupt("file shorter than a footer"));
    }
    let b = &tail[tail.len() - SIZE..];
    let version = read_u32(&b[VERSION..]);
    if version != FORMAT_VERSION {
        return Err(OndaError::UnsupportedFormat(format!(
            "sst footer format version {version} is not implemented by this binary"
        )));
    }
    if read_u32(&b[CRC..]) != crate::encoding::checksum(&b[..CRC]) {
        return Err(corrupt("checksum mismatch"));
    }
    if read_u32(&b[RESERVED..]) != 0 {
        return Err(corrupt("reserved word is not zero"));
    }
    let flags = read_u32(&b[FLAGS..]);
    if flags & !KNOWN_FLAGS != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "sst footer flags {flags:#x} outside known mask {KNOWN_FLAGS:#x}"
        )));
    }
    let caps = read_u64(&b[CAPS..]);
    crate::format::check_caps(caps)?;
    if caps & !TABLE_CAPS != 0 {
        return Err(corrupt("capability word names a database-level bit"));
    }
    if caps & !crate::format::CAP_EXTENDED_RECORDS != 0
        && caps & crate::format::CAP_EXTENDED_RECORDS == 0
    {
        // Merge operands, range fragments and prefix-delta blocks are all
        // defined over the kind-bearing entry; no writer declares one without it.
        return Err(corrupt("capability without CAP_EXTENDED_RECORDS"));
    }
    // Every handle must address bytes ahead of the footer; checking here keeps
    // a garbage length from ever becoming an allocation.
    let limit = file_len - SIZE as u64;
    let handle = |at: usize| -> Result<BlockHandle> {
        let h = BlockHandle {
            offset: read_u64(&b[at..]),
            length: read_u64(&b[at + 8..]),
        };
        if h.offset > limit || h.length > limit - h.offset {
            return Err(corrupt("handle past the end of the file"));
        }
        Ok(h)
    };
    let index = handle(INDEX)?;
    let bloom = handle(BLOOM)?;
    let aux = handle(AUX)?;
    let has_bloom = flags & FLAG_BLOOM != 0;
    if has_bloom != (bloom.length > 0) || (!has_bloom && bloom.offset != 0) {
        return Err(corrupt("bloom flag disagrees with the bloom handle"));
    }
    if aux.length == 0 && aux.offset != 0 {
        return Err(corrupt("empty aux handle with an offset"));
    }
    Ok(TableFooter {
        index,
        bloom: has_bloom.then_some(bloom),
        num_entries: read_u64(&b[NUM_ENTRIES..]),
        max_seq: read_u64(&b[MAX_SEQ..]),
        btree: flags & FLAG_BTREE != 0,
        caps,
        restarts: true,
        vlog: VlogFrames::Epoch1,
        aux: Some(aux),
    })
}

/// Encode the 32-byte epoch-1 value-log header ([`crate::format::vlog_header`]).
pub(crate) fn encode_vlog_header() -> [u8; VLOG_HEADER_LEN] {
    use crate::format::vlog_header::*;
    let mut b = [0u8; HEADER_LEN];
    b[..8].copy_from_slice(&MAGIC);
    put_u32(&mut b[8..], VERSION);
    // flags (12..16) and reserved (16..28) are zero in epoch 1.
    let crc = crate::encoding::checksum(&b[..28]);
    put_u32(&mut b[28..], crc);
    b
}

/// Validate an epoch-1 value-log header: magic and CRC32-C (`Corruption`),
/// version and flags (`UnsupportedFormat`), reserved bytes (`Corruption`).
pub(crate) fn check_vlog_header(b: &[u8]) -> Result<()> {
    use crate::format::vlog_header::*;
    let corrupt = |what: &str| OndaError::Corruption(format!("vlog header: {what}"));
    if b.len() < HEADER_LEN {
        return Err(corrupt("file shorter than its header"));
    }
    if b[..8] != MAGIC {
        return Err(corrupt("magic is not YOLODBVL"));
    }
    let version = read_u32(&b[8..]);
    if version != VERSION {
        return Err(OndaError::UnsupportedFormat(format!(
            "vlog header version {version} is not implemented by this binary"
        )));
    }
    if read_u32(&b[28..]) != crate::encoding::checksum(&b[..28]) {
        return Err(corrupt("checksum mismatch"));
    }
    let flags = read_u32(&b[12..]);
    if flags & !KNOWN_FLAGS != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "vlog header flags {flags:#x} are not implemented by this binary"
        )));
    }
    if b[16..28].iter().any(|&x| x != 0) {
        return Err(corrupt("reserved bytes are not zero"));
    }
    Ok(())
}

/// Metadata describing a finished SSTable. `id` and paths are assigned by the
/// caller (the column family).
#[derive(Debug, Clone, Default)]
pub struct FileMeta {
    pub id: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub max_seq: u64,
    pub klog_size: u64,
    pub vlog_size: u64,
    /// Range-tombstone fragments written into this file's aux section (1.2),
    /// summarized for the catalog. All zero/`None` when the writer was handed
    /// no fragments — which is every table of a database that has not enabled
    /// [`CAP_RANGE_DELETES`](crate::format::CAP_RANGE_DELETES).
    pub range_count: u64,
    pub range_min_seq: u64,
    pub range_max_seq: u64,
    pub range_min_key: Option<Vec<u8>>,
    pub range_max_key: Option<Vec<u8>>,
}

impl FileMeta {
    /// Build a manifest [`SstMeta`](crate::manifest::SstMeta) from this finished
    /// file, assigning its `id` and `level`.
    pub fn to_sst_meta(&self, id: u64, level: u32) -> crate::manifest::SstMeta {
        crate::manifest::SstMeta {
            id,
            level,
            num_entries: self.num_entries,
            num_tombstones: self.num_tombstones,
            max_seq: self.max_seq,
            klog_size: self.klog_size,
            vlog_size: self.vlog_size,
            min_key: self.min_key.clone(),
            max_key: self.max_key.clone(),
            object: None,
            // Partition is a bottom-level compaction concern; the writer/flush
            // paths leave it None and compaction stamps boundary-cut files.
            partition: None,
            // Tier is assigned by the part mover / attach path, not at write
            // time — a freshly written file always lives on the default tier.
            tier: None,
            // Age is stamped by the caller that knows the context: flush/ingest
            // uses the write time, compaction carries the max over its inputs.
            max_entry_time: None,
            // Likewise for the periodic-compaction stamp: only a caller holding
            // CAP_PERIODIC_AGE may set it, and only flush/ingest and compaction
            // output know which clock reading applies.
            last_compaction_time: None,
            // Range summary, straight from the fragments the writer was given.
            range_count: self.range_count,
            range_min_seq: self.range_min_seq,
            range_max_seq: self.range_max_seq,
            range_min_key: self.range_min_key.clone(),
            range_max_key: self.range_max_key.clone(),
        }
    }
}

/// Derive the vlog path from a klog path.
pub(crate) fn vlog_path_for(klog_path: &str) -> String {
    if let Some(stripped) = klog_path.strip_suffix(".klog") {
        format!("{stripped}.vlog")
    } else {
        format!("{klog_path}.vlog")
    }
}

/// A block handle (offset + framed length) within the klog.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BlockHandle {
    pub offset: u64,
    pub length: u64,
}

/// A decompressed data block, either owned (read+decompressed, possibly cached)
/// or, under `mmap-reads`, a zero-copy view into an mmap'd file.
#[derive(Clone, Debug)]
pub(crate) enum Block {
    Owned(std::sync::Arc<[u8]>),
    #[cfg(feature = "mmap-reads")]
    Mapped {
        mmap: std::sync::Arc<memmap2::Mmap>,
        start: usize,
        len: usize,
    },
}

impl Block {
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Block::Owned(a) => a,
            #[cfg(feature = "mmap-reads")]
            Block::Mapped { mmap, start, len } => &mmap[*start..*start + *len],
        }
    }

    /// Whether two handles view the same underlying block (same allocation and,
    /// for mmaps, the same window). Used to reuse a pinned block instead of
    /// bumping the shared refcount on every entry.
    #[inline]
    pub(crate) fn same_backing(&self, other: &Block) -> bool {
        match (self, other) {
            (Block::Owned(a), Block::Owned(b)) => std::sync::Arc::ptr_eq(a, b),
            #[cfg(feature = "mmap-reads")]
            (
                Block::Mapped {
                    mmap: a, start: sa, ..
                },
                Block::Mapped {
                    mmap: b, start: sb, ..
                },
            ) => std::sync::Arc::ptr_eq(a, b) && sa == sb,
            #[cfg(feature = "mmap-reads")]
            _ => false,
        }
    }
}

/// An index separator entry (one per data block).
#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub handle: BlockHandle,
}

/// A decoded data-block entry, with slices addressed by offset into the block.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DecEntry {
    /// Start of the key bytes stored *in this entry*: the whole user key for a
    /// legacy/extended entry, the suffix for a prefix-delta one.
    pub key_start: usize,
    /// Length of the key bytes stored in this entry (see [`Self::key_start`]).
    pub key_len: usize,
    /// Bytes this entry's user key shares with its predecessor's; always `0`
    /// outside a prefix-delta block, and `0` at every restart anchor inside
    /// one. When non-zero the user key is not contiguous in the block, so
    /// [`Self::user_key`] must not be used — the caller holds the
    /// reconstructed key instead (see [`decode_entry_delta`]).
    pub key_shared: usize,
    pub val_start: usize,
    pub val_len: usize, // logical value length (also for vlog values)
    pub seq: u64,
    pub ttl: i64,
    pub flags: u8,
    /// Record kind. A legacy entry has no kind field on disk, so it decodes to
    /// the [`point_kind`](crate::format::point_kind) its flags describe — every
    /// consumer can then ask one question instead of two.
    ///
    /// Narrowed to a byte deliberately: kinds are bounded by
    /// [`MAX_ASSIGNABLE_KIND`](crate::format::MAX_ASSIGNABLE_KIND) (63) and
    /// [`check_kind`](crate::format::check_kind) runs before the cast, so the
    /// byte packs into the padding beside `flags` and `DecEntry` stays the size
    /// it was before 1.1. This entry is returned **by value** once per decoded
    /// entry, and growing it by 8 bytes cost a measurable ~5 % on a full scan
    /// of a family that has no merge operator at all (see
    /// `bench-results/1.1/`).
    pub kind: u8,
    pub vlog_off: u64,
}

impl DecEntry {
    pub fn tombstone(&self) -> bool {
        self.flags & flags::TOMBSTONE != 0
    }
    pub fn single_delete(&self) -> bool {
        self.flags & flags::SINGLE_DELETE != 0
    }
    pub fn has_vlog(&self) -> bool {
        self.flags & flags::HAS_VLOG != 0
    }
    /// The entry's user key, borrowed from the block.
    ///
    /// Only valid when [`key_shared`](Self::key_shared) is zero: a delta entry
    /// that shares bytes with its predecessor has no contiguous key anywhere in
    /// the block, and this would return only its suffix. Restart anchors always
    /// qualify, which is what lets the anchor binary search run with no
    /// materialization at all.
    pub fn user_key<'a>(&self, raw: &'a [u8]) -> &'a [u8] {
        debug_assert_eq!(
            self.key_shared, 0,
            "user_key() on a delta entry that shares a prefix"
        );
        &raw[self.key_start..self.key_start + self.key_len]
    }
    pub fn inline_value<'a>(&self, raw: &'a [u8]) -> &'a [u8] {
        &raw[self.val_start..self.val_start + self.val_len]
    }
}

/// Which entry layout a table's data blocks use — a table-level property read
/// once from the footer's capability word at [`Reader::open`] and threaded to
/// every decode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum EntryLayout {
    /// `flags(1) | klen | vlen | seq | ttl? | key | value|vlog_off` — a table
    /// without `CAP_EXTENDED_RECORDS`: only the three point kinds.
    #[default]
    Base,
    /// `kind uv | modifiers uv | klen | vlen | seq | ttl? | key | value|vlog_off`
    /// (`CAP_EXTENDED_RECORDS` in the footer's capability word).
    Extended,
}

/// Append one data-block entry to `dst` in `layout`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_entry(
    dst: &mut Vec<u8>,
    layout: EntryLayout,
    user_key: &[u8],
    value: &[u8],
    seq: u64,
    ttl: i64,
    kind: u64,
    has_vlog: bool,
    vlog_off: u64,
) {
    let (tombstone, single_delete) = kind_to_point_flags(kind);
    crate::format::debug_check_entry_flags(tombstone, single_delete, has_vlog);
    let fl = crate::format::normalized_entry_flags(tombstone, single_delete, ttl != 0, has_vlog);
    // Normalization may have cleared HAS_VLOG (a tombstone has no separated
    // value); the layout below must follow the byte that was actually written.
    let has_vlog = fl & flags::HAS_VLOG != 0;
    match layout {
        EntryLayout::Base => {
            // A base entry has nowhere to put a kind, so only the three point
            // kinds are representable. `Writer::add` refuses the rest before
            // reaching here; this is the last line of defence.
            debug_assert!(
                crate::format::is_point_kind(kind),
                "legacy block entry cannot carry kind {kind}"
            );
            dst.push(fl);
        }
        EntryLayout::Extended => {
            // The modifier bits are the flag bits, so an extended entry and a
            // legacy entry describe the same thing with the same numbers; the
            // tombstone/single-delete distinction lives in the kind.
            let mods = u64::from(fl) & crate::format::modifiers::KNOWN;
            append_uvarint(dst, kind);
            append_uvarint(dst, mods);
        }
    }
    append_uvarint(dst, user_key.len() as u64);
    append_uvarint(dst, value.len() as u64);
    append_uvarint(dst, seq);
    if ttl != 0 {
        append_varint(dst, ttl);
    }
    dst.extend_from_slice(user_key);
    if has_vlog {
        append_u64(dst, vlog_off);
    } else {
        dst.extend_from_slice(value);
    }
}

/// Decode the entry at `raw[off..]` in `layout`, returning it and the offset
/// just past it.
pub(crate) fn decode_entry(
    raw: &[u8],
    layout: EntryLayout,
    off: usize,
) -> Result<(DecEntry, usize)> {
    let corrupt = || OndaError::Corruption("sst: malformed entry".into());
    if off >= raw.len() {
        return Err(corrupt());
    }
    let (fl, kind, mut p) = match layout {
        EntryLayout::Base => {
            let fl = raw[off];
            crate::format::check_entry_flags(fl)?;
            let kind = crate::format::point_kind(
                fl & flags::TOMBSTONE != 0,
                fl & flags::SINGLE_DELETE != 0,
            ) as u8;
            (fl, kind, off + 1)
        }
        EntryLayout::Extended => {
            let (kind, n) = uvarint(&raw[off..]).ok_or_else(corrupt)?;
            // A point stream, so kind 5 is refused here even though the binary
            // implements it: range fragments live in the aux section.
            crate::format::check_point_kind(kind)?;
            let mut p = off + n;
            let (mods, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
            crate::format::check_modifiers(mods)?;
            p += n;
            // The TTL/vlog bits are also folded back into a legacy flags byte:
            // every consumer of `DecEntry` reads that one representation for
            // them. The kind itself is kept as the kind — 1.1's merge operand
            // has no flags-byte spelling.
            let (tombstone, single_delete) = kind_to_point_flags(kind);
            let mut fl = mods as u8;
            if tombstone {
                fl |= flags::TOMBSTONE;
            }
            if single_delete {
                fl |= flags::SINGLE_DELETE;
            }
            crate::format::check_entry_flags(fl)?;
            // `check_kind` bounded it by MAX_ASSIGNABLE_KIND (63) above.
            (fl, kind as u8, p)
        }
    };
    let (klen, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    p += n;
    let (vlen, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    p += n;
    let (seq, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    p += n;
    let mut ttl = 0i64;
    if fl & flags::HAS_TTL != 0 {
        let (t, n) = varint(&raw[p..]).ok_or_else(corrupt)?;
        p += n;
        ttl = t;
    }
    let klen = klen as usize;
    if klen.checked_add(p).ok_or_else(corrupt)? > raw.len() {
        return Err(corrupt());
    }
    let key_start = p;
    p += klen;
    let has_vlog = fl & flags::HAS_VLOG != 0;
    let (val_start, val_len, vlog_off, next) = if has_vlog {
        if p + 8 > raw.len() {
            return Err(corrupt());
        }
        let off = crate::encoding::read_u64(&raw[p..]);
        (p, vlen as usize, off, p + 8)
    } else {
        let vl = vlen as usize;
        if vl.checked_add(p).ok_or_else(corrupt)? > raw.len() {
            return Err(corrupt());
        }
        (p, vl, 0u64, p + vl)
    };
    Ok((
        DecEntry {
            key_start,
            key_len: klen,
            key_shared: 0,
            val_start,
            val_len,
            seq,
            ttl,
            flags: fl,
            kind,
            vlog_off,
        },
        next,
    ))
}

/// The `(tombstone, single_delete)` pair a record kind implies. A merge operand
/// is neither: it hides nothing.
#[inline]
pub(crate) fn kind_to_point_flags(kind: u64) -> (bool, bool) {
    (
        kind == crate::format::KIND_DELETE || kind == crate::format::KIND_SINGLE_DELETE,
        kind == crate::format::KIND_SINGLE_DELETE,
    )
}

/// Length of the longest common prefix of `a` and `b`.
#[inline]
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Append one prefix-delta data-block entry to `dst`, returning the number of
/// leading bytes it shares with `prev_key` (`0` at a restart anchor, where the
/// caller passes an empty `prev_key`).
///
/// ```text
/// kind uv | modifiers uv | shared_len uv | suffix_len uv | val_len uv |
/// seq uv | ttl varint(if HAS_TTL) | suffix | (value | vlog_off u64 LE)
/// ```
///
/// This is the extended layout with `key_len` split in two; nothing else moves.
/// The order matters twice: `kind`/`modifiers` stay first so `HAS_TTL` is known
/// before the `ttl` slot is reached, and every varint precedes every
/// variable-length field so a decoder can bounds-check `shared_len`,
/// `suffix_len` and `val_len` *before* any memcpy.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_entry_delta(
    dst: &mut Vec<u8>,
    prev_key: &[u8],
    user_key: &[u8],
    value: &[u8],
    seq: u64,
    ttl: i64,
    kind: u64,
    has_vlog: bool,
    vlog_off: u64,
) -> usize {
    let (tombstone, single_delete) = kind_to_point_flags(kind);
    crate::format::debug_check_entry_flags(tombstone, single_delete, has_vlog);
    let fl = crate::format::normalized_entry_flags(tombstone, single_delete, ttl != 0, has_vlog);
    // Normalization may have cleared HAS_VLOG (a tombstone has no separated
    // value); the layout below must follow the bits that were actually written.
    let has_vlog = fl & flags::HAS_VLOG != 0;
    let mods = u64::from(fl) & crate::format::modifiers::KNOWN;
    let shared = shared_prefix_len(prev_key, user_key);
    append_uvarint(dst, kind);
    append_uvarint(dst, mods);
    append_uvarint(dst, shared as u64);
    append_uvarint(dst, (user_key.len() - shared) as u64);
    append_uvarint(dst, value.len() as u64);
    append_uvarint(dst, seq);
    if ttl != 0 {
        append_varint(dst, ttl);
    }
    dst.extend_from_slice(&user_key[shared..]);
    if has_vlog {
        append_u64(dst, vlog_off);
    } else {
        dst.extend_from_slice(value);
    }
    shared
}

/// Decode a prefix-delta entry's fields at `raw[off..]` **without**
/// reconstructing its key: the returned [`DecEntry`] addresses the stored
/// suffix and carries its `shared_len`.
///
/// Every length is bounds-checked against `raw` before it is trusted, so the
/// caller can reconstruct with an unchecked copy afterwards. `raw` is the
/// entries region only (the restart trailer is already split off), which is
/// what makes "an entry may not run past the last one" a bounds check rather
/// than a separate rule.
pub(crate) fn decode_delta_header(raw: &[u8], off: usize) -> Result<(DecEntry, usize)> {
    let corrupt = || OndaError::Corruption("sst: malformed delta entry".into());
    if off >= raw.len() {
        return Err(corrupt());
    }
    let (kind, n) = uvarint(&raw[off..]).ok_or_else(corrupt)?;
    // `check_point_kind`, not `check_kind`: a delta entry is a **point** entry
    // in a data block, exactly as `decode_entry`'s is. Range deletes live in the
    // aux section and transaction-control records (3.2) never reach an SSTable
    // at all, so either one here is a placement no writer produces — and the
    // looser check would fold a control kind into a flags byte with no
    // tombstone bit set and hand back a plain put.
    crate::format::check_point_kind(kind)?;
    let mut p = off + n;
    let (mods, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    crate::format::check_modifiers(mods)?;
    p += n;
    // Folded back into the legacy flags byte, exactly as `decode_entry` does:
    // every consumer of `DecEntry` reads that one representation for TTL and
    // vlog placement, and the kind itself for everything else.
    let (tombstone, single_delete) = kind_to_point_flags(kind);
    let mut fl = mods as u8;
    if tombstone {
        fl |= flags::TOMBSTONE;
    }
    if single_delete {
        fl |= flags::SINGLE_DELETE;
    }
    crate::format::check_entry_flags(fl)?;
    let (shared, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    p += n;
    let (suffix_len, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    p += n;
    let (vlen, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    p += n;
    let (seq, n) = uvarint(&raw[p..]).ok_or_else(corrupt)?;
    p += n;
    let mut ttl = 0i64;
    if fl & flags::HAS_TTL != 0 {
        let (t, n) = varint(&raw[p..]).ok_or_else(corrupt)?;
        p += n;
        ttl = t;
    }
    let shared = usize::try_from(shared).map_err(|_| corrupt())?;
    let suffix_len = usize::try_from(suffix_len).map_err(|_| corrupt())?;
    if suffix_len.checked_add(p).ok_or_else(corrupt)? > raw.len() {
        return Err(corrupt());
    }
    let key_start = p;
    p += suffix_len;
    let has_vlog = fl & flags::HAS_VLOG != 0;
    let (val_start, val_len, vlog_off, next) = if has_vlog {
        if p + 8 > raw.len() {
            return Err(corrupt());
        }
        let off = crate::encoding::read_u64(&raw[p..]);
        (p, vlen as usize, off, p + 8)
    } else {
        let vl = vlen as usize;
        if vl.checked_add(p).ok_or_else(corrupt)? > raw.len() {
            return Err(corrupt());
        }
        (p, vl, 0u64, p + vl)
    };
    Ok((
        DecEntry {
            key_start,
            key_len: suffix_len,
            key_shared: shared,
            val_start,
            val_len,
            seq,
            ttl,
            flags: fl,
            // `check_kind` bounded it by MAX_ASSIGNABLE_KIND (63) above.
            kind: kind as u8,
            vlog_off,
        },
        next,
    ))
}

/// Decode the restart-anchor entry at `raw[off..]`.
///
/// Anchors are self-contained (`shared_len == 0`), which is what lets the
/// anchor binary search run with no key materialization; a non-zero
/// `shared_len` at an offset the restart array names is decoder validation
/// rule 2 and is `Corruption`. [`DecEntry::user_key`] is valid on the result.
pub(crate) fn decode_delta_anchor(raw: &[u8], off: usize) -> Result<(DecEntry, usize)> {
    let (entry, next) = decode_delta_header(raw, off)?;
    if entry.key_shared != 0 {
        return Err(OndaError::Corruption(
            "sst: restart anchor shares a key prefix".into(),
        ));
    }
    Ok((entry, next))
}

/// Decode the prefix-delta entry at `raw[off..]`, advancing `key` from the
/// previous entry's user key to this one's.
///
/// `key` carries the running previous key: empty at the start of a block, and
/// otherwise whatever the last call left there. A restart anchor's
/// `shared_len == 0` truncates it away by itself, so a forward walk never has
/// to know where the anchors are.
///
/// `bytewise` enables decoder validation rule 7 — reconstructed keys are
/// non-decreasing within a block — which is exact only under byte-wise
/// ordering (AGENTS.md invariant 7). Under a custom comparator the check is
/// skipped rather than approximated: ordering is the comparator's to define,
/// and threading a `ComparatorRef` vtable call into the per-entry decode would
/// cost every scan for a case the layer above already orders.
pub(crate) fn decode_entry_delta(
    raw: &[u8],
    off: usize,
    key: &mut Vec<u8>,
    bytewise: bool,
) -> Result<(DecEntry, usize)> {
    let (entry, next) = decode_delta_header(raw, off)?;
    // Rule 1, checked before the reconstruction memcpy.
    if entry.key_shared > key.len() {
        return Err(OndaError::Corruption(
            "sst: delta shared_len exceeds the previous key".into(),
        ));
    }
    let suffix = &raw[entry.key_start..entry.key_start + entry.key_len];
    // Rule 7. The two keys agree on `key[..shared]` by construction, so
    // comparing the suffix against the rest of the previous key is the whole
    // comparison — and it runs before `key` is disturbed.
    if bytewise && suffix < &key[entry.key_shared..] {
        return Err(OndaError::Corruption(
            "sst: delta block keys are not in order".into(),
        ));
    }
    key.truncate(entry.key_shared);
    key.extend_from_slice(suffix);
    Ok((entry, next))
}

/// Validate a prefix-delta block's restart array (decoder validation rule 3):
/// a non-empty block has at least one anchor, the first is at offset 0, and
/// the offsets strictly increase and stay inside the entries region.
///
/// Cheap enough to run on every block load: `restarts` holds one `u32` per
/// `restart_interval` entries — ten of them in a 4 KiB block at the default.
pub(crate) fn validate_delta_restarts(entries_len: usize, restarts: &[u8]) -> Result<()> {
    let corrupt = |what: &str| OndaError::Corruption(format!("sst: delta block {what}"));
    let count = restarts.len() / 4;
    if entries_len == 0 {
        // No writer emits an empty data block, but a truncated one must not
        // become "a block with anchors pointing at nothing".
        return if count == 0 {
            Ok(())
        } else {
            Err(corrupt("is empty but has restart anchors"))
        };
    }
    if count == 0 {
        return Err(corrupt("has no restart anchors"));
    }
    let mut previous: Option<usize> = None;
    for i in 0..count {
        let off = crate::encoding::read_u32(&restarts[i * 4..]) as usize;
        if i == 0 && off != 0 {
            return Err(corrupt("does not start with an anchor at offset 0"));
        }
        if off >= entries_len {
            return Err(corrupt("has a restart offset past its entries"));
        }
        if previous.is_some_and(|p| off <= p) {
            return Err(corrupt("has non-increasing restart offsets"));
        }
        previous = Some(off);
    }
    Ok(())
}

/// Binary-search a block's restart array, returning how many leading anchors
/// sort **strictly before** the target. The entry to start scanning from is the
/// anchor just below that (index `n - 1`, or offset 0 when `n == 0`), so the
/// target cannot lie in a skipped interval.
///
/// `anchor_is_lt` decodes the anchor at a restart *index* and reports the
/// comparison; taking it as a closure is what lets the legacy reader, the delta
/// reader and the iterator share one search over three different decoders.
pub(crate) fn restart_lower_bound(
    count: usize,
    mut anchor_is_lt: impl FnMut(usize) -> Result<bool>,
) -> Result<usize> {
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if anchor_is_lt(mid)? {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Decode the aux block's tagged section list, returning `(tag, payload)` pairs.
///
/// ```text
/// aux payload := section_count uvarint | section x count
/// section     := tag u8 | len uvarint | payload[len]
/// tag 1 = range-delete fragments (defined by 1.2)
/// tag 2.. reserved
/// ```
///
/// The aux block is `block.rs`-framed like every other block, so its bytes are
/// CRC-covered. 1.0 defines the container and writes no sections; an unknown
/// section tag is [`OndaError::UnsupportedFormat`] — the block is intact and
/// names a feature this binary does not implement.
pub(crate) fn decode_aux_sections(payload: &[u8]) -> Result<Vec<(u8, &[u8])>> {
    let corrupt = || OndaError::Corruption("sst: malformed aux block".into());
    let (count, n) = uvarint(payload).ok_or_else(corrupt)?;
    let mut p = &payload[n..];
    let mut out = Vec::new();
    for _ in 0..count {
        let (&tag, rest) = p.split_first().ok_or_else(corrupt)?;
        if tag == 0 || tag > MAX_KNOWN_AUX_SECTION {
            return Err(OndaError::UnsupportedFormat(format!(
                "sst aux section tag {tag} is not implemented by this binary"
            )));
        }
        let (len, n) = uvarint(rest).ok_or_else(corrupt)?;
        let rest = &rest[n..];
        let len = len as usize;
        if rest.len() < len {
            return Err(corrupt());
        }
        out.push((tag, &rest[..len]));
        p = &rest[len..];
    }
    if !p.is_empty() {
        return Err(corrupt());
    }
    Ok(out)
}

/// [`decode_aux_sections`] for tests outside this crate.
///
/// The aux container is a format contract, and the refusal of an unknown
/// section tag is part of it — but the decoder itself is an internal detail, so
/// only this thin, documented wrapper is exported.
#[doc(hidden)]
pub fn decode_aux_sections_for_test(payload: &[u8]) -> Result<Vec<(u8, &[u8])>> {
    decode_aux_sections(payload)
}

/// Highest aux section tag this binary knows. `1` since 1.2 defined the
/// range-delete fragment section.
const MAX_KNOWN_AUX_SECTION: u8 = 1;

/// Aux section tag 1: range-delete fragments (1.2). See
/// [`crate::range_tombstone::encode_fragments`] for the payload.
pub(crate) const AUX_SECTION_RANGE: u8 = 1;

/// Encode an aux block from its `(tag, payload)` sections, in tag order.
///
/// The inverse of [`decode_aux_sections`]; the enclosing `block.rs` frame
/// supplies the CRC (invariant 4).
pub(crate) fn encode_aux_sections(sections: &[(u8, Vec<u8>)]) -> Vec<u8> {
    let mut b = Vec::new();
    append_uvarint(&mut b, sections.len() as u64);
    for (tag, payload) in sections {
        b.push(*tag);
        append_uvarint(&mut b, payload.len() as u64);
        b.extend_from_slice(payload);
    }
    b
}

/// Order `(user_key, seq)` pairs: user key ascending (via `cmp`), seq descending.
pub(crate) fn cmp_internal(
    cmp: &crate::comparator::ComparatorRef,
    a_key: &[u8],
    a_seq: u64,
    b_key: &[u8],
    b_seq: u64,
) -> std::cmp::Ordering {
    cmp.compare(a_key, b_key).then_with(|| b_seq.cmp(&a_seq))
}

/// Encode a compression algorithm for the writer's data blocks.
pub(crate) fn data_block_alg(opts_alg: Compression) -> Compression {
    opts_alg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::normalized_entry_flags;

    /// `decode_entry` must be total over arbitrary bytes at arbitrary offsets:
    /// a `Result`, never a panic. Seeded from the frozen klog corpus.
    #[test]
    fn fuzz_decode_entry_never_panics() {
        let mut seeds: Vec<Vec<u8>> = Vec::new();
        for name in [
            "klog_legacy_flat_restarts_bloom.klog",
            "klog_legacy_btree_norestarts_nobloom.klog",
        ] {
            seeds.push(std::fs::read(crate::util::legacy_fixture(name)).unwrap());
        }
        // A well-formed entry stream, so mutations start from valid framing.
        let mut buf = Vec::new();
        let lay = EntryLayout::Base;
        encode_entry(
            &mut buf,
            lay,
            b"k1",
            b"v",
            1,
            0,
            crate::format::KIND_PUT,
            false,
            0,
        );
        encode_entry(
            &mut buf,
            lay,
            b"k2",
            b"",
            2,
            0,
            crate::format::KIND_SINGLE_DELETE,
            false,
            0,
        );
        encode_entry(
            &mut buf,
            lay,
            b"k3",
            b"vvvv",
            3,
            1_700_000_000,
            crate::format::KIND_PUT,
            true,
            64,
        );
        seeds.push(buf);

        let mut rng = crate::util::FuzzRng::new(0xD1B5_4A32_D192_ED03);
        for seed in &seeds {
            for _ in 0..2000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                let at = rng.below(case.len().max(1));
                for layout in [EntryLayout::Base, EntryLayout::Extended] {
                    let _ = decode_entry(&case, layout, at);
                    let _ = decode_entry(&case, layout, 0);
                }
            }
        }
    }

    /// Hand-build one data-block entry with an arbitrary flags byte.
    fn raw_entry(fl: u8, key: &[u8], tail: &[u8]) -> Vec<u8> {
        let mut b = vec![fl];
        append_uvarint(&mut b, key.len() as u64);
        append_uvarint(&mut b, tail.len() as u64);
        append_uvarint(&mut b, 5);
        b.extend_from_slice(key);
        b.extend_from_slice(tail);
        b
    }

    /// `0x08` once named a `DELTA_SEQ` encoding that no writer ever produced;
    /// it is reserved-unknown and must fail closed.
    #[test]
    fn decode_entry_rejects_unknown_flag_bit() {
        let raw = raw_entry(0x08, b"k", b"v");
        let err = decode_entry(&raw, EntryLayout::Base, 0)
            .expect_err("unknown flag bit must be rejected");
        assert_eq!(err.kind(), "corruption");
    }

    /// `Writer::add` never separates a tombstone value, so this combination
    /// cannot come from any writer.
    #[test]
    fn decode_entry_rejects_tombstone_with_vlog() {
        let mut raw = raw_entry(flags::TOMBSTONE | flags::HAS_VLOG, b"k", b"");
        append_u64(&mut raw, 0x1234);
        let err = decode_entry(&raw, EntryLayout::Base, 0)
            .expect_err("TOMBSTONE with HAS_VLOG must be rejected");
        assert_eq!(err.kind(), "corruption");
    }

    /// `encode_entry` builds its flags byte through
    /// `format::normalized_entry_flags`; the byte assertion goes there because
    /// the encode site debug-asserts the invariant (twin below).
    #[test]
    fn sst_encode_never_sets_vlog_on_tombstone() {
        assert_eq!(
            normalized_entry_flags(true, false, false, true),
            flags::TOMBSTONE
        );
        let mut buf = Vec::new();
        encode_entry(
            &mut buf,
            EntryLayout::Base,
            b"k",
            b"v",
            1,
            0,
            crate::format::KIND_PUT,
            true,
            0x1234,
        );
        assert_eq!(buf[0], flags::HAS_VLOG);
        // A tombstone written with a vlog pointer would be rejected by the
        // strict decoder; the normalized entry stores its (empty) value inline.
        let mut buf = Vec::new();
        encode_entry(
            &mut buf,
            EntryLayout::Base,
            b"k",
            b"",
            1,
            0,
            crate::format::KIND_SINGLE_DELETE,
            false,
            0,
        );
        assert_eq!(buf[0], flags::TOMBSTONE | flags::SINGLE_DELETE);
        let (dec, next) = decode_entry(&buf, EntryLayout::Base, 0).unwrap();
        assert!(dec.tombstone() && dec.single_delete() && !dec.has_vlog());
        assert_eq!(next, buf.len());
    }

    /// Encode one delta entry against `prev` and decode it back.
    fn delta_round_trip(prev: &[u8], key: &[u8], value: &[u8], ttl: i64) -> (Vec<u8>, usize) {
        let mut buf = Vec::new();
        let shared = encode_entry_delta(
            &mut buf,
            prev,
            key,
            value,
            7,
            ttl,
            crate::format::KIND_PUT,
            false,
            0,
        );
        let mut out = prev.to_vec();
        let (dec, next) = decode_entry_delta(&buf, 0, &mut out, true).unwrap();
        assert_eq!(next, buf.len(), "decode must consume the entry exactly");
        assert_eq!(out, key, "reconstructed key");
        assert_eq!(dec.key_shared, shared);
        assert_eq!(dec.seq, 7);
        assert_eq!(dec.ttl, ttl);
        assert_eq!(dec.inline_value(&buf), value);
        (buf, shared)
    }

    #[test]
    fn delta_entry_round_trips_with_shared_prefix() {
        let (buf, shared) = delta_round_trip(b"tenant/a/cluster/1", b"tenant/a/cluster/2", b"v", 0);
        assert_eq!(shared, 17, "only the final byte differs");
        // The suffix is the only key byte stored.
        assert!(
            buf.len() < 17,
            "a 1-byte suffix must not carry the whole key: {buf:?}"
        );
        // A TTL entry takes the same path with one extra field.
        delta_round_trip(b"tenant/a", b"tenant/ab", b"vv", 1_700_000_000);
    }

    #[test]
    fn delta_entry_round_trips_with_zero_shared() {
        let (_, shared) = delta_round_trip(b"", b"anchor-key", b"value", 0);
        assert_eq!(shared, 0, "an anchor stores its whole key");
        let (_, shared) = delta_round_trip(b"aaa", b"zzz", b"value", 0);
        assert_eq!(shared, 0, "nothing shared with an unrelated predecessor");
        // Keys shorter than the 8-byte `key_prefix8` window, and an empty key.
        delta_round_trip(b"ab", b"abc", b"v", 0);
        delta_round_trip(b"", b"", b"v", 0);
    }

    #[test]
    fn delta_entry_rejects_shared_longer_than_prev() {
        let mut buf = Vec::new();
        encode_entry_delta(
            &mut buf,
            b"abcdef",
            b"abcdefgh",
            b"v",
            1,
            0,
            crate::format::KIND_PUT,
            false,
            0,
        );
        // The predecessor is shorter than the recorded shared_len (6).
        let mut out = b"abc".to_vec();
        let err = decode_entry_delta(&buf, 0, &mut out, true)
            .expect_err("shared_len past the previous key must be rejected");
        assert_eq!(err.kind(), "corruption");
        assert!(err.to_string().contains("shared_len"), "{err}");
        // The header alone decodes: the bound is a property of the pair, and
        // checking it needs the predecessor.
        assert_eq!(decode_delta_header(&buf, 0).unwrap().0.key_shared, 6);
    }

    #[test]
    fn delta_entry_rejects_truncated_suffix() {
        let mut buf = Vec::new();
        encode_entry_delta(
            &mut buf,
            b"ab",
            b"abcdefgh",
            b"v",
            1,
            0,
            crate::format::KIND_PUT,
            false,
            0,
        );
        // Drop the value and part of the suffix.
        buf.truncate(buf.len() - 4);
        let mut out = b"ab".to_vec();
        let err = decode_entry_delta(&buf, 0, &mut out, true).expect_err("truncated suffix");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn delta_entry_rejects_truncated_value() {
        let mut buf = Vec::new();
        encode_entry_delta(
            &mut buf,
            b"ab",
            b"abc",
            b"a-long-value",
            1,
            0,
            crate::format::KIND_PUT,
            false,
            0,
        );
        buf.truncate(buf.len() - 3);
        let mut out = b"ab".to_vec();
        let err = decode_entry_delta(&buf, 0, &mut out, true).expect_err("truncated value");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn delta_vlog_entry_carries_eight_offset_bytes() {
        let mut buf = Vec::new();
        // `val_len` stays the LOGICAL value length; the entry stores the
        // 8-byte offset instead of the bytes.
        encode_entry_delta(
            &mut buf,
            b"key0",
            b"key1",
            &vec![b'x'; 4096],
            9,
            0,
            crate::format::KIND_PUT,
            true,
            0x1234_5678_9abc_def0,
        );
        let mut out = b"key0".to_vec();
        let (dec, next) = decode_entry_delta(&buf, 0, &mut out, true).unwrap();
        assert_eq!(out, b"key1");
        assert!(dec.has_vlog());
        assert_eq!(dec.vlog_off, 0x1234_5678_9abc_def0);
        assert_eq!(dec.val_len, 4096, "logical length, not the stored 8 bytes");
        assert_eq!(next, buf.len());
        assert_eq!(next - dec.val_start, 8);
    }

    #[test]
    fn delta_entry_rejects_out_of_order_keys_under_bytewise_order() {
        let mut buf = Vec::new();
        encode_entry_delta(
            &mut buf,
            b"",
            b"aaa",
            b"v",
            1,
            0,
            crate::format::KIND_PUT,
            false,
            0,
        );
        let mut out = b"zzz".to_vec();
        let err = decode_entry_delta(&buf, 0, &mut out, true).expect_err("descending keys");
        assert_eq!(err.kind(), "corruption");
        // A custom comparator defines its own order, so the check is skipped.
        let mut out = b"zzz".to_vec();
        assert!(decode_entry_delta(&buf, 0, &mut out, false).is_ok());
        // Equal keys are legal: the same user key at a lower sequence.
        let mut buf = Vec::new();
        encode_entry_delta(
            &mut buf,
            b"aaa",
            b"aaa",
            b"v",
            1,
            0,
            crate::format::KIND_PUT,
            false,
            0,
        );
        let mut out = b"aaa".to_vec();
        assert!(decode_entry_delta(&buf, 0, &mut out, true).is_ok());
    }

    #[test]
    fn delta_restart_validation_rejects_a_malformed_anchor_array() {
        let ok = [0u8, 0, 0, 0, 16, 0, 0, 0];
        assert!(validate_delta_restarts(64, &ok).is_ok());
        // First anchor not at offset 0.
        assert!(validate_delta_restarts(64, &[4u8, 0, 0, 0]).is_err());
        // Non-increasing.
        assert!(validate_delta_restarts(64, &[0u8, 0, 0, 0, 0, 0, 0, 0]).is_err());
        // Past the entries region.
        assert!(validate_delta_restarts(8, &[0u8, 0, 0, 0, 16, 0, 0, 0]).is_err());
        // Non-empty block with no anchors.
        assert!(validate_delta_restarts(64, &[]).is_err());
        assert!(validate_delta_restarts(0, &[]).is_ok());
    }

    /// The delta decoder must be total over arbitrary bytes: a `Result`, never
    /// a panic, at any offset, with any predecessor. Runs before the writer
    /// gains a delta path anywhere in the engine.
    #[test]
    fn delta_decoder_never_panics_on_arbitrary_bytes() {
        // A well-formed delta run, so mutations start from valid framing.
        let mut buf = Vec::new();
        let keys: [&[u8]; 6] = [b"", b"a", b"tenant/a/1", b"tenant/a/2", b"tenant/b", b"z"];
        let mut prev: &[u8] = b"";
        for (i, k) in keys.iter().enumerate() {
            encode_entry_delta(
                &mut buf,
                prev,
                k,
                b"value",
                i as u64 + 1,
                if i % 2 == 0 { 0 } else { 1_700_000_000 },
                if i == 3 {
                    crate::format::KIND_SINGLE_DELETE
                } else {
                    crate::format::KIND_PUT
                },
                false,
                0,
            );
            prev = k;
        }
        let mut seeds = vec![buf];
        for name in ["klog_legacy_flat_restarts_bloom.klog", "klog_extended.klog"] {
            seeds.push(std::fs::read(crate::util::legacy_fixture(name)).unwrap());
        }

        let mut rng = crate::util::FuzzRng::new(0x51E7_9C42_0AB3_1DD7);
        for seed in &seeds {
            for _ in 0..2000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                let at = rng.below(case.len().max(1));
                let _ = decode_delta_header(&case, at);
                let _ = decode_delta_header(&case, 0);
                let _ = decode_delta_anchor(&case, at);
                for prev in [&b""[..], &b"tenant/a/1"[..]] {
                    let mut key = prev.to_vec();
                    let _ = decode_entry_delta(&case, at, &mut key, true);
                    let mut key = prev.to_vec();
                    let _ = decode_entry_delta(&case, 0, &mut key, false);
                }
                // Walk the whole case forward, the way a block scan would.
                let mut key = Vec::new();
                let mut off = 0usize;
                for _ in 0..64 {
                    match decode_entry_delta(&case, off, &mut key, true) {
                        Ok((_, next)) if next > off => off = next,
                        _ => break,
                    }
                }
                let _ = validate_delta_restarts(case.len(), &case[..case.len() & !3]);
            }
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "TOMBSTONE")]
    fn sst_encode_debug_asserts_tombstone_has_no_vlog() {
        encode_entry(
            &mut Vec::new(),
            EntryLayout::Base,
            b"k",
            b"v",
            1,
            0,
            crate::format::KIND_DELETE,
            true,
            7,
        );
    }
}

#[cfg(test)]
mod size_probe {
    /// `DecEntry` is returned **by value** once per decoded entry on every scan,
    /// so its size is a scan-path cost, not a detail. 1.1 added the record kind
    /// as a byte beside `flags` precisely so this number did not move.
    #[test]
    fn dec_entry_stays_the_size_it_was() {
        assert_eq!(std::mem::size_of::<super::DecEntry>(), 72);
    }
}
