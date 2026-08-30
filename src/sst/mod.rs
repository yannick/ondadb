//! ondaDB SSTables: immutable sorted runs produced by memtable flushes and
//! compaction.
//!
//! An SSTable is a klog file (keys + small values + metadata) and an optional
//! vlog file (large values — WiscKey key/value separation).  klog layout:
//!
//! ```text
//! [data block 0] .. [data block N-1] [bloom block?] [index block] [footer(64)]
//! ```
//!
//! Blocks are framed by [`crate::block`].  Data-block entries are in internal
//! order (user key ascending, sequence descending):
//!
//! ```text
//! flags(1) | key_len uvarint | val_len uvarint | seq uvarint |
//! ttl varint(if HAS_TTL) | key | (value | vlog_off u64 LE if HAS_VLOG)
//! ```

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
use crate::encoding::{append_u64, append_uvarint, append_varint, uvarint, varint};
use crate::error::{OndaError, Result};
use crate::format::flags;

/// Fixed footer size in bytes.
pub(crate) const FOOTER_SIZE: usize = 64;
/// Footer magic: "WAVESST1"-derived value reused for ondaDB klogs.
pub(crate) const FOOTER_MAGIC: u64 = 0x5741_5645_5353_5431;
/// Footer flag: a bloom block is present.
pub(crate) const FOOTER_HAS_BLOOM: u8 = 0x01;
/// Footer flag: the index block is a B+tree root (hybrid klog) rather than a
/// flat single-level index.
pub(crate) const FOOTER_BTREE: u8 = 0x02;
/// Footer flag: data blocks carry a restart-offset trailer
/// (`entries... | restart_off u32 LE x R | R u32 LE`) enabling in-block binary
/// search. Absent on legacy files, whose blocks are entries only.
pub(crate) const FOOTER_RESTARTS: u8 = 0x04;
/// Footer flag: vlog frames use the v2 layout
/// `[crc32c u32 LE][alg u8][comp_len u32 LE][payload]` (payload may be
/// compressed; `alg = None` stores it raw). Absent on legacy files, whose
/// frames are `[crc32c u32 LE][raw value]`.
pub(crate) const FOOTER_VLOG_V2: u8 = 0x08;
/// Mask of every footer flag bit this binary implements (`0x0F`).
///
/// A file setting a bit outside this mask was written by a newer binary and
/// names a feature we do not implement — [`OndaError::UnsupportedFormat`], not
/// `Corruption`.
pub(crate) const KNOWN_FOOTER_FLAGS: u8 =
    FOOTER_HAS_BLOOM | FOOTER_BTREE | FOOTER_RESTARTS | FOOTER_VLOG_V2;
/// Entries per restart interval written by default.
pub(crate) const RESTART_INTERVAL: usize = 8;
/// Default target data-block size used by low-level writers when their option
/// is zero. Engine write paths pass `ColumnFamilyConfig::data_block_size`
/// explicitly; its default is the same 4 KiB value. Existing files are
/// unaffected because block boundaries are self-describing.
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4 << 10;
/// Length of the per-value CRC32-C prefix in the vlog frame.
pub(crate) const VLOG_CRC_LEN: usize = 4;
/// Length of the v2 vlog frame header: crc32c(4) + alg(1) + comp_len(4).
pub(crate) const VLOG_V2_HDR_LEN: usize = 9;

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
    pub key_start: usize,
    pub key_len: usize,
    pub val_start: usize,
    pub val_len: usize, // logical value length (also for vlog values)
    pub seq: u64,
    pub ttl: i64,
    pub flags: u8,
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
    pub fn user_key<'a>(&self, raw: &'a [u8]) -> &'a [u8] {
        &raw[self.key_start..self.key_start + self.key_len]
    }
    pub fn inline_value<'a>(&self, raw: &'a [u8]) -> &'a [u8] {
        &raw[self.val_start..self.val_start + self.val_len]
    }
}

/// Append one data-block entry to `dst`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_entry(
    dst: &mut Vec<u8>,
    user_key: &[u8],
    value: &[u8],
    seq: u64,
    ttl: i64,
    tombstone: bool,
    single_delete: bool,
    has_vlog: bool,
    vlog_off: u64,
) {
    crate::format::debug_check_entry_flags(tombstone, single_delete, has_vlog);
    let fl = crate::format::normalized_entry_flags(tombstone, single_delete, ttl != 0, has_vlog);
    // Normalization may have cleared HAS_VLOG (a tombstone has no separated
    // value); the layout below must follow the byte that was actually written.
    let has_vlog = fl & flags::HAS_VLOG != 0;
    dst.push(fl);
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

/// Decode the entry at `raw[off..]`, returning it and the offset just past it.
pub(crate) fn decode_entry(raw: &[u8], off: usize) -> Result<(DecEntry, usize)> {
    let corrupt = || OndaError::Corruption("sst: malformed entry".into());
    if off >= raw.len() {
        return Err(corrupt());
    }
    let fl = raw[off];
    crate::format::check_entry_flags(fl)?;
    let mut p = off + 1;
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
            val_start,
            val_len,
            seq,
            ttl,
            flags: fl,
            vlog_off,
        },
        next,
    ))
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
            seeds.push(std::fs::read(crate::util::phase1_fixture(name)).unwrap());
        }
        // A well-formed entry stream, so mutations start from valid framing.
        let mut buf = Vec::new();
        encode_entry(&mut buf, b"k1", b"v", 1, 0, false, false, false, 0);
        encode_entry(&mut buf, b"k2", b"", 2, 0, true, true, false, 0);
        encode_entry(&mut buf, b"k3", b"vvvv", 3, 1_700_000_000, false, false, true, 64);
        seeds.push(buf);

        let mut rng = crate::util::FuzzRng::new(0xD1B5_4A32_D192_ED03);
        for seed in &seeds {
            for _ in 0..2000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                let at = rng.below(case.len().max(1));
                let _ = decode_entry(&case, at);
                let _ = decode_entry(&case, 0);
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
        let err = decode_entry(&raw, 0).expect_err("unknown flag bit must be rejected");
        assert_eq!(err.kind(), "corruption");
    }

    /// `Writer::add` never separates a tombstone value, so this combination
    /// cannot come from any writer.
    #[test]
    fn decode_entry_rejects_tombstone_with_vlog() {
        let mut raw = raw_entry(flags::TOMBSTONE | flags::HAS_VLOG, b"k", b"");
        append_u64(&mut raw, 0x1234);
        let err = decode_entry(&raw, 0).expect_err("TOMBSTONE with HAS_VLOG must be rejected");
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
        encode_entry(&mut buf, b"k", b"v", 1, 0, false, false, true, 0x1234);
        assert_eq!(buf[0], flags::HAS_VLOG);
        // A tombstone written with a vlog pointer would be rejected by the
        // strict decoder; the normalized entry stores its (empty) value inline.
        let mut buf = Vec::new();
        encode_entry(&mut buf, b"k", b"", 1, 0, true, true, false, 0);
        assert_eq!(buf[0], flags::TOMBSTONE | flags::SINGLE_DELETE);
        let (dec, next) = decode_entry(&buf, 0).unwrap();
        assert!(dec.tombstone() && dec.single_delete() && !dec.has_vlog());
        assert_eq!(next, buf.len());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "TOMBSTONE")]
    fn sst_encode_debug_asserts_tombstone_has_no_vlog() {
        encode_entry(&mut Vec::new(), b"k", b"v", 1, 0, true, false, true, 7);
    }
}
