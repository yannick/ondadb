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
/// Entries per restart interval written by default.
pub(crate) const RESTART_INTERVAL: usize = 8;
/// Default target data-block size.
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4 << 10;
/// Length of the per-value CRC32-C prefix in the vlog frame.
pub(crate) const VLOG_CRC_LEN: usize = 4;

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
    let mut fl = 0u8;
    if tombstone {
        fl |= flags::TOMBSTONE;
    }
    if single_delete {
        fl |= flags::SINGLE_DELETE;
    }
    if ttl != 0 {
        fl |= flags::HAS_TTL;
    }
    if has_vlog {
        fl |= flags::HAS_VLOG;
    }
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
    if p + klen > raw.len() {
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
        if p + vl > raw.len() {
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
