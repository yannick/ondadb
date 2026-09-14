//! SSTable reader: point lookups and ordered iteration over a finished SSTable.

use std::sync::atomic::{AtomicU64, Ordering as AtOrd};
use std::sync::{Arc, OnceLock};

use super::{
    cmp_internal, decode_aux_sections, decode_delta_anchor, decode_entry, decode_entry_delta,
    restart_lower_bound, validate_delta_restarts, vlog_path_for, Block, BlockHandle, EntryLayout,
    IndexEntry, SstIterator, AUX_HANDLE_LEN, FOOTER_BTREE, FOOTER_EXTENDED_BLOCK, FOOTER_HAS_BLOOM,
    FOOTER_MAGIC, FOOTER_PREFIX_DELTA, FOOTER_RESTARTS, FOOTER_SIZE, FOOTER_VLOG_V2,
    KNOWN_FOOTER_FLAGS, VLOG_CRC_LEN, VLOG_V2_HDR_LEN,
};
use crate::bloom::Bloom;
use crate::cache::{BlockCache, BlockDomain};
use crate::comparator::ComparatorRef;
use crate::config::Compression;
use crate::encoding::{checksum, read_u32, read_u64, uvarint};
use crate::error::{OndaError, Result};
use crate::storage::{ReadHandle, Storage};

/// Reads a finished SSTable.  The footer, index and bloom filter are loaded on
/// open; data blocks are read on demand through the block cache (or, under
/// `mmap-reads`, served zero-copy from an mmap of the klog file).
pub struct Reader {
    klog_path: String,
    vlog_path: String,
    storage: Arc<dyn Storage>,
    bc: Arc<BlockCache>,
    file_id: u64,
    cmp: ComparatorRef,

    pub(crate) index: Vec<IndexEntry>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    num_entries: u64,
    max_seq: u64,
    bloom: Option<Bloom>,
    /// Data blocks carry the restart-offset trailer ([`FOOTER_RESTARTS`]).
    has_restarts: bool,
    /// Vlog frames use the v2 (possibly compressed) layout
    /// ([`FOOTER_VLOG_V2`]).
    vlog_v2: bool,
    /// Data-block entry layout, resolved once from the footer flags. Table-level
    /// by construction ([`FOOTER_EXTENDED_BLOCK`]), so every block of this file
    /// decodes the same way.
    entry_layout: EntryLayout,
    /// Data-block entries are prefix-delta encoded ([`FOOTER_PREFIX_DELTA`]).
    /// Table-level, like [`Self::entry_layout`], and validated at open against
    /// the two flags it requires.
    prefix_delta: bool,
    /// `cmp.is_bytewise()`, resolved once: the delta decoder's in-block order
    /// check is exact only under byte-wise ordering (AGENTS.md invariant 7).
    bytewise: bool,
    /// Aux-block handle of an extended table (`(0, 0)` when absent), `None` for
    /// a legacy table that has no such prefix at all.
    aux_handle: Option<BlockHandle>,
    /// Range-tombstone fragments from aux section 1 (1.2), decoded once at
    /// open: sorted by `start`, disjoint, each with its covering sequences
    /// newest-first. Empty for every legacy and point-only table, which is what
    /// makes the read path's gate a single `is_empty` test.
    ///
    /// Held as owned data rather than a borrowed block: the read path hands
    /// storage to cursors that outlive any pinned block (invariant 8), and one
    /// table's fragment list is orders of magnitude smaller than its point
    /// stream.
    fragments: Arc<[crate::range_tombstone::Fragment]>,

    /// Background-IO admission, or `None` when unlimited. Charged on the paths
    /// that actually issue device IO — a cache hit and an already-faulted mmap
    /// block cost nothing and are charged nothing.
    limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>,

    /// Vlog frames whose CRC this reader has already verified, as a bounded
    /// direct-mapped set of frame offsets ([`VLOG_SLOT_EMPTY`] = free slot).
    /// Same reasoning as the klog `verified` bitmap below — the file is
    /// immutable, so a frame only needs checking on its first read — but it
    /// cannot use the same representation: a data block has a dense small id
    /// (its index position), whereas a vlog frame is addressed only by byte
    /// offset and frames are variable-length (a value just over
    /// `klog_value_threshold` is tens of bytes; a document is megabytes), so
    /// there is no frame count to size a bitmap from and no byte granule
    /// smaller than every frame. Storing the offsets bounds it instead: a
    /// collision costs a re-verification, never a false "verified", because a
    /// slot holds the offset it verified and only an exact match skips the
    /// checksum.
    ///
    /// Unlike the klog bitmap this covers the buffered `pread` path too: that
    /// path re-reads and re-verifies on every get, while the non-mmap klog path
    /// caches the decompressed block and so never re-verifies anyway.
    ///
    /// Allocated on the first vlog read, so tables without large values — and
    /// tables whose large values are never touched — pay nothing. Per-reader
    /// resident bytes are the dominant memory term at scale (see
    /// [`resident_bytes`](Self::resident_bytes)); the 8 KiB this costs a reader
    /// that does touch its vlog is *not* counted there, which reports what is
    /// loaded eagerly at open.
    vlog_verified: OnceLock<Box<[AtomicU64]>>,

    /// Byte ceiling on a decoded vlog value this reader admits to the block
    /// cache (`ColumnFamilyConfig::max_cached_vlog_value_bytes`); 0 disables
    /// vlog value caching. Fixed at open — a `Reader` is immutable and shared
    /// behind an `Arc`, and the setting is per-family durable state, so there
    /// is nothing to reconfigure without reopening the table.
    vlog_cache_limit: usize,

    #[cfg(feature = "mmap-reads")]
    klog_mmap: Option<Arc<memmap2::Mmap>>,
    #[cfg(feature = "mmap-reads")]
    vlog_mmap: parking_lot::Mutex<Option<Arc<memmap2::Mmap>>>,
    /// One bit per data block: set once the block's CRC has been verified.
    /// SSTable bytes are immutable, so each block only needs checking on its
    /// first read — not on every read by every scanning thread.
    #[cfg(feature = "mmap-reads")]
    verified: Vec<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader")
            .field("klog_path", &self.klog_path)
            .field("num_entries", &self.num_entries)
            .field("blocks", &self.index.len())
            .finish()
    }
}

fn corrupt() -> OndaError {
    OndaError::Corruption("sst: corruption detected".into())
}

/// Slots in a reader's vlog CRC-verified set (see `Reader::vlog_verified`).
/// Power of two so the index is a mask. 1024 slots is 8 KiB per reader that
/// touches its vlog, and covers every frame of a default-target (64 MiB)
/// table whose values are 64 KiB or larger; smaller values collide sooner,
/// which only means re-verifying a cheaper checksum.
const VLOG_VERIFIED_SLOTS: usize = 1024;

/// Sentinel for a slot that has verified nothing. No frame can start at
/// `u64::MAX` — the offset is a position in a file.
const VLOG_SLOT_EMPTY: u64 = u64::MAX;

/// `(value, seq, found, deleted, kind)` for one key in one table. `pub(crate)` so
/// the batched planner in `column_family.rs` can name what the block walk it
/// drives by hand returns.
pub(crate) type PointResult = (Option<Vec<u8>>, u64, bool, bool, u64);

fn append_vlog_payload(
    compression: Compression,
    payload: &[u8],
    expected_len: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    if compression == Compression::None {
        if payload.len() != expected_len {
            return Err(corrupt());
        }
        out.extend_from_slice(payload);
        return Ok(());
    }
    let raw = crate::compress::decompress(compression, payload, expected_len)?;
    if raw.len() != expected_len {
        return Err(corrupt());
    }
    out.extend_from_slice(&raw);
    Ok(())
}

/// A data block borrowed for the duration of one point read: either an owned
/// (cached/decompressed) block or, under `mmap-reads`, a plain slice into
/// the reader's mmap — no refcount traffic per get.
pub(crate) enum BlockRef<'a> {
    Owned(Arc<[u8]>),
    #[allow(dead_code)] // only constructed under mmap-reads
    Mapped(&'a [u8]),
}

impl BlockRef<'_> {
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            BlockRef::Owned(a) => a,
            BlockRef::Mapped(s) => s,
        }
    }
}

impl Reader {
    /// Heap bytes this reader holds **resident for the lifetime of the CF**:
    /// the block index and the bloom filter, both loaded eagerly at open.
    ///
    /// Accounting exists because this is the dominant memory term at scale and
    /// it was previously invisible. A 48 GiB store of 14,051 SSTables at the
    /// 4 KiB default block size carries ~12 million index entries, each with a
    /// heap-allocated key — and every one is loaded at open, before a single
    /// read, whether or not that table is ever touched.
    pub fn resident_bytes(&self) -> usize {
        // A Vec<u8> key costs its header plus its bytes plus allocator
        // rounding; count the header and bytes and treat the rest as noise.
        let index: usize = self
            .index
            .iter()
            .map(|e| std::mem::size_of::<IndexEntry>() + e.user_key.len())
            .sum();
        let bloom = self.bloom.as_ref().map_or(0, |b| b.resident_bytes());
        index + bloom + self.min_key.len() + self.max_key.len()
    }

    /// `(index bytes, bloom bytes, index entries)` — so the split can be
    /// reported rather than inferred.
    pub fn resident_breakdown(&self) -> (usize, usize, usize) {
        let index: usize = self
            .index
            .iter()
            .map(|e| std::mem::size_of::<IndexEntry>() + e.user_key.len())
            .sum();
        let bloom = self.bloom.as_ref().map_or(0, |b| b.resident_bytes());
        (index, bloom, self.index.len())
    }

    /// Open the SSTable at `klog_path` on `storage`. `file_id` must be unique
    /// per file for block-cache keying. When `storage.supports_mmap()` is false
    /// (a slow/remote tier), the reader never mmaps and every read goes through
    /// the buffered `pread` path plus the block cache.
    ///
    /// `vlog_cache_limit` is the family's `max_cached_vlog_value_bytes`: the
    /// largest decoded vlog value this reader may admit to the block cache,
    /// or 0 to never cache vlog values.
    pub fn open(
        klog_path: &str,
        storage: Arc<dyn Storage>,
        bc: Arc<BlockCache>,
        file_id: u64,
        cmp: ComparatorRef,
        vlog_cache_limit: usize,
    ) -> Result<Arc<Reader>> {
        Reader::open_with_limiter(klog_path, storage, bc, file_id, cmp, vlog_cache_limit, None)
    }

    /// Like [`open`](Self::open), but the reader charges the bytes it fetches
    /// against `limiter` under the reading thread's
    /// [`IoClass`](crate::ioctrl::IoClass).
    pub fn open_with_limiter(
        klog_path: &str,
        storage: Arc<dyn Storage>,
        bc: Arc<BlockCache>,
        file_id: u64,
        cmp: ComparatorRef,
        vlog_cache_limit: usize,
        limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>,
    ) -> Result<Arc<Reader>> {
        let mut r = Reader {
            klog_path: klog_path.to_string(),
            vlog_path: vlog_path_for(klog_path),
            storage,
            bc,
            file_id,
            cmp,
            limiter,
            index: Vec::new(),
            min_key: Vec::new(),
            max_key: Vec::new(),
            num_entries: 0,
            max_seq: 0,
            bloom: None,
            has_restarts: false,
            vlog_v2: false,
            entry_layout: EntryLayout::Legacy,
            prefix_delta: false,
            bytewise: false,
            aux_handle: None,
            fragments: Arc::from([]),
            vlog_verified: OnceLock::new(),
            vlog_cache_limit,
            #[cfg(feature = "mmap-reads")]
            klog_mmap: None,
            #[cfg(feature = "mmap-reads")]
            vlog_mmap: parking_lot::Mutex::new(None),
            #[cfg(feature = "mmap-reads")]
            verified: Vec::new(),
        };
        r.bytewise = r.cmp.is_bytewise();
        let f = r.storage.open_read(klog_path)?;
        let size = f.size()?;
        if size < FOOTER_SIZE as u64 {
            return Err(corrupt());
        }
        let mut footer = [0u8; FOOTER_SIZE];
        f.read_exact_at(&mut footer, size - FOOTER_SIZE as u64)?;
        if read_u64(&footer[56..64]) != FOOTER_MAGIC {
            return Err(corrupt());
        }
        let index_off = read_u64(&footer[0..8]);
        let index_len = read_u64(&footer[8..16]);
        let bloom_off = read_u64(&footer[16..24]);
        let bloom_len = read_u64(&footer[24..32]);
        r.num_entries = read_u64(&footer[32..40]);
        r.max_seq = read_u64(&footer[40..48]);
        let flags = footer[48];
        if flags & !KNOWN_FOOTER_FLAGS != 0 {
            return Err(OndaError::UnsupportedFormat(format!(
                "sst footer flags {flags:#04x} outside known mask {KNOWN_FOOTER_FLAGS:#04x}"
            )));
        }
        r.has_restarts = flags & FOOTER_RESTARTS != 0;
        r.vlog_v2 = flags & FOOTER_VLOG_V2 != 0;
        r.prefix_delta = flags & FOOTER_PREFIX_DELTA != 0;
        if r.prefix_delta {
            // Both are `Corruption`, not `UnsupportedFormat`: the bits name
            // formats this binary DOES implement, in a combination no writer
            // can produce. The delta layout is defined only over the extended
            // entry, and without a restart trailer a delta block is decodable
            // only from offset 0 — no seek, no reverse iteration.
            if flags & FOOTER_EXTENDED_BLOCK == 0 {
                return Err(OndaError::Corruption(
                    "sst: FOOTER_PREFIX_DELTA without FOOTER_EXTENDED_BLOCK".into(),
                ));
            }
            if flags & FOOTER_RESTARTS == 0 {
                return Err(OndaError::Corruption(
                    "sst: FOOTER_PREFIX_DELTA without FOOTER_RESTARTS".into(),
                ));
            }
        }
        if flags & FOOTER_EXTENDED_BLOCK != 0 {
            r.entry_layout = EntryLayout::Extended;
            // The 16 bytes ahead of the footer are the aux-block handle.
            if size < (FOOTER_SIZE + AUX_HANDLE_LEN) as u64 {
                return Err(corrupt());
            }
            let mut aux = [0u8; AUX_HANDLE_LEN];
            f.read_exact_at(&mut aux, size - (FOOTER_SIZE + AUX_HANDLE_LEN) as u64)?;
            let handle = BlockHandle {
                offset: read_u64(&aux[0..8]),
                length: read_u64(&aux[8..16]),
            };
            // The handle must address bytes that exist, and specifically bytes
            // ahead of the prefix it was read from. Checking before the read
            // keeps a garbage length from being turned into an allocation.
            let limit = size - (FOOTER_SIZE + AUX_HANDLE_LEN) as u64;
            if handle.offset > limit || handle.length > limit - handle.offset {
                return Err(corrupt());
            }
            r.aux_handle = Some(handle);
            if handle.length > 0 {
                // Decoded at open, not lazily: an aux block naming a section
                // this binary does not implement must fail the open, not
                // surface later as a silently missing range delete.
                let (payload, _) = read_block_at(&*f, handle.offset, handle.length)?;
                for (tag, section) in decode_aux_sections(&payload)? {
                    if tag == crate::sst::AUX_SECTION_RANGE {
                        r.fragments = crate::range_tombstone::decode_fragments(section)?.into();
                    }
                }
                // Fragments are written sorted and disjoint; the read path's
                // binary search and its monotonic cursor both depend on it, so
                // a file that claims otherwise is corrupt.
                if r.fragments.windows(2).any(|w| {
                    r.cmp.compare(&w[0].end, &w[1].start).is_gt()
                        || r.cmp.compare(&w[1].start, &w[1].end).is_ge()
                }) {
                    return Err(OndaError::Corruption(
                        "sst: range fragments are unsorted, overlapping or empty".into(),
                    ));
                }
            }
        }

        if flags & FOOTER_HAS_BLOOM != 0 && bloom_len > 0 {
            let (raw, _) = read_block_at(&*f, bloom_off, bloom_len)?;
            r.bloom = Some(Bloom::decode(&raw)?);
        }
        if flags & FOOTER_BTREE != 0 {
            // Hybrid klog: the index handle points at the B+tree root. Walk the
            // tree (root → ... → leaves) to rebuild the in-memory flat index.
            r.load_btree(
                &*f,
                BlockHandle {
                    offset: index_off,
                    length: index_len,
                },
            )?;
        } else {
            let (idx_raw, _) = read_block_at(&*f, index_off, index_len)?;
            r.decode_index(&idx_raw)?;
        }

        // SAFETY (`mmap-reads`): the klog is an immutable, finished SSTable;
        // ondaDB never writes to it after `finish`, and compaction only *unlinks*
        // it (the pages stay valid while this mapping holds the inode). The mmap
        // is owned by the Reader, so views into it live exactly as long as it.
        // A tier that reports `supports_mmap() == false` opts out entirely:
        // `klog_mmap` stays `None` and every read falls through to the buffered
        // `pread` path below.
        #[cfg(feature = "mmap-reads")]
        if r.storage.supports_mmap() {
            let file = f
                .as_file()
                .expect("a tier reporting supports_mmap() must back reads with a local file");
            let mmap = unsafe { memmap2::Mmap::map(file)? };
            // Hint the kernel to start paging the file in now: SSTables are
            // read-hot right after open (recovery, point gets, scans), and
            // asynchronous prefault at open is much cheaper than faulting
            // 4 KiB at a time inside the read loops.
            let _ = mmap.advise(memmap2::Advice::WillNeed);
            r.klog_mmap = Some(Arc::new(mmap));
            let words = r.index.len().div_ceil(64);
            r.verified = (0..words)
                .map(|_| std::sync::atomic::AtomicU64::new(0))
                .collect();
        }
        Ok(Arc::new(r))
    }

    fn decode_index(&mut self, mut p: &[u8]) -> Result<()> {
        let (mk_len, n) = uvarint(p).ok_or_else(corrupt)?;
        p = &p[n..];
        let mk_len = mk_len as usize;
        if p.len() < mk_len {
            return Err(corrupt());
        }
        self.min_key = p[..mk_len].to_vec();
        p = &p[mk_len..];
        let (count, n) = uvarint(p).ok_or_else(corrupt)?;
        p = &p[n..];
        self.index = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (klen, n) = uvarint(p).ok_or_else(corrupt)?;
            p = &p[n..];
            let klen = klen as usize;
            if p.len() < klen {
                return Err(corrupt());
            }
            let key = p[..klen].to_vec();
            p = &p[klen..];
            let (seq, n) = uvarint(p).ok_or_else(corrupt)?;
            p = &p[n..];
            let (off, n) = uvarint(p).ok_or_else(corrupt)?;
            p = &p[n..];
            let (length, n) = uvarint(p).ok_or_else(corrupt)?;
            p = &p[n..];
            self.index.push(IndexEntry {
                user_key: key,
                seq,
                handle: BlockHandle {
                    offset: off,
                    length,
                },
            });
        }
        self.max_key = self
            .index
            .last()
            .map(|e| e.user_key.clone())
            .unwrap_or_else(|| self.min_key.clone());
        Ok(())
    }

    /// Reconstruct the flat index from a B+tree (hybrid klog) by walking from the
    /// root down to the leaves in key order.
    fn load_btree(&mut self, f: &dyn ReadHandle, root: BlockHandle) -> Result<()> {
        self.walk_btree_node(f, root, true)?;
        self.max_key = self
            .index
            .last()
            .map(|e| e.user_key.clone())
            .unwrap_or_else(|| self.min_key.clone());
        Ok(())
    }

    fn walk_btree_node(&mut self, f: &dyn ReadHandle, h: BlockHandle, is_root: bool) -> Result<()> {
        let (block, _) = read_block_at(f, h.offset, h.length)?;
        let mut p = &block[..];
        if p.is_empty() {
            return Err(corrupt());
        }
        let node_type = p[0];
        p = &p[1..];
        if is_root {
            let (mk_len, n) = uvarint(p).ok_or_else(corrupt)?;
            p = &p[n..];
            let mk_len = mk_len as usize;
            if p.len() < mk_len {
                return Err(corrupt());
            }
            self.min_key = p[..mk_len].to_vec();
            p = &p[mk_len..];
        }
        let (count, n) = uvarint(p).ok_or_else(corrupt)?;
        p = &p[n..];
        if node_type == 1 {
            // Leaf: (sep, seq, data_off, data_len) per entry.
            for _ in 0..count {
                let (klen, n) = uvarint(p).ok_or_else(corrupt)?;
                p = &p[n..];
                let klen = klen as usize;
                if p.len() < klen {
                    return Err(corrupt());
                }
                let key = p[..klen].to_vec();
                p = &p[klen..];
                let (seq, n) = uvarint(p).ok_or_else(corrupt)?;
                p = &p[n..];
                let (off, n) = uvarint(p).ok_or_else(corrupt)?;
                p = &p[n..];
                let (length, n) = uvarint(p).ok_or_else(corrupt)?;
                p = &p[n..];
                self.index.push(IndexEntry {
                    user_key: key,
                    seq,
                    handle: BlockHandle {
                        offset: off,
                        length,
                    },
                });
            }
        } else {
            // Internal: (sep, child_off, child_len) per entry; descend in order.
            let mut children = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let (klen, n) = uvarint(p).ok_or_else(corrupt)?;
                p = &p[n..];
                let klen = klen as usize;
                if p.len() < klen {
                    return Err(corrupt());
                }
                p = &p[klen..]; // separator (unused for the full walk)
                let (off, n) = uvarint(p).ok_or_else(corrupt)?;
                p = &p[n..];
                let (length, n) = uvarint(p).ok_or_else(corrupt)?;
                p = &p[n..];
                children.push(BlockHandle {
                    offset: off,
                    length,
                });
            }
            for child in children {
                self.walk_btree_node(f, child, false)?;
            }
        }
        Ok(())
    }

    pub(crate) fn comparator(&self) -> &ComparatorRef {
        &self.cmp
    }

    /// Data-block entry layout of this table (see [`EntryLayout`]).
    #[inline]
    pub(crate) fn entry_layout(&self) -> EntryLayout {
        self.entry_layout
    }

    /// Whether this table's data-block entries are prefix-delta encoded
    /// ([`FOOTER_PREFIX_DELTA`]).
    #[inline]
    pub(crate) fn prefix_delta(&self) -> bool {
        self.prefix_delta
    }

    /// Whether this table's comparator orders byte-wise, which is what makes
    /// the delta decoder's in-block order check exact.
    #[inline]
    pub(crate) fn bytewise(&self) -> bool {
        self.bytewise
    }

    /// This table's aux-block handle as `(offset, length)`, or `None` for a
    /// legacy table (one without [`FOOTER_EXTENDED_BLOCK`], which has no such
    /// prefix at all). `Some((0, 0))` means an extended table with no aux block.
    /// This table's range-tombstone fragments (1.2); empty for a point-only or
    /// legacy table.
    pub fn range_fragments(&self) -> &[crate::range_tombstone::Fragment] {
        &self.fragments
    }

    pub(crate) fn range_fragment_snapshot(&self) -> Arc<[crate::range_tombstone::Fragment]> {
        self.fragments.clone()
    }

    /// Newest sequence at or below `read_seq` of a fragment covering `key`.
    ///
    /// One binary search, and an immediate `None` for a table with no
    /// fragments — the point-read shape of the zero-cost gate.
    #[inline]
    pub fn covering_seq(&self, key: &[u8], read_seq: u64) -> Option<u64> {
        if self.fragments.is_empty() {
            return None;
        }
        crate::range_tombstone::covering_seq_in(&self.cmp, &self.fragments, key, read_seq)
    }

    pub fn aux_block_handle(&self) -> Option<(u64, u64)> {
        self.aux_handle.map(|h| (h.offset, h.length))
    }

    /// Read (and decompress if needed) data block `i`.
    ///
    /// Under `mmap-reads`, an *uncompressed* block is returned as a
    /// zero-copy view into the mmap; compressed blocks are decompressed once and
    /// cached.  Otherwise the block is read through the block cache.
    pub(crate) fn read_data_block(&self, i: usize) -> Result<Block> {
        let h = self.index[i].handle;

        #[cfg(feature = "mmap-reads")]
        if let Some(mmap) = &self.klog_mmap {
            let start = h.offset as usize;
            let end = start + h.length as usize;
            // Verify each block's CRC exactly once per open reader (the file is
            // immutable): first toucher pays the checksum, everyone after reads
            // the already-validated bytes.
            let (word, bit) = (i / 64, 1u64 << (i % 64));
            let seen = self.verified[word].load(AtOrd::Acquire) & bit != 0;
            let parsed = if seen {
                crate::block::block_payload_preverified(&mmap[start..end])?
            } else {
                // First touch of this block in this reader: the same point at
                // which the CRC is paid is the point at which the pages are
                // actually faulted in, so it is the mmap analogue of a cache
                // miss and the only place worth charging.
                crate::ioctrl::charge(&self.limiter, h.length);
                let p = crate::block::block_payload(&mmap[start..end])?;
                self.verified[word].fetch_or(bit, AtOrd::AcqRel);
                p
            };
            let (alg, payload, raw_len, _total) = parsed;
            if alg == Compression::None {
                // Zero-copy: point straight at the mapped raw bytes. No cache
                // traffic at all, so neither a hit nor a miss is recorded.
                crate::perf::bump(|p| p.block_read_bytes += raw_len as u64);
                let payload_start = start + crate::block::BLOCK_HEADER;
                return Ok(Block::Mapped {
                    mmap: mmap.clone(),
                    start: payload_start,
                    len: raw_len,
                });
            }
            // Compressed: decompress once, cache the owned result.
            if let Some(raw) = self.bc.get(self.file_id, h.offset, BlockDomain::Klog) {
                crate::perf::bump(|p| p.block_cache_hits += 1);
                return Ok(Block::Owned(raw));
            }
            crate::perf::bump(|p| {
                p.block_misses += 1;
                p.block_read_bytes += h.length;
            });
            // No IO charge here: under an mmap these bytes were already faulted
            // in (and charged) at the first-touch branch above, and a later
            // cache miss costs a decompression, not a device read. Charging
            // again would double-count every compressed block's first read.
            let raw = crate::compress::decompress(alg, payload, raw_len)?;
            crate::perf::bump(|p| p.bytes_decompressed += raw.len() as u64);
            let arc: Arc<[u8]> = Arc::from(raw.into_boxed_slice());
            self.bc
                .put(self.file_id, h.offset, BlockDomain::Klog, arc.clone());
            return Ok(Block::Owned(arc));
        }

        if let Some(raw) = self.bc.get(self.file_id, h.offset, BlockDomain::Klog) {
            crate::perf::bump(|p| p.block_cache_hits += 1);
            return Ok(Block::Owned(raw));
        }
        crate::perf::bump(|p| {
            p.block_misses += 1;
            p.block_read_bytes += h.length;
        });
        // Charged before the read is issued, so a job cancelled while waiting
        // never consumes the bandwidth it queued for.
        crate::ioctrl::charge(&self.limiter, h.length);
        let f = self.storage.open_read(&self.klog_path)?;
        let (raw, alg) = read_block_at(&*f, h.offset, h.length)?;
        if alg != Compression::None {
            crate::perf::bump(|p| p.bytes_decompressed += raw.len() as u64);
        }
        let arc: Arc<[u8]> = Arc::from(raw.into_boxed_slice());
        self.bc
            .put(self.file_id, h.offset, BlockDomain::Klog, arc.clone());
        Ok(Block::Owned(arc))
    }

    /// Like [`read_data_block`](Self::read_data_block), but for callers that
    /// consume the block within the reader's lifetime: the mmap fast path
    /// returns a borrowed slice instead of bumping the mmap's `Arc` refcount
    /// on every point read.
    pub(crate) fn read_data_block_local(&self, i: usize) -> Result<BlockRef<'_>> {
        #[cfg(feature = "mmap-reads")]
        if let Some(mmap) = &self.klog_mmap {
            let h = self.index[i].handle;
            let start = h.offset as usize;
            let end = start + h.length as usize;
            let (word, bit) = (i / 64, 1u64 << (i % 64));
            let seen = self.verified[word].load(AtOrd::Acquire) & bit != 0;
            let parsed = if seen {
                crate::block::block_payload_preverified(&mmap[start..end])?
            } else {
                crate::ioctrl::charge(&self.limiter, h.length);
                let p = crate::block::block_payload(&mmap[start..end])?;
                self.verified[word].fetch_or(bit, AtOrd::AcqRel);
                p
            };
            let (alg, _payload, raw_len, _total) = parsed;
            if alg == Compression::None {
                crate::perf::bump(|p| p.block_read_bytes += raw_len as u64);
                let payload_start = start + crate::block::BLOCK_HEADER;
                return Ok(BlockRef::Mapped(
                    &mmap[payload_start..payload_start + raw_len],
                ));
            }
        }
        self.read_data_block(i).map(|b| match b {
            Block::Owned(a) => BlockRef::Owned(a),
            #[cfg(feature = "mmap-reads")]
            Block::Mapped { .. } => unreachable!("uncompressed mmap handled above"),
        })
    }

    /// Split a data block's decompressed bytes into its entries region and its
    /// restart-offset array (empty for legacy blocks without the trailer).
    pub(crate) fn split_block<'a>(&self, raw: &'a [u8]) -> Result<(&'a [u8], &'a [u8])> {
        if !self.has_restarts {
            return Ok((raw, &[]));
        }
        if raw.len() < 4 {
            return Err(corrupt());
        }
        let count = read_u32(&raw[raw.len() - 4..]) as usize;
        let trailer = count
            .checked_mul(4)
            .and_then(|t| t.checked_add(4))
            .filter(|&t| t <= raw.len())
            .ok_or_else(corrupt)?;
        let entries_end = raw.len() - trailer;
        let (entries, restarts) = (&raw[..entries_end], &raw[entries_end..raw.len() - 4]);
        if self.prefix_delta {
            // Decoder validation rule 3. A delta block is unreadable without a
            // sound anchor array — every seek and every reverse step enters
            // through one — so it is checked here, at the single point every
            // reader splits a block, rather than at each of the four entry
            // points. One `u32` per `restart_interval` entries makes this ~10
            // comparisons for a 4 KiB block at the default.
            validate_delta_restarts(entries.len(), restarts)?;
        }
        Ok((entries, restarts))
    }

    /// Number of data blocks in this table. A [`find_block`](Self::find_block)
    /// result at or above this means the key sorts past the last block.
    #[inline]
    pub fn data_block_count(&self) -> usize {
        self.index.len()
    }

    /// Whether this table's data blocks are prefix-delta encoded (2.1).
    ///
    /// Public because the property is *self-describing*: a detached, frozen or
    /// mounted table answers this from its own footer, with no manifest
    /// involved, and an operator inspecting a loose klog should be able to ask.
    pub fn is_prefix_delta(&self) -> bool {
        self.prefix_delta
    }

    /// Index of the first data block whose last key is `>= (user_key, seq)`.
    pub(crate) fn find_block(&self, user_key: &[u8], seq: u64) -> usize {
        crate::perf::bump(|p| p.index_seeks += 1);
        let (mut lo, mut hi) = (0, self.index.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let e = &self.index[mid];
            if cmp_internal(&self.cmp, &e.user_key, e.seq, user_key, seq).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// This table's bloom hash of `user_key`, or `None` when it has no filter.
    /// Compute once and pass to [`bloom_may_contain_hash`](Self::bloom_may_contain_hash).
    #[inline]
    pub(crate) fn bloom_hash(&self, user_key: &[u8]) -> Option<u64> {
        self.bloom.as_ref().map(|b| b.hash_of(user_key))
    }

    /// Whether the bloom filter admits a key by its precomputed
    /// [`bloom_hash`](Self::bloom_hash) (`true` when there is no filter).
    #[inline]
    pub(crate) fn bloom_may_contain_hash(&self, h: Option<u64>) -> bool {
        match (&self.bloom, h) {
            (Some(b), Some(h)) => b.may_contain_hash(h),
            _ => true,
        }
    }

    /// Resolve `user_key` as of `read_seq`, including this reader's bloom
    /// check. `found` indicates a version exists in this SSTable; `deleted`
    /// indicates a tombstone or expired entry. The column-family read path
    /// hashes once across candidate tables and therefore calls
    /// [`get_unfiltered`](Self::get_unfiltered) after pre-filtering instead.
    pub fn get(&self, user_key: &[u8], read_seq: u64, now: i64) -> Result<PointResult> {
        if let Some(b) = &self.bloom {
            if !b.may_contain(user_key) {
                return Ok((None, 0, false, false, crate::format::KIND_PUT));
            }
        }
        self.get_unfiltered(user_key, read_seq, now)
    }

    /// Entry offset to start scanning from for `(user_key, read_seq)`.
    ///
    /// `pub(crate)`: the batch point-read path fetches a block once and then
    /// runs this plus [`scan_point_entry`](Self::scan_point_entry) per key
    /// against it, which is exactly the work
    /// [`get_unfiltered`](Self::get_unfiltered) does for a single key.
    pub(crate) fn restart_scan_offset(
        &self,
        raw: &[u8],
        restarts: &[u8],
        user_key: &[u8],
        read_seq: u64,
    ) -> Result<usize> {
        if restarts.len() < 8 {
            return Ok(0);
        }
        // Find the first restart entry >= target, then scan from its
        // predecessor so the target cannot lie in a skipped interval. Restart
        // anchors are self-contained in both layouts (a delta anchor has
        // `shared_len == 0`), so the search needs no key materialization at all.
        let restart_off = |i: usize| read_u32(&restarts[i * 4..]) as usize;
        let lo = restart_lower_bound(restarts.len() / 4, |i| {
            let off = restart_off(i);
            let (entry, _) = if self.prefix_delta {
                decode_delta_anchor(raw, off)?
            } else {
                decode_entry(raw, self.entry_layout, off)?
            };
            Ok(cmp_internal(
                &self.cmp,
                entry.user_key(raw),
                entry.seq,
                user_key,
                read_seq,
            )
            .is_lt())
        })?;
        Ok(if lo > 0 { restart_off(lo - 1) } else { 0 })
    }

    /// Walk a block's entries from `offset` for the newest version of
    /// `user_key` visible at `read_seq`. See
    /// [`restart_scan_offset`](Self::restart_scan_offset) for why this is
    /// `pub(crate)`.
    pub(crate) fn scan_point_entry(
        &self,
        raw: &[u8],
        mut offset: usize,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
    ) -> Result<PointResult> {
        // Delta blocks need a running previous key; legacy blocks borrow each
        // key straight out of the block and allocate nothing.
        let mut scratch = Vec::new();
        while offset < raw.len() {
            let (entry, next) = if self.prefix_delta {
                decode_entry_delta(raw, offset, &mut scratch, self.bytewise)?
            } else {
                decode_entry(raw, self.entry_layout, offset)?
            };
            let entry_key: &[u8] = if self.prefix_delta {
                &scratch
            } else {
                entry.user_key(raw)
            };
            if cmp_internal(&self.cmp, entry_key, entry.seq, user_key, read_seq).is_lt() {
                offset = next;
                continue;
            }
            if self.cmp.compare(entry_key, user_key) != std::cmp::Ordering::Equal {
                break;
            }
            if entry.tombstone() || (entry.ttl != 0 && entry.ttl <= now) {
                return Ok((None, entry.seq, true, true, u64::from(entry.kind)));
            }
            let value = if entry.has_vlog() {
                self.read_vlog(entry.vlog_off, entry.val_len as u64)?
            } else {
                entry.inline_value(raw).to_vec()
            };
            return Ok((Some(value), entry.seq, true, false, u64::from(entry.kind)));
        }
        Ok((None, 0, false, false, crate::format::KIND_PUT))
    }

    /// [`get`](Self::get) without the bloom check, for callers that have
    /// already consulted the filter (see `ColumnFamily::get`).
    pub(crate) fn get_unfiltered(
        &self,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
    ) -> Result<PointResult> {
        let bi = self.find_block(user_key, read_seq);
        if bi >= self.index.len() {
            return Ok((None, 0, false, false, crate::format::KIND_PUT));
        }
        let block = self.read_data_block_local(bi)?;
        let (raw, restarts) = self.split_block(block.bytes())?;
        let offset = self.restart_scan_offset(raw, restarts, user_key, read_seq)?;
        self.scan_point_entry(raw, offset, user_key, read_seq, now)
    }

    /// The slot that can hold "the frame at `off` is verified", allocating the
    /// set on first use.
    fn vlog_verified_slot(&self, off: u64) -> &AtomicU64 {
        let table = self.vlog_verified.get_or_init(|| {
            (0..VLOG_VERIFIED_SLOTS)
                .map(|_| AtomicU64::new(VLOG_SLOT_EMPTY))
                .collect()
        });
        // Fibonacci hash: consecutive frames are strided by the value size, so
        // the low bits alone would cluster for any regular value size.
        let i =
            (off.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & (VLOG_VERIFIED_SLOTS - 1);
        &table[i]
    }

    /// Check the frame at `off` against its stored CRC, at most once per frame
    /// per open reader.
    ///
    /// The mark is published only after a checksum *passes*, so a frame that
    /// fails is never recorded as verified and every later read re-checks it
    /// (and fails again). A racing pair of readers either both verify — the
    /// stores are identical — or one sees the other's mark; a `u64` store
    /// cannot tear, so no reader ever observes a half-written offset.
    #[inline]
    fn verify_vlog_frame(&self, off: u64, payload: &[u8], want: u32) -> Result<()> {
        let slot = self.vlog_verified_slot(off);
        if slot.load(AtOrd::Acquire) == off {
            return Ok(());
        }
        if checksum(payload) != want {
            return Err(corrupt());
        }
        slot.store(off, AtOrd::Release);
        Ok(())
    }

    #[cfg(feature = "mmap-reads")]
    fn read_vlog_from_mmap(&self, off: u64, len: usize, out: &mut Vec<u8>) -> Result<bool> {
        if !self.storage.supports_mmap() {
            return Ok(false);
        }
        let mmap = self.vlog_mmap_handle()?;
        let Ok(start) = usize::try_from(off) else {
            return Ok(false);
        };
        if self.vlog_v2 {
            let Some(header_end) = start.checked_add(VLOG_V2_HDR_LEN) else {
                return Ok(false);
            };
            let Some(header) = mmap.get(start..header_end) else {
                return Ok(false);
            };
            let want = read_u32(&header[0..4]);
            let compression = Compression::from_u8(header[4]).ok_or_else(corrupt)?;
            let payload_len = read_u32(&header[5..9]) as usize;
            if payload_len > len {
                return Err(corrupt());
            }
            let Some(payload_end) = header_end.checked_add(payload_len) else {
                return Ok(false);
            };
            let Some(payload) = mmap.get(header_end..payload_end) else {
                return Ok(false);
            };
            self.verify_vlog_frame(off, payload, want)?;
            append_vlog_payload(compression, payload, len, out)?;
            return Ok(true);
        }
        let Some(value_end) = start
            .checked_add(VLOG_CRC_LEN)
            .and_then(|value_start| value_start.checked_add(len))
        else {
            return Ok(false);
        };
        let Some(frame) = mmap.get(start..value_end) else {
            return Ok(false);
        };
        let want = read_u32(&frame[..VLOG_CRC_LEN]);
        let value = &frame[VLOG_CRC_LEN..];
        self.verify_vlog_frame(off, value, want)?;
        out.extend_from_slice(value);
        Ok(true)
    }

    fn read_vlog_from_file(&self, off: u64, len: usize, out: &mut Vec<u8>) -> Result<()> {
        let file = self.storage.open_read(&self.vlog_path)?;
        if self.vlog_v2 {
            let mut header = [0u8; VLOG_V2_HDR_LEN];
            file.read_exact_at(&mut header, off)?;
            let want = read_u32(&header[0..4]);
            let compression = Compression::from_u8(header[4]).ok_or_else(corrupt)?;
            let payload_len = read_u32(&header[5..9]) as usize;
            // Bound allocation before a corrupt header can request up to 4 GiB.
            // Writers store compressed bytes only when shorter than the raw
            // value, so a larger payload cannot be valid.
            if payload_len > len {
                return Err(corrupt());
            }
            let mut payload = vec![0u8; payload_len];
            file.read_exact_at(&mut payload, off + VLOG_V2_HDR_LEN as u64)?;
            self.verify_vlog_frame(off, &payload, want)?;
            return append_vlog_payload(compression, &payload, len, out);
        }
        let mut crc = [0u8; VLOG_CRC_LEN];
        file.read_exact_at(&mut crc, off)?;
        let want = read_u32(&crc);
        let start = out.len();
        out.resize(start + len, 0);
        file.read_exact_at(&mut out[start..], off + VLOG_CRC_LEN as u64)?;
        if let Err(error) = self.verify_vlog_frame(off, &out[start..], want) {
            out.truncate(start);
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn read_vlog(&self, off: u64, length: u64) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(length as usize);
        self.read_vlog_into(off, length, &mut buf)?;
        Ok(buf)
    }

    /// Append a vlog value to `out`, verifying its CRC32-C frame prefix and
    /// decompressing v2 frames. `off` is the frame start, `length` the
    /// logical (uncompressed) value length.
    pub(crate) fn read_vlog_into(&self, off: u64, length: u64, out: &mut Vec<u8>) -> Result<()> {
        let len = usize::try_from(length).map_err(|_| corrupt())?;
        // Consult the cache *before* the mmap attempt. A v2 frame is
        // decompressed on every mmap read — `vlog_verified` memoizes only the
        // checksum — so under `mmap-reads` this lookup is the one thing that
        // can remove the decompression, not just the I/O.
        if self.vlog_cache_limit > 0 {
            if let Some(cached) = self.bc.get(self.file_id, off, BlockDomain::Vlog) {
                // The domain tag makes the key unambiguous, so this can only
                // differ if the cache handed back something that was never
                // this frame. Trip loudly in debug; in release refuse the read
                // and drop the entry rather than return the wrong bytes.
                debug_assert_eq!(cached.len(), len, "vlog cache entry length");
                if cached.len() != len {
                    self.bc.remove(self.file_id, off, BlockDomain::Vlog);
                    return Err(corrupt());
                }
                out.extend_from_slice(&cached);
                crate::perf::bump(|p| p.vlog_cache_hits += 1);
                return Ok(());
            }
        }
        // Where this value's bytes start, so admission sees exactly the decode
        // and nothing the caller had already buffered.
        let start = out.len();
        // Charged before the read is issued, and at the funnel both paths pass
        // through: charging inside `read_vlog_from_file` alone would leave every
        // vlog read unpaced under `mmap-reads`, the configuration that reads the
        // most. The header is included because the frame is what leaves the
        // device, not the value.
        crate::ioctrl::charge(&self.limiter, length + VLOG_V2_HDR_LEN as u64);
        #[cfg(feature = "mmap-reads")]
        let served = self.read_vlog_from_mmap(off, len, out)?;
        #[cfg(not(feature = "mmap-reads"))]
        let served = false;
        if !served {
            self.read_vlog_from_file(off, len, out)?;
        }
        // One logical read per resolved value, whichever path served it —
        // counted here so the mmap fallback cannot double-count.
        crate::perf::bump(|p| {
            p.vlog_reads += 1;
            p.vlog_read_bytes += length;
        });
        // Admit only here, at the single join point of the two decode paths, so
        // both configs share one admission rule — and only after a complete
        // decode: every error above returned, so nothing cancelled, truncated
        // or CRC-failed can reach this line. An oversized value bypasses
        // without the copy `Arc::from` would cost.
        if self.vlog_cache_limit > 0 && len <= self.vlog_cache_limit && self.bc.enabled() {
            let decoded = &out[start..];
            debug_assert_eq!(decoded.len(), len, "decoded vlog value length");
            self.bc
                .put(self.file_id, off, BlockDomain::Vlog, Arc::from(decoded));
        }
        Ok(())
    }

    /// Lazily mmap the vlog file (created only when large values exist).
    #[cfg(feature = "mmap-reads")]
    fn vlog_mmap_handle(&self) -> Result<Arc<memmap2::Mmap>> {
        let mut guard = self.vlog_mmap.lock();
        if let Some(m) = guard.as_ref() {
            return Ok(m.clone());
        }
        let f = self.storage.open_read(&self.vlog_path)?;
        let file = f
            .as_file()
            .expect("a tier reporting supports_mmap() must back reads with a local file");
        // SAFETY: the vlog of a finished SSTable is immutable (see `open`).
        let mmap = Arc::new(unsafe { memmap2::Mmap::map(file)? });
        let _ = mmap.advise(memmap2::Advice::WillNeed);
        *guard = Some(mmap.clone());
        Ok(mmap)
    }

    /// A bidirectional iterator over this SSTable.
    pub fn iter(self: &Arc<Self>) -> SstIterator {
        SstIterator::new(self.clone())
    }

    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }
    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }
    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }
    pub fn file_id(&self) -> u64 {
        self.file_id
    }
    pub fn klog_path(&self) -> &str {
        &self.klog_path
    }
    pub fn vlog_path(&self) -> &str {
        &self.vlog_path
    }

    /// Evict this file's handles from the file cache.
    pub fn close(&self) {
        self.storage.release(&self.klog_path);
        self.storage.release(&self.vlog_path);
    }
}

/// Read and decode the framed block at `off`, returning the raw bytes **and the
/// algorithm the frame was stored with** — a caller cannot otherwise tell a
/// decompression from a raw copy, and `perf::bytes_decompressed` must count only
/// the former.
fn read_block_at(f: &dyn ReadHandle, off: u64, length: u64) -> Result<(Vec<u8>, Compression)> {
    let mut buf = vec![0u8; length as usize];
    f.read_exact_at(&mut buf, off)?;
    let (alg, payload, raw_len, _total) = crate::block::block_payload(&buf)?;
    let raw = crate::compress::decompress(alg, payload, raw_len)?;
    if raw.len() != raw_len {
        return Err(OndaError::Corruption("block: raw length mismatch".into()));
    }
    Ok((raw, alg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{BlockCache, FileCache};
    use crate::comparator::default_comparator;
    use crate::config::Compression;
    use crate::sst::{Writer, WriterOptions};
    use crate::storage::LocalStorage;

    fn local() -> Arc<dyn Storage> {
        LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads"))
    }

    fn small_reader(dir: &std::path::Path, n: usize) -> Arc<Reader> {
        let klog = dir.join("t.klog");
        let klog = klog.to_str().unwrap();
        let mut w = Writer::new(
            klog,
            WriterOptions {
                compression: Compression::None,
                compression_rules: Vec::new(),
                cmp: default_comparator(),
                enable_bloom: true,
                bloom_fpr: Some(0.01),
                klog_value_threshold: 512,
                block_size: 512,
                expected_entries: n,
                use_btree: false,
                restart_interval: 8,
                extended_entries: false,
                prefix_delta: false,
            },
        )
        .unwrap();
        for i in 0..n {
            let k = format!("key{i:06}");
            w.add(k.as_bytes(), b"value", (i + 1) as u64, 0, crate::format::KIND_PUT)
                .unwrap();
        }
        w.finish().unwrap();
        Reader::open(
            klog,
            local(),
            Arc::new(BlockCache::new(1 << 20)),
            1,
            default_comparator(),
            0,
        )
        .unwrap()
    }

    #[test]
    fn restart_trailer_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let r = small_reader(dir.path(), 500);
        assert!(r.has_restarts, "footer flag must be set");
        for i in 0..500 {
            let k = format!("key{i:06}");
            let (v, _, found, deleted, ..) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
            assert!(found && !deleted, "missing {k}");
            assert_eq!(v.unwrap(), b"value");
        }
        for probe in ["key00000", "key0005000", "aaa", "zzz"] {
            let (_, _, found, ..) = r.get(probe.as_bytes(), u64::MAX, 0).unwrap();
            assert!(!found, "phantom hit for {probe}");
        }
    }

    #[test]
    fn legacy_block_without_trailer_still_reads() {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("legacy.klog");
        let klog = klog.to_str().unwrap();
        let mut w = Writer::new(
            klog,
            WriterOptions {
                compression: Compression::None,
                compression_rules: Vec::new(),
                cmp: default_comparator(),
                enable_bloom: true,
                bloom_fpr: Some(0.01),
                klog_value_threshold: 512,
                block_size: 512,
                expected_entries: 300,
                use_btree: false,
                restart_interval: 0, // legacy: no trailer, no footer flag
                extended_entries: false,
                prefix_delta: false,
            },
        )
        .unwrap();
        for i in 0..300 {
            let k = format!("key{i:06}");
            w.add(k.as_bytes(), b"value", (i + 1) as u64, 0, crate::format::KIND_PUT)
                .unwrap();
        }
        w.finish().unwrap();
        let r = Reader::open(
            klog,
            local(),
            Arc::new(BlockCache::new(1 << 20)),
            2,
            default_comparator(),
            0,
        )
        .unwrap();
        assert!(!r.has_restarts);
        for i in 0..300 {
            let k = format!("key{i:06}");
            let (_, _, found, ..) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
            assert!(found, "missing {k}");
        }
        let mut it = r.iter();
        it.seek_to_first();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        assert_eq!(n, 300);
    }

    #[test]
    fn get_after_bloom_equivalent() {
        let dir = tempfile::tempdir().unwrap();
        let r = small_reader(dir.path(), 500);
        for probe in ["key000000", "key000499", "key000250", "nope", "zzz"] {
            let a = r.get(probe.as_bytes(), u64::MAX, 0).unwrap();
            let b = r.get_unfiltered(probe.as_bytes(), u64::MAX, 0).unwrap();
            assert_eq!(a, b, "get vs get_unfiltered diverge for {probe}");
        }
    }

    #[test]
    fn vlog_payload_append_is_atomic_on_a_length_mismatch() {
        let mut out = b"prefix".to_vec();

        append_vlog_payload(Compression::None, b"value", 5, &mut out).unwrap();
        assert_eq!(out, b"prefixvalue");

        let before = out.clone();
        assert!(append_vlog_payload(Compression::None, b"short", 7, &mut out).is_err());
        assert_eq!(out, before, "a corrupt frame must not append partial data");
    }

    /// Build a table at `path` with `opts`, filling it with prefix-heavy keys.
    fn write_table(path: &str, opts: WriterOptions, n: usize) {
        let mut w = Writer::new(path, opts).unwrap();
        for i in 0..n {
            let k = format!("tenant/alpha/cluster/{:04}/segment", i);
            w.add(k.as_bytes(), b"value", (i + 1) as u64, 0, crate::format::KIND_PUT)
                .unwrap();
        }
        w.finish().unwrap();
    }

    fn delta_opts(restart_interval: usize) -> WriterOptions {
        WriterOptions {
            compression: Compression::None,
            compression_rules: Vec::new(),
            cmp: default_comparator(),
            enable_bloom: false,
            bloom_fpr: None,
            klog_value_threshold: 1 << 20,
            block_size: 512,
            expected_entries: 128,
            use_btree: false,
            restart_interval,
            extended_entries: false,
            prefix_delta: true,
        }
    }

    /// Flip the footer flag byte of the klog at `path`.
    fn patch_footer_flags(path: &str, f: impl Fn(u8) -> u8) {
        let mut bytes = std::fs::read(path).unwrap();
        let at = bytes.len() - FOOTER_SIZE + 48;
        bytes[at] = f(bytes[at]);
        std::fs::write(path, bytes).unwrap();
    }

    fn open_at(path: &str) -> Result<Arc<Reader>> {
        Reader::open(
            path,
            local(),
            Arc::new(BlockCache::new(1 << 20)),
            9,
            default_comparator(),
            0,
        )
    }

    /// The delta layout is defined only over the extended entry, so the two
    /// flags may not be separated — and the combination cannot come from any
    /// writer, which is why it is `Corruption` and not `UnsupportedFormat`.
    #[test]
    fn prefix_delta_without_extended_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("d.klog");
        let klog = klog.to_str().unwrap();
        write_table(klog, delta_opts(8), 64);
        open_at(klog).expect("the unpatched delta table must open");
        patch_footer_flags(klog, |f| f & !FOOTER_EXTENDED_BLOCK);
        let err = open_at(klog).expect_err("delta without extended must be refused");
        assert_eq!(err.kind(), "corruption", "{err}");
        assert!(err.to_string().contains("FOOTER_EXTENDED_BLOCK"), "{err}");
    }

    /// Without a restart trailer a delta block is decodable only from offset 0
    /// — no seek, no reverse iteration.
    #[test]
    fn prefix_delta_without_restarts_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("d.klog");
        let klog = klog.to_str().unwrap();
        write_table(klog, delta_opts(8), 64);
        patch_footer_flags(klog, |f| f & !FOOTER_RESTARTS);
        let err = open_at(klog).expect_err("delta without restarts must be refused");
        assert_eq!(err.kind(), "corruption", "{err}");
        assert!(err.to_string().contains("FOOTER_RESTARTS"), "{err}");
    }

    /// Task-5 refactor guard: `restart_scan_offset` now runs through the shared
    /// `restart_lower_bound`, and must return byte-for-byte the offsets the
    /// open-coded binary search returned over a frozen legacy fixture.
    #[test]
    fn restart_lower_bound_matches_legacy_scan_offset() {
        let path = crate::util::phase1_fixture("klog_legacy_flat_restarts_bloom.klog");
        let path = path.to_str().unwrap();
        let r = open_at(path).unwrap();
        assert!(r.has_restarts && !r.prefix_delta);
        // The pre-refactor body, verbatim.
        let legacy = |raw: &[u8], restarts: &[u8], key: &[u8], seq: u64| -> usize {
            if restarts.len() < 8 {
                return 0;
            }
            let restart_off = |i: usize| read_u32(&restarts[i * 4..]) as usize;
            let (mut lo, mut hi) = (0usize, restarts.len() / 4);
            while lo < hi {
                let mid = (lo + hi) / 2;
                let (entry, _) = decode_entry(raw, r.entry_layout, restart_off(mid)).unwrap();
                if cmp_internal(&r.cmp, entry.user_key(raw), entry.seq, key, seq).is_lt() {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo > 0 {
                restart_off(lo - 1)
            } else {
                0
            }
        };
        let mut probes: Vec<String> = (1..45u64).map(|i| format!("k{i:02}")).collect();
        probes.extend(["a".into(), "k00".into(), "k99".into(), "zzz".into()]);
        let mut checked = 0;
        for bi in 0..r.data_block_count() {
            let block = r.read_data_block_local(bi).unwrap();
            let (raw, restarts) = r.split_block(block.bytes()).unwrap();
            for probe in &probes {
                for seq in [0u64, 25, u64::MAX] {
                    let want = legacy(raw, restarts, probe.as_bytes(), seq);
                    let got = r
                        .restart_scan_offset(raw, restarts, probe.as_bytes(), seq)
                        .unwrap();
                    assert_eq!(got, want, "block {bi}, probe {probe}, seq {seq}");
                    checked += 1;
                }
            }
        }
        assert!(checked > 100, "the fixture must exercise the search");
    }

    /// The batch point-read planner in `column_family.rs` drives the reader's
    /// block walk itself (one block fetch for many keys) instead of calling
    /// `get_unfiltered` per key. This pins that the hand-driven sequence is
    /// equivalent to the packaged one, so the two cannot drift.
    #[test]
    fn get_unfiltered_matches_manual_block_walk() {
        const NOW: i64 = 1_000_000;
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("walk.klog");
        let klog = klog.to_str().unwrap();
        let mut w = Writer::new(
            klog,
            WriterOptions {
                compression: Compression::None,
                compression_rules: Vec::new(),
                cmp: default_comparator(),
                enable_bloom: true,
                bloom_fpr: Some(0.01),
                klog_value_threshold: 512,
                block_size: 256, // several blocks, so `find_block` matters
                expected_entries: 200,
                use_btree: false,
                restart_interval: 4,
                extended_entries: false,
                prefix_delta: false,
            },
        )
        .unwrap();
        for i in 0..200u64 {
            let k = format!("key{i:04}");
            let seq = i + 1;
            match i % 4 {
                // A tombstone, an expired-TTL entry, and two live values.
                1 => w.add(k.as_bytes(), b"", seq, 0, crate::format::KIND_DELETE),
                2 => w.add(k.as_bytes(), b"expired", seq, NOW - 1, crate::format::KIND_PUT),
                _ => w.add(k.as_bytes(), b"live", seq, 0, crate::format::KIND_PUT),
            }
            .unwrap();
        }
        w.finish().unwrap();
        let r = Reader::open(
            klog,
            local(),
            Arc::new(BlockCache::new(1 << 20)),
            1,
            default_comparator(),
            0,
        )
        .unwrap();

        let mut probes: Vec<String> = (0..200u64).map(|i| format!("key{i:04}")).collect();
        // Absent keys before, inside and after the table's range.
        probes.extend(["aaa".into(), "key0000x".into(), "zzz".into()]);
        for probe in &probes {
            let probe = probe.as_bytes();
            let want = r.get_unfiltered(probe, u64::MAX, NOW).unwrap();
            let bi = r.find_block(probe, u64::MAX);
            let got = if bi >= r.data_block_count() {
                (None, 0, false, false, crate::format::KIND_PUT)
            } else {
                let block = r.read_data_block_local(bi).unwrap();
                let (raw, restarts) = r.split_block(block.bytes()).unwrap();
                let off = r
                    .restart_scan_offset(raw, restarts, probe, u64::MAX)
                    .unwrap();
                r.scan_point_entry(raw, off, probe, u64::MAX, NOW).unwrap()
            };
            assert_eq!(
                got,
                want,
                "manual walk diverged for {}",
                String::from_utf8_lossy(probe)
            );
        }
    }
}
