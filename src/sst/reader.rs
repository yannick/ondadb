//! SSTable reader: point lookups and ordered iteration over a finished SSTable.

use std::sync::atomic::{AtomicU64, Ordering as AtOrd};
use std::sync::{Arc, OnceLock};

use super::{
    cmp_internal, decode_entry, vlog_path_for, Block, BlockHandle, IndexEntry, SstIterator,
    FOOTER_BTREE, FOOTER_HAS_BLOOM, FOOTER_MAGIC, FOOTER_RESTARTS, FOOTER_SIZE, FOOTER_VLOG_V2,
    VLOG_CRC_LEN, VLOG_V2_HDR_LEN,
};
use crate::block::read_block;
use crate::bloom::Bloom;
use crate::cache::BlockCache;
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

type PointResult = (Option<Vec<u8>>, u64, bool, bool);

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
    fn bytes(&self) -> &[u8] {
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
    pub fn open(
        klog_path: &str,
        storage: Arc<dyn Storage>,
        bc: Arc<BlockCache>,
        file_id: u64,
        cmp: ComparatorRef,
    ) -> Result<Arc<Reader>> {
        let mut r = Reader {
            klog_path: klog_path.to_string(),
            vlog_path: vlog_path_for(klog_path),
            storage,
            bc,
            file_id,
            cmp,
            index: Vec::new(),
            min_key: Vec::new(),
            max_key: Vec::new(),
            num_entries: 0,
            max_seq: 0,
            bloom: None,
            has_restarts: false,
            vlog_v2: false,
            vlog_verified: OnceLock::new(),
            #[cfg(feature = "mmap-reads")]
            klog_mmap: None,
            #[cfg(feature = "mmap-reads")]
            vlog_mmap: parking_lot::Mutex::new(None),
            #[cfg(feature = "mmap-reads")]
            verified: Vec::new(),
        };
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
        r.has_restarts = flags & FOOTER_RESTARTS != 0;
        r.vlog_v2 = flags & FOOTER_VLOG_V2 != 0;

        if flags & FOOTER_HAS_BLOOM != 0 && bloom_len > 0 {
            let raw = read_block_at(&*f, bloom_off, bloom_len)?;
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
            let idx_raw = read_block_at(&*f, index_off, index_len)?;
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
        let block = read_block_at(f, h.offset, h.length)?;
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
                let p = crate::block::block_payload(&mmap[start..end])?;
                self.verified[word].fetch_or(bit, AtOrd::AcqRel);
                p
            };
            let (alg, payload, raw_len, _total) = parsed;
            if alg == Compression::None {
                // Zero-copy: point straight at the mapped raw bytes.
                let payload_start = start + crate::block::BLOCK_HEADER;
                return Ok(Block::Mapped {
                    mmap: mmap.clone(),
                    start: payload_start,
                    len: raw_len,
                });
            }
            // Compressed: decompress once, cache the owned result.
            if let Some(raw) = self.bc.get(self.file_id, h.offset) {
                return Ok(Block::Owned(raw));
            }
            let raw = crate::compress::decompress(alg, payload, raw_len)?;
            let arc: Arc<[u8]> = Arc::from(raw.into_boxed_slice());
            self.bc.put(self.file_id, h.offset, arc.clone());
            return Ok(Block::Owned(arc));
        }

        if let Some(raw) = self.bc.get(self.file_id, h.offset) {
            return Ok(Block::Owned(raw));
        }
        let f = self.storage.open_read(&self.klog_path)?;
        let raw = read_block_at(&*f, h.offset, h.length)?;
        let arc: Arc<[u8]> = Arc::from(raw.into_boxed_slice());
        self.bc.put(self.file_id, h.offset, arc.clone());
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
                let p = crate::block::block_payload(&mmap[start..end])?;
                self.verified[word].fetch_or(bit, AtOrd::AcqRel);
                p
            };
            let (alg, _payload, raw_len, _total) = parsed;
            if alg == Compression::None {
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
        Ok((&raw[..entries_end], &raw[entries_end..raw.len() - 4]))
    }

    /// Index of the first data block whose last key is `>= (user_key, seq)`.
    pub(crate) fn find_block(&self, user_key: &[u8], seq: u64) -> usize {
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

    /// Resolve `user_key` as of `read_seq`. `found` indicates a version exists in
    /// this SSTable; `deleted` indicates a tombstone or expired entry.
    pub fn get(
        &self,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
    ) -> Result<(Option<Vec<u8>>, u64, bool, bool)> {
        if let Some(b) = &self.bloom {
            if !b.may_contain(user_key) {
                return Ok((None, 0, false, false));
            }
        }
        self.get_unfiltered(user_key, read_seq, now)
    }

    fn restart_scan_offset(
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
        // predecessor so the target cannot lie in a skipped interval.
        let restart_off = |i: usize| read_u32(&restarts[i * 4..]) as usize;
        let (mut lo, mut hi) = (0usize, restarts.len() / 4);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (entry, _) = decode_entry(raw, restart_off(mid))?;
            if cmp_internal(
                &self.cmp,
                entry.user_key(raw),
                entry.seq,
                user_key,
                read_seq,
            )
            .is_lt()
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(if lo > 0 { restart_off(lo - 1) } else { 0 })
    }

    fn scan_point_entry(
        &self,
        raw: &[u8],
        mut offset: usize,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
    ) -> Result<PointResult> {
        while offset < raw.len() {
            let (entry, next) = decode_entry(raw, offset)?;
            let entry_key = entry.user_key(raw);
            if cmp_internal(&self.cmp, entry_key, entry.seq, user_key, read_seq).is_lt() {
                offset = next;
                continue;
            }
            if self.cmp.compare(entry_key, user_key) != std::cmp::Ordering::Equal {
                break;
            }
            if entry.tombstone() || (entry.ttl != 0 && entry.ttl <= now) {
                return Ok((None, entry.seq, true, true));
            }
            let value = if entry.has_vlog() {
                self.read_vlog(entry.vlog_off, entry.val_len as u64)?
            } else {
                entry.inline_value(raw).to_vec()
            };
            return Ok((Some(value), entry.seq, true, false));
        }
        Ok((None, 0, false, false))
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
            return Ok((None, 0, false, false));
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
        #[cfg(feature = "mmap-reads")]
        if self.read_vlog_from_mmap(off, len, out)? {
            return Ok(());
        }
        self.read_vlog_from_file(off, len, out)
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

fn read_block_at(f: &dyn ReadHandle, off: u64, length: u64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; length as usize];
    f.read_exact_at(&mut buf, off)?;
    let (raw, _) = read_block(&buf)?;
    Ok(raw)
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
                bloom_fpr: 0.01,
                klog_value_threshold: 512,
                block_size: 512,
                expected_entries: n,
                use_btree: false,
                restart_interval: 8,
            },
        )
        .unwrap();
        for i in 0..n {
            let k = format!("key{i:06}");
            w.add(k.as_bytes(), b"value", (i + 1) as u64, 0, false, false)
                .unwrap();
        }
        w.finish().unwrap();
        Reader::open(
            klog,
            local(),
            Arc::new(BlockCache::new(1 << 20)),
            1,
            default_comparator(),
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
            let (v, _, found, deleted) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
            assert!(found && !deleted, "missing {k}");
            assert_eq!(v.unwrap(), b"value");
        }
        for probe in ["key00000", "key0005000", "aaa", "zzz"] {
            let (_, _, found, _) = r.get(probe.as_bytes(), u64::MAX, 0).unwrap();
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
                bloom_fpr: 0.01,
                klog_value_threshold: 512,
                block_size: 512,
                expected_entries: 300,
                use_btree: false,
                restart_interval: 0, // legacy: no trailer, no footer flag
            },
        )
        .unwrap();
        for i in 0..300 {
            let k = format!("key{i:06}");
            w.add(k.as_bytes(), b"value", (i + 1) as u64, 0, false, false)
                .unwrap();
        }
        w.finish().unwrap();
        let r = Reader::open(
            klog,
            local(),
            Arc::new(BlockCache::new(1 << 20)),
            2,
            default_comparator(),
        )
        .unwrap();
        assert!(!r.has_restarts);
        for i in 0..300 {
            let k = format!("key{i:06}");
            let (_, _, found, _) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
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
}
