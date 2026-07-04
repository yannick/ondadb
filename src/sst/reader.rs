//! SSTable reader: point lookups and ordered iteration over a finished SSTable.

use std::os::unix::fs::FileExt;
use std::sync::Arc;

use super::{
    cmp_internal, decode_entry, vlog_path_for, Block, BlockHandle, IndexEntry, SstIterator,
    FOOTER_BTREE, FOOTER_HAS_BLOOM, FOOTER_MAGIC, FOOTER_SIZE, VLOG_CRC_LEN,
};
use crate::block::read_block;
use crate::bloom::Bloom;
use crate::cache::{BlockCache, FileCache};
use crate::comparator::ComparatorRef;
#[cfg(feature = "unsafe-fastpath")]
use crate::config::Compression;
use crate::encoding::{checksum, read_u32, read_u64, uvarint};
use crate::error::{OndaError, Result};

/// Reads a finished SSTable.  The footer, index and bloom filter are loaded on
/// open; data blocks are read on demand through the block cache (or, under
/// `unsafe-fastpath`, served zero-copy from an mmap of the klog file).
pub struct Reader {
    klog_path: String,
    vlog_path: String,
    fc: Arc<FileCache>,
    bc: Arc<BlockCache>,
    file_id: u64,
    cmp: ComparatorRef,

    pub(crate) index: Vec<IndexEntry>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    num_entries: u64,
    max_seq: u64,
    bloom: Option<Bloom>,

    #[cfg(feature = "unsafe-fastpath")]
    klog_mmap: Option<Arc<memmap2::Mmap>>,
    #[cfg(feature = "unsafe-fastpath")]
    vlog_mmap: parking_lot::Mutex<Option<Arc<memmap2::Mmap>>>,
    /// One bit per data block: set once the block's CRC has been verified.
    /// SSTable bytes are immutable, so each block only needs checking on its
    /// first read — not on every read by every scanning thread.
    #[cfg(feature = "unsafe-fastpath")]
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

impl Reader {
    /// Open the SSTable at `klog_path`. `file_id` must be unique per file for
    /// block-cache keying.
    pub fn open(
        klog_path: &str,
        fc: Arc<FileCache>,
        bc: Arc<BlockCache>,
        file_id: u64,
        cmp: ComparatorRef,
    ) -> Result<Arc<Reader>> {
        let mut r = Reader {
            klog_path: klog_path.to_string(),
            vlog_path: vlog_path_for(klog_path),
            fc,
            bc,
            file_id,
            cmp,
            index: Vec::new(),
            min_key: Vec::new(),
            max_key: Vec::new(),
            num_entries: 0,
            max_seq: 0,
            bloom: None,
            #[cfg(feature = "unsafe-fastpath")]
            klog_mmap: None,
            #[cfg(feature = "unsafe-fastpath")]
            vlog_mmap: parking_lot::Mutex::new(None),
            #[cfg(feature = "unsafe-fastpath")]
            verified: Vec::new(),
        };
        let f = r.fc.acquire(klog_path)?;
        let size = f.metadata()?.len();
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

        if flags & FOOTER_HAS_BLOOM != 0 && bloom_len > 0 {
            let raw = read_block_at(&f, bloom_off, bloom_len)?;
            r.bloom = Some(Bloom::decode(&raw)?);
        }
        if flags & FOOTER_BTREE != 0 {
            // Hybrid klog: the index handle points at the B+tree root. Walk the
            // tree (root → ... → leaves) to rebuild the in-memory flat index.
            r.load_btree(
                &f,
                BlockHandle {
                    offset: index_off,
                    length: index_len,
                },
            )?;
        } else {
            let idx_raw = read_block_at(&f, index_off, index_len)?;
            r.decode_index(&idx_raw)?;
        }

        // SAFETY (`unsafe-fastpath`): the klog is an immutable, finished SSTable;
        // ondaDB never writes to it after `finish`, and compaction only *unlinks*
        // it (the pages stay valid while this mapping holds the inode). The mmap
        // is owned by the Reader, so views into it live exactly as long as it.
        #[cfg(feature = "unsafe-fastpath")]
        {
            let mmap = unsafe { memmap2::Mmap::map(&*f)? };
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
    fn load_btree(&mut self, f: &std::fs::File, root: BlockHandle) -> Result<()> {
        self.walk_btree_node(f, root, true)?;
        self.max_key = self
            .index
            .last()
            .map(|e| e.user_key.clone())
            .unwrap_or_else(|| self.min_key.clone());
        Ok(())
    }

    fn walk_btree_node(&mut self, f: &std::fs::File, h: BlockHandle, is_root: bool) -> Result<()> {
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
    /// Under `unsafe-fastpath`, an *uncompressed* block is returned as a
    /// zero-copy view into the mmap; compressed blocks are decompressed once and
    /// cached.  Otherwise the block is read through the block cache.
    pub(crate) fn read_data_block(&self, i: usize) -> Result<Block> {
        let h = self.index[i].handle;

        #[cfg(feature = "unsafe-fastpath")]
        if let Some(mmap) = &self.klog_mmap {
            use std::sync::atomic::Ordering as AtOrd;
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
        let f = self.fc.acquire(&self.klog_path)?;
        let raw = read_block_at(&f, h.offset, h.length)?;
        let arc: Arc<[u8]> = Arc::from(raw.into_boxed_slice());
        self.bc.put(self.file_id, h.offset, arc.clone());
        Ok(Block::Owned(arc))
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

    /// Whether the bloom filter admits `user_key` (`true` when no filter).
    pub(crate) fn bloom_may_contain(&self, user_key: &[u8]) -> bool {
        self.bloom.as_ref().is_none_or(|b| b.may_contain(user_key))
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
        let bi = self.find_block(user_key, read_seq);
        if bi >= self.index.len() {
            return Ok((None, 0, false, false));
        }
        let block = self.read_data_block(bi)?;
        let raw = block.bytes();
        let mut off = 0;
        while off < raw.len() {
            let (e, next) = decode_entry(raw, off)?;
            let ek = e.user_key(raw);
            if cmp_internal(&self.cmp, ek, e.seq, user_key, read_seq).is_lt() {
                off = next;
                continue;
            }
            if self.cmp.compare(ek, user_key) != std::cmp::Ordering::Equal {
                return Ok((None, 0, false, false)); // key absent
            }
            if e.tombstone() {
                return Ok((None, e.seq, true, true));
            }
            if e.ttl != 0 && e.ttl <= now {
                return Ok((None, e.seq, true, true));
            }
            if e.has_vlog() {
                let v = self.read_vlog(e.vlog_off, e.val_len as u64)?;
                return Ok((Some(v), e.seq, true, false));
            }
            return Ok((Some(e.inline_value(raw).to_vec()), e.seq, true, false));
        }
        Ok((None, 0, false, false))
    }

    pub(crate) fn read_vlog(&self, off: u64, length: u64) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(length as usize);
        self.read_vlog_into(off, length, &mut buf)?;
        Ok(buf)
    }

    /// Append a vlog value to `out`, verifying its CRC32-C frame prefix. `off` is
    /// the frame start (crc), `length` the logical value length.
    pub(crate) fn read_vlog_into(&self, off: u64, length: u64, out: &mut Vec<u8>) -> Result<()> {
        let len = length as usize;
        #[cfg(feature = "unsafe-fastpath")]
        {
            let mmap = self.vlog_mmap_handle()?;
            let s = off as usize;
            let e = s + VLOG_CRC_LEN + len;
            if e <= mmap.len() {
                let want = read_u32(&mmap[s..s + VLOG_CRC_LEN]);
                let val = &mmap[s + VLOG_CRC_LEN..e];
                if checksum(val) != want {
                    return Err(corrupt());
                }
                out.extend_from_slice(val);
                return Ok(());
            }
        }
        let f = self.fc.acquire(&self.vlog_path)?;
        let mut crc_buf = [0u8; VLOG_CRC_LEN];
        f.read_exact_at(&mut crc_buf, off)?;
        let want = read_u32(&crc_buf);
        let start = out.len();
        out.resize(start + len, 0);
        f.read_exact_at(&mut out[start..], off + VLOG_CRC_LEN as u64)?;
        if checksum(&out[start..]) != want {
            out.truncate(start);
            return Err(corrupt());
        }
        Ok(())
    }

    /// Lazily mmap the vlog file (created only when large values exist).
    #[cfg(feature = "unsafe-fastpath")]
    fn vlog_mmap_handle(&self) -> Result<Arc<memmap2::Mmap>> {
        let mut guard = self.vlog_mmap.lock();
        if let Some(m) = guard.as_ref() {
            return Ok(m.clone());
        }
        let f = self.fc.acquire(&self.vlog_path)?;
        // SAFETY: the vlog of a finished SSTable is immutable (see `open`).
        let mmap = Arc::new(unsafe { memmap2::Mmap::map(&*f)? });
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
        self.fc.evict(&self.klog_path);
        self.fc.evict(&self.vlog_path);
    }
}

fn read_block_at(f: &std::fs::File, off: u64, length: u64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; length as usize];
    f.read_exact_at(&mut buf, off)?;
    let (raw, _) = read_block(&buf)?;
    Ok(raw)
}
