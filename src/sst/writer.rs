//! SSTable writer.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use super::{
    data_block_alg, encode_entry, vlog_path_for, BlockHandle, FileMeta, IndexEntry,
    DEFAULT_BLOCK_SIZE, FOOTER_BTREE, FOOTER_HAS_BLOOM, FOOTER_MAGIC, FOOTER_SIZE, VLOG_CRC_LEN,
};
use crate::block::write_block;
use crate::bloom::Bloom;
use crate::comparator::ComparatorRef;
use crate::config::Compression;
use crate::encoding::{append_uvarint, checksum, put_u32, put_u64};
use crate::error::Result;

/// Configuration for SSTable construction.
#[derive(Clone)]
pub struct WriterOptions {
    pub compression: Compression,
    pub cmp: ComparatorRef,
    pub enable_bloom: bool,
    pub bloom_fpr: f64,
    pub klog_value_threshold: usize,
    pub block_size: usize,
    pub expected_entries: usize,
    /// Write a B+tree (hybrid klog) index instead of a flat single-level index.
    pub use_btree: bool,
}

/// Fan-out (entries per node) for the B+tree index.
const BTREE_FANOUT: usize = 256;

impl std::fmt::Debug for WriterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterOptions")
            .field("compression", &self.compression)
            .field("enable_bloom", &self.enable_bloom)
            .finish()
    }
}

/// Builds a single SSTable.  Entries must be added in internal order (user key
/// ascending; for equal user keys, sequence descending).
pub struct Writer {
    klog_path: String,
    vlog_path: String,
    klog: Option<BufWriter<File>>,
    vlog: Option<BufWriter<File>>,
    opts: WriterOptions,

    cur_block: Vec<u8>,
    index: Vec<IndexEntry>,
    klog_off: u64,
    vlog_off: u64,

    bloom: Option<Bloom>,
    num_entries: u64,
    num_tombstones: u64,
    max_seq: u64,
    min_key: Option<Vec<u8>>,
    last_user_key: Vec<u8>,
    last_seq: u64,
    pending_block: bool,
    finished: bool,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("klog_path", &self.klog_path)
            .field("num_entries", &self.num_entries)
            .finish()
    }
}

impl Writer {
    /// Create an SSTable writer for `klog_path`. The vlog path is derived and
    /// created lazily on the first large value.
    pub fn new(klog_path: &str, mut opts: WriterOptions) -> Result<Writer> {
        if opts.block_size == 0 {
            opts.block_size = DEFAULT_BLOCK_SIZE;
        }
        if opts.klog_value_threshold == 0 {
            opts.klog_value_threshold = 512;
        }
        let f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(klog_path)?;
        let bloom = if opts.enable_bloom {
            Some(Bloom::new(opts.expected_entries.max(1024), opts.bloom_fpr))
        } else {
            None
        };
        Ok(Writer {
            klog_path: klog_path.to_string(),
            vlog_path: vlog_path_for(klog_path),
            klog: Some(BufWriter::with_capacity(256 << 10, f)),
            vlog: None,
            opts,
            cur_block: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            index: Vec::new(),
            klog_off: 0,
            vlog_off: 0,
            bloom,
            num_entries: 0,
            num_tombstones: 0,
            max_seq: 0,
            min_key: None,
            last_user_key: Vec::new(),
            last_seq: 0,
            pending_block: false,
            finished: false,
        })
    }

    /// Append one entry. `value` is ignored for tombstones.
    pub fn add(
        &mut self,
        user_key: &[u8],
        value: &[u8],
        seq: u64,
        ttl: i64,
        tombstone: bool,
        single_delete: bool,
    ) -> Result<()> {
        if self.min_key.is_none() {
            self.min_key = Some(user_key.to_vec());
        }
        if let Some(b) = self.bloom.as_mut() {
            b.add(user_key);
        }

        let mut has_vlog = false;
        let mut vlog_off = 0u64;
        if !tombstone && value.len() >= self.opts.klog_value_threshold {
            vlog_off = self.write_vlog(value)?;
            has_vlog = true;
        }

        encode_entry(
            &mut self.cur_block,
            user_key,
            value,
            seq,
            ttl,
            tombstone,
            single_delete,
            has_vlog,
            vlog_off,
        );
        self.num_entries += 1;
        if tombstone {
            self.num_tombstones += 1;
        }
        if seq > self.max_seq {
            self.max_seq = seq;
        }
        self.last_user_key.clear();
        self.last_user_key.extend_from_slice(user_key);
        self.last_seq = seq;
        self.pending_block = true;

        if self.cur_block.len() >= self.opts.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Append a value to the vlog as a `[crc32c u32 LE][value]` frame and return
    /// the frame's start offset. The crc lets reads detect silent corruption of
    /// large values, matching the checksum coverage klog blocks already have.
    fn write_vlog(&mut self, value: &[u8]) -> Result<u64> {
        if self.vlog.is_none() {
            let f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.vlog_path)?;
            self.vlog = Some(BufWriter::with_capacity(256 << 10, f));
        }
        let off = self.vlog_off;
        let mut hdr = [0u8; 4];
        put_u32(&mut hdr, checksum(value));
        let w = self.vlog.as_mut().unwrap();
        w.write_all(&hdr)?;
        w.write_all(value)?;
        self.vlog_off += VLOG_CRC_LEN as u64 + value.len() as u64;
        Ok(off)
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.cur_block.is_empty() {
            return Ok(());
        }
        let mut framed = Vec::new();
        let n = write_block(
            &mut framed,
            data_block_alg(self.opts.compression),
            &self.cur_block,
        )?;
        self.klog.as_mut().unwrap().write_all(&framed)?;
        self.index.push(IndexEntry {
            user_key: self.last_user_key.clone(),
            seq: self.last_seq,
            handle: BlockHandle {
                offset: self.klog_off,
                length: n as u64,
            },
        });
        self.klog_off += n as u64;
        self.cur_block.clear();
        self.pending_block = false;
        Ok(())
    }

    fn write_meta_block(&mut self, payload: &[u8]) -> Result<BlockHandle> {
        let mut framed = Vec::new();
        let n = write_block(&mut framed, Compression::None, payload)?;
        self.klog.as_mut().unwrap().write_all(&framed)?;
        let h = BlockHandle {
            offset: self.klog_off,
            length: n as u64,
        };
        self.klog_off += n as u64;
        Ok(h)
    }

    fn encode_index(&self) -> Vec<u8> {
        let min_key = self.min_key.as_deref().unwrap_or(&[]);
        let mut dst = Vec::with_capacity(32 + self.index.len() * 32);
        append_uvarint(&mut dst, min_key.len() as u64);
        dst.extend_from_slice(min_key);
        append_uvarint(&mut dst, self.index.len() as u64);
        for e in &self.index {
            append_uvarint(&mut dst, e.user_key.len() as u64);
            dst.extend_from_slice(&e.user_key);
            append_uvarint(&mut dst, e.seq);
            append_uvarint(&mut dst, e.handle.offset);
            append_uvarint(&mut dst, e.handle.length);
        }
        dst
    }

    /// Build the B+tree (hybrid klog) index on disk and return the root handle.
    ///
    /// Leaf nodes hold `(separator, data-block handle)` entries; internal nodes
    /// hold `(separator, child-node handle)` entries; the root additionally
    /// carries the SSTable's min key.  Nodes are written bottom-up so each parent
    /// references already-written children.
    fn write_btree_index(&mut self) -> Result<BlockHandle> {
        // Leaf level: chunk the per-data-block separators into nodes.
        let mut level: Vec<(Vec<u8>, BlockHandle)> = Vec::new();
        let entries: Vec<IndexEntry> = std::mem::take(&mut self.index);
        for chunk in entries.chunks(BTREE_FANOUT) {
            let mut buf = Vec::new();
            buf.push(1u8); // leaf
            append_uvarint(&mut buf, chunk.len() as u64);
            for e in chunk {
                append_uvarint(&mut buf, e.user_key.len() as u64);
                buf.extend_from_slice(&e.user_key);
                append_uvarint(&mut buf, e.seq);
                append_uvarint(&mut buf, e.handle.offset);
                append_uvarint(&mut buf, e.handle.length);
            }
            let sep = chunk.last().map(|e| e.user_key.clone()).unwrap_or_default();
            let handle = self.write_meta_block(&buf)?;
            level.push((sep, handle));
        }
        if level.is_empty() {
            // Empty SSTable: write a single empty leaf as the root.
            let mut buf = vec![1u8];
            append_uvarint(&mut buf, 0);
            level.push((Vec::new(), self.write_meta_block(&buf)?));
        }

        // Build internal levels until a single root remains.
        let min_key = self.min_key.clone().unwrap_or_default();
        loop {
            let is_root = level.len() <= BTREE_FANOUT;
            let mut parent: Vec<(Vec<u8>, BlockHandle)> = Vec::new();
            for chunk in level.chunks(BTREE_FANOUT) {
                let mut buf = Vec::new();
                buf.push(0u8); // internal
                if is_root {
                    append_uvarint(&mut buf, min_key.len() as u64);
                    buf.extend_from_slice(&min_key);
                }
                append_uvarint(&mut buf, chunk.len() as u64);
                for (sep, h) in chunk {
                    append_uvarint(&mut buf, sep.len() as u64);
                    buf.extend_from_slice(sep);
                    append_uvarint(&mut buf, h.offset);
                    append_uvarint(&mut buf, h.length);
                }
                let sep = chunk.last().map(|(s, _)| s.clone()).unwrap_or_default();
                parent.push((sep, self.write_meta_block(&buf)?));
            }
            if is_root {
                return Ok(parent[0].1);
            }
            level = parent;
        }
    }

    /// Flush the final block, write the bloom/index blocks and footer, and
    /// return the SSTable metadata.  The writer must not be used afterwards.
    pub fn finish(mut self) -> Result<FileMeta> {
        if self.pending_block {
            self.flush_block()?;
        }

        let mut footer_flags = 0u8;
        let mut bloom_handle = BlockHandle::default();
        if let Some(b) = self.bloom.take() {
            let enc = b.encode();
            bloom_handle = self.write_meta_block(&enc)?;
            footer_flags |= FOOTER_HAS_BLOOM;
        }

        let index_handle = if self.opts.use_btree {
            footer_flags |= FOOTER_BTREE;
            self.write_btree_index()?
        } else {
            let index_bytes = self.encode_index();
            self.write_meta_block(&index_bytes)?
        };

        let mut footer = [0u8; FOOTER_SIZE];
        put_u64(&mut footer[0..8], index_handle.offset);
        put_u64(&mut footer[8..16], index_handle.length);
        put_u64(&mut footer[16..24], bloom_handle.offset);
        put_u64(&mut footer[24..32], bloom_handle.length);
        put_u64(&mut footer[32..40], self.num_entries);
        put_u64(&mut footer[40..48], self.max_seq);
        footer[48] = footer_flags;
        put_u64(&mut footer[56..64], FOOTER_MAGIC);

        let mut klog = self.klog.take().unwrap();
        klog.write_all(&footer)?;
        klog.flush()?;
        let mut klog = klog.into_inner().map_err(|e| e.into_error())?;
        klog.sync_all()?;
        let klog_size = klog.seek(SeekFrom::End(0))?;

        let mut vlog_size = 0u64;
        if let Some(vlog) = self.vlog.take() {
            let mut vlog = vlog.into_inner().map_err(|e| e.into_error())?;
            vlog.flush()?;
            vlog.sync_all()?;
            vlog_size = vlog.seek(SeekFrom::End(0))?;
        }

        // fsync the containing directory so the newly-created klog/vlog dir entries
        // are durable. Without this a crash can leave the manifest referencing files
        // whose directory entry never reached disk. `sync_all` above only persists
        // file *contents*, not the link in the parent directory.
        sync_parent_dir(&self.klog_path)?;

        self.finished = true;
        Ok(FileMeta {
            id: 0,
            min_key: self.min_key.take().unwrap_or_default(),
            max_key: std::mem::take(&mut self.last_user_key),
            num_entries: self.num_entries,
            num_tombstones: self.num_tombstones,
            max_seq: self.max_seq,
            klog_size,
            vlog_size,
        })
    }

    /// Close and remove partially written files (call on error before finish).
    pub fn abort(mut self) {
        self.klog.take();
        self.vlog.take();
        let _ = std::fs::remove_file(&self.klog_path);
        let _ = std::fs::remove_file(&self.vlog_path);
        self.finished = true;
    }
}

/// fsync the parent directory of `file_path`, making a just-created file's
/// directory entry durable. A missing parent is treated as success.
fn sync_parent_dir(file_path: &str) -> Result<()> {
    if let Some(dir) = Path::new(file_path).parent() {
        // An empty parent means the current directory; skip.
        if dir.as_os_str().is_empty() {
            return Ok(());
        }
        let d = File::open(dir)?;
        d.sync_all()?;
    }
    Ok(())
}
