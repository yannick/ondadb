//! SSTable writer.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use super::{
    data_block_alg, encode_entry, encode_entry_delta, encode_footer, encode_vlog_header,
    vlog_path_for, BlockHandle, EntryLayout, FileMeta, FooterFields, IndexEntry,
    DEFAULT_BLOCK_SIZE, VLOG_FRAME_HDR_LEN, VLOG_HEADER_LEN,
};
use crate::block::write_block;
use crate::bloom::Bloom;
use crate::comparator::ComparatorRef;
use crate::compress::compress as do_compress;
use crate::config::{compression_for_key, Compression, CompressionRule};
use crate::encoding::{append_uvarint, checksum, put_u32};
use crate::error::{OndaError, Result};

/// Configuration for SSTable construction.
#[derive(Clone)]
pub struct WriterOptions {
    pub compression: Compression,
    /// Per-key-prefix compression overrides (longest prefix wins); keys
    /// matching no rule use `compression`. Applied per vlog value and per
    /// klog data block (blocks are cut at rule boundaries).
    pub compression_rules: Vec<CompressionRule>,
    pub cmp: ComparatorRef,
    pub enable_bloom: bool,
    /// False-positive rate for this table's filter, or `None` to write **no**
    /// filter block.
    ///
    /// Distinct from `enable_bloom`, which is the family-wide "never build
    /// filters" switch: this is the per-output-level decision
    /// (`ColumnFamilyConfig::bloom_fpr_for_level`). Either one produces a table
    /// a reader treats as "may contain" for every key.
    pub bloom_fpr: Option<f64>,
    pub klog_value_threshold: usize,
    pub block_size: usize,
    pub expected_entries: usize,
    /// Write a B+tree (hybrid klog) index instead of a flat single-level index.
    pub use_btree: bool,
    /// Entries per in-block restart point; at least 1. Epoch 1 writes a
    /// restart trailer on **every** data block, so `0` — 0.9's "no trailer" —
    /// is refused by [`Writer::new`]. See [`super::RESTART_INTERVAL`].
    pub restart_interval: usize,
    /// Write every data-block entry in the extended (kind-bearing) layout and
    /// declare `CAP_EXTENDED_RECORDS` in the footer's capability word.
    /// Table-level: the word describes the whole file, so this is fixed for
    /// the writer's lifetime.
    pub extended_entries: bool,
    /// Store each user key as `shared_len | suffix` against its predecessor
    /// (`CAP_PREFIX_DELTA` in the footer word). Table-level, like
    /// `extended_entries`, which it implies — the delta layout is defined only
    /// over the extended entry, so `finish` declares both.
    pub prefix_delta: bool,
}

/// Fan-out (entries per node) for the B+tree index.
const BTREE_FANOUT: usize = 256;

/// The stored (post-compression) payload length as it goes into a vlog frame
/// header, or [`OndaError::TooLarge`] when it does not fit.
///
/// The header field is a `u32` (see [`VLOG_FRAME_HDR_LEN`] and `docs/formats.md`),
/// so a 4 GiB payload written with an `as u32` cast would wrap to a small
/// length: the frame's CRC would then cover bytes the reader never reads, the
/// next frame's offset would point into this one's payload, and the table would
/// be silently corrupt from the moment it was written. Refusing the write is
/// the only outcome that keeps "every stored byte is checksummed" true.
///
/// The limit is on the *stored* bytes, not the caller's value: a value larger
/// than 4 GiB that compresses below the limit is representable and accepted.
fn vlog_stored_len(stored_len: usize) -> Result<u32> {
    u32::try_from(stored_len).map_err(|_| {
        OndaError::TooLarge(format!(
            "vlog frame payload is {stored_len} bytes; the frame header stores \
             it in a u32, so {} is the maximum",
            u32::MAX
        ))
    })
}

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
    /// Compression algorithm of the block being built (set from its first
    /// key's rule; `opts.compression` when no rule matches).
    cur_block_alg: Compression,
    /// Restart offsets (entry starts) of the block being built, one per
    /// `restart_interval` entries; empty when the trailer is disabled.
    cur_restarts: Vec<u32>,
    /// Previous entry's user key, for prefix-delta output. Cleared at every
    /// block start and every restart anchor, so sharing never crosses either.
    prev_key: Vec<u8>,
    /// Entries appended to the block being built.
    cur_entries: usize,
    index: Vec<IndexEntry>,
    /// A flushed block whose index separator is deferred until the next key is
    /// known: `(block_last_key, block_last_seq, handle)`. With the following
    /// block's first key in hand, the separator can be shortened (bytewise
    /// comparators only) instead of storing the full last key.
    pending_index: Option<(Vec<u8>, u64, BlockHandle)>,
    klog_off: u64,
    vlog_off: u64,

    /// Background-IO admission, or `None` when unlimited. Every byte this
    /// writer puts on the device is charged before the write is issued.
    limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>,

    /// One hash per key written, or `None` when the filter is disabled.
    ///
    /// The filter is built in [`finish`](Self::finish), not here, because a
    /// bloom filter's bit count and hash count are fixed at construction and
    /// cannot be resized — so sizing it up front means sizing it from a guess.
    /// Every such guess this writer was given turned out to be wrong:
    /// compaction passed a hardcoded 4,096 for tables holding a million
    /// entries, and bulk ingestion divided a byte target by an assumed 64-byte
    /// entry. An overloaded filter sets every bit and admits every key, which
    /// costs memory, a hash and a probe per lookup, and skips nothing.
    ///
    /// Buffering costs 8 bytes per entry until `finish`. It is bounded by the
    /// writer's roll target, not by the store: at the default 64 MiB target and
    /// the smallest entries a real consumer writes (~21 bytes), a full table is
    /// ~3.2M entries and the buffer peaks near 26 MB — one writer at a time,
    /// freed when the table closes. That is the price of a filter that works;
    /// the alternative measured 0 skips in 400,000 lookups.
    bloom_hashes: Option<Vec<u64>>,
    num_entries: u64,
    num_tombstones: u64,
    max_seq: u64,
    min_key: Option<Vec<u8>>,
    last_user_key: Vec<u8>,
    last_seq: u64,
    pending_block: bool,
    finished: bool,
    /// Range-tombstone fragments to write into this table's aux section (1.2),
    /// sorted by `start` and disjoint. Set once, before [`finish`](Self::finish).
    range_fragments: Vec<crate::range_tombstone::Fragment>,
    /// Whether any merge operand (kind 4) was written, so the footer's
    /// capability word can declare `CAP_MERGE_OPERANDS` for exactly the tables
    /// that carry one.
    wrote_merge: bool,
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
    /// Charge every byte this writer emits against `limiter`, under the writing
    /// thread's [`IoClass`](crate::ioctrl::IoClass).
    ///
    /// A builder rather than a [`WriterOptions`] field: the limiter is a
    /// property of the database that owns the writer, not of the table format,
    /// and every existing `WriterOptions` literal describes only the latter.
    pub fn with_limiter(mut self, limiter: Option<Arc<dyn crate::ioctrl::IoLimiter>>) -> Writer {
        self.limiter = limiter;
        self
    }

    /// Attach the range-tombstone fragments this table publishes (1.2).
    ///
    /// Fragments must be sorted by `start`, disjoint, and already clipped to
    /// the interval this output owns — clipping is the caller's job because
    /// only the flush or compaction job knows the output boundaries, and it is
    /// what keeps level->=1 span disjointness true (see `docs/formats.md`).
    ///
    /// Range fragments are defined over the kind-bearing entry, so
    /// [`WriterOptions::extended_entries`] (or the prefix-delta layout, which
    /// implies it) must be set; [`finish`](Self::finish) refuses the
    /// combination otherwise rather than silently dropping the fragments.
    pub fn set_range_fragments(&mut self, frags: Vec<crate::range_tombstone::Fragment>) {
        debug_assert!(
            frags.windows(2).all(|w| w[0].end <= w[1].start),
            "range fragments must be sorted and disjoint"
        );
        self.range_fragments = frags;
    }

    /// Fragments attached so far.
    pub fn range_fragments(&self) -> &[crate::range_tombstone::Fragment] {
        &self.range_fragments
    }

    pub fn new(klog_path: &str, mut opts: WriterOptions) -> Result<Writer> {
        if opts.restart_interval == 0 {
            return Err(OndaError::InvalidArgs(
                "restart_interval must be at least 1: every epoch-1 data block \
                 carries a restart trailer"
                    .into(),
            ));
        }
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
        // `expected_entries` is now only a capacity hint for the hash buffer —
        // getting it wrong costs a realloc, not a filter that admits
        // everything. Cap the pre-allocation so a wildly optimistic hint
        // cannot reserve hundreds of MB for a table that ends up small.
        // No rate means no filter block, so there is nothing to buffer hashes
        // for either — skipping the buffer is the whole memory saving.
        let bloom_hashes = if opts.enable_bloom && opts.bloom_fpr.is_some() {
            Some(Vec::with_capacity(
                opts.expected_entries.clamp(1024, 1 << 20),
            ))
        } else {
            None
        };
        let default_alg = opts.compression;
        Ok(Writer {
            klog_path: klog_path.to_string(),
            vlog_path: vlog_path_for(klog_path),
            klog: Some(BufWriter::with_capacity(256 << 10, f)),
            vlog: None,
            opts,
            cur_block: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            cur_block_alg: default_alg,
            cur_restarts: Vec::new(),
            prev_key: Vec::new(),
            cur_entries: 0,
            index: Vec::new(),
            pending_index: None,
            klog_off: 0,
            vlog_off: 0,
            limiter: None,
            bloom_hashes,
            num_entries: 0,
            num_tombstones: 0,
            max_seq: 0,
            min_key: None,
            last_user_key: Vec::new(),
            last_seq: 0,
            pending_block: false,
            range_fragments: Vec::new(),
            wrote_merge: false,
            finished: false,
        })
    }

    /// Emit the deferred index entry for the last flushed block, shortening
    /// its separator against `next_key` (the first key of the next block).
    /// The separator only has to satisfy `last_key <= sep < next_key`; a
    /// shortened separator keeps the resident index small and its
    /// binary-search comparisons cheap.
    fn settle_pending_index(&mut self, next_key: &[u8]) {
        if let Some((last_key, last_seq, handle)) = self.pending_index.take() {
            let sep = if self.opts.cmp.is_bytewise() {
                shortest_separator(&last_key, next_key)
            } else {
                last_key.clone()
            };
            // seq is only consulted on exact key equality (`cmp_internal`); a
            // strictly-greater synthetic separator never equals a stored key,
            // so 0 is inert. An unshortened separator IS the last key and
            // keeps its real seq, exactly as before.
            let seq = if sep == last_key { last_seq } else { 0 };
            self.index.push(IndexEntry {
                user_key: sep,
                seq,
                handle,
            });
        }
    }

    /// Entry layout this table's data blocks use, fixed for its lifetime — the
    /// footer flag describes the whole file.
    fn entry_layout(&self) -> EntryLayout {
        if self.extended() {
            EntryLayout::Extended
        } else {
            EntryLayout::Base
        }
    }

    /// Whether this table's entries carry the extended (kind-bearing) envelope
    /// — either asked for directly, or implied by prefix-delta output.
    #[inline]
    fn extended(&self) -> bool {
        self.opts.extended_entries || self.opts.prefix_delta
    }

    /// Bytes the restart trailer will add to the block being built.
    ///
    /// Block-size accounting includes it: the trailer rides inside the framed
    /// payload, so a block cut on the entries alone overshoots its target by
    /// `4 * R + 4`. Delta encoding raises entry density, and therefore `R`, so
    /// the overshoot grows exactly where blocks are meant to get denser.
    #[inline]
    fn pending_trailer_len(&self) -> usize {
        4 * self.cur_restarts.len() + 4
    }

    /// Append one entry. `value` is ignored for tombstones.
    ///
    /// `kind` is the record kind ([`KIND_PUT`](crate::format::KIND_PUT) and
    /// friends). A kind outside the three point kinds — 1.1's merge operand —
    /// exists only in the extended entry layout, so a writer that was not asked
    /// for extended entries refuses it rather than writing a byte stream that
    /// says something other than what the caller meant.
    pub fn add(
        &mut self,
        user_key: &[u8],
        value: &[u8],
        seq: u64,
        ttl: i64,
        kind: u64,
    ) -> Result<()> {
        crate::format::check_kind(kind)?;
        if !self.extended() && !crate::format::is_point_kind(kind) {
            return Err(OndaError::InvalidArgs(format!(
                "record kind {kind} needs the extended entry layout; this table \
                 was opened with extended_entries = false"
            )));
        }
        let tombstone =
            kind == crate::format::KIND_DELETE || kind == crate::format::KIND_SINGLE_DELETE;
        self.wrote_merge |= kind == crate::format::KIND_MERGE;
        self.settle_pending_index(user_key);
        // Per-key compression rule; also decides whether this key may share
        // the block being built.
        let alg = compression_for_key(&self.opts.compression_rules, user_key)
            .unwrap_or(self.opts.compression);
        if !self.cur_block.is_empty() && alg != self.cur_block_alg {
            // Rule boundary: cut the block so it stays single-algorithm, and
            // settle its separator against THIS key (a later key could sort
            // past it and misroute index lookups).
            self.flush_block()?;
            self.settle_pending_index(user_key);
        }
        if self.cur_block.is_empty() {
            self.cur_block_alg = alg;
        }
        if self.min_key.is_none() {
            self.min_key = Some(user_key.to_vec());
        }
        if let Some(h) = self.bloom_hashes.as_mut() {
            h.push(crate::bloom::hash_for_new(user_key));
        }

        let mut has_vlog = false;
        let mut vlog_off = 0u64;
        if !tombstone && value.len() >= self.opts.klog_value_threshold {
            vlog_off = self.write_vlog(value, alg)?;
            has_vlog = true;
        }

        let anchor = self.cur_entries.is_multiple_of(self.opts.restart_interval);
        if anchor {
            self.cur_restarts.push(self.cur_block.len() as u32);
            // An anchor is self-contained: reset the predecessor so its
            // `shared_len` is 0 and the entry decodes with no history.
            self.prev_key.clear();
        }
        self.cur_entries += 1;
        if self.opts.prefix_delta {
            encode_entry_delta(
                &mut self.cur_block,
                &self.prev_key,
                user_key,
                value,
                seq,
                ttl,
                kind,
                has_vlog,
                vlog_off,
            );
            self.prev_key.clear();
            self.prev_key.extend_from_slice(user_key);
        } else {
            let layout = self.entry_layout();
            encode_entry(
                &mut self.cur_block,
                layout,
                user_key,
                value,
                seq,
                ttl,
                kind,
                has_vlog,
                vlog_off,
            );
        }
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

        if self.cur_block.len() + self.pending_trailer_len() >= self.opts.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Append a value to the vlog as a frame
    /// `[crc32c u32 LE][codec u8][stored_len u32 LE][stored]` and return the
    /// frame's start offset. The payload is `value` compressed with `alg`,
    /// stored raw (`alg = None`) when compression would not shrink it. The
    /// crc covers the stored payload, matching the checksum coverage klog
    /// blocks already have.
    ///
    /// The file is created on the first large value with its 32-byte header
    /// ([`crate::format::vlog_header`]) ahead of the first frame; frame offsets
    /// are absolute, so every offset this returns is at least
    /// [`VLOG_HEADER_LEN`].
    fn write_vlog(&mut self, value: &[u8], alg: Compression) -> Result<u64> {
        if self.vlog.is_none() {
            let f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.vlog_path)?;
            let mut w = BufWriter::with_capacity(256 << 10, f);
            let header = encode_vlog_header();
            crate::ioctrl::charge(&self.limiter, header.len() as u64);
            w.write_all(&header)?;
            self.vlog = Some(w);
            self.vlog_off = VLOG_HEADER_LEN as u64;
        }
        let (used_alg, payload) = if alg == Compression::None {
            (Compression::None, None)
        } else {
            let c = do_compress(alg, value)?;
            if c.len() < value.len() {
                (alg, Some(c))
            } else {
                (Compression::None, None)
            }
        };
        let stored: &[u8] = payload.as_deref().unwrap_or(value);
        // Refuse before writing anything: a truncated length field would
        // corrupt this frame and every frame after it (see `vlog_stored_len`).
        let stored_len = vlog_stored_len(stored.len())?;
        let off = self.vlog_off;
        let mut hdr = [0u8; VLOG_FRAME_HDR_LEN];
        put_u32(&mut hdr[0..4], checksum(stored));
        hdr[4] = used_alg.codec_id();
        put_u32(&mut hdr[5..9], stored_len);
        // The frame, header included, is what reaches the device.
        crate::ioctrl::charge(
            &self.limiter,
            VLOG_FRAME_HDR_LEN as u64 + stored.len() as u64,
        );
        let w = self.vlog.as_mut().unwrap();
        w.write_all(&hdr)?;
        w.write_all(stored)?;
        self.vlog_off += VLOG_FRAME_HDR_LEN as u64 + stored.len() as u64;
        Ok(off)
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.cur_block.is_empty() {
            return Ok(());
        }
        // Trailer, on every block: restart offsets then their count, all
        // u32 LE. Readers find it from the count in the block's last 4 bytes.
        for i in 0..self.cur_restarts.len() {
            let mut b = [0u8; 4];
            put_u32(&mut b, self.cur_restarts[i]);
            self.cur_block.extend_from_slice(&b);
        }
        let mut b = [0u8; 4];
        put_u32(&mut b, self.cur_restarts.len() as u32);
        self.cur_block.extend_from_slice(&b);
        self.cur_restarts.clear();
        self.cur_entries = 0;
        // Sharing never crosses a block boundary: the next block's first entry
        // is an anchor, and its predecessor is nothing.
        self.prev_key.clear();
        let mut framed = Vec::new();
        let n = write_block(
            &mut framed,
            data_block_alg(self.cur_block_alg),
            &self.cur_block,
        )?;
        crate::ioctrl::charge(&self.limiter, n as u64);
        self.klog.as_mut().unwrap().write_all(&framed)?;
        // Defer the index entry: `add` shortens the separator once the next
        // block's first key is known; `finish` stores the full last key so the
        // reader's `max_key` stays exact.
        self.pending_index = Some((
            self.last_user_key.clone(),
            self.last_seq,
            BlockHandle {
                offset: self.klog_off,
                length: n as u64,
            },
        ));
        self.klog_off += n as u64;
        self.cur_block.clear();
        self.pending_block = false;
        Ok(())
    }

    fn write_meta_block(&mut self, payload: &[u8]) -> Result<BlockHandle> {
        let mut framed = Vec::new();
        let n = write_block(&mut framed, Compression::None, payload)?;
        crate::ioctrl::charge(&self.limiter, n as u64);
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
        if let Some((last_key, last_seq, handle)) = self.pending_index.take() {
            self.index.push(IndexEntry {
                user_key: last_key,
                seq: last_seq,
                handle,
            });
        }

        // The table's capability subset: what a reader must implement to
        // decode these bytes, declared in the footer rather than in flags.
        let mut caps = 0u64;
        if self.extended() {
            caps |= crate::format::CAP_EXTENDED_RECORDS;
        }
        if self.opts.prefix_delta {
            caps |= crate::format::CAP_PREFIX_DELTA;
        }
        if self.wrote_merge {
            caps |= crate::format::CAP_MERGE_OPERANDS;
        }
        let mut bloom_handle = None;
        // Size the filter from the keys actually written, not from a hint. Both
        // halves are matched together so there is no default rate to fall back
        // on: the buffer only exists when a rate was configured.
        if let (Some(hashes), Some(fpr)) = (self.bloom_hashes.take(), self.opts.bloom_fpr) {
            let mut b = Bloom::new(hashes.len().max(1), fpr);
            for h in hashes {
                b.add_hash(h);
            }
            let enc = b.encode();
            bloom_handle = Some(self.write_meta_block(&enc)?);
        }

        let mut aux_handle = None;
        if !self.range_fragments.is_empty() {
            if !self.extended() {
                return Err(OndaError::InvalidArgs(
                    "range fragments require an extended table: a range delete is a \
                     kind-bearing record"
                        .into(),
                ));
            }
            caps |= crate::format::CAP_RANGE_DELETES;
            let payload = crate::sst::encode_aux_sections(&[(
                crate::sst::AUX_SECTION_RANGE,
                crate::range_tombstone::encode_fragments(&self.range_fragments),
            )]);
            aux_handle = Some(self.write_meta_block(&payload)?);
        }

        let index_handle = if self.opts.use_btree {
            self.write_btree_index()?
        } else {
            let index_bytes = self.encode_index();
            self.write_meta_block(&index_bytes)?
        };

        let footer = encode_footer(&FooterFields {
            index: index_handle,
            bloom: bloom_handle,
            num_entries: self.num_entries,
            max_seq: self.max_seq,
            btree: self.opts.use_btree,
            caps,
            aux: aux_handle,
        });
        crate::ioctrl::charge(&self.limiter, footer.len() as u64);

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
        crate::util::sync_parent_dir(Path::new(&self.klog_path))?;

        self.finished = true;
        let range = crate::range_tombstone::summarize(&self.range_fragments);
        Ok(FileMeta {
            range_count: range.count,
            range_min_seq: range.min_seq,
            range_max_seq: range.max_seq,
            range_min_key: range.min_key,
            range_max_key: range.max_key,
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

/// Shortest bytewise separator `s` with `a <= s < b` (requires `a < b`).
/// Returns `a` verbatim when no shorter separator exists (`a` is a prefix of
/// `b`, or the diverging byte cannot be incremented under `b`).
pub(crate) fn shortest_separator(a: &[u8], b: &[u8]) -> Vec<u8> {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    if i >= n {
        return a.to_vec(); // a is a prefix of b (or equal)
    }
    if a[i] < 0xff && a[i] + 1 < b[i] {
        let mut s = a[..=i].to_vec();
        s[i] += 1; // a < s < b, length i+1
        return s;
    }
    // a[i]+1 == b[i]: s equals b's first i+1 bytes, so s < b only when b
    // extends past i; s > a because s[i] > a[i].
    if a[i] < 0xff && a[i] + 1 == b[i] && b.len() > i + 1 {
        let mut s = a[..=i].to_vec();
        s[i] += 1;
        return s;
    }
    a.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{BlockCache, FileCache};
    use crate::comparator::default_comparator;
    use crate::config::Compression;
    use crate::sst::Reader;
    use crate::storage::LocalStorage;
    use std::sync::Arc;

    #[test]
    #[cfg(target_pointer_width = "64")] // a 32-bit usize cannot exceed the field
    fn vlog_stored_len_refuses_above_u32() {
        // Exercised through the length check rather than a 4 GiB value: the
        // guard is factored out precisely so this costs nothing to test.
        assert_eq!(vlog_stored_len(0).unwrap(), 0);
        assert_eq!(
            vlog_stored_len(u32::MAX as usize).unwrap(),
            u32::MAX,
            "a payload of exactly u32::MAX still fits the header field"
        );
        let e = vlog_stored_len(u32::MAX as usize + 1).expect_err("must refuse");
        assert_eq!(e.kind(), "too_large", "got {e:?}");
        assert!(
            e.to_string().contains("4294967295"),
            "the error should name the limit: {e}"
        );
    }

    fn opts(restart_interval: usize, prefix_delta: bool, block_size: usize) -> WriterOptions {
        WriterOptions {
            compression: Compression::None,
            compression_rules: Vec::new(),
            cmp: default_comparator(),
            enable_bloom: false,
            bloom_fpr: None,
            klog_value_threshold: 1 << 20,
            block_size,
            expected_entries: 256,
            use_btree: false,
            restart_interval,
            extended_entries: false,
            prefix_delta,
        }
    }

    /// Every offset the restart array names must decode with `shared_len == 0`
    /// — that is what makes the anchor binary search work with no
    /// materialization, and it is decoder validation rule 2.
    #[test]
    fn delta_writer_emits_zero_shared_at_every_restart() {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("d.klog");
        let klog = klog.to_str().unwrap();
        let mut w = Writer::new(klog, opts(4, true, 512)).unwrap();
        for i in 0..200u64 {
            // Long shared prefix, so a non-anchor entry always shares bytes.
            let k = format!("tenant/alpha/cluster/{i:04}");
            w.add(k.as_bytes(), b"v", i + 1, 0, crate::format::KIND_PUT)
                .unwrap();
        }
        w.finish().unwrap();
        let r = Reader::open(
            klog,
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            3,
            default_comparator(),
            0,
        )
        .unwrap();
        assert!(r.index.len() > 3, "several blocks");
        let mut anchors = 0usize;
        let mut shared_entries = 0usize;
        for bi in 0..r.index.len() {
            let block = r.read_data_block_local(bi).unwrap();
            let (entries, restarts) = r.split_block(block.bytes()).unwrap();
            for chunk in restarts.chunks_exact(4) {
                let off = crate::encoding::read_u32(chunk) as usize;
                let (e, _) = crate::sst::decode_delta_header(entries, off).unwrap();
                assert_eq!(e.key_shared, 0, "restart anchor at {off} shares a prefix");
                anchors += 1;
            }
            // ...and the entries between anchors do share, or the encoding
            // would be doing nothing.
            let mut key = Vec::new();
            let mut off = 0usize;
            while off < entries.len() {
                let (e, next) =
                    crate::sst::decode_entry_delta(entries, off, &mut key, true).unwrap();
                if e.key_shared > 0 {
                    shared_entries += 1;
                }
                off = next;
            }
        }
        assert!(anchors >= 4, "expected several anchors, got {anchors}");
        assert!(
            shared_entries > 100,
            "prefix sharing did nothing: {shared_entries} entries shared"
        );
    }

    /// Every epoch-1 data block carries a restart trailer, so the 0.9 "no
    /// trailer" interval is refused for every layout.
    #[test]
    fn writer_refuses_zero_restart_interval() {
        let dir = tempfile::tempdir().unwrap();
        for delta in [false, true] {
            let klog = dir.path().join("no.klog");
            let err = Writer::new(klog.to_str().unwrap(), opts(0, delta, 512))
                .expect_err("a zero restart interval must be refused");
            assert_eq!(err.kind(), "invalid_args", "{err}");
            assert!(err.to_string().contains("restart_interval"), "{err}");
        }
    }

    /// The restart trailer rides inside the framed payload, so a block cut on
    /// the entries alone overshoots `block_size` by `4 * R + 4`.
    #[test]
    fn block_cut_accounts_for_the_restart_trailer() {
        let dir = tempfile::tempdir().unwrap();
        for (name, delta) in [("legacy.klog", false), ("delta.klog", true)] {
            let klog = dir.path().join(name);
            let klog = klog.to_str().unwrap();
            let mut w = Writer::new(klog, opts(4, delta, 1024)).unwrap();
            for i in 0..400u64 {
                let k = format!("tenant/alpha/{i:04}");
                w.add(
                    k.as_bytes(),
                    b"payload-payload",
                    i + 1,
                    0,
                    crate::format::KIND_PUT,
                )
                .unwrap();
            }
            w.finish().unwrap();
            let r = Reader::open(
                klog,
                LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
                Arc::new(BlockCache::new(1 << 20)),
                4,
                default_comparator(),
                0,
            )
            .unwrap();
            assert!(r.index.len() > 4, "{name}: several blocks");
            for bi in 0..r.index.len() - 1 {
                let block = r.read_data_block_local(bi).unwrap();
                let raw = block.bytes();
                // Entries + trailer, i.e. the whole decompressed block, is what
                // the cut is measured against — the last block is exempt.
                assert!(
                    raw.len() < 1024 + 64,
                    "{name}: block {bi} is {} bytes, past its 1024-byte target \
                     plus one entry",
                    raw.len()
                );
                let (entries, restarts) = r.split_block(raw).unwrap();
                assert!(
                    entries.len() + restarts.len() + 4 >= 1024,
                    "{name}: block {bi} was cut early"
                );
            }
        }
    }

    #[test]
    fn separator_properties() {
        // Deterministic cases.
        assert_eq!(shortest_separator(b"abcXYZ", b"abd000"), b"abd".to_vec());
        assert_eq!(shortest_separator(b"abc", b"abcd"), b"abc".to_vec()); // prefix
        assert_eq!(
            shortest_separator(&[0xff, 0xff], &[0xff, 0xff, 0x01]),
            vec![0xff, 0xff]
        ); // increment overflow -> unshortened
           // a[i]+1 == b[i] with b extending past i -> can shorten.
        assert_eq!(shortest_separator(b"aa", b"ab0"), b"ab".to_vec());
        // a[i]+1 == b[i] with b NOT extending -> cannot (s would equal b).
        assert_eq!(shortest_separator(b"aa", b"ab"), b"aa".to_vec());

        // Property check over pseudo-random pairs: a <= s < b, or s == a.
        let mut state = 0x9e3779b97f4a7c15u64;
        let mut rnd = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..10_000 {
            let la = (rnd() % 12 + 1) as usize;
            let lb = (rnd() % 12 + 1) as usize;
            let a: Vec<u8> = (0..la).map(|_| (rnd() % 6) as u8 + b'a').collect();
            let b: Vec<u8> = (0..lb).map(|_| (rnd() % 6) as u8 + b'a').collect();
            let (a, b) = if a < b {
                (a, b)
            } else if b < a {
                (b, a)
            } else {
                continue;
            };
            let s = shortest_separator(&a, &b);
            assert!(a.as_slice() <= s.as_slice(), "a={a:?} b={b:?} s={s:?}");
            assert!(s.as_slice() < b.as_slice(), "a={a:?} b={b:?} s={s:?}");
            assert!(s.len() <= a.len().max(1), "separator longer than a");
        }
    }

    #[test]
    fn large_key_index_shrinks() {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("big.klog");
        let klog = klog.to_str().unwrap();
        let n = 500usize;
        let mut w = Writer::new(
            klog,
            WriterOptions {
                compression: Compression::None,
                compression_rules: Vec::new(),
                cmp: default_comparator(),
                enable_bloom: false,
                bloom_fpr: Some(0.01),
                klog_value_threshold: 1 << 20, // keep values inline
                block_size: 4 << 10,
                expected_entries: n,
                use_btree: false,
                restart_interval: 8,
                extended_entries: false,
                prefix_delta: false,
            },
        )
        .unwrap();
        let val = vec![b'v'; 100];
        for i in 0..n {
            // 2 KiB keys: 16-byte ordered prefix + 2032 bytes of padding.
            let mut k = format!("{i:016}").into_bytes();
            k.resize(2048, b'x');
            w.add(&k, &val, (i + 1) as u64, 0, crate::format::KIND_PUT)
                .unwrap();
        }
        w.finish().unwrap();
        let r = Reader::open(
            klog,
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            7,
            default_comparator(),
            0,
        )
        .unwrap();
        assert!(r.index.len() > 100, "expected many blocks with 2 KiB keys");
        let total_sep_bytes: usize = r.index.iter().map(|e| e.user_key.len()).sum();
        let full = r.index.len() * 2048;
        // All but the final separator shorten to ~17 bytes (the diverging
        // digit position + 1); the last block keeps its full 2 KiB key.
        assert!(
            total_sep_bytes < full / 10,
            "index not shortened: {total_sep_bytes} of {full} bytes"
        );
        // max_key must remain the exact full last key.
        assert_eq!(r.max_key().len(), 2048);
    }
}
