//! Bidirectional SSTable iterator.
//!
//! Forward iteration decodes each entry once, extending a compact offset index
//! as it goes (so `prev` within a block is O(1)).  Seeks and entering a block
//! while moving backward build the full block index up front.  The iterator owns
//! an `Arc<Reader>` and copies block bytes out via the block cache, so it has no
//! borrow ties to the reader's internals.

use std::sync::Arc;

use super::{cmp_internal, decode_entry, Block, DecEntry, Reader};
use crate::error::Result;

/// Iterates an SSTable's entries in internal order (user key ascending, sequence
/// descending).
#[derive(Debug)]
pub struct SstIterator {
    r: Arc<Reader>,
    block_idx: i64,
    raw: Option<Block>,
    offsets: Vec<u32>,
    pos: i64,
    cur: Option<DecEntry>,
    /// Zero-padded big-endian first 8 bytes of the current user key, cached at
    /// decode time so merge comparisons can usually skip the key slice.
    cur_pfx: u64,
    cur_next: usize,
    valid: bool,
    err: Option<crate::error::OndaError>,
}

/// Zero-padded big-endian packing of a key's first 8 bytes (see
/// `memtable_arena::key_prefix` for the ordering argument).
#[inline]
pub(crate) fn key_prefix8(user_key: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    let n = user_key.len().min(8);
    b[..n].copy_from_slice(&user_key[..n]);
    u64::from_be_bytes(b)
}

impl SstIterator {
    pub(crate) fn new(r: Arc<Reader>) -> SstIterator {
        SstIterator {
            r,
            block_idx: -1,
            raw: None,
            offsets: Vec::new(),
            pos: -1,
            cur: None,
            cur_pfx: 0,
            cur_next: 0,
            valid: false,
            err: None,
        }
    }

    fn num_blocks(&self) -> usize {
        self.r.index.len()
    }

    /// Load block `i`. When `full`, decode all entry offsets up front.
    fn load_block(&mut self, i: i64, full: bool) -> bool {
        if i < 0 || i as usize >= self.num_blocks() {
            self.raw = None;
            self.offsets.clear();
            self.valid = false;
            return false;
        }
        let raw = match self.r.read_data_block(i as usize) {
            Ok(r) => r,
            Err(e) => {
                self.err = Some(e);
                self.raw = None;
                self.valid = false;
                return false;
            }
        };
        self.block_idx = i;
        self.offsets.clear();
        if full {
            let bytes = raw.bytes();
            let mut off = 0usize;
            while off < bytes.len() {
                match decode_entry(bytes, off) {
                    Ok((_, next)) => {
                        self.offsets.push(off as u32);
                        off = next;
                    }
                    Err(e) => {
                        self.err = Some(e);
                        self.raw = Some(raw);
                        return false;
                    }
                }
            }
        }
        self.raw = Some(raw);
        true
    }

    fn decode_at(&mut self, off: usize) {
        let raw = self.raw.as_ref().unwrap().bytes();
        match decode_entry(raw, off) {
            Ok((e, next)) => {
                self.cur_pfx = key_prefix8(e.user_key(raw));
                self.cur = Some(e);
                self.cur_next = next;
                self.valid = true;
            }
            Err(e) => {
                self.err = Some(e);
                self.valid = false;
            }
        }
    }

    /// Cached zero-padded 8-byte prefix of the current user key.
    #[inline]
    pub(crate) fn key_prefix(&self) -> u64 {
        self.cur_pfx
    }

    pub fn valid(&self) -> bool {
        self.valid && self.err.is_none()
    }

    pub fn err(&self) -> Option<&crate::error::OndaError> {
        self.err.as_ref()
    }

    pub fn seek_to_first(&mut self) {
        if !self.load_block(0, false) {
            return;
        }
        self.offsets.clear();
        self.offsets.push(0);
        self.pos = 0;
        self.decode_at(0);
    }

    pub fn seek_to_last(&mut self) {
        let last = self.num_blocks() as i64 - 1;
        if !self.load_block(last, true) {
            return;
        }
        if self.offsets.is_empty() {
            self.valid = false;
            return;
        }
        self.pos = self.offsets.len() as i64 - 1;
        let off = self.offsets[self.pos as usize] as usize;
        self.decode_at(off);
    }

    /// Position on the first entry with `(user_key, seq) >=` the target.
    pub fn seek(&mut self, user_key: &[u8], seq: u64) {
        let bi = self.r.find_block(user_key, seq) as i64;
        if bi as usize >= self.num_blocks() {
            self.valid = false;
            return;
        }
        if !self.load_block(bi, true) {
            return;
        }
        let cmp = self.r.comparator().clone();
        let raw = self.raw.as_ref().unwrap().clone();
        let bytes = raw.bytes();
        let (mut lo, mut hi) = (0usize, self.offsets.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (e, _) = decode_entry(bytes, self.offsets[mid] as usize).unwrap();
            if cmp_internal(&cmp, e.user_key(bytes), e.seq, user_key, seq).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= self.offsets.len() {
            if self.load_block(bi + 1, false) {
                self.offsets.clear();
                self.offsets.push(0);
                self.pos = 0;
                self.decode_at(0);
            } else {
                self.valid = false;
            }
            return;
        }
        self.pos = lo as i64;
        let off = self.offsets[lo] as usize;
        self.decode_at(off);
    }

    /// Position on the last entry with `(user_key, seq) <=` the target.
    pub fn seek_for_prev(&mut self, user_key: &[u8], seq: u64) {
        self.seek(user_key, seq);
        if !self.valid() {
            self.seek_to_last();
            return;
        }
        let cmp = self.r.comparator().clone();
        let raw = self.raw.as_ref().unwrap().clone();
        let cur = self.cur.unwrap();
        if cmp_internal(&cmp, cur.user_key(raw.bytes()), cur.seq, user_key, seq).is_gt() {
            self.prev();
        }
    }

    pub fn next(&mut self) {
        if !self.valid {
            return;
        }
        if (self.pos + 1) < self.offsets.len() as i64 {
            self.pos += 1;
            let off = self.offsets[self.pos as usize] as usize;
            self.decode_at(off);
            return;
        }
        let raw_len = self.raw.as_ref().map(|r| r.len()).unwrap_or(0);
        if self.cur_next >= raw_len {
            if self.load_block(self.block_idx + 1, false) {
                self.offsets.clear();
                self.offsets.push(0);
                self.pos = 0;
                self.decode_at(0);
            } else {
                self.valid = false;
            }
            return;
        }
        self.offsets.push(self.cur_next as u32);
        self.pos += 1;
        let off = self.cur_next;
        self.decode_at(off);
    }

    pub fn prev(&mut self) {
        if self.err.is_some() || self.raw.is_none() {
            return;
        }
        if self.pos > 0 {
            self.pos -= 1;
            let off = self.offsets[self.pos as usize] as usize;
            self.decode_at(off);
            return;
        }
        if self.block_idx >= 1 && self.load_block(self.block_idx - 1, true) {
            if self.offsets.is_empty() {
                self.valid = false;
                return;
            }
            self.pos = self.offsets.len() as i64 - 1;
            let off = self.offsets[self.pos as usize] as usize;
            self.decode_at(off);
            return;
        }
        self.valid = false;
    }

    fn raw(&self) -> &[u8] {
        self.raw.as_ref().unwrap().bytes()
    }

    pub fn user_key(&self) -> &[u8] {
        self.cur.unwrap().user_key(self.raw())
    }
    pub fn seq(&self) -> u64 {
        self.cur.unwrap().seq
    }
    pub fn ttl(&self) -> i64 {
        self.cur.unwrap().ttl
    }
    pub fn is_tombstone(&self) -> bool {
        self.cur.unwrap().tombstone()
    }
    pub fn is_single_delete(&self) -> bool {
        self.cur.unwrap().single_delete()
    }

    /// The current entry's value, reading from the vlog if necessary.
    pub fn value(&self) -> Result<Vec<u8>> {
        let e = self.cur.unwrap();
        if e.has_vlog() {
            self.r.read_vlog(e.vlog_off, e.val_len as u64)
        } else {
            Ok(e.inline_value(self.raw()).to_vec())
        }
    }

    /// Borrowed handle to the current inline value: the retained data block plus
    /// the value's `(start, len)` within it. `None` for vlog-separated values,
    /// which must be read from the vlog file.
    #[inline]
    pub(crate) fn value_block_ref(&self) -> Option<(&Block, usize, usize)> {
        let e = self.cur?;
        if e.has_vlog() {
            return None;
        }
        let block = self.raw.as_ref()?;
        Some((block, e.val_start, e.val_len))
    }

    /// Borrowed handle to the current user key: the retained data block plus the
    /// key's `(start, len)` within it.
    #[inline]
    pub(crate) fn key_block_ref(&self) -> Option<(&Block, usize, usize)> {
        let e = self.cur?;
        let block = self.raw.as_ref()?;
        Some((block, e.key_start, e.key_len))
    }

    /// Append the current value to `out` without an intermediate allocation.
    /// Inline values are copied straight from the cached block; vlog values are
    /// read directly into `out`.
    pub fn value_into(&self, out: &mut Vec<u8>) -> Result<()> {
        let e = self.cur.unwrap();
        if e.has_vlog() {
            self.r.read_vlog_into(e.vlog_off, e.val_len as u64, out)
        } else {
            out.extend_from_slice(e.inline_value(self.raw()));
            Ok(())
        }
    }
}
