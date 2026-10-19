//! Bidirectional SSTable iterator.
//!
//! Forward iteration decodes each entry once, extending a compact offset index
//! as it goes (so `prev` within a block is O(1)).  Seeks and entering a block
//! while moving backward build the full block index up front.  The iterator owns
//! an `Arc<Reader>` and copies block bytes out via the block cache, so it has no
//! borrow ties to the reader's internals.
//!
//! Prefix-delta tables ([`FOOTER_PREFIX_DELTA`](super::FOOTER_PREFIX_DELTA))
//! take a parallel set of paths. An entry offset alone cannot reconstruct a
//! delta key, so the offsets vector is useless there; positioning enters
//! through the restart array instead — binary search over the (self-contained)
//! anchors, then one materialized *run* held in [`RunCursor`]. Forward stepping
//! never touches the cursor: an anchor's `shared_len` is 0, so a running
//! previous-key buffer reconstructs every key with no run bookkeeping in the
//! decode itself.

use std::sync::Arc;

use super::{
    cmp_internal, decode_delta_anchor, decode_delta_header, decode_entry, decode_entry_delta,
    restart_lower_bound, Block, DecEntry, Reader,
};
use crate::error::Result;

/// Ceiling on the reconstructed keys one [`RunCursor`] holds. A run whose keys
/// exceed it is not an error: the cursor keeps the entry offsets and re-decodes
/// keys from the anchor on demand, which costs O(restart_interval) per
/// positioning step instead of growing an unbounded arena.
const MIN_RUN_ARENA_BYTES: usize = 64 << 10;

/// Iterates an SSTable's entries in internal order (user key ascending, sequence
/// descending).
#[derive(Debug)]
pub struct SstIterator {
    r: Arc<Reader>,
    block_idx: i64,
    raw: Option<Block>,
    /// Length of the current block's entries region (excludes the restart
    /// trailer on files that have one). Every walk is bounded by this, never
    /// by the raw block length.
    entries_len: usize,
    offsets: Vec<u32>,
    pos: i64,
    cur: Option<DecEntry>,
    /// Zero-padded big-endian first 8 bytes of the current user key, cached at
    /// decode time so merge comparisons can usually skip the key slice.
    cur_pfx: u64,
    cur_next: usize,
    valid: bool,
    err: Option<crate::error::OndaError>,

    // ---- prefix-delta state; all unused for a legacy table ----------------
    /// This table's entries are prefix-delta encoded (cached from the reader so
    /// the hot paths do not chase the `Arc` on every entry).
    delta: bool,
    /// This table's comparator orders byte-wise, enabling the delta decoder's
    /// in-block order check.
    bytewise: bool,
    /// Reconstructed current user key. A delta key exists contiguously nowhere
    /// in the block, so this buffer — not a block slice — is what
    /// [`user_key`](Self::user_key) serves.
    key_buf: Vec<u8>,
    /// The current block's restart offsets, decoded once per block load.
    restarts: Vec<u32>,
    /// Restart index of the run holding the current entry.
    run_idx: usize,
    /// Position of the current entry within that run.
    run_pos: usize,
    /// The one materialized run, reused across seeks and blocks.
    run: RunCursor,
}

/// One materialized restart run of a prefix-delta block: the entry offsets and
/// sequences (always), plus the reconstructed keys (unless the run exceeded the
/// arena bound).
///
/// Buffers are cleared and reused, never reallocated per entry — the same rule
/// that keeps the pinned-block path cheap (AGENTS.md invariant 8).
#[derive(Debug, Default)]
struct RunCursor {
    /// `(block index, restart index)` this cursor holds, or `None` when empty.
    at: Option<(i64, usize)>,
    /// Entry offsets of the run, in order.
    starts: Vec<u32>,
    /// Sequence per entry, so an in-run search can order `(user_key, seq)`
    /// without re-decoding.
    seqs: Vec<u64>,
    /// Reconstructed keys, back to back; empty when [`Self::overflowed`].
    keys: Vec<u8>,
    /// End offset in `keys` of each entry's key; empty when overflowed.
    key_ends: Vec<u32>,
    /// Running key used while materializing (and while re-decoding after an
    /// overflow).
    scratch: Vec<u8>,
    /// The run's keys exceeded the arena bound, so they are re-decoded from the
    /// anchor on demand rather than held.
    overflowed: bool,
    /// Times the arena bound forced that fallback. A test hook: the fallback is
    /// a correctness path that a size-based test would otherwise only be able
    /// to assert indirectly.
    overflows: u64,
}

impl RunCursor {
    fn reset(&mut self) {
        self.at = None;
        self.starts.clear();
        self.seqs.clear();
        self.keys.clear();
        self.key_ends.clear();
        self.overflowed = false;
    }

    fn len(&self) -> usize {
        self.starts.len()
    }

    /// `(start, end)` of entry `i`'s key within [`Self::keys`], or `None` when
    /// the run overflowed and holds no keys.
    fn key_span(&self, i: usize) -> Option<(usize, usize)> {
        if self.overflowed {
            return None;
        }
        let end = *self.key_ends.get(i)? as usize;
        let start = if i == 0 {
            0
        } else {
            self.key_ends[i - 1] as usize
        };
        Some((start, end))
    }

    /// Decode run `run` of `entries` — the half-open byte range `start..end` —
    /// into this cursor.
    ///
    /// `end` is the next anchor's offset, or the end of the entries region for
    /// the last run. The walk must land on it exactly: that is decoder
    /// validation rule 5 for the final run (no bytes between the last entry and
    /// the trailer) and an entry-boundary check on every anchor before it.
    #[allow(clippy::too_many_arguments)]
    fn materialize(
        &mut self,
        block: i64,
        run: usize,
        entries: &[u8],
        start: usize,
        end: usize,
        bound: usize,
        bytewise: bool,
    ) -> Result<()> {
        self.reset();
        self.scratch.clear();
        if start < end {
            // Rule 2: every offset the restart array names is self-contained.
            decode_delta_anchor(entries, start)?;
        }
        let mut off = start;
        while off < end {
            let (entry, next) = decode_entry_delta(entries, off, &mut self.scratch, bytewise)?;
            self.starts.push(off as u32);
            self.seqs.push(entry.seq);
            if !self.overflowed {
                if self.keys.len() + self.scratch.len() > bound {
                    self.overflowed = true;
                    self.overflows += 1;
                    self.keys.clear();
                    self.key_ends.clear();
                } else {
                    self.keys.extend_from_slice(&self.scratch);
                    self.key_ends.push(self.keys.len() as u32);
                }
            }
            off = next;
        }
        if off != end {
            return Err(crate::error::OndaError::Corruption(
                "sst: delta entry crosses a restart or block boundary".into(),
            ));
        }
        self.at = Some((block, run));
        Ok(())
    }
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
        let delta = r.prefix_delta();
        let bytewise = r.bytewise();
        SstIterator {
            r,
            block_idx: -1,
            raw: None,
            entries_len: 0,
            offsets: Vec::new(),
            pos: -1,
            cur: None,
            cur_pfx: 0,
            cur_next: 0,
            valid: false,
            err: None,
            delta,
            bytewise,
            key_buf: Vec::new(),
            restarts: Vec::new(),
            run_idx: 0,
            run_pos: 0,
            run: RunCursor::default(),
        }
    }

    fn num_blocks(&self) -> usize {
        self.r.index.len()
    }

    /// Times a materialized run exceeded the arena bound and fell back to
    /// re-decoding from its anchor. Test hook (see [`RunCursor::overflows`]).
    #[cfg(test)]
    pub(crate) fn run_arena_overflows(&self) -> u64 {
        self.run.overflows
    }

    /// Load block `i`. When `full`, decode all entry offsets up front — legacy
    /// blocks only: an offset cannot reconstruct a delta key, so a delta block
    /// builds its restart array instead and positions through [`RunCursor`].
    fn load_block(&mut self, i: i64, full: bool) -> bool {
        if i < 0 || i as usize >= self.num_blocks() {
            self.raw = None;
            self.offsets.clear();
            self.restarts.clear();
            self.run.reset();
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
        self.restarts.clear();
        self.entries_len = match self.r.split_block(raw.bytes()) {
            Ok((entries, restarts)) => {
                if self.delta {
                    self.restarts.reserve(restarts.len() / 4);
                    for c in restarts.chunks_exact(4) {
                        self.restarts.push(crate::encoding::read_u32(c));
                    }
                }
                entries.len()
            }
            Err(e) => {
                self.err = Some(e);
                self.raw = None;
                self.valid = false;
                return false;
            }
        };
        self.block_idx = i;
        self.offsets.clear();
        self.run.reset();
        if full && !self.delta {
            let bytes = raw.bytes();
            let mut off = 0usize;
            while off < self.entries_len {
                match decode_entry(bytes, self.r.entry_layout(), off) {
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
        match decode_entry(raw, self.r.entry_layout(), off) {
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
        self.enter_block_start();
    }

    /// Position on the first entry of the block just loaded.
    fn enter_block_start(&mut self) {
        if self.delta {
            self.run_idx = 0;
            self.run_pos = 0;
            self.key_buf.clear();
            self.step_delta(0);
            return;
        }
        self.offsets.clear();
        self.offsets.push(0);
        self.pos = 0;
        self.decode_at(0);
    }

    pub fn seek_to_last(&mut self) {
        let last = self.num_blocks() as i64 - 1;
        if self.delta {
            if !self.load_block(last, false) {
                return;
            }
            self.enter_block_end();
            return;
        }
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

    /// Position on the last entry of the delta block just loaded.
    fn enter_block_end(&mut self) {
        let Some(last_run) = self.restarts.len().checked_sub(1) else {
            self.valid = false;
            return;
        };
        if !self.ensure_run(last_run) {
            return;
        }
        let Some(last) = self.run.len().checked_sub(1) else {
            self.valid = false;
            return;
        };
        self.position_in_run(last_run, last);
    }

    /// Position on the first entry with `(user_key, seq) >=` the target.
    pub fn seek(&mut self, user_key: &[u8], seq: u64) {
        let bi = self.r.find_block(user_key, seq) as i64;
        if bi as usize >= self.num_blocks() {
            self.valid = false;
            return;
        }
        if self.delta {
            self.seek_delta(bi, user_key, seq);
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
            let (e, _) = decode_entry(bytes, self.r.entry_layout(), self.offsets[mid] as usize)
                .expect("load_block already decoded every offset in this block");
            if cmp_internal(&cmp, e.user_key(bytes), e.seq, user_key, seq).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= self.offsets.len() {
            if self.load_block(bi + 1, false) {
                self.enter_block_start();
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
        let cur_seq = self.cur.unwrap().seq;
        let past = {
            let key = self.user_key();
            cmp_internal(&cmp, key, cur_seq, user_key, seq).is_gt()
        };
        if past {
            self.prev();
        }
    }

    pub fn next(&mut self) {
        if !self.valid {
            return;
        }
        if self.delta {
            self.next_delta();
            return;
        }
        if (self.pos + 1) < self.offsets.len() as i64 {
            self.pos += 1;
            let off = self.offsets[self.pos as usize] as usize;
            self.decode_at(off);
            return;
        }
        if self.cur_next >= self.entries_len {
            if self.load_block(self.block_idx + 1, false) {
                self.enter_block_start();
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
        if self.delta {
            self.prev_delta();
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

    // ---- prefix-delta paths ----------------------------------------------

    /// Byte range of restart run `r` within the current block's entries region.
    fn run_range(&self, r: usize) -> (usize, usize) {
        let start = self.restarts[r] as usize;
        let end = self
            .restarts
            .get(r + 1)
            .map(|&o| o as usize)
            .unwrap_or(self.entries_len);
        (start, end)
    }

    /// Materialize run `r` of the current block unless the cursor already
    /// holds it. `false` means the error was recorded and the walk is over.
    fn ensure_run(&mut self, r: usize) -> bool {
        if self.run.at == Some((self.block_idx, r)) {
            return true;
        }
        if r >= self.restarts.len() {
            self.valid = false;
            return false;
        }
        let (start, end) = self.run_range(r);
        // The bound scales with the block: `WriterOptions::block_size` is not
        // persisted, but the decompressed block IS its realized value, so twice
        // the entries region is the same "2 x block_size" ceiling expressed in
        // something the reader can see.
        let bound = MIN_RUN_ARENA_BYTES.max(2 * self.entries_len);
        let block = self.block_idx;
        let bytewise = self.bytewise;
        let entries = &self.raw.as_ref().unwrap().bytes()[..self.entries_len];
        match self
            .run
            .materialize(block, r, entries, start, end, bound, bytewise)
        {
            Ok(()) => true,
            Err(e) => {
                self.err = Some(e);
                self.valid = false;
                false
            }
        }
    }

    /// Decode the delta entry at `off` against whatever `key_buf` currently
    /// holds, publishing it as the current entry.
    fn step_delta(&mut self, off: usize) {
        let entries = &self.raw.as_ref().unwrap().bytes()[..self.entries_len];
        match decode_entry_delta(entries, off, &mut self.key_buf, self.bytewise) {
            Ok((e, next)) => {
                self.cur_pfx = key_prefix8(&self.key_buf);
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

    /// Position on entry `i` of run `r`, which [`ensure_run`](Self::ensure_run)
    /// has already materialized.
    fn position_in_run(&mut self, r: usize, i: usize) {
        let Some(&off) = self.run.starts.get(i) else {
            self.valid = false;
            return;
        };
        let off = off as usize;
        match self.run.key_span(i) {
            Some((ks, ke)) => {
                // Disjoint fields: the arena is read while the key buffer is
                // written, so neither borrow touches the other.
                self.key_buf.clear();
                self.key_buf.extend_from_slice(&self.run.keys[ks..ke]);
                let entries = &self.raw.as_ref().unwrap().bytes()[..self.entries_len];
                match decode_delta_header(entries, off) {
                    Ok((e, next)) => {
                        self.cur_pfx = key_prefix8(&self.key_buf);
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
            None => {
                // Arena-bound fallback: walk the run from its anchor. O(i)
                // reconstruction memcpys, no allocation beyond the key itself.
                self.key_buf.clear();
                let start = self.run.starts[0] as usize;
                let mut at = start;
                for _ in 0..=i {
                    self.step_delta(at);
                    if !self.valid {
                        return;
                    }
                    at = self.cur_next;
                }
            }
        }
        self.run_idx = r;
        self.run_pos = i;
    }

    /// First entry of run `r` that sorts `>= (user_key, seq)`, or `None` when
    /// every entry of the run sorts before it.
    fn find_in_run(&mut self, r: usize, user_key: &[u8], seq: u64) -> Option<usize> {
        debug_assert_eq!(
            self.run.at,
            Some((self.block_idx, r)),
            "run not materialized"
        );
        let cmp = self.r.comparator().clone();
        let n = self.run.len();
        if self.run.overflowed {
            // No arena to search: walk the run once from its anchor, comparing
            // as we go. Linear, not binary — the fallback trades the search for
            // not holding the keys.
            self.key_buf.clear();
            let mut at = self.run.starts[0] as usize;
            for i in 0..n {
                self.step_delta(at);
                if !self.valid {
                    return None;
                }
                if !cmp_internal(&cmp, &self.key_buf, self.cur.unwrap().seq, user_key, seq).is_lt()
                {
                    return Some(i);
                }
                at = self.cur_next;
            }
            return None;
        }
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (ks, ke) = self.run.key_span(mid).expect("materialized run");
            if cmp_internal(
                &cmp,
                &self.run.keys[ks..ke],
                self.run.seqs[mid],
                user_key,
                seq,
            )
            .is_lt()
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (lo < n).then_some(lo)
    }

    fn seek_delta(&mut self, bi: i64, user_key: &[u8], seq: u64) {
        if !self.load_block(bi, false) {
            return;
        }
        let cmp = self.r.comparator().clone();
        let count = self.restarts.len();
        // Anchors are self-contained, so this search materializes nothing.
        let lower = {
            let entries = &self.raw.as_ref().unwrap().bytes()[..self.entries_len];
            let restarts = &self.restarts;
            restart_lower_bound(count, |i| {
                let (e, _) = decode_delta_anchor(entries, restarts[i] as usize)?;
                Ok(cmp_internal(&cmp, e.user_key(entries), e.seq, user_key, seq).is_lt())
            })
        };
        let lower = match lower {
            Ok(l) => l,
            Err(e) => {
                self.err = Some(e);
                self.valid = false;
                return;
            }
        };
        let r = lower.saturating_sub(1);
        if count == 0 {
            self.valid = false;
            return;
        }
        if !self.ensure_run(r) {
            return;
        }
        if let Some(i) = self.find_in_run(r, user_key, seq) {
            self.position_in_run(r, i);
            return;
        }
        if self.err.is_some() {
            return;
        }
        // Past this run: the next anchor is >= the target by construction, so
        // the answer is its first entry — or the next block.
        if r + 1 < count {
            if self.ensure_run(r + 1) {
                self.position_in_run(r + 1, 0);
            }
            return;
        }
        if self.load_block(bi + 1, false) {
            self.enter_block_start();
        } else {
            self.valid = false;
        }
    }

    fn next_delta(&mut self) {
        if self.cur_next >= self.entries_len {
            if self.load_block(self.block_idx + 1, false) {
                self.enter_block_start();
            } else {
                self.valid = false;
            }
            return;
        }
        let off = self.cur_next;
        // An entry starting exactly at the next anchor's offset opens the next
        // run. The decode itself needs no special case: the anchor's
        // `shared_len` is 0, so the running key is truncated away for free.
        let (r, i) = match self.restarts.get(self.run_idx + 1) {
            Some(&anchor) if anchor as usize == off => (self.run_idx + 1, 0),
            _ => (self.run_idx, self.run_pos + 1),
        };
        self.step_delta(off);
        if self.valid {
            self.run_idx = r;
            self.run_pos = i;
        }
    }

    fn prev_delta(&mut self) {
        if self.run_pos > 0 {
            let (r, i) = (self.run_idx, self.run_pos - 1);
            if self.ensure_run(r) {
                self.position_in_run(r, i);
            }
            return;
        }
        if self.run_idx > 0 {
            let r = self.run_idx - 1;
            if !self.ensure_run(r) {
                return;
            }
            match self.run.len().checked_sub(1) {
                Some(last) => self.position_in_run(r, last),
                None => self.valid = false,
            }
            return;
        }
        if self.block_idx >= 1 && self.load_block(self.block_idx - 1, false) {
            self.enter_block_end();
            return;
        }
        self.valid = false;
    }

    // ----------------------------------------------------------------------

    fn raw(&self) -> &[u8] {
        self.raw.as_ref().unwrap().bytes()
    }

    pub fn user_key(&self) -> &[u8] {
        if self.delta {
            return &self.key_buf;
        }
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
    /// Record kind of the current entry; see [`crate::wal::Record::kind`].
    #[inline]
    pub fn kind(&self) -> u64 {
        u64::from(self.cur.unwrap().kind)
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
    ///
    /// Unaffected by prefix-delta encoding: only keys are split, so an inline
    /// value is still one contiguous run of block bytes.
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
    ///
    /// `None` for a prefix-delta table — a delta key exists contiguously
    /// nowhere in the block, so there is nothing to borrow and pinning the
    /// block would hand out a slice of the wrong bytes (AGENTS.md invariant 8).
    /// The merge iterator's existing `None` branch copies `user_key()` into its
    /// own buffer, exactly as it does for memtable children.
    #[inline]
    pub(crate) fn key_block_ref(&self) -> Option<(&Block, usize, usize)> {
        if self.delta {
            return None;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{BlockCache, FileCache};
    use crate::comparator::default_comparator;
    use crate::config::Compression;
    use crate::sst::{Writer, WriterOptions};
    use crate::storage::LocalStorage;

    fn opts(restart_interval: usize, block_size: usize, prefix_delta: bool) -> WriterOptions {
        WriterOptions {
            compression: Compression::None,
            compression_rules: Vec::new(),
            cmp: default_comparator(),
            enable_bloom: false,
            bloom_fpr: None,
            klog_value_threshold: 1 << 20,
            block_size,
            expected_entries: 512,
            use_btree: false,
            restart_interval,
            extended_entries: false,
            prefix_delta,
        }
    }

    fn open(path: &str) -> Arc<Reader> {
        Reader::open(
            path,
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            11,
            default_comparator(),
            0,
        )
        .unwrap()
    }

    /// 1 KiB keys sharing 1023 bytes: one restart run holds far more key bytes
    /// than the arena bound, so the cursor must drop the arena and re-decode
    /// from the anchor instead of growing without limit.
    #[test]
    fn delta_run_arena_bound_is_respected() {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("wide.klog");
        let klog = klog.to_str().unwrap();
        // The interval is larger than a block holds, so each block is exactly
        // one run and the run is as long as the block allows.
        let mut w = Writer::new(klog, opts(1024, 4096, true)).unwrap();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..600u32 {
            let mut k = vec![b'p'; 1023];
            k.extend_from_slice(&i.to_be_bytes()[3..]); // 1024 bytes, 1023 shared
            if i % 256 == 0 && i > 0 {
                // Keep the keys strictly ascending past the single-byte wrap.
                continue;
            }
            keys.push(k);
        }
        keys.sort();
        keys.dedup();
        for (i, k) in keys.iter().enumerate() {
            w.add(k, b"v", (i + 1) as u64, 0, crate::format::KIND_PUT)
                .unwrap();
        }
        w.finish().unwrap();

        let r = open(klog);
        assert!(r.prefix_delta());
        let mut it = r.iter();
        // A reverse walk is what forces run materialization.
        it.seek_to_last();
        let mut seen = 0usize;
        while it.valid() {
            seen += 1;
            it.prev();
        }
        assert_eq!(it.err().map(|e| e.to_string()), None);
        assert_eq!(seen, keys.len(), "reverse walk must see every entry");
        assert!(
            it.run_arena_overflows() > 0,
            "the arena bound never fired: keys are {} bytes each",
            keys[0].len()
        );
        // ...and the fallback still produced the right keys, in order.
        let mut it = r.iter();
        it.seek_to_last();
        for want in keys.iter().rev() {
            assert!(it.valid());
            assert_eq!(it.user_key(), want.as_slice());
            it.prev();
        }
        assert!(!it.valid());
    }

    /// A delta key exists contiguously nowhere in the block, so the merge
    /// iterator must be told there is nothing to pin (AGENTS.md invariant 8).
    #[test]
    fn delta_child_serves_buffered_key() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("l.klog");
        let delta = dir.path().join("d.klog");
        for (path, is_delta) in [(&legacy, false), (&delta, true)] {
            let mut w = Writer::new(path.to_str().unwrap(), opts(4, 512, is_delta)).unwrap();
            for i in 0..100u64 {
                let k = format!("tenant/alpha/{i:04}");
                w.add(k.as_bytes(), b"value", i + 1, 0, crate::format::KIND_PUT)
                    .unwrap();
            }
            w.finish().unwrap();
        }
        let mut it = open(legacy.to_str().unwrap()).iter();
        it.seek_to_first();
        let (_, start, len) = it.key_block_ref().expect("a legacy child pins its key");
        assert_eq!(len, it.user_key().len());
        assert!(start > 0);

        let mut it = open(delta.to_str().unwrap()).iter();
        it.seek_to_first();
        assert!(
            it.key_block_ref().is_none(),
            "a delta child must not hand out a borrow into the block"
        );
        // The key itself is still served, from the reconstruction buffer.
        assert_eq!(it.user_key(), b"tenant/alpha/0000");
        // ...including for an entry that actually shares bytes.
        it.next();
        assert_eq!(it.user_key(), b"tenant/alpha/0001");
        assert!(it.key_block_ref().is_none());
    }

    /// Only keys are split: an inline value is still one contiguous run of
    /// block bytes, so it keeps its pin and the 3x-scan-regression fix stands.
    #[test]
    fn delta_child_still_pins_inline_value() {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("d.klog");
        let klog = klog.to_str().unwrap();
        let mut w = Writer::new(klog, opts(4, 512, true)).unwrap();
        for i in 0..64u64 {
            let k = format!("tenant/alpha/{i:04}");
            w.add(
                k.as_bytes(),
                b"the-inline-value",
                i + 1,
                0,
                crate::format::KIND_PUT,
            )
            .unwrap();
        }
        w.finish().unwrap();
        let r = open(klog);
        let mut it = r.iter();
        it.seek_to_first();
        let mut n = 0;
        while it.valid() {
            let (block, start, len) = it
                .value_block_ref()
                .expect("an inline value stays pinned in a delta block");
            assert_eq!(&block.bytes()[start..start + len], b"the-inline-value");
            n += 1;
            it.next();
        }
        assert_eq!(n, 64);
    }
}
