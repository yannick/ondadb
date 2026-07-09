//! Arena-backed skip-list shard (the `arena-memtable` memtable).
//!
//! Each shard is a skip list whose nodes live in a **chunked arena** — fixed-
//! capacity `Vec<Node>` chunks that are never reallocated, so a `*mut Node` stays
//! valid for the shard's entire life and nodes are freed all at once on drop.
//!
//! Concurrency model: **one writer at a time per shard** (a `Mutex` serializes
//! structural changes) with **lock-free readers**.  A writer fully initializes a
//! node, then publishes it by storing into a predecessor's `next` pointer with
//! `Release`; readers traverse with `Acquire` loads, so they never observe a
//! partially built node.  Because nodes are never freed while the shard is alive,
//! following a loaded pointer is always sound.
//!
//! This is the one place ondaDB uses raw pointers; it is compiled only under the
//! `arena-memtable` feature.

use std::cmp::Ordering;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering as AtOrd};

use parking_lot::Mutex;

use crate::comparator::ComparatorRef;
use crate::format::{self, flags};
use crate::memtable::{Entry, Lookup};

// Memtables are bounded by `write_buffer_size` and split across 256 shards, so
// a shard holds a few thousand entries; with p = 1/4 a height of 8 covers
// shards up to ~64k entries. Smaller towers mean smaller nodes (each level is
// an AtomicPtr) and denser arena chunks.
const MAX_HEIGHT: usize = 8;
const CHUNK: usize = 1024;
const P_NUM: u32 = 1; // p = 1/4
const P_DEN: u32 = 4;

/// Zero-padded big-endian packing of a key's first 8 bytes. Comparing two
/// prefixes is consistent with byte-wise key ordering whenever they differ;
/// equal prefixes require a full key comparison.
#[inline]
fn key_prefix(user_key: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    let n = user_key.len().min(8);
    b[..n].copy_from_slice(&user_key[..n]);
    u64::from_be_bytes(b)
}

struct Node {
    /// First 8 key bytes, inline: most byte-wise probes are decided from the
    /// node's own cache line without dereferencing `data`.
    kprefix: u64,
    /// Sequence number, inline (also encoded in `data`'s trailer).
    nseq: u64,
    /// One allocation per node: `user_key || !seq (8B BE) || value`.
    data: Box<[u8]>,
    /// Length of the user-key prefix in `data`.
    klen: u32,
    ttl: i64,
    flags: u8,
    next: [AtomicPtr<Node>; MAX_HEIGHT],
}

impl Node {
    /// Build a node, packing key, sequence trailer and value into one buffer.
    fn new(user_key: &[u8], value: &[u8], seq: u64, ttl: i64, flags: u8) -> Node {
        let mut data = Vec::with_capacity(user_key.len() + format::TRAILER_SIZE + value.len());
        format::append_internal_key(&mut data, user_key, seq);
        data.extend_from_slice(value);
        Node {
            kprefix: key_prefix(user_key),
            nseq: seq,
            data: data.into_boxed_slice(),
            klen: user_key.len() as u32,
            ttl,
            flags,
            next: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        }
    }
    #[inline]
    fn user_key(&self) -> &[u8] {
        &self.data[..self.klen as usize]
    }
    #[inline]
    fn seq(&self) -> u64 {
        self.nseq
    }
    #[inline]
    fn value(&self) -> &[u8] {
        &self.data[self.klen as usize + format::TRAILER_SIZE..]
    }
}

/// Owns the node arena; chunks are boxed slices that never move.
struct Arena {
    chunks: Vec<Box<[std::mem::MaybeUninit<Node>; CHUNK]>>,
    len_in_last: usize,
}

impl Arena {
    fn new() -> Arena {
        Arena {
            chunks: Vec::new(),
            len_in_last: 0,
        }
    }

    /// Allocate a node in the arena and return a stable pointer to it.
    fn alloc(&mut self, node: Node) -> *mut Node {
        if self.chunks.is_empty() || self.len_in_last == CHUNK {
            // New chunk of uninitialized slots; the box keeps the buffer fixed.
            let chunk: Box<[std::mem::MaybeUninit<Node>; CHUNK]> =
                Box::new([const { std::mem::MaybeUninit::uninit() }; CHUNK]);
            self.chunks.push(chunk);
            self.len_in_last = 0;
        }
        let chunk = self.chunks.last_mut().unwrap();
        let slot = &mut chunk[self.len_in_last];
        slot.write(node);
        self.len_in_last += 1;
        slot.as_mut_ptr()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let num_chunks = self.chunks.len();
        let len_in_last = self.len_in_last;
        // Drop each initialized node in place.
        for (ci, chunk) in self.chunks.iter_mut().enumerate() {
            let n = if ci + 1 == num_chunks {
                len_in_last
            } else {
                CHUNK
            };
            for slot in chunk.iter_mut().take(n) {
                // SAFETY: the first `n` slots of each chunk were initialized.
                unsafe { slot.assume_init_drop() };
            }
        }
    }
}

/// A single arena-backed skip-list shard.
pub(crate) struct ArenaShard {
    head: Box<Node>,
    arena: Mutex<Arena>,
    height: AtomicU32,
    rng: AtomicU32,
    cmp: ComparatorRef,
    /// Comparator is plain byte-wise ordering; compare with an inlined slice
    /// cmp instead of a virtual call on every skip-list step.
    bytewise: bool,
}

// SAFETY: readers only follow Acquire-published pointers to never-freed nodes;
// writers are serialized by `arena`'s Mutex. The raw pointers are into the arena
// owned by this shard, which outlives all access.
unsafe impl Send for ArenaShard {}
unsafe impl Sync for ArenaShard {}

impl ArenaShard {
    pub(crate) fn new(cmp: ComparatorRef) -> ArenaShard {
        let head = Box::new(Node::new(&[], &[], u64::MAX, 0, 0));
        let bytewise = cmp.is_bytewise();
        ArenaShard {
            head,
            arena: Mutex::new(Arena::new()),
            height: AtomicU32::new(1),
            rng: AtomicU32::new(0x2545_F491),
            cmp,
            bytewise,
        }
    }

    fn random_height(&self) -> usize {
        // xorshift; bump a level with probability P_NUM/P_DEN.
        let mut h = 1;
        loop {
            let mut x = self.rng.load(AtOrd::Relaxed);
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            self.rng.store(x, AtOrd::Relaxed);
            if h < MAX_HEIGHT && (x % P_DEN) < P_NUM {
                h += 1;
            } else {
                break;
            }
        }
        h
    }

    /// Order a node against `(user_key, seq)` in internal order
    /// (user key ascending via the comparator, seq descending). `ukpfx` is
    /// `key_prefix(uk)`, precomputed by the caller: for byte-wise ordering most
    /// probes are decided by the inline prefix without touching `node.data`.
    #[inline]
    fn cmp_node(&self, node: &Node, uk: &[u8], ukpfx: u64, seq: u64) -> Ordering {
        let key_ord = if self.bytewise {
            match node.kprefix.cmp(&ukpfx) {
                Ordering::Equal => node.user_key().cmp(uk),
                ord => return ord,
            }
        } else {
            self.cmp.compare(node.user_key(), uk)
        };
        key_ord.then_with(|| seq.cmp(&node.nseq))
    }

    pub(crate) fn put(&self, user_key: &[u8], value: &[u8], seq: u64, ttl: i64, fl: u8) {
        // Build the node (allocation + copies) before taking the writer lock so
        // the critical section is just the traversal and pointer swings.
        let node = Node::new(user_key, value, seq, ttl, fl);
        let mut arena = self.arena.lock();
        self.insert_node(&mut arena, node);
    }

    /// Insert a whole per-shard group from a committed batch under ONE writer
    /// lock. `group` holds indices into `recs` belonging to this shard.
    pub(crate) fn put_group(&self, recs: &[crate::wal::RecordRef<'_>], group: &[u32]) {
        // Prepare every node outside the lock.
        let nodes: Vec<Node> = group
            .iter()
            .map(|&i| {
                let r = &recs[i as usize];
                let fl = crate::memtable::flag_bits(r.tombstone, r.single_delete, r.ttl);
                Node::new(r.key, r.value, r.seq, r.ttl, fl)
            })
            .collect();
        let mut arena = self.arena.lock();
        for node in nodes {
            self.insert_node(&mut arena, node);
        }
    }

    /// Skip-list insert of a prepared node; caller holds the writer lock.
    #[allow(clippy::needless_range_loop)] // `level` indexes both `preds` and `next`
    fn insert_node(&self, arena: &mut Arena, node: Node) {
        let ukpfx = node.kprefix;
        let seq = node.nseq;

        // Find predecessors at each level.
        let mut preds: [*mut Node; MAX_HEIGHT] = [ptr::null_mut(); MAX_HEIGHT];
        let head: *mut Node = &*self.head as *const Node as *mut Node;
        let mut x = head;
        let cur_height = self.height.load(AtOrd::Relaxed) as usize;
        for level in (0..cur_height).rev() {
            loop {
                // SAFETY: `x` points at the head or a live arena node.
                let next = unsafe { (*x).next[level].load(AtOrd::Acquire) };
                if next.is_null() {
                    break;
                }
                if self
                    .cmp_node(unsafe { &*next }, node.user_key(), ukpfx, seq)
                    .is_lt()
                {
                    x = next;
                } else {
                    break;
                }
            }
            preds[level] = x;
        }

        let height = self.random_height();
        if height > cur_height {
            for p in preds.iter_mut().take(height).skip(cur_height) {
                *p = head;
            }
            self.height.store(height as u32, AtOrd::Relaxed);
        }

        let node_ptr = arena.alloc(node);
        // Link the new node, publishing each level with Release.
        for level in 0..height {
            let pred = preds[level];
            // SAFETY: pred is head or a live node; node_ptr is freshly allocated.
            unsafe {
                let succ = (*pred).next[level].load(AtOrd::Acquire);
                (*node_ptr).next[level].store(succ, AtOrd::Relaxed);
                (*pred).next[level].store(node_ptr, AtOrd::Release);
            }
        }
    }

    /// Skip-list descent to the last node `<` the probe (`inclusive: false`)
    /// or `<=` the probe (`inclusive: true`) in internal order (user key
    /// ascending, seq descending). Returns the head sentinel when no node
    /// qualifies.
    fn descend(&self, user_key: &[u8], ukpfx: u64, seq: u64, inclusive: bool) -> *const Node {
        let head: *const Node = &*self.head;
        let mut x = head;
        let cur_height = self.height.load(AtOrd::Relaxed) as usize;
        for level in (0..cur_height).rev() {
            loop {
                let next = unsafe { (*x).next[level].load(AtOrd::Acquire) };
                if next.is_null() {
                    break;
                }
                let ord = self.cmp_node(unsafe { &*next }, user_key, ukpfx, seq);
                if if inclusive { ord.is_le() } else { ord.is_lt() } {
                    x = next as *const Node;
                } else {
                    break;
                }
            }
        }
        x
    }

    /// The first node whose `(user_key, seq)` is `>=` the probe, or null.
    fn find_ge(&self, user_key: &[u8], ukpfx: u64, seq: u64) -> *const Node {
        let x = self.descend(user_key, ukpfx, seq, false);
        unsafe { (*x).next[0].load(AtOrd::Acquire) }
    }

    /// The first node strictly `>` the probe, or null.
    fn find_gt(&self, user_key: &[u8], ukpfx: u64, seq: u64) -> *const Node {
        let x = self.descend(user_key, ukpfx, seq, true);
        unsafe { (*x).next[0].load(AtOrd::Acquire) }
    }

    /// The last node whose `(user_key, seq)` is `<=` the probe, or null.
    fn find_le(&self, user_key: &[u8], ukpfx: u64, seq: u64) -> *const Node {
        let x = self.descend(user_key, ukpfx, seq, true);
        if std::ptr::eq(x, &*self.head) {
            std::ptr::null()
        } else {
            x
        }
    }

    /// The last node strictly `<` the probe, or null.
    fn find_lt(&self, user_key: &[u8], ukpfx: u64, seq: u64) -> *const Node {
        let x = self.descend(user_key, ukpfx, seq, false);
        if std::ptr::eq(x, &*self.head) {
            std::ptr::null()
        } else {
            x
        }
    }

    /// The last node in the shard, or null. `O(log n)` — descends the head
    /// tower following the highest non-null links.
    fn find_last(&self) -> *const Node {
        let head: *const Node = &*self.head;
        let mut x = head;
        let cur_height = self.height.load(AtOrd::Relaxed) as usize;
        for level in (0..cur_height).rev() {
            loop {
                let next = unsafe { (*x).next[level].load(AtOrd::Acquire) };
                if next.is_null() {
                    break;
                }
                x = next;
            }
        }
        if std::ptr::eq(x, head) {
            std::ptr::null()
        } else {
            x
        }
    }

    pub(crate) fn get(&self, user_key: &[u8], read_seq: u64, now: i64) -> Lookup {
        // Find the first node >= (user_key, read_seq).
        let ukpfx = key_prefix(user_key);
        let cand = self.find_ge(user_key, ukpfx, read_seq);
        if cand.is_null() {
            return Lookup::default();
        }
        let node = unsafe { &*cand };
        if self.cmp.compare(node.user_key(), user_key) != Ordering::Equal {
            return Lookup::default();
        }
        let seq = node.seq();
        if node.flags & flags::TOMBSTONE != 0 {
            return Lookup {
                seq,
                found: true,
                deleted: true,
                ..Default::default()
            };
        }
        if node.ttl != 0 && node.ttl <= now {
            return Lookup {
                seq,
                found: true,
                deleted: true,
                ..Default::default()
            };
        }
        Lookup {
            value: node.value().to_vec(),
            seq,
            found: true,
            deleted: false,
        }
    }

    /// Append every entry to `out` (level-0 order is already sorted).
    pub(crate) fn collect(&self, out: &mut Vec<Entry>) {
        let mut x = self.head.next[0].load(AtOrd::Acquire);
        while !x.is_null() {
            let node = unsafe { &*x };
            out.push(Entry {
                user_key: node.user_key().to_vec(),
                value: node.value().to_vec(),
                seq: node.seq(),
                ttl: node.ttl,
                tombstone: node.flags & flags::TOMBSTONE != 0,
                single_delete: node.flags & flags::SINGLE_DELETE != 0,
            });
            x = unsafe { (*x).next[0].load(AtOrd::Acquire) };
        }
    }

    /// A borrowing cursor over this shard's entries in internal order, for the
    /// zero-materialization flush path.
    pub(crate) fn cursor(&self) -> ShardCursor<'_> {
        ShardCursor {
            node: self.head.next[0].load(AtOrd::Acquire),
            _shard: std::marker::PhantomData,
        }
    }

    /// A borrowing cursor positioned at the first entry whose `(user_key, seq)`
    /// is `>=` the probe in internal order — the lazy read-iterator seek.
    pub(crate) fn cursor_ge(&self, user_key: &[u8], seq: u64) -> ShardCursor<'_> {
        ShardCursor {
            node: self.find_ge(user_key, key_prefix(user_key), seq),
            _shard: std::marker::PhantomData,
        }
    }

    /// A cursor at the first entry strictly `>` the probe (direction flips).
    pub(crate) fn cursor_gt(&self, user_key: &[u8], seq: u64) -> ShardCursor<'_> {
        ShardCursor {
            node: self.find_gt(user_key, key_prefix(user_key), seq),
            _shard: std::marker::PhantomData,
        }
    }

    /// A cursor at the last entry whose `(user_key, seq)` is `<=` the probe.
    pub(crate) fn cursor_le(&self, user_key: &[u8], seq: u64) -> ShardCursor<'_> {
        ShardCursor {
            node: self.find_le(user_key, key_prefix(user_key), seq),
            _shard: std::marker::PhantomData,
        }
    }

    /// A cursor at the last entry strictly `<` the probe (backward step).
    pub(crate) fn cursor_lt(&self, user_key: &[u8], seq: u64) -> ShardCursor<'_> {
        ShardCursor {
            node: self.find_lt(user_key, key_prefix(user_key), seq),
            _shard: std::marker::PhantomData,
        }
    }

    /// A cursor at the shard's last entry.
    pub(crate) fn cursor_last(&self) -> ShardCursor<'_> {
        ShardCursor {
            node: self.find_last(),
            _shard: std::marker::PhantomData,
        }
    }
}

/// Walks one shard's level-0 list, borrowing keys/values straight from the
/// arena nodes. Sound because nodes are never freed or mutated while the shard
/// is alive (`'a`), and flush only runs on sealed (writer-quiesced) memtables.
pub(crate) struct ShardCursor<'a> {
    node: *const Node,
    _shard: std::marker::PhantomData<&'a ArenaShard>,
}

// SAFETY: the cursor only reads Acquire-published, never-freed nodes owned by
// the shard it borrows.
unsafe impl Send for ShardCursor<'_> {}

impl<'a> ShardCursor<'a> {
    #[inline]
    pub(crate) fn valid(&self) -> bool {
        !self.node.is_null()
    }
    #[inline]
    fn node(&self) -> &'a Node {
        // SAFETY: `valid()` is checked by callers; nodes live as long as 'a.
        unsafe { &*self.node }
    }
    #[inline]
    pub(crate) fn advance(&mut self) {
        self.node = self.node().next[0].load(AtOrd::Acquire);
    }
    #[inline]
    pub(crate) fn key_prefix(&self) -> u64 {
        self.node().kprefix
    }
    #[inline]
    pub(crate) fn user_key(&self) -> &'a [u8] {
        self.node().user_key()
    }
    #[inline]
    pub(crate) fn value(&self) -> &'a [u8] {
        self.node().value()
    }
    #[inline]
    pub(crate) fn seq(&self) -> u64 {
        self.node().nseq
    }
    #[inline]
    pub(crate) fn ttl(&self) -> i64 {
        self.node().ttl
    }
    #[inline]
    pub(crate) fn tombstone(&self) -> bool {
        self.node().flags & flags::TOMBSTONE != 0
    }
    #[inline]
    pub(crate) fn single_delete(&self) -> bool {
        self.node().flags & flags::SINGLE_DELETE != 0
    }
}

impl std::fmt::Debug for ArenaShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaShard").finish()
    }
}
