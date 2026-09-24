//! Per-operation performance counters.
//!
//! Aggregate counters (`ColumnFamily::point_reads`, `BlockCache::stats`, ...)
//! answer "how much work did this database do"; they cannot answer "why was
//! *this* `get` slow". A [`PerfContext`] is caller-owned and thread-scoped: a
//! caller opens a [`Scope`], runs one operation, and gets back the counters the
//! read path bumped while that scope was the innermost one on this thread. No
//! DB-wide atomic is touched, so two threads measuring at once do not contend.

use std::cell::{Cell, RefCell};
use std::marker::PhantomData;

/// Counters accumulated for one operation.
///
/// **Thread-affine and best-effort.** Counters accumulate on the thread that
/// does the work, into that thread's innermost open scope. An [`crate::Iterator`]
/// is `Send`; moving one to another thread keeps it working, but its counters
/// then land in whatever scope (if any) is open on the *new* thread. That is
/// documented behavior, not a bug to work around.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PerfContext {
    /// Bloom filters consulted, one per candidate SSTable (a table without a
    /// filter still counts — it was still considered).
    pub bloom_probes: u64,
    /// Candidate SSTables the filter ruled out.
    pub bloom_negatives: u64,
    /// Memtable lookups (unified store, active memtable, each sealed memtable).
    pub memtable_probes: u64,
    /// SSTables actually searched after the filter admitted them.
    pub sstable_probes: u64,
    /// Binary searches of an SSTable block index.
    pub index_seeks: u64,
    /// Data blocks served by the block cache.
    pub block_cache_hits: u64,
    /// Data blocks that had to be fetched (from disk, or from the mmap).
    pub block_misses: u64,
    /// Bytes read for data blocks, framed size for a fetch and raw size for a
    /// zero-copy mmap view.
    pub block_read_bytes: u64,
    /// Raw bytes produced by decompressing data blocks.
    pub bytes_decompressed: u64,
    /// Values resolved out of a vlog.
    pub vlog_reads: u64,
    /// Logical (uncompressed) bytes of those values.
    pub vlog_read_bytes: u64,
    /// Separated values served whole from the block cache — no positional
    /// read, no CRC verify, no decompression. Disjoint from `vlog_reads`:
    /// a hit is not a read.
    pub vlog_cache_hits: u64,
    /// `seek*` calls on an iterator (not `next`/`prev`).
    pub iterator_seeks: u64,
    /// Key groups an iterator surfaced to its caller.
    pub iterator_steps: u64,
    /// Data-block fetches [`crate::DB::multi_get`] avoided by resolving several
    /// keys of one batch against a block it had already materialized: for each
    /// distinct block a batch touches, the number of its target keys minus one.
    /// Zero for a batch whose keys all land in different blocks, and zero for
    /// every non-batched read.
    pub multiget_blocks_deduped: u64,
    /// Range-delete sources this operation consulted (1.2): one per memtable,
    /// sealed memtable, unified store and SSTable whose fragments could cover a
    /// key the operation touched.
    ///
    /// Zero for every column family that never issued a `delete_range` — the
    /// gate is one comparison against `range_count == 0` per source, and a
    /// source that is skipped is not counted. A number far above
    /// `sstable_probes` is the signal that fragments have accumulated and want
    /// a compaction to merge them.
    pub range_sources: u64,
    /// Keys this operation found deleted by a covering range tombstone rather
    /// than by a point tombstone or by absence.
    pub range_masked: u64,
    /// Data blocks a [`crate::DB::multi_get`] fetched through its bounded
    /// parallel runner ([`Options::max_concurrent_block_reads`]) instead of
    /// one at a time on the calling thread. Only cold blocks of tables on a
    /// slow tier (one whose storage reports `supports_mmap() == false`) take
    /// that path; zero everywhere else.
    ///
    /// [`Options::max_concurrent_block_reads`]: crate::Options::max_concurrent_block_reads
    pub multiget_parallel_reads: u64,
    /// Parallel block reads that waited for a permit: the database-wide bound,
    /// not the device, was the limit (wavesdb `MultiGetIOWaits`).
    pub multiget_io_waits: u64,
}

impl PerfContext {
    /// All-zero counters. A `const fn` because the thread-local that holds the
    /// innermost frame is const-initialized, and `Default::default` is not
    /// callable there.
    const fn new() -> PerfContext {
        PerfContext {
            bloom_probes: 0,
            bloom_negatives: 0,
            memtable_probes: 0,
            sstable_probes: 0,
            index_seeks: 0,
            block_cache_hits: 0,
            block_misses: 0,
            block_read_bytes: 0,
            bytes_decompressed: 0,
            vlog_reads: 0,
            vlog_read_bytes: 0,
            vlog_cache_hits: 0,
            iterator_seeks: 0,
            iterator_steps: 0,
            multiget_blocks_deduped: 0,
            range_sources: 0,
            range_masked: 0,
            multiget_parallel_reads: 0,
            multiget_io_waits: 0,
        }
    }

    /// Add every counter of `o` into `self` — how a worker thread's counters
    /// reach the scope of the thread that handed it the work.
    pub(crate) fn absorb(&mut self, o: &PerfContext) {
        self.bloom_probes += o.bloom_probes;
        self.bloom_negatives += o.bloom_negatives;
        self.memtable_probes += o.memtable_probes;
        self.sstable_probes += o.sstable_probes;
        self.index_seeks += o.index_seeks;
        self.block_cache_hits += o.block_cache_hits;
        self.block_misses += o.block_misses;
        self.block_read_bytes += o.block_read_bytes;
        self.bytes_decompressed += o.bytes_decompressed;
        self.vlog_reads += o.vlog_reads;
        self.vlog_read_bytes += o.vlog_read_bytes;
        self.vlog_cache_hits += o.vlog_cache_hits;
        self.iterator_seeks += o.iterator_seeks;
        self.iterator_steps += o.iterator_steps;
        self.multiget_blocks_deduped += o.multiget_blocks_deduped;
        self.range_sources += o.range_sources;
        self.range_masked += o.range_masked;
        self.multiget_parallel_reads += o.multiget_parallel_reads;
        self.multiget_io_waits += o.multiget_io_waits;
    }
}

/// The state `bump` touches. Split from the outer frames on purpose: neither
/// field owns anything, so this thread-local needs **no destructor** and its
/// access lowers to a plain TLS load instead of a lazy-init-plus-registration
/// call. `bump` sits inside every block read and every memtable probe, so that
/// difference is the hot path.
struct Hot {
    /// Number of open scopes on this thread. Checked first, so the nil path is
    /// one `Cell` load and a compare — no `RefCell` borrow.
    depth: Cell<usize>,
    /// The innermost scope's counters. Meaningful only while `depth > 0`.
    top: RefCell<PerfContext>,
}

thread_local! {
    static HOT: Hot = const {
        Hot {
            depth: Cell::new(0),
            top: RefCell::new(PerfContext::new()),
        }
    };
    /// Frames of the *enclosing* scopes, innermost last. Touched only by
    /// `enter`/`pop`, which are cold relative to `bump`, so the allocation (and
    /// therefore the destructor) stays out of the hot thread-local.
    static OUTER: RefCell<Vec<PerfContext>> = const { RefCell::new(Vec::new()) };
}

/// An open measurement scope. See [`enter`].
///
/// Deliberately neither `Send` nor `Sync`: the stack it indexes into belongs to
/// the thread that opened it, so dropping one elsewhere would pop an unrelated
/// frame.
#[derive(Debug)]
pub struct Scope(PhantomData<*const ()>);

/// Open a scope on this thread and start counting into a fresh [`PerfContext`].
///
/// Scopes nest as a stack. [`bump`] writes to the **innermost** frame only, and
/// an inner scope's counters do not roll up into the outer one.
pub fn enter() -> Scope {
    HOT.with(|hot| {
        if hot.depth.get() > 0 {
            // Park the enclosing scope's counters; `top` always holds the
            // innermost frame.
            OUTER.with(|outer| outer.borrow_mut().push(*hot.top.borrow()));
        }
        *hot.top.borrow_mut() = PerfContext::new();
        hot.depth.set(hot.depth.get() + 1);
    });
    Scope(PhantomData)
}

/// Pop the innermost frame, if this thread still has one.
///
/// `try_with` because a `Scope` can be dropped while thread-locals are being
/// torn down at thread exit, where `with` would panic.
fn pop() -> Option<PerfContext> {
    HOT.try_with(|hot| {
        let depth = hot.depth.get();
        if depth == 0 {
            return None;
        }
        let popped = *hot.top.borrow();
        // Restoring the enclosing frame is what keeps an inner scope's counters
        // from rolling up into the outer one.
        let restored = if depth > 1 {
            OUTER
                .try_with(|outer| outer.borrow_mut().pop())
                .ok()
                .flatten()
                .unwrap_or_default()
        } else {
            PerfContext::new()
        };
        *hot.top.borrow_mut() = restored;
        hot.depth.set(depth - 1);
        Some(popped)
    })
    .ok()
    .flatten()
}

impl Scope {
    /// Close the scope and return its counters.
    pub fn finish(self) -> PerfContext {
        let ctx = pop().unwrap_or_default();
        // The frame is already gone; let `Drop` not pop the *enclosing* one.
        std::mem::forget(self);
        ctx
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        // A scope abandoned without `finish` — including one unwound past by a
        // panic — must still restore the stack to its prior depth.
        pop();
    }
}

/// Whether this thread has an open scope — so work handed to another thread
/// knows whether its counters are wanted at all.
pub(crate) fn active() -> bool {
    HOT.try_with(|hot| hot.depth.get() > 0).unwrap_or(false)
}

/// Add to the innermost open scope on this thread; a no-op when none is open.
///
/// `f` must not call back into this module: the innermost frame is borrowed
/// while it runs. Every call site is a handful of `+=`.
#[inline]
pub(crate) fn bump(f: impl FnOnce(&mut PerfContext)) {
    let _ = HOT.try_with(|hot| {
        if hot.depth.get() == 0 {
            return;
        }
        f(&mut hot.top.borrow_mut());
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth() -> usize {
        HOT.with(|hot| hot.depth.get())
    }

    #[test]
    fn bump_outside_a_scope_is_a_noop() {
        assert_eq!(depth(), 0);
        bump(|p| p.block_misses += 1);
        assert_eq!(depth(), 0, "a bump must not create a frame");
        // The next scope opened on this thread must still start at zero.
        let scope = enter();
        assert_eq!(scope.finish(), PerfContext::default());
    }

    #[test]
    fn finish_returns_only_this_scopes_counters() {
        let scope = enter();
        bump(|p| p.memtable_probes += 1);
        bump(|p| {
            p.block_misses += 2;
            p.block_read_bytes += 4096;
        });
        let ctx = scope.finish();
        assert_eq!(ctx.memtable_probes, 1);
        assert_eq!(ctx.block_misses, 2);
        assert_eq!(ctx.block_read_bytes, 4096);
        assert_eq!(ctx.bloom_probes, 0);
        assert_eq!(depth(), 0, "finish must pop the frame");
    }

    #[test]
    fn nested_scopes_do_not_leak() {
        let outer = enter();
        bump(|p| p.sstable_probes += 1);
        let inner = enter();
        bump(|p| p.sstable_probes += 10);
        let inner_ctx = inner.finish();
        bump(|p| p.sstable_probes += 100);
        let outer_ctx = outer.finish();
        assert_eq!(inner_ctx.sstable_probes, 10, "inner counts only its own");
        assert_eq!(
            outer_ctx.sstable_probes, 101,
            "the inner scope must not roll up into the outer one"
        );
    }

    #[test]
    fn dropped_scope_pops_the_stack() {
        let outer = enter();
        bump(|p| p.iterator_steps += 1);
        {
            let _inner = enter();
            bump(|p| p.iterator_steps += 7);
        }
        assert_eq!(depth(), 1, "a dropped scope pops its own frame");

        // ...including when the drop runs during an unwind.
        let before = depth();
        let caught = std::panic::catch_unwind(|| {
            let _panicking = enter();
            bump(|p| p.iterator_steps += 7);
            panic!("unwind through an open scope");
        });
        assert!(caught.is_err());
        assert_eq!(depth(), before, "unwinding must leave the stack as it was");

        bump(|p| p.iterator_steps += 1);
        assert_eq!(
            outer.finish().iterator_steps,
            2,
            "discarded frames must not contribute to the surviving scope"
        );
    }
}
