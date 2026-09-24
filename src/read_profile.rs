//! Opt-in, database-wide read profiling (wavesdb `EnableReadProfiling` /
//! `ReadStats`, plan C §1.4 F13).
//!
//! [`PerfContext`] already attributes read-path work to *one* operation on the
//! calling thread. This module answers the other question — "what has the
//! read path of this database done since I started looking" — by **reusing**
//! those same counters instead of adding a second set of bumps to the hot path:
//! while profiling is on, each point read, batched read and iterator step runs
//! inside a private [`perf`](crate::perf) scope whose counters are folded into
//! database-wide atomics when it closes (and handed on to any scope the caller
//! had open, so a caller's own `PerfContext` still sees them).
//!
//! **Off costs one relaxed atomic load per point read or batch**, and nothing
//! per iterator step: an iterator decides at construction whether it is
//! profiled. So an iterator created before [`DB::enable_read_profiling`] is
//! not counted, and one created while profiling was on keeps counting after it
//! is turned off (into counters the next enable resets).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::perf::{PerfContext, PERF_FIELDS};

/// Read-path counters accumulated since the last
/// [`DB::enable_read_profiling(true)`](crate::DB::enable_read_profiling).
/// All zero if profiling has never been enabled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadStats {
    /// Profiled single-key reads (`get`, `get_into`, and every read through a
    /// transaction or snapshot handle that reached the store).
    pub point_reads: u64,
    /// Profiled `multi_get` calls, one per call.
    pub multi_get_calls: u64,
    /// Keys requested across those calls.
    pub multi_get_keys: u64,
    /// Profiled iterator positioning calls: every `seek*`, `next` and `prev`
    /// on an iterator created while profiling was on.
    pub iterator_ops: u64,
    /// The read-path mechanism counters, summed over every profiled operation:
    /// bloom checks and negatives, memtable and table probes, block-cache hits
    /// and block fetches, bytes read and decompressed, vlog reads, and so on —
    /// exactly the fields a per-operation [`PerfContext`] reports. (wavesdb's
    /// `BloomChecks`/`BloomNegatives`/`BlockCacheHits`/`DiskReads` are
    /// `bloom_probes`/`bloom_negatives`/`block_cache_hits`/`block_misses`.)
    pub perf: PerfContext,
}

/// The database-wide aggregate behind [`ReadStats`]. One per open database,
/// shared by every column family through `CfCtx`.
#[derive(Debug, Default)]
pub(crate) struct ReadProfiler {
    enabled: AtomicBool,
    point_reads: AtomicU64,
    multi_get_calls: AtomicU64,
    multi_get_keys: AtomicU64,
    iterator_ops: AtomicU64,
    perf: [AtomicU64; PERF_FIELDS],
}

/// Which operation counter a [`Profiled`] guard charges.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReadOp {
    Point,
    MultiGet(usize),
    IteratorStep,
}

/// An open profiled operation: a private perf scope, folded into the
/// profiler on drop (so every early return is still counted).
#[derive(Debug)]
pub(crate) struct Profiled<'a> {
    profiler: &'a ReadProfiler,
    scope: Option<crate::perf::Scope>,
}

impl ReadProfiler {
    #[inline]
    pub(crate) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Start profiling one operation, or `None` — after a single relaxed
    /// load — when profiling is off.
    #[inline]
    pub(crate) fn begin(&self, op: ReadOp) -> Option<Profiled<'_>> {
        if !self.enabled() {
            return None;
        }
        self.begin_always(op)
    }

    /// [`begin`](Self::begin) without the enabled check, for an iterator
    /// that already decided at construction.
    pub(crate) fn begin_always(&self, op: ReadOp) -> Option<Profiled<'_>> {
        match op {
            ReadOp::Point => self.point_reads.fetch_add(1, Ordering::Relaxed),
            ReadOp::MultiGet(keys) => {
                self.multi_get_keys.fetch_add(keys as u64, Ordering::Relaxed);
                self.multi_get_calls.fetch_add(1, Ordering::Relaxed)
            }
            ReadOp::IteratorStep => self.iterator_ops.fetch_add(1, Ordering::Relaxed),
        };
        Some(Profiled {
            profiler: self,
            scope: Some(crate::perf::enter()),
        })
    }

    pub(crate) fn set_enabled(&self, on: bool) {
        if on {
            // Reset before switching on, so the first counted operation lands
            // in zeroed counters. An operation already in flight when this
            // runs may still add to them; the numbers are statistics, not a
            // ledger.
            for c in [
                &self.point_reads,
                &self.multi_get_calls,
                &self.multi_get_keys,
                &self.iterator_ops,
            ] {
                c.store(0, Ordering::Relaxed);
            }
            for c in &self.perf {
                c.store(0, Ordering::Relaxed);
            }
        }
        self.enabled.store(on, Ordering::Relaxed);
    }

    pub(crate) fn stats(&self) -> ReadStats {
        let mut perf = [0u64; PERF_FIELDS];
        for (out, c) in perf.iter_mut().zip(&self.perf) {
            *out = c.load(Ordering::Relaxed);
        }
        ReadStats {
            point_reads: self.point_reads.load(Ordering::Relaxed),
            multi_get_calls: self.multi_get_calls.load(Ordering::Relaxed),
            multi_get_keys: self.multi_get_keys.load(Ordering::Relaxed),
            iterator_ops: self.iterator_ops.load(Ordering::Relaxed),
            perf: PerfContext::from_fields(perf),
        }
    }

    fn add(&self, ctx: &PerfContext) {
        for (c, v) in self.perf.iter().zip(ctx.fields()) {
            if v != 0 {
                c.fetch_add(v, Ordering::Relaxed);
            }
        }
    }
}

impl Drop for Profiled<'_> {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            let ctx = scope.finish_into_parent();
            self.profiler.add(&ctx);
        }
    }
}

impl crate::DB {
    /// Turn database-wide read profiling on or off; see [`ReadStats`].
    ///
    /// Enabling resets every counter to zero. While on, each profiled read
    /// pays a thread-local scope and a few relaxed atomic adds; while off, a
    /// point read pays one relaxed load. Leave it off on hot production paths
    /// unless you are looking at the numbers.
    pub fn enable_read_profiling(&self, on: bool) {
        self.inner.ctx.read_profile.set_enabled(on);
    }

    /// Whether read profiling is currently on.
    pub fn read_profiling_enabled(&self) -> bool {
        self.inner.ctx.read_profile.enabled()
    }

    /// The read-profiling counters accumulated since profiling was last
    /// enabled.
    pub fn read_stats(&self) -> ReadStats {
        self.inner.ctx.read_profile.stats()
    }
}
