//! Unified memtable mode.
//!
//! Instead of a memtable + WAL per column family, the whole database shares one
//! memtable and one WAL.  Each entry's key is prefixed with an 8-byte big-endian
//! **column-family id** (`fnv64(name)`), so a single bytewise-ordered memtable
//! holds every CF's data grouped by id.  When the shared memtable fills, the
//! flush **splits it by CF** into per-CF L0 SSTables (the LSM levels stay
//! per-CF).  Recovery replays the single WAL and routes each record back to its
//! CF by prefix.
//!
//! Point reads work under any per-CF comparator (an exact prefixed-key lookup);
//! ordered iteration and flush re-sort a CF's slice with that CF's comparator.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::Sender;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::column_family::FlushJob;
use crate::comparator::default_comparator;
use crate::config::Options;
use crate::error::Result;
use crate::memtable::{Entry, Lookup, MemIter, Memtable};
use crate::wal::{self, Wal};

/// Stable column-family id: 64-bit FNV-1a of the name.
pub(crate) fn cf_id(name: &str) -> u64 {
    const OFFSET: u64 = 1469598103934665603;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for &b in name.as_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

fn prefixed(id: u64, user_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + user_key.len());
    k.extend_from_slice(&id.to_be_bytes());
    k.extend_from_slice(user_key);
    k
}

/// Lazy view of one column family's contiguous prefix inside a unified
/// bytewise memtable. Keys exposed to the merge iterator have the CF id
/// stripped; positioning adds it back and stops at either prefix boundary.
pub(crate) struct UnifiedMemIter {
    inner: MemIter,
    prefix: [u8; 8],
    valid: bool,
}

impl UnifiedMemIter {
    fn new(mem: Arc<Memtable>, id: u64) -> UnifiedMemIter {
        UnifiedMemIter {
            inner: mem.iter(),
            prefix: id.to_be_bytes(),
            valid: false,
        }
    }

    #[inline]
    fn refresh_valid(&mut self) {
        self.valid = self.inner.valid() && self.inner.user_key().starts_with(&self.prefix);
    }

    pub(crate) fn valid(&self) -> bool {
        self.valid
    }

    pub(crate) fn seek_to_first(&mut self) {
        self.inner.seek_ge(&self.prefix, u64::MAX);
        self.refresh_valid();
    }

    pub(crate) fn seek_to_last(&mut self) {
        if let Some(next) = u64::from_be_bytes(self.prefix).checked_add(1) {
            self.inner.seek_le(&next.to_be_bytes(), u64::MAX);
        } else {
            self.inner.seek_to_last();
        }
        self.refresh_valid();
    }

    pub(crate) fn seek_ge(&mut self, user_key: &[u8], seq: u64) {
        self.inner
            .seek_ge(&prefixed_key(self.prefix, user_key), seq);
        self.refresh_valid();
    }

    pub(crate) fn seek_le(&mut self, user_key: &[u8], seq: u64) {
        self.inner
            .seek_le(&prefixed_key(self.prefix, user_key), seq);
        self.refresh_valid();
    }

    pub(crate) fn next(&mut self) {
        self.inner.next();
        self.refresh_valid();
    }

    pub(crate) fn prev(&mut self) {
        self.inner.prev();
        self.refresh_valid();
    }

    pub(crate) fn user_key(&self) -> &[u8] {
        &self.inner.user_key()[8..]
    }

    pub(crate) fn key_prefix(&self) -> u64 {
        crate::sst::key_prefix8(self.user_key())
    }

    pub(crate) fn seq(&self) -> u64 {
        self.inner.seq()
    }

    pub(crate) fn ttl(&self) -> i64 {
        self.inner.ttl()
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        self.inner.is_tombstone()
    }

    /// Record kind of the current entry; see [`crate::wal::Record::kind`].
    #[inline]
    pub(crate) fn kind(&self) -> u64 {
        self.inner.kind()
    }

    pub(crate) fn value_ref(&self) -> &[u8] {
        self.inner.value_ref()
    }
}

fn prefixed_key(prefix: [u8; 8], user_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + user_key.len());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(user_key);
    key
}

/// A sealed unified memtable awaiting a split flush.
#[derive(Debug)]
pub(crate) struct UnifiedImm {
    pub mem: Arc<Memtable>,
    pub wal_paths: Vec<String>,
}

struct UState {
    mem: Arc<Memtable>,
    wal: Option<Arc<Wal>>,
    wal_gen: u64,
    pending_wals: Vec<String>,
    imm: Vec<Arc<UnifiedImm>>,
}

struct RotState {
    active_writers: usize,
    rotating: bool,
}

/// The database-wide shared memtable + WAL.
pub(crate) struct UnifiedStore {
    dir: String,
    write_buffer_size: usize,
    sync_mode: crate::config::SyncMode,
    sync_interval: std::time::Duration,
    stall_threshold: usize,
    read_only: bool,
    state: RwLock<UState>,
    rot: Mutex<RotState>,
    cond: Condvar,
    flush_tx: Sender<FlushJob>,
    pending_flush: Arc<AtomicUsize>,
    /// Reserved for close-time backpressure bypass (parity with the per-CF path).
    #[allow(dead_code)]
    closing: Arc<AtomicBool>,
    /// DB-wide fail-stop flag, wired into every WAL this store opens.
    poison: Arc<crate::util::Poison>,
    /// DB-wide physical-sync counter, wired into every WAL this store opens.
    wal_syncs: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for UnifiedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedStore")
            .field("dir", &self.dir)
            .finish()
    }
}

fn wal_path(dir: &str, gen: u64) -> String {
    format!("{dir}/unified-wal-{gen}.log")
}

impl UnifiedStore {
    /// Open (and replay) the unified store. Returns the store, the highest
    /// sequence seen during replay, and everything **pass 1** of prepared-state
    /// recovery collected (3.2).
    ///
    /// Pass 1 only collects: it inserts no prepared record into the memtable and
    /// raises no watermark. The unified WAL is four-striped in every mode but
    /// `SyncMode::Full` and `prepare`/`commit_prepared` are separate API calls
    /// that commonly run on different threads, so a prepare and its decision
    /// have no recoverable relative order — matching them is
    /// `DbInner::resolve_recovered_prepares`'s job, once `DbInner` exists and
    /// `observe_seq` is callable.
    pub(crate) fn open(
        dir: &str,
        opts: &Options,
        flush_tx: Sender<FlushJob>,
        pending_flush: Arc<AtomicUsize>,
        closing: Arc<AtomicBool>,
        poison: Arc<crate::util::Poison>,
        wal_syncs: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<(Arc<UnifiedStore>, u64, crate::prepared::RecoveredPrepares)> {
        let mem = Memtable::new(default_comparator());
        let mut max_seq = 0;
        let mut recovered = crate::prepared::RecoveredPrepares::default();
        let mut gens = Vec::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if let Some(rest) = name.strip_prefix("unified-wal-") {
                    if let Some(num) = rest.strip_suffix(".log") {
                        if let Ok(g) = num.parse::<u64>() {
                            gens.push(g);
                        }
                    }
                }
            }
        }
        gens.sort_unstable();
        let mut replay_paths = Vec::new();
        for g in &gens {
            let p = wal_path(dir, *g);
            replay_paths.push(p.clone());
            let last = Wal::replay(&p, |rec| {
                match rec {
                    crate::wal::ReplayRecord::Point(r) => {
                        mem.put(&r.key, r.value, r.seq, r.ttl, r.kind);
                    }
                    // Schema 2: both bounds carry the 8-byte cf-id prefix and
                    // are stored prefixed, exactly as point keys are — the
                    // unified memtable is keyed that way end to end, so
                    // nothing is stripped here.
                    crate::wal::ReplayRecord::RangeDelete { start, end, seq } => {
                        mem.add_range(&start, &end, seq);
                    }
                    // Pass 1. NOTHING is inserted into the memtable here: a
                    // prepared writeset is uncommitted and may yet be aborted.
                    crate::wal::ReplayRecord::Prepare {
                        id,
                        cf_ids,
                        records,
                    } => {
                        recovered.add_prepare(crate::prepared::RecoveredPrepare {
                            id,
                            cf_ids,
                            records,
                            gen: *g,
                        })?;
                    }
                    crate::wal::ReplayRecord::Decision { id, commit } => {
                        recovered.add_decision(crate::prepared::RecoveredDecision {
                            id,
                            commit,
                            gen: *g,
                        });
                    }
                }
                Ok(())
            })?;
            max_seq = max_seq.max(last);
        }
        let next_gen = gens.last().map(|g| g + 1).unwrap_or(0);
        let wbs = if opts.unified_memtable_write_buffer_size > 0 {
            opts.unified_memtable_write_buffer_size
        } else {
            64 << 20
        };
        let (wal, pending) = if opts.read_only {
            (None, replay_paths)
        } else {
            let p = wal_path(dir, next_gen);
            let w = Wal::open(
                &p,
                opts.unified_memtable_sync_mode,
                opts.unified_memtable_sync_interval,
            )?;
            w.set_poison(poison.clone());
            w.set_sync_counter(wal_syncs.clone());
            let w = Arc::new(w);
            let mut pend = replay_paths;
            pend.push(p);
            (Some(w), pend)
        };
        let store = Arc::new(UnifiedStore {
            dir: dir.to_string(),
            write_buffer_size: wbs,
            sync_mode: opts.unified_memtable_sync_mode,
            sync_interval: opts.unified_memtable_sync_interval,
            stall_threshold: opts.unified_memtable_stall_threshold.max(1),
            read_only: opts.read_only,
            state: RwLock::new(UState {
                mem,
                wal,
                wal_gen: next_gen,
                pending_wals: pending,
                imm: Vec::new(),
            }),
            rot: Mutex::new(RotState {
                active_writers: 0,
                rotating: false,
            }),
            cond: Condvar::new(),
            flush_tx,
            pending_flush,
            closing,
            poison,
            wal_syncs,
        });
        Ok((store, max_seq, recovered))
    }

    /// The current WAL handle **and** the generation it belongs to, read in one
    /// acquisition.
    ///
    /// Both halves must come from the same `state.read()`: rotation replaces the
    /// handle and bumps the generation together, so reading them separately can
    /// name a generation the frame did not land in — and the pin bookkeeping
    /// would then withhold the wrong file. The handle is returned so the caller
    /// can `sync()` **it** rather than whatever `sync_wal` would re-clone; a
    /// frame written just before a rotation cannot be synced through the store.
    pub(crate) fn wal_handle(&self) -> (Option<Arc<Wal>>, u64) {
        let s = self.state.read();
        (s.wal.clone(), s.wal_gen)
    }

    /// Apply a committed batch (records carry their CF id) to the WAL + memtable.
    /// Records borrow the transaction's buffer.
    ///
    /// Under schema 2 **both** bounds carry the 8-byte big-endian cf-id prefix,
    /// exactly as point keys do. A span can never cross a cf-id boundary — both
    /// bounds come from one `delete_range` call on one column family, so they
    /// share a prefix by construction — and that is asserted at encode rather
    /// than assumed, because a crossing span would delete another family's keys.
    pub(crate) fn apply_with_ranges(
        self: &Arc<Self>,
        items: &[(u64, wal::RecordRef<'_>)],
        ranges: &[(u64, wal::RangeRef<'_>)],
    ) -> Result<()> {
        {
            let mut g = self.rot.lock();
            loop {
                let stalled = g.rotating
                    || (self.state.read().imm.len() >= self.stall_threshold
                        && !self.closing.load(Ordering::Relaxed));
                if stalled {
                    self.cond.wait(&mut g);
                } else {
                    break;
                }
            }
            g.active_writers += 1;
        }
        let (wal, mem) = {
            let s = self.state.read();
            (s.wal.clone(), s.mem.clone())
        };
        let res: Result<()> = (|| {
            // Build the id-prefixed keys in one scratch buffer, then borrowed
            // records pointing into it.
            let total: usize = items.iter().map(|(_, r)| 8 + r.key.len()).sum();
            let mut scratch = Vec::with_capacity(total);
            let mut ends = Vec::with_capacity(items.len());
            for (id, r) in items {
                scratch.extend_from_slice(&id.to_be_bytes());
                scratch.extend_from_slice(r.key);
                ends.push(scratch.len());
            }
            let mut start = 0usize;
            let mut recs = Vec::with_capacity(items.len());
            for ((_, r), &end) in items.iter().zip(&ends) {
                recs.push(wal::RecordRef {
                    key: &scratch[start..end],
                    ..*r
                });
                start = end;
            }
            // Range bounds are prefixed in their own scratch buffer, so the
            // point scratch above keeps its exact-size single allocation.
            let range_total: usize = ranges
                .iter()
                .map(|(_, r)| 16 + r.start.len() + r.end.len())
                .sum();
            let mut rscratch = Vec::with_capacity(range_total);
            let mut rends = Vec::with_capacity(ranges.len() * 2);
            for (id, r) in ranges {
                let prefix = id.to_be_bytes();
                rscratch.extend_from_slice(&prefix);
                rscratch.extend_from_slice(r.start);
                rends.push(rscratch.len());
                rscratch.extend_from_slice(&prefix);
                rscratch.extend_from_slice(r.end);
                rends.push(rscratch.len());
            }
            let mut prefixed = Vec::with_capacity(ranges.len());
            let mut start = 0usize;
            for ((_, r), pair) in ranges.iter().zip(rends.chunks(2)) {
                let (mid, end) = (pair[0], pair[1]);
                let range = wal::RangeRef {
                    start: &rscratch[start..mid],
                    end: &rscratch[mid..end],
                    seq: r.seq,
                };
                debug_assert_eq!(
                    range.start[..8],
                    range.end[..8],
                    "a range delete may never cross a cf-id prefix"
                );
                prefixed.push(range);
                start = end;
            }

            if let Some(w) = &wal {
                // Same split as the per-CF WAL: only a batch carrying a
                // non-point kind — 1.1's operand, or a 1.2 range fragment —
                // needs the kind-bearing envelope, so ordinary unified commits
                // keep their legacy frame bytes.
                if prefixed.is_empty()
                    && recs.iter().all(|r| crate::format::is_point_kind(r.kind))
                {
                    w.append_batch(&recs)?;
                } else {
                    let mut batch: Vec<wal::EnvelopeRecord<'_>> =
                        Vec::with_capacity(recs.len() + prefixed.len());
                    batch.extend(recs.iter().copied().map(wal::EnvelopeRecord::Point));
                    batch.extend(prefixed.iter().copied().map(wal::EnvelopeRecord::Range));
                    w.append_batch_envelope(wal::ENVELOPE_SCHEMA_UNIFIED, &batch)?;
                }
            }
            mem.put_batch(&recs);
            for r in &prefixed {
                mem.add_range(r.start, r.end, r.seq);
            }
            Ok(())
        })();
        {
            let mut g = self.rot.lock();
            g.active_writers -= 1;
            self.cond.notify_all();
        }
        res?;
        if mem.approx_size() >= self.write_buffer_size as i64 {
            self.rotate(false);
        }
        Ok(())
    }

    /// Apply an already-durable batch to the **memtable only**, at sequences
    /// `start + slot` (3.2).
    ///
    /// The one apply path a prepared commit uses — both `DB::commit_prepared`
    /// and recovery pass 2 go through here, so there is one code path and one
    /// set of tests. It is not [`apply_with_ranges`](Self::apply_with_ranges)
    /// with the WAL switched off: that method writes the WAL *and* the
    /// memtable, and replaying a prepared writeset through it would append a
    /// second full copy of the writeset, which recovery would then have to
    /// reconcile against the decision that already describes it. The durable
    /// record here is the decision frame the caller has already fsynced.
    ///
    /// The rotation gate and `active_writers` bookkeeping are the same as the
    /// ordinary apply's, because invariant 9 is the same: a rotation may not
    /// swap the memtable out from under a writer mid-batch.
    ///
    /// The `seq` field of each [`wal::RecordRef`] is ignored — a prepared
    /// record carries the `0` sentinel on disk, and its real sequence is the
    /// one the decision names.
    pub(crate) fn apply_memtable_only(
        self: &Arc<Self>,
        items: &[(u64, wal::RecordRef<'_>)],
        start: u64,
    ) -> Result<()> {
        {
            let mut g = self.rot.lock();
            loop {
                let stalled = g.rotating
                    || (self.state.read().imm.len() >= self.stall_threshold
                        && !self.closing.load(Ordering::Relaxed));
                if stalled {
                    self.cond.wait(&mut g);
                } else {
                    break;
                }
            }
            g.active_writers += 1;
        }
        let mem = self.state.read().mem.clone();
        // No fallible step here — unlike `apply_with_ranges`, which wraps its
        // body in a closure so a WAL append failure still reaches the
        // `active_writers` decrement below. The memtable insert cannot fail, so
        // the body runs straight through.
        {
            let total: usize = items.iter().map(|(_, r)| 8 + r.key.len()).sum();
            let mut scratch = Vec::with_capacity(total);
            let mut ends = Vec::with_capacity(items.len());
            for (id, r) in items {
                scratch.extend_from_slice(&id.to_be_bytes());
                scratch.extend_from_slice(r.key);
                ends.push(scratch.len());
            }
            let mut at = 0usize;
            let mut recs = Vec::with_capacity(items.len());
            for (slot, ((_, r), &end)) in items.iter().zip(&ends).enumerate() {
                recs.push(wal::RecordRef {
                    key: &scratch[at..end],
                    seq: start + slot as u64,
                    ..*r
                });
                at = end;
            }
            mem.put_batch(&recs);
        }
        {
            let mut g = self.rot.lock();
            g.active_writers -= 1;
            self.cond.notify_all();
        }
        if mem.approx_size() >= self.write_buffer_size as i64 {
            self.rotate(false);
        }
        Ok(())
    }

    /// Resolve `user_key` for column family `id`.
    pub(crate) fn get(&self, id: u64, user_key: &[u8], read_seq: u64, now: i64) -> Lookup {
        let pk = prefixed(id, user_key);
        let s = self.state.read();
        let r = s.mem.get(&pk, read_seq, now);
        if r.found {
            return r;
        }
        for imm in s.imm.iter().rev() {
            let r = imm.mem.get(&pk, read_seq, now);
            if r.found {
                return r;
            }
        }
        Lookup::default()
    }

    /// Whether the shared store holds any range tombstone.
    ///
    /// One relaxed load per memtable — the unified half of the read path's
    /// zero-cost gate.
    pub(crate) fn has_ranges(&self) -> bool {
        let s = self.state.read();
        !s.mem.ranges().is_empty() || s.imm.iter().any(|i| !i.mem.ranges().is_empty())
    }

    /// Newest range-delete sequence at or below `read_seq` covering
    /// `(id, user_key)` in the shared store, or `None`.
    ///
    /// The store's spans are keyed exactly as its point entries are — cf-id
    /// prefix included — so one prefixed probe answers for the right family
    /// and a span can never reach across the prefix boundary into another.
    pub(crate) fn covering_seq(&self, id: u64, user_key: &[u8], read_seq: u64) -> Option<u64> {
        let s = self.state.read();
        // One relaxed load per memtable when the feature is unused.
        if s.mem.ranges().is_empty() && s.imm.iter().all(|i| i.mem.ranges().is_empty()) {
            return None;
        }
        let pk = prefixed(id, user_key);
        let mut best = s.mem.ranges().covering_seq(&pk, read_seq);
        for imm in &s.imm {
            if let Some(seq) = imm.mem.ranges().covering_seq(&pk, read_seq) {
                best = Some(best.map_or(seq, |b: u64| b.max(seq)));
            }
        }
        best
    }

    /// Add each shared store generation separately, windowed to this family.
    /// Cursors compare user keys without copying or stripping fragment storage.
    ///
    /// Used to build a scan's range mask. The bounds handed in are user keys;
    /// the whole family's keyspace is `[id, id+1)` in prefixed order, which is
    /// what an unbounded scan clips to.
    pub(crate) fn add_range_sources(
        &self,
        mask: &mut crate::range_tombstone::RangeMask,
        id: u64,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        registry: &Arc<crate::range_tombstone::FragmentRegistry>,
    ) {
        let s = self.state.read();
        let lo = prefixed(id, lower.unwrap_or_default());
        // The exclusive top of this family's keyspace is the next cf id; a
        // family with id `u64::MAX` has no successor, so it clips at nothing.
        let hi = match upper {
            Some(u) => Some(prefixed(id, u)),
            None => id.checked_add(1).map(|next| next.to_be_bytes().to_vec()),
        };
        for mem in std::iter::once(&s.mem).chain(s.imm.iter().map(|i| &i.mem)) {
            if !mem.ranges().is_empty() {
                mem.ranges().track_snapshots(registry);
                mask.push_snapshot(
                    mem.ranges().fragment_snapshot(),
                    &default_comparator(),
                    Some(&lo),
                    hi.as_deref(),
                    Some(id.to_be_bytes()),
                );
            }
        }
    }

    pub(crate) fn range_cache_stats(&self) -> crate::range_tombstone::RangeCacheStats {
        let s = self.state.read();
        let mut stats = s.mem.ranges().stats();
        for imm in &s.imm {
            stats += imm.mem.ranges().stats();
        }
        stats
    }

    /// Walk every version of `user_key` for column family `id` visible at
    /// `read_seq`, newest first, across the active store and its immutables.
    /// See [`crate::memtable::Memtable::chain`].
    ///
    /// Unlike [`get`](Self::get) this cannot stop at the first store that has
    /// the key: an operand chain can straddle a rotation, so the older
    /// immutables have to be walked too. Ordering across stores is restored by
    /// the caller, which sorts the gathered versions by sequence.
    pub(crate) fn chain(
        &self,
        id: u64,
        user_key: &[u8],
        read_seq: u64,
        now: i64,
        mut f: impl FnMut(u64, u64, Option<&[u8]>) -> bool,
    ) {
        let pk = prefixed(id, user_key);
        let s = self.state.read();
        let mut stop = false;
        s.mem.chain(&pk, read_seq, now, |seq, kind, value| {
            let go = f(seq, kind, value);
            stop = !go;
            go
        });
        for imm in s.imm.iter().rev() {
            if stop {
                return;
            }
            imm.mem.chain(&pk, read_seq, now, |seq, kind, value| {
                let go = f(seq, kind, value);
                stop = !go;
                go
            });
        }
    }

    /// Extract a column family's entries (prefix stripped) for an iterator
    /// overlay; ordering is the caller's responsibility.
    pub(crate) fn entries_for_cf(&self, id: u64) -> Vec<Entry> {
        let prefix = id.to_be_bytes();
        let mut out = Vec::new();
        let collect = |snap: Vec<Entry>, out: &mut Vec<Entry>| {
            for e in snap {
                if e.user_key.len() >= 8 && e.user_key[..8] == prefix {
                    out.push(Entry {
                        user_key: e.user_key[8..].to_vec(),
                        ..e
                    });
                }
            }
        };
        let s = self.state.read();
        collect(s.mem.snapshot(), &mut out);
        for imm in &s.imm {
            collect(imm.mem.snapshot(), &mut out);
        }
        out
    }

    /// Return one lazy prefix iterator for the active shared memtable and each
    /// immutable predecessor. Valid only for bytewise column-family ordering.
    pub(crate) fn iterators_for_cf(&self, id: u64) -> Vec<UnifiedMemIter> {
        let s = self.state.read();
        let mut out = Vec::with_capacity(1 + s.imm.len());
        out.push(UnifiedMemIter::new(s.mem.clone(), id));
        out.extend(
            s.imm
                .iter()
                .rev()
                .map(|imm| UnifiedMemIter::new(imm.mem.clone(), id)),
        );
        out
    }

    /// Seal the active memtable and enqueue a split flush.
    pub(crate) fn rotate(self: &Arc<Self>, force: bool) {
        let imm = {
            let mut g = self.rot.lock();
            while g.rotating {
                self.cond.wait(&mut g);
            }
            {
                let s = self.state.read();
                // Range tombstones alone are enough to rotate: they are not
                // point entries, so `is_empty` would drop them on the floor.
                if s.mem.is_empty_including_ranges() {
                    return;
                }
                if !force && s.mem.approx_size() < self.write_buffer_size as i64 {
                    return;
                }
            }
            g.rotating = true;
            while g.active_writers > 0 {
                self.cond.wait(&mut g);
            }
            let old_wal;
            let imm;
            {
                let mut s = self.state.write();
                let old_mem = std::mem::replace(&mut s.mem, Memtable::new(default_comparator()));
                imm = Arc::new(UnifiedImm {
                    mem: old_mem,
                    wal_paths: s.pending_wals.clone(),
                });
                s.imm.push(imm.clone());
                old_wal = s.wal.take();
                s.wal_gen += 1;
                let new_path = wal_path(&self.dir, s.wal_gen);
                s.wal = if self.read_only {
                    None
                } else {
                    Wal::open(&new_path, self.sync_mode, self.sync_interval)
                        .ok()
                        .map(|w| {
                            w.set_poison(self.poison.clone());
                            w.set_sync_counter(self.wal_syncs.clone());
                            Arc::new(w)
                        })
                };
                s.pending_wals = vec![new_path];
            }
            if let Some(w) = old_wal {
                let _ = w.close();
            }
            g.rotating = false;
            self.cond.notify_all();
            imm
        };
        self.pending_flush.fetch_add(1, Ordering::SeqCst);
        if self.flush_tx.send(FlushJob::Unified { imm }).is_err() {
            self.pending_flush.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Sealed immutables still waiting for a flush, oldest first. Read-only
    /// snapshot helper: with no flush worker, this is where a read-only open's
    /// WAL-replayed data stays.
    pub(crate) fn sealed(&self) -> Vec<Arc<UnifiedImm>> {
        self.state.read().imm.clone()
    }

    /// Remove a flushed immutable from the queue.
    pub(crate) fn remove_imm(&self, imm: &Arc<UnifiedImm>) {
        // Match the writer predicate's lock order (`rot` then `state`) so a
        // completion cannot notify between a writer's predicate check and wait.
        let _g = self.rot.lock();
        let mut s = self.state.write();
        if let Some(pos) = s.imm.iter().position(|i| Arc::ptr_eq(i, imm)) {
            imm.mem.ranges().retire();
            s.imm.remove(pos);
        }
        drop(s);
        self.cond.notify_all();
    }

    /// fsync the active WAL (no-op when read-only / WAL-less).
    pub(crate) fn sync_wal(&self) -> crate::error::Result<()> {
        let wal = self.state.read().wal.clone();
        match wal {
            Some(w) => w.sync(),
            None => Ok(()),
        }
    }

    /// Close the active WAL's files while leaving the handle in place, so the
    /// next append fails with `InvalidDb` rather than being routed to a fresh
    /// WAL. Test lever; see `DB::close_unified_wal_for_tests`.
    pub(crate) fn close_wal_for_tests(&self) {
        if let Some(w) = self.state.read().wal.as_ref() {
            let _ = w.close();
        }
    }

    /// Close the active WAL (called on database close, after the queue drains).
    pub(crate) fn close(&self) {
        let mut s = self.state.write();
        if let Some(w) = s.wal.take() {
            let _ = w.close();
        }
    }
}

/// Split a sealed unified memtable's entries by column-family id, in key order.
/// Returns `(cf_id, entries)` pairs; entries keep the unified (bytewise) order.
pub(crate) fn split_by_cf(imm: &UnifiedImm) -> Vec<(u64, Vec<Entry>)> {
    let snap = imm.mem.snapshot(); // bytewise: grouped by 8-byte id prefix
    let mut groups: Vec<(u64, Vec<Entry>)> = Vec::new();
    for e in snap {
        if e.user_key.len() < 8 {
            continue;
        }
        let id = u64::from_be_bytes(e.user_key[..8].try_into().unwrap());
        let stripped = Entry {
            user_key: e.user_key[8..].to_vec(),
            ..e
        };
        match groups.last_mut() {
            Some((gid, v)) if *gid == id => v.push(stripped),
            _ => groups.push((id, vec![stripped])),
        }
    }
    groups
}

/// Split a sealed unified memtable's **range tombstones** by column-family id,
/// already fragmented and with the 8-byte prefix stripped.
///
/// A span never crosses a cf-id boundary (asserted at encode), so every
/// fragment belongs to exactly one family and the split is a partition.
pub(crate) fn split_ranges_by_cf(
    imm: &UnifiedImm,
) -> Vec<(u64, Vec<crate::range_tombstone::Fragment>)> {
    if imm.mem.ranges().is_empty() {
        return Vec::new();
    }
    let mut groups: Vec<(u64, Vec<crate::range_tombstone::Fragment>)> = Vec::new();
    for mut f in imm.mem.ranges().fragments(None, None) {
        if f.start.len() < 8 {
            continue;
        }
        let id = u64::from_be_bytes(f.start[..8].try_into().unwrap());
        debug_assert_eq!(
            f.start[..8],
            f.end[..8],
            "a range fragment may never cross a cf-id prefix"
        );
        f.start.drain(..8);
        f.end.drain(..8);
        match groups.last_mut() {
            Some((gid, v)) if *gid == id => v.push(f),
            _ => groups.push((id, vec![f])),
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
    use std::time::Duration;

    fn record<'a>(key: &'a [u8], seq: u64) -> wal::RecordRef<'a> {
        wal::RecordRef {
            key,
            value: b"value",
            seq,
            ttl: 0,
            kind: crate::format::KIND_PUT,
        }
    }

    /// Schema 2 keeps the 8-byte big-endian CF id **inside** the key, so an
    /// envelope-written unified WAL replays into the memtable byte-for-byte the
    /// way the legacy one does — same prefixed key, same per-CF view.
    #[test]
    fn envelope_schema2_keeps_cf_prefix_in_key() {
        let dir = tempfile::tempdir().unwrap();
        let id = cf_id("posts");
        let path = wal_path(dir.path().to_str().unwrap(), 0);
        {
            let w = crate::wal::Wal::open(
                &path,
                crate::config::SyncMode::None,
                std::time::Duration::ZERO,
            )
            .unwrap();
            let key = prefixed(id, b"hello");
            w.append_batch_enveloped(crate::wal::ENVELOPE_SCHEMA_UNIFIED, &[record(&key, 5)])
                .unwrap();
            w.close().unwrap();
        }

        let opts = Options {
            unified_memtable: true,
            ..Options::new(dir.path().to_str().unwrap())
        };
        let (flush_tx, _flush_rx) = unbounded();
        let (store, max_seq, _) = UnifiedStore::open(
            dir.path().to_str().unwrap(),
            &opts,
            flush_tx,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::util::Poison::new()),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();

        assert_eq!(max_seq, 5);
        // The replayed record reached the memtable under its prefixed key: the
        // per-CF view finds it, and it belongs to no other family.
        let hit = store.get(id, b"hello", u64::MAX, 0);
        assert!(
            hit.found,
            "the prefixed key must reach the memtable unchanged"
        );
        assert_eq!(hit.value, b"value");
        assert!(!store.get(cf_id("other"), b"hello", u64::MAX, 0).found);
    }

    /// Build a store with a large write buffer, so nothing rotates by accident.
    fn test_store(dir: &tempfile::TempDir) -> Arc<UnifiedStore> {
        let opts = Options {
            unified_memtable: true,
            ..Options::new(dir.path().to_str().unwrap())
        };
        let (flush_tx, flush_rx) = unbounded();
        // The receiver outlives the store, so a rotation's send cannot fail
        // silently and skew a test that is about the WAL rather than the flush.
        std::mem::forget(flush_rx);
        let (store, _, _) = UnifiedStore::open(
            dir.path().to_str().unwrap(),
            &opts,
            flush_tx,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::util::Poison::new()),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
        store
    }

    #[test]
    fn cached_range_sources_survive_rotation_and_overlap() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            unified_memtable: true,
            ..Options::new(dir.path().to_str().unwrap())
        };
        let (flush_tx, flush_rx) = unbounded();
        let (store, _, _) = UnifiedStore::open(
            dir.path().to_str().unwrap(),
            &opts,
            flush_tx,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::util::Poison::new()),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
        store
            .apply_with_ranges(
                &[],
                &[(
                    7,
                    wal::RangeRef {
                        start: b"f",
                        end: b"z",
                        seq: 5,
                    },
                )],
            )
            .unwrap();
        let registry = Arc::new(crate::range_tombstone::FragmentRegistry::default());
        let mut old = crate::range_tombstone::RangeMask::default();
        store.add_range_sources(&mut old, 7, None, None, &registry);
        let old_bytes = registry.stats().0;
        store.rotate(true);
        assert_eq!(store.state.read().imm.len(), 1);
        store
            .apply_with_ranges(
                &[],
                &[(
                    7,
                    wal::RangeRef {
                        start: b"a",
                        end: b"m",
                        seq: 10,
                    },
                )],
            )
            .unwrap();
        let cmp = default_comparator();
        for _ in 0..100 {
            let mut mask = crate::range_tombstone::RangeMask::default();
            store.add_range_sources(&mut mask, 7, Some(b"g"), Some(b"y"), &registry);
            for (key, seq, want) in [
                (b"g", 5, Some(5)),
                (b"g", 10, Some(10)),
                (b"x", 10, Some(5)),
                (b"g", 9, Some(5)),
            ] {
                assert_eq!(mask.covering_seq(&cmp, key, seq), want);
            }
        }
        assert_eq!(old.covering_seq(&cmp, b"g", 10), Some(5));
        assert_eq!(store.range_cache_stats().builds, 2);
        assert_eq!(store.range_cache_stats().hits, 199);
        let bytes = old_bytes;
        let imm = match flush_rx.recv().unwrap() {
            FlushJob::Unified { imm } => imm,
            FlushJob::PerCf { .. } => panic!("expected unified flush"),
        };
        store.remove_imm(&imm);
        assert_eq!(registry.stats().1, bytes);
        drop(imm);
        assert_eq!(registry.stats().1, bytes);
        drop(old);
        assert_eq!(registry.stats().1, 0);
    }

    fn wal_len(dir: &tempfile::TempDir, gen: u64) -> u64 {
        let base = wal_path(dir.path().to_str().unwrap(), gen);
        (0..8)
            .map(|k| {
                let p = if k == 0 {
                    base.clone()
                } else {
                    format!("{base}.s{k}")
                };
                std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
            })
            .sum()
    }

    /// The whole reason `apply_memtable_only` exists: it must NOT append to the
    /// WAL. Replaying a prepared writeset through the ordinary apply would put a
    /// second full copy of it in the log, which recovery would then have to
    /// reconcile against the decision that already describes it.
    #[test]
    fn apply_memtable_only_writes_no_wal() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(&dir);
        // An ordinary apply first, so the file is non-empty and a later append
        // would be visible as growth rather than as creation.
        store
            .apply_with_ranges(&[(7, record(b"committed", 1))], &[])
            .unwrap();
        let before = wal_len(&dir, 0);
        assert!(before > 0, "the ordinary apply wrote to the WAL");

        store
            .apply_memtable_only(&[(7, record(b"prepared", 0))], 2)
            .unwrap();
        assert_eq!(wal_len(&dir, 0), before, "the memtable-only apply wrote WAL");

        // ...and it did reach the memtable, at the sequence the caller named.
        let hit = store.get(7, b"prepared", u64::MAX, 0);
        assert!(hit.found);
        assert_eq!(hit.seq, 2, "the record lands at `start + slot`");
        assert!(!store.get(7, b"prepared", 1, 0).found, "and not below it");
    }

    /// Invariant 9: a writer holds `active_writers` for its whole apply, and a
    /// rotation waits for the drain before swapping the memtable. The
    /// memtable-only path does the same bookkeeping as the ordinary one, so a
    /// concurrent rotation blocks until it returns.
    #[test]
    fn apply_memtable_only_holds_active_writers() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(&dir);
        store
            .apply_with_ranges(&[(7, record(b"seed", 1))], &[])
            .unwrap();

        // Occupy the writer slot the way an in-flight apply does, then check
        // that a rotation cannot proceed.
        store.rot.lock().active_writers += 1;
        let rotator = store.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            rotator.rotate(true);
            tx.send(()).unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "a rotation must wait for the active writer to drain"
        );
        {
            let mut g = store.rot.lock();
            g.active_writers -= 1;
            store.cond.notify_all();
        }
        rx.recv_timeout(Duration::from_secs(2))
            .expect("the rotation proceeds once the writer drains");
        join.join().unwrap();
    }

    /// The records are in the memtable at their assigned sequences, so they
    /// become visible exactly when the caller publishes the range — not before.
    #[test]
    fn apply_memtable_only_is_visible_after_publish() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(&dir);
        store
            .apply_memtable_only(&[(7, record(b"a", 0)), (7, record(b"b", 0))], 10)
            .unwrap();

        // A reader at the pre-publication watermark sees nothing...
        assert!(!store.get(7, b"a", 9, 0).found);
        assert!(!store.get(7, b"b", 9, 0).found);
        // ...and one at the published watermark sees the whole block.
        assert!(store.get(7, b"a", 11, 0).found);
        assert!(store.get(7, b"b", 11, 0).found);
        assert_eq!(store.get(7, b"a", 11, 0).seq, 10);
        assert_eq!(store.get(7, b"b", 11, 0).seq, 11);
    }

    #[test]
    fn unified_writers_stall_at_the_immutable_threshold_and_resume_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let (flush_tx, flush_rx) = unbounded();
        let opts = Options {
            unified_memtable: true,
            unified_memtable_write_buffer_size: 1,
            unified_memtable_stall_threshold: 6,
            ..Options::new(dir.path().to_str().unwrap())
        };
        let (store, _, _) = UnifiedStore::open(
            dir.path().to_str().unwrap(),
            &opts,
            flush_tx,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::util::Poison::new()),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();

        for seq in 1..=6 {
            let key = format!("key-{seq}");
            store
                .apply_with_ranges(&[(7, record(key.as_bytes(), seq))], &[])
                .unwrap();
        }

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = store.clone();
        let join = std::thread::spawn(move || {
            let result = writer.apply_with_ranges(&[(7, record(b"blocked", 7))], &[]);
            done_tx.send(result).unwrap();
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "the seventh immutable must stall its writer"
        );

        let imm = match flush_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            FlushJob::Unified { imm } => imm,
            FlushJob::PerCf { .. } => panic!("unexpected per-CF flush"),
        };
        store.remove_imm(&imm);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer resumes after immutable removal")
            .unwrap();
        join.join().unwrap();
    }
}
