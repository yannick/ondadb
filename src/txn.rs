//! Transactions and the single-op convenience API.
//!
//! A transaction buffers writes until commit, at which point the database
//! assigns a contiguous block of sequence numbers, appends to each touched
//! column family's WAL and memtable, and publishes the batch.  Five isolation
//! levels are supported; `Snapshot`/`RepeatableRead`/`Serializable` pin a read
//! sequence at `begin`, and `Snapshot`/`Serializable` perform write-write conflict
//! detection on commit. `Serializable` additionally validates that every key read
//! *by point lookup* is unchanged since the snapshot — it does not track
//! range/iterator reads, so phantoms are not detected (it is not full SSI). See
//! [`IsolationLevel::Serializable`](crate::IsolationLevel).

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::column_family::{ColumnFamily, CommitOp};
use crate::config::IsolationLevel;
use crate::db::{DbInner, DB};
use crate::error::{OndaError, Result};
use crate::iterator::Iterator;
use crate::memtable::Memtable;
use crate::perf::PerfContext;
use crate::util::now_nanos;
use crate::wal::RecordRef;

/// Per-column-family commit group: handle, WAL records (borrowing the txn
/// buffer), hook ops, and whether the CF has a commit hook installed (hook ops
/// are only built when it does).
type CfGroup<'a> = (
    Arc<ColumnFamily>,
    Vec<RecordRef<'a>>,
    Vec<crate::wal::RangeRef<'a>>,
    Vec<CommitOp>,
    bool,
);

/// `(offset, len)` range into the transaction's write buffer.
type BufRange = (usize, usize);

struct PreparedCommit {
    order: Vec<usize>,
}

struct CommitApplication {
    hooks: Vec<(Arc<ColumnFamily>, Vec<CommitOp>)>,
    error: Option<OndaError>,
}

/// Slice `buf` at `r`. A free function (not a method) so callers can hold other
/// borrows of the transaction at the same time.
#[inline]
fn buf_slice(buf: &[u8], r: BufRange) -> &[u8] {
    &buf[r.0..r.0 + r.1]
}

struct WriteEntry {
    cf: Arc<ColumnFamily>,
    key: BufRange,
    value: BufRange,
    ttl: i64,
    /// Record kind; see [`crate::wal::Record::kind`]. Kind 5 (1.2's range
    /// delete) means `key` holds `start` and `value` holds `end`, reusing the
    /// same two arena slots the wire format's generic `a`/`b` slots use; `ttl`
    /// is meaningless there and left at its default.
    kind: u64,
}

impl WriteEntry {
    #[inline]
    fn tombstone(&self) -> bool {
        self.kind == crate::format::KIND_DELETE || self.kind == crate::format::KIND_SINGLE_DELETE
    }
    #[inline]
    fn is_merge(&self) -> bool {
        self.kind == crate::format::KIND_MERGE
    }
    #[inline]
    fn is_range(&self) -> bool {
        self.kind == crate::format::KIND_RANGE_DELETE
    }
}

/// What a transaction's own buffer says about one key of a merge family.
struct BufferedChain {
    /// The last base the buffer holds for the key: `Some(Some(v))` a put,
    /// `Some(None)` a delete, `None` no buffered base at all.
    base: Option<Option<Vec<u8>>>,
    /// Operands buffered after that base, oldest first.
    operands: Vec<Vec<u8>>,
}

/// The range deletes this transaction has buffered, as `(cf, start, end)`
/// borrowed from the arena.
fn buffered_ranges<'a>(
    buf: &'a [u8],
    writes: &'a [WriteEntry],
) -> impl std::iter::Iterator<Item = (&'a Arc<ColumnFamily>, &'a [u8], &'a [u8])> {
    writes
        .iter()
        .filter(|w| w.is_range())
        .map(move |w| (&w.cf, buf_slice(buf, w.key), buf_slice(buf, w.value)))
}

/// A multi-operation transaction.
pub struct Txn {
    db: Arc<DbInner>,
    isolation: IsolationLevel,
    read_seq: u64,
    fixed: bool,
    snapshot_held: bool,
    /// Arena holding every buffered key and value back-to-back; `WriteEntry`
    /// stores ranges into it. One grow-only allocation instead of two `Vec`s
    /// per operation.
    buf: Vec<u8>,
    writes: Vec<WriteEntry>,
    read_set: HashSet<(usize, Vec<u8>)>,
    /// First-insertion order for `read_set`, allowing savepoint rollback to
    /// remove only reads performed after that savepoint.
    read_log: Vec<(usize, Vec<u8>)>,
    /// CF handles for every key in `read_set`, so commit-time validation can call
    /// `peek_seq` even on CFs the transaction only read (never wrote).
    read_cfs: HashMap<usize, Arc<ColumnFamily>>,
    /// Named savepoints: `(name, writes_len, buf_len, read_log_len)`.
    savepoints: Vec<(String, usize, usize, usize)>,
    done: bool,
}

impl std::fmt::Debug for Txn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Txn")
            .field("isolation", &self.isolation)
            .field("read_seq", &self.read_seq)
            .field("writes", &self.writes.len())
            .finish()
    }
}

fn cf_id(cf: &Arc<ColumnFamily>) -> usize {
    Arc::as_ptr(cf) as usize
}

thread_local! {
    /// Recycled transaction write buffers. A fresh `Vec` grows by doubling —
    /// re-copying the accumulated payload — on every batch; a recycled buffer
    /// arrives with yesterday's capacity and never grows again in steady
    /// state.
    static BUF_POOL: std::cell::RefCell<Vec<Vec<u8>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Buffer-pool retention caps: don't pin unboundedly large one-off buffers,
/// and keep at most a few per thread.
const BUF_POOL_MAX_CAP: usize = 32 << 20;
const BUF_POOL_MAX_LEN: usize = 4;

fn take_buf() -> Vec<u8> {
    BUF_POOL
        .with(|p| p.borrow_mut().pop())
        .map(|mut b| {
            b.clear();
            b
        })
        .unwrap_or_default()
}

fn put_buf(buf: Vec<u8>) {
    if buf.capacity() == 0 || buf.capacity() > BUF_POOL_MAX_CAP {
        return;
    }
    BUF_POOL.with(|p| {
        let mut g = p.borrow_mut();
        if g.len() < BUF_POOL_MAX_LEN {
            g.push(buf);
        }
    });
}

/// Validate one `delete_range` call: capability, non-nil bounds, and
/// `start < end` under the column family's comparator.
///
/// The comparator matters: `start >= end` is asked of the CF's order, not of
/// the byte order, so a reversed-collation family accepts exactly the intervals
/// its own iterators would walk.
fn validate_range_bounds(
    db: &Arc<DbInner>,
    cf: &Arc<ColumnFamily>,
    start: &[u8],
    end: &[u8],
) -> Result<()> {
    if db.caps() & crate::format::CAP_RANGE_DELETES == 0 {
        return Err(OndaError::InvalidArgs(
            "range deletes require the CAP_RANGE_DELETES format capability;              call DB::enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)              once, before the first delete_range"
                .into(),
        ));
    }
    if start.is_empty() || end.is_empty() {
        return Err(OndaError::InvalidArgs(
            "delete_range bounds must be non-empty: an empty end would delete              nothing and an empty start is indistinguishable from absent"
                .into(),
        ));
    }
    if !cf.comparator().compare(start, end).is_lt() {
        return Err(OndaError::InvalidArgs(format!(
            "delete_range start {start:?} must sort strictly before end {end:?}              under this column family's comparator (the interval is half-open)"
        )));
    }
    Ok(())
}

fn ttl_to_abs(ttl: Duration) -> i64 {
    if ttl.is_zero() {
        0
    } else {
        now_nanos().saturating_add(ttl.as_nanos() as i64)
    }
}

impl DB {
    /// Begin a transaction at the default (Snapshot) isolation level.
    pub fn begin(&self) -> Txn {
        self.begin_with_isolation(IsolationLevel::Snapshot)
    }

    /// Begin a transaction at a specific isolation level.
    pub fn begin_with_isolation(&self, level: IsolationLevel) -> Txn {
        let fixed = matches!(
            level,
            IsolationLevel::RepeatableRead
                | IsolationLevel::Snapshot
                | IsolationLevel::Serializable
        );
        // ReadCommitted floats on the read floor (read-your-own-writes);
        // fixed-snapshot levels pin the gap-free published watermark — but
        // never BELOW this thread's own last commit: a snapshot predating
        // the caller's own serial write makes the conflict check refuse
        // against itself (see `wait_visible_at_own_floor`). Gaps are
        // transient, so this waits them out rather than weakening the
        // conflict check.
        let read_seq = if fixed {
            self.inner.wait_visible_at_own_floor();
            self.inner.visible_seq()
        } else {
            self.inner.read_floor_seq()
        };
        if fixed {
            self.inner.acquire_snapshot(read_seq);
        }
        Txn {
            db: self.inner.clone(),
            isolation: level,
            read_seq,
            fixed,
            snapshot_held: fixed,
            buf: take_buf(),
            writes: Vec::new(),
            read_set: HashSet::new(),
            read_log: Vec::new(),
            read_cfs: HashMap::new(),
            savepoints: Vec::new(),
            done: false,
        }
    }

    /// Put a single key (auto-committed at ReadCommitted).
    pub fn put(
        &self,
        cf: &Arc<ColumnFamily>,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<()> {
        let mut t = self.begin_with_isolation(IsolationLevel::ReadCommitted);
        t.put(cf, key, value, ttl)?;
        t.commit()
    }

    /// Get a single key at the latest committed sequence (raised to this
    /// thread's own last commit — read-your-own-writes).
    pub fn get(&self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<Vec<u8>> {
        cf.get(key, self.inner.read_floor_seq())
    }

    /// [`get`](Self::get), with the read path's [`PerfContext`] for this one
    /// operation.
    ///
    /// The counters are collected on the calling thread and cost nothing to the
    /// threads not measuring; see [`crate::perf`] for the thread-affinity rules.
    pub fn get_with_perf(
        &self,
        cf: &Arc<ColumnFamily>,
        key: &[u8],
    ) -> (Result<Vec<u8>>, PerfContext) {
        let scope = crate::perf::enter();
        let r = self.get(cf, key);
        (r, scope.finish())
    }

    /// Resolve many keys of one column family in a single pass.
    ///
    /// Returns one result per key, in input order, so `values.len() ==
    /// keys.len()`; an empty input yields an empty output. Duplicate keys keep
    /// their own positions and share the work underneath. Each result is what
    /// [`get`](Self::get) would have returned for that key — including
    /// [`OndaError::NotFound`] for a missing, deleted or TTL-expired key — with
    /// two differences that only a batch can have:
    ///
    /// * **One snapshot.** The whole batch reads at one sequence, against one
    ///   snapshot of the memtables and levels, with one clock reading for TTL.
    ///   A flush or compaction running concurrently cannot make two keys of the
    ///   same call disagree; N separate `get`s can.
    /// * **Isolated failures.** A table that fails to open or whose block fails
    ///   its checksum errors only the keys whose resolution needed it. A key a
    ///   strictly newer source had already resolved still returns its value,
    ///   where `get` would have propagated the error.
    ///
    /// The win is deduplicated IO: candidate tables are opened and filtered
    /// once per table rather than once per key, and each distinct data block is
    /// fetched and decompressed once however many of the batch's keys land in
    /// it ([`PerfContext::multiget_blocks_deduped`] counts the savings). Bloom
    /// membership and the index binary search are inherently per key and stay
    /// per key.
    ///
    /// ```no_run
    /// # use ondadb::{DB, Options, ColumnFamilyConfig};
    /// # let db = DB::open(Options::new("/tmp/db")).unwrap();
    /// # let cf = db.create_column_family("default", ColumnFamilyConfig::default()).unwrap();
    /// for (key, value) in ["a".as_bytes(), b"b"].iter().zip(db.multi_get(&cf, &[b"a", b"b"])) {
    ///     match value {
    ///         Ok(v) => println!("{key:?} = {v:?}"),
    ///         Err(e) => println!("{key:?}: {e}"),
    ///     }
    /// }
    /// ```
    pub fn multi_get(&self, cf: &Arc<ColumnFamily>, keys: &[&[u8]]) -> Vec<Result<Vec<u8>>> {
        cf.multi_get(keys, self.inner.read_floor_seq())
    }

    /// [`multi_get`](Self::multi_get), with the read path's [`PerfContext`] for
    /// the batch as a whole.
    pub fn multi_get_with_perf(
        &self,
        cf: &Arc<ColumnFamily>,
        keys: &[&[u8]],
    ) -> (Vec<Result<Vec<u8>>>, PerfContext) {
        let scope = crate::perf::enter();
        let r = self.multi_get(cf, keys);
        (r, scope.finish())
    }

    /// Delete a single key (auto-committed at ReadCommitted).
    pub fn delete(&self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<()> {
        let mut t = self.begin_with_isolation(IsolationLevel::ReadCommitted);
        t.delete(cf, key)?;
        t.commit()
    }

    /// Delete every key in the half-open comparator interval `[start, end)`
    /// (auto-committed at ReadCommitted).
    ///
    /// One record, whatever the interval covers — the point of the feature. The
    /// deletion is recorded at a single sequence number and masks every key in
    /// the range written **at or below** it; a later write to a covered key is
    /// visible again, exactly as it would be after a point tombstone.
    ///
    /// `end` is never itself deleted. Both bounds must be non-empty and
    /// `start` must sort strictly before `end` under `cf`'s comparator;
    /// anything else is [`OndaError::InvalidArgs`].
    ///
    /// The database must have durably enabled
    /// [`CAP_RANGE_DELETES`](crate::format::CAP_RANGE_DELETES) — see
    /// [`DB::enable_format_capabilities`] — before the first call. Enabling it
    /// is one-way and makes the database unreadable by binaries older than 1.2.
    ///
    /// A commit containing a range delete takes the database-wide commit lock
    /// even at [`IsolationLevel::ReadCommitted`], so its span check and its
    /// installation are atomic against every conflict-checking commit. Range
    /// commits are therefore measurably more expensive than point commits; they
    /// are meant to be rare and bulk.
    pub fn delete_range(&self, cf: &Arc<ColumnFamily>, start: &[u8], end: &[u8]) -> Result<()> {
        let mut t = self.begin_with_isolation(IsolationLevel::ReadCommitted);
        t.delete_range(cf, start, end)?;
        t.commit()
    }

    /// Append a merge operand for `key` (auto-committed at ReadCommitted).
    ///
    /// The point of the feature: this is one append, with no read of the
    /// current value and no conflict window around it, where the equivalent
    /// read-modify-write costs a snapshot `get` plus a write-write conflict
    /// check. `cf` must have been created with a
    /// [`merge_operator_name`](crate::ColumnFamilyConfig::merge_operator_name).
    pub fn merge(&self, cf: &Arc<ColumnFamily>, key: &[u8], operand: &[u8]) -> Result<()> {
        let mut t = self.begin_with_isolation(IsolationLevel::ReadCommitted);
        t.merge(cf, key, operand)?;
        t.commit()
    }
}

impl Txn {
    fn buffer(&mut self, cf: &Arc<ColumnFamily>, key: &[u8], value: &[u8], ttl: i64, kind: u64) {
        let koff = self.buf.len();
        self.buf.extend_from_slice(key);
        let voff = self.buf.len();
        self.buf.extend_from_slice(value);
        self.writes.push(WriteEntry {
            cf: cf.clone(),
            key: (koff, key.len()),
            value: (voff, value.len()),
            ttl,
            kind,
        });
    }

    /// Buffer a range delete of `[start, end)`.
    ///
    /// Validation is here rather than at commit because every failure mode is a
    /// property of the call alone: reversed or empty bounds, and the capability.
    /// The one check that *cannot* live here — a point write of this
    /// transaction landing inside this span — needs the whole write set and is
    /// made at commit ([`Txn::check_own_range_overlap`]).
    pub fn delete_range(&mut self, cf: &Arc<ColumnFamily>, start: &[u8], end: &[u8]) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        validate_range_bounds(&self.db, cf, start, end)?;
        let soff = self.buf.len();
        self.buf.extend_from_slice(start);
        let eoff = self.buf.len();
        self.buf.extend_from_slice(end);
        self.writes.push(WriteEntry {
            cf: cf.clone(),
            key: (soff, start.len()),
            value: (eoff, end.len()),
            ttl: 0,
            kind: crate::format::KIND_RANGE_DELETE,
        });
        Ok(())
    }

    /// Newest buffered range delete covering `key` in `cf`, if any.
    ///
    /// Read-your-writes for range deletes: a key covered by a span this
    /// transaction has buffered reads as absent, exactly as a buffered point
    /// tombstone does. One `any` over a list that is empty for every
    /// transaction that never called `delete_range`.
    fn own_range_covers(&self, cf: &Arc<ColumnFamily>, key: &[u8]) -> bool {
        let id = cf_id(cf);
        let cmp = cf.comparator();
        buffered_ranges(&self.buf, &self.writes).any(|(c, start, end)| {
            cf_id(c) == id && cmp.compare(start, key).is_le() && cmp.compare(end, key).is_gt()
        })
    }

    /// Reject a buffered point write that falls inside one of this
    /// transaction's own range spans.
    ///
    /// **Precise, not conservative**: `put(a)` beside `delete_range(m..z)` is
    /// accepted; only an actual containment is refused.
    ///
    /// The reason is write *ordering*, not ties.
    /// [`deduplicated_write_order`](Self::deduplicated_write_order) gives a key
    /// the slot of its **first** insertion while taking its **last** value, so
    /// `put(k,v1); delete_range(k..z); put(k,v2)` would put the range at a
    /// higher sequence than the put and mask `v2` — a value the caller wrote
    /// *after* the range delete. Sequences inside one commit are distinct by
    /// construction (`apply_prepared` assigns `start + slot`), so this is the
    /// only hazard, and v1 refuses it outright.
    fn check_own_range_overlap(&self) -> Result<()> {
        for w in self.writes.iter().filter(|w| !w.is_range()) {
            let key = buf_slice(&self.buf, w.key);
            let id = cf_id(&w.cf);
            let cmp = w.cf.comparator();
            for (c, start, end) in buffered_ranges(&self.buf, &self.writes) {
                if cf_id(c) == id
                    && cmp.compare(start, key).is_le()
                    && cmp.compare(end, key).is_gt()
                {
                    return Err(OndaError::InvalidArgs(format!(
                        "write to key {key:?} falls inside this transaction's own \
                         range delete [{start:?}, {end:?}); v1 rejects the \
                         combination because the range would be ordered after \
                         the key's first write and could mask a later one"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Buffer a put.
    pub fn put(
        &mut self,
        cf: &Arc<ColumnFamily>,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        self.buffer(cf, key, value, ttl_to_abs(ttl), crate::format::KIND_PUT);
        Ok(())
    }

    /// Buffer a delete (tombstone).
    pub fn delete(&mut self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        self.buffer(cf, key, &[], 0, crate::format::KIND_DELETE);
        Ok(())
    }

    /// Buffer a single-delete marker for format compatibility. It currently
    /// has conservative ordinary-tombstone semantics; compaction does not yet
    /// implement the single-delete collapse optimization.
    pub fn single_delete(&mut self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        self.buffer(cf, key, &[], 0, crate::format::KIND_SINGLE_DELETE);
        Ok(())
    }

    /// Buffer a merge operand for `key`.
    ///
    /// Operands compose rather than replace: the value a reader sees is
    /// `full_merge(key, base, operands-oldest-first)`. A merge conflicts
    /// exactly like a write on the same key under
    /// [`Snapshot`](crate::IsolationLevel::Snapshot) and
    /// [`Serializable`](crate::IsolationLevel::Serializable) — it *is* a write,
    /// and conflict detection reads the newest sequence of the key without
    /// looking at kinds at all.
    ///
    /// Operands carry no TTL in v1: a per-operand expiry would resurrect the
    /// base it was folded into.
    pub fn merge(&mut self, cf: &Arc<ColumnFamily>, key: &[u8], operand: &[u8]) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        if cf.merge_op().is_none() {
            return Err(OndaError::InvalidArgs(format!(
                "column family {:?} has no merge operator; set \
                 ColumnFamilyConfig::merge_operator_name when creating it",
                cf.name()
            )));
        }
        if !cf.merge_writes_enabled() {
            // Unreachable through `create_column_family`, which takes the
            // capability before the family exists; a read-only handle is the
            // one way to hold an operator without the permission to write one.
            return Err(OndaError::ReadOnly(
                "merge operands require CAP_MERGE_OPERANDS, which this database has not enabled"
                    .into(),
            ));
        }
        self.buffer(cf, key, operand, 0, crate::format::KIND_MERGE);
        Ok(())
    }

    /// This transaction's buffered chain for `key` in column family `id`, in
    /// write order: the last base it buffered (if any) and every operand
    /// buffered after that base.
    ///
    /// `None` means the buffer says nothing about the key. `Some((None, ops))`
    /// means the buffer holds only operands, so the base still has to come from
    /// the store — which is exactly "overlay merges append to the buffered
    /// chain ahead of the committed ones".
    #[allow(clippy::type_complexity)]
    fn buffered_chain(&self, id: usize, key: &[u8]) -> Option<BufferedChain> {
        let mut seen = false;
        let mut base: Option<Option<Vec<u8>>> = None;
        let mut operands: Vec<Vec<u8>> = Vec::new();
        for w in &self.writes {
            if cf_id(&w.cf) != id || buf_slice(&self.buf, w.key) != key {
                continue;
            }
            seen = true;
            if w.is_merge() {
                operands.push(buf_slice(&self.buf, w.value).to_vec());
            } else if w.tombstone() {
                base = Some(None);
                operands.clear();
            } else {
                base = Some(Some(buf_slice(&self.buf, w.value).to_vec()));
                operands.clear();
            }
        }
        seen.then_some(BufferedChain { base, operands })
    }

    /// Record that this transaction is about to read `key` from the store, and
    /// return the sequence to read it at.
    ///
    /// Under [`IsolationLevel::Serializable`] that record is the read set
    /// commit-time validation replays; under every other level it is a no-op.
    fn note_store_read(&mut self, cf: &Arc<ColumnFamily>, id: usize, key: &[u8]) -> u64 {
        if self.isolation == IsolationLevel::Serializable {
            let read = (id, key.to_vec());
            if self.read_set.insert(read.clone()) {
                self.read_log.push(read);
            }
            self.read_cfs.entry(id).or_insert_with(|| cf.clone());
        }
        if self.fixed {
            self.read_seq
        } else {
            self.db.read_floor_seq()
        }
    }

    /// Fold a buffered operand chain onto `committed`, the value the store
    /// resolves to (already folded over the committed operands, if any).
    fn fold_buffered(
        cf: &Arc<ColumnFamily>,
        key: &[u8],
        committed: Option<&[u8]>,
        operands: &[Vec<u8>],
    ) -> Result<Vec<u8>> {
        let op = cf.merge_op().expect("caller checked the operator");
        let refs: Vec<&[u8]> = operands.iter().map(Vec::as_slice).collect();
        op.full_merge(key, committed, &refs).map_err(|e| {
            OndaError::Corruption(format!(
                "merge operator {:?} failed for key {:?}: {e}",
                op.name(),
                String::from_utf8_lossy(key)
            ))
        })
    }

    /// Read a key, honoring the transaction's own buffered writes.
    pub fn get(&mut self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<Vec<u8>> {
        let id = cf_id(cf);
        // Read-your-writes on a merge family: the buffered chain is resolved
        // first, and only a chain with no buffered base still needs the store —
        // in which case this *is* a read of the store and is recorded as one.
        if cf.merge_op().is_some() {
            if let Some(chain) = self.buffered_chain(id, key) {
                if chain.operands.is_empty() {
                    return match chain.base.expect("a chain with no operand has a base") {
                        Some(value) => Ok(value),
                        None => Err(OndaError::NotFound),
                    };
                }
                if let Some(base) = chain.base {
                    return Self::fold_buffered(cf, key, base.as_deref(), &chain.operands);
                }
                let rs = self.note_store_read(cf, id, key);
                let committed = match cf.get(key, rs) {
                    Ok(value) => Some(value),
                    Err(OndaError::NotFound) => None,
                    Err(e) => return Err(e),
                };
                return Self::fold_buffered(cf, key, committed.as_deref(), &chain.operands);
            }
        }
        // Read-your-writes: scan the buffer backward for the latest write.
        for w in self.writes.iter().rev() {
            if !w.is_range() && cf_id(&w.cf) == id && buf_slice(&self.buf, w.key) == key {
                if w.tombstone() {
                    return Err(OndaError::NotFound);
                }
                return Ok(buf_slice(&self.buf, w.value).to_vec());
            }
        }
        // Read-your-writes for range deletes. Reached only when no buffered
        // point write matched: own-write overlap is rejected at commit, so the
        // two can never both apply to one key.
        if self.own_range_covers(cf, key) {
            return Err(OndaError::NotFound);
        }
        if self.isolation == IsolationLevel::Serializable {
            let read = (id, key.to_vec());
            if self.read_set.insert(read.clone()) {
                self.read_log.push(read);
            }
            self.read_cfs.entry(id).or_insert_with(|| cf.clone());
        }
        let rs = if self.fixed {
            self.read_seq
        } else {
            self.db.read_floor_seq()
        };
        cf.get(key, rs)
    }

    /// [`get`](Self::get), with the read path's [`PerfContext`] for this one
    /// operation.
    pub fn get_with_perf(
        &mut self,
        cf: &Arc<ColumnFamily>,
        key: &[u8],
    ) -> (Result<Vec<u8>>, PerfContext) {
        let scope = crate::perf::enter();
        let r = self.get(cf, key);
        (r, scope.finish())
    }

    /// [`DB::multi_get`](crate::DB::multi_get), honoring this transaction's own
    /// buffered writes.
    ///
    /// A key the transaction has written resolves from the buffer alone
    /// (last write wins, as in [`get`](Self::get)) and never reaches the store;
    /// the rest are resolved as one batch at the transaction's read sequence.
    /// Under [`IsolationLevel::Serializable`] this records exactly the read-set
    /// entries N [`get`](Self::get)s of the same keys would have — buffered
    /// keys excluded, since reading one's own write is not a read of the store.
    pub fn multi_get(&mut self, cf: &Arc<ColumnFamily>, keys: &[&[u8]]) -> Vec<Result<Vec<u8>>> {
        let id = cf_id(cf);
        // A merge family's buffer can hold operands that still need the store's
        // base, which the "buffered or pending, never both" split below cannot
        // express. Resolving those keys one by one loses the batch's block
        // dedup but keeps its semantics identical to N `get`s — and a batch
        // over a family with no operator is untouched.
        if cf.merge_op().is_some() && !self.writes.is_empty() {
            return keys.iter().map(|key| self.get(cf, key)).collect();
        }
        let mut out: Vec<Option<Result<Vec<u8>>>> = (0..keys.len()).map(|_| None).collect();
        // Indices still to resolve from the store, and their keys — built in
        // input order so the batch's results scatter straight back.
        let mut pending_idx: Vec<usize> = Vec::new();
        let mut pending: Vec<&[u8]> = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            // Read-your-writes: scan the buffer backward for the latest write.
            let buffered =
                self.writes.iter().rev().find(|w| {
                    !w.is_range() && cf_id(&w.cf) == id && buf_slice(&self.buf, w.key) == *key
                });
            if let Some(w) = buffered {
                out[i] = Some(if w.tombstone() {
                    Err(OndaError::NotFound)
                } else {
                    Ok(buf_slice(&self.buf, w.value).to_vec())
                });
                continue;
            }
            if self.own_range_covers(cf, key) {
                out[i] = Some(Err(OndaError::NotFound));
                continue;
            }
            if self.isolation == IsolationLevel::Serializable {
                let read = (id, key.to_vec());
                if self.read_set.insert(read.clone()) {
                    self.read_log.push(read);
                }
                self.read_cfs.entry(id).or_insert_with(|| cf.clone());
            }
            pending_idx.push(i);
            pending.push(key);
        }

        let rs = if self.fixed {
            self.read_seq
        } else {
            self.db.read_floor_seq()
        };
        for (i, r) in pending_idx.into_iter().zip(cf.multi_get(&pending, rs)) {
            out[i] = Some(r);
        }
        out.into_iter()
            .map(|r| r.expect("every position is resolved from the buffer or the batch"))
            .collect()
    }

    /// [`multi_get`](Self::multi_get), with the read path's [`PerfContext`] for
    /// the batch as a whole.
    pub fn multi_get_with_perf(
        &mut self,
        cf: &Arc<ColumnFamily>,
        keys: &[&[u8]],
    ) -> (Vec<Result<Vec<u8>>>, PerfContext) {
        let scope = crate::perf::enter();
        let r = self.multi_get(cf, keys);
        (r, scope.finish())
    }

    /// Create a snapshot iterator over `cf` that includes this transaction's
    /// buffered writes.
    pub fn new_iterator(&self, cf: &Arc<ColumnFamily>) -> Iterator {
        self.new_iterator_bounded(cf, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
    }

    /// Like [`new_iterator`](Self::new_iterator), with declared key bounds.
    ///
    /// SSTables whose key range lies entirely outside `[lower, upper]` are
    /// skipped at construction — for a scan touching a narrow key range this
    /// avoids opening (and seeking, i.e. reading a block of) every table in
    /// every level. The iterator also terminates at the bounds: forward
    /// iteration goes invalid at the first key past `upper`, backward at the
    /// first key below `lower`. Seeking outside the declared bounds yields
    /// unspecified (but memory-safe) results.
    pub fn new_iterator_bounded(
        &self,
        cf: &Arc<ColumnFamily>,
        lower: std::ops::Bound<&[u8]>,
        upper: std::ops::Bound<&[u8]>,
    ) -> Iterator {
        let rs = if self.fixed {
            self.read_seq
        } else {
            self.db.read_floor_seq()
        };
        let id = cf_id(cf);
        // Read-only transactions (the overwhelmingly common case for scans)
        // must not construct a throwaway overlay memtable per iterator.
        let overlay: Option<Arc<Memtable>> = if self.writes.is_empty() {
            None
        } else if cf.merge_op().is_some() {
            match self.merge_overlay(cf, id, rs) {
                Ok(overlay) => overlay,
                Err(error) => return Iterator::failed(cf.comparator().clone(), error),
            }
        } else {
            let mem = Memtable::new(cf.comparator().clone());
            let mut any = false;
            for w in &self.writes {
                if cf_id(&w.cf) != id {
                    continue;
                }
                if w.is_range() {
                    // At `rs`, so the overlay's spans mask exactly what the
                    // transaction can already see plus its own writes.
                    mem.add_range(
                        buf_slice(&self.buf, w.key),
                        buf_slice(&self.buf, w.value),
                        rs,
                    );
                } else {
                    mem.put_ref(
                        buf_slice(&self.buf, w.key),
                        buf_slice(&self.buf, w.value),
                        rs,
                        w.ttl,
                        w.kind,
                    );
                }
                any = true;
            }
            if any {
                Some(mem)
            } else {
                None
            }
        };
        cf.new_iterator(rs, overlay, (lower, upper))
    }

    /// Build the overlay memtable for a scan over a merge family.
    ///
    /// Every buffered write of this transaction lands at the same sequence
    /// (`rs`), which is what makes last-write-wins work for puts — the memtable
    /// simply overwrites the internal key. Operands cannot share a sequence
    /// that way: they compose, so a second one at the same key would replace
    /// the first instead of extending the chain. So the buffered chain is
    /// **pre-folded** here, one point read per key the transaction merged, and
    /// the overlay carries a single ordinary put per key. That keeps the merge
    /// iterator free of any overlay special case, at the cost of those reads —
    /// which the scan would have paid anyway.
    fn merge_overlay(
        &self,
        cf: &Arc<ColumnFamily>,
        id: usize,
        rs: u64,
    ) -> Result<Option<Arc<Memtable>>> {
        // Keys this transaction merged. Everything else replays exactly as the
        // ordinary overlay does, TTL and single-delete kind included.
        let mut merged: Vec<&[u8]> = Vec::new();
        let mut any = false;
        for w in &self.writes {
            if cf_id(&w.cf) != id {
                continue;
            }
            any = true;
            let key = buf_slice(&self.buf, w.key);
            if w.is_merge() && !merged.contains(&key) {
                merged.push(key);
            }
        }
        if !any {
            return Ok(None);
        }
        let mem = Memtable::new(cf.comparator().clone());
        for w in &self.writes {
            let key = buf_slice(&self.buf, w.key);
            if cf_id(&w.cf) != id || merged.contains(&key) {
                continue;
            }
            mem.put_ref(key, buf_slice(&self.buf, w.value), rs, w.ttl, w.kind);
        }
        for key in merged {
            let chain = self
                .buffered_chain(id, key)
                .expect("the key came out of the buffer");
            let committed = match chain.base {
                Some(base) => base,
                // No buffered base: the chain continues into the store, which
                // `cf.get` folds for us.
                None => match cf.get(key, rs) {
                    Ok(value) => Some(value),
                    Err(OndaError::NotFound) => None,
                    Err(e) => return Err(e),
                },
            };
            if chain.operands.is_empty() {
                // A base written *after* the last operand supersedes it.
                match committed {
                    Some(value) => mem.put_ref(key, &value, rs, 0, crate::format::KIND_PUT),
                    None => mem.put_ref(key, &[], rs, 0, crate::format::KIND_DELETE),
                }
                continue;
            }
            let folded = Self::fold_buffered(cf, key, committed.as_deref(), &chain.operands)?;
            mem.put_ref(key, &folded, rs, 0, crate::format::KIND_PUT);
        }
        Ok(Some(mem))
    }

    /// Name a savepoint at the current buffer position.
    pub fn set_savepoint(&mut self, name: &str) -> Result<()> {
        self.savepoints.push((
            name.to_string(),
            self.writes.len(),
            self.buf.len(),
            self.read_log.len(),
        ));
        Ok(())
    }

    /// Roll back to a named savepoint, discarding writes made since.
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self
            .savepoints
            .iter()
            .rposition(|(n, _, _, _)| n == name)
            .ok_or_else(|| OndaError::InvalidArgs(format!("no savepoint {name}")))?;
        let (_, wlen, blen, read_len) = self.savepoints[pos];
        self.writes.truncate(wlen);
        self.buf.truncate(blen);
        for read in &self.read_log[read_len..] {
            self.read_set.remove(read);
        }
        self.read_log.truncate(read_len);
        let live_cfs: HashSet<usize> = self.read_set.iter().map(|(id, _)| *id).collect();
        self.read_cfs.retain(|id, _| live_cfs.contains(id));
        self.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// Release a named savepoint (and any nested after it).
    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self
            .savepoints
            .iter()
            .rposition(|(n, _, _, _)| n == name)
            .ok_or_else(|| OndaError::InvalidArgs(format!("no savepoint {name}")))?;
        self.savepoints.truncate(pos);
        Ok(())
    }

    /// [`deduplicated_write_order`](Self::deduplicated_write_order) for a
    /// transaction that buffered at least one merge operand.
    ///
    /// Operands **compose**, so collapsing a key to its last write would commit
    /// one operand and silently drop the rest — and would drop the delete in
    /// `delete(k); merge(k)`, which is the base the operand folds against. What
    /// is genuinely superseded is everything before the key's last *base*, so a
    /// key commits the run starting there: identical to the collapsed order
    /// whenever the run has length one, which is every key of every
    /// merge-free transaction.
    fn merge_aware_write_order(&self) -> Vec<usize> {
        let mut runs: Vec<Vec<usize>> = Vec::new();
        let mut slot_of: HashMap<(usize, &[u8]), usize, xxhash_rust::xxh3::Xxh3DefaultBuilder> =
            HashMap::with_capacity_and_hasher(
                self.writes.len(),
                xxhash_rust::xxh3::Xxh3DefaultBuilder::new(),
            );
        for (index, write) in self.writes.iter().enumerate() {
            match slot_of.entry((cf_id(&write.cf), buf_slice(&self.buf, write.key))) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(runs.len());
                    runs.push(vec![index]);
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let run = &mut runs[*entry.get()];
                    if write.is_merge() {
                        run.push(index);
                    } else {
                        run.clear();
                        run.push(index);
                    }
                }
            }
        }
        runs.concat()
    }

    fn deduplicated_write_order(&self) -> Vec<usize> {
        if self.writes.len() == 1 {
            return vec![0];
        }
        // Only a merge family can produce writes that must not collapse, and
        // the scan is over writes the caller is about to iterate anyway.
        if self.writes.iter().any(WriteEntry::is_merge) {
            return self.merge_aware_write_order();
        }
        // Keys borrow the transaction arena: deduplication allocates only the
        // slot map and order vector, never key/value copies.
        let mut slot_of: HashMap<(usize, &[u8]), usize, xxhash_rust::xxh3::Xxh3DefaultBuilder> =
            HashMap::with_capacity_and_hasher(
                self.writes.len(),
                xxhash_rust::xxh3::Xxh3DefaultBuilder::new(),
            );
        let mut order = Vec::with_capacity(self.writes.len());
        for (index, write) in self.writes.iter().enumerate() {
            // Two range deletes sharing a start bound are distinct records, and
            // a range delete must never collapse into a point write whose key
            // happens to equal its start bound: both take their own slot, and
            // therefore their own sequence.
            if write.is_range() {
                order.push(index);
                continue;
            }
            match slot_of.entry((cf_id(&write.cf), buf_slice(&self.buf, write.key))) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(order.len());
                    order.push(index);
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    order[*entry.get()] = index;
                }
            }
        }
        order
    }

    fn prepare_commit(&self) -> PreparedCommit {
        PreparedCommit {
            order: self.deduplicated_write_order(),
        }
    }

    fn validate_write_conflicts(&self, prepared: &PreparedCommit) -> Result<()> {
        for &index in &prepared.order {
            let write = &self.writes[index];
            let key = buf_slice(&self.buf, write.key);
            if write.cf.peek_seq(key)? > self.read_seq {
                return Err(OndaError::Conflict(format!(
                    "write-write conflict on key {:?}",
                    key.to_vec()
                )));
            }
        }
        Ok(())
    }

    fn validate_read_conflicts(&self) -> Result<()> {
        for (id, key) in &self.read_set {
            let Some(cf) = self.read_cfs.get(id) else {
                continue;
            };
            if cf.peek_seq(key)? > self.read_seq {
                return Err(OndaError::Conflict("read-set changed".into()));
            }
        }
        Ok(())
    }

    /// Conflict checks the span index owns, under `commit_mu`.
    ///
    /// A **range** writer conflicts with any overlapping marker newer than its
    /// read sequence — the only way to learn that something in the interval
    /// changed, since there is no key to `peek_seq`. A **point** writer
    /// additionally checks newer *covering range* markers; point-vs-point stays
    /// with `peek_seq`, which sees the maximum sequence at a key whatever wrote
    /// it.
    fn validate_span_conflicts(&self, prepared: &PreparedCommit) -> Result<()> {
        let Some(index) = self.db.span_index() else {
            return Ok(());
        };
        for &i in &prepared.order {
            let w = &self.writes[i];
            let cf = w.cf.id();
            let cmp = w.cf.comparator();
            let a = buf_slice(&self.buf, w.key);
            if w.is_range() {
                let b = buf_slice(&self.buf, w.value);
                if let Some(seq) = index.range_conflict(cf, cmp, a, b, self.read_seq) {
                    return Err(OndaError::Conflict(format!(
                        "range delete [{a:?}, {b:?}) overlaps a write committed at                          sequence {seq}, after this transaction's snapshot"
                    )));
                }
            } else if let Some(seq) = index.point_conflict(cf, cmp, a, self.read_seq) {
                return Err(OndaError::Conflict(format!(
                    "write to key {a:?} is covered by a range delete committed at                      sequence {seq}, after this transaction's snapshot"
                )));
            }
        }
        Ok(())
    }

    /// Record this commit's writes in the span index. Called under `commit_mu`,
    /// after every record is installed.
    fn insert_span_markers(
        &self,
        prepared: &PreparedCommit,
        start: u64,
        reservation: &mut crate::span_index::SpanReservation<'_>,
    ) {
        let Some(index) = self.db.span_index() else {
            return;
        };
        for (slot, &i) in prepared.order.iter().enumerate() {
            let w = &self.writes[i];
            let seq = start + slot as u64;
            let a = buf_slice(&self.buf, w.key);
            if w.is_range() {
                index.insert_range(
                    reservation,
                    w.cf.id(),
                    a,
                    buf_slice(&self.buf, w.value),
                    seq,
                );
            } else {
                index.insert_point(reservation, w.cf.id(), a, seq);
            }
        }
    }

    fn validate_commit(&self, prepared: &PreparedCommit, needs_write_check: bool) -> Result<()> {
        if needs_write_check {
            self.validate_write_conflicts(prepared)?;
        }
        if self.isolation == IsolationLevel::Serializable {
            self.validate_read_conflicts()?;
        }
        Ok(())
    }

    fn apply_prepared(&self, prepared: &PreparedCommit, start: u64) -> CommitApplication {
        let mut hooks = Vec::new();
        let mut groups: HashMap<usize, CfGroup<'_>> = HashMap::with_capacity(1);
        for (slot, &index) in prepared.order.iter().enumerate() {
            let seq = start + slot as u64;
            let write = &self.writes[index];
            let id = cf_id(&write.cf);
            let group = groups.entry(id).or_insert_with(|| {
                let has_hook = write.cf.has_commit_hook();
                (
                    write.cf.clone(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    has_hook,
                )
            });
            let key = buf_slice(&self.buf, write.key);
            let value = buf_slice(&self.buf, write.value);
            if write.is_range() {
                // Commit hooks describe point writes (`CommitOp` has one key
                // and one value); a range delete has two keys and no value, so
                // v1 does not surface it to hooks rather than inventing a
                // shape a hook could misread as a put.
                group.2.push(crate::wal::RangeRef {
                    start: key,
                    end: value,
                    seq,
                });
                continue;
            }
            if group.4 {
                group.3.push(CommitOp {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    tombstone: write.tombstone(),
                    ttl: write.ttl,
                });
            }
            group.1.push(RecordRef {
                key,
                value,
                seq,
                ttl: write.ttl,
                kind: write.kind,
            });
        }
        let error = if let Some(unified) = &self.db.unified {
            Self::apply_unified_groups(unified, groups, &mut hooks)
        } else {
            Self::apply_per_cf_groups(groups, &mut hooks)
        };
        CommitApplication { hooks, error }
    }

    fn apply_unified_groups(
        unified: &Arc<crate::unified::UnifiedStore>,
        groups: HashMap<usize, CfGroup<'_>>,
        hooks: &mut Vec<(Arc<ColumnFamily>, Vec<CommitOp>)>,
    ) -> Option<OndaError> {
        let item_count: usize = groups
            .values()
            .map(|(_, records, _, _, _)| records.len())
            .sum();
        let range_count: usize = groups
            .values()
            .map(|(_, _, ranges, _, _)| ranges.len())
            .sum();
        let mut items = Vec::with_capacity(item_count);
        let mut ranges = Vec::with_capacity(range_count);
        for (_, (cf, records, cf_ranges, ops, has_hook)) in groups {
            let cf_id = cf.id();
            items.extend(records.into_iter().map(|record| (cf_id, record)));
            ranges.extend(cf_ranges.into_iter().map(|range| (cf_id, range)));
            if has_hook {
                hooks.push((cf, ops));
            }
        }
        unified.apply_with_ranges(&items, &ranges).err()
    }

    fn apply_per_cf_groups(
        groups: HashMap<usize, CfGroup<'_>>,
        hooks: &mut Vec<(Arc<ColumnFamily>, Vec<CommitOp>)>,
    ) -> Option<OndaError> {
        for (_, (cf, records, ranges, ops, has_hook)) in groups {
            if let Err(error) = cf.apply_commit_with_ranges(&records, &ranges) {
                return Some(error);
            }
            if has_hook {
                hooks.push((cf, ops));
            }
        }
        None
    }

    /// Commit the transaction.  Returns [`OndaError::Conflict`] on a
    /// serialization conflict (Snapshot/Serializable).
    pub fn commit(&mut self) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        // Fail-stop: after a durability failure no new commit may be
        // acknowledged. The transaction stays usable for rollback.
        self.db.poison.check()?;
        self.done = true;
        let needs_check = matches!(
            self.isolation,
            IsolationLevel::Snapshot | IsolationLevel::Serializable
        );

        if self.writes.is_empty() {
            self.read_set.clear();
            self.read_log.clear();
            self.read_cfs.clear();
            self.savepoints.clear();
            self.release();
            return Ok(());
        }
        let prepared = self.prepare_commit();
        if self.db.unified.is_none() {
            let first_cf = cf_id(&self.writes[prepared.order[0]].cf);
            if prepared
                .order
                .iter()
                .skip(1)
                .any(|&index| cf_id(&self.writes[index].cf) != first_cf)
            {
                self.release();
                return Err(OndaError::InvalidArgs(
                    "multi-column-family transactions require unified_memtable=true for atomic commit"
                        .into(),
                ));
            }
        }
        let has_range = self.writes.iter().any(|w| w.is_range());
        if has_range {
            if let Err(error) = self.check_own_range_overlap() {
                self.release();
                return Err(error);
            }
        }
        let db = self.db.clone();

        // The span index is inert until CAP_RANGE_DELETES is enabled, so a
        // database that never issues a range delete reserves nothing, takes no
        // lock here, and reaches `commit_mu` exactly as before.
        let tracked = db.caps() & crate::format::CAP_RANGE_DELETES != 0;
        // Capacity FIRST, with no other lock held: waiting for span-index room
        // under `commit_mu` would stall every Snapshot/Serializable commit in
        // the database behind one range writer, and could convoy against the
        // pruner.
        let mut reservation = match db.reserve_span_markers(
            if tracked { prepared.order.len() } else { 0 },
            has_range,
            self.snapshot_held.then_some(self.read_seq),
        ) {
            Ok(reservation) => reservation,
            Err(error) => {
                self.release();
                return Err(error);
            }
        };

        // A commit containing a range delete takes `commit_mu` even at
        // ReadCommitted: its span check and its marker insert must be atomic
        // against every other conflict-checking commit. Point-only commits keep
        // today's behavior exactly.
        let _guard = if needs_check || self.isolation == IsolationLevel::Serializable || has_range {
            Some(db.commit_mu.lock())
        } else {
            None
        };
        if let Err(error) = self.validate_commit(&prepared, needs_check) {
            self.release();
            return Err(error);
        }
        if needs_check {
            if let Err(error) = self.validate_span_conflicts(&prepared) {
                self.release();
                return Err(error);
            }
        }

        let n = prepared.order.len() as u64;
        let start = self.db.reserve_seq(n);
        let commit_seq = start + n - 1;
        let application = self.apply_prepared(&prepared, start);
        // The reserved range must be published even when the apply failed:
        // the gap-free cursor (invariant 5) never advances past an
        // unpublished range, so skipping this would freeze `visible_seq`
        // forever — hiding every later commit from other threads, persisting
        // a stale `global_seq`, and losing/reusing sequences after reopen.
        // Publishing a failed range is safe: its records never reached the
        // WAL or memtable, so nothing unapplied becomes visible (the same
        // publish-before-data pattern `start_ingestion` uses).
        self.db.publish_range(start, start + n);
        if let Some(error) = application.error {
            drop(_guard);
            self.release();
            return Err(error);
        }
        // Markers go in only after full installation, and still under
        // `commit_mu`, so a concurrent range writer either sees this commit or
        // is serialized behind it.
        match (tracked, &mut reservation) {
            (true, Some(reservation)) => self.insert_span_markers(&prepared, start, reservation),
            // A point-only commit that found the index full: it commits, but
            // the window it opened is no longer describable, so range writers
            // reading at or below `commit_seq` must conflict.
            (true, None) => {
                if let Some(index) = self.db.span_index() {
                    index.note_overflow(commit_seq);
                }
            }
            (false, _) => {}
        }
        // Test-only rendezvous, still under `commit_mu` (debug builds only).
        #[cfg(debug_assertions)]
        if has_range {
            crate::db::commit_park::park_if_armed(self.db.instance_id);
        }
        self.db.note_thread_commit(commit_seq);
        drop(_guard);
        // Unconsumed slots go back here as well as on the error paths; the
        // reservation's `Drop` is the backstop, this keeps the index from
        // holding capacity across the commit hooks below.
        drop(reservation);
        if tracked {
            self.db.prune_span_markers();
        }

        for (cf, ops) in &application.hooks {
            cf.run_commit_hook(commit_seq, ops);
        }
        self.writes.clear();
        self.read_set.clear();
        self.read_log.clear();
        self.read_cfs.clear();
        self.savepoints.clear();
        put_buf(std::mem::take(&mut self.buf));
        self.release();
        Ok(())
    }

    /// Discard all buffered writes.
    pub fn rollback(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        self.writes.clear();
        self.read_set.clear();
        self.read_log.clear();
        self.read_cfs.clear();
        self.savepoints.clear();
        put_buf(std::mem::take(&mut self.buf));
        self.release();
        Ok(())
    }

    /// Reset the transaction for reuse at a (possibly new) isolation level.
    pub fn reset(&mut self, level: IsolationLevel) -> Result<()> {
        if !self.done {
            self.rollback()?;
        }
        let fixed = matches!(
            level,
            IsolationLevel::RepeatableRead
                | IsolationLevel::Snapshot
                | IsolationLevel::Serializable
        );
        let read_seq = if fixed {
            self.db.wait_visible_at_own_floor();
            self.db.visible_seq()
        } else {
            self.db.read_floor_seq()
        };
        if fixed {
            self.db.acquire_snapshot(read_seq);
        }
        self.isolation = level;
        self.read_seq = read_seq;
        self.fixed = fixed;
        self.snapshot_held = fixed;
        self.writes.clear();
        if self.buf.capacity() == 0 {
            self.buf = take_buf();
        } else {
            self.buf.clear();
        }
        self.read_set.clear();
        self.read_log.clear();
        self.read_cfs.clear();
        self.savepoints.clear();
        self.done = false;
        Ok(())
    }

    fn release(&mut self) {
        if self.snapshot_held {
            self.db.release_snapshot(self.read_seq);
            self.snapshot_held = false;
        }
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        self.writes.clear();
        put_buf(std::mem::take(&mut self.buf));
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ColumnFamilyConfig;
    use crate::Options;

    #[test]
    fn reset_fixed_snapshot_waits_for_own_commit_floor() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let mut txn = db.begin_with_isolation(IsolationLevel::ReadCommitted);
        let reserved = db.inner.reserve_seq(1);
        db.inner.note_thread_commit(reserved);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let helper_barrier = barrier.clone();
        let inner = db.inner.clone();
        let helper = std::thread::spawn(move || {
            helper_barrier.wait();
            std::thread::sleep(Duration::from_millis(100));
            inner.publish_range(reserved, reserved + 1);
        });
        barrier.wait();

        txn.reset(IsolationLevel::Snapshot).unwrap();
        assert!(
            txn.read_seq >= reserved,
            "reset pinned {} below this thread's own commit floor {reserved}",
            txn.read_seq
        );
        helper.join().unwrap();
        txn.rollback().unwrap();
        db.close().unwrap();
    }

    #[test]
    fn prepared_write_order_is_last_write_wins_in_first_key_order() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        let mut txn = db.begin();
        txn.put(&cf, b"a", b"first", Duration::ZERO).unwrap();
        txn.put(&cf, b"b", b"only", Duration::ZERO).unwrap();
        txn.put(&cf, b"a", b"last", Duration::ZERO).unwrap();

        assert_eq!(txn.deduplicated_write_order(), vec![2, 1]);
    }

    #[test]
    fn txn_buf_reused_across_batches() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        // First batch grows a fresh buffer...
        let mut t = db.begin_with_isolation(IsolationLevel::ReadCommitted);
        assert_eq!(t.buf.capacity(), 0, "first txn on this thread starts cold");
        for i in 0..1000u32 {
            t.put(&cf, &i.to_be_bytes(), &[0u8; 100], Duration::ZERO)
                .unwrap();
        }
        let grown = t.buf.capacity();
        assert!(grown >= 1000 * 104);
        t.commit().unwrap();
        // ...the second arrives with that capacity from the pool: no growth.
        let mut t2 = db.begin_with_isolation(IsolationLevel::ReadCommitted);
        assert_eq!(t2.buf.capacity(), grown, "buffer not recycled");
        for i in 0..1000u32 {
            t2.put(&cf, &i.to_be_bytes(), &[0u8; 100], Duration::ZERO)
                .unwrap();
        }
        assert_eq!(t2.buf.capacity(), grown, "recycled buffer regrew");
        t2.commit().unwrap();
    }
}
