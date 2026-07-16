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
use crate::util::now_nanos;
use crate::wal::RecordRef;

/// Per-column-family commit group: handle, WAL records (borrowing the txn
/// buffer), hook ops, and whether the CF has a commit hook installed (hook ops
/// are only built when it does).
type CfGroup<'a> = (Arc<ColumnFamily>, Vec<RecordRef<'a>>, Vec<CommitOp>, bool);

/// `(offset, len)` range into the transaction's write buffer.
type BufRange = (usize, usize);

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
    tombstone: bool,
    single_delete: bool,
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
    /// CF handles for every key in `read_set`, so commit-time validation can call
    /// `peek_seq` even on CFs the transaction only read (never wrote).
    read_cfs: HashMap<usize, Arc<ColumnFamily>>,
    /// Named savepoints: `(name, writes_len, buf_len)`.
    savepoints: Vec<(String, usize, usize)>,
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
        // fixed-snapshot levels pin the gap-free published watermark.
        let read_seq = if fixed {
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

    /// Delete a single key (auto-committed at ReadCommitted).
    pub fn delete(&self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<()> {
        let mut t = self.begin_with_isolation(IsolationLevel::ReadCommitted);
        t.delete(cf, key)?;
        t.commit()
    }
}

impl Txn {
    fn buffer(
        &mut self,
        cf: &Arc<ColumnFamily>,
        key: &[u8],
        value: &[u8],
        ttl: i64,
        tombstone: bool,
        single_delete: bool,
    ) {
        let koff = self.buf.len();
        self.buf.extend_from_slice(key);
        let voff = self.buf.len();
        self.buf.extend_from_slice(value);
        self.writes.push(WriteEntry {
            cf: cf.clone(),
            key: (koff, key.len()),
            value: (voff, value.len()),
            ttl,
            tombstone,
            single_delete,
        });
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
        self.buffer(cf, key, value, ttl_to_abs(ttl), false, false);
        Ok(())
    }

    /// Buffer a delete (tombstone).
    pub fn delete(&mut self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        self.buffer(cf, key, &[], 0, true, false);
        Ok(())
    }

    /// Buffer a single-delete (a delete hint for keys written at most once).
    pub fn single_delete(&mut self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<()> {
        if self.done {
            return Err(OndaError::InvalidArgs(
                "transaction already finished".into(),
            ));
        }
        self.buffer(cf, key, &[], 0, true, true);
        Ok(())
    }

    /// Read a key, honoring the transaction's own buffered writes.
    pub fn get(&mut self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<Vec<u8>> {
        let id = cf_id(cf);
        // Read-your-writes: scan the buffer backward for the latest write.
        for w in self.writes.iter().rev() {
            if cf_id(&w.cf) == id && buf_slice(&self.buf, w.key) == key {
                if w.tombstone {
                    return Err(OndaError::NotFound);
                }
                return Ok(buf_slice(&self.buf, w.value).to_vec());
            }
        }
        if self.isolation == IsolationLevel::Serializable {
            self.read_set.insert((id, key.to_vec()));
            self.read_cfs.entry(id).or_insert_with(|| cf.clone());
        }
        let rs = if self.fixed {
            self.read_seq
        } else {
            self.db.read_floor_seq()
        };
        cf.get(key, rs)
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
        } else {
            let mem = Memtable::new(cf.comparator().clone());
            let mut any = false;
            for w in &self.writes {
                if cf_id(&w.cf) == id {
                    mem.put_ref(
                        buf_slice(&self.buf, w.key),
                        buf_slice(&self.buf, w.value),
                        rs,
                        w.ttl,
                        w.tombstone,
                        w.single_delete,
                    );
                    any = true;
                }
            }
            if any {
                Some(mem)
            } else {
                None
            }
        };
        cf.new_iterator(rs, overlay, (lower, upper))
    }

    /// Name a savepoint at the current buffer position.
    pub fn set_savepoint(&mut self, name: &str) -> Result<()> {
        self.savepoints
            .push((name.to_string(), self.writes.len(), self.buf.len()));
        Ok(())
    }

    /// Roll back to a named savepoint, discarding writes made since.
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self
            .savepoints
            .iter()
            .rposition(|(n, _, _)| n == name)
            .ok_or_else(|| OndaError::InvalidArgs(format!("no savepoint {name}")))?;
        let (_, wlen, blen) = self.savepoints[pos];
        self.writes.truncate(wlen);
        self.buf.truncate(blen);
        self.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// Release a named savepoint (and any nested after it).
    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self
            .savepoints
            .iter()
            .rposition(|(n, _, _)| n == name)
            .ok_or_else(|| OndaError::InvalidArgs(format!("no savepoint {name}")))?;
        self.savepoints.truncate(pos);
        Ok(())
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
            self.release();
            return Ok(());
        }

        // Dedup writes: last write per (cf, key) wins, sequenced in first-write
        // order. `order[slot]` holds the index (into `self.writes`) of the winning
        // write for that slot. Keys are hashed by reference — no clones. The
        // single-write case (the `DB::put`/`delete` helpers) skips the map.
        let order: Vec<usize> = if self.writes.len() == 1 {
            vec![0]
        } else {
            // xxh3 hashes the whole key in wide lanes — much cheaper than
            // SipHash for this throwaway in-process dedup map.
            let mut slot_of: HashMap<(usize, &[u8]), usize, xxhash_rust::xxh3::Xxh3DefaultBuilder> =
                HashMap::with_capacity_and_hasher(
                    self.writes.len(),
                    xxhash_rust::xxh3::Xxh3DefaultBuilder::new(),
                );
            let mut order = Vec::with_capacity(self.writes.len());
            for (i, w) in self.writes.iter().enumerate() {
                match slot_of.entry((cf_id(&w.cf), buf_slice(&self.buf, w.key))) {
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert(order.len());
                        order.push(i);
                    }
                    std::collections::hash_map::Entry::Occupied(o) => order[*o.get()] = i,
                }
            }
            order
        };

        let db = self.db.clone();
        let _guard = if needs_check || self.isolation == IsolationLevel::Serializable {
            Some(db.commit_mu.lock())
        } else {
            None
        };

        // Write-write conflict detection.
        if needs_check {
            let mut conflict: Option<Vec<u8>> = None;
            for &i in &order {
                let w = &self.writes[i];
                let key = buf_slice(&self.buf, w.key);
                if w.cf.peek_seq(key)? > self.read_seq {
                    conflict = Some(key.to_vec());
                    break;
                }
            }
            if let Some(key) = conflict {
                self.release();
                return Err(OndaError::Conflict(format!(
                    "write-write conflict on key {key:?}"
                )));
            }
        }
        // Serializable: validate the read set too. Every read key's CF handle is
        // retained in `read_cfs`, so read-only keys (in CFs the txn never wrote) are
        // validated as well — not just keys in written CFs.
        if self.isolation == IsolationLevel::Serializable {
            for (id, key) in &self.read_set {
                if let Some(cf) = self.read_cfs.get(id) {
                    if cf.peek_seq(key)? > self.read_seq {
                        self.release();
                        return Err(OndaError::Conflict("read-set changed".into()));
                    }
                }
            }
        }

        let n = order.len() as u64;
        let start = self.db.reserve_seq(n);
        let commit_seq = start + n - 1;

        // Group records per column family. Records BORROW the transaction buffer
        // (zero copies here); the WAL and memtable copy what they need. Hook
        // payloads are only materialized for CFs that actually have a hook.
        let mut applied: Vec<(Arc<ColumnFamily>, Vec<CommitOp>)> = Vec::new();
        let mut apply_err: Option<OndaError> = None;
        {
            let buf = &self.buf;
            let mut groups: HashMap<usize, CfGroup<'_>> = HashMap::with_capacity(1);
            for (slot, &i) in order.iter().enumerate() {
                let seq = start + slot as u64;
                let w = &self.writes[i];
                let id = cf_id(&w.cf);
                let entry = groups.entry(id).or_insert_with(|| {
                    let has_hook = w.cf.has_commit_hook();
                    (w.cf.clone(), Vec::new(), Vec::new(), has_hook)
                });
                let key = buf_slice(buf, w.key);
                let value = buf_slice(buf, w.value);
                if entry.3 {
                    entry.2.push(CommitOp {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        tombstone: w.tombstone,
                        ttl: w.ttl,
                    });
                }
                entry.1.push(RecordRef {
                    key,
                    value,
                    seq,
                    ttl: w.ttl,
                    tombstone: w.tombstone,
                    single_delete: w.single_delete,
                });
            }

            if let Some(u) = &self.db.unified {
                // Unified mode: write every record to the shared store in one batch.
                let mut items = Vec::with_capacity(n as usize);
                for (_, (cf, recs, ops, has_hook)) in groups {
                    let cid = cf.id();
                    for r in recs {
                        items.push((cid, r));
                    }
                    if has_hook {
                        applied.push((cf, ops));
                    }
                }
                apply_err = u.apply(&items).err();
            } else {
                for (_, (cf, recs, ops, has_hook)) in groups {
                    if let Some(e) = cf.apply_commit(&recs).err() {
                        apply_err = Some(e);
                        break;
                    }
                    if has_hook {
                        applied.push((cf, ops));
                    }
                }
            }
        }
        // The reserved range must be published even when the apply failed:
        // the gap-free cursor (invariant 5) never advances past an
        // unpublished range, so skipping this would freeze `visible_seq`
        // forever — hiding every later commit from other threads, persisting
        // a stale `global_seq`, and losing/reusing sequences after reopen.
        // Publishing a failed range is safe: its records never reached the
        // WAL or memtable, so nothing unapplied becomes visible (the same
        // publish-before-data pattern `start_ingestion` uses).
        self.db.publish_range(start, start + n);
        if let Some(e) = apply_err {
            drop(_guard);
            self.release();
            return Err(e);
        }
        self.db.note_thread_commit(start + n - 1);
        drop(_guard);

        for (cf, ops) in &applied {
            cf.run_commit_hook(commit_seq, ops);
        }
        self.writes.clear();
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
        let read_seq = self.db.visible_seq();
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
