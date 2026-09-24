//! Standalone read snapshots (wavesdb `SnapshotHandle`).
//!
//! A [`SnapshotHandle`] is a read view pinned at one sequence **outside** a
//! transaction: no write buffer, no read set, no conflict detection — just the
//! pin. It registers in the same `DbInner::snapshots` refcount a
//! `RepeatableRead`/`Snapshot`/`Serializable` transaction uses, so compaction's
//! version GC (`oldest_snapshot()`) retains every version the handle can see
//! for exactly as long as the handle is alive, and excise and span-index
//! pruning respect it the same way.
//!
//! Cloning is cheap and shares the pin: the sequence is released when the last
//! clone drops (or [`SnapshotHandle::release`] consumes it), never earlier.

use std::ops::Bound;
use std::sync::Arc;

use crate::column_family::ColumnFamily;
use crate::db::{DbInner, DB};
use crate::error::{OndaError, Result};
use crate::iterator::Iterator;

/// The registered pin. One per [`DB::snapshot`] call, however many handles
/// share it.
struct Pin {
    db: Arc<DbInner>,
    seq: u64,
}

impl Drop for Pin {
    fn drop(&mut self) {
        self.db.release_snapshot(self.seq);
    }
}

/// A refcounted read snapshot, created by [`DB::snapshot`].
///
/// Every read through it — [`get`](Self::get), [`multi_get`](Self::multi_get),
/// [`new_iterator`](Self::new_iterator) — sees
/// the database exactly as of [`seq`](Self::seq), however many commits,
/// flushes and compactions land afterwards.
///
/// An iterator created from a handle pins the tables and memtables it reads,
/// so it stays consistent even if the handle is dropped first; what the handle
/// adds is that *new* reads at its sequence stay possible.
///
/// Hold handles briefly: while one is alive, compaction must keep every version
/// newer than its sequence, so a forgotten handle grows the database the way a
/// forgotten transaction does.
#[derive(Clone)]
pub struct SnapshotHandle {
    pin: Arc<Pin>,
}

impl std::fmt::Debug for SnapshotHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotHandle")
            .field("seq", &self.pin.seq)
            .finish()
    }
}

impl SnapshotHandle {
    /// The sequence this snapshot reads at.
    pub fn seq(&self) -> u64 {
        self.pin.seq
    }

    /// Release this handle now. The pin itself goes when the **last** clone
    /// does — this is `drop`, spelled out for call sites that want it visible.
    pub fn release(self) {}

    /// Point read as of the snapshot. `NotFound` for a missing, deleted or
    /// TTL-expired key, exactly as [`DB::get`].
    pub fn get(&self, cf: &Arc<ColumnFamily>, key: &[u8]) -> Result<Vec<u8>> {
        cf.get(key, self.pin.seq)
    }

    /// [`get`](Self::get), appending the value to `buf` instead of allocating
    /// one; see [`DB::get_into`]. Returns the value's length.
    pub fn get_into(&self, cf: &Arc<ColumnFamily>, key: &[u8], buf: &mut Vec<u8>) -> Result<usize> {
        cf.get_into(key, self.pin.seq, buf)
    }

    /// Batched point read as of the snapshot; see [`DB::multi_get`].
    pub fn multi_get(&self, cf: &Arc<ColumnFamily>, keys: &[&[u8]]) -> Vec<Result<Vec<u8>>> {
        cf.multi_get(keys, self.pin.seq)
    }

    /// Iterator over `cf` as of the snapshot.
    pub fn new_iterator(&self, cf: &Arc<ColumnFamily>) -> Iterator {
        self.new_iterator_bounded(cf, Bound::Unbounded, Bound::Unbounded)
    }

    /// [`new_iterator`](Self::new_iterator) with declared key bounds; see
    /// [`Txn::new_iterator_bounded`](crate::Txn::new_iterator_bounded).
    pub fn new_iterator_bounded(
        &self,
        cf: &Arc<ColumnFamily>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Iterator {
        cf.new_iterator(self.pin.seq, None, (lower, upper))
    }

    /// Refuse a handle minted by another database: its sequence means nothing
    /// here, and it does not pin this database's versions.
    fn check_owner(&self, db: &Arc<DbInner>) -> Result<()> {
        if Arc::ptr_eq(&self.pin.db, db) {
            Ok(())
        } else {
            Err(OndaError::InvalidArgs(
                "snapshot handle belongs to a different database".into(),
            ))
        }
    }
}

impl DB {
    /// Pin a read snapshot at the current published sequence.
    ///
    /// The snapshot is taken at the gap-free watermark (`visible_seq`), raised
    /// to this thread's own last commit exactly as a `Snapshot` transaction's
    /// is — a snapshot taken right after a `put` sees that `put`. It is
    /// registered atomically with reading the watermark, so no compaction can
    /// observe a newer "oldest snapshot" in between and collect a version the
    /// handle is entitled to.
    pub fn snapshot(&self) -> SnapshotHandle {
        let seq = self.inner.acquire_fixed_snapshot();
        SnapshotHandle {
            pin: Arc::new(Pin {
                db: self.inner.clone(),
                seq,
            }),
        }
    }

    /// [`get`](Self::get) as of `snapshot`. `InvalidArgs` if the handle came
    /// from another database.
    pub fn get_at(
        &self,
        cf: &Arc<ColumnFamily>,
        key: &[u8],
        snapshot: &SnapshotHandle,
    ) -> Result<Vec<u8>> {
        snapshot.check_owner(&self.inner)?;
        snapshot.get(cf, key)
    }

    /// Iterator over `cf` as of `snapshot`, with declared key bounds (pass
    /// `Bound::Unbounded` twice for a full scan). A handle from another
    /// database yields an iterator that is invalid from the start and reports
    /// `InvalidArgs` through [`Iterator::err`].
    pub fn new_iterator_at(
        &self,
        cf: &Arc<ColumnFamily>,
        snapshot: &SnapshotHandle,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Iterator {
        if let Err(error) = snapshot.check_owner(&self.inner) {
            return Iterator::failed(cf.comparator().clone(), error);
        }
        snapshot.new_iterator_bounded(cf, lower, upper)
    }
}
