//! Keyspace-tailing iterator: a forward-only cursor over an append-only
//! ordered keyspace that resumes where it stopped instead of being rebuilt on
//! every poll.
//!
//! The motivating workload is the queue peek described in [`crate::memtable`]:
//! a consumer repeatedly scans the head of an ordered keyspace waiting for new
//! entries. Building a fresh [`Iterator`] per poll costs one merge-heap
//! construction plus one seek per source — 1.3 ms per scan at 2k memtable
//! entries, which is what motivated `LazyMemIter`. A tail pays that only when
//! it has actually fallen behind.

use std::ops::Bound;
use std::sync::Arc;

use crate::column_family::ColumnFamily;
use crate::db::DbInner;
use crate::error::OndaError;
use crate::iterator::Iterator;

/// A forward-only iterator that can be advanced past its own end.
///
/// **This is not a change feed.** A refreshed tail may observe only keys that
/// compare strictly *greater* than the last key it yielded. An insert, update
/// or delete **at or behind** the cursor is never observed, and an
/// already-yielded key is never yielded again. Use it to consume an
/// append-only ordered keyspace; do not use it to learn what changed.
///
/// Each segment is an ordinary snapshot [`Iterator`] built at
/// `DbInner::read_floor_seq()` — the read-committed floor, captured afresh per
/// segment rather than pinned once. Refreshing a *fixed* snapshot would silently
/// break that snapshot's guarantees, which is why the tail is a `DB`-level API
/// and not a [`Txn`](crate::txn::Txn) one. Within one segment the usual
/// snapshot rules hold: writes committed after the segment was built are
/// invisible until the next [`refresh`](Self::refresh).
///
/// TTL expiry is evaluated per segment against a fresh clock read, so a
/// long-lived tail does not keep serving entries that expired hours ago.
///
/// There is deliberately no `prev` and no `seek_for_prev`: backward motion has
/// no meaning for a cursor whose whole contract is "strictly forward", so it is
/// a compile error rather than a runtime surprise.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # fn demo(db: &ondadb::DB, cf: &Arc<ondadb::ColumnFamily>) {
/// let mut tail = db.new_tailing_iterator(cf);
/// tail.seek_to_first();
/// loop {
///     while tail.valid() {
///         println!("{:?}", tail.key());
///         tail.next();
///     }
///     if tail.err().is_some() {
///         break;
///     }
///     if !tail.refresh() {
///         // Nothing new yet: this is non-blocking, so back off and retry.
///         std::thread::yield_now();
///     }
/// }
/// # }
/// ```
pub struct TailingIterator {
    db: Arc<DbInner>,
    cf: Arc<ColumnFamily>,
    /// The current segment. Exactly one is live at a time.
    it: Iterator,
    /// Last key this tail yielded, and therefore the exclusive lower bound of
    /// the next segment. `None` until something has been yielded.
    cursor: Option<Vec<u8>>,
    /// The read floor `it` was built at. A refresh that would rebuild at the
    /// same floor cannot find anything new, so it does not rebuild.
    floor: u64,
    segments: u64,
}

impl std::fmt::Debug for TailingIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailingIterator")
            .field("valid", &self.it.valid())
            .field("floor", &self.floor)
            .field("segments", &self.segments)
            .finish()
    }
}

impl TailingIterator {
    pub(crate) fn new(db: Arc<DbInner>, cf: Arc<ColumnFamily>) -> TailingIterator {
        let floor = db.read_floor_seq();
        let it = cf.new_iterator(floor, None, (Bound::Unbounded, Bound::Unbounded));
        TailingIterator {
            db,
            cf,
            it,
            cursor: None,
            floor,
            segments: 1,
        }
    }

    /// Position at the first key of the current segment.
    pub fn seek_to_first(&mut self) {
        self.it.seek_to_first();
        self.record_cursor();
    }

    /// Position at the first key >= `key` in the current segment.
    ///
    /// Seeking is a positioning operation on the *current* segment, so it can
    /// move the cursor backwards; the next [`refresh`](Self::refresh) then
    /// resumes strictly after wherever the seek left it. Seeking below a
    /// refreshed segment's lower bound yields unspecified (but memory-safe)
    /// completeness, exactly as it does for a bounded [`Iterator`].
    pub fn seek(&mut self, key: &[u8]) {
        self.it.seek(key);
        self.record_cursor();
    }

    /// Advance to the next key of the current segment.
    ///
    /// Going invalid here means the *segment* is exhausted, not the keyspace —
    /// call [`refresh`](Self::refresh) to pick up anything committed since.
    pub fn next(&mut self) {
        self.it.next();
        self.record_cursor();
    }

    /// Try to continue past the end of the current segment. Non-blocking.
    ///
    /// Rebuilds only when the segment is exhausted **and** the read-committed
    /// floor has advanced since it was built; the new segment starts strictly
    /// after the last yielded key. Returns whether the tail is positioned on an
    /// entry afterwards — `false` both when nothing was rebuilt and when the
    /// rebuilt segment turned out to be empty (a floor can advance because of
    /// writes behind the cursor, or to another column family).
    ///
    /// Both no-op paths are cheap: a `valid()` check and one atomic load.
    pub fn refresh(&mut self) -> bool {
        // Mid-segment: there is still unread data at this snapshot, and
        // rebuilding would skip it.
        if self.it.valid() {
            return false;
        }
        let floor = self.db.read_floor_seq();
        if floor <= self.floor {
            return false;
        }
        // Owned, because the old iterator (which the key may be borrowed from)
        // is dropped by the assignment below.
        let lower = match &self.cursor {
            Some(last) => Bound::Excluded(last.as_slice()),
            None => Bound::Unbounded,
        };
        self.it = self.cf.new_iterator(floor, None, (lower, Bound::Unbounded));
        self.floor = floor;
        self.segments += 1;
        // `seek_to_first` honours the declared lower bound, so this lands on
        // the first key strictly greater than the cursor.
        self.it.seek_to_first();
        self.record_cursor();
        self.it.valid()
    }

    /// Is the tail positioned on an entry?
    pub fn valid(&self) -> bool {
        self.it.valid()
    }

    /// The current key. Empty when not [`valid`](Self::valid).
    pub fn key(&self) -> &[u8] {
        self.it.key()
    }

    /// The current value. Empty when not [`valid`](Self::valid).
    pub fn value(&self) -> &[u8] {
        self.it.value()
    }

    /// The error that made the tail go invalid, if any.
    ///
    /// A segment whose sources could not all be opened is a *failed* iterator,
    /// not a short one — always check this after a walk goes invalid, or a
    /// missing table reads as "no more entries". Cleared by a later successful
    /// rebuild.
    pub fn err(&self) -> Option<&OndaError> {
        self.it.err()
    }

    /// How many underlying iterators this tail has constructed, including the
    /// first. Divided by the number of yielded entries this is the metric the
    /// type exists to lower; it is also what the tests assert no-op refreshes
    /// against.
    pub fn segments(&self) -> u64 {
        self.segments
    }

    /// Record the yielded key as the resume point.
    ///
    /// Exhaustion deliberately does **not** clear it: the cursor is where the
    /// next segment starts, and forgetting it would restart the tail from the
    /// beginning of the keyspace and re-yield everything.
    fn record_cursor(&mut self) {
        if !self.it.valid() {
            return;
        }
        // Runs once per yielded entry, so reuse the buffer rather than
        // allocating a fresh `Vec` per step. Taking it out first keeps the
        // borrow of `self.it` disjoint from the write to `self.cursor`.
        let mut cur = self.cursor.take().unwrap_or_default();
        cur.clear();
        cur.extend_from_slice(self.it.key());
        self.cursor = Some(cur);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::ColumnFamilyConfig;
    use crate::{Options, DB};

    #[test]
    fn cursor_tracks_last_yielded_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        for i in 0..5u32 {
            db.put(&cf, format!("k{i}").as_bytes(), b"v", Duration::ZERO)
                .unwrap();
        }

        let mut tail = db.new_tailing_iterator(&cf);
        assert!(tail.cursor.is_none(), "nothing yielded yet");
        tail.seek_to_first();
        assert_eq!(tail.cursor.as_deref(), Some(b"k0".as_slice()));
        for i in 1..5u32 {
            tail.next();
            assert_eq!(
                tail.cursor.as_deref(),
                Some(format!("k{i}").as_bytes()),
                "cursor must equal the last yielded key"
            );
        }

        // The exhausting step goes invalid and must leave the cursor alone —
        // it is the resume point for the next segment.
        tail.next();
        assert!(!tail.valid());
        assert_eq!(tail.cursor.as_deref(), Some(b"k4".as_slice()));

        // A seek records its landing key like any other yield.
        tail.seek(b"k2");
        assert_eq!(tail.cursor.as_deref(), Some(b"k2".as_slice()));
        db.close().unwrap();
    }
}
