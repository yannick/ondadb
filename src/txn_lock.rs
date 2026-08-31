//! Point locks for pessimistic transactions (3.3).
//!
//! An **opt-in** table of per-key ownership, taken only by transactions begun
//! with [`DB::begin_pessimistic`](crate::DB::begin_pessimistic). An optimistic
//! transaction never touches it: it neither takes locks nor waits for them, and
//! at commit it validates exactly as it always has.
//!
//! This is **not** [`crate::range_lock`]. That one is the compaction/parts span
//! lock: it has no per-owner identity and its waiters are background workers. A
//! transaction lock has an owner (the transaction id), a deadlock rule
//! (wait-die), and a hand-off order (FIFO) — none of which the span lock needs.
//! What is worth copying from it is the release discipline, and here it is
//! stronger still: the whole table releases through one funnel, `Txn::release`,
//! so every terminal path of a transaction — commit, rollback, reset, drop,
//! panic unwind, and the poison check that returns before any of them — frees
//! the locks it held.
//!
//! ## Wait-die
//!
//! Transaction ids come from one `fetch_add` counter, so a lower id is a
//! strictly older transaction and ties are impossible. A requester **older**
//! than the current holder waits; a **younger** one dies immediately with
//! [`OndaError::Conflict`]. Equal ids mean the holder *is* the requester — a
//! re-entrant acquisition, granted at once. Waits therefore always point from
//! an older transaction to a younger one, and a cycle would need the id order
//! to cycle, so there is no deadlock and no cycle detection.
//!
//! FIFO hand-off costs one extra rule. Waiters queue in arrival order, not in
//! age order, so handing the lock to the front of the queue can leave a
//! *younger* waiter behind an older new holder — exactly the edge wait-die
//! forbids, and enough to deadlock against a second key. Concretely: holder
//! T5 with waiters T2 then T4; T5 releases to T2 (FIFO), and T4 would now wait
//! on T2 while T2 may already be waiting on T4 elsewhere. So a hand-off
//! **denies** every remaining waiter younger than the new holder, with the same
//! `Conflict` a younger requester would have received had it arrived one
//! instant later. FIFO order is preserved among the survivors.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::error::{OndaError, Result};

/// One lock's identity: the column family's **durable** id (not a pointer) and
/// the user key.
///
/// The durable id is what 3.2's reservation registry keys on, so the two
/// subsystems index the same space — and a column family dropped and recreated
/// at the same address cannot inherit a stale lock, which is the bug L2 fixed
/// for `THREAD_COMMIT_FLOOR`.
pub(crate) type LockKey = (u64, Vec<u8>);

/// Waiter states. Written only under the table lock; the atomic exists so the
/// cell can be shared through an `Arc` without interior-mutability gymnastics.
const W_QUEUED: u8 = 0;
const W_GRANTED: u8 = 1;
/// Denied because the hand-off would have left this waiter behind a holder
/// younger than itself.
const W_DENIED_WAIT_DIE: u8 = 2;
/// Denied because the owner prepared: the key is a durable 3.2 reservation now,
/// and a granted lock would be a lock whose commit is guaranteed to conflict.
const W_DENIED_RESERVED: u8 = 3;
/// Denied because the database is closing or has fail-stopped.
const W_DENIED_SHUTDOWN: u8 = 4;

/// `LockEntry::owner` for an entry that outlives its owner's release only to
/// carry a `last_commit_seq` stamp forward. Transaction ids start at 1.
const NO_OWNER: u64 = 0;

/// Free-but-stamped entries tolerated before a release sweeps them.
const SWEEP_AT: usize = 64;

struct Waiter {
    txn_id: u64,
    state: AtomicU8,
}

/// What one `acquire` does, decided under an immutable borrow of the table so
/// the action that follows can take a mutable one.
enum Decision {
    /// Nothing holds the key.
    Fresh,
    /// This transaction already holds it (re-entrant): no grant happens.
    Held,
    /// Unowned, kept only for its stamp.
    Takeover(u64),
    /// Wait-die says the requester is the younger one; the value is the holder.
    Die(u64),
    /// Wait-die says the requester is the older one: queue behind the holder.
    Queue,
}

struct LockEntry {
    /// Current holder, or [`NO_OWNER`].
    owner: u64,
    /// Sequence the last owner **committed** at, or 0.
    ///
    /// Read by the next holder, which waits for publication to reach it before
    /// adopting a new snapshot; without it the next holder would refresh to a
    /// watermark that does not yet include its predecessor's write and would
    /// abort on it — the whole point of the feature, lost to a publication gap
    /// that is transient by construction (invariant 5).
    last_commit_seq: u64,
    /// Queued waiters in arrival order.
    waiters: VecDeque<Arc<Waiter>>,
    /// This entry's condvar. Per entry rather than per table so a release wakes
    /// the threads waiting for *this* key and no others; every one of them is
    /// paired with the single table mutex, which is what `parking_lot::Condvar`
    /// requires.
    cv: Arc<Condvar>,
}

#[derive(Default)]
struct Table {
    entries: HashMap<LockKey, LockEntry>,
    /// Keys held per owner, so a release is O(keys held) instead of O(table).
    held: HashMap<u64, Vec<LockKey>>,
    /// Count of entries in [`Table::entries`] with no owner — retained only for
    /// their stamp. Sweeping is amortized against this rather than against the
    /// table size, so a database with many live locks does not pay an O(n)
    /// retain on every release.
    free_stamped: usize,
    /// Set once the database closes; every acquisition then fails with a
    /// duplicate of it instead of parking on a lock nobody will release.
    shutdown: Option<OndaError>,
    /// The error the most recent mass wake-up hands its waiters. Distinct from
    /// `shutdown` because fail-stop wakes waiters without closing the table.
    wake_reason: Option<OndaError>,
}

/// The per-database point-lock table.
pub(crate) struct LockTable {
    table: Mutex<Table>,
}

impl std::fmt::Debug for LockTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let t = self.table.lock();
        f.debug_struct("LockTable")
            .field("entries", &t.entries.len())
            .field("shutdown", &t.shutdown.is_some())
            .finish()
    }
}

impl Default for LockTable {
    fn default() -> Self {
        LockTable::new()
    }
}

impl LockTable {
    pub(crate) fn new() -> LockTable {
        LockTable {
            table: Mutex::new(Table::default()),
        }
    }

    /// Take the lock on `(cf_id, key)` for `txn_id`, waiting if wait-die says
    /// this transaction is the older one.
    ///
    /// `Ok(Some(seq))` is a **grant**, carrying the sequence the previous owner
    /// committed at (0 if there was none) — what the caller's snapshot refresh
    /// waits for. `Ok(None)` means the transaction already held the key: no
    /// grant happened, so there is nothing to refresh onto, and a re-entrant
    /// acquisition must not pay for one. Fails with [`OndaError::Conflict`]
    /// when wait-die kills this transaction, and with the shutdown error once
    /// the database is closing.
    pub(crate) fn acquire(&self, txn_id: u64, cf_id: u64, key: &[u8]) -> Result<Option<u64>> {
        debug_assert_ne!(txn_id, NO_OWNER, "transaction ids start at 1");
        let mut t = self.table.lock();
        if let Some(err) = &t.shutdown {
            return Err(err.duplicate());
        }
        let k: LockKey = (cf_id, key.to_vec());
        let decision = match t.entries.get(&k) {
            None => Decision::Fresh,
            // Re-entrant: the transaction already holds this key. Granting it
            // again is not a deadlock and must not queue behind itself.
            Some(e) if e.owner == txn_id => Decision::Held,
            // An entry kept alive only for its stamp. Nobody is queued on it (a
            // hand-off never leaves waiters behind an unowned entry), so it is
            // taken outright — and its stamp is exactly what this acquisition
            // needs.
            Some(e) if e.owner == NO_OWNER => Decision::Takeover(e.last_commit_seq),
            // Younger than the holder: die rather than wait.
            Some(e) if txn_id > e.owner => Decision::Die(e.owner),
            Some(_) => Decision::Queue,
        };
        let (waiter, cv) = match decision {
            Decision::Fresh => {
                t.entries.insert(
                    k.clone(),
                    LockEntry {
                        owner: txn_id,
                        last_commit_seq: 0,
                        waiters: VecDeque::new(),
                        cv: Arc::new(Condvar::new()),
                    },
                );
                t.held.entry(txn_id).or_default().push(k);
                return Ok(Some(0));
            }
            Decision::Held => return Ok(None),
            Decision::Takeover(seq) => {
                t.entries
                    .get_mut(&k)
                    .expect("just looked it up")
                    .owner = txn_id;
                t.free_stamped = t.free_stamped.saturating_sub(1);
                t.held.entry(txn_id).or_default().push(k);
                return Ok(Some(seq));
            }
            Decision::Die(owner) => {
                return Err(OndaError::Conflict(format!(
                    "wait-die: transaction {txn_id} is younger than transaction {owner}, \
                     which holds the lock on key {key:?} in column family {cf_id}; \
                     retry with a new transaction"
                )));
            }
            Decision::Queue => {
                let waiter = Arc::new(Waiter {
                    txn_id,
                    state: AtomicU8::new(W_QUEUED),
                });
                let entry = t.entries.get_mut(&k).expect("just looked it up");
                entry.waiters.push_back(waiter.clone());
                let cv = entry.cv.clone();
                (waiter, cv)
            }
        };
        loop {
            cv.wait(&mut t);
            match waiter.state.load(Ordering::Relaxed) {
                W_GRANTED => {
                    // The hand-off recorded ownership before waking us, so the
                    // entry is ours and still present.
                    return Ok(Some(t.entries.get(&k).map_or(0, |e| e.last_commit_seq)));
                }
                W_DENIED_SHUTDOWN => {
                    return Err(t.wake_reason.as_ref().map_or_else(
                        || OndaError::InvalidDb("database is closing".into()),
                        OndaError::duplicate,
                    ));
                }
                W_DENIED_RESERVED => {
                    return Err(OndaError::Conflict(format!(
                        "the holder of key {key:?} in column family {cf_id} prepared; \
                         the key is now reserved by a prepared transaction until it is \
                         resolved with commit_prepared or abort_prepared"
                    )));
                }
                W_DENIED_WAIT_DIE => {
                    return Err(OndaError::Conflict(format!(
                        "wait-die: transaction {txn_id} is younger than the transaction \
                         the lock on key {key:?} in column family {cf_id} was handed to; \
                         retry with a new transaction"
                    )));
                }
                // A spurious wake, or a notify meant for another waiter on the
                // same entry: keep waiting.
                _ => continue,
            }
        }
    }

    /// Release every lock `txn_id` holds, handing each to its FIFO successor.
    ///
    /// `committed_at` is the sequence this transaction committed at, or `None`
    /// for a transaction that published nothing — a rollback stamps no entry,
    /// because there is nothing for the next holder to wait for. `visible_seq`
    /// is the caller's published watermark, used only to decide which stamps
    /// are still worth keeping.
    pub(crate) fn release_all(&self, txn_id: u64, committed_at: Option<u64>, visible_seq: u64) {
        let mut t = self.table.lock();
        let Some(keys) = t.held.remove(&txn_id) else {
            return;
        };
        for k in keys {
            {
                let Some(entry) = t.entries.get_mut(&k) else {
                    continue;
                };
                if entry.owner != txn_id {
                    continue;
                }
                if let Some(seq) = committed_at {
                    entry.last_commit_seq = entry.last_commit_seq.max(seq);
                }
            }
            hand_off(&mut t, &k, visible_seq);
        }
        sweep(&mut t, visible_seq);
    }

    /// Drop every lock `txn_id` holds **without** handing any of them on, and
    /// deny every queued waiter with `Conflict` (3.2 composition).
    ///
    /// Called when the owner prepares: from that instant the durable
    /// reservation is the authority, so a waiter granted the in-memory lock
    /// would be granted a lock whose commit is guaranteed to fail against that
    /// reservation. Telling it now is strictly better than telling it later.
    pub(crate) fn deny_all(&self, txn_id: u64) {
        let mut t = self.table.lock();
        let Some(keys) = t.held.remove(&txn_id) else {
            return;
        };
        for k in keys {
            if !t.entries.get(&k).is_some_and(|e| e.owner == txn_id) {
                continue;
            }
            let mut entry = t.entries.remove(&k).expect("just looked it up");
            for w in entry.waiters.drain(..) {
                w.state.store(W_DENIED_RESERVED, Ordering::Relaxed);
            }
            entry.cv.notify_all();
        }
    }

    /// Wake every parked waiter with `err`, leaving the table usable.
    ///
    /// Fail-stop calls this: a poisoned database accepts no further commit, so
    /// a waiter is waiting for a release that may never come. It does **not**
    /// refuse later acquisitions — a poisoned database still lets a transaction
    /// buffer writes and fail at `commit`, and 3.3 does not move that failure
    /// earlier for the transactions that happen to be pessimistic.
    pub(crate) fn wake_all_with_error(&self, err: OndaError) {
        let mut t = self.table.lock();
        t.wake_reason = Some(err);
        wake_every_waiter(&mut t);
    }

    /// Wake every parked waiter with `err` **and** refuse every later
    /// acquisition.
    ///
    /// `DB::close` calls this: after it the database is gone, and a caller that
    /// parked on a lock would outlive it. Releases keep working, so a `Txn`
    /// dropped after close still tidies up.
    pub(crate) fn shut_down(&self, err: OndaError) {
        let mut t = self.table.lock();
        if t.shutdown.is_none() {
            t.shutdown = Some(err.duplicate());
        }
        t.wake_reason = Some(err);
        wake_every_waiter(&mut t);
    }

    /// Does `txn_id` hold `(cf_id, key)`? Observability/test hook.
    #[cfg(test)]
    pub(crate) fn holds(&self, txn_id: u64, cf_id: u64, key: &[u8]) -> bool {
        self.table
            .lock()
            .entries
            .get(&(cf_id, key.to_vec()))
            .is_some_and(|e| e.owner == txn_id)
    }

    /// Number of queued waiters on `(cf_id, key)`. Observability/test hook.
    pub(crate) fn waiters(&self, cf_id: u64, key: &[u8]) -> usize {
        self.table
            .lock()
            .entries
            .get(&(cf_id, key.to_vec()))
            .map_or(0, |e| e.waiters.len())
    }

    /// Is any key owned? Free-but-stamped entries do not count — they are
    /// bookkeeping, not ownership. Observability/test hook.
    pub(crate) fn is_idle(&self) -> bool {
        let t = self.table.lock();
        t.held.is_empty() && t.entries.values().all(|e| e.owner == NO_OWNER)
    }
}

fn wake_every_waiter(t: &mut Table) {
    for entry in t.entries.values_mut() {
        for w in entry.waiters.drain(..) {
            w.state.store(W_DENIED_SHUTDOWN, Ordering::Relaxed);
        }
        entry.cv.notify_all();
    }
}

/// Give `k` to the front of its waiter queue, or park it unowned.
fn hand_off(t: &mut Table, k: &LockKey, visible_seq: u64) {
    let entry = t.entries.get_mut(k).expect("caller looked the entry up");
    match entry.waiters.pop_front() {
        Some(w) => {
            let owner = w.txn_id;
            entry.owner = owner;
            w.state.store(W_GRANTED, Ordering::Relaxed);
            // See the module header: FIFO can otherwise leave a younger waiter
            // waiting on an older holder, which is the one edge wait-die must
            // not have.
            entry.waiters.retain(|w| {
                if w.txn_id < owner {
                    true
                } else {
                    w.state.store(W_DENIED_WAIT_DIE, Ordering::Relaxed);
                    false
                }
            });
            entry.cv.notify_all();
            t.held.entry(owner).or_default().push(k.clone());
        }
        None => {
            // Nobody is waiting. The entry survives only if its stamp is not
            // yet published: a transaction that takes this key next has to wait
            // that publication out, and after it the stamp says nothing a plain
            // `visible_seq()` does not.
            if entry.last_commit_seq > visible_seq {
                entry.owner = NO_OWNER;
                t.free_stamped += 1;
            } else {
                t.entries.remove(k);
            }
        }
    }
}

/// Drop free entries whose stamp publication has caught up with.
fn sweep(t: &mut Table, visible_seq: u64) {
    if t.free_stamped < SWEEP_AT {
        return;
    }
    t.entries
        .retain(|_, e| e.owner != NO_OWNER || e.last_commit_seq > visible_seq);
    // Recomputed rather than decremented: cheap at this point (the retain just
    // walked the table anyway) and self-correcting.
    t.free_stamped = t.entries.values().filter(|e| e.owner == NO_OWNER).count();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    /// Spin until `f` holds, or fail. Used instead of a sleep so the tests do
    /// not encode a guess about scheduling.
    fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
        let start = Instant::now();
        while !f() {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "timed out waiting for {what}"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn fifo_grant_order() {
        let table = Arc::new(LockTable::new());
        // Holder is the youngest, so both waiters are older than it and neither
        // dies. They arrive 4 then 3 — the reverse of age order, so a grant
        // order of [4, 3] can only be FIFO.
        table.acquire(9, 0, b"k").unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));

        let mut threads = Vec::new();
        for (queued, id) in [4u64, 3u64].into_iter().enumerate() {
            let t = table.clone();
            let order = order.clone();
            threads.push(std::thread::spawn(move || {
                t.acquire(id, 0, b"k").unwrap();
                order.lock().push(id);
                t.release_all(id, None, 0);
            }));
            // Park this waiter before the next one is spawned, so arrival order
            // is the test's to control and not the scheduler's.
            wait_until("the waiter to queue", || {
                table.waiters(0, b"k") == queued + 1
            });
        }
        table.release_all(9, None, 0);
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(*order.lock(), vec![4, 3], "grants did not follow arrival");
    }

    #[test]
    fn wait_die_younger_requester_dies() {
        let table = LockTable::new();
        table.acquire(1, 0, b"k").unwrap();
        let err = table.acquire(2, 0, b"k").unwrap_err();
        assert_eq!(err.kind(), "conflict", "younger requester must die: {err}");
    }

    #[test]
    fn wait_die_older_requester_waits() {
        let table = Arc::new(LockTable::new());
        table.acquire(7, 0, b"k").unwrap();
        let waiter = {
            let table = table.clone();
            std::thread::spawn(move || table.acquire(2, 0, b"k").map(|_| ()))
        };
        wait_until("the older requester to park", || table.waiters(0, b"k") == 1);
        assert!(!waiter.is_finished(), "an older requester must wait");
        table.release_all(7, None, 0);
        waiter.join().unwrap().expect("the older requester waits");
        assert!(table.holds(2, 0, b"k"));
    }

    #[test]
    fn reentrant_acquire_by_same_txn_id_is_granted() {
        let table = LockTable::new();
        assert_eq!(table.acquire(5, 0, b"k").unwrap(), Some(0));
        // Equal ids mean the holder IS the requester; granting immediately is
        // the whole tie-break rule — and it is not a grant, so the caller has
        // nothing to refresh onto.
        assert_eq!(table.acquire(5, 0, b"k").unwrap(), None);
        assert!(table.holds(5, 0, b"k"));
        table.release_all(5, None, 0);
        assert!(table.is_idle(), "one release frees a re-entrant acquisition");
    }

    #[test]
    fn wake_all_with_error_never_hangs() {
        let table = Arc::new(LockTable::new());
        table.acquire(9, 0, b"k").unwrap();
        let woken = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for id in [2u64, 3, 4] {
            let table = table.clone();
            let woken = woken.clone();
            threads.push(std::thread::spawn(move || {
                let err = table.acquire(id, 0, b"k").unwrap_err();
                assert_eq!(err.kind(), "invalid_db", "{err}");
                woken.fetch_add(1, Ordering::SeqCst);
            }));
        }
        wait_until("three waiters to park", || table.waiters(0, b"k") == 3);
        table.wake_all_with_error(OndaError::InvalidDb("woken".into()));
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(woken.load(Ordering::SeqCst), 3);
        // A plain wake-up leaves the table usable: fail-stop wakes waiters, it
        // does not close the database.
        table.acquire(1, 0, b"other").unwrap();
    }

    #[test]
    fn shut_down_refuses_later_acquisitions() {
        let table = LockTable::new();
        table.shut_down(OndaError::InvalidDb("closing".into()));
        assert_eq!(
            table.acquire(1, 0, b"k").unwrap_err().kind(),
            "invalid_db",
            "a closed table parks nobody"
        );
    }

    #[test]
    fn hand_off_denies_waiters_younger_than_the_new_holder() {
        // T5 holds; T2 and T4 queue in that order. The hand-off gives the lock
        // to T2 (FIFO) and must NOT leave T4 waiting on the older T2 — that is
        // the edge that turns FIFO into a deadlock against a second key.
        let table = Arc::new(LockTable::new());
        table.acquire(5, 0, b"k").unwrap();
        let older = {
            let table = table.clone();
            std::thread::spawn(move || table.acquire(2, 0, b"k").map(|_| ()))
        };
        wait_until("T2 to park", || table.waiters(0, b"k") == 1);
        let younger = {
            let table = table.clone();
            std::thread::spawn(move || table.acquire(4, 0, b"k").map(|_| ()))
        };
        wait_until("T4 to park", || table.waiters(0, b"k") == 2);
        table.release_all(5, None, 0);
        older.join().unwrap().expect("T2 is granted the lock");
        let err = younger.join().unwrap().unwrap_err();
        assert_eq!(err.kind(), "conflict", "{err}");
    }

    #[test]
    fn commit_stamp_survives_for_the_next_holder() {
        let table = LockTable::new();
        table.acquire(1, 0, b"k").unwrap();
        // Committed at 100 while publication has only reached 90: the entry has
        // to outlive the release, or the next holder refreshes to a watermark
        // that excludes the write it is about to read.
        table.release_all(1, Some(100), 90);
        assert_eq!(table.acquire(2, 0, b"k").unwrap(), Some(100));
        // Once publication catches up the stamp buys nothing and the entry goes.
        table.release_all(2, Some(100), 100);
        assert_eq!(table.acquire(3, 0, b"k").unwrap(), Some(0));
    }

    #[test]
    fn rollback_stamps_nothing() {
        let table = LockTable::new();
        table.acquire(1, 0, b"k").unwrap();
        table.release_all(1, None, 0);
        assert_eq!(
            table.acquire(2, 0, b"k").unwrap(),
            Some(0),
            "a rolled-back owner published nothing to wait for"
        );
    }

    #[test]
    fn free_stamped_entries_are_swept() {
        let table = LockTable::new();
        // Every release parks a free-but-stamped entry; past SWEEP_AT one
        // release collects the ones publication has overtaken.
        for i in 0..(SWEEP_AT as u64 * 2) {
            table.acquire(i + 1, 0, &i.to_be_bytes()).unwrap();
            table.release_all(i + 1, Some(1_000_000), 0);
        }
        let live = table.table.lock().entries.len();
        assert_eq!(live, SWEEP_AT * 2, "unpublished stamps must be kept");
        // Now publication has passed every stamp: the next release sweeps.
        table.acquire(9_999, 0, b"z").unwrap();
        table.release_all(9_999, None, 2_000_000);
        assert!(
            table.table.lock().entries.len() < live,
            "published stamps are not worth keeping"
        );
    }

    #[test]
    fn randomized_multi_key_stress_never_deadlocks() {
        // Wait-die's promise, exercised the only way it can be: many threads,
        // overlapping key sets, mixed ages, and a bounded wall clock. A
        // deadlock shows up as the join never returning, so the test's timeout
        // IS the assertion; the counters only prove it did real work.
        let table = Arc::new(LockTable::new());
        let granted = Arc::new(AtomicUsize::new(0));
        let died = Arc::new(AtomicUsize::new(0));
        const THREADS: usize = 8;
        const ROUNDS: usize = 125_000;
        const KEYS_PER_TXN: usize = 4;
        let mut threads = Vec::new();
        for t in 0..THREADS {
            let table = table.clone();
            let granted = granted.clone();
            let died = died.clone();
            threads.push(std::thread::spawn(move || {
                // A cheap xorshift: deterministic per thread, no dependency.
                let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64).wrapping_mul(0x1234_5678_9ABC);
                let mut next = move || {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng
                };
                for round in 0..ROUNDS {
                    // Ids interleave across threads instead of coming from one
                    // shared counter: threads run at different speeds, so a
                    // slower thread's live transaction is genuinely older than
                    // a faster one's and BOTH wait-die arms get exercised. A
                    // shared counter would make every requester the youngest
                    // and the older-waits arm would never run.
                    let id = 1 + (round * THREADS + t) as u64;
                    let mut ok = true;
                    for _ in 0..KEYS_PER_TXN {
                        let key = (next() % 16).to_be_bytes();
                        match table.acquire(id, 0, &key) {
                            Ok(_) => {
                                granted.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                assert_eq!(e.kind(), "conflict", "{e}");
                                died.fetch_add(1, Ordering::Relaxed);
                                ok = false;
                                break;
                            }
                        }
                    }
                    table.release_all(id, ok.then_some(0), 0);
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert!(table.is_idle(), "every lock was released");
        let total = granted.load(Ordering::Relaxed) + died.load(Ordering::Relaxed);
        // A transaction that dies stops early, so the floor is one acquisition
        // per round; that alone is the 10^6 the acceptance criterion asks for.
        assert!(
            total >= THREADS * ROUNDS,
            "the stress did less work than it claims: {total}"
        );
        // Both outcomes must actually occur, or the run proved nothing about
        // wait-die.
        assert!(granted.load(Ordering::Relaxed) > 0);
        assert!(died.load(Ordering::Relaxed) > 0);
    }
}
