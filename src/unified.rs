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
use crate::memtable::{Entry, Lookup, Memtable};
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
    /// Open (and replay) the unified store. Returns the store and the highest
    /// sequence seen during replay.
    pub(crate) fn open(
        dir: &str,
        opts: &Options,
        flush_tx: Sender<FlushJob>,
        pending_flush: Arc<AtomicUsize>,
        closing: Arc<AtomicBool>,
        poison: Arc<crate::util::Poison>,
        wal_syncs: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<(Arc<UnifiedStore>, u64)> {
        let mem = Memtable::new(default_comparator());
        let mut max_seq = 0;
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
            let last = Wal::replay(&p, |r| {
                mem.put(&r.key, r.value, r.seq, r.ttl, r.tombstone, r.single_delete);
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
        Ok((store, max_seq))
    }

    /// Apply a committed batch (records carry their CF id) to the WAL + memtable.
    /// Records borrow the transaction's buffer.
    pub(crate) fn apply(self: &Arc<Self>, items: &[(u64, wal::RecordRef<'_>)]) -> Result<()> {
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
            if let Some(w) = &wal {
                w.append_batch(&recs)?;
            }
            mem.put_batch(&recs);
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

    /// Seal the active memtable and enqueue a split flush.
    pub(crate) fn rotate(self: &Arc<Self>, force: bool) {
        let imm = {
            let mut g = self.rot.lock();
            while g.rotating {
                self.cond.wait(&mut g);
            }
            {
                let s = self.state.read();
                if s.mem.is_empty() {
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

    /// Remove a flushed immutable from the queue.
    pub(crate) fn remove_imm(&self, imm: &Arc<UnifiedImm>) {
        // Match the writer predicate's lock order (`rot` then `state`) so a
        // completion cannot notify between a writer's predicate check and wait.
        let _g = self.rot.lock();
        let mut s = self.state.write();
        if let Some(pos) = s.imm.iter().position(|i| Arc::ptr_eq(i, imm)) {
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
            tombstone: false,
            single_delete: false,
        }
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
        let (store, _) = UnifiedStore::open(
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
            store.apply(&[(7, record(key.as_bytes(), seq))]).unwrap();
        }

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = store.clone();
        let join = std::thread::spawn(move || {
            let result = writer.apply(&[(7, record(b"blocked", 7))]);
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
