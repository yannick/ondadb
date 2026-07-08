//! Write-ahead log.
//!
//! Each committed batch is appended as ONE length-and-checksum framed unit, so
//! a crash leaves at most a torn frame at the tail, which replay detects and
//! discards — and a multi-record commit replays either whole or not at all
//! (batch atomicity).  Records are never compressed.
//!
//! Frame: `[payload_len u32 LE][crc32c(payload) u32 LE][payload]`
//!
//! Payload (records back-to-back, each):
//! `flags(1) | key_len uvarint | val_len uvarint | seq uvarint |
//!  ttl varint (if HAS_TTL) | key | value`
//!
//! Under [`SyncMode::Full`], concurrent committers are collapsed via **group
//! commit**: the first thread in becomes the leader and writes every queued
//! frame plus a single `fsync`, then wakes the followers.  The other sync modes
//! write directly under the file lock.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;

use crate::config::SyncMode;
use crate::encoding::{
    append_uvarint, append_varint, checksum, put_u32, read_u32, uvarint, varint,
};
use crate::error::{OndaError, Result};
use crate::format::flags;

const HEADER_SIZE: usize = 8; // payload_len(4) + crc(4)

/// One logical WAL entry (owned; produced by replay).
#[derive(Debug, Clone, Default)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    /// Absolute Unix-nanosecond expiry; `0` for none.
    pub ttl: i64,
    pub tombstone: bool,
    pub single_delete: bool,
}

/// A borrowed view of one logical WAL entry, used on the commit path so keys
/// and values can be encoded straight out of the transaction's buffer without
/// materializing owned `Record`s.
#[derive(Debug, Clone, Copy)]
pub struct RecordRef<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub seq: u64,
    pub ttl: i64,
    pub tombstone: bool,
    pub single_delete: bool,
}

impl Record {
    /// Borrowed view of this record.
    pub fn as_ref(&self) -> RecordRef<'_> {
        RecordRef {
            key: &self.key,
            value: &self.value,
            seq: self.seq,
            ttl: self.ttl,
            tombstone: self.tombstone,
            single_delete: self.single_delete,
        }
    }
}

/// Append one record's body (no framing) to `dst`.
fn encode_record_body(dst: &mut Vec<u8>, r: RecordRef<'_>) {
    let mut fl = 0u8;
    if r.tombstone {
        fl |= flags::TOMBSTONE;
    }
    if r.single_delete {
        fl |= flags::SINGLE_DELETE;
    }
    if r.ttl != 0 {
        fl |= flags::HAS_TTL;
    }
    dst.push(fl);
    append_uvarint(dst, r.key.len() as u64);
    append_uvarint(dst, r.value.len() as u64);
    append_uvarint(dst, r.seq);
    if r.ttl != 0 {
        append_varint(dst, r.ttl);
    }
    dst.extend_from_slice(r.key);
    dst.extend_from_slice(r.value);
}

/// Decode one record from the front of `p`, returning it and the bytes
/// consumed. `None` on malformed input.
fn decode_record(p: &[u8]) -> Option<(Record, usize)> {
    if p.is_empty() {
        return None;
    }
    let fl = p[0];
    let mut off = 1usize;
    let mut r = Record {
        tombstone: fl & flags::TOMBSTONE != 0,
        single_delete: fl & flags::SINGLE_DELETE != 0,
        ..Default::default()
    };
    let (klen, n) = uvarint(&p[off..])?;
    off += n;
    let (vlen, n) = uvarint(&p[off..])?;
    off += n;
    let (seq, n) = uvarint(&p[off..])?;
    off += n;
    r.seq = seq;
    if fl & flags::HAS_TTL != 0 {
        let (ttl, n) = varint(&p[off..])?;
        off += n;
        r.ttl = ttl;
    }
    let (klen, vlen) = (klen as usize, vlen as usize);
    let need = klen.checked_add(vlen)?;
    if p.len() - off < need {
        return None;
    }
    r.key = p[off..off + klen].to_vec();
    r.value = p[off + klen..off + need].to_vec();
    Some((r, off + need))
}

struct QueueState {
    queue: Vec<WalReq>,
    flushing: bool,
}

struct WalReq {
    /// Frames already encoded by the committing thread, so the group-commit
    /// leader only writes bytes (encoding happens in parallel across callers).
    buf: Vec<u8>,
    res: Sender<i32>,
}

/// Stripe count for non-`Full` sync modes: concurrent committers append to
/// distinct files instead of convoying on one file mutex. Replay order across
/// stripes is immaterial — sequence numbers define visibility. `Full` mode
/// keeps a single file so group commit can amortize the fsync.
const WAL_STRIPES: usize = 4;

/// Path of stripe `k` for the WAL based at `base`: stripe 0 IS the base path
/// (also the generation marker recovery scans for); others append `.s<k>`.
fn stripe_path(base: &Path, k: usize) -> std::path::PathBuf {
    if k == 0 {
        base.to_path_buf()
    } else {
        std::path::PathBuf::from(format!("{}.s{k}", base.display()))
    }
}

/// Remove every stripe file of the WAL based at `base`.
pub fn remove_wal_files(base: impl AsRef<Path>) {
    for k in 0..WAL_STRIPES {
        let _ = std::fs::remove_file(stripe_path(base.as_ref(), k));
    }
}

struct Shared {
    /// One file per stripe (a single entry under [`SyncMode::Full`]).
    files: Vec<Mutex<Option<File>>>,
    sync: SyncMode,
    size: AtomicI64,
    dirty: AtomicBool,
    qstate: Mutex<QueueState>,
    /// DB-wide fail-stop flag, tripped on any fsync failure (see
    /// [`crate::util::Poison`]). `None` only for standalone WALs in tests.
    poison: Mutex<Option<Arc<crate::util::Poison>>>,
}

impl Shared {
    fn poison(&self, why: String) {
        if let Some(p) = self.poison.lock().as_ref() {
            p.set(why);
        }
    }
}

/// An append-only write-ahead log with configurable durability.
///
/// `close` takes `&self` (background-thread handles live behind a `Mutex`) so a
/// `Wal` can be shared as `Arc<Wal>`; the last `Arc` drop closes it.
pub struct Wal {
    shared: Arc<Shared>,
    stop_tx: Mutex<Option<Sender<()>>>,
    bg: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("size", &self.shared.size.load(Ordering::Relaxed))
            .finish()
    }
}

impl Wal {
    /// Open (creating if needed) the WAL at `path` for appending.  Under
    /// [`SyncMode::Interval`] a background thread fsyncs every `interval`.
    pub fn open(path: impl AsRef<Path>, mode: SyncMode, interval: Duration) -> Result<Wal> {
        let nstripes = if mode == SyncMode::Full {
            1
        } else {
            WAL_STRIPES
        };
        let mut files = Vec::with_capacity(nstripes);
        let mut size = 0i64;
        for k in 0..nstripes {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(stripe_path(path.as_ref(), k))?;
            size += f.metadata()?.len() as i64;
            files.push(Mutex::new(Some(f)));
        }
        let shared = Arc::new(Shared {
            files,
            sync: mode,
            size: AtomicI64::new(size),
            dirty: AtomicBool::new(false),
            qstate: Mutex::new(QueueState {
                queue: Vec::new(),
                flushing: false,
            }),
            poison: Mutex::new(None),
        });
        let (mut stop_tx, mut bg) = (None, None);
        if mode == SyncMode::Interval {
            let iv = if interval.is_zero() {
                Duration::from_millis(128)
            } else {
                interval
            };
            let (tx, rx) = bounded::<()>(1);
            let sh = shared.clone();
            let handle = std::thread::Builder::new()
                .name("onda-wal-sync".into())
                .spawn(move || interval_sync(sh, rx, iv))
                .expect("spawn wal sync thread");
            stop_tx = Some(tx);
            bg = Some(handle);
        }
        Ok(Wal {
            shared,
            stop_tx: Mutex::new(stop_tx),
            bg: Mutex::new(bg),
        })
    }

    /// Wire this WAL to the DB-wide fail-stop flag; fsync failures (group
    /// commit, interval sync, manual sync) will trip it.
    pub(crate) fn set_poison(&self, p: Arc<crate::util::Poison>) {
        *self.shared.poison.lock() = Some(p);
    }

    /// Append a single record.
    pub fn append(&self, r: Record) -> Result<()> {
        self.append_batch(&[r.as_ref()])
    }

    /// Durably append `recs` as ONE frame (encoded here, in the calling
    /// thread): a single header + CRC per commit, and the whole batch replays
    /// atomically — a torn tail can never resurrect half a transaction.
    pub fn append_batch(&self, recs: &[RecordRef<'_>]) -> Result<()> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + recs.len() * 64);
        buf.extend_from_slice(&[0u8; HEADER_SIZE]);
        for r in recs {
            encode_record_body(&mut buf, *r);
        }
        let payload_len = (buf.len() - HEADER_SIZE) as u32;
        let crc = checksum(&buf[HEADER_SIZE..]);
        put_u32(&mut buf[0..], payload_len);
        put_u32(&mut buf[4..], crc);

        // Group commit exists to amortize the fsync under `SyncMode::Full`.
        // Without a per-commit fsync there is nothing to batch: write directly
        // to this thread's stripe file, so concurrent committers don't convoy
        // on a single file mutex.
        if self.shared.sync != SyncMode::Full {
            let stripe = my_stripe(self.shared.files.len());
            let mut guard = self.shared.files[stripe].lock();
            let f = match guard.as_mut() {
                Some(f) => f,
                None => return Err(OndaError::InvalidDb("wal closed".into())),
            };
            f.write_all(&buf)?;
            if self.shared.sync == SyncMode::Interval {
                self.shared.dirty.store(true, Ordering::Relaxed);
            }
            drop(guard);
            self.shared
                .size
                .fetch_add(buf.len() as i64, Ordering::Relaxed);
            return Ok(());
        }

        let (tx, rx) = bounded::<i32>(1);
        let req = WalReq { buf, res: tx };
        {
            let mut qs = self.shared.qstate.lock();
            qs.queue.push(req);
            if qs.flushing {
                drop(qs);
                let code = rx.recv().unwrap_or(-4);
                return code_to_result(code);
            }
            qs.flushing = true;
        }
        loop {
            let batch = {
                let mut qs = self.shared.qstate.lock();
                std::mem::take(&mut qs.queue)
            };
            if batch.is_empty() {
                let mut qs = self.shared.qstate.lock();
                if qs.queue.is_empty() {
                    qs.flushing = false;
                    break;
                }
                continue;
            }
            let code = self.flush_group(&batch);
            for r in &batch {
                let _ = r.res.send(code);
            }
            let mut qs = self.shared.qstate.lock();
            if qs.queue.is_empty() {
                qs.flushing = false;
                break;
            }
        }
        let code = rx.recv().unwrap_or(-4);
        code_to_result(code)
    }

    fn flush_group(&self, batch: &[WalReq]) -> i32 {
        let total: usize = batch.iter().map(|r| r.buf.len()).sum();
        // Group commit only runs under SyncMode::Full, which uses one stripe.
        let mut guard = self.shared.files[0].lock();
        let f = match guard.as_mut() {
            Some(f) => f,
            None => return -10, // closed
        };
        for req in batch {
            if let Err(e) = f.write_all(&req.buf) {
                return OndaError::from(e).code();
            }
        }
        match self.shared.sync {
            SyncMode::Full => {
                if let Err(e) = f.sync_data() {
                    // The kernel may have dropped the dirty pages it failed to
                    // persist; earlier acknowledged commits could be gone.
                    self.shared
                        .poison(format!("wal group-commit fsync failed: {e}"));
                    return OndaError::from(e).code();
                }
            }
            SyncMode::Interval => self.shared.dirty.store(true, Ordering::Relaxed),
            SyncMode::None => {}
        }
        drop(guard);
        self.shared.size.fetch_add(total as i64, Ordering::Relaxed);
        0
    }

    /// fsync every stripe file.
    pub fn sync(&self) -> Result<()> {
        self.shared.dirty.store(false, Ordering::Relaxed);
        for file in &self.shared.files {
            let guard = file.lock();
            match guard.as_ref() {
                Some(f) => {
                    if let Err(e) = f.sync_data() {
                        self.shared.poison(format!("wal fsync failed: {e}"));
                        return Err(e.into());
                    }
                }
                None => return Err(OndaError::InvalidDb("wal closed".into())),
            }
        }
        Ok(())
    }

    /// Current on-disk size in bytes.
    pub fn size(&self) -> i64 {
        self.shared.size.load(Ordering::Relaxed)
    }

    /// fsync and close the underlying file. Safe to call more than once.
    pub fn close(&self) -> Result<()> {
        if let Some(tx) = self.stop_tx.lock().take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.bg.lock().take() {
            let _ = h.join();
        }
        for file in &self.shared.files {
            if let Some(f) = file.lock().take() {
                f.sync_data()?;
            }
        }
        Ok(())
    }

    /// Replay records from the WAL based at `path`, invoking `f` for each.
    /// Every stripe file is replayed; record order across stripes is not
    /// meaningful — sequence numbers define visibility.  A torn or
    /// checksum-failed frame at a stripe's tail ends that stripe cleanly (the
    /// expected result of a crash mid-write); each frame — one committed batch —
    /// replays atomically.  Returns the highest sequence number seen.  Missing
    /// files replay as empty.
    pub fn replay<F>(path: impl AsRef<Path>, mut f: F) -> Result<u64>
    where
        F: FnMut(Record) -> Result<()>,
    {
        let mut last_seq = 0u64;
        for k in 0..WAL_STRIPES {
            let seq = Self::replay_file(stripe_path(path.as_ref(), k), &mut f)?;
            last_seq = last_seq.max(seq);
        }
        Ok(last_seq)
    }

    fn replay_file<F>(path: std::path::PathBuf, f: &mut F) -> Result<u64>
    where
        F: FnMut(Record) -> Result<()>,
    {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        let mut r = BufReader::with_capacity(64 << 10, file);
        let mut last_seq = 0u64;
        let mut header = [0u8; HEADER_SIZE];
        loop {
            if read_full(&mut r, &mut header)?.is_none() {
                return Ok(last_seq); // clean EOF or partial header
            }
            let plen = read_u32(&header[0..4]) as usize;
            let want = read_u32(&header[4..8]);
            let mut payload = vec![0u8; plen];
            if read_full(&mut r, &mut payload)?.is_none() {
                return Ok(last_seq); // torn payload at tail
            }
            if checksum(&payload) != want {
                return Ok(last_seq); // corrupted tail
            }
            // Decode every record in the (verified) frame.
            let mut p = &payload[..];
            while !p.is_empty() {
                let (rec, used) = match decode_record(p) {
                    Some(x) => x,
                    None => return Ok(last_seq), // malformed despite CRC: stop
                };
                p = &p[used..];
                if rec.seq > last_seq {
                    last_seq = rec.seq;
                }
                f(rec)?;
            }
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Read exactly `buf.len()` bytes; `Ok(None)` on a clean/partial EOF.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<Option<()>> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => return Ok(None),
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(Some(()))
}

fn code_to_result(code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(OndaError::from_code(code))
    }
}

/// Sticky per-thread stripe assignment: each committing thread keeps writing
/// the same stripe (page-cache locality), and threads spread round-robin.
fn my_stripe(n: usize) -> usize {
    use std::cell::Cell;
    use std::sync::atomic::AtomicUsize;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
    }
    SLOT.with(|c| {
        let mut v = c.get();
        if v == usize::MAX {
            v = NEXT.fetch_add(1, Ordering::Relaxed);
            c.set(v);
        }
        v % n
    })
}

fn interval_sync(shared: Arc<Shared>, stop: Receiver<()>, interval: Duration) {
    loop {
        match stop.recv_timeout(interval) {
            Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if shared.dirty.swap(false, Ordering::Relaxed) {
                    for file in &shared.files {
                        if let Some(f) = file.lock().as_ref() {
                            if let Err(e) = f.sync_data() {
                                // Commits acknowledged since the last successful
                                // sync may be lost — fail-stop the DB rather
                                // than silently dropping the error.
                                shared.poison(format!("wal interval fsync failed: {e}"));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(key: &str, val: &str, seq: u64) -> Record {
        Record {
            key: key.as_bytes().to_vec(),
            value: val.as_bytes().to_vec(),
            seq,
            ..Default::default()
        }
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal = Wal::open(&path, SyncMode::None, Duration::ZERO).unwrap();
            wal.append(rec("a", "1", 1)).unwrap();
            let (b, c) = (rec("b", "2", 2), rec("c", "3", 3));
            wal.append_batch(&[b.as_ref(), c.as_ref()]).unwrap();
        }
        let mut got = Vec::new();
        let last = Wal::replay(&path, |r| {
            got.push((String::from_utf8(r.key).unwrap(), r.seq));
            Ok(())
        })
        .unwrap();
        assert_eq!(last, 3);
        assert_eq!(got, vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)]);
    }

    #[test]
    fn ttl_and_tombstone_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal = Wal::open(&path, SyncMode::Full, Duration::ZERO).unwrap();
            wal.append(Record {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                seq: 5,
                ttl: 1234567890,
                ..Default::default()
            })
            .unwrap();
            wal.append(Record {
                key: b"d".to_vec(),
                value: Vec::new(),
                seq: 6,
                tombstone: true,
                single_delete: true,
                ..Default::default()
            })
            .unwrap();
        }
        let mut recs = Vec::new();
        Wal::replay(&path, |r| {
            recs.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(recs[0].ttl, 1234567890);
        assert!(recs[1].tombstone && recs[1].single_delete);
    }

    #[test]
    fn torn_tail_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal = Wal::open(&path, SyncMode::None, Duration::ZERO).unwrap();
            wal.append(rec("good", "v", 1)).unwrap();
        }
        // Append garbage (a partial frame) to simulate a crash mid-write.
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[9, 0, 0, 0, 1, 2, 3]).unwrap(); // claims 9 bytes, gives 3
        }
        let mut n = 0;
        let last = Wal::replay(&path, |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 1, "torn record must be skipped");
        assert_eq!(last, 1);
    }

    #[test]
    fn checksum_corruption_truncates_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal = Wal::open(&path, SyncMode::None, Duration::ZERO).unwrap();
            wal.append(rec("a", "1", 1)).unwrap();
            wal.append(rec("b", "2", 2)).unwrap();
        }
        // Corrupt the last byte of the stripe file that holds the records (the
        // test thread's sticky stripe is process-global, so locate it by size).
        {
            use std::io::{Seek, SeekFrom, Write};
            let data_file = (0..WAL_STRIPES)
                .map(|k| stripe_path(&path, k))
                .find(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
                .expect("one stripe holds the records");
            let mut f = OpenOptions::new().write(true).open(&data_file).unwrap();
            let len = f.metadata().unwrap().len();
            f.seek(SeekFrom::Start(len - 1)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let mut keys = Vec::new();
        Wal::replay(&path, |r| {
            keys.push(r.key);
            Ok(())
        })
        .unwrap();
        assert_eq!(keys, vec![b"a".to_vec()]); // second record dropped
    }

    #[test]
    fn group_commit_concurrent() {
        use std::sync::Arc as StdArc;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = StdArc::new(Wal::open(&path, SyncMode::Full, Duration::ZERO).unwrap());
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let wal = wal.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..100u64 {
                    let seq = t * 1000 + i;
                    wal.append(rec("k", "v", seq)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        drop(wal);
        let mut count = 0;
        Wal::replay(&path, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 800);
    }

    #[test]
    fn interval_sync_mode_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal = Wal::open(&path, SyncMode::Interval, Duration::from_millis(10)).unwrap();
            wal.append(rec("a", "1", 1)).unwrap();
            std::thread::sleep(Duration::from_millis(30));
        }
        let mut count = 0;
        Wal::replay(&path, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
    }
}
