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
//! The flags byte is strictly masked (`format::check_entry_flags`): unknown
//! bits and writer-impossible combinations are `Corruption`. A torn tail still
//! ends a stripe cleanly — see [`Wal::replay`] for the split.
//!
//! Under [`SyncMode::Full`], concurrent committers are collapsed via **group
//! commit**: the first thread in becomes the leader and writes every queued
//! frame plus a single `fsync`, then wakes the followers.  The other sync modes
//! write directly under the file lock.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;

use crate::config::SyncMode;
use crate::encoding::{
    append_uvarint, append_varint, checksum, put_u32, read_u32, uvarint, uvarint_len, varint,
    varint_len,
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

/// One entry handed to a [`Wal::replay`] callback.
///
/// An enum rather than a bare [`Record`] because not every record kind is a
/// point write: 1.2's range deletes carry two keys and no value, so replay
/// callers must match on the kind instead of assuming one.
#[derive(Debug, Clone)]
pub enum ReplayRecord {
    /// A put, delete or single-delete.
    Point(Record),
}

/// First payload byte of an **envelope** frame (`CAP_EXTENDED_RECORDS`).
///
/// Safe as a discriminator because a legacy payload starts with a flags byte
/// and no writer-produced flags byte exceeds `KNOWN_ENTRY_FLAGS` (`0x17`) —
/// strict decoding rejects anything above it regardless.
pub const ENVELOPE_TAG: u8 = 0xFF;

/// Envelope schema: per-CF WAL layout; record keys are user keys.
pub const ENVELOPE_SCHEMA_PER_CF: u64 = 1;
/// Envelope schema: unified WAL layout; record keys carry the 8-byte big-endian
/// CF-id prefix **inside** the key, exactly as the legacy unified layout does.
/// There is deliberately no separate cf-id field: a LEB128 id would cost more
/// than the fixed 8 bytes already present, and recovery must be able to decode a
/// detached artifact without consulting options.
pub const ENVELOPE_SCHEMA_UNIFIED: u64 = 2;

/// Encoded length of one envelope record, matching [`encode_envelope_record`]
/// byte for byte.
///
/// The exact frame-size precompute is not an optimization detail: growth
/// reallocations re-copy the whole payload and dominated large-value commits
/// (see [`Wal::append_batch`]).
fn envelope_record_len(r: RecordRef<'_>) -> usize {
    let kind = crate::format::point_kind(r.tombstone || r.single_delete, r.single_delete);
    let mods = if r.ttl != 0 {
        crate::format::modifiers::HAS_TTL
    } else {
        0
    };
    uvarint_len(kind)
        + uvarint_len(mods)
        + uvarint_len(r.key.len() as u64)
        + uvarint_len(r.value.len() as u64)
        + uvarint_len(r.seq)
        + if r.ttl != 0 { varint_len(r.ttl) } else { 0 }
        + r.key.len()
        + r.value.len()
}

/// Append one envelope record to `dst`.
///
/// Field order matches the legacy record deliberately (`alen, blen, seq, ttl?,
/// a, b`), so the size precompute above stays a one-line variation on the
/// legacy one. The `a`/`b` slots are named generically because kind 5 (1.2)
/// puts a range's `(start, end)` in them rather than `(key, value)`.
fn encode_envelope_record(dst: &mut Vec<u8>, r: RecordRef<'_>) {
    crate::format::debug_check_entry_flags(r.tombstone, r.single_delete, false);
    let kind = crate::format::point_kind(r.tombstone || r.single_delete, r.single_delete);
    let mods = if r.ttl != 0 {
        crate::format::modifiers::HAS_TTL
    } else {
        0
    };
    append_uvarint(dst, kind);
    append_uvarint(dst, mods);
    append_uvarint(dst, r.key.len() as u64);
    append_uvarint(dst, r.value.len() as u64);
    append_uvarint(dst, r.seq);
    if r.ttl != 0 {
        append_varint(dst, r.ttl);
    }
    dst.extend_from_slice(r.key);
    dst.extend_from_slice(r.value);
}

/// Decode one envelope record from the front of `p`, returning it and the bytes
/// consumed.
fn decode_envelope_record(p: &[u8]) -> Result<(ReplayRecord, usize)> {
    let corrupt = || OndaError::Corruption("wal: malformed envelope record".into());
    let (kind, n) = uvarint(p).ok_or_else(corrupt)?;
    crate::format::check_kind(kind)?;
    let mut off = n;
    let (mods, n) = uvarint(&p[off..]).ok_or_else(corrupt)?;
    crate::format::check_modifiers(mods)?;
    off += n;
    let (alen, n) = uvarint(&p[off..]).ok_or_else(corrupt)?;
    off += n;
    let (blen, n) = uvarint(&p[off..]).ok_or_else(corrupt)?;
    off += n;
    let (seq, n) = uvarint(&p[off..]).ok_or_else(corrupt)?;
    off += n;
    let mut ttl = 0i64;
    if mods & crate::format::modifiers::HAS_TTL != 0 {
        let (t, n) = varint(&p[off..]).ok_or_else(corrupt)?;
        off += n;
        ttl = t;
    }
    // HAS_VLOG describes an SSTable entry's value placement; a WAL record always
    // carries its value inline, so the bit cannot appear here.
    if mods & crate::format::modifiers::HAS_VLOG != 0 {
        return Err(OndaError::Corruption(
            "wal: envelope record sets HAS_VLOG".into(),
        ));
    }
    let (alen, blen) = (alen as usize, blen as usize);
    let need = alen.checked_add(blen).ok_or_else(corrupt)?;
    if p.len() - off < need {
        return Err(corrupt());
    }
    let rec = Record {
        key: p[off..off + alen].to_vec(),
        value: p[off + alen..off + need].to_vec(),
        seq,
        ttl,
        tombstone: kind == crate::format::KIND_DELETE || kind == crate::format::KIND_SINGLE_DELETE,
        single_delete: kind == crate::format::KIND_SINGLE_DELETE,
    };
    Ok((ReplayRecord::Point(rec), off + need))
}

/// Decode a whole envelope payload (its leading [`ENVELOPE_TAG`] included).
///
/// `count` is verified against the payload: a short payload or trailing bytes
/// are `Corruption`, so a frame can never deliver a partial batch.
fn decode_envelope(payload: &[u8], mut f: impl FnMut(ReplayRecord) -> Result<u64>) -> Result<u64> {
    let corrupt = || OndaError::Corruption("wal: malformed envelope".into());
    debug_assert_eq!(payload.first(), Some(&ENVELOPE_TAG));
    let mut p = &payload[1..];
    let (schema, n) = uvarint(p).ok_or_else(corrupt)?;
    if schema != ENVELOPE_SCHEMA_PER_CF && schema != ENVELOPE_SCHEMA_UNIFIED {
        return Err(OndaError::UnsupportedFormat(format!(
            "wal envelope schema {schema} is not implemented by this binary"
        )));
    }
    p = &p[n..];
    let (count, n) = uvarint(p).ok_or_else(corrupt)?;
    p = &p[n..];
    let mut last_seq = 0u64;
    for _ in 0..count {
        if p.is_empty() {
            return Err(corrupt()); // count promised more records than exist
        }
        let (rec, used) = decode_envelope_record(p)?;
        p = &p[used..];
        last_seq = last_seq.max(f(rec)?);
    }
    if !p.is_empty() {
        return Err(corrupt()); // records past the promised count
    }
    Ok(last_seq)
}

/// Append one record's body (no framing) to `dst`.
fn encode_record_body(dst: &mut Vec<u8>, r: RecordRef<'_>) {
    // Normalize here, not only at decode: `RecordRef` is public, so a caller
    // outside the crate can hand us `single_delete` without `tombstone`.
    crate::format::debug_check_entry_flags(r.tombstone, r.single_delete, false);
    let fl = crate::format::normalized_entry_flags(r.tombstone, r.single_delete, r.ttl != 0, false);
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
/// consumed.
///
/// Every failure is [`OndaError::Corruption`]: the caller only reaches this
/// function for a frame whose CRC already verified, so a body that does not
/// decode was written intact and still contradicts the format. Torn tails are
/// detected one level up, before the CRC, and end a stripe cleanly instead.
fn decode_record(p: &[u8]) -> Result<(Record, usize)> {
    let corrupt = || OndaError::Corruption("wal: malformed record".into());
    if p.is_empty() {
        return Err(corrupt());
    }
    let fl = p[0];
    crate::format::check_entry_flags(fl)?;
    let mut off = 1usize;
    let mut r = Record {
        tombstone: fl & flags::TOMBSTONE != 0,
        single_delete: fl & flags::SINGLE_DELETE != 0,
        ..Default::default()
    };
    let (klen, n) = uvarint(&p[off..]).ok_or_else(corrupt)?;
    off += n;
    let (vlen, n) = uvarint(&p[off..]).ok_or_else(corrupt)?;
    off += n;
    let (seq, n) = uvarint(&p[off..]).ok_or_else(corrupt)?;
    off += n;
    r.seq = seq;
    if fl & flags::HAS_TTL != 0 {
        let (ttl, n) = varint(&p[off..]).ok_or_else(corrupt)?;
        off += n;
        r.ttl = ttl;
    }
    let (klen, vlen) = (klen as usize, vlen as usize);
    let need = klen.checked_add(vlen).ok_or_else(corrupt)?;
    if p.len() - off < need {
        return Err(corrupt());
    }
    r.key = p[off..off + klen].to_vec();
    r.value = p[off + klen..off + need].to_vec();
    Ok((r, off + need))
}

/// Exact encoded size of the payload of one frame, header excluded.
///
/// Kept beside [`encode_frame`] because the two must agree exactly: growth
/// reallocations re-copy the whole payload and dominated large-value commits.
fn frame_payload_len(schema: Option<u64>, recs: &[RecordRef<'_>]) -> usize {
    match schema {
        None => recs
            .iter()
            .map(|r| {
                1 + uvarint_len(r.key.len() as u64)
                    + uvarint_len(r.value.len() as u64)
                    + uvarint_len(r.seq)
                    + if r.ttl != 0 { varint_len(r.ttl) } else { 0 }
                    + r.key.len()
                    + r.value.len()
            })
            .sum(),
        Some(schema) => {
            1 + uvarint_len(schema)
                + uvarint_len(recs.len() as u64)
                + recs.iter().map(|r| envelope_record_len(*r)).sum::<usize>()
        }
    }
}

/// Encode one framed batch: `[payload_len u32][crc32c u32][payload]`.
///
/// `schema` selects the payload form — `None` is the legacy record stream,
/// `Some(schema)` an envelope. (A lock-free pwrite append was tried on the
/// write side and reverted: on macOS/APFS positional writes to one file
/// serialize in the kernel anyway and lose the O_APPEND fast path.)
fn encode_frame(schema: Option<u64>, recs: &[RecordRef<'_>]) -> Vec<u8> {
    let body = frame_payload_len(schema, recs);
    let mut buf = Vec::with_capacity(HEADER_SIZE + body);
    buf.extend_from_slice(&[0u8; HEADER_SIZE]);
    match schema {
        None => {
            for r in recs {
                encode_record_body(&mut buf, *r);
            }
        }
        Some(schema) => {
            buf.push(ENVELOPE_TAG);
            append_uvarint(&mut buf, schema);
            append_uvarint(&mut buf, recs.len() as u64);
            for r in recs {
                encode_envelope_record(&mut buf, *r);
            }
        }
    }
    debug_assert_eq!(buf.len(), HEADER_SIZE + body, "frame size precompute");
    let payload_len = (buf.len() - HEADER_SIZE) as u32;
    let crc = checksum(&buf[HEADER_SIZE..]);
    put_u32(&mut buf[0..], payload_len);
    put_u32(&mut buf[4..], crc);
    buf
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
    /// DB-wide counter of successful physical `sync_data` calls; survives WAL
    /// rotation because the DB owns the `Arc`. `None` for standalone WALs.
    syncs: Mutex<Option<Arc<AtomicU64>>>,
}

impl Shared {
    fn poison(&self, why: String) {
        if let Some(p) = self.poison.lock().as_ref() {
            p.set(why);
        }
    }

    fn count_sync(&self) {
        if let Some(c) = self.syncs.lock().as_ref() {
            c.fetch_add(1, Ordering::Relaxed);
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
        Self::open_inner(path.as_ref(), mode, interval, crate::util::sync_parent_dir)
    }

    fn open_inner(
        path: &Path,
        mode: SyncMode,
        interval: Duration,
        sync_parent: impl FnOnce(&Path) -> Result<()>,
    ) -> Result<Wal> {
        let nstripes = if mode == SyncMode::Full {
            1
        } else {
            WAL_STRIPES
        };
        let mut files = Vec::with_capacity(nstripes);
        let mut size = 0i64;
        let mut created = false;
        for k in 0..nstripes {
            let stripe = stripe_path(path, k);
            created |= !stripe.exists();
            let f = OpenOptions::new().create(true).append(true).open(stripe)?;
            size += f.metadata()?.len() as i64;
            files.push(Mutex::new(Some(f)));
        }
        if created {
            sync_parent(path)?;
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
            syncs: Mutex::new(None),
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

    /// Wire this WAL to the DB-wide physical-sync counter (see
    /// [`crate::DB::wal_sync_count`]); every successful `sync_data` increments it.
    pub(crate) fn set_sync_counter(&self, c: Arc<AtomicU64>) {
        *self.shared.syncs.lock() = Some(c);
    }

    /// Append a single record.
    pub fn append(&self, r: Record) -> Result<()> {
        self.append_batch(&[r.as_ref()])
    }

    /// Durably append `recs` as ONE frame (encoded here, in the calling
    /// thread): a single header + CRC per commit, and the whole batch replays
    /// atomically — a torn tail can never resurrect half a transaction.
    pub fn append_batch(&self, recs: &[RecordRef<'_>]) -> Result<()> {
        self.submit_frame(encode_frame(None, recs))
    }

    /// Like [`append_batch`](Self::append_batch), but writes the batch as an
    /// **envelope** frame under `schema` ([`ENVELOPE_SCHEMA_PER_CF`] or
    /// [`ENVELOPE_SCHEMA_UNIFIED`]).
    ///
    /// A database may only emit envelopes once it has durably enabled
    /// [`CAP_EXTENDED_RECORDS`](crate::format::CAP_EXTENDED_RECORDS), and from
    /// then on every frame it writes is one — the form is a per-database
    /// decision, never a per-frame one. Replay accepts both forms forever, so
    /// a WAL written across an enable replays whole.
    pub fn append_batch_enveloped(&self, schema: u64, recs: &[RecordRef<'_>]) -> Result<()> {
        self.submit_frame(encode_frame(Some(schema), recs))
    }

    /// Durably write one already-framed batch.
    fn submit_frame(&self, buf: Vec<u8>) -> Result<()> {
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
                self.shared.count_sync();
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
                    self.shared.count_sync();
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
                self.shared.count_sync();
            }
        }
        Ok(())
    }

    /// Replay records from the WAL based at `path`, invoking `f` for each.
    /// Every stripe file is replayed; record order across stripes is not
    /// meaningful — sequence numbers define visibility.  A torn or
    /// checksum-failed frame at a stripe's tail ends that stripe cleanly (the
    /// expected result of a crash mid-write); each frame — one committed batch —
    /// replays atomically.  A record that fails to decode *inside* a CRC-valid
    /// frame is not crash residue and fails with [`OndaError::Corruption`].
    /// Returns the highest sequence number seen.  Missing files replay as empty.
    pub fn replay<F>(path: impl AsRef<Path>, mut f: F) -> Result<u64>
    where
        F: FnMut(ReplayRecord) -> Result<()>,
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
        F: FnMut(ReplayRecord) -> Result<()>,
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
            // Past this point the bytes are known-intact: any decode failure
            // is corruption, not a torn tail, and must not be swallowed.
            //
            // The first payload byte selects the form: an envelope frame
            // (0xFF) or the legacy record stream. Both forms may appear in one
            // file — enabling the capability changes what is written next, not
            // what is already there.
            if payload.first() == Some(&ENVELOPE_TAG) {
                let seq = decode_envelope(&payload, |rec| {
                    let seq = match &rec {
                        ReplayRecord::Point(r) => r.seq,
                    };
                    f(rec)?;
                    Ok(seq)
                })?;
                last_seq = last_seq.max(seq);
                continue;
            }
            let mut p = &payload[..];
            while !p.is_empty() {
                let (rec, used) = decode_record(p)?;
                p = &p[used..];
                if rec.seq > last_seq {
                    last_seq = rec.seq;
                }
                f(ReplayRecord::Point(rec))?;
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

fn sync_dirty_files(shared: &Shared) {
    if !shared.dirty.swap(false, Ordering::Relaxed) {
        return;
    }
    for file in &shared.files {
        let guard = file.lock();
        let Some(file) = guard.as_ref() else {
            continue;
        };
        if let Err(error) = file.sync_data() {
            // Commits acknowledged since the last successful sync may be lost;
            // fail-stop rather than silently dropping the error.
            shared.poison(format!("wal interval fsync failed: {error}"));
        } else {
            shared.count_sync();
        }
    }
}

fn interval_sync(shared: Arc<Shared>, stop: Receiver<()>, interval: Duration) {
    loop {
        match stop.recv_timeout(interval) {
            Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => sync_dirty_files(&shared),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every decoder in this feature must be total over arbitrary bytes: a
    /// `Result`, never a panic. Seeded from the frozen corpus so the shapes are
    /// real WAL bytes rather than uniform noise.
    #[test]
    fn fuzz_decode_record_never_panics() {
        let mut seeds: Vec<Vec<u8>> = Vec::new();
        for name in [
            "wal_legacy_all_flags.bin",
            "wal_legacy_empty_frame.bin",
            "wal_legacy_torn_tail.bin",
            "wal_legacy_crc_valid_undecodable.bin",
        ] {
            let bytes = std::fs::read(crate::util::phase1_fixture(name)).unwrap();
            // Frame headers included and excluded: the record decoder must
            // survive both a payload and the raw file it came from.
            seeds.push(bytes[HEADER_SIZE.min(bytes.len())..].to_vec());
            seeds.push(bytes);
        }
        let mut buf = Vec::new();
        for r in wal_fuzz_records() {
            encode_record_body(&mut buf, r.as_ref());
        }
        seeds.push(buf);

        let mut rng = crate::util::FuzzRng::new(0x9E37_79B9_7F4A_7C15);
        for seed in &seeds {
            for _ in 0..2000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                // Both entry points, at every offset a caller could pass.
                let _ = decode_record(&case);
                if !case.is_empty() {
                    let at = rng.below(case.len());
                    let _ = decode_record(&case[at..]);
                }
            }
        }
    }

    /// Fuzz seeds: one record per writer-produced flag combination.
    fn wal_fuzz_records() -> Vec<Record> {
        vec![
            rec("a", "1", 1),
            Record {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                seq: 300000,
                ttl: 1_700_000_000_000_000_000,
                ..Default::default()
            },
            Record {
                key: b"d".to_vec(),
                seq: 3,
                tombstone: true,
                single_delete: true,
                ..Default::default()
            },
        ]
    }

    /// `0x08` once named a `DELTA_SEQ` encoding that no writer ever produced;
    /// it is reserved-unknown and must fail closed.
    #[test]
    fn decode_record_rejects_unknown_flag_bit() {
        let body = [0x08u8, 1, 1, 5, b'k', b'v'];
        let err = decode_record(&body).expect_err("unknown flag bit must be rejected");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn decode_record_rejects_single_delete_without_tombstone() {
        let body = [flags::SINGLE_DELETE, 1, 0, 5, b'k'];
        let err = decode_record(&body).expect_err("SINGLE_DELETE implies TOMBSTONE");
        assert_eq!(err.kind(), "corruption");
    }

    /// Replay a frozen corpus fixture from a private directory.
    fn replay_fixture(name: &str) -> (tempfile::TempDir, Result<(Vec<Record>, u64)>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        std::fs::copy(crate::util::phase1_fixture(name), &path).unwrap();
        let mut got = Vec::new();
        let res = Wal::replay(&path, |r| {
            let ReplayRecord::Point(r) = r;
            got.push(r);
            Ok(())
        })
        .map(|last| (got, last));
        (dir, res)
    }

    /// A torn payload is the expected crash residue, not corruption: the stripe
    /// ends cleanly and every record before the tear is delivered.
    #[test]
    fn torn_payload_stops_replay_cleanly() {
        let (_dir, res) = replay_fixture("wal_legacy_torn_tail.bin");
        let (got, last) = res.expect("a torn tail must not fail replay");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].key, b"good");
        assert_eq!(last, 1);
    }

    /// A frame whose CRC checks out but whose record body does not decode is a
    /// real corruption: the bytes were written intact and still lie.
    #[test]
    fn crc_valid_undecodable_record_is_corruption() {
        let (_dir, res) = replay_fixture("wal_legacy_crc_valid_undecodable.bin");
        let err = res.expect_err("a CRC-valid undecodable record must fail replay");
        assert_eq!(err.kind(), "corruption");
    }

    /// `Wal::append_batch(&[])` is public API and writes a zero-length frame;
    /// replay skips it and keeps reading.
    #[test]
    fn empty_frame_is_skipped_and_replay_continues() {
        let (_dir, res) = replay_fixture("wal_legacy_empty_frame.bin");
        let (got, last) = res.expect("an empty frame must not fail replay");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].key, b"after");
        assert_eq!(last, 9);
    }

    /// `encode_record_body` builds its flags byte through
    /// `format::normalized_entry_flags`; the byte assertion goes there because
    /// the encode site debug-asserts the invariant (twin below).
    #[test]
    fn wal_encode_normalizes_single_delete() {
        use crate::format::normalized_entry_flags;
        assert_eq!(
            normalized_entry_flags(false, true, false, false),
            flags::TOMBSTONE | flags::SINGLE_DELETE
        );
        let mut buf = Vec::new();
        encode_record_body(
            &mut buf,
            RecordRef {
                key: b"k",
                value: b"",
                seq: 1,
                ttl: 0,
                tombstone: true,
                single_delete: true,
            },
        );
        assert_eq!(buf[0], flags::TOMBSTONE | flags::SINGLE_DELETE);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "SINGLE_DELETE")]
    fn wal_encode_debug_asserts_single_delete_implies_tombstone() {
        encode_record_body(
            &mut Vec::new(),
            RecordRef {
                key: b"k",
                value: b"",
                seq: 1,
                ttl: 0,
                tombstone: false,
                single_delete: true,
            },
        );
    }

    // ---- envelope (CAP_EXTENDED_RECORDS) ------------------------------------

    /// The three point kinds, one of each flag shape a writer produces.
    fn envelope_point_records() -> Vec<Record> {
        vec![
            rec("put", "v1", 1),
            Record {
                key: b"ttl".to_vec(),
                value: b"v2".to_vec(),
                seq: 2,
                ttl: 1_700_000_000_000_000_000,
                ..Default::default()
            },
            Record {
                key: b"del".to_vec(),
                seq: 3,
                tombstone: true,
                ..Default::default()
            },
            Record {
                key: b"sdel".to_vec(),
                seq: 4,
                tombstone: true,
                single_delete: true,
                ..Default::default()
            },
        ]
    }

    /// Decode a hand-built envelope payload into its records.
    fn decode_envelope_payload(payload: &[u8]) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        decode_envelope(payload, |rec| {
            let ReplayRecord::Point(r) = rec;
            let seq = r.seq;
            out.push(r);
            Ok(seq)
        })?;
        Ok(out)
    }

    /// One envelope frame's payload, as `append_batch_enveloped` would write it.
    fn envelope_payload(schema: u64, recs: &[Record]) -> Vec<u8> {
        let refs: Vec<RecordRef<'_>> = recs.iter().map(|r| r.as_ref()).collect();
        encode_frame(Some(schema), &refs)[HEADER_SIZE..].to_vec()
    }

    #[test]
    fn envelope_schema1_round_trips_all_point_kinds() {
        let want = envelope_point_records();
        let payload = envelope_payload(ENVELOPE_SCHEMA_PER_CF, &want);
        assert_eq!(payload[0], ENVELOPE_TAG);
        let got = decode_envelope_payload(&payload).unwrap();
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(want.iter()) {
            assert_eq!(g.key, w.key);
            assert_eq!(g.value, w.value);
            assert_eq!(g.seq, w.seq);
            assert_eq!(g.ttl, w.ttl);
            assert_eq!(g.tombstone, w.tombstone);
            assert_eq!(g.single_delete, w.single_delete);
        }
    }

    /// Schema 2 keeps the 8-byte big-endian CF id inside the key; the decoder
    /// hands the prefixed key back untouched, which is what the unified
    /// memtable stores.
    #[test]
    fn envelope_schema2_keeps_cf_prefix_in_key() {
        let cf_id = 0x0123_4567_89ab_cdefu64;
        let mut key = cf_id.to_be_bytes().to_vec();
        key.extend_from_slice(b"user-key");
        let recs = vec![Record {
            key: key.clone(),
            value: b"v".to_vec(),
            seq: 7,
            ..Default::default()
        }];
        let payload = envelope_payload(ENVELOPE_SCHEMA_UNIFIED, &recs);
        let got = decode_envelope_payload(&payload).unwrap();
        assert_eq!(got[0].key, key);
        assert_eq!(&got[0].key[..8], &cf_id.to_be_bytes());
        // No separate cf-id field: the record costs exactly the prefixed key.
        assert_eq!(payload[1], ENVELOPE_SCHEMA_UNIFIED as u8);
    }

    /// A file may hold both forms — enabling the capability changes what is
    /// written next, not what is already on disk.
    #[test]
    fn legacy_and_envelope_frames_interleave() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal = Wal::open(&path, SyncMode::None, Duration::ZERO).unwrap();
            wal.append(rec("legacy", "a", 1)).unwrap();
            let recs = envelope_point_records();
            let refs: Vec<RecordRef<'_>> = recs.iter().map(|r| r.as_ref()).collect();
            wal.append_batch_enveloped(ENVELOPE_SCHEMA_PER_CF, &refs)
                .unwrap();
            wal.append(rec("legacy2", "b", 9)).unwrap();
        }
        let mut keys = Vec::new();
        let last = Wal::replay(&path, |r| {
            let ReplayRecord::Point(r) = r;
            keys.push(String::from_utf8(r.key).unwrap());
            Ok(())
        })
        .unwrap();
        assert_eq!(keys, vec!["legacy", "put", "ttl", "del", "sdel", "legacy2"]);
        assert_eq!(last, 9);
    }

    /// Splice a hand-chosen record body into an otherwise well-formed
    /// one-record envelope payload.
    fn envelope_with_body(body: &[u8]) -> Vec<u8> {
        let mut p = vec![ENVELOPE_TAG];
        append_uvarint(&mut p, ENVELOPE_SCHEMA_PER_CF);
        append_uvarint(&mut p, 1);
        p.extend_from_slice(body);
        p
    }

    /// A record body with the given kind and modifiers, and an empty key/value.
    fn envelope_body(kind: u64, mods: u64) -> Vec<u8> {
        let mut b = Vec::new();
        append_uvarint(&mut b, kind);
        append_uvarint(&mut b, mods);
        append_uvarint(&mut b, 0); // alen
        append_uvarint(&mut b, 0); // blen
        append_uvarint(&mut b, 5); // seq
        b
    }

    /// Kind 5 is assigned (1.2's range delete) but not implemented here: the
    /// bytes are intact and name a real feature, so this binary is the one at
    /// fault.
    #[test]
    fn envelope_unknown_kind_is_unsupported_format() {
        let p = envelope_with_body(&envelope_body(crate::format::KIND_RANGE_DELETE, 0));
        let err = decode_envelope_payload(&p).expect_err("kind 5 is not implemented here");
        assert_eq!(err.kind(), "unsupported_format");
    }

    /// Kinds above 63 are never assigned to anything, so they cannot have come
    /// from a newer writer.
    #[test]
    fn envelope_kind_above_63_is_corruption() {
        let p = envelope_with_body(&envelope_body(64, 0));
        let err = decode_envelope_payload(&p).expect_err("kind 64 is never assigned");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn envelope_unknown_modifier_is_corruption() {
        let p = envelope_with_body(&envelope_body(crate::format::KIND_PUT, 0x08));
        let err = decode_envelope_payload(&p).expect_err("unknown modifiers must fail closed");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn envelope_unknown_schema_is_unsupported_format() {
        let mut p = vec![ENVELOPE_TAG];
        append_uvarint(&mut p, 3); // never assigned
        append_uvarint(&mut p, 0);
        let err = decode_envelope_payload(&p).expect_err("schema 3 is not implemented");
        assert_eq!(err.kind(), "unsupported_format");
    }

    /// `count` is verified against the payload in both directions, so an
    /// envelope frame can never deliver a partial batch.
    #[test]
    fn envelope_count_mismatch_is_corruption() {
        let recs = envelope_point_records();
        let good = envelope_payload(ENVELOPE_SCHEMA_PER_CF, &recs);

        let mut too_many = good.clone();
        too_many[2] = 9; // count byte: promises more records than follow
        assert_eq!(
            decode_envelope_payload(&too_many).unwrap_err().kind(),
            "corruption"
        );

        let mut too_few = good;
        too_few[2] = 1; // records past the promised count
        assert_eq!(
            decode_envelope_payload(&too_few).unwrap_err().kind(),
            "corruption"
        );
    }

    #[test]
    fn envelope_frame_size_precompute_is_exact() {
        let recs = envelope_point_records();
        let refs: Vec<RecordRef<'_>> = recs.iter().map(|r| r.as_ref()).collect();
        for schema in [
            None,
            Some(ENVELOPE_SCHEMA_PER_CF),
            Some(ENVELOPE_SCHEMA_UNIFIED),
        ] {
            let predicted = frame_payload_len(schema, &refs);
            let buf = encode_frame(schema, &refs);
            assert_eq!(buf.len(), HEADER_SIZE + predicted, "schema {schema:?}");
        }
        // The documented cost: +1 byte per point record versus legacy (the kind
        // uvarint replaces nothing; modifiers replace the flags byte).
        let legacy = frame_payload_len(None, &refs);
        let env = frame_payload_len(Some(ENVELOPE_SCHEMA_PER_CF), &refs);
        // +1 per record (the kind uvarint; modifiers replace the flags byte),
        // plus the 3-byte envelope header (tag, schema, count).
        assert_eq!(env - legacy, refs.len() + 3);
    }

    /// The frozen envelope fixtures pin the wire bytes, so a later change to
    /// the field order or the discriminator is a test failure rather than a
    /// silent format break.
    #[test]
    fn envelope_golden_bytes() {
        for (name, schema, recs) in [
            (
                "wal_v2_envelope_schema1.bin",
                ENVELOPE_SCHEMA_PER_CF,
                envelope_point_records(),
            ),
            ("wal_v2_envelope_schema2.bin", ENVELOPE_SCHEMA_UNIFIED, {
                let mut key = 0x0123_4567_89ab_cdefu64.to_be_bytes().to_vec();
                key.extend_from_slice(b"user-key");
                let mut del = 0x0123_4567_89ab_cdefu64.to_be_bytes().to_vec();
                del.extend_from_slice(b"gone");
                vec![
                    Record {
                        key,
                        value: b"v".to_vec(),
                        seq: 7,
                        ..Default::default()
                    },
                    Record {
                        key: del,
                        seq: 8,
                        tombstone: true,
                        ..Default::default()
                    },
                ]
            }),
        ] {
            let bytes = std::fs::read(crate::util::phase1_fixture(name)).unwrap();
            let refs: Vec<RecordRef<'_>> = recs.iter().map(|r| r.as_ref()).collect();
            assert_eq!(encode_frame(Some(schema), &refs), bytes, "{name}");
            // And the committed bytes decode back to the same records.
            let got = decode_envelope_payload(&bytes[HEADER_SIZE..]).unwrap();
            assert_eq!(got.len(), recs.len(), "{name}");
            for (g, w) in got.iter().zip(recs.iter()) {
                assert_eq!(g.key, w.key, "{name}");
                assert_eq!(g.value, w.value, "{name}");
                assert_eq!(g.seq, w.seq, "{name}");
                assert_eq!(g.ttl, w.ttl, "{name}");
                assert_eq!(g.tombstone, w.tombstone, "{name}");
                assert_eq!(g.single_delete, w.single_delete, "{name}");
            }
        }
    }

    /// The envelope decoder must be total over arbitrary bytes, like every
    /// other decoder in this feature.
    #[test]
    fn fuzz_decode_envelope_never_panics() {
        let seeds = [
            envelope_payload(ENVELOPE_SCHEMA_PER_CF, &envelope_point_records()),
            envelope_payload(ENVELOPE_SCHEMA_UNIFIED, &envelope_point_records()),
        ];
        let mut rng = crate::util::FuzzRng::new(0x1234_5678_9ABC_DEF0);
        for seed in &seeds {
            for _ in 0..2000 {
                let mut case = crate::util::fuzz_mutate(&mut rng, seed);
                // decode_envelope is only ever called on a payload whose first
                // byte is the tag (replay dispatches on it).
                if case.is_empty() {
                    case.push(ENVELOPE_TAG);
                }
                case[0] = ENVELOPE_TAG;
                let _ = decode_envelope_payload(&case);
            }
        }
    }

    #[test]
    fn new_wal_creation_propagates_parent_sync_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let calls = std::sync::atomic::AtomicUsize::new(0);

        let err = Wal::open_inner(&path, SyncMode::Full, Duration::ZERO, |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            Err(std::io::Error::other("injected parent sync failure").into())
        })
        .expect_err("a new WAL must not open when its directory sync fails");

        assert!(matches!(err, OndaError::Io(_)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn existing_wal_does_not_require_creation_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        drop(Wal::open(&path, SyncMode::Full, Duration::ZERO).unwrap());
        let calls = std::sync::atomic::AtomicUsize::new(0);

        drop(
            Wal::open_inner(&path, SyncMode::Full, Duration::ZERO, |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap(),
        );

        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_append_replay_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = std::sync::Arc::new(Wal::open(&path, SyncMode::None, Duration::ZERO).unwrap());
        let threads = 8;
        let batches = 500;
        let per_batch = 10;
        let mut handles = Vec::new();
        for t in 0..threads {
            let w = wal.clone();
            handles.push(std::thread::spawn(move || {
                for b in 0..batches {
                    let base = ((t * batches + b) * per_batch) as u64;
                    let keys: Vec<Vec<u8>> = (0..per_batch)
                        .map(|i| format!("k{:010}", base + i as u64).into_bytes())
                        .collect();
                    let recs: Vec<RecordRef<'_>> = keys
                        .iter()
                        .enumerate()
                        .map(|(i, k)| RecordRef {
                            key: k,
                            value: b"v",
                            seq: base + i as u64 + 1,
                            ttl: 0,
                            tombstone: false,
                            single_delete: false,
                        })
                        .collect();
                    w.append_batch(&recs).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        wal.close().unwrap();
        let total = threads * batches * per_batch;
        let mut seen = vec![false; total + 1];
        let mut count = 0usize;
        let last = Wal::replay(&path, |rec| {
            let ReplayRecord::Point(r) = rec;
            assert!(!seen[r.seq as usize], "duplicate seq {}", r.seq);
            seen[r.seq as usize] = true;
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, total, "lost records");
        assert_eq!(last, total as u64);
    }

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
        let last = Wal::replay(&path, |rec| {
            let ReplayRecord::Point(r) = rec;
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
        Wal::replay(&path, |rec| {
            let ReplayRecord::Point(r) = rec;
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
        Wal::replay(&path, |rec| {
            let ReplayRecord::Point(r) = rec;
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

    #[test]
    fn interval_sync_flushes_each_dirty_generation_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::open(&path, SyncMode::Interval, Duration::from_secs(60)).unwrap();
        let syncs = Arc::new(AtomicU64::new(0));
        wal.set_sync_counter(syncs.clone());

        wal.append(rec("a", "1", 1)).unwrap();
        sync_dirty_files(&wal.shared);
        assert_eq!(syncs.load(Ordering::Relaxed), WAL_STRIPES as u64);

        sync_dirty_files(&wal.shared);
        assert_eq!(
            syncs.load(Ordering::Relaxed),
            WAL_STRIPES as u64,
            "an idle interval must not repeat the previous generation's sync"
        );
    }
}
