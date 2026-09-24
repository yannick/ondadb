//! Write-ahead log.
//!
//! Every stripe file of a WAL generation (a *segment*) starts with a 32-byte
//! header ([`crate::format::wal_segment`]): the magic `YOLODBWL`, a version,
//! the layout (per-CF or unified) and the generation, under a CRC32-C. It is
//! written and fsynced before the first frame, so replay can refuse a foreign
//! or 0.9 file at byte 0 instead of mid-frame, and a torn header can only ever
//! sit on a file that holds no frame.
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
//!
//! **User-space write buffer** (opt-in, [`Options::wal_write_buffer_size`],
//! wavesdb `WALWriteBufferSize`): under `Interval` and `None` each stripe may
//! coalesce whole frames in memory and hand them to the OS in one `write`
//! when the buffer fills, at every interval tick, and before any fsync,
//! rotation or close. Frames are only ever appended whole to the buffer, so a
//! flush is a run of complete frames; a crash that tears the flush leaves a
//! torn frame at the tail, which replay already discards (the frame CRC covers
//! the whole payload), so replay still yields a prefix of whole batches. The
//! cost is the documented one: an acknowledged commit still in the buffer dies
//! with the process. `Full` ignores the buffer — every commit is written and
//! fsynced before it is acknowledged, so buffering could only add a copy.
//!
//! [`Options::wal_write_buffer_size`]: crate::Options::wal_write_buffer_size

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

/// Which WAL a segment belongs to, as its header records it: the layout and the
/// generation. Replay checks both against what the file's name and place say,
/// so a segment moved between databases or layouts is refused rather than
/// replayed into the wrong memtable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentId {
    pub layout: crate::manifest::WalLayout,
    pub generation: u64,
}

impl SegmentId {
    /// A per-column-family WAL's generation `generation`.
    pub fn per_cf(generation: u64) -> SegmentId {
        SegmentId {
            layout: crate::manifest::WalLayout::PerColumnFamily,
            generation,
        }
    }

    /// The unified WAL's generation `generation`.
    pub fn unified(generation: u64) -> SegmentId {
        SegmentId {
            layout: crate::manifest::WalLayout::Unified,
            generation,
        }
    }

    fn layout_byte(self) -> u8 {
        match self.layout {
            crate::manifest::WalLayout::PerColumnFamily => {
                crate::format::wal_segment::LAYOUT_PER_CF
            }
            crate::manifest::WalLayout::Unified => crate::format::wal_segment::LAYOUT_UNIFIED,
        }
    }
}

/// Encode the 32-byte segment header for `id`.
pub(crate) fn encode_segment_header(id: SegmentId) -> [u8; SEGMENT_HEADER_LEN] {
    use crate::format::wal_segment::*;
    let mut b = [0u8; HEADER_LEN];
    b[..8].copy_from_slice(&MAGIC);
    put_u32(&mut b[8..], VERSION);
    b[12] = id.layout_byte();
    // 13..16 reserved (zero)
    b[16..24].copy_from_slice(&id.generation.to_le_bytes());
    // 24..28 reserved (zero)
    let crc = checksum(&b[..28]);
    put_u32(&mut b[28..], crc);
    b
}

/// Width of the segment header, and the offset of the first frame.
pub(crate) const SEGMENT_HEADER_LEN: usize = crate::format::wal_segment::HEADER_LEN;

/// What a segment's first bytes say.
#[derive(Debug, PartialEq, Eq)]
enum SegmentHead {
    /// A valid header for the expected segment: frames follow.
    Valid,
    /// No frame can follow: a zero-length file (created, never written) or a
    /// header torn by a crash. The header is fsynced before the first frame is
    /// appended, so a torn one can only sit on a file that holds no frame —
    /// the same crash residue a torn tail is, and just as clean.
    Empty,
}

/// Classify a segment from its first `min(file_len, 32)` bytes.
///
/// A torn header is recognized narrowly — a short prefix of the magic (or of
/// zeros), or a full-width header that fails its CRC or reads all-zero on a
/// file holding nothing past it — so that a 0.9 WAL (frames from byte 0) or a
/// foreign file is refused as `UnsupportedFormat` at byte 0, never mistaken for
/// an empty segment. Past those checks: an unknown version or layout byte is
/// `UnsupportedFormat`; a header naming a different layout or generation than
/// the file's name and place, or a non-zero reserved byte, is `Corruption`.
fn check_segment_head(head: &[u8], file_len: u64, expect: SegmentId) -> Result<SegmentHead> {
    use crate::format::wal_segment::*;
    let foreign = || {
        OndaError::UnsupportedFormat(
            "wal: segment has no yoloDB header (YOLODBWL) — a foreign file, or an ondaDB 0.9 \
             WAL, which is readable only through legacy_onda"
                .into(),
        )
    };
    if file_len == 0 {
        return Ok(SegmentHead::Empty);
    }
    if head.len() < HEADER_LEN {
        let n = head.len().min(8);
        if head[..n] == MAGIC[..n] || head.iter().all(|&b| b == 0) {
            return Ok(SegmentHead::Empty); // torn while the header was written
        }
        return Err(foreign());
    }
    let only_header = file_len == HEADER_LEN as u64;
    if head[..8] != MAGIC {
        if only_header && head.iter().all(|&b| b == 0) {
            return Ok(SegmentHead::Empty); // size persisted, data not
        }
        return Err(foreign());
    }
    let version = read_u32(&head[8..]);
    if version != VERSION {
        return Err(OndaError::UnsupportedFormat(format!(
            "wal segment version {version} is not implemented by this binary"
        )));
    }
    if read_u32(&head[28..]) != checksum(&head[..28]) {
        if only_header {
            return Ok(SegmentHead::Empty);
        }
        return Err(OndaError::Corruption(
            "wal segment header: checksum mismatch".into(),
        ));
    }
    if head[13..16].iter().any(|&b| b != 0) || head[24..28].iter().any(|&b| b != 0) {
        return Err(OndaError::Corruption(
            "wal segment header: reserved bytes are not zero".into(),
        ));
    }
    let layout = head[12];
    if layout != LAYOUT_PER_CF && layout != LAYOUT_UNIFIED {
        return Err(OndaError::UnsupportedFormat(format!(
            "wal segment layout {layout} is not implemented by this binary"
        )));
    }
    if layout != expect.layout_byte() {
        return Err(OndaError::Corruption(format!(
            "wal segment header: layout {layout} where {} was expected",
            expect.layout_byte()
        )));
    }
    let generation = u64::from_le_bytes(head[16..24].try_into().unwrap());
    if generation != expect.generation {
        return Err(OndaError::Corruption(format!(
            "wal segment header: generation {generation} where {} was expected",
            expect.generation
        )));
    }
    Ok(SegmentHead::Valid)
}

/// One logical WAL entry (owned; produced by replay).
#[derive(Debug, Clone)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    /// Absolute Unix-nanosecond expiry; `0` for none.
    pub ttl: i64,
    /// Record kind ([`KIND_PUT`](crate::format::KIND_PUT) and friends).
    ///
    /// One field rather than the `(tombstone, single_delete)` pair it replaced:
    /// the pair could express `single_delete && !tombstone`, which no writer
    /// produces, and 1.1 would have had to add a third boolean whose
    /// combinations with the other two are equally meaningless. A kind has
    /// exactly one value, and [`check_kind`](crate::format::check_kind) is the
    /// single place that decides which values this binary honors.
    pub kind: u64,
}

impl Default for Record {
    fn default() -> Record {
        Record {
            key: Vec::new(),
            value: Vec::new(),
            seq: 0,
            ttl: 0,
            kind: crate::format::KIND_PUT,
        }
    }
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
    /// See [`Record::kind`].
    pub kind: u64,
}

impl RecordRef<'_> {
    /// Whether this record hides older versions of its key.
    #[inline]
    pub fn tombstone(&self) -> bool {
        self.kind == crate::format::KIND_DELETE || self.kind == crate::format::KIND_SINGLE_DELETE
    }
    #[inline]
    pub fn single_delete(&self) -> bool {
        self.kind == crate::format::KIND_SINGLE_DELETE
    }
    /// Whether this record is a merge operand (1.1) rather than a point write.
    #[inline]
    pub fn is_merge(&self) -> bool {
        self.kind == crate::format::KIND_MERGE
    }
}

impl Record {
    /// See [`RecordRef::tombstone`].
    #[inline]
    pub fn tombstone(&self) -> bool {
        self.kind == crate::format::KIND_DELETE || self.kind == crate::format::KIND_SINGLE_DELETE
    }
    #[inline]
    pub fn single_delete(&self) -> bool {
        self.kind == crate::format::KIND_SINGLE_DELETE
    }
    #[inline]
    pub fn is_merge(&self) -> bool {
        self.kind == crate::format::KIND_MERGE
    }
}

impl Record {
    /// Borrowed view of this record.
    pub fn as_ref(&self) -> RecordRef<'_> {
        RecordRef {
            key: &self.key,
            value: &self.value,
            seq: self.seq,
            ttl: self.ttl,
            kind: self.kind,
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
    /// A range delete (kind 5): `[start, end)` is deleted at `seq`.
    ///
    /// Under [`ENVELOPE_SCHEMA_UNIFIED`] **both** bounds still carry the 8-byte
    /// CF-id prefix, exactly as a point record's key does; the unified store
    /// strips them at replay.
    RangeDelete {
        start: Vec<u8>,
        end: Vec<u8>,
        seq: u64,
    },
    /// A durable prepare (kind 16, 3.2): the whole frame decoded as a unit, so
    /// recovery never has to infer the grouping from callback adjacency.
    ///
    /// Every `records` entry carries `seq == 0` — a prepared writeset has not
    /// committed and must never raise the replay watermark. Its keys carry the
    /// 8-byte CF-id prefix, exactly as a committed unified point record's do.
    Prepare {
        id: [u8; 16],
        cf_ids: Vec<u64>,
        records: Vec<Record>,
    },
    /// A decision for a prepared transaction (kinds 17 and 18, 3.2).
    ///
    /// `commit` is `Some((commit_seq, count))` for a commit decision — the
    /// first sequence of the reserved block and the record count, which is what
    /// lets replay raise the watermark to `commit_seq + count - 1` without
    /// reading the prepare frame — and `None` for an abort.
    Decision {
        id: [u8; 16],
        commit: Option<(u64, u64)>,
    },
}

impl ReplayRecord {
    /// Sequence this record contributes to a replay's high-water mark.
    ///
    /// Control records contribute nothing: a prepare is uncommitted (its
    /// records carry the `seq == 0` sentinel) and a decision's sequences are
    /// raised explicitly in recovery pass 2, after the decision is matched to
    /// its prepare.
    pub fn replay_seq(&self) -> u64 {
        match self {
            ReplayRecord::Point(r) => r.seq,
            ReplayRecord::RangeDelete { seq, .. } => *seq,
            ReplayRecord::Prepare { .. } | ReplayRecord::Decision { .. } => 0,
        }
    }
}

/// A range delete as the commit path hands it to the WAL: bounds borrowed from
/// the transaction's write buffer.
///
/// The bounds occupy the same `a`/`b` slots a point record uses for key and
/// value, so the exact frame-size precompute is one more arm rather than a
/// second code path.
#[derive(Debug, Clone, Copy)]
pub struct RangeRef<'a> {
    pub start: &'a [u8],
    pub end: &'a [u8],
    pub seq: u64,
}

/// One record of an envelope batch.
///
/// A commit containing a range delete writes its **whole** batch as one
/// envelope frame — points and ranges together — because WAL batch atomicity
/// (invariant 3) is per frame: splitting the commit across a legacy frame and
/// an envelope frame would let replay surface half of it.
#[derive(Debug, Clone, Copy)]
pub enum EnvelopeRecord<'a> {
    Point(RecordRef<'a>),
    Range(RangeRef<'a>),
    /// A transaction-control record (kinds 16–18, 3.2). The generic `a`/`b`
    /// slots carry whatever the kind defines; `seq` is always the `0` sentinel,
    /// so a control frame can never raise the replay watermark.
    Control(ControlRef<'a>),
}

/// A transaction-control record as the prepare/decision paths hand it to the
/// WAL: the kind plus the two generic slots, borrowed from a caller scratch
/// buffer.
#[derive(Debug, Clone, Copy)]
pub struct ControlRef<'a> {
    pub kind: u64,
    pub a: &'a [u8],
    pub b: &'a [u8],
}

impl EnvelopeRecord<'_> {
    /// Sequence number this record commits at.
    pub fn seq(&self) -> u64 {
        match self {
            EnvelopeRecord::Point(r) => r.seq,
            EnvelopeRecord::Range(r) => r.seq,
            EnvelopeRecord::Control(_) => 0,
        }
    }
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

/// The point record inside `rec`, for the legacy (kind-less) frame form.
///
/// A range delete cannot be expressed without a kind field, so a caller that
/// reaches here with one has mixed the two forms — a bug in this crate, not
/// input a database can be handed. The commit path chooses the envelope form
/// for the whole batch the moment it holds a range delete.
fn legacy_point<'a>(rec: &EnvelopeRecord<'a>) -> RecordRef<'a> {
    match rec {
        EnvelopeRecord::Point(r) => *r,
        EnvelopeRecord::Range(_) => {
            unreachable!("a range delete requires an envelope frame (kind 5)")
        }
        EnvelopeRecord::Control(_) => {
            unreachable!("a transaction-control record requires an envelope frame (kinds 16-18)")
        }
    }
}

/// Wrap a point-only batch as envelope records.
///
/// One small allocation per frame on a path that already builds the frame
/// buffer; it keeps `encode_frame` single-shaped instead of generic over the
/// record form.
fn point_envelope<'a>(recs: &[RecordRef<'a>]) -> Vec<EnvelopeRecord<'a>> {
    recs.iter().copied().map(EnvelopeRecord::Point).collect()
}

/// Encoded length of one envelope record, matching [`encode_envelope_record`]
/// byte for byte.
///
/// The exact frame-size precompute is not an optimization detail: growth
/// reallocations re-copy the whole payload and dominated large-value commits
/// (see [`Wal::append_batch`]).
fn envelope_record_len(rec: EnvelopeRecord<'_>) -> usize {
    match rec {
        EnvelopeRecord::Point(r) => {
            let mods = if r.ttl != 0 {
                crate::format::modifiers::HAS_TTL
            } else {
                0
            };
            uvarint_len(r.kind)
                + uvarint_len(mods)
                + uvarint_len(r.key.len() as u64)
                + uvarint_len(r.value.len() as u64)
                + uvarint_len(r.seq)
                + if r.ttl != 0 { varint_len(r.ttl) } else { 0 }
                + r.key.len()
                + r.value.len()
        }
        // Kind 5 carries no value and no modifiers: the `a`/`b` slots hold the
        // two bounds, so its shape is the point one with the TTL removed.
        EnvelopeRecord::Range(r) => {
            uvarint_len(crate::format::KIND_RANGE_DELETE)
                + uvarint_len(0)
                + uvarint_len(r.start.len() as u64)
                + uvarint_len(r.end.len() as u64)
                + uvarint_len(r.seq)
                + r.start.len()
                + r.end.len()
        }
        // Kinds 16–18: no modifiers, no TTL, and the `seq = 0` sentinel — one
        // byte, since `uvarint_len(0) == 1`.
        EnvelopeRecord::Control(r) => {
            uvarint_len(r.kind)
                + uvarint_len(0)
                + uvarint_len(r.a.len() as u64)
                + uvarint_len(r.b.len() as u64)
                + uvarint_len(0)
                + r.a.len()
                + r.b.len()
        }
    }
}

/// Append one envelope record to `dst`.
///
/// Field order matches the legacy record deliberately (`alen, blen, seq, ttl?,
/// a, b`), so the size precompute above stays a one-line variation on the
/// legacy one. The `a`/`b` slots are named generically because kind 5 (1.2)
/// puts a range's `(start, end)` in them rather than `(key, value)`.
fn encode_envelope_record(dst: &mut Vec<u8>, rec: EnvelopeRecord<'_>) {
    match rec {
        EnvelopeRecord::Point(r) => {
            debug_assert!(
                crate::format::check_kind(r.kind).is_ok(),
                "envelope record with unimplemented kind {}",
                r.kind
            );
            let mods = if r.ttl != 0 {
                crate::format::modifiers::HAS_TTL
            } else {
                0
            };
            append_uvarint(dst, r.kind);
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
        EnvelopeRecord::Range(r) => {
            append_uvarint(dst, crate::format::KIND_RANGE_DELETE);
            append_uvarint(dst, 0); // no modifiers: a range delete has no TTL
            append_uvarint(dst, r.start.len() as u64);
            append_uvarint(dst, r.end.len() as u64);
            append_uvarint(dst, r.seq);
            dst.extend_from_slice(r.start);
            dst.extend_from_slice(r.end);
        }
        EnvelopeRecord::Control(r) => {
            debug_assert!(crate::format::is_control_kind(r.kind));
            append_uvarint(dst, r.kind);
            append_uvarint(dst, 0); // no modifiers: control records have no TTL
            append_uvarint(dst, r.a.len() as u64);
            append_uvarint(dst, r.b.len() as u64);
            // The `seq = 0` sentinel. `Wal::replay_file` derives its high-water
            // mark from record sequences, and a prepared writeset must never
            // raise it: those records are uncommitted and may yet be aborted.
            append_uvarint(dst, 0);
            dst.extend_from_slice(r.a);
            dst.extend_from_slice(r.b);
        }
    }
}

/// One envelope record with its slots still borrowed from the frame payload.
///
/// Split out of [`decode_envelope_record`] because a control frame (3.2) is
/// decoded as a *unit* — the shape rules relate its records to each other — and
/// re-deriving the field order in a second decoder is exactly how the two would
/// drift apart.
struct RawRecord<'a> {
    kind: u64,
    mods: u64,
    seq: u64,
    ttl: i64,
    a: &'a [u8],
    b: &'a [u8],
}

/// Decode one record's fields from the front of `p`, returning it and the bytes
/// consumed. Field-level validity only: nothing here knows which frame the
/// record sits in.
fn decode_raw_record(p: &[u8]) -> Result<(RawRecord<'_>, usize)> {
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
    Ok((
        RawRecord {
            kind,
            mods,
            seq,
            ttl,
            a: &p[off..off + alen],
            b: &p[off + alen..off + need],
        },
        off + need,
    ))
}

/// Decode one **data** envelope record from the front of `p`, returning it and
/// the bytes consumed.
fn decode_envelope_record(p: &[u8]) -> Result<(ReplayRecord, usize)> {
    let (r, used) = decode_raw_record(p)?;
    let (kind, mods, seq) = (r.kind, r.mods, r.seq);
    let (alen, blen) = (r.a.len(), r.b.len());
    // A control kind is only meaningful as the FIRST record of its own frame,
    // where `decode_envelope` dispatches on it. Reaching it here means it was
    // spliced into a data stream — bytes no writer produces.
    if crate::format::is_control_kind(kind) {
        return Err(OndaError::Corruption(format!(
            "wal: transaction-control kind {kind} outside a control frame"
        )));
    }
    if kind == crate::format::KIND_RANGE_DELETE {
        // No value to separate and no TTL, so any modifier here is bytes no
        // writer produces.
        if mods != 0 {
            return Err(OndaError::Corruption(
                "wal: range-delete record carries modifiers".into(),
            ));
        }
        // Empty bounds are refused at the API: an empty `end` would delete
        // nothing, and an empty `start` is indistinguishable from "absent".
        if alen == 0 || blen == 0 {
            return Err(OndaError::Corruption(
                "wal: range-delete record has an empty bound".into(),
            ));
        }
        return Ok((
            ReplayRecord::RangeDelete {
                start: r.a.to_vec(),
                end: r.b.to_vec(),
                seq,
            },
            used,
        ));
    }
    Ok((ReplayRecord::Point(raw_to_point(&r)), used))
}

/// The [`Record`] a data raw record describes. The kind is carried through
/// verbatim — 1.1's operand has no other spelling.
fn raw_to_point(r: &RawRecord<'_>) -> Record {
    Record {
        key: r.a.to_vec(),
        value: r.b.to_vec(),
        seq: r.seq,
        ttl: r.ttl,
        kind: r.kind,
    }
}

/// The 16-byte transaction id in a control record's `a` slot.
fn control_id(a: &[u8]) -> Result<[u8; 16]> {
    a.try_into().map_err(|_| {
        OndaError::Corruption(format!(
            "wal: transaction-control id is {} bytes, not 16",
            a.len()
        ))
    })
}

/// Decode a **control** frame (3.2) as a unit: its records relate to each
/// other, so the shape rules are enforced here rather than inferred by a caller
/// from callback adjacency.
///
/// Every record must carry the `seq == 0` sentinel and no modifiers. Anything
/// else is `Corruption`: these bytes are only ever written by this crate's
/// prepare and decision paths, which produce exactly one shape each.
fn decode_control_frame(payload: &[u8], count: u64) -> Result<ReplayRecord> {
    let corrupt = |why: &str| OndaError::Corruption(format!("wal: control frame {why}"));
    let mut p = payload;
    let mut raws = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if p.is_empty() {
            return Err(corrupt("promises more records than exist"));
        }
        let (r, used) = decode_raw_record(p)?;
        if r.seq != 0 {
            return Err(corrupt("record carries a non-zero sequence"));
        }
        if r.mods & !crate::format::modifiers::HAS_TTL != 0 {
            return Err(corrupt("record carries an unexpected modifier"));
        }
        raws.push((r, used));
        p = &p[used..];
    }
    if !p.is_empty() {
        return Err(corrupt("has records past the promised count"));
    }
    let head = &raws[0].0;
    match head.kind {
        crate::format::KIND_PREPARE => {
            if head.mods != 0 {
                return Err(corrupt("prepare record carries modifiers"));
            }
            let id = control_id(head.a)?;
            if head.b.len() % 8 != 0 {
                return Err(corrupt("prepare cf-id list is not a whole number of ids"));
            }
            let cf_ids: Vec<u64> = head
                .b
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            let mut records = Vec::with_capacity(raws.len() - 1);
            for (r, _) in &raws[1..] {
                // The writeset is replayed through the same memtable path a
                // commit uses, so it may hold exactly what that path has a
                // shape for: the three point kinds and 1.1's merge operand,
                // which is one key and one value like any of them. A range
                // delete (two keys, no value) and a nested control record are
                // refused — `Txn::prepare` refuses them at the API too, so
                // these bytes are ones no writer produces.
                if !crate::format::is_point_kind(r.kind) && r.kind != crate::format::KIND_MERGE {
                    return Err(corrupt("prepare holds a record that is not a point write"));
                }
                records.push(raw_to_point(r));
            }
            Ok(ReplayRecord::Prepare {
                id,
                cf_ids,
                records,
            })
        }
        kind @ (crate::format::KIND_COMMIT_DECISION | crate::format::KIND_ABORT_DECISION) => {
            if raws.len() != 1 {
                return Err(corrupt("decision is not a single record"));
            }
            if head.mods != 0 {
                return Err(corrupt("decision record carries modifiers"));
            }
            let id = control_id(head.a)?;
            if kind == crate::format::KIND_ABORT_DECISION {
                if !head.b.is_empty() {
                    return Err(corrupt("abort decision carries a payload"));
                }
                return Ok(ReplayRecord::Decision { id, commit: None });
            }
            if head.b.len() != 16 {
                return Err(corrupt("commit decision payload is not 16 bytes"));
            }
            let commit_seq = u64::from_le_bytes(head.b[..8].try_into().unwrap());
            let count = u64::from_le_bytes(head.b[8..].try_into().unwrap());
            Ok(ReplayRecord::Decision {
                id,
                commit: Some((commit_seq, count)),
            })
        }
        // Unreachable: `decode_envelope` only routes here on a control kind.
        kind => Err(corrupt(&format!("leads with kind {kind}"))),
    }
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
    // A control frame (3.2) is decoded as a unit. Dispatching on the FIRST
    // record's kind costs one uvarint peek on the data path — the alternative,
    // materializing every frame's records before classifying it, would put an
    // allocation on every replayed commit.
    if let Some((kind, _)) = uvarint(p) {
        if crate::format::is_control_kind(kind) {
            if count == 0 {
                return Err(corrupt());
            }
            f(decode_control_frame(p, count)?)?;
            // Control records never raise the watermark; see `replay_seq`.
            return Ok(0);
        }
    }
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
    // The legacy record has no kind field: everything it can say is said by the
    // flags byte, so a kind outside the three point kinds cannot be written
    // here at all. Callers route such a batch to an envelope frame instead
    // (`ColumnFamily::apply_commit`); reaching this with one is an engine bug.
    debug_assert!(
        crate::format::is_point_kind(r.kind),
        "legacy WAL record cannot carry kind {}",
        r.kind
    );
    let (tombstone, single_delete) = (r.tombstone(), r.single_delete());
    crate::format::debug_check_entry_flags(tombstone, single_delete, false);
    let fl = crate::format::normalized_entry_flags(tombstone, single_delete, r.ttl != 0, false);
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
        kind: crate::format::point_kind(fl & flags::TOMBSTONE != 0, fl & flags::SINGLE_DELETE != 0),
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
fn frame_payload_len(schema: Option<u64>, recs: &[EnvelopeRecord<'_>]) -> usize {
    match schema {
        // The legacy stream has no record kind, so it can only carry points;
        // `encode_frame` panics on a range record here for the same reason.
        None => recs
            .iter()
            .map(|rec| {
                let r = legacy_point(rec);
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
fn encode_frame(schema: Option<u64>, recs: &[EnvelopeRecord<'_>]) -> Vec<u8> {
    let body = frame_payload_len(schema, recs);
    let mut buf = Vec::with_capacity(HEADER_SIZE + body);
    buf.extend_from_slice(&[0u8; HEADER_SIZE]);
    match schema {
        None => {
            for r in recs {
                encode_record_body(&mut buf, legacy_point(r));
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

/// One stripe's file and its user-space write buffer.
///
/// The buffer only ever holds **whole frames** (each appended under the stripe
/// mutex in one piece), which is what makes a torn flush equivalent to a torn
/// unbuffered append: replay sees a prefix of whole frames and then a torn one.
struct Stripe {
    file: File,
    buf: Vec<u8>,
}

impl Stripe {
    /// Hand every buffered frame to the OS in one `write_all`.
    ///
    /// The buffer is cleared even when the write fails: a partial write may
    /// already have landed some of its bytes, and writing them again would
    /// put a second copy of a frame's head after a torn one. The caller
    /// poisons the database instead — the lost frames were acknowledged.
    fn flush(&mut self, writes: &AtomicU64) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let r = self.file.write_all(&self.buf);
        self.buf.clear();
        writes.fetch_add(1, Ordering::Relaxed);
        r
    }
}

struct Shared {
    /// One file per stripe (a single entry under [`SyncMode::Full`]).
    files: Vec<Mutex<Option<Stripe>>>,
    sync: SyncMode,
    /// Per-stripe user-space buffer capacity in bytes; 0 = unbuffered (and
    /// always 0 under [`SyncMode::Full`]).
    buf_cap: usize,
    /// Logical size: every frame appended, buffered or not. Rotation keys off
    /// it, and a buffered frame is as much part of the generation as a
    /// written one.
    size: AtomicI64,
    dirty: AtomicBool,
    /// Some stripe buffer may hold frames the OS has not seen yet.
    buffered: AtomicBool,
    /// `write` calls issued for frames (diagnostics and the coalescing tests).
    writes: AtomicU64,
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

    /// Flush one stripe's buffer, poisoning the database on failure: the
    /// frames in it were acknowledged, so losing them is a durability failure
    /// exactly like a failed fsync.
    fn flush_stripe(&self, st: &mut Stripe) -> Result<()> {
        st.flush(&self.writes).map_err(|e| {
            self.poison(format!("wal buffered write failed: {e}"));
            e.into()
        })
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
    /// Open (creating if needed) the WAL segment `id` at `path` for
    /// appending. Under [`SyncMode::Interval`] a background thread fsyncs every
    /// `interval`.
    ///
    /// Every stripe file gets its segment header before anything else: a new
    /// (or empty, or torn-header) file is given one and **fsynced** before the
    /// open returns, so no frame can ever be appended ahead of a durable
    /// header. An existing file must carry `id`'s header.
    pub fn open(
        path: impl AsRef<Path>,
        mode: SyncMode,
        interval: Duration,
        id: SegmentId,
    ) -> Result<Wal> {
        Self::open_buffered(path, mode, interval, id, 0)
    }

    /// [`open`](Self::open) with a per-stripe user-space write buffer of
    /// `buffer_bytes` (0 = unbuffered). Ignored under [`SyncMode::Full`]; see
    /// the module docs for the durability trade.
    ///
    /// A buffered WAL always runs the background thread — under
    /// [`SyncMode::None`] too, where it only flushes (no fsync) — so buffered
    /// frames reach the OS within one `interval` even when the buffer stays
    /// cold.
    pub fn open_buffered(
        path: impl AsRef<Path>,
        mode: SyncMode,
        interval: Duration,
        id: SegmentId,
        buffer_bytes: usize,
    ) -> Result<Wal> {
        Self::open_inner(
            path.as_ref(),
            mode,
            interval,
            id,
            buffer_bytes,
            crate::util::sync_parent_dir,
        )
    }

    fn open_inner(
        path: &Path,
        mode: SyncMode,
        interval: Duration,
        id: SegmentId,
        buffer_bytes: usize,
        sync_parent: impl FnOnce(&Path) -> Result<()>,
    ) -> Result<Wal> {
        let buf_cap = if mode == SyncMode::Full {
            0
        } else {
            buffer_bytes
        };
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
            let mut f = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(stripe)?;
            let len = f.metadata()?.len();
            let mut head = vec![0u8; (len as usize).min(SEGMENT_HEADER_LEN)];
            if !head.is_empty() {
                std::io::Seek::seek(&mut f, std::io::SeekFrom::Start(0))?;
                f.read_exact(&mut head)?;
            }
            if check_segment_head(&head, len, id)? == SegmentHead::Empty {
                // Nothing can follow a missing or torn header, so rewriting it
                // loses nothing. Durable before the first frame — the ordering
                // the torn-header rule above depends on.
                f.set_len(0)?;
                f.write_all(&encode_segment_header(id))?;
                f.sync_data()?;
            }
            size += f.metadata()?.len() as i64;
            files.push(Mutex::new(Some(Stripe {
                file: f,
                buf: Vec::with_capacity(buf_cap),
            })));
        }
        if created {
            sync_parent(path)?;
        }
        let shared = Arc::new(Shared {
            files,
            sync: mode,
            buf_cap,
            size: AtomicI64::new(size),
            dirty: AtomicBool::new(false),
            buffered: AtomicBool::new(false),
            writes: AtomicU64::new(0),
            qstate: Mutex::new(QueueState {
                queue: Vec::new(),
                flushing: false,
            }),
            poison: Mutex::new(None),
            syncs: Mutex::new(None),
        });
        let (mut stop_tx, mut bg) = (None, None);
        if mode == SyncMode::Interval || buf_cap > 0 {
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
        let recs = point_envelope(recs);
        self.submit_frame(encode_frame(None, &recs))
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
        self.append_batch_envelope(schema, &point_envelope(recs))
    }

    /// [`append_batch_enveloped`](Self::append_batch_enveloped) for a batch
    /// that mixes point writes and range deletes (1.2).
    pub fn append_batch_envelope(&self, schema: u64, recs: &[EnvelopeRecord<'_>]) -> Result<()> {
        self.submit_frame(encode_frame(Some(schema), recs))
    }

    /// Append a durable **prepare** frame (kind 16, 3.2): the transaction id,
    /// the column families it touches, and its whole writeset, as ONE frame.
    ///
    /// One frame because a prepare is atomic exactly as a commit is (invariant
    /// 3): a torn tail must drop the id and its records together, never leave a
    /// registered reservation with half a writeset behind it. Every record
    /// carries `seq = 0` — nothing here has committed.
    ///
    /// The caller is responsible for the durability half: `append` does not
    /// fsync, so a prepare must `sync()` **this same handle** before it returns
    /// `Ok` (a rotation can replace the store's current handle in between).
    pub fn append_prepare(
        &self,
        schema: u64,
        id: &[u8; 16],
        cf_ids: &[u64],
        recs: &[RecordRef<'_>],
    ) -> Result<()> {
        let mut ids = Vec::with_capacity(cf_ids.len() * 8);
        for cf in cf_ids {
            ids.extend_from_slice(&cf.to_le_bytes());
        }
        let mut frame: Vec<EnvelopeRecord<'_>> = Vec::with_capacity(1 + recs.len());
        frame.push(EnvelopeRecord::Control(ControlRef {
            kind: crate::format::KIND_PREPARE,
            a: id,
            b: &ids,
        }));
        frame.extend(recs.iter().map(|r| {
            EnvelopeRecord::Point(RecordRef {
                // The sentinel is applied here rather than trusted from the
                // caller: a prepared record that kept a real sequence would
                // raise the replay watermark for a transaction that may abort.
                seq: 0,
                ..*r
            })
        }));
        self.append_batch_envelope(schema, &frame)
    }

    /// Append a **decision** frame for a prepared transaction (3.2):
    /// `Some((commit_seq, count))` writes the commit decision (kind 17),
    /// `None` the abort decision (kind 18).
    ///
    /// The commit decision carries both the first reserved sequence and the
    /// record count so it is self-sufficient: replay can raise the watermark to
    /// `commit_seq + count - 1` without having found the prepare frame.
    ///
    /// As with [`append_prepare`](Self::append_prepare), the caller must
    /// `sync()` this handle before treating the decision as durable.
    pub fn append_decision(
        &self,
        schema: u64,
        id: &[u8; 16],
        commit: Option<(u64, u64)>,
    ) -> Result<()> {
        let mut payload = Vec::new();
        let kind = match commit {
            Some((commit_seq, count)) => {
                payload.extend_from_slice(&commit_seq.to_le_bytes());
                payload.extend_from_slice(&count.to_le_bytes());
                crate::format::KIND_COMMIT_DECISION
            }
            None => crate::format::KIND_ABORT_DECISION,
        };
        self.append_batch_envelope(
            schema,
            &[EnvelopeRecord::Control(ControlRef {
                kind,
                a: id,
                b: &payload,
            })],
        )
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
            let st = match guard.as_mut() {
                Some(st) => st,
                None => return Err(OndaError::InvalidDb("wal closed".into())),
            };
            let cap = self.shared.buf_cap;
            if cap == 0 {
                st.file.write_all(&buf)?;
                self.shared.writes.fetch_add(1, Ordering::Relaxed);
            } else {
                // Whole frames only: flush what is there before a frame that
                // would overflow it, so the buffer never holds a frame head.
                if !st.buf.is_empty() && st.buf.len() + buf.len() > cap {
                    self.shared.flush_stripe(st)?;
                }
                if buf.len() >= cap {
                    // Too big to coalesce: the copy would buy nothing.
                    st.file.write_all(&buf)?;
                    self.shared.writes.fetch_add(1, Ordering::Relaxed);
                } else {
                    st.buf.extend_from_slice(&buf);
                    self.shared.buffered.store(true, Ordering::Relaxed);
                }
            }
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
            Some(st) => &mut st.file,
            None => return -10, // closed
        };
        for req in batch {
            if let Err(e) = f.write_all(&req.buf) {
                return OndaError::from(e).code();
            }
        }
        self.shared
            .writes
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
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

    /// Flush every stripe's write buffer, then fsync every stripe file.
    pub fn sync(&self) -> Result<()> {
        self.shared.dirty.store(false, Ordering::Relaxed);
        self.shared.buffered.store(false, Ordering::Relaxed);
        for file in &self.shared.files {
            let mut guard = file.lock();
            match guard.as_mut() {
                Some(st) => {
                    self.shared.flush_stripe(st)?;
                    if let Err(e) = st.file.sync_data() {
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

    /// Hand every buffered frame to the OS **without** an fsync: after this a
    /// process crash loses nothing appended before the call (a power loss
    /// still may, exactly as for an unbuffered `None`/`Interval` WAL). A no-op
    /// on an unbuffered WAL.
    pub fn flush_buffer(&self) -> Result<()> {
        flush_buffers(&self.shared)
    }

    /// Logical size in bytes: every frame appended, including frames still
    /// in the write buffer.
    pub fn size(&self) -> i64 {
        self.shared.size.load(Ordering::Relaxed)
    }

    /// `write` calls issued for frames so far (a buffered flush counts once).
    pub fn write_calls(&self) -> u64 {
        self.shared.writes.load(Ordering::Relaxed)
    }

    /// Flush the write buffer, fsync and close the underlying files. Safe to
    /// call more than once.
    ///
    /// Every stripe is flushed and closed even if an earlier one failed —
    /// the first error is returned — and a failed flush poisons the database:
    /// rotation discards this result, so the poison is what keeps a lost
    /// acknowledged frame from going unnoticed.
    pub fn close(&self) -> Result<()> {
        if let Some(tx) = self.stop_tx.lock().take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.bg.lock().take() {
            let _ = h.join();
        }
        let mut first_err = None;
        for file in &self.shared.files {
            if let Some(mut st) = file.lock().take() {
                let r = self
                    .shared
                    .flush_stripe(&mut st)
                    .and_then(|()| st.file.sync_data().map_err(Into::into));
                match r {
                    Ok(()) => self.shared.count_sync(),
                    Err(e) => {
                        first_err.get_or_insert(e);
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Replay records from the WAL segment `id` based at `path`, invoking `f`
    /// for each. Every stripe file is replayed; record order across stripes is
    /// not meaningful — sequence numbers define visibility.
    ///
    /// Each stripe must start with `id`'s segment header (see
    /// `check_segment_head`): a missing or foreign one is `UnsupportedFormat`
    /// at byte 0, a zero-length file or a torn header is an empty stripe. After
    /// it, a torn or checksum-failed frame at a stripe's tail ends that stripe
    /// cleanly (the expected result of a crash mid-write); each frame — one
    /// committed batch — replays atomically. A record that fails to decode
    /// *inside* a CRC-valid frame is not crash residue and fails with
    /// [`OndaError::Corruption`]. Returns the highest sequence number seen.
    /// Missing files replay as empty.
    pub fn replay<F>(path: impl AsRef<Path>, id: SegmentId, mut f: F) -> Result<u64>
    where
        F: FnMut(ReplayRecord) -> Result<()>,
    {
        let mut last_seq = 0u64;
        for k in 0..WAL_STRIPES {
            let seq = Self::replay_file(stripe_path(path.as_ref(), k), id, &mut f)?;
            last_seq = last_seq.max(seq);
        }
        Ok(last_seq)
    }

    fn replay_file<F>(path: std::path::PathBuf, id: SegmentId, f: &mut F) -> Result<u64>
    where
        F: FnMut(ReplayRecord) -> Result<()>,
    {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        let len = file.metadata()?.len();
        let mut r = BufReader::with_capacity(64 << 10, file);
        let mut head = vec![0u8; (len as usize).min(SEGMENT_HEADER_LEN)];
        r.read_exact(&mut head)?;
        let state = check_segment_head(&head, len, id).map_err(|e| match e {
            OndaError::Corruption(m) => OndaError::Corruption(format!("{}: {m}", path.display())),
            OndaError::UnsupportedFormat(m) => {
                OndaError::UnsupportedFormat(format!("{}: {m}", path.display()))
            }
            other => other,
        })?;
        if state == SegmentHead::Empty {
            return Ok(0);
        }
        replay_frames(&mut r, checksum, f)
    }
}

/// Replay every frame from `r` until a clean end or a torn tail, checking each
/// payload with `crc`. Returns the highest record sequence seen.
///
/// Shared with the 0.9 decoder (`legacy_onda::wal`), whose frames are the same
/// shape under a different checksum. The split between the two tail cases is
/// the contract of [`Wal::replay`]: a short header, a short payload or a CRC
/// mismatch ends the stripe cleanly; anything that fails to decode *inside* a
/// verified frame is `Corruption`.
pub(crate) fn replay_frames<R, F>(r: &mut R, crc: fn(&[u8]) -> u32, f: &mut F) -> Result<u64>
where
    R: Read,
    F: FnMut(ReplayRecord) -> Result<()>,
{
    let mut last_seq = 0u64;
    let mut header = [0u8; HEADER_SIZE];
    loop {
        if read_full(r, &mut header)?.is_none() {
            return Ok(last_seq); // clean EOF or partial header
        }
        let plen = read_u32(&header[0..4]) as usize;
        let want = read_u32(&header[4..8]);
        let mut payload = vec![0u8; plen];
        if read_full(r, &mut payload)?.is_none() {
            return Ok(last_seq); // torn payload at tail
        }
        if crc(&payload) != want {
            return Ok(last_seq); // corrupted tail
        }
        let seq = decode_frame_payload(&payload, f)?;
        last_seq = last_seq.max(seq);
    }
}

/// Decode one CRC-verified frame payload, handing each record to `f`, and
/// return the highest sequence it carried.
///
/// Past the CRC the bytes are known-intact: any decode failure is corruption,
/// not a torn tail, and must not be swallowed. The first payload byte selects
/// the form — an envelope frame (0xFF) or the flags-byte record stream — and
/// both may appear in one file: enabling the capability changes what is
/// written next, not what is already there.
pub(crate) fn decode_frame_payload<F>(payload: &[u8], f: &mut F) -> Result<u64>
where
    F: FnMut(ReplayRecord) -> Result<()>,
{
    if payload.first() == Some(&ENVELOPE_TAG) {
        return decode_envelope(payload, |rec| {
            // `last_seq` accounting covers range records too: a WAL whose
            // newest record is a range delete must still restore the sequence
            // it committed at. Control records (3.2) contribute nothing — see
            // `ReplayRecord::replay_seq`.
            let seq = rec.replay_seq();
            f(rec)?;
            Ok(seq)
        });
    }
    let mut last_seq = 0u64;
    let mut p = payload;
    while !p.is_empty() {
        let (rec, used) = decode_record(p)?;
        p = &p[used..];
        last_seq = last_seq.max(rec.seq);
        f(ReplayRecord::Point(rec))?;
    }
    Ok(last_seq)
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

/// Write out every stripe's buffered frames (no fsync). Poisons on failure.
fn flush_buffers(shared: &Shared) -> Result<()> {
    if !shared.buffered.swap(false, Ordering::Relaxed) {
        return Ok(());
    }
    let mut first_err = None;
    for file in &shared.files {
        let mut guard = file.lock();
        if let Some(st) = guard.as_mut() {
            if let Err(e) = shared.flush_stripe(st) {
                first_err.get_or_insert(e);
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn sync_dirty_files(shared: &Shared) {
    // Buffered frames first: an fsync covers only what the OS has been given.
    // A failure has already poisoned the database.
    let _ = flush_buffers(shared);
    if shared.sync != SyncMode::Interval || !shared.dirty.swap(false, Ordering::Relaxed) {
        return;
    }
    for file in &shared.files {
        let guard = file.lock();
        let Some(st) = guard.as_ref() else {
            continue;
        };
        if let Err(error) = st.file.sync_data() {
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
            let bytes = std::fs::read(crate::util::legacy_fixture(name)).unwrap();
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
                kind: crate::format::KIND_SINGLE_DELETE,
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

    /// One point record as a framed batch, exactly as `append_batch` writes it.
    fn point_frame(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
        let r = Record {
            key: key.to_vec(),
            value: value.to_vec(),
            seq,
            ..Default::default()
        };
        encode_frame(None, &point_envelope(&[r.as_ref()]))
    }

    /// Frame an arbitrary payload under a valid CRC.
    fn raw_frame(payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8; HEADER_SIZE];
        put_u32(&mut out[0..], payload.len() as u32);
        put_u32(&mut out[4..], checksum(payload));
        out.extend_from_slice(payload);
        out
    }

    /// Replay `bytes` — frames — as a whole stripe behind a valid segment
    /// header, from a private directory.
    fn replay_bytes(bytes: &[u8]) -> (tempfile::TempDir, Result<(Vec<Record>, u64)>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let mut file = encode_segment_header(SegmentId::per_cf(0)).to_vec();
        file.extend_from_slice(bytes);
        std::fs::write(&path, file).unwrap();
        let mut got = Vec::new();
        let res = Wal::replay(&path, SegmentId::per_cf(0), |r| {
            got.push(point(r));
            Ok(())
        })
        .map(|last| (got, last));
        (dir, res)
    }

    /// A torn payload is the expected crash residue, not corruption: the stripe
    /// ends cleanly and every record before the tear is delivered.
    #[test]
    fn torn_payload_stops_replay_cleanly() {
        // A header claiming 32 payload bytes followed by only 3.
        let mut bytes = point_frame(b"good", b"v", 1);
        bytes.extend_from_slice(&[32, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3]);
        let (_dir, res) = replay_bytes(&bytes);
        let (got, last) = res.expect("a torn tail must not fail replay");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].key, b"good");
        assert_eq!(last, 1);
    }

    /// A frame whose CRC checks out but whose record body does not decode is a
    /// real corruption: the bytes were written intact and still lie.
    #[test]
    fn crc_valid_undecodable_record_is_corruption() {
        // flags 0, klen 5, vlen 0, seq 7 — but only two key bytes follow.
        let mut bytes = point_frame(b"good", b"v", 1);
        bytes.extend_from_slice(&raw_frame(&[0x00, 0x05, 0x00, 0x07, b'a', b'b']));
        let (_dir, res) = replay_bytes(&bytes);
        let err = res.expect_err("a CRC-valid undecodable record must fail replay");
        assert_eq!(err.kind(), "corruption");
    }

    /// `Wal::append_batch(&[])` is public API and writes a zero-length frame;
    /// replay skips it and keeps reading.
    #[test]
    fn empty_frame_is_skipped_and_replay_continues() {
        let mut bytes = encode_frame(None, &[]);
        bytes.extend_from_slice(&point_frame(b"after", b"v", 9));
        let (_dir, res) = replay_bytes(&bytes);
        let (got, last) = res.expect("an empty frame must not fail replay");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].key, b"after");
        assert_eq!(last, 9);
    }

    /// `encode_record_body` builds its flags byte through
    /// `format::normalized_entry_flags`, and the kind is what decides the two
    /// tombstone bits — so `SINGLE_DELETE` without `TOMBSTONE`, the state the
    /// old `(tombstone, single_delete)` pair could express, is now
    /// unrepresentable rather than merely normalized away.
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
                kind: crate::format::KIND_SINGLE_DELETE,
            },
        );
        assert_eq!(buf[0], flags::TOMBSTONE | flags::SINGLE_DELETE);
    }

    /// The legacy record has no kind field, so it cannot carry 1.1's merge
    /// operand. Batches holding one are routed to an envelope frame by
    /// `ColumnFamily::apply_commit`; reaching the legacy encoder with one is an
    /// engine bug and is loud in debug builds.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "cannot carry kind")]
    fn wal_legacy_encode_debug_asserts_point_kind() {
        encode_record_body(
            &mut Vec::new(),
            RecordRef {
                key: b"k",
                value: b"operand",
                seq: 1,
                ttl: 0,
                kind: crate::format::KIND_MERGE,
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
                kind: crate::format::KIND_DELETE,
                ..Default::default()
            },
            Record {
                key: b"sdel".to_vec(),
                seq: 4,
                kind: crate::format::KIND_SINGLE_DELETE,
                ..Default::default()
            },
        ]
    }

    /// The point record inside a replayed record; panics on a range delete, so
    /// a test that accidentally produces one fails loudly instead of silently
    /// dropping it.
    fn point(rec: ReplayRecord) -> Record {
        match rec {
            ReplayRecord::Point(r) => r,
            ReplayRecord::RangeDelete { start, end, seq } => {
                panic!("unexpected range delete {start:?}..{end:?}@{seq}")
            }
            other => panic!("unexpected control record {other:?}"),
        }
    }

    /// Point records as envelope records.
    fn env(recs: &[Record]) -> Vec<EnvelopeRecord<'_>> {
        recs.iter()
            .map(|r| EnvelopeRecord::Point(r.as_ref()))
            .collect()
    }

    /// The point records plus 1.1's merge operand, which has no legacy spelling
    /// at all. Kept out of [`envelope_point_records`] because that one backs the
    /// golden fixtures, whose bytes are the wavesdb contract and never move.
    fn envelope_records_with_merge() -> Vec<Record> {
        let mut recs = envelope_point_records();
        recs.push(Record {
            key: b"merge".to_vec(),
            value: b"operand".to_vec(),
            seq: 5,
            kind: crate::format::KIND_MERGE,
            ..Default::default()
        });
        recs
    }

    /// Decode a hand-built envelope payload into its records.
    fn decode_envelope_payload(payload: &[u8]) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        decode_envelope(payload, |rec| {
            let r = point(rec);
            let seq = r.seq;
            out.push(r);
            Ok(seq)
        })?;
        Ok(out)
    }

    /// Decode a hand-built envelope payload into replay records of any kind.
    fn decode_envelope_any(payload: &[u8]) -> Result<Vec<ReplayRecord>> {
        let mut out = Vec::new();
        decode_envelope(payload, |rec| {
            let seq = rec.replay_seq();
            out.push(rec);
            Ok(seq)
        })?;
        Ok(out)
    }

    /// One envelope frame's payload, as `append_batch_enveloped` would write it.
    fn envelope_payload(schema: u64, recs: &[Record]) -> Vec<u8> {
        encode_frame(Some(schema), &env(recs))[HEADER_SIZE..].to_vec()
    }

    #[test]
    fn envelope_schema1_round_trips_all_point_kinds() {
        let want = envelope_records_with_merge();
        let payload = envelope_payload(ENVELOPE_SCHEMA_PER_CF, &want);
        assert_eq!(payload[0], ENVELOPE_TAG);
        let got = decode_envelope_payload(&payload).unwrap();
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(want.iter()) {
            assert_eq!(g.key, w.key);
            assert_eq!(g.value, w.value);
            assert_eq!(g.seq, w.seq);
            assert_eq!(g.ttl, w.ttl);
            assert_eq!(g.kind, w.kind);
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
            let wal =
                Wal::open(&path, SyncMode::None, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
            wal.append(rec("legacy", "a", 1)).unwrap();
            let recs = envelope_point_records();
            let refs: Vec<RecordRef<'_>> = recs.iter().map(|r| r.as_ref()).collect();
            wal.append_batch_enveloped(ENVELOPE_SCHEMA_PER_CF, &refs)
                .unwrap();
            wal.append(rec("legacy2", "b", 9)).unwrap();
        }
        let mut keys = Vec::new();
        let last = Wal::replay(&path, SegmentId::per_cf(0), |r| {
            keys.push(String::from_utf8(point(r).key).unwrap());
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

    /// A kind inside the assignable range that this binary does not implement:
    /// the bytes are intact and name a feature that may exist in a newer
    /// writer, so this binary is the one at fault. Kind 6 is the first still
    /// unassigned data kind — 4 (merge, 1.1) and 5 (range delete, 1.2) are both
    /// implemented now, so neither can stand in for "too old to read this".
    #[test]
    fn envelope_unknown_kind_is_unsupported_format() {
        const UNASSIGNED_DATA_KIND: u64 = 6;
        const { assert!(UNASSIGNED_DATA_KIND <= crate::format::MAX_ASSIGNABLE_KIND) };
        let p = envelope_with_body(&envelope_body(UNASSIGNED_DATA_KIND, 0));
        let err = decode_envelope_payload(&p).expect_err("kind 6 is not assigned yet");
        assert_eq!(err.kind(), "unsupported_format");
    }

    // ---- range deletes (kind 5, 1.2) ---------------------------------------

    /// The three range records the schema-1 fixture and the round-trip test
    /// share: a plain span, a one-byte-bound span, and a span whose bounds sort
    /// adjacent (`b`/`b\0`) — the tightest non-empty interval there is.
    fn range_records() -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        vec![
            (b"alpha".to_vec(), b"omega".to_vec(), 11),
            (b"a".to_vec(), b"b".to_vec(), 12),
            (b"b".to_vec(), b"b\0".to_vec(), 13),
        ]
    }

    fn range_envelope(recs: &[(Vec<u8>, Vec<u8>, u64)]) -> Vec<EnvelopeRecord<'_>> {
        recs.iter()
            .map(|(start, end, seq)| {
                EnvelopeRecord::Range(RangeRef {
                    start,
                    end,
                    seq: *seq,
                })
            })
            .collect()
    }

    /// Kind 5 uses the same `a`/`b` slots a put uses for key and value, so the
    /// exact frame-size precompute needs no special case — assert that as well
    /// as the round trip.
    #[test]
    fn range_record_round_trips() {
        let want = range_records();
        let recs = range_envelope(&want);
        for schema in [ENVELOPE_SCHEMA_PER_CF, ENVELOPE_SCHEMA_UNIFIED] {
            let predicted = frame_payload_len(Some(schema), &recs);
            let frame = encode_frame(Some(schema), &recs);
            assert_eq!(frame.len(), HEADER_SIZE + predicted, "size precompute");
            let got = decode_envelope_any(&frame[HEADER_SIZE..]).unwrap();
            assert_eq!(got.len(), want.len());
            for (g, (s, e, q)) in got.iter().zip(&want) {
                match g {
                    ReplayRecord::RangeDelete { start, end, seq } => {
                        assert_eq!(start, s);
                        assert_eq!(end, e);
                        assert_eq!(seq, q);
                    }
                    other => panic!("expected a range delete, got {other:?}"),
                }
            }
        }
    }

    /// One frame may mix point writes and range deletes: a commit that does
    /// both must replay atomically, which is only true if it is ONE frame.
    #[test]
    fn range_and_point_records_share_one_frame() {
        let points = envelope_point_records();
        let ranges = range_records();
        let mut recs = env(&points);
        recs.extend(range_envelope(&ranges));
        let frame = encode_frame(Some(ENVELOPE_SCHEMA_PER_CF), &recs);
        let got = decode_envelope_any(&frame[HEADER_SIZE..]).unwrap();
        assert_eq!(got.len(), points.len() + ranges.len());
        assert!(matches!(got[0], ReplayRecord::Point(_)));
        assert!(matches!(
            got[points.len()],
            ReplayRecord::RangeDelete { .. }
        ));
    }

    /// The committed schema-1 range fixture pins the **payload** wire bytes.
    /// The frame header differs from the 0.9 fixture by its checksum only
    /// (IEEE there, CRC32-C here); the envelope inside is unchanged.
    #[test]
    fn range_record_golden_bytes() {
        let bytes = std::fs::read(crate::util::legacy_fixture("wal_v2_range_schema1.bin")).unwrap();
        let recs = range_records();
        let frame = encode_frame(ENVELOPE_SCHEMA_PER_CF.into(), &range_envelope(&recs));
        assert_eq!(frame[HEADER_SIZE..], bytes[HEADER_SIZE..]);
        assert_eq!(frame[..4], bytes[..4], "payload length");
        assert_eq!(read_u32(&frame[4..8]), checksum(&frame[HEADER_SIZE..]));
        let got = decode_envelope_any(&bytes[HEADER_SIZE..]).unwrap();
        assert_eq!(got.len(), recs.len());
    }

    /// A range record carrying a modifier, or an empty bound, is bytes no
    /// writer produces — `Corruption`, not a newer format.
    #[test]
    fn range_record_rejects_modifiers_and_empty_bounds() {
        let p = envelope_with_body(&envelope_body(crate::format::KIND_RANGE_DELETE, 0x02));
        assert_eq!(
            decode_envelope_any(&p).unwrap_err().kind(),
            "corruption",
            "a range delete has no TTL"
        );
        // `envelope_body` writes alen = blen = 0.
        let p = envelope_with_body(&envelope_body(crate::format::KIND_RANGE_DELETE, 0));
        assert_eq!(decode_envelope_any(&p).unwrap_err().kind(), "corruption");
    }

    /// A torn frame containing a range record is dropped whole, exactly as a
    /// torn point frame is: the frame CRC covers the payload (invariant 3), so
    /// replay ends cleanly at the tear rather than surfacing half a commit.
    #[test]
    fn range_torn_frame() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal =
                Wal::open(&path, SyncMode::None, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
            wal.append(rec("kept", "v", 1)).unwrap();
            let ranges = range_records();
            wal.append_batch_envelope(ENVELOPE_SCHEMA_PER_CF, &range_envelope(&ranges))
                .unwrap();
        }
        // Truncate into the middle of the range frame's payload. Which stripe
        // file the two appends landed in is thread-dependent, so tear the one
        // that actually holds them.
        let stripe = (0..WAL_STRIPES)
            .map(|k| stripe_path(&path, k))
            .find(|p| std::fs::metadata(p).is_ok_and(|m| m.len() > SEGMENT_HEADER_LEN as u64))
            .expect("one stripe holds the appends");
        let full = std::fs::metadata(&stripe).unwrap().len();
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&stripe)
            .unwrap();
        f.set_len(full - 4).unwrap();
        drop(f);

        let mut points = Vec::new();
        let mut ranges = 0usize;
        let last = Wal::replay(&path, SegmentId::per_cf(0), |rec| {
            match rec {
                ReplayRecord::Point(r) => points.push(r.seq),
                ReplayRecord::RangeDelete { .. } => ranges += 1,
                other => panic!("unexpected control record {other:?}"),
            }
            Ok(())
        })
        .expect("a torn tail must not fail replay");
        assert_eq!(points, vec![1], "the intact frame still replays");
        assert_eq!(ranges, 0, "the torn frame is dropped WHOLE");
        assert_eq!(last, 1);
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
        let refs = env(&recs);
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
                        kind: crate::format::KIND_DELETE,
                        ..Default::default()
                    },
                ]
            }),
        ] {
            let bytes = std::fs::read(crate::util::legacy_fixture(name)).unwrap();
            // The payload is what is pinned; the frame CRC is IEEE in the 0.9
            // fixture and CRC32-C here.
            let frame = encode_frame(Some(schema), &env(&recs));
            assert_eq!(frame[HEADER_SIZE..], bytes[HEADER_SIZE..], "{name}");
            assert_eq!(frame[..4], bytes[..4], "{name}: payload length");
            // And the committed bytes decode back to the same records.
            let got = decode_envelope_payload(&bytes[HEADER_SIZE..]).unwrap();
            assert_eq!(got.len(), recs.len(), "{name}");
            for (g, w) in got.iter().zip(recs.iter()) {
                assert_eq!(g.key, w.key, "{name}");
                assert_eq!(g.value, w.value, "{name}");
                assert_eq!(g.seq, w.seq, "{name}");
                assert_eq!(g.ttl, w.ttl, "{name}");
                assert_eq!(g.kind, w.kind, "{name}");
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
                let _ = decode_envelope_any(&case);
            }
        }
    }

    // ---- transaction control (kinds 16-18, 3.2) ----------------------------

    /// The fixture id every control-frame test uses: 16 distinguishable bytes,
    /// so a mis-sliced id shows up as wrong content rather than wrong length.
    const TXN_ID: [u8; 16] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10,
    ];

    /// Wrap a hand-built record stream as a schema-2 envelope payload.
    fn control_payload(count: u64, body: &[u8]) -> Vec<u8> {
        let mut p = vec![ENVELOPE_TAG];
        append_uvarint(&mut p, ENVELOPE_SCHEMA_UNIFIED);
        append_uvarint(&mut p, count);
        p.extend_from_slice(body);
        p
    }

    /// The payload `append_prepare` writes for a two-CF, two-record prepare.
    fn prepare_fixture() -> (Vec<u64>, Vec<Record>) {
        let cf_ids = vec![0x0123_4567_89ab_cdefu64, 7];
        let mut put = 0x0123_4567_89ab_cdefu64.to_be_bytes().to_vec();
        put.extend_from_slice(b"k1");
        let mut del = 7u64.to_be_bytes().to_vec();
        del.extend_from_slice(b"k2");
        let recs = vec![
            Record {
                key: put,
                value: b"v1".to_vec(),
                // A real (non-zero) sequence here is deliberate: the encoder
                // must overwrite it with the sentinel.
                seq: 99,
                ..Default::default()
            },
            Record {
                key: del,
                seq: 99,
                kind: crate::format::KIND_DELETE,
                ..Default::default()
            },
        ];
        (cf_ids, recs)
    }

    /// One frame as `append_prepare`/`append_decision` submit it, without a WAL.
    fn prepare_frame(id: &[u8; 16], cf_ids: &[u64], recs: &[Record]) -> Vec<u8> {
        let mut ids = Vec::new();
        for cf in cf_ids {
            ids.extend_from_slice(&cf.to_le_bytes());
        }
        let mut frame = vec![EnvelopeRecord::Control(ControlRef {
            kind: crate::format::KIND_PREPARE,
            a: id,
            b: &ids,
        })];
        frame.extend(recs.iter().map(|r| {
            EnvelopeRecord::Point(RecordRef {
                seq: 0,
                ..r.as_ref()
            })
        }));
        encode_frame(Some(ENVELOPE_SCHEMA_UNIFIED), &frame)
    }

    fn decision_frame(id: &[u8; 16], commit: Option<(u64, u64)>) -> Vec<u8> {
        let mut payload = Vec::new();
        let kind = match commit {
            Some((seq, count)) => {
                payload.extend_from_slice(&seq.to_le_bytes());
                payload.extend_from_slice(&count.to_le_bytes());
                crate::format::KIND_COMMIT_DECISION
            }
            None => crate::format::KIND_ABORT_DECISION,
        };
        encode_frame(
            Some(ENVELOPE_SCHEMA_UNIFIED),
            &[EnvelopeRecord::Control(ControlRef {
                kind,
                a: id,
                b: &payload,
            })],
        )
    }

    /// The kind-16 frame's bytes, spelled out field by field exactly as the
    /// feature document's wire-format table does. Written as a literal rather
    /// than derived from the encoder, so a change to the field order or the
    /// sentinel is a test failure instead of a silent format break.
    #[test]
    fn prepare_frame_golden_bytes() {
        let (cf_ids, recs) = prepare_fixture();
        // Envelope header: tag, schema 2 (unified), count = 1 + 2 records.
        let mut want = vec![ENVELOPE_TAG, 2, 3];
        // record 0: the prepare header.
        want.push(16); // kind
        want.push(0); // modifiers
        want.push(16); // alen: the id
        want.push(16); // blen: two cf ids, 8 bytes each
        want.push(0); // seq sentinel
        want.extend_from_slice(&TXN_ID);
        want.extend_from_slice(&0x0123_4567_89ab_cdefu64.to_le_bytes());
        want.extend_from_slice(&7u64.to_le_bytes());
        // record 1: a put, key = cf id BE || user key.
        want.push(1); // kind
        want.push(0); // modifiers
        want.push(10); // alen: 8 + len("k1")
        want.push(2); // blen: len("v1")
        want.push(0); // seq sentinel
        want.extend_from_slice(&0x0123_4567_89ab_cdefu64.to_be_bytes());
        want.extend_from_slice(b"k1");
        want.extend_from_slice(b"v1");
        // record 2: a delete.
        want.push(2); // kind
        want.push(0); // modifiers
        want.push(10); // alen
        want.push(0); // blen
        want.push(0); // seq sentinel
        want.extend_from_slice(&7u64.to_be_bytes());
        want.extend_from_slice(b"k2");

        let frame = prepare_frame(&TXN_ID, &cf_ids, &recs);
        assert_eq!(&frame[HEADER_SIZE..], &want[..]);
        assert_eq!(read_u32(&frame[0..4]) as usize, want.len());
        assert_eq!(read_u32(&frame[4..8]), checksum(&want));
    }

    #[test]
    fn commit_decision_golden_bytes() {
        let mut want = vec![ENVELOPE_TAG, 2, 1];
        want.push(17); // kind
        want.push(0); // modifiers
        want.push(16); // alen: the id
        want.push(16); // blen: commit_seq || count
        want.push(0); // seq sentinel
        want.extend_from_slice(&TXN_ID);
        want.extend_from_slice(&41u64.to_le_bytes());
        want.extend_from_slice(&3u64.to_le_bytes());

        let frame = decision_frame(&TXN_ID, Some((41, 3)));
        assert_eq!(&frame[HEADER_SIZE..], &want[..]);
    }

    #[test]
    fn abort_decision_golden_bytes() {
        let mut want = vec![ENVELOPE_TAG, 2, 1];
        want.push(18); // kind
        want.push(0); // modifiers
        want.push(16); // alen: the id
        want.push(0); // blen: an abort carries no payload
        want.push(0); // seq sentinel
        want.extend_from_slice(&TXN_ID);

        let frame = decision_frame(&TXN_ID, None);
        assert_eq!(&frame[HEADER_SIZE..], &want[..]);
    }

    /// The `seq == 0` sentinel is what keeps a prepared writeset out of the
    /// replay watermark, so it is asserted on the decoded records and on the
    /// frame's contribution to `last_seq` — the two ways it can be lost.
    #[test]
    fn control_frame_seq_is_zero() {
        let (cf_ids, recs) = prepare_fixture();
        for frame in [
            prepare_frame(&TXN_ID, &cf_ids, &recs),
            decision_frame(&TXN_ID, Some((41, 3))),
            decision_frame(&TXN_ID, None),
        ] {
            let mut last = 0u64;
            decode_envelope(&frame[HEADER_SIZE..], |rec| {
                assert_eq!(rec.replay_seq(), 0, "a control frame raises no watermark");
                if let ReplayRecord::Prepare { records, .. } = &rec {
                    for r in records {
                        assert_eq!(r.seq, 0, "prepared records carry the sentinel");
                    }
                }
                last = last.max(rec.replay_seq());
                Ok(rec.replay_seq())
            })
            .unwrap();
            assert_eq!(last, 0);
        }
    }

    #[test]
    fn control_frame_roundtrip() {
        let cf_ids = vec![11u64, 22, 33];
        let recs: Vec<Record> = (0..5u64)
            .map(|i| {
                let mut key = cf_ids[(i % 3) as usize].to_be_bytes().to_vec();
                key.extend_from_slice(format!("key-{i}").as_bytes());
                Record {
                    key,
                    value: format!("value-{i}").into_bytes(),
                    seq: 0,
                    ttl: if i == 2 { 1_700_000_000_000_000_000 } else { 0 },
                    kind: if i == 3 {
                        crate::format::KIND_DELETE
                    } else {
                        crate::format::KIND_PUT
                    },
                }
            })
            .collect();
        let frame = prepare_frame(&TXN_ID, &cf_ids, &recs);
        let got = decode_envelope_any(&frame[HEADER_SIZE..]).unwrap();
        assert_eq!(got.len(), 1, "a control frame decodes as ONE record");
        match &got[0] {
            ReplayRecord::Prepare {
                id,
                cf_ids: got_ids,
                records,
            } => {
                assert_eq!(id, &TXN_ID);
                assert_eq!(got_ids, &cf_ids);
                assert_eq!(records.len(), recs.len());
                for (g, w) in records.iter().zip(&recs) {
                    assert_eq!(g.key, w.key);
                    assert_eq!(g.value, w.value);
                    assert_eq!(g.ttl, w.ttl);
                    assert_eq!(g.kind, w.kind);
                    assert_eq!(g.seq, 0);
                }
            }
            other => panic!("expected a prepare, got {other:?}"),
        }
        for commit in [Some((41u64, 3u64)), None] {
            let frame = decision_frame(&TXN_ID, commit);
            let got = decode_envelope_any(&frame[HEADER_SIZE..]).unwrap();
            assert!(matches!(
                &got[0],
                ReplayRecord::Decision { id, commit: c } if id == &TXN_ID && *c == commit
            ));
        }
    }

    /// A record body with an explicit sequence, for the shape-rule tests.
    fn control_body(kind: u64, mods: u64, a: &[u8], b: &[u8], seq: u64) -> Vec<u8> {
        let mut out = Vec::new();
        append_uvarint(&mut out, kind);
        append_uvarint(&mut out, mods);
        append_uvarint(&mut out, a.len() as u64);
        append_uvarint(&mut out, b.len() as u64);
        append_uvarint(&mut out, seq);
        out.extend_from_slice(a);
        out.extend_from_slice(b);
        out
    }

    /// A prepared record carrying a real sequence would raise the replay
    /// watermark for a transaction that may still abort — the exact bug the
    /// sentinel exists to prevent, so the decoder refuses the bytes.
    #[test]
    fn prepare_frame_with_nonzero_seq_is_corruption() {
        let mut body = control_body(crate::format::KIND_PREPARE, 0, &TXN_ID, &[], 0);
        body.extend_from_slice(&control_body(crate::format::KIND_PUT, 0, b"k", b"v", 7));
        let err = decode_envelope_any(&control_payload(2, &body))
            .expect_err("a prepared record may not carry a sequence");
        assert_eq!(err.kind(), "corruption");

        // The header record itself, too.
        let head = control_body(crate::format::KIND_PREPARE, 0, &TXN_ID, &[], 4);
        assert_eq!(
            decode_envelope_any(&control_payload(1, &head))
                .unwrap_err()
                .kind(),
            "corruption"
        );
    }

    /// A frame led by kind 16 holds exactly one kind-16 record followed by
    /// point writes. A range delete, a second prepare, or a decision spliced in
    /// is `Corruption`; so is a control kind appearing inside a data frame.
    #[test]
    fn prepare_frame_with_foreign_kind_is_corruption() {
        let head = control_body(crate::format::KIND_PREPARE, 0, &TXN_ID, &[], 0);
        for foreign in [
            crate::format::KIND_RANGE_DELETE,
            crate::format::KIND_PREPARE,
            crate::format::KIND_ABORT_DECISION,
        ] {
            let mut body = head.clone();
            body.extend_from_slice(&control_body(foreign, 0, b"aaaa", b"bbbb", 0));
            let err = decode_envelope_any(&control_payload(2, &body))
                .expect_err("only point writes may follow a prepare header");
            assert_eq!(err.kind(), "corruption", "kind {foreign} inside a prepare");
        }
        // And the mirror: a control kind in the middle of a data frame.
        let mut body = control_body(crate::format::KIND_PUT, 0, b"k", b"v", 1);
        body.extend_from_slice(&control_body(
            crate::format::KIND_COMMIT_DECISION,
            0,
            &TXN_ID,
            &[0u8; 16],
            0,
        ));
        assert_eq!(
            decode_envelope_any(&control_payload(2, &body))
                .unwrap_err()
                .kind(),
            "corruption"
        );
    }

    /// A decision is one record and nothing else, and its payload width is
    /// fixed by the kind.
    #[test]
    fn decision_frame_shape_is_enforced() {
        // Two records under a decision head.
        let mut body = control_body(crate::format::KIND_ABORT_DECISION, 0, &TXN_ID, &[], 0);
        body.extend_from_slice(&control_body(crate::format::KIND_PUT, 0, b"k", b"v", 0));
        assert_eq!(
            decode_envelope_any(&control_payload(2, &body))
                .unwrap_err()
                .kind(),
            "corruption"
        );
        // A commit decision whose payload is not exactly 16 bytes.
        let body = control_body(
            crate::format::KIND_COMMIT_DECISION,
            0,
            &TXN_ID,
            &[0u8; 8],
            0,
        );
        assert_eq!(
            decode_envelope_any(&control_payload(1, &body))
                .unwrap_err()
                .kind(),
            "corruption"
        );
        // An abort decision that carries one.
        let body = control_body(
            crate::format::KIND_ABORT_DECISION,
            0,
            &TXN_ID,
            &[0u8; 16],
            0,
        );
        assert_eq!(
            decode_envelope_any(&control_payload(1, &body))
                .unwrap_err()
                .kind(),
            "corruption"
        );
        // An id that is not 16 bytes.
        let body = control_body(crate::format::KIND_ABORT_DECISION, 0, b"short", &[], 0);
        assert_eq!(
            decode_envelope_any(&control_payload(1, &body))
                .unwrap_err()
                .kind(),
            "corruption"
        );
    }

    /// Control frames go through `Wal::append_prepare`/`append_decision` and
    /// come back out of `Wal::replay` unchanged, interleaved with ordinary
    /// data frames — which is how a real unified WAL holds them.
    #[test]
    fn control_frames_replay_beside_data_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let (cf_ids, recs) = prepare_fixture();
        {
            let wal =
                Wal::open(&path, SyncMode::Full, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
            wal.append(rec("committed", "v", 12)).unwrap();
            let refs: Vec<RecordRef<'_>> = recs.iter().map(|r| r.as_ref()).collect();
            wal.append_prepare(ENVELOPE_SCHEMA_UNIFIED, &TXN_ID, &cf_ids, &refs)
                .unwrap();
            wal.append_decision(ENVELOPE_SCHEMA_UNIFIED, &TXN_ID, Some((13, 2)))
                .unwrap();
            wal.sync().unwrap();
        }
        let mut prepares = 0;
        let mut decisions = Vec::new();
        let mut points = 0;
        let last = Wal::replay(&path, SegmentId::per_cf(0), |r| {
            match r {
                ReplayRecord::Point(_) => points += 1,
                ReplayRecord::Prepare {
                    id,
                    cf_ids: c,
                    records,
                } => {
                    assert_eq!(id, TXN_ID);
                    assert_eq!(c, cf_ids);
                    assert_eq!(records.len(), 2);
                    prepares += 1;
                }
                ReplayRecord::Decision { id, commit } => {
                    assert_eq!(id, TXN_ID);
                    decisions.push(commit);
                }
                ReplayRecord::RangeDelete { .. } => panic!("no range delete was written"),
            }
            Ok(())
        })
        .unwrap();
        assert_eq!((points, prepares), (1, 1));
        assert_eq!(decisions, vec![Some((13, 2))]);
        // The committed point record sets the watermark; the prepare's records
        // and the decision contribute nothing.
        assert_eq!(last, 12, "a control frame must not raise the watermark");
    }

    /// The control decoder must be total over arbitrary bytes, like every other
    /// decoder in this crate.
    #[test]
    fn fuzz_decode_control_frame_never_panics() {
        let (cf_ids, recs) = prepare_fixture();
        let seeds = [
            prepare_frame(&TXN_ID, &cf_ids, &recs)[HEADER_SIZE..].to_vec(),
            decision_frame(&TXN_ID, Some((41, 3)))[HEADER_SIZE..].to_vec(),
            decision_frame(&TXN_ID, None)[HEADER_SIZE..].to_vec(),
        ];
        let mut rng = crate::util::FuzzRng::new(0x5DEE_CE66_D1CE_4B9F);
        for seed in &seeds {
            for _ in 0..2000 {
                let mut case = crate::util::fuzz_mutate(&mut rng, seed);
                if case.is_empty() {
                    case.push(ENVELOPE_TAG);
                }
                case[0] = ENVELOPE_TAG;
                let _ = decode_envelope_any(&case);
            }
        }
    }

    #[test]
    fn new_wal_creation_propagates_parent_sync_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let calls = std::sync::atomic::AtomicUsize::new(0);

        let err = Wal::open_inner(
            &path,
            SyncMode::Full,
            Duration::ZERO,
            SegmentId::per_cf(0),
            0,
            |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(std::io::Error::other("injected parent sync failure").into())
            },
        )
        .expect_err("a new WAL must not open when its directory sync fails");

        assert!(matches!(err, OndaError::Io(_)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn existing_wal_does_not_require_creation_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        drop(Wal::open(&path, SyncMode::Full, Duration::ZERO, SegmentId::per_cf(0)).unwrap());
        let calls = std::sync::atomic::AtomicUsize::new(0);

        drop(
            Wal::open_inner(
                &path,
                SyncMode::Full,
                Duration::ZERO,
                SegmentId::per_cf(0),
                0,
                |_| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .unwrap(),
        );

        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_append_replay_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = std::sync::Arc::new(
            Wal::open(&path, SyncMode::None, Duration::ZERO, SegmentId::per_cf(0)).unwrap(),
        );
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
                            kind: crate::format::KIND_PUT,
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
        let last = Wal::replay(&path, SegmentId::per_cf(0), |rec| {
            let r = point(rec);
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
            let wal =
                Wal::open(&path, SyncMode::None, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
            wal.append(rec("a", "1", 1)).unwrap();
            let (b, c) = (rec("b", "2", 2), rec("c", "3", 3));
            wal.append_batch(&[b.as_ref(), c.as_ref()]).unwrap();
        }
        let mut got = Vec::new();
        let last = Wal::replay(&path, SegmentId::per_cf(0), |rec| {
            let r = point(rec);
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
            let wal =
                Wal::open(&path, SyncMode::Full, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
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
                kind: crate::format::KIND_SINGLE_DELETE,
                ..Default::default()
            })
            .unwrap();
        }
        let mut recs = Vec::new();
        Wal::replay(&path, SegmentId::per_cf(0), |rec| {
            let r = point(rec);
            recs.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(recs[0].ttl, 1234567890);
        assert!(recs[1].tombstone() && recs[1].single_delete());
    }

    #[test]
    fn torn_tail_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let wal =
                Wal::open(&path, SyncMode::None, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
            wal.append(rec("good", "v", 1)).unwrap();
        }
        // Append garbage (a partial frame) to simulate a crash mid-write.
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[9, 0, 0, 0, 1, 2, 3]).unwrap(); // claims 9 bytes, gives 3
        }
        let mut n = 0;
        let last = Wal::replay(&path, SegmentId::per_cf(0), |_| {
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
            let wal =
                Wal::open(&path, SyncMode::None, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
            wal.append(rec("a", "1", 1)).unwrap();
            wal.append(rec("b", "2", 2)).unwrap();
        }
        // Corrupt the last byte of the stripe file that holds the records (the
        // test thread's sticky stripe is process-global, so locate it by size).
        {
            use std::io::{Seek, SeekFrom, Write};
            let data_file = (0..WAL_STRIPES)
                .map(|k| stripe_path(&path, k))
                .find(|p| {
                    std::fs::metadata(p)
                        .map(|m| m.len() > SEGMENT_HEADER_LEN as u64)
                        .unwrap_or(false)
                })
                .expect("one stripe holds the records");
            let mut f = OpenOptions::new().write(true).open(&data_file).unwrap();
            let len = f.metadata().unwrap().len();
            f.seek(SeekFrom::Start(len - 1)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let mut keys = Vec::new();
        Wal::replay(&path, SegmentId::per_cf(0), |rec| {
            let r = point(rec);
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
        let wal = StdArc::new(
            Wal::open(&path, SyncMode::Full, Duration::ZERO, SegmentId::per_cf(0)).unwrap(),
        );
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
        Wal::replay(&path, SegmentId::per_cf(0), |_| {
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
            let wal = Wal::open(
                &path,
                SyncMode::Interval,
                Duration::from_millis(10),
                SegmentId::per_cf(0),
            )
            .unwrap();
            wal.append(rec("a", "1", 1)).unwrap();
            std::thread::sleep(Duration::from_millis(30));
        }
        let mut count = 0;
        Wal::replay(&path, SegmentId::per_cf(0), |_| {
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
        let wal = Wal::open(
            &path,
            SyncMode::Interval,
            Duration::from_secs(60),
            SegmentId::per_cf(0),
        )
        .unwrap();
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

    // ---- segment header (epoch 1) --------------------------------------------

    /// The header, byte for byte.
    #[test]
    fn segment_header_golden_bytes() {
        let h = encode_segment_header(SegmentId::unified(0x0102_0304_0506_0708));
        assert_eq!(&h[..8], b"YOLODBWL");
        assert_eq!(read_u32(&h[8..]), 1, "version");
        assert_eq!(h[12], 2, "unified layout");
        assert_eq!(&h[13..16], &[0, 0, 0]);
        assert_eq!(&h[16..24], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&h[24..28], &[0, 0, 0, 0]);
        assert_eq!(read_u32(&h[28..]), checksum(&h[..28]));
        assert_eq!(encode_segment_header(SegmentId::per_cf(3))[12], 1);
    }

    /// A new WAL writes the header, and it is on disk before any frame: the
    /// file of a WAL opened and closed with no append is exactly the header.
    #[test]
    fn open_writes_the_header_before_any_frame() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-7.log");
        drop(Wal::open(&path, SyncMode::Full, Duration::ZERO, SegmentId::per_cf(7)).unwrap());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            encode_segment_header(SegmentId::per_cf(7)).to_vec()
        );
        // Reopening an existing segment appends behind its header.
        let wal = Wal::open(&path, SyncMode::Full, Duration::ZERO, SegmentId::per_cf(7)).unwrap();
        wal.append(rec("k", "v", 1)).unwrap();
        drop(wal);
        let mut n = 0;
        Wal::replay(&path, SegmentId::per_cf(7), |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 1);
        // The wrong segment id is refused, on replay and on reopen.
        for id in [SegmentId::per_cf(8), SegmentId::unified(7)] {
            assert_eq!(
                Wal::replay(&path, id, |_| Ok(())).unwrap_err().kind(),
                "corruption"
            );
            assert_eq!(
                Wal::open(&path, SyncMode::Full, Duration::ZERO, id)
                    .unwrap_err()
                    .kind(),
                "corruption"
            );
        }
    }

    fn replay_file_bytes(bytes: &[u8]) -> Result<usize> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        std::fs::write(&path, bytes).unwrap();
        let mut n = 0;
        Wal::replay(&path, SegmentId::per_cf(0), |_| {
            n += 1;
            Ok(())
        })?;
        Ok(n)
    }

    /// Every shape a crash can leave at the head of a segment replays as empty:
    /// a created-but-unwritten file, a partial header, a header-sized file whose
    /// bytes never landed, a full header whose CRC fails with nothing behind it.
    #[test]
    fn torn_headers_are_empty_segments() {
        let h = encode_segment_header(SegmentId::per_cf(0));
        assert_eq!(replay_file_bytes(&[]).unwrap(), 0);
        for n in [1usize, 7, 8, 20, 31] {
            assert_eq!(replay_file_bytes(&h[..n]).unwrap(), 0, "prefix {n}");
        }
        assert_eq!(replay_file_bytes(&[0u8; 32]).unwrap(), 0);
        assert_eq!(replay_file_bytes(&[0u8; 5]).unwrap(), 0);
        let mut torn = h;
        torn[20] ^= 0x01;
        assert_eq!(replay_file_bytes(&torn).unwrap(), 0);
        // And a reopen repairs a torn header in place.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        std::fs::write(&path, &h[..11]).unwrap();
        let wal = Wal::open(&path, SyncMode::Full, Duration::ZERO, SegmentId::per_cf(0)).unwrap();
        wal.append(rec("after", "v", 3)).unwrap();
        drop(wal);
        let mut got = Vec::new();
        Wal::replay(&path, SegmentId::per_cf(0), |r| {
            got.push(point(r).key);
            Ok(())
        })
        .unwrap();
        assert_eq!(got, vec![b"after".to_vec()]);
    }

    /// A segment without a yoloDB header — a 0.9 WAL is frames from byte 0 —
    /// is refused at byte 0, never read as an empty stripe or mid-frame.
    #[test]
    fn a_missing_or_foreign_header_is_unsupported_format() {
        let frames = point_frame(b"k", b"v", 1);
        for bytes in [
            frames.clone(),       // a 0.9 WAL
            frames[..6].to_vec(), // a short 0.9 WAL
            b"WAVESST1-and-more-bytes-after-it".to_vec(),
        ] {
            assert_eq!(
                replay_file_bytes(&bytes).unwrap_err().kind(),
                "unsupported_format",
                "{bytes:?}"
            );
        }
        // An all-zero header with frames behind it cannot be torn: the header
        // is fsynced before the first frame.
        let mut zeros = vec![0u8; 32];
        zeros.extend_from_slice(&frames);
        assert_eq!(
            replay_file_bytes(&zeros).unwrap_err().kind(),
            "unsupported_format"
        );
    }

    #[test]
    fn segment_header_corruption_rows() {
        let h = encode_segment_header(SegmentId::per_cf(0));
        let frames = point_frame(b"k", b"v", 1);
        let with = |head: &[u8]| {
            let mut b = head.to_vec();
            b.extend_from_slice(&frames);
            b
        };
        let reseal = |mut b: [u8; 32]| {
            let crc = checksum(&b[..28]);
            b[28..].copy_from_slice(&crc.to_le_bytes());
            b
        };
        assert_eq!(replay_file_bytes(&with(&h)).unwrap(), 1);
        // A CRC failure with frames behind it is corruption, not a torn tail.
        let mut bad = h;
        bad[17] ^= 0x01;
        assert_eq!(
            replay_file_bytes(&with(&bad)).unwrap_err().kind(),
            "corruption"
        );
        // Unknown version or layout byte: a newer format.
        let mut v = h;
        v[8] = 2;
        assert_eq!(
            replay_file_bytes(&with(&reseal(v))).unwrap_err().kind(),
            "unsupported_format"
        );
        let mut l = h;
        l[12] = 9;
        assert_eq!(
            replay_file_bytes(&with(&reseal(l))).unwrap_err().kind(),
            "unsupported_format"
        );
        // Reserved bytes set.
        for at in [13usize, 15, 24, 27] {
            let mut r = h;
            r[at] = 1;
            assert_eq!(
                replay_file_bytes(&with(&reseal(r))).unwrap_err().kind(),
                "corruption",
                "reserved byte {at}"
            );
        }
    }

    /// Every stripe carries the header; a stripe file that was never written
    /// to still replays as empty.
    #[test]
    fn every_stripe_gets_a_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-2.log");
        drop(Wal::open(&path, SyncMode::None, Duration::ZERO, SegmentId::per_cf(2)).unwrap());
        for k in 0..WAL_STRIPES {
            assert_eq!(
                std::fs::read(stripe_path(&path, k)).unwrap(),
                encode_segment_header(SegmentId::per_cf(2)).to_vec(),
                "stripe {k}"
            );
        }
    }

    // ---- user-space write buffer (P6) ----------------------------------

    /// The stripe the calling thread writes (sticky per thread).
    fn my_stripe_path(base: &Path) -> std::path::PathBuf {
        stripe_path(base, my_stripe(WAL_STRIPES))
    }

    fn len_of(p: &Path) -> u64 {
        std::fs::metadata(p).unwrap().len()
    }

    /// A batch of `n` records whose keys encode the batch number, so replay
    /// output can be checked for whole-batch prefixes.
    fn batch_recs(b: usize, n: usize) -> Vec<Record> {
        (0..n)
            .map(|i| {
                rec(
                    &format!("b{b:04}-{i}"),
                    &"v".repeat(1 + (b * 7 + i) % 40),
                    (b * 10 + i + 1) as u64,
                )
            })
            .collect()
    }

    fn replay_keys(path: &Path, id: SegmentId) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        Wal::replay(path, id, |r| {
            out.push(point(r).key);
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn buffered_wal_coalesces_small_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::open_buffered(
            &path,
            SyncMode::None,
            Duration::from_secs(3600),
            SegmentId::per_cf(0),
            64 << 10,
        )
        .unwrap();
        let stripe = my_stripe_path(&path);
        for i in 0..1000u64 {
            wal.append(rec(&format!("k{i:05}"), "v", i + 1)).unwrap();
        }
        let frames_bytes = wal.size() as u64 - SEGMENT_HEADER_LEN as u64 * WAL_STRIPES as u64;
        // ~20 KB of frames: nothing reached the file, and no write was issued.
        assert!(frames_bytes < 64 << 10);
        assert_eq!(wal.write_calls(), 0, "buffered frames were written early");
        assert_eq!(len_of(&stripe), SEGMENT_HEADER_LEN as u64);
        wal.flush_buffer().unwrap();
        assert_eq!(wal.write_calls(), 1, "one flush, one write");
        assert_eq!(len_of(&stripe), SEGMENT_HEADER_LEN as u64 + frames_bytes);
        // More than a buffer's worth: writes happen as it fills, far fewer
        // than one per frame.
        for i in 1000..11_000u64 {
            wal.append(rec(&format!("k{i:05}"), "v", i + 1)).unwrap();
        }
        let calls = wal.write_calls();
        assert!(calls > 1 && calls < 20, "{calls} writes for 10k frames");
        wal.close().unwrap();
        let keys = replay_keys(&path, SegmentId::per_cf(0));
        assert_eq!(keys.len(), 11_000);
        assert_eq!(keys.last().unwrap(), b"k10999");
    }

    #[test]
    fn buffered_wal_sync_and_close_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::open_buffered(
            &path,
            SyncMode::Interval,
            Duration::from_secs(3600),
            SegmentId::per_cf(0),
            1 << 20,
        )
        .unwrap();
        let stripe = my_stripe_path(&path);
        wal.append(rec("a", "1", 1)).unwrap();
        assert_eq!(len_of(&stripe), SEGMENT_HEADER_LEN as u64);
        // An fsync covers only what the OS has: sync must flush first.
        wal.sync().unwrap();
        assert!(len_of(&stripe) > SEGMENT_HEADER_LEN as u64);
        // A 2PC prepare frame is synced on the same handle by its caller.
        let p = rec("p", "x", 9);
        wal.append_prepare(ENVELOPE_SCHEMA_UNIFIED, &[7; 16], &[1], &[p.as_ref()])
            .unwrap();
        let before = len_of(&stripe);
        wal.sync().unwrap();
        assert!(
            len_of(&stripe) > before,
            "prepare frame was not flushed by sync"
        );
        wal.append(rec("b", "2", 2)).unwrap();
        wal.close().unwrap();
        let mut n = 0;
        Wal::replay(&path, SegmentId::per_cf(0), |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 3, "a, the prepare (one control record), b");
    }

    #[test]
    fn buffered_wal_background_flush_under_sync_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::open_buffered(
            &path,
            SyncMode::None,
            Duration::from_millis(5),
            SegmentId::per_cf(0),
            1 << 20,
        )
        .unwrap();
        let stripe = my_stripe_path(&path);
        wal.append(rec("a", "1", 1)).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while len_of(&stripe) == SEGMENT_HEADER_LEN as u64 {
            assert!(
                std::time::Instant::now() < deadline,
                "the interval thread never flushed a cold buffer"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(wal.write_calls(), 1);
    }

    #[test]
    fn full_mode_ignores_the_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::open_buffered(
            &path,
            SyncMode::Full,
            Duration::ZERO,
            SegmentId::per_cf(0),
            1 << 20,
        )
        .unwrap();
        wal.append(rec("a", "1", 1)).unwrap();
        // Acknowledged under Full means written and fsynced.
        assert!(len_of(&path) > SEGMENT_HEADER_LEN as u64);
        assert_eq!(wal.write_calls(), 1);
    }

    #[test]
    fn oversized_frame_bypasses_the_buffer_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Wal::open_buffered(
            &path,
            SyncMode::None,
            Duration::from_secs(3600),
            SegmentId::per_cf(0),
            256,
        )
        .unwrap();
        wal.append(rec("a", "small", 1)).unwrap();
        wal.append(rec("b", &"x".repeat(1000), 2)).unwrap();
        wal.append(rec("c", "small", 3)).unwrap();
        wal.close().unwrap();
        let keys = replay_keys(&path, SegmentId::per_cf(0));
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    /// Crash matrix for a buffered flush: the flush is one `write` of many
    /// frames, and a crash can tear it at any byte. For every cut point the
    /// replay must be exactly the whole batches that fit — never part of a
    /// batch, never a batch after a torn one.
    #[test]
    fn torn_buffered_flush_replays_a_prefix_of_whole_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let id = SegmentId::unified(3);
        let wal = Wal::open_buffered(
            &path,
            SyncMode::None,
            Duration::from_secs(3600),
            id,
            1 << 20,
        )
        .unwrap();
        let stripe = my_stripe_path(&path);
        let mut batches = Vec::new();
        let mut ends = Vec::new(); // file offset at which each frame ends
        let mut off = SEGMENT_HEADER_LEN as u64;
        for b in 0..24 {
            let recs = batch_recs(b, 1 + b % 4);
            let refs: Vec<RecordRef<'_>> = recs.iter().map(|r| r.as_ref()).collect();
            // Alternate the two frame forms: both must tear the same way.
            let before = wal.size();
            if b % 2 == 0 {
                wal.append_batch(&refs).unwrap();
            } else {
                wal.append_batch_enveloped(ENVELOPE_SCHEMA_UNIFIED, &refs)
                    .unwrap();
            }
            off += (wal.size() - before) as u64;
            ends.push(off);
            batches.push(recs);
        }
        assert_eq!(
            wal.write_calls(),
            0,
            "the whole run must be one buffered flush"
        );
        wal.close().unwrap();
        assert_eq!(wal.write_calls(), 1);
        let bytes = std::fs::read(&stripe).unwrap();
        assert_eq!(bytes.len() as u64, off);

        let crash = tempfile::tempdir().unwrap();
        let torn = crash.path().join("wal");
        for cut in SEGMENT_HEADER_LEN..=bytes.len() {
            std::fs::write(&torn, &bytes[..cut]).unwrap();
            let got = replay_keys(&torn, id);
            let whole = ends.iter().take_while(|&&e| e <= cut as u64).count();
            let want: Vec<Vec<u8>> = batches[..whole]
                .iter()
                .flat_map(|b| b.iter().map(|r| r.key.clone()))
                .collect();
            assert_eq!(got, want, "cut at byte {cut}");
        }
    }
}
