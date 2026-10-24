//! wavesdb's WAL, layouts 3 (one log per column family) and 4 (unified), and
//! its durable prepared transactions.
//!
//! A segment is the epoch-1 container — the 32-byte `YOLODBWL` header, checked
//! by the same rules as ondaDB's ([`crate::wal::check_head`]) — around
//! wavesdb 0.8 frames, each one committed batch:
//!
//! ```text
//! frame    payload_len u32 | crc32c(payload) u32 | payload
//! legacy   (flags u8 | klen uv | vlen uv | seq uv | ttl varint? | key | value)*
//! extended 0xff | schema uv = 1 | count uv | env_flags uv = 0 |
//!          (kind uv | mods uv | cf_id uv | klen uv | key | vlen uv | value | seq uv | ttl varint?)*
//! ```
//!
//! The two payload forms coexist in one segment (the envelope starts when
//! `CAP_EXTENDED_RECORDS` is enabled). A frame is decoded whole before any of
//! it is applied, so a commit replays all or nothing; a torn or
//! checksum-failed frame ends the segment cleanly, and a verified frame that
//! does not decode is `Corruption` — wavesdb's `wal.ReplayCounted` split.
//!
//! **Unified keys** carry the family's id as an 8-byte big-endian prefix, as
//! ondaDB's do, so a replayed record goes into the unified memtable as it
//! stands. A range delete carries the prefix on both bounds. An extended
//! record's `cf_id` must agree with its key's prefix.
//!
//! **Prepared transactions** (kinds 16–19) live only in the unified log. The
//! record's key is the caller's identifier (1–128 bytes, no NUL), and a
//! prepare's value is wavesdb's own batch body — family *names*, per-op
//! ordinals and a body CRC — not ondaDB's 16-byte-id frames. A segment is one
//! file with no stripes, so a decision always follows its prepare in log
//! order. Matching happens in [`Prepares::finish`], once every generation is
//! read: a committed batch is installed at `commit_seq - span + 1 + ordinal`,
//! an aborted one is dropped, and an **undecided prepare stays invisible**.
//! The read-only open does not register undecided prepares anywhere: there is
//! no writer to conflict with them, and no ondaDB 16-byte id can name them.

use std::io::{BufReader, Read};
use std::path::Path;

use crate::encoding::{checksum, read_u32, uvarint, varint};
use crate::error::{OndaError, Result};
use crate::format::wal_segment::{LAYOUT_WAVES08_PER_CF, LAYOUT_WAVES08_UNIFIED};
use crate::format::wavesdb_reserved::KIND_SPILL_DESCRIPTOR;
use crate::format::{
    CAP_EXTENDED_RECORDS, CAP_MERGE_OPERANDS, CAP_RANGE_DELETES, CAP_TXN_DECISIONS,
    KIND_ABORT_DECISION, KIND_COMMIT_DECISION, KIND_DELETE, KIND_MERGE, KIND_PREPARE, KIND_PUT,
    KIND_RANGE_DELETE, KIND_SINGLE_DELETE, MAX_ASSIGNABLE_KIND,
};
use crate::wal::{check_head, Record, ReplayRecord, SegmentHead, SEGMENT_HEADER_LEN};

/// Which of wavesdb's two layouts a segment belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Layout {
    /// Layout 3: `cf_<name>/wal-<gen>.log`, bare user keys.
    PerCf,
    /// Layout 4: `unified-wal-<gen>.log`, family-prefixed keys.
    Unified,
}

impl Layout {
    fn byte(self) -> u8 {
        match self {
            Layout::PerCf => LAYOUT_WAVES08_PER_CF,
            Layout::Unified => LAYOUT_WAVES08_UNIFIED,
        }
    }
}

const FRAME_HEADER: usize = 8;
const EXTENDED_MARKER: u8 = 0xff;
/// wavesdb `format.WALEnvelopeSchema`.
const ENVELOPE_SCHEMA: u64 = 1;
/// The legacy flags byte (wavesdb `format.ValidWALFlags`): tombstone, TTL and
/// single delete, the last only with the first.
const FLAG_TOMBSTONE: u8 = 0x01;
const FLAG_HAS_TTL: u8 = 0x02;
const FLAG_SINGLE_DELETE: u8 = 0x10;
const LEGACY_FLAG_MASK: u8 = FLAG_TOMBSTONE | FLAG_HAS_TTL | FLAG_SINGLE_DELETE;
/// The only modifier a WAL record carries (wavesdb `format.WALModifierMask`).
const MOD_HAS_TTL: u64 = 0x02;
/// The width of a unified key's family prefix.
const CF_PREFIX: usize = 8;

/// wavesdb `MaxPreparedTxnIDLen`.
const MAX_PREPARED_ID: usize = 128;
/// wavesdb `preparePayloadVersion` and `prepareFlagUnified`.
const PREPARE_BODY_VERSION: u64 = 1;
const PREPARE_FLAG_UNIFIED: u64 = 1;
/// wavesdb `Serializable`, the highest isolation level.
const MAX_ISOLATION: u64 = 4;

fn corrupt(msg: impl std::fmt::Display) -> OndaError {
    OndaError::Corruption(format!("wavesdb wal: {msg}"))
}

/// One decoded record, before it is routed.
struct Rec {
    kind: u64,
    /// The extended envelope's family id; 0 from a legacy frame.
    cf_id: u64,
    extended: bool,
    key: Vec<u8>,
    value: Vec<u8>,
    seq: u64,
    ttl: i64,
}

/// Replay one wavesdb WAL segment, handing every data record to `f` as a
/// [`ReplayRecord`] and folding control records into `txns`.
///
/// `caps` is the catalog's capability word: a frame needing a bit it lacks
/// (the envelope, a merge operand, a range delete, a control record) is
/// `Corruption`, since nothing writes one before its bit is durable. `txns`
/// is `None` for the per-family layout, which never carries a control record.
/// Returns the highest sequence replayed, a commit decision's included. A
/// missing file replays as empty.
pub(crate) fn replay<F>(
    path: &Path,
    layout: Layout,
    generation: u64,
    caps: u64,
    mut txns: Option<&mut Prepares>,
    f: &mut F,
) -> Result<u64>
where
    F: FnMut(ReplayRecord) -> Result<()>,
{
    let at = |e: OndaError| super::prefix(e, &path.display().to_string());
    if let Some(t) = txns.as_deref_mut() {
        t.generation = generation;
    }
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let len = file.metadata()?.len();
    let mut r = BufReader::with_capacity(64 << 10, file);
    let mut head = vec![0u8; (len as usize).min(SEGMENT_HEADER_LEN)];
    r.read_exact(&mut head)?;
    let implemented = [LAYOUT_WAVES08_PER_CF, LAYOUT_WAVES08_UNIFIED];
    if check_head(&head, len, layout.byte(), generation, &implemented).map_err(at)?
        == SegmentHead::Empty
    {
        return Ok(0);
    }
    let mut remaining = len - SEGMENT_HEADER_LEN as u64;
    let mut offset = SEGMENT_HEADER_LEN as u64;
    let mut last_seq = 0u64;
    let mut header = [0u8; FRAME_HEADER];
    loop {
        if remaining < FRAME_HEADER as u64 || !read_full(&mut r, &mut header)? {
            return Ok(last_seq); // clean end, or a torn frame header
        }
        remaining -= FRAME_HEADER as u64;
        let plen = u64::from(read_u32(&header));
        if plen > remaining {
            // A torn payload — and a corrupt length must not size an
            // allocation the file cannot back.
            return Ok(last_seq);
        }
        remaining -= plen;
        let mut payload = vec![0u8; plen as usize];
        if !read_full(&mut r, &mut payload)? {
            return Ok(last_seq);
        }
        if checksum(&payload) != read_u32(&header[4..]) {
            return Ok(last_seq); // a torn tail
        }
        let frame_at = |e: OndaError| at(super::prefix(e, &format!("frame at offset {offset}")));
        let recs = decode_frame(&payload).map_err(frame_at)?;
        admit(&recs, caps).map_err(frame_at)?;
        for rec in recs {
            last_seq = last_seq.max(rec.seq);
            route(rec, layout, txns.as_deref_mut(), f).map_err(frame_at)?;
        }
        offset += FRAME_HEADER as u64 + plen;
    }
}

/// Read exactly `buf.len()` bytes; `false` on a clean or partial EOF.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => return Ok(false),
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(true)
}

/// A cursor over a verified payload: every short read is corruption.
struct Cur<'a>(&'a [u8]);

impl<'a> Cur<'a> {
    fn uvar(&mut self, what: &str) -> Result<u64> {
        let (v, n) = uvarint(self.0).ok_or_else(|| corrupt(format!("{what} is truncated")))?;
        self.0 = &self.0[n..];
        Ok(v)
    }

    fn var(&mut self, what: &str) -> Result<i64> {
        let (v, n) = varint(self.0).ok_or_else(|| corrupt(format!("{what} is truncated")))?;
        self.0 = &self.0[n..];
        Ok(v)
    }

    fn take(&mut self, n: u64, what: &str) -> Result<&'a [u8]> {
        if n > self.0.len() as u64 {
            return Err(corrupt(format!("{what} overruns the frame")));
        }
        let (v, rest) = self.0.split_at(n as usize);
        self.0 = rest;
        Ok(v)
    }

    fn bytes(&mut self, what: &str) -> Result<&'a [u8]> {
        let n = self.uvar(what)?;
        self.take(n, what)
    }
}

/// Decode one verified frame payload into its records, or fail the frame.
fn decode_frame(payload: &[u8]) -> Result<Vec<Rec>> {
    if payload.first() == Some(&EXTENDED_MARKER) {
        return decode_extended(&payload[1..]);
    }
    let mut c = Cur(payload);
    let mut out = Vec::new();
    while !c.0.is_empty() {
        out.push(decode_legacy(&mut c)?);
    }
    Ok(out)
}

fn decode_legacy(c: &mut Cur<'_>) -> Result<Rec> {
    let flags = c.take(1, "flags")?[0];
    if flags & !LEGACY_FLAG_MASK != 0
        || (flags & FLAG_SINGLE_DELETE != 0 && flags & FLAG_TOMBSTONE == 0)
    {
        return Err(corrupt(format!("legacy record flags {flags:#04x}")));
    }
    let klen = c.uvar("key length")?;
    let vlen = c.uvar("value length")?;
    let seq = c.uvar("sequence")?;
    let ttl = if flags & FLAG_HAS_TTL != 0 {
        c.var("ttl")?
    } else {
        0
    };
    let key = c.take(klen, "key")?.to_vec();
    let value = c.take(vlen, "value")?.to_vec();
    let kind = match (flags & FLAG_TOMBSTONE != 0, flags & FLAG_SINGLE_DELETE != 0) {
        (true, true) => KIND_SINGLE_DELETE,
        (true, false) => KIND_DELETE,
        _ => KIND_PUT,
    };
    Ok(Rec {
        kind,
        cf_id: 0,
        extended: false,
        key,
        value,
        seq,
        ttl,
    })
}

/// Whether `kind` is one of the transaction control kinds wavesdb's WAL
/// carries: prepare, the two decisions and the spill descriptor.
fn is_control(kind: u64) -> bool {
    matches!(
        kind,
        KIND_PREPARE | KIND_COMMIT_DECISION | KIND_ABORT_DECISION | KIND_SPILL_DESCRIPTOR
    )
}

fn decode_extended(p: &[u8]) -> Result<Vec<Rec>> {
    let mut c = Cur(p);
    let schema = c.uvar("envelope schema")?;
    if schema != ENVELOPE_SCHEMA {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb wal: envelope schema {schema} is not implemented by this binary"
        )));
    }
    let count = c.uvar("record count")?;
    // Every record is at least seven bytes, so a count beyond the payload
    // cannot be honest — and it must not size an allocation.
    if count == 0 || count > c.0.len() as u64 {
        return Err(corrupt(format!("record count {count}")));
    }
    if c.uvar("envelope flags")? != 0 {
        return Err(corrupt("envelope flags are set"));
    }
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let kind = c.uvar("kind")?;
        if kind == 0 || kind > MAX_ASSIGNABLE_KIND {
            return Err(corrupt(format!("record kind {kind}")));
        }
        if !(KIND_PUT..=KIND_RANGE_DELETE).contains(&kind) && !is_control(kind) {
            return Err(OndaError::UnsupportedFormat(format!(
                "wavesdb wal: record kind {kind} is not implemented by this binary"
            )));
        }
        // One decision per frame: the frame is what recovery accepts whole,
        // and it is routed by kind (wavesdb `ErrControlFrameShape`).
        if is_control(kind) && count != 1 {
            return Err(corrupt("a control record shares its frame"));
        }
        let mods = c.uvar("modifiers")?;
        if mods & !MOD_HAS_TTL != 0
            || (mods != 0 && kind == KIND_RANGE_DELETE)
            || (mods != 0 && kind == KIND_MERGE)
        {
            return Err(corrupt(format!("modifiers {mods:#x} on kind {kind}")));
        }
        let cf_id = c.uvar("family id")?;
        let key = c.bytes("key")?.to_vec();
        let value = c.bytes("value")?.to_vec();
        let seq = c.uvar("sequence")?;
        let ttl = if mods & MOD_HAS_TTL != 0 {
            c.var("ttl")?
        } else {
            0
        };
        if kind == KIND_RANGE_DELETE && (key.is_empty() || value.is_empty()) {
            return Err(corrupt("a range delete with an empty bound"));
        }
        out.push(Rec {
            kind,
            cf_id,
            extended: true,
            key,
            value,
            seq,
            ttl,
        });
    }
    if !c.0.is_empty() {
        return Err(corrupt("bytes after the declared records"));
    }
    Ok(out)
}

/// wavesdb's admission rule, read side (`frameRequires`): a decoded frame
/// needs the envelope's bit, and each record its kind's.
fn admit(recs: &[Rec], caps: u64) -> Result<()> {
    let mut need = 0;
    for r in recs {
        if r.extended {
            need |= CAP_EXTENDED_RECORDS;
        }
        need |= match r.kind {
            KIND_MERGE => CAP_MERGE_OPERANDS,
            KIND_RANGE_DELETE => CAP_RANGE_DELETES,
            k if is_control(k) => CAP_TXN_DECISIONS,
            _ => 0,
        };
    }
    let missing = need & !caps;
    if missing != 0 {
        return Err(corrupt(format!(
            "frame needs capabilities {missing:#x} the catalog does not declare"
        )));
    }
    Ok(())
}

/// The family id a unified key's prefix names.
fn key_cf(key: &[u8]) -> Option<u64> {
    key.get(..CF_PREFIX)
        .map(|p| u64::from_be_bytes(p.try_into().expect("8 bytes")))
}

fn route<F>(rec: Rec, layout: Layout, txns: Option<&mut Prepares>, f: &mut F) -> Result<()>
where
    F: FnMut(ReplayRecord) -> Result<()>,
{
    if is_control(rec.kind) {
        return match txns {
            Some(t) => t.fold(rec),
            None => Err(corrupt(format!(
                "transaction control kind {} in a per-family log",
                rec.kind
            ))),
        };
    }
    if layout == Layout::Unified {
        let prefix =
            key_cf(&rec.key).ok_or_else(|| corrupt("a unified key without its family prefix"))?;
        if rec.extended && prefix != rec.cf_id {
            return Err(corrupt(format!(
                "record family id {:#x} disagrees with its key prefix {prefix:#x}",
                rec.cf_id
            )));
        }
        if rec.kind == KIND_RANGE_DELETE && key_cf(&rec.value) != Some(prefix) {
            return Err(corrupt("a range delete's bounds name different families"));
        }
    }
    if rec.kind == KIND_RANGE_DELETE {
        return f(ReplayRecord::RangeDelete {
            start: rec.key,
            end: rec.value,
            seq: rec.seq,
        });
    }
    f(ReplayRecord::Point(Record {
        key: rec.key,
        value: rec.value,
        seq: rec.seq,
        ttl: rec.ttl,
        kind: rec.kind,
    }))
}

/// One operation of a prepared batch: the family by index into the batch's
/// name table, and the ordinal that spreads a key written twice over the
/// batch's sequence span.
#[derive(Debug, Clone)]
struct Op {
    cf: usize,
    kind: u64,
    ordinal: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    ttl: i64,
}

/// A decoded prepare body (wavesdb `preparedBatch`).
#[derive(Debug, Clone)]
struct Batch {
    span: u64,
    cfs: Vec<Vec<u8>>,
    ops: Vec<Op>,
}

#[derive(Debug, Clone)]
enum Body {
    Batch(Batch),
    /// A large transaction's spill descriptor (kind 19): its records are in
    /// private chunk files this binary does not read.
    Spill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    /// Committed, its *last* sequence being the payload.
    Commit(u64),
    Abort,
}

#[derive(Debug, Clone)]
struct Txn {
    id: Vec<u8>,
    body: Body,
    decision: Option<Decision>,
    /// The generation the newest decision was read from: a commit below the
    /// replay floor installs nothing (`finish`).
    decision_gen: u64,
}

/// The prepared transactions a unified log names, folded in log order and
/// resolved by [`finish`](Self::finish) once every generation is read.
#[derive(Debug, Default)]
pub(crate) struct Prepares {
    txns: Vec<Txn>,
    /// The generation [`replay`] is reading, stamped on each decision.
    generation: u64,
}

impl Prepares {
    fn find(&mut self, id: &[u8]) -> Option<&mut Txn> {
        self.txns.iter_mut().find(|t| t.id == id)
    }

    /// Fold one control record (wavesdb `replayPreparedControl`).
    fn fold(&mut self, rec: Rec) -> Result<()> {
        let generation = self.generation;
        let id = rec.key;
        if id.is_empty() || id.len() > MAX_PREPARED_ID || id.contains(&0) {
            return Err(corrupt(
                "a control record with an invalid prepared identifier",
            ));
        }
        let show = String::from_utf8_lossy(&id).into_owned();
        match rec.kind {
            KIND_PREPARE | KIND_SPILL_DESCRIPTOR => {
                if self.find(&id).is_some() {
                    return Err(corrupt(format!(
                        "prepared transaction {show:?} is prepared twice"
                    )));
                }
                let body =
                    if rec.kind == KIND_PREPARE {
                        Body::Batch(decode_batch(&rec.value).map_err(|e| {
                            super::prefix(e, &format!("prepared transaction {show:?}"))
                        })?)
                    } else {
                        Body::Spill
                    };
                self.txns.push(Txn {
                    id,
                    body,
                    decision: None,
                    decision_gen: 0,
                });
            }
            _ => {
                let want = if rec.kind == KIND_COMMIT_DECISION {
                    Decision::Commit(rec.seq)
                } else {
                    Decision::Abort
                };
                let t = self.find(&id).ok_or_else(|| {
                    corrupt(format!(
                        "a decision names {show:?}, which this log never prepared"
                    ))
                })?;
                let conflicting = matches!(
                    (t.decision, want),
                    (Some(Decision::Abort), Decision::Commit(_))
                        | (Some(Decision::Commit(_)), Decision::Abort)
                );
                if conflicting {
                    return Err(corrupt(format!(
                        "prepared transaction {show:?} is both committed and aborted"
                    )));
                }
                // A retried commit after a crash records the newest decision.
                t.decision = Some(want);
                t.decision_gen = generation;
            }
        }
        Ok(())
    }

    /// How many prepares have no durable decision: invisible to every read.
    #[cfg(test)]
    pub(crate) fn undecided(&self) -> usize {
        self.txns.iter().filter(|t| t.decision.is_none()).count()
    }

    /// Resolve every prepare (wavesdb `finishPreparedRecovery`): the records
    /// of each committed batch, keyed with the family ids `family_id`
    /// resolves from the batch's names, and the highest sequence a decision
    /// published. Aborted and undecided transactions contribute nothing.
    ///
    /// A commit whose decision lies below `wal_floor` installs nothing: its
    /// records are in a table, or were before compaction changed them. Its
    /// sequences still count toward the returned watermark.
    pub(crate) fn finish(
        self,
        family_id: impl Fn(&str) -> Option<u64>,
        wal_floor: u64,
    ) -> Result<(Vec<ReplayRecord>, u64)> {
        let mut out = Vec::new();
        let mut max_seq = 0;
        for t in self.txns {
            let Some(Decision::Commit(commit_seq)) = t.decision else {
                continue;
            };
            let show = String::from_utf8_lossy(&t.id);
            let b = match t.body {
                Body::Batch(b) => b,
                Body::Spill => {
                    return Err(OndaError::UnsupportedFormat(format!(
                        "wavesdb wal: committed transaction {show:?} was spilled to private \
                         chunk files, which this binary does not read"
                    )))
                }
            };
            let first = (commit_seq + 1)
                .checked_sub(b.span)
                .filter(|&s| s > 0)
                .ok_or_else(|| {
                    corrupt(format!(
                        "transaction {show:?} committed at {commit_seq} with a span of {}",
                        b.span
                    ))
                })?;
            max_seq = max_seq.max(commit_seq);
            if t.decision_gen < wal_floor {
                continue;
            }
            let mut ids = Vec::with_capacity(b.cfs.len());
            for name in &b.cfs {
                let id = std::str::from_utf8(name)
                    .ok()
                    .and_then(&family_id)
                    .ok_or_else(|| {
                        OndaError::InvalidDb(format!(
                            "column family {:?}, which committed prepared transaction {show:?} \
                         writes, does not exist",
                            String::from_utf8_lossy(name)
                        ))
                    })?;
                ids.push(id.to_be_bytes());
            }
            for op in b.ops {
                let prefixed = |k: &[u8]| [&ids[op.cf][..], k].concat();
                let seq = first + op.ordinal;
                out.push(if op.kind == KIND_RANGE_DELETE {
                    // Both bounds prefixed, as the unified log carries them.
                    ReplayRecord::RangeDelete {
                        start: prefixed(&op.key),
                        end: prefixed(&op.value),
                        seq,
                    }
                } else {
                    ReplayRecord::Point(Record {
                        key: prefixed(&op.key),
                        value: op.value,
                        seq,
                        ttl: op.ttl,
                        kind: op.kind,
                    })
                });
            }
        }
        Ok((out, max_seq))
    }
}

/// Decode a prepare body (wavesdb `decodePreparedBatch`):
///
/// ```text
/// version uv = 1 | flags uv | isolation uv | read_seq uv | span uv |
///   ncf uv | (len uv | name)* | nops uv |
///   (cf_idx uv | kind uv | ordinal uv | klen uv | key | vlen uv | value | ttl varint)* |
///   crc32c u32 over everything before it
/// ```
fn decode_batch(p: &[u8]) -> Result<Batch> {
    if p.len() < 4 {
        return Err(corrupt("prepare body shorter than its checksum"));
    }
    let (body, sum) = p.split_at(p.len() - 4);
    if checksum(body) != read_u32(sum) {
        return Err(corrupt("prepare body checksum mismatch"));
    }
    let mut c = Cur(body);
    let version = c.uvar("prepare body version")?;
    if version != PREPARE_BODY_VERSION {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb wal: prepare body version {version} is not implemented by this binary"
        )));
    }
    if c.uvar("prepare flags")? & !PREPARE_FLAG_UNIFIED != 0 {
        return Err(corrupt("unknown prepare body flags"));
    }
    if c.uvar("isolation")? > MAX_ISOLATION {
        return Err(corrupt("isolation level outside the defined range"));
    }
    c.uvar("read sequence")?;
    let span = c.uvar("span")?;
    if span == 0 {
        return Err(corrupt("a prepared batch needs at least one sequence"));
    }
    let ncf = c.uvar("family count")?;
    if ncf == 0 || ncf > c.0.len() as u64 {
        return Err(corrupt(format!("family count {ncf}")));
    }
    let cfs = (0..ncf)
        .map(|_| c.bytes("family name").map(<[u8]>::to_vec))
        .collect::<Result<Vec<_>>>()?;
    let nops = c.uvar("operation count")?;
    if nops == 0 || nops > c.0.len() as u64 {
        return Err(corrupt(format!("operation count {nops}")));
    }
    let mut ops = Vec::with_capacity(nops as usize);
    for _ in 0..nops {
        let cf = c.uvar("family index")?;
        if cf >= ncf {
            return Err(corrupt(
                "an operation names a family the body does not list",
            ));
        }
        // A prepared batch holds data records only.
        let kind = c.uvar("operation kind")?;
        if !(KIND_PUT..=KIND_RANGE_DELETE).contains(&kind) {
            return Err(corrupt(format!(
                "operation kind {kind} is not a data record"
            )));
        }
        let ordinal = c.uvar("ordinal")?;
        if ordinal >= span {
            return Err(corrupt(
                "an operation's ordinal is outside the batch's span",
            ));
        }
        let key = c.bytes("key")?;
        if key.is_empty() {
            return Err(corrupt("an operation with an empty key"));
        }
        let value = c.bytes("value")?;
        let ttl = c.var("ttl")?;
        if kind == KIND_MERGE && ttl != 0 {
            return Err(corrupt("a merge operand cannot carry a TTL"));
        }
        if kind == KIND_RANGE_DELETE && (value.is_empty() || ttl != 0) {
            return Err(corrupt("a range delete needs an end bound and no TTL"));
        }
        ops.push(Op {
            cf: cf as usize,
            kind,
            ordinal,
            key: key.to_vec(),
            value: value.to_vec(),
            ttl,
        });
    }
    if !c.0.is_empty() {
        return Err(corrupt("bytes after the declared operations"));
    }
    Ok(Batch { span, cfs, ops })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{append_u32, append_uvarint, append_varint};
    use crate::format::wal_segment::{HEADER_LEN, MAGIC, VERSION};

    const ALL: u64 =
        CAP_EXTENDED_RECORDS | CAP_MERGE_OPERANDS | CAP_RANGE_DELETES | CAP_TXN_DECISIONS;

    fn header(layout: u8, generation: u64) -> Vec<u8> {
        let mut b = vec![0u8; HEADER_LEN];
        b[..8].copy_from_slice(&MAGIC);
        b[8..12].copy_from_slice(&VERSION.to_le_bytes());
        b[12] = layout;
        b[16..24].copy_from_slice(&generation.to_le_bytes());
        let crc = checksum(&b[..28]);
        b[28..].copy_from_slice(&crc.to_le_bytes());
        b
    }

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        append_u32(&mut out, payload.len() as u32);
        append_u32(&mut out, checksum(payload));
        out.extend_from_slice(payload);
        out
    }

    /// One extended record as wavesdb's `appendExtendedRecordBody` writes it.
    fn ext_rec(
        out: &mut Vec<u8>,
        kind: u64,
        cf: u64,
        key: &[u8],
        value: &[u8],
        seq: u64,
        ttl: i64,
    ) {
        append_uvarint(out, kind);
        append_uvarint(out, if ttl != 0 { MOD_HAS_TTL } else { 0 });
        append_uvarint(out, cf);
        append_uvarint(out, key.len() as u64);
        out.extend_from_slice(key);
        append_uvarint(out, value.len() as u64);
        out.extend_from_slice(value);
        append_uvarint(out, seq);
        if ttl != 0 {
            append_varint(out, ttl);
        }
    }

    fn envelope(count: u64, body: &[u8]) -> Vec<u8> {
        let mut p = vec![EXTENDED_MARKER];
        append_uvarint(&mut p, ENVELOPE_SCHEMA);
        append_uvarint(&mut p, count);
        append_uvarint(&mut p, 0);
        p.extend_from_slice(body);
        p
    }

    fn pk(cf: u64, k: &[u8]) -> Vec<u8> {
        [&cf.to_be_bytes()[..], k].concat()
    }

    /// wavesdb `encodePreparedBatch`, ops as (cf_idx, kind, ordinal, key, value).
    type TestOp<'a> = (u64, u64, u64, &'a [u8], &'a [u8]);

    fn batch(span: u64, cfs: &[&str], ops: &[TestOp<'_>]) -> Vec<u8> {
        let mut b = Vec::new();
        for v in [
            PREPARE_BODY_VERSION,
            PREPARE_FLAG_UNIFIED,
            2,
            10,
            span,
            cfs.len() as u64,
        ] {
            append_uvarint(&mut b, v);
        }
        for n in cfs {
            append_uvarint(&mut b, n.len() as u64);
            b.extend_from_slice(n.as_bytes());
        }
        append_uvarint(&mut b, ops.len() as u64);
        for (cf, kind, ord, k, v) in ops {
            for x in [*cf, *kind, *ord, k.len() as u64] {
                append_uvarint(&mut b, x);
            }
            b.extend_from_slice(k);
            append_uvarint(&mut b, v.len() as u64);
            b.extend_from_slice(v);
            append_varint(&mut b, 0);
        }
        let sum = checksum(&b);
        append_u32(&mut b, sum);
        b
    }

    fn control(kind: u64, id: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
        let mut body = Vec::new();
        ext_rec(&mut body, kind, 0, id, value, seq, 0);
        frame(&envelope(1, &body))
    }

    fn write(dir: &Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, bytes).unwrap();
        p
    }

    fn collect(
        path: &Path,
        layout: Layout,
        generation: u64,
        caps: u64,
        txns: Option<&mut Prepares>,
    ) -> Result<(Vec<ReplayRecord>, u64)> {
        let mut out = Vec::new();
        let last = replay(path, layout, generation, caps, txns, &mut |r| {
            out.push(r);
            Ok(())
        })?;
        Ok((out, last))
    }

    #[test]
    fn legacy_and_extended_frames_replay_with_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = header(LAYOUT_WAVES08_PER_CF, 7);
        // A legacy frame of two records: a put with a TTL and a single delete.
        let mut legacy = vec![FLAG_HAS_TTL];
        for v in [1u64, 2, 5] {
            append_uvarint(&mut legacy, v);
        }
        append_varint(&mut legacy, 99);
        legacy.extend_from_slice(b"kvv");
        legacy.push(FLAG_TOMBSTONE | FLAG_SINGLE_DELETE);
        for v in [1u64, 0, 6] {
            append_uvarint(&mut legacy, v);
        }
        legacy.push(b'k');
        seg.extend(frame(&legacy));
        let mut ext = Vec::new();
        ext_rec(&mut ext, KIND_MERGE, 0, b"m", b"+1", 7, 0);
        ext_rec(&mut ext, KIND_RANGE_DELETE, 0, b"a", b"c", 8, 0);
        seg.extend(frame(&envelope(2, &ext)));
        let whole = seg.len();
        seg.extend(frame(&envelope(2, &ext)));
        seg.truncate(seg.len() - 3); // torn while written
        let p = write(dir.path(), "wal-7.log", &seg);
        let (recs, last) = collect(&p, Layout::PerCf, 7, ALL, None).unwrap();
        assert_eq!(last, 8);
        assert_eq!(recs.len(), 4);
        match &recs[0] {
            ReplayRecord::Point(r) => {
                assert_eq!(
                    (r.key.as_slice(), r.value.as_slice(), r.seq, r.ttl, r.kind),
                    (&b"k"[..], &b"vv"[..], 5, 99, KIND_PUT)
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(
            matches!(&recs[1], ReplayRecord::Point(r) if r.kind == KIND_SINGLE_DELETE && r.seq == 6)
        );
        assert!(
            matches!(&recs[2], ReplayRecord::Point(r) if r.kind == KIND_MERGE && r.value == b"+1")
        );
        assert!(
            matches!(&recs[3], ReplayRecord::RangeDelete { start, end, seq: 8 } if start == b"a" && end == b"c")
        );

        // A verified frame naming a kind this binary lacks is refused, not
        // treated as a torn tail.
        let mut bad = seg[..whole].to_vec();
        let ext_at = HEADER_LEN + FRAME_HEADER + legacy.len() + FRAME_HEADER;
        bad[ext_at + 4] = 9; // the first record's kind: an unassigned data kind
        let crc = checksum(&bad[ext_at..]);
        bad[ext_at - 4..ext_at].copy_from_slice(&crc.to_le_bytes());
        let p = write(dir.path(), "wal-7.log", &bad);
        assert!(matches!(
            collect(&p, Layout::PerCf, 7, ALL, None),
            Err(OndaError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn the_container_and_the_catalog_are_checked() {
        let dir = tempfile::tempdir().unwrap();
        let mut ext = Vec::new();
        ext_rec(&mut ext, KIND_MERGE, 0, b"m", b"+1", 7, 0);
        let body = frame(&envelope(1, &ext));
        let seg = [header(LAYOUT_WAVES08_PER_CF, 2), body.clone()].concat();
        let p = write(dir.path(), "wal-2.log", &seg);
        // The generation and the layout must be the file's.
        assert!(matches!(
            collect(&p, Layout::PerCf, 3, ALL, None),
            Err(OndaError::Corruption(_))
        ));
        assert!(matches!(
            collect(&p, Layout::Unified, 2, ALL, None),
            Err(OndaError::Corruption(_))
        ));
        // ondaDB's own layouts are not this decoder's.
        let ours = write(
            dir.path(),
            "wal-x.log",
            &[header(1, 2), body.clone()].concat(),
        );
        assert!(matches!(
            collect(&ours, Layout::PerCf, 2, ALL, None),
            Err(OndaError::UnsupportedFormat(_))
        ));
        // A merge operand needs its bit, and the envelope its own.
        assert!(matches!(
            collect(&p, Layout::PerCf, 2, CAP_EXTENDED_RECORDS, None),
            Err(OndaError::Corruption(_))
        ));
        assert!(matches!(
            collect(&p, Layout::PerCf, 2, CAP_MERGE_OPERANDS, None),
            Err(OndaError::Corruption(_))
        ));
        // A header-only segment, a torn header and a missing file are empty.
        let p = write(dir.path(), "wal-2.log", &header(LAYOUT_WAVES08_PER_CF, 2));
        assert_eq!(collect(&p, Layout::PerCf, 2, 0, None).unwrap().1, 0);
        let p = write(dir.path(), "wal-2.log", &MAGIC[..5]);
        assert_eq!(collect(&p, Layout::PerCf, 2, 0, None).unwrap().1, 0);
        assert_eq!(
            collect(&dir.path().join("none.log"), Layout::PerCf, 2, 0, None)
                .unwrap()
                .1,
            0
        );
        // A control record in a per-family log.
        let p = write(
            dir.path(),
            "wal-2.log",
            &[
                header(LAYOUT_WAVES08_PER_CF, 2),
                control(KIND_ABORT_DECISION, b"t", b"", 0),
            ]
            .concat(),
        );
        assert!(matches!(
            collect(&p, Layout::PerCf, 2, ALL, None),
            Err(OndaError::Corruption(_))
        ));
    }

    #[test]
    fn unified_keys_must_carry_their_family() {
        let dir = tempfile::tempdir().unwrap();
        let run = |rec: &[u8], count: u64| {
            let p = write(
                dir.path(),
                "unified-wal-0.log",
                &[
                    header(LAYOUT_WAVES08_UNIFIED, 0),
                    frame(&envelope(count, rec)),
                ]
                .concat(),
            );
            collect(&p, Layout::Unified, 0, ALL, Some(&mut Prepares::default()))
        };
        let mut ok = Vec::new();
        ext_rec(&mut ok, KIND_PUT, 5, &pk(5, b"k"), b"v", 1, 0);
        ext_rec(
            &mut ok,
            KIND_RANGE_DELETE,
            5,
            &pk(5, b"a"),
            &pk(5, b"b"),
            2,
            0,
        );
        let (recs, last) = run(&ok, 2).unwrap();
        assert_eq!(last, 2);
        assert!(matches!(&recs[0], ReplayRecord::Point(r) if r.key == pk(5, b"k")));
        assert!(
            matches!(&recs[1], ReplayRecord::RangeDelete { start, end, .. } if *start == pk(5, b"a") && *end == pk(5, b"b"))
        );
        for (cf, key, end) in [
            (6, pk(5, b"k"), None),
            (5, b"short".to_vec(), None),
            (5, pk(5, b"a"), Some(pk(6, b"b"))),
        ] {
            let mut bad = Vec::new();
            match end {
                None => ext_rec(&mut bad, KIND_PUT, cf, &key, b"v", 1, 0),
                Some(end) => ext_rec(&mut bad, KIND_RANGE_DELETE, cf, &key, &end, 1, 0),
            }
            assert!(
                matches!(run(&bad, 1), Err(OndaError::Corruption(_))),
                "{cf} {key:?}"
            );
        }
    }

    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/wavesdb-epoch1")
            .join(name)
            .join("db")
    }

    fn catalog_caps(dir: &Path) -> u64 {
        crate::waves08::recover_catalog(dir).unwrap().caps
    }

    /// The per-family fixtures' third round, as wavesdb's generator wrote it:
    /// every record of every family's WAL tail, its kind, TTL and the round
    /// and index the value carries.
    #[test]
    fn the_per_family_fixture_tails_replay() {
        let dir = fixture("percf-caps");
        let caps = catalog_caps(&dir);
        let mut seqs = Vec::new();
        for cf in ["plain", "rev", "num", "merged", "grouped", "cold"] {
            let (recs, last) = collect(
                &dir.join(format!("cf_{cf}/wal-2.log")),
                Layout::PerCf,
                2,
                caps,
                None,
            )
            .unwrap();
            let (mut puts, mut deletes, mut singles, mut merges, mut ranges, mut ttls) =
                (0, 0, 0, 0, 0, 0);
            for rec in &recs {
                match rec {
                    ReplayRecord::Point(r) => {
                        seqs.push(r.seq);
                        match r.kind {
                            KIND_PUT => {
                                puts += 1;
                                ttls += usize::from(r.ttl != 0);
                                // Every value names round 2 and its key's index.
                                let i = std::str::from_utf8(&r.key)
                                    .ok()
                                    .and_then(|k| k.get(3..))
                                    .and_then(|n| n.parse::<usize>().ok());
                                if let (Some(i), "plain" | "rev" | "cold" | "grouped") = (i, cf) {
                                    let v = String::from_utf8_lossy(&r.value);
                                    let ok = v == format!("value-2-{i}")
                                        || v.starts_with(&format!("2.{i}:"));
                                    assert!(
                                        ok || r.key.starts_with(b"ent"),
                                        "{cf} {:?} = {v:.40}",
                                        String::from_utf8_lossy(&r.key)
                                    );
                                }
                            }
                            KIND_DELETE => deletes += 1,
                            KIND_SINGLE_DELETE => singles += 1,
                            KIND_MERGE => {
                                merges += 1;
                                assert!(r.value.starts_with(b"+2."), "{cf}: {:?}", r.value);
                            }
                            k => panic!("{cf}: kind {k}"),
                        }
                    }
                    ReplayRecord::RangeDelete { start, end, .. } => {
                        ranges += 1;
                        assert_eq!(
                            (start.as_slice(), end.as_slice()),
                            (&b"key130"[..], &b"key140"[..])
                        );
                    }
                    other => panic!("{cf}: {other:?}"),
                }
            }
            assert_eq!(
                last,
                recs.iter().map(ReplayRecord::replay_seq).max().unwrap()
            );
            let n = |f: fn(usize) -> bool| (0..200).filter(|&i| f(i)).count();
            let want = match cf {
                // Puts, TTL puts and entities; deletes where i % 7 == 2; one span.
                "plain" => (
                    200 + n(|i| i % 11 == 0) + n(|i| i % 31 == 0),
                    n(|i| i % 7 == 2),
                    0,
                    0,
                    1,
                    n(|i| i % 11 == 0),
                ),
                "rev" => (200, 0, 1, 0, 0, 0),
                "merged" => (0, 0, 0, 200, 0, 0),
                _ => (200, 0, 0, 0, 0, 0),
            };
            assert_eq!((puts, deletes, singles, merges, ranges, ttls), want, "{cf}");
        }
        // The tail continues the catalog's sequence, never reuses it.
        let flushed = crate::waves08::recover_catalog(&dir)
            .unwrap()
            .cfs
            .iter()
            .flat_map(|cf| cf.tables.iter().map(|t| t.meta.max_seq))
            .max()
            .unwrap();
        assert!(
            seqs.iter().all(|&s| s > flushed),
            "a tail sequence at or below {flushed}"
        );

        // A cleanly closed directory leaves header-only generations; managed
        // mode's unified log too.
        for (fx, rel, layout, generation) in [
            ("percf-closed", "cf_plain/wal-3.log", Layout::PerCf, 3),
            ("managed", "unified-wal-2.log", Layout::Unified, 2),
        ] {
            let dir = fixture(fx);
            let mut txns = Prepares::default();
            let txns = (layout == Layout::Unified).then_some(&mut txns);
            assert_eq!(
                collect(&dir.join(rel), layout, generation, catalog_caps(&dir), txns)
                    .unwrap()
                    .0
                    .len(),
                0
            );
        }
    }

    /// The unified fixtures: round 1 of both families, a range delete, and
    /// the committed prepare `fixture-1` installed at its decision's sequence;
    /// with `unified-prepared`, also `fixture-undecided`, which stays invisible.
    #[test]
    fn the_unified_fixture_tails_replay() {
        for (fx, undecided) in [("unified", 0), ("unified-prepared", 1)] {
            let dir = fixture(fx);
            let catalog = crate::waves08::recover_catalog(&dir).unwrap();
            let id = |name: &str| {
                let m = catalog.to_manifest().unwrap();
                m.cfs
                    .iter()
                    .find(|cf| cf.name == name)
                    .map(|cf| cf.effective_unified_id())
            };
            let (x, y) = (id("x").unwrap(), id("y").unwrap());
            let mut txns = Prepares::default();
            let (recs, last) = collect(
                &dir.join("unified-wal-1.log"),
                Layout::Unified,
                1,
                catalog.caps,
                Some(&mut txns),
            )
            .unwrap();
            let mut per = std::collections::BTreeMap::<u64, usize>::new();
            for rec in &recs {
                match rec {
                    ReplayRecord::Point(r) => {
                        assert_eq!(r.kind, KIND_PUT);
                        *per.entry(key_cf(&r.key).unwrap()).or_default() += 1;
                        let i: usize = std::str::from_utf8(&r.key[9..]).unwrap().parse().unwrap();
                        let v = String::from_utf8_lossy(&r.value);
                        assert!(
                            v == format!("v1-{i}") || v.starts_with(&format!("1.{i}:")),
                            "{v:.40}"
                        );
                    }
                    ReplayRecord::RangeDelete { start, end, .. } => {
                        assert_eq!(
                            (start.clone(), end.clone()),
                            (pk(x, b"u050"), pk(x, b"u060"))
                        );
                    }
                    other => panic!("{other:?}"),
                }
            }
            assert_eq!(per, [(x, 150), (y, 150)].into_iter().collect());
            assert_eq!(txns.undecided(), undecided, "{fx}");
            let (installed, max) = txns.finish(id, 0).unwrap();
            assert_eq!(
                max, last,
                "{fx}: the commit decision is the newest sequence"
            );
            let mut keys: Vec<_> = installed
                .iter()
                .map(|r| match r {
                    ReplayRecord::Point(r) => {
                        assert_eq!((r.value.as_slice(), r.seq), (&b"committed"[..], max));
                        r.key.clone()
                    }
                    other => panic!("{other:?}"),
                })
                .collect();
            keys.sort();
            let mut want = vec![pk(x, b"prepared1"), pk(y, b"prepared1")];
            want.sort();
            assert_eq!(keys, want, "{fx}: only fixture-1 is installed");
        }
    }

    #[test]
    fn prepares_resolve_by_their_decisions() {
        let dir = tempfile::tempdir().unwrap();
        let (x, y) = (11u64, 22u64);
        let ids = |name: &str| match name {
            "x" => Some(x),
            "y" => Some(y),
            _ => None,
        };
        let committed = batch(
            2,
            &["x", "y"],
            &[
                (0, KIND_PUT, 0, b"a", b"1"),
                (0, KIND_MERGE, 1, b"a", b"+"),
                (1, KIND_RANGE_DELETE, 0, b"b", b"d"),
            ],
        );
        let seg = [
            header(LAYOUT_WAVES08_UNIFIED, 3),
            control(KIND_PREPARE, b"t1", &committed, 0),
            control(
                KIND_PREPARE,
                b"t2",
                &batch(1, &["x"], &[(0, KIND_PUT, 0, b"z", b"no")]),
                0,
            ),
            control(
                KIND_PREPARE,
                b"t3",
                &batch(1, &["y"], &[(0, KIND_DELETE, 0, b"q", b"")]),
                0,
            ),
            control(KIND_COMMIT_DECISION, b"t1", b"", 41),
            control(KIND_ABORT_DECISION, b"t2", b"", 0),
        ]
        .concat();
        let p = write(dir.path(), "unified-wal-3.log", &seg);
        let mut txns = Prepares::default();
        let (data, last) = collect(&p, Layout::Unified, 3, ALL, Some(&mut txns)).unwrap();
        assert!(data.is_empty(), "control records are never data");
        assert_eq!(last, 41, "the decision raises the watermark");
        assert_eq!(txns.undecided(), 1);
        let (recs, max) = txns.finish(ids, 0).unwrap();
        assert_eq!(max, 41);
        // Installed at commit_seq - span + 1 + ordinal; t2 aborted, t3 invisible.
        assert_eq!(recs.len(), 3);
        assert!(
            matches!(&recs[0], ReplayRecord::Point(r) if r.key == pk(x, b"a") && r.seq == 40 && r.kind == KIND_PUT)
        );
        assert!(
            matches!(&recs[1], ReplayRecord::Point(r) if r.key == pk(x, b"a") && r.seq == 41 && r.kind == KIND_MERGE)
        );
        assert!(
            matches!(&recs[2], ReplayRecord::RangeDelete { start, end, seq: 40 } if *start == pk(y, b"b") && *end == pk(y, b"d"))
        );

        // A decision for a prepare the log never holds, a double prepare, a
        // contradicting decision and a damaged body are corruption.
        let cases = [
            vec![control(KIND_COMMIT_DECISION, b"t9", b"", 5)],
            vec![
                control(KIND_PREPARE, b"t1", &committed, 0),
                control(KIND_PREPARE, b"t1", &committed, 0),
            ],
            vec![
                control(KIND_PREPARE, b"t1", &committed, 0),
                control(KIND_ABORT_DECISION, b"t1", b"", 0),
                control(KIND_COMMIT_DECISION, b"t1", b"", 9),
            ],
            vec![control(
                KIND_PREPARE,
                b"t1",
                &committed[..committed.len() - 1],
                0,
            )],
            vec![control(KIND_PREPARE, b"t\0", &committed, 0)],
        ];
        for (i, frames) in cases.iter().enumerate() {
            let p = write(
                dir.path(),
                "unified-wal-3.log",
                &[header(LAYOUT_WAVES08_UNIFIED, 3), frames.concat()].concat(),
            );
            let res = collect(&p, Layout::Unified, 3, ALL, Some(&mut Prepares::default()));
            assert!(
                matches!(res, Err(OndaError::Corruption(_))),
                "case {i}: {res:?}"
            );
        }
        // Control records need CAP_TXN_DECISIONS.
        let p = write(dir.path(), "unified-wal-3.log", &seg);
        let res = collect(
            &p,
            Layout::Unified,
            3,
            ALL & !CAP_TXN_DECISIONS,
            Some(&mut Prepares::default()),
        );
        assert!(matches!(res, Err(OndaError::Corruption(_))));

        // A committed batch naming a family the catalog lacks, a committed
        // spill and an impossible span refuse the open.
        let mut t = Prepares::default();
        t.txns.push(Txn {
            id: b"t".to_vec(),
            body: Body::Batch(decode_batch(&committed).unwrap()),
            decision: Some(Decision::Commit(41)),
            decision_gen: 0,
        });
        assert!(t.finish(|_| None, 0).is_err());
        let mut t = Prepares::default();
        t.txns.push(Txn {
            id: b"s".to_vec(),
            body: Body::Spill,
            decision: Some(Decision::Commit(4)),
            decision_gen: 0,
        });
        assert!(matches!(
            t.finish(ids, 0),
            Err(OndaError::UnsupportedFormat(_))
        ));
        let mut t = Prepares::default();
        t.txns.push(Txn {
            id: b"t".to_vec(),
            body: Body::Batch(decode_batch(&committed).unwrap()),
            decision: Some(Decision::Commit(1)),
            decision_gen: 0,
        });
        assert!(matches!(t.finish(ids, 0), Err(OndaError::Corruption(_))));
    }
}
