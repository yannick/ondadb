//! Numbered manifest version edits (2.2): the `MANIFEST-EDITS` wire format and
//! the rules for applying an edit to a [`Manifest`].
//!
//! A full `MANIFEST` rewrite costs O(catalog) bytes and one fsync per
//! structural change — 12.4 MiB per persist at 100k parts, paid by every flush.
//! An *edit* describes only what changed. The durable catalog is therefore a
//! periodic snapshot (`MANIFEST`, unchanged in shape) plus an append-only log of
//! numbered edits (`MANIFEST-EDITS`), and the snapshot's tagged tail records
//! which edit id it already contains.
//!
//! # Torn tails are not the WAL's torn tails
//!
//! The record framing here is shaped like the WAL's (`[len][crc][payload]`) but
//! its contract is the opposite. `wal::replay` treats *any* unreadable trailing
//! frame as a clean tail, because a WAL's last record may legitimately be a
//! partial write. Here only an **EOF-truncated** record header or payload ends
//! replay cleanly: a complete record whose CRC fails, whose op code is unknown,
//! whose id is out of sequence, or whose preconditions do not hold is
//! [`OndaError::Corruption`] — the bytes contradict a format this binary
//! implements. Do not reach for the WAL helpers here.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::encoding::{
    append_u32, append_u64, append_uvarint, append_varint, checksum, read_u32, read_u64, uvarint,
    varint,
};
use crate::error::{OndaError, Result};
use crate::manifest::{CfManifest, Manifest, SstMeta, WalLayout};

/// `YOLODBED`: the yoloDB edit-log magic, 8 ASCII bytes
/// ([`crate::format::edit_log`]). The record framing and op table are shared
/// with 0.9's `ONDE` log; only the header changed (and the checksum, to
/// CRC32-C).
pub const EDIT_LOG_MAGIC: [u8; 8] = crate::format::edit_log::MAGIC;
/// Schema of the edit-log framing. Bumped only by an incompatible change to the
/// header or the record frame — never by adding an op code.
pub const EDIT_LOG_SCHEMA: u32 = crate::format::edit_log::SCHEMA;
/// Fixed header width, at offset 0. Records begin immediately after it.
pub const EDIT_LOG_HEADER_BYTES: usize = crate::format::edit_log::HEADER_BYTES;
/// Fixed record-frame overhead (`len u32 | crc32 u32`).
pub const EDIT_RECORD_HEADER_BYTES: usize = 8;
/// Largest payload a single record may declare. Checked *before* any allocation,
/// so a corrupt length can never ask for gigabytes.
pub const MAX_EDIT_RECORD_BYTES: usize = 64 << 20;

/// Path of the edit log within a database directory.
pub fn edit_log_path(db_dir: impl AsRef<Path>) -> PathBuf {
    db_dir.as_ref().join("MANIFEST-EDITS")
}

/// Path of the edit log's temp file, written by snapshot compaction and renamed
/// over the live log. A leftover copy is a crash artifact, never state.
pub fn edit_log_tmp_path(db_dir: impl AsRef<Path>) -> PathBuf {
    db_dir.as_ref().join("MANIFEST-EDITS.tmp")
}

fn corrupt(msg: impl Into<String>) -> OndaError {
    OndaError::Corruption(msg.into())
}

/// `"ONDE"`, the 0.9 edit-log magic as that binary stored it (a LE `u32`).
const ONDA09_EDIT_LOG_MAGIC: u32 = 0x4F4E_4445;

// ---------------------------------------------------------------------------
// Ops
// ---------------------------------------------------------------------------

/// Op codes, pinned. 13..=63 are unassigned and reject as corruption; codes
/// >= 64 are never assigned, mirroring the WAL's kind rule.
mod code {
    pub const ADD_TABLE: u64 = 1;
    pub const REMOVE_TABLE: u64 = 2;
    pub const UPDATE_TABLE: u64 = 3;
    pub const CREATE_CF: u64 = 4;
    pub const DROP_CF: u64 = 5;
    pub const SET_CF_CONFIG: u64 = 6;
    pub const SET_NEXT_FILE_ID: u64 = 7;
    pub const SET_GLOBAL_SEQ: u64 = 8;
    pub const SET_WAL_LAYOUT: u64 = 9;
    pub const SET_NONCE: u64 = 10;
    pub const SET_CAPABILITY: u64 = 11;
    pub const REMOVE_TABLES: u64 = 12;
    /// Highest op code that may ever be assigned a meaning.
    pub const MAX_ASSIGNABLE: u64 = 63;
}

// The op table is part of the registry (`docs/format-registry.md`): a code is
// never renumbered, so a change here must fail the build, not just a test.
const _: () = assert!(
    code::ADD_TABLE == 1
        && code::REMOVE_TABLE == 2
        && code::UPDATE_TABLE == 3
        && code::CREATE_CF == 4
        && code::DROP_CF == 5
        && code::SET_CF_CONFIG == 6
        && code::SET_NEXT_FILE_ID == 7
        && code::SET_GLOBAL_SEQ == 8
        && code::SET_WAL_LAYOUT == 9
        && code::SET_NONCE == 10
        && code::SET_CAPABILITY == 11
        && code::REMOVE_TABLES == 12
        && code::MAX_ASSIGNABLE == 63
);
const _: () = assert!(
    mask::LEVEL == 0x01
        && mask::TIER == 0x02
        && mask::OBJECT == 0x04
        && mask::PARTITION == 0x08
        && mask::MAX_ENTRY_TIME == 0x10
        && mask::LAST_COMPACTION_TIME == 0x20
);

/// Bits of the [`Op::UpdateTable`] field mask. Present values follow the mask in
/// **ascending bit order**.
mod mask {
    pub const LEVEL: u64 = 0x01;
    pub const TIER: u64 = 0x02;
    pub const OBJECT: u64 = 0x04;
    pub const PARTITION: u64 = 0x08;
    pub const MAX_ENTRY_TIME: u64 = 0x10;
    pub const LAST_COMPACTION_TIME: u64 = 0x20;
    pub const KNOWN: u64 =
        LEVEL | TIER | OBJECT | PARTITION | MAX_ENTRY_TIME | LAST_COMPACTION_TIME;
}

/// The mutable fields of a published table. `None` means "leave alone"; `Some`
/// carries the new value, which for the four optional fields may itself be
/// `None` (clear it).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableUpdate {
    pub level: Option<u32>,
    pub tier: Option<Option<String>>,
    pub object: Option<Option<String>>,
    pub partition: Option<Option<String>>,
    pub max_entry_time: Option<Option<i64>>,
    /// Periodic-compaction age stamp (0.3). A compaction output carries a fresh
    /// stamp, so a job that rewrites in place updates it through this field.
    pub last_compaction_time: Option<Option<i64>>,
}

impl TableUpdate {
    /// The wire mask this update encodes to. Zero means the update is empty,
    /// which is not representable on disk.
    pub fn mask(&self) -> u64 {
        let mut m = 0;
        if self.level.is_some() {
            m |= mask::LEVEL;
        }
        if self.tier.is_some() {
            m |= mask::TIER;
        }
        if self.object.is_some() {
            m |= mask::OBJECT;
        }
        if self.partition.is_some() {
            m |= mask::PARTITION;
        }
        if self.max_entry_time.is_some() {
            m |= mask::MAX_ENTRY_TIME;
        }
        if self.last_compaction_time.is_some() {
            m |= mask::LAST_COMPACTION_TIME;
        }
        m
    }

    /// A part move onto a shared tier changes the tier **and** the object name;
    /// a move onto an unshared tier clears the object. Both are one update.
    pub fn relocation(tier: Option<String>, object: Option<String>) -> TableUpdate {
        TableUpdate {
            tier: Some(tier),
            object: Some(object),
            ..TableUpdate::default()
        }
    }

    fn apply_to(&self, sst: &mut SstMeta) {
        if let Some(level) = self.level {
            sst.level = level;
        }
        if let Some(tier) = &self.tier {
            sst.tier = tier.clone();
        }
        if let Some(object) = &self.object {
            sst.object = object.clone();
        }
        if let Some(partition) = &self.partition {
            sst.partition = partition.clone();
        }
        if let Some(time) = &self.max_entry_time {
            sst.max_entry_time = *time;
        }
        if let Some(time) = &self.last_compaction_time {
            sst.last_compaction_time = *time;
        }
    }
}

/// One catalog mutation. Column families are name-keyed: `CfManifest.name` is
/// the only catalog identity a CF has (`unified::cf_id` is a WAL routing hash
/// and must never be conflated with one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Publish a new table. The id must be absent from the CF.
    AddTable { cf: String, meta: SstMeta },
    /// Retire one table, asserting the level it is retired from.
    RemoveTable {
        cf: String,
        id: u64,
        expected_level: u32,
    },
    /// Mutate a published table in place (the part mover's tier/object flip).
    UpdateTable {
        cf: String,
        id: u64,
        update: TableUpdate,
    },
    /// Register a column family with its opaque config blob.
    CreateCf { name: String, config: Vec<u8> },
    /// Unregister a column family. Every one of its tables must be removed by
    /// the same edit.
    DropCf { name: String },
    /// Replace a column family's config blob — the carrier for partition rules
    /// and tier rules, which are not manifest fields.
    SetCfConfig { name: String, config: Vec<u8> },
    /// Raise the file-id allocator. Monotone.
    SetNextFileId(u64),
    /// Raise the global commit sequence. Monotone (invariant 5).
    SetGlobalSeq(u64),
    /// Flip the database-wide WAL layout. One-way.
    SetWalLayout(WalLayout),
    /// Mint the shared-tier instance nonce. Once only — object names embed it.
    SetNonce(u64),
    /// Record durably enabled format capabilities.
    SetCapability(u64),
    /// Retire a batch of tables from one CF (compaction inputs, FIFO victims).
    RemoveTables { cf: String, ids: Vec<u64> },
}

/// One numbered, atomically applied group of ops.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    pub ops: Vec<Op>,
}

impl VersionEdit {
    pub fn new(ops: Vec<Op>) -> VersionEdit {
        VersionEdit { ops }
    }

    pub fn push(&mut self, op: Op) {
        self.ops.push(op);
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn append_bytes(b: &mut Vec<u8>, v: &[u8]) {
    append_uvarint(b, v.len() as u64);
    b.extend_from_slice(v);
}

fn append_opt_str(b: &mut Vec<u8>, v: Option<&str>) {
    match v {
        None => b.push(0),
        Some(s) => {
            b.push(1);
            append_bytes(b, s.as_bytes());
        }
    }
}

fn append_opt_bytes(b: &mut Vec<u8>, v: Option<&[u8]>) {
    match v {
        None => b.push(0),
        Some(x) => {
            b.push(1);
            append_bytes(b, x);
        }
    }
}

fn append_opt_varint(b: &mut Vec<u8>, v: Option<i64>) {
    match v {
        None => b.push(0),
        Some(x) => {
            b.push(1);
            append_varint(b, x);
        }
    }
}

/// Encode one `SstMeta`. Field order mirrors the struct declaration exactly, so
/// the inventory guard can be read as a field-by-field walk.
fn append_sst_meta(b: &mut Vec<u8>, m: &SstMeta) {
    append_uvarint(b, m.id);
    append_uvarint(b, u64::from(m.level));
    append_uvarint(b, m.num_entries);
    append_uvarint(b, m.num_tombstones);
    append_uvarint(b, m.max_seq);
    append_uvarint(b, m.klog_size);
    append_uvarint(b, m.vlog_size);
    append_bytes(b, &m.min_key);
    append_bytes(b, &m.max_key);
    append_opt_str(b, m.partition.as_deref());
    append_opt_str(b, m.tier.as_deref());
    append_opt_varint(b, m.max_entry_time);
    append_opt_str(b, m.object.as_deref());
    append_opt_varint(b, m.last_compaction_time);
    // 1.2's range summary. Encoded unconditionally rather than behind a
    // presence byte: the edit log is a private, versioned artifact of this
    // database (unlike the manifest, which must stay VERSION-1-shaped for a
    // legacy-only catalog), and five more fields per table cost less than a
    // branch each would.
    append_uvarint(b, m.range_count);
    append_uvarint(b, m.range_min_seq);
    append_uvarint(b, m.range_max_seq);
    append_opt_bytes(b, m.range_min_key.as_deref());
    append_opt_bytes(b, m.range_max_key.as_deref());
}

/// Encode one op, op code first.
pub fn encode_op(b: &mut Vec<u8>, op: &Op) {
    match op {
        Op::AddTable { cf, meta } => {
            append_uvarint(b, code::ADD_TABLE);
            append_bytes(b, cf.as_bytes());
            append_sst_meta(b, meta);
        }
        Op::RemoveTable {
            cf,
            id,
            expected_level,
        } => {
            append_uvarint(b, code::REMOVE_TABLE);
            append_bytes(b, cf.as_bytes());
            append_uvarint(b, *id);
            append_uvarint(b, u64::from(*expected_level));
        }
        Op::UpdateTable { cf, id, update } => {
            append_uvarint(b, code::UPDATE_TABLE);
            append_bytes(b, cf.as_bytes());
            append_uvarint(b, *id);
            append_uvarint(b, update.mask());
            // Ascending bit order, so the decoder is a single ordered walk.
            if let Some(level) = update.level {
                append_uvarint(b, u64::from(level));
            }
            if let Some(tier) = &update.tier {
                append_opt_str(b, tier.as_deref());
            }
            if let Some(object) = &update.object {
                append_opt_str(b, object.as_deref());
            }
            if let Some(partition) = &update.partition {
                append_opt_str(b, partition.as_deref());
            }
            if let Some(time) = &update.max_entry_time {
                append_opt_varint(b, *time);
            }
            if let Some(time) = &update.last_compaction_time {
                append_opt_varint(b, *time);
            }
        }
        Op::CreateCf { name, config } => {
            append_uvarint(b, code::CREATE_CF);
            append_bytes(b, name.as_bytes());
            append_bytes(b, config);
        }
        Op::DropCf { name } => {
            append_uvarint(b, code::DROP_CF);
            append_bytes(b, name.as_bytes());
        }
        Op::SetCfConfig { name, config } => {
            append_uvarint(b, code::SET_CF_CONFIG);
            append_bytes(b, name.as_bytes());
            append_bytes(b, config);
        }
        Op::SetNextFileId(v) => {
            append_uvarint(b, code::SET_NEXT_FILE_ID);
            append_uvarint(b, *v);
        }
        Op::SetGlobalSeq(v) => {
            append_uvarint(b, code::SET_GLOBAL_SEQ);
            append_uvarint(b, *v);
        }
        Op::SetWalLayout(layout) => {
            append_uvarint(b, code::SET_WAL_LAYOUT);
            b.push(u8::from(*layout == WalLayout::Unified));
        }
        Op::SetNonce(nonce) => {
            append_uvarint(b, code::SET_NONCE);
            append_u64(b, *nonce);
        }
        Op::SetCapability(bits) => {
            append_uvarint(b, code::SET_CAPABILITY);
            append_u64(b, *bits);
        }
        Op::RemoveTables { cf, ids } => {
            append_uvarint(b, code::REMOVE_TABLES);
            append_bytes(b, cf.as_bytes());
            append_uvarint(b, ids.len() as u64);
            for id in ids {
                append_uvarint(b, *id);
            }
        }
    }
}

/// Encode the payload of one record: `edit_id u64 LE | op_count | ops`.
pub fn encode_payload(edit_id: u64, edit: &VersionEdit) -> Vec<u8> {
    let mut b = Vec::new();
    append_u64(&mut b, edit_id);
    append_uvarint(&mut b, edit.ops.len() as u64);
    for op in &edit.ops {
        encode_op(&mut b, op);
    }
    b
}

/// Encode a complete record: `len u32 | crc32 u32 | payload`.
pub fn encode_record(edit_id: u64, edit: &VersionEdit) -> Vec<u8> {
    let payload = encode_payload(edit_id, edit);
    let mut b = Vec::with_capacity(EDIT_RECORD_HEADER_BYTES + payload.len());
    append_u32(&mut b, payload.len() as u32);
    append_u32(&mut b, checksum(&payload));
    b.extend_from_slice(&payload);
    b
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// A bounds-checked cursor whose every failure names the op it was decoding.
struct Cur<'a> {
    p: &'a [u8],
    op_index: usize,
}

impl<'a> Cur<'a> {
    fn new(p: &'a [u8]) -> Cur<'a> {
        Cur { p, op_index: 0 }
    }

    fn bad(&self, what: &str) -> OndaError {
        corrupt(format!(
            "manifest edit: op index {}: truncated or invalid {what}",
            self.op_index
        ))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.p.len() < n {
            return Err(self.bad("field"));
        }
        let (v, rest) = self.p.split_at(n);
        self.p = rest;
        Ok(v)
    }

    fn u64le(&mut self) -> Result<u64> {
        Ok(read_u64(self.take(8)?))
    }

    fn uvar(&mut self) -> Result<u64> {
        let (v, n) = uvarint(self.p).ok_or_else(|| self.bad("varint"))?;
        self.p = &self.p[n..];
        Ok(v)
    }

    fn u32_from_uvar(&mut self, what: &str) -> Result<u32> {
        let v = self.uvar()?;
        u32::try_from(v).map_err(|_| {
            corrupt(format!(
                "manifest edit: op index {}: {what} {v} exceeds u32",
                self.op_index
            ))
        })
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.uvar()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        let raw = self.bytes()?;
        String::from_utf8(raw).map_err(|_| {
            corrupt(format!(
                "manifest edit: op index {}: name is not valid UTF-8",
                self.op_index
            ))
        })
    }

    fn opt_string(&mut self) -> Result<Option<String>> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.string()?)),
            other => Err(corrupt(format!(
                "manifest edit: op index {}: optional-string tag {other} is not 0 or 1",
                self.op_index
            ))),
        }
    }

    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.bytes()?)),
            other => Err(corrupt(format!(
                "manifest edit: op index {}: optional-bytes tag {other} is not 0 or 1",
                self.op_index
            ))),
        }
    }

    fn opt_varint(&mut self) -> Result<Option<i64>> {
        match self.byte()? {
            0 => Ok(None),
            1 => {
                let (v, n) = varint(self.p).ok_or_else(|| self.bad("signed varint"))?;
                self.p = &self.p[n..];
                Ok(Some(v))
            }
            other => Err(corrupt(format!(
                "manifest edit: op index {}: optional-int tag {other} is not 0 or 1",
                self.op_index
            ))),
        }
    }

    fn sst_meta(&mut self) -> Result<SstMeta> {
        Ok(SstMeta {
            id: self.uvar()?,
            level: self.u32_from_uvar("level")?,
            num_entries: self.uvar()?,
            num_tombstones: self.uvar()?,
            max_seq: self.uvar()?,
            klog_size: self.uvar()?,
            vlog_size: self.uvar()?,
            min_key: self.bytes()?,
            max_key: self.bytes()?,
            partition: self.opt_string()?,
            tier: self.opt_string()?,
            max_entry_time: self.opt_varint()?,
            object: self.opt_string()?,
            last_compaction_time: self.opt_varint()?,
            range_count: self.uvar()?,
            range_min_seq: self.uvar()?,
            range_max_seq: self.uvar()?,
            range_min_key: self.opt_bytes()?,
            range_max_key: self.opt_bytes()?,
        })
    }

    fn op(&mut self) -> Result<Op> {
        let code = self.uvar()?;
        match code {
            code::ADD_TABLE => Ok(Op::AddTable {
                cf: self.string()?,
                meta: self.sst_meta()?,
            }),
            code::REMOVE_TABLE => Ok(Op::RemoveTable {
                cf: self.string()?,
                id: self.uvar()?,
                expected_level: self.u32_from_uvar("level")?,
            }),
            code::UPDATE_TABLE => {
                let cf = self.string()?;
                let id = self.uvar()?;
                let m = self.uvar()?;
                if m == 0 || m & !mask::KNOWN != 0 {
                    return Err(corrupt(format!(
                        "manifest edit: op index {}: UpdateTable mask {m:#x} \
                         is empty or outside the known mask {:#x}",
                        self.op_index,
                        mask::KNOWN
                    )));
                }
                let mut update = TableUpdate::default();
                if m & mask::LEVEL != 0 {
                    update.level = Some(self.u32_from_uvar("level")?);
                }
                if m & mask::TIER != 0 {
                    update.tier = Some(self.opt_string()?);
                }
                if m & mask::OBJECT != 0 {
                    update.object = Some(self.opt_string()?);
                }
                if m & mask::PARTITION != 0 {
                    update.partition = Some(self.opt_string()?);
                }
                if m & mask::MAX_ENTRY_TIME != 0 {
                    update.max_entry_time = Some(self.opt_varint()?);
                }
                if m & mask::LAST_COMPACTION_TIME != 0 {
                    update.last_compaction_time = Some(self.opt_varint()?);
                }
                Ok(Op::UpdateTable { cf, id, update })
            }
            code::CREATE_CF => Ok(Op::CreateCf {
                name: self.string()?,
                config: self.bytes()?,
            }),
            code::DROP_CF => Ok(Op::DropCf {
                name: self.string()?,
            }),
            code::SET_CF_CONFIG => Ok(Op::SetCfConfig {
                name: self.string()?,
                config: self.bytes()?,
            }),
            code::SET_NEXT_FILE_ID => Ok(Op::SetNextFileId(self.uvar()?)),
            code::SET_GLOBAL_SEQ => Ok(Op::SetGlobalSeq(self.uvar()?)),
            code::SET_WAL_LAYOUT => match self.byte()? {
                0 => Ok(Op::SetWalLayout(WalLayout::PerColumnFamily)),
                1 => Ok(Op::SetWalLayout(WalLayout::Unified)),
                other => Err(corrupt(format!(
                    "manifest edit: op index {}: SetWalLayout byte {other} is not 0 or 1",
                    self.op_index
                ))),
            },
            code::SET_NONCE => Ok(Op::SetNonce(self.u64le()?)),
            code::SET_CAPABILITY => Ok(Op::SetCapability(self.u64le()?)),
            code::REMOVE_TABLES => {
                let cf = self.string()?;
                let count = self.uvar()? as usize;
                // The vector still grows to whatever the bytes contain; the cap
                // only stops a lying count from asking for gigabytes up front.
                let mut ids = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    ids.push(self.uvar()?);
                }
                Ok(Op::RemoveTables { cf, ids })
            }
            other => Err(corrupt(format!(
                "manifest edit: op index {}: op code {other} is {}",
                self.op_index,
                if other > code::MAX_ASSIGNABLE {
                    "above the never-assigned bound 63"
                } else {
                    "unassigned"
                }
            ))),
        }
    }
}

/// Decode one record payload into its edit id and ops. Trailing bytes after the
/// last op are corruption: the payload length is exact.
pub fn decode_payload(payload: &[u8]) -> Result<(u64, VersionEdit)> {
    let mut cur = Cur::new(payload);
    let edit_id = cur.u64le()?;
    let count = cur.uvar()? as usize;
    let mut ops = Vec::with_capacity(count.min(4096));
    for i in 0..count {
        cur.op_index = i;
        ops.push(cur.op()?);
    }
    if !cur.p.is_empty() {
        return Err(corrupt(format!(
            "manifest edit {edit_id}: {} trailing bytes after {count} ops",
            cur.p.len()
        )));
    }
    Ok((edit_id, VersionEdit::new(ops)))
}

// ---------------------------------------------------------------------------
// Log header
// ---------------------------------------------------------------------------

/// The 32-byte header at offset 0 of `MANIFEST-EDITS`. Written once, by
/// snapshot compaction, and fsynced before any record is appended.
///
/// ```text
///  0 magic "YOLODBED" | 8 schema u32 = 1 | 12 base_applied_through u64
/// 20 snapshot_generation u64 | 28 crc32c u32 over bytes 0..28
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EditLogHeader {
    /// No record in this file has an id at or below this value.
    pub base_applied_through: u64,
    /// The snapshot generation this log was opened for. **Informational only**
    /// — see the recovery predicate in [`crate::manifest_edit`] docs and
    /// `recover_catalog`: the legal crash-between-rename state has the snapshot
    /// at `G+1` while the surviving log still says `G`, so equality would
    /// reject a legal database.
    pub snapshot_generation: u64,
}

impl EditLogHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(EDIT_LOG_HEADER_BYTES);
        b.extend_from_slice(&EDIT_LOG_MAGIC);
        append_u32(&mut b, EDIT_LOG_SCHEMA);
        append_u64(&mut b, self.base_applied_through);
        append_u64(&mut b, self.snapshot_generation);
        let crc = checksum(&b);
        append_u32(&mut b, crc);
        debug_assert_eq!(b.len(), EDIT_LOG_HEADER_BYTES);
        b
    }

    /// Decode the header. Never a torn tail — the header is written and fsynced
    /// before the file is ever appended to: a short file, a foreign magic and a
    /// bad header CRC are `Corruption`; an unknown schema, and a 0.9 `ONDE`
    /// log, are `UnsupportedFormat`.
    pub fn decode(data: &[u8]) -> Result<EditLogHeader> {
        if data.len() >= 4 && read_u32(data) == ONDA09_EDIT_LOG_MAGIC {
            return Err(OndaError::UnsupportedFormat(
                "manifest edit log: an ondaDB 0.9 log (ONDE); it is readable only through \
                 legacy_onda, and the database must be upgraded to yoloDB epoch 1"
                    .into(),
            ));
        }
        if data.len() < EDIT_LOG_HEADER_BYTES {
            return Err(corrupt(format!(
                "manifest edit log: file is {} bytes, shorter than the {EDIT_LOG_HEADER_BYTES}-byte header",
                data.len()
            )));
        }
        let head = &data[..EDIT_LOG_HEADER_BYTES];
        if head[..8] != EDIT_LOG_MAGIC {
            return Err(corrupt("manifest edit log: magic is not YOLODBED"));
        }
        let schema = read_u32(&head[8..12]);
        if schema != EDIT_LOG_SCHEMA {
            return Err(OndaError::UnsupportedFormat(format!(
                "manifest edit log: schema {schema} is not implemented by this binary"
            )));
        }
        if read_u32(&head[28..32]) != checksum(&head[..28]) {
            return Err(corrupt("manifest edit log: header CRC mismatch"));
        }
        Ok(EditLogHeader {
            base_applied_through: read_u64(&head[12..20]),
            snapshot_generation: read_u64(&head[20..28]),
        })
    }
}

/// One decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditRecord {
    pub edit_id: u64,
    pub edit: VersionEdit,
}

/// Decode every complete record after the header.
///
/// Returns the records and the byte offset one past the last complete record —
/// the point a writer must append at, so a torn tail is overwritten rather than
/// grown. An EOF-truncated frame header or payload ends the scan cleanly; a
/// complete frame that fails its CRC, or a length above
/// [`MAX_EDIT_RECORD_BYTES`], is corruption.
pub fn decode_records(data: &[u8]) -> Result<(Vec<EditRecord>, usize)> {
    let mut off = EDIT_LOG_HEADER_BYTES;
    let mut out = Vec::new();
    loop {
        if off + EDIT_RECORD_HEADER_BYTES > data.len() {
            break; // truncated frame header: clean tail
        }
        let len = read_u32(&data[off..off + 4]) as usize;
        if len > MAX_EDIT_RECORD_BYTES {
            return Err(corrupt(format!(
                "manifest edit log: record at offset {off} declares {len} bytes, \
                 above the {MAX_EDIT_RECORD_BYTES}-byte cap"
            )));
        }
        let body = off + EDIT_RECORD_HEADER_BYTES;
        if body + len > data.len() {
            break; // truncated payload: clean tail
        }
        let payload = &data[body..body + len];
        if read_u32(&data[off + 4..off + 8]) != checksum(payload) {
            return Err(corrupt(format!(
                "manifest edit log: complete record at offset {off} fails its CRC"
            )));
        }
        let (edit_id, edit) = decode_payload(payload)?;
        out.push(EditRecord { edit_id, edit });
        off = body + len;
    }
    Ok((out, off))
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

/// Simulated state of one column family during validation.
struct CfState {
    exists: bool,
    /// id -> level, seeded lazily from the manifest the first time the CF is
    /// touched, so validation is O(edit) rather than O(catalog) per op.
    tables: HashMap<u64, u32>,
}

/// The candidate catalog an edit is validated against before anything is
/// mutated. Applying an edit is all-or-nothing: every precondition is checked
/// here first, and only then does the second pass touch the real `Manifest`.
///
/// Two seeding modes, and the difference is the whole reason replay stays
/// bounded. `lazy` borrows the manifest and builds a CF's id set the first time
/// that CF is touched — right for one edit against a catalog, since only the
/// touched CFs are ever indexed. `eager` builds every CF's id set once and is
/// then reused across many edits ([`CatalogReplayer`]); doing the lazy thing per
/// record instead makes replay O(catalog × edits), which at 10k tables and 4096
/// records is seconds of hashing rather than milliseconds.
struct Candidate<'a> {
    seed: Option<&'a Manifest>,
    cfs: HashMap<String, CfState>,
    next_file_id: u64,
    global_seq: u64,
    wal_layout: WalLayout,
    instance_nonce: Option<u64>,
    caps: u64,
}

impl<'a> Candidate<'a> {
    fn lazy(m: &'a Manifest) -> Candidate<'a> {
        Candidate {
            seed: Some(m),
            ..Candidate::scalars(m)
        }
    }

    fn eager(m: &Manifest) -> Candidate<'static> {
        Candidate {
            cfs: m
                .cfs
                .iter()
                .map(|cf| {
                    (
                        cf.name.clone(),
                        CfState {
                            exists: true,
                            tables: cf.sstables.iter().map(|s| (s.id, s.level)).collect(),
                        },
                    )
                })
                .collect(),
            ..Candidate::scalars(m)
        }
    }

    /// The scalar fields both constructors share; `seed` and `cfs` are set by
    /// the constructor that knows which mode it wants.
    fn scalars(m: &Manifest) -> Candidate<'static> {
        Candidate {
            seed: None,
            cfs: HashMap::new(),
            next_file_id: m.next_file_id,
            global_seq: m.global_seq,
            wal_layout: m.wal_layout,
            instance_nonce: m.instance_nonce,
            caps: m.caps,
        }
    }

    fn cf(&mut self, name: &str) -> &mut CfState {
        let seed = self.seed;
        self.cfs.entry(name.to_string()).or_insert_with(|| {
            // With an eager seed a missing entry means the CF genuinely does not
            // exist; with a lazy one it means the manifest has no such CF. Both
            // are "absent".
            match seed.and_then(|m| m.cfs.iter().find(|c| c.name == name)) {
                Some(cf) => CfState {
                    exists: true,
                    tables: cf.sstables.iter().map(|s| (s.id, s.level)).collect(),
                },
                None => CfState {
                    exists: false,
                    tables: HashMap::new(),
                },
            }
        })
    }
}

/// Applies a *sequence* of edits to one catalog, keeping the validation index
/// across records.
///
/// Recovery replays every record between two snapshots, and each record's
/// preconditions must hold against the catalog the previous records produced.
/// Rebuilding the index per record would make replay O(catalog × edits); this
/// builds it once. A rejected edit poisons the replayer — recovery aborts with
/// `Corruption` at the first bad record, so no partially-applied catalog escapes
/// it. Use [`apply_edit`] for the single-edit, strictly all-or-nothing case.
pub struct CatalogReplayer {
    manifest: Manifest,
    candidate: Candidate<'static>,
}

impl std::fmt::Debug for CatalogReplayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogReplayer")
            .field("cfs", &self.manifest.cfs.len())
            .finish_non_exhaustive()
    }
}

impl CatalogReplayer {
    pub fn new(manifest: Manifest) -> CatalogReplayer {
        let candidate = Candidate::eager(&manifest);
        CatalogReplayer {
            manifest,
            candidate,
        }
    }

    pub fn apply(&mut self, edit: &VersionEdit) -> Result<()> {
        for (i, op) in edit.ops.iter().enumerate() {
            validate_op(&mut self.candidate, i, op)?;
        }
        for op in &edit.ops {
            mutate(&mut self.manifest, op);
        }
        Ok(())
    }

    pub fn finish(self) -> Manifest {
        self.manifest
    }
}

fn precondition(index: usize, msg: impl std::fmt::Display) -> OndaError {
    corrupt(format!(
        "manifest edit: op index {index}: precondition failed: {msg}"
    ))
}

/// Validate one op against the candidate, mutating the candidate on success.
fn validate_op(c: &mut Candidate<'_>, i: usize, op: &Op) -> Result<()> {
    match op {
        Op::AddTable { cf, meta } => {
            let st = c.cf(cf);
            if !st.exists {
                return Err(precondition(
                    i,
                    format!("AddTable: no column family {cf:?}"),
                ));
            }
            if st.tables.contains_key(&meta.id) {
                return Err(precondition(
                    i,
                    format!("AddTable: table {} already in {cf:?}", meta.id),
                ));
            }
            st.tables.insert(meta.id, meta.level);
        }
        Op::RemoveTable {
            cf,
            id,
            expected_level,
        } => {
            let st = c.cf(cf);
            match st.tables.get(id) {
                None => {
                    return Err(precondition(
                        i,
                        format!("RemoveTable: table {id} absent from {cf:?}"),
                    ))
                }
                Some(level) if level != expected_level => {
                    return Err(precondition(
                        i,
                        format!(
                            "RemoveTable: table {id} is at level {level}, not {expected_level}"
                        ),
                    ))
                }
                Some(_) => {}
            }
            st.tables.remove(id);
        }
        Op::UpdateTable { cf, id, update } => {
            if update.mask() == 0 {
                return Err(precondition(i, "UpdateTable: empty field mask"));
            }
            let st = c.cf(cf);
            match st.tables.get_mut(id) {
                None => {
                    return Err(precondition(
                        i,
                        format!("UpdateTable: table {id} absent from {cf:?}"),
                    ))
                }
                Some(level) => {
                    if let Some(new_level) = update.level {
                        *level = new_level;
                    }
                }
            }
        }
        Op::CreateCf { name, .. } => {
            let st = c.cf(name);
            if st.exists {
                return Err(precondition(
                    i,
                    format!("CreateCF: column family {name:?} already exists"),
                ));
            }
            st.exists = true;
            st.tables.clear();
        }
        Op::DropCf { name } => {
            let st = c.cf(name);
            if !st.exists {
                return Err(precondition(
                    i,
                    format!("DropCF: no column family {name:?}"),
                ));
            }
            if !st.tables.is_empty() {
                return Err(precondition(
                    i,
                    format!(
                        "DropCF: {name:?} still has {} table(s); the same edit must remove them",
                        st.tables.len()
                    ),
                ));
            }
            st.exists = false;
        }
        Op::SetCfConfig { name, .. } => {
            if !c.cf(name).exists {
                return Err(precondition(
                    i,
                    format!("SetCFConfig: no column family {name:?}"),
                ));
            }
        }
        Op::SetNextFileId(v) => {
            if *v < c.next_file_id {
                return Err(precondition(
                    i,
                    format!("SetNextFileID: {v} is below the current {}", c.next_file_id),
                ));
            }
            c.next_file_id = *v;
        }
        Op::SetGlobalSeq(v) => {
            if *v < c.global_seq {
                return Err(precondition(
                    i,
                    format!("SetGlobalSeq: {v} is below the current {}", c.global_seq),
                ));
            }
            c.global_seq = *v;
        }
        Op::SetWalLayout(layout) => {
            if *layout != WalLayout::Unified || c.wal_layout == WalLayout::Unified {
                return Err(precondition(
                    i,
                    "SetWalLayout: the layout flip is one-way, per-CF to unified, once",
                ));
            }
            c.wal_layout = WalLayout::Unified;
        }
        Op::SetNonce(nonce) => {
            if c.instance_nonce.is_some() {
                return Err(precondition(
                    i,
                    "SetNonce: the instance nonce is minted once and never changed",
                ));
            }
            c.instance_nonce = Some(*nonce);
        }
        Op::SetCapability(bits) => {
            crate::format::check_caps(*bits)?;
            c.caps |= *bits;
        }
        Op::RemoveTables { cf, ids } => {
            let st = c.cf(cf);
            for id in ids {
                if st.tables.remove(id).is_none() {
                    return Err(precondition(
                        i,
                        format!("RemoveTables: table {id} absent from {cf:?}"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn cf_index(m: &Manifest, name: &str) -> Option<usize> {
    m.cfs.iter().position(|c| c.name == name)
}

/// Second pass: mutate the real catalog. Every precondition already held, so
/// nothing here can fail — a mismatch would be an internal bug, not bad input.
fn mutate(m: &mut Manifest, op: &Op) {
    match op {
        Op::AddTable { cf, meta } => {
            if let Some(i) = cf_index(m, cf) {
                if meta.level == 0 {
                    // L0 is newest-first and `ColumnFamily::load` keeps the
                    // manifest's relative order within a level, so a replayed
                    // `AddTable` must land where `install_handles_l0` put the
                    // handle — at the front. Appending would reopen the family
                    // with its newest L0 table treated as its oldest, and
                    // newest-first shadowing is a correctness invariant
                    // (`src/compaction.rs`, the L0 input-selection comment).
                    m.cfs[i].sstables.insert(0, meta.clone());
                } else {
                    // Every level below L0 is re-sorted by `min_key` at load,
                    // so position in the vector carries no meaning there.
                    m.cfs[i].sstables.push(meta.clone());
                }
            }
        }
        Op::RemoveTable { cf, id, .. } => {
            if let Some(i) = cf_index(m, cf) {
                m.cfs[i].sstables.retain(|s| s.id != *id);
            }
        }
        Op::UpdateTable { cf, id, update } => {
            if let Some(i) = cf_index(m, cf) {
                if let Some(sst) = m.cfs[i].sstables.iter_mut().find(|s| s.id == *id) {
                    update.apply_to(sst);
                }
            }
        }
        Op::CreateCf { name, config } => m.cfs.push(CfManifest {
            name: name.clone(),
            config: config.clone(),
            sstables: Vec::new(),
            unified_id: None,
        }),
        Op::DropCf { name } => m.cfs.retain(|c| &c.name != name),
        Op::SetCfConfig { name, config } => {
            if let Some(i) = cf_index(m, name) {
                m.cfs[i].config = config.clone();
            }
        }
        Op::SetNextFileId(v) => m.next_file_id = *v,
        Op::SetGlobalSeq(v) => m.global_seq = *v,
        Op::SetWalLayout(layout) => m.wal_layout = *layout,
        Op::SetNonce(nonce) => m.instance_nonce = Some(*nonce),
        Op::SetCapability(bits) => m.caps |= *bits,
        Op::RemoveTables { cf, ids } => {
            if let Some(i) = cf_index(m, cf) {
                let drop: std::collections::HashSet<u64> = ids.iter().copied().collect();
                m.cfs[i].sstables.retain(|s| !drop.contains(&s.id));
            }
        }
    }
}

/// Apply one edit to `m`, all-or-nothing.
///
/// Two passes: the first validates every op against a candidate that costs
/// O(edit), not O(catalog); the second mutates. A failed precondition therefore
/// leaves `m` byte-for-byte untouched, which is what makes a replay that stops
/// mid-edit impossible.
pub fn apply_edit(m: &mut Manifest, edit: &VersionEdit) -> Result<()> {
    {
        let mut c = Candidate::lazy(m);
        for (i, op) in edit.ops.iter().enumerate() {
            validate_op(&mut c, i, op)?;
        }
    }
    for op in &edit.ops {
        mutate(m, op);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The log writer and the snapshot-compaction protocol
// ---------------------------------------------------------------------------

/// Snapshot compaction fires once the log exceeds this many bytes, unless the
/// snapshot itself is larger (rewriting a 40 MiB snapshot to reclaim 4 MiB of
/// log is not a saving). Constants, not options, in v1.
pub const SNAPSHOT_MIN_EDIT_BYTES: u64 = 4 << 20;
/// ...or once it holds this many records, whichever comes first. Replay cost is
/// per record, so a log of many tiny edits is compacted on count alone.
pub const MAX_EDITS_BEFORE_SNAPSHOT: u64 = 4096;

/// The snapshot-compaction trigger, checked after each append inside the same
/// `manifest_mu` section that appended.
pub fn snapshot_due(edit_bytes: u64, edit_count: u64, snapshot_bytes: u64) -> bool {
    edit_bytes > SNAPSHOT_MIN_EDIT_BYTES.max(snapshot_bytes)
        || edit_count > MAX_EDITS_BEFORE_SNAPSHOT
}

/// An open `MANIFEST-EDITS` positioned for append.
///
/// Every `append` is a complete record followed by an fsync, and that fsync is
/// **the commit point** of a catalog transaction: WAL reclaim and obsolete-input
/// deletion key off it, never off a snapshot write (AGENTS.md invariant 1).
#[derive(Debug)]
pub struct EditLog {
    path: PathBuf,
    file: std::fs::File,
    header: EditLogHeader,
    bytes: u64,
    count: u64,
}

impl EditLog {
    /// Write a fresh log — header only, no records — as a temp file, fsync it,
    /// rename it over the live path and fsync the directory. Steps 3 and 4 of
    /// the snapshot-compaction protocol, and the only way a header is ever
    /// written: the live file is **never** truncated in place.
    pub fn create(db_dir: impl AsRef<Path>, header: EditLogHeader) -> Result<EditLog> {
        let dir = db_dir.as_ref();
        let path = edit_log_path(dir);
        let tmp = edit_log_tmp_path(dir);
        let bytes = header.encode();
        {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)?;
            crate::util::fault::check(crate::util::fault::Call::Write)?;
            f.write_all(&bytes)?;
            crate::util::fault::check(crate::util::fault::Call::Sync)?;
            f.sync_all()?;
        }
        crate::util::fault::check(crate::util::fault::Call::Rename)?;
        std::fs::rename(&tmp, &path)?;
        crate::util::sync_parent_dir(&path)?;
        let file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Ok(EditLog {
            path,
            file,
            header,
            bytes: bytes.len() as u64,
            count: 0,
        })
    }

    /// Reopen an existing log for append after recovery has read it.
    ///
    /// `valid_end` is the offset one past the last complete record (the second
    /// value `decode_records` returns). A torn tail is truncated away here, so
    /// the next record is written over it rather than after it — an appended
    /// record following an EOF-truncated one would be unreachable, since replay
    /// stops at the first partial frame.
    pub fn open_for_append(
        db_dir: impl AsRef<Path>,
        header: EditLogHeader,
        valid_end: u64,
        count: u64,
    ) -> Result<EditLog> {
        let path = edit_log_path(db_dir.as_ref());
        let file = std::fs::OpenOptions::new().write(true).open(&path)?;
        if file.metadata()?.len() != valid_end {
            file.set_len(valid_end)?;
            file.sync_all()?;
        }
        drop(file);
        let file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Ok(EditLog {
            path,
            file,
            header,
            bytes: valid_end,
            count,
        })
    }

    pub fn header(&self) -> EditLogHeader {
        self.header
    }

    /// Bytes in the file, header included.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Records appended since the header.
    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one complete record and make it durable. Returns the number of
    /// bytes the record occupies.
    ///
    /// The record is written in a single `write_all`, so a crash can only leave
    /// an EOF-truncated frame — the one shape replay treats as a clean tail.
    pub fn append(&mut self, edit_id: u64, edit: &VersionEdit) -> Result<u64> {
        let rec = encode_record(edit_id, edit);
        if rec.len() - EDIT_RECORD_HEADER_BYTES > MAX_EDIT_RECORD_BYTES {
            return Err(OndaError::InvalidArgs(format!(
                "manifest edit {edit_id}: {} bytes exceeds the {MAX_EDIT_RECORD_BYTES}-byte \
                 record cap",
                rec.len() - EDIT_RECORD_HEADER_BYTES
            )));
        }
        crate::util::fault::check(crate::util::fault::Call::Write)?;
        self.file.write_all(&rec)?;
        crate::util::fault::check(crate::util::fault::Call::Flush)?;
        self.file.flush()?;
        crate::util::fault::check(crate::util::fault::Call::Sync)?;
        self.file.sync_all()?;
        self.bytes += rec.len() as u64;
        self.count += 1;
        Ok(rec.len() as u64)
    }
}

/// The four-step snapshot-compaction protocol, in order.
///
/// 1. write `MANIFEST.tmp` {generation `G+1`, `applied_through = N`,
///    `next_edit_id = N+1`, full catalog} and fsync it;
/// 2. rename it over `MANIFEST` and fsync the directory;
/// 3. write `MANIFEST-EDITS.tmp` {header with `base_applied_through = N`} and
///    fsync it;
/// 4. rename it over `MANIFEST-EDITS` and fsync the directory again.
///
/// Steps 1–2 are [`Manifest::save`]; steps 3–4 are [`EditLog::create`]. A crash
/// anywhere leaves a consistent database: before step 2 the old snapshot and the
/// old log still describe it; between 2 and 4 the new snapshot plus the *old*
/// log do, because replay skips ids at or below `applied_through`. The live log
/// is never truncated in place, which is what makes that true.
///
/// `manifest` is updated in place to the compacted cursor, and the caller must
/// hold `manifest_mu` across the whole call: a record appended to the old file
/// between step 1 and step 4 would be silently dropped by step 4's rename.
pub fn compact_snapshot(
    db_dir: impl AsRef<Path>,
    manifest: &mut Manifest,
    applied_through: u64,
) -> Result<EditLog> {
    let dir = db_dir.as_ref();
    let generation = manifest.generation + 1;
    manifest.generation = generation;
    manifest.applied_through = applied_through;
    manifest.next_edit_id = applied_through + 1;
    manifest.save(crate::manifest::manifest_path(dir))?;
    EditLog::create(
        dir,
        EditLogHeader {
            base_applied_through: applied_through,
            snapshot_generation: generation,
        },
    )
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

/// Delete the two crash artifacts a snapshot compaction can leave behind.
///
/// `MANIFEST.tmp` and `MANIFEST-EDITS.tmp` are written, fsynced and renamed; a
/// surviving copy is therefore always a partial write from a crash, never
/// state, and is never read. Called at open, before anything is loaded, and
/// only on a writable open — a read-only open never writes, not even an unlink.
pub fn sweep_manifest_temp_files(db_dir: impl AsRef<Path>) -> Result<()> {
    let dir = db_dir.as_ref();
    for path in [
        crate::manifest::manifest_path(dir).with_extension("tmp"),
        edit_log_tmp_path(dir),
    ] {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Load the snapshot and replay the edit log into it.
///
/// The predicate that decides whether a log belongs to a snapshot is
/// **`header.base_applied_through <= snapshot.applied_through`**, and that is
/// the whole predicate. `snapshot_generation` is informational: the legal
/// crash-between-the-two-renames state has `MANIFEST.generation == G+1` while
/// the surviving log still says `G`, so requiring equality would reject a
/// database that is perfectly consistent. Do not add a second check here.
pub fn recover_catalog(db_dir: impl AsRef<Path>) -> Result<Manifest> {
    let dir = db_dir.as_ref();
    // r1: a CRC-invalid snapshot fails the open; a missing one is empty.
    let m = Manifest::load(crate::manifest::manifest_path(dir))?;
    let data = match std::fs::read(edit_log_path(dir)) {
        Ok(d) => d,
        // r6: no log is the state a database has before its first append, and
        // the state a fresh backup/checkpoint destination is handed.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(reconciled(m)),
        Err(e) => return Err(e.into()),
    };

    // r2: a log this binary would have to interpret, written by a database that
    // never announced the capability, is state this binary cannot vouch for.
    if m.caps & crate::format::CAP_MANIFEST_EDITS == 0 {
        return Err(corrupt(
            "manifest edit log: present without CAP_MANIFEST_EDITS in the snapshot",
        ));
    }

    let header = EditLogHeader::decode(&data)?;
    // r3: the whole predicate.
    if header.base_applied_through > m.applied_through {
        return Err(corrupt(format!(
            "manifest edit log: base_applied_through {} is ahead of the snapshot's \
             applied_through {}",
            header.base_applied_through, m.applied_through
        )));
    }

    let (records, _) = decode_records(&data)?;
    // r4: ids are contiguous from the log's own base; a gap or a duplicate
    // anywhere in the file is corruption, not a tail.
    let base = header.base_applied_through;
    let snapshot_applied_through = m.applied_through;
    let mut applied_through = snapshot_applied_through;
    // One index for the whole replay: rebuilding it per record would make
    // recovery O(catalog x edits).
    let mut replayer = CatalogReplayer::new(m);
    for (i, rec) in records.iter().enumerate() {
        let expected = base + 1 + i as u64;
        if rec.edit_id != expected {
            return Err(corrupt(format!(
                "manifest edit log: record id {} where {expected} was expected \
                 (a gap or a duplicate is never a torn tail)",
                rec.edit_id
            )));
        }
        if rec.edit_id <= snapshot_applied_through {
            continue; // already contained in the snapshot
        }
        replayer.apply(&rec.edit).map_err(|e| match e {
            OndaError::Corruption(msg) => corrupt(format!("manifest edit {}: {msg}", rec.edit_id)),
            other => other,
        })?;
        applied_through = rec.edit_id;
    }
    let mut m = replayer.finish();
    m.applied_through = applied_through;
    m.next_edit_id = applied_through + 1;
    Ok(reconciled(m))
}

/// Reopen the log a [`recover_catalog`] just replayed, positioned for append
/// with any torn tail truncated away. `None` when the database has no log yet.
///
/// Writable opens only: it truncates and appends. A read-only open replays the
/// log and stops there (r8).
pub fn open_log_for_append(db_dir: impl AsRef<Path>) -> Result<Option<EditLog>> {
    let dir = db_dir.as_ref();
    let data = match std::fs::read(edit_log_path(dir)) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let header = EditLogHeader::decode(&data)?;
    let (records, valid_end) = decode_records(&data)?;
    EditLog::open_for_append(dir, header, valid_end as u64, records.len() as u64).map(Some)
}

/// r7: the allocator and the sequence must dominate everything the catalog
/// actually references, whichever edit last touched them.
fn reconciled(mut m: Manifest) -> Manifest {
    let mut max_id = 0;
    let mut max_seq = 0;
    for cf in &m.cfs {
        for sst in &cf.sstables {
            max_id = max_id.max(sst.id);
            max_seq = max_seq.max(sst.max_seq);
        }
    }
    m.next_file_id = m.next_file_id.max(max_id + 1);
    m.global_seq = m.global_seq.max(max_seq);
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_all_set() -> SstMeta {
        SstMeta {
            id: 9,
            level: 4,
            num_entries: 300,
            num_tombstones: 7,
            max_seq: 4_000,
            klog_size: 1 << 20,
            vlog_size: 1 << 18,
            min_key: b"k-min".to_vec(),
            max_key: b"k-max".to_vec(),
            partition: Some("img".into()),
            tier: Some("cold".into()),
            max_entry_time: Some(-1_700_000_000_000_000_001),
            object: Some("cf-x/deadbeef-9".into()),
            last_compaction_time: Some(-1_700_000_000_000_000_002),
            range_count: 3,
            range_min_seq: 11,
            range_max_seq: 42,
            range_min_key: Some(b"r-min".to_vec()),
            range_max_key: Some(b"r-max".to_vec()),
        }
    }

    fn meta_all_none() -> SstMeta {
        SstMeta {
            id: 1,
            level: 0,
            num_entries: 1,
            num_tombstones: 0,
            max_seq: 1,
            klog_size: 10,
            vlog_size: 0,
            min_key: Vec::new(),
            max_key: Vec::new(),
            partition: None,
            tier: None,
            max_entry_time: None,
            object: None,
            last_compaction_time: None,
            range_count: 0,
            range_min_seq: 0,
            range_max_seq: 0,
            range_min_key: None,
            range_max_key: None,
        }
    }

    fn all_ops() -> Vec<Op> {
        vec![
            Op::AddTable {
                cf: "a".into(),
                meta: meta_all_set(),
            },
            Op::RemoveTable {
                cf: "a".into(),
                id: 3,
                expected_level: 2,
            },
            Op::UpdateTable {
                cf: "a".into(),
                id: 4,
                update: TableUpdate {
                    level: Some(6),
                    tier: Some(Some("cold".into())),
                    object: Some(None),
                    partition: Some(Some("img".into())),
                    max_entry_time: Some(Some(42)),
                    last_compaction_time: Some(Some(43)),
                },
            },
            Op::CreateCf {
                name: "b".into(),
                config: vec![1, 2, 3],
            },
            Op::DropCf { name: "c".into() },
            Op::SetCfConfig {
                name: "a".into(),
                config: vec![4, 5],
            },
            Op::SetNextFileId(1234),
            Op::SetGlobalSeq(5678),
            Op::SetWalLayout(WalLayout::Unified),
            Op::SetNonce(0xFEED_FACE_DEAD_BEEF),
            Op::SetCapability(crate::format::CAP_MANIFEST_EDITS),
            Op::RemoveTables {
                cf: "a".into(),
                ids: vec![1, 2, 3],
            },
        ]
    }

    fn round_trip(ops: Vec<Op>) -> VersionEdit {
        let edit = VersionEdit::new(ops);
        let payload = encode_payload(77, &edit);
        let (id, back) = decode_payload(&payload).unwrap();
        assert_eq!(id, 77);
        back
    }

    #[test]
    fn every_op_round_trips() {
        let ops = all_ops();
        assert_eq!(round_trip(ops.clone()).ops, ops);
    }

    #[test]
    fn sst_meta_round_trips_with_all_options_set() {
        let ops = vec![Op::AddTable {
            cf: "cf".into(),
            meta: meta_all_set(),
        }];
        assert_eq!(round_trip(ops.clone()).ops, ops);
    }

    #[test]
    fn sst_meta_round_trips_with_all_options_none() {
        let ops = vec![Op::AddTable {
            cf: "cf".into(),
            meta: meta_all_none(),
        }];
        assert_eq!(round_trip(ops.clone()).ops, ops);
    }

    /// The mask's values are positional, so a decoder that reads them in any
    /// other order silently swaps two `Option<String>` fields.
    #[test]
    fn update_mask_values_decode_in_ascending_bit_order() {
        let update = TableUpdate {
            level: Some(2),
            tier: Some(Some("T".into())),
            object: Some(Some("O".into())),
            partition: Some(Some("P".into())),
            max_entry_time: Some(Some(9)),
            last_compaction_time: Some(Some(11)),
        };
        assert_eq!(update.mask(), 0x3F);
        let ops = vec![Op::UpdateTable {
            cf: "cf".into(),
            id: 1,
            update,
        }];
        let back = round_trip(ops.clone());
        assert_eq!(back.ops, ops);
        // Every single-bit subset also round-trips in isolation.
        for update in [
            TableUpdate {
                level: Some(3),
                ..Default::default()
            },
            TableUpdate {
                tier: Some(None),
                ..Default::default()
            },
            TableUpdate {
                object: Some(Some("o".into())),
                ..Default::default()
            },
            TableUpdate {
                partition: Some(None),
                ..Default::default()
            },
            TableUpdate {
                max_entry_time: Some(Some(-5)),
                ..Default::default()
            },
        ] {
            let ops = vec![Op::UpdateTable {
                cf: "cf".into(),
                id: 1,
                update,
            }];
            assert_eq!(round_trip(ops.clone()).ops, ops);
        }
    }

    #[test]
    fn unknown_op_code_is_corruption_naming_the_index() {
        for code in [0u64, 13, 63, 64, 1000] {
            let mut payload = Vec::new();
            append_u64(&mut payload, 5);
            append_uvarint(&mut payload, 2);
            encode_op(&mut payload, &Op::SetGlobalSeq(1));
            append_uvarint(&mut payload, code);
            let err = decode_payload(&payload).expect_err("an unassigned op code must fail closed");
            assert_eq!(err.kind(), "corruption");
            assert!(
                err.to_string().contains("op index 1"),
                "message must name the op index: {err}"
            );
        }
    }

    #[test]
    fn zero_update_mask_is_corruption() {
        let mut payload = Vec::new();
        append_u64(&mut payload, 1);
        append_uvarint(&mut payload, 1);
        append_uvarint(&mut payload, code::UPDATE_TABLE);
        append_bytes(&mut payload, b"cf");
        append_uvarint(&mut payload, 7);
        append_uvarint(&mut payload, 0);
        let err = decode_payload(&payload).expect_err("an empty update mask must fail closed");
        assert_eq!(err.kind(), "corruption");
        // A bit above the top assigned one is equally rejected.
        let mut payload = Vec::new();
        append_u64(&mut payload, 1);
        append_uvarint(&mut payload, 1);
        append_uvarint(&mut payload, code::UPDATE_TABLE);
        append_bytes(&mut payload, b"cf");
        append_uvarint(&mut payload, 7);
        append_uvarint(&mut payload, 0x20);
        assert_eq!(
            decode_payload(&payload).unwrap_err().kind(),
            "corruption",
            "a mask bit above 0x10 must fail closed"
        );
    }

    #[test]
    fn invalid_utf8_cf_name_is_corruption() {
        let mut payload = Vec::new();
        append_u64(&mut payload, 1);
        append_uvarint(&mut payload, 1);
        append_uvarint(&mut payload, code::DROP_CF);
        append_bytes(&mut payload, &[0xFF, 0xFE]);
        let err = decode_payload(&payload).expect_err("a non-UTF-8 name must fail closed");
        assert_eq!(err.kind(), "corruption");
        assert!(err.to_string().contains("UTF-8"), "{err}");
    }

    #[test]
    fn set_wal_layout_rejects_bytes_other_than_zero_and_one() {
        for byte in [2u8, 0xFF] {
            let mut payload = Vec::new();
            append_u64(&mut payload, 1);
            append_uvarint(&mut payload, 1);
            append_uvarint(&mut payload, code::SET_WAL_LAYOUT);
            payload.push(byte);
            let err = decode_payload(&payload).expect_err("only 0 and 1 are assigned");
            assert_eq!(err.kind(), "corruption");
        }
    }

    #[test]
    fn trailing_bytes_after_the_last_op_are_corruption() {
        let mut payload = encode_payload(3, &VersionEdit::new(vec![Op::SetGlobalSeq(1)]));
        payload.push(0);
        assert_eq!(decode_payload(&payload).unwrap_err().kind(), "corruption");
    }

    /// The op decoder is reachable from any byte string a corrupt or hostile
    /// file can hold, so it must be total: a `Result`, never a panic.
    #[test]
    fn op_decoder_never_panics_on_arbitrary_bytes() {
        let mut seeds: Vec<Vec<u8>> = vec![encode_payload(1, &VersionEdit::new(all_ops()))];
        for op in all_ops() {
            seeds.push(encode_payload(2, &VersionEdit::new(vec![op])));
        }
        seeds.push(Vec::new());
        let mut rng = crate::util::FuzzRng::new(0x9E37_79B9_7F4A_7C15);
        for seed in &seeds {
            for _ in 0..4000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                let _ = decode_payload(&case);
                // Also through the framing, where a lying length is the hazard.
                let mut framed = Vec::new();
                append_u32(&mut framed, case.len() as u32);
                append_u32(&mut framed, checksum(&case));
                framed.extend_from_slice(&case);
                let mut file = EditLogHeader {
                    base_applied_through: 0,
                    snapshot_generation: 1,
                }
                .encode();
                file.extend_from_slice(&framed);
                let _ = decode_records(&file);
            }
        }
    }

    // -- Apply / preconditions ------------------------------------------------

    fn seeded() -> Manifest {
        let mut m = Manifest::default();
        apply_edit(
            &mut m,
            &VersionEdit::new(vec![
                Op::CreateCf {
                    name: "cf".into(),
                    config: vec![1],
                },
                Op::AddTable {
                    cf: "cf".into(),
                    meta: SstMeta {
                        id: 1,
                        level: 2,
                        ..SstMeta::default()
                    },
                },
            ]),
        )
        .unwrap();
        m
    }

    fn reject(m: &mut Manifest, ops: Vec<Op>) -> OndaError {
        let before = format!("{m:?}");
        let err = apply_edit(m, &VersionEdit::new(ops)).expect_err("precondition must reject");
        assert_eq!(err.kind(), "corruption");
        assert_eq!(before, format!("{m:?}"), "a rejected edit must not mutate");
        err
    }

    #[test]
    fn add_table_rejects_a_duplicate_id() {
        let mut m = seeded();
        reject(
            &mut m,
            vec![Op::AddTable {
                cf: "cf".into(),
                meta: SstMeta {
                    id: 1,
                    ..SstMeta::default()
                },
            }],
        );
        // ... and an unknown column family.
        reject(
            &mut m,
            vec![Op::AddTable {
                cf: "nope".into(),
                meta: SstMeta {
                    id: 9,
                    ..SstMeta::default()
                },
            }],
        );
    }

    #[test]
    fn remove_table_rejects_a_wrong_level() {
        let mut m = seeded();
        reject(
            &mut m,
            vec![Op::RemoveTable {
                cf: "cf".into(),
                id: 1,
                expected_level: 5,
            }],
        );
        reject(
            &mut m,
            vec![Op::RemoveTable {
                cf: "cf".into(),
                id: 77,
                expected_level: 2,
            }],
        );
        apply_edit(
            &mut m,
            &VersionEdit::new(vec![Op::RemoveTable {
                cf: "cf".into(),
                id: 1,
                expected_level: 2,
            }]),
        )
        .expect("the right level removes");
        assert!(m.cfs[0].sstables.is_empty());
    }

    #[test]
    fn update_table_rejects_a_missing_id() {
        let mut m = seeded();
        reject(
            &mut m,
            vec![Op::UpdateTable {
                cf: "cf".into(),
                id: 42,
                update: TableUpdate {
                    level: Some(1),
                    ..Default::default()
                },
            }],
        );
    }

    #[test]
    fn create_cf_rejects_an_existing_name() {
        let mut m = seeded();
        reject(
            &mut m,
            vec![Op::CreateCf {
                name: "cf".into(),
                config: Vec::new(),
            }],
        );
    }

    /// A CF is dropped only once the same edit has removed every one of its
    /// tables — otherwise the drop would silently orphan them.
    #[test]
    fn drop_cf_rejects_leftover_tables_in_the_same_edit() {
        let mut m = seeded();
        reject(&mut m, vec![Op::DropCf { name: "cf".into() }]);
        apply_edit(
            &mut m,
            &VersionEdit::new(vec![
                Op::RemoveTables {
                    cf: "cf".into(),
                    ids: vec![1],
                },
                Op::DropCf { name: "cf".into() },
            ]),
        )
        .expect("drop after removing every table");
        assert!(m.cfs.is_empty());
        // And a second drop of a now-absent CF is rejected.
        reject(&mut m, vec![Op::DropCf { name: "cf".into() }]);
    }

    #[test]
    fn set_next_file_id_rejects_a_decrease() {
        let mut m = seeded();
        apply_edit(&mut m, &VersionEdit::new(vec![Op::SetNextFileId(50)])).unwrap();
        reject(&mut m, vec![Op::SetNextFileId(49)]);
        apply_edit(&mut m, &VersionEdit::new(vec![Op::SetNextFileId(50)]))
            .expect("equal is allowed");
    }

    #[test]
    fn set_global_seq_rejects_a_decrease() {
        let mut m = seeded();
        apply_edit(&mut m, &VersionEdit::new(vec![Op::SetGlobalSeq(9)])).unwrap();
        reject(&mut m, vec![Op::SetGlobalSeq(8)]);
    }

    #[test]
    fn set_wal_layout_is_one_way() {
        let mut m = seeded();
        apply_edit(
            &mut m,
            &VersionEdit::new(vec![Op::SetWalLayout(WalLayout::Unified)]),
        )
        .unwrap();
        assert_eq!(m.wal_layout, WalLayout::Unified);
        reject(&mut m, vec![Op::SetWalLayout(WalLayout::Unified)]);
        let mut fresh = seeded();
        reject(
            &mut fresh,
            vec![Op::SetWalLayout(WalLayout::PerColumnFamily)],
        );
    }

    #[test]
    fn set_nonce_rejects_a_second_mint() {
        let mut m = seeded();
        apply_edit(&mut m, &VersionEdit::new(vec![Op::SetNonce(7)])).unwrap();
        reject(&mut m, vec![Op::SetNonce(7)]);
        reject(&mut m, vec![Op::SetNonce(8)]);
    }

    #[test]
    fn set_capability_rejects_unknown_bits() {
        let mut m = seeded();
        let err = apply_edit(&mut m, &VersionEdit::new(vec![Op::SetCapability(1 << 40)]))
            .expect_err("an unimplemented capability bit must fail closed");
        assert_eq!(err.kind(), "unsupported_format");
        assert_eq!(m.caps, 0, "a rejected edit must not mutate");
        apply_edit(
            &mut m,
            &VersionEdit::new(vec![Op::SetCapability(crate::format::CAP_MANIFEST_EDITS)]),
        )
        .unwrap();
        assert_eq!(m.caps, crate::format::CAP_MANIFEST_EDITS);
    }

    #[test]
    fn remove_tables_rejects_an_absent_id() {
        let mut m = seeded();
        reject(
            &mut m,
            vec![Op::RemoveTables {
                cf: "cf".into(),
                ids: vec![1, 2],
            }],
        );
        assert_eq!(m.cfs[0].sstables.len(), 1, "the present id stays too");
    }

    /// The all-or-nothing guarantee, stated once on its own: a failure in the
    /// middle of a multi-op edit leaves nothing behind.
    #[test]
    fn a_failed_precondition_leaves_the_manifest_untouched() {
        let mut m = seeded();
        reject(
            &mut m,
            vec![
                Op::SetGlobalSeq(100),
                Op::AddTable {
                    cf: "cf".into(),
                    meta: SstMeta {
                        id: 2,
                        ..SstMeta::default()
                    },
                },
                Op::CreateCf {
                    name: "cf".into(), // already exists: rejects the whole edit
                    config: Vec::new(),
                },
            ],
        );
        assert_eq!(m.global_seq, 0);
        assert_eq!(m.cfs[0].sstables.len(), 1);
    }

    /// A CF created and populated by the same edit is the `clone_column_family`
    /// shape; it must validate against the candidate, not the old catalog.
    #[test]
    fn a_cf_created_and_populated_in_one_edit_applies() {
        let mut m = seeded();
        apply_edit(
            &mut m,
            &VersionEdit::new(vec![
                Op::CreateCf {
                    name: "clone".into(),
                    config: vec![7],
                },
                Op::AddTable {
                    cf: "clone".into(),
                    meta: SstMeta {
                        id: 20,
                        level: 1,
                        ..SstMeta::default()
                    },
                },
                Op::AddTable {
                    cf: "clone".into(),
                    meta: SstMeta {
                        id: 21,
                        level: 1,
                        ..SstMeta::default()
                    },
                },
            ]),
        )
        .unwrap();
        let clone = m.cfs.iter().find(|c| c.name == "clone").unwrap();
        assert_eq!(clone.sstables.len(), 2);
        assert_eq!(clone.config, vec![7]);
    }

    /// The replayer is an optimization of "apply these edits in order", so it
    /// must produce byte-identical catalogs. If the two ever diverge, recovery
    /// and `catalog_txn` disagree about what a log means.
    #[test]
    fn the_replayer_matches_repeated_apply_edit() {
        let edits: Vec<VersionEdit> = vec![
            VersionEdit::new(vec![Op::CreateCf {
                name: "b".into(),
                config: vec![9],
            }]),
            VersionEdit::new(vec![Op::AddTable {
                cf: "b".into(),
                meta: SstMeta {
                    id: 10,
                    level: 1,
                    ..SstMeta::default()
                },
            }]),
            VersionEdit::new(vec![Op::UpdateTable {
                cf: "cf".into(),
                id: 1,
                update: TableUpdate::relocation(Some("cold".into()), Some("o".into())),
            }]),
            VersionEdit::new(vec![Op::SetGlobalSeq(400), Op::SetNextFileId(99)]),
            VersionEdit::new(vec![
                Op::RemoveTable {
                    cf: "cf".into(),
                    id: 1,
                    expected_level: 2,
                },
                Op::DropCf { name: "cf".into() },
            ]),
        ];
        let mut one_by_one = seeded();
        for e in &edits {
            apply_edit(&mut one_by_one, e).unwrap();
        }
        let mut replayer = CatalogReplayer::new(seeded());
        for e in &edits {
            replayer.apply(e).unwrap();
        }
        assert_eq!(
            format!("{:?}", replayer.finish()),
            format!("{one_by_one:?}")
        );
    }

    /// The replayer enforces the same preconditions; it only shares the index.
    #[test]
    fn the_replayer_rejects_what_apply_edit_rejects() {
        let mut replayer = CatalogReplayer::new(seeded());
        let dup = VersionEdit::new(vec![Op::AddTable {
            cf: "cf".into(),
            meta: SstMeta {
                id: 1,
                ..SstMeta::default()
            },
        }]);
        assert_eq!(replayer.apply(&dup).unwrap_err().kind(), "corruption");
        assert_eq!(
            apply_edit(&mut seeded(), &dup).unwrap_err().kind(),
            "corruption"
        );
    }

    // -- Log codec ------------------------------------------------------------

    #[test]
    fn header_bytes_are_frozen() {
        let h = EditLogHeader {
            base_applied_through: 0x0102_0304_0506_0708,
            snapshot_generation: 0x1112_1314_1516_1718,
        };
        let bytes = h.encode();
        assert_eq!(bytes.len(), 32);
        assert_eq!(&bytes[0..8], b"YOLODBED");
        assert_eq!(&bytes[8..12], &[1, 0, 0, 0]);
        assert_eq!(&bytes[12..20], &[8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(
            &bytes[20..28],
            &[0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11]
        );
        assert_eq!(read_u32(&bytes[28..32]), checksum(&bytes[..28]));
        assert_eq!(EditLogHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn record_bytes_are_frozen() {
        let edit = VersionEdit::new(vec![Op::SetGlobalSeq(300)]);
        let rec = encode_record(0x0A0B_0C0D_0E0F_1011, &edit);
        // payload: edit id (8 LE) | op count (uvarint 1) | code 8 | uvarint 300
        let payload = &rec[8..];
        assert_eq!(
            payload,
            &[
                0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, // edit id
                0x01, // one op
                0x08, // SetGlobalSeq
                0xAC, 0x02, // 300 as LEB128
            ]
        );
        assert_eq!(read_u32(&rec[0..4]) as usize, payload.len());
        assert_eq!(read_u32(&rec[4..8]), checksum(payload));
    }

    fn log_with(records: &[(u64, VersionEdit)]) -> Vec<u8> {
        let mut file = EditLogHeader {
            base_applied_through: 0,
            snapshot_generation: 1,
        }
        .encode();
        for (id, edit) in records {
            file.extend_from_slice(&encode_record(*id, edit));
        }
        file
    }

    fn sample_records() -> Vec<(u64, VersionEdit)> {
        vec![
            (1, VersionEdit::new(vec![Op::SetGlobalSeq(1)])),
            (2, VersionEdit::new(vec![Op::SetNextFileId(9)])),
        ]
    }

    #[test]
    fn short_file_is_corruption() {
        for len in [0usize, 1, 27, 31] {
            let err = EditLogHeader::decode(&vec![0u8; len])
                .expect_err("a file shorter than the header is never a torn tail");
            assert_eq!(err.kind(), "corruption");
        }
    }

    #[test]
    fn bad_header_crc_is_corruption() {
        let mut file = log_with(&sample_records());
        file[14] ^= 0xFF;
        assert_eq!(
            EditLogHeader::decode(&file).unwrap_err().kind(),
            "corruption"
        );
    }

    #[test]
    fn unknown_magic_is_corruption() {
        let mut head = EditLogHeader {
            base_applied_through: 3,
            snapshot_generation: 1,
        }
        .encode();
        head[0] = b'W'; // the wavesdb namespace, deliberately rejected
        let crc = checksum(&head[..28]);
        crate::encoding::put_u32(&mut head[28..32], crc);
        let err = EditLogHeader::decode(&head).expect_err("a foreign magic must fail closed");
        assert_eq!(err.kind(), "corruption");
        // A 0.9 log is a named refusal.
        head[..4].copy_from_slice(&ONDA09_EDIT_LOG_MAGIC.to_le_bytes());
        assert_eq!(
            EditLogHeader::decode(&head).unwrap_err().kind(),
            "unsupported_format"
        );
    }

    #[test]
    fn unknown_schema_is_unsupported_format() {
        let mut head = EditLogHeader {
            base_applied_through: 3,
            snapshot_generation: 1,
        }
        .encode();
        head[8] = 2;
        let crc = checksum(&head[..28]);
        crate::encoding::put_u32(&mut head[28..32], crc);
        assert_eq!(
            EditLogHeader::decode(&head).unwrap_err().kind(),
            "unsupported_format"
        );
    }

    #[test]
    fn truncated_record_header_is_a_clean_tail() {
        let full = log_with(&sample_records());
        for cut in 1..EDIT_RECORD_HEADER_BYTES {
            let mut file = full.clone();
            file.truncate(full.len() - encoded_len(&sample_records()[1]) + cut);
            let (records, _) = decode_records(&file).expect("an EOF-truncated frame ends replay");
            assert_eq!(records.len(), 1, "cut={cut}");
        }
    }

    fn encoded_len(rec: &(u64, VersionEdit)) -> usize {
        encode_record(rec.0, &rec.1).len()
    }

    #[test]
    fn truncated_record_payload_is_a_clean_tail() {
        let full = log_with(&sample_records());
        let last = encoded_len(&sample_records()[1]);
        for cut in EDIT_RECORD_HEADER_BYTES..last {
            let mut file = full.clone();
            file.truncate(full.len() - last + cut);
            let (records, off) = decode_records(&file).expect("a partial payload ends replay");
            assert_eq!(records.len(), 1, "cut={cut}");
            assert_eq!(
                off,
                full.len() - last,
                "append must overwrite the torn tail"
            );
        }
    }

    #[test]
    fn complete_record_with_bad_crc_is_corruption() {
        let mut file = log_with(&sample_records());
        let last = file.len() - 1;
        file[last] ^= 0xFF; // inside the final payload
        let err = decode_records(&file)
            .expect_err("a complete record with a bad CRC is never a torn tail");
        assert_eq!(err.kind(), "corruption");
        assert!(err.to_string().contains("CRC"), "{err}");
    }

    #[test]
    fn oversized_record_length_is_corruption_before_allocation() {
        let mut file = EditLogHeader {
            base_applied_through: 0,
            snapshot_generation: 1,
        }
        .encode();
        append_u32(&mut file, u32::MAX); // > 64 MiB, and nothing follows it
        append_u32(&mut file, 0);
        let err = decode_records(&file).expect_err("an oversized length must fail closed");
        assert_eq!(err.kind(), "corruption");
        assert!(err.to_string().contains("cap"), "{err}");
    }

    #[test]
    fn records_round_trip_through_the_log() {
        let recs = sample_records();
        let (back, off) = decode_records(&log_with(&recs)).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].edit_id, 1);
        assert_eq!(back[1].edit, recs[1].1);
        assert_eq!(off, log_with(&recs).len());
    }
}
