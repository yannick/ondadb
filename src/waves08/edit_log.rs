//! wavesdb's epoch-1 `MANIFEST-EDITS`: the `YOLODBED` header with **schema
//! 2**, and wavesdb 0.8's edit payloads.
//!
//! ```text
//! header (32 bytes)  magic "YOLODBED" | schema u32 = 2 | base_applied_through u64
//!                    | snapshot_generation u64 | crc32c u32 over bytes 0..28
//! record             len u32 (edit_id + payload, 8..=64 MiB) | crc32c u32 over the
//!                    same bytes | edit_id u64 | payload
//! payload            op_count uvarint | (kind u8 | body)*
//! ```
//!
//! Op kinds (wavesdb `OpKind`, pinned by its `TestEditGolden`): 1 AddTable,
//! 2 RemoveTable, 3 UpdateTable, 4 AddBlob, 5 UpdateBlob, 6 RemoveBlob,
//! 7 CreateCF, 8 DropCF, 9 RenameCF, 10 SetCFConfig, 11 SetNextFileID,
//! 12 SetGlobalSeq, 13 SetWALLayout, 14 SetNonce, 15 SetCapability,
//! 16 SetManagedState; 17 and 18 are reserved and refused, like any other kind.
//! The same numbers mean different ops in ondaDB's schema 1, which is why the
//! schema, not the op byte, decides the table.
//!
//! **Canonical payloads.** wavesdb refuses a payload that does not re-encode
//! to its own bytes. This decoder holds the same line field by field: every
//! uvarint minimal, blob references strictly ascending, and every field a
//! table's flag word announces non-empty (the flag word is derived from the
//! values, so an announced empty field is an encoding no writer makes). Those
//! are exactly the places wavesdb's encoder has a choice to make.
//!
//! **Recovery rules** (wavesdb `ReadEditLog` and `Recover`): a short header,
//! a foreign magic or a bad header CRC is `Corruption` and a schema other than
//! 2 is `UnsupportedFormat`; an EOF inside a record header or body is a torn
//! tail that ends replay cleanly, while a *complete* record failing its CRC,
//! or a length outside `8..=64 MiB`, is `Corruption`; ids are non-zero and
//! strictly increasing; records at or below the snapshot's applied-through
//! are skipped, the rest must be contiguous; each op's precondition must hold
//! against the catalog it is applied to; and a replayed catalog may not name a
//! file at or above `next_file_id` nor a table above `global_seq`.

use std::collections::BTreeMap;

use super::manifest::blob_refs;
use super::{check_dependencies, corrupt, Blob, Catalog, Family, Reader, Table, KNOWN_CAPS};
use crate::encoding::{checksum, read_u32, read_u64};
use crate::error::{OndaError, Result};
use crate::format::edit_log::{HEADER_BYTES, MAGIC, SCHEMA_WAVES08};
use crate::format::wavesdb_reserved::CAP_WAVES08_BODIES;
use crate::format::{CAP_MANIFEST_EDITS, CAP_PERIODIC_AGE};
use crate::manifest::{SstMeta, WalLayout};

/// wavesdb's `MaxEditBytes`: the cap on a record's `len` (id included).
pub const MAX_EDIT_BYTES: u32 = 64 << 20;
const RECORD_HEADER: usize = 8;

/// The decoded header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub base_applied_through: u64,
    pub snapshot_generation: u64,
}

/// Op kinds.
pub mod op {
    pub const ADD_TABLE: u8 = 1;
    pub const REMOVE_TABLE: u8 = 2;
    pub const UPDATE_TABLE: u8 = 3;
    pub const ADD_BLOB: u8 = 4;
    pub const UPDATE_BLOB: u8 = 5;
    pub const REMOVE_BLOB: u8 = 6;
    pub const CREATE_CF: u8 = 7;
    pub const DROP_CF: u8 = 8;
    pub const RENAME_CF: u8 = 9;
    pub const SET_CF_CONFIG: u8 = 10;
    pub const SET_NEXT_FILE_ID: u8 = 11;
    pub const SET_GLOBAL_SEQ: u8 = 12;
    pub const SET_WAL_LAYOUT: u8 = 13;
    pub const SET_NONCE: u8 = 14;
    pub const SET_CAPABILITY: u8 = 15;
    pub const SET_MANAGED_STATE: u8 = 16;
}

/// UpdateTable field mask (wavesdb `Field*`), in encoding order.
pub mod field {
    pub const LEVEL: u64 = 1 << 0;
    pub const NUM_ENTRIES: u64 = 1 << 1;
    pub const NUM_TOMBSTONES: u64 = 1 << 2;
    pub const MAX_SEQ: u64 = 1 << 3;
    pub const KLOG_SIZE: u64 = 1 << 4;
    pub const VLOG_SIZE: u64 = 1 << 5;
    pub const MIN_KEY: u64 = 1 << 6;
    pub const MAX_KEY: u64 = 1 << 7;
    pub const BLOB_BYTES: u64 = 1 << 8;
    pub const PARTITION: u64 = 1 << 9;
    pub const TIER: u64 = 1 << 10;
    pub const OBJECT: u64 = 1 << 11;
    pub const MAX_ENTRY_TIME: u64 = 1 << 12;
    pub const CAPS: u64 = 1 << 13;
    pub const LAST_COMPACTION_TIME: u64 = 1 << 14;
    pub const MERGE_COUNT: u64 = 1 << 15;
    pub const MERGE_CHAIN_MAX: u64 = 1 << 16;
    pub const RANGE_COUNT: u64 = 1 << 17;
    pub const RANGE_MIN_SEQ: u64 = 1 << 18;
    pub const RANGE_MAX_SEQ: u64 = 1 << 19;
    pub const RANGE_MIN_KEY: u64 = 1 << 20;
    pub const RANGE_MAX_KEY: u64 = 1 << 21;
    pub const TABLE_KNOWN: u64 = (1 << 22) - 1;

    /// UpdateBlob's mask.
    pub const SIZE: u64 = 1 << 0;
    pub const LIVE_BYTES: u64 = 1 << 1;
    pub const BLOB_KNOWN: u64 = SIZE | LIVE_BYTES;

    /// CreateCF / SetCFConfig's mask.
    pub const CONFIG: u64 = 1 << 0;
    pub const UNIFIED_ID: u64 = 1 << 1;
    pub const CF_KNOWN: u64 = CONFIG | UNIFIED_ID;

    /// SetManagedState's mask.
    pub const DISCARD_SEQ: u64 = 1 << 0;
    pub const LAST_MANAGED_COMMIT: u64 = 1 << 1;
    pub const MANAGED_KNOWN: u64 = DISCARD_SEQ | LAST_MANAGED_COMMIT;
}

/// The flag word of a whole table inside AddTable (wavesdb `sstFlag*`). Not
/// the snapshot's section bits: the edit log kept 0.8's order.
mod sst_flag {
    pub const PARTITION: u64 = 1 << 0;
    pub const TIER: u64 = 1 << 1;
    pub const OBJECT: u64 = 1 << 2;
    pub const MAX_ENTRY_TIME: u64 = 1 << 3;
    pub const CAPS: u64 = 1 << 4;
    pub const LAST_COMPACTION_TIME: u64 = 1 << 5;
    pub const MERGE: u64 = 1 << 6;
    pub const RANGE: u64 = 1 << 7;
    pub const KNOWN: u64 = (1 << 8) - 1;
}

/// A table as an edit carries it: wavesdb's flat field set, where an empty
/// value means absent. Converted to a [`Table`] when applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EditTable {
    pub id: u64,
    pub level: u32,
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub max_seq: u64,
    pub klog_size: u64,
    pub vlog_size: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub blob_bytes: BTreeMap<u64, u64>,
    pub partition: String,
    pub tier: String,
    pub object: String,
    pub max_entry_time: i64,
    pub caps: u64,
    pub last_compaction_time: i64,
    pub merge_count: u64,
    pub merge_chain_max: u64,
    pub range_count: u64,
    pub range_min_seq: u64,
    pub range_max_seq: u64,
    pub range_min_key: Vec<u8>,
    pub range_max_key: Vec<u8>,
}

impl EditTable {
    fn from_table(t: &Table) -> EditTable {
        let m = &t.meta;
        EditTable {
            id: m.id,
            level: m.level,
            num_entries: m.num_entries,
            num_tombstones: m.num_tombstones,
            max_seq: m.max_seq,
            klog_size: m.klog_size,
            vlog_size: m.vlog_size,
            min_key: m.min_key.clone(),
            max_key: m.max_key.clone(),
            blob_bytes: t.blob_bytes.clone(),
            partition: m.partition.clone().unwrap_or_default(),
            tier: m.tier.clone().unwrap_or_default(),
            object: m.object.clone().unwrap_or_default(),
            max_entry_time: m.max_entry_time.unwrap_or(0),
            caps: m.table_caps.unwrap_or(0),
            last_compaction_time: m.last_compaction_time.unwrap_or(0),
            merge_count: t.merge_count,
            merge_chain_max: t.merge_chain_max,
            range_count: m.range_count,
            range_min_seq: m.range_min_seq,
            range_max_seq: m.range_max_seq,
            range_min_key: m.range_min_key.clone().unwrap_or_default(),
            range_max_key: m.range_max_key.clone().unwrap_or_default(),
        }
    }

    fn to_table(&self) -> Table {
        let some_str = |s: &String| (!s.is_empty()).then(|| s.clone());
        let some_key = |k: &Vec<u8>| (!k.is_empty()).then(|| k.clone());
        Table {
            meta: SstMeta {
                id: self.id,
                level: self.level,
                num_entries: self.num_entries,
                num_tombstones: self.num_tombstones,
                max_seq: self.max_seq,
                klog_size: self.klog_size,
                vlog_size: self.vlog_size,
                min_key: self.min_key.clone(),
                max_key: self.max_key.clone(),
                partition: some_str(&self.partition),
                tier: some_str(&self.tier),
                max_entry_time: (self.max_entry_time != 0).then_some(self.max_entry_time),
                object: some_str(&self.object),
                last_compaction_time: (self.last_compaction_time != 0)
                    .then_some(self.last_compaction_time),
                range_count: self.range_count,
                range_min_seq: self.range_min_seq,
                range_max_seq: self.range_max_seq,
                range_min_key: some_key(&self.range_min_key),
                range_max_key: some_key(&self.range_max_key),
                table_caps: (self.caps != 0).then_some(self.caps),
            },
            merge_count: self.merge_count,
            merge_chain_max: self.merge_chain_max,
            blob_bytes: self.blob_bytes.clone(),
        }
    }
}

/// One decoded op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    AddTable { cf: String, table: EditTable },
    RemoveTable { cf: String, id: u64, level: u32 },
    UpdateTable { cf: String, id: u64, mask: u64, table: EditTable },
    AddBlob { cf: String, blob: Blob },
    UpdateBlob { cf: String, id: u64, mask: u64, size: u64, live_bytes: u64 },
    RemoveBlob { cf: String, id: u64 },
    CreateCf { cf: String, mask: u64, config: Vec<u8>, unified_id: u64 },
    DropCf { cf: String },
    RenameCf { cf: String, new_name: String },
    SetCfConfig { cf: String, mask: u64, config: Vec<u8>, unified_id: u64 },
    SetNextFileId(u64),
    SetGlobalSeq(u64),
    SetWalLayout(u64),
    SetNonce(u64),
    SetCapability(u64),
    SetManagedState { mask: u64, discard_seq: u64, last_managed_commit: u64 },
}

/// One complete, CRC-valid record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub edit_id: u64,
    pub ops: Vec<Op>,
}

/// What [`read`] found after the header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contents {
    pub header: Header,
    pub records: Vec<Record>,
    /// Whether the log ends in an incomplete record (a crash mid-append).
    pub torn: bool,
}

/// Decode the 32-byte header.
pub fn decode_header(data: &[u8]) -> Result<Header> {
    if data.len() < HEADER_BYTES {
        return Err(corrupt(format!(
            "edit log header truncated ({} bytes)",
            data.len()
        )));
    }
    if data[..8] != MAGIC {
        return Err(corrupt("edit log magic is not YOLODBED"));
    }
    if read_u32(&data[28..32]) != checksum(&data[..28]) {
        return Err(corrupt("edit log header checksum"));
    }
    let schema = read_u32(&data[8..12]);
    if schema != SCHEMA_WAVES08 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb edit log schema {schema} (expected {SCHEMA_WAVES08})"
        )));
    }
    Ok(Header {
        base_applied_through: read_u64(&data[12..20]),
        snapshot_generation: read_u64(&data[20..28]),
    })
}

/// Read the whole log: the header and every complete record.
pub fn read(data: &[u8]) -> Result<Contents> {
    let header = decode_header(data)?;
    let mut off = HEADER_BYTES;
    let mut records: Vec<Record> = Vec::new();
    let mut torn = false;
    while off < data.len() {
        let p = &data[off..];
        if p.len() < RECORD_HEADER {
            torn = true;
            break;
        }
        let len = read_u32(p);
        if !(8..=MAX_EDIT_BYTES).contains(&len) {
            return Err(corrupt(format!(
                "edit record length {len} at offset {off}"
            )));
        }
        let len = len as usize;
        if p.len() - RECORD_HEADER < len {
            torn = true;
            break;
        }
        let body = &p[RECORD_HEADER..RECORD_HEADER + len];
        if checksum(body) != read_u32(&p[4..]) {
            return Err(corrupt(format!("edit record checksum at offset {off}")));
        }
        let edit_id = read_u64(body);
        let ops = decode_payload(&body[8..]).map_err(|e| {
            super::prefix(e, &format!("edit {edit_id} at offset {off}"))
        })?;
        if edit_id == 0 || records.last().is_some_and(|r| edit_id <= r.edit_id) {
            return Err(corrupt(format!(
                "edit id {edit_id} at offset {off} does not increase"
            )));
        }
        records.push(Record { edit_id, ops });
        off += RECORD_HEADER + len;
    }
    Ok(Contents {
        header,
        records,
        torn,
    })
}

/// Decode one payload: `op_count | op*`, every byte consumed.
pub fn decode_payload(p: &[u8]) -> Result<Vec<Op>> {
    let mut r = Reader::new(p, true);
    let n = r.count("op count")?;
    let mut ops = Vec::with_capacity(n as usize);
    for _ in 0..n {
        ops.push(decode_op(&mut r)?);
    }
    if !r.p.is_empty() {
        return Err(corrupt(format!(
            "{} trailing bytes after the last op",
            r.p.len()
        )));
    }
    Ok(ops)
}

fn decode_op(r: &mut Reader<'_>) -> Result<Op> {
    let kind = r.take(1, "op kind")?[0];
    let check_mask = |mask: u64, known: u64, allow_zero: bool| -> Result<u64> {
        if (mask == 0 && !allow_zero) || mask & !known != 0 {
            return Err(corrupt(format!("op {kind} field mask {mask:#x}")));
        }
        Ok(mask)
    };
    Ok(match kind {
        op::ADD_TABLE => Op::AddTable {
            cf: r.string("cf")?,
            table: decode_table(r)?,
        },
        op::REMOVE_TABLE => Op::RemoveTable {
            cf: r.string("cf")?,
            id: r.uvar("table id")?,
            level: r.level()?,
        },
        op::UPDATE_TABLE => {
            let cf = r.string("cf")?;
            let id = r.uvar("table id")?;
            let mask = check_mask(r.uvar("field mask")?, field::TABLE_KNOWN, false)?;
            let table = decode_masked_table(r, mask)?;
            Op::UpdateTable { cf, id, mask, table }
        }
        op::ADD_BLOB => Op::AddBlob {
            cf: r.string("cf")?,
            blob: Blob {
                id: r.uvar("blob id")?,
                size: r.uvar("blob size")?,
                live_bytes: r.uvar("blob live bytes")?,
            },
        },
        op::UPDATE_BLOB => {
            let cf = r.string("cf")?;
            let id = r.uvar("blob id")?;
            let mask = check_mask(r.uvar("field mask")?, field::BLOB_KNOWN, false)?;
            let size = if mask & field::SIZE != 0 { r.uvar("blob size")? } else { 0 };
            let live_bytes = if mask & field::LIVE_BYTES != 0 {
                r.uvar("blob live bytes")?
            } else {
                0
            };
            Op::UpdateBlob { cf, id, mask, size, live_bytes }
        }
        op::REMOVE_BLOB => Op::RemoveBlob {
            cf: r.string("cf")?,
            id: r.uvar("blob id")?,
        },
        op::CREATE_CF | op::SET_CF_CONFIG => {
            let cf = r.string("cf")?;
            let create = kind == op::CREATE_CF;
            // CreateCF always carries a config, so its mask may be zero.
            let mask = check_mask(r.uvar("field mask")?, field::CF_KNOWN, create)?;
            let config = if create || mask & field::CONFIG != 0 {
                r.bytes("config")?
            } else {
                Vec::new()
            };
            let unified_id = if mask & field::UNIFIED_ID != 0 {
                r.uvar("unified id")?
            } else {
                0
            };
            if create {
                Op::CreateCf { cf, mask, config, unified_id }
            } else {
                Op::SetCfConfig { cf, mask, config, unified_id }
            }
        }
        op::DROP_CF => Op::DropCf { cf: r.string("cf")? },
        op::RENAME_CF => Op::RenameCf {
            cf: r.string("cf")?,
            new_name: r.string("new name")?,
        },
        op::SET_NEXT_FILE_ID => Op::SetNextFileId(r.uvar("next file id")?),
        op::SET_GLOBAL_SEQ => Op::SetGlobalSeq(r.uvar("global sequence")?),
        op::SET_WAL_LAYOUT => Op::SetWalLayout(r.uvar("WAL layout")?),
        op::SET_NONCE => Op::SetNonce(r.uvar("nonce")?),
        op::SET_CAPABILITY => Op::SetCapability(r.uvar("capability bits")?),
        op::SET_MANAGED_STATE => {
            let mask = check_mask(r.uvar("field mask")?, field::MANAGED_KNOWN, false)?;
            let discard_seq = if mask & field::DISCARD_SEQ != 0 {
                r.uvar("discard sequence")?
            } else {
                0
            };
            let last_managed_commit = if mask & field::LAST_MANAGED_COMMIT != 0 {
                r.uvar("last managed commit")?
            } else {
                0
            };
            Op::SetManagedState { mask, discard_seq, last_managed_commit }
        }
        // 17 and 18 are reserved for later wavesdb features; never skip an op
        // whose meaning is unknown.
        other => return Err(corrupt(format!("edit op kind {other} is not known"))),
    })
}

/// A capability word as a table carries it: an unknown bit is a newer format
/// and a dependency violation is corruption; zero is legal only where `zero`
/// says (an UpdateTable may set a table back to "no capabilities").
fn decode_caps(r: &mut Reader<'_>, zero: bool) -> Result<u64> {
    let c = r.uvar("table capability word")?;
    if c == 0 && !zero {
        return Err(corrupt("a table announces a zero capability word"));
    }
    if c & !KNOWN_CAPS != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb table capability word {c:#x} outside known mask {KNOWN_CAPS:#x}"
        )));
    }
    check_dependencies(c)?;
    Ok(c)
}

/// A whole table (AddTable).
fn decode_table(r: &mut Reader<'_>) -> Result<EditTable> {
    let mut t = EditTable {
        id: r.uvar("table id")?,
        level: r.level()?,
        num_entries: r.uvar("entry count")?,
        num_tombstones: r.uvar("tombstone count")?,
        max_seq: r.uvar("max sequence")?,
        klog_size: r.uvar("klog size")?,
        vlog_size: r.uvar("vlog size")?,
        min_key: r.bytes("min key")?,
        max_key: r.bytes("max key")?,
        blob_bytes: blob_refs(r, 0)?,
        ..EditTable::default()
    };
    let id = t.id;
    let flags = r.uvar("table flags")?;
    if flags & !sst_flag::KNOWN != 0 {
        return Err(corrupt(format!("table {id} flags {flags:#x}")));
    }
    // The flag word is derived from which fields are non-empty, so an
    // announced field decoding empty is an encoding no writer makes.
    let empty = |what: &str| corrupt(format!("table {id} announces {what} but carries none"));
    if flags & sst_flag::PARTITION != 0 {
        t.partition = r.string("partition")?;
        if t.partition.is_empty() {
            return Err(empty("a partition"));
        }
    }
    if flags & sst_flag::TIER != 0 {
        t.tier = r.string("tier")?;
        if t.tier.is_empty() {
            return Err(empty("a tier"));
        }
    }
    if flags & sst_flag::OBJECT != 0 {
        t.object = r.string("object")?;
        if t.object.is_empty() {
            return Err(empty("an object key"));
        }
    }
    if flags & sst_flag::MAX_ENTRY_TIME != 0 {
        t.max_entry_time = r.uvar("max entry time")? as i64;
        if t.max_entry_time == 0 {
            return Err(empty("a max entry time"));
        }
    }
    if flags & sst_flag::CAPS != 0 {
        t.caps = decode_caps(r, false)?;
    }
    if flags & sst_flag::LAST_COMPACTION_TIME != 0 {
        t.last_compaction_time = r.uvar("last compaction time")? as i64;
        if t.last_compaction_time == 0 {
            return Err(empty("a last compaction time"));
        }
    }
    if flags & sst_flag::MERGE != 0 {
        t.merge_count = r.uvar("merge count")?;
        t.merge_chain_max = r.uvar("merge chain")?;
        if t.merge_count == 0 && t.merge_chain_max == 0 {
            return Err(empty("merge counters"));
        }
    }
    if flags & sst_flag::RANGE != 0 {
        t.range_count = r.uvar("range count")?;
        t.range_min_seq = r.uvar("range min sequence")?;
        t.range_max_seq = r.uvar("range max sequence")?;
        t.range_min_key = r.bytes("range min key")?;
        t.range_max_key = r.bytes("range max key")?;
        if t.range_count == 0
            || t.range_max_seq < t.range_min_seq
            || t.range_min_key.is_empty()
            || t.range_max_key.is_empty()
        {
            return Err(corrupt(format!(
                "table {id} announces an incoherent range summary"
            )));
        }
    }
    Ok(t)
}

/// The fields an UpdateTable's mask selects, in mask-bit order.
fn decode_masked_table(r: &mut Reader<'_>, mask: u64) -> Result<EditTable> {
    let mut t = EditTable::default();
    let has = |bit: u64| mask & bit != 0;
    if has(field::LEVEL) {
        t.level = r.level()?;
    }
    if has(field::NUM_ENTRIES) {
        t.num_entries = r.uvar("entry count")?;
    }
    if has(field::NUM_TOMBSTONES) {
        t.num_tombstones = r.uvar("tombstone count")?;
    }
    if has(field::MAX_SEQ) {
        t.max_seq = r.uvar("max sequence")?;
    }
    if has(field::KLOG_SIZE) {
        t.klog_size = r.uvar("klog size")?;
    }
    if has(field::VLOG_SIZE) {
        t.vlog_size = r.uvar("vlog size")?;
    }
    if has(field::MIN_KEY) {
        t.min_key = r.bytes("min key")?;
    }
    if has(field::MAX_KEY) {
        t.max_key = r.bytes("max key")?;
    }
    if has(field::BLOB_BYTES) {
        t.blob_bytes = blob_refs(r, 0)?;
    }
    if has(field::PARTITION) {
        t.partition = r.string("partition")?;
    }
    if has(field::TIER) {
        t.tier = r.string("tier")?;
    }
    if has(field::OBJECT) {
        t.object = r.string("object")?;
    }
    if has(field::MAX_ENTRY_TIME) {
        t.max_entry_time = r.uvar("max entry time")? as i64;
    }
    if has(field::CAPS) {
        t.caps = decode_caps(r, true)?;
    }
    if has(field::LAST_COMPACTION_TIME) {
        t.last_compaction_time = r.uvar("last compaction time")? as i64;
    }
    if has(field::MERGE_COUNT) {
        t.merge_count = r.uvar("merge count")?;
    }
    if has(field::MERGE_CHAIN_MAX) {
        t.merge_chain_max = r.uvar("merge chain")?;
    }
    if has(field::RANGE_COUNT) {
        t.range_count = r.uvar("range count")?;
    }
    if has(field::RANGE_MIN_SEQ) {
        t.range_min_seq = r.uvar("range min sequence")?;
    }
    if has(field::RANGE_MAX_SEQ) {
        t.range_max_seq = r.uvar("range max sequence")?;
    }
    if has(field::RANGE_MIN_KEY) {
        t.range_min_key = r.bytes("range min key")?;
    }
    if has(field::RANGE_MAX_KEY) {
        t.range_max_key = r.bytes("range max key")?;
    }
    Ok(t)
}

/// Copy the masked fields of `src` onto `dst` (wavesdb `applyMaskedTable`).
fn apply_mask(dst: &mut EditTable, mask: u64, src: &EditTable) {
    let has = |bit: u64| mask & bit != 0;
    if has(field::LEVEL) {
        dst.level = src.level;
    }
    if has(field::NUM_ENTRIES) {
        dst.num_entries = src.num_entries;
    }
    if has(field::NUM_TOMBSTONES) {
        dst.num_tombstones = src.num_tombstones;
    }
    if has(field::MAX_SEQ) {
        dst.max_seq = src.max_seq;
    }
    if has(field::KLOG_SIZE) {
        dst.klog_size = src.klog_size;
    }
    if has(field::VLOG_SIZE) {
        dst.vlog_size = src.vlog_size;
    }
    if has(field::MIN_KEY) {
        dst.min_key = src.min_key.clone();
    }
    if has(field::MAX_KEY) {
        dst.max_key = src.max_key.clone();
    }
    if has(field::BLOB_BYTES) {
        dst.blob_bytes = src.blob_bytes.clone();
    }
    if has(field::PARTITION) {
        dst.partition = src.partition.clone();
    }
    if has(field::TIER) {
        dst.tier = src.tier.clone();
    }
    if has(field::OBJECT) {
        dst.object = src.object.clone();
    }
    if has(field::MAX_ENTRY_TIME) {
        dst.max_entry_time = src.max_entry_time;
    }
    if has(field::CAPS) {
        dst.caps = src.caps;
    }
    if has(field::LAST_COMPACTION_TIME) {
        dst.last_compaction_time = src.last_compaction_time;
    }
    if has(field::MERGE_COUNT) {
        dst.merge_count = src.merge_count;
    }
    if has(field::MERGE_CHAIN_MAX) {
        dst.merge_chain_max = src.merge_chain_max;
    }
    if has(field::RANGE_COUNT) {
        dst.range_count = src.range_count;
    }
    if has(field::RANGE_MIN_SEQ) {
        dst.range_min_seq = src.range_min_seq;
    }
    if has(field::RANGE_MAX_SEQ) {
        dst.range_max_seq = src.range_max_seq;
    }
    if has(field::RANGE_MIN_KEY) {
        dst.range_min_key = src.range_min_key.clone();
    }
    if has(field::RANGE_MAX_KEY) {
        dst.range_max_key = src.range_max_key.clone();
    }
}

/// A table must need no capability the database lacks, and carries an age
/// stamp only under `CAP_PERIODIC_AGE`.
fn check_table_against(m: &Catalog, t: &EditTable) -> std::result::Result<(), String> {
    if t.caps & !m.caps != 0 {
        return Err(format!(
            "table {} needs {:#x}, database declares {:#x}",
            t.id, t.caps, m.caps
        ));
    }
    if t.last_compaction_time != 0 && m.caps & CAP_PERIODIC_AGE == 0 {
        return Err(format!(
            "table {} carries a last compaction time without CAP_PERIODIC_AGE",
            t.id
        ));
    }
    Ok(())
}

fn family<'a>(m: &'a mut Catalog, cf: &str) -> std::result::Result<&'a mut Family, String> {
    m.family_mut(cf)
        .ok_or_else(|| format!("column family {cf:?} absent"))
}

/// Apply one op under wavesdb's preconditions. A refused op is corruption:
/// an edit is only ever derived from the catalog it is applied to.
fn apply_op(m: &mut Catalog, op: &Op) -> std::result::Result<(), String> {
    match op {
        Op::AddTable { cf, table } => {
            check_table_against(m, table)?;
            let f = family(m, cf)?;
            if f.tables.iter().any(|t| t.meta.id == table.id) {
                return Err(format!("table {} already present in {cf:?}", table.id));
            }
            f.tables.push(table.to_table());
        }
        Op::RemoveTable { cf, id, level } => {
            let f = family(m, cf)?;
            let i = f
                .tables
                .iter()
                .position(|t| t.meta.id == *id)
                .ok_or_else(|| format!("table {id} absent from {cf:?}"))?;
            if f.tables[i].meta.level != *level {
                return Err(format!(
                    "table {id} is at level {}, not {level}",
                    f.tables[i].meta.level
                ));
            }
            f.tables.remove(i);
        }
        Op::UpdateTable { cf, id, mask, table } => {
            let f = family(m, cf)?;
            let i = f
                .tables
                .iter()
                .position(|t| t.meta.id == *id)
                .ok_or_else(|| format!("table {id} absent from {cf:?}"))?;
            let mut upd = EditTable::from_table(&f.tables[i]);
            apply_mask(&mut upd, *mask, table);
            check_table_against(m, &upd)?;
            family(m, cf)?.tables[i] = upd.to_table();
        }
        Op::AddBlob { cf, blob } => {
            let f = family(m, cf)?;
            if f.blobs.iter().any(|b| b.id == blob.id) {
                return Err(format!("blob {} already present in {cf:?}", blob.id));
            }
            f.blobs.push(*blob);
        }
        Op::UpdateBlob { cf, id, mask, size, live_bytes } => {
            let f = family(m, cf)?;
            let b = f
                .blobs
                .iter_mut()
                .find(|b| b.id == *id)
                .ok_or_else(|| format!("blob {id} absent from {cf:?}"))?;
            if mask & field::SIZE != 0 {
                b.size = *size;
            }
            if mask & field::LIVE_BYTES != 0 {
                b.live_bytes = *live_bytes;
            }
        }
        Op::RemoveBlob { cf, id } => {
            let f = family(m, cf)?;
            let i = f
                .blobs
                .iter()
                .position(|b| b.id == *id)
                .ok_or_else(|| format!("blob {id} absent from {cf:?}"))?;
            f.blobs.remove(i);
        }
        Op::CreateCf { cf, mask, config, unified_id } => {
            if m.family_mut(cf).is_some() {
                return Err(format!("column family {cf:?} already present"));
            }
            m.cfs.push(Family {
                name: cf.clone(),
                config: config.clone(),
                unified_id: stored_id(cf, *mask, *unified_id),
                ..Family::default()
            });
        }
        Op::DropCf { cf } => {
            let i = m
                .cfs
                .iter()
                .position(|f| f.name == *cf)
                .ok_or_else(|| format!("column family {cf:?} absent"))?;
            let f = &m.cfs[i];
            if !f.tables.is_empty() || !f.blobs.is_empty() {
                return Err(format!(
                    "column family {cf:?} still holds {} tables and {} blob files",
                    f.tables.len(),
                    f.blobs.len()
                ));
            }
            m.cfs.remove(i);
        }
        Op::RenameCf { cf, new_name } => {
            if new_name.is_empty() || m.family_mut(new_name).is_some() {
                return Err(format!("column family {new_name:?} already present"));
            }
            let f = family(m, cf)?;
            f.name = new_name.clone();
            // An id stored because it differed from the old name's may now
            // equal the new one's; the stored form never repeats the default.
            f.unified_id = f
                .unified_id
                .filter(|&id| id != crate::unified::cf_id(&f.name));
        }
        Op::SetCfConfig { cf, mask, config, unified_id } => {
            let f = family(m, cf)?;
            if mask & field::CONFIG != 0 {
                f.config = config.clone();
            }
            if mask & field::UNIFIED_ID != 0 {
                // Zero is wavesdb's "derive it from the name".
                f.unified_id = stored_id(cf, *mask, *unified_id);
            }
        }
        Op::SetNextFileId(v) => {
            if *v < m.next_file_id {
                return Err(format!("next file id {v} below current {}", m.next_file_id));
            }
            m.next_file_id = *v;
        }
        Op::SetGlobalSeq(v) => {
            if *v < m.global_seq {
                return Err(format!("global sequence {v} below current {}", m.global_seq));
            }
            m.global_seq = *v;
        }
        Op::SetWalLayout(v) => {
            if *v != 1 {
                return Err(format!("WAL layout is one-way: value {v}"));
            }
            m.wal_layout = WalLayout::Unified;
        }
        Op::SetNonce(v) => {
            if m.instance_nonce.is_some() {
                return Err("instance nonce already set".into());
            }
            if *v == 0 {
                return Err("instance nonce zero".into());
            }
            m.instance_nonce = Some(*v);
        }
        Op::SetCapability(bits) => {
            if bits & !KNOWN_CAPS != 0 {
                return Err(format!("unknown capability bits {:#x}", bits & !KNOWN_CAPS));
            }
            let next = m.caps | bits;
            check_dependencies(next).map_err(|e| e.to_string())?;
            m.caps = next;
        }
        Op::SetManagedState { mask, discard_seq, last_managed_commit } => {
            // Both are floors the engine only raises; lowering one would
            // resurrect discarded versions or re-issue an owned sequence.
            if mask & field::DISCARD_SEQ != 0 {
                if *discard_seq < m.discard_seq {
                    return Err(format!(
                        "discard sequence {discard_seq} below current {}",
                        m.discard_seq
                    ));
                }
                m.discard_seq = *discard_seq;
            }
            if mask & field::LAST_MANAGED_COMMIT != 0 {
                if *last_managed_commit < m.last_managed_commit {
                    return Err(format!(
                        "last managed commit {last_managed_commit} below current {}",
                        m.last_managed_commit
                    ));
                }
                m.last_managed_commit = *last_managed_commit;
            }
        }
    }
    Ok(())
}

/// The unified id a family stores: `None` when absent, zero, or equal to the
/// one its name derives.
fn stored_id(cf: &str, mask: u64, id: u64) -> Option<u64> {
    (mask & field::UNIFIED_ID != 0 && id != 0 && id != crate::unified::cf_id(cf)).then_some(id)
}

/// Replay a log (`data`, the whole file) onto its snapshot.
pub fn replay(mut m: Catalog, data: &[u8]) -> Result<Catalog> {
    if m.caps & CAP_MANIFEST_EDITS == 0 {
        return Err(corrupt(
            "MANIFEST-EDITS exists but the snapshot does not declare CAP_MANIFEST_EDITS",
        ));
    }
    debug_assert!(m.caps & CAP_WAVES08_BODIES != 0, "dispatched on the profile");
    let log = read(data)?;
    if log.header.base_applied_through > m.applied_through {
        return Err(corrupt(format!(
            "edit log base {} is ahead of the snapshot's applied-through {}",
            log.header.base_applied_through, m.applied_through
        )));
    }
    let mut last = m.applied_through;
    let mut replayed = false;
    for rec in &log.records {
        if rec.edit_id <= last {
            continue; // already folded into the snapshot (reset-crash window)
        }
        if rec.edit_id != last + 1 {
            return Err(corrupt(format!(
                "edit id gap: {} follows {last}",
                rec.edit_id
            )));
        }
        for (i, op) in rec.ops.iter().enumerate() {
            apply_op(&mut m, op)
                .map_err(|why| corrupt(format!("edit {} op {i}: {why}", rec.edit_id)))?;
        }
        last = rec.edit_id;
        replayed = true;
    }
    m.applied_through = last;
    m.next_edit_id = m.next_edit_id.max(last + 1);
    for f in &mut m.cfs {
        f.blobs.sort_unstable_by_key(|b| b.id);
    }
    if replayed {
        cross_check(&m)?;
    }
    Ok(m)
}

/// wavesdb recovery rule 6: each op is validated only against the state it
/// mutates, so a log well formed record by record can still name a file the
/// allocator would hand out again, or a table holding rows above the
/// sequence. Both are silent and expensive, so a replayed catalog is checked.
fn cross_check(m: &Catalog) -> Result<()> {
    for f in &m.cfs {
        for t in &f.tables {
            if t.meta.id >= m.next_file_id {
                return Err(corrupt(format!(
                    "replayed catalog has table {} in {:?} at or above next_file_id {}",
                    t.meta.id, f.name, m.next_file_id
                )));
            }
            if t.meta.max_seq > m.global_seq {
                return Err(corrupt(format!(
                    "replayed catalog has table {} in {:?} with max_seq {} above global_seq {}",
                    t.meta.id, f.name, t.meta.max_seq, m.global_seq
                )));
            }
        }
        for b in &f.blobs {
            if b.id >= m.next_file_id {
                return Err(corrupt(format!(
                    "replayed catalog has blob {} in {:?} at or above next_file_id {}",
                    b.id, f.name, m.next_file_id
                )));
            }
        }
    }
    Ok(())
}
