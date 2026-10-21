//! wavesdb's epoch-1 `MANIFEST`: the epoch-1 layout ondaDB writes, with the
//! sections only wavesdb produces.
//!
//! ```text
//! header   magic "YOLODBMF" | version u32 = 1 | caps u64 | db_flags u32
//!          | next_file_id u64 | global_seq u64
//! db       0x2 nonce u64 | 0x4 edit cursor (generation, applied, next) u64×3
//!          | 0x8 managed state: discard_seq u64 | last_managed_commit u64
//! cf*      flags uvarint | name | config (JSON) | 0x1 unified id u64
//!          | 0x2 blobs: count | (id | size | live_bytes)* ascending
//!          | tables: count | table*
//! table    id | level | entries | tombstones | max_seq | klog_size
//!          | vlog_size | min_key | max_key | flags uvarint, then by bit:
//!          0x1 partition | 0x2 tier | 0x4 object | 0x8 max_entry_time
//!          | 0x10 last_compaction_time | 0x20 range summary | 0x40 caps
//!          | 0x80 merge: count | chain_max | 0x100 blob refs: count | (id | bytes)*
//! trailer  crc32c u32 over everything before it
//! ```
//!
//! The order of checks is wavesdb's (`decodeEpoch1`): the whole-file CRC, then
//! the version, the capability word and the section masks (an unknown bit is
//! `UnsupportedFormat`), then the layout, where anything a writer cannot
//! produce — a section without the bit admitting it, a section at its empty
//! default, a stored id equal to the derived one — is `Corruption`.

use std::collections::BTreeMap;

use super::{check_dependencies, check_table_caps, corrupt, Blob, Catalog, Family, Reader, Table};
use crate::encoding::{checksum, read_u32};
use crate::error::{OndaError, Result};
use crate::format::manifest_file::{
    CF_SHARED_BLOBS, CF_UNIFIED_ID, DB_EDIT_LOG, DB_INSTANCE_NONCE, DB_MANAGED_STATE,
    DB_UNIFIED_WAL, HEADER_LEN, MAGIC, SST_BLOB_REFS, SST_LAST_COMPACTION_TIME,
    SST_MAX_ENTRY_TIME, SST_MERGE_STATS, SST_OBJECT, SST_PARTITION, SST_RANGE, SST_TABLE_CAPS,
    SST_TIER, VERSION,
};
use crate::format::wavesdb_reserved::{CAP_MANAGED_MODE, CAP_VALUES_SHARED_BLOBS, CAP_WAVES08_BODIES};
use crate::format::{CAP_MANIFEST_EDITS, CAP_PERIODIC_AGE, CAP_RANGE_DELETES};
use crate::manifest::{SstMeta, WalLayout};

/// The database sections wavesdb writes.
pub const DB_KNOWN: u32 = DB_UNIFIED_WAL | DB_INSTANCE_NONCE | DB_EDIT_LOG | DB_MANAGED_STATE;
/// The family sections wavesdb writes.
pub const CF_KNOWN: u64 = CF_UNIFIED_ID | CF_SHARED_BLOBS;
/// The table sections wavesdb writes.
pub const SST_KNOWN: u64 = SST_PARTITION
    | SST_TIER
    | SST_OBJECT
    | SST_MAX_ENTRY_TIME
    | SST_LAST_COMPACTION_TIME
    | SST_RANGE
    | SST_TABLE_CAPS
    | SST_MERGE_STATS
    | SST_BLOB_REFS;

const _: () = assert!(DB_KNOWN == 0xF && CF_KNOWN == 0x3 && SST_KNOWN == 0x1FF);

/// Decode a wavesdb epoch-1 `MANIFEST` into a [`Catalog`].
///
/// A manifest without `CAP_WAVES08_BODIES` is ondaDB's own and is refused
/// here as `InvalidArgs`: the caller dispatched on the wrong profile.
pub fn decode(data: &[u8]) -> Result<Catalog> {
    if data.len() < 8 || data[..8] != MAGIC {
        return Err(corrupt("MANIFEST magic is not YOLODBMF"));
    }
    if data.len() < HEADER_LEN + 4 {
        return Err(corrupt(format!(
            "MANIFEST is {} bytes, shorter than its header",
            data.len()
        )));
    }
    let (body, stored_crc) = data.split_at(data.len() - 4);
    if read_u32(stored_crc) != checksum(body) {
        return Err(corrupt("MANIFEST checksum mismatch"));
    }
    let mut r = Reader::new(&body[8..], false);
    let version = r.u32("version")?;
    if version != VERSION {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb manifest version {version} is not implemented by this binary"
        )));
    }
    let caps = r.u64("capability word")?;
    if caps & !super::KNOWN_CAPS != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb manifest capabilities {caps:#x} outside known mask {:#x}",
            super::KNOWN_CAPS
        )));
    }
    check_dependencies(caps)?;
    if caps & CAP_WAVES08_BODIES == 0 {
        return Err(OndaError::InvalidArgs(
            "not a wavesdb catalog: the manifest does not declare CAP_WAVES08_BODIES".into(),
        ));
    }
    let db_flags = r.u32("database flags")?;
    if db_flags & !DB_KNOWN != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb manifest database flags {db_flags:#x} outside known mask {DB_KNOWN:#x}"
        )));
    }
    let mut m = Catalog {
        caps,
        next_file_id: r.u64("next file id")?,
        global_seq: r.u64("global sequence")?,
        ..Catalog::default()
    };
    if db_flags & DB_UNIFIED_WAL != 0 {
        m.wal_layout = WalLayout::Unified;
    }
    if db_flags & DB_INSTANCE_NONCE != 0 {
        m.instance_nonce = Some(r.u64("instance nonce")?);
    }
    if db_flags & DB_EDIT_LOG != 0 {
        m.generation = r.u64("snapshot generation")?;
        m.applied_through = r.u64("applied-through edit id")?;
        m.next_edit_id = r.u64("next edit id")?;
        if m.applied_through.checked_add(1) != Some(m.next_edit_id) {
            return Err(corrupt(format!(
                "edit cursor: next edit id {} is not applied-through {} + 1",
                m.next_edit_id, m.applied_through
            )));
        }
        if m.generation == 0 && m.applied_through == 0 {
            return Err(corrupt("edit-log section at its empty default"));
        }
    }
    if db_flags & DB_MANAGED_STATE != 0 {
        if caps & CAP_MANAGED_MODE == 0 {
            return Err(corrupt("managed-sequence state without CAP_MANAGED_MODE"));
        }
        m.discard_seq = r.u64("discard sequence")?;
        m.last_managed_commit = r.u64("last managed commit")?;
        if m.discard_seq == 0 && m.last_managed_commit == 0 {
            return Err(corrupt("managed-sequence section at its empty default"));
        }
    }
    let cf_count = r.count("cf count")?;
    for _ in 0..cf_count {
        m.cfs.push(decode_family(&mut r, caps)?);
    }
    if !r.p.is_empty() {
        return Err(corrupt(format!(
            "{} trailing bytes after the last column family",
            r.p.len()
        )));
    }
    // wavesdb's fillEditWatermark: a catalog declaring the edit log whose
    // cursor sits at the default is a fresh log of generation 1.
    if caps & CAP_MANIFEST_EDITS != 0 && m.generation == 0 {
        m.generation = 1;
    }
    Ok(m)
}

fn decode_family(r: &mut Reader<'_>, caps: u64) -> Result<Family> {
    let flags = r.uvar("cf flags")?;
    if flags & !CF_KNOWN != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb manifest cf flags {flags:#x} outside known mask {CF_KNOWN:#x}"
        )));
    }
    let mut cf = Family {
        name: r.string("cf name")?,
        config: r.bytes("cf config")?,
        ..Family::default()
    };
    if flags & CF_UNIFIED_ID != 0 {
        let id = r.u64("cf unified id")?;
        if id == crate::unified::cf_id(&cf.name) {
            return Err(corrupt(format!(
                "cf {:?} stores the unified id its name derives",
                cf.name
            )));
        }
        cf.unified_id = Some(id);
    }
    if flags & CF_SHARED_BLOBS != 0 {
        if caps & CAP_VALUES_SHARED_BLOBS == 0 {
            return Err(corrupt(format!(
                "cf {:?} lists blob files without CAP_VALUES_SHARED_BLOBS",
                cf.name
            )));
        }
        let n = r.count("blob count")?;
        if n == 0 {
            return Err(corrupt(format!("cf {:?} announces zero blob files", cf.name)));
        }
        for _ in 0..n {
            let b = Blob {
                id: r.uvar("blob id")?,
                size: r.uvar("blob size")?,
                live_bytes: r.uvar("blob live bytes")?,
            };
            if cf.blobs.last().is_some_and(|last| b.id <= last.id) {
                return Err(corrupt(format!("cf {:?} blob ids are not ascending", cf.name)));
            }
            cf.blobs.push(b);
        }
    }
    let n = r.count("table count")?;
    for _ in 0..n {
        let t = decode_table(r, caps)
            .map_err(|e| super::prefix(e, &format!("cf {:?}", cf.name)))?;
        cf.tables.push(t);
    }
    Ok(cf)
}

fn decode_table(r: &mut Reader<'_>, caps: u64) -> Result<Table> {
    let mut meta = SstMeta {
        id: r.uvar("table id")?,
        level: r.level()?,
        num_entries: r.uvar("entry count")?,
        num_tombstones: r.uvar("tombstone count")?,
        max_seq: r.uvar("max sequence")?,
        klog_size: r.uvar("klog size")?,
        vlog_size: r.uvar("vlog size")?,
        min_key: r.bytes("min key")?,
        max_key: r.bytes("max key")?,
        ..SstMeta::default()
    };
    let id = meta.id;
    let flags = r.uvar("table flags")?;
    if flags & !SST_KNOWN != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb table {id} section flags {flags:#x} outside known mask {SST_KNOWN:#x}"
        )));
    }
    let need = |bit: u64, section: &str| -> Result<()> {
        if caps & bit == 0 {
            return Err(corrupt(format!(
                "table {id} carries {section} without capability {bit:#x}"
            )));
        }
        Ok(())
    };
    if flags & SST_PARTITION != 0 {
        meta.partition = Some(r.string("partition")?);
    }
    if flags & SST_TIER != 0 {
        meta.tier = Some(r.string("tier")?);
    }
    if flags & SST_OBJECT != 0 {
        meta.object = Some(r.string("object")?);
    }
    if flags & SST_MAX_ENTRY_TIME != 0 {
        meta.max_entry_time = Some(r.uvar("max entry time")? as i64);
    }
    if flags & SST_LAST_COMPACTION_TIME != 0 {
        need(CAP_PERIODIC_AGE, "a compaction stamp")?;
        meta.last_compaction_time = Some(r.uvar("last compaction time")? as i64);
    }
    if flags & SST_RANGE != 0 {
        need(CAP_RANGE_DELETES, "a range summary")?;
        meta.range_count = r.uvar("range count")?;
        meta.range_min_seq = r.uvar("range min sequence")?;
        meta.range_max_seq = r.uvar("range max sequence")?;
        let min_key = r.bytes("range min key")?;
        let max_key = r.bytes("range max key")?;
        if meta.range_count == 0
            || min_key.is_empty()
            || max_key.is_empty()
            || meta.range_min_seq > meta.range_max_seq
        {
            return Err(corrupt(format!(
                "table {id} range summary a writer cannot produce"
            )));
        }
        meta.range_min_key = Some(min_key);
        meta.range_max_key = Some(max_key);
    }
    if flags & SST_TABLE_CAPS != 0 {
        let t = r.uvar("table capability word")?;
        // The database word is the union of every artifact's, so a table bit
        // outside it is not a newer format: the table is not this catalog's.
        if t == 0 || t & !caps != 0 {
            return Err(corrupt(format!(
                "table {id} capability word {t:#x} is zero or outside the database's {caps:#x}"
            )));
        }
        check_table_caps(t)?;
        meta.table_caps = Some(t);
    }
    let mut table = Table::default();
    if flags & SST_MERGE_STATS != 0 {
        need(CAP_WAVES08_BODIES, "merge statistics")?;
        table.merge_count = r.uvar("merge count")?;
        table.merge_chain_max = r.uvar("merge chain")?;
        if table.merge_count == 0 && table.merge_chain_max == 0 {
            return Err(corrupt(format!(
                "table {id} merge statistics at their empty default"
            )));
        }
    }
    if flags & SST_BLOB_REFS != 0 {
        need(CAP_VALUES_SHARED_BLOBS, "blob references")?;
        table.blob_bytes = blob_refs(r, id)?;
        if table.blob_bytes.is_empty() {
            return Err(corrupt(format!("table {id} announces zero blob references")));
        }
    }
    table.meta = meta;
    Ok(table)
}

/// `count | (id | bytes)*`, ids strictly ascending (which also refuses a
/// duplicate a map would silently collapse).
pub(crate) fn blob_refs(r: &mut Reader<'_>, table: u64) -> Result<BTreeMap<u64, u64>> {
    let n = r.count("blob reference count")?;
    let mut out = BTreeMap::new();
    let mut last = None;
    for _ in 0..n {
        let id = r.uvar("blob id")?;
        let bytes = r.uvar("blob bytes")?;
        if last.is_some_and(|l| id <= l) {
            return Err(corrupt(format!(
                "table {table} blob references are not ascending"
            )));
        }
        last = Some(id);
        out.insert(id, bytes);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/wavesdb-epoch1")
                .join(name),
        )
        .unwrap()
    }

    /// The managed fixture's snapshot holds everything (no edit log): one
    /// family, its two tables, and the managed-state section.
    #[test]
    fn managed_snapshot_decodes() {
        let m = decode(&fixture("managed/db/MANIFEST")).unwrap();
        assert_eq!(m.wal_layout, WalLayout::Unified);
        assert!(m.caps & CAP_MANAGED_MODE != 0);
        assert!(m.discard_seq > 0 && m.last_managed_commit >= m.discard_seq);
        assert_eq!(m.cfs.len(), 1);
        assert_eq!(m.cfs[0].name, "m");
        let mut ids: Vec<_> = m.cfs[0].tables.iter().map(|t| t.meta.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, [1, 2]);
    }

    /// Every flipped byte is caught by the whole-file CRC.
    #[test]
    fn a_flipped_byte_is_corruption() {
        let good = fixture("managed/db/MANIFEST");
        for i in 0..good.len() {
            let mut b = good.clone();
            b[i] ^= 0x40;
            let e = decode(&b).unwrap_err();
            assert!(
                matches!(e, OndaError::Corruption(_)),
                "byte {i}: {e:?}"
            );
        }
    }

    /// Bits are checked in wavesdb's order: an unknown capability is a newer
    /// format, a known word without the wavesdb body bit is not this decoder's.
    #[test]
    fn capability_word_rows() {
        let good = fixture("managed/db/MANIFEST");
        let with_caps = |caps: u64| {
            let mut b = good[..good.len() - 4].to_vec();
            b[12..20].copy_from_slice(&caps.to_le_bytes());
            let crc = checksum(&b);
            b.extend_from_slice(&crc.to_le_bytes());
            decode(&b).unwrap_err()
        };
        let caps = crate::encoding::read_u64(&good[12..20]);
        assert!(matches!(with_caps(caps | 1 << 11), OndaError::UnsupportedFormat(_)));
        assert!(matches!(with_caps(caps & !CAP_WAVES08_BODIES), OndaError::InvalidArgs(_)));
        assert!(matches!(
            with_caps(caps & !CAP_MANAGED_MODE),
            OndaError::Corruption(_)
        ));
    }
}
