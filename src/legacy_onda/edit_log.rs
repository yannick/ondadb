//! 0.9.x `MANIFEST-EDITS` decoder (magic `ONDE`, schema 1).
//!
//! ```text
//! header (28 bytes, offset 0):
//!   magic u32 = 0x4F4E_4445 ("ONDE"; on disk 45 44 4E 4F) | schema u32 = 1
//!   | base_applied_through u64 | snapshot_generation u64 | crc32-ieee u32 over [0, 24)
//! records (contiguous from offset 28):
//!   len u32 | crc32-ieee(payload) u32 | payload
//! ```
//!
//! The record payload — `edit_id u64 | op_count uvarint | op*` — is the same op
//! table epoch 1 writes, so it is decoded by
//! [`crate::manifest_edit::decode_payload`]. Only the container (header and
//! frame checksums) differs, and that is what is frozen here. If an epoch-1
//! change ever alters an op's payload, this module must fork the op decoder
//! rather than follow it.
//!
//! The recovery rules are 0.9's, unchanged: a short file, a bad header CRC or an
//! unknown magic/schema is `Corruption`; only an **EOF-truncated** record ends
//! replay cleanly; ids are contiguous from `base_applied_through + 1`; the log
//! belongs to the snapshot iff `base_applied_through <= applied_through`.

use std::path::Path;

use super::checksum_ieee as checksum;
use crate::encoding::{read_u32, read_u64};
use crate::error::{OndaError, Result};
use crate::manifest::Manifest;
use crate::manifest_edit::{CatalogReplayer, EditLogHeader, EditRecord};

/// `"ONDE"`, stored little-endian.
pub const MAGIC: u32 = 0x4F4E_4445;
/// The only schema 0.9 wrote.
pub const SCHEMA: u32 = 1;
/// Fixed header width; records begin immediately after it.
pub const HEADER_BYTES: usize = 28;
/// Record-frame overhead (`len u32 | crc u32`).
const RECORD_HEADER_BYTES: usize = 8;
/// 0.9's per-record payload cap, checked before any allocation.
const MAX_RECORD_BYTES: usize = 64 << 20;

fn corrupt(msg: impl Into<String>) -> OndaError {
    OndaError::Corruption(msg.into())
}

/// Decode the 28-byte `ONDE` header.
pub fn decode_header(data: &[u8]) -> Result<EditLogHeader> {
    if data.len() < HEADER_BYTES {
        return Err(corrupt(format!(
            "0.9 manifest edit log: file is {} bytes, shorter than the {HEADER_BYTES}-byte header",
            data.len()
        )));
    }
    let head = &data[..HEADER_BYTES];
    if read_u32(&head[24..28]) != checksum(&head[..24]) {
        return Err(corrupt("0.9 manifest edit log: header CRC mismatch"));
    }
    let magic = read_u32(&head[0..4]);
    if magic != MAGIC {
        return Err(corrupt(format!(
            "0.9 manifest edit log: magic {magic:#010x} is not {MAGIC:#010x}"
        )));
    }
    let schema = read_u32(&head[4..8]);
    if schema != SCHEMA {
        return Err(corrupt(format!(
            "0.9 manifest edit log: schema {schema} is not {SCHEMA}"
        )));
    }
    Ok(EditLogHeader {
        base_applied_through: read_u64(&head[8..16]),
        snapshot_generation: read_u64(&head[16..24]),
    })
}

/// Decode every complete record after the header. An EOF-truncated frame ends
/// the scan cleanly; a complete frame failing its CRC is `Corruption`.
pub fn decode_records(data: &[u8]) -> Result<Vec<EditRecord>> {
    let mut off = HEADER_BYTES;
    let mut out = Vec::new();
    loop {
        if off + RECORD_HEADER_BYTES > data.len() {
            break;
        }
        let len = read_u32(&data[off..off + 4]) as usize;
        if len > MAX_RECORD_BYTES {
            return Err(corrupt(format!(
                "0.9 manifest edit log: record at offset {off} declares {len} bytes"
            )));
        }
        let body = off + RECORD_HEADER_BYTES;
        if body + len > data.len() {
            break;
        }
        let payload = &data[body..body + len];
        if read_u32(&data[off + 4..off + 8]) != checksum(payload) {
            return Err(corrupt(format!(
                "0.9 manifest edit log: complete record at offset {off} fails its CRC"
            )));
        }
        let (edit_id, edit) = crate::manifest_edit::decode_payload(payload)?;
        out.push(EditRecord { edit_id, edit });
        off = body + len;
    }
    Ok(out)
}

/// Replay a 0.9 edit log (`data`, the whole file) onto its snapshot `m`.
pub fn replay(m: Manifest, data: &[u8]) -> Result<Manifest> {
    if m.caps & crate::format::CAP_MANIFEST_EDITS == 0 {
        return Err(corrupt(
            "0.9 manifest edit log: present without CAP_MANIFEST_EDITS in the snapshot",
        ));
    }
    let header = decode_header(data)?;
    if header.base_applied_through > m.applied_through {
        return Err(corrupt(format!(
            "0.9 manifest edit log: base_applied_through {} is ahead of the snapshot's \
             applied_through {}",
            header.base_applied_through, m.applied_through
        )));
    }
    let records = decode_records(data)?;
    let base = header.base_applied_through;
    let snapshot_applied_through = m.applied_through;
    let mut applied_through = snapshot_applied_through;
    let mut replayer = CatalogReplayer::new(m);
    for (i, rec) in records.iter().enumerate() {
        let expected = base + 1 + i as u64;
        if rec.edit_id != expected {
            return Err(corrupt(format!(
                "0.9 manifest edit log: record id {} where {expected} was expected",
                rec.edit_id
            )));
        }
        if rec.edit_id <= snapshot_applied_through {
            continue;
        }
        replayer.apply(&rec.edit).map_err(|e| match e {
            OndaError::Corruption(msg) => {
                corrupt(format!("0.9 manifest edit {}: {msg}", rec.edit_id))
            }
            other => other,
        })?;
        applied_through = rec.edit_id;
    }
    let mut m = replayer.finish();
    m.applied_through = applied_through;
    m.next_edit_id = applied_through + 1;
    Ok(m)
}

/// Load a 0.9 directory's snapshot and replay its edit log, if any, into it —
/// 0.9's `recover_catalog`, with the configs still in their stored (0.9) form.
pub fn recover_raw(dir: &Path) -> Result<Manifest> {
    let snapshot = match std::fs::read(crate::manifest::manifest_path(dir)) {
        Ok(d) => super::manifest::decode(&d)?,
        // 0.9 read a missing MANIFEST as an empty database; so does this.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Manifest::default(),
        Err(e) => return Err(e.into()),
    };
    let m = match std::fs::read(crate::manifest_edit::edit_log_path(dir)) {
        Ok(data) => replay(snapshot, &data)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => snapshot,
        Err(e) => return Err(e.into()),
    };
    Ok(reconciled(m))
}

/// 0.9's rule r7: the allocator and the sequence dominate everything the
/// catalog references.
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

    fn fixture_log() -> Vec<u8> {
        std::fs::read(crate::util::legacy_fixture_path("db-caps/MANIFEST-EDITS")).unwrap()
    }

    /// The 0.9.1-written log in the `db-caps` fixture decodes: an IEEE header,
    /// contiguous ids from its base, every record CRC-valid.
    #[test]
    fn fixture_log_decodes() {
        let data = fixture_log();
        let h = decode_header(&data).unwrap();
        let recs = decode_records(&data).unwrap();
        assert!(!recs.is_empty(), "the fixture log carries records");
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.edit_id, h.base_applied_through + 1 + i as u64);
        }
    }

    #[test]
    fn header_corruption_rows_fail_closed() {
        let data = fixture_log();
        // Truncated below the header.
        for len in [0, 4, HEADER_BYTES - 1] {
            assert_eq!(
                decode_header(&data[..len]).unwrap_err().kind(),
                "corruption"
            );
        }
        // A bit flip anywhere in the header fails its CRC.
        for at in 0..HEADER_BYTES {
            let mut d = data.clone();
            d[at] ^= 0x10;
            assert_eq!(
                decode_header(&d).unwrap_err().kind(),
                "corruption",
                "flip at {at}"
            );
        }
        // A wrong magic or schema under a valid CRC is still refused.
        for (at, val) in [(0usize, 0x1234_5678u32), (4, 2)] {
            let mut d = data.clone();
            d[at..at + 4].copy_from_slice(&val.to_le_bytes());
            let crc = checksum(&d[..24]);
            d[24..28].copy_from_slice(&crc.to_le_bytes());
            assert_eq!(decode_header(&d).unwrap_err().kind(), "corruption");
        }
    }

    #[test]
    fn record_corruption_rows() {
        let data = fixture_log();
        let n = decode_records(&data).unwrap().len();
        // A torn tail is clean: drop the last byte, one fewer record.
        let torn = &data[..data.len() - 1];
        assert_eq!(decode_records(torn).unwrap().len(), n - 1);
        // A flipped payload byte in a complete record is corruption.
        let mut d = data.clone();
        let last = d.len() - 1;
        d[last] ^= 0x01;
        assert_eq!(decode_records(&d).unwrap_err().kind(), "corruption");
        // An epoch-1 checksum (CRC32-C) over a 0.9 frame is corruption.
        let mut d = data.clone();
        let len = read_u32(&d[HEADER_BYTES..]) as usize;
        let payload = d[HEADER_BYTES + 8..HEADER_BYTES + 8 + len].to_vec();
        let c = crc32c_for_test(&payload);
        d[HEADER_BYTES + 4..HEADER_BYTES + 8].copy_from_slice(&c.to_le_bytes());
        assert_eq!(decode_records(&d).unwrap_err().kind(), "corruption");
    }

    /// A reference CRC32-C, bit by bit, so this test does not depend on which
    /// polynomial `encoding::checksum` currently implements.
    fn crc32c_for_test(b: &[u8]) -> u32 {
        let mut c = !0u32;
        for &x in b {
            c ^= u32::from(x);
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    (c >> 1) ^ 0x82F6_3B78
                } else {
                    c >> 1
                };
            }
        }
        !c
    }
}
