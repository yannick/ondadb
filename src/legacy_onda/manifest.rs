//! 0.9.x `MANIFEST` decoder (magic `WVMF`, versions 1 and 2).
//!
//! A frozen copy of the 0.9.1 decode path: a positional body, the three
//! positional tail sections, and the `ONDA*` 8-byte tagged tails, all under a
//! whole-file **IEEE** CRC-32 (see [`super::checksum_ieee`]). Nothing here may
//! change to follow an epoch-1 format change — the bytes it reads were written
//! by a binary that no longer exists. The only encoder is `#[cfg(test)]`.
//!
//! Layout, as 0.9 wrote it:
//!
//! ```text
//! magic u32 = 0x5756_4D46 ("WVMF") | version u32 in {1, 2}
//! | next_file_id u64 | global_seq u64 | cf_count uvarint
//! | per CF: name* | config* | sst_count uvarint
//!   | per SST: id, level, num_entries, num_tombstones, max_seq,
//!              klog_size, vlog_size (uvarint) | min_key* | max_key*
//! | positional tails: partition | tier | max_entry_time
//! | tagged tails: ONDAOBJ1 ONDAINS1 ONDACAP1 ONDARNG1 ONDAAGE1 ONDAMED1 ONDAWAL1
//! | crc32-ieee u32
//! ```

use super::checksum_ieee as checksum;
#[cfg(test)]
use crate::encoding::{append_u32, append_u64, append_uvarint};
use crate::encoding::{read_u32, read_u64, uvarint};
use crate::error::{OndaError, Result};
use crate::manifest::{CfManifest, Manifest, SstMeta, WalLayout};

/// `"WVMF"`, stored as a little-endian `u32`.
pub const MAGIC: u32 = 0x5756_4D46;
/// Lowest manifest version, and the one still written whenever the database
/// uses no format capability — the same lowest-version discipline the
/// positional tails follow, so a legacy-only database stays readable by every
/// binary that ever opened it.
const VERSION_V1: u32 = 1;
/// Manifest version written once a capability is enabled: it is the fence that
/// makes an older binary refuse the file (the version is checked by equality
/// there, so it fails closed without knowing why).
const VERSION_V2: u32 = 2;
/// Every manifest tail tag is this wide.
const TAG_LEN: usize = 8;
const WAL_LAYOUT_TAG: &[u8; 8] = b"ONDAWAL1";
const OBJECT_TAG: &[u8; 8] = b"ONDAOBJ1";
const INSTANCE_TAG: &[u8; 8] = b"ONDAINS1";
const FORMAT_CAPS_TAG: &[u8; 8] = b"ONDACAP1";
/// Per-table periodic-compaction age state (0.3), written only by a database
/// that has enabled [`CAP_PERIODIC_AGE`](crate::format::CAP_PERIODIC_AGE).
const LAST_COMPACTION_TAG: &[u8; 8] = b"ONDAAGE1";
/// Per-table range-tombstone summary (1.2), written only by a database that has
/// enabled [`CAP_RANGE_DELETES`](crate::format::CAP_RANGE_DELETES).
///
/// A **tagged** tail, never appended to the positional `SstMeta` body:
/// [`decode_sstable`] initializes optional fields to `None` and relies on tails
/// to fill them, and appending to the body would break both VERSION-1 readers
/// and the append-tolerant decode.
const RANGE_TAG: &[u8; 8] = b"ONDARNG1";
/// Edit-log bookkeeping (2.2): `generation | applied_through | next_edit_id`,
/// three `u64` LE. Emitted only once a database has an edit log, so a legacy
/// database's bytes are unchanged.
const MANIFEST_EDITS_TAG: &[u8; 8] = b"ONDAMED1";

/// Decode a 0.9.x `MANIFEST` (magic `WVMF`, version 1 or 2, IEEE CRC).
///
/// Per-CF config blobs are returned **as stored** — the 0.9 positional
/// encoding. [`super::recover_catalog`] converts them; this function only
/// decodes the container, so it can be pinned against the frozen fixtures
/// byte for byte.
pub fn decode(data: &[u8]) -> Result<Manifest> {
    let body = verified_manifest_body(data)?;
    let mut cursor = ManifestCursor::new(body);
    let header = decode_manifest_header(&mut cursor)?;
    let mut cfs = decode_manifest_column_families(&mut cursor, header.column_family_count)?;
    let p = decode_positional_tails(cursor.into_remaining(), &mut cfs)?;
    let tags = decode_tagged_tails(p, &mut cfs)?;
    // Version gate, after the tail: a capability word may only appear in a
    // manifest that already announces itself as v2.
    if (tags.caps != 0 || tags.edits.is_some()) && header.version == VERSION_V1 {
        return Err(corrupt_manifest());
    }
    if tags.last_compaction && tags.caps & crate::format::CAP_PERIODIC_AGE == 0 {
        return Err(corrupt_manifest());
    }
    if tags.range && tags.caps & crate::format::CAP_RANGE_DELETES == 0 {
        return Err(corrupt_manifest());
    }
    crate::format::check_caps(tags.caps)?;
    let edits = tags.edits.unwrap_or(EditLogTail {
        generation: 0,
        applied_through: 0,
        next_edit_id: 1,
    });
    if edits.applied_through.checked_add(1) != Some(edits.next_edit_id) {
        return Err(corrupt_manifest());
    }
    Ok(Manifest {
        next_file_id: header.next_file_id,
        global_seq: header.global_seq,
        cfs,
        wal_layout: tags.wal_layout,
        instance_nonce: tags.instance_nonce,
        caps: tags.caps,
        generation: edits.generation,
        applied_through: edits.applied_through,
        next_edit_id: edits.next_edit_id,
    })
}

/// Test-only 0.9 encoder, kept solely to build decoder inputs the frozen
/// fixtures do not cover. No production path writes a 0.9 manifest.
#[cfg(test)]
pub(crate) fn encode(manifest: &Manifest) -> Vec<u8> {
    let mut b = Vec::new();
    encode_manifest_header(&mut b, manifest);
    encode_manifest_column_families(&mut b, &manifest.cfs);
    let tails = ManifestTailPresence::detect(manifest);
    encode_positional_tails(&mut b, &manifest.cfs, tails);
    encode_tagged_tails(&mut b, manifest, tails);
    let crc = checksum(&b);
    append_u32(&mut b, crc);
    b
}

fn corrupt_manifest() -> OndaError {
    OndaError::Corruption("manifest: corrupt or invalid".into())
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct ManifestTailPresence {
    partition: bool,
    tier: bool,
    time: bool,
    object: bool,
    nonce: bool,
    caps: bool,
    last_compaction: bool,
    /// Whether the [`RANGE_TAG`] section is emitted.
    range: bool,
    edits: bool,
    layout: bool,
}

#[cfg(test)]
impl ManifestTailPresence {
    fn detect(manifest: &Manifest) -> Self {
        let has =
            |pick: fn(&SstMeta) -> bool| manifest.cfs.iter().any(|cf| cf.sstables.iter().any(pick));
        Self {
            partition: has(|sst| sst.partition.is_some()),
            tier: has(|sst| sst.tier.is_some()),
            time: has(|sst| sst.max_entry_time.is_some()),
            object: has(|sst| sst.object.is_some()),
            nonce: manifest.instance_nonce.is_some(),
            caps: manifest.caps != 0,
            // Gated on the capability, not just on the data: the stamp is a
            // capability-bearing artifact, so a manifest that carries it must
            // also carry the bit that tells an older binary to refuse the file.
            // Nothing stamps a table before the bit is durable, so the
            // conjunction never silently drops a stamp.
            last_compaction: manifest.caps & crate::format::CAP_PERIODIC_AGE != 0
                && has(|sst| sst.last_compaction_time.is_some()),
            // Same conjunction, for the same reason: a range summary is a
            // capability-bearing artifact, so a manifest carrying one must also
            // carry the bit that tells an older binary to refuse the file.
            // Nothing writes a fragment before the bit is durable, so this
            // never silently drops a summary.
            range: manifest.caps & crate::format::CAP_RANGE_DELETES != 0
                && has(|sst| sst.range_count > 0),
            edits: manifest.generation != 0
                || manifest.applied_through != 0
                || manifest.next_edit_id != 1,
            layout: manifest.wal_layout == WalLayout::Unified,
        }
    }

    /// Whether any tagged tail section is emitted.
    ///
    /// Load-bearing: the positional decoder is gated only on non-emptiness, so
    /// a tag emitted without the three positional sections ahead of it would be
    /// read as a partition name section — silent corruption rather than
    /// rejection. Every new tag must be added here as well as to the encoder.
    fn tagged(self) -> bool {
        self.object || self.nonce || self.caps || self.last_compaction || self.range || self.edits
    }
}

#[cfg(test)]
fn encode_manifest_header(b: &mut Vec<u8>, manifest: &Manifest) {
    append_u32(b, MAGIC);
    // Lowest version that can express this manifest: a database using no
    // capability and no edit log keeps writing v1 bytes forever.
    append_u32(
        b,
        if manifest.caps != 0 || ManifestTailPresence::detect(manifest).edits {
            VERSION_V2
        } else {
            VERSION_V1
        },
    );
    append_u64(b, manifest.next_file_id);
    append_u64(b, manifest.global_seq);
    append_uvarint(b, manifest.cfs.len() as u64);
}

#[cfg(test)]
fn encode_manifest_column_families(b: &mut Vec<u8>, cfs: &[CfManifest]) {
    for cf in cfs {
        append_bytes(b, cf.name.as_bytes());
        append_bytes(b, &cf.config);
        append_uvarint(b, cf.sstables.len() as u64);
        for sst in &cf.sstables {
            append_uvarint(b, sst.id);
            append_uvarint(b, u64::from(sst.level));
            append_uvarint(b, sst.num_entries);
            append_uvarint(b, sst.num_tombstones);
            append_uvarint(b, sst.max_seq);
            append_uvarint(b, sst.klog_size);
            append_uvarint(b, sst.vlog_size);
            append_bytes(b, &sst.min_key);
            append_bytes(b, &sst.max_key);
        }
    }
}

#[cfg(test)]
fn encode_positional_tails(b: &mut Vec<u8>, cfs: &[CfManifest], presence: ManifestTailPresence) {
    // Later positional sections imply all earlier sections. Tagged tails also
    // imply all three so an old positional reader never mistakes a tag for data.
    if presence.partition || presence.tier || presence.time || presence.layout || presence.tagged()
    {
        encode_name_section(b, cfs, |sst| sst.partition.as_deref());
    }
    if presence.tier || presence.time || presence.layout || presence.tagged() {
        encode_name_section(b, cfs, |sst| sst.tier.as_deref());
    }
    if presence.time || presence.layout || presence.tagged() {
        encode_u64_section(b, cfs, |sst| sst.max_entry_time.map(|time| time as u64));
    }
}

#[cfg(test)]
fn encode_tagged_tails(b: &mut Vec<u8>, manifest: &Manifest, presence: ManifestTailPresence) {
    if presence.object {
        b.extend_from_slice(OBJECT_TAG);
        encode_name_section(b, &manifest.cfs, |sst| sst.object.as_deref());
    }
    if let Some(nonce) = manifest.instance_nonce {
        b.extend_from_slice(INSTANCE_TAG);
        append_u64(b, nonce);
    }
    if presence.caps {
        b.extend_from_slice(FORMAT_CAPS_TAG);
        append_u64(b, manifest.caps);
    }
    if presence.range {
        b.extend_from_slice(RANGE_TAG);
        encode_range_section(b, &manifest.cfs);
    }
    if presence.last_compaction {
        b.extend_from_slice(LAST_COMPACTION_TAG);
        encode_u64_section(b, &manifest.cfs, |sst| {
            sst.last_compaction_time.map(|time| time as u64)
        });
    }
    if presence.edits {
        b.extend_from_slice(MANIFEST_EDITS_TAG);
        append_u64(b, manifest.generation);
        append_u64(b, manifest.applied_through);
        append_u64(b, manifest.next_edit_id);
    }
    if presence.layout {
        b.extend_from_slice(WAL_LAYOUT_TAG);
        b.push(1);
    }
}

struct ManifestCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> ManifestCursor<'a> {
    fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(read_u32(self.bytes(4)?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(read_u64(self.bytes(8)?))
    }

    fn uvar(&mut self) -> Result<u64> {
        let (value, used) = uvarint(self.remaining).ok_or_else(corrupt_manifest)?;
        self.remaining = &self.remaining[used..];
        Ok(value)
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.remaining.len() < len {
            return Err(corrupt_manifest());
        }
        let (value, remaining) = self.remaining.split_at(len);
        self.remaining = remaining;
        Ok(value)
    }

    fn byte_vec(&mut self) -> Result<Vec<u8>> {
        let len = self.uvar()? as usize;
        Ok(self.bytes(len)?.to_vec())
    }

    fn into_remaining(self) -> &'a [u8] {
        self.remaining
    }
}

fn verified_manifest_body(data: &[u8]) -> Result<&[u8]> {
    if data.len() < 4 {
        return Err(corrupt_manifest());
    }
    let (body, stored_crc) = data.split_at(data.len() - 4);
    if read_u32(stored_crc) != checksum(body) {
        return Err(corrupt_manifest());
    }
    Ok(body)
}

/// The fixed part of a decoded manifest header.
struct ManifestHeader {
    version: u32,
    next_file_id: u64,
    global_seq: u64,
    column_family_count: usize,
}

fn decode_manifest_header(cursor: &mut ManifestCursor<'_>) -> Result<ManifestHeader> {
    if cursor.u32()? != MAGIC {
        return Err(corrupt_manifest());
    }
    let version = cursor.u32()?;
    if version != VERSION_V1 && version != VERSION_V2 {
        return Err(corrupt_manifest());
    }
    Ok(ManifestHeader {
        version,
        next_file_id: cursor.u64()?,
        global_seq: cursor.u64()?,
        column_family_count: cursor.uvar()? as usize,
    })
}

/// Cap what a count field may pre-allocate. The vector still grows to whatever
/// the bytes actually contain; this only stops a CRC-valid manifest whose count
/// lies from asking for gigabytes before the first element is read.
fn capacity_hint(count: usize) -> usize {
    count.min(4096)
}

fn decode_manifest_column_families(
    cursor: &mut ManifestCursor<'_>,
    count: usize,
) -> Result<Vec<CfManifest>> {
    let mut cfs = Vec::with_capacity(capacity_hint(count));
    for _ in 0..count {
        let name = String::from_utf8(cursor.byte_vec()?).map_err(|_| corrupt_manifest())?;
        let config = cursor.byte_vec()?;
        let table_count = cursor.uvar()? as usize;
        let mut sstables = Vec::with_capacity(capacity_hint(table_count));
        for _ in 0..table_count {
            sstables.push(decode_sstable(cursor)?);
        }
        cfs.push(CfManifest {
            name,
            config,
            sstables,
            unified_id: None,
        });
    }
    Ok(cfs)
}

fn decode_sstable(cursor: &mut ManifestCursor<'_>) -> Result<SstMeta> {
    Ok(SstMeta {
        id: cursor.uvar()?,
        level: cursor.uvar()? as u32,
        num_entries: cursor.uvar()?,
        num_tombstones: cursor.uvar()?,
        max_seq: cursor.uvar()?,
        klog_size: cursor.uvar()?,
        vlog_size: cursor.uvar()?,
        min_key: cursor.byte_vec()?,
        max_key: cursor.byte_vec()?,
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
    })
}

fn decode_positional_tails<'a>(mut p: &'a [u8], cfs: &mut [CfManifest]) -> Result<&'a [u8]> {
    if !p.is_empty() {
        p = decode_name_section(p, cfs, |sst, name| sst.partition = Some(name))?;
    }
    if !p.is_empty() {
        p = decode_name_section(p, cfs, |sst, name| sst.tier = Some(name))?;
    }
    if !p.is_empty() {
        p = decode_u64_section(p, cfs, |sst, value| sst.max_entry_time = Some(value as i64))?;
    }
    Ok(p)
}

/// Everything the tagged-tail section carries.
#[derive(Default)]
struct TaggedTails {
    wal_layout: WalLayout,
    instance_nonce: Option<u64>,
    caps: u64,
    /// Whether [`LAST_COMPACTION_TAG`] was present, checked against `caps`
    /// after the loop — the tag may legally precede or follow the caps word.
    last_compaction: bool,
    /// Whether [`RANGE_TAG`] was present, checked against `caps` the same way.
    range: bool,
    edits: Option<EditLogTail>,
}

/// Payload of [`MANIFEST_EDITS_TAG`].
#[derive(Clone, Copy)]
struct EditLogTail {
    generation: u64,
    applied_through: u64,
    next_edit_id: u64,
}

/// Decode the tagged tail sections by dispatching on each 8-byte tag.
///
/// This rejects exactly what the previous fixed sequence rejected — an unknown
/// or short residual was already `Corruption`, because `decode_wal_layout`
/// demanded an exact 9-byte remainder. The loop shape is what makes the tag set
/// extensible: a new section is one more arm, not another positional hazard.
/// The default arm must stay `Corruption` so a tag this binary does not know is
/// never mistaken for data.
fn decode_tagged_tails(mut p: &[u8], cfs: &mut [CfManifest]) -> Result<TaggedTails> {
    let mut out = TaggedTails::default();
    let mut seen_object = false;
    let mut seen_caps = false;
    let mut seen_layout = false;
    while !p.is_empty() {
        if p.len() < TAG_LEN {
            return Err(corrupt_manifest());
        }
        let (tag, rest) = p.split_at(TAG_LEN);
        p = if tag == OBJECT_TAG {
            // Duplicates are corruption: the encoder emits each tag at most
            // once, and a second copy would silently overwrite the first.
            if std::mem::replace(&mut seen_object, true) {
                return Err(corrupt_manifest());
            }
            decode_name_section(rest, cfs, |sst, name| sst.object = Some(name))?
        } else if tag == INSTANCE_TAG {
            if out.instance_nonce.is_some() {
                return Err(corrupt_manifest());
            }
            if rest.len() < 8 {
                return Err(corrupt_manifest());
            }
            out.instance_nonce = Some(read_u64(rest));
            &rest[8..]
        } else if tag == FORMAT_CAPS_TAG {
            if std::mem::replace(&mut seen_caps, true) {
                return Err(corrupt_manifest());
            }
            if rest.len() < 8 {
                return Err(corrupt_manifest());
            }
            out.caps = read_u64(rest);
            &rest[8..]
        } else if tag == LAST_COMPACTION_TAG {
            if std::mem::replace(&mut out.last_compaction, true) {
                return Err(corrupt_manifest());
            }
            decode_u64_section(rest, cfs, |sst, value| {
                sst.last_compaction_time = Some(value as i64)
            })?
        } else if tag == RANGE_TAG {
            if std::mem::replace(&mut out.range, true) {
                return Err(corrupt_manifest());
            }
            decode_range_section(rest, cfs)?
        } else if tag == MANIFEST_EDITS_TAG {
            if out.edits.is_some() {
                return Err(corrupt_manifest());
            }
            if rest.len() < 24 {
                return Err(corrupt_manifest());
            }
            out.edits = Some(EditLogTail {
                generation: read_u64(&rest[0..8]),
                applied_through: read_u64(&rest[8..16]),
                next_edit_id: read_u64(&rest[16..24]),
            });
            &rest[24..]
        } else if tag == WAL_LAYOUT_TAG {
            if std::mem::replace(&mut seen_layout, true) {
                return Err(corrupt_manifest());
            }
            out.wal_layout = decode_layout_byte(rest)?;
            &rest[1..]
        } else {
            return Err(corrupt_manifest());
        };
    }
    Ok(out)
}

/// Decode the single payload byte of [`WAL_LAYOUT_TAG`]; only `1` is assigned.
fn decode_layout_byte(p: &[u8]) -> Result<WalLayout> {
    match p.first() {
        Some(1) => Ok(WalLayout::Unified),
        _ => Err(corrupt_manifest()),
    }
}

/// Encode one tail section: for each CF in order, a uvarint count of tables
/// carrying a name (as selected by `pick`), then `(table_index, name)` pairs.
#[cfg(test)]
fn encode_name_section(
    b: &mut Vec<u8>,
    cfs: &[CfManifest],
    pick: impl Fn(&SstMeta) -> Option<&str>,
) {
    for cf in cfs {
        let named: Vec<(usize, &str)> = cf
            .sstables
            .iter()
            .enumerate()
            .filter_map(|(i, s)| pick(s).map(|n| (i, n)))
            .collect();
        append_uvarint(b, named.len() as u64);
        for (i, name) in named {
            append_uvarint(b, i as u64);
            append_bytes(b, name.as_bytes());
        }
    }
}

/// Decode one tail section written by [`encode_name_section`], invoking `set`
/// for each `(table, name)` pair. Returns the unconsumed remainder.
fn decode_name_section<'a>(
    mut p: &'a [u8],
    cfs: &mut [CfManifest],
    set: impl Fn(&mut SstMeta, String),
) -> Result<&'a [u8]> {
    let bad = || OndaError::Corruption("manifest: corrupt or invalid".into());
    for cf in cfs.iter_mut() {
        let (count, n) = uvarint(p).ok_or_else(bad)?;
        p = &p[n..];
        for _ in 0..count {
            let (idx, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let (name, rest) = take_bytes(p).ok_or_else(bad)?;
            p = rest;
            let sst = cf.sstables.get_mut(idx as usize).ok_or_else(bad)?;
            set(sst, String::from_utf8(name).map_err(|_| bad())?);
        }
    }
    Ok(p)
}

/// Encode one tail section whose per-table payload is a `u64`: for each CF in
/// order, a uvarint count of tables carrying a value (as selected by `pick`),
/// then `(table_index, value)` uvarint pairs. Mirrors [`encode_name_section`]
/// with a numeric payload in place of a byte string.
#[cfg(test)]
fn encode_u64_section(b: &mut Vec<u8>, cfs: &[CfManifest], pick: impl Fn(&SstMeta) -> Option<u64>) {
    for cf in cfs {
        let valued: Vec<(usize, u64)> = cf
            .sstables
            .iter()
            .enumerate()
            .filter_map(|(i, s)| pick(s).map(|v| (i, v)))
            .collect();
        append_uvarint(b, valued.len() as u64);
        for (i, v) in valued {
            append_uvarint(b, i as u64);
            append_uvarint(b, v);
        }
    }
}

/// Decode one tail section written by [`encode_u64_section`], invoking `set`
/// for each `(table, value)` pair. Returns the unconsumed remainder.
fn decode_u64_section<'a>(
    mut p: &'a [u8],
    cfs: &mut [CfManifest],
    set: impl Fn(&mut SstMeta, u64),
) -> Result<&'a [u8]> {
    let bad = || OndaError::Corruption("manifest: corrupt or invalid".into());
    for cf in cfs.iter_mut() {
        let (count, n) = uvarint(p).ok_or_else(bad)?;
        p = &p[n..];
        for _ in 0..count {
            let (idx, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let (val, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let sst = cf.sstables.get_mut(idx as usize).ok_or_else(bad)?;
            set(sst, val);
        }
    }
    Ok(p)
}

/// Encode the [`RANGE_TAG`] section: for each CF in order, a uvarint count of
/// tables carrying fragments, then one record per such table.
///
/// ```text
/// per CF: count uvarint
///         { table_index uvarint | range_count uvarint
///           | range_min_seq uvarint | range_max_seq uvarint
///           | min_key bytes | max_key bytes } x count
/// ```
///
/// The bounds are unconditional rather than optional: a table with
/// `range_count > 0` has both by construction (a fragment has two bounds), so
/// an optional tag would encode a state no writer can produce.
#[cfg(test)]
fn encode_range_section(b: &mut Vec<u8>, cfs: &[CfManifest]) {
    for cf in cfs {
        let with_ranges: Vec<(usize, &SstMeta)> = cf
            .sstables
            .iter()
            .enumerate()
            .filter(|(_, s)| s.range_count > 0)
            .collect();
        append_uvarint(b, with_ranges.len() as u64);
        for (i, sst) in with_ranges {
            append_uvarint(b, i as u64);
            append_uvarint(b, sst.range_count);
            append_uvarint(b, sst.range_min_seq);
            append_uvarint(b, sst.range_max_seq);
            append_bytes(b, sst.range_min_key.as_deref().unwrap_or_default());
            append_bytes(b, sst.range_max_key.as_deref().unwrap_or_default());
        }
    }
}

/// Decode the section written by [`encode_range_section`]. Returns the
/// unconsumed remainder.
fn decode_range_section<'a>(mut p: &'a [u8], cfs: &mut [CfManifest]) -> Result<&'a [u8]> {
    let bad = corrupt_manifest;
    for cf in cfs.iter_mut() {
        let (count, n) = uvarint(p).ok_or_else(bad)?;
        p = &p[n..];
        for _ in 0..count {
            let (idx, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let (range_count, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let (min_seq, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let (max_seq, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let (min_key, rest) = take_bytes(p).ok_or_else(bad)?;
            let (max_key, rest) = take_bytes(rest).ok_or_else(bad)?;
            p = rest;
            // A record naming zero fragments, or an empty bound, is a state the
            // encoder never emits: it would be indistinguishable from absence.
            if range_count == 0 || min_key.is_empty() || max_key.is_empty() || min_seq > max_seq {
                return Err(bad());
            }
            let sst = cf.sstables.get_mut(idx as usize).ok_or_else(bad)?;
            sst.range_count = range_count;
            sst.range_min_seq = min_seq;
            sst.range_max_seq = max_seq;
            sst.range_min_key = Some(min_key);
            sst.range_max_key = Some(max_key);
        }
    }
    Ok(p)
}

#[cfg(test)]
fn append_bytes(dst: &mut Vec<u8>, b: &[u8]) {
    append_uvarint(dst, b.len() as u64);
    dst.extend_from_slice(b);
}

fn take_bytes(p: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let (n64, n) = uvarint(p)?;
    let p = &p[n..];
    let len = n64 as usize;
    if p.len() < len {
        return None;
    }
    Some((p[..len].to_vec(), &p[len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Names of every frozen VERSION-1 manifest fixture, in emission order of
    /// the tail sections they exercise.
    const V1_FIXTURES: &[&str] = &[
        "manifest_v1_notail.bin",
        "manifest_v1_partition.bin",
        "manifest_v1_tier.bin",
        "manifest_v1_time.bin",
        "manifest_v1_unified.bin",
        "manifest_v1_object.bin",
        "manifest_v1_nonce.bin",
    ];

    /// Read a frozen fixture and re-checksum it with `extra` spliced in before
    /// the trailing CRC, so the tail decoder — not the CRC — is what rejects.
    fn fixture_with_extra_tail(name: &str, extra: &[u8]) -> Vec<u8> {
        let bytes = std::fs::read(crate::util::legacy_fixture(name)).unwrap();
        let mut body = bytes[..bytes.len() - 4].to_vec();
        body.extend_from_slice(extra);
        let crc = checksum(&body);
        append_u32(&mut body, crc);
        body
    }

    /// The manifest decoder must be total over arbitrary bytes: a `Result`,
    /// never a panic. The mutated body is re-checksummed on purpose — a
    /// CRC-valid manifest whose interior lies is exactly the class of input the
    /// whole-file CRC cannot catch.
    #[test]
    fn fuzz_manifest_decode_never_panics() {
        let seeds: Vec<Vec<u8>> = V1_FIXTURES
            .iter()
            .chain(std::iter::once(&V2_FIXTURE))
            .map(|n| std::fs::read(crate::util::legacy_fixture(n)).unwrap())
            .collect();
        let mut rng = crate::util::FuzzRng::new(0x2545_F491_4F6C_DD1D);
        for seed in &seeds {
            for _ in 0..2000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                let _ = decode(&case);
                if case.len() > 4 {
                    let mut body = case[..case.len() - 4].to_vec();
                    let crc = checksum(&body);
                    append_u32(&mut body, crc);
                    let _ = decode(&body);
                }
            }
        }
    }

    #[test]
    fn all_v1_tail_fixtures_decode_identically() {
        for name in V1_FIXTURES {
            let bytes = std::fs::read(crate::util::legacy_fixture(name)).unwrap();
            let m = decode(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(
                encode(&m),
                bytes,
                "{name}: re-encode must be byte-identical"
            );
        }
    }

    #[test]
    fn unknown_tag_is_corruption() {
        let bytes = fixture_with_extra_tail("manifest_v1_object.bin", b"ONDAXXX1\0\0\0\0\0\0\0\0");
        let err = decode(&bytes).expect_err("an unknown tail tag must fail closed");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn duplicate_tag_is_corruption() {
        let mut extra = INSTANCE_TAG.to_vec();
        extra.extend_from_slice(&[0u8; 8]);
        let bytes = fixture_with_extra_tail("manifest_v1_nonce.bin", &extra);
        let err = decode(&bytes).expect_err("a repeated tail tag must fail closed");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn short_residual_is_corruption() {
        let bytes = fixture_with_extra_tail("manifest_v1_object.bin", b"ONDAWAL");
        let err = decode(&bytes).expect_err("a short tail residual must fail closed");
        assert_eq!(err.kind(), "corruption");
    }

    /// The v2 fixture is the case the positional decoder gets wrong if
    /// `ManifestTailPresence::tagged()` is not extended, so it is frozen too.
    const V2_FIXTURE: &str = "manifest_v2_caps_only.bin";

    /// Version field of an encoded manifest (bytes 4..8).
    fn encoded_version(bytes: &[u8]) -> u32 {
        read_u32(&bytes[4..8])
    }

    /// Byte offset of `tag` inside an encoded manifest.
    fn tag_offset(bytes: &[u8], tag: &[u8; 8]) -> Option<usize> {
        bytes.windows(TAG_LEN).position(|w| w == tag)
    }

    // ---- range summary (ONDARNG1, 1.2) ------------------------------------

    /// A `sample()` whose second table carries range fragments.
    fn sample_with_ranges() -> Manifest {
        let mut m = sample();
        m.caps = crate::format::CAP_RANGE_DELETES | crate::format::CAP_EXTENDED_RECORDS;
        let t = &mut m.cfs[0].sstables[1];
        t.range_count = 3;
        t.range_min_seq = 17;
        t.range_max_seq = 42;
        // Deliberately OUTSIDE the point bounds in both directions: fragments
        // are clipped to the output *interval*, which reaches past the first
        // and last point key of the table that owns it.
        t.range_min_key = Some(b"aa".to_vec());
        t.range_max_key = Some(b"nnn".to_vec());
        m
    }

    #[test]
    fn sst_meta_range_tail_round_trips() {
        let m = sample_with_ranges();
        let enc = encode(&m);
        assert_eq!(encoded_version(&enc), VERSION_V2);
        let d = decode(&enc).unwrap();
        let t = &d.cfs[0].sstables[1];
        assert_eq!(t.range_count, 3);
        assert_eq!(t.range_min_seq, 17);
        assert_eq!(t.range_max_seq, 42);
        assert_eq!(t.range_min_key.as_deref(), Some(&b"aa"[..]));
        assert_eq!(t.range_max_key.as_deref(), Some(&b"nnn"[..]));
        // The table with no fragments stays entirely at its defaults.
        let t0 = &d.cfs[0].sstables[0];
        assert_eq!(t0.range_count, 0);
        assert_eq!(t0.range_min_key, None);
        assert_eq!(encode(&d), enc, "re-encode must be byte-identical");
        // Position: after ONDACAP1, before ONDAWAL1.
        let caps = tag_offset(&enc, FORMAT_CAPS_TAG).expect("caps tag");
        let range = tag_offset(&enc, RANGE_TAG).expect("range tag");
        assert!(caps < range, "ONDARNG1 must follow ONDACAP1");
    }

    /// The `ManifestTailPresence::tagged()` hazard, mirroring 1.0's caps test:
    /// a manifest whose ONLY tail is the range summary must still emit the
    /// three positional sections ahead of it, or a positional decoder reads
    /// `ONDARNG1` as a partition name section.
    #[test]
    fn range_tail_only_manifest_emits_all_positional_sections() {
        let m = sample_with_ranges();
        assert_eq!(m.wal_layout, WalLayout::PerColumnFamily, "no layout tail");
        assert!(m.instance_nonce.is_none(), "no nonce tail");
        let enc = encode(&m);
        let range = tag_offset(&enc, RANGE_TAG).expect("range tag");
        // Everything between the CF bodies and the first tag is positional, and
        // the tag must not be the first thing after them.
        let presence = ManifestTailPresence::detect(&m);
        assert!(presence.range && presence.tagged());
        let mut positional = Vec::new();
        encode_positional_tails(&mut positional, &m.cfs, presence);
        assert!(
            !positional.is_empty(),
            "a tag-only manifest must still carry the three positional sections"
        );
        assert!(
            enc[..range]
                .windows(positional.len())
                .any(|w| w == positional.as_slice()),
            "the positional sections must appear ahead of ONDARNG1"
        );
        // And it round-trips through the real decoder.
        assert_eq!(encode(&decode(&enc).unwrap()), enc);
    }

    /// Every table of every frozen legacy fixture decodes to "no fragments",
    /// and the field never appears in the bytes.
    #[test]
    fn legacy_table_reports_range_count_zero() {
        for name in V1_FIXTURES.iter().chain([V2_FIXTURE].iter()) {
            let bytes = std::fs::read(crate::util::legacy_fixture(name)).unwrap();
            let m = decode(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(tag_offset(&bytes, RANGE_TAG).is_none(), "{name}");
            for cf in &m.cfs {
                for t in &cf.sstables {
                    assert_eq!(t.range_count, 0, "{name} table {}", t.id);
                    assert_eq!(t.range_min_key, None, "{name} table {}", t.id);
                    assert_eq!(t.range_max_key, None, "{name} table {}", t.id);
                    assert!(!t.has_ranges(), "{name} table {}", t.id);
                }
            }
        }
    }

    /// The summary is a capability-bearing artifact: bytes carrying it without
    /// `CAP_RANGE_DELETES` were truncated or hand-edited.
    #[test]
    fn range_tail_without_the_capability_is_corruption() {
        let mut m = sample_with_ranges();
        let enc = encode(&m);
        // Rewrite the caps word in place, clearing only CAP_RANGE_DELETES.
        let caps_at = tag_offset(&enc, FORMAT_CAPS_TAG).unwrap() + TAG_LEN;
        let mut bad = enc.clone();
        let stripped = m.caps & !crate::format::CAP_RANGE_DELETES;
        bad[caps_at..caps_at + 8].copy_from_slice(&stripped.to_le_bytes());
        let crc_at = bad.len() - 4;
        let crc = checksum(&bad[..crc_at]);
        bad[crc_at..].copy_from_slice(&crc.to_le_bytes());
        let err = decode(&bad).expect_err("a summary without the bit must fail closed");
        assert_eq!(err.kind(), "corruption");
        // And with the bit cleared at the source, the summary is simply not
        // emitted — the encoder never writes an unauthorized artifact.
        m.caps = stripped;
        assert!(tag_offset(&encode(&m), RANGE_TAG).is_none());
    }

    /// A record naming zero fragments, or an empty bound, is a state the
    /// encoder cannot produce.
    #[test]
    fn range_section_rejects_unwritable_records() {
        let mut cfs = vec![CfManifest {
            name: "x".into(),
            config: Vec::new(),
            sstables: vec![SstMeta::default()],
            unified_id: None,
        }];
        // count = 1, index = 0, range_count = 0 -> refused.
        let mut p = Vec::new();
        append_uvarint(&mut p, 1);
        append_uvarint(&mut p, 0);
        append_uvarint(&mut p, 0);
        append_uvarint(&mut p, 1);
        append_uvarint(&mut p, 2);
        append_bytes(&mut p, b"a");
        append_bytes(&mut p, b"z");
        assert!(decode_range_section(&p, &mut cfs).is_err());
        // A table index past the end of the CF.
        let mut p = Vec::new();
        append_uvarint(&mut p, 1);
        append_uvarint(&mut p, 9);
        append_uvarint(&mut p, 1);
        append_uvarint(&mut p, 1);
        append_uvarint(&mut p, 2);
        append_bytes(&mut p, b"a");
        append_bytes(&mut p, b"z");
        assert!(decode_range_section(&p, &mut cfs).is_err());
    }

    #[test]
    fn edits_tail_round_trips() {
        let mut m = sample();
        m.caps = crate::format::CAP_MANIFEST_EDITS;
        m.generation = 5;
        m.applied_through = 4_096;
        m.next_edit_id = 4_097;
        let enc = encode(&m);
        assert_eq!(encoded_version(&enc), VERSION_V2);
        let d = decode(&enc).unwrap();
        assert_eq!(d.generation, 5);
        assert_eq!(d.applied_through, 4_096);
        assert_eq!(d.next_edit_id, 4_097);
        assert_eq!(encode(&d), enc);
    }

    /// The tail's bytes are the interoperability contract: tag, then three
    /// little-endian `u64`s in declaration order, sitting after `ONDACAP1` and
    /// before `ONDAWAL1`.
    #[test]
    fn edits_tail_bytes_are_frozen() {
        let mut m = sample();
        m.caps = crate::format::CAP_MANIFEST_EDITS;
        m.wal_layout = WalLayout::Unified;
        m.generation = 0x0102_0304_0506_0708;
        m.applied_through = 6;
        m.next_edit_id = 7;
        let enc = encode(&m);
        let at = tag_offset(&enc, MANIFEST_EDITS_TAG).expect("the tag must be present");
        assert_eq!(&enc[at..at + 8], b"ONDAMED1");
        assert_eq!(&enc[at + 8..at + 16], &[8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(read_u64(&enc[at + 16..at + 24]), 6);
        assert_eq!(read_u64(&enc[at + 24..at + 32]), 7);
        // Placement: after the capability tag, before the WAL-layout tag.
        assert!(tag_offset(&enc, FORMAT_CAPS_TAG).unwrap() < at);
        assert!(at < tag_offset(&enc, WAL_LAYOUT_TAG).unwrap());
    }

    #[test]
    fn edits_tail_coexists_with_object_and_nonce_tags() {
        let mut m = sample();
        m.caps = crate::format::CAP_MANIFEST_EDITS;
        m.instance_nonce = Some(0xABCD_EF01_2345_6789);
        m.cfs[0].sstables[0].object = Some("cf-default/1".into());
        m.cfs[0].sstables[1].tier = Some("cold".into());
        m.generation = 2;
        m.applied_through = 9;
        m.next_edit_id = 10;
        let enc = encode(&m);
        let d = decode(&enc).unwrap();
        assert_eq!(d.instance_nonce, m.instance_nonce);
        assert_eq!(d.cfs[0].sstables[0].object.as_deref(), Some("cf-default/1"));
        assert_eq!(d.cfs[0].sstables[1].tier.as_deref(), Some("cold"));
        assert_eq!(
            (d.generation, d.applied_through, d.next_edit_id),
            (2, 9, 10)
        );
        assert_eq!(encode(&d), enc);
        assert!(
            tag_offset(&enc, OBJECT_TAG).unwrap() < tag_offset(&enc, MANIFEST_EDITS_TAG).unwrap()
        );
    }

    /// Every manifest ever written before 2.2 lacks the tail, and must decode
    /// as "no log yet" rather than as an inconsistent cursor.
    #[test]
    fn a_manifest_without_the_edits_tail_decodes_as_generation_zero() {
        for name in V1_FIXTURES {
            let bytes = std::fs::read(crate::util::legacy_fixture(name)).unwrap();
            let m = decode(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(m.generation, 0, "{name}");
            assert_eq!(m.applied_through, 0, "{name}");
            assert_eq!(m.next_edit_id, 1, "{name}");
            assert_eq!(encode(&m), bytes, "{name}: still byte-identical");
        }
    }

    /// The tail is a v2 feature, so a VERSION-1 manifest carrying it is bytes
    /// no writer produces — the same fence the capability word gets.
    #[test]
    fn edits_tail_under_version_1_is_corruption() {
        let mut extra = MANIFEST_EDITS_TAG.to_vec();
        extra.extend_from_slice(&[0u8; 24]);
        let bytes = fixture_with_extra_tail("manifest_v1_object.bin", &extra);
        assert_eq!(
            decode(&bytes).unwrap_err().kind(),
            "corruption",
            "an edit-log tail under VERSION 1 must fail closed"
        );
    }

    #[test]
    fn duplicate_edits_tag_is_corruption() {
        let mut m = sample();
        m.caps = crate::format::CAP_MANIFEST_EDITS;
        m.generation = 1;
        m.applied_through = 1;
        m.next_edit_id = 2;
        let enc = encode(&m);
        let mut body = enc[..enc.len() - 4].to_vec();
        body.extend_from_slice(MANIFEST_EDITS_TAG);
        body.extend_from_slice(&[0u8; 24]);
        let crc = checksum(&body);
        append_u32(&mut body, crc);
        assert_eq!(decode(&body).unwrap_err().kind(), "corruption");
    }

    /// `next_edit_id` is always one past `applied_through`; any other pairing
    /// is a lie no writer tells.
    #[test]
    fn an_inconsistent_edit_cursor_is_corruption() {
        let mut m = sample();
        m.caps = crate::format::CAP_MANIFEST_EDITS;
        m.generation = 1;
        m.applied_through = 10;
        m.next_edit_id = 10;
        let enc = encode(&m);
        assert_eq!(decode(&enc).unwrap_err().kind(), "corruption");
    }

    #[test]
    fn caps_tail_round_trips() {
        let mut m = sample();
        m.caps = crate::format::CAP_EXTENDED_RECORDS | crate::format::CAP_PERIODIC_AGE;
        let enc = encode(&m);
        assert_eq!(encoded_version(&enc), VERSION_V2);
        let d = decode(&enc).unwrap();
        assert_eq!(d.caps, m.caps);
        // The tag sits between ONDAINS1 and ONDAWAL1, and the positional
        // sections still round-trip beside it.
        assert_eq!(d.cfs[0].sstables[1].partition.as_deref(), Some("img"));
        assert_eq!(encode(&d), enc);
    }

    /// A caps word with no partition/tier/time data must still emit all three
    /// positional sections ahead of the tag, or the positional decoder reads
    /// `ONDACAP1` as a partition name section.
    #[test]
    fn caps_only_manifest_emits_all_positional_sections() {
        let mut m = sample();
        for s in &mut m.cfs[0].sstables {
            s.partition = None;
        }
        m.caps = crate::format::CAP_EXTENDED_RECORDS;
        let enc = encode(&m);

        // Three all-empty positional sections (one uvarint count per CF, and
        // the sample has one CF) precede the tag.
        let tag_at = enc
            .windows(TAG_LEN)
            .position(|w| w == FORMAT_CAPS_TAG)
            .expect("the caps tag must be emitted");
        assert_eq!(&enc[tag_at - 3..tag_at], &[0u8, 0, 0], "empty sections");

        let d = decode(&enc).unwrap();
        assert_eq!(d.caps, crate::format::CAP_EXTENDED_RECORDS);
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));
        assert!(d.cfs[0].sstables.iter().all(|s| s.tier.is_none()));
        assert!(d.cfs[0].sstables.iter().all(|s| s.max_entry_time.is_none()));
    }

    #[test]
    fn zero_caps_writes_version_1() {
        let enc = encode(&sample());
        assert_eq!(encoded_version(&enc), VERSION_V1);
        assert!(
            !enc.windows(TAG_LEN).any(|w| w == FORMAT_CAPS_TAG),
            "an unused caps tag must not leak into the encoding"
        );
    }

    #[test]
    fn unknown_caps_bit_is_unsupported_format() {
        let mut m = sample();
        m.caps = 1 << 40; // never assigned
        let err = decode(&encode(&m)).expect_err("unknown caps must fail closed");
        assert_eq!(err.kind(), "unsupported_format");
    }

    /// A caps tail in a v1 manifest is a contradiction: the encoder bumps the
    /// version exactly when it emits the tag, so these bytes were tampered with.
    #[test]
    fn caps_tag_under_version_1_is_corruption() {
        let mut m = sample();
        m.caps = crate::format::CAP_EXTENDED_RECORDS;
        let mut enc = encode(&m);
        let body_len = enc.len() - 4;
        enc.truncate(body_len);
        enc[4..8].copy_from_slice(&VERSION_V1.to_le_bytes());
        let crc = checksum(&enc);
        append_u32(&mut enc, crc);
        let err = decode(&enc).expect_err("a v1 manifest may not carry caps");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn duplicate_caps_tag_is_corruption() {
        let mut extra = FORMAT_CAPS_TAG.to_vec();
        extra.extend_from_slice(&1u64.to_le_bytes());
        let bytes = fixture_with_extra_tail(V2_FIXTURE, &extra);
        let err = decode(&bytes).expect_err("a repeated caps tag must fail closed");
        assert_eq!(err.kind(), "corruption");
    }

    /// The frozen v2 fixture decodes to the documented value and re-encodes to
    /// exactly the committed bytes.
    #[test]
    fn frozen_v2_caps_fixture_round_trips() {
        let bytes = std::fs::read(crate::util::legacy_fixture(V2_FIXTURE)).unwrap();
        assert_eq!(encoded_version(&bytes), VERSION_V2);
        let m = decode(&bytes).unwrap();
        assert_eq!(m.caps, crate::format::CAP_EXTENDED_RECORDS);
        assert!(m.cfs.iter().all(|cf| cf
            .sstables
            .iter()
            .all(|s| s.partition.is_none() && s.tier.is_none() && s.max_entry_time.is_none())));
        assert_eq!(encode(&m), bytes);
    }

    // ---- 0.3: periodic-compaction age state -------------------------------

    /// Task 1's first test: the stamp survives a save/load, per table, with
    /// `Some` and `None` mixed inside one column family — and the encoding is
    /// stable enough to re-emit byte-identically.
    #[test]
    fn last_compaction_time_round_trips() {
        let mut m = sample();
        m.caps = crate::format::CAP_PERIODIC_AGE;
        m.cfs[0].sstables[0].last_compaction_time = Some(1_700_000_000_000_000_000);
        m.cfs[0].sstables[1].last_compaction_time = None;
        let enc = encode(&m);
        assert_eq!(encoded_version(&enc), VERSION_V2);
        assert!(enc.windows(TAG_LEN).any(|w| w == LAST_COMPACTION_TAG));

        let d = decode(&enc).unwrap();
        assert_eq!(
            d.cfs[0].sstables[0].last_compaction_time,
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(d.cfs[0].sstables[1].last_compaction_time, None);
        // The age stamp must not have leaked into the mover's field.
        assert!(d.cfs[0].sstables.iter().all(|s| s.max_entry_time.is_none()));
        assert_eq!(encode(&d), enc);

        // All-`None` under the same capability emits no tail at all.
        let mut none = sample();
        none.caps = crate::format::CAP_PERIODIC_AGE;
        let enc = encode(&none);
        assert!(!enc.windows(TAG_LEN).any(|w| w == LAST_COMPACTION_TAG));
        let d = decode(&enc).unwrap();
        assert!(d.cfs[0]
            .sstables
            .iter()
            .all(|s| s.last_compaction_time.is_none()));
    }

    /// Every manifest written before 0.3 decodes to `None`, which the picker
    /// reads as "unknown, therefore never eligible".
    #[test]
    fn legacy_manifest_decodes_last_compaction_time_as_none() {
        for name in V1_FIXTURES.iter().chain(std::iter::once(&V2_FIXTURE)) {
            let bytes = std::fs::read(crate::util::legacy_fixture(name)).unwrap();
            let m = decode(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(
                m.cfs
                    .iter()
                    .all(|cf| cf.sstables.iter().all(|s| s.last_compaction_time.is_none())),
                "{name}: a pre-0.3 manifest carries no age state"
            );
            // And the field's mere existence must not change the bytes.
            assert_eq!(encode(&m), bytes, "{name}: re-encode must be identical");
        }
    }

    /// The stamp is capability-bearing: bytes carrying it without
    /// `CAP_PERIODIC_AGE` were tampered with, and must fail closed rather than
    /// be read as valid age state.
    #[test]
    fn last_compaction_tail_without_capability_is_corruption() {
        let mut extra = LAST_COMPACTION_TAG.to_vec();
        // One CF in the fixture, one table carrying a stamp.
        append_uvarint(&mut extra, 1);
        append_uvarint(&mut extra, 0);
        append_uvarint(&mut extra, 7);
        let bytes = fixture_with_extra_tail(V2_FIXTURE, &extra);
        let err = decode(&bytes).expect_err("age state needs its capability bit");
        assert_eq!(err.kind(), "corruption");
    }

    #[test]
    fn duplicate_last_compaction_tag_is_corruption() {
        let mut m = sample();
        m.caps = crate::format::CAP_PERIODIC_AGE;
        m.cfs[0].sstables[0].last_compaction_time = Some(5);
        let enc = encode(&m);
        let mut body = enc[..enc.len() - 4].to_vec();
        body.extend_from_slice(LAST_COMPACTION_TAG);
        append_uvarint(&mut body, 0);
        let crc = checksum(&body);
        append_u32(&mut body, crc);
        let err = decode(&body).expect_err("a repeated age tag must fail closed");
        assert_eq!(err.kind(), "corruption");
    }

    fn sample() -> Manifest {
        Manifest {
            next_file_id: 42,
            global_seq: 99,
            generation: 0,
            applied_through: 0,
            next_edit_id: 1,
            wal_layout: WalLayout::PerColumnFamily,
            instance_nonce: None,
            caps: 0,
            cfs: vec![CfManifest {
                name: "default".into(),
                config: vec![1, 2, 3, 4],
                sstables: vec![
                    SstMeta {
                        id: 1,
                        level: 0,
                        num_entries: 100,
                        num_tombstones: 5,
                        max_seq: 50,
                        klog_size: 4096,
                        vlog_size: 0,
                        min_key: b"aaa".to_vec(),
                        max_key: b"zzz".to_vec(),
                        partition: None,
                        tier: None,
                        max_entry_time: None,
                        object: None,
                        last_compaction_time: None,
                        ..Default::default()
                    },
                    SstMeta {
                        id: 2,
                        level: 1,
                        num_entries: 200,
                        num_tombstones: 0,
                        max_seq: 60,
                        klog_size: 8192,
                        vlog_size: 1024,
                        min_key: b"aaa".to_vec(),
                        max_key: b"mmm".to_vec(),
                        partition: Some("img".into()),
                        tier: None,
                        max_entry_time: None,
                        object: None,
                        last_compaction_time: None,
                        ..Default::default()
                    },
                ],
                unified_id: None,
            }],
        }
    }

    #[test]
    fn every_checksummed_manifest_truncation_is_panic_free() {
        let encoded = encode(&sample());
        let body_len = encoded.len() - 4;

        for cut in 0..body_len {
            let mut truncated = encoded[..cut].to_vec();
            let crc = checksum(&truncated);
            append_u32(&mut truncated, crc);
            let decoded = std::panic::catch_unwind(|| decode(&truncated));
            assert!(decoded.is_ok(), "decoder panicked at body length {cut}");
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let m = sample();
        let enc = encode(&m);
        let d = decode(&enc).unwrap();
        assert_eq!(d.next_file_id, 42);
        assert_eq!(d.global_seq, 99);
        assert_eq!(d.cfs.len(), 1);
        assert_eq!(d.cfs[0].name, "default");
        assert_eq!(d.cfs[0].config, vec![1, 2, 3, 4]);
        assert_eq!(d.cfs[0].sstables, m.cfs[0].sstables);
    }

    #[test]
    fn legacy_manifest_decodes_as_per_column_family_wal_layout() {
        let d = decode(&encode(&sample())).unwrap();
        assert_eq!(d.wal_layout, WalLayout::PerColumnFamily);
    }

    #[test]
    fn unified_wal_layout_survives_manifest_round_trip() {
        let mut m = sample();
        m.wal_layout = WalLayout::Unified;
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d.wal_layout, WalLayout::Unified);
        assert_eq!(d.cfs[0].sstables, m.cfs[0].sstables);
    }

    #[test]
    fn corruption_detected() {
        let m = sample();
        let mut enc = encode(&m);
        let n = enc.len();
        enc[n / 2] ^= 0xFF;
        assert!(decode(&enc).is_err());
    }

    #[test]
    fn partition_survives_round_trip() {
        // The sample tags table #2 with partition "img"; #1 has none.
        let d = decode(&encode(&sample())).unwrap();
        assert_eq!(d.cfs[0].sstables[0].partition, None);
        assert_eq!(d.cfs[0].sstables[1].partition.as_deref(), Some("img"));
    }

    #[test]
    fn no_partition_manifest_is_byte_identical_to_legacy() {
        // A manifest with no partitions must not emit the tail section, so its
        // bytes match what a pre-partition build would have written (and thus
        // decodes to all-None). We simulate the legacy encoding by stripping
        // partitions and confirming the encoding is unchanged.
        let mut m = sample();
        for cf in &mut m.cfs {
            for s in &mut cf.sstables {
                s.partition = None;
            }
        }
        let enc = encode(&m);
        let d = decode(&enc).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));
    }

    #[test]
    fn tier_survives_round_trip_alongside_partition() {
        // Tag table #2 with a tier; it already carries partition "img". Both the
        // partition and the tier must survive independently.
        let mut m = sample();
        m.cfs[0].sstables[1].tier = Some("hdd".into());
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d.cfs[0].sstables[0].partition, None);
        assert_eq!(d.cfs[0].sstables[0].tier, None);
        assert_eq!(d.cfs[0].sstables[1].partition.as_deref(), Some("img"));
        assert_eq!(d.cfs[0].sstables[1].tier.as_deref(), Some("hdd"));
    }

    #[test]
    fn tier_without_any_partition_round_trips() {
        // A table may carry a tier with no partition tag at all: the encoder then
        // emits an (all-empty) partition section followed by the tier section, and
        // the decoder must still read the tier back and leave partitions None.
        let mut m = sample();
        for s in &mut m.cfs[0].sstables {
            s.partition = None;
        }
        m.cfs[0].sstables[0].tier = Some("hdd".into());
        let d = decode(&encode(&m)).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));
        assert_eq!(d.cfs[0].sstables[0].tier.as_deref(), Some("hdd"));
        assert_eq!(d.cfs[0].sstables[1].tier, None);
    }

    #[test]
    fn p1_manifest_with_partition_only_decodes_tier_to_none() {
        // A P1-era manifest carries the partition section but no tier section.
        // Decoding it under the tier-aware format must leave every `tier` None
        // (the partition section consumes the whole tail, so no tier bytes remain).
        let m = sample(); // table #2 tagged "img", no tiers anywhere
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d.cfs[0].sstables[1].partition.as_deref(), Some("img"));
        assert!(d.cfs[0].sstables.iter().all(|s| s.tier.is_none()));
    }

    #[test]
    fn max_entry_time_survives_round_trip_alongside_partition_and_tier() {
        let mut m = sample();
        m.cfs[0].sstables[0].max_entry_time = Some(1_700_000_000_000_000_000);
        m.cfs[0].sstables[1].tier = Some("hdd".into());
        m.cfs[0].sstables[1].max_entry_time = Some(1_650_000_000_000_000_000);
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(
            d.cfs[0].sstables[0].max_entry_time,
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(d.cfs[0].sstables[0].tier, None);
        assert_eq!(d.cfs[0].sstables[1].partition.as_deref(), Some("img"));
        assert_eq!(d.cfs[0].sstables[1].tier.as_deref(), Some("hdd"));
        assert_eq!(
            d.cfs[0].sstables[1].max_entry_time,
            Some(1_650_000_000_000_000_000)
        );
    }

    #[test]
    fn max_entry_time_without_partition_or_tier_round_trips() {
        // A table may carry only a max_entry_time (freshly flushed, never
        // partitioned or moved): the encoder emits all-empty partition and tier
        // sections ahead of the time section, and the decoder reads the time back
        // while leaving partition/tier None.
        let mut m = sample();
        for s in &mut m.cfs[0].sstables {
            s.partition = None;
        }
        m.cfs[0].sstables[0].max_entry_time = Some(42);
        let d = decode(&encode(&m)).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));
        assert!(d.cfs[0].sstables.iter().all(|s| s.tier.is_none()));
        assert_eq!(d.cfs[0].sstables[0].max_entry_time, Some(42));
        assert_eq!(d.cfs[0].sstables[1].max_entry_time, None);
    }

    #[test]
    fn p3_manifest_without_time_section_decodes_time_to_none() {
        // The sample tags a partition but no times; decoding under the
        // time-aware format must leave every max_entry_time None.
        let d = decode(&encode(&sample())).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.max_entry_time.is_none()));
    }

    #[test]
    fn legacy_manifest_without_tail_decodes_to_none() {
        // Build the body exactly as a pre-partition writer would: encode a
        // partition-free manifest (which emits no tail), then confirm a decoder
        // that now understands partitions reads every table as None. Adding a
        // partition and re-encoding must produce a strictly longer blob (the
        // tail), proving the tail is the only new on-disk data.
        let mut legacy = sample();
        for cf in &mut legacy.cfs {
            for s in &mut cf.sstables {
                s.partition = None;
            }
        }
        let legacy_enc = encode(&legacy);
        let d = decode(&legacy_enc).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));

        let with_part = sample(); // table #2 tagged "img"
        assert!(
            encode(&with_part).len() > legacy_enc.len(),
            "partition tail must add bytes on top of the legacy body"
        );
    }

    #[test]
    fn object_and_nonce_survive_round_trip() {
        let mut m = sample();
        m.instance_nonce = Some(0xdead_beef_cafe_f00d);
        m.cfs[0].sstables[1].tier = Some("cas".into());
        m.cfs[0].sstables[1].object = Some("cf-default/00c0ffee-7".into());
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d.instance_nonce, Some(0xdead_beef_cafe_f00d));
        assert_eq!(d.cfs[0].sstables[0].object, None);
        assert_eq!(
            d.cfs[0].sstables[1].object.as_deref(),
            Some("cf-default/00c0ffee-7")
        );
        // the positional sections still round-trip beside the tags
        assert_eq!(d.cfs[0].sstables[1].partition.as_deref(), Some("img"));
        assert_eq!(d.cfs[0].sstables[1].tier.as_deref(), Some("cas"));
    }

    #[test]
    fn manifest_without_objects_or_nonce_stays_pre_a2_byte_identical() {
        // The A2 tags must not appear unless used: a database that never
        // declares a shared tier keeps writing manifests a pre-A2 binary
        // reads. Byte-level check: no tag magic anywhere in the encoding.
        let enc = encode(&sample());
        for tag in [&b"ONDAOBJ1"[..], &b"ONDAINS1"[..]] {
            assert!(
                !enc.windows(tag.len()).any(|w| w == tag),
                "unused A2 tag leaked into the manifest encoding"
            );
        }
    }

    #[test]
    fn nonce_alone_round_trips_with_empty_positional_sections() {
        // A nonce can be minted before anything is partitioned or tiered; the
        // encoder then emits all-empty positional sections ahead of the tag
        // (the invariant the positional decoder relies on).
        let mut m = sample();
        for s in &mut m.cfs[0].sstables {
            s.partition = None;
        }
        m.instance_nonce = Some(7);
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d.instance_nonce, Some(7));
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));
        assert!(d.cfs[0].sstables.iter().all(|s| s.object.is_none()));
    }
}
