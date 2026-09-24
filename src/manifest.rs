//! Durable catalog: the next file id, the global commit sequence, and, per
//! column family, its serialized config and the set of SSTables organized by
//! level.
//!
//! The snapshot is rewritten in full by a persist or a snapshot compaction
//! (with `CAP_MANIFEST_EDITS`, changes in between go to `MANIFEST-EDITS`).
//! Writes are crash-atomic: a temp file is written, fsynced, and renamed over
//! the live manifest, then the directory is fsynced. Per-CF config is an opaque
//! blob supplied by the caller, keeping this module decoupled from the engine's
//! option types.
//!
//! # Epoch-1 encoding
//!
//! ```text
//!  0  magic "YOLODBMF" | 8 version u32 = 1 | 12 caps u64 | 20 db_flags u32
//! 24  next_file_id u64 | 32 global_seq u64
//! 40  [DB_INSTANCE_NONCE] nonce u64
//!     [DB_EDIT_LOG] generation u64 | applied_through u64 | next_edit_id u64
//!     cf_count uvarint
//!     per CF: cf_flags uvarint | name* | config* | [CF_UNIFIED_ID] id u64
//!             | sst_count uvarint
//!       per SST: id, level, num_entries, num_tombstones, max_seq, klog_size,
//!                vlog_size (uvarint) | min_key* | max_key* | sst_flags uvarint
//!                | [SST_PARTITION] name* | [SST_TIER] name* | [SST_OBJECT] stem*
//!                | [SST_MAX_ENTRY_TIME] uvarint | [SST_LAST_COMPACTION_TIME] uvarint
//!                | [SST_RANGE] count | min_seq | max_seq | min_key* | max_key*
//!     crc32c u32 over everything before it
//! ```
//!
//! (`*` = uvarint length + bytes.) Every optional field is a **flagged
//! section**, in ascending bit order, under a strict mask — the discipline
//! wavesdb's v3/v4 manifest proved out — replacing 0.9's positional tails and
//! `ONDA*` tagged tails. The capability word is a fixed `u64` in the header.
//! Bit values are in [`crate::format::manifest_file`].

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::encoding::{
    append_u32, append_u64, append_uvarint, checksum, read_u32, read_u64, uvarint,
};
use crate::error::{OndaError, Result};
use crate::format::manifest_file::{
    CF_KNOWN, CF_UNIFIED_ID, DB_EDIT_LOG, DB_INSTANCE_NONCE, DB_KNOWN, DB_UNIFIED_WAL, HEADER_LEN,
    MAGIC, SST_KNOWN, SST_LAST_COMPACTION_TIME, SST_MAX_ENTRY_TIME, SST_OBJECT, SST_PARTITION,
    SST_RANGE, SST_TIER, VERSION,
};

/// WAL/memtable layout persisted for the whole database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalLayout {
    /// Legacy/default layout: one WAL and memtable per column family.
    #[default]
    PerColumnFamily,
    /// One database-wide WAL and memtable, with CF-id-prefixed keys.
    Unified,
}

/// One SSTable in the catalog.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SstMeta {
    pub id: u64,
    pub level: u32,
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub max_seq: u64,
    pub klog_size: u64,
    pub vlog_size: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    /// Partition this table belongs to, set only for bottom-level files that
    /// compaction cut on a partition boundary (see
    /// [`ColumnFamilyConfig::partition_rules`](crate::config::ColumnFamilyConfig::partition_rules)).
    /// `None` means the implicit default partition (or a file written before
    /// partitioning existed — old manifests decode every table to `None`).
    pub partition: Option<String>,
    /// Storage tier holding this table's files, by name (see
    /// [`TierDef`](crate::config::TierDef)). `None` means the implicit default
    /// tier — the database directory. Only bottom-level parts may carry a tier;
    /// WAL and upper levels always live on the default tier. Old manifests
    /// (written before tiering) decode every table to `None`.
    pub tier: Option<String>,
    /// Wall-clock time (nanoseconds since the Unix epoch) of the newest entry in
    /// this table, stamped approximately by the writer: flush/ingest output takes
    /// the write time, and compaction carries forward the maximum over its
    /// inputs so re-compacting cold data does not make it look freshly written.
    /// Drives the age gate of the part mover
    /// ([`TierRule::min_age`](crate::config::TierRule::min_age)). `None` means
    /// the age is unknown (a legacy manifest, or a table whose lineage never
    /// carried a timestamp); the mover treats an unknown age as ineligible.
    pub max_entry_time: Option<i64>,
    /// Tier-root-relative path stem of this table's files on a **shared** tier
    /// (A2): the klog lives at `{tier_root}/{object}.klog`. `None` for every
    /// table on the default or a non-shared tier, and for all pre-A2 manifests
    /// — those resolve by the legacy id-derived path. Set by a part move onto
    /// a shared tier (`cf-{cf}/{instance:016x}-{id}`) or adopted verbatim by
    /// [`attach_part_by_ref`](crate::DB::attach_part_by_ref), so the name a
    /// table was published under never changes, whichever database reads it.
    pub object: Option<String>,
    /// Wall-clock time (nanoseconds since the Unix epoch) at which this table
    /// was last *written by a compaction* — the age state periodic compaction
    /// (0.3) revisits tables against
    /// ([`ColumnFamilyConfig::periodic_compaction_interval`](crate::config::ColumnFamilyConfig::periodic_compaction_interval)).
    ///
    /// Deliberately **not** [`max_entry_time`](Self::max_entry_time): that field
    /// carries the maximum forward over a compaction's inputs so cold data does
    /// not look freshly written, which is what the part mover's age gate needs
    /// and exactly what a periodic trigger must not have — carrying it forward
    /// would leave a just-rewritten table instantly re-eligible.
    ///
    /// `None` means *unknown*, and unknown is never eligible: a legacy manifest,
    /// a table written before the capability was enabled, a foreign mount, or a
    /// part attached from another database whose compaction history this one
    /// does not own. Persisted only behind
    /// [`CAP_PERIODIC_AGE`](crate::format::CAP_PERIODIC_AGE).
    pub last_compaction_time: Option<i64>,

    /// Range-tombstone fragments this table's aux section carries (1.2).
    ///
    /// `0` for every legacy table and for every table written before
    /// [`CAP_RANGE_DELETES`](crate::format::CAP_RANGE_DELETES) was enabled —
    /// which is exactly what makes the read path's gate free: one comparison
    /// against zero skips the whole feature for a point-only table.
    pub range_count: u64,
    /// Lowest sequence in any of this table's fragment stacks; `0` when
    /// `range_count == 0`.
    pub range_min_seq: u64,
    /// Highest sequence in any of this table's fragment stacks; `0` when
    /// `range_count == 0`.
    pub range_max_seq: u64,
    /// Lowest fragment `start` in this table, or `None` when it carries none.
    ///
    /// May sort **below** [`min_key`](Self::min_key): fragments are clipped to
    /// the *output interval* a compaction assigns, and the first output of a
    /// job owns everything from the job span's lower edge — including the gap
    /// between that edge and its own first point key.
    pub range_min_key: Option<Vec<u8>>,
    /// Highest fragment `end` in this table (exclusive), or `None`.
    ///
    /// May sort **above** [`max_key`](Self::max_key), by the mirror of the rule
    /// above. The read path's gap-owner rule reads exactly this field.
    pub range_max_key: Option<Vec<u8>>,
}

impl SstMeta {
    /// Whether this table carries range-tombstone fragments.
    #[inline]
    pub fn has_ranges(&self) -> bool {
        self.range_count > 0
    }

    /// Lowest key this table has anything to say about — its point minimum, or
    /// the fragment minimum when that sorts lower.
    pub fn span_min<'a>(&'a self, cmp: &crate::comparator::ComparatorRef) -> &'a [u8] {
        match &self.range_min_key {
            Some(k) if cmp.compare(k, &self.min_key).is_lt() => k,
            _ => &self.min_key,
        }
    }

    /// Highest key this table has anything to say about.
    ///
    /// Inclusive, like [`max_key`](Self::max_key): a fragment's `end` is
    /// exclusive, so the last key it can cover is strictly below it and
    /// `range_max_key` is a safe inclusive upper bound.
    pub fn span_max<'a>(&'a self, cmp: &crate::comparator::ComparatorRef) -> &'a [u8] {
        match &self.range_max_key {
            Some(k) if cmp.compare(k, &self.max_key).is_gt() => k,
            _ => &self.max_key,
        }
    }

    /// Does this table's **span** (points plus fragments) contain `key`?
    pub fn span_contains(&self, cmp: &crate::comparator::ComparatorRef, key: &[u8]) -> bool {
        cmp.compare(key, self.span_min(cmp)).is_ge() && cmp.compare(key, self.span_max(cmp)).is_le()
    }
}
#[derive(Debug, Clone, Default)]
pub struct CfManifest {
    pub name: String,
    pub config: Vec<u8>, // opaque, caller-defined serialization
    pub sstables: Vec<SstMeta>,
    /// The column family's unified-layout id — the 8-byte big-endian prefix
    /// its keys carry in a unified WAL and memtable — when it is **not**
    /// FNV-1a-64 of the name. `None` (the default, and what every family
    /// written so far has) means the derived id.
    ///
    /// Stored so the id can diverge from the name (plan C F5′: clearing a
    /// family under the unified layout by giving it a fresh id), and so a 0.9
    /// directory — whose ids used a truncated FNV basis — can be opened with
    /// the ids its WAL keys actually carry.
    pub unified_id: Option<u64>,
}

impl CfManifest {
    /// The unified id this family's keys carry.
    pub fn effective_unified_id(&self) -> u64 {
        self.unified_id
            .unwrap_or_else(|| crate::unified::cf_id(&self.name))
    }

    /// The id to store: `None` when it equals the derived one, so the encoding
    /// has exactly one spelling of "the default".
    fn stored_unified_id(&self) -> Option<u64> {
        self.unified_id
            .filter(|&id| id != crate::unified::cf_id(&self.name))
    }
}

/// The whole database catalog.
#[derive(Debug, Clone)]
pub struct Manifest {
    pub next_file_id: u64,
    pub global_seq: u64,
    pub cfs: Vec<CfManifest>,
    pub wal_layout: WalLayout,
    /// Per-database nonce naming this instance's objects on shared tiers
    /// (A2). Minted once, the first time a shared tier is configured, and
    /// never changed afterwards: object names embed it, so a new nonce would
    /// orphan every object the old one named. `None` until minted — a
    /// database with no shared tier never mints one, keeping its manifest
    /// readable by pre-A2 binaries.
    pub instance_nonce: Option<u64>,
    /// Format capabilities this database has durably enabled (see
    /// [`crate::format::KNOWN_CAPS`]). `0` — the default — means the database
    /// writes only legacy artifacts, and its manifest stays VERSION 1.
    pub caps: u64,
    /// Snapshot generation, incremented by each snapshot compaction (2.2).
    /// **Informational**: recovery logs it and the golden fixtures pin it, but
    /// it carries no decision power — see [`Manifest::applied_through`].
    pub generation: u64,
    /// Highest edit id this snapshot already contains. Recovery skips log
    /// records at or below it and applies the rest, and the log header's
    /// `base_applied_through` must not exceed it.
    pub applied_through: u64,
    /// Id the next appended edit takes. Always `applied_through + 1` at the
    /// moment a snapshot is written; a manifest with no edit-log tail decodes
    /// to `1`, which is the same "nothing has been appended" statement a fresh
    /// database makes.
    pub next_edit_id: u64,
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            next_file_id: 1,
            global_seq: 0,
            cfs: Vec::new(),
            wal_layout: WalLayout::PerColumnFamily,
            instance_nonce: None,
            caps: 0,
            generation: 0,
            applied_through: 0,
            next_edit_id: 1,
        }
    }
}

/// Path of the manifest within a database directory.
pub fn manifest_path(db_dir: impl AsRef<Path>) -> PathBuf {
    db_dir.as_ref().join("MANIFEST")
}

impl Manifest {
    /// Read the manifest at `path`. A missing file yields an empty manifest.
    pub fn load(path: impl AsRef<Path>) -> Result<Manifest> {
        let data = match std::fs::read(path.as_ref()) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Manifest::default()),
            Err(e) => return Err(e.into()),
        };
        Manifest::decode(&data)
    }

    /// Atomically write the manifest to `path`.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let data = self.encode();
        let tmp = path.with_extension("tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)?;
            crate::util::fault::check(crate::util::fault::Call::Write)?;
            f.write_all(&data)?;
            crate::util::fault::check(crate::util::fault::Call::Sync)?;
            f.sync_all()?;
        }
        crate::util::fault::check(crate::util::fault::Call::Rename)?;
        std::fs::rename(&tmp, path)?;
        // fsync the directory so the rename is durable. The error propagates:
        // under the edit-log protocol two renames in a row are load-bearing
        // (snapshot compaction), and a dropped directory fsync there can lose
        // the rename that makes a fresh log authoritative.
        crate::util::sync_parent_dir(path)?;
        Ok(())
    }

    /// Encode as an epoch-1 `MANIFEST` ([`crate::format::manifest_file`]).
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(64 + self.cfs.len() * 64);
        b.extend_from_slice(&MAGIC);
        append_u32(&mut b, VERSION);
        append_u64(&mut b, self.caps);
        let db_flags = self.db_flags();
        append_u32(&mut b, db_flags);
        append_u64(&mut b, self.next_file_id);
        append_u64(&mut b, self.global_seq);
        debug_assert_eq!(b.len(), HEADER_LEN);
        // Database sections, ascending bit order. DB_UNIFIED_WAL has no payload.
        if let Some(nonce) = self.instance_nonce {
            append_u64(&mut b, nonce);
        }
        if db_flags & DB_EDIT_LOG != 0 {
            append_u64(&mut b, self.generation);
            append_u64(&mut b, self.applied_through);
            append_u64(&mut b, self.next_edit_id);
        }
        append_uvarint(&mut b, self.cfs.len() as u64);
        for cf in &self.cfs {
            let unified_id = cf.stored_unified_id();
            append_uvarint(
                &mut b,
                if unified_id.is_some() {
                    CF_UNIFIED_ID
                } else {
                    0
                },
            );
            append_bytes(&mut b, cf.name.as_bytes());
            append_bytes(&mut b, &cf.config);
            if let Some(id) = unified_id {
                append_u64(&mut b, id);
            }
            append_uvarint(&mut b, cf.sstables.len() as u64);
            for sst in &cf.sstables {
                encode_sstable(&mut b, sst, self.caps);
            }
        }
        let crc = checksum(&b);
        append_u32(&mut b, crc);
        b
    }

    /// The database flag word this manifest encodes with: a section is present
    /// exactly when its field differs from the empty-database default.
    fn db_flags(&self) -> u32 {
        let mut f = 0;
        if self.wal_layout == WalLayout::Unified {
            f |= DB_UNIFIED_WAL;
        }
        if self.instance_nonce.is_some() {
            f |= DB_INSTANCE_NONCE;
        }
        if self.generation != 0 || self.applied_through != 0 || self.next_edit_id != 1 {
            f |= DB_EDIT_LOG;
        }
        f
    }

    /// Decode an epoch-1 `MANIFEST`.
    ///
    /// The whole-file CRC32-C is checked first, so a flipped bit anywhere is
    /// `Corruption`. Then: a version other than 1, a capability bit or a
    /// section flag this binary does not implement is `UnsupportedFormat`;
    /// everything else that contradicts the layout — including a section a
    /// writer cannot produce (an age stamp without `CAP_PERIODIC_AGE`, a range
    /// summary without `CAP_RANGE_DELETES`, an edit cursor out of step, a
    /// unified id equal to the one derived from the name) — is `Corruption`.
    /// A 0.9 manifest (`WVMF`) is refused as `UnsupportedFormat`: it is read
    /// only through `legacy_onda`.
    pub(crate) fn decode(data: &[u8]) -> Result<Manifest> {
        if data.len() >= 4 && read_u32(data) == ONDA09_MAGIC {
            return Err(OndaError::UnsupportedFormat(
                "manifest: an ondaDB 0.9 MANIFEST (WVMF); it is readable only through \
                 legacy_onda, and the database must be upgraded to yoloDB epoch 1"
                    .into(),
            ));
        }
        if data.len() < 8 || data[..8] != MAGIC {
            return Err(corrupt("magic is not YOLODBMF"));
        }
        if data.len() < HEADER_LEN + 4 {
            return Err(corrupt("shorter than its header"));
        }
        let (body, stored_crc) = data.split_at(data.len() - 4);
        if read_u32(stored_crc) != checksum(body) {
            return Err(corrupt("checksum mismatch"));
        }
        let mut c = Cursor { p: &body[8..] };
        let version = c.u32()?;
        if version != VERSION {
            return Err(OndaError::UnsupportedFormat(format!(
                "manifest version {version} is not implemented by this binary"
            )));
        }
        let caps = c.u64()?;
        crate::format::check_caps(caps)?;
        let db_flags = c.u32()?;
        if db_flags & !DB_KNOWN != 0 {
            return Err(OndaError::UnsupportedFormat(format!(
                "manifest database flags {db_flags:#x} outside known mask {DB_KNOWN:#x}"
            )));
        }
        let mut m = Manifest {
            next_file_id: c.u64()?,
            global_seq: c.u64()?,
            caps,
            ..Manifest::default()
        };
        if db_flags & DB_UNIFIED_WAL != 0 {
            m.wal_layout = WalLayout::Unified;
        }
        if db_flags & DB_INSTANCE_NONCE != 0 {
            m.instance_nonce = Some(c.u64()?);
        }
        if db_flags & DB_EDIT_LOG != 0 {
            m.generation = c.u64()?;
            m.applied_through = c.u64()?;
            m.next_edit_id = c.u64()?;
            // `next_edit_id` names the id the next append takes, so it is always
            // one past what the snapshot contains; and the section is present
            // only when the cursor differs from a fresh database's.
            if m.applied_through.checked_add(1) != Some(m.next_edit_id) {
                return Err(corrupt("edit cursor: next_edit_id != applied_through + 1"));
            }
            if (m.generation, m.applied_through) == (0, 0) {
                return Err(corrupt("edit-log section at its empty-database default"));
            }
            // Not coupled to CAP_MANIFEST_EDITS: a checkpoint or backup stamps
            // its snapshot generation 1 whatever the source enabled.
        }
        let cf_count = c.uvar()?;
        m.cfs = Vec::with_capacity(capacity_hint(cf_count));
        for _ in 0..cf_count {
            let cf_flags = c.uvar()?;
            if cf_flags & !CF_KNOWN != 0 {
                return Err(OndaError::UnsupportedFormat(format!(
                    "manifest column-family flags {cf_flags:#x} outside known mask {CF_KNOWN:#x}"
                )));
            }
            let name = c.string()?;
            let config = c.bytes()?;
            let unified_id = if cf_flags & CF_UNIFIED_ID != 0 {
                let id = c.u64()?;
                if id == crate::unified::cf_id(&name) {
                    return Err(corrupt("a stored unified id equal to the derived one"));
                }
                Some(id)
            } else {
                None
            };
            let table_count = c.uvar()?;
            let mut sstables = Vec::with_capacity(capacity_hint(table_count));
            for _ in 0..table_count {
                sstables.push(decode_sstable(&mut c, caps)?);
            }
            m.cfs.push(CfManifest {
                name,
                config,
                sstables,
                unified_id,
            });
        }
        if !c.p.is_empty() {
            return Err(corrupt("trailing bytes after the last column family"));
        }
        Ok(m)
    }
}

/// `"WVMF"`, the 0.9 manifest magic, as that binary stored it (a LE `u32`).
pub(crate) const ONDA09_MAGIC: u32 = 0x5756_4D46;

/// Whether `dir` holds an ondaDB 0.9 database: a `MANIFEST` whose first four
/// bytes are 0.9's `WVMF` magic. `false` for a missing or empty manifest (an
/// empty directory is nobody's format) and for an epoch-1 one.
///
/// Compiled in every build, unlike the decoders: a binary without
/// `legacy-onda` still has to *recognize* a 0.9 directory to refuse it with a
/// message that names the missing feature.
pub fn is_onda09_dir(dir: impl AsRef<Path>) -> Result<bool> {
    use std::io::Read;
    let mut f = match std::fs::File::open(manifest_path(dir)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let mut magic = [0u8; 4];
    match f.read_exact(&mut magic) {
        Ok(()) => Ok(read_u32(&magic) == ONDA09_MAGIC),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn corrupt(what: &str) -> OndaError {
    OndaError::Corruption(format!("manifest: {what}"))
}

/// Cap what a count field may pre-allocate. The vector still grows to whatever
/// the bytes actually contain; this only stops a CRC-valid manifest whose count
/// lies from asking for gigabytes before the first element is read.
fn capacity_hint(count: u64) -> usize {
    count.min(4096) as usize
}

fn append_bytes(dst: &mut Vec<u8>, b: &[u8]) {
    append_uvarint(dst, b.len() as u64);
    dst.extend_from_slice(b);
}

/// One table record: the fixed fields, then `sst_flags` and the flagged fields
/// in ascending bit order ([`crate::format::manifest_file`]).
fn encode_sstable(b: &mut Vec<u8>, sst: &SstMeta, caps: u64) {
    append_uvarint(b, sst.id);
    append_uvarint(b, u64::from(sst.level));
    append_uvarint(b, sst.num_entries);
    append_uvarint(b, sst.num_tombstones);
    append_uvarint(b, sst.max_seq);
    append_uvarint(b, sst.klog_size);
    append_uvarint(b, sst.vlog_size);
    append_bytes(b, &sst.min_key);
    append_bytes(b, &sst.max_key);
    // The two capability-bearing fields are written only under their bit:
    // nothing stamps a table before the bit is durable, so the conjunction
    // never drops a stamp — and a manifest carrying one always also carries the
    // bit that tells an older binary to refuse the file.
    let age = sst
        .last_compaction_time
        .filter(|_| caps & crate::format::CAP_PERIODIC_AGE != 0);
    let range = sst.range_count > 0 && caps & crate::format::CAP_RANGE_DELETES != 0;
    let mut flags = 0u64;
    if sst.partition.is_some() {
        flags |= SST_PARTITION;
    }
    if sst.tier.is_some() {
        flags |= SST_TIER;
    }
    if sst.object.is_some() {
        flags |= SST_OBJECT;
    }
    if sst.max_entry_time.is_some() {
        flags |= SST_MAX_ENTRY_TIME;
    }
    if age.is_some() {
        flags |= SST_LAST_COMPACTION_TIME;
    }
    if range {
        flags |= SST_RANGE;
    }
    append_uvarint(b, flags);
    if let Some(p) = &sst.partition {
        append_bytes(b, p.as_bytes());
    }
    if let Some(t) = &sst.tier {
        append_bytes(b, t.as_bytes());
    }
    if let Some(o) = &sst.object {
        append_bytes(b, o.as_bytes());
    }
    if let Some(t) = sst.max_entry_time {
        append_uvarint(b, t as u64);
    }
    if let Some(t) = age {
        append_uvarint(b, t as u64);
    }
    if range {
        append_uvarint(b, sst.range_count);
        append_uvarint(b, sst.range_min_seq);
        append_uvarint(b, sst.range_max_seq);
        append_bytes(b, sst.range_min_key.as_deref().unwrap_or_default());
        append_bytes(b, sst.range_max_key.as_deref().unwrap_or_default());
    }
}

fn decode_sstable(c: &mut Cursor<'_>, caps: u64) -> Result<SstMeta> {
    let mut sst = SstMeta {
        id: c.uvar()?,
        level: u32::try_from(c.uvar()?).map_err(|_| corrupt("table level exceeds u32"))?,
        num_entries: c.uvar()?,
        num_tombstones: c.uvar()?,
        max_seq: c.uvar()?,
        klog_size: c.uvar()?,
        vlog_size: c.uvar()?,
        min_key: c.bytes()?,
        max_key: c.bytes()?,
        ..SstMeta::default()
    };
    let flags = c.uvar()?;
    if flags & !SST_KNOWN != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "manifest table flags {flags:#x} outside known mask {SST_KNOWN:#x}"
        )));
    }
    if flags & SST_PARTITION != 0 {
        sst.partition = Some(c.string()?);
    }
    if flags & SST_TIER != 0 {
        sst.tier = Some(c.string()?);
    }
    if flags & SST_OBJECT != 0 {
        sst.object = Some(c.string()?);
    }
    if flags & SST_MAX_ENTRY_TIME != 0 {
        sst.max_entry_time = Some(c.uvar()? as i64);
    }
    if flags & SST_LAST_COMPACTION_TIME != 0 {
        if caps & crate::format::CAP_PERIODIC_AGE == 0 {
            return Err(corrupt("age stamp without CAP_PERIODIC_AGE"));
        }
        sst.last_compaction_time = Some(c.uvar()? as i64);
    }
    if flags & SST_RANGE != 0 {
        if caps & crate::format::CAP_RANGE_DELETES == 0 {
            return Err(corrupt("range summary without CAP_RANGE_DELETES"));
        }
        let count = c.uvar()?;
        let min_seq = c.uvar()?;
        let max_seq = c.uvar()?;
        let min_key = c.bytes()?;
        let max_key = c.bytes()?;
        // A record naming zero fragments, or an empty bound, is a state the
        // encoder never emits: it would be indistinguishable from absence.
        if count == 0 || min_key.is_empty() || max_key.is_empty() || min_seq > max_seq {
            return Err(corrupt("range summary a writer cannot produce"));
        }
        sst.range_count = count;
        sst.range_min_seq = min_seq;
        sst.range_max_seq = max_seq;
        sst.range_min_key = Some(min_key);
        sst.range_max_key = Some(max_key);
    }
    Ok(sst)
}

/// Checked reads over the manifest body.
struct Cursor<'a> {
    p: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.p.len() < n {
            return Err(corrupt("truncated"));
        }
        let (v, rest) = self.p.split_at(n);
        self.p = rest;
        Ok(v)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(read_u32(self.take(4)?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(read_u64(self.take(8)?))
    }

    fn uvar(&mut self) -> Result<u64> {
        let (v, n) = uvarint(self.p).ok_or_else(|| corrupt("malformed uvarint"))?;
        self.p = &self.p[n..];
        Ok(v)
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.uvar()?).map_err(|_| corrupt("length exceeds usize"))?;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).map_err(|_| corrupt("invalid UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                        ..Default::default()
                    },
                ],
                unified_id: None,
            }],
        }
    }

    /// Every section in use at once: all database flags, a stored unified id,
    /// and a table carrying every flagged field.
    fn everything() -> Manifest {
        let mut m = sample();
        m.caps = crate::format::CAP_EXTENDED_RECORDS
            | crate::format::CAP_RANGE_DELETES
            | crate::format::CAP_PERIODIC_AGE
            | crate::format::CAP_MANIFEST_EDITS;
        m.wal_layout = WalLayout::Unified;
        m.instance_nonce = Some(0xdead_beef_cafe_f00d);
        m.generation = 3;
        m.applied_through = 17;
        m.next_edit_id = 18;
        m.cfs[0].unified_id = Some(0x0123_4567_89ab_cdef);
        let t = &mut m.cfs[0].sstables[1];
        t.tier = Some("cold".into());
        t.object = Some("cf-default/00c0ffee-2".into());
        t.max_entry_time = Some(1_700_000_000_000_000_000);
        t.last_compaction_time = Some(1_700_000_000_000_000_123);
        t.range_count = 3;
        t.range_min_seq = 17;
        t.range_max_seq = 42;
        t.range_min_key = Some(b"aa".to_vec());
        t.range_max_key = Some(b"nnn".to_vec());
        m.cfs.push(CfManifest {
            name: "second".into(),
            config: Vec::new(),
            sstables: Vec::new(),
            unified_id: None,
        });
        m
    }

    fn assert_same(a: &Manifest, b: &Manifest) {
        assert_eq!(a.next_file_id, b.next_file_id);
        assert_eq!(a.global_seq, b.global_seq);
        assert_eq!(a.caps, b.caps);
        assert_eq!(a.wal_layout, b.wal_layout);
        assert_eq!(a.instance_nonce, b.instance_nonce);
        assert_eq!(
            (a.generation, a.applied_through, a.next_edit_id),
            (b.generation, b.applied_through, b.next_edit_id)
        );
        assert_eq!(a.cfs.len(), b.cfs.len());
        for (x, y) in a.cfs.iter().zip(&b.cfs) {
            assert_eq!(x.name, y.name);
            assert_eq!(x.config, y.config);
            assert_eq!(x.unified_id, y.unified_id);
            assert_eq!(x.sstables, y.sstables);
        }
    }

    #[test]
    fn round_trips_with_every_section() {
        for m in [sample(), everything(), Manifest::default()] {
            let enc = m.encode();
            let d = Manifest::decode(&enc).unwrap();
            assert_same(&d, &m);
            assert_eq!(d.encode(), enc, "re-encode must be byte-identical");
        }
    }

    /// The fixed header and a minimal body, byte for byte.
    #[test]
    fn golden_bytes_of_an_empty_database() {
        let enc = Manifest::default().encode();
        let mut want = b"YOLODBMF".to_vec();
        want.extend_from_slice(&1u32.to_le_bytes()); // version
        want.extend_from_slice(&0u64.to_le_bytes()); // caps
        want.extend_from_slice(&0u32.to_le_bytes()); // db flags
        want.extend_from_slice(&1u64.to_le_bytes()); // next_file_id
        want.extend_from_slice(&0u64.to_le_bytes()); // global_seq
        want.push(0); // cf_count
        let crc = checksum(&want);
        want.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(enc, want);
    }

    /// One table with one flagged field, decoded by hand.
    #[test]
    fn golden_bytes_of_a_table_record() {
        let m = Manifest {
            cfs: vec![CfManifest {
                name: "c".into(),
                config: vec![9],
                sstables: vec![SstMeta {
                    id: 5,
                    level: 1,
                    num_entries: 2,
                    num_tombstones: 0,
                    max_seq: 7,
                    klog_size: 300,
                    vlog_size: 0,
                    min_key: b"a".to_vec(),
                    max_key: b"b".to_vec(),
                    tier: Some("t".into()),
                    ..Default::default()
                }],
                unified_id: None,
            }],
            ..Manifest::default()
        };
        let enc = m.encode();
        let body = &enc[HEADER_LEN..enc.len() - 4];
        assert_eq!(
            body,
            &[
                1, // cf_count
                0, // cf_flags
                1, b'c', // name
                1, 9, // config
                1, // sst_count
                5, 1, 2, 0, 7, 0xAC, 0x02, 0, // id level entries tombs max_seq klog vlog
                1, b'a', 1, b'b', // min, max
                0x02, // sst_flags: SST_TIER
                1, b't',
            ][..]
        );
    }

    /// A unified id equal to the name's FNV-1a is omitted, so every existing
    /// family encodes no id at all; a diverged one round-trips.
    #[test]
    fn unified_id_is_stored_only_when_it_diverges() {
        let mut m = sample();
        m.cfs[0].unified_id = Some(crate::unified::cf_id("default"));
        let derived = m.encode();
        m.cfs[0].unified_id = None;
        assert_eq!(derived, m.encode(), "the derived id is never written");
        assert_eq!(Manifest::decode(&derived).unwrap().cfs[0].unified_id, None);
        m.cfs[0].unified_id = Some(7);
        let d = Manifest::decode(&m.encode()).unwrap();
        assert_eq!(d.cfs[0].unified_id, Some(7));
        assert_eq!(d.cfs[0].effective_unified_id(), 7);
    }

    /// Re-seal the CRC after an edit, so a test reaches the checks behind it.
    fn resealed(mut b: Vec<u8>) -> Vec<u8> {
        let n = b.len() - 4;
        let crc = checksum(&b[..n]);
        b[n..].copy_from_slice(&crc.to_le_bytes());
        b
    }

    #[test]
    fn corruption_rows_fail_closed() {
        let good = everything().encode();
        // Every single-bit flip is caught by the CRC (or the magic).
        for at in 0..good.len() {
            let mut b = good.clone();
            b[at] ^= 0x01;
            let err = Manifest::decode(&b).expect_err("a flipped bit must be refused");
            assert_eq!(err.kind(), "corruption", "flip at {at}");
        }
        // Truncation at every length.
        for n in 0..good.len() {
            assert!(Manifest::decode(&good[..n]).is_err(), "truncated to {n}");
        }
        // Trailing bytes under a valid CRC.
        let mut b = good[..good.len() - 4].to_vec();
        b.push(0);
        b.extend_from_slice(&[0; 4]);
        assert_eq!(
            Manifest::decode(&resealed(b)).unwrap_err().kind(),
            "corruption"
        );
        // Unknown version / database flag / capability bit.
        let mut b = good.clone();
        b[8] = 2;
        assert_eq!(
            Manifest::decode(&resealed(b)).unwrap_err().kind(),
            "unsupported_format"
        );
        let mut b = good.clone();
        b[20] |= 0x08;
        assert_eq!(
            Manifest::decode(&resealed(b)).unwrap_err().kind(),
            "unsupported_format"
        );
        let mut b = good.clone();
        b[13] |= 0x10; // caps bit 12
        assert_eq!(
            Manifest::decode(&resealed(b)).unwrap_err().kind(),
            "unsupported_format"
        );
        // A 0.9 manifest is a named refusal, not corruption.
        let mut b = good.clone();
        b[..4].copy_from_slice(&ONDA09_MAGIC.to_le_bytes());
        assert_eq!(
            Manifest::decode(&b).unwrap_err().kind(),
            "unsupported_format"
        );
    }

    /// Sections a writer cannot produce are corruption even under a valid CRC.
    #[test]
    fn unwritable_sections_are_corruption() {
        let reject = |m: &Manifest| {
            // Encode without the encoder's conjunctions by patching the caps
            // word of an encoding that carried the section.
            Manifest::decode(&m.encode())
                .unwrap_err()
                .kind()
                .to_string()
        };
        // An age stamp or a range summary without its capability bit.
        for strip in [
            crate::format::CAP_PERIODIC_AGE,
            crate::format::CAP_RANGE_DELETES,
        ] {
            let m = everything();
            let mut b = m.encode();
            let caps = m.caps & !strip;
            b[12..20].copy_from_slice(&caps.to_le_bytes());
            assert_eq!(
                Manifest::decode(&resealed(b)).unwrap_err().kind(),
                "corruption",
                "strip {strip:#x}"
            );
        }
        // An edit-log section at the empty-database default.
        let mut b = sample().encode();
        b.truncate(HEADER_LEN);
        b[20] |= DB_EDIT_LOG as u8;
        for v in [0u64, 0, 1] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b.push(0); // no column families
        b.extend_from_slice(&[0; 4]);
        assert_eq!(
            Manifest::decode(&resealed(b)).unwrap_err().kind(),
            "corruption"
        );
        // An edit cursor out of step.
        let mut m = everything();
        m.next_edit_id = 99;
        assert_eq!(reject(&m), "corruption");
        // A range summary naming zero fragments.
        let mut m = everything();
        m.cfs[0].sstables[1].range_min_key = Some(Vec::new());
        assert_eq!(reject(&m), "corruption");
    }

    /// A stored unified id equal to the derived one is non-canonical.
    #[test]
    fn a_stored_id_equal_to_the_derived_one_is_corruption() {
        let mut m = sample();
        let marker = 0x1122_3344_5566_7788u64;
        m.cfs[0].unified_id = Some(marker);
        let mut b = m.encode();
        let at = b
            .windows(8)
            .position(|w| w == marker.to_le_bytes())
            .expect("the id bytes");
        b[at..at + 8].copy_from_slice(&crate::unified::cf_id("default").to_le_bytes());
        assert_eq!(
            Manifest::decode(&resealed(b)).unwrap_err().kind(),
            "corruption"
        );
    }

    /// The decoder is total over arbitrary bytes, CRC-valid or not.
    #[test]
    fn fuzz_decode_never_panics() {
        let seeds = [sample().encode(), everything().encode()];
        let mut rng = crate::util::FuzzRng::new(0x2545_F491_4F6C_DD1D);
        for seed in &seeds {
            for _ in 0..3000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                let _ = Manifest::decode(&case);
                if case.len() > 4 {
                    let _ = Manifest::decode(&resealed(case));
                }
            }
        }
    }

    #[test]
    fn save_load_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = manifest_path(dir.path());
        sample().save(&path).unwrap();
        let d = Manifest::load(&path).unwrap();
        assert_eq!(d.next_file_id, 42);
        assert_eq!(d.cfs[0].sstables.len(), 2);
        // No stray temp file left behind.
        assert!(!path.with_extension("tmp").exists());
    }

    /// A directory whose read bit is clear still accepts a rename (write+search
    /// are enough) but refuses `File::open`, which is exactly the call the
    /// post-rename directory fsync makes. Returns `false` when the process can
    /// read such a directory anyway (root), in which case the test is skipped.
    #[cfg(unix)]
    fn directory_permissions_are_enforced(probe: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(probe).unwrap();
        std::fs::set_permissions(probe, std::fs::Permissions::from_mode(0o300)).unwrap();
        let readable = std::fs::File::open(probe).is_ok();
        std::fs::set_permissions(probe, std::fs::Permissions::from_mode(0o700)).unwrap();
        !readable
    }

    /// The directory fsync after the rename must propagate its error: snapshot
    /// compaction performs two renames whose durability is load-bearing, and a
    /// dropped fsync there can lose the rename that makes a fresh edit log
    /// authoritative.
    #[cfg(unix)]
    #[test]
    fn save_propagates_a_directory_fsync_failure() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        if !directory_permissions_are_enforced(&root.path().join("probe")) {
            return; // running as root: directory permission bits do not apply
        }
        let dir = root.path().join("db");
        std::fs::create_dir(&dir).unwrap();
        let path = manifest_path(&dir);
        let m = sample();
        m.save(&path).expect("baseline save must succeed");
        // Pre-create the temp file so reopening it needs only search permission,
        // leaving the directory `File::open` as the single failing call.
        std::fs::write(path.with_extension("tmp"), b"").unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o300)).unwrap();
        let res = m.save(&path);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let err = res.expect_err("an unreadable parent directory must fail the save");
        assert_eq!(err.kind(), "io");
    }

    #[test]
    fn missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let m = Manifest::load(manifest_path(dir.path())).unwrap();
        assert_eq!(m.next_file_id, 1);
        assert_eq!(m.global_seq, 0);
        assert!(m.cfs.is_empty());
    }

    /// Sizing probe for the whole-manifest rewrite cost (see the 2.2 edit log).
    #[test]
    #[ignore = "sizing probe, not a gate — run with --ignored --nocapture"]
    fn manifest_encoded_size_at_scale() {
        for parts in [1_000usize, 10_000, 100_000] {
            let mut m = Manifest::default();
            let mut cf = CfManifest {
                name: "ns".into(),
                ..CfManifest::default()
            };
            for i in 0..parts {
                cf.sstables.push(SstMeta {
                    id: i as u64,
                    level: 6,
                    num_entries: 50_000,
                    max_seq: 1 << 40,
                    klog_size: 64 << 20,
                    min_key: format!("namespace/cluster-{i:08}/segment/000000").into_bytes(),
                    max_key: format!("namespace/cluster-{i:08}/segment/999999").into_bytes(),
                    partition: Some(format!("cluster-{i:08}")),
                    ..Default::default()
                });
            }
            m.cfs.push(cf);
            println!("{parts} parts: {} bytes", m.encode().len());
        }
    }
}
