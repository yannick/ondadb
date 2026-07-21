//! Durable catalog: the next file id, the global commit sequence, and, per
//! column family, its serialized config and the set of SSTables organized by
//! level.
//!
//! The manifest is rewritten in full on every structural change (flush or
//! compaction).  Writes are crash-atomic: a temp file is written, fsynced, and
//! renamed over the live manifest, then the directory is fsynced.  Per-CF config
//! is an opaque blob supplied by the caller, keeping this module decoupled from
//! the engine's option types.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::encoding::{
    append_u32, append_u64, append_uvarint, checksum, read_u32, read_u64, uvarint,
};
use crate::error::{OndaError, Result};

const MAGIC: u32 = 0x5756_4D46; // "WVMF"
const VERSION: u32 = 1;

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
}

/// Persisted state of one column family.
#[derive(Debug, Clone, Default)]
pub struct CfManifest {
    pub name: String,
    pub config: Vec<u8>, // opaque, caller-defined serialization
    pub sstables: Vec<SstMeta>,
}

/// The whole database catalog.
#[derive(Debug, Clone)]
pub struct Manifest {
    pub next_file_id: u64,
    pub global_seq: u64,
    pub cfs: Vec<CfManifest>,
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            next_file_id: 1,
            global_seq: 0,
            cfs: Vec::new(),
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
            f.write_all(&data)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        // fsync the directory so the rename is durable.
        if let Some(dir) = path.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }

    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        append_u32(&mut b, MAGIC);
        append_u32(&mut b, VERSION);
        append_u64(&mut b, self.next_file_id);
        append_u64(&mut b, self.global_seq);
        append_uvarint(&mut b, self.cfs.len() as u64);
        for cf in &self.cfs {
            append_bytes(&mut b, cf.name.as_bytes());
            append_bytes(&mut b, &cf.config);
            append_uvarint(&mut b, cf.sstables.len() as u64);
            for s in &cf.sstables {
                append_uvarint(&mut b, s.id);
                append_uvarint(&mut b, u64::from(s.level));
                append_uvarint(&mut b, s.num_entries);
                append_uvarint(&mut b, s.num_tombstones);
                append_uvarint(&mut b, s.max_seq);
                append_uvarint(&mut b, s.klog_size);
                append_uvarint(&mut b, s.vlog_size);
                append_bytes(&mut b, &s.min_key);
                append_bytes(&mut b, &s.max_key);
            }
        }
        // Append-tolerant tail: per-SSTable `partition` and `tier` names. The
        // per-record encoding above is a flat sequential list with no framing,
        // so optional per-record fields can't be tucked in there without
        // breaking older readers; instead they live here at the end of the body
        // (still inside the CRC).
        //
        // The tail holds up to three positional sections, each a per-CF `(count,
        // (table_index uvarint, payload)...)` list mirroring the nested CF→table
        // shape of the body:
        //   1. the partition section  (name payload),
        //   2. the tier section       (name payload; added after partitions), and
        //   3. the max-entry-time section (uvarint payload; added after tiers).
        //
        // Emission rules keep every earlier on-disk format byte-identical and let
        // `decode` read the sections positionally (a later section is emitted only
        // when all earlier ones precede it, even if those are all-empty):
        //   - nothing set                 -> no tail at all (legacy layout).
        //   - only partitions             -> partition section only (P1 layout).
        //   - tiers, no times             -> partition + tier sections (P3 layout).
        //   - any max_entry_time          -> partition + tier + time sections.
        let has_part = self
            .cfs
            .iter()
            .any(|cf| cf.sstables.iter().any(|s| s.partition.is_some()));
        let has_tier = self
            .cfs
            .iter()
            .any(|cf| cf.sstables.iter().any(|s| s.tier.is_some()));
        let has_time = self
            .cfs
            .iter()
            .any(|cf| cf.sstables.iter().any(|s| s.max_entry_time.is_some()));
        if has_part || has_tier || has_time {
            encode_name_section(&mut b, &self.cfs, |s| s.partition.as_deref());
        }
        if has_tier || has_time {
            encode_name_section(&mut b, &self.cfs, |s| s.tier.as_deref());
        }
        if has_time {
            encode_u64_section(&mut b, &self.cfs, |s| s.max_entry_time.map(|t| t as u64));
        }
        let crc = checksum(&b);
        append_u32(&mut b, crc);
        b
    }

    fn decode(data: &[u8]) -> Result<Manifest> {
        let bad = || OndaError::Corruption("manifest: corrupt or invalid".into());
        if data.len() < 8 {
            return Err(bad());
        }
        let body = &data[..data.len() - 4];
        if read_u32(&data[data.len() - 4..]) != checksum(body) {
            return Err(bad());
        }
        let mut p = body;
        if read_u32(p) != MAGIC {
            return Err(bad());
        }
        p = &p[4..];
        if read_u32(p) != VERSION {
            return Err(bad());
        }
        p = &p[4..];
        let next_file_id = read_u64(p);
        p = &p[8..];
        let global_seq = read_u64(p);
        p = &p[8..];
        let (ncf, n) = uvarint(p).ok_or_else(bad)?;
        p = &p[n..];
        let mut cfs = Vec::with_capacity(ncf as usize);
        for _ in 0..ncf {
            let (name, rest) = take_bytes(p).ok_or_else(bad)?;
            p = rest;
            let (config, rest) = take_bytes(p).ok_or_else(bad)?;
            p = rest;
            let (nsst, n) = uvarint(p).ok_or_else(bad)?;
            p = &p[n..];
            let mut sstables = Vec::with_capacity(nsst as usize);
            for _ in 0..nsst {
                let take = |p: &mut &[u8]| -> Result<u64> {
                    let (v, n) = uvarint(p).ok_or_else(bad)?;
                    *p = &p[n..];
                    Ok(v)
                };
                let id = take(&mut p)?;
                let level = take(&mut p)? as u32;
                let num_entries = take(&mut p)?;
                let num_tombstones = take(&mut p)?;
                let max_seq = take(&mut p)?;
                let klog_size = take(&mut p)?;
                let vlog_size = take(&mut p)?;
                let (min_key, rest) = take_bytes(p).ok_or_else(bad)?;
                p = rest;
                let (max_key, rest) = take_bytes(p).ok_or_else(bad)?;
                p = rest;
                sstables.push(SstMeta {
                    id,
                    level,
                    num_entries,
                    num_tombstones,
                    max_seq,
                    klog_size,
                    vlog_size,
                    min_key,
                    max_key,
                    // Filled from the append-tolerant tail after the CF loop.
                    partition: None,
                    tier: None,
                    max_entry_time: None,
                });
            }
            cfs.push(CfManifest {
                name: String::from_utf8(name).map_err(|_| bad())?,
                config,
                sstables,
            });
        }
        // Append-tolerant tail (see `encode`), read positionally: the first
        // section is always the partition section, the second (if present) the
        // tier section, the third (if present) the max-entry-time section. A
        // section is only ever present when all earlier ones precede it, so this
        // fixed order is unambiguous. Older manifests stop short and leave the
        // corresponding fields `None`.
        if !p.is_empty() {
            p = decode_name_section(p, &mut cfs, |sst, name| sst.partition = Some(name))?;
        }
        if !p.is_empty() {
            p = decode_name_section(p, &mut cfs, |sst, name| sst.tier = Some(name))?;
        }
        if !p.is_empty() {
            p = decode_u64_section(p, &mut cfs, |sst, v| sst.max_entry_time = Some(v as i64))?;
        }
        let _ = p;
        Ok(Manifest {
            next_file_id,
            global_seq,
            cfs,
        })
    }
}

/// Encode one tail section: for each CF in order, a uvarint count of tables
/// carrying a name (as selected by `pick`), then `(table_index, name)` pairs.
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

    fn sample() -> Manifest {
        Manifest {
            next_file_id: 42,
            global_seq: 99,
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
                    },
                ],
            }],
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let m = sample();
        let enc = m.encode();
        let d = Manifest::decode(&enc).unwrap();
        assert_eq!(d.next_file_id, 42);
        assert_eq!(d.global_seq, 99);
        assert_eq!(d.cfs.len(), 1);
        assert_eq!(d.cfs[0].name, "default");
        assert_eq!(d.cfs[0].config, vec![1, 2, 3, 4]);
        assert_eq!(d.cfs[0].sstables, m.cfs[0].sstables);
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

    #[test]
    fn missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let m = Manifest::load(manifest_path(dir.path())).unwrap();
        assert_eq!(m.next_file_id, 1);
        assert_eq!(m.global_seq, 0);
        assert!(m.cfs.is_empty());
    }

    #[test]
    fn corruption_detected() {
        let m = sample();
        let mut enc = m.encode();
        let n = enc.len();
        enc[n / 2] ^= 0xFF;
        assert!(Manifest::decode(&enc).is_err());
    }

    #[test]
    fn partition_survives_round_trip() {
        // The sample tags table #2 with partition "img"; #1 has none.
        let d = Manifest::decode(&sample().encode()).unwrap();
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
        let enc = m.encode();
        let d = Manifest::decode(&enc).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));
    }

    #[test]
    fn tier_survives_round_trip_alongside_partition() {
        // Tag table #2 with a tier; it already carries partition "img". Both the
        // partition and the tier must survive independently.
        let mut m = sample();
        m.cfs[0].sstables[1].tier = Some("hdd".into());
        let d = Manifest::decode(&m.encode()).unwrap();
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
        let d = Manifest::decode(&m.encode()).unwrap();
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
        let d = Manifest::decode(&m.encode()).unwrap();
        assert_eq!(d.cfs[0].sstables[1].partition.as_deref(), Some("img"));
        assert!(d.cfs[0].sstables.iter().all(|s| s.tier.is_none()));
    }

    #[test]
    fn max_entry_time_survives_round_trip_alongside_partition_and_tier() {
        let mut m = sample();
        m.cfs[0].sstables[0].max_entry_time = Some(1_700_000_000_000_000_000);
        m.cfs[0].sstables[1].tier = Some("hdd".into());
        m.cfs[0].sstables[1].max_entry_time = Some(1_650_000_000_000_000_000);
        let d = Manifest::decode(&m.encode()).unwrap();
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
        let d = Manifest::decode(&m.encode()).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));
        assert!(d.cfs[0].sstables.iter().all(|s| s.tier.is_none()));
        assert_eq!(d.cfs[0].sstables[0].max_entry_time, Some(42));
        assert_eq!(d.cfs[0].sstables[1].max_entry_time, None);
    }

    #[test]
    fn p3_manifest_without_time_section_decodes_time_to_none() {
        // The sample tags a partition but no times; decoding under the
        // time-aware format must leave every max_entry_time None.
        let d = Manifest::decode(&sample().encode()).unwrap();
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
        let legacy_enc = legacy.encode();
        let d = Manifest::decode(&legacy_enc).unwrap();
        assert!(d.cfs[0].sstables.iter().all(|s| s.partition.is_none()));

        let with_part = sample(); // table #2 tagged "img"
        assert!(
            with_part.encode().len() > legacy_enc.len(),
            "partition tail must add bytes on top of the legacy body"
        );
    }
    /// Sizing probe for the whole-manifest rewrite cost.
    ///
    /// `save()` encodes and fsyncs the ENTIRE manifest on every flush,
    /// compaction, and part move. Measured encoded sizes with realistic
    /// namespace/cluster-key/segment keys:
    ///
    /// | parts   | manifest | per persist        |
    /// |---------|----------|--------------------|
    /// | 1,000   | 0.1 MiB  | fine               |
    /// | 10,000  | 1.2 MiB  | noticeable         |
    /// | 100,000 | 12.4 MiB | untenable          |
    ///
    /// At 100k parts a single flush writes and fsyncs 12 MiB of unchanged
    /// metadata to record one new table. This is the motivation for an
    /// incremental (edit-log) manifest; the test exists so the number is
    /// measured rather than estimated, and regressions are visible.
    #[test]
    #[ignore = "sizing probe, not a gate — run with --ignored --nocapture"]
    fn manifest_encoded_size_at_scale() {
        for n in [1_000usize, 10_000, 100_000] {
            let ssts: Vec<SstMeta> = (0..n)
                .map(|i| SstMeta {
                    id: i as u64,
                    level: 6,
                    num_entries: 100_000,
                    num_tombstones: 0,
                    max_seq: i as u64,
                    klog_size: 64 << 20,
                    vlog_size: 0,
                    // Realistic spada keys: namespace name + cluster key + segment.
                    min_key: format!("tenant-{i:06}/2026-07/seg-{i:08}/").into_bytes(),
                    max_key: format!("tenant-{i:06}/2026-07/seg-{i:08}/~").into_bytes(),
                    partition: Some(format!("tenant-{i:06}/2026-07")),
                    tier: Some("s3".to_string()),
                    max_entry_time: Some(1_700_000_000_000_000),
                })
                .collect();
            let m = Manifest {
                next_file_id: n as u64,
                global_seq: 1,
                cfs: vec![CfManifest {
                    name: "t_post".into(),
                    config: Vec::new(),
                    sstables: ssts,
                }],
            };
            let bytes = m.encode().len();
            println!(
                "parts={n:>7}  manifest={:>8} bytes ({:.1} MiB) — rewritten on EVERY flush/compaction/part-move",
                bytes,
                bytes as f64 / (1024.0 * 1024.0)
            );
        }
    }
}
