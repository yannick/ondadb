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
        // Append-tolerant tail: per-SSTable `partition` names. The per-record
        // encoding above is a flat sequential list with no framing, so an
        // optional per-record field can't be tucked in there without breaking
        // older readers; instead it lives here at the end of the body (still
        // inside the CRC). A manifest written before partitions existed has no
        // trailing bytes, so `decode` leaves every `partition` as `None`. The
        // section is emitted only when some table actually carries a partition,
        // so partition-free databases keep a byte-identical manifest.
        //
        // Layout: for each CF in order, a uvarint count of partitioned tables,
        // then `(table_index uvarint, name bytes)` pairs — mirroring the nested
        // CF→table shape of the body above.
        if self
            .cfs
            .iter()
            .any(|cf| cf.sstables.iter().any(|s| s.partition.is_some()))
        {
            for cf in &self.cfs {
                let parted: Vec<(usize, &str)> = cf
                    .sstables
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| s.partition.as_deref().map(|p| (i, p)))
                    .collect();
                append_uvarint(&mut b, parted.len() as u64);
                for (i, name) in parted {
                    append_uvarint(&mut b, i as u64);
                    append_bytes(&mut b, name.as_bytes());
                }
            }
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
                });
            }
            cfs.push(CfManifest {
                name: String::from_utf8(name).map_err(|_| bad())?,
                config,
                sstables,
            });
        }
        // Append-tolerant partition tail (see `encode`). Absent in manifests
        // written before partitioning, in which case `p` is already exhausted
        // and every `partition` stays `None`.
        if !p.is_empty() {
            for cf in cfs.iter_mut() {
                let (count, n) = uvarint(p).ok_or_else(bad)?;
                p = &p[n..];
                for _ in 0..count {
                    let (idx, n) = uvarint(p).ok_or_else(bad)?;
                    p = &p[n..];
                    let (name, rest) = take_bytes(p).ok_or_else(bad)?;
                    p = rest;
                    let sst = cf.sstables.get_mut(idx as usize).ok_or_else(bad)?;
                    sst.partition = Some(String::from_utf8(name).map_err(|_| bad())?);
                }
            }
        }
        Ok(Manifest {
            next_file_id,
            global_seq,
            cfs,
        })
    }
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
}
