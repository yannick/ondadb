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
                });
            }
            cfs.push(CfManifest {
                name: String::from_utf8(name).map_err(|_| bad())?,
                config,
                sstables,
            });
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
}
