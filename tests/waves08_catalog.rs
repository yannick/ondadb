//! The wavesdb catalog reader (plan C step 2c) against databases wavesdb
//! itself wrote (`tests/fixtures/wavesdb-epoch1/`).
//!
//! A catalog is checked against the directory it describes, not against
//! numbers copied out of it: every table it lists is a `cf_<name>/<id>.klog`
//! on disk and every such file is listed, likewise each shared `<id>.blob`,
//! and a table's declared capability word is the one its footer carries.

#![cfg(feature = "waves08")]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use ondadb::manifest::WalLayout;
use ondadb::waves08::{self, Catalog};
use ondadb::{ColumnFamilyConfig, OndaError};

const FIXTURES: [&str; 5] = ["percf-caps", "percf-closed", "unified", "unified-prepared", "managed"];

fn db_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/wavesdb-epoch1")
        .join(name)
        .join("db")
}

fn catalog(name: &str) -> Catalog {
    waves08::recover_catalog(db_dir(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

/// The ids of the files in `dir` with extension `ext`.
fn file_ids(dir: &Path, ext: &str) -> BTreeSet<u64> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension()? == ext).then(|| p.file_stem()?.to_str()?.parse().ok())?
        })
        .collect()
}

fn footer_caps(klog: &Path) -> u64 {
    let b = std::fs::read(klog).unwrap();
    let f = &b[b.len() - 96..];
    assert_eq!(&f[88..96], b"YOLOST01", "{} footer magic", klog.display());
    u64::from_le_bytes(f[56..64].try_into().unwrap())
}

#[test]
fn every_fixture_is_a_wavesdb_dir() {
    for name in FIXTURES {
        assert!(waves08::is_waves_dir(db_dir(name)).unwrap(), "{name}");
    }
    // ondaDB's own epoch-1 directories are not.
    let tmp = tempfile::tempdir().unwrap();
    let db = ondadb::DB::open(ondadb::Options::new(tmp.path().to_str().unwrap())).unwrap();
    db.close().unwrap();
    assert!(!waves08::is_waves_dir(tmp.path()).unwrap());
    assert!(!waves08::is_waves_dir(tmp.path().join("absent")).unwrap());
}

#[test]
fn catalogs_match_their_directories() {
    for name in FIXTURES {
        let m = catalog(name);
        assert!(!m.cfs.is_empty(), "{name}: no families");
        let mut dirs: BTreeSet<String> = std::fs::read_dir(db_dir(name))
            .unwrap()
            .filter_map(|e| {
                let e = e.unwrap();
                e.file_type().unwrap().is_dir().then(|| e.file_name().into_string().unwrap())
            })
            .collect();
        for cf in &m.cfs {
            let dir = db_dir(name).join(format!("cf_{}", cf.name));
            assert!(dirs.remove(&format!("cf_{}", cf.name)), "{name}: no dir for {}", cf.name);
            let tables: BTreeSet<u64> = cf.tables.iter().map(|t| t.meta.id).collect();
            assert_eq!(tables, file_ids(&dir, "klog"), "{name}/{}: tables", cf.name);
            let blobs: BTreeSet<u64> = cf.blobs.iter().map(|b| b.id).collect();
            assert_eq!(blobs, file_ids(&dir, "blob"), "{name}/{}: blob files", cf.name);
            for t in &cf.tables {
                assert!(t.meta.id < m.next_file_id && t.meta.max_seq <= m.global_seq);
                let klog = dir.join(format!("{}.klog", t.meta.id));
                let declared = t.meta.table_caps.unwrap_or(0);
                assert_eq!(declared, footer_caps(&klog), "{name}/{} table {}", cf.name, t.meta.id);
                for blob in t.blob_bytes.keys() {
                    assert!(blobs.contains(blob), "{name}: table {} names blob {blob}", t.meta.id);
                }
            }
        }
        assert!(dirs.is_empty(), "{name}: directories with no family: {dirs:?}");
    }
}

#[test]
fn layout_and_managed_state() {
    for name in FIXTURES {
        let m = catalog(name);
        let unified = name != "percf-caps" && name != "percf-closed";
        let want = if unified { WalLayout::Unified } else { WalLayout::PerColumnFamily };
        assert_eq!(m.wal_layout, want, "{name}");
        let managed = name == "managed";
        assert_eq!(m.discard_seq != 0, managed, "{name}: discard floor");
        assert_eq!(m.last_managed_commit != 0, managed, "{name}: last managed commit");
    }
    let m = catalog("managed");
    assert!(m.discard_seq < m.last_managed_commit);
    assert!(m.last_managed_commit <= m.global_seq);
}

/// Each family's JSON config arrives as an ondaDB config with the settings
/// that change how its bytes read.
#[test]
fn configs_translate() {
    for name in ["percf-caps", "percf-closed"] {
        let manifest = catalog(name).to_manifest().unwrap();
        let cfg = |cf: &str| {
            let blob = &manifest.cfs.iter().find(|c| c.name == cf).unwrap().config;
            ColumnFamilyConfig::decode(blob).unwrap()
        };
        assert_eq!(cfg("plain").comparator_name, "memcmp");
        assert_eq!(cfg("rev").comparator_name, "reverse");
        assert_eq!(cfg("num").comparator_name, "int64");
        assert_eq!(cfg("merged").merge_operator_name.as_deref(), Some("fixture.append"));
        assert_eq!(cfg("plain").merge_operator_name, None);
    }
}

/// The crashed and the cleanly closed directory hold the same families; the
/// closed one has folded its log no further than the crashed one got.
#[test]
fn crashed_and_closed_agree_on_families() {
    let names = |m: &Catalog| m.cfs.iter().map(|c| c.name.clone()).collect::<BTreeSet<_>>();
    assert_eq!(names(&catalog("percf-caps")), names(&catalog("percf-closed")));
    assert_eq!(names(&catalog("unified")), names(&catalog("unified-prepared")));
}

/// A log record whose CRC fails is corruption; a log cut inside a record is
/// a torn tail that recovers to the records before it.
#[test]
fn edit_log_damage_rows() {
    let src = db_dir("percf-caps");
    let log = std::fs::read(src.join("MANIFEST-EDITS")).unwrap();
    let parsed = waves08::edit_log::read(&log).unwrap();
    assert!(!parsed.torn && parsed.records.len() > 2);

    let copy = |log: &[u8]| {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::copy(src.join("MANIFEST"), tmp.path().join("MANIFEST")).unwrap();
        std::fs::write(tmp.path().join("MANIFEST-EDITS"), log).unwrap();
        tmp
    };

    // Flip the last byte of the last record: a complete record, bad CRC.
    let mut flipped = log.clone();
    *flipped.last_mut().unwrap() ^= 1;
    let tmp = copy(&flipped);
    assert!(matches!(
        waves08::recover_catalog(tmp.path()),
        Err(OndaError::Corruption(_))
    ));

    // Cut the last record short: torn, so the catalog is the one before it.
    let tmp = copy(&log[..log.len() - 1]);
    let torn = waves08::recover_catalog(tmp.path()).unwrap();
    let last = parsed.records.last().unwrap().edit_id;
    assert_eq!(torn.applied_through, last - 1);

    // A header cut short is corruption, not a tail.
    let tmp = copy(&log[..20]);
    assert!(matches!(
        waves08::recover_catalog(tmp.path()),
        Err(OndaError::Corruption(_))
    ));
}

/// ondaDB's own schema-1 log, or any schema but 2, is not wavesdb's.
#[test]
fn a_foreign_schema_is_unsupported() {
    let mut log = std::fs::read(db_dir("percf-caps").join("MANIFEST-EDITS")).unwrap();
    log[8..12].copy_from_slice(&1u32.to_le_bytes());
    let crc = ondadb::encoding::checksum(&log[..28]);
    log[28..32].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        waves08::edit_log::read(&log),
        Err(OndaError::UnsupportedFormat(_))
    ));
}
