//! A frozen catalog — `MANIFEST` plus a `MANIFEST-EDITS` holding every one of
//! the 13 schema-1 ops — and the catalog `recover_catalog` makes of it.
//!
//! `tests/fixtures/epoch1-edits/` is committed to git and vendored by wavesdb,
//! whose schema-1 reader (yoloDB plan C step 2c) must arrive at `EXPECTED`, the
//! dump below, from the same bytes. The edits are chosen for the rules a
//! reader can get wrong:
//! - records at or below the snapshot's `applied_through` are skipped, even
//!   ones that would fail their preconditions if applied;
//! - a replayed L0 `AddTable` lands at the *front* of the family's tables;
//! - `RemoveTables` asserts no level;
//! - `SetCfUnifiedId` with the derived id means "no override";
//! - `UpdateTable` with all six mask bits, clearing as well as setting;
//! - a table above `next_file_id`/`global_seq` is *reconciled*, not refused;
//! - a torn tail ends replay cleanly.
//!
//! Regenerate with `cargo test --test epoch1_edit_corpus -- --ignored` for a
//! reviewed format change only.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ondadb::config::{Compression, SyncMode};
use ondadb::format::{
    fnv1a64, CAP_EXTENDED_RECORDS, CAP_MANIFEST_EDITS, CAP_PERIODIC_AGE, CAP_RANGE_DELETES,
};
use ondadb::manifest::{CfManifest, Manifest, SstMeta, WalLayout};
use ondadb::manifest_edit::{recover_catalog, EditLog, EditLogHeader, Op, TableUpdate, VersionEdit};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/epoch1-edits")
}

const FILES: &[&str] = &["MANIFEST", "MANIFEST-EDITS", "EXPECTED"];

fn table(id: u64, level: u32, max_seq: u64) -> SstMeta {
    SstMeta {
        id,
        level,
        num_entries: 10 * id,
        num_tombstones: id,
        max_seq,
        klog_size: 4096 * id,
        vlog_size: 0,
        min_key: format!("k{id}a").into_bytes(),
        max_key: format!("k{id}z").into_bytes(),
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
        table_caps: None,
    }
}

fn config(compression: Compression) -> Vec<u8> {
    ondadb::ColumnFamilyConfig {
        compression,
        compression_per_level: Vec::new(),
        sync_mode: SyncMode::Full,
        ..Default::default()
    }
    .encode()
}

fn snapshot() -> Manifest {
    Manifest {
        next_file_id: 5,
        global_seq: 100,
        generation: 2,
        applied_through: 4,
        next_edit_id: 5,
        wal_floor: 0,
        wal_layout: WalLayout::PerColumnFamily,
        instance_nonce: None,
        caps: CAP_EXTENDED_RECORDS | CAP_MANIFEST_EDITS,
        cfs: vec![CfManifest {
            name: "default".into(),
            config: config(Compression::None),
            sstables: vec![table(3, 0, 40), table(4, 1, 90)],
            unified_id: None,
        }],
    }
}

fn edits() -> Vec<(u64, VersionEdit)> {
    let full = SstMeta {
        partition: Some("p1".into()),
        tier: Some("cold".into()),
        max_entry_time: Some(-5),
        object: Some("cf-default/00000000000000aa-7".into()),
        last_compaction_time: Some(1_700_000_000_000_000_000),
        range_count: 2,
        range_min_seq: 11,
        range_max_seq: 12,
        range_min_key: Some(b"r1".to_vec()),
        range_max_key: Some(b"r9".to_vec()),
        vlog_size: 777,
        ..table(7, 0, 150)
    };
    vec![
        // 3 and 4 are inside the snapshot: skipped, never validated.
        (3, VersionEdit::new(vec![Op::SetGlobalSeq(50)])),
        (
            4,
            VersionEdit::new(vec![Op::AddTable {
                cf: "default".into(),
                meta: table(3, 0, 40),
            }]),
        ),
        (
            5,
            VersionEdit::new(vec![
                Op::CreateCf {
                    name: "logs".into(),
                    config: config(Compression::Zstd),
                },
                Op::SetCfUnifiedId {
                    name: "logs".into(),
                    id: 0xdead_beef,
                },
                Op::AddTable {
                    cf: "logs".into(),
                    meta: table(5, 0, 60),
                },
                Op::AddTable {
                    cf: "logs".into(),
                    meta: table(6, 0, 70),
                },
                Op::SetNextFileId(7),
            ]),
        ),
        (
            6,
            VersionEdit::new(vec![
                Op::AddTable {
                    cf: "default".into(),
                    meta: full,
                },
                Op::UpdateTable {
                    cf: "default".into(),
                    id: 4,
                    update: TableUpdate {
                        level: Some(2),
                        tier: Some(Some("warm".into())),
                        ..TableUpdate::default()
                    },
                },
                Op::SetGlobalSeq(200),
            ]),
        ),
        (
            7,
            VersionEdit::new(vec![
                Op::RemoveTable {
                    cf: "default".into(),
                    id: 3,
                    expected_level: 0,
                },
                Op::RemoveTables {
                    cf: "logs".into(),
                    ids: vec![5, 6],
                },
                Op::SetCfConfig {
                    name: "logs".into(),
                    config: config(Compression::Lz4),
                },
                Op::SetCfUnifiedId {
                    name: "logs".into(),
                    id: fnv1a64(b"logs"),
                },
            ]),
        ),
        (
            8,
            VersionEdit::new(vec![
                Op::SetWalLayout(WalLayout::Unified),
                Op::SetNonce(0x0123_4567_89ab_cdef),
                Op::SetCapability(CAP_PERIODIC_AGE | CAP_RANGE_DELETES),
            ]),
        ),
        (
            9,
            VersionEdit::new(vec![
                Op::CreateCf {
                    name: "tmp".into(),
                    config: config(Compression::Snappy),
                },
                Op::DropCf { name: "tmp".into() },
                // Above both counters: recovery reconciles them.
                Op::AddTable {
                    cf: "default".into(),
                    meta: table(20, 3, 500),
                },
            ]),
        ),
        (
            10,
            VersionEdit::new(vec![Op::UpdateTable {
                cf: "default".into(),
                id: 7,
                update: TableUpdate {
                    level: Some(1),
                    tier: Some(None),
                    object: Some(None),
                    partition: Some(Some("p2".into())),
                    max_entry_time: Some(None),
                    last_compaction_time: Some(Some(5)),
                },
            }]),
        ),
    ]
}

/// A torn tail: a record header promising more payload than the file holds.
const TORN_TAIL: [u8; 11] = [64, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3];

fn generate(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    snapshot().save(dir.join("MANIFEST")).unwrap();
    {
        let mut log = EditLog::create(
            dir,
            EditLogHeader {
                base_applied_through: 2,
                snapshot_generation: 1,
            },
        )
        .unwrap();
        for (id, e) in edits() {
            log.append(id, &e).unwrap();
        }
    }
    let path = dir.join("MANIFEST-EDITS");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(&TORN_TAIL);
    std::fs::write(&path, bytes).unwrap();
    let m = recover_catalog(dir).unwrap();
    std::fs::write(dir.join("EXPECTED"), dump(&m)).unwrap();
}

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn opt<T: std::fmt::Display>(v: &Option<T>) -> String {
    v.as_ref().map_or_else(|| "-".into(), |x| format!("={x}"))
}

fn opt_hex(v: &Option<Vec<u8>>) -> String {
    v.as_ref().map_or_else(|| "-".into(), |x| format!("={}", hex(x)))
}

/// A line-oriented dump both engines can print: catalog scalars, then each
/// family in catalog order, then its tables in catalog order.
fn dump(m: &Manifest) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "next_file_id {}", m.next_file_id);
    let _ = writeln!(s, "global_seq {}", m.global_seq);
    let _ = writeln!(s, "applied_through {}", m.applied_through);
    let _ = writeln!(s, "next_edit_id {}", m.next_edit_id);
    let _ = writeln!(s, "wal_unified {}", m.wal_layout == WalLayout::Unified);
    let _ = writeln!(s, "nonce {}", opt(&m.instance_nonce));
    let _ = writeln!(s, "caps {:#x}", m.caps);
    for cf in &m.cfs {
        let _ = writeln!(
            s,
            "cf {} unified_id {} config {}",
            cf.name,
            opt(&cf.unified_id),
            hex(&cf.config)
        );
        for t in &cf.sstables {
            let _ = writeln!(
                s,
                "  table {} level {} entries {} tombstones {} max_seq {} klog {} vlog {} \
                 min {} max {} partition {} tier {} max_entry_time {} object {} \
                 last_compaction_time {} ranges {} {} {} {} {}",
                t.id,
                t.level,
                t.num_entries,
                t.num_tombstones,
                t.max_seq,
                t.klog_size,
                t.vlog_size,
                hex(&t.min_key),
                hex(&t.max_key),
                opt(&t.partition),
                opt(&t.tier),
                opt(&t.max_entry_time),
                opt(&t.object),
                opt(&t.last_compaction_time),
                t.range_count,
                t.range_min_seq,
                t.range_max_seq,
                opt_hex(&t.range_min_key),
                opt_hex(&t.range_max_key),
            );
        }
    }
    s
}

#[test]
#[ignore = "regenerates committed fixtures; run manually for a reviewed format change"]
fn regenerate_edit_corpus() {
    let tmp = tempfile::tempdir().unwrap();
    generate(tmp.path());
    std::fs::create_dir_all(fixture_dir()).unwrap();
    for name in FILES {
        std::fs::copy(tmp.path().join(name), fixture_dir().join(name)).unwrap();
    }
}

/// The live encoders, and recovery, reproduce every committed byte.
#[test]
fn the_edit_corpus_is_reproduced() {
    let tmp = tempfile::tempdir().unwrap();
    generate(tmp.path());
    for name in FILES {
        let produced = std::fs::read(tmp.path().join(name)).unwrap();
        let committed = std::fs::read(fixture_dir().join(name)).unwrap();
        assert!(
            produced == committed,
            "{name} moved; if this is a deliberate format change, run \
             `cargo test --test epoch1_edit_corpus -- --ignored`"
        );
    }
}

/// What the corpus is for, stated outright rather than only snapshotted.
#[test]
fn the_committed_catalog_recovers_as_documented() {
    let tmp = tempfile::tempdir().unwrap();
    for name in ["MANIFEST", "MANIFEST-EDITS"] {
        std::fs::copy(fixture_dir().join(name), tmp.path().join(name)).unwrap();
    }
    let m = recover_catalog(tmp.path()).unwrap();
    assert_eq!((m.applied_through, m.next_edit_id), (10, 11));
    // Reconciled past table 20 and its max_seq 500.
    assert_eq!((m.next_file_id, m.global_seq), (21, 500));
    assert_eq!(m.wal_layout, WalLayout::Unified);
    assert_eq!(m.instance_nonce, Some(0x0123_4567_89ab_cdef));
    assert_eq!(
        m.caps,
        CAP_EXTENDED_RECORDS | CAP_MANIFEST_EDITS | CAP_PERIODIC_AGE | CAP_RANGE_DELETES
    );
    let names: Vec<_> = m.cfs.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["default", "logs"]);
    // The replayed L0 add went to the front; 3 is gone.
    let ids: Vec<_> = m.cfs[0].sstables.iter().map(|t| t.id).collect();
    assert_eq!(ids, [7, 4, 20]);
    let (t7, t4) = (&m.cfs[0].sstables[0], &m.cfs[0].sstables[1]);
    assert_eq!((t7.level, t7.partition.as_deref()), (1, Some("p2")));
    assert_eq!((t7.tier.as_ref(), t7.object.as_ref()), (None, None));
    assert_eq!((t7.max_entry_time, t7.last_compaction_time), (None, Some(5)));
    assert_eq!((t4.level, t4.tier.as_deref()), (2, Some("warm")));
    assert!(m.cfs[1].sstables.is_empty());
    // The derived id canonicalized to "no override".
    assert_eq!(m.cfs[1].unified_id, None);
    assert_eq!(m.cfs[1].config, config(Compression::Lz4));
    assert_eq!(
        std::fs::read_to_string(fixture_dir().join("EXPECTED")).unwrap(),
        dump(&m)
    );
}
