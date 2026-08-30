//! Numbered manifest version edits (2.2): the catalog-inventory guard, the log
//! codec's frozen bytes, and recovery.
//!
//! The inventory guard is the reason this file exists. Rust has no reflection,
//! so nothing can prove mechanically that the edit-op set covers every field of
//! the durable catalog; the manual enumeration below *is* the guard. When a
//! field is added to `Manifest`, `CfManifest` or `SstMeta`, this test must be
//! extended in the same change — and it fails loudly if the new field is not
//! reachable through some op.

use ondadb::config::{PartitionRule, TierRule};
use ondadb::manifest::{CfManifest, Manifest, SstMeta, WalLayout};
use ondadb::manifest_edit::{apply_edit, Op, TableUpdate, VersionEdit};
use ondadb::ColumnFamilyConfig;

/// Every field of `SstMeta`, each set to a value distinguishable from both the
/// `Default` value and from every other field.
fn distinguishable_table() -> SstMeta {
    SstMeta {
        id: 7,
        level: 3,
        num_entries: 1_234,
        num_tombstones: 56,
        max_seq: 7_890,
        klog_size: 65_536,
        vlog_size: 4_096,
        min_key: b"aaa-min".to_vec(),
        max_key: b"zzz-max".to_vec(),
        partition: Some("img".into()),
        tier: Some("cold".into()),
        max_entry_time: Some(1_700_000_000_000_000_001),
        object: Some("cf-default/00000000deadbeef-7".into()),
        last_compaction_time: Some(1_700_000_000_000_000_002),
    }
}

/// The whole durable catalog with every field distinguishable.
fn distinguishable_manifest() -> Manifest {
    Manifest {
        next_file_id: 4_242,
        global_seq: 9_999,
        cfs: vec![CfManifest {
            name: "photos".into(),
            config: vec![9, 8, 7, 6, 5],
            sstables: vec![distinguishable_table()],
        }],
        wal_layout: WalLayout::Unified,
        instance_nonce: Some(0xDEAD_BEEF_CAFE_F00D),
        caps: ondadb::format::CAP_MANIFEST_EDITS | ondadb::format::CAP_EXTENDED_RECORDS,
        // The three edit-log bookkeeping fields are the log's own cursor, not
        // catalog content: they are written by snapshot compaction and can never
        // be changed by an edit (an edit lives *inside* the log it would name).
        // Kept at their fresh-snapshot values here so the comparison below is
        // about the catalog.
        generation: 0,
        applied_through: 0,
        next_edit_id: 1,
    }
}

/// The inventory guard: 5 `Manifest` fields + 3 `CfManifest` fields + 14
/// `SstMeta` fields, each reachable through an op, verified by rebuilding the
/// whole catalog from `Manifest::default()` with edits alone.
///
/// Partition rules and tier rules are deliberately **absent** from this list:
/// they are not manifest fields. They live inside the opaque
/// `CfManifest.config` blob (`DbInner::persist_manifest` writes
/// `cf.effective_config().encode()`), so `CreateCF`/`SetCFConfig` carry them —
/// see `partition_rules_and_tier_defs_travel_in_the_config_blob`. `TierDef`
/// itself is an `Options` field and is never persisted in the manifest at all.
#[test]
fn every_manifest_field_is_covered_by_an_op() {
    let want = distinguishable_manifest();
    let cf = &want.cfs[0];
    let table = &cf.sstables[0];

    let mut got = Manifest::default();
    let edit = VersionEdit::new(vec![
        // Manifest.cfs, CfManifest.name, CfManifest.config
        Op::CreateCf {
            name: cf.name.clone(),
            config: cf.config.clone(),
        },
        // CfManifest.sstables, and all 13 SstMeta fields
        Op::AddTable {
            cf: cf.name.clone(),
            meta: table.clone(),
        },
        // Manifest.next_file_id
        Op::SetNextFileId(want.next_file_id),
        // Manifest.global_seq
        Op::SetGlobalSeq(want.global_seq),
        // Manifest.wal_layout
        Op::SetWalLayout(WalLayout::Unified),
        // Manifest.instance_nonce
        Op::SetNonce(want.instance_nonce.unwrap()),
        // Manifest.caps
        Op::SetCapability(want.caps),
    ]);
    apply_edit(&mut got, &edit).expect("the rebuild edit must apply");

    // Manifest: 5 fields.
    assert_eq!(got.next_file_id, want.next_file_id, "Manifest.next_file_id");
    assert_eq!(got.global_seq, want.global_seq, "Manifest.global_seq");
    assert_eq!(got.cfs.len(), 1, "Manifest.cfs");
    assert_eq!(got.wal_layout, want.wal_layout, "Manifest.wal_layout");
    assert_eq!(
        got.instance_nonce, want.instance_nonce,
        "Manifest.instance_nonce"
    );
    assert_eq!(got.caps, want.caps, "Manifest.caps");
    // The remaining three `Manifest` fields are the edit log's own cursor. They
    // are deliberately **not** op-covered: an edit lives inside the log it would
    // otherwise name, so only snapshot compaction may write them. Asserting
    // that an edit leaves them alone is what keeps that true — and makes a
    // future field added to `Manifest` fail this test rather than slip past it.
    assert_eq!(got.generation, 0, "Manifest.generation (snapshot-only)");
    assert_eq!(
        got.applied_through, 0,
        "Manifest.applied_through (snapshot-only)"
    );
    assert_eq!(got.next_edit_id, 1, "Manifest.next_edit_id (snapshot-only)");

    // CfManifest: 3 fields.
    let g = &got.cfs[0];
    assert_eq!(g.name, cf.name, "CfManifest.name");
    assert_eq!(g.config, cf.config, "CfManifest.config");
    assert_eq!(g.sstables.len(), 1, "CfManifest.sstables");

    // SstMeta: 13 fields, compared field by field so a failure names the field.
    let t = &g.sstables[0];
    assert_eq!(t.id, table.id, "SstMeta.id");
    assert_eq!(t.level, table.level, "SstMeta.level");
    assert_eq!(t.num_entries, table.num_entries, "SstMeta.num_entries");
    assert_eq!(
        t.num_tombstones, table.num_tombstones,
        "SstMeta.num_tombstones"
    );
    assert_eq!(t.max_seq, table.max_seq, "SstMeta.max_seq");
    assert_eq!(t.klog_size, table.klog_size, "SstMeta.klog_size");
    assert_eq!(t.vlog_size, table.vlog_size, "SstMeta.vlog_size");
    assert_eq!(t.min_key, table.min_key, "SstMeta.min_key");
    assert_eq!(t.max_key, table.max_key, "SstMeta.max_key");
    assert_eq!(t.partition, table.partition, "SstMeta.partition");
    assert_eq!(t.tier, table.tier, "SstMeta.tier");
    assert_eq!(
        t.max_entry_time, table.max_entry_time,
        "SstMeta.max_entry_time"
    );
    assert_eq!(t.object, table.object, "SstMeta.object");

    // And the whole catalog, encoded, is byte-identical to the target.
    assert_eq!(
        encoded(&got),
        encoded(&want),
        "the rebuilt catalog must be byte-identical"
    );
}

/// The mutating ops reach the four `SstMeta` fields a live database changes
/// after a table is published — the part mover's tier/object flip, a partition
/// restamp, and the age stamp — plus the level a compaction moves a table to.
#[test]
fn update_table_covers_every_mutable_table_field() {
    let mut m = Manifest::default();
    apply_edit(
        &mut m,
        &VersionEdit::new(vec![
            Op::CreateCf {
                name: "default".into(),
                config: Vec::new(),
            },
            Op::AddTable {
                cf: "default".into(),
                meta: SstMeta {
                    id: 1,
                    level: 0,
                    ..SstMeta::default()
                },
            },
        ]),
    )
    .unwrap();
    apply_edit(
        &mut m,
        &VersionEdit::new(vec![Op::UpdateTable {
            cf: "default".into(),
            id: 1,
            update: TableUpdate {
                level: Some(6),
                tier: Some(Some("cold".into())),
                object: Some(Some("cf-default/1".into())),
                partition: Some(Some("img".into())),
                max_entry_time: Some(Some(-17)),
                last_compaction_time: Some(Some(-18)),
            },
        }]),
    )
    .unwrap();
    let t = &m.cfs[0].sstables[0];
    assert_eq!(t.level, 6);
    assert_eq!(t.tier.as_deref(), Some("cold"));
    assert_eq!(t.object.as_deref(), Some("cf-default/1"));
    assert_eq!(t.partition.as_deref(), Some("img"));
    assert_eq!(t.max_entry_time, Some(-17));
    assert_eq!(t.last_compaction_time, Some(-18));
}

/// Partition rules and tier rules are **not** manifest fields: they are encoded
/// into the opaque `CfManifest.config` blob. Without this the inventory guard
/// above would report them as uncovered; with it, `CreateCF` and `SetCFConfig`
/// are demonstrably their carrier.
#[test]
fn partition_rules_and_tier_defs_travel_in_the_config_blob() {
    let cfg = ColumnFamilyConfig {
        partition_rules: vec![
            PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            },
            PartitionRule {
                prefix: b"vid/".to_vec(),
                name: "vid".into(),
            },
        ],
        tier_rules: vec![TierRule {
            prefix: b"img/".to_vec(),
            tier: "cold".into(),
            min_age: std::time::Duration::from_secs(3_600),
        }],
        ..ColumnFamilyConfig::default()
    };

    let mut m = Manifest::default();
    apply_edit(
        &mut m,
        &VersionEdit::new(vec![Op::CreateCf {
            name: "default".into(),
            config: cfg.encode(),
        }]),
    )
    .unwrap();
    let back = ColumnFamilyConfig::decode(&m.cfs[0].config);
    assert_eq!(back.partition_rules, cfg.partition_rules);
    assert_eq!(back.tier_rules.len(), 1);
    assert_eq!(back.tier_rules[0].tier, "cold");

    // And a later rule change is one SetCFConfig, not a new field.
    let mut cfg2 = cfg.clone();
    cfg2.partition_rules.pop();
    apply_edit(
        &mut m,
        &VersionEdit::new(vec![Op::SetCfConfig {
            name: "default".into(),
            config: cfg2.encode(),
        }]),
    )
    .unwrap();
    assert_eq!(
        ColumnFamilyConfig::decode(&m.cfs[0].config).partition_rules,
        cfg2.partition_rules
    );
}

/// Byte-level equality of two catalogs, through the manifest's own encoder.
fn encoded(m: &Manifest) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MANIFEST");
    m.save(&path).unwrap();
    std::fs::read(&path).unwrap()
}

// ---------------------------------------------------------------------------
// The fault shim, the snapshot-compaction protocol, and recovery
// ---------------------------------------------------------------------------

use ondadb::manifest::manifest_path;
use ondadb::manifest_edit::{
    compact_snapshot, edit_log_path, edit_log_tmp_path, open_log_for_append, recover_catalog,
    snapshot_due, sweep_manifest_temp_files, EditLog, EditLogHeader,
};
use ondadb::util::fault;
use std::path::Path;

/// A database directory holding a snapshot with one CF and one table, plus a
/// log carrying `edits` further single-op edits (each adding one table).
fn seeded_dir(edits: u64) -> (tempfile::TempDir, Manifest) {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Manifest {
        caps: ondadb::format::CAP_MANIFEST_EDITS,
        ..Manifest::default()
    };
    apply_edit(
        &mut m,
        &VersionEdit::new(vec![
            Op::CreateCf {
                name: "default".into(),
                config: vec![1, 2],
            },
            Op::AddTable {
                cf: "default".into(),
                meta: SstMeta {
                    id: 1,
                    level: 0,
                    max_seq: 10,
                    ..SstMeta::default()
                },
            },
        ]),
    )
    .unwrap();
    let mut log = compact_snapshot(dir.path(), &mut m, 0).unwrap();
    for i in 1..=edits {
        let edit = VersionEdit::new(vec![Op::AddTable {
            cf: "default".into(),
            meta: SstMeta {
                id: i + 1,
                level: 0,
                max_seq: 10 + i,
                ..SstMeta::default()
            },
        }]);
        log.append(i, &edit).unwrap();
        apply_edit(&mut m, &edit).unwrap();
    }
    (dir, m)
}

fn table_ids(m: &Manifest) -> Vec<u64> {
    let mut ids: Vec<u64> = m.cfs[0].sstables.iter().map(|s| s.id).collect();
    ids.sort_unstable();
    ids
}

fn log_bytes(dir: &Path) -> Vec<u8> {
    std::fs::read(edit_log_path(dir)).unwrap()
}

#[test]
fn shim_fails_the_selected_call_and_no_other() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Manifest {
        caps: ondadb::format::CAP_MANIFEST_EDITS,
        ..Manifest::default()
    };
    // The second sync of a snapshot compaction is the new log's; the first is
    // the snapshot's, and it must go through untouched.
    fault::fail_nth(fault::Call::Sync, 2);
    let err = compact_snapshot(dir.path(), &mut m, 0).expect_err("the selected call must fail");
    fault::clear();
    assert_eq!(err.kind(), "io", "{err:?}");
    assert!(
        manifest_path(dir.path()).exists(),
        "the first sync succeeded, so the snapshot is on disk"
    );
    assert!(
        !edit_log_path(dir.path()).exists(),
        "only the selected call failed, and it is the one that never completed"
    );
    // With the plan cleared, the same call sequence succeeds end to end.
    let mut m2 = Manifest {
        caps: ondadb::format::CAP_MANIFEST_EDITS,
        ..Manifest::default()
    };
    compact_snapshot(dir.path(), &mut m2, 0).expect("no plan, no failure");
    assert!(edit_log_path(dir.path()).exists());
}

// -- S rows: a crash after each of the four protocol steps -------------------

/// S1 — the new snapshot is written and fsynced but never renamed: the old
/// snapshot and the old log both survive, and recovery replays the log.
#[test]
fn crash_after_snapshot_write_replays_the_old_log() {
    let (dir, live) = seeded_dir(3);
    let before = log_bytes(dir.path());
    let mut m = live.clone();
    fault::fail_nth(fault::Call::Rename, 1);
    let err = compact_snapshot(dir.path(), &mut m, 3).expect_err("the snapshot rename fails");
    fault::clear();
    assert_eq!(err.kind(), "io");
    assert_eq!(log_bytes(dir.path()), before, "the live log is untouched");
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(table_ids(&back), table_ids(&live));
    assert_eq!(back.applied_through, 3);
}

/// S2 — the snapshot rename lands but the new log is never written: the new
/// snapshot plus the *old* log describe the database, because replay skips
/// every id at or below `applied_through`.
#[test]
fn crash_after_snapshot_rename_replays_the_old_log() {
    let (dir, live) = seeded_dir(3);
    let before = log_bytes(dir.path());
    let mut m = live.clone();
    fault::fail_nth(fault::Call::Write, 2);
    let err = compact_snapshot(dir.path(), &mut m, 3).expect_err("the new log's write fails");
    fault::clear();
    assert_eq!(err.kind(), "io");
    assert_eq!(log_bytes(dir.path()), before, "the live log is untouched");
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(table_ids(&back), table_ids(&live));
    assert_eq!(back.generation, live.generation + 1);
    assert_eq!(back.applied_through, 3);
}

/// S3 — the new log's temp file is written and fsynced but never renamed. The
/// same on-disk state as S2, reached through a different call.
#[test]
fn crash_after_new_log_write_replays_the_old_log() {
    let (dir, live) = seeded_dir(3);
    let before = log_bytes(dir.path());
    let mut m = live.clone();
    fault::fail_nth(fault::Call::Rename, 2);
    let err = compact_snapshot(dir.path(), &mut m, 3).expect_err("the log rename fails");
    fault::clear();
    assert_eq!(err.kind(), "io");
    assert_eq!(log_bytes(dir.path()), before, "the live log is untouched");
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(table_ids(&back), table_ids(&live));
}

/// S4 — every step lands: the log restarts empty at the new base.
#[test]
fn crash_after_new_log_rename_starts_clean() {
    let (dir, live) = seeded_dir(3);
    let mut m = live.clone();
    let log = compact_snapshot(dir.path(), &mut m, 3).unwrap();
    assert_eq!(log.count(), 0);
    assert_eq!(log.header().base_applied_through, 3);
    assert_eq!(log.header().snapshot_generation, live.generation + 1);
    assert_eq!(
        log_bytes(dir.path()).len(),
        28,
        "a fresh log is its header and nothing else"
    );
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(table_ids(&back), table_ids(&live));
    assert_eq!(back.applied_through, 3);
    assert_eq!(back.next_edit_id, 4);
}

/// The live log is replaced by a rename, never truncated in place — that is
/// exactly what makes the S1..S3 states safe.
#[test]
fn compaction_never_truncates_the_live_log() {
    let (dir, live) = seeded_dir(2);
    let before = log_bytes(dir.path());
    assert!(before.len() > 28);
    let mut m = live.clone();
    for (call, nth) in [
        (fault::Call::Write, 1),
        (fault::Call::Sync, 1),
        (fault::Call::Rename, 1),
        (fault::Call::Write, 2),
        (fault::Call::Sync, 2),
        (fault::Call::Rename, 2),
    ] {
        let mut candidate = m.clone();
        fault::fail_nth(call, nth);
        let _ = compact_snapshot(dir.path(), &mut candidate, 2);
        fault::clear();
        assert_eq!(
            log_bytes(dir.path()),
            before,
            "{call:?}#{nth} must leave the live log byte-identical"
        );
    }
    compact_snapshot(dir.path(), &mut m, 2).unwrap();
    assert_eq!(log_bytes(dir.path()).len(), 28);
}

#[test]
fn trigger_fires_on_byte_threshold() {
    assert!(!snapshot_due(4 << 20, 10, 1 << 10));
    assert!(snapshot_due((4 << 20) + 1, 10, 1 << 10));
    // A snapshot larger than the log dominates: rewriting 8 MiB to reclaim
    // 5 MiB of log is not a saving.
    assert!(!snapshot_due(5 << 20, 10, 8 << 20));
    assert!(snapshot_due((8 << 20) + 1, 10, 8 << 20));
}

#[test]
fn trigger_fires_on_count_threshold() {
    assert!(!snapshot_due(1024, 4096, 1 << 30));
    assert!(snapshot_due(1024, 4097, 1 << 30));
}

// -- R rows ------------------------------------------------------------------

#[test]
fn recovery_applies_records_after_applied_through() {
    let (dir, live) = seeded_dir(5);
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(table_ids(&back), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(table_ids(&back), table_ids(&live));
    assert_eq!(back.applied_through, 5);
    assert_eq!(back.next_edit_id, 6);
}

#[test]
fn recovery_skips_records_at_or_below_applied_through() {
    let (dir, live) = seeded_dir(4);
    // Put a snapshot that already contains edits 1 and 2 in front of the same
    // four-record log — exactly the S2/S3 state. Replaying 1 and 2 again would
    // trip AddTable's "id absent" precondition, so a skip that does not happen
    // is a loud failure rather than a silent one.
    let mut at_two = Manifest {
        caps: ondadb::format::CAP_MANIFEST_EDITS,
        generation: 2,
        applied_through: 2,
        next_edit_id: 3,
        ..Manifest::default()
    };
    apply_edit(
        &mut at_two,
        &VersionEdit::new(vec![
            Op::CreateCf {
                name: "default".into(),
                config: vec![1, 2],
            },
            Op::AddTable {
                cf: "default".into(),
                meta: SstMeta {
                    id: 1,
                    max_seq: 10,
                    ..SstMeta::default()
                },
            },
        ]),
    )
    .unwrap();
    for id in 1..=2u64 {
        apply_edit(
            &mut at_two,
            &VersionEdit::new(vec![Op::AddTable {
                cf: "default".into(),
                meta: SstMeta {
                    id: id + 1,
                    max_seq: 10 + id,
                    ..SstMeta::default()
                },
            }]),
        )
        .unwrap();
    }
    at_two.save(manifest_path(dir.path())).unwrap();
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(
        table_ids(&back),
        table_ids(&live),
        "each edit must be applied exactly once"
    );
    assert_eq!(back.applied_through, 4);
}

/// R7 — a log whose base is ahead of the snapshot describes edits the snapshot
/// never saw, so the two files do not belong together.
#[test]
fn recovery_rejects_a_base_ahead_of_applied_through() {
    let (dir, _live) = seeded_dir(0);
    EditLog::create(
        dir.path(),
        EditLogHeader {
            base_applied_through: 7,
            snapshot_generation: 1,
        },
    )
    .unwrap();
    let err = recover_catalog(dir.path()).expect_err("a base ahead of the snapshot is corruption");
    assert_eq!(err.kind(), "corruption");
    assert!(err.to_string().contains("ahead"), "{err}");
}

/// A *behind* base is legal, and is the crash-between-the-renames state: the
/// generation disagreeing with the snapshot's carries no decision power.
#[test]
fn recovery_accepts_a_stale_generation_in_the_log_header() {
    let (dir, live) = seeded_dir(2);
    let header = open_log_for_append(dir.path()).unwrap().unwrap().header();
    assert_eq!(header.base_applied_through, 0);
    assert!(header.snapshot_generation < live.generation + 1);
    let mut m = live.clone();
    fault::fail_nth(fault::Call::Write, 2); // crash between the two renames
    let _ = compact_snapshot(dir.path(), &mut m, 2);
    fault::clear();
    let back = recover_catalog(dir.path()).expect("a stale generation must not reject");
    assert_eq!(table_ids(&back), table_ids(&live));
}

/// R1 — a header this binary cannot verify is corruption, never a torn tail:
/// it is fsynced before the first record is ever appended.
#[test]
fn recovery_rejects_a_bad_log_header() {
    let (dir, _live) = seeded_dir(2);
    let mut bytes = log_bytes(dir.path());
    bytes[10] ^= 0xFF; // inside base_applied_through, so the header CRC fails
    std::fs::write(edit_log_path(dir.path()), &bytes).unwrap();
    let err = recover_catalog(dir.path()).expect_err("a bad header CRC is corruption");
    assert_eq!(err.kind(), "corruption");
}

/// R3 — a missing id in the middle of the file.
#[test]
fn recovery_rejects_an_id_gap() {
    let (dir, _live) = seeded_dir(3);
    let mut log = EditLog::create(
        dir.path(),
        EditLogHeader {
            base_applied_through: 0,
            snapshot_generation: 1,
        },
    )
    .unwrap();
    for id in [1u64, 3] {
        log.append(
            id,
            &VersionEdit::new(vec![Op::AddTable {
                cf: "default".into(),
                meta: SstMeta {
                    id: id + 10,
                    ..SstMeta::default()
                },
            }]),
        )
        .unwrap();
    }
    let err = recover_catalog(dir.path()).expect_err("an id gap is corruption");
    assert_eq!(err.kind(), "corruption");
    assert!(err.to_string().contains("gap or a duplicate"), "{err}");
}

/// R2 — a complete record whose CRC fails is corruption, not a torn tail.
#[test]
fn recovery_rejects_a_complete_record_with_a_bad_crc() {
    let (dir, _live) = seeded_dir(2);
    let mut bytes = log_bytes(dir.path());
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(edit_log_path(dir.path()), &bytes).unwrap();
    let err = recover_catalog(dir.path()).expect_err("a bad record CRC is corruption");
    assert_eq!(err.kind(), "corruption");
}

/// The other side of R2/R5: a genuinely EOF-truncated tail ends replay cleanly,
/// and the next append overwrites it.
#[test]
fn recovery_treats_an_eof_truncated_tail_as_clean() {
    let (dir, _live) = seeded_dir(3);
    let bytes = log_bytes(dir.path());
    for cut in 1..12 {
        std::fs::write(edit_log_path(dir.path()), &bytes[..bytes.len() - cut]).unwrap();
        let back = recover_catalog(dir.path()).expect("a torn tail is not corruption");
        assert_eq!(back.applied_through, 2, "cut={cut}");
        assert_eq!(table_ids(&back), vec![1, 2, 3]);
    }
    // Reopening for append truncates the partial frame away, so the record
    // written next is reachable rather than stranded behind it.
    let mut log = open_log_for_append(dir.path()).unwrap().unwrap();
    log.append(
        3,
        &VersionEdit::new(vec![Op::AddTable {
            cf: "default".into(),
            meta: SstMeta {
                id: 4,
                ..SstMeta::default()
            },
        }]),
    )
    .unwrap();
    assert_eq!(
        table_ids(&recover_catalog(dir.path()).unwrap()),
        vec![1, 2, 3, 4]
    );
}

/// R4 — a record whose preconditions do not hold against the catalog it lands
/// on is an invalid transition, and therefore corruption.
#[test]
fn recovery_rejects_a_failed_precondition() {
    let (dir, _live) = seeded_dir(1);
    let mut log = open_log_for_append(dir.path()).unwrap().unwrap();
    // Table 2 was already added by edit 1.
    log.append(
        2,
        &VersionEdit::new(vec![Op::AddTable {
            cf: "default".into(),
            meta: SstMeta {
                id: 2,
                ..SstMeta::default()
            },
        }]),
    )
    .unwrap();
    let err = recover_catalog(dir.path()).expect_err("a duplicate id is an invalid transition");
    assert_eq!(err.kind(), "corruption");
    assert!(err.to_string().contains("precondition"), "{err}");
}

/// R5 — a log present without `CAP_MANIFEST_EDITS` means a newer binary wrote
/// state this one cannot interpret.
#[test]
fn recovery_rejects_a_log_without_the_capability() {
    let (dir, live) = seeded_dir(2);
    let mut without = live.clone();
    without.caps = 0;
    without.save(manifest_path(dir.path())).unwrap();
    let err = recover_catalog(dir.path()).expect_err("a log without the capability is corruption");
    assert_eq!(err.kind(), "corruption");
    assert!(err.to_string().contains("CAP_MANIFEST_EDITS"), "{err}");
}

/// R6 — no log at all is the state before the first append, and the state a
/// fresh backup/checkpoint destination is handed.
#[test]
fn recovery_accepts_a_missing_log_for_the_transition_state() {
    let (dir, live) = seeded_dir(0);
    std::fs::remove_file(edit_log_path(dir.path())).unwrap();
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(table_ids(&back), table_ids(&live));
    assert_eq!(back.applied_through, 0);
    assert_eq!(back.next_edit_id, 1);
    assert!(open_log_for_append(dir.path()).unwrap().is_none());
}

#[test]
fn recovery_reconciles_next_file_id_and_global_seq() {
    let (dir, _live) = seeded_dir(0);
    let mut log = open_log_for_append(dir.path()).unwrap().unwrap();
    log.append(
        1,
        &VersionEdit::new(vec![Op::AddTable {
            cf: "default".into(),
            meta: SstMeta {
                id: 900,
                max_seq: 5_000,
                ..SstMeta::default()
            },
        }]),
    )
    .unwrap();
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(back.next_file_id, 901, "the allocator must clear every id");
    assert_eq!(back.global_seq, 5_000, "the sequence must dominate max_seq");
}

/// R8 — the two temp files are crash artifacts, never state: recovery never
/// reads them and the open-time sweep removes them.
#[test]
fn recovery_unlinks_leftover_tmp_files() {
    let (dir, live) = seeded_dir(1);
    let manifest_tmp = manifest_path(dir.path()).with_extension("tmp");
    std::fs::write(&manifest_tmp, b"garbage that is not a manifest").unwrap();
    std::fs::write(edit_log_tmp_path(dir.path()), b"garbage that is not a log").unwrap();
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(table_ids(&back), table_ids(&live));
    sweep_manifest_temp_files(dir.path()).unwrap();
    assert!(!manifest_tmp.exists());
    assert!(!edit_log_tmp_path(dir.path()).exists());
    // Idempotent: a missing temp file is not an error.
    sweep_manifest_temp_files(dir.path()).unwrap();
}

// -- Scale ------------------------------------------------------------------

/// A catalog with `n` bottom-level tables, shaped like a real parts/tiers
/// deployment (every table carries a partition, a tier and an age stamp).
fn big_catalog(n: u64) -> Manifest {
    let mut m = Manifest {
        caps: ondadb::format::CAP_MANIFEST_EDITS,
        ..Manifest::default()
    };
    let mut ops = vec![Op::CreateCf {
        name: "t_post".into(),
        config: vec![0u8; 256],
    }];
    for id in 1..=n {
        ops.push(Op::AddTable {
            cf: "t_post".into(),
            meta: SstMeta {
                id,
                level: 6,
                num_entries: 100_000,
                num_tombstones: 12,
                max_seq: id * 1_000,
                klog_size: 64 << 20,
                vlog_size: 128 << 20,
                min_key: format!("tenant-{id:08}/aaaaaaaa").into_bytes(),
                max_key: format!("tenant-{id:08}/zzzzzzzz").into_bytes(),
                partition: Some(format!("p{}", id % 64)),
                tier: Some("cold".into()),
                max_entry_time: Some(1_700_000_000_000_000_000 + id as i64),
                object: Some(format!("cf-t_post/00000000deadbeef-{id}")),
                last_compaction_time: Some(1_700_000_000_000_000_000 + id as i64),
            },
        });
    }
    apply_edit(&mut m, &VersionEdit::new(ops)).unwrap();
    m
}

/// One structural change at table `id`: what a part move writes.
fn one_change(id: u64) -> VersionEdit {
    VersionEdit::new(vec![Op::UpdateTable {
        cf: "t_post".into(),
        id,
        update: TableUpdate::relocation(
            Some("warm".into()),
            Some(format!("cf-t_post/00000000deadbeef-{id}")),
        ),
    }])
}

/// The acceptance criterion: between snapshots, the bytes a structural change
/// makes durable are O(edit), not O(catalog), and replaying a full log stays
/// bounded.
///
/// The bound is expressed as a ratio against the full-rewrite cost measured in
/// the same process, never as an absolute byte count or a wall-clock figure —
/// this machine's IO is thermally noisy (AGENTS.md), and an absolute threshold
/// would be a flake generator. The ratio is ~4 orders of magnitude, so a 100x
/// margin still fails loudly if the append ever becomes O(catalog).
#[test]
fn ten_thousand_tables_replay_within_the_bound() {
    const TABLES: u64 = 10_000;
    const EDITS: u64 = 4_096;

    let dir = tempfile::tempdir().unwrap();
    let mut m = big_catalog(TABLES);
    let mut log = compact_snapshot(dir.path(), &mut m, 0).unwrap();
    let snapshot_bytes = std::fs::metadata(manifest_path(dir.path())).unwrap().len();

    let mut edit_bytes = 0u64;
    for i in 1..=EDITS {
        let edit = one_change((i % TABLES) + 1);
        edit_bytes += log.append(i, &edit).unwrap();
        apply_edit(&mut m, &edit).unwrap();
    }

    // O(edit): one structural change costs a record, not a catalog.
    let per_edit = edit_bytes / EDITS;
    assert!(
        per_edit * 100 < snapshot_bytes,
        "one edit is {per_edit} bytes against a {snapshot_bytes}-byte snapshot; \
         an append that is O(catalog) would not clear this margin"
    );
    // ...and the whole log between two snapshots still costs less than one
    // rewrite, which is the property the compaction trigger has to preserve.
    assert!(
        edit_bytes < snapshot_bytes,
        "the whole log ({edit_bytes} bytes) must cost less than one full \
         rewrite ({snapshot_bytes} bytes)"
    );

    // Bounded replay: the recovered catalog is exactly the live one.
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(back.applied_through, EDITS);
    assert_eq!(back.cfs[0].sstables.len() as u64, TABLES);
    assert_eq!(
        back.cfs[0]
            .sstables
            .iter()
            .filter(|s| s.tier.as_deref() == Some("warm"))
            .count(),
        m.cfs[0]
            .sstables
            .iter()
            .filter(|s| s.tier.as_deref() == Some("warm"))
            .count()
    );
}

/// Sizing probe for the 2.2 bench record: append bytes, fsync count and replay
/// time for a 10k-table catalog under 4096 structural changes, edits versus
/// full snapshots. Not a gate — run it with
/// `cargo test --release --test manifest_edits -- --ignored --nocapture`.
#[test]
#[ignore = "sizing probe, not a gate — run with --ignored --nocapture"]
fn manifest_edit_scale_probe() {
    const TABLES: u64 = 10_000;
    const EDITS: u64 = 4_096;
    use std::time::Instant;

    let mut m = big_catalog(TABLES);

    // Full-snapshot mode: every structural change rewrites and fsyncs the whole
    // manifest (one write fsync + one directory fsync per change).
    let full_dir = tempfile::tempdir().unwrap();
    let full_path = manifest_path(full_dir.path());
    m.save(&full_path).unwrap();
    let snapshot_bytes = std::fs::metadata(&full_path).unwrap().len();
    let t0 = Instant::now();
    let mut full_bytes = 0u64;
    for i in 1..=EDITS {
        let edit = one_change((i % TABLES) + 1);
        apply_edit(&mut m, &edit).unwrap();
        m.save(&full_path).unwrap();
        full_bytes += snapshot_bytes;
    }
    let full_write = t0.elapsed();
    let t0 = Instant::now();
    let _ = Manifest::load(&full_path).unwrap();
    let full_replay = t0.elapsed();

    // Edit mode: one appended, fsynced record per change.
    let mut m2 = big_catalog(TABLES);
    let edit_dir = tempfile::tempdir().unwrap();
    let mut log = compact_snapshot(edit_dir.path(), &mut m2, 0).unwrap();
    let t0 = Instant::now();
    let mut edit_bytes = 0u64;
    for i in 1..=EDITS {
        let edit = one_change((i % TABLES) + 1);
        edit_bytes += log.append(i, &edit).unwrap();
    }
    let edit_write = t0.elapsed();
    let t0 = Instant::now();
    let back = recover_catalog(edit_dir.path()).unwrap();
    let edit_replay = t0.elapsed();
    assert_eq!(back.applied_through, EDITS);

    println!("tables={TABLES} changes={EDITS} snapshot={snapshot_bytes} bytes");
    println!(
        "full   bytes={full_bytes:>12} fsyncs={:>6} write={full_write:>12.2?} \
         replay={full_replay:>10.2?}",
        EDITS * 2
    );
    println!(
        "edits  bytes={edit_bytes:>12} fsyncs={:>6} write={edit_write:>12.2?} \
         replay={edit_replay:>10.2?}",
        EDITS
    );
    println!(
        "ratio  bytes={:.1}x write={:.1}x",
        full_bytes as f64 / edit_bytes as f64,
        full_write.as_secs_f64() / edit_write.as_secs_f64()
    );
}

/// R6 (release fence) — a decoder frozen at VERSION 1, compiled into this test
/// rather than expressed as a constant, must refuse a v2 snapshot. That is what
/// makes "an older binary fails closed" a tested claim rather than an assertion
/// about a number this change could have edited.
#[test]
fn a_frozen_version_one_decoder_refuses_a_v2_snapshot() {
    /// The version gate exactly as every pre-1.0 release compiled it.
    fn version_one_decoder(bytes: &[u8]) -> Result<(), String> {
        if bytes.len() < 12 {
            return Err("short".into());
        }
        let crc_at = bytes.len() - 4;
        let stored = u32::from_le_bytes(bytes[crc_at..].try_into().unwrap());
        if stored != ondadb::encoding::checksum(&bytes[..crc_at]) {
            return Err("crc".into());
        }
        if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != 0x5756_4D46 {
            return Err("magic".into());
        }
        match u32::from_le_bytes(bytes[4..8].try_into().unwrap()) {
            1 => Ok(()),
            v => Err(format!("version {v}")),
        }
    }

    let (dir, _live) = seeded_dir(1);
    let v2 = std::fs::read(manifest_path(dir.path())).unwrap();
    assert_eq!(
        version_one_decoder(&v2),
        Err("version 2".into()),
        "a v2 snapshot must fail closed on a frozen v1 decoder"
    );
    // ...while a database that never enabled a capability still writes v1.
    let legacy_dir = tempfile::tempdir().unwrap();
    Manifest::default()
        .save(manifest_path(legacy_dir.path()))
        .unwrap();
    let bytes = std::fs::read(manifest_path(legacy_dir.path())).unwrap();
    assert_eq!(version_one_decoder(&bytes), Ok(()));
}
