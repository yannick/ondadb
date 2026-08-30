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
        range_count: 4,
        range_min_seq: 101,
        range_max_seq: 202,
        range_min_key: Some(b"rng-min".to_vec()),
        range_max_key: Some(b"rng-max".to_vec()),
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
        // CAP_RANGE_DELETES is load-bearing here, not decoration: the manifest
        // encoder only emits the ONDARNG1 tail for a database that holds the
        // bit, so without it the range fields would round-trip through the
        // edit log but vanish from the encoded catalog — and the byte-identity
        // assertion at the end of the guard would pass while losing data.
        caps: ondadb::format::CAP_MANIFEST_EDITS
            | ondadb::format::CAP_EXTENDED_RECORDS
            | ondadb::format::CAP_RANGE_DELETES,
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

/// The inventory guard: 5 `Manifest` fields + 3 `CfManifest` fields + 19
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
        // CfManifest.sstables, and all 19 SstMeta fields
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

    // SstMeta: 19 fields, compared field by field so a failure names the field.
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
    assert_eq!(
        t.last_compaction_time, table.last_compaction_time,
        "SstMeta.last_compaction_time"
    );
    assert_eq!(t.range_count, table.range_count, "SstMeta.range_count");
    assert_eq!(
        t.range_min_seq, table.range_min_seq,
        "SstMeta.range_min_seq"
    );
    assert_eq!(
        t.range_max_seq, table.range_max_seq,
        "SstMeta.range_max_seq"
    );
    assert_eq!(
        t.range_min_key, table.range_min_key,
        "SstMeta.range_min_key"
    );
    assert_eq!(
        t.range_max_key, table.range_max_key,
        "SstMeta.range_max_key"
    );

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
                ..Default::default()
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

// ---------------------------------------------------------------------------
// Slice 9: the migrated call sites, with `CAP_MANIFEST_EDITS` on
// ---------------------------------------------------------------------------
//
// Every one of these drives a real database. The point is not that the edit
// arrives — slices 2-8 proved the codec and the transaction — but that each
// migrated site emits ONE record with the right ops, and that the effects the
// site is responsible for (WAL reclaim, file deletion, rollback) hang off that
// record's fsync rather than off a manifest rewrite.

use ondadb::manifest_edit::decode_records;
use ondadb::{Options, TierDef, DB};
use std::sync::Arc;
use std::time::Duration;

/// A writable database with the edit log enabled and nothing in its log yet.
fn live_db(dir: &Path) -> DB {
    let db = DB::open(Options::new(dir.path_str())).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
        .unwrap();
    db
}

/// `Path::to_str().unwrap()`, spelled once.
trait PathStr {
    fn path_str(&self) -> &str;
}
impl PathStr for Path {
    fn path_str(&self) -> &str {
        self.to_str().expect("test paths are UTF-8")
    }
}

/// A column family the compactor leaves alone, so a test that counts records
/// counts only the ones its own operation wrote.
fn quiet_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        l1_file_count_trigger: 64,
        ..ColumnFamilyConfig::default()
    }
}

/// Every edit currently in the log, in order.
fn records(dir: &Path) -> Vec<VersionEdit> {
    match std::fs::read(edit_log_path(dir)) {
        Ok(data) => decode_records(&data)
            .unwrap()
            .0
            .into_iter()
            .map(|r| r.edit)
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("{e}"),
    }
}

/// One line per op, so an assertion reads like the migration table in
/// `docs/plans/phase-2-formats/features/22-manifest-edit-log.md`.
fn shape(edit: &VersionEdit) -> Vec<String> {
    edit.ops
        .iter()
        .map(|op| match op {
            Op::AddTable { cf, meta } => format!("AddTable({cf},{})", meta.id),
            Op::RemoveTable { cf, id, .. } => format!("RemoveTable({cf},{id})"),
            Op::UpdateTable { cf, id, update } => {
                format!("UpdateTable({cf},{id},mask={:#x})", update.mask())
            }
            Op::CreateCf { name, .. } => format!("CreateCf({name})"),
            Op::DropCf { name } => format!("DropCf({name})"),
            Op::SetCfConfig { name, .. } => format!("SetCfConfig({name})"),
            Op::SetNextFileId(v) => format!("SetNextFileId({v})"),
            Op::SetGlobalSeq(v) => format!("SetGlobalSeq({v})"),
            Op::SetWalLayout(l) => format!("SetWalLayout({l:?})"),
            Op::SetNonce(n) => format!("SetNonce({n})"),
            Op::SetCapability(b) => format!("SetCapability({b})"),
            Op::RemoveTables { cf, ids } => format!("RemoveTables({cf},{ids:?})"),
        })
        .collect()
}

fn wal_files(cf_dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(cf_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("wal-"))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dst = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &dst);
        } else {
            std::fs::copy(entry.path(), &dst).unwrap();
        }
    }
}

/// AGENTS.md invariant 1, restated: the flush's WAL is released only after the
/// `AddTable` record's fsync — and one flush is one record.
#[test]
fn flush_writes_one_add_table_edit_and_reclaims_its_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let before = records(dir.path()).len();
    let cf_dir = dir.path().join("cf-default");
    for i in 0..64u32 {
        db.put(&cf, format!("k{i:04}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    let wal_before = wal_files(&cf_dir);
    assert!(!wal_before.is_empty(), "the writes went somewhere");

    db.flush_memtable(&cf).unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1, "one flush, one record");
    let ops = shape(&after[before]);
    assert_eq!(ops.len(), 1, "an L0 flush adds exactly one table: {ops:?}");
    assert!(ops[0].starts_with("AddTable(default,"), "{ops:?}");

    // The flushed generation's WAL is gone; only the live one remains.
    let wal_after = wal_files(&cf_dir);
    assert!(
        wal_after.len() < wal_before.len() + 1,
        "the flushed WAL generation survived the edit fsync: {wal_before:?} -> {wal_after:?}"
    );
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(back.cfs[0].sstables.len(), 1);
    db.close().unwrap();
}

/// A5 at database scope: the state a crash leaves *between* the edit's fsync
/// and the WAL unlink — SSTable present, edit durable, WAL still on disk —
/// reopens with every row exactly once. Reconstructed byte-for-byte rather than
/// simulated: `B` is the pre-flush directory with `A`'s post-flush catalog and
/// SSTables dropped in, which is exactly what that window looks like.
#[test]
fn crash_between_edit_fsync_and_wal_delete_replays_cleanly() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    {
        let db = live_db(a.path());
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        for i in 0..64u32 {
            db.put(&cf, format!("k{i:04}").as_bytes(), b"v", Duration::ZERO)
                .unwrap();
        }
        // Snapshot the pre-flush directory: the WAL holds every row.
        copy_tree(a.path(), b.path());
        db.flush_memtable(&cf).unwrap();
        // Now graft the post-flush catalog and its SSTable onto that snapshot.
        std::fs::copy(manifest_path(a.path()), manifest_path(b.path())).unwrap();
        std::fs::copy(edit_log_path(a.path()), edit_log_path(b.path())).unwrap();
        for entry in std::fs::read_dir(a.path().join("cf-default")).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let n = name.to_string_lossy();
            if n.ends_with(".klog") || n.ends_with(".vlog") {
                std::fs::copy(entry.path(), b.path().join("cf-default").join(&*n)).unwrap();
            }
        }
        db.close().unwrap();
    }
    let db = DB::open(Options::new(b.path().path_str())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    for i in 0..64u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:04}").as_bytes()).unwrap(),
            b"v",
            "row {i} did not survive the replay"
        );
    }
    assert!(
        cf.approximate_len() >= 64,
        "the replay must not have dropped rows"
    );
    db.close().unwrap();
}

/// The shared WAL covers every CF slice, so the slices travel in ONE record.
///
/// The flush is driven by the real trigger — the shared memtable overflowing —
/// because `flush_memtable` rotates a *per-CF* memtable, which unified mode does
/// not have. The wait is on the observable outcome with a deadline, never a
/// fixed sleep.
#[test]
fn unified_flush_writes_one_edit_for_every_cf_slice() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        unified_memtable: true,
        unified_memtable_write_buffer_size: 64 * 1024,
        ..Options::new(dir.path().path_str())
    };
    let db = DB::open(opts).unwrap();
    let cfs = db
        .create_column_families(&[("a", quiet_cfg()), ("b", quiet_cfg())])
        .unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
        .unwrap();
    let before = records(dir.path()).len();
    for i in 0..4_000u32 {
        db.put(
            &cfs[0],
            format!("a{i:06}").as_bytes(),
            b"VA",
            Duration::ZERO,
        )
        .unwrap();
        db.put(
            &cfs[1],
            format!("b{i:06}").as_bytes(),
            b"VB",
            Duration::ZERO,
        )
        .unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while records(dir.path()).len() == before && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }

    let after = records(dir.path());
    assert!(after.len() > before, "the unified flush wrote a record");
    // Every record a unified flush writes covers every CF that had a slice —
    // one `AddTable` per CF, never one record per CF.
    for edit in &after[before..] {
        let ops = shape(edit);
        assert_eq!(ops.len(), 2, "one AddTable per CF slice: {ops:?}");
        assert!(ops.iter().any(|o| o.starts_with("AddTable(a,")), "{ops:?}");
        assert!(ops.iter().any(|o| o.starts_with("AddTable(b,")), "{ops:?}");
    }
    db.close().unwrap();
}

/// Ingest is an `AddTable` producer and belongs with flush: however many tables
/// it rolled, they are published by one record.
#[test]
fn ingest_finish_writes_one_edit_for_all_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                // `roll_bytes` is `max(write_buffer_size, 1 MiB)`, so this is
                // the smallest roll the engine offers; ~3 MiB then rolls three.
                write_buffer_size: 1 << 20,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let before = records(dir.path()).len();

    let mut ingestion = db.start_ingestion(&cf).unwrap();
    for i in 0..3_000u32 {
        ingestion
            .write(format!("k{i:06}").as_bytes(), &[7u8; 1024], Duration::ZERO)
            .unwrap();
    }
    let n = ingestion.finish().unwrap();
    assert_eq!(n, 3_000);

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1, "one ingestion, one record");
    let ops = shape(&after[before]);
    assert!(
        ops.len() > 1,
        "the roll size should have produced several tables: {ops:?}"
    );
    assert!(
        ops.iter().all(|o| o.starts_with("AddTable(default,")),
        "{ops:?}"
    );
    db.close().unwrap();
}

/// The rollback story of the whole attach/ingest/clone family: a failed append
/// publishes nothing and unlinks every table the operation wrote.
#[test]
fn ingest_unlinks_its_tables_when_the_append_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let cf_dir = dir.path().join("cf-default");
    let before_ssts = sst_names(&cf_dir);
    let before_records = records(dir.path()).len();

    let mut ingestion = db.start_ingestion(&cf).unwrap();
    for i in 0..256u32 {
        ingestion
            .write(format!("k{i:06}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    // The next `Write` on this thread is the record append.
    fault::fail_nth(fault::Call::Write, 1);
    let err = ingestion.finish().expect_err("the append must fail");
    fault::clear();
    assert_eq!(err.kind(), "io", "{err:?}");

    assert_eq!(
        sst_names(&cf_dir),
        before_ssts,
        "a failed append must leave no table behind"
    );
    assert_eq!(
        records(dir.path()).len(),
        before_records,
        "nothing reached the log"
    );
    assert!(db.get(&cf, b"k000000").is_err() || db.get(&cf, b"k000000").is_ok());
    drop(db); // poisoned by the durability failure
}

fn sst_names(cf_dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(cf_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".klog") || n.ends_with(".vlog"))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// One compaction is one record: every input retired and every output added,
/// and the inputs are unlinked only after it.
#[test]
fn compaction_writes_one_edit_with_removes_and_adds() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                l1_file_count_trigger: 64, // no background compaction races us
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for round in 0..3u32 {
        for i in 0..64u32 {
            db.put(
                &cf,
                format!("k{i:04}").as_bytes(),
                format!("v{round}").as_bytes(),
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
    }
    let cf_dir = dir.path().join("cf-default");
    let before_files = sst_names(&cf_dir);
    let before = records(dir.path()).len();

    db.compact(&cf).unwrap();

    let after = records(dir.path());
    assert!(after.len() > before, "the compaction wrote a record");
    let ops = shape(&after[before]);
    assert!(
        ops[0].starts_with("RemoveTables(default,"),
        "the inputs are retired first: {ops:?}"
    );
    assert!(
        ops[1..].iter().all(|o| o.starts_with("AddTable(default,")),
        "and the outputs added in the same record: {ops:?}"
    );
    assert_ne!(
        sst_names(&cf_dir),
        before_files,
        "the inputs are unlinked after the edit's fsync"
    );
    assert_eq!(db.get(&cf, b"k0000").unwrap(), b"v2");
    db.close().unwrap();
}

/// Detach's commit point moved from the manifest rewrite to the edit fsync;
/// what it writes is one `RemoveTables`.
#[test]
fn detach_writes_one_remove_tables_edit() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db.create_column_family("default", parts_cfg()).unwrap();
    materialize_parts(&db, &cf);
    let before = records(dir.path()).len();

    db.detach_part(&cf, "img").unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1, "one detach, one record");
    let ops = shape(&after[before]);
    assert_eq!(ops.len(), 1, "{ops:?}");
    assert!(ops[0].starts_with("RemoveTables(default,"), "{ops:?}");
    assert!(db.get(&cf, b"img/000").is_err());
    db.close().unwrap();
}

/// The mover's flip is one record of `UpdateTable{Tier, Object}` — a partially
/// applied move is not representable.
#[test]
fn relocate_flip_is_one_edit_of_tier_and_object_updates() {
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().path_str());
    opts.tiers = vec![TierDef::new("hdd", hdd.path().path_str())];
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
        .unwrap();
    let cf = db.create_column_family("default", parts_cfg()).unwrap();
    materialize_parts(&db, &cf);
    let before = records(dir.path()).len();

    db.move_part_to_tier(&cf, "img", "hdd").unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1, "one flip, one record");
    let ops = shape(&after[before]);
    assert!(!ops.is_empty());
    for op in &ops {
        // 0x02 Tier | 0x04 Object — a move onto a shared tier changes both, and
        // a move onto an unshared one clears the object, which is the same mask.
        assert!(op.starts_with("UpdateTable(default,"), "{ops:?}");
        assert!(op.ends_with("mask=0x6)"), "{ops:?}");
    }
    let back = recover_catalog(dir.path()).unwrap();
    assert!(back.cfs[0]
        .sstables
        .iter()
        .any(|s| s.tier.as_deref() == Some("hdd")));
    db.close().unwrap();
}

/// Partition rules are not manifest fields — they ride the CF's config blob,
/// so a rule change is a `SetCFConfig`.
#[test]
fn partition_rule_changes_write_a_set_cf_config_edit() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let before = records(dir.path()).len();

    db.add_partition_rule(
        &cf,
        PartitionRule {
            prefix: b"img/".to_vec(),
            name: "img".into(),
        },
    )
    .unwrap();
    db.remove_partition_rule(&cf, b"img/").unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 2);
    assert_eq!(shape(&after[before]), vec!["SetCfConfig(default)"]);
    assert_eq!(shape(&after[before + 1]), vec!["SetCfConfig(default)"]);
    // A duplicate is still rejected before anything is written.
    let err = db
        .add_partition_rule(
            &cf,
            PartitionRule {
                prefix: b"a/".to_vec(),
                name: "a".into(),
            },
        )
        .and_then(|()| {
            db.add_partition_rule(
                &cf,
                PartitionRule {
                    prefix: b"a/".to_vec(),
                    name: "a".into(),
                },
            )
        })
        .expect_err("an exact-duplicate prefix is rejected");
    assert!(matches!(err, ondadb::OndaError::InvalidArgs(_)), "{err:?}");
    db.close().unwrap();
}

/// The batch always promised one manifest write; it is now one record with N
/// `CreateCF` ops.
#[test]
fn create_column_families_writes_one_edit_for_the_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let before = records(dir.path()).len();

    db.create_column_families(&[
        ("a", ColumnFamilyConfig::default()),
        ("b", ColumnFamilyConfig::default()),
        ("c", ColumnFamilyConfig::default()),
    ])
    .unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1, "one batch, one record");
    assert_eq!(
        shape(&after[before]),
        vec!["CreateCf(a)", "CreateCf(b)", "CreateCf(c)"]
    );
    // A colliding batch creates nothing and writes nothing.
    let err = db
        .create_column_families(&[
            ("d", ColumnFamilyConfig::default()),
            ("a", ColumnFamilyConfig::default()),
        ])
        .expect_err("a colliding name fails the batch");
    assert!(matches!(err, ondadb::OndaError::Exists(_)), "{err:?}");
    assert_eq!(records(dir.path()).len(), before + 1);
    db.close().unwrap();
}

/// The correctness improvement the inversion buys: the `DropCF` edit is durable
/// before the directory is unlinked, so no crash window leaves a catalog naming
/// a directory that is gone.
#[test]
fn drop_cf_makes_the_edit_durable_before_deleting_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family("victim", ColumnFamilyConfig::default())
        .unwrap();
    db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    let before = records(dir.path()).len();

    db.drop_column_family("victim").unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1);
    let ops = shape(&after[before]);
    assert_eq!(ops.len(), 2, "tables first, then the family: {ops:?}");
    assert!(ops[0].starts_with("RemoveTables(victim,"), "{ops:?}");
    assert_eq!(ops[1], "DropCf(victim)");
    assert!(!dir.path().join("cf-victim").exists());
    let back = recover_catalog(dir.path()).unwrap();
    assert!(back.cfs.iter().all(|c| c.name != "victim"));
    db.close().unwrap();
}

/// Clear is drop + create in ONE record, and the registry never shows a gap.
#[test]
fn clear_cf_is_one_drop_plus_create_edit() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    let before = records(dir.path()).len();

    let fresh = db.clear_column_family("default").unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1, "one clear, one record");
    let ops = shape(&after[before]);
    assert_eq!(ops.len(), 3, "{ops:?}");
    assert!(ops[0].starts_with("RemoveTables(default,"), "{ops:?}");
    assert_eq!(ops[1], "DropCf(default)");
    assert_eq!(ops[2], "CreateCf(default)");
    assert!(db.get(&fresh, b"k").is_err(), "the family is empty");
    assert!(
        db.get_column_family("default").is_some(),
        "and still registered"
    );
    db.close().unwrap();
}

/// The destination family is created *and* populated by one record, so it never
/// exists on disk as an empty catalog entry a crash could strand.
#[test]
fn clone_cf_is_one_create_plus_add_table_edit() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family("src", ColumnFamilyConfig::default())
        .unwrap();
    for i in 0..32u32 {
        db.put(&cf, format!("k{i:04}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let before = records(dir.path()).len();

    let dst = db.clone_column_family("src", "dst").unwrap();

    let after = records(dir.path());
    assert_eq!(after.len(), before + 1, "one clone, one record");
    let ops = shape(&after[before]);
    assert_eq!(ops[0], "CreateCf(dst)");
    assert!(ops.len() > 1, "the tables ride the same record: {ops:?}");
    assert!(
        ops[1..].iter().all(|o| o.starts_with("AddTable(dst,")),
        "{ops:?}"
    );
    assert_eq!(db.get(&dst, b"k0000").unwrap(), b"v");
    db.close().unwrap();
}

/// `close` is a final snapshot compaction: the log restarts empty, so the next
/// open replays nothing, and a failed one is the caller's to see.
#[test]
fn close_compacts_the_log_and_reports_failure() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = live_db(dir.path());
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        assert!(!records(dir.path()).is_empty(), "the flush wrote a record");
        db.close().unwrap();
    }
    assert!(
        records(dir.path()).is_empty(),
        "close compacted the log away"
    );
    let back = recover_catalog(dir.path()).unwrap();
    assert_eq!(back.applied_through, back.next_edit_id - 1);
    assert_eq!(back.cfs[0].sstables.len(), 1);

    // ...and a failed final compaction is returned, not swallowed.
    let db = DB::open(Options::new(dir.path().path_str())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v");
    std::fs::create_dir(manifest_path(dir.path()).with_extension("tmp")).unwrap();
    let err = db
        .close()
        .expect_err("close must report its persist failure");
    assert_eq!(err.kind(), "io", "{err:?}");
    std::fs::remove_dir(manifest_path(dir.path()).with_extension("tmp")).unwrap();
}

/// L0 is read newest-first and `ColumnFamily::load` preserves the manifest's
/// within-level order, so a replayed `AddTable` at level 0 must land where
/// `install_handles_l0` put the handle. Appending instead would reopen the
/// family with its newest table shadowed by its oldest.
#[test]
fn l0_add_tables_replay_newest_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = live_db(dir.path());
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                l1_file_count_trigger: 64,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for round in 0..4u32 {
        db.put(&cf, b"k", format!("v{round}").as_bytes(), Duration::ZERO)
            .unwrap();
        db.flush_memtable(&cf).unwrap();
    }
    // In memory the newest write wins.
    assert_eq!(db.get(&cf, b"k").unwrap(), b"v3");
    // And so it must after a replay of the same four records.
    let back = recover_catalog(dir.path()).unwrap();
    let l0: Vec<u64> = back.cfs[0]
        .sstables
        .iter()
        .filter(|s| s.level == 0)
        .map(|s| s.id)
        .collect();
    let mut newest_first = l0.clone();
    newest_first.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(l0, newest_first, "L0 replayed oldest-first: {l0:?}");
    db.close().unwrap();

    let db = DB::open(Options::new(dir.path().path_str())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(
        db.get(&cf, b"k").unwrap(),
        b"v3",
        "the newest L0 table must still shadow the older ones after a reopen"
    );
    db.close().unwrap();
}

/// Site 1 and site 2: the two scalar catalog fields, each written once.
#[test]
fn nonce_and_wal_layout_edits_apply_once() {
    let dir = tempfile::tempdir().unwrap();
    let shared = tempfile::tempdir().unwrap();
    {
        // A database with a shared tier mints a nonce; without the capability
        // that is a snapshot, so enable it first and reopen.
        let db = live_db(dir.path());
        db.create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.close().unwrap();
    }
    let mut opts = Options::new(dir.path().path_str());
    opts.tiers = vec![TierDef::new("pub", shared.path().path_str()).shared()];
    let db = DB::open(opts.clone()).unwrap();
    let edits = records(dir.path());
    assert_eq!(
        edits.iter().flat_map(shape).collect::<Vec<_>>(),
        vec![format!(
            "SetNonce({})",
            recover_catalog(dir.path()).unwrap().instance_nonce.unwrap()
        )],
        "the nonce is minted by one edit"
    );
    db.close().unwrap();

    // Reopening does not re-mint: `SetNonce` would fail its own precondition.
    let db = DB::open(opts).unwrap();
    assert!(
        records(dir.path()).is_empty(),
        "close compacted, and the reopen minted nothing"
    );
    db.close().unwrap();
}

// -- fixtures shared with the parts tests ------------------------------------

fn parts_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        partition_rules: vec![
            PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            },
            PartitionRule {
                prefix: b"log/".to_vec(),
                name: "log".into(),
            },
        ],
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    }
}

fn materialize_parts(db: &DB, cf: &Arc<ondadb::ColumnFamily>) {
    for i in 0..5u32 {
        db.put(cf, format!("img/{i:03}").as_bytes(), b"IMG", Duration::ZERO)
            .unwrap();
        db.put(cf, format!("log/{i:03}").as_bytes(), b"LOG", Duration::ZERO)
            .unwrap();
        db.put(cf, format!("etc/{i:03}").as_bytes(), b"ETC", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(cf).unwrap();
    db.compact(cf).unwrap();
}

/// Structural-operation latency at the 10k-table fixture: flush and compaction,
/// full-snapshot mode versus the edit log (2.2 slice 11).
///
/// This is the `../bench`-side half of the acceptance criterion. The model probe
/// above measures the catalog layer in isolation; this one measures what a
/// running database actually pays for one flush and one compaction when its
/// catalog is large — which is the cost RV-M5 is about, and the only place a
/// user feels it.
///
/// Shape: a `bulk` column family carrying the catalog weight (10k tables, never
/// read, never compacted) and a `hot` one where the measured operations happen.
/// `persist_manifest` rebuilds **every** CF, so a flush on `hot` re-encodes all
/// 10k of `bulk`'s tables in full-snapshot mode and appends ~100 bytes with the
/// log on. The fixture is built once and copied per run: building it costs
/// O(n^2) bytes in full-snapshot mode, which is the very thing under test.
///
/// Not a gate — timings on this machine are thermally noisy (AGENTS.md).
/// Run with
/// `cargo test --release --test manifest_edits -- --ignored --nocapture
///  structural_op_latency_probe`.
#[test]
#[ignore = "sizing probe, not a gate — run with --ignored --nocapture"]
fn structural_op_latency_probe() {
    use std::time::Instant;

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }
    let tables = env_usize("ONDADB_PROBE_TABLES", 10_000);
    let flushes = env_usize("ONDADB_PROBE_FLUSHES", 200);
    let compactions = env_usize("ONDADB_PROBE_COMPACTIONS", 25);
    let runs = env_usize("ONDADB_PROBE_RUNS", 5);

    /// A family the compactor never picks: the catalog ballast.
    fn ballast_cfg() -> ColumnFamilyConfig {
        ColumnFamilyConfig {
            l1_file_count_trigger: u32::MAX,
            l1_base_bytes: u64::MAX / 4,
            write_buffer_size: 1 << 20,
            ..ColumnFamilyConfig::default()
        }
    }
    /// The family under measurement: flushes stay flushes until we ask.
    fn hot_cfg() -> ColumnFamilyConfig {
        ColumnFamilyConfig {
            l1_file_count_trigger: u32::MAX,
            l1_base_bytes: u64::MAX / 4,
            ..ColumnFamilyConfig::default()
        }
    }

    fn pct(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return f64::NAN;
        }
        let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[idx]
    }

    // -- fixture ------------------------------------------------------------
    //
    // The ballast catalog is written directly rather than flushed into
    // existence: producing 10k tables through 10k real flushes costs O(n^2)
    // manifest bytes in the very mode under test, which would make the fixture
    // the experiment. `bulk`'s tables are placeholders — never read, never
    // compacted (its triggers are set past any real value) — because what is
    // being measured is the cost of making the *catalog* durable, and that cost
    // is a function of the catalog, not of the bytes behind it.
    fn build_fixture(dir: &Path, tables: usize) {
        std::fs::create_dir_all(dir.join("cf-bulk")).unwrap();
        std::fs::create_dir_all(dir.join("cf-hot")).unwrap();
        let mut ssts = Vec::with_capacity(tables);
        for id in 1..=tables as u64 {
            std::fs::write(
                dir.join("cf-bulk").join(format!("{id}.klog")),
                b"placeholder",
            )
            .unwrap();
            ssts.push(SstMeta {
                id,
                level: 6,
                num_entries: 1,
                max_seq: id,
                klog_size: 11,
                vlog_size: 0,
                min_key: format!("b{id:07}").into_bytes(),
                max_key: format!("b{id:07}").into_bytes(),
                ..SstMeta::default()
            });
        }
        let m = Manifest {
            next_file_id: tables as u64 + 1,
            global_seq: tables as u64,
            cfs: vec![
                CfManifest {
                    name: "bulk".into(),
                    config: ballast_cfg().encode(),
                    sstables: ssts,
                },
                CfManifest {
                    name: "hot".into(),
                    config: hot_cfg().encode(),
                    sstables: Vec::new(),
                },
            ],
            ..Manifest::default()
        };
        m.save(manifest_path(dir)).unwrap();
    }

    let probe_dir = tempfile::tempdir().unwrap();
    let sizing = probe_dir.path().join("sizing");
    build_fixture(&sizing, tables);
    let snapshot_bytes = std::fs::metadata(manifest_path(sizing)).unwrap().len();
    println!("# fixture: {tables} tables, MANIFEST={snapshot_bytes} bytes (synthetic ballast)");
    println!("# {flushes} flushes and {compactions} compactions per arm, {runs} runs");

    for run in 1..=runs {
        for edits in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            build_fixture(dir.path(), tables);
            let db = DB::open(Options::new(dir.path().path_str())).unwrap();
            if edits {
                db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
                    .unwrap();
            }
            let hot = db.get_column_family("hot").unwrap();

            let mut flush_ms: Vec<f64> = Vec::with_capacity(flushes);
            for i in 0..flushes {
                db.put(&hot, format!("h{i:07}").as_bytes(), b"v", Duration::ZERO)
                    .unwrap();
                let t = Instant::now();
                db.flush_memtable(&hot).unwrap();
                flush_ms.push(t.elapsed().as_secs_f64() * 1e3);
            }

            let mut compact_ms: Vec<f64> = Vec::with_capacity(compactions);
            for c in 0..compactions {
                for j in 0..4 {
                    db.put(
                        &hot,
                        format!("c{c:04}{j:04}").as_bytes(),
                        b"v",
                        Duration::ZERO,
                    )
                    .unwrap();
                    db.flush_memtable(&hot).unwrap();
                }
                let t = Instant::now();
                db.compact(&hot).unwrap();
                compact_ms.push(t.elapsed().as_secs_f64() * 1e3);
            }
            // Before `close`, which compacts the log away.
            let log_bytes = std::fs::metadata(edit_log_path(dir.path()))
                .map(|m| m.len())
                .unwrap_or(0);
            db.close().unwrap();

            flush_ms.sort_by(f64::total_cmp);
            compact_ms.sort_by(f64::total_cmp);
            let mode = if edits { "edits" } else { "full" };
            let (fp50, fp99) = (pct(&flush_ms, 0.50), pct(&flush_ms, 0.99));
            let (cp50, cp99) = (pct(&compact_ms, 0.50), pct(&compact_ms, 0.99));
            println!(
                "run={run} mode={mode:<5} flush p50={fp50:>8.2}ms p99={fp99:>8.2}ms  \
                 compact p50={cp50:>8.2}ms p99={cp99:>8.2}ms  log={log_bytes}B"
            );
        }
    }
}
