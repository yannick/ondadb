//! Part lifecycle (P2) and storage-tier substrate (P3): detach / attach /
//! freeze, and cross-tier moves.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ondadb::{
    ColumnFamily, ColumnFamilyConfig, MovePhase, MovePhaseEvent, MovePhaseObserver, OndaError,
    Options, PartitionRule, TierDef, TierRule, DB,
};

/// A CF configured so that `img/` and `log/` keys form their own bottom-level
/// parts (compaction cuts bottom files at partition boundaries).
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
        // Compact eagerly so a single flush lands in the bottom level.
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    }
}

/// Fill a few small (inline-value) keys per partition, then flush + compact so
/// every partition has a materialized, single-block bottom-level part.
fn materialize_parts(db: &DB, cf: &Arc<ColumnFamily>) {
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

#[test]
fn detach_hides_then_attach_restores_and_preexisting_iterator_unaffected() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", parts_cfg()).unwrap();
    materialize_parts(&db, &cf);

    // Sanity: all partitions readable up front.
    assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
    assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");

    // Open a snapshot iterator BEFORE the detach; it pins the part's SSTable
    // handles (and their loaded blocks), so it must keep seeing the img data
    // after the files are moved aside.
    let snap = db.begin();
    let mut it = snap.new_iterator(&cf);
    it.seek_to_first();

    let detached = db.detach_part(&cf, "img").unwrap();
    assert_eq!(detached.partition, "img");
    assert!(!detached.table_ids.is_empty());

    // New reads no longer see the img partition; other partitions untouched.
    assert!(db.get(&cf, b"img/000").is_err(), "img must be hidden");
    assert!(db.get(&cf, b"img/004").is_err());
    assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");
    assert_eq!(db.get(&cf, b"etc/000").unwrap(), b"ETC");

    // A fresh iterator (post-detach) also does not see img.
    {
        let s2 = db.begin();
        let mut it2 = s2.new_iterator(&cf);
        it2.seek_to_first();
        let mut saw_img = false;
        while it2.valid() {
            if it2.key().starts_with(b"img/") {
                saw_img = true;
            }
            it2.next();
        }
        assert!(!saw_img, "post-detach iterator must not see img");
    }

    // The pre-detach iterator still yields every img key.
    let mut img_seen = 0;
    while it.valid() {
        if it.key().starts_with(b"img/") {
            img_seen += 1;
        }
        it.next();
    }
    assert_eq!(
        img_seen, 5,
        "pre-detach iterator must still see all img keys"
    );
    drop(snap);

    // Attach the detached part back; the range is now free of live bottom
    // tables, so it slots into the bottom level and becomes visible again.
    db.attach_part(&cf, &detached.dir).unwrap();
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG",
            "img/{i} must be visible after attach"
        );
    }
    db.close().unwrap();
}

#[test]
fn detach_is_durable_across_reopen() {
    // The detach's manifest record is atomic and durable: after reopening, the
    // partition is still gone (and the other partitions remain).
    let dir = tempfile::tempdir().unwrap();
    let detached_dir;
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("default", parts_cfg()).unwrap();
        materialize_parts(&db, &cf);
        let d = db.detach_part(&cf, "img").unwrap();
        detached_dir = d.dir.clone();
        db.close().unwrap();
    }
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.get_column_family("default").unwrap();
        assert!(
            db.get(&cf, b"img/000").is_err(),
            "detach must survive reopen"
        );
        assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");

        // And re-attaching after reopen restores it durably too.
        db.attach_part(&cf, &detached_dir).unwrap();
        assert_eq!(db.get(&cf, b"img/002").unwrap(), b"IMG");
        db.close().unwrap();
    }
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.get_column_family("default").unwrap();
        assert_eq!(db.get(&cf, b"img/002").unwrap(), b"IMG");
        db.close().unwrap();
    }
}

#[test]
fn attach_overlapping_range_goes_to_l0() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", parts_cfg()).unwrap();
    materialize_parts(&db, &cf);

    let detached = db.detach_part(&cf, "img").unwrap();
    // First attach: no live bottom table for the range -> bottom level.
    db.attach_part(&cf, &detached.dir).unwrap();
    let l0_before = cf.stats().levels[0].0;

    // Second attach of the SAME files: now overlaps the just-attached bottom
    // table, so it must fall back to L0.
    db.attach_part(&cf, &detached.dir).unwrap();
    let l0_after = cf.stats().levels[0].0;
    assert!(
        l0_after > l0_before,
        "overlapping attach must land in L0 (before={l0_before}, after={l0_after})"
    );
    // Data still reads correctly with the duplicate part present.
    assert_eq!(db.get(&cf, b"img/003").unwrap(), b"IMG");
    db.close().unwrap();
}

#[test]
fn freeze_part_is_independently_openable() {
    let dir = tempfile::tempdir().unwrap();
    let frozen = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("default", parts_cfg()).unwrap();
        materialize_parts(&db, &cf);
        db.freeze_part(&cf, "img", frozen.path()).unwrap();
        // Freeze does not remove the live part.
        assert_eq!(db.get(&cf, b"img/001").unwrap(), b"IMG");
        db.close().unwrap();
    }
    // The frozen directory opens as a standalone database holding only img.
    let db = DB::open(Options::new(frozen.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in frozen slice");
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG"
        );
    }
    // Only the img part was frozen: log/etc are absent.
    assert!(db.get(&cf, b"log/000").is_err());
    assert!(db.get(&cf, b"etc/000").is_err());
    db.close().unwrap();
}

#[test]
fn attach_rejects_foreign_file_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let junk = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", parts_cfg()).unwrap();
    materialize_parts(&db, &cf);

    // A file that is not a valid ondaDB SSTable (bad footer / CRC).
    std::fs::write(junk.path().join("999.klog"), b"this is not an sstable").unwrap();
    let err = db.attach_part(&cf, junk.path());
    assert!(err.is_err(), "foreign file must be rejected");

    // The rejection left the CF unchanged and usable.
    assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
    // No stray copied file lingers in the CF directory beyond the real parts.
    db.close().unwrap();
}

#[test]
fn move_part_to_tier_reads_correctly_with_mmap_off() {
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let hdd_root = hdd.path().to_str().unwrap().to_string();

    let mut opts = Options::new(dir.path().to_str().unwrap());
    // A second local tier that forces the buffered (no-mmap) read path.
    opts.tiers = vec![TierDef::new("hdd", hdd_root.clone()).without_mmap()];

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", parts_cfg()).unwrap();
        materialize_parts(&db, &cf);

        db.move_part_to_tier(&cf, "img", "hdd").unwrap();

        // The part's files now live under the hdd tier root, not the DB dir.
        let tier_cf_dir = std::path::Path::new(&hdd_root).join("cf-default");
        let moved: Vec<_> = std::fs::read_dir(&tier_cf_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "klog"))
            .collect();
        assert!(!moved.is_empty(), "img klog must exist on the hdd tier");

        // Reads (through the no-mmap tier) still return the data; other
        // partitions (default tier) are unaffected.
        for i in 0..5u32 {
            assert_eq!(
                db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
                b"IMG"
            );
        }
        assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");
        db.close().unwrap();
    }

    // The tier assignment persists: on reopen the part is read back from hdd.
    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("default").unwrap();
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG",
            "img/{i} must read back from the hdd tier after reopen"
        );
    }
    assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");
    db.close().unwrap();
}

#[derive(Debug)]
struct RecordingMoveObserver {
    phases: Mutex<Vec<MovePhase>>,
    fail_at: Option<MovePhase>,
}

impl RecordingMoveObserver {
    fn new(fail_at: Option<MovePhase>) -> Self {
        Self {
            phases: Mutex::new(Vec::new()),
            fail_at,
        }
    }
}

impl MovePhaseObserver for RecordingMoveObserver {
    fn observe(&self, event: &MovePhaseEvent<'_>) -> ondadb::Result<()> {
        self.phases.lock().unwrap().push(event.phase.clone());
        if self
            .fail_at
            .as_ref()
            .is_some_and(|phase| phase.same_kind(&event.phase))
        {
            return Err(OndaError::InvalidArgs(format!(
                "injected move failure at {:?}",
                event.phase
            )));
        }
        Ok(())
    }
}

#[test]
fn observed_move_phases_cover_every_durability_boundary_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.tiers = vec![TierDef::new("hdd", hdd.path().to_str().unwrap())];
    let db = DB::open(opts).unwrap();
    let cf = db.create_column_family("default", parts_cfg()).unwrap();
    materialize_parts(&db, &cf);
    let observer = RecordingMoveObserver::new(None);

    db.move_part_to_tier_observed(&cf, "img", "hdd", &observer)
        .unwrap();

    let phases = observer.phases.lock().unwrap().clone();
    assert!(matches!(
        phases.first(),
        Some(MovePhase::CopyComplete { .. })
    ));
    assert_eq!(
        phases[phases.len() - 3..],
        [
            MovePhase::DestinationSynced,
            MovePhase::ManifestFlipped,
            MovePhase::SourceDeleteFinished { remaining_files: 0 },
        ]
    );
    db.close().unwrap();
}

#[test]
fn observed_move_pre_commit_failures_reopen_source_placement() {
    for phase in [
        MovePhase::CopyComplete {
            object_index: 1,
            object_count: 1,
        },
        MovePhase::DestinationSynced,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let hdd = tempfile::tempdir().unwrap();
        let hdd_root = hdd.path().to_str().unwrap().to_string();
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.tiers = vec![TierDef::new("hdd", hdd_root.clone())];
        {
            let db = DB::open(opts.clone()).unwrap();
            let cf = db.create_column_family("default", parts_cfg()).unwrap();
            materialize_parts(&db, &cf);
            let observer = RecordingMoveObserver::new(Some(phase.clone()));
            let error = db
                .move_part_to_tier_observed(&cf, "img", "hdd", &observer)
                .unwrap_err();
            assert!(error.to_string().contains("injected move failure"));
            assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
            db.close().unwrap();
        }

        let db = DB::open(opts).unwrap();
        let cf = db.get_column_family("default").unwrap();
        assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
        assert!(
            klog_count(&hdd_root) == 0,
            "wrong durable placement after {phase:?}"
        );
        db.close().unwrap();
    }
}

#[test]
fn observed_move_lost_response_after_manifest_flip_is_safe_to_retry() {
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.tiers = vec![TierDef::new("hdd", hdd.path().to_str().unwrap())];

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", parts_cfg()).unwrap();
        materialize_parts(&db, &cf);
        let observer = RecordingMoveObserver::new(Some(MovePhase::ManifestFlipped));

        // Model a caller that loses the first response after the durable commit
        // and therefore cannot know whether the move took effect.
        let _lost_response = db.move_part_to_tier_observed(&cf, "img", "hdd", &observer);
        db.move_part_to_tier(&cf, "img", "hdd").unwrap();

        assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
        db.close().unwrap();
    }

    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
    db.close().unwrap();
}

#[test]
fn moving_a_mixed_tier_part_reuses_already_placed_tables() {
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.tiers = vec![TierDef::new("hdd", hdd.path().to_str().unwrap())];

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db
            .create_column_family(
                "default",
                ColumnFamilyConfig {
                    partition_rules: vec![PartitionRule {
                        prefix: b"img/".to_vec(),
                        name: "img".into(),
                    }],
                    l1_file_count_trigger: 1,
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();

        db.put(&cf, b"img/a", b"A", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        let detached = db.detach_part(&cf, "img").unwrap();

        db.put(&cf, b"img/z", b"Z", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        db.move_part_to_tier(&cf, "img", "hdd").unwrap();

        // The detached, disjoint range slots directly into the bottom level on
        // the default tier beside the img table that already lives on hdd.
        db.attach_part(&cf, &detached.dir).unwrap();
        assert_eq!(db.get(&cf, b"img/a").unwrap(), b"A");
        assert_eq!(db.get(&cf, b"img/z").unwrap(), b"Z");

        // Healing the mixed placement must copy only img/a's table. Recopying
        // img/z's already-placed table aliases source and destination paths and
        // truncates the live klog.
        db.move_part_to_tier(&cf, "img", "hdd").unwrap();
        assert_eq!(db.get(&cf, b"img/a").unwrap(), b"A");
        assert_eq!(db.get(&cf, b"img/z").unwrap(), b"Z");
        db.close().unwrap();
    }

    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"img/a").unwrap(), b"A");
    assert_eq!(db.get(&cf, b"img/z").unwrap(), b"Z");
    db.close().unwrap();
}

#[test]
fn observed_move_post_commit_observer_errors_do_not_hide_success() {
    for fail_at in [
        MovePhase::ManifestFlipped,
        MovePhase::SourceDeleteFinished { remaining_files: 0 },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let hdd = tempfile::tempdir().unwrap();
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.tiers = vec![TierDef::new("hdd", hdd.path().to_str().unwrap())];
        let db = DB::open(opts).unwrap();
        let cf = db.create_column_family("default", parts_cfg()).unwrap();
        materialize_parts(&db, &cf);
        let observer = RecordingMoveObserver::new(Some(fail_at));

        db.move_part_to_tier_observed(&cf, "img", "hdd", &observer)
            .unwrap();

        assert!(matches!(
            observer.phases.lock().unwrap().last(),
            Some(MovePhase::SourceDeleteFinished { .. })
        ));
        assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
        db.close().unwrap();
    }
}

// ---- A5: live partition-rule addition -------------------------------------

/// A rule added to a live CF takes effect only on the *next* bottom compaction
/// (write-side-only): existing bottom files keep their stamps until a later
/// compaction re-cuts them, and the new boundary then materializes a fresh part.
#[test]
fn add_partition_rule_live_cuts_only_future_bottom_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    // Start with only the img/ partition; log/ and etc/ share the default part.
    let cfg = ColumnFamilyConfig {
        partition_rules: vec![PartitionRule {
            prefix: b"img/".to_vec(),
            name: "img".into(),
        }],
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    };
    let cf = db.create_column_family("default", cfg).unwrap();
    materialize_parts(&db, &cf);

    // freeze_part is a non-destructive probe: it errors (NotFound) unless a
    // bottom part is stamped with the partition. img/ is a part; log/ is not.
    db.freeze_part(&cf, "img", tempfile::tempdir().unwrap().path())
        .expect("img part exists after initial materialize");
    assert!(
        db.freeze_part(&cf, "log", tempfile::tempdir().unwrap().path())
            .is_err(),
        "log/ is not a partition yet (lives in the default part)"
    );

    // Add the log/ rule on the LIVE cf.
    db.add_partition_rule(
        &cf,
        PartitionRule {
            prefix: b"log/".to_vec(),
            name: "log".into(),
        },
    )
    .unwrap();

    // Write-side-only: no data is rewritten, so the pre-existing bottom files
    // still hold log/ in the default part — no "log" part materialized yet.
    assert!(
        db.freeze_part(&cf, "log", tempfile::tempdir().unwrap().path())
            .is_err(),
        "existing bottom files must keep their stamps until recompacted"
    );

    // The next compaction re-cuts the bottom on the new boundary.
    db.compact(&cf).unwrap();
    db.freeze_part(&cf, "log", tempfile::tempdir().unwrap().path())
        .expect("log part is materialized by the post-add compaction");
    db.freeze_part(&cf, "img", tempfile::tempdir().unwrap().path())
        .expect("img part is untouched by the re-cut");

    // All data still reads correctly across the re-partitioning.
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG"
        );
        assert_eq!(
            db.get(&cf, format!("log/{i:03}").as_bytes()).unwrap(),
            b"LOG"
        );
        assert_eq!(
            db.get(&cf, format!("etc/{i:03}").as_bytes()).unwrap(),
            b"ETC"
        );
    }
    db.close().unwrap();
}

/// An exact-duplicate prefix is rejected with a clear `invalid_args` error;
/// distinct (even nested) prefixes are accepted.
#[test]
fn add_partition_rule_rejects_exact_duplicate_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cfg = ColumnFamilyConfig {
        partition_rules: vec![PartitionRule {
            prefix: b"img/".to_vec(),
            name: "img".into(),
        }],
        ..ColumnFamilyConfig::default()
    };
    let cf = db.create_column_family("default", cfg).unwrap();

    // Same prefix as an existing rule (even under a different name) is rejected.
    let err = db
        .add_partition_rule(
            &cf,
            PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img2".into(),
            },
        )
        .unwrap_err();
    assert_eq!(err.kind(), "invalid_args");
    assert!(
        format!("{err}").contains("duplicate"),
        "duplicate error must name the problem, got: {err}"
    );

    // A distinct nested prefix is accepted (longest-prefix-wins resolves it)...
    db.add_partition_rule(
        &cf,
        PartitionRule {
            prefix: b"img/thumb/".to_vec(),
            name: "thumb".into(),
        },
    )
    .unwrap();
    // ...and re-adding that now-present prefix is itself a duplicate.
    assert!(db
        .add_partition_rule(
            &cf,
            PartitionRule {
                prefix: b"img/thumb/".to_vec(),
                name: "x".into(),
            },
        )
        .is_err());
    db.close().unwrap();
}

/// A live-added rule is persisted through the manifest rewrite and is still
/// active after reopen (it rejects a duplicate and drives boundary cutting).
#[test]
fn added_partition_rule_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cfg = ColumnFamilyConfig {
            partition_rules: vec![PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            }],
            l1_file_count_trigger: 1,
            ..ColumnFamilyConfig::default()
        };
        let cf = db.create_column_family("default", cfg).unwrap();
        db.add_partition_rule(
            &cf,
            PartitionRule {
                prefix: b"log/".to_vec(),
                name: "log".into(),
            },
        )
        .unwrap();
        db.close().unwrap();
    }

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    // The persisted rule is active: re-adding it is a duplicate error.
    assert!(
        db.add_partition_rule(
            &cf,
            PartitionRule {
                prefix: b"log/".to_vec(),
                name: "log".into(),
            },
        )
        .is_err(),
        "log/ rule must survive reopen"
    );
    // And it drives boundary cutting on freshly written data.
    materialize_parts(&db, &cf);
    db.freeze_part(&cf, "log", tempfile::tempdir().unwrap().path())
        .expect("reopened log/ rule cuts a log part");
    db.freeze_part(&cf, "img", tempfile::tempdir().unwrap().path())
        .expect("img part cut as well");
    db.close().unwrap();
}

/// Concurrent adds are race-free: validation and the append happen under one
/// lock, so a duplicate-prefix stampede yields exactly one winner, and distinct
/// prefixes all land and persist.
#[test]
fn concurrent_add_partition_rule_is_race_free() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();

    let db_ref = &db;
    let cf_ref = &cf;

    // (a) Many threads racing to add the SAME prefix: exactly one may succeed.
    let ok_count = std::sync::atomic::AtomicUsize::new(0);
    let ok_ref = &ok_count;
    std::thread::scope(|s| {
        for _ in 0..16 {
            s.spawn(move || {
                let r = db_ref.add_partition_rule(
                    cf_ref,
                    PartitionRule {
                        prefix: b"race/".to_vec(),
                        name: "race".into(),
                    },
                );
                if r.is_ok() {
                    ok_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(
        ok_count.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "exactly one concurrent add of a duplicate prefix may win"
    );

    // (b) Many threads each adding a DISTINCT prefix: all succeed, none lost.
    std::thread::scope(|s| {
        for i in 0..16u32 {
            s.spawn(move || {
                db_ref
                    .add_partition_rule(
                        cf_ref,
                        PartitionRule {
                            prefix: format!("p{i:02}/").into_bytes(),
                            name: format!("p{i}"),
                        },
                    )
                    .unwrap();
            });
        }
    });
    // Every distinct prefix is present now (re-adding each is a duplicate).
    for i in 0..16u32 {
        assert!(
            db.add_partition_rule(
                &cf,
                PartitionRule {
                    prefix: format!("p{i:02}/").into_bytes(),
                    name: "dup".into(),
                },
            )
            .is_err(),
            "p{i:02}/ must be present after the concurrent adds"
        );
    }

    // The final rule set persists across reopen (manifest reflects every add).
    db.close().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    for i in 0..16u32 {
        assert!(
            db.add_partition_rule(
                &cf,
                PartitionRule {
                    prefix: format!("p{i:02}/").into_bytes(),
                    name: "dup".into(),
                },
            )
            .is_err(),
            "p{i:02}/ must persist across reopen"
        );
    }
    assert!(db
        .add_partition_rule(
            &cf,
            PartitionRule {
                prefix: b"race/".to_vec(),
                name: "dup".into(),
            },
        )
        .is_err());
    db.close().unwrap();
}

#[test]
fn detach_unknown_partition_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", parts_cfg()).unwrap();
    materialize_parts(&db, &cf);
    assert!(
        db.detach_part(&cf, "nope").is_err(),
        "detaching a partition with no bottom tables must error"
    );
    db.close().unwrap();
}

// ---- P4: tier rules + background part mover --------------------------------

/// A `parts_cfg` extended with tier rules: `img/` moves to `hdd` as soon as the
/// part has any measurable age (`min_age = 0`), while `log/` targets `hdd` only
/// after an hour — so a freshly written `log/` part must stay put.
fn mover_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        tier_rules: vec![
            TierRule {
                prefix: b"img/".to_vec(),
                tier: "hdd".into(),
                min_age: Duration::ZERO,
            },
            TierRule {
                prefix: b"log/".to_vec(),
                tier: "hdd".into(),
                min_age: Duration::from_secs(3600),
            },
        ],
        ..parts_cfg()
    }
}

/// Count `<id>.klog` files directly under a tier's `cf-default` directory.
fn klog_count(tier_root: &str) -> usize {
    let dir = std::path::Path::new(tier_root).join("cf-default");
    match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "klog"))
            .count(),
        Err(_) => 0,
    }
}

/// Every `<id>.klog`/`<id>.vlog` file directly under a tier's `cf-default` dir.
fn part_files(tier_root: &str) -> Vec<std::path::PathBuf> {
    let dir = std::path::Path::new(tier_root).join("cf-default");
    match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "klog" || x == "vlog"))
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[test]
fn part_mover_moves_aged_part_but_not_young_one() {
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let hdd_root = hdd.path().to_str().unwrap().to_string();

    let mut opts = Options::new(dir.path().to_str().unwrap());
    // Second local tier, no-mmap, so reads exercise the buffered path.
    opts.tiers = vec![TierDef::new("hdd", hdd_root.clone()).without_mmap()];
    // Disable the scheduled cadence: this test drives the mover explicitly.
    opts.part_mover_interval = Duration::ZERO;

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", mover_cfg()).unwrap();
        materialize_parts(&db, &cf);

        // img/ (min_age 0) is eligible; log/ (min_age 1h) is far too young.
        let moved = db.run_part_mover().unwrap();
        assert_eq!(moved, 1, "only the img/ part should move");

        // The img part now lives on hdd; log/ and the default part stay on ssd.
        assert_eq!(klog_count(&hdd_root), 1, "img klog must be on hdd");

        // Reads are correct after the move: img through the no-mmap hdd tier,
        // everything else from the default tier.
        for i in 0..5u32 {
            assert_eq!(
                db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
                b"IMG"
            );
            assert_eq!(
                db.get(&cf, format!("log/{i:03}").as_bytes()).unwrap(),
                b"LOG"
            );
            assert_eq!(
                db.get(&cf, format!("etc/{i:03}").as_bytes()).unwrap(),
                b"ETC"
            );
        }

        // Idempotent: a second pass places nothing new (img already on hdd, log
        // still too young).
        assert_eq!(db.run_part_mover().unwrap(), 0, "re-run must be a no-op");
        db.close().unwrap();
    }

    // The tier assignment is durable: reopen reads img back from hdd.
    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(klog_count(&hdd_root), 1, "img part persists on hdd");
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG"
        );
        assert_eq!(
            db.get(&cf, format!("log/{i:03}").as_bytes()).unwrap(),
            b"LOG"
        );
    }
    db.close().unwrap();
}

#[test]
fn startup_sweeps_orphan_copy_left_on_target_by_crash_before_flip() {
    // Simulate a crash between the mover's copy and its manifest flip: files are
    // copied onto the target tier but the manifest still attributes them to the
    // source. On reopen the startup sweep must delete every such target orphan,
    // and the data must still read from the untouched source.
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let hdd_root = hdd.path().to_str().unwrap().to_string();
    let ssd_root = dir.path().to_str().unwrap().to_string();

    let mut opts = Options::new(&ssd_root);
    opts.tiers = vec![TierDef::new("hdd", hdd_root.clone())];
    opts.part_mover_interval = Duration::ZERO;

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", parts_cfg()).unwrap();
        materialize_parts(&db, &cf);
        db.close().unwrap();
    }

    // Mimic the mover's pre-flip copy: replicate the ssd part files onto hdd
    // without touching the manifest (the crash lands before the flip).
    let hdd_cf = std::path::Path::new(&hdd_root).join("cf-default");
    std::fs::create_dir_all(&hdd_cf).unwrap();
    for src in part_files(&ssd_root) {
        let dst = hdd_cf.join(src.file_name().unwrap());
        std::fs::copy(&src, &dst).unwrap();
    }
    assert!(klog_count(&hdd_root) > 0, "orphan copies planted on hdd");

    // Reopen: the manifest says every table is on ssd, so all hdd copies are
    // orphans and get swept.
    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(
        klog_count(&hdd_root),
        0,
        "startup GC must delete the target-tier orphans"
    );
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG"
        );
        assert_eq!(
            db.get(&cf, format!("log/{i:03}").as_bytes()).unwrap(),
            b"LOG"
        );
    }
    db.close().unwrap();
}

#[test]
fn startup_sweeps_orphan_source_left_by_crash_after_flip() {
    // Simulate a crash between the mover's manifest flip and its source delete:
    // the manifest already says the part is on hdd, but a stale copy lingers on
    // ssd. On reopen the sweep must delete the ssd orphan (its id is on hdd per
    // the manifest) while leaving legitimately-ssd files (log/, default) intact.
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let hdd_root = hdd.path().to_str().unwrap().to_string();
    let ssd_root = dir.path().to_str().unwrap().to_string();

    let mut opts = Options::new(&ssd_root);
    opts.tiers = vec![TierDef::new("hdd", hdd_root.clone())];
    opts.part_mover_interval = Duration::ZERO;

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", parts_cfg()).unwrap();
        materialize_parts(&db, &cf);
        // A real, complete move: manifest now says img is on hdd, ssd copies gone.
        db.move_part_to_tier(&cf, "img", "hdd").unwrap();

        // Recreate the ssd orphan the crash would have left: copy img's hdd files
        // (the img part's ids) back into the ssd cf-dir.
        let ssd_cf = std::path::Path::new(&ssd_root).join("cf-default");
        for src in part_files(&hdd_root) {
            let dst = ssd_cf.join(src.file_name().unwrap());
            std::fs::copy(&src, &dst).unwrap();
        }
        db.close().unwrap();
    }

    let ssd_before = klog_count(&ssd_root);
    // Reopen: the img orphan on ssd (id now attributed to hdd) is swept; the
    // remaining ssd parts (log/, default) survive.
    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert!(
        klog_count(&ssd_root) < ssd_before,
        "the ssd source orphan must be swept"
    );
    assert_eq!(klog_count(&hdd_root), 1, "the real img part stays on hdd");
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG"
        );
        assert_eq!(
            db.get(&cf, format!("log/{i:03}").as_bytes()).unwrap(),
            b"LOG"
        );
        assert_eq!(
            db.get(&cf, format!("etc/{i:03}").as_bytes()).unwrap(),
            b"ETC"
        );
    }
    db.close().unwrap();
}

#[test]
fn tier_rules_survive_reopen_and_drive_mover_after_restart() {
    // tier_rules are persisted with the CF config, so a mover pass after reopen
    // still relocates an eligible part with no re-configuration.
    let dir = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let hdd_root = hdd.path().to_str().unwrap().to_string();

    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.tiers = vec![TierDef::new("hdd", hdd_root.clone())];
    opts.part_mover_interval = Duration::ZERO;

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", mover_cfg()).unwrap();
        materialize_parts(&db, &cf);
        db.close().unwrap();
    }

    // No rules re-supplied here — they must come back from the manifest.
    let db = DB::open(opts).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(
        db.run_part_mover().unwrap(),
        1,
        "reopened CF's persisted tier_rules must still drive the mover"
    );
    assert_eq!(klog_count(&hdd_root), 1);
    for i in 0..5u32 {
        assert_eq!(
            db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG"
        );
    }
    db.close().unwrap();
}
