//! A2 — attach-by-reference over a shared tier (`SPADINO-A2.md`).
//!
//! The topology under test is spadino's: one database seals immutable parts
//! and publishes them onto a shared tier; other databases mount them by
//! reference, copying nothing. The shared root here is a local directory —
//! the `Storage` seam makes the S3 case the same code path (`tests/s3_tier.rs`
//! covers the backend; the env-gated variant at the bottom covers the
//! combination).

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, PartitionRule, TierDef, DB};

fn shared_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        partition_rules: vec![PartitionRule {
            prefix: b"img/".to_vec(),
            name: "img".into(),
        }],
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    }
}

fn open_with_shared(dir: &str, shared_root: &str) -> DB {
    let mut opts = Options::new(dir);
    opts.tiers = vec![TierDef::new("cas", shared_root).shared()];
    DB::open(opts).unwrap()
}

fn fill_and_publish(db: &DB, cf: &Arc<ColumnFamily>, val: &[u8]) {
    for i in 0..8u32 {
        db.put(cf, format!("img/{i:03}").as_bytes(), val, Duration::ZERO)
            .unwrap();
    }
    // One value past the WiscKey threshold, so the part carries a vlog and
    // the object-name pairing (same stem, .klog/.vlog) is exercised.
    let big = vec![0x42u8; 4096];
    db.put(cf, b"img/big", &big, Duration::ZERO).unwrap();
    db.flush_memtable(cf).unwrap();
    db.compact(cf).unwrap();
    db.move_part_to_tier(cf, "img", "cas").unwrap();
}

/// Count `.klog`/`.vlog` files under a directory tree.
fn sst_files_under(dir: &std::path::Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "klog" || x == "vlog") {
                found.push(p.to_string_lossy().into_owned());
            }
        }
    }
    found.sort();
    found
}

#[test]
fn attach_by_ref_mounts_without_copying() {
    let shared = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let shared_root = shared.path().to_str().unwrap();

    // Publisher: seal a part and move it onto the shared tier.
    let db1 = open_with_shared(d1.path().to_str().unwrap(), shared_root);
    let cf1 = db1.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db1, &cf1, b"IMG");
    let part = db1.export_part(&cf1, "img").unwrap();
    assert!(
        part.tables.iter().all(|t| t.object.is_some()),
        "a part published onto a shared tier must carry object names"
    );
    let shared_before = sst_files_under(shared.path());
    assert!(
        !shared_before.is_empty(),
        "objects must live under the shared root"
    );

    // Sharer: fresh database, same shared tier — mounts by reference.
    let db2 = open_with_shared(d2.path().to_str().unwrap(), shared_root);
    let cf2 = db2.create_column_family("default", shared_cfg()).unwrap();
    db2.attach_part_by_ref(&cf2, &part, "cas").unwrap();

    for i in 0..8u32 {
        assert_eq!(
            db2.get(&cf2, format!("img/{i:03}").as_bytes()).unwrap(),
            b"IMG",
            "mounted part must read identically"
        );
    }
    assert_eq!(
        db2.get(&cf2, b"img/big").unwrap(),
        vec![0x42u8; 4096],
        "vlog-resident value must read through the object-named pair"
    );
    // Zero copies: the sharer's own directory holds no sstable files, and the
    // shared root holds exactly what the publisher put there.
    assert!(
        sst_files_under(d2.path()).is_empty(),
        "attach-by-ref must not copy bytes into the sharer's directory"
    );
    assert_eq!(sst_files_under(shared.path()), shared_before);
}

#[test]
fn two_databases_sharing_a_root_cannot_collide() {
    let shared = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let root = shared.path().to_str().unwrap();

    // Both databases publish a part; their local file ids overlap (both are
    // young databases with small id counters), which is precisely the legacy
    // collision this test pins.
    let db1 = open_with_shared(d1.path().to_str().unwrap(), root);
    let cf1 = db1.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db1, &cf1, b"ONE");

    let db2 = open_with_shared(d2.path().to_str().unwrap(), root);
    let cf2 = db2.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db2, &cf2, b"TWO");

    // Distinct nonces ⇒ distinct object names ⇒ nobody overwrote anybody.
    assert_eq!(db1.get(&cf1, b"img/000").unwrap(), b"ONE");
    assert_eq!(db2.get(&cf2, b"img/000").unwrap(), b"TWO");
    let names = sst_files_under(shared.path());
    let klogs = names.iter().filter(|n| n.ends_with(".klog")).count();
    assert!(
        klogs >= 2,
        "each publisher's part must survive under its own object name, got {names:?}"
    );
}

#[test]
fn foreign_lineage_is_adopted_and_local_writes_continue() {
    let shared = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let root = shared.path().to_str().unwrap();

    let db1 = open_with_shared(d1.path().to_str().unwrap(), root);
    let cf1 = db1.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db1, &cf1, b"IMG");
    let part = db1.export_part(&cf1, "img").unwrap();
    let part_max_seq = part.tables.iter().map(|t| t.max_seq).max().unwrap();
    assert!(part_max_seq > 0);

    // The sharer is EMPTY: its visible sequence is far below the part's.
    let db2 = open_with_shared(d2.path().to_str().unwrap(), root);
    let cf2 = db2.create_column_family("default", shared_cfg()).unwrap();
    db2.attach_part_by_ref(&cf2, &part, "cas").unwrap();
    assert_eq!(db2.get(&cf2, b"img/007").unwrap(), b"IMG");

    // Its own writes keep working above the adopted floor, and both are
    // visible together.
    db2.put(&cf2, b"own/1", b"MINE", Duration::ZERO).unwrap();
    assert_eq!(db2.get(&cf2, b"own/1").unwrap(), b"MINE");
    assert_eq!(db2.get(&cf2, b"img/000").unwrap(), b"IMG");

    // And the mount survives reopen (manifest round-trip with objects).
    drop(db2);
    let db2 = open_with_shared(d2.path().to_str().unwrap(), root);
    let cf2 = db2.get_column_family("default").unwrap();
    assert_eq!(db2.get(&cf2, b"img/003").unwrap(), b"IMG");
    assert_eq!(db2.get(&cf2, b"own/1").unwrap(), b"MINE");
}

#[test]
fn refusals_are_typed_not_partial() {
    let shared = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let root = shared.path().to_str().unwrap();

    let db1 = open_with_shared(d1.path().to_str().unwrap(), root);
    let cf1 = db1.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db1, &cf1, b"IMG");
    let part = db1.export_part(&cf1, "img").unwrap();

    // A database whose tier is NOT declared shared refuses the mount.
    let mut opts = Options::new(d2.path().to_str().unwrap());
    opts.tiers = vec![TierDef::new("cas", root)]; // same root, not shared()
    let db2 = DB::open(opts).unwrap();
    let cf2 = db2.create_column_family("default", shared_cfg()).unwrap();
    match db2.attach_part_by_ref(&cf2, &part, "cas") {
        Err(OndaError::InvalidArgs(msg)) => assert!(msg.contains("shared")),
        other => panic!("expected a typed refusal, got {other:?}"),
    }

    // A part exported from a NON-shared publication carries no object names
    // and is refused before anything is staged.
    let d3 = tempfile::tempdir().unwrap();
    let unshared_tier = tempfile::tempdir().unwrap();
    let mut opts3 = Options::new(d3.path().to_str().unwrap());
    opts3.tiers = vec![TierDef::new("cold", unshared_tier.path().to_str().unwrap())];
    let db3 = DB::open(opts3).unwrap();
    let cf3 = db3.create_column_family("default", shared_cfg()).unwrap();
    for i in 0..4u32 {
        db3.put(&cf3, format!("img/{i:03}").as_bytes(), b"X", Duration::ZERO)
            .unwrap();
    }
    db3.flush_memtable(&cf3).unwrap();
    db3.compact(&cf3).unwrap();
    db3.move_part_to_tier(&cf3, "img", "cold").unwrap();
    let objectless = db3.export_part(&cf3, "img").unwrap();
    assert!(objectless.tables.iter().all(|t| t.object.is_none()));
    let db4 = open_with_shared(tempfile::tempdir().unwrap().path().to_str().unwrap(), root);
    let cf4 = db4.create_column_family("default", shared_cfg()).unwrap();
    match db4.attach_part_by_ref(&cf4, &objectless, "cas") {
        Err(OndaError::InvalidArgs(msg)) => assert!(msg.contains("object")),
        other => panic!("expected a typed refusal, got {other:?}"),
    }
}

#[test]
fn shared_publications_are_immovable_undetachable_unfreezable() {
    let shared = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let root = shared.path().to_str().unwrap();

    let mut opts = Options::new(d1.path().to_str().unwrap());
    opts.tiers = vec![
        TierDef::new("cas", root).shared(),
        TierDef::new("hdd", second.path().to_str().unwrap()),
    ];
    let db = DB::open(opts).unwrap();
    let cf = db.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db, &cf, b"IMG");
    let published = sst_files_under(shared.path());

    // Detach and freeze refuse (a publication other databases may reference
    // must never be unlinked or hard-linked away).
    assert!(matches!(
        db.detach_part(&cf, "img"),
        Err(OndaError::InvalidArgs(_))
    ));
    assert!(matches!(
        db.freeze_part(&cf, "img", tempfile::tempdir().unwrap().path()),
        Err(OndaError::InvalidArgs(_))
    ));
    // A move OFF the shared tier is a silent no-op (the mover's shared filter
    // excludes the tables), and the shared objects are untouched.
    db.move_part_to_tier(&cf, "img", "hdd").unwrap();
    assert_eq!(sst_files_under(shared.path()), published);
    assert!(sst_files_under(second.path()).is_empty());
    // Reads still come from the shared tier.
    assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
}

/// The publisher's own restart must keep resolving its shared objects (its
/// manifest carries the object names; the startup sweep must not touch the
/// shared root even though the ids look "wrong-tier" to a naive walk).
#[test]
fn publisher_reopen_keeps_shared_objects_and_sweep_leaves_them() {
    let shared = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let root = shared.path().to_str().unwrap();

    let db = open_with_shared(d1.path().to_str().unwrap(), root);
    let cf = db.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db, &cf, b"IMG");
    let published = sst_files_under(shared.path());
    drop(db);

    let db = open_with_shared(d1.path().to_str().unwrap(), root);
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
    assert_eq!(
        sst_files_under(shared.path()),
        published,
        "the startup sweep must never delete on a shared tier"
    );
}

/// S3 variant of the mount: gated exactly like `tests/s3_tier.rs` — compiles
/// under `--features s3`, no-ops without `ONDADB_S3_ENDPOINT`.
#[cfg(feature = "s3")]
#[test]
fn attach_by_ref_mounts_from_s3() {
    let Some(cfg) = ({
        std::env::var("ONDADB_S3_ENDPOINT")
            .ok()
            .map(|endpoint| ondadb::S3Config {
                bucket: std::env::var("ONDADB_S3_BUCKET").unwrap_or_else(|_| "ayu".into()),
                region: std::env::var("ONDADB_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
                endpoint,
                access_key: std::env::var("ONDADB_S3_KEY").unwrap_or_else(|_| "ayu".into()),
                secret_key: std::env::var("ONDADB_S3_SECRET")
                    .unwrap_or_else(|_| "ayudevsecret".into()),
                path_style: true,
            })
    }) else {
        eprintln!("skipping: ONDADB_S3_ENDPOINT not set");
        return;
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("ondadb-a2-test/{nanos}");

    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let open = |dir: &str| {
        let mut opts = Options::new(dir);
        opts.tiers = vec![ondadb::TierDef::s3("cas", &prefix, cfg.clone()).shared()];
        DB::open(opts).unwrap()
    };

    let db1 = open(d1.path().to_str().unwrap());
    let cf1 = db1.create_column_family("default", shared_cfg()).unwrap();
    fill_and_publish(&db1, &cf1, b"IMG");
    let part = db1.export_part(&cf1, "img").unwrap();
    assert!(part.tables.iter().all(|t| t.object.is_some()));

    let db2 = open(d2.path().to_str().unwrap());
    let cf2 = db2.create_column_family("default", shared_cfg()).unwrap();
    db2.attach_part_by_ref(&cf2, &part, "cas").unwrap();
    assert_eq!(db2.get(&cf2, b"img/000").unwrap(), b"IMG");
    assert_eq!(db2.get(&cf2, b"img/big").unwrap(), vec![0x42u8; 4096]);
    assert!(
        sst_files_under(d2.path()).is_empty(),
        "S3 mount must copy nothing local"
    );
}
