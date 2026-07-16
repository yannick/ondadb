//! Part lifecycle (P2) and storage-tier substrate (P3): detach / attach /
//! freeze, and cross-tier moves.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, PartitionRule, TierDef, DB};

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
    assert_eq!(img_seen, 5, "pre-detach iterator must still see all img keys");
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
        assert!(db.get(&cf, b"img/000").is_err(), "detach must survive reopen");
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
