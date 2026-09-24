//! P7: S3 storage tier — end-to-end part mover onto an S3-compatible object
//! store (MinIO), read-back via range GETs + block cache, and persistence across
//! a DB reopen.
//!
//! These tests are gated twice: they only compile under `--features s3`, and at
//! runtime they no-op unless `ONDADB_S3_ENDPOINT` is set, so
//! `cargo test --features s3` stays green when no MinIO is reachable. Run them
//! against MinIO with:
//!
//! ```sh
//! ONDADB_S3_ENDPOINT=http://192.168.65.11:9000 \
//! ONDADB_S3_KEY=ayu ONDADB_S3_SECRET=ayudevsecret ONDADB_S3_BUCKET=ayu \
//!   cargo test --features s3 --test s3_tier -- --nocapture --test-threads=1
//! ```
#![cfg(feature = "s3")]

use std::sync::Arc;
use std::time::Duration;

use ondadb::{
    ColumnFamily, ColumnFamilyConfig, Options, PartitionRule, S3Config, S3Storage, Storage,
    TierDef, TierRule, DB,
};

/// An S3 tier config from the environment, or `None` to skip.
fn env_s3() -> Option<S3Config> {
    let endpoint = std::env::var("ONDADB_S3_ENDPOINT").ok()?;
    Some(S3Config {
        bucket: std::env::var("ONDADB_S3_BUCKET").unwrap_or_else(|_| "ayu".into()),
        region: std::env::var("ONDADB_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        endpoint,
        access_key: std::env::var("ONDADB_S3_KEY").unwrap_or_else(|_| "ayu".into()),
        secret_key: std::env::var("ONDADB_S3_SECRET").unwrap_or_else(|_| "ayudevsecret".into()),
        path_style: true,
        ..S3Config::default()
    })
}

fn unique_prefix() -> String {
    // The clock alone is not unique: macOS reports microseconds, so tests the
    // harness starts together can mint the same prefix and trample each
    // other's objects (same table ids, same keys). Add pid + a counter.
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("ondadb-tier-test/{nanos}-{}-{seq}", std::process::id())
}

/// CF with img/ and log/ partitions; img/ is tiered to `s3` as soon as the part
/// has any age.
fn s3_mover_cfg() -> ColumnFamilyConfig {
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
        tier_rules: vec![TierRule {
            prefix: b"img/".to_vec(),
            tier: "s3".into(),
            min_age: Duration::ZERO,
        }],
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    }
}

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

/// Best-effort removal of the objects a run created, so the dev bucket does not
/// accumulate. Uses the same backend the DB used.
fn cleanup(cfg: &S3Config, prefix: &str) {
    if let Ok(s3) = S3Storage::new(cfg) {
        let dir = format!("{prefix}/cf-default");
        if let Ok(names) = s3.list(&dir) {
            for n in names {
                let _ = s3.delete(&format!("{dir}/{n}"));
            }
        }
    }
}

#[test]
fn part_mover_moves_aged_part_to_s3_and_reads_back_across_reopen() {
    let Some(cfg) = env_s3() else {
        eprintln!("skipping s3 tier test: ONDADB_S3_ENDPOINT not set");
        return;
    };
    let prefix = unique_prefix();

    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    // The s3 tier's root is an in-bucket key prefix. mmap is forced off for S3.
    opts.tiers = vec![TierDef::s3("s3", prefix.clone(), cfg.clone())];
    // Drive the mover explicitly.
    opts.part_mover_interval = Duration::ZERO;

    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", s3_mover_cfg()).unwrap();
        materialize_parts(&db, &cf);

        // The img/ part is aged (min_age 0) and relocates to the s3 tier.
        let moved = db.run_part_mover().unwrap();
        assert_eq!(moved, 1, "exactly the img/ part should move to s3");

        // The moved part's klog now lives in the object store, not the DB dir.
        let s3 = S3Storage::new(&cfg).unwrap();
        let listed = s3.list(&format!("{prefix}/cf-default")).unwrap();
        assert!(
            listed.iter().any(|n| n.ends_with(".klog")),
            "an img klog must exist on the s3 tier: {listed:?}"
        );

        // Reads through the S3 tier (range GET + block cache) return the data;
        // other partitions (default tier) are unaffected.
        for i in 0..5u32 {
            assert_eq!(
                db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
                b"IMG",
                "img/{i} must read back from s3"
            );
        }
        assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");
        assert_eq!(db.get(&cf, b"etc/000").unwrap(), b"ETC");

        // A second mover pass is a no-op (idempotent).
        assert_eq!(db.run_part_mover().unwrap(), 0, "re-run must be a no-op");
        db.close().unwrap();
    }

    // Reopen: the same Options re-supplies the s3 TierDef, and the manifest still
    // places the img/ part on s3, so it reads back from the object store.
    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.get_column_family("default").unwrap();
        for i in 0..5u32 {
            assert_eq!(
                db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(),
                b"IMG",
                "img/{i} must read back from s3 after reopen"
            );
        }
        assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");
        db.close().unwrap();
    }

    cleanup(&cfg, &prefix);
}

/// F9: a part on S3 demotes back to the default tier — its objects are copied
/// down with range GETs, the catalog flips, and the S3 objects are deleted
/// through the tier's backend. Values above the klog threshold make the part
/// carry a vlog, so both object kinds travel.
#[test]
fn part_demotes_off_s3_back_to_the_default_tier() {
    let Some(cfg) = env_s3() else {
        eprintln!("skipping s3 demote test: ONDADB_S3_ENDPOINT not set");
        return;
    };
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.tiers = vec![TierDef::s3("s3", prefix.clone(), cfg.clone())];
    opts.part_mover_interval = Duration::ZERO;
    let big = vec![b'V'; 4 << 10];
    let s3 = S3Storage::new(&cfg).unwrap();
    let s3_tables = || {
        s3.list(&format!("{prefix}/cf-default"))
            .unwrap()
            .into_iter()
            .filter(|n| n.ends_with(".klog") || n.ends_with(".vlog"))
            .count()
    };
    {
        let db = DB::open(opts.clone()).unwrap();
        let cf = db.create_column_family("default", s3_mover_cfg()).unwrap();
        for i in 0..5u32 {
            db.put(&cf, format!("img/{i:03}").as_bytes(), &big, Duration::ZERO)
                .unwrap();
            db.put(&cf, format!("log/{i:03}").as_bytes(), b"LOG", Duration::ZERO)
                .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        assert_eq!(db.run_part_mover().unwrap(), 1);
        assert_eq!(s3_tables(), 2, "klog + vlog on s3");

        db.move_part_to_default_tier(&cf, "img").unwrap();
        assert_eq!(s3_tables(), 0, "the S3 source objects are retired");
        for i in 0..5u32 {
            assert_eq!(db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(), big);
        }
        db.close().unwrap();
    }
    // Local after reopen — with no S3 tier configured at all.
    let mut local_only = opts.clone();
    local_only.tiers.clear();
    let db = DB::open(local_only).unwrap();
    let cf = db.get_column_family("default").unwrap();
    for i in 0..5u32 {
        assert_eq!(db.get(&cf, format!("img/{i:03}").as_bytes()).unwrap(), big);
    }
    assert_eq!(db.get(&cf, b"log/000").unwrap(), b"LOG");
    db.close().unwrap();
    cleanup(&cfg, &prefix);
}

/// F7 end-to-end on a real object store: a receipts checkpoint (every object
/// store-verified), then a lazy remote open that reads through range GETs with
/// no HEAD per table, then a download restore; and a prefix without a MANIFEST
/// is "no checkpoint".
#[test]
fn object_checkpoint_roundtrip_on_s3() {
    use ondadb::checkpoint::{
        open_remote_checkpoint, restore_from_object_store, ObjectCheckpointOptions,
    };
    let Some(cfg) = env_s3() else {
        eprintln!("skipping s3 checkpoint test: ONDADB_S3_ENDPOINT not set");
        return;
    };
    let prefix = unique_prefix();
    let s3 = S3Storage::new(&cfg).unwrap();
    let src = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(src.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let big = vec![b'B'; 2 << 10];
    for i in 0..50u32 {
        db.put(&cf, format!("k{i:03}").as_bytes(), &big, Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.put(&cf, b"small", b"s", Duration::ZERO).unwrap();
    let ck = db
        .checkpoint_to_object_store(
            s3.as_ref(),
            &prefix,
            &ObjectCheckpointOptions {
                receipts: true,
                ..Default::default()
            },
        )
        .unwrap();
    db.close().unwrap();
    assert!(!ck.receipts.is_empty());
    assert!(
        ck.receipts.iter().all(|r| r.store_verified),
        "every upload must be store-verified: {:?}",
        ck.receipts
    );

    // Lazy open over a read-only view of the bucket.
    let ro = S3Storage::new(&S3Config {
        read_only: true,
        ..cfg.clone()
    })
    .unwrap();
    let metrics = ro.metrics();
    let mount = tempfile::tempdir().unwrap();
    let mut o = Options::new(mount.path().join("m").to_str().unwrap());
    o.read_only = true;
    let remote = open_remote_checkpoint(ro.clone(), &prefix, o).unwrap();
    let rcf = remote.get_column_family("default").unwrap();
    assert_eq!(remote.get(&rcf, b"k007").unwrap(), big);
    assert_eq!(remote.get(&rcf, b"small").unwrap(), b"s");
    assert_eq!(
        metrics.heads.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "one HEAD for the MANIFEST download, none per table"
    );
    assert!(metrics.range_gets.load(std::sync::atomic::Ordering::Relaxed) > 0);
    remote.close().unwrap();

    let dest = tempfile::tempdir().unwrap();
    restore_from_object_store(ro.as_ref(), &prefix, dest.path().join("db")).unwrap();
    let mut o = Options::new(dest.path().join("db").to_str().unwrap());
    o.read_only = true;
    let restored = DB::open(o).unwrap();
    let rcf = restored.get_column_family("default").unwrap();
    assert_eq!(restored.get(&rcf, b"k049").unwrap(), big);
    restored.close().unwrap();

    // Nothing under a fresh prefix: NotFound, not corruption.
    let empty = format!("{prefix}-nothing");
    assert!(matches!(
        restore_from_object_store(ro.as_ref(), &empty, dest.path().join("e")),
        Err(ondadb::OndaError::NotFound)
    ));

    for r in &ck.receipts {
        let _ = s3.delete(&r.key);
    }
}

/// On an S3-resident part every uncached vlog read is a range GET, which is
/// where the value cache pays for itself most visibly. The second read of a hot
/// large value must issue **no** request at all: the klog block and the decoded
/// vlog value are both resident by then.
///
/// The tier is registered with `TierDef::custom` rather than `TierDef::s3` for
/// one reason only: it hands the test the very `S3Storage` the DB will use, so
/// `S3Metrics.range_gets` counts the DB's own requests. The backend, and
/// therefore the read path, is identical.
#[test]
fn warm_vlog_value_issues_no_range_get() {
    let Some(cfg) = env_s3() else {
        eprintln!("skipping s3 vlog cache test: ONDADB_S3_ENDPOINT not set");
        return;
    };
    let prefix = unique_prefix();

    let s3 = S3Storage::new(&cfg).unwrap();
    let metrics = s3.metrics();

    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.tiers = vec![TierDef::custom("s3", prefix.clone(), s3.clone())];
    opts.part_mover_interval = Duration::ZERO;

    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                // 1 MiB ceiling: comfortably above the 32 KiB value below.
                max_cached_vlog_value_bytes: 1 << 20,
                ..s3_mover_cfg()
            },
        )
        .unwrap();

    // One separated value (well above the 512-byte default threshold) plus
    // enough neighbours to make a real part.
    let big = vec![b'V'; 32 << 10];
    for i in 0..5u32 {
        db.put(&cf, format!("img/{i:03}").as_bytes(), &big, Duration::ZERO)
            .unwrap();
        db.put(
            &cf,
            format!("log/{i:03}").as_bytes(),
            b"LOG",
            Duration::ZERO,
        )
        .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(db.run_part_mover().unwrap(), 1, "the img/ part must move");

    // Cold: this is the read that pays for the range GETs.
    assert_eq!(db.get(&cf, b"img/002").unwrap(), big);

    let before = metrics
        .range_gets
        .load(std::sync::atomic::Ordering::Relaxed);
    let hits_before = db.stats().vlog_cache_hits;
    for _ in 0..3 {
        assert_eq!(db.get(&cf, b"img/002").unwrap(), big);
    }
    let after = metrics
        .range_gets
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        after, before,
        "a warm large-value read must not touch the object store"
    );
    assert_eq!(
        db.stats().vlog_cache_hits - hits_before,
        3,
        "each warm read must be a vlog cache hit"
    );

    db.close().unwrap();
    cleanup(&cfg, &prefix);
}
