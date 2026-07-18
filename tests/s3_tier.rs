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
    })
}

fn unique_prefix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("ondadb-tier-test/{nanos}")
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
