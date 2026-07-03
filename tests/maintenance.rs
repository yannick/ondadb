//! Tests for checkpoint, backup, clone, and stats.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, DB};

fn fill(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, n: u32) {
    for i in 0..n {
        db.put(cf, format!("k{i:05}").as_bytes(), b"value", Duration::ZERO)
            .unwrap();
    }
}

#[test]
fn stats_report_levels_and_counts() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    fill(&db, &cf, 500);
    db.flush_memtable(&cf).unwrap();
    let s = cf.stats();
    assert_eq!(s.name, "default");
    assert!(s.num_entries >= 500);
    assert!(s.levels[0].0 >= 1, "expected at least one L0 file");
    let ds = db.stats();
    assert_eq!(ds.num_column_families, 1);
    assert!(ds.total_sstables >= 1);
    db.close().unwrap();
}

#[test]
fn checkpoint_is_readable() {
    let dir = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        fill(&db, &cf, 1000);
        db.checkpoint(ckpt.path()).unwrap();
        db.close().unwrap();
    }
    // Open the checkpoint directory as a database and verify the data.
    let db = DB::open(Options::new(ckpt.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in checkpoint");
    for i in 0..1000u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:05}").as_bytes()).unwrap(),
            b"value"
        );
    }
    db.close().unwrap();
}

#[test]
fn backup_is_independent_copy() {
    let dir = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        fill(&db, &cf, 500);
        db.backup(backup.path()).unwrap();
        db.close().unwrap();
    }
    let db = DB::open(Options::new(backup.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in backup");
    assert_eq!(db.get(&cf, b"k00042").unwrap(), b"value");
    db.close().unwrap();
}

#[test]
fn backup_consistent_during_compaction() {
    // Take a backup while heavy write + flush + compaction churn is running. The
    // backup's manifest must reference only files that exist in the backup, and it
    // must reopen with every key present. Before deletion-deferral, a compaction
    // could unlink an SSTable mid-backup, leaving the copied manifest referencing a
    // missing file (or the link failing).
    let dir = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let cfg = ColumnFamilyConfig {
        write_buffer_size: 32 * 1024, // tiny -> many flushes
        l1_file_count_trigger: 2,     // frequent compactions
        ..ColumnFamilyConfig::default()
    };
    let n = 30_000u32;
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("default", cfg).unwrap();
        // Write enough to have flushes/compactions in flight, then back up.
        for i in 0..n {
            db.put(&cf, format!("k{i:08}").as_bytes(), b"value", Duration::ZERO)
                .unwrap();
        }
        db.backup(backup.path()).unwrap();
        db.close().unwrap();
    }
    // The backup must be self-consistent and complete.
    let db = DB::open(Options::new(backup.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in backup");
    for i in 0..n {
        assert_eq!(
            db.get(&cf, format!("k{i:08}").as_bytes()).unwrap(),
            b"value",
            "missing k{i} in backup"
        );
    }
    db.close().unwrap();
}

#[test]
fn clone_column_family_shares_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let src = db
        .create_column_family("src", ColumnFamilyConfig::default())
        .unwrap();
    fill(&db, &src, 300);
    let dst = db.clone_column_family("src", "dst").unwrap();
    for i in 0..300u32 {
        assert_eq!(
            db.get(&dst, format!("k{i:05}").as_bytes()).unwrap(),
            b"value"
        );
    }
    // Writes to dst do not affect src.
    db.put(&dst, b"only-dst", b"x", Duration::ZERO).unwrap();
    assert!(db.get(&src, b"only-dst").is_err());
    db.close().unwrap();
}
