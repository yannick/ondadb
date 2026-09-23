//! F6: the changed-table diff behind incremental backups.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, DB};

fn put_n(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, prefix: &str, n: u32) {
    for i in 0..n {
        db.put(
            cf,
            format!("{prefix}{i:03}").as_bytes(),
            b"v",
            Duration::ZERO,
        )
        .unwrap();
    }
}

/// wavesdb's `TestSSTablesSince`, ported: only the second flush's table holds
/// writes made after `seq1`, each result names its family, and "since 0" is
/// every table.
#[test]
fn sstables_since_reports_tables_holding_newer_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();

    put_n(&db, &cf, "a", 50);
    db.flush_memtable(&cf).unwrap();
    let seq1 = db.live_sstables().iter().map(|t| t.max_seq).max().unwrap();

    put_n(&db, &cf, "b", 50);
    db.flush_memtable(&cf).unwrap();

    let since = db.sstables_since(seq1);
    assert_eq!(
        since.len(),
        1,
        "one table holds writes after seq1: {since:?}"
    );
    assert_eq!(since[0].cf, "default");
    assert!(since[0].max_seq > seq1);
    assert_eq!(db.sstables_since(0).len(), 2);
    db.close().unwrap();
}

/// The diff is by identity, so it catches what `sstables_since` cannot: a
/// compaction that rewrites only OLD data into a new table. An incremental
/// that shipped `sstables_since` alone would restore a catalog naming a table
/// it never uploaded.
#[test]
fn sstables_diff_reports_added_and_removed_across_compaction_and_families() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let other = db
        .create_column_family("other", ColumnFamilyConfig::default())
        .unwrap();

    put_n(&db, &cf, "a", 20);
    db.flush_memtable(&cf).unwrap();
    put_n(&db, &other, "o", 20);
    db.flush_memtable(&other).unwrap();
    let prior = db.live_sstables();
    assert_eq!(prior.len(), 2);
    let seq1 = prior.iter().map(|t| t.max_seq).max().unwrap();

    // Nothing changed: an empty diff.
    let same = db.sstables_diff(&prior);
    assert!(same.added.is_empty() && same.removed.is_empty(), "{same:?}");

    // Rewrite `default`'s only table without adding data.
    db.compact(&cf).unwrap();
    let old_default = prior.iter().find(|t| t.cf == "default").unwrap().clone();
    let diff = db.sstables_diff(&prior);
    assert_eq!(diff.removed, vec![old_default.clone()]);
    assert_eq!(diff.added.len(), 1, "{diff:?}");
    let rewritten = &diff.added[0];
    assert_eq!(rewritten.cf, "default");
    assert_ne!(rewritten.id, old_default.id);
    assert!(
        rewritten.max_seq <= seq1,
        "the rewrite holds only old data, so max_seq cannot flag it"
    );
    assert!(
        db.sstables_since(seq1).is_empty(),
        "sstables_since cannot see a pure rewrite — that is why the diff exists"
    );

    // Dropping a family removes its tables from the live set.
    db.drop_column_family("other").unwrap();
    let diff = db.sstables_diff(&prior);
    assert_eq!(diff.removed.len(), 2, "{diff:?}");
    assert!(diff.removed.iter().any(|t| t.cf == "other"));
    db.close().unwrap();
}
