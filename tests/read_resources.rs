//! Process-wide read resources shared by read-only databases (wavesdb
//! `ReadResources`, `23648c8` / `f6b3def`).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ondadb::{
    ColumnFamilyConfig, Compression, OndaError, Options, ReadResourceOptions, ReadResources, DB,
};

const KEYS: usize = 200;

fn key(i: usize) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

/// Write `KEYS` keys whose values are tagged `tag`, flushed into one table.
/// Two databases built this way get the **same table ids** (fresh catalogs mint
/// ids identically) and different contents — the case a shared cache keyed by
/// bare file id would get wrong.
fn build(dir: &Path, tag: &str) -> Vec<u64> {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "c",
            ColumnFamilyConfig {
                // Compressed, so reads go through the block cache under
                // `mmap-reads` too (uncompressed blocks are zero-copy views).
                compression: Compression::Zstd,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for i in 0..KEYS {
        db.put(&cf, &key(i), format!("{tag}-{i}").as_bytes(), Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let ids = cf
        .table_metadata()
        .iter()
        .flatten()
        .map(|t| t.id)
        .collect();
    db.close().unwrap();
    ids
}

fn open_leased(dir: &Path, rr: &Arc<ReadResources>, ns: Option<&str>) -> ondadb::Result<DB> {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.read_only = true;
    opts.read_resources = Some(rr.clone());
    opts.read_cache_namespace = ns.map(str::to_owned);
    DB::open(opts)
}

fn check_all(db: &DB, tag: &str) {
    let cf = db.get_column_family("c").unwrap();
    for i in 0..KEYS {
        assert_eq!(
            db.get(&cf, &key(i)).unwrap(),
            format!("{tag}-{i}").as_bytes(),
            "database {tag} served another database's bytes"
        );
    }
}

fn resources() -> Arc<ReadResources> {
    ReadResources::new(ReadResourceOptions {
        block_cache_bytes: 8 << 20,
        max_open_files: 32,
        max_open_readers: 16,
        max_reader_bytes: 8 << 20,
    })
}

#[test]
fn writable_open_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let rr = resources();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.read_resources = Some(rr.clone());
    assert!(matches!(DB::open(opts), Err(OndaError::InvalidArgs(_))));
    assert_eq!(rr.stats().leases, 0);
}

/// The critical property: two databases with identical table ids and
/// different contents, sharing one cache, each read their own bytes — hot and
/// cold, interleaved, under the default (path) namespace and under distinct
/// caller namespaces alike.
#[test]
fn identical_table_ids_never_alias() {
    let (da, db_) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ids_a = build(da.path(), "alpha");
    let ids_b = build(db_.path(), "beta");
    assert_eq!(ids_a, ids_b, "fixture: both databases must use the same table ids");

    for names in [(None, None), (Some("ckpt-a"), Some("ckpt-b"))] {
        let rr = resources();
        let a = open_leased(da.path(), &rr, names.0).unwrap();
        let b = open_leased(db_.path(), &rr, names.1).unwrap();
        for _ in 0..2 {
            check_all(&a, "alpha");
            check_all(&b, "beta");
        }
        let s = rr.stats();
        assert_eq!(s.leases, 2);
        assert_eq!(s.table.open_readers, 2, "one reader per database: {s:?}");
        assert!(s.block.entries > 0 && s.block.hits > 0, "{s:?}");
        a.close().unwrap();
        b.close().unwrap();
        rr.close();
    }
}

/// Two opens under one namespace (here: the same directory) share one reader
/// and its blocks; closing one does not flush what the other still uses.
#[test]
fn same_namespace_shares_readers() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), "v");
    let rr = resources();
    let a = open_leased(dir.path(), &rr, None).unwrap();
    check_all(&a, "v");
    let after_a = rr.stats();
    assert_eq!(after_a.table.open_readers, 1);

    let b = open_leased(dir.path(), &rr, None).unwrap();
    check_all(&b, "v");
    let after_b = rr.stats();
    assert_eq!(after_b.table.open_readers, 1, "the second open re-opened the table");
    assert!(after_b.table.hits > after_a.table.hits);
    assert_eq!(after_b.table.misses, after_a.table.misses);
    assert!(after_b.block.hits > after_a.block.hits, "blocks were not shared");

    a.close().unwrap();
    assert_eq!(rr.stats().table.open_readers, 1, "closing one open flushed a shared reader");
    check_all(&b, "v");
    b.close().unwrap();
    // The namespace's last database is gone: its reader and blocks with it.
    let s = rr.stats();
    assert_eq!((s.table.open_readers, s.block.entries, s.leases), (0, 0, 0), "{s:?}");
}

/// `close` stops new leases at once, lets leased databases keep working, and
/// empties the caches when the last of them closes.
#[test]
fn close_defers_until_the_last_lease() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    build(d1.path(), "one");
    build(d2.path(), "two");
    let d3 = tempfile::tempdir().unwrap();
    build(d3.path(), "three");
    let rr = resources();
    let a = open_leased(d1.path(), &rr, None).unwrap();
    let b = open_leased(d2.path(), &rr, None).unwrap();
    check_all(&a, "one");
    check_all(&b, "two");

    rr.close();
    assert!(rr.stats().closing);
    assert!(matches!(
        open_leased(d3.path(), &rr, None),
        Err(OndaError::InvalidArgs(_))
    ));
    check_all(&b, "two"); // a leased database is not interrupted

    a.close().unwrap();
    assert_eq!(rr.stats().table.open_readers, 1, "a's own namespace is purged");
    check_all(&b, "two");
    b.close().unwrap();
    let s = rr.stats();
    assert_eq!(
        (s.leases, s.table.open_readers, s.block.entries, s.open_files),
        (0, 0, 0, 0),
        "{s:?}"
    );
    rr.close(); // idempotent
}

/// A database whose handle is dropped without `close` still returns its lease.
#[test]
fn dropping_a_database_releases_its_lease() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), "d");
    let rr = resources();
    {
        let db = open_leased(dir.path(), &rr, None).unwrap();
        check_all(&db, "d");
        assert_eq!(rr.stats().leases, 1);
    }
    assert_eq!(rr.stats().leases, 0);
}
