//! `DB::compact_range` (plan C F3, wavesdb `CompactRange`): manual compaction
//! of a key span through the ordinary job path.

use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, PartitionRule, DB};

fn cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        // Nothing compacts unless the test asks.
        l1_file_count_trigger: 64,
        ..ColumnFamilyConfig::default()
    }
}

fn open(dir: &std::path::Path, cfg: ColumnFamilyConfig) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", cfg).unwrap();
    (db, cf)
}

fn put(db: &DB, cf: &Arc<ColumnFamily>, k: &str, v: &str) {
    db.put(cf, k.as_bytes(), v.as_bytes(), Duration::ZERO).unwrap();
}

/// Which level each table id sits in, as `(level, min_key)` pairs.
fn layout(cf: &Arc<ColumnFamily>) -> Vec<(usize, String)> {
    cf.table_metadata()
        .iter()
        .enumerate()
        .flat_map(|(level, tables)| {
            tables
                .iter()
                .map(move |t| (level, String::from_utf8(t.min_key.clone()).unwrap()))
        })
        .collect()
}

/// The span's tables go down; the rest stay; every value reads back.
#[test]
fn compacts_only_the_tables_in_the_span() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), cfg());
    // L0, oldest first: [a*], [m*]. Then a push so a deeper level exists.
    for i in 0..50 {
        put(&db, &cf, &format!("a{i:03}"), "old");
    }
    db.flush_memtable(&cf).unwrap();
    for i in 0..50 {
        put(&db, &cf, &format!("a{i:03}"), "new");
    }
    db.flush_memtable(&cf).unwrap();
    for i in 0..50 {
        put(&db, &cf, &format!("m{i:03}"), "m");
    }
    db.flush_memtable(&cf).unwrap();
    assert_eq!(cf.stats().levels[0].0, 3);

    db.compact_range(&cf, Included(b"a".as_slice()), Excluded(b"b".as_slice()))
        .unwrap();
    let l = layout(&cf);
    // The newest file [m*] is outside the span and newer than every in-span
    // file, so it stays in L0; both a-files went to the bottom as one table.
    assert_eq!(l.iter().filter(|(lv, _)| *lv == 0).count(), 1, "{l:?}");
    assert!(l.contains(&(0, "m000".into())), "{l:?}");
    let bottom = cf.stats().num_levels - 1;
    assert!(bottom >= 1);
    assert!(l.contains(&(bottom, "a000".into())), "{l:?}");
    // Shadowed versions were collapsed: 50 a-keys + 50 m-keys.
    assert_eq!(cf.stats().num_entries, 100);
    for i in 0..50 {
        assert_eq!(db.get(&cf, format!("a{i:03}").as_bytes()).unwrap(), b"new");
        assert_eq!(db.get(&cf, format!("m{i:03}").as_bytes()).unwrap(), b"m");
    }
    db.close().unwrap();
}

/// Bounds follow the iterator convention: an `Excluded` upper bound equal to
/// a table's only key does not select it; `Included` does.
#[test]
fn bounds_are_included_excluded_like_bounded_iterators() {
    for (upper, moved) in [(Excluded(b"m".as_slice()), false), (Included(b"m".as_slice()), true)] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), cfg());
        put(&db, &cf, "c", "1"); // older L0 file: {c}
        db.flush_memtable(&cf).unwrap();
        put(&db, &cf, "m", "2"); // newer L0 file: {m}
        db.flush_memtable(&cf).unwrap();
        db.compact_range(&cf, Unbounded, upper).unwrap();
        let l = layout(&cf);
        assert_eq!(
            l.contains(&(0, "m".into())),
            !moved,
            "upper {upper:?}: {l:?}"
        );
        assert!(!l.contains(&(0, "c".into())), "{{c}} is in the span: {l:?}");
        assert_eq!(db.get(&cf, b"c").unwrap(), b"1");
        assert_eq!(db.get(&cf, b"m").unwrap(), b"2");
        db.close().unwrap();
    }
}

/// Tombstones in the span reach the bottom and are dropped with the puts they
/// shadowed; tombstones outside the span are untouched.
#[test]
fn drops_tombstones_in_the_span_only() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), cfg());
    for i in 0..100 {
        put(&db, &cf, &format!("a{i:03}"), "v");
        put(&db, &cf, &format!("z{i:03}"), "v");
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap(); // everything at the bottom, split a*/z* below
    for i in 0..100 {
        db.delete(&cf, format!("a{i:03}").as_bytes()).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    for i in 0..100 {
        db.delete(&cf, format!("z{i:03}").as_bytes()).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    assert_eq!(cf.stats().num_tombstones, 200);

    db.compact_range(&cf, Included(b"a".as_slice()), Excluded(b"b".as_slice()))
        .unwrap();
    let s = cf.stats();
    // The z-tombstones' L0 file is newer than the in-span one and outside the
    // span, so it stayed; the a-tombstones and the a-puts are gone.
    assert_eq!(s.num_tombstones, 100, "{s:?}");
    assert!(db.get(&cf, b"a000").is_err());
    assert!(db.get(&cf, b"z000").is_err());
    db.close().unwrap();
}

/// A live snapshot keeps what it sees: the range compaction may not drop a
/// tombstone's shadowed put that an older snapshot still reads.
#[test]
fn a_live_snapshot_keeps_its_versions() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), cfg());
    put(&db, &cf, "k", "v1");
    db.flush_memtable(&cf).unwrap();
    let snap = db.snapshot();
    db.delete(&cf, b"k").unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact_range(&cf, Unbounded, Unbounded).unwrap();
    assert_eq!(snap.get(&cf, b"k").unwrap(), b"v1");
    assert!(db.get(&cf, b"k").is_err());
    drop(snap);
    db.compact_range(&cf, Unbounded, Unbounded).unwrap();
    assert_eq!(cf.stats().num_entries, 0, "{:?}", cf.stats());
    db.close().unwrap();
}

/// Bottom output is cut at partition boundaries, as any bottom compaction's.
#[test]
fn bottom_output_is_partition_cut() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(
        dir.path(),
        ColumnFamilyConfig {
            partition_rules: vec![
                PartitionRule {
                    prefix: b"img/".to_vec(),
                    name: "img".into(),
                },
                PartitionRule {
                    prefix: b"txt/".to_vec(),
                    name: "txt".into(),
                },
            ],
            ..cfg()
        },
    );
    for i in 0..20 {
        put(&db, &cf, &format!("img/{i:03}"), "i");
        put(&db, &cf, &format!("txt/{i:03}"), "t");
    }
    db.flush_memtable(&cf).unwrap();
    db.compact_range(&cf, Unbounded, Unbounded).unwrap();
    let meta = cf.table_metadata();
    let bottom = meta.last().unwrap();
    let mut parts: Vec<Option<String>> = bottom.iter().map(|t| t.partition.clone()).collect();
    parts.sort();
    assert_eq!(parts, vec![Some("img".into()), Some("txt".into())]);
    db.close().unwrap();
}

/// A read-only handle refuses; an empty span is a no-op.
#[test]
fn read_only_refuses_and_empty_span_is_a_noop() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = open(dir.path(), cfg());
        put(&db, &cf, "k", "v");
        db.flush_memtable(&cf).unwrap();
        let before = cf.stats().compaction_count;
        let none: Bound<&[u8]> = Included(b"x".as_slice());
        db.compact_range(&cf, none, Unbounded).unwrap();
        assert_eq!(cf.stats().compaction_count, before);
        db.close().unwrap();
    }
    let mut o = Options::new(dir.path().to_str().unwrap());
    o.read_only = true;
    let db = DB::open(o).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert!(matches!(
        db.compact_range(&cf, Unbounded, Unbounded),
        Err(OndaError::ReadOnly(_))
    ));
}
