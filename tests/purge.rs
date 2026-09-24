//! `DB::purge` / `DB::purge_column_family` (plan C F4, wavesdb `Purge` /
//! `PurgeColumnFamily`): flush, then compact the whole family to the bottom.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, OndaError, Options, DB};

fn cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        l1_file_count_trigger: 64,
        ..ColumnFamilyConfig::default()
    }
}

fn populated_levels(cf: &Arc<ColumnFamily>) -> usize {
    cf.stats().levels.iter().filter(|(n, _)| *n > 0).count()
}

/// wavesdb's `TestPurgeCollapsesAndPreservesData`: overwritten keys across
/// several tables collapse into one level and the latest values survive.
#[test]
fn purge_collapses_and_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", cfg()).unwrap();
    for round in 0..4 {
        for i in 0..300 {
            db.put(
                &cf,
                format!("k{i:04}").as_bytes(),
                format!("r{round}-v{i}").as_bytes(),
                Duration::ZERO,
            )
            .unwrap();
        }
        if round < 3 {
            db.flush_memtable(&cf).unwrap();
        }
        // The last round stays in the memtable: purge flushes it.
    }
    db.purge_column_family(&cf).unwrap();
    for i in 0..300 {
        assert_eq!(
            db.get(&cf, format!("k{i:04}").as_bytes()).unwrap(),
            format!("r3-v{i}").as_bytes()
        );
    }
    let s = cf.stats();
    assert_eq!(populated_levels(&cf), 1, "{s:?}");
    assert_eq!(s.levels[0].0, 0, "nothing left in L0: {s:?}");
    assert_eq!(s.num_entries, 300, "shadowed versions reclaimed: {s:?}");
    assert_eq!(s.memtable_entries, 0);
    db.close().unwrap();
}

/// Tombstones (with what they shadow) and expired TTL entries are dropped.
#[test]
fn purge_drops_tombstones_and_expired_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", cfg()).unwrap();
    for i in 0..100 {
        db.put(&cf, format!("live{i:03}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
        db.put(&cf, format!("dead{i:03}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
        db.put(
            &cf,
            format!("ttl{i:03}").as_bytes(),
            b"v",
            Duration::from_millis(1),
        )
        .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    for i in 0..100 {
        db.delete(&cf, format!("dead{i:03}").as_bytes()).unwrap();
    }
    std::thread::sleep(Duration::from_millis(20)); // past every TTL
    db.purge_column_family(&cf).unwrap();
    let s = cf.stats();
    assert_eq!(s.num_tombstones, 0, "{s:?}");
    assert_eq!(s.num_entries, 100, "only the live keys remain: {s:?}");
    assert_eq!(db.get(&cf, b"live000").unwrap(), b"v");
    db.close().unwrap();
}

/// `purge` covers every family; a read-only handle refuses both calls.
#[test]
fn purge_covers_every_family_and_refuses_read_only() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let a = db.create_column_family("a", cfg()).unwrap();
        let b = db.create_column_family("b", cfg()).unwrap();
        for cf in [&a, &b] {
            for round in 0..3 {
                for i in 0..50 {
                    db.put(cf, format!("k{i:03}").as_bytes(), &[round], Duration::ZERO)
                        .unwrap();
                }
                db.flush_memtable(cf).unwrap();
            }
        }
        db.purge().unwrap();
        for cf in [&a, &b] {
            assert_eq!(cf.stats().num_entries, 50);
            assert_eq!(cf.stats().levels[0].0, 0);
            assert_eq!(db.get(cf, b"k000").unwrap(), [2]);
        }
        db.close().unwrap();
    }
    let mut o = Options::new(dir.path().to_str().unwrap());
    o.read_only = true;
    let db = DB::open(o).unwrap();
    let a = db.get_column_family("a").unwrap();
    assert!(matches!(db.purge(), Err(OndaError::ReadOnly(_))));
    assert!(matches!(db.purge_column_family(&a), Err(OndaError::ReadOnly(_))));
}
