//! Point reads and scans must agree on what a key is, for every built-in
//! comparator: every key a scan yields is readable by `get` with the same
//! value, and keys a comparator orders "alike" (letter case, for
//! `case_insensitive`) stay distinct keys. Before 0.10.1, `case_insensitive`
//! called `k10` and `K10` equal: `get("K10")` missed a stored `k10`, a later
//! `put("K10")` was invisible to `get("k10")`, and a scan merged the two.

use std::collections::BTreeMap;
use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, DB};

const COMPARATORS: [&str; 6] = [
    "memcmp",
    "reverse",
    "lexicographic",
    "uint64",
    "int64",
    "case_insensitive",
];

/// Keys that collide under some comparator's "ignore this difference" rule:
/// case variants, 8-byte integers, and lengths around 8.
fn keys() -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = ["k10", "K10", "k1", "K1", "Abc", "aBC", "abc", "ABC", "z", "Z"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();
    for n in [0u64, 1, 255, 256, u64::MAX, 1 << 63] {
        v.push(n.to_be_bytes().to_vec());
    }
    v.push(b"1234567".to_vec());
    v.push(b"123456789".to_vec());
    v
}

fn scan(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut t = db.begin();
    let mut it = t.new_iterator(cf);
    let mut out = Vec::new();
    it.seek_to_first();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    drop(it);
    t.rollback().unwrap();
    out
}

fn check(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, model: &BTreeMap<Vec<u8>, Vec<u8>>, ctx: &str) {
    let scanned = scan(db, cf);
    assert_eq!(
        scanned.len(),
        model.len(),
        "{ctx}: the scan merged distinct keys: {scanned:?}"
    );
    for (k, v) in &scanned {
        assert_eq!(model.get(k), Some(v), "{ctx}: scan yielded {k:?} => {v:?}");
    }
    for (k, v) in model {
        let got = db
            .get(cf, k)
            .unwrap_or_else(|e| panic!("{ctx}: get({k:?}) failed: {e}; scan: {scanned:?}"));
        assert_eq!(&got, v, "{ctx}: get({k:?})");
    }
}

#[test]
fn get_and_scan_agree_for_every_builtin_comparator() {
    for unified in [false, true] {
        for name in COMPARATORS {
            let ctx = format!("{name}, unified={unified}");
            let dir = tempfile::tempdir().unwrap();
            let mut o = Options::new(dir.path().to_str().unwrap());
            o.unified_memtable = unified;
            let db = DB::open(o).unwrap();
            let cf = db
                .create_column_family(
                    "c",
                    ColumnFamilyConfig {
                        comparator_name: name.into(),
                        ..ColumnFamilyConfig::default()
                    },
                )
                .unwrap();
            let mut model = BTreeMap::new();
            for (i, k) in keys().into_iter().enumerate() {
                let v = format!("v{i}").into_bytes();
                db.put(&cf, &k, &v, Duration::ZERO).unwrap();
                model.insert(k, v);
            }
            check(&db, &cf, &model, &format!("{ctx}, memtable"));
            db.flush_memtable(&cf).unwrap();
            check(&db, &cf, &model, &format!("{ctx}, flushed"));
            // Overwrite one spelling of each case pair: the other must keep
            // its own value, in the memtable over a table and after a flush.
            for k in ["K10", "aBC"] {
                let v = format!("new-{k}").into_bytes();
                db.put(&cf, k.as_bytes(), &v, Duration::ZERO).unwrap();
                model.insert(k.as_bytes().to_vec(), v);
            }
            check(&db, &cf, &model, &format!("{ctx}, overwrite over table"));
            db.flush_memtable(&cf).unwrap();
            check(&db, &cf, &model, &format!("{ctx}, overwrite flushed"));
            db.close().unwrap();
        }
    }
}
