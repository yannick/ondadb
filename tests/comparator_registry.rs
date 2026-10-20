//! Custom comparators resolve by name through `Options::comparators`, exactly
//! as merge operators resolve through `Options::merge_fns`. A family records
//! only its comparator's name; an open that cannot resolve the name must refuse
//! before reading the family, because reading it in another order makes point
//! reads miss present keys and compaction write misordered tables
//! (yolodb-format divergence T1). Before this registry existed a custom
//! comparator could not be attached to a family at all.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ondadb::comparator::FnComparator;
use ondadb::{ColumnFamily, ColumnFamilyConfig, ComparatorRef, Options, DB};

/// Orders keys by their last byte, then by their bytes: a total order that is
/// `Equal` only for identical keys, and visibly not byte order.
fn suffix_first() -> ComparatorRef {
    Arc::new(FnComparator::new("suffix-first", suffix_order))
}

fn suffix_order(a: &[u8], b: &[u8]) -> Ordering {
    a.last().cmp(&b.last()).then_with(|| a.cmp(b))
}

fn named(name: &str) -> ComparatorRef {
    Arc::new(FnComparator::new(name, |a: &[u8], b: &[u8]| a.cmp(b)))
}

fn opts(dir: &Path, unified: bool, comparators: Vec<ComparatorRef>) -> Options {
    let mut o = Options::new(dir.to_str().unwrap());
    o.unified_memtable = unified;
    o.comparators = comparators;
    o
}

fn cfg(comparator: &str) -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        comparator_name: comparator.into(),
        ..Default::default()
    }
}

fn scan(db: &DB, cf: &Arc<ColumnFamily>) -> Vec<String> {
    let mut t = db.begin();
    let mut it = t.new_iterator(cf);
    let mut out = Vec::new();
    it.seek_to_first();
    while it.valid() {
        out.push(String::from_utf8(it.key().to_vec()).unwrap());
        it.next();
    }
    drop(it);
    t.rollback().unwrap();
    out
}

/// Every file under `dir` with its bytes: a refused open must change nothing.
fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                let rel = path.strip_prefix(base).unwrap().to_string_lossy().into_owned();
                out.insert(rel, std::fs::read(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

#[test]
fn registry_is_validated_at_open() {
    for (registry, what) in [
        (vec![named("")], "an empty name"),
        (vec![named("memcmp")], "a built-in's name"),
        (vec![named("case_insensitive")], "a built-in's name"),
        (vec![named("mine"), named("mine")], "a duplicate"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let err = DB::open(opts(dir.path(), false, registry)).expect_err(what);
        assert_eq!(err.kind(), "invalid_args", "{what}: {err}");
    }
    let dir = tempfile::tempdir().unwrap();
    DB::open(opts(dir.path(), false, vec![named("a"), named("b")]))
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn unregistered_comparator_is_refused_at_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(opts(dir.path(), false, vec![])).unwrap();
    let err = db.create_column_family("f", cfg("suffix-first")).unwrap_err();
    assert_eq!(err.kind(), "invalid_args", "{err}");
    assert!(err.to_string().contains("Options::comparators"), "{err}");
    let err = db
        .create_column_families(&[("g", cfg("memcmp")), ("h", cfg("suffix-first"))])
        .unwrap_err();
    assert_eq!(err.kind(), "invalid_args", "{err}");
    assert!(db.get_column_family("g").is_none(), "a refused batch creates nothing");
    db.close().unwrap();
}

#[test]
fn reopen_requires_the_registration() {
    let keys = ["a1", "b1", "c2", "d1", "a2", "a3", "b3"];
    let mut want: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    want.sort_by(|a, b| suffix_order(a.as_bytes(), b.as_bytes()));
    assert_ne!(want, {
        let mut bytewise = want.clone();
        bytewise.sort();
        bytewise
    });

    for unified in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = DB::open(opts(dir.path(), unified, vec![suffix_first()])).unwrap();
            let cf = db.create_column_family("f", cfg("suffix-first")).unwrap();
            // Half in a table, half only in the WAL, so both are read back.
            for k in &keys[..4] {
                db.put(&cf, k.as_bytes(), b"v", Duration::ZERO).unwrap();
            }
            db.flush_memtable(&cf).unwrap();
            for k in &keys[4..] {
                db.put(&cf, k.as_bytes(), b"v", Duration::ZERO).unwrap();
            }
            assert_eq!(scan(&db, &cf), want, "unified={unified}: before reopen");
            db.close().unwrap();
        }

        let before = snapshot(dir.path());
        let err = DB::open(opts(dir.path(), unified, vec![])).expect_err("no registry");
        assert_eq!(err.kind(), "invalid_args", "unified={unified}: {err}");
        assert!(err.to_string().contains("suffix-first"), "{err}");
        let mut ro = opts(dir.path(), unified, vec![named("other")]);
        ro.read_only = true;
        let err = DB::open(ro).expect_err("read-only, wrong registry");
        assert_eq!(err.kind(), "invalid_args", "unified={unified}: {err}");
        assert_eq!(snapshot(dir.path()), before, "unified={unified}: a refused open wrote");

        let db = DB::open(opts(dir.path(), unified, vec![suffix_first()])).unwrap();
        let cf = db.get_column_family("f").unwrap();
        assert_eq!(scan(&db, &cf), want, "unified={unified}: after reopen");
        for k in keys {
            assert_eq!(db.get(&cf, k.as_bytes()).unwrap(), b"v", "unified={unified}: get {k}");
        }
        db.compact(&cf).unwrap();
        assert_eq!(scan(&db, &cf), want, "unified={unified}: after compaction");
        let clone = db.clone_column_family("f", "f2").unwrap();
        assert_eq!(scan(&db, &clone), want, "unified={unified}: clone");
        let cleared = db.clear_column_family("f").unwrap();
        db.put(&cleared, b"z0", b"v", Duration::ZERO).unwrap();
        db.put(&cleared, b"a9", b"v", Duration::ZERO).unwrap();
        assert_eq!(scan(&db, &cleared), ["z0", "a9"], "unified={unified}: cleared keeps its order");
        db.close().unwrap();
    }
}

/// The same no-write guarantee for the other names the catalog stores: an
/// unregistered merge operator used to be refused only after the unified
/// layout had created its next WAL generation.
#[test]
fn refused_merge_operator_open_writes_nothing() {
    #[derive(Debug)]
    struct Concat;
    impl ondadb::MergeOperator for Concat {
        fn name(&self) -> &str {
            "concat"
        }
        fn full_merge(
            &self,
            _key: &[u8],
            existing: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> Result<Vec<u8>, String> {
            let mut out = existing.unwrap_or_default().to_vec();
            operands.iter().for_each(|o| out.extend_from_slice(o));
            Ok(out)
        }
    }
    for unified in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut o = opts(dir.path(), unified, vec![]);
            o.merge_fns = vec![Arc::new(Concat)];
            let db = DB::open(o).unwrap();
            let mut c = cfg("memcmp");
            c.merge_operator_name = Some("concat".into());
            let cf = db.create_column_family("m", c).unwrap();
            db.merge(&cf, b"k", b"x").unwrap();
            db.close().unwrap();
        }
        let before = snapshot(dir.path());
        let err = DB::open(opts(dir.path(), unified, vec![])).expect_err("no operator");
        assert_eq!(err.kind(), "invalid_args", "unified={unified}: {err}");
        assert_eq!(snapshot(dir.path()), before, "unified={unified}: a refused open wrote");
    }
}
