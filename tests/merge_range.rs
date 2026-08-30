//! Merge operators (1.1) composed with range tombstones (1.2).
//!
//! Nothing in either feature's own tests exercises the pair, and the pair has a
//! rule of its own: **a range delete is, for one key, a deleted base at its
//! sequence**. Operands above the span fold onto nothing; operands at or below
//! it — and the base under them — are masked. The three read paths (point,
//! batch and both scan directions) must all say the same thing, before and
//! after a flush and a compaction, because each resolves the chain with
//! different code.

use std::sync::Arc;
use std::time::Duration;

use ondadb::format::CAP_RANGE_DELETES;
use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, Options, DB};

/// Appends operands to the base, `|`-separated; a missing base renders as the
/// empty string, so "masked base" and "no base" are the same observable and a
/// dropped operand shows up as a missing field rather than a silent zero.
#[derive(Debug)]
struct Concat;

impl MergeOperator for Concat {
    fn name(&self) -> &str {
        "test.concat.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let mut out = String::from_utf8(existing.unwrap_or(b"").to_vec())
            .map_err(|e| format!("base is not utf-8: {e}"))?;
        for operand in operands {
            out.push('|');
            out.push_str(&String::from_utf8(operand.to_vec()).map_err(|e| e.to_string())?);
        }
        Ok(out.into_bytes())
    }
}

fn open_merge_ranged(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "m",
            ColumnFamilyConfig {
                merge_operator_name: Some(Concat.name().to_string()),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    (db, cf)
}

/// What every read path says about `key`, as `Option<Vec<u8>>`. They must
/// agree; the assertion is here rather than at each call site so a divergence
/// names itself.
fn read_all(db: &DB, cf: &Arc<ColumnFamily>, key: &[u8]) -> Option<Vec<u8>> {
    let point = db.get(cf, key).ok();
    let batch = db.multi_get(cf, &[key]).pop().unwrap().ok();
    assert_eq!(point, batch, "point and batch reads disagree on {key:?}");

    for backward in [false, true] {
        let t = db.begin();
        let mut it = t.new_iterator(cf);
        let mut found = None;
        if backward {
            it.seek_to_last();
            while it.valid() {
                if it.key() == key {
                    found = Some(it.value().to_vec());
                }
                it.prev();
            }
        } else {
            it.seek_to_first();
            while it.valid() {
                if it.key() == key {
                    found = Some(it.value().to_vec());
                }
                it.next();
            }
        }
        assert!(it.err().is_none(), "{:?}", it.err());
        assert_eq!(
            found, point,
            "scan (backward={backward}) disagrees with the point read on {key:?}"
        );
    }
    point
}

/// Operands written *after* a span are above it: the span masks the base they
/// would otherwise have folded onto, and nothing else.
#[test]
fn operands_above_a_span_fold_onto_no_base() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"old").unwrap();
    db.delete_range(&cf, b"k", b"l").unwrap();
    db.merge(&cf, b"k", b"new1").unwrap();
    db.merge(&cf, b"k", b"new2").unwrap();

    // `base` and `old` are both at or below the span; only the two operands
    // above it survive, folded onto an absent base.
    assert_eq!(read_all(&db, &cf, b"k").as_deref(), Some(&b"|new1|new2"[..]));
    db.flush_memtable(&cf).unwrap();
    assert_eq!(
        read_all(&db, &cf, b"k").as_deref(),
        Some(&b"|new1|new2"[..]),
        "the answer must not change when the chain and the fragment move to an SSTable"
    );
    db.compact(&cf).unwrap();
    assert_eq!(
        read_all(&db, &cf, b"k").as_deref(),
        Some(&b"|new1|new2"[..]),
        "nor when compaction folds and re-fragments them"
    );
    db.close().unwrap();
}

/// A span above the whole chain hides it, exactly as a point tombstone would.
#[test]
fn a_span_above_the_chain_hides_it() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();
    db.delete_range(&cf, b"k", b"l").unwrap();

    assert_eq!(read_all(&db, &cf, b"k"), None);
    db.flush_memtable(&cf).unwrap();
    assert_eq!(read_all(&db, &cf, b"k"), None);
    db.compact(&cf).unwrap();
    assert_eq!(
        read_all(&db, &cf, b"k"),
        None,
        "compaction must not resurrect a masked chain"
    );
    db.close().unwrap();
}

/// A chain with no base at all under a span that predates it: the span changes
/// nothing, because there was nothing for it to mask.
#[test]
fn a_span_below_a_baseless_chain_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.delete_range(&cf, b"k", b"l").unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();

    assert_eq!(read_all(&db, &cf, b"k").as_deref(), Some(&b"|a|b"[..]));
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(read_all(&db, &cf, b"k").as_deref(), Some(&b"|a|b"[..]));
    db.close().unwrap();
}

/// The span covers a *range*, so a key beside the chain is untouched and the
/// scan still surfaces exactly the surviving keys in order.
#[test]
fn a_span_masks_only_the_keys_it_covers() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    for key in [&b"a"[..], b"m", b"z"] {
        db.put(&cf, key, b"base", Duration::ZERO).unwrap();
        db.merge(&cf, key, b"1").unwrap();
    }
    db.delete_range(&cf, b"b", b"n").unwrap();
    db.merge(&cf, b"m", b"2").unwrap();

    assert_eq!(read_all(&db, &cf, b"a").as_deref(), Some(&b"base|1"[..]));
    assert_eq!(read_all(&db, &cf, b"m").as_deref(), Some(&b"|2"[..]));
    assert_eq!(read_all(&db, &cf, b"z").as_deref(), Some(&b"base|1"[..]));

    let t = db.begin();
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    let mut keys = Vec::new();
    while it.valid() {
        keys.push(it.key().to_vec());
        it.next();
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    assert_eq!(keys, vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]);
    db.close().unwrap();
}

/// Fully masked keys disappear from the scan rather than surfacing as an empty
/// fold — the case that a `merged` group without a sequence of its own would
/// get wrong in the opposite direction.
#[test]
fn a_fully_masked_chain_is_absent_from_the_scan() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge_ranged(dir.path());
    db.put(&cf, b"a", b"keep", Duration::ZERO).unwrap();
    db.put(&cf, b"m", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"m", b"1").unwrap();
    db.delete_range(&cf, b"b", b"n").unwrap();

    for stage in ["memtable", "flushed", "compacted"] {
        let t = db.begin();
        let mut it = t.new_iterator(&cf);
        it.seek_to_first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(it.key().to_vec());
            it.next();
        }
        assert!(it.err().is_none(), "{:?}", it.err());
        assert_eq!(keys, vec![b"a".to_vec()], "at stage {stage}");
        drop(it);
        drop(t);
        match stage {
            "memtable" => db.flush_memtable(&cf).unwrap(),
            "flushed" => db.compact(&cf).unwrap(),
            _ => {}
        }
    }
    db.close().unwrap();
}
