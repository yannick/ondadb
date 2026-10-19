//! A transaction iterator's overlay is built only from the buffered writes its
//! bounds can reach (wavesdb f5a5dc4). The iterator never yields a key outside
//! its bounds, so a bounded iterator must answer exactly what the unbounded one
//! answers, restricted to the bounds — under the family's comparator, across
//! every record kind, in both directions, and across savepoint rollback.
//!
//! The unbounded iterator is itself checked against per-key `Txn::get`, so the
//! equivalence is anchored to the point-read path rather than to whatever the
//! overlay happened to do before.

use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use ondadb::format::CAP_RANGE_DELETES;
use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, OndaError, Options, Txn, DB};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

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
        let mut out = existing.map(<[u8]>::to_vec).unwrap_or_default();
        for operand in operands {
            out.push(b'|');
            out.extend_from_slice(operand);
        }
        Ok(out)
    }
}

type Rows = Vec<(Vec<u8>, Vec<u8>)>;
type Cmp = fn(&[u8], &[u8]) -> Ordering;
type Bounds<'a> = (Bound<&'a [u8]>, Bound<&'a [u8]>);

fn bytewise(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

fn reverse(a: &[u8], b: &[u8]) -> Ordering {
    b.cmp(a)
}

fn ord_of(comparator: &str) -> Cmp {
    match comparator {
        "memcmp" => bytewise,
        "reverse" => reverse,
        other => panic!("no test ordering for {other}"),
    }
}

fn open(dir: &std::path::Path, comparator: &str, merge: bool) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                comparator_name: comparator.into(),
                merge_operator_name: merge.then(|| Concat.name().to_string()),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    (db, cf)
}

fn forward(it: &mut ondadb::Iterator) -> Rows {
    let mut rows = Vec::new();
    while it.valid() {
        rows.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    rows
}

fn backward(it: &mut ondadb::Iterator) -> Rows {
    let mut rows = Vec::new();
    while it.valid() {
        rows.push((it.key().to_vec(), it.value().to_vec()));
        it.prev();
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    rows
}

fn in_bounds(ord: Cmp, key: &[u8], lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    let above = match lower {
        Bound::Unbounded => true,
        Bound::Included(l) => ord(key, l).is_ge(),
        Bound::Excluded(l) => ord(key, l).is_gt(),
    };
    let below = match upper {
        Bound::Unbounded => true,
        Bound::Included(u) => ord(key, u).is_le(),
        Bound::Excluded(u) => ord(key, u).is_lt(),
    };
    above && below
}

/// The bounded iterator over `[lower, upper]` answers what the unbounded one
/// answers inside those bounds: forward, backward, and from a seek either way.
fn assert_bounded_matches(
    t: &Txn,
    cf: &Arc<ColumnFamily>,
    ord: Cmp,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    ctx: &str,
) {
    let mut full = t.new_iterator(cf);
    full.seek_to_first();
    let expected: Rows = forward(&mut full)
        .into_iter()
        .filter(|(k, _)| in_bounds(ord, k, lower, upper))
        .collect();

    let mut it = t.new_iterator_bounded(cf, lower, upper);
    it.seek_to_first();
    assert_eq!(forward(&mut it), expected, "forward, {ctx} {lower:?}..{upper:?}");
    it.seek_to_last();
    let mut back = backward(&mut it);
    back.reverse();
    assert_eq!(back, expected, "backward, {ctx} {lower:?}..{upper:?}");

    if let Some((mid, _)) = expected.get(expected.len() / 2) {
        let mut it = t.new_iterator_bounded(cf, lower, upper);
        it.seek(mid);
        let from_mid: Rows = expected
            .iter()
            .filter(|(k, _)| ord(k, mid).is_ge())
            .cloned()
            .collect();
        assert_eq!(forward(&mut it), from_mid, "seek, {ctx} {lower:?}..{upper:?}");
        it.seek_for_prev(mid);
        let mut to_mid = backward(&mut it);
        to_mid.reverse();
        let upto: Rows = expected
            .iter()
            .filter(|(k, _)| ord(k, mid).is_le())
            .cloned()
            .collect();
        assert_eq!(to_mid, upto, "seek_for_prev, {ctx} {lower:?}..{upper:?}");
    }
}

/// The unbounded iterator answers what `Txn::get` answers for every key.
fn assert_scan_matches_point_reads(
    t: &mut Txn,
    cf: &Arc<ColumnFamily>,
    ord: Cmp,
    keyspace: &[Vec<u8>],
    ctx: &str,
) {
    let mut keys = keyspace.to_vec();
    keys.sort_by(|a, b| ord(a, b));
    let mut expected = Rows::new();
    for k in keys {
        match t.get(cf, &k) {
            Ok(v) => expected.push((k, v)),
            Err(OndaError::NotFound) => {}
            Err(e) => panic!("get {k:?}: {e}"),
        }
    }
    let mut it = t.new_iterator(cf);
    it.seek_to_first();
    assert_eq!(forward(&mut it), expected, "scan vs point reads, {ctx}");
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:02}").into_bytes()
}

/// A bound endpoint: a real key, a key between two real ones, or a key past
/// either end of the keyspace.
fn endpoint(rng: &mut StdRng, n: usize) -> Vec<u8> {
    match rng.gen_range(0..4) {
        0 => b"a".to_vec(),
        1 => b"z".to_vec(),
        2 => {
            let mut k = key(rng.gen_range(0..n));
            k.push(b'x');
            k
        }
        _ => key(rng.gen_range(0..n)),
    }
}

fn bound(rng: &mut StdRng, k: &[u8]) -> Bound<Vec<u8>> {
    match rng.gen_range(0..5) {
        0 => Bound::Unbounded,
        1 | 2 => Bound::Included(k.to_vec()),
        _ => Bound::Excluded(k.to_vec()),
    }
}

fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
    }
}

/// `(start, end)` ordered under `ord`, or `None` if the two keys are equal.
fn span(ord: Cmp, a: Vec<u8>, b: Vec<u8>) -> Option<(Vec<u8>, Vec<u8>)> {
    match ord(&a, &b) {
        Ordering::Less => Some((a, b)),
        Ordering::Greater => Some((b, a)),
        Ordering::Equal => None,
    }
}

fn covers(ord: Cmp, (s, e): &(Vec<u8>, Vec<u8>), k: &[u8]) -> bool {
    ord(s, k).is_le() && ord(e, k).is_gt()
}

/// A key no generated span can cover (spans run between `k00` and `k29`).
const SENTINEL: &[u8] = b"zz";

fn randomized(seed: u64, comparator: &str, merge: bool) {
    const N: usize = 30;
    let ord = ord_of(comparator);
    let ctx = format!("seed {seed} {comparator} merge={merge}");
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), comparator, merge);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut keyspace: Vec<Vec<u8>> = (0..N).map(key).collect();

    // Committed state, half of it flushed so the overlay merges with tables too.
    for step in 0..80 {
        let k = key(rng.gen_range(0..N));
        match rng.gen_range(0..10) {
            0..=4 => db.put(&cf, &k, format!("c{step}").as_bytes(), Duration::ZERO).unwrap(),
            5 => db.delete(&cf, &k).unwrap(),
            6 | 7 if merge => db.merge(&cf, &k, format!("c{step}").as_bytes()).unwrap(),
            8 => {
                if let Some((s, e)) = span(ord, k, key(rng.gen_range(0..N))) {
                    db.delete_range(&cf, &s, &e).unwrap();
                }
            }
            _ => db.put(&cf, &k, b"c", Duration::ZERO).unwrap(),
        }
        if step == 40 {
            db.flush_memtable(&cf).unwrap();
        }
    }
    // The newest committed write sits outside every span the transaction can
    // buffer: a span does not yet mask a committed version at exactly the
    // transaction's read sequence (see `own_span_masks_the_newest_committed_write`).
    db.put(&cf, SENTINEL, b"newest", Duration::ZERO).unwrap();
    keyspace.push(SENTINEL.to_vec());

    let mut t = db.begin();
    // Mirror of the buffer's points and spans, so a generated write never
    // falls inside one of the transaction's own spans (commit refuses that,
    // and `get` and a scan are only defined to agree without it).
    let mut points: Vec<Vec<u8>> = Vec::new();
    let mut spans: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut savepoints: Vec<(String, usize, usize)> = Vec::new();
    for step in 0..120 {
        let k = key(rng.gen_range(0..N));
        let own_span = spans.iter().any(|s| covers(ord, s, &k));
        match rng.gen_range(0..14) {
            0..=3 if !own_span => {
                t.put(&cf, &k, format!("t{step}").as_bytes(), Duration::ZERO).unwrap();
                points.push(k);
            }
            4 if !own_span => {
                t.delete(&cf, &k).unwrap();
                points.push(k);
            }
            5 if !own_span => {
                t.single_delete(&cf, &k).unwrap();
                points.push(k);
            }
            6..=8 if merge && !own_span => {
                t.merge(&cf, &k, format!("t{step}").as_bytes()).unwrap();
                points.push(k);
            }
            9 => {
                if let Some(s) = span(ord, k, key(rng.gen_range(0..N))) {
                    if !points.iter().any(|p| covers(ord, &s, p)) {
                        t.delete_range(&cf, &s.0, &s.1).unwrap();
                        spans.push(s);
                    }
                }
            }
            10 => {
                let name = format!("sp{step}");
                t.set_savepoint(&name).unwrap();
                savepoints.push((name, points.len(), spans.len()));
            }
            11 if !savepoints.is_empty() => {
                let at = rng.gen_range(0..savepoints.len());
                let (name, np, ns) = savepoints[at].clone();
                t.rollback_to_savepoint(&name).unwrap();
                points.truncate(np);
                spans.truncate(ns);
                savepoints.truncate(at + 1);
            }
            _ => {}
        }
        if step % 15 == 14 {
            assert_scan_matches_point_reads(&mut t, &cf, ord, &keyspace, &ctx);
            for _ in 0..12 {
                let lo = endpoint(&mut rng, N);
                let hi = endpoint(&mut rng, N);
                let (lo, hi) = (bound(&mut rng, &lo), bound(&mut rng, &hi));
                assert_bounded_matches(&t, &cf, ord, as_ref(&lo), as_ref(&hi), &ctx);
            }
        }
    }
    t.rollback().unwrap();
    drop(cf);
    db.close().unwrap();
}

#[test]
fn randomized_bytewise() {
    for seed in 0..8 {
        randomized(seed, "memcmp", false);
    }
}

#[test]
fn randomized_bytewise_merge() {
    for seed in 0..8 {
        randomized(seed, "memcmp", true);
    }
}

#[test]
fn randomized_reverse_comparator() {
    for seed in 0..8 {
        randomized(seed, "reverse", false);
    }
}

#[test]
fn randomized_reverse_comparator_merge() {
    for seed in 0..8 {
        randomized(seed, "reverse", true);
    }
}

fn keys(rows: &Rows) -> Vec<&[u8]> {
    rows.iter().map(|(k, _)| k.as_slice()).collect()
}

/// Range deletes straddling the lower bound, inside, ending exactly at the
/// lower bound, and starting exactly at the upper bound — under both
/// inclusive and exclusive ends.
#[test]
fn range_deletes_at_and_across_the_bounds() {
    for merge in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), "memcmp", merge);
        for k in [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i"] {
            db.put(&cf, k, b"v", Duration::ZERO).unwrap();
        }
        let mut t = db.begin();
        t.delete_range(&cf, b"b", b"d").unwrap(); // ends exactly at the lower bound
        t.delete_range(&cf, b"e", b"f").unwrap(); // inside
        t.delete_range(&cf, b"g", b"i").unwrap(); // starts exactly at the upper bound
        t.delete_range(&cf, b"0", b"1").unwrap(); // wholly outside

        let mut it = t.new_iterator_bounded(&cf, Bound::Included(b"d"), Bound::Excluded(b"g"));
        it.seek_to_first();
        assert_eq!(keys(&forward(&mut it)), [b"d", b"f"], "merge={merge}");
        let mut it = t.new_iterator_bounded(&cf, Bound::Included(b"d"), Bound::Included(b"g"));
        it.seek_to_first();
        assert_eq!(keys(&forward(&mut it)), [b"d", b"f"], "merge={merge}");
        let mut it = t.new_iterator_bounded(&cf, Bound::Excluded(b"c"), Bound::Included(b"h"));
        it.seek_to_last();
        assert_eq!(keys(&backward(&mut it)), [b"f", b"d"], "merge={merge}");

        for (lo, hi) in [
            (Bound::Included(&b"d"[..]), Bound::Excluded(&b"g"[..])),
            (Bound::Excluded(&b"c"[..]), Bound::Included(&b"g"[..])),
            (Bound::Unbounded, Bound::Excluded(&b"e"[..])),
            (Bound::Included(&b"h"[..]), Bound::Unbounded),
        ] {
            assert_bounded_matches(&t, &cf, bytewise, lo, hi, &format!("merge={merge}"));
        }
        drop(t);
        drop(cf);
        db.close().unwrap();
    }
}

/// A transaction's own range delete on a merge family masks the keys it
/// covers in a scan and in a point read — it is an interval, not a put of its
/// end key at its start key.
#[test]
fn merge_family_own_range_delete_masks() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), "memcmp", true);
    for k in [b"k1", b"k2", b"k3", b"k4", b"k5"] {
        db.put(&cf, k, b"v", Duration::ZERO).unwrap();
    }
    let mut t = db.begin();
    t.merge(&cf, b"k5", b"op").unwrap();
    t.delete_range(&cf, b"k2", b"k4").unwrap();
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    assert_eq!(
        forward(&mut it),
        vec![
            (b"k1".to_vec(), b"v".to_vec()),
            (b"k4".to_vec(), b"v".to_vec()),
            (b"k5".to_vec(), b"v|op".to_vec()),
        ]
    );
    for k in [b"k2", b"k3"] {
        assert!(
            matches!(t.get(&cf, k), Err(OndaError::NotFound)),
            "{k:?} is under the transaction's own span: {:?}",
            t.get(&cf, k)
        );
    }
    t.rollback().unwrap();
    drop(cf);
    db.close().unwrap();
}

/// A buffered `delete_range(k33, k38)` hides every committed key in
/// `[k33, k38)` and is never read as a value (`k33 => "k38"`), on a merge family
/// and a plain one, forward and backward, bounded and unbounded — while an
/// operand outside the span still folds onto its committed base.
#[test]
fn own_range_delete_is_a_span_not_a_value() {
    for merge in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), "memcmp", merge);
        for i in 30..=40 {
            db.put(&cf, &key(i), b"v", Duration::ZERO).unwrap();
        }
        let mut t = db.begin();
        if merge {
            t.merge(&cf, &key(31), b"op").unwrap();
        }
        t.delete_range(&cf, b"k33", b"k38").unwrap();
        let k31 = if merge { &b"v|op"[..] } else { &b"v"[..] };
        let want: Rows = [30, 31, 32, 38, 39, 40]
            .into_iter()
            .map(|i| (key(i), if i == 31 { k31.to_vec() } else { b"v".to_vec() }))
            .collect();
        for k in 33..38 {
            assert!(
                matches!(t.get(&cf, &key(k)), Err(OndaError::NotFound)),
                "merge={merge} get {:?}",
                key(k)
            );
        }
        let bounds: [Bounds<'_>; 4] = [
            (Bound::Unbounded, Bound::Unbounded),
            (Bound::Included(b"k31"), Bound::Included(b"k39")),
            (Bound::Excluded(b"k32"), Bound::Excluded(b"k38")),
            (Bound::Included(b"k34"), Bound::Unbounded),
        ];
        for (lo, hi) in bounds {
            let ctx = format!("merge={merge} {lo:?}..{hi:?}");
            let expected: Rows = want
                .iter()
                .filter(|(k, _)| in_bounds(bytewise, k, lo, hi))
                .cloned()
                .collect();
            let mut it = t.new_iterator_bounded(&cf, lo, hi);
            it.seek_to_first();
            assert_eq!(forward(&mut it), expected, "forward, {ctx}");
            it.seek_to_last();
            let mut back = backward(&mut it);
            back.reverse();
            assert_eq!(back, expected, "backward, {ctx}");
            assert_bounded_matches(&t, &cf, bytewise, lo, hi, &ctx);
        }
        t.rollback().unwrap();
        drop(cf);
        db.close().unwrap();
    }
}

/// A merged key inside the bounds keeps its whole chain — the committed base,
/// the buffered operands on either side of unrelated out-of-bounds writes —
/// and a buffered base re-anchors it.
#[test]
fn merge_chain_inside_bounds_is_complete() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), "memcmp", true);
    db.put(&cf, b"m", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"m", b"c1").unwrap();
    db.put(&cf, b"n", b"base", Duration::ZERO).unwrap();
    let mut t = db.begin();
    t.merge(&cf, b"m", b"t1").unwrap();
    for i in 0..100 {
        t.merge(&cf, format!("x{i:03}").as_bytes(), b"o").unwrap();
    }
    t.merge(&cf, b"m", b"t2").unwrap();
    t.merge(&cf, b"n", b"lost").unwrap();
    t.put(&cf, b"n", b"rebased", Duration::ZERO).unwrap();
    t.merge(&cf, b"n", b"t3").unwrap();
    let mut it = t.new_iterator_bounded(&cf, Bound::Included(b"m"), Bound::Included(b"n"));
    it.seek_to_first();
    assert_eq!(
        forward(&mut it),
        vec![
            (b"m".to_vec(), b"base|c1|t1|t2".to_vec()),
            (b"n".to_vec(), b"rebased|t3".to_vec()),
        ]
    );
    drop(t);
    drop(cf);
    db.close().unwrap();
}

/// Writes rolled back to a savepoint leave neither bounded nor unbounded
/// scans, whether they were inside or outside the bounds.
#[test]
fn savepoint_rollback_inside_and_outside_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), "reverse", false);
    db.put(&cf, b"c", b"committed", Duration::ZERO).unwrap();
    let mut t = db.begin();
    t.put(&cf, b"d", b"kept", Duration::ZERO).unwrap();
    t.set_savepoint("sp").unwrap();
    t.put(&cf, b"c", b"gone", Duration::ZERO).unwrap();
    t.put(&cf, b"a", b"gone", Duration::ZERO).unwrap();
    t.delete_range(&cf, b"z", b"x").unwrap();
    t.rollback_to_savepoint("sp").unwrap();
    // Reverse order: "d" sorts before "c".
    let mut it = t.new_iterator_bounded(&cf, Bound::Included(b"e"), Bound::Included(b"b"));
    it.seek_to_first();
    assert_eq!(
        forward(&mut it),
        vec![
            (b"d".to_vec(), b"kept".to_vec()),
            (b"c".to_vec(), b"committed".to_vec()),
        ]
    );
    assert_bounded_matches(&t, &cf, reverse, Bound::Included(b"e"), Bound::Excluded(b"c"), "rev");
    t.rollback().unwrap();
    drop(cf);
    db.close().unwrap();
}

/// A key written twice in one transaction reads as its last write in either
/// direction. Every overlay entry shares the read sequence, and the arena
/// memtable (`unsafe-fastpath`) keeps an equal `(key, seq)` as a second node,
/// so a backward scan used to surface the first write.
#[test]
fn rewritten_key_reads_its_last_write_backward() {
    for merge in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), "memcmp", merge);
        let mut t = db.begin();
        t.put(&cf, b"k", b"first", Duration::ZERO).unwrap();
        t.put(&cf, b"k", b"second", Duration::ZERO).unwrap();
        t.put(&cf, b"j", b"gone", Duration::ZERO).unwrap();
        t.delete(&cf, b"j").unwrap();
        for bounded in [false, true] {
            let mut it = if bounded {
                t.new_iterator_bounded(&cf, Bound::Included(b"a"), Bound::Included(b"z"))
            } else {
                t.new_iterator(&cf)
            };
            let want = vec![(b"k".to_vec(), b"second".to_vec())];
            it.seek_to_last();
            assert_eq!(backward(&mut it), want, "merge={merge} bounded={bounded}");
            it.seek_to_first();
            assert_eq!(forward(&mut it), want, "merge={merge} bounded={bounded}");
        }
        t.rollback().unwrap();
        drop(cf);
        db.close().unwrap();
    }
}

/// Known bug, not fixed here: the overlay's spans sit at the transaction's read
/// sequence `rs`, and a span masks only versions strictly below it, so the
/// newest committed write — the one *at* `rs` — survives the transaction's own
/// range delete in a scan while `Txn::get` reports it deleted. The same holds
/// before and after bounding the overlay.
#[test]
#[ignore = "known bug: an own span does not mask the committed version at the read sequence"]
fn own_span_masks_the_newest_committed_write() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), "memcmp", false);
    db.put(&cf, b"a", b"v", Duration::ZERO).unwrap();
    db.put(&cf, b"b", b"v", Duration::ZERO).unwrap();
    let mut t = db.begin();
    t.delete_range(&cf, b"a", b"c").unwrap();
    assert!(matches!(t.get(&cf, b"b"), Err(OndaError::NotFound)));
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    assert_eq!(forward(&mut it), Rows::new(), "\"b\" is under the transaction's own span");
    t.rollback().unwrap();
    drop(cf);
    db.close().unwrap();
}
