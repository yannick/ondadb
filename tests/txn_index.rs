//! The transaction's buffered-write index (0.10.1) against the buffer scan it
//! replaces and against a plain model of read-your-own-writes.
//!
//! Every seeded script runs three times — index always (`min = 0`), never
//! (`min = usize::MAX`) and at the default threshold, which builds the index
//! partway through the script — and every read of every run must equal the
//! model. The scripts mix puts, deletes, single deletes, merge operands, range
//! deletes, savepoint set/rollback/release and `get`/`get_into`/`multi_get`
//! over three families: a plain one, a merge family, and a
//! `case_insensitive` one, where the buffer matches key **bytes** while range
//! spans use the comparator.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ondadb::{
    ColumnFamily, ColumnFamilyConfig, IsolationLevel, MergeOperator, OndaError, Options, Txn, DB,
};
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
        for o in operands {
            out.push(b'+');
            out.extend_from_slice(o);
        }
        Ok(out)
    }
}

/// Family indices into `Fixture::cfs`.
const PLAIN: usize = 0;
const MERGE: usize = 1;
const NOCASE: usize = 2;

struct Fixture {
    _dir: tempfile::TempDir,
    db: DB,
    cfs: Vec<Arc<ColumnFamily>>,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
        .unwrap();
    let plain = db
        .create_column_family("plain", ColumnFamilyConfig::default())
        .unwrap();
    let merge = db
        .create_column_family(
            "merge",
            ColumnFamilyConfig {
                merge_operator_name: Some(Concat.name().to_string()),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let nocase = db
        .create_column_family(
            "nocase",
            ColumnFamilyConfig {
                comparator_name: "case_insensitive".into(),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    Fixture {
        _dir: dir,
        db,
        cfs: vec![plain, merge, nocase],
    }
}

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    SingleDelete(Vec<u8>),
    Merge(Vec<u8>, Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
    SetSavepoint(String),
    Rollback(String),
    Release(String),
    Get(Vec<u8>),
    GetInto(Vec<u8>),
    MultiGet(Vec<Vec<u8>>),
}

/// A key from a small space so writes collide into chains; on the
/// case-insensitive family half of them are upper-cased, so byte-distinct keys
/// the comparator calls equal are common.
fn key(rng: &mut StdRng, cf: usize) -> Vec<u8> {
    let k = format!("k{:02}", rng.gen_range(0..40)).into_bytes();
    if cf == NOCASE && rng.gen_bool(0.5) {
        k.to_ascii_uppercase()
    } else {
        k
    }
}

fn script(seed: u64, len: usize) -> Vec<(usize, Op)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut ops = Vec::with_capacity(len);
    let mut savepoints = 0usize;
    for n in 0..len {
        let cf = rng.gen_range(0..3);
        let value = format!("v{n}").into_bytes();
        let op = match rng.gen_range(0..100) {
            0..=29 => Op::Put(key(&mut rng, cf), value),
            30..=35 => Op::Delete(key(&mut rng, cf)),
            36..=38 => Op::SingleDelete(key(&mut rng, cf)),
            39..=53 if cf == MERGE => Op::Merge(key(&mut rng, cf), value),
            39..=53 => Op::Put(key(&mut rng, cf), value),
            54..=55 => {
                let a = rng.gen_range(0..40);
                let b = (a + rng.gen_range(1..8)).min(40);
                Op::Range(
                    format!("k{a:02}").into_bytes(),
                    format!("k{b:02}").into_bytes(),
                )
            }
            56..=58 => {
                savepoints += 1;
                Op::SetSavepoint(format!("sp{}", rng.gen_range(0..savepoints)))
            }
            59..=60 if savepoints > 0 => {
                Op::Rollback(format!("sp{}", rng.gen_range(0..savepoints)))
            }
            61 if savepoints > 0 => Op::Release(format!("sp{}", rng.gen_range(0..savepoints))),
            62..=84 => Op::Get(key(&mut rng, cf)),
            85..=92 => Op::GetInto(key(&mut rng, cf)),
            _ => Op::MultiGet(
                (0..rng.gen_range(1..6))
                    .map(|_| key(&mut rng, cf))
                    .collect(),
            ),
        };
        ops.push((cf, op));
    }
    ops
}

/// What the store held when the transaction began: every even key in every
/// family, lower case.
fn seed_store(f: &Fixture) -> HashMap<(usize, Vec<u8>), Vec<u8>> {
    let mut store = HashMap::new();
    for (cf_ix, cf) in f.cfs.iter().enumerate() {
        for i in (0..40).step_by(2) {
            let k = format!("k{i:02}").into_bytes();
            let v = format!("store{cf_ix}-{i}").into_bytes();
            f.db.put(cf, &k, &v, Duration::ZERO).unwrap();
            store.insert((cf_ix, k), v);
        }
    }
    store
}

/// The model: the transaction's buffer as a log, read the way `Txn` documents.
struct Model {
    store: HashMap<(usize, Vec<u8>), Vec<u8>>,
    log: Vec<(usize, Op)>,
    savepoints: Vec<(String, usize)>,
}

fn norm(cf: usize, key: &[u8]) -> Vec<u8> {
    if cf == NOCASE {
        key.to_ascii_lowercase()
    } else {
        key.to_vec()
    }
}

impl Model {
    /// The store's answer, by key bytes. (A point read of the store in a
    /// `case_insensitive` family that has not flushed matches bytes too —
    /// `DB::get` of `K10` misses a stored `k10` — which is the store's
    /// behaviour, not the buffer's, and outside what this test pins.)
    fn store_get(&self, cf: usize, key: &[u8]) -> Option<Vec<u8>> {
        self.store.get(&(cf, key.to_vec())).cloned()
    }

    fn read(&self, cf: usize, key: &[u8]) -> Option<Vec<u8>> {
        // The key's point writes, by byte equality, oldest first.
        let chain: Vec<&Op> = self
            .log
            .iter()
            .filter(|(c, op)| {
                *c == cf
                    && match op {
                        Op::Put(k, _) | Op::Delete(k) | Op::SingleDelete(k) | Op::Merge(k, _) => {
                            k == key
                        }
                        _ => false,
                    }
            })
            .map(|(_, op)| op)
            .collect();
        if let Some(last_base) = chain.iter().rposition(|op| !matches!(op, Op::Merge(..))) {
            let operands: Vec<&[u8]> = chain[last_base + 1..]
                .iter()
                .map(|op| match op {
                    Op::Merge(_, v) => v.as_slice(),
                    _ => unreachable!(),
                })
                .collect();
            let base = match chain[last_base] {
                Op::Put(_, v) => Some(v.clone()),
                _ => None,
            };
            if operands.is_empty() {
                return base;
            }
            return Some(Concat.full_merge(key, base.as_deref(), &operands).unwrap());
        }
        if !chain.is_empty() {
            // Operands only: they fold onto the store's value.
            let operands: Vec<&[u8]> = chain
                .iter()
                .map(|op| match op {
                    Op::Merge(_, v) => v.as_slice(),
                    _ => unreachable!(),
                })
                .collect();
            let committed = self.store_get(cf, key);
            return Some(
                Concat
                    .full_merge(key, committed.as_deref(), &operands)
                    .unwrap(),
            );
        }
        // No point write: the transaction's own spans, under the comparator.
        let k = norm(cf, key);
        let covered = self.log.iter().any(|(c, op)| match op {
            Op::Range(s, e) => *c == cf && norm(cf, s) <= k && norm(cf, e) > k,
            _ => false,
        });
        if covered {
            None
        } else {
            self.store_get(cf, key)
        }
    }
}

fn found(r: ondadb::Result<Vec<u8>>) -> Option<Vec<u8>> {
    match r {
        Ok(v) => Some(v),
        Err(OndaError::NotFound) => None,
        Err(e) => panic!("unexpected read error {e:?}"),
    }
}

/// Run `ops` through a fresh transaction with the index threshold pinned to
/// `min`, checking every read against the model. The transaction rolls back,
/// so one seeded fixture serves every run.
fn run(
    f: &Fixture,
    store: &HashMap<(usize, Vec<u8>), Vec<u8>>,
    seed: u64,
    ops: &[(usize, Op)],
    min: usize,
) {
    let mut model = Model {
        store: store.clone(),
        log: Vec::new(),
        savepoints: Vec::new(),
    };
    let mut t: Txn = f.db.begin();
    t.set_write_index_min_for_tests(min);
    for (step, (cf_ix, op)) in ops.iter().enumerate() {
        let cf = &f.cfs[*cf_ix];
        let ctx = || format!("seed {seed} min {min} step {step}: {op:?} on cf {cf_ix}");
        match op {
            Op::Put(k, v) => t.put(cf, k, v, Duration::ZERO).unwrap(),
            Op::Delete(k) => t.delete(cf, k).unwrap(),
            Op::SingleDelete(k) => t.single_delete(cf, k).unwrap(),
            Op::Merge(k, v) => t.merge(cf, k, v).unwrap(),
            Op::Range(s, e) => t.delete_range(cf, s, e).unwrap(),
            Op::SetSavepoint(name) => {
                t.set_savepoint(name).unwrap();
                model.savepoints.push((name.clone(), model.log.len()));
            }
            Op::Rollback(name) => {
                let pos = model.savepoints.iter().rposition(|(n, _)| n == name);
                let r = t.rollback_to_savepoint(name);
                match pos {
                    Some(pos) => {
                        r.unwrap();
                        model.log.truncate(model.savepoints[pos].1);
                        model.savepoints.truncate(pos + 1);
                    }
                    None => assert!(r.is_err(), "{}", ctx()),
                }
            }
            Op::Release(name) => {
                let pos = model.savepoints.iter().rposition(|(n, _)| n == name);
                let r = t.release_savepoint(name);
                match pos {
                    Some(pos) => {
                        r.unwrap();
                        model.savepoints.truncate(pos);
                    }
                    None => assert!(r.is_err(), "{}", ctx()),
                }
            }
            Op::Get(k) => assert_eq!(found(t.get(cf, k)), model.read(*cf_ix, k), "{}", ctx()),
            Op::GetInto(k) => {
                let mut out = b"prefix".to_vec();
                let got = match t.get_into(cf, k, &mut out) {
                    Ok(n) => {
                        assert_eq!(n, out.len() - 6, "{}", ctx());
                        Some(out[6..].to_vec())
                    }
                    Err(OndaError::NotFound) => {
                        assert_eq!(out, b"prefix", "{}", ctx());
                        None
                    }
                    Err(e) => panic!("{}: {e:?}", ctx()),
                };
                assert_eq!(got, model.read(*cf_ix, k), "{}", ctx());
            }
            Op::MultiGet(keys) => {
                let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
                let got: Vec<Option<Vec<u8>>> =
                    t.multi_get(cf, &refs).into_iter().map(found).collect();
                let want: Vec<Option<Vec<u8>>> =
                    keys.iter().map(|k| model.read(*cf_ix, k)).collect();
                assert_eq!(got, want, "{}", ctx());
            }
        }
        if matches!(
            op,
            Op::Put(..) | Op::Delete(_) | Op::SingleDelete(_) | Op::Merge(..) | Op::Range(..)
        ) {
            model.log.push((*cf_ix, op.clone()));
        }
    }
    t.rollback().unwrap();
}

#[test]
fn indexed_and_scanned_reads_match_the_model() {
    let f = fixture();
    let store = seed_store(&f);
    for seed in 0..60u64 {
        let ops = script(seed, 400);
        for min in [0, usize::MAX, 32] {
            run(&f, &store, seed, &ops, min);
        }
    }
}

/// One long script: the index is built early and then maintained across
/// hundreds of appends and several rollbacks, which is where a stale chain
/// head would surface.
#[test]
fn long_script_matches_the_model() {
    let f = fixture();
    let store = seed_store(&f);
    let ops = script(0xfeed, 5_000);
    for min in [0, usize::MAX, 32] {
        run(&f, &store, 0xfeed, &ops, min);
    }
}

/// Serializable read tracking is unchanged by the index: a read answered from
/// the buffer records nothing, a read that reaches the store records the key —
/// so another writer's commit to the latter (and only the latter) aborts us.
#[test]
fn serializable_read_set_is_unchanged_by_the_index() {
    for min in [0, usize::MAX] {
        for touch_store_read in [false, true] {
            let f = fixture();
            let cf = &f.cfs[PLAIN];
            f.db.put(cf, b"other", b"0", Duration::ZERO).unwrap();
            let mut t = f.db.begin_with_isolation(IsolationLevel::Serializable);
            t.set_write_index_min_for_tests(min);
            for i in 0..100 {
                t.put(cf, format!("own{i:03}").as_bytes(), b"x", Duration::ZERO)
                    .unwrap();
            }
            assert_eq!(t.get(cf, b"own050").unwrap(), b"x");
            assert_eq!(t.get(cf, b"other").unwrap(), b"0");
            let theirs = if touch_store_read {
                b"other".as_slice()
            } else {
                b"unread".as_slice()
            };
            f.db.put(cf, theirs, b"1", Duration::ZERO).unwrap();
            let r = t.commit();
            if touch_store_read {
                assert!(matches!(r, Err(OndaError::Conflict(_))), "min {min}: {r:?}");
            } else {
                r.unwrap();
            }
        }
    }
}

/// A buffered range delete is an interval, not a write to its start bound.
/// Before 0.10.1 a merge family's buffered-chain scan matched a range delete
/// whose start equalled the key and read its *end bound* back as the key's
/// value; the index never filed range deletes, so the bug lived only in the
/// scan — which is why both paths are checked.
#[test]
fn range_delete_start_is_not_a_buffered_write_on_a_merge_family() {
    for min in [0, usize::MAX] {
        let f = fixture();
        let cf = &f.cfs[MERGE];
        f.db.put(cf, b"k33", b"stored", Duration::ZERO).unwrap();
        f.db.put(cf, b"k34", b"stored", Duration::ZERO).unwrap();
        let mut t = f.db.begin();
        t.set_write_index_min_for_tests(min);
        t.delete_range(cf, b"k33", b"k38").unwrap();
        assert!(
            matches!(t.get(cf, b"k33"), Err(OndaError::NotFound)),
            "min {min}"
        );
        assert!(
            matches!(t.get(cf, b"k34"), Err(OndaError::NotFound)),
            "min {min}"
        );
        let mut out = Vec::new();
        assert!(matches!(
            t.get_into(cf, b"k33", &mut out),
            Err(OndaError::NotFound)
        ));
        let got = t.multi_get(cf, &[b"k33".as_slice()]);
        assert!(matches!(got[0], Err(OndaError::NotFound)), "min {min}");
        // An operand after the span still sees its buffered chain alone.
        t.merge(cf, b"k40", b"x").unwrap();
        assert_eq!(t.get(cf, b"k40").unwrap(), b"+x");
    }
}
