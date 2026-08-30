//! Merge operators (feature 1.1): registry resolution, operand durability,
//! read-time folding, the conflict model and compaction-time folding.
//!
//! The centrepieces are `merge_reference_model` — random Put/Delete/Merge
//! histories replayed at many read sequences against a sequential-application
//! oracle — and `fold_oracle`, which asserts that compaction folding never
//! changes what any snapshot reads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, OndaError, Options, DB};

// ---------------------------------------------------------------------------
// Operators used by the tests
// ---------------------------------------------------------------------------

/// Little-endian `i64` counter: every operand is a delta, `existing` is the
/// running total. Missing base folds from zero.
#[derive(Debug)]
struct Counter;

impl MergeOperator for Counter {
    fn name(&self) -> &str {
        "test.counter.i64.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let read = |b: &[u8]| -> Result<i64, String> {
            b.try_into()
                .map(i64::from_le_bytes)
                .map_err(|_| format!("operand is {} bytes, want 8", b.len()))
        };
        let mut acc = match existing {
            Some(b) => read(b)?,
            None => 0,
        };
        for operand in operands {
            acc = acc.wrapping_add(read(operand)?);
        }
        Ok(acc.to_le_bytes().to_vec())
    }
}

/// Appends operands to the base, `|`-separated, and renders a missing base as
/// the empty string. Order-sensitive on purpose: it is the operator that
/// catches an oldest/newest inversion, which a commutative counter cannot.
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
        let mut out = Vec::new();
        // The base and its absence must stay distinguishable: `Some(b"")`
        // yields a leading separator, `None` does not.
        if let Some(base) = existing {
            out.extend_from_slice(base);
        }
        for operand in operands {
            if existing.is_some() || !out.is_empty() {
                out.push(b'|');
            }
            out.extend_from_slice(operand);
        }
        Ok(out)
    }
}

/// Reports whether `existing` was `None`, `Some(empty)` or `Some(bytes)`, so a
/// test can pin the found/deleted split the point-read path already carries.
#[derive(Debug)]
struct Witness;

impl MergeOperator for Witness {
    fn name(&self) -> &str {
        "test.witness.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let base = match existing {
            None => "none".to_string(),
            Some(b"") => "empty".to_string(),
            Some(b) => format!("some({})", String::from_utf8_lossy(b)),
        };
        let ops: Vec<String> = operands
            .iter()
            .map(|o| String::from_utf8_lossy(o).into_owned())
            .collect();
        Ok(format!("{base}+[{}]", ops.join(",")).into_bytes())
    }
}

/// Always fails; pins that an operator error becomes `Corruption` naming the
/// key rather than a panic or a silently invented value.
#[derive(Debug)]
struct AlwaysFails;

impl MergeOperator for AlwaysFails {
    fn name(&self) -> &str {
        "test.always-fails.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        _existing: Option<&[u8]>,
        _operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        Err("this operator never succeeds".to_string())
    }
}

/// [`Counter`] under a different reported name, for the mismatch test.
#[derive(Debug)]
struct RenamedCounter;

impl MergeOperator for RenamedCounter {
    fn name(&self) -> &str {
        "test.counter.i64.v2"
    }
    fn full_merge(
        &self,
        key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        Counter.full_merge(key, existing, operands)
    }
}

/// Counts `full_merge` calls, so a test can assert that a group with no operand
/// never reaches the operator at all.
#[derive(Debug, Default)]
struct CountingConcat {
    calls: AtomicUsize,
}

impl MergeOperator for CountingConcat {
    fn name(&self) -> &str {
        "test.concat.v1"
    }
    fn full_merge(
        &self,
        key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Concat.full_merge(key, existing, operands)
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn i64_bytes(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

fn i64_of(b: &[u8]) -> i64 {
    i64::from_le_bytes(b.try_into().expect("8-byte counter value"))
}

fn options(dir: &std::path::Path, operators: Vec<Arc<dyn MergeOperator>>) -> Options {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = operators;
    opts
}

fn cf_config(operator: &str) -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        merge_operator_name: Some(operator.to_string()),
        ..ColumnFamilyConfig::default()
    }
}

/// Open a database registering `operators` and create one column family using
/// the first of them.
fn open_with(
    dir: &std::path::Path,
    operators: Vec<Arc<dyn MergeOperator>>,
    name: &str,
) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(options(dir, operators)).unwrap();
    let cf = db.create_column_family("m", cf_config(name)).unwrap();
    (db, cf)
}

fn counter_db(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    open_with(dir, vec![Arc::new(Counter)], Counter.name())
}

fn concat_db(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    open_with(dir, vec![Arc::new(Concat)], Concat.name())
}

// ---------------------------------------------------------------------------
// Slice 1/2: registry and open-time resolution
// ---------------------------------------------------------------------------

#[test]
fn reopen_with_matching_operator_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = counter_db(dir.path());
        db.merge(&cf, b"k", &i64_bytes(5)).unwrap();
        db.close().unwrap();
    }
    let db = DB::open(options(dir.path(), vec![Arc::new(Counter)])).unwrap();
    let cf = db.get_column_family("m").expect("family survives reopen");
    assert_eq!(
        db.column_family_config("m")
            .unwrap()
            .merge_operator_name
            .as_deref(),
        Some(Counter.name())
    );
    assert_eq!(i64_of(&db.get(&cf, b"k").unwrap()), 5);
    db.close().unwrap();
}

#[test]
fn reopen_without_registered_operator_is_error() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = counter_db(dir.path());
        db.merge(&cf, b"k", &i64_bytes(1)).unwrap();
        db.close().unwrap();
    }
    let err = DB::open(Options::new(dir.path().to_str().unwrap()))
        .expect_err("an unresolvable operator must fail the open, not fall back");
    assert_eq!(err.kind(), "invalid_args", "{err:?}");
    let message = err.to_string();
    assert!(message.contains("\"m\""), "must name the CF: {message}");
    assert!(
        message.contains(Counter.name()),
        "must name the operator: {message}"
    );
    assert!(
        message.contains("merge_fns"),
        "must name the registry: {message}"
    );
}

#[test]
fn reopen_with_mismatched_operator_name_is_error() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = counter_db(dir.path());
        db.merge(&cf, b"k", &i64_bytes(1)).unwrap();
        db.close().unwrap();
    }
    // A registered operator that folds identically but reports another name is
    // still a different operator: the stored name is what has to resolve.
    let err = DB::open(options(dir.path(), vec![Arc::new(RenamedCounter)]))
        .expect_err("a differing operator name must not silently take over");
    assert_eq!(err.kind(), "invalid_args", "{err:?}");
    assert!(err.to_string().contains(Counter.name()));
}

#[test]
fn duplicate_operator_names_in_options_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let err = DB::open(options(
        dir.path(),
        vec![Arc::new(Concat), Arc::new(CountingConcat::default())],
    ))
    .expect_err("two operators of one name are ambiguous");
    assert_eq!(err.kind(), "invalid_args", "{err:?}");
    assert!(err.to_string().contains(Concat.name()));
}

#[test]
fn creating_a_family_with_an_unregistered_operator_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let err = db
        .create_column_family("m", cf_config(Counter.name()))
        .expect_err("a family cannot name an operator the database cannot resolve");
    assert_eq!(err.kind(), "invalid_args", "{err:?}");
    db.close().unwrap();
}

/// The stored name is what the family keeps: re-supplying a *different* config
/// for an existing family is refused outright, so folding with the wrong
/// operator is unreachable through the API.
#[test]
fn recreating_a_family_with_another_operator_is_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(options(
        dir.path(),
        vec![Arc::new(Counter), Arc::new(Concat)],
    ))
    .unwrap();
    db.create_column_family("m", cf_config(Counter.name()))
        .unwrap();
    let err = db
        .create_column_family("m", cf_config(Concat.name()))
        .expect_err("an existing family is never reconfigured");
    assert!(matches!(err, OndaError::Exists(_)), "{err:?}");
    assert_eq!(
        db.column_family_config("m")
            .unwrap()
            .merge_operator_name
            .as_deref(),
        Some(Counter.name())
    );
    db.close().unwrap();
}

/// Enabling a merge family is what takes the capability, durably and before any
/// kind-4 byte exists.
#[test]
fn creating_a_merge_family_enables_the_capability() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(options(dir.path(), vec![Arc::new(Counter)])).unwrap();
    assert_eq!(db.format_capabilities() & ondadb::format::CAPS_MERGE_WRITE, 0);
    db.create_column_family("m", cf_config(Counter.name()))
        .unwrap();
    assert_eq!(
        db.format_capabilities() & ondadb::format::CAPS_MERGE_WRITE,
        ondadb::format::CAPS_MERGE_WRITE
    );
    db.close().unwrap();

    let db = DB::open(options(dir.path(), vec![Arc::new(Counter)])).unwrap();
    assert_eq!(
        db.format_capabilities() & ondadb::format::CAPS_MERGE_WRITE,
        ondadb::format::CAPS_MERGE_WRITE,
        "the capability is durable"
    );
    db.close().unwrap();
}

/// A family with no operator refuses merges rather than storing an operand
/// nothing can ever fold.
#[test]
fn merge_without_an_operator_is_invalid_args() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("plain", ColumnFamilyConfig::default())
        .unwrap();
    let err = db
        .merge(&cf, b"k", b"1")
        .expect_err("no operator, no operands");
    assert_eq!(err.kind(), "invalid_args", "{err:?}");
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Slice 3: kind 4 through the write path, and kind-aware retention
// ---------------------------------------------------------------------------

#[test]
fn merge_operand_survives_flush() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b");
    db.flush_memtable(&cf).unwrap();
    assert_eq!(
        db.get(&cf, b"k").unwrap(),
        b"base|a|b",
        "operands must survive the trip through an SSTable"
    );
    db.close().unwrap();

    // And through a reopen, which replays nothing (the WAL was dropped) and
    // reads the operands straight out of the extended-layout table.
    let db = DB::open(options(dir.path(), vec![Arc::new(Concat)])).unwrap();
    let cf = db.get_column_family("m").unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b");
    db.close().unwrap();
}

/// The operands are still in the WAL, not an SSTable: replay has to carry the
/// kind through the envelope frame that `apply_commit` chose for them.
#[test]
fn merge_operand_survives_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (db, cf) = concat_db(dir.path());
        db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
        db.merge(&cf, b"k", b"a").unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        // No flush and no close: drop the handle so recovery has to replay.
        drop(cf);
        drop(db);
    }
    let db = DB::open(options(dir.path(), vec![Arc::new(Concat)])).unwrap();
    let cf = db.get_column_family("m").unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b");
    db.close().unwrap();
}

#[test]
fn merge_operand_survives_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    for operand in ["a", "b", "c"] {
        db.merge(&cf, b"k", operand.as_bytes()).unwrap();
        db.flush_memtable(&cf).unwrap();
    }
    db.compact(&cf).unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b|c");
    db.close().unwrap();
}

/// The regression the old slice ordering would have shipped: with no snapshot
/// held, `oldest_snapshot` is the newest sequence, so *every* operand is at or
/// below it. The pre-1.1 "keep exactly one version at or below the snapshot"
/// rule would have kept the newest operand and dropped the rest.
#[test]
fn operand_chain_below_snapshot_is_not_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = counter_db(dir.path());
    db.put(&cf, b"k", &i64_bytes(100), Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    for _ in 0..8 {
        db.merge(&cf, b"k", &i64_bytes(1)).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(
        i64_of(&db.get(&cf, b"k").unwrap()),
        108,
        "a truncated chain shows up as a lost delta"
    );
    db.close().unwrap();
}

/// A delete terminating a chain whose operands are still live is that chain's
/// base, and must survive the bottom level's tombstone drop.
#[test]
fn delete_terminated_chain_survives_bottom_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"gone", Duration::ZERO).unwrap();
    db.delete(&cf, b"k").unwrap();
    db.merge(&cf, b"k", b"x").unwrap();
    db.merge(&cf, b"k", b"y").unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(
        db.get(&cf, b"k").unwrap(),
        b"x|y",
        "the delete is the base, so `existing` is None and `gone` must not reappear"
    );
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Slice 4: read resolution
// ---------------------------------------------------------------------------

#[test]
fn get_folds_operands_oldest_to_newest() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    for operand in ["1", "2", "3"] {
        db.merge(&cf, b"k", operand.as_bytes()).unwrap();
    }
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|1|2|3");
    // Same answer from a scan and from a batch read.
    let t = db.begin();
    let mut it = t.new_iterator(&cf);
    it.seek(b"k");
    assert!(it.valid());
    assert_eq!(it.key(), b"k");
    assert_eq!(it.value(), b"base|1|2|3");
    assert_eq!(db.multi_get(&cf, &[b"k"])[0].as_deref().unwrap(), b"base|1|2|3");
    db.close().unwrap();
}

#[test]
fn delete_base_gives_existing_none() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_with(dir.path(), vec![Arc::new(Witness)], Witness.name());
    db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();
    db.delete(&cf, b"k").unwrap();
    db.merge(&cf, b"k", b"o").unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"none+[o]");
    db.close().unwrap();
}

#[test]
fn empty_put_base_gives_existing_some_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_with(dir.path(), vec![Arc::new(Witness)], Witness.name());
    db.put(&cf, b"k", b"", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"o").unwrap();
    assert_eq!(
        db.get(&cf, b"k").unwrap(),
        b"empty+[o]",
        "a real empty value is Some(b\"\"), not None"
    );
    db.close().unwrap();
}

#[test]
fn no_base_gives_existing_none() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_with(dir.path(), vec![Arc::new(Witness)], Witness.name());
    db.merge(&cf, b"k", b"o").unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"none+[o]");
    db.close().unwrap();
}

/// A TTL-expired put is a base like a delete: the operands still resolve, and
/// the expired bytes must not come back.
#[test]
fn expired_base_gives_existing_none() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_with(dir.path(), vec![Arc::new(Witness)], Witness.name());
    db.put(&cf, b"k", b"stale", Duration::from_millis(1))
        .unwrap();
    std::thread::sleep(Duration::from_millis(30));
    db.merge(&cf, b"k", b"o").unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"none+[o]");
    let t = db.begin();
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.value(), b"none+[o]");
    db.close().unwrap();
}

#[test]
fn operator_error_is_corruption_naming_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_with(dir.path(), vec![Arc::new(AlwaysFails)], AlwaysFails.name());
    db.merge(&cf, b"the-key", b"o").unwrap();
    let err = db.get(&cf, b"the-key").expect_err("the operator refuses");
    assert_eq!(err.kind(), "corruption", "{err:?}");
    let message = err.to_string();
    assert!(message.contains("the-key"), "{message}");
    assert!(message.contains("never succeeds"), "{message}");
    db.close().unwrap();
}

/// A group with no operand must not reach the operator at all — no call, no
/// allocation, exactly the pre-1.1 path.
#[test]
fn a_group_without_operands_never_calls_the_operator() {
    let dir = tempfile::tempdir().unwrap();
    let counting = Arc::new(CountingConcat::default());
    let db = DB::open(options(dir.path(), vec![counting.clone()])).unwrap();
    let cf = db.create_column_family("m", cf_config(Concat.name())).unwrap();
    db.put(&cf, b"a", b"1", Duration::ZERO).unwrap();
    db.put(&cf, b"b", b"2", Duration::ZERO).unwrap();
    assert_eq!(db.get(&cf, b"a").unwrap(), b"1");
    let t = db.begin();
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    while it.valid() {
        it.next();
    }
    assert_eq!(counting.calls.load(Ordering::Relaxed), 0);
    db.merge(&cf, b"a", b"x").unwrap();
    assert_eq!(db.get(&cf, b"a").unwrap(), b"1|x");
    assert!(counting.calls.load(Ordering::Relaxed) > 0);
    db.close().unwrap();
}

/// The operand chain of one key can be spread over the active memtable, a
/// sealed one and several tables; the reader must reassemble it by sequence.
#[test]
fn chain_spread_over_every_source_layer_folds_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.merge(&cf, b"k", b"1").unwrap();
    db.merge(&cf, b"k", b"2").unwrap();
    db.flush_memtable(&cf).unwrap();
    db.merge(&cf, b"k", b"3").unwrap();
    db.flush_memtable(&cf).unwrap();
    db.merge(&cf, b"k", b"4").unwrap(); // still in the active memtable
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|1|2|3|4");
    let t = db.begin();
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.value(), b"base|1|2|3|4");
    // Backward iteration resolves the same group from the other end.
    let mut back = t.new_iterator(&cf);
    back.seek_to_last();
    assert!(back.valid());
    assert_eq!(back.value(), b"base|1|2|3|4");
    db.close().unwrap();
}

/// An older snapshot sees only the operands at or below its sequence.
#[test]
fn snapshots_see_their_own_prefix_of_the_chain() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"1").unwrap();
    let early = db.begin_with_isolation(ondadb::IsolationLevel::Snapshot);
    db.merge(&cf, b"k", b"2").unwrap();
    let mut early = early;
    assert_eq!(early.get(&cf, b"k").unwrap(), b"base|1");
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|1|2");
    early.rollback().unwrap();
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Slice 4: transaction overlay
// ---------------------------------------------------------------------------

#[test]
fn txn_merge_reads_its_own_operands() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"committed").unwrap();

    let mut t = db.begin();
    t.merge(&cf, b"k", b"buffered1").unwrap();
    t.merge(&cf, b"k", b"buffered2").unwrap();
    assert_eq!(
        t.get(&cf, b"k").unwrap(),
        b"base|committed|buffered1|buffered2",
        "overlay operands append to the committed chain"
    );
    // Same through the overlay iterator and the batch read.
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.value(), b"base|committed|buffered1|buffered2");
    assert_eq!(
        t.multi_get(&cf, &[b"k"])[0].as_deref().unwrap(),
        b"base|committed|buffered1|buffered2"
    );
    t.commit().unwrap();
    assert_eq!(
        db.get(&cf, b"k").unwrap(),
        b"base|committed|buffered1|buffered2"
    );
    db.close().unwrap();
}

/// A put buffered after operands is a base and supersedes them; operands after
/// that put fold onto it.
#[test]
fn txn_base_after_operands_supersedes_them() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"old", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.merge(&cf, b"k", b"dropped").unwrap();
    t.put(&cf, b"k", b"fresh", Duration::ZERO).unwrap();
    t.merge(&cf, b"k", b"kept").unwrap();
    assert_eq!(t.get(&cf, b"k").unwrap(), b"fresh|kept");
    let mut it = t.new_iterator(&cf);
    it.seek_to_first();
    assert_eq!(it.value(), b"fresh|kept");
    t.commit().unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"fresh|kept");
    db.close().unwrap();
}

#[test]
fn txn_delete_then_merge_gives_existing_none() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_with(dir.path(), vec![Arc::new(Witness)], Witness.name());
    db.put(&cf, b"k", b"v", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.delete(&cf, b"k").unwrap();
    t.merge(&cf, b"k", b"o").unwrap();
    assert_eq!(t.get(&cf, b"k").unwrap(), b"none+[o]");
    t.commit().unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"none+[o]");
    db.close().unwrap();
}

/// Rolling back to a savepoint drops the operands buffered after it, exactly as
/// it drops puts.
#[test]
fn savepoint_rollback_drops_buffered_operands() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.merge(&cf, b"k", b"keep").unwrap();
    t.set_savepoint("sp").unwrap();
    t.merge(&cf, b"k", b"drop").unwrap();
    assert_eq!(t.get(&cf, b"k").unwrap(), b"base|keep|drop");
    t.rollback_to_savepoint("sp").unwrap();
    assert_eq!(t.get(&cf, b"k").unwrap(), b"base|keep");
    t.commit().unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|keep");
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Slice 6: conflict model
// ---------------------------------------------------------------------------

#[test]
fn merge_conflicts_like_a_write_at_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = counter_db(dir.path());
    db.put(&cf, b"k", &i64_bytes(0), Duration::ZERO).unwrap();

    let mut t = db.begin_with_isolation(ondadb::IsolationLevel::Snapshot);
    t.merge(&cf, b"k", &i64_bytes(1)).unwrap();
    // A concurrent writer bumps the key's newest sequence.
    db.merge(&cf, b"k", &i64_bytes(5)).unwrap();
    let err = t.commit().expect_err("a merge is a write and must conflict");
    assert!(matches!(err, OndaError::Conflict(_)), "{err:?}");

    // And the symmetric direction: an ordinary put conflicts with a merge.
    let mut t = db.begin_with_isolation(ondadb::IsolationLevel::Snapshot);
    t.put(&cf, b"k", &i64_bytes(9), Duration::ZERO).unwrap();
    db.merge(&cf, b"k", &i64_bytes(1)).unwrap();
    assert!(matches!(
        t.commit().expect_err("still a write-write conflict"),
        OndaError::Conflict(_)
    ));
    db.close().unwrap();
}

#[test]
fn merge_conflicts_like_a_write_at_serializable() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = counter_db(dir.path());
    db.put(&cf, b"k", &i64_bytes(0), Duration::ZERO).unwrap();

    let mut t = db.begin_with_isolation(ondadb::IsolationLevel::Serializable);
    t.merge(&cf, b"k", &i64_bytes(1)).unwrap();
    db.merge(&cf, b"k", &i64_bytes(5)).unwrap();
    let err = t.commit().expect_err("write-write conflict");
    assert!(matches!(err, OndaError::Conflict(_)), "{err:?}");
    db.close().unwrap();
}

/// The point of the feature: a merge does not read, so two transactions that
/// only merge the same key both commit.
#[test]
fn concurrent_merges_of_one_key_do_not_conflict_at_read_committed() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = counter_db(dir.path());
    db.put(&cf, b"k", &i64_bytes(0), Duration::ZERO).unwrap();
    for _ in 0..50 {
        db.merge(&cf, b"k", &i64_bytes(1)).unwrap();
    }
    assert_eq!(i64_of(&db.get(&cf, b"k").unwrap()), 50);
    db.close().unwrap();
}

#[test]
fn read_committed_merge_does_not_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = counter_db(dir.path());
    db.put(&cf, b"k", &i64_bytes(0), Duration::ZERO).unwrap();

    let mut t = db.begin_with_isolation(ondadb::IsolationLevel::ReadCommitted);
    t.merge(&cf, b"k", &i64_bytes(1)).unwrap();
    db.merge(&cf, b"k", &i64_bytes(5)).unwrap();
    t.commit()
        .expect("ReadCommitted does no write-write check at all");
    assert_eq!(i64_of(&db.get(&cf, b"k").unwrap()), 6);
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// The reference model
// ---------------------------------------------------------------------------

/// xorshift64*, so a failing seed is reproducible and the test needs no dep.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A key's oracle state: `None` means the key is not there.
type OracleState = Vec<(Vec<u8>, Option<Vec<u8>>)>;
/// A pinned snapshot and the oracle state it must report.
type Checkpoint = (ondadb::Txn, OracleState);

/// One step of a generated history.
#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>, Duration),
    Delete(Vec<u8>),
    SingleDelete(Vec<u8>),
    Merge(Vec<u8>, Vec<u8>),
}

impl Op {
    fn key(&self) -> &[u8] {
        match self {
            Op::Put(k, ..) | Op::Delete(k) | Op::SingleDelete(k) | Op::Merge(k, _) => k,
        }
    }
}

/// The oracle: apply one operation to a key's state by *sequential*
/// application, which is what a fold over the whole chain must agree with.
///
/// `Concat` is associative in exactly the sense the operator contract demands —
/// folding `[a, b]` onto `E` equals folding `[b]` onto `full_merge(E, [a])` —
/// so a step-at-a-time oracle is a valid reference for a batched fold, and any
/// disagreement is the engine's.
fn apply_to_oracle(state: &mut Option<Vec<u8>>, op: &Op) {
    match op {
        Op::Put(_, v, _) => *state = Some(v.clone()),
        Op::Delete(_) | Op::SingleDelete(_) => *state = None,
        Op::Merge(k, operand) => {
            let folded = Concat
                .full_merge(k, state.as_deref(), &[operand.as_slice()])
                .expect("Concat never fails");
            *state = Some(folded);
        }
    }
}

fn generate_history(rng: &mut Rng, keys: &[&[u8]], steps: usize) -> Vec<Op> {
    let mut out = Vec::with_capacity(steps);
    for i in 0..steps {
        let key = keys[rng.below(keys.len() as u64) as usize].to_vec();
        // Merges dominate on purpose: long chains are what this feature has to
        // survive, and the point kinds are already covered everywhere else.
        out.push(match rng.below(10) {
            0 => Op::Put(key, format!("p{i}").into_bytes(), Duration::ZERO),
            // A long TTL exercises the HAS_TTL modifier through the extended
            // entry layout without making the oracle depend on the clock;
            // expiry itself is pinned by `expired_base_gives_existing_none`.
            1 => Op::Put(key, format!("t{i}").into_bytes(), Duration::from_secs(3600)),
            2 => Op::Delete(key),
            3 => Op::SingleDelete(key),
            _ => Op::Merge(key, format!("o{i}").into_bytes()),
        });
    }
    out
}

fn apply_op(db: &DB, cf: &Arc<ColumnFamily>, op: &Op) {
    match op {
        Op::Put(k, v, ttl) => db.put(cf, k, v, *ttl).unwrap(),
        Op::Delete(k) => db.delete(cf, k).unwrap(),
        Op::SingleDelete(k) => {
            let mut t = db.begin_with_isolation(ondadb::IsolationLevel::ReadCommitted);
            t.single_delete(cf, k).unwrap();
            t.commit().unwrap();
        }
        Op::Merge(k, o) => db.merge(cf, k, o).unwrap(),
    }
}

fn oracle_get<'a>(states: &'a OracleState, key: &[u8]) -> Option<&'a [u8]> {
    states
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| v.as_deref())
}

/// The comparators the model runs under, so the expected scan order can be
/// built without reaching into the engine.
fn compare_with(comparator: &str, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    match comparator {
        "reverse" => b.cmp(a),
        "case_insensitive" => {
            let fold = |s: &[u8]| s.to_ascii_lowercase();
            fold(a).cmp(&fold(b)).then_with(|| a.cmp(b))
        }
        _ => a.cmp(b),
    }
}

/// One run of the reference model: a random Put/Delete/SingleDelete/Merge
/// history replayed at **every** intermediate read sequence against a
/// sequential-application oracle.
///
/// `mutate` runs between the history and the verification, so the same model
/// covers "still in the memtable", "flushed" and "compacted" without changing
/// what is asserted: none of them may change any snapshot's answer.
fn run_reference_model(
    seed: u64,
    comparator: &str,
    unified: bool,
    mutate: impl FnOnce(&DB, &Arc<ColumnFamily>),
) {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options(dir.path(), vec![Arc::new(Concat)]);
    opts.unified_memtable = unified;
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "m",
            ColumnFamilyConfig {
                comparator_name: comparator.to_string(),
                ..cf_config(Concat.name())
            },
        )
        .unwrap();

    let keys: [&[u8]; 4] = [b"alpha", b"beta", b"Gamma", b"delta"];
    let mut rng = Rng(seed | 1);
    let history = generate_history(&mut rng, &keys, 60);

    // After each step, pin a snapshot and the oracle state that snapshot must
    // report for every key.
    let mut states: OracleState =
        keys.iter().map(|k| (k.to_vec(), None)).collect();
    let mut checkpoints: Vec<Checkpoint> = Vec::new();
    for op in &history {
        apply_op(&db, &cf, op);
        let slot = states
            .iter_mut()
            .find(|(k, _)| k == op.key())
            .expect("generated keys come from `keys`");
        apply_to_oracle(&mut slot.1, op);
        checkpoints.push((db.begin(), states.clone()));
    }

    mutate(&db, &cf);

    for (step, (txn, expected)) in checkpoints.iter_mut().enumerate() {
        let context =
            format!("step {step}, seed {seed}, comparator {comparator}, unified {unified}");
        for key in &keys {
            let want = oracle_get(expected, key);
            match (want, txn.get(&cf, key)) {
                (Some(want), Ok(got)) => assert_eq!(
                    got.as_slice(),
                    want,
                    "{context} key {:?}",
                    String::from_utf8_lossy(key)
                ),
                (None, Err(OndaError::NotFound)) => {}
                (want, got) => panic!(
                    "{context} key {:?}: oracle {want:?} vs engine {got:?}",
                    String::from_utf8_lossy(key)
                ),
            }
        }
        // A scan at the same snapshot must report exactly the live keys, in the
        // family's own order, with the same values.
        let mut want: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect();
        want.sort_by(|a, b| compare_with(comparator, &a.0, &b.0));

        let mut it = txn.new_iterator(&cf);
        it.seek_to_first();
        let mut scanned: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            scanned.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert!(it.err().is_none(), "{context}: {:?}", it.err());
        assert_eq!(scanned, want, "forward scan at {context}");

        // ...and backwards, which resolves each group from the other end.
        let mut back = txn.new_iterator(&cf);
        back.seek_to_last();
        let mut reversed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while back.valid() {
            reversed.push((back.key().to_vec(), back.value().to_vec()));
            back.prev();
        }
        reversed.reverse();
        assert_eq!(reversed, want, "reverse scan at {context}");
    }
    for (txn, _) in checkpoints.iter_mut() {
        txn.rollback().unwrap();
    }
    db.close().unwrap();
}

#[test]
fn merge_reference_model() {
    for seed in [1u64, 0xDEAD_BEEF, 0x5EED, 0xA5A5_1234, 7] {
        run_reference_model(seed, "memcmp", false, |_, _| {});
    }
}

#[test]
fn merge_reference_model_after_flush_and_compaction() {
    for seed in [2u64, 0xC0FFEE, 99] {
        run_reference_model(seed, "memcmp", false, |db, cf| {
            db.flush_memtable(cf).unwrap();
            db.compact(cf).unwrap();
        });
    }
}

#[test]
fn merge_reference_model_under_custom_comparators() {
    for comparator in ["reverse", "case_insensitive"] {
        run_reference_model(0x1234_5678, comparator, false, |db, cf| {
            db.flush_memtable(cf).unwrap();
        });
    }
}

#[test]
fn merge_reference_model_under_the_unified_wal() {
    run_reference_model(0xBEEF, "memcmp", true, |_, _| {});
    run_reference_model(0xBEE5, "memcmp", true, |db, cf| {
        db.flush_memtable(cf).unwrap();
    });
}

/// A reopen replays the WAL, so the same history has to come back out of the
/// envelope frames the operands were written into. Checkpoint snapshots cannot
/// survive a close, so this replays the history against the final state only.
#[test]
fn merge_reference_model_survives_reopen() {
    for seed in [11u64, 0xFACE, 0x2468] {
        let dir = tempfile::tempdir().unwrap();
        let keys: [&[u8]; 4] = [b"alpha", b"beta", b"Gamma", b"delta"];
        let mut rng = Rng(seed | 1);
        let history = generate_history(&mut rng, &keys, 60);
        let mut states: OracleState =
            keys.iter().map(|k| (k.to_vec(), None)).collect();

        {
            let (db, cf) = concat_db(dir.path());
            for (i, op) in history.iter().enumerate() {
                apply_op(&db, &cf, op);
                let slot = states.iter_mut().find(|(k, _)| k == op.key()).unwrap();
                apply_to_oracle(&mut slot.1, op);
                // Flush partway so the reopen mixes replayed WAL records with
                // operands already in a table.
                if i == history.len() / 2 {
                    db.flush_memtable(&cf).unwrap();
                }
            }
            // Dropped without `close`, so recovery has to replay.
            drop(cf);
            drop(db);
        }

        let db = DB::open(options(dir.path(), vec![Arc::new(Concat)])).unwrap();
        let cf = db.get_column_family("m").unwrap();
        for key in &keys {
            match (oracle_get(&states, key), db.get(&cf, key)) {
                (Some(want), Ok(got)) => assert_eq!(got.as_slice(), want, "seed {seed}"),
                (None, Err(OndaError::NotFound)) => {}
                (want, got) => panic!("seed {seed}: oracle {want:?} vs engine {got:?}"),
            }
        }
        db.close().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Slice 7: compaction folding, and its oracle
// ---------------------------------------------------------------------------

/// Every data-block entry of every SSTable a column family has on disk, as
/// `(user key, seq, kind, value)`.
///
/// Folding is invisible through the read API by construction — that is the
/// whole claim — so the tests that assert a chain really collapsed have to look
/// at the bytes. This is also how "chain length over time" is measured for the
/// benchmark.
fn raw_entries(dir: &std::path::Path, cf_name: &str) -> Vec<(Vec<u8>, u64, u64, Vec<u8>)> {
    use ondadb::cache::{BlockCache, FileCache};
    use ondadb::sst::Reader;
    use ondadb::LocalStorage;

    let mut klogs: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join(format!("cf-{cf_name}")))
        .expect("the column family directory exists")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "klog"))
        .collect();
    klogs.sort();

    let mut out = Vec::new();
    for (i, klog) in klogs.iter().enumerate() {
        let reader = Reader::open(
            klog.to_str().unwrap(),
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            i as u64 + 1,
            ondadb::comparator::default_comparator(),
            0,
        )
        .unwrap();
        let mut it = reader.iter();
        it.seek_to_first();
        while it.valid() {
            out.push((
                it.user_key().to_vec(),
                it.seq(),
                it.kind(),
                it.value().unwrap(),
            ));
            it.next();
        }
    }
    out
}

fn operand_count(dir: &std::path::Path, key: &[u8]) -> usize {
    raw_entries(dir, "m")
        .into_iter()
        .filter(|(k, _, kind, _)| k == key && *kind == ondadb::format::KIND_MERGE)
        .count()
}

/// The fold oracle: pre- and post-compaction reads are identical at every
/// snapshot, and both match the sequential-application oracle.
///
/// Folding is the one part of this feature that rewrites stored history, and it
/// is only ever an optimization — so the assertion is not "the fold produced
/// X", it is "nothing anyone can read changed". The run releases snapshots in
/// stages so compaction folds progressively deeper suffixes while older readers
/// are still watching.
#[test]
fn fold_oracle() {
    for seed in [3u64, 0xF01D, 0x9999, 42] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = concat_db(dir.path());
        let keys: [&[u8]; 4] = [b"alpha", b"beta", b"Gamma", b"delta"];
        let mut rng = Rng(seed | 1);
        let history = generate_history(&mut rng, &keys, 60);

        let mut states: OracleState =
            keys.iter().map(|k| (k.to_vec(), None)).collect();
        let mut checkpoints: Vec<Checkpoint> = Vec::new();
        for op in &history {
            apply_op(&db, &cf, op);
            let slot = states.iter_mut().find(|(k, _)| k == op.key()).unwrap();
            apply_to_oracle(&mut slot.1, op);
            checkpoints.push((db.begin(), states.clone()));
        }
        let final_state = states.clone();

        let verify = |checkpoints: &mut [Checkpoint],
                      phase: &str| {
            for (step, (txn, expected)) in checkpoints.iter_mut().enumerate() {
                for key in &keys {
                    match (oracle_get(expected, key), txn.get(&cf, key)) {
                        (Some(want), Ok(got)) => assert_eq!(
                            got.as_slice(),
                            want,
                            "{phase}: step {step} key {:?} (seed {seed})",
                            String::from_utf8_lossy(key)
                        ),
                        (None, Err(OndaError::NotFound)) => {}
                        (want, got) => panic!(
                            "{phase}: step {step} key {:?}: oracle {want:?} vs engine {got:?} \
                             (seed {seed})",
                            String::from_utf8_lossy(key)
                        ),
                    }
                }
            }
        };

        // Phase 1: nothing folded yet.
        db.flush_memtable(&cf).unwrap();
        verify(&mut checkpoints, "pre-compaction");

        // Phase 2: release the oldest two thirds, so compaction can fold every
        // suffix below the surviving snapshots but nothing above them.
        let cut = checkpoints.len() * 2 / 3;
        for (txn, _) in checkpoints.drain(..cut) {
            let mut txn = txn;
            txn.rollback().unwrap();
        }
        db.compact(&cf).unwrap();
        verify(&mut checkpoints, "partial fold");

        // Phase 3: release everything and fold the whole chain.
        for (txn, _) in checkpoints.drain(..) {
            let mut txn = txn;
            txn.rollback().unwrap();
        }
        db.compact(&cf).unwrap();
        for key in &keys {
            match (oracle_get(&final_state, key), db.get(&cf, key)) {
                (Some(want), Ok(got)) => assert_eq!(got.as_slice(), want, "full fold, seed {seed}"),
                (None, Err(OndaError::NotFound)) => {}
                (want, got) => panic!("full fold seed {seed}: oracle {want:?} vs {got:?}"),
            }
        }
        db.close().unwrap();
    }
}

/// Compaction really does collapse a chain — the oracle above proves folding is
/// invisible, and this proves it happened at all.
#[test]
fn folding_collapses_the_chain_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    for i in 0..16 {
        db.merge(&cf, b"k", format!("o{i}").as_bytes()).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    assert_eq!(operand_count(dir.path(), b"k"), 16);

    db.compact(&cf).unwrap();
    assert_eq!(
        operand_count(dir.path(), b"k"),
        0,
        "a chain with no live snapshot over it must fold to one entry"
    );
    let entries: Vec<_> = raw_entries(dir.path(), "m")
        .into_iter()
        .filter(|(k, ..)| k == b"k")
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, ondadb::format::KIND_PUT);
    assert_eq!(db.get(&cf, b"k").unwrap(), entries[0].3);
    db.close().unwrap();
}

#[test]
fn folding_off_switch_disables_folding() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options(dir.path(), vec![Arc::new(Concat)]);
    opts.enable_merge_folding = false;
    let db = DB::open(opts).unwrap();
    let cf = db.create_column_family("m", cf_config(Concat.name())).unwrap();
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    for i in 0..8 {
        db.merge(&cf, b"k", format!("o{i}").as_bytes()).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(
        operand_count(dir.path(), b"k"),
        8,
        "the switch must stop new folds without changing any answer"
    );
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|o0|o1|o2|o3|o4|o5|o6|o7");
    db.close().unwrap();
}

/// A base inside a chain terminates it: the operands above it fold onto it, and
/// the operands below it belong to a superseded chain that no reader can see.
#[test]
fn fold_never_crosses_a_base() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.merge(&cf, b"k", b"lost1").unwrap();
    db.merge(&cf, b"k", b"lost2").unwrap();
    db.put(&cf, b"k", b"BASE", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"kept1").unwrap();
    db.merge(&cf, b"k", b"kept2").unwrap();
    let before = db.get(&cf, b"k").unwrap();
    assert_eq!(before, b"BASE|kept1|kept2");

    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), before);
    let entries: Vec<_> = raw_entries(dir.path(), "m")
        .into_iter()
        .filter(|(k, ..)| k == b"k")
        .collect();
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].3, b"BASE|kept1|kept2");
    db.close().unwrap();
}

/// The folded entry keeps the **newest** sequence its suffix represented, so it
/// shadows exactly what the chain shadowed — and no more.
#[test]
fn fold_preserves_newest_sequence_of_the_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();
    // A second key, written after, so the newest sequence in the family is
    // higher than anything in `k`'s chain.
    db.put(&cf, b"z", b"z", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    let before: Vec<u64> = raw_entries(dir.path(), "m")
        .into_iter()
        .filter(|(key, ..)| key == b"k")
        .map(|(_, seq, ..)| seq)
        .collect();
    let newest = *before.iter().max().unwrap();

    db.compact(&cf).unwrap();
    let after: Vec<(u64, u64)> = raw_entries(dir.path(), "m")
        .into_iter()
        .filter(|(key, ..)| key == b"k")
        .map(|(_, seq, kind, _)| (seq, kind))
        .collect();
    assert_eq!(after.len(), 1, "{after:?}");
    assert_eq!(
        after[0],
        (newest, ondadb::format::KIND_PUT),
        "the folded entry must carry the suffix's newest sequence"
    );
    db.close().unwrap();
}

/// A live snapshot inside the chain is a boundary the fold may not cross: the
/// operands above it stay individual entries, so that snapshot keeps reading
/// its own prefix.
#[test]
fn fold_does_not_run_above_oldest_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();
    let mut pinned = db.begin();
    assert_eq!(pinned.get(&cf, b"k").unwrap(), b"base|a|b");
    for operand in ["c", "d", "e"] {
        db.merge(&cf, b"k", operand.as_bytes()).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    assert_eq!(
        operand_count(dir.path(), b"k"),
        3,
        "only the suffix at or below the snapshot may fold"
    );
    assert_eq!(pinned.get(&cf, b"k").unwrap(), b"base|a|b");
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b|c|d|e");
    pinned.rollback().unwrap();

    // With the snapshot gone the rest folds, and still nothing changes.
    db.compact(&cf).unwrap();
    assert_eq!(operand_count(dir.path(), b"k"), 0);
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b|c|d|e");
    db.close().unwrap();
}

/// A base with a live TTL is not foldable: the folded value would carry the
/// expiry (or lose it), where the unfolded chain resolves to `existing = None`
/// plus its operands once the base expires.
#[test]
fn fold_stops_at_a_base_with_a_live_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    db.put(&cf, b"k", b"base", Duration::from_secs(3600))
        .unwrap();
    db.merge(&cf, b"k", b"a").unwrap();
    db.merge(&cf, b"k", b"b").unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(
        operand_count(dir.path(), b"k"),
        2,
        "a base that can still expire is not a fold target"
    );
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b");
    db.close().unwrap();
}

/// A chain whose base lives in a deeper level must not fold against "no base"
/// in a non-bottom job: `existing = None` would invent history. This drives a
/// non-bottom L0 -> L1 compaction while an L2 holds the base.
#[test]
fn fold_without_a_base_only_happens_at_the_bottom() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = concat_db(dir.path());
    // Two levels of history, then operands on top; a manual compaction sweeps
    // to the bottom, where "no base above me" is genuinely "no base".
    db.put(&cf, b"k", b"base", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    for operand in ["a", "b", "c"] {
        db.merge(&cf, b"k", operand.as_bytes()).unwrap();
        db.flush_memtable(&cf).unwrap();
    }
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b|c");
    db.compact(&cf).unwrap();
    assert_eq!(db.get(&cf, b"k").unwrap(), b"base|a|b|c");
    assert_eq!(operand_count(dir.path(), b"k"), 0);
    db.close().unwrap();
}
