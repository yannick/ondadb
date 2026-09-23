//! A data block that fails its checksum mid-table must surface as an error
//! from every consumer that walks tables entry by entry — never as the end of
//! the table.
//!
//! An `SstIterator` that hits a bad block reports `!valid()`, which is exactly
//! what an exhausted one reports; only `err()` tells them apart. A consumer
//! that forgets to ask turns a checksum failure into a *short answer that looks
//! complete*: a scan that stops early, a merge chain that folds without its
//! older operands, and — worst — a compaction that installs a truncated output
//! and retires the inputs holding the rest of the data.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ondadb::config::Compression;
use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, Options, DB};

const KEYS: u32 = 2000;

fn cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        // Uncompressed, small blocks: a byte flipped at a third of the file
        // lands inside some data block with plenty of blocks after it.
        compression: Compression::None,
        compression_per_level: Vec::new(),
        data_block_size: 1024,
        ..ColumnFamilyConfig::default()
    }
}

fn open(dir: &Path) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = match db.get_column_family("default") {
        Some(cf) => cf,
        None => db.create_column_family("default", cfg()).unwrap(),
    };
    (db, cf)
}

fn key(i: u32) -> Vec<u8> {
    format!("key{i:06}").into_bytes()
}

fn value(i: u32, round: u32) -> Vec<u8> {
    format!("value-{round}-{i:06}-{}", "x".repeat(64)).into_bytes()
}

/// Two overlapping L0 tables: every key in the first, every tenth key
/// overwritten in the second. Returns the expected final value per key and
/// the first (older) table's klog path.
fn build(dir: &Path) -> (Vec<Vec<u8>>, PathBuf) {
    let (db, cf) = open(dir);
    for i in 0..KEYS {
        db.put(&cf, &key(i), &value(i, 0), Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    for i in (0..KEYS).step_by(10) {
        db.put(&cf, &key(i), &value(i, 1), Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let levels = cf.table_metadata();
    assert_eq!(levels[0].len(), 2, "expected two L0 tables: {levels:?}");
    // L0 is newest first.
    let older = levels[0][1].id;
    db.close().unwrap();
    let expected = (0..KEYS)
        .map(|i| value(i, if i % 10 == 0 { 1 } else { 0 }))
        .collect();
    (expected, dir.join("cf-default").join(format!("{older}.klog")))
}

/// Flip one byte a third of the way into `klog`; returns the original bytes.
fn corrupt(klog: &Path) -> Vec<u8> {
    let original = std::fs::read(klog).unwrap();
    let mut bytes = original.clone();
    let at = bytes.len() / 3;
    bytes[at] ^= 0xFF;
    std::fs::write(klog, &bytes).unwrap();
    original
}

fn table_ids(cf: &ColumnFamily) -> Vec<u64> {
    let mut ids: Vec<u64> = cf.table_metadata().iter().flatten().map(|t| t.id).collect();
    ids.sort_unstable();
    ids
}

fn assert_every_key(dir: &Path, expected: &[Vec<u8>]) {
    let (db, cf) = open(dir);
    for (i, want) in expected.iter().enumerate() {
        let got = db
            .get(&cf, &key(i as u32))
            .unwrap_or_else(|e| panic!("key {i} unreadable after restore: {e:?}"));
        assert_eq!(&got, want, "key {i}");
    }
    db.close().unwrap();
}

/// S1: a compaction whose input hits a corrupt block must fail the job, keep
/// every input table installed and on disk, and install nothing.
#[test]
fn compaction_input_corruption_fails_the_job_and_keeps_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let (expected, klog) = build(dir.path());
    let original = corrupt(&klog);

    let (db, cf) = open(dir.path());
    let before = table_ids(&cf);
    let result = db.compact(&cf);
    assert!(
        result.is_err(),
        "compaction over a corrupt input block must fail, got Ok; tables now {:?}",
        cf.table_metadata()
    );
    assert_eq!(
        table_ids(&cf),
        before,
        "a failed compaction must leave the catalog unchanged"
    );
    assert!(klog.exists(), "the corrupt input must not be retired");
    db.close().unwrap();

    // Repair the block: every key must still be there, which is only true if
    // no truncated output replaced the inputs.
    std::fs::write(&klog, &original).unwrap();
    assert_every_key(dir.path(), &expected);
}

/// A user scan over a corrupt block must end with `err()` set, not end early
/// looking complete.
#[test]
fn scan_over_corrupt_block_reports_error() {
    let dir = tempfile::tempdir().unwrap();
    let (_, klog) = build(dir.path());
    corrupt(&klog);

    let (db, cf) = open(dir.path());
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut seen = 0u32;
    while it.valid() {
        seen += 1;
        it.next();
    }
    assert!(
        it.err().is_some(),
        "scan stopped after {seen} of {KEYS} keys with no error"
    );
    assert!(seen < KEYS);

    // Backward too: the walk that crosses the bad block from above.
    let mut it = txn.new_iterator(&cf);
    it.seek_to_last();
    let mut seen = 0u32;
    while it.valid() {
        seen += 1;
        it.prev();
    }
    assert!(
        it.err().is_some(),
        "reverse scan stopped after {seen} of {KEYS} keys with no error"
    );
    drop(txn);
    db.close().unwrap();
}

/// Appends operands, `|`-separated: any operand the fold loses shows up.
#[derive(Debug)]
struct Concat;

impl MergeOperator for Concat {
    fn name(&self) -> &str {
        "test.corrupt_block.concat.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let mut out = existing.map(<[u8]>::to_vec).unwrap_or_default();
        for operand in operands {
            if !out.is_empty() {
                out.push(b'|');
            }
            out.extend_from_slice(operand);
        }
        Ok(out)
    }
}

fn open_merge(dir: &Path) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    let cf = match db.get_column_family("m") {
        Some(cf) => cf,
        None => db
            .create_column_family(
                "m",
                ColumnFamilyConfig {
                    merge_operator_name: Some(Concat.name().to_string()),
                    ..cfg()
                },
            )
            .unwrap(),
    };
    (db, cf)
}

/// A point read of a merge chain that straddles a corrupt block must fail,
/// not fold the operands it gathered before the bad block.
#[test]
fn merge_chain_over_corrupt_block_reports_error() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open_merge(dir.path());
    // One key, a chain long enough to span many 1 KiB blocks.
    for i in 0..400u32 {
        let operand = format!("operand-{i:04}-{}", "y".repeat(40));
        db.merge(&cf, b"chain", operand.as_bytes()).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let id = cf.table_metadata()[0][0].id;
    let intact = db.get(&cf, b"chain").unwrap();
    db.close().unwrap();
    corrupt(&dir.path().join("cf-m").join(format!("{id}.klog")));

    let (db, cf) = open_merge(dir.path());
    if let Ok(v) = db.get(&cf, b"chain") {
        panic!(
            "a chain with a corrupt block folded to {} of {} bytes",
            v.len(),
            intact.len()
        );
    }
    db.close().unwrap();
}
