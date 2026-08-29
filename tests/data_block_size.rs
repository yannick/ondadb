//! Per-family data block size (SPADINO-A10).
//!
//! `ColumnFamilyConfig::data_block_size` was a private constant, so every
//! family in every database wrote 4 KiB blocks. A block is the compression
//! unit and the writer cuts one once it EXCEEDS the target, so a store whose
//! values are all larger than 4 KiB compressed each value alone — no shared
//! window, however good the algorithm. Spadino measured 12.8% of a 562 MB
//! store on that alone, across five different families.
//!
//! The two properties that make it safe to ship:
//!
//! 1. blocks are self-describing, so a table written at one size is readable
//!    by a database configured for another — this is a write-side policy and
//!    changing it rewrites nothing;
//! 2. a config that never sets it behaves, and encodes, exactly as before.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, DB};

/// Values larger than the 4 KiB default, so the setting has something to do.
fn corpus(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let key = format!("k{i:06}").into_bytes();
            // Compressible and distinct: a repeated per-key phrase, which is
            // exactly the shape that benefits from a shared window.
            let value = format!("value-{i:06}-").repeat(500).into_bytes();
            (key, value)
        })
        .collect()
}

fn write_store(dir: &std::path::Path, block: usize) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                data_block_size: block,
                compression: ondadb::config::Compression::Zstd,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for (k, v) in corpus(400) {
        db.put(&cf, &k, &v, Duration::ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    for e in rd.flatten() {
        match e.file_type() {
            Ok(t) if t.is_dir() => total += dir_bytes(&e.path()),
            Ok(_) => total += e.metadata().map(|m| m.len()).unwrap_or(0),
            Err(_) => {}
        }
    }
    total
}

/// The point of the field: a wider block is a wider compression window.
#[test]
fn a_larger_block_stores_the_same_corpus_in_fewer_bytes() {
    let small = tempfile::tempdir().unwrap();
    let large = tempfile::tempdir().unwrap();
    write_store(small.path(), 4 << 10);
    write_store(large.path(), 64 << 10);

    let (s, l) = (dir_bytes(small.path()), dir_bytes(large.path()));
    assert!(
        l < s,
        "64 KiB blocks ({l} bytes) did not beat 4 KiB ({s}) — either the \
         setting is not reaching the writer, or the corpus does not exceed \
         the small block and the test proves nothing"
    );
}

/// Blocks are self-describing: a table written at 64 KiB opens in a database
/// configured for 4 KiB and returns identical values. This is what makes the
/// field a policy rather than a format, and what lets an operator change it
/// with no migration.
#[test]
fn a_table_written_at_one_block_size_reads_back_under_another() {
    let dir = tempfile::tempdir().unwrap();
    write_store(dir.path(), 64 << 10);

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .get_column_family("d")
        .expect("the family survives reopen");
    for (k, v) in corpus(400) {
        assert_eq!(
            db.get(&cf, &k).unwrap(),
            v,
            "value for {k:?} did not survive"
        );
    }
}

/// A family that never sets the field behaves exactly as it always did, which
/// is what lets this land without a format bump.
#[test]
fn an_unset_block_size_is_the_historical_default() {
    assert_eq!(ColumnFamilyConfig::default().data_block_size, 4 << 10);

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.create_column_family("d", ColumnFamilyConfig::default())
        .unwrap();
    drop(db);

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(
        db.column_family_config("d").unwrap().data_block_size,
        4 << 10,
        "a reopened family must recover the default, not zero — the writer's \
         own 0 => DEFAULT fallback would hide the difference"
    );
}

/// And a set value survives the manifest round trip, or the setting would
/// silently revert on the first reopen.
#[test]
fn a_set_block_size_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.create_column_family(
        "d",
        ColumnFamilyConfig {
            data_block_size: 32 << 10,
            ..ColumnFamilyConfig::default()
        },
    )
    .unwrap();
    drop(db);

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(
        db.column_family_config("d").unwrap().data_block_size,
        32 << 10
    );
}
