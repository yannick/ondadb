//! A bloom filter must keep filtering after compaction.
//!
//! It did not. `sst::writer::Writer::new` sized the filter from
//! `WriterOptions::expected_entries` and `Bloom::new` allocates a **fixed** bit
//! array that never grows — while `compaction.rs::cf_writer_opts` passed a
//! hardcoded `expected_entries: 4096` for every table compaction produced. A
//! compacted table holding a million entries therefore got a filter designed
//! for four thousand, saturated every bit, and answered "maybe present" to
//! every query.
//!
//! That is worse than having no filter: the bytes are still built, written,
//! loaded into memory and hashed against on every lookup, and nothing is ever
//! skipped. And because a leveled LSM keeps almost all of its data in
//! compacted levels, it meant blooms were effectively off for the whole
//! steady-state database. Measured on a real consumer store (spada, 33.5M
//! entries in one column family): 400,000 point reads for keys that provably
//! did not exist produced **zero** bloom skips.
//!
//! The fix removes the guess rather than improving it: the writer buffers each
//! key's hash and builds the filter in `finish()` from the count it actually
//! wrote, so no caller can size a filter wrongly.

use std::time::Duration;

use ondadb::config::{ColumnFamilyConfig, Options};
use ondadb::DB;

/// Entries the shape a search index writes: a small key and a small value.
/// The bug is about entry COUNT versus the writer's guess, and small entries
/// are where a byte-based guess goes furthest wrong.
const N: usize = 400_000;
const MISSES: usize = 50_000;

/// Only EVEN counters are written.
fn key(i: usize) -> Vec<u8> {
    let mut k = vec![0xAA, 0xBB, 0xCC, 0x00, 0x01];
    k.extend_from_slice(&(i as u64).to_be_bytes());
    k
}

/// Absent, but strictly INSIDE the written key range.
///
/// This matters: a key past the table's `max_key` is rejected by the handle's
/// range check before the filter is consulted at all, so a test using
/// out-of-range keys measures the range check and passes no matter how broken
/// the filter is.
fn absent_key(i: usize) -> Vec<u8> {
    key(2 * i + 1)
}

/// `(candidate tables considered, tables actually probed, bloom skips)` over
/// `MISSES` lookups of keys that do not exist.
fn miss_stats(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>) -> (u64, u64, u64) {
    let before = cf.stats();
    let mut txn = db.begin();
    for i in 0..MISSES {
        assert!(
            txn.get(cf, &absent_key(i)).is_err(),
            "an absent key was found; the corpus is wrong, not the filter"
        );
    }
    txn.rollback().expect("rollback");
    let after = cf.stats();
    let probes = after.sst_probes - before.sst_probes;
    let skips = after.bloom_skips - before.bloom_skips;
    (probes + skips, probes, skips)
}

fn fill(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>) {
    let value = vec![0x5A; 8];
    let mut ing = db.start_ingestion(cf).expect("start_ingestion");
    for i in 0..N {
        ing.write(&key(2 * i), &value, Duration::ZERO)
            .expect("write");
    }
    ing.finish().expect("finish");
}

/// Every key must still be readable — a filter that rejects everything would
/// "skip" perfectly and lose all the data, so the skip rate below is only
/// meaningful next to this.
fn assert_all_readable(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>) {
    let mut txn = db.begin();
    for i in (0..N).step_by(N / 500) {
        assert!(
            txn.get(cf, &key(2 * i)).is_ok(),
            "key {i} went missing; a skip rate over lost data means nothing"
        );
    }
    txn.rollback().expect("rollback");
}

#[test]
fn the_filter_still_skips_after_compaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = DB::open(Options::new(&*dir.path().to_string_lossy())).expect("open");
    let cfg = ColumnFamilyConfig::default();
    assert!(
        cfg.enable_bloom_filter,
        "this test measures the filter; it must be enabled by default"
    );

    let cf = db.create_column_family("data", cfg).expect("cf");
    fill(&db, &cf);

    // Before compaction the filter works — this is the non-vacuity guard. If
    // this ever drops, the test is failing for some reason other than the one
    // it exists to catch.
    let (cand0, _, skips0) = miss_stats(&db, &cf);
    assert!(
        cand0 > 0,
        "no table was even considered; the corpus is wrong"
    );
    let before_rate = skips0 as f64 / cand0 as f64;
    assert!(
        before_rate > 0.9,
        "an L0 table's filter already fails to skip ({:.1} %) — this test can \
         no longer tell you anything about compaction",
        100.0 * before_rate
    );

    db.compact(&cf).expect("compact");
    assert_all_readable(&db, &cf);

    let (cand, probes, skips) = miss_stats(&db, &cf);
    assert!(cand > 0, "no table considered after compaction");
    let rate = skips as f64 / cand as f64;
    assert!(
        rate > 0.9,
        "after compaction the filter skips {:.1} % of {cand} candidate tables \
         ({probes} probed, {skips} skipped) — a filter sized by a guess rather \
         than by the entries actually written saturates and admits everything, \
         which costs memory and a hash per lookup and buys nothing",
        100.0 * rate
    );

    db.close().expect("close");
}

/// The same property stated where it belongs: the writer must not depend on
/// its caller guessing the entry count correctly.
#[test]
fn a_wrong_expected_entries_hint_cannot_break_the_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = DB::open(Options::new(&*dir.path().to_string_lossy())).expect("open");
    let cf = db
        .create_column_family("data", ColumnFamilyConfig::default())
        .expect("cf");

    // `start_ingestion` derives its hint from a fixed bytes-per-entry guess
    // (`roll_bytes / 64`), so entries far smaller than 64 bytes overshoot the
    // real count by a wide margin — the same class of error compaction made,
    // just less extreme.
    fill(&db, &cf);
    db.compact(&cf).expect("compact");

    let (cand, probes, skips) = miss_stats(&db, &cf);
    let rate = skips as f64 / cand.max(1) as f64;
    assert!(
        rate > 0.9,
        "filter skips only {:.1} % ({probes} probed / {skips} skipped of {cand}) \
         — sizing must come from the entries written, not from a hint",
        100.0 * rate
    );
    db.close().expect("close");
}
