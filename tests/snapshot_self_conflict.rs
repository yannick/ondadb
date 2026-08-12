//! A fixed-snapshot transaction never conflicts with ITS OWN thread's
//! earlier, strictly-serial commit.
//!
//! Found live (spada S-158): a raft store rewrites one hot HardState key in
//! serial Snapshot-isolation transactions on a single thread. `visible_seq`
//! advances gap-free, so while a slower commit on ANOTHER thread was still
//! in flight, the rewriter's own completed commit sat above the watermark;
//! its next `begin` pinned a snapshot BELOW its own write, and the
//! write-write conflict check then refused against that write — a false
//! conflict with itself, surfacing as a fail-stop ~40 minutes into real
//! ingest. `begin` now waits out the transient publication gap.

use ondadb::{ColumnFamilyConfig, IsolationLevel, Options, DB};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[test]
fn serial_snapshot_rewrites_survive_a_slow_concurrent_committer() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("d", ColumnFamilyConfig::default())
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    // The slow lane mirrors the production writer exactly: ReadCommitted
    // auto-commit puts (`DB::put`) — the isolation level that skips the
    // commit mutex, so its reserve→WAL→publish window runs CONCURRENT with
    // the rewriter's commits and holds the gap-free cursor down. In spada
    // this is the sealer thread's base-descriptor markers and the CAS
    // worker's bundle digests.
    let slow = {
        let db = db.clone();
        let cf = cf.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let big = vec![0x5Au8; 256 * 1024];
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let k = format!("slow-{i}");
                db.put(&cf, k.as_bytes(), &big, std::time::Duration::ZERO)
                    .unwrap();
                i += 1;
            }
        })
    };

    // The rewriter: strictly serial Snapshot transactions on THIS thread,
    // rewriting one hot key — the raft HardState shape. Every commit is
    // ordered after the previous by program order, so a conflict here can
    // only be a false conflict with our own write.
    let mut conflicts = 0u64;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut rounds = 0u64;
    while std::time::Instant::now() < deadline {
        let mut txn = db.begin_with_isolation(IsolationLevel::Snapshot);
        txn.put(
            &cf,
            b"hardstate",
            format!("hs-{rounds}").as_bytes(),
            std::time::Duration::ZERO,
        )
        .unwrap();
        if let Err(e) = txn.commit() {
            let msg = e.to_string();
            assert!(msg.contains("conflict"), "unexpected error: {msg}");
            conflicts += 1;
        }
        rounds += 1;
    }
    stop.store(true, Ordering::Relaxed);
    slow.join().unwrap();

    assert!(rounds > 100, "the rewriter must actually have cycled");
    assert_eq!(
        conflicts, 0,
        "a serial single-thread rewriter false-conflicted with itself \
         {conflicts} times in {rounds} rounds"
    );
}

/// Read-your-own-INGEST under concurrent commits: a fixed snapshot begun
/// right after `Ingestion::finish` must see every ingested row, even while
/// another thread's slow auto-commit holds the gap-free watermark down.
/// (The 0.7.4 fix covered `Txn::commit` via the thread floor; the ingestion
/// path never noted its floor, and spada's seal read-back verification came
/// up short — reported as segment corruption — until it did.)
#[test]
fn a_fixed_snapshot_after_finish_sees_the_ingested_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("d", ColumnFamilyConfig::default())
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    // Two noise lanes with LARGE values: each auto-commit's reserve→publish
    // window spans its whole WAL write, maximizing the chance one straddles
    // an entire ingestion (reserve at start_ingestion → begin after finish).
    let slows: Vec<_> = (0..2)
        .map(|t| {
            let db = db.clone();
            let cf = cf.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let big = vec![0x5Au8; 4 * 1024 * 1024];
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let k = format!("noise-{t}-{i}");
                    db.put(&cf, k.as_bytes(), &big, std::time::Duration::ZERO)
                        .unwrap();
                    i += 1;
                }
            })
        })
        .collect();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
    let mut rounds = 0u64;
    let mut short_reads = 0u64;
    while std::time::Instant::now() < deadline {
        let prefix = format!("seg-{rounds}/");
        let mut ing = db.start_ingestion(&cf).unwrap();
        for j in 0..8 {
            ing.write(
                format!("{prefix}{j:04}").as_bytes(),
                b"lane-entry",
                std::time::Duration::ZERO,
            )
            .unwrap();
        }
        ing.finish().unwrap();

        // The read-back a seal verification does: fixed snapshot, bounded
        // scan over the prefix.
        let txn = db.begin();
        let mut seen = 0u64;
        {
            let start = prefix.clone().into_bytes();
            let mut end = start.clone();
            *end.last_mut().unwrap() += 1;
            let mut it = txn.new_iterator_bounded(
                &cf,
                std::ops::Bound::Included(start.as_slice()),
                std::ops::Bound::Excluded(end.as_slice()),
            );
            it.seek(&start);
            while it.valid() {
                seen += 1;
                it.next();
            }
            assert!(it.err().is_none(), "scan errored: {:?}", it.err());
        }
        if seen != 8 {
            short_reads += 1;
        }
        rounds += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for s in slows {
        s.join().unwrap();
    }

    assert!(rounds > 20, "the loop must actually have cycled ({rounds})");
    assert_eq!(
        short_reads, 0,
        "{short_reads} of {rounds} read-backs missed their own ingested rows"
    );
}
