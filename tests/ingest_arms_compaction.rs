//! Bulk ingest must arm compaction, exactly as a memtable flush does.
//!
//! It did not, and the consequence was not subtle. A bulk-loaded store
//! accumulated **14,051 L0 SSTables for a million documents** because nothing
//! ever asked the compactor to look: `Ingestion::finish` installed its tables
//! and persisted the manifest, and the `compact_tx` send that the flush path
//! performs was simply absent.
//!
//! Every one of those tables is opened at startup with its block index and
//! bloom filter resident, which is how the consumer reached 12 GB before it had
//! finished opening. So this is a memory bug and a read-amplification bug that
//! presented as neither.
//!
//! The assertion is on **L0 file count settling**, not on a timer expiring:
//! compaction is asynchronous, so the test waits for the observable outcome with
//! a deadline and reports the count it actually saw.

use std::time::{Duration, Instant};

use ondadb::{ColumnFamilyConfig, Options, DB};

/// Ingest `batches` tables of `per` keys each, one `Ingestion` per table, and
/// return the L0 file count once it stops changing (or the deadline expires).
fn ingest_and_settle(dir: &std::path::Path, batches: usize, per: usize) -> usize {
    let db = DB::open(Options {
        path: dir.to_string_lossy().into_owned(),
        ..Options::default()
    })
    .expect("open");
    let cf = db
        .create_column_family(
            "bulk",
            ColumnFamilyConfig {
                // Compact as soon as a couple of files exist, so the test does
                // not depend on the default trigger's exact value.
                l1_file_count_trigger: 2,
                ..ColumnFamilyConfig::default()
            },
        )
        .expect("create cf");

    for b in 0..batches {
        let mut ing = db.start_ingestion(&cf).expect("start ingestion");
        for i in 0..per {
            // Disjoint, ascending key ranges per batch: ingestion requires
            // strictly ascending keys, and non-overlapping tables are the case
            // most likely to be left alone by a compactor, so this is the
            // conservative fixture.
            let key = format!("{:04}/{:08}", b, i);
            ing.write(key.as_bytes(), b"v", Duration::ZERO)
                .expect("write");
        }
        ing.finish().expect("finish");
    }

    // Wait for L0 to settle rather than sleeping a fixed amount.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = usize::MAX;
    let mut stable = 0;
    while Instant::now() < deadline {
        let n = cf.l0_file_count();
        if n == last {
            stable += 1;
            if stable >= 5 {
                break;
            }
        } else {
            stable = 0;
            last = n;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let final_count = cf.l0_file_count();
    db.close().expect("close");
    final_count
}

/// Twelve bulk-ingested tables must not still be twelve L0 files.
///
/// **Fails without the `compact_tx` send in `Ingestion::finish`**: every table
/// stays in L0 forever, and the count equals the number of batches.
#[test]
fn bulk_ingest_does_not_leave_every_table_in_l0() {
    let dir = tempfile::tempdir().unwrap();
    const BATCHES: usize = 12;

    let l0 = ingest_and_settle(dir.path(), BATCHES, 200);

    assert!(
        l0 < BATCHES,
        "after {BATCHES} bulk-ingested tables, L0 still holds {l0} files — \
         compaction was never armed, so a bulk-loaded store grows one L0 table \
         per ingestion without bound (this is how a real store reached 14,051)"
    );
}

/// The data must survive being compacted, which is the half of this that a
/// file-count assertion cannot see.
#[test]
fn every_key_survives_the_compaction_that_ingest_now_triggers() {
    let dir = tempfile::tempdir().unwrap();
    const BATCHES: usize = 8;
    const PER: usize = 150;

    ingest_and_settle(dir.path(), BATCHES, PER);

    // Reopen, so the read goes through whatever compaction actually left on
    // disk rather than through anything still cached in the writing process.
    let db = DB::open(Options {
        path: dir.path().to_string_lossy().into_owned(),
        ..Options::default()
    })
    .expect("reopen");
    let cf = db
        .get_column_family("bulk")
        .expect("cf present after reopen");
    let mut txn = db.begin();
    for b in 0..BATCHES {
        for i in 0..PER {
            let key = format!("{:04}/{:08}", b, i);
            let got = txn
                .get(&cf, key.as_bytes())
                .unwrap_or_else(|e| panic!("key {key} missing after compaction: {e}"));
            assert_eq!(got, b"v", "key {key} has the wrong value after compaction");
        }
    }
    txn.rollback().expect("rollback");
    db.close().expect("close");
}

/// A single ingestion below the trigger must NOT be compacted away eagerly —
/// the send is conditional, and a test that only checked "L0 shrinks" would
/// pass just as well if the condition had been dropped.
#[test]
fn a_single_ingestion_below_the_trigger_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options {
        path: dir.path().to_string_lossy().into_owned(),
        ..Options::default()
    })
    .expect("open");
    let cf = db
        .create_column_family(
            "one",
            ColumnFamilyConfig {
                l1_file_count_trigger: 8,
                ..ColumnFamilyConfig::default()
            },
        )
        .expect("create cf");

    let mut ing = db.start_ingestion(&cf).expect("start");
    for i in 0..100 {
        ing.write(format!("k{i:06}").as_bytes(), b"v", Duration::ZERO)
            .expect("write");
    }
    ing.finish().expect("finish");

    std::thread::sleep(Duration::from_millis(500));
    let l0 = cf.l0_file_count();
    assert_eq!(
        l0, 1,
        "one ingestion under a trigger of 8 should stay in L0; found {l0} files, \
         so the trigger condition is not being honoured"
    );
    db.close().expect("close");
}

/// 0.6-A: bulk ingest writes L0 tables on the *caller's* thread, so a
/// spawn-time-only IO class would leave the whole load unclassified.
#[test]
fn ingest_finish_is_charged_as_flush() {
    use ondadb::ioctrl::{IoClass, IoLimiter, RecordingLimiter};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(RecordingLimiter::default());
    let limiter: Arc<dyn IoLimiter> = recorder.clone();
    let db = DB::open(Options {
        path: dir.path().to_string_lossy().into_owned(),
        io_limiter: Some(limiter),
        ..Options::default()
    })
    .expect("open");
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .expect("cf");
    recorder.clear();

    let mut ingestion = db.start_ingestion(&cf).expect("start");
    let value = vec![b'v'; 256];
    for i in 0..5000u32 {
        ingestion
            .write(format!("k{i:07}").as_bytes(), &value, Duration::ZERO)
            .expect("write");
    }
    let written = ingestion.finish().expect("finish");
    assert_eq!(written, 5000);

    // Ingest output is L0, exactly like a flush, and is classified as such.
    assert!(
        recorder.bytes_for(IoClass::Flush) > 0,
        "ingest must charge under Flush"
    );
    assert_eq!(
        recorder.count_for(IoClass::Foreground),
        0,
        "no ingest byte may be charged as foreground"
    );
    db.close().expect("close");
}
