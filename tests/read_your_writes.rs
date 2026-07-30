//! Read-your-own-writes under concurrent commits from other threads.
//! Found by the marekvs chaos harness: INCR (get+put on one thread) lost
//! 84/4000 increments while another thread wrote unrelated keys.

use ondadb::{ColumnFamilyConfig, Options, DB};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[test]
fn get_sees_own_put_under_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("d", ColumnFamilyConfig::default())
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let noise = {
        let db = db.clone();
        let cf = cf.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let k = format!("noise-{i}");
                db.put(&cf, k.as_bytes(), b"x", std::time::Duration::ZERO)
                    .unwrap();
                i += 1;
            }
        })
    };

    let mut lost = 0usize;
    for i in 0..50_000u64 {
        let v = i.to_be_bytes();
        db.put(&cf, b"rmw-key", &v, std::time::Duration::ZERO)
            .unwrap();
        match db.get(&cf, b"rmw-key") {
            Ok(read) if read == v => {}
            Ok(read) => {
                lost += 1;
                if lost <= 3 {
                    eprintln!(
                        "iteration {i}: wrote {:?}, read back {:?}",
                        u64::from_be_bytes(v),
                        u64::from_be_bytes(read.as_slice().try_into().unwrap())
                    );
                }
            }
            Err(e) => panic!("get failed: {e:?}"),
        }
    }
    stop.store(true, Ordering::Relaxed);
    noise.join().unwrap();
    assert_eq!(
        lost, 0,
        "{lost} of 50000 reads missed the write that preceded them on the same thread"
    );
}

/// The same property under enough contention to actually expose its failure.
///
/// `get_sees_own_put_under_concurrent_writes` above is the regression test for
/// the lost-update bug (de50da9). It also, intermittently, catches a SECOND and
/// different failure that only appears with `--features unsafe-fastpath`: the
/// `get` returns `Err(NotFound)` for a key this thread just wrote successfully
/// — not a stale value, an absent one.
///
/// It was first seen at roughly 2/48 with eight concurrent copies of the test
/// binary, and never when that test ran alone (0/8 isolated, 0/24 under
/// synthetic CPU load) — it needs real contention, not merely a busy CPU.
/// v0.5.0 reproduces it too, so it is not a 0.6.0 regression. See
/// `docs/concurrency-and-safety.md` for the analysis and the two unverified
/// leads (`ArenaShard` publication ordering; `THREAD_COMMIT_FLOOR` keyed by
/// `DbInner` address).
///
/// Driving eight independent databases at once reproduces it inside a single
/// process and much more often: **about one run in three or four**, versus
/// 2-in-48 per binary copy. Measured control — six consecutive runs pass under
/// default features while the same build fails every few runs under
/// `unsafe-fastpath`, which is the evidence that this is feature-specific
/// rather than general flakiness.
///
/// It is `#[ignore]`d deliberately: one-in-four is still a flaky gate, and a
/// suite that goes red at random teaches people to rerun rather than read. Run
/// it explicitly while working the bug — it reports every violation it saw, so
/// a single run gives more than one data point:
///
/// ```sh
/// cargo test --features unsafe-fastpath --test read_your_writes -- --ignored --nocapture
/// ```
#[test]
#[ignore = "contention stress for the unsafe-fastpath NotFound defect; too rare to gate CI"]
fn get_sees_own_put_under_heavy_multi_db_contention() {
    const DBS: usize = 8;
    const ITERS: u64 = 20_000;

    let failures = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut workers = Vec::new();
    for db_index in 0..DBS {
        let failures = failures.clone();
        workers.push(std::thread::spawn(move || {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
            let cf = db
                .create_column_family("d", ColumnFamilyConfig::default())
                .unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let noise = {
                let db = db.clone();
                let cf = cf.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut i = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let k = format!("noise-{i}");
                        db.put(&cf, k.as_bytes(), b"x", std::time::Duration::ZERO)
                            .unwrap();
                        i += 1;
                    }
                })
            };
            for i in 0..ITERS {
                let v = i.to_be_bytes();
                db.put(&cf, b"rmw-key", &v, std::time::Duration::ZERO)
                    .unwrap();
                match db.get(&cf, b"rmw-key") {
                    Ok(read) if read == v => {}
                    // The original lost-update shape: a stale but present value.
                    Ok(read) => failures.lock().unwrap().push(format!(
                        "db {db_index} iteration {i}: wrote {}, read back {}",
                        i,
                        u64::from_be_bytes(read.as_slice().try_into().unwrap())
                    )),
                    // The unsafe-fastpath shape this test exists for.
                    Err(e) => failures
                        .lock()
                        .unwrap()
                        .push(format!("db {db_index} iteration {i}: get failed: {e:?}")),
                }
            }
            stop.store(true, Ordering::Relaxed);
            noise.join().unwrap();
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    let failures = failures.lock().unwrap();
    assert!(
        failures.is_empty(),
        "{} read-your-own-writes violations across {DBS} databases; first few:\n{}",
        failures.len(),
        failures.iter().take(5).cloned().collect::<Vec<_>>().join("\n")
    );
}
