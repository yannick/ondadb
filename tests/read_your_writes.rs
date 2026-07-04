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
                db.put(&cf, k.as_bytes(), b"x", std::time::Duration::ZERO).unwrap();
                i += 1;
            }
        })
    };

    let mut lost = 0usize;
    for i in 0..50_000u64 {
        let v = i.to_be_bytes();
        db.put(&cf, b"rmw-key", &v, std::time::Duration::ZERO).unwrap();
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
    assert_eq!(lost, 0, "{lost} of 50000 reads missed the write that preceded them on the same thread");
}
