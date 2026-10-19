//! The user-space WAL write buffer (`Options::wal_write_buffer_size`, P6):
//! what an acknowledged commit may lose and what it may not.
//!
//! Crashes are real process deaths: a child (`crash_helper`) writes and exits
//! without `close` or `Drop`, because `Drop` closes the WAL and a close flushes
//! the buffer — an in-process "crash" would prove nothing about it.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, SyncMode, DB};

const ZERO: Duration = Duration::ZERO;
const DIR_ENV: &str = "ONDA_WALBUF_CRASH_DIR";
const MODE_ENV: &str = "ONDA_WALBUF_CRASH_MODE";
/// Long enough that the background flusher never fires inside a test.
const NO_TICK: Duration = Duration::from_secs(3600);

fn per_cf_opts(path: &str, buffer: usize) -> Options {
    Options {
        wal_write_buffer_size: buffer,
        ..Options::new(path)
    }
}

fn unified_opts(path: &str, buffer: usize) -> Options {
    Options {
        unified_memtable: true,
        unified_memtable_sync_mode: SyncMode::None,
        unified_memtable_sync_interval: NO_TICK,
        wal_write_buffer_size: buffer,
        ..Options::new(path)
    }
}

fn cfg(mode: SyncMode) -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        sync_mode: mode,
        sync_interval: NO_TICK,
        // No rotation mid-test: a flush makes everything before it durable
        // and would hide what the buffer lost.
        write_buffer_size: 64 << 20,
        ..ColumnFamilyConfig::default()
    }
}

fn cf(db: &DB, name: &str, mode: SyncMode) -> Arc<ColumnFamily> {
    match db.get_column_family(name) {
        Some(cf) => cf,
        None => db.create_column_family(name, cfg(mode)).unwrap(),
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Not a real test: the child half of the crash simulation. A no-op unless
/// the env vars are set.
#[test]
fn crash_helper() {
    let (Ok(dir), Ok(mode)) = (std::env::var(DIR_ENV), std::env::var(MODE_ENV)) else {
        return;
    };
    match mode.as_str() {
        // 1000 synced commits, then 1000 acknowledged but still buffered.
        "per_cf_sync_point" | "per_cf_interval_sync_point" => {
            let sync = if mode == "per_cf_sync_point" {
                SyncMode::None
            } else {
                SyncMode::Interval
            };
            let db = DB::open(per_cf_opts(&dir, 1 << 20)).unwrap();
            let c = cf(&db, "c", sync);
            for i in 0..1000 {
                db.put(&c, &key(i), b"v", ZERO).unwrap();
            }
            db.sync_wal().unwrap();
            for i in 1000..2000 {
                db.put(&c, &key(i), b"v", ZERO).unwrap();
            }
            crash(db);
        }
        // A small buffer that fills many times: the tail of the last flush
        // and whatever was buffered after it are lost, whole batches only.
        "unified_pairs" => {
            let db = DB::open(unified_opts(&dir, 4 << 10)).unwrap();
            let c = cf(&db, "c", SyncMode::None);
            for i in 0..3000 {
                let mut t = db.begin();
                t.put(&c, format!("a{i:06}").as_bytes(), b"va", ZERO)
                    .unwrap();
                t.put(&c, format!("b{i:06}").as_bytes(), b"vb", ZERO)
                    .unwrap();
                t.commit().unwrap();
            }
            crash(db);
        }
        // A prepare is forced durable whatever the buffer says.
        "unified_prepare" => {
            let db = DB::open(unified_opts(&dir, 1 << 20)).unwrap();
            db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
                .unwrap();
            let c = cf(&db, "c", SyncMode::None);
            db.put(&c, b"buffered", b"v", ZERO).unwrap();
            let mut t = db.begin();
            t.put(&c, b"prepared", b"pv", ZERO).unwrap();
            let _p = t.prepare(&[5; 16]).unwrap();
            crash(db);
        }
        other => panic!("unknown crash mode {other}"),
    }
}

/// Die with `db` still open: no `close`, and — because `exit` never returns —
/// no `Drop` either, which would close the WAL and flush its buffer.
fn crash(db: DB) -> ! {
    let _db = db;
    std::process::exit(0)
}

fn run_crash(dir: &std::path::Path, mode: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_helper", "--exact", "--nocapture"])
        .env(DIR_ENV, dir.to_str().unwrap())
        .env(MODE_ENV, mode)
        .status()
        .expect("spawn crash helper");
    assert!(status.success(), "crash helper child failed");
}

#[test]
fn buffered_wal_survives_clean_close_per_cf_and_unified() {
    for unified in [false, true] {
        for mode in [SyncMode::None, SyncMode::Interval, SyncMode::Full] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            let opts = |p: &str| {
                if unified {
                    Options {
                        unified_memtable_sync_mode: mode,
                        ..unified_opts(p, 64 << 10)
                    }
                } else {
                    per_cf_opts(p, 64 << 10)
                }
            };
            {
                let db = DB::open(opts(path)).unwrap();
                let c = cf(&db, "c", mode);
                for i in 0..500 {
                    db.put(&c, &key(i), b"v", ZERO).unwrap();
                }
                db.close().unwrap();
            }
            let db = DB::open(opts(path)).unwrap();
            let c = cf(&db, "c", mode);
            for i in 0..500 {
                assert_eq!(
                    db.get(&c, &key(i)).unwrap(),
                    b"v",
                    "unified={unified} mode={mode:?} key {i}"
                );
            }
        }
    }
}

/// `sync_wal` is the durability point it has always been: with a buffer it
/// flushes before it fsyncs. What follows it is lost with the process — the
/// documented trade — and recovery is a prefix, never a hole.
#[test]
fn crash_keeps_everything_before_sync_wal() {
    for mode in ["per_cf_sync_point", "per_cf_interval_sync_point"] {
        let dir = tempfile::tempdir().unwrap();
        run_crash(dir.path(), mode);
        let db = DB::open(per_cf_opts(dir.path().to_str().unwrap(), 1 << 20)).unwrap();
        let c = cf(&db, "c", SyncMode::None);
        let present: Vec<bool> = (0..2000).map(|i| db.get(&c, &key(i)).is_ok()).collect();
        assert!(
            present[..1000].iter().all(|&p| p),
            "{mode}: a synced commit was lost"
        );
        let n = present.iter().take_while(|&&p| p).count();
        assert!(
            present[n..].iter().all(|&p| !p),
            "{mode}: recovery has a hole"
        );
        // 1000 tiny frames never fill a 1 MiB buffer and the tick never fires:
        // the unsynced tail was still buffered, so it is gone. If this starts
        // failing the buffer is not buffering.
        assert_eq!(n, 1000, "{mode}: buffered commits survived a process crash");
    }
}

#[test]
fn crash_mid_stream_recovers_whole_batches() {
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path(), "unified_pairs");
    let db = DB::open(unified_opts(dir.path().to_str().unwrap(), 4 << 10)).unwrap();
    let c = cf(&db, "c", SyncMode::None);
    let mut recovered = 0;
    for i in 0..3000 {
        let a = db.get(&c, format!("a{i:06}").as_bytes()).is_ok();
        let b = db.get(&c, format!("b{i:06}").as_bytes()).is_ok();
        assert_eq!(a, b, "pair {i} recovered half a batch");
        if a {
            assert_eq!(recovered, i, "pair {i} recovered after a lost one");
            recovered += 1;
        }
    }
    // A 4 KiB buffer flushed many times over 3000 commits; only its last
    // unflushed fill may be lost.
    assert!(recovered > 2000, "only {recovered} pairs recovered");
    assert!(recovered < 3000, "nothing was buffered at the crash");
}

#[test]
fn crash_after_prepare_keeps_the_prepare() {
    let dir = tempfile::tempdir().unwrap();
    run_crash(dir.path(), "unified_prepare");
    let db = DB::open(unified_opts(dir.path().to_str().unwrap(), 1 << 20)).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
        .unwrap();
    let c = cf(&db, "c", SyncMode::None);
    let prepared = db.list_prepared();
    assert_eq!(prepared.len(), 1, "an acknowledged prepare was lost");
    assert_eq!(prepared[0].id, [5; 16]);
    // The prepare's fsync flushed the buffer, so the commit before it on the
    // same stripe survived too.
    assert_eq!(db.get(&c, b"buffered").unwrap(), b"v");
    db.abort_prepared(&[5; 16]).unwrap();
}
