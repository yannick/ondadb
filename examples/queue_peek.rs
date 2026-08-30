//! Queue-peek harness for 0.9 (keyspace-tailing iterator).
//!
//! The workload is ondaDB's documented queue peek (`memtable.rs` header): a
//! producer appends to an ordered keyspace while consumers poll the tail of it.
//! The two modes differ only in the poll body:
//!
//! * `-mode rebuild` — the baseline available before 0.9: every poll builds a
//!   fresh `Txn::new_iterator_bounded(cf, Excluded(last_yielded), Unbounded)`.
//! * `-mode tail` — `DB::new_tailing_iterator` plus `refresh()`, which
//!   constructs an iterator only when the segment is exhausted *and* the
//!   read-committed floor has advanced.
//!
//! Both modes poll as fast as they can and `yield_now()` on an empty poll, so
//! with one consumer the run is producer-bound and the *construction count* is
//! the meaningful signal; raise `-consumers` to make the poll path the
//! bottleneck and let the difference reach wall time. Compare modes within one
//! run, never absolute numbers across sessions (`docs/performance.md`).
//!
//! ```sh
//! cargo run --release --example queue_peek -- -mode tail    -consumers 8
//! cargo run --release --example queue_peek -- -mode rebuild -consumers 8
//! ```

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, DB};

const VALUE: &[u8] = b"queue-peek-payload-------------------------------";

/// Give up rather than hang if a consumer stops making progress.
const DEADLINE: Duration = Duration::from_secs(600);

fn key(i: u64) -> Vec<u8> {
    format!("q{i:012}").into_bytes()
}

struct Args {
    mode: String,
    ops: u64,
    backlog: u64,
    batch: u64,
    consumers: usize,
    /// Small buffers plus a high L0 trigger let L0 files pile up *above* the
    /// cursor, where bound pruning cannot remove them — the shape that makes a
    /// per-poll rebuild expensive rather than nearly free.
    write_buffer: usize,
    l0_trigger: u32,
    /// When non-zero, run the fixed-work poll phase instead of the streaming
    /// one: no producer, exactly this many polls of an already-drained queue.
    /// A real queue peek spends most of its polls finding nothing, and that is
    /// the poll 0.9 makes cheap; the streaming phase, by contrast, is bounded
    /// by the producer and cannot show a consumer-side saving at all.
    idle_polls: u64,
    db_path: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: "tail".into(),
        ops: 200_000,
        backlog: 50_000,
        batch: 200,
        consumers: 1,
        write_buffer: 4 * 1024 * 1024,
        l0_trigger: 4,
        idle_polls: 0,
        db_path: "queue_peek_data".into(),
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let val = |i: &mut usize| -> String {
            *i += 1;
            argv.get(*i).cloned().unwrap_or_else(|| {
                eprintln!("queue_peek: missing value for {}", argv[*i - 1]);
                std::process::exit(2);
            })
        };
        match argv[i].as_str() {
            "-mode" => a.mode = val(&mut i),
            "-ops" => a.ops = val(&mut i).parse().expect("-ops"),
            "-backlog" => a.backlog = val(&mut i).parse().expect("-backlog"),
            "-batch" => a.batch = val(&mut i).parse().expect("-batch"),
            "-consumers" => a.consumers = val(&mut i).parse().expect("-consumers"),
            "-write-buffer" => a.write_buffer = val(&mut i).parse().expect("-write-buffer"),
            "-l0-trigger" => a.l0_trigger = val(&mut i).parse().expect("-l0-trigger"),
            "-idle-polls" => a.idle_polls = val(&mut i).parse().expect("-idle-polls"),
            "-db" => a.db_path = val(&mut i),
            other => {
                eprintln!("queue_peek: unknown flag {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    if a.mode != "tail" && a.mode != "rebuild" {
        eprintln!("queue_peek: -mode must be tail or rebuild");
        std::process::exit(2);
    }
    if a.consumers == 0 {
        eprintln!("queue_peek: -consumers must be positive");
        std::process::exit(2);
    }
    a
}

/// Append `[from, to)` in batched transactions.
fn produce(db: &DB, cf: &Arc<ColumnFamily>, from: u64, to: u64, batch: u64) {
    let mut i = from;
    while i < to {
        let end = (i + batch).min(to);
        let mut txn = db.begin();
        for seq in i..end {
            txn.put(cf, &key(seq), VALUE, Duration::ZERO).unwrap();
        }
        txn.commit().unwrap();
        i = end;
    }
}

/// Consume `total` entries with a 0.9 tail, returning iterator constructions.
fn consume_tailing(db: &DB, cf: &Arc<ColumnFamily>, total: u64, start: Instant) -> u64 {
    let mut tail = db.new_tailing_iterator(cf);
    tail.seek_to_first();
    let mut seen = 0u64;
    while seen < total {
        while tail.valid() {
            std::hint::black_box(tail.key());
            seen += 1;
            tail.next();
        }
        assert!(tail.err().is_none(), "{:?}", tail.err());
        if seen == total {
            break;
        }
        assert!(start.elapsed() < DEADLINE, "stalled at {seen}/{total}");
        if !tail.refresh() {
            std::thread::yield_now();
        }
    }
    tail.segments()
}

/// The pre-0.9 baseline: one iterator construction per poll.
fn consume_rebuilding(db: &DB, cf: &Arc<ColumnFamily>, total: u64, start: Instant) -> u64 {
    let mut seen = 0u64;
    let mut constructions = 0u64;
    let mut last: Option<Vec<u8>> = None;
    while seen < total {
        let mut txn = db.begin();
        let lower = match &last {
            Some(k) => Bound::Excluded(k.as_slice()),
            None => Bound::Unbounded,
        };
        let mut it = txn.new_iterator_bounded(cf, lower, Bound::Unbounded);
        constructions += 1;
        it.seek_to_first();
        let mut got = false;
        while it.valid() {
            std::hint::black_box(it.key());
            last = Some(it.key().to_vec());
            seen += 1;
            got = true;
            it.next();
        }
        assert!(it.err().is_none(), "{:?}", it.err());
        drop(it);
        let _ = txn.rollback();
        if seen == total {
            break;
        }
        assert!(start.elapsed() < DEADLINE, "stalled at {seen}/{total}");
        if !got {
            std::thread::yield_now();
        }
    }
    constructions
}

/// Fixed-work poll phase: drain the queue, then poll it `polls` times with no
/// producer running. Returns `(elapsed, iterator constructions)`.
///
/// No `yield_now`, no producer, identical poll counts — so the two modes differ
/// only in what one poll of an up-to-date queue costs.
fn idle_poll(db: &DB, cf: &Arc<ColumnFamily>, mode: &str, polls: u64) -> (Duration, u64) {
    if mode == "tail" {
        let mut tail = db.new_tailing_iterator(cf);
        tail.seek_to_first();
        while tail.valid() {
            tail.next();
        }
        let start = Instant::now();
        for _ in 0..polls {
            std::hint::black_box(tail.refresh());
        }
        let elapsed = start.elapsed();
        (elapsed, tail.segments())
    } else {
        let mut last: Option<Vec<u8>> = None;
        {
            let mut txn = db.begin();
            let mut it = txn.new_iterator_bounded(cf, Bound::Unbounded, Bound::Unbounded);
            it.seek_to_first();
            while it.valid() {
                last = Some(it.key().to_vec());
                it.next();
            }
            drop(it);
            let _ = txn.rollback();
        }
        let start = Instant::now();
        for _ in 0..polls {
            let mut txn = db.begin();
            let lower = match &last {
                Some(k) => Bound::Excluded(k.as_slice()),
                None => Bound::Unbounded,
            };
            let mut it = txn.new_iterator_bounded(cf, lower, Bound::Unbounded);
            it.seek_to_first();
            std::hint::black_box(it.valid());
            drop(it);
            let _ = txn.rollback();
        }
        let elapsed = start.elapsed();
        (elapsed, polls)
    }
}

fn main() {
    let a = parse_args();
    let _ = std::fs::remove_dir_all(&a.db_path);
    let db = Arc::new(DB::open(Options::new(&a.db_path)).expect("open"));
    let cf = db
        .create_column_family(
            "queue",
            ColumnFamilyConfig {
                write_buffer_size: a.write_buffer,
                l1_file_count_trigger: a.l0_trigger,
                l0_queue_stall_threshold: a.l0_trigger.saturating_mul(4).max(64),
                ..ColumnFamilyConfig::default()
            },
        )
        .expect("create cf");

    // A backlog gives the keyspace real depth (SSTables plus a live memtable),
    // so a rebuild is not measuring an empty merge heap.
    produce(&db, &cf, 0, a.backlog, a.batch);
    db.flush_memtable(&cf).expect("flush backlog");

    if a.idle_polls > 0 {
        let (elapsed, c) = idle_poll(&db, &cf, &a.mode, a.idle_polls);
        let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
        println!(
            "idle-poll({}) {} polls in {:.3} ms; {:.0} polls/sec; {:.1} ns/poll; constructions={c}; constructions_per_poll={:.6}",
            a.mode,
            a.idle_polls,
            elapsed.as_secs_f64() * 1000.0,
            a.idle_polls as f64 / secs,
            elapsed.as_nanos() as f64 / a.idle_polls as f64,
            c as f64 / a.idle_polls as f64,
        );
        db.close().expect("close");
        let _ = std::fs::remove_dir_all(&a.db_path);
        return;
    }

    let total = a.backlog + a.ops;
    let constructions = AtomicU64::new(0);

    let writer = {
        let db = db.clone();
        let cf = cf.clone();
        let (backlog, ops, batch) = (a.backlog, a.ops, a.batch);
        std::thread::spawn(move || produce(&db, &cf, backlog, backlog + ops, batch))
    };

    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..a.consumers {
            let db = &db;
            let cf = &cf;
            let constructions = &constructions;
            let mode = a.mode.as_str();
            s.spawn(move || {
                let c = if mode == "tail" {
                    consume_tailing(db, cf, total, start)
                } else {
                    consume_rebuilding(db, cf, total, start)
                };
                constructions.fetch_add(c, Ordering::Relaxed);
            });
        }
    });
    let elapsed = start.elapsed();
    writer.join().unwrap();

    let yielded = total * a.consumers as u64;
    let c = constructions.load(Ordering::Relaxed);
    let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "queue-peek({}) consumers={} {yielded} entries in {:.3} ms; {:.0} ops/sec; constructions={c}; constructions_per_entry={:.6}",
        a.mode,
        a.consumers,
        elapsed.as_secs_f64() * 1000.0,
        yielded as f64 / secs,
        c as f64 / yielded as f64,
    );

    db.close().expect("close");
    let _ = std::fs::remove_dir_all(&a.db_path);
}
