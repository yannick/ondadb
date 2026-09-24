//! Bounded parallel block reads for batched point reads
//! (`Options::max_concurrent_block_reads`, wavesdb `MaxConcurrentBlockReads`,
//! plan C P5).
//!
//! The slow tier is a caller-provided `Storage` (`TierDef::custom`) wrapping a
//! local directory: every positional read sleeps, and the wrapper records how
//! many reads were in flight at once and can fail one chosen block. That is the
//! shape of an S3 tier — no mmap, a block read costs milliseconds — without
//! needing an object store.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ondadb::cache::FileCache;
use ondadb::storage::{ReadHandle, StorageWriter};
use ondadb::{
    ColumnFamily, ColumnFamilyConfig, LocalStorage, OndaError, Options, PartitionRule, Storage,
    TierDef, TierRule, DB,
};
use parking_lot::Mutex;

const ZERO: Duration = Duration::ZERO;
const KEYS: usize = 3000;

#[derive(Default)]
struct Probe {
    delay: Mutex<Duration>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    /// `(path, offset)` of every klog read while recording.
    recorded: Mutex<Option<Vec<(String, u64)>>>,
    fail: Mutex<Option<(String, u64)>>,
}

impl Probe {
    fn reset_peak(&self) {
        self.peak.store(0, Ordering::SeqCst);
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct SlowStorage {
    inner: Arc<LocalStorage>,
    probe: Arc<ProbeHandle>,
}

/// `Probe` behind a `Debug`-able newtype (the `Storage` bound wants `Debug`).
struct ProbeHandle(Probe);
impl std::fmt::Debug for ProbeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Probe")
    }
}

struct SlowHandle {
    inner: Arc<dyn ReadHandle>,
    path: String,
    probe: Arc<ProbeHandle>,
}

impl ReadHandle for SlowHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> ondadb::Result<()> {
        let p = &self.probe.0;
        let klog = self.path.ends_with(".klog");
        if klog {
            if let Some(rec) = p.recorded.lock().as_mut() {
                rec.push((self.path.clone(), offset));
            }
            if p.fail.lock().as_ref() == Some(&(self.path.clone(), offset)) {
                return Err(std::io::Error::other("injected block read failure").into());
            }
        }
        let n = p.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        p.peak.fetch_max(n, Ordering::SeqCst);
        let delay = *p.delay.lock();
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        let r = self.inner.read_exact_at(buf, offset);
        p.in_flight.fetch_sub(1, Ordering::SeqCst);
        r
    }

    fn size(&self) -> ondadb::Result<u64> {
        self.inner.size()
    }
}

impl Storage for SlowStorage {
    fn open_read(&self, path: &str) -> ondadb::Result<Arc<dyn ReadHandle>> {
        Ok(Arc::new(SlowHandle {
            inner: self.inner.open_read(path)?,
            path: path.to_string(),
            probe: self.probe.clone(),
        }))
    }
    fn create(&self, path: &str) -> ondadb::Result<Box<dyn StorageWriter>> {
        self.inner.create(path)
    }
    fn ensure_dir(&self, dir: &str) -> ondadb::Result<()> {
        self.inner.ensure_dir(dir)
    }
    fn delete(&self, path: &str) -> ondadb::Result<()> {
        self.inner.delete(path)
    }
    fn rename(&self, from: &str, to: &str) -> ondadb::Result<()> {
        self.inner.rename(from, to)
    }
    fn list(&self, dir: &str) -> ondadb::Result<Vec<String>> {
        self.inner.list(dir)
    }
    fn supports_mmap(&self) -> bool {
        false
    }
    fn release(&self, path: &str) {
        self.inner.release(path)
    }
}

struct Fixture {
    _dirs: (tempfile::TempDir, tempfile::TempDir),
    db: DB,
    cf: Arc<ColumnFamily>,
    probe: Arc<ProbeHandle>,
}

fn key(i: usize) -> Vec<u8> {
    format!("img/{i:05}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    format!("value-{i:05}-{}", "x".repeat(48)).into_bytes()
}

/// `KEYS` keys in one part moved onto the slow tier, in ~512-byte blocks, with
/// the block cache off so every batch is cold.
fn fixture(limit: usize) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let tier_dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(ProbeHandle(Probe::default()));
    let storage = Arc::new(SlowStorage {
        inner: LocalStorage::new(Arc::new(FileCache::new(64)), false),
        probe: probe.clone(),
    });
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.tiers = vec![TierDef::custom(
        "slow",
        tier_dir.path().to_str().unwrap(),
        storage,
    )];
    opts.part_mover_interval = ZERO;
    opts.block_cache_size = 0;
    opts.max_concurrent_block_reads = limit;
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                partition_rules: vec![PartitionRule {
                    prefix: b"img/".to_vec(),
                    name: "img".into(),
                }],
                tier_rules: vec![TierRule {
                    prefix: b"img/".to_vec(),
                    tier: "slow".into(),
                    min_age: ZERO,
                }],
                l1_file_count_trigger: 1,
                data_block_size: 512,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for i in 0..KEYS {
        db.put(&cf, &key(i), &value(i), ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(db.run_part_mover().unwrap(), 1, "the img/ part must move");
    // Open the moved table's reader now: its footer/index/bloom reads are not
    // data-block reads, and the bound is only about the latter.
    assert_eq!(db.get(&cf, &key(0)).unwrap(), value(0));
    Fixture {
        _dirs: (dir, tier_dir),
        db,
        cf,
        probe,
    }
}

fn batch_keys(step: usize) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = (0..KEYS).step_by(step).map(key).collect();
    keys.push(b"img/absent".to_vec());
    keys.push(key(7)); // a duplicate
    keys
}

fn check_matches_get(f: &Fixture, keys: &[Vec<u8>], got: &[ondadb::Result<Vec<u8>>]) {
    assert_eq!(got.len(), keys.len());
    for (k, r) in keys.iter().zip(got) {
        match (f.db.get(&f.cf, k), r) {
            (Ok(a), Ok(b)) => assert_eq!(&a, b, "key {:?}", String::from_utf8_lossy(k)),
            (Err(OndaError::NotFound), Err(OndaError::NotFound)) => {}
            (a, b) => panic!(
                "key {:?}: get {a:?} vs multi_get {b:?}",
                String::from_utf8_lossy(k)
            ),
        }
    }
}

#[test]
fn parallel_reads_are_bounded_and_answer_like_get() {
    let f = fixture(3);
    *f.probe.0.delay.lock() = Duration::from_millis(2);
    let keys = batch_keys(7);
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    f.probe.0.reset_peak();
    let (got, perf) = f.db.multi_get_with_perf(&f.cf, &refs);
    let peak = f.probe.0.peak();
    assert!(peak <= 3, "{peak} reads in flight, bound is 3");
    assert!(peak >= 2, "the batch never overlapped a read (peak {peak})");
    assert!(perf.multiget_parallel_reads >= 4, "{perf:?}");
    // Every parallel read is a block fetch the caller's scope accounts for.
    assert!(
        perf.block_misses >= perf.multiget_parallel_reads,
        "{perf:?}"
    );
    *f.probe.0.delay.lock() = ZERO;
    check_matches_get(&f, &keys, &got);
}

#[test]
fn concurrent_batches_share_one_bound() {
    let f = Arc::new(fixture(3));
    *f.probe.0.delay.lock() = Duration::from_millis(2);
    f.probe.0.reset_peak();
    let handles: Vec<_> = (0..4)
        .map(|t| {
            let f = f.clone();
            std::thread::spawn(move || {
                let keys: Vec<Vec<u8>> = (t..KEYS).step_by(11).map(key).collect();
                let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                let got = f.db.multi_get(&f.cf, &refs);
                for (k, r) in keys.iter().zip(got) {
                    let i: usize = std::str::from_utf8(&k[4..]).unwrap().parse().unwrap();
                    assert_eq!(r.unwrap(), value(i));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let peak = f.probe.0.peak();
    assert!(
        peak <= 3,
        "{peak} reads in flight across batches, bound is 3"
    );
}

#[test]
fn a_bound_of_one_keeps_the_sequential_path() {
    let f = fixture(1);
    let keys = batch_keys(5);
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    f.probe.0.reset_peak();
    let (got, perf) = f.db.multi_get_with_perf(&f.cf, &refs);
    assert_eq!(perf.multiget_parallel_reads, 0);
    assert_eq!(f.probe.0.peak(), 1);
    check_matches_get(&f, &keys, &got);
}

#[test]
fn the_default_tier_keeps_the_sequential_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::new(dir.path().to_str().unwrap());
    opts.block_cache_size = 0;
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                data_block_size: 512,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for i in 0..KEYS {
        db.put(&cf, &key(i), &value(i), ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let keys = batch_keys(3);
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let (got, perf) = db.multi_get_with_perf(&cf, &refs);
    assert_eq!(perf.multiget_parallel_reads, 0, "a local table fanned out");
    // Blocks were read (under mmap an uncompressed block is a zero-copy view,
    // counted in `block_read_bytes` but never as a miss).
    assert!(perf.block_read_bytes > 0, "{perf:?}");
    assert_eq!(got[1].as_ref().unwrap(), &value(3));
}

/// wavesdb's contract (`TestMultiGetPartialCorruption`): errors are per key.
/// A failed block read fails exactly the keys whose answer needed that block —
/// the same keys a `get` fails on — and every other slot is answered.
#[test]
fn a_failed_block_read_fails_only_its_keys() {
    let f = fixture(4);
    let keys = batch_keys(3);
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    *f.probe.0.recorded.lock() = Some(Vec::new());
    let (_, perf) = f.db.multi_get_with_perf(&f.cf, &refs);
    assert!(perf.multiget_parallel_reads >= 4, "{perf:?}");
    let reads = f.probe.0.recorded.lock().take().unwrap();
    assert!(reads.len() > 8, "{} block reads", reads.len());
    let mut sorted = reads.clone();
    sorted.sort();
    *f.probe.0.fail.lock() = Some(sorted[sorted.len() / 2].clone());

    let (got, perf) = f.db.multi_get_with_perf(&f.cf, &refs);
    assert!(perf.multiget_parallel_reads >= 4, "{perf:?}");
    let mut failed = 0;
    for (k, r) in keys.iter().zip(&got) {
        match (f.db.get(&f.cf, k), r) {
            (Ok(a), Ok(b)) => assert_eq!(&a, b),
            (Err(OndaError::NotFound), Err(OndaError::NotFound)) => {}
            (Err(a), Err(b)) => {
                failed += 1;
                assert!(format!("{b}").contains("injected"), "{b}");
                assert_eq!(a.to_string(), b.to_string());
            }
            (a, b) => panic!(
                "key {:?}: get {a:?} vs multi_get {b:?}",
                String::from_utf8_lossy(k)
            ),
        }
    }
    assert!(failed >= 1, "the failed block failed no key");
    assert!(
        failed < keys.len() / 4,
        "{failed} keys failed for one block"
    );
}

/// Provisional speed evidence, not a gate: one cold batch against a tier whose
/// block reads take 2 ms, at bounds 1 and 8. Run with `--ignored --nocapture`.
#[test]
#[ignore]
fn slow_tier_batch_latency() {
    for limit in [1, 8] {
        let f = fixture(limit);
        *f.probe.0.delay.lock() = Duration::from_millis(2);
        let keys = batch_keys(20);
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let mut best = Duration::MAX;
        for _ in 0..5 {
            let t = std::time::Instant::now();
            let got = f.db.multi_get(&f.cf, &refs);
            best = best.min(t.elapsed());
            assert!(got[0].is_ok());
        }
        eprintln!(
            "max_concurrent_block_reads={limit}: {} keys, best of 5 {best:?}",
            keys.len()
        );
    }
}
