//! Tests for checkpoint, backup, clone, and stats.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, PartitionRule, TierDef, DB};

fn fill(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, n: u32) {
    for i in 0..n {
        db.put(cf, format!("k{i:05}").as_bytes(), b"value", Duration::ZERO)
            .unwrap();
    }
}

fn tiered_options(db: &std::path::Path, tier: &std::path::Path) -> Options {
    let mut options = Options::new(db.to_str().unwrap());
    options.tiers = vec![TierDef::new("hdd", tier.to_str().unwrap().to_string())];
    options
}

fn materialize_tiered_partition(db: &DB) -> std::sync::Arc<ondadb::ColumnFamily> {
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                partition_rules: vec![PartitionRule {
                    prefix: b"img/".to_vec(),
                    name: "img".into(),
                }],
                l1_file_count_trigger: 1,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    db.put(&cf, b"img/000", b"IMG", Duration::ZERO).unwrap();
    db.put(&cf, b"etc/000", b"ETC", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    db.move_part_to_tier(&cf, "img", "hdd").unwrap();
    cf
}

fn assert_snapshot_is_self_contained(path: &std::path::Path) {
    let db = DB::open(Options::new(path.to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"img/000").unwrap(), b"IMG");
    assert_eq!(db.get(&cf, b"etc/000").unwrap(), b"ETC");
    db.close().unwrap();
}

#[test]
fn tiered_backup_is_default_tier_self_contained() {
    let source = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let db = DB::open(tiered_options(source.path(), tier.path())).unwrap();
    materialize_tiered_partition(&db);
    db.backup(backup.path()).unwrap();
    db.close().unwrap();

    assert_snapshot_is_self_contained(backup.path());
}

#[test]
fn tiered_checkpoint_is_default_tier_self_contained() {
    let source = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    let db = DB::open(tiered_options(source.path(), tier.path())).unwrap();
    materialize_tiered_partition(&db);
    db.checkpoint(checkpoint.path()).unwrap();
    db.close().unwrap();

    assert_snapshot_is_self_contained(checkpoint.path());
}

#[test]
fn clone_of_tiered_cf_copies_data_to_the_default_tier() {
    let source = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let db = DB::open(tiered_options(source.path(), tier.path())).unwrap();
    let src = materialize_tiered_partition(&db);
    let clone = db.clone_column_family("default", "clone").unwrap();

    assert_eq!(db.get(&clone, b"img/000").unwrap(), b"IMG");
    assert_eq!(db.get(&clone, b"etc/000").unwrap(), b"ETC");
    assert_eq!(db.get(&src, b"img/000").unwrap(), b"IMG");
    db.close().unwrap();
}

#[test]
fn stats_report_levels_and_counts() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    fill(&db, &cf, 500);
    db.flush_memtable(&cf).unwrap();
    let s = cf.stats();
    assert_eq!(s.name, "default");
    assert!(s.num_entries >= 500);
    assert!(s.levels[0].0 >= 1, "expected at least one L0 file");
    let ds = db.stats();
    assert_eq!(ds.num_column_families, 1);
    assert!(ds.total_sstables >= 1);
    db.close().unwrap();
}

#[test]
fn checkpoint_is_readable() {
    let dir = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        fill(&db, &cf, 1000);
        db.checkpoint(ckpt.path()).unwrap();
        db.close().unwrap();
    }
    // Open the checkpoint directory as a database and verify the data.
    let db = DB::open(Options::new(ckpt.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in checkpoint");
    for i in 0..1000u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:05}").as_bytes()).unwrap(),
            b"value"
        );
    }
    db.close().unwrap();
}

#[test]
fn backup_is_independent_copy() {
    let dir = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        fill(&db, &cf, 500);
        db.backup(backup.path()).unwrap();
        db.close().unwrap();
    }
    let db = DB::open(Options::new(backup.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in backup");
    assert_eq!(db.get(&cf, b"k00042").unwrap(), b"value");
    db.close().unwrap();
}

#[test]
fn backup_consistent_during_compaction() {
    // Take a backup while heavy write + flush + compaction churn is running. The
    // backup's manifest must reference only files that exist in the backup, and it
    // must reopen with every key present. Before deletion-deferral, a compaction
    // could unlink an SSTable mid-backup, leaving the copied manifest referencing a
    // missing file (or the link failing).
    let dir = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let cfg = ColumnFamilyConfig {
        write_buffer_size: 32 * 1024, // tiny -> many flushes
        l1_file_count_trigger: 2,     // frequent compactions
        ..ColumnFamilyConfig::default()
    };
    let n = 30_000u32;
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("default", cfg).unwrap();
        // Write enough to have flushes/compactions in flight, then back up.
        for i in 0..n {
            db.put(&cf, format!("k{i:08}").as_bytes(), b"value", Duration::ZERO)
                .unwrap();
        }
        db.backup(backup.path()).unwrap();
        db.close().unwrap();
    }
    // The backup must be self-consistent and complete.
    let db = DB::open(Options::new(backup.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in backup");
    for i in 0..n {
        assert_eq!(
            db.get(&cf, format!("k{i:08}").as_bytes()).unwrap(),
            b"value",
            "missing k{i} in backup"
        );
    }
    db.close().unwrap();
}

#[test]
fn clone_column_family_shares_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let src = db
        .create_column_family("src", ColumnFamilyConfig::default())
        .unwrap();
    fill(&db, &src, 300);
    let dst = db.clone_column_family("src", "dst").unwrap();
    for i in 0..300u32 {
        assert_eq!(
            db.get(&dst, format!("k{i:05}").as_bytes()).unwrap(),
            b"value"
        );
    }
    // Writes to dst do not affect src.
    db.put(&dst, b"only-dst", b"x", Duration::ZERO).unwrap();
    assert!(db.get(&src, b"only-dst").is_err());
    db.close().unwrap();
}

#[test]
fn approximate_len_and_read_stats() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();

    for i in 0..500u32 {
        db.put(&cf, format!("k{i:04}").as_bytes(), b"v", Duration::ZERO)
            .unwrap();
    }
    // All 500 still in the memtable.
    let s = cf.stats();
    assert_eq!(s.memtable_entries, 500);
    assert_eq!(s.approximate_len, 500);
    assert_eq!(cf.approximate_len(), 500);

    db.flush_memtable(&cf).unwrap();
    let s = cf.stats();
    assert_eq!(s.memtable_entries, 0);
    assert_eq!(s.num_entries, 500);
    assert_eq!(s.approximate_len, 500);

    // Point reads hit the SSTable; misses should be answered by the bloom
    // filter without probing.
    for i in 0..100u32 {
        assert!(db.get(&cf, format!("k{i:04}").as_bytes()).is_ok());
    }
    // Misses chosen inside the SSTable's [min,max] key range, so they pass
    // range filtering and are answered by the bloom filter.
    for i in 0..100u32 {
        let _ = db.get(&cf, format!("k{i:04}miss").as_bytes());
    }
    let s = cf.stats();
    assert_eq!(s.point_reads, 200);
    assert!(s.sst_probes >= 100, "hits must probe: {}", s.sst_probes);
    assert!(
        s.bloom_skips >= 90,
        "most misses should be bloom-skipped: {}",
        s.bloom_skips
    );
    db.close().unwrap();
}

#[test]
fn compaction_failure_is_reported_in_stats() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                klog_value_threshold: 64,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    db.put(&cf, b"large", &[b'V'; 4096], Duration::ZERO)
        .unwrap();
    db.flush_memtable(&cf).unwrap();

    let vlog = std::fs::read_dir(dir.path().join("cf-default"))
        .unwrap()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "vlog")
        })
        .expect("flush created a vlog");
    let mut bytes = std::fs::read(&vlog).unwrap();
    bytes.last_mut().map(|byte| *byte ^= 0xff).unwrap();
    std::fs::write(&vlog, bytes).unwrap();

    let error = db
        .compact(&cf)
        .expect_err("corrupt input must fail compaction");
    assert_eq!(error.kind(), "corruption");
    let stats = cf.stats();
    assert_eq!(stats.compaction_failures, 1);
    let last = stats
        .last_compaction_error
        .expect("last compaction error should be retained")
        .to_ascii_lowercase();
    assert!(
        last.contains("checksum") || last.contains("corrupt"),
        "{last}"
    );
}

#[test]
fn compaction_filter_removes_and_respects_snapshots() {
    use std::sync::Arc;

    use ondadb::FilterDecision;

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    // trigger=1 so every flushed L0 file makes compact() actually rewrite.
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();

    for i in 0..100u32 {
        let v: &[u8] = if i % 2 == 0 { b"purge" } else { b"keep" };
        db.put(&cf, format!("k{i:03}").as_bytes(), v, Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();

    cf.set_compaction_filter(Some(Arc::new(|_k: &[u8], v: &[u8]| {
        if v == b"purge" {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    })));

    // A version written after this point is newer than the filter's snapshot
    // horizon during compaction only if a snapshot pins it — hold one.
    let snap = db.begin();
    db.put(&cf, b"k000", b"purge", Duration::ZERO).unwrap(); // newer, protected
    db.flush_memtable(&cf).unwrap();

    db.compact(&cf).unwrap();

    // Old "purge" versions are gone; "keep" survives.
    assert!(db.get(&cf, b"k002").is_err(), "filtered key must be gone");
    assert_eq!(db.get(&cf, b"k001").unwrap(), b"keep");
    // k000's newer version was written after the snapshot => protected.
    assert_eq!(db.get(&cf, b"k000").unwrap(), b"purge");
    drop(snap);

    // Without the snapshot the rewrite is now eligible. Push a fresh L0 file
    // overlapping the key range so compact() merges L1 through the filter
    // (non-overlapping next-level tables are retained, not rewritten).
    db.put(&cf, b"k050x", b"keep", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert!(db.get(&cf, b"k000").is_err());

    // Clearing the filter stops removals.
    cf.set_compaction_filter(None);
    db.put(&cf, b"again", b"purge", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(db.get(&cf, b"again").unwrap(), b"purge");
    db.close().unwrap();
}

#[test]
fn fifo_compaction_evicts_oldest_tables() {
    use ondadb::CompactionStyle;

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "fifo",
            ColumnFamilyConfig {
                compaction_style: CompactionStyle::Fifo,
                fifo_max_bytes: 8 * 1024, // a few small tables
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();

    // 8 generations of ~2 KiB tables; the size cap keeps only the newest few.
    for gen in 0..8u32 {
        for i in 0..20u32 {
            db.put(
                &cf,
                format!("g{gen}-k{i:02}").as_bytes(),
                &[b'v'; 100],
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
    }
    db.compact(&cf).unwrap();

    let s = cf.stats();
    let l0_bytes: u64 = s.levels.first().map(|(_, b)| *b).unwrap_or(0);
    assert!(
        l0_bytes <= 8 * 1024,
        "size cap must hold after eviction: {l0_bytes}"
    );
    // Newest generation survives, oldest is gone (cache semantics).
    assert_eq!(db.get(&cf, b"g7-k00").unwrap(), [b'v'; 100]);
    assert!(
        db.get(&cf, b"g0-k00").is_err(),
        "oldest gen must be evicted"
    );

    // Eviction is durable (manifest persisted before file deletion).
    db.close().unwrap();
    drop(cf);
    drop(db);
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("fifo").unwrap();
    assert_eq!(db.get(&cf, b"g7-k00").unwrap(), [b'v'; 100]);
    assert!(db.get(&cf, b"g0-k00").is_err());
    db.close().unwrap();
}

#[test]
fn fifo_ttl_evicts_aged_tables() {
    use ondadb::CompactionStyle;

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "fifo",
            ColumnFamilyConfig {
                compaction_style: CompactionStyle::Fifo,
                fifo_ttl: Duration::from_millis(400),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();

    db.put(&cf, b"old", b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    std::thread::sleep(Duration::from_millis(600));
    db.put(&cf, b"new", b"v", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();

    db.compact(&cf).unwrap();
    assert!(db.get(&cf, b"old").is_err(), "aged table must be evicted");
    assert_eq!(db.get(&cf, b"new").unwrap(), b"v");
    db.close().unwrap();
}

#[test]
#[ignore = "manual FIFO TTL metadata-selection timing probe"]
fn fifo_ttl_selection_probe() {
    use ondadb::CompactionStyle;

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "fifo-probe",
            ColumnFamilyConfig {
                compaction_style: CompactionStyle::Fifo,
                fifo_ttl: Duration::from_secs(3_600),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for i in 0..100u32 {
        db.put(
            &cf,
            format!("key-{i:03}").as_bytes(),
            b"value",
            Duration::ZERO,
        )
        .unwrap();
        db.flush_memtable(&cf).unwrap();
    }
    // Let automatically queued FIFO passes settle so this times one explicit
    // selection pass rather than scheduler backlog.
    std::thread::sleep(Duration::from_millis(500));
    let before = cf.stats().levels[0].0;
    let start = std::time::Instant::now();
    db.compact(&cf).unwrap();
    let elapsed = start.elapsed();
    let after = cf.stats().levels[0].0;
    eprintln!(
        "FIFO TTL selection: {} us, {} victims across {before} tables",
        elapsed.as_micros(),
        before.saturating_sub(after)
    );
    db.close().unwrap();
}

#[path = "support/levels.rs"]
mod levels;

/// The overlapping-level fixture generator (0.2) is a shared asset: the
/// picker's write-amplification benchmark compares two runs against each
/// other, and 0.1's and 0.8's benchmarks will reuse it, so a fixture that
/// drifted between runs would silently turn every such comparison into noise.
/// One seed must therefore produce one table sequence, byte for byte.
#[test]
fn level_fixture_is_deterministic() {
    let geometry = levels::LevelGeometry {
        // Small enough to stay a test, large enough to cut several tables.
        batches: 3,
        keys_per_batch: 900,
        ..levels::LevelGeometry::default()
    };

    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let a = geometry.materialize_quiescent(first.path());
    let b = geometry.materialize_quiescent(second.path());

    assert_eq!(a.len(), geometry.batches, "one flushed table per batch");
    assert_eq!(
        levels::fingerprint(&a),
        levels::fingerprint(&b),
        "same seed produced a different table sequence"
    );

    // A different seed must actually move the geometry, or the determinism
    // above would be the trivial kind.
    let third = tempfile::tempdir().unwrap();
    let other = geometry
        .clone()
        .with_seed(0x5EED)
        .materialize_quiescent(third.path());
    assert_ne!(levels::fingerprint(&a), levels::fingerprint(&other));
}

// ---------------------------------------------------------------------------
// 0.6-A: background IO classes and the rate limiter.
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtOrd};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use ondadb::ioctrl::{Clock, IoClass, IoLimiter, RecordingLimiter, TokenBucket};

/// A clock the test drives: `now` moves only when someone waits on it, and a
/// wait advances it instead of sleeping, so pacing is asserted in simulated
/// time and the test costs nothing in wall-clock seconds.
#[derive(Debug, Default)]
struct FakeClock {
    base: OnceLock<Instant>,
    elapsed_nanos: AtomicU64,
}

impl FakeClock {
    fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.elapsed_nanos.load(AtOrd::SeqCst))
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.base.get_or_init(Instant::now) + self.elapsed()
    }
    fn wait(&self, dur: Duration) {
        self.elapsed_nanos
            .fetch_add(dur.as_nanos() as u64, AtOrd::SeqCst);
    }
}

/// A token bucket that also records, per class, what it admitted and how much
/// simulated time that class spent waiting.
#[derive(Debug)]
struct PacedRecorder {
    clock: Arc<FakeClock>,
    bucket: TokenBucket,
    /// class name -> (bytes, waited nanos). Keyed by name because `IoClass` is
    /// only what the engine reports; the test never constructs one to look up.
    seen: Mutex<HashMap<String, (u64, u64)>>,
    /// class name -> number of charges. Separate from `seen` because a paced
    /// deletion is counted per *file*, and bytes alone cannot tell how many
    /// files a total covers.
    counts: Mutex<HashMap<String, usize>>,
}

impl PacedRecorder {
    fn new(rate: u64, burst: u64) -> Arc<PacedRecorder> {
        let clock = Arc::new(FakeClock::default());
        Arc::new(PacedRecorder {
            bucket: TokenBucket::with_clock(rate, burst, clock.clone()),
            clock,
            seen: Mutex::new(HashMap::new()),
            counts: Mutex::new(HashMap::new()),
        })
    }
    fn entry(&self, class: IoClass) -> (u64, u64) {
        self.seen
            .lock()
            .unwrap()
            .get(&format!("{class:?}"))
            .copied()
            .unwrap_or((0, 0))
    }
    fn bytes_for(&self, class: IoClass) -> u64 {
        self.entry(class).0
    }
    fn waited(&self, class: IoClass) -> Duration {
        Duration::from_nanos(self.entry(class).1)
    }
    /// Total simulated time the bucket made *anyone* wait.
    ///
    /// The per-class figure cannot carry the pacing assertion on its own:
    /// tokens are shared, so time one class spends waiting refills the bucket
    /// for another, and a class can therefore be paced without ever calling
    /// `wait` itself. The clock is the only complete account.
    fn total_simulated(&self) -> Duration {
        self.clock.elapsed()
    }
    /// Number of charges seen under `class`.
    fn count_for(&self, class: IoClass) -> usize {
        self.counts
            .lock()
            .unwrap()
            .get(&format!("{class:?}"))
            .copied()
            .unwrap_or(0)
    }
    /// Record a charge without admitting it through the bucket — the class is
    /// accounted for but never delayed, and the simulated clock does not move.
    fn record_free(&self, class: IoClass, bytes: u64) {
        self.note(class, bytes, Duration::ZERO);
    }
    fn note(&self, class: IoClass, bytes: u64, waited: Duration) {
        let key = format!("{class:?}");
        let mut seen = self.seen.lock().unwrap();
        let e = seen.entry(key.clone()).or_insert((0, 0));
        e.0 += bytes;
        e.1 += waited.as_nanos() as u64;
        drop(seen);
        *self.counts.lock().unwrap().entry(key).or_insert(0) += 1;
    }
    /// Bytes charged under every background class.
    fn background_bytes(&self) -> u64 {
        self.bytes_for(IoClass::Flush)
            + self.bytes_for(IoClass::Compaction)
            + self.bytes_for(IoClass::ObsoleteDelete)
    }
}

impl IoLimiter for PacedRecorder {
    fn charge(&self, class: IoClass, bytes: u64) {
        let before = self.clock.elapsed();
        self.bucket.charge(class, bytes);
        let waited = self.clock.elapsed().saturating_sub(before);
        self.note(class, bytes, waited);
    }
    fn cancel(&self) {
        self.bucket.cancel();
    }
}

fn limited_options(dir: &std::path::Path, limiter: Arc<dyn IoLimiter>) -> Options {
    let mut options = Options::new(dir.to_str().unwrap());
    options.io_limiter = Some(limiter);
    options
}

/// A CF with several flushed generations of the same keys, so a compaction has
/// real merge work across levels rather than one file to rename.
fn layered_cf(db: &DB) -> Arc<ondadb::ColumnFamily> {
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                l1_file_count_trigger: 2,
                write_buffer_size: 8 << 10,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for round in 0..4u32 {
        for i in 0..600u32 {
            db.put(
                &cf,
                format!("k{i:05}").as_bytes(),
                format!("value-{round}-{i:04}").as_bytes(),
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
    }
    cf
}

#[test]
fn worker_threads_report_their_io_class() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(RecordingLimiter::default());
    let db = DB::open(limited_options(dir.path(), recorder.clone())).unwrap();
    let _cf = layered_cf(&db);
    // The flushes above ran on `onda-flush`; wait for the compaction worker to
    // pick up the CF the last flush armed.
    let deadline = Instant::now() + Duration::from_secs(30);
    while recorder.bytes_for(IoClass::Compaction) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let flush_bytes = recorder.bytes_for(IoClass::Flush);
    let compaction_bytes = recorder.bytes_for(IoClass::Compaction);
    db.close().unwrap();

    assert!(
        flush_bytes > 0,
        "the flush worker must charge under Flush (saw {} charges total)",
        recorder.charges().len()
    );
    assert!(
        compaction_bytes > 0,
        "the compaction worker must charge under Compaction"
    );
}

#[test]
fn manual_compaction_is_charged_as_background() {
    // The F6.1 regression pin: `DB::compact` runs the largest burst the engine
    // produces on the *caller's* thread. A spawn-time-only class would leave
    // every one of those bytes labelled Foreground, and therefore unpaced.
    let dir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(RecordingLimiter::default());
    let db = DB::open(limited_options(dir.path(), recorder.clone())).unwrap();
    let cf = layered_cf(&db);
    // Let flush-armed background compaction settle, then measure only the sweep.
    std::thread::sleep(Duration::from_millis(300));
    recorder.clear();

    db.compact(&cf).unwrap();

    let compaction = recorder.bytes_for(IoClass::Compaction);
    let foreground = recorder.count_for(IoClass::Foreground);
    db.close().unwrap();
    assert!(
        compaction > 0,
        "manual compaction must charge under Compaction"
    );
    assert_eq!(
        foreground, 0,
        "manual compaction must charge nothing as Foreground"
    );
}

#[test]
fn foreground_reads_are_classified_foreground() {
    // The other half of the pin: a user thread must stay Foreground, or the
    // limiter cannot tell the two apart at all.
    let dir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(RecordingLimiter::default());
    let db = DB::open(limited_options(dir.path(), recorder.clone())).unwrap();
    let cf = layered_cf(&db);
    db.compact(&cf).unwrap();
    recorder.clear();
    for i in 0..600u32 {
        db.get(&cf, format!("k{i:05}").as_bytes()).unwrap();
    }
    let foreground = recorder.bytes_for(IoClass::Foreground);
    db.close().unwrap();
    // Only that the reading thread stayed Foreground. Deliberately not "and
    // nothing else was charged": a background compaction worker may still be
    // draining what the flushes armed, and asserting on its absence would be a
    // timing test, not a classification test.
    assert!(
        foreground > 0,
        "cold point reads must be charged, under Foreground"
    );
}

#[test]
fn limited_compaction_stretches_over_fake_clock() {
    // A tight limit must stretch compaction across (simulated) time while
    // concurrent point reads wait for nothing at all.
    let dir = tempfile::tempdir().unwrap();
    let rate = 16 << 10; // 16 KiB/s — tight enough that any real work waits
    let paced = PacedRecorder::new(rate, rate);
    let db = DB::open(limited_options(dir.path(), paced.clone())).unwrap();
    let cf = layered_cf(&db);
    std::thread::sleep(Duration::from_millis(300));

    let stop = Arc::new(AtomicBool::new(false));
    let perf = std::thread::scope(|scope| {
        let reader_cf = cf.clone();
        let reader_stop = stop.clone();
        let reader_db = &db;
        let reader = scope.spawn(move || {
            let mut totals = ondadb::PerfContext::default();
            while !reader_stop.load(AtOrd::SeqCst) {
                for i in 0..600u32 {
                    let s = ondadb::perf::enter();
                    let _ = reader_db.get(&reader_cf, format!("k{i:05}").as_bytes());
                    let c = s.finish();
                    totals.block_misses += c.block_misses;
                    totals.block_read_bytes += c.block_read_bytes;
                }
            }
            totals
        });
        db.compact(&cf).unwrap();
        stop.store(true, AtOrd::SeqCst);
        reader.join().unwrap()
    });

    let compaction_bytes = paced.bytes_for(IoClass::Compaction);
    let background_bytes = paced.background_bytes();
    let simulated = paced.total_simulated();
    let foreground_wait = paced.waited(IoClass::Foreground);
    db.close().unwrap();

    assert!(
        compaction_bytes > 0,
        "the compaction under test charged nothing"
    );
    assert!(
        background_bytes > 4 * rate,
        "not enough background work to pace: {background_bytes} bytes"
    );
    // Work-conserving: one full burst is free, everything after it is paid at
    // the rate. That is an exact lower bound on the *total* simulated time,
    // whichever class happened to be the one parked when it elapsed.
    let floor = Duration::from_secs_f64(background_bytes.saturating_sub(rate) as f64 / rate as f64);
    assert!(
        simulated >= floor.mul_f64(0.75),
        "pacing should consume at least ~{floor:?} of simulated time, got \
         {simulated:?} for {background_bytes} background bytes"
    );
    assert_eq!(
        foreground_wait,
        Duration::ZERO,
        "foreground reads must never wait on the limiter"
    );
    assert!(
        perf.block_read_bytes > 0,
        "the concurrent reads must actually have touched the device"
    );
}

#[test]
fn close_wakes_a_blocked_background_charge() {
    // Close must cancel the limiter *before* it waits for the final flush to
    // drain. At one byte per second that flush would otherwise take about as
    // many seconds as it has bytes, and close would never return.
    //
    // The write buffer is left at its default so nothing rotates during the
    // puts: this pins the close path specifically, not the writer backpressure
    // a tight limit also (correctly) produces.
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path().to_str().unwrap());
    options.background_io_bytes_per_second = 1;
    options.background_io_burst_bytes = 1;
    let db = DB::open(options).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let value = vec![b'v'; 512];
    for i in 0..4000u32 {
        db.put(&cf, format!("k{i:05}").as_bytes(), &value, Duration::ZERO)
            .unwrap();
    }
    // ~2 MB of memtable is about to be flushed. Unpaced that is milliseconds;
    // at one byte per second it is three weeks.
    let started = Instant::now();
    db.close().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "close must cancel the limiter before draining flushes; took {:?}",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// 0.6-B: paced obsolete-file deletion.
// ---------------------------------------------------------------------------

/// A limiter that paces **only** `ObsoleteDelete`, admitting every other class
/// at once and merely recording it.
///
/// In production flush, compaction and deletion share one bucket — they share
/// one device. A test that let them share the *simulated clock* could not say
/// whether the time it measured was spent pacing deletions or pacing the
/// compaction that produced them. Here the clock advances if and only if a
/// deletion waited.
#[derive(Debug)]
struct DeletePacer {
    inner: Arc<PacedRecorder>,
}

impl DeletePacer {
    fn new(rate: u64, burst: u64) -> Arc<DeletePacer> {
        Arc::new(DeletePacer {
            inner: PacedRecorder::new(rate, burst),
        })
    }
}

impl IoLimiter for DeletePacer {
    fn charge(&self, class: IoClass, bytes: u64) {
        if class == IoClass::ObsoleteDelete {
            self.inner.charge(class, bytes);
        } else {
            self.inner.record_free(class, bytes);
        }
    }
    fn cancel(&self) {
        self.inner.cancel();
    }
}

/// Every SSTable file currently in `cf_dir`.
fn sst_files(cf_dir: &std::path::Path) -> std::collections::HashSet<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(cf_dir) else {
        return std::collections::HashSet::new();
    };
    entries
        .filter_map(|e| {
            let path = e.ok()?.path();
            let ext = path.extension()?.to_str()?.to_string();
            (ext == "klog" || ext == "vlog").then_some(path)
        })
        .collect()
}

/// `rounds` L0 files over the same key span, with background compaction held
/// off by an unreachable trigger so the *test* decides when they all become
/// obsolete at once.
fn stacked_l0(db: &DB, rounds: u32) -> Arc<ondadb::ColumnFamily> {
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1 << 20,
                write_buffer_size: 8 << 10,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for round in 0..rounds {
        for i in 0..200u32 {
            db.put(
                &cf,
                format!("k{i:05}").as_bytes(),
                format!("value-{round}-{i:04}").as_bytes(),
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
    }
    cf
}

/// Wait until every file in `files` is gone, or fail.
fn await_unlinked(files: &std::collections::HashSet<std::path::PathBuf>, within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        let left = files.iter().filter(|p| p.exists()).count();
        if left == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{left}/{} obsolete files still on disk after {within:?}",
            files.len()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn unpaced_deletion_is_immediate() {
    // Pins today's observable behaviour at the default rate of 0: no worker, no
    // channel, and the unlink has already happened when the compaction that
    // obsoleted the file returns. Anything slower would be a regression for
    // every database that never configures pacing at all.
    let dir = tempfile::tempdir().unwrap();
    let options = Options::new(dir.path().to_str().unwrap());
    assert_eq!(
        options.obsolete_delete_bytes_per_second, 0,
        "unpaced must be the default"
    );
    let db = DB::open(options).unwrap();
    let cf = stacked_l0(&db, 8);
    let cf_dir = dir.path().join("cf-default");
    let before = sst_files(&cf_dir);
    assert!(!before.is_empty(), "no SST files to obsolete");

    db.compact(&cf).unwrap();

    let survivors = before.iter().filter(|p| p.exists()).count();
    db.close().unwrap();
    assert_eq!(
        survivors, 0,
        "unpaced deletion must unlink inline; {survivors} input files survived compact()"
    );
}

#[test]
fn paced_deletion_spreads_over_fake_clock() {
    // A hundred files going obsolete at once is the delete storm the feature
    // exists for. Under a rate the storm must be spread across (simulated)
    // time instead of issued as one burst of unlinks — and every file must
    // still be gone at the end.
    let dir = tempfile::tempdir().unwrap();
    let rate = 64 << 10; // 64 KiB/s of unlink credit
    let pacer = DeletePacer::new(rate, rate);
    let mut options = limited_options(dir.path(), pacer.clone());
    options.obsolete_delete_bytes_per_second = rate;
    let db = DB::open(options).unwrap();
    let cf = stacked_l0(&db, 100);
    let cf_dir = dir.path().join("cf-default");
    let obsolete = sst_files(&cf_dir);
    assert!(
        obsolete.len() >= 100,
        "want a real storm, got {} files",
        obsolete.len()
    );

    db.compact(&cf).unwrap();
    await_unlinked(&obsolete, Duration::from_secs(60));

    let charged = pacer.inner.bytes_for(IoClass::ObsoleteDelete);
    let simulated = pacer.inner.total_simulated();
    let count = pacer.inner.count_for(IoClass::ObsoleteDelete);
    db.close().unwrap();

    assert!(
        count >= obsolete.len(),
        "every obsolete file must be charged: {count} charges for {} files",
        obsolete.len()
    );
    assert!(
        charged >= 100 * 4096,
        "a missing vlog still costs the metadata minimum; charged {charged}"
    );
    // Work-conserving: one burst is free, everything after it is paid at the
    // rate. That is a lower bound on the simulated time the storm must take.
    let floor = Duration::from_secs_f64(charged.saturating_sub(rate) as f64 / rate as f64);
    assert!(
        simulated >= floor.mul_f64(0.75),
        "pacing should consume at least ~{floor:?} of simulated time for \
         {charged} bytes, got {simulated:?}"
    );
}

#[test]
fn paused_deletion_still_defers_with_worker() {
    // The pause is what makes a backup self-consistent, and it now has to win
    // against a *worker* rather than against the caller's own unlink. If one
    // queued task escaped it, the copied manifest would name a file the backup
    // does not contain. Same shape as `backup_consistent_during_compaction`,
    // with pacing on.
    let dir = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path().to_str().unwrap());
    options.obsolete_delete_bytes_per_second = 32 << 10;
    let n = 30_000u32;
    {
        let db = DB::open(options).unwrap();
        let cf = db
            .create_column_family(
                "default",
                ColumnFamilyConfig {
                    write_buffer_size: 32 * 1024, // tiny -> many flushes
                    l1_file_count_trigger: 2,     // frequent compactions
                    ..ColumnFamilyConfig::default()
                },
            )
            .unwrap();
        for i in 0..n {
            db.put(&cf, format!("k{i:08}").as_bytes(), b"value", Duration::ZERO)
                .unwrap();
        }
        db.backup(backup.path()).unwrap();
        db.close().unwrap();
    }
    let db = DB::open(Options::new(backup.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").expect("cf in backup");
    for i in 0..n {
        assert_eq!(
            db.get(&cf, format!("k{i:08}").as_bytes()).unwrap(),
            b"value",
            "missing k{i} in a backup taken under paced deletion"
        );
    }
    db.close().unwrap();
}

/// A limiter that makes every `ObsoleteDelete` take a fixed, real interval, and
/// deliberately ignores `cancel`.
///
/// Close cancels the limiter first thing, which normally lets the queue drain
/// at full speed — and at full speed a "did close wait for the queue?" test
/// only ever races the worker and passes either way. An embedder's limiter is
/// under no obligation to honour `cancel` instantly, so this one does not, and
/// close is then forced to actually wait for the tail of the queue.
#[derive(Debug)]
struct SlowDeleter {
    per_file: Duration,
    deletes: AtomicU64,
}

impl SlowDeleter {
    fn new(per_file: Duration) -> Arc<SlowDeleter> {
        Arc::new(SlowDeleter {
            per_file,
            deletes: AtomicU64::new(0),
        })
    }
    fn deletes(&self) -> u64 {
        self.deletes.load(AtOrd::SeqCst)
    }
}

impl IoLimiter for SlowDeleter {
    fn charge(&self, class: IoClass, _bytes: u64) {
        // Only deletion is slowed: flush and compaction charge thousands of
        // blocks, and delaying those would measure the wrong thing entirely.
        if class == IoClass::ObsoleteDelete {
            self.deletes.fetch_add(1, AtOrd::SeqCst);
            std::thread::sleep(self.per_file);
        }
    }
}

#[test]
fn close_drains_deletion_queue_before_lock_release() {
    // Deferred deletes already had to finish before the directory lock went
    // away; the worker's queue is the same obligation. A queue abandoned at
    // close would leave files no manifest names, which the next open's orphan
    // sweep would then have to guess about.
    let dir = tempfile::tempdir().unwrap();
    let per_file = Duration::from_millis(20);
    let slow = SlowDeleter::new(per_file);
    let mut options = limited_options(dir.path(), slow.clone());
    options.obsolete_delete_bytes_per_second = 1 << 20; // any rate: spawns the worker
    let db = DB::open(options).unwrap();
    let cf = stacked_l0(&db, 40);
    let cf_dir = dir.path().join("cf-default");
    let obsolete = sst_files(&cf_dir);
    assert!(obsolete.len() >= 40);
    db.compact(&cf).unwrap();

    let started = Instant::now();
    db.close().unwrap();
    let closed_in = started.elapsed();
    // Read the filesystem *after* close returned and before anything else: if
    // the queue outlived close, these files are still here.
    let left = obsolete.iter().filter(|p| p.exists()).count();

    assert_eq!(
        left, 0,
        "close must drain the deletion queue; {left} files survived it"
    );
    // Close cannot have raced the worker to that result: the queue costs at
    // least this much wall-clock time to get through.
    let queued = slow.deletes();
    assert!(queued >= obsolete.len() as u64);
    assert!(
        closed_in >= per_file.mul_f64(obsolete.len() as f64 * 0.5),
        "close returned in {closed_in:?}, too fast to have waited for \
         {queued} paced unlinks"
    );
    // The lock is released only after that drain, so a fresh open must succeed.
    let reopened = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert!(reopened.get_column_family("default").is_some());
    reopened.close().unwrap();
}
