//! Tests for checkpoint, backup, clone, and stats.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, OndaError, Options, PartitionRule, TierDef, DB};

fn fill(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, n: u32) {
    fill_range(db, cf, 0, n);
}

fn fill_range(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, from: u32, to: u32) {
    for i in from..to {
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

/// Copy a live database directory: the image a crash would leave, with
/// committed writes present only in the WAL.
fn copy_tree(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// S4: a read-only open replays the WAL into memtables but runs no flush
/// worker, so the checkpoint's flush cannot move that data into a table. The
/// snapshot must still carry it — without writing a byte into the read-only
/// source.
#[test]
fn read_only_checkpoint_and_backup_keep_wal_only_data() {
    for unified in [false, true] {
        let src = tempfile::tempdir().unwrap();
        let crashed = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        let options = |path: &std::path::Path, read_only: bool| {
            let mut o = Options::new(path.to_str().unwrap());
            o.unified_memtable = unified;
            o.read_only = read_only;
            o
        };
        {
            let db = DB::open(options(src.path(), false)).unwrap();
            let cf = db
                .create_column_family("default", ColumnFamilyConfig::default())
                .unwrap();
            let other = db
                .create_column_family("other", ColumnFamilyConfig::default())
                .unwrap();
            fill_range(&db, &cf, 0, 100);
            db.flush_memtable(&cf).unwrap();
            // Everything from here on lives only in the WAL.
            fill_range(&db, &cf, 100, 200);
            db.put(&cf, b"k00050", b"newer", Duration::ZERO).unwrap();
            db.delete(&cf, b"k00010").unwrap();
            // A range delete over flushed and WAL-only keys alike: the
            // snapshot's table must carry the fragment, not just points.
            db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
                .unwrap();
            db.delete_range(&cf, b"k00090", b"k00110").unwrap();
            db.put(&other, b"o", b"only-in-wal", Duration::ZERO)
                .unwrap();
            db.sync_wal().unwrap();
            copy_tree(src.path(), crashed.path());
            db.close().unwrap();
        }
        let before: Vec<String> = walk(crashed.path());

        let db = DB::open(options(crashed.path(), true)).unwrap();
        let cf = db.get_column_family("default").unwrap();
        assert_eq!(
            db.get(&cf, b"k00150").unwrap(),
            b"value",
            "unified={unified}"
        );
        db.checkpoint(dest.path().join("ckpt")).unwrap();
        db.backup(dest.path().join("bk")).unwrap();
        db.close().unwrap();
        assert_eq!(
            walk(crashed.path()),
            before,
            "unified={unified}: the read-only source must not change"
        );

        for name in ["ckpt", "bk"] {
            let path = dest.path().join(name);
            for read_only in [true, false] {
                let db = DB::open(options(&path, read_only)).unwrap();
                let cf = db.get_column_family("default").unwrap();
                let other = db.get_column_family("other").unwrap();
                for i in 0..200u32 {
                    let key = format!("k{i:05}");
                    let got = db.get(&cf, key.as_bytes());
                    match i {
                        10 | 90..=109 => assert!(
                            matches!(got, Err(OndaError::NotFound)),
                            "{name} unified={unified}: deleted key came back: {got:?}"
                        ),
                        50 => assert_eq!(got.unwrap(), b"newer", "{name} unified={unified}"),
                        _ => assert_eq!(
                            got.unwrap_or_else(|e| panic!(
                                "{name} unified={unified} read_only={read_only}: {key} lost: {e:?}"
                            )),
                            b"value"
                        ),
                    }
                }
                assert_eq!(db.get(&other, b"o").unwrap(), b"only-in-wal");
                if !read_only {
                    // A writable open must not reuse a sequence the snapshot's
                    // tables already hold: a new write has to win.
                    db.put(&cf, b"k00050", b"newest", Duration::ZERO).unwrap();
                    assert_eq!(db.get(&cf, b"k00050").unwrap(), b"newest");
                }
                db.close().unwrap();
            }
        }
    }
}

/// Every file path under `dir` with its size, sorted.
fn walk(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            out.extend(walk(&entry.path()));
        } else {
            let len = entry.metadata().unwrap().len();
            out.push(format!("{} {len}", entry.path().display()));
        }
    }
    out.sort();
    out
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

// ---------------------------------------------------------------------------
// 0.3 — periodic compaction with durable age state
// ---------------------------------------------------------------------------

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};

use ondadb::format::CAP_PERIODIC_AGE;
use ondadb::manifest::{manifest_path, Manifest};

/// A fixed base reading, far from any real clock, so a stamp taken from the
/// fake can never be confused for one taken from `now_nanos`.
const T0: i64 = 1_000_000_000_000;
/// The interval every periodic test below configures. Four seconds derives a
/// one-second scan cadence — the floor — which is what bounds these tests'
/// wall-clock cost while leaving eligibility entirely on the fake clock.
const INTERVAL: Duration = Duration::from_secs(4);

/// Install a clock the test drives, returning the handle that moves it.
fn fake_clock(db: &DB) -> Arc<AtomicI64> {
    let now = Arc::new(AtomicI64::new(T0));
    let handle = now.clone();
    db.set_clock_for_tests(Arc::new(move || handle.load(Ordering::SeqCst)));
    now
}

fn periodic_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        periodic_compaction_interval: INTERVAL,
        l1_file_count_trigger: 2,
        ..ColumnFamilyConfig::default()
    }
}

/// Every table's age state, straight out of the persisted catalog.
fn persisted_stamps(dir: &Path) -> Vec<Option<i64>> {
    Manifest::load(manifest_path(dir))
        .unwrap()
        .cfs
        .iter()
        .flat_map(|cf| {
            cf.sstables
                .iter()
                .map(|s| s.last_compaction_time)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Total on-disk bytes of a database's SSTables.
fn sst_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "klog" || x == "vlog") {
                total += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

/// Poll `f` until it holds or the deadline passes. Periodic work runs on the
/// compaction worker's own cadence, so the test waits for the effect rather
/// than assuming a timing.
fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    f()
}

/// Make the next manifest write fail, by occupying the temp path
/// `Manifest::save` needs. Returns the path so the caller can free it again.
fn block_manifest_writes(dir: &Path) -> std::path::PathBuf {
    let tmp = manifest_path(dir).with_extension("tmp");
    std::fs::create_dir(&tmp).expect("MANIFEST.tmp must not already exist");
    tmp
}

fn shared_tier_options(dir: &Path, shared_root: &Path) -> Options {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.tiers = vec![TierDef::new("cas", shared_root.to_str().unwrap().to_string()).shared()];
    opts
}

/// Task 2: the enable transition stamps every local, non-mounted table in the
/// same manifest write that persists the capability — and stamps nothing else.
#[test]
fn periodic_enable_stamps_local_tables_once() {
    let shared = tempfile::tempdir().unwrap();
    let publisher = tempfile::tempdir().unwrap();
    let sharer = tempfile::tempdir().unwrap();

    // A published part on a shared tier, so the sharer below has something to
    // mount by reference — a table it may never rewrite and must never stamp.
    let shared_cfg = ColumnFamilyConfig {
        partition_rules: vec![PartitionRule {
            prefix: b"img/".to_vec(),
            name: "img".into(),
        }],
        min_levels: 2,
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    };
    let db1 = DB::open(shared_tier_options(publisher.path(), shared.path())).unwrap();
    let cf1 = db1
        .create_column_family("default", shared_cfg.clone())
        .unwrap();
    for i in 0..8u32 {
        db1.put(
            &cf1,
            format!("img/{i:03}").as_bytes(),
            b"IMG",
            Duration::ZERO,
        )
        .unwrap();
    }
    db1.flush_memtable(&cf1).unwrap();
    db1.compact(&cf1).unwrap();
    db1.move_part_to_tier(&cf1, "img", "cas").unwrap();
    let part = db1.export_part(&cf1, "img").unwrap();
    drop(cf1);
    db1.close().unwrap();

    let db = DB::open(shared_tier_options(sharer.path(), shared.path())).unwrap();
    let cf = db.create_column_family("default", shared_cfg).unwrap();
    db.attach_part_by_ref(&cf, &part, "cas").unwrap();
    // Two local tables of the sharer's own.
    for i in 0..4u32 {
        db.put(
            &cf,
            format!("log/{i:03}").as_bytes(),
            b"LOG",
            Duration::ZERO,
        )
        .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    for i in 4..8u32 {
        db.put(
            &cf,
            format!("log/{i:03}").as_bytes(),
            b"LOG",
            Duration::ZERO,
        )
        .unwrap();
    }
    db.flush_memtable(&cf).unwrap();

    let clock = fake_clock(&db);
    assert!(
        persisted_stamps(sharer.path()).iter().all(Option::is_none),
        "nothing carries age state before the capability exists"
    );
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();

    let manifest = Manifest::load(manifest_path(sharer.path())).unwrap();
    assert_eq!(manifest.caps & CAP_PERIODIC_AGE, CAP_PERIODIC_AGE);
    let tables = &manifest.cfs[0].sstables;
    let mounted: Vec<_> = tables.iter().filter(|t| t.object.is_some()).collect();
    let local: Vec<_> = tables.iter().filter(|t| t.object.is_none()).collect();
    assert_eq!(mounted.len(), 1, "one mounted part table");
    assert!(local.len() >= 2, "the sharer's own tables: {}", local.len());
    assert!(
        local.iter().all(|t| t.last_compaction_time == Some(T0)),
        "every local table takes the enable time in the same manifest write"
    );
    assert!(
        mounted.iter().all(|t| t.last_compaction_time.is_none()),
        "a foreign mount is never stamped and never eligible"
    );

    // Re-enabling is a no-op: the bit is already active, so nothing is
    // re-stamped even though the clock has moved on.
    clock.store(T0 + 100 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();
    let after = Manifest::load(manifest_path(sharer.path())).unwrap();
    assert_eq!(
        after.cfs[0]
            .sstables
            .iter()
            .map(|t| t.last_compaction_time)
            .collect::<Vec<_>>(),
        tables
            .iter()
            .map(|t| t.last_compaction_time)
            .collect::<Vec<_>>(),
        "a second enable must not re-stamp"
    );
    drop(cf);
    db.close().unwrap();
}

/// Failure matrix, row 1: a crash during the enable-time stamping leaves the
/// old manifest. Reopen shows neither the capability nor any stamp, and the
/// enable can simply be retried.
#[test]
fn periodic_enable_crash() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("default", periodic_cfg()).unwrap();
        fill(&db, &cf, 32);
        db.flush_memtable(&cf).unwrap();
        assert!(
            !persisted_stamps(dir.path()).is_empty(),
            "one flushed table"
        );

        let _clock = fake_clock(&db);
        let blocked = block_manifest_writes(dir.path());
        let error = db
            .enable_format_capabilities(CAP_PERIODIC_AGE)
            .expect_err("the manifest write cannot succeed");
        assert!(
            !matches!(error, OndaError::InvalidArgs(_)),
            "a durability failure, not a caller error: {error:?}"
        );
        assert_eq!(
            db.format_capabilities(),
            0,
            "the bit is not active when its manifest write failed"
        );
        assert!(db.poisoned().is_some(), "a failed persist fail-stops");
        drop(cf);
        let _ = db.close();
        std::fs::remove_dir(&blocked).unwrap();
    }

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(db.format_capabilities(), 0, "no capability survived");
    assert!(
        persisted_stamps(dir.path()).iter().all(Option::is_none),
        "no stamp survived either"
    );
    // The retry is clean.
    let cf = db.get_column_family("default").unwrap();
    let clock = fake_clock(&db);
    clock.store(T0, Ordering::SeqCst);
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();
    assert!(persisted_stamps(dir.path()).iter().all(|s| *s == Some(T0)));
    drop(cf);
    db.close().unwrap();
}

/// Failure matrix, row 2: stamps are durable. A database restarted more often
/// than its interval still becomes eligible, because the clock the trigger
/// reads is on disk rather than in the process.
#[test]
fn periodic_stamp_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        // No compaction workers: this test is about the persisted state, not
        // about the scheduler acting on it.
        let mut opts = Options::new(dir.path().to_str().unwrap());
        opts.num_compaction_threads = 1;
        let db = DB::open(opts).unwrap();
        let cf = db.create_column_family("default", periodic_cfg()).unwrap();
        let _clock = fake_clock(&db);
        fill(&db, &cf, 32);
        db.flush_memtable(&cf).unwrap();
        db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();
        drop(cf);
        db.close().unwrap();
    }
    let stamps = persisted_stamps(dir.path());
    assert!(!stamps.is_empty());
    assert!(stamps.iter().all(|s| *s == Some(T0)), "{stamps:?}");

    // Reopen: the capability comes back with the manifest, the stamps with it,
    // and one interval past the STAMP (not past this open) makes it eligible.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(db.format_capabilities(), CAP_PERIODIC_AGE);
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(cf.stats().periodic_compactions, 0);
    let clock = fake_clock(&db);
    clock.store(T0 + 10 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);
    assert!(
        wait_until(Duration::from_secs(20), || cf.stats().periodic_compactions
            >= 1),
        "a stamp that predates this open by an interval is eligible now"
    );
    let after = persisted_stamps(dir.path());
    assert!(
        after
            .iter()
            .all(|s| *s == Some(T0 + 10 * INTERVAL.as_nanos() as i64)),
        "the rewrite re-stamps at the current reading: {after:?}"
    );
    drop(cf);
    db.close().unwrap();
}

/// Failure matrix, row 3: a periodic job whose manifest write fails rolls back
/// exactly like any other compaction — the reopened database shows the old
/// view, and the table keeps its old stamp, so it stays eligible.
#[test]
fn periodic_job_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let blocked;
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db.create_column_family("default", periodic_cfg()).unwrap();
        let clock = fake_clock(&db);
        fill(&db, &cf, 64);
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();

        blocked = block_manifest_writes(dir.path());
        clock.store(T0 + 10 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);
        assert!(
            wait_until(Duration::from_secs(20), || cf.stats().compaction_failures
                >= 1),
            "the periodic job's manifest write must fail"
        );
        assert_eq!(
            cf.stats().periodic_compactions,
            0,
            "a failed job is not counted"
        );
        drop(cf);
        let _ = db.close();
    }
    std::fs::remove_dir(&blocked).unwrap();

    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("default").unwrap();
    // The old view: the pre-job stamp, so the table is still eligible.
    let stamps = persisted_stamps(dir.path());
    assert!(
        stamps.iter().all(|s| *s == Some(T0)),
        "the failed job published nothing: {stamps:?}"
    );
    for i in 0..64u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:05}").as_bytes()).unwrap(),
            b"value",
            "no data was lost by the rollback"
        );
    }
    // And it is picked up again, now that the manifest can be written.
    let clock = fake_clock(&db);
    clock.store(T0 + 20 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);
    assert!(
        wait_until(Duration::from_secs(20), || cf.stats().periodic_compactions
            >= 1),
        "the table stayed eligible and the retry succeeds"
    );
    drop(cf);
    db.close().unwrap();
}

/// Task 7: a bottom-level table is rewritten **in place**. No level is created
/// for age reasons alone.
#[test]
fn periodic_rewrites_bottom_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", periodic_cfg()).unwrap();
    let clock = fake_clock(&db);
    fill(&db, &cf, 200);
    db.flush_memtable(&cf).unwrap();
    // Sweep everything to the bottom, so the only eligible table below is a
    // bottom one and the pick must be the in-place shape.
    db.compact(&cf).unwrap();
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();

    let levels_before = cf.stats().num_levels;
    let bottom_before: Vec<u64> = Manifest::load(manifest_path(dir.path())).unwrap().cfs[0]
        .sstables
        .iter()
        .map(|t| t.id)
        .collect();
    assert!(!bottom_before.is_empty());

    clock.store(T0 + 10 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);
    assert!(
        wait_until(Duration::from_secs(20), || cf.stats().periodic_compactions
            >= 1),
        "an aged bottom table must be revisited"
    );

    assert_eq!(
        cf.stats().num_levels,
        levels_before,
        "an age trigger must never create a deeper level"
    );
    let manifest = Manifest::load(manifest_path(dir.path())).unwrap();
    let after: Vec<u64> = manifest.cfs[0].sstables.iter().map(|t| t.id).collect();
    assert_ne!(after, bottom_before, "the table really was rewritten");
    assert!(
        manifest.cfs[0]
            .sstables
            .iter()
            .all(|t| t.level as usize == levels_before - 1),
        "the rewrite stayed in the bottom level"
    );
    for i in 0..200u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:05}").as_bytes()).unwrap(),
            b"value"
        );
    }
    drop(cf);
    db.close().unwrap();
}

/// Task 9: the counter distinguishes age work from capacity work.
#[test]
fn periodic_compactions_counter_increments() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", periodic_cfg()).unwrap();
    let clock = fake_clock(&db);

    // Capacity work first: enough L0 files to trip the file-count trigger.
    for round in 0..6u32 {
        fill_range(&db, &cf, round * 100, (round + 1) * 100);
        db.flush_memtable(&cf).unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(20), || cf.stats().compaction_count >= 1),
        "the L0 trigger must produce capacity work"
    );
    assert_eq!(
        cf.stats().periodic_compactions,
        0,
        "capacity work is never counted as periodic"
    );

    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();
    clock.store(T0 + 10 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);
    assert!(
        wait_until(Duration::from_secs(20), || cf.stats().periodic_compactions
            >= 1),
        "one aged table takes the counter to one"
    );
    assert!(
        cf.stats().compaction_count >= cf.stats().periodic_compactions,
        "periodic jobs are a subset of all compactions"
    );
    drop(cf);
    db.close().unwrap();
}

/// Task 10 (acceptance): an idle database with no writes at all reclaims the
/// space its expired TTL entries hold — and then stops, rather than looping.
///
/// The fixture is deliberately quiescent: one flushed L0 file, a file-count
/// trigger it cannot reach, and no manual compaction. Nothing here produces
/// capacity work, so every compaction after the enable has to be the age
/// trigger's — which is asserted, not assumed.
#[test]
fn idle_ttl_database_reclaims_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                periodic_compaction_interval: INTERVAL,
                // One L0 file can never reach this, so no capacity trigger can
                // fire while the age trigger is under test.
                l1_file_count_trigger: 16,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let clock = fake_clock(&db);

    // TTL is evaluated on the REAL clock (`now_nanos`), deliberately: the fake
    // drives the periodic trigger only. So the data is written with a genuine
    // short TTL and the test waits it out once.
    let payload = vec![b'x'; 512];
    for i in 0..1000u32 {
        db.put(
            &cf,
            format!("exp{i:05}").as_bytes(),
            &payload,
            Duration::from_secs(3),
        )
        .unwrap();
    }
    for i in 0..100u32 {
        db.put(
            &cf,
            format!("keep{i:05}").as_bytes(),
            &payload,
            Duration::ZERO,
        )
        .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();

    let compactions_before = cf.stats().compaction_count;
    let bytes_before = sst_bytes(dir.path());
    let entries_before = cf.stats().num_entries;
    assert_eq!(entries_before, 1100, "the whole fixture is on disk");
    assert!(bytes_before > 0);

    // No writes from here on. Wait for the TTL to actually expire on the real
    // clock, then move the periodic clock past the interval.
    std::thread::sleep(Duration::from_secs(4));
    assert_eq!(
        cf.stats().num_entries,
        entries_before,
        "nothing reclaims before the age trigger fires — that is the gap this \
         feature exists to close"
    );
    clock.store(T0 + 10 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);

    assert!(
        wait_until(Duration::from_secs(30), || {
            cf.stats().periodic_compactions >= 1
                && cf.stats().num_entries <= 100
                && sst_bytes(dir.path()) < bytes_before / 2
        }),
        "an idle database must reclaim expired TTL space: {} bytes / {} entries -> {} / {}",
        bytes_before,
        entries_before,
        sst_bytes(dir.path()),
        cf.stats().num_entries
    );
    assert_eq!(
        cf.stats().compaction_count - compactions_before,
        cf.stats().periodic_compactions,
        "every compaction since the enable was age-triggered, so the reclaim \
         is this feature's and not a trigger that would have fired anyway"
    );

    // The surviving data is intact...
    for i in 0..100u32 {
        assert_eq!(
            db.get(&cf, format!("keep{i:05}").as_bytes()).unwrap(),
            payload
        );
    }
    // ...and the trigger settles: the rewrite re-stamped its output, so a
    // stationary clock produces no further work. This is the "no repeated
    // immediate job loop" half of the acceptance criterion.
    let settled = cf.stats().periodic_compactions;
    std::thread::sleep(Duration::from_secs(5));
    let later = cf.stats().periodic_compactions;
    assert_eq!(
        later, settled,
        "periodic work must not loop on its own output: {settled} -> {later}"
    );
    drop(cf);
    db.close().unwrap();
}

/// Task 10, second half: the trigger adds **no new drop rule**. Data hidden
/// behind a live snapshot is retained exactly as it is under capacity work.
#[test]
fn snapshots_retain_hidden_data_under_periodic() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", periodic_cfg()).unwrap();
    let clock = fake_clock(&db);

    for i in 0..100u32 {
        db.put(&cf, format!("k{i:05}").as_bytes(), b"old", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    let mut snapshot = db.begin();
    assert_eq!(snapshot.get(&cf, b"k00000").unwrap(), b"old");

    // Shadow every key and delete half, then push it all to the bottom.
    for i in 0..100u32 {
        let key = format!("k{i:05}");
        if i % 2 == 0 {
            db.delete(&cf, key.as_bytes()).unwrap();
        } else {
            db.put(&cf, key.as_bytes(), b"new", Duration::ZERO).unwrap();
        }
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();

    clock.store(T0 + 10 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);
    assert!(
        wait_until(Duration::from_secs(20), || cf.stats().periodic_compactions
            >= 1),
        "the aged bottom table is revisited"
    );

    // The snapshot still sees what it saw. A periodic rewrite is an ordinary
    // compaction: it may drop nothing the oldest live snapshot can still read.
    for i in 0..100u32 {
        assert_eq!(
            snapshot.get(&cf, format!("k{i:05}").as_bytes()).unwrap(),
            b"old",
            "key {i} was hidden from the snapshot by a periodic rewrite"
        );
    }
    drop(snapshot);
    drop(cf);
    db.close().unwrap();
}

/// The one wall-clock test: no fake anywhere, a two-second interval, and the
/// engine's own clock. Everything above pins semantics; this pins that the
/// wiring works when nothing is injected.
#[test]
fn periodic_reclaims_on_the_real_clock() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                periodic_compaction_interval: Duration::from_secs(2),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    fill(&db, &cf, 100);
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();
    assert!(
        wait_until(Duration::from_secs(30), || cf.stats().periodic_compactions
            >= 1),
        "the real clock must reach the interval on its own"
    );
    for i in 0..100u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:05}").as_bytes()).unwrap(),
            b"value"
        );
    }
    drop(cf);
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// 2.2 — checkpoint and backup materialize a fresh snapshot with no edit log
// ---------------------------------------------------------------------------

/// A database with `CAP_MANIFEST_EDITS` enabled, one CF, and data on disk.
fn edits_source(dir: &std::path::Path) -> (DB, std::sync::Arc<ondadb::ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
        .unwrap();
    fill(&db, &cf, 64);
    db.flush_memtable(&cf).unwrap();
    (db, cf)
}

fn assert_fresh_snapshot_only(dest: &std::path::Path) {
    assert!(
        !ondadb::manifest_edit::edit_log_path(dest).exists(),
        "the destination is snapshot-only: it grows a log when it is first \
         opened writable and mutated, not before"
    );
    let m = ondadb::manifest_edit::recover_catalog(dest).unwrap();
    assert_eq!(m.generation, 1, "a fresh generation, not the source's");
    assert_eq!(m.applied_through, 0);
    assert_eq!(m.next_edit_id, 1);
}

#[test]
fn checkpoint_writes_a_fresh_snapshot_with_no_edit_log() {
    let dir = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let (db, cf) = edits_source(dir.path());
    assert!(ondadb::manifest_edit::edit_log_path(dir.path()).exists());
    db.checkpoint(dest.path().join("cp")).unwrap();
    drop(cf);
    db.close().unwrap();
    assert_fresh_snapshot_only(&dest.path().join("cp"));

    let back = DB::open(Options::new(dest.path().join("cp").to_str().unwrap())).unwrap();
    let cf = back.get_column_family("default").unwrap();
    assert_eq!(back.get(&cf, b"k00000").unwrap(), b"value");
    back.close().unwrap();
}

#[test]
fn backup_writes_a_fresh_snapshot_with_no_edit_log() {
    let dir = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let (db, cf) = edits_source(dir.path());
    db.backup(dest.path().join("bk")).unwrap();
    drop(cf);
    db.close().unwrap();
    assert_fresh_snapshot_only(&dest.path().join("bk"));

    let back = DB::open(Options::new(dest.path().join("bk").to_str().unwrap())).unwrap();
    let cf = back.get_column_family("default").unwrap();
    assert_eq!(back.get(&cf, b"k00063").unwrap(), b"value");
    back.close().unwrap();
}

/// A read-only source cannot force a snapshot compaction, so `snapshot_to`
/// must replay the edit log into the catalog it copies. Without that, every
/// edit since the last compaction is silently dropped and the
/// "read-only-capable backup" claim is false.
#[test]
fn backup_from_a_read_only_db_includes_uncompacted_edits() {
    let dir = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    {
        let (db, cf) = edits_source(dir.path());
        drop(cf);
        db.close().unwrap();
    }
    // Append an edit the on-disk snapshot does not contain — a partition
    // restamp, which needs no new file, so the only thing under test is whether
    // the backup replayed the log or stopped at the snapshot.
    let before = ondadb::manifest_edit::recover_catalog(dir.path()).unwrap();
    let table_id = before.cfs[0].sstables[0].id;
    let mut log = ondadb::manifest_edit::open_log_for_append(dir.path())
        .unwrap()
        .expect("the source has a log");
    log.append(
        before.next_edit_id,
        &ondadb::manifest_edit::VersionEdit::new(vec![ondadb::manifest_edit::Op::UpdateTable {
            cf: "default".into(),
            id: table_id,
            update: ondadb::manifest_edit::TableUpdate {
                partition: Some(Some("uncompacted".into())),
                ..Default::default()
            },
        }]),
    )
    .unwrap();
    drop(log);

    let mut o = Options::new(dir.path().to_str().unwrap());
    o.read_only = true;
    let db = DB::open(o).unwrap();
    db.backup(dest.path().join("ro")).unwrap();
    db.close().unwrap();

    let copied = ondadb::manifest_edit::recover_catalog(dest.path().join("ro")).unwrap();
    assert_eq!(
        copied.cfs[0]
            .sstables
            .iter()
            .find(|s| s.id == table_id)
            .and_then(|s| s.partition.as_deref()),
        Some("uncompacted"),
        "the backup must carry the uncompacted edit, not just the snapshot"
    );
    assert_fresh_snapshot_only(&dest.path().join("ro"));
}

/// Guards RV-F1: the destination catalog must be self-contained, so every
/// table's tier and object annotation is cleared.
#[test]
fn backup_clears_tier_and_object_on_every_table() {
    let dir = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let db = DB::open(tiered_options(dir.path(), tier.path())).unwrap();
    let cf = materialize_tiered_partition(&db);
    db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
        .unwrap();
    db.backup(dest.path().join("bk")).unwrap();
    drop(cf);
    db.close().unwrap();
    let m = ondadb::manifest_edit::recover_catalog(dest.path().join("bk")).unwrap();
    for cfm in &m.cfs {
        for sst in &cfm.sstables {
            assert_eq!(sst.tier, None, "table {} keeps a tier", sst.id);
            assert_eq!(sst.object, None, "table {} keeps an object", sst.id);
        }
    }
    assert_fresh_snapshot_only(&dest.path().join("bk"));
}

/// A frozen part is a one-shot standalone artifact: a plain snapshot, never a
/// log, whatever the source database has enabled.
#[test]
fn frozen_part_has_no_edit_log() {
    let dir = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_MANIFEST_EDITS)
        .unwrap();
    fill(&db, &cf, 64);
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    let out = dest.path().join("frozen");
    let froze = db.freeze_part(&cf, "img", &out).is_ok();
    drop(cf);
    db.close().unwrap();
    if froze {
        assert!(
            !ondadb::manifest_edit::edit_log_path(&out).exists(),
            "a frozen slice is a plain snapshot with no log"
        );
        let m = ondadb::manifest_edit::recover_catalog(&out).unwrap();
        assert_eq!((m.generation, m.applied_through, m.next_edit_id), (0, 0, 1));
    }
}

/// `close`'s final persist is the closing snapshot compaction, and its failure
/// is the caller's (the prerequisite `close` fix, seen through 2.2's lens).
#[test]
fn close_compacts_the_log_and_reports_failure() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = edits_source(dir.path());
    // A structural change lands in the log...
    let before = ondadb::manifest_edit::recover_catalog(dir.path()).unwrap();
    let generation_before = before.generation;
    drop(cf);
    db.close().unwrap();
    // ...and close leaves a compacted snapshot with an empty log behind it.
    let after = ondadb::manifest_edit::recover_catalog(dir.path()).unwrap();
    assert!(
        after.generation > generation_before,
        "close ran a snapshot compaction"
    );
    let bytes = std::fs::read(ondadb::manifest_edit::edit_log_path(dir.path())).unwrap();
    assert_eq!(
        bytes.len(),
        ondadb::manifest_edit::EDIT_LOG_HEADER_BYTES,
        "the log restarts at its header"
    );
    assert_eq!(after.applied_through, after.next_edit_id - 1);
}

/// Tombstone-density trigger (plan C P4): a flush that leaves a table mostly
/// tombstones gets it compacted without any size trigger firing, the
/// tombstones are dropped at the bottom, and the setting survives a reopen.
fn delete_heavy(dir: &std::path::Path, trigger: f64) -> (DB, std::sync::Arc<ondadb::ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                tombstone_density_trigger: trigger,
                tombstone_density_min_entries: 100,
                // No size trigger: only the density trigger can compact.
                l1_file_count_trigger: 64,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    fill(&db, &cf, 1_000);
    db.flush_memtable(&cf).unwrap();
    for i in 0..900u32 {
        db.delete(&cf, format!("k{i:05}").as_bytes()).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    (db, cf)
}

#[test]
fn tombstone_density_trigger_reclaims_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = delete_heavy(dir.path(), 0.5);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    // The counter is bumped just after the job installs, so wait for both.
    while cf.stats().num_tombstones > 0 || cf.stats().tombstone_density_compactions == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "tombstones never reclaimed: {:?}",
            cf.stats()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let stats = cf.stats();
    assert!(stats.tombstone_density_compactions >= 1, "{stats:?}");
    assert_eq!(stats.num_entries, 100, "only the undeleted keys remain");
    assert!(db.get(&cf, b"k00000").is_err());
    assert_eq!(db.get(&cf, b"k00950").unwrap(), b"value");
    db.close().unwrap();

    // Persisted: a reopen keeps the trigger.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cfg = db.column_family_config("default").unwrap();
    assert_eq!(cfg.tombstone_density_trigger, 0.5);
    assert_eq!(cfg.tombstone_density_min_entries, 100);
    db.close().unwrap();
}

/// The control: with the trigger off the same workload keeps its tombstones,
/// so the test above is measuring the trigger and not some other compaction.
#[test]
fn without_the_density_trigger_tombstones_stay() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = delete_heavy(dir.path(), 0.0);
    std::thread::sleep(Duration::from_millis(300));
    let stats = cf.stats();
    assert_eq!(stats.num_tombstones, 900, "{stats:?}");
    assert_eq!(stats.tombstone_density_compactions, 0);
    db.close().unwrap();
}
