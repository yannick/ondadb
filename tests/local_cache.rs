//! The local disk cache in front of remote tiers (`Options::local_cache_path`,
//! plan C P8), end to end through a database.
//!
//! The "remote" tier is a `TierDef::custom` backend over a local directory
//! that counts every positional read reaching it — the shape of an S3 tier
//! (no mmap, every read a range GET) without an object store. The S3-backed
//! variant of these tests lives in `tests/s3_tier.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ondadb::cache::FileCache;
use ondadb::storage::{ReadHandle, StorageWriter};
use ondadb::{
    ColumnFamilyConfig, LocalStorage, Options, PartitionRule, Storage, TierDef, TierRule, DB,
};

const ZERO: Duration = Duration::ZERO;
const KEYS: usize = 800;

#[derive(Debug)]
struct Counting {
    inner: Arc<LocalStorage>,
    table_reads: Arc<AtomicUsize>,
}

struct CountingHandle {
    inner: Arc<dyn ReadHandle>,
    table: bool,
    reads: Arc<AtomicUsize>,
}

impl ReadHandle for CountingHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> ondadb::Result<()> {
        if self.table {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.read_exact_at(buf, offset)
    }
    fn size(&self) -> ondadb::Result<u64> {
        self.inner.size()
    }
}

impl Storage for Counting {
    fn open_read(&self, path: &str) -> ondadb::Result<Arc<dyn ReadHandle>> {
        Ok(Arc::new(CountingHandle {
            inner: self.inner.open_read(path)?,
            table: path.ends_with(".klog") || path.ends_with(".vlog"),
            reads: self.table_reads.clone(),
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

struct Env {
    db_dir: tempfile::TempDir,
    tier_dir: tempfile::TempDir,
    cache_dir: tempfile::TempDir,
    reads: Arc<AtomicUsize>,
}

impl Env {
    fn new() -> Env {
        Env {
            db_dir: tempfile::tempdir().unwrap(),
            tier_dir: tempfile::tempdir().unwrap(),
            cache_dir: tempfile::tempdir().unwrap(),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn options(&self, cache: bool) -> Options {
        let storage = Arc::new(Counting {
            inner: LocalStorage::new(Arc::new(FileCache::new(16)), false),
            table_reads: self.reads.clone(),
        });
        let mut o = Options::new(self.db_dir.path().to_str().unwrap());
        o.tiers = vec![TierDef::custom(
            "remote",
            self.tier_dir.path().to_str().unwrap(),
            storage,
        )];
        o.part_mover_interval = ZERO;
        // Memory caches off: every read of a table goes to the tier, so the
        // disk cache is the only thing that can spare the backend.
        o.block_cache_size = 0;
        if cache {
            o.local_cache_path = Some(self.cache_dir.path().to_str().unwrap().into());
        }
        o
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("img/{i:05}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    // Every 10th value is large enough to live in the vlog.
    let n = if i.is_multiple_of(10) { 2000 } else { 40 };
    let mut v = format!("v{i:05}-").into_bytes();
    v.resize(n, b'x');
    v
}

fn populate(env: &Env) {
    let db = DB::open(env.options(true)).unwrap();
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
                    tier: "remote".into(),
                    min_age: ZERO,
                }],
                l1_file_count_trigger: 1,
                data_block_size: 1024,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for i in 0..KEYS {
        db.put(&cf, &key(i), &value(i), ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(db.run_part_mover().unwrap(), 1);
    db.close().unwrap();
}

fn read_all(db: &DB) {
    let cf = db.get_column_family("default").unwrap();
    for i in 0..KEYS {
        assert_eq!(db.get(&cf, &key(i)).unwrap(), value(i), "key {i}");
    }
}

#[test]
fn a_restart_reads_from_the_disk_cache_not_the_tier() {
    let env = Env::new();
    populate(&env);

    let db = DB::open(env.options(true)).unwrap();
    let before = env.reads();
    read_all(&db); // cold: the tier serves, the cache admits
    let cold = env.reads() - before;
    assert!(cold > 10, "only {cold} tier reads for a cold pass");
    let st = db.local_cache_stats().unwrap();
    assert!(st.admits > 0 && st.entries > 0, "{st:?}");
    db.close().unwrap();
    drop(db);

    // A new process image: memory caches are gone, the disk cache is not.
    let db = DB::open(env.options(true)).unwrap();
    let before = env.reads();
    read_all(&db);
    assert_eq!(env.reads() - before, 0, "a warm restart went to the tier");
    let st = db.local_cache_stats().unwrap();
    assert!(st.hits > 0, "{st:?}");
    db.close().unwrap();
}

#[test]
fn without_the_option_every_read_goes_to_the_tier() {
    let env = Env::new();
    populate(&env);
    let db = DB::open(env.options(false)).unwrap();
    assert!(db.local_cache_stats().is_none());
    let before = env.reads();
    read_all(&db);
    let first = env.reads() - before;
    let before = env.reads();
    read_all(&db);
    // The first pass also opened the reader (footer, index, bloom); the second
    // still reads every block from the tier.
    assert!(env.reads() - before >= first - 8, "no cache, no savings");
    assert!(
        env.reads() - before >= KEYS,
        "a pass read fewer blocks than keys"
    );
}

/// Crash safety: every cache file damaged between runs (cut short, or a byte
/// flipped — what a crash mid-admission or a bad disk leaves) must read as a
/// miss. Answers stay correct; the tier serves them; the entries heal.
#[test]
fn damaged_entries_are_misses_never_wrong_answers() {
    let env = Env::new();
    populate(&env);
    {
        let db = DB::open(env.options(true)).unwrap();
        read_all(&db);
        db.close().unwrap();
    }
    let mut files = Vec::new();
    for shard in std::fs::read_dir(env.cache_dir.path()).unwrap().flatten() {
        for f in std::fs::read_dir(shard.path()).unwrap().flatten() {
            files.push(f.path());
        }
    }
    assert!(files.len() > 10);
    for (n, f) in files.iter().enumerate() {
        let mut b = std::fs::read(f).unwrap();
        if n.is_multiple_of(2) {
            b.truncate(b.len() / 2);
        } else {
            let at = b.len() - 7;
            b[at] ^= 0x01;
        }
        std::fs::write(f, b).unwrap();
    }
    let db = DB::open(env.options(true)).unwrap();
    let before = env.reads();
    read_all(&db);
    assert!(env.reads() > before, "damaged entries were served");
    let st = db.local_cache_stats().unwrap();
    assert!(st.corrupt as usize >= files.len() / 2, "{st:?}");
    db.close().unwrap();
    // Healed: the next run is warm again.
    let db = DB::open(env.options(true)).unwrap();
    let before = env.reads();
    read_all(&db);
    assert_eq!(env.reads() - before, 0);
}

/// Two incarnations of one directory — the database wiped and re-created, its
/// table ids restarting — must not share entries, even though the tier paths
/// repeat.
#[test]
fn a_recreated_database_does_not_see_the_old_entries() {
    let env = Env::new();
    populate(&env);
    {
        let db = DB::open(env.options(true)).unwrap();
        read_all(&db);
        db.close().unwrap();
    }
    // Wipe both the database and its tier objects, then build a new database
    // at the same place whose values differ.
    for d in [env.db_dir.path(), env.tier_dir.path()] {
        for e in std::fs::read_dir(d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                std::fs::remove_dir_all(p).unwrap();
            } else {
                std::fs::remove_file(p).unwrap();
            }
        }
    }
    std::thread::sleep(Duration::from_millis(5));
    let db = DB::open(env.options(true)).unwrap();
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
                    tier: "remote".into(),
                    min_age: ZERO,
                }],
                l1_file_count_trigger: 1,
                data_block_size: 1024,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for i in 0..KEYS {
        db.put(&cf, &key(i), b"second-incarnation", ZERO).unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    assert_eq!(db.run_part_mover().unwrap(), 1);
    for i in (0..KEYS).step_by(7) {
        assert_eq!(db.get(&cf, &key(i)).unwrap(), b"second-incarnation");
    }
    db.close().unwrap();
}
