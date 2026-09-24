//! F6: the changed-table diff behind incremental backups.

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, DB};

fn put_n(db: &DB, cf: &std::sync::Arc<ondadb::ColumnFamily>, prefix: &str, n: u32) {
    for i in 0..n {
        db.put(
            cf,
            format!("{prefix}{i:03}").as_bytes(),
            b"v",
            Duration::ZERO,
        )
        .unwrap();
    }
}

/// wavesdb's `TestSSTablesSince`, ported: only the second flush's table holds
/// writes made after `seq1`, each result names its family, and "since 0" is
/// every table.
#[test]
fn sstables_since_reports_tables_holding_newer_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();

    put_n(&db, &cf, "a", 50);
    db.flush_memtable(&cf).unwrap();
    let seq1 = db.live_sstables().iter().map(|t| t.max_seq).max().unwrap();

    put_n(&db, &cf, "b", 50);
    db.flush_memtable(&cf).unwrap();

    let since = db.sstables_since(seq1);
    assert_eq!(
        since.len(),
        1,
        "one table holds writes after seq1: {since:?}"
    );
    assert_eq!(since[0].cf, "default");
    assert!(since[0].max_seq > seq1);
    assert_eq!(db.sstables_since(0).len(), 2);
    db.close().unwrap();
}

/// The diff is by identity, so it catches what `sstables_since` cannot: a
/// compaction that rewrites only OLD data into a new table. An incremental
/// that shipped `sstables_since` alone would restore a catalog naming a table
/// it never uploaded.
#[test]
fn sstables_diff_reports_added_and_removed_across_compaction_and_families() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let other = db
        .create_column_family("other", ColumnFamilyConfig::default())
        .unwrap();

    put_n(&db, &cf, "a", 20);
    db.flush_memtable(&cf).unwrap();
    put_n(&db, &other, "o", 20);
    db.flush_memtable(&other).unwrap();
    let prior = db.live_sstables();
    assert_eq!(prior.len(), 2);
    let seq1 = prior.iter().map(|t| t.max_seq).max().unwrap();

    // Nothing changed: an empty diff.
    let same = db.sstables_diff(&prior);
    assert!(same.added.is_empty() && same.removed.is_empty(), "{same:?}");

    // Rewrite `default`'s only table without adding data.
    db.compact(&cf).unwrap();
    let old_default = prior.iter().find(|t| t.cf == "default").unwrap().clone();
    let diff = db.sstables_diff(&prior);
    assert_eq!(diff.removed, vec![old_default.clone()]);
    assert_eq!(diff.added.len(), 1, "{diff:?}");
    let rewritten = &diff.added[0];
    assert_eq!(rewritten.cf, "default");
    assert_ne!(rewritten.id, old_default.id);
    assert!(
        rewritten.max_seq <= seq1,
        "the rewrite holds only old data, so max_seq cannot flag it"
    );
    assert!(
        db.sstables_since(seq1).is_empty(),
        "sstables_since cannot see a pure rewrite — that is why the diff exists"
    );

    // Dropping a family removes its tables from the live set.
    db.drop_column_family("other").unwrap();
    let diff = db.sstables_diff(&prior);
    assert_eq!(diff.removed.len(), 2, "{diff:?}");
    assert!(diff.removed.iter().any(|t| t.cf == "other"));
    db.close().unwrap();
}

// ---- F7: object-store checkpoints ----------------------------------------

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc as StdArc;

use ondadb::cache::FileCache;
use ondadb::checkpoint::{
    open_remote_checkpoint, restore_from_object_store, ObjectCheckpointOptions,
    REMOTE_CHECKPOINT_TIER,
};
use ondadb::storage::{is_not_found, CreateOutcome, ObjectInfo, ReadHandle, StorageWriter};
use ondadb::{LocalStorage, OndaError, Storage, TierDef};

/// A local directory standing in for an object store, with request counters
/// and a fault switch: every write past `fail_after` fails, as a crash or a
/// network outage mid-checkpoint would.
#[derive(Debug)]
struct TestStore {
    inner: StdArc<LocalStorage>,
    puts: AtomicUsize,
    sizes: StdArc<AtomicUsize>,
    reads: StdArc<AtomicUsize>,
    fail_after: Option<usize>,
    conditional: bool,
}

impl TestStore {
    fn new() -> StdArc<TestStore> {
        StdArc::new(TestStore::with(None, true))
    }
    fn with(fail_after: Option<usize>, conditional: bool) -> TestStore {
        TestStore {
            inner: LocalStorage::new(StdArc::new(FileCache::new(64)), false),
            puts: AtomicUsize::new(0),
            sizes: StdArc::new(AtomicUsize::new(0)),
            reads: StdArc::new(AtomicUsize::new(0)),
            fail_after,
            conditional,
        }
    }
    fn charge(&self) -> ondadb::Result<()> {
        let n = self.puts.fetch_add(1, Ordering::SeqCst);
        match self.fail_after {
            Some(limit) if n >= limit => Err(OndaError::Io(std::io::Error::other(
                "injected store failure",
            ))),
            _ => Ok(()),
        }
    }
}

struct CountingHandle {
    inner: StdArc<dyn ReadHandle>,
    sizes: StdArc<AtomicUsize>,
    reads: StdArc<AtomicUsize>,
}

impl ReadHandle for CountingHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> ondadb::Result<()> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_exact_at(buf, offset)
    }
    fn size(&self) -> ondadb::Result<u64> {
        self.sizes.fetch_add(1, Ordering::SeqCst);
        self.inner.size()
    }
}

impl Storage for TestStore {
    fn open_read(&self, path: &str) -> ondadb::Result<StdArc<dyn ReadHandle>> {
        Ok(StdArc::new(CountingHandle {
            inner: self.inner.open_read(path)?,
            sizes: self.sizes.clone(),
            reads: self.reads.clone(),
        }))
    }
    fn create(&self, path: &str) -> ondadb::Result<Box<dyn StorageWriter>> {
        self.charge()?;
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
    fn put_object(&self, path: &str, data: &[u8]) -> ondadb::Result<ObjectInfo> {
        self.charge()?;
        self.inner.put_object(path, data)
    }
    fn create_if_absent(&self, path: &str, data: &[u8]) -> ondadb::Result<CreateOutcome> {
        if !self.conditional {
            return Err(OndaError::InvalidArgs("no conditional writes".into()));
        }
        self.charge()?;
        self.inner.create_if_absent(path, data)
    }
}

fn big(i: u32) -> Vec<u8> {
    // Above the 512-byte klog threshold, so tables carry a vlog too.
    format!("{i:05}-").repeat(200).into_bytes()
}

/// Two families, flushed + compacted data, separated values, and one more
/// flush on top so the catalog spans levels.
fn populated(dir: &std::path::Path) -> DB {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("default", ColumnFamilyConfig::default())
        .unwrap();
    let other = db
        .create_column_family("other", ColumnFamilyConfig::default())
        .unwrap();
    for i in 0..60u32 {
        db.put(&cf, format!("k{i:03}").as_bytes(), &big(i), Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    for i in 60..80u32 {
        db.put(&cf, format!("k{i:03}").as_bytes(), b"small", Duration::ZERO)
            .unwrap();
    }
    db.put(&other, b"o", b"other-value", Duration::ZERO)
        .unwrap();
    // Left in the memtable on purpose: the checkpoint must flush it.
    db.put(&cf, b"memtable-only", b"m", Duration::ZERO).unwrap();
    db
}

fn assert_populated(db: &DB) {
    let cf = db.get_column_family("default").unwrap();
    for i in 0..60u32 {
        assert_eq!(db.get(&cf, format!("k{i:03}").as_bytes()).unwrap(), big(i));
    }
    for i in 60..80u32 {
        assert_eq!(
            db.get(&cf, format!("k{i:03}").as_bytes()).unwrap(),
            b"small"
        );
    }
    assert_eq!(db.get(&cf, b"memtable-only").unwrap(), b"m");
    let other = db.get_column_family("other").unwrap();
    assert_eq!(db.get(&other, b"o").unwrap(), b"other-value");
}

fn prefix_of(dir: &tempfile::TempDir) -> String {
    dir.path().join("bucket/ckpt").to_str().unwrap().to_string()
}

fn read_only(path: &std::path::Path) -> Options {
    let mut o = Options::new(path.to_str().unwrap());
    o.read_only = true;
    o
}

#[test]
fn object_checkpoint_restores_to_an_equal_database() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let prefix = prefix_of(&bucket);
    let store = TestStore::new();

    let db = populated(src.path());
    let ck = db
        .checkpoint_to_object_store(store.as_ref(), &prefix, &ObjectCheckpointOptions::default())
        .unwrap();
    assert_eq!(
        ck.tables,
        db.live_sstables(),
        "the checkpoint names the live set"
    );
    assert!(ck.tables.iter().any(|t| t.vlog_size > 0), "exercise vlogs");
    assert!(ck.receipts.is_empty());
    db.close().unwrap();

    // Layout: cf-<name>/<id>.{klog,vlog} plus MANIFEST.
    for t in &ck.tables {
        let base = format!("{prefix}/cf-{}/{}", t.cf, t.id);
        assert!(std::path::Path::new(&format!("{base}.klog")).exists());
        assert_eq!(
            std::path::Path::new(&format!("{base}.vlog")).exists(),
            t.vlog_size > 0
        );
    }
    assert!(std::path::Path::new(&format!("{prefix}/MANIFEST")).exists());

    let restored = dest.path().join("db");
    restore_from_object_store(store.as_ref(), &prefix, &restored).unwrap();
    for ro in [true, false] {
        let mut o = Options::new(restored.to_str().unwrap());
        o.read_only = ro;
        let db = DB::open(o).unwrap();
        assert_populated(&db);
        db.close().unwrap();
    }
    // A second restore into a live database directory is refused.
    assert!(matches!(
        restore_from_object_store(store.as_ref(), &prefix, &restored),
        Err(OndaError::Exists(_))
    ));
}

#[test]
fn object_checkpoint_reads_tables_from_other_tiers() {
    let src = tempfile::tempdir().unwrap();
    let hdd = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let prefix = prefix_of(&bucket);
    let store = TestStore::new();

    let mut opts = Options::new(src.path().to_str().unwrap());
    opts.tiers = vec![TierDef::new("hdd", hdd.path().to_str().unwrap())];
    let db = DB::open(opts).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                partition_rules: vec![ondadb::PartitionRule {
                    prefix: b"img/".to_vec(),
                    name: "img".into(),
                }],
                l1_file_count_trigger: 1,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    for i in 0..5u32 {
        db.put(&cf, format!("img/{i}").as_bytes(), b"IMG", Duration::ZERO)
            .unwrap();
        db.put(&cf, format!("etc/{i}").as_bytes(), b"ETC", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();
    db.move_part_to_tier(&cf, "img", "hdd").unwrap();

    db.checkpoint_to_object_store(store.as_ref(), &prefix, &ObjectCheckpointOptions::default())
        .unwrap();
    db.close().unwrap();

    let restored = dest.path().join("db");
    restore_from_object_store(store.as_ref(), &prefix, &restored).unwrap();
    // No tier in the options: the restored catalog places everything locally.
    let db = DB::open(read_only(&restored)).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"img/3").unwrap(), b"IMG");
    assert_eq!(db.get(&cf, b"etc/3").unwrap(), b"ETC");
    db.close().unwrap();
}

#[test]
fn remote_checkpoint_opens_lazily_without_a_size_probe_per_table() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let prefix = prefix_of(&bucket);
    let store = TestStore::new();

    let db = populated(src.path());
    let ck = db
        .checkpoint_to_object_store(store.as_ref(), &prefix, &ObjectCheckpointOptions::default())
        .unwrap();
    db.close().unwrap();

    let path = local.path().join("mount");
    let sizes_before = store.sizes.load(Ordering::SeqCst);
    let remote = open_remote_checkpoint(store.clone(), &prefix, read_only(&path)).unwrap();
    assert_populated(&remote);
    assert_eq!(
        store.sizes.load(Ordering::SeqCst) - sizes_before,
        1,
        "only the MANIFEST download may ask for a size; tables are seeded from it"
    );
    assert!(store.reads.load(Ordering::SeqCst) > 0);
    // Nothing but derived metadata lands locally.
    let local_tables = walk_ext(&path, &["klog", "vlog"]);
    assert!(
        local_tables.is_empty(),
        "tables were downloaded: {local_tables:?}"
    );
    // It is immutable.
    let cf = remote.get_column_family("default").unwrap();
    assert!(matches!(
        remote.put(&cf, b"x", b"y", Duration::ZERO),
        Err(OndaError::ReadOnly(_))
    ));
    assert_eq!(ck.tables.len(), remote.live_sstables().len());
    remote.close().unwrap();

    // Writable, a reused directory, and the reserved tier name are refused.
    assert!(matches!(
        open_remote_checkpoint(
            store.clone(),
            &prefix,
            Options::new(local.path().join("rw").to_str().unwrap())
        ),
        Err(OndaError::InvalidArgs(_))
    ));
    assert!(matches!(
        open_remote_checkpoint(store.clone(), &prefix, read_only(&path)),
        Err(OndaError::Exists(_))
    ));
    let mut clash = read_only(&local.path().join("clash"));
    clash.tiers = vec![TierDef::new(REMOTE_CHECKPOINT_TIER, "/tmp/x")];
    assert!(matches!(
        open_remote_checkpoint(store.clone(), &prefix, clash),
        Err(OndaError::InvalidArgs(_))
    ));
}

/// Every `*.<ext>` file under `dir`.
fn walk_ext(dir: &std::path::Path, exts: &[&str]) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk_ext(&p, exts));
        } else if p
            .extension()
            .is_some_and(|x| exts.iter().any(|want| x == *want))
        {
            out.push(p);
        }
    }
    out
}

/// A checkpoint interrupted anywhere before its MANIFEST upload leaves a
/// prefix with no MANIFEST — which every reader reports as "no checkpoint",
/// never as corruption, and which leaves no half-built local directory.
#[test]
fn interrupted_checkpoint_is_no_checkpoint() {
    let src = tempfile::tempdir().unwrap();
    let db = populated(src.path());
    let objects = db.live_sstables().len();
    for fail_after in [0usize, 1, 2] {
        let bucket = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        let prefix = prefix_of(&bucket);
        let store = StdArc::new(TestStore::with(Some(fail_after), true));
        let e = db
            .checkpoint_to_object_store(
                store.as_ref(),
                &prefix,
                &ObjectCheckpointOptions {
                    upload_concurrency: 1,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(e.to_string().contains("injected"), "{e}");
        assert!(
            !std::path::Path::new(&format!("{prefix}/MANIFEST")).exists(),
            "fail_after={fail_after}: MANIFEST committed before its tables"
        );
        let restored = dest.path().join("db");
        assert!(matches!(
            restore_from_object_store(store.as_ref(), &prefix, &restored),
            Err(OndaError::NotFound)
        ));
        assert!(!restored.join("MANIFEST").exists());
        assert!(matches!(
            open_remote_checkpoint(store.clone(), &prefix, read_only(&dest.path().join("m"))),
            Err(OndaError::NotFound)
        ));
        assert!(!dest.path().join("m/MANIFEST").exists());
    }
    assert!(objects > 0);
    db.close().unwrap();
}

/// A restore that dies after fetching the MANIFEST but before the last table
/// leaves no MANIFEST in the destination: the directory is not a database.
#[test]
fn interrupted_restore_leaves_no_manifest() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let prefix = prefix_of(&bucket);
    let store = TestStore::new();
    let db = populated(src.path());
    let ck = db
        .checkpoint_to_object_store(store.as_ref(), &prefix, &ObjectCheckpointOptions::default())
        .unwrap();
    db.close().unwrap();

    // Lose one table object: the restore must fail and commit nothing.
    let victim = &ck.tables[ck.tables.len() - 1];
    std::fs::remove_file(format!("{prefix}/cf-{}/{}.klog", victim.cf, victim.id)).unwrap();
    let restored = dest.path().join("db");
    let e = restore_from_object_store(store.as_ref(), &prefix, &restored).unwrap_err();
    assert!(is_not_found(&e), "{e}");
    assert!(!restored.join("MANIFEST").exists());
    assert!(!restored.join(".restore-MANIFEST").exists());
}

#[test]
fn incremental_checkpoint_uploads_only_new_tables() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let prefix = prefix_of(&bucket);
    let store = TestStore::new();

    let db = populated(src.path());
    let first = db
        .checkpoint_to_object_store(store.as_ref(), &prefix, &ObjectCheckpointOptions::default())
        .unwrap();
    let puts_first = store.puts.load(Ordering::SeqCst);

    let cf = db.get_column_family("default").unwrap();
    for i in 0..10u32 {
        db.put(&cf, format!("n{i:03}").as_bytes(), b"new", Duration::ZERO)
            .unwrap();
    }
    db.delete(&cf, b"k000").unwrap();
    db.flush_memtable(&cf).unwrap();
    let diff = db.sstables_diff(&first.tables);
    let second = db
        .checkpoint_to_object_store(
            store.as_ref(),
            &prefix,
            &ObjectCheckpointOptions {
                parent: Some(first.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    let uploaded = store.puts.load(Ordering::SeqCst) - puts_first;
    let new_objects: usize = diff
        .added
        .iter()
        .map(|t| 1 + usize::from(t.vlog_size > 0))
        .sum();
    assert!(new_objects > 0);
    assert_eq!(
        uploaded,
        new_objects + 1,
        "exactly the new tables' objects plus the MANIFEST"
    );
    assert_eq!(second.tables, db.live_sstables());
    db.close().unwrap();

    let restored = dest.path().join("db");
    restore_from_object_store(store.as_ref(), &prefix, &restored).unwrap();
    let db = DB::open(read_only(&restored)).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"n005").unwrap(), b"new");
    assert!(matches!(db.get(&cf, b"k000"), Err(OndaError::NotFound)));
    assert_eq!(db.get(&cf, b"k001").unwrap(), big(1));
    db.close().unwrap();
}

#[test]
fn receipts_are_verified_idempotent_and_refuse_foreign_objects() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let prefix = prefix_of(&bucket);
    let store = TestStore::new();
    let db = populated(src.path());
    let with_receipts = ObjectCheckpointOptions {
        receipts: true,
        ..Default::default()
    };

    let ck = db
        .checkpoint_to_object_store(store.as_ref(), &prefix, &with_receipts)
        .unwrap();
    let objects: usize = ck
        .tables
        .iter()
        .map(|t| 1 + usize::from(t.vlog_size > 0))
        .sum();
    assert_eq!(ck.receipts.len(), objects + 1);
    assert_eq!(
        ck.receipts[0].key,
        format!("{prefix}/MANIFEST"),
        "MANIFEST first"
    );
    for r in &ck.receipts {
        let bytes = std::fs::read(&r.key).unwrap();
        assert_eq!(bytes.len() as u64, r.size, "{}", r.key);
        let digest: [u8; 32] = sha2::Digest::finalize(sha2::Digest::chain_update(
            <sha2::Sha256 as sha2::Digest>::new(),
            &bytes,
        ))
        .into();
        assert_eq!(digest, r.sha256, "{}", r.key);
    }

    // A retry into the same prefix finds identical objects and accepts them.
    let again = db
        .checkpoint_to_object_store(store.as_ref(), &prefix, &with_receipts)
        .unwrap();
    assert_eq!(
        again
            .receipts
            .iter()
            .map(|r| (&r.key, r.sha256))
            .collect::<Vec<_>>(),
        ck.receipts
            .iter()
            .map(|r| (&r.key, r.sha256))
            .collect::<Vec<_>>()
    );

    // An object that differs — same size, other bytes — is refused.
    let victim = ck
        .receipts
        .iter()
        .find(|r| r.key.ends_with(".klog"))
        .unwrap();
    let mut bytes = std::fs::read(&victim.key).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&victim.key, &bytes).unwrap();
    let e = db
        .checkpoint_to_object_store(store.as_ref(), &prefix, &with_receipts)
        .unwrap_err();
    assert!(matches!(e, OndaError::Exists(_)), "{e}");

    // Receipts need a conditional store, and cannot be incremental.
    let plain = TestStore::with(None, false);
    let other_prefix = format!("{prefix}-plain");
    assert!(db
        .checkpoint_to_object_store(&plain, &other_prefix, &with_receipts)
        .is_err());
    assert!(!std::path::Path::new(&format!("{other_prefix}/MANIFEST")).exists());
    assert!(matches!(
        db.checkpoint_to_object_store(
            store.as_ref(),
            &prefix,
            &ObjectCheckpointOptions {
                receipts: true,
                parent: Some(ck.clone()),
                ..Default::default()
            }
        ),
        Err(OndaError::InvalidArgs(_))
    ));
    db.close().unwrap();
}

/// A read-only source carries its WAL-replayed memtables into the checkpoint
/// (0.9.1's snapshot rule), and the source directory is not written.
#[test]
fn read_only_source_checkpoints_wal_only_data() {
    let src = tempfile::tempdir().unwrap();
    let crashed = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let prefix = prefix_of(&bucket);
    let store = TestStore::new();
    {
        let db = DB::open(Options::new(src.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("default", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&cf, b"flushed", b"1", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        db.put(&cf, b"wal-only", b"2", Duration::ZERO).unwrap();
        db.delete(&cf, b"flushed").unwrap();
        db.sync_wal().unwrap();
        copy_tree(src.path(), crashed.path());
        db.close().unwrap();
    }
    let before = walk_ext(crashed.path(), &["klog", "vlog", "log"]);
    let db = DB::open(read_only(crashed.path())).unwrap();
    db.checkpoint_to_object_store(store.as_ref(), &prefix, &ObjectCheckpointOptions::default())
        .unwrap();
    db.close().unwrap();
    assert_eq!(walk_ext(crashed.path(), &["klog", "vlog", "log"]), before);

    let restored = dest.path().join("db");
    restore_from_object_store(store.as_ref(), &prefix, &restored).unwrap();
    let db = DB::open(read_only(&restored)).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(db.get(&cf, b"wal-only").unwrap(), b"2");
    assert!(matches!(db.get(&cf, b"flushed"), Err(OndaError::NotFound)));
    db.close().unwrap();
}

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let dst = to.join(e.file_name());
        if e.path().is_dir() {
            copy_tree(&e.path(), &dst);
        } else if e.file_name() != "LOCK" {
            std::fs::copy(e.path(), &dst).unwrap();
        }
    }
}
