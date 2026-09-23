//! Generates the ondaDB 0.9.1 legacy database-directory fixtures used by the
//! epoch-1 `legacy_onda` read-only tests (and, later, the auto-upgrade tests).
//!
//! Run from a checkout of release 0.9.1 (`72e0430`):
//!   cargo run --release --example gen_legacy_fixtures -- <out_dir>
//!
//! Each fixture directory ends with a WAL tail that was never flushed: the DB
//! handle is leaked after `sync_wal`, which is how a crash looks on disk.
//! `expected.txt` records what 0.9.1 itself reads back at that moment.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{
    ColumnFamily, ColumnFamilyConfig, Compression, MergeOperator, Options, SyncMode, DB,
};

#[derive(Debug)]
struct Concat;

impl MergeOperator for Concat {
    fn name(&self) -> &str {
        "fixture.concat.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let mut out = existing.map(|b| b.to_vec()).unwrap_or_default();
        for op in operands {
            if !out.is_empty() {
                out.push(b'|');
            }
            out.extend_from_slice(op);
        }
        Ok(out)
    }
}

const TEN_YEARS: Duration = Duration::from_secs(10 * 365 * 24 * 3600);

fn val(i: usize, len: usize) -> Vec<u8> {
    let mut v = format!("v{i:05}-").into_bytes();
    while v.len() < len {
        v.push(b'a' + (v.len() % 26) as u8);
    }
    v.truncate(len.max(1));
    v
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn dump(db: &DB, names: &[&str], out: &std::path::Path) {
    let mut s = String::new();
    for name in names {
        let cf = db.get_column_family(name).unwrap();
        s.push_str(&format!("cf {name}\n"));
        let t = db.begin();
        let mut it = t.new_iterator(&cf);
        it.seek_to_first();
        while it.valid() {
            let digest = <sha2::Sha256 as sha2::Digest>::digest(it.value());
            s.push_str(&format!(
                "{} {} {}\n",
                hex(it.key()),
                it.value().len(),
                hex(&digest)
            ));
            it.next();
        }
    }
    std::fs::write(out.join("expected.txt"), s).unwrap();
}

fn leak(db: DB) {
    db.sync_wal().unwrap();
    // Leak: no final flush, so the WAL tail stays WAL-only (a crash image).
    std::mem::forget(db);
}

fn base_opts(dir: &std::path::Path) -> Options {
    let mut o = Options::new(dir.to_str().unwrap());
    o.merge_fns = vec![Arc::new(Concat)];
    o
}

fn fill_points(db: &DB, cf: &Arc<ColumnFamily>, from: usize, to: usize, large_every: usize) {
    for i in from..to {
        let k = format!("key{i:05}");
        let len = if large_every > 0 && i % large_every == 0 { 600 } else { 24 };
        if i % 7 == 0 {
            db.put(cf, k.as_bytes(), &val(i, len), TEN_YEARS).unwrap();
        } else {
            db.put(cf, k.as_bytes(), &val(i, len), Duration::ZERO).unwrap();
        }
    }
}

/// Per-CF WAL layout, no capabilities: a VERSION-1 manifest, legacy entries,
/// FNV-free xxh3 blooms, LZ4 (codec 2) blocks and vlog frames.
fn gen_percf(root: &std::path::Path) {
    let dir = root.join("db-percf");
    let db = DB::open(base_opts(&dir)).unwrap();
    let alpha = db
        .create_column_family(
            "alpha",
            ColumnFamilyConfig {
                sync_mode: SyncMode::Full,
                ..Default::default()
            },
        )
        .unwrap();
    let beta = db
        .create_column_family(
            "beta",
            ColumnFamilyConfig {
                compression: Compression::Lz4,
                use_btree: true,
                block_restart_interval: 4,
                data_block_size: 1024,
                bloom_fpr_per_level: vec![0.01, 0.05],
                sync_mode: SyncMode::Full,
                ..Default::default()
            },
        )
        .unwrap();
    fill_points(&db, &alpha, 0, 400, 10);
    fill_points(&db, &beta, 0, 300, 3);
    db.flush_memtable(&alpha).unwrap();
    db.flush_memtable(&beta).unwrap();
    for i in (0..400).step_by(5) {
        db.delete(&alpha, format!("key{i:05}").as_bytes()).unwrap();
    }
    {
        let mut t = db.begin();
        for i in (1..400).step_by(11) {
            t.single_delete(&alpha, format!("key{i:05}").as_bytes()).unwrap();
        }
        t.commit().unwrap();
    }
    fill_points(&db, &alpha, 350, 450, 4);
    db.flush_memtable(&alpha).unwrap();
    db.compact(&alpha).unwrap();
    db.compact(&beta).unwrap();
    // WAL-only tail.
    fill_points(&db, &alpha, 440, 480, 6);
    db.delete(&alpha, b"key00002").unwrap();
    fill_points(&db, &beta, 290, 320, 2);
    dump(&db, &["alpha", "beta"], &dir);
    leak(db);
}

/// Every capability enabled: VERSION-2 manifest with caps/edits/age/range
/// tails, a non-empty MANIFEST-EDITS, extended + prefix-delta tables, merge
/// operands, range tombstones, and envelope WAL frames.
fn gen_caps(root: &std::path::Path) {
    use ondadb::format::*;
    let dir = root.join("db-caps");
    let db = DB::open(base_opts(&dir)).unwrap();
    db.enable_format_capabilities(
        CAP_EXTENDED_RECORDS
            | CAP_MERGE_OPERANDS
            | CAP_RANGE_DELETES
            | CAP_PREFIX_DELTA
            | CAP_MANIFEST_EDITS
            | CAP_PERIODIC_AGE,
    )
    .unwrap();
    let m = db
        .create_column_family(
            "m",
            ColumnFamilyConfig {
                merge_operator_name: Some("fixture.concat.v1".into()),
                enable_prefix_delta_keys: true,
                compression: Compression::Zstd,
                periodic_compaction_interval: Duration::from_secs(3600),
                sync_mode: SyncMode::Full,
                ..Default::default()
            },
        )
        .unwrap();
    let plain = db
        .create_column_family(
            "plain",
            ColumnFamilyConfig {
                compression: Compression::Snappy,
                sync_mode: SyncMode::Full,
                ..Default::default()
            },
        )
        .unwrap();
    fill_points(&db, &m, 0, 200, 9);
    for i in 0..60 {
        db.merge(&m, format!("key{i:05}").as_bytes(), format!("op{i}").as_bytes())
            .unwrap();
    }
    db.delete_range(&m, b"key00100", b"key00120").unwrap();
    fill_points(&db, &plain, 0, 150, 0);
    db.flush_memtable(&m).unwrap();
    db.flush_memtable(&plain).unwrap();
    for i in 50..90 {
        db.merge(&m, format!("key{i:05}").as_bytes(), b"late").unwrap();
    }
    db.delete_range(&plain, b"key00010", b"key00020").unwrap();
    db.flush_memtable(&m).unwrap();
    db.flush_memtable(&plain).unwrap();
    db.compact(&m).unwrap();
    // WAL-only tail: merges, a range delete and point writes in envelopes.
    for i in 80..100 {
        db.merge(&m, format!("key{i:05}").as_bytes(), b"tail").unwrap();
    }
    db.delete_range(&m, b"key00150", b"key00160").unwrap();
    fill_points(&db, &plain, 140, 170, 5);
    dump(&db, &["m", "plain"], &dir);
    leak(db);
}

/// Unified WAL layout: the manifest's ONDAWAL1 tag, and WAL keys prefixed
/// with the 0.9 (wrong-basis) FNV cf id.
fn gen_unified(root: &std::path::Path) {
    let dir = root.join("db-unified");
    let mut o = base_opts(&dir);
    o.unified_memtable = true;
    o.unified_memtable_sync_mode = SyncMode::Full;
    let db = DB::open(o).unwrap();
    let a = db
        .create_column_family("ua", ColumnFamilyConfig::default())
        .unwrap();
    let b = db
        .create_column_family(
            "ub",
            ColumnFamilyConfig {
                compression: Compression::Flate,
                ..Default::default()
            },
        )
        .unwrap();
    fill_points(&db, &a, 0, 120, 8);
    fill_points(&db, &b, 0, 80, 0);
    db.rotate_unified_for_tests();
    while db.pending_flushes_for_tests() > 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    fill_points(&db, &a, 100, 160, 0);
    db.delete(&b, b"key00003").unwrap();
    dump(&db, &["ua", "ub"], &dir);
    leak(db);
}

fn main() {
    let root = std::path::PathBuf::from(std::env::args().nth(1).expect("out dir"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    gen_percf(&root);
    gen_caps(&root);
    gen_unified(&root);
    // Drop the advisory lock files: they carry no state.
    for d in ["db-percf", "db-caps", "db-unified"] {
        let _ = std::fs::remove_file(root.join(d).join("LOCK"));
    }
    println!("fixtures written to {}", root.display());
}
