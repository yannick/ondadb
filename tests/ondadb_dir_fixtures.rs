//! The ondaDB directory corpus (plan C step 2c): whole databases exactly as
//! ondaDB writes them, for the other engine's read-only open.
//!
//! `tests/fixtures/ondadb-dirs/` is committed to git and vendored by wavesdb,
//! whose reader of ondaDB directories must scan every family to the same
//! answer. It is the mirror of `tests/fixtures/wavesdb-epoch1/`: each fixture
//! is `db/` (the directory, LOCK removed), `meta.json` (writer, layout,
//! whether it crashed, the containers it must hold) and `expected.json`
//! (every family's scan as the writer read it: the row count, the first and
//! last key, and the SHA-256 of `hex(key)=hex(value)\n` over the whole scan,
//! wavesdb's `digestScan`). `SHA256SUMS` pins the bytes.
//!
//! - `percf-caps`: the per-family layout with every capability it can use —
//!   extended records, merge operands, range deletes, prefix-delta blocks and
//!   the edit log — a reverse and an int64 comparator, a merge operator,
//!   single deletes, TTLs, entities, values in per-table `.vlog` files and a
//!   codec per family. Stopped without a close, its last round committed from
//!   several threads, so the WAL tail is spread over the stripes `.s1`-`.s3`.
//! - `percf-plain`: no capability at all — legacy flag-byte WAL records,
//!   base-format tables, no edit log — closed.
//! - `unified`: the unified layout with range deletes, closed.
//! - `unified-prepared`: the unified layout with 3.2 prepared transactions —
//!   committed (one in a flushed generation, so its decision outlives its
//!   prepare), aborted and one left undecided, whose writes a reader must not
//!   show — stopped without a close, striped.
//!
//! The bytes are not reproducible (file ids, nonces and stripe placement
//! follow the run), so the generator is not a golden test: regenerate with
//! `ONDADB_DIR_FIXTURES=1 cargo test --test ondadb_dir_fixtures -- --ignored`
//! only for a reviewed format change, then re-vendor wavesdb's copy.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ondadb::config::{ColumnFamilyConfig, Compression, SyncMode};
use ondadb::entity::EntityColumn;
use ondadb::format::{
    CAP_EXTENDED_RECORDS, CAP_MANIFEST_EDITS, CAP_MERGE_OPERANDS, CAP_PREFIX_DELTA,
    CAP_RANGE_DELETES, CAP_TXN_DECISIONS, CAP_WAL_FLOOR,
};
use ondadb::{ColumnFamily, Options, DB};
use sha2::{Digest, Sha256};

const FIXTURES: &[&str] = &["percf-caps", "percf-plain", "unified", "unified-prepared"];

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ondadb-dirs")
}

/// The merge operator the `merged` family is written with: the operands
/// concatenated onto the base, as wavesdb's fixture `fixture.append` does.
#[derive(Debug)]
struct Append;

impl ondadb::MergeOperator for Append {
    fn name(&self) -> &str {
        "fixture.append"
    }

    fn full_merge(&self, _key: &[u8], existing: Option<&[u8]>, operands: &[&[u8]]) -> Result<Vec<u8>, String> {
        let mut out = existing.unwrap_or_default().to_vec();
        for o in operands {
            out.extend_from_slice(o);
        }
        Ok(out)
    }
}

fn options(dir: &Path, unified: bool) -> Options {
    let mut o = Options::new(dir.to_str().unwrap());
    o.merge_fns = vec![Arc::new(Append)];
    o.num_compaction_threads = 1;
    o.unified_memtable = unified;
    // Not Full: a Full WAL is a single stripe, and the crashed fixtures exist
    // to spread their tails over all four.
    o.unified_memtable_sync_mode = SyncMode::None;
    o
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Every family's scan at the latest snapshot, as (hex key, hex value) rows.
fn scan_all(db: &DB) -> BTreeMap<String, Vec<(String, String)>> {
    let snap = db.snapshot();
    let mut out = BTreeMap::new();
    for name in db.list_column_families() {
        let cf = db.get_column_family(&name).unwrap();
        let mut it = snap.new_iterator(&cf);
        it.seek_to_first();
        let mut rows = Vec::new();
        while it.valid() {
            rows.push((hex(it.key()), hex(it.value())));
            it.next();
        }
        assert!(it.err().is_none(), "{name}: {:?}", it.err());
        out.insert(name, rows);
    }
    out
}

/// `expected.json` for a scan, byte for byte: families in name order, one
/// space of indent per level, as wavesdb's `e1WriteJSON` writes its own.
fn render_expected(scan: &BTreeMap<String, Vec<(String, String)>>) -> String {
    let mut s = String::from("{\n");
    for (i, (name, rows)) in scan.iter().enumerate() {
        let mut h = Sha256::new();
        for (k, v) in rows {
            h.update(format!("{k}={v}\n"));
        }
        let (first, last) = match (rows.first(), rows.last()) {
            (Some(f), Some(l)) => (f.0.as_str(), l.0.as_str()),
            _ => ("", ""),
        };
        let sep = if i + 1 == scan.len() { "" } else { "," };
        write!(
            s,
            " \"{name}\": {{\n  \"rows\": {},\n  \"first\": \"{first}\",\n  \"last\": \"{last}\",\n  \"sha256\": \"{}\"\n }}{sep}\n",
            rows.len(),
            hex(&h.finalize())
        )
        .unwrap();
    }
    s.push_str("}\n");
    s
}

struct Meta {
    unified: bool,
    crashed: bool,
    containers: &'static [&'static str],
}

fn render_meta(m: &Meta, writer: &str) -> String {
    let containers: Vec<String> = m.containers.iter().map(|c| format!("  \"{c}\"")).collect();
    format!(
        "{{\n \"writer\": \"{writer}\",\n \"unified\": {},\n \"crashed\": {},\n \"containers\": [\n{}\n ]\n}}\n",
        m.unified,
        m.crashed,
        containers.join(",\n")
    )
}

/// A value large enough for the table's `.vlog`, distinct per (round, i).
fn big_value(round: usize, i: usize) -> Vec<u8> {
    let mut v = format!("{round}.{i}:").into_bytes();
    let words = ["alpha ", "bravo ", "charlie ", "delta ", "echo ", "foxtrot "];
    let mut n = round * 7919 + i;
    while v.len() < 700 + i % 300 {
        v.extend_from_slice(words[n % words.len()].as_bytes());
        n = n.wrapping_mul(2_654_435_761).wrapping_add(1) >> 3;
    }
    v
}

const CENTURY: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

fn family(db: &DB, name: &str, cfg: ColumnFamilyConfig) -> Arc<ColumnFamily> {
    db.create_column_family(name, cfg).unwrap()
}

fn cfg(compression: Compression, sync: SyncMode) -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        compression,
        compression_per_level: Vec::new(),
        sync_mode: sync,
        ..ColumnFamilyConfig::default()
    }
}

/// One round of per-family writes, on the calling thread.
fn percf_round(db: &DB, fams: &[Arc<ColumnFamily>], round: usize, keys: std::ops::Range<usize>, caps: bool) {
    let (plain, num) = (&fams[0], &fams[1]);
    for i in keys {
        let k = format!("key{i:05}").into_bytes();
        let v = if i % 2 == 0 { big_value(round, i) } else { format!("value-{round}-{i}").into_bytes() };
        db.put(plain, &k, &v, Duration::ZERO).unwrap();
        let n = ((i as i64 - 100) * (round as i64 + 1)).to_be_bytes();
        db.put(num, &n, &v, Duration::ZERO).unwrap();
        if i % 7 == round {
            db.delete(plain, &k).unwrap();
        }
        if i % 11 == 0 {
            db.put(plain, format!("ttl{i:04}").as_bytes(), &v, CENTURY).unwrap();
        }
        if i % 31 == 0 {
            let cols = [EntityColumn::new("a", v[..8].to_vec()), EntityColumn::new("b", i.to_string())];
            db.put_entity(plain, format!("ent{i:04}").as_bytes(), &cols, Duration::ZERO).unwrap();
        }
        if caps {
            let (rev, merged) = (&fams[2], &fams[3]);
            db.put(rev, &k, &v, Duration::ZERO).unwrap();
            db.merge(merged, &k[..7], format!("+{round}.{i}").as_bytes()).unwrap();
        }
    }
}

fn gen_percf(dir: &Path, caps: bool) -> (Meta, BTreeMap<String, Vec<(String, String)>>) {
    let db = DB::open(options(dir, false)).unwrap();
    // The crashed fixture's WAL is striped; the closed one keeps Full.
    let sync = if caps { SyncMode::None } else { SyncMode::Full };
    if caps {
        db.enable_format_capabilities(
            CAP_EXTENDED_RECORDS | CAP_MERGE_OPERANDS | CAP_RANGE_DELETES | CAP_PREFIX_DELTA | CAP_MANIFEST_EDITS,
        )
        .unwrap();
    }
    let mut fams = vec![
        family(&db, "plain", ColumnFamilyConfig { klog_value_threshold: 256, ..cfg(Compression::Zstd, sync) }),
        family(
            &db,
            "num",
            ColumnFamilyConfig { comparator_name: "int64".into(), ..cfg(Compression::Snappy, sync) },
        ),
    ];
    if caps {
        fams.push(family(
            &db,
            "rev",
            ColumnFamilyConfig {
                comparator_name: "reverse".into(),
                enable_prefix_delta_keys: true,
                ..cfg(Compression::Lz4, sync)
            },
        ));
        fams.push(family(
            &db,
            "merged",
            ColumnFamilyConfig { merge_operator_name: Some("fixture.append".into()), ..cfg(Compression::Flate, sync) },
        ));
    }
    let rounds = if caps { 3 } else { 2 };
    for round in 0..rounds {
        let last = round + 1 == rounds;
        if last && caps {
            // The tail: four threads, so ondaDB hands each its own stripe.
            std::thread::scope(|s| {
                for t in 0..4 {
                    let (db, fams) = (&db, &fams);
                    s.spawn(move || percf_round(db, fams, round, t * 50..t * 50 + 50, true));
                }
            });
        } else {
            percf_round(&db, &fams, round, 0..200, caps);
        }
        let mut txn = db.begin();
        txn.single_delete(&fams[0], format!("key{:05}", 10 + round).as_bytes()).unwrap();
        txn.commit().unwrap();
        if caps {
            let (lo, hi) = (format!("key{:05}", 50 + round * 40), format!("key{:05}", 60 + round * 40));
            db.delete_range(&fams[0], lo.as_bytes(), hi.as_bytes()).unwrap();
        }
        if last {
            break;
        }
        for cf in &fams {
            db.flush_memtable(cf).unwrap();
        }
        if round == 0 {
            for cf in &fams {
                db.compact(cf).unwrap();
            }
        }
    }
    let scan = scan_all(&db);
    if caps {
        db.sync_wal().unwrap();
        // No close: the WAL tail is what a crash leaves.
        std::mem::forget(db);
        (
            Meta {
                unified: false,
                crashed: true,
                containers: &["YOLODBMF", "YOLODBED", "YOLOST01", "YOLODBVL", "YOLODBWL", "wal-tail", "wal-stripe"],
            },
            scan,
        )
    } else {
        db.close().unwrap();
        (Meta { unified: false, crashed: false, containers: &["YOLODBMF", "YOLOST01", "YOLODBVL", "YOLODBWL"] }, scan)
    }
}

fn txn_id(name: &str) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..name.len()].copy_from_slice(name.as_bytes());
    id
}

fn gen_unified(dir: &Path, prepared: bool) -> (Meta, BTreeMap<String, Vec<(String, String)>>) {
    let db = DB::open(options(dir, true)).unwrap();
    let mut caps = CAP_EXTENDED_RECORDS | CAP_RANGE_DELETES | CAP_MANIFEST_EDITS;
    if prepared {
        caps |= CAP_TXN_DECISIONS;
    }
    db.enable_format_capabilities(caps).unwrap();
    let x = family(&db, "x", ColumnFamilyConfig::default());
    let y = family(&db, "y", ColumnFamilyConfig::default());
    for round in 0..2 {
        let write = |range: std::ops::Range<usize>| {
            for i in range {
                let k = format!("u{i:04}").into_bytes();
                let v = if i % 13 == 0 { big_value(round, i) } else { format!("v{round}-{i}").into_bytes() };
                db.put(&x, &k, &v, Duration::ZERO).unwrap();
                db.put(&y, &k, &v, Duration::ZERO).unwrap();
            }
        };
        if round == 1 && prepared {
            std::thread::scope(|s| {
                for t in 0..4 {
                    let write = &write;
                    s.spawn(move || write(t * 40..t * 40 + 40));
                }
            });
        } else {
            write(0..160);
        }
        let (lo, hi) = (format!("u{:04}", 20 + round * 30), format!("u{:04}", 30 + round * 30));
        db.delete_range(&x, lo.as_bytes(), hi.as_bytes()).unwrap();
        if prepared {
            let mut txn = db.begin();
            txn.put(&x, format!("prepared{round}").as_bytes(), b"committed", Duration::ZERO).unwrap();
            txn.put(&y, format!("prepared{round}").as_bytes(), b"committed", Duration::ZERO).unwrap();
            let id = txn_id(&format!("fixture-{round}"));
            txn.prepare(&id).unwrap();
            db.commit_prepared(&id).unwrap();
            let mut txn = db.begin();
            txn.put(&x, format!("aborted{round}").as_bytes(), b"never-visible", Duration::ZERO).unwrap();
            let id = txn_id(&format!("aborted-{round}"));
            txn.prepare(&id).unwrap();
            db.abort_prepared(&id).unwrap();
        }
        if round == 0 {
            // DB::flush_memtable rotates only the family's own memtable, which
            // the unified layout leaves empty; seal the shared one instead.
            db.rotate_unified_for_tests();
            while db.pending_flushes_for_tests() > 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
    if !prepared {
        let scan = scan_all(&db);
        db.close().unwrap();
        return (
            Meta { unified: true, crashed: false, containers: &["YOLODBMF", "YOLODBED", "YOLOST01", "YOLODBVL", "YOLODBWL"] },
            scan,
        );
    }
    // An undecided prepare: its writes are invisible to the scan, and a
    // reader must keep them so.
    let mut txn = db.begin();
    txn.put(&x, b"undecided", b"never-visible", Duration::ZERO).unwrap();
    txn.delete(&y, b"u0001").unwrap();
    txn.prepare(&txn_id("undecided")).unwrap();
    let scan = scan_all(&db);
    db.sync_wal().unwrap();
    std::mem::forget(db);
    (
        Meta {
            unified: true,
            crashed: true,
            containers: &["YOLODBMF", "YOLODBED", "YOLOST01", "YOLODBVL", "YOLODBWL", "wal-tail", "wal-stripe"],
        },
        scan,
    )
}

fn walk(dir: &Path, root: &Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, root, out);
        } else {
            out.push(path.strip_prefix(root).unwrap().to_str().unwrap().replace('\\', "/"));
        }
    }
}

/// Which containers `dir` holds: an 8-byte magic at the head of `MANIFEST`,
/// `MANIFEST-EDITS`, a `.vlog` or a WAL stripe, `YOLOST01` at the tail of a
/// `.klog`; `wal-tail` for a stripe holding a frame past its 32-byte header,
/// `wal-stripe` when that stripe is not stripe 0.
fn containers(dir: &Path) -> std::collections::BTreeSet<&'static str> {
    let mut files = Vec::new();
    walk(dir, dir, &mut files);
    let mut found = std::collections::BTreeSet::new();
    for f in files {
        let b = std::fs::read(dir.join(&f)).unwrap();
        let base = f.rsplit('/').next().unwrap();
        let head = |m: &[u8]| b.len() >= 8 && &b[..8] == m;
        if base == "MANIFEST" && head(b"YOLODBMF") {
            found.insert("YOLODBMF");
        } else if base == "MANIFEST-EDITS" && head(b"YOLODBED") {
            found.insert("YOLODBED");
        } else if base.ends_with(".klog") && b.len() >= 8 && &b[b.len() - 8..] == b"YOLOST01" {
            found.insert("YOLOST01");
        } else if base.ends_with(".vlog") && head(b"YOLODBVL") {
            found.insert("YOLODBVL");
        } else if base.contains("wal-") && head(b"YOLODBWL") {
            found.insert("YOLODBWL");
            if b.len() > 32 {
                found.insert("wal-tail");
                if !base.ends_with(".log") {
                    found.insert("wal-stripe");
                }
            }
        }
    }
    found
}

fn sha256sums(root: &Path) -> String {
    let mut files = Vec::new();
    walk(root, root, &mut files);
    files.retain(|f| f != "SHA256SUMS");
    files.sort();
    let mut out = String::new();
    for f in files {
        writeln!(out, "{}  {f}", hex(&Sha256::digest(std::fs::read(root.join(&f)).unwrap()))).unwrap();
    }
    out
}

#[test]
#[ignore = "regenerates committed fixtures; run manually for a reviewed format change"]
fn regenerate_dir_corpus() {
    if std::env::var_os("ONDADB_DIR_FIXTURES").is_none() {
        return;
    }
    let writer = format!("ondadb {}", env!("CARGO_PKG_VERSION"));
    let root = fixture_root();
    let _ = std::fs::remove_dir_all(&root);
    for name in FIXTURES {
        let dst = root.join(name);
        let dir = dst.join("db");
        let (meta, scan) = match *name {
            "percf-caps" => gen_percf(&dir, true),
            "percf-plain" => gen_percf(&dir, false),
            "unified" => gen_unified(&dir, false),
            "unified-prepared" => gen_unified(&dir, true),
            _ => unreachable!(),
        };
        let _ = std::fs::remove_file(dir.join("LOCK"));
        std::fs::write(dst.join("meta.json"), render_meta(&meta, &writer)).unwrap();
        std::fs::write(dst.join("expected.json"), render_expected(&scan)).unwrap();
        let found = containers(&dir);
        for c in meta.containers {
            assert!(found.contains(c), "{name}: holds no {c}");
        }
    }
    std::fs::write(root.join("SHA256SUMS"), sha256sums(&root)).unwrap();
}

/// Every file is listed in `SHA256SUMS` with its digest, and nothing else is.
#[test]
fn the_corpus_matches_sha256sums() {
    let root = fixture_root();
    let committed = std::fs::read_to_string(root.join("SHA256SUMS")).expect("SHA256SUMS");
    assert_eq!(committed, sha256sums(&root), "the files on disk are not the ones SHA256SUMS pins");
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let path = entry.unwrap().path();
        let to = dst.join(path.file_name().unwrap());
        if path.is_dir() {
            copy_dir(&path, &to);
        } else {
            std::fs::copy(&path, &to).unwrap();
        }
    }
}

/// Each fixture still holds the containers its `meta.json` promises, and
/// ondaDB reopens it — replaying a crashed tail across its stripes and
/// resolving the prepares — to exactly the scan it wrote down.
#[test]
fn every_fixture_reopens_to_its_expected_scan() {
    for name in FIXTURES {
        let src = fixture_root().join(name);
        let meta = std::fs::read_to_string(src.join("meta.json")).unwrap();
        let found = containers(&src.join("db"));
        for c in ["YOLODBMF", "YOLODBED", "YOLOST01", "YOLODBVL", "YOLODBWL", "wal-tail", "wal-stripe"] {
            if meta.contains(&format!("\"{c}\"")) {
                assert!(found.contains(c), "{name}: holds no {c}");
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("db");
        copy_dir(&src.join("db"), &dir);
        let mut o = options(&dir, meta.contains("\"unified\": true"));
        o.read_only = true;
        let db = DB::open(o).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let want = std::fs::read_to_string(src.join("expected.json")).unwrap();
        assert_eq!(render_expected(&scan_all(&db)), want, "{name}");
        let prepared: Vec<_> = db.list_prepared().iter().map(|p| p.id).collect();
        if *name == "unified-prepared" {
            // Recovered and registered, never shown; a close refuses while
            // it is undecided, so the handle is dropped as a participant
            // leaves one.
            assert_eq!(prepared, vec![txn_id("undecided")], "{name}");
            drop(db);
        } else {
            assert!(prepared.is_empty(), "{name}");
            db.close().unwrap();
        }
    }
}

/// `unified-prepared` predates the replay floor: it holds `CAP_TXN_DECISIONS`
/// without `CAP_WAL_FLOOR`. A read-write open takes the bit before its first
/// flush, still shows exactly the fixture's scan and still recovers the
/// undecided prepare; the flush after it records a floor, and the next open
/// reads the same.
#[test]
fn a_pre_floor_directory_takes_the_floor_on_a_read_write_open() {
    let src = fixture_root().join("unified-prepared");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("db");
    copy_dir(&src.join("db"), &dir);
    let before = ondadb::manifest_edit::recover_catalog(&dir).unwrap();
    assert_ne!(before.caps & CAP_TXN_DECISIONS, 0);
    assert_eq!(before.caps & CAP_WAL_FLOOR, 0, "the fixture predates the floor");
    let want = std::fs::read_to_string(src.join("expected.json")).unwrap();
    {
        let db = DB::open(options(&dir, true)).unwrap();
        assert_ne!(db.format_capabilities() & CAP_WAL_FLOOR, 0);
        assert_eq!(render_expected(&scan_all(&db)), want);
        assert_eq!(
            db.list_prepared().iter().map(|p| p.id).collect::<Vec<_>>(),
            vec![txn_id("undecided")]
        );
        // Under the unified layout this flushes the shared memtable.
        db.flush_memtable(&db.get_column_family("x").unwrap()).unwrap();
        drop(db);
    }
    let after = ondadb::manifest_edit::recover_catalog(&dir).unwrap();
    assert_ne!(after.caps & CAP_WAL_FLOOR, 0);
    assert!(after.wal_floor > 0, "the flush recorded a floor");
    let db = DB::open(options(&dir, true)).unwrap();
    assert_eq!(render_expected(&scan_all(&db)), want);
    assert_eq!(db.list_prepared().len(), 1);
    drop(db);
}
