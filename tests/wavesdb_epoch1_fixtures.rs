//! The wavesdb epoch-1 corpus (plan C step 2c): databases exactly as a pinned
//! wavesdb revision writes them, in yoloDB epoch-1 containers with wavesdb's
//! own bodies (`CAP_WAVES08_BODIES`).
//!
//! `tests/fixtures/wavesdb-epoch1/` is copied byte for byte from wavesdb
//! `testdata/epoch1-waves/`; its generator is frozen there and run by
//! `scripts/epoch1-fixtures.sh` in a worktree of the pinned revision. Each
//! fixture is `db/` (the directory, LOCK removed), `meta.json` (writer,
//! layout, whether it crashed) and `expected.json` (every family's scan, as
//! the writer read it). `SHA256SUMS` pins the bytes, so a fixture cannot drift
//! from the copy yolodb-format mirrors, and a read-only `DB::open` of each
//! must scan to `expected.json`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wavesdb-epoch1")
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

/// Every file is listed in `SHA256SUMS` with its digest, and every listed
/// file exists: an added, removed or rewritten fixture file fails here.
#[test]
fn fixtures_match_sha256sums() {
    let root = fixture_root();
    let sums = std::fs::read_to_string(root.join("SHA256SUMS")).expect("SHA256SUMS");
    let mut want = BTreeMap::new();
    for line in sums.lines() {
        let (digest, name) = line.split_once("  ").unwrap_or_else(|| panic!("malformed line {line:?}"));
        assert!(want.insert(name.to_string(), digest.to_string()).is_none(), "{name} listed twice");
    }
    let mut files = Vec::new();
    walk(&root, &root, &mut files);
    files.retain(|f| f != "SHA256SUMS");
    files.sort();
    let listed: Vec<_> = want.keys().cloned().collect();
    assert_eq!(files, listed, "the files on disk are not the ones SHA256SUMS lists");
    for (name, digest) in &want {
        let got: String = Sha256::digest(std::fs::read(root.join(name)).unwrap())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(&got, digest, "{name} does not match SHA256SUMS");
    }
}

/// The five fixtures are all there, each with its three parts and a wavesdb
/// directory: an epoch-1 snapshot, never a LOCK.
#[test]
fn every_fixture_is_complete() {
    let root = fixture_root();
    for name in ["percf-caps", "percf-closed", "unified", "unified-prepared", "managed"] {
        let dir = root.join(name);
        for part in ["meta.json", "expected.json", "db/MANIFEST"] {
            assert!(dir.join(part).is_file(), "{name}: missing {part}");
        }
        assert!(!dir.join("db/LOCK").exists(), "{name}: a LOCK was vendored");
        let manifest = std::fs::read(dir.join("db/MANIFEST")).unwrap();
        assert_eq!(&manifest[..8], b"YOLODBMF", "{name}: MANIFEST is not an epoch-1 snapshot");
    }
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

/// Every file under `dir` but the LOCK, with its bytes.
fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = Vec::new();
    walk(dir, dir, &mut files);
    files
        .into_iter()
        .filter(|f| f != "LOCK")
        .map(|f| {
            let b = std::fs::read(dir.join(&f)).unwrap();
            (f, b)
        })
        .collect()
}

/// A read-write open of a wavesdb directory is `UnsupportedFormat` naming
/// wavesdb, whatever features are built — and it writes nothing first (only
/// the LOCK every open takes), so the refusal costs the directory nothing.
#[test]
fn a_read_write_open_is_refused_untouched() {
    for name in ["percf-caps", "percf-closed", "unified", "unified-prepared", "managed"] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("db");
        copy_dir(&fixture_root().join(name).join("db"), &dir);
        let before = snapshot(&dir);
        let err = match ondadb::DB::open(ondadb::Options::new(dir.to_str().unwrap())) {
            Ok(_) => panic!("{name}: a wavesdb directory opened read-write"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, ondadb::OndaError::UnsupportedFormat(m) if m.contains("wavesdb")),
            "{name}: {err:?}"
        );
        assert!(before == snapshot(&dir), "{name}: the refused open changed the directory");
    }
}

/// The merge operator the `merged` families were written with: the fixture
/// generator's append, which concatenates the operands onto the base.
#[cfg(feature = "waves08")]
#[derive(Debug)]
struct Append;

#[cfg(feature = "waves08")]
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

#[cfg(feature = "waves08")]
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(feature = "waves08")]
fn open_read_only(dir: &Path) -> ondadb::Result<ondadb::DB> {
    let mut opts = ondadb::Options::new(dir.to_str().unwrap());
    opts.read_only = true;
    opts.merge_fns = vec![std::sync::Arc::new(Append)];
    ondadb::DB::open(opts)
}

/// Every fixture opens read-only through `DB::open` — the layout taken from
/// the catalog, not the options — and every family scans to exactly what
/// wavesdb read back when it wrote it: the row count, the first and last key,
/// and the digest of `hex(key)=hex(value)\n` over the whole scan (wavesdb's
/// `digestScan`). That covers both WAL layouts, a crashed tail, undecided and
/// decided prepares, merge chains, a reversed comparator, grouped and shared
/// value files, and codecs 7 and 8. Point reads agree with the scan at both
/// ends, and the directory is byte-for-byte what was copied in, apart from
/// the LOCK — a write on the handle is the engine's usual read-only no-op.
#[cfg(feature = "waves08")]
#[test]
fn every_fixture_reads_as_wavesdb_wrote_it() {
    for name in ["percf-caps", "percf-closed", "unified", "unified-prepared", "managed"] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("db");
        copy_dir(&fixture_root().join(name).join("db"), &dir);
        let before = snapshot(&dir);
        let expected: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixture_root().join(name).join("expected.json")).unwrap()).unwrap();
        let expected = expected.as_object().unwrap();

        let db = open_read_only(&dir).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let mut families = db.list_column_families();
        families.sort();
        let mut want_families: Vec<_> = expected.keys().cloned().collect();
        want_families.sort();
        assert_eq!(families, want_families, "{name}: families");

        for cf_name in &families {
            let want = &expected[cf_name];
            let cf = db.get_column_family(cf_name).unwrap();
            let snap = db.snapshot();
            let mut it = snap.new_iterator(&cf);
            it.seek_to_first();
            let mut h = Sha256::new();
            let mut rows = 0u64;
            let (mut first, mut last) = (None, None);
            while it.valid() {
                h.update(format!("{}={}\n", hex(it.key()), hex(it.value())));
                if first.is_none() {
                    first = Some((it.key().to_vec(), it.value().to_vec()));
                }
                last = Some((it.key().to_vec(), it.value().to_vec()));
                rows += 1;
                it.next();
            }
            assert!(it.err().is_none(), "{name}/{cf_name}: {:?}", it.err());
            assert_eq!(rows, want["rows"].as_u64().unwrap(), "{name}/{cf_name}: rows");
            assert_eq!(hex(&h.finalize()), want["sha256"].as_str().unwrap(), "{name}/{cf_name}: digest");
            for (end, row) in [("first", first), ("last", last)] {
                let (key, value) = row.unwrap_or_else(|| panic!("{name}/{cf_name}: empty"));
                assert_eq!(hex(&key), want[end].as_str().unwrap(), "{name}/{cf_name}: {end} key");
                assert_eq!(db.get(&cf, &key).unwrap(), value, "{name}/{cf_name}: point read of the {end} key");
            }
        }

        let cf = db.get_column_family(&families[0]).unwrap();
        let _ = db.put(&cf, b"x", b"y", std::time::Duration::ZERO);
        db.close().unwrap();
        assert!(before == snapshot(&dir), "{name}: the read-only open changed the directory");
    }
}

/// A managed-sequence wavesdb database carries its discard floor into the
/// handle — the catalog's, at or below everything the open made visible —
/// and no other database has one.
#[cfg(feature = "waves08")]
#[test]
fn the_managed_discard_floor_is_carried() {
    for name in ["percf-caps", "percf-closed", "unified", "unified-prepared", "managed"] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("db");
        copy_dir(&fixture_root().join(name).join("db"), &dir);
        let catalog = ondadb::waves08::recover_catalog(&dir).unwrap();
        let db = open_read_only(&dir).unwrap();
        if name == "managed" {
            assert!(catalog.discard_seq > 0, "the managed fixture has no floor");
            assert_eq!(db.discard_seq(), Some(catalog.discard_seq));
            assert!(catalog.discard_seq <= db.visible_seq_for_tests(), "{name}: floor above visible");
        } else {
            assert_eq!(db.discard_seq(), None, "{name}");
        }
        db.close().unwrap();
    }
}

/// `waves08::open_read_only` is for wavesdb directories only.
#[cfg(feature = "waves08")]
#[test]
fn waves_open_refuses_other_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let epoch1 = tmp.path().join("epoch1");
    ondadb::DB::open(ondadb::Options::new(epoch1.to_str().unwrap())).unwrap().close().unwrap();
    let mut opts = ondadb::Options::new(epoch1.to_str().unwrap());
    opts.read_only = true;
    let err = match ondadb::waves08::open_read_only(opts) {
        Ok(_) => panic!("an epoch-1 directory opened as wavesdb"),
        Err(e) => e,
    };
    assert!(matches!(err, ondadb::OndaError::InvalidArgs(_)), "{err:?}");
}
