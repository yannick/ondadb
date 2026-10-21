//! The wavesdb epoch-1 corpus (plan C step 2c): databases exactly as a pinned
//! wavesdb revision writes them, in yoloDB epoch-1 containers with wavesdb's
//! own bodies (`CAP_WAVES08_BODIES`).
//!
//! `tests/fixtures/wavesdb-epoch1/` is copied byte for byte from wavesdb
//! `testdata/epoch1-waves/`; its generator is frozen there and run by
//! `scripts/epoch1-fixtures.sh` in a worktree of the pinned revision. Each
//! fixture is `db/` (the directory, LOCK removed), `meta.json` (writer,
//! layout, whether it crashed) and `expected.json` (every family's scan, as
//! the writer read it). The reader of wavesdb directories is checked against
//! `expected.json` as it lands; until then this file pins the bytes, so a
//! fixture cannot drift from the copy yolodb-format mirrors.

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
