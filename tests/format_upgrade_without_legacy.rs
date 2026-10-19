//! Built without `legacy-onda`, a 0.9 directory is a hard `UnsupportedFormat`
//! refusal that names the missing feature — through `DB::open` and through the
//! offline upgrade — and nothing is written.
#![cfg(not(feature = "legacy-onda"))]

use std::path::Path;

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let to = dst.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), &to).unwrap();
        }
    }
}

#[test]
fn a_0_9_directory_is_refused_naming_the_feature() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-onda/db-percf");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("db");
    copy_dir(&src, &dir);
    let before = std::fs::read(dir.join("MANIFEST")).unwrap();
    for read_only in [false, true] {
        let mut opts = ondadb::Options::new(dir.to_str().unwrap());
        opts.read_only = read_only;
        let err = ondadb::DB::open(opts.clone())
            .map(|_| ())
            .expect_err("refused");
        assert_eq!(err.kind(), "unsupported_format", "{err}");
        assert!(err.to_string().contains("legacy-onda"), "{err}");
        let err = ondadb::upgrade::upgrade(opts)
            .map(|_| ())
            .expect_err("refused");
        assert!(err.to_string().contains("legacy-onda"), "{err}");
    }
    assert_eq!(std::fs::read(dir.join("MANIFEST")).unwrap(), before);
    let siblings: Vec<_> = std::fs::read_dir(tmp.path()).unwrap().flatten().collect();
    assert_eq!(siblings.len(), 1, "nothing beside the database");
}
