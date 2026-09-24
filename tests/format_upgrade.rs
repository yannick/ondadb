//! The automatic upgrade of ondaDB 0.9.x directories to yoloDB format epoch 1
//! (plan C §1.3), driven against the three whole database directories 0.9.1
//! wrote (`tests/fixtures/legacy-onda/db-*`).
//!
//! * every fixture upgrades on a plain `DB::open` and reads back exactly what
//!   0.9.1 read (`expected.txt`), then keeps working as an epoch-1 database;
//! * `Forbid`, `ReadOnlyLegacy` and read-only opens never write;
//! * every preflight failure, and a failure injected at every pre-swap step,
//!   leaves the source **byte-identical** (a full-tree hash);
//! * a crash injected after **every** protocol step (a child process that
//!   exits inside the observer, like `tests/engine_regressions.rs`) is resolved
//!   by the next open — completed or rolled back and redone — and never loses a
//!   byte of the source.
#![cfg(feature = "legacy-onda")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ondadb::upgrade::{self, UpgradeObserver, UpgradePhase};
use ondadb::{FormatUpgrade, FormatUpgradeVerify, OndaError, Options, DB};

const FIXTURES: [&str; 3] = ["db-percf", "db-caps", "db-unified"];

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-onda")
}

/// The merge operator `db-caps` was written with (see the generator).
#[derive(Debug)]
struct Concat;

impl ondadb::MergeOperator for Concat {
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

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Every file under `dir` (relative path → sha256), `LOCK` excluded: the lock
/// file is empty, carries no state, and any open (0.9's too) creates it.
fn tree_hash(dir: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            if e.file_type().unwrap().is_dir() {
                walk(root, &p, out);
            } else if e.file_name() != "LOCK" {
                let rel = p.strip_prefix(root).unwrap().to_string_lossy().to_string();
                let digest = <sha2::Sha256 as sha2::Digest>::digest(std::fs::read(&p).unwrap());
                out.insert(rel, hex(&digest));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// What 0.9.1 itself read, per family: `key-hex value-len sha256(value)-hex`.
fn expected_scan(name: &str) -> Vec<(String, Vec<String>)> {
    let text = std::fs::read_to_string(fixture_root().join(name).join("expected.txt")).unwrap();
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for line in text.lines() {
        if let Some(cf) = line.strip_prefix("cf ") {
            out.push((cf.to_string(), Vec::new()));
        } else {
            out.last_mut().unwrap().1.push(line.to_string());
        }
    }
    out
}

fn scan(db: &DB, cf_name: &str) -> Vec<String> {
    let cf = db
        .get_column_family(cf_name)
        .unwrap_or_else(|| panic!("no family {cf_name}"));
    let txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut got = Vec::new();
    while it.valid() {
        let digest = <sha2::Sha256 as sha2::Digest>::digest(it.value());
        got.push(format!(
            "{} {} {}",
            hex(it.key()),
            it.value().len(),
            hex(&digest)
        ));
        it.next();
    }
    assert!(it.err().is_none(), "{cf_name}: {:?}", it.err());
    got
}

fn assert_scans_expected(db: &DB, name: &str) {
    for (cf, want) in expected_scan(name) {
        let got = scan(db, &cf);
        assert_eq!(got.len(), want.len(), "{name}/{cf}: row count");
        assert_eq!(got, want, "{name}/{cf}");
    }
}

/// A writable copy of fixture `name` at `<tmp>/<name>`.
struct Case {
    _tmp: tempfile::TempDir,
    parent: PathBuf,
    dir: PathBuf,
    name: &'static str,
}

impl Case {
    fn new(name: &'static str) -> Case {
        let tmp = tempfile::tempdir().unwrap();
        let parent = tmp.path().to_path_buf();
        let dir = parent.join(name);
        copy_dir(&fixture_root().join(name), &dir);
        Case {
            _tmp: tmp,
            parent,
            dir,
            name,
        }
    }

    fn opts(&self) -> Options {
        let mut o = Options::new(self.dir.to_str().unwrap());
        o.merge_fns = vec![Arc::new(Concat)];
        o.unified_memtable = self.name == "db-unified";
        o
    }

    /// The fixture's own bytes.
    fn original(&self) -> BTreeMap<String, String> {
        tree_hash(&fixture_root().join(self.name))
    }

    /// Siblings the protocol may create, by kind.
    fn siblings(&self, infix: &str) -> Vec<PathBuf> {
        let prefix = format!(".{}.{infix}", self.name);
        let mut v: Vec<PathBuf> = std::fs::read_dir(&self.parent)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
            .map(|e| e.path())
            .collect();
        v.sort();
        v
    }

    fn upgrade_dirs(&self) -> Vec<PathBuf> {
        self.siblings("yolo-upgrade-")
    }

    fn backups(&self) -> Vec<PathBuf> {
        self.siblings("pre-yolo-")
    }

    fn journal(&self) -> PathBuf {
        self.parent
            .join(format!(".{}.yolo-upgrade.journal", self.name))
    }

    /// The source is exactly the fixture and nothing was left beside it.
    fn assert_untouched(&self, why: &str) {
        assert!(
            ondadb::manifest::is_onda09_dir(&self.dir).unwrap(),
            "{why}: no longer a 0.9 directory"
        );
        assert_eq!(
            tree_hash(&self.dir),
            self.original(),
            "{why}: source changed"
        );
        assert!(
            self.upgrade_dirs().is_empty(),
            "{why}: upgrade dir left behind"
        );
        assert!(self.backups().is_empty(), "{why}: backup created");
        assert!(!self.journal().exists(), "{why}: journal left behind");
    }

    /// Upgraded: the path is epoch 1, nothing is left mid-protocol, and the
    /// single backup is byte-for-byte the fixture.
    fn assert_upgraded(&self, why: &str) {
        assert!(
            !ondadb::manifest::is_onda09_dir(&self.dir).unwrap(),
            "{why}: still a 0.9 directory"
        );
        assert!(
            self.upgrade_dirs().is_empty(),
            "{why}: upgrade dir left behind"
        );
        assert!(!self.journal().exists(), "{why}: journal left behind");
        assert!(
            !self.dir.join(upgrade::MARKER).exists(),
            "{why}: marker left in the live directory"
        );
        // A file the engine does not own (the fixture's `expected.txt`) is
        // carried into the upgraded directory, not stranded in the backup.
        assert_eq!(
            std::fs::read(self.dir.join("expected.txt")).ok(),
            std::fs::read(fixture_root().join(self.name).join("expected.txt")).ok(),
            "{why}: foreign file not carried over"
        );
        let backups = self.backups();
        assert_eq!(backups.len(), 1, "{why}: backups {backups:?}");
        assert_eq!(
            tree_hash(&backups[0]),
            self.original(),
            "{why}: backup differs"
        );
    }
}

/// The acceptance test: a plain `DB::open` upgrades every 0.9.1 directory, and
/// the result reads exactly what 0.9.1 read — tables, the WAL-only tail, merge
/// operands, range tombstones, TTLs, the unified layout's 0.9 cf ids — then
/// accepts writes and survives a reopen as an ordinary epoch-1 database.
#[test]
fn every_fixture_auto_upgrades_and_scans_equal() {
    for verify in [FormatUpgradeVerify::Scan, FormatUpgradeVerify::Counts] {
        for name in FIXTURES {
            let case = Case::new(name);
            let legacy_caps = {
                let db = ondadb::legacy_onda::open_read_only(case.opts()).unwrap();
                let caps = db.format_capabilities();
                db.close().unwrap();
                caps
            };
            let mut opts = case.opts();
            opts.format_upgrade_verify = verify;
            let db = DB::open(opts.clone()).unwrap_or_else(|e| panic!("{name}: {e}"));
            let report = db.last_format_upgrade().expect("an upgrade report");
            assert!(!report.resumed);
            assert_eq!(report.verify, verify);
            assert_eq!(report.column_families, 2, "{name}");
            assert!(
                report.tables >= 2 && report.entries > 0,
                "{name}: {report:?}"
            );
            assert!(report.source_bytes > 0 && report.upgraded_bytes > 0);
            assert_eq!(
                report.backup_path.as_deref(),
                case.backups().first().map(|p| p.as_path())
            );
            case.assert_upgraded(name);
            assert_eq!(db.format_capabilities(), legacy_caps, "{name}: CAP word");
            assert_scans_expected(&db, name);
            // An ordinary epoch-1 database from here on.
            let (cf_name, _) = expected_scan(name).remove(0);
            let cf = db.get_column_family(&cf_name).unwrap();
            db.put(&cf, b"after-upgrade", b"v", std::time::Duration::ZERO)
                .unwrap();
            db.close().unwrap();

            let db = DB::open(opts).unwrap();
            assert!(db.last_format_upgrade().is_none(), "{name}: upgraded twice");
            let cf = db.get_column_family(&cf_name).unwrap();
            assert_eq!(db.get(&cf, b"after-upgrade").unwrap(), b"v");
            db.delete(&cf, b"after-upgrade").unwrap();
            assert_scans_expected(&db, name);
            db.close().unwrap();
        }
    }
}

#[test]
fn keep_backup_false_deletes_the_backup_after_the_open() {
    let case = Case::new("db-percf");
    let mut opts = case.opts();
    opts.format_upgrade_keep_backup = false;
    let db = DB::open(opts).unwrap();
    let report = db.last_format_upgrade().unwrap();
    assert_eq!(report.backup_path, None);
    assert!(case.backups().is_empty());
    assert!(case.upgrade_dirs().is_empty() && !case.journal().exists());
    assert_scans_expected(&db, "db-percf");
    db.close().unwrap();
}

#[test]
fn forbid_refuses_and_writes_nothing() {
    for read_only in [false, true] {
        let case = Case::new("db-caps");
        let mut opts = case.opts();
        opts.format_upgrade = FormatUpgrade::Forbid;
        opts.read_only = read_only;
        let err = DB::open(opts).map(|_| ()).expect_err("Forbid must refuse");
        assert_eq!(err.kind(), "unsupported_format");
        assert!(
            err.to_string()
                .contains("format upgrade forbidden by Options::format_upgrade"),
            "{err}"
        );
        case.assert_untouched("forbid");
    }
}

/// `ReadOnlyLegacy`, and a read-only open under `Auto`, read the 0.9
/// directory as it is and never rebuild it.
#[test]
fn read_only_legacy_and_read_only_opens_never_upgrade() {
    for (mode, read_only) in [
        (FormatUpgrade::ReadOnlyLegacy, false),
        (FormatUpgrade::ReadOnlyLegacy, true),
        (FormatUpgrade::Auto, true),
    ] {
        for name in FIXTURES {
            let case = Case::new(name);
            let mut opts = case.opts();
            opts.format_upgrade = mode;
            opts.read_only = read_only;
            let db = DB::open(opts).unwrap_or_else(|e| panic!("{name} {mode:?}: {e}"));
            assert!(db.last_format_upgrade().is_none());
            assert_scans_expected(&db, name);
            let cf = db.get_column_family(&expected_scan(name)[0].0).unwrap();
            // The handle is read-only whatever `read_only` said.
            assert!(db.create_column_family("new", Default::default()).is_err());
            let _ = db.put(&cf, b"x", b"y", std::time::Duration::ZERO);
            db.close().unwrap();
            case.assert_untouched(&format!("{name} {mode:?} read_only={read_only}"));
        }
    }
}

/// A 0.9 process (here: another handle) holding the directory's LOCK blocks
/// the upgrade, which then fails without writing anything.
#[test]
fn a_held_lock_blocks_the_upgrade() {
    let case = Case::new("db-percf");
    let holder = ondadb::legacy_onda::open_read_only(case.opts()).unwrap();
    let err = DB::open(case.opts())
        .map(|_| ())
        .expect_err("the lock is held");
    assert_eq!(err.kind(), "locked", "{err}");
    holder.close().unwrap();
    case.assert_untouched("locked");
    // Released: the upgrade runs.
    let db = DB::open(case.opts()).unwrap();
    assert!(db.last_format_upgrade().is_some());
    db.close().unwrap();
}

#[derive(Default)]
struct Inject {
    fail_at: Option<UpgradePhase>,
    exit_at: Option<UpgradePhase>,
    free: Option<u64>,
}

impl UpgradeObserver for Inject {
    fn on_phase(&self, phase: UpgradePhase) -> ondadb::Result<()> {
        if self.exit_at == Some(phase) {
            // A crash at this boundary: no unwinding, no Drop, no cleanup.
            std::process::exit(0);
        }
        if self.fail_at == Some(phase) {
            return Err(OndaError::Unknown(format!("injected failure at {phase:?}")));
        }
        Ok(())
    }
    fn available_bytes(&self, _dir: &Path) -> Option<u64> {
        self.free
    }
}

#[test]
fn preflight_failures_leave_the_source_byte_identical() {
    // Not enough space.
    let case = Case::new("db-caps");
    let obs = Inject {
        free: Some(1024),
        ..Default::default()
    };
    let err = upgrade::open_observed(case.opts(), &obs).err().unwrap();
    match &err {
        OndaError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::StorageFull, "{err}"),
        other => panic!("expected StorageFull, got {other}"),
    }
    case.assert_untouched("no space");

    // A layout the upgraded database could not be opened with.
    let case = Case::new("db-unified");
    let mut opts = case.opts();
    opts.unified_memtable = false;
    assert_eq!(DB::open(opts).err().unwrap().kind(), "invalid_args");
    case.assert_untouched("layout mismatch");

    // A merge operator... is not needed to upgrade (operands are copied, never
    // folded), but a normal open still insists on it afterwards — which is
    // what the offline binary is for. Here: upgrading without the operator
    // succeeds, and the reopen with it reads everything.
    let case = Case::new("db-caps");
    let mut bare = Options::new(case.dir.to_str().unwrap());
    bare.format_upgrade_keep_backup = true;
    let report = upgrade::upgrade(bare).unwrap().expect("upgraded");
    assert!(!report.resumed);
    case.assert_upgraded("offline, no operator");
    let db = DB::open(case.opts()).unwrap();
    assert_scans_expected(&db, "db-caps");
    db.close().unwrap();
}

/// A failure at every step before the journal removes the upgrade directory
/// and leaves the source exactly as it was; the next open upgrades.
#[test]
fn a_failure_before_the_swap_rolls_back_completely() {
    for phase in UpgradePhase::ALL.into_iter().filter(|p| !p.is_swap()) {
        for name in FIXTURES {
            let case = Case::new(name);
            let obs = Inject {
                fail_at: Some(phase),
                ..Default::default()
            };
            let err = upgrade::open_observed(case.opts(), &obs).err().unwrap();
            assert!(err.to_string().contains("injected failure"), "{err}");
            case.assert_untouched(&format!("{name} failed at {phase:?}"));
            let db = DB::open(case.opts()).unwrap();
            assert_scans_expected(&db, name);
            db.close().unwrap();
            case.assert_upgraded(&format!("{name} after {phase:?}"));
        }
    }
}

/// A failure during the swap is resolved on the spot (the rebuild is
/// verified, so it rolls forward); the open still reports the error, and the
/// next one opens the upgraded database.
#[test]
fn a_failure_during_the_swap_rolls_forward() {
    for phase in UpgradePhase::ALL.into_iter().filter(|p| p.is_swap()) {
        let case = Case::new("db-percf");
        let obs = Inject {
            fail_at: Some(phase),
            ..Default::default()
        };
        let err = upgrade::open_observed(case.opts(), &obs).err().unwrap();
        assert!(err.to_string().contains("injected failure"), "{err}");
        let db = DB::open(case.opts()).unwrap();
        assert_scans_expected(&db, "db-percf");
        db.close().unwrap();
        case.assert_upgraded(&format!("failed at {phase:?}"));
    }
}

/// Tampers with the rebuilt directory before its manifest is written: swaps
/// the contents of two of family `alpha`'s tables, so every per-family total
/// still matches and only the per-table comparison can tell.
struct SwapTables {
    parent: PathBuf,
}

impl UpgradeObserver for SwapTables {
    fn on_phase(&self, phase: UpgradePhase) -> ondadb::Result<()> {
        if phase != UpgradePhase::TablesWritten {
            return Ok(());
        }
        let up = std::fs::read_dir(&self.parent)
            .unwrap()
            .flatten()
            .find(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".db-percf.yolo-upgrade-")
            })
            .expect("the upgrade directory")
            .path()
            .join("cf-alpha");
        let mut ids: Vec<String> = std::fs::read_dir(&up)
            .unwrap()
            .flatten()
            .filter_map(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.strip_suffix(".klog").map(str::to_string)
            })
            .collect();
        ids.sort();
        assert!(ids.len() >= 2, "alpha has a table and a replayed memtable");
        for ext in ["klog", "vlog"] {
            let a = up.join(format!("{}.{ext}", ids[0]));
            let b = up.join(format!("{}.{ext}", ids[1]));
            if a.exists() && b.exists() {
                let tmp = up.join("swap.tmp");
                std::fs::rename(&a, &tmp).unwrap();
                std::fs::rename(&b, &a).unwrap();
                std::fs::rename(&tmp, &b).unwrap();
            }
        }
        Ok(())
    }
}

/// Verification is live: a rebuilt table that does not match its source
/// fails the upgrade before the swap, and the source is untouched.
#[test]
fn verification_catches_a_rebuilt_table_that_differs_from_its_source() {
    let case = Case::new("db-percf");
    let obs = SwapTables {
        parent: case.parent.clone(),
    };
    let err = upgrade::open_observed(case.opts(), &obs)
        .map(|_| ())
        .expect_err("verification must fail");
    assert!(err.kind() == "corruption", "unexpected error kind: {err}");
    assert!(
        err.to_string().contains("format upgrade verification"),
        "{err}"
    );
    case.assert_untouched("verification failure");
}

// ---- the crash matrix ----------------------------------------------------------

const CRASH_DIR_ENV: &str = "ONDA_UPGRADE_CRASH_DIR";
const CRASH_FIXTURE_ENV: &str = "ONDA_UPGRADE_CRASH_FIXTURE";
const CRASH_PHASE_ENV: &str = "ONDA_UPGRADE_CRASH_PHASE";

/// Not a real test: the child half of the crash matrix. Runs only when the
/// env vars are set.
#[test]
fn upgrade_crash_helper() {
    let (Ok(dir), Ok(fixture), Ok(phase)) = (
        std::env::var(CRASH_DIR_ENV),
        std::env::var(CRASH_FIXTURE_ENV),
        std::env::var(CRASH_PHASE_ENV),
    ) else {
        return;
    };
    let phase = UpgradePhase::ALL
        .into_iter()
        .find(|p| format!("{p:?}") == phase)
        .expect("known phase");
    let mut opts = Options::new(dir);
    opts.merge_fns = vec![Arc::new(Concat)];
    opts.unified_memtable = fixture == "db-unified";
    let obs = Inject {
        exit_at: Some(phase),
        ..Default::default()
    };
    let _ = upgrade::open_observed(opts, &obs);
    panic!("the observer should have exited at {phase:?}");
}

fn crash_at(case: &Case, phase: UpgradePhase) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["upgrade_crash_helper", "--exact", "--nocapture"])
        .env(CRASH_DIR_ENV, case.dir.to_str().unwrap())
        .env(CRASH_FIXTURE_ENV, case.name)
        .env(CRASH_PHASE_ENV, format!("{phase:?}"))
        .status()
        .expect("spawn crash helper");
    assert!(status.success(), "crash helper failed at {phase:?}");
}

/// A crash after every protocol step: the next open either completes the swap
/// (journal written) or finds the source untouched and upgrades again — and in
/// both cases reads exactly the fixture's contents, with exactly one backup
/// byte-identical to it and nothing else left behind.
#[test]
fn a_crash_after_every_step_completes_or_rolls_back() {
    for phase in UpgradePhase::ALL {
        for name in FIXTURES {
            let case = Case::new(name);
            crash_at(&case, phase);
            let why = format!("{name} crashed after {phase:?}");
            if phase.is_swap() {
                assert!(case.journal().exists(), "{why}: no journal");
                // A read-only open writes nothing, so it cannot finish the
                // swap; it refuses rather than read a half-swapped path.
                if phase != UpgradePhase::JournalDone {
                    let mut ro = case.opts();
                    ro.read_only = true;
                    assert_eq!(DB::open(ro).err().unwrap().kind(), "invalid_args", "{why}");
                }
            } else {
                // Before the journal the source is untouched by construction
                // (an upgrade directory may be left behind; the next upgrade
                // sweeps it).
                assert_eq!(
                    tree_hash(&case.dir),
                    case.original(),
                    "{why}: source changed"
                );
            }
            let db = DB::open(case.opts()).unwrap_or_else(|e| panic!("{why}: {e}"));
            let report = db.last_format_upgrade().expect("a report");
            assert_eq!(report.resumed, phase.is_swap(), "{why}");
            assert_scans_expected(&db, name);
            db.close().unwrap();
            case.assert_upgraded(&why);
        }
    }
}

/// The rollback branch: the swap was interrupted after the source moved, and
/// the rebuilt directory is then found damaged. The next open must put the
/// source back rather than finish with a broken directory — and then upgrade
/// it afresh.
#[test]
fn a_damaged_rebuild_mid_swap_is_rolled_back() {
    let case = Case::new("db-caps");
    crash_at(&case, UpgradePhase::SourceRenamed);
    assert!(
        !case.dir.exists(),
        "the source should be at its backup name"
    );
    let ups = case.upgrade_dirs();
    assert_eq!(ups.len(), 1);
    std::fs::remove_file(ups[0].join("MANIFEST")).unwrap();
    let db = DB::open(case.opts()).unwrap();
    let report = db.last_format_upgrade().unwrap();
    assert!(!report.resumed, "rolled back, then upgraded afresh");
    assert_scans_expected(&db, "db-caps");
    db.close().unwrap();
    case.assert_upgraded("rolled back and redone");
}

/// The offline entry point (`yolodb upgrade`) completes an interrupted swap
/// too, and is a no-op on an epoch-1 directory.
#[test]
fn offline_upgrade_resumes_and_is_idempotent() {
    let case = Case::new("db-unified");
    crash_at(&case, UpgradePhase::UpgradeRenamed);
    let report = upgrade::upgrade(case.opts()).unwrap().expect("resumed");
    assert!(report.resumed);
    case.assert_upgraded("offline resume");
    assert!(upgrade::upgrade(case.opts()).unwrap().is_none());
    let db = DB::open(case.opts()).unwrap();
    assert!(db.last_format_upgrade().is_none());
    assert_scans_expected(&db, "db-unified");
    db.close().unwrap();
}
