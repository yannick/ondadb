//! Automatic upgrade of ondaDB **0.9.x** directories to yoloDB format epoch 1
//! (plan C §1.3).
//!
//! [`DB::open`] on a directory whose `MANIFEST` is a 0.9 one (`WVMF`) consults
//! [`Options::format_upgrade`]: `Auto` (the default) rebuilds the database into
//! epoch 1 in a sibling directory, verifies it, swaps it into place and opens
//! it; `Forbid` refuses; `ReadOnlyLegacy` — and every read-only open — opens
//! the 0.9 directory read-only through `legacy_onda` without rebuilding.
//!
//! # The protocol
//!
//! The source directory `P` is **never modified before the swap**; every step
//! before it can fail and leave the 0.9 database byte-identical (apart from an
//! empty `LOCK` file, which 0.9 itself always created).
//!
//! 1. `P/LOCK` is taken **exclusively** and held throughout, so no 0.9 process
//!    (and no second upgrader) can write concurrently.
//! 2. Preflight: the requested WAL layout must match the catalog's, no table
//!    may live on a non-default tier or an object store, no prepared
//!    transaction may be unresolved, and the parent filesystem must have room
//!    for the source's bytes plus a margin.
//! 3. `<parent>/.<name>.yolo-upgrade-<nonce>/` (`U`) is created on the same
//!    filesystem, with an `UPGRADE-IN-PROGRESS` marker written first and
//!    `U/LOCK` held exclusively too.
//! 4. The source is opened read-only through `legacy_onda` (WAL replay
//!    included) and **every table is transcoded one-for-one** into an epoch-1
//!    table with the same id, level, partition and age stamps; the replayed
//!    memtables are written as the L0 tables a flush would have produced. Raw
//!    internal entries are copied — sequences, TTLs, tombstones, single
//!    deletes, unfolded merge operands, range-tombstone fragments — so the
//!    rebuilt LSM is the source's LSM, not a re-derivation of its visible
//!    contents. The catalog keeps the `CAP_*` word, the WAL layout and each
//!    family's config (already re-encoded as TLV by `recover_catalog`); the
//!    epoch-1 `MANIFEST` is written **last**. Unified ids revert to the
//!    epoch-1 FNV of the name: no WAL survives the rebuild, so the 0.9 ids
//!    (`legacy_onda::cf_id_09`), which only the replay needed, are not carried.
//! 5. `U` is reopened read-only and compared with the source: per-family entry
//!    counts, range-fragment counts and maximum sequence, plus (under
//!    [`FormatUpgradeVerify::Scan`]) a streaming entry-by-entry comparison of
//!    every rebuilt table against the table or memtable it came from.
//! 6. The swap — two renames under a durable journal
//!    `<parent>/.<name>.yolo-upgrade.journal`: journal `swapping` → rename
//!    `P` → `.<name>.pre-yolo-<nonce>` (the backup) → rename `U` → `P` →
//!    journal `done`, marker removed. Every step is followed by a directory
//!    fsync. A crash anywhere in it is resolved by the next open, which reads
//!    the journal before anything else: a complete `U` is rolled forward, an
//!    incomplete one rolled back.
//! 7. The backup is kept unless [`Options::format_upgrade_keep_backup`] is
//!    `false`, in which case it is deleted once the upgraded database opened.
//! 8. [`DB::last_format_upgrade`] returns an [`UpgradeReport`].
//!
//! [`DB::open`]: crate::DB::open
//! [`DB::last_format_upgrade`]: crate::DB::last_format_upgrade
//! [`Options::format_upgrade`]: crate::Options::format_upgrade
//! [`Options::format_upgrade_keep_backup`]: crate::Options::format_upgrade_keep_backup

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::config::{FormatUpgradeVerify, Options};
use crate::db::DB;
use crate::encoding::{append_u32, read_u32};
use crate::error::{OndaError, Result};
use crate::format::upgrade_journal as uj;

/// The marker file every upgrade directory carries from the moment it exists
/// until the swap is `done`. Empty: only its presence means anything.
pub const MARKER: &str = "UPGRADE-IN-PROGRESS";

/// What one format upgrade did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeReport {
    /// The database path (the one given to the open).
    pub path: PathBuf,
    /// Bytes the 0.9 directory held.
    pub source_bytes: u64,
    /// Bytes the rebuilt epoch-1 directory held at the swap.
    pub upgraded_bytes: u64,
    /// Wall time of the rebuild, verification and swap.
    pub duration: Duration,
    /// Where the replaced 0.9 directory now lives, or `None` once it was
    /// deleted ([`Options::format_upgrade_keep_backup`] = `false`).
    ///
    /// [`Options::format_upgrade_keep_backup`]: crate::Options::format_upgrade_keep_backup
    pub backup_path: Option<PathBuf>,
    /// Column families rebuilt.
    pub column_families: usize,
    /// Epoch-1 tables written (transcoded tables plus replayed memtables).
    pub tables: usize,
    /// Internal entries written (every version, tombstone and operand).
    pub entries: u64,
    /// The highest sequence the rebuilt database holds.
    pub max_seq: u64,
    /// The verification the rebuild passed.
    pub verify: FormatUpgradeVerify,
    /// `true` when this open **completed** an upgrade a crash had interrupted
    /// mid-swap, rather than running one; the counters then describe the
    /// directory as found.
    pub resumed: bool,
}

/// A protocol boundary, reported to an [`UpgradeObserver`] **after** the step
/// it names is complete (and durable, where the step writes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpgradePhase {
    /// Step 2: the lock is held and every preflight check passed. Nothing is
    /// written yet.
    Preflighted,
    /// Step 3: the upgrade directory and its marker exist.
    UpgradeDirCreated,
    /// Step 4: every epoch-1 table is written and synced; no manifest yet.
    TablesWritten,
    /// Step 4 end: the epoch-1 manifest is written — the directory is complete.
    ManifestWritten,
    /// Step 5: the rebuilt directory verified equal to the source.
    Verified,
    /// Step 6.1: the swap journal is durable in state `swapping`.
    JournalWritten,
    /// Step 6.2: the source is renamed to its backup name.
    SourceRenamed,
    /// Step 6.3: the upgrade directory is renamed into the database path.
    UpgradeRenamed,
    /// Step 6.4: the journal says `done` and the marker is gone.
    JournalDone,
}

impl UpgradePhase {
    /// Every phase, in protocol order.
    pub const ALL: [UpgradePhase; 9] = [
        UpgradePhase::Preflighted,
        UpgradePhase::UpgradeDirCreated,
        UpgradePhase::TablesWritten,
        UpgradePhase::ManifestWritten,
        UpgradePhase::Verified,
        UpgradePhase::JournalWritten,
        UpgradePhase::SourceRenamed,
        UpgradePhase::UpgradeRenamed,
        UpgradePhase::JournalDone,
    ];

    /// Whether the phase comes after the journal is written — i.e. whether
    /// the source may already have moved.
    pub fn is_swap(self) -> bool {
        matches!(
            self,
            UpgradePhase::JournalWritten
                | UpgradePhase::SourceRenamed
                | UpgradePhase::UpgradeRenamed
                | UpgradePhase::JournalDone
        )
    }
}

/// Synchronous observer of a format upgrade: progress output for the
/// `yolodb upgrade` binary, and the fault hook of the crash matrix
/// (`tests/format_upgrade.rs`). The observer cannot change what the protocol
/// writes; an `Err` from [`on_phase`](Self::on_phase) fails the upgrade at
/// that boundary exactly as an I/O error there would, and a process exit
/// inside it is a crash at that boundary.
pub trait UpgradeObserver: Send + Sync {
    /// Called after each protocol step.
    fn on_phase(&self, _phase: UpgradePhase) -> Result<()> {
        Ok(())
    }

    /// Called after each table (transcoded or flushed) is written: `done` of
    /// `total` in family `cf`. `total` counts transcoded tables only; replayed
    /// memtables report `done > total`.
    fn on_table(&self, _cf: &str, _done: usize, _total: usize) {}

    /// Free bytes on the upgrade directory's filesystem, **overriding** the
    /// measured value. A test lever for the free-space preflight; `None` (the
    /// default) measures.
    fn available_bytes(&self, _dir: &Path) -> Option<u64> {
        None
    }
}

/// The observer that observes nothing — what [`DB::open`](crate::DB::open)
/// passes.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoObserver;

impl UpgradeObserver for NoObserver {}

// ---- paths -------------------------------------------------------------------

/// The four names the protocol derives from a database path.
#[derive(Debug, Clone)]
struct Paths {
    /// The database directory, absolute.
    db: PathBuf,
    parent: PathBuf,
    name: String,
    journal: PathBuf,
}

impl Paths {
    fn new(path: &str) -> Result<Paths> {
        let mut abs = std::path::absolute(path)?;
        if abs.file_name().is_none() {
            // `..`, `/` and friends: only the resolved directory has a name.
            abs = std::fs::canonicalize(&abs)?;
        }
        let name = abs
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                OndaError::InvalidArgs(format!(
                    "{path}: the database directory needs a UTF-8 name for a format upgrade"
                ))
            })?
            .to_string();
        let parent = abs
            .parent()
            .ok_or_else(|| OndaError::InvalidArgs(format!("{path}: no parent directory")))?
            .to_path_buf();
        let journal = parent.join(format!(".{name}.yolo-upgrade.journal"));
        Ok(Paths {
            db: abs,
            parent,
            name,
            journal,
        })
    }

    fn upgrade_prefix(&self) -> String {
        format!(".{}.yolo-upgrade-", self.name)
    }

    fn backup_prefix(&self) -> String {
        format!(".{}.pre-yolo-", self.name)
    }

    fn journal_tmp(&self) -> PathBuf {
        self.journal.with_extension("journal.tmp")
    }
}

fn utf8(p: &Path) -> Result<&str> {
    p.to_str()
        .ok_or_else(|| OndaError::InvalidArgs(format!("{}: path is not UTF-8", p.display())))
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Total bytes of every regular file under `dir` (symlinks not followed).
fn dir_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    for e in rd.flatten() {
        let Ok(md) = e.path().symlink_metadata() else {
            continue;
        };
        if md.is_dir() {
            total += dir_size(&e.path());
        } else if md.is_file() {
            total += md.len();
        }
    }
    total
}

fn rename_durably(from: &Path, to: &Path, parent: &Path) -> Result<()> {
    crate::util::fault::check(crate::util::fault::Call::Rename)?;
    std::fs::rename(from, to)?;
    sync_dir(parent)
}

fn remove_dir_durably(dir: &Path, parent: &Path) -> Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    sync_dir(parent)
}

fn remove_marker(dir: &Path) -> Result<()> {
    match std::fs::remove_file(dir.join(MARKER)) {
        Ok(()) => sync_dir(dir),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

// ---- the journal ---------------------------------------------------------------

/// The swap journal (`crate::format::upgrade_journal`). Names, never paths:
/// all three directories are siblings in the journal's own directory, and a
/// decoded name is refused unless it is a plain file name with the prefix the
/// protocol gives it — a damaged or planted journal cannot steer a rename or a
/// `remove_dir_all` anywhere else.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Journal {
    state: u8,
    name: String,
    upgrade: String,
    backup: String,
}

impl Journal {
    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(64);
        b.extend_from_slice(&uj::MAGIC);
        append_u32(&mut b, uj::VERSION);
        b.push(self.state);
        for s in [&self.name, &self.upgrade, &self.backup] {
            append_u32(&mut b, s.len() as u32);
            b.extend_from_slice(s.as_bytes());
        }
        let crc = crate::encoding::checksum(&b);
        append_u32(&mut b, crc);
        b
    }

    fn decode(data: &[u8], paths: &Paths) -> Result<Journal> {
        let bad = |what: &str| {
            OndaError::Corruption(format!(
                "format upgrade journal {}: {what}",
                paths.journal.display()
            ))
        };
        if data.len() < 8 + 4 + 1 + 3 * 4 + 4 || data[..8] != uj::MAGIC {
            return Err(bad("not a YOLODBUJ journal"));
        }
        let (body, crc) = data.split_at(data.len() - 4);
        if read_u32(crc) != crate::encoding::checksum(body) {
            return Err(bad("checksum mismatch"));
        }
        let version = read_u32(&body[8..]);
        if version != uj::VERSION {
            return Err(OndaError::UnsupportedFormat(format!(
                "format upgrade journal {}: version {version}",
                paths.journal.display()
            )));
        }
        let state = body[12];
        if state != uj::STATE_SWAPPING && state != uj::STATE_DONE {
            return Err(bad(&format!("unknown state {state}")));
        }
        let mut p = &body[13..];
        let mut names = Vec::with_capacity(3);
        for _ in 0..3 {
            if p.len() < 4 {
                return Err(bad("truncated"));
            }
            let len = read_u32(p) as usize;
            p = &p[4..];
            if p.len() < len {
                return Err(bad("truncated"));
            }
            let s = std::str::from_utf8(&p[..len]).map_err(|_| bad("name is not UTF-8"))?;
            names.push(s.to_string());
            p = &p[len..];
        }
        if !p.is_empty() {
            return Err(bad("trailing bytes"));
        }
        let [name, upgrade, backup]: [String; 3] = names.try_into().expect("three names");
        let plain = |s: &str, prefix: &str| {
            s.len() > prefix.len()
                && s.starts_with(prefix)
                && !s.contains('/')
                && !s.contains('\\')
                && !s.contains('\0')
        };
        if name != paths.name
            || !plain(&upgrade, &paths.upgrade_prefix())
            || !plain(&backup, &paths.backup_prefix())
        {
            return Err(bad("names do not belong to this database"));
        }
        Ok(Journal {
            state,
            name,
            upgrade,
            backup,
        })
    }
}

/// Replace the journal atomically: temp file, fsync, rename, parent fsync.
fn write_journal(paths: &Paths, j: &Journal) -> Result<()> {
    use std::io::Write as _;
    let tmp = paths.journal_tmp();
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        f.write_all(&j.encode())?;
        f.sync_all()?;
    }
    rename_durably(&tmp, &paths.journal, &paths.parent)
}

fn read_journal(paths: &Paths) -> Result<Option<Journal>> {
    match std::fs::read(&paths.journal) {
        Ok(data) => Journal::decode(&data, paths).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn remove_journal(paths: &Paths) -> Result<()> {
    let _ = std::fs::remove_file(paths.journal_tmp());
    match std::fs::remove_file(&paths.journal) {
        Ok(()) => sync_dir(&paths.parent),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

// ---- crash recovery ---------------------------------------------------------------

/// Whether `dir` holds a complete epoch-1 database: a `MANIFEST` whose catalog
/// recovers, and every table file it names present at its recorded size. The
/// journal is written only after verification, so a directory the journal
/// names fails this only if something outside the protocol damaged it.
fn catalog_complete(dir: &Path) -> bool {
    if !crate::manifest::manifest_path(dir).is_file() {
        return false;
    }
    let Ok(m) = crate::manifest_edit::recover_catalog(dir) else {
        return false;
    };
    m.cfs.iter().all(|cf| {
        let cf_dir = dir.join(crate::format::cf_dir_name(&cf.name));
        cf_dir.is_dir()
            && cf.sstables.iter().all(|t| {
                let size = |ext: &str| {
                    std::fs::metadata(cf_dir.join(format!("{}.{ext}", t.id)))
                        .map(|m| m.len())
                        .ok()
                };
                size("klog") == Some(t.klog_size)
                    && (t.vlog_size == 0 || size("vlog") == Some(t.vlog_size))
            })
    })
}

#[derive(Debug, PartialEq, Eq)]
enum Recovered {
    /// The swap is complete: `P` is the epoch-1 database.
    Completed,
    /// The swap was undone: `P` is the untouched 0.9 database again.
    RolledBack,
}

/// Mark the swap `done` and drop the marker from the now-live directory.
fn mark_done(paths: &Paths, j: &Journal) -> Result<()> {
    let done = Journal {
        state: uj::STATE_DONE,
        ..j.clone()
    };
    write_journal(paths, &done)?;
    remove_marker(&paths.db)
}

/// Resolve an interrupted swap. The caller holds the lock that excludes a live
/// upgrader (see [`resolve_interrupted`]).
fn recover_locked(paths: &Paths, j: &Journal) -> Result<Recovered> {
    let p = &paths.db;
    let u = paths.parent.join(&j.upgrade);
    let b = paths.parent.join(&j.backup);
    let (p_exists, u_exists, b_exists) = (p.exists(), u.exists(), b.exists());
    if j.state == uj::STATE_DONE {
        remove_marker(p)?;
        return Ok(Recovered::Completed);
    }
    if u_exists && catalog_complete(&u) {
        // Roll forward: the rebuilt directory was verified before the journal
        // existed, so finishing the swap is always right.
        match (p_exists, b_exists) {
            (true, false) => rename_durably(p, &b, &paths.parent)?,
            (false, _) => {}
            (true, true) => {
                return Err(OndaError::Corruption(format!(
                    "format upgrade of {}: the database, its backup {} and the upgrade \
                     directory {} all exist; resolve by hand (the backup is the 0.9 \
                     original)",
                    p.display(),
                    b.display(),
                    u.display()
                )))
            }
        }
        rename_durably(&u, p, &paths.parent)?;
        mark_done(paths, j)?;
        return Ok(Recovered::Completed);
    }
    if !u_exists && p_exists && b_exists {
        // Both renames happened; only the journal update was lost.
        if crate::manifest::is_onda09_dir(p)? || !catalog_complete(p) {
            return Err(OndaError::Corruption(format!(
                "format upgrade of {}: journal says swapping, the backup {} exists, but the \
                 database directory is not a complete epoch-1 database; resolve by hand",
                p.display(),
                b.display()
            )));
        }
        mark_done(paths, j)?;
        return Ok(Recovered::Completed);
    }
    // Roll back: put the source back where it was and drop the rebuild.
    if !p_exists && b_exists {
        rename_durably(&b, p, &paths.parent)?;
    }
    if u_exists {
        remove_dir_durably(&u, &paths.parent)?;
    }
    remove_journal(paths)?;
    Ok(Recovered::RolledBack)
}

/// Resolve a swap a crash interrupted, before `opts.path` is read at all.
///
/// Returns the report of a **completed** upgrade (the caller finalizes it
/// after its open), `None` when there was nothing to do or the swap was rolled
/// back. A read-only open writes nothing: a `done` journal is ignored, and a
/// `swapping` one — the database path may not even exist — is refused.
fn resolve_interrupted(opts: &Options) -> Result<Option<UpgradeReport>> {
    let Ok(paths) = Paths::new(&opts.path) else {
        return Ok(None);
    };
    let Some(j) = read_journal(&paths)? else {
        return Ok(None);
    };
    if opts.read_only {
        if j.state == uj::STATE_DONE {
            return Ok(None);
        }
        return Err(OndaError::InvalidArgs(format!(
            "{}: a format upgrade was interrupted mid-swap (journal {}); a read-write \
             open completes or rolls it back",
            paths.db.display(),
            paths.journal.display()
        )));
    }
    let t0 = Instant::now();
    // The lock that excludes a live upgrader: it holds the source's LOCK
    // wherever the source is (P, or B after the first rename) and the upgrade
    // directory's (U, or P after the second), so whichever of them exists
    // first in this order is held by it if it is still running.
    let b = paths.parent.join(&j.backup);
    let u = paths.parent.join(&j.upgrade);
    let lock_dir = [&b, &paths.db, &u]
        .into_iter()
        .find(|d| d.is_dir())
        .cloned();
    let _lock = match &lock_dir {
        Some(d) => Some(
            crate::db::acquire_dir_lock(utf8(d)?, false).map_err(|e| match e {
                OndaError::Locked(_) => OndaError::Locked(format!(
                    "{}: a format upgrade is in progress in another process or handle",
                    paths.db.display()
                )),
                other => other,
            })?,
        ),
        None => None,
    };
    // Re-read under the lock: the upgrader may have finished meanwhile.
    let Some(j) = read_journal(&paths)? else {
        return Ok(None);
    };
    match recover_locked(&paths, &j)? {
        Recovered::RolledBack => Ok(None),
        Recovered::Completed => {
            let catalog = crate::manifest_edit::recover_catalog(&paths.db)?;
            let tables = catalog.cfs.iter().map(|c| c.sstables.len()).sum();
            let entries = catalog
                .cfs
                .iter()
                .flat_map(|c| c.sstables.iter())
                .map(|t| t.num_entries)
                .sum();
            let backup = paths.parent.join(&j.backup);
            Ok(Some(UpgradeReport {
                path: PathBuf::from(&opts.path),
                source_bytes: dir_size(&backup),
                upgraded_bytes: dir_size(&paths.db),
                duration: t0.elapsed(),
                backup_path: backup.exists().then_some(backup),
                column_families: catalog.cfs.len(),
                tables,
                entries,
                max_seq: catalog.global_seq,
                verify: opts.format_upgrade_verify,
                resumed: true,
            }))
        }
    }
}

/// After the upgraded database opened: drop the backup if asked to, then the
/// journal. Best effort — the database is already open and correct, and a
/// journal left in state `done` makes the next open retry exactly this.
fn finalize(opts: &Options, report: &mut UpgradeReport) {
    let Ok(paths) = Paths::new(&opts.path) else {
        return;
    };
    if !opts.format_upgrade_keep_backup {
        if let Some(b) = &report.backup_path {
            if remove_dir_durably(b, &paths.parent).is_err() {
                return;
            }
        }
        report.backup_path = None;
    }
    let _ = remove_journal(&paths);
}

// ---- entry points -------------------------------------------------------------------

/// [`DB::open`](crate::DB::open) with an observer on the format upgrade — the
/// crash matrix's entry point. Identical to `DB::open` for an epoch-1
/// directory.
pub fn open_observed(opts: Options, observer: &dyn UpgradeObserver) -> Result<DB> {
    if opts.path.is_empty() {
        return DB::open_epoch1(opts);
    }
    let resumed = resolve_interrupted(&opts)?;
    if crate::manifest::is_onda09_dir(&opts.path)? {
        return open_legacy(opts, observer);
    }
    let db = DB::open_epoch1(opts.clone())?;
    if let Some(mut report) = resumed {
        finalize(&opts, &mut report);
        *db.inner.format_upgrade.lock() = Some(report);
    }
    Ok(db)
}

#[cfg(not(feature = "legacy-onda"))]
fn open_legacy(opts: Options, _observer: &dyn UpgradeObserver) -> Result<DB> {
    Err(legacy_feature_missing(&opts.path))
}

#[cfg(not(feature = "legacy-onda"))]
fn legacy_feature_missing(path: &str) -> OndaError {
    OndaError::UnsupportedFormat(format!(
        "{path}: an ondaDB 0.9 database; this binary was built without the `legacy-onda` \
         feature, which is what reads and upgrades the 0.9 format"
    ))
}

#[cfg(feature = "legacy-onda")]
fn open_legacy(opts: Options, observer: &dyn UpgradeObserver) -> Result<DB> {
    use crate::config::FormatUpgrade;
    match opts.format_upgrade {
        FormatUpgrade::Forbid => Err(OndaError::UnsupportedFormat(
            "ondaDB 0.9 format; format upgrade forbidden by Options::format_upgrade".into(),
        )),
        FormatUpgrade::ReadOnlyLegacy => crate::legacy_onda::open_read_only(opts),
        FormatUpgrade::Auto if opts.read_only => crate::legacy_onda::open_read_only(opts),
        FormatUpgrade::Auto => {
            let report = run_upgrade(&opts, observer, true)?;
            let db = DB::open_epoch1(opts.clone())?;
            if let Some(mut report) = report {
                finalize(&opts, &mut report);
                *db.inner.format_upgrade.lock() = Some(report);
            }
            Ok(db)
        }
    }
}

/// Upgrade the database at `opts.path` without opening it — what
/// `yolodb upgrade` runs. Completes an interrupted swap first. Returns `None`
/// for a directory that is already epoch 1. The WAL layout is taken from the
/// 0.9 catalog, so `opts.unified_memtable` need not match it;
/// `opts.format_upgrade` is ignored (calling this *is* the request).
pub fn upgrade(opts: Options) -> Result<Option<UpgradeReport>> {
    upgrade_observed(opts, &NoObserver)
}

/// [`upgrade`] with an observer (progress output, fault injection).
pub fn upgrade_observed(
    opts: Options,
    observer: &dyn UpgradeObserver,
) -> Result<Option<UpgradeReport>> {
    if opts.path.is_empty() {
        return Err(OndaError::InvalidArgs("empty path".into()));
    }
    if let Some(mut report) = resolve_interrupted(&opts)? {
        finalize(&opts, &mut report);
        return Ok(Some(report));
    }
    if !crate::manifest::is_onda09_dir(&opts.path)? {
        return Ok(None);
    }
    #[cfg(feature = "legacy-onda")]
    {
        let Some(mut report) = run_upgrade(&opts, observer, false)? else {
            return Ok(None);
        };
        finalize(&opts, &mut report);
        Ok(Some(report))
    }
    #[cfg(not(feature = "legacy-onda"))]
    {
        let _ = observer;
        Err(legacy_feature_missing(&opts.path))
    }
}

// ---- the rebuild (legacy-onda only) ----------------------------------------------------

#[cfg(feature = "legacy-onda")]
mod rebuild {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::column_family::ColumnFamily;
    use crate::manifest::{Manifest, SstMeta, WalLayout};
    use crate::memtable::Entry;
    use crate::range_tombstone::Fragment;
    use crate::sst::Reader;

    /// Stands in for a merge operator the caller did not register. The rebuild
    /// copies operands and never folds one (a read-only open runs no
    /// compaction, and verification compares raw entries), so it is never
    /// invoked; it exists so a family written with an operator opens at all —
    /// which is what lets `yolodb upgrade` run without the application's code.
    #[derive(Debug)]
    struct OpaqueMerge(String);

    impl crate::config::MergeOperator for OpaqueMerge {
        fn name(&self) -> &str {
            &self.0
        }
        fn full_merge(
            &self,
            _key: &[u8],
            _existing: Option<&[u8]>,
            _operands: &[&[u8]],
        ) -> std::result::Result<Vec<u8>, String> {
            Err(format!(
                "merge operator {:?} is not registered; the format upgrade copies operands \
                 without folding them",
                self.0
            ))
        }
    }

    /// The partitioner counterpart of [`OpaqueMerge`]: consulted only by
    /// compaction, which a read-only open never runs.
    #[derive(Debug)]
    struct OpaquePartition(String);

    impl crate::config::PartitionFn for OpaquePartition {
        fn boundary_len(&self, key: &[u8]) -> usize {
            key.len()
        }
        fn name(&self, _key: &[u8]) -> String {
            String::new()
        }
        fn scheme_name(&self) -> &str {
            &self.0
        }
    }

    /// The options every internal open of the upgrade uses: read-only, no
    /// shared read resources, and a placeholder for every merge operator or
    /// derived partitioner the catalog names but `opts` does not register.
    pub(super) fn source_options(opts: &Options, catalog: &Manifest) -> Result<Options> {
        let mut o = opts.clone();
        o.read_only = true;
        o.migrate_to_unified = false;
        o.read_resources = None;
        o.read_cache_namespace = None;
        o.unified_memtable = catalog.wal_layout == WalLayout::Unified;
        for cf in &catalog.cfs {
            let cfg = crate::ColumnFamilyConfig::decode(&cf.config)?;
            if let Some(name) = cfg.merge_operator_name {
                if !o.merge_fns.iter().any(|m| m.name() == name) {
                    o.merge_fns.push(Arc::new(OpaqueMerge(name)));
                }
            }
            if let crate::config::PartitionScheme::Unresolved(name) = cfg.partition_scheme {
                if !o.partition_fns.iter().any(|p| p.scheme_name() == name) {
                    o.partition_fns.push(Arc::new(OpaquePartition(name)));
                }
            }
        }
        Ok(o)
    }

    /// Where one rebuilt table's contents came from, for verification.
    enum Origin {
        /// Transcoded from this source table.
        Table(Box<SstMeta>),
        /// Written from a replayed memtable. The entries are kept only under
        /// `Scan` verification.
        Memtable {
            entries: Vec<Entry>,
            fragments: Vec<Fragment>,
        },
    }

    struct BuiltTable {
        cf: String,
        meta: SstMeta,
        origin: Origin,
    }

    /// Source-side totals of one family, counted while writing.
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    struct Totals {
        entries: u64,
        fragments: u64,
        max_seq: u64,
    }

    impl Totals {
        fn add_fragments(&mut self, frags: &[Fragment]) {
            self.fragments += frags.len() as u64;
            for f in frags {
                if let Some(&s) = f.seqs.first() {
                    self.max_seq = self.max_seq.max(s);
                }
            }
        }
    }

    pub(super) struct Built {
        manifest: Manifest,
        tables: Vec<BuiltTable>,
        totals: HashMap<String, Totals>,
    }

    impl Built {
        pub(super) fn entries(&self) -> u64 {
            self.totals.values().map(|t| t.entries).sum()
        }
        pub(super) fn tables(&self) -> usize {
            self.tables.len()
        }
        pub(super) fn families(&self) -> usize {
            self.manifest.cfs.len()
        }
        pub(super) fn max_seq(&self) -> u64 {
            self.manifest.global_seq
        }
    }

    fn cf_of(db: &DB, name: &str) -> Result<Arc<ColumnFamily>> {
        db.get_column_family(name).ok_or_else(|| {
            OndaError::Corruption(format!(
                "format upgrade: column family {name:?} is in the catalog but did not open"
            ))
        })
    }

    /// Transcode one 0.9 table into an epoch-1 table with the same id at
    /// `dst_klog`. `None` when the table holds nothing at all.
    fn transcode(
        cf: &Arc<ColumnFamily>,
        src: &SstMeta,
        dst_klog: &str,
        bottom: bool,
        totals: &mut Totals,
    ) -> Result<Option<SstMeta>> {
        let reader = cf.open_reader_for(src)?;
        let fragments = reader.range_fragments().to_vec();
        let mut wo = crate::compaction::cf_writer_opts(cf, &cf.cmp(), src.level, bottom);
        // A table the source wrote extended (kind-bearing) stays extended
        // whatever the family's current gates say: its merge operands or
        // fragments have nowhere else to live.
        if reader.entry_layout() == crate::sst::EntryLayout::Extended || !fragments.is_empty() {
            wo.extended_entries = true;
        }
        wo.expected_entries = reader.num_entries() as usize;
        let mut w = crate::sst::Writer::new(dst_klog, wo)?;
        w.set_range_fragments(fragments.clone());
        let mut it = reader.iter();
        it.seek_to_first();
        let mut value = Vec::new();
        let mut n = 0u64;
        while it.valid() {
            value.clear();
            it.value_into(&mut value)?;
            w.add(it.user_key(), &value, it.seq(), it.ttl(), it.kind())?;
            totals.max_seq = totals.max_seq.max(it.seq());
            n += 1;
            it.next();
        }
        if let Some(e) = it.err() {
            return Err(e.duplicate());
        }
        if n == 0 && fragments.is_empty() {
            w.abort();
            return Ok(None);
        }
        totals.entries += n;
        totals.add_fragments(&fragments);
        let file = w.finish()?;
        let mut meta = file.to_sst_meta(src.id, src.level);
        // Stamps the writer cannot know: they describe the table's lineage,
        // which a transcode does not change.
        meta.partition = src.partition.clone();
        meta.max_entry_time = src.max_entry_time;
        meta.last_compaction_time = src.last_compaction_time;
        Ok(Some(meta))
    }

    /// Step 4: write every family of `legacy` into `up` and return the
    /// epoch-1 catalog, written last.
    pub(super) fn build(
        legacy: &DB,
        catalog: &Manifest,
        up: &Path,
        keep_entries: bool,
        observer: &dyn UpgradeObserver,
    ) -> Result<Built> {
        let mut manifest = catalog.clone();
        let mut tables = Vec::new();
        let mut totals: HashMap<String, Totals> = HashMap::new();
        let mut max_id = 0u64;
        for cfm in &mut manifest.cfs {
            let cf = cf_of(legacy, &cfm.name)?;
            let dir = up.join(crate::format::cf_dir_name(&cfm.name));
            std::fs::create_dir(&dir)?;
            // The level shape, for the bottom-level filter policy (the same
            // predicate compaction output uses).
            let depth = cfm
                .sstables
                .iter()
                .map(|t| t.level as usize + 1)
                .max()
                .unwrap_or(1);
            let mut shape: Vec<Vec<()>> = vec![Vec::new(); depth];
            for t in &cfm.sstables {
                shape[t.level as usize].push(());
            }
            let total = cfm.sstables.len();
            let t = totals.entry(cfm.name.clone()).or_default();
            let mut rebuilt = Vec::with_capacity(total);
            for (i, src) in cfm.sstables.iter().enumerate() {
                let klog = dir.join(format!("{}.klog", src.id));
                let bottom = crate::compaction::target_is_bottom(&shape, src.level as usize);
                if let Some(meta) = transcode(&cf, src, utf8(&klog)?, bottom, t)? {
                    max_id = max_id.max(meta.id);
                    tables.push(BuiltTable {
                        cf: cfm.name.clone(),
                        meta: meta.clone(),
                        origin: Origin::Table(Box::new(src.clone())),
                    });
                    rebuilt.push(meta);
                }
                observer.on_table(&cfm.name, i + 1, total);
            }
            cfm.sstables = rebuilt;
            // No WAL survives the rebuild, so the 0.9 ids the replay needed go:
            // every family takes the epoch-1 id derived from its name.
            cfm.unified_id = None;
        }
        write_memtables(
            legacy,
            &mut manifest,
            up,
            keep_entries,
            &mut tables,
            &mut totals,
            observer,
        )?;
        carry_foreign_entries(&legacy.inner.dir, up)?;
        for t in &tables {
            max_id = max_id.max(t.meta.id);
        }
        let data_max = totals.values().map(|t| t.max_seq).max().unwrap_or(0);
        manifest.global_seq = manifest
            .global_seq
            .max(legacy.inner.visible_seq())
            .max(data_max);
        manifest.next_file_id = manifest.next_file_id.max(max_id + 1);
        observer.on_phase(UpgradePhase::TablesWritten)?;
        // Last, and durably: `save` is temp + fsync + rename + fsync of `up`,
        // which also makes every `cf-*` directory entry durable (each table's
        // own entry was synced by `Writer::finish`).
        manifest.save(crate::manifest::manifest_path(up))?;
        observer.on_phase(UpgradePhase::ManifestWritten)?;
        Ok(Built {
            manifest,
            tables,
            totals,
        })
    }

    /// Whether a top-level entry of a 0.9 directory is the engine's own — state
    /// the rebuild re-derives (catalog, WALs, tables) or a lock/temp file.
    fn is_engine_entry(name: &str) -> bool {
        name == "LOCK"
            || name == super::MARKER
            || name.starts_with("MANIFEST")
            || name.starts_with("unified-wal-")
            || name.starts_with(crate::format::CF_DIR_PREFIX)
    }

    /// Copy every top-level entry the engine does not own (an operator's notes,
    /// an application's sidecar file) from the source into the rebuild, so the
    /// swap does not strand it in the backup — where
    /// `format_upgrade_keep_backup = false` would delete it.
    fn carry_foreign_entries(src: &str, up: &Path) -> Result<()> {
        fn copy(from: &Path, to: &Path) -> Result<()> {
            let md = from.symlink_metadata()?;
            if md.is_dir() {
                std::fs::create_dir(to)?;
                for e in std::fs::read_dir(from)? {
                    let e = e?;
                    copy(&e.path(), &to.join(e.file_name()))?;
                }
                sync_dir(to)
            } else if md.is_file() {
                std::fs::copy(from, to)?;
                std::fs::File::open(to)?.sync_all()?;
                Ok(())
            } else {
                // A symlink or special file is recreated by nobody: leaving it
                // in the backup is the only thing that does not guess.
                Ok(())
            }
        }
        for e in std::fs::read_dir(src)? {
            let e = e?;
            let Some(name) = e.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if !is_engine_entry(&name) {
                copy(&e.path(), &up.join(&name))?;
            }
        }
        Ok(())
    }

    /// The replayed WAL tail: seal every memtable (a read-only open runs no
    /// flush) and write each as the L0 table a flush would have produced — the
    /// read-only checkpoint's path (`DB::write_sealed_memtables`), here with
    /// the contents kept for verification.
    fn write_memtables(
        legacy: &DB,
        manifest: &mut Manifest,
        up: &Path,
        keep_entries: bool,
        tables: &mut Vec<BuiltTable>,
        totals: &mut HashMap<String, Totals>,
        observer: &dyn UpgradeObserver,
    ) -> Result<()> {
        let cfs: Vec<Arc<ColumnFamily>> = manifest
            .cfs
            .iter()
            .map(|c| cf_of(legacy, &c.name))
            .collect::<Result<_>>()?;
        for cf in &cfs {
            legacy.flush_memtable(cf)?;
        }
        if let Some(u) = &legacy.inner.unified {
            u.rotate(true);
        }
        // Per family, oldest first.
        let mut added: Vec<(String, SstMeta, Vec<Entry>, Vec<Fragment>)> = Vec::new();
        let mut write =
            |cf: &ColumnFamily, entries: Vec<Entry>, fragments: Vec<Fragment>| -> Result<()> {
                let id = legacy.inner.next_file_id();
                let klog = up
                    .join(crate::format::cf_dir_name(cf.name()))
                    .join(format!("{id}.klog"));
                if let Some(meta) =
                    cf.write_detached_l0(utf8(&klog)?, &entries, fragments.clone(), id)?
                {
                    let t = totals.entry(cf.name().to_string()).or_default();
                    t.entries += entries.len() as u64;
                    for e in &entries {
                        t.max_seq = t.max_seq.max(e.seq);
                    }
                    t.add_fragments(&fragments);
                    observer.on_table(cf.name(), usize::MAX, 0);
                    added.push((cf.name().to_string(), meta, entries, fragments));
                }
                Ok(())
            };
        // Per-family memtables hold data older than the unified store's.
        for cf in &cfs {
            for (entries, fragments) in cf.sealed_contents() {
                write(cf, entries, fragments)?;
            }
        }
        if let Some(u) = &legacy.inner.unified {
            for imm in u.sealed() {
                let mut ranges = crate::unified::split_ranges_by_cf(&imm);
                let mut slices = crate::unified::split_by_cf(&imm);
                for (cf_id, _) in &ranges {
                    if !slices.iter().any(|(id, _)| id == cf_id) {
                        slices.push((*cf_id, Vec::new()));
                    }
                }
                for (cf_id, mut entries) in slices {
                    // Keyed by the family's *stored* id — for a 0.9 catalog the
                    // truncated-basis `cf_id_09` its WAL keys carry. Records of
                    // a family that no longer exists are owned by nobody.
                    let Some(cf) = cfs.iter().find(|cf| cf.id() == cf_id) else {
                        continue;
                    };
                    let fragments = ranges
                        .iter_mut()
                        .find(|(id, _)| *id == cf_id)
                        .map(|(_, f)| std::mem::take(f))
                        .unwrap_or_default();
                    cf.sort_internal(&mut entries);
                    write(cf, entries, fragments)?;
                }
            }
        }
        for (name, meta, entries, fragments) in added {
            let cfm = manifest
                .cfs
                .iter_mut()
                .find(|c| c.name == name)
                .ok_or(OndaError::NotFound)?;
            // Newest first in L0, as the catalog lists it.
            cfm.sstables.insert(0, meta.clone());
            tables.push(BuiltTable {
                cf: name,
                meta,
                origin: Origin::Memtable {
                    entries: if keep_entries { entries } else { Vec::new() },
                    fragments,
                },
            });
        }
        Ok(())
    }

    /// Receives one source entry: `(user_key, value, seq, ttl, kind)`.
    type EntrySink<'a> = dyn FnMut(&[u8], &[u8], u64, i64, u64) -> Result<()> + 'a;

    fn mismatch(cf: &str, id: u64, what: String) -> OndaError {
        OndaError::Corruption(format!(
            "format upgrade verification: family {cf:?}, table {id}: {what}"
        ))
    }

    /// Compare a rebuilt table entry by entry with the stream it came from.
    fn compare_table(
        cf: &str,
        new: &Arc<Reader>,
        source: &mut dyn FnMut(&mut EntrySink<'_>) -> Result<()>,
    ) -> Result<()> {
        let id = new.file_id();
        let mut it = new.iter();
        it.seek_to_first();
        let mut value = Vec::new();
        let mut i = 0u64;
        source(&mut |key, val, seq, ttl, kind| {
            if !it.valid() {
                return Err(mismatch(cf, id, format!("ends after {i} entries")));
            }
            value.clear();
            it.value_into(&mut value)?;
            if it.user_key() != key
                || it.seq() != seq
                || it.ttl() != ttl
                || it.kind() != kind
                || value.as_slice() != val
            {
                return Err(mismatch(
                    cf,
                    id,
                    format!(
                        "entry {i} differs (key {:?} seq {} vs source key {:?} seq {seq})",
                        it.user_key(),
                        it.seq(),
                        key
                    ),
                ));
            }
            i += 1;
            it.next();
            Ok(())
        })?;
        if let Some(e) = it.err() {
            return Err(e.duplicate());
        }
        if it.valid() {
            return Err(mismatch(
                cf,
                id,
                format!("holds more than the source's {i} entries"),
            ));
        }
        Ok(())
    }

    /// Step 5: reopen `up` read-only and compare it with the source.
    pub(super) fn verify(
        built: &Built,
        legacy: &DB,
        verify_opts: Options,
        up_lock: &std::fs::File,
        mode: FormatUpgradeVerify,
    ) -> Result<()> {
        let vdb = DB::open_with_format(
            verify_opts,
            crate::format::FormatProfile::Epoch1,
            Some(up_lock.try_clone()?),
        )?;
        let got = vdb.inner.visible_seq();
        if got < built.manifest.global_seq {
            return Err(OndaError::Corruption(format!(
                "format upgrade verification: rebuilt database reads at sequence {got}, the \
                 source at {}",
                built.manifest.global_seq
            )));
        }
        for cfm in &built.manifest.cfs {
            let vcf = cf_of(&vdb, &cfm.name)?;
            // The catalog the reopen loaded is the one that was written.
            let mut loaded: Vec<(u64, u32)> = vcf
                .table_metadata()
                .iter()
                .flatten()
                .map(|m| (m.id, m.level))
                .collect();
            let mut want: Vec<(u64, u32)> = cfm.sstables.iter().map(|m| (m.id, m.level)).collect();
            loaded.sort_unstable();
            want.sort_unstable();
            if loaded != want {
                return Err(OndaError::Corruption(format!(
                    "format upgrade verification: family {:?} reopened with tables {loaded:?}, \
                     {want:?} were written",
                    cfm.name
                )));
            }
            let mut seen = Totals::default();
            for t in built.tables.iter().filter(|t| t.cf == cfm.name) {
                let r = vcf.open_reader_for(&t.meta)?;
                seen.entries += r.num_entries();
                seen.max_seq = seen.max_seq.max(r.max_seq());
                seen.add_fragments(r.range_fragments());
                if mode != FormatUpgradeVerify::Scan {
                    continue;
                }
                let want_frags: Vec<Fragment> = match &t.origin {
                    Origin::Table(src) => {
                        let src_cf = cf_of(legacy, &cfm.name)?;
                        let src_reader = src_cf.open_reader_for(src)?;
                        compare_table(&cfm.name, &r, &mut |f| {
                            let mut it = src_reader.iter();
                            it.seek_to_first();
                            let mut v = Vec::new();
                            while it.valid() {
                                v.clear();
                                it.value_into(&mut v)?;
                                f(it.user_key(), &v, it.seq(), it.ttl(), it.kind())?;
                                it.next();
                            }
                            match it.err() {
                                Some(e) => Err(e.duplicate()),
                                None => Ok(()),
                            }
                        })?;
                        src_reader.range_fragments().to_vec()
                    }
                    Origin::Memtable { entries, fragments } => {
                        compare_table(&cfm.name, &r, &mut |f| {
                            for e in entries {
                                f(&e.user_key, &e.value, e.seq, e.ttl, e.kind)?;
                            }
                            Ok(())
                        })?;
                        fragments.clone()
                    }
                };
                if r.range_fragments() != want_frags.as_slice() {
                    return Err(mismatch(
                        &cfm.name,
                        t.meta.id,
                        "range fragments differ".into(),
                    ));
                }
            }
            let want = built.totals.get(&cfm.name).copied().unwrap_or_default();
            // The footer's max_seq may count a fragment's sequence the source
            // side saw through the fragment itself: compare the maxima over
            // both, and the counts exactly.
            if seen.entries != want.entries
                || seen.fragments != want.fragments
                || seen.max_seq.max(want.max_seq) != want.max_seq
            {
                return Err(OndaError::Corruption(format!(
                    "format upgrade verification: family {:?}: rebuilt {} entries / {} \
                     fragments / max seq {}, source {} / {} / {}",
                    cfm.name,
                    seen.entries,
                    seen.fragments,
                    seen.max_seq,
                    want.entries,
                    want.fragments,
                    want.max_seq
                )));
            }
        }
        vdb.close()
    }

    /// Step 2's catalog checks.
    pub(super) fn preflight_catalog(
        opts: &Options,
        catalog: &Manifest,
        check_layout: bool,
    ) -> Result<()> {
        if check_layout && !catalog.cfs.is_empty() {
            let requested = if opts.unified_memtable {
                WalLayout::Unified
            } else {
                WalLayout::PerColumnFamily
            };
            let migrating = opts.migrate_to_unified
                && requested == WalLayout::Unified
                && catalog.wal_layout == WalLayout::PerColumnFamily;
            if requested != catalog.wal_layout && !migrating {
                return Err(OndaError::InvalidArgs(format!(
                    "WAL layout mismatch: database is {:?}, requested {requested:?} (checked \
                     before the format upgrade, which would otherwise leave an epoch-1 \
                     database this open cannot use)",
                    catalog.wal_layout
                )));
            }
        }
        for cf in &catalog.cfs {
            if let Some(t) = cf
                .sstables
                .iter()
                .find(|t| t.tier.is_some() || t.object.is_some())
            {
                return Err(OndaError::FormatUpgradeUnsupported(format!(
                    "column family {:?} has table {} on {} — the format upgrade rewrites \
                     default-tier tables only. Move every part back to the default tier \
                     with a 0.9.x binary (move_part_to_tier) and retry, or open with \
                     FormatUpgrade::ReadOnlyLegacy to read the database as it is",
                    cf.name,
                    t.id,
                    match (&t.tier, &t.object) {
                        (Some(tier), Some(_)) => format!("the shared tier {tier:?} (an object)"),
                        (Some(tier), None) => format!("the tier {tier:?}"),
                        _ => "an object store".to_string(),
                    }
                )));
            }
        }
        Ok(())
    }
}

/// Free bytes on the filesystem holding `dir`, or `None` where it cannot be
/// measured.
#[cfg(all(unix, feature = "legacy-onda"))]
fn available_bytes(dir: &Path) -> Option<u64> {
    rustix::fs::statvfs(dir)
        .ok()
        .map(|s| s.f_bavail.saturating_mul(s.f_frsize))
}

#[cfg(all(not(unix), feature = "legacy-onda"))]
fn available_bytes(_dir: &Path) -> Option<u64> {
    None
}

/// The whole protocol, steps 1–6. `None` when, once the lock was held, the
/// directory turned out to be epoch 1 already (another process upgraded it).
#[cfg(feature = "legacy-onda")]
fn run_upgrade(
    opts: &Options,
    observer: &dyn UpgradeObserver,
    check_layout: bool,
) -> Result<Option<UpgradeReport>> {
    let t0 = Instant::now();
    let paths = Paths::new(&opts.path)?;
    // Step 1.
    let src_lock = crate::db::acquire_dir_lock(&opts.path, false)?;
    if paths.journal.exists() {
        return Err(OndaError::Locked(format!(
            "{}: another process is swapping a format upgrade into place",
            paths.db.display()
        )));
    }
    if !crate::manifest::is_onda09_dir(&paths.db)? {
        return Ok(None);
    }
    sweep_stale_upgrade_dirs(&paths)?;

    // Step 2.
    let catalog = crate::legacy_onda::recover_catalog(&paths.db)?;
    rebuild::preflight_catalog(opts, &catalog, check_layout)?;
    let src_opts = rebuild::source_options(opts, &catalog)?;
    let legacy =
        crate::legacy_onda::open_read_only_locked(src_opts.clone(), Some(src_lock.try_clone()?))?;
    let prepared = legacy.list_prepared();
    if !prepared.is_empty() {
        return Err(OndaError::FormatUpgradeUnsupported(format!(
            "{} unresolved prepared transaction(s); commit or abort them with a 0.9.x binary \
             (commit_prepared / abort_prepared) and retry — nothing is ever aborted \
             automatically",
            prepared.len()
        )));
    }
    let source_bytes = dir_size(&paths.db);
    let need = source_bytes.saturating_add((source_bytes / 10).max(16 << 20));
    if let Some(avail) = observer
        .available_bytes(&paths.parent)
        .or_else(|| available_bytes(&paths.parent))
    {
        if avail < need {
            return Err(OndaError::Io(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                format!(
                    "format upgrade of {} needs {need} free bytes in {} (the source's \
                     {source_bytes} plus a margin); {avail} are available",
                    paths.db.display(),
                    paths.parent.display()
                ),
            )));
        }
    }
    observer.on_phase(UpgradePhase::Preflighted)?;

    // Step 3.
    let nonce = format!("{:016x}", crate::db::mint_instance_nonce(utf8(&paths.db)?));
    let up_name = format!("{}{nonce}", paths.upgrade_prefix());
    let backup_name = format!("{}{nonce}", paths.backup_prefix());
    let up = paths.parent.join(&up_name);
    std::fs::create_dir(&up)?;
    let built = (|| -> Result<(rebuild::Built, std::fs::File, u64)> {
        std::fs::File::create(up.join(MARKER))?.sync_all()?;
        // Held until the swap is done: with the source's lock, it is what a
        // concurrent opener's crash recovery would have to take.
        let up_lock = crate::db::acquire_dir_lock(utf8(&up)?, false)?;
        sync_dir(&up)?;
        sync_dir(&paths.parent)?;
        observer.on_phase(UpgradePhase::UpgradeDirCreated)?;
        // Step 4.
        let keep = opts.format_upgrade_verify == FormatUpgradeVerify::Scan;
        let built = rebuild::build(&legacy, &catalog, &up, keep, observer)?;
        // Step 5.
        let mut vopts = src_opts.clone();
        vopts.path = utf8(&up)?.to_string();
        rebuild::verify(&built, &legacy, vopts, &up_lock, opts.format_upgrade_verify)?;
        observer.on_phase(UpgradePhase::Verified)?;
        let upgraded_bytes = dir_size(&up);
        Ok((built, up_lock, upgraded_bytes))
    })();
    // The source handle is done with either way; its readers close before any
    // rename.
    drop(legacy);
    let (built, up_lock, upgraded_bytes) = match built {
        Ok(b) => b,
        Err(e) => {
            // A failure before the journal: the source was never touched.
            let _ = remove_dir_durably(&up, &paths.parent);
            return Err(e);
        }
    };

    // Step 6.
    let journal = Journal {
        state: uj::STATE_SWAPPING,
        name: paths.name.clone(),
        upgrade: up_name,
        backup: backup_name.clone(),
    };
    if let Err(e) = write_journal(&paths, &journal) {
        if paths.journal.exists() {
            let _ = recover_locked(&paths, &journal);
        } else {
            let _ = std::fs::remove_file(paths.journal_tmp());
            let _ = remove_dir_durably(&up, &paths.parent);
        }
        return Err(e);
    }
    let backup = paths.parent.join(&backup_name);
    let swapped = (|| -> Result<()> {
        observer.on_phase(UpgradePhase::JournalWritten)?;
        rename_durably(&paths.db, &backup, &paths.parent)?;
        observer.on_phase(UpgradePhase::SourceRenamed)?;
        rename_durably(&up, &paths.db, &paths.parent)?;
        observer.on_phase(UpgradePhase::UpgradeRenamed)?;
        mark_done(&paths, &journal)?;
        observer.on_phase(UpgradePhase::JournalDone)?;
        Ok(())
    })();
    if let Err(e) = swapped {
        // Resolve now what the next open would: roll forward (the rebuild is
        // verified) or, failing that, leave the journal for the next open.
        if let Ok(Some(j)) = read_journal(&paths) {
            let _ = recover_locked(&paths, &j);
        }
        return Err(e);
    }
    drop(up_lock);
    drop(src_lock);
    Ok(Some(UpgradeReport {
        path: PathBuf::from(&opts.path),
        source_bytes,
        upgraded_bytes,
        duration: t0.elapsed(),
        backup_path: Some(backup),
        column_families: built.families(),
        tables: built.tables(),
        entries: built.entries(),
        max_seq: built.max_seq(),
        verify: opts.format_upgrade_verify,
        resumed: false,
    }))
}

/// Remove upgrade directories an earlier upgrader left behind when it crashed
/// before writing its journal. Only directories carrying the marker go: the
/// marker is written first, so anything without it is not the protocol's.
/// The caller holds the source lock and has checked that no journal exists.
#[cfg(feature = "legacy-onda")]
fn sweep_stale_upgrade_dirs(paths: &Paths) -> Result<()> {
    let _ = std::fs::remove_file(paths.journal_tmp());
    let prefix = paths.upgrade_prefix();
    let mut removed = false;
    for e in std::fs::read_dir(&paths.parent)?.flatten() {
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(&prefix) {
            continue;
        }
        let dir = e.path();
        if dir.is_dir() && dir.join(MARKER).is_file() {
            std::fs::remove_dir_all(&dir)?;
            removed = true;
        }
    }
    if removed {
        sync_dir(&paths.parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths_in(dir: &Path) -> Paths {
        Paths::new(dir.join("db").to_str().unwrap()).unwrap()
    }

    #[test]
    fn journal_round_trips_and_refuses_foreign_names() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let j = Journal {
            state: uj::STATE_SWAPPING,
            name: "db".into(),
            upgrade: ".db.yolo-upgrade-00ff".into(),
            backup: ".db.pre-yolo-00ff".into(),
        };
        let bytes = j.encode();
        assert_eq!(&bytes[..8], b"YOLODBUJ");
        assert_eq!(Journal::decode(&bytes, &paths).unwrap(), j);
        // Every flipped bit is caught.
        for i in 0..bytes.len() {
            let mut b = bytes.clone();
            b[i] ^= 0x01;
            assert!(Journal::decode(&b, &paths).is_err(), "bit flip at {i}");
        }
        // A well-formed journal naming a directory outside the protocol's
        // sibling names is refused, not followed.
        for (upgrade, backup) in [
            ("../elsewhere", ".db.pre-yolo-00ff"),
            (".db.yolo-upgrade-00ff", "/etc"),
            (".db.yolo-upgrade-", ".db.pre-yolo-00ff"),
            (".other.yolo-upgrade-1", ".db.pre-yolo-00ff"),
        ] {
            let bad = Journal {
                upgrade: upgrade.into(),
                backup: backup.into(),
                ..j.clone()
            };
            assert_eq!(
                Journal::decode(&bad.encode(), &paths).unwrap_err().kind(),
                "corruption",
                "{upgrade} / {backup}"
            );
        }
        let other = Journal {
            name: "other".into(),
            ..j
        };
        assert!(Journal::decode(&other.encode(), &paths).is_err());
    }

    #[test]
    fn paths_derive_sibling_names() {
        let tmp = tempfile::tempdir().unwrap();
        let p = Paths::new(tmp.path().join("store").to_str().unwrap()).unwrap();
        assert_eq!(p.name, "store");
        assert_eq!(
            p.journal.file_name().unwrap(),
            ".store.yolo-upgrade.journal"
        );
        assert!(!p
            .journal
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(&p.upgrade_prefix()));
        let dotted = Paths::new(&format!("{}/store/.", tmp.path().display())).unwrap();
        assert_eq!(dotted.name, "store");
    }

    // ---- preflight refusals that need a 0.9 byte the fixtures lack -------------
    //
    // These build their inputs with the `#[cfg(test)]` 0.9 encoders, which is
    // why they live in-crate rather than in `tests/format_upgrade.rs`.

    #[cfg(feature = "legacy-onda")]
    fn copy_fixture(name: &str, dst: &Path) {
        fn copy(src: &Path, dst: &Path) {
            std::fs::create_dir_all(dst).unwrap();
            for e in std::fs::read_dir(src).unwrap() {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    copy(&e.path(), &dst.join(e.file_name()));
                } else {
                    std::fs::copy(e.path(), dst.join(e.file_name())).unwrap();
                }
            }
        }
        copy(&crate::util::legacy_fixture_path(name), dst);
    }

    /// Every file under `dir` but `LOCK`, with its bytes.
    #[cfg(feature = "legacy-onda")]
    fn tree(dir: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut out = std::collections::BTreeMap::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap().flatten() {
                if e.file_type().unwrap().is_dir() {
                    stack.push(e.path());
                } else if e.file_name() != "LOCK" {
                    out.insert(e.path(), std::fs::read(e.path()).unwrap());
                }
            }
        }
        out
    }

    #[cfg(feature = "legacy-onda")]
    fn rewrite_legacy_manifest(dir: &Path, f: impl FnOnce(&mut crate::manifest::Manifest)) {
        let path = crate::manifest::manifest_path(dir);
        let mut m = crate::legacy_onda::manifest::decode(&std::fs::read(&path).unwrap()).unwrap();
        f(&mut m);
        std::fs::write(&path, crate::legacy_onda::manifest::encode(&m)).unwrap();
    }

    #[cfg(feature = "legacy-onda")]
    fn assert_refused_untouched(dir: &Path, opts: Options, want_kind: &str) {
        let before = tree(dir);
        let err = DB::open(opts)
            .map(|_| ())
            .expect_err("the upgrade must refuse");
        assert_eq!(err.kind(), want_kind, "{err}");
        assert_eq!(tree(dir), before, "the source changed");
        assert!(crate::manifest::is_onda09_dir(dir).unwrap());
        let parent = dir.parent().unwrap();
        let siblings: Vec<_> = std::fs::read_dir(parent).unwrap().flatten().collect();
        assert_eq!(
            siblings.len(),
            1,
            "something was created beside the database"
        );
    }

    /// A table on a named tier (or an object store) is step-2 work: refused
    /// with the distinct error, before anything is written.
    #[cfg(feature = "legacy-onda")]
    #[test]
    fn tiered_tables_refuse_the_upgrade() {
        for object in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("db");
            copy_fixture("db-percf", &dir);
            rewrite_legacy_manifest(&dir, |m| {
                let name = m.cfs[0].name.clone();
                let t = &mut m.cfs[0].sstables[0];
                t.tier = Some("hdd".into());
                if object {
                    t.object = Some(format!("cf-{name}/00000000000000aa-{}", t.id));
                }
            });
            let opts = Options::new(dir.to_str().unwrap());
            let err_kind = "format_upgrade_unsupported";
            assert_refused_untouched(&dir, opts.clone(), err_kind);
            let msg = DB::open(opts).err().unwrap().to_string();
            assert!(
                msg.contains("hdd") && msg.contains("ReadOnlyLegacy"),
                "{msg}"
            );
        }
    }

    /// An unresolved prepared transaction is the operator's call: refused,
    /// never aborted, and the source stays as it was.
    #[cfg(feature = "legacy-onda")]
    #[test]
    fn unresolved_prepares_refuse_the_upgrade() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("db");
        copy_fixture("db-unified", &dir);
        rewrite_legacy_manifest(&dir, |m| {
            m.caps |= crate::format::CAP_TXN_DECISIONS | crate::format::CAP_EXTENDED_RECORDS;
        });
        // A prepare frame: the payload 0.9 wrote is the epoch-1 payload (only
        // the container differs), so build it with the epoch-1 WAL and
        // re-frame it under 0.9's IEEE CRC onto the fixture's unified WAL.
        let scratch_dir = tempfile::tempdir().unwrap();
        let scratch = scratch_dir.path().join("scratch.log");
        {
            let w = crate::wal::Wal::open(
                &scratch,
                crate::config::SyncMode::None,
                Duration::ZERO,
                crate::wal::SegmentId::unified(1),
            )
            .unwrap();
            let mut key = crate::legacy_onda::cf_id_09("ua").to_be_bytes().to_vec();
            key.extend_from_slice(b"prepared-key");
            w.append_prepare(
                crate::wal::ENVELOPE_SCHEMA_UNIFIED,
                &[7u8; 16],
                &[crate::legacy_onda::cf_id_09("ua")],
                &[crate::wal::RecordRef {
                    key: &key,
                    value: b"prepared-value",
                    seq: 0,
                    ttl: 0,
                    kind: crate::format::KIND_PUT,
                }],
            )
            .unwrap();
            w.sync().unwrap();
            w.close().unwrap();
        }
        // The WAL stripes by writer thread, so the frame may be in any of the
        // four stripe files.
        let mut legacy_frames = Vec::new();
        for stripe in ["", ".s1", ".s2", ".s3"] {
            let Ok(bytes) = std::fs::read(format!("{}{stripe}", scratch.display())) else {
                continue;
            };
            let mut frames = bytes
                .get(crate::format::wal_segment::HEADER_LEN..)
                .unwrap_or_default();
            while !frames.is_empty() {
                let len = read_u32(frames) as usize;
                let payload = &frames[8..8 + len];
                append_u32(&mut legacy_frames, len as u32);
                append_u32(
                    &mut legacy_frames,
                    crate::legacy_onda::checksum_ieee(payload),
                );
                legacy_frames.extend_from_slice(payload);
                frames = &frames[8 + len..];
            }
        }
        assert!(!legacy_frames.is_empty(), "the prepare frame was written");
        let wal = dir.join("unified-wal-1.log");
        let mut data = std::fs::read(&wal).unwrap();
        data.extend_from_slice(&legacy_frames);
        std::fs::write(&wal, data).unwrap();

        let mut opts = Options::new(dir.to_str().unwrap());
        opts.unified_memtable = true;
        // The legacy handle sees the prepare...
        {
            let db = crate::legacy_onda::open_read_only(opts.clone()).unwrap();
            assert_eq!(db.list_prepared().len(), 1);
            drop(db);
        }
        // ...and the upgrade refuses it.
        assert_refused_untouched(&dir, opts.clone(), "format_upgrade_unsupported");
        let msg = DB::open(opts).err().unwrap().to_string();
        assert!(msg.contains("prepared"), "{msg}");
    }
}
