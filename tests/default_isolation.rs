//! `Options::default_isolation` drives `DB::begin` (wavesdb `d789912`).

use std::time::Duration;

use ondadb::{ColumnFamilyConfig, IsolationLevel, OndaError, Options, DB};

fn open(dir: &std::path::Path, level: Option<IsolationLevel>) -> DB {
    let mut opts = Options::new(dir.to_str().unwrap());
    if let Some(level) = level {
        opts.default_isolation = level;
    }
    DB::open(opts).unwrap()
}

#[test]
fn unset_default_is_snapshot() {
    assert_eq!(Options::default().default_isolation, IsolationLevel::Snapshot);
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), None);
    assert_eq!(db.begin().isolation(), IsolationLevel::Snapshot);
    assert_eq!(db.begin_pessimistic().isolation(), IsolationLevel::Snapshot);
    db.close().unwrap();
}

#[test]
fn begin_honors_the_configured_default() {
    for level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Snapshot,
        IsolationLevel::Serializable,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), Some(level));
        assert_eq!(db.begin().isolation(), level);
        assert_eq!(db.begin_pessimistic().isolation(), level);
        // An explicit level still wins over the default.
        assert_eq!(
            db.begin_with_isolation(IsolationLevel::Serializable).isolation(),
            IsolationLevel::Serializable
        );
        db.close().unwrap();
    }
}

/// Not just the label: the transaction behaves at the configured level. A
/// `ReadCommitted` default floats (sees a commit that landed after `begin`);
/// the `Snapshot` default does not.
#[test]
fn configured_default_changes_visibility() {
    for (level, sees_later_commit) in [
        (IsolationLevel::ReadCommitted, true),
        (IsolationLevel::Snapshot, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), Some(level));
        let cf = db
            .create_column_family("c", ColumnFamilyConfig::default())
            .unwrap();
        db.put(&cf, b"k", b"old", Duration::ZERO).unwrap();
        let mut txn = db.begin();
        db.put(&cf, b"k", b"new", Duration::ZERO).unwrap();
        let want: &[u8] = if sees_later_commit { b"new" } else { b"old" };
        assert_eq!(txn.get(&cf, b"k").unwrap(), want, "{level:?}");
        txn.rollback().unwrap();
        db.close().unwrap();
    }
}

/// The default is host policy, not data: it is not persisted, so a reopen
/// without it is back to `Snapshot`.
#[test]
fn default_is_not_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), Some(IsolationLevel::Serializable));
    db.create_column_family("c", ColumnFamilyConfig::default())
        .unwrap();
    db.close().unwrap();
    drop(db);
    let db = open(dir.path(), None);
    assert_eq!(db.begin().isolation(), IsolationLevel::Snapshot);
    // The database still works as before.
    let cf = db.get_column_family("c").unwrap();
    assert!(matches!(db.get(&cf, b"absent"), Err(OndaError::NotFound)));
    db.close().unwrap();
}
