//! Two-pass recovery of prepared transactions, the 3.2 crash matrix, and the
//! WAL generation pins.
//!
//! Every crash here is a **drop without close**: the process image is what a
//! crash destroys, and dropping the handle without `close` leaves exactly the
//! on-disk state a crash would — the WAL as it stands, the memtable gone. (A
//! `Drop` with unresolved prepares deliberately does not delete the pinned
//! generations; that is the `drop_with_pins_releases_dir_lock_and_keeps_wal`
//! contract, and it is what makes these tests possible.)

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamily, ColumnFamilyConfig, Options, SyncMode, DB};

fn unified_opts(path: &str) -> Options {
    Options {
        unified_memtable: true,
        // Every prepared frame is fsynced explicitly whatever the mode; the
        // default (`SyncMode::None`, four stripes) is the configuration whose
        // stripe-order-freedom recovery must survive, so it is the one tested.
        ..Options::new(path)
    }
}

fn open_unified(path: &str) -> DB {
    let db = DB::open(unified_opts(path)).unwrap();
    db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
        .unwrap();
    db
}

fn cf(db: &DB, name: &str) -> Arc<ColumnFamily> {
    db.create_column_family(name, ColumnFamilyConfig::default())
        .unwrap()
}

fn get_cf(db: &DB, name: &str) -> Arc<ColumnFamily> {
    db.get_column_family(name).unwrap()
}

fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

fn wait_for_flush(db: &DB) {
    for _ in 0..2000 {
        if db.pending_flushes_for_tests() == 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("the flush queue did not drain");
}

/// Base names (no stripe suffix) of the unified WAL generations on disk.
fn unified_wal_generations(path: &str) -> Vec<u64> {
    let mut out: Vec<u64> = std::fs::read_dir(path)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    name.strip_prefix("unified-wal-")?
                        .strip_suffix(".log")?
                        .parse()
                        .ok()
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_unstable();
    out
}

// ---- crash matrix ---------------------------------------------------------

/// Crash after the prepare fsync, before `prepare` returned to its caller:
/// the reservation is restored, the records are invisible, and no sequence was
/// reserved — so the watermark is exactly where the last commit left it.
#[test]
fn prep_crash_after_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let before;
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();
        before = db.visible_seq_for_tests();
        let mut t = db.begin();
        t.put(&a, b"prepared", b"pv", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        drop(db); // crash
    }
    let db = open_unified(&path);
    let a = get_cf(&db, "a");
    assert_eq!(db.list_prepared().len(), 1, "the reservation is restored");
    assert_eq!(db.list_prepared()[0].id, id(1));
    assert!(db.get(&a, b"prepared").is_err(), "records stay invisible");
    assert_eq!(db.get(&a, b"seed").unwrap(), b"v");
    assert_eq!(
        db.visible_seq_for_tests(),
        before,
        "a prepare reserved no sequence, so recovery restores none"
    );
    // ...and the reservation still refuses another writer.
    assert_eq!(
        db.put(&a, b"prepared", b"other", Duration::ZERO)
            .unwrap_err()
            .kind(),
        "conflict"
    );
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

/// A prepare frame never raises the replay watermark: its records carry the
/// `seq == 0` sentinel. Assert it directly — the next sequence after a reopen
/// is what it was before the prepare.
#[test]
fn prepare_frame_does_not_raise_max_seq() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let before;
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();
        before = db.visible_seq_for_tests();
        let mut t = db.begin();
        for i in 0..10u32 {
            t.put(&a, format!("p{i}").as_bytes(), b"v", Duration::ZERO)
                .unwrap();
        }
        t.prepare(&id(1)).unwrap();
        drop(db);
    }
    let db = open_unified(&path);
    assert_eq!(db.visible_seq_for_tests(), before);
    let a = get_cf(&db, "a");
    // The very next ordinary commit takes the next sequence, not one ten
    // higher: nothing was reserved on the prepare's behalf.
    db.put(&a, b"after", b"v", Duration::ZERO).unwrap();
    assert_eq!(db.visible_seq_for_tests(), before + 1);
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

/// A torn decision frame is crash residue, not corruption: the frame CRC fails,
/// the stripe ends cleanly, and the transaction is simply still prepared — so
/// the coordinator retries `commit_prepared`.
///
/// The tear is written as a partial frame rather than by truncating a real
/// decision, because the two are indistinguishable to replay — a frame whose
/// declared length outruns the file ends the stripe either way — and building
/// it here keeps the test from depending on the exact byte length of a decision
/// frame.
#[test]
fn prep_torn_decision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let prepare_gen;
    {
        // One stripe (`SyncMode::Full`), so the tear lands in a file whose
        // identity is known rather than in whichever of four a thread picked.
        let db = DB::open(Options {
            unified_memtable_sync_mode: SyncMode::Full,
            ..unified_opts(&path)
        })
        .unwrap();
        db.enable_format_capabilities(ondadb::format::CAP_TXN_DECISIONS)
            .unwrap();
        let a = cf(&db, "a");
        let mut t = db.begin();
        t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        prepare_gen = *unified_wal_generations(&path).last().unwrap();
        drop(db); // crash before the decision could be written whole
    }
    // A frame header promising 64 payload bytes, followed by 3 — exactly what a
    // crash mid-write leaves behind.
    {
        use std::io::Write;
        let file = format!("{path}/unified-wal-{prepare_gen}.log");
        let mut f = std::fs::OpenOptions::new().append(true).open(&file).unwrap();
        f.write_all(&[64, 0, 0, 0, 0xAA, 0xBB, 0xCC, 0xDD, 1, 2, 3])
            .unwrap();
    }

    let db = DB::open(Options {
        unified_memtable_sync_mode: SyncMode::Full,
        ..unified_opts(&path)
    })
    .unwrap();
    let a = get_cf(&db, "a");
    assert_eq!(
        db.list_prepared().len(),
        1,
        "a torn decision leaves the transaction prepared"
    );
    assert!(db.get(&a, b"k").is_err(), "nothing was applied");
    // The coordinator retries and it commits.
    db.commit_prepared(&id(1)).unwrap();
    assert_eq!(db.get(&a, b"k").unwrap(), b"v");
    db.close().unwrap();
}

/// Crash after the decision fsync, before the apply: pass 2 raises the
/// watermark to `commit_seq + count - 1` **first**, then applies the prepare's
/// records. Without the watermark call the next ordinary commit would reuse
/// those sequences (AGENTS.md invariant 5).
#[test]
fn prep_crash_before_apply() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();
        let mut t = db.begin();
        t.put(&a, b"k1", b"v1", Duration::ZERO).unwrap();
        t.put(&a, b"k2", b"v2", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        // Write and fsync the decision, then crash before the memtable apply.
        db.commit_prepared_stop_after_decision_for_tests(&id(1))
            .unwrap();
        drop(db);
    }
    let db = open_unified(&path);
    let a = get_cf(&db, "a");
    assert!(db.list_prepared().is_empty(), "the decision resolved it");
    assert_eq!(db.get(&a, b"k1").unwrap(), b"v1");
    assert_eq!(db.get(&a, b"k2").unwrap(), b"v2");

    // The recovered block's sequences are consumed, not reused.
    let after_recovery = db.visible_seq_for_tests();
    db.put(&a, b"next", b"v", Duration::ZERO).unwrap();
    assert_eq!(db.visible_seq_for_tests(), after_recovery + 1);
    // ...and the value written at the recovered block is not shadowed by it.
    assert_eq!(db.get(&a, b"k1").unwrap(), b"v1");
    db.close().unwrap();
}

/// Crash mid apply. The memtable is volatile, so whatever half-applied state
/// existed dies with the process; pass 2 re-applies the whole writeset from the
/// prepare frame at `commit_seq + slot`. Idempotency is *observable*, not
/// structural — see `pinned_generation_replay_is_idempotent_in_both_backends`.
#[test]
fn prep_crash_mid_apply() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        let mut t = db.begin();
        for i in 0..8u32 {
            t.put(&a, format!("k{i}").as_bytes(), b"v", Duration::ZERO)
                .unwrap();
        }
        t.prepare(&id(1)).unwrap();
        db.commit_prepared_stop_after_decision_for_tests(&id(1))
            .unwrap();
        // Half the writeset, as a torn apply would have left it — and then the
        // process dies, taking the memtable with it.
        drop(db);
    }
    let db = open_unified(&path);
    let a = get_cf(&db, "a");
    for i in 0..8u32 {
        assert_eq!(
            db.get(&a, format!("k{i}").as_bytes()).unwrap(),
            b"v",
            "record {i} must be re-applied whole"
        );
    }
    db.close().unwrap();
}

/// A prepare recovered from a crash can still be aborted, and the abort leaves
/// no sequence gap: no sequence was ever reserved for it.
#[test]
fn prep_abort_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        let mut t = db.begin();
        t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        drop(db);
    }
    let db = open_unified(&path);
    let a = get_cf(&db, "a");
    let before = db.visible_seq_for_tests();
    db.abort_prepared(&id(1)).unwrap();
    assert_eq!(db.visible_seq_for_tests(), before, "no gap to close");
    assert!(db.list_prepared().is_empty());
    db.put(&a, b"k", b"other", Duration::ZERO).unwrap();
    assert_eq!(db.get(&a, b"k").unwrap(), b"other");
    db.close().unwrap();

    // ...and the abort is durable: the reopen does not resurrect it.
    let db = open_unified(&path);
    assert!(db.list_prepared().is_empty());
    db.close().unwrap();
}

/// A decision naming an id no surviving prepare frame carries is a no-op. The
/// pair was retired in that order — prepare first — so its records are already
/// durable in L0.
#[test]
fn prep_decision_without_prepare_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        let mut t = db.begin();
        t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        db.commit_prepared(&id(1)).unwrap();
        // Flush, so the applied records are durable in L0 and the prepare's
        // generation is retirable; then write a decision for an id whose
        // prepare frame is gone.
        db.rotate_unified_for_tests();
        wait_for_flush(&db);
        db.append_abort_decision_for_tests(&id(7)).unwrap();
        drop(db);
    }
    let db = open_unified(&path);
    let a = get_cf(&db, "a");
    assert!(
        db.list_prepared().is_empty(),
        "an orphan decision registers nothing"
    );
    assert_eq!(db.get(&a, b"k").unwrap(), b"v");
    db.close().unwrap();
}

/// Repeated opens with an unresolved prepare produce an identical registry and
/// identical reads. Three opens, per the exit criteria.
#[test]
fn prep_recovery_idempotent_across_opens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        let b = cf(&db, "b");
        db.put(&a, b"committed", b"cv", Duration::ZERO).unwrap();
        let mut t = db.begin();
        t.put(&a, b"p1", b"v1", Duration::ZERO).unwrap();
        t.put(&b, b"p2", b"v2", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        // A second, committed prepare, so recovery has both arms to resolve
        // every time.
        let mut t2 = db.begin();
        t2.put(&a, b"done", b"dv", Duration::ZERO).unwrap();
        t2.prepare(&id(2)).unwrap();
        db.commit_prepared(&id(2)).unwrap();
        drop(db);
    }
    let mut snapshots = Vec::new();
    for open in 0..3 {
        let db = open_unified(&path);
        let a = get_cf(&db, "a");
        let list = db.list_prepared();
        snapshots.push((
            list.iter().map(|i| (i.id, i.bytes)).collect::<Vec<_>>(),
            db.get(&a, b"committed").unwrap(),
            db.get(&a, b"done").unwrap(),
            db.get(&a, b"p1").is_err(),
            db.visible_seq_for_tests(),
        ));
        assert_eq!(list.len(), 1, "open {open}");
        assert_eq!(list[0].id, id(1), "open {open}");
        drop(db);
    }
    assert_eq!(snapshots[0], snapshots[1]);
    assert_eq!(snapshots[1], snapshots[2]);

    let db = open_unified(&path);
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

/// A pinned generation is fully re-replayed on the next open, and re-inserting
/// records already durable in L0 must be **observably** idempotent — the
/// SkipMap backend replaces at `(key, seq)` while the arena backend links a new
/// node that shadows identically. Reads must agree in both, which is why this
/// runs in both feature configurations.
#[test]
fn pinned_generation_replay_is_idempotent_in_both_backends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        // A committed batch that will be replayed again and again, because the
        // prepare below pins its generation.
        for i in 0..64u32 {
            db.put(&a, format!("k{i:03}").as_bytes(), b"v0", Duration::ZERO)
                .unwrap();
        }
        let mut t = db.begin();
        t.put(&a, b"pinned", b"pv", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        drop(db);
    }
    let mut reads = Vec::new();
    for _ in 0..3 {
        let db = open_unified(&path);
        let a = get_cf(&db, "a");
        let mut round: Vec<Vec<u8>> = Vec::new();
        for i in 0..64u32 {
            round.push(db.get(&a, format!("k{i:03}").as_bytes()).unwrap());
        }
        // A full scan too: a duplicate node that shadowed differently would
        // change the iteration, not just a point read.
        let txn = db.begin();
        let mut it = txn.new_iterator(&a);
        let mut scanned = 0usize;
        it.seek_to_first();
        while it.valid() {
            scanned += 1;
            it.next();
        }
        reads.push((round, scanned));
        drop(db);
    }
    assert_eq!(reads[0], reads[1], "a repeated replay changed what is read");
    assert_eq!(reads[1], reads[2]);
    assert_eq!(reads[0].1, 64, "the prepared record is not in the scan");

    let db = open_unified(&path);
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

// ---- WAL generation pins --------------------------------------------------

/// A generation holding an unresolved prepare is withheld: the flush persists
/// the catalog, and the files stay.
#[test]
fn prep_wal_pin_withholds_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = open_unified(&path);
    let a = cf(&db, "a");
    db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.put(&a, b"prepared", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();
    let pinned = *unified_wal_generations(&path).last().unwrap();

    // Force a rotation and let the flush complete.
    db.put(&a, b"more", b"v", Duration::ZERO).unwrap();
    db.rotate_unified_for_tests();
    wait_for_flush(&db);

    assert!(
        db.wal_generation_is_withheld(pinned),
        "generation {pinned} holds an unresolved prepare"
    );
    assert!(
        unified_wal_generations(&path).contains(&pinned),
        "the pinned generation's files must survive the flush"
    );
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}

/// The acceptance criterion: **zero** `unified-wal-*.log` files remain after
/// resolve + flush + close, and the withheld map is empty.
#[test]
fn pin_releases_after_decision_and_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = open_unified(&path);
        let a = cf(&db, "a");
        db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();
        let mut t = db.begin();
        t.put(&a, b"prepared", b"v", Duration::ZERO).unwrap();
        t.prepare(&id(1)).unwrap();
        // Rotate so the decision lands in a LATER generation than the prepare —
        // the case pinning only the prepare would get wrong.
        db.rotate_unified_for_tests();
        wait_for_flush(&db);
        db.commit_prepared(&id(1)).unwrap();
        db.rotate_unified_for_tests();
        wait_for_flush(&db);

        assert_eq!(
            db.withheld_wal_generations(),
            0,
            "every pin cleared once the pair was resolved and flushed"
        );
        db.close().unwrap();
    }
    // Close writes a final manifest and leaves the *current* generation's
    // (empty) file behind, exactly as it does without the feature. What must be
    // gone is every generation the pins were withholding.
    let left = unified_wal_generations(&path);
    assert!(
        left.len() <= 1,
        "resolve + flush + close must leave no withheld generation, found {left:?}"
    );

    let db = open_unified(&path);
    let a = get_cf(&db, "a");
    assert_eq!(db.get(&a, b"prepared").unwrap(), b"v");
    assert!(db.list_prepared().is_empty());
    db.close().unwrap();
}

/// A prepare and its decision in the SAME generation retire together with the
/// file — the (c)-exclusion edge case. Without it the predicate is circular and
/// the generation is withheld forever.
#[test]
fn prepare_and_decision_in_same_generation_retire_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = open_unified(&path);
    let a = cf(&db, "a");
    db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();
    let gen = *unified_wal_generations(&path).last().unwrap();

    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();
    // No rotation in between: both frames are in `gen`.
    db.commit_prepared(&id(1)).unwrap();

    db.rotate_unified_for_tests();
    wait_for_flush(&db);
    assert_eq!(db.withheld_wal_generations(), 0);
    assert!(
        !unified_wal_generations(&path).contains(&gen),
        "a same-generation pair retires atomically with its file"
    );
    db.close().unwrap();
}

/// The ordering rule: **the prepare is unlinked before its decision, never the
/// other way round.** A crash after unlinking the prepare leaves a decision for
/// an unknown id, which recovery treats as a no-op — safe. The reverse would
/// leave a phantom prepare for a transaction that already committed, and a
/// coordinator retry would then apply the writeset twice.
#[test]
fn decision_generation_not_unlinked_before_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = open_unified(&path);
    let a = cf(&db, "a");
    db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();
    let prepare_gen = *unified_wal_generations(&path).last().unwrap();

    // Rotate: the decision will land in a strictly later generation.
    db.put(&a, b"more", b"v", Duration::ZERO).unwrap();
    db.rotate_unified_for_tests();
    wait_for_flush(&db);
    let decision_gen = *unified_wal_generations(&path).last().unwrap();
    assert!(decision_gen > prepare_gen);
    assert!(
        db.wal_generation_is_withheld(prepare_gen),
        "the prepare's generation is withheld while unresolved"
    );

    db.commit_prepared(&id(1)).unwrap();
    // The decision's own generation has not flushed yet, so the prepare's is
    // STILL withheld: retiring it now would be the phantom-prepare bug.
    assert!(
        db.wal_generation_is_withheld(prepare_gen),
        "the prepare's generation waits for the decision's to flush"
    );
    assert!(unified_wal_generations(&path).contains(&prepare_gen));

    db.rotate_unified_for_tests();
    wait_for_flush(&db);
    let left = unified_wal_generations(&path);
    assert!(
        !left.contains(&prepare_gen) && !left.contains(&decision_gen),
        "both retire once the decision's generation has flushed, found {left:?}"
    );
    assert_eq!(db.withheld_wal_generations(), 0);
    db.close().unwrap();
}

/// An id may not be reused while its previous instance's frames are still on
/// disk: two prepare frames with one id is a state recovery cannot
/// disambiguate, so the reuse is refused up front instead.
#[test]
fn id_is_not_reusable_until_its_generations_retire() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = open_unified(&path);
    let a = cf(&db, "a");
    db.put(&a, b"seed", b"v", Duration::ZERO).unwrap();

    let mut t = db.begin();
    t.put(&a, b"k", b"v", Duration::ZERO).unwrap();
    t.prepare(&id(1)).unwrap();
    db.rotate_unified_for_tests();
    wait_for_flush(&db);
    db.commit_prepared(&id(1)).unwrap();

    // Resolved, but the pair's generations are not both unlinked yet.
    let mut again = db.begin();
    again.put(&a, b"k2", b"v", Duration::ZERO).unwrap();
    assert_eq!(again.prepare(&id(1)).unwrap_err().kind(), "exists");

    db.rotate_unified_for_tests();
    wait_for_flush(&db);
    let mut ok = db.begin();
    ok.put(&a, b"k2", b"v", Duration::ZERO).unwrap();
    ok.prepare(&id(1))
        .expect("the id is reusable once both generations are gone");
    db.abort_prepared(&id(1)).unwrap();
    db.close().unwrap();
}
