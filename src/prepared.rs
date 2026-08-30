//! Durable prepared transactions (3.2): the in-memory half of two-phase
//! commit.
//!
//! ondaDB is a **participant**, never a coordinator: it holds the durable
//! prepare and resolves it by a stable external id, and every decision about
//! *whether* to commit belongs to the layer above. Nothing here ever aborts a
//! prepare on its own — not on a timeout, not at close, not at open.
//!
//! Two structures do the work. The [`PreparedRegistry`] holds every unresolved
//! prepare, keyed both by id (for resolve) and by `(cf_id, user key)` (for the
//! reservation check every commit performs under `commit_mu`). [`WalGenState`]
//! holds the WAL generation pins: a generation may not be unlinked while a
//! prepare or a not-yet-durable decision still lives in it.
//!
//! The two are separate locks on purpose — the registry is taken inside
//! `commit_mu` on the write path, while the generation state is taken by flush
//! workers that hold no commit lock at all. They never nest: no code path holds
//! one while acquiring the other. See `docs/concurrency-and-safety.md`.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::error::{OndaError, Result};

/// One buffered write of a prepared transaction, as `(offset, len)` ranges into
/// the entry's arena.
///
/// The column family is named by its **durable** id (`ColumnFamily::id()`, the
/// FNV-1a of the name) and never by an `Arc` address: a dropped-and-recreated
/// family reuses the address but keeps the id, and the reservation must follow
/// the data, not the handle. Same key 3.3's lock manager will use.
#[derive(Debug, Clone)]
pub(crate) struct PreparedWrite {
    pub cf_id: u64,
    pub key: (usize, usize),
    pub value: (usize, usize),
    pub ttl: i64,
    /// Record kind; see [`crate::wal::Record::kind`].
    pub kind: u64,
}

/// One unresolved prepared transaction.
#[derive(Debug)]
pub(crate) struct PreparedEntry {
    pub id: [u8; 16],
    pub cf_ids: Vec<u64>,
    /// The transaction's arena, moved out of the `Txn` rather than copied.
    /// It never re-enters `txn::BUF_POOL`: pinning an up-to-32-MiB buffer in a
    /// thread-local for an unbounded prepare lifetime is exactly what the pool's
    /// caps exist to prevent.
    pub buf: Vec<u8>,
    pub writes: Vec<PreparedWrite>,
    /// `util::now_nanos` at prepare, for [`PreparedInfo::age`].
    pub prepared_at: i64,
    /// Charge against `Options::max_prepared_bytes`.
    pub bytes: usize,
    /// Unified WAL generation the prepare frame landed in.
    pub prepare_gen: u64,
}

impl PreparedEntry {
    /// This entry's writes as `(cf_id, key, value)` triples borrowed from the
    /// arena.
    pub fn records(&self) -> impl std::iter::Iterator<Item = (u64, &[u8], &[u8], &PreparedWrite)> {
        self.writes.iter().map(move |w| {
            (
                w.cf_id,
                &self.buf[w.key.0..w.key.0 + w.key.1],
                &self.buf[w.value.0..w.value.0 + w.value.1],
                w,
            )
        })
    }
}

/// Operator-visible description of one unresolved prepared transaction.
///
/// This is the whole of ondaDB's answer to an abandoned prepare: it reports,
/// and a human (or the coordinator) decides. Nothing is ever auto-aborted, so
/// `age` growing without bound is information, not a trigger.
#[derive(Debug, Clone)]
pub struct PreparedInfo {
    /// The coordinator-supplied id.
    pub id: [u8; 16],
    /// Time since the prepare was made durable. Measured from a wall clock, so
    /// it survives neither a clock step nor a reopen exactly; it is an operator
    /// signal, not a deadline.
    pub age: Duration,
    /// Bytes this prepare holds against
    /// [`Options::max_prepared_bytes`](crate::Options::max_prepared_bytes).
    pub bytes: usize,
    /// The column families the prepare touches, by
    /// [`ColumnFamily::id()`](crate::ColumnFamily::id).
    pub cf_ids: Vec<u64>,
}

/// Reservation registry: every unresolved prepare, by id and by reserved key.
#[derive(Debug, Default)]
pub(crate) struct PreparedRegistry {
    by_id: HashMap<[u8; 16], PreparedEntry>,
    /// `(cf.id(), user key) -> owning transaction id`. First preparer wins.
    by_key: HashMap<(u64, Vec<u8>), [u8; 16]>,
    /// Ids that are resolved but whose WAL generations are not both unlinked.
    ///
    /// An id may only be reused once its prepare *and* its decision are off
    /// disk: reusing it earlier would put two prepare frames with the same id
    /// in the replayed set, which recovery cannot disambiguate.
    retiring: std::collections::HashSet<[u8; 16]>,
    bytes: usize,
    max_bytes: usize,
}

impl PreparedRegistry {
    pub fn new(max_bytes: usize) -> PreparedRegistry {
        PreparedRegistry {
            max_bytes,
            ..Default::default()
        }
    }

    /// Number of unresolved prepares.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether any key is reserved. The ordinary commit path's fast exit.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// The transaction reserving `(cf_id, key)`, if any.
    pub fn owner_of(&self, cf_id: u64, key: &[u8]) -> Option<[u8; 16]> {
        // The probe allocates only when the registry is non-empty, and the
        // ordinary commit path checks `is_empty` before ever calling this.
        self.by_key.get(&(cf_id, key.to_vec())).copied()
    }

    /// The transaction reserving any key inside `[start, end)` of `cf_id`,
    /// under `cmp`, with the reserved key that matched.
    ///
    /// A **range delete** covering a reserved key is another writer changing
    /// it, so it has to be refused like a point write to that key — and a hash
    /// probe cannot answer a span. This is a linear scan of the registry, which
    /// is what the shape allows: the registry holds one entry per unresolved
    /// prepare (normally none), and range deletes are documented as rare and
    /// bulk. The ordinary point path never reaches here.
    pub fn owner_in_range(
        &self,
        cf_id: u64,
        cmp: &crate::comparator::ComparatorRef,
        start: &[u8],
        end: &[u8],
    ) -> Option<([u8; 16], Vec<u8>)> {
        self.by_key
            .iter()
            .find(|((cf, key), _)| {
                *cf == cf_id && cmp.compare(start, key).is_le() && cmp.compare(end, key).is_gt()
            })
            .map(|((_, key), id)| (*id, key.clone()))
    }

    /// Whether `id` names a live prepare, or one whose generations are still
    /// being retired.
    pub fn knows(&self, id: &[u8; 16]) -> bool {
        self.by_id.contains_key(id) || self.retiring.contains(id)
    }

    /// Whether `id` was resolved and its WAL generations are not yet unlinked.
    ///
    /// This is what makes a coordinator retry idempotent: a second
    /// `commit_prepared` for an id in this state returns `Ok` without applying
    /// anything, rather than `NotFound`.
    pub fn is_retiring(&self, id: &[u8; 16]) -> bool {
        self.retiring.contains(id)
    }

    pub fn get(&self, id: &[u8; 16]) -> Option<&PreparedEntry> {
        self.by_id.get(id)
    }

    /// Bytes currently charged against the cap.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Reject `entry` if admitting it would exceed the byte cap.
    ///
    /// `TooLarge` rather than `MemoryLimit`: the doc comment on the former —
    /// "value or request exceeds a hard size limit" — is what this is, and
    /// `MemoryLimit` stays dead rather than gaining one caller.
    pub fn check_capacity(&self, bytes: usize) -> Result<()> {
        if self.bytes.saturating_add(bytes) > self.max_bytes {
            return Err(OndaError::TooLarge(format!(
                "prepared transaction of {bytes} bytes would take the database past \
                 max_prepared_bytes ({}); {} bytes are held by {} unresolved prepare(s)",
                self.max_bytes,
                self.bytes,
                self.by_id.len()
            )));
        }
        Ok(())
    }

    /// Register a prepare, reserving every key it writes.
    ///
    /// The caller has already checked for key overlap and for a duplicate id
    /// under `commit_mu`; this is the state change alone, so the check and the
    /// registration cannot be split by another writer.
    pub fn register(&mut self, entry: PreparedEntry) {
        debug_assert!(!self.knows(&entry.id));
        for (cf_id, key, _, _) in entry.records() {
            self.by_key.insert((cf_id, key.to_vec()), entry.id);
        }
        self.bytes = self.bytes.saturating_add(entry.bytes);
        self.by_id.insert(entry.id, entry);
    }

    /// Force-register a prepare recovered from the WAL, past the byte cap.
    ///
    /// The cap governs *new* prepares. Refusing to open a database because of
    /// durable state already on disk is the wrong failure mode: the operator
    /// would have no way to see, let alone abort, what is blocking them.
    pub fn register_recovered(&mut self, entry: PreparedEntry) {
        self.register(entry);
    }

    /// Whether the recovered set alone is over the cap (open still succeeds;
    /// this is what the caller reports).
    pub fn over_capacity(&self) -> bool {
        self.bytes > self.max_bytes
    }

    /// Drop a prepare's registration and every key it reserved, returning the
    /// entry so the caller can apply or discard its records.
    ///
    /// The id stays *retiring* until both its WAL generations are unlinked, so
    /// a coordinator cannot reuse it while two prepare frames could coexist on
    /// disk.
    pub fn resolve(&mut self, id: &[u8; 16]) -> Option<PreparedEntry> {
        let entry = self.by_id.remove(id)?;
        for (cf_id, key, _, _) in entry.records() {
            // Only if this transaction still owns it: a key is never handed
            // over, but being explicit keeps the map consistent under any
            // future overlap rule.
            if self.by_key.get(&(cf_id, key.to_vec())) == Some(id) {
                self.by_key.remove(&(cf_id, key.to_vec()));
            }
        }
        self.bytes = self.bytes.saturating_sub(entry.bytes);
        self.retiring.insert(*id);
        Some(entry)
    }

    /// Release a retiring id for reuse — called once its generations are gone.
    pub fn forget(&mut self, id: &[u8; 16]) {
        self.retiring.remove(id);
    }

    /// Every unresolved prepare, for [`crate::DB::list_prepared`].
    pub fn list(&self, now: i64) -> Vec<PreparedInfo> {
        let mut out: Vec<PreparedInfo> = self
            .by_id
            .values()
            .map(|e| PreparedInfo {
                id: e.id,
                age: Duration::from_nanos(now.saturating_sub(e.prepared_at).max(0) as u64),
                bytes: e.bytes,
                cf_ids: e.cf_ids.clone(),
            })
            .collect();
        // Stable output: a `HashMap` iteration order would make the operator
        // view reshuffle between two calls that observed the same state.
        out.sort_unstable_by_key(|i| i.id);
        out
    }

    /// Whether any unresolved prepare names `cf_id`.
    pub fn touches_cf(&self, cf_id: u64) -> bool {
        self.by_id.values().any(|e| e.cf_ids.contains(&cf_id))
    }
}

/// A resolved prepare whose WAL generations are not both unlinked yet.
#[derive(Debug, Clone, Copy)]
struct Tombstone {
    id: [u8; 16],
    prepare_gen: u64,
    decision_gen: u64,
    prepare_deleted: bool,
    decision_deleted: bool,
}

/// WAL generation pins and the retirement sweep.
///
/// A unified WAL generation is normally unlinked as soon as the immutable it
/// backs has flushed and the catalog edit is durable (invariant 1). A prepare
/// breaks that: its frame lives in generation `P` and its decision in whatever
/// generation `D >= P` is current at resolve time, and unlinking `P` while `D`
/// is still unflushed would let a reopen see a prepare with no decision and
/// re-register a reservation for a transaction that already committed — a
/// coordinator retry would then apply the writeset a second time.
///
/// So the pin covers the **pair**, with one ordering rule: *the prepare is
/// unlinked before its decision, never the other way round.* A crash after
/// unlinking `P` leaves a decision for an unknown id, which recovery treats as
/// a no-op — safe, because `P` only went once `D` was flushed, i.e. once the
/// applied records were durable in L0.
#[derive(Debug, Default)]
pub(crate) struct WalGenState {
    /// Generations whose flush has persisted the catalog but whose files are
    /// withheld by a pin, by generation number.
    flushed: BTreeMap<u64, String>,
    /// Unresolved prepares per generation. A multiset: several prepares may
    /// share a generation, and each holds it independently.
    live: BTreeMap<u64, usize>,
    tombs: Vec<Tombstone>,
}

impl WalGenState {
    /// Generation number of a unified WAL path, by the same rule
    /// `UnifiedStore::open` scans with (`unified-wal-<gen>.log`).
    ///
    /// A stripe suffix (`.s1`) never appears here: `imm.wal_paths` holds base
    /// paths, and `wal::remove_wal_files` expands them.
    pub fn gen_of_path(path: &str) -> Option<u64> {
        let name = path.rsplit('/').next()?;
        name.strip_prefix("unified-wal-")?
            .strip_suffix(".log")?
            .parse()
            .ok()
    }

    /// Pin `gen` for an unresolved prepare.
    pub fn pin(&mut self, gen: u64) {
        *self.live.entry(gen).or_insert(0) += 1;
    }

    /// Release a prepare's pin and record the pair, so the sweep can enforce
    /// "a decision is never retired before its prepare".
    ///
    /// Every call must be matched by an earlier [`pin`](Self::pin) of
    /// `prepare_gen`: the counter is a multiset, and an unmatched release would
    /// silently drop *another* transaction's pin on the same generation.
    /// Recovery resolves prepares it never pinned, and uses
    /// [`record_pair`](Self::record_pair) for exactly that reason.
    pub fn resolved(&mut self, id: [u8; 16], prepare_gen: u64, decision_gen: u64) {
        match self.live.get_mut(&prepare_gen) {
            Some(n) => {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    self.live.remove(&prepare_gen);
                }
            }
            None => debug_assert!(false, "resolved generation {prepare_gen} was never pinned"),
        }
        self.record_pair(id, prepare_gen, decision_gen);
    }

    /// Record a resolved pair **without** releasing a pin.
    ///
    /// Recovery's path: a prepare whose decision was already on disk is never
    /// registered and never pinned, but its two generations still have to
    /// retire in the right order.
    pub fn record_pair(&mut self, id: [u8; 16], prepare_gen: u64, decision_gen: u64) {
        self.tombs.push(Tombstone {
            id,
            prepare_gen,
            decision_gen,
            prepare_deleted: false,
            decision_deleted: false,
        });
    }

    /// Mark a flushed generation's files as withheld, pending the sweep.
    pub fn mark_flushed(&mut self, gen: u64, path: String) {
        self.flushed.insert(gen, path);
    }

    /// Whether `gen`'s files are currently withheld by a pin.
    pub fn is_withheld(&self, gen: u64) -> bool {
        self.flushed.contains_key(&gen)
    }

    /// Number of generations whose files the pins are withholding.
    pub fn withheld(&self) -> usize {
        self.flushed.len()
    }

    /// Run the retirement fixpoint, returning the paths to unlink **in unlink
    /// order** and the ids whose pairs are now fully retired.
    ///
    /// The caller does the unlinking outside the lock; the order is the
    /// contract, so it is returned rather than left to the caller's iteration.
    pub fn sweep(&mut self) -> (Vec<String>, Vec<[u8; 16]>) {
        let mut unlink = Vec::new();
        let mut retired = Vec::new();
        while let Some(gen) = self.next_retirable() {
            // `next_retirable` only ever names a key of `flushed`, so the
            // removal cannot miss; `expect` states that rather than silently
            // ending the fixpoint early on a bug.
            let path = self
                .flushed
                .remove(&gen)
                .expect("next_retirable names a withheld generation");
            unlink.push(path);
            for t in &mut self.tombs {
                if t.prepare_gen == gen {
                    t.prepare_deleted = true;
                }
                if t.decision_gen == gen {
                    t.decision_deleted = true;
                }
            }
            for t in &self.tombs {
                if t.prepare_deleted && t.decision_deleted {
                    retired.push(t.id);
                }
            }
            self.tombs.retain(|t| !(t.prepare_deleted && t.decision_deleted));
        }
        (unlink, retired)
    }

    /// The lowest withheld generation every retirement condition holds for.
    fn next_retirable(&self) -> Option<u64> {
        self.flushed.keys().copied().find(|&g| {
            // (a) no unresolved prepare lives here.
            if self.live.get(&g).copied().unwrap_or(0) > 0 {
                return false;
            }
            // (b) every prepare that lived here has a decision that is itself
            // flushed or already gone — otherwise a crash would leave the
            // phantom prepare this whole mechanism exists to prevent.
            if self.tombs.iter().any(|t| {
                t.prepare_gen == g
                    && !t.decision_deleted
                    && !self.flushed.contains_key(&t.decision_gen)
            }) {
                return false;
            }
            // (c) a decision generation waits for its prepare. The
            // `prepare_gen != g` exclusion is what keeps the predicate from
            // being circular when the pair shares one generation: those two
            // retire atomically with the file.
            if self
                .tombs
                .iter()
                .any(|t| t.decision_gen == g && t.prepare_gen != g && !t.prepare_deleted)
            {
                return false;
            }
            true
        })
    }
}

/// One prepare recovered by pass 1, before any decision has been matched to it.
#[derive(Debug)]
pub(crate) struct RecoveredPrepare {
    pub id: [u8; 16],
    pub cf_ids: Vec<u64>,
    /// Records exactly as the frame carried them: keys still cf-id prefixed,
    /// every `seq` the `0` sentinel.
    pub records: Vec<crate::wal::Record>,
    pub gen: u64,
}

/// One decision recovered by pass 1.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecoveredDecision {
    pub id: [u8; 16],
    pub commit: Option<(u64, u64)>,
    pub gen: u64,
}

/// What pass 1 of recovery collected, handed across the open boundary.
///
/// Order-free by construction: the unified WAL is four-striped in every mode
/// but `SyncMode::Full`, `prepare` and `commit_prepared` are separate API calls
/// that commonly run on different threads, and `Wal::replay` states outright
/// that record order across stripes is not meaningful. So pass 1 *collects* and
/// pass 2 *resolves*; nothing anywhere depends on having seen a prepare before
/// its decision.
#[derive(Debug, Default)]
pub(crate) struct RecoveredPrepares {
    pub prepares: Vec<RecoveredPrepare>,
    pub decisions: Vec<RecoveredDecision>,
}

impl RecoveredPrepares {
    /// Record a prepare frame. Two *unresolved* prepares with the same id
    /// cannot both be legal: an id is reusable only once its previous
    /// instance's frames are off disk, which `PreparedRegistry::knows` enforces
    /// at prepare time.
    pub fn add_prepare(&mut self, p: RecoveredPrepare) -> Result<()> {
        if self.prepares.iter().any(|q| q.id == p.id) {
            return Err(OndaError::Corruption(format!(
                "two prepare records share transaction id {:02x?}; an id may only be \
                 reused once the previous instance is resolved and its WAL generations \
                 are unlinked",
                p.id
            )));
        }
        self.prepares.push(p);
        Ok(())
    }

    pub fn add_decision(&mut self, d: RecoveredDecision) {
        self.decisions.push(d);
    }

    /// The newest decision for `id`, or `None`.
    ///
    /// "Newest" is by generation: a retried `commit_prepared` after a crash can
    /// legitimately write a second decision in a later generation, and the
    /// later one is the one whose sequences were reserved most recently. Within
    /// one generation the stripes have no order, but a second decision in the
    /// *same* generation can only be a retry of an identical one — the entry is
    /// resolved before `commit_mu` is dropped.
    pub fn decision_for(&self, id: &[u8; 16]) -> Option<RecoveredDecision> {
        self.decisions
            .iter()
            .filter(|d| &d.id == id)
            .max_by_key(|d| d.gen)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u8, cf_id: u64, key: &[u8], bytes: usize) -> PreparedEntry {
        PreparedEntry {
            id: [id; 16],
            cf_ids: vec![cf_id],
            buf: key.to_vec(),
            writes: vec![PreparedWrite {
                cf_id,
                key: (0, key.len()),
                value: (key.len(), 0),
                ttl: 0,
                kind: crate::format::KIND_PUT,
            }],
            prepared_at: 0,
            bytes,
            prepare_gen: 0,
        }
    }

    /// The reservation key is the CF's **durable** id, not its handle address.
    /// A dropped-and-recreated family reuses the `Arc` address (review finding
    /// L2's lesson) but keeps the FNV id, so a reservation must follow the id.
    #[test]
    fn registry_key_uses_durable_cf_id() {
        let (a, b) = (crate::unified::cf_id("alpha"), crate::unified::cf_id("beta"));
        assert_ne!(a, b);
        let mut reg = PreparedRegistry::new(1 << 20);
        reg.register(entry(1, a, b"shared-key", 10));

        assert_eq!(reg.owner_of(a, b"shared-key"), Some([1u8; 16]));
        assert_eq!(
            reg.owner_of(b, b"shared-key"),
            None,
            "the same user key in another family is a different reservation"
        );
        // Re-deriving the id from the name yields the same key, which is what
        // makes the reservation survive a handle being dropped and recreated.
        assert_eq!(
            reg.owner_of(crate::unified::cf_id("alpha"), b"shared-key"),
            Some([1u8; 16])
        );
    }

    #[test]
    fn registry_byte_cap_rejects_with_too_large() {
        let mut reg = PreparedRegistry::new(100);
        reg.register(entry(1, 7, b"k1", 80));
        assert!(reg.check_capacity(20).is_ok(), "exactly at the cap fits");
        let err = reg
            .check_capacity(21)
            .expect_err("21 more bytes is past the cap");
        assert_eq!(err.kind(), "too_large");
        assert_eq!(reg.len(), 1, "a refused prepare registers nothing");
    }

    #[test]
    fn registry_resolve_frees_bytes() {
        let mut reg = PreparedRegistry::new(100);
        reg.register(entry(1, 7, b"k1", 80));
        assert_eq!(reg.bytes(), 80);
        assert!(reg.check_capacity(80).is_err());

        let got = reg.resolve(&[1u8; 16]).expect("registered above");
        assert_eq!(got.bytes, 80);
        assert_eq!(reg.bytes(), 0);
        assert!(reg.is_empty(), "the reserved key is released too");
        assert!(reg.check_capacity(100).is_ok());
        // The id is not reusable until its generations are unlinked.
        assert!(reg.knows(&[1u8; 16]));
        reg.forget(&[1u8; 16]);
        assert!(!reg.knows(&[1u8; 16]));
    }

    /// The pin covers the PAIR. A generation holding a prepare is withheld
    /// until the decision's own generation has flushed, and then the prepare is
    /// unlinked strictly first.
    #[test]
    fn sweep_unlinks_prepare_before_decision() {
        let mut g = WalGenState::default();
        g.pin(3);
        g.mark_flushed(3, "unified-wal-3.log".into());
        assert!(g.sweep().0.is_empty(), "an unresolved prepare pins gen 3");

        g.resolved([9u8; 16], 3, 5);
        assert!(
            g.sweep().0.is_empty(),
            "gen 3 waits for the decision's generation to flush"
        );

        g.mark_flushed(5, "unified-wal-5.log".into());
        let (unlink, retired) = g.sweep();
        assert_eq!(
            unlink,
            vec!["unified-wal-3.log", "unified-wal-5.log"],
            "the prepare is unlinked before its decision"
        );
        assert_eq!(retired, vec![[9u8; 16]]);
        assert_eq!(g.withheld(), 0);
    }

    /// A prepare and its decision in the SAME generation retire atomically with
    /// the file — condition (c)'s exclusion. Without it the predicate is
    /// circular and the generation never retires.
    #[test]
    fn sweep_retires_same_generation_pair_atomically() {
        let mut g = WalGenState::default();
        g.pin(4);
        g.resolved([1u8; 16], 4, 4);
        g.mark_flushed(4, "unified-wal-4.log".into());
        let (unlink, retired) = g.sweep();
        assert_eq!(unlink, vec!["unified-wal-4.log"]);
        assert_eq!(retired, vec![[1u8; 16]]);
    }

    /// One generation may hold several prepares; each pins independently.
    #[test]
    fn sweep_waits_for_every_pin_on_a_generation() {
        let mut g = WalGenState::default();
        g.pin(2);
        g.pin(2);
        g.mark_flushed(2, "unified-wal-2.log".into());
        g.resolved([1u8; 16], 2, 2);
        assert!(g.sweep().0.is_empty(), "the second prepare still pins gen 2");
        g.resolved([2u8; 16], 2, 2);
        assert_eq!(g.sweep().0, vec!["unified-wal-2.log"]);
    }

    /// Recovery resolves prepares it never pinned. Doing that through
    /// `resolved` would decrement a counter another transaction's pin owns, and
    /// the generation would be unlinked with a live prepare still in it — found
    /// live by `prep_recovery_idempotent_across_opens`, where the second reopen
    /// lost the unresolved prepare entirely.
    #[test]
    fn recording_a_recovered_pair_does_not_release_another_pin() {
        let mut g = WalGenState::default();
        g.pin(0); // an unresolved prepare recovered from generation 0
        // ...and a second prepare in the same generation whose decision was
        // already on disk, so recovery resolves it without ever pinning.
        g.record_pair([2u8; 16], 0, 0);
        g.mark_flushed(0, "unified-wal-0.log".into());
        assert!(
            g.sweep().0.is_empty(),
            "generation 0 still holds an unresolved prepare"
        );
        g.resolved([1u8; 16], 0, 0);
        assert_eq!(g.sweep().0, vec!["unified-wal-0.log"]);
    }

    /// Unpinned generations retire immediately — the ordinary flush path must
    /// not become slower because the feature exists.
    #[test]
    fn sweep_retires_unpinned_generations_at_once() {
        let mut g = WalGenState::default();
        g.mark_flushed(1, "unified-wal-1.log".into());
        g.mark_flushed(2, "unified-wal-2.log".into());
        assert_eq!(
            g.sweep().0,
            vec!["unified-wal-1.log", "unified-wal-2.log"]
        );
    }

    #[test]
    fn generation_parses_out_of_the_path() {
        assert_eq!(
            WalGenState::gen_of_path("/tmp/db/unified-wal-17.log"),
            Some(17)
        );
        assert_eq!(WalGenState::gen_of_path("unified-wal-0.log"), Some(0));
        assert_eq!(WalGenState::gen_of_path("/tmp/db/wal-0.log"), None);
        assert_eq!(WalGenState::gen_of_path("/tmp/db/MANIFEST"), None);
    }

    #[test]
    fn duplicate_unresolved_prepare_id_is_corruption() {
        let mut r = RecoveredPrepares::default();
        let mk = |gen| RecoveredPrepare {
            id: [7u8; 16],
            cf_ids: vec![1],
            records: Vec::new(),
            gen,
        };
        r.add_prepare(mk(1)).unwrap();
        let err = r.add_prepare(mk(2)).expect_err("an id may not be reused live");
        assert_eq!(err.kind(), "corruption");
    }

    /// A retried `commit_prepared` after a crash writes a second decision; the
    /// later generation is the one that names the sequences actually reserved.
    #[test]
    fn newest_decision_wins() {
        let mut r = RecoveredPrepares::default();
        r.add_decision(RecoveredDecision {
            id: [1u8; 16],
            commit: Some((10, 2)),
            gen: 1,
        });
        r.add_decision(RecoveredDecision {
            id: [1u8; 16],
            commit: Some((40, 2)),
            gen: 4,
        });
        assert_eq!(r.decision_for(&[1u8; 16]).unwrap().commit, Some((40, 2)));
        assert!(r.decision_for(&[2u8; 16]).is_none());
    }
}
