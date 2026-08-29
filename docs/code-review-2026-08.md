# ondaDB code review — 2026-08-29

**Scope:** full read of `src/` (~16k lines: `db`, `column_family`, `txn`, `memtable`,
`wal`, `sst/{writer,reader,iter,mod}`, `iterator`, `compaction`, `manifest`, `parts`,
`unified`, `storage`, `storage_s3`, `config`, `ingest`, `maintenance`, `table_cache`,
`range_lock`, `block`, `bloom`, `compress`, `comparator`, `encoding`, `format`, `error`,
`util`, `cache/*`, `memtable_arena`) plus the in-module and `tests/` suites, checked
against the claims in `docs/` and `AGENTS.md`.

**Method:** static review anchored on the documented invariants (durability ordering,
manifest serialization, checksum coverage, gap-free visibility, rotation protocol,
comparator stability, pinned blocks, part-move ordering), plus two scratch integration
repros (run, confirmed, then removed) and a full test run. Anchors are function/type
names; line numbers rot.

**Verification state at review time:** `cargo build` + `cargo build --features
unsafe-fastpath` clean; all **23 test binaries pass in both feature configurations**
(this includes the known intermittent `read_your_writes` flake — it did not fire today;
see §6.1). `cargo clippy` not re-run here.

---

## 1. Executive summary

ondaDB is a genuinely well-engineered LSM tree. The hard parts — crash-atomic manifest,
gap-free MVCC visibility, WAL batch atomicity per frame, checksummed everything,
zero-materialization flush, bounded compaction jobs with range locks, tiered parts with
a defensible move protocol — are implemented carefully and documented unusually
honestly. The test suite is broad and adversarial against its own history.

That said, the review found **five correctness/durability issues I would fix before
trusting the engine with data**, three of which were confirmed with live repros rather
than by reading alone:

| # | Severity | Finding | Confirmed |
| --- | ---------- | --------- | ----------- |
| F1 | **High** | `backup()` / `checkpoint()` / `clone_column_family()` silently omit tiered tables; the copied manifest references files that were never copied | ✅ repro |
| F2 | **High** | `attach_part` can install mutually-overlapping tables into the bottom level, breaking the disjointness that leveled reads rely on | ✅ repro |
| F3 | **High** | Multi-CF transactions are not atomic in the default (per-CF WAL) layout — neither across a crash nor across an in-process partial apply — and this is documented nowhere | code-traced |
| F4 | **High** | `attach_part` copies files with `std::fs::copy` and flips the manifest without fsyncing the copies — violates the engine's own "never route a reader to a file that is not durably in place" invariant | code-traced |
| F5 | **Med-High** | WAL files are created without a parent-directory fsync; under `SyncMode::Full` an acknowledged commit can vanish after a crash in the new-file window | code-traced |

Beyond those, the biggest *systemic* observations are:

- **Tiering is the least-polished subsystem.** F1, F4, the S3 orphan-GC gaps (known),
  and the compaction-input-delete gap (known) all cluster there. Every one of them is
  "only a storage leak / only a backup problem" in isolation — but F1 is silent data
  loss in a *backup*, which is the operation people run specifically to be safe.
- **A meaningful slice of the public configuration surface is dead** (§4): twelve
  options that are declared, defaulted, validated — and never read, including
  `num_compaction_threads` (exactly one compaction worker is spawned regardless) and
  `default_isolation_level` (`DB::begin()` hardcodes `Snapshot`, the field says
  `ReadCommitted`).
- **The documented 16 KiB default block size is not actually in force** (§3, M6): every
  engine write path passes 4 KiB explicitly, so the carefully-argued index-memory
  rationale attached to `DEFAULT_BLOCK_SIZE` in `sst/mod.rs` describes a default no
  caller uses.
- **Unified-memtable mode has a scan pathology** (§3, M1): every iterator construction
  materializes and clones the *entire* shared memtable — the exact O(entries)-per-scan
  cost the lazy per-CF iterator was built to eliminate, made worse by cross-CF scope.

None of the five top issues invalidate the core commit path; four of the five live in
administrative/tiering paths. Severity reflects "will eat data," not "will corrupt the
LSM in normal operation."

---

## 2. How to read the findings

Each finding gives: location (function anchors), what happens, why it matters,
evidence, and a suggested fix direction. Severities:

- **High** — silent data loss, wrong answers, or a violated durability contract.
- **Medium** — real operational/perf/robustness cost, or a correctness issue needing
  unusual-but-legal input.
- **Low** — fragile invariant, cosmetic, or defensive hardening.

Findings marked ✅ were reproduced with a scratch integration test during this review
(test deleted afterwards; the repro steps are included so they can be re-created as
regression tests — they should be).

---

## 3. Detailed findings

### F1 (High) — Backups, checkpoints, and CF clones silently drop tiered tables

**Location:** `maintenance.rs::snapshot_to` (and `clone_column_family`).

**What happens.** `snapshot_to` iterates the freshly-persisted manifest and, for every
`SstMeta`, builds the source path as:

```rust
let src = format!("{}/{}.{ext}", self.inner.cf_dir(&cfm.name), sst.id);
if !Path::new(&src).exists() {
    continue; // vlog absent when the SSTable has no large values
}
```

`cf_dir` is the **default tier** (`<db>/cf-<name>`). A table whose `sst.tier` is a
named tier (or which carries an `object` name on a shared tier) lives elsewhere; the
`exists()` check fails and the `continue` — written to skip absent *vlogs* — silently
skips the whole table pair. The backup's manifest, however, is a byte-copy of the
source manifest (`manifest.save(dir.join("MANIFEST"))`), so it still references the
tiered ids. `clone_column_family` has the same shape (`if Path::new(&s).exists()`),
so a clone of a CF with tiered parts is missing those files too.

**Why it matters.** A backup that restores to a database whose reads fail with `Io
NotFound` for exactly the data someone tiered off. There is no warning at backup time;
the failure surfaces at restore time, which is the worst possible moment. Worse: if the
*original* tier root is still reachable, the restored copy appears to work — it reads
through the live tier directory — so a drill can pass while an off-site restore is
broken.

**Evidence (repro, confirmed 2026-08-29).** CF with partition `img/` moved to a local
`hdd` tier; `db.backup(dir)`; reopen:

```
backup+tier       img/000 = [73,77,71]  — read THROUGH the original tier dir (not standalone)
backup-standalone img/000 = Io(... NotFound ...) — tiered data LOST
backup-standalone etc/000 = [69,84,67]  — default-tier data fine
```

**Fix direction.** Resolve the source path through the tier registry exactly like
`ColumnFamily::klog_path_for(&meta)` does, read through `storage_for(meta.tier)`, and
write into the backup under a **default-tier layout** while rewriting the copied
manifest's `tier`/`object` fields to `None` (a backup should be self-contained). Same
for `clone_column_family`. Add a regression test that backs up a tiered CF and restores
it with no tiers configured.

---

### F2 (High) — `attach_part` can install overlapping tables into the bottom level

**Location:** `parts.rs::attach_part` (staging loop), `column_family.rs::
insert_bottom_sorted`.

**What happens.** For each staged table the placement decision is:

```rust
let at_bottom = !cf.bottom_overlaps(&min_key, &max_key);
```

`bottom_overlaps` is evaluated against the **live level set**, and all staged tables
are inserted only after the whole loop. Staged tables never see *each other*. Two
tables in the attach directory whose ranges overlap one another (but not live data)
are therefore both `insert_bottom_sorted`'d into a level whose readers assume
disjoint, sorted tables (`find_overlapping` binary-searches and picks **one** table
per level ≥ 1).

**Why it matters.** The level-≥1 disjointness invariant is load-bearing: point reads
binary-search for the single candidate table and never look at a second one. With two
overlapping bottom tables, one table's data is unreachable through point reads, and
scans see two sorted runs with potentially identical internal keys (same key, same
seq — ties the merge resolves arbitrarily). This is wrong answers, silently.

**Reachability.** Normal flows (a detached part of one partition) produce disjoint
files. But the API takes *any* directory of klogs; nothing validates mutual
disjointness, and the "same lineage" guard is weak — `reader.max_seq() > visible` is
satisfied by any database whose sequence counter happens to be past the files' seqs
(the repro hit exactly this: two independent databases each assigned seqs 1–4, and
both were accepted). A consumer that assembles an attach dir from multiple sources,
or a partially-overlapping export, trips it.

**Evidence (repro, confirmed).** Two one-CF databases each compacted to one bottom
table over `k0..k3` with values `AAA`/`BBB`; both klogs copied into one dir; attached
into a target CF whose bottom level held only `m0..m9`:

```
levels after attach: [(3, 550)]   // one level, three tables: m-run + TWO overlapping k0..k3 runs
get(k1) = [65,65,65]              // "AAA" wins the binary search; "BBB" unreachable
```

**Fix direction.** During staging, track the union of staged extents and route a table
to L0 when it overlaps either the live bottom level **or any previously staged table**
(alternatively: attach to bottom only if the whole staged set is mutually disjoint,
else all of it goes to L0). While there, consider making the lineage check real
(see F3's cousin in §6.4 — cross-DB attach is explicitly future work, so at minimum
document that "lineage" means "seq watermark", not provenance).

---

### F3 (High) — Multi-CF transactions are not atomic in per-CF WAL mode (undocumented)

**Location:** `txn.rs::apply_per_cf_groups`, `txn.rs::commit`, `wal.rs::append_batch`.

**What happens.** A `Txn` spanning two column families commits by writing **one WAL
frame per CF** (`apply_commit` per group), each to that CF's own WAL. Two failure
modes:

1. **Crash between the two appends.** Recovery replays per-CF WALs independently:
   CF A's batch replays, CF B's never existed. The transaction is durable on one CF
   and absent on the other. The WAL-frame atomicity invariant ("one frame per
   committed batch… replays either whole or not at all") holds *per frame* — but the
   transaction is two frames, and nothing binds them.
2. **In-process partial apply.** `apply_per_cf_groups` iterates a `HashMap<usize,
   CfGroup>` — **nondeterministic iteration order** — and returns the first error. If
   CF B's `apply_commit` fails (disk full, WAL write error) after CF A's succeeded,
   CF A's records are in its WAL and memtable. `commit` then *unconditionally*
   publishes the whole reserved range:

   ```rust
   self.db.publish_range(start, start + n);
   if let Some(error) = application.error { … return Err(error); }
   ```

   So the error is returned to the caller **and** CF A's half of the transaction
   becomes visible. The comment defending `publish_range`-on-failure ("its records
   never reached the WAL or memtable") is true only for the *poison-before-apply* case
   that `poisoned_txn_commit_does_not_publish` tests — it is false for a mid-apply
   failure of a later CF group.

**Why it matters.** "A commit that returns an error must not have published any of its
writes" is the contract the engine's own regression test pins — and it does not hold
for multi-CF transactions. Neither `docs/architecture.md` (write path) nor
`docs/concurrency-and-safety.md` mentions that cross-CF atomicity is absent; a reader
would reasonably assume a "transaction" is atomic across the CFs it touches.
Notably, **unified-memtable mode fixes this** — one shared WAL, one frame covering
all CFs — which makes the asymmetry a layout decision that was never written down.

**Fix direction (pick one and document it).**

- *Minimum:* document that per-CF-mode transactions are atomic per column family only,
  and reject (or split) multi-CF commits that request atomicity they won't get.
- *Better:* keep a per-txn commit record — e.g. write CF frames plus a small
  commit-marker frame to a shared journal (or to the lowest CF id) that recovery
  treats as the transaction's commit point, discarding frames whose marker is absent.
- Regardless: make `apply_per_cf_groups` iterate in a deterministic order (sorted by
  CF id) so partial-failure outcomes are at least reproducible, and only publish
  ranges whose apply actually succeeded (the gap-free cursor machinery can be fed the
  successful prefix; a failed suffix must still be published-as-empty as today — see
  the existing comment — but the *successful prefix plus failed suffix* case currently
  publishes the prefix as if the whole txn committed, which is exactly what should be
  avoided or at least documented).

---

### F4 (High) — `attach_part` flips the manifest before the copied files are durable

**Location:** `parts.rs::attach_part` (copy loop + `persist_manifest`).

**What happens.** Attach copies files with plain `std::fs::copy`, installs handles,
then calls `persist_manifest()`. Nothing fsyncs the copied files (or the CF directory)
before the manifest rename makes them the source of truth. Compare `relocate_part`,
which routes every copy through `Storage::create` + `StorageWriter::finish` — the
local implementation fsyncs file **and** parent dir — precisely so the manifest flip
only ever names durable bytes.

**Why it matters.** This is a direct violation of the module's own stated invariant:
*"Every catalog change is one crash-atomic manifest rewrite, so a crash can only
leave orphan files, never route a reader to a file that is not durably in place."*
After a crash in the window between the manifest rename and the page cache flushing
the copies, the manifest names ids whose klogs are empty or torn. The footer magic
check will at least fail loudly at open (`Corruption`) — but the *original* detached
files are gone (moved by `detach_part`), so this is unrecoverable data loss for the
attached part, not a clean reopen.

**Fix direction.** Copy via `cf.tiers().storage_for(None).create(...)` +
`finish()` (the same choke point the mover uses), or `std::fs::copy` followed by an
explicit `File::sync_all` on the destination plus a parent-dir fsync, before
`persist_manifest`. One-line-ish fix; the machinery already exists.

---

### F5 (Med-High) — WAL creation is not followed by a directory fsync

**Location:** `wal.rs::Wal::open` (stripe file creation), `db.rs::
acquire_dir_lock`/`open_impl` (`create_dir_all`), `column_family.rs::create`.

**What happens.** `Wal::open` creates/truncates the stripe files and never fsyncs the
parent directory. Same for the CF directory at `ColumnFamily::create`
(`create_dir_all`, no sync) — the subsequent `Manifest::save` fsyncs the *DB root*
(making the `cf-<name>` dirent durable) but nothing ever syncs the CF directory
itself, so the `wal-0.log` dirent inside it is not durable. The SSTable path got this
right (`Writer::finish` → `sync_parent_dir`, with an explicit comment explaining why);
the WAL path did not.

**Why it matters.** Under `SyncMode::Full`, `Txn::commit` returns `Ok` after
`sync_data` on the WAL file. A crash shortly after the *first* WAL of a new
generation/CF can leave the file's directory entry unpersisted: the file vanishes,
and acknowledged commits are lost. This is the classic LevelDB/RocksDB "sync the
parent on file creation" requirement; its absence is a real durability-contract gap
in a narrow but real window (rotation opens a new generation every
`write_buffer_size` bytes, so it recurs regularly on long-lived databases).

**Fix direction.** In `Wal::open`, after creating a stripe file that did not
previously exist (or unconditionally on generation bump), `File::open(parent)` +
`sync_all` — the same `sync_parent_dir` helper `sst/writer.rs` already has (hoist it
to a shared util). Also sync the CF directory once at CF creation.

---

### M1 (Medium) — Unified-memtable mode: every scan materializes the whole shared memtable

**Location:** `unified.rs::entries_for_cf`, called from `column_family.rs::
append_memtable_children`.

**What happens.** In unified mode, building a CF iterator calls
`u.entries_for_cf(self.id)`, which runs `s.mem.snapshot()` — a **full materialization
of the shared memtable across all CFs, with a `Vec` clone of every entry's key and
value** — filters it by the 8-byte CF-id prefix, then re-inserts the CF's slice into
a fresh overlay `Memtable` (another copy). `snapshot()` is then also run on every
sealed imm. This happens on **every** `new_iterator` / `Txn::new_iterator_bounded`
call.

**Why it matters.** This is precisely the pathology `LazyMemIter` was created to fix
(the docs record 1.3 ms per scan at 2k entries, growing linearly) — reintroduced in
unified mode, and *wider*: the cost scales with the **total** memtable across all
CFs, not the scanned CF's slice, and clones every value twice. A scan-heavy workload
that opts into unified mode gets O(database) iterator construction. The unified docs
say "ordered iteration and flush re-sort a CF's slice" — true, but silent about the
cost.

**Fix direction.** A prefix-range lazy iterator: the unified memtable is bytewise
ordered by `cf_id || key`, so a CF's slice is the contiguous range
`[id||\x00…, id||\xff…]`. Two `lower_bound` cursors over the 16 shards (the same
`LazyMemIter` machinery keyed on the prefixed compare) give a lazy, zero-copy CF
slice; the prefix strip happens in `user_key()`. Until then, at minimum document the
cost so users don't enable unified mode for scan-heavy workloads.

---

### M2 (Medium) — Compaction failures are swallowed with no trace

**Location:** `db.rs::compact_worker` (`let _ = compaction::run(&db, &cf);`),
`compaction.rs::run` (error aborts the job loop).

**What happens.** An I/O failure inside a compaction (output write failure, vlog read
error, tier failure) propagates to the worker, which discards it. Unlike flush
failures (which poison the DB) and manifest failures (which poison), compaction
failure has no observable consequence: no poison, no log, no counter. The level debt
the failed job was going to pay stays; writers get throttled by
`pace_for_compaction_debt`; nothing tells anyone why.

**Why it matters.** A persistently failing disk/tier turns into "writes are slow and
`compaction_debt` is pinned near the hard limit" — diagnosable only by inference.
There is a defensible design argument (compaction failure isn't data loss; fail-stop
would be too aggressive), but *zero observability* is not that argument.

**Fix direction.** At minimum a failure counter + last-error string on the CF
(surfaced in `CfStats`); optionally a bounded retry with backoff inside `run` before
giving up. Poisoning is probably wrong here — but silence definitely is.

---

### M3 (Medium) — `commit_mu` is held across fsync and rotation stalls

**Location:** `txn.rs::commit` (guard scope), `column_family.rs::apply_commit` (gate
loop), `column_family.rs::rotate_memtable`.

**What happens.** Snapshot/Serializable commits hold the DB-wide `commit_mu` from
conflict validation through the entire `apply_commit` — which includes the WAL append
- fsync (`SyncMode::Full`), the memtable insert, and, on the size-trigger path, a
full `rotate_memtable` (writer drain + `Wal::open` + level swap). `apply_commit`'s
stall gate (`imm.len() >= l0_queue_stall_threshold` → `cond.wait`) also runs under
`commit_mu`.

**Why it matters.** No deadlock was found (nothing acquires `commit_mu` while holding
`rot`/`state`, and flush completion needs no `commit_mu`), but the latency coupling
is severe: one Snapshot-level commit that stalls behind a flush drain blocks **every
other Snapshot/Serializable commit in the database** for the drain's duration, and
serializes all of them behind each other's fsyncs. The docs describe `commit_mu` as
guarding "conflict check → apply → publish" — the apply part is the problem.

**Fix direction.** Shrink the critical section: validate under `commit_mu`, drop it,
apply, re-validate-or-publish under it (the standard optimistic pattern), or track
per-CF write intents. At minimum, move the stall-gate wait outside the `commit_mu`
span (it is already outside `active_writers`, which is what rotation needs — the
ordering constraint is only validation-vs-apply).

---

### M4 (Medium) — Crash-orphan SSTables in the CF directory are never collected

**Location:** `db.rs::sweep_move_orphans` / `sst_is_misplaced` (unknown ids are
explicitly untouched), `db.rs::flush_per_cf`, `compaction.rs::compact_inputs`.

**What happens.** A crash between `Writer::finish` (file durable) and
`persist_manifest` leaves a fully-written SSTable that no manifest ever referenced.
The startup sweep only deletes **known-id** files sitting in the *wrong tier
location*; unknown ids are deliberately skipped (correctly — they might be in-flight
output of a concurrent process… except the LOCK file excludes that). These orphans
accumulate in `<db>/cf-<name>/` forever. `next_file_id` reuse partially self-heals
(a reused id truncates the orphan when a new writer opens the same path) but only
probabilistically. The same applies to interrupted compaction outputs. (The S3/named
tier variants of this are documented known gaps; the **default-tier** variant is
not.)

**Why it matters.** Slow storage leak with no GC path; on a long-lived database with
frequent crashes (or kill -9 during load tests) it can grow large enough to matter,
and nothing reports it.

**Fix direction.** At open, after recovery, any `<id>.klog`/`.vlog` in a CF directory
whose id is not in the loaded manifest and which is older than some grace period (to
avoid racing a just-started flush in the same process — the LOCK file already
excludes other processes) can be unlinked. This is exactly `sweep_move_orphans`
extended to "unknown id in default location" — with the grace-period caveat spelled
out.

---

### M5 (Medium) — Manifest is fully rewritten on every structural change

**Location:** `manifest.rs::save` (whole-file encode + fsync + rename), called from
`persist_manifest` on every flush, compaction, part move, CF create/drop.

**What happens.** The crate's own sizing probe (`manifest_encoded_size_at_scale`,
ignored test) documents it: ~12.4 MiB re-encoded and fsynced *per persist* at 100k
parts; 1.2 MiB at 10k. Every L0 flush pays it.

**Why it matters.** It is the scaling wall for the partitioned/tiered use cases the
parts machinery exists for. The team knows (the test's comment says "the motivation
for an incremental (edit-log) manifest") — recorded here so it's in the issue list,
not just a test comment.

**Fix direction.** Incremental edit log with periodic full-rewrite compaction of the
manifest itself (the standard LevelDB approach). Large change; plan it.

---

### M6 (Medium) — The documented 16 KiB default block size is not in force on any write path

**Location:** `sst/mod.rs::DEFAULT_BLOCK_SIZE` (= 16 KiB, with a long measured
rationale about index memory), vs `column_family.rs::DATA_BLOCK_SIZE` (= 4 KiB) used
in `writer_opts` (flush + ingest), and `compaction.rs::cf_writer_opts`
(`block_size: 4 << 10`).

**What happens.** `DEFAULT_BLOCK_SIZE` only applies when a caller passes
`block_size: 0`, which no engine path does. Every table the engine writes — flush,
ingest, compaction — uses **4 KiB** blocks. The 0.8-era rationale attached to the
16 KiB constant ("a quarter of the index entries", motivated by the 12 GB-RSS
incident) therefore describes a default that is dead config. The actual fix for that
incident was the `TableCache` — which is fine — but the block-size story in
`sst/mod.rs` and in reviewers' heads is wrong.

**Why it matters.** Doc/config drift with real consequences: index-entry counts (and
thus resident reader memory per table) are 4× what the documentation implies, and
anyone tuning `block_size` "back down" from a default of 16 KiB is tuning from a
number that was never active.

**Fix direction.** Either wire the intent (use `DEFAULT_BLOCK_SIZE` in
`writer_opts`/`cf_writer_opts` after benchmarking) or delete the constant and fix the
comment to say blocks are 4 KiB by policy. One truth, written once.

---

### M7 (Medium) — One compaction worker; `num_compaction_threads` is dead

**Location:** `db.rs::spawn_workers` (spawns exactly one `onda-compact` thread),
`config.rs::Options::num_compaction_threads` (default 2, never read — see §4).

**What happens.** The 0.8.0 range-lock design explicitly enables concurrent
compactions ("jobs on disjoint ranges share no inputs, so they run concurrently"),
and `docs/architecture.md` says so. But there is exactly one background compaction
thread, so background jobs never overlap. Concurrency is reachable only between the
background worker and a manual `DB::compact`, or two manual compacts.

**Why it matters.** The advertised parallelism doesn't exist by default; on the
multi-core boxes the compaction docs benchmark against, debt pays down at 1/N the
possible rate.

**Fix direction.** Spawn `num_compaction_threads.max(1)` workers (the channel is
already multi-consumer; `pick_compaction`'s try-lock-and-skip picker is already
safe for it — that is visibly what it was designed for). Verify the part-mover
`mover_running` guard still holds (it does — it's a CAS).

---

### M8 (Medium) — `flush_memtable`/`close`/`checkpoint` wait on the *global* flush counter

**Location:** `db.rs::flush_memtable`, `db.rs::close` — both spin on
`pending_flush > 0`, a DB-wide counter fed by every CF's rotations.

**What happens.** "Flush CF A and wait" actually waits for every CF's in-flight
flush, including ones triggered continuously by concurrent writers to unrelated CFs.
Under sustained multi-CF write load, `flush_memtable` (and therefore `checkpoint`,
`backup`, `clone_column_family`, and `close`) can wait arbitrarily longer than the
target CF's own flush.

**Fix direction.** Track pending flushes per CF (an `AtomicUsize` on the CF or a
count keyed by CF in `DbInner`) and wait on the specific CF's counter; close can
still drain globally.

---

### M9 (Medium) — Reader open trusts the manifest's level field without bound

**Location:** `column_family.rs::load` — `vec![Vec::new(); max_level]` where
`max_level = s.level as usize + 1`.

**What happens.** A CRC-valid manifest carrying a large `level` (corruption that
happens to pass CRC — CRC guards accidents, not this — or a manifest from a
future/hand-edited source) makes `load` attempt to allocate `level + 1` empty
`Vec`s: a `u32::MAX` level is a ~34 GB allocation (abort) before any validation
runs; a merely-large level silently creates thousands of empty levels that then
interact with `bottom_level_index()` semantics (the "bottom" becomes an empty
level, so tombstone-drop-at-bottom decisions change).

**Fix direction.** Clamp/reject levels above a sane maximum (the compaction geometry
implies single-digit levels; anything > 64 is corrupt) with a `Corruption` error.

---

### M10 (Medium) — Robustness: panics on malformed-but-CRC-valid data

**Location:** `sst/iter.rs::seek` — `decode_entry(bytes, self.offsets[mid] as
usize).unwrap()`; also `format.rs::user_key`/`split_internal_key` slice panics on
keys shorter than the 8-byte trailer (reachable from a corrupt memtable key, which
cannot normally exist).

**What happens.** Block CRCs catch accidental corruption before these paths in most
cases, but a *validly framed* block whose entries are malformed (writer bug, or bit
flip that recomputes… not possible with CRC — but a hostile/buggy custom `Storage`
backend or a future format change can produce it) turns an I/O error into a process
panic inside a scan. The reader elsewhere is careful to return `Corruption`.

**Fix direction.** Replace the `unwrap` with the same `err = Some(e)` pattern
`load_block` already uses.

---

### L1 (Low) — Transaction-overlay entries tie with committed entries at `seq == read_seq`

**Location:** `txn.rs::new_iterator_bounded` (overlay `put_ref(..., rs, ...)` where
`rs` is the read seq), `iterator.rs::MergingIter::before` (ties compare false both
ways), `iterator.rs::VisibleVersion::consider` (first-seen wins).

**What happens.** Buffered txn writes are stamped at exactly `read_seq`, the same
seq a concurrently-committed write by *another* thread can carry (`visible_seq ==
read_seq` means entries at that seq exist). When both an overlay entry and a live
entry hold the same `(key, seq)`, the merge's winner is decided by heap order —
which today favors the overlay because it is child 0 and binary-heap sift-downs
never swap on ties. That is correct **by accident of implementation**, not by
contract: any change to child ordering or heap mechanics silently flips it, and the
failure mode is a txn's iterator seeing a stale value for its own buffered write
(point `get` is unaffected — it checks the buffer first).

**Fix direction.** Make it explicit: stamp overlay entries at a seq guaranteed
above any live entry (e.g. `read_seq` is fine for *fixed* snapshots if you also
define tie-break "earlier child wins" as a tested invariant; cleanest is a dedicated
overlay-first rule in the merge or `u64::MAX`-style sentinel semantics documented in
one place). Add a regression test with a forced tie.

---

### L2 (Low) — `THREAD_COMMIT_FLOOR` keyed by `DbInner` address

**Location:** `db.rs::db_key` / thread-local map.

Already flagged in `docs/concurrency-and-safety.md`: a dropped DB whose allocation
address is reused hands a stale read floor to the successor, breaking
read-your-own-writes for that thread until it commits. Fix cheaply by keying on a
monotonic id minted per `DbInner` (an `AtomicU64` counter at construction) instead of
the pointer. Listed here because it's a one-line fix for a documented latent bug.

### L3 (Low) — Savepoint rollback ignores the Serializable read set

**Location:** `txn.rs::rollback_to_savepoint` truncates writes/buf but not
`read_set`/`read_cfs`.

Reads performed after the savepoint remain in the validation set, so the commit can
abort on conflicts with keys the transaction no longer logically read. Conservative
(spurious abort), never incorrect. Trim the read set alongside (it needs the same
`(cf, key)` recording discipline; simplest is to snapshot the read set length at
`set_savepoint` only if keys are recorded in order — they are, so an index bound
works).

### L4 (Low) — `Txn::reset` skips `wait_visible_at_own_floor`

**Location:** `txn.rs::reset` vs `begin_with_isolation`.

`begin` waits out a publication gap before pinning a fixed snapshot (the 0.7.4
self-conflict fix); `reset` pins `visible_seq()` directly, so a reset-to-Snapshot txn
can re-introduce the self-conflict the begin path fixed. Same two-line treatment.

### L5 (Low) — `peek_seq` can observe in-flight writes

**Location:** `column_family.rs::peek_seq` (reads memtable/SSTs at `u64::MAX`).

A concurrent ReadCommitted commit that has inserted into the memtable but not yet
published is visible to conflict validation, producing rare spurious `Conflict`
aborts. Conservative and safe; noting for completeness so nobody "fixes" it into an
under-check.

### L6 (Low) — FIFO eviction details

**Location:** `column_family.rs::take_fifo_victims`.

TTL-based victim selection stats every klog (`std::fs::metadata`) **while holding the
CF state write lock**, on every FIFO compaction tick; age is file mtime (documented
approximation — a restore resets ages); paths are default-tier only (benign today
because FIFO data never leaves L0, but it will bite if FIFO ever tiers). Cache the
mtimes or move the stats outside the lock.

---

## 4. Unimplemented features and dead configuration

Verified by grep — each item below is declared, defaulted, and **never read** by any
engine code (declaration + `Default` only):

**Dead `Options` fields**

| Field | Notes |
| --- | --- |
| `num_compaction_threads` | One worker spawned regardless (see M7). |
| `max_concurrent_flushes` | Comment says "0 => == num_flush_threads"; nothing reads it. |
| `max_memory_usage` | Comment claims "0 => auto (≈75% system memory)" — no such mechanism exists anywhere. |
| `log_level` (+ `LogLevel` enum) | There is no logging subsystem at all. |
| `unified_memtable_skip_list_max_level` / `unified_memtable_skip_list_probability` | The unified store uses the plain `Memtable`; these were for a never-landed unified skip-list. |

**Dead `ColumnFamilyConfig` fields**

| Field | Notes |
| --- | --- |
| `default_isolation_level` | `DB::begin()` hardcodes `IsolationLevel::Snapshot`; the field defaults to `ReadCommitted` and is never consulted. Actively misleading. |
| `min_levels`, `dividing_level_offset` | Level count is derived from data; neither is read. |
| `tombstone_density_trigger`, `tombstone_density_min_entries` | No tombstone-density-triggered compaction exists (default `0.0` = "disabled" is the only value). |
| `index_sample_ratio`, `block_index_prefix_len`, `enable_block_indexes` | The index is exhaustive per data block; none of these are read. |
| `skip_list_max_level`, `skip_list_probability` | Memtable heights are constants in `memtable.rs`/`memtable_arena.rs`. |
| `comparator_ctx_str` | Comparators take no context. |
| `min_disk_space` | No disk-space guard exists. |

**Dead API/format surface**

- `format::flags::DELTA_SEQ` — defined, never written or read; sequence delta-encoding
  is unimplemented.
- `OndaError::Busy` ("e.g. compaction in progress") and `OndaError::MemoryLimit` —
  never constructed anywhere.
- `bloom.rs::encode_sparse` / `decode_sparse` — a complete sparse serialization with
  tests, never used by the writer or reader (only the dense form is).
- `Wal::size()` — public, unused by the engine.
- `single_delete` — the flag round-trips through WAL, memtable, SST and compaction,
  but **no code implements its semantics**: reads treat it as a plain tombstone
  (`flags & TOMBSTONE` is set alongside it) and compaction performs no
  single-delete/next-version collapse. The API doc ("a delete hint for keys written
  at most once") promises an optimization that does not exist; behavior is *correct*
  (conservative), the feature is not implemented.

**Genuinely missing features (beyond dead knobs)**

- **Compaction rate limiting** — no I/O rate limiter; compaction and foreground reads
  compete unthrottled (only write-*pacing* against debt exists).
- **Incremental manifest** (see M5).
- **Per-range approximate size / metadata queries** — nothing like RocksDB's
  `GetApproximateSizes`, which range-compaction and tooling usually build on.
- **Multi-process anything** — the LOCK file excludes concurrent opens; there is no
  read-only secondary/replica mode (documented non-goal, listed for completeness).
- **Orphan GC for default-tier crash residue** (see M4) and for S3 (documented).
- **Backup/checkpoint of tiered data** (F1 — this is the unimplemented half of
  "backup works").

---

## 5. Documentation drift

Docs are unusually good; these are the places they no longer match the code:

1. **Shard count.** `AGENTS.md`, `docs/architecture.md`, and the
   `memtable_arena.rs` header all say **256** shards; `memtable.rs::NUM_SHARDS` is
   **16** (with a comment explaining a measured 5.9× scan win from the reduction —
   the change just never propagated to the other three places).
2. **`forbid(unsafe_code)`.** `AGENTS.md` says the default build is
   `#![forbid(unsafe_code)]`; `lib.rs` uses `deny` with a comment explaining why
   (the Linux `CLOCK_REALTIME_COARSE` exception). AGENTS.md is stale — and the
   distinction matters to anyone auditing the unsafe surface.
3. **Lock inventory.** `docs/concurrency-and-safety.md`'s lock table still lists
   `detach_part`/`attach_part`/`relocate_part` under `ColumnFamily::compact_mu`
   ("a part move holds it across the copy…"). Since 0.8.0 those operations take
   **range locks** (`lock_partition_span`); `compact_mu` is only taken by manual
   compact and FIFO. The same doc's later sections describe the range-lock design
   correctly — the table contradicts the prose.
4. **Block size** (see M6) — `sst/mod.rs` documents 16 KiB as the operative default;
   every write path uses 4 KiB.
5. **Cross-CF transaction atomicity** (see F3) — documented nowhere, in either
   direction.
6. Minor: `sst/reader.rs` doc-comment for `consider_sstables`'s bloom handling vs
   `Reader::get` — the CF path deliberately bypasses `Reader::get`'s internal bloom
   check to hash once; fine, but the reader-level `get` doc doesn't say callers are
   expected to pre-filter.

---

## 6. Known limitations — acknowledged, verified, not re-litigated

For completeness; each is already documented in-tree and confirmed by this review to
match the code:

1. **`read_your_writes` intermittent failure under `unsafe-fastpath`** — transient
   NotFound, never reproduced single-process, narrowed to the arena memtable itself
   (`docs/concurrency-and-safety.md`). Did not fire in today's runs. Still open; the
   `height` Relaxed store remains the stated prime suspect.
2. **Serializable = point-read validation only** — range scans untracked, phantoms
   possible; documented on `IsolationLevel::Serializable` and in the txn module docs
   (plus the in-code TODO for full SSI).
3. **`detach_part` is not snapshot-consistent** — by design, documented.
4. **S3 tiering gaps** — crash-mid-move orphan sweep and compaction's obsolete-input
   delete are local-path only, so S3 orphans leak (manifest stays authoritative;
   reads unaffected). Documented in AGENTS.md and `docs/parts-and-tiers.md`; code
   confirmed (`sweep_cf_location` walks `std::fs`; `remove_compaction_inputs` uses
   default-tier paths).
5. **Cross-DB attach with seq remapping** — future work; the current lineage check is
   a seq-watermark comparison only (see F2 for why that's weaker than it sounds).
6. **`clear_column_family` unsupported in unified mode** — rejected with an error,
   documented.
7. **Documented non-goals** — read replicas, Spooky/DCA compaction, range compaction,
   CF rename/hot-reconfig, write-amp statistics. Confirmed absent.

---

## 7. What was verified during this review

- `cargo build` and `cargo build --features unsafe-fastpath` — clean.
- `cargo test` and `cargo test --features unsafe-fastpath` — **all 23 test binaries
  `test result: ok`** (checked per-binary, not via tail).
- F1 and F2 reproduced with a scratch integration test (`tests/review_repro.rs`,
  since deleted; repro steps embedded in §3 so they can be reinstated as regression
  tests — recommended).
- Dead-option claims verified by exhaustive grep (declaration/`Default` sites only).
- Durability orderings re-traced on the flush path (`Writer::finish` → sync klog+vlog
  → dir fsync → `persist_manifest` → WAL delete) and compaction path — both correct.
- Lock-order claims in the docs spot-checked against acquisitions found in code
  (one drift, §5.3).

## 8. Suggested fix order

1. F1 (backup of tiered data) — silent data loss in the safety operation.
2. F4 + F5 (fsync ordering for attach + WAL creation) — small diffs, close real
   durability windows, machinery already exists.
3. F3 — at minimum the documentation half immediately; the mechanical half
   (deterministic apply order) is a one-liner, the atomicity half is a design
   decision.
4. F2 — staging-loop overlap check; small.
5. M6 + M7 + the dead-options sweep (§4) — one trust-restoring cleanup PR: delete or
   wire twelve dead knobs, one truth about block size, spawn the compaction threads
   the docs promise.
6. M1 (unified scan pathology) if unified mode has users; else document.
7. M2/M3/M4/M5/M8–M10 and the L-items as opportunity permits.
