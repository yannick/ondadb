# Changelog

## Unreleased — Breaking: yoloDB format epoch 1

**ondaDB moves onto yoloDB format epoch 1**, the on-disk format family ondaDB
and wavesdb converge on (plan C step 1). Every persisted identifier changes,
so **a 0.9.x database is not readable by this release's engine**, and a
directory written by this release is not readable by 0.9.x. The crate is still
`ondadb`; only the format changed. See `docs/formats.md` and the registry in
`docs/format-registry.md`.

### Breaking: the format epoch

- **CRC32-C everywhere.** `encoding::checksum` now computes CRC32-C (the `crc32c`
  crate: hardware CRC on aarch64 and x86-64). 0.9 documented CRC32-C but computed
  CRC-32/IEEE; every artifact's checksum changes.
- **SSTable footer** — 96 bytes, magic `YOLOST01`, `format_version` 1, a CRC32-C
  over the footer (0.9's was unchecksummed), a table **capability word**, and the
  aux-block handle inside the footer. Footer flags are only `0x01` bloom and
  `0x02` btree: restart trailers are on **every** data block
  (`WriterOptions::restart_interval = 0` is refused), vlog frames have one
  format, and extended/prefix-delta/range meaning moved into the capability word.
- **Value logs** start with a 32-byte `YOLODBVL` header; frame offsets are
  absolute (the first frame is at 32).
- **Codec ids**: LZ4 (and `Lz4Fast`) is stored as **6** — the same raw LZ4 block
  bytes 0.9 stored as 2. Ids **2 and 4 are burned** (0.9 LZ4 / wavesdb zstd) and
  refused as `UnsupportedFormat`; 7 (zstd-dict) and 8 (brotli) are reserved.
  `Compression::from_u8` is replaced by `Compression::codec_id` /
  `Compression::from_codec_id`.
- **Bloom blocks**: the hash id leads (`1 | m | k | words`); only xxh3 filters
  exist in epoch 1.
- **MANIFEST** — magic `YOLODBMF`, version 1, the capability word as a fixed
  `u64` header field, and every optional field (WAL layout, nonce, edit cursor,
  partition/tier/object/max-entry-time, age stamps, range summaries) as a
  **flagged section** under a strict mask, replacing the positional tails and
  `ONDA*` tagged tails. A new per-CF **unified id** section (`CfManifest::unified_id`)
  is stored only when the id diverges from FNV-1a-64 of the name.
- **MANIFEST-EDITS** — a 32-byte `YOLODBED` header; record framing unchanged
  (under CRC32-C).
- **WAL segments** — every stripe file starts with a 32-byte `YOLODBWL` header
  (version, layout, generation), written and fsynced before the first frame.
  Replay refuses a segment without one as `UnsupportedFormat` at byte 0; a
  zero-length file or a torn header is an empty segment. `Wal::open` and
  `Wal::replay` take a `wal::SegmentId`.
- **CF config blob** — a `YOLODBCF` TLV (`tag | len | value`, ascending tags,
  defaults elided, durations in **nanoseconds**). Unknown tags are **preserved**
  on a decode→encode round trip (`ColumnFamilyConfig::unknown_config_tags`).
  Decoding is strict: `ColumnFamilyConfig::decode` now returns `Result`.
- **Unified CF ids** use the correct FNV-1a-64 offset basis
  (`14695981039346656037`, as wavesdb); 0.9 dropped a digit.
- Error taxonomy, everywhere: malformed bytes are `Corruption`; an unknown
  version, flag, capability bit, codec, kind or config enum value is
  `UnsupportedFormat`.

### Reading 0.9.x databases

- The 0.9 decoders are frozen, decode-only, in `ondadb::legacy_onda`, behind the
  new **default-on** cargo feature `legacy-onda`. `legacy_onda::open_read_only`
  opens a 0.9 directory read-only through the engine — tables, WAL tails
  (per-CF or unified), merge operands, range tombstones and the edit log — which
  is the source side of the planned automatic upgrade. Without the feature a 0.9
  directory is a hard `UnsupportedFormat` refusal.
- **Migration today:** open the 0.9 database with `legacy_onda::open_read_only`
  and copy every column family into a new database (iterate and write, or
  `Ingestion`). Automatic in-place upgrade on open (plan C §1.3) is the next step
  and is not in this release.

### Other

- `docs/format-registry.md` is now the yoloDB registry; `src/format.rs` pins every
  epoch-1 number with a `const` assertion and a golden test, and
  `tests/fixtures/epoch1/` is the frozen corpus. The 0.9 corpus moved to
  `tests/fixtures/legacy-onda/`, with three whole 0.9.1 database directories.
- The unified WAL rotation opens its next segment before draining writers, as
  the per-CF rotation already did, so the segment-header fsync does not extend
  the write gate.

### Ported from wavesdb (plan C step 1, §1.4)

#### Object-store checkpoints (wavesdb `CheckpointToObjectStore`)

- `DB::checkpoint_to_object_store(store, prefix, &ObjectCheckpointOptions)`
  uploads `<prefix>/cf-<name>/<id>.{klog,vlog}` then `<prefix>/MANIFEST`
  last (the commit marker); incremental via `parent`; `receipts` gives
  create-if-absent publication with per-object size + SHA-256 receipts.
  Returns an `ObjectCheckpoint { global_seq, next_file_id, tables,
  receipts }`. A read-only source is accepted (its WAL-only data is written
  as L0 tables in a scratch dir, per 0.9.1's snapshot rule).
- `restore_from_object_store(store, prefix, dir)` — MANIFEST fetched first,
  written last; `NotFound` when the prefix holds no checkpoint.
- `open_remote_checkpoint(store, prefix, opts)` — lazy, read-only mount:
  one MANIFEST GET, then range GETs; sizes seeded from the MANIFEST.
- Internal: `snapshot_to` is split into `plan_snapshot` + placement, so local
  and object checkpoints copy the identical file set. No on-disk format
  change: the MANIFEST bytes are the ones a local checkpoint writes.

#### Demote a part to the default tier (wavesdb `4fa392c`)

- `DB::move_part_to_default_tier(cf, partition)`; `move_part_to_tier` and
  `move_part_to_tier_observed` accept the reserved name `"ssd"` for the
  same thing (it was an "unknown tier" error before). Same crash-safe
  protocol as a move onto a tier; sources are now read through their own
  tier's `Storage`, so a part on S3 comes back via range GETs and its S3
  objects are deleted through the tier's backend, still behind
  `pause_deletions`. The policy mover does not demote (an `"ssd"` rule still
  only stops moves).
- `tests/s3_tier.rs`: prefixes are now unique per test (pid + counter), so
  parallel S3 tests no longer collide on macOS's microsecond clock.

#### Incremental-backup diff (wavesdb `SSTablesSince`)

- `DB::live_sstables()`, `DB::sstables_since(seq)` and
  `DB::sstables_diff(&prior)` (new module `checkpoint`, types
  `CheckpointTable`, `TableSetDiff`). `sstables_since` is wavesdb's
  `max_seq > seq` filter; it cannot see a compaction that rewrites only old
  data, so `sstables_diff` — by `(cf, id)` identity, reporting `added` and
  `removed` — is the one an incremental backup should use.

#### S3 parity with wavesdb v0.8.2–v0.8.6 (feature `s3`)

- `S3Config` gains `session_token`, `anonymous`, `profile` and `read_only`,
  and derives `Default`. Credential precedence: explicit keys (+ token) >
  anonymous > named profile (no fallback) > default chain (env → shared file
  → web-identity STS → instance metadata, resolved lazily).
  `S3Config::credential_source()` and `S3CredentialSource` expose the choice.
  **Source-compatibility note:** a struct literal of `S3Config` must now end
  in `..S3Config::default()`.
- `read_only` refuses writes locally with `OndaError::ReadOnly`; no bucket
  probe or create happens in any mode.
- Uploads send `x-amz-checksum-sha256` and check the store's echo.
- `Storage` gains default-implemented `put_object` (returns an `ObjectInfo`
  receipt), `create_if_absent` (`CreateOutcome`), `list_prefixes`
  (`PrefixPage`, paginated child-prefix listing) and `is_read_only`;
  `LocalStorage` and `S3Storage` implement them. A 404 now surfaces as
  `io::ErrorKind::NotFound` (`storage::is_not_found`). `S3Metrics` gains
  `lists`.
- Fixed the `--features s3` test build (a stale 4-tuple destructure of
  `Reader::get`).

#### Added

- **Shared read resources** (wavesdb `ReadResources`, `23648c8`,
  `f6b3def`): `ReadResources::new(ReadResourceOptions { block_cache_bytes,
  max_open_files, max_open_readers, max_reader_bytes })` builds one block
  cache, file-handle cache and reader cache that any number of **read-only**
  opens lease through `Options::read_resources`, so N immutable databases
  share one budget instead of N. Cache keys are namespaced per lease
  (`Options::read_cache_namespace`, default the directory's canonical path),
  so identical table ids in different databases never alias. A writable open
  with resources is `InvalidArgs`; `close()` refuses new leases and the caches
  are emptied when the last leased database closes. `stats()` reports block
  hits/misses/evictions/entries/bytes, reader-cache stats, open files,
  leases and closing. `CacheStats` gains `evictions`.
- **`get_into`** caller-buffer point reads on `DB`, `Txn` and
  `SnapshotHandle`: the value is appended to a caller-owned `Vec<u8>` (its
  length is returned; a miss leaves the buffer unchanged), so a reused buffer
  makes a hit allocation-free for the value — from the memtable and from a
  cached table block alike. Same candidate pass as `get`, so results are
  identical (checked by the randomized read oracle).
- **Standalone read snapshots** (wavesdb `SnapshotHandle`): `DB::snapshot()`
  returns a refcounted `SnapshotHandle` that pins its sequence exactly as a
  `Snapshot` transaction does, so compaction retains every version it can
  see until the last clone drops. Read through `SnapshotHandle::{get,
  multi_get, new_iterator, new_iterator_bounded}` or `DB::get_at` /
  `DB::new_iterator_at`; a handle from another database is `InvalidArgs`.
  The pin is registered atomically with reading the watermark.
- **`Options::default_isolation`** (wavesdb `d789912`): the isolation level
  `DB::begin` and `DB::begin_pessimistic` use. Defaults to `Snapshot`, so
  nothing changes unless it is set; not persisted. `Txn::isolation()` reports
  a transaction's level. The per-family `default_isolation_level` stays
  reserved (a transaction spans families, so no family's setting could decide).

#### Performance

- **User-space WAL write buffer** (wavesdb `WALWriteBufferSize`, plan C P6):
  `Options::wal_write_buffer_size` (bytes, default `0` = off, not persisted)
  coalesces whole frames per WAL stripe — per-CF and unified — into one
  `write` when the buffer fills, on every sync-interval tick (a flush-only
  thread runs under `SyncMode::None` too), and before every fsync
  (`sync_wal`, prepare and decision frames), rotation and close. `SyncMode::Full`
  ignores it. **Durability trade:** an acknowledged commit still in the
  buffer is lost on a *process* crash (unbuffered `None` loses it only on
  power loss); batch atomicity and frame bytes are unchanged, and a crash
  tearing a buffered write replays a prefix of whole batches. A failed
  buffered write poisons the database. `Wal::open_buffered`,
  `Wal::flush_buffer` and `Wal::write_calls` are the WAL-level API;
  `onda_bench -wal_buffer <bytes>`. Provisional (loaded machine, 5 runs,
  1 thread, 1 put per commit, 200k ops, `SyncMode::None`): 8.6k–32k ops/s
  unbuffered vs 166k–387k ops/s with 256 KiB.
- **MultiGet bounded parallel block reads** (wavesdb
  `MaxConcurrentBlockReads`, plan C P5): `Options::max_concurrent_block_reads`
  (default 8; 0/1 = sequential; not persisted) bounds, database-wide, the
  data-block reads batched gets keep in flight on **slow tiers** (storage with
  `supports_mmap() == false`: S3, custom, `without_mmap`). A table plan with
  at least four cold slow-tier blocks fetches them on scoped threads, a window
  of `4 × bound` blocks at a time; local tables and warm blocks keep the
  sequential path unchanged, as does `get`. Answers are identical; errors
  stay per key (a failed block fails exactly its keys — wavesdb's contract).
  `PerfContext` gains `multiget_parallel_reads` and `multiget_io_waits`
  (worker counters merge into the caller's scope). Provisional: a 152-key cold
  batch against a 2 ms-per-read tier took 404 ms at bound 1 and 60 ms at 8
  (best of 5, loaded machine).
- **Point reads stop early by table `max_seq`** (wavesdb `5ef39df`). `get`
  and `multi_get` skip a candidate table whose `max_seq` is at or below the
  version already in hand (point hit, tombstone, or covering range delete),
  so a memtable hit reads no table and an L0 hit reads nothing older.
  Results are unchanged (randomized oracle against the exhaustive walk); a
  corrupt table older than the answer is no longer probed, so it no longer
  fails the read.

## 0.9.1

**Shared-bug corrective release.** Five defects that wavesdb fixed after
ondaDB's last audit of it (`1a052a4..v0.8.6`) were checked against ondaDB;
four were present and are fixed here, each with a regression test that failed
first. No on-disk byte and no public signature changes: a 0.9.1 database is a
0.9.0 database, and rolling back is a binary swap.

### Correctness and durability

- **Compaction no longer loses data on a read error.** An SSTable iterator
  that hit a bad block or I/O error looked exhausted, so compaction merged
  the rest of the job without that input's remaining entries, installed the
  short output and retired the inputs (a reproduction kept 813 of 2,000
  keys). The job now fails before any output is installed and the inputs
  stay. The same pattern is fixed in user scans, where `Iterator::err()`
  now reports a child's failure instead of the walk ending early, and in
  merge-chain point reads. (wavesdb `eacc833`)
- **A compaction size cut never splits one key's versions** across two
  output tables. A point read probes only the first matching table in a
  level, so a snapshot read of an older version in the second table
  returned `NotFound`. An output may now exceed `target_file_size` by one
  key's version chain. (wavesdb `fc32015`)
- **Checkpoint and backup of a read-only database keep WAL-only data.** A
  read-only open replays the WAL into memtables but runs no flush, so the
  copy silently lacked those records (a deleted key came back). The
  snapshot now writes the sealed memtables as the destination's newest L0
  tables, touching only the destination.
- **A range delete over a key a `Serializable` transaction read is a
  conflict.** Validation checked point versions only; it now also asks the
  span index. This is still point-read validation, not phantom protection.
  (wavesdb `3591223`)

### Scheduling

- Periodic (age) compaction takes at most four jobs per pass, then
  re-queues its family, so a whole database aging at once no longer holds a
  worker for its entire backlog. Capacity work is unaffected. (wavesdb
  `887f8ad`)

## 0.9.0

**The wavesdb roadmap, phases 0-3.** Sixteen features, in twenty commits, across
the runtime, the record-kind system, the on-disk formats and the transaction
layer. The theme is
extensibility with an escape hatch: everything that changes a stored byte is
gated behind a manifest capability bit, nothing is enabled by default, and an
upgraded database keeps writing 0.8.2 bytes until an operator asks otherwise.

### Breaking changes

- `wal::Record`, `wal::RecordRef`, `memtable::Entry`, `sst::Writer::add` and
  `PointResult` carry a single `kind: u64` where they carried
  `(tombstone: bool, single_delete: bool)`. The pair could express
  `single_delete && !tombstone`, a state no writer produces; the kind cannot.
  `Record::tombstone()` and `single_delete()` remain as accessors.
- `wal` replay callbacks take a `ReplayRecord` enum rather than a `Record`, so a
  kind this binary does not implement can never be replayed as a put.
- `flags::DELTA_SEQ` is removed and `0x08` is reserved-unknown. Entry flags,
  footer flags and manifest tags now **fail closed**: bytes naming a feature
  this binary lacks are the new `OndaError::UnsupportedFormat` (code `-16`),
  distinct from `Corruption`.

### Format capabilities (all opt-in, all one-way)

`DB::enable_format_capabilities` persists a capability bit before the first byte
using it exists. Seven bits are defined, pinned for wavesdb compatibility and
never reused: `CAP_EXTENDED_RECORDS`, `CAP_MERGE_OPERANDS`, `CAP_RANGE_DELETES`,
`CAP_PREFIX_DELTA`, `CAP_MANIFEST_EDITS`, `CAP_PERIODIC_AGE`,
`CAP_TXN_DECISIONS`. Record kinds are pinned the same way (1 put, 2 delete,
3 single-delete, 4 merge, 5 range delete, 6-15 reserved, 16-31 transaction
control, 32-63 reserved, above 63 never assigned).

A database that enables nothing is byte-identical to 0.8.2 and remains readable
by 0.8.2. That is what makes every feature below individually rollback-safe.

### Transactions

- **Merge operators (1.1).** `DB::merge(cf, key, operand)` and
  `Txn::merge(..)` append a merge operand with no read of the current value and
  no conflict window; the operand is folded against everything below it at read
  time by the family's registered `MergeOperator`. Measured against the
  equivalent Get+Put at equal durability: 1.15x uncontended, **3.02x
  contended**, with zero retries against 1,231-2,278. Compaction folds operand
  suffixes at or below the oldest snapshot (`Options::enable_merge_folding`, a
  rollout switch), which keeps read cost flat where an unfolded 160k-operand
  chain costs 8.8x. A family with no operator runs exactly the pre-1.1 code on
  the point-read path; its scans measure 1.05x, reported honestly and left
  unexplained rather than guessed at.
- **Range tombstones (1.2).** `DB::delete_range(cf, start, end)` records the
  deletion of a half-open comparator interval **once**, at a single sequence,
  instead of one tombstone per key. Honored on every read path, in flush
  fragmentation, in compaction retention and in conflict detection. A commit
  holding a span takes the database-wide commit lock at every isolation level,
  so its span check and its installation are atomic; range commits are meant to
  be rare and bulk.
- **Delete-only excise (1.2).** Retires a whole SSTable by catalog edit, without
  reading a byte of it, when durable fragments prove every key it holds is
  already deleted. Runs as a pre-pass in the compaction picker and as
  `DB::excise_covered(cf)`; reports through `CfStats::excised_tables` /
  `excised_bytes`.
- **Prepared transactions (3.2).** `Txn::prepare(&id)` durably prepares a
  transaction; `DB::commit_prepared` / `abort_prepared` / `list_prepared`
  resolve it by a stable external id, across a process restart. A
  storage-engine **participant** only: no coordinator election, no consensus, no
  timeout decisions, and nothing is ever aborted automatically. A `prepare` that
  returns `Ok` cannot subsequently lose a conflict. Unified layout only.
- **Pessimistic locking (3.3).** `DB::begin_pessimistic` /
  `begin_pessimistic_with_isolation` begin a transaction that takes a **point
  lock** on every key it writes or reads through the new
  `Txn::get_for_update(cf, key)`, and waits for ownership instead of
  abort-retrying. Opt-in per transaction and off by default; an optimistic
  transaction is unchanged and never touches the lock table. Deadlock freedom is
  **wait-die** on a new per-database transaction id: an older requester waits, a
  younger one dies immediately with `Conflict`. Three consequences a caller has
  to plan for:
  - **`put` can block**, where it was an arena memcpy. Holding a foreign lock
    across one adds a deadlock edge wait-die does not cover — it orders
    transactions, not foreign locks.
  - **A conflict can surface from `put`, `merge` or `get_for_update`**, not only
    from `commit`. That is wait-die killing the younger transaction, and the
    caller retries exactly as it would after a commit conflict.
  - **At `Snapshot`, a lock grant refreshes the read snapshot.** Without it the
    feature buys nothing — the transaction waits for the hot key, is granted it,
    and still aborts on the write it waited for (measured: 85 commit conflicts
    over 2,000 contended rounds without the refresh, zero with it). The price is
    that pessimistic `Snapshot` is no longer snapshot-isolated across grants:
    read skew becomes possible. `Serializable` revalidates its read set before
    adopting the new snapshot, turning the same situation into an earlier abort.
    `RepeatableRead` never refreshes, so it keeps its snapshot and can still lose
    an update it read before the grant — locks add ordering, not isolation.
  Span locks are **not** in v1, so a range delete in a pessimistic transaction
  takes no lock at all. A `prepare` converts the transaction's locks into 3.2
  reservations and wakes every waiter with `Conflict`, because from that instant
  the durable reservation is the authority. **The throughput case is not made** —
  see "Measurement honesty" below and `bench-results/3.3/2026-08-31/`.

### Reads

- **MultiGet (0.4).** `DB::multi_get` / `Txn::multi_get` resolve N keys of one
  family in one snapshot-consistent pass, with one block fetch and one
  decompression per distinct block however many of the batch's keys land in it.
  Batches of 16-256 run 2.4x-3.4x faster than the equivalent sequential `get`s
  at equal cache state; batch size 1 is unchanged.
- **Tailing iterators (0.9).** `DB::new_tailing_iterator(cf)` is a forward-only
  cursor over an append-only keyspace that can be advanced past its own end
  rather than rebuilt per poll: 12.4 ns per idle poll against 427.7 ns to
  rebuild. Deliberately not a change feed — a refreshed tail observes only keys
  strictly greater than its last yielded key.
- **Per-level Bloom policy (0.1).** `bloom_fpr_per_level` and
  `optimize_filters_for_hits`, both defaulting to today's behaviour.
- **vlog value cache (0.5).** Per-family `max_cached_vlog_value_bytes`
  (default 0 = off) caches decoded vlog values. It carries a correctness fix
  that stays regardless: the block cache's key now names a `BlockDomain`, so a
  klog block and a vlog frame at the same offset of the same file can no longer
  alias.
- **PerfContext (0.10).** Caller-owned, per-operation read-path counters —
  bloom probes, memtable and SSTable probes, block-cache hits and misses, bytes
  decompressed, vlog reads, iterator seeks and steps — so a performance claim
  can be attributed to a mechanism instead of inferred from wall time. No
  DB-wide atomic is touched; the nil path is one thread-local load and a
  compare.

### Compaction and IO

- **Minimum-overlap-ratio picking (0.2).** Within a level, candidates are
  visited cheapest-first by overlap bytes per byte of candidate. On a fixture
  with varying key density, compaction bytes per ingested byte fall from 2.768
  to 2.452 — 4.7x the run-to-run noise band. On a uniform fixture the effect is
  0.2%, which is recorded rather than hidden: once a tree settles there is
  nothing to choose between candidates.
- **Parallel subcompactions (0.8).** One large bounded compaction may partition
  its key range into half-open spans merged concurrently into **one** atomic
  install. Off by default (`max_subcompactions = 1`).
- **Periodic compaction (0.3).** `periodic_compaction_interval` revisits tables
  after a configured age so an idle family reclaims expired TTL entries,
  tombstones and shadowed versions. Backed by a new durable
  `SstMeta::last_compaction_time` under `CAP_PERIODIC_AGE` — deliberately not
  `max_entry_time`, which the part mover's `min_age` gate needs to mean
  something else.
- **Background IO classes and rate limiter (0.6).** Bounds background bandwidth
  so flush and compaction cannot monopolise the device, with a work-conserving
  token bucket and an injectable clock (so every pacing assertion is exact and
  instant rather than spending the wall time it simulates). Obsolete-file
  deletion is a paced worker of its own. All rates default to 0 = off.

### Formats

- **Prefix-delta data blocks (2.1).** Each data-block user key is stored as the
  bytes it does not share with its predecessor, behind `FOOTER_PREFIX_DELTA` and
  a per-family option. A space-for-CPU trade, opt-in.
- **Manifest edit log (2.2).** `MANIFEST-EDITS` replaces O(catalog) full
  manifest rewrites with a periodic snapshot plus an append-only log of
  numbered, CRC-framed catalog edits. **Every catalog mutation now goes through
  `DbInner::catalog_txn`**, which makes the edit durable before it publishes and
  hands the publish closure a token — publishing outside a transaction no longer
  compiles. Two prerequisites landed with it and matter on their own:
  `Manifest::save`'s post-rename directory fsync propagates its error instead of
  discarding it, and `close()` no longer discards its final persist result.
- **Strict legacy decoding (1.0).** A frozen corpus of 0.8.2-generated fixtures
  (`tests/fixtures/phase1/`) is committed and pinned *before* any strictness
  landed, and every fail-closed check above is proven against it. Per-decoder
  fuzz corpora seeded from those fixtures found two hardening fixes: the entry
  decoder's offset arithmetic is now checked, and the manifest's count fields no
  longer pre-allocate unbounded capacity.

### Cross-feature rules

Two rules exist only where features meet, and belong to no single feature's
author. They are implemented once each and pinned in `tests/composition.rs`:

- A range delete is, for one key, a **deleted base at its sequence**. A merge
  chain's operands above the span survive and fold onto nothing; operands at or
  below it, and the base under them, are masked. This holds identically for
  point reads, batch reads, both scan directions and compaction.
- A prepare frame carries a **merge operand** but never a range delete. An
  operand is one key and one value, which is what the replay path has a shape
  for; two keys and no value is not, and `Txn::prepare` refuses it at the API
  rather than letting replay discover it.
- A pessimistic transaction's **point lock becomes a durable reservation at
  `prepare`**, on the same `(cf_id, key)` pair and inside the same `commit_mu`
  acquisition, and every waiter is woken with `Conflict` rather than granted a
  lock whose commit is guaranteed to fail. After a crash only the reservation
  exists: a new transaction takes the lock and is refused at *commit* until a
  coordinator resolves the prepare, so nobody hangs on a crashed owner.
- A pessimistic **merge** takes its key's lock and a pessimistic **range
  delete** takes none. A merge is a write for conflict purposes in this engine,
  so an unlocked one would abort at commit exactly as an unlocked put would; a
  range delete needs a lock on the *interval*, and a point lock on its start
  bound would protect one key while reading as if it protected the span.

### Measurement honesty

This machine is thermally noisy (±15-20% run-to-run, worse under sustained
load), and several of these features were measured while sibling builds were
running. Where a benchmark did not decisively meet its acceptance criterion the
feature ships **default-off** and its `bench-results/<feature>/<date>/summary.md`
records what a valid re-measurement would require, rather than reporting a
number nobody believes. That applies to 0.1 (miss-heavy), 0.5, 0.6, 0.8, 0.9's
streaming phase and 2.2's write-time ratio.

**3.3 is the one that missed its throughput criterion outright, and it ships
anyway** because it is opt-in per transaction and its *correctness* criterion is
met unconditionally: zero commit conflicts in every pessimistic run, against
thousands in every optimistic one. On retry-corrected throughput a tight
begin-acquire-commit loop on one key is **0.61x** at two threads and **0.17x** at
eight; p99 is 3.2-3.8x better at two threads and in a realistic transaction
shape, and 3.0x worse in the eight-thread tight loop. The cause is understood and
written down: wait-die kills the *younger* requester, and in a tight loop the
requester is always the younger party, so the "older waits" arm almost never runs
and the mode degenerates into spin-abort-retry.
`bench-results/3.3/2026-08-31/README.md` has the tables and names the two design
alternatives (retaining a restarted transaction's timestamp; wound-wait) that
would be expected to change it.

The S3-gated acceptance arms of 0.4 and 0.5 were **not run** — no
`ONDADB_S3_ENDPOINT` was available — and are not claimed.

## 0.8.2

**August 2026 code-review corrective release.** This release closes the five
highest-severity findings, hardens recovery and transaction edge cases, and
incorporates the published 0.8.1 per-column-family data-block-size work while
preserving the `v0.8.1` release ancestry.

### Correctness and durability

- Backup, checkpoint, and column-family clone now resolve tiered tables and
  create self-contained, durable default-tier copies. Their manifests no
  longer point at source-tier objects that were not copied.
- Classic and by-reference part attach validate input tables against each
  other as well as live bottom-level tables. Mutually overlapping input is
  routed to overlap-tolerant L0 instead of violating leveled-read ordering.
- Attach finishes and syncs destination storage before its manifest flip.
- Newly created WAL stripes fsync their parent directory, closing the crash
  window in which an acknowledged full-sync commit could lose its directory
  entry.
- Transactions spanning more than one column family are now rejected in the
  per-CF WAL layout. Use `unified_memtable = true` when atomic cross-CF commits
  are required; unified mode records the entire commit in one WAL frame.

### Recovery, transactions, and operations

- Startup removes unreferenced SST output left in the default tier by a crash;
  the documented named-tier/S3 orphan-GC gaps remain.
- Manifest levels above 64 are rejected before allocation.
- Serializable savepoint rollback now discards later read dependencies, and
  transaction reset preserves the caller's publication floor.
- Thread-local commit floors use stable database instance ids rather than
  reusable allocation addresses.
- Exact transaction-overlay ties have an explicit, tested overlay-first rule.
- `flush_memtable(cf)` waits only for that CF's flushes, and background
  compaction honors `num_compaction_threads`.
- `CfStats` now reports a compaction failure count and latest error instead of
  silently discarding failures.

### Performance and configuration

- Unified bytewise iteration uses lazy prefix-bounded cursors instead of
  cloning the entire shared memtable. The review probe's median iterator setup
  fell from 51,439 ns to 1,864 ns (27.6x); custom comparators retain the
  materialized re-sort required by their ordering.
- From 0.8.1, `ColumnFamilyConfig::data_block_size` controls flush, ingestion,
  and compaction output per family. It defaults to the historical 4 KiB and is
  persisted in the optional `ONDABLK1` config tail; existing SSTables remain
  self-describing and require no migration.
- Inactive public tuning fields are now documented as reserved/ignored rather
  than implying mechanisms that do not exist. `single_delete` documentation
  states its current conservative tombstone behavior.

The complete finding-by-finding disposition, including measured no-change and
deferred architectural items, is in
[`docs/code-review-2026-08-resolution.md`](docs/code-review-2026-08-resolution.md).

## 0.8.1

**Per-family data block size (SPADINO-A10).** Added
`ColumnFamilyConfig::data_block_size`, defaulting to the historical 4 KiB, as a
write-side policy used by flush, ingestion, and compaction. The optional
`ONDABLK1` config tail preserves non-default values without changing legacy
default encodings.

## 0.8.0

**Sustained writes.** Compaction jobs are bounded, writers pace against
compaction debt, and `close()` no longer pays off a backlog it never reported.
No format migration: a 0.7.8 database opens unchanged.

### What was wrong

Compaction took the **whole** source level plus every target-level file it
overlapped. Under random keys an L0 file spans nearly the entire keyspace, so
each L0→L1 push-down rewrote all of L1, and each L1→L2 all of L2 — the work in
one job grew with the dataset.

Nothing in the write path knew. `l0_queue_stall_threshold` gates on sealed
memtables awaiting **flush**, and flush was never the bottleneck: isolating the
phases showed the flush queue draining in ~130 ms whether 5M or 20M records had
been written. So ingest ran at memtable speed however far compaction had fallen
behind, and the debt surfaced at close.

Measured on a 24-core M2 Ultra (16 B keys, 100 B values, 8 threads), the
reported Put rate sat flat at ~4.6M ops/s from 5M through 20M records while the
close that followed it went **2.5 s → 10.8 s → 35 s**. The rate an application
measured was one the engine could not sustain, and the gap widened with the
dataset.

### What changed

- **Bounded jobs.** A compaction takes one file from the source level plus only
  the target files its range overlaps — about
  `target_file_size * (1 + level_size_ratio)` regardless of level size. A
  per-level cursor sweeps the keyspace so successive jobs advance instead of
  re-picking the head. L0 takes the **oldest** `l1_file_count_trigger` files,
  which is safe because `levels[0]` is newest-first and reads walk it in that
  order, so a version left in a newer L0 file still shadows the copy pushed
  down.
- **`target_file_size` and `l1_base_bytes`** are new `ColumnFamilyConfig`
  fields, held apart from `write_buffer_size`. They had all been the same value,
  so L1 held exactly **one** file whose range covered everything beneath it:
  partial compaction was not merely unimplemented, the geometry made it
  impossible. Defaults 16 MiB and 256 MiB, giving ~16 files in L1.
- **Write pacing.** `soft_pending_compaction_bytes` (default 2 GiB) delays each
  commit in proportion to the excess; `hard_pending_compaction_bytes` (8 GiB)
  blocks until a compaction completes. Debt is a gauge cached on the column
  family and refreshed by flush and compaction, so the write path reads one
  atomic rather than walking every level. Readable via
  `CfStats::compaction_debt`.
- **Range locks (`range_lock.rs`)** replace `cf.compact_mu` as the exclusion
  mechanism. Compaction `try_acquire`s and picks other work when a range is
  held; `detach_part`, `attach_part`, `attach_part_by_ref` and `relocate_part`
  `acquire_blocking` over their span. **This part is load-bearing for
  correctness:** those four relied on `compact_mu` so the bottom level could not
  be rewritten between their snapshot and their removal, and when compaction
  stopped taking that mutex they had to name a range or lose the guarantee — the
  failure being a tier move and a compaction rewriting the same bottom tables,
  one unlinking the other's inputs. `compact_mu` now guards only whole-CF
  operations (the `DB::compact` sweep, FIFO eviction), which additionally take
  the whole keyspace. Lock order is always `compact_mu` → range lock.
- **The background part mover** blocks only its own partition instead of the
  whole column family, and runs one pass at a time now that several compaction
  workers exist. A foreign mount (`attach_part_by_ref`) no longer blocks its
  entire level — only the ranges that actually overlap it.
- **`finish_compactions_on_close` works.** It was declared in `Options` and read
  by nothing: the compaction worker tested its stop flag only when its queue ran
  dry, so close drained the backlog whatever the setting said. Default stays
  `false`; leftover debt is legal LSM state the next open resumes from.

### Results

Same machine and workload. "Settled" counts the close that follows the ingest,
which is the rate at which records actually become durable SSTables:

| Records | 0.7.8 close | 0.8.0 close | 0.7.8 settled | 0.8.0 settled | |
|---|---|---|---|---|---|
| 5M  | 2 702 ms  | 1 086 ms | 1.36M ops/s | 2.15M ops/s | 1.6x |
| 10M | 10 916 ms | 428 ms   | 0.77M ops/s | 3.45M ops/s | 4.5x |
| 20M | 36 666 ms | 1 195 ms | 0.49M ops/s | 3.45M ops/s | 7.0x |

The ratio is not the point — the **shape** is. 0.7.8 halved its settled rate
each time the data doubled (1.36M → 0.77M → 0.49M); 0.8.0 holds it flat past
10M. The gain therefore keeps growing with dataset size, which is what "the
work in one job grew with the dataset" costs you.

Peak Put barely moves (~4.6M → ~4.0-4.4M ops/s): the ingest path was never the
problem, and pacing only engages once debt is real.

### Reads, and the trade-off this makes

On a settled tree the new geometry costs nothing. Cold `Get` over 5M records is
**1.41M ops/s against 0.7.8's 1.40M** (3 runs each), and scans are equal or
better — smaller SSTables did not hurt point reads, because levels below L0 are
disjoint and binary-searched, so one file is probed per level regardless of how
many it holds.

What *does* change is the state a database is in immediately after opening. A
close that abandons compaction leaves L0 deeper, and L0 files overlap, so a
point read probes **every** one of them — read cost is linear in L0 depth.
Reading straight after such a close, with compaction still catching up:

| | L0 files when reads begin | cold Get |
|---|---|---|
| default (close abandons compaction) | 6 | ~0.78M ops/s |
| `finish_compactions_on_close = true` | 2 | ~1.41M ops/s |

This is the deferred work becoming visible somewhere, and the choice is which
somewhere. If your workload loads a dataset, closes, reopens and immediately
serves point reads, set `finish_compactions_on_close = true` — that restores
0.7.8's behaviour exactly, at the cost of a ~3.2 s close (0.7.8: 2.5 s; the
difference is the extra write amplification partial merges incur, since
overlapping target files are re-merged more often). For a long-running database
compaction keeps up and the distinction does not arise.

An earlier attempt to fix this by ranking L0 above equally-overfull deeper
levels in the picker was **reverted**: it changed nothing, because the L0 depth
in question is what `close()` left behind and reads begin before any compaction
has had a chance to run. Measured identical at 6 files with and without it.

### Compatibility

The new geometry rides in a tagged manifest tail (`ONDACMP1`) emitted only when
it differs from the defaults, so a config left alone still encodes byte-for-byte
as earlier releases wrote it and a pre-0.8.0 manifest decodes to the new
defaults. Verified end to end: a database written by the released 0.7.8 binary
opens under 0.8.0 with all records readable, and recompacting it under the new
picker loses nothing. Existing SSTables keep their old sizes until compaction
re-cuts them.

310 tests green, including `tests/sustained_writes.rs`.

## 0.7.8

**Attach-by-reference over shared tiers (A2)**, plus a target-conditional S3 TLS
backend. Non-shared tiers are byte-for-byte unchanged from 0.7.7 in every
persisted structure and every path; a database that never declares a shared tier
is indistinguishable from before.

- **Shared tiers and zero-copy attach — `TierDef::shared()` +
  `DB::attach_part_by_ref`.** This expresses a one-writer / many-disposable-reader
  topology directly: one database seals immutable parts onto an object store and N
  query databases mount them without copying a byte. Three things made it
  impossible before, each addressed here:

  - **Object naming no longer collides.** A move onto a *shared* tier names each
    file `cf-{cf}/{instance:016x}-{id}` relative to the tier root, where
    `instance` is a per-database nonce minted once and persisted. Two databases
    pointed at one root can no longer overwrite each other's objects (before, tier
    paths derived from the per-database file id, so both eventually moved *their*
    id 7). Moves onto non-shared tiers keep the legacy id-derived path exactly.
  - **`attach_part_by_ref` copies nothing.** It registers an exported
    `PartManifest`'s tables in the catalog under fresh local ids that resolve to
    the shared objects. Each table's footer/index/bloom is opened and CRC-verified
    through the tier backend (the same validation `Reader::open` always does), and
    the manifest's `num_entries`/`max_seq` claims are cross-checked against the
    footer — a mismatch rejects the whole part, nothing installed. The target
    adopts the part's sequence lineage via the recovery path's `observe_seq`, so a
    fresh (empty) database can attach foreign-lineage parts that `attach_part`
    would refuse.
  - **Shared tiers are delete-free.** The engine never deletes an object on a
    shared tier: the mover will not move a part *off* one, `detach`/`freeze` refuse
    shared publications, the startup orphan sweep skips shared roots, and
    compaction's obsolete-input deletion resolves default-tier paths only.
    Reclaiming shared objects is the coordinating layer's job (as with the
    "no internal object CAS" rule) — without this, one sharer's hygiene would be
    another's data loss.

  **A sharer never compacts mounted parts.** A2's read-only-sharer safety argument
  held for the mover but not for local compaction: the LSM could pick mounted
  (foreign-nonce) tables as compaction triggers or inputs and silently rebuild
  shared bytes as local tables. `is_foreign_mount` now excludes them from both,
  and a push-down that would overlap a mounted table in the target level aborts.
  Pinned by `a_sharer_never_compacts_mounted_parts`.

  **Persistence.** Two new tagged manifest-tail sections following the `ONDAWAL1`
  precedent: `ONDAOBJ1` (per-CF table→object names, emitted only when some table
  carries an object) and `ONDAINS1` (the 8-byte instance nonce, emitted once
  minted). `PartTable.object` carries the name through `export_part` but is
  excluded from the part digest — a rename is not a rewrite. A manifest carrying
  neither tag is byte-identical to a pre-A2 encoding; a pre-A2 binary refuses a
  tagged manifest fail-stop (checksummed unknown tail → corruption error), the
  same downgrade posture as the 0.3.0 tier tail. New public surface:
  `TierDef::shared`, `DB::attach_part_by_ref`, `SstMeta::object`,
  `PartTable::object`. Full guide in `docs/parts-and-tiers.md`; the design
  rationale is `SPADINO-A2.md`. Mutable sharing remains documented as unsupported
  — the safety argument is that a shared part is immutable and single-writer.

- **The S3 TLS backend is target-conditional (feature `s3`).** macOS links
  native-tls (Security.framework): rustls' native-roots loader panics
  (`InvalidCertificate(BadEncoding)`) on a keychain holding an unparseable
  trust-store cert — and it hit live on a macOS host talking *plain HTTP* to a
  MinIO endpoint, where TLS should not even engage. Every other target — including
  musl, which the static from-scratch CI images and spada's builds link against —
  returns to tokio-rustls-tls. The `s3` feature activates whichever backend
  matches the target; no API or format change.

## 0.7.7

**Large values were re-checksummed on every read, and a 4 GiB one was written
corrupt.** Two vlog defects, one performance and one silent-corruption.

- **A vlog frame's CRC is now verified once per open reader, not once per
  read.** A klog data block has had CRC-once semantics for a while; a vlog frame
  never did, so every read of a large value re-checksummed the entire stored
  payload. That checksum is most of the read: CRC32-C runs at ~6.3 GB/s on the
  baseline machine, so a 5 MB value spent ~800 µs per read re-verifying bytes it
  had already verified. Measured repeat-read throughput (median of 3, same
  build, `tests/vlog_read_bench.rs`): with mmap reads **6.97 → 45.1 GB/s** at
  400 KiB and **6.52 → 47.3 GB/s** at 5 MiB; on the buffered `pread` path
  **4.85 → 9.29** and **3.87 → 6.47 GB/s**. Unlike the klog bitmap this covers
  both paths.

  Semantics are unchanged in the direction that matters: the first read of every
  frame verifies, a frame that fails verification is never marked, and re-opening
  the table re-verifies. The mark is a bounded direct-mapped set of frame offsets
  (8 KiB, allocated on a reader's first vlog read), not a bitmap — a vlog frame
  has no dense index to number bits by, and a collision costs a re-verification
  rather than a skipped one.

- **Vlog values are still not cached in the block cache**, now on purpose and on
  evidence: caching them measured a **32% regression** on the mmap path (the
  value gets memcpy'd out of an `Arc` instead of straight from the mapping) and a
  3.1× win on the buffered path that is paid for by evicting ~256 klog blocks per
  MiB of value to avoid re-reading what the OS page cache already holds. The full
  table and the one case that would change the answer (remote `s3` tiers, where a
  miss is an HTTP GET) are in `docs/performance.md`.

- **A value whose stored form reached 4 GiB was silently truncated.** The frame
  header stores the payload length in a `u32` and the writer cast to it, so a
  ≥4 GiB stored payload wrapped to a small length: the CRC then covered bytes no
  reader would read, and the following frame's offset pointed inside this one's
  payload — a table corrupt from the moment it was written, with no error
  anywhere. `Writer` now refuses such a value with `OndaError::TooLarge`. The
  limit is on the stored (post-compression) bytes, so a larger value that
  compresses under the limit is still accepted.

- **The v2 frame layout is now documented** (`docs/formats.md` described only
  v1), including the stored-length invariant. The reader enforces it: a v2 frame
  whose header claims more stored bytes than the value's logical length is
  rejected as corrupt instead of driving a blind allocation of up to 4 GiB.

## 0.7.6

**A read-back of just-ingested rows could come up short — read-your-own-ingest.**
This completes the fix started in 0.7.4.

- **A fixed snapshot begun right after `Ingestion::finish` could pin below the
  ingestion's sequence.** The 0.7.4 fix made a fixed-snapshot begin wait for the
  thread's *own* commit floor, but only `Txn::commit` recorded that floor; the
  ingestion path (reserve + publish at `start_ingestion`) never did. So a fixed
  snapshot opened immediately after an ingestion finished could pin *below* the
  ingestion's seq whenever another thread's earlier-reserved commit was holding
  the gap-free watermark down — and a read of the just-ingested rows through that
  snapshot came back short.

  The ingestion path now notes its thread's commit floor the same way
  `Txn::commit` does. This was a live-only, intermittent failure: spada's seal
  verification read it as segment corruption ("materialized key count differs from
  the prepared lane") for two days, surviving three fixes on its own side. The
  stress test reproduces 9–15% short reads per run before the fix and zero after.

## 0.7.5

**The open-reader cache is now bounded in bytes, and bounded by default.**

- **BEHAVIOUR CHANGE — `Options::max_open_reader_bytes` defaults to 1 GiB.**
  There was no byte bound before; a store that relied on that now has a
  ceiling. Set it to `0` to restore the old unbounded-in-bytes behaviour. The
  count bound (`max_open_readers`, default 512) is unchanged in both value and
  meaning.

- **Why a count was not enough.** `max_open_readers` bounds memory only if
  readers cost the same, and they do not: per-reader resident cost (block index
  plus bloom filter) tracks the table's key count. Measured on spada's staging
  cluster on 2026-08-09, it varied about **30x with segment size alone** — under
  a megabyte per reader at 256-document segments, about 20 MB at 8192-document
  segments. spada's configured count of 4096 did not change; the memory it
  implied went from a few hundred megabytes to roughly 10 GiB, and nodes OOMed
  repeatedly. Nothing in a count could have expressed that, because the count
  was never the constraint. (spada `decisions.md` S-161; the original count
  bound is S-122/S-123.)

- **Both bounds are enforced.** The cache evicts least-recently-used readers
  until the open count is within `max_open_readers` **and** their resident bytes
  are within `max_open_reader_bytes`. Either bound set to `0`/unlimited leaves
  the other in force. Victims are chosen by recency, not by size: evicting the
  largest reader would free the budget fastest and then re-open the most
  expensive table.

- **The budget bounds what the cache pins, not the process peak.** Eviction
  drops the cache's `Arc`; a caller mid-read keeps its own, so the reader lives
  until that caller is done. Peak is `max_open_reader_bytes` plus the bytes of
  concurrent in-flight readers — the same honest caveat the count bound has
  always carried, now stated in bytes.

- **A budget below one reader's cost keeps one reader**, rather than emptying
  the cache and re-opening on every access. `DB::table_cache_bytes` will then
  read above the budget, which is where an operator should see it.

- New API, all additive: `Options::max_open_reader_bytes`,
  `DB::table_cache_bytes() -> (resident, budget)`,
  `DB::set_max_open_reader_bytes`, `TableCache::with_byte_budget`,
  `TableCache::byte_stats`, `TableCache::set_max_bytes`, and
  `table_cache::DEFAULT_MAX_OPEN_READER_BYTES`. `TableCache::new` keeps its
  signature and stays byte-unbounded.

## 0.7.4

- **A fixed snapshot never predates its own thread's last commit.**

## 0.7.3

**Committed writes could be silently lost.** This fixes the durability bug
recorded as a known issue in 0.7.1, and it was never a WAL bug.

- **A compaction could discard a table a concurrent flush had just installed.**
  The level-set swap was a non-atomic read-modify-write: compaction read the
  levels under a read lock, built a replacement, dropped the lock, and then
  overwrote the whole vector. A flush finishing in that window inserted its new
  L0 table and had it thrown away by the overwrite.

  Nothing deleted the orphaned file — it had never been a compaction input — so
  the bytes stayed on disk while the level set, and the manifest persisted
  immediately afterwards, no longer referenced them. Worse, the flush that
  produced that table had already reclaimed its WAL, correctly, against an
  earlier manifest that *did* contain it. So the data existed in exactly one
  place that nothing would ever read.

  The rebuild now happens inside a single exclusive lock (`update_levels`), and
  the target level is re-derived from live state rather than from a
  pre-compaction snapshot, so a table arriving mid-compaction survives instead
  of being overwritten. A debug assertion makes the invariant loud: a table may
  leave the level set only if it was one of that compaction's inputs.

  **Measured:** `writes_progress_after_crash_recovery_with_wal_backlog` failed
  2 runs in 8 before and 0 in 16 after. The signature was always the same —
  one contiguous run of ~122 keys, a single memtable generation, gone. Whole
  generations vanishing rather than scattered keys is what pointed at a table,
  not at frames.

  Found by instrumenting the swap: it reported dropping non-input table 10, and
  the first missing key was then located inside `10.klog`, on disk, absent from
  the manifest.

## 0.7.2

Read-path concurrency and cost. Everything in 0.7.1 plus the work that had been
sitting on the `parts-and-tiers` branch — which is what the consumer actually
compiled against, so this release makes the tag match reality.

- **The table cache stops serializing reads.** It was one process-global
  `Mutex`, and its *hit* path wrote a recency tick — so a cache hit took an
  exclusive lock, shared by every column family. Measured on an 8-core box,
  point-read throughput through the engine was **flat from 1 to 8 threads**
  with SSTables present, and scaled 4.6x with none: the difference was this
  lock. It is now sixteen shards with second-chance (CLOCK) replacement; a hit
  takes a shard **read** lock and one relaxed bit store. The `max_open` bound
  stays **global and exact** via a shared open count with rotating cross-shard
  eviction — it is a memory contract (0.7.0), so sharding it would have been a
  regression dressed as a speedup.
- **TTL checks use `CLOCK_REALTIME_COARSE`.** A precise `SystemTime::now` per
  get measured ~2 % of warm reads.
- **`uvarint` gains 3- and 4-byte fast paths.** Real sequence numbers made
  every block decode fall into the general byte loop.
- **`new_iterator` binary-searches sorted levels** for the overlapping run
  instead of testing every table in the column family — O(log n + overlap) per
  level.

No format change and no API change.

## 0.7.1

One bug fix, and it is a large one: **bloom filters were effectively off for the
whole steady-state database.**

- **A filter is sized from the keys actually written, not from a caller's
  guess.** `Bloom::new` allocates a fixed bit array that cannot grow, and the
  SSTable writer sized it from `WriterOptions::expected_entries` *before seeing
  a single key* — so every caller had to guess, and both guessed wrong:
  compaction passed a hardcoded `4096` for every table it produced, and bulk
  ingestion divided a byte target by an assumed 64-byte entry.

  A compacted table holding a million entries therefore carried a filter
  designed for four thousand. Every bit ends up set, so `may_contain` answers
  "maybe" to everything — strictly worse than no filter at all, because the
  bytes are still built, written, loaded into memory and hashed against on
  every lookup, and nothing is ever skipped. Because a leveled LSM keeps nearly
  all of its data in compacted levels, this meant blooms did nothing for the
  steady state. Measured on a real consumer store with 33.5M entries in one
  column family: **400,000 point reads for keys that provably did not exist
  produced zero bloom skips.**

  The fix removes the guess rather than improving it. The writer buffers one
  hash per key and builds the filter in `finish()` from the count it actually
  wrote, so no caller can size a filter wrongly; `expected_entries` survives
  only as a capacity hint for that buffer. Identical data through
  ingest / put / compact now measures **99.0 % skips on all three paths**
  (was 100 % / 99 % / **0 %**), and the filters get *smaller*: 9.59 bits/key
  against the 9.6 the 0.01 target implies, where the old L0 tables were
  over-provisioned at 17.4.

  **Cost:** 8 bytes per entry buffered until `finish`, bounded by the writer's
  roll target rather than by the store — at a 64 MiB target and the smallest
  entries a real consumer writes (~21 bytes), a full table is ~3.2M entries and
  the buffer peaks near 26 MB, for one writer at a time.

  No format change: a filter written by 0.7.0 is still readable, it was simply
  useless. Existing tables get correct filters as compaction rewrites them.
  Pinned by `tests/bloom_survives_compaction.rs`.

**Known issue, pre-existing and not fixed here.**
`writes_progress_after_crash_recovery_with_wal_backlog` fails intermittently:
after a crash with a WAL backlog, a reopened database counted **8,878 of 9,000**
keys — 122 writes lost across the crash boundary. Measured at **2 failures in 8
runs, and the same 2 in 8 at v0.7.0**, so this release neither introduces nor
worsens it; it was already present in 0.7.0 as shipped. It is recorded here
because an intermittent *durability* failure is the wrong thing to leave as an
unexplained red test, and because the rate means roughly one CI run in four will
show it. Not diagnosed: whether the loss is in WAL replay or in the flush of
recovered generations.

**Diagnosed and fixed in 0.7.3** — and it was neither of those. The WAL was
reclaimed correctly; a concurrent compaction discarded the flushed table.

## 0.7.0

Memory and compaction. Two behaviour changes and one changed default, all
driven by a consumer that reached **10 GB resident and was killed by the kernel
opening a 48 GiB store**, before serving a single request.

- **Bounded open readers (`Options::max_open_readers`, default 512).** Opening
  an SSTable eagerly loads its whole block index and bloom filter, and both stay
  resident while the reader does. `ColumnFamily::open` opened *every* table named
  in the manifest and never closed one, so resident memory was proportional to
  **total stored bytes** rather than to the working set — paid at startup,
  whether or not a table was ever read.

  This is RocksDB's mechanism, which this engine had omitted while copying its
  eager per-table load: *"If `cache_index_and_filter_blocks` is false (which is
  default), the number of index/filter blocks is controlled by option
  `max_open_files`."* `SstHandle` no longer owns a reader; it holds the
  information to re-open one and asks a shared least-recently-used `TableCache`.

  Closing is safe because a reader is a pure, re-derivable view of an immutable
  file: it costs a re-open and cannot change an answer. **Memory is bounded by
  `max_open` plus concurrent in-flight readers**, not by `max_open` alone — an
  in-flight caller holds an `Arc`, so eviction drops only the cache's reference.
  Measured on the store above: 6.6 GB at ten seconds into startup became 784 MB,
  and flat rather than climbing.

  **`Iterator` behaviour change:** `new_iterator` cannot return a `Result`, so a
  reader that fails to open now yields an iterator that is invalid and carries
  the error on `err()`. **Callers that walk `while it.valid()` without checking
  `err()` will read a failed scan as an empty one.** Omitting the table instead
  would return a short answer that looks complete, which is worse; but the
  contract is now load-bearing where it previously could not fire.

- **Bulk ingest arms compaction.** `Ingestion::finish` installed its L0 tables,
  persisted the manifest, and omitted the `compact_tx` send that a memtable
  flush performs — so a bulk-loaded store grew one permanent L0 table per
  ingestion and nothing ever asked the compactor to look. A real store reached
  **14,051 L0 tables for a million documents**; rebuilt with this fix, the same
  corpus shape produced **58**. Verified by removal: with the send deleted, the
  test reports "after 12 bulk-ingested tables, L0 still holds 12 files".

- **`DEFAULT_BLOCK_SIZE` 4 KiB → 16 KiB.** Every data block costs one resident
  index entry. Measured afterwards at ~3 % of the resident total (the bloom
  filter dominates an un-compacted store by 32×), so this is a small win
  honestly labelled: it scales the index down by a constant and bounds nothing.
  Existing files are unaffected — block size is a property of the file that
  wrote it. Callers doing many small random point reads should set
  `SstOptions::block_size` down.

- **New accounting:** `Reader::resident_bytes`/`resident_breakdown`,
  `ColumnFamily::resident_reader_bytes`, `ColumnFamily::l0_file_count`,
  `DB::table_cache_stats`, `DB::reader_memory`, `DB::set_max_open_readers`.
  Added *before* optimising, because this memory grew to dominate while being
  invisible, and two successive diagnoses were wrong without it.

**Not in this release:** the vlog value cache. It is a latency fix that *adds*
memory, and it should not land before resident memory is settled.


## 0.6.0

Two additive changes, no API or format break. Minor bump for the new public
surface: `DB::export_part` (plus the `PartManifest`/`PartTable` types) and
unified-mode's `unified_memtable_stall_threshold` and `migrate_to_unified`.

- **Part export (`DB::export_part`)** — new method returning a `PartManifest`:
  a part's tables (key ranges, sequence bounds, sizes, tier) plus a SHA-256
  content digest, computed by reading the bytes. Where `freeze_part` produces
  an openable *database directory*, this produces a *description* a caller can
  send elsewhere. Motivation: inside a database a part is identified by its
  tables' file ids, which are local counters, so a consumer coordinating parts
  across machines had to maintain a content→part mapping itself, outside the
  engine that owns the facts.

  The digest is a **physical** identity, and the documentation says so
  prominently because the stronger reading is the natural one to assume: it is
  independent of file ids, paths, tier and database instance (two databases
  that did the same writes agree), but *not* of write history, since SSTable
  entries carry database-global sequence numbers. It answers "does the peer
  already have exactly these bytes?" and deliberately does not answer "did two
  replicas independently rebuild the same data?" — that needs a hash over
  logical content, which only the consumer can compute. Both properties have
  tests, including one asserting the limitation, so a change that made the
  digest logical cannot land without updating the docs with it.

  A remote-tier part is hashed through its own tier backend rather than being
  pulled local; a tier move leaves the digest unchanged. Deletions are paused
  for the duration, the same discipline as `checkpoint`/`freeze_part`.

- **Unified WAL durability hardening** — unified memtable mode now has bounded
  immutable backpressure (`unified_memtable_stall_threshold`, default 6),
  persists its WAL layout in the manifest, rejects accidental
  per-CF/unified reopen mismatches, and supports an explicit crash-safe per-CF
  migration via `migrate_to_unified`. A cross-CF transaction in unified
  `SyncMode::Full` remains one checksummed WAL frame and now has a regression
  test proving it performs exactly one physical WAL sync and survives reopen.

## 0.5.0

Three additive changes, no API or format break. Minor bump: two new public
`DB` methods (the durability-inspection hooks).

- **Durability-inspection hooks** — new `DB::column_family_config(name)`
  returns a column family's *effective* durable configuration (what a reopen
  restores, including live-added partition rules), and
  `DB::wal_sync_count()` counts successful physical `sync_data()` calls
  across every WAL the DB opens (per-CF, unified, and post-rotation; wired
  like the poison flag, so rotation never resets it). Motivation: a consumer
  whose correctness depends on `SyncMode::Full` (spada's raft O1 boundary)
  can now *verify* the recorded mode instead of assuming the config it
  opened with, and tests can assert on real physical syncs — under
  `SyncMode::None` the counter never advances, which is exactly what a
  durability test should pin. Added by the spada raft-correctness
  remediation (its S-077/S-082 decisions).

- **Batch column-family creation** — new
  `DB::create_column_families(&[(&str, ColumnFamilyConfig)])` creates a batch
  of CFs and persists the manifest **once** for the whole batch instead of
  once per CF. Semantics are identical to N sequential `create_column_family`
  calls (handles returned in input order); all names are validated up front
  (length, comparator, config, collisions against existing CFs, duplicates
  within the batch), so a conflicting batch creates nothing and never
  persists, and the registry write lock is held across the batch so a
  concurrent creator can never observe it half-built. Motivation: each
  per-CF creation ran a full manifest rebuild + persist — a temp-file
  `sync_all()` (`F_FULLFSYNC` on macOS) plus a directory fsync — and the
  manifest is a full rebuild over all CFs, so after N creations the last
  write already contained everything the first N−1 wrote. A consumer opening
  11 CFs at boot paid ~22 fsyncs for the information content of 2; measured
  209.8 ms per-CF vs **22.5 ms batched** for 11 CFs (median of 8, real
  `F_FULLFSYNC`, ~9.3×). The single-CF path is untouched. Deliberately *not*
  done: lazy WAL materialization — `Wal::open` performs no fsync in any sync
  mode (WAL creation was never the cost), and deferring it would complicate
  the crash-consistency-critical commit/rotation path to save ~22 ms of file
  creates.
- **S3 backend: bounded retry on transport errors** (feature `s3`) —
  every request the backend issues is wrapped in a bounded retry: up to 4
  attempts, 25/50/100 ms backoff, retrying **only** transport-level
  `S3Error::Hyper`/`S3Error::Io`. This closes the hyper 0.14 keep-alive
  reuse race (hyperium/hyper#2136): rust-s3 0.35's tokio backend drives a
  raw `hyper::Client` with a 90 s idle pool and no retry, so a store (or a
  NAT in front of it) that drops a pooled idle connection first kills the
  next request mid-flight with `IncompleteMessage` — a bodied PUT is the
  most exposed because hyper will not replay it. Retrying is sound because
  every operation this backend performs is idempotent by construction:
  part objects use unique never-reused ids and are written whole (single
  PUT), and reads/HEAD/COPY/DELETE/LIST are idempotent by nature. HTTP
  status failures surface as `Ok` with a non-2xx code and can never
  trigger a retry; nothing non-idempotent exists to be retried.

## 0.4.0

Derived-partitioning milestone (A5). Additive — no manifest or API break; a
rules-only column family encodes byte-for-byte as before.

- Partitions may be computed from the key by a consumer-supplied `PartitionFn`
  (`PartitionScheme::Derived`) rather than enumerated in `partition_rules`. The
  partition becomes a *function* of the key, which is what a consumer keying by
  `(namespace, cluster_key)` needs — its partition count is a function of the
  data, not a list anyone can write out in advance. Persisted by scheme name and
  resolved on open through `Options::partition_fns`, mirroring comparators; a
  missing or mismatched implementation is a hard error, never a silent fallback
  to rule-based cutting.
- `DB::list_partitions` returns the materialized bottom-level partitions
  (`PartitionInfo`), read-only, so a consumer can verify its partitioner
  produced the physical separation it intended.
- Debug-only guard in bottom compaction against a `PartitionFn` that is not
  order-compatible — the misimplementation that would otherwise produce a bottom
  SSTable spanning two partitions. No release-build cost.

## 0.3.2

- Derived partitioning (A5) landed (see 0.4.0 for the consumer-facing
  additions and hardening that finalized it).
- Zero-materialization flush merge.

## 0.3.1

- Make part-tier move retries safe after a lost post-commit response, including
  partitions containing a mix of already-moved and off-target SSTables.
- Add deterministic move-phase observation for crash and durability testing.
- Preserve more than 255 per-level compression, compression-rule, partition,
  and tier policies without breaking manifests written by ondaDB 0.3.0.
- Release the database directory lock when the final public `DB` handle drops.
