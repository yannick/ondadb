# wavesdb roadmap → ondaDB feature assessment

**Date:** 2026-08-30 · **Input:** `/Volumes/HOME/code/storage-engines/wavesdb/docs/plans/`
(README, implementation-plan, validation ledger, all 25 feature docs) plus its
`outputs/cutting-edge-gap-analysis.md` research report. **Compared against:** ondaDB
at 0.8.0, including the findings in [`code-review-2026-08.md`](code-review-2026-08.md).

## Why the wavesdb plans transfer unusually well

The two engines are format siblings. Verified side by side:

| Surface | ondaDB | wavesdb |
| --- | --- | --- |
| WAL frame | `[len u32][crc32c u32][payload]` | identical |
| SST block frame | `[alg(1)][compLen(4)][rawLen(4)][crc(4)]` | identical |
| klog footer magic | `WAVESST1`-derived (`0x574156…`) | same family |
| Entry flags byte | `0x01` Tombstone, `0x02` HasTTL, `0x04` HasVlog, `0x08` **DELTA_SEQ (unused)**, `0x10` SingleDelete | same, except `0x08` = VlogGrouped (live) |
| Per-CF WAL generations, unified mode, striped commit serialization, range locks for compaction, manifest temp+fsync+rename, parts/tiers, table cache with byte budget | yes | yes |

Consequences: (a) the wavesdb plans' *repository-aware* designs (baseline anchors,
crash matrices, call-site lists) map almost 1:1 onto ondaDB's Rust equivalents;
(b) the corrected decisions in their `validation.md` are lessons ondaDB gets for
free — several were independently rediscovered in our own review; (c) if the two
engines ever mount each other's shared-tier objects (`attach_part_by_ref` /
wavesdb's mounts), **capability-bit and record-kind numbering must be coordinated**
(§7).

Where ondaDB is *ahead* of the wavesdb baseline, the corresponding plan is already
done and should be skipped: bounded compaction jobs + range-lock concurrency +
debt-based write pacing with soft/hard limits (their 0.8.0-equivalent is wavesdb's
motivating example of the *old* behavior), lazy memtable iterators, shortened index
separators, B+tree index option, S3 tier backend, byte-budgeted reader cache.

---

## 1. Adopt — clear fit, cheap, fills a verified ondaDB gap

### 1.1 Strict decoding + capability framework (wavesdb 1.0) — the gate for everything else

ondaDB's decoders have exactly the hole wavesdb found: `wal.rs::decode_record` and
`sst/mod.rs::decode_entry` test known bits with `flags & X != 0` and **ignore
unknown bits and invalid combinations**. Five of eight flag bits are consumed and
the roadmap needs more record kinds (merge, range delete, txn control) than the
three remaining bits can carry — the identical wall that pushed wavesdb to manifest
v4.

Wavesdb's two-change split is right for us too:

- **Change A — strict masks** on every legacy decoding surface (WAL records, klog
  entries, block headers, footer flags, manifest tail tags). Also gives a principled
  home for review finding M9 (manifest `level` field trusted without bound →
  reject instead of allocating 4B levels).
- **Change B — capability word** in the manifest (ondaDB is at `VERSION = 1`; bump
  to 2 with a known-bit-checked `FormatCapabilities` u64, following ondaDB's
  existing append-tolerant tail pattern), plus versioned record envelopes with an
  explicit `kind` field separate from modifier flags. Persist-before-use, unknown
  bits fail closed, golden fixtures pin the numbering.

One ondaDB-specific decision: `flags::DELTA_SEQ` (0x08) is dead in ondaDB while the
same bit is *live* (`VlogGrouped`) in wavesdb. If cross-engine mounts ever matter,
the kind registry must account for that divergence now, while it's cheap.

Effort: 2–4 wks. Prerequisite for: merge operators, range deletes, periodic
compaction metadata, 2PC records, prefix-delta blocks.

### 1.2 Manifest edit log (wavesdb 2.2, local slice only)

This is the fix for review finding **M5** (12.4 MiB re-encoded + fsynced per
structural change at 100k parts — ondaDB's own sizing probe). The wavesdb design is
directly reusable: `MANIFEST` snapshot + `MANIFEST-EDITS` append log, monotonic
edit IDs, snapshot `appliedThrough`, the two-file ordered-rename compaction
protocol, and — critically — their corrected recovery rule: *a complete record
with a bad CRC is corruption, never a torn tail*.

**Cheaper for ondaDB than for wavesdb:** ondaDB's manifest is local-only by design
("the commit point is the local manifest's fsync+rename, not an S3 object" —
AGENTS.md). Wavesdb's hardest slices — immutable generation objects, conditional
CAS pointer, replica resync, promotion — **do not apply**. Skip them; the local
protocol is the whole v1 scope. ondaDB also has fewer persist call sites to migrate
(flush, unified flush, compaction, mover/parts, CF lifecycle, nonce) and a simpler
model (no `BlobMeta`).

Their `catalogTxn` recommendation ("durable before publish, publish before retire"
as one helper, no catalog mutation outside it) is worth adopting wholesale — it
would have prevented the class of bug in review finding F1's neighborhood.

Effort: 4–7 wks (vs wavesdb's 7–11).

### 1.3 Vlog value cache (wavesdb 0.5 blob value cache)

ondaDB's WiscKey read path is uncached below the block cache: `Reader::read_vlog`
performs a positional read + CRC + decompress on every access; `vlog_verified`
memoizes the *checksum*, never the bytes. With the default
`klog_value_threshold = 512`, most non-trivial values live in vlogs — every hot
large value pays disk + decompress forever.

The wavesdb design maps cleanly: admit decoded values ≤ a persisted CF limit
(`MaxCachedVlogValueBytes`, 0 = off) through the **existing `BlockCache`** keyed
`(file_id, vlog_offset)` — ondaDB's cache is already keyed by `(file_id, offset)`
and vlog frames never share offsets with klog blocks, so no new cache and no new
key domain is strictly needed. Admit only after CRC/decompress success; hit must
validate length against the klog pointer's `val_len`.

Effort: 1–2 wks. Biggest per-line-of-code win in the whole roadmap for
value-heavy ondaDB workloads (spada's ~11 KiB posting frames).

### 1.4 Overlap-aware compaction picking (wavesdb 0.2)

ondaDB's `build_job` (levels ≥ 1) picks the first candidate from the per-level
sweep cursor — round-robin fairness, overlap-blind. Wavesdb's score
(`overlapBytes(candidate) / candidateBytes`, integer cross-multiplication, cursor
as cyclic tiebreak) is a contained change to `pick_compaction`/`build_job` with no
format work. RocksDB reports write amp dropping "by more than half" from this one
change. Keep the L0 oldest-window rule untouched (it's a correctness invariant in
both engines). ondaDB's foreign-mount veto (`gather_target` returning `None`)
slots in as their "mount veto" already does.

Effort: 1–2 wks.

### 1.5 Periodic compaction (wavesdb 0.3) — ondaDB has the exact hole

ondaDB reclaims TTL entries and tombstone debris **only inside a compaction**, and
compactions trigger only on L0 file count or level bytes. An idle database never
reclaims anything: expired TTL data sits on disk forever unless the operator calls
`DB::compact` manually. That is wavesdb's motivating case verbatim.

Two of their corrections apply directly:

- `SstMeta.max_entry_time` **cannot** be the periodic clock — ondaDB's compaction
  deliberately carries it forward as the max over inputs *for the tier mover's age
  gate*. Reusing it would either make periodic outputs instantly re-eligible or
  reset cold data's age and break tier placement. Use a separate
  `last_compaction_time` field under the capability framework (§1.1).
- Stamp legacy tables at **capability-enable time** inside the same manifest
  write, not at open ("eligible one interval after open" is not restart-safe).

ondaDB already has the scheduling shape: the part mover's DB ticker cadence
(`part_mover_interval` on the compaction worker) is the pattern for the periodic
check; `run_manual`'s in-place bottom rewrite (`compact_into(last, last)`) is
already written and is exactly the "bottom eligible table" path. Refuse the option
for `CompactionStyle::Fifo` (FIFO has its own age eviction).

Effort: 2–3 wks after §1.1.

### 1.6 Per-operation PerfContext (wavesdb 0.10)

ondaDB has aggregate relaxed counters (`point_reads`, `bloom_skips`, `sst_probes`,
cache stats) but nothing caller-owned and per-operation. A `PerfContext` struct
passed down (Rust: `Option<&mut PerfContext>` or a thread-local — no `context.Context`
needed) is the measurement prerequisite their roadmap correctly puts first, and it
is also the natural place to surface review finding M2 (compaction failures are
swallowed — add a failure counter + last-error while touching observability).

Effort: 1–2 wks.

---

## 2. Adopt — high value, schedule deliberately (bigger or gated)

### 2.1 Range tombstones + excise (wavesdb 1.2) — the flagship

The single biggest user-facing gap in ondaDB: deleting a keyspace means one
tombstone per key; dropping a range without rewriting requires partition rules
configured *in advance*. Everything in the wavesdb design transfers, and two of
their corrected decisions are load-bearing for ondaDB too:

- **Do not shard range tombstones by start key into the point memtable** — a point
  lookup for `k` must find every covering span. Dedicated fragment structures
  beside the point entries (ondaDB: a second ordered structure per `Memtable`,
  fragmented at flush), separate range-delete blocks in the SST with footer
  handles, `CoveringSeq(key, read_seq)` resolution folded into
  `ColumnFamily::get` and the `Iterator` merge.
- **Excise requires** full comparator coverage, `table.max_seq < tombstone.seq`,
  `tombstone.seq <= oldest_snapshot`, a retained durable owner, and
  manifest-before-unlink — ondaDB's `persist_manifest` → `remove_sst_file`
  ordering is already exactly this shape.

ondaDB-specific synergies the wavesdb plan explicitly calls out and we get for
free: **partition cuts are mandatory fragment boundaries** (bottom SSTs already
never span partitions), and **the part machinery gives whole-part metadata drops
that Pebble lacks** — `detach_part` is already a metadata-only range removal for
partition-granular ranges; excise fills the sub-part granularity. ondaDB's
`range_locks` supply the excise veto/acquire, and the coarser `commit_mu` makes
their v1 "range commits serialize" guard nearly free (Snapshot/Serializable commits
already hold it — see review M3 for the latency caveat this adds).

Two ondaDB-only interactions the plan must add to its 10-surface checklist:
`attach_part`/`attach_part_by_ref` must validate incoming range blocks (and review
finding F2's disjointness fix should land first, or range fragments widen the
breakage), and the S3 tier's "no delete on shared tiers" rule is another excise
veto alongside foreign mounts.

Effort: 8–13 wks, after §1.1. Deliver range semantics first, excise second —
their split is right.

### 2.2 IO rate limiter + paced deletion (wavesdb 0.6)

Review finding (§4 "genuinely missing"): ondaDB paces writers by *debt bytes* but
has no *bandwidth* dimension — a compaction burst saturates the device and stalls
foreground reads; `remove_sst_file` unlinks immediately outside pauses. The SILK
lesson their plan leads with is the right framing. ondaDB mapping: a work-conserving
token bucket (`ioctrl`-style trait) charged at flush/compaction read+write
boundaries, an IO class threaded into `Storage`/`ReadHandle` calls from background
call sites only (foreground reads never wait), and the existing
`file_deletion` pause state extended into an owned paced worker. No `context.Context`
in Rust — a `Limiter` handle on the call path is simpler.

Effort: 3–5 wks. Do the IO classes/limiter review before the deletion worker, as
they recommend.

### 2.3 Prefix-delta key encoding (wavesdb 2.1)

ondaDB stores the **full user key per entry** but — unlike wavesdb — already has
the machinery this feature needs: `FOOTER_RESTARTS`, `RESTART_INTERVAL = 8`, the
restart-offset trailer, `restart_scan_offset` binary search, and `SstIterator`'s
offset walk. Adding `sharedLen/suffixLen` deltas with restart entries as full-key
anchors is an incremental extension of existing structures rather than the
from-scratch reverse-iterator problem wavesdb faces. Their open-decision answers
(trailer at end of payload; byte-prefix only, never comparator-aware; lazy restart
keys; `SeekForPrev` = seek then prev) all carry over.

Caveat that applies doubly to ondaDB: block compression already recovers most of
the *disk* bytes on prefix-heavy data; the wins are decompressed cache residency
and decode bandwidth. Gate on measurement (their interval sweep), keep opt-in, and
note the interaction with review M6 — settle whether blocks are 4 KiB or 16 KiB
*before* measuring delta encoding, or the numbers will be confounded.

Effort: 3–5 wks, after §1.1.

### 2.4 MultiGet (wavesdb 0.4)

No batched point lookup exists in ondaDB. Their corrected premise is the useful
part: bloom/index work stays per key; the win is **deduplicating block fetches**
(one read + decode per distinct block) plus optional bounded parallel IO. In Rust
this is *simpler* than their design — no cancellation contexts; a plan built from
ondaDB's existing `point_read_sources` enumeration, grouped by `SstHandle`, with
per-table bloom/index batching in `Reader`, and optional `std::thread::scope`
parallelism under a DB-wide semaphore. Must resolve at one `read_seq` with results
identical to sequential `get`s (their oracle-test discipline).

Effort: 2–4 wks.

### 2.5 Subcompactions (wavesdb 0.8) — but only after multi-worker lands

ondaDB currently spawns **one** compaction worker (review M7), so parallel spans
within a job are moot until `num_compaction_threads` is honored. After that, the
fit is unusually good: ondaDB's **partition boundaries are ready-made, always-safe
split points** for bottom jobs (the wavesdb plan calls this out as the ideal
boundary source), `range_locks` already exclude concurrent part operations, and
`CompactionOutputBuilder` already freezes job-wide decisions. Their v1 exclusion
list carries over (no spans with a `CompactionFilterFn`, FIFO, manual whole-range
rewrites).

Effort: 3–5 wks, gated on M7.

---

## 3. Adopt with conditions / product-direction dependent

| Feature | Verdict for ondaDB | Notes |
| --- | --- | --- |
| **0.7 global memtable budget** | Worth it *or* delete the option | ondaDB has the identical dead `max_memory_usage` ("0 => auto ≈75%" — nothing). Either adopt wavesdb's soft-budget design (7/8 trigger, hard wait, honest "not RSS" contract) or delete the field per review §4. Don't leave it lying. |
| **0.1 per-level bloom FPR** | Nice-to-have | Small; needs §1.6 counters for evidence. Their corrected claim matters: skipping the *bottom* filter only pays on hit-heavy workloads. ondaDB's bloom is already xxh3 + correctly sized from real key counts. |
| **0.9 tailing iterator** | Cheap, take it | Narrow semantics as corrected (keys strictly after the cursor; **not CDC** — updates behind the cursor are invisible). ondaDB's documented queue-peek workloads are the target. DB-level only, never Txn (a fixed snapshot must not silently refresh). |
| **1.1 merge operators** | Yes, after range deletes | Same argument as wavesdb: counters/HLLs currently pay a snapshot Get per update under MVCC. Their fold rule (contiguous suffix wholly below `oldest_snapshot`, stop at bases/deletes/mounts) plugs straight into `VersionRetention`. 5–8 wks. |
| **3.2 durable 2PC** | If ayu/spada need a coordinator participant | Unified-layout-only maps to ondaDB's unified mode — which conveniently is also the only layout with cross-CF crash atomicity (review F3). Prepare/decision frames force flush+sync regardless of `SyncMode`; WAL generation pins prevent flush retiring a generation holding the only prepared batch. 6–10 wks. |
| **3.3 pessimistic locking** | Later | Interval-aware lock manager, new (do not reuse `range_locks` — same reasoning as wavesdb). Fits after 1.2's interval representation exists. 4–7 wks. |

## 4. Skip or spike-only for ondaDB

| Candidate | Why not now |
| --- | --- |
| **3.1 managed sequence mode** | Only if an external coordinator (ayu) must own sequence assignment. The discard-floor/`ErrHistoryDiscarded` machinery is real work with one consumer. Keep the design link, defer the feature. |
| **3.4 large-txn private spill** | Niche; depends on 3.2's decision records. ondaDB's `Txn` arena + `BUF_POOL` (32 MiB × 4/thread) bounds ordinary batches already. |
| **4.1 tiered / lazy leveling** | ondaDB documents Spooky/DCA as a non-goal and implements classic leveled. The wavesdb correction is the whole cost: tiered levels need **durable run identity** (their `levels [][]*tableHandle` disjointness assumption is ours too — `find_overlapping`), which needs the edit log (§1.2) first. Spike only, after 1.2, and gate on their criterion (mixed-read p99 at matched cache). |
| **4.2 wide-column entities** | A value-framing API with zero storage changes — cheap but low priority for ondaDB's consumers. Take the design as-is if wanted. |
| **4.3 WAL failover** | Fix review F5 (WAL dir fsync) first; then note ondaDB's WAL is *striped* in non-Full modes, which their unified-only scoping doesn't cover. Niche ops feature; prototype only. |
| **4.4 trace/replay** | Useful for ondaDB's measurement discipline (performance.md) but self-contained tooling. Their fidelity-mode warning (hashed traces are load tests, not state-fidelity replays) is the part worth remembering. Medium priority. |
| **4.5 O_DIRECT / io_uring** | Rust's library story is *better* than Go's (maintained io_uring crates exist), but ondaDB is deliberately no-async. O_DIRECT-for-compaction-reads is the plausible cheap half (aligned buffer pool, separate `Storage` handle so the table cache's fds stay buffered). Spike only, after MultiGet shows syscall-bound time. |

---

## 5. What ondaDB should NOT copy from the wavesdb plans

- **The object-store manifest publication protocol** (generation objects + CAS
  pointer). ondaDB's stated stance is that object CAS is ayu's layer, not the
  engine's; the local edit log (§1.2) needs none of it. Revisit only if ondaDB ever
  grows replica mode.
- **Blob GC / staleness machinery.** wavesdb has *shared* blob files across tables
  (hence `BlobMeta`, reference accounting, blob GC). ondaDB's vlog is per-SSTable —
  obsoleting a table obsoletes its vlog. Their 0.5 value cache transfers; their
  blob-GC plumbing does not apply.
- **Replica/promotion slices** anywhere in the plans — out of scope for ondaDB
  (documented non-goal).

## 6. Suggested sequencing for ondaDB

Adjusting wavesdb's Wave A–E for ondaDB's state (and folding in the review's fix
order, since several findings are de-facto prerequisites):

1. **First, the review's P1 items** (F1 backup-of-tiered-data, F4/F5 fsync
   ordering, F3 documentation, M7 compaction workers, M6 block-size truth). M7 and
   M6 specifically unblock subcompactions and honest prefix-delta benchmarks.
2. **Wave A (gate):** strict decoding → manifest capabilities + record envelopes
   (§1.1) → PerfContext (§1.6). Nothing durable ships before this.
3. **Wave B (reversible runtime):** overlap picking (§1.4) → vlog value cache
   (§1.3) → IO limiter + paced deletes (§2.2) → MultiGet (§2.4) → memtable budget
   or option deletion (§3) → tailing iterator (§3). Each independently revertable.
4. **Wave C (durable):** manifest edit log (§1.2 — arguably belongs earlier given
   M5's severity) → periodic compaction (§1.5) → prefix-delta blocks (§2.3) →
   range tombstones then excise (§2.1) → merge operators (§3).
5. **Wave D/E:** 2PC only on product need; tiered compaction and io_uring as
   time-boxed spikes with wavesdb's spike contract (pre-registered gate, deletion
   list, post-mortem).

Their benchmark-evidence format (baseline/candidate JSONL, ≥10 repetitions,
`env.txt`, gates measured against the baseline's own spread) is a straight upgrade
of `docs/performance.md`'s methodology — adopt it as process regardless of which
features are taken.

## 7. Cross-engine coordination note

Both engines consume the same shared-tier mounting model (ondaDB
`attach_part_by_ref`, wavesdb mounts) and already share byte-level formats. If
either lands a capability word, record-kind registry, or block-encoding marker,
the numbering should be coordinated **before** first write, not reconciled after —
the cheap moment is now, while ondaDB's `DELTA_SEQ` bit is still dead and its
manifest is still `VERSION = 1`. A one-page shared registry (which engine owns
which bits/kinds, and the agreed meaning of `0x08`) would prevent the
"same mask, different meaning" failure wavesdb's own risk register names as its
top compatibility risk.

> **Resolved, 2026-08-31 — and not entirely in time.** The registry asked for
> above is [`format-registry.md`](format-registry.md). Capability bits 0–6 and
> record kinds 1–5 / 16–18 turned out to have been assigned *identically* by
> both engines independently, which is the part of the contract that held.
> `0x08` did not: 0.9.0 retired `DELTA_SEQ` and made the bit reserved-unknown
> while wavesdb was already writing it as `FlagVlogGrouped`. The bit is
> wavesdb's — ondaDB never wrote it, wavesdb does — and ondaDB's rejection of
> it stays, because a grouped pointer addresses a compression group this engine
> cannot decompress; failing closed beats misreading. `format::wavesdb_reserved`
> now pins that assignment and the other wavesdb-owned numbers with build-time
> assertions, so the bit can never be reclaimed here. wavesdb gated the encoding
> behind capability bit 8 (`CapVlogGrouping`) so the incompatibility is declared
> in the manifest and surfaces at open rather than mid-scan.
