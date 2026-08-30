# 0.6-B — paced obsolete-file deletion: delete-storm measurement

**Date:** 2026-08-30 · **Branch:** `worktree-agent-aaf7dd7cb472f0ca7` (on
`roadmap/wave-a`) · **Host:** macOS (darwin 25.5.0), Apple silicon, APFS
**Harness:** `tests/io_limiter_bench.rs::delete_storm_paced_versus_unpaced`,
`#[ignore]`d; re-run with

```sh
RUNS=5 ONDADB_DELETE_RATE=4194304 \
ONDADB_BENCH_OUT=bench-results/0.6B/2026-08-30/raw.jsonl \
cargo test --release --test io_limiter_bench -- --ignored --nocapture \
  delete_storm_paced_versus_unpaced
```

Raw per-phase records: `raw.jsonl` (10 records = 5 runs x 2 phases), retained.

## What was measured

Per run, on a fresh database: 120 L0 files (1,500 keys x 128-byte values each,
`write_buffer_size` 256 KiB, `l1_file_count_trigger` unreachable so background
compaction never fires), then one `DB::compact` sweep — which retires **480
files** (L0→L1 plus the in-place bottom rewrite, klog and vlog each) in a single
event. Point reads run throughout on another thread; `sync_wal` latency is
sampled every 100 ms. The measured window runs from the start of the sweep to
the moment the last obsolete file is gone.

| Phase | `obsolete_delete_bytes_per_second` |
|---|---|
| `delete_unpaced` | 0 — unlinked inline inside `compact` (today's behaviour) |
| `delete_paced` | 4 MiB/s — queued to the `onda-delete` worker |

Flush and compaction bandwidth is unlimited in **both** arms, so the only
variable is how the unlinks are distributed.

## Results (median of 5 runs; latencies in microseconds)

| Phase | window s | files retired | delete MB | reads | p50 | p99 | p99.9 | max | fsync p99 |
|---|---|---|---|---|---|---|---|---|---|
| delete_unpaced | 1.06 | 480 (uncharged) | — | 323,121 | 0 | 0 | 320 | 28,879 | 23,999 |
| delete_paced | 6.25 | 480 | 28.55 | 11,307,592 | 0 | 0 | 1 | 14,471 | 34,209 |

Per-run window (s), sorted:

- unpaced: 0.409, 0.481, 1.056, 1.270, 1.410
- paced: 6.151, 6.209, 6.251, 6.285, 6.356

## Finding 1 — the deletion rate is enforced, precisely

Every paced run retired the same 480 files and charged the same 28,550,545
bytes (deterministic: file sizes plus the 4,096-byte metadata floor for the
empty vlogs), and every one took 6.15–6.36 s. Against the 4.19 MB/s ceiling:

| run | delete MB | window s | MB/s | MB/s less one burst |
|---|---|---|---|---|
| 0 | 28.55 | 6.285 | 4.54 | 3.87 |
| 1 | 28.55 | 6.356 | 4.49 | 3.83 |
| 2 | 28.55 | 6.151 | 4.64 | 3.96 |
| 3 | 28.55 | 6.209 | 4.60 | 3.92 |
| 4 | 28.55 | 6.251 | 4.57 | 3.89 |

4.49–4.64 MB/s measured, 3.83–3.96 MB/s once the initial full burst the bucket
hands out at open is subtracted: the configured 4.19 MB/s is bracketed from both
sides, and the spread across runs is 2.4 %. The storm is stretched **5.9x**
(median 6.25 s against 1.06 s). The mechanism does what it says.

## Finding 2 — no foreground benefit is demonstrable on this host

The paced arm's read tail looks better (p99.9 of 1 us against 320 us; max
14.5 ms against 28.9 ms), and that number should **not** be reported as the
feature working. Three reasons:

1. **The two windows are not comparable.** The unpaced window is 1.06 s of
   nothing but compaction; the paced window is the same compaction plus ~5 s in
   which the only remaining background work is the unlink queue. Per-second read
   throughput differs 5.8x between the arms (median 305 k/s unpaced against
   1.81 M/s paced) for exactly that reason. A percentile taken over a window
   whose *composition* changed between arms cannot attribute the difference to
   deletion pacing.
2. **The reads never reach the device.** p50 and p99 are 0 us in both arms — a
   28 MB working set on a host with many GB of RAM is served from the block
   cache and the page cache. This is the same defect recorded for 0.6-A, and it
   has the same consequence: the contention the feature exists to relieve is not
   present in the experiment.
3. **The host is not quiet enough for a tail comparison.** Read counts in the
   unpaced arm span 98,408 to 1,810,322 — an **18.4x spread** within one
   configuration, well past the 1.3x validity bar 0.6-A set for itself.
   `fsync_p99` ranges 7 ms to 448 ms with no relation to the arm, which is
   physically meaningless.

What *is* real and repeatable: an unpaced sweep issues 480 unlinks in a burst
inside `compact`, and a paced one spreads the identical work over a window the
operator chose, to within 2.4 % run-to-run.

## Gate decision

**Acceptance not demonstrated. Ship with the default at 0 — unpaced.**

That is the shipped state and needs no further action:
`obsolete_delete_bytes_per_second` defaults to `0`, no channel and no thread are
created, and `remove_sst_file` unlinks on the caller's thread exactly as every
release before 0.6 — pinned by `db.rs::no_deletion_worker_when_unpaced` and
`tests/maintenance.rs::unpaced_deletion_is_immediate`. The rollback described in
the feature doc *is* the default configuration.

Verified and relied upon regardless of the acceptance question:

- the deletion rate is enforced to within the burst allowance (Finding 1, and
  `tests/maintenance.rs::paced_deletion_spreads_over_fake_clock` in simulated
  time);
- an empty vlog still costs one metadata block, so a storm of tiny deletions is
  paced too (`db.rs::retire_charges_metadata_minimum_for_empty_vlog`);
- the deletion pause still defers every unlink with the worker in play
  (`paused_deletion_still_defers_with_worker`,
  `db.rs::paused_deletion_defers_to_the_pending_list`, and the unchanged
  `backup_consistent_during_compaction`);
- `close` drains the queue before releasing the directory lock
  (`close_drains_deletion_queue_before_lock_release` — verified load-bearing by
  removing the drain, which fails it with 108 surviving files);
- poison releases a parked worker
  (`db.rs::poison_does_not_hang_the_deletion_worker`).

Before any non-zero default is recommended, the delete-storm phase needs the
same two fixes 0.6-A's does — a quiet host and a working set larger than RAM —
plus one of its own: **equal-length measurement windows** in the two arms, so
the latency comparison is not reading a difference in window composition.
