# 0.9 keyspace-tailing iterator — acceptance benchmark, 2026-08-30

Harness: `examples/queue_peek.rs`, driver `run.sh` (5 runs, modes alternated
run-by-run so thermal drift hits both equally). Release build, default feature
set, macOS / Darwin 25.5.0. Raw output: `idle-poll.txt`, `streaming.txt`.

Baseline = the only thing available before 0.9: a fresh
`Txn::new_iterator_bounded(cf, Excluded(last_yielded), Unbounded)` per poll.
Treatment = `DB::new_tailing_iterator` + `refresh()`.

## Gate

From `docs/plans/phase-0-runtime/features/09-tailing-iterators.md`:

> Queue-peek phase: iterator construction amortizes to roughly one rebuild per
> refreshed batch rather than one per poll, and throughput improves beyond the
> >=5-run baseline spread versus rebuild-per-poll.

**Verdict: the construction half is met decisively; the throughput half is met
on fixed work and is NOT demonstrable end-to-end on this harness.**

* Construction amortization — **met.** ~1,900x fewer constructions per entry.
* Throughput — **met on the fixed-work poll phase (34x, non-overlapping
  ranges); not demonstrated on the streaming phase**, which is producer-bound
  and whose run-to-run spread swamps any consumer-side difference.

## Phase A — idle-poll (fixed work, no producer)

Both modes drain the queue, then perform exactly 200,000 polls of an
up-to-date queue with no producer running and no `yield_now`. Identical work;
the only difference is what one poll costs. This is the state a real queue peek
spends most of its polls in.

| run | tail ns/poll | rebuild ns/poll |
| --: | --: | --: |
| 1 | 13.3 | 382.2 |
| 2 | 11.2 | 401.2 |
| 3 | 12.4 | 531.6 |
| 4 | 13.6 | 438.3 |
| 5 | 11.5 | 385.4 |
| **mean** | **12.40** | **427.74** |
| range | 11.2-13.6 | 382.2-531.6 |

**34.5x faster per poll**, with ranges an order of magnitude apart — far outside
any run-to-run spread this machine produces. Constructions: 1 (tail, the initial
segment) versus 200,000 (rebuild, one per poll).

This measurement is *conservative toward the baseline*: after the drain the
cursor sits above every SSTable, so bound pruning already removes them and the
rebuild's ~428 ns is its memtable-only best case. A tail that is behind, or a CF
with L0 files above the cursor, makes the baseline worse and the tail unchanged.

## Phase B — streaming (one producer, one consumer, end-to-end)

220,000 entries (20k backlog + 200k appended concurrently), consumer polls as
fast as it can and yields on an empty poll.

| run | tail ops/sec | rebuild ops/sec | tail ctor/entry | rebuild ctor/entry |
| --: | --: | --: | --: | --: |
| 1 | 138,664 | 73,471 | 0.00342 | 6.14 |
| 2 | 63,218 | 36,373 | 0.00341 | 13.50 |
| 3 | 51,307 | 65,610 | 0.00391 | 6.55 |
| 4 | 123,591 | 104,934 | 0.00386 | 2.27 |
| 5 | 51,914 | 66,788 | 0.00397 | 7.64 |
| **mean** | **85,739** | **69,435** | **0.00371** | **7.22** |
| range | 51,307-138,664 | 36,373-104,934 | | |

**Constructions per yielded entry: ~1,944x fewer** (0.00371 vs 7.22) — one
construction per ~270 entries yielded, against 7.2 per entry. That is exactly
the "one rebuild per refreshed batch rather than one per poll" the gate asks
for, and it is the one number here that is stable across runs.

**Throughput: no conclusion is supportable from this phase.** The nominal mean
favours the tail by 23%, but the ranges overlap almost completely (tail spans
2.7x across five runs, rebuild 2.9x). Decisive evidence that this is noise, not
signal: I ran this phase twice — once before and once after a hot-path
refactor that only removed a per-entry allocation — and **the mean ordering
flipped**. The first run's means were tail 46,844 / rebuild 56,757 (baseline
ahead by 21%); this run's are tail 85,739 / rebuild 69,435 (tail ahead by 23%).
Nothing in the change explains a swing that large in both directions. I am
reporting it as inconclusive rather than claiming the favourable run.

The structural reason is that both modes are bounded by the *producer*, not the
consumer. The rebuild consumer burns ~7 iterator constructions per entry and
still finishes in comparable time, because it was waiting on the writer either
way. The saving is real CPU that this configuration cannot convert into wall
clock. Phase A isolates it.

I tried to make phase B consumer-bound before concluding this: 8 concurrent
consumers, and a 256 KiB write buffer with `l1_file_count_trigger` raised to 500
to pile L0 files above the cursor (both exposed as harness flags). Neither
changed the picture — the producer stayed the bottleneck in every
configuration, and extra consumer threads mostly added scheduler churn to the
tail's tighter poll loop.

## Honest caveats

* Single machine, thermally noisy (+/-15-20% under sustained load, worse after
  load — `docs/performance.md`). This second run additionally shared the box
  with other concurrent `cargo` workloads, which is visible in phase B's wider
  spread and in phase A run 3's 531 ns outlier.
* Phase A's poll loop has no producer, so `refresh()` takes its cheapest path
  every time (one `valid()` check plus one atomic load). That is the honest
  characterization of an *idle* poll — the dominant case for a queue peek — but
  it is not the cost of a poll that finds data, which pays a full construction
  in both modes.
* `constructions_per_entry` in phase B depends on producer pacing; the absolute
  value is not portable across machines, the ratio between modes is the signal.
* Both phases were re-run against the final committed code (after the
  `record_cursor` allocation-reuse refactor); the numbers above are that run.

## Reproduce

```sh
bench-results/0.9/2026-08-30/run.sh 5
```
