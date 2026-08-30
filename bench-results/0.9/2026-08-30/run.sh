#!/bin/sh
# 0.9 acceptance benchmark: keyspace-tailing iterator vs rebuild-per-poll.
#
# Two phases, both alternating modes run-by-run so thermal drift hits each mode
# equally (this machine drifts 15-20% under sustained load; see
# docs/performance.md).
#
#   A. idle-poll  — fixed work: identical poll counts, no producer. Isolates
#                   the cost of one poll of an up-to-date queue.
#   B. streaming  — one producer, one consumer, end-to-end wall time.
#
# Usage: bench-results/0.9/2026-08-30/run.sh [runs]
set -eu

RUNS="${1:-5}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OUT="$ROOT/bench-results/0.9/2026-08-30"
cd "$ROOT"

cargo build --release --example queue_peek

: >"$OUT/idle-poll.txt"
: >"$OUT/streaming.txt"

i=1
while [ "$i" -le "$RUNS" ]; do
  for mode in tail rebuild; do
    printf 'run %s ' "$i" >>"$OUT/idle-poll.txt"
    cargo run -q --release --example queue_peek -- \
      -mode "$mode" -backlog 50000 -idle-polls 200000 \
      -db "$OUT/.data-$mode" >>"$OUT/idle-poll.txt"
  done
  i=$((i + 1))
done

i=1
while [ "$i" -le "$RUNS" ]; do
  for mode in tail rebuild; do
    printf 'run %s ' "$i" >>"$OUT/streaming.txt"
    cargo run -q --release --example queue_peek -- \
      -mode "$mode" -consumers 1 -ops 200000 -backlog 20000 \
      -db "$OUT/.data-$mode" >>"$OUT/streaming.txt"
  done
  i=$((i + 1))
done

echo "wrote $OUT/idle-poll.txt and $OUT/streaming.txt"
