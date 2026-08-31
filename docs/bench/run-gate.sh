#!/usr/bin/env bash
# The full RFC-0044 slice-1 gate: segment sweep in both orderings, then the high-cardinality gate.
set -u
BIN=./target/release/burrmill-bench
echo "== segment sweep, 2,000,000 rows, 512 parties =="
for seg in 1 100 1000 10000; do
  for order in duck_first burrmill_first; do
    ROWS=2000000 SEGMENTS=$seg REPEATS=5 ORDER=$order $BIN 2>&1 | grep -E '^(RESULT|Error)'
  done
done
echo
echo "== high-cardinality gate, 100 segments =="
for addrs in 10000 200000 1000000; do
  for order in duck_first burrmill_first; do
    ROWS=2000000 SEGMENTS=100 ADDRS=$addrs REPEATS=5 ORDER=$order $BIN 2>&1 | grep -E '^(RESULT|Error)'
  done
done
echo
echo "== operator-only peak RSS (no DuckDB linked, no DataFusion session) =="
# REPEATS=1, and that is not a detail. Allocator retention accumulates across folds inside one
# process, so REPEATS=5 reported 608 MB where a single fold reports 345 - a number about the harness,
# not the operator. Same reason the fixture is written once and re-read.
for addrs in 512 200000 1000000; do
  rm -rf /tmp/burrmill-fx && ROWS=2000000 SEGMENTS=100 ADDRS=$addrs KEEP_FIXTURE=/tmp/burrmill-fx $BIN >/dev/null 2>&1
  printf "addrs=%-8s " "$addrs"; REPEATS=1 $BIN fold /tmp/burrmill-fx 2>&1 | grep -E '^(FOLD|Error)'
done
echo
echo "== peak RSS against thread count, 1M groups =="
# The 256 MB gate does not say at what parallelism, and parallelism is the load-bearing variable:
# the aggregate is flat at 99 MB and everything else scales with cores. Roadmap 1.2c.
rm -rf /tmp/burrmill-fx && ROWS=2000000 SEGMENTS=100 ADDRS=1000000 KEEP_FIXTURE=/tmp/burrmill-fx $BIN >/dev/null 2>&1
for t in 1 4 8 16 32; do
  printf "threads=%-4s " "$t"
  RAYON_NUM_THREADS=$t REPEATS=1 $BIN fold /tmp/burrmill-fx 2>&1 | grep -E '^(FOLD|Error)'
done
rm -rf /tmp/burrmill-fx
