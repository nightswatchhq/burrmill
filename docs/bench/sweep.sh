#!/usr/bin/env bash
# RFC-0044 slice 1 gate sweep.
#
# Two orderings, because whichever engine runs first pays to warm the page cache and the second gets
# it free. A ratio that survives both is about the engines; a ratio that does not is about the cache.
set -u
BIN=./target/release/burrmill-bench
ROWS=${ROWS:-2000000}
REPEATS=${REPEATS:-5}
for seg in ${SEGMENTS_SWEEP:-1 100 1000 10000}; do
  for order in duck_first burrmill_first; do
    ROWS=$ROWS SEGMENTS=$seg REPEATS=$REPEATS ORDER=$order "$BIN" 2>&1 | grep -E '^(RESULT|PARITY|Error)'
  done
done
