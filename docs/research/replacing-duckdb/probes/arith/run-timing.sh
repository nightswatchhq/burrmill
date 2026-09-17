#!/bin/bash
cd ~/scratch/burrmill-arith
: > out-timing.txt
for i in 1 2 3; do
  for m in builtin checked builtin-text checked-text checked-texttext scan; do
    python3 ptime.py ./target/release/cost $m 2>&1 | tee -a out-timing.txt
  done
done
echo TIMING-DONE | tee -a out-timing.txt
