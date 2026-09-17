#!/bin/bash
export PATH=$HOME/.cargo/bin:$PATH
cd ~/scratch/burrmill-arith
while pgrep -f "release/cost gen" >/dev/null; do sleep 5; done
cat out-cost-gen.txt
du -sh data/cost
cargo build --release --bin cost > build-cost.log 2>&1
: > out-cost.txt
./target/release/cost verify 2>&1 | tee -a out-cost.txt
for i in 1 2 3; do
  for m in builtin checked builtin-text checked-text checked-texttext scan; do
    /usr/bin/time -v ./target/release/cost $m 2> time-$m-$i.txt | tee -a out-cost.txt
    grep -E "Elapsed|Maximum resident" time-$m-$i.txt | sed "s/^/  [$m #$i] /" | tee -a out-cost.txt
  done
done
echo COST-DONE | tee -a out-cost.txt
rm -rf ~/scratch/burrmill-arith-551; mkdir -p ~/scratch/burrmill-arith-551
cp -r src examples Cargo.toml ~/scratch/burrmill-arith-551/
cd ~/scratch/burrmill-arith-551
sed -i 's/datafusion = "=55.0.0"/datafusion = "=55.1.0"/' Cargo.toml
cargo build --release --examples --bins > build-551.log 2>&1
grep -E "^(error|warning)" -A 6 build-551.log | head -40
for e in exp1 exp2 exp3 exp4; do ./target/release/examples/$e > out-$e.txt 2>&1; done
echo ALL-DONE > done.flag
