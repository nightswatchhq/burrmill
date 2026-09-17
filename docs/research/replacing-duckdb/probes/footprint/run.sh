#!/usr/bin/env bash
# Footprint harness. Usage: run.sh <variant dir> ; writes results/<variant>.txt as key=value lines.
set -u
export PATH=$HOME/.cargo/bin:$PATH
export CARGO_TERM_COLOR=never
ROOT=~/scratch/burrmill-footprint
V=$1
cd $ROOT/$V || exit 1
mkdir -p $ROOT/results
OUT=$ROOT/results/$V.txt
LOG=$ROOT/results/$V.log
: > $OUT; : > $LOG
rec() { echo "$1=$2" | tee -a $OUT; }
now() { date +%s.%N; }
# t <key> <cmd...> : wall-clock the command, record seconds and exit status.
t() {
  local key=$1; shift
  local s=$(now)
  echo "### $key: $*" >> $LOG
  "$@" > $LOG.$key 2>&1; local rc=$?
  cat $LOG.$key >> $LOG
  local e=$(now)
  rec "$key" "$(awk -v a=$s -v b=$e 'BEGIN{printf "%.1f", b-a}')"
  [ $rc -ne 0 ] && rec "${key}_rc" "$rc"
  return $rc
}
bytes() { stat -c %s "$1"; }
testbins() { find "$1/debug/deps" -maxdepth 1 -type f -name 't[0-9][0-9]-*' ! -name '*.d' -perm -u+x; }

rec host "$(hostname) $(nproc) cores $(rustc -V)"
rec deps_normal "$(cargo tree -e normal --prefix none 2>/dev/null | sort -u | wc -l)"
rec deps_all "$(cargo tree --prefix none 2>/dev/null | sort -u | wc -l)"
cargo fetch >> $LOG 2>&1

for profile in default nodebug; do
  if [ $profile = nodebug ]; then
    export CARGO_PROFILE_DEV_DEBUG=0 CFLAGS=-g0 CXXFLAGS=-g0
  else
    unset CARGO_PROFILE_DEV_DEBUG CFLAGS CXXFLAGS
  fi
  TD=$ROOT/$V/target-$profile
  export CARGO_TARGET_DIR=$TD
  # -j8 first, -j32 last, so the tree left behind is the -j32 test build the incremental runs sit on.
  for j in 8 32; do
    rm -rf $TD; t ${profile}_build_clean_j$j cargo build -j$j
    rm -rf $TD; t ${profile}_test_clean_j$j  cargo test --no-run -j$j
  done
  rec ${profile}_target_bytes "$(du -sb $TD | cut -f1)"
  rec ${profile}_target_debug_deps_bytes "$(du -sb $TD/debug/deps | cut -f1)"
  one=$(testbins $TD | head -1)
  rec ${profile}_testbin_one_bytes "$(bytes "$one")"
  rec ${profile}_testbin_count "$(testbins $TD | wc -l)"
  rec ${profile}_testbin_total_bytes "$(testbins $TD | xargs stat -c %s | awk '{s+=$1} END{print s}')"
  rec ${profile}_libbin_bytes "$(bytes $TD/debug/consumer-bin)"
  touch src/lib.rs; t ${profile}_incr_lib_test cargo test --no-run -j32
  touch src/lib.rs; t ${profile}_incr_lib_build cargo build -j32
  touch tests/t07.rs; t ${profile}_incr_test_test cargo test --no-run -j32
  unset CARGO_TARGET_DIR
done
unset CARGO_PROFILE_DEV_DEBUG CFLAGS CXXFLAGS

TD=$ROOT/$V/target-release; export CARGO_TARGET_DIR=$TD
rm -rf $TD; t release_build_j32 cargo build --release -j32
B=$TD/release/consumer-bin
rec release_bin_bytes "$(bytes $B)"
cp $B $TD/consumer-bin.stripped && strip $TD/consumer-bin.stripped
rec release_bin_stripped_bytes "$(bytes $TD/consumer-bin.stripped)"
rec release_ldd "$(ldd $B | awk '{print $1}' | paste -sd' ')"
rec release_glibcxx "$(objdump -T $B 2>/dev/null | grep -o 'GLIBCXX_[0-9.]*' | sort -uV | tail -1)"
rec release_glibc "$(objdump -T $B 2>/dev/null | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1)"
rec release_run "$($B 2>&1 | head -1)"
unset CARGO_TARGET_DIR

TD=$ROOT/$V/target-musl; export CARGO_TARGET_DIR=$TD
rm -rf $TD
if t musl_build cargo build --release --target x86_64-unknown-linux-musl -j32; then
  M=$TD/x86_64-unknown-linux-musl/release/consumer-bin
  rec musl_ok yes
  rec musl_bin_bytes "$(bytes $M)"
  rec musl_ldd "$(ldd $M 2>&1 | head -1)"
  rec musl_run "$($M 2>&1 | head -1)"
else
  rec musl_ok no
  rec musl_err "$(grep -E 'error|warning: .*failed|Failed' $LOG.musl_build | head -3 | tr '\n' '|' | cut -c1-400)"
fi
unset CARGO_TARGET_DIR

TD=$ROOT/$V/target-wasm; export CARGO_TARGET_DIR=$TD
rm -rf $TD
if t wasm_check cargo check --target wasm32-unknown-unknown -j32; then
  rec wasm_ok yes
else
  rec wasm_ok no
  rec wasm_err "$(grep -E '^error' $LOG.wasm_check | head -3 | tr '\n' '|' | cut -c1-400)"
fi
unset CARGO_TARGET_DIR
rec done "$(date -Is)"
