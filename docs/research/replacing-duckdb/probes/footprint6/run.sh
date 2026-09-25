#!/usr/bin/env bash
# Stage 6.0 footprint spike. Nuthatch-shaped: line-tables-only, CXXFLAGS=-g0, six
# test binaries (four query, two do not), thin-LTO stripped release.
# Usage: run.sh [duck|umbrella|components|burrmill]
set -eu
export PATH=$HOME/.cargo/bin:$PATH
export CARGO_TERM_COLOR=never
export CFLAGS=-g0 CXXFLAGS=-g0
ROOT="$(cd "$(dirname "$0")" && pwd)"
V=${1:?variant}
cd "$ROOT/$V"

if [ "$V" = components ] && [ ! -f src/generated/physical_planner.rs ]; then
  cargo fetch
  python3 "$ROOT/rewrite_planner.py"
fi

mkdir -p "$ROOT/results"
OUT="$ROOT/results/$V.txt"
LOG="$ROOT/results/$V.log"
: > "$OUT"; : > "$LOG"
rec() { echo "$1=$2" | tee -a "$OUT"; }
now() { date +%s.%N; }
t() {
  local key=$1; shift
  local s; s=$(now)
  echo "### $key: $*" >> "$LOG"
  if "$@" >> "$LOG" 2>&1; then
    local rc=0
  else
    local rc=$?
  fi
  local e; e=$(now)
  rec "$key" "$(awk -v a="$s" -v b="$e" 'BEGIN{printf "%.1f", b-a}')"
  [ "$rc" -ne 0 ] && rec "${key}_rc" "$rc"
  return "$rc"
}
bytes() { stat -c %s "$1"; }
onebin() {
  find "$1/debug/deps" -maxdepth 1 -type f -name "$2-*" ! -name '*.d' -perm -u+x | head -1
}

rec host "$(hostname) $(nproc) cores $(rustc -V)"
rec variant "$V"
rec profile "line-tables-only CXXFLAGS=-g0 nuthatch-shaped 6 tests"
rec deps_normal "$(cargo tree -e normal --prefix none 2>/dev/null | sort -u | wc -l | tr -d ' ')"

TD="$ROOT/$V/target-dev"
export CARGO_TARGET_DIR=$TD
rm -rf "$TD"
t test_clean_j32 cargo test --no-run -j32
rec target_bytes "$(du -sb "$TD" | cut -f1)"
QB=$(onebin "$TD" t01)
NB=$(onebin "$TD" t05)
rec testbin_query_bytes "$(bytes "$QB")"
rec testbin_noquery_bytes "$(bytes "$NB")"
rec testbin_count "$(find "$TD/debug/deps" -maxdepth 1 -type f -name 't0*-*' ! -name '*.d' -perm -u+x | wc -l | tr -d ' ')"
touch src/lib.rs
t incr_lib_test cargo test --no-run -j32
unset CARGO_TARGET_DIR

TD="$ROOT/$V/target-release"
export CARGO_TARGET_DIR=$TD
rm -rf "$TD"
t release_build_j32 cargo build --release -j32
B="$TD/release/consumer-bin"
rec release_bin_bytes "$(bytes "$B")"
rec release_ldd "$(ldd "$B" | awk '{print $1}' | paste -sd' ')"
rec release_glibcxx "$(objdump -T "$B" 2>/dev/null | grep -o 'GLIBCXX_[0-9.]*' | sort -uV | tail -1 || true)"
rec release_glibc "$(objdump -T "$B" 2>/dev/null | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1 || true)"
# A variant reading nuthatch's segment naming carries its own `segments/`, linked to the same file.
FX="$ROOT/fixture"; [ -d "$ROOT/$V/segments" ] && FX="$ROOT/$V/segments"
rec release_run "$("$B" "$FX" 2>&1 | head -1)"
unset CARGO_TARGET_DIR
rec done "$(date -Is)"
