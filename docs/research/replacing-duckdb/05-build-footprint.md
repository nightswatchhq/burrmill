# 05 · Build footprint: bundled DuckDB, DataFusion, and Burrmill

Investigation 05, reported 2026-09-16 by a research agent (Fable 5.1), and run on the thinkpad. The
report is kept verbatim below. The harness, per-variant results and manifests are kept in
[probes/footprint](probes/footprint/README.md).

## Checked or corrected before filing

- **Nuthatch's real profile.** The agent could not see it (nuthatch is not on the thinkpad), so it
  was read here:
  - `[profile.dev] debug = "line-tables-only"`;
  - `.cargo/config.toml` sets `CXXFLAGS = "-g0"`;
  - release uses `lto = "thin"` and `strip = true`;
  - `duckdb` has features `bundled`, `parquet` and `json`.

  So nuthatch sits between the agent's `default` and `debug = 0` columns, much nearer the latter,
  especially on the C++ side. **The fair comparison for nuthatch is the `debug = 0` column.** The
  exact `line-tables-only` profile that burrmill#1 names was not measured.
- **The `sqllogictest` finding is confirmed, and it is fixed.** It was listed under
  `[dependencies]` in `crates/burrmill/Cargo.toml`, beneath a copy of the comment calling it a
  dev-dependency. It is only used by `tests/slt_corpus.rs`, and the bench crate declares its own.
  After removing it:
  - `cargo tree -p burrmill -e normal` no longer contains it;
  - `cargo test -p burrmill` passes (49 tests).

  The agent's variant D predates the fix.
- **Arrow's default features in Burrmill** (`arrow-json`, `csv`, `ipc`, and the ahash-on-wasm
  problem) have not been trimmed yet.
- **burrmill#1's third figure was not measured separately.** That figure is the size of a test
  executable that does *not* query the engine; all 20 test files here query it.

## Headlines for the plan

**Against burrmill#1's gate** (`debug = 0` column, the nearest to nuthatch's profile):

| figure | DuckDB (bundled, `-g0`) | DataFusion default | DataFusion trimmed | DF components (lower bound) | Burrmill today |
|---|---:|---:|---:|---:|---:|
| cold `target/` after `test --no-run` | 4,602 MB | 8,129 MB | 7,613 MB | 4,222 MB | **1,923 MB** |
| one querying test binary | 115 MB | 313 MB | 294 MB | 140 MB | **59 MB** |
| incremental: touch lib, `test --no-run` | 3 s | 5 s | 5 s | 3 s | **1 s** |
| release binary, stripped (default release profile) | 40 MB | 124 MB | 119 MB | 43 MB | **18 MB** |
| `ldd` beyond libc | libstdc++ | none | none | none | none |

- **A DataFusion-backed Burrmill loses the footprint argument outright.** Its test binaries are
  about 2.6x DuckDB's and its release binary about 3x. It would fail burrmill#1 on target size,
  test-binary size and incremental time. DuckDB's C++ shrinks 4x under `-g0`, while DataFusion's
  monomorphised Rust keeps its code. RFC-0042 §14 found only two realised wins from dropping
  DuckDB, and they were disk and binary size, so this is a real loss, not a rounding error.
- **Feature trims do not help (4-5%).** DataFusion switches on Arrow's and Parquet's default
  features regardless, including every codec and `zstd-sys`. `debug = 0` is the trim that matters,
  and nuthatch mostly has it already.
- **The component-crate route is roughly footprint-neutral with DuckDB:**
  - 43 MB stripped against 40;
  - 140 MB per test binary against 115;
  - a smaller target directory.

  But it is a lower bound: no physical planner, no optimizer crates. Getting there means Burrmill
  owns a physical planner, since `DefaultPhysicalPlanner` lives in the umbrella crate, about
  3k lines (investigation 02).
- **What DataFusion does win:** no libstdc++, glibc 2.34 instead of 2.38 when built on Debian 13,
  and no C++ cross-compiler. **musl still needs a C cross-compiler** for `zstd-sys`, whichever
  DataFusion shape is used.
- **Burrmill as it stands beats both by 3-4x on size and 5-8x on build time.**
  - `cargo check --target wasm32-unknown-unknown` passes once the consumer enables
    `getrandom/js`.
  - musl is blocked only by `psm`, which comes in through sqlparser's `recursive-protection`.
- **A shadow-mode nuthatch carries both engines, and two Arrows** (DuckDB pins arrow 58). The
  footprint gets worse before it gets better, so the shadow period should be short and
  feature-gated.

---

## The report, verbatim

# Build footprint: bundled DuckDB vs DataFusion vs Burrmill, for a Nuthatch-shaped consumer

Measured on tp (Debian 13, 32 cores, rustc 1.98.1), everything under `~/scratch/burrmill-footprint`. Five throwaway packages, each a lib, a release bin running one real query over a 5,000-row Parquet file, and 20 integration-test files. All four real engines return the same answer (96 parties, identical first row).

- **A** `duckdb = "=1.10504.0"`, features `bundled` + `parquet`. The brief said `bundled` only; in libduckdb-sys the Parquet extension is opt-in and a `bundled`-only build can't read Parquet without fetching the extension off the network, so `parquet` is what Nuthatch must be using.
- **B** `datafusion = "=55.0.0"`, default features. (Sub-crates resolve to 55.1.0 under the pinned umbrella.)
- **C** `datafusion` 55, `default-features = false, features = ["parquet", "sql"]`. Nothing else was needed for this query.
- **E** component crates only (datafusion-sql, -expr, -physical-plan, -datasource-parquet, -functions, -functions-aggregate), linked but not runnable: there is no physical planner outside `datafusion` core, so this is a lower bound for that route, not a working engine.
- **D** `burrmill` as a path dependency, as-is.

Two profiles: default dev, and `CARGO_PROFILE_DEV_DEBUG=0` with `CFLAGS=-g0 CXXFLAGS=-g0`. The `-g0` took: the nodebug DuckDB archive has 0 of 341 objects with `.debug_info`, the default one has all 341.

## Clean build wall time, seconds

| metric | A DuckDB | B DF default | C DF trimmed | E DF components | D Burrmill |
|---|---:|---:|---:|---:|---:|
| dev build -j32 | 116 | 123 | 152 | 38 | 14 |
| dev build -j8 | 173 | 126 | 128 | 38 | 30* |
| dev test --no-run -j32 | 125 | 134 | 132 | 45 | 16 |
| dev test --no-run -j8 | 182 | 136 | 198 | 46 | 17 |
| dev debug=0 build -j32 | 81 | 119 | 117 | 34 | 16 |
| dev debug=0 build -j8 | 128 | 122 | 119 | 36 | 16 |
| dev debug=0 test --no-run -j32 | 80 | 122 | 121 | 36 | 14 |
| dev debug=0 test --no-run -j8 | 131 | 125 | 123 | 38 | 34* |
| release build -j32 | 148 | 198 | 194 | 87 | 44 |

\* Noise. Rerun 3x each: D dev build is 14.2-14.6 s at -j32 and 15.1-15.2 s at -j8. D and the DataFusion variants are critical-path bound (the arrow chain), so -j8 vs -j32 barely matters; only DuckDB's 341 C++ objects use the cores. Single runs otherwise, expect +-10%.

## Sizes after `cargo test --no-run`, MB

| metric | A DuckDB | B DF default | C DF trimmed | E DF components | D Burrmill |
|---|---:|---:|---:|---:|---:|
| dev target/ | 24,160 | 26,734 | 25,500 | 18,983 | 6,849 |
| dev one test binary | 768 | 1,117 | 1,068 | 778 | 262 |
| dev all 20 test binaries | 15,351 | 22,347 | 21,354 | 15,551 | 5,250 |
| dev debug=0 target/ | 4,602 | 8,129 | 7,613 | 4,222 | 1,923 |
| dev debug=0 one test binary | 115 | 313 | 294 | 140 | 59 |
| dev debug=0 all 20 test binaries | 2,295 | 6,268 | 5,874 | 2,791 | 1,176 |
| release binary | 50 | 175 | 165 | 60 | 23 |
| release binary, stripped | 40 | 124 | 119 | 43 | 18 |

The brief's DuckDB figures were wrong in both directions: the bundled archive is 2.36 GB in a dev build (not 1.7), 548 MB with `-g0`, 123 MB in release; a dev test binary is 768 MB, not 250. Nuthatch's 250 MB suggests it already has reduced debuginfo or a different profile; I could not check, Nuthatch isn't on this Mac.

## Incremental rebuild at -j32, seconds

| metric | A DuckDB | B DF default | C DF trimmed | E DF components | D Burrmill |
|---|---:|---:|---:|---:|---:|
| dev touch lib, test --no-run | 15 | 16 | 14 | 10 | 2 |
| dev touch lib, build | 2 | 2 | 2 | 1 | 1 |
| dev touch one test, test --no-run | 11 | 13 | 12 | 8 | 2 |
| dev debug=0 touch lib, test --no-run | 3 | 5 | 5 | 3 | 1 |
| dev debug=0 touch one test, test --no-run | 3 | 4 | 4 | 3 | 1 |

Touching one test file relinks only that binary; the cost is the link of a ~1 GB binary, hence the DataFusion and DuckDB numbers.

## Portability

| metric | A DuckDB | B DF default | C DF trimmed | E DF components | D Burrmill |
|---|---|---|---|---|---|
| crates in release graph | 93 | 329 | 284 | 268 | 143 (82 without sqllogictest) |
| ldd beyond libc/libm/libgcc_s | libstdc++.so.6 | none | none | none | none |
| GLIBCXX / GLIBC needed | 3.4.29 / 2.38 | - / 2.34 | - / 2.34 | - / 2.34 | - / 2.34 |
| C/C++ in graph | libduckdb-sys | zstd-sys, liblzma-sys, libbz2-rs-sys, psm | zstd-sys | zstd-sys | psm only |
| musl build | fails: no `x86_64-linux-musl-g++` for libduckdb-sys | fails: `x86_64-linux-musl-gcc` for zstd-sys | same | same | fails: `x86_64-linux-musl-gcc` for psm (stacker, via sqlparser `recursive-protection`) |
| wasm32 check | fails (crossterm via comfy-table via arrow-cast 58; C++ would never build anyway) | fails: getrandom 0.3 | fails: getrandom 0.3, then zstd-sys even with the cfg fix | fails: uuid | fails: getrandom 0.2 via ahash; **passes** with `getrandom = { version = "0.2", features = ["js"] }` in the consumer |

tp has no musl C toolchain and I did not install one. That is itself the finding: every variant with any C in the graph needs a cross C compiler for musl; D's only C is `psm`, and turning off sqlparser's `recursive-protection` would remove it. DuckDB also pins Arrow 58, so a Nuthatch carrying both engines links two Arrows.

## Commands

```
rsync -az --exclude target/ --exclude .git/ ~/Projects/burrmill/ tp:~/scratch/burrmill-footprint/burrmill/
# per variant, ./run.sh <V>, which does for profile in default nodebug (nodebug: CARGO_PROFILE_DEV_DEBUG=0 CFLAGS=-g0 CXXFLAGS=-g0), CARGO_TARGET_DIR=target-<profile>:
rm -rf $TD; cargo build -j8;  rm -rf $TD; cargo test --no-run -j8
rm -rf $TD; cargo build -j32; rm -rf $TD; cargo test --no-run -j32
du -sb $TD; stat -c %s $TD/debug/deps/t[0-9][0-9]-*   # executables only
touch src/lib.rs; cargo test --no-run -j32; touch src/lib.rs; cargo build -j32; touch tests/t07.rs; cargo test --no-run -j32
cargo build --release -j32; strip; ldd; objdump -T | grep -o 'GLIBC\(XX\)\?_[0-9.]*'
cargo build --release --target x86_64-unknown-linux-musl -j32
cargo check --target wasm32-unknown-unknown -j32
cargo tree -e normal --prefix none | sort -u | wc -l
```

Wall time via `date +%s.%N` around each command. Everything is left in `~/scratch/burrmill-footprint` on tp (`results/<V>.txt`, per-step logs, all target dirs, roughly 150 GB; safe to delete). `~/Projects` on tp untouched; nothing committed.

## Verdict

**A DataFusion-backed Burrmill loses to bundled DuckDB on every footprint axis except the C++ toolchain ones.** In default dev, DataFusion test binaries are 1.4x DuckDB's (1.07-1.12 GB vs 768 MB), the target tree is 5-10% bigger, clean builds are equal or slower, and incremental relinks are equal. With `debug = 0` the gap widens: DataFusion test binaries are 2.6x DuckDB's (294-313 MB vs 115 MB), because `-g0` shrinks DuckDB's C++ by 4x while DataFusion's monomorphised Rust keeps its code. Release binaries are 3x DuckDB's (119-124 MB vs 40 MB stripped). DataFusion wins only on `ldd` (no libstdc++, glibc 2.34 not 2.38) and on not needing a C++ cross-compiler; it still needs a C one.

**Burrmill as it stands beats both, by 3-4x on every size and 5-8x on every build time**: 262 MB dev test binary vs 768/1,068, 5.2 GB for 20 vs 15.4/21.4, 14 s clean build vs 116/152, 18 MB stripped release vs 40/119, 2 s incremental vs 11-16.

**Which DataFusion trim matters:** none of the feature trims. `default-features = false` with `parquet, sql` saved 4-5% on size and nothing on time, because DataFusion declares `arrow` and `parquet` with their default features on (IPC, CSV, JSON, prettyprint; brotli, flate2, lz4, zstd), so those and `zstd-sys` come regardless. The component-crate route builds 3x faster and links at 778 MB dev, still 3x Burrmill and no smaller than DuckDB, and it isn't an engine without a planner. The trim that matters is `[profile.dev] debug = 0`: 3.3-3.5x on DataFusion, 6.7x on DuckDB, 4.5x on Burrmill. Nuthatch should set it regardless of engine.

Two Burrmill-side items, both cheap: `sqllogictest` sits under `[dependencies]` as well as `[dev-dependencies]` in `crates/burrmill/Cargo.toml` (61 of 143 shipped crates come only through it; moving it cuts the release build 44 to 34 s, sizes unchanged), and arrow's default features pull `arrow-json`/`csv`/`ipc` and the ahash-on-wasm problem for nothing Burrmill uses.

**Not measured:** Nuthatch's actual profile and DuckDB feature set (repo not on this Mac); a musl build with a real musl C/C++ toolchain; whether DataFusion's `datafusion-functions-*` monomorphisation dominates the 1 GB (no `cargo-bloat` pass); macOS. Timings are single runs on a laptop except where noted.
