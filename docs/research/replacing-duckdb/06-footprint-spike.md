# 6.0 · Footprint spike: component crates plus an owned planner

Measured 2026-09-17 on the thinkpad (Debian 13, 32 cores, rustc 1.98.1). The
harness is `docs/research/replacing-duckdb/probes/footprint6/`. Raw
`key=value` lines are in `probes/footprint6/results/{duck,umbrella,components}.txt`.

This is phase 0 of the replacing-DuckDB plan. It decides whether a DataFusion-backed
Burrmill ships on component crates (C) or the umbrella crate (U), and whether
either of them passes burrmill#1.

## Corrections before the table

- Investigation 02 costed `DefaultPhysicalPlanner` at about 3k lines. The non-test
  body in datafusion 55.0.0 is 3,278 lines (5,762 with tests). C copies that file,
  stubs the `COPY` arm (umbrella-only file sinks), and builds it against the
  component crates. It is not a 200-line subset planner.
- Investigation 05's "component crates are about even with DuckDB" was variant E:
  the crates linked, the query not run, `debug = 0`. That bound does not survive a
  working planner in nuthatch's real profile.

## What was measured

A throwaway consumer with nuthatch's actual shape, not 05's:

- `[profile.dev] debug = "line-tables-only"`
- `CFLAGS=-g0 CXXFLAGS=-g0`
- six integration test binaries: four call the engine, two do not
- `[profile.release] lto = "thin", strip = true`
- DuckDB `1.10504.0` with `bundled`, `parquet`, `json`
- DataFusion 55.0.0
- the same 5,000-row Parquet fixture and the same net-balances SQL

All three engines returned the same answer: **90 parties, first =
`(0x…0001, 3185)`**. C is a working engine, not a link-cost lower bound. The
planner is the full `DefaultPhysicalPlanner` (joins, windows, aggregates, the
lot), so the 11 statements from 04 are in scope; this spike timed the 05
net-balances query, not those 11. DuckDB features are `bundled + parquet + json`;
nuthatch also enables `vscalar` / `vscalar-arrow`, which would make DuckDB
larger and are not in this comparison.

C's public surface is a concrete `net_balances(dir) -> Vec<(String, String)>`.
Generics instantiate in the consumer lib, once.

## burrmill#1's four figures

| figure | duck | umbrella (U) | components (C) | C / duck | U / duck |
|---|---:|---:|---:|---:|---:|
| cold `cargo test --no-run` target | 3,269 MB | 4,482 MB | 4,142 MB | **1.27x** | 1.37x |
| querying test binary | 162 MB | 493 MB | 446 MB | **2.75x** | 3.04x |
| non-querying test binary | 7.2 MB | 7.2 MB | 7.2 MB | 1.00x | 1.00x |
| incremental `test --no-run` after touch lib | 1.1 s | 1.9 s | 1.9 s | **1.73x** | 1.73x |

Plus the figures 05 published, restated on this profile:

| figure | duck | U | C |
|---|---:|---:|---:|
| clean `test --no-run` wall | 77 s | 121 s | 87 s |
| release binary (already stripped) | 41 MB | 127 MB | 105 MB |
| crates in the normal graph | 93 | 329 | 281 |
| `ldd` beyond libc/libgcc_s/libm | libstdc++ | none | none |
| GLIBC / GLIBCXX | 2.38 / 3.4.29 | 2.34 / — | 2.34 / — |

## Gate

C is **not** within burrmill#1. Three of four figures fail. The querying test
binary is the one that would refill a disk: 446 MB against DuckDB's 162 MB, on
every integration test that actually runs SQL.

C **does** beat U, on every size axis, by 8–17%. Clean compile is 87 s against
U's 121 s and DuckDB's 77 s. The owned planner is not the expensive part of U;
the umbrella's extra datasources and default features are, and they are modest.

E at `debug = 0` was 140 MB per test binary and 43 MB stripped, against DuckDB
115 and 40. A working planner in `line-tables-only` is 446 and 105. Instantiating
the operators, not listing the crates, is what the lower bound omitted.

The non-querying binary is 7.2 MB for all three: `--gc-sections` drops an unused
engine. That figure will not look like this inside nuthatch, where the library
references the engine from serving code the tests all link.

## Verdict

**C is the DataFusion route, if there is one.** It works, it is smaller than U,
and U has no footprint argument left once C exists.

**Neither route passes burrmill#1.** Replacing DuckDB with a DataFusion-backed
Burrmill is a trade: 2.75× querying test binaries, 2.6× release binary, 1.27×
dev target, in exchange for no libstdc++, glibc 2.34, a panic confined to the
query, and the exactness and allowlist work that is phase 1.

That trade is an RFC-0042 amendment with these numbers, signed by Chief, or
the swap does not proceed. The amendment is not written here.

Phase 1, if the trade is accepted, builds on C: `NestCatalog`, lockdown,
`CheckedArithmetic`, the encoder, the dialect, `FoldSubstitution`. The public
API stays concrete. DataFusion stays behind a feature flag so a nest that only
runs owned folds never compiles it.
