# Burrmill

**SQL over sealed Parquet segments plus a live tip, with exact integer arithmetic and
refuse-on-overflow, faster than DuckDB on the queries an indexer actually runs. One binary, nothing
to configure.**

Status: **slice 1** of [RFC-0044](docs/rfc/RFC-0044-burrmill.md). One owned plan shape, one owned
operator, the allowlist, and a head-to-head harness against both incumbents. Not usable as a general
query engine and not trying to be.

## What it is

A query engine-*layer*. Burrmill owns the semantics **and** the execution of a deliberately small
admitted subset of SQL - the shapes a blockchain indexer runs on its hot path - all the way down to
the vectorised operator. It rents Arrow for the in-memory format and its kernels, and parquet-rs for
decode, permanently and without embarrassment: nobody solo-maintains a better SIMD kernel library,
and there is no evidence that trying would pay.

It is not a general database. It needs to be faster than DuckDB on *these* shapes over *this*
layout, and honest about everything else.

## The three claims

**Faster.** The operator's ancestor measured 0.55-0.85x DuckDB across 24 of 24 configurations on
`net_balances`, where general DataFusion measured 2.53-2.80x *slower* on the identical query. That
three-to-fivefold swing between renting general execution and owning a specialised one is the whole
architectural argument. `cargo run -p burrmill-bench --release` re-runs it, parity first.

**Exact.** Integer overflow returns `BurrmillError::Overflow`, never a wrapped number. Said precisely,
because a generated corpus made the difference visible: it refuses when an intermediate **partial
sum** leaves `i128`, not only when the answer does, so a party whose values are `MAX, +1, -1` is
declined even though the true sum is exactly `MAX`. DuckDB does the same, in both directions - each
engine refuses cases the other answers. No wrong number is ever returned either way, which is the
guarantee that matters; "refuses on overflow" is simply a little more eager than it reads. DataFusion's
integer arithmetic silently wraps - `SELECT 10000000000 * 10000000000` yields `7766279631452241920`
where Postgres, Trino and Snowflake all raise - and as of August 2026 there is still no core config
flag to stop it (issues #17539, #14771, #20034, all open). Worse, it is inconsistent by operation:
`%` errors on `i32::MIN % -1` while `+` wraps on `i32::MIN + -1`.

DuckDB is better and errors on `HUGEINT` overflow, but it is **not watertight**, and the generated
corpus found where. Credit one party with `i128::MAX` and then `1`, across two files, with two
threads, and DuckDB returns **`i128::MIN`** for a sum whose true value is `MAX + 1`: a wrapped
balance, silently. At one thread, or over a single file, the same query refuses correctly - so the
check is in the single-threaded path and missing from the partial-aggregate combine, which means it
only goes wrong once the data is large enough to parallelise. Measured on libduckdb-sys 1.10501.0;
`cargo run -p burrmill-bench --release -- duckdb-gaps` reproduces it across the grid.

Refusing, everywhere, is a guarantee neither of them offers.

**Closed.** Table names resolve against a positive allowlist of registered providers, and Burrmill
registers **no file-I/O SQL functions at all** - no `read_parquet`, no `read_csv`, no `COPY TO`, no
`getenv`. `read_parquet('/etc/passwd')` does not fail a check; it has nowhere in the grammar to
parse to. That is a different claim from a denylist, which is the model that let DuckDB's
`sniff_csv` keep reading the filesystem with `enable_external_access=false` set (CVE-2024-41672) and
that made Grafana's DuckDB-backed SQL Expressions a CVSS 9.9 local file read (CVE-2024-9264).

## What it does not do yet

Said plainly, because a README that implies otherwise is the thing this project is against.

- **One plan shape.** Signed union fold - one table read twice, one column crediting and one
  debiting the same signed value, grouped by the party. Everything else is `NotAllowed`.
- **Cold segments only.** The redb hot tip and the hot/cold seam are the next slice, and the seam is
  the highest-risk invariant in the design.
- **Two `TRY_CAST` divergences from DuckDB, one fixed and one open.** Surrounding whitespace is now
  trimmed, as DuckDB does - it was silently dropping the row and returning a short balance. DuckDB
  also accepts `1e18`, `7.0` and `1_000`, and rounds `7.9` to **8**, where Burrmill returns NULL.
  Adopting silent rounding into an engine whose first claim is exactness is a decision, not a patch.
  `cargo run -p burrmill-bench --release -- cast` prints the whole divergence table.
- **No streaming.** A fold's result is materialised; it is one row per party, so this costs little
  today and will be revisited with the async cancellation contract.
- **No DataFusion fallback.** Deliberately not yet built. Building the fast path first is what makes
  the go/no-go cheap.

## Layout

    crates/burrmill        the library. Pure Rust: arrow, parquet, rustc-hash, rayon, sqlparser.
                           No DuckDB. No DataFusion. No C++.
    crates/burrmill-bench  publish = false. Where the oracles live, so they can never reach the
                           shipped graph.

## Running the gate

```sh
# Synthetic fixture, nest-shaped (bimodal segment sizes), parity checked before any timing.
ROWS=2000000 SEGMENTS=100 REPEATS=5 cargo run -p burrmill-bench --release

# The high-cardinality gate - where DataFusion's two-phase aggregation is weakest.
ROWS=2000000 SEGMENTS=100 ADDRS=1000000 REPEATS=5 cargo run -p burrmill-bench --release

# Prove the parity guard actually refuses. No RESULT line must be printed.
BREAK_PARITY=1 cargo run -p burrmill-bench --release

# Page-cache order is a confound; a ratio that survives both orderings is about the engines.
ORDER=burrmill_first cargo run -p burrmill-bench --release

# Against real sealed segments, read-only.
cargo run -p burrmill-bench --release -- inspect /path/to/nest/segments
cargo run -p burrmill-bench --release -- explain /path/to/nest/segments
```

## Licence

MIT OR Apache-2.0.
