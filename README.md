# Burrmill

**SQL over sealed Parquet segments plus a live tip, with exact integer arithmetic and
refuse-on-overflow, faster than DuckDB on the queries an indexer actually runs. One binary, nothing
to configure.**

Status: **slice 1 of [RFC-0044](docs/rfc/RFC-0044-burrmill.md), gate passed.** One owned plan shape,
one owned operator, the allowlist, a generated corpus against two oracles, and a head-to-head harness
against both incumbents. Not usable as a general query engine and not trying to be.

The gate is "≤1.0x DuckDB at exact parity under 256 MB peak RSS", and both legs are met **at eight
threads per query, with all three engines held to the same budget**: 0.38-0.87x across fourteen
configurations with parity verified on every one, and 210 MB at 989,690 groups. The parallelism is
part of the claim rather than a footnote, because the same binary measures 145 MB on one thread and
340 on thirty-two, and a budget that depends on the host's core count is not a budget.

## What it is

A query engine-*layer*. Burrmill owns the semantics **and** the execution of a deliberately small
admitted subset of SQL - the shapes a blockchain indexer runs on its hot path - all the way down to
the vectorised operator. It rents Arrow for the in-memory format and its kernels, and parquet-rs for
decode, permanently and without embarrassment: nobody solo-maintains a better SIMD kernel library,
and there is no evidence that trying would pay.

It is not a general database. It needs to be faster than DuckDB on *these* shapes over *this*
layout, and honest about everything else.

## The three claims

**Faster.** On a real nest's own authored views — the actual queries, not a synthetic stand-in —
**0.80x, 0.95x and 1.01x** DuckDB over 6,000 to 9,745 sealed segments, parity verified, same files
and same eight threads (`burrmill-bench views`). That test did not exist until recently and the
claim did not survive it first time: re-reading immutable Parquet footers cost 57-93 ms of every
query and the ratios were 1.20-1.38x until they were cached.

On the synthetic sweep: 0.38-0.87x DuckDB across fourteen configurations, parity verified on every one, on a
twelve-column nest-shaped fixture with every engine on the same eight threads. On the same runs
general DataFusion measures 3.6x DuckDB at ten thousand segments — the many-small-files layout a nest
actually produces — while beating it at high cardinality. That swing between renting general
execution and owning a specialised one is the whole architectural argument.
`cargo run -p burrmill-bench --release` re-runs it, parity first.

**Exact.** Integer overflow returns `BurrmillError::Overflow`, never a wrapped number. Said precisely,
because a generated corpus made the difference visible: it refuses when an intermediate **partial
sum** leaves `i128`, and the answer decides. A party whose values are `MAX, +1, -1` sums to exactly
`MAX` and is returned; an entry whose running total wanders outside the range carries a high word
until the rows are produced. It costs nothing when nothing overflows, which is always. DuckDB still
refuses that case, in both directions, so the two engines disagree about which queries are
*answerable* even where neither returns a wrong number. DataFusion's
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
- **A narrower value domain than DuckDB, deliberately.** Surrounding whitespace is trimmed, as DuckDB
  does; it was silently dropping the row and returning a short balance. But DuckDB also reads
  `1e18`, `7.0` and `1_000`, and **rounds `7.9` to 8**, and Burrmill will not guess at any of them: a
  value that carries digits but is not a canonical integer is **refused, loudly, naming the value**.
  Diverging out loud costs a query; diverging silently costs someone's answer, and silently agreeing
  would mean adopting rounding into an engine whose first claim is exactness.
  `cargo run -p burrmill-bench --release -- cast` prints the divergence table.
- **Eight threads per query by default.** `Limits::max_threads`. Deliberate: the cores past it buy
  6% and cost the concurrency story that is most of why a serving path wants this.
- **It does not yet win under concurrent load, and that was expected to be the easy part.** RFC-0044
  §3.5 reasoned that DuckDB's single-connection mutex - throughput flat from one client to
  thirty-two, p99 29.5 ms to 7066 ms - made this the project's easiest headline. Measured: that
  reproduces exactly for DuckDB *embedded the way nuthatch embeds it*, but embedded its own way, one
  database with a connection per client, DuckDB reaches **171 qps at 32 clients where Burrmill
  manages 96**, with a worst-client p99 of **423 ms against 2378 ms**, and it serves every client
  where Burrmill *used to* starve some outright. The absence of a lock was never the whole story:
  what replaced it is a bounded shared pool, and a pool that hands every worker to one query at a
  time starves the queue just as a mutex does — at a twenty-second window one client completed 220
  queries and another completed **none**.
  
  A FIFO admission gate (roadmap 5.3) fixed the liveness half: fairness 0.00 to **0.81**, worst-client
  p99 **2378 ms to 601 ms**, every client served, for eight per cent of throughput. The throughput
  half is now partly closed too (5.3a): a sharing query takes a slice of the pool rather than all of
  it, giving 100 qps and 0.94 fairness at 32 clients, and **beating DuckDB outright at four**.
  Burrmill wins outright below eight clients (**133-143 qps against 110** at four), is markedly fairer at every count
  (0.89 against 0.57 at thirty-two) and has a comparable tail. It trails on raw throughput above
  sixteen clients: 135 against 160. That gap is diagnosed and not fixed — it is utilisation rather
  than work, since the fold costs 53 ms on one thread against DuckDB'''s implied 48, but eight threads
  buy only 3.3x.
  `docs/bench/serve-concurrency.txt` has the numbers;
  `cargo run -p burrmill-bench --release -- serve <fixture>` re-runs them.
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

# Against real sealed segments, read-only. A ratio that is mostly fixed cost is not printed as a
# number; the field reads UNSAFE_fixed_duck=NNpct instead. See docs/bench/method.md.
cargo run -p burrmill-bench --release -- inspect /path/to/nest/segments
cargo run -p burrmill-bench --release -- explain /path/to/nest/segments
cargo run -p burrmill-bench --release -- nest /path/to/nest/segments <table-prefix>
```

## Checking it is right

Correctness has its own commands, and they are not the benchmark. `cargo test` runs the fast half of
this on every invocation.

```sh
# Generated cases against DuckDB: answers, refusals, and the ones where the two disagree about
# whether the query is answerable at all.
CASES=3000 cargo run -p burrmill-bench --release -- gen

# The hand-computed .slt corpus, pointed at the other engine. It runs against Burrmill in `cargo
# test`; this is the same files against DuckDB, which is the reason for using a standard format.
cargo run -p burrmill-bench --release -- slt

# Where Burrmill's TRY_CAST and DuckDB's disagree, printed rather than assumed.
cargo run -p burrmill-bench --release -- cast

# DuckDB silently wrapping a HUGEINT sum, reproduced across threads and file counts.
cargo run -p burrmill-bench --release -- duckdb-gaps

# The seal-layout canary against a real nest: naming, schema, and every table opening.
BURRMILL_NEST=/path/to/nest/segments cargo test --test seal_layout -- --nocapture

# More generated cases than the default, or one exact case again.
BURRMILL_CASES=5000 cargo test --test generated_folds
BURRMILL_SEED=1234 cargo test --test generated_folds
```

## Licence

MIT OR Apache-2.0.
