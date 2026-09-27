# Burrmill

**SQL over sealed Parquet segments plus a live tip, in DuckDB's dialect, with exact integer
arithmetic that refuses rather than wraps. Faster than DuckDB on the queries an indexer actually
runs, at a third of the memory, in one Rust binary with no C++ in it.**

Status, 2026-09-26: **Gate 1 of [RFC-0044 Amendment 2](docs/rfc/RFC-0044-burrmill.md) is taken as passed, and the
migration of nuthatch from DuckDB to Burrmill is decided.** Every authored view of a real production
nest is byte-identical to DuckDB's answer, the set runs at 0.65x DuckDB's time, the memory gate
passes as nuthatch would run it, and the larger build footprint has been accepted as the price of
taking DuckDB's C++ out of nuthatch. The plan with its tick lists is
[docs/research/replacing-duckdb/migration.md](docs/research/replacing-duckdb/migration.md); the
measurements behind every number below are in [docs/progress-log.md](docs/progress-log.md), newest
first, with transcripts under [docs/bench/](docs/bench/). Nothing in nuthatch runs on Burrmill yet:
that is phase 2, and it started on 2026-09-26.

## What it is

Two layers, one crate.

**The engine** (`burrmill::Engine`, feature `datafusion`) is DataFusion 55 as the host planner and
executor, with Burrmill owning the parts DataFusion gets wrong for an indexer:

- **`CheckedArithmetic`**: every sum, product and cast in a plan is rewritten to refuse on overflow
  instead of wrapping, exact through CTEs, joins and windows, `TRY_CAST` tainted and its sums made
  exact through unions and negation (`tests/df_checked.rs`).
- **DuckDB's dialect**: authored views are DuckDB SQL and stay that way. Integer literals are fitted
  as DuckDB fits them (`UBIGINT - 1` stays `UBIGINT`), casts round as DuckDB's do (half to even from a
  float, half away from a decimal), `DECIMAL` to `DOUBLE` reproduces two DuckDB roundings that arrow
  does correctly, `HUGEINT` is `DECIMAL(38,0)`, `list_reduce` is DuckDB's, list comprehensions and
  `IS DISTINCT FROM` parse as DuckDB parses them, and identifiers resolve without case.
- **Plan shapes DataFusion lacks**: a range join for `x >= start AND x <= until` (DataFusion runs it
  as a nested loop), top-per-group, a distinct split, shared repeated subqueries, DuckDB's
  `MoveConstants`, correlated subqueries decorrelated by key, and repartitions removed where the
  scans beneath read under 4 MiB.
- **`FoldSubstitution`**: where a plan contains the signed-union `GROUP BY` an indexer's balance
  rebuilds are made of, the owned operator below is swapped in, provably equal, visible in `EXPLAIN`.
- **A catalogue that knows the nest and nothing else**: `Engine::open_nest` reads the seal manifest,
  offers `_dec` and `_overflow` beside every wide column as nuthatch does, and registers no table
  factory and no file function. An unconfigured DataFusion `SessionContext` will read `/etc/hosts`.

**The operator** (`burrmill::Burrmill`, the default build) is the vectorised partitioned fold that
the RFC began with: arrow for the format and kernels, parquet-rs for decode, checked `i128`
arithmetic, eight threads per query, a FIFO admission gate. It runs alone for the shapes it admits
and inside the engine's plans for everything else.

**The parser role** (`burrmill::inspect::reach`, default build) is nuthatch's allowlist and
reachability rules on sqlparser's AST, failing closed.

## The three claims

**Faster.** Measured 2026-09-24 and 25 on a 32-core ThinkPad against a 1,925-segment copy of the
graph-allocations nest that Lodestar reads in production, DuckDB set up exactly as nuthatch sets it
up, every view's whole output compared as a multiset of nuthatch-encoded rows before any timing was
printed (`burrmill-bench engine-views`, `docs/bench/phase1b-thinkpad.txt`,
`docs/bench/rangejoin-thinkpad.txt`):

| | DuckDB | Burrmill |
|---|---:|---:|
| authored views byte-identical | | **22 of 22** |
| time-weighted over the 22 | 39.6 s | **0.65x** |
| views within 1.5x of DuckDB | | **22 of 22** |
| worst view | | `lodestar_disputes`, 8 rows, 0.80x |

Re-run on 2026-09-27 with everything since in: 22 of 22 identical, **0.63x**, 22 of 22 within 1.5x
(`docs/bench/engine-views-thinkpad-2026-09-27.txt`). It was not always so: on 2026-09-24 the same
harness measured 1.54x, with stock DataFusion at 1.81x on the same nest, and the owned plan shapes
above are what closed it. An earlier 0.71x was measured on a 38,428-segment copy where DuckDB pays
per file; the compacted layout is the fairer test and is the one quoted.

Serving the 12 views that were portable on 2026-09-24 to 1, 4, 16 and 32 concurrent clients
(`burrmill-bench serve-views`, `docs/bench/serve-views-thinkpad.txt`): **14.7 against 7.2 qps at
one client and 32.9 against 15.1 at 32**, worst p99 1,350 ms against 7,424, fairness 0.90 against
0.00, and 3.9 GB of process memory against 15.4, all at 32 clients with `ulimit -n` raised, since
DuckDB fails outright at the default. Not re-run over the 22.

**Exact.** Integer overflow returns an error, never a wrapped number, on both layers. The owned fold
refuses when an intermediate partial sum leaves `i128`; the engine's `CheckedArithmetic` makes the
same promise on DataFusion's plans, where stock DataFusion silently wraps
(`SELECT 10000000000 * 10000000000` is `7766279631452241920`, and as of August 2026 there is no core
flag to stop it: #17539, #14771, #20034). DuckDB errors on `HUGEINT` overflow but not watertight: in
the 1.5 line a `SUM` over two files on two threads returns `i128::MIN` for a true `MAX + 1`
([duckdb#24081](https://github.com/duckdb/duckdb/issues/24081), fixed on `main` after 1.5.5;
`burrmill-bench duckdb-gaps` reproduces it). The differential fuzzer also found a DuckDB wrong answer
nobody had reported, a text comparison against a `TIMESTAMPTZ` cast dropping every row once the
session's zone has been consulted, written up in
[docs/upstream/](docs/upstream/duckdb-cast-comparison-null-constant.md) and not yet filed.

**Closed.** The owned path resolves table names against a positive allowlist and registers no
file-I/O function at all: `read_parquet('/etc/passwd')` has nowhere in the grammar to parse to. The
engine refuses anything but `SELECT` at the surface, registers no table factories, and had
`input_file_name()` removed after a path leak (6.2). `reach` agrees with nuthatch's own walk over
DuckDB on 38 of 46 security-corpus statements and is stricter on the other 8, never looser
(`burrmill-bench reach-parity`).

## How it is checked

The confidence comes from running both engines on the same statements, not from reading either.

- **`burrmill-bench fuzz`**: SQL drawn from a typed grammar over awkward data, run on DuckDB and on
  the engine, answers compared as multisets. `CASES=<n> SEED=<n>`; a difference is reported with the
  seed that reproduces it (`SEED=<n> CASES=1 PRINT=1`), and `SQL=<query>` runs one statement. It
  found nine faults in Burrmill on its first day, three of them silent wrong answers, then a
  DataFusion wrong answer on subqueries used as predicates, then the two DuckDB findings above. The
  last runs: 25,000 cases on five seeds, 8,000 on four, 6,000 on three, each with no differing answer
  (`docs/bench/fuzz.txt`).
- **Parity harnesses**, all against DuckDB: `dialect-parity` 209/209 statements, `encode-parity`
  17/17 (nuthatch's JSON, byte for byte), `error-parity` 14/14 (nuthatch's error classes),
  `reach-parity` 38/46 identical and 8 stricter, `rewrite-parity <nest> <views-dir>` for a rewritten
  view against its original.
- **The memory gate**: `examples/hosted_fold`, the fold in a binary shaped like nuthatch after the
  swap, **241 to 245 MB peak RSS at 989,690 groups** against the 256 MB gate, DuckDB at 572 to 598 MB
  on the same fold (`docs/bench/memory-gate-thinkpad-2026-09-25.txt`). The bench binary reads 20 MB
  higher because it links DuckDB and the umbrella crate as oracles, and their code pages count.
- **The generated fold corpus** (`burrmill-bench gen`, `tests/generated_folds.rs`) and the `.slt`
  files, which run against Burrmill in `cargo test` and against DuckDB with `burrmill-bench slt`.

## What it does not do, and where it loses

Said plainly, because a README that implies otherwise is the thing this project is against.

- **The binary is bigger and the build is not faster.** On a nuthatch-shaped consumer, same machine,
  32 jobs (`probes/footprint6/results/burrmill.txt`): querying test binary 495 MB against DuckDB's
  162, release binary 112 MB against 41, `target/` 4.76 GB against 3.27, clean test build 84 s
  against 77, incremental 1.8 s against 1.1. DuckDB is one C++ archive compiled once; DataFusion is
  dozens of crates whose generic operators are instantiated per type into every test binary. The
  footprint is accepted; the build time is [burrmill#7](https://github.com/nightswatchhq/burrmill/issues/7).
- **Small queries.** An eight-row view runs eighteen DataFusion operators. `lodestar_disputes` is
  0.80x DuckDB after the small-input rule, and the worst ratio on the nest.
- **`HUGEINT` stops at 38 digits.** `DECIMAL(38,0)` reaches 10^38 - 1 where DuckDB's reaches
  2^127 - 1. A value between refuses; it does not answer wrongly. No real nest has produced one.
- **An integer compared with a boolean** (`1 = false`) casts in DuckDB and refuses here.
- **What DuckDB computes and Burrmill refuses, by design**: a `SUM` over a `TRY_CAST` that DuckDB
  answers by dropping what did not fit. The fuzzer counts these separately and they are allowed.
- **What Burrmill computes and DuckDB refuses**: `TIMESTAMPTZ + INTERVAL` and a `TIMESTAMPTZ` to
  `DATE` cast, which nuthatch's DuckDB, built without ICU, will not do.
- **Four of DuckDB's six roles in nuthatch are still DuckDB's.** The executor is replaced and the
  parser role partway; the canonical plan for grafting identity, entity lowering, the DuneSQL
  translation and the `entities.toml` function vocabulary still ask DuckDB's parser. They move in
  phase 2.
- **The owned operator alone runs 0 of 65 real statements.** Every fold sub-plan in the workload
  admits (8/8), but each sits inside a CTE or a join, so whole statements run on the engine.
- **No redb `HotTip`.** The hot/cold seam holds under concurrent seal (COR-1), but the only tip is
  in-memory (roadmap 3.4).
- **DataFusion joins do not yield to cancellation** (apache/datafusion#19358). A statement with a join
  cannot promise the one-morsel cancellation bound; the mitigation is a per-query timeout at
  nuthatch's 30 s guard.
- **4.2c is still owed**: the earliest `views` bench gave DuckDB no `parquet_metadata_cache`, worth
  about 9% on the curation fold. The Gate 1 harnesses set DuckDB up as nuthatch does; the old
  synthetic sweep has not been re-run with it.
- **The seven rewritten views** live on the nest's `pete/portable-views` branch on the ThinkPad, not
  pushed. Two more (`lodestar_delegator_stakes`, `lodestar_delegators`) run unchanged.

## Layout

    crates/burrmill          the library. Default build: arrow, parquet, rustc-hash, rayon,
                             hashbrown, sqlparser. Feature `datafusion` adds the engine: about
                             sixteen datafusion-* component crates pinned =55.0.0, tokio,
                             object_store. No DuckDB. No C++. `src/df/` holds the engine and its
                             rules, one file each.
    crates/burrmill/examples hosted_fold: the memory gate in a Burrmill-only binary.
    crates/burrmill-bench    publish = false. Both oracles (DuckDB, the umbrella DataFusion) live
                             here, so neither can reach the shipped graph. Note it lends the crate
                             DataFusion features by unification; test a consumer on burrmill alone.
    docs/progress-log.md     every measurement, newest first
    docs/research/replacing-duckdb  the investigations, plan.md, and migration.md with the tick lists
    docs/upstream            bugs found in DuckDB, DataFusion and nuthatch, drafted for filing
    docs/bench               transcripts the numbers above are quoted from

## Running Gate 1

```sh
# Every authored view of a real nest on both engines: parity as a multiset of nuthatch JSON rows,
# then warm timings. Read-only; nothing is written to the nest.
cargo run -p burrmill-bench --release -- engine-views /path/to/nest

# One view's plan on both engines, or one SQL file.
cargo run -p burrmill-bench --release -- engine-analyze /path/to/nest lodestar_epochs
cargo run -p burrmill-bench --release -- engine-sql /path/to/nest query.sql

# A rewritten view against its original, both on DuckDB, then Burrmill against DuckDB.
cargo run -p burrmill-bench --release -- rewrite-parity /path/to/nest /path/to/views

# All 22 views served to 1, 4, 16 and 32 clients on both engines.
cargo run -p burrmill-bench --release -- serve-views /path/to/nest

# The memory gate, as nuthatch would run it (Linux, for the /proc split).
cargo run -p burrmill --release --example hosted_fold --features datafusion /path/to/nest/segments

# Stock DataFusion on the same nest, the control.
cargo run -p burrmill-bench --release -- df-views /path/to/nest
```

Set `TZ=UTC` in the environment before any of these, as nuthatch's hosts do; the harnesses take
DuckDB's zone from it and never `SET TimeZone`, because doing so switches on the DuckDB bug above.

## Checking it is right

```sh
# Differential fuzzing against DuckDB. A difference prints the seed that reproduces it.
CASES=5000 SEED=7 cargo run -p burrmill-bench --release -- fuzz
SEED=7 CASES=1 PRINT=1 cargo run -p burrmill-bench --release -- fuzz
SQL="SELECT substr('10', -3, 2)" cargo run -p burrmill-bench --release -- fuzz

# The parity harnesses.
cargo run -p burrmill-bench --release -- dialect-parity
cargo run -p burrmill-bench --release -- encode-parity
cargo run -p burrmill-bench --release -- error-parity
cargo run -p burrmill-bench --release -- reach-parity

# Generated fold cases against DuckDB, and the .slt corpus pointed at DuckDB.
CASES=3000 cargo run -p burrmill-bench --release -- gen
cargo run -p burrmill-bench --release -- slt

# Where the two engines' casts disagree, printed; DuckDB's HUGEINT wrap, reproduced.
cargo run -p burrmill-bench --release -- cast
cargo run -p burrmill-bench --release -- duckdb-gaps

# The seal-layout canary against a real nest; more generated cases; one case again.
BURRMILL_NEST=/path/to/nest/segments cargo test --test seal_layout -- --nocapture
BURRMILL_CASES=5000 cargo test --test generated_folds
BURRMILL_SEED=1234 cargo test --test generated_folds
```

`cargo test` runs the fast half of this on every invocation.

## Running the slice 1 gate

The owned operator's original gate, "at most 1.0x DuckDB at exact parity under 256 MB peak RSS at
eight threads", still runs and still passes: 0.38 to 0.87x across fourteen configurations, 199 to
218 MB at 989,690 groups depending on the stage.

```sh
ROWS=2000000 SEGMENTS=100 REPEATS=5 cargo run -p burrmill-bench --release
ROWS=2000000 SEGMENTS=100 ADDRS=1000000 REPEATS=5 cargo run -p burrmill-bench --release
BREAK_PARITY=1 cargo run -p burrmill-bench --release      # the guard must refuse; no RESULT line
ORDER=burrmill_first cargo run -p burrmill-bench --release # page-cache order is a confound
cargo run -p burrmill-bench --release -- inspect /path/to/nest/segments
cargo run -p burrmill-bench --release -- explain /path/to/nest/segments
cargo run -p burrmill-bench --release -- nest /path/to/nest/segments <table-prefix>
cargo run -p burrmill-bench --release -- serve <fixture-dir>
```

A real-nest ratio that is mostly fixed cost is not printed as a number; the field reads
`UNSAFE_fixed_duck=NNpct` instead (`docs/bench/method.md`).

## Licence

MIT OR Apache-2.0.
