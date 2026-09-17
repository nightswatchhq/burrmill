# Replacing DuckDB in nuthatch

Opened 2026-09-16. Everything here is on the Burrmill side. **Nothing in this directory modifies
nuthatch**, which stays a read-only subject (RFC-0044 Amendment 1) until a plan is agreed.

## The direction

Chief, 2026-09-16: swap DuckDB out of nuthatch for a Rust engine. Burrmill may rent DataFusion or
anything else under the hood. The goal is the swap, not beating DuckDB on every shape.

What that changes:

- **The bar.** It is now "no regression a nest's users feel, plus the wins DuckDB cannot offer". It
  is no longer "≤1.0x DuckDB on every shape". Latency and memory are judged against the budgets
  nests already run under.
- **The one gate that does not move: answer parity on every authored statement.** A swap that
  changes one balance is worse than keeping DuckDB.
- **The default outcome.** RFC-0042's keep-DuckDB outcome (2026-08-30) is no longer the default.

## The plan

**[plan.md](plan.md)** is the synthesis of the five investigations: the design, a footprint spike
first, three phases with gates, and the risks that could stop it. What follows here is the
direction and the evidence.

## Working design (confirmed by the investigations; see the plan for the footprint fork)

**DataFusion plans every statement, and Burrmill supplies what DataFusion does not do well:**

- **Owned fold operators**, substituted into DataFusion's physical plan wherever a subtree matches.
  Coverage then counts per subtree. A4's 0/65 statement coverage only mattered while Burrmill had
  to run whole statements itself; all 8 fold sub-plans already admit.
- **Checked arithmetic.** A plan rewrite replaces wrapping `sum`, `+`, `-` and `*` with checked
  versions, or refuses the plan. A result must never be wrong, including for values up to uint256.
- **A positive allowlist** of tables and functions: no DDL, no DML, no file-reading functions.
- **A nest catalogue** over explicit file lists, with Parquet metadata cached for good, because
  sealed segments are content-addressed and cannot change.
- **A native `HotTip` provider** for the seam. COR-1 is already tested (stage 3).

**In nuthatch, later and in this order:**

1. An engine trait behind `/sql`, the CLI, MCP and the views, with DuckDB still behind it.
2. Shadow mode: both engines answer, differences are logged, and DuckDB's answer is served.
3. The default flips after a release cycle with no differences and latency and memory within budget.
4. DuckDB becomes a dev-dependency, kept as an oracle.
5. DuckDB is removed. These last two steps are RFC-0044 slices 7-8.

## Where Burrmill can be strictly better than DuckDB

- **Exact to uint256, refusing on overflow.** DuckDB's `DECIMAL` stops at 38 digits, which is why
  nuthatch carries the `_dec` / `_overflow` workaround (`src/analytics.rs:3347`). A uint256 is 78
  digits.
- **Closed by construction.** Nothing in the grammar reaches the filesystem. DuckDB instead
  needs a directory allowlist added on top, and nuthatch has needed two filesystem-escape fixes.
- **No C++ in the process.** The #1152 assertion and the #1165 segfaults were native crashes inside
  DuckDB's C++ code, which Rust cannot catch. A Rust panic can be confined to the one query.
- **Built for immutable segments.** Caches never need invalidating, pruning can use `block_number`,
  and owned operators run the heavy shapes.
- **The hot tip read where it lives.** Today nuthatch copies the tip into DuckDB temp tables
  (`load_hot_temp`).
- **Later: incrementally maintained views** with `dbsp`, which nuthatch already ships, in place of
  the per-request memo.

## Where DuckDB stays ahead unless measurement says otherwise

- **Throughput above about sixteen concurrent clients:** 160 against 135 qps (roadmap 5.2-5.5),
  with DuckDB embedded the way it is meant to be, one database and a connection per client.
  Nuthatch does not embed it that way; see finding 4.
- **General SQL breadth.**
- **The dialect.** Every existing nest's views are written in DuckDB's. The constructs with no
  DataFusion path are almost all in the Lodestar and qos views (01, 04).
- **Build footprint, measured (05).** Through the umbrella `datafusion` crate, test binaries are
  2.6x DuckDB's at `-g0`, and the release binary is 3x. The component-crate route is about even,
  but it needs a Burrmill-owned physical planner and is unproven.

## Investigations

Launched and reported 2026-09-16. Each report file puts the corrections made before filing at the
top, followed by the agent's report verbatim. Probe code and scripts are kept in `probes/`.

| # | Question | Where it ran | Report |
|---|---|---|---|
| 01 | What nuthatch needs from DuckDB: every touchpoint, and a function/syntax census of all authored SQL across the nests, classified against DataFusion 55 | read-only | [01-duckdb-surface.md](01-duckdb-surface.md) |
| 02 | The state of DataFusion and other embeddable Rust engines for this workload | web, crate source, probes | [02-datafusion-state.md](02-datafusion-state.md) |
| 03 | Can DataFusion refuse on every overflow up to uint256, and at what cost | thinkpad, prototype | [03-exact-arithmetic.md](03-exact-arithmetic.md) |
| 04 | The real authored statements on DataFusion against DuckDB, parity first, plus what the 3.6x at 10k segments consists of | this Mac, worktree | [04-real-views-on-datafusion.md](04-real-views-on-datafusion.md) |
| 05 | Build footprint: bundled DuckDB against DataFusion, full and trimmed, and Burrmill today | thinkpad | [05-build-footprint.md](05-build-footprint.md) |

## Findings (verified 2026-09-16)

1. **The parallel `SUM(HUGEINT)` wrap is already reported upstream** as duckdb#24081. It was fixed
   on `main` by #24168 after 1.5.5 was cut and has not been backported, and it still reproduces on
   the 1.5.5 CLI. Nuthatch pins 1.5.4. See `docs/upstream/duckdb-hugeint-parallel-wrap.md`. The
   correctness case cannot rest on it past DuckDB's next major release.
2. **The 4.2 real-view comparison caches footers on one side only.** DuckDB's
   `parquet_metadata_cache` is off by default. On the curation fold it measures 151 ms off and
   138 ms on. Roadmap 4.2c.
3. **The external research brief is stale or wrong in several places.** It is kept, with its
   corrections, in [00-external-brief.md](00-external-brief.md).
4. **Nuthatch embeds DuckDB one instance per concurrent query.** `src/analytics.rs` lends its
   cached connection to one query, and a concurrent query opens a new instance. Several instances
   sharing a spill directory is how #1165's crashes arose. That is a nuthatch-side fix, noted and
   not acted on.
5. **256 bits needs more than i256.**
   - uint256 is 78 digits, `Decimal256` holds 76, and i256 reaches 2^255-1.
   - Burrmill today returns i128 and carries a high word only for intermediate partial sums.
   - An exact uint256 path needs a wider accumulator. Roadmap 2.1b has already declined 16 bytes
     per group on memory grounds, so the cost has to be measured (investigation 03).
6. **Nuthatch's own RFC-0042 §3a is all or nothing.** DuckDB stays in every role, or goes from
   every role and never executes a user query in a shipped binary. Its roles include SQL parser
   (`json_serialize_sql` at 7 sites), RFC-0041 oracle, restart seed and admission bounding, as well
   as executor. Its recorded reopen conditions are:
   - 2027-09-01;
   - a named musl user;
   - five roles built in two-day boxes;
   - DataFusion #17539 closed;
   - nuthatch #357 scheduled.

   Chief's direction supersedes the date. The rule itself is worth keeping: a half-swap is the
   worst end state. See 01.
7. **Nuthatch's cold velocity seed is always empty.** `(block_number / w) * w` is `DOUBLE` in
   DuckDB, and `as_u64()` rejects it. Drafted at `docs/upstream/nuthatch-cold-velocity-seed-empty.md`,
   not sent. A DataFusion port would silently change this, which is exactly the kind of difference
   shadow mode has to surface rather than hide.

8. **Speed is not the obstacle** (04).
   - DataFusion on the real authored views is 0.71x DuckDB time-weighted, and 0.55x on the worst
     six with a provider that knows its files.
   - The README's 3.6x was DataFusion's defaults.
   - The agent's first figure, 0.60x, timed DuckDB's row formatting and sort against it. It was
     corrected before filing.
9. **Exact arithmetic on DataFusion is achievable, as a Burrmill-owned rule** (03). The cost is
   +100-130 MB at 1M groups, which a per-type accumulator should roughly halve.
10. **Burrmill shipped `sqllogictest` in its normal dependencies**, under a comment saying it did
    not. Fixed on 2026-09-16: it is gone from `cargo tree -e normal`, and `cargo test -p burrmill`
    passes (05).

## Decisions owed to Chief

- A backport request on duckdb#24081. It is outward-facing.
- Whether to send the nuthatch `cold_velocity` report. It is a live correctness bug, independent of
  the swap.
- Whether the work items that come out of this become GitHub issues on this public repo.
- Timing. `docs/frozen-for-2027.md` does not list RFC-0042. The RFC carries its own reopen
  conditions, and this direction supersedes the date (01).
- When nuthatch may be touched: phase 1b (our own views) and phase 2 (the engine trait) both need
  it.
- If the component route fails the phase 0 spike: whether to accept the umbrella crate's footprint
  regression in exchange for the correctness and C++-free wins.
