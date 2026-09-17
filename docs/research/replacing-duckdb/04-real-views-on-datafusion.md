# 04 · The real authored views on DataFusion, against DuckDB

Investigation 04, reported 2026-09-16 by a research agent (Fable 5.1), working in its own worktree.
The harness is the new `burrmill-bench df-views` subcommand (`crates/burrmill-bench/src/df_views.rs`),
and its recorded output is `docs/bench/df-views.txt`.

## Correction made before filing

**The agent's headline, 0.60-0.64x DuckDB time-weighted, was unfair to DuckDB, and it is not the
number below.**

- **The asymmetry.** The timed DuckDB call went through the parity helper, which boxes every cell
  into a `Value`, formats it as a string, joins each row and sorts the result. The timed DataFusion
  call ended at Arrow batches. On `lodestar_delegations` that charged 608k rows of client-side
  string work to DuckDB alone.
- **The fix.** DuckDB is now timed through `query_arrow`, with its row count checked against the
  parity pass (`duck_arrow` in `df_views.rs`). Both nests were re-run, and `docs/bench/df-views.txt`
  holds the fair run.
- **What survives the correction:**
  - the coverage figures;
  - the rewrite rules;
  - every DataFusion-against-DataFusion comparison in the diagnosis, since those were always
    measured the same way;
  - the conclusion that DataFusion, properly configured, is not slower than DuckDB on this
    workload.
- **What changed:** every "vs DuckDB" figure. The corrected numbers are below.

## Results (fair run, graph-allocations-nest, 38,428 segments, 8 threads)

**Coverage: 11 of 22 statements run on DataFusion with parity, with no mismatches.**

- 5 fail to parse: 2 DuckDB `ASOF LEFT JOIN`s and 3 list comprehensions.
- 2 fail to plan:
  - `port_queue`, on DataFusion's ORDER BY ambiguity rule, which nuthatch #996 also hit;
  - `lodestar_delegator_stakes`, on `list_reduce`.
- 4 fail by cascade from those.

dips-nest: 2/2 parity, both at 1 ms or under.

**Rewrites needed, applied to the DataFusion side only:**

| Rule | Statements | Rewrite |
|---|---:|---|
| `hugeint` | 13 | `CAST(x AS HUGEINT)` → `DECIMAL(38,0)` |
| `intdiv` | 5 | `a // b` → `CAST((a - a % b) / b AS DECIMAL(38,0))`, exact where decimal `/` would round |
| `tuple-cmp` | 1 | `(a, b) > (c, d)` expanded to scalar comparisons |

**Time-weighted: DataFusion 0.71x DuckDB** (2,856 ms against 4,028). DataFusion is ahead on 7 of 11.

| statement | rows | DuckDB ms | DataFusion ms | ratio |
|---|---:|---:|---:|---:|
| deployment_signal | 13,896 | 185 | 172 | 0.93 |
| open_allocations | 6,779 | 57 | 74 | 1.30 |
| lodestar_allocations | 248,487 | 367 | 451 | 1.23 |
| epoch_boundaries | 267 | 57 | 103 | 1.81 |
| lodestar_disputes | 8 | 1 | 2 | noise |
| lodestar_escrow_transactions | 73,718 | 118 | 48 | 0.41 |
| lodestar_delegations | 608,457 | 628 | 296 | 0.47 |
| lodestar_curator_signals | 15,054 | 750 | 508 | 0.68 |
| lodestar_curators | 1,819 | 347 | 270 | 0.78 |
| lodestar_provisions | 95 | 356 | 235 | 0.66 |
| lodestar_deployments | 24,757 | 1,162 | 697 | 0.60 |

The table above uses DataFusion's `ListingTable` with statistics off and a 1 GiB footer cache.

**Diagnosis on the worst six.** Each row changes one thing. DuckDB takes 1,763 ms on these six.

| DataFusion configuration | sum ms | vs its defaults | vs DuckDB |
|---|---:|---:|---:|
| defaults: listing, statistics on, 50 MiB footer cache | 3,634 | 1.00 | 2.06x |
| statistics off | 2,131 | 0.59 | 1.21x |
| statistics off, cache off | 2,173 | 0.60 | 1.23x |
| statistics off, cache 1 GiB | 1,582 | 0.44 | 0.90x |
| statistics on, cache 1 GiB | 2,076 | 0.57 | 1.18x |
| **custom provider (file sizes known, no listing), cache 1 GiB** | **974** | **0.27** | **0.55x** |
| same, one file group | 970 | 0.27 | 0.55x |
| custom provider, cache off | 2,176 | 0.60 | 1.23x |
| custom provider, footers pre-parsed by Burrmill's morsel cut | 965 | 0.27 | 0.55x |

## What it means for the plan

- **The "3.6x" is DataFusion's defaults, not DataFusion.** Three things account for it:
  - statistics collection at plan time;
  - a 50 MiB footer cache too small for a 6-10k-segment table, which is worth nothing over no
    cache;
  - a per-file `head` on every scan.

  A nest table provider that already knows its files removes all three, and needs no engine
  change. The README's synthetic 3.6x has not been re-measured with these remedies.
- **Burrmill's footer cache is not a differentiator.** A correctly sized DataFusion cache matches
  it (965 against 974). The one difference is who pays the one-off parse: Burrmill's parallel cut
  takes about 1 s for 38k footers at registration.
- **Where DataFusion still loses:**
  - small-output statements, where about 50 ms of per-query listing shows under `ListingTable`
    (the custom provider should remove it; not re-measured per statement);
  - `lodestar_allocations`, at 1.23x.
- **Scaling across partitions.** One file group costs the same as eight, so the per-file open path
  does not parallelise. That is the next thing to look at.

## Not measured

- The synthetic fold sweep under the remedied configuration.
- Concurrency, and DataFusion's peak RSS.
- A DuckDB-first ordering control. The parity pass warms both engines, then repeats interleave.
- More than one run per nest after the correction; the agent's two pre-correction runs agreed
  within 0.04.
- nuthatch's own 15-repeat floor.
- `union_by_name` on the DuckDB side. Nuthatch uses it and this bench does not, so if anything
  DuckDB is measured faster here than nuthatch runs it.
- DuckDB is the bench's bundled 1.5.1; nuthatch ships 1.5.4.

## Files changed (in the main checkout, uncommitted)

- `crates/burrmill-bench/src/df_views.rs` (new): the loader, the rewriter, a `SegmentTable`
  provider, a `PreparsedFactory`, the runner and the diagnosis. `duck_arrow` was added at filing.
- `crates/burrmill-bench/src/main.rs`: the `df-views <nest-dir>` arm.
- `crates/burrmill-bench/Cargo.toml` and `Cargo.lock`: `object_store`, `futures`, `async-trait`,
  `bytes` and `serde_json`, bench only. No version changes.
- `docs/bench/df-views.txt` (new).

The agent's worktree build is clean (`cargo check --tests -p burrmill-bench`). After the timing fix,
it was rebuilt and re-run in the worktree, and the files were copied across unchanged. The main
checkout has not been rebuilt, because its target directory was cleared and bundled DuckDB takes
minutes to build from cold.
