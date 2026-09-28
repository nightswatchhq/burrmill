# The ledger's windows in chain order, not in memory

2026-09-28. Design note for the allocations nest's ledger family, which needs 1.5-2 GB on Burrmill
against DuckDB's 512 MB (progress log, "What each view needs"). Not built.

## Why it costs what it costs

`lodestar_indexer_ledger` and the views over it are windows of one shape:
`f(...) OVER (PARTITION BY <indexer> ORDER BY k ...)` with `k = block_number * 100000 + log_index`,
over unions of event streams, and joins back on the keys those windows carry. DataFusion sorts every
window's whole input by `(partition, order)` in memory, and none of those sorts, nor the join builds,
can spill (`ExternalSorterMerge`, `HashJoinInput`, `can spill: false`). At one thread and 1 GB the
ledger's five largest holders were two join builds of ~108 MB and three sort merges of 76-101 MB.

## What the data already gives

Sealed segments are in chain order. Measured over the whole thinkpad copy of the nest: 71 tables,
1,924 files, 4,757,547 rows, **0 rows out of `(block_number, log_index)` order within a file, 0 files
overlapping or touching another of the same table**. The order every one of these windows asks for is
the order the rows are stored in; the sort is recomputing it.

## What stands between that and a streaming window

Measured on a thinkpad copy of Burrmill behind a switch, with
`sum(...) OVER (PARTITION BY lower(indexer) ORDER BY block_number, log_index ROWS UNBOUNDED PRECEDING)`
over `rewards__rewards_assigned` (510,692 rows), answers identical to DuckDB throughout:

1. **Burrmill never declares an ordering.** `SegmentTable` deals files round-robin into groups.
   Sorting files by the first block in their footer, giving each group a contiguous run, and
   declaring `[block_number, log_index]` through `FileScanConfigBuilder::with_output_ordering` is
   not enough on its own:
2. **DataFusion drops a declared ordering it cannot prove.** With several files in a group it keeps
   the ordering only if each `PartitionedFile` carries min/max statistics showing the files are in
   order and disjoint (`validate_orderings`); Burrmill collects none. Attaching exact min/max for
   `block_number` and `log_index` from the footers makes the scan report
   `output_ordering=[block_number, log_index]`.
3. **DataFusion still sorts, on purpose.** Given ordered input and a `PARTITION BY` it is not sorted
   on, the window could run in `InputOrderMode::Linear`, but `get_best_fitting_window` refuses that
   mode for bounded input: "removing the sort is not helpful" (`datafusion-physical-plan` 55,
   `windows/mod.rs:619`). True for speed; the opposite of true under a memory bound. Neither
   `prefer_existing_sort` nor one partition changes it.

## The design

- **Ordered scans.** `SegmentTable` orders files by footer range, groups them contiguously, attaches
  `block_number`/`log_index` statistics and declares the ordering. The footers are read once per
  table, as nuthatch's DuckDB binding already does at DDL time. The hot tail is sorted before it is
  appended, which is cheap: it is the window, not the history.
- **A physical rule, after `EnforceSorting`:** a `BoundedWindowAggExec` whose input is a `SortExec`
  on `(partition, order)` and whose input below that already satisfies `order` is rebuilt in
  `InputOrderMode::Linear` on that input, without the sort. Across partitions the hash repartition
  must preserve order (`RepartitionExec::with_preserve_order`), which merges rather than sorts. The
  rule is Burrmill's, next to the others in `df/rule.rs`, and costs no fork.
- **The views order by what they mean.** `ORDER BY k` hides the order from the planner, because
  `bn * 100000 + li` follows `(bn, li)` only while `log_index < 100000`. The views already rely on
  that (they use `k` as a unique key), but the engine should not: the ledger's windows are to say
  `ORDER BY block_number, log_index` (and `, src` where streams tie), which both engines read, and
  which must reach parity on DuckDB before anything else changes.
- **Then the join-backs.** With ordered windows, carrying values instead of keys (`last_value(x
  IGNORE NULLS)`, now read in DuckDB's spelling) removes the two ~108 MB builds; the 37 s that
  rewrite cost earlier was not the window function, which measures 116-421 ms alone.

## How it is judged

The per-view minimum budget, twice running, on the thinkpad copy, for the whole family, against
DuckDB's 512 MB; `engine-views` 22/22 identical; `dialect-parity`, `fuzz` and the shadow replay
unchanged. If the ledger does not come under 512 MB with its sorts gone, the remaining holders are
named from the refusal and this note is amended rather than extended.

## Not in this

The owned-operator alternative (a Burrmill streaming per-key fold) is kept in reserve: the rule above
reuses DataFusion's own window execution and should be tried first. RFC-0059 folds are not the
vehicle (they evaluate on DuckDB, and serving from `/sql` is S4, unfiled).
