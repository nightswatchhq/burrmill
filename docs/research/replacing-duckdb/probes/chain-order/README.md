# Chain-order windows: the experiment, as run

Source of the 2026-09-28 thinkpad experiment behind `../../ledger-windows.md`, kept because it is the
only copy. It does not build against `main`: it was a scratch tree with switches read from the
environment (`BM_ORDERED`, `BM_NORULE`, `BM_GUARD`, `BM_THREADS`, `BM_MEM`, `BURRMILL_SPILL`).

- `catalog.rs`: `SegmentTable` ordering files by their footer range, attaching exact `block_number`
  and `log_index` min/max statistics (without which DataFusion drops a declared ordering), reading an
  ordered table as one group, and declaring `[block_number, log_index]`.
- `chainorder.rs`: the physical rule that rebuilds a sorted `BoundedWindowAggExec` in
  `InputOrderMode::Linear` over an order-preserving repartition, and `OrderedAs`, which pins the
  merge keys to the window's own order (`../../../upstream/datafusion-merge-keeps-orderings-it-lost.md`).

Measured with it, on the value-carrying ledger views and spilling, at 2 threads: the ledger and the
indexer pool at 384 MB, 22/22 views identical to DuckDB. The rest of the family stays above 512 MB;
see the progress log.
