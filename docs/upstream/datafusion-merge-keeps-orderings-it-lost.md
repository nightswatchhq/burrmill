# DataFusion: an order-preserving merge claims orderings its merge keys do not give

**Status: drafted, not filed.** Found 2026-09-28 while building Burrmill's chain-order window rule
(`docs/research/replacing-duckdb/ledger-windows.md`). Not yet reduced to a plan without Burrmill's
rule in it. Filing upstream is Chief's call. DataFusion 55.0.0.

## What happens

`RepartitionExec::with_preserve_order` merges its input partitions by the input's *first* declared
ordering (`sort_exprs` returns `self.input.output_ordering()`), but the merged output's equivalence
properties keep every ordering the input claimed (`eq_properties_helper(input, preserve_order)`).
Where the input's orderings are only equivalent because of per-partition constants, the merged
stream is ordered by the merge keys and not by the others, yet reports all of them.

## Where it bit

A `UNION ALL` of three branches, each read in `(block_number, log_index)` order, with columns
`bn, ob, ol`:

- two branches: `bn` is `CAST(NULL AS UBIGINT)`, a constant, and `ob, ol` are the block and log
  index;
- one branch: `bn` and `ob` are both `block_number`, so `bn = ob`.

Each branch is ordered by `(ob, ol)` and, through its constant or its equality, by
`(bn, ob, ol)`; DataFusion reports both for the union, correctly per partition. An order-preserving
hash repartition below a window `PARTITION BY sp ORDER BY ob, ol` then merged by
`sort_exprs=bn ASC NULLS LAST, ob ASC, ol ASC`: every row of the third branch came out before every
row of the other two, which is `(bn, ob, ol)` order and not `(ob, ol)` order. The window, trusting
`(ob, ol)`, carried nothing forward; an inner join on the carried key then dropped all 488,495 rows
of the ledger's `legacy_reward_share`. No error; a wrong answer.

## Why it has not shown up in DataFusion itself

Stock DataFusion only builds that shape (an order-preserving merge feeding a consumer that relies
on a *secondary* ordering) under `prefer_existing_sort` or for unbounded inputs, and the
equivalences have to come from per-branch constants. Burrmill's rule relied on
`ordering_satisfy` after the merge; it now accepts the rewrite only when the merge keys literally
begin with the window's `ORDER BY`, and parity is 22/22 again at 2 and 8 threads.

## To file, it needs

A reproduction in plain DataFusion: two ordered `MemTable`s (one with a constant NULL column, one
where that column equals the order key) under `UNION ALL`, `prefer_existing_sort = true`, and a
consumer that requires the secondary ordering, showing the merged plan's claimed ordering against
the rows it emits.
