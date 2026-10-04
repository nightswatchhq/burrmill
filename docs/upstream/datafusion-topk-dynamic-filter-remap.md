# DataFusion: a top-k dynamic filter over a key inside another key prunes the answer

**Status: drafted, not filed.** Found by #52 (PR #61), reduced to plain DataFusion 55.0.0 with no
Burrmill rule in the plan for #64 on 2026-10-04. Filing upstream is Chief's call. Burrmill routes
around it in `df/topkfilter.rs`, which gives such a sort a filter the scan never sees.

The issue text as it would be filed:

---

Title: TopK dynamic filter returns wrong rows when one sort key contains another (DynamicFilterPhysicalExpr remaps the inner column first)

### Describe the bug

With `datafusion.execution.parquet.pushdown_filters = true`, an `ORDER BY ... LIMIT` whose sort keys include an expression that contains another sort key (here `CASE WHEN b % 2 = 0 THEN p ELSE q END` and `b`) returns the wrong rows. With `enable_topk_dynamic_filter_pushdown = false` the answer is right.

`DynamicFilterPhysicalExpr::remap_children` rewrites the filter with `transform_up`, matching each node against the original children by equality. The inner child `b@2` is matched first and replaced with the scan's `b@0`, so the enclosing `CASE` no longer equals the original child and is never remapped. Its `p@0` keeps the sort's column index, which at the scan is `b`. The predicate pushed into the Parquet scan then compares `b`, not `31999 - b`, with the TopK threshold and drops the rows that hold the answer.

With `b` a `UInt64` column the same remap fails loudly instead (`Invalid comparison operation: UInt64 < Decimal128(21, 0)`), since `p` is a decimal there.

### To Reproduce

DataFusion 55.0.0, these statements run one by one through `SessionContext::sql` with an otherwise default config:

```sql
SET datafusion.execution.target_partitions = 1;
SET datafusion.execution.parquet.pushdown_filters = true;
COPY (SELECT value AS b FROM generate_series(0, 31999))
  TO '/tmp/t.parquet' STORED AS PARQUET OPTIONS ('format.max_row_group_size' '500');
CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '/tmp/t.parquet';

WITH s AS (SELECT b, 31999 - b AS p, 31999 - b AS q FROM t)
SELECT p FROM s ORDER BY CASE WHEN b % 2 = 0 THEN p ELSE q END, b LIMIT 3;
```

The `EXPLAIN ANALYZE` of the same query (metrics trimmed) shows the scan's predicate reading `p@0`, an index into the sort's input, against the file schema, where index 0 is `b`:

```
SortExec: TopK(fetch=3), expr=[CASE WHEN b@2 % 2 = 0 THEN p@0 ELSE p@0 END ASC NULLS LAST, b@2 ASC NULLS LAST], preserve_partitioning=[false], filter=[CASE WHEN b@2 % 2 = 0 THEN p@0 ELSE p@0 END < 16000 OR CASE WHEN b@2 % 2 = 0 THEN p@0 ELSE p@0 END = 16000 AND b@2 < 15999]
  ProjectionExec: expr=[__common_expr_1@0 as p, __common_expr_1@0 as q, b@1 as b]
    DataSourceExec: ..., projection=[31999 - b@0 as __common_expr_1, b], output_ordering=[b@1 ASC NULLS LAST], file_type=parquet, predicate=DynamicFilter [ CASE WHEN b@0 % 2 = 0 THEN p@0 ELSE p@0 END < 16000 OR CASE WHEN b@0 % 2 = 0 THEN p@0 ELSE p@0 END = 16000 AND b@0 < 15999 ], dynamic_rg_pruning=eligible
```

`b@2` became `b@0`, as it should; `p@0` should have become `31999 - b@0` and stayed `p@0`.

### Expected behavior

```
+---+
| p |
+---+
| 0 |
| 1 |
| 2 |
+---+
```

which is what `SET datafusion.optimizer.enable_topk_dynamic_filter_pushdown = false` returns.

### Actual behavior

```
+-------+
| p     |
+-------+
| 15998 |
| 15999 |
| 16000 |
+-------+
```

### Additional context

- Version: 55.0.0 (all `datafusion-*` crates at 55.0.0). `remap_children` is unchanged on `main` as of 2026-10-04.
- With `target_partitions = 8` the same query happened to answer correctly in our runs; with one partition it is wrong every time.
- A possible fix: remap top-down, replacing the largest matching child and not descending into it (`transform_down` returning `TreeNodeRecursion::Jump` after a replacement), so an outer child is matched before any child inside it.
