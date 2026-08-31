# DuckDB: `SUM(HUGEINT)` silently wraps when the aggregate runs in parallel

**Status: drafted, not sent.** Reporting this is outward-facing and goes under Chief's name, so it
waits for him. Everything below is reproducible from this repo.

## Summary

`SUM` over `HUGEINT` returns a wrapped value instead of raising `Out of Range Error`, whenever the
aggregation actually runs in parallel — which needs both **two or more threads** and **two or more
row groups / files**. With either at one, the same query correctly refuses. The check appears to be
present in the single-threaded path and missing from the partial-aggregate combine.

The practical shape of this is the worst kind: it is correct on every small test and goes wrong only
once the data is large enough to parallelise.

## Reproduction

Two rows credit one party with `i128::MAX` and then `1`. The true sum is `MAX + 1`, which no 128-bit
integer holds.

```sql
-- two Parquet files, one row each
CREATE VIEW overflow AS SELECT * FROM read_parquet('seg-*.parquet');
SET threads TO 2;

SELECT addr, SUM(d)::VARCHAR AS net FROM (
  SELECT "to"   AS addr,  TRY_CAST("value" AS HUGEINT) AS d FROM overflow
  UNION ALL
  SELECT "from" AS addr, -TRY_CAST("value" AS HUGEINT) AS d FROM overflow
) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr;
```

| threads | files | result |
|---:|---:|---|
| 1 | 1, 2, 3 | `Out of Range Error: Overflow in HUGEINT addition: 170141183460469231731687303715884105727 + 1` |
| 2 | 1 | same error, correct |
| **2** | **2, 3** | **returns `-170141183460469231731687303715884105728`** |
| **4** | **2, 3** | **returns `-170141183460469231731687303715884105728`** |

`-170141183460469231731687303715884105728` is `i128::MIN`. The true value is `i128::MAX + 1`, so the
sum has wrapped by exactly `2^128`.

Note that the other party in the same result, whose sum is `-(MAX + 1)`, is `i128::MIN` **correctly** —
that one is representable. The two rows are indistinguishable to a reader.

## Version

`libduckdb-sys 1.10501.0` (DuckDB 1.5.1), bundled build, macOS arm64 and Debian 13 x86-64. Both
reproduce.

## Runnable form

    cargo run -p burrmill-bench --release -- duckdb-gaps

Prints the full threads × files grid.

## Why it matters to us

Burrmill's stated guarantee is that an integer overflow is refused rather than wrapped, because a
wrapped balance is a wrong answer that looks exactly like a balance — and we measure ourselves
against DuckDB precisely because DuckDB is the incumbent that gets this right. Our `.slt` corpus
carries `skipif duckdb` on this one case with a pointer to this file; the skip comes out when the
fix lands.
