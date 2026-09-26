# DuckDB: comparing text with a TIMESTAMPTZ cast to text drops rows, or keeps the wrong ones

**Status: drafted, not filed.** Found 2026-09-25 by the differential fuzzer, root cause located
2026-09-26. Filing upstream is Chief's call. Nuthatch is not modified from this repo.

## What happens

Once the session's time zone has been consulted, `CAST(<timestamptz> AS VARCHAR) <op> '<text>'`
gives a wrong answer whenever `<text>` does not parse as a timestamp:

- `<`, `<=`, `>`, `>=`, `=`, `<>`: every row is dropped (the plan is `EMPTY_RESULT`);
- `IS NOT DISTINCT FROM`: exactly the rows whose timestamp is NULL are kept;
- `IS DISTINCT FROM`: those rows are dropped.

The zone counts as consulted if the statement calls a zone-dependent function on a TIMESTAMPTZ
(`year(...)`, `date_trunc`, ...), reads `current_setting('TimeZone')`, or the session ran
`SET TimeZone`. **The switch is sticky**: once any statement on a connection has consulted the
zone, every later statement on that connection is affected, including ones that consult nothing.
The fuzzer found two cases that differ partway through a run and agree when run alone. A literal
that does parse (`'2024-01-01'`) behaves.

## Reproduction

DuckDB 1.5.4 through `libduckdb-sys 1.10504.0` (nuthatch's pin, built without ICU), zone from the
environment (`TZ=UTC`), no `SET`:

```sql
SELECT year(to_timestamp(1700000000 + x)) AS y FROM range(3) r(x)
WHERE 'bob' <> CAST(to_timestamp(1700000000 + x) AS VARCHAR);
-- expected: three rows of 2023; DuckDB: no rows

SELECT count(*) AS n FROM range(3) r(x)
WHERE 'bob' <> CAST(to_timestamp(1700000000 + x) AS VARCHAR);
-- 3, correctly: nothing here consults the zone
```

Not checked: the official CLI (with ICU loaded), or DuckDB `main`.

## Cause

`ComparisonSimplificationRule::Apply` (`src/optimizer/rule/comparison_simplification.cpp`) moves a
cast from the column side onto the constant: `CAST(ts AS VARCHAR) < 'bob'` becomes
`ts < CAST('bob' AS TIMESTAMPTZ)`. It casts the constant with `TryCastAs(..., strict = true)` and
bails out when that fails, but its other guard is skipped for a NULL result:

```cpp
if (!cast_constant.IsNull() &&
    !BoundCastExpression::CastIsInvertible(cast_expression.return_type, target_type)) {
    return nullptr;
}
```

So when the cast of `'bob'` yields NULL rather than failing, the comparison becomes `ts < NULL`.
That folds to an empty result, or, under `IS NOT DISTINCT FROM`, matches the NULL timestamps.
Which path the VARCHAR to TIMESTAMPTZ cast takes evidently depends on the zone having been
consulted. That dependence is inferred from the behaviour above, not traced in the source.

Even for a constant that does parse, the rewrite compares as timestamps where the query compared
text. The two orders agree for DuckDB's own text format in one zone, which is probably why the rule
treats the cast as invertible.

## Consequence here

- Burrmill compares the text, as written, and is right in every case above.
- The parity harnesses used `SET TimeZone = 'UTC'`, which consulted the zone on every query and
  put the bug into the oracle where nuthatch, which sets no zone, would not meet it. They now set
  `TZ=UTC` in the process environment instead, as a UTC host would.
- The `KNOWN` entry in `dialect-parity` keeps the `year(...)` form, which nuthatch can meet. Nuthatch
  caches one DuckDB connection per nest and reuses it across queries until the nest's inputs change
  (`DuckCache`, `src/analytics.rs:66` in nuthatch, checked 2026-09-26). So one query using `year()`
  or `date_trunc` on a TIMESTAMPTZ is enough to expose every later query served from that cache.

## A second effect of the same setting

Without ICU, `TIMESTAMPTZ + INTERVAL` is a binder error ("No function matches ...
'+(TIMESTAMP WITH TIME ZONE, INTERVAL)'"), but not after `SET TimeZone`, which the harnesses used
to run. With the zone from the environment, as in nuthatch, it is refused again. Burrmill computes
it; the fuzzer counts that as designed, not looser.
