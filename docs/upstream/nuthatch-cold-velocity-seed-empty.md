# Nuthatch: the cold velocity seed is always empty

**Status: drafted, not sent.** Found 2026-09-16 by the DuckDB census
(`docs/research/replacing-duckdb/01-duckdb-surface.md`). Nuthatch is not modified from this repo,
and reporting this is Chief's call.

## What happens

`analytics::cold_velocity` (`src/analytics.rs:2288-2317`, nuthatch v3.8.4) groups sealed transfers
into windows with:

```sql
(block_number / {w}) * {w} AS ws
```

`block_number` is `UBIGINT`, and DuckDB's `/` is float division, so `ws` comes back as `DOUBLE`.
`value_to_json` (`:3433`) maps `DOUBLE` to a JSON float, and the row loop then does:

```rust
let (Some(addr), Some(ws), Some(cnt)) =
    (r["addr"].as_str(), r["ws"].as_u64(), r["cnt"].as_i64())
else {
    continue;
};
```

`serde_json` returns `None` from `as_u64()` for any float, including `5.0`. **Every row is skipped**,
so `cold_velocity` returns an empty vector without an error.

Its caller (`src/indexer.rs:7256-7271`) rebuilds the velocity view from that cold seed plus a
replay of the unsealed tip. After any restart, the sealed half contributes nothing. Velocity windows
that span sealed blocks then under-report volume and count until the data is re-derived some other
way. The rebuild log reports `0 cold-seeded`, which looks like an empty table rather than a fault.

## Evidence

- DuckDB 1.5.5 CLI: `SELECT typeof((CAST(12 AS UBIGINT) / 5) * 5)` returns `DOUBLE`, value `12.0`.
  `/` has been float division since DuckDB 0.8, so the bundled 1.5.4 behaves the same.
- `value_to_json`: `ValueRef::Double(f) => Value::from(f)`.
- `serde_json::Number::as_u64` returns `None` unless the number is a non-negative integer
  representation.
- The census reports that no test covers `cold_velocity`.

**Not done:** an end-to-end run against a nest with `velocity_window` configured, and a check of
which production nests configure it at all.

## Fix

Use `//` (integer division) in the SQL, which keeps `ws` as `UBIGINT`. Add a test that seeds a
sealed segment and asserts a non-empty cold seed. Note that DataFusion's `/` on integers already
truncates, so a DataFusion port would silently "fix" this, and the parity harness would report it
as a difference.
