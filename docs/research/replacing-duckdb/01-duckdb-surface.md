# 01 · What nuthatch needs from DuckDB

Investigation 01, reported 2026-09-16 by a research agent (Fable 5.1). The report is kept verbatim
below.

**Scope and method.** The agent worked read-only against nuthatch v3.8.4 (`duckdb` crate 1.10504.0,
i.e. DuckDB 1.5.4), and every authored `.sql` file under `~/Projects/*-nest`, `nests/` and
`nests-mvp/`. It checked items against the datafusion 55.0.0 and sqlparser 0.62.0 source, and
probed DuckDB semantics on the 1.5.5 CLI rather than the bundled 1.5.4. Its census scripts are kept
in [probes/census](probes/census/README.md).

**Checked before filing:**

- The `cold_velocity` finding, confirmed. `(UBIGINT / 5) * 5` is `DOUBLE` on the 1.5.5 CLI,
  `value_to_json` maps `DOUBLE` to a JSON float, and `as_u64()` rejects floats. Written up as
  `docs/upstream/nuthatch-cold-velocity-seed-empty.md`.
- The RFC-0042 §3a quotation and the reopen conditions were not re-read here.

## Headlines for the plan

- **DuckDB fills several roles in nuthatch, not one.**
  - **Executor:** `/sql`, `/q/{name}`, `/explain`, the CLI and trusted folds.
  - **Parser:** `json_serialize_sql` at 7 sites. These are the `/sql` security allowlist, table
    reachability, the graft canonical plan, the RFC-0041 entity gates, DBSP lowering and the Dune
    translator.
  - **Reference oracle** and **restart seed** for RFC-0041 entities.
  - **Admission bounding** for `/q/{name}`, via `EXPLAIN (FORMAT JSON)` operator names.

  Nuthatch's RFC-0042 §3a allows only two end states: DuckDB in every role, or in none. A swap
  has to replace all of them, and the parser role is the one nobody has counted until now.
- **The biggest single piece is the catalogue nuthatch builds on every query.**
  - 117 base tables over explicit segment lists, with `union_by_name` schema merging.
  - Declared NULL-typed columns, and the `_dec` / `_overflow` derivations.
  - The hot tip loaded through `CREATE TEMP TABLE` plus the Appender.
  - Authored views re-run per request, in file order.

  In Burrmill terms, this is a nest `TableProvider` plus a `MemTable` or native `HotTip`.
- **The authored SQL is small and uneven.**
  - 97 unique files hold 132 statements: 118 views and 14 checks.
  - There are 305 `CAST ... AS HUGEINT`.
  - The DuckDB-only syntax with no DataFusion path sits almost entirely in graph-allocations
    (Lodestar) and qos: 8 ASOF joins, 7 list comprehensions, 8 `list_reduce` lambdas, 3 `TRY()`,
    28 `arg_min`/`arg_max`, 22 JSON function calls, and 6 `from_hex`/`decode` calls.
  - Everything else is ordinary: CTEs, joins, `FILTER`, windows, `GROUP BY ALL`, `QUALIFY`.
- **Some differences change results without any error:**
  - `/` returns DOUBLE in DuckDB (74 uses);
  - quoted identifiers are case-insensitive in DuckDB (543 of them);
  - `to_timestamp` returns TIMESTAMPTZ;
  - implicit VARCHAR comparisons (`= true`, `IN (10)`);
  - `substr` with a negative start.

  Only answer-by-answer parity catches these.
- **Clients depend on the exact result encoding.** HUGEINT and every scale-0 DECIMAL come back as
  digit strings, `COUNT(*)` as a number, and timestamps as Rust `Debug` text. Tests and check
  fixtures pin this, and `sql_errors.rs` matches DuckDB's error text word for word.
- **Nuthatch details that surfaced along the way.**
  - **The `cold_velocity` seed is always empty** (confirmed above).
  - **The `json_serialize_sql` allowlist fails open when no parse is available.** This is
    deliberate and documented in nuthatch's `reject_unknown_table_refs`: the older denylist stays in
    front, and a statement DuckDB cannot parse cannot run either. A replacement has to be at least as
    strict.
  - One result-encoding robustness detail from this report was taken out of this public record at
    filing and reported to Chief directly.
- **Nuthatch's own prior measurements agree with Burrmill's.**
  - General DataFusion was 1.85-2.65x DuckDB on `net_balances` (RFC-0013), and 2.53-2.80x at 10k
    segments (#964).
  - An owned Rust fold was 0.53-0.85x (#987).
  - DataFusion parsed 27/27 views and planned 16/24; all 8 failures were HUGEINT (slice 4).

---

## The report, verbatim

# What Nuthatch needs from DuckDB (nuthatch v3.8.4, duckdb crate 1.10504.0 = DuckDB 1.5.4)

Read-only; nothing modified, built, committed or pushed. Paths are relative to /Users/pepe/Projects/nuthatch unless absolute. DataFusion 55.0.0 and sqlparser 0.62.0 were not in the cargo registry on this machine (it was re-extracted today, 2026-09-16 11:17); I fetched them with `cargo fetch` into a scratchpad crate and read the source from `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`. DuckDB semantics were probed with the Homebrew CLI, which is **1.5.5**, not the bundled 1.5.4.

## 1. DuckDB touchpoints in Rust

`tests/duckdb_containment.rs:61-70` pins the files that may name `duckdb::` to eight (analytics, entities, entity_lower, graft, seal, port_emit, authored_entity_spike, dune_views); `:437-460` asserts no `pub` signature exposes a DuckDB type. Everything that executes SQL goes through `src/analytics.rs`; the other sites use only DuckDB's parser on a bare `Connection::open_in_memory()`.

| file:line | role | DuckDB feature relied on |
|---|---|---|
| `src/analytics.rs:287-343` `open_locked_duckdb` | every `/sql`, `/q/{name}`, `/explain`, local CLI, trusted folds | `Config::max_memory` (512 MB default), `.threads` (2), `temp_directory` (private `nuthatch-duckdb-{pid}-{seq}`), `max_temp_directory_size`; `open_in_memory_with_flags`; `SET disabled_optimizers='compressed_materialization'`; `SET allowed_directories=[...]; SET enable_external_access=false; SET lock_configuration=true` |
| `src/analytics.rs:44-75, 100, 1180-1205` | connection cache (#295) | one cached `Connection` per nest dir, capacity 16, keyed on `(sealed_through, excluded segments, sha256 of nuthatch.toml + views/*.sql + labels/*.json + offchain catalogue)`; a concurrent caller finds the slot removed and opens a second instance, so permits are a memory bound |
| `src/analytics.rs:177-285` `SpillDir` | spill isolation (#1165) | DuckDB names spill files by index inside `temp_directory`; two instances in one dir overwrote each other's blocks and SEGV'd (25/min at four permits); fixed with exclusive `create_dir` per instance plus a PID-liveness sweep |
| `src/analytics.rs:1130-1160` | `/sql` text gates | `SELECT`/`WITH` prefix, WITH-prefixed DML, `;` stacking, `FORBIDDEN_FNS` denylist (`:1500-1538`, incl. `duckdb_settings`, `getenv`), replacement scan `FROM '...'` (`:2151`) |
| `src/analytics.rs:1794-1862`, `:2036-2107` | parser allowlist and reachability | `SELECT json_serialize_sql('<sql>')`; walks nodes `type: TABLE_FUNCTION` (`function.function_name`) and `BASE_TABLE` (`table_name`, `schema_name`); `ALLOWED_TABLE_FNS = generate_series, range, unnest` (`:1771`); **fails open** if the parse is unavailable |
| `src/analytics.rs:1872-1914`, `:623-633` | sweep bound, admission | `duckdb_views()` (`view_name`, `sql`, `internal`), `duckdb_tables()` |
| `src/analytics.rs:527-707` | RFC-0048 declared-query admission | `EXPLAIN (FORMAT JSON)` read from column 1; physical operator names whitelisted (`READ_PARQUET`, `SEQ_SCAN`, `HASH_JOIN`, `PIECEWISE_MERGE_JOIN`, `PERFECT_HASH_GROUP_BY`, `TOP_N`, `STREAMING_LIMIT`, `CTE_SCAN`, ...); cost = `READ_PARQUET` count × widest table |
| `src/analytics.rs:2471-2748` `define_views_bound` | base-table views, rebuilt per query and narrowed to reachable tables (#896) | `CREATE OR REPLACE VIEW "t" AS SELECT *, TRY_CAST("c" AS DECIMAL(38,0)) AS "c_dec", ("c" IS NOT NULL AND TRY_CAST(...) IS NULL) AS "c_overflow" FROM (SELECT * FROM read_parquet(['a.parquet',...], union_by_name=true) UNION ALL BY NAME SELECT CAST(NULL AS UBIGINT\|VARCHAR) AS "col",... WHERE false) UNION ALL BY NAME SELECT ... FROM "__hot_t"`; eager view binding doubles as the corruption probe (`SELECT 1 FROM read_parquet([f], union_by_name=true) LIMIT 0`, `:2705`) |
| `src/analytics.rs:2762-2823` `load_hot_temp` | hot tip into DuckDB | `DROP TABLE IF EXISTS; CREATE TEMP TABLE "__hot_t" (...)`; `conn.appender()`, `append_row(&[&dyn ToSql])`, `DuckValue::UBigInt/Text/Null`. Types are by column *name*: `block_number`, `log_index`, `_seq`, `block_timestamp` UBIGINT, everything else VARCHAR (COR-4), matching Parquet (`src/seal.rs:509-526`: UInt64 / Utf8) |
| `src/analytics.rs:2843-2869` | authored `views/*.sql` | split on top-level `;` (`:2937`), `CREATE VIEW` → `CREATE OR REPLACE VIEW` (`:2912`), one `execute_batch` per statement, failures logged at debug; eager binding makes file order matter |
| `src/analytics.rs:2872-2909` | offchain snapshots | `read_parquet([...], union_by_name=true)` views named `offchain__{t}` |
| `src/analytics.rs:2322-2347` | labels | `read_json('<dir>/labels/*.json', format='array', columns={address:'VARCHAR', label:'VARCHAR'})`, static json extension (`tests/duckdb_extensions_are_static.rs:100-126`) |
| `src/analytics.rs:2350-2408` | factory `{template}__children` | `UNION ALL` + `QUALIFY row_number() OVER (PARTITION BY address ORDER BY ...) = 1` |
| `src/analytics.rs:2193-2320` | trusted restart seeds (`net_balances`, `over_i128_transfers`, `cold_exposure`, `cold_velocity`) | `TRY_CAST("v" AS HUGEINT)`, unary minus, `SUM(d)::VARCHAR`, `HAVING SUM(d) <> 0`, `COUNT(*)::VARCHAR`, `JOIN labels ON lower(...)`, `(block_number / w) * w` |
| `src/analytics.rs:2411-2432` `get_row` | point-read fallback | `SELECT * FROM "t" WHERE block_number = N AND log_index = M LIMIT 1` |
| `src/analytics.rs:1401-1456`, `:3420-3438` | result materialisation | `prepare`/`query([])`, `column_names()` after execute, `ValueRef` → JSON (§5), row cap n+1, 64 MiB byte cap |
| `src/analytics.rs:878-935`, `:1247-1276` | timeout | `conn.interrupt_handle()` from a watchdog thread; raw `Interrupted!` mapped to budget text (#529) |
| `src/analytics.rs:3194-3287`, `:3063-3137` | `nuthatch check`, RFC-0041 entity output columns, error enrichment | bare `open_in_memory()` with **no lockdown**, `stmt.column_names()` |
| `src/graft.rs:131-153, 263-266, 1164-1198` | RFC-0033 graft canonical plan | `json_serialize_sql` key order per version, `SELECT version()`; "DuckDB changed CTE semantics at 1.4" (`:24-26`) |
| `src/entities.rs:314-321, 391-420` | RFC-0041 entity shape gate | `json_serialize_sql`; `duckdb_functions() WHERE function_type='aggregate'`; relies on parser alias canonicalisation (`percentile_cont`→`quantile_cont`, #969) |
| `src/entity_lower.rs:53-58`, `src/dune_views.rs:50, 120-148`, `src/authored_entity_spike.rs:188-227, 445-473, 1156-1204` | DBSP lowering, Dune translator, RFC-0041 oracle | `json_serialize_sql`; oracle uses `CREATE TABLE ... HUGEINT`, `duckdb::params!`, `SUM(...)::VARCHAR` |
| `src/graph_query.rs:937, 1160-1168, 1421, 2041`; `src/port_emit.rs:1127, 1203`; `src/recipes.rs:46-137` | GraphQL→SQL, port view emitter, recipes | text only; emit `coalesce((SELECT to_json(list(t.s)) FROM (SELECT struct_pack("k" := ...))), '[]')`, `lpad/translate/split_part/substr/length` numeric sort key, `LIKE ... ESCAPE '\'`, `last(x ORDER BY ...) FILTER (WHERE ...)`, HUGEINT net-balance views; executed via `serve.rs:2340 analytics::query_guarded` |
| `src/main.rs:559-592, 729-760` | `nuthatch sql` local / REPL | `analytics::query_hot_cold` (30 s, 50,000 rows); `.tables`/`.schema` use `information_schema.tables/columns` and `starts_with()` |
| `src/serve.rs:2838-2900, 3087-3135, 3137-3260, 3402-3466` | `/sql`, `/explain` (`SELECT * FROM (<q>) AS _explain LIMIT 0`) | §5 |
| `src/mcp.rs:289, 339-350, 408-505` | MCP `sql` tool | HTTP to `/sql?q=&max_rows=200` |
| `src/sqlmemo.rs:42-79` | memo (#1186) | textual scan for DuckDB volatile names (`random`, `uuid*`, `now`, `today`, `current_*`, ...) |
| `src/sql_errors.rs:16-232` | error hints | matches DuckDB message text verbatim (§5) |
| tests: `duckdb_extensions_are_static.rs:50-113`, `concurrent_sql_does_not_corrupt.rs`, `engine_batch_boundary.rs:74-95, 594`, `e2e_trino_contract.rs:281-288, 417-465`, `e2e_solo.rs:1045-1082`, `e2e_minio_publish.rs:141-147` | tests only | `SET extension_directory`, `COPY ... TO (FORMAT PARQUET)`, `ATTACH ':memory:'`, `SELECT * REPLACE`, `CAST(value AS HUGEINT)`, bind probes |
| `tools/df-gate/src/{main.rs:490-508, bin/view_exec.rs:38-143, bin/view_dialect.rs:17-64}` | gate tooling, outside the workspace | own bundled duckdb; only exhaustive `Value` match in the repo |

Build: `Cargo.toml:92` `duckdb = { version = "1", features = ["bundled", "parquet", "json"] }`, no httpfs; `.cargo/config.toml` `CXXFLAGS=-g0` (1.5 GB of DWARF in a 1.7 GB rlib).

Workarounds in comments: compressed materialisation off in every build (`analytics.rs:317-334`, #1152/#1165, `D_ASSERT(min_val <= input)` on a filtered `ORDER BY` over one segment); per-instance spill dirs (`:177-285`); `allowed_directories` inert until `enable_external_access=false` (`:24-29, 296-299`, #289); `conn.prepare` is not single-statement, `SELECT 1; COPY ... TO` wrote files (`:1715-1731`, #153); `"read_csv"(...)` quoted-name evasion (`:2109-2121`); `json_serialize_sql` refuses non-SELECT, so `CREATE VIEW` is split by hand (`:1916-1978`, `graft.rs:1048`); eager binding re-read every footer per request, `SELECT 1` = 1.2 s / 32,000 `openat` (`:2832-2842`, #1183); `No files found that match the pattern` re-planned once (`:868-876, 1004-1030`, #1162); `Invalid Error: don't know what type:` names nothing (`sql_errors.rs:160-232`, #433).

## 2. SQL feature census

Views are not wrapped: each file carries its own `CREATE VIEW` statements. `checks/*.sql` are bare SELECT/WITH run through `analytics::query` (`check.rs:105`). Scripts and raw counts: `census.py`, `refs.py` (counts are case-insensitive after comment stripping). *[Filing note: the report gave a scratchpad path. The scripts and their outputs are now kept in [probes/census](probes/census/README.md), and both reproduce byte for byte.]*

**Files.** 154 `.sql` on disk; 106 unique by md5; 97 unique authored files once `tests/fixtures/dune_emit` and `tools/` are excluded: 4,275 lines, 132 statements (118 `CREATE VIEW`, 14 checks). 26 of the 97 are the commented-out `10-example.sql` starter (0 statements). `examples/`, `skills/`, `docs/` hold no `.sql`.

| nest (unique files) | files | lines | stmts | views |
|---|---|---|---|---|
| graph-allocations | 18 (4 checks) | 1842 | 26 | 22 |
| qos-reo-typed | 11 (3 checks) | 532 | 27 | 24 |
| qos-reo wt-peers / wt-range / original (unique only) | 5 / 1 / 3 | 346 / 56 / 186 | 19 / 4 / 11 | 34 |
| arcaidia | 10 | 313 | 10 | 10 |
| perpl | 2 | 127 | 5 | 5 |
| graph-gns | 5 (2 checks) | 128 | 9 | 7 |
| graph-tap-escrow | 5 (3 checks) | 110 | 7 | 4 |
| graph-staking | 4 (2 checks) | 57 | 5 | 3 |
| obib-case2, dips, nests-mvp/subgraph-availability-oracle, hackathon, epoch-block-oracle | 2/1/2/2/1 | 178 | 9 | 9 |
| opolis, peeranha, spookyswap, demo-usdc, obib-case3, 16 nests-mvp, 4 evaluation variants | 25 starters | 400 | 0 | 0 |

qos variants: wt-publisher = typed except a 3-line publisher address; wt-peers differs from typed by 3/21/31/63/35 lines across five files; wt-range = wt-peers except `40-qos-freshness.sql`. Only wt-peers/wt-range use `QUALIFY`, named `WINDOW`, `range()`; only typed/original use `TRY()`, `from_json`, `json_type`, `unnest`.

Docs and skills: 497 files scanned, 229 SQL snippets (141 runnable, 50 templates); ~80 distinct statements; nearly all `count(*)`/`sum` with `TRY_CAST(... AS DECIMAL(38,0))`, `to_timestamp`, `date_trunc`, `round`, `arg_min`, `LEFT JOIN`, `USING`. 39 of the 141 runnable snippets are `SELECT count(*) FROM transfers` from the skill template at `src/project.rs:1885`, a table no nest has. `claude-skills/nuthatch-builder` is a symlink into `nuthatch/skills`, counted once. No `semantic.toml` or `nuthatch.toml` carries SQL except `[[webhooks]] where`, spliced raw (`webhooks.rs:103-114`).

Catalogue shape assumed: 117 distinct unqualified base tables in `main`: 107 `<alias>__<event>`, 6 `[[calls]]` tables, 4 IPFS tables; 55 views reference other views; no view reads `blocks`, `labels`, `*__children`, `_meta`, `transactions`. Implicit columns relied on: `block_number` (333 refs), `log_index` (163), `block_timestamp` (120), `tx_hash` (62), `address` (44), `tx_from` (3), `<col>_dec` (25 names). 543 double-quoted identifiers (camelCase params, `"from"`, `"to"`).

**Feature table (97 authored files; nests: GA graph-allocations, GG gns, GS staking, GT tap-escrow, AR arcaidia, PP perpl, Q/QT/QP/QR qos variants, OB obib-case2, HK hackathon, DI dips, EB epoch-block-oracle) with the DataFusion 55.0.0 class from §3:**

| feature | count | nests | representative | class |
|---|---|---|---|---|
| `CAST(x AS HUGEINT)` (+2 TRY_CAST) | 305 | GA, PP, OB | `graph-allocations-nest/views/40-lodestar-allocations.sql:63` | **d+b**: `HUGEINT` is in DF's unsupported-type arm (`datafusion-sql/src/planner.rs:897-912`); rewrite to `DECIMAL(38,0)`; range 10^38-1 < 2^127, and DF arithmetic is `add_wrapping`/`mul_wrapping` (`physical-expr/src/expressions/binary.rs:94, 624, 633`) and `SUM` accumulators wrap (`functions-aggregate/src/sum.rs:316, 499, 551`) where DuckDB raises `Out of Range Error` on `+`/`*` (probed) |
| CAST → BIGINT / VARCHAR / DOUBLE / INTEGER / BOOLEAN / DATE | 130/68/47/18/1/1 | 8 nests | | a |
| CAST → DECIMAL(38,0) | 5 | GS, GT, OB | | a (Decimal128) |
| `::VARCHAR`, `::DOUBLE` | 41 | GA, GS, GT, QT, QP | | a |
| `TRY_CAST` | 2 (+ every generated `_dec`) | OB | | a (`Expr::TryCast`, `expr/mod.rs:365-375`) |
| `TRY(expr)` | 3 | QT | `qos-reo-nest-typed/views/15-qos-postings.sql:16` | e: sqlparser 0.62 has no `TRY(` function form |
| `CAST(('0x' \|\| hex) AS BIGINT)` | 2 | GA | `90-lodestar-indexers.sql:224` | c: Arrow Utf8→Int64 cast does not parse `0x` |
| CTEs (138 defs, 38 WITH, max 15/statement) | 138 | 9 | | a |
| unaliased derived tables | 48 | | | a (`relation/mod.rs:216-219`) |
| scalar subqueries / `IN (SELECT)` / `NOT EXISTS` | 39/8/4 | GA, GT, Q | | a for uncorrelated; correlated forms depend on DF's decorrelation, not verified per query |
| JOIN / LEFT / FULL / CROSS / comma / `USING` | 37/69/4/1/3/4 | 10 | | a |
| **ASOF [LEFT] JOIN** | 8 | GA | `90-lodestar-indexers.sql:45` | e: sqlparser only parses Snowflake `ASOF JOIN ... MATCH_CONDITION` (`parser/mod.rs:15769-15780`); needs a textual rewrite to a window/lateral form before parsing |
| LATERAL | 2 | GA, Q | | a (`relation/join.rs`) |
| UNION ALL / UNION | 160 / 2 | 10 | | a |
| GROUP BY / ordinal / HAVING | 108/51/4 | 13 | | a |
| **GROUP BY ALL** | 20 | QT, QP, QR | `05-qos-publisher.sql:25` | a (`select.rs:262-271`) |
| **QUALIFY**, named WINDOW | 2, 3 | QP, QR | | a (`select.rs:129-130, 273`) |
| OVER / PARTITION BY / `ROWS UNBOUNDED PRECEDING` / explicit frame | 33/27/3/2 | 9 | `arcaidia-nest/views/60-fee_snapshots.sql:14` | a |
| row_number / dense_rank / lag / lead | 17/2/1/2 | | | a (`functions-window/src/lib.rs:69-82`) |
| **FILTER (WHERE)** | 62 | 7 | | a |
| sum / count / count(DISTINCT) / min / max | 214/61/22/26/31 | | | a (sum of Decimal128 widens precision to 38 and wraps: b) |
| **arg_max / arg_min** (8 with list-valued key `[a,b]`) | 11/17 | AR, QT, QP | `qos-reo-nest-typed/views/30-qos-daily.sql:30` | c: not in DF's aggregate set; `first_value(x ORDER BY k)` covers scalar keys (d), list keys need a UDAF |
| `list(struct ORDER BY k)` | 1 | GA | `90-lodestar-indexers.sql:360` | d: `array_agg(... ORDER BY)` |
| CASE / COALESCE / NULLIF / GREATEST / LEAST / IS DISTINCT FROM / BETWEEN / LIKE | 39/112/73/12/1/12/4/4 | | | a |
| `//` integer division | 24 | GA, Q, QT, QP | | a (`expr/binary_op.rs:65` DuckIntegerDivide → IntegerDivide) |
| `/` | 74 | 8 | | **b**: DuckDB `/` on integers, HUGEINT and DECIMAL yields DOUBLE (probed); DF divides integers integrally and decimals as decimal |
| `%` on HUGEINT, unary minus (40 of them `-CAST(... AS HUGEINT)`) | 1, 50 | GA, AR, OB | | a on Decimal128 |
| `POWER(10,n)`, `abs`, `1e6` literals | 9/12/7 | PP, QT | | a |
| `to_timestamp(ubigint)` | 3 | GG, GS, PP | `graph-gns-nest/views/20-activity.sql:4` | b: DuckDB returns TIMESTAMPTZ (session zone); DF returns naive Timestamp |
| `date_trunc` | 2 | GG, GS | | a |
| `DATE '...'` + integer | 10, 8 | Q, QT, QP | `wt-peers/views/10-qos-documents.sql:11` | b: DF coerces Int to `Interval(MonthDayNano)` (`type_coercion/binary.rs:2008`); unit not verified |
| lower / `\|\|` / strpos / substr (7 with negative start) | 184/72/7/15 | | `85-lodestar-params.sql:22` | a, except negative `substr` start: b (Postgres semantics, not from-the-end) |
| `string_split(x,'')` | 7 | GA | | c/could not tell: DF `string_to_array` exists; empty-delimiter behaviour not verified |
| `from_hex` / `unhex` / `decode(blob)` | 2/2/4 | GA, Q, QT | `90-lodestar-indexers.sql:223` | d: `decode(x,'hex')`, `arrow_cast(...,'Utf8')` |
| **list comprehension** `[e FOR c IN l]` | 7 | GA | `85-lodestar-params.sql:21` | e: not in sqlparser 0.62 |
| **`list_reduce(l, lambda acc, d: ...)`** | 8 | GA | `85-lodestar-params.sql:23` | c+could not tell: sqlparser parses `lambda` (`parser/mod.rs:1615`) and DF 55 has higher-order `array_transform`/`array_filter` with lambdas, but no reduce |
| list_prepend, list literals, struct literal `{'k': v}` | 1/17/4 | GA | | a (`list_*` aliases in functions-nested; `SQLExpr::Dictionary` at `expr/mod.rs:677`) |
| `unnest(...)` in select list, `range(a,b)`, `VALUES (...) AS t(c)` | 4/1/9 | Q, QT, QP, GA | | a |
| `from_json(x,'<structure>')`, `json_type`, `json_extract_string` | 4/2/16 | Q, QT, AR | `arcaidia-nest/views/10-vault_policy.sql:12` | c: no JSON functions in DF 55 core (external `datafusion-functions-json` exists, not checked) |
| generated: `read_parquet([...], union_by_name=true)` | every base view | | `analytics.rs:2664` | d: no SQL `read_parquet` in DF; register a TableProvider with a name-merged schema (`ListingTable` + SchemaAdapter) |
| generated: `UNION ALL BY NAME` | every base view | | `analytics.rs:2682` | a (`set_expr.rs:150`, `builder.rs:882`) |
| generated: `CAST(NULL AS UBIGINT)` | every base view | | `analytics.rs:3382-3417` | d: `UBIGINT`, `UTINYINT`, `USMALLINT` are unsupported type names (`planner.rs:897-912`); `BIGINT UNSIGNED` works (`:740`) |
| generated: `CREATE OR REPLACE VIEW` | all | | | a (`statement.rs:318`) |
| generated: `CREATE TEMP TABLE` + Appender | hot tip | | `analytics.rs:2794-2807` | d: "Temporary tables not supported" (`statement.rs:364-365`); register a MemTable from RecordBatches |
| generated: `read_json(format='array', columns=...)` | labels | | `analytics.rs:2335` | c: DF JSON datasource is NDJSON only; load in Rust |
| generated: `QUALIFY row_number()`, `lower`, `HAVING SUM<>0`, `COUNT(*)::VARCHAR` | children, folds | | | a |
| generated: `(block_number / w) * w` | `cold_velocity` | | `analytics.rs:2299` | b, and a latent bug: DuckDB returns DOUBLE, `r["ws"].as_u64()` (`:2306`) is `None` for `5.0`, so every cold velocity row is dropped; DF would return UInt64 and change behaviour. No test covers it. |
| generated: `to_json(list(...))`, `struct_pack("k" := ...)` | graph_query | | `graph_query.rs:1160-1168` | c + d (`named_struct`, JSON UDF) |
| generated: `last(x ORDER BY ...) FILTER`, `LIKE ... ESCAPE '\'` | port_emit, graph_query | | | d (`last_value`), a (`escape_char` at `expr/mod.rs:501`) |
| `json_serialize_sql` | 7 sites | | | d: replace with a sqlparser AST walk (DuckDbDialect parses `//`, `EXCLUDE`, lambdas, dictionaries, `FROM`-first, trailing commas; `dialect/duckdb.rs:27-135`) |
| `duckdb_views()/tables()/functions()`, `information_schema.tables/columns` | analytics, entities, CLI | | | b: `information_schema.{tables,views,columns,routines}` (`catalog/src/information_schema.rs:51-57`); column names differ (`view_name`→`table_name`, no `sql` body column for views without keeping DDL) |
| `EXPLAIN (FORMAT JSON)` operator whitelist | admission | | `analytics.rs:588-676` | d: DF has no JSON physical-plan format with DuckDB's operator names; walk the `ExecutionPlan` tree in Rust instead |
| `json_serialize_sql` alias canonicalisation, `version()`, `SET disabled_optimizers`, `allowed_directories`, `interrupt_handle` | entities, graft, lockdown, timeout | | | d/n.a.: DF has `version()`; no filesystem sandbox is needed if no file-reading table functions are registered; cancellation is dropping the stream |
| not used anywhere: UHUGEINT, INT128, BLOB/TIMESTAMP/INTERVAL/UUID/MAP types, WITH RECURSIVE, PIVOT, EXCLUDE/REPLACE, DISTINCT ON, ORDER BY ALL, `->` lambdas, epoch_ms, strftime/strptime, regexp_*, string_agg, median/quantile, first/last_value, PRAGMA/SET, `$$`, backticks, `read_parquet` in views | 0 | | | |

Storage facts a DuckDB reader takes for granted (`seal.rs:509-526`, `schema.json`): only four UInt64 columns; every ABI value is nullable Utf8 (uint256 as decimal text, bools as `'true'/'false'`, addresses lowercase hex); `_dec` = `TRY_CAST(text AS DECIMAL(38,0))`. Views lean on: VARCHAR `= 10`/`IN (10,11)`/`= true` binding by implicit cast while `> 9` is a binder error; `max(varchar)` being lexical; two views comparing epochs as text `BETWEEN` (`wt-peers/views/10-qos-documents.sql:43,75`); quoted identifiers resolving case-insensitively (DF quoted identifiers are case-sensitive, class b); DECIMAL(38,0) `sum` producing a 39-digit result without error (probed). One inconsistency worth knowing before relying on "DuckDB refuses on overflow": `SUM(HUGEINT)` over a two-row `UNION ALL` **wrapped** to -2^127 on the 1.5.5 CLI, while the constant-folded `range(3)` form raised `Overflow in HUGEINT multiplication`; `+` raised `Overflow in addition of INT128`.

## 4. Prior work

| doc | decision / numbers |
|---|---|
| `docs/rfcs/0013` (2026-07-18) | DataFusion is the destination, benchmark-gated. Gate 2026-08-02 on `net_balances`: DuckDB/DF 41/76 ms at 2M rows (1.85x), 95/244 at 8M (2.57x), 229/606 at 20M (2.65x), parity identical; +56 crates; MSRV 1.85→1.88 the only blocker (`:144-170`). Verdict "gate not met" (`docs/bench/rfc-0013-datafusion-gate.json`). |
| `docs/rfcs/0041` (implemented 2026-08-28) | DuckDB kept in four roles: parser, reference oracle, restart seed, entity serving (`:211-230`). Overflow is a fault, not a wrap; DF #17539 wrapping named the number-one risk (`:173-199`). `indexer_rewards` p50 2.15 s → 87.7 ms. §13: Lodestar's 2.4 s ledger view (2.02 s ASOF arms) not expressible in v1; #1189 memo is the mitigation. |
| `docs/rfcs/0042` (parked 2026-08-30, KEEP DuckDB at 78 %) | §3a binary rule (`:69-87`): "Only two end states are acceptable: 1. DuckDB stays, in every role slice 0 inventoried ... 2. DuckDB goes entirely ... It does not execute a user's query in a shipped binary under any condition, including a fast path for one size band." Expected vs realised (§7 `:191`, §14 `:273-317`): build time partly (DuckDB 10.6 % of a 223 s Linux clean release, 8.0 % macOS; wasmtime 21.3 %); target disk yes (245 MB objects, 93 % of native bytes); binary size unquantified (A3 inconclusive, 102,449,024 bytes byte-identical); cross-compilation/containers no named beneficiary in 347 issues; toolchain Linux only (GLIBCXX_3.4.29). Kept regressions: 5 of 6 roles unbuilt; HUGEINT compat layer (4/5 views rewritten, #996); `port_queue` stops binding; DF wraps and the Rust operator shipped an i128 wrap (#998, three sites); general SQL 2.53-2.80x slower at 10k segments (#964/#981). Struck: concurrency claim (harness mutex, 14.7 vs 81.5 qps). Reopen 2027-09-01, or a named musl user, five roles built in 2-day boxes, DF #17539 closed, or #357 scheduled. |
| `0042-slice0-bom.md` | libduckdb-sys 352 objects / 245,073,046 bytes; Linux binary 102,359,456 bytes; macOS 87.7 MB; eight DuckDB sites. |
| `0042-slice5-decision-input.md` | fold 24/24 parity, Rust operator 0.55-0.85x of DuckDB; views 5/5 parity on 248,487 rows at 0.81-1.64x; DF general SQL 2.53-2.80x; candidate RSS unmeasured; five published claims corrected. |
| `0042-slice6-report-...ec26929f.md` / `...994c939b.md` | horizon-nest 10,480 segments: mutex 14.76 qps flat, gate16 81.5 qps at 32 clients, unbounded 1,313 MB RSS, gate2 135 MB; A4 12 shapes/12 operators all stock in DF; KEEP at 78 %. Second report: A3 static link failed (`ldd` still lists libstdc++.so.6), no admissible decision. |
| `docs/rfcs/0047` | Does not reopen the engine; C4 turns 512 MB / 2 threads / 2 permits into settings under `(permits × memory_limit) + ingestion_reservation + headroom ≤ 2 GiB`. Native 256-bit arithmetic explicitly out of scope. |
| `docs/bench/rfc-0042-*` | slice2: DF 0.84x at 2M, 2.38-2.78x at 8M, 2.56-2.60x at 20M. 964: 1→10,000 segments DuckDB 68→310 ms, DF 37→856 ms. 987: Rust fold operator 0.53-0.82x at all 24 configs (first version 8.04x slower, fixed with rustc-hash + rayon). 992/997: restart-to-ready 49.6 ms/0 seg, 196 ms/1,000, 801 ms/5,000, 1.7 s/11,000. 996: 38,428 segments, 5/5 parity, HUGEINT 4/5. slice4: DF parses 27/27 views, plans 16/24, all 8 failures HUGEINT; 14 of 22 first-run failures were probe bugs. |
| `docs/frozen-for-2027.md` | Freezes #280 (Turso), #357 (derivation reuse, a §14 reopen trigger), #278 (revm). RFC-0042 is not listed; it carries its own reopen conditions. |
| `tools/df-gate` | `main.rs` RFC-0013 gate with checked i128 (#998/#1014); `view_dialect` 27/27 parse; `view_plan` 16/24; `view_exec` 5/5; `parser_probe` alias canonicalisation; `fold_profile` decode 98 ms + fold 224 ms on 200k rows. Own Cargo.toml, outside the workspace. |

DuckDB operational failures as the repo describes them:
- #1152: debug builds switch off compressed materialisation, whose assertion aborts on a filtered `ORDER BY` over one segment (`src/analytics.rs:317-322`, `docs/releases/v3.4.0.md:67-69`).
- #1165: concurrent `/sql` segfaulted a production nest, 25 SEGVs in a minute at four permits; spill directories overwrote each other (`tests/concurrent_sql_does_not_corrupt.rs:1-3`, `analytics.rs:177-192`, `docs/releases/v3.5.1.md:4-19`); permits now share `min(512 MB, 1024 MB/permits)` (`v3.5.0.md:50-55`).
- #1183: every `/sql` re-bound every authored view; `SELECT 1` 1.24 s / 32,000 file opens → 20 ms; 47 statements 158 s → 89 s (`docs/releases/v3.5.1.md:23-31`).
- #1186: deterministic memo for repeated statements, 89 s / 42 s across four statements (`src/sqlmemo.rs:1-4`, `docs/releases/v3.6.0.md:4-20`).
- #1189: the PR shipping #1186, volatile-function and ordering fixes (`sqlmemo.rs:39`, `docs/rfcs/0041:535-537`).
- #1428: **not cited by number anywhere in the repo.** Commit `926238cb` and `.cargo/config.toml:2-4` (`-g0`; cold `cargo test --no-run` 24.1 GB/141 s → 16.2 GB/112 s, `docs/progress-log.md:16-22`) describe it; a build-size fix, not a runtime failure.
- #1429: cited only as the shared `tests/it.rs` integration binary (`.github/workflows/ci.yml:359`); **no DuckDB failure described.**
- `/sql` filesystem escapes: #153 `SELECT 1; COPY (...) TO '~/.zshrc'` wrote the file (`analytics.rs:1715-1731`, `docs/security-audit-2026-07-31.md:42`); audit finding 1 `"read_csv"('/etc/passwd')` (High, fixed 0.9.3, `:17, 30-52`); finding 2 `duckdb_settings()` path disclosure (`:53-71`); finding 5 the `json_serialize_sql` allowlist (`:106-118`); #289 `allowed_directories` inert until `enable_external_access=false` (`analytics.rs:24-29`); replacement scans (`:2151-2190`).
- Also: #318 first `read_parquet` downloaded an extension (1120 ms vs 4 ms); #430/#433/#435 corrupt segment reduces rather than fails; #529 `Interrupted!` leak; #840 cache keyed on content (497/500 mtime collisions on btrfs); #896 `SELECT 1` 2,465 ms on 38,428 segments; #1162 seal under a planned query; #946 GLIBCXX_3.4.29 floor.

## 5. How results are presented

Type mapping (`analytics.rs:3420-3438`, duckdb-rs `row.rs:451-457`):

| DuckDB result type | JSON |
|---|---|
| NULL, BOOLEAN | `null`, `true/false` |
| TINYINT..BIGINT, UTINYINT..UBIGINT | number |
| FLOAT, DOUBLE (`AVG`, any `/`) | float number (`5.0`) |
| HUGEINT and **every DECIMAL of scale 0, including `DECIMAL(38,0)`** (duckdb-rs returns `ValueRef::HugeInt` for scale-0 Decimal128) | **string** of digits: `SUM(value_dec)`, `SUM(block_number)` (UBIGINT sums widen to HUGEINT), `COUNT(*)::VARCHAR` |
| VARCHAR | string (lossy UTF-8) |
| DECIMAL with scale > 0 | `rust_decimal` Display *[filing note: a robustness detail was taken out of this public record and reported to Chief]* |
| TIMESTAMP, DATE, BLOB, LIST, STRUCT, MAP, INTERVAL, UUID | Rust `Debug` of `ValueRef`, e.g. `"Timestamp(Microsecond, 1700000000000000)"` |

So `COUNT(*)` is a number and any big or unsigned `SUM` is a string; the checks fixtures (`checks/expected/*.json`), `tests/e2e_solo.rs:280` and `tests/engine_batch_boundary.rs` depend on exactly this. Row objects are `serde_json::Map`; the CLI and MCP renderers take first-seen key order (`main.rs:775-790`, `mcp.rs:457`).

`/sql` (`serve.rs:2838-2900, 3412-3466`): `GET /sql?q=&max_rows=`; 16 KiB query cap, 30 s timeout, `max_rows` clamped to 50,000, 64 MiB result-byte cap, 2 permits (ceiling 16, 256 queued, 503 when saturated), hot tip capped at 2,000,000 rows / 64 MiB. Body `{count, truncated, degraded, degraded_tables, tip_unavailable, rows, cached, provenance:{as_of, sealed_through, source:"hot+sealed", registry_hash, nid, entities}}`. Errors: 400 `{error}`; 503 `{error, sealed_through}` or `{error, hot_source_bytes, budget, sealed_through}`. `/explain` returns `{valid:true}` or the same enriched error; `/q/{name}` adds `AdmissionRefusal` text.

Error text clients see (`sql_errors.rs:16-232`, `serve.rs:3768-3789`): the absolute nest dir becomes `<nest>`, then hints key on DuckDB's exact wording: `Table with name X does not exist`, `Referenced column "X" not found in FROM clause`, `Parser Error: syntax error at or near`, `No function matches ... 'sum(VARCHAR)'`, `Cannot mix values of type VARCHAR and BOOLEAN`, `bool_and(VARCHAR)`, `Out of Memory Error` (+ `max_temp_directory_size`), `Invalid Error: don't know what type:`; `missing_table_of` (`analytics.rs:3294`) and `segment_vanished` (`:949`) parse DuckDB text too. Nuthatch's own strings that tests pin: `only SELECT/WITH queries are allowed on the read-only SQL surface`, `query exceeded the {N}s time budget on the read-only SQL surface` (`engine_batch_boundary.rs:594`), `table function \`x\` is not permitted here - the SQL surface serves this nest's tables and views only`, `query uses forbidden filesystem/network function`, CLI `(result truncated at 50000 rows)`, MCP `… truncated at {n} rows - aggregate (GROUP BY), tighten the WHERE, or raise \`limit\`.` and the leading "⚠ incomplete" notice.

CLI: local mode calls `analytics::query_hot_cold` directly (same guard), `--json` prints one object per line, else an ASCII table, caveats to stderr; `.tables`/`.schema` need `information_schema.tables`/`.columns` (`table_name`, `column_name`, `data_type`, `ordinal_position`) hiding `__hot_*`. MCP: HTTP to `/sql` with `max_rows` default 200, fixed-width text, provenance line last.

## What a replacement must provide, ranked by how much authored SQL and traffic depends on it

1. **The catalogue nuthatch builds per query**: 117 unqualified base tables over explicit segment lists with by-name schema union (`union_by_name`), NULL-typed declared columns, `_dec`/`_overflow` derivations, a hot-tip table appended from JSON rows, `CREATE OR REPLACE VIEW` re-run per request, and views over views in file order. Every statement on every surface depends on it. DF needs a TableProvider/MemTable route for `read_parquet(...)`, `CREATE TEMP TABLE` and the Appender, and `UBIGINT` spelled `BIGINT UNSIGNED`.
2. **Exact big-integer semantics**: 305 `CAST AS HUGEINT`, 40 `-CAST(... AS HUGEINT)`, `//` and `%` on HUGEINT, `SUM` of `_dec`, and the trusted `net_balances` fold. DF has no i128 type and wraps on overflow; either an `Int128`-like UDF surface or a Decimal128(38,0) rewrite with checked arithmetic, and the JSON string encoding of results must be reproduced.
3. **Result encoding and error text**: numbers vs digit-strings per type, `Debug` fallbacks, the `/sql` body shape, and the DuckDB error phrases `sql_errors.rs` and the tests match on.
4. **The parser role**: `json_serialize_sql` at 7 sites (security allowlist, reachability, graft canonical plan, RFC-0041 gates, DBSP lowering, Dune translation), `duckdb_views()/tables()/functions()`, alias canonicalisation, `information_schema` for the REPL. Replaceable with sqlparser + DF's information schema, but the allowlist currently fails open, so the replacement has to be at least as strict.
5. **`/` yielding DOUBLE, implicit VARCHAR casts, case-insensitive quoted identifiers, TIMESTAMPTZ from `to_timestamp`**: 74 `/`, 543 quoted identifiers, `= true`/`IN (10)` on text columns. Class-b differences that silently change results.
6. **DuckDB-only syntax with no DF path**: 8 ASOF joins, 7 list comprehensions, 8 `list_reduce` lambdas, 3 `TRY()`, plus 28 arg_min/arg_max, 22 JSON function calls and 6 `from_hex`/`decode` calls. All but the JSON calls are graph-allocations (Lodestar) and qos; they need rewrites or UDFs.
7. **Resource walls**: 512 MB / 2 threads / per-instance spill / interruptible timeout / 50,000-row and 64 MiB caps; DF's memory pool and disk manager cover the shape, not the settings names.
8. **Admission bounding**: the `EXPLAIN (FORMAT JSON)` operator whitelist for `/q/{name}` must be rebuilt over DF's physical plan.

Not verified: DF's decorrelation of GA's correlated scalar subqueries per query; `string_to_array(x, '')` behaviour; the interval unit DF assigns to `DATE + int`; whether an external JSON UDF crate matches `json_extract_string` paths; DuckDB 1.5.4 (bundled) vs 1.5.5 (CLI) for the probes above.
