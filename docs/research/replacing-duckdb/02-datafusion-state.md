# 02 · Renting DataFusion, and the other Rust engines

Investigation 02, reported 2026-09-16 by a research agent (Fable 5.1). The report is kept verbatim
below.

**Method.** The agent read the crate source in the local cargo registry, ran probes against
datafusion 55.0.0 in a throwaway binary, and read the web. The probe is kept at
[probes/dfprobe](probes/dfprobe/src/main.rs); run it with `cargo run` in that directory.
Each claim carries a marker: [code], [probe], [doc] or [vendor].

**Spot-checked against the registry source before filing:**

- the grouped SUM accumulator's `add_wrapping` closure (`sum.rs:316`);
- `DECIMAL256_MAX_PRECISION = 76` (`arrow-schema-59.2.0/src/datatype.rs:888`);
- `BinaryExpr::fail_on_overflow` defaulting to `false` (`binary.rs:94`);
- `collect_statistics` defaulting to `true` (`config.rs:847`).

All four match.

## Headlines for the plan

- **The design holds.**
  - Footer, listing and statistics caches exist and are shared per `RuntimeEnv`.
  - A custom `TableProvider` or `ParquetFileReaderFactory` can hand DataFusion Burrmill's own file
    list and parsed footers.
  - Operator substitution by `PhysicalOptimizerRule` is used in production by InfluxDB 3 and others.
  - `SQLOptions`, an empty table-factory map and a custom catalog give a genuine positive allowlist.
- **Overflow is Burrmill's to own, and it is not a switch.**
  - Every built-in SUM path wraps, including the partial merge.
  - `+`, `-` and `*` wrap unless a rule sets `fail_on_overflow`.
  - Decimal arithmetic does not check precision.
  - Decimal256 **cannot hold `type(uint256).max`** (probe: the CAST errors), so values of 10^76 and
    above need their own representation or a loud refusal.
- **An unconfigured DataFusion is open.** Default `SessionContext::sql` ran
  `CREATE EXTERNAL TABLE ... LOCATION '/etc/hosts'`, read it back, and ran `COPY ... TO` a file. The
  allowlist is mandatory, not a refinement.
- **The dialect gaps are silent.**
  - `/` is integer division in DataFusion and float division in DuckDB.
  - NULL ordering on DESC differs.
  - `'1' + 1` fails to plan.

  Parity checking has to catch these, because nothing errors.
- **Real risks.**
  - Joins do not yield to cancellation: #19358 is open and its fix was abandoned.
  - The memory-pool issues #20714 and #24994 are open, and they sit in the regime of the 256 MB
    gate.
  - Each major release carries dozens of breaking changes (43 in 55.0).
  - Per-file open cost stays per file whatever the caches do.
- **No alternative engine is better.** DataFusion is the only maintained embeddable crate with
  both the SQL breadth and Decimal256. Polars stops at 38 digits and is slow listing many files;
  GlareDB has been idle for ten months; Databend is not a library; Sail is a server; Feldera needs a
  JVM to compile SQL; Turso and GlueSQL are not analytical.

---

## The report, verbatim

# Renting DataFusion for Burrmill: the facts, as of 2026-09-16

Legend: **[code]** read in `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/` (datafusion-*-55.0.0, arrow-*-59.2.0, parquet-59.2.0, sqlparser-0.62.0; paths below are relative to that). **[probe]** executed against datafusion 55.0.0 by a throwaway binary in the scratchpad (`dfprobe`, no repo touched). **[doc]** docs/blog/issue. **[vendor]** benchmark or vendor claim. Note: the registry had no DataFusion sources until `cargo fetch --locked` populated it; the burrmill `Cargo.lock` resolves datafusion 55.0.0 against arrow 59.2.0.

## 1. Releases

- Latest: **55.1.0, 2026-09-11**; 55.0.0 2026-08-18; 54.1.0 07-21; 54.0.0 06-08; 53.0.0 03-23; 52.0.0 01-12; 51.0.0 2025-11-19 [doc, crates.io API]. Cadence: a major every 2-3 months, minors between. 56.0.0 planned "Sep/Oct 2026" (issue #24461, open).
- MSRV: **1.94.0** for 55.0/55.1 (`datafusion-55.0.0/Cargo.toml:14` [code]; crates.io `rust_version`). Nuthatch's 1.95.0 clears it; 56 has no MSRV stated yet.
- What changed since 52 that matters: metadata cache (50.0, PR #16971), list-files cache on by default (52.0, PR #19366), file-statistics cache (52.0), `repartition_file_min_size` 10 MB to 1 MiB (55.0, #22439), aggregate spill hardening and "46% less memory" on mixed GROUP BY keys (55.0, [vendor]), pluggable spill backends (55.0). Nothing on overflow, Decimal256, SQLOptions or tokio in 53-55 [doc: blog posts 2026-04-02, 06-12, 08-25].
- Bump cost: the 55.0 upgrade page lists **43** breaking items (`TableProvider::scan` projection `Option<&[usize]>`, `ExecutionPlan::apply_expressions` and `GroupsAccumulator::convert_to_state` now required, catalog/planner traits moved to `datafusion-session`, `ListingOptions::collect_stat` removed, `CachedParquetFileReader` removed); 54.0 listed 29 (`as_any()` dropped from the core traits, `MemoryPool: 'static + Any`); 56.0 already lists 14 (physical filter pushdown resolves columns **by position**, missing Parquet null counts treated as unknown). https://datafusion.apache.org/library-user-guide/upgrading/55.0.0.html [doc]. Budget a day or two per major if you implement `TableProvider`/`ExecutionPlan`/UDAF traits; the traits churn every release.

## 2. Many small files

- **Listing.** `ListingTable::list_files_for_scan` (`datafusion-catalog-listing-55.0.0/src/table.rs:782`) goes through `ListingTableUrl::list_all_files` (`datafusion-datasource-55.0.0/src/url.rs:307`), which consults `cache_manager.get_list_files_cache()` at `url.rs:396` [code]. Default list-files cache: **1 MiB, no TTL** (`datafusion-execution-55.0.0/src/cache/cache_manager.rs:32-34`). One `ObjectMeta` per file; 38k files will overflow 1 MiB, so raise `datafusion.runtime.list_files_cache_limit` or the listing is redone per query. Side effect to know: it is session-global and DROP/CREATE did not invalidate it (#19573, fixed) [doc].
- **Footer cache.** Built in: `FileMetadataCache = dyn Cache<Path, CachedFileMetadataEntry>` (`cache_manager.rs:90`), default LRU **50 MB** (`:38`), key `datafusion.runtime.metadata_cache_limit` (`'0M'` disables). It lives on `RuntimeEnv.cache_manager`, so it is **shared by every query on that RuntimeEnv** and keyed only by object path plus `ObjectMeta` validity (`is_valid_for`, `:284`) [code]. The Parquet path consults it in `datafusion-datasource-parquet-55.0.0/src/metadata.rs:212-276`; page index is fetched eagerly when a cache is present (`:249`). No `datafusion.execution.parquet.cache_metadata` key exists in 55 [code].
- **Statistics.** `datafusion.execution.collect_statistics` **default true** (`datafusion-common-55.0.0/src/config.rs:847`). `list_files_for_scan` calls `do_collect_statistics_and_ordering` per file (`table.rs:834`), bounded by `meta_fetch_concurrency` **32** (`config.rs:949`), with a 20 MiB file-statistics cache (`cache_manager.rs:36`) validated by a schema fingerprint. First query over 10k files pays 10k footer reads; later ones hit both caches if the limits are large enough. Older evidence of the first-query cost: #16365 (8.9-10.4 s first query, "reading the footers"), #9219 (16-43% planning gain without stats) [doc].
- **Partitioning.** `target_partitions` defaults to core count (`config.rs:853`); `repartition_file_min_size` **1 MiB** (`:1555`); `FileGroupPartitioner` in `datafusion-datasource-55.0.0/src/file_groups.rs:131` splits by byte range. A 6k-file table is round-robined into `target_partitions` groups; each file is opened by a fresh `AsyncFileReader` from `ParquetFileReaderFactory::create_reader` (`datafusion-datasource-parquet-55.0.0/src/reader.rs:64`), i.e. one open, one footer fetch (or cache hit), one reader build per file. This is the same per-morsel cost Burrmill measured in roadmap 4.2b.
- **Known issues.** No upstream benchmark on thousands of tiny files was found; 55.0 claims "TPC-H Q22 68% faster" from the min-size change [vendor]. #25251 (2026-09-13, open) wants the Arrow schema derived from the footer at lazy open; #1187 (2021, open) listing before pruning.
- **Supplying a pre-known list and pre-parsed footers.** Three hooks, all real: (a) a custom `TableProvider::scan` returning `DataSourceExec` over a `FileScanConfig` you build from your own `PartitionedFile`s (`datafusion-datasource-55.0.0/src/mod.rs`, fields `object_meta`, `statistics`, `extensions`, `metadata_size_hint`, `arrow_schema`), so no listing at all; (b) `ParquetSource::with_parquet_file_reader_factory` (`source.rs:407`) with your own factory whose `AsyncFileReader::get_metadata` returns an `Arc<ParquetMetaData>` you already hold, the pattern shown in the 2025-08-15 "external Parquet indexes" blog; the trait doc warns to include the page index or it is re-fetched (`reader.rs:49-63`) [code]; (c) `CacheManagerConfig::with_file_metadata_cache` (`cache_manager.rs:497`) to plug a cache pre-warmed with `CachedParquetMetaData` (`metadata.rs:926`). `with_parquet_file_reader_factory` is per-source, so (b) needs a `PhysicalOptimizerRule` or your own provider to inject it.

## 3. Overflow semantics

- **Integer `+ - *`.** `BinaryExpr` carries `fail_on_overflow: bool`, default **false** (`datafusion-physical-expr-55.0.0/src/expressions/binary.rs:94`); `evaluate` dispatches to arrow `add_wrapping/sub_wrapping/mul_wrapping` unless it is set (`:623-633`); nothing in core sets it (`with_fail_on_overflow`, `:99`, has no non-test caller) [code]. Constant folding runs the same physical expr through `ConstEvaluator` (`datafusion-optimizer-55.0.0/src/simplify_expressions/expr_simplifier.rs:483`). **[probe]** `10000000000*10000000000` = 7766279631452241920; `9223372036854775807 + 1` = -9223372036854775808 both folded and per-row; `Int32 MIN + -1` = 2147483647; `Int32 MIN % -1` = **0** (arrow `rem` documents this, `arrow-arith-59.2.0/src/numeric.rs:77`), so #14771's error no longer reproduces; `-(Int32 MIN)` = Int32 MIN.
- **Decimal `+ - *`.** Same wrapping kernels; Decimal256 result precision capped at 76 (`datafusion-expr-common-55.0.0/src/type_coercion/binary.rs:1151`). [probe] `Decimal256(76) + Decimal256(76)` of 76 nines returned 1999...9 (77 digits, beyond declared precision, silently).
- **SUM.** `datafusion-functions-aggregate-55.0.0/src/sum.rs`: grouped `PrimitiveGroupsAccumulator` closure `|x, y| *x = x.add_wrapping(y)` (`:316`); `SumAccumulator::update_batch` uses `arrow::compute::sum` (itself wrapping, `arrow-arith-59.2.0/src/aggregate.rs:943`; `sum_checked` exists at `:897` and is unused) then `add_wrapping` (`:499`); `SlidingSumAccumulator` likewise (`:551,559`); `merge_batch` is `update_batch` (`:504`) so partial combine wraps too. Applies to Int64, UInt64, Decimal128, Decimal256 (`:81-100`). [probe] `SUM(Int64)` of MAX,1 = i64::MIN in both simple and grouped paths; `SUM(Decimal128(38))` of 38 nines + 1 = 10^38 (39 digits, no precision check). **AVG** also `add_wrapping` (`average.rs:717-1093`) but [probe] AVG over two 76-nines Decimal256 raised "Arithmetic Overflow in AvgAccumulator" (the division step is checked); do not rely on that.
- **CAST text to decimal.** `arrow-cast-59.2.0/src/parse.rs:910 parse_decimal` errors on `digits > precision` ("parse decimal overflow") and on anything non-canonical; `generic_string_to_decimal_cast` (`cast/decimal.rs:649`) maps errors to NULL when `CastOptions.safe`; DataFusion `CAST` uses `safe: false` (`expressions/cast.rs:38`), `TRY_CAST` `safe: true` (`try_cast.rs:90`) [code]. [probe] CAST of 39 digits to DECIMAL(38,0) errors, TRY_CAST gives NULL; `'7.9'` becomes **8**, `' 7 '` 7, `'1e18'` and `'1_000'` error/NULL. **Trap:** `DECIMAL256_MAX_PRECISION = 76` (`arrow-schema-59.2.0/src/datatype.rs:888`), and i256 tops at 2^255-1 (77 digits): `CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS DECIMAL(76,0))` **errors** [probe]. `type(uint256).max` allowances are common in ERC-20 data; Decimal256 cannot store them at all.
- **Issues.** #17539 (numeric overflow should error, 2025-09-12) open, assigned, no PR; #14771 (`MIN % -1`, 2025-02-19) open though the behaviour changed; #20034 (ANSI negate, 2026-01-27) open. **No ANSI or fail-on-overflow config in core** (no such key in `config.rs`) [code]. Scattered 2026 fixes only (#25129 date_trunc, #25234 interval propagation).
- **Enforcement points.** `SessionContext::register_udaf` / `deregister_udaf` (`datafusion-55.0.0/src/execution/context/mod.rs:1660,1701`) replace `sum` by name; `register_function_rewrite` (`:2127`), `add_analyzer_rule` (`:501`), `add_optimizer_rule` (`:486`), `with_physical_optimizer_rules` (`session_state.rs:1331`). A `PhysicalOptimizerRule` can rewrite every `BinaryExpr` to `.with_fail_on_overflow(true)`, which makes `+ - *` use arrow's checked kernels; SUM needs a replacement UDAF because the accumulator is hard-wired to `add_wrapping`. `i256` has `checked_add/sub/mul/div/rem/pow/neg` (`arrow-buffer-59.2.0/src/bigint/mod.rs:378-555`) so a checked Decimal256 UDAF is cheap to write.
- **Decimal256 coverage.** sum, avg, min/max, count, median handle it; stddev/variance do not (0 mentions) [code]. Arithmetic and casts cover it; open arrow-rs issues #10946, #10978 on decimal-to-decimal casts with large or negative scales [doc].

## 4. Security surface

- Default `SessionContext::sql` executes DDL, DML, COPY and SET: `context/mod.rs:688-761` dispatches `CreateExternalTable`, `CreateMemoryTable`, `CreateView`, `CreateCatalog[Schema]`, `Drop*`, `CreateFunction`, `SetVariable`, `Prepare/Execute/Deallocate` [code]. [probe] on a fresh context: `CREATE EXTERNAL TABLE pw STORED AS CSV LOCATION '/etc/hosts'` then `SELECT count(*) FROM pw` returned rows; `COPY (SELECT 1) TO '/private/tmp/...csv'` wrote a file; `CREATE VIEW` and `SET` succeeded. Local filesystem is registered on the default object-store registry (`datafusion-execution-55.0.0/src/object_store.rs:212`) [code].
- `SQLOptions` (`context/mod.rs:2280`) has exactly three switches: `allow_ddl`, `allow_dml`, `allow_statements`; `verify_plan` walks the logical plan after planning and refuses `Ddl`, `Dml`, `Copy`, `Statement` (`:2345-2354`). [probe] with all three false: CREATE EXTERNAL TABLE, COPY, SET, CREATE VIEW, INSERT refused; `SELECT 1` allowed.
- Table functions registered by core: **only `generate_series` and `range`** (`datafusion-functions-table-55.0.0/src/lib.rs:59-64`; [probe] `read_parquet('/etc/hosts')` fails "table function not found"). `SELECT * FROM '/etc/hosts'` fails unless `enable_url_table()` is called (`context/mod.rs:414`). datafusion-cli adds `parquet_metadata` and the `read_*` family in its own crate, not in core [doc].
- Positive allowlist: build `SessionStateBuilder` with `with_catalog_list` (`session_state.rs:1362`) holding only your `MemoryCatalogProvider`/custom `TableProvider`s, `with_table_factories(HashMap::new())` (`:1486`) so `CREATE EXTERNAL TABLE` has no factory, `with_object_store` left unregistered or a registry with no `file://`, `sql_with_options` with all three `SQLOptions` false, and `deregister_udtf("generate_series"/"range")` if you want zero table functions. 261 scalar, 46 aggregate, 11 window functions come in by default [probe]; use `default-features = false` and register only the function packages you want.

## 5. Memory and concurrency

- Pools (`datafusion-execution-55.0.0/src/memory_pool/pool.rs`): `UnboundedMemoryPool` (default), `GreedyMemoryPool` (`:77`), `FairSpillPool` (`:168`, "(pool_size - unspillable) / num_spillable" per spillable consumer, unspillable first-come), `TrackConsumersPool` wrapper (`:405`) [code]. Open: #24994 (2026-09-06) TrackConsumersPool serialises every grow/shrink on one lock; #20714 (2026-03) RSS exceeds budget; #25047 two Greedy consumers fight for the last KiB [doc].
- Spilling operators (files with `SpillManager` [code]): grouped hash aggregate (`aggregates/grouped_hash_stream.rs:1167 fn spill`), sort, sort-merge join, nested-loop join (54.0), repartition. Hash join spills only behind `enable_hash_join_spilling` (#24768, 2026-08) [doc]. Aggregate memory: #6937 closed by dynamic early-emit; today the partial phase has a skip-probe (`skip_partial_aggregation_probe_ratio_threshold` 0.8 / `_rows_threshold` 100_000, `config.rs:1015-1019`) and spills with sort-merge re-grouping (`grouped_hash_stream.rs:228-233`) [code]. 55.0 claims 46% less memory on mixed keys [vendor].
- Per-query pools: `TaskContext::with_runtime` (`task.rs:166`) lets one query run with its own `RuntimeEnv` (own pool) while sharing nothing else; the caches, however, hang off `RuntimeEnv`, so a per-query RuntimeEnv loses the footer cache unless you share the `CacheManager` by cloning its Arcs into each builder [code].
- Cancellation: `physical_plan::coop` wraps streams to consume tokio task budget per batch (`coop.rs:105-198`); `EnsureCooperative` rule wraps leaves and eager roots (`datafusion-physical-optimizer-55.0.0/src/ensure_coop.rs`). Only `DataSourceExec` and `MemorySourceConfig` streams are cooperative by construction (`file_scan_config/mod.rs:729`, `memory.rs:84`); joins are not. **#19358 open; PR #19360 closed unmerged 2026-05-08** [doc]. A hash join build over a large side can still block cancellation.
- Runtime: docs say use a separate tokio runtime for plans, not `spawn_blocking`; `thread_pools.rs` example carries a `CpuRuntime`; no `DedicatedExecutor` in core (#13692 open, PR #13690 closed) [doc]. `datafusion-common-runtime` only has `SpawnedTask`/`JoinSet` [code].
- Many concurrent queries: no scheduler-level fairness; the only cross-query arbitration is the memory pool, and FairSpillPool's fairness is per spillable consumer, not per query [code]. No upstream issue on cross-query fairness was found.

## 6. Custom physical operators

Three routes, all in 55: (1) `UserDefinedLogicalNode` (`datafusion-expr-55.0.0/src/logical_plan/extension.rs:32`) plus `ExtensionPlanner` (`datafusion-session-55.0.0/src/planner.rs:96`) registered through a `QueryPlanner` (`:34`, `with_query_planner` at `session_state.rs:1353`): an `AnalyzerRule`/`OptimizerRule` matches the fold subtree in the logical plan (CTEs are already inlined by then) and replaces it with your node; the planner maps it to your `ExecutionPlan`. (2) `PhysicalOptimizerRule` (`datafusion-session-55.0.0/src/physical_optimizer.rs:52`) matching `AggregateExec(UnionExec(DataSourceExec...))` and substituting your plan after the fact. (3) `TableProvider::scan` returning your own `ExecutionPlan`, which covers scan-with-projection but not the fold. Real users (GitHub code search, 2026-09-16): GreptimeDB `src/query/src/range_select/planner.rs` and `promql/extension_plan/planner.rs` (route 1), InfluxDB 3 `iox_query/src/exec/context.rs` (1) and eleven `physical_optimizer/*` rules (2), delta-rs `delta_datafusion/planner.rs`, cnosdb, cube, Sail, openobserve, spiceai `datafusion-dml/src/planner.rs`, Lance `io/exec/optimizer.rs`. Route 2 is the least code and survives CTEs and joins around the subtree; route 1 is the one upstream documents.

## 7. Build footprint

- Features (`datafusion-55.0.0/Cargo.toml [features]` [code]): defaults are `nested_expressions, crypto_expressions, datetime_expressions, encoding_expressions, regex_expressions, string_expressions, unicode_expressions, compression (liblzma, bzip2, flate2, zstd), parquet, recursive_protection, sql`. `avro`, `parquet_encryption`, `serde`, `math_expressions` are off. CSV/JSON/Arrow datasource crates are **unconditional** dependencies; only avro is gated. `default-features = false, features = ["parquet", "sql"]` is the shrink path; parquet keeps `default-features = true` on the `parquet` crate (all codecs).
- Figures: datafusion-cli 68 MB (v39) to 92 MB (main, 2024-12), .text 27.7 MiB (#13816); epic #24727 (2026-08-27, open) measures 70-145k IR lines per monomorphised target, "~3.3 bytes per IR line"; compile ~40 s for the core crate on an unspecified machine (#13814, 2024) [doc]. No 2026 published before/after for the feature cut.
- Component crates: practical. `datafusion-expr` has ~92 reverse deps (lance, vegafusion, sedona), `datafusion-physical-plan` 40, `datafusion-sql` 14 [doc, crates.io]. The umbrella is mostly re-exports plus `SessionContext`/`SessionState`/`physical_planner.rs`; you can take `datafusion-sql + -expr + -optimizer + -physical-expr + -physical-plan + -physical-optimizer + -datasource-parquet + -functions-aggregate + -session` and write your own planner glue, but the `DefaultPhysicalPlanner` lives in the umbrella, so you either take it or rewrite ~3k lines.

## 8. Dialect

- `DuckDbDialect` (`sqlparser-0.62.0/src/dialect/duckdb.rs`) enables: trailing commas, `FILTER` on aggregates, bitwise shifts, named args `=`/`:=`, `{'a':1}` struct and map literals, lambdas, FROM-first, `* EXCLUDE`/`REPLACE`, `ORDER BY ALL`, `NOTNULL`, `INSTALL/LOAD/DETACH`, `T[]` typedefs, comma-separated TRIM [code]. `GROUP BY ALL`, `QUALIFY`, `//`, `::` parse in every dialect. DataFusion accepts `datafusion.sql_parser.dialect = 'duckdb'` (`config.rs Dialect::available` includes `"duckdb"`) but the dialect only changes parsing [code].
- [probe] on 55.0.0, generic dialect: `GROUP BY ALL`, `* EXCLUDE (y)`, `QUALIFY rn = 1`, `x::INT`, FROM-first all plan and run; `7 // 2` parses to `Operator::IntegerDivide` (`datafusion-sql-55.0.0/src/expr/binary_op.rs:65`) but execution fails "Operator DIV is not yet supported"; `'1' + 1` fails to plan (DuckDB casts). PIVOT/UNPIVOT, aggregate `FILTER`, `WITHIN GROUP` unsupported in core [doc].
- Semantics vs DuckDB: integer `/` truncates in DataFusion, DuckDB `/` is float division since 0.8 (`//` is integer) [doc, duckdb.org numeric functions]; NULL order: DataFusion `default_null_ordering = "nulls_max"` (`config.rs:314`, i.e. NULLS LAST on ASC, FIRST on DESC [probe]), DuckDB is NULLS LAST both ways [doc, duckdb.org orderby]; set `nulls_last` to match. Unquoted identifiers are lower-cased (`enable_ident_normalization`, `:275`); strings map to `Utf8View` (`:296`). No DuckDB-compat function crate exists; `datafusion-functions-extra` 0.3.0 (2025-09-11) is the nearest [doc].

## 9. Precedent (general workloads, none uint256)

Bauplan moved DuckDB to DataFusion (2025-11-05, ~2x p50, cites memory spikes and hackability, warns on ident case) [vendor]; ParadeDB went the other way, DataFusion to DuckDB, in pg_lakehouse v0.8.0 (2024-06-27); GlareDB left DataFusion for its own engine (lessons in #13525, 2024-11) and is idle since 2025-11-14; InfluxDB 3 (FDAP), GreptimeDB, Arroyo (2024-03, 3x throughput, lost window functions for a while), LanceDB, dbt Fusion/SDF, Sail all build on it; Spice.ai uses DataFusion as core with DuckDB as one accelerator and now pushes Vortex-backed "Cayenne" claiming 1.5x DuckDB (2025-12-17) [vendor]; Seafowl and Exon are stale (2025-02/03). ClickBench: 55.0 blog claims fastest on partitioned Parquet on c7a.metal-48xlarge and second on c6a.4xlarge as of 2026-08-24 [vendor]; ClickHouse's own 2026-05-27 page ranks DuckDB second overall and omits DataFusion [vendor]. General-workload evidence only.

## 10. Other components

- **Vortex**: `vortex-datafusion` 0.86.1 (2026-09-11, Apache-2.0), LF AI incubation, 0.86.0 carried breaking API changes, no 1.0 [doc].
- **arrow-rs i256/Decimal256**: full checked/wrapping/overflowing op set, `from_string`, signed only, 76-digit cap; open cast issues #10946/#10978 (Sept 2026) [doc, code].
- **Polars**: Decimal stable 2025-12-03, 128-bit, precision 38, no 256 [doc].
- **DBSP**: `dbsp` 0.349.0 (2026-09-12, MIT/Apache), release every ~2 days, 73 deps, docs.rs build broken since 0.324; no Arrow/Parquet in the crate [doc].
- **256-bit**: `ruint` 1.20.1, `alloy-primitives` 1.7.3, `ethnum`, `bnum`; no Arrow canonical extension type for int256 (list: tensor, json, uuid, opaque, bool8, variant, timestamp_with_offset); Goldsky's `streamling` (2026) moved from `FixedSizeBinary(32)` extension types to `decimal_arb(78,0)` over LargeBinary and notes DataFusion still dispatches on the physical type, so they hand-rewrite SQL [doc].

## Whole-engine Rust alternatives (scope addition)

uint256 max is 78 digits; **no surveyed engine holds it natively** (Polars, GlareDB, Feldera, Sail 38; Databend, DataFusion 76). The question is which engine makes a custom exact aggregate cheapest.

| Engine | Library? | Licence | Maturity / cadence | 128/256 decimal, overflow | Parquet, small files | SQL breadth | Build |
|---|---|---|---|---|---|---|---|
| **DataFusion 55.1** | yes | Apache-2.0 | major every 2-3 months; 43 breaking items in 55 | 128+256 to 76 digits; **wraps** in `+ - *` and SUM | native, footer/list/stats caches, per-file open | full: CTE, window, joins, QUALIFY | ~30 crates, 68-92 MB CLI binary |
| Polars SQL 0.55.2 (2026-08-06) | yes, `polars-sql` | MIT | 39.7k stars, monthly-ish | 128 only, 38 digits; decimal `sum_reduce` **errors** on overflow, but a `wrapping_sum_arr` i128 path sits beside it, group-by path unverified | native; #17259/#19538: metadata parse dominates, 35k files >30 s to list | CTE, joins, GROUP BY; windows second-class, ROWS default, past PARTITION/ORDER bugs | heavy, ~20 workspace crates |
| GlareDB 25.6.3 (2025-06-19) | yes, `glaredb_core` + `_rt_native` + `_ext_default`, "API unstable" | MIT | last commit 2025-11-05, ten months idle | Decimal64/128, 38 digits; overflow semantics undocumented | own arrow-rs fork reader; no multi-file doc | CTE, window, join present | own parser/executor, no DataFusion |
| Databend nightly (v1.2.944, 2026-09-16) | **no crate**; Python in-process only; git-dep the ~115-dep workspace | Apache-2.0 + Elastic-2.0 | daily nightlies, 9.4k stars | 128 and 256 to 76; **SUM raises `ErrorCode::Overflow`** | native, server-shaped | full | whole warehouse |
| Sail 0.7.1 (2026-08-24) | no crates; Spark Connect server on DataFusion 55 | Apache-2.0 | monthly | Spark: 38 digits, ANSI overflow tests | DataFusion's | Spark SQL | 37 in-tree crates |
| Feldera / `dbsp` 0.349.0 | yes, circuit API only | MIT/Apache | ~every 2 days | 38; UDA example panics if i256 result does not fit | none in crate | **SQL needs the Java/Calcite compiler (JVM at build time, emits Rust)** | 73 deps |
| Turso 0.7.2 / 0.8.0-pre (2026-09) | yes, `turso` | MIT | 24k stars, pre-1.0 | no DECIMAL | **no Parquet** | partial windows, no recursive CTE | small |
| GlueSQL 0.20 (2026-08-30) | yes | Apache-2.0 | 3.1k stars | 96-bit mantissa | storage backend exists | no windows (PR #1969 unmerged) | small |

Verdicts: DataFusion, the only maintained crate with the SQL breadth and Decimal256, but you must own overflow. Polars, embeddable and errors on decimal overflow in one path, wrong digit count and slow on many files. GlareDB, cleanest embedding, unmaintained. Databend, right semantics, not a library. Sail, a DataFusion server. Feldera, IVM not ad hoc query, JVM in the build. Turso and GlueSQL, not analytical.

## Implications for a Burrmill that rents DataFusion

**Solved already.** Footer, listing and statistics caches exist, are shared per `RuntimeEnv`, and have config knobs (§2). `PartitionedFile` plus a custom `TableProvider` or `ParquetFileReaderFactory` lets Burrmill hand over its own file list and already-parsed `ParquetMetaData`, so footers are read once per process, which is what fixed roadmap 4.2. `SQLOptions` plus an empty table-factory map, a custom catalog list and no `file://` store gives a genuine positive allowlist; core registers no file-reading table functions (§4, verified by execution). Extension points to substitute the fold operator exist and are used in production by GreptimeDB, InfluxDB 3 and delta-rs (§6). Cooperative yielding covers scans and aggregates. Dialect coverage for the nest views (GROUP BY ALL, EXCLUDE, QUALIFY, `::`) is there.

**Burrmill must add.** (1) Exactness: a replacement `sum` UDAF (and any `avg`) on checked `i256`, registered over the built-in by name, plus a rule setting `fail_on_overflow` on every `BinaryExpr`, plus precision enforcement after decimal arithmetic since DataFusion validates none of it; the built-ins wrap in every accumulator including partial merge, so this is not optional. (2) A representation for values ≥ 10^76: Decimal256 cannot hold `type(uint256).max`, so either two limbs, a `FixedSizeBinary(32)`/`LargeBinary` extension type with its own UDFs (Goldsky's route, with the dispatch pain they describe), or refusing such rows loudly. (3) The `/` and NULL-order differences (`nulls_last` config; rewrite `/` to float or refuse). (4) A dedicated CPU runtime and per-query `RuntimeEnv` with a shared `CacheManager`, because core offers no scheduler and no cross-query fairness; Burrmill's admission gate stays. (5) Version-bump labour on every major: the traits Burrmill would implement changed in 54 and 55 and change again in 56.

**Real risks.** Overflow refusal is not a configuration; it is a fork-shaped commitment maintained against a 43-item-per-major API. Joins do not yield (#19358 open, fix abandoned), so the one-morsel cancellation bound Burrmill just demonstrated cannot be promised for any statement with a join. Memory: #20714 (RSS over budget) and #24994 (pool lock contention) are open, and the 256 MB at 8 threads gate is exactly the regime they describe. Per-file open cost stays per file whatever the cache does, so the 3.6x at ten thousand segments is structural until files are coalesced or the scan is owned. Decimal256 is 76 digits, and that is arrow-rs, not something a rule can change.

Artefacts: probe source was in the session scratchpad; no repository file was modified. *[Filing note: the probe is kept at `probes/dfprobe`, with its `COPY` target moved from the scratchpad to `/tmp` so it runs anywhere.]*
