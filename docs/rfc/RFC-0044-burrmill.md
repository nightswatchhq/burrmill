# RFC-0044: Burrmill — a Rust-native query engine-layer for Nuthatch

- Status: Draft. Not a carve-out from the 2026 feature freeze. This RFC does not start any work.
- Date: 2026-08-30. Nature: Design-and-cost RFC. Conditional on the outcome of RFC-0042.
- Depends on: RFC-0042 (DuckDB-removal investigation, slice-6 experiments); RFC-0034 (query allowlist); RFC-0041 (authored incremental entities, DBSP); RFC-0009 (content-addressed sealed segments); RFC-0018 (authored SQL); RFC-0004 (measurement discipline).
- Blocks: any Rust-native query migration that is not this design; any claim Burrmill is faster than DuckDB before it is measured under RFC-0004.

## §0 Provenance and guardrails

This RFC is written before RFC-0042's slice-6 decision, deliberately, to defuse the momentum failure mode: a "remove DuckDB" verdict arriving with no costed design attached, so that the removal becomes its own justification. The guardrails are unchanged from the original draft and are load-bearing: this is a **Draft**, it is **not** a carve-out from the 2026 feature freeze, it **starts no work**, and it becomes work only if slice-6 concludes "remove DuckDB (Tier 1)" **and** the board records that outcome. Nothing below is a commitment to build; it is a commitment to know the price before we decide.

What has changed since the original draft is the **thesis**, and the change is deliberate. The original framed Burrmill as "own semantics, rent execution" — a thin, disciplined layer over DataFusion that never touches an execution primitive. The goal is now blunter: whatever gives the best achievable performance and safety on Nuthatch's workload, up to and including owning execution. So it is no longer "a tasteful glue layer". It is a **batteries-included, product-quality query library that demonstrably beats DuckDB on Nuthatch's workload** — one binary, no assembly required — where "beats" is defined under RFC-0004 measurement discipline, not by vibe.

The rework is evidence-driven about where **owning** execution wins and where **renting** (DataFusion, Arrow, parquet-rs) is already at or beyond parity. The short version of the evidence, argued in §4: on Nuthatch's measured hot path — the heavy folds over sealed segments — owning specialised operators already wins (the #987 result: **0.55–0.85× DuckDB across 24/24 configurations**, i.e. faster everywhere, at exact parity). On the cold path of rarely-used general SQL shapes, tuned DataFusion 55 is close enough to DuckDB that reimplementing it would be waste. The reworked architecture therefore makes **owned, specialised execution the default for everything on the measured hot path** and relegates DataFusion to a shrinking cold-path fallback, governed by a **coverage ratchet** so that each release runs more of the real workload on owned operators. This is a hybrid that is honest about being on a trajectory towards owning the engine, not a permanent two-engine marriage.

## §1 Thesis: beat DuckDB for Nuthatch, in one binary

Burrmill is a **query engine-layer**: it owns the semantics *and* the execution of everything on Nuthatch's hot path, and rents general execution only for the long, cold tail while that tail is being retired.

**Owned (product surface + hot-path execution):** the admitted SQL subset (RFC-0034); exact integer/decimal arithmetic with refuse-on-overflow; null semantics; canonical ordering; the hot∪cold seam; reorg-visible state; limits, timeouts, cancellation and the memory pool; the security boundary; and — this is the reframing — the **vectorised operators for the admitted subset**: cache-aware aggregation, parallel morsel-style scans of sealed segments, seam-aware union, checked folds, and (pending experiment A4) a small planner for the closed set of plan shapes.

**Rented (cold path + substrate):** DataFusion 55/56 planning-and-execution *for general query shapes not yet covered by owned operators*; Arrow as the in-memory format and kernel library; parquet-rs for Parquet decode; sqlparser-rs for parsing. Arrow and parquet-rs are rented **permanently** — reimplementing SIMD Arrow kernels or a Parquet decoder is not a tractable side-project and there is no evidence it would pay off (§4.2). DataFusion is rented **conditionally and decreasingly**.

Experiment A4 (RFC-0042 §5.3) remains the pivot. It measures how small the closed set of plan shapes actually is. If A4 says the admitted workload reduces to roughly a dozen plan shapes, Burrmill can own a tiny planner and DataFusion becomes a genuinely optional fallback — the "DuckDB in Rust" outcome. If A4 says the shape set is open-ended, Burrmill keeps DataFusion as a permanent cold-path engine and only owns operators, not planning. **We follow the evidence; A4 decides which.** The original draft's Q1 ("layer or engine") is retained but its default answer has flipped from "layer" to "engine-layer, own as much of the hot path as the parity corpus can defend".

## §2 Why DuckDB is fast, and what we must replicate or beat

To beat DuckDB we must be specific about what makes it fast. DuckDB's speed rests on a well-documented trifecta plus a set of storage/reader defaults:

1. **Vectorised, push-based execution.** DuckDB processes columnar batches of up to `STANDARD_VECTOR_SIZE` (2048) values rather than rows, amortising interpretation overhead and enabling SIMD. It began as a pull-based "Vector Volcano" and moved to a **push-based** model specifically to express parallel pipelines cleanly (Greybeam "DuckDB Internals Part 2"). A `UNION ALL` — which is *exactly* Nuthatch's hot∪cold seam — becomes two independently scheduled pipelines pushing into a shared sink. This is directly relevant: our seam is a `UNION ALL` and we should schedule it the same way.
2. **Morsel-driven parallelism.** Work is split into "morsels" that idle threads pull from a shared queue, giving natural load-balancing and NUMA-aware scheduling (Leis et al., HyPer lineage). Parallel aggregation uses **thread-local hash tables** merged at the end, eliminating contention on a shared structure and scaling near-linearly on most GROUP BYs (letsbuildsolutions deep-dive; MotherDuck glossary).
3. **Lightweight compression + zone maps + ART.** Row groups carry min/max **zone maps** so a scan skips chunks whose range excludes the filter. Lightweight, per-type compression (with dictionary vectors that can be operated on without full decompression) keeps bytes and cache pressure down. **Adaptive Radix Tree (ART)** indexes accelerate point and very-selective (<0.1%) lookups and enforce PK/unique constraints (DuckDB docs, "Indexing"; "ART storage" blog).
4. **Late materialisation + batteries-included Parquet defaults.** The late-materialisation optimizer (DuckDB PR #15692 by Hannes Mühleisen) landed in DuckDB 1.3, which per MotherDuck's release post "defers fetching columns until absolutely necessary, resulting in 3–10x faster reads for queries with LIMIT" — plus caching, dictionary handling, and multi-threaded row-group parallelism, all on by default. DuckDB reads Parquet fast *out of the box*, with no tuning knobs to set.
5. **Out-of-core operators.** Special join/sort/aggregate algorithms degrade gracefully to disk rather than falling off a cliff, using as much memory as available and writing as little as possible.

What we must **replicate**: vectorised batch execution; thread-local hash aggregation; zone-map/row-group pruning (rented via parquet-rs, §4.2); push-based seam scheduling. What we must **beat**: DuckDB's default single-connection concurrency behaviour under load (§3.5, the #986 result), and its arithmetic overflow story (§3.2 — DuckDB does not fully refuse-on-overflow either; this is a feature we can own outright). What we should **not** try to replicate: a general ART secondary-index subsystem (Nuthatch's access is scan-and-fold over sealed segments, not point lookups), general out-of-core join spilling (the admitted subset does not contain arbitrary large joins), or a general cost-based join-ordering optimiser (the closed shape set does not need one).

The honest asymmetry: DuckDB is a general engine tuned over years by a funded team. Burrmill does not need to be general. It needs to be faster than DuckDB on **Nuthatch's** shapes, over **Nuthatch's** sealed-segment layout, and merely *adequate* everywhere else. The #987 result shows that a specialised operator over the exact hot-path shape beats the general engine handily; the bet is that the same holds for the other heavy folds.

## §3 Owned semantics and the product surface

### §3.1 The admitted subset and the plan-shape count
RFC-0034's allowlist is enforced at plan level, not by string-matching SQL. A4 counts the distinct plan shapes the real workload produces; the count decides whether §3.9's owned planner is feasible. The allowlist is also the security boundary (§3.6) and the thing the parity corpus enumerates (§5) — its finiteness is what makes owning execution tractable for one person.

### §3.2 Exact arithmetic — a feature DuckDB itself does not fully give
This is Burrmill's clearest correctness win and it is worth being precise, because "beats DuckDB on safety" has to mean something. Nuthatch indexes a blockchain: balances, exposures and velocities are exact integers up to uint256, and a silently-wrapped sum is a **wrong answer that looks right** — the worst possible failure for an indexer.

DataFusion's integer arithmetic **silently wraps** and this is confirmed still-broken as of August 2026 (statuses verified by live GitHub fetch):
- **#17539** ("Numeric overflow should result in query error") is **OPEN**, labelled bug, opened Sep 2025 by a DataFusion maintainer, assigned but with **no linked PR and no milestone**. The reproducer `SELECT 10000000000 * 10000000000` returns the wrapped `7766279631452241920` where Postgres, Trino and Snowflake all raise an error.
- **#14771** (`-2147483648 % -1`) is **OPEN**. It documents an inconsistency: integer modulo raises an Arrow "Arithmetic overflow" error while `i32::MIN + -1` **silently wraps** to `2147483647`.
- **#20034** (ANSI mode for `negate`) is **OPEN**, opened Jan 2026; its own code comment states "all operations currently use wrapping behavior" and ANSI checked-overflow "is not yet implemented".

Crucially, core DataFusion v55 has **no user-facing config flag** (no `spark.sql.ansi.enabled` equivalent) to turn on checked integer arithmetic engine-wide; ANSI work lives in the `datafusion-spark`/Comet accelerator, not core, and is itself incomplete. Overflow handling is *inconsistent by operation*: some arrow-rs kernels error, scalar constant-folding wraps. That inconsistency is itself a hazard — you cannot reason about it.

DuckDB is better here but **not** watertight: it, too, does not fully refuse on every integer overflow path, which is exactly why "refuse-on-overflow, everywhere, as a guarantee" is a feature Burrmill can offer that neither renting DataFusion nor keeping DuckDB gives us.

Burrmill's owned position:
- Arrow `Decimal128/256` kernels **already error on overflow** — good; we keep them for the decimal path.
- The integer path needs **owned checked operators** (`CheckedSumI128` and friends) or a rewrite-to-decimal; this is Q2. Given the folds are already owned operators (§4.3), checked arithmetic is *free to add there* — it is our code.
- `uint256` = 78 decimal digits, exceeding `Decimal256`'s max precision of **76** digits. So uint256 stays as exact text plus a lossy `DECIMAL(38,0)` `_dec` view; Q3 asks whether an optional `DECIMAL(76,0)` view is worth it.
- `HUGEINT → DECIMAL(38,0)` AST rewrite (4/5 authored views needed it in slice 5; a stated, tested divergence).

Refuse-on-overflow is promoted from a semantic nicety to a **headline product feature**: Burrmill returns `BurrmillError::Overflow` rather than a plausible wrong number. This is in the risks table as Critical because a silent overflow reaching a user result is the failure we most need to prevent.

### §3.3 Canonical ordering
Burrmill owns a canonical ordering and applies it as an operator, replacing engine order. DataFusion's partitioned output is non-deterministic; a blockchain indexer that returns rows in a different order across runs is unusable as an oracle and confusing to users. Canonical ordering is visible in EXPLAIN (§3.8) and is a built-in default, not an opt-in.

### §3.4 COR-1: the seam invariant
The highest-risk invariant. Nuthatch's data is a redb **hot tip** ∪ sealed **cold** Parquet, modelled as a disjoint `UNION ALL` over a single monotonic boundary, with reorg-as-retraction (RFC-0041). This is the one place where owning execution and getting semantics right are the same job: the seam must be scheduled as two pipelines into a shared sink (§2, DuckDB's own UNION ALL model) *and* must never double-count or drop a row across the boundary under concurrent sealing. COR-1 is tested by property tests under concurrent seal (slice 2). It is M/Critical in the risks table.

### §3.5 Limits, cancellation, concurrency, memory
DuckDB today sits behind a single-connection mutex in Nuthatch (#991). The #986 concurrency sweep is the empirical case for owning this: **DuckDB went 40.3 → 39.6 qps flat from 1→32 clients, with p99 latency degrading 29.5 ms → 7066 ms**; the Rust path went **43 → 107 qps with p99 944 ms**. That is the single most compelling "beat DuckDB" number we have for a serving workload, and it comes from *not* holding a global lock.

Burrmill's owned concurrency model:
- **No global lock**; a per-query `SessionContext` (or owned equivalent).
- Per-query Tokio task with abort for cancellation. Caveat: DataFusion cancellation has a known gap — **#19358** (joins do not yield to cancellation) with **PR #19360** (`make_cooperative`) proposed; for owned operators we control the yield points ourselves, so cancellation is a design property, not a hope. This is Q5, the cancellation contract.
- Per-query `MemoryPool` (`FairSpillPool`); note **#5162** (cartesian joins can ignore the pool) — the allowlist forbids the shapes that trigger it.
- Row/byte caps enforced above the stream, so a runaway query is bounded regardless of operator.

### §3.6 Filesystem boundary — the positive allowlist (strengthened)
This is Burrmill's second headline safety feature and it is *structural*, not configurational. Burrmill registers a **positive allowlist of data providers** — redb hot store, sealed cold segments, authored views — and **registers no file-I/O SQL functions at all**. There is no `read_csv`, no `read_parquet(path)`, no `COPY TO`, no `getenv`. The attack surface is not "denied", it is **absent**.

Contrast with DuckDB's denylist model, which has repeatedly failed in exactly this class, and which Nuthatch has had to ship advisories against (v0.6.2 `COPY TO` write; v0.9.3 quoted `read_csv` read):
- **CVE-2024-41672** — `sniff_csv` reads filesystem content **even with `enable_external_access=false`** (verbatim from DuckDB advisory GHSA-w2gf-jxc9-pf2q: "content in filesystem is accessible for reading using `sniff_csv`, even with `enable_external_access=false`"). CVSS is reported inconsistently across databases: 7.5 (v3.1, GitLab), 8.7 (v4.0, Snyk), "Moderate" (GitHub). Fixed in DuckDB 1.1.0. This is a **config-bypass** — precisely the failure mode a denylist invites.
- **CVE-2024-9264** — Grafana's DuckDB-backed "SQL Expressions" feature allowed `read_csv_auto('/etc/passwd')`-class local file read and command injection. Grafana Labs' own security release states "The CVSS v3.1 score for this vulnerability is 9.9 Critical"; the Wiz vulnerability database corroborates "a critical CVSS v3.1 base score of 9.9 and a CVSS v4.0 score of 9.4" (some trackers list a v3.1 of 8.8, so treat the exact number as reported-inconsistently, but it is Critical). This is a Grafana CVE, not DuckDB-core, but it is the canonical illustration of what happens when DuckDB's file functions are reachable from user SQL.
- **CVE-2025-59037** — npm supply-chain: malicious `duckdb@1.3.3` / `@duckdb/node-api@1.3.3` / `@duckdb/duckdb-wasm@1.29.2` published 8 Sep 2025 via a phishing-driven 2FA reset (spoofed `npmjs.help`); code targeted crypto transactions. Rated High; clean re-releases 1.3.4/1.30.0. Not a code-execution bug in the engine, but a reminder that a C++ dependency with a broad distribution surface carries supply-chain risk a single vendored Rust crate does not.

On **memory-safety CVEs** specifically: a specific published DuckDB CVE for an out-of-bounds/use-after-free reachable from a *well-formed* file or trusted query **could not be located** as of August 2026 — flagged as unverified. This is partly because DuckDB's own security policy explicitly declines to treat crafted/corrupted-file crashes as vulnerabilities ("We fix these as bugs, but do not treat them as vulnerabilities"), so the CVE record understates the crash surface. The honest safety claim is therefore *narrower and stronger*: it is not "DuckDB has a long list of memory-safety CVEs" (it does not, partly by policy); it is that **DuckDB is a large C++ codebase where a malformed-file crash is a maintenance bug by policy, whereas Burrmill's decode path is memory-safe Rust (parquet-rs, arrow-rs) where the same class is a panic or `Result`, not a memory-corruption primitive.** The nearest recent DuckDB-core CVE, **CVE-2025-64429** (Nov 2025), is a *cryptographic* flaw in block encryption (insecure RNG fallback, GCM→CTR downgrade), fixed in 1.4.2 — a different class, but evidence that the C++ surface produces real CVEs.

### §3.7 Fuzzing and differential testing
Owning execution raises the testing bar, and we accept it. The plan:
- **sqllogictest-rs** (0.28.x) corpus run over both engines during migration; identical logical results required.
- **Property/differential testing** via a Nuthatch-owned, allowlist-constrained generator. We do **not** adopt Rust SQLancer wholesale (SQLancer is JVM and targets general dialects), but we borrow its oracles: **NoREC** and **TLP** are dialect-agnostic and apply to our subset. DuckDB and DataFusion both use SQLancer/SQLsmith-style fuzzing in CI (DataFusion runs a `datafusion-sqllancer` fork); we mirror the practice at our scale.
- **DuckDB as a dev-only differential oracle** during migration, plus DataFusion-batch as a second oracle for role-3 (RFC-0041 DBSP reference). DuckDB leaves dev-deps after one full clean release cycle (slice 8).
- Query verification in the DuckDB style (run optimised vs unoptimised, require identical results) applied to the owned planner if A4 green-lights it.

### §3.8 EXPLAIN / EXPLAIN ANALYZE
`EXPLAIN` and `EXPLAIN ANALYZE` are part of the batteries-included surface, with canonical-ordering and seam nodes visible, and (for owned operators) per-operator timing and row counts. This is not optional polish: it is how a parity failure gets diagnosed without attaching a profiler.

### §3.9 Owned planner (conditional on A4)
If A4 shows the admitted workload reduces to a small closed set of plan shapes (target: ~a dozen), Burrmill ships its **own tiny planner** for that set — pattern-match the parsed AST to a known shape, emit the owned physical plan directly, skip DataFusion's optimiser entirely. This matters for latency: DataFusion's per-query planning was ~4–5 ms and is being driven towards ~100 µs (v53 plan-clone work), but for Nuthatch's restart-to-ready budget (67.7/74.4 ms) even a few ms of planning overhead on a hot serving path is worth removing when the shape is known in advance. DataFusion's planner remains the fallback for any shape the owned planner does not recognise — the coverage ratchet (§4.6) shrinks that set over time.

## §4 Rented execution and the specialised-operator bet

### §4.1 DataFusion 55/56 — what we rent, and how it is trending
Pinned baseline: **DataFusion 55.0.0**, whose own release notes state it "represents roughly 9 weeks of development and 877 commits… Thanks to the 175 contributors (a new record!)", on Arrow/Parquet 59.x. **56.0.0 is scheduled for October 2026.** Cadence is roughly 6–9 weeks. Extension points (`TableProvider`, `ExecutionPlan`, UDF/UDAF, `OptimizerRule`, `RelationPlanner`) are mature and are what make the hybrid possible.

The performance picture as of 2026, stated honestly with vendor-claim caveats flagged:
- On **hot ClickBench over Parquet**, DataFusion has held or traded the top spot since Nov 2024, ahead of DuckDB/ClickHouse/chDB on the same hardware (DataFusion/InfluxData blog — **vendor/community benchmark, flagged**). On **cold** runs DuckDB is "slightly faster" (same source, stated plainly by the DataFusion team). Independent commentary (QuestDB "Lies, Damn Lies and Database Benchmarks") shows ClickBench rankings are highly sensitive to harness choices — e.g. DuckDB's `parquet_metadata_cache` is defeated by a fresh-process-per-query harness — so **treat single-number ClickBench claims as directional, not decisive**.
- On **TPC-H**, the average gap vs DuckDB has been reported as "less than 2×" (community). This is the number that matters for Burrmill's cold path: uncovered general shapes pay a penalty that is real but bounded.
- Known weak spots that bear on Nuthatch: **high-cardinality aggregates** (the two-phase aggregation strategy and re-hashing across phases — issues #6937, #11679, #11680; memory grows with cores) — this is *directly* on Nuthatch's path and is why the folds are owned; **cold Parquet reads**; **planning overhead on tiny queries** (improving, §3.9); and **filter pushdown is still not on by default** as of v55 — `datafusion.execution.parquet.pushdown_filters` must be set to `true`, and there is an open EPIC (#20324) tracking regressions that block making it default. That last point is important: DuckDB's late materialisation is *default-on*; DataFusion's is a knob. A "batteries-included" Burrmill must set these knobs for the user.

Cost: pin the exact version, bump quarterly gated by the parity corpus. This remains the **largest ongoing rented cost** and the biggest churn risk (H/M in the risks table). The coverage ratchet (§4.6) is partly a hedge — the less of the workload runs on DataFusion, the less its API churn hurts.

### §4.2 parquet-rs and Arrow — rented permanently
parquet-rs (Arrow/Parquet 59.x) supplies the async reader with projection, row-group and page-index pruning, `with_row_filter`, bloom filters, and range coalescing. Snappy is pure-Rust via `snap` 1.1.2 (no C). The reader supplies **mechanism, not predicate logic** — we bring the predicates. Its knobs (`schema_force_view_types` for StringView, `max_predicate_cache_size`, `binary_as_string`, `coerce_int96`) are ours to set as batteries-included defaults.

The cold-read gap is the honest caveat. Historically DataFusion/parquet-rs cold Parquet scans lagged DuckDB (the extreme #5404, "20× slower", was 2023-era and has largely closed via StringView, filter pushdown and metadata caching). As of 2026 DuckDB retains a **slight** cold-run edge, driven by its default-on late materialisation and metadata caching. For Nuthatch this is mitigated structurally: sealed segments are content-addressed and long-lived, so warm-cache behaviour dominates, and we control row-group sizing at seal time (DuckDB itself recommends 100k–1M-row row groups; sub-5000-row groups degrade 5–10×). We should **not** attempt io_uring/mmap heroics or a custom Parquet decoder: no evidence a small team can beat parquet-rs, and the cold gap is small and shrinking. If the cold gap ever dominates, the escape hatch is a **file-format** change (Vortex/Lance) rather than a decoder rewrite — noted and rejected for now (§9, appendix).

On file formats: **Vortex** is the interesting 2026 development — lazy decompression, compute pushdown into the encoding, and vendor headline figures (vortex.dev/GitHub) of "100x faster random access reads (vs. modern Apache Parquet), 10-20x faster scans, 5x faster writes, Similar compression ratios", with DuckDB itself now shipping a Vortex extension reporting "on par or better than Parquet v2 on TPC-H". These are **vendor claims, flagged**; independent tests are far more modest — Daniel Beach's benchmark on ~24 GB of real Backblaze data found "The jump from CSV to any columnar format (~200×) dwarfs Vortex's marginal advantage over Parquet (~11%)". Vortex is explicitly **out of scope** for Burrmill v1 — changing the on-disk format is RFC-0009's territory, not this RFC's — but it is the most credible future lever and worth watching.

### §4.3 Specialised ExecutionPlans — the core bet, restated as the default
This is the evidence that flips the thesis. On `net_balances` at 10k segments, **general DataFusion is 2.53–2.80× slower than DuckDB** (#964; RFC-0013 reported 1.6–2.7×, gap widening with segment count). But the **specialised operator (#987) is 0.55–0.85× DuckDB — faster — at every size and layout tested, with exact parity across 24/24 configurations.** That is a ~3–5× swing from renting general execution to owning a specialised operator on the same shape.

The reworked bet is therefore stronger than the original's "heavy folds get operators, everything else rides DataFusion's 2.5× penalty". It is: **everything on the measured hot path gets an owned, vectorised, specialised operator by default** — balances, exposure, velocity (RFC-0041), the seam union, the canonical sort, the high-cardinality aggregation that DataFusion is known to be weak at. DataFusion executes only the cold tail of shapes not yet owned, and only until the ratchet retires them. The techniques are well-understood and available in pure Rust:
- **Cache-aware aggregation** on `hashbrown` (Rust's SwissTable port; SIMD probing, ~2× FxHashMap, 8× std, 1 byte overhead/entry) with **thread-local partial tables** merged at the end — the exact pattern DuckDB uses, and the pattern DataFusion's two-phase strategy fumbles at high cardinality.
- **Morsel-style scanning** of sealed segments: split row groups into morsels, work-stealing across a thread pool. Whether the pool is Tokio, rayon, or a small custom scheduler is an experiment (Polars 2.0 is moving to "morsel-driven parallelism with Rust's async state machines"; DataFusion uses Tokio; DuckDB uses a custom pool). Default assumption: rayon or a custom pool for CPU-bound fold work, Tokio for I/O, kept separate.
- **Vectorised interpretation, not JIT.** Cranelift-based expression codegen is attractive in theory but the evidence says no for v1: Cranelift compiles ~20–35% faster than LLVM but still 16× slower than a single-pass backend, and produces code ~2× slower than LLVM. For Nuthatch's short-lived per-query expressions the compile cost is not amortised. Vectorised interpretation over Arrow batches is the right default; JIT is a possible later experiment for hot authored views, not v1 scope. This is the classic Sompolski/Żukowski/Boncz "vectorization vs compilation" trade-off, and for our query lifetimes vectorisation wins.

### §4.4 redb provider and authored views
A redb `TableProvider` exposes the hot tip with a seam parameter; authored views (RFC-0018) register as `LogicalPlan`s. As owned operators absorb more shapes, these providers feed owned physical plans directly rather than DataFusion's.

### §4.5 Column-ambiguity and dialect shims
DataFusion is stricter than DuckDB on column ambiguity (the `port_queue` case). Preferred fix is a normalisation pass in `burrmill::parse` rather than a per-query shim, so the divergence is owned and tested, not scattered.

### §4.6 The coverage ratchet
The governance mechanism that makes "hybrid now, own more later" honest rather than a euphemism for "hybrid forever". Every release publishes a **coverage ratio**: the fraction of the real measured workload (by query count and by time) executed on owned operators vs DataFusion. The ratio is **monotonic per release** — it may not go down — and each increment is gated by the parity corpus (§5). This is the operational expression of the thesis: each release, more of Nuthatch runs on the engine we own and can prove is faster and safer, and DataFusion's residual footprint is a number we watch shrink, not an architecture we are stuck with. If A4 says the shape set is small, the ratchet's terminal state is ~100% owned + DataFusion removed (slice 7); if A4 says it is open-ended, the terminal state is a stable hot-path-owned/cold-path-rented split, and that is an acceptable, honest outcome too.

## §5 Parity corpus and oracle discipline
- **sqllogictest-rs** corpus over both engines, identical logical results, is the release gate.
- **Property/differential** testing via the allowlist-constrained generator, using NoREC/TLP oracles (§3.7).
- **Role-3 dual oracle** (DataFusion batch + DuckDB) for the RFC-0041 DBSP reference during migration.
- The corpus is the **parity oracle DuckDB leaves behind**: even under a "keep DuckDB" verdict (§12), the corpus survives as a DuckDB regression suite.
- Caveat retained: the corpus tests only what it enumerates. Semantics-divergence risk is real and is why the generator, not just hand-written cases, matters.

## §6 Measurement (RFC-0004)
Unchanged discipline, applied to a more ambitious scope:
- Noise floor established first; **A/B/A/B interleaved** runs.
- **Peak RSS via the footprint job's method; the 256 MB CI gate is a hard gate** (fail = keep signal). This gate constrains the owned-operator design: thread-local aggregation hash tables and morsel buffers must be budgeted, because DataFusion's high-cardinality aggregation is known to grow memory with core count (#6937) — an owned operator must do better, under 256 MB, or it does not ship.
- **≤2 GB per-chain-cursor** budget.
- **horizon-nest 10,923-segment** reference dataset.
- **restart-to-ready 67.7 ms @10 blocks / 74.4 ms @500** (current df-gate numbers; to be reproduced in-graph — Q7).
- **High-cardinality aggregate** is a **named gate** — the one place DataFusion is weakest and the owned operator must demonstrably win.
- New gate: **coverage ratio** (§4.6) must be non-decreasing release-over-release.

## §7 Slices (each runnable, gated; fail → stop + keep-amendment)
Reworked to **front-load the owned fast path** — the original deferred specialised operators to slice 3, which is backwards for a "beat DuckDB" thesis. The owned operator is the whole point, so it moves early.

- **Slice 0 — skeleton + parser boundary + harness** (~2–3 w). `burrmill::parse` on sqlparser-rs; byte-identical harness; the public `Burrmill` handle stub.
- **Slice 1 — owned operator spike + parity, early** (~6–8 w). Port the #987 specialised operator into Burrmill proper (not `tools/df-gate`), with **checked arithmetic (Q2)** built in; reproduce **≤1.0× DuckDB** on `net_balances` and the high-cardinality gate, exact parity. This is the go/no-go: if we cannot reproduce #987 in-graph, the thesis is unproven and we stop here cheaply.
- **Slice 2 — redb provider + hot∪cold + COR-1** (~6–8 w). Seam as scheduled UNION ALL; COR-1 property test under concurrent seal.
- **Slice 3 — cold-path DataFusion behind a flag** (~4–6 w). 5/5 authored views exact parity in the **0.81–1.64×** envelope (slice-5 #996 result); this is the *fallback*, deliberately built after the fast path.
- **Slice 4 — the rest of the folds + HUGEINT rewrite** (~6–10 w). Exposure, velocity operators; `HUGEINT→DECIMAL(38,0)`; folds ≤1.0× DuckDB.
- **Slice 5 — limits/cancellation/mempool/concurrency** (~4–6 w). Re-run **#986**; require **p99 ≤ DuckDB** and RSS ≤256 MB. This is where we bank the concurrency win.
- **Slice 6 — remaining roles 3–6 + owned planner experiment (A4)** (~6–10 w). Restart budget reproduced in-graph; vocabulary no-escape; if A4 says the shape set is small, ship the owned planner (§3.9).
- **Slice 7 — default-on, coverage ratchet ≥ target** (~3–4 w). Shipped binary **never executes a user query on DuckDB** (RFC-0042 §3a binary rule); DuckDB → dev-deps; Tier-2 becomes C++-free (drops `libstdc++.so.6 GLIBCXX_3.4.29`; DuckDB was **93% of native-artefact bytes / 245 MB objects but only 10.6% of Linux build time; binary ~97.6 MB unstripped**).
- **Slice 8 — drop DuckDB from dev-deps** after one clean calendar release cycle (~1 w).
- **Slice 9 (separate RFC) — Tier-3 native tail:** ring/aws-lc-rs, zstd-sys (`ruzstd` 0.9.0 is 1.4–3.5× slower; the Snappy path avoids it), mimalloc, wasmtime (21.3% of build time).

## §8 Costs — stated as costs
The scope is more ambitious than the original "layer" (which was 40–60 engineer-weeks pre-slice-7), because owning the fast path means owning operators, an aggregation hash table, a scheduler seam, and possibly a planner. Honest re-estimate:

- **Owned-execution core** (slices 1, 2, 4): the specialised operators, checked arithmetic, cache-aware aggregation, morsel scheduling, seam. **~26–36 engineer-weeks.** The #987 work is a de-risking prior — the pattern is proven, the cost is porting-and-hardening, not research.
- **Cold-path DataFusion + providers** (slice 3): **~4–6 w** (mostly done in slice 5 of RFC-0042).
- **Concurrency/limits/mempool** (slice 5): **~4–6 w.**
- **Roles 3–6 + owned planner** (slice 6): **~6–10 w**, with wide variance depending on A4.
- **Cutover + ratchet + dev-dep removal** (slices 7–8): **~4–5 w.**
- **Total pre-Tier-3: ~48–68 engineer-weeks**, i.e. modestly above the original because the scope is genuinely larger. This is the honest price of "beat DuckDB" over "wrap DataFusion".

Constraints and permanent costs:
- **Engineering capacity is the binding constraint on *when*, not on *whether*.** At anything short of full-time this stretches well past a calendar year, which is an argument for the aggressive slice-1 go/no-go rather than against the project.
- **DataFusion quarterly bump forever** while any cold path remains (the ratchet reduces but does not eliminate this until DataFusion is removed).
- **Two engines during migration** — worse before better.
- **Owning execution owns the bugs.** A specialised operator with a subtle aggregation error is a Nuthatch correctness bug, not an upstream issue to file. This is the real cost of the reframing and the reason §3.7's fuzzing/differential discipline is non-negotiable.
- **HUGEINT shim is permanent.**
- **The DuckDB-has / we-must-build list:** batteries-included Parquet defaults (we set the knobs), out-of-core spilling (we avoid the shapes that need it), general join ordering (allowlist forbids the shapes), cold-run scan speed (structurally mitigated), ART point-lookup indexes (not our access pattern).

## §9 Non-goals
- **Not a general database.** Burrmill owns execution for the *admitted subset*, not arbitrary SQL. (This is the one non-goal the rework changes: the original said "not an engine, no execution primitives"; the rework says "own the hot-path execution primitives, stay narrow".)
- No daemon; no server.
- No Turso/redb replacement (redb stays the hot store).
- No custom Parquet decoder and no on-disk format change in this RFC (Vortex/Lance are RFC-0009's call).
- No Tier-3 native work here (separate RFC).
- No user-visible dialect change without gating.
- No query-routing that lets a *user query* hit DuckDB in a shipped binary (RFC-0042 §3a binary rule; the cold path is DataFusion, never DuckDB, once shipped).
- No JIT/codegen in v1 (vectorised interpretation is the default; JIT is a later experiment at most).

## §10 Risks
| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| COR-1 seam bug (double-count/drop) | M | Critical | Property tests under concurrent seal (slice 2); scheduled-UNION-ALL model |
| Silent overflow reaches a user result | M | Critical | Owned checked operators (§3.2); refuse-on-overflow gate; differential oracle |
| Owned operator has a subtle correctness bug | M | Critical | Parity corpus + NoREC/TLP fuzzing (§3.7); 24/24-style exhaustive config parity |
| Peak RSS > 256 MB (owned aggregation grows with cores) | M | High | Hard CI gate; thread-local table budgeting; beat DataFusion's #6937 behaviour |
| Uncovered cold shapes' ~2× penalty dominates real workload | M | High | Coverage ratchet (§4.6); A4 measures how small the tail is |
| DataFusion API churn (quarterly bumps) | H | M | Pin version; corpus-gated bumps; ratchet shrinks exposure |
| Cranelift/JIT rabbit-hole consumes available capacity | M | High | Explicit non-goal in v1 (§4.3); vectorised interpretation only |
| Shared Arrow bug blinds the dual oracle | L | High | DuckDB second oracle during migration (different codebase) |
| Cancellation delay (#19358) on cold path | M | M | Owned operators control yield points (§3.5); Q5 |
| Capacity below full-time | H | M | Front-loaded slice-1 go/no-go; scope stated honestly |
| Filesystem escape | L | Critical | Positive allowlist, no file-I/O functions registered at all (§3.6) |
| A4 returns "shape set open-ended" | M | M | Not fatal — terminal state becomes stable hot-owned/cold-rented split; do not build owned planner |
| Cold Parquet gap vs DuckDB persists | L | M | Structural (content-addressed segments, warm cache, row-group sizing); format change is the escape hatch |

## §11 Placement and packaging
`crates/burrmill` in the workspace; extract to its own repo only on a second consumer. crates.io name `burrmill` was free as of 2026-08-30. Licence MIT OR Apache-2.0. One-line product description: **"SQL over Parquet segments plus a live tip, with exact decimals and refuse-on-overflow, faster than DuckDB on the queries Nuthatch actually runs — in one binary, no configuration."**

Batteries-included public surface (Appendix B): a single `Burrmill` handle; `open(nest)`; `query(sql, limits) -> SendableRecordBatchStream` (streaming Arrow); `explain(sql)`; built-in canonical ordering; built-in `Limits { max_rows, max_bytes, timeout, mem_pool_bytes }` with sane defaults so that *good performance requires no configuration*. Providers `HotStoreProvider` / `SealedSegments`; owned operators `NetBalancesExec` and siblings; `CheckedSumI128` and the checked-arithmetic family; `BurrmillError { NotAllowed, Overflow, Timeout, Cancelled, Seam, DataFusion }`.

## §12 A "no" is a result
- If RFC-0042 **keeps** DuckDB, this document is the record of the price of the alternative — its purpose is served whether or not a line of Burrmill is written.
- If any **slice gate fails**, we stop and file a **keep-amendment** with the quantified blocker (the RFC-0042 escape hatch). A failed slice-1 (cannot reproduce #987 in-graph) is a cheap, early, honest "no".
- Either way, the **parity corpus survives** as a DuckDB regression suite, so the work is not wasted.

## §13 Open questions
- **Q1** — layer or engine-layer, and how small is the plan-shape set? (**A4** — default answer flipped to "own the hot path".)
- **Q2** — overflow-enforcement mechanism: owned checked integer operators vs rewrite-to-decimal.
- **Q3** — is an optional `DECIMAL(76,0)` uint256 view worth the cost over the lossy `DECIMAL(38,0)` `_dec` view?
- **Q4** — oracle sufficiency: does DuckDB-as-second-oracle survive its own removal (slice 8)?
- **Q5** — cancellation contract for owned operators (yield-point discipline).
- **Q6** — real peak RSS of owned aggregation at horizon-nest scale, under the 256 MB gate.
- **Q7** — real in-graph restart-to-ready with the owned planner (vs current 67.7/74.4 ms df-gate numbers).
- **Q8 (new)** — scheduler choice for morsel folds: rayon vs custom pool vs Tokio, measured on the fold workload.

## Appendix A — research notes (versions/dates/sources; vendor claims flagged)
- **DataFusion 55.0.0**: release notes state "roughly 9 weeks of development and 877 commits… 175 contributors (a new record!)"; Arrow/Parquet 59.x. **56.0.0 scheduled Oct 2026.** (DataFusion release issue #22393; 55.0.0 blog draft.)
- **Overflow issues confirmed OPEN as of Aug 2026 (live GitHub fetch):** #17539 (silent Int64 wrap; reproducer `10000000000*10000000000 = 7766279631452241920`; assigned, no PR, no milestone); #14771 (`%` errors, `+` wraps; inconsistent); #20034 (negate ANSI; "all operations currently use wrapping behavior"). No default checked-arithmetic mode in core v55; ANSI work lives in `datafusion-spark`/Comet and is incomplete.
- **Parquet filter pushdown still not default in v55** — `datafusion.execution.parquet.pushdown_filters=true` required; regression EPIC #20324 tracks blockers; DuckDB's late materialisation is default-on (DuckDB 1.3, PR #15692).
- **Decimal256 max precision 76 vs uint256 78 digits** (Arrow/DataFusion docs).
- **#987 specialised operator: 0.55–0.85× DuckDB, 24/24 exact parity** (Nuthatch repo — ground truth). **#964 / RFC-0013: general DataFusion 2.53–2.80× (1.6–2.7×) slower on net_balances, gap widens with segment count** (repo).
- **#986 concurrency sweep** (repo): DuckDB 40.3→39.6 qps, p99 29.5 ms→7066 ms (1→32 clients); Rust path 43→107 qps, p99 944 ms. **#991** single-connection mutex. **#996** 5/5 authored views exact parity 0.81–1.64×.
- **restart-to-ready 67.7 ms @10 / 74.4 ms @500; horizon-nest 10,923 segments; 256 MB peak-RSS gate; ≤2 GB per-chain cursor** (repo ground truth).
- **DuckDB internals:** vectorised push-based execution (STANDARD_VECTOR_SIZE 2048); morsel-driven parallelism (HyPer/Leis); thread-local hash aggregation; zone maps; ART indexes (<0.1% selectivity, PK/unique); late materialisation default-on (PR #15692), 3–10× faster LIMIT reads (DuckDB 1.3 / MotherDuck). Sources: Greybeam, MotherDuck, letsbuildsolutions, DuckDB docs.
- **DuckDB CVE class:** CVE-2024-41672 (`sniff_csv` reads FS with `enable_external_access=false`; CVSS 7.5 v3.1 / 8.7 v4.0 / "Moderate" — inconsistent across DBs; fixed 1.1.0); CVE-2024-9264 (Grafana DuckDB SQL Expressions LFI/RCE; Grafana states v3.1 9.9 Critical, Wiz corroborates v4.0 9.4; some trackers list 8.8); CVE-2025-59037 (npm supply-chain malware, 8 Sep 2025, phishing 2FA reset; High; clean re-release 1.3.4/1.30.0); CVE-2025-64429 (block-encryption crypto flaw, Nov 2025, fixed 1.4.2 — different class). **No specific memory-safety (OOB/UAF-from-well-formed-file/trusted-query) CVE located — unverified; DuckDB policy declines to treat crafted-file crashes as vulnerabilities.**
- **Rust performance building blocks:** hashbrown SwissTable (~2× FxHashMap, 8× std, 1 byte/entry overhead, SIMD probing); Cranelift (~20–35% faster compile than LLVM but 16× slower than single-pass; code ~2× slower than LLVM — argues against JIT for short-lived queries); vectorisation-vs-compilation (Sompolski/Żukowski/Boncz, DaMoN'11).
- **Precedents:** Bauplan "Duck Hunt" (Nov 5 2025) — migrated ephemeral SQL engine DuckDB→DataFusion, cited hackability/governance ("open-source *product* vs *project*"), eliminated only C++ dependency; **vendor blog, flagged**. InfluxDB 3 IOx (10s of millions of plans/day — vendor claim). Polars 2.0 roadmap: morsel-driven parallelism + async state machines; on small data measurably quicker than DuckDB, within 20–50% on single-node (2026 comparisons, mixed independent sources). Feldera/DBSP (RFC-0041 lineage): current release line ~v0.324+ (Jul 2026), pre-1.0, Apache-2.0; ad-hoc queries served by DataFusion, continuous pipelines by DBSP.
- **File formats (vendor claims flagged):** Vortex vendor headline (vortex.dev) — "100x faster random access reads (vs. modern Apache Parquet), 10-20x faster scans, 5x faster writes, Similar compression ratios"; DuckDB Vortex extension "on par or better than Parquet v2 on TPC-H" (DuckDB blog); Polar Signals "70% average query improvement" switching Parquet→Vortex; independent (Daniel Beach) ~11% over Parquet on real data. Out of scope for v1.
- **Testing:** sqllogictest-rs 0.28.x; DataFusion uses a `datafusion-sqllancer` fork; DuckDB uses SQLsmith/fuzzer + query-verification (optimised vs unoptimised). NoREC/TLP oracles are dialect-agnostic and adopted here.
- **Alternatives rejected:** Polars (decimal support historically unstable); Vortex/Lance (formats, not engines; RFC-0009 territory); Turso (OLTP); from-scratch planner *without* A4 evidence (engine-trap); Cranelift JIT in v1 (compile cost not amortised).
- **crates.io `burrmill`** free as of 2026-08-30.

## Appendix B — public-surface sketch
```rust
pub struct Burrmill { /* opaque */ }

pub struct Limits {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub timeout: Duration,
    pub mem_pool_bytes: u64,
}
impl Default for Limits { /* sane batteries-included defaults */ }

impl Burrmill {
    pub fn open(nest: &Nest) -> Result<Self, BurrmillError>;
    pub fn query(&self, sql: &str, limits: Limits)
        -> Result<SendableRecordBatchStream, BurrmillError>;
    pub fn explain(&self, sql: &str) -> Result<String, BurrmillError>;
}

// Providers (positive allowlist; no file-IO functions registered):
struct HotStoreProvider; // redb tip, seam-aware
struct SealedSegments;   // content-addressed cold Parquet

// Owned operators (default on the hot path):
struct NetBalancesExec;  // #987 lineage, checked arithmetic
struct CheckedSumI128;   // refuse-on-overflow fold family

pub enum BurrmillError {
    NotAllowed, Overflow, Timeout, Cancelled, Seam, DataFusion,
}
```

---

## Amendment 1 — placement, 2026-08-31

§11 said `crates/burrmill` inside the nuthatch workspace, extracted "only on a second consumer". That is superseded by a direct instruction: Burrmill lives in **its own repository from the first commit**, and nuthatch is **not modified** — it is a read-only test subject, its sealed segments used as a corpus and nothing more.

This is the better arrangement anyway, for three reasons the RFC's own text already argues:
- §4.1's largest ongoing cost is DataFusion churn. A separate repo means a DataFusion bump cannot break a nuthatch build, which is what makes the quarterly bump a scheduled chore rather than an outage.
- §7 slice 7's C++-free claim is easiest to hold when the shipped library and the oracles are separate manifests. Here they are separate workspace members with `publish = false` on the one holding DuckDB.
- §12's "a no is a result" is much cheaper to act on when the answer is "archive a repo" rather than "unpick a crate from a workspace".

The cost, stated: no path dependency on nuthatch, so anything Burrmill needs from it (`seal::SEGMENTS_DIR` and the segment naming convention) is duplicated rather than imported, and a change to nuthatch's seal layout will not break Burrmill's build - it will break its *reading*, silently, until a test catches it. That test is owed.
