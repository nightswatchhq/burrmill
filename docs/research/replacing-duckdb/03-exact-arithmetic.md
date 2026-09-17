# 03 · Exact arithmetic on DataFusion, by experiment

Investigation 03, reported 2026-09-16 by a research agent (Fable 5.1). The report is kept verbatim
below.

**Where and what.**

- **Machine:** the thinkpad (32 cores).
- **Engines:** datafusion 55.0.0, and again on 55.1.0 with identical outcomes. DuckDB 1.5.3 was
  already installed there.
- **The prototype:** a five-limb `I320` accumulator, checked `sum`/`avg` UDAFs, checked scalars,
  and an `AnalyzerRule` that swaps them in.
- **Kept at [probes/arith](probes/arith/).** Source, experiments and every output, for 55.0.0
  (`out/`) and 55.1.0 (`out-551/`). `Cargo.toml` gained an empty `[workspace]` table so it builds
  inside this repo's tree. The scripts still `cd ~/scratch/burrmill-arith`, the thinkpad path.
  Rebuilt on the MacBook at filing: `cargo test --release --lib` passes its 4 `I320` tests, and the
  `Cargo.lock` from that build is kept.

**Checked before filing: finding 3 is incomplete.** The report says integer literals wider than
i64 become Float64 and "no plan rule can catch it". The first half is right
(`datafusion-sql-55.0.0/src/expr/value.rs:100-118`). But the same function parses them as decimal
when `datafusion.sql_parser.parse_float_as_decimal` is set (`datafusion-common-55.0.0/src/config.rs:272`,
default `false`). That fixes the literal at parse time, not in a plan rule. It also turns every
float literal into a decimal, which changes other semantics, and **it was not tested**.

## Headlines for the plan

- **Refuse-on-overflow is achievable on DataFusion, but only as a policy Burrmill owns.**
  - No setting does it.
  - The built-ins are inconsistent: `/` and unary `-` error, while `+`, `-`, `*` and `%` wrap.
    Decimal `+` checks for physical overflow but not precision. `AVG` refuses even when only a
    partial sum overflows, while `SUM` wraps.
  - The prototype's rule makes every case error or answer exactly, through CTEs, joins, windows,
    scalar subqueries, `HAVING` and `WHERE`, on both 55.0.0 and 55.1.0.
- **DataFusion's SUM is modular arithmetic.** It is wrong exactly when the true answer leaves the
  type, and an overflowing partial sum is harmless. It has no "wrong only in parallel" bug of the
  kind DuckDB has. DuckDB 1.5.3 wraps `SUM(DECIMAL(38,0))` past i128 at 32 threads over two files,
  and refuses at one thread.
- **Precision is enforced only on CAST.** A `Decimal128(38,0)` sum carries and prints 39 digits,
  in DuckDB as well.
- **Two things no plan rewrite can fix.**
  - `TRY_CAST` to decimal returns NULL, and SUM then drops the row. Nuthatch's `_dec` pattern is
    therefore wrong at ingest, not at the sum.
  - Wide integer literals: see the correction above.

  Both have to be dealt with at the SQL surface.
- **uint256 max has no Decimal256 home.** An exact uint256 balance out of DataFusion is canonical
  text (`checked_sum_text`), or a refusal above 10^76-1. With the 320-bit accumulator, all nine test
  parties including max uint256 came out exact.
- **Refusal can be both order-independent and correct.** `MAX, +1, -1` answers and `MAX, +1`
  refuses. That is ROADMAP 2.1b, paid for with a wider accumulator.
- **Cost, at 1M groups over 8 partitions (the worst case for state size):**

  | Input | Checked sum against the built-in | Extra memory |
  |---|---|---|
  | Decimal128 | +70 ms, 1.7x the aggregate | +130 MB |
  | Text (nuthatch's actual shape) | same speed as `SUM(CAST(...))` | +100 MB |

  Memory is the number that matters against the 256 MB gate. A per-type accumulator (i128 plus a
  high word for Decimal128) would roughly halve it. **Not measured against Burrmill's gate
  fixture.**
- **The rule has to be a whitelist.** It must refuse anything it does not know can overflow
  safely. `power`, `factorial`, bit aggregates, `array_sum`, integer `-` on timestamps and future
  functions were not covered.
- **For nuthatch as it stands:** DuckDB's `BIGNUM` already gives exact uint256 sums at 32 threads
  (`SUM(amount::BIGNUM)`). It accepts non-canonical text and truncates `7.9` to 7, where DuckDB's
  own `TRY_CAST` rounds it to 8. If DuckDB stayed, that would replace the `_overflow` flag outright.

---

## The report, verbatim

# Can DataFusion refuse on overflow everywhere? An experiment

**Where.** tp (Debian, 32 cores), `~/scratch/burrmill-arith`. Throwaway crate on `datafusion = "=55.0.0"` (arrow 59.3.0; sub-crates resolve to 55.1.0 under the 55.0.0 facade). crates.io's newest release is 55.1.0; the same crate rebuilt on it is in `~/scratch/burrmill-arith-551` and **behaved identically on every case** (diffs are file paths, row order in unordered results, and which parallel partition's error surfaced first). DuckDB v1.5.3 was already at `~/.local/bin/duckdb`; nothing was built. `/usr/bin/time` does not exist on tp, so peak RSS comes from a 10-line Python wrapper reading `getrusage(RUSAGE_CHILDREN).ru_maxrss`, the same counter GNU time prints. Every number below is copied from program output; all outputs are on tp as `out-*.txt` and locally under `probes/arith/out/` (55.0.0) and `out-551/` (55.1.0). Sources are in the same directory (`src/{i320,checked,rule,util}.rs`, `src/bin/cost.rs`, `examples/exp1-4.rs`, `exp5.sql`, `exp5b.sql`, `ptime.py`). *[Filing note: the report gave scratchpad paths; the files are now in `probes/arith`.]*

## Outcomes

**wrap** = wrong number returned silently; **>prec** = a printed value exceeding the declared decimal precision (fits the physical i128/i256); **error** = refused; **NULL** = silently dropped; **ok** = exact. DF = DataFusion 55.0.0 and 55.1.0 stock. DDB = DuckDB 1.5.3 at 32 threads.

| # | Case (true answer) | DF stock | DF + rule | DuckDB 1.5.3 |
|---|---|---|---|---|
| 1a | `SUM(Int64)`, 4 files, 4 partitions; EXPLAIN shows `AggregateExec: mode=Partial` under `mode=Final` over 4 file groups (2^63) | **wrap** -2^63 | error naming 9223372036854775808 | ok (widens to HUGEINT) |
| 1b | same, 1 partition; and 1 file with 4 row groups (tiny file stays one partition, `mode=Single`) | **wrap** | error | ok |
| 1c | partial overflows, final fits (MAX-1) | ok | ok | ok |
| 1d | `SUM(Int64) GROUP BY` | **wrap** | error | ok |
| 1e | `9223372036854775807 + 1`, constant-folded (EXPLAIN shows the literal) | **wrap** | error at plan time | error |
| 1f | `10000000000 * 10000000000`, folded | **wrap** 7766279631452241920 | error | error |
| 1g | `v + 1`, `v * 2`, `v - (-1)`, `v * v` on a column | **wrap** | error | error |
| 1h | `-v` on i64::MIN, `abs(MIN)` | error (arrow's neg/abs are checked) | error | error |
| 1i | `MIN % -1` vs `MIN / -1` | `%` **wraps** to 0, `/` errors | n/a | n/a |
| 1j | `CAST(i64 AS INT)`, `CAST('9223372036854775808' AS BIGINT)`, `arrow_cast` | error | error | error |
| 1k | `TRY_CAST` of the same | NULL | NULL | NULL |
| 2a | `SUM(Decimal128(38,0))` = 10^38 | **>prec** 39 digits in a Decimal128(38,0) | error | **>prec** same |
| 2b | `SUM(Decimal128)` = 2·(10^38-1) > i128, two files | **wrap** -14028…1145 at 1p and 6p | error | **wrap** -14028…1458 at 32 threads; **error** at 1 thread |
| 2c | partial > i128, final fits | ok | ok | error (refuses on the partial) |
| 2d | `SUM(Decimal256(76,0))` = 10^76 | **>prec** 77 digits | error | reads column as DOUBLE: 1e+76 |
| 2e | `SUM(Decimal256)` = 6·(10^76-1) > i256, six files | **wrap** -5579…3994 | error | DOUBLE 6.000000000000001e+76 |
| 2f | partial > i256, final fits | ok | ok | DOUBLE |
| 2g | `AVG(Decimal)` on 2b/2c/2e/2f | error "Arithmetic Overflow in AvgAccumulator", *also* on 2c/2f where the mean fits | error (mine keeps 4 extra places; a 38-digit mean does not fit) | DOUBLE of the wrapped sum: -7.0e37 |
| 2h | `a + b`, `a * b`, `a * 10` on Decimal128 columns past i128 | error (arrow checked kernels) | error | error |
| 2i | `a + 1` = 10^38 in Decimal128(38,0) | **>prec** | error (precision validated) | error |
| 2j | `a + b` = 2·(10^76-1) in Decimal256(76,0) | **>prec** | error | n/a |
| 2k | 38-digit integer literal | parsed as **Float64**; `…999 + 1` = 1e38 | unchanged: nothing integral left to check | HUGEINT, exact |
| 2l | `CAST('<77 or 78 digits>' AS DECIMAL(76,0))`, `arrow_cast` | error | error | DECIMAL(76,0) is a binder error |
| 2m | `TRY_CAST` of the same | NULL | NULL | NULL |
| 2n | `CAST(Decimal256 AS DECIMAL(38,0))`, `CAST(Decimal128 AS BIGINT)` | error | error | n/a |
| 3a | Nuthatch today: `TRY_CAST(text AS DECIMAL(38,0))` + `_overflow` flag | balance NULL or **wrong** (`fits76`=0, `partial_over`=0, `p255`=1) but the flag is 1 on every such party | same | same |
| 3b | `UNION ALL`, `CAST(text AS DECIMAL(76,0))`, `SUM - SUM` | error at the CAST of the 78-digit value | error | binder error |
| 3c | same with `TRY_CAST` | `max`, `p255` NULL-dropped; then the subtraction of two *wrapped* Decimal256 sums happened to error | error from checked_sum naming 5999…994 | n/a |
| 3d | `SUM(VARCHAR)` | plan error (no float coercion) | plan error | error |
| 3e | `checked_sum(text)` → Decimal256, 1 and 4 partitions | n/a | ok for the four parties that fit, including `partial_over` whose partial exceeds i256; error naming the value otherwise | `SUM(amount::BIGNUM)`: **ok, all nine parties exact**, incl. max uint256 |
| 3f | `checked_sum_text(text)` | n/a | ok, all nine exact | BIGNUM as above |
| 3g | non-canonical text `" 7"`, `"007"`, `"+1"`, `"1e3"`, `"7.0"` | n/a | error naming `" 7"` | BIGNUM: `" 7"`, `"1e3"` error; `"007"`→7, `"+1"`→1, `"7.9"`→**7**, where `TRY_CAST('7.9' AS DECIMAL)`→**8** |
| 4b | rule through CTE, join, scalar subquery, `SUM(v) OVER ()`, HAVING, WHERE | n/a | all caught; `checked_sum`/`checked_sub` visible in the logical plan | n/a |
| 4b | `SUM(DISTINCT)`, `SUM(Float64)`, `AVG(Float64)` | n/a | plan refused | n/a |

Three structural findings:

1. **DataFusion's SUM is modular arithmetic.** Wrapping is associative, so the partial/final split never matters: the answer is wrong exactly when the *true* answer leaves the type, and an overflowing partial is harmless (1c, 2c, 2f). This is the mirror image of Burrmill and DuckDB, which refuse on the partial. DataFusion has no "wrong only with parallelism" bug here; DuckDB does (2b, the README's finding, reproduced on 1.5.3 through two-file `DECIMAL(38,0)` parquet; a native HUGEINT table refuses at both thread counts).
2. **Decimal precision is enforced only on CAST.** Neither SUM nor `+`/`*` validate the result against the declared precision, so a `Decimal128(38,0)` column carries and prints 39 digits; only the physical i128/i256 overflow is caught by the scalar kernels, and SUM catches nothing. DuckDB shows the same 39-digit leak on 2a.
3. **The literal parser is a trap.** Any integer literal wider than i64 becomes Float64 (2k). No plan rule can catch it.

There is no configuration knob: `information_schema.df_settings` has no entry matching `overflow`, `checked` or `wrap`.

## The prototype

**Accumulator.** `I320` is a five-limb two's-complement integer (i256 plus one carry word): checked add/sub/neg, parse of *canonical* decimal text only (`0` or `-?[1-9][0-9]*`, anything else is an error naming the value), formatting, and narrowing to i64/i128/i256 returning `None` rather than truncating. 2^63 uint256 values cannot leave it in either direction, so the accumulator never refuses; refusal happens once, at output. Unit-tested against i128/i256 edges and uint256 max.

**UDAFs.** One `AggregateUDFImpl` in three modes. `checked_sum` keeps the input's family: Int64 → Int64, Decimal128(p,s) → Decimal128(38,s), Decimal256(p,s) → Decimal256(76,s), Utf8 → Decimal256(76,0). `checked_sum_text` returns canonical text and cannot overflow. `checked_avg` adds four decimal places, truncated. All share one `GroupsAccumulator` (`Vec<I320>` plus `Vec<u64>` count, 48 bytes a group; the count doubles as the null tracker) with `convert_to_state`, so the skip-partial path works; state is `FixedSizeBinary(40)` + `UInt64`, merged in I320. `evaluate` narrows per group and errors with the exact value (`checked_sum overflow: exact result 5999…994 does not fit Decimal256(76, 0)`); decimal outputs go through arrow's `validate_decimal_precision`, so 2a/2d refuse too. A `Single` wrapper provides the plain `Accumulator` for window and non-grouped paths. FILTER, empty input (NULL), and the non-canonical refusal are all exercised in exp4.

**Scalars.** `checked_add/sub/mul/neg` call arrow's *checked* kernels (`numeric::add`, where DataFusion uses `add_wrapping`) and then validate decimal precision, which the kernels do not. Return type comes from `BinaryTypeCoercer`, so plan schemas are unchanged.

**Rule.** `CheckedArithmetic` is an `AnalyzerRule` appended after `TypeCoercion`, so operand types are final. It walks every node with `transform_up_with_subqueries` (CTEs are already inlined), rewrites `sum`/`avg` in aggregate and window form, `+ - *` where both operands are integer or decimal, and unary `-`. Replacements are aliased to the original expression's schema name in Projection/Aggregate/Window nodes so parents referencing `sum(t.v)` by name keep resolving, then the node schema is recomputed. It refuses the plan on `DISTINCT`, on `sum`/`avg` of floats, and on a `+ - *` with one integer/decimal operand it cannot pair. Floats, dates and intervals pass through. Constant folding then evaluates the checked UDFs, so 1e/1f error at plan time. About 120 lines; the whole crate is 1,484 lines including experiments.

## Cost

10M rows, `k = row % 1_000_000`, `v` random Decimal128(38,0) below 10^33 plus its text `t`, 8 parquet files × 10 row groups, `target_partitions = 8`, `SELECT k, <agg> FROM t GROUP BY k`, results collected. `cost verify` first joined the result sets: 1,000,000 groups, 1,000,000 equal, for both `checked_sum(v)` and `checked_sum_text(t)` against the built-in. Runs 2 and 3 (run 1 is cold cache); query time excludes process start, RSS is process peak:

| Aggregate | query s | peak RSS |
|---|---|---|
| `COUNT(v)` (scan floor) | 0.080-0.084 | 202-208 MB |
| built-in `SUM(v)` Decimal128 | 0.093 | 215-219 MB |
| `checked_sum(v)` Decimal128 | 0.161-0.163 | 343-349 MB |
| built-in `SUM(CAST(t AS DECIMAL(38,0)))` | 0.329-0.362 | 261-275 MB |
| `checked_sum(t)` from text | 0.328 | 370-375 MB |
| `checked_sum_text(t)` | 0.354-0.386 | 368-397 MB |

So on Decimal128 input the checked sum is **+70 ms (1.7x the aggregate, 2.3x the sum's own cost above the scan floor) and +130 MB** at 1M groups × 8 partitions (every partition sees nearly every key here, the worst case for state size). On text input, which is Nuthatch's actual shape, parsing dominates and the checked sum is **as fast as the built-in `SUM(CAST(...))`, +100 MB**. The memory is the 48-byte state against 17, times partial copies in flight, plus the FixedSizeBinary state arrays; a 320-bit accumulator on the Int64/Decimal128 paths is overkill and a per-type accumulator (i128 plus high word for Decimal128) would roughly halve it.

## Verdict

**"Refuse on overflow everywhere" is achievable on DataFusion, but only as a policy layered on top: a rule that swaps every arithmetic operator and every summing aggregate for your own.** The engine offers nothing towards it, there is no flag, and the built-ins are inconsistent by operation: `/` and unary `-` error, `+ - * %` wrap, decimal `+` is i128-checked but not precision-checked, `AVG` refuses on partials while `SUM` wraps. The prototype makes every case in experiments 1-3 error or answer exactly, through CTEs, joins, windows and subqueries, on 55.0.0 and 55.1.0.

The price:

- **Code.** ~350 lines of accumulator and functions, ~120 of rule. The real cost is the closed-world obligation: the rule must enumerate everything that can overflow and refuse what it does not know. Uncovered and untested here: `power`, `factorial`, bit aggregates, `array_sum`, integer `-` on timestamps, and whatever a future release adds. A denylist is the wrong shape; it is the README's file-I/O argument again, and the answer is the same: whitelist the plan.
- **Two things no plan rewrite can fix.** `TRY_CAST` to decimal returns NULL by definition and SUM then drops the row (3a, 3c); the Nuthatch pattern is wrong at ingest, not at the sum. And integer literals over 19 digits are Float64 before any rule runs (2k). Both must be refused at the SQL surface.
- **uint256 does not fit Decimal256.** Max uint256 is 78 digits; `Decimal256(76,0)` refuses 77-digit values that fit i256 (2l) and cannot hold 2^255 at all. An exact uint256 balance out of DataFusion is therefore text (`checked_sum_text`) or a decision to refuse balances over 10^76-1. Owning the aggregate, as this prototype does, is what lifts that ceiling.
- **Speed and memory.** 1.7x on the aggregate and +130 MB for Decimal128 input; parity and +100 MB for text input; see above. Against Burrmill's 256 MB gate the memory is the number that matters.
- **The order-independence question is settled.** With a 320-bit accumulator the sum is order-independent *and* refuses correctly: `MAX, +1, -1` answers, `MAX, +1` refuses. That is ROADMAP 2.1b's "16 bytes per group" paid, and it is the only design that does both.

DuckDB, for the record: strictly better than stock DataFusion on scalars and casts; the same 39-digit leak on `SUM(DECIMAL(38,0))`; wraps `SUM(DECIMAL(38,0))` past i128 at 32 threads over two files while refusing at one; reads Decimal256 parquet as DOUBLE; `AVG(DECIMAL)` is a DOUBLE of the wrapped sum. Its `BIGNUM` type (new to me) gives exact uint256 sums today at 32 threads, at the price of accepting non-canonical text and truncating `7.9` to 7 where its own `TRY_CAST` rounds to 8. If Nuthatch stays on DuckDB, `SUM(amount::BIGNUM)` replaces the `_overflow` flag outright.

## Not tested

- DataFusion releases other than 55.0.0 and 55.1.0.
- Aggregates other than `sum`/`avg`; window frames needing `retract_batch`; `Utf8View`/`LargeUtf8` input through the rule (the UDAF coerces them; the built-in refuses them, 3d).
- Whether `simplify_expressions` or `unwrap_cast` could re-introduce a built-in `+` after the rule ran; it did not in any case here, but I did not prove it.
- The prototype's RSS at partition counts other than 8, or against Burrmill's own gate fixture.
- A single large parquet file split by byte range across partitions (DataFusion's `repartition_file_min_size` is 10 MB; my one-file case was too small and ran as one partition, and given finding 1 it would not have changed the answer).
- DuckDB at libduckdb-sys 1.10501.0 as the README measured; 1.5.3 was what tp had.

Nothing was committed or pushed; `~/Projects` on tp was not touched.
