# Progress log

Newest first. One entry per RFC-0044 slice.

---

## Slice 1 — owned operator, in-graph — 2026-08-31

**Gate: PASS on latency, FAIL on peak RSS.** The thesis reproduces; the memory budget does not.

### Latency, against DuckDB (>1.0 means slower)

Segment sweep, 2,000,000 rows, 512 parties, 5 repeats, median, both orderings agree to two decimals:

| segments | DuckDB | DataFusion 55 | Burrmill | ratio |
|---:|---:|---:|---:|---:|
| 1 | 30 ms | 24 ms | 17 ms | **0.57** |
| 100 | 17 ms | 29 ms | 13 ms | **0.76** |
| 1 000 | 71 ms | 49 ms | 45 ms | **0.63** |
| 10 000 | 678 ms | 626 ms | 434 ms | **0.64** |

High-cardinality gate, 100 segments, 2,000,000 rows:

| parties | DuckDB | DataFusion 55 | Burrmill | ratio |
|---:|---:|---:|---:|---:|
| 512 | 17 ms | 29 ms | 13 ms | **0.76** |
| 9 896 | 20 ms | 32 ms | 19–20 ms | **0.95–1.00** |
| 197 936 | 66 ms | 60 ms | 52 ms | **0.79** |
| 989 690 | 162 ms | 121 ms | 74 ms | **0.46** |

Real sealed segments, `graph-allocations-nest`, read-only, nothing written or copied:

| table | segments | rows | parties | DuckDB | Burrmill | ratio |
|---|---:|---:|---:|---:|---:|---:|
| `escrow__deposit` | 869 | 6 712 | 104 | 619 ms | 67 ms | **0.11** |
| `escrow__escrow_collected` | 905 | 67 004 | 69 | 622 ms | 182 ms | **0.29** |
| `staking__stake_delegated_withdrawn` | 2 875 | 102 766 | 96 353 | 762 ms | 467 ms | **0.61** |
| `staking_legacy__stake_delegated` | 2 985 | 346 288 | 309 664 | 788 ms | 559 ms | **0.71** |

Every row above had parity verified before any timing was printed, and `BREAK_PARITY=1` was run to
watch the guard refuse rather than assume it would.

Two caveats on the real-nest numbers, because they flatter. The tables with few rows and many
segments are metadata-bound rather than compute-bound - `escrow__deposit` is six thousand rows spread
over eight hundred and sixty-nine files - so 0.11x is a statement about opening files, not about
folding. And DuckDB's `read_parquet` glob appears to pay a fixed cost around 620 ms on this
directory regardless of table size, which is suspiciously flat and worth understanding before the
number is quoted anywhere.

### Peak RSS: FAIL

Operator only, no DuckDB linked and no DataFusion session, against the 256 MB gate:

| parties | peak RSS | verdict |
|---:|---:|---|
| 509 | 48 MB | pass |
| 197 936 | 682 MB | **fail, 2.7x over** |
| 989 690 | 1 008 MB | **fail, 3.9x over** |

The cause is understood and is not mysterious: twelve workers each build a table spanning the whole
key space, so a million distinct parties means roughly twelve million live entries to produce
989,690 rows. The answer itself is about 57 MB. Everything above that is thread-local duplication -
which is, precisely, the DataFusion behaviour this RFC criticises in §4.1 (#6937, memory growing
with core count). Radix partitioning fixed the *time*; it did not fix this, because partitioning a
worker's table does not stop that worker from seeing every key.

The fix is input repartitioning - workers exchanging morsels so each owns a key range rather than a
row range - and it is a real piece of work, not a tuning knob. Recorded as owed, not as done.

### What was built

- `crates/burrmill` — the library. arrow, parquet, rustc-hash, rayon, sqlparser, hashbrown. No
  DuckDB, no DataFusion, no C++.
- `crates/burrmill-bench` — `publish = false`. Both oracles live here so neither can reach the
  shipped graph.
- The admitted subset and its refusals (19 tests), the owned planner for the signed-fold shape, the
  owned partitioned aggregation, checked i128 arithmetic, canonical ordering, `EXPLAIN`, and a
  read-only real-nest harness.

### Four defects found by measurement, in order

Each of these looked fine and was wrong, and none would have been found by reading the code.

1. **A morsel was "one row group".** `ArrowWriter`'s default row group is 1,048,576 rows, so two
   million rows on a single-segment table was two units of work: the fold ran on two threads against
   DuckDB's twelve. 1.16x. Fixed by making a morsel a bounded row range.
2. **Ten thousand footers parsed serially** before any folding began, a prologue DuckDB does not pay.
3. **A clock read per merged entry** — five million of them at ten thousand morsels, to enforce a
   budget that cannot change that fast.
4. **One hash table per work unit.** Ten thousand tiny segments meant ten thousand fresh maps each
   growing to five hundred entries, so nearly every key allocated its own boxed string rather than
   hitting an existing one: four million allocations for a two-million-row query. The rows had not
   changed, only how they were filed.

Then the aggregation itself, in three measured versions — map-per-unit with a serial merge (3.11x at
200k groups), map-per-thread with a parallel tree merge (1.96x), and arena keys with radix
partitions promoted on demand (0.79x). Promoting unconditionally cost the ten-thousand-group case
1.55x against 0.90x, which is why the table starts as one and splits itself only when it grows.

### Owed

- **Peak RSS at high cardinality** (above). The named blocker.
- The seam, the hot tip, and COR-1 — slice 2, and the highest-risk invariant in the design.
- A test that fails when nuthatch's seal layout changes. Burrmill has no path dependency on nuthatch
  by design, so a layout change will not break the build; it will break the *reading*, silently.
- Differential fuzzing (NoREC/TLP). Nineteen hand-written tests are not the corpus §3.7 asks for.
- The DuckDB glob's flat ~620 ms on the real nest directory, understood rather than quoted.
