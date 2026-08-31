# Progress log

Newest first. One entry per RFC-0044 slice.

---

## Stage 2.1 and 2.2 — a generated corpus, and the two things it found — 2026-08-31

Nineteen hand-written refusals and three overflow tests were not a corpus. There are now two oracles,
and they found a semantic bug and a semantic surprise within a few thousand cases.

### Two oracles, because they catch different things

**A non-optimising reference**, in `tests/generated_folds.rs`, running on every `cargo test`: the
same semantics in fifteen lines with a `BTreeMap`, on one thread, straight from the generated rows,
no Parquet and no parallelism. It catches machinery bugs — morsel splitting, the shared aggregate
under locks, the parallel sort — and it survives DuckDB's removal in Q4, which is why it lives in the
library.

It cannot catch a *misconception*. The reference implements this author's reading of what `TRY_CAST`
means, so if that reading is wrong the reference and the engine are confidently wrong together.
Hence **`burrmill-bench gen` against DuckDB**, which is an independent implementation of the standard
and the only thing here that is ground truth about semantics rather than about code.

Three properties per case: the answer matches the reference; the answer does not depend on how rows
were split into segments; the answer does not depend on thread count.

**Mutation-checked, because a generator that goes green on its first run is a clean result nobody
asked for.** Replacing `checked_add` with `wrapping_add` fails it; dropping the last row of every
batch fails it. Both report a reproducible seed.

3,000 DuckDB cases: **2,684 answers agreed exactly, 308 both refused, 8 order-dependent.**

### 2.2 — the boundary is now actually reached

86 of 180 generated cases in the library test reach the refusal path. The benchmark fixture topped
out around 1e20 against an `i128::MAX` of 1.7e38, so no amount of running it could ever have got
there; the boundary was pinned by three hand-written tests and nothing else.

### Finding one: `TRY_CAST` was silently dropping rows

Eight of twenty edge literals differ between `str::parse::<i128>()` and DuckDB's `TRY_CAST`:

| literal | Burrmill | DuckDB |
|---|---|---|
| `" 7"`, `"7 "`, `"\t7"`, `"  -5  "` | NULL | 7, 7, 7, -5 |
| `"1e18"` | NULL | 1000000000000000000 |
| `"7.0"` / `"7.9"` | NULL | 7 / **8** |
| `"1_000"` | NULL | 1000 |

The whitespace group is a plain bug and is fixed: `" 7"` is seven, and Burrmill was skipping the row
and returning a short balance. It is exactly the class that survives a benchmark, because on real
nest data — `uint256` as plain digits — the two are identical.

The rest are not bugs but choices, and DuckDB rounding `"7.9"` to **8** is not one to adopt casually
into an engine whose first claim is exactness. Left open as 2.1a, with `burrmill-bench cast` printing
the whole table so it is visible rather than buried. The generator deliberately does not draw those
four literals: a corpus that fails for a reason nobody has chosen yet is just noise.

### Finding two: refusal is order-dependent, in both engines

The corpus turned up DuckDB refusing where Burrmill answered, and Burrmill refusing where DuckDB
answered. Neither returns a wrong number. They disagree about whether the query is *answerable*.

Checked addition is order-dependent. A party whose values are `i128::MAX, +1, -1` sums to exactly
`i128::MAX` and fits — but any order that meets the `+1` first overflows on the way to an answer it
could have represented.

So **"exact integer arithmetic, refuses on overflow" is a little more eager than it reads**: it
refuses when an intermediate partial sum leaves the range, not when the answer does. The guarantee
that matters is intact — no wrapped number, ever, in either engine — but the README now says this
plainly rather than letting the claim imply more than it delivers.

Fixing it means accumulating wider than `i128`, which costs 16 bytes per group against a memory gate
already being missed at 32 threads. That is a trade, not a patch, so it is 2.1b and
`tests/generated_folds.rs` pins today's behaviour with a test that says in as many words that a
deliberate fix should invert it rather than delete it.

---

## Roadmap 1.4 — the seal-layout canary, and the empty answer it found — 2026-08-31

Burrmill has no path dependency on nuthatch by design, which is what makes the pure-Rust dependency
claim true and lets the two evolve separately. The cost is that a layout change cannot break the
build; it breaks the *reading*, at runtime, quietly. `tests/seal_layout.rs` writes the contract down
as assertions instead of as a paragraph, and `BURRMILL_NEST=<dir>` checks it against a real nest.

### It found a real bug before it found anything about layout

Pointing the harness at a table name that does not exist: **DuckDB refused with "No files found that
match the pattern". Burrmill planned `files=0 morsels=0` and would have returned an empty answer.**

That is the failure this project exists to refuse, in its purest form. "No rows" and "no such table"
are different answers, and the first is a wrong answer that looks entirely plausible — the balances
came back empty, so presumably nobody staked anything. A nest keeps all 34 of its tables' segments in
one directory, so a mistyped name is always exactly one prefix away.

`open_nest_table` now returns `BurrmillError::NoSegments` and names the prefixes that *are* present,
because the mistake is nearly always a near-miss and a list of neighbours turns a puzzled afternoon
into a glance.

### The canary's own grammar was wrong, and that is the point of having one

The contract asserted that a table is `<contract>__<event>`. The first run against a real nest failed
on `grt_total_supply` — a **call** table, sealed from an `eth_call` rather than from a log, carrying
`calldata` / `result` / `reverted` / `content_address` and no event name at all. The grammar was this
crate's assumption, not nuthatch's.

Corrected to what is true: `<table>-<hex content hash>.parquet`, where the table part may or may not
be event-shaped. The event/call mix is now *reported* rather than asserted, because a nest growing a
third shape is news and not necessarily a fault.

Filed as 1.4a: **the fold has only ever run on event tables.** The signed fold has no meaning on a
call table, but the catalog will open one quite happily, and the admitted subset should probably have
an opinion about that.

### The hazard that is not hypothetical

A real nest carries both `staking__stake_delegated` and `staking__stake_delegated_withdrawn`. Bare
`starts_with` would fold the second into the first and report delegations larger than they are — a
wrong balance that reads as perfectly reasonable. The trailing separator is what prevents it, which
makes the separator load-bearing and puts it in a test rather than in a comment.

Verified the canary actually fires: renaming a real segment's separator from `-` to `_` fails it with
"the layout Burrmill assumes has changed and every table would now resolve to zero segments". A guard
nobody has watched refuse is not a guard.

Against the real nest: 38,428 segments, 34 tables, all of them open.

---

## Roadmap 1.1a and 1.1b — making the harness able to catch what it missed — 2026-08-31

Three measurement defects turned up in one day and the parity guard saw none of them, because in
every case both engines agreed on the answer. These two items are the guards for that class, and both
are demonstrated rather than asserted.

### 1.1a — the fixture is now as wide as a real event

The fixture had four columns; a real nuthatch event has twelve, two of them 66-character hex hashes.
The fold reads three, so on a four-column table projection pushdown is worth nothing — which is why
the operator shipped for an entire slice with **no projection at all** and not one measurement
noticed.

Reproduced by deliberately disabling the projection again:

| fixture | projection on | projection off | penalty |
|---|---:|---:|---:|
| narrow, 512 groups | 14 ms | 15 ms | 1.07x |
| **nest-shaped**, 512 groups | 14 ms | 35 ms | **2.5x** |
| narrow, 200k groups | 35 ms | 35 ms | 1.00x |
| **nest-shaped**, 200k groups | 34 ms | 55 ms | **1.6x** |

On the old fixture the defect is unmeasurable. The general rule, worth stating because it is not only
about columns: **a fixture may be unrealistic only in the ways the experiment is about.** Segment
sizes were nest-shaped from day one and the schema was not, and the schema is where the bug lived.

**It also changed the competitive picture, in Burrmill's favour and honestly.** On the wide table
DataFusion goes from roughly 0.8-0.9x DuckDB to **2.0-3.9x**, because a wide table punishes an engine
that decodes columns nobody asked for. Burrmill's ratios improved for the mirror-image reason, to
0.17-0.67. Every published sweep number is restated on the new fixture, and the old ones should not
be compared against: they were measured on a table no nest ever writes.

Widening the schema cost no memory at all, which is the projection working: three columns are decoded
whatever the file holds.

### 1.1b — a ratio dominated by fixed cost is not printed as a number

The nest harness now halves its input and checks whether each engine's time follows, estimating fixed
cost by linear extrapolation from the two points. Crude, and quite enough to tell 5% fixed from 90%.
If more than half of either engine's time is independent of how much data it read, **no ratio is
printed**: the field reads `UNSAFE_fixed_duck=66pct_burr=17pct` and a warning goes to stderr.

On `escrow__deposit` — the table whose 0.11 was this morning's most flattering published figure —
`glob_ratio` is now withheld at 66% fixed, while the like-for-like `ratio=0.48` passes at 8% and 17%.
It refuses exactly the number that was wrong and passes the ones that are not.

Making the wrong figure *unavailable* rather than discouraged is the point. A number that is merely
discouraged is a number somebody pastes into a README.

**Its own first version was wrong in the same way it exists to prevent**: it measured the
explicit-list path and then gated the glob-path ratio, which is a different quantity. Caught the first
time it ran, which is at least the intended failure mode.

### Also fixed: the gate script was inflating its own headline

`run-gate.sh` measured operator RSS with `REPEATS=5`. Allocator retention accumulates across folds
inside one process, so it reported 608 MB where a single fold reports 339. The script now uses
`REPEATS=1` and sweeps thread count, since parallelism is the load-bearing variable the gate does not
name.

### Where the gate stands on the realistic fixture

1M groups, 32-core box, true peak, single fold:

| threads | peak RSS | verdict |
|---:|---:|---|
| 1 | 147 MB | pass |
| 4 | 180 MB | pass |
| 8 | 204 MB | pass |
| 16 | 251-259 MB | **straddles the 256 MB line** |
| 32 | 339 MB | fail |

Sixteen threads gives 251 on one run and 259 on another. That is not a pass and not a comfortable
fail, and declaring either would be picking the run that suits. It is the sharpest argument yet for
1.2c: **a budget with no stated parallelism cannot be evaluated at all.**

---

## Roadmap 1.2 — one aggregate for the query, not one per worker — 2026-08-31

**The shape change is done and holds. The gate is met up to eight threads and missed above sixteen.**
Peak RSS at a million groups fell 2.2x to 3.4x depending on core count, and latency improved at every
core count rather than being traded away.

### What changed

Each worker used to build an aggregation spanning the whole key space, and twelve of them were merged
at the end. A million distinct parties therefore meant roughly twelve million live entries to produce
a 57 MB answer — memory growing with core count, which is exactly the DataFusion behaviour RFC-0044
§4.1 criticises by name (#6937).

Now there is one `SharedAgg`: sixty-four radix partitions, each behind its own lock, written to by
every worker. A key exists in exactly one table however many threads touched it. **`agg_bytes` is 99
MB at one thread and 99 MB at thirty-two**, which is the claim stated as a measurement rather than as
an intention.

The lock is paid for by batching. A `Scatter` buffers rows per partition and drains 4,096 at a time.
The size matters in both directions and the two regimes are far apart: a synthetic segment is 20,000
rows, a real nest segment averages **116**, so flushing per batch would have meant sixty-four lock
acquisitions per hundred-odd rows. The constant was swept rather than guessed; 16,384 was the first
guess and was worse on both memory and latency.

**The merge phase is gone entirely.** There is nothing left to merge; the workers were writing into
the answer as they went.

### Peak RSS and latency, 1M groups, like for like

Same machine (32-core Debian), same fixture, same true high-water mark, single run. The `before`
column is commit `eec0699` built and measured on that same box, because a before-and-after across two
machines and two different definitions of "RSS" is not a comparison.

| threads | before | after | before ms | after ms |
|---:|---:|---:|---:|---:|
| 1 | 538 MB | **146 MB** | 870 | **575** |
| 4 | 557 MB | **186 MB** | 286 | **221** |
| 8 | 584 MB | **210 MB** | 192 | **185** |
| 16 | 608 MB | **259 MB** | 183 | **162** |
| 32 | 769 MB | **349 MB** | 194 | **167** |

Against the 256 MB gate: **pass at 1, 4 and 8 threads; miss by 1% at 16; miss by 36% at 32.**

Two things follow. The first is that the old code was 538 MB *on a single thread*, so thread
duplication was never the whole story — the answer's own representation was most of it. The second is
that **the RFC's gate does not say at what parallelism**, and parallelism is now the load-bearing
variable: the aggregate is flat at 99 MB and everything else costs about 6 MB per thread. A budget
that a build passes on a laptop and fails on a build server is not a budget yet.

### Ratios, unchanged or better

Parity verified on all fourteen sweep configurations. Burrmill ratios 0.16-0.67 against DuckDB, all
comfortably inside the ≤1.0 gate, and better than before at every point except one.

The exception: 512 groups over 100 segments went 0.76 to 0.82. Sixty-four partition tables are pure
overhead for an aggregate that fits in L2 as one, and a shared aggregate cannot use the old promotion
threshold because the partitions are what make it shared. Recorded as a real if small trade rather
than rounded away.

### Three further defects found on the way

1. **The answer allocated a `Box<str>` per group.** The exact per-key malloc this module's header says
   it removed from the table, reintroduced at the output and costing more there. The answer now
   carries one contiguous arena plus a 32-byte index row, and hash tables are freed per partition as
   their rows are built.

   This took three attempts and the middle one was wrong in an instructive way. Keeping the
   sixty-four partition arenas in place and giving each row a partition index avoids a copy, and
   measured **79 ms of output phase against 26 ms** — so the second attempt concatenated them into
   one arena, which sorted fast and allocated a second 42 MB copy of every key to do it. The third
   attempt keeps the arenas where they are and hoists their base pointers into a flat `&[&[u8]]`
   before sorting. That was the entire cause of the 79 ms: indexing a `Vec<Vec<u8>>` inside the
   comparator is two bounds-checked indirections, and sixty-four pointers in a flat slice stay in L1.
   The copy was never necessary; it was paying 42 MB to work around a nested index.

2. **The RSS gate harness was measuring itself.** `fold_only` went through `oracles::burrmill`, which
   collects the answer into `Vec<(String, i128)>` for the parity comparison — a million fresh
   `String`s inside the number that was supposed to be the operator's, worth about 80 MB.

3. **`rss_mb()` reported two different quantities depending on the machine.** `ps -o rss=` on macOS is
   the process's RSS *right now*; `VmHWM` on Linux is a true high-water mark. Peak in a fold happens
   while the aggregate and the answer are both live, and by the time the harness asked, that moment
   had passed — so the development laptop systematically reported the friendlier number. Both now go
   through `getrusage`, and the macOS figure agrees with `/usr/bin/time -l` to the megabyte.

   That is three measurement defects in one day, all the same shape: the harness flattering the thing
   it exists to check. The parity guard cannot see any of them, because in every case both engines
   agreed on the answer.

4. **`max_bytes` was a field in `Limits` that nothing read.** Now enforced against the answer's own
   bytes, which the arena representation makes cheap to compute.

### Where the remaining memory is, and why the obvious fixes are the wrong ones

Three candidates were on the table for closing the gap. Two were ruled out by measurement rather than
by argument, which is the only reason the third is worth doing.

**Not streaming the output.** A globally sorted answer needs every row live whatever you do with it,
so streaming cannot reduce the peak of a `query()` that returns one. It is a serving-path change
(stage 5), not a memory one.

**Not bounding concurrent decode**, which was the first guess and looked obviously right. With a
trivial aggregate the same 2M-row scan costs 24 MB on one thread and 64 MB on thirty-two — **1.3 MB
per thread**. At 989,690 groups the per-thread term is 5.6 MB. The scaling is therefore not the
Parquet decoder at all; it follows the aggregate's size, which means concurrent hash-table growth and
the memory freed behind it.

**Most of the remaining overage is free memory that has not been returned.** The fold frees 57 MB of
hash tables in one go when it turns the aggregate into rows, and glibc holds a free that size rather
than handing it back. Forcing prompt return quantifies it:

| threads | default | `MALLOC_TRIM_THRESHOLD_=131072` | retained |
|---:|---:|---:|---:|
| 8 | 208 MB | 172 MB | 36 MB |
| 16 | 260 MB | 213 MB | 47 MB |
| 32 | 338 MB | 271 MB | 67 MB |

It is still RSS and it still counts. The point of measuring it is that the overage is mostly *not*
live data, so "allocate less" is not the fix — the tables have to exist and then stop existing. A
serving binary removes it with one environment variable. The library does not call `malloc_trim`
itself: reaching into the host's allocator is not a library's business, and it does not exist on
macOS.

Two things also worth knowing about the measurement. `REPEATS` must be 1, because retention
accumulates across folds inside one process and turns 349 MB into 507. And **the gate does not name a
thread count**, which is now the load-bearing omission: the same binary passes at eight threads and
fails at thirty-two. Filed as 1.2c, an RFC amendment rather than a patch.

**Slice 1 remains unpassed.**

---

## Roadmap 1.1 — the real-nest comparison was measuring the wrong thing — 2026-08-31

**The published real-nest ratios were wrong and are restated.** They are also, after a defect the
correction exposed, still under the gate — but by a different margin and for a different reason, and
the two facts have to be reported in that order.

### What the flat ~620 ms was

DuckDB's time on the real nest did not move with rows. Fitting the four recorded points against
segment count alone gives 553 ms fixed plus 0.076 ms per segment, with residuals of 0.2, 0.4, -9.0
and +8.7 ms — a 1.2% fit against file count while row counts ranged over fifty-twofold. A query
engine's time does not do that. Something other than the query was being measured.

It was the catalog. A nest keeps every table's segments in **one** directory, and that directory
holds **38,429 files**; the four tables under test are 2.3% to 7.8% of it. DuckDB's
`read_parquet('<dir>/<prefix>-*.parquet')` re-enumerated and re-matched all 38,429 inside every
timed repeat, while Burrmill's `open_nest_table` did its one `read_dir` *outside* the timer. The
harness was charging one engine for catalog construction and handing the other the same work free.

Decomposed, DuckDB's fixed cost is ~88 ms of `glob()` plus ~178 ms of bind, before a row is read.

### The correction, before any fix

Handing DuckDB the identical file list — same bytes, same plan, no pattern to expand — and running
Burrmill unchanged:

| table | published | like-for-like |
|---|---:|---:|
| `escrow__deposit` | 0.11 | **1.00** |
| `escrow__escrow_collected` | 0.29 | **2.98** |
| `staking__stake_delegated_withdrawn` | 0.61 | **2.12** |
| `staking_legacy__stake_delegated` | 0.71 | **2.24** |

Burrmill was 2.1x to 3.0x *slower* than DuckDB on three of four real tables and level on the fourth.
The 0.11 was 869 files' worth of fold against 38,429 files' worth of directory scan.

### The fifth defect found by measurement: no projection pushdown

The gap the correction exposed had a cause. `fold_morsel` built its reader with `with_row_groups`
and no `ProjectionMask`, so the fold decoded **every column in the segment** and used three. The
synthetic fixture is four columns wide, so this cost essentially nothing and was invisible for the
whole of slice 1. A real nest event is twelve to fourteen columns, two of them 64-character hex
hashes.

Adding the mask cut `scan_ms` by 2.7x to 4.4x: 504 ms to 129 ms on `staking_legacy__stake_delegated`,
173 ms to 39 ms on `escrow__escrow_collected`.

### Corrected figures

Apple M5 Pro, 18 threads, median of 5, parity verified first and separately between DuckDB's glob
and its own explicit file list. `list_ratio` is the like-for-like number the gate applies to.

| table | segments | rows | groups | list_ratio | cold_ratio | published (void) |
|---|---:|---:|---:|---:|---:|---:|
| `escrow__deposit` | 869 | 6,712 | 104 | **0.48** | 0.29 | 0.11 |
| `escrow__escrow_collected` | 905 | 67,004 | 69 | **0.78** | 0.32 | 0.29 |
| `staking__stake_delegated_withdrawn` | 2,875 | 102,766 | 96,353 | **0.76** | 0.46 | 0.61 |
| `staking_legacy__stake_delegated` | 2,985 | 346,288 | 309,664 | **0.77** | 0.49 | 0.71 |

`cold_ratio` is both engines building their own catalog, which is the honest reading of a
cold process; Burrmill wins it because a parallel `read_dir` plus parallel footer parse beats a glob
plus a serial bind. That is a real win and it is a *catalog* win, not a fold win, and it should never
again be quoted as though it were the latter.

The synthetic sweep is unaffected — it uses a purpose-built fixture directory and never touched this
path — and re-running it after the projection change shows no regression: 0.48 at a million groups,
0.82 at 200k, 0.54 at 1000 segments, against 0.46, 0.79 and 0.63 published. `BREAK_PARITY=1` still
refuses with no RESULT line.

### What this does not change

**Peak RSS still fails.** 982 MB on `staking_legacy__stake_delegated` against a 256 MB gate.
Projection pushdown does not touch it, because the memory is in twelve thread-local tables each
spanning the whole key space, not in the decoded columns. Item 1.2 remains the blocker and slice 1
remains unpassed.

### Owed, added

- **The harness gave one engine a catalog and charged the other for it, for a whole slice, and the
  tell was sitting in the results file** — a constant where a slope belonged. Worth a gate that
  refuses a comparison whose timings do not vary with input size, since the parity guard cannot see
  this class of error at all.
- **Projection pushdown existed nowhere and no test would have caught it.** The fixture cannot: it is
  four columns wide by construction. A fixture with a realistic schema width is owed.

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
