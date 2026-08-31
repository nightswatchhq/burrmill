# Roadmap

RFC-0044 §7 is the *design* ladder. This is the schedule, and where the two disagree, this one is
the record of what measurement said. It is rewritten after every stage rather than defended.

**Where §7 was wrong, stated first.** It costed slices 0 and 1 together at eight to eleven weeks;
the code took a day. That is not a triumph, it is a calibration error in the other direction too:
§7 costed the *hardening* at zero, and hardening is where the remaining work actually lives. A fold
that beats DuckDB on a bench is perhaps a fifth of a fold you would let answer a stranger's query.
§7 also has no slice for input repartitioning, because nobody knew it would be needed until the
memory gate failed. Treat its engineer-week figures as an order of magnitude, not a plan.

---

## Stage 1 — finish slice 1 · IN PROGRESS

Slice 1's gate is "≤1.0x DuckDB at exact parity **under the 256 MB RSS gate**". Latency passes
everywhere and has got better. RSS is now under the budget up to eight threads and over it above
sixteen, which by the RFC's own rule is still a stop rather than a pass, so nothing below this line
starts until it clears.

**1.2 has moved the blocker rather than cleared it, and it has changed what the gate means.** One
shared aggregate instead of one per worker cut peak RSS at a million groups by 2.2-3.4x and improved
latency at every core count. Measured like for like on one 32-core box against commit `eec0699`:
**538 MB → 146 MB at one thread, 584 → 210 at eight, 769 → 349 at thirty-two.** The aggregate is 99
MB at every thread count, so that part of the claim is exact; everything else costs about 6 MB per
thread. The gate passes to 8 threads, misses by 1% at 16 and by 36% at 32 — and 36 to 67 MB of that
is memory glibc has freed and not returned, not working set.

**1.1 is done and it cost the real-nest numbers.** They were measuring a 38,429-file directory scan
charged to DuckDB and not to Burrmill. Restated like-for-like they are 0.48-0.78x rather than
0.11-0.71x, and they only reach that because 1.1 turned up a missing projection pushdown on the way.
The synthetic sweep never touched that path and stands unchanged.

| # | Work | Done when |
|---|---|---|
| ~~1.1~~ | ~~Explain DuckDB's flat ~620 ms on the real nest directory~~ | **DONE 2026-08-31. Both halves happened: the comparison was wrong and the numbers are restated downward.** The nest's segments directory holds 38,429 files and DuckDB re-globbed all of them inside every timed run while Burrmill's `read_dir` sat outside the timer. Corrected, the published 0.11-0.71 became 1.00-2.98 — Burrmill *slower*. A fifth defect found in the process (the fold had no projection pushdown and decoded 14 columns to read 3) brings it to 0.48-0.78. See the progress log |
| ~~1.1a~~ | ~~A realistic-width synthetic fixture~~ | **DONE 2026-08-31.** Twelve columns matching a real nuthatch event, two of them 66-character hex. Demonstrated rather than asserted: with the projection deliberately disabled, the old fixture shows a **1.00-1.07x** penalty and the new one **1.6-2.5x**. It also flipped DataFusion from ~0.8-0.9x DuckDB to **2.0-3.9x**, because a wide table punishes an engine that decodes columns nobody asked for. Every published sweep number is restated on it |
| ~~1.1b~~ | ~~A gate that refuses a flat comparison~~ | **DONE 2026-08-31.** The nest harness halves its input and checks whether each engine's time follows. Over 50% fixed and **no ratio is printed** — the field reads `UNSAFE_fixed_duck=66pct_burr=17pct`. It refuses exactly the 0.11 that was published this morning and passes the legitimate ones. Its own first version gated a different quantity from the one it measured, and was caught the first time it ran |
| 1.2 | ~~Input repartitioning~~ **One shared partitioned aggregate, not one per worker** · PARTIALLY DONE | Peak RSS at 1M groups **538→146 MB at 1 thread, 584→210 at 8, 769→349 at 32**, latency better at every count, parity verified on all 14 sweep configurations. `agg_bytes` is 99 MB at every thread count, which is the claim as a measurement. **Passes to 8 threads, misses by 1% at 16 and 36% at 32**, so the gate is not passed |
| 1.2a | The last of the per-thread cost | Two candidates ruled out by measurement, not argument: **streaming the output cannot help** (a globally sorted answer needs every row live), and **it is not the decoder** — with a trivial aggregate the same scan costs 1.3 MB/thread against 5.6 MB/thread at 1M groups, so the scaling follows the aggregate. What is left is concurrent hash-table growth and 36-67 MB of freed-but-unreturned memory behind it. Needs partition pre-sizing from a cardinality estimate, or an arena-allocated table |
| 1.2c | **State the gate's parallelism** | 256 MB is not a budget until it says at how many threads. The same binary passes on a laptop and fails on a build server, and that is a specification defect rather than a code one. An RFC amendment, not a patch |
| 1.2b | The small-aggregate regression | 509 groups went 48 MB → 76 MB and 0.76 → 0.82x. Sixty-four partition tables are pure overhead for an aggregate that fits in L2 as one, and a shared aggregate cannot use the old promotion threshold because the partitions are what make it shared. Small, real, and recorded rather than rounded away |
| ~~1.3~~ | ~~A real global memory budget~~ | **DONE as a consequence of 1.2.** `mem_pool_bytes` is now checked against the query's whole aggregation rather than one worker's share, because there is only one. Still not process RSS — decode buffers sit outside it — and the doc comment says so. `max_bytes`, which was a field nothing read, is now enforced too |
| ~~1.4~~ | ~~Seal-layout canary~~ | **DONE 2026-08-31.** `tests/seal_layout.rs` writes the layout contract down as assertions, and `BURRMILL_NEST=<dir>` checks it against a real nest (38,428 segments, 34 tables). It found a genuine bug on the way — **a table name matching no segments returned an empty answer instead of refusing**, where DuckDB refuses — and then found that its own `<contract>__<event>` grammar was wrong: `grt_total_supply` is a *call* table sealed from an `eth_call`, not a log |
| 1.4a | The fold has only ever run on event tables | The canary turned up a second sealed shape (`calldata` / `result` / `reverted`, no event name). The signed fold has no meaning on one, but the catalog opens it quite happily. Worth knowing what the admitted subset should say about it |

**1.1 was first** because it was an evening's work and it decided whether the best numbers already
published were real. They were not. Publishing a flattering measurement and defending it later is
the failure mode RFC-0004 exists to prevent, and the lesson to carry is that the parity guard — the
thing this project is proudest of — is blind to this entire class of error. Parity says the two
engines agree on the answer. It says nothing about whether they were asked the same question.

**Stop condition, and where it stands:** if repartitioning cannot hold 256 MB without giving back the
latency win, the honest outcome is a keep-amendment quantifying the trade, not a quietly relaxed
budget. Nothing has been given back — latency improved — so the condition has not been triggered.
The budget has not been relaxed either. The gate simply is not met at a million groups, and that is
recorded as a fail rather than argued away.

---

## Stage 2 — earn the right to be trusted

Owning execution means owning the bugs, and nineteen hand-written refusals are not a corpus.

| # | Work | Done when |
|---|---|---|
| 2.1 | ~~Allowlist-constrained query generator~~ **· DONE, and it found things** | Two oracles: a non-optimising reference in `tests/generated_folds.rs` that runs on every `cargo test` and survives DuckDB's removal, and `burrmill-bench gen` against DuckDB itself. **3,000 cases: 2,684 answers agreed exactly, 308 both refused, 8 order-dependent.** Mutation-checked — a `wrapping_add` and a dropped row are both caught with reproducible seeds. Still owed: nightly CI |
| 2.1a | **Decide the `TRY_CAST` divergence** | 8 of 20 edge literals differed. Whitespace is fixed (` 7` is seven, and we were dropping the row). DuckDB also takes `1e18`, `7.0`, `1_000` and **rounds `7.9` to 8**; adopting silent rounding into an engine whose first claim is exactness needs a decision. `burrmill-bench cast` prints the table |
| 2.1b | **Decide whether refusal should be order-independent** | Refusal fires when an intermediate partial sum leaves `i128`, not when the answer does: `MAX, +1, -1` sums to exactly `MAX` and is declined. DuckDB does it too, in both directions. Fixing it means accumulating wider than `i128`, which costs 16 bytes per group — against a memory gate already being missed. `tests/generated_folds.rs` pins today's behaviour so a fix inverts a test deliberately |
| ~~2.2~~ | ~~Overflow reached by generation~~ | **DONE.** 86 of 180 generated cases in the library test reach the refusal path, against a benchmark fixture that topped out at 1e20 and could never have reached it at all |
| 2.3 | `sqllogictest-rs` corpus | The parity oracle that survives DuckDB's own removal (Q4) |

---

## Stage 3 — the seam (RFC-0044 §3.4, slice 2)

The highest-risk invariant in the design, and the only place where owning execution and getting the
semantics right are the same job.

| # | Work | Done when |
|---|---|---|
| 3.1 | redb hot-tip provider with a seam parameter | Reads the tip at a pinned boundary |
| 3.2 | Seam as two scheduled pipelines into one sink | The `UNION ALL` model DuckDB uses, because our seam *is* a `UNION ALL` |
| 3.3 | COR-1 property tests under concurrent seal | No row is double-counted or dropped while a segment seals underneath a running query |

**Stop condition:** COR-1 is a correctness invariant, not a performance target. A seam that is fast
and occasionally double-counts is worthless.

---

## Stage 4 — more shapes, and the ratchet that measures them

Today Burrmill owns exactly one plan shape. The coverage ratio (§4.6) is therefore 1 shape / n, and
publishing that honestly from the start is what stops "hybrid now, own more later" becoming
"hybrid forever".

| # | Work | Done when |
|---|---|---|
| 4.1 | Count the plan shapes a real workload produces (experiment A4) | The number decides whether the owned planner is a dozen patterns or an open set |
| 4.2 | The remaining heavy folds — exposure, velocity | Each ≤1.0x DuckDB at parity under the RSS gate |
| 4.3 | Publish the coverage ratio per release, monotonic | It may not go down |

---

## Stage 5 — serving

Where the concurrency win is banked. #986 measured DuckDB going from 40.3 to 39.6 qps between one
client and thirty-two while p99 went 29.5 ms to 7066 ms, because it sits behind one connection
mutex. Burrmill takes no global lock, so this should be the easiest headline in the project — which
is exactly why it must be measured rather than assumed.

| # | Work | Done when |
|---|---|---|
| 5.1 | Streaming results and the async cancellation contract (Q5) | Cancellation delay bounded by one morsel, demonstrated |
| 5.2 | Re-run the #986 sweep | p99 ≤ DuckDB's at 32 clients, RSS within budget |

---

## Not on this roadmap

- **DataFusion as a cold-path fallback.** §7 puts it at slice 3. Deliberately deferred: building the
  fallback before the fast path is finished is how the fast path stops being finished.
- **JIT, on-disk format changes, a custom Parquet decoder.** Non-goals in §9 and they stay non-goals.
- **crates.io.** The name is reserved by being unclaimed. Publishing a query engine that owns one
  plan shape and overruns its memory budget would be a promise we cannot keep.
