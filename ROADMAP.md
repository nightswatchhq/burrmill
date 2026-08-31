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

Slice 1's gate is "≤1.0x DuckDB at exact parity **under the 256 MB RSS gate**". Latency passed
everywhere; RSS failed by 3.9x. By the RFC's own rule that is a stop, not a pass, so nothing below
this line starts until it clears.

**1.1 is done and it cost the real-nest numbers.** They were measuring a 38,429-file directory scan
charged to DuckDB and not to Burrmill. Restated like-for-like they are 0.48-0.78x rather than
0.11-0.71x, and they only reach that because 1.1 turned up a missing projection pushdown on the way.
The synthetic sweep never touched that path and stands unchanged. **1.2 is now the whole of the
remaining gate.**

| # | Work | Done when |
|---|---|---|
| ~~1.1~~ | ~~Explain DuckDB's flat ~620 ms on the real nest directory~~ | **DONE 2026-08-31. Both halves happened: the comparison was wrong and the numbers are restated downward.** The nest's segments directory holds 38,429 files and DuckDB re-globbed all of them inside every timed run while Burrmill's `read_dir` sat outside the timer. Corrected, the published 0.11-0.71 became 1.00-2.98 — Burrmill *slower*. A fifth defect found in the process (the fold had no projection pushdown and decoded 14 columns to read 3) brings it to 0.48-0.78. See the progress log |
| 1.1a | A realistic-width synthetic fixture | The fixture is 4 columns and a real event is 12-14, which is precisely why the missing projection survived a whole slice. A fixture that cannot exhibit the defect cannot guard against it |
| 1.1b | A gate that refuses a flat comparison | The parity guard cannot see a timing that does not vary with input size, and that is what hid 1.1 in plain sight in the results file. Done when a run whose times are independent of row count fails rather than prints |
| 1.2 | Input repartitioning: workers exchange morsels so each owns a key range, not a row range | Peak RSS ≤256 MB at 1M groups; latency ratios do not regress past 1.0 |
| 1.3 | A real global memory budget | `mem_pool_bytes` means the process, not one worker; the current doc comment admits it does not |
| 1.4 | Seal-layout canary | A test fails when nuthatch's segment naming or schema changes. Burrmill has no path dependency by design, so today a layout change breaks its *reading*, silently |

**1.1 was first** because it was an evening's work and it decided whether the best numbers already
published were real. They were not. Publishing a flattering measurement and defending it later is
the failure mode RFC-0004 exists to prevent, and the lesson to carry is that the parity guard — the
thing this project is proudest of — is blind to this entire class of error. Parity says the two
engines agree on the answer. It says nothing about whether they were asked the same question.

**Stop condition:** if repartitioning cannot hold 256 MB without giving back the latency win, the
honest outcome is a keep-amendment quantifying the trade, not a quietly relaxed budget.

---

## Stage 2 — earn the right to be trusted

Owning execution means owning the bugs, and nineteen hand-written refusals are not a corpus.

| # | Work | Done when |
|---|---|---|
| 2.1 | Allowlist-constrained query generator, NoREC and TLP oracles | A generated corpus runs green against DuckDB nightly |
| 2.2 | Overflow reached by generation, not only by unit test | The fixture's values top out around 1e20 against an `i128::MAX` of 1.7e38, so the boundary is currently pinned by three hand-written tests and nothing else |
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
