# How Burrmill is measured

RFC-0004 discipline. None of this is ceremony: every rule here exists because its absence produced a
wrong number that somebody nearly acted on.

## Parity first, untimed, before any figure is printed

An earlier version of the nuthatch gate printed its `RESULT` line and *then* bailed on a parity
failure, so an invalid comparison could be copied into a record before anyone read the error. A
timing between two engines that disagree is not fast-versus-slow, it is meaningless. The harness now
runs all three engines once, untimed, compares row sequences, and refuses with no `RESULT` line if
they differ.

Prove the guard works rather than assuming it:

    BREAK_PARITY=1 ./target/release/burrmill-bench

It drops one row from Burrmill's answer on purpose. The run must refuse. A guard nobody has watched
refuse is not a guard.

## Run order is a confound

Whichever engine goes first pays to warm the OS page cache on the segments; the second gets it free.
The first such measurement in this lineage was 3.9x apart for exactly that reason. `ORDER` runs it
both ways, and only a ratio that survives both orderings is a statement about the engines.

There is a trap inside the trap: `std::env::var(k).is_ok()` is true for an **empty** value, so
`ORDER=` reads as "set". That bug silently forced one ordering across a whole sweep, and it was
found by the ordering control - which is the entire point of having one.

## Repeats inside one process, median reported

Writing a ten-thousand-file fixture costs far more than the query it is for, so a fresh process per
repeat would be measuring the fixture writer. `REPEATS=n` writes once and times n rounds; the median
is reported and every sample is printed beside it, because a median without its spread hides the run
that went four times slower.

## The fixture is nest-shaped, not benchmark-shaped

Segment sizes on a live nest are **bimodal**: backfill batches at 20,000 rows and cuts on a
data-chosen block boundary, while the tip path seals whatever just finalised - a few blocks carrying
a handful of rows. Measured on `horizon-nest`: 80% of segments under 20 KB, 6.3 KB median.

An even split across files is a different problem and would flatter whichever engine handles uniform
work best. The writer does not offer one.

The rows are also *offsets into one generated table*, so the union of any layout is exactly the rows
a single file of the same total would hold. **The fold must therefore return an identical answer at
every segment count**, and a layout that changes the answer is caught by the parity check without
needing a separate oracle.

Values are around 1e20: past `i64` and nowhere near `i128`. That is deliberate - it is why a 128-bit
cast is in the query at all, and a fixture of small values would let a broken cast pass unnoticed.
It also means the sweep **does not** reach the overflow boundary, which is why refuse-on-overflow is
pinned by unit tests at `i128::MAX` rather than by this harness.

## RSS is measured on the operator, not on the harness

The combined harness has DuckDB linked and a DataFusion session instantiated, so its footprint is
mostly theirs. Reporting that as Burrmill's would be a number that does not measure what it claims -
here it errs pessimistic, which is no better. Use `fold` against a kept fixture for the real figure:

    KEEP_FIXTURE=/tmp/fx ROWS=2000000 SEGMENTS=100 ./target/release/burrmill-bench
    REPEATS=5 ./target/release/burrmill-bench fold /tmp/fx

## The high-cardinality gate

`ADDRS` sets the number of distinct parties. This is the named gate of RFC-0044 §6 and the one place
DataFusion is known to be weakest: its two-phase aggregation re-hashes across phases and its memory
grows with core count (#6937, #11680). An owned operator that did the same under a 256 MB budget
would not deserve to ship, so it is measured rather than asserted.

## The fixture is as wide as a real event, not as wide as the query

Segment sizes were nest-shaped from the start; the *schema* was not. The first fixture had four
columns and a real nuthatch event has twelve, two of them 66-character hex hashes. The fold reads
three columns, so on a four-column fixture projection pushdown is worth nothing - and the operator
shipped for an entire slice with **no projection at all**, decoding fourteen columns to read three
on the real nest and running 2.2x DuckDB because of it. Not one measurement noticed.

Measured both ways, with the projection deliberately disabled to reproduce the defect:

| fixture | projection on | projection off | penalty |
|---|---:|---:|---:|
| narrow, 512 groups | 14 ms | 15 ms | 1.07x |
| **nest-shaped**, 512 groups | 14 ms | 35 ms | **2.5x** |
| narrow, 200k groups | 35 ms | 35 ms | 1.00x |
| **nest-shaped**, 200k groups | 34 ms | 55 ms | **1.6x** |

On the narrow fixture the defect is unmeasurable. A fixture that cannot exhibit a defect cannot
guard against it, and the general rule this is an instance of is that **the fixture has to be
unrealistic only in the ways the experiment is about**. `NARROW=1` writes the old schema, and exists
only so this table can be reproduced.

## A ratio dominated by fixed cost is not printed as a number

The parity guard compares answers. It cannot see a timing that is measuring the wrong thing, and
three separate times in one day it did not: a harness that charged DuckDB for a 38,429-file directory
scan Burrmill was given free, an RSS gate that counted a million of its own `String`s, and an
`rss_mb()` that reported current RSS on macOS and a peak high-water mark on Linux. In every case both
engines agreed on the answer, so parity said nothing.

The nest harness now halves its input and checks whether each engine's time follows. Fixed cost is
estimated by linear extrapolation from the two points - crude, and quite enough to tell 5% fixed from
90%. If more than half of either engine's time is independent of how much data it read, **no ratio is
printed**: the field reads `UNSAFE_fixed_duck=66pct_burr=17pct` and a warning goes to stderr. RFC-0004's
discipline is to make the wrong figure unavailable rather than merely discouraged, because a number
that is merely discouraged is a number somebody will paste into a README.

The headline `ratio` is the like-for-like one, DuckDB handed the same catalog Burrmill holds. The
glob-path number is reported beside it as `glob_ratio` and carries its own gate, which it fails on
the small tables - correctly, since that is precisely the figure that was published and wrong.

The first version of this check measured the list path and then gated the glob-path ratio, which is a
different quantity: the same mistake in miniature as the one it exists to prevent. It was caught the
first time it ran, which is at least the intended failure mode.

## Every engine gets the same thread budget

Burrmill bounds its own parallelism (`Limits::max_threads`, default 8), because a memory budget that
depends on the host's core count is not a budget: the same binary measured 145 MB on one thread and
340 on thirty-two at a million groups.

The moment that bound landed, two configurations went from about 0.5x DuckDB to 1.3x. It looked
exactly like a regression and it was a harness fault: eight threads were being compared against
thirty-two and the difference called an engine. The same shape as the 38,429-file glob, three items
earlier in the same day.

So the harness now sets `SET threads TO n` on DuckDB and `target_partitions` on DataFusion to match,
and `THREADS=n` moves all three together. **A ratio is a statement about engines only when everything
else is held equal**, and parallelism is now one of the things to hold rather than one to inherit.
