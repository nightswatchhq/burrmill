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
