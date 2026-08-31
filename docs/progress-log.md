# Progress log

Newest first. One entry per RFC-0044 slice.

---

## 5.1 — the cancellation contract, and the gate that had quietly broken it — 2026-08-31

RFC-0044 §3.5 makes a specific promise: **the delay between asking a query to stop and it stopping is
bounded by one morsel.** It contrasts this with DataFusion, whose joins do not yield to cancellation
at all (#19358). It is one of the reasons given for owning an operator rather than renting one.

It had **never been tested**. There was no cancellation test in the repository.

### Half of it was true

A query already folding stops promptly: the check sits at the top of `fold_morsel` and does what it
says.

### The other half had been broken three items earlier, by me

Roadmap 5.3 put an admission gate in front of the pool to fix a starvation bug. A yield point *inside*
the fold says nothing about a query that has not reached the fold — and with the gate occupied, a
cancelled newcomer sat in the queue until admitted and only then noticed.

Measured: **57 ms to notice, against a whole query of 110 ms.** "Bounded by one morsel" had become
"bounded by however long everybody ahead of you takes", silently, in a commit whose own tests all
passed.

The gate is a yield point now: a timed wait with a two-millisecond notice, well inside a morsel, paid
only by threads that are already blocked. Serving throughput and single-query latency are both
unchanged — 136/136/117 qps at 4/16/32 clients against 133-143/127-135/115-124, and 173-195 ms
against a band of 185-199.

### The subtle half of the fix

Admission is a ticket queue, so a waiter that gives up must **forfeit** its turn rather than merely
walk away. A ticket taken and never completed holds the line up for everyone behind it: the gate
would lose one slot per cancellation and seize after `width` of them. That has its own test, and the
test would *hang* rather than fail if the forfeit were wrong — which is why the comment says so
beside it.

### The lesson, which is the day's most-repeated one in yet another costume

A fix for one property broke another, and nothing caught it because **the broken property had no
test**. Not "the test was weak" — there was no test at all, for a guarantee written into the RFC and
into the crate's own documentation. Three sessions of work sat on top of it.

The first version of the test also passed while proving nothing: the query hogging the gate ran in a
loop, so it released its turn between iterations and let the queued query straight in. A test that
cannot fail is not evidence, and it took making it adversarial before the bug appeared.

---

## 5.5 — it did not need a scheduler, it needed the constant tuned at the right size — 2026-08-31

5.5 was filed as "work-stealing across queries; a scheduler, not a constant". Before costing that,
the question worth asking was why a *single* query only gets 3.3x from eight threads.

**It depends on query size.** A 181 ms fold scales 7.9x. A 48 ms one scales 4x. Not per-segment cost
— the speedup is the same with one segment as with two hundred — and not decode.

It is **lock contention** on the aggregate's sixty-four partitions. Each flush takes up to sixty-four
locks, so a worker that flushes proportionally more often contends proportionally more, and a small
query flushes more often relative to its total work. `FLUSH_ROWS` 4,096 → 16,384 takes a 500k-row
scan from **18 ms to 11** at eight threads while the single-threaded cost does not move at all —
parallel time changes, serial time does not, which is contention and nothing else.

### Why the constant was wrong

It was swept in roadmap 1.2, honestly and with a table, at **a million groups on thirty-two
threads**. At that operating point memory binds: every worker holds a scatter buffer, so the constant
multiplies by the fold's width, and 4,096 won on peak RSS.

A serving query is small, runs at eight threads, and is nowhere near the memory budget. There the
same constant is a *contention* knob rather than a memory one, and the answer is four times larger.
The sweep was not wrong; it answered a question about the wrong workload, and I never went back to
ask whether the workload had changed underneath it.

That is worth naming, because it is the day's most repeatable mistake in a new form: a number
measured properly, at an operating point that later stopped being the one that mattered.

### What it bought

| clients | before | after |
|---:|---:|---:|
| 4 | 123 qps | **133-143** |
| 16 | 123 qps | **127-135** |
| 32 | 112 qps | **115-124** |

Tails better, fairness unchanged at ~0.90, and the memory gate holds at **218 MB** with ratios
0.43-0.50 and parity verified. Six megabytes for eight to sixteen per cent.

### And a reminder that a busy machine lies

The first run of this measured the 1M-group fold at 653 ms against 196, and serving at 41 qps — with
**DuckDB collapsing by the same factor**, which is what gave it away. A DuckDB rebuild was running:
load average 34, five `cc1plus` at 100%. Every number in that block was void. Two engines degrading
together is a machine, not a change; had only one moved I might have believed it.

---

## 5.4 — the throughput gap was utilisation, not work — 2026-08-31

The obvious suspects were per-query fixed cost: footers parsed afresh on every query, no plan cache,
no metadata cache. All wrong, and the measurement that settled it took two minutes.

**The fold is 53 ms at one thread.** DuckDB's implied serial cost at 168 qps on eight threads is
~48 ms. The work is comparable; we are not doing more of it. What we were doing was using eight
threads to get **3.3x** — scaling saturates at four and the eighth thread buys nothing.

Under load that compounds. A sharing query was split into exactly `d` coarse groups, so two groups of
250k rows each left a worker idle for however much the slower one ran over by. Eight threads were
about two thirds busy against a work-bound of ~151 qps.

Splitting each admitted share **twice** recovers most of it. Swept, two runs each, 32 clients:

| groups per share | qps | worst p99 | fair |
|---|---:|---:|---:|
| d × 1 | 101, 104 | 432, 451 ms | 0.89, 0.94 |
| **d × 2** | **112, 113** | **446, 402 ms** | **0.95** |
| d × 3 | 89, 91 | 515, 526 ms | 0.81 |

A sharp optimum rather than a trend: one group per worker balances badly, three brings the per-group
fixed cost back. Two is a constant in the code and not a setting, because a library that claims
nothing to configure should not grow a knob the moment a number is inconvenient.

Over-decomposing is only safe **because admission bounds concurrency now**. Before the gate, it was
precisely what let one query take the whole pool and starve the queue. The same change would have
made things worse two items ago.

### Where the serving picture stands

| clients | duck_multi | burrmill |
|---:|---|---|
| 1 | 56 qps | **77 qps** |
| 4 | 109 qps, fair 0.88 | **123 qps**, fair 0.85 |
| 16 | **149 qps**, fair 0.49 | 123 qps, **fair 0.93** |
| 32 | **167 qps**, fair 0.57 | 112 qps, **fair 0.89** |

Burrmill wins outright to four clients, is markedly fairer at every count, and has a comparable tail
(410 ms against 400 at thirty-two). It trails on raw throughput above eight clients.

Single query unchanged: `181 188 189 189 190` ms against a baseline band of 185-199.

### What is left, costed rather than guessed

112 qps against a work-bound of ~151 is about 74% utilisation. DuckDB reaches 168, which is *above*
our work-bound, so it is both roughly 10% cheaper per query and better utilised. Closing that means
work-stealing **across** queries instead of nested rayon pools — a scheduler, not a tuning constant —
and it is filed as 5.5 to be costed before anyone starts it.

---

## 5.3a — a sharing query takes a slice of the pool, not all of it — 2026-08-31

The gate fixed *how many* queries start. It did nothing about how **wide** each one got, so four
admitted queries still fought over eight workers and throughput stayed where 5.2 left it. A query
admitted while others are queued now splits into exactly `pool / in_flight` groups, which caps its
parallel width because rayon cannot run more groups at once than there are.

At 32 clients on the 32-core box, against 5.3:

| | 5.2 | 5.3 (gate) | 5.3a (+degree) |
|---|---:|---:|---:|
| throughput | 96 qps | 89 | **100** |
| worst-client p99 | 2378 ms | 601 | **478** |
| fairness | 0.00, starved | 0.81 | **0.94** |

At four clients Burrmill now **beats** `duck_multi` outright — 108 qps against 107 — and it is fairer
at every count: 0.94 against 0.72 at thirty-two. Liveness, tail and fairness are all now at or better
than DuckDB's.

### The bug in the first version, which the benchmark found and the code could not show

"Alone" was inferred: a degree equal to the pool size meant nobody else was waiting. That comparison
was made against `rayon::current_num_threads()` inside the pool, which is not necessarily the number
the caller divided by — and when the two disagreed, a solo query was treated as *sharing* and capped
to eight groups instead of over-decomposed four-ways-per-thread for balance.

The cost was invisible in the median and plain in the tail: single-query samples went from
`185 188 189 190 191 199` to `187 189 195 198 242 266`. Nothing in the code looked wrong, because
nothing in the code *was* wrong except an assumption about what a library function returns.

The caller now says `None` for "I have the pool to myself" rather than leaving the executor to work
it out. Single query back to `185 191 191 192 192 197` against a baseline band of 185-199.

**A/B against the one changed line is what found it.** Removing `.with_degree(...)` and rebuilding
took two minutes and turned "probably noise, the code path is identical" into a measurement. It was
not noise, and the code path was not identical.

### The standing gap, stated plainly

Burrmill is **100 qps at 32 clients against DuckDB's 168** — about 0.6x — and that is now the honest
remaining weakness. It is no longer a fairness or liveness problem; it is raw throughput under load
and it is undiagnosed. The obvious suspects are per-query fixed cost (footers are parsed afresh on
every query, and there is no plan or metadata cache at all) and a narrow query still paying the full
morsel-scheduling setup. Filed as 5.4 rather than guessed at.

---

## 5.3 — the starvation was a liveness bug, and it is gone — 2026-08-31

5.2 reported clients being "starved". Before building anything I checked whether that was my harness
rather than the engine, because each client makes an untimed warm-up call and one still inside it
when the window closes reports zero queries and reads as starved.

It was not the harness, and the check made the finding much sharper. **At a twenty-second window, one
client completed 220 queries and another completed none** — it spent the entire twenty seconds inside
its first query. That is not a slow queue. A query that never runs is worse than a query that is
refused, because the caller has nothing to act on.

### Why, given there is no lock

Rayon's workers prefer their own local deques and look at the injector — where every queued query
waits — only when those run dry. A query in flight keeps eight workers generating subtasks for each
other, so under continuous load the injector can go unvisited indefinitely. **A bounded pool with no
fairness starves the queue exactly as a mutex does**, and the absence of a mutex says nothing
whatever about it. DuckDB behind its connection mutex fails the same way and for the same reason;
we had simply assumed the mutex was the mechanism rather than one instance of it.

### The gate

A ticket lock with a width, in front of the pool. A query takes a ticket and proceeds when
`ticket < released + width`, so admission is first-come-first-served and at most `width` queries are
inside at once. Nothing clever — the point is that the ordering is **ours** and therefore knowable,
rather than an emergent property of a work-stealing scheduler that was never asked to be fair.

At 32 clients on the 32-core box:

| | before | after |
|---|---:|---:|
| fairness | 0.00, some client served nothing | **0.81, everybody served** |
| worst-client p99 | 2378 ms | **601 ms** |
| throughput | 96 qps | 89 qps |

**Liveness restored and the tail cut fourfold, for eight per cent of throughput.** A panicking query
releases its turn on unwind, tested, because one bad query wedging the serving path would be a worse
bug than the one being fixed.

### What the width is not

Sweeping admission width from 1 to 32 moves throughput only **72 to 98 qps** while the worst tail
goes 428 ms to 3023 ms and fairness collapses to zero. So width is a fairness knob, not a throughput
one, and the default of half the pool sits at the knee. Choosing it by measurement rather than by
taste is the only reason to trust it.

### What remains, stated precisely

Burrmill is **89 qps at 32 clients against `duck_multi`'s 170**, and admission cannot close that: the
whole width sweep tops out at 98. DuckDB stops parallelising a query when there are others waiting;
we split every query across the whole pool regardless of how many are queued behind it. The fix is a
degree on the fold — `coalesce` producing exactly `d` groups with `d = pool / in_flight` — which caps
a query's *width* rather than merely limiting how many start. Filed as 5.3a rather than done, because
it touches the hot path and the gate it would be built on has only just been measured.

---

## 5.2 — the concurrency claim did not survive being measured — 2026-08-31

RFC-0044 §7 calls this "the easiest headline in the project", on the grounds that DuckDB sits behind
one connection mutex and Burrmill takes no global lock. Half of that is right and the conclusion is
wrong.

### What reproduces

DuckDB embedded the way nuthatch embeds it — one `Connection` behind a `Mutex` — behaves exactly as
#986 said. On a 32-core box: **55 qps at one client, 49 at thirty-two**, flat, and at every count
above one some client is served **not at all**. The mutex finding is real.

### What does not

Embedded its own way — one database, a connection per client — DuckDB scales.

| clients | duck_shared | duck_multi | burrmill |
|---:|---:|---:|---:|
| 1 | 55 qps | 56 qps | **80 qps** |
| 4 | 54 | 109 | 99 |
| 16 | 53 | **155** | 103 |
| 32 | 49 | **171** | 96 |

Worst-client p99 at 32 clients: DuckDB **423 ms**, Burrmill **2378 ms**. Fairness: DuckDB serves
every client, Burrmill serves some of them nothing at all.

**Burrmill is the faster engine at one client and the slower one at sixteen.**

### Why, and it is not the lock

There is no lock. There is a **bounded shared pool**, and a pool that hands all eight workers to one
query at a time starves the queue exactly as a mutex does. The thread budget makes it plain — 32
clients, 32-core box:

| threads per query | qps | fairness |
|---:|---:|---:|
| 1 | 15 | 0.67, everybody served |
| 2 | 32 | starved |
| 4 | 55 | starved |
| 8 | 99 | starved |
| 16 | 129 | starved |

One thread per query serves everyone at 15 qps. Eight threads serve some clients nothing at 99. And
the arithmetic between those rows is the finding: **eight independent single-threaded queries would
be about 120 qps by that scaling, where eight-way parallelising one query at a time gives 99.**
Parallelism per query is not free under load, and Burrmill spends it regardless of how many clients
are waiting. DuckDB stops parallelising each query when there are others to run; we do not.

Filed as 5.3: a fair queue in front of the pool, and per-query parallelism that shrinks as load
rises. Note that no constant fixes this — `max_threads` picks between "slow and fair" and "faster and
starving", and neither is a serving profile.

### The measurement lied first, as usual

The first version reported **82 qps at 32 clients with a 14 ms p99**, which cannot both be true: 32
clients sharing one mutex at 12 ms a query is not 82 qps. Pooling every client's latencies weights by
throughput, so a starved client contributes almost no samples and cannot move a percentile however
badly it was treated. The per-client counts said what was happening: one client ran 191 queries and
another ran **none**.

`worstp99` is now the worst client's own p99 and `fair` is the slowest client's share of the
fastest's. That is the seventh measurement fault in this project's short life, and the second where
the naive metric was flattering in *both* directions at once.

There was also a strawman in DuckDB's favour, caught on the first run: `Connection::open_in_memory()`
per client is a fresh **database** per client, so four clients meant four independent DuckDB
instances with eight threads each. It read as DuckDB scaling beautifully at 221 qps against eight
threads' worth of work, which is what gave it away.

### Why this was the right order

5.1 is streaming results and the async cancellation contract, and it would have been built directly
on top of the assumption this just falsified. Measuring the claim before building on it is the whole
of the argument for doing 5.2 first, and it is the second time today that running the experiment
first saved building the wrong thing.

---

## 4.1d — every fold in the workload now plans and runs — 2026-08-31

**Fold sub-plans: 8 of 8.** The last one carries two aggregates over a composite key, which the real
delegation view writes to keep tokens and shares together.

Unlike the composite key, this one genuinely changes the aggregate, so the constraint was that a
single-`SUM` fold must not pay for it. Aggregates past the first live in a side map keyed by
`(arena offset, index)` — the same shape as the `wide` overflow map beside it — so `Entry` keeps its
one inline `i128` and the hot loop is untouched. Widening `Entry` to carry m sums would have put
sixteen bytes per group on every query in order to serve one of them.

The canonical sort has to carry the extras: they are addressed by row position, so sorting the index
alone would leave every row past the first sum pointing at somebody else's number — a wrong balance
with no error at all. Sorting a permutation and applying it is what that costs, and only the
multi-`SUM` case pays.

`HAVING` is refused when there is more than one `SUM`, because "drop the rows that net out" has to
say *which* sum, and discarding a row whose other sum is non-zero would throw away a real answer.

### It was not free twice, and the benchmark said so both times

**A one-element loop is not the same as no loop.** Putting every arm through `for (j, vi) in vis` with
a bounds-checked `b.values[j]` per row cost **15%** — 100 ms to 116. The one-sum, one-column arm now
has its own list, built once per batch.

**`Pending` grew from 32 bytes to 48.** Adding a bare `u16` for the aggregate index next to an `i128`
costs sixteen bytes to alignment, and every worker's scatter buffer with it. It is packed into the
spare high bits of the length instead: keys are addresses, and sixteen bits of length is 65,535 bytes
of one.

After both: 199 MB and 178 ms on the canonical box against 208 MB and 177 ms before — inside the
previous spread, parity verified, both corpora green.

That is three times in this stage that new machinery was not free until it was measured. The rule is
easy to state and apparently hard to obey: **a feature nobody used still has to cost nothing.**

### Where the coverage ratio actually stands

Every fold-shaped sub-plan in a 65-statement real workload now plans and executes. **Statement-level
coverage is still 0 of 65**, because all eight sit inside a `WITH` binding or a join, and that is the
honest number to publish beside the other one. Owning the fold is the point — it is the heavy part —
but §4.6's published ratio does not move until CTEs and joins are admitted, which is 4.1f.

---

## 4.1c — composite keys, and a decision about what *not* to build — 2026-08-31

Fold sub-plans **6 of 8 to 7 of 8**. The two remaining folds looked like one job and are two, which is
the whole content of this item.

- `SELECT curator, position, SUM(sig) ... GROUP BY 1, 2` needs a **composite key**. The aggregate
  never learns about it: the executor builds one byte string and the table hashes bytes.
- `SELECT delegator, sp, SUM(tok), SUM(sh) ... GROUP BY 1, 2` needs **two aggregates**. `Entry`
  carries one `i128` and would need m, which is a different operator and a real regression risk to a
  gate that only just started passing.

So the cheap, safe one is done and the expensive one is filed as 4.1d rather than rushed at the end
of a long day.

A key column is now a bare column, a string literal, `lower`/`upper` of a column, a cast of one to
text, or any `||` concatenation of those. The literal is not decoration: the real curation view tags
its key `'v:' || id` against `'n:' || id`, and without the tag a subgraph's *version* signal and its
*name* signal - different ids in different namespaces - are added together as one position. A test
asserts both halves of that: tagged gives two positions of 100 and 5, untagged gives one of 105.

**Length-prefixed, not delimiter-joined.** `("ab","c")` and `("a","bc")` are different keys, and any
separator byte turns up in data somebody has not shown me yet. Four bytes of length per column beats
a delimiter and a hope.

### Two things the tests caught, both the same mistake

**The prefix condition did not match the un-prefix condition.** The executor prefixed whenever it
left the fast path; `Rows` un-prefixed only when the arity was above one. A `lower(addr)` key is
arity one *and* off the fast path, so the caller got `"*\0\0\00xabcdef..."` with a correct sum
attached. Two halves of one decision must share one condition.

**And the fast path stopped being fast.** Deciding per row whether an arm was simple, by reaching
through a `Vec<Vec<Option<StringArray>>>`, cost **20%** of the fold - 100 ms to 120. The split is now
made once per batch, so an arm with one bare column keeps a `StringArray` in hand and a loop with
nothing in it but the push. Back to 101-103 ms, and 208 MB on the canonical box with parity verified.

New machinery has to be free when it is not used. It was not, and only the benchmark said so.

---

## 4.1b — the subset was not too narrow so much as wrong about SQL — 2026-08-31

Fold sub-plans admitted go **1 of 8 to 6 of 8**. The interesting part is why, because I expected one
cause and there were three, and two of them were Burrmill being wrong rather than strict.

**`lower()` on the group key** (4 folds). The real ERC-20 balance view is
`SELECT lower("to") AS addr, ... UNION ALL SELECT lower("from")`, because an address differing only
in case is the same address and a fold that treats them as two parties reports two half balances with
no error at all. Admitted as a deliberately tiny allowlist — `lower` and `upper`, one bare column —
rather than an expression evaluator, because admitting arbitrary expressions is admitting a language.
The list grows one measured entry at a time.

**An alias demanded where SQL does not name one** (1 fold). A union takes its column names from its
first arm; later arms are positional. `SELECT indexer, -CAST(tokens AS HUGEINT) FROM ...` is perfectly
well formed as a second arm, and Burrmill refused it over a rule SQL does not have.

**Later arms checked against the first** (1 fold). Worse than the last one, because I *added* that
check in 4.1a. A five-table staking fold reads `indexer AS sp` in its first arm and `"serviceProvider"`
in its third — two different columns holding the same thing, which is precisely what a union is for.
Even an explicit alias on a later arm is ignored by every engine, so the check is gone.

Both of the last two were **my rules, not the RFC's**, and both were refusing SQL that every other
engine accepts. "The admitted subset is small" is a design decision; "the admitted subset is wrong
about the language" is a bug, and it is worth keeping the two apart.

`lower()` and the positional-arm shape are tested by **execution**, not by planning: a test asserts
that three spellings of one address fold to one party with the right total, and that without `lower()`
the same data is three parties. The difference is a fact rather than a claim.

No regression: 100-104 ms and 245 MB at a million groups, which is the pre-generalisation baseline to
the millisecond.

### A correction to yesterday's A4 headline

I wrote that the one-table-read-twice shape "occurs **zero** times in the workload it was built for".
That was wrong. My detector counted union arms without checking whether the arms read *different*
tables, so a two-arm fold over one table was filed as multi-table.

Measured properly: of the 8 folds, **4 read one table** — the ERC-20 `Transfer` shape, where one row
carries both a payer and a payee — and 4 read several. The one-table shape was being refused over a
`lower()` call, not over its shape.

The substance survives: the multi-table form was entirely unsupported and is 4 of 8. But "the operator
was built for a shape that does not exist" was too strong, and the tool now reports the split so the
claim cannot drift again. Sixth measurement fault of the day, and the second I published before
catching.

### What is left

Two folds, both the same thing: a **composite group key with several aggregates** —
`SELECT delegator, sp, SUM(tok), SUM(sh) ... GROUP BY 1, 2`. That is a genuinely different operator,
not another relaxation, and it is filed as 4.1c.

---

## 4.1a — the fold is n-branch now, and the shape it was built for was the wrong one — 2026-08-31

A4 found that Burrmill folded *one table read twice* and that no real query does. `SignedFold` now
carries `Vec<FoldBranch>` — table, key column, value column, sign, cast mode — and the old shape is
the degenerate case of the same table listed twice with opposite signs. Nothing was lost, which is
usually the sign that the general form was right all along.

Three changes came with it, each because the workload asked for it rather than because it was tidy:

- **n arms, not two.** A4 found folds with four and five arms, so the old limit was a statement about
  the implementation rather than about the shape.
- **`CAST` as well as `TRY_CAST`, kept distinct.** Every real fold is written with plain `CAST`. The
  old rule refused it to protect a semantic nobody had asked for. They mean different things —
  `TRY_CAST` skips a bad value, `CAST` errors — so the plan carries which was written and the
  executor honours it. Reading one as the other would change an answer silently.
- **`GROUP BY 1`.** What the views are written with, unambiguous over a two-column projection.
  Refusing it was refusing a spelling rather than a shape.

### The performance trap, and it was real

The degenerate shape becomes two arms over one table, and the naive reading is two passes over files
one pass already has in hand. So branches are grouped by table and a table is scanned once however
many arms name it.

That was not enough on its own. The benchmark went **104 ms to 120** anyway, because both arms then
parsed the *same* forty-digit value text independently, once each per row. Parsing per distinct value
column instead of per arm put it back: 99-116 ms, inside run-to-run spread. Peak RSS at a million
groups is 210 MB against 210 before, the ratios are 0.40-0.90, and parity is verified throughout.

Trading measured throughput for coverage is a fair trade; doing it without noticing is not.

### What it actually bought

**Fold sub-plans admitted: 1 of 8, up from 0.** Stated at the sub-plan level because every one of
those folds sits inside a `WITH` binding or a join, so statement-level coverage stays 0/65 until CTEs
and joins are admitted — which is a different item, now filed as 4.1d.

Modest, and the remaining barriers are counted rather than guessed:

| still refused | count |
|---|---:|
| the group key is an expression, not a bare column | 4 |
| the outer projection is wider than key + sum | 2 |
| a computed projection with no explicit alias | 1 |

Computed group keys are the next lever: real views write `'v:' || CAST("subgraphDeploymentID" AS
VARCHAR) AS position`.

### And a self-inflicted one worth recording

Rewriting the executor's row loop, I gave a Python edit a start anchor after its end anchor. Python
slices that to an empty string, `replace("")` matches between every character, and a 534-line file
became **1,084,412 lines**. Restored from HEAD and redone with unique, asserted anchors. *Query: why
did I think an unchecked `s.index()` pair was a safe way to edit a file?*

---

## Experiment A4 — the operator is built for a shape the workload does not contain — 2026-08-31

§4.6's coverage ratio is `owned shapes / n`, and the whole "hybrid now, own more later" argument
turns on whether `n` is a dozen or unbounded. It has been open since the RFC was written. It is
answerable, and the answer was sitting in the authored views of every nest on this machine.

126 view files, 65 statements, every one parsed and handed to **the real planner** rather than to a
model of the admitted subset that could drift from it.

### The count

**32 distinct shapes. 22 plan families.** Two numbers because the question means two things, and
publishing only the flattering one would have been the day's fifth measurement fault. A shape is the
exact feature set; a family collapses the aggregate mix, because a grouped aggregate carrying k
accumulators is one operator parameterised by k rather than k operators.

Neither is a dozen. The top five families cover 48% and the tail is long. But the 22 families are
compositions of just **nine primitives** — cte, set-op, join, subquery, window, distinct, group-by,
having, agg — which argues for owning operators that compose rather than enumerating plan patterns.
That is what §4.3 already says; what A4 adds is that the planner cannot be a lookup table of shapes.

### The finding that matters more than the count

**Coverage ratio: 0 of 65.**

Burrmill's admitted shape is "one table read twice, one column crediting and one debiting the same
signed value". Every single `UNION ALL` in the entire workload reads **different** tables:

```sql
SELECT dep, SUM(tok) FROM (
  SELECT "subgraphDeploymentID" AS dep,  CAST(tokens AS HUGEINT) AS tok FROM curation__signalled
  UNION ALL
  SELECT "subgraphDeploymentID" AS dep, -CAST(tokens AS HUGEINT) AS tok FROM curation__burned
) GROUP BY 1
```

Which is obvious in hindsight and was not obvious in advance: **a credit and a debit are different
events, so they are different tables.** A single table carrying both a payer and a payee column is
the ERC-20 `Transfer` shape, and the authored views do not fold one.

**8 of 65 statements are n-table signed folds** — five with two branches, two with four, one with
five. The one-table case Burrmill owns occurs **zero** times. The benchmark query came from the #987
spike rather than from the workload, and nobody checked.

Generalising `SignedFold` from one table to n branches takes coverage from 0% to about 12% and throws
nothing away: the current shape is the degenerate case of the general one, and the machinery for two
pipelines into one aggregate was built last item for the seam.

After that, **projection width is the next lever**: 41 of 65 refusals are "projects exactly the key
and the sum; got N items", with N from 3 to 13.

### The measurement nearly lied, again

The first version of the fold detector reported **0 of 65** n-table folds. It looked only at the
top-level query body, and the fold above lives inside a `WITH` binding. Publishing 0 would have been
wrong and rather damaging.

It was caught by having read one of the files first and disbelieving the tool when it disagreed. That
is the fifth measurement fault in a day and the first one caught *before* anything was published,
which is at least the right direction. **A measurement that finds nothing is the one to distrust
hardest.**

---

## Stage 3 — the seam, and the three bugs COR-1 found — 2026-08-31

The highest-risk invariant in the design, M/Critical in the risks table. It holds, and getting there
cost three real bugs — two of them mine, one nuthatch's.

### The RFC's summary of the seam was not what the code does

§3.4 describes "a redb hot tip ∪ sealed cold Parquet, modelled as a disjoint `UNION ALL` over a
single monotonic boundary". Reading nuthatch's `store.rs` and `seal.rs` instead of the precis:

- The redb store is `entities` / `meta` / `blocks` / `outbox`, keyed strings holding JSON. Its own
  header says it is "the tip layer for **entity point-reads**".
- Rows leave hot when their block range is final. All tables seal together per range, so
  **`sealed_through` is one global watermark**, not per-table.
- `prune_and_set_meta` advances the watermark and prunes hot **in one transaction**.
- Cold is append-only and never sees a reorg; reorgs only touch hot.

That last pair is the whole invariant, and it settles a question Chief raised in passing: **the
choice of hot store is not load-bearing.** COR-1 rests on the store offering snapshot isolation,
which redb does and Turso would too. RFC-0044 §9 already lists "No Turso/redb replacement" as a
non-goal, and the mechanism agrees with the non-goal.

### The ordering, which is the whole thing

Pin `S`. Cold is `block <= S`, hot is `block > S`, and both must come from a view where those are
true at the same instant. The dangerous order is: read the watermark, then read hot — a seal landing
in between moves a range into a segment nobody listed and out of the hot rows that were read. The
range is in **neither half**, and a fold that drops rows returns a short balance, which looks exactly
like a balance.

So `HotTip::snapshot` hands back the watermark and the rows **together**. A two-call interface is the
bug's natural shape, and the trait is built so it cannot be written rather than so it can be
documented.

### Bug one: the ordering constraint spanned the catalog, and the API let a caller break it

`query()` took the hot snapshot but used the catalog the caller had already built — so the cold
listing was from whenever the handle was opened. COR-1 caught it on run 0.

A caller cannot be expected to open its handle at the right instant; it has no way to know when that
is. `SealedSegments` now remembers where it was discovered and the seam re-lists it **after** pinning
the snapshot. Sealing is append-only, so a later listing is a superset of what the watermark
promises.

### Bug two: `sealed_through: u64` had a sentinel that could not mean both things

Zero has to mean "nothing is sealed" and "block zero is sealed", and it cannot. The test found it on
a genesis-block row within five runs. It is `Option<u64>` now — a sentinel in a boundary is exactly
where seam bugs live, so the ambiguity is removed rather than special-cased.

### Bug three, and it is nuthatch's: segments are installed non-atomically

`seal.rs:176` is `std::fs::write(seg_dir.join(&file), &bytes)`. That creates the file and then writes
it, so a reader globbing `segments/` sees a zero-length Parquet. The test reproduced it faithfully
and failed with *"Parquet file too small. Size is 0 but need 8"*. The manifest ninety lines further
down the same file **is** installed tmp-then-rename, with a comment explaining precisely why.

Burrmill cannot work around this. Given a segment it cannot parse it can skip it — correct if the
file was mid-write, and a silently short balance if it was corrupt — or refuse, which is correct for
corruption and fails every query issued during a seal. It cannot tell them apart, so it refuses and
says so, naming the file and pointing at the write. The fix belongs at the writer, where the
distinction is free. Drafted at `docs/upstream/nuthatch-segment-install-not-atomic.md`, not sent.

### The test

A conserved quantity: every row credits one party and debits another by the same amount, so the
per-party answer is fixed by the data and does not depend on where the boundary sits. Both failure
arms show up in that one number — a double-count inflates two balances, a drop deflates two — and
**neither would raise an error on its own.**

**167 to 250 folds per run, every one of them overlapping an active sealer**, stable across four
runs. The test counts the overlap and fails if it is small, because a COR-1 test that folds a settled
nest passes just as happily with the invariant broken.

### What is deliberately not built

**No redb dependency.** The hot rows live there as JSON entities in a schema that is nuthatch's
business, and 1.4 is the standing lesson about guessing at nuthatch's layout. What is owned here is
the invariant; a redb-backed `HotTip` is a thin adapter and belongs where that encoding is known.

---

## The four open decisions, made — and slice 1's gate passes — 2026-08-31

Chief handed over the four decisions with the instruction to call them for performance and safety.
They are not independent, and that is what decides them: bounding parallelism is what buys the
memory to make the refusal order-independent.

### 1.2c — the gate applies at eight threads per query, and the operator enforces it

`Limits::max_threads`, default 8, with the handle owning a pool that size. RFC-0044's concurrency
argument is about thirty-two *clients*, not thirty-two threads for one of them; #986 measured DuckDB
going 40.3 to 39.6 qps between one client and thirty-two while p99 went 29.5 ms to 7066 ms, and a
fold that hands every core to a single query has reinvented that by another route.

The cores past eight are not buying anything either. At 1M groups on a 32-core box: 601 ms at 1
thread, 223 at 4, **171 at 8**, 156 at 16, 144 at 32. Eight is within 6% of the whole machine and
leaves twenty-four cores for other queries.

**And it is what makes `mem_pool_bytes` mean something.** The same binary measured 145 MB on one
thread and 340 on thirty-two. A budget that depends on the host's core count is not a budget.

### 2.1a — refuse, do not guess

Three options and only one is safe. Matching DuckDB means adopting `7.9 → 8` — silent rounding, into
an engine whose first claim is exactness — and implementing a numeric grammar by guess at every other
edge. Continuing to skip means a row DuckDB counts and we drop, which is a short balance that looks
entirely plausible.

So: a value that carries digits but is not a canonical integer is **refused, naming the value**.
`TRY_CAST`'s NULL still applies to data that is genuinely absent or non-numeric, which is what the
query asked for; it is text carrying a number that this will not interpret, because interpreting it
is guessing. Diverging from DuckDB out loud costs a query. Diverging silently costs someone's answer.

### 2.1b — yes, and it turned out to be free

Refusal now depends on the **answer**, not on whether some partial sum left the range. `MAX, +1, -1`
sums to exactly `MAX` and is returned; `MAX, +1` is refused whatever order it arrives in. A query
that succeeds on Tuesday and fails on Wednesday with the same data is not something a serving engine
can offer.

The feared cost was 16 bytes per group — ~28 MB at a million groups against a budget already being
missed. It costs **nothing**, because the high word lives in a side map keyed by arena offset and an
entry only acquires one once its running total actually leaves `i128`, which on real data is never.
`Entry` did not grow. Peak RSS at 1M groups: 210 MB before, 210 MB after.

The first implementation was wrong in an instructive way: promoting an entry to wide set its high
word to zero rather than sign-extending it, so a negative running sum was silently reinterpreted as
`2^128 - n`. The generated corpus caught it within a second of the test running. That is the entire
argument for having built it.

### 2.3a — drafted, not sent

Reporting the DuckDB wrap goes out under Chief's name, so it waits for him.
`docs/upstream/duckdb-hugeint-parallel-wrap.md` has the reproduction and the version.

### The harness was unfair again, in the same shape as this morning

The moment the thread bound landed, two configurations went from about 0.5x DuckDB to **1.3x**. It
read exactly like a regression. It was the harness: eight threads compared against thirty-two, the
difference called an engine — the same fault as the 38,429-file glob, three items earlier the same
day. The harness now sets `SET threads TO n` on DuckDB and `target_partitions` on DataFusion, and
`THREADS=n` moves all three together.

That is the fourth time today that the thing being measured was the harness. The pattern is worth
naming: **every one of them was invisible to the parity guard**, because in every case the engines
agreed on the answer and disagreed about what question they had been asked.

### The gate, restated

| leg | result |
|---|---|
| latency | **0.38-0.87x DuckDB** across 14 configurations, parity verified on all |
| memory | **210 MB** at 989,690 groups, gate 256, at the default 8-thread budget |

macOS measures 240-246 MB where Linux measures 210, because its allocator returns less. Both pass;
the difference is worth knowing rather than averaging away.

Also worth recording, since the fixture is finally realistic: DataFusion at an equal thread budget is
**3.6x DuckDB at ten thousand segments** — the many-small-files layout a nest actually produces — and
*faster* than DuckDB at a million groups. It is not uniformly the slow one, and saying so is the
difference between an argument and a slogan.

---

## Stage 2.3 — the corpus, and the DuckDB bug it found on its first run — 2026-08-31

`crates/burrmill/tests/slt/` holds hand-computed expectations over tables small enough to check on
paper. They run against Burrmill on every `cargo test`, at three segment layouts, and against DuckDB
through `burrmill-bench slt`. Both green, and mutation-checked: a wrong expected value and a wrong
expected error both fail it.

Expectations are hand-computed rather than recorded from the engine. A corpus whose answers came out
of the thing under test is a regression test — worth having, but it cannot tell you the engine was
ever right.

### Why the standard format, and the vindication

The argument for `sqllogictest` over something bespoke was that DuckDB leaves the graph in Q4, and
every oracle that *is* DuckDB leaves with it, whereas a `.slt` file can be pointed at either engine
and keeps working afterwards. That argument paid on the first run, in a way I did not expect.

**DuckDB silently wraps a `HUGEINT` sum.**

Two rows credit one party with `i128::MAX` and then `1`. The true sum is `MAX + 1`, which no 128-bit
integer holds. DuckDB returns **`i128::MIN`**:

| threads | files | result |
|---:|---:|---|
| 1 | 1, 2, 3 | refuses correctly |
| 2 | 1 | refuses correctly |
| **2, 4** | **2, 3** | **`0xbb = i128::MIN`, wrapped, silently** |

It only goes wrong once the aggregation genuinely runs in parallel, which needs both more than one
thread and more than one file. So the check is in the single-threaded path and missing from the
partial-aggregate combine — and it therefore only misbehaves when the data is big enough to matter.
Measured on libduckdb-sys 1.10501.0, reproducible with `burrmill-bench duckdb-gaps`.

The README hedged that DuckDB "is not watertight everywhere". It now says exactly where, because a
hedge with a reproduction attached is an argument and a hedge without one is a hope. This is the
project's own thesis handed to it by the incumbent: a wrong number that looks exactly like a balance.

The corpus keeps asserting the correct behaviour and carries `skipif duckdb` on that one statement,
with the reason written at the line rather than in a commit message. If `duckdb-gaps` ever prints
"refused" everywhere, the skip comes out.

Filed as 2.3a: reporting it upstream is outward-facing, so it is Chief's call rather than mine.

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
