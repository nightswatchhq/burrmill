# RFC-0045: One engine, smaller: Burrmill's footprint and its production debts after 4.1.0

- Status: Draft, 2026-10-02. Written the day the migration finished; nothing in it is started.
- Depends on: RFC-0044 Amendment 2 (the swap, done in nuthatch 4.1.0); burrmill#6 (arrow default
  features), burrmill#7 (build time); nuthatch#1428 (one test binary), nuthatch#1649 (the glibc
  floor); the cold audit's burrmill#9 to #24.
- Numbering: Burrmill's RFCs are numbered in their own series from here. RFC-0044 took nuthatch's
  next number when it was written inside that tree; nuthatch's RFC-0045 is offchain data and is
  unrelated to this one.

## §0 What this RFC is for

RFC-0044 Amendment 2 accepted, on 2026-09-26, a larger binary as the price of taking DuckDB's C++
out of nuthatch. The price turned out to be 86.5 MB on the Linux artifact, 108.5 to 195.0 MB, and
a week of production added four debts the bench harness had not shown: about 2.5x DuckDB's time a
statement through `/sql`, 2 GB sessions where DuckDB had 256 MB, a one-day statement that reads
every segment, and a planning cost on deep views that took a CI job from 13 minutes to 45. The
operator's log and the post *One engine* have the day-by-day.

This RFC turns the list of things owed into slices with gates that can fail, footprint first because
that is the one that was accepted sight unseen. It does not reopen the engine choice. Every number
below was measured on 2026-10-02 on the published artifacts or on a symbolled host build of nuthatch
at `79d3180` with Burrmill at `17e0a22`; the commands are in §7 so the tables can be reproduced
rather than believed.

## §1 Where the 195 MB is

### 1.1 The artifact, section by section

`objdump -h` on the published `x86_64-unknown-linux-gnu` binaries, both stripped at build:

| section | 4.0.2 | 4.1.0 | change |
|---|---:|---:|---:|
| `.text` | 82.5 MB | 147.1 MB | +64.6 |
| `.eh_frame` + `.eh_frame_hdr` | 8.5 | 19.8 | +11.3 |
| `.rela.dyn` | 4.0 | 8.0 | +4.0 |
| `.rodata` | 6.2 | 7.6 | +1.4 |
| `.gcc_except_table` | 3.8 | 7.0 | +3.2 |
| `.data.rel.ro` | 2.1 | 5.3 | +3.2 |
| file | 108.5 | 195.0 | +86.5 |

Three quarters of the growth is code. 4.0.2's `.text` held DuckDB's C++ as well as the Rust; 4.1.0's
holds Rust only, and is 65 MB larger. The unwinding tables (`.eh_frame`, `.gcc_except_table`) are
27 MB of the file, up from 12.

### 1.2 By crate

A release build of nuthatch on this machine (aarch64 macOS, the shipped profile: `lto = "thin"`,
default codegen units, symbols kept), every function listed with `cargo bloat --message-format json`
and attributed to the first crate its symbol names that is not `core`, `alloc` or `std`. That rule
puts `drop_in_place<sqlparser::ast::Statement>` on sqlparser and `<Vec<arrow::X> as Clone>` on
arrow, which is where a reader would look for them. cargo-bloat's own crate table is not used: under
thin LTO it attributed 13.7 MiB to sqlparser and its function filter found 1.5, a disagreement that
disqualifies both. The binary: 124.6 MB of text, 213.8 MB file, 268,478 functions.

| crate or stack | text | note |
|---|---:|---|
| sqlparser | 24.6 MiB | one version, 0.62, shared by nuthatch and Burrmill |
| `std`, `core`, `alloc` | 20.6 | generic instantiations named only by std types |
| datafusion (19 crates) | 19.2 | expr 4.5, common 4.5, physical-plan 2.6, functions 1.1, nested 1.1, aggregate 1.1, physical-expr 1.0, the rest under 1 |
| arrow (two versions) | 15.6 | array 8.3, select 1.6, schema 1.2, cast 1.1, buffer 0.9, arith 0.9, ord 0.7 |
| wasmtime, cranelift, wasm tooling | 9.6 | the transform layer; not Burrmill's |
| nuthatch | 4.2 | |
| dbsp | 3.5 | |
| parquet (two versions) | 3.3 | |
| stacker | 2.9 | `recursive_protection`, 10,204 `grow` closures |
| tokio | 1.4 | |
| hashbrown | 1.3 | |
| object_store (two versions) | 1.0 | |
| burrmill | 0.8 | the engine's own code |

Burrmill's own code is under a megabyte. What it costs is what it instantiates.

### 1.3 sqlparser, by what kind of code it is

| kind | text | functions |
|---|---:|---:|
| derived `Hash` | 6.33 MiB | 5,347 |
| `drop_in_place` | 5.74 | 20,318 |
| derived `Visit` | 3.66 | 8,823 |
| derived `PartialEq` | 3.43 | 7,436 |
| derived `VisitMut` | 1.88 | 4,569 |
| derived `Clone` | 1.53 | 1,251 |
| the parser | 0.85 | 530 |
| derived `Debug` | 0.85 | 1,164 |
| `Display`, tokenizer, other | 0.34 | |

The parser is under a megabyte. The other 24 MiB are derived trait impls over the whole of
sqlparser's grammar, `CreateTable`, `AlterUser`, `GrantObjects` and `CreateRole` included, of which
Burrmill admits `SELECT`. `<Statement as Hash>::hash` alone is 863 KiB and is present **45 times**;
`<Statement as PartialEq>::eq` is present 55 times, each copy 11 to 26 KB. Those impls take no type
parameter but the hasher, so the copies are the same code emitted again per codegen unit, and
thin LTO does not fold them.

Counting that effect across the binary: functions with an identical demangled name account for
89 MiB beyond their first copy. That is an upper bound, since one name covers distinct generic
instantiations too, but 22.4 MiB of it is sqlparser, whose impls have nothing to be distinct over.

### 1.4 Twice in the graph

`cargo tree -d` on nuthatch's release graph: arrow 58 (nuthatch, for IPC and the Parquet writer) and
arrow 59 (Burrmill and DataFusion 55); parquet 58 and 59 the same way; object_store 0.12 (nuthatch's
registry, feldera-storage under dbsp, and Burrmill's own pin) and 0.13 (DataFusion). sqlparser is one
version, 0.62, because both sides moved to it. Burrmill's `object_store = "0.12"` is out of step with
the DataFusion it hosts and is the one of these Burrmill can fix alone.

## §2 Footprint: the levers, each measured

Measured in the same host build as §1.2, one change at a time, the manifest restored after each.
A host build is the right instrument for ratios and the wrong one for absolute Linux sizes; the
percentages are what carry over, and S1's gate re-measures on the artifact.

| build | text | file | functions | build time |
|---|---:|---:|---:|---:|
| shipped profile (`lto = "thin"`) | 124.6 MB | 213.8 MB | 268,478 | 224 s |
| A: nuthatch on arrow 59, parquet 59 | 120.6 | 207.2 | 259,921 | |
| B: `lto = "fat"`, `codegen-units = 1` | 91.1 | 145.9 | 137,126 | 778 s |
| C: B, with sqlparser at `opt-level = "z"` | 89.3 | 145.2 | 144,070 | 748 s |

### 2.1 One codegen unit, fat LTO (nuthatch's profile)

The largest lever by a distance: a third off the file, 131,000 functions gone, and sqlparser alone
from 24.6 to 12.5 MiB, its `Statement::hash` copies from 45 to 11. Applied to the artifact, 195 MB
becomes about 133 by proportion. The cost is the release build: 3.5x longer here, and the release
job runs it twice (embedded and scaled) on a shared runner. Nothing else changes: not a line of
code, not a dependency.

What it does to query time is not known and is a gate, not an assumption. Fat LTO usually helps a
hot loop and never hurts it much, but "usually" is not a measurement; the engine-views bench and
`serve-views` run before and after on the same nest and the change is kept only if neither is worse
than 2%.

### 2.2 One arrow (nuthatch's manifest, Burrmill's re-export)

Moving nuthatch to arrow 59 and parquet 59 compiled with no code change and took 4.0 MB of text and
6.5 MB of file. Smaller than the 15.6 MiB attributed to arrow suggests, because nuthatch's own arrow
use is narrow (IPC and the Parquet writer) and DataFusion's reader code is not shared with it either
way. It stays worth doing for a reason beyond size: with one arrow, Burrmill can re-export its
`arrow` and `parquet` so the pin cannot drift again, and the seam nuthatch's `Cargo.toml` describes
as "its arrow is not ours, so no batch crosses; rows cross as JSON" can carry batches instead of
JSON. That is a speed change on every `/sql` answer and on `load_hot`, and it is measured under
§3.2, not assumed here.

object_store: Burrmill moves its own pin to 0.13 to match DataFusion. nuthatch and dbsp stay on
0.12 until dbsp moves, so the graph goes from three object_stores to two, not one.

### 2.3 sqlparser: stop paying for the grammar we refuse

Three parts, in order of certainty.

**Size-optimise it.** sqlparser runs on the order of a few hundred statements a day on a nest;
nothing in it is hot. `[profile.release.package.sqlparser] opt-level = "z"` in nuthatch's manifest
took a further 1.7 MB of text on top of B at no cost anyone will measure. The same treatment is
likely right for `datafusion-sql` and the `inspect` module's walk, and is measured the same way.

**What keeps `Hash` and `PartialEq` on `Statement` alive, and why it is not ours to cut.** Nothing
in Burrmill or nuthatch hashes or compares a parsed statement: Burrmill's `ShareRepeats` hashes a
`LogicalPlan`, its UDF structs hash a `Signature`, nuthatch's graft types hold no AST. The link map
(§7) says where the copies come from: the codegen units of `datafusion-expr`, `datafusion-optimizer`,
`datafusion-functions` and burrmill, six per unit, one per hasher type each of them uses. They are
instantiated by hashing DataFusion's own `Expr`. `Expr::Wildcard` carries a `WildcardOptions`, which
derives `Hash` and `PartialEq` and holds sqlparser's `IlikeSelectItem`, `ExcludeSelectItem`,
`ExceptSelectItem`, `RenameSelectItem` and `ReplaceSelectElement`; the last holds a sqlparser `Expr`,
which reaches `Query` through `Expr::Subquery` and `Statement` through `SetExpr::Insert`. So every
`HashSet<Expr>` in the optimiser and every `==` on an expression carries the derives for the whole
grammar, and that is 9.8 MiB of a 4.1.0 binary for a `SELECT * EXCLUDE (...)` nobody has written.

DataFusion already has shim types for exactly these five, used when `datafusion-expr`'s `sql` feature
is off, but `datafusion-sql` requires the feature on, so the shims are unreachable for anyone who
plans SQL. The fix is upstream and small: make `WildcardOptions` use the shims unconditionally and
convert at the sqlparser boundary in `datafusion-sql`, which is where the conversion already happens
in the other direction. S2 files that patch; until it lands, the local answer is §2.1's fat LTO
(45 copies to 11) and `opt-level = "z"` on sqlparser (each copy smaller), and nothing in Burrmill's
tree pretends to fix it.

**Upstream.** sqlparser derives `Hash`, `PartialEq`, `Visit` and `VisitMut` on every node for every
consumer; a consumer that parses `SELECT` carries `AlterUser`. A feature that gated the derives the
way `serde` and `visitor` already are would be a small upstream change with a large effect for every
embedder. It is worth a conversation and a patch; it is not on this RFC's critical path, and the
gate for S2 does not depend on it.

### 2.4 Burrmill's own features

- **arrow default features off** (burrmill#6): `arrow-json`, `arrow-csv` and `arrow-ipc` are in the
  graph twice and Burrmill uses none of them. The owned fold path needs `arrow`, `parquet` and the
  compute kernels and no more.
- **`datafusion-functions` audit.** Burrmill enables `datetime`, `encoding`, `math`, `regex`,
  `string` and `unicode` expressions. The dialect corpus (273 statements) and the six nests' views
  and entity SQL are the whole demand; each feature is turned off in turn and the corpus and the
  views re-run. A feature nothing reaches goes. `print_long_array` is in the binary 881 times at
  815 KiB, which is `Debug` formatting of arrays that nothing on a serving path should call; the
  caller is found the same way as §2.3's.
- **`recursive_protection`** stays. stacker is 2.9 MiB and 10,204 `grow` closures, and it is the
  guard that burrmill#11 (the planner's stack overflow aborting the host) will depend on being
  present, not absent.

### 2.5 Declined, with the reason

- **`panic = "abort"`.** The unwinding tables are 27 MB of the artifact and this would remove most
  of them. It is not available: dbsp catches unwinds in `dbsp_handle.rs` and `z1.rs` to turn a
  worker's panic into an error, nuthatch's `folds.rs` does the same, and Burrmill's own promise that
  a panic is confined to the query rests on it. Stated so nobody re-measures it.
- **`opt-level = "s"` or `"z"` for the whole tree.** The engine's kernels are the hot path; this is
  the one knob that trades the speed we are already behind on.
- **A static musl binary, UPX, `-Z` flags.** Not on stable, not reproducible, or not a footprint
  change at all.
- **wasmtime and cranelift**, 9.6 MiB, and actix-web under feldera-types: real, and not Burrmill's.
  The second is an upstream ask to Feldera; the first is the transform layer's.

### 2.6 The gate that keeps it won

nuthatch's CI already fails the build on RAM; it does not look at the binary. S1 adds the release
artifact's text and file size to the footprint job as a ratchet: a PR that grows either by more
than 2% fails unless the PR body names the growth. Burrmill's half is `probes/footprint6` run on a
nuthatch-shaped consumer per PR, which is what burrmill#7 asked for and never got. Build time is
reported beside size in both, because §2.1 trades one for the other and the trade should stay
visible.

## §3 The production debts

Each of these was measured on a real nest between 2026-10-01 and 02 and is in the progress log.
This section says what is known about the cause and what the gate is; it does not pretend to know
the fix where the cause is not yet established.

### 3.1 A one-day statement reads every segment

Lodestar's eight statements over one day of QoS data take 3.2 to 4.1 s against DuckDB's 0.6 to
0.7, because the date range arrives through a join and Burrmill reads every segment where DuckDB
reads the day's.

What is known. DataFusion 55 has `enable_join_dynamic_filter_pushdown` on by default, Burrmill's
catalogue sets `pushdown_filters = true` on its `ParquetSource` and implements
`supports_filters_pushdown`, and the session is built `with_collect_statistics(false)`. Statistics
off is why DataFusion could not choose a build side (the `BuildOnSmaller` rule exists because of
it) and is the first suspect here too: a dynamic filter that reaches the scan can prune row groups
from footer statistics, but it cannot prune files whose statistics were never collected, and the
seal manifest already knows each segment's block range.

S0 for this slice is a measurement, not a design: `EXPLAIN ANALYZE` on one of the eight statements,
reading `files_pruned`, `row_groups_pruned_statistics` and `bytes_scanned` on the scan, and whether
the dynamic filter appears on it at all. Then one of two fixes: statistics from the seal manifest
(the block and timestamp range per segment, which costs no I/O), or an owned rule, `DayRange`, that
evaluates a provably one-row join side first, which `onerow.rs` already recognises, and plants the
range as literals on the scan before planning. The second is deterministic and visible in `EXPLAIN`,
in the house style.

Gate: the eight statements under 1.05 s each (1.5x DuckDB) on the QoS copy, `bytes_scanned` within
2x of the day's segments, parity unchanged.

### 3.2 Memory per session, and the permits

The allocations nest refuses 12 or 13 of its 103 dashboard statements at 1 GB and 18 at 768 MB, so
it runs at two sessions of 2 GB and eight threads where DuckDB ran four of 256 MB, and at four
clients turned away 19 of 360 requests as busy where DuckDB turned away none. Separately, glibc's
retention of freed pages took the QoS nest to 7 GB resident; nuthatch 4.1.0 answers that with
jemalloc on Linux and it is not in this RFC.

What is known. Each session owns a pool of its budget less an eighth for the footer cache, with
`sort_spill_reservation_bytes` scaled down to fit; without a spill directory, over the bound is a
refusal. DataFusion's sorts and aggregates spill; its hash join build side does not in 55. Four
sessions of 256 MB refuse because one statement's join needs more than 256 MB; two of 2 GB admit
it and turn the third client away.

Proposal: one pool per nest, not per session. `RuntimeEnv` is shareable across `SessionContext`s;
a `FairSpillPool` of the cursor's analytics budget serves N permits, so a single large join may
take 1.5 GB while three small statements run beside it, and the budget is the budget. Spill on for
the operators that can; for the join, either DataFusion 56's spilling build side, if it ships one,
or an owned partitioned hash join, which the RFC-0044 §4 fold operator already half is. Measured
first: the dashboard's 103 statements with their peak reservations recorded, so the permit count is
derived from the data rather than guessed.

Gate: all 103 admitted at a 2 GB total, zero busy at four clients, p99 no worse than 2x DuckDB's
128 to 184 ms, memory wall held.

### 3.3 Planning cost on deep views

The network endpoint's queries plan through about thirty views into 5,000 to 6,000 logical nodes;
a contract suite that took 32 s on DuckDB takes 116 s in a release build and the `graph` CI job went
from 13 minutes to 45. gdb put the time in physical planning (`equivalence::properties`,
`Statistics`, `ChildStats`); DataFusion 56 fixes a quadratic walk in `EnsureRequirements`.

Two things, in order. DataFusion 56, measured on the suite, which may be most of it. Then a plan
cache: a nest's views are fixed for the life of a catalogue, so the optimised logical plan of each
view keyed by (view text, catalogue identity, `burrmill::ENGINE`) is computed once per session and
reused, where today every query that touches a view re-plans it. The key re-keys on the same hash
grafting already uses, so a stale plan is not possible by construction.

Gate: the contract suite under 40 s release; the `graph` job under 20 minutes; the router tests'
ten-times allowance under `cfg!(test)` removed again.

### 3.4 Small statements

An eight-row view runs eighteen DataFusion operators and `lodestar_disputes` is 0.80x DuckDB after
the small-input rule, the worst ratio on the nest. `smallinputs.rs` already removes repartitions
under 4 MiB of scan; the next step is to measure where the remaining time goes on that view (plan or
execute) before touching anything. No gate until the measurement says what to gate.

### 3.5 The audit's host-killers

The cold audit of 2026-10-02 filed burrmill#9 to #24. Two of them end the process rather than the
query, which breaks the one promise RFC-0044 §1 made that DuckDB could not: #11, `plan_query`
overflowing the stack on a long chain of binary operators and aborting the host, and #10, hidden
`__raw`, `__hot` and `__union` registrations addressable by a public statement and bypassing the
historical window. They are not footprint and they go first in S2 regardless, because an engine
that can be made to abort its host by a `SELECT` has no business being smaller.

## §4 Slices and gates

Each slice ends with the engine-views bench at 22 of 22 identical, dialect-parity at 273 of 273, and
`serve-views` p99 not worse than before it. A slice whose gate fails stops and files what it found;
nothing in a later slice assumes an earlier one passed.

| slice | where | what | gate |
|---|---|---|---|
| S0 | both | the measurements in §1 and §2 (done), the `EXPLAIN ANALYZE` of §3.1, the `-why_live` chain of §2.3 | numbers in the progress log |
| S1 | nuthatch | arrow 59, parquet 59; `lto = "fat"`, `codegen-units = 1`; sqlparser at `opt-level = "z"`; the size ratchet in CI | artifact at or under 140 MB; engine-views and serve-views within 2% of 4.1.0; release job within its runner's time |
| S2 | burrmill | #11 and #10; arrow features off (#6); `object_store` 0.13; the `WildcardOptions` patch filed upstream; `datafusion-functions` audit; re-export arrow | consumer release binary down a further 10 MB on `probes/footprint6`; corpus and views unchanged |
| S3 | burrmill | day pruning (§3.1) | eight QoS statements under 1.05 s, `bytes_scanned` within 2x of the day |
| S4 | burrmill, nuthatch | the shared pool and derived permits (§3.2); batches across the seam instead of JSON | 103 of 103 admitted at 2 GB, zero busy at four clients, p99 under 2x DuckDB |
| S5 | burrmill | DataFusion 56; the view plan cache (§3.3) | contract suite under 40 s, `graph` job under 20 minutes |

S1 is a week of nuthatch work and a release. S2 is the same order in Burrmill and moves the pin.
S3 to S5 are each a sprint with a measurement at the start that may shorten them, and S3 is the one
Lodestar feels.

## §5 Non-goals

Replacing DataFusion, a second engine, publishing to crates.io, `panic = "abort"`, a size-optimised
hot path, and any change to what the engine answers. A statement that is byte-identical to DuckDB
today is byte-identical after every slice, or the slice does not land.

## §6 Open questions for Chief

1. §2.1 trades a 13-minute release build (on this laptop; the runner will be slower) for a third of
   the binary. The release job already runs the build twice. Is that trade acceptable as the
   default, or does the fat profile apply to tagged releases only, with `thin` kept for CI's
   per-commit builds?
2. Re-exporting `arrow` and `parquet` from Burrmill (§2.2) makes their major version part of
   Burrmill's API, which is what stops the drift and is also a commitment. Yes or no.
3. The 2% size ratchet in §2.6: too tight, too loose, or right.

## §7 How the numbers were taken

```sh
# §1.1: the published artifacts, both stripped at build
gh release download v4.1.0 --repo nightswatchhq/nuthatch -p 'nuthatch-x86_64-unknown-linux-gnu.tar.gz'
objdump -h nuthatch            # section sizes; .text, .eh_frame, .gcc_except_table, .rela.dyn

# §1.2 to §1.4: a symbolled host build of nuthatch at 79d3180, Burrmill pinned at 17e0a22
CARGO_PROFILE_RELEASE_STRIP=false cargo bloat --release -n 0 --message-format json > bloat.json
cargo tree -d -e normal --locked | grep -E '^(arrow|parquet|object_store|sqlparser) '
# attribution: first crate named in the demangled symbol that is not core/alloc/std (jq over bloat.json)

# §2: one change at a time, manifest restored after each
sed -i -E 's/^arrow = \{ version = "58"/arrow = { version = "59"/; s/^parquet = \{ version = "58"/parquet = { version = "59"/' Cargo.toml
CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 CARGO_PROFILE_RELEASE_STRIP=false cargo build --release
printf '\n[profile.release.package.sqlparser]\nopt-level = "z"\n' >> Cargo.toml

# §2.3: which codegen unit emitted each surviving copy. ld64's -why_live is swallowed by rustc on a
# successful link, so a link map is the instrument (-Wl,-Map=... with GNU ld on Linux).
RUSTFLAGS="-C link-arg=-Wl,-map,/tmp/link.map" cargo build --release
grep -E 'sqlparser\.\.ast\.\.Statement\$u20\$as\$u20\$core\.\.hash\.\.Hash\$GT\$4hash' /tmp/link.map | awk '{print $3}' | sort | uniq -c
# then look each [index] up in the map's "# Object files:" table; the object name carries the crate
```
