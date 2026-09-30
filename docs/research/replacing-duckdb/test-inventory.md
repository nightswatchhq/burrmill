# Nuthatch's tests after DuckDB

Phase 3b says "~45 DuckDB-oracle tests moved or retired". Measured on 2026-09-29 against nuthatch
`pete/burrmill-watchdog` (the whole unmerged stack: folds, entities, dialect, 38 digits, the sweep,
the watchdog), that line undercounts in one direction and misses the larger item in the other.

## The larger item: every test that goes through `analytics::query`

`analytics::engine()` is `DuckEngine` unless a test overrides it, so the ~1,400 tests that query a
nest through nuthatch's own API run on DuckDB without saying so. About 240 fold tests run on both
(`on_burrmill!` in `folds.rs`) and four more switch by hand. At cutover the default flips and the
rest meet Burrmill for the first time. Measured by flipping it (a scratch-only
`PROBE_BURRMILL_DEFAULT`, `cargo test --release --features shadow-burrmill --no-fail-fast`):

| run | failed |
|---|---:|
| DuckDB default (control) | 4, all known before today |
| Burrmill default, first run | 164 |
| after the host-runtime fix (`ac131ef`) | 20 |
| after `error()`, the unstamped window, `first`/`last`, footers, `column_names`, and the admission tests' wording | 6 |

What each cluster was:

- **140: the engine panicked inside a host's runtime.** `burrmill::Engine` owns a tokio runtime and
  blocked on it, and dropped it, from whatever thread called; from a thread already driving
  nuthatch's own runtime tokio refuses both. The bench had worked round it ten times by hand. Now
  the engine runs on a scoped thread there and shuts its runtime down in the background.
- **7: DuckDB's `first`/`last` aggregates**, which the port emitter writes. Now `first_value`/
  `last_value`; `dialect-parity` 241/241.
- **2: historical queries dropped unstamped rows silently** where DuckDB refuses them, and Burrmill
  had no `error()` to refuse with. Both added; a window over an unstamped row now refuses with
  DuckDB's message.
- **2: a corrupt segment went unnoticed.** Burrmill read one footer per table; DuckDB's bind reads
  them all, which is how nuthatch's sweep learns a table is short. Now every footer, once per engine.
- **2: `column_names` read its names off the first batch**, and a result with no rows has none.
  Now from the plan, as DuckDB's prepare gives them.
- **2: the admission tests named DuckDB's operator** (`DELIM`). Burrmill refuses the same statement
  for its nested loop; both names accepted, the guarantee unchanged.

The 6 left: two fail on DuckDB too (`entity_lower`'s key order, `authoring_eval_board`'s 120 s
wait), and four are the checked rule refusing an aggregate over `value_dec`, a `TRY_CAST` that
dropped a value that did not fit (`SUM` in `bigint_columns_get_decimal_and_overflow_views` and in
the off-chain entity, `MAX`/`MIN` in the eval harness, `wide_values` in the Trino contract). DuckDB
answers from the rows that fit. Which one nuthatch wants after cutover is Chief's decision; the
tests follow it.

**Since, the same day:** Chief decided refuse, and the checked rule now refuses only at a row
whose cast dropped a value (progress log). The off-chain entity and the eval harness pass; the
bigint test and the Trino fixture say "the ones that fit" and pass on both engines. Left: the two
that fail on DuckDB too. The Trino contract's `sender_kinds` was a Burrmill bug, not DuckDB's
(corrected 2026-09-30: a drifted table's columns came from its first segment); fixed, it agrees on
both engines and fails only on the view order it fails on under DuckDB too.

Also found, not chased: a hot JSON row missing a numeric field reads as 0 on Burrmill where
DuckDB's `read_json` gives NULL. And `serve::tests::a_statement_reading_outside_the_nest_is_refused_and_never_remembered`
counts the process-wide memo across an `.await`, so a parallel test that remembers an answer fails
it (5 against 3 once in a full run, 3/3 alone); the refusal it guards held.

## The 69 that touch DuckDB directly

Classified by reading every body and the helpers it calls.

| class | count | what it means at removal |
|---|---:|---|
| ORACLE | 12 | DuckDB is the reference for a port or for Burrmill. Kept while DuckDB is a dev-dependency; at removal, retired or expectations frozen as literals |
| LEAVES | 25 | Tests DuckDB's own machinery. Deleted with it, or a Burrmill equivalent where the behaviour still matters |
| FIXTURE | 11 | DuckDB used as a tool (write or probe Parquet, run a check); the behaviour is nuthatch's. Port to the parquet crate or to Burrmill |
| STANDIN | 16 | `DuckEngine` or a bare `Connection` as an arbitrary `Session`. Re-point at Burrmill |
| FALSE | 5 | "duckdb" only in a name |

Five of the LEAVES exercise `engine_duck::reach` but have engine-agnostic bodies; re-pointed, they
become Burrmill's tests (20 LEAVES, 21 STANDIN). Missed by a name search and to be counted: the
cross-nest `conn()` schema test (LEAVES), `entities::the_port_gates_every_statement_exactly_as_duckdbs_parse_did`
(ORACLE), `e2e_trino_contract::every_translated_view_returns_the_rows_the_nest_view_returns`
(FIXTURE, via `translated_views`), and every spike test that calls `authored_entity_spike::compile`
(DuckDB parses there; the file is deleted in packaging).

**Burrmill equivalents that matter**, the rest being DuckDB's own:

- The `/sql` allowlist's security cases (`read_xlsx`, `st_read`, `iceberg_scan`, `postgres_scan`,
  an invented `read_totally_new_format`, a path or URL in table position) were in no Burrmill
  corpus. Now run on both walks (nuthatch `pete/burrmill-watchdog`, `allowlist_sessions`).
- `collect_separates_a_bind_failure_from_a_read_failure`: Burrmill's `collect` must sort errors into
  `Binding` and `Executing` as DuckDB's does, or the degradation sweep amplifies.
- `a_case_guard_does_not_decode_an_empty_predeployment_word`: a `CASE` must not evaluate its untaken
  UDF branch on Burrmill either.
- `unconfigured_duckdb_still_opens_at_todays_walls`: Burrmill's default memory, threads and spill.
- The FIXTURE tests that prove "footer intact, pages corrupt" with DuckDB's `read_parquet` bind:
  where Burrmill draws the line between binding and reading may differ, so the premise is re-checked,
  not translated.
- `a_query_that_names_no_table_reaches_no_segment` asserts DuckDB's "Conversion Error" wording.

The list is regenerated by `probes/duck-tests.py`, run from the nuthatch tree.

## Appendix: the 69

| file | test | class |
|---|---|---|
| analytics.rs | relation_membership_preserves_existence_with_nulls_and_duplicates | FIXTURE |
| analytics.rs | dependency_discovery_distinguishes_local_ctes_from_entity_views | LEAVES (re-pointable) |
| analytics.rs | dependency_closure_reaches_sources_beyond_eight_views | STANDIN |
| analytics.rs | a_fact_window_exposes_only_its_range_and_names_only_overlapping_segments | STANDIN |
| analytics.rs | a_second_query_reuses_the_duckdb_connection | LEAVES |
| analytics.rs | changing_or_removing_an_authored_view_invalidates_the_duckdb_cache | LEAVES |
| analytics.rs | a_page_corrupt_segment_with_an_intact_footer_reduces_the_table_rather_than_failing_the_query | FIXTURE |
| analytics.rs | a_page_corrupt_segment_names_its_table_in_the_result | FIXTURE |
| analytics.rs | an_undefinable_view_degrades_with_every_segment_intact | STANDIN |
| analytics.rs | collect_separates_a_bind_failure_from_a_read_failure | LEAVES (needs an equivalent) |
| analytics.rs | a_query_that_names_no_table_reaches_no_segment | STANDIN |
| analytics.rs | the_table_refs_walk_reports_what_the_statement_reached | LEAVES (re-pointable) |
| analytics.rs | an_authored_view_resolves_on_a_cold_nest_with_no_rows | STANDIN |
| analytics.rs | a_view_the_statement_cannot_reach_is_not_redefined | STANDIN |
| analytics.rs | without_a_schema_the_view_cannot_resolve_which_is_why_we_regenerate_it | STANDIN |
| analytics.rs | a_view_joining_a_populated_and_a_never_fired_table_resolves_once_the_live_schema_is_supplied | STANDIN |
| analytics.rs | the_real_constructor_chain_reproduces_663_and_the_fix_resolves_it | STANDIN |
| analytics.rs | one_premature_view_does_not_kill_the_others_in_its_file | STANDIN |
| analytics.rs | the_allowlist_refuses_functions_the_denylist_never_heard_of | now on both |
| analytics.rs | a_path_in_table_position_is_not_a_table_name | now on both |
| analytics.rs | ordinary_analytical_sql_still_passes_the_allowlist | now on both |
| analytics.rs | one_file_per_schema_is_kept_in_order | STANDIN |
| analytics.rs | a_schema_only_binding_describes_like_the_whole_union | STANDIN |
| analytics.rs | the_serialized_ast_carries_each_tables_schema | LEAVES |
| analytics.rs | the_sqlparser_port_finds_what_duckdb_found | ORACLE |
| analytics_budget.rs | lowering_the_reservation_to_buy_duckdb_memory_is_refused | FALSE |
| analytics_budget.rs | no_accepted_split_gives_duckdb_more_than_the_measured_default | FALSE |
| analytics_budget.rs | parse_size_for_duckdb_normalises | FALSE |
| analytics_scalars.rs | burrmill_answers_every_function_as_duckdb_does | ORACLE |
| analytics_scalars.rs | a_case_guard_does_not_decode_an_empty_predeployment_word | LEAVES (needs an equivalent) |
| analytics_scalars.rs | sql_scalars_preserve_full_width_and_nulls_and_refuse_bad_inputs | LEAVES (needs an equivalent) |
| authored_entity_spike.rs | four tests against `duckdb_reference` | ORACLE (file deleted) |
| engine_burrmill.rs | duckdb_and_burrmill_agree_on_a_sealed_and_hot_nest | ORACLE |
| engine_burrmill.rs | duckdb_and_burrmill_agree_on_the_sql_nuthatch_generates | ORACLE (freeze outputs) |
| engine_burrmill.rs | shadow_replay_over_a_nest | ORACLE |
| engine_duck.rs | unconfigured_duckdb_still_opens_at_todays_walls | LEAVES (needs an equivalent) |
| engine_shadow.rs | four shadow tests on `DuckEngine` | STANDIN (while shadow mode lasts) |
| entities.rs | the_duckdb_gate_depended_on_its_parser_renaming_aliases | LEAVES |
| entities.rs | the_aggregate_list_is_duckdbs_catalogue | ORACLE (already frozen; retire) |
| entity_offchain.rs | an_offchain_table_resolves_case_insensitively_as_duckdb_does | FALSE |
| graft.rs | table_refs_from_sqlparser_match_the_duckdb_walk | ORACLE |
| graft.rs | refusals_from_sqlparser_match_the_duckdb_walk | ORACLE |
| port_emit.rs | three emitted-check tests | FIXTURE |
| seal.rs | segments_failing_verification_catches_page_corruption_that_still_binds | FIXTURE |
| tests/duckdb_containment.rs | ten tests | LEAVES (shrinks to zero, deleted) |
| tests/duckdb_extensions_are_static.rs | two tests | LEAVES |
| tests/e2e_entity_reorg.rs | an_offchain_entity_equals_duckdb_after_each_appended_snapshot | FALSE |
| tests/e2e_solo.rs | two corrupt-segment tests | FIXTURE |
| tests/e2e_trino_contract.rs | the_fixture_drifts_and_duckdb_reads_it_by_name | FIXTURE |
| tests/network_contract.rs | sparse_lock_fold_matches_the_original_event_by_event | FIXTURE |
