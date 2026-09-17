# Build footprint harness (investigation 05)

These were run on the thinkpad (Debian 13, 32 cores, rustc 1.98.1) under
`~/scratch/burrmill-footprint`, on 2026-09-16.

- `run.sh <variant>`: the harness. It writes `results/<variant>.txt` as `key=value` lines.
- `results/`: per-variant figures (`?.txt`), the combined logs (`?.log`), and the musl and wasm
  failure logs. The per-step clean and incremental build logs were not copied.
- `variants/<V>/`: each consumer's `Cargo.toml`, `src/`, and one of its 20 identical-shaped
  integration tests (`t01.rs`). Variant D's path dependency points at the rsynced burrmill copy on
  the thinkpad.
- `harness.log`, `smoke.log`: the agent's run notes.

The variants are A (bundled DuckDB), B (DataFusion 55, default features), C (DataFusion 55,
`parquet` and `sql` only), E (DataFusion component crates, no planner, not runnable), and D
(burrmill as it was on 2026-09-16, before `sqllogictest` left its normal dependencies).
