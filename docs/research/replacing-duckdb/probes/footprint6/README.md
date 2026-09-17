# Stage 6.0 footprint spike

Nuthatch-shaped consumer: `[profile.dev] debug = "line-tables-only"`, `CXXFLAGS=-g0`, six
integration test binaries (four call the engine, two do not), release `lto = "thin"` and
`strip = true`.

Variants:

- `duck` — `duckdb 1.10504.0` with `bundled`, `parquet`, `json` (nuthatch's features minus `vscalar`)
- `umbrella` — `datafusion = 55.0.0` default features, `SessionContext`
- `components` — component crates plus a port of `DefaultPhysicalPlanner` (no umbrella crate)

The planner is generated at measure time from the cargo registry by `rewrite_planner.py`.
It is not committed.

Run on the thinkpad:

```
rsync -az --exclude target --exclude .git --exclude '*/target-*' \
  docs/research/replacing-duckdb/probes/footprint6/ thinkpad:~/scratch/footprint6/
ssh thinkpad 'export PATH=$HOME/.cargo/bin:$PATH; cd ~/scratch/footprint6; ./run.sh duck'
```

Measured 2026-09-17. Querying test binary 162 / 493 / 446 MB (duck / umbrella / components);
release 41 / 127 / 105 MB; all three engines 90 parties on the fixture. Report:
[`../../06-footprint-spike.md`](../../06-footprint-spike.md).
