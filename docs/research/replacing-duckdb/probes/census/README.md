# SQL census scripts (investigation 01)

These are the scripts behind the census in `../../01-duckdb-surface.md`, with their 2026-09-16
outputs.

- `unique_authored.txt`: the 97 unique authored `.sql` files, relative to `~/Projects`.
- `census.py`: feature counts. Output: `census_authored.txt`.
- `refs.py`: CAST targets, table references and per-nest statistics. Output: `refs_authored.txt`.

To re-run:

    python3 census.py unique_authored.txt --per-file > census_authored.txt
    python3 refs.py unique_authored.txt > refs_authored.txt

Set `CENSUS_ROOT` if the nests do not live under `~/Projects`.
