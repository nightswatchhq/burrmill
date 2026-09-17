#!/usr/bin/env python3
"""CAST targets, table references, function-name breakdowns, per-nest stats."""
import re, sys, os, collections
ROOT = os.environ.get("CENSUS_ROOT", os.path.expanduser("~/Projects"))
files = [l.strip() for l in open(sys.argv[1]) if l.strip()]
I = re.I

def strip(s):
    s = re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
    return re.sub(r"--[^\n]*", " ", s)

def nest_of(p):
    parts = p.split("/")
    return parts[0] + "/" + parts[1] if parts[0] in ("nests-mvp", "nuthatch") else parts[0]

texts = {f: strip(open(os.path.join(ROOT, f), encoding="utf-8", errors="replace").read()) for f in files}
alltext = "\n".join(texts.values())

def hist(title, rx, text=alltext, flags=I, key=lambda m: m.group(1).upper()):
    c = collections.Counter(key(m) for m in re.finditer(rx, text, flags))
    print(f"\n### {title}  (regex: `{rx}`)")
    for k, v in c.most_common():
        print(f"- {k}: {v}")

hist("CAST(... AS <type>) targets", r"\bAS\s+((?:HUGEINT|UHUGEINT|BIGINT|UBIGINT|INTEGER|INT|SMALLINT|DOUBLE|FLOAT|REAL|VARCHAR|TEXT|BOOLEAN|BOOL|DATE|TIMESTAMP\w*|DECIMAL\s*\([^)]*\)|NUMERIC\s*\([^)]*\)|BLOB|JSON)(?:\[\])?)\s*\)")
hist(":: cast targets", r"::\s*([A-Za-z_]+(?:\s*\([^)]*\))?)")
hist("json_* / list_* / array_* / struct_* / map_* / regexp_* / string fns by name", r"\b((?:json|list|array|struct|map|regexp|str|string)_\w+)\s*\(")
hist("all function-call names (lowercased, top 80)", r"\b([a-z_][a-z0-9_]*)\s*\(", key=lambda m: m.group(1).lower())
hist("_dec companion column refs", r"\b(\w+_dec)\b", key=lambda m: m.group(1).lower())
hist("block_* / timestamp column refs", r"\b(block_number|block_timestamp|block_time|timestamp|tx_hash|transaction_hash|log_index|tx_index|evt_index|block_hash|contract_address|address|_meta\w*|_labels\w*)\b", key=lambda m: m.group(1).lower())

# table references
defined = set()
for f, t in texts.items():
    for m in re.finditer(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\"?([\w.]+)\"?", t, I):
        defined.add(m.group(1).lower())
cte = set()
for f, t in texts.items():
    for m in re.finditer(r"\b(\w+)\s+AS\s*\(\s*(?:SELECT|WITH|FROM|VALUES)\b", t, I):
        cte.add(m.group(1).lower())
refs = collections.Counter(); refnests = collections.defaultdict(set)
for f, t in texts.items():
    for m in re.finditer(r"\b(?:FROM|JOIN)\s+\"?([A-Za-z_][\w.]*)\"?", t, I):
        name = m.group(1).lower()
        if name in ("select", "values", "unnest", "read_parquet", "generate_series", "range", "lateral"):
            continue
        refs[name] += 1; refnests[name].add(nest_of(f))
print("\n### table references in FROM/JOIN (regex `\\b(?:FROM|JOIN)\\s+\"?([A-Za-z_][\\w.]*)\"?`), classified")
base = {k: v for k, v in refs.items() if k not in defined and k not in cte}
views = {k: v for k, v in refs.items() if k in defined}
ctes = {k: v for k, v in refs.items() if k in cte and k not in defined}
print(f"defined views: {len(defined)}; distinct base-table names referenced: {len(base)}; view refs: {len(views)}; cte refs: {len(ctes)}")
print("\n**Base tables (not a CREATE VIEW name, not a CTE name):**")
for k, v in sorted(base.items(), key=lambda kv: -kv[1]):
    print(f"- `{k}`: {v} refs, nests: {', '.join(sorted(refnests[k]))}")
print("\n**Views referenced by other views/checks:** " + ", ".join(f"{k}({v})" for k, v in sorted(views.items(), key=lambda kv: -kv[1])))
print("\n**Base-table naming shapes:**")
shapes = collections.Counter()
for k in base:
    if "__" in k: shapes["<contract>__<event> (double underscore)"] += 1
    elif k.startswith("_"): shapes["_underscore-prefixed system table"] += 1
    elif "." in k: shapes["dotted (schema.table)"] += 1
    else: shapes["plain name"] += 1
for k, v in shapes.items(): print(f"- {k}: {v}")

# per-nest stats
print("\n### per-nest stats (files, lines, top-level statements = column-0 CREATE/SELECT/WITH/... keywords, CREATE VIEW count)")
st = collections.defaultdict(lambda: [0, 0, 0, 0])
for f in files:
    raw = open(os.path.join(ROOT, f), encoding="utf-8", errors="replace").read()
    n = nest_of(f)
    st[n][0] += 1
    st[n][1] += raw.count("\n") + (0 if raw.endswith("\n") else 1)
    st[n][2] += len(re.findall(r"^(CREATE|SELECT|WITH|PRAGMA|SET|INSERT|COPY|DESCRIBE|SUMMARIZE|FROM)\b", raw, re.M))
    st[n][3] += len(re.findall(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b", strip(raw), I))
print("| nest | files | lines | top-level stmts | CREATE VIEW |")
print("|---|---|---|---|---|")
tot = [0, 0, 0, 0]
for n, v in sorted(st.items()):
    print(f"| {n} | {v[0]} | {v[1]} | {v[2]} | {v[3]} |")
    for i in range(4): tot[i] += v[i]
print(f"| **total** | {tot[0]} | {tot[1]} | {tot[2]} | {tot[3]} |")
