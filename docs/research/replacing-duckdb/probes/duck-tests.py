import re, sys, pathlib, collections
marks = re.compile(r'duckdb::|DuckEngine|engine_duck::|Connection::open|\bConnection\b|serialize_sql|json_serialize_sql|duckdb_')
out = collections.defaultdict(list)
for f in sorted(list(pathlib.Path('src').rglob('*.rs')) + list(pathlib.Path('tests').rglob('*.rs'))):
    lines = f.read_text().split('\n')
    i = 0
    while i < len(lines):
        if re.match(r'\s*#\[(tokio::)?test', lines[i]):
            j = i
            while j < len(lines) and not re.search(r'\bfn\s+\w+', lines[j]): j += 1
            if j >= len(lines): break
            name = re.search(r'\bfn\s+(\w+)', lines[j]).group(1)
            depth, k, started = 0, j, False
            body = []
            while k < len(lines):
                depth += lines[k].count('{') - lines[k].count('}')
                if '{' in lines[k]: started = True
                body.append(lines[k])
                if started and depth <= 0: break
                k += 1
            hits = sorted(set(m.group(0) for l in body for m in marks.finditer(l)))
            if hits: out[str(f)].append((name, hits))
            i = k + 1
        else:
            i += 1
total = 0
for f, ts in out.items():
    print(f"{f}: {len(ts)}")
    total += len(ts)
    for n, h in ts: print(f"    {n}  [{', '.join(h)}]")
print("TOTAL", total)
