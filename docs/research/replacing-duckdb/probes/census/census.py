#!/usr/bin/env python3
"""SQL feature census over authored nuthatch SQL.

Usage: census.py <listfile> [--per-file]
Reads each path (relative to /Users/pepe/Projects), strips -- and /* */ comments and
string literals are kept (needed for LIKE patterns etc.), then counts regex matches.
"""
import re, sys, os, collections

ROOT = os.environ.get("CENSUS_ROOT", os.path.expanduser("~/Projects"))
files = [l.strip() for l in open(sys.argv[1]) if l.strip()]
per_file = "--per-file" in sys.argv

def nest_of(p):
    parts = p.split("/")
    if parts[0] in ("nests-mvp", "nuthatch"):
        return parts[0] + "/" + parts[1] if parts[1] not in ("tests", "evaluation-2026-08-23", "tools") else parts[0] + "/" + parts[1]
    return parts[0]

def strip_comments(s):
    s = re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
    s = re.sub(r"--[^\n]*", " ", s)
    return s

I = re.I
# name -> regex (applied to comment-stripped text, case-insensitive)
P = collections.OrderedDict([
    # ---- statements / structure
    ("CREATE VIEW", r"\bCREATE\s+(OR\s+REPLACE\s+)?VIEW\b"),
    ("CREATE OR REPLACE VIEW", r"\bCREATE\s+OR\s+REPLACE\s+VIEW\b"),
    ("CREATE TABLE/MACRO/other", r"\bCREATE\s+(TABLE|MACRO|TEMP|TEMPORARY|SEQUENCE|TYPE)\b"),
    ("WITH (CTE)", r"(^|[^\w])WITH\s+(RECURSIVE\s+)?\w+\s+AS\s*\("),
    ("WITH RECURSIVE", r"\bWITH\s+RECURSIVE\b"),
    ("SELECT", r"\bSELECT\b"),
    ("SELECT DISTINCT", r"\bSELECT\s+DISTINCT\b(?!\s+ON)"),
    ("DISTINCT ON", r"\bDISTINCT\s+ON\s*\("),
    ("SELECT * EXCLUDE", r"\bEXCLUDE\s*\("),
    ("SELECT * REPLACE", r"\*\s*REPLACE\s*\("),
    ("SELECT * / t.*", r"\bSELECT\s+(\w+\.)?\*"),
    ("FROM-first syntax (stmt or subquery opens with FROM)", r"(?:\A|;|\(|\bUNION\s+(?:ALL\s+)?)\s*FROM\s+[\"\w]"),
    ("CTE definitions (name AS ()", r"\b(?!SELECT|FROM|WHERE|AND|OR|ON|JOIN)\w+\s+AS\s*\(\s*(SELECT|WITH|FROM|VALUES)\b"),
    ("JOIN (any)", r"\bJOIN\b"),
    ("LEFT JOIN", r"\bLEFT\s+(OUTER\s+)?JOIN\b"),
    ("INNER JOIN", r"\bINNER\s+JOIN\b"),
    ("FULL JOIN", r"\bFULL\s+(OUTER\s+)?JOIN\b"),
    ("RIGHT JOIN", r"\bRIGHT\s+(OUTER\s+)?JOIN\b"),
    ("CROSS JOIN", r"\bCROSS\s+JOIN\b"),
    ("SEMI/ANTI JOIN", r"\b(SEMI|ANTI)\s+JOIN\b"),
    ("ASOF JOIN", r"\bASOF\s+JOIN\b"),
    ("NATURAL JOIN", r"\bNATURAL\s+JOIN\b"),
    ("POSITIONAL JOIN", r"\bPOSITIONAL\s+JOIN\b"),
    ("LATERAL", r"\bLATERAL\b"),
    ("JOIN ... USING (col)", r"\bUSING\s*\("),
    ("comma join (FROM a, b)", r"\bFROM\s+\w+(\s+(AS\s+)?\w+)?\s*,\s*\w+"),
    ("UNION ALL", r"\bUNION\s+ALL\b"),
    ("UNION (distinct)", r"\bUNION\s+(?!ALL|BY)"),
    ("UNION BY NAME", r"\bUNION\s+(ALL\s+)?BY\s+NAME\b"),
    ("INTERSECT/EXCEPT", r"\b(INTERSECT|EXCEPT)\b"),
    ("GROUP BY", r"\bGROUP\s+BY\b"),
    ("GROUP BY ALL", r"\bGROUP\s+BY\s+ALL\b"),
    ("GROUP BY <ordinal>", r"\bGROUP\s+BY\s+\d"),
    ("GROUPING SETS/ROLLUP/CUBE", r"\b(GROUPING\s+SETS|ROLLUP|CUBE)\b"),
    ("HAVING", r"\bHAVING\b"),
    ("ORDER BY", r"\bORDER\s+BY\b"),
    ("ORDER BY ALL", r"\bORDER\s+BY\s+ALL\b"),
    ("ORDER BY <ordinal>", r"\bORDER\s+BY\s+\d"),
    ("NULLS FIRST/LAST", r"\bNULLS\s+(FIRST|LAST)\b"),
    ("LIMIT", r"\bLIMIT\b"),
    ("OFFSET", r"\bOFFSET\b"),
    ("QUALIFY", r"\bQUALIFY\b"),
    ("WINDOW clause (named)", r"\bWINDOW\s+\w+\s+AS\b"),
    ("OVER (", r"\bOVER\s*\("),
    ("PARTITION BY", r"\bPARTITION\s+BY\b"),
    ("ROWS BETWEEN", r"\bROWS\s+BETWEEN\b"),
    ("RANGE BETWEEN", r"\bRANGE\s+BETWEEN\b"),
    ("ROWS/RANGE UNBOUNDED (short form)", r"\b(ROWS|RANGE)\s+(UNBOUNDED|CURRENT)\b"),
    ("UNBOUNDED PRECEDING", r"\bUNBOUNDED\s+PRECEDING\b"),
    ("UNBOUNDED FOLLOWING", r"\bUNBOUNDED\s+FOLLOWING\b"),
    ("CURRENT ROW", r"\bCURRENT\s+ROW\b"),
    ("n PRECEDING/FOLLOWING", r"\b\d+\s+(PRECEDING|FOLLOWING)\b"),
    ("EXCLUDE CURRENT ROW etc (frame)", r"\bEXCLUDE\s+(CURRENT\s+ROW|GROUP|TIES|NO\s+OTHERS)\b"),
    ("FILTER (WHERE", r"\)\s*FILTER\s*\(\s*WHERE\b"),
    ("WITHIN GROUP", r"\bWITHIN\s+GROUP\b"),
    ("CASE", r"\bCASE\b"),
    ("EXISTS", r"\bEXISTS\s*\("),
    ("NOT EXISTS", r"\bNOT\s+EXISTS\s*\("),
    ("IN (subquery)", r"\bIN\s*\(\s*SELECT\b"),
    ("NOT IN (subquery)", r"\bNOT\s+IN\s*\(\s*SELECT\b"),
    ("IN (list)", r"\bIN\s*\(\s*(?!SELECT)[^)]"),
    ("scalar subquery (SELECT in expr)", r"[=<>(,+\-]\s*\(\s*SELECT\b"),
    ("ANY/ALL (quantified)", r"[=<>]\s*(ANY|ALL)\s*\("),
    ("column IN table (DuckDB)", r"\bIN\s+\w+\s*(\)|$|\s+(AND|OR|WHERE|GROUP|ORDER))"),
    ("IS DISTINCT FROM", r"\bIS\s+(NOT\s+)?DISTINCT\s+FROM\b"),
    ("IS NULL / IS NOT NULL", r"\bIS\s+(NOT\s+)?NULL\b"),
    ("BETWEEN", r"(?<!ROWS )(?<!RANGE )\bBETWEEN\b"),
    ("LIKE", r"(?<!I)\bLIKE\b"),
    ("ILIKE", r"\bILIKE\b"),
    ("SIMILAR TO", r"\bSIMILAR\s+TO\b"),
    ("VALUES (", r"\bVALUES\s*\("),
    ("SAMPLE/TABLESAMPLE", r"\b(USING\s+SAMPLE|TABLESAMPLE|SAMPLE\s+\d)\b"),
    ("SUMMARIZE/DESCRIBE/PRAGMA/SET/EXPLAIN", r"^\s*(SUMMARIZE|DESCRIBE|PRAGMA|SET|EXPLAIN|SHOW)\b"),
    ("PIVOT/UNPIVOT", r"\b(UN)?PIVOT\b"),
    ("TRUE/FALSE literal", r"\b(TRUE|FALSE)\b"),
    ("NULL literal", r"(?<!IS )(?<!IS NOT )\bNULL\b"),
    ("INTERVAL literal", r"\bINTERVAL\b"),
    ("DATE literal", r"\bDATE\s+'"),
    ("TIMESTAMP literal", r"\bTIMESTAMP\s+'"),
    ("dollar-quoted string", r"\$\$|\$\w+\$"),
    ("$n / ? params", r"\$\d+|\?\s"),
    ("double-quoted identifier", r'"[^"\n]+"'),
    ("backtick identifier", r"`\w+`"),
    ("struct literal {'a':..}", r"\{\s*'\w+'\s*:"),
    ("list literal [..]", r"(?<![\w\]\)])\[[^\]]*\]"),
    ("lambda x -> ...", r"\b\w+\s*->\s*"),
    ("method chaining .func(", r"\w\.(lower|upper|len|length|trim|abs|round|cast|list_\w+|str\w+)\s*\("),
    ("trailing comma before FROM/)", r",\s*(FROM|\))\b"),
    ("E'..' escape string", r"\bE'"),
    ("main. prefix", r"\bmain\.\w+"),
    ("nest. prefix", r"\bnest\.\w+"),
    ("hex literal 0x", r"'0x[0-9a-fA-F]{6,}'"),
    ("numeric literal >= 1e18 / big int", r"\b\d{19,}\b|\b1e\d+\b|\b10\s*\*\*|\bpow(er)?\s*\(\s*10"),
    ("exponent literal 1e18 style", r"\b\d+e\d+\b"),
    # ---- casts and types
    ("CAST(", r"(?<!TRY_)\bCAST\s*\("),
    ("TRY_CAST(", r"\bTRY_CAST\s*\("),
    (":: cast", r"::\s*[A-Za-z]"),
    ("HUGEINT", r"\bHUGEINT\b"),
    ("UHUGEINT", r"\bUHUGEINT\b"),
    ("INT128", r"\bINT128\b"),
    ("DECIMAL(p,s)", r"\bDECIMAL\s*\(\s*\d+\s*,\s*\d+\s*\)"),
    ("DECIMAL(38,0)", r"\bDECIMAL\s*\(\s*38\s*,\s*0\s*\)"),
    ("DECIMAL/NUMERIC bare", r"\b(DECIMAL|NUMERIC)\b(?!\s*\()"),
    ("BIGINT", r"\bBIGINT\b"),
    ("UBIGINT", r"\bUBIGINT\b"),
    ("INTEGER/INT", r"\b(INTEGER|INT)\b(?!\s*\()"),
    ("UINTEGER/USMALLINT/UTINYINT", r"\b(UINTEGER|USMALLINT|UTINYINT)\b"),
    ("SMALLINT/TINYINT", r"\b(SMALLINT|TINYINT)\b"),
    ("DOUBLE", r"\bDOUBLE\b"),
    ("FLOAT/REAL", r"\b(FLOAT|REAL)\b"),
    ("VARCHAR/TEXT/STRING type", r"\b(VARCHAR|TEXT|STRING)\b"),
    ("BLOB/BYTEA", r"\b(BLOB|BYTEA)\b"),
    ("BOOLEAN/BOOL", r"\b(BOOLEAN|BOOL)\b"),
    ("TIMESTAMP type", r"\bTIMESTAMP(TZ|_S|_MS|_NS)?\b(?!\s+')"),
    ("TIMESTAMP WITH TIME ZONE", r"\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b"),
    ("DATE type", r"\bDATE\b(?!\s+'|_)(?!\()"),
    ("UUID", r"\bUUID\b"),
    ("JSON type", r"\bJSON\b(?!_)"),
    ("MAP/STRUCT/LIST type", r"\b(MAP|STRUCT|LIST)\s*\("),
    ("ENUM", r"\bENUM\b"),
    # ---- numeric ops
    ("// integer division", r"//"),
    ("/ division", r"(?<!/)/(?!/)"),
    ("% modulo", r"\s%\s"),
    ("power/pow/**", r"\b(power|pow)\s*\(|\*\*"),
    ("abs(", r"\babs\s*\("),
    ("round(", r"\bround\s*\("),
    ("floor(", r"\bfloor\s*\("),
    ("ceil(", r"\bceil(ing)?\s*\("),
    ("greatest(", r"\bgreatest\s*\("),
    ("least(", r"\bleast\s*\("),
    ("sign(", r"\bsign\s*\("),
    ("bit ops & | << >> xor", r"\s(&|<<|>>)\s|\bxor\s*\(|\b~"),
    ("unary minus (negation after ( or ,)", r"[(,]\s*-\s*[a-zA-Z_(]"),
    ("subtraction a - b", r"[\w)]\s+-\s+[\w(]"),
    # ---- aggregates
    ("sum(", r"\bsum\s*\("),
    ("count(*)", r"\bcount\s*\(\s*\*\s*\)"),
    ("count(col)", r"\bcount\s*\(\s*(?!\*|DISTINCT)[^)]"),
    ("count(DISTINCT", r"\bcount\s*\(\s*DISTINCT\b"),
    ("avg(", r"\bavg\s*\("),
    ("min(", r"\bmin\s*\("),
    ("max(", r"\bmax\s*\("),
    ("median(", r"\bmedian\s*\("),
    ("quantile*(", r"\bquantile\w*\s*\("),
    ("approx_*(", r"\bapprox_\w+\s*\("),
    ("arg_max/max_by", r"\b(arg_max|max_by|argmax)\s*\("),
    ("arg_min/min_by", r"\b(arg_min|min_by|argmin)\s*\("),
    ("string_agg/listagg/group_concat", r"\b(string_agg|listagg|list_agg|group_concat)\s*\("),
    ("array_agg/list(", r"\b(array_agg|list)\s*\("),
    ("first(/last( aggregate", r"\b(first|last|any_value)\s*\("),
    ("bool_or/bool_and", r"\b(bool_or|bool_and)\s*\("),
    ("bit_or/bit_and agg", r"\b(bit_or|bit_and|bit_xor)\s*\("),
    ("histogram(", r"\bhistogram\s*\("),
    ("mode(", r"\bmode\s*\("),
    ("stddev/var/corr/regr", r"\b(stddev\w*|var_\w+|variance|corr|covar\w*|regr_\w+)\s*\("),
    ("product(", r"\bproduct\s*\("),
    ("favg/fsum", r"\b(favg|fsum|kahan_sum)\s*\("),
    # ---- window fns
    ("row_number(", r"\brow_number\s*\("),
    ("rank(/dense_rank(", r"\b(dense_)?rank\s*\("),
    ("lag(", r"\blag\s*\("),
    ("lead(", r"\blead\s*\("),
    ("first_value(", r"\bfirst_value\s*\("),
    ("last_value(", r"\blast_value\s*\("),
    ("nth_value(", r"\bnth_value\s*\("),
    ("ntile/percent_rank/cume_dist", r"\b(ntile|percent_rank|cume_dist)\s*\("),
    # ---- time
    ("epoch_ms(", r"\bepoch_ms\s*\("),
    ("epoch(", r"\bepoch\s*\("),
    ("to_timestamp(", r"\bto_timestamp\s*\("),
    ("make_timestamp(", r"\bmake_timestamp\w*\s*\("),
    ("make_date(", r"\bmake_date\s*\("),
    ("strftime(", r"\bstrftime\s*\("),
    ("strptime(", r"\bstrptime\s*\("),
    ("date_trunc(", r"\bdate_trunc\s*\("),
    ("time_bucket(", r"\btime_bucket\s*\("),
    ("date_part(", r"\bdate_part\s*\("),
    ("extract(", r"\bextract\s*\("),
    ("date_diff/datediff(", r"\b(date_diff|datediff|date_sub|datesub)\s*\("),
    ("date_add/dateadd(", r"\b(date_add|dateadd)\s*\("),
    ("age(", r"\bage\s*\("),
    ("now()/current_timestamp/current_date", r"\bnow\s*\(\)|\bcurrent_(timestamp|date|time)\b|\bget_current_timestamp\s*\("),
    ("to_days/to_seconds/to_hours etc", r"\bto_(days|seconds|hours|minutes|milliseconds|microseconds|years|months)\s*\("),
    ("::DATE / ::TIMESTAMP cast", r"::\s*(DATE|TIMESTAMP\w*)\b"),
    ("year(/month(/day(/hour(/dayofweek(", r"\b(year|month|day|hour|minute|second|dayofweek|weekday|week|yearweek|dayofyear|isodow)\s*\("),
    ("timezone(/AT TIME ZONE", r"\btimezone\s*\(|\bAT\s+TIME\s+ZONE\b"),
    # ---- strings
    ("lower(", r"\blower\s*\("),
    ("upper(", r"\bupper\s*\("),
    ("substr/substring(", r"\bsubstr(ing)?\s*\("),
    ("left(", r"\bleft\s*\("),
    ("right(", r"\bright\s*\("),
    ("length(/len(", r"\b(length|len|strlen|char_length|character_length)\s*\("),
    ("concat(", r"\bconcat\s*\("),
    ("concat_ws(", r"\bconcat_ws\s*\("),
    ("|| concat", r"\|\|"),
    ("split_part(", r"\bsplit_part\s*\("),
    ("string_split/str_split/split(", r"\b(string_split|str_split|split|string_to_array)\s*\("),
    ("replace(", r"\breplace\s*\("),
    ("trim/ltrim/rtrim(", r"\b(l|r)?trim\s*\("),
    ("starts_with/prefix(", r"\b(starts_with|prefix)\s*\("),
    ("ends_with/suffix(", r"\b(ends_with|suffix)\s*\("),
    ("contains(", r"\bcontains\s*\("),
    ("position/strpos/instr(", r"\b(position|strpos|instr)\s*\("),
    ("lpad/rpad(", r"\b(lpad|rpad)\s*\("),
    ("repeat(/reverse(", r"\b(repeat|reverse)\s*\("),
    ("regexp_matches(", r"\bregexp_matches\s*\("),
    ("regexp_extract(", r"\bregexp_extract(_all)?\s*\("),
    ("regexp_replace(", r"\bregexp_replace\s*\("),
    ("regexp_full_match(", r"\bregexp_full_match\s*\("),
    ("~ regex operator", r"\s!?~\*?\s"),
    ("printf/format(", r"\b(printf|format)\s*\("),
    ("hex(/to_hex(", r"\b(to_)?hex\s*\("),
    ("unhex(/from_hex(", r"\b(unhex|from_hex)\s*\("),
    ("encode(/decode(", r"\b(encode|decode)\s*\("),
    ("hash/md5/sha256(", r"\b(hash|md5|sha256|sha1)\s*\("),
    ("bit_length/octet_length", r"\b(bit_length|octet_length)\s*\("),
    ("ord(/chr(/ascii(/unicode(", r"\b(ord|chr|ascii|unicode)\s*\("),
    ("string_agg", r"\bstring_agg\s*\("),
    # ---- null/conditional
    ("coalesce(", r"\bcoalesce\s*\("),
    ("nullif(", r"\bnullif\s*\("),
    ("ifnull(", r"\bifnull\s*\("),
    ("if(", r"(?<![\w_])if\s*\("),
    ("iif(", r"\biif\s*\("),
    ("nvl(/nvl2(", r"\bnvl2?\s*\("),
    ("typeof(", r"\btypeof\s*\("),
    # ---- list / struct / map / json
    ("list_*(", r"\blist_\w+\s*\("),
    ("array_*(", r"\barray_\w+\s*\("),
    ("struct_*(", r"\bstruct_\w+\s*\("),
    ("unnest(", r"\bunnest\s*\("),
    ("map_*(/map(", r"\bmap(_\w+)?\s*\("),
    ("json_*(", r"\bjson_\w+\s*\("),
    ("-> / ->> json ops", r"->>?\s*'"),
    ("[i] list index", r"\w\[\d+\]|\w\[-?\d+:"),
    ("generate_series/range(", r"\b(generate_series|range)\s*\("),
    ("row(/struct_pack(", r"\b(row|struct_pack)\s*\("),
    ("apply(/filter(/reduce( list fns", r"\b(apply|filter(?!\s*\(\s*WHERE)|reduce|list_transform|list_filter|list_reduce)\s*\("),
    ("enum_*", r"\benum_\w+\s*\("),
    # ---- table functions / IO
    ("read_parquet(", r"\bread_parquet\s*\("),
    ("union_by_name", r"\bunion_by_name\b"),
    ("read_csv/read_json/read_text/glob(", r"\b(read_csv\w*|read_json\w*|read_text|glob|parquet_scan|parquet_metadata|parquet_schema)\s*\("),
    ("duckdb_*()/information_schema", r"\bduckdb_\w+\s*\(|\binformation_schema\."),
    # ---- misc
    ("gen_random_uuid/uuid(", r"\b(gen_random_uuid|uuid)\s*\("),
    ("random(", r"\brandom\s*\("),
    ("current_setting/version(", r"\b(current_setting|version)\s*\("),
    ("lambda keyword (lambda x, y: ...)", r"\blambda\s+\w+"),
    ("list comprehension [.. FOR x IN ..]", r"\[[^\]]*\bFOR\s+\w+\s+IN\b"),
    ("ordered aggregate agg(x ORDER BY ..)", r"\b(list|array_agg|string_agg|first|last|arg_max|arg_min)\s*\([^()]*\bORDER\s+BY\b"),
    ("CAST('0x'||hex AS BIGINT/HUGEINT) (hex-string to int)", r"CAST\s*\(\s*\(?\s*'0x'\s*\|\|"),
    ("substr(..., -n) negative start", r"\bsubstr\w*\s*\([^()]*,\s*-\d+"),
    ("i64 max sentinel 9223372036854775807", r"9223372036854775807"),
    ("ASOF LEFT JOIN", r"\bASOF\s+LEFT\s+JOIN\b"),
    ("block_number * N + log_index ordering key", r"block_number\s*\*\s*\d+\s*\+\s*log_index"),
    ("decode(", r"\bdecode\s*\("),
    ("from_json(", r"\bfrom_json\s*\("),
    ("json_extract_string(", r"\bjson_extract_string\s*\("),
    ("json_type(", r"\bjson_type\s*\("),
    ("->> / -> json extraction op", r"->>?\s*'"),
    ("column IN table", r"\bIN\s+\w+\s*(\)|$|\s+(AND|OR|WHERE|GROUP|ORDER))"),
    ("COUNT(*) FILTER", r"\bCOUNT\s*\(\s*\*\s*\)\s*FILTER"),
    ("SUM(...) FILTER", r"\bSUM\s*\([^()]*\)\s*FILTER"),
    ("MIN/MAX(...) FILTER", r"\b(MIN|MAX)\s*\([^()]*\)\s*FILTER"),
    ("arg_min/arg_max(...) FILTER", r"\barg_(min|max)\s*\([^()]*\)\s*FILTER"),
    ("SUM(x) OVER (window)", r"\bSUM\s*\([^()]*\)\s*OVER\s*\("),
    ("ROW_NUMBER() OVER ... rn = 1 dedupe", r"\brn\s*=\s*1\b"),
    ("TRY( expression", r"\bTRY\s*\("),
    ("range(a,b) table function", r"\bFROM\s+range\s*\("),
    ("arg_min/arg_max with list key [..]", r"\barg_(min|max)\s*\(\s*[\w.]+\s*,\s*\["),
    ("DATE 'x' + int arithmetic", r"\bDATE\s+'[^']+'\s*[+-]"),
    ("unnest(CASE/list) in select list", r"\bunnest\s*\(\s*(CASE|\[|TRY|from_json)"),
    ("calls table ref", r"\bFROM\s+calls\b"),
    ("tx_from column", r"\btx_from\b"),
    ("ordered agg list(x ORDER BY)", r"\blist\s*\((?:[^()]|\([^()]*\))*\bORDER\s+BY\b"),
    ("substr(..., -n) negative (nested)", r"\bsubstr\w*\s*\((?:[^()]|\([^()]*(?:\([^()]*\))?[^()]*\))*,\s*-\d+\s*\)"),
    ("AS alias", r"\bAS\s+\w+"),
    ("* (star)", r"\*"),
])

totals = collections.Counter()
nests = collections.defaultdict(set)
examples = {}
lines_total = 0
perfile = {}
for f in files:
    path = os.path.join(ROOT, f)
    raw = open(path, encoding="utf-8", errors="replace").read()
    lines_total += raw.count("\n") + (0 if raw.endswith("\n") else 1)
    s = strip_comments(raw)
    pf = collections.Counter()
    for name, rx in P.items():
        ms = list(re.finditer(rx, s, flags=I | re.M))
        if ms:
            pf[name] = len(ms)
            totals[name] += len(ms)
            nests[name].add(nest_of(f))
            if name not in examples:
                # find line number in original: approximate by searching stripped text offset
                off = ms[0].start()
                ln = s[:off].count("\n") + 1
                examples[name] = f"{f}:{ln}"
    perfile[f] = pf

print(f"files={len(files)} lines={lines_total}")
print()
print("| feature | count | files | nests | example |")
print("|---|---|---|---|---|")
for name in P:
    c = totals.get(name, 0)
    nf = sum(1 for f in files if perfile[f].get(name))
    print(f"| {name} | {c} | {nf} | {len(nests[name])}: {', '.join(sorted(nests[name]))} | {examples.get(name,'')} |")

if per_file:
    print()
    print("## per-file distinct feature counts (top 15)")
    ranked = sorted(perfile.items(), key=lambda kv: -len([k for k in kv[1] if k not in ("AS alias","* (star)","SELECT","NULL literal","double-quoted identifier","/ division","IN (list)")]))
    for f, pf in ranked[:15]:
        n = len([k for k in pf if k not in ("AS alias","* (star)","SELECT","NULL literal","double-quoted identifier","/ division","IN (list)")])
        print(f"{n:3d} distinct features  {f}")
