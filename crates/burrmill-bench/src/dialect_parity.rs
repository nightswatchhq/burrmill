//! `dialect-parity` (roadmap 6.5): the same SQL through DuckDB and `burrmill::Engine`, each result
//! encoded as nuthatch encodes it, compared byte for byte.
//!
//! DuckDB reads the fixture through views built as nuthatch builds them (`_dec` by `TRY_CAST`);
//! Burrmill opens the same directory as a nest. A difference is either the dialect layer's to close
//! or listed in `KNOWN` with the reason it stands.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

const CORPUS: &[&str] = &[
    "SELECT count(*) FROM transfer",
    "SELECT sum(value_dec) FROM transfer",
    "SELECT sum(block_number) FROM transfer",
    "SELECT avg(block_number) FROM transfer",
    "SELECT avg(block_number - log_index) AS a, avg(CAST(value AS HUGEINT)) AS b, avg(CAST(\"tokensRewards\" AS DECIMAL(10,2))) AS c, avg(greatest(18, log_index)) AS d FROM transfer",
    "SELECT \"from\", avg(block_number) AS a FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT block_number, avg(log_index) OVER (ORDER BY block_number, log_index) AS a FROM transfer ORDER BY 1, 2",
    "SELECT block_number / 100 AS q FROM transfer ORDER BY 1",
    "SELECT block_number // 100 AS b, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT CAST(value AS HUGEINT) * 2 AS v FROM transfer ORDER BY 1",
    "SELECT -CAST(value AS HUGEINT) AS v FROM transfer ORDER BY 1",
    "SELECT \"from\", sum(CAST(value AS HUGEINT)) AS s FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT 7 / 2 AS a, 7 // 2 AS b, -7 // 2 AS c, 7 % -2 AS d, 1 / 0 AS e, 1 // 0 AS f, 0.5 / 2 AS g",
    "SELECT to_timestamp(block_timestamp) AS t FROM transfer ORDER BY 1 LIMIT 2",
    "SELECT date_trunc('day', to_timestamp(block_timestamp)) AS d, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT extract(year FROM to_timestamp(block_timestamp)) AS y FROM transfer LIMIT 1",
    "SELECT round(CAST(value AS DOUBLE) / 1e18, 4) AS r FROM transfer ORDER BY 1",
    "SELECT lower(\"to\") AS t FROM transfer ORDER BY 1",
    "SELECT count(*) AS n FROM transfer WHERE enabled = 'true'",
    "SELECT count(*) AS n FROM transfer WHERE value = 10",
    "SELECT count(*) AS n FROM transfer WHERE value IN (10, 4)",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" = 10",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" IN (5, 7, 10)",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" > 9",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" BETWEEN 1 AND 9",
    "SELECT count(*) AS n FROM transfer WHERE 9 < \"tokensRewards\"",
    "SELECT count(*) AS n FROM transfer WHERE CAST(\"tokensRewards\" AS INTEGER) > 9",
    "SELECT count(*) AS n FROM transfer WHERE enabled = true",
    "SELECT count(*) AS n FROM transfer WHERE enabled AND true",
    "SELECT count(*) AS n FROM transfer WHERE NOT enabled",
    "SELECT \"tokensRewards\" <> 5 AS ne FROM transfer ORDER BY 1",
    "SELECT CAST(value AS HUGEINT), block_number // 2, NOT (block_number > 2) FROM transfer ORDER BY 2",
    "SELECT \"Value\", VALUE, \"TOKENSREWARDS\", t.\"FROM\", \"Value\" || 'x', sum(Block_Number) OVER () FROM Transfer t ORDER BY 1",
    "SELECT count(*) FROM TRANSFER T WHERE t.ENABLED = 'true'",
    "SELECT table_name FROM information_schema.tables WHERE NOT starts_with(table_name, '__hot_') ORDER BY table_name",
    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'transfer' ORDER BY ordinal_position",
    "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables ORDER BY 3",
    "SELECT column_name, ordinal_position, is_nullable FROM information_schema.columns WHERE table_name = 'label' ORDER BY 2",
    "SELECT *, value_dec, \"from\" FROM transfer ORDER BY block_number, log_index",
    "SELECT 1 AS a, 2 AS a, block_number AS a FROM transfer ORDER BY 3",
    "SELECT block_number - 1 AS a, COALESCE(block_number - log_index, 0) AS b, CASE WHEN log_index = 0 THEN 0 ELSE block_number END AS c, block_number * 2 + 1 AS d FROM transfer ORDER BY 1",
    "SELECT t.\"from\", CAST(t.block_number / 10 AS BIGINT) AS block_number FROM transfer t ORDER BY t.block_number DESC",
    "SELECT CAST(0.5::DOUBLE AS BIGINT) a, CAST(1.5::DOUBLE AS BIGINT) b, CAST(2.5::DOUBLE AS BIGINT) c, CAST(-0.5::DOUBLE AS BIGINT) d, CAST(-1.5::DOUBLE AS BIGINT) e, CAST(0.6::DOUBLE AS BIGINT) f, CAST(2.5::DECIMAL(3,1) AS BIGINT) l, CAST(-2.5::DECIMAL(3,1) AS BIGINT) m, CAST(2.4::DECIMAL(3,1) AS BIGINT) n",
    "SELECT CAST(CAST(value AS DOUBLE) / 3 AS BIGINT) AS q, CAST(block_number / 4 AS INTEGER) AS r FROM transfer WHERE block_number < 100 ORDER BY 1, 2",
    "SELECT k FROM (SELECT block_number * 100000 + log_index AS k FROM transfer UNION ALL SELECT block_number * 100000 + log_index FROM transfer) ORDER BY 1 LIMIT 3",
    "SELECT CAST('0x1F' AS BIGINT) a, CAST(' 0x1f' AS BIGINT) b, CAST('0b101' AS BIGINT) c, CAST('0x1_f' AS INTEGER) d, CAST('0x7fffffffffffffff' AS BIGINT) e, CAST('42' AS BIGINT) f",
    "SELECT CAST('0xffffffffffffffff' AS BIGINT)",
    "SELECT CAST('-0x1f' AS BIGINT)",
    "SELECT decode(from_hex('68c3a96c6c6f')) a, decode(from_hex('')) b, from_hex('abc') c, from_hex('4142') d",
    "SELECT decode(from_hex('ff'))",
    "SELECT from_hex('zz')",
    "SELECT CAST(('0x' || substr('000000000000000000000000000000000000000000000000000000000000002a', 49, 16)) AS BIGINT) AS n",
    "SELECT max(\"from\") AS m FROM transfer",
    "SELECT (block_number, log_index) > (2, 0) AS gt FROM transfer ORDER BY block_number, log_index",
    "SELECT CAST(block_number AS UBIGINT) AS b FROM transfer ORDER BY 1 LIMIT 1",
    "SELECT \"tokensRewards\" AS r FROM transfer ORDER BY 1",
    "SELECT t.\"to\", l.name FROM transfer t LEFT JOIN label l ON l.addr = t.\"to\" ORDER BY 1, 2",
    "SELECT count(DISTINCT \"from\") AS n FROM transfer",
    "SELECT \"from\" || ':' || CAST(block_number AS VARCHAR) AS k FROM transfer ORDER BY 1",
    "SELECT 1 = true AS a, 2 = true AS b, 0 = false AS c, 1 < true AS d, 2 > false AS e, 1.5 = true AS f",
    "SELECT block_number FROM transfer WHERE (block_number = 1) = true OR log_index = true ORDER BY 1",
    "SELECT true IN (1, 2) AS a, 1 IN (true, false) AS b, 2 IN (true) AS c",
    "SELECT 1 IS DISTINCT FROM 2 AND 3 IS DISTINCT FROM 3 OR 4 IS NOT DISTINCT FROM 4 AS v",
    "SELECT block_number FROM transfer WHERE \"from\" IS DISTINCT FROM '0xa' AND log_index = 0 OR \"to\" IS NOT DISTINCT FROM '0xe' ORDER BY 1",
    "SELECT CAST(2.5::DOUBLE AS HUGEINT) AS a, CAST(-3.5::DOUBLE AS HUGEINT) AS b, CAST(2.5::DOUBLE AS DECIMAL(38,0)) AS c",
    "SELECT CAST(CAST('47582028310819253533' AS HUGEINT) AS DOUBLE) AS a, CAST(CAST('9791626625542365.709860864' AS DECIMAL(38,9)) AS DOUBLE) AS b",
    "SELECT CAST(value AS HUGEINT)::DOUBLE / 3 AS d FROM transfer ORDER BY 1",
    "SELECT 1.5 AS a, 1.5 + 1 AS b, 1.5 * 2.25 AS c, 0.5 AS d, -2.50 AS e, 100.0 / 3 AS f, 1e3 AS g, 1.5e2 AS h, 0.001 AS i",
    "SELECT 1.5 - 0.25 AS a, 10.5 % 3 AS b, block_number * 1.5 AS c, block_number + 0.25 AS d FROM transfer ORDER BY 1, 3",
    "SELECT round(2.345, 2) AS a, CAST(1.25 AS DOUBLE) AS b, 1.10 = 1.1 AS c, 12345678901234567890.5 AS d",
    "SELECT round(9.995, 2) AS a, round(-2.345, 2) AS b, round(2.345) AS c, round(2.345, 5) AS d, round(1234.5, -2) AS e, round(2.5) AS f, round(-2.5) AS g",
    "SELECT round(CAST(value AS HUGEINT) / 7, 3) AS r FROM transfer ORDER BY 1",
    "SELECT * FROM (SELECT block_number, block_number FROM transfer) ORDER BY 1",
    "SELECT count(*) AS n FROM (SELECT t.\"to\", l.addr AS \"to\" FROM transfer t JOIN label l ON l.addr = t.\"to\")",
    "SELECT * FROM (SELECT t.\"from\", l.* FROM transfer t JOIN label l ON l.addr = t.\"from\") ORDER BY 1, 2, 3",
    "WITH x AS (SELECT block_number, block_number + 0 FROM transfer) SELECT * FROM x ORDER BY 1",
    "SELECT * FROM (SELECT l.addr, l.* FROM label l) ORDER BY 1",
    "SELECT block_number FROM (SELECT block_number, block_number FROM transfer) ORDER BY 1",
    "SELECT * FROM (SELECT * FROM label a JOIN label b ON a.addr = b.addr) ORDER BY 1",
    "SELECT TRY_CAST('0xa' AS BIGINT) a, TRY_CAST('0XA' AS BIGINT) b, TRY_CAST('0xzz' AS BIGINT) c, TRY_CAST(' 12 ' AS BIGINT) d, TRY_CAST('abc' AS BIGINT) e, TRY_CAST('0xffffffffffffffff' AS BIGINT) f, TRY_CAST('1.5' AS BIGINT) g, TRY_CAST('' AS BIGINT) h",
    "SELECT CAST(' 12 ' AS BIGINT) d, CAST('1.5' AS BIGINT) g, CAST('-2.5' AS BIGINT) i, CAST('1e2' AS BIGINT) j, CAST('+7' AS BIGINT) k",
    "SELECT TRY_CAST(\"from\" AS BIGINT) AS f, TRY_CAST(value AS INTEGER) AS v FROM transfer ORDER BY block_number, log_index",
    "SELECT f, sum(v) AS s, count(*) AS n FROM (SELECT \"to\" AS f, TRY_CAST(lower(\"from\") AS BIGINT) AS v FROM transfer) GROUP BY f ORDER BY 1",
    "SELECT sum(TRY_CAST(\"from\" AS BIGINT)) AS a, sum(TRY_CAST(value AS HUGEINT)) AS b, sum(TRY_CAST(enabled AS BIGINT)) AS c FROM transfer",
    "SELECT block_number // 2 AS a, block_number // log_index AS b, greatest(block_number, 82) AS c, least(log_index, 1) AS d FROM transfer ORDER BY block_number, log_index",
    "SELECT block_number AS x FROM transfer INTERSECT SELECT 3 ORDER BY 1",
    "SELECT CAST(block_number AS BIGINT) - 10 AS x FROM transfer EXCEPT SELECT log_index FROM transfer ORDER BY 1",
    "SELECT block_number AS x FROM transfer EXCEPT ALL SELECT log_index FROM transfer ORDER BY 1",
    "SELECT block_number AS x FROM transfer UNION SELECT -1 ORDER BY 1",
    "SELECT \"to\" AS x FROM transfer INTERSECT ALL SELECT \"from\" FROM transfer ORDER BY 1",
    "SELECT \"to\" AS x, log_index AS y FROM transfer EXCEPT ALL SELECT \"from\", 0 FROM transfer ORDER BY 1, 2",
    "SELECT n AS x FROM (VALUES (1), (1), (1), (NULL), (NULL), (2)) t(n) INTERSECT ALL SELECT n FROM (VALUES (1), (1), (NULL), (3)) u(n) ORDER BY 1",
    "SELECT n AS x FROM (VALUES (1), (1), (1), (NULL), (NULL), (2)) t(n) EXCEPT ALL SELECT n FROM (VALUES (1), (NULL), (3)) u(n) ORDER BY 1",
    "SELECT DISTINCT \"to\" AS x FROM transfer EXCEPT ALL SELECT \"from\" FROM transfer ORDER BY 1",
    "SELECT 32 AS x FROM transfer EXCEPT ALL SELECT CAST(log_index AS BIGINT) * block_number FROM transfer ORDER BY 1",
    "SELECT log_index AS x FROM transfer EXCEPT SELECT greatest(log_index, CAST(block_number AS BIGINT)) FROM transfer ORDER BY 1",
    "SELECT log_index AS x FROM transfer INTERSECT SELECT 1.5 ORDER BY 1",
    "SELECT log_index FROM transfer WHERE CASE WHEN block_number < -16 THEN 48 ELSE log_index END < (block_number - 95)",
    "SELECT \"to\" AS t FROM transfer ORDER BY 1 DESC LIMIT 3",
    "SELECT n FROM (VALUES (1), (NULL), (2)) t(n) ORDER BY n DESC",
    "SELECT string_agg(n::VARCHAR, ',' ORDER BY n DESC) AS a FROM (VALUES (1), (NULL), (2)) t(n)",
    "SELECT CAST(to_timestamp(block_timestamp) AS VARCHAR) AS a, to_timestamp(block_timestamp) || '|' AS b FROM transfer ORDER BY 1",
    "SELECT year(to_timestamp(block_timestamp)) AS y, month(to_timestamp(block_timestamp)) AS m, dayofweek(to_timestamp(block_timestamp)) AS w, epoch(to_timestamp(block_timestamp)) AS e, hour(to_timestamp(block_timestamp)) AS h FROM transfer ORDER BY 1, 2, 3, 4",
    "SELECT strftime(to_timestamp(block_timestamp), '%Y-%m-%d %H:%M|%b %a %j') AS s, date_diff('day', to_timestamp(1700000000), to_timestamp(block_timestamp)) AS d, datediff('month', to_timestamp(1700000000), to_timestamp(block_timestamp)) AS m FROM transfer ORDER BY 1",
    "SELECT regexp_extract(value, '[0-9]{2}') AS a, regexp_matches(\"from\", '(?i)0XA') AS b, regexp_replace(value, '0', 'z', 'g') AS c, regexp_replace(value, '(1)', '\\1\\1') AS d, regexp_replace(value, '^1', '\\2') AS e FROM transfer ORDER BY block_number, log_index",
    "SELECT sign(CAST(block_number AS BIGINT) - 3) AS a, sign(-2.5) AS b, NULLIF(block_number, 2) AS c FROM transfer ORDER BY block_number, log_index",
    "SELECT 1 AS a ORDER BY 'x'",
    "SELECT \"from\", arg_max(block_number, block_timestamp * 10 + log_index) AS a, arg_min(\"to\", block_timestamp * 10 + log_index) AS b, max_by(value, block_timestamp * 10 + log_index) AS c, min_by(value, block_timestamp * 10 + log_index) FILTER (WHERE value <> '4') AS d FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT arg_max(x, y) AS a, arg_min(x, y) AS b FROM (VALUES (1, 5), (NULL, 9), (3, NULL), (4, 6)) t(x, y)",
    "SELECT json_extract_string('{\"a\": {\"b\": 7}, \"c\": [1, 2]}', '$.a.b') AS a, json_extract('{\"a\": {\"b\": 7}}', '$.a') AS b, json_extract_string('{\"c\": [1, 2]}', '$.c[1]') AS c, json_extract_string('{\"a\": 1}', '$.zz') AS d",
    "SELECT '{\"a\": 1, \"b\": \"x\"}'->>'b' AS a, '{\"a\": {\"k\": 2}}'->'a' AS b, '{\"a\": {\"k\": 2}}'->>'$.a.k' AS c",
    "SELECT json_type('{\"a\": 1}') AS a, json_type('[1]') AS b, json_type('1.5') AS c, json_type('\"x\"') AS d",
    "SELECT json_extract('{\"a\": [1, {\"b\": null}], \"c\": \"x\", \"d\": 1.50, \"e\": 12345678901234567890}', '$.a[1]') a, json_extract_string('{\"a\": {\"b\": null}}', '$.a.b') b, json_extract_string('{\"c\": \"x\"}', 'c') c, json_extract('[10, 20]', 1) d, json_extract_string('{\"d\": 1.50}', '$.d') e, json_extract('{\"e\": 12345678901234567890}', '$.e') f, json_type('{\"e\": 12345678901234567890}', '$.e') g, json_type('{\"x\": -1}', '$.x') h, json_type('null') i, json_type('true') j, json_extract('{\"a b\": 1}', '$.\"a b\"') k, json_extract('[1,2,3]', '$[#-1]') l, json_extract('{\"a\":1}', '$.z') m",
    "SELECT '[1,2]'->1 AS v, '{\"a\": {\"b\": 3}}'->'a'->>'b' AS w, json_extract_string(NULL, '$.a') AS x",
    "SELECT json_extract('not json', '$.a')",
    "SELECT from_json('{\"a\": 1, \"b\": \"x\"}', '{\"a\": \"INTEGER\", \"b\": \"VARCHAR\"}').b AS b",
    "SELECT TRY(CAST('x' AS INTEGER)) AS a, TRY(1 // 0) AS b, TRY(CAST('7' AS INTEGER)) AS c",
    "SELECT j, TRY(json_extract_string(j, '$.a')) AS a FROM (VALUES ('{\"a\": 1}'), ('bad'), (NULL), ('{\"a\": \"x\"}')) t(j) ORDER BY 1",
    "SELECT TRY(from_hex(h)) IS NULL AS failed FROM (VALUES ('4142'), ('zz'), ('00')) t(h) ORDER BY h",
    "SELECT \"from\", block_number FROM transfer QUALIFY row_number() OVER (PARTITION BY \"from\" ORDER BY block_number DESC, log_index DESC) = 1 ORDER BY 1",
    "SELECT block_number, sum(log_index) OVER w AS s FROM transfer WINDOW w AS (ORDER BY block_number, log_index) ORDER BY 1, 2",
    "SELECT range AS r FROM range(3) ORDER BY 1",
    "SELECT * FROM range(2, 11, 4) ORDER BY 1",
    "SELECT * FROM generate_series(5, 1, -2) ORDER BY 1 DESC",
    "SELECT count(*) AS n FROM range(0)",
    "SELECT r.x, t.block_number FROM range(1, 3) r(x) JOIN transfer t ON t.block_number = r.x ORDER BY 1, 2",
    "SELECT * FROM generate_series(1, 3) t(x) ORDER BY 1",
    "SELECT unnest([1, 2, 3]) AS u",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE x = 3 OR x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) ORDER BY 1",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE CASE WHEN x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) THEN true ELSE x = 3 END ORDER BY 1",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE x = 3 OR x IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) ORDER BY 1",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) ORDER BY 1",
    "SELECT (SELECT count(DISTINCT addr) FROM label) AS n_all, (SELECT count(DISTINCT addr) FROM label WHERE addr NOT IN (SELECT \"to\" AS addr FROM transfer WHERE log_index > 0)) AS n_active",
    "SELECT \"from\", \"from\" IN (SELECT \"to\" FROM transfer) AS bare_outer, t.\"from\" IN (SELECT \"to\" FROM transfer) AS qualified FROM transfer t ORDER BY block_number, log_index",
    "SELECT 7.5::DOUBLE // 2 AS a, -7.5::DOUBLE // 2 AS b, 7::DOUBLE // 0 AS c, CAST(7 AS BIGINT) // 2.0::DOUBLE AS f, 1.5 // 1 AS g, block_number // 2.5 AS h FROM transfer ORDER BY block_number, log_index",
    "SELECT \"from\", bool_and(enabled = 'true') AS a, count(value = '10') AS b, bool_or('10' = value) AS c, bool_or(\"from\" = \"to\") AS d FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT \"from\", count(DISTINCT CASE WHEN \"to\" <> \"from\" THEN \"to\" ELSE '0xz' END) AS d, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT k, count(*) AS n FROM (SELECT CASE WHEN \"to\" <> \"from\" THEN \"to\" ELSE '0xz' END AS k FROM transfer GROUP BY 1) GROUP BY 1 ORDER BY 1",
    "SELECT to_timestamp(64814395658) AS a, to_timestamp(1.5) AS b, to_timestamp(-1.5) AS c, CAST(to_timestamp(64814395658) AS VARCHAR) AS d",
    "SELECT x, x IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) AS a, x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) AS b, x IN (SELECT y FROM (VALUES (1)) s(y)) AS c, x IN (SELECT y FROM (VALUES (1)) s(y) WHERE false) AS d FROM (VALUES (1), (2), (NULL)) t(x) ORDER BY x",
    "SELECT block_number, \"to\" IN (SELECT addr FROM label) AS labelled, EXISTS (SELECT 1 FROM label l WHERE l.addr = t.\"from\") AS known, NOT EXISTS (SELECT 1 FROM label l WHERE l.addr = t.\"to\" AND l.name = 'carol') AS not_carol FROM transfer t ORDER BY block_number, log_index",
    "SELECT \"from\", count(*) FILTER (WHERE true) AS n, bool_or(\"to\" IN (SELECT addr FROM label WHERE name <> 'alice')) AS any_label FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT CASE WHEN EXISTS (SELECT 1 FROM label l WHERE lower(l.addr) = lower(t.\"from\")) THEN 'known' ELSE 'unknown' END AS k FROM transfer t ORDER BY block_number, log_index",
    "SELECT \"from\", sum(CASE WHEN \"to\" IN (SELECT addr FROM label) THEN 1 ELSE 0 END) AS to_labelled, count(*) FILTER (WHERE EXISTS (SELECT 1 FROM label l WHERE l.addr = transfer.\"from\")) AS from_labelled FROM transfer GROUP BY 1 HAVING bool_or(\"to\" NOT IN (SELECT addr FROM label)) IS NOT NULL ORDER BY 1",
    "SELECT year(to_timestamp(block_timestamp)) AS y FROM transfer WHERE 'bob' <> CAST(to_timestamp(block_timestamp) AS VARCHAR) ORDER BY 1",
    "SELECT date_trunc('day', CAST(to_timestamp(block_timestamp) AS DATE)) AS a, CAST(to_timestamp(block_timestamp) AS DATE) + INTERVAL 33 HOUR AS b, CAST(to_timestamp(block_timestamp) AS DATE) + 3 AS c FROM transfer ORDER BY 1, 2",
    "SELECT CASE WHEN log_index = 0 THEN CAST(value AS DECIMAL(20,2)) ELSE 2.5::DOUBLE END AS a, COALESCE(TRY_CAST(value AS HUGEINT), 0.5::DOUBLE) AS b FROM transfer ORDER BY block_number, log_index",
    "SELECT log_index FROM transfer WHERE block_number - 95 > 0",
    "SELECT block_number - 95 AS d FROM transfer",
    "SELECT length('héllo') AS a, char_length('ab') AS b, length(\"from\") AS c FROM transfer ORDER BY block_number, log_index",
    "SELECT count(*) AS n FROM (SELECT * FROM transfer x JOIN transfer y ON x.block_number = y.block_number)",
    "WITH j AS (SELECT * FROM label a JOIN label b ON a.addr = b.addr) SELECT addr_1, name_1 FROM j ORDER BY 1",
    "SELECT block_number FROM transfer WHERE block_number > 1.5 AND block_number < 150.0 ORDER BY 1",
    "SELECT sum(block_number * 0.5) AS s, avg(block_number) + 0.5 AS a FROM transfer",
    "SELECT list_reduce([1, 2, 3], lambda a, x: a * 10 + x) AS a, list_reduce([1.5, 2.25], lambda a, x: a + x) AS b",
    "SELECT \"from\", list_reduce(list(block_number ORDER BY block_number, log_index), lambda a, x: a * 1000 + x) AS r FROM transfer GROUP BY 1 ORDER BY 1",
];

/// Differences that stand, and why.
const KNOWN: &[(&str, &str)] = &[
    (
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM label l WHERE lower(l.addr) = lower(t.\"from\")) THEN 'known' ELSE 'unknown' END AS k FROM transfer t ORDER BY block_number, log_index",
        "a refusal, not an answer: DataFusion 55 cannot decorrelate a subquery correlated through an \
         expression of the outer row (lower(t.x)), in any position; correlated through a column it can",
    ),
    (
        "SELECT year(to_timestamp(block_timestamp)) AS y FROM transfer WHERE 'bob' <> CAST(to_timestamp(block_timestamp) AS VARCHAR) ORDER BY 1",
        "a DuckDB 1.5 bug, not reported upstream yet: with a date part projected, a <> between text and a \
         cast timestamp drops every row, for <>, >= and the rest (project the timestamp itself and all rows return). Burrmill keeps them",
    ),
    (
    "SELECT log_index FROM transfer WHERE block_number - 95 > 0",
    "DuckDB rewrites x - 95 > 0 to x > 95 before evaluating, so its UBIGINT underflow never happens; \
     Burrmill evaluates what was written and refuses. Which overflows surface is the optimiser's",
)];

fn fixture(root: &std::path::Path) -> anyhow::Result<()> {
    let segs = root.join("segments");
    std::fs::create_dir_all(&segs)?;
    let s = |v: &[&str]| Arc::new(StringArray::from(v.to_vec())) as ArrayRef;
    let u = |v: &[u64]| Arc::new(UInt64Array::from(v.to_vec())) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("enabled", DataType::Utf8, true),
        Field::new("tokensRewards", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            u(&[1, 2, 2, 3, 150, 301]),
            u(&[0, 0, 1, 0, 2, 0]),
            u(&[1700000000, 1700000012, 1700000012, 1700086400, 1700172800, 1703980800]),
            s(&["0xa", "0xb", "0xA", "0xc", "0xa", "0xd"]),
            s(&["0xb", "0xc", "0xc", "0xa", "0xe", "0xa"]),
            s(&["10", "4", "1", "010", "250000000000000000000", "7"]),
            s(&["true", "false", "true", "true", "false", "true"]),
            s(&["5", "6", "7", "8", "9", "10"]),
        ],
    )?;
    let f = std::fs::File::create(segs.join(format!("transfer-{:064x}.parquet", 1)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    let lschema = Arc::new(Schema::new(vec![
        Field::new("addr", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let labels = RecordBatch::try_new(lschema.clone(), vec![s(&["0xa", "0xc"]), s(&["alice", "carol"])])?;
    let f = std::fs::File::create(segs.join(format!("label-{:064x}.parquet", 2)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, lschema, None)?;
    w.write(&labels)?;
    w.close()?;
    std::fs::write(
        root.join("schema.json"),
        r#"{"tables":[{"table":"transfer","columns":[{"name":"from","storage":"text"},{"name":"to","storage":"text"},{"name":"value","storage":"word32"},{"name":"tokensRewards","storage":"word32"}]}]}"#,
    )?;
    Ok(())
}

pub fn run() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    fixture(tmp.path())?;
    let segs = tmp.path().join("segments");
    let duck = duckdb::Connection::open_in_memory()?;
    // nuthatch sets no zone, so DuckDB follows the host's; hosted servers run in UTC, and so does
    // Burrmill, deterministically.
    duck.execute_batch("SET TimeZone = 'UTC';")?;
    duck.execute_batch(&format!(
        "CREATE VIEW transfer AS SELECT *, TRY_CAST(\"value\" AS DECIMAL(38,0)) AS \"value_dec\", \
         (\"value\" IS NOT NULL AND TRY_CAST(\"value\" AS DECIMAL(38,0)) IS NULL) AS \"value_overflow\", \
         TRY_CAST(\"tokensRewards\" AS DECIMAL(38,0)) AS \"tokensRewards_dec\", \
         (\"tokensRewards\" IS NOT NULL AND TRY_CAST(\"tokensRewards\" AS DECIMAL(38,0)) IS NULL) AS \"tokensRewards_overflow\" \
         FROM read_parquet('{0}/transfer-*.parquet');
         CREATE VIEW label AS SELECT * FROM read_parquet('{0}/label-*.parquet');",
        segs.display()
    ))?;
    let root = tmp.path().to_path_buf();
    let engine = std::thread::spawn(move || burrmill::Engine::open_nest(&root)).join().expect("open")?;
    let engine = Arc::new(engine);
    let mut failed = 0;
    for sql in CORPUS {
        let want = match crate::encode_parity::nuthatch_rows(&duck, sql) {
            Ok(v) => serde_json::to_string(&v)?,
            Err(e) => format!("ERROR {}", e.to_string().lines().next().unwrap_or("")),
        };
        let e2 = Arc::clone(&engine);
        let q = sql.to_string();
        let got = std::thread::spawn(move || -> String {
            match e2.sql(&q) {
                Ok(bs) => {
                    let mut rows = Vec::new();
                    for b in &bs {
                        match burrmill::df::encode::rows(b) {
                            Ok(r) => rows.extend(r),
                            Err(e) => return format!("ERROR {e}"),
                        }
                    }
                    serde_json::to_string(&Value::Array(rows)).unwrap()
                }
                Err(e) => format!("ERROR {}", e.to_string().replace('\n', " | ")),
            }
        })
        .join()
        .expect("engine thread");
        let known = KNOWN.iter().find(|(k, _)| k == sql).map(|(_, why)| *why);
        let both_refuse = want.starts_with("ERROR") && got.starts_with("ERROR");
        let tag = match (want == got || both_refuse, known) {
            (true, _) if both_refuse => "BOTH-REFUSE".to_string(),
            (true, _) => "SAME ".to_string(),
            (false, Some(why)) => format!("KNOWN ({why})"),
            (false, None) => {
                failed += 1;
                "DIFF ".to_string()
            }
        };
        println!("{tag}  {sql}");
        if want != got && !both_refuse {
            println!("    duckdb   {}", want.chars().take(300).collect::<String>());
            println!("    burrmill {}", got.chars().take(300).collect::<String>());
        }
    }
    println!("DIALECT\tcases={}\tdiffering={failed}", CORPUS.len());
    std::thread::spawn(move || drop(engine)).join().expect("drop engine");
    anyhow::ensure!(failed == 0, "{failed} dialect differences");
    Ok(())
}

/// `duck-names <sql>`: DuckDB's own column names for a statement over an empty `t`, one per line.
pub fn duck_names(sql: &str) -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(
        "CREATE TABLE t(\"from\" VARCHAR, \"to\" VARCHAR, \"value\" VARCHAR, block_number UBIGINT, \
         log_index UBIGINT, \"tokensRewards\" VARCHAR);",
    )?;
    let mut stmt = duck.prepare(sql)?;
    let rows = stmt.query([])?;
    for n in rows.as_ref().map(|s| s.column_names()).unwrap_or_default() {
        println!("{n}");
    }
    Ok(())
}

/// `duck-keywords`: DuckDB's keyword list with categories, for the naming printer's quoting rule.
pub fn duck_keywords() -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    let mut stmt = duck.prepare("SELECT keyword_name, keyword_category FROM duckdb_keywords() ORDER BY 1")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let (k, c): (String, String) = (r.get(0)?, r.get(1)?);
        println!("{k}\t{c}");
    }
    Ok(())
}

/// `duck-eval <sql>`: DuckDB's answer as nuthatch's JSON, for probing semantics.
pub fn duck_eval(sql: &str) -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    println!("{}", serde_json::to_string(&crate::encode_parity::nuthatch_rows(&duck, sql)?)?);
    Ok(())
}
