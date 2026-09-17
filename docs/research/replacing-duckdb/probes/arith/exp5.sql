.bail off
.mode box
SELECT version() AS duckdb;
SET threads=32;
-- ===== 1. integers =====
SELECT 'i64_over 32t' AS c, SUM(v) AS s, typeof(SUM(v)) AS t FROM 'data/i64_over/*.parquet';
SELECT 'i64_partial 32t' AS c, SUM(v) AS s FROM 'data/i64_partial/*.parquet';
SELECT 'i64_over grouped' AS c, v > 0 AS k, SUM(v) FROM 'data/i64_over/*.parquet' GROUP BY k ORDER BY k;
SELECT 'fold +' AS c, 9223372036854775807 + 1;
SELECT 'fold + bigint' AS c, 9223372036854775807::BIGINT + 1::BIGINT;
SELECT 'fold *' AS c, 10000000000::BIGINT * 10000000000::BIGINT;
SELECT 'fold neg' AS c, -(-9223372036854775807::BIGINT - 1::BIGINT);
SELECT 'runtime v + 1' AS c, v + 1 FROM 'data/i64_over/*.parquet' WHERE v > 0;
SELECT 'runtime v * 2' AS c, v * 2 FROM 'data/i64_over/*.parquet' WHERE v > 0;
SELECT 'runtime -v MIN' AS c, -v FROM 'data/i64_min/*.parquet';
SELECT 'CAST i64->i32' AS c, CAST(9223372036854775807 AS INTEGER);
SELECT 'TRY_CAST i64->i32' AS c, TRY_CAST(9223372036854775807 AS INTEGER);
SELECT 'CAST text->i64' AS c, CAST('9223372036854775808' AS BIGINT);
SELECT 'TRY_CAST text->i64' AS c, TRY_CAST('9223372036854775808' AS BIGINT);
-- the README case: HUGEINT partials fit, combine overflows, two files, many threads
COPY (SELECT 170141183460469231731687303715884105727::HUGEINT AS v) TO 'data/h128_a.parquet';
COPY (SELECT 1::HUGEINT AS v) TO 'data/h128_b.parquet';
SELECT 'HUGEINT MAX+1, 2 files, 32t' AS c, SUM(v) FROM read_parquet(['data/h128_a.parquet','data/h128_b.parquet']);
SELECT 'HUGEINT MAX+1, 1 file (union all), 32t' AS c, SUM(v) FROM (SELECT * FROM 'data/h128_a.parquet' UNION ALL SELECT * FROM 'data/h128_b.parquet');
SET threads=1;
SELECT 'HUGEINT MAX+1, 2 files, 1t' AS c, SUM(v) FROM read_parquet(['data/h128_a.parquet','data/h128_b.parquet']);
SET threads=32;
-- ===== 2. decimals =====
SELECT 'd38_over 32t' AS c, SUM(v) AS s, typeof(SUM(v)) AS t FROM 'data/d38_over/*.parquet';
SELECT 'd38_i128 32t' AS c, SUM(v) AS s FROM 'data/d38_i128/*.parquet';
SELECT 'd38_partial 32t' AS c, SUM(v) AS s FROM 'data/d38_partial/*.parquet';
SET threads=1;
SELECT 'd38_over 1t' AS c, SUM(v) AS s FROM 'data/d38_over/*.parquet';
SELECT 'd38_i128 1t' AS c, SUM(v) AS s FROM 'data/d38_i128/*.parquet';
SELECT 'd38_partial 1t' AS c, SUM(v) AS s FROM 'data/d38_partial/*.parquet';
SET threads=32;
SELECT 'd38_i128 avg' AS c, AVG(v) AS a, typeof(AVG(v)) AS t FROM 'data/d38_i128/*.parquet';
SELECT 'd38_partial avg' AS c, AVG(v) AS a FROM 'data/d38_partial/*.parquet';
SELECT 'd38 a + b' AS c, a + b FROM (SELECT v a, v b FROM 'data/d38_i128/*.parquet' LIMIT 1);
SELECT 'd38 a * b' AS c, a * b FROM (SELECT v a, v b FROM 'data/d38_i128/*.parquet' LIMIT 1);
SELECT 'd38 a + 1' AS c, a + 1 FROM (SELECT v a FROM 'data/d38_i128/*.parquet' LIMIT 1);
SELECT 'd38 a * 10' AS c, a * 10 FROM (SELECT v a FROM 'data/d38_i128/*.parquet' LIMIT 1);
SELECT 'fold d38 + 1' AS c, 99999999999999999999999999999999999999 + 1, typeof(99999999999999999999999999999999999999);
SELECT 'fold d38 * d38' AS c, 99999999999999999999999999999999999999 * 99999999999999999999999999999999999999;
DESCRIBE SELECT * FROM 'data/d76_over/*.parquet';
SELECT 'd76_over' AS c, SUM(v) FROM 'data/d76_over/*.parquet';
SELECT 'd76_i256' AS c, SUM(v) FROM 'data/d76_i256/*.parquet';
SELECT 'CAST 38 digits' AS c, CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0));
SELECT 'CAST 39 digits' AS c, CAST('999999999999999999999999999999999999999' AS DECIMAL(38,0));
SELECT 'TRY_CAST 39 digits' AS c, TRY_CAST('999999999999999999999999999999999999999' AS DECIMAL(38,0));
SELECT 'CAST uint256 max -> DECIMAL(38,0)' AS c, CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS DECIMAL(38,0));
SELECT 'TRY_CAST uint256 max -> DECIMAL(38,0)' AS c, TRY_CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS DECIMAL(38,0));
SELECT 'CAST uint256 max -> HUGEINT' AS c, CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS HUGEINT);
SELECT 'CAST uint256 max -> UHUGEINT' AS c, CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UHUGEINT);
SELECT 'CAST 76 digits -> DECIMAL(76,0)' AS c, CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0));
SELECT 'TRY_CAST 7.9 -> DECIMAL(38,0)' AS c, TRY_CAST('7.9' AS DECIMAL(38,0));
-- ===== 3. nuthatch pattern =====
CREATE VIEW credits AS SELECT * FROM 'data/credits/*.parquet';
CREATE VIEW debits AS SELECT * FROM 'data/debits/*.parquet';
WITH m AS (
  SELECT party, TRY_CAST(amount AS DECIMAL(38,0)) AS c, CAST('0' AS DECIMAL(38,0)) AS d,
         CASE WHEN TRY_CAST(amount AS DECIMAL(38,0)) IS NULL THEN 1 ELSE 0 END AS ov FROM credits
  UNION ALL
  SELECT party, CAST('0' AS DECIMAL(38,0)), TRY_CAST(amount AS DECIMAL(38,0)),
         CASE WHEN TRY_CAST(amount AS DECIMAL(38,0)) IS NULL THEN 1 ELSE 0 END FROM debits
)
SELECT 'nuthatch today 38' AS c, party, SUM(c) - SUM(d) AS balance, MAX(ov) AS _overflow FROM m GROUP BY party ORDER BY party;
WITH m AS (
  SELECT party, CAST(amount AS DECIMAL(38,0)) AS c, CAST('0' AS DECIMAL(38,0)) AS d FROM credits
  UNION ALL
  SELECT party, CAST('0' AS DECIMAL(38,0)), CAST(amount AS DECIMAL(38,0)) FROM debits
)
SELECT 'union CAST 38' AS c, party, SUM(c) - SUM(d) AS balance FROM m GROUP BY party ORDER BY party;
WITH m AS (
  SELECT party, CAST(amount AS DECIMAL(76,0)) AS c FROM credits
)
SELECT 'union CAST 76' AS c, party, SUM(c) FROM m GROUP BY party ORDER BY party;
SELECT 'SUM over VARCHAR' AS c, party, SUM(amount) FROM credits GROUP BY party ORDER BY party;
WITH m AS (
  SELECT party, CAST(amount AS HUGEINT) AS c, 0::HUGEINT AS d FROM credits WHERE party IN ('small','neg','fits76','partial_over')
  UNION ALL
  SELECT party, 0::HUGEINT, CAST(amount AS HUGEINT) FROM debits WHERE party IN ('small','neg','fits76','partial_over')
)
SELECT 'union HUGEINT, parties that fit' AS c, party, SUM(c) - SUM(d) AS balance FROM m GROUP BY party ORDER BY party;
