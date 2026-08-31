//! DuckDB and DataFusion running the same question, so the answer can be checked and the time
//! compared.
//!
//! Both of them are **dev-only oracles**, not fallbacks. Nothing in this file is reachable from the
//! shipped library.

use std::path::Path;
use std::time::Instant;

pub type Rows = Vec<(String, i128)>;

/// The thread budget every engine in this file is held to.
///
/// **All three engines get the same number of threads, and this is not a nicety.** Burrmill bounds
/// its own parallelism as of roadmap 1.2c, so leaving DuckDB on a 32-core box's defaults would have
/// compared eight threads against thirty-two and called the difference an engine. It duly did: two
/// configurations went from 0.5x to 1.3x the moment the bound landed, which looked like a
/// regression and was a harness fault - the same shape as the 38,429-file glob that 1.1 was about.
///
/// A ratio is a statement about engines only when everything else is held equal, and parallelism is
/// now something to hold equal rather than something to inherit.
pub fn thread_budget() -> usize {
    std::env::var("THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| burrmill::Limits::default().max_threads)
}

/// The incumbent. `HUGEINT` is DuckDB's 128-bit integer and it **errors** on overflow rather than
/// wrapping, which is the behaviour Burrmill matches deliberately.
pub fn duckdb(seg: &Path) -> anyhow::Result<(Rows, u128)> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch(&format!("SET threads TO {};", thread_budget()))?;
    let glob = format!("{}/*.parquet", seg.display());
    conn.execute_batch(&format!("CREATE VIEW t AS SELECT * FROM read_parquet('{glob}');"))?;
    let sql = "SELECT addr, SUM(d)::VARCHAR AS net FROM (\
                 SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                 UNION ALL \
                 SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";
    let t = Instant::now();
    let mut stmt = conn.prepare(sql)?;
    let mut out = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let addr: String = r.get(0)?;
        let net: String = r.get(1)?;
        out.push((addr, net.parse::<i128>()?));
    }
    Ok((out, t.elapsed().as_millis()))
}

/// General DataFusion on the same shape: the "rent execution" arm.
///
/// The dialect difference is stated rather than papered over. DataFusion has no `HUGEINT`;
/// `DECIMAL(38,0)` is the same 128-bit width. Note also that it reads Parquet strings back as
/// `Utf8View` where the writer wrote `Utf8`, so the result columns are cast rather than assumed -
/// otherwise the comparison would be about string layouts instead of about the answer.
pub async fn datafusion(seg: &Path) -> anyhow::Result<(Rows, u128)> {
    use datafusion::prelude::*;
    let mut config = SessionConfig::new();
    config = config.with_target_partitions(thread_budget());
    let ctx = SessionContext::new_with_config(config);
    // The **directory**, not a file: with many segments, registering one would silently compare all
    // of DuckDB's rows against one of DataFusion's.
    ctx.register_parquet("t", seg.to_str().unwrap(), ParquetReadOptions::default()).await?;
    let sql = "SELECT addr, CAST(SUM(d) AS VARCHAR) AS net FROM (\
                 SELECT \"to\" AS addr, TRY_CAST(\"value\" AS DECIMAL(38,0)) AS d FROM t \
                 UNION ALL \
                 SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS DECIMAL(38,0)) AS d FROM t\
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";
    let t = Instant::now();
    let batches = ctx.sql(sql).await?.collect().await?;
    let ms = t.elapsed().as_millis();

    let mut out = Vec::new();
    for b in batches {
        let col = |i: usize| -> anyhow::Result<datafusion::arrow::array::ArrayRef> {
            Ok(datafusion::arrow::compute::cast(
                b.column(i),
                &datafusion::arrow::datatypes::DataType::Utf8,
            )?)
        };
        let addr_arr = col(0)?;
        let net_arr = col(1)?;
        let addr = addr_arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("addr column will not cast to Utf8"))?;
        let net = net_arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("net column will not cast to Utf8"))?;
        for i in 0..b.num_rows() {
            out.push((addr.value(i).to_string(), net.value(i).parse::<i128>()?));
        }
    }
    Ok((out, ms))
}

/// Burrmill: the same question through the owned planner and the owned operator.
pub fn burrmill(seg: &Path) -> anyhow::Result<(Rows, u128, burrmill::FoldMetrics)> {
    let mut catalog = burrmill::Catalog::new();
    catalog.register(burrmill::SealedSegments::discover("t", seg)?);
    let db = burrmill::Burrmill::with_threads(catalog, thread_budget())?;
    let sql = "SELECT addr, SUM(d) AS net FROM (\
                 SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                 UNION ALL \
                 SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";
    let t = Instant::now();
    let answer = db.query(sql, burrmill::Limits::default())?;
    let ms = t.elapsed().as_millis();
    let rows = answer.rows().iter().map(|(k, v)| (k.to_string(), v)).collect();
    Ok((rows, ms, answer.metrics()))
}
