//! `gate-duck <nest> <set.tsv> <out-dir>`: every statement of a nuthatch release-gate query set
//! through DuckDB, set up over the nest as `engine-views` sets it up, each answer written as the
//! body nuthatch's `/sql` returns. nuthatch's `scripts/gate/reference.sh` compares these with the
//! binary under test (nuthatch#1796), so an answer is checked against more than the last release.
//!
//! `<id>.json` holds an answer, `<id>.err` why DuckDB would not run the statement, and `views.err`
//! each authored view DuckDB would not define. Nothing is skipped silently.

use std::path::Path;

use serde_json::{json, Value};

use crate::df_views::load_nest_allowing_no_views;

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(400).collect()
}

pub fn run(nest: &str, set: &str, out: &str) -> anyhow::Result<()> {
    let out = Path::new(out);
    std::fs::create_dir_all(out)?;
    let nest = load_nest_allowing_no_views(Path::new(nest))?;
    let conn = crate::engine_views::duck(&nest)?;
    let mut view_faults = String::new();
    for v in &nest.views {
        if let Err(e) = conn.execute_batch(&v.text) {
            view_faults.push_str(&format!(
                "{}\t{}\t{}\n",
                v.name,
                v.file,
                first_line(&e.to_string())
            ));
        }
    }
    std::fs::write(out.join("views.err"), &view_faults)?;

    let (mut answered, mut refused) = (0, 0);
    for line in std::fs::read_to_string(set)?.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut cols = line.splitn(4, '\t');
        let (Some(id), Some(_), Some(_), Some(sql)) =
            (cols.next(), cols.next(), cols.next(), cols.next())
        else {
            anyhow::bail!("malformed line in {set}: {}", first_line(line));
        };
        let _ = std::fs::remove_file(out.join(format!("{id}.json")));
        let _ = std::fs::remove_file(out.join(format!("{id}.err")));
        match crate::encode_parity::nuthatch_rows(&conn, sql) {
            Ok(Value::Array(rows)) => {
                answered += 1;
                println!("ok\t{id}\t{}", rows.len());
                let body = json!({ "count": rows.len(), "rows": rows });
                std::fs::write(
                    out.join(format!("{id}.json")),
                    serde_json::to_string(&body)?,
                )?;
            }
            Ok(other) => anyhow::bail!("{id}: not a row array: {other}"),
            Err(e) => {
                refused += 1;
                let why = first_line(&e.to_string());
                println!("error\t{id}\t{why}");
                std::fs::write(out.join(format!("{id}.err")), format!("{why}\n"))?;
            }
        }
    }
    println!(
        "GATE-DUCK\tanswered={answered}\trefused={refused}\tviews_not_defined={}",
        view_faults.lines().count()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    /// A nest that authors no views is ordinary: its statements read the sealed tables directly.
    #[test]
    fn a_nest_with_no_views_is_answered() {
        let dir = tempfile::tempdir().unwrap();
        let nest = dir.path().join("nest");
        std::fs::create_dir_all(nest.join("segments")).unwrap();
        std::fs::create_dir_all(nest.join("views")).unwrap();
        std::fs::write(nest.join("schema.json"), r#"{"tables":[]}"#).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_number",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![7_u64, 8, 9]))],
        )
        .unwrap();
        let f = std::fs::File::create(nest.join("segments/transfer-0000000001.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let set = dir.path().join("set.tsv");
        std::fs::write(&set, "n\tt\tsrc\tSELECT count(*) AS n FROM transfer\n").unwrap();
        let out = dir.path().join("out");

        super::run(
            nest.to_str().unwrap(),
            set.to_str().unwrap(),
            out.to_str().unwrap(),
        )
        .unwrap();

        let body = std::fs::read_to_string(out.join("n.json")).unwrap();
        assert!(body.contains(r#""count":1"#), "{body}");
        assert!(body.contains('3'), "{body}");
    }
}
