//! The 6.6 memory gate in a binary shaped like nuthatch after the swap: Burrmill with `datafusion`
//! and nothing else linked. The bench binary carries DuckDB and the umbrella crate too, and the code
//! pages a query touches in it count in its RSS: 55 MB file-backed against the bare fold's 21, with
//! the anonymous memory level. `cargo run --release --example hosted_fold --features datafusion <segments>`
//!
//! Linux only for the split, which samples `/proc/self/statm` and keeps the largest reading.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SQL: &str = "SELECT addr, SUM(d) AS net FROM (\
    SELECT \"to\" AS addr, CAST(\"value\" AS DECIMAL(38,0)) AS d FROM t \
    UNION ALL \
    SELECT \"from\" AS addr, -CAST(\"value\" AS DECIMAL(38,0)) AS d FROM t\
    ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args().nth(1).ok_or("usage: hosted_fold <segments>")?;
    let peak = Arc::new(Mutex::new((0usize, 0usize)));
    let p = Arc::clone(&peak);
    std::thread::spawn(move || loop {
        if let Ok(t) = std::fs::read_to_string("/proc/self/statm") {
            let f: Vec<usize> = t.split_whitespace().filter_map(|x| x.parse().ok()).collect();
            let mut g = p.lock().unwrap();
            if f[1] * 4096 > g.0 {
                *g = (f[1] * 4096, f[2] * 4096);
            }
        }
        std::thread::sleep(Duration::from_millis(2));
    });
    let engine = burrmill::Engine::open_segments(std::path::Path::new(&dir))?;
    let t = Instant::now();
    let mut rows = 0;
    engine.sql_for_each(SQL, |b| {
        rows += b.num_rows();
        Ok(())
    })?;
    let ms = t.elapsed().as_millis();
    let hwm = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.to_string()))
        .unwrap_or_default();
    let (rss, file) = *peak.lock().unwrap();
    let mb = |b: usize| b / (1024 * 1024);
    println!(
        "HOSTED\tgroups={rows}\tms={ms}\t{}\tsampled_rss_mb={}\tfile_mb={}\tanon_mb={}",
        hwm.split_whitespace().collect::<Vec<_>>().join("="),
        mb(rss),
        mb(file),
        mb(rss - file)
    );
    Ok(())
}
