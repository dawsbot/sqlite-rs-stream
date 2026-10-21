//! Rough sync/restore timings against a local directory replica.
//!
//!   cargo run --release --example bench -- 100000
use std::time::Instant;

use rusqlite::Connection;
use sqlite_rs_stream::{Options, Replica, Replicator, RestoreOptions, restore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let rows: i64 = std::env::args().nth(1).map(|s| s.parse()).transpose()?.unwrap_or(100_000);
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("bench.db");
    let conn = Connection::open(&db)?;
    conn.execute_batch(
        "PRAGMA journal_mode = wal; PRAGMA wal_autocheckpoint = 0;
         CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, n INTEGER);",
    )?;
    let replica = Replica::from_url(dir.path().join("replica").to_str().unwrap())?;
    let r = Replicator::open(&db, replica.clone(), Options::default()).await?;
    r.sync().await?;

    conn.execute_batch("BEGIN")?;
    let mut stmt = conn.prepare("INSERT INTO t (name, n) VALUES (?1, ?2)")?;
    for i in 0..rows {
        stmt.execute((format!("row-{i}"), i))?;
    }
    drop(stmt);
    conn.execute_batch("COMMIT")?;
    let wal_mb = std::fs::metadata(dir.path().join("bench.db-wal"))?.len() as f64 / 1e6;

    let t = Instant::now();
    let res = r.sync().await?.expect("changes");
    let sync = t.elapsed();

    let out = dir.path().join("restored.db");
    let t = Instant::now();
    restore(&replica, &out, &RestoreOptions::default()).await?;
    let rest = t.elapsed();

    let n: i64 = Connection::open(&out)?.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?;
    assert_eq!(n, rows);
    let rate = |d: std::time::Duration| rows as f64 / d.as_secs_f64() / 1000.0;
    println!("{rows} rows, {wal_mb:.2} MB WAL, {} pages, {:.2} MB LTX", res.pages, res.size as f64 / 1e6);
    println!("sync():    {:>7.1} ms  {:>6.0}k rows/s", sync.as_secs_f64() * 1e3, rate(sync));
    println!("restore(): {:>7.1} ms  {:>6.0}k rows/s", rest.as_secs_f64() * 1e3, rate(rest));
    Ok(())
}
