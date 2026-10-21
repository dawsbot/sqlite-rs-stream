use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use tracing::{error, info};

use sqlite_rs_stream::ltx::{self, Txid};
use sqlite_rs_stream::{Leaser, Options, Replica, Replicator, RestoreOptions, restore};

#[derive(Parser)]
#[command(version, about = "Litestream-compatible SQLite replication to S3 or a directory")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Continuously replicate a database until interrupted.
    Replicate {
        db: PathBuf,
        /// Replica URL: a directory, file:///dir, or s3://bucket/prefix.
        replica: String,
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        sync_interval: Duration,
        /// Hold a lease on the replica so only one host replicates.
        #[arg(long)]
        lease: bool,
    },
    /// Rebuild a database from a replica.
    Restore {
        replica: String,
        #[arg(short, long)]
        output: PathBuf,
        /// Restore up to this TXID (hex) instead of the latest.
        #[arg(long, value_parser = parse_txid)]
        txid: Option<u64>,
        /// Overwrite the output file if it exists.
        #[arg(long)]
        force: bool,
    },
    /// List the LTX files in a replica.
    Files { replica: String },
    /// Verify a local LTX file and print its header.
    Verify { file: PathBuf },
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().map_err(|_| format!("invalid duration: {s}"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(n)),
        "s" | "" => Ok(Duration::from_secs(n)),
        "m" => Ok(Duration::from_secs(n * 60)),
        "h" => Ok(Duration::from_secs(n * 3600)),
        _ => Err(format!("invalid duration unit in {s} (use ms, s, m, h)")),
    }
}

fn parse_txid(s: &str) -> Result<u64, String> {
    ltx::parse_txid(s).ok_or_else(|| format!("invalid txid: {s}"))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "sqlite_rs_stream=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    if let Err(e) = run(Cli::parse()).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Replicate { db, replica, sync_interval, lease } => {
            let replica = Replica::from_url(&replica)?;
            let leaser = lease.then(|| Leaser::new(replica.store().clone()));
            let mut held = match &leaser {
                Some(l) => Some(l.acquire().await?),
                None => None,
            };
            let opts = Options { sync_interval, ..Default::default() };
            let replicator = Arc::new(Replicator::open(&db, replica.clone(), opts).await?);
            info!(db = %db.display(), replica = replica.url(), "replicating");
            let handle = replicator.clone().start();

            let mut renew = tokio::time::interval(leaser.as_ref().map_or(Duration::from_secs(3600), |l| l.ttl / 3));
            renew.tick().await;
            let result = loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break Ok(()),
                    _ = renew.tick(), if leaser.is_some() => {
                        let l = leaser.as_ref().unwrap();
                        match l.renew(held.as_ref().unwrap()).await {
                            Ok(next) => held = Some(next),
                            Err(e) => break Err(anyhow::Error::from(e).context("lost replica lease")),
                        }
                    }
                }
            };
            handle.stop().await?;
            if let (Some(l), Some(lease)) = (&leaser, &held)
                && result.is_ok()
                && let Err(e) = l.release(lease).await
            {
                error!(error = %e, "release lease");
            }
            if let Some(txid) = replicator.txid() {
                info!(txid = %Txid(txid), "stopped");
            }
            result
        }
        Cmd::Restore { replica, output, txid, force } => {
            let replica = Replica::from_url(&replica)?;
            let res = restore(&replica, &output, &RestoreOptions { txid, overwrite: force }).await?;
            println!(
                "restored {} to txid {} ({} pages of {} bytes, {} files)",
                output.display(),
                Txid(res.txid),
                res.pages,
                res.page_size,
                res.files.len()
            );
            Ok(())
        }
        Cmd::Files { replica } => {
            let replica = Replica::from_url(&replica)?;
            let files = replica.list_all().await?;
            println!("{:<6} {:<16} {:<16} {:>12}  CREATED", "LEVEL", "MIN_TXID", "MAX_TXID", "SIZE");
            for f in files {
                println!(
                    "{:<6} {:<16} {:<16} {:>12}  {}",
                    f.level,
                    Txid(f.min_txid),
                    Txid(f.max_txid),
                    f.size,
                    f.created_at.format("%Y-%m-%dT%H:%M:%S%.3fZ")
                );
            }
            Ok(())
        }
        Cmd::Verify { file } => {
            let f = File::open(&file).with_context(|| format!("open {}", file.display()))?;
            let (h, t, pages) = match ltx::verify(BufReader::new(f)) {
                Ok(v) => v,
                Err(e) => bail!("{}: {e}", file.display()),
            };
            println!("{}: ok", file.display());
            println!("  txid:      {}-{}", Txid(h.min_txid), Txid(h.max_txid));
            println!("  page size: {}", h.page_size);
            println!("  commit:    {}", h.commit);
            println!("  pages:     {pages}");
            println!(
                "  wal:       offset={} size={} salt={:08x}/{:08x}",
                h.wal_offset, h.wal_size, h.wal_salt1, h.wal_salt2
            );
            println!("  checksum:  {:016x}", t.file_checksum);
            Ok(())
        }
    }
}
