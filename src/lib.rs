//! Litestream-compatible SQLite replication that runs inside your process.
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! use std::sync::Arc;
//! use sqlite_rs_stream::{Options, Replica, Replicator};
//!
//! let replica = Replica::from_url("s3://my-bucket/app.db")?;
//! let replicator = Arc::new(Replicator::open("app.db", replica, Options::default()).await?);
//! let handle = replicator.clone().start();
//! // ... use app.db with any SQLite connection ...
//! replicator.sync().await?; // optional: force a sync now
//! handle.stop().await?;
//! # Ok(()) }
//! ```

pub mod db;
pub mod lease;
pub mod ltx;
pub mod replica;
pub mod restore;
pub mod wal;

pub use db::{Options, Replicator, ReplicatorHandle, Stats, SyncResult};
pub use lease::{Lease, LeaseError, Leaser};
pub use replica::{FileInfo, Layout, Replica};
pub use restore::{RestoreInfo, RestoreOptions, restore};
