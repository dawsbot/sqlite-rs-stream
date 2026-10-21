//! In-process replicator: tails a database's WAL, ships each batch of
//! committed transactions to the replica as an L0 LTX file, owns
//! checkpointing, and compacts/snapshots/prunes the replica.
//!
//! Consistency model (same idea as Litestream):
//! - A read transaction is held open on our own connection at all times
//!   except while we checkpoint. While it is open SQLite cannot reset the WAL
//!   behind our back, so frames we have not shipped cannot disappear.
//! - We checkpoint ourselves: take the write lock on a second connection, ship
//!   the WAL tail, run a PASSIVE checkpoint, release. If the WAL was fully
//!   backfilled the next writer restarts it, which bumps salt-1 by exactly one
//!   — that restart is expected and replication continues incrementally.
//! - Any other WAL change we cannot account for produces a full snapshot as
//!   the next L0 file, which is always safe to apply.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use rusqlite::Connection;
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::ltx::{self, Decoder, Encoder, Txid};
use crate::replica::{FileInfo, Replica, SNAPSHOT_LEVEL, restore_plan};
use crate::wal::{self, ScanError, WalHeader};

#[derive(Debug, Clone)]
pub struct Options {
    /// How often the background task ships new WAL frames.
    pub sync_interval: Duration,
    /// Checkpoint once the WAL holds at least this many frames.
    pub checkpoint_frames: u64,
    /// Also checkpoint at least this often while the WAL is non-empty.
    pub checkpoint_interval: Duration,
    /// Compaction interval for levels 1, 2, ... (level 0 is raw syncs).
    pub compaction_levels: Vec<Duration>,
    /// How often a full snapshot is written to level 9.
    pub snapshot_interval: Duration,
    /// Snapshots older than this are deleted (the newest is always kept).
    pub snapshot_retention: Duration,
    /// Files already covered by the next level up are kept this long.
    pub l0_retention: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync_interval: Duration::from_secs(1),
            checkpoint_frames: 1000,
            checkpoint_interval: Duration::from_secs(60),
            compaction_levels: vec![Duration::from_secs(30), Duration::from_secs(5 * 60), Duration::from_secs(60 * 60)],
            snapshot_interval: Duration::from_secs(24 * 60 * 60),
            snapshot_retention: Duration::from_secs(24 * 60 * 60),
            l0_retention: Duration::from_secs(5 * 60),
        }
    }
}

/// What a sync shipped.
#[derive(Debug, Clone)]
pub struct SyncResult {
    pub txid: u64,
    pub pages: usize,
    pub snapshot: bool,
    pub size: u64,
}

/// Replication position: the last TXID shipped and where it ended in the WAL.
#[derive(Debug, Clone, Copy)]
struct Pos {
    txid: u64,
    salt1: u32,
    salt2: u32,
    /// WAL offset just past the last shipped commit frame.
    wal_end: u64,
    commit: u32,
}

struct Prepared {
    file: NamedTempFile,
    pos: Pos,
    wal_pages: BTreeMap<u32, u64>,
    pages: usize,
    snapshot: bool,
    /// Continued into a new WAL generation after our own checkpoint.
    restarted: bool,
}

struct Inner {
    wal_path: PathBuf,
    db_file: File,
    /// Holds the long-lived read transaction.
    conn: Connection,
    /// Takes the write lock while we checkpoint.
    writer: Connection,
    page_size: u32,
    reading: bool,
    pos: Option<Pos>,
    /// Committed page -> frame offset for the current WAL, up to `pos.wal_end`.
    wal_pages: BTreeMap<u32, u64>,
    /// Set after a checkpoint that fully backfilled the WAL.
    expect_restart: bool,
    last_checkpoint: Instant,
}

pub struct Replicator {
    inner: Arc<Mutex<Inner>>,
    /// Serializes syncs, checkpoints and maintenance.
    op_lock: tokio::sync::Mutex<()>,
    replica: Replica,
    opts: Options,
    staging: TempDir,
    syncs: AtomicU64,
    snapshots: AtomicU64,
    checkpoints: AtomicU64,
    wal_restarts: AtomicU64,
}

/// Counters since `open`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// L0 files shipped.
    pub syncs: u64,
    /// L0 files that were full snapshots (initial sync or WAL discontinuity).
    pub snapshots: u64,
    /// Checkpoints that ran.
    pub checkpoints: u64,
    /// WAL restarts after our own checkpoint, followed without a snapshot.
    pub wal_restarts: u64,
}

fn open_conn(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
    conn.busy_timeout(Duration::from_secs(5))?;
    let mode: String = conn.query_row("PRAGMA journal_mode = wal", [], |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        bail!("could not enable WAL mode (journal_mode={mode})");
    }
    conn.execute_batch("PRAGMA wal_autocheckpoint = 0;")?;
    Ok(conn)
}

impl Replicator {
    /// Opens `db_path` for replication. The database is created if needed and
    /// switched to WAL mode. Nothing is shipped until `sync` or `start`.
    pub async fn open(db_path: impl AsRef<Path>, replica: Replica, opts: Options) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        let inner = tokio::task::spawn_blocking(move || -> Result<Inner> {
            let conn = open_conn(&db_path)?;
            // Same bookkeeping table Litestream uses; writing it guarantees the
            // WAL exists and has a valid header.
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
                 INSERT INTO _litestream_seq (id, seq) VALUES (1, 1)
                   ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            )?;
            let page_size: u32 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
            let writer = open_conn(&db_path)?;
            let mut wal_path = db_path.clone().into_os_string();
            wal_path.push("-wal");
            let mut inner = Inner {
                wal_path: wal_path.into(),
                db_file: File::open(&db_path)?,
                conn,
                writer,
                page_size,
                reading: false,
                pos: None,
                wal_pages: BTreeMap::new(),
                expect_restart: false,
                last_checkpoint: Instant::now(),
            };
            inner.begin_read()?;
            Ok(inner)
        })
        .await??;
        let staging = tempfile::Builder::new().prefix("sqlite-rs-stream-").tempdir()?;
        Ok(Replicator {
            inner: Arc::new(Mutex::new(inner)),
            op_lock: Default::default(),
            replica,
            opts,
            staging,
            syncs: AtomicU64::new(0),
            snapshots: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
            wal_restarts: AtomicU64::new(0),
        })
    }

    pub fn replica(&self) -> &Replica {
        &self.replica
    }

    pub fn stats(&self) -> Stats {
        Stats {
            syncs: self.syncs.load(Ordering::Relaxed),
            snapshots: self.snapshots.load(Ordering::Relaxed),
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
            wal_restarts: self.wal_restarts.load(Ordering::Relaxed),
        }
    }

    /// Last TXID shipped by this process, if any.
    pub fn txid(&self) -> Option<u64> {
        self.inner.lock().unwrap().pos.map(|p| p.txid)
    }

    /// Ships all committed WAL frames not yet replicated. Returns `None` when
    /// there was nothing new.
    pub async fn sync(&self) -> Result<Option<SyncResult>> {
        let _g = self.op_lock.lock().await;
        self.sync_locked().await
    }

    async fn sync_locked(&self) -> Result<Option<SyncResult>> {
        let needs_base = self.inner.lock().unwrap().pos.is_none();
        let base_txid = if needs_base { self.replica.max_txid().await? } else { 0 };

        let inner = self.inner.clone();
        let dir = self.staging.path().to_path_buf();
        let prepared = tokio::task::spawn_blocking(move || inner.lock().unwrap().prepare(base_txid, &dir)).await??;
        let Some(p) = prepared else { return Ok(None) };

        let txid = p.pos.txid;
        let restarted = p.restarted;
        let info = self.replica.put_file(0, txid, txid, p.file.path()).await?;
        let result = SyncResult { txid, pages: p.pages, snapshot: p.snapshot, size: info.size };
        self.inner.lock().unwrap().commit(p);
        self.syncs.fetch_add(1, Ordering::Relaxed);
        if result.snapshot {
            self.snapshots.fetch_add(1, Ordering::Relaxed);
        }
        if restarted {
            self.wal_restarts.fetch_add(1, Ordering::Relaxed);
        }
        debug!(txid = %Txid(txid), pages = result.pages, snapshot = result.snapshot, restarted, "synced");
        Ok(Some(result))
    }

    /// Ships the WAL, then copies it into the database file while writers are
    /// blocked so that no unshipped frame can be checkpointed away.
    pub async fn checkpoint(&self) -> Result<()> {
        let _g = self.op_lock.lock().await;
        // Ship most frames without blocking writers, then only the remainder
        // while holding the write lock.
        self.sync_locked().await?;

        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.lock().unwrap().lock_writers()).await??;
        let res = self.sync_locked().await;
        let inner = self.inner.clone();
        let ok = res.is_ok();
        tokio::task::spawn_blocking(move || inner.lock().unwrap().finish_checkpoint(ok)).await??;
        if ok {
            self.checkpoints.fetch_add(1, Ordering::Relaxed);
        }
        res.map(|_| ())
    }

    fn needs_checkpoint(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        let Some(pos) = inner.pos else { return false };
        let frames = pos.wal_end.saturating_sub(wal::HEADER_SIZE) / (wal::FRAME_HEADER_SIZE + inner.page_size as u64);
        frames >= self.opts.checkpoint_frames
            || (frames > 0 && inner.last_checkpoint.elapsed() >= self.opts.checkpoint_interval)
    }

    /// Merges files from `level - 1` that are newer than anything at `level`
    /// into one file at `level`.
    pub async fn compact(&self, level: u32) -> Result<Option<FileInfo>> {
        let _g = self.op_lock.lock().await;
        self.compact_locked(level).await
    }

    async fn compact_locked(&self, level: u32) -> Result<Option<FileInfo>> {
        if level == 0 || level >= SNAPSHOT_LEVEL {
            bail!("invalid compaction level {level}");
        }
        let src = self.replica.list(level - 1).await?;
        let dst = self.replica.list(level).await?;
        let dst_max = dst.iter().map(|f| f.max_txid).max();

        let mut run: Vec<FileInfo> = Vec::new();
        let mut cur = dst_max;
        for f in src {
            match cur {
                Some(c) if f.max_txid <= c => continue,
                Some(c) if f.min_txid > c + 1 => break,
                _ => {}
            }
            cur = Some(f.max_txid);
            run.push(f);
        }
        if run.is_empty() {
            return Ok(None);
        }
        let info = self.merge_and_upload(&run, level).await?;
        debug!(level, min = %Txid(info.min_txid), max = %Txid(info.max_txid), inputs = run.len(), "compacted");
        Ok(Some(info))
    }

    /// Writes a full snapshot of the latest replicated state to level 9.
    pub async fn snapshot(&self) -> Result<Option<FileInfo>> {
        let _g = self.op_lock.lock().await;
        self.snapshot_locked().await
    }

    async fn snapshot_locked(&self) -> Result<Option<FileInfo>> {
        let files = self.replica.list_all().await?;
        if files.is_empty() {
            return Ok(None);
        }
        let plan = restore_plan(&files, None)?;
        if plan.len() == 1 && plan[0].level == SNAPSHOT_LEVEL {
            return Ok(None); // already current
        }
        let info = self.merge_and_upload(&plan, SNAPSHOT_LEVEL).await?;
        info!(txid = %Txid(info.max_txid), size = info.size, "snapshot written");
        Ok(Some(info))
    }

    async fn merge_and_upload(&self, inputs: &[FileInfo], level: u32) -> Result<FileInfo> {
        let dir = tempfile::tempdir_in(self.staging.path())?;
        let mut paths = Vec::with_capacity(inputs.len());
        for (i, f) in inputs.iter().enumerate() {
            let p = dir.path().join(format!("{i}.ltx"));
            self.replica.download(f, &p).await?;
            paths.push(p);
        }
        let out = NamedTempFile::new_in(dir.path())?;
        let out_file = out.reopen()?;
        let hdr = tokio::task::spawn_blocking(move || -> Result<ltx::Header> {
            let decoders =
                paths.iter().map(|p| Ok(Decoder::new(BufReader::new(File::open(p)?))?)).collect::<Result<Vec<_>>>()?;
            let (w, hdr, _) = ltx::compact(decoders, BufWriter::new(out_file))?;
            w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
            Ok(hdr)
        })
        .await??;
        self.replica.put_file(level, hdr.min_txid, hdr.max_txid, out.path()).await
    }

    /// Deletes replica files no longer needed to restore the latest state.
    pub async fn enforce_retention(&self) -> Result<usize> {
        let _g = self.op_lock.lock().await;
        self.enforce_retention_locked().await
    }

    async fn enforce_retention_locked(&self) -> Result<usize> {
        let now = Utc::now();
        let older_than = |f: &FileInfo, d: Duration| now - f.created_at > chrono::Duration::from_std(d).unwrap();
        let mut doomed: Vec<FileInfo> = Vec::new();

        let snaps = self.replica.list(SNAPSHOT_LEVEL).await?;
        let newest = snaps.iter().map(|f| f.max_txid).max();
        let (old_snaps, kept): (Vec<_>, Vec<_>) =
            snaps.into_iter().partition(|f| Some(f.max_txid) != newest && older_than(f, self.opts.snapshot_retention));
        doomed.extend(old_snaps);
        let oldest_kept_snapshot = kept.iter().map(|f| f.max_txid).min();

        let max_level = self.opts.compaction_levels.len() as u32;
        let mut levels = Vec::new();
        for level in 0..=max_level {
            levels.push(self.replica.list(level).await?);
        }
        for level in 0..=max_level as usize {
            if level < max_level as usize {
                // Covered by the next level up and past its grace period.
                let Some(covered) = levels[level + 1].iter().map(|f| f.max_txid).max() else { continue };
                let grace = if level == 0 { self.opts.l0_retention } else { self.opts.compaction_levels[level] };
                doomed.extend(levels[level].iter().filter(|f| f.max_txid <= covered && older_than(f, grace)).cloned());
            } else if let Some(snap) = oldest_kept_snapshot {
                doomed.extend(levels[level].iter().filter(|f| f.max_txid <= snap).cloned());
            }
        }
        self.replica.delete(&doomed).await?;
        if !doomed.is_empty() {
            debug!(n = doomed.len(), "retention removed files");
        }
        Ok(doomed.len())
    }

    /// Runs replication in a background task until `ReplicatorHandle::stop`.
    pub fn start(self: Arc<Self>) -> ReplicatorHandle {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let this = self.clone();
        let task = tokio::spawn(async move {
            let mut last_compaction = vec![Instant::now(); this.opts.compaction_levels.len()];
            let mut last_snapshot: Option<Instant> = None;
            let mut ticker = tokio::time::interval(this.opts.sync_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = stop_rx.changed() => break,
                }
                if let Err(e) = this.sync().await {
                    warn!(error = %format!("{e:#}"), "sync failed");
                    continue;
                }
                if this.needs_checkpoint()
                    && let Err(e) = this.checkpoint().await
                {
                    warn!(error = %format!("{e:#}"), "checkpoint failed");
                }
                let mut changed = false;
                for (i, last) in last_compaction.iter_mut().enumerate() {
                    if last.elapsed() >= this.opts.compaction_levels[i] {
                        *last = Instant::now();
                        match this.compact(i as u32 + 1).await {
                            Ok(r) => changed |= r.is_some(),
                            Err(e) => warn!(level = i + 1, error = %format!("{e:#}"), "compaction failed"),
                        }
                    }
                }
                if last_snapshot.is_none_or(|t| t.elapsed() >= this.opts.snapshot_interval) {
                    match this.snapshot().await {
                        Ok(r) => {
                            last_snapshot = Some(Instant::now());
                            changed |= r.is_some();
                        }
                        Err(e) => warn!(error = %format!("{e:#}"), "snapshot failed"),
                    }
                }
                if changed && let Err(e) = this.enforce_retention().await {
                    warn!(error = %format!("{e:#}"), "retention failed");
                }
            }
            if let Err(e) = this.sync().await {
                warn!(error = %format!("{e:#}"), "final sync failed");
            }
        });
        ReplicatorHandle { stop: stop_tx, task }
    }
}

pub struct ReplicatorHandle {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ReplicatorHandle {
    /// Stops the background task after one final sync.
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.task.await?;
        Ok(())
    }
}

enum Plan {
    Snapshot(&'static str),
    Incremental(u64),
}

impl Inner {
    fn begin_read(&mut self) -> Result<()> {
        if !self.reading {
            self.conn.execute_batch("BEGIN")?;
            self.conn.query_row("SELECT COUNT(1) FROM _litestream_seq", [], |r| r.get::<_, i64>(0))?;
            self.reading = true;
        }
        Ok(())
    }

    fn end_read(&mut self) -> Result<()> {
        if self.reading {
            self.conn.execute_batch("ROLLBACK")?;
            self.reading = false;
        }
        Ok(())
    }

    fn lock_writers(&mut self) -> Result<()> {
        // Take the write lock *before* dropping our read transaction. In the
        // other order, while we wait for the lock another connection's
        // checkpoint can backfill everything and the next write restarts the
        // WAL, discarding frames we have not shipped.
        self.writer.execute_batch("BEGIN IMMEDIATE").context("acquire write lock for checkpoint")?;
        self.end_read()
    }

    fn finish_checkpoint(&mut self, synced: bool) -> Result<()> {
        let res = if synced {
            self.conn
                .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))
                })
                .map_err(anyhow::Error::from)
        } else {
            Ok((1, 0, 0))
        };
        self.writer.execute_batch("ROLLBACK")?;
        self.begin_read()?;
        self.last_checkpoint = Instant::now();
        let (busy, log, done) = res?;
        // Fully backfilled: the next writer will restart the WAL.
        self.expect_restart = busy == 0 && log > 0 && log == done;
        debug!(busy, log, done, "checkpoint");
        Ok(())
    }

    fn plan(&self, hdr: Option<&WalHeader>) -> Plan {
        let Some(pos) = self.pos else { return Plan::Snapshot("initial sync") };
        let Some(h) = hdr else {
            // No WAL: if we had never seen one either, nothing changed.
            return if pos.salt1 == 0 && pos.salt2 == 0 { Plan::Incremental(0) } else { Plan::Snapshot("wal removed") };
        };
        if pos.salt1 == 0 && pos.salt2 == 0 {
            Plan::Incremental(wal::HEADER_SIZE)
        } else if (h.salt1, h.salt2) == (pos.salt1, pos.salt2) {
            Plan::Incremental(pos.wal_end.max(wal::HEADER_SIZE))
        } else if self.expect_restart && h.salt1 == pos.salt1.wrapping_add(1) {
            Plan::Incremental(wal::HEADER_SIZE)
        } else {
            Plan::Snapshot("wal restarted unexpectedly")
        }
    }

    fn prepare(&mut self, base_txid: u64, dir: &Path) -> Result<Option<Prepared>> {
        let hdr = wal::read_header(&self.wal_path)?;
        if let Some(h) = &hdr
            && h.page_size != self.page_size
        {
            bail!("wal page size {} does not match database page size {}", h.page_size, self.page_size);
        }
        let wal_file = match &hdr {
            Some(_) => Some(File::open(&self.wal_path)?),
            None => None,
        };

        let mut plan = self.plan(hdr.as_ref());
        let mut scan = wal::Scan::default();
        let mut start = wal::HEADER_SIZE;
        if let (Plan::Incremental(off), Some(h), Some(f)) = (&plan, &hdr, &wal_file) {
            start = *off;
            match wal::scan(f, h, start) {
                Ok(s) => scan = s,
                Err(ScanError::PrevFrameMismatch) => plan = Plan::Snapshot("wal overwritten"),
                Err(ScanError::Io(e)) => return Err(e.into()),
            }
        }
        if let Plan::Snapshot(reason) = plan {
            debug!(
                reason,
                wal = ?hdr.map(|h| (h.salt1, h.salt2, h.checkpoint_seq)),
                pos = ?self.pos.map(|p| (p.salt1, p.salt2, p.wal_end)),
                expect_restart = self.expect_restart,
                "snapshotting"
            );
            start = wal::HEADER_SIZE;
            scan = match (&hdr, &wal_file) {
                (Some(h), Some(f)) => match wal::scan(f, h, start) {
                    Ok(s) => s,
                    Err(ScanError::PrevFrameMismatch) => unreachable!("scan from header never checks prev frame"),
                    Err(ScanError::Io(e)) => return Err(e.into()),
                },
                _ => wal::Scan::default(),
            };
        }
        let snapshot = matches!(plan, Plan::Snapshot(_));
        if !snapshot && scan.end == 0 {
            return Ok(None);
        }

        let page_size = self.page_size;
        let commit =
            if scan.commit > 0 { scan.commit } else { (self.db_file.metadata()?.len() / page_size as u64) as u32 };
        if commit == 0 {
            return Ok(None); // empty database, nothing to replicate yet
        }

        // Pages from the whole current WAL that are visible after this sync.
        let mut wal_pages =
            if snapshot || start == wal::HEADER_SIZE { BTreeMap::new() } else { self.wal_pages.clone() };
        wal_pages.extend(scan.pages.iter().map(|(&k, &v)| (k, v)));
        wal_pages.retain(|&pgno, _| pgno <= commit);

        let lock = ltx::lock_pgno(page_size);
        let pgnos: BTreeSet<u32> = if snapshot {
            (1..=commit).filter(|&p| p != lock).collect()
        } else {
            let prev_commit = self.pos.map(|p| p.commit).unwrap_or(0);
            scan.pages.keys().copied().chain((prev_commit + 1..=commit).filter(|&p| p != lock)).collect()
        };

        let txid = self.pos.map(|p| p.txid).unwrap_or(base_txid) + 1;
        let (wal_offset, wal_size, salt1, salt2) = match &hdr {
            Some(h) => (start, if scan.end > 0 { scan.end - start } else { 0 }, h.salt1, h.salt2),
            None => (0, 0, 0, 0),
        };
        let header = ltx::Header {
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size,
            commit,
            min_txid: txid,
            max_txid: txid,
            timestamp: Utc::now().timestamp_millis(),
            wal_offset: wal_offset as i64,
            wal_size: wal_size as i64,
            wal_salt1: salt1,
            wal_salt2: salt2,
            ..Default::default()
        };

        let file = NamedTempFile::new_in(dir)?;
        let mut enc = Encoder::new(BufWriter::new(file.reopen()?), header)?;
        let mut data = vec![0u8; page_size as usize];
        for &pgno in &pgnos {
            match (wal_pages.get(&pgno), &wal_file) {
                (Some(&off), Some(f)) => wal::read_frame_page(f, off, &mut data)?,
                _ => self
                    .db_file
                    .read_exact_at(&mut data, (pgno as u64 - 1) * page_size as u64)
                    .with_context(|| format!("read database page {pgno}"))?,
            }
            enc.encode_page(pgno, &data)?;
        }
        let (w, _) = enc.finish(0)?;
        w.into_inner().map_err(|e| e.into_error())?.sync_all()?;

        let wal_end = if scan.end > 0 { scan.end } else { start };
        Ok(Some(Prepared {
            file,
            pos: Pos { txid, salt1, salt2, wal_end: if hdr.is_some() { wal_end } else { 0 }, commit },
            wal_pages,
            pages: pgnos.len(),
            snapshot,
            restarted: !snapshot && self.pos.is_some_and(|p| (p.salt1, p.salt2) != (salt1, salt2)),
        }))
    }

    fn commit(&mut self, p: Prepared) {
        self.pos = Some(p.pos);
        self.wal_pages = p.wal_pages;
        self.expect_restart = false;
    }
}
