//! Rebuilds a SQLite database from a replica.

use std::fs::{self, File, OpenOptions};
use std::io::BufReader;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::ltx::Decoder;
use crate::replica::{FileInfo, Replica, restore_plan};

#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// Restore up to and including this TXID instead of the latest.
    pub txid: Option<u64>,
    /// Replace `output` if it already exists.
    pub overwrite: bool,
}

#[derive(Debug, Clone)]
pub struct RestoreInfo {
    pub txid: u64,
    pub page_size: u32,
    pub pages: u32,
    pub files: Vec<FileInfo>,
}

/// Downloads the files in the restore plan one at a time and writes their
/// pages into `output`. Memory use is bounded by one page, not the database.
pub async fn restore(replica: &Replica, output: &Path, opts: &RestoreOptions) -> Result<RestoreInfo> {
    if output.exists() && !opts.overwrite {
        bail!("output database already exists: {}", output.display());
    }
    let files = replica.list_all().await?;
    let plan = restore_plan(&files, opts.txid)?;

    let parent = output.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let staging = tempfile::Builder::new().prefix(".restore-").tempdir_in(parent)?;
    let db_tmp: PathBuf = staging.path().join("db");
    let db = OpenOptions::new().read(true).write(true).create_new(true).open(&db_tmp)?;

    let mut page_size = 0u32;
    let mut commit = 0u32;
    for (i, info) in plan.iter().enumerate() {
        let ltx_path = staging.path().join(format!("{i}.ltx"));
        replica.download(info, &ltx_path).await?;
        let db = db.try_clone()?;
        let (ps, c) = tokio::task::spawn_blocking(move || apply_ltx(&db, &ltx_path))
            .await?
            .with_context(|| format!("apply {} (level {})", info.filename(), info.level))?;
        if page_size != 0 && ps != page_size {
            bail!("page size changed from {page_size} to {ps} at {}", info.filename());
        }
        page_size = ps;
        commit = c;
    }
    db.set_len(commit as u64 * page_size as u64)?;
    db.sync_all()?;
    drop(db);

    for suffix in ["-wal", "-shm"] {
        let mut p = output.as_os_str().to_owned();
        p.push(suffix);
        let _ = fs::remove_file(PathBuf::from(p));
    }
    fs::rename(&db_tmp, output)?;
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }

    Ok(RestoreInfo { txid: plan.last().unwrap().max_txid, page_size, pages: commit, files: plan })
}

/// Writes every page of one LTX file into `db` and verifies its checksum.
/// Returns (page size, commit).
fn apply_ltx(db: &File, ltx_path: &Path) -> Result<(u32, u32)> {
    let mut dec = Decoder::new(BufReader::new(File::open(ltx_path)?))?;
    let hdr = dec.header().clone();
    let mut data = vec![0u8; hdr.page_size as usize];
    while let Some(pgno) = dec.next_page(&mut data)? {
        db.write_all_at(&data, (pgno as u64 - 1) * hdr.page_size as u64)?;
    }
    dec.finish()?;
    // The lock page is never stored, so extend explicitly; the final set_len
    // trims any excess.
    let len = hdr.commit as u64 * hdr.page_size as u64;
    if db.metadata()?.len() < len {
        db.set_len(len)?;
    }
    Ok((hdr.page_size, hdr.commit))
}
