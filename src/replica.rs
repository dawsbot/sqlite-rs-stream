//! Replica storage: LTX files laid out exactly like a Litestream replica on
//! any `object_store` backend (local directory, S3 and S3-compatible stores,
//! in-memory). Litestream uses a different key layout per backend; see
//! [`Layout`].

use std::path::Path as FsPath;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::ltx;

/// Litestream keeps full snapshots at this level.
pub const SNAPSHOT_LEVEL: u32 = 9;

/// Files larger than this are uploaded with multipart uploads.
const MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;
const MULTIPART_CHUNK: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInfo {
    pub level: u32,
    pub min_txid: u64,
    pub max_txid: u64,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

impl FileInfo {
    pub fn filename(&self) -> String {
        ltx::format_filename(self.min_txid, self.max_txid)
    }
}

/// Where LTX files live under the replica root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// `ltx/<level>/<file>` — Litestream's file, GCS, Azure, SFTP and WebDAV backends.
    LtxDir,
    /// `<level as %04x>/<file>` — Litestream's S3 and OSS backends.
    HexLevel,
}

impl Layout {
    fn level_dir(self, level: u32) -> String {
        match self {
            Layout::LtxDir => format!("ltx/{level}"),
            Layout::HexLevel => format!("{level:04x}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Replica {
    store: Arc<dyn ObjectStore>,
    url: String,
    layout: Layout,
}

impl Replica {
    pub fn new(store: Arc<dyn ObjectStore>, url: impl Into<String>, layout: Layout) -> Self {
        Replica { store, url: url.into(), layout }
    }

    /// Opens a replica from a URL:
    /// - `/some/dir` or `file:///some/dir` — a local directory
    /// - `s3://bucket/prefix` — S3 (credentials, region and endpoint from the
    ///   standard `AWS_*` environment variables)
    /// - `memory://` — in-process memory, for tests
    pub fn from_url(url: &str) -> Result<Self> {
        if let Some(rest) = url.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                bail!("s3 url requires a bucket: {url}");
            }
            let s3 = AmazonS3Builder::from_env().with_bucket_name(bucket).build().context("configure s3")?;
            let prefix = prefix.trim_matches('/');
            let store: Arc<dyn ObjectStore> =
                if prefix.is_empty() { Arc::new(s3) } else { Arc::new(PrefixStore::new(s3, prefix)) };
            return Ok(Self::new(store, url, Layout::HexLevel));
        }
        if url == "memory://" {
            return Ok(Self::new(Arc::new(InMemory::new()), url, Layout::LtxDir));
        }
        let dir = url.strip_prefix("file://").unwrap_or(url);
        if dir.contains("://") {
            bail!("unsupported replica url: {url}");
        }
        std::fs::create_dir_all(dir).with_context(|| format!("create replica dir {dir}"))?;
        let store = LocalFileSystem::new_with_prefix(dir).context("open replica dir")?;
        Ok(Self::new(Arc::new(store), url, Layout::LtxDir))
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    pub fn path(&self, level: u32, min_txid: u64, max_txid: u64) -> Path {
        Path::from(format!("{}/{}", self.layout.level_dir(level), ltx::format_filename(min_txid, max_txid)))
    }

    /// Lists LTX files at one level, sorted by TXID.
    pub async fn list(&self, level: u32) -> Result<Vec<FileInfo>> {
        let prefix = Path::from(self.layout.level_dir(level));
        let mut out: Vec<FileInfo> = self
            .store
            .list(Some(&prefix))
            .try_filter_map(|meta| async move {
                let name = meta.location.filename().unwrap_or_default();
                Ok(ltx::parse_filename(name).map(|(min_txid, max_txid)| FileInfo {
                    level,
                    min_txid,
                    max_txid,
                    size: meta.size,
                    created_at: meta.last_modified,
                }))
            })
            .try_collect()
            .await
            .with_context(|| format!("list level {level}"))?;
        out.sort_by_key(|f| (f.min_txid, f.max_txid));
        Ok(out)
    }

    /// Lists LTX files at every level, sorted by level then TXID.
    pub async fn list_all(&self) -> Result<Vec<FileInfo>> {
        let mut all = Vec::new();
        for level in 0..=SNAPSHOT_LEVEL {
            all.extend(self.list(level).await?);
        }
        Ok(all)
    }

    /// Highest TXID stored at any level, or 0 for an empty replica.
    pub async fn max_txid(&self) -> Result<u64> {
        Ok(self.list_all().await?.iter().map(|f| f.max_txid).max().unwrap_or(0))
    }

    /// Uploads a local LTX file, streaming large files as multipart uploads.
    pub async fn put_file(&self, level: u32, min_txid: u64, max_txid: u64, local: &FsPath) -> Result<FileInfo> {
        let location = self.path(level, min_txid, max_txid);
        let mut f = tokio::fs::File::open(local).await?;
        let size = f.metadata().await?.len();
        if size <= MULTIPART_THRESHOLD {
            let mut buf = Vec::with_capacity(size as usize);
            f.read_to_end(&mut buf).await?;
            self.store.put(&location, PutPayload::from(buf)).await.with_context(|| format!("put {location}"))?;
        } else {
            let upload = self.store.put_multipart(&location).await?;
            let mut w = WriteMultipart::new_with_chunk_size(upload, MULTIPART_CHUNK);
            let mut buf = vec![0u8; MULTIPART_CHUNK];
            loop {
                let n = f.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                w.wait_for_capacity(4).await?;
                w.write(&buf[..n]);
            }
            w.finish().await.with_context(|| format!("multipart put {location}"))?;
        }
        Ok(FileInfo { level, min_txid, max_txid, size, created_at: Utc::now() })
    }

    /// Streams a replica file into a local file.
    pub async fn download(&self, info: &FileInfo, dest: &FsPath) -> Result<()> {
        let location = self.path(info.level, info.min_txid, info.max_txid);
        let mut stream = self.store.get(&location).await.with_context(|| format!("get {location}"))?.into_stream();
        let mut f = tokio::fs::File::create(dest).await?;
        while let Some(chunk) = stream.next().await {
            f.write_all(&chunk?).await?;
        }
        f.flush().await?;
        Ok(())
    }

    pub async fn delete(&self, infos: &[FileInfo]) -> Result<()> {
        for info in infos {
            let location = self.path(info.level, info.min_txid, info.max_txid);
            match self.store.delete(&location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e).with_context(|| format!("delete {location}")),
            }
        }
        Ok(())
    }
}

/// Picks the files needed to rebuild the database up to `target` (or the
/// latest TXID): the newest snapshot at or before the target, then the file
/// from any level that extends the contiguous TXID range furthest.
/// This follows Litestream's `CalcRestorePlan`.
pub fn restore_plan(files: &[FileInfo], target: Option<u64>) -> Result<Vec<FileInfo>> {
    let within = |f: &FileInfo| target.is_none_or(|t| f.max_txid <= t);
    let mut plan: Vec<FileInfo> = Vec::new();
    if let Some(snap) = files.iter().filter(|f| f.level == SNAPSHOT_LEVEL && within(f)).max_by_key(|f| f.max_txid) {
        plan.push(snap.clone());
    }
    let mut current = plan.last().map(|f| f.max_txid).unwrap_or(0);
    loop {
        if target.is_some_and(|t| current >= t) {
            break;
        }
        let next = files
            .iter()
            .filter(|f| f.level < SNAPSHOT_LEVEL && within(f))
            .filter(|f| f.min_txid <= current + 1 && f.max_txid > current)
            // Prefer the furthest reach, then the higher (more compacted) level.
            .max_by_key(|f| (f.max_txid, f.level));
        match next {
            Some(f) => {
                current = f.max_txid;
                plan.push(f.clone());
            }
            None => break,
        }
    }
    if plan.is_empty() {
        bail!("no LTX files available to restore");
    }
    if plan[0].min_txid != 1 {
        bail!("replica has no snapshot: earliest restorable file starts at txid {}", ltx::Txid(plan[0].min_txid));
    }
    if let Some(t) = target
        && current != t
    {
        bail!("txid {} is not restorable; nearest is {}", ltx::Txid(t), ltx::Txid(current));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fi(level: u32, min: u64, max: u64) -> FileInfo {
        FileInfo { level, min_txid: min, max_txid: max, size: 0, created_at: Utc::now() }
    }

    #[test]
    fn plan_prefers_snapshot_then_compacted() {
        let files = vec![
            fi(0, 1, 1),
            fi(0, 2, 2),
            fi(0, 3, 3),
            fi(0, 4, 4),
            fi(0, 5, 5),
            fi(1, 1, 3),
            fi(1, 4, 4),
            fi(9, 1, 2),
        ];
        let plan = restore_plan(&files, None).unwrap();
        let ids: Vec<_> = plan.iter().map(|f| (f.level, f.min_txid, f.max_txid)).collect();
        assert_eq!(ids, vec![(9, 1, 2), (1, 1, 3), (1, 4, 4), (0, 5, 5)]);
    }

    #[test]
    fn plan_to_target() {
        let files = vec![fi(0, 1, 1), fi(0, 2, 2), fi(0, 3, 3)];
        let plan = restore_plan(&files, Some(2)).unwrap();
        assert_eq!(plan.len(), 2);
        assert!(restore_plan(&[fi(0, 2, 2)], None).is_err());
    }
}
