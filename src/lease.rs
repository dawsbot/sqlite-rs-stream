//! Distributed lease on the replica so exactly one host replicates at a time.
//! Uses the same `lock.json` object and JSON shape as Litestream's S3 leaser,
//! guarded by conditional writes (If-None-Match / If-Match on the ETag).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};

pub const LEASE_PATH: &str = "lock.json";
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    pub generation: i64,
    pub expires_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    #[serde(skip)]
    version: Option<UpdateVersion>,
}

impl Lease {
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("lease already held by {owner} until {expires_at}")]
    Held { owner: String, expires_at: DateTime<Utc> },
    #[error("lease not held")]
    NotHeld,
    #[error("replica store does not support conditional writes needed for leasing: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub struct Leaser {
    store: Arc<dyn ObjectStore>,
    path: Path,
    pub ttl: Duration,
    pub owner: String,
}

impl Leaser {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let host = gethostname::gethostname().to_string_lossy().into_owned();
        let owner = if host.is_empty() {
            format!("pid-{}", std::process::id())
        } else {
            format!("{host}:{}", std::process::id())
        };
        Leaser { store, path: Path::from(LEASE_PATH), ttl: DEFAULT_TTL, owner }
    }

    async fn read(&self) -> Result<Option<Lease>, LeaseError> {
        let res = match self.store.get(&self.path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(anyhow::Error::from(e).context("read lease").into()),
        };
        let version = UpdateVersion { e_tag: res.meta.e_tag.clone(), version: res.meta.version.clone() };
        let bytes = res.bytes().await.context("read lease body")?;
        let mut lease: Lease = serde_json::from_slice(&bytes).context("decode lease")?;
        lease.version = Some(version);
        Ok(Some(lease))
    }

    async fn write(&self, lease: &mut Lease, mode: PutMode) -> Result<(), LeaseError> {
        let body = serde_json::to_vec(lease).context("encode lease")?;
        let opts = PutOptions { mode, ..Default::default() };
        match self.store.put_opts(&self.path, PutPayload::from(body), opts).await {
            Ok(r) => {
                lease.version = Some(UpdateVersion { e_tag: r.e_tag, version: r.version });
                Ok(())
            }
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                Err(LeaseError::NotHeld)
            }
            Err(e @ object_store::Error::NotImplemented { .. }) => Err(LeaseError::Unsupported(e.to_string())),
            Err(e) => Err(anyhow::Error::from(e).context("write lease").into()),
        }
    }

    /// Takes the lease if it is free or expired.
    pub async fn acquire(&self) -> Result<Lease, LeaseError> {
        let existing = self.read().await?;
        if let Some(l) = &existing
            && !l.is_expired()
        {
            return Err(LeaseError::Held { owner: l.owner.clone(), expires_at: l.expires_at });
        }
        let mut lease = Lease {
            generation: existing.as_ref().map_or(1, |l| l.generation + 1),
            expires_at: Utc::now() + self.ttl,
            owner: self.owner.clone(),
            version: None,
        };
        let mode = match existing.and_then(|l| l.version) {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        match self.write(&mut lease, mode).await {
            Err(LeaseError::NotHeld) => match self.read().await? {
                Some(cur) => Err(LeaseError::Held { owner: cur.owner, expires_at: cur.expires_at }),
                None => Err(LeaseError::NotHeld),
            },
            r => r.map(|_| lease),
        }
    }

    /// Extends a held lease. Fails with `NotHeld` if someone else took it.
    pub async fn renew(&self, lease: &Lease) -> Result<Lease, LeaseError> {
        let version = lease.version.clone().ok_or(LeaseError::NotHeld)?;
        let mut next = Lease {
            generation: lease.generation,
            expires_at: Utc::now() + self.ttl,
            owner: self.owner.clone(),
            version: None,
        };
        self.write(&mut next, PutMode::Update(version)).await?;
        Ok(next)
    }

    /// Gives the lease up by marking it expired, so the next acquirer does not
    /// have to wait out the TTL.
    pub async fn release(&self, lease: &Lease) -> Result<(), LeaseError> {
        let version = lease.version.clone().ok_or(LeaseError::NotHeld)?;
        let mut expired = Lease {
            generation: lease.generation,
            expires_at: DateTime::<Utc>::UNIX_EPOCH,
            owner: self.owner.clone(),
            version: None,
        };
        self.write(&mut expired, PutMode::Update(version)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn exclusive_acquire_renew_release() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut a = Leaser::new(store.clone());
        a.owner = "a".into();
        let mut b = Leaser::new(store.clone());
        b.owner = "b".into();

        let la = a.acquire().await.unwrap();
        assert_eq!(la.generation, 1);
        assert!(matches!(b.acquire().await, Err(LeaseError::Held { ref owner, .. }) if owner == "a"));

        let la = a.renew(&la).await.unwrap();
        a.release(&la).await.unwrap();

        let lb = b.acquire().await.unwrap();
        assert_eq!(lb.generation, 2);
        // A's stale lease can no longer be renewed.
        assert!(matches!(a.renew(&la).await, Err(LeaseError::NotHeld)));
    }

    #[test]
    fn reads_litestream_json() {
        let l: Lease = serde_json::from_str(
            r#"{"generation":3,"expires_at":"2026-10-21T10:08:00.123456789-05:00","owner":"host:42"}"#,
        )
        .unwrap();
        assert_eq!(l.generation, 3);
        assert_eq!(l.owner, "host:42");
        assert_eq!(l.expires_at.to_rfc3339(), "2026-10-21T15:08:00.123456789+00:00");
    }
}
