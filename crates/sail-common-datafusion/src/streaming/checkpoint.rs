//! `CheckpointStore` — object-store-backed streaming checkpoint I/O.
//!
//! Vajra's streaming checkpoint (source offset records, the driver's batch-id markers, operator
//! state snapshots) was originally local-FS (`std::fs`). That blocks cloud-native HA: on Kubernetes
//! a pod restart loses a local checkpoint, so exactly-once recovery can't survive node loss. Flink
//! and Spark Structured Streaming checkpoint to durable object storage (S3/HDFS/GCS) for exactly
//! this reason.
//!
//! This abstraction routes all checkpoint I/O through `object_store`, so a `checkpointLocation` of
//! `file://…` (or a bare local path) **or** `s3://…` / `gs://…` works uniformly.
//!
//! **Atomic commit without rename.** Object stores have no atomic rename (S3 "rename" is copy+delete
//! — not atomic). So the streaming commit cannot promote `staged`→`committed` by rename. Instead
//! every checkpoint artifact is a **single object**, and the commit is a single atomic `put` of the
//! committed object (object stores guarantee a `put` is atomic — readers see all-or-nothing). This
//! is why operator state is serialized to one blob rather than a multi-file directory. Same pattern
//! Flink uses (a single atomic checkpoint-metadata write is the commit point).

use std::sync::Arc;

use bytes::Bytes;
use datafusion_common::{plan_datafusion_err, Result};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

#[derive(Clone)]
pub struct CheckpointStore {
    store: Arc<dyn ObjectStore>,
    /// Base prefix within the store for this checkpoint location.
    base: StorePath,
}

impl std::fmt::Debug for CheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CheckpointStore({})", self.base)
    }
}

impl CheckpointStore {
    /// Build from a `checkpointLocation` — a URL (`file://`, `s3://`, `gs://`) or a bare local path
    /// (treated as `file://`). Cloud stores read credentials from the environment / instance role
    /// (the same path the Iceberg/S3 warehouse uses), so no secrets are threaded here.
    pub fn from_location(location: &str) -> Result<Self> {
        let err = |e: object_store::Error| plan_datafusion_err!("checkpoint store: {e}");
        if !location.contains("://") {
            // Bare local path → LocalFileSystem rooted at FS root; base = the absolute path.
            let store = Arc::new(object_store::local::LocalFileSystem::new());
            let base = StorePath::from_absolute_path(location)
                .map_err(|e| plan_datafusion_err!("checkpoint path {location}: {e}"))?;
            return Ok(Self { store, base });
        }
        let url = Url::parse(location)
            .map_err(|e| plan_datafusion_err!("checkpoint url {location}: {e}"))?;
        let base = StorePath::from(url.path().trim_start_matches('/'));
        let store: Arc<dyn ObjectStore> = match url.scheme() {
            "file" => Arc::new(object_store::local::LocalFileSystem::new()),
            "s3" | "s3a" => Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_url(url.as_str())
                    .build()
                    .map_err(err)?,
            ),
            "gs" => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_url(url.as_str())
                    .build()
                    .map_err(err)?,
            ),
            other => {
                return Err(plan_datafusion_err!(
                    "unsupported checkpoint scheme '{other}' (use file://, s3://, or gs://)"
                ))
            }
        };
        Ok(Self { store, base })
    }

    /// Construct directly from a store + base prefix (for tests / reuse of an existing store).
    pub fn from_store(store: Arc<dyn ObjectStore>, base: StorePath) -> Self {
        Self { store, base }
    }

    fn child(&self, rel: &str) -> StorePath {
        let mut p = self.base.clone();
        for seg in rel.split('/').filter(|s| !s.is_empty()) {
            p = p.join(seg);
        }
        p
    }

    /// Read an artifact; `None` if absent.
    pub async fn get(&self, rel: &str) -> Result<Option<Bytes>> {
        match self.store.get(&self.child(rel)).await {
            Ok(r) => {
                let b = r
                    .bytes()
                    .await
                    .map_err(|e| plan_datafusion_err!("checkpoint get {rel}: {e}"))?;
                Ok(Some(b))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(plan_datafusion_err!("checkpoint get {rel}: {e}")),
        }
    }

    /// Atomically write an artifact (single-object `put` — all-or-nothing; the commit point).
    pub async fn put(&self, rel: &str, data: Bytes) -> Result<()> {
        self.store
            .put(&self.child(rel), data.into())
            .await
            .map(|_| ())
            .map_err(|e| plan_datafusion_err!("checkpoint put {rel}: {e}"))
    }

    /// Relative leaf names directly under `prefix_rel` (one level; non-recursive enough for the
    /// offsets/state dirs which hold flat numbered/named objects).
    pub async fn list(&self, prefix_rel: &str) -> Result<Vec<String>> {
        use futures::StreamExt;
        let prefix = self.child(prefix_rel);
        let mut out = vec![];
        let mut listing = self.store.list(Some(&prefix));
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|e| plan_datafusion_err!("checkpoint list {prefix_rel}: {e}"))?;
            if let Some(name) = meta.location.filename() {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }

    /// Delete an artifact; absent is not an error (idempotent).
    pub async fn delete(&self, rel: &str) -> Result<()> {
        match self.store.delete(&self.child(rel)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(plan_datafusion_err!("checkpoint delete {rel}: {e}")),
        }
    }

    /// Commit a single-object artifact: copy `staged_rel` → `committed_rel` via one atomic `put`
    /// (object stores have no atomic rename, so the committed object's `put` *is* the commit). The
    /// staged object is then removed (best-effort). No-op if there is nothing staged.
    pub async fn promote(&self, staged_rel: &str, committed_rel: &str) -> Result<()> {
        if let Some(data) = self.get(staged_rel).await? {
            self.put(committed_rel, data).await?;
            let _ = self.delete(staged_rel).await;
        }
        Ok(())
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn put_get_list_promote_roundtrip() {
        let store = CheckpointStore::from_store(Arc::new(InMemory::new()), StorePath::from("ck"));
        assert!(store.get("sources/0/committed").await.unwrap().is_none());

        // Write-ahead staged, then atomically promote to committed.
        store
            .put("sources/0/staged", Bytes::from_static(b"{\"batch_id\":3}"))
            .await
            .unwrap();
        store
            .promote("sources/0/staged", "sources/0/committed")
            .await
            .unwrap();
        let c = store.get("sources/0/committed").await.unwrap().unwrap();
        assert_eq!(&c[..], b"{\"batch_id\":3}");
        // staged removed after promotion.
        assert!(store.get("sources/0/staged").await.unwrap().is_none());

        // list returns leaf names.
        store.put("offsets/0", Bytes::from_static(b"x")).await.unwrap();
        store.put("offsets/1", Bytes::from_static(b"x")).await.unwrap();
        let mut names = store.list("offsets").await.unwrap();
        names.sort();
        assert_eq!(names, vec!["0".to_string(), "1".to_string()]);

        store.delete("offsets/0").await.unwrap();
        store.delete("offsets/0").await.unwrap(); // idempotent
        assert!(store.get("offsets/0").await.unwrap().is_none());
    }
}
