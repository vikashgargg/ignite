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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use datafusion_common::{plan_datafusion_err, Result};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

/// Process-global cache of built object-store clients, keyed by `scheme://authority` (bucket).
///
/// **Why (MEASURED, frame-pointer CPU profile 2026-07-12):** `CheckpointStore::from_location` is called
/// by EVERY streaming operator (KafkaSource / ShuffleWrite / WindowAccum) during execution — and building
/// an `AmazonS3Builder`/GCS client each time constructs a fresh `reqwest` client, which calls
/// `rustls_native_certs::load_native_certs` and **base64-parses the entire system CA trust store on every
/// call**. The profile showed this chain (`from_location → S3Builder::build → ClientBuilder::build →
/// load_native_certs → base64::decode`) at **~30% of on-CPU time** (the actual Flight-shuffle IPC was 1.3%).
/// Object-store clients are designed to be long-lived + shared (Ballista caches shuffle clients — REFERENCES
/// §4); caching them here builds the TLS/reqwest client ONCE per bucket and reuses it, eliminating the waste.
/// The per-call `base` prefix still varies (cheap); only the client is shared.
fn store_cache() -> &'static Mutex<HashMap<String, Arc<dyn ObjectStore>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<dyn ObjectStore>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

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
        let scheme = url.scheme();
        let store: Arc<dyn ObjectStore> = match scheme {
            // LocalFileSystem is cheap to build (no TLS/HTTP client) — no need to cache.
            "file" => Arc::new(object_store::local::LocalFileSystem::new()),
            // Cloud clients build a reqwest+TLS client (loads the native CA store) — cache per bucket so
            // it is built ONCE and reused across every operator's checkpoint access (see `store_cache`).
            "s3" | "s3a" | "gs" => {
                let key = format!("{scheme}://{}", url.authority());
                let mut cache = store_cache().lock().unwrap_or_else(|e| e.into_inner());
                if let Some(existing) = cache.get(&key) {
                    Arc::clone(existing)
                } else {
                    let built: Arc<dyn ObjectStore> = if scheme == "gs" {
                        Arc::new(
                            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                                .with_url(url.as_str())
                                .build()
                                .map_err(err)?,
                        )
                    } else {
                        Arc::new(
                            object_store::aws::AmazonS3Builder::from_env()
                                .with_url(url.as_str())
                                .build()
                                .map_err(err)?,
                        )
                    };
                    cache.insert(key, Arc::clone(&built));
                    built
                }
            }
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

    /// Relative paths (under the checkpoint base) of all objects below `prefix_rel`, recursively.
    /// Used by the commit step to find every `*/staged` artifact (one per source / operator) to
    /// promote.
    pub async fn list_rel(&self, prefix_rel: &str) -> Result<Vec<String>> {
        use futures::StreamExt;
        let prefix = self.child(prefix_rel);
        let base_str = format!("{}/", self.base);
        let mut out = vec![];
        let mut listing = self.store.list(Some(&prefix));
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|e| plan_datafusion_err!("checkpoint list {prefix_rel}: {e}"))?;
            let full = meta.location.as_ref();
            let rel = full.strip_prefix(&base_str).unwrap_or(full).to_string();
            out.push(rel);
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
