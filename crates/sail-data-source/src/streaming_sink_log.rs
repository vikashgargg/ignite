//! Streaming file-sink commit log — Vajra's implementation of Spark's `_spark_metadata`.
//!
//! This is the **sink-side exactly-once** mechanism. A streaming file sink that simply appends
//! files to the output directory cannot survive a crash: a micro-batch may write its output
//! files and then die before its source offsets are committed, so on restart the source replays
//! the same input and writes the output *again* — leaving duplicate/orphan files that a naive
//! reader would see (see `crates/sail-data-source/src/formats/file_stream.rs`, which names this
//! exact "crash-mid-run output-duplicate window").
//!
//! Spark closes the window with a transaction log under `<output>/_spark_metadata`: one file per
//! committed micro-batch (`_spark_metadata/<batchId>`), listing the files that batch added.
//! Readers (`MetadataLogFileIndex`) trust **only** files referenced by committed batches, so
//! orphan files from a crashed-then-retried batch are invisible. The single atomic write of the
//! batch-metadata file *is* the commit point (object stores give no cross-file atomic rename, so
//! a single small file is the only thing we can commit atomically).
//!
//! Vajra layout (chosen to scale past Spark's per-trigger full-dir listing): each micro-batch
//! writes its data files into a per-batch subdirectory `<output>/<batchId>/`, so committing a
//! batch only needs to list that one bounded subdirectory rather than the whole output. The
//! metadata file records store-relative paths; readers reconstruct full URLs the same way the
//! streaming file *source* does.
//!
//! Format (matches Spark `FileStreamSinkLog` v1): a `v1` version line, then one JSON
//! [`SinkFileStatus`] per line. Every `COMPACT_INTERVAL` batches the log is compacted into a
//! `<batchId>.compact` file holding the full live set, so readers never replay an unbounded
//! number of delta files.

use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path as StorePath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

/// Version header written as the first line of every metadata file (Spark `FileStreamSinkLog`).
const VERSION_LINE: &str = "v1";
/// Compact the delta files into a single `<batchId>.compact` every this many batches. Matches
/// Spark's default `spark.sql.streaming.fileSink.log.compactInterval`.
const COMPACT_INTERVAL: u64 = 10;
/// Directory name (relative to the output path) holding the commit log. Matches Spark.
pub const METADATA_DIR: &str = "_spark_metadata";

/// One committed output file, as recorded in the commit log. Field names/shape match Spark's
/// `SinkFileStatus` so the log stays inspectable and close to Spark's on-disk format. `path` is
/// the **store-relative** object path (stable across runs and object stores).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkFileStatus {
    pub path: String,
    pub size: u64,
    #[serde(rename = "isDir")]
    pub is_dir: bool,
    #[serde(rename = "modificationTime")]
    pub modification_time: i64,
    #[serde(rename = "blockReplication")]
    pub block_replication: i32,
    #[serde(rename = "blockSize")]
    pub block_size: i64,
    /// `"add"` or `"delete"`. The append-only streaming sink only writes `"add"`.
    pub action: String,
}

impl SinkFileStatus {
    fn add(meta: &ObjectMeta) -> Self {
        Self {
            path: meta.location.as_ref().to_string(),
            size: meta.size,
            is_dir: false,
            modification_time: meta.last_modified.timestamp_millis(),
            block_replication: 1,
            block_size: 0,
            action: "add".to_string(),
        }
    }
}

/// `<base>/_spark_metadata`
fn metadata_dir(base: &StorePath) -> StorePath {
    base.clone().join(METADATA_DIR)
}

/// `<base>/_spark_metadata/<batch_id>[.compact]`
fn batch_file(base: &StorePath, batch_id: u64, compact: bool) -> StorePath {
    let name = if compact {
        format!("{batch_id}.compact")
    } else {
        batch_id.to_string()
    };
    metadata_dir(base).join(name)
}

/// Serialize a metadata file body: the `v1` header then one JSON [`SinkFileStatus`] per line.
fn encode_log(entries: &[SinkFileStatus]) -> Result<String, serde_json::Error> {
    let mut body = String::from(VERSION_LINE);
    for e in entries {
        body.push('\n');
        body.push_str(&serde_json::to_string(e)?);
    }
    body.push('\n');
    Ok(body)
}

/// Parse a metadata file body into its entries, tolerating the version header and blank lines.
fn decode_log(body: &str) -> Vec<SinkFileStatus> {
    body.lines()
        .filter(|l| !l.is_empty() && *l != VERSION_LINE)
        .filter_map(|l| serde_json::from_str::<SinkFileStatus>(l).ok())
        .collect()
}

/// True if `<base>/_spark_metadata` exists (i.e. this output path is governed by a commit log).
pub async fn has_metadata_log(
    store: &Arc<dyn ObjectStore>,
    base: &StorePath,
) -> object_store::Result<bool> {
    let dir = metadata_dir(base);
    let mut listing = store.list(Some(&dir));
    Ok(listing.next().await.transpose()?.is_some())
}

/// `<base>/<batch_id>` — the per-batch data subdirectory.
fn batch_dir(base: &StorePath, batch_id: u64) -> StorePath {
    base.clone().join(batch_id.to_string())
}

/// Remove every file under `<base>/<batch_id>/`. Called before (re)writing a batch so a retried
/// batch starts from a clean subdirectory: orphan files from a crashed earlier attempt of the
/// *same* batch are deleted before the (idempotent) metadata commit lists the directory.
pub async fn clean_batch_dir(
    store: &Arc<dyn ObjectStore>,
    base: &StorePath,
    batch_id: u64,
) -> object_store::Result<()> {
    let prefix = batch_dir(base, batch_id);
    let mut listing = store.list(Some(&prefix));
    let mut paths = vec![];
    while let Some(meta) = listing.next().await.transpose()? {
        paths.push(meta.location);
    }
    for p in paths {
        store.delete(&p).await?;
    }
    Ok(())
}

/// List the data files a batch wrote — the contents of `<base>/<batch_id>/`.
pub async fn list_batch_files(
    store: &Arc<dyn ObjectStore>,
    base: &StorePath,
    batch_id: u64,
) -> object_store::Result<Vec<ObjectMeta>> {
    let prefix = batch_dir(base, batch_id);
    let mut listing = store.list(Some(&prefix));
    let mut out = vec![];
    while let Some(meta) = listing.next().await.transpose()? {
        out.push(meta);
    }
    Ok(out)
}

/// Commit micro-batch `batch_id` by atomically writing its metadata file. `metas` are the output
/// files the batch wrote (typically the contents of `<base>/<batch_id>/`). This single atomic
/// write is the commit point: until it lands, the batch is uncommitted and its files are orphan.
///
/// On a compaction boundary the full live set (all prior committed adds plus this batch) is
/// written to `<batch_id>.compact`; otherwise just this batch's adds go to `<batch_id>`.
pub async fn commit_batch(
    store: &Arc<dyn ObjectStore>,
    base: &StorePath,
    batch_id: u64,
    metas: &[ObjectMeta],
) -> object_store::Result<()> {
    let adds: Vec<SinkFileStatus> = metas.iter().map(SinkFileStatus::add).collect();
    let is_compaction = (batch_id + 1).is_multiple_of(COMPACT_INTERVAL);
    let (target, entries) = if is_compaction {
        // Roll the prior live set forward with this batch's adds into a single compact file.
        let mut live = read_live_entries(store, base, Some(batch_id)).await?;
        live.extend(adds);
        (batch_file(base, batch_id, true), live)
    } else {
        (batch_file(base, batch_id, false), adds)
    };
    let body = encode_log(&entries).map_err(|e| object_store::Error::Generic {
        store: "spark_metadata",
        source: Box::new(e),
    })?;
    // `put` is atomic (LocalFileSystem stages to a temp file then renames; object stores do a
    // single atomic PUT), and a retried batch idempotently overwrites the same file.
    store
        .put(&target, PutPayload::from(body.into_bytes()))
        .await?;
    Ok(())
}

/// Read all committed metadata files up to and including `up_to` (or all, if `None`) and fold
/// them into the live set of [`SinkFileStatus`] entries, honoring compaction and delete actions.
async fn read_live_entries(
    store: &Arc<dyn ObjectStore>,
    base: &StorePath,
    up_to: Option<u64>,
) -> object_store::Result<Vec<SinkFileStatus>> {
    let dir = metadata_dir(base);
    // Enumerate (batch_id, is_compact, store_path) for every metadata file.
    let mut files: Vec<(u64, bool, StorePath)> = vec![];
    let mut listing = store.list(Some(&dir));
    while let Some(meta) = listing.next().await.transpose()? {
        let name = meta.location.filename().unwrap_or("");
        let (id_str, compact) = match name.strip_suffix(".compact") {
            Some(s) => (s, true),
            None => (name, false),
        };
        if let Ok(id) = id_str.parse::<u64>() {
            if up_to.is_none_or(|u| id <= u) {
                files.push((id, compact, meta.location.clone()));
            }
        }
    }
    // The newest compaction file is a complete snapshot; only delta files after it matter.
    files.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let latest_compact = files
        .iter()
        .filter(|(_, c, _)| *c)
        .map(|(id, _, _)| *id)
        .max();
    let mut live: Vec<SinkFileStatus> = vec![];
    let mut deleted: std::collections::HashSet<String> = Default::default();
    for (id, compact, path) in &files {
        if let Some(c) = latest_compact {
            // Skip everything strictly before the latest compaction snapshot, and skip the older
            // duplicate (non-compact) entry for the compaction batch itself.
            if *id < c || (*id == c && !*compact) {
                continue;
            }
        }
        let bytes = store.get(path).await?.bytes().await?;
        let body = String::from_utf8_lossy(bytes.as_ref());
        for e in decode_log(&body) {
            match e.action.as_str() {
                "delete" => {
                    deleted.insert(e.path.clone());
                }
                _ => live.push(e),
            }
        }
    }
    if !deleted.is_empty() {
        live.retain(|e| !deleted.contains(&e.path));
    }
    Ok(live)
}

/// The live set of committed output files for `base`, as store-relative paths — or `None` if
/// `base` has no commit log (so the caller should fall back to plain directory listing).
pub async fn read_committed_files(
    store: &Arc<dyn ObjectStore>,
    base: &StorePath,
) -> object_store::Result<Option<Vec<StorePath>>> {
    if !has_metadata_log(store, base).await? {
        return Ok(None);
    }
    let live = read_live_entries(store, base, None).await?;
    Ok(Some(
        live.into_iter().map(|e| StorePath::from(e.path)).collect(),
    ))
}

/// Like [`read_committed_files`] but pairs each committed file with its recorded modification
/// time (epoch ms) — for the streaming file *source* to read another stream's committed output
/// in deterministic file order (FIFO) while picking up newly committed batches each trigger.
pub async fn read_committed_with_mtime(
    store: &Arc<dyn ObjectStore>,
    base: &StorePath,
) -> object_store::Result<Option<Vec<(StorePath, i64)>>> {
    if !has_metadata_log(store, base).await? {
        return Ok(None);
    }
    let live = read_live_entries(store, base, None).await?;
    Ok(Some(
        live.into_iter()
            .map(|e| (StorePath::from(e.path), e.modification_time))
            .collect(),
    ))
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn meta(path: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: StorePath::from(path),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[tokio::test]
    async fn roundtrip_and_compaction_and_orphans() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = StorePath::from("out");

        // No log yet -> reader falls back.
        assert!(read_committed_files(&store, &base).await.unwrap().is_none());

        // Commit 12 batches (crosses the compaction boundary at batch_id 9: (9+1)%10==0).
        let mut expected = vec![];
        for b in 0..12u64 {
            let p = format!("out/{b}/part-{b}.parquet");
            expected.push(p.clone());
            commit_batch(&store, &base, b, &[meta(&p, 100 + b)])
                .await
                .unwrap();
        }
        // An orphan file from a crashed batch (never committed) must be invisible.
        store
            .put(
                &StorePath::from("out/99/orphan.parquet"),
                PutPayload::from("x"),
            )
            .await
            .unwrap();

        let mut got: Vec<String> = read_committed_files(&store, &base)
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|p| p.as_ref().to_string())
            .collect();
        got.sort();
        expected.sort();
        assert_eq!(
            got, expected,
            "committed set must equal all adds, no orphan"
        );

        // A compaction snapshot must exist at batch 9.
        assert!(has_metadata_log(&store, &base).await.unwrap());
        let dir = metadata_dir(&base);
        let mut names: Vec<String> = vec![];
        let mut l = store.list(Some(&dir));
        while let Some(m) = l.next().await.transpose().unwrap() {
            names.push(m.location.filename().unwrap().to_string());
        }
        assert!(names.contains(&"9.compact".to_string()), "names={names:?}");
    }
}
