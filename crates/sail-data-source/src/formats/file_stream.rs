//! Streaming file source — the streaming counterpart of the batch listing table.
//!
//! `spark.readStream.format("parquet"|"csv"|"json").load(dir)`. Built on DataFusion's
//! `ListingTable` (file I/O + split enumeration) + Vajra's flow-event streaming, modelled on
//! Spark `FileStreamSource` / Flink `FileSource`:
//!
//! - **Parallel split reading**: `FileSourceExec` preserves the `ListingTable` partitioning,
//!   so file/row-group splits are read concurrently across `target_partitions`.
//! - **Cross-run exactly-once**: each scan re-lists the directory, reads only files **not in
//!   the committed processed-files log** (`<ck>/sources/0/{staged,committed}`, promoted by the
//!   runner after the batch output is durable — the same offset-WAL commit the rate source
//!   uses), so a clean restart never reprocesses already-committed files.
//!
//! MVP scope: processes the files available at scan time (suited to `trigger(availableNow=True)`
//! / one-shot ETL). Continuous new-file polling is a tracked follow-up; closing the crash-
//! mid-run output-duplicate window additionally needs the file-sink commit log. See
//! docs/design/streaming-file-source.md.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{plan_err, Constraints, Result};
use futures::StreamExt;
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::source::StreamSource;

/// A streaming source backed by files in a directory.
#[derive(Debug)]
pub struct FileStreamSource {
    /// Directory/glob URLs to list (hidden files already excluded by the attached glob).
    urls: Vec<ListingTableUrl>,
    /// The same listing options the batch reader would use (format, partition cols, …).
    listing_options: ListingOptions,
    schema: SchemaRef,
    constraints: Constraints,
    /// `maxFilesPerTrigger`: cap new files processed per micro-batch (backpressure). The rest
    /// are picked up by later triggers. `None` = no cap (Spark default).
    max_files_per_trigger: Option<usize>,
}

impl FileStreamSource {
    pub fn new(
        urls: Vec<ListingTableUrl>,
        listing_options: ListingOptions,
        schema: SchemaRef,
        constraints: Constraints,
        max_files_per_trigger: Option<usize>,
    ) -> Self {
        Self {
            urls,
            listing_options,
            schema,
            constraints,
            max_files_per_trigger,
        }
    }
}

#[async_trait::async_trait]
impl StreamSource for FileStreamSource {
    fn data_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        // The file source behaves the same per micro-batch; continuous (`ProcessingTime`) is
        // driven by the runner re-plan loop, so `bounded` is not needed here.
        _bounded: bool,
        checkpoint_location: Option<&str>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Already-committed files (cross-run exactly-once: never reprocess these).
        let seen: HashSet<String> = checkpoint_location
            .map(read_committed_files)
            .unwrap_or_default();
        // Collect NEW files (not yet committed), with mod time, for deterministic ordering +
        // `maxFilesPerTrigger` backpressure. Identifier = the store-relative object path
        // (stable across runs and object stores).
        let mut new_files: Vec<(chrono::DateTime<chrono::Utc>, String, ListingTableUrl)> = vec![];
        for base in &self.urls {
            let store = state.runtime_env().object_store(base)?;
            // Reconstruct a full URL store-agnostically: base scheme+authority + the object path
            // (works for file://, s3://, gs://, …).
            let mut prefix = base.object_store().as_str().to_string();
            if !prefix.ends_with('/') {
                prefix.push('/');
            }
            // If the input directory is itself the output of a streaming file sink, honor its
            // `_spark_metadata` commit log: the available files are exactly the committed ones
            // (orphan/partial files of a crashed batch are invisible). Newly committed batches
            // appear in later triggers. Otherwise fall back to plain directory listing.
            let committed = crate::streaming_sink_log::read_committed_with_mtime(
                &store,
                &base.prefix().clone(),
            )
            .await
            .map_err(|e| datafusion_common::DataFusionError::ObjectStore(Box::new(e)))?;
            if let Some(committed) = committed {
                for (rel, mtime_ms) in committed {
                    let id = rel.as_ref().to_string();
                    if !seen.contains(&id) {
                        let url = ListingTableUrl::parse(format!("{prefix}{}", rel.as_ref()))?;
                        let mtime = chrono::DateTime::from_timestamp_millis(mtime_ms)
                            .unwrap_or_default();
                        new_files.push((mtime, id, url));
                    }
                }
                continue;
            }
            let mut files = base.list_all_files(state, store.as_ref(), "").await?;
            while let Some(meta) = files.next().await {
                let meta = meta?;
                let id = meta.location.as_ref().to_string();
                if !seen.contains(&id) {
                    let url = ListingTableUrl::parse(format!("{prefix}{}", meta.location.as_ref()))?;
                    new_files.push((meta.last_modified, id, url));
                }
            }
        }
        // Deterministic FIFO: oldest files first (Spark `latestFirst=false` default), tie-broken
        // by path — so `maxFilesPerTrigger` takes a stable prefix and later triggers continue.
        new_files.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        if let Some(max) = self.max_files_per_trigger {
            new_files.truncate(max);
        }
        // `processed` = committed ∪ the files taken this micro-batch (only these get committed).
        let mut processed = seen;
        let mut new_urls: Vec<ListingTableUrl> = Vec::with_capacity(new_files.len());
        for (_, id, url) in new_files {
            processed.insert(id);
            new_urls.push(url);
        }

        let data_plan: Arc<dyn ExecutionPlan> = if new_urls.is_empty() {
            // Nothing new: emit an empty stream (then `EndOfData`) with the projected schema.
            let schema = match projection {
                Some(p) => Arc::new(self.schema.project(p)?),
                None => Arc::clone(&self.schema),
            };
            Arc::new(EmptyExec::new(schema))
        } else {
            let config = ListingTableConfig::new_with_multi_paths(new_urls)
                .with_listing_options(self.listing_options.clone())
                .with_schema(Arc::clone(&self.schema));
            let table = ListingTable::try_new(config)?.with_constraints(self.constraints.clone());
            table.scan(state, projection, filters, limit).await?
        };

        // Write-ahead the batch id + new processed-files set; the runner promotes staged →
        // committed (single atomic rename) after the batch output is durable. Embedding the batch
        // id makes recovery exact (see `SourceOffsetRecord`).
        if let Some(ck) = checkpoint_location {
            write_staged_files(ck, current_batch_id(ck), &processed);
        }

        Ok(Arc::new(FileSourceExec::try_new(data_plan)?))
    }
}

fn sources_dir(checkpoint_location: &str) -> PathBuf {
    Path::new(checkpoint_location).join("sources").join("0")
}

/// The file source's offset record: the micro-batch id **and** the cumulative processed-files set,
/// serialized as one unit. Keeping the batch id inside the record is what makes recovery exact:
/// the runner commits a batch with a single atomic rename of `staged` → `committed`, so the batch
/// number and the source position advance together. A crash before the rename leaves `staged`
/// (batch N still in flight) → recovery reprocesses batch **N** (same number → the sink
/// idempotently overwrites `_spark_metadata/N`); a crash after sees `committed` at N → the next
/// batch is N+1. Neither a duplicate nor a silent-loss window remains. (Older checkpoints stored a
/// bare newline list with no id; those are still read for the file set, falling back to the
/// `<cp>/offsets` markers for numbering.)
#[derive(serde::Serialize, serde::Deserialize)]
struct SourceOffsetRecord {
    batch_id: u64,
    files: Vec<String>,
}

fn read_record(checkpoint_location: &str, name: &str) -> Option<SourceOffsetRecord> {
    let body = std::fs::read_to_string(sources_dir(checkpoint_location).join(name)).ok()?;
    serde_json::from_str(&body).ok()
}

/// Read the durably-committed set of processed object paths, if any. Parses the JSON record,
/// falling back to the legacy newline-list format for checkpoints written before the batch id was
/// embedded.
pub fn read_committed_files(checkpoint_location: &str) -> HashSet<String> {
    let Ok(body) = std::fs::read_to_string(sources_dir(checkpoint_location).join("committed"))
    else {
        return HashSet::new();
    };
    match serde_json::from_str::<SourceOffsetRecord>(&body) {
        Ok(rec) => rec.files.into_iter().collect(),
        Err(_) => body
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect(),
    }
}

/// Latest committed batch id from the driver's `<cp>/offsets` markers (the numbering fallback for
/// non-file/non-replayable sources, and for fresh checkpoints).
fn latest_offset_batch_id(checkpoint_location: &str) -> Option<u64> {
    let dir = Path::new(checkpoint_location).join("offsets");
    std::fs::read_dir(dir).ok().and_then(|rd| {
        rd.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str().and_then(|s| s.parse::<u64>().ok()))
            .max()
    })
}

/// The micro-batch id this checkpoint is on — used by both the file source (to label its `staged`
/// record) and the file sink (to name `<out>/<id>/` + `_spark_metadata/<id>`), so the two always
/// agree. See [`SourceOffsetRecord`] for why this is exact under crashes.
pub fn current_batch_id(checkpoint_location: &str) -> u64 {
    if let Some(rec) = read_record(checkpoint_location, "staged") {
        return rec.batch_id; // in-flight batch → reprocess at the same id
    }
    if let Some(rec) = read_record(checkpoint_location, "committed") {
        return rec.batch_id + 1; // last fully committed → next id
    }
    latest_offset_batch_id(checkpoint_location)
        .map(|n| n + 1)
        .unwrap_or(0)
}

/// Stage (write-ahead) the batch id + processed-files set; the runner commits it (atomic rename
/// `staged` → `committed`) after the output is durable.
fn write_staged_files(checkpoint_location: &str, batch_id: u64, files: &HashSet<String>) {
    let dir = sources_dir(checkpoint_location);
    let _ = std::fs::create_dir_all(&dir);
    let rec = SourceOffsetRecord {
        batch_id,
        files: files.iter().cloned().collect(),
    };
    if let Ok(body) = serde_json::to_string(&rec) {
        let _ = std::fs::write(dir.join("staged"), body);
    }
}

/// Wraps a batch file-scan plan as a flow-event source: each data batch becomes an append-only
/// `FlowEvent::Data`; each partition emits its own `EndOfData`. Partitioning is preserved so
/// file/row-group splits are read in parallel (Flink `SplitEnumerator` / Spark file tasks).
#[derive(Debug)]
pub struct FileSourceExec {
    input: Arc<dyn ExecutionPlan>,
    data_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FileSourceExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let data_schema = input.schema();
        let output_schema = Arc::new(to_flow_event_schema(&data_schema));
        // One output partition per input file group (whole files — row-group splitting is
        // disabled for streaming scans, see sail-plan/src/lib.rs). Each partition emits its
        // files' rows then its own `EndOfData`; the parallel sink writes one file per partition
        // concurrently, and completes only after all-N `EndOfData` (Flink-style per-split
        // readers). Verified safe at whole-file granularity (the row-group-split path that lost
        // data is now disabled).
        let n = input
            .properties()
            .output_partitioning()
            .partition_count()
            .max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(n),
            EmissionType::Both,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            data_schema,
            properties,
        })
    }
}

impl DisplayAs for FileSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSourceExec")
    }
}

impl ExecutionPlan for FileSourceExec {
    fn name(&self) -> &str {
        "FileSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match <[_; 1]>::try_from(children) {
            Ok([input]) => Ok(Arc::new(FileSourceExec::try_new(input)?)),
            Err(_) => plan_err!("{} requires exactly one child", self.name()),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Partition `partition` reads its whole-file group split and emits that split's rows
        // then its own `EndOfData`. Whole-file granularity (row-group splitting disabled for
        // streaming) keeps this correct; the parallel sink drains all partitions to all-N
        // `EndOfData` before completing.
        let data_stream = self.input.execute(partition, context)?;
        let events = data_stream
            .map(|r| r.map(FlowEvent::append_only_data))
            .chain(futures::stream::once(async {
                Ok(FlowEvent::Marker(FlowMarker::EndOfData))
            }));
        let stream = Box::pin(FlowEventStreamAdapter::new(
            Arc::clone(&self.data_schema),
            events,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    // Exercises the recovery numbering that closes the crash-mid-commit duplicate window: the
    // batch id and processed-files set are committed as one atomic record, so an in-flight
    // `staged` replays at the SAME id while a clean `committed` advances to the next.
    #[test]
    fn current_batch_id_reflects_atomic_offset_record() {
        let dir = std::env::temp_dir().join(format!("vajra_fs_eo_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cp = dir.to_str().unwrap();

        // Fresh checkpoint: batch 0.
        assert_eq!(current_batch_id(cp), 0);

        // Batch 0 committed -> next is 1.
        let mut files: HashSet<String> = HashSet::new();
        files.insert("a/0/f0.parquet".to_string());
        write_staged_files(cp, 0, &files);
        std::fs::rename(
            sources_dir(cp).join("staged"),
            sources_dir(cp).join("committed"),
        )
        .unwrap();
        assert_eq!(current_batch_id(cp), 1);
        assert_eq!(read_committed_files(cp), files);

        // Batch 1 staged but NOT committed (crash mid-commit) -> reprocess at 1, not 2.
        files.insert("a/1/f1.parquet".to_string());
        write_staged_files(cp, 1, &files);
        assert_eq!(current_batch_id(cp), 1);

        // Legacy newline-list committed (no embedded id) is still read for the file set.
        let _ = std::fs::remove_file(sources_dir(cp).join("staged"));
        std::fs::write(sources_dir(cp).join("committed"), "x/old.parquet\n").unwrap();
        assert!(read_committed_files(cp).contains("x/old.parquet"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
