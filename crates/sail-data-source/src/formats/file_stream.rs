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

        // Write-ahead the new processed-files set; the runner promotes staged → committed
        // after the batch output is durable (exactly-once recovery).
        if let Some(ck) = checkpoint_location {
            write_staged_files(ck, &processed);
        }

        Ok(Arc::new(FileSourceExec::try_new(data_plan)?))
    }
}

fn sources_dir(checkpoint_location: &str) -> PathBuf {
    Path::new(checkpoint_location).join("sources").join("0")
}

/// Read the durably-committed set of processed object paths, if any.
pub fn read_committed_files(checkpoint_location: &str) -> HashSet<String> {
    std::fs::read_to_string(sources_dir(checkpoint_location).join("committed"))
        .map(|s| {
            s.lines()
                .filter(|l| !l.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Stage (write-ahead) the processed-files set; the runner commits it after the output is durable.
fn write_staged_files(checkpoint_location: &str, files: &HashSet<String>) {
    let dir = sources_dir(checkpoint_location);
    let _ = std::fs::create_dir_all(&dir);
    let body = files.iter().cloned().collect::<Vec<_>>().join("\n");
    let _ = std::fs::write(dir.join("staged"), body);
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
