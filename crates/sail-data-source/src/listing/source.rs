use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, not_impl_err, plan_err, DataFusionError, GetExt, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::datasource::{
    find_path_in_options, get_partition_columns_and_file_schema, OptionLayer, SinkInfo, SourceInfo,
    TableFormat,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

use crate::utils::split_parquet_compression_string;

/// Trait for schema inference logic
#[async_trait::async_trait]
pub trait SchemaInfer: Debug + Send + Sync + 'static {
    /// Get schema based on options. Each implementation can handle its own
    /// special cases like inferSchema=false.
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        list_options: &ListingOptions,
    ) -> Result<Schema>;
}

/// Default schema inferrer that uses DataFusion's built-in inference
#[derive(Debug)]
pub struct DefaultSchemaInfer;

#[async_trait::async_trait]
impl SchemaInfer for DefaultSchemaInfer {
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        list_options: &ListingOptions,
    ) -> Result<Schema> {
        Ok(list_options
            .format
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone())
    }
}

/// A trait for creating format instances when reading and writing listing files.
pub trait FormatFactory: Debug + Send + Sync + 'static {
    type Read: ReadFormat;
    type Write: WriteFormat;

    /// The name of the format.
    fn name() -> &'static str;

    /// Creates the read format.
    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read>;

    /// Creates the write format.
    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write>;
}

/// A trait for format-specific logic for reading listing files.
pub trait ReadFormat: Debug + Send + Sync + 'static {
    fn create_read_format(
        &self,
        compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>>;

    /// Per-read override for the file extension used when listing files.
    /// Returning `None` keeps the default extension supplied by `ListingOptions`.
    fn file_extension_override(&self) -> Result<Option<String>> {
        Ok(None)
    }

    /// Get the schema inferrer for this format
    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer>;
}

/// A trait for format-specific logic for writing listing files.
pub trait WriteFormat: Debug + Send + Sync + 'static {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)>;
}

#[derive(Debug, Default)]
pub struct ListingTableFormat<T: FormatFactory> {
    phantom: PhantomData<T>,
}

#[async_trait]
impl<T: FormatFactory> TableFormat for ListingTableFormat<T> {
    fn name(&self) -> &str {
        T::name()
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let SourceInfo {
            paths,
            schema,
            constraints,
            partition_by,
            bucket_by: _,
            sort_order,
            options,
            is_streaming,
        } = info;

        // `maxFilesPerTrigger` (streaming backpressure): cap new files per micro-batch.
        // Read by reference (latest layer/item wins) before `options` is consumed below.
        let max_files_per_trigger: Option<usize> = if is_streaming {
            options.iter().rev().find_map(|layer| match layer {
                OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => {
                    items
                        .iter()
                        .rev()
                        .find(|(k, _)| k.eq_ignore_ascii_case("maxFilesPerTrigger"))
                        .and_then(|(_, v)| v.parse::<usize>().ok())
                }
                _ => None,
            })
        } else {
            None
        };
        let read_format = T::read(ctx, options)?;
        let urls = crate::url::resolve_listing_urls(ctx, paths).await?;
        let file_format = read_format.create_read_format(None)?;
        let extension_with_compression =
            file_format.compression_type().and_then(|compression_type| {
                match file_format.get_ext_with_compression(&compression_type) {
                    // if the extension is the same as the file format, we don't need to add it
                    Ok(ext) if ext != file_format.get_ext() => Some(ext),
                    _ => None,
                }
            });
        let file_extension_override = read_format.file_extension_override()?;

        let config = ctx.config();
        let mut listing_options = ListingOptions::new(file_format)
            .with_target_partitions(config.target_partitions())
            .with_collect_stat(config.collect_statistics());
        if let Some(ext) = file_extension_override {
            listing_options = listing_options.with_file_extension(ext);
        }

        let (schema, partition_by) = match schema {
            Some(schema) if !schema.fields().is_empty() => {
                // Detect compression from the actual files so e.g.
                // `data.csv.gz` plus an explicit schema works without
                // `option("compression", "gzip")`.
                crate::listing::utils::detect_listing_compression(
                    ctx,
                    &urls,
                    &mut listing_options,
                    &extension_with_compression,
                    &read_format,
                )
                .await?;
                // When the caller did not supply partition columns, auto-
                // discover them from `key=value` segments in the listing
                // paths (matching the no-schema branch's behavior via
                // `infer_partitions_from_path`). Without this, columns
                // that exist only in the directory tree (e.g. `part=x/`)
                // are treated as file columns, and the parquet/CSV reader
                // fails because the file itself doesn't contain them.
                //
                // `ListingOptions::infer_partitions` uses DataFusion's
                // case-sensitive `list_all_files`, so we have to clear
                // the file-extension filter first or files like
                // `data.PARQUET` won't be visible during discovery.
                let partition_by = if partition_by.is_empty() {
                    listing_options.file_extension = "".to_string();
                    let mut discovered = vec![];
                    for url in &urls {
                        for name in listing_options.infer_partitions(ctx, url).await? {
                            if !discovered.contains(&name) {
                                discovered.push(name);
                            }
                        }
                    }
                    discovered
                        .into_iter()
                        .filter(|name| {
                            schema
                                .fields()
                                .iter()
                                .any(|f| f.name().eq_ignore_ascii_case(name))
                        })
                        .collect()
                } else {
                    partition_by
                };
                let (partition_by, schema) =
                    get_partition_columns_and_file_schema(&schema, partition_by)?;
                (Arc::new(schema), partition_by)
            }
            _ => {
                let schema = crate::listing::utils::resolve_listing_schema(
                    ctx,
                    &urls,
                    &mut listing_options,
                    &extension_with_compression,
                    &read_format,
                )
                .await?;
                let partition_by = partition_by
                    .into_iter()
                    .map(|col| (col, DataType::Utf8))
                    .collect();
                (schema, partition_by)
            }
        };

        // Clear the file-extension filter on the listing options so that
        // DataFusion's scan-time listing accepts every file admitted by the
        // URL (which in turn excludes hidden files via the default glob
        // attached in `resolve_listing_urls`). This matches Spark's
        // behavior of reading every non-hidden file in a directory
        // regardless of its extension.
        let listing_options = listing_options
            .with_file_extension("")
            .with_file_sort_order(vec![sort_order])
            .with_table_partition_cols(partition_by);

        if is_streaming {
            // `spark.readStream` over files: a streaming source that re-lists the directory,
            // reads only files not yet committed (cross-run exactly-once), and reads them in
            // parallel — built on the same listing config as the batch reader.
            return Ok(provider_as_source(Arc::new(
                sail_common_datafusion::streaming::source::StreamSourceTableProvider::new(Arc::new(
                    crate::formats::file_stream::FileStreamSource::new(
                        urls,
                        listing_options,
                        schema,
                        constraints,
                        max_files_per_trigger,
                    ),
                )),
            )));
        }

        // Sink-side exactly-once: honor a `_spark_metadata` commit log if present, reading only
        // committed files (explicit file URLs) instead of listing the directory (which would
        // expose orphan/partial files from a crashed-then-retried micro-batch).
        let batch_urls = match committed_urls_if_logged(ctx, &urls).await? {
            Some(committed) => committed,
            None => urls,
        };
        let config = ListingTableConfig::new_with_multi_paths(batch_urls);
        let config = if listing_options.table_partition_cols.is_empty() {
            config
                .with_listing_options(listing_options)
                .infer_partitions_from_path(ctx)
                .await?
        } else {
            for url in config.table_paths.iter() {
                listing_options.validate_partitions(ctx, url).await?;
            }
            config.with_listing_options(listing_options)
        };
        // The schema must be set after the listing options, otherwise it will panic.
        let config = config.with_schema(schema);
        let config = crate::listing::utils::rewrite_listing_partitions(config, ctx).await?;
        let listing_table = Arc::new(ListingTable::try_new(config)?.with_constraints(constraints));
        Ok(provider_as_source(listing_table))
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in listing table options");
        };
        let SinkInfo {
            input,
            // TODO: sink mode is ignored since the file formats only support append operation
            mode: _,
            partition_by,
            bucket_by,
            sort_order,
            mut options,
            logical_schema: _,
            declared_schema: _,
        } = info;
        // Sink-side exactly-once: a streaming file write with a checkpoint location uses the
        // `_spark_metadata` commit log. The reserved option is consumed here (stripped before the
        // format writer sees it). See `crate::streaming_sink_log`.
        let commit_log_checkpoint = sail_common_datafusion::datasource::take_option(
            &mut options,
            sail_common_datafusion::datasource::STREAM_CHECKPOINT_OPTION,
        );
        // Streaming write: the input is a flow-event stream. Decode it to a plain data
        // stream (skip markers, strip flow-event fields) so the normal file writer can
        // durably persist it (durable streaming sink — see docs/design/streaming-exactly-once.md).
        // Durable for availableNow/once triggers; continuous needs per-batch commit (follow-up).
        let streaming = is_flow_event_schema(&input.schema());
        // Original flow-event input + its partition count, captured before the single-partition
        // decode below, so a multi-partition streaming write can fan into N parallel sinks.
        let flow_input = Arc::clone(&input);
        let n_parts = input.properties().output_partitioning().partition_count();
        let input: Arc<dyn ExecutionPlan> = if streaming {
            Arc::new(crate::streaming_decode::FlowEventToDataExec::try_new(input)?)
        } else {
            input
        };
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for writing listing table format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for writing listing table format");
        }
        // always write multi-file output
        let path = if path.ends_with(object_store::path::DELIMITER) {
            path
        } else {
            format!("{path}{}", object_store::path::DELIMITER)
        };
        let table_paths = crate::url::resolve_listing_urls(ctx, vec![path.clone()]).await?;
        let object_store_url = if let Some(path) = table_paths.first() {
            path.object_store()
        } else {
            return internal_err!("empty listing table path: {path}");
        };
        // We do not need to specify the exact data type for partition columns,
        // since the type is inferred from the record batch during writing.
        // This is how DataFusion handles physical planning for `LogicalPlan::Copy`.
        let table_partition_cols = partition_by
            .iter()
            .map(|field| (field.column.clone(), DataType::Null))
            .collect::<Vec<_>>();
        let write_format = T::write(ctx, options)?;
        let (format, compression) = write_format.create_write_format()?;
        let file_extension = if let Some(file_compression_type) = format.compression_type() {
            match format.get_ext_with_compression(&file_compression_type) {
                Ok(ext) => ext,
                Err(_) => format.get_ext(),
            }
        } else {
            let ext = format.get_ext();
            if let Some(compression) = compression {
                if matches!(ext.as_str(), ".parquet" | "parquet") {
                    let ext = ext.strip_prefix('.').unwrap_or(&ext);
                    let compression = compression.strip_prefix('.').unwrap_or(&compression);
                    let (compression, _level) =
                        split_parquet_compression_string(&compression.to_lowercase())?;
                    let file_compression_type = FileCompressionType::from_str(compression.as_str());
                    let compression = match file_compression_type {
                        // Parquet has compression types not supported by FileCompressionType
                        Ok(compression) => compression.get_ext(),
                        Err(_) => compression,
                    };
                    let compression = compression.strip_prefix('.').unwrap_or(&compression);
                    let result = format!("{compression}.{ext}");
                    result
                } else {
                    ext
                }
            } else {
                ext
            }
        };
        // With the commit log enabled, this batch's data files go into a per-batch subdirectory
        // `<base>/<batchId>/` so committing only needs to list that bounded subdir; the commit
        // log itself lives at `<base>/_spark_metadata`. `batch_id` is derived from the same
        // checkpoint offset log the streaming driver uses, so the two stay in lockstep.
        let commit_ctx: Option<(u64, object_store::path::Path)> = match &commit_log_checkpoint {
            Some(cp) if streaming => {
                let batch_id = next_batch_id(cp);
                let base_store_path = table_paths
                    .first()
                    .map(|u| u.prefix().clone())
                    .unwrap_or_default();
                Some((batch_id, base_store_path))
            }
            _ => None,
        };
        let (sink_original_url, sink_table_paths) = match &commit_ctx {
            Some((batch_id, _)) => {
                let sub = format!("{path}{batch_id}{}", object_store::path::DELIMITER);
                let tp = crate::url::resolve_listing_urls(ctx, vec![sub.clone()]).await?;
                (sub, tp)
            }
            None => (path.clone(), table_paths.clone()),
        };
        let conf = FileSinkConfig {
            original_url: sink_original_url,
            object_store_url: object_store_url.clone(),
            file_group: Default::default(),
            table_paths: sink_table_paths,
            output_schema: input.schema(),
            table_partition_cols,
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension,
            file_output_mode: FileOutputMode::Automatic,
        };
        // Wrap a finished streaming write pipeline in the commit-log exec when enabled, else adapt
        // to the empty-schema completion stream the streaming driver expects.
        let finalize = |writer: Arc<dyn ExecutionPlan>| -> Arc<dyn ExecutionPlan> {
            match &commit_ctx {
                Some((batch_id, base)) => Arc::new(
                    crate::streaming_decode::StreamingSinkCommitExec::new(
                        writer,
                        object_store_url.clone(),
                        base.clone(),
                        *batch_id,
                    ),
                ),
                None => Arc::new(crate::streaming_decode::EmptySinkAdapterExec::new(writer)),
            }
        };
        // Parallel streaming write: fan the N-partition flow-event source into N independent
        // single-partition sinks (one file per source partition), driven concurrently. This
        // sidesteps DataFusion `DataSinkExec`'s single-partition requirement and gives ~N×
        // write throughput. See docs/design/streaming-parallelism.md (Phase 1).
        if streaming && n_parts > 1 {
            let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(n_parts);
            for i in 0..n_parts {
                let part = Arc::new(crate::streaming_decode::PartitionSelectExec::new(
                    Arc::clone(&flow_input),
                    i,
                ));
                let data_i = Arc::new(crate::streaming_decode::FlowEventToDataExec::try_new(part)?);
                let sink_i = format
                    .create_writer_physical_plan(data_i, ctx, conf.clone(), sort_order.clone())
                    .await?;
                children.push(sink_i);
            }
            let parallel = Arc::new(crate::streaming_decode::ParallelStreamSinkExec::new(children));
            return Ok(finalize(parallel));
        }
        let writer = format
            .create_writer_physical_plan(input, ctx, conf, sort_order)
            .await?;
        if streaming {
            // The streaming-query sink contract expects an empty-schema output; the file
            // writer emits a count row. Adapt it (draining triggers the durable writes).
            Ok(finalize(writer))
        } else {
            Ok(writer)
        }
    }
}

/// The next micro-batch id for a streaming query, derived from its committed offset log at
/// `<checkpoint>/offsets` — the same convention the streaming driver uses, so the sink's commit
/// log and the driver's offset/state commit stay in lockstep. Returns 0 when no batch has been
/// committed yet.
fn next_batch_id(checkpoint: &str) -> u64 {
    let dir = std::path::Path::new(checkpoint).join("offsets");
    std::fs::read_dir(&dir)
        .ok()
        .and_then(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().to_str().and_then(|s| s.parse::<u64>().ok()))
                .max()
        })
        .map(|m| m + 1)
        .unwrap_or(0)
}

/// Sink-side exactly-once on the read path: if any `base` directory carries a `_spark_metadata`
/// commit log, expand it to the explicit set of **committed** output files (reconstructing full
/// URLs store-agnostically, like the streaming file source), so the reader sees only committed
/// data and never the orphan/partial files of a crashed-then-retried micro-batch. Returns `None`
/// when no base is governed by a commit log (the caller then lists directories as usual).
async fn committed_urls_if_logged(
    ctx: &dyn Session,
    base_urls: &[ListingTableUrl],
) -> Result<Option<Vec<ListingTableUrl>>> {
    let mut out: Vec<ListingTableUrl> = vec![];
    let mut any_logged = false;
    for base in base_urls {
        let store = ctx.runtime_env().object_store(base)?;
        let base_path = base.prefix().clone();
        let committed = crate::streaming_sink_log::read_committed_files(&store, &base_path)
            .await
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?;
        match committed {
            Some(rel_paths) => {
                any_logged = true;
                let mut prefix = base.object_store().as_str().to_string();
                if !prefix.ends_with('/') {
                    prefix.push('/');
                }
                for p in rel_paths {
                    out.push(ListingTableUrl::parse(format!("{prefix}{}", p.as_ref()))?);
                }
            }
            None => out.push(base.clone()),
        }
    }
    Ok(any_logged.then_some(out))
}
