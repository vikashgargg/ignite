/// Permissive JSON reading: invalid lines become null rows instead of errors.
///
/// This implements Spark's PERMISSIVE mode for the user-provided-schema case.
/// Malformed JSON lines (e.g. CSV lines in a mixed directory) are replaced
/// with an empty JSON object `{}` so that every declared column comes back
/// as null rather than causing an `ArrowError`.
///
/// The no-schema / `_corrupt_record` variant is a separate, future task (C5).
use std::any::Any;
use std::fmt;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::ReaderBuilder;
use datafusion_common::config::JsonOptions;
use datafusion_common::{Result, Statistics};
use datafusion_datasource::decoder::{Decoder, DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, RangeCalculation, TableSchema, calculate_range};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion_session::Session;
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectMeta, ObjectStore};

// ---------------------------------------------------------------------------
// PermissiveJsonDecoder — Decoder trait implementation
// ---------------------------------------------------------------------------

/// Wraps Arrow's JSON decoder with lenient per-line validation.
/// Invalid JSON lines are replaced with `{}` (empty object) so that
/// each column for that row comes back as null.
pub struct PermissiveJsonDecoder {
    inner: datafusion::arrow::json::reader::Decoder,
    /// Bytes not yet terminated by a newline.
    buf: Vec<u8>,
}

impl fmt::Debug for PermissiveJsonDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermissiveJsonDecoder").finish()
    }
}

impl PermissiveJsonDecoder {
    pub fn new(schema: SchemaRef, batch_size: usize) -> Self {
        let inner = ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .build_decoder()
            .expect("building JSON decoder should not fail");
        Self {
            inner,
            buf: Vec::new(),
        }
    }

    fn drain_complete_lines(&mut self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.buf.len());
        let mut start = 0;
        for i in 0..self.buf.len() {
            if self.buf[i] == b'\n' {
                let line = &self.buf[start..i];
                let trimmed = trim_ascii(line);
                if !trimmed.is_empty() {
                    if serde_json::from_slice::<serde_json::Value>(trimmed).is_ok() {
                        output.extend_from_slice(line);
                    } else {
                        output.extend_from_slice(b"{}");
                    }
                    output.push(b'\n');
                }
                start = i + 1;
            }
        }
        let remaining = self.buf[start..].to_vec();
        self.buf = remaining;
        output
    }

    fn drain_remaining(&mut self) -> Vec<u8> {
        let trimmed: Vec<u8> = trim_ascii(&self.buf).to_vec();
        self.buf.clear();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let json_line = if serde_json::from_slice::<serde_json::Value>(&trimmed).is_ok() {
            trimmed
        } else {
            b"{}".to_vec()
        };
        let mut output = json_line;
        output.push(b'\n');
        output
    }
}

impl Decoder for PermissiveJsonDecoder {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let n = buf.len();
        self.buf.extend_from_slice(buf);
        let cleaned = self.drain_complete_lines();
        if !cleaned.is_empty() {
            self.inner.decode(&cleaned)?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let remaining = self.drain_remaining();
        if !remaining.is_empty() {
            self.inner.decode(&remaining)?;
        }
        self.inner.flush()
    }

    fn can_flush_early(&self) -> bool {
        false
    }
}

fn trim_ascii(b: &[u8]) -> &[u8] {
    let start = b.iter().position(|c| !c.is_ascii_whitespace()).unwrap_or(b.len());
    let end = b.iter().rposition(|c| !c.is_ascii_whitespace()).map(|i| i + 1).unwrap_or(0);
    if start >= end { &[] } else { &b[start..end] }
}

// ---------------------------------------------------------------------------
// PermissiveJsonOpener — FileOpener
// ---------------------------------------------------------------------------

struct PermissiveJsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for PermissiveJsonOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_schema);
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type.to_owned();

        Ok(Box::pin(async move {
            let calculated_range = calculate_range(&partitioned_file, &store, None).await?;
            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(r)) => Some(r.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(futures::stream::poll_fn(|_| std::task::Poll::Ready(None)).boxed());
                }
            };
            let options = GetOptions { range, ..Default::default() };
            let result = store
                .get_opts(&partitioned_file.object_meta.location, options)
                .await?;

            let decoder = PermissiveJsonDecoder::new(Arc::clone(&schema), batch_size);
            let deser = DecoderDeserializer::new(decoder);

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let bytes_reader: Box<dyn Read> = match partitioned_file.range {
                        None => Box::new(file_compression_type.convert_read(file)?),
                        Some(_) => {
                            file.seek(SeekFrom::Start(result.range.start as _))?;
                            let limit = result.range.end - result.range.start;
                            Box::new(file_compression_type.convert_read(file.take(limit))?)
                        }
                    };
                    let mut data = Vec::new();
                    BufReader::new(bytes_reader).read_to_end(&mut data)?;
                    let bytes = Bytes::from(data);
                    let input = futures::stream::once(std::future::ready(
                        Ok::<_, datafusion_common::DataFusionError>(bytes),
                    ));
                    let stream = deserialize_stream(input, deser);
                    Ok(stream.map_err(Into::into).boxed())
                }
                GetResultPayload::Stream(s) => {
                    let s = s.map_err(datafusion_common::DataFusionError::from);
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();
                    let stream = deserialize_stream(input, deser);
                    Ok(stream.map_err(Into::into).boxed())
                }
            }
        }))
    }
}

// ---------------------------------------------------------------------------
// PermissiveJsonSource — FileSource
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PermissiveJsonSource {
    table_schema: TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl PermissiveJsonSource {
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
        }
    }
}

impl fmt::Debug for PermissiveJsonSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermissiveJsonSource").finish()
    }
}

impl FileSource for PermissiveJsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let projected_schema =
            Arc::new(file_schema.project(&self.projection.file_indices)?);

        let mut opener: Arc<dyn FileOpener> = Arc::new(PermissiveJsonOpener {
            batch_size: self
                .batch_size
                .expect("batch size must be set before creating opener"),
            projected_schema,
            file_compression_type: base_config.file_compression_type,
            object_store,
        });

        opener = ProjectionOpener::try_new(
            self.projection.clone(),
            Arc::clone(&opener),
            self.table_schema.file_schema(),
        )?;

        Ok(opener)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut s = self.clone();
        s.batch_size = Some(batch_size);
        Arc::new(s)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "json"
    }
}

// ---------------------------------------------------------------------------
// PermissiveJsonFormat — FileFormat
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PermissiveJsonFormat {
    options: JsonOptions,
}

impl PermissiveJsonFormat {
    pub fn new(options: JsonOptions) -> Self {
        Self { options }
    }

    fn inner(&self) -> datafusion::datasource::file_format::json::JsonFormat {
        datafusion::datasource::file_format::json::JsonFormat::default()
            .with_options(self.options.clone())
    }
}

#[async_trait::async_trait]
impl FileFormat for PermissiveJsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner().get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner().compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner().infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner().infer_stats(state, store, table_schema, object).await
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(FileCompressionType::from(self.options.compression))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(PermissiveJsonSource::new(table_schema))
    }
}
