/// Permissive JSON reading: supports PERMISSIVE / DROPMALFORMED / FAILFAST modes.
///
/// PERMISSIVE (default): malformed lines produce a null row; if the schema includes
/// a `_corrupt_record` column (or the column named by `column_name_of_corrupt_record`),
/// the raw malformed line is captured there.
///
/// DROPMALFORMED: malformed lines are silently skipped.
///
/// FAILFAST: any malformed line causes the read to abort with an error.
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::ReaderBuilder;
use datafusion::arrow::json::reader::infer_json_schema_from_iterator;
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
use object_store::{GetOptions, GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt};

// ---------------------------------------------------------------------------
// JsonMode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum JsonMode {
    Permissive,
    DropMalformed,
    FailFast,
}

impl JsonMode {
    fn parse(s: &str) -> Self {
        match s.trim().to_uppercase().as_str() {
            "DROPMALFORMED" => JsonMode::DropMalformed,
            "FAILFAST" => JsonMode::FailFast,
            _ => JsonMode::Permissive,
        }
    }
}

// ---------------------------------------------------------------------------
// PermissiveJsonDecoder — Decoder trait implementation
// ---------------------------------------------------------------------------

/// Wraps Arrow's JSON decoder and implements Spark's three read modes.
///
/// When `corrupt_col_idx` is set (schema contains `_corrupt_record`), the raw
/// text of each malformed line is injected into that column; all other fields
/// for that row become null.  Valid rows get a null in `_corrupt_record`.
///
/// Multi-batch design: lines are staged in `line_queue` before being fed to
/// the Arrow inner decoder one at a time.  When the inner decoder fills a
/// batch (returns 0), we self-flush it into `batch_queue` and continue
/// draining `line_queue`.  The outer framework drains `batch_queue` via
/// `flush()` + `can_flush_early()`.  This is correct for files of any size.
pub struct PermissiveJsonDecoder {
    inner: datafusion::arrow::json::reader::Decoder,
    /// Raw input buffer — holds bytes of the current incomplete line.
    buf: Vec<u8>,
    mode: JsonMode,
    corrupt_col_idx: Option<usize>,
    output_schema: Option<SchemaRef>,
    /// Metadata for rows currently in the inner decoder's buffer.
    /// `None` = valid row, `Some(bytes)` = raw malformed line.
    pending_meta: VecDeque<Option<Vec<u8>>>,
    /// Cleaned lines that have been validated/transformed but not yet fed to
    /// the inner decoder.  Each entry: (cleaned_line_with_newline, corrupt_meta).
    line_queue: VecDeque<(Vec<u8>, Option<Vec<u8>>)>,
    /// Completed RecordBatches produced when the inner decoder overflowed.
    /// Drained by `flush()` / `can_flush_early()`.
    batch_queue: VecDeque<RecordBatch>,
}

impl fmt::Debug for PermissiveJsonDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermissiveJsonDecoder").finish()
    }
}

impl PermissiveJsonDecoder {
    pub fn new(
        inner_schema: SchemaRef,
        batch_size: usize,
        mode: JsonMode,
        corrupt_col_idx: Option<usize>,
        output_schema: Option<SchemaRef>,
    ) -> Self {
        let inner = ReaderBuilder::new(inner_schema)
            .with_batch_size(batch_size)
            .build_decoder()
            .expect("building JSON decoder should not fail");
        Self {
            inner,
            buf: Vec::new(),
            mode,
            corrupt_col_idx,
            output_schema,
            pending_meta: VecDeque::new(),
            line_queue: VecDeque::new(),
            batch_queue: VecDeque::new(),
        }
    }

    /// Scan `self.buf` for complete (newline-terminated) lines, validate/
    /// transform each one per mode, and push the result into `self.line_queue`.
    /// Leaves any trailing incomplete line in `self.buf`.
    fn drain_buf_to_queue(&mut self) -> Result<(), ArrowError> {
        let mut start = 0;
        for i in 0..self.buf.len() {
            if self.buf[i] == b'\n' {
                let trimmed = trim_ascii(&self.buf[start..i]).to_vec();
                if !trimmed.is_empty() {
                    self.classify_line(&trimmed)?;
                }
                start = i + 1;
            }
        }
        let remaining = self.buf[start..].to_vec();
        self.buf = remaining;
        Ok(())
    }

    /// Process the partial line remaining in `self.buf` (end-of-stream).
    fn flush_buf_to_queue(&mut self) -> Result<(), ArrowError> {
        let trimmed: Vec<u8> = trim_ascii(&self.buf).to_vec();
        self.buf.clear();
        if !trimmed.is_empty() {
            self.classify_line(&trimmed)?;
        }
        Ok(())
    }

    /// Validate one line and push a `(cleaned_bytes, meta)` entry into
    /// `self.line_queue` (or return an error in FAILFAST mode).
    fn classify_line(&mut self, trimmed: &[u8]) -> Result<(), ArrowError> {
        let is_valid = serde_json::from_slice::<serde_json::Value>(trimmed).is_ok();
        if is_valid {
            let mut cleaned = trimmed.to_vec();
            cleaned.push(b'\n');
            self.line_queue.push_back((cleaned, None));
        } else {
            match self.mode {
                JsonMode::Permissive => {
                    let meta = if self.corrupt_col_idx.is_some() {
                        Some(trimmed.to_vec())
                    } else {
                        None
                    };
                    self.line_queue.push_back((b"{}\n".to_vec(), meta));
                }
                JsonMode::DropMalformed => { /* skip */ }
                JsonMode::FailFast => {
                    return Err(ArrowError::ParseError(format!(
                        "Malformed JSON record (FAILFAST mode): {}",
                        String::from_utf8_lossy(trimmed)
                    )));
                }
            }
        }
        Ok(())
    }

    /// Feed entries from `self.line_queue` into the inner Arrow decoder.
    ///
    /// When the inner decoder fills a batch (returns 0 bytes consumed), we
    /// flush it ourselves, inject the corrupt column, and push the result into
    /// `self.batch_queue`.  Continues until `line_queue` is empty.
    fn drain_queue_to_inner(&mut self) -> Result<(), ArrowError> {
        while !self.line_queue.is_empty() {
            let consumed = {
                let (line_bytes, _) = self.line_queue.front().unwrap();
                self.inner.decode(line_bytes)?
            };
            if consumed == 0 {
                // Inner is full: self-flush and continue.
                if let Some(batch) = self.inner.flush()? {
                    let batch = self.inject_corrupt_col(batch)?;
                    self.batch_queue.push_back(batch);
                }
                continue; // retry the same front entry
            }
            let (_, meta) = self.line_queue.pop_front().unwrap();
            if self.corrupt_col_idx.is_some() {
                self.pending_meta.push_back(meta);
            }
        }
        Ok(())
    }

    /// Inject the `_corrupt_record` column into a batch produced by the inner
    /// decoder, draining the matching entries from `pending_meta`.
    fn inject_corrupt_col(&mut self, batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
        let (idx, schema) = match (&self.corrupt_col_idx, &self.output_schema) {
            (Some(i), Some(s)) => (*i, Arc::clone(s)),
            _ => return Ok(batch),
        };
        let n = batch.num_rows();
        let metas: Vec<Option<Vec<u8>>> = self.pending_meta.drain(..n).collect();
        let corrupt_array: ArrayRef = Arc::new(StringArray::from(
            metas
                .iter()
                .map(|m| m.as_deref().and_then(|b| std::str::from_utf8(b).ok()))
                .collect::<Vec<Option<&str>>>(),
        ));
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.insert(idx, corrupt_array);
        RecordBatch::try_new(schema, columns)
    }
}

impl Decoder for PermissiveJsonDecoder {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let n = buf.len();

        if n == 0 {
            // End-of-stream: process any partial final line, then drain.
            self.flush_buf_to_queue()?;
            self.drain_queue_to_inner()?;
            return Ok(0); // triggers flush()
        }

        self.buf.extend_from_slice(buf);
        self.drain_buf_to_queue()?;
        self.drain_queue_to_inner()?;
        Ok(n)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        // Return completed overflow batches before asking inner to flush.
        if let Some(batch) = self.batch_queue.pop_front() {
            return Ok(Some(batch));
        }
        match self.inner.flush()? {
            None => Ok(None),
            Some(batch) => self.inject_corrupt_col(batch).map(Some),
        }
    }

    fn can_flush_early(&self) -> bool {
        !self.batch_queue.is_empty()
    }
}

fn trim_ascii(b: &[u8]) -> &[u8] {
    let start = b
        .iter()
        .position(|c| !c.is_ascii_whitespace())
        .unwrap_or(b.len());
    let end = b
        .iter()
        .rposition(|c| !c.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(0);
    if start >= end {
        &[]
    } else {
        &b[start..end]
    }
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

/// Effective name of the corrupt-record column: explicit option > default `_corrupt_record`.
fn corrupt_col_name(explicit: &str) -> &str {
    if explicit.is_empty() {
        "_corrupt_record"
    } else {
        explicit
    }
}

/// Split a schema into (inner_schema_without_corrupt_col, Option<(corrupt_col_idx, full_schema)>).
fn split_corrupt_col(
    schema: &SchemaRef,
    col_name: &str,
) -> (SchemaRef, Option<(usize, SchemaRef)>) {
    let name = corrupt_col_name(col_name);
    let idx = schema.fields().iter().position(|f| f.name() == name);
    match idx {
        None => (Arc::clone(schema), None),
        Some(i) => {
            let inner_fields: Vec<_> = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, f)| Arc::clone(f))
                .collect();
            let inner = Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
                inner_fields,
                schema.metadata().clone(),
            ));
            (inner, Some((i, Arc::clone(schema))))
        }
    }
}

// ---------------------------------------------------------------------------
// PermissiveJsonOpener — FileOpener
// ---------------------------------------------------------------------------

struct PermissiveJsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    mode: JsonMode,
    corrupt_col_name: String,
}

impl FileOpener for PermissiveJsonOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_schema);
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type.to_owned();
        let mode = self.mode.clone();
        let col_name = self.corrupt_col_name.clone();

        Ok(Box::pin(async move {
            let calculated_range =
                calculate_range(&partitioned_file, &store, None).await?;
            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(r)) => Some(r.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(|_| std::task::Poll::Ready(None)).boxed(),
                    );
                }
            };
            let options = GetOptions {
                range,
                ..Default::default()
            };
            let result = store
                .get_opts(&partitioned_file.object_meta.location, options)
                .await?;

            let (inner_schema, corrupt_info) = split_corrupt_col(&schema, &col_name);
            let (corrupt_col_idx, output_schema) = match corrupt_info {
                Some((idx, s)) => (Some(idx), Some(s)),
                None => (None, None),
            };
            let decoder = PermissiveJsonDecoder::new(
                inner_schema,
                batch_size,
                mode,
                corrupt_col_idx,
                output_schema,
            );
            let deser = DecoderDeserializer::new(decoder);

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let bytes_reader: Box<dyn Read> = match partitioned_file.range {
                        None => Box::new(file_compression_type.convert_read(file)?),
                        Some(_) => {
                            file.seek(SeekFrom::Start(result.range.start as _))?;
                            let limit = result.range.end - result.range.start;
                            Box::new(
                                file_compression_type.convert_read(file.take(limit))?,
                            )
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
    mode: JsonMode,
    corrupt_col_name: String,
}

impl PermissiveJsonSource {
    pub fn new(
        table_schema: impl Into<TableSchema>,
        mode: JsonMode,
        corrupt_col_name: String,
    ) -> Self {
        let table_schema = table_schema.into();
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
            mode,
            corrupt_col_name,
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
            mode: self.mode.clone(),
            corrupt_col_name: self.corrupt_col_name.clone(),
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
    mode: JsonMode,
    corrupt_col_name: String,
}

impl PermissiveJsonFormat {
    pub fn new(options: JsonOptions, mode: String, corrupt_col_name: String) -> Self {
        Self {
            options,
            mode: JsonMode::parse(&mode),
            corrupt_col_name,
        }
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
        if self.mode != JsonMode::Permissive {
            return self.inner().infer_schema(state, store, objects).await;
        }

        // PERMISSIVE mode: DataFusion's default inferrer errors on malformed lines.
        // We filter to valid JSON lines only, infer from those, then append _corrupt_record.
        let max_records = self.options.schema_infer_max_rec.unwrap_or(1000);
        let mut merged_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> = Vec::new();
        let mut merged_meta = std::collections::HashMap::new();

        for object in objects {
            let raw = store.get(&object.location).await?.bytes().await?;
            let mut values: Vec<serde_json::Value> = Vec::new();
            for line in raw.split(|b| *b == b'\n') {
                let trimmed = trim_ascii(line);
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(trimmed) {
                    values.push(v);
                    if values.len() >= max_records {
                        break;
                    }
                }
            }
            if values.is_empty() {
                continue;
            }
            let file_schema = infer_json_schema_from_iterator(
                values.iter().map(|v| Ok::<_, ArrowError>(v)),
            )?;
            merged_meta.extend(file_schema.metadata().clone());
            for field in file_schema.fields() {
                if !merged_fields.iter().any(|f| f.name() == field.name()) {
                    merged_fields.push(Arc::clone(field));
                }
            }
        }

        // Append _corrupt_record unless already present.
        let col = corrupt_col_name(&self.corrupt_col_name);
        if !merged_fields.iter().any(|f| f.name() == col) {
            merged_fields.push(Arc::new(datafusion::arrow::datatypes::Field::new(
                col,
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            )));
        }
        Ok(Arc::new(
            datafusion::arrow::datatypes::Schema::new_with_metadata(merged_fields, merged_meta),
        ))
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner()
            .infer_stats(state, store, table_schema, object)
            .await
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
        Arc::new(PermissiveJsonSource::new(
            table_schema,
            self.mode.clone(),
            self.corrupt_col_name.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_datasource::decoder::Decoder;

    use super::{JsonMode, PermissiveJsonDecoder, split_corrupt_col};

    fn schema_with_corrupt() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("_corrupt_record", DataType::Utf8, true),
        ]))
    }

    fn schema_without_corrupt() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn make_decoder(schema: &Arc<Schema>, mode: JsonMode, col: &str) -> PermissiveJsonDecoder {
        let (inner_schema, corrupt_info) = split_corrupt_col(schema, col);
        let (idx, out) = match corrupt_info {
            Some((i, s)) => (Some(i), Some(s)),
            None => (None, None),
        };
        PermissiveJsonDecoder::new(inner_schema, 1024, mode, idx, out)
    }

    #[test]
    fn test_permissive_all_valid() {
        let schema = schema_without_corrupt();
        let mut dec = make_decoder(&schema, JsonMode::Permissive, "");

        let input = b"{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n";
        dec.decode(input).unwrap();
        let batch = dec.flush().unwrap().expect("should have a batch");
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_permissive_with_corrupt_col() {
        let schema = schema_with_corrupt();
        let mut dec = make_decoder(&schema, JsonMode::Permissive, "_corrupt_record");

        let input = b"{\"id\":1,\"name\":\"alice\"}\nnot_json\n{\"id\":3,\"name\":\"carol\"}\n";
        dec.decode(input).unwrap();
        let batch = dec.flush().unwrap().expect("batch");
        assert_eq!(batch.num_rows(), 3);

        // Find the _corrupt_record column
        let idx = batch.schema().index_of("_corrupt_record").unwrap();
        let corrupt_col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert!(corrupt_col.is_null(0), "row 0 is valid, should be null");
        assert_eq!(corrupt_col.value(1), "not_json", "row 1 is malformed");
        assert!(corrupt_col.is_null(2), "row 2 is valid, should be null");
    }

    #[test]
    fn test_dropmalformed() {
        let schema = schema_without_corrupt();
        let mut dec = make_decoder(&schema, JsonMode::DropMalformed, "");

        let input = b"{\"id\":1,\"name\":\"alice\"}\nbad\n{\"id\":3,\"name\":\"carol\"}\n";
        dec.decode(input).unwrap();
        let batch = dec.flush().unwrap().expect("batch");
        assert_eq!(batch.num_rows(), 2, "malformed row dropped");
    }

    #[test]
    fn test_failfast() {
        let schema = schema_without_corrupt();
        let mut dec = make_decoder(&schema, JsonMode::FailFast, "");

        let input = b"{\"id\":1,\"name\":\"alice\"}\nbad_json\n";
        let result = dec.decode(input);
        assert!(result.is_err(), "FAILFAST should error on malformed line");
    }

    #[test]
    fn test_split_corrupt_col_present() {
        let schema = schema_with_corrupt();
        let (inner, info) = split_corrupt_col(&schema, "_corrupt_record");
        assert_eq!(inner.fields().len(), 2);
        let (idx, full) = info.unwrap();
        assert_eq!(idx, 2);
        assert_eq!(full.fields().len(), 3);
    }

    #[test]
    fn test_split_corrupt_col_absent() {
        let schema = schema_without_corrupt();
        let (inner, info) = split_corrupt_col(&schema, "_corrupt_record");
        assert_eq!(inner.fields().len(), 2);
        assert!(info.is_none());
    }

    /// Test the full streaming pipeline as used by PermissiveJsonOpener:
    /// DecoderDeserializer wrapping PermissiveJsonDecoder, fed via deserialize_stream.
    #[tokio::test]
    async fn test_streaming_pipeline_permissive() {
        use bytes::Bytes;
        use datafusion_common::DataFusionError;
        use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
        use futures::TryStreamExt;

        let schema = schema_with_corrupt();
        let (inner_schema, corrupt_info) = split_corrupt_col(&schema, "_corrupt_record");
        let (corrupt_col_idx, output_schema) = match corrupt_info {
            Some((i, s)) => (Some(i), Some(s)),
            None => (None, None),
        };
        let decoder = PermissiveJsonDecoder::new(
            inner_schema,
            1024,
            JsonMode::Permissive,
            corrupt_col_idx,
            output_schema,
        );
        let deser = DecoderDeserializer::new(decoder);

        let data =
            Bytes::from_static(b"{\"id\":1,\"name\":\"alice\"}\nbad_json\n{\"id\":3,\"name\":\"carol\"}\n");
        let input = futures::stream::once(std::future::ready(
            Ok::<_, DataFusionError>(data),
        ));
        let batches: Vec<_> = deserialize_stream(input, deser)
            .map_err(|e: datafusion::arrow::error::ArrowError| {
                datafusion_common::DataFusionError::from(e)
            })
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        let batch = &batches[0];
        let idx = batch.schema().index_of("_corrupt_record").unwrap();
        let corrupt_col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert!(corrupt_col.is_null(0), "valid row → null corrupt_record");
        assert_eq!(corrupt_col.value(1), "bad_json", "malformed row captured");
        assert!(corrupt_col.is_null(2), "valid row → null corrupt_record");
    }
}
