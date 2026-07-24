//! Subprocess-based UDF worker.
//!
//! When `ZELOX_PYTHON` is set (installed by `install.sh` to the venv's
//! `python3`), UDF execution is routed through a child process running
//! `udf_worker.py` under that Python interpreter instead of the PyO3-embedded
//! Python.  This decouples the binary's compiled Python version from the
//! user's venv Python version, mirroring Apache Spark's `pyspark/worker.py`
//! approach.
//!
//! The worker process is kept alive for the lifetime of the thread (one
//! worker per thread via `thread_local!`).

use std::cell::RefCell;
use std::io::{Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;

use base64::Engine as _;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;

use crate::error::{PyUdfError, PyUdfResult};

/// Source of `udf_worker.py` (embedded in the binary at compile time).
const UDF_WORKER_SOURCE: &str = include_str!("python/udf_worker.py");
/// Source of `spark.py` (embedded so the worker can exec it at runtime).
const SPARK_PY_SOURCE: &str = include_str!("python/spark.py");

// ---------------------------------------------------------------------------
// Thread-local worker cache
// ---------------------------------------------------------------------------

thread_local! {
    static WORKER: RefCell<Option<UdfWorker>> = const { RefCell::new(None) };
}

/// Returns `true` if `ZELOX_PYTHON` is set (subprocess mode enabled).
pub fn is_subprocess_mode() -> bool {
    std::env::var_os("ZELOX_PYTHON").is_some()
}

/// Execute `f` with a mutable reference to the thread-local worker,
/// spawning it on first use.  Returns `None` if subprocess mode is not
/// enabled.
pub fn with_worker<F, T>(f: F) -> Option<PyUdfResult<T>>
where
    F: FnOnce(&mut UdfWorker) -> PyUdfResult<T>,
{
    if !is_subprocess_mode() {
        return None;
    }
    Some(WORKER.with(|cell| {
        let mut guard = cell.borrow_mut();
        // Check if existing worker is alive; if not, clear it.
        let needs_spawn = match &mut *guard {
            Some(w) => !w.is_alive(),
            None => true,
        };
        if needs_spawn {
            *guard = None; // drop dead worker
            match UdfWorker::spawn() {
                Ok(w) => *guard = Some(w),
                Err(e) => return Err(e),
            }
        }
        match guard.as_mut() {
            Some(worker) => f(worker),
            // Unreachable: a worker was just ensured above. Handle explicitly
            // rather than unwrap() (workspace denies unwrap_used).
            None => Err(PyUdfError::invalid("UDF worker unexpectedly absent")),
        }
    }))
}

// ---------------------------------------------------------------------------
// Worker struct
// ---------------------------------------------------------------------------

pub struct UdfWorker {
    process: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    /// Temp file holding `spark.py` — kept alive so the path remains valid.
    _spark_py_tmp: tempfile::NamedTempFile,
    /// Temp file holding `udf_worker.py` — kept alive so the path remains valid.
    _worker_tmp: tempfile::NamedTempFile,
}

impl Drop for UdfWorker {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

impl UdfWorker {
    /// Spawn a new worker subprocess using `$ZELOX_PYTHON`.
    pub fn spawn() -> PyUdfResult<Self> {
        use std::io::Write as _;

        let python = std::env::var("ZELOX_PYTHON")
            .map_err(|_| PyUdfError::internal("ZELOX_PYTHON not set"))?;

        // Write spark.py to a temp file so the worker can import it.
        let mut spark_py_tmp =
            tempfile::NamedTempFile::new().map_err(PyUdfError::IoError)?;
        spark_py_tmp
            .write_all(SPARK_PY_SOURCE.as_bytes())
            .map_err(PyUdfError::IoError)?;
        spark_py_tmp.flush().map_err(PyUdfError::IoError)?;

        // Write udf_worker.py to a temp file.
        let mut worker_tmp =
            tempfile::NamedTempFile::new().map_err(PyUdfError::IoError)?;
        worker_tmp
            .write_all(UDF_WORKER_SOURCE.as_bytes())
            .map_err(PyUdfError::IoError)?;
        worker_tmp.flush().map_err(PyUdfError::IoError)?;

        let spark_py_path = spark_py_tmp.path().to_owned();
        let worker_path = worker_tmp.path().to_owned();

        let mut child = Command::new(&python)
            .arg(&worker_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .env("ZELOX_SPARK_PY", &spark_py_path)
            .spawn()
            .map_err(|e| PyUdfError::internal(format!("failed to spawn UDF worker: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| PyUdfError::internal("no stdin on UDF worker"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| PyUdfError::internal("no stdout on UDF worker"))?;

        Ok(Self {
            process: child,
            stdin,
            stdout,
            _spark_py_tmp: spark_py_tmp,
            _worker_tmp: worker_tmp,
        })
    }

    /// Returns `true` if the child process is still running.
    pub fn is_alive(&mut self) -> bool {
        matches!(self.process.try_wait(), Ok(None))
    }

    /// Execute a scalar UDF on a batch of inputs.
    ///
    /// # Arguments
    /// - `payload` — raw UDF bytes (the Spark serializer format, as stored in `PySparkUDF.payload`)
    /// - `kind` — UDF kind string ("batch", "arrow_batch", etc.)
    /// - `eval_type` — PySpark eval_type integer (big-endian first 4 bytes of `payload`)
    /// - `input_types` — Arrow DataTypes of input columns
    /// - `output_type` — Arrow DataType of output
    /// - `args` — input Arrow arrays
    /// - `number_rows` — number of rows in the batch
    #[expect(clippy::too_many_arguments)]
    pub fn execute_scalar(
        &mut self,
        payload: &[u8],
        kind: &str,
        eval_type: i32,
        input_types: &[DataType],
        output_type: &DataType,
        args: &[ArrayRef],
        config: &crate::config::PySparkUdfConfig,
        number_rows: usize,
    ) -> PyUdfResult<ArrayRef> {
        // ---- serialise Arrow types as IPC schema bytes → base64 ----
        let input_type_blobs: Vec<String> = input_types
            .iter()
            .map(serialize_type_b64)
            .collect::<PyUdfResult<_>>()?;
        let output_type_blob = serialize_type_b64(output_type)?;

        // ---- build header JSON ----
        let config_json = serde_json::json!({
            "session_timezone": config.session_timezone,
            "assign_columns_by_name": config.pandas_grouped_map_assign_columns_by_name,
            "arrow_convert_safely": config.pandas_convert_to_arrow_array_safely,
            "arrow_max_records_per_batch": config.arrow_max_records_per_batch,
            "python_udf_pandas_conversion_enabled": config.python_udf_pandas_conversion_enabled,
            "python_udtf_pandas_conversion_enabled": config.python_udtf_pandas_conversion_enabled,
            "python_udf_pandas_int_to_decimal_coercion_enabled": config.python_udf_pandas_int_to_decimal_coercion_enabled,
            "binary_as_bytes": config.binary_as_bytes,
        });

        let header = serde_json::json!({
            "eval_type": eval_type,
            "udf_kind": kind,
            "number_rows": number_rows,
            "input_type_blobs": input_type_blobs,
            "output_type_blob": output_type_blob,
            "config": config_json,
        });
        let header_bytes = serde_json::to_vec(&header)
            .map_err(|e| PyUdfError::internal(format!("header serialisation: {e}")))?;

        // ---- serialise input Arrow arrays as IPC stream ----
        // The worker expects a RecordBatch stream where each column is one input.
        let arrow_batch_bytes = if args.is_empty() {
            Vec::new()
        } else {
            serialize_arrays_as_ipc_stream(args, number_rows)?
        };

        // ---- write to worker stdin ----
        write_u32_le(&mut self.stdin, header_bytes.len() as u32)?;
        self.stdin.write_all(&header_bytes)?;

        // payload: strip the 4-byte eval_type prefix (it's already in the header)
        // The actual UDF bytes expected by pyspark.worker.read_udfs start after eval_type.
        let udf_data = if payload.len() >= 4 { &payload[4..] } else { payload };
        write_u32_le(&mut self.stdin, udf_data.len() as u32)?;
        self.stdin.write_all(udf_data)?;

        write_u32_le(&mut self.stdin, arrow_batch_bytes.len() as u32)?;
        if !arrow_batch_bytes.is_empty() {
            self.stdin.write_all(&arrow_batch_bytes)?;
        }
        self.stdin.flush()?;

        // ---- read response from stdout ----
        let status = read_u8(&mut self.stdout)?;
        let result_len = read_u32_le(&mut self.stdout)?;
        let result_bytes = read_exact(&mut self.stdout, result_len as usize)?;

        if status == 0x01 {
            let msg = String::from_utf8_lossy(&result_bytes).into_owned();
            return Err(PyUdfError::internal(format!("UDF worker error:\n{msg}")));
        }

        // Decode result Arrow IPC stream → single array
        let array = deserialize_ipc_stream_to_array(&result_bytes)?;
        Ok(array)
    }
}

// ---------------------------------------------------------------------------
// Serialisation helpers
// ---------------------------------------------------------------------------

/// Serialise a single DataType as a one-field Arrow IPC schema, then base64-encode it.
fn serialize_type_b64(dt: &DataType) -> PyUdfResult<String> {
    use datafusion::arrow::datatypes::{Field, Schema};

    let schema = Schema::new(vec![Field::new("_", dt.clone(), true)]);
    let mut buf = Vec::new();
    {
        // Write IPC schema message only (no record batch).
        // We use an empty StreamWriter then immediately finish — that writes
        // the schema followed by the EOS marker.
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| PyUdfError::internal(format!("IPC schema write: {e}")))?;
        writer
            .finish()
            .map_err(|e| PyUdfError::internal(format!("IPC schema finish: {e}")))?;
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(&buf))
}

/// Serialise a slice of `ArrayRef` as an Arrow IPC stream (single RecordBatch).
fn serialize_arrays_as_ipc_stream(
    args: &[ArrayRef],
    number_rows: usize,
) -> PyUdfResult<Vec<u8>> {
    use datafusion::arrow::datatypes::{Field, Schema};

    let fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("_{i}"), a.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema.clone(), args.to_vec())
        .map_err(|e| PyUdfError::internal(format!("RecordBatch construction: {e}")))?;

    // Override row count for zero-column case.
    let batch = if args.is_empty() {
        use datafusion::arrow::array::RecordBatchOptions;
        RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(number_rows)),
        )
        .map_err(|e| PyUdfError::internal(format!("RecordBatch 0-col: {e}")))?
    } else {
        batch
    };

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())
        .map_err(|e| PyUdfError::internal(format!("IPC stream write: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| PyUdfError::internal(format!("IPC write batch: {e}")))?;
    writer
        .finish()
        .map_err(|e| PyUdfError::internal(format!("IPC finish: {e}")))?;
    Ok(buf)
}

/// Read an Arrow IPC stream produced by the worker (one RecordBatch, one column).
fn deserialize_ipc_stream_to_array(data: &[u8]) -> PyUdfResult<ArrayRef> {
    use std::io::Cursor;

    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| PyUdfError::internal(format!("IPC stream read: {e}")))?;
    let batch = reader
        .next()
        .ok_or_else(|| PyUdfError::internal("worker returned empty IPC stream"))?
        .map_err(|e| PyUdfError::internal(format!("IPC batch read: {e}")))?;
    if batch.num_columns() == 0 {
        return Err(PyUdfError::internal("worker returned 0-column result batch"));
    }
    Ok(batch.column(0).clone())
}

// ---------------------------------------------------------------------------
// Low-level I/O helpers
// ---------------------------------------------------------------------------

fn write_u32_le(w: &mut impl Write, val: u32) -> PyUdfResult<()> {
    w.write_all(&val.to_le_bytes()).map_err(PyUdfError::IoError)
}

fn read_u8(r: &mut impl Read) -> PyUdfResult<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf).map_err(PyUdfError::IoError)?;
    Ok(buf[0])
}

fn read_u32_le(r: &mut impl Read) -> PyUdfResult<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).map_err(PyUdfError::IoError)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_exact(r: &mut impl Read, n: usize) -> PyUdfResult<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).map_err(PyUdfError::IoError)?;
    Ok(buf)
}
