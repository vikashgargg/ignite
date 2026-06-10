//! Operator-state snapshot/restore for stateful exactly-once recovery.
//!
//! Stateful streaming operators (windowed aggregation, joins) keep their state as
//! `Vec<RecordBatch>`. On a checkpoint they **stage** that state (write-ahead) under
//! `<checkpointLocation>/state/<operator-id>/staged/`; the runner promotes staged →
//! committed once the batch output is durable (same WAL→commit protocol as source
//! offsets — Spark `StateStore` / `MicroBatchExecution` model). On restart the operator
//! restores from `committed/`. State batches are serialized with Arrow IPC.

use std::path::{Path, PathBuf};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::arrow::record_batch::RecordBatch;

fn staged_dir(checkpoint_location: &str, operator_id: &str) -> PathBuf {
    Path::new(checkpoint_location)
        .join("state")
        .join(operator_id)
        .join("staged")
}

fn committed_dir(checkpoint_location: &str, operator_id: &str) -> PathBuf {
    Path::new(checkpoint_location)
        .join("state")
        .join(operator_id)
        .join("committed")
}

/// Stage (write-ahead) an operator's state batches + an `i64` metadata vector (e.g.
/// watermark, emitted window ends) under the checkpoint location. Best-effort: a failure
/// to persist must not crash the query (recovery simply falls back to no state).
pub fn stage_state(
    checkpoint_location: &str,
    operator_id: &str,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    meta: &[i64],
) {
    let dir = staged_dir(checkpoint_location, operator_id);
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    // State batches → Arrow IPC file.
    if let Ok(file) = std::fs::File::create(dir.join("batches.arrow")) {
        if let Ok(mut w) = IpcFileWriter::try_new(file, schema.as_ref()) {
            for b in batches {
                let _ = w.write(b);
            }
            let _ = w.finish();
        }
    }
    // Metadata → newline-separated i64s.
    let meta_str = meta
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let _ = std::fs::write(dir.join("meta"), meta_str);
}

/// Restore an operator's committed state batches + metadata, if present.
pub fn restore_state(checkpoint_location: &str, operator_id: &str) -> (Vec<RecordBatch>, Vec<i64>) {
    let dir = committed_dir(checkpoint_location, operator_id);
    let mut batches = vec![];
    if let Ok(file) = std::fs::File::open(dir.join("batches.arrow")) {
        if let Ok(reader) = IpcFileReader::try_new(file, None) {
            for b in reader.flatten() {
                batches.push(b);
            }
        }
    }
    let meta = std::fs::read_to_string(dir.join("meta"))
        .ok()
        .map(|s| s.lines().filter_map(|l| l.trim().parse::<i64>().ok()).collect())
        .unwrap_or_default();
    (batches, meta)
}

/// Promote every operator's staged state to committed (atomic dir rename), once the
/// batch output is durable. Mirrors the source-offset commit step.
pub fn commit_state(checkpoint_location: &str) -> std::io::Result<()> {
    let state_root = Path::new(checkpoint_location).join("state");
    let Ok(entries) = std::fs::read_dir(&state_root) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let op_dir = entry.path();
        let staged = op_dir.join("staged");
        if staged.exists() {
            let committed = op_dir.join("committed");
            let _ = std::fs::remove_dir_all(&committed);
            std::fs::rename(&staged, &committed)?;
        }
    }
    Ok(())
}
