//! Operator-state snapshot/restore for stateful exactly-once recovery.
//!
//! Stateful streaming operators (windowed aggregation, joins) keep their state as
//! `Vec<RecordBatch>` plus a small `i64` metadata vector (watermark, emitted window ends, …). On a
//! checkpoint they **stage** that state (write-ahead) to `state/<operator-id>/staged`; the runner
//! promotes staged → committed once the batch output is durable (same WAL→commit protocol as source
//! offsets — Spark `StateStore` / `MicroBatchExecution`). On restart the operator restores from
//! `state/<operator-id>/committed`.
//!
//! State is a **single object** in the [`CheckpointStore`] (local FS or object store): one blob of
//! `[u32 meta_len][meta_len × i64][Arrow-IPC batches]`. Single-object is required because object
//! stores have no atomic rename — the commit is one atomic `put` of `committed`. This is why the
//! older multi-file (`batches.arrow` + `meta`) layout was collapsed into one blob.

use std::io::Cursor;

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use sail_common_datafusion::streaming::checkpoint::CheckpointStore;

fn staged_key(operator_id: &str) -> String {
    format!("state/{operator_id}/staged")
}

fn committed_key(operator_id: &str) -> String {
    format!("state/{operator_id}/committed")
}

/// Serialize state into a single blob: `[u32 LE meta_len][meta_len × i64 LE][Arrow-IPC]`.
fn encode_state(schema: &SchemaRef, batches: &[RecordBatch], meta: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&(meta.len() as u32).to_le_bytes());
    for v in meta {
        out.extend_from_slice(&v.to_le_bytes());
    }
    let mut ipc = Vec::new();
    {
        let mut w = IpcFileWriter::try_new(&mut ipc, schema.as_ref()).ok()?;
        for b in batches {
            w.write(b).ok()?;
        }
        w.finish().ok()?;
    }
    out.extend_from_slice(&ipc);
    Some(out)
}

/// Inverse of [`encode_state`]; returns empty on any malformed input (recovery falls back to no
/// state rather than crashing).
fn decode_state(bytes: &[u8]) -> (Vec<RecordBatch>, Vec<i64>) {
    if bytes.len() < 4 {
        return (vec![], vec![]);
    }
    let meta_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut off = 4;
    let mut meta = Vec::with_capacity(meta_len);
    for _ in 0..meta_len {
        let Some(slice) = bytes.get(off..off + 8) else {
            return (vec![], meta);
        };
        meta.push(i64::from_le_bytes(slice.try_into().unwrap_or([0; 8])));
        off += 8;
    }
    let mut batches = vec![];
    if let Some(ipc) = bytes.get(off..) {
        if let Ok(reader) = IpcFileReader::try_new(Cursor::new(ipc.to_vec()), None) {
            for b in reader.flatten() {
                batches.push(b);
            }
        }
    }
    (batches, meta)
}

/// Stage (write-ahead) an operator's state as a single object. Best-effort: a persistence failure
/// must not crash the query (recovery falls back to no state).
pub async fn stage_state(
    ck: &CheckpointStore,
    operator_id: &str,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    meta: &[i64],
) {
    if let Some(blob) = encode_state(schema, batches, meta) {
        let _ = ck.put(&staged_key(operator_id), Bytes::from(blob)).await;
    }
}

/// Restore an operator's committed state, if present.
pub async fn restore_state(ck: &CheckpointStore, operator_id: &str) -> (Vec<RecordBatch>, Vec<i64>) {
    match ck.get(&committed_key(operator_id)).await {
        Ok(Some(bytes)) => decode_state(&bytes),
        _ => (vec![], vec![]),
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;

    use super::*;

    #[tokio::test]
    async fn state_roundtrip_single_blob() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let ck = CheckpointStore::from_store(Arc::new(InMemory::new()), StorePath::from("ck"));

        // Nothing committed yet.
        let (b, m) = restore_state(&ck, "window-0").await;
        assert!(b.is_empty() && m.is_empty());

        // Stage + promote (commit) + restore.
        stage_state(&ck, "window-0", &schema, &[batch.clone()], &[42, 100, 200]).await;
        ck.promote("state/window-0/staged", "state/window-0/committed")
            .await
            .unwrap();
        let (b, m) = restore_state(&ck, "window-0").await;
        assert_eq!(m, vec![42, 100, 200]);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].num_rows(), 3);
    }
}
