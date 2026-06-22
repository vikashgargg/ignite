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

/// Per-epoch state key for CONTINUOUS (realtime) exactly-once (F3-c). On each `Checkpoint{epoch}`
/// barrier a stateful operator writes its state here (write-ahead, before the barrier reaches the
/// realtime sink); the sink's atomic `realtime/committed={epoch,offsets}` then makes that epoch
/// authoritative. On restart the operator restores exactly the COMMITTED epoch's state — the same
/// epoch the source seeks offsets for — so state + offsets are a consistent global snapshot
/// (Chandy-Lamport). Keyed per (operator, partition) by `operator_id`.
fn epoch_key(operator_id: &str, epoch: u64) -> String {
    format!("state/{operator_id}/epoch-{epoch}")
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
pub async fn restore_state(
    ck: &CheckpointStore,
    operator_id: &str,
) -> (Vec<RecordBatch>, Vec<i64>) {
    match ck.get(&committed_key(operator_id)).await {
        Ok(Some(bytes)) => decode_state(&bytes),
        _ => (vec![], vec![]),
    }
}

/// Write an operator's state for a specific epoch (continuous EO write-ahead — see [`epoch_key`]).
/// Best-effort; a failure must not crash the query.
pub async fn stage_epoch_state(
    ck: &CheckpointStore,
    operator_id: &str,
    epoch: u64,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    meta: &[i64],
) {
    if let Some(blob) = encode_state(schema, batches, meta) {
        let _ = ck.put(&epoch_key(operator_id, epoch), Bytes::from(blob)).await;
    }
}

/// Restore an operator's state staged at `epoch` (the committed epoch on restart), if present.
pub async fn restore_epoch_state(
    ck: &CheckpointStore,
    operator_id: &str,
    epoch: u64,
) -> (Vec<RecordBatch>, Vec<i64>) {
    match ck.get(&epoch_key(operator_id, epoch)).await {
        Ok(Some(bytes)) => decode_state(&bytes),
        _ => (vec![], vec![]),
    }
}

/// Best-effort delete of an old epoch's state object (GC — keep a small trailing window so a
/// restart can always find the committed epoch; never delete at or after the committed epoch).
pub async fn gc_epoch_state(ck: &CheckpointStore, operator_id: &str, epoch: u64) {
    let _ = ck.delete(&epoch_key(operator_id, epoch)).await;
}

/// F5 spill key: a numbered partial-state blob spilled out of RAM when the operator's in-memory
/// state exceeds its budget. Arrow-IPC ↔ `CheckpointStore` (object-store = Flink-2.0-ForSt shape;
/// REFERENCES §3/§5) — Vajra spills in Arrow with no JVM, beating RocksDB's local-disk-only model.
/// See docs/design/streaming-spillable-state-f5.md.
fn spill_key(operator_id: &str, index: u64) -> String {
    format!("state/{operator_id}/spill-{index}")
}

/// Spill a chunk of partial-state batches to blob `index` (write-ahead, evicts them from RAM).
/// Best-effort; on failure the caller keeps the batches in memory (correct, just not bounded).
pub async fn write_spill(
    ck: &CheckpointStore,
    operator_id: &str,
    index: u64,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> bool {
    match encode_state(schema, batches, &[]) {
        Some(blob) => ck
            .put(&spill_key(operator_id, index), Bytes::from(blob))
            .await
            .is_ok(),
        None => false,
    }
}

/// Read back a spilled chunk (for the finalize merge). Empty if absent/malformed.
pub async fn read_spill(
    ck: &CheckpointStore,
    operator_id: &str,
    index: u64,
) -> Vec<RecordBatch> {
    match ck.get(&spill_key(operator_id, index)).await {
        Ok(Some(bytes)) => decode_state(&bytes).0,
        _ => vec![],
    }
}

/// Delete a spilled chunk once consumed (GC).
pub async fn delete_spill(ck: &CheckpointStore, operator_id: &str, index: u64) {
    let _ = ck.delete(&spill_key(operator_id, index)).await;
}

/// Read the committed epoch from the realtime/committed record (the single atomic object the
/// realtime sink writes per epoch: JSON `{"epoch":N,"offsets":{...}}`). `None` if absent (a
/// micro-batch run, or a fresh start). Used by stateful operators to pick which epoch's state to
/// restore — the same epoch the Kafka source seeks offsets for.
pub async fn committed_epoch(ck: &CheckpointStore) -> Option<u64> {
    let bytes = ck.get("realtime/committed").await.ok().flatten()?;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    v.get("epoch")?.as_u64()
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
        stage_state(&ck, "window-0", &schema, std::slice::from_ref(&batch), &[42, 100, 200]).await;
        ck.promote("state/window-0/staged", "state/window-0/committed")
            .await
            .unwrap();
        let (b, m) = restore_state(&ck, "window-0").await;
        assert_eq!(m, vec![42, 100, 200]);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].num_rows(), 3);
    }

    // F3-c: continuous EO restores EXACTLY the committed epoch's state (the same epoch the source
    // seeks offsets for), not a later-staged-but-uncommitted epoch — else recovery would over-count.
    #[tokio::test]
    async fn epoch_state_restores_committed_epoch() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let row = |x: i64| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![x]))]).unwrap()
        };
        let ck = CheckpointStore::from_store(Arc::new(InMemory::new()), StorePath::from("ck"));

        // Operator wrote state for epochs 1, 2, 3 (3 = staged but NOT yet committed).
        stage_epoch_state(&ck, "window-0", 1, &schema, &[row(11)], &[1]).await;
        stage_epoch_state(&ck, "window-0", 2, &schema, &[row(22)], &[2]).await;
        stage_epoch_state(&ck, "window-0", 3, &schema, &[row(33)], &[3]).await;

        // Sink committed epoch 2 (its atomic realtime/committed record).
        ck.put(
            "realtime/committed",
            Bytes::from(br#"{"epoch":2,"offsets":{"t:0":500}}"#.to_vec()),
        )
        .await
        .unwrap();

        // Recovery picks epoch 2 — not 3 (uncommitted, would over-count) nor 1 (stale).
        assert_eq!(committed_epoch(&ck).await, Some(2));
        let (b, m) = restore_epoch_state(&ck, "window-0", 2).await;
        assert_eq!(m, vec![2]);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 22);

        // GC drops an old epoch; committed epoch's state remains restorable.
        gc_epoch_state(&ck, "window-0", 1).await;
        assert!(restore_epoch_state(&ck, "window-0", 1).await.0.is_empty());
        assert_eq!(restore_epoch_state(&ck, "window-0", 2).await.1, vec![2]);
    }

    // F5: spill primitive — write numbered partial-state chunks out of RAM, read them back for the
    // finalize merge, GC when consumed (Arrow-IPC ↔ object-store; the building block for bounded state).
    #[tokio::test]
    async fn spill_chunks_roundtrip_and_gc() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let chunk = |a: i64, b: i64| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![a, b]))]).unwrap()
        };
        let ck = CheckpointStore::from_store(Arc::new(InMemory::new()), StorePath::from("ck"));

        assert!(write_spill(&ck, "window-0", 0, &schema, &[chunk(1, 2)]).await);
        assert!(write_spill(&ck, "window-0", 1, &schema, &[chunk(3, 4)]).await);
        // read each chunk back independently (lazy finalize streams them one at a time)
        let c0 = read_spill(&ck, "window-0", 0).await;
        let c1 = read_spill(&ck, "window-0", 1).await;
        assert_eq!(c0.len(), 1);
        assert_eq!(c0[0].num_rows(), 2);
        assert_eq!(c1[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 3);
        // GC a consumed chunk
        delete_spill(&ck, "window-0", 0).await;
        assert!(read_spill(&ck, "window-0", 0).await.is_empty());
        assert_eq!(read_spill(&ck, "window-0", 1).await.len(), 1);
    }
}
