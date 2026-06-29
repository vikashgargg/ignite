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

// ---------------------------------------------------------------------------
// Incremental checkpointing (Flink ForSt-class, unified with F5 spill).
//
// A checkpoint is a MANIFEST that REFERENCES the operator's immutable spill chunks (the SST-analog;
// REFERENCES §3b) + a small in-RAM residual, rather than re-copying the whole state. The bulk
// (spilled chunks) was written off the barrier critical path during F5 spill, so a checkpoint writes
// only O(residual + manifest) — not O(total state). Chunks are immutable + refcounted: GC'd only
// when no retained epoch's manifest references them (= Flink SharedStateRegistry).
// See docs/design/streaming-incremental-checkpoint.md.
// ---------------------------------------------------------------------------

fn manifest_key(operator_id: &str, epoch: u64) -> String {
    format!("state/{operator_id}/epoch-{epoch}/manifest")
}
fn residual_key(operator_id: &str, epoch: u64) -> String {
    format!("state/{operator_id}/epoch-{epoch}/residual")
}

/// Default key-group count (= max parallelism), like Flink's `maxParallelism`. Keys are pre-partitioned
/// into G key-groups so rescale re-assigns key-group RANGES to instances (REFERENCES §2). Fixed at job
/// start.
pub const DEFAULT_KEY_GROUPS: u16 = 128;

/// Map a key's hash to its key-group `[0, g)`. Stable across parallelism changes → the unit of
/// keyed-state redistribution on rescale.
pub fn key_group(key_hash: u64, g: u16) -> u16 {
    if g == 0 {
        0
    } else {
        (key_hash % g as u64) as u16
    }
}

/// Instance `i` of `m` owns the contiguous key-group range `[lo, hi)` (Flink-style even split).
pub fn instance_key_group_range(i: usize, m: usize, g: u16) -> (u16, u16) {
    if m == 0 {
        return (0, g);
    }
    let g = g as usize;
    let lo = i * g / m;
    let hi = (i + 1) * g / m;
    (lo as u16, hi as u16)
}

/// Select the chunk ids whose key-group coverage intersects the owned range `[lo, hi)` — the chunks a
/// rescaled instance must read. `kg_ranges[k]` is chunk `chunks[k]`'s `[kg_lo, kg_hi)` coverage; if it
/// is empty/short (legacy manifest with no KG info) the chunk is assumed to cover ALL key-groups and is
/// always selected (the always-correct filter path).
pub fn chunks_for_range(chunks: &[u64], kg_ranges: &[(u16, u16)], lo: u16, hi: u16) -> Vec<u64> {
    chunks
        .iter()
        .enumerate()
        .filter(|(k, _)| match kg_ranges.get(*k) {
            Some(&(clo, chi)) => clo < hi && lo < chi, // ranges overlap
            None => true,                              // no KG info ⇒ may hold any key
        })
        .map(|(_, &c)| c)
        .collect()
}

/// Manifest blob: `[u32 meta_len][meta_len × i64][u32 n_chunks][n_chunks × u64][u32 n_kg][n_kg ×
/// (u16,u16)]` (LE) — meta + referenced spill-chunk ids + each chunk's key-group `[lo,hi)` coverage
/// (for rescale). The KG section is OPTIONAL/trailing: legacy manifests stop after chunks and decode
/// with empty KG ranges (⇒ treated as "covers all key-groups"). Mirrors `encode_state`'s framing.
fn encode_manifest(meta: &[i64], chunks: &[u64]) -> Vec<u8> {
    encode_manifest_kg(meta, chunks, &[])
}

fn encode_manifest_kg(meta: &[i64], chunks: &[u64], kg_ranges: &[(u16, u16)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + meta.len() * 8 + chunks.len() * 8 + kg_ranges.len() * 4);
    out.extend_from_slice(&(meta.len() as u32).to_le_bytes());
    for v in meta {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for c in chunks {
        out.extend_from_slice(&c.to_le_bytes());
    }
    out.extend_from_slice(&(kg_ranges.len() as u32).to_le_bytes());
    for (lo, hi) in kg_ranges {
        out.extend_from_slice(&lo.to_le_bytes());
        out.extend_from_slice(&hi.to_le_bytes());
    }
    out
}

fn decode_manifest(bytes: &[u8]) -> (Vec<i64>, Vec<u64>, Vec<(u16, u16)>) {
    let mut off = 0usize;
    let read_u32 = |bytes: &[u8], off: usize| -> Option<u32> {
        bytes.get(off..off + 4).map(|s| u32::from_le_bytes(s.try_into().unwrap_or([0; 4])))
    };
    let Some(meta_len) = read_u32(bytes, off) else {
        return (vec![], vec![], vec![]);
    };
    off += 4;
    let mut meta = Vec::with_capacity(meta_len as usize);
    for _ in 0..meta_len {
        let Some(s) = bytes.get(off..off + 8) else {
            return (meta, vec![], vec![]);
        };
        meta.push(i64::from_le_bytes(s.try_into().unwrap_or([0; 8])));
        off += 8;
    }
    let Some(n_chunks) = read_u32(bytes, off) else {
        return (meta, vec![], vec![]);
    };
    off += 4;
    let mut chunks = Vec::with_capacity(n_chunks as usize);
    for _ in 0..n_chunks {
        let Some(s) = bytes.get(off..off + 8) else {
            return (meta, chunks, vec![]);
        };
        chunks.push(u64::from_le_bytes(s.try_into().unwrap_or([0; 8])));
        off += 8;
    }
    // Optional trailing key-group section (legacy manifests end here ⇒ empty).
    let mut kg_ranges = vec![];
    if let Some(n_kg) = read_u32(bytes, off) {
        off += 4;
        let read_u16 = |off: usize| -> Option<u16> {
            bytes.get(off..off + 2).map(|s| u16::from_le_bytes(s.try_into().unwrap_or([0; 2])))
        };
        for _ in 0..n_kg {
            let (Some(lo), Some(hi)) = (read_u16(off), read_u16(off + 2)) else {
                break;
            };
            kg_ranges.push((lo, hi));
            off += 4;
        }
    }
    (meta, chunks, kg_ranges)
}

/// Incrementally stage an epoch: write only the small in-RAM `residual` + a manifest that REFERENCES
/// the already-persisted spill `chunks`. Does NOT re-copy the chunk blobs (they are immutable and
/// were written during spill). Best-effort. The residual carries the same `schema` as the chunks.
pub async fn stage_epoch_incremental(
    ck: &CheckpointStore,
    operator_id: &str,
    epoch: u64,
    schema: &SchemaRef,
    residual: &[RecordBatch],
    chunks: &[u64],
    meta: &[i64],
) {
    if let Some(blob) = encode_state(schema, residual, &[]) {
        let _ = ck
            .put(&residual_key(operator_id, epoch), Bytes::from(blob))
            .await;
    }
    let _ = ck
        .put(
            &manifest_key(operator_id, epoch),
            Bytes::from(encode_manifest(meta, chunks)),
        )
        .await;
}

/// Restore the full state of an epoch staged incrementally: read the manifest, then the residual +
/// every referenced chunk. Returns `(residual ++ chunks, meta)`. Empty if the manifest is absent.
pub async fn restore_epoch_incremental(
    ck: &CheckpointStore,
    operator_id: &str,
    epoch: u64,
) -> (Vec<RecordBatch>, Vec<i64>) {
    let Ok(Some(mbytes)) = ck.get(&manifest_key(operator_id, epoch)).await else {
        return (vec![], vec![]);
    };
    let (meta, chunks, _kg_ranges) = decode_manifest(&mbytes);
    let mut batches = match ck.get(&residual_key(operator_id, epoch)).await {
        Ok(Some(rbytes)) => decode_state(&rbytes).0,
        _ => vec![],
    };
    for id in chunks {
        batches.extend(read_spill(ck, operator_id, id).await);
    }
    (batches, meta)
}

/// Drop a subsumed epoch's manifest + residual (NOT its chunks — those may still be referenced by a
/// retained epoch; clean chunks via [`gc_unreferenced_chunks`]).
pub async fn gc_epoch_incremental(ck: &CheckpointStore, operator_id: &str, epoch: u64) {
    let _ = ck.delete(&manifest_key(operator_id, epoch)).await;
    let _ = ck.delete(&residual_key(operator_id, epoch)).await;
}

/// SharedStateRegistry-style chunk GC: delete every `candidate` chunk that is referenced by NONE of
/// the `retained_epochs`' manifests. (Caller supplies the candidate set = chunks it may delete, e.g.
/// the operator's known chunk ids.) A chunk shared by a retained epoch survives.
pub async fn gc_unreferenced_chunks(
    ck: &CheckpointStore,
    operator_id: &str,
    retained_epochs: &[u64],
    candidates: &[u64],
) {
    let mut referenced = std::collections::HashSet::new();
    for &e in retained_epochs {
        if let Ok(Some(mbytes)) = ck.get(&manifest_key(operator_id, e)).await {
            let (_, chunks, _) = decode_manifest(&mbytes);
            referenced.extend(chunks);
        }
    }
    for &c in candidates {
        if !referenced.contains(&c) {
            delete_spill(ck, operator_id, c).await;
        }
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

    // Incremental checkpoint: two epochs SHARE chunks (only the delta + manifest are written per
    // epoch); restore reassembles the full state; refcount GC keeps shared chunks alive while a
    // retained epoch references them and drops the rest (Flink SharedStateRegistry — REFERENCES §3b).
    #[tokio::test]
    async fn incremental_checkpoint_shares_chunks_and_refcounts() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let chunk = |a: i64| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![a]))]).unwrap()
        };
        let ck = CheckpointStore::from_store(Arc::new(InMemory::new()), StorePath::from("ck"));
        let op = "window-0";

        // Three immutable chunks already on the store (written during spill).
        for (i, v) in [10i64, 20, 30].into_iter().enumerate() {
            assert!(write_spill(&ck, op, i as u64, &schema, &[chunk(v)]).await);
        }

        // Epoch 1: references chunks [0,1] + a small residual (value 100), meta [5].
        stage_epoch_incremental(&ck, op, 1, &schema, &[chunk(100)], &[0, 1], &[5]).await;
        // Epoch 2: SHARES chunks [0,1], adds new chunk 2, residual (value 200), meta [6].
        stage_epoch_incremental(&ck, op, 2, &schema, &[chunk(200)], &[0, 1, 2], &[6]).await;

        // Restore each epoch = residual ++ referenced chunks (full state, incrementally assembled).
        let sum = |bs: &[RecordBatch]| -> i64 {
            bs.iter()
                .flat_map(|b| {
                    let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().clone();
                    (0..b.num_rows()).map(move |i| a.value(i)).collect::<Vec<_>>()
                })
                .sum()
        };
        let (b1, m1) = restore_epoch_incremental(&ck, op, 1).await;
        assert_eq!(m1, vec![5]);
        assert_eq!(sum(&b1), 100 + 10 + 20, "epoch1 = residual + chunks 0,1");
        let (b2, m2) = restore_epoch_incremental(&ck, op, 2).await;
        assert_eq!(m2, vec![6]);
        assert_eq!(sum(&b2), 200 + 10 + 20 + 30, "epoch2 = residual + chunks 0,1,2");

        // Subsume epoch 1 (retain only {2}); GC its manifest+residual, then unreferenced chunks.
        gc_epoch_incremental(&ck, op, 1).await;
        gc_unreferenced_chunks(&ck, op, &[2], &[0, 1, 2]).await;
        // chunks 0,1,2 are all referenced by epoch 2 -> all survive; epoch 2 still restores fully.
        let (b2b, _) = restore_epoch_incremental(&ck, op, 2).await;
        assert_eq!(sum(&b2b), 200 + 10 + 20 + 30, "epoch2 intact after epoch1 subsumed");
        // epoch 1's manifest is gone.
        assert!(restore_epoch_incremental(&ck, op, 1).await.0.is_empty());

        // Now subsume epoch 2 as well (retain none); all chunks become unreferenced -> deleted.
        gc_epoch_incremental(&ck, op, 2).await;
        gc_unreferenced_chunks(&ck, op, &[], &[0, 1, 2]).await;
        for i in 0..3u64 {
            assert!(read_spill(&ck, op, i).await.is_empty(), "chunk {i} GC'd when unreferenced");
        }
    }

    // PROVES the Flink-beating property: an incremental checkpoint writes O(residual + manifest) per
    // epoch — BOUNDED even as total state GROWS — whereas a full snapshot re-copies O(total state)
    // every epoch. Exercises the real `stage_epoch_incremental` path and measures actual store bytes.
    // REFERENCES §3b (ForSt/SharedStateRegistry); docs/design/streaming-incremental-checkpoint.md.
    #[tokio::test]
    async fn incremental_checkpoint_write_is_o_delta_not_o_state() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let big = |n: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from((0..n).collect::<Vec<_>>()))],
            )
            .unwrap()
        };
        let ck = CheckpointStore::from_store(Arc::new(InMemory::new()), StorePath::from("ck"));
        let op = "window-0";
        let residual = big(1); // small in-RAM tail (constant)
        let chunk_rows = 2000i64; // each spilled chunk is large
        let n_epochs = 8u64;

        // Spill chunks ahead (immutable; written off the barrier path during F5 spill).
        for i in 0..n_epochs {
            assert!(write_spill(&ck, op, i, &schema, &[big(chunk_rows)]).await);
        }

        let mut prev_inc = 0usize;
        let mut last_inc = 0usize;
        let mut last_full = 0usize;
        for e in 1..=n_epochs {
            // Epoch e's state GROWS: it references e chunks + the small residual.
            let chunks: Vec<u64> = (0..e).collect();
            stage_epoch_incremental(&ck, op, e, &schema, &[residual.clone()], &chunks, &[e as i64])
                .await;
            // Bytes WRITTEN for this checkpoint = residual blob + manifest (NOT the chunk blobs).
            let r = ck.get(&residual_key(op, e)).await.ok().flatten().map_or(0, |b| b.len());
            let m = ck.get(&manifest_key(op, e)).await.ok().flatten().map_or(0, |b| b.len());
            let inc = r + m;
            // Full-snapshot baseline: residual ++ ALL e chunk batches re-copied every epoch.
            let mut all = vec![residual.clone()];
            for _ in 0..e {
                all.push(big(chunk_rows));
            }
            let full = encode_state(&schema, &all, &[]).map_or(0, |b| b.len());

            // Per-epoch incremental growth is only the manifest's extra chunk ref (~8B), NOT a chunk.
            if e > 1 {
                assert!(
                    inc.saturating_sub(prev_inc) <= 32,
                    "epoch {e}: incremental write grew by {} bytes — must be ~8B/chunk-ref, not O(state)",
                    inc.saturating_sub(prev_inc)
                );
            }
            prev_inc = inc;
            last_inc = inc;
            last_full = full;
        }
        // At scale, the incremental checkpoint is an order of magnitude smaller than a full snapshot.
        assert!(
            last_inc * 10 < last_full,
            "incremental {last_inc}B must be << full {last_full}B at {n_epochs} epochs"
        );
    }

    // Rescale step 1: key-groups + manifest KG-range round-trip + chunk selection for a rescaled
    // instance's owned range, with legacy (no-KG) back-compat. REFERENCES §2 (Flink key-groups).
    #[test]
    fn rescale_key_groups_manifest_and_selection() {
        // key_group is stable in [0,g); instance ranges tile [0,g) without gaps/overlap.
        assert_eq!(key_group(0, 0), 0); // g=0 guard
        for h in [0u64, 1, 127, 128, 300, u64::MAX] {
            assert!(key_group(h, 128) < 128);
        }
        // 8 key-groups across 3 instances tile [0,8) exactly once.
        let m = 3;
        let mut covered = vec![0u8; 8];
        for i in 0..m {
            let (lo, hi) = instance_key_group_range(i, m, 8);
            for kg in lo..hi {
                covered[kg as usize] += 1;
            }
        }
        assert!(covered.iter().all(|&c| c == 1), "key-groups must tile exactly once: {covered:?}");

        // Manifest with per-chunk KG coverage round-trips through the OPTIONAL trailing section.
        let meta = vec![7i64, -1];
        let chunks = vec![10u64, 20, 30];
        let kg = vec![(0u16, 3u16), (3, 6), (6, 8)]; // chunk 10→kg[0,3), 20→[3,6), 30→[6,8)
        let blob = encode_manifest_kg(&meta, &chunks, &kg);
        let (dm, dc, dkg) = decode_manifest(&blob);
        assert_eq!((dm, dc, dkg), (meta.clone(), chunks.clone(), kg.clone()));

        // Rescaled instance owning [2,5) must read chunks 10 (covers [0,3)) and 20 (covers [3,6)).
        assert_eq!(chunks_for_range(&chunks, &kg, 2, 5), vec![10, 20]);
        // Instance owning [6,8) reads only chunk 30.
        assert_eq!(chunks_for_range(&chunks, &kg, 6, 8), vec![30]);

        // Legacy back-compat: encode_manifest (no KG) decodes with empty KG ranges, and selection then
        // returns ALL chunks (a chunk with unknown coverage may hold any key — always-correct path).
        let legacy = encode_manifest(&meta, &chunks);
        let (_, lc, lkg) = decode_manifest(&legacy);
        assert!(lkg.is_empty());
        assert_eq!(chunks_for_range(&lc, &lkg, 2, 5), vec![10, 20, 30]);
    }
}
