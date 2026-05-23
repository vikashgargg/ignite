#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tonic::async_trait;
use tonic::transport::server::TcpIncoming;

use crate::error::ExecutionResult;
use crate::id::{JobId, TaskStreamKey};
use crate::rpc::ClientOptions;
use crate::stream::error::TaskStreamResult;
use crate::stream::reader::TaskStreamSource;
use crate::stream_service::{TaskStreamFetcher, TaskStreamFlightClient, TaskStreamFlightServer};

/// A test fetcher that serves a single pre-built source stream on any key.
struct SingleSourceFetcher {
    source: Arc<Mutex<Option<TaskStreamSource>>>,
}

#[async_trait]
impl TaskStreamFetcher for SingleSourceFetcher {
    async fn fetch(
        &self,
        _key: TaskStreamKey,
        sender: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ExecutionResult<()> {
        let source = self
            .source
            .lock()
            .await
            .take()
            .expect("source already consumed");
        let _ = sender.send(Ok(source));
        Ok(())
    }
}

/// Proves that data written into a channel-backed stream is correctly transported
/// across the Arrow Flight wire protocol and arrives with the right values.
///
/// This exercises the full `do_get` path:
///   writer (mpsc::Sender) → TaskStreamFlightServer → gRPC → TaskStreamFlightClient → reader
#[tokio::test]
async fn test_arrow_flight_shuffle_roundtrip() {
    // Build 100 rows: a single Int32 column with values 0..100.
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
    let values: Int32Array = (0i32..100).collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();

    // Wire up a channel-backed source stream.
    let (tx, rx) = tokio::sync::mpsc::channel::<TaskStreamResult<RecordBatch>>(128);
    let source: TaskStreamSource =
        Box::pin(tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(rx));

    // Send the batch from a background task; dropping `tx` closes the stream.
    let batch_for_write = batch.clone();
    tokio::spawn(async move {
        tx.send(Ok(batch_for_write)).await.unwrap();
    });

    // Start an Arrow Flight server on a random loopback port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let fetcher = SingleSourceFetcher {
        source: Arc::new(Mutex::new(Some(source))),
    };
    let flight_service = FlightServiceServer::new(TaskStreamFlightServer::new(Box::new(fetcher)));
    let incoming = TcpIncoming::from(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(flight_service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    // Give the server a moment to bind and start accepting connections.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Fetch via the Arrow Flight client (the same path a remote worker would use).
    let key = TaskStreamKey {
        job_id: JobId::from(1u64),
        stage: 0,
        partition: 0,
        attempt: 0,
        channel: 0,
    };
    let client = TaskStreamFlightClient::new(ClientOptions {
        enable_tls: false,
        host: "127.0.0.1".to_string(),
        port,
    });
    let stream = client.fetch_task_stream(key, schema.clone()).await.unwrap();

    // Collect and verify.
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 100, "expected 100 rows over Arrow Flight, got {total_rows}");

    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for i in 0i32..100 {
        assert_eq!(col.value(i as usize), i, "row {i} mismatch");
    }

    let _ = shutdown_tx.send(());
}
