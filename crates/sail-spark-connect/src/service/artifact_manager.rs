use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::artifact::ArtifactStore;
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::add_artifacts_request::Payload;
use crate::spark::connect::add_artifacts_response::ArtifactSummary;
use crate::spark::connect::artifact_statuses_response::ArtifactStatus;

pub(crate) async fn handle_add_artifacts(
    ctx: &SessionContext,
    stream: impl Stream<Item = Result<Payload, Status>>,
) -> SparkResult<Vec<ArtifactSummary>> {
    let store = ctx.extension::<ArtifactStore>()?;
    let mut summaries = Vec::new();
    let mut in_flight: Option<(String, Vec<u8>)> = None;

    futures::pin_mut!(stream);
    while let Some(msg) = stream.next().await {
        let payload = msg.map_err(|e| SparkError::internal(e.to_string()))?;
        match payload {
            Payload::Batch(batch) => {
                for artifact in batch.artifacts {
                    let name = artifact.name.clone();
                    let data = artifact.data.map(|c| c.data).unwrap_or_default();
                    store.store(name.clone(), data);
                    summaries.push(ArtifactSummary { name, is_crc_successful: true });
                }
            }
            Payload::BeginChunk(begin) => {
                if let Some((prev_name, prev_data)) = in_flight.take() {
                    store.store(prev_name.clone(), prev_data);
                    summaries.push(ArtifactSummary { name: prev_name, is_crc_successful: true });
                }
                let first_bytes = begin.initial_chunk.map(|c| c.data).unwrap_or_default();
                in_flight = Some((begin.name, first_bytes));
            }
            Payload::Chunk(chunk) => {
                if let Some((_, ref mut buf)) = in_flight {
                    buf.extend_from_slice(&chunk.data);
                }
            }
        }
    }

    if let Some((name, data)) = in_flight {
        store.store(name.clone(), data);
        summaries.push(ArtifactSummary { name, is_crc_successful: true });
    }

    Ok(summaries)
}

pub(crate) async fn handle_artifact_statuses(
    ctx: &SessionContext,
    names: Vec<String>,
) -> SparkResult<HashMap<String, ArtifactStatus>> {
    let store = ctx.extension::<ArtifactStore>()?;
    Ok(names
        .into_iter()
        .map(|name| {
            let exists = store.exists(&name);
            (name, ArtifactStatus { exists })
        })
        .collect())
}
