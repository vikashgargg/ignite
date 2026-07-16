use std::io::Cursor;
use std::pin::Pin;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, TryStreamExt};
use log::debug;
use prost::Message;
use tokio::sync::oneshot;
use tonic::{async_trait, Request, Response, Status, Streaming};

use crate::error::ExecutionResult;
use crate::id::TaskStreamKey;
use crate::stream::gen::TaskStreamTicket;
use crate::stream::reader::TaskStreamSource;

#[async_trait]
pub trait TaskStreamFetcher: Send + Sync {
    async fn fetch(
        &self,
        key: TaskStreamKey,
        sender: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ExecutionResult<()>;
}

pub struct TaskStreamFlightServer {
    fetcher: Box<dyn TaskStreamFetcher>,
}

impl TaskStreamFlightServer {
    pub fn new(fetcher: Box<dyn TaskStreamFetcher>) -> Self {
        Self { fetcher }
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[async_trait]
impl FlightService for TaskStreamFlightServer {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    type DoGetStream = BoxedFlightStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let Ticket { ticket } = request.into_inner();
        let ticket = {
            let mut buf = Cursor::new(&ticket);
            TaskStreamTicket::decode(&mut buf)
                .map_err(|e| Status::invalid_argument(e.to_string()))?
        };
        debug!("{ticket:?}");
        let TaskStreamTicket {
            job_id,
            stage,
            partition,
            attempt,
            channel,
        } = ticket;
        let (tx, rx) = oneshot::channel();
        let key = TaskStreamKey {
            job_id: job_id.into(),
            stage: stage as usize,
            partition: partition as usize,
            attempt: attempt as usize,
            channel: channel as usize,
        };
        self.fetcher.fetch(key, tx).await?;
        let stream = rx
            .await
            .map_err(|_| Status::internal("failed to receive task stream"))??;
        // Coalesce the routed sub-batches into large batches BEFORE the Flight IPC encode — the keyed
        // shuffle splits each batch into `n` tiny sub-batches, and per-batch IPC framing/serialize is the
        // measured distributed throughput gap (24k ~4k-row Flight messages at 100M/16-way). Marker-aware
        // PULL combinator: flush-before-marker keeps a barrier a consistent cut, and it flushes on stream
        // end so buffered rows are never abandoned (the bug a sender-side coalescer hit). Distributed-only
        // (this Flight path); the in-process exchange is untouched (VAJRA_SHUFFLE_BATCH_ROWS=0 disables).
        let target = sail_physical_plan::streaming::exchange::shuffle_batch_rows();
        let stream: TaskStreamSource = if target > 1 {
            Box::pin(sail_physical_plan::streaming::exchange::coalesce_flow_events(
                stream, target,
            ))
        } else {
            stream
        };
        let stream = stream.map_err(|e| FlightError::Tonic(Box::new(e.into())));
        let stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(Status::from);
        if sail_common_datafusion::streaming::event::encoding::wm_prof_enabled() {
            // WM_PROF: time the cross-pod SEND (produce + IPC-encode each FlightData) + count messages,
            // so the distributed-shuffle serialize cost is attributed per producer pod (SHUFFLE_SEND_NS).
            use futures::StreamExt;
            let timed = futures::stream::unfold(Box::pin(stream), |mut st| async move {
                let t = std::time::Instant::now();
                match st.next().await {
                    Some(item) => {
                        use sail_common_datafusion::streaming::event::encoding as prof;
                        prof::prof_add(&prof::SHUFFLE_SEND_NS, t.elapsed().as_nanos() as u64);
                        if item.is_ok() {
                            prof::SHUFFLE_SEND_BATCHES
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Some((item, st))
                    }
                    None => None,
                }
            });
            Ok(Response::new(Box::pin(timed) as Self::DoGetStream))
        } else {
            Ok(Response::new(Box::pin(stream) as Self::DoGetStream))
        }
    }

    type DoPutStream = BoxedFlightStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    type DoExchangeStream = BoxedFlightStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    type ListActionsStream = BoxedFlightStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}
