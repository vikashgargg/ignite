use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{PlanType, StringifiedPlan};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use log::warn;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::{oneshot, watch};

use crate::error::{SparkError, SparkResult, SparkThrowable};
use crate::spark::connect;
use crate::web_ui;

/// Keep the last N micro-batch progress reports (Spark default is 100).
const MAX_RECENT_PROGRESS: usize = 100;

pub struct StreamingQuery {
    name: String,
    info: Vec<StringifiedPlan>,
    error: watch::Receiver<Option<SparkThrowable>>,
    stopped: watch::Receiver<bool>,
    signal: Option<oneshot::Sender<()>>,
    awaitable: bool,
    /// Ring buffer of recent `StreamingQueryProgress` JSON reports (newest last), written by
    /// the run loop per micro-batch and read by `lastProgress`/`recentProgress`.
    progress: Arc<Mutex<VecDeque<String>>>,
}

/// An executed micro-batch: its physical plan (for reading leaf-scan row metrics) + output stream.
pub type PlannedStream = (Arc<dyn ExecutionPlan>, SendableRecordBatchStream);

/// Produces a fresh (bounded) micro-batch (plan + stream) by re-planning + executing the query —
/// used for continuous (`ProcessingTime`) triggers, where each trigger is a fresh availableNow-style
/// micro-batch that reuses the proven offset/state commit + recovery (Spark `MicroBatchExecution`
/// re-plan model). See docs/design/streaming-file-source.md.
pub type MakeStream = Box<
    dyn Fn() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = SparkResult<PlannedStream>> + Send>,
        > + Send
        + Sync,
>;

/// How `StreamingQuery::run` drives execution.
enum StreamDriver {
    /// `availableNow`/`once` (bounded): consume a single pre-built (plan, stream), then stop.
    Once(Arc<dyn ExecutionPlan>, SendableRecordBatchStream),
    /// `ProcessingTime` (continuous): re-plan + execute a bounded micro-batch each `interval`,
    /// committing after each, until stopped.
    Continuous {
        make_stream: MakeStream,
        interval: Duration,
    },
}

/// Sum the output rows of the plan's leaf nodes (sources/scans) — `numInputRows` for one
/// micro-batch. Leaf scans (e.g. the parquet scan under the streaming file source) record
/// `output_rows` via DataFusion metrics; sources without metrics contribute 0.
///
/// Leaves are deduplicated by `Arc` identity: the parallel streaming sink fans the *same*
/// source `Arc` into N writer children, so a naive walk would count it N times.
fn count_input_rows(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    fn walk(plan: &Arc<dyn ExecutionPlan>, seen: &mut std::collections::HashSet<*const ()>) -> u64 {
        let id = Arc::as_ptr(plan) as *const ();
        if !seen.insert(id) {
            return 0; // already counted this shared node
        }
        let children = plan.children();
        if children.is_empty() {
            return plan.metrics().and_then(|m| m.output_rows()).unwrap_or(0) as u64;
        }
        children.iter().map(|c| walk(c, seen)).sum()
    }
    walk(plan, &mut std::collections::HashSet::new())
}

/// Build a Spark `StreamingQueryProgress`-shaped JSON for one micro-batch (the fields the
/// PySpark `StreamingQueryProgress`/`SourceProgress`/`SinkProgress` parsers require).
fn progress_json(
    id: &str,
    run_id: &str,
    name: &str,
    batch_id: u64,
    num_input_rows: u64,
    duration_ms: u128,
    source_desc: &str,
    sink_desc: &str,
) -> String {
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let rate = if duration_ms > 0 {
        (num_input_rows as f64) * 1000.0 / (duration_ms as f64)
    } else {
        0.0
    };
    let name_json = if name.is_empty() {
        "null".to_string()
    } else {
        format!("{name:?}")
    };
    format!(
        "{{\"id\":{id:?},\"runId\":{run_id:?},\"name\":{name_json},\"timestamp\":{timestamp:?},\
         \"batchId\":{batch_id},\"batchDuration\":{duration_ms},\
         \"durationMs\":{{\"triggerExecution\":{duration_ms}}},\"eventTime\":{{}},\
         \"stateOperators\":[],\
         \"sources\":[{{\"description\":{source_desc:?},\"startOffset\":null,\"endOffset\":null,\
         \"latestOffset\":null,\"numInputRows\":{num_input_rows},\
         \"inputRowsPerSecond\":{rate:.1},\"processedRowsPerSecond\":{rate:.1}}}],\
         \"sink\":{{\"description\":{sink_desc:?},\"numOutputRows\":{num_input_rows}}},\
         \"numInputRows\":{num_input_rows},\"inputRowsPerSecond\":{rate:.1},\
         \"processedRowsPerSecond\":{rate:.1}}}"
    )
}

/// Consume one (bounded) micro-batch stream to completion. Writes the per-batch offset marker
/// and returns whether it ran without error (the durability signal for committing offsets).
async fn consume_stream(
    mut stream: SendableRecordBatchStream,
    offsets_dir: &Option<PathBuf>,
    batch_id: &mut u64,
    ui_id: &str,
    error: &watch::Sender<Option<SparkThrowable>>,
) -> bool {
    let mut clean = true;
    while let Some(x) = stream.next().await {
        match x {
            Ok(_) => {
                if let Some(dir) = offsets_dir {
                    let offset_file = dir.join(batch_id.to_string());
                    let payload = format!(
                        "v1\n{{\"batchId\":{batch_id},\"timestamp\":{}}}\n",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis())
                            .unwrap_or(0)
                    );
                    if let Err(e) = std::fs::write(&offset_file, payload) {
                        warn!("Failed to write checkpoint offset {batch_id}: {e}");
                    }
                }
                web_ui::increment_batch(ui_id).await;
                *batch_id += 1;
            }
            Err(e) => {
                clean = false;
                let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                let _ = error.send(Some(cause.into()));
            }
        }
    }
    clean
}

fn read_latest_batch_id(offsets_dir: &Path) -> Option<u64> {
    std::fs::read_dir(offsets_dir)
        .ok()?
        .filter_map(|e| {
            let name = e.ok()?.file_name();
            name.to_str()?.parse::<u64>().ok()
        })
        .max()
}

impl StreamingQuery {
    pub fn new(
        query_id: String,
        run_id: String,
        name: String,
        info: Vec<StringifiedPlan>,
        plan: Arc<dyn ExecutionPlan>,
        stream: SendableRecordBatchStream,
        checkpoint_location: Option<String>,
    ) -> Self {
        Self::spawn(
            query_id,
            run_id,
            name,
            info,
            StreamDriver::Once(plan, stream),
            checkpoint_location,
        )
    }

    /// Continuous (`ProcessingTime`) query: re-plan + execute a bounded micro-batch every
    /// `interval`, committing after each (reusing the availableNow exactly-once + state
    /// recovery), until stopped.
    pub fn new_continuous(
        query_id: String,
        run_id: String,
        name: String,
        info: Vec<StringifiedPlan>,
        make_stream: MakeStream,
        interval: Duration,
        checkpoint_location: Option<String>,
    ) -> Self {
        Self::spawn(
            query_id,
            run_id,
            name,
            info,
            StreamDriver::Continuous {
                make_stream,
                interval,
            },
            checkpoint_location,
        )
    }

    fn spawn(
        query_id: String,
        run_id: String,
        name: String,
        info: Vec<StringifiedPlan>,
        driver: StreamDriver,
        checkpoint_location: Option<String>,
    ) -> Self {
        let initial_batch_id = checkpoint_location
            .as_deref()
            .map(|loc| {
                let offsets_dir = PathBuf::from(loc).join("offsets");
                read_latest_batch_id(&offsets_dir)
                    .map(|id| id + 1)
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        let ui_id = uuid::Uuid::new_v4().to_string();
        {
            let id = ui_id.clone();
            let n = name.clone();
            tokio::spawn(async move {
                web_ui::register_query(id, n).await;
            });
        }

        let (signal_tx, signal_rx) = oneshot::channel();
        let (error_tx, error_rx) = watch::channel(None);
        let (stopped_tx, stopped_rx) = watch::channel(false);
        let ui_id_run = ui_id.clone();
        let progress = Arc::new(Mutex::new(VecDeque::new()));
        let progress_run = Arc::clone(&progress);
        let name_run = name.clone();
        tokio::spawn(async move {
            Self::run(
                signal_rx,
                error_tx,
                stopped_tx,
                driver,
                checkpoint_location,
                initial_batch_id,
                ui_id_run,
                query_id,
                run_id,
                name_run,
                progress_run,
            )
            .await;
        });
        Self {
            name,
            info,
            error: error_rx,
            stopped: stopped_rx,
            signal: Some(signal_tx),
            awaitable: true,
            progress,
        }
    }

    /// Recent micro-batch progress reports as JSON (newest last) — `lastProgress`/`recentProgress`.
    pub fn recent_progress(&self) -> Vec<String> {
        self.progress
            .lock()
            .map(|p| p.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn status(&self) -> StreamingQueryStatus {
        let stopped = *self.stopped.borrow();
        let default_message = if stopped {
            "The query is not active"
        } else {
            "The query is active"
        };
        StreamingQueryStatus {
            name: self.name.clone(),
            message: self
                .error
                .borrow()
                .as_ref()
                .map(|x| x.message().to_string())
                .unwrap_or_else(|| default_message.to_string()),
            is_active: !stopped,
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn run(
        signal: oneshot::Receiver<()>,
        error: watch::Sender<Option<SparkThrowable>>,
        stopped: watch::Sender<bool>,
        driver: StreamDriver,
        checkpoint_location: Option<String>,
        initial_batch_id: u64,
        ui_id: String,
        query_id: String,
        run_id: String,
        name: String,
        progress: Arc<Mutex<VecDeque<String>>>,
    ) {
        let offsets_dir = checkpoint_location.as_deref().map(|loc| {
            let mut p = PathBuf::from(loc);
            p.push("offsets");
            p
        });
        if let Some(ref dir) = offsets_dir {
            if let Err(e) = std::fs::create_dir_all(dir) {
                warn!("Failed to create checkpoint offsets dir {:?}: {e}", dir);
            }
        }
        if initial_batch_id > 0 {
            log::info!("Streaming checkpoint recovery: resuming from batch {initial_batch_id}");
        }
        let mut batch_id: u64 = initial_batch_id;
        let mut mb_id: u64 = 0; // micro-batch (trigger) index, for progress reporting

        let record = |plan: &Arc<dyn ExecutionPlan>, mb_id: u64, duration_ms: u128| {
            if let Ok(mut p) = progress.lock() {
                p.push_back(progress_json(
                    &query_id,
                    &run_id,
                    &name,
                    mb_id,
                    count_input_rows(plan),
                    duration_ms,
                    "streaming source",
                    "streaming sink",
                ));
                while p.len() > MAX_RECENT_PROGRESS {
                    p.pop_front();
                }
            }
        };

        match driver {
            StreamDriver::Once(plan, stream) => {
                let t0 = std::time::Instant::now();
                tokio::select! {
                    _ = signal => {}
                    clean = consume_stream(stream, &offsets_dir, &mut batch_id, &ui_id, &error) => {
                        // The micro-batch completed (availableNow/once). If clean, the output is
                        // durable, so commit the sources' staged offsets (write-ahead →
                        // committed) — exactly-once recovery on the next run.
                        if clean {
                            if let Some(ref loc) = checkpoint_location {
                                commit_source_offsets(loc);
                            }
                            record(&plan, mb_id, t0.elapsed().as_millis());
                        }
                    }
                }
            }
            StreamDriver::Continuous {
                make_stream,
                interval,
            } => {
                // Spark micro-batch model: each trigger is a fresh bounded micro-batch that
                // re-plans (picking up new files) and reuses the availableNow commit + state
                // recovery. Each micro-batch commits only after its output is durable (clean
                // end), so a crash replays only the uncommitted micro-batch — exactly-once.
                let mut signal = signal;
                loop {
                    let t0 = std::time::Instant::now();
                    let made = tokio::select! {
                        _ = &mut signal => break,
                        m = make_stream() => m,
                    };
                    let (plan, stream) = match made {
                        Ok(ps) => ps,
                        Err(e) => {
                            let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                            let _ = error.send(Some(cause.into()));
                            break;
                        }
                    };
                    let clean = tokio::select! {
                        _ = &mut signal => break,
                        c = consume_stream(stream, &offsets_dir, &mut batch_id, &ui_id, &error) => c,
                    };
                    if clean {
                        if let Some(ref loc) = checkpoint_location {
                            commit_source_offsets(loc);
                        }
                        record(&plan, mb_id, t0.elapsed().as_millis());
                        mb_id += 1;
                    } else {
                        break; // error already reported
                    }
                    // Wait for the next trigger (interruptible by stop).
                    tokio::select! {
                        _ = &mut signal => break,
                        _ = tokio::time::sleep(interval) => {}
                    }
                }
            }
        }

        web_ui::mark_stopped(&ui_id).await;
        let _ = stopped.send(true);
    }
}

/// Promote every source's staged (write-ahead) offset to committed (atomic rename),
/// once the batch output is durable. This is the commit step of the offset WAL →
/// commit-log protocol (Spark `MicroBatchExecution` model) — see
/// docs/design/streaming-exactly-once.md.
fn commit_source_offsets(checkpoint_location: &str) {
    // Source offsets: `<loc>/sources/<id>/staged` (file) -> `committed`.
    promote_staged(&PathBuf::from(checkpoint_location).join("sources"), false);
    // Operator state: `<loc>/state/<op>/staged` (dir) -> `committed`.
    promote_staged(&PathBuf::from(checkpoint_location).join("state"), true);
}

/// Promote every `<root>/<id>/staged` to `committed` (atomic rename), once the batch
/// output is durable — the commit step of the offset/state WAL protocol.
fn promote_staged(root: &std::path::Path, is_dir: bool) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let staged = entry.path().join("staged");
        if staged.exists() {
            let committed = entry.path().join("committed");
            if is_dir {
                let _ = std::fs::remove_dir_all(&committed);
            }
            if let Err(e) = std::fs::rename(&staged, &committed) {
                warn!("Failed to commit {staged:?}: {e}");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamingQueryId {
    pub query_id: String,
    pub run_id: String,
}

impl From<connect::StreamingQueryInstanceId> for StreamingQueryId {
    fn from(value: connect::StreamingQueryInstanceId) -> Self {
        Self {
            query_id: value.id,
            run_id: value.run_id,
        }
    }
}

impl From<StreamingQueryId> for connect::StreamingQueryInstanceId {
    fn from(value: StreamingQueryId) -> Self {
        Self {
            id: value.query_id,
            run_id: value.run_id,
        }
    }
}

pub struct StreamingQueryManager {
    queries: HashMap<StreamingQueryId, StreamingQuery>,
}

impl StreamingQueryManager {
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
        }
    }

    pub fn add_query(&mut self, id: StreamingQueryId, query: StreamingQuery) {
        self.queries.insert(id, query);
    }

    pub fn stop_query(&mut self, id: &StreamingQueryId) -> SparkResult<()> {
        let Some(query) = self.queries.get_mut(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        if let Some(signal) = query.signal.take() {
            let _ = signal.send(());
        };
        Ok(())
    }

    pub fn explain_query(&self, id: &StreamingQueryId, extended: bool) -> SparkResult<String> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        let mut result = String::new();
        let mut write = |kind: &'static str, t: &PlanType| {
            for item in query.info.iter() {
                if &item.plan_type == t {
                    result.push_str("== ");
                    result.push_str(kind);
                    result.push_str(" ==\n");
                    result.push_str(item.plan.trim_end_matches('\n'));
                    result.push_str("\n\n");
                }
            }
        };
        if extended {
            write("Initial Logical Plan", &PlanType::InitialLogicalPlan);
            write("Final Logical Plan", &PlanType::FinalLogicalPlan);
        }
        write("Final Physical Plan", &PlanType::FinalPhysicalPlan);
        Ok(result)
    }

    pub fn get_query_status(&self, id: &StreamingQueryId) -> SparkResult<StreamingQueryStatus> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.status())
    }

    pub fn recent_progress(&self, id: &StreamingQueryId) -> SparkResult<Vec<String>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.recent_progress())
    }

    pub fn get_query_error(&self, id: &StreamingQueryId) -> SparkResult<Option<SparkThrowable>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.error.borrow().clone())
    }

    pub fn find_query_by_query_id(
        &self,
        query_id: &str,
    ) -> SparkResult<(StreamingQueryId, StreamingQueryStatus)> {
        for (id, query) in self.queries.iter() {
            if id.query_id == query_id {
                return Ok((id.clone(), query.status()));
            }
        }
        Err(SparkError::invalid(format!(
            "streaming query not found by query id: {query_id}"
        )))
    }

    pub fn list_active_queries(&self) -> Vec<(StreamingQueryId, StreamingQueryStatus)> {
        self.queries
            .iter()
            .filter_map(|(id, query)| {
                if !*query.stopped.borrow() {
                    Some((id.clone(), query.status()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn await_query(
        &self,
        id: &StreamingQueryId,
    ) -> SparkResult<Option<StreamingQueryAwaitHandle>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        if !query.awaitable {
            Ok(None)
        } else {
            let stopped = query.stopped.clone();
            Ok(Some(StreamingQueryAwaitHandle { stopped }))
        }
    }

    pub fn await_queries(&self) -> SparkResult<StreamingQueryAwaitHandleSet> {
        let mut handles = Vec::new();
        for query in self.queries.values() {
            if query.awaitable {
                handles.push(StreamingQueryAwaitHandle {
                    stopped: query.stopped.clone(),
                });
            }
        }
        Ok(StreamingQueryAwaitHandleSet::new(handles))
    }

    pub fn reset_stopped_queries(&mut self) {
        for query in self.queries.values_mut() {
            if *query.stopped.borrow() {
                query.awaitable = false;
            }
        }
    }
}

pub struct StreamingQueryStatus {
    pub name: String,
    pub message: String,
    pub is_active: bool,
}

pub fn timeout_millis(value: i64) -> SparkResult<Duration> {
    if value < 0 {
        return Err(SparkError::invalid(format!(
            "invalid timeout value: {value}"
        )));
    }
    Ok(Duration::from_millis(value as u64))
}

pub struct StreamingQueryAwaitHandle {
    stopped: watch::Receiver<bool>,
}

impl StreamingQueryAwaitHandle {
    async fn wait(mut self) -> () {
        // We ignore the receiver error since the streaming query must have been
        // terminated in that case.
        let _ = self.stopped.wait_for(|x| *x).await;
    }

    pub async fn terminated(self, timeout: Option<Duration>) -> SparkResult<bool> {
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, self.wait()).await {
                Ok(()) => Ok(true),
                Err(_) => Ok(false),
            }
        } else {
            self.wait().await;
            Ok(true)
        }
    }
}

pub struct StreamingQueryAwaitHandleSet {
    handles: Vec<StreamingQueryAwaitHandle>,
}

impl StreamingQueryAwaitHandleSet {
    pub fn new(handles: Vec<StreamingQueryAwaitHandle>) -> Self {
        Self { handles }
    }

    pub async fn any_terminated(self, timeout: Option<Duration>) -> SparkResult<bool> {
        let mut join_set = tokio::task::JoinSet::new();
        for handle in self.handles {
            join_set.spawn(handle.wait());
        }
        let next = async move {
            match join_set.join_next().await {
                Some(Ok(())) => Ok(true),
                Some(Err(e)) => Err(SparkError::internal(format!(
                    "failed to await any termination for streaming queries: {e}"
                ))),
                None => Ok(false),
            }
        };
        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, next)
                .await
                .unwrap_or_else(|_| Ok(false))
        } else {
            next.await
        }
    }
}
