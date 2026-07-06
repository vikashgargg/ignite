use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::debug;
use prost::Message;
use sail_common_datafusion::error::CommonErrorCause;
use sail_delta_lake::physical_plan::DeltaPhysicalExprAdapterFactory;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{Actor, ActorContext};
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{trace_execution_plan, TracingExecOptions};
use tokio::sync::oneshot;

use crate::codec::RemoteExecutionCodec;
use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec, StageInputExec};
use crate::stream_accessor::{StreamAccessor, StreamAccessorMessage};
use crate::task::definition::{TaskDefinition, TaskInput, TaskOutput};
use crate::task_runner::monitor::TaskMonitor;
use crate::task_runner::{TaskRunner, TaskRunnerMessage};

impl TaskRunner {
    pub fn new() -> Self {
        Self {
            signals: HashMap::new(),
            codec: Box::new(RemoteExecutionCodec),
        }
    }

    pub fn run_task<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: TaskKey,
        definition: TaskDefinition,
        context: Arc<TaskContext>,
    ) where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let stream = match self.execute_plan(ctx, &key, definition, context) {
            Ok(x) => x,
            Err(e) => {
                let event = T::Message::report_task_status(
                    key,
                    TaskStatus::Failed,
                    Some(format!("failed to execute plan: {e}")),
                    Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
                );
                ctx.send(event);
                return;
            }
        };
        let handle = ctx.handle().clone();
        let (tx, rx) = oneshot::channel();
        self.signals.insert(key.clone(), tx);
        let monitor = TaskMonitor::new(handle, key, stream, rx);
        ctx.spawn(monitor.run());
    }

    pub fn stop_task(&mut self, key: &TaskKey) {
        if let Some(signal) = self.signals.remove(key) {
            let _ = signal.send(());
        }
    }

    /// Deserializes and prepares a physical plan for execution on this node.
    fn execute_plan<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskKey,
        definition: TaskDefinition,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<SendableRecordBatchStream>
    where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let plan = PhysicalPlanNode::decode(definition.plan.as_ref())?;
        let plan = plan.try_into_physical_plan(&context, self.codec.as_ref())?;
        let plan = self.rewrite_parquet_adapters(plan)?;
        let plan = self.rewrite_shuffle(
            ctx,
            key,
            &definition.inputs,
            &definition.output,
            plan,
            &context,
        )?;
        debug!(
            "{} execution plan\n{}",
            TaskKeyDisplay(key),
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let options = TracingExecOptions {
            metrics: global_metrics(),
            job_id: Some(key.job_id.into()),
            stage: Some(key.stage),
            attempt: Some(key.attempt),
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        let stream = plan.execute(key.partition, context)?;
        Ok(stream)
    }

    fn rewrite_parquet_adapters(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform(|node| {
            let Some(ds) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(config) = ds.data_source().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(node));
            };
            // Distributed execution runs each output partition as an ISOLATED task. DataFusion 54's
            // morsel-driven file scan pools every file into a `SharedWorkSource` across sibling
            // partition streams so in-process siblings steal work and read each file exactly once.
            // A distributed task has no in-process siblings, so it would drain the whole pool and
            // re-read every file once per partition (N file groups -> N× duplication). Marking the
            // scan `partitioned_by_file_group` disables the shared work source (see
            // `FileScanConfig::create_sibling_state`), so each partition reads ONLY its own file
            // group via `WorkSource::Local` — which is exactly the fixed one-group-per-task model
            // the scheduler already assigns.
            let mut builder =
                FileScanConfigBuilder::from(config.clone()).with_partitioned_by_file_group(true);
            // Parquet additionally gets the Delta schema-evolution expr adapter.
            if ds.downcast_to_file_source::<ParquetSource>().is_some() {
                builder = builder.with_expr_adapter(Some(Arc::new(DeltaPhysicalExprAdapterFactory {})));
            }
            let new_exec = DataSourceExec::from_data_source(builder.build());
            Ok(Transformed::yes(new_exec as Arc<dyn ExecutionPlan>))
        });
        Ok(result.data()?)
    }

    fn rewrite_shuffle<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskKey,
        inputs: &[TaskInput],
        output: &TaskOutput,
        plan: Arc<dyn ExecutionPlan>,
        context: &TaskContext,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>>
    where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let handle = ctx.handle();
        let result = plan.transform(move |node| {
            if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
                let Some(input) = inputs.get(*placeholder.input()) else {
                    return internal_err!(
                        "stage input index {} out of bounds for {}",
                        placeholder.input(),
                        TaskKeyDisplay(key)
                    );
                };
                let locations = input.locations(key.job_id);
                let accessor = StreamAccessor::new(handle.clone());
                let shuffle = ShuffleReadExec::new(
                    locations,
                    Arc::new(accessor),
                    placeholder.properties().clone(),
                );
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else {
                Ok(Transformed::no(node))
            }
        });
        let plan = result.data()?;
        let schema = plan.schema();
        let accessor = StreamAccessor::new(handle.clone());
        let mut locations = vec![vec![]; plan.output_partitioning().partition_count()];
        match locations.get_mut(key.partition) {
            Some(x) => x.extend(output.locations(key)),
            None => {
                return Err(ExecutionError::InternalError(format!(
                    "invalid partition: {}",
                    TaskKeyDisplay(key)
                )));
            }
        };
        let partitioning = output.partitioning(context, &schema, self.codec.as_ref())?;
        let shuffle = ShuffleWriteExec::new(plan, locations, Arc::new(accessor), partitioning);
        Ok(Arc::new(shuffle))
    }
}
