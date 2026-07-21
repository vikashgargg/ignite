use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use zelox_common::spec;
use zelox_common_datafusion::rename::physical_plan::rename_physical_plan;

use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::resolver::plan::NamedPlan;
use crate::resolver::PlanResolver;
use crate::streaming::rewriter::{is_streaming_plan, rewrite_streaming_plan};

pub mod catalog;
pub mod config;
pub mod error;
pub mod explain;
pub mod formatter;
pub mod function;
pub mod memory_buffer;
pub mod resolver;
mod streaming;

/// Executes a logical plan.
/// Catalog commands and barrier nodes are handled by the physical planner.
pub async fn execute_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

pub async fn resolve_and_execute_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    resolve_and_execute_plan_with_options(ctx, config, plan, false, None, None, false, 0).await
}

/// Like [`resolve_and_execute_plan`], but allows requesting bounded streaming
/// execution (trigger `availableNow`/`once`): stream sources scan the available
/// data and then end, so the streaming query terminates. `update_mode` +
/// `allowed_lateness_micros` select the windowed-aggregation changelog (update) output
/// (see docs/design/streaming-update-retraction-mode.md); default false/0 = append.
pub async fn resolve_and_execute_plan_with_options(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
    bounded: bool,
    checkpoint_location: Option<String>,
    realtime_interval_ms: Option<u64>,
    update_mode: bool,
    allowed_lateness_micros: i64,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let mut info = vec![];
    let resolver = PlanResolver::new(ctx, config);
    let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let streaming = is_streaming_plan(&plan)?;
    let plan = if streaming {
        rewrite_streaming_plan(
            plan,
            bounded,
            checkpoint_location,
            realtime_interval_ms,
            update_mode,
            allowed_lateness_micros,
        )?
    } else {
        plan
    };
    info.push(plan.to_stringified(PlanType::FinalLogicalPlan));
    let plan = if streaming {
        // A streaming pipeline is single-consumer: the driver polls the sink's
        // partition 0. DataFusion's physical optimizer otherwise inserts a
        // `RepartitionExec: RoundRobinBatch(N)` to parallelize the single-partition
        // source; with only partition 0 consumed, the other partitions' channels fill,
        // backpressure stalls the round-robin distributor, and NO batch ever reaches
        // the sink (continuous queries then produce nothing). Disable round-robin
        // repartitioning for streaming plans so the pipeline runs unbroken. (Scoped
        // narrowly — `target_partitions` and other rules are left untouched to avoid
        // affecting existing streaming operators.)
        let mut config = session_state.config().clone();
        config.options_mut().optimizer.enable_round_robin_repartition = false;
        // Streaming file sources read at whole-file granularity. Row-group byte-range
        // splitting (`repartition_file_scans`, active when target_partitions > file count)
        // produces split partitions that the streaming read/sink path drains incorrectly —
        // disable it so each partition is a whole file group (the verified-correct regime),
        // enabling safe parallel-per-file source/sink fan-out.
        config.options_mut().optimizer.repartition_file_scans = false;
        let streaming_state = SessionStateBuilder::new_from_existing(session_state.clone())
            .with_config(config)
            .build();
        streaming_state
            .query_planner()
            .create_physical_plan(&plan, &streaming_state)
            .await?
    } else {
        session_state
            .query_planner()
            .create_physical_plan(&plan, &session_state)
            .await?
    };
    let plan = if let Some(fields) = fields {
        rename_physical_plan(plan, &fields)?
    } else {
        plan
    };
    info.push(StringifiedPlan::new(
        PlanType::FinalPhysicalPlan,
        displayable(plan.as_ref()).indent(true).to_string(),
    ));
    Ok((plan, info))
}
