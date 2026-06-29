use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::{source_as_provider, TableProvider};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{internal_err, not_impl_err, plan_err, Result};
use datafusion_expr::{
    col, lit, or, Aggregate, Distinct, DistinctOn, Explain, Expr, FetchType, Filter, Join,
    Projection, SkipType, SubqueryAlias, TableScan, Union, UserDefinedLogicalNode, Window,
};
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;
use sail_common_datafusion::streaming::event::schema::{
    is_flow_event_schema, MARKER_FIELD_NAME, RETRACTED_FIELD_NAME,
};
use sail_common_datafusion::streaming::source::{StreamSource, StreamSourceTableProvider};
use sail_logical_plan::barrier::BarrierNode;
use sail_logical_plan::file_write::FileWriteNode;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::show_string::ShowStringNode;
use sail_logical_plan::streaming::collector::StreamCollectorNode;
use sail_logical_plan::streaming::dedup::StreamDeduplicateNode;
use sail_logical_plan::streaming::filter::StreamFilterNode;
use sail_logical_plan::streaming::foreach_batch::ForeachBatchSinkNode;
use sail_logical_plan::streaming::limit::StreamLimitNode;
use sail_logical_plan::streaming::memory_sink::MemorySinkNode;
use sail_logical_plan::streaming::source_adapter::StreamSourceAdapterNode;
use sail_logical_plan::streaming::source_wrapper::StreamSourceWrapperNode;
use sail_logical_plan::streaming::stream_join::StreamJoinNode;
use sail_logical_plan::streaming::watermark::WatermarkNode;
use sail_logical_plan::streaming::window_accum::WindowAccumNode;

/// A logical plan rewriter that rewrites a batch logical plan
/// into a streaming logical plan. All the nodes (except the sink) in the plan
/// will have a flow event schema which contains additional fields
/// along with the original data fields.
struct StreamingRewriter {
    /// Trigger `availableNow`/`once`: stream sources scan available data then stop.
    bounded: bool,
    /// Streaming `checkpointLocation`, threaded to sources for offset recovery.
    checkpoint_location: Option<String>,
    /// `Trigger.Continuous` epoch-commit interval (millis), threaded to sources for realtime
    /// exactly-once (source emits `Checkpoint{epoch}` + pre-commits offsets at this cadence).
    realtime_interval_ms: Option<u64>,
    /// `outputMode("update")`: window aggregation emits a changelog (retract+insert) instead of
    /// append-on-close. Default false (append). See docs/design/streaming-update-retraction-mode.md.
    update_mode: bool,
    /// Update mode only: `allowedLateness` window-state retention past close (micros).
    allowed_lateness_micros: i64,
    /// Per-partition watermark (realtime): the source `partition` column name, set when a realtime
    /// Kafka stream source is seen (force-kept past projection pruning), preserved through projections,
    /// and attached to the WatermarkNode so WatermarkExec does MIN-over-partitions with idleness.
    preserve_partition: Option<String>,
}

impl StreamingRewriter {
    fn f_up_extension(&mut self, extension: Extension) -> Result<Transformed<LogicalPlan>> {
        let node = extension.node.as_ref();
        if node.as_any().is::<RangeNode>() {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(
                    LogicalPlan::Extension(extension),
                ))?),
            })))
        } else if let Some(show) = node.as_any().downcast_ref::<ShowStringNode>() {
            let input = LogicalPlan::Extension(Extension {
                node: Arc::new(StreamCollectorNode::try_new(Arc::clone(show.input()))?),
            });
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: show.with_exprs_and_inputs(vec![], vec![input])?,
            })))
        } else if node.as_any().is::<sail_logical_plan::repartition::ExplicitRepartitionNode>() {
            // Streaming repartition: pass the node through unchanged so the physical planner maps it
            // to the marker-aware `StreamExchangeExec` (keyed) for parallel stateless processing; the
            // sink writer re-aligns N→1 via `StreamBarrierAlignExec`. (See planner.rs.)
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else if let Some(wm) = node.as_any().downcast_ref::<WatermarkNode>() {
            // Per-partition watermark: if a source `partition` column was preserved to here, attach it
            // so WatermarkExec does MIN-over-partitions with a pure-time startup grace + idleness. No
            // partition-count N needed (passed 0): the grace is time-based, not count-based.
            if let Some(p) = self.preserve_partition.clone() {
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(wm.clone().with_partition_watermark(p, 0)),
                })))
            } else {
                Ok(Transformed::no(LogicalPlan::Extension(extension)))
            }
        } else if node.as_any().is::<FileWriteNode>()
            || node.as_any().is::<ForeachBatchSinkNode>()
            || node.as_any().is::<MemorySinkNode>()
            || node.as_any().is::<BarrierNode>()
        {
            // Passthrough nodes left untouched by the streaming rewriter:
            // - FileWriteNode / ForeachBatchSinkNode / MemorySinkNode: sinks.
            // - BarrierNode: TODO — real streaming support.
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else {
            plan_err!("unsupported extension node for streaming: {node:?}")
        }
    }
}

impl TreeNodeRewriter for StreamingRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(extension) => self.f_up_extension(extension),
            LogicalPlan::Projection(projection) => {
                let Projection {
                    mut expr, input, ..
                } = projection;
                expr.insert(0, col(MARKER_FIELD_NAME));
                expr.insert(1, col(RETRACTED_FIELD_NAME));
                // Per-partition watermark: carry the source `partition` column up if the user's
                // projection would drop it, so it reaches the WatermarkNode (harmless extra column).
                if let Some(p) = &self.preserve_partition {
                    let have = input.schema().has_column_with_unqualified_name(p);
                    let kept = expr
                        .iter()
                        .any(|e| matches!(e, Expr::Column(c) if c.name() == *p));
                    if have && !kept {
                        expr.push(col(p));
                    }
                }
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(expr, input)?,
                )))
            }
            LogicalPlan::Filter(filter) => {
                let Filter {
                    predicate, input, ..
                } = filter;
                let predicate = or(predicate, col(MARKER_FIELD_NAME).is_not_null());
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamFilterNode::new(input, predicate)),
                })))
            }
            LogicalPlan::Window(window) => {
                // Stateless per-micro-batch analytic window (e.g. rank, lag, row_number).
                // Strip flow-event schema → apply window on data columns → re-add flow-event cols.
                let streaming_input = window.input.clone();
                let data_schema = try_from_flow_event_schema(streaming_input.schema().inner())?;
                let data_cols: Vec<_> = data_schema
                    .fields()
                    .iter()
                    .map(|f| col(f.name().as_str()))
                    .collect();
                let data_only = Arc::new(LogicalPlan::Projection(Projection::try_new(
                    data_cols,
                    Arc::clone(&streaming_input),
                )?));
                let new_window = Arc::new(LogicalPlan::Window(Window::try_new(
                    window.window_expr.clone(),
                    data_only,
                )?));
                let mut out_exprs: Vec<_> = new_window
                    .schema()
                    .columns()
                    .into_iter()
                    .map(datafusion_expr::Expr::Column)
                    .collect();
                out_exprs.push(
                    lit(datafusion_common::ScalarValue::Binary(None)).alias(MARKER_FIELD_NAME),
                );
                out_exprs.push(lit(false).alias(RETRACTED_FIELD_NAME));
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(out_exprs, new_window)?,
                )))
            }
            LogicalPlan::Aggregate(agg) => {
                let streaming_input = agg.input.clone();
                let data_schema = try_from_flow_event_schema(streaming_input.schema().inner())?;

                // Build the data-only pipeline (filter retracted + project columns).
                let filter = LogicalPlan::Filter(Filter::try_new(
                    col(RETRACTED_FIELD_NAME).eq(lit(false)),
                    Arc::clone(&streaming_input),
                )?);
                let data_cols: Vec<_> = data_schema
                    .fields()
                    .iter()
                    .map(|f| col(f.name().as_str()))
                    .collect();
                let data_only =
                    LogicalPlan::Projection(Projection::try_new(data_cols, Arc::new(filter))?);

                // Detect event-time window aggregation.
                // Criteria: (1) group_by contains a window() alias ("window" or "session_window"),
                // AND (2) there is a WatermarkNode somewhere in the input subtree.
                let is_window_group = has_event_time_window_group(&agg.group_expr);

                // `dropDuplicates`/`DISTINCT` is planned as an Aggregate that just keeps the
                // first row per group key — either `aggr=[]` (full-row distinct) or
                // `aggr=[first_value(col), ...]` (`dropDuplicates(subset)`). A global aggregate
                // over an unbounded stream is pipeline-breaking; this is really **stateful
                // deduplication** (Spark `dropDuplicates` / Flink keyed dedup), so route it to
                // `StreamDeduplicateNode` (one row per key, all columns retained).
                // `aggr=[]` (full-row DISTINCT) or `aggr=[first_value(col), ...]`
                // (`dropDuplicates(subset)`); group keys must be plain columns.
                let is_dedup_aggregate = !is_window_group
                    && agg.aggr_expr.iter().all(is_first_value_agg)
                    && agg
                        .group_expr
                        .iter()
                        .all(|e| matches!(strip_qualifiers(e), Expr::Column(_)));
                if is_dedup_aggregate {
                    let key_cols: Vec<String> = agg
                        .group_expr
                        .iter()
                        .filter_map(|e| match strip_qualifiers(e) {
                            Expr::Column(c) => Some(c.name),
                            _ => None,
                        })
                        .collect();
                    // Keyed stateful dedup over the flow-event stream (markers preserved).
                    // If the input carries a watermark, dedup state is bounded (evict keys
                    // older than the watermark, drop late rows) — Spark dropDuplicates +
                    // withWatermark / dropDuplicatesWithinWatermark.
                    let event_time_col = find_watermark_info(&streaming_input).map(|(c, _)| c);
                    let dedup = LogicalPlan::Extension(Extension {
                        node: Arc::new(StreamDeduplicateNode::new_with_watermark(
                            Arc::clone(&streaming_input),
                            key_cols,
                            event_time_col,
                        )),
                    });
                    // Reconstruct the Aggregate's output schema from the deduped first row
                    // (`first_value(c)` ⇒ `col(c)`), re-adding the flow-event columns so the
                    // parent projection resolves unchanged.
                    // Match the Aggregate's output field *qualifiers* too (e.g. `?table?.#0`),
                    // so the parent projection — which still references the qualified names —
                    // resolves against this reconstruction.
                    let mut proj: Vec<Expr> =
                        vec![col(MARKER_FIELD_NAME), col(RETRACTED_FIELD_NAME)];
                    let n_group = agg.group_expr.len();
                    for (i, g) in agg.group_expr.iter().enumerate() {
                        let (q, f) = agg.schema.qualified_field(i);
                        proj.push(strip_qualifiers(g).alias_qualified(q.cloned(), f.name()));
                    }
                    for (j, a) in agg.aggr_expr.iter().enumerate() {
                        let (q, f) = agg.schema.qualified_field(n_group + j);
                        let inner = first_value_inner(a).cloned().unwrap_or_else(|| a.clone());
                        proj.push(strip_qualifiers(&inner).alias_qualified(q.cloned(), f.name()));
                    }
                    return Ok(Transformed::yes(LogicalPlan::Projection(
                        Projection::try_new(proj, Arc::new(dedup))?,
                    )));
                }

                let watermark_info = if is_window_group {
                    find_watermark_info(&streaming_input)
                } else {
                    None
                };

                if let Some((event_time_col, delay_micros)) = watermark_info {
                    // Stateful event-time window aggregation via WindowAccumNode.
                    // The physical planner will map this to WindowAccumExec.
                    // Streaming data schemas are unqualified; strip relation qualifiers from
                    // the group/aggregate exprs (e.g. `?table?."#1"` → `"#1"`) so an
                    // inline-expression group key (`value % 10`) resolves against the data
                    // schema — same as the stream-join path. Detection ran on the original.
                    let group_expr: Vec<Expr> =
                        agg.group_expr.iter().map(strip_qualifiers).collect();
                    let aggr_expr: Vec<Expr> =
                        agg.aggr_expr.iter().map(strip_qualifiers).collect();
                    // Compute what the aggregate output schema would be.
                    let agg_output_schema = {
                        let trial = Aggregate::try_new(
                            Arc::new(data_only.clone()),
                            group_expr.clone(),
                            aggr_expr.clone(),
                        )?;
                        trial.schema.clone()
                    };
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(WindowAccumNode::new(
                            // Feed the flow-event input so Watermark markers reach the
                            // operator; the aggregate exprs are built against the data
                            // schema in the physical planner. `data_only` above is used
                            // only to derive the aggregate output schema.
                            (*streaming_input).clone(),
                            group_expr,
                            aggr_expr,
                            event_time_col,
                            delay_micros,
                            Arc::new(datafusion_common::DFSchema::try_from(
                                std::sync::Arc::new(
                                    sail_common_datafusion::streaming::event::schema::to_flow_event_schema(
                                        agg_output_schema.as_arrow()
                                    )
                                )
                            )?),
                            agg_output_schema,
                            self.checkpoint_location.clone(),
                        )
                        .with_output_mode(self.update_mode, self.allowed_lateness_micros)),
                    })));
                }

                // Stateless per-micro-batch aggregation (append-mode).
                let new_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(data_only),
                    agg.group_expr.clone(),
                    agg.aggr_expr.clone(),
                )?);
                let mut out_exprs: Vec<_> = new_agg
                    .schema()
                    .columns()
                    .into_iter()
                    .map(datafusion_expr::Expr::Column)
                    .collect();
                out_exprs.push(
                    lit(datafusion_common::ScalarValue::Binary(None)).alias(MARKER_FIELD_NAME),
                );
                out_exprs.push(lit(false).alias(RETRACTED_FIELD_NAME));
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(out_exprs, Arc::new(new_agg))?,
                )))
            }
            LogicalPlan::Sort(_) => {
                plan_err!("sort is not supported for streaming: {plan:?}")
            }
            LogicalPlan::Join(join) => {
                // A join where BOTH sides are unbounded streams needs a stateful,
                // watermark-bounded operator (keyed dual-state + min-merge eviction).
                // The per-micro-batch path below only matches rows within the same
                // batch — it silently produces no cross-batch matches (0 rows). Fail
                // loudly instead of returning wrong results; see
                // docs/design/streaming-stream-join.md. Stream × static joins (one
                // bounded side) still work via the per-micro-batch path.
                let left_data_schema = try_from_flow_event_schema(join.left.schema().inner())?;
                let right_data_schema = try_from_flow_event_schema(join.right.schema().inner())?;

                if contains_stream_source(&join.left) && contains_stream_source(&join.right) {
                    // Stateful stream × stream join (see docs/design/streaming-stream-join.md).
                    // Inner equi-join, optionally with a residual time-range (interval) filter.
                    if join.join_type != datafusion_expr::JoinType::Inner {
                        return not_impl_err!(
                            "stream-stream join: only inner join is supported yet (got {:?})",
                            join.join_type
                        );
                    }
                    // Build data-only projections only to derive the join output schema.
                    // Output flow schema = marker/retracted ++ left data cols ++ right data
                    // cols, preserving each input's relation qualifier (e.g. `a`/`b`) so the
                    // consuming plan can resolve qualified column references. The two input
                    // flow schemas start with [_marker, _retracted, <qualified data...>].
                    let left_df = join.left.schema();
                    let right_df = join.right.schema();
                    let mut qfields: Vec<(
                        Option<datafusion_common::TableReference>,
                        std::sync::Arc<datafusion::arrow::datatypes::Field>,
                    )> = vec![
                        (None, left_df.field(0).clone()),
                        (None, left_df.field(1).clone()),
                    ];
                    for (q, f) in left_df.iter().skip(2) {
                        qfields.push((q.cloned(), f.clone()));
                    }
                    for (q, f) in right_df.iter().skip(2) {
                        qfields.push((q.cloned(), f.clone()));
                    }
                    let flow_schema = Arc::new(datafusion_common::DFSchema::new_with_metadata(
                        qfields,
                        std::collections::HashMap::new(),
                    )?);
                    // Derive the interval-join time columns + bounds directly from the
                    // residual filter (the interval condition); these drive state eviction.
                    // If not a recognizable bounded interval, state stays unbounded (Spark
                    // default) but matches are still correct (the filter is applied).
                    // Strip relation qualifiers: streaming data schemas are unqualified and
                    // `#N` ids are unique, so `a."#2"` from an explicit join condition must
                    // become `"#2"` for the node's expressions to resolve downstream.
                    let on: Vec<(Expr, Expr)> = join
                        .on
                        .iter()
                        .map(|(l, r)| (strip_qualifiers(l), strip_qualifiers(r)))
                        .collect();
                    let filter = join.filter.as_ref().map(strip_qualifiers);
                    let bounds = filter
                        .as_ref()
                        .and_then(|f| extract_interval_bounds(f, &left_data_schema, &right_data_schema));
                    let (left_event_time, right_event_time, interval_bounds) = match bounds {
                        Some((l, r, lo, hi)) => (Some(l), Some(r), Some((lo, hi))),
                        None => (None, None, None),
                    };
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(StreamJoinNode::new(
                            Arc::clone(&join.left),
                            Arc::clone(&join.right),
                            on,
                            filter,
                            join.join_type,
                            left_event_time,
                            right_event_time,
                            interval_bounds,
                            self.checkpoint_location.clone(),
                            flow_schema,
                        )),
                    })));
                }
                // Per-micro-batch join (stream × static): strip flow-event columns from
                // both sides, run the original join on data-only schemas, re-add columns.

                let left_cols: Vec<_> = left_data_schema
                    .fields()
                    .iter()
                    .map(|f| col(f.name().as_str()))
                    .collect();
                let left_data = Arc::new(LogicalPlan::Projection(Projection::try_new(
                    left_cols,
                    Arc::clone(&join.left),
                )?));

                let right_cols: Vec<_> = right_data_schema
                    .fields()
                    .iter()
                    .map(|f| col(f.name().as_str()))
                    .collect();
                let right_data = Arc::new(LogicalPlan::Projection(Projection::try_new(
                    right_cols,
                    Arc::clone(&join.right),
                )?));

                let new_join = Arc::new(LogicalPlan::Join(Join::try_new(
                    left_data,
                    right_data,
                    join.on.clone(),
                    join.filter.clone(),
                    join.join_type,
                    join.join_constraint,
                    join.null_equality,
                    join.null_aware,
                )?));

                let mut out_exprs: Vec<_> = new_join
                    .schema()
                    .columns()
                    .into_iter()
                    .map(datafusion_expr::Expr::Column)
                    .collect();
                out_exprs.push(
                    lit(datafusion_common::ScalarValue::Binary(None)).alias(MARKER_FIELD_NAME),
                );
                out_exprs.push(lit(false).alias(RETRACTED_FIELD_NAME));

                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(out_exprs, new_join)?,
                )))
            }
            LogicalPlan::Repartition(repartition) => {
                // Repartitioning is a no-op in streaming: data arrives as
                // a single flow-event stream; explicit partitioning has no
                // meaning at the micro-batch level.
                Ok(Transformed::yes((*repartition.input).clone()))
            }
            LogicalPlan::TableScan(ref scan) => {
                if let Ok(provider) = source_as_provider(&scan.source) {
                    if let Some(source) = get_stream_source_opt(provider.as_ref()) {
                        let NamedStreamSource { source, names } = source;
                        let TableScan {
                            table_name,
                            source: _,
                            projection,
                            projected_schema: _,
                            filters,
                            fetch,
                        } = scan;
                        // Per-partition watermark (realtime): the Kafka `partition` column gets pruned
                        // by projection pushdown (the user query doesn't reference it), so force it back
                        // into the scan projection and record its name to thread to the WatermarkNode.
                        // Idleness guard in WatermarkExec means this can never stall.
                        let mut projection = projection.clone();
                        if self.realtime_interval_ms.is_some() {
                            let full = provider.schema();
                            if let Some(pidx) =
                                full.fields().iter().position(|f| f.name() == "partition")
                            {
                                if let Some(p) = projection.as_mut() {
                                    if !p.contains(&pidx) {
                                        p.push(pidx);
                                    }
                                }
                                self.preserve_partition = Some(full.field(pidx).name().to_string());
                            }
                        }
                        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                            node: Arc::new(StreamSourceWrapperNode::try_new(
                                table_name.clone(),
                                source,
                                names,
                                projection,
                                filters.clone(),
                                *fetch,
                                self.bounded,
                                self.checkpoint_location.clone(),
                                self.realtime_interval_ms,
                            )?),
                        })))
                    } else {
                        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                            node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(plan))?),
                        })))
                    }
                } else {
                    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(plan))?),
                    })))
                }
            }
            LogicalPlan::Union(union) => Ok(Transformed::yes(LogicalPlan::Union(
                Union::try_new_with_loose_types(union.inputs)?,
            ))),
            LogicalPlan::SubqueryAlias(alias) => Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(alias.input, alias.alias)?,
            ))),
            LogicalPlan::Limit(ref limit) => {
                let SkipType::Literal(skip) = limit.get_skip_type()? else {
                    return plan_err!("streaming limit requires literal skip: {plan:?}");
                };
                let FetchType::Literal(fetch) = limit.get_fetch_type()? else {
                    return plan_err!("streaming limit requires literal fetch: {plan:?}");
                };
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamLimitNode::new(Arc::clone(&limit.input), skip, fetch)),
                })))
            }
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(plan))?),
                })))
            }
            LogicalPlan::Unnest(_) => {
                // We need to preserve all markers in the unnested record batches.
                // This can be done by having a placeholder one-element nested value
                // for each marker row.
                not_impl_err!("streaming unnest: {plan:?}")
            }
            LogicalPlan::RecursiveQuery(_) => {
                not_impl_err!("recursive streaming query: {plan:?}")
            }
            LogicalPlan::Subquery(_) => {
                internal_err!("not rewritten before streaming rewriter: {plan:?}")
            }
            LogicalPlan::Distinct(distinct) => {
                // Convert to stateful streaming deduplication.
                // Bottom-up rewriting means `input` already has the flow-event schema.
                let (input, key_cols) = match distinct {
                    Distinct::All(input) => {
                        let data_schema = try_from_flow_event_schema(input.schema().inner())?;
                        let key_cols = data_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect();
                        (input, key_cols)
                    }
                    Distinct::On(DistinctOn { on_expr, input, .. }) => {
                        let key_cols = on_expr
                            .iter()
                            .filter_map(|e| match e {
                                Expr::Column(c) => Some(c.name.clone()),
                                _ => None,
                            })
                            .collect();
                        (input, key_cols)
                    }
                };
                // Bounded dedup state when the input carries a watermark (evict keys older
                // than the watermark, drop late rows).
                let event_time_col = find_watermark_info(&input).map(|(c, _)| c);
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamDeduplicateNode::new_with_watermark(
                        input,
                        key_cols,
                        event_time_col,
                    )),
                })))
            }
            LogicalPlan::Explain(explain) => {
                let input = LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamCollectorNode::try_new(Arc::clone(&explain.plan))?),
                });
                Ok(Transformed::yes(LogicalPlan::Explain(Explain {
                    plan: Arc::new(input),
                    ..explain
                })))
            }
            LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_) => {
                internal_err!("unexpected command for streaming rewriter: {plan:?}")
            }
        }
    }
}

fn is_streaming_table_provider(provider: &dyn TableProvider) -> bool {
    if provider.as_any().is::<StreamSourceTableProvider>() {
        true
    } else if let Some(rename) = provider.as_any().downcast_ref::<RenameTableProvider>() {
        is_streaming_table_provider(rename.inner().as_ref())
    } else {
        false
    }
}

struct NamedStreamSource {
    source: Arc<dyn StreamSource>,
    names: Option<Vec<String>>,
}

fn get_stream_source_opt(provider: &dyn TableProvider) -> Option<NamedStreamSource> {
    if let Some(stream) = provider
        .as_any()
        .downcast_ref::<StreamSourceTableProvider>()
    {
        Some(NamedStreamSource {
            source: stream.source().clone(),
            names: None,
        })
    } else if let Some(rename) = provider.as_any().downcast_ref::<RenameTableProvider>() {
        if let Some(stream) = get_stream_source_opt(rename.inner().as_ref()) {
            Some(NamedStreamSource {
                source: stream.source,
                names: Some(
                    rename
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect(),
                ),
            })
        } else {
            None
        }
    } else {
        None
    }
}

pub fn is_streaming_plan(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|plan| {
        if let LogicalPlan::TableScan(scan) = plan {
            Ok(source_as_provider(&scan.source)
                .is_ok_and(|p| is_streaming_table_provider(p.as_ref())))
        } else {
            Ok(false)
        }
    })
}

/// Rewrite a logical plan for streaming execution.
/// This function needs to be called on an optimized logical plan, and after
/// all logical commands are executed. An error will be returned if the plan
/// contains logical command nodes or nodes that should be eliminated by the
/// optimizer (e.g. subquery).
pub fn rewrite_streaming_plan(
    plan: LogicalPlan,
    bounded: bool,
    checkpoint_location: Option<String>,
    realtime_interval_ms: Option<u64>,
    update_mode: bool,
    allowed_lateness_micros: i64,
) -> Result<LogicalPlan> {
    // Systemic qualifier normalization: streaming operators decode flow events into the
    // **unqualified** data schema, while the resolver tags columns with a synthetic
    // relation (`?table?`). That mismatch has bitten window aggregation, dedup, and joins
    // individually. Strip relation qualifiers once, up front — safe because the resolver's
    // internal `#N` column ids are globally unique (no cross-relation ambiguity) — so every
    // downstream streaming operator sees consistent, unqualified columns.
    let plan = strip_plan_qualifiers(plan)?;
    let node = plan.rewrite(&mut StreamingRewriter {
        bounded,
        checkpoint_location,
        realtime_interval_ms,
        update_mode,
        allowed_lateness_micros,
        preserve_partition: None,
    })?;
    let plan = node.data;

    if is_flow_event_schema(plan.schema().inner()) {
        // If the plan has a flow event schema, it is a streaming query without sink.
        // So we need to collect the (retractable) data batches.
        // During physical planning, the stream collector will return an error if the plan
        // is not bounded, since the query result cannot have infinite size.
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(StreamCollectorNode::try_new(Arc::new(plan))?),
        }))
    } else {
        Ok(plan)
    }
}

/// Returns true if any group_by expression is an `Alias` named "window" or "session_window",
/// indicating an event-time window grouping produced by `spark_window()` or `spark_session_window()`.
fn has_event_time_window_group(group_expr: &[datafusion_expr::Expr]) -> bool {
    use datafusion_expr::Expr;
    group_expr.iter().any(|e| match e {
        Expr::Alias(alias) => matches!(alias.name.as_str(), "window" | "session_window"),
        _ => false,
    })
}

/// True if the (rewritten) plan subtree contains a real streaming source
/// (`StreamSourceWrapperNode`), i.e. an unbounded stream — as opposed to a static
/// table that was adapted to streaming. Used to distinguish stream×stream joins
/// (both sides unbounded) from stream×static joins.
fn contains_stream_source(plan: &LogicalPlan) -> bool {
    use datafusion_common::tree_node::TreeNodeRecursion;
    let mut found = false;
    let _ = plan.apply(|p| {
        if let LogicalPlan::Extension(ext) = p {
            if ext.node.as_any().is::<StreamSourceWrapperNode>() {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Extract the interval-join time columns and bounds from a residual time-range filter
/// of the canonical (Spark/Flink) form `right_ts >= left_ts + L AND right_ts <= left_ts + U`
/// — a match requires `right.ts ∈ [left.ts + L, left.ts + U]`. Returns
/// `(left_ts_col, right_ts_col, lower_micros, upper_micros)`, or `None` if the filter is
/// not a recognizable bounded interval condition (then the join keeps unbounded state,
/// matching Spark's default; matches stay correct since the filter is still applied).
fn extract_interval_bounds(
    filter: &Expr,
    left_schema: &datafusion::arrow::datatypes::Schema,
    right_schema: &datafusion::arrow::datatypes::Schema,
) -> Option<(String, String, i64, i64)> {
    use datafusion_expr::Operator;
    let mut conj: Vec<&Expr> = vec![];
    collect_conjuncts(filter, &mut conj);
    let (mut lower, mut upper) = (None::<i64>, None::<i64>);
    let (mut left_ts, mut right_ts) = (None::<String>, None::<String>);
    for c in conj {
        let Expr::BinaryExpr(be) = c else { continue };
        let is_lower = matches!(be.op, Operator::GtEq | Operator::Gt);
        let is_upper = matches!(be.op, Operator::LtEq | Operator::Lt);
        if !is_lower && !is_upper {
            continue;
        }
        // Canonical form: `right_ts CMP left_ts (+/- duration)`.
        let Expr::Column(cmp) = be.left.as_ref() else { continue };
        let Some((other_name, offset)) = parse_col_offset(be.right.as_ref()) else { continue };
        if right_schema.field_with_name(&cmp.name).is_err()
            || left_schema.field_with_name(&other_name).is_err()
        {
            continue;
        }
        right_ts = Some(cmp.name.clone());
        left_ts = Some(other_name);
        if is_lower {
            lower = Some(offset);
        } else {
            upper = Some(offset);
        }
    }
    match (left_ts, right_ts, lower, upper) {
        (Some(l), Some(r), Some(lo), Some(hi)) => Some((l, r, lo, hi)),
        _ => None,
    }
}

/// Strip relation qualifiers from all column references in an expression. The streaming
/// data schemas are unqualified and internal `#N` ids are globally unique, so a
/// qualified `a."#2"` from an explicit join condition must become `"#2"` to resolve.
/// Is this aggregate expression `first_value(...)`? `dropDuplicates(subset)` is planned as
/// `Aggregate(group=[keys], aggr=[first_value(col), ...])` — keep the first row per key.
fn is_first_value_agg(e: &Expr) -> bool {
    matches!(e, Expr::AggregateFunction(af) if af.func.name().eq_ignore_ascii_case("first_value"))
}

/// The column argument of a `first_value(col)` aggregate, if any.
fn first_value_inner(e: &Expr) -> Option<&Expr> {
    match e {
        Expr::AggregateFunction(af) if af.func.name().eq_ignore_ascii_case("first_value") => {
            af.params.args.first()
        }
        _ => None,
    }
}

/// Strip relation qualifiers from every column reference in every node of a plan (the
/// streaming counterpart to per-operator stripping). Bottom-up so each node is rebuilt
/// against its already-normalized children. Safe given globally-unique `#N` column ids.
fn strip_plan_qualifiers(plan: LogicalPlan) -> Result<LogicalPlan> {
    use datafusion_common::tree_node::TreeNode;
    plan.transform_up(|p| {
        // Aggregate is special: stripping the relation qualifier from a column INSIDE an aggregate
        // argument (e.g. `first_value(?table?.#0)`) re-derives the aggregate's auto-generated output
        // field name (→ `first_value(#0)`), which silently breaks a parent projection's column
        // reference (still the literal string `first_value(?table?.#0)`) — the
        // `No field named "first_value(?table?.#0)"` error on `dropDuplicates`/aggregations over a
        // qualified column. So we strip only the GROUP exprs here (their names are columns, stable
        // under stripping) and leave `aggr_expr` untouched to keep the output field names stable.
        // The streaming rewriter's window/dedup handlers strip aggregate-arg qualifiers locally where
        // they actually build the physical operators (see the `Aggregate` arm above), so unqualified
        // columns still reach the streaming operators.
        if let LogicalPlan::Aggregate(agg) = &p {
            let group_expr: Vec<Expr> = agg.group_expr.iter().map(strip_qualifiers).collect();
            let input = Arc::new(agg.input.as_ref().clone());
            return Ok(Transformed::yes(LogicalPlan::Aggregate(Aggregate::try_new(
                input,
                group_expr,
                agg.aggr_expr.clone(),
            )?)));
        }
        let exprs: Vec<Expr> = p.expressions().iter().map(strip_qualifiers).collect();
        let inputs: Vec<LogicalPlan> = p.inputs().into_iter().cloned().collect();
        Ok(Transformed::yes(p.with_new_exprs(exprs, inputs)?))
    })
    .map(|t| t.data)
}

fn strip_qualifiers(e: &Expr) -> Expr {
    e.clone()
        .transform(|n| {
            Ok::<_, datafusion_common::DataFusionError>(match n {
                Expr::Column(c) => Transformed::yes(Expr::Column(datafusion_common::Column::new(
                    None::<datafusion_common::TableReference>,
                    c.name,
                ))),
                other => Transformed::no(other),
            })
        })
        .map(|t| t.data)
        .unwrap_or_else(|_| e.clone())
}

/// Flatten a conjunction (`a AND b AND c`) into its conjuncts.
fn collect_conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryExpr(be) = e {
        if be.op == datafusion_expr::Operator::And {
            collect_conjuncts(&be.left, out);
            collect_conjuncts(&be.right, out);
            return;
        }
    }
    out.push(e);
}

/// Parse `col` or `col +/- <duration literal>` → `(col_name, offset_micros)`.
fn parse_col_offset(e: &Expr) -> Option<(String, i64)> {
    match e {
        Expr::Column(c) => Some((c.name.clone(), 0)),
        Expr::BinaryExpr(be) => {
            let Expr::Column(c) = be.left.as_ref() else { return None };
            let micros = duration_literal_micros(be.right.as_ref())?;
            match be.op {
                datafusion_expr::Operator::Plus => Some((c.name.clone(), micros)),
                datafusion_expr::Operator::Minus => Some((c.name.clone(), -micros)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Convert a duration/interval literal to microseconds.
fn duration_literal_micros(e: &Expr) -> Option<i64> {
    use datafusion_common::ScalarValue;
    let Expr::Literal(sv, _) = e else { return None };
    match sv {
        ScalarValue::DurationMicrosecond(Some(v)) => Some(*v),
        ScalarValue::DurationMillisecond(Some(v)) => Some(v.checked_mul(1_000)?),
        ScalarValue::DurationSecond(Some(v)) => Some(v.checked_mul(1_000_000)?),
        ScalarValue::DurationNanosecond(Some(v)) => Some(v / 1_000),
        ScalarValue::IntervalDayTime(Some(i)) => {
            Some((i.days as i64).checked_mul(86_400_000_000)? + (i.milliseconds as i64) * 1_000)
        }
        _ => None,
    }
}

/// Searches the plan subtree for a `WatermarkNode` and returns `(event_time_col, delay_micros)`.
fn find_watermark_info(plan: &LogicalPlan) -> Option<(String, i64)> {
    use datafusion_common::tree_node::TreeNodeRecursion;
    let mut result: Option<(String, i64)> = None;
    let _ = plan.apply(|p| {
        if let LogicalPlan::Extension(ext) = p {
            if let Some(wm) = ext.node.as_any().downcast_ref::<WatermarkNode>() {
                result = Some((wm.event_time_col.clone(), wm.delay_micros));
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    result
}
