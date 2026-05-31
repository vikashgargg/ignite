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
use sail_logical_plan::streaming::watermark::WatermarkNode;
use sail_logical_plan::streaming::window_accum::WindowAccumNode;

/// A logical plan rewriter that rewrites a batch logical plan
/// into a streaming logical plan. All the nodes (except the sink) in the plan
/// will have a flow event schema which contains additional fields
/// along with the original data fields.
struct StreamingRewriter;

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
        } else if node.as_any().is::<FileWriteNode>() {
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else if node.as_any().is::<ForeachBatchSinkNode>() {
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else if node.as_any().is::<MemorySinkNode>() {
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else if node.as_any().is::<BarrierNode>() {
            // TODO: support BarrierNode for streaming properly.
            Ok(Transformed::no(LogicalPlan::Extension(extension)))
        } else if node.as_any().is::<WatermarkNode>() {
            // WatermarkNode is a transparent passthrough; it is consumed by the
            // parent Aggregate handler for event-time window detection.
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
                let watermark_info = if is_window_group {
                    find_watermark_info(&streaming_input)
                } else {
                    None
                };

                if let Some((event_time_col, delay_micros)) = watermark_info {
                    // Stateful event-time window aggregation via WindowAccumNode.
                    // The physical planner will map this to WindowAccumExec.
                    let data_schema_ref =
                        Arc::new(datafusion_common::DFSchema::try_from(data_schema.clone())?);
                    // Compute what the aggregate output schema would be.
                    let agg_output_schema = {
                        let trial = Aggregate::try_new(
                            Arc::new(data_only.clone()),
                            agg.group_expr.clone(),
                            agg.aggr_expr.clone(),
                        )?;
                        trial.schema.clone()
                    };
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(WindowAccumNode::new(
                            data_only,
                            agg.group_expr.clone(),
                            agg.aggr_expr.clone(),
                            event_time_col,
                            delay_micros,
                            Arc::new(datafusion_common::DFSchema::try_from(
                                std::sync::Arc::new(
                                    sail_common_datafusion::streaming::event::schema::to_flow_event_schema(
                                        &agg_output_schema.as_arrow()
                                    )
                                )
                            )?),
                            agg_output_schema,
                        )),
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
                // Per-micro-batch join: strip flow-event columns from both sides,
                // run the original join on data-only schemas, then re-add flow-event
                // columns. This handles stream × static and stream × stream joins.
                // State-based windowed joins (stream × stream with watermark) are not
                // yet supported and will produce per-batch results instead.
                let left_data_schema = try_from_flow_event_schema(join.left.schema().inner())?;
                let right_data_schema = try_from_flow_event_schema(join.right.schema().inner())?;

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
                        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                            node: Arc::new(StreamSourceWrapperNode::try_new(
                                table_name.clone(),
                                source,
                                names,
                                projection.clone(),
                                filters.clone(),
                                *fetch,
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
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamDeduplicateNode::new(input, key_cols)),
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
pub fn rewrite_streaming_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    let node = plan.rewrite(&mut StreamingRewriter)?;
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
