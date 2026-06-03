use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion_expr::LogicalPlan;

mod lateral_join;
pub use lateral_join::DecorrelateLateralProjection;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let Analyzer {
        function_rewrites: _,
        rules: built_in_rules,
    } = Analyzer::default();

    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![];
    rules.extend(built_in_rules);
    rules
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let Optimizer { rules } = Optimizer::default();
    // Custom rules are prepended so they run before DataFusion's built-in rules.
    // `DecorrelateLateralProjection` must run before `DecorrelateLateralJoin`
    // because it handles the simple case where OuterRef only appears in
    // Projection expressions (e.g. `LATERAL (SELECT t1.a + 1)`), rewriting
    // it into a CrossJoin + Projection. The remaining complex cases (OuterRef
    // in Filter/Aggregate) are left for DataFusion's `DecorrelateLateralJoin`.
    let mut custom: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(DecorrelateLateralProjection::new())];
    // Replace DataFusion's `optimize_projections` with a safe version that
    // skips plans containing RecursiveQuery nodes.  DataFusion 53's rule
    // incorrectly reduces RecursiveQuery children to zero columns when the
    // parent (e.g. COUNT(*)) does not reference any specific column, which
    // causes "Unexpected empty work table" at execution time.
    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = rules
        .into_iter()
        .map(|rule| -> Arc<dyn OptimizerRule + Send + Sync> {
            if rule.name() == "optimize_projections" {
                Arc::new(SafeOptimizeProjections { inner: rule })
            } else {
                rule
            }
        })
        .collect();
    custom.extend(rules);
    custom
}

/// Wrapper around DataFusion's `optimize_projections` rule that skips plans
/// containing `RecursiveQuery` nodes to prevent incorrect schema reduction.
#[derive(Debug)]
struct SafeOptimizeProjections {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
}

impl OptimizerRule for SafeOptimizeProjections {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        self.inner.apply_order()
    }

    #[expect(deprecated)]
    fn supports_rewrite(&self) -> bool {
        self.inner.supports_rewrite()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if plan_has_recursive_query(&plan) {
            return Ok(Transformed::no(plan));
        }
        self.inner.rewrite(plan, config)
    }
}

fn plan_has_recursive_query(plan: &LogicalPlan) -> bool {
    let mut found = false;
    // The visitor closure is infallible (always returns Ok), so the traversal
    // cannot error; `found` is set by side effect. Discard the Result rather
    // than expect() (workspace denies expect_used).
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::RecursiveQuery(_)) {
            found = true;
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop)
        } else {
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        }
    });
    found
}
