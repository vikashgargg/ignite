use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{not_impl_err, Result};
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use zelox_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat, TableFormatRegistry};

/// Vortex columnar file format support.
///
/// Actual read/write is deferred until `vortex-datafusion` crate confirms
/// DataFusion 53.x compatibility. Until then, both operations return
/// `not_impl_err!` so that Vortex table references produce a clear error
/// rather than a silent panic.
#[derive(Debug, Default)]
pub struct VortexTableFormat;

impl VortexTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))
    }
}

#[async_trait]
impl TableFormat for VortexTableFormat {
    fn name(&self) -> &str {
        "vortex"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Vortex read is not yet implemented (pending vortex-datafusion 53.x compat)")
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Vortex write is not yet implemented (pending vortex-datafusion 53.x compat)")
    }
}
