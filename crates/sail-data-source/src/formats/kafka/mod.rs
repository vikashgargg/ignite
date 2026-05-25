mod options;
mod reader;

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::source::StreamSourceTableProvider;

pub use crate::formats::kafka::options::KafkaReadOptions;
pub use crate::formats::kafka::reader::KafkaSourceExec;
use crate::formats::kafka::reader::KafkaStreamSource;

/// Kafka streaming source — reads from one or more Kafka topics and emits a
/// Spark-compatible record schema: key, value, topic, partition, offset,
/// timestamp, timestampType.
#[derive(Debug)]
pub struct KafkaTableFormat;

#[async_trait]
impl TableFormat for KafkaTableFormat {
    fn name(&self) -> &str {
        "kafka"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let SourceInfo {
            paths: _,
            schema: _,
            constraints,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;
        if !constraints.deref().is_empty() {
            return plan_err!("the kafka table format does not support constraints");
        }
        if !partition_by.is_empty() {
            return plan_err!("the kafka table format does not support partitioning");
        }
        if bucket_by.is_some() || !sort_order.is_empty() {
            return plan_err!("the kafka table format does not support bucketing");
        }

        let flat_options: Vec<(String, String)> = options
            .into_iter()
            .flat_map(|layer| layer.into_opaque_options().into_iter())
            .collect();
        let opts = KafkaReadOptions::from_options(flat_options)?;
        let source = KafkaStreamSource::new(opts);
        Ok(provider_as_source(Arc::new(
            StreamSourceTableProvider::new(Arc::new(source)),
        )))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("kafka table format writer (use foreachBatch or Kafka sink for writes)")
    }
}
