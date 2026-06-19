mod options;
mod reader;
mod sink;

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use sail_common_datafusion::datasource::{PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_common_datafusion::streaming::source::StreamSourceTableProvider;

pub use crate::formats::kafka::options::KafkaReadOptions;
pub use crate::formats::kafka::reader::KafkaSourceExec;
use crate::formats::kafka::reader::KafkaStreamSource;
pub use crate::formats::kafka::sink::KafkaSinkExec;

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
            is_streaming: _,
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
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            logical_schema: _,
            declared_schema: _,
        } = info;
        // The Kafka sink writes a streaming (flow-event) input row-by-row to Kafka.
        if !is_flow_event_schema(&input.schema()) {
            return plan_err!("the kafka sink only supports streaming writes (writeStream)");
        }
        if !matches!(mode, PhysicalSinkMode::Append) {
            return not_impl_err!("the kafka sink only supports append mode");
        }
        if !partition_by.is_empty() {
            return plan_err!("the kafka sink does not support partitioning");
        }
        if bucket_by.is_some() || sort_order.is_some() {
            return plan_err!("the kafka sink does not support bucketing/sorting");
        }
        // Flatten option layers (later layers override earlier).
        let flat: HashMap<String, String> = options
            .into_iter()
            .flat_map(|layer| layer.into_opaque_options().into_iter())
            .collect();
        let mut bootstrap_servers = String::new();
        let mut topic = String::new();
        let mut value_col: Option<String> = None;
        let mut key_col: Option<String> = None;
        let mut extra: HashMap<String, String> = HashMap::new();
        for (k, v) in flat {
            match k.to_lowercase().as_str() {
                "kafka.bootstrap.servers" | "bootstrap.servers" | "bootstrapservers" => {
                    bootstrap_servers = v;
                }
                "topic" => topic = v,
                "value" | "valuecolumn" | "value.column" => value_col = Some(v),
                "key" | "keycolumn" | "key.column" => key_col = Some(v),
                // Reserved streaming/sink options the Kafka producer must not see.
                "checkpointlocation" | "path" => {}
                lk if lk.starts_with("kafka.") => {
                    extra.insert(lk[6..].to_string(), v);
                }
                _ => {}
            }
        }
        Ok(Arc::new(KafkaSinkExec::try_new(
            input,
            bootstrap_servers,
            topic,
            value_col,
            key_col,
            extra,
        )?))
    }
}
