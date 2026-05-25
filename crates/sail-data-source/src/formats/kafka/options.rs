use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};

#[derive(Debug, Clone)]
pub struct KafkaReadOptions {
    /// Kafka bootstrap servers, e.g. "localhost:9092".
    pub bootstrap_servers: String,
    /// Comma-separated list of topics to subscribe to.
    pub subscribe: Option<String>,
    /// Regex pattern for topic subscription (mutually exclusive with `subscribe`).
    pub subscribe_pattern: Option<String>,
    /// Starting offsets: "earliest" or "latest" (default: "latest").
    pub starting_offsets: String,
    /// Consumer group ID (auto-generated if not supplied).
    pub group_id: String,
    /// Maximum number of records collected into a single micro-batch.
    pub max_batch_size: usize,
    /// How long to wait (ms) for a batch to fill before flushing a partial batch.
    pub fetch_timeout_ms: u64,
    /// Extra rdkafka options: keys have the "kafka." prefix already stripped.
    pub extra: HashMap<String, String>,
}

impl KafkaReadOptions {
    pub fn from_options(options: Vec<(String, String)>) -> Result<Self> {
        let mut bootstrap_servers = String::new();
        let mut subscribe = None;
        let mut subscribe_pattern = None;
        let mut starting_offsets = "latest".to_string();
        let mut group_id = Self::generate_group_id("vajra");
        let mut max_batch_size: usize = 1000;
        let mut fetch_timeout_ms: u64 = 500;
        let mut extra = HashMap::new();

        for (key, value) in options {
            match key.to_lowercase().as_str() {
                "kafka.bootstrap.servers" | "bootstrapservers" | "bootstrap.servers" => {
                    bootstrap_servers = value;
                }
                "subscribe" => {
                    subscribe = Some(value);
                }
                "subscribepattern" => {
                    subscribe_pattern = Some(value);
                }
                "startingoffsets" | "startingoffset" => {
                    starting_offsets = value;
                }
                "group.id" => {
                    group_id = value;
                }
                "groupidprefix" => {
                    group_id = Self::generate_group_id(&value);
                }
                "maxbatchsize" | "maxoffsetspertrigger" | "maxoffsetspermicrobatch" => {
                    max_batch_size = value.parse::<usize>().map_err(|e| {
                        DataFusionError::Plan(format!("invalid {key}: {e}"))
                    })?;
                }
                "fetchtimeoutms" => {
                    fetch_timeout_ms = value.parse::<u64>().map_err(|e| {
                        DataFusionError::Plan(format!("invalid fetchtimeoutms: {e}"))
                    })?;
                }
                k if k.starts_with("kafka.") => {
                    extra.insert(k[6..].to_string(), value);
                }
                _ => {}
            }
        }

        if bootstrap_servers.is_empty() {
            return Err(DataFusionError::Plan(
                "kafka.bootstrap.servers is required for the Kafka source".to_string(),
            ));
        }
        if subscribe.is_none() && subscribe_pattern.is_none() {
            return Err(DataFusionError::Plan(
                "either 'subscribe' or 'subscribePattern' is required for the Kafka source"
                    .to_string(),
            ));
        }

        Ok(Self {
            bootstrap_servers,
            subscribe,
            subscribe_pattern,
            starting_offsets,
            group_id,
            max_batch_size,
            fetch_timeout_ms,
            extra,
        })
    }

    fn generate_group_id(prefix: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{prefix}-{ms:010}")
    }
}
