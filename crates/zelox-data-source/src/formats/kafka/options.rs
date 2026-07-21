use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};

/// DataFusion's default execution batch size — a good throughput/latency balance
/// and far below any i32-offset risk.
pub const DEFAULT_ARROW_BATCH_ROWS: usize = 8192;
/// Hard ceiling on rows per Arrow RecordBatch flushed by the Kafka source. Keeps
/// a single batch's variable-length (Utf8/Binary) columns safely under Arrow's
/// i32 `OffsetBuffer` limit (2 GiB) even with multi-KB values: 262144 rows ×
/// ~8 KiB ≈ 2 GiB, so realistic payloads stay well clear.
pub const MAX_ARROW_BATCH_ROWS: usize = 262_144;

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
    /// Maximum rows collected into a single **Arrow RecordBatch** before it is
    /// flushed downstream. This is an internal execution-buffer size — NOT the
    /// per-trigger admission limit (see `max_offsets_per_trigger`). It is clamped
    /// to `MAX_ARROW_BATCH_ROWS` so a single batch's variable-length columns can
    /// never exceed Arrow's i32 `OffsetBuffer` limit (2 GiB). DataFusion's default
    /// execution batch size is 8192; we match that.
    pub max_batch_size: usize,
    /// Spark `maxOffsetsPerTrigger`: the maximum number of Kafka offsets admitted
    /// per micro-batch (rate/admission control). Distinct from `max_batch_size`
    /// (the Arrow buffer size) — conflating them is what caused giant 20M-row
    /// batches to overflow `from_json`'s i32 string offsets. `None` = no limit.
    pub max_offsets_per_trigger: Option<usize>,
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
        let mut group_id = Self::generate_group_id("zelox");
        let mut max_batch_size: usize = DEFAULT_ARROW_BATCH_ROWS;
        let mut max_offsets_per_trigger: Option<usize> = None;
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
                "maxbatchsize" => {
                    // Arrow execution-buffer size; clamp so a single batch can never
                    // overflow Arrow's i32 string/binary OffsetBuffer (2 GiB).
                    let n = value
                        .parse::<usize>()
                        .map_err(|e| DataFusionError::Plan(format!("invalid {key}: {e}")))?;
                    max_batch_size = n.clamp(1, MAX_ARROW_BATCH_ROWS);
                }
                "maxoffsetspertrigger" | "maxoffsetspermicrobatch" => {
                    // Spark admission limit (offsets per micro-batch) — NOT the Arrow
                    // buffer size. Does not affect per-batch memory / offset width.
                    max_offsets_per_trigger = Some(
                        value
                            .parse::<usize>()
                            .map_err(|e| DataFusionError::Plan(format!("invalid {key}: {e}")))?,
                    );
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
            max_offsets_per_trigger,
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
