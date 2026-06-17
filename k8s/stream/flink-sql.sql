-- Flink side of the Vajra-vs-Flink streaming head-to-head.
-- IDENTICAL logical query to scripts/stream_windowed_agg.py (Vajra):
--   10s event-time TUMBLE window, GROUP BY window + k, COUNT(*).
-- Bounded read (scan.bounded.mode = latest-offset) so Flink consumes the whole
-- pre-loaded backlog and the job terminates -> wall time = catch-up throughput,
-- directly comparable to Flink's published windowed-aggregation events/s.

SET 'execution.runtime-mode' = 'streaming';
SET 'table.dml-sync' = 'true';
SET 'parallelism.default' = '16';
SET 'pipeline.object-reuse' = 'true';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2s';
SET 'table.exec.mini-batch.size' = '50000';

CREATE TABLE events (
  k INT,
  ts BIGINT,
  v INT,
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'kafka.stream.svc.cluster.local:9092',
  'properties.group.id' = 'flink-wagg',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- blackhole: the aggregation result is tiny (windows x keys) vs 100M input
-- events, so sink format is negligible; this isolates pure windowed-agg compute
-- (the standard Flink throughput-benchmark sink).
CREATE TABLE sink (
  window_start TIMESTAMP(3),
  k INT,
  cnt BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO sink
SELECT window_start, k, COUNT(*)
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, k;
