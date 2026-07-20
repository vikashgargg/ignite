-- Flink REALTIME (UNBOUNDED streaming) side of the Vajra-Trigger.RealTime head-to-head.
-- IDENTICAL logical query to scripts/stream_realtime_drain.py (Vajra): 10s event-time TUMBLE window,
-- GROUP BY window + k, COUNT(*). NO scan.bounded.mode => TRUE continuous streaming (Flink's core
-- advantage, the mode we want to beat), submitted async (dml-sync=false); the harness measures the
-- catch-up DRAIN of the pre-loaded backlog via the source consumer-group lag, and consumes wagg_out for
-- completeness. Mini-batch ON = Flink's best realtime throughput. Checkpointing ON = per-checkpoint EO +
-- KafkaSource offset commit (so the drain poll can read group `flink-wagg` progress).

SET 'execution.runtime-mode' = 'streaming';
SET 'table.dml-sync' = 'false';
SET 'execution.checkpointing.interval' = '5s';
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
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE wagg_out (
  window_start TIMESTAMP(3),
  k INT,
  cnt BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'wagg_out',
  'properties.bootstrap.servers' = 'kafka.stream.svc.cluster.local:9092',
  'format' = 'json'
);

-- GROUP BY window_start, window_end (both) = append-only window aggregation (each window fires once),
-- accepted by the plain kafka sink.
INSERT INTO wagg_out
SELECT window_start, k, COUNT(*)
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, k;
