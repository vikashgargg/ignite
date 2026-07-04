-- Flink CORRECTNESS variant of flink-sql.sql: identical 10s tumbling keyed COUNT, but the aggregation
-- result is written to a Kafka topic `wagg_out` (JSON) so the harness can consume it and verify the SAME
-- correctness Vajra reports (distinct (window,k) groups + sum(count)). Bounded read + dml-sync so the job
-- terminates once the backlog is fully aggregated. This makes the Vajra-vs-Flink comparison BOTH-correct.

SET 'execution.runtime-mode' = 'streaming';
SET 'table.dml-sync' = 'true';
SET 'parallelism.default' = '16';
SET 'pipeline.object-reuse' = 'true';

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
  'properties.group.id' = 'flink-wagg-verify',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
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

-- GROUP BY window_start, window_end (both, from the TVF) makes this an APPEND-ONLY window aggregation
-- (each window fires once) rather than a streaming GroupAggregate that emits retractions — so the plain
-- kafka sink accepts it.
INSERT INTO wagg_out
SELECT window_start, k, COUNT(*)
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, k;
