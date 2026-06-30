-- Flink side of the Vajra-vs-Flink LATENCY head-to-head (dimension S3).
-- IDENTICAL work to scripts/stream_latency_query.py (Vajra): Kafka lat_in -> raw value
-- passthrough -> Kafka lat_out, CONTINUOUS streaming (unbounded). The producer embeds a
-- wall-clock produce_ts in each value; the shared latency consumer computes now - produce_ts
-- on lat_out and reports p50/p99/p99.9/max. `format=raw` passes the whole value through
-- unchanged (no parse/transform) so the number is the engine's produce->output pipeline
-- latency (where no-JVM/no-GC should win on the TAIL), not query compute.
--
-- Continuous job: the orchestrator starts it async, runs produce+measure for DURATION_S,
-- then cancels the Flink job. (No scan.bounded.mode, no dml-sync.)

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '16';
SET 'pipeline.object-reuse' = 'true';

CREATE TABLE lat_in (
  v STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'lat_in',
  'properties.bootstrap.servers' = 'kafka.stream.svc.cluster.local:9092',
  'properties.group.id' = 'flink-lat',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'raw'
);

CREATE TABLE lat_out (
  v STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'lat_out',
  'properties.bootstrap.servers' = 'kafka.stream.svc.cluster.local:9092',
  'format' = 'raw'
);

INSERT INTO lat_out SELECT v FROM lat_in;
