-- Flink realtime windowed COUNT -> S3 (MinIO) parquet. IDENTICAL logical query to Vajra's
-- stream_realtime_drain.py (10s event-time TUMBLE, GROUP BY window+k). Validated on the box: 10 windows,
-- 10000 rows, sum=100,000,000 on MinIO, byte-identical aggregates to Vajra.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '5s';
SET 'parallelism.default' = '8';
-- UTC so window_start renders IDENTICALLY to Vajra's UTC window (else TO_TIMESTAMP_LTZ uses the box TZ
-- and the per-(window,k) data-correctness diff sees a label offset, not a value mismatch).
SET 'table.local-time-zone' = 'UTC';
CREATE TABLE events (
  k INT, ts BIGINT, v INT,
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND
) WITH ('connector'='kafka','topic'='events',
  'properties.bootstrap.servers'='kafka.stream.svc.cluster.local:9092',
  'properties.group.id'='flink-s3','scan.startup.mode'='earliest-offset',
  'format'='json','json.ignore-parse-errors'='true');
CREATE TABLE flink_out (window_start TIMESTAMP(3), k INT, cnt BIGINT)
  WITH ('connector'='filesystem','path'='s3a://vajra/flink_out','format'='parquet');
INSERT INTO flink_out
SELECT window_start, k, COUNT(*)
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
WHERE k >= 0
GROUP BY window_start, window_end, k;
