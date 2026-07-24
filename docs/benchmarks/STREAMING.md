# Zelox streaming — throughput (first data point)

A real, reproducible micro-benchmark of Zelox's Structured Streaming micro-batch
engine, measured end-to-end through the live Spark Connect path (2026-06-09).

## What it measures
A `rate` source under `.trigger(availableNow=True)` emits a bounded batch of *N*
rows, a streaming aggregation (`groupBy().count()`, complete mode) consumes them,
and the query terminates. We time `start()` → `awaitTermination()`. Because the
output is a single aggregated row, the wall-time reflects the **engine's**
source-generation + flow-event + aggregation throughput, not sink I/O.

## Result (single node, 8 cores, **debug build**)

| Rows (availableNow batch) | Wall time | Throughput |
|---|--:|--:|
| 100,000 | 0.013 s | 7.8 M rows/s |
| 1,000,000 | 0.036 s | 27.5 M rows/s |
| 5,000,000 | 0.175 s | **28.6 M rows/s** |

Throughput climbs as fixed per-query overhead amortizes, leveling at **~28 M
rows/s** for this workload. This is a **debug** build on a laptop; a release build
(LTO) and more cores would be materially higher.

## Reproduce
```bash
cargo build -p zelox-cli --bin zelox
RUST_LOG=error ./target/debug/zelox server --port 50099 &
# pyspark[connect]==3.5.3 client:
python - <<'PY'
from pyspark.sql import SparkSession
import time
s = SparkSession.builder.remote("sc://localhost:50099").getOrCreate()
for n in (100_000, 1_000_000, 5_000_000):
    df = s.readStream.format("rate").option("rowsPerSecond", str(n)).load()
    q = (df.groupBy().count().writeStream.format("console")
         .outputMode("complete").trigger(availableNow=True).start())
    t = time.time(); q.awaitTermination(120); el = time.time() - t
    print(f"{n} rows: {el:.3f}s -> {n/el/1e6:.2f}M rows/s")
PY
```

## Honest caveats / what's next (to be "on par with Flink")
- **Debug build, single node, one aggregation.** Not yet a standardized streaming
  benchmark.
- A credible Flink/Spark-Streaming comparison needs: a **release build on a real
  cluster**, an **unthrottled high-volume source**, a **measurable sink**, and a
  **standard workload** — e.g. the **Nexmark** streaming suite or the Yahoo
  Streaming Benchmark — plus **sustained** (not bounded-once) throughput and
  **end-to-end latency** percentiles.
- Streaming **progress metrics** (processedRowsPerSecond, input rate) are not yet
  reported by Zelox; wiring those up is a prerequisite for standard streaming
  benchmarks and is tracked in [../STREAMING.md](../STREAMING.md).
- Local **memory** and **file** streaming sinks have gaps in single-node mode
  (catalog/listing-table resolution); the **console** sink is the verified path.
  Hardening sinks is part of the streaming roadmap.

This establishes streaming throughput is in the **tens of millions of rows/sec**
range even unoptimized — the foundation for a Flink-class story once the standard
harness + release/cluster setup land.
