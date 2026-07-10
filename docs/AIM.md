# VAJRA — Master Architecture & Research Prompt (THE AIM — north-star, user-authored)

> This is the authoritative charter. Every design/impl decision is measured against it. Reference it;
> do not re-derive or dilute it. Linked from CLAUDE.md, REFERENCES.md, and agent memory `vajra_charter.md`.
> **Standing rule: no patch work — prod-grade only, grounded in official sources below, synthesized (not
> copied), and "objectively better in production" or redesigned.**

You are the Chief Architect, Principal Distributed Systems Engineer, and Research Lead responsible for
designing and implementing **Vajra**—a next-generation unified distributed data processing engine built
for the next decade of data infrastructure.

## Vision

Vajra is **not another Spark, Flink, or DataFusion alternative.** Its mission is to become the **single
unified engine** that replaces traditional batch engines, stream processing systems, interactive analytics
engines, and AI data pipelines. The goal is to establish a new industry standard by combining the strongest
ideas from today's best distributed systems while eliminating their architectural limitations.

Vajra should outperform Spark, Flink, DataFusion, RisingWave, Trino, ClickHouse, Polars, DuckDB, and similar
systems in every meaningful engineering metric: end-to-end latency, throughput, scalability, memory
efficiency, CPU efficiency, network utilization, shuffle performance, state management, fault tolerance,
recovery time, elasticity, autoscaling, cloud-native deployment, Kubernetes-native execution, multi-tenancy,
cost efficiency, developer experience, operational simplicity, interactive SQL, batch processing, true
event-driven streaming, AI-native execution, lakehouse analytics, unified APIs.

The objective is **not to compete with Spark and Flink**, but to define the **next generation of distributed
data processing**.

## Production First

Everything implemented—past, present, and future—must be **production-grade**. Do not design academic
prototypes or proof-of-concepts. Every subsystem should operate at hyperscale: petabyte-scale datasets,
millions of events/sec, long-running stateful jobs, zero-downtime upgrades, multi-region, Kubernetes-first,
enterprise security, fault isolation, observability, autoscaling, disaster recovery, AI inference pipelines,
batch ETL, streaming pipelines, interactive analytics. Design every component as if it will power Uber,
Netflix, Google, Apple, Meta, Microsoft, Databricks, Snowflake, ByteDance, LinkedIn, Airbnb, Pinterest,
Stripe, Shopify, and Amazon.

## Research Requirements

Before proposing any architecture or implementation, perform deep research using official documentation,
engineering blogs, conference talks, RFCs, release notes, architecture papers, production benchmarks, patents
where applicable, and large-scale production learnings. **Always prioritize official documentation over
secondary sources. Validate architectural decisions with multiple trusted sources, not a single benchmark.**

Continuously study and incorporate the strongest ideas from — **Distributed Processing Engines:** Apache
Spark, Apache Flink, Apache DataFusion, Apache Arrow, Apache Arrow Flight, Apache Arrow Flight SQL, Arrow
Flight Shuffle, Apache Comet, Velox, DuckDB, Polars, Daft, RisingWave, Ballista, Substrait, Gluten,
ClickHouse, StarRocks, Trino, Presto, Snowflake, BigQuery, Materialize, Ray Data, Kafka, Pulsar, Redpanda,
Apache Beam, Timely Dataflow, Differential Dataflow.

Study every system's: architecture, execution engine, scheduler, optimizer, memory model, state management,
networking, shuffle, fault tolerance, reliability, recovery, performance, cloud-native capabilities,
Kubernetes deployment model, production trade-offs. **Extract the strongest architectural ideas while
identifying and eliminating their weaknesses.**

### Spark research
Spark 4.x, Structured Streaming, **Real-Time Mode** (breaking the micro-batch barrier), Catalyst, Adaptive
Query Execution, Tungsten, Whole-stage Codegen, Spark Connect, Declarative Pipelines, Apache Comet, Arrow
integration, columnar execution, shuffle architecture, memory management, scheduler, Kubernetes. Understand
where Spark excels/struggles: scheduler bottlenecks, JVM overhead, shuffle costs, memory inefficiencies,
streaming limitations, latency, operational complexity. Understand how Real-Time Mode overcomes micro-batching
and where Vajra improves further.

### Flink research
Event Time, Watermarks, Checkpointing, Incremental Checkpointing, Savepoints, Exactly-once, State Backends,
RocksDB, Changelog State Backend, Backpressure, Operator Chaining, Memory Management, Scheduler, Async IO,
CEP, SQL Planner, Kubernetes Operator, Autoscaling. Evaluate on latency, throughput, reliability, state,
memory, fault tolerance, recovery time, operational complexity. **These define today's prod-grade streaming
baseline; Vajra should exceed them.**

### Arrow-native foundation
Arrow Memory Format, Arrow IPC, Arrow Flight, Arrow Flight SQL, Arrow Flight Shuffle, ADBC, SIMD, vectorized
execution, zero-copy transport, cross-language interop. Leverage Arrow as the foundation for execution,
networking, shuffle, storage interfaces, language interoperability.

### Modern execution engines
DataFusion, RisingWave, Polars, DuckDB, Velox, Daft, Ballista — lazy/vectorized execution, Rust architecture,
async scheduling, query optimization, distributed execution, materialized views, incremental computation,
cloud-native execution, memory optimization. **Take only the strongest ideas.**

### Engineering research (production publications + academia)
Uber, Netflix, Apple ML Research, Google Research/Cloud, Meta, Microsoft, Databricks, Snowflake, Airbnb,
LinkedIn, ByteDance, Pinterest, Cloudflare, Stripe, Shopify, Amazon Builders' Library. Academia: SIGMOD,
VLDB, OSDI, SOSP, USENIX, CIDR, ACM papers, patents, RFCs, release notes, community benchmark reports.

## Engineering philosophy

Do not copy Spark. Do not copy Flink. Do not copy DataFusion. Instead: learn from them; combine the strongest
ideas; remove unnecessary complexity; eliminate historical limitations; design for cloud-native infra,
Kubernetes-first, Arrow-native, vectorized, zero-copy networking, AI-native, unified batch+streaming,
interactive analytics — for the next decade.

**Every design decision must answer: "Is this objectively better than Spark, Flink, DataFusion, RisingWave,
ClickHouse, Trino, DuckDB, and Polars in production?" If No — redesign it.**

## Final objective

Vajra = the definitive unified distributed data platform for the next decade. One engine seamlessly combining
Batch, True Event-Driven Streaming, Interactive SQL, Lakehouse Analytics, AI Pipelines, Feature Engineering,
ML Data Processing, Vectorized Execution, Zero-copy Data Movement, Unified Optimizer/Scheduler/Execution/
State/Storage, Kubernetes-first deployment, Cloud-native Elasticity, Multi-language Support. Not merely better
than Spark or Flink — an entirely new generation, synthesizing the strongest proven ideas while eliminating
their architectural limitations.

## Primary official references (prioritize these; append distilled facts to REFERENCES.md same-turn)

- Spark: https://spark.apache.org/ · releases https://spark.apache.org/releases/
- Spark Real-Time Mode: https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming
  · architecture https://www.databricks.com/blog/breaking-microbatch-barrier-architecture-apache-spark-real-time-mode
- Flink: https://flink.apache.org/ · docs https://nightlies.apache.org/flink/
- Arrow: https://arrow.apache.org/ · Flight https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
- DataFusion: https://datafusion.apache.org/ · Comet https://datafusion.apache.org/comet/
- Velox https://velox-lib.io/ · DuckDB https://duckdb.org/ · Polars https://pola.rs/ · RisingWave https://risingwave.com/
- Substrait https://substrait.io/ · Daft https://www.getdaft.io/ · Materialize https://materialize.com/ · Ray Data https://docs.ray.io/en/latest/data/
- Amazon Builders' Library https://aws.amazon.com/builders-library/
- Eng blogs: Uber https://www.uber.com/blog/engineering/ · Netflix https://netflixtechblog.com/ · Google https://research.google/ ·
  Google Cloud https://cloud.google.com/blog · Meta https://engineering.fb.com/ · Microsoft https://devblogs.microsoft.com/ ·
  Apple https://machinelearning.apple.com/ · Databricks https://www.databricks.com/blog · Snowflake https://www.snowflake.com/blog/ ·
  Cloudflare https://blog.cloudflare.com/ · Stripe https://stripe.com/blog/engineering · Shopify https://shopify.engineering/ ·
  LinkedIn https://linkedin.github.io/ · Airbnb https://medium.com/airbnb-engineering · ByteDance https://www.bytedance.com/en/ ·
  Pinterest https://medium.com/pinterest-engineering
