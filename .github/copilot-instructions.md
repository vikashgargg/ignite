Sail is a unified and distributed multimodal computation framework.
It is a drop-in replacement for Apache Spark via the Spark Connect protocol.
It aims to unify batch, streaming, and AI workloads, offering high performance and low infrastructure costs.
It is written in Rust and Python, and is built using technologies such as Apache Arrow, Apache DataFusion, Tokio, and PyO3.

## Project Layout

- `crates/`: All Rust crates.
  - `zelox-build-scripts`: Rust code generation logic to be used in `build.rs`.
  - `zelox-cache`: Caching implementations.
  - `zelox-catalog`: Catalog interface and common utilities.
  - `zelox-catalog-*`: Catalog implementations.
  - `zelox-cli`: Command-line interface entry point.
  - `zelox-common`: Sail configuration, query plan specification, and utilities.
  - `zelox-common-datafusion`: DataFusion utilities.
  - `zelox-data-source`: Data source implementations.
  - `zelox-delta-lake`: Delta Lake integration.
  - `zelox-execution`: Distributed execution implementation.
  - `zelox-flight`: Arrow Flight SQL server implementation.
  - `zelox-function`: Scalar and aggregate functions.
  - `zelox-gold-test`: SQL gold tests.
  - `zelox-iceberg`: Apache Iceberg integration.
  - `zelox-logical-optimizer`: Custom logical optimization rules.
  - `zelox-logical-plan`: Custom logical plan nodes.
  - `zelox-object-store`: Object store implementations and utilities.
  - `zelox-physical-optimizer`: Custom physical optimization rules.
  - `zelox-physical-plan`: Custom physical plan nodes.
  - `zelox-plan`: Logical plan resolver.
  - `zelox-python`: Native module for the `pyzelox` Python package.
  - `zelox-python-udf`: Python UDF support.
  - `zelox-server`: gRPC server utilities and actor implementation.
  - `zelox-session`: Session management.
  - `zelox-spark-connect`: Spark Connect protocol implementation.
  - `zelox-sql-*`: Sail SQL parser and analyzer.
  - `zelox-telemetry`: OpenTelemetry integration.
- `docs/`: Documentation site built with VitePress.
- `python/`: Source code for the `pyzelox` Python package.
- `scripts/`: Various scripts for development and testing purposes.
