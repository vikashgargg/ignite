---
title: Why Zelox?
rank: 2
---

# Why Zelox?

Today's cloud environments and data workloads pose challenges not anticipated by solutions developed a decade ago. Organizations choose Zelox because it accelerates execution, reduces resource consumption, simplifies data infrastructure, and enables seamless migration.

## Performance

Zelox delivers predictable performance characteristics across diverse workloads.

Zelox leverages Apache Arrow for optimal CPU cache utilization and enables vectorized operations via SIMD instructions. The columnar memory layout offers superior performance compared to row-oriented data models seen in Apache Spark or Apache Flink.

By eliminating the JVM, Zelox is free from GC (garbage collection) overhead during query execution. Latency spikes caused by GC pauses will also be eliminated when Zelox supports data streaming in the near future.

Python UDFs (user-defined functions) are highly performant in Zelox. The [PyO3](https://pyo3.rs/) library embeds a Python interpreter in the Zelox process. The Arrow format enables zero-copy data sharing between Zelox and Python, making your Python code a native part of Zelox.

## Memory Efficiency

Rust's zero-cost abstractions allow for modular Zelox internals with a low memory footprint. The Zelox process starts within seconds and consumes only a few dozen megabytes of memory when idle. This means you can scale Zelox workers quickly and efficiently as the load increases.

There is no need for JVM tuning anymore. You no longer need to worry about memory usage due to overhead in JVM objects or squeeze performance out of Spark memory configuration.

In our [Benchmark Results](../benchmark-results/), Zelox delivers a 4x speed-up over Apache Spark and reduces hardware costs by up to 94% due to the combined effect of shorter query execution times and lower memory usage.

## Robustness

Zelox benefits from Rust's unique approach to memory management. The _ownership_ rules and reference _lifetimes_ enforced at compile time eliminate whole categories of memory bugs. Combined with libraries such as [Tokio](https://tokio.rs/), Zelox enjoys _fearless concurrency_, meaning that safe async code is a natural ingredient of Zelox internals. The end result is a correct and performant compute engine runtime that you can trust.

## Compatibility

Zelox features a drop-in replacement for Spark SQL and the Spark DataFrame API. Your Spark client session communicates with the Zelox server over gRPC via the Spark Connect protocol.

Zelox treats compatibility with Spark seriously. If there is a behavior mismatch between Zelox and Spark, we consider it a bug. As you explore the documentation, you will find that Zelox already supports most common usages of Spark. Our supported features keep expanding toward full parity with Spark.

## Simplicity

The `zelox` command-line interface (CLI) is the single entry point for all Zelox commands. The CLI is available either by installing the `pyzelox` Python library or building the standalone binary from source. You can also use the Python API to start the Zelox server within your PySpark code.

As a unified engine, Zelox enables you to run ad-hoc SQL queries, execute distributed batch jobs, or preprocess data for AI models within a single environment, eliminating the need to switch runtimes or move data between systems. We strive for a smooth developer experience as you scale your workloads from your laptop to a production cluster.
