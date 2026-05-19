# Ignite — Apple Container image
#
# Apple Container (the `container` CLI on macOS 26 / Tahoe) reads this file
# the same way Docker does — the format is identical.  The ENTRYPOINT/CMD
# run inside a Linux/arm64 VM managed by Apple Virtualization Framework.
#
# Use the Makefile target to build — it handles Apple Container workarounds:
#
#   container builder start --cpus 4 --memory 5g
#   make container-build                    # ~25-35 min first time
#
# Run after building:
#   container run --name ignite -p 50051:50051 ignite:latest
#
# Test from inside the container:
#   container exec -it ignite python3
#   >>> from pyspark.sql import SparkSession
#   >>> spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
#   >>> spark.sql("SELECT 1 + 1 AS result").show()
#
# ── Apple Container workarounds ───────────────────────────────────────────────
# 1. Subdirectory bug (issue #425): `container build` sends only root-level
#    files to the builder VM — subdirectories are silently dropped.
#    Fix: `make container-build` bundles crates/ as crates.tar.gz (a single
#    root-level file) before calling `container build`.
#
# 2. DNS bug (issue #656): builder VM may have a stale DNS nameserver after
#    `container system stop/start`.
#    Fix: `make container-build` patches /etc/resolv.conf in the buildkit
#    container to use 8.8.8.8 before starting the build.

ARG PYTHON_VERSION=3.12
ARG PYSPARK_VERSION=4.1.1
ARG RUST_VERSION=1.95.0

# ── Stage 1: Rust builder ─────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS builder

ARG RUST_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        libc6-dev \
        protobuf-compiler \
        libprotobuf-dev \
        curl \
        git \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init \
    && chmod a+x /tmp/rustup-init \
    && /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION} \
    && rm /tmp/rustup-init

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# crates.tar.gz is created by `make container-build` (workaround for Apple
# Container issue #425 — only root-level files reach the builder VM).
COPY Cargo.toml Cargo.lock crates.tar.gz ./
RUN tar -xzf crates.tar.gz && rm crates.tar.gz

RUN cargo build --release -p sail-cli --bins \
    && cp /app/target/release/ignite /usr/local/bin/ignite

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim

ARG PYSPARK_VERSION

# libpython is already present via the python:3.12-slim base.
# Only add ca-certificates for TLS (S3, GCS, ADLS object store reads).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# pyspark-client is the lightweight Spark Connect-only client (no JVM).
RUN pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}"

COPY --from=builder /usr/local/bin/ignite /usr/local/bin/ignite

EXPOSE 50051

ENTRYPOINT ["/usr/local/bin/ignite"]
CMD ["server", "--ip", "0.0.0.0", "--port", "50051"]
