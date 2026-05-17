# syntax=docker/dockerfile:1
#
# Ignite — Apple Container image
#
# Optimised for the apple/container CLI on macOS 26 (Tahoe).
# The builder stage compiles Rust natively inside the Linux/arm64 VM that
# Apple Container uses as its build environment, so no cross-compilation is
# needed.  The runtime stage is a minimal image containing only the ignite
# binary, Python 3.12, and pyspark-client for interactive testing.
#
# Build (one-time, ~25-35 min):
#   container builder start --cpus 4 --memory 5g
#   container build --platform linux/arm64 -f docker/apple/Dockerfile -t ignite:latest .
#
# Run:
#   container run --name ignite -p 50051:50051 ignite:latest
#
# Connect from inside the container:
#   container exec -it ignite python3
#   >>> from pyspark.sql import SparkSession
#   >>> spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
#   >>> spark.sql("SELECT 1 + 1 AS result").show()

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

# .dockerignore limits the context to Cargo.toml, Cargo.lock, and crates/
# so only source files (no 50 GB target/) are sent to the builder VM.
COPY . .

# Cargo registry and git cache are mounted so repeated builds reuse downloads.
# The /app/target/ cache is NOT mounted here because Apple Container's BuildKit
# cannot copy from a cache mount into the final stage; instead we copy the
# binary out explicitly after the build.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo build --release -p sail-cli --bins \
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
