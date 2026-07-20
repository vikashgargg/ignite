# Vajra — standard Docker image (K8s / Linux CI builds)
#
# This Dockerfile builds from source without the Apple Container tarball
# workaround (see docker/apple/Dockerfile for Apple Container builds).
#
# Usage:
#   docker build -t vajra:latest -f docker/Dockerfile .
#   docker run -p 50051:50051 vajra:latest

ARG PYTHON_VERSION=3.12
ARG PYSPARK_VERSION=4.0.0
ARG RUST_VERSION=1.95.0

# ── Stage 1: Rust builder ─────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS builder

ARG RUST_VERSION
# Cap parallel cargo jobs on low-RAM hosts to avoid OOM during the final link
# (e.g. `--build-arg CARGO_JOBS=2` on an 8 GB Mac). Defaults to all cores for CI.
ARG CARGO_JOBS

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        g++ \
        libc6-dev \
        protobuf-compiler \
        libprotobuf-dev \
        curl \
        git \
        pkg-config \
        cmake \
        make \
        perl \
        libcurl4-openssl-dev \
        libssl-dev \
        zlib1g-dev \
        libsasl2-dev \
        libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init \
    && chmod a+x /tmp/rustup-init \
    && /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION} \
    && rm /tmp/rustup-init

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Layer: manifests only — cached unless Cargo.lock changes
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo fetch --locked

# CARGO_FEATURES: jemalloc = prod allocator (returns freed pages via decay, bounds RSS the way
# Flink/RocksDB/ForSt do) + heap-profiler (prof:true via MALLOC_CONF names live-heap call-stacks).
ARG CARGO_FEATURES=jemalloc
# Prod default: stripped (small image). Profiling builds pass RELEASE_DEBUG=1 + RELEASE_STRIP=false so
# jeprof can attribute heap call-stacks to source (box_onbox_confidence.sh does this).
ARG RELEASE_DEBUG=0
ARG RELEASE_STRIP=true
RUN CARGO_BUILD_JOBS=${CARGO_JOBS:-$(nproc)} \
    CARGO_PROFILE_RELEASE_LTO=off \
    CARGO_PROFILE_RELEASE_DEBUG=${RELEASE_DEBUG} \
    CARGO_PROFILE_RELEASE_STRIP=${RELEASE_STRIP} \
    cargo build --release -p sail-cli --bins ${CARGO_FEATURES:+--features ${CARGO_FEATURES}} \
    && cp /app/target/release/vajra /usr/local/bin/vajra

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim

ARG PYSPARK_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Full pyspark (not pyspark-client) required for UDF cloudpickle deserialization.
# pandas + pyarrow required for Arrow UDFs and Pandas UDFs.
# grpcio + grpcio-status + protobuf are the Spark Connect client deps so in-container
# `vajra run` / `vajra shell` (which connect via sc://) work, not just the server.
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" pandas pyarrow \
        grpcio grpcio-status protobuf googleapis-common-protos

COPY --from=builder /usr/local/bin/vajra /usr/local/bin/vajra

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD python3 -c \
        "import socket; s = socket.create_connection(('localhost', 50051), timeout=4); s.close()" \
    || exit 1

ENTRYPOINT ["/usr/local/bin/vajra"]
CMD ["server", "--ip", "0.0.0.0", "--port", "50051"]
