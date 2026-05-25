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
ARG RUST_VERSION=1.86.0

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

# Layer: manifests only — cached unless Cargo.lock changes
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo fetch --locked

RUN CARGO_BUILD_JOBS=$(nproc) \
    CARGO_PROFILE_RELEASE_LTO=off \
    cargo build --release -p sail-cli --bins \
    && cp /app/target/release/vajra /usr/local/bin/vajra

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim

ARG PYSPARK_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Full pyspark (not pyspark-client) required for UDF cloudpickle deserialization.
# pandas + pyarrow required for Arrow UDFs and Pandas UDFs.
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" pandas pyarrow

COPY --from=builder /usr/local/bin/vajra /usr/local/bin/vajra

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD python3 -c \
        "import socket; s = socket.create_connection(('localhost', 50051), timeout=4); s.close()" \
    || exit 1

ENTRYPOINT ["/usr/local/bin/vajra"]
CMD ["server", "--ip", "0.0.0.0", "--port", "50051"]
