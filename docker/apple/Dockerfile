# Vajra — Apple Container image  (वज्र: Sanskrit for thunderbolt)
#
# Apple Container (the `container` CLI on macOS 26 / Tahoe) reads this file
# the same way Docker does — the format is identical.  The ENTRYPOINT/CMD
# run inside a Linux/arm64 VM managed by Apple Virtualization Framework.
#
# Use the Makefile target to build — it handles Apple Container workarounds:
#
#   container builder start --cpus 4 --memory 8g --dns 8.8.8.8
#   make container-build                    # ~25-35 min first time
#
# Run after building:
#   mkdir -p /tmp/vajra
#   container run --name vajra -p 50051:50051 -v /tmp/vajra:/tmp/vajra vajra:latest
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
ARG PYSPARK_VERSION=4.0.0
ARG RUST_VERSION=1.95.0

# ── Stage 1: Rust builder ─────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS builder

ARG RUST_VERSION

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

# ── Layer: dependency manifests only (cached unless Cargo.lock changes) ────────
COPY Cargo.toml Cargo.lock manifests.tar.gz ./
RUN tar -xzf manifests.tar.gz && rm manifests.tar.gz
RUN cargo fetch --locked

# ── Layer: full source (invalidated on any src change) ────────────────────────
COPY crates.tar.gz ./
RUN tar -xzf crates.tar.gz && rm crates.tar.gz

# Low-memory build settings for constrained builders (e.g. 8 GB Macs):
# 1 codegen job + no debug symbols keeps peak RSS well under ~3 GB so the
# build VM does not swap-thrash. LTO off keeps it single-pass.
RUN CARGO_BUILD_JOBS=1 \
    CARGO_PROFILE_RELEASE_LTO=off \
    CARGO_PROFILE_RELEASE_DEBUG=false \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
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
# Default: single-node local mode.
# Distributed modes via SAIL_MODE env var:
#   local-cluster:      container run -e SAIL_MODE=local-cluster -v /tmp/vajra:/tmp/vajra vajra:latest
#   kubernetes-cluster: set SAIL_MODE=kubernetes-cluster in K8s manifest (see k8s/sail.yaml)
CMD ["server", "--ip", "0.0.0.0", "--port", "50051"]
