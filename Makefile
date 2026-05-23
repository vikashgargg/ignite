# Ignite build targets
# Usage: make <target>

.PHONY: help dev check test clippy fmt build-linux build-macos build-all release clean bench bench-sf1 bench-sf10 container-build container-build-clean

CARGO := $(shell which cargo)
BINARY := target/debug/ignite
RELEASE_DIR := target/release

# PyO3 requires the target Python version when cross-compiling to a different OS/arch.
# Set to match the Python version used in CI (3.11).
PYO3_CROSS_PYTHON_VERSION ?= 3.11

help:
	@echo "Ignite build targets:"
	@echo "  make dev          Build debug binary (fast, for local testing)"
	@echo "  make check        cargo check all crates (fastest correctness check)"
	@echo "  make test         Run unit tests"
	@echo "  make clippy       Run clippy linter"
	@echo "  make fmt          Check formatting"
	@echo "  make fmt-fix      Auto-fix formatting"
	@echo "  make build-linux  Cross-compile Linux x86_64 + aarch64 musl binaries"
	@echo "  make build-macos  Build macOS universal binary (x86_64 + aarch64)"
	@echo "  make build-all    All cross-compilation targets"
	@echo "  make release      Release build for native target"
	@echo "  make bench        Run TPC-H SF-1 benchmark (in-memory, requires duckdb)"
	@echo "  make bench-sf1    Same as bench"
	@echo "  make bench-sf10   Run TPC-H SF-10 benchmark (larger, ~60s)"
	@echo "  make container-build        Build Apple Container image (uses layer cache)"
	@echo "  make container-build-clean  Same, but forces a clean rebuild (--no-cache)"
	@echo "  make clean                  cargo clean"

dev:
	$(CARGO) build -p sail-cli
	@echo "Binary: $(BINARY)"

check:
	$(CARGO) check --workspace

test:
	$(CARGO) test --workspace --lib -- --test-threads=4

clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

fmt:
	$(CARGO) fmt --all -- --check

fmt-fix:
	$(CARGO) fmt --all

release:
	$(CARGO) build --release -p sail-cli
	@echo "Binary: $(RELEASE_DIR)/ignite"
	@ls -lh $(RELEASE_DIR)/ignite

build-linux:
	@echo "Building Linux x86_64 musl..."
	PYO3_CROSS_PYTHON_VERSION=$(PYO3_CROSS_PYTHON_VERSION) \
		$(CARGO) zigbuild --release -p sail-cli --target x86_64-unknown-linux-musl
	@echo "Building Linux aarch64 musl..."
	PYO3_CROSS_PYTHON_VERSION=$(PYO3_CROSS_PYTHON_VERSION) \
		$(CARGO) zigbuild --release -p sail-cli --target aarch64-unknown-linux-musl
	@echo ""
	@echo "Binaries:"
	@ls -lh target/x86_64-unknown-linux-musl/release/ignite
	@ls -lh target/aarch64-unknown-linux-musl/release/ignite
	@file target/x86_64-unknown-linux-musl/release/ignite
	@file target/aarch64-unknown-linux-musl/release/ignite

# Detect Python for PyO3 native link on macOS. Prefer a Python with python3-config
# (Homebrew/pyenv 3.11+) over the CommandLineTools Python (which lacks it).
PYTHON_BIN := $(shell command -v python3.11 2>/dev/null || \
                       command -v python3.12 2>/dev/null || \
                       command -v python3.13 2>/dev/null || \
                       command -v python3 2>/dev/null || \
                       echo python3)
PYTHON_LIB := $(shell $(PYTHON_BIN) -c \
  "import sys; print(sys.prefix + '/lib')" 2>/dev/null)

build-macos:
	@echo "Building macOS x86_64..."
	$(CARGO) zigbuild --release -p sail-cli --target x86_64-apple-darwin
	@echo "Building macOS aarch64 (native)..."
	PYO3_PYTHON=$(PYTHON_BIN) \
	RUSTFLAGS="$(if $(PYTHON_LIB),-L $(PYTHON_LIB))" \
	$(CARGO) build --release -p sail-cli --target aarch64-apple-darwin
	@echo "Creating universal binary..."
	lipo -create -output target/ignite-universal2-apple-darwin \
		target/x86_64-apple-darwin/release/ignite \
		target/aarch64-apple-darwin/release/ignite
	@echo ""
	@echo "Universal binary:"
	@ls -lh target/ignite-universal2-apple-darwin
	@file target/ignite-universal2-apple-darwin

build-all: build-linux build-macos

bench: bench-sf1

bench-sf1:
	@echo "Building Ignite release binary..."
	$(CARGO) build --release -p sail-cli
	@echo ""
	$(RELEASE_DIR)/ignite bench --scale-factor 1

bench-sf10:
	@echo "Building Ignite release binary..."
	$(CARGO) build --release -p sail-cli
	@echo ""
	$(RELEASE_DIR)/ignite bench --scale-factor 10

size-report:
	@echo "=== Binary Size Report ==="
	@for f in \
		target/x86_64-unknown-linux-musl/release/ignite \
		target/aarch64-unknown-linux-musl/release/ignite \
		target/x86_64-apple-darwin/release/ignite \
		target/aarch64-apple-darwin/release/ignite \
		target/ignite-universal2-apple-darwin; do \
		[ -f "$$f" ] && printf "%-60s %s\n" "$$f" "$$(ls -lh $$f | awk '{print $$5}')"; \
	done

clean:
	$(CARGO) clean

# ── Apple Container build ─────────────────────────────────────────────────────
# Workarounds for Apple Container bugs:
#   #425 — only root-level files reach the builder VM (subdirs silently dropped)
#   #656 — builder VM may have stale DNS after system restart
#
# Build strategy (layer caching):
#   manifests.tar.gz  — crates/*/Cargo.toml only; invalidates `cargo fetch` layer
#   crates.tar.gz     — full source; invalidates compile layer
#   Cargo.lock        — changes → both layers above must rerun
#
# First build:  ~25-35 min (download deps + compile)
# Source-only:  ~12-18 min (deps cached, recompile changed crates only)
# Cargo.lock:   ~20-25 min (re-fetch + recompile)
#
# Prerequisites: `container builder start --cpus 4 --memory 8g --dns 8.8.8.8`
_container-ctx:
	@echo "=== Fixing buildkit DNS (Apple Container issue #656) ==="
	container exec buildkit /bin/sh -c 'echo "nameserver 8.8.8.8" > /etc/resolv.conf' 2>/dev/null || true
	@echo "=== Creating build context in /tmp/ignite-apple-ctx ==="
	rm -rf /tmp/ignite-apple-ctx
	mkdir -p /tmp/ignite-apple-ctx
	cp Cargo.toml Cargo.lock /tmp/ignite-apple-ctx/
	cp docker/apple/Dockerfile /tmp/ignite-apple-ctx/Dockerfile
	find crates -name "Cargo.toml" | sort | tar -czf /tmp/ignite-apple-ctx/manifests.tar.gz -T -
	tar -czf /tmp/ignite-apple-ctx/crates.tar.gz crates/
	@echo "=== Build context ==="
	@du -sh /tmp/ignite-apple-ctx/

container-build: _container-ctx
	@echo "=== Running container build (incremental, uses layer cache) ==="
	container build --platform linux/arm64 -t ignite:latest /tmp/ignite-apple-ctx
	@echo "=== Done. Run with: container run --name ignite -p 50051:50051 ignite:latest ==="

container-build-clean: _container-ctx
	@echo "=== Running container build (clean, no cache) ==="
	container build --no-cache --platform linux/arm64 -t ignite:latest /tmp/ignite-apple-ctx
	@echo "=== Done. Run with: container run --name ignite -p 50051:50051 ignite:latest ==="
