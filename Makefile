# Vajra build targets  (binary: vajra, internal crates: sail-*)
# Usage: make <target>

.PHONY: help dev check test clippy fmt build-linux build-macos build-all release clean \
        bench bench-sf1 bench-sf10 size-report \
        container-build container-build-clean container-run container-run-cluster \
        docker-build _docker-ctx _container-ctx \
        kind-setup kind-teardown \
        helm-install helm-upgrade helm-uninstall helm-lint \
        smoke-setup scorecard scorecard-container scorecard-k8s

CARGO      := $(shell which cargo)
BINARY     := target/debug/vajra
RELEASE_DIR := target/release

# PyO3 requires the target Python version when cross-compiling to a different OS/arch.
PYO3_CROSS_PYTHON_VERSION ?= 3.11

# Image + container names
IMAGE      ?= vajra:latest

# ── Smoke test venv ───────────────────────────────────────────────────────────
# Python version must match the container (ARG PYTHON_VERSION in Dockerfile).
CONTAINER_PY   := $(shell grep -oE 'ARG PYTHON_VERSION=[0-9]+\.[0-9]+' docker/apple/Dockerfile | cut -d= -f2)
SMOKE_VENV     := .venvs/smoke
SMOKE_PYTHON   := $(SMOKE_VENV)/bin/python
SMOKE_PYPATH   := $(SMOKE_VENV)/lib/python$(CONTAINER_PY)/site-packages
SCORECARD      := $(SMOKE_PYTHON) scripts/spark_compat_score.py
CONTAINER  ?= vajra

help:
	@echo "Vajra build targets:"
	@echo "  make dev                     Build debug binary (fast, for local testing)"
	@echo "  make check                   cargo check all crates (fastest correctness check)"
	@echo "  make test                    Run unit tests"
	@echo "  make clippy                  Run clippy linter"
	@echo "  make fmt                     Check formatting"
	@echo "  make fmt-fix                 Auto-fix formatting"
	@echo "  make build-linux             Cross-compile Linux x86_64 + aarch64 musl binaries"
	@echo "  make build-macos             Build macOS universal binary (x86_64 + aarch64)"
	@echo "  make build-all               All cross-compilation targets"
	@echo "  make release                 Release build for native target"
	@echo "  make bench                   Run TPC-H SF-1 benchmark (in-memory, requires duckdb)"
	@echo "  make bench-sf1               Same as bench"
	@echo "  make bench-sf10              Run TPC-H SF-10 benchmark (larger, ~60s)"
	@echo "  make container-build         Build Apple Container image (uses layer cache)"
	@echo "  make container-build-clean   Same, but forces a clean rebuild (--no-cache)"
	@echo "  make container-run           Run vajra container in local (single-node) mode"
	@echo "  make container-run-cluster   Run vajra container in local-cluster mode"
	@echo "  make docker-build            Build vajra Docker image for use with kind/k8s"
	@echo "  make kind-setup              Create kind cluster, load image, deploy vajra"
	@echo "  make kind-teardown           Delete kind cluster"
	@echo "  make helm-lint               Lint the Helm chart"
	@echo "  make helm-install            Install/upgrade Vajra via Helm into current k8s context"
	@echo "  make helm-upgrade            Upgrade existing Helm release"
	@echo "  make helm-uninstall          Uninstall Vajra Helm release"
	@echo "  make smoke-setup             Create .venvs/smoke with correct Python version"
	@echo "  make scorecard               Run 71-test compat scorecard (local binary, debug)"
	@echo "  make scorecard-container     Run scorecard against running Apple Container (:50051)"
	@echo "  make scorecard-k8s           Run scorecard against running k8s port-forward (:50051)"
	@echo "  make clean                   cargo clean"

# ── Helm ─────────────────────────────────────────────────────────────────────
HELM       := $(shell command -v helm 2>/dev/null || echo helm)
HELM_CHART := helm/vajra
HELM_RELEASE ?= vajra
HELM_NAMESPACE ?= vajra

helm-lint:
	$(HELM) lint $(HELM_CHART)

helm-install:
	$(HELM) upgrade --install $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(HELM_NAMESPACE) --create-namespace

helm-upgrade: helm-install

helm-uninstall:
	$(HELM) uninstall $(HELM_RELEASE) --namespace $(HELM_NAMESPACE) || true
	kubectl delete namespace $(HELM_NAMESPACE) --ignore-not-found

# ── Smoke venv + scorecard ────────────────────────────────────────────────────
smoke-setup:
	bash scripts/setup-smoke-venv.sh

# Local binary mode — starts its own server, no running container needed.
scorecard: $(SMOKE_PYTHON)
	DYLD_FRAMEWORK_PATH=/Library/Developer/CommandLineTools/Library/Frameworks \
	PYTHONPATH=$(SMOKE_PYPATH) \
	VAJRA_BIN=$(RELEASE_DIR)/vajra \
		$(SCORECARD)

# Remote container/k8s mode — requires a server already running on :50051.
# File-based tests (JSON, Parquet, Delta) use /tmp/vajra which is mounted
# into every container via -v /tmp/vajra:/tmp/vajra.
scorecard-container scorecard-k8s: $(SMOKE_PYTHON)
	@mkdir -p /tmp/vajra
	SPARK_REMOTE=sc://localhost:50051 \
	PYTHONPATH=$(SMOKE_PYPATH) \
		$(SCORECARD)

$(SMOKE_PYTHON):
	@echo "Smoke venv not found — run: make smoke-setup"
	@exit 1

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
	@echo "Binary: $(RELEASE_DIR)/vajra"
	@ls -lh $(RELEASE_DIR)/vajra

# ── Cross-compilation ─────────────────────────────────────────────────────────
build-linux:
	@echo "Building Linux x86_64 musl..."
	PYO3_CROSS_PYTHON_VERSION=$(PYO3_CROSS_PYTHON_VERSION) \
		$(CARGO) zigbuild --release -p sail-cli --target x86_64-unknown-linux-musl
	@echo "Building Linux aarch64 musl..."
	PYO3_CROSS_PYTHON_VERSION=$(PYO3_CROSS_PYTHON_VERSION) \
		$(CARGO) zigbuild --release -p sail-cli --target aarch64-unknown-linux-musl
	@echo ""
	@echo "Binaries:"
	@ls -lh target/x86_64-unknown-linux-musl/release/vajra
	@ls -lh target/aarch64-unknown-linux-musl/release/vajra
	@file target/x86_64-unknown-linux-musl/release/vajra
	@file target/aarch64-unknown-linux-musl/release/vajra

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
	lipo -create -output target/vajra-universal2-apple-darwin \
		target/x86_64-apple-darwin/release/vajra \
		target/aarch64-apple-darwin/release/vajra
	@echo ""
	@echo "Universal binary:"
	@ls -lh target/vajra-universal2-apple-darwin
	@file target/vajra-universal2-apple-darwin

build-all: build-linux build-macos

# ── Benchmarks ────────────────────────────────────────────────────────────────
bench: bench-sf1

bench-sf1:
	@echo "Building Vajra release binary..."
	$(CARGO) build --release -p sail-cli
	@echo ""
	$(RELEASE_DIR)/vajra bench --scale-factor 1

bench-sf10:
	@echo "Building Vajra release binary..."
	$(CARGO) build --release -p sail-cli
	@echo ""
	$(RELEASE_DIR)/vajra bench --scale-factor 10

size-report:
	@echo "=== Binary Size Report ==="
	@for f in \
		target/x86_64-unknown-linux-musl/release/vajra \
		target/aarch64-unknown-linux-musl/release/vajra \
		target/x86_64-apple-darwin/release/vajra \
		target/aarch64-apple-darwin/release/vajra \
		target/vajra-universal2-apple-darwin; do \
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
# First build:  ~25-35 min (8 GB builder, no other VMs running)
# Source-only:  ~12-18 min (deps cached, recompile changed crates only)
# Cargo.lock:   ~20-25 min (re-fetch + recompile)
#
# Prerequisites: `container builder start --cpus 4 --memory 8g --dns 8.8.8.8`
_container-ctx:
	@echo "=== Fixing buildkit DNS (Apple Container issue #656) ==="
	container exec buildkit /bin/sh -c 'echo "nameserver 8.8.8.8" > /etc/resolv.conf' 2>/dev/null || true
	@echo "=== Creating build context in /tmp/vajra-ctx ==="
	rm -rf /tmp/vajra-ctx
	mkdir -p /tmp/vajra-ctx
	cp Cargo.toml Cargo.lock /tmp/vajra-ctx/
	cp docker/apple/Dockerfile /tmp/vajra-ctx/Dockerfile
	bash scripts/make-manifests.sh /tmp/vajra-ctx/manifests.tar.gz
	tar -czf /tmp/vajra-ctx/crates.tar.gz crates/
	@echo "=== Build context ==="
	@du -sh /tmp/vajra-ctx/

container-build: _container-ctx
	@echo "=== Running container build (incremental, uses layer cache) ==="
	container build --platform linux/arm64 -t $(IMAGE) /tmp/vajra-ctx
	@echo "=== Done. Run with: container run --name $(CONTAINER) -p 50051:50051 -v /tmp/vajra:/tmp/vajra $(IMAGE) ==="

container-build-clean: _container-ctx
	@echo "=== Running container build (clean, no cache) ==="
	container build --no-cache --platform linux/arm64 -t $(IMAGE) /tmp/vajra-ctx
	@echo "=== Done. Run with: container run --name $(CONTAINER) -p 50051:50051 -v /tmp/vajra:/tmp/vajra $(IMAGE) ==="

# ── Apple Container run helpers ───────────────────────────────────────────────
container-run:
	@echo "=== Starting Vajra (local single-node mode) ==="
	mkdir -p /tmp/vajra
	container run --rm --name $(CONTAINER) -p 50051:50051 \
		-v /tmp/vajra:/tmp/vajra \
		$(IMAGE)

container-run-cluster:
	@echo "=== Starting Vajra (local-cluster distributed mode) ==="
	mkdir -p /tmp/vajra
	container run --rm --name $(CONTAINER)-cluster -p 50051:50051 \
		-e SAIL_MODE=local-cluster \
		-v /tmp/vajra:/tmp/vajra \
		$(IMAGE)

# ── Docker build (for kind/k8s) ───────────────────────────────────────────────
_docker-ctx:
	@echo "=== Creating build context in /tmp/vajra-ctx ==="
	rm -rf /tmp/vajra-ctx
	mkdir -p /tmp/vajra-ctx
	cp Cargo.toml Cargo.lock /tmp/vajra-ctx/
	cp docker/apple/Dockerfile /tmp/vajra-ctx/Dockerfile
	bash scripts/make-manifests.sh /tmp/vajra-ctx/manifests.tar.gz
	tar -czf /tmp/vajra-ctx/crates.tar.gz crates/
	@echo "=== Build context ==="
	@du -sh /tmp/vajra-ctx/

docker-build: _docker-ctx
	@echo "=== Building Docker image $(IMAGE) (linux/arm64) ==="
	CARGO_PROFILE_RELEASE_LTO=off CARGO_BUILD_JOBS=2 \
	docker build --platform linux/arm64 -t $(IMAGE) /tmp/vajra-ctx
	@echo "=== Done. Load into kind with: kind load docker-image $(IMAGE) ==="

# ── kind Kubernetes cluster ───────────────────────────────────────────────────
KIND_CLUSTER ?= vajra-dev

kind-setup:
	@echo "=== Creating kind cluster '$(KIND_CLUSTER)' ==="
	mkdir -p /tmp/vajra /private/tmp/vajra /private/tmp/sail
	kind create cluster --name $(KIND_CLUSTER) --config k8s/kind-config.yaml
	@echo "=== Loading $(IMAGE) into kind ==="
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)
	@echo "=== Deploying Vajra to kind ==="
	kubectl apply -f k8s/sail.yaml
	@echo "=== Waiting for pod to be ready ==="
	kubectl rollout status deployment/vajra-spark-server -n vajra --timeout=120s
	@echo ""
	@echo "=== Port-forward: kubectl port-forward -n vajra svc/vajra-spark-server 50051:50051 ==="

kind-teardown:
	@echo "=== Deleting kind cluster '$(KIND_CLUSTER)' ==="
	kind delete cluster --name $(KIND_CLUSTER)
