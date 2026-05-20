# Ignite — Build Status

> Last updated: 2026-05-20  
> Branch: `phase1/production-hardening`  
> Day: **10 of Phase 1**

---

## What's Done

### Foundation (Days 1–2) ✅
- Forked `lakehq/sail` → `ignite`; binary renamed; CLI restructured
- GitHub Actions CI (`ignite-ci.yml`): check / test / clippy / fmt on every push
- Cross-compile: Linux x86_64 + aarch64 musl via `cargo-zigbuild`; macOS universal2 via `lipo`
- Release workflow (`release-binary.yml`): publishes binaries on `v*` tags
- `install.sh` for `curl | sh` install

### Compat Audit (Day 3) ✅
- 94 skip/xfail annotations across 34 test files triaged into 10 categories
- Full audit documented in [COMPAT.md](COMPAT.md)
- Gold test suite: all passing

### Spark Compat Fixes — Batch 1 (Days 4–6) ✅

| Item | Description | Commit area |
|---|---|---|
| C1 DELETE | DELETE without WHERE via `lit(true)` predicate | `sail-delta-lake` |
| C1 UPDATE | UPDATE SET as Copy-on-Write via CASE/WHEN + Truncate | `sail-plan/resolver/command/update.rs` |
| C2/C10 monotonic_id | Pre-projection of volatile fn before aggregate | `sail-plan/resolver/query/aggregate.rs` |
| C4 FILTER | Stale skip removed; DataFusion already supported it | `test_group_by.py` |
| C6 INSERT OVERWRITE | Stale skip removed | `test_write_table.py` |
| C8 Managed tables | `is_external` flag; MANAGED default when no LOCATION | `sail-plan` + catalog impls |

### `ignite bench` (Day 5) ✅
- TPC-H 22-query harness: `ignite bench --scale-factor N`
- DuckDB-backed data generation; timing table output
- `make bench-sf1` / `make bench-sf10` Makefile targets

### C5 JSON Permissive Mode (Day 9) ✅ — merged PR #1
- **File:** `crates/sail-data-source/src/formats/json/permissive.rs`
- `PermissiveJsonDecoder`: line-by-line `serde_json` validation; PERMISSIVE / DROPMALFORMED / FAILFAST
- `_corrupt_record` column injection with raw malformed line text
- `columnNameOfCorruptRecord` option respected
- 7 Rust unit tests + streaming pipeline test (`DecoderDeserializer + deserialize_stream`)
- 5 PySpark smoke tests in `scripts/smoke_json_permissive.py` — all green
- Skip markers removed from `test_json_schema_show`, `test_json_schema_collect`

### Production Hardening (Day 10) ✅ — current branch
- **SIGTERM handling** (`crates/sail-cli/src/spark/server.rs`): `tokio::select!` on SIGINT + SIGTERM
  - Fixes `docker stop` / `container stop` / K8s pod eviction graceful shutdown
- **Readiness log**: `"Ignite ready on {addr} (Spark Connect gRPC)"` — smoke test and orchestrators detect startup
- **Apple Container build optimisation** (`docker/apple/Dockerfile` + `Makefile`):
  - `manifests.tar.gz` layer: `crates/*/Cargo.toml` only → `cargo fetch --locked` (cached unless `Cargo.lock` changes)
  - `crates.tar.gz` layer: full source → compile (only invalidated on source changes)
  - Result: source-only rebuilds drop from ~25 min → ~12–18 min
  - Workarounds: issue #425 (subdirs silently dropped) + issue #656 (stale DNS)
- **HEALTHCHECK**: TCP probe on port 50051; `--start-period 90s`

---

## What's In Progress

### `phase1/production-hardening` (current branch)
All items above committed. Branch pushed; PR not yet opened.

---

## What's Open (Next Up)

| Item | Priority | Notes |
|---|---|---|
| C3 UDF implicit type casting | P2 | `udf(lambda x: x)("int_col")` should return `"1"` not `1`; fix in `sail-python-udf` output coercion |
| C5 no-schema `_corrupt_record` | P2 | Schema inference must detect malformed lines + add `_corrupt_record: String` column |
| Convert smoke test to pytest | P2 | Move `scripts/smoke_json_permissive.py` → `python/pysail/tests/spark/test_json_permissive.py` |
| PR for `phase1/production-hardening` | P1 | Open against `phase1/foundation` |
| TPC-H benchmark baseline | P2 | Run SF-10 on current build; record numbers in PLAN.md |
| Delta Lake write path | P2 | W6 goal per PLAN.md |
| PyPI package (`ignite-pyspark`) | P2 | W8 goal per PLAN.md |

---

## How to Run

### Build
```sh
make dev                  # debug binary → target/debug/ignite
make release              # release binary → target/release/ignite
make build-linux          # cross-compile Linux musl (x86_64 + aarch64)
make build-macos          # macOS universal2
```

### Test
```sh
# Rust unit tests
PYO3_CONFIG_FILE=.cargo/pyo3-config.txt \
DYLD_FRAMEWORK_PATH=/Library/Developer/CommandLineTools/Library/Frameworks \
  cargo test --workspace --lib -- --test-threads=4

# PySpark smoke test (requires running server)
IGNITE_BIN=./target/debug/ignite \
DYLD_FRAMEWORK_PATH=/Library/Developer/CommandLineTools/Library/Frameworks \
  .venvs/smoke/bin/python scripts/smoke_json_permissive.py
```

### Apple Container
```sh
container builder start --cpus 4 --memory 8g --dns 8.8.8.8
make container-build              # incremental (uses layer cache)
make container-build-clean        # force full rebuild
container run --name ignite -p 50051:50051 ignite:latest
```

### Benchmark
```sh
make bench-sf1     # TPC-H SF-1  (~30s)
make bench-sf10    # TPC-H SF-10 (~5 min)
```

---

## Branch Map

| Branch | Status | Contents |
|---|---|---|
| `phase1/foundation` | Base | Foundation + compat fixes Days 1–9 (squash-merged from C5 PR) |
| `phase1/production-hardening` | Active | SIGTERM + Apple Container optimisation + this doc |
| `phase1/c5-corrupt-record` | Merged | C5 JSON permissive mode (squash-merged) |

---

## Key Files

| File | Purpose |
|---|---|
| [PLAN.md](PLAN.md) | Architecture, LLD, week-by-week execution plan, day tracker |
| [COMPAT.md](COMPAT.md) | Spark compat gap audit + fix status |
| [STATUS.md](STATUS.md) | This file — what's done, open, how to run |
| `crates/sail-data-source/src/formats/json/permissive.rs` | C5 JSON permissive decoder |
| `crates/sail-cli/src/spark/server.rs` | Server startup + SIGTERM/SIGINT shutdown |
| `docker/apple/Dockerfile` | Apple Container image with layer-cache build |
| `Makefile` | All build, test, bench, container targets |
| `scripts/smoke_json_permissive.py` | PySpark end-to-end smoke test for C5 |
| `.cargo/pyo3-config.txt` | PyO3 arm64 linking config for local dev |
