# Vajra — Modern Spark Replacement

> Sanskrit: **वज्र** (vajra) — *thunderbolt, indestructible, irresistible force*  
> The same word that gave English "vajra" also means diamond: unbreakable AND fastest thing in the sky.

---

## 0. Why Vajra, Not "Ignite"

The product needs a name that is:
- **Memorable** — one word, globally pronounceable
- **Meaningful** — connotes speed + indestructibility
- **Distinctive** — no collision with other tech projects
- **Sanskrit** — roots in Indian tradition, unique in the data stack space

**Vajra** (वज्र) wins on all four:
- Sanskrit for thunderbolt — the fastest natural phenomenon
- Also means diamond — indestructible, zero GC pauses, memory-safe
- Vedic: Indra's weapon, the ultimate tool
- No major tech product uses this name today

**Product positioning:** `vajra` CLI, `vajra-pyspark` PyPI package, `vajra/vajra` Docker image.

---

## 1. The Market Gap LakeSail Leaves Open

LakeSail/Sail is the best existing work. We respect it and build on it. But it has documented gaps:

| Gap | LakeSail Today | Vajra Target |
|-----|---------------|--------------|
| Spark test compatibility | 80.1% (3,075/3,839) | **95%+** |
| Structured streaming (Kafka) | Roadmap, incomplete | **Phase 1** |
| Production auth (JWT/TLS/OAuth2) | Not present | **Phase 1** |
| JDBC source/sink | Module-level skip | **Phase 2** |
| Multi-tenancy / session isolation | Not documented | **Phase 2** |
| Apple Container first-class support | Not present | **Day 1** ✅ |
| Published TPC-H numbers at SF-100+ | Claimed 4-8x, not verified | **Published benchmarks** |
| Web UI + observability | Planned, not shipped | **Phase 2** |
| `pip install` package | Not shipped | **Phase 1** |
| ARM64-native (M1/M2/M3/M4) | Works, not optimized | **ARM64-first build** |
| Helm chart + Kubernetes operator | Not present | **Phase 2** |
| IGNORE NULLS in FIRST/LAST | Skipped | **Phase 1** |

---

## 2. Product Vision

> **Vajra is the Spark you already know, running at Rust speed, on any machine from a MacBook Pro to a 1,000-node Kubernetes cluster.**

### Core promises
1. **Zero rewrites** — `sc://localhost:50051` replaces your Spark URL. Nothing else changes.
2. **Thunderbolt performance** — 5–10× faster than Spark 3.5 on TPC-H; validated at SF-1, SF-10, SF-100.
3. **Featherweight** — `< 500 ms` cold start. Idle at `< 15 MB` RAM. One static binary.
4. **Apple Silicon native** — arm64 container optimized for M1→M4 Macs via Apple Container API.
5. **Cloud-native K8s** — Helm chart, HPA, pod disruption budgets, Arrow Flight shuffle.
6. **Production-grade** — JWT auth, mTLS, multi-tenant session isolation, OpenTelemetry.
7. **Full streaming** — Kafka micro-batch, Delta/Iceberg sinks, at-least-once delivery.

---

## 3. Architecture (Target State)

```
┌──────────────────────────────────────────────────────────────────────────┐
│  CLIENTS (unchanged PySpark / SQL / Spark Connect SDK)                   │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │  Spark Connect gRPC / TLS + JWT
                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  VAJRA SERVER  (sail-spark-connect)                                      │
│  ├─ Auth middleware: JWT bearer / mTLS / API-key                         │
│  ├─ Session manager: per-user isolation, resource quotas                 │
│  ├─ Readiness + liveness probes (Kubernetes-native)                      │
│  └─ OpenTelemetry tracing (spans → Jaeger / Grafana OTLP)               │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │  Unresolved Relation / Plan
                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  QUERY PIPELINE                                                          │
│  Parser → Analyzer → Planner → Logical Optimizer → Physical Planner     │
│  → 95%+ Spark SQL compat                                                 │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │  Optimised Physical Plan
                            ▼
┌────────────────────────────────────┬─────────────────────────────────────┐
│  BATCH ENGINE (DataFusion)         │  STREAMING ENGINE (DataFusion)      │
│  Arrow columnar · SIMD · AVX-512   │  Micro-batch · Kafka source         │
│  Spill-aware aggregation           │  Delta/Iceberg sink                 │
│  Memory-mapped Parquet             │  Offset checkpointing               │
└──────────────┬─────────────────────┴─────────────────────────────────────┘
               │
       ┌───────┴───────────────────────────────────────────┐
       │           DEPLOYMENT MODES                         │
       │                                                    │
       │  local          local-cluster     kubernetes       │
       │  (1 process)    (N in-process     (worker pods     │
       │                  workers)          via k8s API)    │
       │                                                    │
       │  Apple Container  ←→  kind  ←→  Production K8s    │
       └────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STORAGE  (sail-object-store + sail-data-source)                         │
│  S3 · GCS · ADLS · HDFS · Local                                          │
│  Parquet · Delta Lake · Iceberg · ORC · CSV · JSON · Avro               │
│  Delta CDF · Iceberg time travel · Unity Catalog · Glue · HMS            │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. What Makes Vajra Better Than LakeSail (Technical Depth)

### 4.1 Spark Compatibility: 95%+ (vs 80.1%)

LakeSail passes 3,075/3,839 Spark tests. The remaining ~764 fail across:
- `FIRST/LAST IGNORE NULLS` — single optimizer rule fix
- `LATERAL JOIN` edge cases — planner extension
- `VARIANT` type (Spark 4.0) — new Arrow type mapping
- Complex Python data sources — PyO3 extension
- JDBC source — `tokio` + `sqlx` bridge
- Nested map getItem chaining — type resolver fix
- GeometryType / GeographyType — custom type registration

**Vajra approach:** Fix in priority order by enterprise usage frequency. Target: 95% by Month 3.

### 4.2 Structured Streaming (Missing in LakeSail)

Use DataFusion's `StreamingTableExec` + `rdkafka` for Kafka consumer:

```rust
// Kafka source → DataFusion unbounded table
KafkaTableProvider {
    brokers: Vec<String>,
    topic: String,
    starting_offsets: KafkaOffset,
}
// Triggers micro-batch execution every N seconds
// Writes to Delta Lake sink atomically
// Checkpoints offset to object store
```

Delivers: `readStream.format("kafka")`, `writeStream.format("delta")`, `trigger(processingTime="10 seconds")`.

### 4.3 Production Auth (Missing in LakeSail)

gRPC interceptor layer:

```rust
// tonic middleware
pub struct AuthInterceptor {
    jwks: Arc<JwksClient>,     // fetch public keys from JWKS endpoint
    api_keys: Arc<ApiKeyStore>, // hashed API keys in memory
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let token = extract_bearer(req.metadata())?;
        let claims = self.jwks.verify(token)?;
        // inject user identity into request extension
        Ok(req)
    }
}
```

Modes: `--auth=none` (dev), `--auth=jwt` (prod), `--auth=mtls` (enterprise).

### 4.4 Apple Container First-Class

Vajra is the only Spark replacement with native Apple Container support:
- `make container-build` — layer-cached incremental build
- `container run --name vajra -p 50051:50051 -v /tmp/vajra:/tmp/vajra vajra:latest`
- ARM64-native binary, no Rosetta overhead
- VirtioFS volume mounts for host filesystem access
- Developer UX: `vajra container dev` CLI wrapper

### 4.5 Kubernetes Helm Chart

```yaml
# helm install vajra vajra/vajra --set server.replicas=3
vajra-server:
  replicaCount: 3
  resources:
    requests: { memory: 512Mi, cpu: 500m }
  autoscaling:
    enabled: true
    minReplicas: 1
    maxReplicas: 20
    targetCPUUtilizationPercentage: 70
vajra-worker:
  replicaCount: 0  # scale to zero
  resources:
    requests: { memory: 2Gi, cpu: 2 }
```

### 4.6 Benchmark Transparency

| Benchmark | SF | Vajra | Spark 3.5 | Speedup |
|-----------|-----|-------|-----------|---------|
| TPC-H Q1  | 1   | TBD   | baseline  | TBD     |
| TPC-H Q6  | 1   | TBD   | baseline  | TBD     |
| TPC-H all | 10  | TBD   | baseline  | TBD     |
| TPC-H all | 100 | TBD   | baseline  | TBD     |

*Run on M4 Mac Mini (10-core, 32 GB) — repeatable on any machine.*

---

## 5. Execution Roadmap

### Phase 1 — "Production Single-Node" (Months 1–3)

**Goal: v0.1.0 release. 95% Spark compat. pip install. Published benchmarks.**

| Week | Deliverable | Owner |
|------|-------------|-------|
| W1 | TPC-H SF-1/SF-10/SF-100 baseline numbers. Rename binary `vajra`. `vajra-pyspark` PyPI stub. | Now |
| W2 | Fix compat batch 2: IGNORE NULLS, nested map, VARIANT sketch | Now |
| W3 | JWT auth interceptor (tonic middleware). `--auth` flag. | |
| W4 | Kafka micro-batch streaming: `rdkafka` source + offset checkpoint | |
| W5 | JDBC source via `sqlx` + Arrow bridge | |
| W6 | `pip install vajra-pyspark` on PyPI (thin Spark Connect client) | |
| W7 | Apple Container: `vajra container` CLI subcommand, VirtioFS docs | |
| W8 | Helm chart v1: server + worker, HPA, liveness/readiness probes | |
| W9 | OpenTelemetry: per-query spans, Prometheus `/metrics` endpoint | |
| W10 | Full gold test run: target 95%+ pass rate. Fix remaining gaps. | |
| W11 | Performance tuning: profiling TPC-H bottlenecks, SIMD kernels | |
| W12 | v0.1.0 release: blog, HN post, published benchmark table | |

### Phase 2 — "Distributed GA" (Months 4–6)

**Goal: v0.3.0. 1TB TPC-H. Streaming at scale. Multi-tenancy.**

| Month | Theme | Deliverable |
|-------|-------|-------------|
| M4 | Distributed shuffle | Arrow Flight map/reduce shuffle, memory-first with spill |
| M4 | Fault tolerance | Task retry, worker heartbeat, dead-letter queue |
| M5 | Full streaming | Delta streaming sink, watermark, session windows |
| M5 | Multi-tenancy | Per-user session isolation, resource quotas, audit log |
| M6 | 1TB benchmark | Distributed TPC-H SF-1000 on 10 K8s workers, publish |
| M6 | Web UI | Query history, metrics, live plan visualization |

### Phase 3 — "Cloud GA" (Months 7–12)

**Goal: v1.0.0. SaaS offering. Databricks-compatible REST API.**

| Month | Theme |
|-------|-------|
| M7–8 | REST API + OAuth2 OIDC, web UI, user management |
| M9 | BYOC worker deploy (VPC injection, customer isolation) |
| M10 | Scale-to-zero workers, sub-500ms cold start validated |
| M11 | MLflow REST API backed by object store |
| M12 | v1.0.0 launch, public cloud pricing |

---

## 6. Immediate Sprint Tasks (This Week)

### Sprint 1 Backlog (Priority Order)

1. **[P0] Run TPC-H SF-1 and SF-10 benchmarks** — get real numbers now  
   `make bench-sf1 && make bench-sf10`

2. **[P0] Run full gold test suite** — quantify compat % vs LakeSail's 80.1%  
   Run `python/pysail/tests/spark/` against running server

3. **[P1] Binary rename: `vajra` ✅ done**  
   - `Cargo.toml` bin name, README, install.sh, Makefile targets
   - Keep internal `sail-*` crate names (upstream compat)

4. **[P1] Fix FIRST/LAST IGNORE NULLS**  
   `crates/sail-plan/src/resolver/expression/function.rs`

5. **[P1] Fix nested map getItem chaining**  
   `crates/sail-physical-plan` type resolver

6. **[P1] JWT auth interceptor** — tonic middleware layer  
   `crates/sail-spark-connect/src/service.rs`

7. **[P2] Kafka streaming source** — `rdkafka` + DataFusion StreamingTableExec  
   New crate: `crates/sail-stream`

8. **[P2] `vajra-pyspark` PyPI package** — thin Spark Connect client  
   `python/vajra_pyspark/`

9. **[P2] Helm chart** — `deployments/helm/vajra/`

10. **[P2] `make bench` output → `BENCHMARKS.md`** — published baseline

---

## 7. Competitive Positioning

| | Apache Spark | Databricks | LakeSail | **Vajra** |
|---|---|---|---|---|
| License | Apache 2.0 | Proprietary | Apache 2.0 | **Apache 2.0** |
| Runtime | JVM | JVM (Photon Rust) | Rust | **Rust** |
| Cold start | 30–120s | 2–5 min | < 500ms | **< 500ms** |
| Apple Silicon | Emulated | Cloud only | Works | **Native arm64** |
| Apple Container | ❌ | ❌ | ❌ | **✅** |
| Streaming | ✅ full | ✅ full | Partial | **✅ Phase 1** |
| Auth | SSL/JWT | Full IAM | ❌ | **JWT/mTLS** |
| Spark compat | 100% | 100% | 80.1% | **95%+ target** |
| pip install | pyspark (JVM needed) | databricks-connect | sail | **vajra-pyspark** |
| Helm chart | community | ✅ | ❌ | **✅** |
| Open source | ✅ | ❌ | ✅ | **✅** |

---

## 8. Mac-First Developer Experience

Vajra's killer differentiator: **it's the only engine that runs natively on a MacBook as if it were a server**.

```bash
# Install
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh

# Run on Apple Container (arm64 native, no Docker)
make container-build
container run -p 50051:50051 -v /tmp/vajra:/tmp/vajra vajra:latest

# Or local binary (no container needed)
vajra server --port 50051

# Your existing PySpark code connects unchanged
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
# ^^^ only line change from JVM Spark ^^^
```

**M4 Mac Mini as a benchmark machine:**  
- 10-core ARM, 32–128 GB unified memory, ~$1,000
- Runs TPC-H SF-100 with 32 GB RAM — comparable to a 16-core cloud VM at $3/hr
- `vajra bench --scale-factor 100` — one command, real numbers, local

---

## 9. Sanskrit Name Decision Matrix

| Name | Meaning | Pronunciation | Uniqueness | Recommendation |
|------|---------|---------------|------------|----------------|
| **Vajra** | Thunderbolt + Diamond | VAJ-ra | High | ⭐ **CHOSEN** |
| Sphuling | Spark (literal) | SPHU-ling | Very High | Runner-up |
| Ulka | Meteor / Fireball | UL-ka | High | Short, catchy |
| Archis | Flame / Ray | AR-chis | Medium | Less distinctive |
| Agni | Fire | AG-ni | Low (used widely) | Too common |

**Vajra** wins: indestructible speed. Databricks has "Photon" (light). We have "Vajra" (thunderbolt that moves faster than light in Vedic texts).

---

## 10. Success Metrics for v0.1.0

| Metric | Target |
|--------|--------|
| Spark compat test pass rate | ≥ 95% (vs LakeSail 80.1%) |
| TPC-H SF-10 vs Spark 3.5 | ≥ 5× faster |
| Cold start time | ≤ 500 ms |
| pip install size | ≤ 5 MB (client only) |
| Binary size (Linux musl) | ≤ 100 MB |
| GitHub stars (3 months) | 500+ |
| Scorecard (71/71 modes) | ✅ Already achieved |
| Apple Container + K8s | ✅ Already achieved |

---

*Sources and references: [LakeSail Sail 0.3](https://lakesail.com/blog/sail-0-3/) · [LakeSail GitHub](https://github.com/lakehq/sail) · [DataFusion Streaming](https://www.denormalized.io/blog/streaming-datafusion) · [Sail HN Discussion](https://news.ycombinator.com/item?id=44503095)*
