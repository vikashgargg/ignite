# Vajra — Threat Model & Security Findings

First-pass internal security review (2026-06-06). Scope: the Spark Connect gRPC
server, its auth/TLS, the Web UI, and the dependency supply chain. This is **not**
a substitute for a third-party penetration test (still outstanding — see
[PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) §3).

## Assets
- **Data**: the contents of tables/files Vajra reads/writes (Parquet, Delta,
  Iceberg, object stores).
- **Credentials**: object-store keys, catalog tokens, the Bearer auth token, TLS
  private keys.
- **Compute/availability**: the driver + worker processes.
- **Query metadata**: plans, schemas, metrics (exposed via Web UI / reflection).

## Trust boundaries
1. **Client → driver** (Spark Connect gRPC, default `:50051`). Primary boundary.
2. **Driver → workers** (internal gRPC). Assumed same trust zone today.
3. **Engine → object store / catalog** (S3, HMS, Glue, Unity, REST).
4. **Operator → Web UI** (HTTP `:4040`).

## Attacker model
- **Network attacker** who can reach `:50051` / `:4040`.
- **Malicious client** that can send arbitrary Spark Connect / SQL.
- **Passive eavesdropper** on the wire (if TLS off).
- Out of scope for now: a hostile co-tenant on the same host; a compromised worker.

---

## Findings

| ID | Severity | Area | Status |
|---|---|---|---|
| F1 | Medium | Non-constant-time Bearer token compare (timing side-channel) | **fixed** |
| F2 | Low–Med | gRPC reflection unauthenticated | **fixed** (off when auth on) |
| F3 | Medium | Web UI binds `0.0.0.0:4040`, no auth, no TLS | **fixed** (loopback default) |
| F4 | Medium | Bearer token accepted over cleartext (no TLS requirement) | **fixed** (refuses to start) |
| F5 | Low | Insecure-by-default (no auth unless a token is set) | doc |
| F6 | Medium | Resource-exhaustion / DoS | **mitigated** (caps added) |
| D1 | High→**Fixed** | 4 RUSTSEC vulns in `astral-tokio-tar` (dev-dep) | **fixed** |
| D2 | Low | 3 unmaintained transitive crates (json, paste, proc-macro-error) | accepted |

### F1 — Non-constant-time token comparison (Medium)
`crates/sail-spark-connect/src/entrypoint.rs:57` compares the presented Bearer
token with `provided == expected` (plain `String` equality, short-circuits on
first differing byte). This leaks token bytes via response-timing to an attacker
who can make many requests.
**Fix:** constant-time compare (e.g. `subtle::ConstantTimeEq` or
`constant_time_eq`). Small, isolated change.

### F2 — Unauthenticated reflection (Low–Medium) — **FIXED**
The Bearer interceptor wraps only the Spark Connect service, while gRPC
**reflection** + **health** were added to the base router without it, so reflection
exposed the full proto schema to anonymous clients.
**Fixed:** `ServerBuilderOptions.reflection` now gates the reflection service, and
`entrypoint.rs` sets `reflection: expected_token.is_none()` — i.e. when an auth
token is configured, anonymous reflection is **off**. Health is intentionally left
open (liveness probes need it; it discloses nothing sensitive).

### F3 — Web UI exposed, unauthenticated (Medium) — **FIXED**
`web_ui::serve` previously bound `0.0.0.0:4040` unconditionally.
**Fixed:** added `UiConfig { enabled, host, port }` defaulting to **`127.0.0.1:4040`**,
so the unauthenticated UI is not reachable off-host by default. Operators can set
`SAIL_UI__HOST` (e.g. `0.0.0.0` behind a network policy) or `SAIL_UI__ENABLED=false`.

### F4 — Bearer token over cleartext (Medium) — **FIXED**
The token interceptor functioned regardless of TLS, so with TLS off the token
travelled in request metadata in the clear.
**Fixed:** the server now **refuses to start** when a token is set without TLS,
unless `SAIL_AUTH__ALLOW_INSECURE_TOKEN=true` is set explicitly (trusted-network
escape hatch). Guidance also in SECURITY.md.

### F5 — Insecure by default (Low, by design)
With no `--auth-token`/TLS, the server accepts all clients (dev convenience). This
is acceptable for local dev but must be explicit. Documented in SECURITY.md; the
hardened-deployment guidance covers production.

### F6 — Resource-exhaustion / DoS (Medium) — **MITIGATED**
- Inbound message size capped (`max_decoding_message_size`) — already present.
- **Added:** finite per-connection caps — `max_concurrent_streams` (256) and
  `concurrency_limit_per_connection` (256) in `ServerBuilderOptions`, so one client
  cannot open unbounded streams / in-flight requests.
- **Memory:** a bounded memory pool already exists (`SAIL_RUNTIME__MEMORY_POOL__TYPE
  =fair|greedy` + `..._MAX_SIZE`); the *default* is `unbounded`. **Production
  deployments should set a bounded pool** (documented in SECURITY.md). We keep the
  default unbounded to avoid silently capping legitimate large analytics queries.
- **Still open (operability, not default-on for analytics):** a per-query wall-time
  budget — risky to default since long queries are legitimate. Tracked in
  PRODUCTION_READINESS §4 (reliability) as a configurable knob.

### D1 — Dependency CVEs (was High) — **FIXED**
`cargo audit` initially reported **4 vulnerabilities**, all in `astral-tokio-tar`
0.5.6 (PAX desync, symlink chmod, PAX-extension validation) — pulled in **only via
`testcontainers` (a dev-dependency)**, i.e. **not in the shipped release binary**.
Resolved by bumping `testcontainers` 0.26.3 → 0.27.3, which moves
`astral-tokio-tar` to 0.6.2 (and removes `rustls-pemfile`). `cargo audit` now
reports **0 vulnerabilities**.

### D2 — Unmaintained transitive crates (Low, accepted)
`json` 0.12.4, `paste` 1.0.15, `proc-macro-error` 0.4.12 are flagged unmaintained
(not vulnerable). All transitive/build-time. Accepted for now; revisit if a CVE is
filed or an upstream drops them. The weekly `cargo audit` schedule will catch any
escalation.

---

## Remediation priority
1. ~~**F1** constant-time token compare~~ — **done**.
2. ~~**F3 / F4** Web UI default-localhost + refuse-token-without-TLS~~ — **done**.
3. ~~**F2** reflection off when auth enabled~~ — **done**.
4. ~~**F6** connection caps + memory-pool guidance~~ — **done** (per-query wall-time
   budget remains as a configurable operability knob, PRODUCTION_READINESS §4).
5. Keep the `cargo audit` + `cargo deny` CI gate green (D1 fixed; D2 watched) — ongoing.

All code-level findings (F1–F6) are addressed. Remaining for GA are the items this
review did **not** cover (below): a real **pentest**, **fuzzing**, and the
driver↔worker / catalog-credential authz review.

## What this review did NOT cover (still required for GA)
- A real **penetration test** (third-party or dedicated internal red-team).
- **Fuzzing** the SQL parser and the Connect/protobuf decode path.
- Authn/z review of the **driver↔worker** channel and the **catalog/object-store**
  credential flows.
- Secrets-in-logs audit under real workloads.
