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
| F1 | Medium | Non-constant-time Bearer token compare (timing side-channel) | open |
| F2 | Low–Med | gRPC reflection + health are unauthenticated | open |
| F3 | Medium | Web UI binds `0.0.0.0:4040`, no auth, no TLS | open |
| F4 | Medium | Bearer token accepted over cleartext (no TLS requirement) | open |
| F5 | Low | Insecure-by-default (no auth unless a token is set) | doc |
| F6 | Medium | No per-query resource/time limits or connection caps (DoS) | partial |
| D1 | High→**Fixed** | 4 RUSTSEC vulns in `astral-tokio-tar` (dev-dep) | **fixed** |
| D2 | Low | 3 unmaintained transitive crates (json, paste, proc-macro-error) | accepted |

### F1 — Non-constant-time token comparison (Medium)
`crates/sail-spark-connect/src/entrypoint.rs:57` compares the presented Bearer
token with `provided == expected` (plain `String` equality, short-circuits on
first differing byte). This leaks token bytes via response-timing to an attacker
who can make many requests.
**Fix:** constant-time compare (e.g. `subtle::ConstantTimeEq` or
`constant_time_eq`). Small, isolated change.

### F2 — Unauthenticated reflection + health (Low–Medium)
The Bearer interceptor wraps only the Spark Connect service
(`InterceptedService::new(configured, interceptor)`), while the gRPC **reflection**
and **health** services are added to the base router (`crates/sail-server/src/builder.rs`)
**without** the interceptor. Reflection exposes the full proto service schema to
anonymous clients (there is already a `// TODO: turn off reflection in production`).
**Fix:** disable reflection in production builds/config, and/or place health +
reflection behind the same auth layer.

### F3 — Web UI exposed, unauthenticated (Medium)
`crates/sail-spark-connect/src/web_ui.rs:124` binds `0.0.0.0:4040` with no auth and
no TLS, and it starts unconditionally. It can disclose query plans, schemas, and
metrics to anyone who can reach the port.
**Fix:** bind to `127.0.0.1` by default (configurable), make it opt-in, and put it
behind auth/TLS when enabled. Document the exposure until then (done in SECURITY.md).

### F4 — Bearer token over cleartext (Medium)
The token interceptor functions regardless of TLS, so with TLS off the token
travels in request metadata in the clear and is trivially sniffable/replayable.
**Fix:** refuse to start (or loudly warn) when an auth token is set without TLS;
document "token ⇒ require TLS." Interim mitigation documented in SECURITY.md.

### F5 — Insecure by default (Low, by design)
With no `--auth-token`/TLS, the server accepts all clients (dev convenience). This
is acceptable for local dev but must be explicit. Documented in SECURITY.md; the
hardened-deployment guidance covers production.

### F6 — Resource-exhaustion / DoS (Medium, partial)
Good: inbound message size is capped (`max_decoding_message_size`). Gaps: no
per-query memory or wall-time limit, no cap on concurrent sessions/connections, so
a hostile or accidental heavy query can exhaust host memory/CPU.
**Fix:** per-query memory pool + time budget; connection/session caps; backpressure.
Tracked in PRODUCTION_READINESS §4.

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
1. **F1** constant-time token compare — quick, do first.
2. **F3 / F4** Web UI default-localhost + require-TLS-with-token — closes the two
   most reachable network exposures.
3. **F2** reflection off in prod.
4. **F6** query resource limits + connection caps (larger; PRODUCTION_READINESS §4).
5. Keep the `cargo audit` + `cargo deny` CI gate green (D1 fixed; D2 watched).

## What this review did NOT cover (still required for GA)
- A real **penetration test** (third-party or dedicated internal red-team).
- **Fuzzing** the SQL parser and the Connect/protobuf decode path.
- Authn/z review of the **driver↔worker** channel and the **catalog/object-store**
  credential flows.
- Secrets-in-logs audit under real workloads.
