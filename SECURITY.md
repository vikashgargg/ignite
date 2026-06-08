# Security Policy

## Status
Vajra is **pre-1.0 (`v0.6.0-alpha`)**. It has security *features* (Bearer-token
auth, TLS/mTLS) but has **not yet had a third-party penetration test or formal
security audit**. Treat it as not-yet-hardened: run it on **trusted networks
behind your own authentication/perimeter** until the §3 items in
[docs/PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md) are complete. The
internal threat model and current findings are in [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md).

## Supported versions
Until 1.0, only the latest `main` / latest release receives security fixes.

## Reporting a vulnerability
**Do not open a public issue for security problems.**

- Use **GitHub → Security → "Report a vulnerability"** (private advisory) on this
  repository, or
- email the maintainer with a description, affected version/commit, reproduction
  steps, and impact.

We aim to acknowledge within **72 hours**, agree a disclosure timeline, fix in a
private branch, and credit reporters who wish to be credited. Please allow
**coordinated disclosure** before publishing details.

## Automated supply-chain gates
Every push/PR (and a weekly schedule) runs `.github/workflows/security.yml`:
- **`cargo audit`** — RUSTSEC CVE scan of the dependency tree.
- **`cargo deny`** — advisories + license policy + banned crates + source allow-list
  (config: [`deny.toml`](deny.toml)).

Run them locally with `cargo audit` and `cargo deny check`.

## Hardened deployment (until GA)
Enforced defaults (as of the security pass):
- **A token without TLS is refused at startup.** Set `--tls-cert/--tls-key` when
  using `--auth-token`; the token rides in request metadata and is only
  confidential over TLS. Override only on a trusted network with
  `SAIL_AUTH__ALLOW_INSECURE_TOKEN=true`.
- **The Web UI binds to `127.0.0.1:4040` by default** (it is unauthenticated).
  Expose it deliberately with `SAIL_UI__HOST=0.0.0.0` behind your own network
  policy, or disable it with `SAIL_UI__ENABLED=false`.
- **gRPC reflection is disabled automatically when an auth token is set**, so a
  secured server does not allow anonymous schema enumeration.

Still recommended:
- Use **mTLS** (`--tls-ca`) for service-to-service trust where possible.
- Do not expose the gRPC port to untrusted networks.
- Run with least-privilege OS/K8s permissions and per-pod resource limits
  (per-query resource limits are not yet enforced — see THREAT_MODEL.md F6).
