<!-- Thanks for contributing to Zelox! Keep changes prod-grade and honest. -->

## Summary

<!-- What does this PR do and why? Link the issue / board ticket (e.g. ZELOX-D1). -->

Closes #

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Performance / benchmark
- [ ] Docs / knowledge base
- [ ] CI / release / infra
- [ ] Refactor / cleanup (no behavior change)

## Checklist

- [ ] `cargo clippy --all-targets -- -D warnings` is clean (workspace denies `unwrap`/`expect`/`panic`).
- [ ] `cargo fmt --check` passes.
- [ ] Tests added/updated and passing (`cargo test`); streaming changes cite the matrix cell they advance
      in `docs/STREAMING_ARCHITECTURE.md` and meet its done-criteria.
- [ ] New physical-plan fields round-trip in `zelox-execution/src/codec.rs` (or logged as a single-node gap).
- [ ] Docs updated (CODEMAP / STATUS / BENCHMARKS / CHANGELOG) where relevant.
- [ ] Performance/competitive claims are **measured head-to-head** and path-dependence is flagged
      (per the honest-claims bar) — no estimated or unqualified "beats Spark/Flink" claims.

## Testing / measurement

<!-- How was this verified? Include commands, benchmark numbers (with scale + conditions), or gate output. -->
