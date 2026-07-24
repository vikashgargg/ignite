# Contributing to Zelox

Contributions are more than welcome!

Zelox is a fork of [Sail](https://github.com/lakehq/sail) (by LakeSail, Apache-2.0); see the `NOTICE`
file for attribution. Issues and PRs for **Zelox** belong in this repository.

Please submit [GitHub issues](https://github.com/vikashgargg/zelox/issues) for bug reports and feature
requests (templates will guide you). You are welcome to ask questions in
[GitHub Discussions](https://github.com/vikashgargg/zelox/discussions).

Feel free to open a [pull request](https://github.com/vikashgargg/zelox/pulls) for a code change. The
PR template lists the prod-grade checklist we hold every change to.

## Prod-grade bar

Every change is expected to be production-grade and honest:

- **Lint:** `cargo clippy --all-targets -- -D warnings` must be clean (the workspace denies
  `unwrap`/`expect`/`panic`/`allow` outside test modules). `cargo fmt --check` must pass.
- **Tests:** add or update tests; run `cargo test`. Streaming changes cite the feature-matrix cell they
  advance in [`docs/STREAMING_ARCHITECTURE.md`](docs/STREAMING_ARCHITECTURE.md) and meet its done-criteria.
- **Distributed contract:** a new physical-plan field must round-trip in
  [`zelox-execution/src/codec.rs`](crates/zelox-execution/src/codec.rs), or be logged as a single-node gap.
- **Honest claims:** performance/competitive claims must be **measured head-to-head** with the scale and
  conditions stated, and path-dependence flagged — no estimated or unqualified "beats Spark/Flink" claims.
- **Docs:** update `CHANGELOG.md` and the relevant knowledge-base docs (CODEMAP / STATUS / BENCHMARKS)
  in the same change.

See [`docs/CODEMAP.md`](docs/CODEMAP.md) to orient in the codebase.
