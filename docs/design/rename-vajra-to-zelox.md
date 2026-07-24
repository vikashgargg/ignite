# RFC: Rename Vajra → Zelox (product) + `zelox-*` → `zelox-*` (crates) + new repo

**Status:** PLANNED (not started). **Owner:** TBD. **Trigger:** run as a dedicated migration branch
**after** the in-flight streaming perf work (T7_FUSE + jemalloc, branch `throughput/from-json-tape-e2e-verify`)
merges — so we do not invalidate the `t7jp` image or break the kind/EKS confidence run mid-flight.

## 1. Why

Zelox is a **new product**, not a Sail redistribution. We forked LakeSail/Sail as a starting point but
have diverged substantially (streaming engine, EO/checkpoint, T7 fusion, memory discipline). The `zelox-*`
crate namespace and the `vajra` working name both need to become the product identity **Zelox**, in a new
repository. Per AIM.md this is a synthesize-not-copy product; the rename makes the divergence explicit.

## 2. Naming decisions (authoritative mapping)

| Layer | From | To | Notes |
|---|---|---|---|
| Product / brand (docs, UI) | Vajra / vajra | Zelox / zelox | prose + headings |
| Crate dirs + names | `crates/zelox-<x>` / `zelox-<x>` | `crates/zelox-<x>` / `zelox-<x>` | 37 crates; `git mv` to keep history |
| Rust lib paths (imports) | `sail_<x>` | `zelox_<x>` | ~8.7k `use` / path refs — the bulk |
| Binary | `vajra` | `zelox` | `zelox-cli` → `zelox-cli`, `[[bin]] name` |
| Runtime env prefix | `ZELOX_*` | `ZELOX_*` | e.g. `ZELOX_T7_FUSE` → `ZELOX_T7_FUSE` |
| Config env prefix | `ZELOX_*` | `ZELOX_*` | `ZELOX_RUNTIME__…`, `ZELOX_MODE` |
| Config file namespace | `sail` keys in `application.yaml` | `zelox` | + the loader prefix |
| Container image / ECR repo | `vajra` | `zelox` | new ECR repo `zelox`; keep `vajra` until cutover |
| k8s resources | `vajra-stream`, `vajra-client` | `zelox-stream`, `zelox-client` | namespace `stream` unchanged |
| GitHub repo | (current) | new repo `zelox` | see §7 |

Convention: hyphen form `zelox-<x>` for crate names/dirs/deps; underscore `zelox_<x>` for Rust paths;
UPPER `ZELOX_` for env. **`zelox-build-scripts` → `zelox-build-scripts`** (hyphenated, not `zeloxbuild-`).

## 3. Scope (measured 2026-07-20, excl. `target/`,`.git/`)

- 37 `zelox-*` crates.
- `sail_` (Rust paths): **8,681** occ / 462 files — largest bucket.
- `zelox-` (Cargo deps, dirs): **1,141** / 176 files.
- `ZELOX_`: **376** / 102 · `ZELOX_`: **406** / 107.
- `vajra`: **1,145** / 162 · `Vajra`: **993** / 200.
- **Total ≈ 12,700 occurrences / ~500 files.** ⇒ scripted + compiler-verified, never hand-edited.

## 4. Sequencing (do NOT interleave with feature work)

1. Land + merge the current perf branch (T7_FUSE + jemalloc) and any EKS confirm. Freeze other feature PRs.
2. Cut a single dedicated branch `chore/rename-zelox`. No other changes ride along.
3. Execute §5 in one mechanical pass; land as one squashed-history-preserving migration.
4. Cut over infra (ECR repo, images, CI, repo) in §6/§7.
5. Update agent memory + CLAUDE.md + AIM.md brand last.

## 5. Mechanical migration procedure (tooled, reversible per-step)

Order matters: dirs → crate names → deps → Rust paths → env → brand → infra. Compile after each phase.

1. **Crate dirs (history-preserving):** for each `crates/zelox-<x>`: `git mv crates/zelox-<x> crates/zelox-<x>`.
2. **Crate names + deps (Cargo):** in every `Cargo.toml`, rename `name = "zelox-<x>"` → `"zelox-<x>"` and
   every `zelox-<x> = { … }` / `zelox-<x>.workspace` dependency key. Update root `Cargo.toml` members glob.
   Rename the bin: `zelox-cli` → `zelox-cli`, `[[bin]] name = "vajra"` → `"zelox"`.
3. **Rust paths:** repo-wide `sail_<x>` → `zelox_<x>` (word-boundary regex; the 8.7k bucket). `cargo build`
   is the verifier — a missed ref fails to compile, so this phase is self-checking.
4. **Env vars:** `ZELOX_` → `ZELOX_` and `ZELOX_` → `ZELOX_` across code + yaml + scripts. **Add a
   back-compat shim** in the config/env loader that still reads the old prefixes for one release with a
   deprecation warning (prod-grade: don't strand existing deployments).
5. **Config namespace:** `application.yaml` `sail:` root → `zelox:`; update the config loader prefix.
6. **Brand prose:** `Vajra`/`vajra` → `Zelox`/`zelox` in `docs/`, `README`, `CLAUDE.md`, `AIM.md`,
   comments. Exclude historical references to upstream *Sail/LakeSail* (keep attribution accurate).
7. **Scripts + manifests:** rename script files (`git mv`), k8s resource names, image tags.

**Verification gates (each must pass before merge):** `cargo build --workspace`, `cargo clippy
--all-targets -D warnings`, full test suite, T1 correctness gate, `grep -rI 'sail_\|zelox-\|ZELOX_\|vajra'`
returns only intentional upstream-attribution hits.

## 6. Infra cutover

- New ECR repo `zelox`; build `zelox:<tag>` images; keep `vajra` repo until all clusters cut over, then
  delete. Update `scripts/eks_build_image.sh` repo name + `docker/Dockerfile` bin path.
- CI/workflow files: image names, cache keys, artifact names.
- Helm/k8s: chart name, release name, resource labels.

## 7. New repository

- **Preferred:** create the new repo `zelox` and push the renamed tree (fresh start for a new product
  identity, as requested). Preserve full history via a normal push of the migrated branch.
- Archive/redirect the old repo. Update remotes, badges, docs links, `Co-Authored-By` unaffected.
- Decide upstream relationship: renaming crates **diverges from LakeSail/Sail**, making future upstream
  cherry-picks harder. Accepted tradeoff (Zelox is a distinct product); record any still-tracked upstream
  modules in `docs/REFERENCES.md`.

## 8. Risks & mitigations

- **In-flight images/clusters** — do the rename only when no perf run is live; the `t7jp`/`vajra:*` images
  keep working under the old name until cutover.
- **Env-var breakage for existing deploys** — the §5.4 back-compat shim (read old prefix + warn) for one
  release.
- **Merge conflicts with open branches** — freeze feature work; rebase the rename last.
- **Upstream divergence** — accepted; document tracked modules.
- **Partial rename** — the compiler + the §5 grep gate make a half-done rename impossible to merge green.
