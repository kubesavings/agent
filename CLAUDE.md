# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`kubesavings-agent` is a Rust binary that runs as a Kubernetes CronJob. On each execution it: loads config from env vars → collects cluster metrics via the K8s API → serializes to protobuf → POSTs to the KubeSavings API.

## Commands

```bash
cargo build                          # debug build
cargo build --release                # release build (musl target for Docker: see Dockerfile)
cargo test                           # run all tests
cargo test <test_name>               # run a single test by name (substring match)
cargo test -- --nocapture            # show println output in tests
cargo bench                          # run criterion benchmarks (benches/collector_bench.rs)
cargo clippy -- -D warnings          # lint
cargo fmt                            # format
RUST_LOG=debug cargo run             # run locally (requires KUBES AVINGS_API_KEY etc. in env)
```

## Architecture

The execution flow is strictly linear — `main.rs` calls three modules in sequence:

1. **`config.rs`** — Reads all config from environment variables. `Config::from_env()` validates the endpoint (strips injected paths, rejects non-https except localhost) and the cluster ID (UUID chars only, ≤36 chars). No config file; env-only.

2. **`collector.rs`** — Queries the live K8s API. Key design points:
   - Resolves pod → workload owner by tracing `ownerReferences`: Pod → ReplicaSet → Deployment (pre-fetches the RS→Deployment map per namespace to avoid per-pod API calls).
   - Fetches instantaneous usage from `metrics-server` at `/apis/metrics.k8s.io/v1beta1/pods`; gracefully degrades to zero-usage if metrics-server is unavailable.
   - Namespace activity is determined by the most-recent pod start time, supplemented by K8s Event `lastTimestamp`. Events have ~1h TTL by default, so pods are the primary signal.
   - Cost estimates use hardcoded rates: `CPU_COST_PER_VCPU_HOUR = $0.048`, `MEM_COST_PER_GB_HOUR = $0.006`. Monthly cost = request-based (not actual usage).
   - Cloud provider auto-detected from node labels (EKS/GKE/AKS) if not set via env.

3. **`sender.rs`** — Encodes `AgentSnapshot` as protobuf and POSTs to `{api_endpoint}/api/clusters/{cluster_id}/snapshot`. Uses exponential backoff (5s → 20s → 80s, capped at 120s, 3 attempts). Returns 401 immediately without retry.

4. **`types.rs`** — Re-exports prost-generated types from `OUT_DIR/kubesavings.v1.rs`. Protobuf schema is compiled at build time via `build.rs` and `prost-build`. **Edit the `.proto` file, not the generated code.**

## Configuration (Environment Variables)

| Variable | Required | Default |
|---|---|---|
| `KUBESAVINGS_API_KEY` | Yes | — |
| `KUBESAVINGS_CLUSTER_ID` | Yes (UUID format) | — |
| `KUBESAVINGS_API_ENDPOINT` | No | `https://app.kubesavings.io` |
| `KUBESAVINGS_INCLUDE_NAMESPACES` | No (CSV) | all namespaces |
| `KUBESAVINGS_EXCLUDE_NAMESPACES` | No (CSV) | `kube-system,kube-public,kube-node-lease` |
| `KUBESAVINGS_CLOUD_PROVIDER` | No | auto-detected |
| `RUST_LOG` | No | `info` |

## Release

The chart and the image are versioned **independently**, each from its own
source-of-truth file. Releases are triggered by a push to `main` — no manual git
tags. The workflow (`.github/workflows/release.yml`) detects what changed and runs
only the matching job:

1. **image** — runs when code changes (`src/**`, `proto/**`, `build.rs`,
   `Cargo.toml`, `Cargo.lock`, `Dockerfile`). Version = `Cargo.toml` `version`.
   Builds the multi-stage `Dockerfile` and pushes to GHCR:
   ```
   ghcr.io/<owner>/kubesavings-agent:<version>
   ghcr.io/<owner>/kubesavings-agent:<major>.<minor>
   ```
2. **helm** — runs when `helm/**` changes. Chart version = `helm/Chart.yaml`
   `version`; `appVersion` (the image the chart deploys, kept in `Chart.yaml` and
   `values.yaml`) tracks the image version. Packages the chart and pushes it as an
   OCI artifact to GHCR:
   ```
   ghcr.io/<owner>/charts/kubesavings-agent:<chart-version>
   ```

**Versioning rule:** code change → bump `Cargo.toml` `version` (new image).
Chart change (templates, or pointing `values.yaml`/`appVersion` at a new image) →
bump `helm/Chart.yaml` `version` (new chart). A change that ships a new agent
feature touches both: bump the app version, and bump the chart version + its
`appVersion`/`values.yaml` image tag so users get the new image.

To cut a release, just merge to `main`:
```bash
# edit Cargo.toml version (code) and/or helm/Chart.yaml version (chart), then:
git push origin main
```

**Public visibility (one-time):** GHCR packages are private by default and there
is no API to change visibility, so it must be set once per package (it then sticks
for all future tags). Either set the org default (Settings → Packages → "Package
creation" → Public), or after the first publish open each package's settings →
"Change visibility" → Public.

To install/upgrade from the published chart:
```bash
helm upgrade --install kubesavings-agent \
  oci://ghcr.io/<owner>/charts/kubesavings-agent \
  --version 1.2.3 \
  --set agent.apiKey=<key> \
  --set agent.clusterId=<uuid>
```

## Deployment

- Helm chart in `helm/` deploys as a `CronJob` (default schedule: `0 * * * *` — hourly).
- Docker image is a static musl binary on `scratch`; built with cargo-chef for layer caching.
- RBAC: read-only ClusterRole covering pods, nodes, namespaces, deployments, statefulsets, daemonsets, replicasets, events, and the metrics API.

---

## Behavioral Guidelines

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
