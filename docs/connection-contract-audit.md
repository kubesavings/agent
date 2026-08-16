# Agent ↔ Backend connection contract — audit and proposal

Scope: the wire contract between `kubesavings-agent` and the KubeSavings backend —
`proto/kubesavings.proto`, the agent's `collector.rs`/`sender.rs`, and the backend's
`POST /api/clusters/{id}/snapshot` ingest plus `app/services/analysis/`.

Status: **audit + proposal. No behaviour is changed by this document.**

---

## 1. How the contract works today

```
collector.rs ──► AgentSnapshot (protobuf) ──► POST /api/clusters/{id}/snapshot
                                                 │  X-Api-Key: <cluster key>
                                                 │  Content-Type: application/x-protobuf
                                                 ▼
                                      stored verbatim as MetricSnapshot.raw_data (JSON text)
                                                 │
                                                 ▼  (daily job, 02:00 UTC — not per snapshot)
                                      analyze_latest_snapshot() ──► Recommendation rows
```

Two wire formats share one endpoint: protobuf (what the agent sends) and JSON. Both
are funnelled through the same Pydantic `AgentSnapshot` bound-check, then stored as
JSON. Ingestion only refreshes denormalised cluster metadata; recommendations are
recomputed by the daily job.

### 1.1 The zero-value convention

The protobuf contract defines `0` / `""` / `false` as "not present". betterproto's
`to_dict()` omits zero-valued fields, so over protobuf an unset field is an *absent
key*; over JSON it is an *explicit zero*. The backend already compensates for this in
one place (`normalize_workload_limits`, which rewrites a JSON `0` limit to `None`).

This convention is the root cause of several findings below: for the `actual_*` usage
fields, `0` legitimately means both **"measured zero"** and **"no data"**, and nothing
in the contract distinguishes them.

---

## 2. Field-by-field state

Legend: ✅ populated / consumed · ⚠️ populated but the semantics do not match ·
❌ never populated / never read.

### `AgentSnapshot`

| # | Field | Agent | Backend | Verdict |
|---|---|---|---|---|
| 1 | `k8s_version` | ✅ | ✅ cluster metadata, `outdated-k8s` | OK |
| 2 | `node_count` | ✅ | ✅ cluster metadata | OK |
| 3 | `cloud_provider` | ✅ | ✅ metadata + pricing lookup | OK |
| 4 | `workloads` | ✅ | ✅ | OK |
| 5 | `namespaces` | ✅ | ✅ | OK |
| 6 | `estimated_cluster_cost_usd` | ✅ | ✅ `cluster.total_monthly_cost_usd` | OK |
| 7 | `collected_at` | ✅ RFC3339 | ❌ **discarded** — ingest writes `utcnow()` | dead |
| 8 | `region` | ✅ | ✅ | OK |
| 9 | `node_pools` | ✅ | ✅ `cloud_pricing` | OK |
| 10 | `agent_version` | ✅ | ✅ `outdated-agent` | OK |

### `WorkloadMetrics`

| # | Field | Agent | Backend | Verdict |
|---|---|---|---|---|
| 1–5 | `namespace`…`replicas` | ✅ | ✅ | OK |
| 6–9 | requests / limits | ✅ | ✅ (`0` limit → `None`) | OK |
| 10 | `actual_cpu_avg_m` | ⚠️ instantaneous | ✅ idle gate | see F5 |
| 11 | `actual_cpu_p95_m` | ⚠️ **= avg, not a P95** | ✅ right-sizing | see F5 |
| 12 | `actual_memory_avg_mi` | ⚠️ instantaneous | ❌ never read | dead |
| 13 | `actual_memory_p95_mi` | ⚠️ **= avg, not a P95** | ✅ right-sizing | see F5 |
| 14 | `observation_days` | ⚠️ **workload age** | ✅ read as observation window | see F4 |
| 15 | `estimated_monthly_cost_usd` | ✅ | ❌ never read (backend recomputes) | dead |
| 16 | `last_active_timestamp` | ❌ always `""` | ❌ never read | dead |
| 17–27 | HPA / CronJob / KEDA block | ❌ never populated | ✅ three whole categories | see F3 |

---

## 3. Findings

### F1 — `main` does not build (blocker, agent repo)

CI on `kubesavings/agent@main` has been red since 2026-08-10 (runs `31424418311`,
`31464432065`, `31464439062`). `Cargo.toml` pins `k8s-openapi = "0.24"` with feature
`v1_29`, while the Dependabot bump to `kube = "4.2"` pulls `k8s-openapi 0.28`
transitively. Both versions end up in the tree, the version feature is enabled only on
`0.24`, and `0.28`'s build script aborts:

```
None of the v1_* features are enabled on the k8s-openapi crate.
```

Nothing has shipped from `main` since. **Every other item in this document is blocked
behind this one.**

Fixing it is not a one-line version bump. `k8s-openapi 0.28` supports `v1_32`…`v1_36`
only, so the agent's minimum supported Kubernetes moves **1.29 → 1.32**; and `0.28`
replaced `chrono::DateTime<Utc>` with `jiff::Timestamp` inside `metav1::Time`, which
breaks five call sites in `collector.rs` (`days_since` at lines 358, 404, 449, 590,
764). Verified locally: with `k8s-openapi = { version = "0.28", features = ["v1_32"] }`
the k8s-openapi build script passes and the five `jiff` type errors are all that
remain.

Both halves are decisions, not mechanics — dropping K8s 1.29–1.31 support needs a
call. Raised here rather than fixed silently.

### F2 — `sender.rs` tests drifted from the `SnapshotResponse` schema

Commit `a473faf` fixed a genuine wire bug (the response layout gained `status = 1`,
shifting `recommendations` to 2 and `total_savings_usd` to 3) but did not update the
tests in the same file. `src/sender.rs` still builds the response as:

```rust
fn ok_response(recommendations: i64, savings: f64) -> Vec<u8> {
    SnapshotResponse { recommendations, total_savings_usd: savings }.encode_to_vec()
}
```

The generated struct is `{ status: String, recommendations: u32, total_savings_usd: f64 }`,
so this is a missing-field error plus an `i64`/`u32` mismatch. This is by inspection of
the generated code, not from a green compile — F1 stops the build before the
`#[cfg(test)]` modules are type-checked, which is exactly why it went unnoticed.

Expect it to surface the moment F1 is fixed.

### F3 — Three recommendation categories are dead in production

`WorkloadMetrics` fields 17–27 (HPA, CronJob and KEDA context) are fully specified,
generated on both sides, and consumed by `_check_hpa_misconfig`, `_check_cronjob_waste`
and `_check_keda_idle`. The agent never populates any of them — `build_workload_metrics`
closes with `..Default::default()` and a comment saying so.

Every one of those checks starts with a guard (`if not wl.get("has_hpa")`,
`if not wl.get("keda_scaled")`, `cronjob_recent_failures == 0`) so all three return
empty for every cluster, always. Migration `0004_autoscaler_categories` shipped the
enum values; `hpa-misconfig`, `cronjob-waste` and `keda-idle` have never produced a
row and cannot until the collector fills these fields.

**This is the single highest-value item and it needs no contract change** — the
contract is already there and already correct.

### F4 — `observation_days` means two different things

The agent computes it from the workload's `creationTimestamp` (`days_since(...)` at
`collector.rs:354/400/445`) — it is **workload age**.

The backend reads it as **length of the observation window** and gates on it:
`ANALYZER_RIGHTSIZING_MIN_OBS_DAYS = 3`, `ANALYZER_IDLE_MIN_OBS_DAYS = 7`,
`ANALYZER_HPA_MIN_OBS_DAYS = 3`. It then prints it back to the user verbatim:

> "…P95 usage is only 12.3m (4% utilization) **over 200 days of observation**."

A Deployment created 200 days ago that the agent first saw ten minutes ago reports
`observation_days = 200`. Every gate opens immediately, and the recommendation text
claims 200 days of evidence behind a single reading.

### F5 — The P95 fields are not percentiles, and the analysis ignores stored history

`collector.rs` reads instantaneous per-pod usage from metrics-server and assigns the
same number to both the average and the P95:

```rust
// For instantaneous metrics-server data, avg ≈ p95 (single snapshot point).
let (actual_cpu_avg_m, actual_cpu_p95_m, ...) = { let cpu = u.cpu_per_pod(); (cpu, cpu, ...) };
```

Meanwhile `analyze_all_active_clusters` → `analyze_latest_snapshot` reads **only the
most recent snapshot**. The backend retains 90 days of snapshots (365 on Pro) and uses
exactly one of them. So right-sizing — the headline recommendation — rests on one
instantaneous sample, described to the user as a P95 over N days (F4).

The data needed to compute a real P95 is already in the database. It is a backend
change, not an agent one; the agent is a stateless CronJob and cannot hold history.

### F6 — `0` usage is ambiguous, and the two wire formats disagree because of it

`collector.rs` degrades to zero usage when metrics-server is unavailable. Nothing in
the snapshot says whether metrics-server answered, so a cluster without it reports
every container at 0m CPU — indistinguishable from genuinely idle.

The two formats then diverge, because the backend's idle check defaults an *absent*
key to a deliberately non-idle sentinel:

```python
cpu_avg = _safe_float(wl.get("actual_cpu_avg_m", 999.0), default=999.0)
```

* **protobuf** — `0.0` is omitted by betterproto → key absent → `999.0` → never idle.
  A genuinely idle workload also produces no right-sizing rec (`cpu_p95 > 0` gate),
  so it yields **no recommendation at all**.
* **JSON** — explicit `0.0` → below the 5m threshold → **every** workload flagged
  idle when metrics-server is missing.

Same cluster, same collector, opposite results depending on `Content-Type`. This is the
same class of bug `normalize_workload_limits` was written to patch for the limit
fields, unfixed for the usage fields.

### F7 — `collected_at` is sent and thrown away

The agent stamps `Utc::now().to_rfc3339()`; ingest stores `collected_at=utcnow()`, the
*receipt* time. The field is validated by the Pydantic model and then never read.

Mostly harmless today — but it means snapshot ordering, the free-plan throttle and any
future time-series work key off server receipt rather than collection time, so a
delayed or retried CronJob run is indistinguishable from a fresh one.

### F8 — Two cost models, inconsistently applied

The agent computes `estimated_monthly_cost_usd` per workload with hardcoded rates
(`$0.048`/vCPU-h, `$0.006`/GB-h). The backend never reads it (F2 table, field 15) —
it recomputes with `_workload_cost_usd`, preferring node-pool-derived rates from
`cloud_pricing` when available.

Except for **`zombie-namespace`**: `_check_zombie_namespace` takes no `pricing`
argument and uses `NamespaceMetrics.estimated_monthly_cost_usd` — the agent's
hardcoded figure — directly as the savings number. So one category prices at flat
defaults while every other category prices at real instance rates, on the same
dashboard and in the same headline total.

### F9 — Public API reference is behind the schema

`templates/docs_api_reference.html` documents the snapshot body as `k8s_version`,
`node_count`, `cloud_provider`, `workloads[]`, `namespaces[]`,
`estimated_cluster_cost_usd`, `collected_at`. Missing: `region`, `node_pools`,
`agent_version` — and it is the page aimed at people building a custom collector.

---

## 4. Proposed changes

Ordered by dependency. **Nothing new should be added to the schema until the eleven
fields already in it are populated** — the contract's problem today is unfilled
capacity, not missing capacity.

### Stage 0 — Unblock (agent)

1. Resolve F1. Requires an explicit decision on dropping K8s 1.29–1.31 (see §5).
2. Fix the `sender.rs` tests (F2) in the same change; they are the contract's only
   regression test.

*Verify: `cargo test --all-targets --locked` green, CI green on `main`.*

### Stage 1 — Fill the contract that already exists (agent only, no proto change)

Populate `WorkloadMetrics` 17–27 in `collector.rs`:

| Fields | Source | RBAC needed |
|---|---|---|
| `has_hpa`, `hpa_min_replicas`, `hpa_max_replicas`, `hpa_target_cpu_pct`, `hpa_current_replicas` | `autoscaling/v2` HPA, matched to workload via `spec.scaleTargetRef` | `horizontalpodautoscalers` (get/list) |
| `cronjob_suspended`, `cronjob_last_schedule_ts`, `cronjob_recent_failures` | `batch/v1` CronJob `spec.suspend`, `status.lastScheduleTime`; failures from owned Jobs | `cronjobs`, `jobs` (get/list) |
| `keda_scaled`, `keda_min_replicas`, `keda_trigger_types` | `keda.sh/v1alpha1` ScaledObject (CRD — absent on most clusters, must degrade silently) | `scaledobjects` (get/list, optional) |

Activates `hpa-misconfig`, `cronjob-waste` and `keda-idle` for every existing customer
with no backend change and no schema change. The Helm ClusterRole needs the new
read verbs.

*Verify: a fixture cluster with one HPA-managed over-provisioned Deployment produces
exactly one `hpa-misconfig` row.*

### Stage 2 — Fix the misleading semantics

**2a. Move the observation window to the backend (F4, F5).**

The backend owns the history; the agent cannot. Concretely:

* Agent keeps reporting what it actually knows, under an honest name — add
  `workload_age_days` and stop overloading `observation_days`.
* Backend derives the real observation window per workload from distinct snapshot
  days in `metric_snapshots`, and computes `actual_*_p95_*` as a true percentile over
  that series, falling back to the agent's instantaneous value only while fewer than
  `ANALYZER_RIGHTSIZING_MIN_OBS_DAYS` of history exist.
* Recommendation text then quotes a window the system can actually defend.

Note this makes `metric_snapshots.raw_data` a query target. It is `Text`, not `JSONB`
(per `CLAUDE.md`), so this needs either a JSONB migration or a narrow extracted
per-workload metrics table. That is the main cost of Stage 2 and worth sizing before
committing.

**2b. Disambiguate zero (F6).** Add to `AgentSnapshot`:

```proto
  bool metrics_available = 11;  // metrics-server answered; false ⇒ every actual_* is "no data", not zero
```

Backend: when `metrics_available` is false, skip idle and right-sizing entirely rather
than inferring from zeros, and surface "metrics-server not detected" on the cluster
page — today that misconfiguration is invisible to the user. Also normalise the
JSON/protobuf split the same way `normalize_workload_limits` does, so both formats
behave identically.

**2c. Housekeeping.** Mark `last_active_timestamp = 16` and
`WorkloadMetrics.estimated_monthly_cost_usd = 15` `reserved` (never read, and 15
duplicates a calculation the backend owns); or populate 16 and use it as the
per-workload activity signal. Either is fine — carrying them as live-looking fields is
not. Persist `collected_at` as a distinct column from receipt time (F7). Route
`zombie-namespace` through `cloud_pricing` like every other category (F8). Refresh the
public API reference (F9).

### Stage 3 — New metadata

Only after Stage 1. Each entry is justified by a capability the backend cannot deliver
today, not by "might be useful later".

**3a. `NodePool` — real capacity.** Currently a pool carries only instance type,
region, capacity type and count, so cluster-level waste is invisible: the backend can
price nodes but cannot see how much of them is unused.

```proto
  uint32 cpu_allocatable_m    = 5;
  uint32 memory_allocatable_mi = 6;
  uint32 cpu_capacity_m        = 7;
  uint32 memory_capacity_mi    = 8;
```

Unlocks bin-packing / underutilised-node recommendations — typically a larger number
than per-container right-sizing — and lets the backend sanity-check its cost estimate
against real capacity instead of summing requests.

**3b. `WorkloadMetrics` — right-sizing guardrails.** Right-sizing currently recommends
shrinking memory with no safety signal at all:

```proto
  uint32 restart_count    = 28;  // container restarts in the observation window
  uint32 oom_killed_count = 29;  // OOMKilled terminations
  string qos_class        = 30;  // "Guaranteed" | "Burstable" | "BestEffort"
  uint32 pdb_min_available = 31; // PodDisruptionBudget floor; 0 = none
```

* `oom_killed_count` / `restart_count` — never recommend a memory reduction for a
  container that has been OOMKilled. This is a live correctness gap: today the
  recommendation can only make it worse.
* `qos_class` — a `Guaranteed` pod requires `request == limit`; a request-only
  recommendation silently demotes it to `Burstable` and changes its eviction priority.
* `pdb_min_available` — `idle-workload` remediates with `--replicas=0`, which a PDB
  will block. Either suppress the rec or say so in the remediation.

**3c. `AgentSnapshot` — collection health.**

```proto
  uint32 collection_duration_ms = 12;
  uint32 partial_failures       = 13;  // namespaces skipped due to API errors
```

The collector already degrades gracefully past API errors, so a partial snapshot is
today indistinguishable from a complete one — on either side. With these, "cluster
cost dropped 40%" can be told apart from "half the namespaces failed to list".

Suggested field numbers assume nothing else lands first; renumber on merge.

---

## 5. Open questions

1. **Minimum Kubernetes version.** Fixing F1 via `k8s-openapi 0.28` drops support for
   K8s 1.29–1.31 (0.28 offers `v1_32`…`v1_36` only). Pinning `kube` back to a 0.24-
   compatible release keeps the range but reverts the Dependabot bump and any fixes in
   it. Needs a product call.
2. **Stage 2a storage.** True P95 over history requires `raw_data` to be queryable —
   JSONB migration, or a narrow extracted metrics table? Sizing this is prerequisite to
   committing to 2a.
3. **Dead fields 15 and 16** — reserve, or populate and use? Reserving is cheaper;
   populating 16 gives per-workload activity, which nothing provides today.
4. **KEDA CRD access.** Stage 1's ScaledObject read touches a CRD that most clusters
   do not have. Confirm the collector treats a missing CRD as "not scaled" rather than
   as a collection failure, and that the ClusterRole tolerates the absent resource.
