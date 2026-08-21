use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Instant;

use chrono::Utc;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Event, Namespace, Node, Pod};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use k8s_openapi::jiff::Timestamp;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use kube::{Api, Client};
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::types::{AgentSnapshot, NamespaceMetrics, NodePool, WorkloadMetrics};

const CPU_COST_PER_VCPU_HOUR: f64 = 0.048;
const MEM_COST_PER_GB_HOUR: f64 = 0.006;

// Node label keys for cost-pricing metadata. Each has a current key and a legacy
// (pre-1.17 `beta`/`failure-domain`) key kept as a fallback for older nodes.
const LABEL_INSTANCE_TYPE: &str = "node.kubernetes.io/instance-type";
const LABEL_INSTANCE_TYPE_LEGACY: &str = "beta.kubernetes.io/instance-type";
const LABEL_REGION: &str = "topology.kubernetes.io/region";
const LABEL_REGION_LEGACY: &str = "failure-domain.beta.kubernetes.io/region";

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("Kubernetes client error: {0}")]
    Kube(#[from] kube::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Subsystems whose data this pass failed to fetch.
///
/// Every `warn!` in this module marks a list call that returned an error and was
/// swallowed so the rest of the pass could continue. That is the right call — a
/// snapshot missing its PodDisruptionBudgets still beats no snapshot — but it
/// leaves the backend reading absence as measurement: no PDB rows looks exactly
/// like no PDBs existing. Recording the subsystem is what separates the two.
///
/// Deduplicated because the failure is almost always the same permission or the
/// same unreachable apiserver repeating once per namespace, and fifty copies of
/// `pods` tells the reader nothing the first one didn't.
#[derive(Default)]
struct Degradations(Mutex<BTreeSet<&'static str>>);

impl Degradations {
    fn record(&self, subsystem: &'static str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(subsystem);
    }

    /// Sorted by construction — `BTreeSet` gives the wire a stable order, so two
    /// snapshots degraded the same way compare equal instead of by luck.
    fn into_vec(self) -> Vec<String> {
        self.0
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .into_iter()
            .map(str::to_string)
            .collect()
    }
}

/// Resolved workload owner for a pod (traced through ReplicaSet to Deployment if needed).
struct PodOwner {
    workload_type: String,
    workload_name: String,
}

/// Per-workload signals that say whether shrinking its requests is safe.
///
/// Aggregated across the workload's pods in the same pass that resolves owners,
/// so this costs no extra API calls.
#[derive(Default)]
pub(crate) struct WorkloadHealth {
    restart_count: u32,
    oom_killed_count: u32,
    qos_class: String,
    has_pdb: bool,
    pdb_min_available: u32,
}

/// Key: (namespace, workload_type, workload_name).
type HealthMap = HashMap<(String, String, String), WorkloadHealth>;

/// Per-container usage from the metrics-server (instantaneous, current values).
pub(crate) struct ContainerMetric {
    name: String,
    cpu_m: f64,
    mem_mi: f64,
}

/// Aggregated resource usage across all currently-running pods for one (workload, container).
#[derive(Default)]
struct WorkloadUsage {
    cpu_m_total: f64,
    mem_mi_total: f64,
    pod_count: u32,
}

impl WorkloadUsage {
    /// Average CPU per pod (millicores).
    fn cpu_per_pod(&self) -> f64 {
        if self.pod_count == 0 {
            0.0
        } else {
            self.cpu_m_total / self.pod_count as f64
        }
    }

    /// Average memory per pod (MiB).
    fn mem_per_pod(&self) -> f64 {
        if self.pod_count == 0 {
            0.0
        } else {
            self.mem_mi_total / self.pod_count as f64
        }
    }
}

// Key: (namespace, workload_type, workload_name, container_name)
type UsageMap = HashMap<(String, String, String, String), WorkloadUsage>;

/// Parse a Kubernetes CPU quantity to millicores, preserving fractional precision.
///
/// metrics-server reports CPU in nanocores, so a sidecar drawing `"800000n"` is
/// 0.8m. Rounding that to an integer would report the container as completely
/// idle and drive its rightsizing recommendation to zero, so the usage path
/// keeps the fractional value. Requests and limits use
/// [`parse_cpu_to_millicores`], whose integer form matches the protobuf schema.
///
/// Examples: "500m" → 500.0, "2" → 2000.0, "100n" → 0.0001
pub fn parse_cpu_millicores(s: &str) -> f64 {
    let s = s.trim();
    if let Some(m) = s.strip_suffix('m') {
        m.parse().unwrap_or(0.0)
    } else if let Some(n) = s.strip_suffix('n') {
        // nanocores → millicores (1m = 1_000_000n)
        n.parse::<f64>().unwrap_or(0.0) / 1_000_000.0
    } else if let Some(u) = s.strip_suffix('u') {
        // microcores → millicores
        u.parse::<f64>().unwrap_or(0.0) / 1_000.0
    } else {
        // whole cores
        s.parse::<f64>().unwrap_or(0.0) * 1000.0
    }
}

/// Parse a Kubernetes memory quantity to MiB, preserving fractional precision.
/// Handles: Ki, Mi, Gi, Ti, k, M, G, T, and plain bytes.
pub fn parse_memory_mib(s: &str) -> f64 {
    let s = s.trim();
    if let Some(v) = s.strip_suffix("Ti") {
        v.parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0
    } else if let Some(v) = s.strip_suffix("Gi") {
        v.parse::<f64>().unwrap_or(0.0) * 1024.0
    } else if let Some(v) = s.strip_suffix("Mi") {
        v.parse().unwrap_or(0.0)
    } else if let Some(v) = s.strip_suffix("Ki") {
        v.parse::<f64>().unwrap_or(0.0) / 1024.0
    } else if let Some(v) = s.strip_suffix('T') {
        v.parse::<f64>().unwrap_or(0.0) * 1_000_000_000_000.0 / 1_048_576.0
    } else if let Some(v) = s.strip_suffix('G') {
        v.parse::<f64>().unwrap_or(0.0) * 1_000_000_000.0 / 1_048_576.0
    } else if let Some(v) = s.strip_suffix('M') {
        v.parse::<f64>().unwrap_or(0.0) * 1_000_000.0 / 1_048_576.0
    } else if let Some(v) = s.strip_suffix('k') {
        v.parse::<f64>().unwrap_or(0.0) * 1_000.0 / 1_048_576.0
    } else {
        // Plain bytes
        s.parse::<f64>().unwrap_or(0.0) / 1_048_576.0
    }
}

/// Whole millicores, for the `uint32` request/limit fields of the wire schema.
/// Truncates; a float→int cast in Rust saturates, so negatives clamp to 0.
pub fn parse_cpu_to_millicores(s: &str) -> u32 {
    parse_cpu_millicores(s) as u32
}

/// Whole MiB, for the `uint32` request/limit fields of the wire schema.
pub fn parse_memory_to_mib(s: &str) -> u32 {
    parse_memory_mib(s) as u32
}

pub fn monthly_cost(cpu_m: u32, mem_mi: u32, replicas: u32) -> f64 {
    let cpu_vcpu = cpu_m as f64 / 1000.0;
    let mem_gib = mem_mi as f64 / 1024.0;
    (cpu_vcpu * CPU_COST_PER_VCPU_HOUR + mem_gib * MEM_COST_PER_GB_HOUR)
        * replicas as f64
        * 24.0
        * 30.0
}

/// Days elapsed since `ts`, floored to whole days. Returns 0 if ts is in the future.
///
/// Takes a `jiff::Timestamp` because that is what `k8s-openapi` wraps in
/// `metav1::Time` (it moved off `chrono` in 0.28); using the re-export rather
/// than a direct `jiff` dependency keeps the two in lockstep.
fn days_since(ts: &Timestamp) -> u32 {
    (Timestamp::now().duration_since(*ts).as_secs() / 86_400).max(0) as u32
}

/// Map a node's `spec.providerID` to a canonical cloud-provider name.
///
/// The providerID is formatted `<scheme>://<provider-specific-id>`. We take the
/// scheme and normalize the well-known ones (`gce` → `gcp`). An empty providerID
/// (or one without a scheme) yields "" — we report unknown rather than guess.
fn parse_provider_id(provider_id: &str) -> String {
    match provider_id.split_once("://") {
        Some(("aws", _)) => "aws".to_string(),
        Some(("gce", _)) => "gcp".to_string(),
        Some(("azure", _)) => "azure".to_string(),
        Some((scheme, _)) => scheme.to_string(),
        None => String::new(),
    }
}

/// Best-effort spot/preemptible detection from node labels. Treats the node as
/// "spot" if any of the Karpenter / EKS / GKE / AKS spot signals are present,
/// otherwise "on-demand".
fn capacity_type_from_labels(labels: &BTreeMap<String, String>) -> &'static str {
    let is_spot = labels
        .get("karpenter.sh/capacity-type")
        .is_some_and(|v| v == "spot")
        || labels
            .get("eks.amazonaws.com/capacityType")
            .is_some_and(|v| v == "SPOT")
        || labels
            .get("cloud.google.com/gke-spot")
            .is_some_and(|v| v == "true")
        || labels
            .get("kubernetes.azure.com/scalesetpriority")
            .is_some_and(|v| v == "spot");
    if is_spot {
        "spot"
    } else {
        "on-demand"
    }
}

/// Per-node pricing metadata extracted from labels and `spec.providerID`.
struct NodeMeta {
    instance_type: String,
    region: String,
    capacity_type: String,
    provider: String,
}

fn node_meta(node: &Node) -> NodeMeta {
    let empty = BTreeMap::new();
    let labels = node.metadata.labels.as_ref().unwrap_or(&empty);

    let instance_type = labels
        .get(LABEL_INSTANCE_TYPE)
        .or_else(|| labels.get(LABEL_INSTANCE_TYPE_LEGACY))
        .cloned()
        .unwrap_or_default();
    let region = labels
        .get(LABEL_REGION)
        .or_else(|| labels.get(LABEL_REGION_LEGACY))
        .cloned()
        .unwrap_or_default();
    let capacity_type = capacity_type_from_labels(labels).to_string();
    let provider = node
        .spec
        .as_ref()
        .and_then(|s| s.provider_id.as_deref())
        .map(parse_provider_id)
        .unwrap_or_default();

    NodeMeta {
        instance_type,
        region,
        capacity_type,
        provider,
    }
}

/// One node's `[cpu_allocatable_m, memory_allocatable_mi, cpu_capacity_m,
/// memory_capacity_mi]`, zeroed where the node status does not report them.
///
/// `allocatable` is capacity minus kubelet/system reservations and eviction
/// thresholds — the figure a scheduler actually places pods against — so it is
/// the one a packing calculation must divide by. `capacity` comes along because
/// the gap between them is itself worth seeing.
fn node_resources(node: &Node) -> [u32; 4] {
    let status = match node.status.as_ref() {
        Some(s) => s,
        None => return [0; 4],
    };
    let read = |map: Option<&BTreeMap<String, Quantity>>, key: &str, cpu: bool| -> u32 {
        map.and_then(|m| m.get(key))
            .map(|q| {
                if cpu {
                    parse_cpu_to_millicores(&q.0)
                } else {
                    parse_memory_to_mib(&q.0)
                }
            })
            .unwrap_or(0)
    };
    [
        read(status.allocatable.as_ref(), "cpu", true),
        read(status.allocatable.as_ref(), "memory", false),
        read(status.capacity.as_ref(), "cpu", true),
        read(status.capacity.as_ref(), "memory", false),
    ]
}

/// Group nodes into pools keyed by (instance_type, region, capacity_type) and
/// derive the cluster's primary region and cloud provider.
///
/// Returns `(node_pools, primary_region, cloud_provider)`. Nodes missing the
/// instance-type label still count toward a pool (with `instance_type == ""`).
/// The primary region is the most-common non-empty region (ties broken
/// alphabetically for deterministic output); "" if no node reports a region.
/// The cloud provider is the first non-empty providerID-derived value.
pub fn aggregate_node_pools(nodes: &[Node]) -> (Vec<NodePool>, String, String) {
    // Per pool: node count plus running totals of the four resource figures, so
    // the emitted per-node values are an average over the pool's nodes.
    let mut counts: HashMap<(String, String, String), (u32, [u64; 4])> = HashMap::new();
    let mut region_counts: HashMap<String, u32> = HashMap::new();
    let mut provider = String::new();

    for node in nodes {
        let meta = node_meta(node);
        if provider.is_empty() && !meta.provider.is_empty() {
            provider = meta.provider.clone();
        }
        if !meta.region.is_empty() {
            *region_counts.entry(meta.region.clone()).or_default() += 1;
        }
        let entry = counts
            .entry((meta.instance_type, meta.region, meta.capacity_type))
            .or_insert((0, [0; 4]));
        entry.0 += 1;
        let [cpu_alloc, mem_alloc, cpu_cap, mem_cap] = node_resources(node);
        entry.1[0] += cpu_alloc as u64;
        entry.1[1] += mem_alloc as u64;
        entry.1[2] += cpu_cap as u64;
        entry.1[3] += mem_cap as u64;
    }

    let mut node_pools: Vec<NodePool> = counts
        .into_iter()
        .map(
            |((instance_type, region, capacity_type), (node_count, totals))| {
                let per_node = |total: u64| -> u32 {
                    if node_count == 0 {
                        0
                    } else {
                        (total / node_count as u64).min(u32::MAX as u64) as u32
                    }
                };
                NodePool {
                    instance_type,
                    region,
                    capacity_type,
                    node_count,
                    cpu_allocatable_m: per_node(totals[0]),
                    memory_allocatable_mi: per_node(totals[1]),
                    cpu_capacity_m: per_node(totals[2]),
                    memory_capacity_mi: per_node(totals[3]),
                }
            },
        )
        .collect();
    // Deterministic ordering keeps snapshots stable across runs and testable.
    node_pools.sort_by(|a, b| {
        (&a.instance_type, &a.region, &a.capacity_type).cmp(&(
            &b.instance_type,
            &b.region,
            &b.capacity_type,
        ))
    });

    // Most-common region wins; on a tie pick the alphabetically smallest.
    let region = region_counts
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
        .map(|(r, _)| r)
        .unwrap_or_default();

    (node_pools, region, provider)
}

/// Autoscaler context for one workload, as the wire schema's optional fields
/// expect it. Defaults are the proto zero-values, which the backend reads as
/// "no autoscaler present".
#[derive(Default)]
pub(crate) struct AutoscalerContext {
    has_hpa: bool,
    hpa_min_replicas: u32,
    hpa_max_replicas: u32,
    hpa_target_cpu_pct: u32,
    hpa_current_replicas: u32,
    keda_scaled: bool,
    keda_min_replicas: u32,
    keda_trigger_types: String,
}

/// Key: (workload_type, workload_name) within one namespace — the same pair a
/// `scaleTargetRef` names.
type AutoscalerMap = HashMap<(String, String), AutoscalerContext>;

/// Copy a workload's right-sizing guardrails onto its wire metrics. A workload
/// whose pods were never seen keeps the proto zero-values, which the backend
/// reads as "no signal" rather than "safe to shrink".
fn apply_workload_health(wl: &mut WorkloadMetrics, health: Option<&WorkloadHealth>) {
    let Some(h) = health else {
        return;
    };
    wl.restart_count = h.restart_count;
    wl.oom_killed_count = h.oom_killed_count;
    wl.qos_class = h.qos_class.clone();
    wl.has_pdb = h.has_pdb;
    wl.pdb_min_available = h.pdb_min_available;
}

/// Copy a workload's autoscaler context onto its wire metrics. A workload with
/// no HPA and no ScaledObject keeps the proto zero-values.
fn apply_autoscaler_context(wl: &mut WorkloadMetrics, ctx: Option<&AutoscalerContext>) {
    let Some(ctx) = ctx else {
        return;
    };
    wl.has_hpa = ctx.has_hpa;
    wl.hpa_min_replicas = ctx.hpa_min_replicas;
    wl.hpa_max_replicas = ctx.hpa_max_replicas;
    wl.hpa_target_cpu_pct = ctx.hpa_target_cpu_pct;
    wl.hpa_current_replicas = ctx.hpa_current_replicas;
    wl.keda_scaled = ctx.keda_scaled;
    wl.keda_min_replicas = ctx.keda_min_replicas;
    wl.keda_trigger_types = ctx.keda_trigger_types.clone();
}

/// Render a Kubernetes timestamp as the backend parses it: RFC3339, whole
/// seconds, `Z` suffix.
///
/// `Timestamp`'s own `Display` can emit fractional seconds, and the backend
/// parses with Python's `datetime.fromisoformat`, which rejects more than six
/// fractional digits. Formatting explicitly keeps that from ever mattering.
fn format_rfc3339(ts: &Timestamp) -> String {
    ts.strftime("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Non-negative `i32` → `u32`, clamping negatives (which the API never sends) to 0.
fn to_u32(v: i32) -> u32 {
    v.max(0) as u32
}

/// Build the per-namespace autoscaler map from HorizontalPodAutoscalers and KEDA
/// ScaledObjects.
///
/// Both are optional: a cluster with no HPAs, or without KEDA's CRD installed,
/// yields an empty map rather than an error. A KEDA-scaled workload also carries
/// an operator-managed HPA, so it legitimately reports both `has_hpa` and
/// `keda_scaled`.
async fn fetch_autoscaler_context(
    client: &Client,
    ns: &str,
    degraded: &Degradations,
) -> AutoscalerMap {
    let mut map: AutoscalerMap = HashMap::new();

    let hpas: Api<HorizontalPodAutoscaler> = Api::namespaced(client.clone(), ns);
    match hpas.list(&ListParams::default()).await {
        Ok(list) => {
            for hpa in &list.items {
                if let Some(f) = hpa_fields(hpa) {
                    let entry = map.entry((f.kind, f.name)).or_default();
                    entry.has_hpa = true;
                    entry.hpa_min_replicas = f.min_replicas;
                    entry.hpa_max_replicas = f.max_replicas;
                    entry.hpa_target_cpu_pct = f.target_cpu_pct;
                    entry.hpa_current_replicas = f.current_replicas;
                }
            }
        }
        Err(e) => {
            warn!(namespace = %ns, error = %e, "failed_to_list_hpas");
            degraded.record("horizontalpodautoscalers");
        }
    }

    // KEDA is a CRD most clusters do not have. Its absence is a 404 from the
    // apiserver, which is expected — log at debug and move on, so a missing CRD
    // never looks like a collection failure.
    let gvk = GroupVersionKind::gvk("keda.sh", "v1alpha1", "ScaledObject");
    let scaled_objects: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), ns, &ApiResource::from_gvk(&gvk));
    match scaled_objects.list(&ListParams::default()).await {
        Ok(list) => {
            for so in &list.items {
                if let Some(f) = keda_fields(&so.data["spec"]) {
                    let entry = map.entry((f.kind, f.name)).or_default();
                    entry.keda_scaled = true;
                    entry.keda_min_replicas = f.min_replicas;
                    entry.keda_trigger_types = f.trigger_types;
                }
            }
        }
        Err(e) => debug!(namespace = %ns, error = %e, "keda_scaledobjects_unavailable"),
    }

    map
}

/// The HPA fields the wire schema carries, plus the target it scales.
pub(crate) struct HpaFields {
    kind: String,
    name: String,
    min_replicas: u32,
    max_replicas: u32,
    target_cpu_pct: u32,
    current_replicas: u32,
}

/// Extract an HPA's scale target and limits. `None` if it names no target.
fn hpa_fields(hpa: &HorizontalPodAutoscaler) -> Option<HpaFields> {
    let spec = hpa.spec.as_ref()?;
    let target = &spec.scale_target_ref;
    if target.name.is_empty() {
        return None;
    }
    // The CPU target lives in the Resource metric naming "cpu"; other metric
    // types (memory, external, custom) have no field in the wire schema and
    // leave the percentage at 0.
    let target_cpu_pct = spec
        .metrics
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|m| m.resource.as_ref())
        .find(|r| r.name.eq_ignore_ascii_case("cpu"))
        .and_then(|r| r.target.average_utilization)
        .map(to_u32)
        .unwrap_or(0);

    Some(HpaFields {
        kind: target.kind.clone(),
        name: target.name.clone(),
        min_replicas: spec.min_replicas.map(to_u32).unwrap_or(0),
        max_replicas: to_u32(spec.max_replicas),
        target_cpu_pct,
        current_replicas: hpa
            .status
            .as_ref()
            .and_then(|s| s.current_replicas)
            .map(to_u32)
            .unwrap_or(0),
    })
}

/// The KEDA fields the wire schema carries, plus the target it scales.
pub(crate) struct KedaFields {
    kind: String,
    name: String,
    min_replicas: u32,
    trigger_types: String,
}

/// Extract a ScaledObject's target and floor from its untyped `spec`.
/// `None` if it names no target.
fn keda_fields(spec: &Value) -> Option<KedaFields> {
    let name = spec["scaleTargetRef"]["name"].as_str()?;
    let trigger_types: Vec<&str> = spec["triggers"]
        .as_array()
        .map(|ts| ts.as_slice())
        .unwrap_or_default()
        .iter()
        .filter_map(|t| t["type"].as_str())
        .collect();

    Some(KedaFields {
        // KEDA defaults scaleTargetRef.kind to Deployment when omitted.
        kind: spec["scaleTargetRef"]["kind"]
            .as_str()
            .unwrap_or("Deployment")
            .to_string(),
        name: name.to_string(),
        min_replicas: spec["minReplicaCount"].as_i64().unwrap_or(0).max(0) as u32,
        trigger_types: trigger_types.join(","),
    })
}

/// Count failed Jobs per owning CronJob name.
///
/// Only the Jobs the apiserver still holds are visible, and a CronJob's history
/// limits (3 failed by default) bound that to recent runs — which is exactly the
/// "recent failures" the wire schema asks for.
async fn fetch_cronjob_failures(
    client: &Client,
    ns: &str,
    degraded: &Degradations,
) -> HashMap<String, u32> {
    let mut failures: HashMap<String, u32> = HashMap::new();
    let jobs: Api<Job> = Api::namespaced(client.clone(), ns);
    match jobs.list(&ListParams::default()).await {
        Ok(list) => {
            for job in list.items {
                let failed = job.status.as_ref().and_then(|s| s.failed).unwrap_or(0);
                if failed <= 0 {
                    continue;
                }
                for owner in job.metadata.owner_references.iter().flatten() {
                    if owner.kind == "CronJob" {
                        *failures.entry(owner.name.clone()).or_default() += 1;
                    }
                }
            }
        }
        Err(e) => {
            warn!(namespace = %ns, error = %e, "failed_to_list_jobs");
            degraded.record("jobs");
        }
    }
    failures
}

pub async fn collect(config: &Config) -> Result<AgentSnapshot, CollectorError> {
    // Timed from before the client handshake: connecting to the apiserver is part
    // of what makes a pass slow, and excluding it would hide the most common cause.
    let started = Instant::now();
    let degraded = Degradations::default();

    let client = Client::try_default().await?;

    // Kubernetes version
    let k8s_version = fetch_k8s_version(&client).await;

    // Nodes
    let nodes_api: Api<Node> = Api::all(client.clone());
    let nodes = nodes_api.list(&ListParams::default()).await?;
    let node_count = nodes.items.len() as u32;

    // Aggregate per-node pricing metadata: pools grouped by
    // (instance_type, region, capacity_type), the primary region, and the
    // providerID-derived cloud provider.
    let (node_pools, region, provider_from_nodes) = aggregate_node_pools(&nodes.items);

    // Cloud provider: explicit config wins, then the providerID-derived value,
    // then a best-effort label heuristic for nodes that don't expose a providerID.
    let cloud_provider = config
        .cloud_provider
        .clone()
        .or_else(|| (!provider_from_nodes.is_empty()).then(|| provider_from_nodes.clone()))
        .or_else(|| {
            nodes.items.first().and_then(|n| {
                let labels = n.metadata.labels.as_ref()?;
                if labels.contains_key("eks.amazonaws.com/nodegroup")
                    || labels.contains_key("alpha.eksctl.io/cluster-name")
                {
                    Some("AWS".to_string())
                } else if labels.contains_key("cloud.google.com/gke-nodepool") {
                    Some("GCP".to_string())
                } else if labels.contains_key("kubernetes.azure.com/agentpool") {
                    Some("Azure".to_string())
                } else {
                    None
                }
            })
        });

    // Determine target namespaces
    let target_namespaces = get_target_namespaces(&client, config).await?;

    // Collect instantaneous pod metrics from the metrics-server.
    // Key: (namespace, pod_name) → Vec<ContainerMetric>
    let (pod_metrics, metrics_available) = match fetch_pod_metrics(&client, &degraded).await {
        Some(m) => (m, true),
        None => (HashMap::new(), false),
    };

    // Build pod → workload owner map (resolves ReplicaSet → Deployment).
    // Also returns the most-recent pod start time per namespace for activity tracking.
    let (pod_owner_map, pod_activity, workload_health) =
        build_pod_owner_map(&client, &target_namespaces, &degraded).await;

    // Aggregate pod metrics into per-(workload, container) usage totals
    let usage_map = aggregate_workload_usage(&pod_metrics, &pod_owner_map);

    // Build workload and namespace metrics
    let mut workloads: Vec<WorkloadMetrics> = Vec::new();
    let mut namespace_workload_counts: HashMap<String, u32> = HashMap::new();
    let mut namespace_costs: HashMap<String, f64> = HashMap::new();

    for ns in &target_namespaces {
        let mut ns_workload_count = 0u32;
        let mut ns_cost = 0.0f64;

        // HPA/KEDA context for this namespace, keyed by the (kind, name) pair a
        // scaleTargetRef names. Fetched once per namespace rather than per workload.
        let autoscalers = fetch_autoscaler_context(&client, ns, &degraded).await;

        // Deployments
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), ns);
        match deployments.list(&ListParams::default()).await {
            Ok(dep_list) => {
                for dep in dep_list.items {
                    let name = dep.metadata.name.clone().unwrap_or_default();
                    let replicas = dep.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1) as u32;
                    let obs_days = dep
                        .metadata
                        .creation_timestamp
                        .as_ref()
                        .map(|ts| days_since(&ts.0))
                        .unwrap_or(0);
                    let ctx = autoscalers.get(&("Deployment".to_string(), name.clone()));
                    let health =
                        workload_health.get(&(ns.clone(), "Deployment".to_string(), name.clone()));

                    for container in dep
                        .spec
                        .as_ref()
                        .and_then(|s| s.template.spec.as_ref())
                        .map(|ps| ps.containers.as_slice())
                        .unwrap_or_default()
                    {
                        let usage_key = (
                            ns.clone(),
                            "Deployment".to_string(),
                            name.clone(),
                            container.name.clone(),
                        );
                        let mut wl = build_workload_metrics(
                            ns,
                            "Deployment",
                            &name,
                            &container.name,
                            replicas,
                            obs_days,
                            &container.resources,
                            usage_map.get(&usage_key),
                        );
                        apply_autoscaler_context(&mut wl, ctx);
                        apply_workload_health(&mut wl, health);
                        ns_cost += wl.estimated_monthly_cost_usd;
                        workloads.push(wl);
                    }
                    ns_workload_count += 1;
                }
            }
            Err(e) => {
                warn!(namespace = %ns, error = %e, "failed_to_list_deployments");
                degraded.record("deployments");
            }
        }

        // StatefulSets
        let statefulsets: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
        match statefulsets.list(&ListParams::default()).await {
            Ok(sts_list) => {
                for sts in sts_list.items {
                    let name = sts.metadata.name.clone().unwrap_or_default();
                    let replicas = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1) as u32;
                    let obs_days = sts
                        .metadata
                        .creation_timestamp
                        .as_ref()
                        .map(|ts| days_since(&ts.0))
                        .unwrap_or(0);
                    let ctx = autoscalers.get(&("StatefulSet".to_string(), name.clone()));
                    let health =
                        workload_health.get(&(ns.clone(), "StatefulSet".to_string(), name.clone()));

                    for container in sts
                        .spec
                        .as_ref()
                        .and_then(|s| s.template.spec.as_ref())
                        .map(|ps| ps.containers.as_slice())
                        .unwrap_or_default()
                    {
                        let usage_key = (
                            ns.clone(),
                            "StatefulSet".to_string(),
                            name.clone(),
                            container.name.clone(),
                        );
                        let mut wl = build_workload_metrics(
                            ns,
                            "StatefulSet",
                            &name,
                            &container.name,
                            replicas,
                            obs_days,
                            &container.resources,
                            usage_map.get(&usage_key),
                        );
                        apply_autoscaler_context(&mut wl, ctx);
                        apply_workload_health(&mut wl, health);
                        ns_cost += wl.estimated_monthly_cost_usd;
                        workloads.push(wl);
                    }
                    ns_workload_count += 1;
                }
            }
            Err(e) => {
                warn!(namespace = %ns, error = %e, "failed_to_list_statefulsets");
                degraded.record("statefulsets");
            }
        }

        // DaemonSets (one pod per node)
        let daemonsets: Api<DaemonSet> = Api::namespaced(client.clone(), ns);
        match daemonsets.list(&ListParams::default()).await {
            Ok(ds_list) => {
                for ds in ds_list.items {
                    let name = ds.metadata.name.clone().unwrap_or_default();
                    let obs_days = ds
                        .metadata
                        .creation_timestamp
                        .as_ref()
                        .map(|ts| days_since(&ts.0))
                        .unwrap_or(0);
                    let health =
                        workload_health.get(&(ns.clone(), "DaemonSet".to_string(), name.clone()));

                    for container in ds
                        .spec
                        .as_ref()
                        .and_then(|s| s.template.spec.as_ref())
                        .map(|ps| ps.containers.as_slice())
                        .unwrap_or_default()
                    {
                        let usage_key = (
                            ns.clone(),
                            "DaemonSet".to_string(),
                            name.clone(),
                            container.name.clone(),
                        );
                        let mut wl = build_workload_metrics(
                            ns,
                            "DaemonSet",
                            &name,
                            &container.name,
                            node_count,
                            obs_days,
                            &container.resources,
                            usage_map.get(&usage_key),
                        );
                        apply_workload_health(&mut wl, health);
                        ns_cost += wl.estimated_monthly_cost_usd;
                        workloads.push(wl);
                    }
                    ns_workload_count += 1;
                }
            }
            Err(e) => {
                warn!(namespace = %ns, error = %e, "failed_to_list_daemonsets");
                degraded.record("daemonsets");
            }
        }

        // CronJobs. Emitted so the backend can see suspended/failing schedules and
        // missing limits; their requests are deliberately kept out of `ns_cost`,
        // since a CronJob reserves capacity only while a run is in flight and
        // billing it for the whole month would inflate the namespace's cost (and
        // with it the zombie-namespace saving derived from it).
        let cronjobs: Api<CronJob> = Api::namespaced(client.clone(), ns);
        match cronjobs.list(&ListParams::default()).await {
            Ok(cj_list) => {
                let failures = if cj_list.items.is_empty() {
                    HashMap::new()
                } else {
                    fetch_cronjob_failures(&client, ns, &degraded).await
                };

                for cj in cj_list.items {
                    let name = cj.metadata.name.clone().unwrap_or_default();
                    let obs_days = cj
                        .metadata
                        .creation_timestamp
                        .as_ref()
                        .map(|ts| days_since(&ts.0))
                        .unwrap_or(0);
                    let spec = cj.spec.as_ref();
                    let suspended = spec.and_then(|s| s.suspend).unwrap_or(false);
                    let last_schedule = cj
                        .status
                        .as_ref()
                        .and_then(|s| s.last_schedule_time.as_ref())
                        .map(|t| format_rfc3339(&t.0))
                        .unwrap_or_default();
                    let recent_failures = failures.get(&name).copied().unwrap_or(0);

                    for container in spec
                        .and_then(|s| s.job_template.spec.as_ref())
                        .and_then(|js| js.template.spec.as_ref())
                        .map(|ps| ps.containers.as_slice())
                        .unwrap_or_default()
                    {
                        // A CronJob has no steady-state replicas; one run at a time.
                        let mut wl = build_workload_metrics(
                            ns,
                            "CronJob",
                            &name,
                            &container.name,
                            1,
                            obs_days,
                            &container.resources,
                            None,
                        );
                        wl.cronjob_suspended = suspended;
                        wl.cronjob_last_schedule_ts = last_schedule.clone();
                        wl.cronjob_recent_failures = recent_failures;
                        workloads.push(wl);
                    }
                    ns_workload_count += 1;
                }
            }
            Err(e) => {
                warn!(namespace = %ns, error = %e, "failed_to_list_cronjobs");
                degraded.record("cronjobs");
            }
        }

        namespace_workload_counts.insert(ns.clone(), ns_workload_count);
        namespace_costs.insert(ns.clone(), ns_cost);
    }

    // Build namespace metrics, combining pod-based activity and event-based activity
    let mut namespaces: Vec<NamespaceMetrics> = Vec::new();
    for ns in &target_namespaces {
        let workload_count = *namespace_workload_counts.get(ns).unwrap_or(&0);
        let cost = *namespace_costs.get(ns).unwrap_or(&0.0);

        // Use most-recent pod start time; supplement with events for namespaces with no pods
        let from_pods = pod_activity.get(ns).copied().unwrap_or(u32::MAX);
        let from_events = fetch_namespace_last_activity_from_events(&client, ns, &degraded).await;
        let days_since_last_activity = from_pods.min(from_events);

        namespaces.push(NamespaceMetrics {
            name: ns.clone(),
            workload_count,
            // Clamp to 0 when we couldn't determine activity (avoids false zombie detection)
            days_since_last_activity: if days_since_last_activity == u32::MAX {
                0
            } else {
                days_since_last_activity
            },
            estimated_monthly_cost_usd: cost,
        });
    }

    let estimated_cluster_cost_usd = workloads.iter().map(|w| w.estimated_monthly_cost_usd).sum();

    // Saturating: a pass that somehow ran for 49 days reports the u32 ceiling
    // rather than wrapping to a small number that reads as healthy.
    let duration_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
    let partial_failures = degraded.into_vec();

    info!(
        node_count,
        workloads = workloads.len(),
        namespaces = namespaces.len(),
        duration_ms = duration_ms,
        partial_failures = ?partial_failures,
        "collection_complete"
    );

    Ok(AgentSnapshot {
        k8s_version: k8s_version.unwrap_or_default(),
        node_count,
        cloud_provider: cloud_provider.unwrap_or_default(),
        workloads,
        namespaces,
        estimated_cluster_cost_usd,
        collected_at: Utc::now().to_rfc3339(),
        region,
        node_pools,
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
        metrics_available,
        collection_duration_ms: duration_ms,
        partial_failures,
    })
}

/// Build a map from (namespace, pod_name) → PodOwner by resolving owner references.
/// ReplicaSets are resolved up to their parent Deployment.
///
/// Also returns a map of namespace → days since most-recent pod start time, for activity tracking.
async fn build_pod_owner_map(
    client: &Client,
    namespaces: &[String],
    degraded: &Degradations,
) -> (
    HashMap<(String, String), PodOwner>,
    HashMap<String, u32>,
    HealthMap,
) {
    let mut owner_map: HashMap<(String, String), PodOwner> = HashMap::new();
    let mut pod_activity: HashMap<String, u32> = HashMap::new();
    let mut health: HealthMap = HashMap::new();

    for ns in namespaces {
        // Pre-fetch ReplicaSet → Deployment mapping to avoid per-pod API calls
        let rs_to_deployment: HashMap<String, String> = {
            let rs_api: Api<ReplicaSet> = Api::namespaced(client.clone(), ns);
            match rs_api.list(&ListParams::default()).await {
                Ok(rs_list) => rs_list
                    .items
                    .iter()
                    .filter_map(|rs| {
                        let rs_name = rs.metadata.name.clone()?;
                        let dep_name = rs
                            .metadata
                            .owner_references
                            .as_ref()?
                            .iter()
                            .find(|o| o.kind == "Deployment")?
                            .name
                            .clone();
                        Some((rs_name, dep_name))
                    })
                    .collect(),
                Err(e) => {
                    warn!(namespace = %ns, error = %e, "failed_to_list_replicasets");
                    degraded.record("replicasets");
                    HashMap::new()
                }
            }
        };

        // PodDisruptionBudgets guarding pods in this namespace, reduced to the
        // (matchLabels, minAvailable) pairs the guardrail needs.
        let pdbs = fetch_pdbs(client, ns, degraded).await;

        let pods_api: Api<Pod> = Api::namespaced(client.clone(), ns);
        match pods_api.list(&ListParams::default()).await {
            Ok(pod_list) => {
                let mut ns_most_recent_start: u32 = u32::MAX;

                for pod in &pod_list.items {
                    let pod_name = match pod.metadata.name.as_deref() {
                        Some(n) => n.to_string(),
                        None => continue,
                    };

                    // Track most-recent pod start for namespace activity
                    if let Some(start_days) = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.start_time.as_ref())
                        .map(|t| days_since(&t.0))
                    {
                        ns_most_recent_start = ns_most_recent_start.min(start_days);
                    }

                    // Resolve pod → workload owner
                    let owner = pod
                        .metadata
                        .owner_references
                        .as_ref()
                        .and_then(|refs| refs.first())
                        .and_then(|owner_ref| match owner_ref.kind.as_str() {
                            "ReplicaSet" => {
                                let (workload_type, workload_name) =
                                    if let Some(dep) = rs_to_deployment.get(&owner_ref.name) {
                                        ("Deployment".to_string(), dep.clone())
                                    } else {
                                        ("ReplicaSet".to_string(), owner_ref.name.clone())
                                    };
                                Some(PodOwner {
                                    workload_type,
                                    workload_name,
                                })
                            }
                            "StatefulSet" => Some(PodOwner {
                                workload_type: "StatefulSet".to_string(),
                                workload_name: owner_ref.name.clone(),
                            }),
                            "DaemonSet" => Some(PodOwner {
                                workload_type: "DaemonSet".to_string(),
                                workload_name: owner_ref.name.clone(),
                            }),
                            _ => None,
                        });

                    if let Some(o) = owner {
                        // Accumulate the workload's health from this pod before the
                        // owner is moved into the map.
                        let key = (ns.clone(), o.workload_type.clone(), o.workload_name.clone());
                        let entry = health.entry(key).or_default();
                        if let Some(status) = pod.status.as_ref() {
                            for cs in status.container_statuses.iter().flatten() {
                                entry.restart_count += to_u32(cs.restart_count);
                                // `last_state.terminated` is the *previous* run, which is
                                // where an OOMKill shows up once the container restarted.
                                let oom = cs
                                    .last_state
                                    .as_ref()
                                    .and_then(|s| s.terminated.as_ref())
                                    .and_then(|t| t.reason.as_deref())
                                    .is_some_and(|r| r == "OOMKilled");
                                if oom {
                                    entry.oom_killed_count += 1;
                                }
                            }
                            if entry.qos_class.is_empty() {
                                if let Some(q) = status.qos_class.as_deref() {
                                    entry.qos_class = q.to_string();
                                }
                            }
                        }
                        if let Some(min) = pdb_floor(&pdbs, pod.metadata.labels.as_ref()) {
                            entry.has_pdb = true;
                            entry.pdb_min_available = entry.pdb_min_available.max(min);
                        }

                        owner_map.insert((ns.clone(), pod_name), o);
                    }
                }

                pod_activity.insert(ns.clone(), ns_most_recent_start);
            }
            Err(e) => {
                warn!(namespace = %ns, error = %e, "failed_to_list_pods");
                degraded.record("pods");
            }
        }
    }

    (owner_map, pod_activity, health)
}

/// A namespace's PodDisruptionBudgets as (matchLabels, minAvailable) pairs.
///
/// Only `spec.selector.matchLabels` is honoured — `matchExpressions` is vanishingly
/// rare on PDBs and supporting it would mean reimplementing selector evaluation for
/// a guardrail whose question is "is this workload guarded at all". A PDB using only
/// `matchExpressions` therefore selects nothing here and its workloads keep
/// `has_pdb = false`, which degrades to today's behaviour rather than misreporting.
///
/// `minAvailable` expressed as a percentage yields 0: `has_pdb` is the flag that
/// says the workload is guarded, the count is supplementary.
async fn fetch_pdbs(
    client: &Client,
    ns: &str,
    degraded: &Degradations,
) -> Vec<(BTreeMap<String, String>, u32)> {
    let api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), ns);
    match api.list(&ListParams::default()).await {
        Ok(list) => list
            .items
            .into_iter()
            .filter_map(|pdb| {
                let spec = pdb.spec?;
                let labels = spec.selector?.match_labels?;
                if labels.is_empty() {
                    return None;
                }
                let min = match spec.min_available {
                    Some(IntOrString::Int(n)) => to_u32(n),
                    _ => 0,
                };
                Some((labels, min))
            })
            .collect(),
        Err(e) => {
            warn!(namespace = %ns, error = %e, "failed_to_list_poddisruptionbudgets");
            degraded.record("poddisruptionbudgets");
            Vec::new()
        }
    }
}

/// The largest `minAvailable` among the PDBs selecting these pod labels, or `None`
/// when no PDB selects them. A PDB selects a pod when every one of its matchLabels
/// is present on the pod with the same value.
fn pdb_floor(
    pdbs: &[(BTreeMap<String, String>, u32)],
    pod_labels: Option<&BTreeMap<String, String>>,
) -> Option<u32> {
    let pod_labels = pod_labels?;
    pdbs.iter()
        .filter(|(selector, _)| selector.iter().all(|(k, v)| pod_labels.get(k) == Some(v)))
        .map(|(_, min)| *min)
        .max()
}

/// Aggregate per-container pod metrics into per-(workload, container) usage totals.
///
/// Since metrics-server provides instantaneous values, avg and p95 will both equal
/// the current point-in-time reading per pod averaged across all replicas.
fn aggregate_workload_usage(
    pod_metrics: &HashMap<(String, String), Vec<ContainerMetric>>,
    pod_owner_map: &HashMap<(String, String), PodOwner>,
) -> UsageMap {
    let mut usage_map: UsageMap = HashMap::new();

    for ((ns, pod_name), containers) in pod_metrics {
        if let Some(owner) = pod_owner_map.get(&(ns.clone(), pod_name.clone())) {
            for c in containers {
                let key = (
                    ns.clone(),
                    owner.workload_type.clone(),
                    owner.workload_name.clone(),
                    c.name.clone(),
                );
                let usage = usage_map.entry(key).or_default();
                usage.cpu_m_total += c.cpu_m;
                usage.mem_mi_total += c.mem_mi;
                usage.pod_count += 1;
            }
        }
    }

    usage_map
}

// Builder-style helper: each parameter maps directly to a field the caller has
// already resolved, so threading a struct through would add indirection without
// reducing the call site's work.
#[allow(clippy::too_many_arguments)]
fn build_workload_metrics(
    ns: &str,
    workload_type: &str,
    workload_name: &str,
    container_name: &str,
    replicas: u32,
    observation_days: u32,
    resources: &Option<k8s_openapi::api::core::v1::ResourceRequirements>,
    usage: Option<&WorkloadUsage>,
) -> WorkloadMetrics {
    let (cpu_request_m, cpu_limit_m, memory_request_mi, memory_limit_mi) = match resources {
        Some(res) => {
            let requests = res.requests.as_ref();
            let limits = res.limits.as_ref();
            (
                requests
                    .and_then(|r| r.get("cpu"))
                    .map(|q| parse_cpu_to_millicores(&q.0))
                    .unwrap_or(0),
                limits
                    .and_then(|l| l.get("cpu"))
                    .map(|q| parse_cpu_to_millicores(&q.0))
                    .unwrap_or(0),
                requests
                    .and_then(|r| r.get("memory"))
                    .map(|q| parse_memory_to_mib(&q.0))
                    .unwrap_or(0),
                limits
                    .and_then(|l| l.get("memory"))
                    .map(|q| parse_memory_to_mib(&q.0))
                    .unwrap_or(0),
            )
        }
        None => (0, 0, 0, 0),
    };

    // For instantaneous metrics-server data, avg ≈ p95 (single snapshot point).
    // Both are set to the per-pod average across all currently-running replicas.
    let (actual_cpu_avg_m, actual_cpu_p95_m, actual_memory_avg_mi, actual_memory_p95_mi) =
        match usage {
            Some(u) if u.pod_count > 0 => {
                let cpu = u.cpu_per_pod();
                let mem = u.mem_per_pod();
                (cpu, cpu, mem, mem)
            }
            _ => (0.0, 0.0, 0.0, 0.0),
        };

    WorkloadMetrics {
        namespace: ns.to_string(),
        workload_type: workload_type.to_string(),
        workload_name: workload_name.to_string(),
        container_name: container_name.to_string(),
        replicas,
        cpu_request_m,
        cpu_limit_m,
        memory_request_mi,
        memory_limit_mi,
        actual_cpu_avg_m,
        actual_cpu_p95_m,
        actual_memory_avg_mi,
        actual_memory_p95_mi,
        observation_days,
        estimated_monthly_cost_usd: monthly_cost(cpu_request_m, memory_request_mi, replicas),
        // Autoscaler fields (HPA/CronJob/KEDA) are not collected yet; leave them at
        // their proto zero-values so the backend checks treat them as "not present".
        ..Default::default()
    }
}

/// Returns the number of days since the most recent event in the namespace.
///
/// Uses Kubernetes core/v1 Events, which are retained for approximately 1 hour by default
/// (configurable via `--event-ttl` on the API server). For clusters with default retention,
/// this primarily detects *very recent* inactivity. Pair with pod start-time tracking
/// (see `build_pod_owner_map`) for reliable zombie namespace detection.
///
/// Returns `u32::MAX` if no events could be found (caller treats this as "unknown / active").
async fn fetch_namespace_last_activity_from_events(
    client: &Client,
    ns: &str,
    degraded: &Degradations,
) -> u32 {
    let events_api: Api<Event> = Api::namespaced(client.clone(), ns);
    match events_api.list(&ListParams::default()).await {
        Ok(event_list) => {
            let latest = event_list
                .items
                .iter()
                .filter_map(|e| e.last_timestamp.as_ref().map(|t| t.0))
                .max();
            match latest {
                Some(ts) => days_since(&ts),
                None => u32::MAX, // no events — can't determine
            }
        }
        Err(e) => {
            warn!(namespace = %ns, error = %e, "failed_to_fetch_events");
            degraded.record("events");
            u32::MAX
        }
    }
}

/// Fetch current pod metrics from the metrics-server API.
/// Returns a map of (namespace, pod_name) → Vec<ContainerMetric>.
/// `None` means metrics-server did not answer; `Some(map)` means it did, even if
/// the map is empty (a cluster can legitimately have no running pods).
///
/// The distinction matters downstream: the wire schema defines a `0` usage value
/// as both "measured zero" and "no data", so without this the backend cannot tell
/// an idle cluster from one with no metrics-server installed.
async fn fetch_pod_metrics(
    client: &Client,
    degraded: &Degradations,
) -> Option<HashMap<(String, String), Vec<ContainerMetric>>> {
    let req = match http::Request::builder()
        .uri("/apis/metrics.k8s.io/v1beta1/pods")
        .body(vec![])
    {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "failed_to_build_metrics_request");
            degraded.record("metrics-server");
            return None;
        }
    };

    match client.request::<Value>(req).await {
        Ok(val) => Some(parse_pod_metrics_json(&val)),
        Err(e) => {
            warn!(error = %e, "metrics_server_unavailable_continuing_without_usage_data");
            degraded.record("metrics-server");
            None
        }
    }
}

/// Parse a `metrics.k8s.io/v1beta1/pods` (PodMetricsList) JSON body into per-pod
/// container usage. Kept separate from the API call so it can be exercised against
/// captured responses from multiple Kubernetes / metrics-server versions.
///
/// The metrics-server unit conventions have drifted across versions (e.g. CPU as
/// nanocores `"…n"` vs millicores `"…m"`, memory as `"Ki"` vs `"Mi"`); the underlying
/// quantity parsers handle those, so this stays a thin shape-extraction layer.
pub(crate) fn parse_pod_metrics_json(
    val: &Value,
) -> HashMap<(String, String), Vec<ContainerMetric>> {
    let mut map = HashMap::new();
    if let Some(items) = val.get("items").and_then(|i| i.as_array()) {
        for item in items {
            let metadata = match item.get("metadata") {
                Some(m) => m,
                None => continue,
            };
            let pod_name = metadata
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            let pod_ns = metadata
                .get("namespace")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();

            let containers: Vec<ContainerMetric> = item
                .get("containers")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|c| {
                            let name = c.get("name")?.as_str()?.to_string();
                            let usage = c.get("usage")?;
                            let cpu_m = usage
                                .get("cpu")
                                .and_then(|v| v.as_str())
                                .map(parse_cpu_millicores)
                                .unwrap_or(0.0);
                            let mem_mi = usage
                                .get("memory")
                                .and_then(|v| v.as_str())
                                .map(parse_memory_mib)
                                .unwrap_or(0.0);
                            Some(ContainerMetric {
                                name,
                                cpu_m,
                                mem_mi,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            if !containers.is_empty() {
                map.insert((pod_ns, pod_name), containers);
            }
        }
    }
    map
}

async fn fetch_k8s_version(client: &Client) -> Option<String> {
    let req = http::Request::builder().uri("/version").body(vec![]).ok()?;

    let v = client.request::<Value>(req).await.ok()?;
    parse_k8s_version(&v)
}

/// Extract a `major.minor` version string from a `/version` (apimachinery `Info`) JSON body.
///
/// Managed control planes append a `+` to the minor field (e.g. EKS/GKE report
/// `"minor": "27+"`), so the raw minor is preserved as-is — callers that need a
/// numeric comparison should strip the trailing `+`.
pub(crate) fn parse_k8s_version(v: &Value) -> Option<String> {
    let major = v.get("major")?.as_str()?;
    let minor = v.get("minor")?.as_str()?;
    Some(format!("{}.{}", major, minor))
}

async fn get_target_namespaces(
    client: &Client,
    config: &Config,
) -> Result<Vec<String>, CollectorError> {
    if !config.include_namespaces.is_empty() {
        return Ok(config.include_namespaces.clone());
    }

    let ns_api: Api<Namespace> = Api::all(client.clone());
    let ns_list = ns_api.list(&ListParams::default()).await?;

    let namespaces: Vec<String> = ns_list
        .items
        .into_iter()
        .filter_map(|n| n.metadata.name)
        .filter(|name| !config.exclude_namespaces.contains(name))
        .collect();

    Ok(namespaces)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::jiff::SignedDuration;

    #[test]
    fn test_parse_cpu_millicores() {
        assert_eq!(parse_cpu_to_millicores("500m"), 500);
        assert_eq!(parse_cpu_to_millicores("2"), 2000);
        assert_eq!(parse_cpu_to_millicores("1.5"), 1500);
        assert_eq!(parse_cpu_to_millicores("100m"), 100);
        assert_eq!(parse_cpu_to_millicores("0"), 0);
        assert_eq!(parse_cpu_to_millicores("1000000000n"), 1000);
        assert_eq!(parse_cpu_to_millicores("500000000n"), 500);
    }

    #[test]
    fn test_parse_memory_mib() {
        assert_eq!(parse_memory_to_mib("512Mi"), 512);
        assert_eq!(parse_memory_to_mib("1Gi"), 1024);
        assert_eq!(parse_memory_to_mib("2Gi"), 2048);
        assert_eq!(parse_memory_to_mib("256Ki"), 0); // whole-MiB form truncates
        assert_eq!(parse_memory_to_mib("1073741824"), 1024); // 1 GiB in bytes
        assert_eq!(parse_memory_to_mib("500Mi"), 500);
        assert_eq!(parse_memory_to_mib("4Gi"), 4096);
    }

    // ── Fractional precision on the usage path ─────────────────────────────────
    //
    // Requests/limits are whole numbers on the wire (uint32), but usage is a
    // double. Truncating usage to an integer reports small containers as idle and
    // biases every rightsizing recommendation downward, so these pin the
    // fractional behavior of the parsers the metrics path uses.

    #[test]
    fn test_parse_cpu_millicores_keeps_sub_millicore_usage() {
        // metrics-server reports nanocores; 800000n is 0.8m, not "idle".
        assert!((parse_cpu_millicores("800000n") - 0.8).abs() < 1e-9);
        assert!((parse_cpu_millicores("100n") - 0.0001).abs() < 1e-9);
        assert!((parse_cpu_millicores("2500000n") - 2.5).abs() < 1e-9);
        assert!((parse_cpu_millicores("1500u") - 1.5).abs() < 1e-9);
        // The integer form still collapses these to zero, which is why the
        // usage path must not use it.
        assert_eq!(parse_cpu_to_millicores("800000n"), 0);
    }

    #[test]
    fn test_parse_cpu_millicores_matches_integer_form_on_whole_values() {
        for s in ["500m", "2", "1.5", "0", "1000000000n"] {
            assert_eq!(
                parse_cpu_millicores(s) as u32,
                parse_cpu_to_millicores(s),
                "mismatch for {s}"
            );
        }
    }

    #[test]
    fn test_parse_memory_mib_keeps_sub_mib_usage() {
        assert!((parse_memory_mib("512Ki") - 0.5).abs() < 1e-9);
        assert!((parse_memory_mib("256Ki") - 0.25).abs() < 1e-9);
        assert!((parse_memory_mib("1536Ki") - 1.5).abs() < 1e-9);
        assert_eq!(parse_memory_to_mib("512Ki"), 0);
    }

    #[test]
    fn test_parse_memory_decimal_suffixes_scale_correctly() {
        // Decimal suffixes are powers of 1000; T is 1e12, not 1e9.
        assert!((parse_memory_mib("1M") - 1_000_000.0 / 1_048_576.0).abs() < 1e-6);
        assert!((parse_memory_mib("1G") - 1_000_000_000.0 / 1_048_576.0).abs() < 1e-6);
        assert!((parse_memory_mib("1T") - 1_000_000_000_000.0 / 1_048_576.0).abs() < 1e-3);
        // 1T must be 1000x 1G, not equal to it.
        assert!(parse_memory_mib("1T") > parse_memory_mib("1G") * 999.0);
        // Binary suffixes are powers of 1024.
        assert_eq!(parse_memory_mib("1Ti"), 1024.0 * 1024.0);
    }

    #[test]
    fn test_parsers_reject_garbage_without_panicking() {
        for s in ["", "abc", "m", "Mi", "-5m", "1e", "  "] {
            let _ = parse_cpu_millicores(s);
            let _ = parse_memory_mib(s);
            // Negative quantities must never wrap around on the uint32 path.
            assert_eq!(parse_cpu_to_millicores(s), 0, "cpu {s:?}");
            assert_eq!(parse_memory_to_mib(s), 0, "mem {s:?}");
        }
    }

    // ── Autoscaler context ─────────────────────────────────────────────────
    //
    // These parse the API shapes the collector reads. The three categories they
    // feed (hpa-misconfig, cronjob-waste, keda-idle) are gated behind
    // has_hpa/keda_scaled, so a silent parse regression turns them all off with
    // no error anywhere — exactly the failure these guard against.

    #[test]
    fn hpa_fields_extracts_target_and_cpu_percentage() {
        let hpa: HorizontalPodAutoscaler = serde_json::from_value(serde_json::json!({
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": { "name": "api" },
            "spec": {
                "scaleTargetRef": { "apiVersion": "apps/v1", "kind": "Deployment", "name": "api" },
                "minReplicas": 3,
                "maxReplicas": 10,
                "metrics": [
                    // A non-CPU metric first, to pin that the CPU one is still found.
                    { "type": "Resource", "resource": { "name": "memory",
                        "target": { "type": "Utilization", "averageUtilization": 90 } } },
                    { "type": "Resource", "resource": { "name": "cpu",
                        "target": { "type": "Utilization", "averageUtilization": 75 } } }
                ]
            },
            "status": { "currentReplicas": 3, "desiredReplicas": 3 }
        }))
        .expect("HPA fixture should deserialize");

        let f = hpa_fields(&hpa).expect("HPA names a target");
        assert_eq!(f.kind, "Deployment");
        assert_eq!(f.name, "api");
        assert_eq!(f.min_replicas, 3);
        assert_eq!(f.max_replicas, 10);
        assert_eq!(f.target_cpu_pct, 75);
        assert_eq!(f.current_replicas, 3);
    }

    #[test]
    fn hpa_without_cpu_metric_reports_zero_percent_not_a_wrong_one() {
        let hpa: HorizontalPodAutoscaler = serde_json::from_value(serde_json::json!({
            "spec": {
                "scaleTargetRef": { "kind": "StatefulSet", "name": "queue" },
                "maxReplicas": 5,
                "metrics": [
                    { "type": "External", "external": { "metric": { "name": "lag" },
                        "target": { "type": "AverageValue", "averageValue": "100" } } }
                ]
            }
        }))
        .expect("HPA fixture should deserialize");

        let f = hpa_fields(&hpa).expect("HPA names a target");
        assert_eq!(f.target_cpu_pct, 0);
        // minReplicas omitted — the wire schema's "not present" is 0, not 1.
        assert_eq!(f.min_replicas, 0);
        assert_eq!(f.kind, "StatefulSet");
    }

    #[test]
    fn keda_fields_defaults_kind_to_deployment_and_joins_triggers() {
        let spec = serde_json::json!({
            "scaleTargetRef": { "name": "consumer" },
            "minReplicaCount": 2,
            "triggers": [ { "type": "kafka" }, { "type": "cron" } ]
        });

        let f = keda_fields(&spec).expect("ScaledObject names a target");
        assert_eq!(f.kind, "Deployment");
        assert_eq!(f.name, "consumer");
        assert_eq!(f.min_replicas, 2);
        assert_eq!(f.trigger_types, "kafka,cron");
    }

    #[test]
    fn keda_fields_tolerates_a_spec_with_nothing_in_it() {
        assert!(keda_fields(&serde_json::json!({})).is_none());
        // Present target, everything else missing: floor 0, no triggers.
        let f = keda_fields(&serde_json::json!({ "scaleTargetRef": { "name": "w" } }))
            .expect("target named");
        assert_eq!(f.min_replicas, 0);
        assert_eq!(f.trigger_types, "");
    }

    #[test]
    fn autoscaler_context_left_absent_keeps_proto_zero_values() {
        let mut wl = WorkloadMetrics::default();
        apply_autoscaler_context(&mut wl, None);
        assert!(!wl.has_hpa);
        assert!(!wl.keda_scaled);
        assert_eq!(wl.hpa_min_replicas, 0);
        assert_eq!(wl.keda_trigger_types, "");
    }

    #[test]
    fn a_keda_scaled_workload_reports_both_hpa_and_keda() {
        // KEDA creates an operator-managed HPA, so both flags are legitimately set.
        let ctx = AutoscalerContext {
            has_hpa: true,
            hpa_min_replicas: 1,
            hpa_max_replicas: 20,
            hpa_target_cpu_pct: 70,
            hpa_current_replicas: 1,
            keda_scaled: true,
            keda_min_replicas: 1,
            keda_trigger_types: "kafka".to_string(),
        };
        let mut wl = WorkloadMetrics::default();
        apply_autoscaler_context(&mut wl, Some(&ctx));

        assert!(wl.has_hpa && wl.keda_scaled);
        assert_eq!(wl.hpa_max_replicas, 20);
        assert_eq!(wl.keda_min_replicas, 1);
        assert_eq!(wl.keda_trigger_types, "kafka");
    }

    // ── Node capacity ──────────────────────────────────────────────────────

    fn node_with_resources(alloc_cpu: &str, alloc_mem: &str, cap_cpu: &str, cap_mem: &str) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": { "name": "n", "labels": { "node.kubernetes.io/instance-type": "m5.large" } },
            "status": {
                "allocatable": { "cpu": alloc_cpu, "memory": alloc_mem },
                "capacity": { "cpu": cap_cpu, "memory": cap_mem }
            }
        }))
        .expect("node fixture should deserialize")
    }

    #[test]
    fn node_resources_reads_allocatable_and_capacity() {
        // Allocatable is deliberately lower than capacity: kubelet/system
        // reservations and eviction thresholds come off the top, and it is
        // allocatable that a scheduler actually places against.
        let n = node_with_resources("1930m", "7134Mi", "2", "8Gi");
        assert_eq!(node_resources(&n), [1930, 7134, 2000, 8192]);
    }

    #[test]
    fn node_resources_are_zero_when_the_status_is_missing() {
        let bare: Node = serde_json::from_value(serde_json::json!({ "metadata": { "name": "n" } }))
            .expect("node fixture should deserialize");
        assert_eq!(node_resources(&bare), [0; 4]);
    }

    #[test]
    fn node_pool_capacity_is_per_node_not_the_pool_total() {
        // Three identical nodes in one pool: the emitted figures describe ONE
        // node, so a consumer multiplies by node_count. Summing instead would
        // silently treble every packing calculation.
        let nodes = vec![
            node_with_resources("2", "8Gi", "2", "8Gi"),
            node_with_resources("2", "8Gi", "2", "8Gi"),
            node_with_resources("2", "8Gi", "2", "8Gi"),
        ];
        let (pools, _, _) = aggregate_node_pools(&nodes);

        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].node_count, 3);
        assert_eq!(pools[0].cpu_allocatable_m, 2000);
        assert_eq!(pools[0].memory_allocatable_mi, 8192);
    }

    // ── Right-sizing guardrails ────────────────────────────────────────────

    fn pdb(pairs: &[(&str, &str)], min: u32) -> (BTreeMap<String, String>, u32) {
        (
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            min,
        )
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn pdb_selects_a_pod_only_when_every_match_label_agrees() {
        let pdbs = vec![pdb(&[("app", "api"), ("tier", "web")], 2)];

        // All selector labels present with matching values.
        let pod = labels(&[("app", "api"), ("tier", "web"), ("extra", "ignored")]);
        assert_eq!(pdb_floor(&pdbs, Some(&pod)), Some(2));

        // One label missing — a PDB requiring both must not select this pod.
        let partial = labels(&[("app", "api")]);
        assert_eq!(pdb_floor(&pdbs, Some(&partial)), None);

        // Present but different value.
        let wrong = labels(&[("app", "api"), ("tier", "batch")]);
        assert_eq!(pdb_floor(&pdbs, Some(&wrong)), None);
    }

    #[test]
    fn pdb_floor_takes_the_strictest_of_several_matching_budgets() {
        let pdbs = vec![pdb(&[("app", "api")], 1), pdb(&[("app", "api")], 4)];
        let pod = labels(&[("app", "api")]);
        assert_eq!(pdb_floor(&pdbs, Some(&pod)), Some(4));
    }

    #[test]
    fn a_percentage_pdb_still_marks_the_workload_guarded() {
        // fetch_pdbs maps a percentage minAvailable to 0; the caller sets has_pdb
        // from the Some(_), so the workload is still known to be guarded.
        let pdbs = vec![pdb(&[("app", "api")], 0)];
        let pod = labels(&[("app", "api")]);
        assert_eq!(pdb_floor(&pdbs, Some(&pod)), Some(0));
    }

    #[test]
    fn a_pod_with_no_labels_is_never_selected() {
        let pdbs = vec![pdb(&[("app", "api")], 2)];
        assert_eq!(pdb_floor(&pdbs, None), None);
    }

    #[test]
    fn workload_health_left_absent_keeps_proto_zero_values() {
        let mut wl = WorkloadMetrics::default();
        apply_workload_health(&mut wl, None);
        assert_eq!(wl.oom_killed_count, 0);
        assert_eq!(wl.restart_count, 0);
        assert_eq!(wl.qos_class, "");
        assert!(!wl.has_pdb);
    }

    #[test]
    fn workload_health_carries_the_signals_that_make_shrinking_unsafe() {
        let h = WorkloadHealth {
            restart_count: 7,
            oom_killed_count: 2,
            qos_class: "Guaranteed".to_string(),
            has_pdb: true,
            pdb_min_available: 3,
        };
        let mut wl = WorkloadMetrics::default();
        apply_workload_health(&mut wl, Some(&h));

        assert_eq!(wl.oom_killed_count, 2);
        assert_eq!(wl.restart_count, 7);
        assert_eq!(wl.qos_class, "Guaranteed");
        assert!(wl.has_pdb);
        assert_eq!(wl.pdb_min_available, 3);
    }

    #[test]
    fn an_empty_metrics_response_is_not_the_same_as_no_metrics_server() {
        // metrics-server answering with zero pods is a real, empty result; the
        // caller distinguishes it from a failed call by Some(empty) vs None. If
        // these ever collapse back into one value, the backend loses its only way
        // to tell an idle cluster from one without metrics-server.
        let empty = parse_pod_metrics_json(&serde_json::json!({ "items": [] }));
        assert!(empty.is_empty());

        let populated = parse_pod_metrics_json(&serde_json::json!({
            "items": [{
                "metadata": { "namespace": "default", "name": "api-0" },
                "containers": [{ "name": "main", "usage": { "cpu": "250m", "memory": "128Mi" } }]
            }]
        }));
        assert_eq!(populated.len(), 1);
    }

    #[test]
    fn cronjob_timestamps_are_whole_seconds_the_backend_can_parse() {
        // The backend parses this with Python's datetime.fromisoformat, which
        // rejects more than six fractional digits — so emit none at all.
        let ts: Timestamp = "2024-05-01T12:05:00.123456789Z"
            .parse()
            .expect("timestamp parses");
        assert_eq!(format_rfc3339(&ts), "2024-05-01T12:05:00Z");
    }

    #[test]
    fn test_days_since_past() {
        let ts = Timestamp::now() - SignedDuration::from_hours(5 * 24);
        assert_eq!(days_since(&ts), 5);
    }

    #[test]
    fn test_days_since_future_clamps_to_zero() {
        let ts = Timestamp::now() + SignedDuration::from_hours(3 * 24);
        assert_eq!(days_since(&ts), 0);
    }

    #[test]
    fn test_workload_usage_per_pod() {
        let u = WorkloadUsage {
            cpu_m_total: 300.0,
            mem_mi_total: 768.0,
            pod_count: 3,
        };
        assert_eq!(u.cpu_per_pod(), 100.0);
        assert_eq!(u.mem_per_pod(), 256.0);
    }

    #[test]
    fn test_workload_usage_zero_pods() {
        let u = WorkloadUsage::default();
        assert_eq!(u.cpu_per_pod(), 0.0);
        assert_eq!(u.mem_per_pod(), 0.0);
    }

    #[test]
    fn test_monthly_cost_single_replica() {
        // 1 vCPU @ $0.048/hr * 24 * 30 = $34.56 for CPU
        // 1 GiB @ $0.006/hr * 24 * 30 = $4.32 for memory
        let cost = monthly_cost(1000, 1024, 1);
        let expected = (1.0 * CPU_COST_PER_VCPU_HOUR + 1.0 * MEM_COST_PER_GB_HOUR) * 24.0 * 30.0;
        assert!(
            (cost - expected).abs() < 0.001,
            "cost={cost}, expected={expected}"
        );
    }

    #[test]
    fn test_monthly_cost_scales_with_replicas() {
        let single = monthly_cost(500, 512, 1);
        let triple = monthly_cost(500, 512, 3);
        assert!((triple - single * 3.0).abs() < 0.001);
    }

    #[test]
    fn test_monthly_cost_zero_resources() {
        assert_eq!(monthly_cost(0, 0, 5), 0.0);
    }

    #[test]
    fn test_aggregate_workload_usage_single_pod() {
        let mut pod_metrics: HashMap<(String, String), Vec<ContainerMetric>> = HashMap::new();
        pod_metrics.insert(
            ("default".to_string(), "api-abc".to_string()),
            vec![ContainerMetric {
                name: "main".to_string(),
                cpu_m: 100.0,
                mem_mi: 256.0,
            }],
        );

        let mut owner_map: HashMap<(String, String), PodOwner> = HashMap::new();
        owner_map.insert(
            ("default".to_string(), "api-abc".to_string()),
            PodOwner {
                workload_type: "Deployment".to_string(),
                workload_name: "api".to_string(),
            },
        );

        let usage = aggregate_workload_usage(&pod_metrics, &owner_map);
        let key = (
            "default".to_string(),
            "Deployment".to_string(),
            "api".to_string(),
            "main".to_string(),
        );
        let u = usage.get(&key).expect("usage entry should exist");
        assert_eq!(u.pod_count, 1);
        assert_eq!(u.cpu_m_total, 100.0);
        assert_eq!(u.mem_mi_total, 256.0);
    }

    #[test]
    fn test_aggregate_workload_usage_multiple_pods() {
        let mut pod_metrics: HashMap<(String, String), Vec<ContainerMetric>> = HashMap::new();
        for i in 0..3u32 {
            pod_metrics.insert(
                ("default".to_string(), format!("api-pod-{i}")),
                vec![ContainerMetric {
                    name: "main".to_string(),
                    cpu_m: 50.0,
                    mem_mi: 128.0,
                }],
            );
        }

        let mut owner_map: HashMap<(String, String), PodOwner> = HashMap::new();
        for i in 0..3u32 {
            owner_map.insert(
                ("default".to_string(), format!("api-pod-{i}")),
                PodOwner {
                    workload_type: "Deployment".to_string(),
                    workload_name: "api".to_string(),
                },
            );
        }

        let usage = aggregate_workload_usage(&pod_metrics, &owner_map);
        let key = (
            "default".to_string(),
            "Deployment".to_string(),
            "api".to_string(),
            "main".to_string(),
        );
        let u = usage.get(&key).expect("usage entry should exist");
        assert_eq!(u.pod_count, 3);
        assert_eq!(u.cpu_m_total, 150.0);
        assert_eq!(u.mem_per_pod(), 128.0);
    }

    #[test]
    fn test_aggregate_workload_usage_ignores_unmapped_pods() {
        let mut pod_metrics: HashMap<(String, String), Vec<ContainerMetric>> = HashMap::new();
        pod_metrics.insert(
            ("default".to_string(), "orphan-pod".to_string()),
            vec![ContainerMetric {
                name: "main".to_string(),
                cpu_m: 200.0,
                mem_mi: 512.0,
            }],
        );

        // No entry in owner_map → pod is unmapped, should be silently skipped
        let owner_map: HashMap<(String, String), PodOwner> = HashMap::new();
        let usage = aggregate_workload_usage(&pod_metrics, &owner_map);
        assert!(usage.is_empty());
    }

    // ── /version endpoint compatibility (K8s 1.21+) ────────────────────────────
    //
    // The apimachinery `Info` payload returned by `GET /version` is stable in
    // shape across releases, but the *values* differ: vanilla clusters report a
    // plain minor ("21".."31"), while managed control planes (EKS/GKE/AKS) append
    // a "+" to signal a patched build. These tests pin the parser's behavior for
    // every minor the agent is expected to run against.

    use serde_json::json;

    #[test]
    fn test_parse_k8s_version_vanilla_1_21_through_1_31() {
        for minor in 21..=31u32 {
            let body = json!({
                "major": "1",
                "minor": minor.to_string(),
                "gitVersion": format!("v1.{minor}.0"),
                "platform": "linux/amd64",
            });
            assert_eq!(
                parse_k8s_version(&body),
                Some(format!("1.{minor}")),
                "failed for minor {minor}"
            );
        }
    }

    #[test]
    fn test_parse_k8s_version_managed_cluster_plus_suffix() {
        // EKS: `kubectl version` shows minor "27+" for the EKS 1.27 control plane.
        let eks = json!({ "major": "1", "minor": "27+", "gitVersion": "v1.27.9-eks-5e0fdde" });
        assert_eq!(parse_k8s_version(&eks), Some("1.27+".to_string()));

        // GKE reports the same "+" convention on older channels.
        let gke = json!({ "major": "1", "minor": "21+", "gitVersion": "v1.21.14-gke.700" });
        assert_eq!(parse_k8s_version(&gke), Some("1.21+".to_string()));
    }

    #[test]
    fn test_parse_k8s_version_missing_fields_returns_none() {
        assert_eq!(parse_k8s_version(&json!({ "major": "1" })), None);
        assert_eq!(parse_k8s_version(&json!({ "minor": "29" })), None);
        assert_eq!(parse_k8s_version(&json!({})), None);
        // Non-string fields (defensive against malformed proxies)
        assert_eq!(parse_k8s_version(&json!({ "major": 1, "minor": 29 })), None);
    }

    // ── metrics.k8s.io/v1beta1 compatibility ───────────────────────────────────
    //
    // metrics-server returns Kubernetes quantity strings whose unit conventions
    // have shifted between versions. Older/in-cluster metrics-server emits CPU in
    // nanocores ("…n") and memory in kibibytes ("…Ki"); some builds emit whole
    // millicores/mebibytes. The parser must normalize all of them.

    #[test]
    fn test_parse_pod_metrics_nanocores_and_ki() {
        // Shape emitted by metrics-server on 1.21–1.24 in-cluster: nanocores + Ki.
        let body = json!({
            "kind": "PodMetricsList",
            "apiVersion": "metrics.k8s.io/v1beta1",
            "items": [{
                "metadata": { "name": "api-7d9", "namespace": "default" },
                "containers": [{
                    "name": "main",
                    // 250_000_000 nanocores = 250 millicores
                    "usage": { "cpu": "250000000n", "memory": "262144Ki" }
                }]
            }]
        });

        let map = parse_pod_metrics_json(&body);
        let c = &map[&("default".to_string(), "api-7d9".to_string())][0];
        assert_eq!(c.name, "main");
        assert_eq!(c.cpu_m, 250.0);
        assert_eq!(c.mem_mi, 256.0); // 262144 Ki = 256 Mi
    }

    #[test]
    fn test_parse_pod_metrics_millicores_and_mi() {
        // Some metrics-server builds report already-reduced units.
        let body = json!({
            "items": [{
                "metadata": { "name": "web-0", "namespace": "shop" },
                "containers": [{ "name": "nginx", "usage": { "cpu": "250m", "memory": "512Mi" } }]
            }]
        });

        let map = parse_pod_metrics_json(&body);
        let c = &map[&("shop".to_string(), "web-0".to_string())][0];
        assert_eq!(c.cpu_m, 250.0);
        assert_eq!(c.mem_mi, 512.0);
    }

    #[test]
    fn test_parse_pod_metrics_multi_container_multi_pod() {
        let body = json!({
            "items": [
                {
                    "metadata": { "name": "api-a", "namespace": "prod" },
                    "containers": [
                        { "name": "app", "usage": { "cpu": "100m", "memory": "128Mi" } },
                        { "name": "sidecar", "usage": { "cpu": "10m", "memory": "32Mi" } }
                    ]
                },
                {
                    "metadata": { "name": "api-b", "namespace": "prod" },
                    "containers": [
                        { "name": "app", "usage": { "cpu": "1", "memory": "1Gi" } }
                    ]
                }
            ]
        });

        let map = parse_pod_metrics_json(&body);
        assert_eq!(map.len(), 2);

        let a = &map[&("prod".to_string(), "api-a".to_string())];
        assert_eq!(a.len(), 2);

        let b = &map[&("prod".to_string(), "api-b".to_string())][0];
        assert_eq!(b.cpu_m, 1000.0); // "1" whole core
        assert_eq!(b.mem_mi, 1024.0); // 1Gi
    }

    #[test]
    fn test_parse_pod_metrics_empty_and_missing_items() {
        assert!(parse_pod_metrics_json(&json!({ "items": [] })).is_empty());
        // metrics-server unavailable / unexpected body → empty, never panics.
        assert!(parse_pod_metrics_json(&json!({})).is_empty());
        assert!(parse_pod_metrics_json(&json!({ "items": "garbage" })).is_empty());
    }

    #[test]
    fn test_parse_pod_metrics_skips_container_without_usage() {
        // A container missing `usage` is dropped; pods with no usable containers
        // are omitted entirely.
        let body = json!({
            "items": [{
                "metadata": { "name": "p", "namespace": "n" },
                "containers": [{ "name": "no-usage" }]
            }]
        });
        assert!(parse_pod_metrics_json(&body).is_empty());
    }

    // ── Node pricing metadata: providerID parsing + pool aggregation ────────────

    #[test]
    fn test_parse_provider_id() {
        assert_eq!(parse_provider_id("aws:///us-east-1a/i-0abc123"), "aws");
        assert_eq!(
            parse_provider_id("gce://my-proj/us-central1-a/gke-node"),
            "gcp"
        );
        assert_eq!(
            parse_provider_id("azure:///subscriptions/x/resourceGroups/y/vm/z"),
            "azure"
        );
        assert_eq!(parse_provider_id(""), "");
        // Unknown scheme passes through; a value with no scheme is unknown.
        assert_eq!(parse_provider_id("digitalocean://12345"), "digitalocean");
        assert_eq!(parse_provider_id("no-scheme"), "");
    }

    /// Build a minimal Node with the given labels and providerID.
    fn node_fixture(labels: Value, provider_id: &str) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": "n", "labels": labels },
            "spec": { "providerID": provider_id }
        }))
        .expect("node fixture should deserialize")
    }

    #[test]
    fn test_aggregate_node_pools_mixed() {
        let on_demand_labels = json!({
            "node.kubernetes.io/instance-type": "m5.large",
            "topology.kubernetes.io/region": "us-east-1"
        });
        let spot_labels = json!({
            "node.kubernetes.io/instance-type": "m5.large",
            "topology.kubernetes.io/region": "us-east-1",
            "karpenter.sh/capacity-type": "spot"
        });
        // No instance-type label, but still in the region and counted.
        let no_type_labels = json!({
            "topology.kubernetes.io/region": "us-east-1"
        });

        let nodes = vec![
            node_fixture(on_demand_labels.clone(), "aws:///us-east-1a/i-1"),
            node_fixture(on_demand_labels, "aws:///us-east-1a/i-2"),
            node_fixture(spot_labels, "aws:///us-east-1b/i-3"),
            node_fixture(no_type_labels, "aws:///us-east-1c/i-4"),
        ];

        let (pools, region, provider) = aggregate_node_pools(&nodes);

        assert_eq!(region, "us-east-1");
        assert_eq!(provider, "aws");

        // node_count across all pools must still equal the 4 input nodes.
        let total: u32 = pools.iter().map(|p| p.node_count).sum();
        assert_eq!(total, 4);

        let find = |it: &str, cap: &str| {
            pools
                .iter()
                .find(|p| {
                    p.instance_type == it && p.region == "us-east-1" && p.capacity_type == cap
                })
                .map(|p| p.node_count)
        };
        assert_eq!(find("m5.large", "on-demand"), Some(2));
        assert_eq!(find("m5.large", "spot"), Some(1));
        assert_eq!(find("", "on-demand"), Some(1));
        assert_eq!(pools.len(), 3);
    }

    #[test]
    fn test_node_meta_legacy_labels_and_no_provider() {
        // Legacy label keys are honored; an empty providerID stays unknown.
        let node = node_fixture(
            json!({
                "beta.kubernetes.io/instance-type": "n1-standard-4",
                "failure-domain.beta.kubernetes.io/region": "europe-west1"
            }),
            "",
        );
        let meta = node_meta(&node);
        assert_eq!(meta.instance_type, "n1-standard-4");
        assert_eq!(meta.region, "europe-west1");
        assert_eq!(meta.capacity_type, "on-demand");
        assert_eq!(meta.provider, "");
    }

    #[test]
    fn test_capacity_type_spot_signals() {
        let spot_signals = [
            ("eks.amazonaws.com/capacityType", "SPOT"),
            ("cloud.google.com/gke-spot", "true"),
            ("kubernetes.azure.com/scalesetpriority", "spot"),
            ("karpenter.sh/capacity-type", "spot"),
        ];
        for (key, val) in spot_signals {
            let mut labels = BTreeMap::new();
            labels.insert(key.to_string(), val.to_string());
            assert_eq!(capacity_type_from_labels(&labels), "spot", "for {key}");
        }
        // On-demand / absent signals.
        let mut on_demand = BTreeMap::new();
        on_demand.insert(
            "eks.amazonaws.com/capacityType".to_string(),
            "ON_DEMAND".to_string(),
        );
        assert_eq!(capacity_type_from_labels(&on_demand), "on-demand");
        assert_eq!(capacity_type_from_labels(&BTreeMap::new()), "on-demand");
    }

    #[test]
    fn degradations_dedup_and_sort() {
        let d = Degradations::default();
        // The same subsystem failing once per namespace is the common case.
        d.record("pods");
        d.record("pods");
        d.record("metrics-server");
        d.record("deployments");
        assert_eq!(
            d.into_vec(),
            vec!["deployments", "metrics-server", "pods"],
            "one entry per subsystem, in a stable order"
        );
    }

    #[test]
    fn degradations_empty_on_a_clean_pass() {
        assert!(Degradations::default().into_vec().is_empty());
    }
}
