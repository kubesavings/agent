use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{Event, Namespace, Node, Pod};
use kube::api::ListParams;
use kube::{Api, Client};
use serde_json::Value;
use thiserror::Error;
use tracing::{info, warn};

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

/// Resolved workload owner for a pod (traced through ReplicaSet to Deployment if needed).
struct PodOwner {
    workload_type: String,
    workload_name: String,
}

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

/// Parse Kubernetes CPU string to millicores.
/// Examples: "500m" → 500, "2" → 2000, "100n" → 0
pub fn parse_cpu_to_millicores(s: &str) -> u32 {
    if let Some(m) = s.strip_suffix('m') {
        m.parse().unwrap_or(0)
    } else if let Some(n) = s.strip_suffix('n') {
        // nanocores → millicores (1m = 1_000_000n)
        (n.parse::<u64>().unwrap_or(0) / 1_000_000) as u32
    } else if let Some(u) = s.strip_suffix('u') {
        // microcores → millicores
        (u.parse::<u64>().unwrap_or(0) / 1_000) as u32
    } else {
        // whole cores
        (s.parse::<f64>().unwrap_or(0.0) * 1000.0) as u32
    }
}

/// Parse Kubernetes memory string to MiB.
/// Handles: Ki, Mi, Gi, Ti, K, M, G, T, and plain bytes.
pub fn parse_memory_to_mib(s: &str) -> u32 {
    let s = s.trim();
    if let Some(v) = s.strip_suffix("Ti") {
        (v.parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0) as u32
    } else if let Some(v) = s.strip_suffix("Gi") {
        (v.parse::<f64>().unwrap_or(0.0) * 1024.0) as u32
    } else if let Some(v) = s.strip_suffix("Mi") {
        v.parse().unwrap_or(0)
    } else if let Some(v) = s.strip_suffix("Ki") {
        (v.parse::<f64>().unwrap_or(0.0) / 1024.0) as u32
    } else if let Some(v) = s.strip_suffix('T') {
        (v.parse::<f64>().unwrap_or(0.0) * 1_000_000_000.0 / 1_048_576.0) as u32
    } else if let Some(v) = s.strip_suffix('G') {
        (v.parse::<f64>().unwrap_or(0.0) * 1_000_000_000.0 / 1_048_576.0) as u32
    } else if let Some(v) = s.strip_suffix('M') {
        (v.parse::<f64>().unwrap_or(0.0) * 1_000_000.0 / 1_048_576.0) as u32
    } else if let Some(v) = s.strip_suffix('k') {
        (v.parse::<f64>().unwrap_or(0.0) * 1_000.0 / 1_048_576.0) as u32
    } else {
        // Plain bytes
        (s.parse::<f64>().unwrap_or(0.0) / 1_048_576.0) as u32
    }
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
fn days_since(ts: &DateTime<Utc>) -> u32 {
    Utc::now().signed_duration_since(*ts).num_days().max(0) as u32
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

/// Group nodes into pools keyed by (instance_type, region, capacity_type) and
/// derive the cluster's primary region and cloud provider.
///
/// Returns `(node_pools, primary_region, cloud_provider)`. Nodes missing the
/// instance-type label still count toward a pool (with `instance_type == ""`).
/// The primary region is the most-common non-empty region (ties broken
/// alphabetically for deterministic output); "" if no node reports a region.
/// The cloud provider is the first non-empty providerID-derived value.
pub fn aggregate_node_pools(nodes: &[Node]) -> (Vec<NodePool>, String, String) {
    let mut counts: HashMap<(String, String, String), u32> = HashMap::new();
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
        *counts
            .entry((meta.instance_type, meta.region, meta.capacity_type))
            .or_default() += 1;
    }

    let mut node_pools: Vec<NodePool> = counts
        .into_iter()
        .map(
            |((instance_type, region, capacity_type), node_count)| NodePool {
                instance_type,
                region,
                capacity_type,
                node_count,
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

pub async fn collect(config: &Config) -> Result<AgentSnapshot, CollectorError> {
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
    let pod_metrics = fetch_pod_metrics(&client).await;

    // Build pod → workload owner map (resolves ReplicaSet → Deployment).
    // Also returns the most-recent pod start time per namespace for activity tracking.
    let (pod_owner_map, pod_activity) = build_pod_owner_map(&client, &target_namespaces).await;

    // Aggregate pod metrics into per-(workload, container) usage totals
    let usage_map = aggregate_workload_usage(&pod_metrics, &pod_owner_map);

    // Build workload and namespace metrics
    let mut workloads: Vec<WorkloadMetrics> = Vec::new();
    let mut namespace_workload_counts: HashMap<String, u32> = HashMap::new();
    let mut namespace_costs: HashMap<String, f64> = HashMap::new();

    for ns in &target_namespaces {
        let mut ns_workload_count = 0u32;
        let mut ns_cost = 0.0f64;

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
                        let wl = build_workload_metrics(
                            ns,
                            "Deployment",
                            &name,
                            &container.name,
                            replicas,
                            obs_days,
                            &container.resources,
                            usage_map.get(&usage_key),
                        );
                        ns_cost += wl.estimated_monthly_cost_usd;
                        workloads.push(wl);
                    }
                    ns_workload_count += 1;
                }
            }
            Err(e) => warn!(namespace = %ns, error = %e, "failed_to_list_deployments"),
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
                        let wl = build_workload_metrics(
                            ns,
                            "StatefulSet",
                            &name,
                            &container.name,
                            replicas,
                            obs_days,
                            &container.resources,
                            usage_map.get(&usage_key),
                        );
                        ns_cost += wl.estimated_monthly_cost_usd;
                        workloads.push(wl);
                    }
                    ns_workload_count += 1;
                }
            }
            Err(e) => warn!(namespace = %ns, error = %e, "failed_to_list_statefulsets"),
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
                        let wl = build_workload_metrics(
                            ns,
                            "DaemonSet",
                            &name,
                            &container.name,
                            node_count,
                            obs_days,
                            &container.resources,
                            usage_map.get(&usage_key),
                        );
                        ns_cost += wl.estimated_monthly_cost_usd;
                        workloads.push(wl);
                    }
                    ns_workload_count += 1;
                }
            }
            Err(e) => warn!(namespace = %ns, error = %e, "failed_to_list_daemonsets"),
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
        let from_events = fetch_namespace_last_activity_from_events(&client, ns).await;
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

    info!(
        node_count,
        workloads = workloads.len(),
        namespaces = namespaces.len(),
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
    })
}

/// Build a map from (namespace, pod_name) → PodOwner by resolving owner references.
/// ReplicaSets are resolved up to their parent Deployment.
///
/// Also returns a map of namespace → days since most-recent pod start time, for activity tracking.
async fn build_pod_owner_map(
    client: &Client,
    namespaces: &[String],
) -> (HashMap<(String, String), PodOwner>, HashMap<String, u32>) {
    let mut owner_map: HashMap<(String, String), PodOwner> = HashMap::new();
    let mut pod_activity: HashMap<String, u32> = HashMap::new();

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
                    HashMap::new()
                }
            }
        };

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
                        owner_map.insert((ns.clone(), pod_name), o);
                    }
                }

                pod_activity.insert(ns.clone(), ns_most_recent_start);
            }
            Err(e) => {
                warn!(namespace = %ns, error = %e, "failed_to_list_pods");
            }
        }
    }

    (owner_map, pod_activity)
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
        last_active_timestamp: String::new(),
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
async fn fetch_namespace_last_activity_from_events(client: &Client, ns: &str) -> u32 {
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
            u32::MAX
        }
    }
}

/// Fetch current pod metrics from the metrics-server API.
/// Returns a map of (namespace, pod_name) → Vec<ContainerMetric>.
async fn fetch_pod_metrics(client: &Client) -> HashMap<(String, String), Vec<ContainerMetric>> {
    let req = match http::Request::builder()
        .uri("/apis/metrics.k8s.io/v1beta1/pods")
        .body(vec![])
    {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "failed_to_build_metrics_request");
            return HashMap::new();
        }
    };

    match client.request::<Value>(req).await {
        Ok(val) => parse_pod_metrics_json(&val),
        Err(e) => {
            warn!(error = %e, "metrics_server_unavailable_continuing_without_usage_data");
            HashMap::new()
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
                                .map(|s| parse_cpu_to_millicores(s) as f64)
                                .unwrap_or(0.0);
                            let mem_mi = usage
                                .get("memory")
                                .and_then(|v| v.as_str())
                                .map(|s| parse_memory_to_mib(s) as f64)
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
        assert_eq!(parse_memory_to_mib("256Ki"), 0); // rounds down
        assert_eq!(parse_memory_to_mib("1073741824"), 1024); // 1 GiB in bytes
        assert_eq!(parse_memory_to_mib("500Mi"), 500);
        assert_eq!(parse_memory_to_mib("4Gi"), 4096);
    }

    #[test]
    fn test_days_since_past() {
        let ts = Utc::now() - chrono::Duration::days(5);
        assert_eq!(days_since(&ts), 5);
    }

    #[test]
    fn test_days_since_future_clamps_to_zero() {
        let ts = Utc::now() + chrono::Duration::days(3);
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
}
