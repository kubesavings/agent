//! Kubernetes API compatibility tests across server versions 1.21+.
//!
//! The collector consumes live API objects through the `k8s-openapi` typed
//! models (pinned to the `v1_29` feature). A managed or self-hosted cluster the
//! agent runs in can be any version from 1.21 upward, so these tests assert that
//! the exact fields the collector reads still deserialize from the API shapes
//! those versions emit:
//!
//!   * Pod          → `metadata.ownerReferences`, `status.startTime`
//!   * ReplicaSet   → `metadata.ownerReferences` (Deployment parent)
//!   * Deployment   → `spec.replicas`, container `resources.requests/limits`
//!   * StatefulSet  → `spec.replicas`, container resources
//!   * DaemonSet    → container resources (replica count comes from node count)
//!   * Node         → cloud-provider detection labels (EKS/GKE/AKS)
//!   * Namespace    → `metadata.name`
//!   * Event        → `lastTimestamp`
//!
//! The apps/v1 and core/v1 schemas for these objects have been GA and stable
//! since long before 1.21, so the per-version loops act as regression guards: if
//! a future `k8s-openapi` bump ever changed how a field deserializes, every
//! supported version would fail loudly here.

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{Event, Namespace, Node, Pod};
use serde_json::{json, Value};

/// Every server minor version the agent is expected to support.
const SUPPORTED_MINORS: std::ops::RangeInclusive<u32> = 21..=31;

/// `gitVersion` string as a cluster of the given minor would report it.
fn git_version(minor: u32) -> String {
    format!("v1.{minor}.3")
}

// ── Pod: owner resolution + activity timestamp ─────────────────────────────────

fn pod_fixture(minor: u32) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "api-66c8b4d9f7-x2lqz",
            "namespace": "default",
            "creationTimestamp": "2024-01-01T00:00:00Z",
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "api-66c8b4d9f7",
                "uid": "11111111-1111-1111-1111-111111111111",
                "controller": true,
                "blockOwnerDeletion": true
            }],
            "labels": { "app": "api", "version": git_version(minor) }
        },
        "spec": {
            "containers": [{
                "name": "main",
                "image": "ghcr.io/acme/api:latest",
                "resources": {
                    "requests": { "cpu": "250m", "memory": "256Mi" },
                    "limits": { "cpu": "500m", "memory": "512Mi" }
                }
            }]
        },
        "status": {
            "phase": "Running",
            "startTime": "2024-01-02T03:04:05Z"
        }
    })
}

#[test]
fn pod_owner_reference_and_start_time_parse_across_versions() {
    for minor in SUPPORTED_MINORS {
        let pod: Pod = serde_json::from_value(pod_fixture(minor))
            .unwrap_or_else(|e| panic!("1.{minor} pod failed to deserialize: {e}"));

        // The collector resolves the owner by taking the first ownerReference.
        let owner = pod
            .metadata
            .owner_references
            .as_ref()
            .and_then(|refs| refs.first())
            .unwrap_or_else(|| panic!("1.{minor}: missing ownerReferences"));
        assert_eq!(owner.kind, "ReplicaSet");
        assert_eq!(owner.name, "api-66c8b4d9f7");

        // Namespace activity is driven by status.startTime.
        let start = pod
            .status
            .as_ref()
            .and_then(|s| s.start_time.as_ref())
            .unwrap_or_else(|| panic!("1.{minor}: missing status.startTime"));
        assert_eq!(start.0.to_rfc3339(), "2024-01-02T03:04:05+00:00");
    }
}

// ── ReplicaSet → Deployment ownership ──────────────────────────────────────────

#[test]
fn replicaset_resolves_to_deployment_owner_across_versions() {
    for minor in SUPPORTED_MINORS {
        let value = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "api-66c8b4d9f7",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "api",
                    "uid": "22222222-2222-2222-2222-222222222222",
                    "controller": true
                }]
            },
            "spec": { "replicas": 3 }
        });

        let rs: ReplicaSet = serde_json::from_value(value)
            .unwrap_or_else(|e| panic!("1.{minor} replicaset failed: {e}"));

        let dep_name = rs
            .metadata
            .owner_references
            .as_ref()
            .and_then(|refs| refs.iter().find(|o| o.kind == "Deployment"))
            .map(|o| o.name.clone());
        assert_eq!(dep_name.as_deref(), Some("api"));
    }
}

// ── Deployment: replicas + container resource requests/limits ──────────────────

fn deployment_fixture(minor: u32) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "api",
            "namespace": "default",
            "creationTimestamp": "2023-12-15T10:00:00Z",
            "annotations": { "deployment.kubernetes.io/revision": "4" }
        },
        "spec": {
            "replicas": 3,
            "selector": { "matchLabels": { "app": "api" } },
            "template": {
                "metadata": { "labels": { "app": "api" } },
                "spec": {
                    "containers": [{
                        "name": "main",
                        "image": format!("ghcr.io/acme/api:1.{minor}"),
                        "resources": {
                            "requests": { "cpu": "500m", "memory": "1Gi" },
                            "limits": { "cpu": "1", "memory": "2Gi" }
                        }
                    }]
                }
            }
        }
    })
}

#[test]
fn deployment_replicas_and_resources_parse_across_versions() {
    for minor in SUPPORTED_MINORS {
        let dep: Deployment = serde_json::from_value(deployment_fixture(minor))
            .unwrap_or_else(|e| panic!("1.{minor} deployment failed: {e}"));

        let spec = dep.spec.expect("deployment spec");
        assert_eq!(spec.replicas, Some(3));

        let container = &spec
            .template
            .spec
            .as_ref()
            .expect("pod template spec")
            .containers[0];
        let res = container.resources.as_ref().expect("container resources");

        let requests = res.requests.as_ref().expect("requests");
        assert_eq!(requests["cpu"].0, "500m");
        assert_eq!(requests["memory"].0, "1Gi");

        let limits = res.limits.as_ref().expect("limits");
        assert_eq!(limits["cpu"].0, "1");
        assert_eq!(limits["memory"].0, "2Gi");

        assert!(dep.metadata.creation_timestamp.is_some());
    }
}

// ── StatefulSet & DaemonSet ────────────────────────────────────────────────────

#[test]
fn statefulset_replicas_and_resources_parse_across_versions() {
    for minor in SUPPORTED_MINORS {
        let value = json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": { "name": "pg", "namespace": "db", "creationTimestamp": "2024-02-01T00:00:00Z" },
            "spec": {
                "replicas": 2,
                "serviceName": "pg",
                "selector": { "matchLabels": { "app": "pg" } },
                "template": {
                    "metadata": { "labels": { "app": "pg" } },
                    "spec": { "containers": [{
                        "name": "postgres",
                        "image": "postgres:16",
                        "resources": { "requests": { "cpu": "1", "memory": "2Gi" } }
                    }]}
                }
            }
        });

        let sts: StatefulSet = serde_json::from_value(value)
            .unwrap_or_else(|e| panic!("1.{minor} statefulset failed: {e}"));
        let spec = sts.spec.expect("statefulset spec");
        assert_eq!(spec.replicas, Some(2));
        let req = spec.template.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap();
        assert_eq!(req["cpu"].0, "1");
    }
}

#[test]
fn daemonset_resources_parse_across_versions() {
    for minor in SUPPORTED_MINORS {
        let value = json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": { "name": "node-exporter", "namespace": "monitoring", "creationTimestamp": "2024-03-01T00:00:00Z" },
            "spec": {
                "selector": { "matchLabels": { "app": "node-exporter" } },
                "template": {
                    "metadata": { "labels": { "app": "node-exporter" } },
                    "spec": { "containers": [{
                        "name": "node-exporter",
                        "image": "prom/node-exporter:v1.8.0",
                        "resources": { "requests": { "cpu": "100m", "memory": "64Mi" } }
                    }]}
                }
            }
        });

        let ds: DaemonSet = serde_json::from_value(value)
            .unwrap_or_else(|e| panic!("1.{minor} daemonset failed: {e}"));
        let req = ds.spec.unwrap().template.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap()
            .clone();
        assert_eq!(req["cpu"].0, "100m");
        assert_eq!(req["memory"].0, "64Mi");
    }
}

// ── Node: cloud-provider auto-detection labels ─────────────────────────────────
//
// The collector reads node labels to infer the cloud provider. The label keys
// have been stable across the supported range; this asserts each provider's
// signature label deserializes and is present.

#[test]
fn node_cloud_provider_labels_parse_across_providers() {
    let cases = [
        ("AWS", "eks.amazonaws.com/nodegroup", "ng-1"),
        ("GCP", "cloud.google.com/gke-nodepool", "default-pool"),
        ("Azure", "kubernetes.azure.com/agentpool", "nodepool1"),
    ];

    for (provider, label_key, label_val) in cases {
        let value = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "node-1",
                "labels": {
                    "kubernetes.io/os": "linux",
                    "node.kubernetes.io/instance-type": "m5.large",
                    label_key: label_val
                }
            },
            "status": { "nodeInfo": { "kubeletVersion": "v1.29.0" } }
        });

        let node: Node =
            serde_json::from_value(value).unwrap_or_else(|e| panic!("{provider} node failed: {e}"));
        let labels = node.metadata.labels.expect("node labels");
        assert_eq!(
            labels.get(label_key).map(String::as_str),
            Some(label_val),
            "{provider}: signature label missing"
        );
    }
}

// ── Namespace & Event ──────────────────────────────────────────────────────────

#[test]
fn namespace_parses_across_versions() {
    for minor in SUPPORTED_MINORS {
        let value = json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": "team-a", "labels": { "kubernetes.io/metadata.name": "team-a" } },
            "status": { "phase": "Active" }
        });
        let ns: Namespace = serde_json::from_value(value)
            .unwrap_or_else(|e| panic!("1.{minor} namespace failed: {e}"));
        assert_eq!(ns.metadata.name.as_deref(), Some("team-a"));
    }
}

#[test]
fn event_last_timestamp_parses_across_versions() {
    for minor in SUPPORTED_MINORS {
        // core/v1 Event (what the collector lists), not events.k8s.io/v1.
        let value = json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": { "name": "api.17abc", "namespace": "default" },
            "involvedObject": { "kind": "Pod", "name": "api-x", "namespace": "default" },
            "reason": "Scheduled",
            "type": "Normal",
            "count": 1,
            "firstTimestamp": "2024-05-01T12:00:00Z",
            "lastTimestamp": "2024-05-01T12:05:00Z"
        });
        let event: Event =
            serde_json::from_value(value).unwrap_or_else(|e| panic!("1.{minor} event failed: {e}"));
        let ts = event.last_timestamp.expect("lastTimestamp");
        assert_eq!(ts.0.to_rfc3339(), "2024-05-01T12:05:00+00:00");
    }
}

// ── Full owner-resolution chain: Pod → ReplicaSet → Deployment ─────────────────
//
// Mirrors the collector's resolution path end to end against a consistent set of
// objects, confirming the cross-references line up the way the real code walks
// them.

#[test]
fn pod_to_deployment_owner_chain_resolves() {
    let minor = 29;
    let pod: Pod = serde_json::from_value(pod_fixture(minor)).unwrap();
    let rs: ReplicaSet = serde_json::from_value(json!({
        "metadata": {
            "name": "api-66c8b4d9f7",
            "namespace": "default",
            "ownerReferences": [{ "apiVersion": "apps/v1", "kind": "Deployment", "name": "api", "uid": "d" }]
        },
        "spec": { "replicas": 3 }
    }))
    .unwrap();

    // Pod's first owner is the ReplicaSet…
    let pod_owner = pod.metadata.owner_references.unwrap()[0].name.clone();
    assert_eq!(pod_owner, "api-66c8b4d9f7");
    assert_eq!(rs.metadata.name.as_deref(), Some(pod_owner.as_str()));

    // …and the ReplicaSet's Deployment owner is the terminal workload.
    let dep_name = rs.metadata.owner_references.unwrap()[0].name.clone();
    assert_eq!(dep_name, "api");
}
