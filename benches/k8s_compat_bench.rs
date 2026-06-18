//! Benchmarks for the Kubernetes API-compatibility hot path.
//!
//! On every CronJob run the agent deserializes the live cluster's API objects
//! and metrics-server response before doing any cost math. These benches measure
//! that decode path against the response shapes emitted by server versions 1.21+,
//! so a regression in the parsing cost (e.g. a `k8s-openapi` bump) shows up here.
//!
//! Only the crate's public surface is exercised — the typed `k8s-openapi` models
//! the collector consumes, plus the public quantity parsers — mirroring what the
//! collector actually does per object.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use serde_json::{json, Value};

use kubesavings_agent::collector::{parse_cpu_to_millicores, parse_memory_to_mib};

/// Server minors the agent supports; benched at the endpoints and middle.
const VERSIONS: &[u32] = &[21, 26, 31];

fn pod_json(minor: u32) -> String {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "api-66c8b4d9f7-x2lqz",
            "namespace": "default",
            "creationTimestamp": "2024-01-01T00:00:00Z",
            "ownerReferences": [{
                "apiVersion": "apps/v1", "kind": "ReplicaSet",
                "name": "api-66c8b4d9f7", "uid": "1", "controller": true
            }],
            "labels": { "app": "api", "pod-template-hash": "66c8b4d9f7" }
        },
        "spec": { "containers": [{
            "name": "main",
            "image": format!("ghcr.io/acme/api:1.{minor}"),
            "resources": {
                "requests": { "cpu": "250m", "memory": "256Mi" },
                "limits": { "cpu": "500m", "memory": "512Mi" }
            }
        }]},
        "status": { "phase": "Running", "startTime": "2024-01-02T03:04:05Z" }
    })
    .to_string()
}

fn deployment_json(minor: u32) -> String {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "api", "namespace": "default", "creationTimestamp": "2023-12-15T10:00:00Z" },
        "spec": {
            "replicas": 3,
            "selector": { "matchLabels": { "app": "api" } },
            "template": {
                "metadata": { "labels": { "app": "api" } },
                "spec": { "containers": [{
                    "name": "main",
                    "image": format!("ghcr.io/acme/api:1.{minor}"),
                    "resources": {
                        "requests": { "cpu": "500m", "memory": "1Gi" },
                        "limits": { "cpu": "1", "memory": "2Gi" }
                    }
                }]}
            }
        }
    })
    .to_string()
}

/// metrics-server `PodMetricsList` with `pods` pods, using the unit convention a
/// given minor's in-cluster metrics-server emits (older = nanocores/Ki).
fn metrics_json(minor: u32, pods: usize) -> String {
    let (cpu, mem) = if minor <= 24 {
        ("250000000n", "262144Ki")
    } else {
        ("250m", "256Mi")
    };
    let items: Vec<Value> = (0..pods)
        .map(|i| {
            json!({
                "metadata": { "name": format!("api-{i}"), "namespace": "default" },
                "containers": [{ "name": "main", "usage": { "cpu": cpu, "memory": mem } }]
            })
        })
        .collect();
    json!({
        "kind": "PodMetricsList",
        "apiVersion": "metrics.k8s.io/v1beta1",
        "items": items
    })
    .to_string()
}

fn bench_deserialize_pod(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize_pod");
    for &minor in VERSIONS {
        let body = pod_json(minor);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("1.{minor}")),
            &body,
            |b, s| {
                b.iter(|| {
                    let pod: Pod = serde_json::from_str(black_box(s)).unwrap();
                    black_box(pod)
                })
            },
        );
    }
    group.finish();
}

fn bench_deserialize_deployment(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize_deployment");
    for &minor in VERSIONS {
        let body = deployment_json(minor);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("1.{minor}")),
            &body,
            |b, s| {
                b.iter(|| {
                    let dep: Deployment = serde_json::from_str(black_box(s)).unwrap();
                    black_box(dep)
                })
            },
        );
    }
    group.finish();
}

/// The metrics path the collector takes: decode the response body to a generic
/// `Value` (as `kube::Client::request::<Value>` does), then walk it.
fn bench_parse_metrics_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_metrics_list_100pods");
    for &minor in VERSIONS {
        let body = metrics_json(minor, 100);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("1.{minor}")),
            &body,
            |b, s| {
                b.iter(|| {
                    let v: Value = serde_json::from_str(black_box(s)).unwrap();
                    // Mirror the collector's per-container quantity parsing.
                    let mut total = 0u32;
                    if let Some(items) = v.get("items").and_then(|i| i.as_array()) {
                        for item in items {
                            if let Some(cs) = item.get("containers").and_then(|c| c.as_array()) {
                                for cont in cs {
                                    if let Some(u) = cont.get("usage") {
                                        if let Some(cpu) = u.get("cpu").and_then(|x| x.as_str()) {
                                            total += parse_cpu_to_millicores(cpu);
                                        }
                                        if let Some(mem) = u.get("memory").and_then(|x| x.as_str())
                                        {
                                            total += parse_memory_to_mib(mem);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    black_box(total)
                })
            },
        );
    }
    group.finish();
}

/// Quantity-string parsing isolated by the unit conventions different versions
/// emit (nanocores/Ki on older metrics-server, millicores/Mi on newer).
fn bench_quantity_parsing_by_version(c: &mut Criterion) {
    let cases = [
        ("nanocores_ki", "250000000n", "262144Ki"),
        ("millicores_mi", "250m", "256Mi"),
        ("whole_gi", "1", "1Gi"),
    ];
    let mut group = c.benchmark_group("quantity_parsing");
    for (label, cpu, mem) in cases {
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &(cpu, mem),
            |b, &(cpu, mem)| {
                b.iter(|| {
                    (
                        parse_cpu_to_millicores(black_box(cpu)),
                        parse_memory_to_mib(black_box(mem)),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_deserialize_pod,
    bench_deserialize_deployment,
    bench_parse_metrics_list,
    bench_quantity_parsing_by_version,
);
criterion_main!(benches);
