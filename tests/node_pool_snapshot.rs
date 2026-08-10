//! Snapshot integration test for the node-pool pricing metadata.
//!
//! Verifies the end-to-end path the collector takes: live Node objects (carrying
//! the standard instance-type / region / capacity-type labels) are aggregated into
//! `NodePool` entries, placed on an `AgentSnapshot`, and survive the protobuf
//! encode/decode round-trip the sender performs before POSTing to the backend.

use k8s_openapi::api::core::v1::Node;
use kubesavings_agent::collector::aggregate_node_pools;
use kubesavings_agent::types::AgentSnapshot;
use prost::Message;
use serde_json::json;

fn node(instance_type: &str, region: &str, provider_id: &str) -> Node {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "node",
            "labels": {
                "kubernetes.io/os": "linux",
                "node.kubernetes.io/instance-type": instance_type,
                "topology.kubernetes.io/region": region
            }
        },
        "spec": { "providerID": provider_id }
    }))
    .expect("node should deserialize")
}

#[test]
fn snapshot_carries_node_pools_and_region_through_protobuf() {
    let nodes = vec![
        node("m5.xlarge", "us-east-1", "aws:///us-east-1a/i-1"),
        node("m5.xlarge", "us-east-1", "aws:///us-east-1a/i-2"),
        node("c5.2xlarge", "us-east-1", "aws:///us-east-1b/i-3"),
    ];

    let (node_pools, region, cloud_provider) = aggregate_node_pools(&nodes);

    let snapshot = AgentSnapshot {
        k8s_version: "1.29".to_string(),
        node_count: nodes.len() as u32,
        cloud_provider,
        workloads: vec![],
        namespaces: vec![],
        estimated_cluster_cost_usd: 0.0,
        collected_at: "2026-06-26T00:00:00+00:00".to_string(),
        region,
        node_pools,
        agent_version: "1.2.0".to_string(),
    };

    // Round-trip through protobuf, exactly as the sender does.
    let bytes = snapshot.encode_to_vec();
    let decoded = AgentSnapshot::decode(bytes.as_slice()).expect("snapshot should decode");

    assert_eq!(decoded.region, "us-east-1");
    assert_eq!(decoded.cloud_provider, "aws");
    assert!(
        !decoded.node_pools.is_empty(),
        "node_pools must be populated"
    );

    let total: u32 = decoded.node_pools.iter().map(|p| p.node_count).sum();
    assert_eq!(total, 3);

    // The two m5.xlarge nodes collapse into one pool; c5.2xlarge is its own.
    assert_eq!(decoded.node_pools.len(), 2);
    let m5 = decoded
        .node_pools
        .iter()
        .find(|p| p.instance_type == "m5.xlarge")
        .expect("m5.xlarge pool");
    assert_eq!(m5.node_count, 2);
    assert_eq!(m5.region, "us-east-1");
    assert_eq!(m5.capacity_type, "on-demand");
}
