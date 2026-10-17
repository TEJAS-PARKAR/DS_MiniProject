"""Generate Kubernetes manifests for N independent TCP branch-node pods."""
import argparse
from pathlib import Path
import yaml

ALGORITHMS = {"ricart_agrawala", "suzuki_kasami", "raymond_tree"}


def build_manifest(nodes: int, algorithm: str, image: str) -> str:
    if nodes not in {5, 10, 20, 50}:
        raise ValueError("--nodes must be one of 5, 10, 20, or 50")
    if algorithm not in ALGORITHMS:
        raise ValueError(f"--algorithm must be one of {sorted(ALGORITHMS)}")

    app_config = {
        "system": {"algorithm": algorithm, "protocol_version": "1.0", "random_seed": 42},
        "nodes": [
            {"node_id": f"branch-{i}", "host": f"branch-{i}.branch-headless", "port": 9001, "health_port": 10001}
            for i in range(nodes)
        ],
        "network": {"mean_delay_ms": 5.0, "jitter_ms": 0.0, "message_loss_rate": 0.0, "connection_timeout_ms": 5000.0},
        "timeouts": {"message_delivery_timeout_ms": 3000.0, "token_response_timeout_ms": 10000.0,
                     "heartbeat_interval_ms": 1000.0, "heartbeat_timeout_ms": 5000.0,
                     "max_retries": 3, "retry_backoff_ms": 500.0},
        "workload": {"requests_per_node": 5, "mean_think_time_ms": 20.0,
                     "critical_section_duration_ms": 2.0, "arrival_distribution": "exponential", "workload_seed": 42},
        "experiment": {"duration_seconds": 60.0, "collect_metrics": True, "export_traces": True},
    }
    docs = [
        {
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "branch-headless"},
            "spec": {"clusterIP": "None", "publishNotReadyAddresses": True,
                     "selector": {"app": "dme-branch"},
                     "ports": [{"name": "tcp", "port": 9001, "targetPort": 9001},
                               {"name": "health", "port": 10001, "targetPort": 10001}]},
        },
        {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "dme-config"},
            "data": {"kubernetes.yaml": yaml.safe_dump(app_config, sort_keys=False)},
        },
        {
            "apiVersion": "apps/v1", "kind": "StatefulSet",
            "metadata": {"name": "branch"},
            "spec": {"serviceName": "branch-headless", "replicas": nodes,
                     "podManagementPolicy": "Parallel", "selector": {"matchLabels": {"app": "dme-branch"}},
                     "template": {
                         "metadata": {"labels": {"app": "dme-branch"}},
                         "spec": {"terminationGracePeriodSeconds": 15,
                                  "containers": [{
                                      "name": "branch-node", "image": image, "imagePullPolicy": "IfNotPresent",
                                      "command": ["/bin/sh", "-c"],
                                      "args": ['NODE_SUFFIX="${HOSTNAME##*-}"; exec python -m src.node.branch_node --node-id "branch-${NODE_SUFFIX}" --config /app/config/kubernetes.yaml --initial-token-holder branch-0 --enable-failure-detection'],
                                      "ports": [{"name": "tcp", "containerPort": 9001}, {"name": "health", "containerPort": 10001}],
                                      "readinessProbe": {"httpGet": {"path": "/health/ready", "port": "health"}, "initialDelaySeconds": 3, "periodSeconds": 5},
                                      "livenessProbe": {"httpGet": {"path": "/health/live", "port": "health"}, "initialDelaySeconds": 10, "periodSeconds": 10},
                                      "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "500m", "memory": "512Mi"}},
                                      "volumeMounts": [{"name": "config", "mountPath": "/app/config/kubernetes.yaml", "subPath": "kubernetes.yaml", "readOnly": True}],
                                  }], "volumes": [{"name": "config", "configMap": {"name": "dme-config"}}]},
                     }},
        },
    ]
    return "\n---\n".join(yaml.safe_dump(doc, sort_keys=False) for doc in docs)


def main():
    parser = argparse.ArgumentParser(description="Generate Kubernetes YAML for DME branch nodes")
    parser.add_argument("--nodes", type=int, choices=[5, 10, 20, 50], default=5)
    parser.add_argument("--algorithm", choices=sorted(ALGORITHMS), default="ricart_agrawala")
    parser.add_argument("--image", default="dme-benchmarking:latest")
    parser.add_argument("--output", default="k8s/generated-cluster.yaml")
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_manifest(args.nodes, args.algorithm, args.image), encoding="utf-8")
    print(f"Wrote {output} ({args.nodes} nodes, {args.algorithm})")


if __name__ == "__main__":
    main()
