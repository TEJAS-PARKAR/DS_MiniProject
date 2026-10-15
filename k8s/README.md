# Kubernetes deployment

This folder deploys the existing real-TCP `BranchNode` runtime as independent Kubernetes pods. It is distinct from the discrete-event simulator benchmark runner in `src/experiments/runner.py`; simulator metrics must not be described as measurements from real Kubernetes networking.

## Build and load the image

Build the repository's Docker image as `distributed-blood-bank:latest` and make it available to your cluster. For Minikube, use its Docker daemon before building; for kind, build locally and load the image into kind. For a remote cluster, push the image to a registry and pass that image to the generator.

## Generate and deploy

From the repository root:

```bash
python k8s/generate_manifest.py --nodes 5 --algorithm ricart_agrawala --output k8s/generated-cluster.yaml
kubectl apply -f k8s/generated-cluster.yaml
kubectl get pods -l app=dme-branch -w
kubectl logs branch-0
```

Supported sizes are 5, 10, 20, and 50. Algorithms are `ricart_agrawala`, `suzuki_kasami`, and `raymond_tree`. To change algorithm or process count, regenerate the manifest and apply it again. This is a protocol-node deployment, not an automatic workload/metrics controller for live pods yet.

The StatefulSet uses a headless service for stable pod DNS, TCP port 9001, health port 10001, readiness/liveness probes, and one independent node process per pod. The first pod is configured as the initial token holder for token algorithms. For Ricart–Agrawala this argument is ignored by the algorithm.

## Important limitations

- The node runtime does not yet expose an experiment API to schedule identical workloads across live pods or aggregate all pod events into the required benchmark CSV. Use the discrete-event experiment runner for repeatable algorithm comparisons until that live-cluster controller/collector is implemented.
- The simulator supports synthetic message delay and loss; the Kubernetes manifests currently configure a reliable runtime baseline and do not install a network fault-injection layer.
- Heartbeats and suspicion are not proof of failure. Do not claim token recovery or fault tolerance beyond the implemented fail-closed policies.
- Synthetic blood-bank domain models are not clinically validated and must not be used for transfusion decisions.
