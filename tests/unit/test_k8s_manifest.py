import yaml
from k8s.generate_manifest import build_manifest


def test_manifest_has_one_pod_per_logical_process_and_tcp_ports():
    docs = list(yaml.safe_load_all(build_manifest(5, "suzuki_kasami", "test-image:latest")))
    stateful_set = next(d for d in docs if d["kind"] == "StatefulSet")
    config_map = next(d for d in docs if d["kind"] == "ConfigMap")
    assert stateful_set["spec"]["replicas"] == 5
    assert stateful_set["spec"]["template"]["spec"]["containers"][0]["image"] == "test-image:latest"
    app_config = yaml.safe_load(config_map["data"]["kubernetes.yaml"])
    assert len(app_config["nodes"]) == 5
    assert app_config["system"]["algorithm"] == "suzuki_kasami"


def test_manifest_rejects_unsupported_sizes():
    try:
        build_manifest(6, "ricart_agrawala", "test-image:latest")
    except ValueError as exc:
        assert "5, 10, 20, or 50" in str(exc)
    else:
        raise AssertionError("unsupported node count should be rejected")
