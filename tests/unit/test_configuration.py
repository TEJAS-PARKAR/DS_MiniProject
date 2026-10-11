"""Unit tests for configuration loading and validation."""
import pytest
from pathlib import Path
from src.common.configuration import (
    load_config,
    load_config_from_dict,
    ConfigurationError,
    AppConfig,
    NodeConfig,
    SystemConfig,
    NetworkConfig,
    TimeoutConfig,
    WorkloadConfig,
)


def test_load_default_yaml_config():
    config_path = Path("config/defaults.yaml")
    assert config_path.is_file(), "config/defaults.yaml must exist"
    app_config = load_config(str(config_path))

    assert isinstance(app_config, AppConfig)
    assert app_config.system.algorithm == "ricart_agrawala"
    assert len(app_config.nodes) == 4
    assert app_config.nodes[0].node_id == "branch-0"
    assert app_config.nodes[0].port == 9001
    assert app_config.network.mean_delay_ms == 10.0
    assert app_config.timeouts.max_retries == 3
    assert app_config.workload.requests_per_node == 10


def test_duplicate_node_ids_rejected():
    raw = {
        "system": {"algorithm": "ricart_agrawala"},
        "nodes": [
            {"node_id": "branch-0", "host": "127.0.0.1", "port": 9001},
            {"node_id": "branch-0", "host": "127.0.0.1", "port": 9002},
        ],
    }
    with pytest.raises(ConfigurationError, match="Duplicate node_id detected"):
        load_config_from_dict(raw)


def test_duplicate_node_addresses_rejected():
    raw = {
        "system": {"algorithm": "suzuki_kasami"},
        "nodes": [
            {"node_id": "branch-0", "host": "127.0.0.1", "port": 9001},
            {"node_id": "branch-1", "host": "127.0.0.1", "port": 9001},
        ],
    }
    with pytest.raises(ConfigurationError, match="Duplicate node address detected"):
        load_config_from_dict(raw)


def test_invalid_algorithm_rejected():
    raw = {
        "system": {"algorithm": "non_existent_algo"},
        "nodes": [{"node_id": "branch-0", "host": "127.0.0.1", "port": 9001}],
    }
    with pytest.raises(ConfigurationError, match="Unsupported algorithm"):
        load_config_from_dict(raw)


def test_port_out_of_range():
    raw = {
        "system": {"algorithm": "raymond_tree"},
        "nodes": [{"node_id": "branch-0", "host": "127.0.0.1", "port": 70000}],
    }
    with pytest.raises(ConfigurationError, match="Port must be an integer between 1 and 65535"):
        load_config_from_dict(raw)


def test_empty_nodes_list_rejected():
    raw = {
        "system": {"algorithm": "ricart_agrawala"},
        "nodes": [],
    }
    with pytest.raises(ConfigurationError, match="Nodes list cannot be empty"):
        load_config_from_dict(raw)


def test_negative_delay_rejected():
    raw = {
        "system": {"algorithm": "ricart_agrawala"},
        "nodes": [{"node_id": "branch-0", "host": "127.0.0.1", "port": 9001}],
        "network": {"mean_delay_ms": -5.0},
    }
    with pytest.raises(ConfigurationError, match="mean_delay_ms cannot be negative"):
        load_config_from_dict(raw)


def test_invalid_loss_rate_rejected():
    raw = {
        "system": {"algorithm": "ricart_agrawala"},
        "nodes": [{"node_id": "branch-0", "host": "127.0.0.1", "port": 9001}],
        "network": {"message_loss_rate": 1.5},
    }
    with pytest.raises(ConfigurationError, match="message_loss_rate must be between 0.0 and 1.0"):
        load_config_from_dict(raw)


def test_heartbeat_timeout_relationship():
    raw = {
        "system": {"algorithm": "ricart_agrawala"},
        "nodes": [{"node_id": "branch-0", "host": "127.0.0.1", "port": 9001}],
        "timeouts": {
            "heartbeat_interval_ms": 2000.0,
            "heartbeat_timeout_ms": 1000.0,  # invalid: must be > interval
        },
    }
    with pytest.raises(ConfigurationError, match="heartbeat_timeout_ms.*strictly greater"):
        load_config_from_dict(raw)


def test_missing_file_raises_error():
    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        load_config("non_existent_config.yaml")
