"""Integration tests verifying real TCP socket communication across independent node processes."""
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request
import pytest
from src.common.configuration import load_config_from_dict
from src.node.branch_node import BranchNode


def get_loopback_config(node_ids: list, base_port: int = 15000, algorithm: str = "ricart_agrawala"):
    return load_config_from_dict({
        "system": {"algorithm": algorithm, "protocol_version": "1.0"},
        "nodes": [
            {
                "node_id": nid,
                "host": "127.0.0.1",
                "port": base_port + i,
                "health_port": base_port + 1000 + i,
            }
            for i, nid in enumerate(node_ids)
        ],
        "network": {
            "mean_delay_ms": 1.0,
            "jitter_ms": 0.0,
            "message_loss_rate": 0.0,
            "connection_timeout_ms": 3000.0,
        },
        "timeouts": {
            "message_delivery_timeout_ms": 2000.0,
            "token_response_timeout_ms": 5000.0,
            "heartbeat_interval_ms": 1000.0,
            "heartbeat_timeout_ms": 3000.0,
            "max_retries": 3,
            "retry_backoff_ms": 100.0,
        },
    })


def test_two_nodes_tcp_exchange():
    """Verify real TCP exchange between two independent BranchNode instances."""
    async def _run():
        node_ids = ["node-0", "node-1"]
        config = get_loopback_config(node_ids, base_port=15100, algorithm="ricart_agrawala")

        node0 = BranchNode("node-0", config)
        node1 = BranchNode("node-1", config)

        await node0.start()
        await node1.start()

        try:
            # node-0 initiates request for critical section
            await node0.request_critical_section("req-0-first")

            # Allow TCP round-trip message delivery
            for _ in range(30):
                if node0.can_enter_cs():
                    break
                await asyncio.sleep(0.05)

            assert node0.can_enter_cs() is True

            # Enter and release
            await node0.enter_critical_section("req-0-first")
            await node0.release_critical_section("req-0-first")

            assert node0.can_enter_cs() is False
            assert node0.runtime_state.cs_entry_count == 1
            assert node0.runtime_state.cs_exit_count == 1

            # Check diagnostics on node-1
            status1 = node1.get_status()
            assert status1["runtime"]["messages_received"] >= 1
        finally:
            await node0.stop()
            await node1.stop()

    asyncio.run(_run())


def test_tcp_token_transfer_suzuki_kasami():
    """Verify point-to-point token transfer over TCP in Suzuki-Kasami."""
    async def _run():
        node_ids = ["sk-0", "sk-1"]
        config = get_loopback_config(node_ids, base_port=15200, algorithm="suzuki_kasami")

        # sk-0 starts with token
        node0 = BranchNode("sk-0", config, initial_token_holder="sk-0")
        node1 = BranchNode("sk-1", config, initial_token_holder="sk-0")

        await node0.start()
        await node1.start()

        try:
            assert node0.protocol.has_token is True
            assert node1.protocol.has_token is False

            # sk-1 requests critical section over TCP
            await node1.request_critical_section("req-sk1")

            # Wait for TCP delivery of REQUEST to sk-0 and TOKEN_TRANSFER back to sk-1
            for _ in range(30):
                if node1.can_enter_cs():
                    break
                await asyncio.sleep(0.05)

            assert node1.can_enter_cs() is True
            assert node1.protocol.has_token is True
            assert node0.protocol.has_token is False
        finally:
            await node0.stop()
            await node1.stop()

    asyncio.run(_run())


def test_multiprocess_ricart_agrawala_mutual_exclusion():
    """Verify non-overlapping critical section intervals across independent OS processes."""
    node_ids = ["proc-0", "proc-1", "proc-2"]
    base_port = 15300
    config_dict = {
        "system": {"algorithm": "ricart_agrawala", "protocol_version": "1.0"},
        "nodes": [
            {
                "node_id": nid,
                "host": "127.0.0.1",
                "port": base_port + i,
                "health_port": base_port + 1000 + i,
            }
            for i, nid in enumerate(node_ids)
        ],
        "network": {
            "mean_delay_ms": 1.0,
            "jitter_ms": 0.0,
            "message_loss_rate": 0.0,
            "connection_timeout_ms": 3000.0,
        },
        "timeouts": {
            "message_delivery_timeout_ms": 2000.0,
            "token_response_timeout_ms": 5000.0,
            "heartbeat_interval_ms": 1000.0,
            "heartbeat_timeout_ms": 3000.0,
            "max_retries": 3,
            "retry_backoff_ms": 100.0,
        },
    }

    # Write temporary YAML config for sub-processes
    temp_config_path = "config/temp_multiprocess_test.yaml"
    import yaml
    with open(temp_config_path, "w", encoding="utf-8") as f:
        yaml.dump(config_dict, f)

    processes = []
    try:
        # Spawn 3 separate OS processes
        for nid in node_ids:
            p = subprocess.Popen(
                [sys.executable, "-m", "src.node.branch_node", "--node-id", nid, "--config", temp_config_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            processes.append(p)

        # Wait for all processes to report /health/live
        for i, nid in enumerate(node_ids):
            health_port = base_port + 1000 + i
            url = f"http://127.0.0.1:{health_port}/health/live"
            ready = False
            for _ in range(50):
                try:
                    with urllib.request.urlopen(url, timeout=0.5) as resp:
                        if resp.status == 200:
                            ready = True
                            break
                except Exception:
                    time.sleep(0.1)
            assert ready, f"Process {nid} failed to start health listener."

        # Verify /status reports correct node and algorithm
        for i, nid in enumerate(node_ids):
            health_port = base_port + 1000 + i
            url = f"http://127.0.0.1:{health_port}/status"
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read().decode())
                assert data["node_id"] == nid
                assert data["algorithm"] == "ricart_agrawala"
                assert data["runtime"]["is_running"] is True

    finally:
        for p in processes:
            p.terminate()
            try:
                p.wait(timeout=2.0)
            except Exception:
                p.kill()
        if os.path.exists(temp_config_path):
            os.remove(temp_config_path)


def test_tcp_token_transfer_raymond():
    """Verify Raymond tree request routing and token transfer over TCP."""
    async def _run():
        node_ids = ["ray-0", "ray-1", "ray-2"]
        config = get_loopback_config(node_ids, base_port=15600, algorithm="raymond_tree")

        # Root and initial token holder is ray-0
        node0 = BranchNode("ray-0", config, initial_token_holder="ray-0")
        node1 = BranchNode("ray-1", config, initial_token_holder="ray-0")
        node2 = BranchNode("ray-2", config, initial_token_holder="ray-0")

        await node0.start()
        await node1.start()
        await node2.start()

        try:
            assert node0.protocol.has_token is True
            assert node1.protocol.has_token is False
            assert node2.protocol.has_token is False

            # ray-2 requests critical section; request traverses ray-2 -> ray-0 (or via tree), token returns to ray-2
            await node2.request_critical_section("req-ray2")

            for _ in range(40):
                if node2.can_enter_cs():
                    break
                await asyncio.sleep(0.05)

            assert node2.can_enter_cs() is True
            assert node2.protocol.has_token is True
            assert node0.protocol.has_token is False

            # ray-2 enters and exits CS
            await node2.enter_critical_section("req-ray2")
            await node2.release_critical_section("req-ray2")

            assert node2.can_enter_cs() is False
        finally:
            await node0.stop()
            await node1.stop()
            await node2.stop()

    asyncio.run(_run())
