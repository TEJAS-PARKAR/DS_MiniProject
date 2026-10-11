"""Unit tests for the diagnostic HealthServer HTTP endpoints."""
import asyncio
import json
import urllib.request
import urllib.error
import pytest
from src.node.health import HealthServer


def test_health_endpoints():
    async def _run():
        ready_state = {"ready": False}
        status_data = {
            "node_id": "test-node",
            "algorithm": "ricart_agrawala",
            "messages_count": 42,
        }

        server = HealthServer(
            host="127.0.0.1",
            port=19099,
            status_provider=lambda: status_data,
            ready_provider=lambda: ready_state["ready"],
        )
        await server.start()

        def http_get(url: str):
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                return resp.status, json.loads(resp.read().decode())

        try:
            base_url = "http://127.0.0.1:19099"

            # 1. /health/live (run in thread to avoid blocking the event loop)
            status, data = await asyncio.to_thread(http_get, f"{base_url}/health/live")
            assert status == 200
            assert data["status"] == "alive"

            # 2. /health/ready when NOT ready -> 503
            def get_ready_expect_503():
                try:
                    urllib.request.urlopen(f"{base_url}/health/ready", timeout=2.0)
                    return 200
                except urllib.error.HTTPError as e:
                    return e.code

            code = await asyncio.to_thread(get_ready_expect_503)
            assert code == 503

            # Make ready
            ready_state["ready"] = True
            status, data = await asyncio.to_thread(http_get, f"{base_url}/health/ready")
            assert status == 200
            assert data["status"] == "ready"

            # 3. /status
            status, data = await asyncio.to_thread(http_get, f"{base_url}/status")
            assert status == 200
            assert data["node_id"] == "test-node"
            assert data["algorithm"] == "ricart_agrawala"
            assert data["messages_count"] == 42

            # 4. Unknown endpoint -> 404
            def get_unknown_expect_404():
                try:
                    urllib.request.urlopen(f"{base_url}/unknown", timeout=2.0)
                    return 200
                except urllib.error.HTTPError as e:
                    return e.code

            code404 = await asyncio.to_thread(get_unknown_expect_404)
            assert code404 == 404

        finally:
            await server.stop()

    asyncio.run(_run())
