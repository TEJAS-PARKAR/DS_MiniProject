"""Lightweight asynchronous HTTP server providing health and status diagnostic endpoints."""
import asyncio
import json
import logging
from typing import Callable, Dict, Any, Optional

logger = logging.getLogger("node.health")


class HealthServer:
    """Async HTTP diagnostic server serving /health/live, /health/ready, and /status."""

    def __init__(
        self,
        host: str,
        port: int,
        status_provider: Callable[[], Dict[str, Any]],
        ready_provider: Callable[[], bool],
    ) -> None:
        self.host = host
        self.port = port
        self.status_provider = status_provider
        self.ready_provider = ready_provider
        self._server: Optional[asyncio.Server] = None
        self._is_running = False

    async def start(self) -> None:
        """Start listening for HTTP requests."""
        self._server = await asyncio.start_server(self._handle_request, self.host, self.port)
        self._is_running = True
        logger.info("Health HTTP server listening on http://%s:%d", self.host, self.port)

    async def _handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle incoming HTTP request."""
        try:
            line = await reader.readline()
            if not line:
                return

            request_line = line.decode("utf-8", errors="replace").strip()
            parts = request_line.split()
            if len(parts) < 2:
                self._send_response(writer, 400, {"error": "Bad Request"})
                return

            method, path = parts[0], parts[1]

            # Drain remaining HTTP headers
            while True:
                header = await reader.readline()
                if not header or header in (b"\r\n", b"\n"):
                    break

            if method != "GET":
                await self._send_response(writer, 405, {"error": "Method Not Allowed"})
                return

            # Route paths
            if path in ("/health/live", "/health/liveness"):
                await self._send_response(writer, 200, {"status": "alive"})
            elif path in ("/health/ready", "/health/readiness"):
                if self.ready_provider():
                    await self._send_response(writer, 200, {"status": "ready"})
                else:
                    await self._send_response(writer, 503, {"status": "not_ready"})
            elif path in ("/status", "/health/status"):
                status_data = self.status_provider()
                await self._send_response(writer, 200, status_data)
            else:
                await self._send_response(writer, 404, {"error": "Not Found"})

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("Error processing health HTTP request: %s", e)
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def _send_response(
        self, writer: asyncio.StreamWriter, status_code: int, body_dict: Dict[str, Any]
    ) -> None:
        """Send HTTP response with JSON payload."""
        status_text = {
            200: "OK",
            400: "Bad Request",
            404: "Not Found",
            405: "Method Not Allowed",
            503: "Service Unavailable",
        }.get(status_code, "Unknown")

        body_bytes = json.dumps(body_dict, indent=2).encode("utf-8")
        headers = [
            f"HTTP/1.1 {status_code} {status_text}",
            "Content-Type: application/json; charset=utf-8",
            f"Content-Length: {len(body_bytes)}",
            "Connection: close",
            "",
            "",
        ]
        header_bytes = "\r\n".join(headers).encode("utf-8")
        writer.write(header_bytes + body_bytes)
        try:
            await writer.drain()
        except Exception:
            pass

    async def stop(self) -> None:
        """Stop HTTP server and release port."""
        self._is_running = False
        if self._server:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=1.0)
            except Exception:
                pass
            self._server = None
        logger.info("Health HTTP server on %s:%d stopped", self.host, self.port)
