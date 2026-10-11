"""Asynchronous TCP transport layer with explicit framing, connection management, and retries."""
import asyncio
import json
import logging
from typing import Callable, Coroutine, Dict, List, Optional, Tuple, Any
from src.common.messages import ProtocolMessage, MessageType
from src.common.configuration import AppConfig

MAX_FRAME_SIZE = 65536  # 64 KB maximum frame size
DEFAULT_ENCODING = "utf-8"

logger = logging.getLogger("common.network")


class FramingError(Exception):
    """Base exception for framing errors."""
    pass


class FrameSizeExceededError(FramingError):
    """Raised when an incoming frame exceeds the maximum allowed size."""
    pass


class MalformedFrameError(FramingError):
    """Raised when an incoming frame cannot be decoded into a valid ProtocolMessage."""
    pass


def encode_frame(message: ProtocolMessage) -> bytes:
    """Encode a ProtocolMessage into a newline-delimited UTF-8 byte frame.

    Raises:
        FrameSizeExceededError: If the serialized frame exceeds MAX_FRAME_SIZE.
    """
    json_str = message.to_json()
    encoded = (json_str + "\n").encode(DEFAULT_ENCODING)
    if len(encoded) > MAX_FRAME_SIZE:
        raise FrameSizeExceededError(
            f"Serialized frame size ({len(encoded)} bytes) exceeds maximum ({MAX_FRAME_SIZE} bytes)."
        )
    return encoded


class MessageFramer:
    """Stream framer that decodes newline-delimited UTF-8 JSON frames from raw TCP bytes."""

    def __init__(self, max_frame_size: int = MAX_FRAME_SIZE) -> None:
        self.max_frame_size = max_frame_size
        self._buffer = bytearray()

    def feed_data(self, chunk: bytes) -> List[ProtocolMessage]:
        """Feed raw TCP bytes into the framer buffer and extract all complete frames.

        Returns:
            List of successfully decoded ProtocolMessage instances.

        Raises:
            FrameSizeExceededError: If buffer without delimiter exceeds max_frame_size.
            MalformedFrameError: If a frame contains malformed JSON or invalid schema.
        """
        self._buffer.extend(chunk)
        messages: List[ProtocolMessage] = []

        while True:
            # Check for newline delimiter
            newline_pos = self._buffer.find(b"\n")
            if newline_pos == -1:
                # No delimiter yet: check buffer size limit
                if len(self._buffer) > self.max_frame_size:
                    overflow_len = len(self._buffer)
                    self._buffer.clear()
                    raise FrameSizeExceededError(
                        f"Unfinished frame size ({overflow_len} bytes) exceeded maximum ({self.max_frame_size} bytes)."
                    )
                break

            # Frame delimited at newline_pos
            if newline_pos > self.max_frame_size:
                self._buffer.clear()
                raise FrameSizeExceededError(
                    f"Frame size ({newline_pos} bytes) exceeded maximum ({self.max_frame_size} bytes)."
                )

            line_bytes = self._buffer[:newline_pos]
            del self._buffer[:newline_pos + 1]

            # Strip carriage returns if present
            line_str = line_bytes.decode(DEFAULT_ENCODING, errors="replace").strip()
            if not line_str:
                # Empty frame / keepalive delimiter
                continue

            try:
                msg = ProtocolMessage.from_json(line_str)
                messages.append(msg)
            except Exception as e:
                raise MalformedFrameError(f"Failed to parse ProtocolMessage frame: {e}") from e

        return messages


class AsyncTcpServer:
    """Asynchronous TCP server listening on host:port and delivering frames to callback."""

    def __init__(
        self,
        host: str,
        port: int,
        on_message_received: Callable[[ProtocolMessage], Coroutine[Any, Any, None]],
        max_frame_size: int = MAX_FRAME_SIZE,
    ) -> None:
        self.host = host
        self.port = port
        self.on_message_received = on_message_received
        self.max_frame_size = max_frame_size
        self._server: Optional[asyncio.Server] = None
        self._is_running = False
        self._active_clients: set = set()
        self._handler_tasks: set = set()

    @property
    def is_serving(self) -> bool:
        return self._server is not None and self._server.is_serving()

    async def start(self) -> None:
        """Start listening on the configured address."""
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self._is_running = True
        logger.info("TCP server listening on %s:%d", self.host, self.port)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Process incoming client stream."""
        framer = MessageFramer(max_frame_size=self.max_frame_size)
        peer_name = writer.get_extra_info("peername")
        self._active_clients.add(writer)
        task = asyncio.current_task()
        if task:
            self._handler_tasks.add(task)
        try:
            while self._is_running:
                chunk = await reader.read(4096)
                if not chunk:
                    break  # Client closed connection
                try:
                    messages = framer.feed_data(chunk)
                    for msg in messages:
                        await self.on_message_received(msg)
                except FramingError as fe:
                    logger.warning("Framing error from client %s: %s", peer_name, fe)
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("Error handling client %s: %s", peer_name, e)
        finally:
            if task:
                self._handler_tasks.discard(task)
            self._active_clients.discard(writer)
            try:
                writer.close()
            except Exception:
                pass

    async def stop(self) -> None:
        """Stop server and release sockets."""
        self._is_running = False
        for task in list(self._handler_tasks):
            task.cancel()
        for writer in list(self._active_clients):
            try:
                writer.close()
            except Exception:
                pass
        self._active_clients.clear()
        if self._server:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=1.0)
            except Exception:
                pass
            self._server = None
        logger.info("TCP server on %s:%d stopped", self.host, self.port)


class AsyncTcpTransport:
    """Manages outbound TCP client connections to peer endpoints with bounded retries."""

    def __init__(
        self,
        node_id: str,
        config: AppConfig,
    ) -> None:
        self.node_id = node_id
        self.config = config
        self.max_retries = config.timeouts.max_retries
        self.retry_backoff_sec = config.timeouts.retry_backoff_ms / 1000.0
        self.connection_timeout_sec = config.network.connection_timeout_ms / 1000.0

        # Peer endpoint mapping: peer_node_id -> (host, port)
        self.endpoints: Dict[str, Tuple[str, int]] = {
            n.node_id: (n.host, n.port) for n in config.nodes if n.node_id != node_id
        }

        # Cached connections: peer_id -> (StreamReader, StreamWriter)
        self._connections: Dict[str, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._lock = asyncio.Lock()

        # Transport error log: bounded list of (timestamp, peer, error_desc)
        self.recent_errors: List[Dict[str, Any]] = []

    def _record_error(self, peer: str, error: str) -> None:
        self.recent_errors.append({
            "peer": peer,
            "error": error,
        })
        if len(self.recent_errors) > 20:
            self.recent_errors.pop(0)

    async def _get_connection(
        self, peer_id: str
    ) -> Optional[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """Retrieve existing healthy connection or open a new one with bounded retries."""
        if peer_id not in self.endpoints:
            self._record_error(peer_id, f"Unknown peer destination '{peer_id}'")
            return None

        host, port = self.endpoints[peer_id]

        async with self._lock:
            # Check existing connection
            if peer_id in self._connections:
                reader, writer = self._connections[peer_id]
                if not writer.is_closing():
                    return reader, writer
                else:
                    self._close_cached(peer_id)

            # Attempt to connect with bounded retries
            for attempt in range(1, self.max_retries + 1):
                try:
                    conn_fut = asyncio.open_connection(host, port)
                    reader, writer = await asyncio.wait_for(
                        conn_fut, timeout=self.connection_timeout_sec
                    )
                    self._connections[peer_id] = (reader, writer)
                    return reader, writer
                except Exception as e:
                    self._record_error(
                        peer_id,
                        f"Attempt {attempt}/{self.max_retries} failed to {host}:{port}: {e}",
                    )
                    if attempt < self.max_retries and self.retry_backoff_sec > 0:
                        await asyncio.sleep(self.retry_backoff_sec)

            return None

    def _close_cached(self, peer_id: str) -> None:
        if peer_id in self._connections:
            _, writer = self._connections.pop(peer_id)
            try:
                writer.close()
            except Exception:
                pass

    async def send_to_peer(self, peer_id: str, message: ProtocolMessage) -> bool:
        """Send a single ProtocolMessage to a specific peer over TCP.

        Returns:
            True if frame written and drained successfully, False on error.
        """
        frame = encode_frame(message)
        conn = await self._get_connection(peer_id)
        if conn is None:
            return False

        reader, writer = conn
        try:
            writer.write(frame)
            await asyncio.wait_for(writer.drain(), timeout=self.connection_timeout_sec)
            return True
        except Exception as e:
            self._record_error(peer_id, f"Error writing frame: {e}")
            self._close_cached(peer_id)
            return False

    async def send_message(self, message: ProtocolMessage) -> bool:
        """Send a message to its intended destination (or broadcast if destination is None)."""
        if message.destination_id is not None:
            return await self.send_to_peer(message.destination_id, message)
        else:
            # Broadcast to all configured peers
            results = await asyncio.gather(
                *[self.send_to_peer(peer_id, message) for peer_id in self.endpoints],
                return_exceptions=True,
            )
            return all(r is True for r in results)

    async def close(self) -> None:
        """Close all open peer connections."""
        async with self._lock:
            for peer_id in list(self._connections.keys()):
                self._close_cached(peer_id)
