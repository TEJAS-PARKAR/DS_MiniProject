"""Standalone branch-node process orchestrating protocol, TCP transport, and health checks."""
import argparse
import asyncio
import logging
import signal
import sys
import uuid
from typing import Dict, List, Optional, Any
from src.common.configuration import AppConfig, NodeConfig, load_config, ConfigurationError
from src.common.messages import ProtocolMessage, MessageType
from src.common.protocol_interfaces import IMutualExclusionProtocol
from src.common.network import AsyncTcpServer, AsyncTcpTransport
from src.common.logging_utils import get_structured_logger
from src.algorithms.ricart_agrawala import RicartAgrawala
from src.algorithms.suzuki_kasami import SuzukiKasami
from src.algorithms.raymond_tree import Raymond
from src.node.state import NodeRuntimeState
from src.node.health import HealthServer
from src.node.failure_detector import FailureDetector

logger = logging.getLogger("node.branch_node")


def create_protocol_instance(
    node_id: str, config: AppConfig, initial_token_holder: Optional[str] = None
) -> IMutualExclusionProtocol:
    """Factory creating the configured mutual exclusion algorithm instance."""
    algo_name = config.system.algorithm.lower()
    all_node_ids = [n.node_id for n in config.nodes]

    if algo_name == "ricart_agrawala":
        proto = RicartAgrawala(node_id, config)
        proto.initialize(all_node_ids)
        return proto
    elif algo_name == "suzuki_kasami":
        proto = SuzukiKasami(node_id, config)
        proto.initialize(all_node_ids, initial_token_holder=initial_token_holder)
        return proto
    elif algo_name == "raymond_tree":
        proto = Raymond(node_id, config)
        proto.initialize(all_node_ids, initial_token_holder=initial_token_holder)
        return proto
    else:
        raise ConfigurationError(f"Unsupported algorithm '{algo_name}' for BranchNode.")


class BranchNode:
    """Manages an independent branch-node process in the distributed mutual exclusion network."""

    def __init__(
        self,
        node_id: str,
        config: AppConfig,
        initial_token_holder: Optional[str] = None,
        enable_failure_detection: bool = False,
    ) -> None:
        self.node_id = node_id
        self.config = config
        self.enable_failure_detection = enable_failure_detection

        # Unique incarnation identifier to prevent stale session reuse upon restart
        self.incarnation_id = str(uuid.uuid4())[:8]

        # Locate own node configuration
        matching_nodes = [n for n in config.nodes if n.node_id == node_id]
        if not matching_nodes:
            raise ConfigurationError(
                f"Node '{node_id}' does not exist in configured membership list: "
                f"{[n.node_id for n in config.nodes]}"
            )
        self.node_config: NodeConfig = matching_nodes[0]

        # Structured diagnostic logger
        self.logger = get_structured_logger(node_id, config.system.algorithm)

        # Runtime lifecycle & metric state
        self.runtime_state = NodeRuntimeState(node_id)

        # Core mutual-exclusion protocol
        self.protocol: IMutualExclusionProtocol = create_protocol_instance(
            node_id, config, initial_token_holder=initial_token_holder
        )

        # Outbound TCP transport
        self.transport = AsyncTcpTransport(node_id, config)

        # Inbound TCP server
        self.tcp_server = AsyncTcpServer(
            host=self.node_config.host,
            port=self.node_config.port,
            on_message_received=self._on_message_received,
        )

        # Failure detector
        peer_ids = {n.node_id for n in config.nodes} - {node_id}
        self.failure_detector = FailureDetector(
            node_id=node_id,
            peers=peer_ids,
            heartbeat_timeout_ms=config.timeouts.heartbeat_timeout_ms,
            algorithm=config.system.algorithm,
        )

        # Diagnostic HTTP Health server
        self.health_server = HealthServer(
            host=self.node_config.host,
            port=self.node_config.get_health_port(),
            status_provider=self.get_status,
            ready_provider=lambda: self.runtime_state.is_ready,
        )

        # Processing lock ensuring sequential, race-free protocol state transitions
        self._protocol_lock = asyncio.Lock()

        # Asynchronous event notifying callers when mutual exclusion is granted
        self._cs_granted_event = asyncio.Event()

        # Background heartbeat and failure detection tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._failure_detector_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start servers and mark node as ready."""
        self.runtime_state.is_running = True

        # Start TCP communication listener
        await self.tcp_server.start()

        # Start HTTP health diagnostic listener
        await self.health_server.start()

        # Start background heartbeat & failure detection if enabled
        if self.enable_failure_detection:
            await self.start_failure_detection()

        self.runtime_state.is_ready = True
        self.logger.log_event("NODE_STARTED", f"Node '{self.node_id}' (epoch={self.incarnation_id}) initialized and ready.")

    async def stop(self) -> None:
        """Gracefully shut down servers and release resources."""
        await self.stop_failure_detection()

        self.runtime_state.is_ready = False
        self.runtime_state.is_running = False

        await self.transport.close()
        await self.tcp_server.stop()
        await self.health_server.stop()

        self.logger.log_event("NODE_STOPPED", f"Node '{self.node_id}' shut down cleanly.")

    async def start_failure_detection(self) -> None:
        """Start background heartbeat broadcast and failure detection loops."""
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        if self._failure_detector_task is None or self._failure_detector_task.done():
            self._failure_detector_task = asyncio.create_task(self._failure_detection_loop())

    async def stop_failure_detection(self) -> None:
        """Cancel background failure detection tasks."""
        for task in (self._heartbeat_task, self._failure_detector_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._heartbeat_task = None
        self._failure_detector_task = None

    async def _heartbeat_loop(self) -> None:
        """Periodic background broadcast of HEARTBEAT messages to all peers."""
        interval = max(0.01, self.config.timeouts.heartbeat_interval_ms / 1000.0)
        try:
            while self.runtime_state.is_running:
                await asyncio.sleep(interval)
                for peer in sorted(self.failure_detector.peers):
                    msg = ProtocolMessage(
                        msg_type=MessageType.HEARTBEAT,
                        sender_id=self.node_id,
                        destination_id=peer,
                        protocol_version=self.config.system.protocol_version,
                        payload={"incarnation_id": self.incarnation_id},
                    )
                    await self.transport.send_message(msg)
        except asyncio.CancelledError:
            pass

    async def _failure_detection_loop(self) -> None:
        """Periodic evaluation of peer heartbeats."""
        eval_interval = max(0.01, self.config.timeouts.heartbeat_interval_ms / 2000.0)
        try:
            while self.runtime_state.is_running:
                await asyncio.sleep(eval_interval)
                self.failure_detector.evaluate_health()
        except asyncio.CancelledError:
            pass

    async def _on_message_received(self, message: ProtocolMessage) -> None:
        """Callback invoked by TCP server when a validated frame is decoded."""
        self.runtime_state.messages_received += 1
        self.failure_detector.record_message(message.sender_id)

        self.logger.log_event(
            "MESSAGE_RECEIVED",
            f"Received {message.msg_type} from {message.sender_id}",
            request_id=message.request_id,
            message_id=message.message_id,
        )

        # Heartbeats only refresh liveness, never mutate protocol state
        if message.msg_type == MessageType.HEARTBEAT:
            return

        # Route to protocol under lock to prevent concurrent mutation races
        outbound_messages: List[ProtocolMessage] = []
        async with self._protocol_lock:
            outbound_messages = self.protocol.handle_message(message)
            if self.protocol.can_enter_cs():
                self._cs_granted_event.set()

        # Transmit any generated outbound responses over TCP (outside protocol lock)
        for out_msg in outbound_messages:
            success = await self.transport.send_message(out_msg)
            if success:
                self.runtime_state.messages_sent += 1
            else:
                self.runtime_state.messages_failed += 1

    async def request_critical_section(self, request_id: str) -> None:
        """Initiate request to enter the critical section."""
        outbound: List[ProtocolMessage] = []
        async with self._protocol_lock:
            outbound = self.protocol.request_cs(request_id)
            if self.protocol.can_enter_cs():
                self._cs_granted_event.set()
            else:
                self._cs_granted_event.clear()

        self.logger.log_event(
            "REQUEST_CS_INITIATED",
            f"Requesting CS with ID '{request_id}'",
            request_id=request_id,
        )

        # Deliver generated request messages over TCP
        for msg in outbound:
            success = await self.transport.send_message(msg)
            if success:
                self.runtime_state.messages_sent += 1
            else:
                self.runtime_state.messages_failed += 1

    async def enter_critical_section(self, request_id: str) -> None:
        """Enter the critical section when permitted."""
        async with self._protocol_lock:
            if not self.protocol.can_enter_cs():
                raise RuntimeError(
                    f"Node '{self.node_id}' cannot enter critical section: permission not yet granted."
                )
            self.protocol.on_cs_enter()
            self.runtime_state.record_cs_entry(request_id)

        self.logger.log_event(
            "CRITICAL_SECTION_ENTERED",
            f"Entered CS for request '{request_id}'",
            request_id=request_id,
        )

    async def release_critical_section(self, request_id: str) -> None:
        """Release the critical section and forward outbound messages."""
        outbound: List[ProtocolMessage] = []
        async with self._protocol_lock:
            outbound = self.protocol.release_cs()
            self.runtime_state.record_cs_exit(request_id)
            self._cs_granted_event.clear()

        self.logger.log_event(
            "CRITICAL_SECTION_RELEASED",
            f"Released CS for request '{request_id}'",
            request_id=request_id,
        )

        # Deliver deferred replies or token transfers over TCP
        for msg in outbound:
            success = await self.transport.send_message(msg)
            if success:
                self.runtime_state.messages_sent += 1
            else:
                self.runtime_state.messages_failed += 1

    def can_enter_cs(self) -> bool:
        """Check if local node holds permission to enter."""
        return self.protocol.can_enter_cs()

    async def wait_for_cs_grant(self, timeout: Optional[float] = None) -> bool:
        """Asynchronously wait until the local protocol grants critical section entry.

        Returns True if permission is granted, False if timed out.
        """
        if self.protocol.can_enter_cs():
            return True
        try:
            if timeout is not None:
                await asyncio.wait_for(self._cs_granted_event.wait(), timeout=timeout)
            else:
                await self._cs_granted_event.wait()
            return self.protocol.can_enter_cs()
        except asyncio.TimeoutError:
            return False

    def get_status(self) -> Dict[str, Any]:
        """Aggregate diagnostic view for health/status endpoint."""
        status = {
            "node_id": self.node_id,
            "incarnation_id": self.incarnation_id,
            "algorithm": self.config.system.algorithm,
            "runtime": self.runtime_state.to_dict(),
            "protocol": self.protocol.get_diagnostics(),
            "failure_detector": self.failure_detector.to_dict(),
            "transport_errors": list(self.transport.recent_errors),
        }
        return status


def main() -> None:
    """CLI entrypoint for running an independent branch-node process."""
    parser = argparse.ArgumentParser(description="Distributed Blood Bank Branch Node")
    parser.add_argument("--node-id", required=True, help="Unique ID of this node")
    parser.add_argument("--config", default="config/defaults.yaml", help="Path to YAML config")
    parser.add_argument("--initial-token-holder", default=None, help="Initial token owner node ID")
    parser.add_argument(
        "--enable-failure-detection",
        action="store_true",
        help="Enable background heartbeats and failure detector",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    node = BranchNode(
        node_id=args.node_id,
        config=config,
        initial_token_holder=args.initial_token_holder,
        enable_failure_detection=args.enable_failure_detection,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def runner():
        await node.start()
        stop_event = asyncio.Event()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                # Windows event loop does not support add_signal_handler
                pass

        try:
            while node.runtime_state.is_running:
                await asyncio.sleep(0.5)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await node.stop()

    try:
        loop.run_until_complete(runner())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
