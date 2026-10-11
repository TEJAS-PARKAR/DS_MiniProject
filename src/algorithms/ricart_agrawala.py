"""Ricart-Agrawala distributed mutual exclusion protocol implementation.

Adheres strictly to the IMutualExclusionProtocol non-blocking interface.
All state transitions are deterministic and thread-safe.
Outbound messages are returned as ProtocolMessage instances to decouple protocol
state from socket transport.
"""
from enum import Enum
import threading
from typing import List, Dict, Set, Optional, Tuple, Any
from src.common.protocol_interfaces import IMutualExclusionProtocol
from src.common.messages import ProtocolMessage, MessageType
from src.common.configuration import AppConfig


class NodeState(str, Enum):
    """Protocol state of a branch node participating in Ricart-Agrawala."""
    IDLE = "IDLE"
    REQUESTING = "REQUESTING"
    EXECUTING = "EXECUTING"


class RicartAgrawala(IMutualExclusionProtocol):
    """Production implementation of the Ricart-Agrawala mutual exclusion protocol.

    Properties & Ordering Rules:
    - Logical clock synchronization via Lamport's algorithm.
    - Tie-breaking: Lexicographical comparison of (request_timestamp, node_id).
    - Quorum: Critical section entry requires permission (REPLY) from all N - 1 peers.
    """

    def __init__(self, node_id: str, config: AppConfig) -> None:
        super().__init__(node_id, config)
        self._lock = threading.RLock()

        # Protocol state
        self.state: NodeState = NodeState.IDLE
        self.clock: int = 0
        self.current_request_id: Optional[str] = None
        self.request_timestamp: Optional[int] = None

        # Participating peers (excluding self)
        self.peers: Set[str] = set()

        # Tracking replies for active request
        self.replies_received: Set[str] = set()

        # Deferred replies: maps deferred peer node_id -> peer's request_id
        self.deferred_requests: Dict[str, Optional[str]] = {}

        # Diagnostic counters
        self.total_requests_made: int = 0
        self.total_entries: int = 0
        self.total_releases: int = 0

    def initialize(self, peers: List[str]) -> None:
        """Initialize participating peer membership."""
        with self._lock:
            # Filter out self if present; participating peers must be distinct
            self.peers = {p for p in peers if p != self.node_id}
            self.state = NodeState.IDLE
            self.clock = 0
            self.current_request_id = None
            self.request_timestamp = None
            self.replies_received.clear()
            self.deferred_requests.clear()

    def request_cs(self, request_id: str) -> List[ProtocolMessage]:
        """Request entry to critical section.

        Raises:
            ValueError: If request_id is empty or invalid.
            RuntimeError: If node is already requesting or executing.

        Returns:
            List of outbound REQUEST ProtocolMessages to all peer nodes.
        """
        if not request_id or not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("request_id must be a non-empty string.")

        with self._lock:
            if self.state != NodeState.IDLE:
                raise RuntimeError(
                    f"Cannot request critical section: Node '{self.node_id}' is currently in state '{self.state.value}'."
                )

            # Advance Lamport clock and record request timestamp
            self.clock += 1
            self.request_timestamp = self.clock
            self.current_request_id = request_id.strip()
            self.state = NodeState.REQUESTING
            self.replies_received.clear()
            self.total_requests_made += 1

            # If there are no peers (single-node system), permission is immediate
            if not self.peers:
                return []

            outbound: List[ProtocolMessage] = []
            for peer in sorted(self.peers):
                outbound.append(
                    ProtocolMessage(
                        msg_type=MessageType.REQUEST,
                        sender_id=self.node_id,
                        destination_id=peer,
                        request_id=self.current_request_id,
                        logical_clock=self.request_timestamp,
                        protocol_version=self.config.system.protocol_version,
                    )
                )

            return outbound

    def handle_message(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Process incoming protocol message from peer.

        Returns:
            List of response messages generated (e.g. immediate REPLY).
        """
        if not isinstance(message, ProtocolMessage):
            raise ValueError(f"Expected ProtocolMessage, got {type(message).__name__}")

        with self._lock:
            sender = message.sender_id

            # Reject messages from unknown / non-participating nodes
            if sender not in self.peers and sender != self.node_id:
                return []

            # Check destination routing (must be addressed to this node or broadcast)
            if message.destination_id is not None and message.destination_id != self.node_id:
                return []

            if message.msg_type == MessageType.REQUEST:
                return self._handle_request(message)
            elif message.msg_type == MessageType.REPLY:
                return self._handle_reply(message)

            return []

    def _handle_request(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Process incoming REQUEST according to Lamport priority rules."""
        sender = message.sender_id
        req_clock = message.logical_clock or 0

        # Update Lamport clock on message receive
        self.clock = max(self.clock, req_clock) + 1

        incoming_priority = (req_clock, sender)
        should_defer = False

        if self.state == NodeState.EXECUTING:
            should_defer = True
        elif self.state == NodeState.REQUESTING:
            if self.request_timestamp is not None:
                own_priority = (self.request_timestamp, self.node_id)
                # Lower tuple has higher priority
                if own_priority < incoming_priority:
                    should_defer = True

        if should_defer:
            self.deferred_requests[sender] = message.request_id
            return []

        # Grant immediate permission
        self.clock += 1
        return [
            ProtocolMessage(
                msg_type=MessageType.REPLY,
                sender_id=self.node_id,
                destination_id=sender,
                request_id=message.request_id,
                logical_clock=self.clock,
                protocol_version=self.config.system.protocol_version,
            )
        ]

    def _handle_reply(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Process incoming REPLY message."""
        sender = message.sender_id
        rep_clock = message.logical_clock or 0

        # Update Lamport clock on message receive
        self.clock = max(self.clock, rep_clock) + 1

        # Stale reply check: Node must be in REQUESTING state
        if self.state != NodeState.REQUESTING:
            return []

        # Correlation check: message request_id must match current active request
        if message.request_id is not None and self.current_request_id is not None:
            if message.request_id != self.current_request_id:
                # Stale reply for an older or different request
                return []

        # Deduplication: record sender (sets inherently deduplicate repeated replies)
        self.replies_received.add(sender)
        return []

    def can_enter_cs(self) -> bool:
        """Check if node is currently eligible to enter the critical section."""
        with self._lock:
            if self.state != NodeState.REQUESTING:
                return False
            return self.replies_received.issuperset(self.peers)

    def on_cs_enter(self) -> None:
        """Record entry into the critical section.

        Raises:
            RuntimeError: If node is not eligible or already executing.
        """
        with self._lock:
            if not self.can_enter_cs():
                raise RuntimeError(
                    f"Node '{self.node_id}' cannot enter critical section: permission not yet granted."
                )
            self.state = NodeState.EXECUTING
            self.total_entries += 1

    def release_cs(self) -> List[ProtocolMessage]:
        """Release the critical section and flush deferred replies.

        Raises:
            RuntimeError: If node is not currently executing in critical section.

        Returns:
            Outbound REPLY messages to all deferred requesting nodes.
        """
        with self._lock:
            if self.state != NodeState.EXECUTING:
                raise RuntimeError(
                    f"Node '{self.node_id}' cannot release critical section: currently in state '{self.state.value}'."
                )

            # Advance clock for release event
            self.clock += 1

            # Construct replies for all deferred nodes
            outbound: List[ProtocolMessage] = []
            for deferred_node, deferred_req_id in sorted(self.deferred_requests.items()):
                outbound.append(
                    ProtocolMessage(
                        msg_type=MessageType.REPLY,
                        sender_id=self.node_id,
                        destination_id=deferred_node,
                        request_id=deferred_req_id,
                        logical_clock=self.clock,
                        protocol_version=self.config.system.protocol_version,
                    )
                )

            # Safely reset state after constructing outbound messages
            self.deferred_requests.clear()
            self.replies_received.clear()
            self.current_request_id = None
            self.request_timestamp = None
            self.state = NodeState.IDLE
            self.total_releases += 1

            return outbound

    def get_diagnostics(self) -> Dict[str, Any]:
        """Return diagnostic snapshot of local protocol state."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "clock": self.clock,
                "current_request_id": self.current_request_id,
                "request_timestamp": self.request_timestamp,
                "replies_count": len(self.replies_received),
                "replies_received": sorted(self.replies_received),
                "deferred_count": len(self.deferred_requests),
                "deferred_requests": dict(self.deferred_requests),
                "peers": sorted(self.peers),
                "total_requests_made": self.total_requests_made,
                "total_entries": self.total_entries,
                "total_releases": self.total_releases,
            }
