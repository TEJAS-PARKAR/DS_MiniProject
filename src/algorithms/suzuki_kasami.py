"""Suzuki-Kasami token-based distributed mutual exclusion protocol.

Implements the standard broadcast token-based mutual exclusion algorithm
adhering strictly to the IMutualExclusionProtocol non-blocking interface.
"""
from collections import deque
import threading
from typing import List, Dict, Set, Optional, Any
from src.common.protocol_interfaces import IMutualExclusionProtocol
from src.common.messages import ProtocolMessage, MessageType
from src.common.configuration import AppConfig
from src.algorithms.ricart_agrawala import NodeState


class SuzukiKasami(IMutualExclusionProtocol):
    """Production implementation of Suzuki-Kasami distributed mutual exclusion.

    Protocol Properties:
    - Token-based mutual exclusion with sequence-numbered broadcasts.
    - RN[i][j]: Highest request sequence number received by node i from node j.
    - LN[j]: Highest request sequence number of node j that has been served by the token.
    - Exactly one token exists in the network at any time.
    """

    def __init__(self, node_id: str, config: AppConfig) -> None:
        super().__init__(node_id, config)
        self._lock = threading.RLock()

        self.state: NodeState = NodeState.IDLE
        self.peers: Set[str] = set()
        self.all_nodes: List[str] = [node_id]

        # Request number table RN[j]
        self.RN: Dict[str, int] = {node_id: 0}

        # Token state (present if and only if self.has_token is True)
        self.has_token: bool = False
        self.token_ln: Optional[Dict[str, int]] = None
        self.token_queue: Optional[deque[str]] = None

        self.current_request_id: Optional[str] = None

        # Diagnostic counters
        self.total_requests_made: int = 0
        self.total_entries: int = 0
        self.total_releases: int = 0
        self.token_transfers_sent: int = 0
        self.token_transfers_received: int = 0

    def initialize(
        self,
        peers: List[str],
        initial_token_holder: Optional[str] = None,
    ) -> None:
        """Initialize membership and token ownership.

        Args:
            peers: List of all participating node IDs in the system.
            initial_token_holder: Node ID that holds the single token at start.
                                 Defaults to lexicographically smallest node ID.
        """
        with self._lock:
            all_set = set(peers) | {self.node_id}
            self.all_nodes = sorted(all_set)
            self.peers = {p for p in self.all_nodes if p != self.node_id}
            self.RN = {nid: 0 for nid in self.all_nodes}

            if initial_token_holder is None:
                initial_token_holder = self.all_nodes[0]

            if initial_token_holder not in all_set:
                raise ValueError(
                    f"Initial token holder '{initial_token_holder}' must be one of {self.all_nodes}"
                )

            self.has_token = (self.node_id == initial_token_holder)
            if self.has_token:
                self.token_ln = {nid: 0 for nid in self.all_nodes}
                self.token_queue = deque()
            else:
                self.token_ln = None
                self.token_queue = None

            self.state = NodeState.IDLE
            self.current_request_id = None

    def request_cs(self, request_id: str) -> List[ProtocolMessage]:
        """Request entry to critical section.

        Raises:
            ValueError: If request_id is empty or invalid.
            RuntimeError: If node is already requesting or executing.

        Returns:
            List of outbound REQUEST messages to all peers (or empty if token held).
        """
        if not request_id or not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("request_id must be a non-empty string.")

        with self._lock:
            if self.state != NodeState.IDLE:
                raise RuntimeError(
                    f"Cannot request critical section: Node '{self.node_id}' is in state '{self.state.value}'."
                )

            self.state = NodeState.REQUESTING
            self.current_request_id = request_id.strip()
            self.total_requests_made += 1

            # Case 1: Node already possesses the token
            if self.has_token:
                return []

            # Case 2: Node does not possess token -> advance local RN and broadcast REQUEST
            self.RN[self.node_id] += 1
            seq = self.RN[self.node_id]

            outbound: List[ProtocolMessage] = []
            for peer in sorted(self.peers):
                outbound.append(
                    ProtocolMessage(
                        msg_type=MessageType.REQUEST,
                        sender_id=self.node_id,
                        destination_id=peer,
                        request_id=self.current_request_id,
                        logical_clock=seq,
                        payload={"sequence_number": seq},
                        protocol_version=self.config.system.protocol_version,
                    )
                )

            return outbound

    def handle_message(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Process incoming protocol message (REQUEST or TOKEN_TRANSFER)."""
        if not isinstance(message, ProtocolMessage):
            raise ValueError(f"Expected ProtocolMessage, got {type(message).__name__}")

        with self._lock:
            sender = message.sender_id
            if sender not in self.peers and sender != self.node_id:
                return []

            if message.destination_id is not None and message.destination_id != self.node_id:
                return []

            if message.msg_type == MessageType.REQUEST:
                return self._handle_request(message)
            elif message.msg_type == MessageType.TOKEN_TRANSFER:
                return self._handle_token(message)

            return []

    def _handle_request(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Handle incoming REQUEST according to Suzuki-Kasami RN/LN rules."""
        sender = message.sender_id
        seq = message.logical_clock or message.payload.get("sequence_number", 0)

        # Sequence number must be positive
        if seq <= 0:
            return []

        # Update local knowledge of requester's highest sequence number
        self.RN[sender] = max(self.RN[sender], seq)

        # Check if local node has token and can transfer it (only when IDLE)
        if (
            self.has_token
            and self.state == NodeState.IDLE
            and self.token_ln is not None
            and self.token_queue is not None
        ):
            # Requester has an outstanding request: RN[sender] == LN[sender] + 1
            if self.RN[sender] == self.token_ln.get(sender, 0) + 1:
                # Enqueue requester if not already present
                if sender not in self.token_queue:
                    self.token_queue.append(sender)

                return self._transfer_token_if_needed()

        return []

    def _handle_token(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Handle incoming TOKEN_TRANSFER."""
        payload = message.payload
        if not isinstance(payload, dict) or "LN" not in payload or "queue" not in payload:
            return []

        # Validate token payload structures
        raw_ln = payload["LN"]
        raw_q = payload["queue"]
        if not isinstance(raw_ln, dict) or not isinstance(raw_q, list):
            return []

        # Assume token possession
        self.has_token = True
        self.token_ln = {k: int(v) for k, v in raw_ln.items()}
        self.token_queue = deque(raw_q)
        self.token_transfers_received += 1

        # If locally requesting, authorization is now granted
        if self.state == NodeState.REQUESTING:
            return []

        # If idle and there are pending requests in the token queue, forward token
        if self.state == NodeState.IDLE and self.token_queue:
            return self._transfer_token_if_needed()

        return []

    def _transfer_token_if_needed(self) -> List[ProtocolMessage]:
        """Transfer token to the head of the token queue if available."""
        if not self.has_token or self.state == NodeState.EXECUTING:
            return []

        if self.token_ln is None or self.token_queue is None or not self.token_queue:
            return []

        receiver = self.token_queue.popleft()
        token_payload = {
            "LN": dict(self.token_ln),
            "queue": list(self.token_queue),
        }

        # Clear local possession
        self.has_token = False
        self.token_ln = None
        self.token_queue = None
        self.token_transfers_sent += 1

        return [
            ProtocolMessage(
                msg_type=MessageType.TOKEN_TRANSFER,
                sender_id=self.node_id,
                destination_id=receiver,
                payload=token_payload,
                protocol_version=self.config.system.protocol_version,
            )
        ]

    def can_enter_cs(self) -> bool:
        """Node can enter critical section if requesting and possessing token."""
        with self._lock:
            return self.state == NodeState.REQUESTING and self.has_token

    def on_cs_enter(self) -> None:
        """Mark node as executing in critical section."""
        with self._lock:
            if not self.can_enter_cs():
                raise RuntimeError(
                    f"Node '{self.node_id}' cannot enter critical section: token not possessed."
                )
            self.state = NodeState.EXECUTING
            self.total_entries += 1

    def release_cs(self) -> List[ProtocolMessage]:
        """Release critical section, update LN, enqueue waiting nodes, transfer if needed."""
        with self._lock:
            if self.state != NodeState.EXECUTING:
                raise RuntimeError(
                    f"Node '{self.node_id}' cannot release critical section: currently in state '{self.state.value}'."
                )

            if not self.has_token or self.token_ln is None or self.token_queue is None:
                raise RuntimeError("Critical section state violation: token missing on release.")

            # Rule: LN[i] = RN[i] for completed local request
            self.token_ln[self.node_id] = self.RN[self.node_id]

            # Rule: For every node j not in token queue, if RN[j] == LN[j] + 1, enqueue j
            for nid in sorted(self.all_nodes):
                if nid != self.node_id and nid not in self.token_queue:
                    if self.RN.get(nid, 0) == self.token_ln.get(nid, 0) + 1:
                        self.token_queue.append(nid)

            self.state = NodeState.IDLE
            self.current_request_id = None
            self.total_releases += 1

            # Transfer token if queue has pending requesters
            return self._transfer_token_if_needed()

    def get_diagnostics(self) -> Dict[str, Any]:
        """Return diagnostic view of protocol state."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "has_token": self.has_token,
                "RN": dict(self.RN),
                "LN": dict(self.token_ln) if self.token_ln is not None else None,
                "token_queue": list(self.token_queue) if self.token_queue is not None else None,
                "current_request_id": self.current_request_id,
                "peers": sorted(self.peers),
                "total_requests_made": self.total_requests_made,
                "total_entries": self.total_entries,
                "total_releases": self.total_releases,
                "token_transfers_sent": self.token_transfers_sent,
                "token_transfers_received": self.token_transfers_received,
            }
