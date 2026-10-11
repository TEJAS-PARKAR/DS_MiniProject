"""Raymond's tree-based token distributed mutual exclusion protocol.

Implements Raymond's logical tree mutual exclusion algorithm adhering
strictly to the IMutualExclusionProtocol non-blocking interface.
"""
from collections import deque
import threading
from typing import List, Dict, Set, Optional, Tuple, Any
from src.common.protocol_interfaces import IMutualExclusionProtocol
from src.common.messages import ProtocolMessage, MessageType
from src.common.configuration import AppConfig
from src.algorithms.ricart_agrawala import NodeState


def validate_tree_topology(nodes: List[str], edges: List[Tuple[str, str]]) -> Dict[str, Set[str]]:
    """Validate that the given nodes and edges form a connected acyclic tree.

    Returns:
        Adjacency dictionary mapping node_id -> set of neighbor node_ids.

    Raises:
        ValueError: If graph is cyclic, disconnected, has missing nodes, or invalid edges.
    """
    if len(nodes) == 0:
        raise ValueError("Tree cannot have an empty node set.")

    if len(nodes) != len(set(nodes)):
        raise ValueError("Duplicate node IDs detected in tree configuration.")

    node_set = set(nodes)
    if len(nodes) > 1 and len(edges) != len(nodes) - 1:
        raise ValueError(
            f"Tree with {len(nodes)} nodes must have exactly {len(nodes) - 1} edges, got {len(edges)}."
        )

    adj: Dict[str, Set[str]] = {nid: set() for nid in nodes}
    for u, v in edges:
        if u not in node_set or v not in node_set:
            raise ValueError(f"Edge ({u}, {v}) references node not in configured node set.")
        if u == v:
            raise ValueError(f"Self-loop detected on node '{u}'.")
        adj[u].add(v)
        adj[v].add(u)

    # Connectivity and cycle check via BFS
    visited = set()
    queue = deque([nodes[0]])
    visited.add(nodes[0])
    parent: Dict[str, Optional[str]] = {nodes[0]: None}

    while queue:
        curr = queue.popleft()
        for neighbor in adj[curr]:
            if neighbor == parent[curr]:
                continue
            if neighbor in visited:
                raise ValueError(f"Cycle detected in tree involving node '{neighbor}'.")
            visited.add(neighbor)
            parent[neighbor] = curr
            queue.append(neighbor)

    if len(visited) != len(nodes):
        unreached = node_set - visited
        raise ValueError(f"Disconnected tree topology: unreachable nodes {sorted(unreached)}.")

    return adj


class Raymond(IMutualExclusionProtocol):
    """Production implementation of Raymond's logical tree-based mutual exclusion.

    Protocol Properties:
    - Directed logical tree topology oriented toward the current token holder.
    - Each node maintains a `holder` pointer directing requests toward the token.
    - Local FIFO request queue with duplicate request suppression.
    - `asked` flag prevents redundant upstream request forwarding.
    - In every step, exactly one logical token exists in the network.
    """

    def __init__(self, node_id: str, config: AppConfig) -> None:
        super().__init__(node_id, config)
        self._lock = threading.RLock()

        self.state: NodeState = NodeState.IDLE
        self.neighbors: Set[str] = set()
        self.all_nodes: List[str] = [node_id]

        self.holder: str = node_id
        self.has_token: bool = False
        self.request_queue: deque[str] = deque()
        self.asked: bool = False

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
        tree_edges: Optional[List[Tuple[str, str]]] = None,
        initial_token_holder: Optional[str] = None,
    ) -> None:
        """Initialize topology, tree orientation, and token possession.

        Args:
            peers: All participating node IDs in the system.
            tree_edges: Optional explicit undirected edges defining the tree.
                        Defaults to star/binary tree rooted at initial_token_holder.
            initial_token_holder: Starting holder of the token.
                                 Defaults to lexicographically smallest node ID.
        """
        with self._lock:
            all_set = set(peers) | {self.node_id}
            self.all_nodes = sorted(all_set)

            if initial_token_holder is None:
                initial_token_holder = self.all_nodes[0]

            if initial_token_holder not in all_set:
                raise ValueError(
                    f"Initial token holder '{initial_token_holder}' must be in configured nodes."
                )

            # Generate default tree topology if not provided:
            # node at index i connected to (i-1)//2
            if tree_edges is None:
                tree_edges = [
                    (self.all_nodes[i], self.all_nodes[(i - 1) // 2])
                    for i in range(1, len(self.all_nodes))
                ]

            adj = validate_tree_topology(self.all_nodes, tree_edges)
            self.neighbors = set(adj[self.node_id])

            # Orient tree edges toward initial_token_holder using BFS
            parents = self._compute_parents_toward_root(adj, initial_token_holder)
            self.has_token = (self.node_id == initial_token_holder)
            if self.has_token:
                self.holder = self.node_id
            else:
                self.holder = parents[self.node_id]

            self.request_queue = deque()
            self.asked = False
            self.state = NodeState.IDLE
            self.current_request_id = None

    def _compute_parents_toward_root(
        self, adj: Dict[str, Set[str]], root: str
    ) -> Dict[str, str]:
        """BFS from root to determine every node's parent pointer pointing toward root."""
        parents: Dict[str, str] = {root: root}
        queue = deque([root])
        while queue:
            curr = queue.popleft()
            for neighbor in adj[curr]:
                if neighbor not in parents:
                    parents[neighbor] = curr
                    queue.append(neighbor)
        return parents

    def request_cs(self, request_id: str) -> List[ProtocolMessage]:
        """Request entry to critical section.

        Raises:
            ValueError: If request_id is empty or invalid.
            RuntimeError: If node is already requesting or executing.

        Returns:
            List of outbound ProtocolMessages (REQUEST toward holder).
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

            # Enqueue local node with duplicate suppression
            if self.node_id not in self.request_queue:
                self.request_queue.append(self.node_id)

            outbound: List[ProtocolMessage] = []

            # If node does not hold token, forward request toward holder
            outbound.extend(self._make_request())

            # If node already holds token and is not executing, assign privilege
            if self.has_token and self.state != NodeState.EXECUTING:
                outbound.extend(self._assign_privilege())

            return outbound

    def handle_message(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Process incoming protocol message (REQUEST or TOKEN_TRANSFER)."""
        if not isinstance(message, ProtocolMessage):
            raise ValueError(f"Expected ProtocolMessage, got {type(message).__name__}")

        with self._lock:
            sender = message.sender_id

            # Reject messages from non-neighbors (unless self)
            if sender not in self.neighbors and sender != self.node_id:
                return []

            if message.destination_id is not None and message.destination_id != self.node_id:
                return []

            if message.msg_type in (MessageType.REQUEST, MessageType.TOKEN_REQUEST):
                return self._handle_request(message)
            elif message.msg_type == MessageType.TOKEN_TRANSFER:
                return self._handle_token(message)

            return []

    def _handle_request(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Handle incoming REQUEST from neighbor."""
        sender = message.sender_id

        # Enqueue requester with duplicate suppression
        if sender not in self.request_queue:
            self.request_queue.append(sender)

        outbound: List[ProtocolMessage] = []

        # If node does not hold token, forward request toward holder if needed
        outbound.extend(self._make_request())

        # If node holds token and is IDLE, assign privilege
        if self.has_token and self.state == NodeState.IDLE:
            outbound.extend(self._assign_privilege())

        return outbound

    def _handle_token(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Handle incoming TOKEN_TRANSFER from neighbor."""
        self.has_token = True
        self.holder = self.node_id
        self.token_transfers_received += 1

        return self._assign_privilege()

    def _make_request(self) -> List[ProtocolMessage]:
        """Send REQUEST toward holder if request_queue non-empty, without token, and not already asked."""
        if self.request_queue and not self.has_token and not self.asked:
            self.asked = True
            return [
                ProtocolMessage(
                    msg_type=MessageType.REQUEST,
                    sender_id=self.node_id,
                    destination_id=self.holder,
                    request_id=self.current_request_id,
                    protocol_version=self.config.system.protocol_version,
                )
            ]
        return []

    def _assign_privilege(self) -> List[ProtocolMessage]:
        """Assign privilege to head of queue or forward token along tree."""
        if not self.has_token or self.state == NodeState.EXECUTING:
            return []

        if not self.request_queue:
            return []

        next_node = self.request_queue.popleft()
        self.asked = False

        if next_node == self.node_id:
            # Privilege assigned to local node
            self.has_token = True
            self.holder = self.node_id
            return []
        else:
            # Transfer token to neighbor next_node
            self.holder = next_node
            self.has_token = False
            self.token_transfers_sent += 1

            outbound = [
                ProtocolMessage(
                    msg_type=MessageType.TOKEN_TRANSFER,
                    sender_id=self.node_id,
                    destination_id=next_node,
                    request_id=self.current_request_id,
                    protocol_version=self.config.system.protocol_version,
                )
            ]

            # If there are still pending requests in queue, forward request to new holder
            if self.request_queue and not self.asked:
                self.asked = True
                outbound.append(
                    ProtocolMessage(
                        msg_type=MessageType.REQUEST,
                        sender_id=self.node_id,
                        destination_id=self.holder,
                        protocol_version=self.config.system.protocol_version,
                    )
                )

            return outbound

    def can_enter_cs(self) -> bool:
        """Node can enter critical section if requesting, possessing token, and holder is self."""
        with self._lock:
            return self.state == NodeState.REQUESTING and self.has_token and (self.holder == self.node_id)

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
        """Release critical section and assign privilege to next queued request."""
        with self._lock:
            if self.state != NodeState.EXECUTING:
                raise RuntimeError(
                    f"Node '{self.node_id}' cannot release critical section: currently in state '{self.state.value}'."
                )

            self.state = NodeState.IDLE
            self.current_request_id = None
            self.total_releases += 1

            return self._assign_privilege()

    def get_diagnostics(self) -> Dict[str, Any]:
        """Return diagnostic snapshot of protocol state."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "has_token": self.has_token,
                "holder": self.holder,
                "neighbors": sorted(self.neighbors),
                "request_queue": list(self.request_queue),
                "asked": self.asked,
                "current_request_id": self.current_request_id,
                "total_requests_made": self.total_requests_made,
                "total_entries": self.total_entries,
                "total_releases": self.total_releases,
                "token_transfers_sent": self.token_transfers_sent,
                "token_transfers_received": self.token_transfers_received,
            }
