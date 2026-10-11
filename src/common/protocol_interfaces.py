"""Abstract interfaces and protocol contracts for distributed mutual exclusion algorithms.

Architecture Contract:
Algorithms implement purely event-driven, non-blocking state machines:
1. They accept local commands (request_cs, release_cs) and incoming network messages (handle_message).
2. They mutate local protocol state deterministically.
3. They return outbound ProtocolMessage instances for the network transport layer to serialize and deliver.
4. They expose query methods (can_enter_cs, get_diagnostics) without side effects.
5. Sockets, timeouts, and network threads are handled by external network runtimes,
   preventing blocking I/O from contaminating algorithmic state.
6. The experiment controller never centrally grants permission; permission is solely
   derived from local protocol state and peer message exchanges.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from src.common.messages import ProtocolMessage
from src.common.configuration import AppConfig


class IMutualExclusionProtocol(ABC):
    """Abstract Base Class for all distributed mutual exclusion algorithms."""

    def __init__(self, node_id: str, config: AppConfig) -> None:
        """Initialize protocol state machine with local identity and configuration."""
        self.node_id = node_id
        self.config = config

    @abstractmethod
    def initialize(self, peers: List[str]) -> None:
        """Initialize topology, neighbor relations, or initial token ownership among peers."""
        pass

    @abstractmethod
    def request_cs(self, request_id: str) -> List[ProtocolMessage]:
        """Initiate request to enter the critical section.

        Returns:
            A list of outgoing ProtocolMessage instances to be transmitted to peers.
        """
        pass

    @abstractmethod
    def handle_message(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        """Process an incoming protocol message from a peer.

        Returns:
            A list of outgoing ProtocolMessage instances to be transmitted in response.
        """
        pass

    @abstractmethod
    def can_enter_cs(self) -> bool:
        """Query whether the local node currently holds mutual-exclusion permission."""
        pass

    @abstractmethod
    def on_cs_enter(self) -> None:
        """Called when the node transitions into the critical section."""
        pass

    @abstractmethod
    def release_cs(self) -> List[ProtocolMessage]:
        """Exit the critical section and release lock/transfer token.

        Returns:
            A list of outgoing ProtocolMessage instances (deferred replies, token pass).
        """
        pass

    @abstractmethod
    def get_diagnostics(self) -> Dict[str, Any]:
        """Return diagnostic view of local protocol state (for metrics and auditing)."""
        pass
