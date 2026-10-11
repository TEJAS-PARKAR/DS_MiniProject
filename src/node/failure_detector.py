"""Heartbeat failure detector and peer liveness tracker.

Architecture:
Implements an asynchronous suspicion-based failure detector for distributed nodes.
Distinguishes between SUSPECTED (delayed heartbeats, potential network partition)
and CONFIRMED_FAILED (repeated missed heartbeats or fatal transport errors).

CRITICAL DISTRIBUTED SYSTEMS PRINCIPLE:
Failure detection is fundamentally heuristic (Chandra-Toueg). A suspected or timed-out
peer may still be executing or holding permissions. Therefore, failure detection
NEVER unilaterally regenerates tokens or bypasses mutual exclusion quorums;
it triggers safe fail-closed policies to protect inventory consistency.
"""
from enum import Enum
import logging
import time
from typing import Dict, List, Set, Optional, Tuple, Any
from src.common.logging_utils import get_structured_logger

logger = logging.getLogger("node.failure_detector")


class PeerHealthStatus(str, Enum):
    """Liveness classification of a remote participating branch node."""
    HEALTHY = "HEALTHY"                    # Active heartbeats / recent valid messages
    SUSPECTED = "SUSPECTED"                # Missed heartbeats beyond heartbeat_timeout
    CONFIRMED_FAILED = "CONFIRMED_FAILED"  # Past confirmation_timeout or fatal transport drop


class FailureDetector:
    """Tracks peer heartbeat timestamps and evaluates liveness transitions."""

    def __init__(
        self,
        node_id: str,
        peers: Set[str],
        heartbeat_timeout_ms: float = 3000.0,
        confirmation_timeout_ms: Optional[float] = None,
        algorithm: str = "ricart_agrawala",
    ) -> None:
        self.node_id = node_id
        self.peers: Set[str] = set(peers) - {node_id}
        self.heartbeat_timeout_sec: float = max(0.001, heartbeat_timeout_ms / 1000.0)

        # Default confirmation timeout is 2.5x heartbeat timeout if not specified
        if confirmation_timeout_ms is not None:
            self.confirmation_timeout_sec: float = max(
                self.heartbeat_timeout_sec, confirmation_timeout_ms / 1000.0
            )
        else:
            self.confirmation_timeout_sec: float = self.heartbeat_timeout_sec * 2.5

        self.logger = get_structured_logger(node_id, algorithm)

        # Last communication timestamp per peer
        now = time.time()
        self.last_seen: Dict[str, float] = {peer: now for peer in self.peers}
        self.peer_status: Dict[str, PeerHealthStatus] = {
            peer: PeerHealthStatus.HEALTHY for peer in self.peers
        }

        # Transition event log
        self.transition_history: List[Dict[str, Any]] = []

    def record_message(self, peer_id: str, timestamp: Optional[float] = None) -> None:
        """Record receipt of a heartbeat or protocol message from a peer."""
        if peer_id not in self.peers:
            return

        now = timestamp if timestamp is not None else time.time()
        self.last_seen[peer_id] = now
        prev_status = self.peer_status.get(peer_id, PeerHealthStatus.HEALTHY)

        if prev_status != PeerHealthStatus.HEALTHY:
            # Transition back to HEALTHY (Reconnection / Recovery)
            self.peer_status[peer_id] = PeerHealthStatus.HEALTHY
            record = {
                "timestamp": now,
                "peer_id": peer_id,
                "previous_status": prev_status.value,
                "new_status": PeerHealthStatus.HEALTHY.value,
                "reason": "heartbeat_or_message_received",
            }
            self.transition_history.append(record)
            self.logger.log_event(
                "PEER_RECOVERED",
                f"Peer '{peer_id}' transitioned from {prev_status.value} to HEALTHY",
                details={"peer_id": peer_id, "previous_status": prev_status.value},
            )

    def evaluate_health(
        self, now: Optional[float] = None
    ) -> List[Tuple[str, PeerHealthStatus, PeerHealthStatus]]:
        """Evaluate peer health against configured timeouts and record transitions.

        Returns:
            List of (peer_id, previous_status, new_status) transitions.
        """
        current_time = now if now is not None else time.time()
        transitions: List[Tuple[str, PeerHealthStatus, PeerHealthStatus]] = []

        for peer in sorted(self.peers):
            last = self.last_seen.get(peer, 0.0)
            elapsed = current_time - last
            curr_status = self.peer_status.get(peer, PeerHealthStatus.HEALTHY)
            new_status = curr_status

            if elapsed >= self.confirmation_timeout_sec:
                new_status = PeerHealthStatus.CONFIRMED_FAILED
            elif elapsed >= self.heartbeat_timeout_sec:
                new_status = PeerHealthStatus.SUSPECTED
            else:
                new_status = PeerHealthStatus.HEALTHY

            if new_status != curr_status:
                self.peer_status[peer] = new_status
                transitions.append((peer, curr_status, new_status))

                record = {
                    "timestamp": current_time,
                    "peer_id": peer,
                    "previous_status": curr_status.value,
                    "new_status": new_status.value,
                    "elapsed_sec": elapsed,
                    "reason": (
                        "confirmation_timeout_exceeded"
                        if new_status == PeerHealthStatus.CONFIRMED_FAILED
                        else "heartbeat_timeout_exceeded"
                    ),
                }
                self.transition_history.append(record)

                event_name = (
                    "PEER_CONFIRMED_FAILED"
                    if new_status == PeerHealthStatus.CONFIRMED_FAILED
                    else "PEER_SUSPECTED"
                )
                self.logger.log_event(
                    event_name,
                    f"Peer '{peer}' transitioned from {curr_status.value} to {new_status.value} (silent {elapsed:.2f}s)",
                    details={
                        "peer_id": peer,
                        "previous_status": curr_status.value,
                        "new_status": new_status.value,
                        "elapsed_sec": elapsed,
                    },
                )

        return transitions

    def confirm_failure(self, peer_id: str, reason: str = "transport_failure") -> None:
        """Immediately transition peer to CONFIRMED_FAILED on unrecoverable transport drop."""
        if peer_id not in self.peers:
            return

        curr_status = self.peer_status.get(peer_id, PeerHealthStatus.HEALTHY)
        if curr_status != PeerHealthStatus.CONFIRMED_FAILED:
            now = time.time()
            self.peer_status[peer_id] = PeerHealthStatus.CONFIRMED_FAILED
            record = {
                "timestamp": now,
                "peer_id": peer_id,
                "previous_status": curr_status.value,
                "new_status": PeerHealthStatus.CONFIRMED_FAILED.value,
                "reason": reason,
            }
            self.transition_history.append(record)
            self.logger.log_event(
                "PEER_CONFIRMED_FAILED",
                f"Peer '{peer_id}' confirmed failed due to '{reason}'",
                details={"peer_id": peer_id, "previous_status": curr_status.value, "reason": reason},
            )

    def is_healthy(self, peer_id: str) -> bool:
        """Check if peer is currently healthy."""
        return self.peer_status.get(peer_id) == PeerHealthStatus.HEALTHY

    def is_suspected(self, peer_id: str) -> bool:
        """Check if peer is suspected."""
        return self.peer_status.get(peer_id) == PeerHealthStatus.SUSPECTED

    def is_confirmed_failed(self, peer_id: str) -> bool:
        """Check if peer is confirmed failed."""
        return self.peer_status.get(peer_id) == PeerHealthStatus.CONFIRMED_FAILED

    def get_suspected_peers(self) -> List[str]:
        """Return list of peers currently in SUSPECTED status."""
        return [p for p in sorted(self.peers) if self.peer_status.get(p) == PeerHealthStatus.SUSPECTED]

    def get_failed_peers(self) -> List[str]:
        """Return list of peers currently in CONFIRMED_FAILED status."""
        return [p for p in sorted(self.peers) if self.peer_status.get(p) == PeerHealthStatus.CONFIRMED_FAILED]

    def get_unhealthy_peers(self) -> List[str]:
        """Return list of peers currently not in HEALTHY status."""
        return [p for p in sorted(self.peers) if self.peer_status.get(p) != PeerHealthStatus.HEALTHY]

    def has_unhealthy_peers(self) -> bool:
        """Check if any participating peer is not healthy."""
        return any(s != PeerHealthStatus.HEALTHY for s in self.peer_status.values())

    def to_dict(self) -> Dict[str, Any]:
        """Diagnostic summary of peer health for status endpoint."""
        now = time.time()
        return {
            "node_id": self.node_id,
            "monitored_peers": sorted(self.peers),
            "peer_status": {p: self.peer_status.get(p, PeerHealthStatus.HEALTHY).value for p in sorted(self.peers)},
            "last_seen_seconds_ago": {
                p: round(now - self.last_seen.get(p, now), 3) for p in sorted(self.peers)
            },
            "unhealthy_peers": [p for p, s in self.peer_status.items() if s != PeerHealthStatus.HEALTHY],
            "total_transition_events": len(self.transition_history),
            "recent_transitions": self.transition_history[-5:] if self.transition_history else [],
        }
