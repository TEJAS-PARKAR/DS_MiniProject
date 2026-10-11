"""Local node runtime state and execution metrics."""
import time
from typing import Dict, List, Optional, Any


class NodeRuntimeState:
    """Tracks runtime lifecycle, message counts, and critical section activity for a branch node."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.is_running: bool = False
        self.is_ready: bool = False
        self.started_at: float = time.time()

        # Message & transport counters
        self.messages_received: int = 0
        self.messages_sent: int = 0
        self.messages_failed: int = 0
        self.frames_malformed: int = 0

        # Critical section timing and history
        self.cs_entry_count: int = 0
        self.cs_exit_count: int = 0
        self.active_cs_entry_time: Optional[float] = None
        self.active_request_id: Optional[str] = None
        self.cs_history: List[Dict[str, Any]] = []

    def record_cs_entry(self, request_id: str) -> None:
        """Record entry into critical section."""
        now = time.time()
        self.cs_entry_count += 1
        self.active_cs_entry_time = now
        self.active_request_id = request_id

    def record_cs_exit(self, request_id: str) -> None:
        """Record exit from critical section."""
        now = time.time()
        entry_time = self.active_cs_entry_time or now
        duration = max(0.0, now - entry_time)
        self.cs_exit_count += 1
        self.cs_history.append({
            "request_id": request_id,
            "entry_time": entry_time,
            "exit_time": now,
            "duration_sec": duration,
        })
        self.active_cs_entry_time = None
        self.active_request_id = None

    def to_dict(self) -> Dict[str, Any]:
        """Diagnostic summary of runtime state."""
        return {
            "node_id": self.node_id,
            "is_running": self.is_running,
            "is_ready": self.is_ready,
            "uptime_seconds": time.time() - self.started_at,
            "messages_received": self.messages_received,
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "frames_malformed": self.frames_malformed,
            "cs_entry_count": self.cs_entry_count,
            "cs_exit_count": self.cs_exit_count,
            "in_critical_section": self.active_cs_entry_time is not None,
            "active_request_id": self.active_request_id,
            "completed_cs_count": len(self.cs_history),
        }
