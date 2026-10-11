"""Mutual-exclusion safety monitor and invariant checker."""
from typing import Set, List, Dict, Any


class SafetyMonitor:
    """Verifies that at most one process enters the critical section simultaneously."""

    def __init__(self) -> None:
        self.active_processes: Set[int] = set()
        self.violations: List[Dict[str, Any]] = []

    def enter(self, process_id: int, time: float) -> bool:
        """Record entry to critical section, checking mutual exclusion invariants."""
        if process_id in self.active_processes:
            self.violations.append({
                "time": float(time),
                "type": "duplicate_entry",
                "process_id": process_id,
                "active_processes": sorted(self.active_processes),
            })
            return False

        if self.active_processes:
            self.violations.append({
                "time": float(time),
                "type": "simultaneous_critical_section",
                "process_id": process_id,
                "active_processes": sorted(self.active_processes),
            })
            return False

        self.active_processes.add(process_id)
        return True

    def exit(self, process_id: int, time: float) -> bool:
        """Record exit from critical section, checking that entry preceded exit."""
        if process_id not in self.active_processes:
            self.violations.append({
                "time": float(time),
                "type": "exit_without_entry",
                "process_id": process_id,
            })
            return False

        self.active_processes.remove(process_id)
        return True

    @property
    def is_safe(self) -> bool:
        """Return True if no safety violations have occurred."""
        return len(self.violations) == 0
