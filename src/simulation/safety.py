"""Mutual-exclusion safety monitor and invariant checker."""
from typing import Set, List, Dict, Any


class SafetyMonitor:
    """Verifies that at most one process enters the critical section simultaneously."""

    def __init__(self) -> None:
        self.active_processes: Set[int] = set()
        self.violations: List[Dict[str, Any]] = []

    def enter(self, process_id: int, time: float) -> bool:
        """Record entry to critical section, checking mutual exclusion invariants."""
        safe = True
        if process_id in self.active_processes:
            self.violations.append({
                "time": float(time),
                "type": "duplicate_entry",
                "process_id": process_id,
                "active_processes": sorted(self.active_processes),
            })
            safe = False

        if self.active_processes:
            self.violations.append({
                "time": float(time),
                "type": "simultaneous_critical_section",
                "process_id": process_id,
                "active_processes": sorted(self.active_processes),
            })
            safe = False

        self.active_processes.add(process_id)
        return safe

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


def detect_overlapping_intervals(intervals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return every pair of overlapping CS intervals belonging to distinct processes.

    Each item must contain ``process_id``, ``entry_time`` and ``exit_time``.
    Intervals are treated as half-open [entry, exit), so touching boundaries are safe.
    This independent check can audit exported event traces and injected fault fixtures.
    """
    normalized = []
    for item in intervals:
        pid = item["process_id"]
        start = float(item["entry_time"])
        end = float(item["exit_time"])
        if end < start:
            raise ValueError(f"exit_time precedes entry_time for process {pid}")
        normalized.append((pid, start, end))

    overlaps: List[Dict[str, Any]] = []
    for i, (pid_a, start_a, end_a) in enumerate(normalized):
        for pid_b, start_b, end_b in normalized[i + 1:]:
            if pid_a == pid_b:
                continue
            overlap_start = max(start_a, start_b)
            overlap_end = min(end_a, end_b)
            if overlap_start < overlap_end:
                overlaps.append({
                    "type": "overlapping_intervals",
                    "process_a": pid_a,
                    "process_b": pid_b,
                    "overlap_start": overlap_start,
                    "overlap_end": overlap_end,
                })
    return overlaps
