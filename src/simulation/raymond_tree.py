"""Simulation engine baseline implementation of Raymond tree-based algorithm (from Phase 1)."""
from collections import deque
from typing import Dict, Set, List, Optional, Tuple, Any
from src.simulation.engine import Simulation


class SimulationRaymond:
    """Simulation-level Raymond's logical tree-based mutual exclusion algorithm."""

    def __init__(
        self,
        sim: Simulation,
        tree_edges: Optional[List[Tuple[int, int]]] = None,
        initial_token_holder: int = 0,
    ) -> None:
        self.sim = sim
        self.n = sim.config.process_count

        if tree_edges is None:
            tree_edges = [(i, (i - 1) // 2) for i in range(1, self.n)]

        self.neighbors: Dict[int, Set[int]] = {pid: set() for pid in range(self.n)}
        for a, b in tree_edges:
            self.neighbors[a].add(b)
            self.neighbors[b].add(a)

        self.holder: Dict[int, Optional[int]] = {pid: None for pid in range(self.n)}
        self.has_token: Dict[int, bool] = {pid: False for pid in range(self.n)}
        self.request_queue: Dict[int, deque] = {
            pid: deque() for pid in range(self.n)
        }
        self.asked: Dict[int, bool] = {pid: False for pid in range(self.n)}

        self.holder[initial_token_holder] = initial_token_holder
        self.has_token[initial_token_holder] = True

        self._initialize_holders(initial_token_holder)

        self.completed = 0
        self.wait_times: List[float] = []

    def _initialize_holders(self, root: int) -> None:
        parent = {root: root}
        queue = deque([root])
        while queue:
            current = queue.popleft()
            for neighbor in self.neighbors[current]:
                if neighbor not in parent:
                    parent[neighbor] = current
                    queue.append(neighbor)

        for pid in range(self.n):
            if pid != root:
                self.holder[pid] = parent[pid]

    def request_cs(self, pid: int) -> None:
        process = self.sim.processes[pid]
        if not process.active or process.requesting or process.in_critical_section:
            return

        process.requesting = True
        process.request_time = self.sim.time
        self.request_queue[pid].append(pid)
        self.sim.log(pid, "REQUEST")

        self._make_request(pid)

    def _make_request(self, pid: int) -> None:
        if (
            self.request_queue[pid]
            and not self.has_token[pid]
            and not self.asked[pid]
        ):
            target = self.holder[pid]
            if target is not None:
                self.asked[pid] = True
                self._send(
                    pid,
                    target,
                    {"type": "REQUEST", "sender": pid},
                )
        self._assign_privilege(pid)

    def _send(self, sender: int, receiver: int, message: Dict[str, Any]) -> None:
        self.sim.send(
            sender,
            receiver,
            message,
            lambda msg, r=receiver: self._receive(r, msg),
        )

    def _receive(self, pid: int, msg: Dict[str, Any]) -> None:
        if msg["type"] == "REQUEST":
            sender = msg["sender"]
            self.request_queue[pid].append(sender)
            self._make_request(pid)
        elif msg["type"] == "TOKEN":
            self.has_token[pid] = True
            self.holder[pid] = pid
            self.sim.log(pid, "TOKEN_RECEIVED")
            self._assign_privilege(pid)

    def _assign_privilege(self, pid: int) -> None:
        process = self.sim.processes[pid]
        if not self.has_token[pid] or process.in_critical_section:
            return
        if not self.request_queue[pid]:
            return

        next_process = self.request_queue[pid].popleft()
        self.asked[pid] = False

        if next_process == pid:
            if not self.sim.safety.enter(pid, self.sim.time):
                self.sim.log(pid, "SAFETY_VIOLATION")

            process.requesting = False
            process.in_critical_section = True
            process.entry_time = self.sim.time

            if process.request_time is not None:
                self.wait_times.append(process.entry_time - process.request_time)
                process.request_time = None

            self.sim.log(pid, "ENTER_CS")

            self.sim.scheduler.schedule(
                self.sim.time + self.sim.config.critical_section_time,
                lambda p=pid: self.release_cs(p),
                description=f"CS exit for process {pid}",
            )
        else:
            self.holder[pid] = next_process
            self.has_token[pid] = False
            self.sim.log(pid, "TOKEN_SENT", {"to": next_process})
            self._send(pid, next_process, {"type": "TOKEN"})

            if self.request_queue[pid] and not self.asked[pid]:
                self._make_request(pid)

    def release_cs(self, pid: int) -> None:
        process = self.sim.processes[pid]
        if not process.in_critical_section:
            return

        self.sim.safety.exit(pid, self.sim.time)
        process.in_critical_section = False
        process.exit_time = self.sim.time
        self.completed += 1

        self.sim.log(pid, "EXIT_CS")
        self._assign_privilege(pid)
