"""Simulation engine baseline implementation of Ricart-Agrawala algorithm (from Phase 1)."""
from typing import Dict, Set, Optional, Tuple, List, Any
from src.simulation.engine import Simulation


class SimulationRicartAgrawala:
    """Simulation-level Ricart-Agrawala permission-based mutual exclusion."""

    def __init__(self, sim: Simulation) -> None:
        self.sim = sim
        self.n = sim.config.process_count
        self.clock: Dict[int, int] = {pid: 0 for pid in sim.processes}
        self.request: Dict[int, Optional[Tuple[int, int]]] = {
            pid: None for pid in sim.processes
        }
        self.replies: Dict[int, Set[int]] = {pid: set() for pid in sim.processes}
        self.deferred: Dict[int, Dict[int, int]] = {pid: {} for pid in sim.processes}
        self.request_number: Dict[int, int] = {pid: 0 for pid in sim.processes}
        self.completed = 0
        self.wait_times: List[float] = []

    def _tick(self, pid: int) -> int:
        """Advance local Lamport clock."""
        self.clock[pid] += 1
        return self.clock[pid]

    def request_cs(self, pid: int) -> None:
        """Process initiates request for critical section."""
        process = self.sim.processes[pid]
        if not process.active or process.requesting or process.in_critical_section:
            return

        timestamp = self._tick(pid)
        self.request_number[pid] += 1
        request_id = self.request_number[pid]

        self.request[pid] = (timestamp, pid)
        self.replies[pid] = set()
        process.requesting = True
        process.request_time = self.sim.time

        self.sim.log(pid, "REQUEST", {"request_id": request_id})

        for other in self.sim.processes:
            if other != pid:
                self.sim.send(
                    pid,
                    other,
                    {
                        "type": "REQUEST",
                        "timestamp": timestamp,
                        "sender": pid,
                        "request_id": request_id,
                    },
                    lambda msg, receiver=other: self._receive(receiver, msg),
                )

        self._try_enter(pid)

    def _receive(self, receiver: int, msg: Dict[str, Any]) -> None:
        """Handle incoming message at receiving process."""
        pid = receiver
        self.clock[pid] = max(self.clock[pid], msg.get("timestamp", 0)) + 1

        if msg["type"] == "REQUEST":
            sender = msg["sender"]
            incoming_priority = (msg["timestamp"], sender)
            own_priority = self.request[pid]
            process = self.sim.processes[pid]

            should_defer = (
                process.in_critical_section
                or (
                    process.requesting
                    and own_priority is not None
                    and own_priority < incoming_priority
                )
            )

            if should_defer:
                self.deferred[pid][sender] = int(msg.get("request_id", 0))
                self.sim.log(pid, "DEFER_REPLY", {"to": sender, "request_id": msg.get("request_id")})
            else:
                self._send_reply(pid, sender, int(msg.get("request_id", 0)))

        elif msg["type"] == "REPLY":
            # Ignore delayed replies from an earlier request by the same process.
            if msg.get("request_id") == self.request_number[pid]:
                self.replies[pid].add(msg["sender"])
                self._try_enter(pid)

    def _send_reply(self, sender: int, receiver: int, request_id: int) -> None:
        """Send REPLY message to requester."""
        self.sim.send(
            sender,
            receiver,
            {"type": "REPLY", "sender": sender, "request_id": request_id},
            lambda msg, r=receiver: self._receive(r, msg),
        )

    def _try_enter(self, pid: int) -> None:
        """Attempt to enter critical section if all replies received."""
        process = self.sim.processes[pid]
        if not process.active or not process.requesting:
            return

        if len(self.replies[pid]) != self.n - 1:
            return

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

    def release_cs(self, pid: int) -> None:
        """Process exits critical section and sends deferred replies."""
        process = self.sim.processes[pid]
        if not process.in_critical_section:
            return

        self.sim.safety.exit(pid, self.sim.time)
        process.in_critical_section = False
        process.exit_time = self.sim.time
        self.completed += 1

        self.sim.log(pid, "EXIT_CS")

        waiting = sorted(
            self.deferred[pid].items(),
            key=lambda item: (self.request[item[0]] or (float("inf"), item[0])),
        )
        self.deferred[pid].clear()

        for other, deferred_request_id in waiting:
            self._send_reply(pid, other, deferred_request_id)

        self.request[pid] = None
