"""Simulation engine baseline implementation of Suzuki-Kasami algorithm (from Phase 1)."""
from collections import deque
from typing import Dict, List, Set, Optional, Any
from src.simulation.engine import Simulation


class SimulationSuzukiKasami:
    """Simulation-level Suzuki-Kasami broadcast token-based mutual exclusion."""

    def __init__(self, sim: Simulation) -> None:
        self.sim = sim
        self.n = sim.config.process_count

        self.RN: Dict[int, List[int]] = {
            i: [0] * self.n for i in range(self.n)
        }

        self.token_holder: Optional[int] = 0
        self.token: Optional[Dict[str, Any]] = {
            "LN": [0] * self.n,
            "queue": deque(),
        }

        self.request_number: List[int] = [0] * self.n
        self.pending: Set[int] = set()
        self.completed = 0
        self.wait_times: List[float] = []

    def request_cs(self, pid: int) -> None:
        """Process initiates request for critical section."""
        process = self.sim.processes[pid]
        if not process.active or process.requesting or process.in_critical_section:
            return

        self.request_number[pid] += 1
        request_id = self.request_number[pid]
        process.requesting = True
        process.request_time = self.sim.time
        self.pending.add(pid)

        self.sim.log(pid, "REQUEST", {"request_id": request_id})

        if self.token_holder == pid:
            self._try_enter(pid)
            return

        for other in range(self.n):
            if other == pid:
                continue
            self.sim.send(
                pid,
                other,
                {
                    "type": "REQUEST",
                    "sender": pid,
                    "request_number": request_id,
                },
                lambda msg, receiver=other: self._receive(receiver, msg),
            )

    def _receive(self, receiver: int, msg: Dict[str, Any]) -> None:
        """Handle incoming REQUEST or TOKEN message."""
        pid = receiver
        kind = msg["type"]

        if kind == "REQUEST":
            sender = msg["sender"]
            number = msg["request_number"]
            self.RN[pid][sender] = max(self.RN[pid][sender], number)

            if (
                self.token_holder == pid
                and not self.sim.processes[pid].in_critical_section
                and self.token is not None
                and self.RN[pid][sender] == self.token["LN"][sender] + 1
            ):
                self._enqueue(sender)
                self._transfer_if_needed(pid)

        elif kind == "TOKEN":
            self.token = msg["token"]
            self.token_holder = pid
            self.sim.log(pid, "TOKEN_RECEIVED")
            self._try_enter(pid)

    def _enqueue(self, pid: int) -> None:
        """Enqueue requester into token queue if not already present."""
        if self.token is None:
            return
        if pid != self.token_holder and pid not in self.token["queue"]:
            self.token["queue"].append(pid)

    def _transfer_if_needed(self, pid: int) -> None:
        """Transfer token to next waiting node if token available and not in CS."""
        if self.token_holder != pid:
            return
        if self.sim.processes[pid].in_critical_section:
            return
        if self.token is None or not self.token["queue"]:
            return

        receiver = self.token["queue"].popleft()
        token_copy = {
            "LN": list(self.token["LN"]),
            "queue": deque(self.token["queue"]),
        }
        self.sim.log(pid, "TOKEN_SENT", {"to": receiver})

        self.token_holder = None
        self.token = None

        self.sim.send(
            pid,
            receiver,
            {"type": "TOKEN", "token": token_copy},
            lambda msg, r=receiver: self._receive(r, msg),
        )

    def _try_enter(self, pid: int) -> None:
        """Attempt to enter critical section if holding token."""
        process = self.sim.processes[pid]
        if (
            not process.active
            or not process.requesting
            or self.token_holder != pid
        ):
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
        """Release critical section and transfer token if requested."""
        process = self.sim.processes[pid]
        if not process.in_critical_section:
            return

        self.sim.safety.exit(pid, self.sim.time)
        process.in_critical_section = False
        process.exit_time = self.sim.time
        self.completed += 1

        self.sim.log(pid, "EXIT_CS")

        if self.token is not None:
            self.token["LN"][pid] = self.request_number[pid]
            self.pending.discard(pid)

            for other in range(self.n):
                if self.RN[pid][other] == self.token["LN"][other] + 1:
                    self._enqueue(other)

            self._transfer_if_needed(pid)
