"""Discrete-event scheduler for simulation experiments."""
import heapq
from itertools import count
from typing import Callable, Tuple, Any, List


class EventScheduler:
    """Priority queue-based discrete-event scheduler."""

    def __init__(self) -> None:
        self.current_time: float = 0.0
        self._events: List[Tuple[float, int, Callable[[], Any], str]] = []
        self._sequence = count()

    def schedule(self, time: float, callback: Callable[[], Any], description: str = "") -> None:
        """Schedule an event at simulation time."""
        if time < self.current_time:
            raise ValueError(f"Cannot schedule event in past: {time} < {self.current_time}")
        event = (float(time), next(self._sequence), callback, description)
        heapq.heappush(self._events, event)

    def has_events(self) -> bool:
        """Check if any events remain in the queue."""
        return bool(self._events)

    def pop_next_event(self) -> Tuple[Callable[[], Any], str]:
        """Pop the next scheduled event and advance simulation time."""
        if not self._events:
            raise IndexError("No events remaining.")
        time, sequence, callback, description = heapq.heappop(self._events)
        self.current_time = time
        return callback, description

    def run(self, until: float = float("inf")) -> int:
        """Process all scheduled events up to simulation time 'until'."""
        processed = 0
        while self._events and self._events[0][0] <= until:
            callback, description = self.pop_next_event()
            callback()
            processed += 1

        if until != float("inf"):
            self.current_time = max(self.current_time, until)

        return processed
