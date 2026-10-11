"""Simulated message network with configurable delay, jitter, and loss."""
import random
from typing import Callable, Any, Dict, List
from src.simulation.scheduler import EventScheduler


class MessageNetwork:
    """Network simulation layer modeling transmission delay and packet loss."""

    def __init__(
        self,
        scheduler: EventScheduler,
        mean_delay: float = 1.0,
        loss_rate: float = 0.0,
        seed: int = 42,
    ) -> None:
        if mean_delay < 0:
            raise ValueError("Mean delay cannot be negative.")
        if not (0.0 <= loss_rate <= 1.0):
            raise ValueError("Loss rate must be between 0 and 1.")

        self.scheduler = scheduler
        self.mean_delay = mean_delay
        self.loss_rate = loss_rate
        self.rng = random.Random(seed)

        self.messages_sent = 0
        self.messages_delivered = 0
        self.messages_dropped = 0
        self.message_log: List[Dict[str, Any]] = []

    def send(
        self,
        sender: Any,
        receiver: Any,
        message: Dict[str, Any],
        handler: Callable[[Dict[str, Any]], None],
    ) -> None:
        """Send a message from sender to receiver with simulated delay or packet drop."""
        self.messages_sent += 1

        if self.rng.random() < self.loss_rate:
            self.messages_dropped += 1
            self.message_log.append({
                "time": self.scheduler.current_time,
                "sender": sender,
                "receiver": receiver,
                "message": message,
                "status": "dropped",
            })
            return

        delay = (
            self.rng.expovariate(1.0 / self.mean_delay)
            if self.mean_delay > 0
            else 0.0
        )
        arrival_time = self.scheduler.current_time + delay

        def deliver() -> None:
            self.messages_delivered += 1
            self.message_log.append({
                "time": self.scheduler.current_time,
                "sender": sender,
                "receiver": receiver,
                "message": message,
                "status": "delivered",
            })
            handler(message)

        self.scheduler.schedule(
            arrival_time,
            deliver,
            description=f"Message {sender} -> {receiver}",
        )
