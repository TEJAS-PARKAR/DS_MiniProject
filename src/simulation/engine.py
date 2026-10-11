"""Simulation harness coordinating discrete scheduler, network, logger, and safety."""
from dataclasses import dataclass
from typing import Dict, Any, Optional, Callable
from src.simulation.scheduler import EventScheduler
from src.simulation.network import MessageNetwork
from src.simulation.process import Process
from src.simulation.logger import EventLogger
from src.simulation.safety import SafetyMonitor


@dataclass
class ExperimentConfig:
    """Experiment configuration parameters."""

    process_count: int = 5
    simulation_time: float = 1000.0
    mean_network_delay: float = 1.0
    message_loss_rate: float = 0.0
    critical_section_time: float = 2.0
    mean_think_time: float = 5.0
    random_seed: int = 42


class Simulation:
    """Shared simulation engine."""

    def __init__(self, config: ExperimentConfig) -> None:
        self.config = config
        self.scheduler = EventScheduler()
        self.network = MessageNetwork(
            scheduler=self.scheduler,
            mean_delay=config.mean_network_delay,
            loss_rate=config.message_loss_rate,
            seed=config.random_seed,
        )
        self.processes: Dict[int, Process] = {
            pid: Process(pid) for pid in range(config.process_count)
        }
        self.logger = EventLogger()
        self.safety = SafetyMonitor()

    @property
    def time(self) -> float:
        """Current simulation time."""
        return self.scheduler.current_time

    def log(
        self, process_id: int, event_type: str, details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record an event in the simulation logger."""
        self.logger.log(self.time, process_id, event_type, details)

    def send(
        self,
        sender: Any,
        receiver: Any,
        message: Dict[str, Any],
        handler: Callable[[Dict[str, Any]], None],
    ) -> None:
        """Route message through network layer."""
        self.network.send(sender, receiver, message, handler)

    def run(self, until: Optional[float] = None) -> int:
        """Run scheduler until end time."""
        if until is None:
            until = self.config.simulation_time
        return self.scheduler.run(until=until)

    def results_summary(self) -> Dict[str, Any]:
        """Produce top-level summary of simulation execution."""
        return {
            "simulated_time": self.time,
            "messages_sent": self.network.messages_sent,
            "messages_delivered": self.network.messages_delivered,
            "messages_dropped": self.network.messages_dropped,
            "events_logged": len(self.logger.events),
            "safety_violations": len(self.safety.violations),
        }
