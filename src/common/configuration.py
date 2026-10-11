"""Configuration models and YAML loading utilities with strict validation."""
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Optional
import yaml

SUPPORTED_ALGORITHMS = {
    "ricart_agrawala",
    "suzuki_kasami",
    "raymond_tree",
    "resilient_logical_tree",
}

SUPPORTED_DISTRIBUTIONS = {"exponential", "uniform", "constant"}


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass


@dataclass
class NodeConfig:
    """Configuration for an individual distributed node."""
    node_id: str
    host: str
    port: int
    health_port: Optional[int] = None

    def get_health_port(self) -> int:
        return self.health_port if self.health_port is not None else self.port + 1000

    def validate(self) -> None:
        if not self.node_id or not isinstance(self.node_id, str) or not self.node_id.strip():
            raise ConfigurationError("Node ID must be a non-empty string.")
        if not self.host or not isinstance(self.host, str) or not self.host.strip():
            raise ConfigurationError(f"Node '{self.node_id}': Host must be a non-empty string.")
        if not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            raise ConfigurationError(
                f"Node '{self.node_id}': Port must be an integer between 1 and 65535, got {self.port}."
            )
        hp = self.get_health_port()
        if not (1 <= hp <= 65535):
            raise ConfigurationError(
                f"Node '{self.node_id}': Health port must be between 1 and 65535, got {hp}."
            )
        if hp == self.port:
            raise ConfigurationError(
                f"Node '{self.node_id}': Health port ({hp}) cannot be identical to TCP communication port."
            )


@dataclass
class SystemConfig:
    """Core system-level protocol configuration."""
    algorithm: str
    protocol_version: str = "1.0"
    random_seed: int = 42

    def validate(self) -> None:
        if self.algorithm not in SUPPORTED_ALGORITHMS:
            raise ConfigurationError(
                f"Unsupported algorithm '{self.algorithm}'. Valid options: {sorted(SUPPORTED_ALGORITHMS)}"
            )
        if self.protocol_version != "1.0":
            raise ConfigurationError(
                f"Unsupported protocol version '{self.protocol_version}'. Supported: ['1.0']"
            )
        if not isinstance(self.random_seed, int):
            raise ConfigurationError("random_seed must be an integer.")


@dataclass
class NetworkConfig:
    """Network emulation parameters."""
    mean_delay_ms: float = 10.0
    jitter_ms: float = 2.0
    message_loss_rate: float = 0.0
    connection_timeout_ms: float = 3000.0

    def validate(self) -> None:
        if self.mean_delay_ms < 0:
            raise ConfigurationError(f"mean_delay_ms cannot be negative, got {self.mean_delay_ms}")
        if self.jitter_ms < 0:
            raise ConfigurationError(f"jitter_ms cannot be negative, got {self.jitter_ms}")
        if not (0.0 <= self.message_loss_rate <= 1.0):
            raise ConfigurationError(
                f"message_loss_rate must be between 0.0 and 1.0, got {self.message_loss_rate}"
            )
        if self.connection_timeout_ms <= 0:
            raise ConfigurationError(
                f"connection_timeout_ms must be positive, got {self.connection_timeout_ms}"
            )


@dataclass
class TimeoutConfig:
    """Protocol timeout and retry policies."""
    message_delivery_timeout_ms: float = 2000.0
    token_response_timeout_ms: float = 5000.0
    heartbeat_interval_ms: float = 1000.0
    heartbeat_timeout_ms: float = 3000.0
    max_retries: int = 3
    retry_backoff_ms: float = 500.0

    def validate(self) -> None:
        if self.message_delivery_timeout_ms <= 0:
            raise ConfigurationError("message_delivery_timeout_ms must be positive.")
        if self.token_response_timeout_ms <= 0:
            raise ConfigurationError("token_response_timeout_ms must be positive.")
        if self.heartbeat_interval_ms <= 0:
            raise ConfigurationError("heartbeat_interval_ms must be positive.")
        if self.heartbeat_timeout_ms <= self.heartbeat_interval_ms:
            raise ConfigurationError(
                f"heartbeat_timeout_ms ({self.heartbeat_timeout_ms}) must be strictly greater than "
                f"heartbeat_interval_ms ({self.heartbeat_interval_ms})."
            )
        if self.max_retries < 0:
            raise ConfigurationError("max_retries cannot be negative.")
        if self.retry_backoff_ms < 0:
            raise ConfigurationError("retry_backoff_ms cannot be negative.")


@dataclass
class WorkloadConfig:
    """Critical section workload profile."""
    requests_per_node: int = 10
    mean_think_time_ms: float = 50.0
    critical_section_duration_ms: float = 20.0
    arrival_distribution: str = "exponential"
    workload_seed: int = 101

    def validate(self) -> None:
        if self.requests_per_node < 1:
            raise ConfigurationError("requests_per_node must be at least 1.")
        if self.mean_think_time_ms < 0:
            raise ConfigurationError("mean_think_time_ms cannot be negative.")
        if self.critical_section_duration_ms < 0:
            raise ConfigurationError("critical_section_duration_ms cannot be negative.")
        if self.arrival_distribution not in SUPPORTED_DISTRIBUTIONS:
            raise ConfigurationError(
                f"Invalid arrival_distribution '{self.arrival_distribution}'. "
                f"Valid: {sorted(SUPPORTED_DISTRIBUTIONS)}"
            )


@dataclass
class ExperimentConfigModel:
    """Experiment runtime metadata."""
    duration_seconds: float = 30.0
    collect_metrics: bool = True
    export_traces: bool = True

    def validate(self) -> None:
        if self.duration_seconds <= 0:
            raise ConfigurationError("duration_seconds must be positive.")


@dataclass
class AppConfig:
    """Complete application and experiment configuration."""
    system: SystemConfig
    nodes: List[NodeConfig]
    network: NetworkConfig = field(default_factory=NetworkConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    workload: WorkloadConfig = field(default_factory=WorkloadConfig)
    experiment: ExperimentConfigModel = field(default_factory=ExperimentConfigModel)

    def validate(self) -> None:
        """Validate all nested configs and global cross-cutting invariants."""
        self.system.validate()
        self.network.validate()
        self.timeouts.validate()
        self.workload.validate()
        self.experiment.validate()

        if not self.nodes:
            raise ConfigurationError("Nodes list cannot be empty.")

        node_ids = set()
        node_addresses = set()
        for node in self.nodes:
            node.validate()
            if node.node_id in node_ids:
                raise ConfigurationError(f"Duplicate node_id detected: '{node.node_id}'")
            node_ids.add(node.node_id)

            address = (node.host, node.port)
            if address in node_addresses:
                raise ConfigurationError(
                    f"Duplicate node address detected: {node.host}:{node.port} (Node '{node.node_id}')"
                )
            node_addresses.add(address)


def load_config_from_dict(data: Dict[str, Any]) -> AppConfig:
    """Build and validate AppConfig from a raw dictionary."""
    if not isinstance(data, dict):
        raise ConfigurationError("Configuration root must be a dictionary.")

    required_sections = {"system", "nodes"}
    missing = required_sections - set(data.keys())
    if missing:
        raise ConfigurationError(f"Missing required configuration sections: {sorted(missing)}")

    # System
    sys_data = data["system"]
    if not isinstance(sys_data, dict):
        raise ConfigurationError("'system' section must be a dictionary.")
    if "algorithm" not in sys_data:
        raise ConfigurationError("Missing 'algorithm' in system configuration.")
    system = SystemConfig(
        algorithm=sys_data["algorithm"],
        protocol_version=sys_data.get("protocol_version", "1.0"),
        random_seed=sys_data.get("random_seed", 42),
    )

    # Nodes
    raw_nodes = data["nodes"]
    if not isinstance(raw_nodes, list):
        raise ConfigurationError("'nodes' section must be a list.")
    nodes = []
    for idx, n in enumerate(raw_nodes):
        if not isinstance(n, dict):
            raise ConfigurationError(f"Node entry #{idx} must be a dictionary.")
        for req in ("node_id", "host", "port"):
            if req not in n:
                raise ConfigurationError(f"Node entry #{idx} is missing required field '{req}'.")
        health_port = n.get("health_port")
        if health_port is not None:
            health_port = int(health_port)
        nodes.append(NodeConfig(node_id=n["node_id"], host=n["host"], port=n["port"], health_port=health_port))

    # Network
    net_data = data.get("network", {})
    network = NetworkConfig(
        mean_delay_ms=float(net_data.get("mean_delay_ms", 10.0)),
        jitter_ms=float(net_data.get("jitter_ms", 2.0)),
        message_loss_rate=float(net_data.get("message_loss_rate", 0.0)),
        connection_timeout_ms=float(net_data.get("connection_timeout_ms", 3000.0)),
    )

    # Timeouts
    to_data = data.get("timeouts", {})
    timeouts = TimeoutConfig(
        message_delivery_timeout_ms=float(to_data.get("message_delivery_timeout_ms", 2000.0)),
        token_response_timeout_ms=float(to_data.get("token_response_timeout_ms", 5000.0)),
        heartbeat_interval_ms=float(to_data.get("heartbeat_interval_ms", 1000.0)),
        heartbeat_timeout_ms=float(to_data.get("heartbeat_timeout_ms", 3000.0)),
        max_retries=int(to_data.get("max_retries", 3)),
        retry_backoff_ms=float(to_data.get("retry_backoff_ms", 500.0)),
    )

    # Workload
    wl_data = data.get("workload", {})
    workload = WorkloadConfig(
        requests_per_node=int(wl_data.get("requests_per_node", 10)),
        mean_think_time_ms=float(wl_data.get("mean_think_time_ms", 50.0)),
        critical_section_duration_ms=float(wl_data.get("critical_section_duration_ms", 20.0)),
        arrival_distribution=wl_data.get("arrival_distribution", "exponential"),
        workload_seed=int(wl_data.get("workload_seed", 101)),
    )

    # Experiment
    exp_data = data.get("experiment", {})
    experiment = ExperimentConfigModel(
        duration_seconds=float(exp_data.get("duration_seconds", 30.0)),
        collect_metrics=bool(exp_data.get("collect_metrics", True)),
        export_traces=bool(exp_data.get("export_traces", True)),
    )

    config = AppConfig(
        system=system,
        nodes=nodes,
        network=network,
        timeouts=timeouts,
        workload=workload,
        experiment=experiment,
    )
    config.validate()
    return config


def load_config(file_path: str) -> AppConfig:
    """Load and validate an AppConfig from a YAML file path."""
    path = Path(file_path)
    if not path.is_file():
        raise ConfigurationError(f"Configuration file not found: {file_path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as e:
        raise ConfigurationError(f"Failed to parse YAML file '{file_path}': {e}") from e

    return load_config_from_dict(data)
