"""Unit tests for discrete-event SimulationRaymond baseline (Phase 1)."""
from src.simulation.engine import ExperimentConfig, Simulation
from src.simulation.raymond_tree import SimulationRaymond


def test_simulation_raymond_staggered():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=100.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=2.0,
        random_seed=42,
    )
    sim = Simulation(config)
    ray = SimulationRaymond(sim)

    for pid in range(config.process_count):
        sim.scheduler.schedule(
            pid * 0.5,
            lambda p=pid: ray.request_cs(p),
            description=f"Request by process {pid}",
        )

    sim.run()

    assert sim.safety.is_safe
    assert ray.completed == 5
    token_holders = [pid for pid, owns in ray.has_token.items() if owns]
    assert len(token_holders) == 1


def test_simulation_raymond_tree_invariants():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=100.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=2.0,
        random_seed=42,
    )
    sim = Simulation(config)
    ray = SimulationRaymond(sim)

    for pid in range(config.process_count):
        sim.scheduler.schedule(
            pid * 0.5,
            lambda p=pid: ray.request_cs(p),
            description=f"Request by process {pid}",
        )

    sim.run()

    token_owners = [pid for pid, owns in ray.has_token.items() if owns]
    assert len(token_owners) == 1
    root = token_owners[0]

    for start in range(config.process_count):
        current = start
        visited = set()
        while current != ray.holder[current] and current not in visited:
            visited.add(current)
            current = ray.holder[current]
        assert current == root


def test_simulation_raymond_repeated():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=200.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=1.0,
        random_seed=456,
    )
    sim = Simulation(config)
    ray = SimulationRaymond(sim)
    requests_per_process = 3

    for pid in range(config.process_count):
        for round_no in range(requests_per_process):
            request_time = pid * 0.2 + round_no * 20.0
            sim.scheduler.schedule(
                request_time,
                lambda p=pid: ray.request_cs(p),
                description=f"Process {pid}, round {round_no + 1}",
            )

    sim.run()

    assert sim.safety.is_safe
    assert ray.completed == 15
    token_owners = [pid for pid, owns in ray.has_token.items() if owns]
    assert len(token_owners) == 1
