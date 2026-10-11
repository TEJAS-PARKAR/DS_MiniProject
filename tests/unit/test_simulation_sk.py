"""Unit tests for discrete-event SimulationSuzukiKasami baseline (Phase 1)."""
from src.simulation.engine import ExperimentConfig, Simulation
from src.simulation.suzuki_kasami import SimulationSuzukiKasami


def test_simulation_sk_staggered():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=100.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=2.0,
        random_seed=42,
    )
    sim = Simulation(config)
    sk = SimulationSuzukiKasami(sim)

    for pid in range(config.process_count):
        sim.scheduler.schedule(
            pid * 0.5,
            lambda p=pid: sk.request_cs(p),
            description=f"Request by process {pid}",
        )

    sim.run()

    assert sim.safety.is_safe
    assert sk.completed == 5
    assert sk.token_holder is not None
    assert sk.token is not None
    assert len(sk.token["queue"]) == 0


def test_simulation_sk_token_invariants():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=100.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=2.0,
        random_seed=42,
    )
    sim = Simulation(config)
    sk = SimulationSuzukiKasami(sim)

    for pid in range(config.process_count):
        sim.scheduler.schedule(
            pid * 0.5,
            lambda p=pid: sk.request_cs(p),
            description=f"Request by process {pid}",
        )

    sim.run()

    assert sk.token_holder in sim.processes
    assert sk.token is not None
    assert len(sk.token["queue"]) == len(set(sk.token["queue"]))
    assert sk.token_holder not in sk.token["queue"]


def test_simulation_sk_simultaneous():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=100.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=2.0,
        random_seed=123,
    )
    sim = Simulation(config)
    sk = SimulationSuzukiKasami(sim)

    for pid in range(config.process_count):
        sim.scheduler.schedule(
            0.0,
            lambda p=pid: sk.request_cs(p),
            description=f"Simultaneous request by process {pid}",
        )

    sim.run()

    assert sim.safety.is_safe
    assert sk.completed == 5
    assert sk.token_holder is not None
    assert len(sk.token["queue"]) == 0


def test_simulation_sk_repeated():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=200.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=1.0,
        random_seed=456,
    )
    sim = Simulation(config)
    sk = SimulationSuzukiKasami(sim)
    requests_per_process = 3

    for pid in range(config.process_count):
        for round_no in range(requests_per_process):
            request_time = pid * 0.2 + round_no * 20.0
            sim.scheduler.schedule(
                request_time,
                lambda p=pid: sk.request_cs(p),
                description=f"Process {pid}, round {round_no + 1}",
            )

    sim.run()

    assert sim.safety.is_safe
    assert sk.completed == 15
    assert sk.token_holder is not None
