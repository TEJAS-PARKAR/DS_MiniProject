"""Unit tests for discrete-event SimulationRicartAgrawala baseline (Phase 1)."""
from src.simulation.engine import ExperimentConfig, Simulation
from src.simulation.ricart_agrawala import SimulationRicartAgrawala


def test_simulation_ra_staggered():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=100.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=2.0,
        random_seed=42,
    )
    sim = Simulation(config)
    ra = SimulationRicartAgrawala(sim)

    for pid in range(config.process_count):
        sim.scheduler.schedule(
            pid * 0.5,
            lambda p=pid: ra.request_cs(p),
            description=f"Request by process {pid}",
        )

    sim.run()

    assert sim.safety.is_safe
    assert len(sim.safety.violations) == 0
    assert ra.completed == 5
    assert len(ra.wait_times) == 5
    assert sim.network.messages_sent == 40


def test_simulation_ra_simultaneous():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=100.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=2.0,
        random_seed=123,
    )
    sim = Simulation(config)
    ra = SimulationRicartAgrawala(sim)

    for pid in range(config.process_count):
        sim.scheduler.schedule(
            0.0,
            lambda p=pid: ra.request_cs(p),
            description=f"Simultaneous request by process {pid}",
        )

    sim.run()

    assert sim.safety.is_safe
    assert ra.completed == 5
    entries = [e for e in sim.logger.events if e["event_type"] == "ENTER_CS"]
    exits = [e for e in sim.logger.events if e["event_type"] == "EXIT_CS"]
    assert len(entries) == 5
    assert len(exits) == 5


def test_simulation_ra_repeated():
    config = ExperimentConfig(
        process_count=5,
        simulation_time=200.0,
        mean_network_delay=1.0,
        message_loss_rate=0.0,
        critical_section_time=1.0,
        random_seed=456,
    )
    sim = Simulation(config)
    ra = SimulationRicartAgrawala(sim)
    requests_per_process = 3

    for pid in range(config.process_count):
        for round_no in range(requests_per_process):
            request_time = pid * 0.2 + round_no * 20.0
            sim.scheduler.schedule(
                request_time,
                lambda p=pid: ra.request_cs(p),
                description=f"Process {pid}, round {round_no + 1}",
            )

    sim.run()

    assert sim.safety.is_safe
    assert ra.completed == 15
    assert sim.network.messages_sent == 120
