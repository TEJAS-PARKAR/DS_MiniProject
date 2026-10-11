"""Unit tests for MessageNetwork."""
import pytest
from src.simulation.scheduler import EventScheduler
from src.simulation.network import MessageNetwork


def test_network_message_delivery():
    scheduler = EventScheduler()
    received = []
    network = MessageNetwork(
        scheduler=scheduler,
        mean_delay=1.0,
        loss_rate=0.0,
        seed=42,
    )

    network.send(
        sender=0,
        receiver=1,
        message={"type": "TEST"},
        handler=lambda msg: received.append(msg),
    )

    scheduler.run()

    assert network.messages_sent == 1
    assert network.messages_delivered == 1
    assert network.messages_dropped == 0
    assert len(received) == 1
    assert received[0] == {"type": "TEST"}


def test_network_message_drop():
    scheduler = EventScheduler()
    received = []
    network = MessageNetwork(
        scheduler=scheduler,
        mean_delay=1.0,
        loss_rate=1.0,  # 100% loss
        seed=42,
    )

    network.send(
        sender=0,
        receiver=1,
        message={"type": "TEST"},
        handler=lambda msg: received.append(msg),
    )

    scheduler.run()

    assert network.messages_sent == 1
    assert network.messages_delivered == 0
    assert network.messages_dropped == 1
    assert len(received) == 0


def test_network_invalid_parameters():
    scheduler = EventScheduler()
    with pytest.raises(ValueError, match="Mean delay cannot be negative"):
        MessageNetwork(scheduler=scheduler, mean_delay=-1.0)

    with pytest.raises(ValueError, match="Loss rate must be between 0 and 1"):
        MessageNetwork(scheduler=scheduler, loss_rate=1.5)
