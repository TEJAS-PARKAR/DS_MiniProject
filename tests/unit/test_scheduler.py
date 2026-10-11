"""Unit tests for EventScheduler."""
import pytest
from src.simulation.scheduler import EventScheduler


def test_scheduler_ordering():
    scheduler = EventScheduler()
    event_log = []

    scheduler.schedule(5.0, lambda: event_log.append("Event at time 5"), "first")
    scheduler.schedule(2.0, lambda: event_log.append("Event at time 2"), "second")
    scheduler.schedule(5.0, lambda: event_log.append("Second event at time 5"), "third")

    processed = scheduler.run()

    assert processed == 3
    assert event_log == [
        "Event at time 2",
        "Event at time 5",
        "Second event at time 5",
    ]
    assert scheduler.current_time == 5.0


def test_scheduler_cannot_schedule_in_past():
    scheduler = EventScheduler()
    scheduler.schedule(5.0, lambda: None)
    scheduler.run()

    with pytest.raises(ValueError, match="Cannot schedule event in past"):
        scheduler.schedule(4.0, lambda: None)


def test_scheduler_empty_pop():
    scheduler = EventScheduler()
    with pytest.raises(IndexError, match="No events remaining"):
        scheduler.pop_next_event()
