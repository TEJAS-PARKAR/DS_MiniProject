"""Unit tests for SafetyMonitor."""
from src.simulation.safety import SafetyMonitor


def test_safety_valid_sequential_access():
    monitor = SafetyMonitor()
    assert monitor.enter(0, 1.0)
    assert monitor.exit(0, 2.0)
    assert monitor.enter(1, 2.1)
    assert monitor.exit(1, 3.0)
    assert monitor.is_safe
    assert len(monitor.violations) == 0


def test_safety_simultaneous_access_detected():
    monitor = SafetyMonitor()
    assert monitor.enter(0, 1.0)
    assert not monitor.enter(1, 1.5)
    assert not monitor.is_safe
    assert len(monitor.violations) == 1
    assert monitor.violations[0]["type"] == "simultaneous_critical_section"


def test_safety_duplicate_entry_detected():
    monitor = SafetyMonitor()
    assert monitor.enter(0, 1.0)
    assert not monitor.enter(0, 1.2)
    assert not monitor.is_safe
    assert monitor.violations[0]["type"] == "duplicate_entry"


def test_safety_exit_without_entry_detected():
    monitor = SafetyMonitor()
    assert not monitor.exit(0, 1.0)
    assert not monitor.is_safe
    assert monitor.violations[0]["type"] == "exit_without_entry"
