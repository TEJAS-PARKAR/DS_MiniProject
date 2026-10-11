"""Unit tests for the suspicion-based failure detector and peer liveness tracking."""
import time
import pytest
from src.node.failure_detector import FailureDetector, PeerHealthStatus


def test_initial_health_state():
    """Verify newly initialized failure detector starts with all peers healthy."""
    fd = FailureDetector(
        node_id="branch-0",
        peers={"branch-1", "branch-2"},
        heartbeat_timeout_ms=1000.0,
        confirmation_timeout_ms=2500.0,
    )
    assert fd.is_healthy("branch-1")
    assert fd.is_healthy("branch-2")
    assert not fd.has_unhealthy_peers()
    assert len(fd.get_suspected_peers()) == 0
    assert len(fd.get_failed_peers()) == 0


def test_heartbeat_timeout_transitions_to_suspected():
    """Verify that exceeding heartbeat timeout transitions peer from HEALTHY to SUSPECTED."""
    now = 1000.0
    fd = FailureDetector(
        node_id="branch-0",
        peers={"branch-1"},
        heartbeat_timeout_ms=1000.0,
        confirmation_timeout_ms=2500.0,
    )
    fd.last_seen["branch-1"] = now

    # At elapsed = 0.5s: still HEALTHY
    transitions = fd.evaluate_health(now=now + 0.5)
    assert len(transitions) == 0
    assert fd.is_healthy("branch-1")

    # At elapsed = 1.1s: exceeds 1.0s heartbeat timeout -> SUSPECTED
    transitions = fd.evaluate_health(now=now + 1.1)
    assert len(transitions) == 1
    peer, old_status, new_status = transitions[0]
    assert peer == "branch-1"
    assert old_status == PeerHealthStatus.HEALTHY
    assert new_status == PeerHealthStatus.SUSPECTED

    assert fd.is_suspected("branch-1")
    assert not fd.is_healthy("branch-1")
    assert fd.has_unhealthy_peers()
    assert fd.get_suspected_peers() == ["branch-1"]


def test_confirmation_timeout_transitions_to_confirmed_failed():
    """Verify that exceeding confirmation timeout transitions peer to CONFIRMED_FAILED."""
    now = 1000.0
    fd = FailureDetector(
        node_id="branch-0",
        peers={"branch-1"},
        heartbeat_timeout_ms=1000.0,
        confirmation_timeout_ms=2500.0,
    )
    fd.last_seen["branch-1"] = now

    # At elapsed = 2.6s: exceeds confirmation timeout -> CONFIRMED_FAILED
    transitions = fd.evaluate_health(now=now + 2.6)
    assert len(transitions) == 1
    peer, old_status, new_status = transitions[0]
    assert peer == "branch-1"
    assert new_status == PeerHealthStatus.CONFIRMED_FAILED

    assert fd.is_confirmed_failed("branch-1")
    assert fd.get_failed_peers() == ["branch-1"]


def test_heartbeat_received_recovers_to_healthy():
    """Verify that receiving a heartbeat restores a SUSPECTED or FAILED peer to HEALTHY."""
    now = 1000.0
    fd = FailureDetector(
        node_id="branch-0",
        peers={"branch-1"},
        heartbeat_timeout_ms=1000.0,
        confirmation_timeout_ms=2500.0,
    )
    fd.last_seen["branch-1"] = now

    # Force transition to SUSPECTED
    fd.evaluate_health(now=now + 1.5)
    assert fd.is_suspected("branch-1")

    # Heartbeat arrives
    fd.record_message("branch-1", timestamp=now + 1.6)
    assert fd.is_healthy("branch-1")
    assert not fd.is_suspected("branch-1")

    # History records recovery event
    assert len(fd.transition_history) >= 2
    recovery_event = fd.transition_history[-1]
    assert recovery_event["peer_id"] == "branch-1"
    assert recovery_event["new_status"] == PeerHealthStatus.HEALTHY.value
    assert recovery_event["reason"] == "heartbeat_or_message_received"


def test_confirm_failure_direct_transition():
    """Verify that explicit fatal transport drop confirms failure immediately."""
    fd = FailureDetector(
        node_id="branch-0",
        peers={"branch-1"},
        heartbeat_timeout_ms=1000.0,
    )
    assert fd.is_healthy("branch-1")

    fd.confirm_failure("branch-1", reason="connection_refused_on_connect")
    assert fd.is_confirmed_failed("branch-1")
    assert fd.get_failed_peers() == ["branch-1"]

    # History logs the specific reason
    event = fd.transition_history[-1]
    assert event["reason"] == "connection_refused_on_connect"


def test_failure_detector_diagnostics():
    """Verify to_dict returns complete structured diagnostic status."""
    fd = FailureDetector(
        node_id="branch-0",
        peers={"branch-1", "branch-2"},
        heartbeat_timeout_ms=1000.0,
    )
    diag = fd.to_dict()
    assert diag["node_id"] == "branch-0"
    assert diag["monitored_peers"] == ["branch-1", "branch-2"]
    assert "peer_status" in diag
    assert "last_seen_seconds_ago" in diag
    assert "total_transition_events" in diag
