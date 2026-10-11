"""Deterministic tests for failure detection, fault scenarios, and safe recovery policies."""
import asyncio
import time
import pytest
from src.common.configuration import load_config_from_dict
from src.node.branch_node import BranchNode
from src.blood_bank.models import (
    BloodGroup,
    ComponentType,
    UnitStatus,
    BloodUnit,
    AllocationRequest,
    AllocationStatus,
)
from src.blood_bank.inventory import BloodBankInventory
from src.blood_bank.allocation_service import AllocationService
from src.node.failure_detector import PeerHealthStatus


def get_two_node_test_config(base_port: int, algorithm: str = "ricart_agrawala"):
    return load_config_from_dict({
        "system": {"algorithm": algorithm, "protocol_version": "1.0"},
        "nodes": [
            {"node_id": "branch-0", "host": "127.0.0.1", "port": base_port, "health_port": base_port + 1000},
            {"node_id": "branch-1", "host": "127.0.0.1", "port": base_port + 1, "health_port": base_port + 1001},
        ],
        "network": {
            "mean_delay_ms": 1.0, "jitter_ms": 0.0, "message_loss_rate": 0.0, "connection_timeout_ms": 1000.0,
        },
        "timeouts": {
            "message_delivery_timeout_ms": 500.0,
            "token_response_timeout_ms": 1000.0,
            "heartbeat_interval_ms": 100.0,
            "heartbeat_timeout_ms": 300.0,
            "max_retries": 1,
            "retry_backoff_ms": 10.0,
        },
    })


def test_node_crash_before_requesting_cs():
    """Verify that if a node crashes before requesting CS, peers detect failure and fail closed."""
    async def _run():
        config = get_two_node_test_config(base_port=17200, algorithm="ricart_agrawala")
        node0 = BranchNode("branch-0", config, enable_failure_detection=True)
        node1 = BranchNode("branch-1", config, enable_failure_detection=True)

        await node0.start()
        await node1.start()

        try:
            # Both nodes exchange heartbeats initially
            await asyncio.sleep(0.15)
            assert node0.failure_detector.is_healthy("branch-1")

            # Simulate crash of branch-1 before requesting CS
            await node1.stop()

            # Wait for node0's failure detector to detect silence past heartbeat timeout (300ms)
            await asyncio.sleep(0.45)
            assert not node0.failure_detector.is_healthy("branch-1")
            assert (
                node0.failure_detector.is_suspected("branch-1")
                or node0.failure_detector.is_confirmed_failed("branch-1")
            )

            # Node0 attempts allocation with fail-closed enabled
            inv = BloodBankInventory([
                BloodUnit("U-1", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
            ])
            service0 = AllocationService(node0, inv, fail_closed_on_unhealthy=True)

            req = AllocationRequest("req-fail-closed", "branch-0", BloodGroup.O_NEG, ComponentType.RBC, 1)
            res = await service0.execute_allocation(req, timeout_sec=0.5)

            # Must fail closed: quorum cannot be safely formed without branch-1
            assert res.status == AllocationStatus.FAILED_CLOSED
            assert res.cs_entered is False
            assert "Fail-closed" in res.error_message
            # Inventory remains unmutated
            assert inv.get_unit("U-1").status == UnitStatus.AVAILABLE

        finally:
            await node0.stop()

    asyncio.run(_run())


def test_node_crash_while_waiting_for_cs():
    """Verify that if a node crashes while waiting for CS, cleanup occurs and protocol is not released."""
    async def _run():
        config = get_two_node_test_config(base_port=17300, algorithm="suzuki_kasami")
        # branch-1 starts with token, but is never started (offline / crashed)
        node0 = BranchNode("branch-0", config, initial_token_holder="branch-1", enable_failure_detection=True)
        await node0.start()

        try:
            # Mark branch-1 as confirmed failed
            node0.failure_detector.confirm_failure("branch-1", reason="simulated_crash")

            inv = BloodBankInventory([
                BloodUnit("U-1", BloodGroup.A_POS, ComponentType.RBC, "branch-0")
            ])
            service0 = AllocationService(node0, inv, fail_closed_on_unhealthy=True)

            req = AllocationRequest("req-wait-crash", "branch-0", BloodGroup.A_POS, ComponentType.RBC, 1)
            res = await service0.execute_allocation(req, timeout_sec=0.2)

            # Node was waiting for token, token holder is dead -> fail closed
            assert res.status == AllocationStatus.FAILED_CLOSED
            assert res.cs_entered is False
            # Verify protocol was NEVER released since CS was never entered
            assert node0.runtime_state.cs_entry_count == 0
            assert node0.runtime_state.cs_exit_count == 0
            assert inv.get_unit("U-1").status == UnitStatus.AVAILABLE

        finally:
            await node0.stop()

    asyncio.run(_run())


def test_node_crash_while_holding_token_in_cs_and_no_regeneration():
    """Verify that when a token holder crashes in CS, remaining nodes DO NOT regenerate token on timeout."""
    async def _run():
        config = get_two_node_test_config(base_port=17400, algorithm="suzuki_kasami")

        # branch-0 holds token
        node0 = BranchNode("branch-0", config, initial_token_holder="branch-0")
        node1 = BranchNode("branch-1", config, initial_token_holder="branch-0")

        await node0.start()
        await node1.start()

        try:
            # node0 enters CS
            await node0.request_critical_section("holder-req")
            await node0.enter_critical_section("holder-req")
            assert node0.protocol.can_enter_cs() or node0.protocol.state.value == "EXECUTING"

            # node0 crashes while inside CS without releasing
            await node0.stop()

            # node1 requests CS: it does NOT have token. Token holder has crashed.
            node1.failure_detector.confirm_failure("branch-0", reason="crashed_in_cs")

            inv = BloodBankInventory([
                BloodUnit("U-1", BloodGroup.B_POS, ComponentType.RBC, "branch-1")
            ])
            service1 = AllocationService(node1, inv, fail_closed_on_unhealthy=True)

            req1 = AllocationRequest("req-node1", "branch-1", BloodGroup.B_POS, ComponentType.RBC, 1)
            res1 = await service1.execute_allocation(req1, timeout_sec=0.3)

            # SAFETY INVARIANT: node1 MUST NOT manufacture a duplicate token!
            # Must fail closed and report that coordinated recovery is required
            assert res1.status == AllocationStatus.FAILED_CLOSED
            assert res1.cs_entered is False
            assert not node1.protocol.has_token
            assert node1.runtime_state.cs_entry_count == 0
            assert inv.get_unit("U-1").status == UnitStatus.AVAILABLE

        finally:
            await node1.stop()

    asyncio.run(_run())


def test_node_unreachable_and_reconnection():
    """Verify that when an unreachable node reconnects, failure detector transitions back to HEALTHY."""
    async def _run():
        config = get_two_node_test_config(base_port=17500, algorithm="ricart_agrawala")
        node0 = BranchNode("branch-0", config, enable_failure_detection=True)
        node1 = BranchNode("branch-1", config, enable_failure_detection=True)

        await node0.start()
        await node1.start()

        try:
            # Initially healthy
            await asyncio.sleep(0.15)
            assert node0.failure_detector.is_healthy("branch-1")

            # Temporarily stop node1 to simulate unreachable network drop
            await node1.stop()
            await asyncio.sleep(0.45)
            assert not node0.failure_detector.is_healthy("branch-1")

            # Node1 reconnects / restarts with fresh server
            node1_reconnected = BranchNode("branch-1", config, enable_failure_detection=True)
            await node1_reconnected.start()

            try:
                # Allow heartbeats to re-establish
                await asyncio.sleep(0.25)
                # Node0 detects renewed heartbeats and marks branch-1 HEALTHY again
                assert node0.failure_detector.is_healthy("branch-1")

                # Verify transition history recorded PEER_RECOVERED
                transitions = [
                    t for t in node0.failure_detector.transition_history
                    if t["new_status"] == PeerHealthStatus.HEALTHY.value and t["previous_status"] != PeerHealthStatus.HEALTHY.value
                ]
                assert len(transitions) >= 1
            finally:
                await node1_reconnected.stop()

        finally:
            await node0.stop()

    asyncio.run(_run())


def test_recovered_node_cannot_reuse_stale_permissions():
    """Verify that a restarted node has a distinct incarnation ID and cannot reuse stale in-flight state."""
    config = get_two_node_test_config(base_port=17600, algorithm="suzuki_kasami")

    node_v1 = BranchNode("branch-0", config, initial_token_holder="branch-0")
    epoch1 = node_v1.incarnation_id

    # Restart instance
    node_v2 = BranchNode("branch-0", config, initial_token_holder="branch-1")
    epoch2 = node_v2.incarnation_id

    # Incarnations must be distinct
    assert epoch1 != epoch2

    # node_v2 starts fresh without token (does not inherit stale token privilege)
    assert node_v2.protocol.has_token is False
    assert node_v2.protocol.can_enter_cs() is False
    assert node_v2.runtime_state.cs_entry_count == 0


def test_crash_during_allocation_leaves_uncertain_reservation():
    """Verify that a crash after reserving units leaves them in uncertain state without unsafe automatic release."""
    inv = BloodBankInventory([
        BloodUnit("U-UNCERTAIN-1", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
    ])

    # Node reserves unit
    reserved = inv.reserve(["U-UNCERTAIN-1"], "alloc-crash-test")
    assert len(reserved) == 1
    assert inv.get_unit("U-UNCERTAIN-1").status == UnitStatus.RESERVED

    # Simulating crash of allocating node:
    # 1. Units MUST NOT be automatically released upon timeout
    inv.mark_reservation_uncertain("alloc-crash-test", reason="allocating_node_crashed")

    uncertain = inv.get_uncertain_reservations()
    assert len(uncertain) == 1
    assert uncertain[0]["unit_id"] == "U-UNCERTAIN-1"
    assert uncertain[0]["metadata"]["is_uncertain"] is True

    # 2. Another allocation request CANNOT allocate or double-allocate this reserved unit
    with pytest.raises(Exception):  # InvalidStateTransitionError or not found in eligible
        inv.allocate(["U-UNCERTAIN-1"], "alloc-intruder-request")

    # 3. Explicit administrative reconciliation: operator confirms transaction was aborted
    reconciled = inv.reconcile_uncertain_reservation("alloc-crash-test", action="ABORT", operator_note="Operator confirmed client aborted")
    assert len(reconciled) == 1
    assert inv.get_unit("U-UNCERTAIN-1").status == UnitStatus.AVAILABLE
    assert inv.get_unit("U-UNCERTAIN-1").metadata.get("is_uncertain") is None

    # Now the unit can be safely allocated by a new valid request
    new_alloc = inv.allocate(["U-UNCERTAIN-1"], "alloc-new-legitimate")
    assert len(new_alloc) == 1
    assert inv.get_unit("U-UNCERTAIN-1").status == UnitStatus.ALLOCATED
