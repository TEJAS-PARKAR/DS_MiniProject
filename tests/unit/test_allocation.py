"""Unit tests for allocation requests, protected operations, and DME lifecycle."""
import asyncio
import pytest
from src.common.configuration import load_config_from_dict
from src.node.branch_node import BranchNode
from src.blood_bank.models import (
    BloodGroup,
    ComponentType,
    UnitStatus,
    BloodUnit,
    AllocationRequest,
    AllocationResult,
    AllocationStatus,
)
from src.blood_bank.inventory import BloodBankInventory
from src.blood_bank.compatibility import SyntheticCompatibilityPolicy
from src.blood_bank.allocation_service import AllocationService


def get_single_node_config(algorithm: str = "suzuki_kasami"):
    """Single node configuration for isolated unit testing."""
    return load_config_from_dict({
        "system": {"algorithm": algorithm, "protocol_version": "1.0"},
        "nodes": [
            {
                "node_id": "branch-0",
                "host": "127.0.0.1",
                "port": 17001,
                "health_port": 18001,
            }
        ],
        "network": {
            "mean_delay_ms": 1.0,
            "jitter_ms": 0.0,
            "message_loss_rate": 0.0,
            "connection_timeout_ms": 1000.0,
        },
        "timeouts": {
            "message_delivery_timeout_ms": 1000.0,
            "token_response_timeout_ms": 1000.0,
            "heartbeat_interval_ms": 1000.0,
            "heartbeat_timeout_ms": 2000.0,
            "max_retries": 1,
            "retry_backoff_ms": 50.0,
        },
    })


def test_allocation_request_validation():
    """Verify validation of allocation requests."""
    valid_branches = {"branch-0", "branch-1"}

    # Valid request
    req = AllocationRequest(
        request_id="req-1",
        requesting_branch="branch-0",
        blood_group=BloodGroup.O_NEG,
        component_type=ComponentType.RBC,
        quantity=2,
    )
    req.validate(valid_branches=valid_branches)
    assert req.quantity == 2

    # Zero or negative quantity
    with pytest.raises(ValueError, match="strictly positive integer"):
        AllocationRequest("req-2", "branch-0", BloodGroup.A_POS, ComponentType.RBC, 0).validate()

    with pytest.raises(ValueError, match="strictly positive integer"):
        AllocationRequest("req-3", "branch-0", BloodGroup.A_POS, ComponentType.RBC, -5).validate()

    # Empty request ID
    with pytest.raises(ValueError, match="non-empty string"):
        AllocationRequest("", "branch-0", BloodGroup.A_POS, ComponentType.RBC, 1).validate()

    # Unknown branch
    with pytest.raises(ValueError, match="Unknown requesting branch"):
        AllocationRequest("req-4", "branch-unknown", BloodGroup.A_POS, ComponentType.RBC, 1).validate(
            valid_branches=valid_branches
        )


def test_successful_protected_allocation():
    """Verify complete protected allocation lifecycle with single token-holder node."""
    async def _run():
        config = get_single_node_config("suzuki_kasami")
        node = BranchNode("branch-0", config, initial_token_holder="branch-0")
        await node.start()

        try:
            inv = BloodBankInventory([
                BloodUnit("U-1", BloodGroup.O_NEG, ComponentType.RBC, "branch-0"),
                BloodUnit("U-2", BloodGroup.O_NEG, ComponentType.RBC, "branch-0"),
            ])
            service = AllocationService(node, inv, valid_branches={"branch-0"})

            req = AllocationRequest(
                request_id="req-alloc-1",
                requesting_branch="branch-0",
                blood_group=BloodGroup.O_NEG,
                component_type=ComponentType.RBC,
                quantity=1,
            )

            result = await service.execute_allocation(req)

            assert result.status == AllocationStatus.SUCCESS
            assert result.cs_entered is True
            assert len(result.allocated_units) == 1
            assert result.allocated_units[0] == "U-1"

            # Check unit status in authoritative inventory
            unit1 = inv.get_unit("U-1")
            assert unit1.status == UnitStatus.ALLOCATED
            assert unit1.allocation_id == "req-alloc-1"

            # Protocol must have exited CS cleanly
            assert node.runtime_state.cs_entry_count == 1
            assert node.runtime_state.cs_exit_count == 1
            assert node.runtime_state.active_request_id is None
        finally:
            await node.stop()

    asyncio.run(_run())


def test_insufficient_inventory_releases_cs_cleanly():
    """Verify that insufficient inventory does not hold CS and returns INSUFFICIENT_STOCK."""
    async def _run():
        config = get_single_node_config("suzuki_kasami")
        node = BranchNode("branch-0", config, initial_token_holder="branch-0")
        await node.start()

        try:
            inv = BloodBankInventory([
                BloodUnit("U-1", BloodGroup.O_NEG, ComponentType.RBC, "branch-0"),
            ])
            service = AllocationService(node, inv)

            # Request 5 units when only 1 exists
            req = AllocationRequest(
                request_id="req-excess-qty",
                requesting_branch="branch-0",
                blood_group=BloodGroup.O_NEG,
                component_type=ComponentType.RBC,
                quantity=5,
            )

            result = await service.execute_allocation(req)

            assert result.status == AllocationStatus.INSUFFICIENT_STOCK
            assert result.cs_entered is True
            assert len(result.allocated_units) == 0
            assert "Insufficient inventory" in result.error_message

            # Existing unit remains available
            assert inv.get_unit("U-1").status == UnitStatus.AVAILABLE

            # Protocol cleanly released
            assert node.runtime_state.cs_entry_count == 1
            assert node.runtime_state.cs_exit_count == 1
        finally:
            await node.stop()

    asyncio.run(_run())


def test_invalid_request_fails_before_cs_entry():
    """Verify pre-validation failure never requests or enters CS and never releases protocol."""
    async def _run():
        config = get_single_node_config("suzuki_kasami")
        node = BranchNode("branch-0", config, initial_token_holder="branch-0")
        await node.start()

        try:
            inv = BloodBankInventory()
            service = AllocationService(node, inv, valid_branches={"branch-0"})

            # Invalid requesting branch
            req = AllocationRequest(
                request_id="req-invalid-branch",
                requesting_branch="branch-unknown",
                blood_group=BloodGroup.A_POS,
                component_type=ComponentType.RBC,
                quantity=1,
            )

            result = await service.execute_allocation(req)

            assert result.status == AllocationStatus.VALIDATION_FAILED
            assert result.cs_entered is False

            # Mutual exclusion was NEVER entered or released
            assert node.runtime_state.cs_entry_count == 0
            assert node.runtime_state.cs_exit_count == 0
        finally:
            await node.stop()

    asyncio.run(_run())


def test_timeout_waiting_for_cs_never_releases_protocol():
    """Verify that if CS grant times out, protocol is never released and units are untouched."""
    async def _run():
        # branch-0 does NOT hold token (branch-1 does, but branch-1 is offline) -> times out
        config = load_config_from_dict({
            "system": {"algorithm": "suzuki_kasami", "protocol_version": "1.0"},
            "nodes": [
                {"node_id": "branch-0", "host": "127.0.0.1", "port": 17010, "health_port": 18010},
                {"node_id": "branch-1", "host": "127.0.0.1", "port": 17011, "health_port": 18011},
            ],
            "network": {"mean_delay_ms": 1.0, "jitter_ms": 0.0, "message_loss_rate": 0.0, "connection_timeout_ms": 500.0},
            "timeouts": {
                "message_delivery_timeout_ms": 500.0,
                "token_response_timeout_ms": 500.0,
                "heartbeat_interval_ms": 1000.0,
                "heartbeat_timeout_ms": 2000.0,
                "max_retries": 1,
                "retry_backoff_ms": 10.0,
            },
        })
        node = BranchNode("branch-0", config, initial_token_holder="branch-1")
        await node.start()

        try:
            inv = BloodBankInventory([
                BloodUnit("U-1", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
            ])
            service = AllocationService(node, inv)

            req = AllocationRequest(
                request_id="req-timeout",
                requesting_branch="branch-0",
                blood_group=BloodGroup.O_NEG,
                component_type=ComponentType.RBC,
                quantity=1,
            )

            result = await service.execute_allocation(req, timeout_sec=0.1)

            assert result.status == AllocationStatus.MUTUAL_EXCLUSION_FAILED
            assert result.cs_entered is False
            assert node.runtime_state.cs_entry_count == 0
            assert node.runtime_state.cs_exit_count == 0

            # Unit remains untouched
            assert inv.get_unit("U-1").status == UnitStatus.AVAILABLE
        finally:
            await node.stop()

    asyncio.run(_run())


def test_no_double_allocation_across_sequential_requests():
    """Verify that allocated units are never allocated to a second request."""
    async def _run():
        config = get_single_node_config("suzuki_kasami")
        node = BranchNode("branch-0", config, initial_token_holder="branch-0")
        await node.start()

        try:
            inv = BloodBankInventory([
                BloodUnit("U-1", BloodGroup.A_POS, ComponentType.RBC, "branch-0")
            ])
            service = AllocationService(node, inv)

            # Request 1: Allocates U-1
            req1 = AllocationRequest("req-1", "branch-0", BloodGroup.A_POS, ComponentType.RBC, 1)
            res1 = await service.execute_allocation(req1)
            assert res1.status == AllocationStatus.SUCCESS
            assert res1.allocated_units == ["U-1"]

            # Request 2: Asks for another A+ unit; only U-1 existed, now ALLOCATED
            req2 = AllocationRequest("req-2", "branch-0", BloodGroup.A_POS, ComponentType.RBC, 1)
            res2 = await service.execute_allocation(req2)
            assert res2.status == AllocationStatus.INSUFFICIENT_STOCK
            assert len(res2.allocated_units) == 0

            # U-1 still owned by req-1
            assert inv.get_unit("U-1").allocation_id == "req-1"
            assert inv.get_unit("U-1").status == UnitStatus.ALLOCATED
        finally:
            await node.stop()

    asyncio.run(_run())


def test_protected_reservation_workflow():
    """Verify reserve_only=True transitions units to RESERVED, not ALLOCATED."""
    async def _run():
        config = get_single_node_config("suzuki_kasami")
        node = BranchNode("branch-0", config, initial_token_holder="branch-0")
        await node.start()

        try:
            inv = BloodBankInventory([
                BloodUnit("U-RES-1", BloodGroup.B_POS, ComponentType.PLATELETS, "branch-0")
            ])
            service = AllocationService(node, inv)

            req = AllocationRequest("req-reserve", "branch-0", BloodGroup.B_POS, ComponentType.PLATELETS, 1)
            res = await service.execute_allocation(req, reserve_only=True)

            assert res.status == AllocationStatus.SUCCESS
            assert res.allocated_units == ["U-RES-1"]
            assert inv.get_unit("U-RES-1").status == UnitStatus.RESERVED
            assert inv.get_unit("U-RES-1").allocation_id == "req-reserve"
        finally:
            await node.stop()

    asyncio.run(_run())


def test_safe_cleanup_when_allocation_fails_after_entry():
    """Verify that if an error occurs inside CS, the protocol is safely released."""
    async def _run():
        config = get_single_node_config("suzuki_kasami")
        node = BranchNode("branch-0", config, initial_token_holder="branch-0")
        await node.start()

        try:
            inv = BloodBankInventory([
                BloodUnit("U-ERR-1", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
            ])
            service = AllocationService(node, inv)

            # Force inventory.allocate to raise an unexpected runtime error
            def faulty_allocate(*args, **kwargs):
                raise RuntimeError("Simulated transaction crash inside critical section")

            inv.allocate = faulty_allocate

            req = AllocationRequest("req-fail", "branch-0", BloodGroup.O_NEG, ComponentType.RBC, 1)
            res = await service.execute_allocation(req)

            assert res.status == AllocationStatus.ERROR
            assert res.cs_entered is True
            assert "Simulated transaction crash" in res.error_message

            # Protocol MUST have been released via finally block!
            assert node.runtime_state.cs_entry_count == 1
            assert node.runtime_state.cs_exit_count == 1
            assert node.runtime_state.active_request_id is None
        finally:
            await node.stop()

    asyncio.run(_run())

