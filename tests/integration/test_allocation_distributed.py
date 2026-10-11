"""Distributed integration test for concurrent blood allocations coordinated by DME over real TCP sockets.

OWNERSHIP & CONSISTENCY MODEL (OPTION A):
The test instantiates an authoritative in-process BloodBankInventory instance.
Branch nodes act as independent distributed runtime agents communicating over TCP sockets.
Each node hosts an AllocationService instance accessing the authoritative inventory.
Access to the inventory is strictly serialized by the chosen distributed mutual exclusion
algorithm (Ricart-Agrawala or Suzuki-Kasami).

LIMITATION NOTICE:
This integration test validates process-local authoritative inventory coordinated via
TCP-connected distributed mutual exclusion. It does not claim that process-local in-memory
state is shared across isolated container address spaces without an external distributed store.
"""
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


def get_two_node_loopback_config(base_port: int, algorithm: str = "ricart_agrawala"):
    """Generate loopback configuration for two TCP nodes."""
    return load_config_from_dict({
        "system": {"algorithm": algorithm, "protocol_version": "1.0"},
        "nodes": [
            {
                "node_id": "branch-0",
                "host": "127.0.0.1",
                "port": base_port,
                "health_port": base_port + 1000,
            },
            {
                "node_id": "branch-1",
                "host": "127.0.0.1",
                "port": base_port + 1,
                "health_port": base_port + 1001,
            },
        ],
        "network": {
            "mean_delay_ms": 1.0,
            "jitter_ms": 0.0,
            "message_loss_rate": 0.0,
            "connection_timeout_ms": 3000.0,
        },
        "timeouts": {
            "message_delivery_timeout_ms": 3000.0,
            "token_response_timeout_ms": 5000.0,
            "heartbeat_interval_ms": 1000.0,
            "heartbeat_timeout_ms": 3000.0,
            "max_retries": 3,
            "retry_backoff_ms": 50.0,
        },
    })


def test_concurrent_allocations_ricart_agrawala():
    """Verify concurrent allocation requests across two TCP nodes under Ricart-Agrawala DME."""
    async def _run():
        config = get_two_node_loopback_config(base_port=16100, algorithm="ricart_agrawala")

        node0 = BranchNode("branch-0", config)
        node1 = BranchNode("branch-1", config)

        await node0.start()
        await node1.start()

        try:
            # Single authoritative inventory with exactly ONE scarce O- RBC unit
            authoritative_inv = BloodBankInventory([
                BloodUnit("SCARCE-O-NEG-01", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
            ])

            service0 = AllocationService(node0, authoritative_inv, valid_branches={"branch-0", "branch-1"})
            service1 = AllocationService(node1, authoritative_inv, valid_branches={"branch-0", "branch-1"})

            # Create concurrent allocation requests from both branches
            req0 = AllocationRequest(
                request_id="req-branch-0",
                requesting_branch="branch-0",
                blood_group=BloodGroup.O_NEG,
                component_type=ComponentType.RBC,
                quantity=1,
            )
            req1 = AllocationRequest(
                request_id="req-branch-1",
                requesting_branch="branch-1",
                blood_group=BloodGroup.O_NEG,
                component_type=ComponentType.RBC,
                quantity=1,
            )

            # Fire both requests concurrently
            res0, res1 = await asyncio.gather(
                service0.execute_allocation(req0, timeout_sec=5.0),
                service1.execute_allocation(req1, timeout_sec=5.0),
            )

            # Both must complete with CS entered
            assert res0.cs_entered is True
            assert res1.cs_entered is True

            # Exactly one request must succeed, and the other must get INSUFFICIENT_STOCK
            results = [res0, res1]
            successes = [r for r in results if r.status == AllocationStatus.SUCCESS]
            insufficient = [r for r in results if r.status == AllocationStatus.INSUFFICIENT_STOCK]

            assert len(successes) == 1, f"Expected exactly 1 success, got {[r.status for r in results]}"
            assert len(insufficient) == 1, f"Expected exactly 1 insufficient, got {[r.status for r in results]}"

            # The winner must hold the single scarce unit
            winner = successes[0]
            assert winner.allocated_units == ["SCARCE-O-NEG-01"]

            unit = authoritative_inv.get_unit("SCARCE-O-NEG-01")
            assert unit.status == UnitStatus.ALLOCATED
            assert unit.allocation_id == winner.request_id

            # Verify that critical section intervals were sequential, never overlapping
            h0 = node0.runtime_state.cs_history
            h1 = node1.runtime_state.cs_history
            assert len(h0) == 1
            assert len(h1) == 1

            entry0, exit0 = h0[0]["entry_time"], h0[0]["exit_time"]
            entry1, exit1 = h1[0]["entry_time"], h1[0]["exit_time"]

            # Mutual exclusion invariant: either node0 finished before node1 entered, or vice versa
            no_overlap = (exit0 <= entry1) or (exit1 <= entry0)
            assert no_overlap, f"Critical sections overlapped! Node0: ({entry0}, {exit0}), Node1: ({entry1}, {exit1})"

        finally:
            await node0.stop()
            await node1.stop()

    asyncio.run(_run())


def test_concurrent_allocations_suzuki_kasami_sufficient_stock():
    """Verify concurrent allocations with Suzuki-Kasami where sufficient units exist for both."""
    async def _run():
        config = get_two_node_loopback_config(base_port=16200, algorithm="suzuki_kasami")

        # branch-0 initially holds token
        node0 = BranchNode("branch-0", config, initial_token_holder="branch-0")
        node1 = BranchNode("branch-1", config, initial_token_holder="branch-0")

        await node0.start()
        await node1.start()

        try:
            # Authoritative inventory with TWO A+ RBC units
            authoritative_inv = BloodBankInventory([
                BloodUnit("UNIT-A-POS-01", BloodGroup.A_POS, ComponentType.RBC, "branch-0"),
                BloodUnit("UNIT-A-POS-02", BloodGroup.A_POS, ComponentType.RBC, "branch-1"),
            ])

            service0 = AllocationService(node0, authoritative_inv, valid_branches={"branch-0", "branch-1"})
            service1 = AllocationService(node1, authoritative_inv, valid_branches={"branch-0", "branch-1"})

            req0 = AllocationRequest("sk-req-0", "branch-0", BloodGroup.A_POS, ComponentType.RBC, 1)
            req1 = AllocationRequest("sk-req-1", "branch-1", BloodGroup.A_POS, ComponentType.RBC, 1)

            res0, res1 = await asyncio.gather(
                service0.execute_allocation(req0, timeout_sec=5.0),
                service1.execute_allocation(req1, timeout_sec=5.0),
            )

            # Both should succeed
            assert res0.status == AllocationStatus.SUCCESS
            assert res1.status == AllocationStatus.SUCCESS

            # Allocated units must be completely disjoint (NO double allocation!)
            set0 = set(res0.allocated_units)
            set1 = set(res1.allocated_units)
            assert len(set0.intersection(set1)) == 0, f"Double allocation detected: {set0.intersection(set1)}"
            assert set0.union(set1) == {"UNIT-A-POS-01", "UNIT-A-POS-02"}

            # Both units are ALLOCATED
            assert authoritative_inv.available_count() == 0
            assert authoritative_inv.total_count() == 2

            # Critical section execution order was strictly serialized
            h0 = node0.runtime_state.cs_history
            h1 = node1.runtime_state.cs_history
            entry0, exit0 = h0[0]["entry_time"], h0[0]["exit_time"]
            entry1, exit1 = h1[0]["entry_time"], h1[0]["exit_time"]

            no_overlap = (exit0 <= entry1) or (exit1 <= entry0)
            assert no_overlap, "Critical sections overlapped in Suzuki-Kasami!"

        finally:
            await node0.stop()
            await node1.stop()

    asyncio.run(_run())
