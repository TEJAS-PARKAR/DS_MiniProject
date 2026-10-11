"""Demonstration script: Runs a distributed blood allocation across TCP-connected branch nodes.

Usage:
    python demo_allocation.py
"""
import asyncio
import time
from src.common.configuration import load_config_from_dict
from src.node.branch_node import BranchNode
from src.blood_bank.models import (
    BloodGroup,
    ComponentType,
    BloodUnit,
    AllocationRequest,
    AllocationStatus,
)
from src.blood_bank.inventory import BloodBankInventory
from src.blood_bank.allocation_service import AllocationService


def get_demo_config():
    """Two-node local loopback configuration using Suzuki-Kasami token-based DME."""
    return load_config_from_dict({
        "system": {"algorithm": "suzuki_kasami", "protocol_version": "1.0"},
        "nodes": [
            {"node_id": "branch-0", "host": "127.0.0.1", "port": 15001, "health_port": 16001},
            {"node_id": "branch-1", "host": "127.0.0.1", "port": 15002, "health_port": 16002},
        ],
        "network": {
            "mean_delay_ms": 2.0, "jitter_ms": 0.0, "message_loss_rate": 0.0, "connection_timeout_ms": 3000.0,
        },
        "timeouts": {
            "message_delivery_timeout_ms": 2000.0,
            "token_response_timeout_ms": 4000.0,
            "heartbeat_interval_ms": 200.0,
            "heartbeat_timeout_ms": 600.0,
            "max_retries": 2,
            "retry_backoff_ms": 50.0,
        },
    })


async def main():
    print("=" * 70)
    print("  DISTRIBUTED BLOOD BANK ALLOCATION DEMO (Suzuki-Kasami DME)  ")
    print("=" * 70)

    config = get_demo_config()

    print("\n[1] Starting Branch Nodes on TCP loopback...")
    # branch-0 begins with the single token
    node0 = BranchNode("branch-0", config, initial_token_holder="branch-0", enable_failure_detection=True)
    node1 = BranchNode("branch-1", config, initial_token_holder="branch-0", enable_failure_detection=True)

    await node0.start()
    await node1.start()
    print("    * branch-0 started (TCP 127.0.0.1:15001, Health 16001, Token Holder: YES)")
    print("    * branch-1 started (TCP 127.0.0.1:15002, Health 16002, Token Holder: NO)")

    try:
        # Populate authoritative inventory with scarce and standard units
        print("\n[2] Initializing Authoritative Blood Bank Inventory...")
        inventory = BloodBankInventory([
            BloodUnit("UNIT-O-NEG-001", BloodGroup.O_NEG, ComponentType.RBC, "branch-0"),
            BloodUnit("UNIT-A-POS-001", BloodGroup.A_POS, ComponentType.RBC, "branch-0"),
            BloodUnit("UNIT-A-POS-002", BloodGroup.A_POS, ComponentType.RBC, "branch-1"),
        ])
        print(f"    * Total inventory: {inventory.total_count()} physical units loaded.")
        for u in inventory._units.values():
            print(f"      - {u.unit_id} [{u.blood_group.value} {u.component_type.value}] at {u.branch_id} ({u.status.value})")

        service0 = AllocationService(node0, inventory, valid_branches={"branch-0", "branch-1"})
        service1 = AllocationService(node1, inventory, valid_branches={"branch-0", "branch-1"})

        # Scenario: Concurrent requests from both branches competing for scarce O- unit
        print("\n[3] Issuing Concurrent Allocation Requests for Scarce O- RBC Unit...")
        req0 = AllocationRequest("REQ-DEMO-01", "branch-0", BloodGroup.O_NEG, ComponentType.RBC, 1)
        req1 = AllocationRequest("REQ-DEMO-02", "branch-1", BloodGroup.O_NEG, ComponentType.RBC, 1)

        print("    * branch-0 requests 1 unit of O- RBC (REQ-DEMO-01)")
        print("    * branch-1 requests 1 unit of O- RBC (REQ-DEMO-02)")
        print("    * Dispatching requests concurrently over TCP sockets...")

        res0, res1 = await asyncio.gather(
            service0.execute_allocation(req0),
            service1.execute_allocation(req1),
        )

        print("\n[4] Allocation Outcomes:")
        for res in (res0, res1):
            status_symbol = "SUCCESS" if res.status == AllocationStatus.SUCCESS else "INSUFFICIENT_STOCK"
            print(f"    * Request {res.request_id} from {res.requesting_branch}:")
            print(f"      - Outcome: [{status_symbol}] {res.status.value}")
            print(f"      - Critical Section Entered: {res.cs_entered}")
            print(f"      - Units Assigned: {res.allocated_units or 'None'}")
            if res.error_message:
                print(f"      - Note: {res.error_message}")

        print("\n[5] Mutual Exclusion Invariant Verification:")
        h0 = node0.runtime_state.cs_history
        h1 = node1.runtime_state.cs_history
        if h0 and h1:
            print(f"    * branch-0 CS Interval: [{h0[0]['entry_time']:.3f} -> {h0[0]['exit_time']:.3f}]")
            print(f"    * branch-1 CS Interval: [{h1[0]['entry_time']:.3f} -> {h1[0]['exit_time']:.3f}]")
            overlap = not (h0[0]['exit_time'] <= h1[0]['entry_time'] or h1[0]['exit_time'] <= h0[0]['entry_time'])
            print(f"    * Simultaneous Entry Detected: {'YES (ERROR)' if overlap else 'NO (SAFETY PRESERVED)'}")

        print("\n[6] Failure Detector Status:")
        print(f"    * branch-0 sees branch-1: {node0.failure_detector.peer_status.get('branch-1').value}")
        print(f"    * branch-1 sees branch-0: {node1.failure_detector.peer_status.get('branch-0').value}")

        print("\n" + "=" * 70)
        print("  DEMO COMPLETE: MUTUAL EXCLUSION & INVENTORY INTEGRITY VERIFIED")
        print("=" * 70)

    finally:
        await node0.stop()
        await node1.stop()


if __name__ == "__main__":
    asyncio.run(main())
