"""Unit and multi-node harness tests for the Suzuki-Kasami protocol implementation."""
import pytest
from typing import Dict, List
from collections import deque
from src.common.configuration import load_config_from_dict
from src.common.messages import ProtocolMessage, MessageType
from src.algorithms.suzuki_kasami import SuzukiKasami
from src.algorithms.ricart_agrawala import NodeState
from src.simulation.safety import SafetyMonitor


def make_test_config(node_ids: List[str]):
    return load_config_from_dict({
        "system": {"algorithm": "suzuki_kasami", "protocol_version": "1.0"},
        "nodes": [{"node_id": nid, "host": "127.0.0.1", "port": 9000 + i} for i, nid in enumerate(node_ids)],
    })


# 1. Initialization with one unique token owner
def test_sk_initialization_unique_token_owner():
    config = make_test_config(["n0", "n1", "n2"])
    sk0 = SuzukiKasami("n0", config)
    sk0.initialize(["n0", "n1", "n2"], initial_token_holder="n0")

    sk1 = SuzukiKasami("n1", config)
    sk1.initialize(["n0", "n1", "n2"], initial_token_holder="n0")

    assert sk0.has_token is True
    assert sk0.token_ln == {"n0": 0, "n1": 0, "n2": 0}
    assert list(sk0.token_queue) == []
    assert sk0.state == NodeState.IDLE

    assert sk1.has_token is False
    assert sk1.token_ln is None
    assert sk1.token_queue is None
    assert sk1.state == NodeState.IDLE


# 2. A request from a node without the token produces broadcast messages
def test_sk_request_broadcast_from_non_holder():
    config = make_test_config(["n0", "n1", "n2"])
    sk1 = SuzukiKasami("n1", config)
    sk1.initialize(["n0", "n1", "n2"], initial_token_holder="n0")

    out_msgs = sk1.request_cs("req-1")
    assert len(out_msgs) == 2
    assert all(m.msg_type == MessageType.REQUEST for m in out_msgs)
    assert all(m.sender_id == "n1" for m in out_msgs)
    assert {m.destination_id for m in out_msgs} == {"n0", "n2"}
    assert all(m.logical_clock == 1 for m in out_msgs)
    assert sk1.RN["n1"] == 1


# 3. Current token owner can enter without broadcast
def test_sk_request_from_holder_authorizes_immediately():
    config = make_test_config(["n0", "n1", "n2"])
    sk0 = SuzukiKasami("n0", config)
    sk0.initialize(["n0", "n1", "n2"], initial_token_holder="n0")

    out_msgs = sk0.request_cs("req-0")
    assert len(out_msgs) == 0  # No broadcast needed when possessing token
    assert sk0.can_enter_cs()

    sk0.on_cs_enter()
    assert sk0.get_diagnostics()["state"] == NodeState.EXECUTING.value


# 4. Receiving a request updates RN correctly
def test_sk_receiving_request_updates_rn():
    config = make_test_config(["n0", "n1", "n2"])
    sk0 = SuzukiKasami("n0", config)
    sk0.initialize(["n0", "n1", "n2"], initial_token_holder="n0")
    sk0.request_cs("req-0")
    sk0.on_cs_enter()

    # n1 sends REQUEST with seq=5 while n0 is executing
    req = ProtocolMessage(
        msg_type=MessageType.REQUEST,
        sender_id="n1",
        destination_id="n0",
        request_id="req-1",
        logical_clock=5,
    )
    res = sk0.handle_message(req)
    assert len(res) == 0  # Does not transfer while executing
    assert sk0.RN["n1"] == 5


# 5. Token is transferred to an eligible requester
def test_sk_token_transfer_to_eligible_requester():
    config = make_test_config(["n0", "n1"])
    sk0 = SuzukiKasami("n0", config)
    sk0.initialize(["n0", "n1"], initial_token_holder="n0")

    # n1 requests while n0 is IDLE
    req = ProtocolMessage(
        msg_type=MessageType.REQUEST,
        sender_id="n1",
        destination_id="n0",
        request_id="req-1",
        logical_clock=1,
    )
    res = sk0.handle_message(req)
    assert len(res) == 1
    token_msg = res[0]
    assert token_msg.msg_type == MessageType.TOKEN_TRANSFER
    assert token_msg.destination_id == "n1"
    assert token_msg.sender_id == "n0"
    assert sk0.has_token is False


# 6. LN is updated correctly when leaving the critical section
def test_sk_ln_updated_upon_release():
    config = make_test_config(["n0", "n1"])
    sk0 = SuzukiKasami("n0", config)
    sk0.initialize(["n0", "n1"], initial_token_holder="n0")

    sk0.request_cs("req-0")
    sk0.on_cs_enter()
    assert sk0.token_ln["n0"] == 0

    sk0.release_cs()
    assert sk0.token_ln["n0"] == 0  # RN[n0] was 0 because holder requested without incrementing


# 7. Pending requests enqueued without duplicates and transferred upon release
def test_sk_pending_requests_enqueued_and_transferred():
    config = make_test_config(["n0", "n1", "n2"])
    sk0 = SuzukiKasami("n0", config)
    sk0.initialize(["n0", "n1", "n2"], initial_token_holder="n0")
    sk0.request_cs("req-0")
    sk0.on_cs_enter()

    # n1 and n2 send requests while n0 is in CS
    sk0.handle_message(ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n1", destination_id="n0", request_id="r1", logical_clock=1
    ))
    sk0.handle_message(ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n2", destination_id="n0", request_id="r2", logical_clock=1
    ))

    # Release CS -> enqueues n1 and n2 and transfers token to head (n1)
    out_msgs = sk0.release_cs()
    assert len(out_msgs) == 1
    assert out_msgs[0].msg_type == MessageType.TOKEN_TRANSFER
    assert out_msgs[0].destination_id == "n1"
    # Remaining in token queue should be n2
    assert out_msgs[0].payload["queue"] == ["n2"]
    assert sk0.has_token is False


# 8. Duplicate REQUEST messages do not create duplicate logical work
def test_sk_duplicate_request_handling():
    config = make_test_config(["n0", "n1"])
    sk0 = SuzukiKasami("n0", config)
    sk0.initialize(["n0", "n1"], initial_token_holder="n0")
    sk0.request_cs("req-0")
    sk0.on_cs_enter()

    req = ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n1", destination_id="n0", request_id="r1", logical_clock=1
    )
    sk0.handle_message(req)
    sk0.handle_message(req)  # Duplicate message

    assert sk0.RN["n1"] == 1
    out_msgs = sk0.release_cs()
    assert len(out_msgs) == 1
    assert out_msgs[0].payload["queue"] == []  # n1 popped, no duplicate n1 remains


# 9, 10, 11, 12, 13. Multi-node harness test for concurrent requests, safety, and invariants
class SKNetworkHarness:
    """Delivers ProtocolMessage instances between SuzukiKasami nodes."""

    def __init__(self, node_ids: List[str], initial_token_holder: str = "n0"):
        config = make_test_config(node_ids)
        self.nodes: Dict[str, SuzukiKasami] = {
            nid: SuzukiKasami(nid, config) for nid in node_ids
        }
        for nid, node in self.nodes.items():
            node.initialize(node_ids, initial_token_holder=initial_token_holder)

        self.message_queue: deque[ProtocolMessage] = deque()
        self.safety = SafetyMonitor()

    def broadcast(self, messages: List[ProtocolMessage]):
        for msg in messages:
            self.message_queue.append(msg)

    def run_until_idle(self):
        while self.message_queue:
            msg = self.message_queue.popleft()
            dest = msg.destination_id
            if dest and dest in self.nodes:
                responses = self.nodes[dest].handle_message(msg)
                self.broadcast(responses)


def test_sk_multinode_concurrent_requests_and_safety():
    node_ids = ["n0", "n1", "n2", "n3"]
    harness = SKNetworkHarness(node_ids, initial_token_holder="n0")

    # Verify initial invariant: exactly one token owner
    owners = [nid for nid in node_ids if harness.nodes[nid].has_token]
    assert owners == ["n0"]

    # All nodes request CS
    for nid in node_ids:
        msgs = harness.nodes[nid].request_cs(f"req-{nid}")
        harness.broadcast(msgs)

    harness.run_until_idle()

    served_order = []
    sim_time = 1.0

    while True:
        ready = [nid for nid in node_ids if harness.nodes[nid].can_enter_cs()]
        if not ready:
            break
        # Exactly one node must be ready to enter at any given time
        assert len(ready) == 1
        active = ready[0]
        served_order.append(active)

        # Enter CS
        assert harness.safety.enter(active, sim_time)
        harness.nodes[active].on_cs_enter()

        # Invariant: exactly one token owner
        owners = [nid for nid in node_ids if harness.nodes[nid].has_token]
        assert owners == [active]

        sim_time += 1.0

        # Exit CS
        assert harness.safety.exit(active, sim_time)
        rel_msgs = harness.nodes[active].release_cs()
        harness.broadcast(rel_msgs)
        harness.run_until_idle()

    assert harness.safety.is_safe
    assert len(harness.safety.violations) == 0
    assert len(served_order) == 4
    # All 4 nodes were served
    assert set(served_order) == set(node_ids)

    # Invariant: exactly one token owner remains at the end
    final_owners = [nid for nid in node_ids if harness.nodes[nid].has_token]
    assert len(final_owners) == 1


def test_sk_repeated_requests_and_token_invariants():
    node_ids = ["n0", "n1", "n2"]
    harness = SKNetworkHarness(node_ids, initial_token_holder="n0")

    sim_time = 1.0
    for round_no in range(3):
        for nid in node_ids:
            msgs = harness.nodes[nid].request_cs(f"req-{nid}-{round_no}")
            harness.broadcast(msgs)
            harness.run_until_idle()

            if harness.nodes[nid].can_enter_cs():
                assert harness.safety.enter(nid, sim_time)
                harness.nodes[nid].on_cs_enter()
                sim_time += 0.5
                assert harness.safety.exit(nid, sim_time)
                rel_msgs = harness.nodes[nid].release_cs()
                harness.broadcast(rel_msgs)
                harness.run_until_idle()

    # Flush any remaining pending requests
    while True:
        ready = [nid for nid in node_ids if harness.nodes[nid].can_enter_cs()]
        if not ready:
            break
        active = ready[0]
        assert harness.safety.enter(active, sim_time)
        harness.nodes[active].on_cs_enter()
        sim_time += 0.5
        assert harness.safety.exit(active, sim_time)
        rel_msgs = harness.nodes[active].release_cs()
        harness.broadcast(rel_msgs)
        harness.run_until_idle()

    assert harness.safety.is_safe
    final_owners = [nid for nid in node_ids if harness.nodes[nid].has_token]
    assert len(final_owners) == 1
