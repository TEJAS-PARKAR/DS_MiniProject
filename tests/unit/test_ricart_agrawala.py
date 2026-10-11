"""Unit and multi-node harness tests for the Ricart-Agrawala protocol implementation."""
import pytest
from typing import Dict, List, Tuple
from collections import deque
from src.common.configuration import load_config_from_dict
from src.common.messages import ProtocolMessage, MessageType
from src.algorithms.ricart_agrawala import RicartAgrawala, NodeState
from src.simulation.safety import SafetyMonitor


def make_test_config(node_ids: List[str]):
    return load_config_from_dict({
        "system": {"algorithm": "ricart_agrawala", "protocol_version": "1.0"},
        "nodes": [{"node_id": nid, "host": "127.0.0.1", "port": 9000 + i} for i, nid in enumerate(node_ids)],
    })


# 1. Initialization and idle state
def test_initialization_and_idle_state():
    config = make_test_config(["n1", "n2", "n3"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2", "n3"])

    diag = ra.get_diagnostics()
    assert diag["node_id"] == "n1"
    assert diag["state"] == NodeState.IDLE.value
    assert diag["clock"] == 0
    assert diag["peers"] == ["n2", "n3"]
    assert diag["current_request_id"] is None
    assert diag["replies_count"] == 0
    assert not ra.can_enter_cs()


# 2. A request generates exactly N-1 REQUEST messages
def test_request_generates_n_minus_1_messages():
    config = make_test_config(["n1", "n2", "n3", "n4"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2", "n3", "n4"])

    out_msgs = ra.request_cs("req-1")
    assert len(out_msgs) == 3
    assert all(m.msg_type == MessageType.REQUEST for m in out_msgs)
    assert all(m.sender_id == "n1" for m in out_msgs)
    assert {m.destination_id for m in out_msgs} == {"n2", "n3", "n4"}
    assert all(m.request_id == "req-1" for m in out_msgs)
    assert all(m.logical_clock == 1 for m in out_msgs)


# 3. A node cannot enter before receiving all N-1 replies
def test_cannot_enter_before_all_replies():
    config = make_test_config(["n1", "n2", "n3"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2", "n3"])
    ra.request_cs("req-1")

    # Deliver reply from only 1 peer out of 2
    reply_msg = ProtocolMessage(
        msg_type=MessageType.REPLY,
        sender_id="n2",
        destination_id="n1",
        request_id="req-1",
        logical_clock=2,
    )
    ra.handle_message(reply_msg)

    assert not ra.can_enter_cs()
    with pytest.raises(RuntimeError, match="cannot enter critical section: permission not yet granted"):
        ra.on_cs_enter()


# 4. A node becomes eligible after all valid replies arrive
def test_eligible_after_all_valid_replies():
    config = make_test_config(["n1", "n2", "n3"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2", "n3"])
    ra.request_cs("req-1")

    ra.handle_message(ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="n2", destination_id="n1", request_id="req-1", logical_clock=2
    ))
    ra.handle_message(ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="n3", destination_id="n1", request_id="req-1", logical_clock=2
    ))

    assert ra.can_enter_cs()
    ra.on_cs_enter()
    assert ra.get_diagnostics()["state"] == NodeState.EXECUTING.value


# 5. Simultaneous requests are resolved by timestamp and node-ID ordering
def test_simultaneous_request_priority_tie_breaking():
    config = make_test_config(["n1", "n2"])
    ra1 = RicartAgrawala("n1", config)
    ra1.initialize(["n1", "n2"])
    ra2 = RicartAgrawala("n2", config)
    ra2.initialize(["n1", "n2"])

    # Both request with same logical clock = 1
    reqs1 = ra1.request_cs("req-n1")
    reqs2 = ra2.request_cs("req-n2")

    # ra1 has higher priority than ra2 because ("n1" < "n2") with same timestamp 1
    # When ra1 receives ra2's request: ra1 defers replying to ra2
    res1 = ra1.handle_message(reqs2[0])
    assert len(res1) == 0  # Deferred
    assert "n2" in ra1.get_diagnostics()["deferred_requests"]

    # When ra2 receives ra1's request: ra1 has priority, so ra2 grants immediate reply
    res2 = ra2.handle_message(reqs1[0])
    assert len(res2) == 1
    assert res2[0].msg_type == MessageType.REPLY
    assert res2[0].destination_id == "n1"


# 6. An executing node defers incoming requests
def test_executing_node_defers_requests():
    config = make_test_config(["n1", "n2"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2"])
    ra.request_cs("req-1")
    ra.handle_message(ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="n2", destination_id="n1", request_id="req-1", logical_clock=2
    ))
    ra.on_cs_enter()

    # While executing, incoming request from n2 arrives with any timestamp
    incoming_req = ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n2", destination_id="n1", request_id="req-n2", logical_clock=0
    )
    responses = ra.handle_message(incoming_req)
    assert len(responses) == 0
    assert "n2" in ra.get_diagnostics()["deferred_requests"]


# 7. A requesting node defers replies when its own request has priority
def test_requesting_node_defers_when_own_request_has_priority():
    config = make_test_config(["n1", "n2"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2"])
    ra.request_cs("req-1")  # local timestamp = 1

    # Incoming request from n2 with timestamp = 5 (lower priority)
    incoming_req = ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n2", destination_id="n1", request_id="req-n2", logical_clock=5
    )
    res = ra.handle_message(incoming_req)
    assert len(res) == 0
    assert "n2" in ra.get_diagnostics()["deferred_requests"]


# 8. Deferred replies are generated upon release
def test_deferred_replies_generated_upon_release():
    config = make_test_config(["n1", "n2", "n3"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2", "n3"])
    ra.request_cs("req-1")
    ra.handle_message(ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="n2", destination_id="n1", request_id="req-1", logical_clock=2
    ))
    ra.handle_message(ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="n3", destination_id="n1", request_id="req-1", logical_clock=2
    ))
    ra.on_cs_enter()

    # Requests arrive while n1 is in CS
    ra.handle_message(ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n2", destination_id="n1", request_id="req-n2", logical_clock=5
    ))
    ra.handle_message(ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n3", destination_id="n1", request_id="req-n3", logical_clock=6
    ))

    # Release CS
    release_msgs = ra.release_cs()
    assert len(release_msgs) == 2
    assert all(m.msg_type == MessageType.REPLY for m in release_msgs)
    destinations = {m.destination_id for m in release_msgs}
    assert destinations == {"n2", "n3"}
    assert ra.get_diagnostics()["deferred_count"] == 0
    assert ra.get_diagnostics()["state"] == NodeState.IDLE.value


# 9. Duplicate replies do not increase the reply count
def test_duplicate_replies_deduplicated():
    config = make_test_config(["n1", "n2", "n3"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2", "n3"])
    ra.request_cs("req-1")

    reply = ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="n2", destination_id="n1", request_id="req-1", logical_clock=2
    )
    ra.handle_message(reply)
    ra.handle_message(reply)  # Duplicate delivery
    ra.handle_message(reply)  # Triplicate delivery

    diag = ra.get_diagnostics()
    assert diag["replies_count"] == 1
    assert diag["replies_received"] == ["n2"]
    assert not ra.can_enter_cs()


# 10. Replies from unknown nodes are rejected
def test_replies_from_unknown_nodes_rejected():
    config = make_test_config(["n1", "n2"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2"])
    ra.request_cs("req-1")

    bogus_reply = ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="intruder", destination_id="n1", request_id="req-1", logical_clock=2
    )
    res = ra.handle_message(bogus_reply)
    assert len(res) == 0
    assert ra.get_diagnostics()["replies_count"] == 0


# 11. Stale replies do not complete a newer request
def test_stale_replies_ignored():
    config = make_test_config(["n1", "n2"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2"])
    ra.request_cs("req-current")

    # Reply arrives with old request ID
    stale_reply = ProtocolMessage(
        msg_type=MessageType.REPLY, sender_id="n2", destination_id="n1", request_id="req-stale-old", logical_clock=2
    )
    ra.handle_message(stale_reply)
    assert ra.get_diagnostics()["replies_count"] == 0
    assert not ra.can_enter_cs()


# 12. Duplicate requests do not create duplicate logical work
def test_duplicate_request_rejection():
    config = make_test_config(["n1", "n2"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2"])
    ra.request_cs("req-1")

    # Cannot request while already requesting
    with pytest.raises(RuntimeError, match="Cannot request critical section"):
        ra.request_cs("req-1")

    # Invalid empty request id
    with pytest.raises(ValueError, match="request_id must be a non-empty string"):
        ra.request_cs("   ")


# 13. Invalid state transitions are rejected safely
def test_invalid_state_transitions():
    config = make_test_config(["n1", "n2"])
    ra = RicartAgrawala("n1", config)
    ra.initialize(["n1", "n2"])

    # Cannot enter CS while IDLE
    with pytest.raises(RuntimeError, match="permission not yet granted"):
        ra.on_cs_enter()

    # Cannot release CS while IDLE
    with pytest.raises(RuntimeError, match="cannot release critical section"):
        ra.release_cs()


# 14 & 15. In-memory multi-node test harness producing no overlapping critical-section entries
class InMemoryNetworkHarness:
    """Delivers ProtocolMessage instances between RicartAgrawala instances."""

    def __init__(self, node_ids: List[str]):
        config = make_test_config(node_ids)
        self.nodes: Dict[str, RicartAgrawala] = {
            nid: RicartAgrawala(nid, config) for nid in node_ids
        }
        for nid, node in self.nodes.items():
            node.initialize(node_ids)

        self.message_queue: deque[ProtocolMessage] = deque()
        self.safety = SafetyMonitor()
        self.timeline: List[Tuple[float, str, str]] = []  # (time, node_id, event)
        self.sim_clock = 0.0

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


def test_multinode_harness_mutual_exclusion():
    node_ids = ["node-A", "node-B", "node-C", "node-D"]
    harness = InMemoryNetworkHarness(node_ids)

    # All 4 nodes request CS simultaneously
    for i, nid in enumerate(node_ids):
        msgs = harness.nodes[nid].request_cs(f"req-{nid}")
        harness.broadcast(msgs)

    # Process all network messages to allow priority resolution
    harness.run_until_idle()

    # Verify that exactly one node can enter first (node-A has lexicographical priority)
    eligible = [nid for nid in node_ids if harness.nodes[nid].can_enter_cs()]
    assert len(eligible) == 1
    first_node = eligible[0]
    assert first_node == "node-A"

    # Simulate sequential execution and exit
    order_of_execution = []
    current_time = 1.0

    while True:
        ready = [nid for nid in node_ids if harness.nodes[nid].can_enter_cs()]
        if not ready:
            break
        active = ready[0]
        order_of_execution.append(active)

        # Enter CS
        assert harness.safety.enter(active, current_time)
        harness.nodes[active].on_cs_enter()

        current_time += 1.0

        # Exit CS
        assert harness.safety.exit(active, current_time)
        release_msgs = harness.nodes[active].release_cs()
        harness.broadcast(release_msgs)
        harness.run_until_idle()

    assert harness.safety.is_safe
    assert len(harness.safety.violations) == 0
    assert order_of_execution == ["node-A", "node-B", "node-C", "node-D"]


def test_reordered_message_delivery():
    """Verify protocol safety when network delivers messages out of order."""
    node_ids = ["n1", "n2", "n3"]
    harness = InMemoryNetworkHarness(node_ids)

    # n1 and n2 request
    msgs1 = harness.nodes["n1"].request_cs("req-n1")
    msgs2 = harness.nodes["n2"].request_cs("req-n2")

    # Invert message delivery order (n2's messages delivered before n1's)
    harness.broadcast(msgs2)
    harness.broadcast(msgs1)
    harness.run_until_idle()

    # n1 still has priority because its timestamp and node_id ("n1" < "n2") win
    eligible = [nid for nid in node_ids if harness.nodes[nid].can_enter_cs()]
    assert eligible == ["n1"]
