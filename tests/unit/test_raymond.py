"""Unit and multi-node harness tests for Raymond's tree-based protocol implementation."""
import pytest
from typing import Dict, List, Tuple
from collections import deque
from src.common.configuration import load_config_from_dict
from src.common.messages import ProtocolMessage, MessageType
from src.algorithms.raymond_tree import Raymond, validate_tree_topology
from src.algorithms.ricart_agrawala import NodeState
from src.simulation.safety import SafetyMonitor


def make_test_config(node_ids: List[str]):
    return load_config_from_dict({
        "system": {"algorithm": "raymond_tree", "protocol_version": "1.0"},
        "nodes": [{"node_id": nid, "host": "127.0.0.1", "port": 9000 + i} for i, nid in enumerate(node_ids)],
    })


# 1 & 14. Tree topology validation & rejection of cyclic or disconnected graphs
def test_raymond_tree_topology_validation():
    # Valid star tree
    nodes = ["n0", "n1", "n2", "n3"]
    edges = [("n0", "n1"), ("n0", "n2"), ("n0", "n3")]
    adj = validate_tree_topology(nodes, edges)
    assert len(adj["n0"]) == 3

    # Reject cycle
    cycle_edges = [("n0", "n1"), ("n1", "n2"), ("n2", "n0")]
    with pytest.raises(ValueError, match="Tree with 3 nodes must have exactly 2 edges|Cycle detected"):
        validate_tree_topology(["n0", "n1", "n2"], cycle_edges)

    # Reject disconnected graph
    disc_edges = [("n0", "n1"), ("n2", "n3")]
    with pytest.raises(ValueError, match="Tree with 4 nodes must have exactly 3 edges|Disconnected"):
        validate_tree_topology(["n0", "n1", "n2", "n3"], disc_edges)

    # Reject edge with unknown node
    with pytest.raises(ValueError, match="references node not in configured node set"):
        validate_tree_topology(["n0", "n1"], [("n0", "unknown")])


# 2 & 3. Initialization with unique token owner and correct holder pointers
def test_raymond_initialization_and_holder_pointers():
    node_ids = ["n0", "n1", "n2"]
    # Linear tree: n0 - n1 - n2; token at n0
    edges = [("n0", "n1"), ("n1", "n2")]
    config = make_test_config(node_ids)

    r0 = Raymond("n0", config)
    r0.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    r1 = Raymond("n1", config)
    r1.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    r2 = Raymond("n2", config)
    r2.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    # Invariants
    assert r0.has_token is True
    assert r0.holder == "n0"

    assert r1.has_token is False
    assert r1.holder == "n0"  # n1's parent towards root n0 is n0

    assert r2.has_token is False
    assert r2.holder == "n1"  # n2's parent towards root n0 is n1


# 4 & 5. Request forwarding toward token holder with duplicate suppression
def test_raymond_request_forwarding_and_duplicate_suppression():
    node_ids = ["n0", "n1", "n2"]
    edges = [("n0", "n1"), ("n1", "n2")]
    config = make_test_config(node_ids)

    r2 = Raymond("n2", config)
    r2.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    # n2 requests CS -> should send REQUEST towards its holder n1
    out_msgs = r2.request_cs("req-n2")
    assert len(out_msgs) == 1
    assert out_msgs[0].msg_type == MessageType.REQUEST
    assert out_msgs[0].destination_id == "n1"
    assert out_msgs[0].sender_id == "n2"
    assert r2.asked is True
    assert list(r2.request_queue) == ["n2"]

    # When neighbor sends a duplicate request, it should not duplicate entry in queue
    r1 = Raymond("n1", config)
    r1.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    req_msg = out_msgs[0]
    out1 = r1.handle_message(req_msg)
    assert len(out1) == 1  # Forwarded toward n0
    assert out1[0].destination_id == "n0"
    assert list(r1.request_queue) == ["n2"]

    # Second delivery of duplicate request
    out2 = r1.handle_message(req_msg)
    assert len(out2) == 0  # Duplicate suppressed
    assert list(r1.request_queue) == ["n2"]


# 6, 7, 8. Token transfer, holder update, and privilege assignment
def test_raymond_token_transfer_and_holder_updates():
    node_ids = ["n0", "n1"]
    edges = [("n0", "n1")]
    config = make_test_config(node_ids)

    r0 = Raymond("n0", config)
    r0.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    r1 = Raymond("n1", config)
    r1.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    # n1 requests CS
    req_msgs = r1.request_cs("req-n1")

    # n0 receives request from n1 while IDLE
    res0 = r0.handle_message(req_msgs[0])
    assert len(res0) == 1
    token_msg = res0[0]
    assert token_msg.msg_type == MessageType.TOKEN_TRANSFER
    assert token_msg.destination_id == "n1"

    # n0 transferred token: holder updated to n1
    assert r0.has_token is False
    assert r0.holder == "n1"

    # n1 receives TOKEN_TRANSFER
    r1.handle_message(token_msg)
    assert r1.has_token is True
    assert r1.holder == "n1"
    assert r1.can_enter_cs()


# 9. Release CS and continued service of remaining requests
def test_raymond_release_and_continued_service():
    node_ids = ["n0", "n1"]
    edges = [("n0", "n1")]
    config = make_test_config(node_ids)

    r0 = Raymond("n0", config)
    r0.initialize(node_ids, tree_edges=edges, initial_token_holder="n0")

    # n0 enters CS locally
    r0.request_cs("req-n0")
    r0.on_cs_enter()

    # n1 requests while n0 is executing
    r0.handle_message(ProtocolMessage(
        msg_type=MessageType.REQUEST, sender_id="n1", destination_id="n0", request_id="req-n1"
    ))
    assert list(r0.request_queue) == ["n1"]

    # n0 releases CS
    outbound = r0.release_cs()
    assert len(outbound) == 1
    assert outbound[0].msg_type == MessageType.TOKEN_TRANSFER
    assert outbound[0].destination_id == "n1"
    assert r0.holder == "n1"
    assert r0.has_token is False


# 10, 11, 12, 13. Multi-node harness across different branches of tree
class RaymondNetworkHarness:
    """Delivers ProtocolMessage instances between Raymond tree nodes."""

    def __init__(self, node_ids: List[str], tree_edges: List[Tuple[str, str]], initial_token_holder: str = "n0"):
        config = make_test_config(node_ids)
        self.nodes: Dict[str, Raymond] = {
            nid: Raymond(nid, config) for nid in node_ids
        }
        for nid, node in self.nodes.items():
            node.initialize(node_ids, tree_edges=tree_edges, initial_token_holder=initial_token_holder)

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


def test_raymond_multinode_tree_concurrent_requests():
    # Tree:
    #         n0 (root / initial token)
    #        /  \
    #       n1   n2
    #      /
    #     n3
    node_ids = ["n0", "n1", "n2", "n3"]
    edges = [("n0", "n1"), ("n0", "n2"), ("n1", "n3")]
    harness = RaymondNetworkHarness(node_ids, tree_edges=edges, initial_token_holder="n0")

    # Check initial invariant: exactly one token owner
    owners = [nid for nid in node_ids if harness.nodes[nid].has_token]
    assert owners == ["n0"]

    # Simultaneous requests from n3 and n2 (different branches of tree)
    msgs3 = harness.nodes["n3"].request_cs("req-n3")
    msgs2 = harness.nodes["n2"].request_cs("req-n2")
    harness.broadcast(msgs3)
    harness.broadcast(msgs2)

    harness.run_until_idle()

    # Sequential execution and release
    served_order = []
    sim_time = 1.0

    while True:
        ready = [nid for nid in node_ids if harness.nodes[nid].can_enter_cs()]
        if not ready:
            break
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
    assert set(served_order) == {"n2", "n3"}
    # Token ended up at the last served node
    final_owners = [nid for nid in node_ids if harness.nodes[nid].has_token]
    assert len(final_owners) == 1
