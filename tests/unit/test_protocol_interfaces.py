"""Unit tests verifying protocol interface contracts and behavior."""
import pytest
from typing import List, Dict, Any
from src.common.protocol_interfaces import IMutualExclusionProtocol
from src.common.messages import ProtocolMessage, MessageType
from src.common.configuration import load_config_from_dict, AppConfig


def test_cannot_instantiate_abstract_protocol():
    raw_config = {
        "system": {"algorithm": "ricart_agrawala"},
        "nodes": [{"node_id": "branch-0", "host": "127.0.0.1", "port": 9001}],
    }
    config = load_config_from_dict(raw_config)

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        IMutualExclusionProtocol("branch-0", config)  # type: ignore


class MockProtocol(IMutualExclusionProtocol):
    """Mock implementation to verify protocol contract."""

    def __init__(self, node_id: str, config: AppConfig) -> None:
        super().__init__(node_id, config)
        self.peers: List[str] = []
        self.in_cs = False
        self.has_permission = False
        self.request_counter = 0

    def initialize(self, peers: List[str]) -> None:
        self.peers = list(peers)

    def request_cs(self, request_id: str) -> List[ProtocolMessage]:
        self.request_counter += 1
        # Generate non-blocking outgoing messages for all peers
        return [
            ProtocolMessage(
                msg_type=MessageType.REQUEST,
                sender_id=self.node_id,
                destination_id=peer,
                request_id=request_id,
                logical_clock=self.request_counter,
            )
            for peer in self.peers
        ]

    def handle_message(self, message: ProtocolMessage) -> List[ProtocolMessage]:
        if message.msg_type == MessageType.REQUEST:
            return [
                ProtocolMessage(
                    msg_type=MessageType.REPLY,
                    sender_id=self.node_id,
                    destination_id=message.sender_id,
                    request_id=message.request_id,
                )
            ]
        elif message.msg_type == MessageType.REPLY:
            self.has_permission = True
            return []
        return []

    def can_enter_cs(self) -> bool:
        return self.has_permission and not self.in_cs

    def on_cs_enter(self) -> None:
        self.in_cs = True

    def release_cs(self) -> List[ProtocolMessage]:
        self.in_cs = False
        self.has_permission = False
        return [
            ProtocolMessage(
                msg_type=MessageType.RELEASE,
                sender_id=self.node_id,
                destination_id=None,
            )
        ]

    def get_diagnostics(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "in_cs": self.in_cs,
            "has_permission": self.has_permission,
            "peers_count": len(self.peers),
        }


def test_mock_protocol_contract_execution():
    raw_config = {
        "system": {"algorithm": "ricart_agrawala"},
        "nodes": [
            {"node_id": "branch-0", "host": "127.0.0.1", "port": 9001},
            {"node_id": "branch-1", "host": "127.0.0.1", "port": 9002},
        ],
    }
    config = load_config_from_dict(raw_config)
    proto = MockProtocol("branch-0", config)
    proto.initialize(["branch-1"])

    assert not proto.can_enter_cs()

    # Step 1: Request CS -> emits non-blocking messages
    out_msgs = proto.request_cs("req-1")
    assert len(out_msgs) == 1
    assert out_msgs[0].msg_type == MessageType.REQUEST
    assert out_msgs[0].destination_id == "branch-1"

    # Step 2: Handle incoming reply
    reply_msg = ProtocolMessage(
        msg_type=MessageType.REPLY,
        sender_id="branch-1",
        destination_id="branch-0",
        request_id="req-1",
    )
    responses = proto.handle_message(reply_msg)
    assert len(responses) == 0
    assert proto.can_enter_cs()

    # Step 3: Enter CS
    proto.on_cs_enter()
    assert not proto.can_enter_cs()  # Already in CS

    # Step 4: Diagnostics
    diag = proto.get_diagnostics()
    assert diag["in_cs"] is True
    assert diag["has_permission"] is True

    # Step 5: Release CS
    rel_msgs = proto.release_cs()
    assert len(rel_msgs) == 1
    assert rel_msgs[0].msg_type == MessageType.RELEASE
    assert proto.get_diagnostics()["in_cs"] is False
