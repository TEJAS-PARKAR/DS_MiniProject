"""Unit tests for ProtocolMessage serialization, deserialization, and schema validation."""
import pytest
import json
from src.common.messages import ProtocolMessage, MessageType, SUPPORTED_PROTOCOL_VERSIONS


def test_valid_message_creation():
    msg = ProtocolMessage(
        msg_type=MessageType.REQUEST,
        sender_id="branch-0",
        destination_id="branch-1",
        request_id="req-123",
        logical_clock=5,
        payload={"reason": "blood_allocation"},
    )
    assert msg.protocol_version == "1.0"
    assert msg.msg_type == MessageType.REQUEST
    assert msg.sender_id == "branch-0"
    assert msg.destination_id == "branch-1"
    assert msg.request_id == "req-123"
    assert msg.logical_clock == 5
    assert msg.payload == {"reason": "blood_allocation"}
    assert msg.timestamp > 0


def test_json_round_tripping():
    original = ProtocolMessage(
        msg_type=MessageType.TOKEN_TRANSFER,
        sender_id="branch-2",
        destination_id="branch-3",
        request_id="req-999",
        logical_clock=10,
        payload={"token": {"LN": [1, 2, 3], "queue": ["branch-1"]}},
    )
    json_str = original.to_json()
    decoded = ProtocolMessage.from_json(json_str)

    assert decoded.protocol_version == original.protocol_version
    assert decoded.message_id == original.message_id
    assert decoded.msg_type == original.msg_type
    assert decoded.sender_id == original.sender_id
    assert decoded.destination_id == original.destination_id
    assert decoded.request_id == original.request_id
    assert decoded.logical_clock == original.logical_clock
    assert decoded.payload == original.payload
    assert decoded.timestamp == pytest.approx(original.timestamp, abs=1e-3)


def test_missing_required_fields():
    data = {
        "protocol_version": "1.0",
        "sender_id": "branch-0",
        # missing message_id and msg_type
    }
    with pytest.raises(ValueError, match="Missing required message fields"):
        ProtocolMessage.from_dict(data)


def test_malformed_json():
    with pytest.raises(ValueError, match="Malformed JSON payload"):
        ProtocolMessage.from_json("{not-valid-json")


def test_unsupported_protocol_version():
    with pytest.raises(ValueError, match="Unsupported protocol version"):
        ProtocolMessage(
            msg_type=MessageType.REQUEST,
            sender_id="branch-0",
            protocol_version="2.0",
        )


def test_invalid_message_type():
    with pytest.raises(ValueError, match="Unknown message type"):
        ProtocolMessage(
            msg_type="INVALID_TYPE",  # type: ignore
            sender_id="branch-0",
        )


def test_invalid_sender_id():
    with pytest.raises(ValueError, match="Sender ID must be a non-empty string"):
        ProtocolMessage(
            msg_type=MessageType.REQUEST,
            sender_id="",
        )


def test_invalid_destination_id():
    with pytest.raises(ValueError, match="Destination ID, if provided, must be a non-empty string"):
        ProtocolMessage(
            msg_type=MessageType.REQUEST,
            sender_id="branch-0",
            destination_id="   ",
        )


def test_invalid_logical_clock():
    with pytest.raises(ValueError, match="Logical clock must be a non-negative integer"):
        ProtocolMessage(
            msg_type=MessageType.REQUEST,
            sender_id="branch-0",
            logical_clock=-1,
        )


def test_broadcast_message_destination_is_none():
    msg = ProtocolMessage(
        msg_type=MessageType.REQUEST,
        sender_id="branch-0",
        destination_id=None,
    )
    assert msg.destination_id is None
    d = msg.to_dict()
    assert d["destination_id"] is None
    decoded = ProtocolMessage.from_dict(d)
    assert decoded.destination_id is None
