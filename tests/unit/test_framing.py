"""Unit tests for message framing, partial reads, and framing error conditions."""
import pytest
from src.common.messages import ProtocolMessage, MessageType
from src.common.network import (
    encode_frame,
    MessageFramer,
    FrameSizeExceededError,
    MalformedFrameError,
    MAX_FRAME_SIZE,
)


def test_encode_and_decode_single_frame():
    msg = ProtocolMessage(
        msg_type=MessageType.REQUEST,
        sender_id="node-1",
        destination_id="node-2",
        request_id="req-100",
        logical_clock=5,
    )
    frame = encode_frame(msg)
    assert frame.endswith(b"\n")

    framer = MessageFramer()
    decoded = framer.feed_data(frame)
    assert len(decoded) == 1
    assert decoded[0].msg_type == MessageType.REQUEST
    assert decoded[0].sender_id == "node-1"
    assert decoded[0].request_id == "req-100"
    assert decoded[0].logical_clock == 5


def test_partial_reads_assembly():
    msg = ProtocolMessage(
        msg_type=MessageType.REPLY,
        sender_id="node-2",
        destination_id="node-1",
        request_id="req-100",
    )
    frame = encode_frame(msg)

    # Split frame into 3 partial chunks
    part1 = frame[:15]
    part2 = frame[15:35]
    part3 = frame[35:]

    framer = MessageFramer()
    assert framer.feed_data(part1) == []
    assert framer.feed_data(part2) == []
    final_msgs = framer.feed_data(part3)
    assert len(final_msgs) == 1
    assert final_msgs[0].msg_type == MessageType.REPLY
    assert final_msgs[0].sender_id == "node-2"


def test_multiple_frames_in_single_chunk():
    msg1 = ProtocolMessage(msg_type=MessageType.REQUEST, sender_id="n1", request_id="r1")
    msg2 = ProtocolMessage(msg_type=MessageType.REPLY, sender_id="n2", request_id="r2")
    msg3 = ProtocolMessage(msg_type=MessageType.TOKEN_TRANSFER, sender_id="n3")

    combined = encode_frame(msg1) + encode_frame(msg2) + encode_frame(msg3)

    framer = MessageFramer()
    msgs = framer.feed_data(combined)
    assert len(msgs) == 3
    assert msgs[0].msg_type == MessageType.REQUEST
    assert msgs[1].msg_type == MessageType.REPLY
    assert msgs[2].msg_type == MessageType.TOKEN_TRANSFER


def test_empty_frames_ignored():
    msg = ProtocolMessage(msg_type=MessageType.REQUEST, sender_id="n1")
    chunk = b"\n\n\r\n" + encode_frame(msg) + b"\n\n"

    framer = MessageFramer()
    msgs = framer.feed_data(chunk)
    assert len(msgs) == 1
    assert msgs[0].msg_type == MessageType.REQUEST


def test_oversized_frame_rejected():
    framer = MessageFramer(max_frame_size=100)
    huge_chunk = b"A" * 150  # No newline, exceeds 100 bytes
    with pytest.raises(FrameSizeExceededError, match="exceeded maximum"):
        framer.feed_data(huge_chunk)


def test_oversized_encoded_frame_rejected():
    # Attempting to encode a message larger than MAX_FRAME_SIZE
    huge_payload = {"data": "X" * (MAX_FRAME_SIZE + 10)}
    msg = ProtocolMessage(
        msg_type=MessageType.REQUEST,
        sender_id="n1",
        payload=huge_payload,
    )
    with pytest.raises(FrameSizeExceededError, match="exceeds maximum"):
        encode_frame(msg)


def test_malformed_json_frame_rejected():
    framer = MessageFramer()
    corrupt_frame = b"{\"not_valid_json\": 123 \n"
    with pytest.raises(MalformedFrameError, match="Failed to parse ProtocolMessage"):
        framer.feed_data(corrupt_frame)
