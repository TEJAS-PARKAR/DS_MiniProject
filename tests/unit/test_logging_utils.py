"""Unit tests for structured diagnostic logging utilities."""
import json
import logging
from src.common.logging_utils import (
    StructuredLogger,
    StructuredJsonFormatter,
    get_structured_logger,
)


def test_structured_log_event():
    logger = StructuredLogger(node_id="branch-0", algorithm="ricart_agrawala")
    record = logger.log_event(
        event_type="REQUEST_CS",
        message="Requesting critical section access",
        request_id="req-101",
        message_id="msg-202",
        details={"logical_clock": 4},
    )

    assert record["node_id"] == "branch-0"
    assert record["algorithm"] == "ricart_agrawala"
    assert record["event_type"] == "REQUEST_CS"
    assert record["request_id"] == "req-101"
    assert record["message_id"] == "msg-202"
    assert record["details"] == {"logical_clock": 4}
    assert "timestamp" in record


def test_structured_json_formatter():
    formatter = StructuredJsonFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    setattr(record, "node_id", "branch-1")
    setattr(record, "algorithm", "suzuki_kasami")
    setattr(record, "event_type", "TOKEN_RECEIVED")

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["level"] == "INFO"
    assert parsed["message"] == "Test message"
    assert parsed["node_id"] == "branch-1"
    assert parsed["algorithm"] == "suzuki_kasami"
    assert parsed["event_type"] == "TOKEN_RECEIVED"
    assert "timestamp" in parsed


def test_get_structured_logger():
    s_logger = get_structured_logger(node_id="branch-2", algorithm="raymond_tree")
    assert isinstance(s_logger, StructuredLogger)
    assert s_logger.node_id == "branch-2"
    assert s_logger.algorithm == "raymond_tree"
