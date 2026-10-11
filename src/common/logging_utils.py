"""Structured diagnostic logging utilities for distributed node events."""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import logging
from typing import Optional, Dict, Any


class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as JSON lines with standardized diagnostic fields."""

    def format(self, record: logging.LogRecord) -> str:
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        # Add custom structured fields if passed via extra
        for field in ("node_id", "algorithm", "event_type", "request_id", "message_id", "details"):
            val = getattr(record, field, None)
            if val is not None:
                data[field] = val
        return json.dumps(data)


class StructuredLogger:
    """Wrapper around python standard logging providing explicit structured event logging.

    Diagnostic logs are strictly observational and decoupled from authoritative
    protocol state.
    """

    def __init__(self, node_id: str, algorithm: str, logger_name: Optional[str] = None) -> None:
        self.node_id = node_id
        self.algorithm = algorithm
        self._logger = logging.getLogger(logger_name or f"node.{node_id}")

    def log_event(
        self,
        event_type: str,
        message: str = "",
        request_id: Optional[str] = None,
        message_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        level: int = logging.INFO,
    ) -> Dict[str, Any]:
        """Emit a structured diagnostic log event."""
        extra = {
            "node_id": self.node_id,
            "algorithm": self.algorithm,
            "event_type": event_type,
            "request_id": request_id,
            "message_id": message_id,
            "details": details or {},
        }
        self._logger.log(level, message or event_type, extra=extra)

        # Return dict representation for test inspection
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node_id": self.node_id,
            "algorithm": self.algorithm,
            "event_type": event_type,
            "request_id": request_id,
            "message_id": message_id,
            "details": details or {},
        }

    def info(self, msg: str, **kwargs: Any) -> None:
        self._logger.info(msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._logger.warning(msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self._logger.error(msg, **kwargs)


def get_structured_logger(
    node_id: str,
    algorithm: str,
    level: int = logging.INFO,
    json_output: bool = True,
) -> StructuredLogger:
    """Create and configure a StructuredLogger for a specific node."""
    logger_name = f"node.{node_id}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        if json_output:
            handler.setFormatter(StructuredJsonFormatter())
        else:
            handler.setFormatter(
                logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
            )
        logger.addHandler(handler)

    return StructuredLogger(node_id, algorithm, logger_name)
