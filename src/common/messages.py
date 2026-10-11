"""Wire protocol message schemas and serialization utilities."""
from dataclasses import dataclass, field, asdict
from enum import Enum
import json
import time
import uuid
from typing import Optional, Dict, Any

SUPPORTED_PROTOCOL_VERSIONS = {"1.0"}


class MessageType(str, Enum):
    """Enumeration of message types used in distributed mutual exclusion protocols.

    Protocol Mapping:
    - REQUEST: Ricart-Agrawala, Suzuki-Kasami (broadcast request), Raymond (tree request)
    - REPLY: Ricart-Agrawala (permission reply)
    - TOKEN_REQUEST: Explicit upstream token request (tree algorithms)
    - TOKEN_TRANSFER: Token payload handover (Suzuki-Kasami, Raymond, Rabhi)
    - RELEASE: Optional completion notification
    - HEARTBEAT: Health and diagnostic liveness probe
    - ACKNOWLEDGEMENT: Safe point-to-point delivery confirmation for retries
    """
    REQUEST = "REQUEST"
    REPLY = "REPLY"
    TOKEN_REQUEST = "TOKEN_REQUEST"
    TOKEN_TRANSFER = "TOKEN_TRANSFER"
    RELEASE = "RELEASE"
    HEARTBEAT = "HEARTBEAT"
    ACKNOWLEDGEMENT = "ACKNOWLEDGEMENT"


@dataclass
class ProtocolMessage:
    """Documented and versioned message structure for inter-node communication.

    Attributes:
        protocol_version: Protocol wire specification version (e.g. '1.0').
        message_id: Unique identifier for this message instance.
        sender_id: Identifier of the originating node.
        destination_id: Optional identifier of the intended recipient (None for broadcast).
        msg_type: Type of message from MessageType enum.
        request_id: Optional identifier of the critical section request.
        logical_clock: Optional Lamport logical timestamp or sequence counter.
        payload: Optional arbitrary dictionary payload (e.g. token queue, LN vector).
        timestamp: Creation wall-clock timestamp (seconds since UNIX epoch).
    """
    msg_type: MessageType
    sender_id: str
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    destination_id: Optional[str] = None
    request_id: Optional[str] = None
    logical_clock: Optional[int] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    protocol_version: str = "1.0"

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate all required fields and invariants."""
        if not self.protocol_version or self.protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
            raise ValueError(
                f"Unsupported protocol version: '{self.protocol_version}'. "
                f"Supported: {sorted(SUPPORTED_PROTOCOL_VERSIONS)}"
            )

        if not self.message_id or not isinstance(self.message_id, str) or not self.message_id.strip():
            raise ValueError("Message ID must be a non-empty string.")

        if not self.sender_id or not isinstance(self.sender_id, str) or not self.sender_id.strip():
            raise ValueError("Sender ID must be a non-empty string.")

        if self.destination_id is not None:
            if not isinstance(self.destination_id, str) or not self.destination_id.strip():
                raise ValueError("Destination ID, if provided, must be a non-empty string.")

        if isinstance(self.msg_type, str):
            try:
                self.msg_type = MessageType(self.msg_type)
            except ValueError:
                raise ValueError(
                    f"Unknown message type '{self.msg_type}'. "
                    f"Valid types: {[m.value for m in MessageType]}"
                )
        elif not isinstance(self.msg_type, MessageType):
            raise ValueError(f"Invalid message type instance: {self.msg_type}")

        if self.logical_clock is not None:
            if not isinstance(self.logical_clock, int) or self.logical_clock < 0:
                raise ValueError(f"Logical clock must be a non-negative integer, got {self.logical_clock}")

        if not isinstance(self.payload, dict):
            raise ValueError("Payload must be a dictionary.")

        if not isinstance(self.timestamp, (int, float)) or self.timestamp < 0:
            raise ValueError(f"Timestamp must be a non-negative number, got {self.timestamp}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "protocol_version": self.protocol_version,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "destination_id": self.destination_id,
            "msg_type": self.msg_type.value,
            "request_id": self.request_id,
            "logical_clock": self.logical_clock,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }

    def to_json(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProtocolMessage":
        """Construct and validate a ProtocolMessage from a dictionary."""
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict for message decoding, got {type(data).__name__}")

        required = {"protocol_version", "message_id", "sender_id", "msg_type"}
        missing = required - set(data.keys())
        if missing:
            raise ValueError(f"Missing required message fields: {sorted(missing)}")

        try:
            msg_type = MessageType(data["msg_type"])
        except (ValueError, KeyError) as e:
            raise ValueError(f"Invalid or unknown msg_type: '{data.get('msg_type')}'") from e

        return cls(
            protocol_version=data["protocol_version"],
            message_id=data["message_id"],
            sender_id=data["sender_id"],
            destination_id=data.get("destination_id"),
            msg_type=msg_type,
            request_id=data.get("request_id"),
            logical_clock=data.get("logical_clock"),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", time.time()),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "ProtocolMessage":
        """Deserialize and validate a ProtocolMessage from JSON string."""
        try:
            data = json.loads(json_str)
        except Exception as e:
            raise ValueError(f"Malformed JSON payload: {e}") from e
        return cls.from_dict(data)
