import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

MESSAGE_SCHEMA_VERSION = "1.0"

VALID_MESSAGE_TYPES = {
    "task.assign",
    "task.ack",
    "task.status_update",
    "task.completed",
    "task.failed",
    "monitor.heartbeat",
    "monitor.summary",
    "error.delivery_failure",
}

VALID_PRIORITIES = {"low", "medium", "high"}


class Message:
    def __init__(
        self,
        sender_id: str,
        receiver_id: str,
        message_type: str,
        payload: dict[str, Any],
        correlation_id: Optional[str] = None,
        message_id: Optional[str] = None,
        timestamp: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        priority: str = "medium",
    ):
        if message_type not in VALID_MESSAGE_TYPES:
            raise ValueError(
                f"Invalid message_type '{message_type}'. "
                f"Valid: {sorted(VALID_MESSAGE_TYPES)}"
            )
        if priority not in VALID_PRIORITIES:
            raise ValueError(
                f"Invalid priority '{priority}'. Valid: {sorted(VALID_PRIORITIES)}"
            )

        self.schema_version = MESSAGE_SCHEMA_VERSION
        self.message_id = message_id or str(uuid.uuid4())
        self.correlation_id = correlation_id or str(uuid.uuid4())
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.message_type = message_type
        self.timestamp = timestamp or datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        self.ttl_seconds = ttl_seconds
        self.priority = priority
        self.payload = payload

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "schema_version": self.schema_version,
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "message_type": self.message_type,
            "timestamp": self.timestamp,
            "priority": self.priority,
            "payload": self.payload,
        }
        if self.ttl_seconds is not None:
            d["ttl_seconds"] = self.ttl_seconds
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        return cls(
            sender_id=data["sender_id"],
            receiver_id=data["receiver_id"],
            message_type=data["message_type"],
            payload=data.get("payload", {}),
            correlation_id=data.get("correlation_id"),
            message_id=data.get("message_id"),
            timestamp=data.get("timestamp"),
            ttl_seconds=data.get("ttl_seconds"),
            priority=data.get("priority", "medium"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        return cls.from_dict(json.loads(json_str))

    def __repr__(self) -> str:
        return (
            f"Message(id={self.message_id[:8]}, "
            f"type={self.message_type}, "
            f"from={self.sender_id}, "
            f"to={self.receiver_id})"
        )
