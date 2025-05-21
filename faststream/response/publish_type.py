from enum import Enum


class PublishType(str, Enum):
    PUBLISH = "PUBLISH"
    """Regular `broker/publisher.publish(...)` call."""

    REPLY = "REPLY"
    """Response to RPC/Reply-To request."""

    REQUEST = "REQUEST"
    """RPC request call."""
