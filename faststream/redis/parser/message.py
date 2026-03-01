from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream.message import encode_message

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage


class MessageFormat(ABC):
    """A class to represent a raw Redis message."""

    __slots__ = (
        "data",
        "headers",
    )

    def __init__(
        self,
        data: bytes,
        headers: dict[str, Any] | None = None,
    ) -> None:
        self.data = data
        self.headers = headers or {}

    @classmethod
    def build(
        cls,
        *,
        message: Union[Sequence["SendableMessage"], "SendableMessage"],
        reply_to: str | None,
        headers: dict[str, Any] | None,
        correlation_id: str,
        serializer: Optional["SerializerProto"] = None,
    ) -> "MessageFormat":
        payload, content_type = encode_message(message, serializer=serializer)

        headers_to_send = {
            "correlation_id": correlation_id,
        }

        if content_type:
            headers_to_send["content-type"] = content_type

        if reply_to:
            headers_to_send["reply_to"] = reply_to

        if headers is not None:
            headers_to_send.update(headers)

        return cls(
            data=payload,
            headers=headers_to_send,
        )

    @classmethod
    @abstractmethod
    def encode(
        cls,
        *,
        message: Union[Sequence["SendableMessage"], "SendableMessage"],
        reply_to: str | None,
        headers: dict[str, Any] | None,
        correlation_id: str,
        serializer: Optional["SerializerProto"] = None,
    ) -> bytes:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def parse(cls, data: bytes) -> tuple[bytes, dict[str, Any]]:
        raise NotImplementedError
