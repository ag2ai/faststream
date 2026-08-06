from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream._internal.parser import DefaultCodec

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream.response.response import PublishCommand


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
    async def build(
        cls,
        *,
        cmd: "PublishCommand",
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> "MessageFormat":
        codec_instance = codec or DefaultCodec()
        payload, content_type = await codec_instance.encode(cmd, serializer)

        headers_to_send = {
            "correlation_id": cmd.correlation_id or "",
        }

        if content_type:
            headers_to_send["content-type"] = content_type

        if cmd.reply_to:
            headers_to_send["reply_to"] = cmd.reply_to

        if cmd.headers is not None:
            headers_to_send.update(cmd.headers)

        return cls(
            data=payload,
            headers=headers_to_send,
        )

    @classmethod
    @abstractmethod
    async def encode(
        cls,
        *,
        cmd: "PublishCommand",
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> bytes:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def parse(cls, data: bytes) -> tuple[bytes, dict[str, Any]]:
        raise NotImplementedError
