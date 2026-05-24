from abc import abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from faststream.message.utils import decode_message, encode_message

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import DecodedMessage, SendableMessage
    from faststream.message import StreamMessage

MsgType = TypeVar("MsgType")


class ParserProto(Protocol[MsgType]):
    """Protocol for parsing raw messages into StreamMessage."""

    @abstractmethod
    async def parse_message(self, message: MsgType) -> "StreamMessage[MsgType]":
        """Parse a raw message into a StreamMessage."""
        ...


class DecoderProto(Protocol):
    """Protocol for decoding StreamMessage into DecodedMessage."""

    @abstractmethod
    async def decode_message(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        """Decode a StreamMessage into a DecodedMessage."""
        ...


class BatchParserProto(Protocol[MsgType]):
    """Protocol for parsing a batch of raw messages into StreamMessage."""

    @abstractmethod
    async def parse_batch(
        self, messages: Sequence[MsgType]
    ) -> "StreamMessage[Sequence[MsgType]]":
        """Parse a batch of raw messages into a StreamMessage."""
        ...


class BatchDecoderProto(Protocol[MsgType]):
    """Protocol for decoding a batch of StreamMessage into list of DecodedMessage."""

    @abstractmethod
    async def decode_batch(
        self, msg: "StreamMessage[Sequence[MsgType]]"
    ) -> list["DecodedMessage"]:
        """Decode a batch of StreamMessage into a list of DecodedMessage."""
        ...


class CodecProto(Protocol):
    """Protocol for encoding and decoding message bodies."""

    @abstractmethod
    async def decode(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        """Decode a StreamMessage body into a Python object."""
        ...

    @abstractmethod
    async def encode(
        self,
        msg: "SendableMessage",
        serializer: "SerializerProto | None" = None,
    ) -> tuple[bytes, str | None]:
        """Encode a Python object into bytes and content_type."""
        ...


class DefaultCodec:
    """Default codec that delegates to the shared encode_message/decode_message functions."""

    async def decode(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        return decode_message(msg)

    async def encode(
        self,
        msg: "SendableMessage",
        serializer: "SerializerProto | None" = None,
    ) -> tuple[bytes, str | None]:
        return encode_message(msg, serializer)
