from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from typing_extensions import Self

from faststream._internal.parser import BatchCodecProto
from faststream.response import Response

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.parser import CodecProto


class Tombstone(bytes):
    """An explicit request to publish a keyed Kafka record with a null value."""

    __slots__ = ()

    def __new__(cls, value: bytes = b"") -> Self:
        if value:
            msg = "TOMBSTONE cannot carry a payload"
            raise ValueError(msg)
        return super().__new__(cls, b"")

    def __repr__(self) -> str:
        return "TOMBSTONE"

    __str__ = __repr__


TOMBSTONE = Tombstone()


def tombstone_to_none(body: bytes | None) -> bytes | None:
    return None if isinstance(body, Tombstone) else body


async def encode_or_tombstone(
    body: "SendableMessage",
    codec: "CodecProto",
    serializer: "SerializerProto | None",
    *,
    key: object,
    none_is_tombstone: bool,
) -> tuple[bytes | None, str | None]:
    if isinstance(body, Tombstone):
        if key is None:
            msg = "a Kafka tombstone requires a key"
            raise ValueError(msg)
        return TOMBSTONE, None
    if body is None and none_is_tombstone:
        return None, None
    return await codec.encode(body, serializer)


async def encode_kafka_batch(
    bodies: Sequence["SendableMessage | Response"],
    codec: "CodecProto",
    serializer: "SerializerProto | None",
    key_for: Callable[[int], object],
) -> tuple[Sequence[tuple[bytes | None, str | None]], bool]:
    # A response without a key can still wrap a message body.
    encoded_inputs = [
        body.body if isinstance(body, Response) else body for body in bodies
    ]
    has_tombstones = any(isinstance(body, Tombstone) for body in encoded_inputs)
    if isinstance(codec, BatchCodecProto):
        if has_tombstones:
            msg = "TOMBSTONE batches require per-record encoding, not BatchCodecProto"
            raise ValueError(msg)
        return await codec.encode_batch(encoded_inputs, serializer), False

    return [
        await encode_or_tombstone(
            body,
            codec,
            serializer,
            key=key_for(index),
            none_is_tombstone=False,
        )
        for index, body in enumerate(encoded_inputs)
    ], has_tombstones
