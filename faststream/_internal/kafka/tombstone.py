"""Tombstones: Kafka records whose value is null.

A tombstone is the delete marker of a compacted topic, so it has to survive
the round trip as something other than an empty payload. `Tombstone` is an
empty `bytes` subclass: it compares equal to `b""` and is `bytes` everywhere
the framework expects bytes, and `isinstance(body, Tombstone)` is what tells
the two apart.

Note that any operation producing a new `bytes` from a tombstone - slicing,
concatenation, `bytes(...)`, `join(...)` - returns a plain `bytes`, so an
`isinstance` check belongs before that kind of work, not after it.
"""

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from typing_extensions import Self

from faststream._internal.parser import BatchCodecProto

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.parser import CodecProto


class Tombstone(bytes):
    """An empty `bytes` marking a record published with a null value."""

    __slots__ = ()

    def __new__(cls, value: bytes = b"") -> Self:
        """Build the marker, rejecting any attempt to give it a payload.

        `bytes.__getnewargs__` hands `b""` back on unpickle, hence the
        argument.
        """
        if value:
            msg = f"{cls.__name__} carries no data, got {value!r}"
            raise ValueError(msg)
        return super().__new__(cls, b"")

    def __repr__(self) -> str:
        return "TOMBSTONE"

    # NOTE: bytes doesn't route str()/f-strings through __repr__, so without
    # this a tombstone reads as b"" in every log line that doesn't use !r
    __str__ = __repr__


TOMBSTONE: Tombstone = Tombstone()


def value_or_tombstone(value: bytes | None) -> bytes:
    """Map a raw record value to a body, marking a null one."""
    return TOMBSTONE if value is None else value


async def encode_or_tombstone(
    message: "SendableMessage",
    codec: "CodecProto",
    serializer: "SerializerProto | None",
    *,
    key: bytes | str | None = None,
    none_is_tombstone: bool = False,
) -> tuple[bytes | None, str | None]:
    """Encode a body, or answer with a null value for a tombstone.

    An explicit tombstone requires a key, since compaction deletes per key.
    `none_is_tombstone` carries each broker's own legacy rule for a `None`
    body and never implies the key requirement.
    """
    if isinstance(message, Tombstone):
        if key is None:
            msg = "a Kafka tombstone requires a key"
            raise ValueError(msg)
        return None, None

    if none_is_tombstone and message is None:
        return None, None

    return await codec.encode(message, serializer)


async def encode_batch_or_tombstone(
    bodies: Sequence["SendableMessage"],
    codec: "CodecProto",
    serializer: "SerializerProto | None",
    key_for: Callable[[int], bytes | str | None],
) -> Sequence[tuple[bytes | None, str | None]]:
    """Encode a batch, keeping each element's own key for its tombstone check.

    A custom `BatchCodecProto` encodes the batch as a whole and has no way to
    express a null value for one record, so a tombstone cannot ride along.
    """
    if isinstance(codec, BatchCodecProto):
        if any(isinstance(body, Tombstone) for body in bodies):
            msg = "a tombstone in a batch isn't supported with a custom BatchCodecProto"
            raise ValueError(msg)
        return await codec.encode_batch(bodies, serializer)

    # NOTE: no `none_is_tombstone` here - a bare None in a batch encodes
    # normally, exactly as it did before tombstones existed
    return [
        await encode_or_tombstone(body, codec, serializer, key=key_for(position))
        for position, body in enumerate(bodies)
    ]
