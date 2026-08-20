import contextlib
import json
import warnings
from collections.abc import Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from uuid import uuid4

from faststream._internal._compat import json_dumps, json_loads
from faststream._internal.constants import TOMBSTONE, ContentTypes, Tombstone

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import DecodedMessage, SendableMessage
    from faststream._internal.parser import CodecProto

    from .message import StreamMessage


def gen_cor_id() -> str:
    """Generate random string to use as ID."""
    return str(uuid4())


def value_or_tombstone(value: bytes | None) -> "bytes | Tombstone":
    return TOMBSTONE if value is None else value


async def encode_or_tombstone(
    message: "SendableMessage | Tombstone",
    codec: "CodecProto",
    serializer: Optional["SerializerProto"],
    *,
    key: "bytes | str | None" = None,
    none_is_tombstone: bool = False,
) -> tuple[bytes | None, str | None]:
    if isinstance(message, Tombstone):
        if key is None:
            msg = "a Kafka tombstone requires a key"
            raise ValueError(msg)
        return None, None

    if none_is_tombstone and message is None:
        warnings.warn(
            "Publishing `None` as a tombstone is deprecated, use "
            "`faststream.message.TOMBSTONE` instead. `None` will be encoded "
            "normally in 0.8.",
            DeprecationWarning,
            stacklevel=4,
        )
        return None, None

    return await codec.encode(message, serializer)


def decode_message(message: "StreamMessage[Any]") -> "DecodedMessage":
    """Decodes a message."""
    body: Any = getattr(message, "body", message)

    # NOTE: a tombstone carries no payload, so any content-type on it is a lie
    if isinstance(body, Tombstone):
        return body

    m: DecodedMessage = body

    if content_type := getattr(message, "content_type", False):
        with contextlib.suppress(ValueError):
            content_type = ContentTypes(cast("str", content_type))

        if content_type is ContentTypes.TEXT:
            m = body.decode()

        elif content_type is ContentTypes.JSON or (
            isinstance(content_type, str)
            and content_type.startswith(ContentTypes.JSON.value)
        ):
            m = json_loads(body)

    else:
        # content-type not set
        with suppress(json.JSONDecodeError, UnicodeDecodeError):
            m = json_loads(body)

    return m


def encode_message(
    msg: Union[Sequence["SendableMessage"], "SendableMessage"],
    serializer: Optional["SerializerProto"],
) -> tuple[bytes, str | None]:
    """Encodes a message."""
    if msg is None:
        return (
            b"",
            None,
        )

    if isinstance(msg, bytes):
        return (
            msg,
            None,
        )

    if isinstance(msg, str):
        return (
            msg.encode(),
            ContentTypes.TEXT.value,
        )

    if serializer is not None:
        return (
            serializer.encode(msg),
            ContentTypes.JSON.value,
        )

    return (
        json_dumps(msg),
        ContentTypes.JSON.value,
    )
