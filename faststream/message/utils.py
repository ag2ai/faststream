import contextlib
import json
from collections.abc import Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final, Optional, Union, cast
from uuid import uuid4

from faststream._internal._compat import json_dumps, json_loads
from faststream._internal.constants import ContentTypes

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import DecodedMessage, SendableMessage
    from faststream._internal.parser import CodecProto

    from .message import StreamMessage


def gen_cor_id() -> str:
    """Generate random string to use as ID."""
    return str(uuid4())


# NOTE: sentinel for a genuine null message body, distinct from b"" or b"null"
class Tombstone:
    __slots__ = ()

    def __repr__(self) -> str:
        return "TOMBSTONE"

    def __bool__(self) -> bool:
        return False


TOMBSTONE: Final = Tombstone()


# NOTE: body/bodies stay Any - StreamMessage.body is `bytes | Any` even for
# a batch message (a Sequence[bytes] at runtime), so a precise element type
# here doesn't match real call sites. isinstance (not `is TOMBSTONE`) is
# still required so len(body) narrows correctly in the non-tombstone case.
def body_size(body: Any) -> int:
    return 0 if isinstance(body, Tombstone) else len(body)


def batch_body_size(bodies: Sequence[Any]) -> int:
    return sum(body_size(b) for b in bodies)


def value_or_tombstone(value: bytes | None) -> "bytes | Tombstone":
    return TOMBSTONE if value is None else value


def ensure_tombstone_key(key: "bytes | str | None") -> None:
    if key is None:
        msg = "a Kafka tombstone requires a key"
        raise ValueError(msg)


async def encode_or_tombstone(
    message: "SendableMessage | Tombstone",
    codec: "CodecProto",
    serializer: Optional["SerializerProto"],
) -> tuple[bytes | None, str | None]:
    # NOTE: isinstance, not `is` - see body_size above for why.
    if isinstance(message, Tombstone):
        return None, None
    return await codec.encode(message, serializer)


def decode_message(message: "StreamMessage[Any]") -> "DecodedMessage":
    """Decodes a message."""
    body: Any = getattr(message, "body", message)

    # NOTE: message.body is TOMBSTONE only for a genuine wire-level null
    # value (see parser value_or_tombstone) - a real b"null" payload never
    # hits this branch and decodes normally below.
    if body is TOMBSTONE:
        return None

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
