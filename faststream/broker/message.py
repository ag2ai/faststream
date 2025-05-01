import json
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from uuid import uuid4

from typing_extensions import deprecated

from faststream._compat import dump_json, json_loads
from faststream.constants import ContentTypes
from faststream.types import EMPTY

if TYPE_CHECKING:
    from faststream.types import AnyDict, DecodedMessage, SendableMessage

# prevent circular imports
MsgType = TypeVar("MsgType")


class AckStatus(str, Enum):
    acked = "acked"
    nacked = "nacked"
    rejected = "rejected"


class SourceType(str, Enum):
    Consume = "Consume"
    """Message consumed by basic subscriber flow."""

    Response = "Response"
    """RPC response consumed."""


def gen_cor_id() -> str:
    """Generate random string to use as ID."""
    return str(uuid4())


@dataclass
class StreamMessage(Generic[MsgType]):
    """Generic class to represent a stream message."""

    raw_message: "MsgType"

    body: Union[bytes, Any]
    headers: "AnyDict" = field(default_factory=dict)
    batch_headers: List["AnyDict"] = field(default_factory=list)
    path: "AnyDict" = field(default_factory=dict)

    content_type: Optional[str] = None
    reply_to: str = ""
    message_id: str = field(default_factory=gen_cor_id)  # pragma: no cover
    correlation_id: str = field(
        default_factory=gen_cor_id  # pragma: no cover
    )

    processed: bool = field(default=False, init=False)
    committed: Optional[AckStatus] = field(default=None, init=False)
    _source_type: SourceType = field(default=SourceType.Consume)
    _decoded_body: Optional["DecodedMessage"] = field(default=None, init=False)

    async def ack(self) -> None:
        if not self.committed:
            self.committed = AckStatus.acked

    async def nack(self) -> None:
        if not self.committed:
            self.committed = AckStatus.nacked

    async def reject(self) -> None:
        if not self.committed:
            self.committed = AckStatus.rejected

    async def decode(self) -> Optional["DecodedMessage"]:
        """Serialize the message by lazy decoder."""
        # TODO: make it lazy after `decoded_body` removed
        return self._decoded_body

    @property
    @deprecated(
        "Deprecated in **FastStream 0.5.19**. "
        "Please, use `decode` lazy method instead. "
        "Argument will be removed in **FastStream 0.6.0**.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    def decoded_body(self) -> Optional["DecodedMessage"]:
        return self._decoded_body

    @decoded_body.setter
    @deprecated(
        "Deprecated in **FastStream 0.5.19**. "
        "Please, use `decode` lazy method instead. "
        "Argument will be removed in **FastStream 0.6.0**.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    def decoded_body(self, value: Optional["DecodedMessage"]) -> None:
        self._decoded_body = value


def decode_message(message: "StreamMessage[Any]") -> "DecodedMessage":
    """Decodes a message."""
    body: Any = getattr(message, "body", message)
    m: DecodedMessage = body

    if (content_type := getattr(message, "content_type", EMPTY)) is not EMPTY:
        content_type = cast("Optional[str]", content_type)

        if not content_type:
            with suppress(json.JSONDecodeError, UnicodeDecodeError):
                m = json_loads(body)

        elif ContentTypes.text.value in content_type:
            m = body.decode()

        elif ContentTypes.json.value in content_type:
            m = json_loads(body)

    else:
        with suppress(json.JSONDecodeError, UnicodeDecodeError):
            m = json_loads(body)

    return m


def encode_message(
    msg: Union[Sequence["SendableMessage"], "SendableMessage"],
) -> Tuple[bytes, Optional[str]]:
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
            ContentTypes.text.value,
        )

    return (
        dump_json(msg),
        ContentTypes.json.value,
    )
