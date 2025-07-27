from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional, Union

from faststream._internal._compat import dump_json, json_loads

from .message import MessageFormat

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import AnyDict, SendableMessage


class JSONMessageFormat(MessageFormat):
    """Message format to encode into JSON and parse it."""

    @classmethod
    def encode(
        cls,
        *,
        message: Union[Sequence["SendableMessage"], "SendableMessage"],
        reply_to: str | None,
        headers: Optional["AnyDict"],
        correlation_id: str,
        serializer: Optional["SerializerProto"] = None,
    ) -> bytes:
        msg = cls.build(
            message=message,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id,
            serializer=serializer,
        )
        return dump_json({
            "data": msg.data,
            "headers": msg.headers,
        })

    @classmethod
    def parse(cls, data: bytes) -> tuple[bytes, "AnyDict"]:
        headers: AnyDict
        try:
            parsed_data = json_loads(data)
            final_data = parsed_data["data"].encode()
            headers = parsed_data.get("headers", {})
        except Exception:
            # Raw Redis message format
            final_data = data
            headers = {}
        return final_data, headers
