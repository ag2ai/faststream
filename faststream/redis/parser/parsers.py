from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Protocol

from faststream._internal._compat import dump_json, json_loads
from faststream._internal.basic_types import DecodedMessage
from faststream._internal.constants import EMPTY, ContentTypes
from faststream._internal.utils.path import match_path
from faststream.message import decode_message, gen_cor_id
from faststream.redis.message import (
    FAKE_CONSUMER,
    ConsumerProtocol,
    RedisBatchListMessage,
    RedisBatchStreamMessage,
    RedisChannelMessage,
    RedisListMessage,
    RedisStreamMessage,
    _RedisStreamMessageMixin,
    bDATA_KEY,
)

if TYPE_CHECKING:
    from re import Pattern

    from faststream.message import StreamMessage

    from .message import MessageFormat


class ParserConfig(Protocol):
    __slots__ = ()

    @property
    def message_format(self) -> type["MessageFormat"]: ...


@dataclass(slots=True)
class SimpleParserConfig:
    message_format: type["MessageFormat"]


class SimpleParser:
    __slots__ = (
        "config",
        "pattern",
    )

    msg_class: type["StreamMessage[Any]"]

    def __init__(
        self,
        config: "ParserConfig" = EMPTY,
        pattern: Optional["Pattern[str]"] = None,
    ) -> None:
        self.pattern = pattern
        self.config = config

    async def parse_message(
        self,
        message: Mapping[str, Any],
    ) -> "StreamMessage[Mapping[str, Any]]":
        data, headers, batch_headers = self._parse_data(message)

        id_ = gen_cor_id()

        return self._build_message(
            raw_message=message,
            body=data,
            # Only pattern-subscribed messages have "pattern" set;
            # guard here before calling match_path.
            path=match_path(self.pattern, message["channel"])
            if message.get("pattern")
            else {},
            headers=headers,
            batch_headers=batch_headers,
            reply_to=headers.get("reply_to", ""),
            content_type=headers.get("content-type"),
            message_id=headers.get("message_id", id_),
            correlation_id=headers.get("correlation_id", id_),
        )

    def _build_message(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> "StreamMessage[Mapping[str, Any]]":
        return self.msg_class(*args, **kwargs)

    def _parse_data(
        self,
        message: Mapping[str, Any],
    ) -> tuple[bytes, dict[str, Any], list[dict[str, Any]]]:
        return (*self.config.message_format.parse(message["data"]), [])

    async def decode_message(  # noqa: PLR6301
        self,
        msg: "StreamMessage[Any]",
    ) -> DecodedMessage:
        return decode_message(msg)


class RedisPubSubParser(SimpleParser):
    __slots__ = ()

    msg_class = RedisChannelMessage


class RedisListParser(SimpleParser):
    __slots__ = ()

    msg_class = RedisListMessage


class RedisBatchListParser(SimpleParser):
    __slots__ = ()

    msg_class = RedisBatchListMessage

    def _parse_data(
        self,
        message: Mapping[str, Any],
    ) -> tuple[bytes, dict[str, Any], list[dict[str, Any]]]:
        body: list[Any] = []
        batch_headers: list[dict[str, Any]] = []

        for x in message["data"]:
            msg_data, msg_headers = _decode_batch_body_item(x, self.config.message_format)
            body.append(msg_data)
            batch_headers.append(msg_headers)

        first_msg_headers = next(iter(batch_headers), {})

        return (
            dump_json(body),
            {
                **first_msg_headers,
                "content-type": ContentTypes.JSON.value,
            },
            batch_headers,
        )


class _StreamParser(SimpleParser):
    __slots__ = ("consumer", "group")

    msg_class: type["_RedisStreamMessageMixin[Any]"]

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.consumer: ConsumerProtocol = FAKE_CONSUMER
        self.group: str | None = None

    def _setup(self, consumer: ConsumerProtocol, group: str | None) -> None:
        self.consumer = consumer
        self.group = group

    def _build_message(
        self, *args: Any, **kwargs: Any
    ) -> "StreamMessage[Mapping[str, Any]]":
        return self.msg_class(*args, consumer=self.consumer, group=self.group, **kwargs)


class RedisStreamParser(_StreamParser):
    __slots__ = ()

    msg_class = RedisStreamMessage

    def _parse_data(
        self,
        message: Mapping[str, Any],
    ) -> tuple[bytes, dict[str, Any], list[dict[str, Any]]]:
        data = message["data"]
        return (
            *self.config.message_format.parse(data.get(bDATA_KEY) or dump_json(data)),
            [],
        )


class RedisBatchStreamParser(_StreamParser):
    __slots__ = ()

    msg_class = RedisBatchStreamMessage

    def _parse_data(
        self,
        message: Mapping[str, Any],
    ) -> tuple[bytes, dict[str, Any], list[dict[str, Any]]]:
        body: list[Any] = []
        batch_headers: list[dict[str, Any]] = []

        for x in message["data"]:
            msg_data, msg_headers = _decode_batch_body_item(
                x.get(bDATA_KEY, x),
                self.config.message_format,
            )

            body.append(msg_data)
            batch_headers.append(msg_headers)

        first_msg_headers = next(iter(batch_headers), {})

        return (
            dump_json(body),
            {
                **first_msg_headers,
                "content-type": ContentTypes.JSON.value,
            },
            batch_headers,
        )


def _decode_batch_body_item(
    msg_content: bytes, message_format: type["MessageFormat"]
) -> tuple[Any, dict[str, Any]]:
    msg_body, headers = message_format.parse(msg_content)
    try:
        return json_loads(msg_body), headers
    except Exception:
        return msg_body, headers
