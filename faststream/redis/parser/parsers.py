from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Protocol

from faststream._internal._compat import dump_json, json_loads
from faststream._internal.basic_types import DecodedMessage
from faststream._internal.constants import EMPTY, ContentTypes
from faststream.message import decode_message, gen_cor_id
from faststream.redis.message import (
    RedisBatchListMessage,
    RedisBatchStreamMessage,
    RedisChannelMessage,
    RedisListMessage,
    RedisStreamMessage,
    bDATA_KEY,
)

if TYPE_CHECKING:
    from re import Pattern

    from faststream.message import StreamMessage

    from .message import MessageFormat


class ParserConfig(Protocol):
    @property
    def message_format(self) -> type["MessageFormat"]: ...


@dataclass(slots=True)
class SimpleParserConfig:
    message_format: type["MessageFormat"]


class SimpleParser:
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

        return self.msg_class(
            raw_message=message,
            body=data,
            path=self.get_path(message),
            headers=headers,
            batch_headers=batch_headers,
            reply_to=headers.get("reply_to", ""),
            content_type=headers.get("content-type"),
            message_id=headers.get("message_id", id_),
            correlation_id=headers.get("correlation_id", id_),
        )

    def _parse_data(
        self,
        message: Mapping[str, Any],
    ) -> tuple[bytes, dict[str, Any], list[dict[str, Any]]]:
        return (*self.config.message_format.parse(message["data"]), [])

    def get_path(self, message: Mapping[str, Any]) -> dict[str, Any]:
        if (
            (path_re := self.pattern)
            and message.get("pattern")
            and (match := path_re.match(message["channel"]))
        ):
            return match.groupdict()

        return {}

    async def decode_message(
        self,
        msg: "StreamMessage[Any]",
    ) -> DecodedMessage:
        return decode_message(msg)


class RedisPubSubParser(SimpleParser):
    msg_class = RedisChannelMessage


class RedisListParser(SimpleParser):
    msg_class = RedisListMessage


class RedisBatchListParser(SimpleParser):
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


class RedisStreamParser(SimpleParser):
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


class RedisBatchStreamParser(SimpleParser):
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
