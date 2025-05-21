from typing import TYPE_CHECKING, Optional, Union

from faststream.prometheus import (
    ConsumeAttrs,
    MetricsSettingsProvider,
)
from faststream.redis.response import RedisPublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream.message.message import StreamMessage


class BaseRedisMetricsSettingsProvider(
    MetricsSettingsProvider["AnyDict", RedisPublishCommand]
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "redis"

    def get_publish_destination_name_from_cmd(
        self,
        cmd: RedisPublishCommand,
    ) -> str:
        return cmd.destination


class RedisMetricsSettingsProvider(BaseRedisMetricsSettingsProvider):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[AnyDict]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": _get_destination(msg.raw_message),
            "message_size": len(msg.body),
            "messages_count": 1,
        }


class BatchRedisMetricsSettingsProvider(BaseRedisMetricsSettingsProvider):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[AnyDict]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": _get_destination(msg.raw_message),
            "message_size": len(msg.body),
            "messages_count": len(msg.raw_message["data"]),
        }


def settings_provider_factory(
    msg: Optional["AnyDict"],
) -> Union[
    RedisMetricsSettingsProvider,
    BatchRedisMetricsSettingsProvider,
]:
    if msg is not None and msg.get("type", "").startswith("b"):
        return BatchRedisMetricsSettingsProvider()
    return RedisMetricsSettingsProvider()


def _get_destination(kwargs: "AnyDict") -> str:
    return kwargs.get("channel") or kwargs.get("list") or kwargs.get("stream") or ""
