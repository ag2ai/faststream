from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import override

from faststream.exceptions import SetupError
from faststream.redis.parser import BinaryMessageFormatV1, MessageFormat
from faststream.redis.schemas import INCORRECT_SETUP_MSG
from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, PublishCommand, Response

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline

    from faststream._internal.basic_types import SendableMessage


class DestinationType(str, Enum):
    Channel = "channel"
    List = "list"
    Stream = "stream"


class RedisResponse(Response):
    def __init__(
        self,
        body: Optional["SendableMessage"] = None,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        maxlen: int | None = None,
        message_format: type["MessageFormat"] = BinaryMessageFormatV1,
    ) -> None:
        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )
        self.maxlen = maxlen
        self.message_format = message_format

    @override
    def as_publish_command(self) -> "RedisPublishCommand":
        return RedisPublishCommand(
            self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
            maxlen=self.maxlen,
            message_format=self.message_format,
            channel="fake-channel",  # it will be replaced by reply-sender
        )


class RedisPublishCommand(BatchPublishCommand):
    destination_type: DestinationType

    def __init__(
        self,
        message: "SendableMessage",
        /,
        *messages: "SendableMessage",
        _publish_type: "PublishType",
        correlation_id: str | None = None,
        channel: str | None = None,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        timeout: float | None = 30.0,
        pipeline: Optional["Pipeline[bytes]"] = None,
        message_format: type["MessageFormat"] = BinaryMessageFormatV1,
    ) -> None:
        super().__init__(
            message,
            *messages,
            _publish_type=_publish_type,
            correlation_id=correlation_id,
            reply_to=reply_to,
            destination="",
            headers=headers,
        )

        self.pipeline = pipeline

        self.message_format = message_format

        self.set_destination(
            channel=channel,
            list=list,
            stream=stream,
        )

        # Stream option
        self.maxlen = maxlen

        # Request option
        self.timeout = timeout

    def set_destination(
        self,
        *,
        channel: str | None = None,
        list: str | None = None,
        stream: str | None = None,
    ) -> None:
        if channel is not None:
            self.destination_type = DestinationType.Channel
            self.destination = channel
        elif list is not None:
            self.destination_type = DestinationType.List
            self.destination = list
        elif stream is not None:
            self.destination_type = DestinationType.Stream
            self.destination = stream
        else:
            raise SetupError(INCORRECT_SETUP_MSG)

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        batch: bool = False,
        message_format: type["MessageFormat"] = BinaryMessageFormatV1,
    ) -> "RedisPublishCommand":
        if isinstance(cmd, RedisPublishCommand):
            # NOTE: Should return a copy probably.
            return cmd

        body, extra_bodies = cls._parse_bodies(cmd.body, batch=batch)

        return cls(
            body,
            *extra_bodies,
            channel=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            message_format=message_format,
            _publish_type=cmd.publish_type,
        )
