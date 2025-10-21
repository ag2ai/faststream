from abc import abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import override

from faststream._internal.endpoint.publisher import (
    PublisherSpecification,
    PublisherUsecase,
)
from faststream.message import gen_cor_id
from faststream.redis.response import RedisPublishCommand
from faststream.response.publish_type import PublishType

from .producer import RedisFastProducer

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import PublisherMiddleware
    from faststream.redis.message import RedisChannelMessage
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.response import PublishCommand

    from .config import RedisPublisherConfig


class LogicPublisher(PublisherUsecase):
    """A class to represent a Redis publisher."""

    def __init__(
        self,
        config: "RedisPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self.config = config

        self.reply_to = config.reply_to
        self.headers = config.headers or {}

        self.producer = self.config._outer_config.producer

    async def start(self) -> None:
        await super().start()

        broker_producer = self.config._outer_config.producer

        self.producer = RedisFastProducer(
            connection=self.config._outer_config.connection,
            parser=broker_producer._parser.custom_func,
            decoder=broker_producer._decoder.custom_func,
            message_format=self.config.message_format,
            serializer=self.config._outer_config.fd_config._serializer,
        )

    @abstractmethod
    def subscriber_property(self, *, name_only: bool) -> dict[str, Any]:
        raise NotImplementedError


class ChannelPublisher(LogicPublisher):
    def __init__(
        self,
        config: "RedisPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
        *,
        channel: "PubSub",
    ) -> None:
        super().__init__(config, specification)

        self._channel = channel

    @property
    def channel(self) -> "PubSub":
        return self._channel.add_prefix(self._outer_config.prefix)

    @override
    def subscriber_property(self, *, name_only: bool) -> dict[str, Any]:
        return {
            "channel": self.channel.name if name_only else self.channel,
            "list": None,
            "stream": None,
        }

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        *,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int:
        cmd = RedisPublishCommand(
            message,
            channel=channel or self.channel.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.config.message_format,
        )
        result: int = await self._basic_publish(
            cmd,
            producer=self.producer,
            _extra_middlewares=(),
        )
        return result

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(cmd, message_format=self.config.message_format)

        cmd.set_destination(channel=self.channel.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        await self._basic_publish(
            cmd,
            producer=self.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = 30.0,
    ) -> "RedisChannelMessage":
        cmd = RedisPublishCommand(
            message,
            channel=channel or self.channel.name,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
            message_format=self.config.message_format,
        )

        msg: RedisChannelMessage = await self._basic_request(
            cmd,
            producer=self.producer,
        )
        return msg


class ListPublisher(LogicPublisher):
    def __init__(
        self,
        config: "RedisPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
        *,
        list: "ListSub",
    ) -> None:
        super().__init__(config, specification)

        self._list = list

    @property
    def list(self) -> "ListSub":
        return self._list.add_prefix(self._outer_config.prefix)

    @override
    def subscriber_property(self, *, name_only: bool) -> dict[str, Any]:
        return {
            "channel": None,
            "list": self.list.name if name_only else self.list,
            "stream": None,
        }

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        list: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        *,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int:
        cmd = RedisPublishCommand(
            message,
            list=list or self.list.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.config.message_format,
        )

        result: int = await self._basic_publish(
            cmd,
            producer=self.producer,
            _extra_middlewares=(),
        )
        return result

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(cmd, message_format=self.config.message_format)

        cmd.set_destination(list=self.list.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        await self._basic_publish(
            cmd,
            producer=self.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        list: str | None = None,
        *,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = 30.0,
    ) -> "RedisChannelMessage":
        cmd = RedisPublishCommand(
            message,
            list=list or self.list.name,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
            message_format=self.config.message_format,
        )

        msg: RedisChannelMessage = await self._basic_request(
            cmd,
            producer=self.producer,
        )
        return msg


class ListBatchPublisher(ListPublisher):
    @override
    async def publish(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int:
        cmd = RedisPublishCommand(
            *messages,
            list=list or self.list.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.config.message_format,
        )

        result: int = await self._basic_publish_batch(
            cmd,
            producer=self.producer,
            _extra_middlewares=(),
        )
        return result

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(
            cmd, batch=True, message_format=self.config.message_format
        )

        cmd.set_destination(list=self.list.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        await self._basic_publish_batch(
            cmd,
            producer=self.producer,
            _extra_middlewares=_extra_middlewares,
        )


class StreamPublisher(LogicPublisher):
    def __init__(
        self,
        config: "RedisPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
        *,
        stream: "StreamSub",
    ) -> None:
        super().__init__(config, specification)
        self._stream = stream

    @property
    def stream(self) -> "StreamSub":
        return self._stream.add_prefix(self._outer_config.prefix)

    @override
    def subscriber_property(self, *, name_only: bool) -> dict[str, Any]:
        return {
            "channel": None,
            "list": None,
            "stream": self.stream.name if name_only else self.stream,
        }

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        stream: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        *,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> bytes:
        cmd = RedisPublishCommand(
            message,
            stream=stream or self.stream.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            maxlen=maxlen or self.stream.maxlen,
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.config.message_format,
        )

        result: bytes = await self._basic_publish(
            cmd,
            producer=self.producer,
            _extra_middlewares=(),
        )
        return result

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(cmd, message_format=self.config.message_format)

        cmd.set_destination(stream=self.stream.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to
        cmd.maxlen = self.stream.maxlen

        await self._basic_publish(
            cmd,
            producer=self.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        stream: str | None = None,
        *,
        maxlen: int | None = None,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = 30.0,
    ) -> "RedisChannelMessage":
        cmd = RedisPublishCommand(
            message,
            stream=stream or self.stream.name,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            maxlen=maxlen or self.stream.maxlen,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
            message_format=self.config.message_format,
        )

        msg: RedisChannelMessage = await self._basic_request(
            cmd,
            producer=self.producer,
        )
        return msg
