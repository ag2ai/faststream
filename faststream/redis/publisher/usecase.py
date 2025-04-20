from abc import abstractmethod
from collections.abc import Iterable, Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Annotated, Optional, Union

from typing_extensions import Doc, override

from faststream._internal.publisher.usecase import PublisherUsecase
from faststream.message import gen_cor_id
from faststream.redis.message import UnifyRedisDict
from faststream.redis.response import RedisPublishCommand
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, SendableMessage
    from faststream._internal.types import BrokerMiddleware, PublisherMiddleware
    from faststream.redis.message import RedisMessage
    from faststream.redis.publisher.producer import RedisFastProducer
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.response.response import PublishCommand


class LogicPublisher(PublisherUsecase[UnifyRedisDict]):
    """A class to represent a Redis publisher."""

    _producer: "RedisFastProducer"

    def __init__(
        self,
        *,
        reply_to: str,
        headers: Optional["AnyDict"],
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        super().__init__(
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
        )

        self.reply_to = reply_to
        self.headers = headers or {}

    @abstractmethod
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        raise NotImplementedError


class ChannelPublisher(LogicPublisher):
    def __init__(
        self,
        *,
        channel: "PubSub",
        reply_to: str,
        headers: Optional["AnyDict"],
        # Regular publisher options
        broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        super().__init__(
            reply_to=reply_to,
            headers=headers,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
        )

        self.channel = channel

    @override
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        return {
            "channel": self.channel.name if name_only else self.channel,
            "list": None,
            "stream": None,
        }

    def add_prefix(self, prefix: str) -> None:
        channel = deepcopy(self.channel)
        channel.name = f"{prefix}{channel.name}"
        self.channel = channel

    @override
    async def publish(
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ] = None,
        channel: Annotated[
            Optional[str],
            Doc("Redis PubSub object name to send message."),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
    ) -> int:
        cmd = RedisPublishCommand(
            message,
            channel=channel or self.channel.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_publish(cmd, _extra_middlewares=())

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(cmd)

        cmd.set_destination(channel=self.channel.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        await self._basic_publish(cmd, _extra_middlewares=_extra_middlewares)

    @override
    async def request(
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ] = None,
        channel: Annotated[
            Optional[str],
            Doc("Redis PubSub object name to send message."),
        ] = None,
        *,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
        ] = 30.0,
    ) -> "RedisMessage":
        cmd = RedisPublishCommand(
            message,
            channel=channel or self.channel.name,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.REQUEST,
            timeout=timeout,
        )

        msg: RedisMessage = await self._basic_request(cmd)
        return msg


class ListPublisher(LogicPublisher):
    def __init__(
        self,
        *,
        list: "ListSub",
        reply_to: str,
        headers: Optional["AnyDict"],
        # Regular publisher options
        broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        super().__init__(
            reply_to=reply_to,
            headers=headers,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
        )

        self.list = list

    @override
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        return {
            "channel": None,
            "list": self.list.name if name_only else self.list,
            "stream": None,
        }

    def add_prefix(self, prefix: str) -> None:
        list_sub = deepcopy(self.list)
        list_sub.name = f"{prefix}{list_sub.name}"
        self.list = list_sub

    @override
    async def publish(
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ] = None,
        list: Annotated[
            Optional[str],
            Doc("Redis List object name to send message."),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
    ) -> int:
        cmd = RedisPublishCommand(
            message,
            list=list or self.list.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )

        return await self._basic_publish(cmd, _extra_middlewares=())

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(cmd)

        cmd.set_destination(list=self.list.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        await self._basic_publish(cmd, _extra_middlewares=_extra_middlewares)

    @override
    async def request(
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ] = None,
        list: Annotated[
            Optional[str],
            Doc("Redis List object name to send message."),
        ] = None,
        *,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
        ] = 30.0,
    ) -> "RedisMessage":
        cmd = RedisPublishCommand(
            message,
            list=list or self.list.name,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.REQUEST,
            timeout=timeout,
        )

        msg: RedisMessage = await self._basic_request(cmd)
        return msg


class ListBatchPublisher(ListPublisher):
    @override
    async def publish(  # type: ignore[override]
        self,
        *messages: Annotated[
            "SendableMessage",
            Doc("Messages bodies to send."),
        ],
        list: Annotated[
            str,
            Doc("Redis List object name to send messages."),
        ],
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
    ) -> int:
        cmd = RedisPublishCommand(
            *messages,
            list=list or self.list.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )

        return await self._basic_publish_batch(cmd, _extra_middlewares=())

    @override
    async def _publish(  # type: ignore[override]
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(cmd, batch=True)

        cmd.set_destination(list=self.list.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        await self._basic_publish_batch(cmd, _extra_middlewares=_extra_middlewares)


class StreamPublisher(LogicPublisher):
    def __init__(
        self,
        *,
        stream: "StreamSub",
        reply_to: str,
        headers: Optional["AnyDict"],
        # Regular publisher options
        broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        super().__init__(
            reply_to=reply_to,
            headers=headers,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
        )

        self.stream = stream

    @override
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        return {
            "channel": None,
            "list": None,
            "stream": self.stream.name if name_only else self.stream,
        }

    def add_prefix(self, prefix: str) -> None:
        stream_sub = deepcopy(self.stream)
        stream_sub.name = f"{prefix}{stream_sub.name}"
        self.stream = stream_sub

    @override
    async def publish(
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ] = None,
        stream: Annotated[
            Optional[str],
            Doc("Redis Stream object name to send message."),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        *,
        maxlen: Annotated[
            Optional[int],
            Doc(
                "Redis Stream maxlen publish option. "
                "Remove eldest message if maxlen exceeded.",
            ),
        ] = None,
    ) -> bytes:
        cmd = RedisPublishCommand(
            message,
            stream=stream or self.stream.name,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            maxlen=maxlen or self.stream.maxlen,
            _publish_type=PublishType.PUBLISH,
        )

        return await self._basic_publish(cmd, _extra_middlewares=())

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RedisPublishCommand.from_cmd(cmd)

        cmd.set_destination(stream=self.stream.name)

        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to
        cmd.maxlen = self.stream.maxlen

        await self._basic_publish(cmd, _extra_middlewares=_extra_middlewares)

    @override
    async def request(
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ] = None,
        stream: Annotated[
            Optional[str],
            Doc("Redis Stream object name to send message."),
        ] = None,
        *,
        maxlen: Annotated[
            Optional[int],
            Doc(
                "Redis Stream maxlen publish option. "
                "Remove eldest message if maxlen exceeded.",
            ),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
        ] = 30.0,
    ) -> "RedisMessage":
        cmd = RedisPublishCommand(
            message,
            stream=stream or self.stream.name,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.REQUEST,
            maxlen=maxlen or self.stream.maxlen,
            timeout=timeout,
        )

        msg: RedisMessage = await self._basic_request(cmd)
        return msg
