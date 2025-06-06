from abc import abstractmethod
from contextlib import AsyncExitStack
from copy import deepcopy
from functools import partial
from itertools import chain
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, Optional, Sequence

from typing_extensions import Annotated, Doc, deprecated, override

from faststream.broker.message import SourceType, gen_cor_id
from faststream.broker.publisher.usecase import PublisherUsecase
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.redis.message import UnifyRedisDict
from faststream.redis.schemas import ListSub, PubSub, StreamSub
from faststream.utils.functions import return_input

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline

    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware
    from faststream.redis.message import RedisMessage
    from faststream.redis.publisher.producer import RedisFastProducer
    from faststream.types import AnyDict, AsyncFunc, SendableMessage


class LogicPublisher(PublisherUsecase[UnifyRedisDict]):
    """A class to represent a Redis publisher."""

    _producer: Optional["RedisFastProducer"]

    def __init__(
        self,
        *,
        reply_to: str,
        headers: Optional["AnyDict"],
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            # AsyncAPI args
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        self.reply_to = reply_to
        self.headers = headers

        self._producer = None

    @abstractmethod
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        raise NotImplementedError()


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
        # AsyncAPI options
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            reply_to=reply_to,
            headers=headers,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        self.channel = channel

    def __hash__(self) -> int:
        return hash(f"publisher:pubsub:{self.channel.name}")

    @override
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        return {
            "channel": self.channel.name if name_only else self.channel,
            "list": None,
            "stream": None,
        }

    def add_prefix(self, prefix: str) -> None:
        channel = deepcopy(self.channel)
        channel.name = "".join((prefix, channel.name))
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
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        *,
        # rpc args
        rpc: Annotated[
            bool,
            Doc("Whether to wait for reply in blocking mode."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        rpc_timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method with `timeout` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = 30.0,
        raise_timeout: Annotated[
            bool,
            Doc(
                "Whetever to raise `TimeoutError` or return `None` at **rpc_timeout**. "
                "RPC request returns `None` at timeout by default."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "`request` always raises TimeoutError instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        pipeline: Annotated[
            Optional["Pipeline[bytes]"],
            Doc(
                "Optional Redis `Pipeline` object to batch multiple commands. "
                "Use it to group Redis operations for optimized execution and reduced latency."
            ),
        ] = None,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
        **kwargs: Any,  # option to suppress maxlen
    ) -> Optional[Any]:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        channel_sub = PubSub.validate(channel or self.channel)
        reply_to = reply_to or self.reply_to
        headers = headers or self.headers
        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        return await call(
            message,
            channel=channel_sub.name,
            pipeline=pipeline,
            # basic args
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id,
            # RPC args
            rpc=rpc,
            rpc_timeout=rpc_timeout,
            raise_timeout=raise_timeout,
        )

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
                "**correlation_id** is a useful option to trace messages."
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
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
    ) -> "RedisMessage":
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        kwargs = {
            "channel": PubSub.validate(channel or self.channel).name,
            # basic args
            "headers": headers or self.headers,
            "correlation_id": correlation_id or gen_cor_id(),
            "timeout": timeout,
        }
        request: AsyncFunc = self._producer.request

        for pub_m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            request = partial(pub_m, request)

        published_msg = await request(
            message,
            **kwargs,
        )

        async with AsyncExitStack() as stack:
            return_msg: Callable[[RedisMessage], Awaitable[RedisMessage]] = return_input
            for m in self._broker_middlewares[::-1]:
                mid = m(published_msg)
                await stack.enter_async_context(mid)
                return_msg = partial(mid.consume_scope, return_msg)

            parsed_msg = await self._producer._parser(published_msg)
            parsed_msg._decoded_body = await self._producer._decoder(parsed_msg)
            parsed_msg._source_type = SourceType.Response
            return await return_msg(parsed_msg)

        raise AssertionError("unreachable")


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
        # AsyncAPI options
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            reply_to=reply_to,
            headers=headers,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        self.list = list

    def __hash__(self) -> int:
        return hash(f"publisher:list:{self.list.name}")

    @override
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        return {
            "channel": None,
            "list": self.list.name if name_only else self.list,
            "stream": None,
        }

    def add_prefix(self, prefix: str) -> None:
        list_sub = deepcopy(self.list)
        list_sub.name = "".join((prefix, list_sub.name))
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
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        *,
        # rpc args
        rpc: Annotated[
            bool,
            Doc("Whether to wait for reply in blocking mode."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        rpc_timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method with `timeout` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = 30.0,
        raise_timeout: Annotated[
            bool,
            Doc(
                "Whetever to raise `TimeoutError` or return `None` at **rpc_timeout**. "
                "RPC request returns `None` at timeout by default."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "`request` always raises TimeoutError instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        pipeline: Annotated[
            Optional["Pipeline[bytes]"],
            Doc(
                "Optional Redis `Pipeline` object to batch multiple commands. "
                "Use it to group Redis operations for optimized execution and reduced latency."
            ),
        ] = None,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
        **kwargs: Any,  # option to suppress maxlen
    ) -> Any:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        list_sub = ListSub.validate(list or self.list)
        reply_to = reply_to or self.reply_to
        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        return await call(
            message,
            list=list_sub.name,
            pipeline=pipeline,
            # basic args
            reply_to=reply_to,
            headers=headers or self.headers,
            correlation_id=correlation_id,
            # RPC args
            rpc=rpc,
            rpc_timeout=rpc_timeout,
            raise_timeout=raise_timeout,
        )

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
                "**correlation_id** is a useful option to trace messages."
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
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
    ) -> "RedisMessage":
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        kwargs = {
            "list": ListSub.validate(list or self.list).name,
            # basic args
            "headers": headers or self.headers,
            "correlation_id": correlation_id or gen_cor_id(),
            "timeout": timeout,
        }

        request: AsyncFunc = self._producer.request

        for pub_m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            request = partial(pub_m, request)

        published_msg = await request(
            message,
            **kwargs,
        )

        async with AsyncExitStack() as stack:
            return_msg: Callable[[RedisMessage], Awaitable[RedisMessage]] = return_input
            for m in self._broker_middlewares[::-1]:
                mid = m(published_msg)
                await stack.enter_async_context(mid)
                return_msg = partial(mid.consume_scope, return_msg)

            parsed_msg = await self._producer._parser(published_msg)
            parsed_msg._decoded_body = await self._producer._decoder(parsed_msg)
            parsed_msg._source_type = SourceType.Response
            return await return_msg(parsed_msg)

        raise AssertionError("unreachable")


class ListBatchPublisher(ListPublisher):
    @override
    async def publish(  # type: ignore[override]
        self,
        message: Annotated[
            Iterable["SendableMessage"],
            Doc("Message body to send."),
        ] = (),
        list: Annotated[
            Optional[str],
            Doc("Redis List object name to send message."),
        ] = None,
        *,
        correlation_id: Annotated[
            Optional[str],
            Doc("Has no real effect. Option to be compatible with original protocol."),
        ] = None,
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
        pipeline: Annotated[
            Optional["Pipeline[bytes]"],
            Doc(
                "Optional Redis `Pipeline` object to batch multiple commands. "
                "Use it to group Redis operations for optimized execution and reduced latency."
            ),
        ] = None,
        **kwargs: Any,  # option to suppress maxlen
    ) -> None:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        list_sub = ListSub.validate(list or self.list)
        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish_batch

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        await call(
            *message,
            list=list_sub.name,
            correlation_id=correlation_id,
            headers=headers or self.headers,
            pipeline=pipeline,
        )


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
        # AsyncAPI options
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            reply_to=reply_to,
            headers=headers,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        self.stream = stream

    def __hash__(self) -> int:
        return hash(f"publisher:stream:{self.stream.name}")

    @override
    def subscriber_property(self, *, name_only: bool) -> "AnyDict":
        return {
            "channel": None,
            "list": None,
            "stream": self.stream.name if name_only else self.stream,
        }

    def add_prefix(self, prefix: str) -> None:
        stream_sub = deepcopy(self.stream)
        stream_sub.name = "".join((prefix, stream_sub.name))
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
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        *,
        maxlen: Annotated[
            Optional[int],
            Doc(
                "Redis Stream maxlen publish option. "
                "Remove eldest message if maxlen exceeded."
            ),
        ] = None,
        # rpc args
        rpc: Annotated[
            bool,
            Doc("Whether to wait for reply in blocking mode."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        rpc_timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method with `timeout` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = 30.0,
        raise_timeout: Annotated[
            bool,
            Doc(
                "Whetever to raise `TimeoutError` or return `None` at **rpc_timeout**. "
                "RPC request returns `None` at timeout by default."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "`request` always raises TimeoutError instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        pipeline: Annotated[
            Optional["Pipeline[bytes]"],
            Doc(
                "Optional Redis `Pipeline` object to batch multiple commands. "
                "Use it to group Redis operations for optimized execution and reduced latency."
            ),
        ] = None,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
    ) -> Optional[Any]:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        stream_sub = StreamSub.validate(stream or self.stream)
        maxlen = maxlen or stream_sub.maxlen
        reply_to = reply_to or self.reply_to
        headers = headers or self.headers
        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        return await call(
            message,
            stream=stream_sub.name,
            maxlen=maxlen,
            pipeline=pipeline,
            # basic args
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id,
            # RPC args
            rpc=rpc,
            rpc_timeout=rpc_timeout,
            raise_timeout=raise_timeout,
        )

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
                "Remove eldest message if maxlen exceeded."
            ),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
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
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
    ) -> "RedisMessage":
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        kwargs = {
            "stream": StreamSub.validate(stream or self.stream).name,
            # basic args
            "headers": headers or self.headers,
            "correlation_id": correlation_id or gen_cor_id(),
            "timeout": timeout,
        }

        request: AsyncFunc = self._producer.request

        for pub_m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            request = partial(pub_m, request)

        published_msg = await request(
            message,
            **kwargs,
        )

        async with AsyncExitStack() as stack:
            return_msg: Callable[[RedisMessage], Awaitable[RedisMessage]] = return_input
            for m in self._broker_middlewares[::-1]:
                mid = m(published_msg)
                await stack.enter_async_context(mid)
                return_msg = partial(mid.consume_scope, return_msg)

            parsed_msg = await self._producer._parser(published_msg)
            parsed_msg._decoded_body = await self._producer._decoder(parsed_msg)
            parsed_msg._source_type = SourceType.Response
            return await return_msg(parsed_msg)

        raise AssertionError("unreachable")
