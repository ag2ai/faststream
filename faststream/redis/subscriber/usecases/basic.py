import logging
from abc import abstractmethod
from collections.abc import Callable, Iterable, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, TypeAlias, Union, cast

import anyio
from typing_extensions import overload, override

from faststream._internal.endpoint.subscriber import (
    SubscriberSpecification,
    SubscriberUsecase,
)
from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.subscriber.utils import default_filter
from faststream._internal.types import P_HandlerParams, T_HandlerReturn
from faststream.redis.call_wrapper import RedisHandlerCallWrapper
from faststream.redis.message import (
    UnifyRedisDict,
)
from faststream.redis.publisher.fake import RedisFakePublisher

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from redis.asyncio.client import Redis

    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream._internal.types import CustomCallable, Filter
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.configs import RedisBrokerConfig
    from faststream.redis.subscriber.config import RedisSubscriberConfig


TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes

# Delay (seconds) before a subscriber retries after a message-fetch error.
# Shared by all Redis subscriber loops to avoid busy-looping on persistent
# connection errors (e.g. while a Sentinel pool fails over to a new master).
CONSUME_ERROR_BACKOFF_SECONDS = 5


class LogicSubscriber(TasksMixin, SubscriberUsecase[UnifyRedisDict]):
    """A class to represent a Redis handler."""

    _outer_config: "RedisBrokerConfig"
    _call_wrapper_class = RedisHandlerCallWrapper

    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(config, specification, calls)
        self.config = config

    @overload
    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
    ) -> "RedisHandlerCallWrapper[P_HandlerParams, T_HandlerReturn]": ...

    @overload
    def __call__(
        self,
        func: None = None,
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        "RedisHandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
    ]: ...

    @override
    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn] | None = None,
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
    ) -> Union[
        "RedisHandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
        Callable[
            [Callable[P_HandlerParams, T_HandlerReturn]],
            "RedisHandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
        ],
    ]:
        # The base builds the wrapper from `_call_wrapper_class`; this only narrows the name
        return cast(
            "RedisHandlerCallWrapper[P_HandlerParams, T_HandlerReturn] | Callable["
            "[Callable[P_HandlerParams, T_HandlerReturn]], "
            "RedisHandlerCallWrapper[P_HandlerParams, T_HandlerReturn]]",
            super().__call__(
                func,
                filter=filter,
                parser=parser,
                decoder=decoder,
                dependencies=dependencies,
            ),
        )

    @property
    def _client(self) -> "Redis[bytes]":
        return cast("Redis[bytes]", self._outer_config.connection.client)

    def _make_response_publisher(
        self,
        message: "BrokerStreamMessage[UnifyRedisDict]",
    ) -> Sequence["PublisherProto"]:
        return (
            RedisFakePublisher(
                self._outer_config.producer,
                channel=message.reply_to,
                message_format=self.config.message_format,
            ),
        )

    @override
    async def start(
        self,
        *args: Any,
    ) -> None:
        await super().start()

        self._post_start()

        start_signal = anyio.Event()

        if self.calls:
            self.add_task(self._consume, args, {"start_signal": start_signal})

            with anyio.fail_after(3.0):
                await start_signal.wait()

        else:
            start_signal.set()

    async def _consume(self, *args: Any, start_signal: anyio.Event) -> None:
        connected = True

        while self.running:
            try:
                await self._get_msgs(*args)

            except Exception as e:  # noqa: PERF203
                self._log(
                    log_level=logging.ERROR,
                    message="Message fetch error",
                    exc_info=e,
                )

                if connected:
                    connected = False

                await anyio.sleep(CONSUME_ERROR_BACKOFF_SECONDS)

            else:
                if not connected:
                    connected = True

            finally:
                if not start_signal.is_set():
                    with suppress(Exception):
                        start_signal.set()

    @abstractmethod
    async def _get_msgs(self, *args: Any) -> None:
        raise NotImplementedError

    @staticmethod
    def build_log_context(
        message: Optional["BrokerStreamMessage[Any]"],
        channel: str = "",
    ) -> dict[str, str]:
        return {
            "channel": channel,
            "message_id": getattr(message, "message_id", ""),
        }

    async def consume_one(self, msg: Any) -> None:
        await self.consume(msg)


class ConcurrentSubscriber(
    ConcurrentMixin["BrokerStreamMessage[Any]"],
    LogicSubscriber,
):
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
        max_workers: int,
    ) -> None:
        super().__init__(config, specification, calls, max_workers=max_workers)

    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "BrokerStreamMessage[Any]") -> None:
        await self._put_msg(msg)
