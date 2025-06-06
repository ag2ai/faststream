import logging
from abc import abstractmethod
from collections.abc import Sequence
from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

import anyio
from typing_extensions import TypeAlias, override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream.redis.message import (
    UnifyRedisDict,
)
from faststream.redis.publisher.fake import RedisFakePublisher

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

    from faststream._internal.endpoint.publisher import BasePublisherProto
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.configs import RedisBrokerConfig, RedisSubscriberConfig


TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes


class LogicSubscriber(TasksMixin, SubscriberUsecase[UnifyRedisDict]):
    """A class to represent a Redis handler."""

    _outer_config: "RedisBrokerConfig"

    @property
    def _client(self) -> "Redis[bytes]":
        return self._outer_config.connection.client

    def _make_response_publisher(
        self,
        message: "BrokerStreamMessage[UnifyRedisDict]",
    ) -> Sequence["BasePublisherProto"]:
        return (
            RedisFakePublisher(
                self._outer_config.producer,
                channel=message.reply_to,
            ),
        )

    @override
    async def start(
        self,
        *args: Any,
    ) -> None:
        if self.tasks:
            return

        await super().start()

        self._post_start()

        start_signal = anyio.Event()

        if self.calls:
            self.add_task(self._consume(*args, start_signal=start_signal))

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
                    logging.ERROR,
                    message="Message fetch error",
                    exc_info=e,
                )

                if connected:
                    connected = False

                await anyio.sleep(5)

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

    async def consume_one(self, msg: "BrokerStreamMessage") -> None:
        await self.consume(msg)


class ConcurrentSubscriber(ConcurrentMixin["BrokerStreamMessage"], LogicSubscriber):
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        /,
        *,
        max_workers: int,
    ) -> None:
        super().__init__(config, max_workers=max_workers)

        self._client = None

    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "BrokerStreamMessage") -> None:
        await self._put_msg(msg)
