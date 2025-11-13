from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Optional, TypeAlias

import anyio
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.redis.message import (
    BatchListMessage,
    DefaultListMessage,
    RedisListMessage,
)
from faststream.redis.parser import (
    RedisBatchListParser,
    RedisListParser,
)

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.schemas import ListSub
    from faststream.redis.subscriber.config import RedisSubscriberConfig
    from faststream.redis.subscriber.specification import RedisSubscriberSpecification

TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes


class _ListHandlerMixin(LogicSubscriber):
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(config, specification, calls)
        self._read_lock = anyio.Lock()
        assert config.list_sub
        self._list_sub = config.list_sub

    @property
    def list_sub(self) -> "ListSub":
        return self._list_sub.add_prefix(self._outer_config.prefix)

    def get_log_context(
        self,
        message: Optional["BrokerStreamMessage[Any]"],
    ) -> dict[str, str]:
        return self.build_log_context(
            message=message,
            channel=self.list_sub.name,
        )

    @override
    async def _consume(  # type: ignore[override]
        self,
        client: "Redis[bytes]",
        *,
        start_signal: "anyio.Event",
    ) -> None:
        if await client.ping():
            start_signal.set()
        await super()._consume(client, start_signal=start_signal)

    @override
    async def start(self) -> None:
        await super().start(self._client)

    @override
    async def stop(self) -> None:
        with anyio.move_on_after(self._outer_config.graceful_timeout):
            async with self._read_lock:
                await super().stop()

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "RedisListMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        sleep_interval = timeout / 10
        raw_message = None

        with anyio.move_on_after(timeout):
            while (  # noqa: ASYNC110
                raw_message := await self._client.lpop(name=self.list_sub.name)
            ) is None:
                await anyio.sleep(sleep_interval)

        if not raw_message:
            return None

        redis_incoming_msg = DefaultListMessage(
            type="list",
            data=raw_message,
            channel=self.list_sub.name,
        )

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        msg: RedisListMessage = await process_msg(  # type: ignore[assignment]
            msg=redis_incoming_msg,
            middlewares=(
                m(redis_incoming_msg, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["RedisListMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        timeout = 5
        sleep_interval = timeout / 10
        raw_message = None

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while True:
            with anyio.move_on_after(timeout):
                while (  # noqa: ASYNC110
                    raw_message := await self._client.lpop(name=self.list_sub.name)
                ) is None:
                    await anyio.sleep(sleep_interval)

            if not raw_message:
                continue

            redis_incoming_msg = DefaultListMessage(
                type="list",
                data=raw_message,
                channel=self.list_sub.name,
            )

            msg: RedisListMessage = await process_msg(  # type: ignore[assignment]
                msg=redis_incoming_msg,
                middlewares=(
                    m(redis_incoming_msg, context=context)
                    for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
            yield msg


class ListSubscriber(_ListHandlerMixin):
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "RedisSubscriberSpecification",
        calls: "CallsCollection[Any]",
    ) -> None:
        parser = RedisListParser(config)
        config.parser = parser.parse_message
        config.decoder = parser.decode_message
        super().__init__(config, specification, calls)

    async def _get_msgs(self, client: "Redis[bytes]") -> None:
        async with self._read_lock:
            raw_msg = await client.blpop(
                self.list_sub.name,
                timeout=self.list_sub.polling_interval,
            )

            if raw_msg:
                _, msg_data = raw_msg

                msg = DefaultListMessage(
                    type="list",
                    data=msg_data,
                    channel=self.list_sub.name,
                )

                await self.consume_one(msg)


class ListBatchSubscriber(_ListHandlerMixin):
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "RedisSubscriberSpecification",
        calls: "CallsCollection[Any]",
    ) -> None:
        parser = RedisBatchListParser(config)
        config.parser = parser.parse_message
        config.decoder = parser.decode_message
        super().__init__(config, specification, calls)

    async def _get_msgs(self, client: "Redis[bytes]") -> None:
        async with self._read_lock:
            raw_msgs = await client.lpop(
                name=self.list_sub.name,
                count=self.list_sub.max_records,
            )

            if raw_msgs:
                msg = BatchListMessage(
                    type="blist",
                    channel=self.list_sub.name,
                    data=raw_msgs,
                )

                await self.consume_one(msg)

        if not raw_msgs:
            await anyio.sleep(self.list_sub.polling_interval)


class ListConcurrentSubscriber(
    ConcurrentMixin["BrokerStreamMessage[Any]"],
    ListSubscriber,
):
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "BrokerStreamMessage[Any]") -> None:
        await self._put_msg(msg)
