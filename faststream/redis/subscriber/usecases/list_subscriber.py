from collections.abc import AsyncIterator, Iterable
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeAlias

import anyio
from typing_extensions import override

from faststream._internal.endpoint.derived import Resolved
from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.utils.path import Address
from faststream.redis.address import DeclaredAddress
from faststream.redis.message import (
    BatchListMessage,
    DefaultListMessage,
    RedisListMessage,
)
from faststream.redis.parser import (
    RedisBatchListParser,
    RedisListParser,
)
from faststream.redis.schemas import ListSub

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.subscriber.config import RedisSubscriberConfig

TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes


class _ListHandlerMixin(LogicSubscriber):
    #: Whether this Subscriber pops the list in batches. Read while it is being
    #: constructed — it chooses the class — so a Config value cannot change it.
    batch: ClassVar[bool] = False

    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(config, specification, calls)
        self._read_lock = anyio.Lock()
        assert config.list_sub is not None
        self._declared_list = DeclaredAddress(
            config.list_sub,
            ListSub,
            built_as={"batch": self.batch},
        )
        self._list_sub: Resolved[ListSub] = self._derived.add(
            Resolved("a Subscriber's list"),
        )

    @override
    def _prepare(self) -> None:
        """Build the list before anything reads it.

        First, because everything performed afterwards — the address check, the
        parser, the log context the logger is built from — reads it back as a
        field.
        """
        self._list_sub.set(self._declared_list.build(self._outer_config))
        super()._prepare()

    @property
    def list_sub(self) -> "ListSub":
        """The list this Subscriber pops from."""
        return self._list_sub.get()

    @override
    def subscription_addresses(self) -> Iterable["Address"]:
        """The list this Subscriber pops from, and it is never a template.

        Redis matches a pattern on a channel only — a list is popped by the
        name given, verbatim. So a `{param}` in one is a character like any
        other, and `Address.literal` is what says so: a `Path()` parameter
        naming it is refused at `connect()` rather than going unfilled for
        every message.
        """
        yield Address.literal(
            self.list_sub.name,
            self._declared_list.config_key(self._outer_config),
        )

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
    parser_class = RedisListParser

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
    batch: ClassVar[bool] = True
    parser_class = RedisBatchListParser

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
