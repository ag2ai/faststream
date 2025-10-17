from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Optional, TypeAlias

import anyio
from redis.asyncio.client import (
    PubSub as RPubSub,
)
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.redis.message import (
    PubSubMessage,
    RedisChannelMessage,
)
from faststream.redis.parser import (
    RedisPubSubParser,
)

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.schemas import PubSub
    from faststream.redis.subscriber.config import RedisSubscriberConfig


TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes


class ChannelSubscriber(LogicSubscriber):
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        assert config.channel_sub
        parser = RedisPubSubParser(config, pattern=config.channel_sub.path_regex)
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification, calls)

        self._channel = config.channel_sub
        self.subscription: RPubSub | None = None

    @property
    def channel(self) -> "PubSub":
        return self._channel.add_prefix(self._outer_config.prefix)

    def get_log_context(
        self,
        message: Optional["BrokerStreamMessage[Any]"],
    ) -> dict[str, str]:
        return self.build_log_context(
            message=message,
            channel=self.channel.name,
        )

    @override
    async def start(self) -> None:
        if self.subscription:
            return

        self.subscription = psub = self._client.pubsub()

        if self.channel.pattern:
            await psub.psubscribe(self.channel.name)
        else:
            await psub.subscribe(self.channel.name)

        await super().start(psub)

    async def stop(self) -> None:
        await super().stop()

        if self.subscription is not None:
            await self.subscription.unsubscribe()
            await self.subscription.aclose()  # type: ignore[attr-defined]
            self.subscription = None

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "RedisChannelMessage | None":
        assert self.subscription, "You should start subscriber at first."
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        sleep_interval = timeout / 10

        raw_message: PubSubMessage | None = None

        with anyio.move_on_after(timeout):
            while (raw_message := await self._get_message(self.subscription)) is None:  # noqa: ASYNC110
                await anyio.sleep(sleep_interval)

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        msg: RedisChannelMessage | None = await process_msg(  # type: ignore[assignment]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["RedisChannelMessage"]:  # type: ignore[override]
        assert self.subscription, "You should start subscriber at first."
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        timeout = 5
        sleep_interval = timeout / 10

        raw_message: PubSubMessage | None = None

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while True:
            with anyio.move_on_after(timeout):
                while (  # noqa: ASYNC110
                    raw_message := await self._get_message(self.subscription)
                ) is None:
                    await anyio.sleep(sleep_interval)

            if raw_message is None:
                continue

            msg: RedisChannelMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
            yield msg

    async def _get_message(self, psub: RPubSub) -> PubSubMessage | None:
        raw_msg = await psub.get_message(
            ignore_subscribe_messages=True,
            timeout=self.channel.polling_interval,
        )

        if raw_msg:
            return PubSubMessage(
                type=raw_msg["type"],
                data=raw_msg["data"],
                channel=raw_msg["channel"].decode(),
                pattern=raw_msg["pattern"],
            )

        return None

    async def _get_msgs(self, psub: RPubSub) -> None:
        if msg := await self._get_message(psub):
            await self.consume_one(msg)


class ChannelConcurrentSubscriber(
    ConcurrentMixin["BrokerStreamMessage[Any]"],
    ChannelSubscriber,
):
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "BrokerStreamMessage[Any]") -> None:
        await self._put_msg(msg)
