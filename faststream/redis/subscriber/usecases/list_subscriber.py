from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

import anyio
from typing_extensions import TypeAlias, override

from faststream._internal.configs import SpecificationConfigs
from faststream._internal.subscriber.mixins import ConcurrentMixin
from faststream._internal.subscriber.utils import process_msg
from faststream.middlewares import AckPolicy
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
from faststream.redis.subscriber.configs import RedisSubscriberBaseConfigs

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

    from faststream.message import StreamMessage as BrokerStreamMessage


TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes


class _ListHandlerMixin(LogicSubscriber):
    def __init__(
        self,
        *,
        base_configs: RedisSubscriberBaseConfigs,
        list: ListSub,
    ) -> None:
        super().__init__(base_configs=base_configs)

        self.list_sub = list

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
        start_signal.set()
        await super()._consume(client, start_signal=start_signal)

    @override
    async def start(self) -> None:
        if self.tasks:
            return

        assert self._client, "You should setup subscriber at first."  # nosec B101

        await super().start(self._client)

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "Optional[RedisListMessage]":
        assert self._client, "You should start subscriber at first."  # nosec B101
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

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

        context = self._state.get().di_state.context

        msg: RedisListMessage = await process_msg(  # type: ignore[assignment]
            msg=redis_incoming_msg,
            middlewares=(
                m(redis_incoming_msg, context=context) for m in self._broker_middlewares
            ),
            parser=self._parser,
            decoder=self._decoder,
        )
        return msg

    def add_prefix(self, prefix: str) -> None:
        new_list = deepcopy(self.list_sub)
        new_list.name = f"{prefix}{new_list.name}"
        self.list_sub = new_list


class ListSubscriber(_ListHandlerMixin):
    def __init__(
        self, *, list: ListSub, base_configs: RedisSubscriberBaseConfigs
    ) -> None:
        parser = RedisListParser()
        base_configs.default_parser = parser.parse_message
        base_configs.default_decoder = parser.decode_message
        base_configs.ack_policy = AckPolicy.DO_NOTHING
        super().__init__(list=list, base_configs=base_configs)

    async def _get_msgs(self, client: "Redis[bytes]") -> None:
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


class BatchListSubscriber(_ListHandlerMixin):
    def __init__(
        self,
        *,
        base_configs: RedisSubscriberBaseConfigs,
        list: ListSub,
    ) -> None:
        parser = RedisBatchListParser()
        base_configs.default_parser = parser.parse_message
        base_configs.default_decoder = parser.decode_message
        base_configs.ack_policy = AckPolicy.DO_NOTHING
        super().__init__(list=list, base_configs=base_configs)

    async def _get_msgs(self, client: "Redis[bytes]") -> None:
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

        else:
            await anyio.sleep(self.list_sub.polling_interval)


class ConcurrentListSubscriber(ConcurrentMixin["BrokerStreamMessage"], ListSubscriber):
    def __init__(
        self,
        *,
        base_configs: RedisSubscriberBaseConfigs,
        specification_configs: SpecificationConfigs,
        list: ListSub,
        max_workers: int,
    ) -> None:
        super().__init__(
            base_configs=base_configs,
            specification_configs=specification_configs,
            list=list,
            max_workers=max_workers,
        )

    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "BrokerStreamMessage") -> None:
        await self._put_msg(msg)
