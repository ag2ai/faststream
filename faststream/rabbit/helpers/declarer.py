from typing import TYPE_CHECKING, Optional, Protocol, cast

import aio_pika

from faststream._internal.constants import EMPTY
from faststream.rabbit.schemas import Channel, RabbitQueue

if TYPE_CHECKING:
    import aio_pika

    from faststream.rabbit.schemas import Channel, RabbitExchange, RabbitQueue

    from .channel_manager import ChannelManager


class RabbitDeclarer(Protocol):
    """An utility class to declare RabbitMQ queues and exchanges."""

    def disconnect(self) -> None: ...

    async def declare_queue(
        self,
        queue: "RabbitQueue",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustQueue":
        """Declare a queue."""
        ...

    async def declare_exchange(
        self,
        exchange: "RabbitExchange",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustExchange":
        """Declare an exchange, parent exchanges and bind them each other."""
        ...


class FakeRabbitDeclarer(RabbitDeclarer):
    def disconnect(self) -> None:
        raise NotImplementedError

    async def declare_queue(
        self,
        queue: "RabbitQueue",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustQueue":
        raise NotImplementedError

    async def declare_exchange(
        self,
        exchange: "RabbitExchange",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustExchange":
        raise NotImplementedError


class RabbitDeclarerImpl(RabbitDeclarer):
    __slots__ = ("__channel_manager", "__exchanges", "__queues")

    def __init__(self, channel_manager: "ChannelManager") -> None:
        self.__channel_manager = channel_manager
        self._queues: dict[RabbitQueue, aio_pika.RobustQueue] = {}
        self._exchanges: dict[RabbitExchange, aio_pika.RobustExchange] = {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(queues={list(self._queues.keys())}, exchanges={list(self._exchanges.keys())})"

    def disconnect(self) -> None:
        self._queues.clear()
        self._exchanges.clear()

    async def declare_queue(
        self,
        queue: "RabbitQueue",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustQueue":
        if (q := self._queues.get(queue)) is None:
            if declare is EMPTY:
                declare = queue.declare

            channel_obj = await self.__channel_manager.get_channel(channel)

            self._queues[queue] = q = cast(
                "aio_pika.RobustQueue",
                await channel_obj.declare_queue(
                    name=queue.name,
                    durable=queue.durable,
                    exclusive=queue.exclusive,
                    passive=not declare,
                    auto_delete=queue.auto_delete,
                    arguments=queue.arguments,
                    timeout=queue.timeout,
                    robust=queue.robust,
                ),
            )

        return q

    async def declare_exchange(
        self,
        exchange: "RabbitExchange",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustExchange":
        channel_obj = await self.__channel_manager.get_channel(channel)

        if not exchange.name:
            return channel_obj.default_exchange

        if (exch := self._exchanges.get(exchange)) is None:
            if declare is EMPTY:
                declare = exchange.declare

            self._exchanges[exchange] = exch = cast(
                "aio_pika.RobustExchange",
                await channel_obj.declare_exchange(
                    name=exchange.name,
                    type=exchange.type.value,
                    durable=exchange.durable,
                    auto_delete=exchange.auto_delete,
                    passive=not declare,
                    arguments=exchange.arguments,
                    timeout=exchange.timeout,
                    robust=exchange.robust,
                    internal=False,  # deprecated RMQ option
                ),
            )

            if exchange.bind_to is not None:
                parent = await self.declare_exchange(exchange.bind_to)
                await exch.bind(
                    exchange=parent,
                    routing_key=exchange.routing(),
                    arguments=exchange.bind_arguments,
                    timeout=exchange.timeout,
                    robust=exchange.robust,
                )

        return exch
