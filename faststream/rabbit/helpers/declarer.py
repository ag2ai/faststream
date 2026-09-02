from copy import deepcopy
from typing import TYPE_CHECKING, Any, Optional, Protocol, cast

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    import aio_pika

    from faststream.rabbit.schemas import Channel, RabbitExchange, RabbitQueue

    from .channel_manager import ChannelManager


def _queue_declaration(queue: "RabbitQueue") -> dict[str, Any]:
    return {
        "durable": queue.durable,
        "exclusive": queue.exclusive,
        "auto_delete": queue.auto_delete,
        "arguments": deepcopy(queue.arguments or {}),
    }


def _exchange_declaration(exchange: "RabbitExchange") -> dict[str, Any]:
    return {
        "type": exchange.type,
        "durable": exchange.durable,
        "auto_delete": exchange.auto_delete,
        "arguments": deepcopy(exchange.arguments or {}),
    }


def _validate_declaration(
    object_type: str,
    name: str,
    previous: dict[str, Any],
    current: dict[str, Any],
) -> None:
    conflicting = tuple(
        setting for setting, value in current.items() if previous[setting] != value
    )
    if conflicting:
        msg = (
            f"{object_type} {name!r} is already declared with conflicting settings: "
            f"{', '.join(conflicting)}."
        )
        raise SetupError(msg)


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
        self._queues: dict[
            str,
            tuple[dict[str, Any] | None, aio_pika.RobustQueue],
        ] = {}
        self._exchanges: dict[
            str,
            tuple[dict[str, Any] | None, aio_pika.RobustExchange],
        ] = {}

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
        if declare is EMPTY:
            declare = queue.declare

        current = _queue_declaration(queue)
        cached = self._queues.get(queue.name)

        if cached is None or (declare and cached[0] is None):
            channel_obj = await self.__channel_manager.get_channel(channel)

            q = cast(
                "aio_pika.RobustQueue",
                await channel_obj.declare_queue(
                    name=queue.name,
                    durable=queue.durable,
                    exclusive=queue.exclusive,
                    passive=not declare,
                    auto_delete=queue.auto_delete,
                    arguments=deepcopy(queue.arguments),
                    timeout=queue.timeout,
                    robust=queue.robust,
                ),
            )
            self._queues[queue.name] = (current if declare else None, q)

        else:
            previous, q = cached
            if declare:
                assert previous is not None
                _validate_declaration(
                    "RabbitQueue",
                    queue.name,
                    previous,
                    current,
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

        if declare is EMPTY:
            declare = exchange.declare

        current = _exchange_declaration(exchange)
        cached = self._exchanges.get(exchange.name)

        if cached is None or (declare and cached[0] is None):
            exch = cast(
                "aio_pika.RobustExchange",
                await channel_obj.declare_exchange(
                    name=exchange.name,
                    type=exchange.type.value,
                    durable=exchange.durable,
                    auto_delete=exchange.auto_delete,
                    passive=not declare,
                    arguments=deepcopy(exchange.arguments),
                    timeout=exchange.timeout,
                    robust=exchange.robust,
                    internal=False,  # deprecated RMQ option
                ),
            )
            self._exchanges[exchange.name] = (
                current if declare else None,
                exch,
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

        else:
            previous, exch = cached
            if declare:
                assert previous is not None
                _validate_declaration(
                    "RabbitExchange",
                    exchange.name,
                    previous,
                    current,
                )

        return exch
