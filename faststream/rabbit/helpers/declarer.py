from copy import deepcopy
from typing import TYPE_CHECKING, Any, Optional, Protocol, cast

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

if TYPE_CHECKING:
    import aio_pika

    from faststream.rabbit.schemas import Channel

    from .channel_manager import ChannelManager


def _validate_declaration(
    schema_type: str,
    object_name: str,
    cached_settings: dict[str, Any],
    requested_settings: dict[str, Any],
) -> None:
    conflicting = tuple(
        setting
        for setting, value in requested_settings.items()
        if cached_settings[setting] != value
    )
    if conflicting:
        msg = (
            f"{schema_type} {object_name!r} is already declared with conflicting settings: "
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

        # Keep a nested snapshot so later schema mutations are detected.
        requested_settings = {
            "durable": queue.durable,
            "exclusive": queue.exclusive,
            "auto_delete": queue.auto_delete,
            "arguments": deepcopy(queue.arguments or {}),
        }
        cached_queue = self._queues.get(queue.name)

        if cached_queue is None or (declare and cached_queue[0] is None):
            channel_obj = await self.__channel_manager.get_channel(channel)

            declared_queue = cast(
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
            self._queues[queue.name] = (
                requested_settings if declare else None,
                declared_queue,
            )

        else:
            cached_settings, declared_queue = cached_queue
            if declare:
                assert cached_settings is not None
                _validate_declaration(
                    RabbitQueue.__name__,
                    queue.name,
                    cached_settings,
                    requested_settings,
                )

        return declared_queue

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

        # Keep a nested snapshot so later schema mutations are detected.
        requested_settings = {
            "type": exchange.type,
            "durable": exchange.durable,
            "auto_delete": exchange.auto_delete,
            "arguments": deepcopy(exchange.arguments or {}),
        }
        cached_exchange = self._exchanges.get(exchange.name)

        if cached_exchange is None or (declare and cached_exchange[0] is None):
            declared_exchange = cast(
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
            self._exchanges[exchange.name] = (
                requested_settings if declare else None,
                declared_exchange,
            )

            if exchange.bind_to is not None:
                parent = await self.declare_exchange(exchange.bind_to)
                await declared_exchange.bind(
                    exchange=parent,
                    routing_key=exchange.routing(),
                    arguments=exchange.bind_arguments,
                    timeout=exchange.timeout,
                    robust=exchange.robust,
                )

        else:
            cached_settings, declared_exchange = cached_exchange
            if declare:
                assert cached_settings is not None
                _validate_declaration(
                    RabbitExchange.__name__,
                    exchange.name,
                    cached_settings,
                    requested_settings,
                )

        return declared_exchange
