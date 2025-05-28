import warnings
from typing import TYPE_CHECKING, Dict, Optional, cast

from typing_extensions import Annotated, deprecated

from faststream.types import EMPTY

if TYPE_CHECKING:
    import aio_pika

    from faststream.rabbit.schemas import Channel, RabbitExchange, RabbitQueue

    from .channel_manager import ChannelManager


class RabbitDeclarer:
    """An utility class to declare RabbitMQ queues and exchanges."""

    __slots__ = ("__channel_manager", "__exchanges", "__queues")

    def __init__(self, channel_manage: "ChannelManager") -> None:
        self.__channel_manager = channel_manage
        self.__queues: Dict[RabbitQueue, aio_pika.RobustQueue] = {}
        self.__exchanges: Dict[RabbitExchange, aio_pika.RobustExchange] = {}

    async def declare_queue(
        self,
        queue: "RabbitQueue",
        declare: bool = EMPTY,
        passive: Annotated[
            bool,
            deprecated("Use `declare` instead. Will be removed in the 0.6.0 release."),
        ] = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustQueue":
        """Declare a queue."""
        if (q := self.__queues.get(queue)) is None:
            channel_obj = await self.__channel_manager.get_channel(channel)
            if passive is not EMPTY:
                warnings.warn(
                    DeprecationWarning(
                        "Use `declare` instead. Will be removed in the 0.6.0 release.",
                    ),
                    stacklevel=2,
                )
                declare = not passive
            elif declare is EMPTY:
                declare = queue.declare

            self.__queues[queue] = q = cast(
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
        passive: Annotated[
            bool,
            deprecated("Use `declare` instead. Will be removed in the 0.6.0 release."),
        ] = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustExchange":
        """Declare an exchange, parent exchanges and bind them each other."""
        channel_obj = await self.__channel_manager.get_channel(channel)
        if not exchange.name:
            return channel_obj.default_exchange

        if (exch := self.__exchanges.get(exchange)) is None:
            if passive is not EMPTY:
                warnings.warn(
                    DeprecationWarning(
                        "Use `declare` instead. Will be removed in the 0.6.0 release.",
                    ),
                    stacklevel=2,
                )
                declare = not passive
            elif declare is EMPTY:
                declare = exchange.declare

            self.__exchanges[exchange] = exch = cast(
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
                    routing_key=exchange.routing,
                    arguments=exchange.bind_arguments,
                    timeout=exchange.timeout,
                    robust=exchange.robust,
                )

        return exch
