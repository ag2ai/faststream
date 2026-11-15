from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Optional, Protocol, TypeVar, cast

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

if TYPE_CHECKING:
    from aio_pika import RobustExchange, RobustQueue

    from faststream.rabbit.schemas import Channel

    from .channel_manager import ChannelManager


_Resource = TypeVar("_Resource")


@dataclass(slots=True)
class _CachedDeclaration(Generic[_Resource]):
    settings: dict[str, Any]
    resource: _Resource
    passive: bool


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

    __slots__ = ()

    def disconnect(self) -> None: ...

    async def declare_queue(
        self,
        queue: "RabbitQueue",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "RobustQueue":
        """Declare a queue."""
        ...

    async def declare_exchange(
        self,
        exchange: "RabbitExchange",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "RobustExchange":
        """Declare an exchange, parent exchanges and bind them each other."""
        ...


class FakeRabbitDeclarer(RabbitDeclarer):
    __slots__ = ()

    def disconnect(self) -> None:
        raise NotImplementedError

    async def declare_queue(
        self,
        queue: "RabbitQueue",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "RobustQueue":
        raise NotImplementedError

    async def declare_exchange(
        self,
        exchange: "RabbitExchange",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "RobustExchange":
        raise NotImplementedError


class RabbitDeclarerImpl(RabbitDeclarer):
    __slots__ = ("__channel_manager", "_exchanges", "_queues")

    def __init__(self, channel_manager: "ChannelManager") -> None:
        self.__channel_manager = channel_manager
        self._queues: dict[str, _CachedDeclaration[RobustQueue]] = {}
        self._exchanges: dict[str, _CachedDeclaration[RobustExchange]] = {}

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
    ) -> "RobustQueue":
        if declare is EMPTY:
            declare = queue.declare

        queue_name = queue.name
        durable = queue.durable
        exclusive = queue.exclusive
        auto_delete = queue.auto_delete
        arguments = deepcopy(queue.arguments or {})
        timeout = queue.timeout
        robust = queue.robust
        requested_settings: dict[str, Any] = {
            "durable": durable,
            "exclusive": exclusive,
            "auto_delete": auto_delete,
            "arguments": deepcopy(arguments),
        }
        # Empty names request independent generated queues (see issue #2034).
        cached_queue = self._queues.get(queue_name) if queue_name else None
        if cached_queue is not None and (
            not declare
            or not cached_queue.passive
            or cached_queue.settings == requested_settings
        ):
            if declare and not cached_queue.passive:
                _validate_declaration(
                    RabbitQueue.__name__,
                    queue_name,
                    cached_queue.settings,
                    requested_settings,
                )
            return cached_queue.resource

        channel_obj = await self.__channel_manager.get_channel(channel)
        declared_queue = cast(
            "RobustQueue",
            await channel_obj.declare_queue(
                name=queue_name,
                durable=durable,
                exclusive=exclusive,
                passive=not declare,
                auto_delete=auto_delete,
                arguments=arguments,
                timeout=timeout,
                robust=robust,
            ),
        )
        if queue_name:
            self._queues[queue_name] = _CachedDeclaration(
                settings=requested_settings,
                resource=declared_queue,
                passive=not declare,
            )
        return declared_queue

    async def declare_exchange(
        self,
        exchange: "RabbitExchange",
        declare: bool = EMPTY,
        *,
        channel: Optional["Channel"] = None,
    ) -> "RobustExchange":
        exchange_name = exchange.name
        if not exchange_name:
            channel_obj = await self.__channel_manager.get_channel(channel)
            return channel_obj.default_exchange

        if declare is EMPTY:
            declare = exchange.declare

        # Keep cache history independent of the client's mutable arguments.
        arguments = deepcopy(exchange.arguments or {})
        requested_settings: dict[str, Any] = {
            "type": exchange.type,
            "durable": exchange.durable,
            "auto_delete": exchange.auto_delete,
            "arguments": arguments,
        }
        cached_exchange = self._exchanges.get(exchange_name)
        if cached_exchange is not None and (
            not declare
            or not cached_exchange.passive
            or cached_exchange.settings == requested_settings
        ):
            if declare and not cached_exchange.passive:
                _validate_declaration(
                    RabbitExchange.__name__,
                    exchange_name,
                    cached_exchange.settings,
                    requested_settings,
                )
            return cached_exchange.resource

        channel_obj = await self.__channel_manager.get_channel(channel)
        declared_exchange = cast(
            "RobustExchange",
            await channel_obj.declare_exchange(
                name=exchange_name,
                type=exchange.type.value,
                durable=exchange.durable,
                auto_delete=exchange.auto_delete,
                passive=not declare,
                arguments=arguments,
                timeout=exchange.timeout,
                robust=exchange.robust,
                internal=False,
            ),
        )
        entry = _CachedDeclaration(
            settings=requested_settings,
            resource=declared_exchange,
            passive=not declare,
        )
        # Register before recursion so cyclic exchange bindings terminate.
        self._exchanges[exchange_name] = entry
        try:
            if exchange.bind_to is not None:
                parent = await self.declare_exchange(exchange.bind_to)
                await declared_exchange.bind(
                    exchange=parent,
                    routing_key=exchange.routing(),
                    arguments=exchange.bind_arguments,
                    timeout=exchange.timeout,
                    robust=exchange.robust,
                )
        except BaseException:
            # Failed or cancelled bindings must be retried (see issue #2034).
            if self._exchanges.get(exchange_name) is entry:
                if cached_exchange is None:
                    self._exchanges.pop(exchange_name)
                else:
                    self._exchanges[exchange_name] = cached_exchange
            raise

        return declared_exchange
