import warnings
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import override

from faststream._internal.proto import NameRequired
from faststream.rabbit.schemas.constants import ExchangeType

if TYPE_CHECKING:
    from aio_pika.abc import TimeoutType


class RabbitExchange(NameRequired):
    """A class to represent a RabbitMQ exchange."""

    __slots__ = (
        "arguments",
        "auto_delete",
        "bind_arguments",
        "bind_to",
        "durable",
        "name",
        "robust",
        "routing_key",
        "timeout",
        "type",
    )

    def __repr__(self) -> str:
        if self.declare:
            body = f", robust={self.robust}, durable={self.durable}, auto_delete={self.auto_delete})"
        else:
            body = ""

        return f"{self.__class__.__name__}({self.name}, type={self.type}, routing_key='{self.routing()}'{body})"

    def __hash__(self) -> int:
        """Supports hash to store real objects in declarer."""
        return sum(
            (
                hash(self.name),
                hash(self.type),
                hash(self.routing_key),
                int(self.durable),
                int(self.auto_delete),
            ),
        )

    def routing(self) -> str:
        """Return real routing_key of object."""
        return self.routing_key or self.name

    def __init__(
        self,
        name: str = "",
        type: ExchangeType = ExchangeType.DIRECT,
        durable: bool = False,
        auto_delete: bool = False,
        # custom
        declare: bool = True,
        arguments: dict[str, Any] | None = None,
        timeout: "TimeoutType" = None,
        robust: bool = True,
        bind_to: Optional["RabbitExchange"] = None,
        bind_arguments: dict[str, Any] | None = None,
        routing_key: str = "",
    ) -> None:
        """Initialize a RabbitExchange object.

        Args:
            name: RabbitMQ exchange name.
            type: RabbitMQ exchange type.
            durable: Whether the object is durable.
            auto_delete: The exchange will be deleted after connection closed.
            declare: Whether to exchange automatically or just connect to it.
            arguments: Exchange declarationg arguments.
            timeout: Send confirmation time from RabbitMQ.
            robust: Whether to declare exchange object as restorable.
            bind_to: Another `RabbitExchange` object to bind the current one to.
            bind_arguments: Exchange-exchange binding options.
            routing_key: Explicit binding routing key.
        """
        if routing_key and bind_to is None:  # pragma: no cover
            warnings.warn(
                (
                    "\nRabbitExchange `routing_key` is using to bind exchange to another one."
                    "\nIt can be used only with the `bind_to` argument, please setup it too."
                ),
                category=RuntimeWarning,
                stacklevel=1,
            )

        super().__init__(name)

        self.type = type
        self.durable = durable
        self.auto_delete = auto_delete
        self.robust = robust
        self.timeout = timeout
        self.arguments = arguments
        self.declare = declare
        self.bind_to = bind_to
        self.bind_arguments = bind_arguments
        self.routing_key = routing_key

    @override
    @classmethod
    def validate(  # type: ignore[override]
        cls,
        value: Union[str, "RabbitExchange", None],
        **kwargs: Any,
    ) -> "RabbitExchange":
        exch = super().validate(value, **kwargs)
        if exch is None:
            exch = RabbitExchange()
        return exch
