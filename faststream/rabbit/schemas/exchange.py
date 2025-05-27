import warnings
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import Annotated, Doc, deprecated, override

from faststream.broker.schemas import NameRequired
from faststream.rabbit.schemas.constants import ExchangeType
from faststream.types import EMPTY, AnyDict

if TYPE_CHECKING:
    from aio_pika.abc import TimeoutType


class RabbitExchange(NameRequired):
    """A class to represent a RabbitMQ exchange."""

    __slots__ = (
        "arguments",
        "auto_delete",
        "bind_arguments",
        "bind_to",
        "declare",
        "durable",
        "name",
        "passive",
        "robust",
        "routing_key",
        "timeout",
        "type",
    )

    def __hash__(self) -> int:
        return sum(
            (
                hash(self.name),
                hash(self.type),
                hash(self.routing_key),
                int(self.durable),
                int(self.auto_delete),
            )
        )

    @property
    def routing(self) -> str:
        """Return real routing_key of object."""
        return self.routing_key or self.name

    def __init__(
        self,
        name: Annotated[
            str,
            Doc("RabbitMQ exchange name."),
        ] = "",
        type: Annotated[
            ExchangeType,
            Doc(
                "RabbitMQ exchange type. "
                "You can find detail information in the official RabbitMQ documentation: "
                "https://www.rabbitmq.com/tutorials/amqp-concepts#exchanges"
                "\n"
                "Or in the FastStream one: "
                "https://faststream.airt.ai/latest/rabbit/examples/"
            ),
        ] = ExchangeType.DIRECT,
        durable: Annotated[
            bool,
            Doc("Whether the object is durable."),
        ] = False,
        auto_delete: Annotated[
            bool,
            Doc("The exchange will be deleted after connection closed."),
        ] = False,
        declare: Annotated[
            bool,
            Doc(
                "Whether to exchange automatically or just connect to it. "
                "If you want to connect to an existing exchange, set this to `False`. "
                "Copy of `passive` aio-pike option."
            ),
        ] = True,
        passive: Annotated[
            bool,
            deprecated("Use `declare` instead. Will be removed in the 0.6.0 release."),
            Doc("Do not create exchange automatically."),
        ] = EMPTY,
        arguments: Annotated[
            Optional[AnyDict],
            Doc(
                "Exchange declarationg arguments. "
                "You can find usage example in the official RabbitMQ documentation: "
                "https://www.rabbitmq.com/docs/ae"
            ),
        ] = None,
        timeout: Annotated[
            "TimeoutType",
            Doc("Send confirmation time from RabbitMQ."),
        ] = None,
        robust: Annotated[
            bool,
            Doc("Whether to declare exchange object as restorable."),
        ] = True,
        bind_to: Annotated[
            Optional["RabbitExchange"],
            Doc(
                "Another `RabbitExchange` object to bind the current one to. "
                "You can find more information in the official RabbitMQ blog post: "
                "https://www.rabbitmq.com/blog/2010/10/19/exchange-to-exchange-bindings"
            ),
        ] = None,
        bind_arguments: Annotated[
            Optional[AnyDict],
            Doc("Exchange-exchange binding options."),
        ] = None,
        routing_key: Annotated[
            str,
            Doc("Explicit binding routing key."),
        ] = "",
    ) -> None:
        """Initialize a RabbitExchange object."""
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
        self.passive = passive
        self.timeout = timeout
        self.arguments = arguments

        if passive is not EMPTY:
            warnings.warn(
                DeprecationWarning(
                    "Use `declare` instead. Will be removed in the 0.6.0 release.",
                ),
                stacklevel=2,
            )
            self.declare = not passive
        else:
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
