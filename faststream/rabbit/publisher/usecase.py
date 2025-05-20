from contextlib import AsyncExitStack
from copy import deepcopy
from functools import partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    Optional,
    Sequence,
    Union,
)

from aio_pika import IncomingMessage
from typing_extensions import Annotated, Doc, TypedDict, Unpack, deprecated, override

from faststream.broker.message import SourceType, gen_cor_id
from faststream.broker.publisher.usecase import PublisherUsecase
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.rabbit.schemas import BaseRMQInformation, RabbitQueue
from faststream.rabbit.subscriber.usecase import LogicSubscriber
from faststream.utils.functions import return_input

if TYPE_CHECKING:
    from aio_pika.abc import DateType, HeadersType, TimeoutType

    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware
    from faststream.rabbit.message import RabbitMessage
    from faststream.rabbit.publisher.producer import AioPikaFastProducer
    from faststream.rabbit.schemas.exchange import RabbitExchange
    from faststream.rabbit.types import AioPikaSendableMessage
    from faststream.types import AnyDict, AsyncFunc


# should be public to use in imports
class RequestPublishKwargs(TypedDict, total=False):
    """Typed dict to annotate RabbitMQ requesters."""

    headers: Annotated[
        Optional["HeadersType"],
        Doc(
            "Message headers to store metainformation. "
            "Can be overridden by `publish.headers` if specified."
        ),
    ]
    mandatory: Annotated[
        Optional[bool],
        Doc(
            "Client waits for confirmation that the message is placed to some queue. "
            "RabbitMQ returns message to client if there is no suitable queue."
        ),
    ]
    immediate: Annotated[
        Optional[bool],
        Doc(
            "Client expects that there is consumer ready to take the message to work. "
            "RabbitMQ returns message to client if there is no suitable consumer."
        ),
    ]
    timeout: Annotated[
        "TimeoutType",
        Doc("Send confirmation time from RabbitMQ."),
    ]
    persist: Annotated[
        Optional[bool],
        Doc("Restore the message on RabbitMQ reboot."),
    ]

    priority: Annotated[
        Optional[int],
        Doc("The message priority (0 by default)."),
    ]
    message_type: Annotated[
        Optional[str],
        Doc("Application-specific message type, e.g. **orders.created**."),
    ]
    content_type: Annotated[
        Optional[str],
        Doc(
            "Message **content-type** header. "
            "Used by application, not core RabbitMQ. "
            "Will be set automatically if not specified."
        ),
    ]
    user_id: Annotated[
        Optional[str],
        Doc("Publisher connection User ID, validated if set."),
    ]
    expiration: Annotated[
        Optional["DateType"],
        Doc("Message expiration (lifetime) in seconds (or datetime or timedelta)."),
    ]
    content_encoding: Annotated[
        Optional[str],
        Doc("Message body content encoding, e.g. **gzip**."),
    ]


class PublishKwargs(RequestPublishKwargs, total=False):
    """Typed dict to annotate RabbitMQ publishers."""

    reply_to: Annotated[
        Optional[str],
        Doc(
            "Reply message routing key to send with (always sending to default exchange)."
        ),
    ]


class LogicPublisher(
    PublisherUsecase[IncomingMessage],
    BaseRMQInformation,
):
    """A class to represent a RabbitMQ publisher."""

    app_id: Optional[str]

    _producer: Optional["AioPikaFastProducer"]

    def __init__(
        self,
        *,
        routing_key: str,
        queue: "RabbitQueue",
        exchange: "RabbitExchange",
        message_kwargs: "PublishKwargs",
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[IncomingMessage]"],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            # AsyncAPI args
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        self.routing_key = routing_key

        request_kwargs = dict(message_kwargs)
        self.reply_to = request_kwargs.pop("reply_to", None)
        self.message_kwargs = request_kwargs

        # BaseRMQInformation
        self.queue = queue
        self.exchange = exchange

        # Setup it later
        self.app_id = None
        self.virtual_host = ""

    @override
    def setup(  # type: ignore[override]
        self,
        *,
        producer: Optional["AioPikaFastProducer"],
        app_id: Optional[str],
        virtual_host: str,
    ) -> None:
        self.app_id = app_id
        self.virtual_host = virtual_host
        super().setup(producer=producer)

    @property
    def routing(self) -> str:
        """Return real routing_key of Publisher."""
        return self.routing_key or self.queue.routing

    def __hash__(self) -> int:
        return LogicSubscriber.get_routing_hash(self.queue, self.exchange) + hash(
            self.routing_key
        )

    @override
    async def publish(
        self,
        message: "AioPikaSendableMessage",
        queue: Annotated[
            Union["RabbitQueue", str, None],
            Doc("Message routing key to publish with."),
        ] = None,
        exchange: Annotated[
            Union["RabbitExchange", str, None],
            Doc("Target exchange to publish message to."),
        ] = None,
        *,
        routing_key: Annotated[
            str,
            Doc(
                "Message routing key to publish with. "
                "Overrides `queue` option if presented."
            ),
        ] = "",
        # message args
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        message_id: Annotated[
            Optional[str],
            Doc("Arbitrary message id. Generated automatically if not present."),
        ] = None,
        timestamp: Annotated[
            Optional["DateType"],
            Doc("Message publish timestamp. Generated automatically if not present."),
        ] = None,
        # rpc args
        rpc: Annotated[
            bool,
            Doc("Whether to wait for reply in blocking mode."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        rpc_timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method with `timeout` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = 30.0,
        raise_timeout: Annotated[
            bool,
            Doc(
                "Whetever to raise `TimeoutError` or return `None` at **rpc_timeout**. "
                "RPC request returns `None` at timeout by default."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "`request` always raises TimeoutError instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
        **publish_kwargs: "Unpack[PublishKwargs]",
    ) -> Optional[Any]:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        kwargs: AnyDict = {
            "routing_key": routing_key
            or self.routing_key
            or RabbitQueue.validate(queue or self.queue).routing,
            "exchange": exchange or self.exchange.name,
            "app_id": self.app_id,
            "correlation_id": correlation_id or gen_cor_id(),
            "message_id": message_id,
            "timestamp": timestamp,
            # specific args
            "rpc": rpc,
            "rpc_timeout": rpc_timeout,
            "raise_timeout": raise_timeout,
            "reply_to": self.reply_to,
            **self.message_kwargs,
            **publish_kwargs,
        }

        call: AsyncFunc = self._producer.publish

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        return await call(message, **kwargs)

    @override
    async def request(
        self,
        message: "AioPikaSendableMessage",
        queue: Annotated[
            Union["RabbitQueue", str, None],
            Doc("Message routing key to publish with."),
        ] = None,
        exchange: Annotated[
            Union["RabbitExchange", str, None],
            Doc("Target exchange to publish message to."),
        ] = None,
        *,
        routing_key: Annotated[
            str,
            Doc(
                "Message routing key to publish with. "
                "Overrides `queue` option if presented."
            ),
        ] = "",
        # message args
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        message_id: Annotated[
            Optional[str],
            Doc("Arbitrary message id. Generated automatically if not present."),
        ] = None,
        timestamp: Annotated[
            Optional["DateType"],
            Doc("Message publish timestamp. Generated automatically if not present."),
        ] = None,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
        **publish_kwargs: "Unpack[RequestPublishKwargs]",
    ) -> "RabbitMessage":
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        kwargs: AnyDict = {
            "routing_key": routing_key
            or self.routing_key
            or RabbitQueue.validate(queue or self.queue).routing,
            "exchange": exchange or self.exchange.name,
            "app_id": self.app_id,
            "correlation_id": correlation_id or gen_cor_id(),
            "message_id": message_id,
            "timestamp": timestamp,
            # specific args
            **self.message_kwargs,
            **publish_kwargs,
        }

        request: AsyncFunc = self._producer.request

        for pub_m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            request = partial(pub_m, request)

        published_msg = await request(
            message,
            **kwargs,
        )

        async with AsyncExitStack() as stack:
            return_msg: Callable[[RabbitMessage], Awaitable[RabbitMessage]] = (
                return_input
            )
            for m in self._broker_middlewares[::-1]:
                mid = m(published_msg)
                await stack.enter_async_context(mid)
                return_msg = partial(mid.consume_scope, return_msg)

            parsed_msg = await self._producer._parser(published_msg)
            parsed_msg._decoded_body = await self._producer._decoder(parsed_msg)
            parsed_msg._source_type = SourceType.Response
            return await return_msg(parsed_msg)

        raise AssertionError("unreachable")

    def add_prefix(self, prefix: str) -> None:
        """Include Publisher in router."""
        new_q = deepcopy(self.queue)
        new_q.name = prefix + new_q.name
        self.queue = new_q
