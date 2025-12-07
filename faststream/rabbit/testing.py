from collections.abc import Generator, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from unittest import mock
from unittest.mock import AsyncMock

import aiormq
import anyio
from aio_pika.message import IncomingMessage
from pamqp import commands as spec
from pamqp.header import ContentHeader
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.testing.broker import TestBroker, change_producer
from faststream.exceptions import SubscriberNotFound
from faststream.message import gen_cor_id
from faststream.rabbit.broker.broker import RabbitBroker
from faststream.rabbit.parser import AioPikaParser
from faststream.rabbit.publisher.producer import AioPikaFastProducer
from faststream.rabbit.schemas import (
    ExchangeType,
    RabbitExchange,
    RabbitQueue,
)

if TYPE_CHECKING:
    from aio_pika.abc import DateType, HeadersType
    from fast_depends.library.serializer import SerializerProto

    from faststream.rabbit.publisher import RabbitPublisher
    from faststream.rabbit.response import RabbitPublishCommand
    from faststream.rabbit.subscriber import RabbitSubscriber
    from faststream.rabbit.types import AioPikaSendableMessage

__all__ = ("TestRabbitBroker",)


class TestRabbitBroker(TestBroker[RabbitBroker]):
    """A class to test RabbitMQ brokers."""

    @contextmanager
    def _patch_broker(self, broker: "RabbitBroker") -> Generator[None, None, None]:
        with (
            mock.patch.object(
                broker,
                "_channel",
                new_callable=AsyncMock,
            ),
            mock.patch.object(
                broker.config,
                "declarer",
                new_callable=AsyncMock,
            ),
            super()._patch_broker(broker),
        ):
            yield

    @contextmanager
    def _patch_producer(self, broker: RabbitBroker) -> Iterator[None]:
        fake_producer = FakeProducer(broker)

        with ExitStack() as es:
            es.enter_context(
                change_producer(broker.config.broker_config, fake_producer),
            )
            yield

    @staticmethod
    async def _fake_connect(broker: "RabbitBroker", *args: Any, **kwargs: Any) -> None:
        pass

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: "RabbitBroker",
        publisher: "RabbitPublisher",
    ) -> tuple["RabbitSubscriber", bool]:
        sub: RabbitSubscriber | None = None
        for handler in broker.subscribers:
            handler = cast("RabbitSubscriber", handler)
            if _is_handler_matches(
                handler,
                publisher.routing(),
                {},
                publisher.exchange,
            ):
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(
                queue=publisher.routing(),
                exchange=publisher.exchange,
                persistent=False,
            )
        else:
            is_real = True

        return sub, is_real


class PatchedMessage(IncomingMessage):
    """Patched message class for testing purposes.

    This class extends aio_pika's IncomingMessage class and is used to simulate RabbitMQ message handling during tests.
    """

    routing_key: str

    async def ack(self, multiple: bool = False) -> None:
        """Asynchronously acknowledge a message."""

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        """Nack the message."""

    async def reject(self, requeue: bool = False) -> None:
        """Rejects a task."""


def build_message(
    message: "AioPikaSendableMessage" = "",
    queue: Union["RabbitQueue", str] = "",
    exchange: Union["RabbitExchange", str, None] = None,
    *,
    routing_key: str = "",
    persist: bool = False,
    reply_to: str | None = None,
    headers: Optional["HeadersType"] = None,
    content_type: str | None = None,
    content_encoding: str | None = None,
    priority: int | None = None,
    correlation_id: str | None = None,
    expiration: Optional["DateType"] = None,
    message_id: str | None = None,
    timestamp: Optional["DateType"] = None,
    message_type: str | None = None,
    user_id: str | None = None,
    app_id: str | None = None,
    serializer: Optional["SerializerProto"] = None,
) -> PatchedMessage:
    """Build a patched RabbitMQ message for testing."""
    que = RabbitQueue.validate(queue)
    exch = RabbitExchange.validate(exchange)

    routing = routing_key or que.routing()

    correlation_id = correlation_id or gen_cor_id()
    msg = AioPikaParser.encode_message(
        message=message,
        persist=persist,
        reply_to=reply_to,
        headers=headers,
        content_type=content_type,
        content_encoding=content_encoding,
        priority=priority,
        correlation_id=correlation_id,
        expiration=expiration,
        message_id=message_id or correlation_id,
        timestamp=timestamp,
        message_type=message_type,
        user_id=user_id,
        app_id=app_id,
        serializer=serializer,
    )

    return PatchedMessage(
        aiormq.abc.DeliveredMessage(
            delivery=spec.Basic.Deliver(
                exchange=getattr(exch, "name", ""),
                routing_key=routing,
            ),
            header=ContentHeader(
                properties=spec.Basic.Properties(
                    content_type=msg.content_type,
                    headers=msg.headers,
                    reply_to=msg.reply_to,
                    content_encoding=msg.content_encoding,
                    priority=msg.priority,
                    correlation_id=msg.correlation_id,
                    message_id=msg.message_id,
                    timestamp=msg.timestamp,
                    message_type=message_type,
                    user_id=msg.user_id,
                    app_id=msg.app_id,
                ),
            ),
            body=msg.body,
            channel=AsyncMock(),
        ),
    )


class FakeProducer(AioPikaFastProducer):
    """A fake RabbitMQ producer for testing purposes.

    This class extends AioPikaFastProducer and is used to simulate RabbitMQ message publishing during tests.
    """

    def __init__(self, broker: RabbitBroker) -> None:
        self.broker = broker

        default_parser = AioPikaParser()
        self._parser = ParserComposition(broker._parser, default_parser.parse_message)
        self._decoder = ParserComposition(
            broker._decoder,
            default_parser.decode_message,
        )

    @override
    async def publish(
        self,
        cmd: "RabbitPublishCommand",
    ) -> None:
        """Publish a message to a RabbitMQ queue or exchange."""
        incoming = build_message(
            message=cmd.body,
            exchange=cmd.exchange,
            routing_key=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            serializer=self.broker.config.fd_config._serializer,
            **cmd.message_options,
        )

        called = False
        for handler in self.broker.subscribers:  # pragma: no branch
            handler = cast("RabbitSubscriber", handler)
            if _is_handler_matches(
                handler,
                incoming.routing_key,
                incoming.headers,
                cmd.exchange,
            ):
                called = True
                await self._execute_handler(incoming, handler)

        if not called:
            raise SubscriberNotFound

    @override
    async def request(
        self,
        cmd: "RabbitPublishCommand",
    ) -> "PatchedMessage":
        """Make a synchronous request to RabbitMQ."""
        incoming = build_message(
            message=cmd.body,
            exchange=cmd.exchange,
            routing_key=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            serializer=self.broker.config.fd_config._serializer,
            **cmd.message_options,
        )

        for handler in self.broker.subscribers:  # pragma: no branch
            handler = cast("RabbitSubscriber", handler)
            if _is_handler_matches(
                handler,
                incoming.routing_key,
                incoming.headers,
                cmd.exchange,
            ):
                with anyio.fail_after(cmd.timeout):
                    return await self._execute_handler(incoming, handler)

        raise SubscriberNotFound

    async def _execute_handler(
        self,
        msg: PatchedMessage,
        handler: "RabbitSubscriber",
    ) -> "PatchedMessage":
        result = await handler.process_message(msg)
        return build_message(
            routing_key=msg.routing_key,
            message=result.body,
            headers=result.headers,
            correlation_id=result.correlation_id,
            serializer=self.broker.config.fd_config._serializer,
        )


def _is_handler_matches(
    handler: "RabbitSubscriber",
    routing_key: str,
    headers: Optional["Mapping[Any, Any]"] = None,
    exchange: Optional["RabbitExchange"] = None,
) -> bool:
    headers = headers or {}
    exchange = RabbitExchange.validate(exchange)

    if handler.exchange != exchange:
        return False

    if handler.exchange is None or handler.exchange.type == ExchangeType.DIRECT:
        return handler.routing() == routing_key

    if handler.exchange.type == ExchangeType.FANOUT:
        return True

    if handler.exchange.type == ExchangeType.TOPIC:
        return apply_pattern(handler.routing(), routing_key)

    if handler.exchange.type == ExchangeType.HEADERS:
        queue_headers = (handler.queue.bind_arguments or {}).copy()

        if not queue_headers:
            return True

        match_rule = queue_headers.pop("x-match", "all")

        full_match = True
        is_headers_empty = True
        for k, v in queue_headers.items():
            if headers.get(k) != v:
                full_match = False
            else:
                is_headers_empty = False

        if is_headers_empty:
            return False

        return full_match or (match_rule == "any")

    raise AssertionError


def apply_pattern(pattern: str, current: str) -> bool:
    """Apply a pattern to a routing key."""
    pattern_queue = iter(pattern.split("."))
    current_queue = iter(current.split("."))

    pattern_symb = next(pattern_queue, None)
    while pattern_symb:
        if (next_symb := next(current_queue, None)) is None:
            return False

        if pattern_symb == "#":
            next_pattern = next(pattern_queue, None)

            if next_pattern is None:
                return True

            if (next_symb := next(current_queue, None)) is None:
                return False

            while next_pattern == "*":
                next_pattern = next(pattern_queue, None)
                if (next_symb := next(current_queue, None)) is None:
                    return False

            while next_symb != next_pattern:
                if (next_symb := next(current_queue, None)) is None:
                    return False

            pattern_symb = next(pattern_queue, None)

        elif pattern_symb in {"*", next_symb}:
            pattern_symb = next(pattern_queue, None)

        else:
            return False

    return next(current_queue, None) is None
