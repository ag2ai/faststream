from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any, cast

import anyio

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.testing.broker import TestBroker, change_producer
from faststream.exceptions import SubscriberNotFound
from faststream.message import encode_message
from faststream.mq.broker import MQBroker
from faststream.mq.helpers.ids import generate_mq_id, normalize_mq_id
from faststream.mq.message import MQRawMessage
from faststream.mq.parser import MQParser
from faststream.mq.publisher.producer import AsyncMQFastProducer
from faststream.mq.schemas import MQQueue

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream.mq.publisher.usecase import MQPublisher
    from faststream.mq.response import MQPublishCommand
    from faststream.mq.subscriber.usecase import MQSubscriber

__all__ = ("TestMQBroker",)


class TestMQBroker(TestBroker[MQBroker]):
    @contextmanager
    def _patch_producer(self, broker: MQBroker) -> Iterator[None]:
        fake_producer = FakeProducer(broker)

        with ExitStack() as es:
            es.enter_context(change_producer(broker.config.broker_config, fake_producer))
            yield

    @staticmethod
    async def _fake_connect(
        broker: MQBroker,
        *args: Any,
        **kwargs: Any,
    ) -> object:
        return object()

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: MQBroker,
        publisher: "MQPublisher",
    ) -> tuple["MQSubscriber", bool]:
        sub: MQSubscriber | None = None
        for handler in broker.subscribers:
            handler = cast("MQSubscriber", handler)
            if handler.routing() == publisher.routing():
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(queue=publisher.routing(), persistent=False)
        else:
            is_real = True

        return sub, is_real


class FakeProducer(AsyncMQFastProducer):
    is_test_producer = True

    def __init__(self, broker: MQBroker) -> None:
        self.broker = broker

        default_parser = MQParser()
        self._parser = ParserComposition(broker._parser, default_parser.parse_message)
        self._decoder = ParserComposition(broker._decoder, default_parser.decode_message)

    async def connect(self, *, connection_config: Any, serializer: Any) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def ping(self, timeout: float) -> bool:
        return True

    async def publish(self, cmd: "MQPublishCommand") -> None:
        incoming = build_message(
            message=cmd.body,
            queue=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            message_id=cmd.message_id,
            serializer=self.broker.config.fd_config._serializer,
        )

        called = False
        for handler in self.broker.subscribers:
            handler = cast("MQSubscriber", handler)
            if handler.routing() == cmd.destination:
                called = True
                if handler.calls:
                    await self._execute_handler(incoming, handler)
                else:
                    await handler.put_test_message(incoming)

        if not called:
            raise SubscriberNotFound

    async def request(self, cmd: "MQPublishCommand") -> MQRawMessage:
        incoming = build_message(
            message=cmd.body,
            queue=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            message_id=cmd.message_id,
            serializer=self.broker.config.fd_config._serializer,
        )

        for handler in self.broker.subscribers:
            handler = cast("MQSubscriber", handler)
            if handler.routing() == cmd.destination:
                with anyio.fail_after(cmd.timeout):
                    return await self._execute_handler(incoming, handler)

        raise SubscriberNotFound

    async def publish_batch(self, cmd: "MQPublishCommand") -> None:
        msg = "IBM MQ doesn't support publishing in batches."
        raise NotImplementedError(msg)

    async def _execute_handler(
        self,
        msg: MQRawMessage,
        handler: "MQSubscriber",
    ) -> MQRawMessage:
        result = await handler.process_message(msg)
        return build_message(
            queue=msg.queue,
            message=result.body,
            headers=result.headers,
            correlation_id=msg.message_id,
            message_id=getattr(result, "message_id", None),
            serializer=self.broker.config.fd_config._serializer,
        )


def build_message(
    *,
    queue: str,
    message: Any = None,
    headers: dict[str, Any] | None = None,
    correlation_id: str | None = None,
    reply_to: str = "",
    message_id: str | None = None,
    serializer: "SerializerProto | None" = None,
) -> MQRawMessage:
    body, content_type = encode_message(message, serializer)
    headers = headers or {}
    if content_type:
        headers = {"content-type": content_type, **headers}

    normalized_correlation_id = normalize_mq_id(
        correlation_id,
        field_name="correlation_id",
    )
    normalized_message_id = normalize_mq_id(
        message_id,
        field_name="message_id",
    )

    return MQRawMessage(
        body=body,
        queue=MQQueue.validate(queue).routing(),
        headers=headers,
        reply_to=reply_to,
        content_type=content_type,
        correlation_id=normalized_correlation_id,
        message_id=normalized_message_id or generate_mq_id(),
    )
