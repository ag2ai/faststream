import asyncio
import base64
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast, overload
from unittest.mock import AsyncMock, MagicMock

import anyio
from typing_extensions import override

from faststream._internal.parser import DefaultCodec
from faststream._internal.testing.broker import EnterType, TestBroker, change_producer
from faststream.exceptions import SubscriberNotFound
from faststream.message import gen_cor_id
from faststream.sqs.broker.broker import SQSBroker
from faststream.sqs.parser import BASE64_BODY_ATTR
from faststream.sqs.publisher.producer import SQSFastProducer
from faststream.sqs.response import SQSPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream.sqs.publisher.usecase import SQSPublisher
    from faststream.sqs.subscriber.usecase import SQSSubscriber

__all__ = ("TestSQSBroker",)


def _queue_matches(handler_queue: str, destination: str) -> bool:
    return handler_queue.removesuffix(".fifo") == destination.removesuffix(".fifo")


class TestSQSBroker(TestBroker[SQSBroker, EnterType]):
    """In-memory test double for SQSBroker.

    Routes published messages directly to matching subscribers (by queue
    name) without any AWS connection.
    """

    # ``TYPE_CHECKING``-only overloads: they bind ``EnterType`` (single broker
    # vs tuple) for mypy without adding a runtime ``__init__`` frame, which the
    # AST-based ``connect_only`` detection in ``TestBroker`` relies on.
    if TYPE_CHECKING:

        @overload
        def __init__(
            self: "TestSQSBroker[SQSBroker]",
            broker: SQSBroker,
            /,
            *,
            with_real: bool = False,
            connect_only: bool | None = None,
        ) -> None: ...

        @overload
        def __init__(
            self: "TestSQSBroker[tuple[SQSBroker, ...]]",
            *brokers: SQSBroker,
            with_real: bool = False,
            connect_only: bool | None = None,
        ) -> None: ...

        def __init__(
            self,
            *brokers: SQSBroker,
            with_real: bool = False,
            connect_only: bool | None = None,
        ) -> None: ...

    def create_publisher_fake_subscriber(
        self,
        broker: SQSBroker,
        publisher: "SQSPublisher",
    ) -> tuple["SQSSubscriber", bool]:
        sub: SQSSubscriber | None = None
        for handler in (s for b in self.brokers for s in b.subscribers):
            handler = cast("SQSSubscriber", handler)
            if _queue_matches(handler.queue, publisher.queue):
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(publisher.queue, persistent=False)
        else:
            is_real = True

        return sub, is_real

    @contextmanager
    def _patch_producer(self, broker: SQSBroker) -> Iterator[None]:
        fake_producer = FakeProducer(broker, self.brokers)
        with change_producer(broker.config.broker_config, fake_producer):
            yield

    async def _fake_connect(  # type: ignore[override]
        self,
        broker: SQSBroker,
        *args: Any,
        **kwargs: Any,
    ) -> MagicMock:
        fake_client = MagicMock()

        async def _echo_url(*, QueueName: str, **_: Any) -> dict[str, str]:  # noqa: N803
            return {"QueueUrl": QueueName}

        async def _idle(**_: Any) -> dict[str, Any]:
            await asyncio.sleep(1e9)
            return {}  # pragma: no cover

        fake_client.get_queue_url = AsyncMock(side_effect=_echo_url)
        fake_client.create_queue = AsyncMock(side_effect=_echo_url)
        fake_client.receive_message = AsyncMock(side_effect=_idle)
        fake_client.delete_message = AsyncMock(return_value={})
        fake_client.delete_message_batch = AsyncMock(return_value={})
        fake_client.change_message_visibility = AsyncMock(return_value={})
        fake_client.change_message_visibility_batch = AsyncMock(return_value={})

        # The same wiring hook the real SQSBroker._connect uses.
        broker.config.connect(fake_client)
        return fake_client


class FakeProducer(SQSFastProducer):
    """In-memory producer that routes messages directly to matching subscribers."""

    def __init__(
        self,
        broker: SQSBroker,
        brokers: Sequence[SQSBroker],
    ) -> None:
        super().__init__(parser=broker._parser, decoder=broker._decoder)
        self.broker = broker
        self.brokers = brokers
        self.codec = broker.config.broker_codec or DefaultCodec()

    @property
    def subscribers(self) -> Iterable["SQSSubscriber"]:
        return (cast("SQSSubscriber", s) for b in self.brokers for s in b.subscribers)

    @override
    async def publish(self, cmd: "SQSPublishCommand") -> None:
        msg = await build_message(
            message=cmd.body,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            headers=cmd.headers,
            serializer=self.broker.config.fd_config._serializer,
            codec=self.codec,
        )

        for handler in self.subscribers:
            if _queue_matches(handler.queue, cmd.destination):
                msg_to_send = [msg] if handler._batch else msg
                await handler.process_message(msg_to_send)

    @override
    async def publish_batch(self, cmd: "SQSPublishCommand") -> None:
        for body in cmd.batch_bodies:
            await self.publish(
                SQSPublishCommand(
                    body,
                    queue=cmd.destination,
                    headers=cmd.headers,
                    correlation_id=gen_cor_id(),
                    group_id=getattr(cmd, "group_id", None),
                    _publish_type=cmd.publish_type,
                ),
            )

    @override
    async def request(self, cmd: "SQSPublishCommand") -> Any:
        msg = await build_message(
            message=cmd.body,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            serializer=self.broker.config.fd_config._serializer,
            codec=self.codec,
        )

        for handler in self.subscribers:
            if not _queue_matches(handler.queue, cmd.destination):
                continue

            msg_to_send = [msg] if handler._batch else msg
            with anyio.fail_after(cmd.timeout or 30.0):
                result = await handler.process_message(msg_to_send)

            return await build_message(
                message=result.body,
                queue=cmd.destination,
                correlation_id=result.correlation_id,
                headers=result.headers,
                serializer=self.broker.config.fd_config._serializer,
                codec=self.codec,
            )

        raise SubscriberNotFound


async def build_message(
    message: "SendableMessage",
    queue: str,
    *,
    correlation_id: str | None = None,
    reply_to: str = "",
    headers: dict[str, str] | None = None,
    serializer: Optional["SerializerProto"] = None,
    codec: Optional["CodecProto"] = None,
) -> dict[str, Any]:
    """Build a fake raw SQS message dict from publish parameters."""
    if codec is None:
        codec = DefaultCodec()
    payload, content_type = await codec.encode(message, serializer=serializer)

    base64_body = False
    if isinstance(payload, bytes):
        try:
            body = payload.decode()
        except UnicodeDecodeError:
            # mirror SQSFastProducer: SQS bodies are text-only
            body = base64.b64encode(payload).decode()
            base64_body = True
    else:
        body = str(payload)

    attributes: dict[str, Any] = {}
    if base64_body:
        attributes[BASE64_BODY_ATTR] = {"DataType": "String", "StringValue": "1"}
    for key, value in (headers or {}).items():
        attributes[key] = {"DataType": "String", "StringValue": str(value)}
    if content_type:
        attributes["content-type"] = {"DataType": "String", "StringValue": content_type}
    if reply_to:
        attributes["reply_to"] = {"DataType": "String", "StringValue": reply_to}
    cid = correlation_id or gen_cor_id()
    attributes["correlation_id"] = {"DataType": "String", "StringValue": cid}

    return {
        "MessageId": gen_cor_id(),
        "ReceiptHandle": gen_cor_id(),
        "Body": body,
        "MessageAttributes": attributes,
    }
