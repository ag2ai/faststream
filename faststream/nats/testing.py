from collections.abc import Generator, Iterable, Iterator
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast
from unittest.mock import AsyncMock

import anyio
from nats.aio.msg import Msg
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.testing.broker import TestBroker
from faststream.exceptions import SubscriberNotFound
from faststream.message import encode_message, gen_cor_id
from faststream.nats.broker import NatsBroker
from faststream.nats.parser import NatsParser
from faststream.nats.publisher.producer import NatsFastProducer
from faststream.nats.schemas.js_stream import is_subject_match_wildcard

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.configs.broker import ConfigComposition
    from faststream.nats.configs import NatsBrokerConfig
    from faststream.nats.publisher.usecase import LogicPublisher
    from faststream.nats.response import NatsPublishCommand
    from faststream.nats.subscriber.usecases.basic import LogicSubscriber

__all__ = ("TestNatsBroker",)


@contextmanager
def change_producer(
    config: "ConfigComposition[NatsBrokerConfig]",
    producer: "NatsFastProducer",
) -> Generator[None, None, None]:
    old_producer, config.broker_config.producer = (
        config.broker_config.producer,
        producer,
    )
    old_js_producer, config.broker_config.js_producer = (
        config.broker_config.js_producer,
        producer,
    )
    yield
    config.broker_config.producer = old_producer
    config.broker_config.js_producer = old_js_producer


class TestNatsBroker(TestBroker[NatsBroker]):
    """A class to test NATS brokers."""

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: NatsBroker,
        publisher: "LogicPublisher",
    ) -> tuple["LogicSubscriber[Any]", bool]:
        publisher_stream = publisher.stream.name if publisher.stream else None

        sub: LogicSubscriber[Any] | None = None
        for handler in broker.subscribers:
            handler = cast("LogicSubscriber[Any]", handler)
            if _is_handler_matches(handler, publisher.subject, publisher_stream):
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(publisher.subject, persistent=False)
        else:
            is_real = True

        return sub, is_real

    @contextmanager
    def _patch_producer(self, broker: NatsBroker) -> Iterator[None]:
        fake_producer = FakeProducer(broker)

        with ExitStack() as es:
            es.enter_context(change_producer(broker.config, fake_producer))
            yield

    async def _fake_connect(
        self,
        broker: NatsBroker,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if not broker.config.connection_state:
            broker.config.connection_state.connect(AsyncMock(), AsyncMock())

    def _fake_start(self, broker: NatsBroker, *args: Any, **kwargs: Any) -> None:
        if not broker.config.connection_state:
            broker.config.connection_state.connect(AsyncMock(), AsyncMock())
        return super()._fake_start(broker, *args, **kwargs)


class FakeProducer(NatsFastProducer):
    def __init__(self, broker: NatsBroker) -> None:
        self.broker = broker

        default = NatsParser(pattern="", is_ack_disabled=True)
        self._parser = ParserComposition(broker._parser, default.parse_message)
        self._decoder = ParserComposition(broker._decoder, default.decode_message)

    @override
    async def publish(self, cmd: "NatsPublishCommand") -> None:
        incoming = build_message(
            message=cmd.body,
            subject=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            serializer=self.broker.config.fd_config._serializer,
        )

        for handler in _find_handler(
            cast("list[LogicSubscriber[Any]]", self.broker.subscribers),
            cmd.destination,
            cmd.stream,
        ):
            msg: list[PatchedMessage] | PatchedMessage

            if (pull := getattr(handler, "pull_sub", None)) and pull.batch:
                msg = [incoming]
            else:
                msg = incoming

            await self._execute_handler(msg, cmd.destination, handler)

    @override
    async def request(self, cmd: "NatsPublishCommand") -> "PatchedMessage":
        incoming = build_message(
            message=cmd.body,
            subject=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            serializer=self.broker.config.fd_config._serializer,
        )

        for handler in _find_handler(
            cast("list[LogicSubscriber[Any]]", self.broker.subscribers),
            cmd.destination,
            cmd.stream,
        ):
            msg: list[PatchedMessage] | PatchedMessage

            if (pull := getattr(handler, "pull_sub", None)) and pull.batch:
                msg = [incoming]
            else:
                msg = incoming

            with anyio.fail_after(cmd.timeout):
                return await self._execute_handler(msg, cmd.destination, handler)

        raise SubscriberNotFound

    async def _execute_handler(
        self,
        msg: Any,
        subject: str,
        handler: "LogicSubscriber[Any]",
    ) -> "PatchedMessage":
        result = await handler.process_message(msg)

        return build_message(
            subject=subject,
            message=result.body,
            headers=result.headers,
            correlation_id=result.correlation_id,
            serializer=self.broker.config.fd_config._serializer,
        )


def _find_handler(
    subscribers: Iterable["LogicSubscriber[Any]"],
    subject: str,
    stream: str | None = None,
) -> Generator["LogicSubscriber[Any]", None, None]:
    published_queues = set()
    for handler in subscribers:
        if _is_handler_matches(handler, subject, stream):
            if queue := getattr(handler, "queue", None):
                if queue in published_queues:
                    continue
                else:
                    published_queues.add(queue)
            yield handler


def _is_handler_matches(
    handler: "LogicSubscriber[Any]",
    subject: str,
    stream: str | None = None,
) -> bool:
    if stream:
        if not (handler_stream := getattr(handler, "stream", None)):
            return False

        if stream != handler_stream.name:
            return False

    if is_subject_match_wildcard(subject, handler.clear_subject):
        return True

    for filter_subject in handler.filter_subjects or ():
        if is_subject_match_wildcard(subject, filter_subject):
            return True

    return False


def build_message(
    message: "SendableMessage",
    subject: str,
    *,
    reply_to: str = "",
    correlation_id: str | None = None,
    headers: dict[str, str] | None = None,
    serializer: Optional["SerializerProto"] = None,
) -> "PatchedMessage":
    msg, content_type = encode_message(message, serializer=serializer)
    return PatchedMessage(
        _client=None,  # type: ignore[arg-type]
        subject=subject,
        reply=reply_to,
        data=msg,
        headers={
            "content-type": content_type or "",
            "correlation_id": correlation_id or gen_cor_id(),
            **(headers or {}),
        },
    )


class PatchedMessage(Msg):
    async def ack(self) -> None:
        pass

    async def ack_sync(
        self,
        timeout: float = 1,
    ) -> "PatchedMessage":  # pragma: no cover
        return self

    async def nak(self, delay: float | None = None) -> None:
        pass

    async def term(self) -> None:
        pass

    async def in_progress(self) -> None:
        pass
