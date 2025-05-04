from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
)
from unittest.mock import AsyncMock

import anyio
from nats.aio.msg import Msg
from typing_extensions import override

from faststream._internal.subscriber.utils import resolve_custom_func
from faststream._internal.testing.broker import TestBroker
from faststream.exceptions import SubscriberNotFound
from faststream.message import encode_message, gen_cor_id
from faststream.nats.broker import NatsBroker
from faststream.nats.broker.state import ConnectedState
from faststream.nats.parser import NatsParser
from faststream.nats.publisher.producer import NatsFastProducer
from faststream.nats.schemas.js_stream import is_subject_match_wildcard

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream.nats.publisher.specified import SpecificationPublisher
    from faststream.nats.response import NatsPublishCommand
    from faststream.nats.subscriber.usecases.basic import LogicSubscriber

__all__ = ("TestNatsBroker",)


class TestNatsBroker(TestBroker[NatsBroker]):
    """A class to test NATS brokers."""

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: NatsBroker,
        publisher: "SpecificationPublisher",
    ) -> tuple["LogicSubscriber[Any, Any]", bool]:
        sub: Optional[LogicSubscriber[Any, Any]] = None
        publisher_stream = publisher.stream.name if publisher.stream else None
        for handler in broker._subscribers:
            if _is_handler_matches(handler, publisher.subject, publisher_stream):
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(publisher.subject)
        else:
            is_real = True

        return sub, is_real

    @contextmanager
    def _patch_producer(self, broker: NatsBroker) -> Iterator[None]:
        old_js_producer, old_producer = broker._js_producer, broker._producer
        fake_producer = broker._js_producer = FakeProducer(broker)

        broker._state.patch_value(producer=fake_producer)
        try:
            yield
        finally:
            broker._js_producer = old_js_producer
            broker._state.patch_value(producer=old_producer)

    async def _fake_connect(
        self,
        broker: NatsBroker,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncMock:
        if not broker._connection_state:
            broker._connection_state = ConnectedState(AsyncMock(), AsyncMock())
        return AsyncMock()

    def _fake_start(self, broker: NatsBroker, *args: Any, **kwargs: Any) -> None:
        if not broker._connection_state:
            broker._connection_state = ConnectedState(AsyncMock(), AsyncMock())
        return super()._fake_start(broker, *args, **kwargs)


class FakeProducer(NatsFastProducer):
    def __init__(self, broker: NatsBroker) -> None:
        self.broker = broker

        default = NatsParser(pattern="", is_ack_disabled=True)
        self._parser = resolve_custom_func(broker._parser, default.parse_message)
        self._decoder = resolve_custom_func(broker._decoder, default.decode_message)

    @override
    async def publish(  # type: ignore[override]
        self, cmd: "NatsPublishCommand"
    ) -> None:
        incoming = build_message(
            message=cmd.body,
            subject=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
        )

        for handler in _find_handler(
            self.broker._subscribers,
            cmd.destination,
            cmd.stream,
        ):
            msg: Union[list[PatchedMessage], PatchedMessage]

            if (pull := getattr(handler, "pull_sub", None)) and pull.batch:
                msg = [incoming]
            else:
                msg = incoming

            await self._execute_handler(msg, cmd.destination, handler)

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "NatsPublishCommand",
    ) -> "PatchedMessage":
        incoming = build_message(
            message=cmd.body,
            subject=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
        )

        for handler in _find_handler(
            self.broker._subscribers,
            cmd.destination,
            cmd.stream,
        ):
            msg: Union[list[PatchedMessage], PatchedMessage]

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
        handler: "LogicSubscriber[Any, Any]",
    ) -> "PatchedMessage":
        result = await handler.process_message(msg)

        return build_message(
            subject=subject,
            message=result.body,
            headers=result.headers,
            correlation_id=result.correlation_id,
        )


def _find_handler(
    subscribers: Iterable["LogicSubscriber[Any, Any]"],
    subject: str,
    stream: Optional[str] = None,
) -> Generator["LogicSubscriber[Any, Any]", None, None]:
    published_queues = set()
    for handler in subscribers:  # pragma: no branch
        if _is_handler_matches(handler, subject, stream):
            if queue := getattr(handler, "queue", None):
                if queue in published_queues:
                    continue
                else:
                    published_queues.add(queue)
            yield handler


def _is_handler_matches(
    handler: "LogicSubscriber[Any, Any]",
    subject: str,
    stream: Optional[str] = None,
) -> bool:
    if stream:
        if not (handler_stream := getattr(handler, "stream", None)):
            return False

        if stream != handler_stream.name:
            return False

    if is_subject_match_wildcard(subject, handler.clear_subject):
        return True

    for filter_subject in handler.config.filter_subjects or ():
        if is_subject_match_wildcard(subject, filter_subject):
            return True

    return False


def build_message(
    message: "SendableMessage",
    subject: str,
    *,
    reply_to: str = "",
    correlation_id: Optional[str] = None,
    headers: Optional[dict[str, str]] = None,
) -> "PatchedMessage":
    msg, content_type = encode_message(message)
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

    async def nak(self, delay: Optional[float] = None) -> None:
        pass

    async def term(self) -> None:
        pass

    async def in_progress(self) -> None:
        pass
