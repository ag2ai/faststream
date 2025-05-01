from contextlib import AsyncExitStack
from functools import partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from confluent_kafka import Message
from typing_extensions import override

from faststream.broker.message import SourceType, gen_cor_id
from faststream.broker.publisher.usecase import PublisherUsecase
from faststream.broker.types import MsgType
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.utils.functions import return_input

if TYPE_CHECKING:
    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware
    from faststream.confluent.message import KafkaMessage
    from faststream.confluent.publisher.producer import AsyncConfluentFastProducer
    from faststream.types import AnyDict, AsyncFunc, SendableMessage


class LogicPublisher(PublisherUsecase[MsgType]):
    """A class to publish messages to a Kafka topic."""

    _producer: Optional["AsyncConfluentFastProducer"]

    def __init__(
        self,
        *,
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: Optional[str],
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[MsgType]"],
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

        self.topic = topic
        self.partition = partition
        self.reply_to = reply_to
        self.headers = headers

        self._producer = None

    def __hash__(self) -> int:
        return hash(self.topic)

    def add_prefix(self, prefix: str) -> None:
        self.topic = "".join((prefix, self.topic))

    @override
    async def request(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        timeout: float = 0.5,
        # publisher specific
        _extra_middlewares: Iterable["PublisherMiddleware"] = (),
    ) -> "KafkaMessage":
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        kwargs: AnyDict = {
            "key": key,
            # basic args
            "timeout": timeout,
            "timestamp_ms": timestamp_ms,
            "topic": topic or self.topic,
            "partition": partition or self.partition,
            "headers": headers or self.headers,
            "correlation_id": correlation_id or gen_cor_id(),
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

        published_msg = await request(message, **kwargs)

        async with AsyncExitStack() as stack:
            return_msg: Callable[[KafkaMessage], Awaitable[KafkaMessage]] = return_input
            for m in self._broker_middlewares[::-1]:
                mid = m(published_msg)
                await stack.enter_async_context(mid)
                return_msg = partial(mid.consume_scope, return_msg)

            parsed_msg = await self._producer._parser(published_msg)
            parsed_msg._decoded_body = await self._producer._decoder(parsed_msg)
            parsed_msg._source_type = SourceType.Response
            return await return_msg(parsed_msg)

        raise AssertionError("unreachable")

    async def flush(self) -> None:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101
        await self._producer.flush()


class DefaultPublisher(LogicPublisher[Message]):
    def __init__(
        self,
        *,
        key: Optional[bytes],
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: Optional[str],
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[Message]"],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            topic=topic,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            # publisher args
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            # AsyncAPI args
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        self.key = key

    @override
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        reply_to: str = "",
        no_confirm: bool = False,
        # publisher specific
        _extra_middlewares: Iterable["PublisherMiddleware"] = (),
    ) -> Optional[Any]:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        kwargs: AnyDict = {
            "key": key or self.key,
            # basic args
            "no_confirm": no_confirm,
            "topic": topic or self.topic,
            "partition": partition or self.partition,
            "timestamp_ms": timestamp_ms,
            "headers": headers or self.headers,
            "reply_to": reply_to or self.reply_to,
            "correlation_id": correlation_id or gen_cor_id(),
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
        message: "SendableMessage",
        topic: str = "",
        *,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        timeout: float = 0.5,
        # publisher specific
        _extra_middlewares: Iterable["PublisherMiddleware"] = (),
    ) -> "KafkaMessage":
        return await super().request(
            message=message,
            topic=topic,
            key=key or self.key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id,
            timeout=timeout,
            _extra_middlewares=_extra_middlewares,
        )


class BatchPublisher(LogicPublisher[Tuple[Message, ...]]):
    @override
    async def publish(
        self,
        message: Union["SendableMessage", Iterable["SendableMessage"]],
        *extra_messages: "SendableMessage",
        topic: str = "",
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        reply_to: str = "",
        no_confirm: bool = False,
        # publisher specific
        _extra_middlewares: Iterable["PublisherMiddleware"] = (),
    ) -> None:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        msgs: Iterable[SendableMessage]
        if extra_messages:
            msgs = (cast("SendableMessage", message), *extra_messages)
        else:
            msgs = cast("Iterable[SendableMessage]", message)

        kwargs: AnyDict = {
            "topic": topic or self.topic,
            "no_confirm": no_confirm,
            "partition": partition or self.partition,
            "timestamp_ms": timestamp_ms,
            "headers": headers or self.headers,
            "reply_to": reply_to or self.reply_to,
            "correlation_id": correlation_id or gen_cor_id(),
        }

        call: AsyncFunc = self._producer.publish_batch

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        await call(*msgs, **kwargs)
