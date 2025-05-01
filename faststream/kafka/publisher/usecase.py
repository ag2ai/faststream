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

from aiokafka import ConsumerRecord
from typing_extensions import Annotated, Doc, override

from faststream.broker.message import SourceType, gen_cor_id
from faststream.broker.publisher.usecase import PublisherUsecase
from faststream.broker.types import MsgType
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.utils.functions import return_input

if TYPE_CHECKING:
    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware
    from faststream.kafka.message import KafkaMessage
    from faststream.kafka.publisher.producer import AioKafkaFastProducer
    from faststream.types import AsyncFunc, SendableMessage


class LogicPublisher(PublisherUsecase[MsgType]):
    """A class to publish messages to a Kafka topic."""

    _producer: Optional["AioKafkaFastProducer"]

    def __init__(
        self,
        *,
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: str,
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
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ],
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ] = "",
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        timeout: Annotated[
            float,
            Doc("Timeout to send RPC request."),
        ] = 0.5,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
    ) -> "KafkaMessage":
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        topic = topic or self.topic
        partition = partition or self.partition
        headers = headers or self.headers
        correlation_id = correlation_id or gen_cor_id()

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
            topic=topic,
            key=key,
            partition=partition,
            headers=headers,
            timeout=timeout,
            correlation_id=correlation_id,
            timestamp_ms=timestamp_ms,
        )

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
        return await self._producer.flush()


class DefaultPublisher(LogicPublisher[ConsumerRecord]):
    def __init__(
        self,
        *,
        key: Optional[bytes],
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: str,
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
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
            reply_to=reply_to,
            headers=headers,
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
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ],
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ] = "",
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message topic name to send response."),
        ] = "",
        no_confirm: Annotated[
            bool,
            Doc("Do not wait for Kafka publish confirmation."),
        ] = False,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
    ) -> Optional[Any]:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        topic = topic or self.topic
        key = key or self.key
        partition = partition or self.partition
        headers = headers or self.headers
        reply_to = reply_to or self.reply_to
        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        return await call(
            message,
            topic=topic,
            key=key,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            correlation_id=correlation_id,
            timestamp_ms=timestamp_ms,
            no_confirm=no_confirm,
        )

    @override
    async def request(
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ],
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ] = "",
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        timeout: Annotated[
            float,
            Doc("Timeout to send RPC request."),
        ] = 0.5,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
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


class BatchPublisher(LogicPublisher[Tuple["ConsumerRecord", ...]]):
    @override
    async def publish(
        self,
        message: Annotated[
            Union["SendableMessage", Iterable["SendableMessage"]],
            Doc("One message or iterable messages bodies to send."),
        ],
        *extra_messages: Annotated[
            "SendableMessage",
            Doc("Messages bodies to send."),
        ],
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ] = "",
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc("Messages headers to store metainformation."),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message topic name to send response."),
        ] = "",
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        no_confirm: Annotated[
            bool,
            Doc("Do not wait for Kafka publish confirmation."),
        ] = False,
        # publisher specific
        _extra_middlewares: Annotated[
            Iterable["PublisherMiddleware"],
            Doc("Extra middlewares to wrap publishing process."),
        ] = (),
    ) -> None:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        msgs: Iterable[SendableMessage]
        if extra_messages:
            msgs = (cast("SendableMessage", message), *extra_messages)
        else:
            msgs = cast("Iterable[SendableMessage]", message)

        topic = topic or self.topic
        partition = partition or self.partition
        headers = headers or self.headers
        reply_to = reply_to or self.reply_to
        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish_batch

        for m in chain(
            self._middlewares[::-1],
            (
                _extra_middlewares
                or (m(None).publish_scope for m in self._broker_middlewares[::-1])
            ),
        ):
            call = partial(m, call)

        await call(
            *msgs,
            topic=topic,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            correlation_id=correlation_id,
            timestamp_ms=timestamp_ms,
            no_confirm=no_confirm,
        )
