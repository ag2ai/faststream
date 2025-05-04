from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Literal, Optional, Union, overload

from aiokafka import ConsumerRecord
from typing_extensions import Doc, override

from faststream._internal.publisher.usecase import PublisherUsecase
from faststream._internal.types import MsgType
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.kafka.message import KafkaMessage
from faststream.kafka.response import KafkaPublishCommand
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    import asyncio

    from aiokafka.structs import RecordMetadata

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import BrokerMiddleware, PublisherMiddleware
    from faststream.kafka.message import KafkaMessage
    from faststream.kafka.publisher.producer import AioKafkaFastProducer
    from faststream.response.response import PublishCommand


class LogicPublisher(PublisherUsecase[MsgType]):
    """A class to publish messages to a Kafka topic."""

    _producer: "AioKafkaFastProducer"

    def __init__(
        self,
        *,
        topic: str,
        partition: Optional[int],
        headers: Optional[dict[str, str]],
        reply_to: str,
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[MsgType]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        super().__init__(
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
        )

        self.topic = topic
        self.partition = partition
        self.reply_to = reply_to
        self.headers = headers or {}

    def add_prefix(self, prefix: str) -> None:
        self.topic = f"{prefix}{self.topic}"

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
            """,
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """,
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """,
            ),
        ] = None,
        headers: Annotated[
            Optional[dict[str, str]],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        timeout: Annotated[
            float,
            Doc("Timeout to send RPC request."),
        ] = 0.5,
    ) -> "KafkaMessage":
        cmd = KafkaPublishCommand(
            message,
            topic=topic or self.topic,
            key=key,
            partition=partition or self.partition,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timestamp_ms=timestamp_ms,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )

        msg: KafkaMessage = await self._basic_request(cmd)
        return msg

    async def flush(self) -> None:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101
        return await self._producer.flush()


class DefaultPublisher(LogicPublisher[ConsumerRecord]):
    def __init__(
        self,
        *,
        key: Union[bytes, str, None],
        topic: str,
        partition: Optional[int],
        headers: Optional[dict[str, str]],
        reply_to: str,
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        super().__init__(
            topic=topic,
            partition=partition,
            reply_to=reply_to,
            headers=headers,
            # publisher args
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
        )

        self.key = key

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: Union[bytes, Any, None] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        reply_to: str = "",
        no_confirm: Literal[True],
    ) -> "asyncio.Future[RecordMetadata]": ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: Union[bytes, Any, None] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        reply_to: str = "",
        no_confirm: Literal[False] = False,
    ) -> "RecordMetadata": ...

    @override
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: Union[bytes, Any, None] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        """Publishes a message to Kafka.

        Args:
            message:
                Message body to send.
            topic:
                Topic where the message will be published.
            key:
                A key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is `None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is `None`, partition is chosen randomly).
                Must be type `bytes`, or be serializable to bytes via configured
                `key_serializer`
            partition:
                Specify a partition. If not set, the partition will be
                selected using the configured `partitioner`
            timestamp_ms:
                Epoch milliseconds (from Jan 1 1970 UTC) to use as
                the message timestamp. Defaults to current time.
            headers:
                Message headers to store metainformation.
            correlation_id:
                Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            reply_to:
                Reply message topic name to send response.
            no_confirm:
                Do not wait for Kafka publish confirmation.

        Returns:
            `asyncio.Future[RecordMetadata]` if no_confirm = True.
            `RecordMetadata` if no_confirm = False.
        """
        cmd = KafkaPublishCommand(
            message,
            topic=topic or self.topic,
            key=key or self.key,
            partition=partition or self.partition,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timestamp_ms=timestamp_ms,
            no_confirm=no_confirm,
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_publish(cmd, _extra_middlewares=())

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "KafkaPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = KafkaPublishCommand.from_cmd(cmd)

        cmd.destination = self.topic
        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        cmd.partition = cmd.partition or self.partition
        cmd.key = cmd.key or self.key

        await self._basic_publish(cmd, _extra_middlewares=_extra_middlewares)

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
            """,
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """,
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """,
            ),
        ] = None,
        headers: Annotated[
            Optional[dict[str, str]],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        timeout: Annotated[
            float,
            Doc("Timeout to send RPC request."),
        ] = 0.5,
    ) -> "KafkaMessage":
        return await super().request(
            message,
            topic=topic,
            key=key or self.key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id,
            timeout=timeout,
        )


class BatchPublisher(LogicPublisher[tuple["ConsumerRecord", ...]]):
    @overload
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        no_confirm: Literal[True],
    ) -> "asyncio.Future[RecordMetadata]": ...

    @overload
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        no_confirm: Literal[False] = False,
    ) -> "RecordMetadata": ...

    @override
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        no_confirm: bool = False,
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        """Publish a message batch as a single request to broker.

        Args:
            *messages:
                Messages bodies to send.
            topic:
                Topic where the message will be published.
            partition:
                Specify a partition. If not set, the partition will be
                selected using the configured `partitioner`
            timestamp_ms:
                Epoch milliseconds (from Jan 1 1970 UTC) to use as
                the message timestamp. Defaults to current time.
            headers:
                Message headers to store metainformation.
            reply_to:
                Reply message topic name to send response.
            correlation_id:
                Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            no_confirm:
                Do not wait for Kafka publish confirmation.

        Returns:
            `asyncio.Future[RecordMetadata]` if no_confirm = True.
            `RecordMetadata` if no_confirm = False.
        """
        cmd = KafkaPublishCommand(
            *messages,
            key=None,
            topic=topic or self.topic,
            partition=partition or self.partition,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timestamp_ms=timestamp_ms,
            no_confirm=no_confirm,
            _publish_type=PublishType.PUBLISH,
        )

        return await self._basic_publish_batch(cmd, _extra_middlewares=())

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "KafkaPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = KafkaPublishCommand.from_cmd(cmd, batch=True)

        cmd.destination = self.topic
        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        cmd.partition = cmd.partition or self.partition

        await self._basic_publish_batch(cmd, _extra_middlewares=_extra_middlewares)
