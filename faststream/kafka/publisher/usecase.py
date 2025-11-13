from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal, Union, cast, overload

from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.kafka.message import KafkaMessage
from faststream.kafka.response import KafkaPublishCommand
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    import asyncio

    from aiokafka.structs import RecordMetadata

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.kafka.message import KafkaMessage
    from faststream.response.response import PublishCommand

    from .config import KafkaPublisherConfig
    from .producer import AioKafkaFastProducer


class LogicPublisher(PublisherUsecase):
    """A class to publish messages to a Kafka topic."""

    def __init__(
        self,
        config: "KafkaPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self._topic = config.topic
        self.partition = config.partition
        self.reply_to = config.reply_to
        self.headers = config.headers or {}

    @property
    def topic(self) -> str:
        return f"{self._outer_config.prefix}{self._topic}"

    @override
    async def request(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        timeout: float = 0.5,
    ) -> "KafkaMessage":
        """Send a request message to Kafka topic.

        Args:
            message: Message body to send.
            topic: Topic where the message will be published.
            key: A key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is `None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is `None`, partition is chosen randomly).
                Must be type `bytes`, or be serializable to bytes via configured
                `key_serializer`.
            partition: Specify a partition. If not set, the partition will be
                selected using the configured `partitioner`.
            timestamp_ms: Epoch milliseconds (from Jan 1 1970 UTC) to use as
                the message timestamp. Defaults to current time.
            headers: Message headers to store metainformation.
            correlation_id: Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            timeout: Timeout to send RPC request.

        Returns:
            KafkaMessage: The response message.
        """
        cmd = KafkaPublishCommand(
            message,
            topic=topic or self.topic,
            key=key,
            partition=partition if partition is not None else self.partition,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timestamp_ms=timestamp_ms,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )

        msg: KafkaMessage = await self._basic_request(
            cmd,
            producer=self._outer_config.producer,
        )
        return msg

    async def flush(self) -> None:
        producer = cast("AioKafkaFastProducer", self._outer_config.producer)
        await producer.flush()


class DefaultPublisher(LogicPublisher):
    def __init__(
        self,
        config: "KafkaPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self.key = config.key

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[False] = False,
    ) -> "RecordMetadata": ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[True] = ...,
    ) -> "asyncio.Future[RecordMetadata]": ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]: ...

    @override
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
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
            partition=partition if partition is not None else self.partition,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timestamp_ms=timestamp_ms,
            no_confirm=no_confirm,
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

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

        cmd.partition = cmd.partition if cmd.partition is not None else self.partition
        cmd.key = cmd.key or self.key

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        timeout: float = 0.5,
    ) -> "KafkaMessage":
        """Send a request message and wait for a response.

        Args:
            message: Message body to send.
            topic: Topic where the message will be published.
            key: A key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is `None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is `None`, partition is chosen randomly).
                Must be type `bytes`, or be serializable to bytes via configured
                `key_serializer`.
            partition: Specify a partition. If not set, the partition will be
                selected using the configured `partitioner`.
            timestamp_ms: Epoch milliseconds (from Jan 1 1970 UTC) to use as
                the message timestamp. Defaults to current time.
            headers: Message headers to store metainformation.
            correlation_id: Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            timeout: Timeout to send RPC request.

        Returns:
            The response message.
        """
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


class BatchPublisher(LogicPublisher):
    def __init__(
        self,
        config: "KafkaPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)
        self.key = config.key

    @overload
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: Literal[False] = False,
    ) -> "RecordMetadata": ...

    @overload
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: Literal[True] = ...,
    ) -> "asyncio.Future[RecordMetadata]": ...

    @overload
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: bool = False,
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]: ...

    @override
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: bool = False,
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        """Publish a message batch as a single request to broker.

        Args:
            *messages:
                Messages bodies to send.
            topic:
                Topic where the message will be published.
            key:
                A single key to associate with every message in this batch. If a
                partition is not specified and the producer uses the default
                partitioner, messages with the same key will be routed to the
                same partition. Must be bytes or serializable to bytes via the
                configured key serializer. If omitted, falls back to the
                publisher's default key (if configured).
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
            key=key or self.key,
            topic=topic or self.topic,
            partition=partition if partition is not None else self.partition,
            reply_to=reply_to or self.reply_to,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            timestamp_ms=timestamp_ms,
            no_confirm=no_confirm,
            _publish_type=PublishType.PUBLISH,
        )

        return await self._basic_publish_batch(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

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

        cmd.partition = cmd.partition if cmd.partition is not None else self.partition
        cmd.key = cmd.key or self.key

        await self._basic_publish_batch(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )
