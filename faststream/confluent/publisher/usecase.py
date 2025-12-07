import asyncio
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal, Union, cast, overload

from confluent_kafka import Message
from typing_extensions import override

from faststream._internal.endpoint.publisher import (
    PublisherSpecification,
    PublisherUsecase,
)
from faststream.confluent.response import KafkaPublishCommand
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import PublisherMiddleware
    from faststream.confluent.message import KafkaMessage
    from faststream.response.response import PublishCommand

    from .config import KafkaPublisherConfig
    from .producer import AsyncConfluentFastProducer


class LogicPublisher(PublisherUsecase):
    """A class to publish messages to a Kafka topic."""

    def __init__(
        self,
        config: "KafkaPublisherConfig",
        specifcication: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specifcication)

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
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        timeout: float = 0.5,
    ) -> "KafkaMessage":
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
        producer = cast("AsyncConfluentFastProducer", self._outer_config.producer)
        await producer.flush()


class DefaultPublisher(LogicPublisher):
    def __init__(
        self,
        config: "KafkaPublisherConfig",
        specifcication: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specifcication)

        self.key = config.key

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[True] = ...,
    ) -> asyncio.Future[Message | None]: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[False] = False,
    ) -> Message | None: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> asyncio.Future[Message | None] | Message | None: ...

    @override
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> asyncio.Future[Message | None] | Message | None:
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
        msg: asyncio.Future[Message | None] | Message | None = await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )
        return msg

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
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        timeout: float = 0.5,
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


class BatchPublisher(LogicPublisher):
    def __init__(
        self,
        config: "KafkaPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)
        self.key = config.key

    @override
    async def publish(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> None:
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

        await self._basic_publish_batch(
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
