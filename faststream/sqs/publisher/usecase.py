from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Union, cast

from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from faststream.sqs.response import SQSBatchPublishCommand, SQSPublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand
    from faststream.sqs.configs import SQSBrokerConfig
    from faststream.sqs.message import SQSMessage

    from .config import SQSPublisherConfig


class SQSPublisher(PublisherUsecase):
    """Base SQS publisher (see ``SQSDefaultPublisher`` / ``SQSBatchPublisher``)."""

    _outer_config: "SQSBrokerConfig"

    def __init__(
        self,
        config: "SQSPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self._queue = config.queue
        self.headers = config.headers or {}
        self.group_id = config.group_id
        self.deduplication_id = config.deduplication_id
        self.delay_seconds = config.delay_seconds

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self._queue}"

    @override
    async def request(
        self,
        message: "SendableMessage",
        queue: str = "",
        *,
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = 30.0,
    ) -> "SQSMessage":
        cmd = SQSPublishCommand(
            message,
            queue=queue or self.queue,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            group_id=self.group_id,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )
        return cast(
            "SQSMessage",
            await self._basic_request(cmd, producer=self._outer_config.producer),
        )


class SQSDefaultPublisher(SQSPublisher):
    """Publisher sending one message per ``SendMessage`` request."""

    @override
    async def publish(
        self,
        message: "SendableMessage",
        queue: str = "",
        *,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        group_id: str | None = None,
        deduplication_id: str | None = None,
        delay_seconds: int | None = None,
    ) -> Any:
        cmd = SQSPublishCommand(
            message,
            queue=queue or self.queue,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            group_id=group_id or self.group_id,
            deduplication_id=deduplication_id or self.deduplication_id,
            delay_seconds=delay_seconds
            if delay_seconds is not None
            else self.delay_seconds,
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
        cmd: Union["PublishCommand", "SQSPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """Called in the subscriber (reply-to) flow only."""
        cmd = SQSPublishCommand.from_cmd(cmd)
        cmd.destination = cmd.destination or self.queue
        cmd.add_headers(self.headers, override=False)
        cmd.group_id = cmd.group_id or self.group_id

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )


class SQSBatchPublisher(SQSPublisher):
    """Publisher sending several messages as one ``SendMessageBatch`` request."""

    @override
    async def publish(
        self,
        *messages: "SendableMessage",
        queue: str = "",
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        group_id: str | None = None,
        deduplication_id: str | None = None,
    ) -> Any:
        cmd = SQSBatchPublishCommand(
            *messages,
            queue=queue or self.queue,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            group_id=group_id or self.group_id,
            deduplication_id=deduplication_id or self.deduplication_id,
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
        cmd: Union["PublishCommand", "SQSPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """Called in the subscriber (reply-to / decorator) flow only."""
        batch_cmd = SQSBatchPublishCommand.from_cmd(cmd, batch=True)
        batch_cmd.destination = batch_cmd.destination or self.queue
        batch_cmd.add_headers(self.headers, override=False)
        batch_cmd.group_id = batch_cmd.group_id or self.group_id
        batch_cmd.deduplication_id = batch_cmd.deduplication_id or self.deduplication_id

        await self._basic_publish_batch(
            batch_cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )
