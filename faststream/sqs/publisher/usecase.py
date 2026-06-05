from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Union

from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from faststream.sqs.response import SQSPublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand
    from faststream.sqs.broker.config import SQSBrokerConfig

    from .config import SQSPublisherConfig


class SQSPublisher(PublisherUsecase):
    """Publisher for an SQS queue."""

    _outer_config: "SQSBrokerConfig"

    def __init__(
        self,
        config: "SQSPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self.queue = config.queue
        self.headers = config.headers or {}
        self.group_id = config.group_id
        self.deduplication_id = config.deduplication_id
        self.delay_seconds = config.delay_seconds

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
            delay_seconds=delay_seconds if delay_seconds is not None else self.delay_seconds,
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

    @override
    async def request(
        self,
        message: "SendableMessage",
        queue: str = "",
        *,
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = 30.0,
    ) -> Any:
        cmd = SQSPublishCommand(
            message,
            queue=queue or self.queue,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            group_id=self.group_id,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )
        return await self._basic_request(cmd, producer=self._outer_config.producer)
