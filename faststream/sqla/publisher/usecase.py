from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union

from sqlalchemy.ext.asyncio import AsyncConnection
from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.exceptions import FeatureNotSupportedException
from faststream.sqla.response import SqlaPublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand
    from faststream.sqla.configs.broker import SqlaBrokerConfig
    from faststream.sqla.publisher.config import SqlaPublisherConfig


class LogicPublisher(PublisherUsecase):
    _outer_config: "SqlaBrokerConfig"

    def __init__(
        self,
        config: "SqlaPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)
        self._queue = config.queue
        self.headers = config.headers or {}

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self._queue}"

    @override
    async def publish(
        self,
        message: "SendableMessage",
        queue: str = "",
        headers: dict[str, str] | None = None,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
        correlation_id: str | None = None,
    ) -> None:
        cmd = SqlaPublishCommand(
            message,
            queue=queue or self.queue,
            headers=self.headers | (headers or {}),
            next_attempt_at=next_attempt_at,
            connection=connection,
        )

        return await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "SqlaPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        cmd = SqlaPublishCommand.from_cmd(cmd)

        cmd.destination = self.queue
        cmd.add_headers(self.headers, override=False)

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage",
        queue: str,
        next_attempt_at: datetime | None = None,
    ) -> None:
        raise FeatureNotSupportedException
