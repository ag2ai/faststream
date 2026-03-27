from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.message import gen_cor_id
from faststream.mq.response import MQPublishCommand
from faststream.mq.schemas import MQQueue
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand

    from .config import MQPublisherConfig


class MQPublisher(PublisherUsecase):
    def __init__(
        self,
        config: "MQPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self.queue = config.queue
        self.headers = config.headers
        self.reply_to = config.reply_to
        self.reply_to_qmgr = config.reply_to_qmgr
        self.priority = config.priority
        self.persistence = config.persistence
        self.expiry = config.expiry
        self.message_type = config.message_type

    def routing(self, *, queue: "MQQueue | str | None" = None) -> str:
        if queue is None:
            return self._outer_config.prefix + self.queue.routing()
        return self._outer_config.prefix + MQQueue.validate(queue).routing()

    @override
    async def publish(
        self,
        message: Any,
        queue: "MQQueue | str | None" = None,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        message_id: str | None = None,
    ) -> None:
        cmd = MQPublishCommand(
            message,
            destination=self.routing(queue=queue),
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            message_id=message_id,
            reply_to=reply_to or self.reply_to,
            reply_to_qmgr=reply_to_qmgr or self.reply_to_qmgr,
            priority=priority if priority is not None else self.priority,
            persistence=persistence if persistence is not None else self.persistence,
            expiry=expiry if expiry is not None else self.expiry,
            message_type=message_type or self.message_type,
            _publish_type=PublishType.PUBLISH,
        )
        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    @override
    async def _publish(
        self,
        cmd: "MQPublishCommand | PublishCommand",
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        real_cmd = MQPublishCommand.from_cmd(cmd)
        real_cmd.destination = self.routing()
        real_cmd.reply_to = real_cmd.reply_to or self.reply_to
        real_cmd.reply_to_qmgr = real_cmd.reply_to_qmgr or self.reply_to_qmgr
        real_cmd.priority = (
            self.priority if real_cmd.priority is None else real_cmd.priority
        )
        real_cmd.persistence = (
            self.persistence if real_cmd.persistence is None else real_cmd.persistence
        )
        real_cmd.expiry = self.expiry if real_cmd.expiry is None else real_cmd.expiry
        real_cmd.message_type = real_cmd.message_type or self.message_type
        real_cmd.add_headers(self.headers, override=False)

        await self._basic_publish(
            real_cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: Any,
        queue: "MQQueue | str | None" = None,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        message_id: str | None = None,
        timeout: float = 0.5,
    ) -> Any:
        cmd = MQPublishCommand(
            message,
            destination=self.routing(queue=queue),
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            message_id=message_id,
            reply_to=reply_to or self.reply_to,
            reply_to_qmgr=reply_to_qmgr or self.reply_to_qmgr,
            priority=priority if priority is not None else self.priority,
            persistence=persistence if persistence is not None else self.persistence,
            expiry=expiry if expiry is not None else self.expiry,
            message_type=message_type or self.message_type,
            timeout=timeout,
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_request(
            cmd,
            producer=self._outer_config.producer,
        )
