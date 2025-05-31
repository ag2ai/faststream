from collections.abc import Iterable
from typing import TYPE_CHECKING, Annotated, Optional, Union

from aio_pika import IncomingMessage
from typing_extensions import Doc, Unpack, override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream._internal.utils.data import filter_by_dict
from faststream.message import gen_cor_id
from faststream.rabbit.configs import RabbitPublisherConfig
from faststream.rabbit.response import RabbitPublishCommand
from faststream.rabbit.schemas import RabbitExchange, RabbitQueue
from faststream.response.publish_type import PublishType

from .options import MessageOptions, PublishOptions

if TYPE_CHECKING:
    import aiormq

    from faststream._internal.types import PublisherMiddleware
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.message import RabbitMessage
    from faststream.rabbit.types import AioPikaSendableMessage
    from faststream.response.response import PublishCommand


# should be public to use in imports
class RequestPublishKwargs(MessageOptions, PublishOptions, total=False):
    """Typed dict to annotate RabbitMQ requesters."""


class PublishKwargs(MessageOptions, PublishOptions, total=False):
    """Typed dict to annotate RabbitMQ publishers."""

    reply_to: Annotated[
        Optional[str],
        Doc(
            "Reply message routing key to send with (always sending to default exchange).",
        ),
    ]


class LogicPublisher(PublisherUsecase[IncomingMessage]):
    """A class to represent a RabbitMQ publisher."""

    _outer_config: "RabbitBrokerConfig"

    def __init__(self, config: RabbitPublisherConfig, /) -> None:
        super().__init__(config)

        self.queue = config.queue
        self.routing_key = config.routing_key

        self.exchange = config.exchange

        self.headers = config.message_kwargs.pop("headers") or {}
        self.reply_to: str = config.message_kwargs.pop("reply_to", None) or ""
        self.timeout = config.message_kwargs.pop("timeout", None)

        message_options, _ = filter_by_dict(MessageOptions, dict(config.message_kwargs))
        self._message_options = message_options

        publish_options, _ = filter_by_dict(PublishOptions, dict(config.message_kwargs))
        self.publish_options = publish_options

    @property
    def message_options(self) -> "MessageOptions":
        if self._outer_config.app_id and "app_id" not in self._message_options:
            message_options = self._message_options.copy()
            message_options["app_id"] = self._outer_config.app_id
            return message_options

        return self._message_options

    def routing(
        self,
        *,
        queue: Union["RabbitQueue", str, None] = None,
        routing_key: str = "",
    ) -> str:
        if not routing_key:
            if q := RabbitQueue.validate(queue):
                routing_key = q.routing()
            else:
                routing_key = self.routing_key or self.queue.routing()

            routing_key = f"{self._outer_config.prefix}{routing_key}"

        return routing_key

    async def start(self) -> None:
        if self.exchange is not None:
            await self._outer_config.declarer.declare_exchange(self.exchange)
        return await super().start()

    @override
    async def publish(
        self,
        message: "AioPikaSendableMessage",
        queue: Union["RabbitQueue", str, None] = None,
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        routing_key: str = "",
        # message args
        correlation_id: Optional[str] = None,
        # publisher specific
        **publish_kwargs: "Unpack[PublishKwargs]",
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]:
        headers = self.headers | publish_kwargs.pop("headers", {})
        cmd = RabbitPublishCommand(
            message,
            routing_key=self.routing(queue=queue, routing_key=routing_key),
            exchange=RabbitExchange.validate(exchange or self.exchange),
            correlation_id=correlation_id or gen_cor_id(),
            headers=headers,
            _publish_type=PublishType.PUBLISH,
            **(self.publish_options | self.message_options | publish_kwargs),
        )

        frame: Optional[aiormq.abc.ConfirmationFrameType] = await self._basic_publish(
            cmd,
            _extra_middlewares=(),
        )
        return frame

    @override
    async def _publish(
        self,
        cmd: Union["RabbitPublishCommand", "PublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = RabbitPublishCommand.from_cmd(cmd)

        cmd.destination = self.routing()
        cmd.reply_to = cmd.reply_to or self.reply_to
        cmd.add_headers(self.headers, override=False)

        cmd.timeout = cmd.timeout or self.timeout

        cmd.message_options = {**self.message_options, **cmd.message_options}
        cmd.publish_options = {**self.publish_options, **cmd.publish_options}

        await self._basic_publish(cmd, _extra_middlewares=_extra_middlewares)

    @override
    async def request(
        self,
        message: "AioPikaSendableMessage",
        queue: Union["RabbitQueue", str, None] = None,
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        routing_key: str = "",
        correlation_id: Optional[str] = None,
        **publish_kwargs: "Unpack[RequestPublishKwargs]",
    ) -> "RabbitMessage":
        headers = self.headers | publish_kwargs.pop("headers", {})
        cmd = RabbitPublishCommand(
            message,
            routing_key=self.routing(queue=queue, routing_key=routing_key),
            exchange=RabbitExchange.validate(exchange or self.exchange),
            correlation_id=correlation_id or gen_cor_id(),
            headers=headers,
            _publish_type=PublishType.PUBLISH,
            **(self.publish_options | self.message_options | publish_kwargs),
        )

        msg: RabbitMessage = await self._basic_request(cmd)
        return msg
