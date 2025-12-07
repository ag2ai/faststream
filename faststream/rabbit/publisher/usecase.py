from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import Unpack, override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream._internal.utils.data import filter_by_dict
from faststream.message import gen_cor_id
from faststream.rabbit.response import RabbitPublishCommand
from faststream.rabbit.schemas import RabbitExchange, RabbitQueue
from faststream.response.publish_type import PublishType

from .options import BasicMessageOptions, PublishKwargs, PublishOptions

if TYPE_CHECKING:
    import aiormq

    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.message import RabbitMessage
    from faststream.rabbit.types import AioPikaSendableMessage
    from faststream.response.response import PublishCommand

    from .config import RabbitPublisherConfig


class RabbitPublisher(PublisherUsecase):
    """A class to represent a RabbitMQ publisher."""

    _outer_config: "RabbitBrokerConfig"

    def __init__(
        self,
        config: "RabbitPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self.queue = config.queue
        self.routing_key = config.routing_key

        self.exchange = config.exchange

        self.headers = config.message_kwargs.pop("headers") or {}
        self.reply_to = config.message_kwargs.pop("reply_to", None) or ""
        self.timeout = config.message_kwargs.pop("timeout", None)

        message_options, _ = filter_by_dict(
            BasicMessageOptions,
            dict(config.message_kwargs),
        )
        self._message_options = message_options

        publish_options, _ = filter_by_dict(PublishOptions, dict(config.message_kwargs))
        self.publish_options = publish_options

    @property
    def message_options(self) -> "BasicMessageOptions":
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
                r = self.routing_key or self.queue.routing()
                routing_key = f"{self._outer_config.prefix}{r}"

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
        **publish_kwargs: "Unpack[PublishKwargs]",
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]:
        if "headers" in publish_kwargs:
            headers = self.headers | (publish_kwargs.pop("headers") or {})
        else:
            headers = self.headers

        correlation_id = publish_kwargs.pop("correlation_id", gen_cor_id())

        cmd = RabbitPublishCommand(
            message,
            routing_key=self.routing(queue=queue, routing_key=routing_key),
            exchange=RabbitExchange.validate(exchange or self.exchange),
            headers=headers,
            correlation_id=correlation_id,
            _publish_type=PublishType.PUBLISH,
            **(self.publish_options | self.message_options | publish_kwargs),  # type: ignore[operator]
        )

        frame: aiormq.abc.ConfirmationFrameType | None = await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
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

        cmd.exchange = RabbitExchange.validate(cmd._exchange or self.exchange)

        cmd.destination = self.routing()
        cmd.reply_to = cmd.reply_to or self.reply_to
        cmd.add_headers(self.headers, override=False)

        cmd.timeout = cmd.timeout or self.timeout

        cmd.message_options = {**self.message_options, **cmd.message_options}
        cmd.publish_options = {**self.publish_options, **cmd.publish_options}

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "AioPikaSendableMessage",
        queue: Union["RabbitQueue", str, None] = None,
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        routing_key: str = "",
        **publish_kwargs: "Unpack[PublishKwargs]",
    ) -> "RabbitMessage":
        if "headers" in publish_kwargs:
            headers = self.headers | (publish_kwargs.pop("headers") or {})
        else:
            headers = self.headers

        correlation_id = publish_kwargs.pop("correlation_id", gen_cor_id())

        cmd = RabbitPublishCommand(
            message,
            routing_key=self.routing(queue=queue, routing_key=routing_key),
            exchange=RabbitExchange.validate(exchange or self.exchange),
            correlation_id=correlation_id,
            headers=headers,
            _publish_type=PublishType.PUBLISH,
            **(self.publish_options | self.message_options | publish_kwargs),  # type: ignore[operator]
        )

        msg: RabbitMessage = await self._basic_request(
            cmd,
            producer=self._outer_config.producer,
        )
        return msg
