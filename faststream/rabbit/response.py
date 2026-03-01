from typing import TYPE_CHECKING, Union

from typing_extensions import Unpack, override

from faststream.rabbit.schemas.exchange import RabbitExchange
from faststream.response import PublishCommand, Response
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from aio_pika.abc import TimeoutType

    from faststream.rabbit.publisher.options import (
        BasicMessageOptions,
        MessageOptions,
        PublishOptions,
    )
    from faststream.rabbit.types import AioPikaSendableMessage


class RabbitResponse(Response):
    def __init__(
        self,
        body: "AioPikaSendableMessage",
        *,
        timeout: "TimeoutType" = None,
        mandatory: bool = True,
        immediate: bool = False,
        exchange: RabbitExchange | str | None = None,
        **message_options: Unpack["MessageOptions"],
    ) -> None:
        headers = message_options.pop("headers", {})
        correlation_id = message_options.pop("correlation_id", None)

        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )

        self.exchange = None if exchange is None else RabbitExchange.validate(exchange)
        self.message_options: BasicMessageOptions = message_options
        self.publish_options: PublishOptions = {
            "mandatory": mandatory,
            "immediate": immediate,
            "timeout": timeout,
        }

    @override
    def as_publish_command(self) -> "RabbitPublishCommand":
        return RabbitPublishCommand(
            message=self.body,
            _publish_type=PublishType.PUBLISH,
            routing_key="",
            exchange=self.exchange,
            **self.publish_options,
            headers=self.headers,
            correlation_id=self.correlation_id,
            **self.message_options,
        )


class RabbitPublishCommand(PublishCommand):
    def __init__(
        self,
        message: "AioPikaSendableMessage",
        *,
        _publish_type: PublishType,
        routing_key: str = "",
        exchange: RabbitExchange | None = None,
        # publish kwargs
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        **message_options: Unpack["MessageOptions"],
    ) -> None:
        headers = message_options.pop("headers", {})
        reply_to = message_options.pop("reply_to", None) or ""
        correlation_id = message_options.pop("correlation_id", None)

        super().__init__(
            body=message,
            destination=routing_key,
            correlation_id=correlation_id,
            headers=headers,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )

        self._exchange = exchange

        self.timeout = timeout

        self.message_options: BasicMessageOptions = message_options
        self.publish_options: PublishOptions = {
            "mandatory": mandatory,
            "immediate": immediate,
        }

    @property
    def exchange(self) -> RabbitExchange:
        if self._exchange is not None:
            return self._exchange
        return RabbitExchange()

    @exchange.setter
    def exchange(self, value: RabbitExchange) -> None:
        self._exchange = value

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "RabbitPublishCommand"],
    ) -> "RabbitPublishCommand":
        if isinstance(cmd, RabbitPublishCommand):
            # NOTE: Should return a copy probably.
            return cmd

        return cls(
            message=cmd.body,
            routing_key=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )
