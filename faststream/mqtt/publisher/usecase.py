from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Union

from typing_extensions import override
from zmqtt import QoS

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.mqtt.response import MQTTPublishCommand
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.mqtt.broker.config import MQTTBrokerConfig
    from faststream.response.response import PublishCommand

    from .config import MQTTPublisherConfig


class MQTTPublisher(PublisherUsecase):
    """Publisher for MQTT topics."""

    outer_config: "MQTTBrokerConfig"

    def __init__(
        self,
        config: "MQTTPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self._topic = config.topic
        self.qos = config.qos
        self.retain = config.retain
        self.headers = config.headers or {}

    @property
    def topic(self) -> str:
        return f"{self.outer_config.prefix}{self._topic}"

    @override
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        qos: QoS | None = None,
        retain: bool | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        cmd = MQTTPublishCommand(
            message,
            topic=topic or self.topic,
            qos=qos if qos is not None else self.qos,
            retain=retain if retain is not None else self.retain,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or self.outer_config.id_generator(),
            publish_type=PublishType.PUBLISH,
        )

        await self.basic_publish(
            cmd,
            producer=self.outer_config.producer,
            extra_middlewares=(),
        )

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "MQTTPublishCommand"],
        *,
        extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """Called in subscriber flow only."""
        cmd = MQTTPublishCommand.from_cmd(cmd)

        cmd.destination = cmd.destination or self.topic
        cmd.add_headers(self.headers, override=False)
        cmd.qos = cmd.qos or self.qos
        cmd.retain = cmd.retain or self.retain

        await self.basic_publish(
            cmd,
            producer=self.outer_config.producer,
            extra_middlewares=extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        correlation_id: str | None = None,
        reply_to: str = "",
        timeout: float | None = 30.0,
    ) -> Any:
        cmd = MQTTPublishCommand(
            message,
            topic=topic or self.topic,
            qos=self.qos,
            retain=self.retain,
            headers=self.headers,
            correlation_id=correlation_id or self.outer_config.id_generator(),
            reply_to=reply_to,
            timeout=timeout,
            publish_type=PublishType.REQUEST,
        )
        return await self.basic_request(
            cmd,
            producer=self.outer_config.producer,
        )
