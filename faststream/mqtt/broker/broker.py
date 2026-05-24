import logging
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
)

import zmqtt
from fast_depends import Provider, dependency_provider
from typing_extensions import override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.middlewares import AckPolicy
from faststream.mqtt.broker.config import MQTTBrokerConfig
from faststream.mqtt.publisher.producer import (
    ZmqttBaseProducer,
    ZmqttProducerV5,
    ZmqttProducerV311,
)
from faststream.mqtt.response import MQTTPublishCommand
from faststream.mqtt.security import parse_security
from faststream.mqtt.subscriber.usecase import MQTTBaseSubscriber
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_mqtt_logger_state
from .registrator import MQTTRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.mqtt.message import MQTTMessage
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


class MQTTBroker(
    MQTTRegistrator,
    BrokerUsecase[zmqtt.Message, zmqtt.MQTTClient],
):
    """MQTT broker for FastStream using the zmqtt client library."""

    def __init__(
        self,
        host: str = "localhost:1883",
        port: int = EMPTY,
        *,
        client_id: str = "",
        keepalive: int = 60,
        clean_session: bool = True,
        version: Literal["3.1.1", "5.0"] = "5.0",
        reconnect: zmqtt.ReconnectConfig | None = None,
        session_expiry_interval: int = 0,
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[MQTTRegistrator] = (),
        ack_policy: AckPolicy = EMPTY,
        # AsyncAPI args
        specification_url: str | None = None,
        protocol_version: str | None = None,
        description: str | None = None,
        tags: Iterable["Tag | TagDict"] = (),
        security: Optional["BaseSecurity"] = None,
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        secure_kwargs = parse_security(security)

        producer: ZmqttBaseProducer
        if version == "5.0":
            producer = ZmqttProducerV5(parser=parser, decoder=decoder)
        else:
            producer = ZmqttProducerV311(parser=parser, decoder=decoder)

        if ":" in host:
            host, p = host.split(":", 2)
        else:
            p = "1883"
        if port is EMPTY:
            port = int(p)

        if specification_url is None:
            specification_url = f"mqtt://{host}:{port}"

        super().__init__(
            host=host,
            port=port,
            client_id=client_id,
            keepalive=keepalive,
            clean_session=clean_session,
            version=version,
            reconnect=reconnect,
            session_expiry_interval=session_expiry_interval,
            **secure_kwargs,
            # broker config
            routers=routers,
            config=MQTTBrokerConfig(
                version=version,
                producer=producer,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_mqtt_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                extra_context={
                    "broker": self,
                },
            ),
            specification=BrokerSpec(
                description=description,
                url=[specification_url],
                protocol="mqtt",
                protocol_version=protocol_version or version,
                tags=tags,
                security=security,
            ),
        )

    @override
    async def _connect(self) -> zmqtt.MQTTClient:
        client = zmqtt.MQTTClient(**self._connection_kwargs)
        await client.connect()
        self.config.connect(client)
        return client

    @override
    async def start(self) -> None:
        await self.connect()
        c = MQTTBaseSubscriber.build_log_context(None, "")
        self.config.logger.log("Connection established", logging.INFO, c)
        await super().start()

    @override
    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)

        if self._connection is not None:
            await self._connection.disconnect()
            self._connection = None

        self.config.disconnect()

    @override
    async def ping(self, timeout: float | None = None) -> bool:
        if self._connection is None:
            return False
        try:
            await self._connection.ping(timeout=timeout or 5.0)
        except Exception:
            return False
        else:
            return True

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        topic: str = "",
        *,
        qos: zmqtt.QoS = zmqtt.QoS.AT_MOST_ONCE,
        retain: bool = False,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
    ) -> None:
        """Publish a message to an MQTT topic.

        Args:
            message: Message body to send.
            topic: MQTT topic to publish to.
            qos: QoS level (0, 1, or 2).
            retain: Whether the broker should retain the message.
            headers: Message headers (MQTT 5.0 user properties).
            correlation_id: Correlation ID for message tracing.
            reply_to: Response topic (MQTT 5.0 response_topic property).
        """
        cmd = MQTTPublishCommand(
            message,
            topic=topic,
            qos=qos,
            retain=retain,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            reply_to=reply_to,
            _publish_type=PublishType.PUBLISH,
        )

        await self._basic_publish(cmd, producer=self.config.producer)

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        topic: str = "",
        /,
        timeout: float = 0.5,
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        qos: zmqtt.QoS = zmqtt.QoS.AT_MOST_ONCE,
        reply_to: str = "",
    ) -> "MQTTMessage":
        cmd = MQTTPublishCommand(
            message,
            topic=topic,
            correlation_id=correlation_id or gen_cor_id(),
            headers=headers,
            qos=qos,
            reply_to=reply_to,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )
        msg: MQTTMessage = await self._basic_request(cmd, producer=self.config.producer)
        return msg
