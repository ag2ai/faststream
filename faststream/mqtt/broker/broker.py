import logging
import ssl
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    Optional,
)

import zmqtt
from fast_depends import Provider, dependency_provider
from typing_extensions import deprecated, override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.exceptions import FeatureNotSupportedException
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
        host: str = "localhost",
        port: int = 1883,
        *,
        client_id: str = "",
        username: str | None = None,
        password: str | None = None,
        tls: ssl.SSLContext | bool = False,
        keepalive: int = 60,
        clean_session: bool = True,
        version: Literal["3.1.1", "5.0"] = "3.1.1",
        reconnect: zmqtt.ReconnectConfig | None = None,
        session_expiry_interval: int = 0,
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[MQTTRegistrator] = (),
        specification_url: str | None = None,
        protocol_version: str | None = None,
        description: str | None = None,
        tags: Iterable["Tag | TagDict"] = (),
        security: Optional["BaseSecurity"] = None,
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        secure_kwargs = parse_security(security)

        self._host = host
        self._port = port
        self._client_id = client_id
        self._username = secure_kwargs.get("username", username)
        self._password = secure_kwargs.get("password", password)
        self._tls = secure_kwargs.get("tls", tls)
        self._keepalive = keepalive
        self._clean_session = clean_session
        self._version: Final = version
        self._reconnect = reconnect
        self._session_expiry_interval = session_expiry_interval

        producer: ZmqttBaseProducer
        if version == "5.0":
            producer = ZmqttProducerV5(parser=parser, decoder=decoder)
        else:
            producer = ZmqttProducerV311(parser=parser, decoder=decoder)

        if specification_url is None:
            specification_url = f"mqtt://{host}:{port}"

        super().__init__(
            routers=routers,
            config=MQTTBrokerConfig(
                version=version,
                producer=producer,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
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
        client = zmqtt.MQTTClient(
            self._host,
            self._port,
            client_id=self._client_id,
            username=self._username,
            password=self._password,
            tls=self._tls,
            keepalive=self._keepalive,
            clean_session=self._clean_session,
            version=self._version,
            reconnect=self._reconnect,
            session_expiry_interval=self._session_expiry_interval,
        )
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

    @deprecated(
        "Deprecated in **FastStream 0.5.44**. "
        "Please, use `stop` method instead. "
        "Method `close` will be removed in **FastStream 0.7.0**.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    async def close(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await self.stop(exc_type, exc_val, exc_tb)

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

    def _check_v311_features(
        self,
        headers: "dict[str, str] | None",
        correlation_id: "str | None",
    ) -> None:
        if headers:
            msg = "MQTT 3.1.1 does not support message headers. Use MQTT 5.0."
            raise FeatureNotSupportedException(msg)
        if correlation_id is not None:
            msg = "MQTT 3.1.1 does not support correlation_id. Use MQTT 5.0."
            raise FeatureNotSupportedException(msg)

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
        if self._version == "3.1.1":
            self._check_v311_features(headers, correlation_id)
        cmd = MQTTPublishCommand(
            message,
            topic=topic,
            qos=qos,
            retain=retain,
            headers=headers,
            correlation_id=correlation_id,
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
        if self._version == "3.1.1":
            self._check_v311_features(headers, correlation_id)
        cmd = MQTTPublishCommand(
            message,
            topic=topic,
            correlation_id=correlation_id,
            headers=headers,
            qos=qos,
            reply_to=reply_to,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )
        msg: MQTTMessage = await self._basic_request(cmd, producer=self.config.producer)
        return msg
