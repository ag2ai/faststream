from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

from typing_extensions import override

from faststream._internal._compat import HAS_OPENTELEMETRY
from faststream._internal.configs import BrokerConfig, UnderlyingDriverAnnotation
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.mqtt.parser import MQTTVersion
from faststream.mqtt.publisher.producer import ZmqttFakeProducer

if TYPE_CHECKING:
    from collections.abc import Mapping

    import zmqtt

    from faststream._internal.types import BrokerMiddleware
    from faststream.mqtt.publisher.producer import ZmqttBaseProducer

if HAS_OPENTELEMETRY:
    from faststream.opentelemetry.middleware import TelemetryMiddleware


MQTTVersionUnset = cast("str", object())


def _context_annotations() -> "Mapping[Any, Any]":
    # `annotations` reaches this module through the broker, so the
    # objects a row needs only exist once the package is built.
    from zmqtt.client import MQTTClient

    from faststream.mqtt import annotations
    from faststream.mqtt.broker.broker import MQTTBroker as MQTTBrokerDriver
    from faststream.mqtt.message import MQTTMessage as MQTTMessageDriver

    return MappingProxyType(
        {
            MQTTClient: UnderlyingDriverAnnotation(
                annotations.Client, "faststream.mqtt.annotations", "Client"
            ),
            MQTTBrokerDriver: UnderlyingDriverAnnotation(
                annotations.MQTTBroker, "faststream.mqtt.annotations", "MQTTBroker"
            ),
            MQTTMessageDriver: UnderlyingDriverAnnotation(
                annotations.MQTTMessage, "faststream.mqtt.annotations", "MQTTMessage"
            ),
        },
    )


@dataclass(kw_only=True)
class MQTTBrokerConfig(BrokerConfig):
    version: MQTTVersion | Literal["unset"] = "unset"

    producer: "ZmqttBaseProducer" = field(default_factory=ZmqttFakeProducer)
    _client: Optional["zmqtt.MQTTClient"] = field(default=None, init=False, repr=False)

    @override
    def _default_driver_annotations(self) -> "Mapping[Any, Any]":
        return _context_annotations()

    def __post_init__(self) -> None:
        super().__post_init__()

        for m in self.broker_middlewares:
            self._validate_middleware(m)

    @property
    def client(self) -> "zmqtt.MQTTClient":
        if self._client is None:
            msg = "MQTT broker is not connected. Call connect() first."
            raise IncorrectState(msg)
        return self._client

    def connect(self, client: "zmqtt.MQTTClient") -> None:
        self._client = client
        self.producer.connect(
            client, self.fd_config._serializer, codec=self.broker_codec or DefaultCodec()
        )

    def disconnect(self) -> None:
        self._client = None
        self.producer.disconnect()

    def add_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        self._validate_middleware(middleware)
        return super().add_middleware(middleware)

    def insert_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        self._validate_middleware(middleware)
        return super().insert_middleware(middleware)

    def _validate_middleware(self, middleware: "BrokerMiddleware[Any]") -> None:
        if (
            HAS_OPENTELEMETRY
            and self.version == "3.1.1"
            and isinstance(middleware, TelemetryMiddleware)
        ):
            msg = "Opentelementry don`t work in 3.1.1 mqtt"
            raise FeatureNotSupportedException(msg)
