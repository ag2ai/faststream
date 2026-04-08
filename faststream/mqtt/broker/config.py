from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

from faststream._internal.configs import BrokerConfig
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.mqtt.publisher.producer import ZmqttFakeProducer
from faststream.opentelemetry.middleware import TelemetryMiddleware

if TYPE_CHECKING:
    import zmqtt

    from faststream._internal.types import BrokerMiddleware
    from faststream.mqtt.publisher.producer import ZmqttBaseProducer


MQTTVersionUnset = cast("str", object())


@dataclass(kw_only=True)
class MQTTBrokerConfig(BrokerConfig):
    version: Literal["3.1.1", "5.0", "unset"] = "unset"

    producer: "ZmqttBaseProducer" = field(default_factory=ZmqttFakeProducer)
    _client: Optional["zmqtt.MQTTClient"] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
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
        self.producer.connect(client, self.fd_config._serializer)

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
        if self.version == "3.1.1" and isinstance(middleware, TelemetryMiddleware):
            msg = "Opentelementry don`t work in 3.1.1 mqtt"
            raise FeatureNotSupportedException(msg)
