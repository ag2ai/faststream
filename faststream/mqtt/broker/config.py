from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Optional

from faststream._internal.configs import BrokerConfig
from faststream.exceptions import IncorrectState
from faststream.mqtt.publisher.producer import ZmqttFakeProducer

if TYPE_CHECKING:
    import zmqtt

    from faststream.mqtt.publisher.producer import ZmqttBaseProducer


@dataclass(kw_only=True)
class MQTTBrokerConfig(BrokerConfig):
    version: Literal["3.1.1", "5.0"] = "3.1.1"

    producer: "ZmqttBaseProducer" = field(default_factory=ZmqttFakeProducer)
    _client: Optional["zmqtt.MQTTClient"] = field(default=None, init=False, repr=False)

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
