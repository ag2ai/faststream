from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import aiomqtt
from paho.mqtt.client import MQTT_CLEAN_START_FIRST_ONLY, CleanStartOption

from faststream._internal.configs import BrokerConfig
from faststream.mqtt.publisher.producer import AiomqttFastProducer

if TYPE_CHECKING:
    from types import TracebackType


@dataclass(kw_only=True)
class MQTTBrokerConfig(BrokerConfig):
    hostname: str
    port: int = 1883
    username: str | None = None
    password: str | None = None
    keepalive: int = 60
    bind_address: str = ""
    bind_port: int = 0
    clean_start: CleanStartOption = MQTT_CLEAN_START_FIRST_ONLY
    __client: aiomqtt.Client | None = field(init=False, default=None)

    async def connect(self) -> aiomqtt.Client:
        return await self.client.__aenter__()

    async def disconnect(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: "TracebackType | None" = None,
    ) -> None:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def client(self) -> aiomqtt.Client:
        self.__client = self.__client or self.__get_client()
        self.producer = AiomqttFastProducer(self.__client)
        return self.__client

    def create_client(self) -> aiomqtt.Client:
        return self.__get_client()

    def __get_client(self) -> aiomqtt.Client:
        return aiomqtt.Client(
            self.hostname,
            self.port,
            username=self.username,
            password=self.password,
            keepalive=self.keepalive,
            bind_address=self.bind_address,
            bind_port=self.bind_port,
            clean_start=self.clean_start,
            protocol=aiomqtt.ProtocolVersion.V5,
        )
