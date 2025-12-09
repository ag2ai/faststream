from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional

import aiomqtt
import anyio
from fast_depends import dependency_provider
from typing_extensions import override

from faststream._internal.basic_types import SendableMessage
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.mqtt.broker.registrator import MQTTRegistrator
from faststream.mqtt.configs.broker import MQTTBrokerConfig
from faststream.mqtt.response import MQTTPublishCommand
from faststream.response import PublishType
from faststream.security import BaseSecurity
from faststream.specification.schema import BrokerSpec, Tag, TagDict

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends import Provider
    from fast_depends.library.serializer import SerializerProto


class MQTTBroker(
    MQTTRegistrator,
    BrokerUsecase[aiomqtt.Message, aiomqtt.Client],
):
    def __init__(
        self,
        *,
        # mqtt broker params
        hostname: str = "localhost",
        port: int = 1883,
        username: str | None = None,
        password: str | None = None,
        keepalive: int = 60,
        bind_address: str = "",
        bind_port: int = 0,
        routers: Iterable[MQTTRegistrator] = (),
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "auto",
        description: str | None = None,
        tags: Iterable["Tag | TagDict"] = (),
    ) -> None:
        if specification_url is None:
            specification_url = hostname
        super().__init__(
            config=MQTTBrokerConfig(
                hostname="localhost",
                port=port,
                username=username,
                password=password,
                keepalive=keepalive,
                bind_address=bind_address,
                bind_port=bind_port,
                extra_context={
                    "broker": self,
                },
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
            ),
            specification=BrokerSpec(
                url=[specification_url],
                protocol=None,
                protocol_version=None,
                description="MQTT Broker",
                tags=[],
                security=None,
            ),
            routers=routers,
        )

    @override
    async def _connect(self) -> aiomqtt.Client:
        return await self.config.broker_config.connect()

    async def start(self) -> None:
        await self.connect()
        await super().start()

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.broker_config.disconnect(exc_type, exc_val, exc_tb)
        self._connection = None

    async def publish(self, message: "SendableMessage", topic: str) -> Any:
        cmd = MQTTPublishCommand(
            body=message,
            destination=topic,
            _publish_type=PublishType.PUBLISH,
        )
        return await super()._basic_publish(
            cmd, producer=self.config.broker_config.producer
        )

    async def ping(self, timeout: float | None) -> bool:
        sleep_time = (timeout or 10) / 10
        ping_client = self.config.broker_config.create_client()

        with anyio.move_on_after(timeout) as cancel_scope:
            while True:
                if cancel_scope.cancel_called:
                    return False
                try:
                    async with ping_client:
                        pass
                except aiomqtt.MqttError:
                    await anyio.sleep(sleep_time)
                else:
                    return True
        return False
