from dataclasses import dataclass, field

from faststream._internal.configs import BrokerConfig
from faststream.mq.helpers.client import MQConnectionConfig
from faststream.mq.publisher.producer import AsyncMQFastProducer, FakeMQFastProducer


@dataclass(kw_only=True)
class MQBrokerConfig(BrokerConfig):
    connection_config: MQConnectionConfig = field(
        default_factory=lambda: MQConnectionConfig(
            queue_manager="",
            channel=None,
            conn_name=None,
        ),
    )
    producer: AsyncMQFastProducer = field(default_factory=FakeMQFastProducer)

    async def connect(self) -> None:
        await self.producer.connect(
            connection_config=self.connection_config,
            serializer=self.fd_config._serializer,
        )

    async def disconnect(self) -> None:
        await self.producer.disconnect()
