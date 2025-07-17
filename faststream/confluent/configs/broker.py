from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream.__about__ import SERVICE_NAME
from faststream._internal.configs import BrokerConfig
from faststream.confluent.helpers import (
    AdminService,
    AsyncConfluentConsumer,
    AsyncConfluentProducer,
    ConfluentFastConfig,
)
from faststream.confluent.publisher.producer import (
    AsyncConfluentFastProducer,
    FakeConfluentFastProducer,
)

if TYPE_CHECKING:
    from faststream._internal.logger import LoggerState


@dataclass
class ConsumerBuilder:
    config: "ConfluentFastConfig"
    admin: "AdminService"
    logger: "LoggerState"

    def __call__(self, *topics: str, **kwargs: Any) -> "AsyncConfluentConsumer":
        return AsyncConfluentConsumer(
            *topics,
            config=self.config,
            admin_service=self.admin,
            logger=self.logger,
            **kwargs,
        )


@dataclass(kw_only=True)
class KafkaBrokerConfig(BrokerConfig):
    connection_config: "ConfluentFastConfig" = field(
        default_factory=ConfluentFastConfig,
    )

    admin: "AdminService" = field(default_factory=AdminService)
    client_id: str | None = SERVICE_NAME

    builder: Callable[..., AsyncConfluentConsumer] = field(init=False)
    producer: "AsyncConfluentFastProducer" = field(
        default_factory=FakeConfluentFastProducer,
    )

    def __post_init__(self) -> None:
        self.builder = ConsumerBuilder(
            config=self.connection_config,
            admin=self.admin,
            logger=self.logger,
        )

    async def connect(self) -> "None":
        native_producer = AsyncConfluentProducer(
            config=self.connection_config,
            logger=self.logger,
        )
        self.producer.connect(native_producer, serializer=self.fd_config._serializer)
        await self.admin.connect(self.connection_config)

    async def disconnect(self) -> "None":
        await self.producer.disconnect()
        await self.admin.disconnect()
