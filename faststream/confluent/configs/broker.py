from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from faststream.__about__ import SERVICE_NAME
from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
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
    from faststream.confluent.schemas import Topic


# Driver class to the context annotation that injects it, both as import
# paths so this table needs no imports of its own.
CONTEXT_ANNOTATIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "faststream.confluent.helpers.client.AsyncConfluentConsumer": "faststream.confluent.annotations.Consumer",
        "faststream.confluent.broker.broker.KafkaBroker": "faststream.confluent.annotations.KafkaBroker",
        "faststream.confluent.message.KafkaMessage": "faststream.confluent.annotations.KafkaMessage",
        "faststream.confluent.publisher.producer.AsyncConfluentFastProducer": "faststream.confluent.annotations.KafkaProducer",
    },
)


@dataclass
class ConsumerBuilder:
    config: "ConfluentFastConfig"
    admin: "AdminService"
    logger: "LoggerState"

    def __call__(self, *topics: "Topic", **kwargs: Any) -> "AsyncConfluentConsumer":
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

    underlying_driver_annotations: "Mapping[str, str]" = CONTEXT_ANNOTATIONS

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
        self.producer.connect(
            native_producer,
            serializer=self.fd_config._serializer,
            codec=self.broker_codec or DefaultCodec(),
        )
        await self.admin.connect(
            self.connection_config,
            logger=self.logger,
        )

    async def disconnect(self) -> "None":
        await self.producer.disconnect()
        await self.admin.disconnect()
