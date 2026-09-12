from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.configs import BrokerConfig, UnderlyingDriverAnnotation
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


def _context_annotations() -> "Mapping[Any, Any]":
    # `annotations` reaches this module through the broker, so the
    # objects a row needs only exist once the package is built.
    from faststream.confluent import annotations
    from faststream.confluent.broker.broker import KafkaBroker as KafkaBrokerDriver
    from faststream.confluent.helpers.client import AsyncConfluentConsumer
    from faststream.confluent.message import KafkaMessage as KafkaMessageDriver
    from faststream.confluent.publisher.producer import AsyncConfluentFastProducer

    return MappingProxyType(
        {
            AsyncConfluentConsumer: UnderlyingDriverAnnotation(
                annotations.Consumer, "faststream.confluent.annotations", "Consumer"
            ),
            KafkaBrokerDriver: UnderlyingDriverAnnotation(
                annotations.KafkaBroker, "faststream.confluent.annotations", "KafkaBroker"
            ),
            KafkaMessageDriver: UnderlyingDriverAnnotation(
                annotations.KafkaMessage,
                "faststream.confluent.annotations",
                "KafkaMessage",
            ),
            AsyncConfluentFastProducer: UnderlyingDriverAnnotation(
                annotations.KafkaProducer,
                "faststream.confluent.annotations",
                "KafkaProducer",
            ),
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

    @override
    def _default_driver_annotations(self) -> "Mapping[Any, Any]":
        return _context_annotations()

    def __post_init__(self) -> None:
        super().__post_init__()

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
