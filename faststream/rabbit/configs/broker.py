from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import BrokerConfig, UnderlyingDriverAnnotation
from faststream._internal.parser import DefaultCodec
from faststream.rabbit.helpers.channel_manager import FakeChannelManager
from faststream.rabbit.helpers.declarer import FakeRabbitDeclarer
from faststream.rabbit.publisher.producer import FakeAioPikaFastProducer

if TYPE_CHECKING:
    from collections.abc import Mapping

    from aio_pika import RobustConnection

    from faststream.rabbit.helpers import ChannelManager, RabbitDeclarer
    from faststream.rabbit.publisher.producer import AioPikaFastProducer


def _context_annotations_factory() -> "Mapping[Any, Any]":
    # `annotations` reaches this module through the broker, so the
    # objects a row needs only exist once the package is built.
    from aio_pika.robust_channel import RobustChannel
    from aio_pika.robust_connection import RobustConnection

    from faststream.rabbit import annotations
    from faststream.rabbit.broker.broker import RabbitBroker as RabbitBrokerDriver
    from faststream.rabbit.message import RabbitMessage as RabbitMessageDriver
    from faststream.rabbit.publisher.producer import AioPikaFastProducer

    return MappingProxyType(
        {
            RobustConnection: UnderlyingDriverAnnotation(
                type_hint=annotations.Connection,
                module="faststream.rabbit.annotations",
                name="Connection",
            ),
            RobustChannel: UnderlyingDriverAnnotation(
                type_hint=annotations.Channel,
                module="faststream.rabbit.annotations",
                name="Channel",
            ),
            RabbitBrokerDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RabbitBroker,
                module="faststream.rabbit.annotations",
                name="RabbitBroker",
            ),
            RabbitMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RabbitMessage,
                module="faststream.rabbit.annotations",
                name="RabbitMessage",
            ),
            AioPikaFastProducer: UnderlyingDriverAnnotation(
                type_hint=annotations.RabbitProducer,
                module="faststream.rabbit.annotations",
                name="RabbitProducer",
            ),
        },
    )


@dataclass(kw_only=True)
class RabbitBrokerConfig(BrokerConfig):
    channel_manager: "ChannelManager" = field(default_factory=FakeChannelManager)
    declarer: "RabbitDeclarer" = field(default_factory=FakeRabbitDeclarer)
    producer: "AioPikaFastProducer" = field(default_factory=FakeAioPikaFastProducer)

    virtual_host: str = ""
    app_id: str | None = None

    default_driver_annotations: "Mapping[Any, Any]" = field(
        default_factory=_context_annotations_factory,
    )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id: {id(self)})"

    def connect(self, connection: "RobustConnection") -> None:
        self.channel_manager.connect(connection)
        self.producer.connect(
            serializer=self.fd_config._serializer,
            codec=self.broker_codec or DefaultCodec(),
        )

    def disconnect(self) -> None:
        self.channel_manager.disconnect()
        self.declarer.disconnect()
        self.producer.disconnect()
