from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream.rabbit.helpers.channel_manager import FakeChannelManager
from faststream.rabbit.helpers.declarer import FakeRabbitDeclarer
from faststream.rabbit.publisher.producer import FakeAioPikaFastProducer

if TYPE_CHECKING:
    from collections.abc import Mapping

    from aio_pika import RobustConnection

    from faststream.rabbit.helpers import ChannelManager, RabbitDeclarer
    from faststream.rabbit.publisher.producer import AioPikaFastProducer


def _context_annotations() -> "Mapping[type[Any], Any]":
    # `annotations` reaches this module through the broker, so it can only be
    # imported once the package is built.
    from faststream.rabbit.annotations import CONTEXT_ANNOTATIONS

    return CONTEXT_ANNOTATIONS


@dataclass(kw_only=True)
class RabbitBrokerConfig(BrokerConfig):
    channel_manager: "ChannelManager" = field(default_factory=FakeChannelManager)
    declarer: "RabbitDeclarer" = field(default_factory=FakeRabbitDeclarer)
    producer: "AioPikaFastProducer" = field(default_factory=FakeAioPikaFastProducer)

    virtual_host: str = ""
    app_id: str | None = None

    underlying_driver_annotations: "Mapping[type[Any], Any]" = field(
        default_factory=_context_annotations
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
