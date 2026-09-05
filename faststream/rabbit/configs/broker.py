from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream.rabbit.helpers.channel_manager import FakeChannelManager
from faststream.rabbit.helpers.declarer import FakeRabbitDeclarer
from faststream.rabbit.publisher.producer import FakeAioPikaFastProducer

if TYPE_CHECKING:
    from aio_pika import RobustConnection

    from faststream.rabbit.helpers import ChannelManager, RabbitDeclarer
    from faststream.rabbit.publisher.producer import AioPikaFastProducer


# Driver class to the context annotation that injects it, both as import
# paths so this table needs no imports of its own.
CONTEXT_ANNOTATIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "aio_pika.robust_connection.RobustConnection": "faststream.rabbit.annotations.Connection",
        "aio_pika.robust_channel.RobustChannel": "faststream.rabbit.annotations.Channel",
        "faststream.rabbit.broker.broker.RabbitBroker": "faststream.rabbit.annotations.RabbitBroker",
        "faststream.rabbit.message.RabbitMessage": "faststream.rabbit.annotations.RabbitMessage",
        "faststream.rabbit.publisher.producer.AioPikaFastProducer": "faststream.rabbit.annotations.RabbitProducer",
    },
)


@dataclass(kw_only=True)
class RabbitBrokerConfig(BrokerConfig):
    channel_manager: "ChannelManager" = field(default_factory=FakeChannelManager)
    declarer: "RabbitDeclarer" = field(default_factory=FakeRabbitDeclarer)
    producer: "AioPikaFastProducer" = field(default_factory=FakeAioPikaFastProducer)

    virtual_host: str = ""
    app_id: str | None = None

    underlying_driver_annotations: "Mapping[str, str]" = CONTEXT_ANNOTATIONS

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
