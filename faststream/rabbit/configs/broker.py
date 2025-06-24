from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs import BrokerConfig
from faststream.rabbit.helpers.channel_manager import FakeChannelManager
from faststream.rabbit.helpers.declarer import FakeRabbitDeclarer
from faststream.rabbit.publisher.producer import FakeAioPikaFastProducer

if TYPE_CHECKING:
    from aio_pika import RobustConnection

    from faststream.rabbit.helpers import ChannelManager, RabbitDeclarer
    from faststream.rabbit.publisher.producer import AioPikaFastProducer


@dataclass(kw_only=True)
class RabbitBrokerConfig(BrokerConfig):
    channel_manager: "ChannelManager" = field(default_factory=FakeChannelManager)
    declarer: "RabbitDeclarer" = field(default_factory=FakeRabbitDeclarer)
    producer: "AioPikaFastProducer" = field(default_factory=FakeAioPikaFastProducer)

    virtual_host: str = ""
    app_id: str | None = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id: {id(self)})"

    def connect(self, connection: "RobustConnection") -> None:
        self.channel_manager.connect(connection)
        self.producer.connect(serializer=self.fd_config._serializer)

    def disconnect(self) -> None:
        self.channel_manager.disconnect()
        self.declarer.disconnect()
        self.producer.disconnect()
