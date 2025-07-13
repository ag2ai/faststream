from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs import BrokerConfig
from faststream.nats.broker.state import BrokerState
from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer
from faststream.nats.publisher.producer import FakeNatsFastProducer

if TYPE_CHECKING:
    from nats.aio.client import Client

    from faststream.nats.publisher.producer import NatsFastProducer


@dataclass(kw_only=True)
class NatsBrokerConfig(BrokerConfig):
    producer: "NatsFastProducer" = field(default_factory=FakeNatsFastProducer)
    js_producer: "NatsFastProducer" = field(default_factory=FakeNatsFastProducer)
    connection_state: BrokerState = field(default_factory=BrokerState)
    kv_declarer: KVBucketDeclarer = field(default_factory=KVBucketDeclarer)
    os_declarer: OSBucketDeclarer = field(default_factory=OSBucketDeclarer)

    def connect(self, connection: "Client") -> None:
        stream = connection.jetstream()

        self.producer.connect(connection, serializer=self.fd_config._serializer)

        self.js_producer.connect(stream, serializer=self.fd_config._serializer)
        self.kv_declarer.connect(stream)
        self.os_declarer.connect(stream)

        self.connection_state.connect(connection, stream)

    def disconnect(self) -> None:
        self.producer.disconnect()
        self.js_producer.disconnect()
        self.kv_declarer.disconnect()
        self.os_declarer.disconnect()

        self.connection_state.disconnect()
