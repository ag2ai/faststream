from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from typing_extensions import TypedDict

from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream.nats.broker.state import BrokerState
from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer
from faststream.nats.publisher.producer import FakeNatsFastProducer

if TYPE_CHECKING:
    from nats.aio.client import Client

    from faststream.nats.publisher.producer import NatsFastProducer


class JsInitOptions(TypedDict, total=False):
    prefix: str
    domain: str | None
    timeout: float
    publish_async_max_pending: int


# Driver class to the context annotation that injects it, both as import
# paths so this table needs no imports of its own.
CONTEXT_ANNOTATIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "nats.aio.client.Client": "faststream.nats.annotations.Client",
        "nats.js.client.JetStreamContext": "faststream.nats.annotations.JsClient",
        "nats.js.object_store.ObjectStore": "faststream.nats.annotations.ObjectStorage",
        "faststream.nats.broker.broker.NatsBroker": "faststream.nats.annotations.NatsBroker",
        "faststream.nats.message.NatsMessage": "faststream.nats.annotations.NatsMessage",
        "faststream.nats.message.NatsKvMessage": "faststream.nats.annotations.NatsKvMessage",
    },
)


@dataclass(kw_only=True)
class NatsBrokerConfig(BrokerConfig):
    js_options: JsInitOptions | dict[str, Any] = field(default_factory=dict)

    producer: "NatsFastProducer" = field(default_factory=FakeNatsFastProducer)
    js_producer: "NatsFastProducer" = field(default_factory=FakeNatsFastProducer)
    connection_state: BrokerState = field(default_factory=BrokerState)
    kv_declarer: KVBucketDeclarer = field(default_factory=KVBucketDeclarer)
    os_declarer: OSBucketDeclarer = field(default_factory=OSBucketDeclarer)

    underlying_driver_annotations: "Mapping[str, str]" = CONTEXT_ANNOTATIONS

    def connect(self, connection: "Client") -> None:
        stream = connection.jetstream(**self.js_options)

        self.producer.connect(
            connection,
            serializer=self.fd_config._serializer,
            codec=self.broker_codec or DefaultCodec(),
        )

        self.js_producer.connect(
            stream,
            serializer=self.fd_config._serializer,
            codec=self.broker_codec or DefaultCodec(),
        )
        self.kv_declarer.connect(stream)
        self.os_declarer.connect(stream)

        self.connection_state.connect(connection, stream)

    def disconnect(self) -> None:
        self.producer.disconnect()
        self.js_producer.disconnect()
        self.kv_declarer.disconnect()
        self.os_declarer.disconnect()

        self.connection_state.disconnect()
