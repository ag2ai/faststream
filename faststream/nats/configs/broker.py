from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from typing_extensions import TypedDict

from faststream._internal.configs import BrokerConfig, UnderlyingDriverAnnotation
from faststream._internal.parser import DefaultCodec
from faststream.nats.broker.state import BrokerState
from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer
from faststream.nats.publisher.producer import FakeNatsFastProducer

if TYPE_CHECKING:
    from collections.abc import Mapping

    from nats.aio.client import Client

    from faststream.nats.publisher.producer import NatsFastProducer


class JsInitOptions(TypedDict, total=False):
    prefix: str
    domain: str | None
    timeout: float
    publish_async_max_pending: int


def _context_annotations_factory() -> "Mapping[Any, Any]":
    # `annotations` reaches this module through the broker, so the
    # objects a row needs only exist once the package is built.
    from nats.aio.client import Client as ClientDriver  # noqa: PLC0415
    from nats.js.client import JetStreamContext  # noqa: PLC0415
    from nats.js.object_store import ObjectStore  # noqa: PLC0415

    from faststream.nats import annotations  # noqa: PLC0415
    from faststream.nats.broker.broker import (  # noqa: PLC0415
        NatsBroker as NatsBrokerDriver,
    )
    from faststream.nats.message import (  # noqa: PLC0415
        NatsKvMessage as NatsKvMessageDriver,
        NatsMessage as NatsMessageDriver,
    )

    return MappingProxyType(
        {
            ClientDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.Client,
                module="faststream.nats.annotations",
                name="Client",
            ),
            JetStreamContext: UnderlyingDriverAnnotation(
                type_hint=annotations.JsClient,
                module="faststream.nats.annotations",
                name="JsClient",
            ),
            ObjectStore: UnderlyingDriverAnnotation(
                type_hint=annotations.ObjectStorage,
                module="faststream.nats.annotations",
                name="ObjectStorage",
            ),
            NatsBrokerDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.NatsBroker,
                module="faststream.nats.annotations",
                name="NatsBroker",
            ),
            NatsMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.NatsMessage,
                module="faststream.nats.annotations",
                name="NatsMessage",
            ),
            NatsKvMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.NatsKvMessage,
                module="faststream.nats.annotations",
                name="NatsKvMessage",
            ),
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

    default_driver_annotations: "Mapping[Any, Any]" = field(
        default_factory=_context_annotations_factory,
    )

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
