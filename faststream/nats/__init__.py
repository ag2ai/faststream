from faststream.exceptions import INSTALL_FASTSTREAM_NATS

try:
    from nats.js.api import (
        AckPolicy,
        ConsumerConfig,
        DeliverPolicy,
        DiscardPolicy,
        ExternalStream,
        Placement,
        RePublish,
        ReplayPolicy,
        RetentionPolicy,
        StorageType,
        StreamConfig,
        StreamSource,
    )
except ImportError(INSTALL_FASTSTREAM_NATS) as e:
    raise ImportError(INSTALL_FASTSTREAM_NATS) from e

from faststream.nats.annotations import NatsMessage
from faststream.nats.broker.broker import NatsBroker
from faststream.nats.response import NatsResponse
from faststream.nats.router import NatsPublisher, NatsRoute, NatsRouter
from faststream.nats.schemas import JStream, KvWatch, ObjWatch, PullSub
from faststream.nats.testing import TestNatsBroker
from faststream.testing.app import TestApp

__all__ = (
    "AckPolicy",
    # Nats imports
    "ConsumerConfig",
    "DeliverPolicy",
    "DiscardPolicy",
    "ExternalStream",
    "JStream",
    "KvWatch",
    "NatsBroker",
    "NatsMessage",
    "NatsPublisher",
    "NatsResponse",
    "NatsRoute",
    "NatsRouter",
    "ObjWatch",
    "Placement",
    "PullSub",
    "RePublish",
    "ReplayPolicy",
    "RetentionPolicy",
    "StorageType",
    "StreamConfig",
    "StreamSource",
    "TestApp",
    "TestNatsBroker",
)
