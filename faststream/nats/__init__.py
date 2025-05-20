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

    from faststream.testing.app import TestApp

    from .annotations import NatsMessage
    from .broker.broker import NatsBroker
    from .response import NatsResponse
    from .router import NatsPublisher, NatsRoute, NatsRouter
    from .schemas import JStream, KvWatch, ObjWatch, PullSub
    from .testing import TestNatsBroker

except ImportError as e:
    from faststream.exceptions import INSTALL_FASTSTREAM_NATS

    raise ImportError(INSTALL_FASTSTREAM_NATS) from e


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
