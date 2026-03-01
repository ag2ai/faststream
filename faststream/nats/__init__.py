from faststream._internal.testing.app import TestApp

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

    from .annotations import NatsMessage
    from .broker import NatsBroker, NatsPublisher, NatsRoute, NatsRouter
    from .response import NatsPublishCommand, NatsResponse
    from .schemas import JStream, KvWatch, ObjWatch, PubAck, PullSub, Schedule
    from .testing import TestNatsBroker

except ImportError as e:
    if "'nats'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_NATS

    raise ImportError(INSTALL_FASTSTREAM_NATS) from e


__all__ = (
    "AckPolicy",
    "ConsumerConfig",
    "DeliverPolicy",
    "DiscardPolicy",
    "ExternalStream",
    "JStream",
    "KvWatch",
    "NatsBroker",
    "NatsMessage",
    "NatsPublishCommand",
    "NatsPublisher",
    "NatsResponse",
    "NatsRoute",
    "NatsRouter",
    "ObjWatch",
    "Placement",
    "PubAck",
    "PullSub",
    "RePublish",
    "ReplayPolicy",
    "RetentionPolicy",
    "Schedule",
    "StorageType",
    "StreamConfig",
    "StreamSource",
    "TestApp",
    "TestNatsBroker",
)
