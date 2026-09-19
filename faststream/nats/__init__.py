from typing import TYPE_CHECKING, TypeAlias

from faststream._internal.parser import ParserProto
from faststream._internal.testing.app import TestApp

if TYPE_CHECKING:
    from nats.aio.msg import Msg

NatsParserType: TypeAlias = ParserProto["Msg"]

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
    from .security import (
        NatsCredentials,
        NatsJWT,
        NatsNKey,
        NatsSecurity,
        NatsToken,
        NatsUserPassword,
    )
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
    "NatsCredentials",
    "NatsJWT",
    "NatsMessage",
    "NatsNKey",
    "NatsParserType",
    "NatsPublishCommand",
    "NatsPublisher",
    "NatsResponse",
    "NatsRoute",
    "NatsRouter",
    "NatsSecurity",
    "NatsToken",
    "NatsUserPassword",
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
