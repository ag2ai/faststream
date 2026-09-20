from importlib.util import find_spec
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
    from .testing import TestNatsBroker

except ImportError as e:
    # the package is installed: the failure is its own, not a missing extra
    if find_spec("nats") is not None:
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
    "NatsParserType",
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
