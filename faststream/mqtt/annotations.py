from typing import Annotated

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.mqtt.broker.broker import MQTTBroker as MB  # noqa: N814
from faststream.mqtt.message import MQTTMessage as MM  # noqa: N814
from faststream.params import NoCast

__all__ = (
    "ContextRepo",
    "Logger",
    "MQTTBroker",
    "MQTTMessage",
    "NoCast",
)

MQTTMessage = Annotated[MM, Context("message")]
MQTTBroker = Annotated[MB, Context("broker")]
