from typing import Annotated

from zmqtt import MQTTClient

from faststream.annotations import ContextRepo, Logger
from faststream.context import Context
from faststream.mqtt.broker.broker import MQTTBroker as MB  # noqa: N814
from faststream.mqtt.message import MQTTMessage as MM  # noqa: N814
from faststream.params import NoCast

__all__ = (
    "Client",
    "ContextRepo",
    "Logger",
    "MQTTBroker",
    "MQTTMessage",
    "NoCast",
)

Client = Annotated[MQTTClient, Context("broker._connection")]
MQTTMessage = Annotated[MM, Context("message")]
MQTTBroker = Annotated[MB, Context("broker")]
