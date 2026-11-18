from typing import Annotated

from zmqtt import MQTTClient

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.mqtt.broker.broker import MQTTBroker as MB  # ruff: ignore[camelcase-imported-as-constant]
from faststream.mqtt.message import MQTTMessage as MM  # ruff: ignore[camelcase-imported-as-constant]
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
