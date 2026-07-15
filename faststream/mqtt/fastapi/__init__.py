from typing import Annotated

import zmqtt

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.mqtt.broker.broker import MQTTBroker as MB  # noqa: N814
from faststream.mqtt.message import MQTTMessage as MM  # noqa: N814

from .fastapi import MQTTRouter

__all__ = (
    "Client",
    "Context",
    "ContextRepo",
    "Logger",
    "MQTTBroker",
    "MQTTMessage",
    "MQTTRouter",
)

MQTTMessage = Annotated[MM, Context("message")]
MQTTBroker = Annotated[MB, Context("broker")]
Client = Annotated[zmqtt.MQTTClient, Context("broker._connection")]
