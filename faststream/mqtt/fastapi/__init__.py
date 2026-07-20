import warnings
from typing import Annotated

import zmqtt

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.mqtt.broker.broker import MQTTBroker as MB  # noqa: N814
from faststream.mqtt.message import MQTTMessage as MM  # noqa: N814

from .fastapi import MQTTRouter

warnings.warn(
    "The integration has been moved to the faststream_fastapi package"
    " and will be removed in 1.0.0 version."
    "\n`pip install faststream_fastapi`"
    "\nhttps://github.com/faststream-community/faststream_fastapi",
    DeprecationWarning,
    stacklevel=2,
)

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
