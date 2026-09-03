from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, Final

from zmqtt import MQTTClient

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
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


CONTEXT_ANNOTATIONS: Final[Mapping[type[Any], Any]] = MappingProxyType(
    {
        MQTTClient: Client,
        MB: MQTTBroker,
        MM: MQTTMessage,
    },
)
