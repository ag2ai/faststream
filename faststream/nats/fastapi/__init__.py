from typing import Annotated

from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.nats.broker import NatsBroker as NB
from faststream.nats.message import NatsMessage as NM

from .fastapi import NatsRouter

NatsMessage = Annotated[NM, Context("message")]
NatsBroker = Annotated[NB, Context("broker")]
Client = Annotated[NatsClient, Context("broker.config.connection_state.connection")]
JsClient = Annotated[JetStreamContext, Context("broker.config.connection_state.stream")]

__all__ = (
    "Client",
    "Context",
    "ContextRepo",
    "JsClient",
    "Logger",
    "NatsBroker",
    "NatsMessage",
    "NatsMessage",
    "NatsRouter",
)
