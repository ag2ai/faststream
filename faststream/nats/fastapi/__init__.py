import warnings
from typing import Annotated

from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.nats.broker import NatsBroker as NB
from faststream.nats.message import NatsMessage as NM

from .fastapi import NatsRouter

warnings.warn(
    "The integration has been moved to the faststream_fastapi package"
    " and will be removed in 1.0.0 version."
    "\n`pip install faststream_fastapi`"
    "\nhttps://github.com/faststream-community/faststream_fastapi",
    DeprecationWarning,
    stacklevel=2,
)

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
    "NatsRouter",
)
