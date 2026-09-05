from typing import Annotated

from nats.aio.client import Client as _NatsClient
from nats.js.client import JetStreamContext as _JetStream
from nats.js.object_store import ObjectStore as _ObjectStore

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.nats.broker import NatsBroker as _Broker
from faststream.nats.message import (
    NatsKvMessage as _KVMessage,
    NatsMessage as _Message,
)
from faststream.nats.subscriber.usecases.object_storage_subscriber import (
    OBJECT_STORAGE_CONTEXT_KEY,
)
from faststream.params import NoCast

__all__ = (
    "Client",
    "ContextRepo",
    "JsClient",
    "Logger",
    "NatsBroker",
    "NatsMessage",
    "NoCast",
    "ObjectStorage",
)

ObjectStorage = Annotated[_ObjectStore, Context(OBJECT_STORAGE_CONTEXT_KEY)]
NatsMessage = Annotated[_Message, Context("message")]
NatsKvMessage = Annotated[_KVMessage, Context("message")]
NatsBroker = Annotated[_Broker, Context("broker")]
Client = Annotated[_NatsClient, Context("broker.config.connection_state.connection")]
JsClient = Annotated[_JetStream, Context("broker.config.connection_state.stream")]
