from typing import Annotated

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast

from .broker import MQBroker as MB
from .message import MQMessage as MM
from .publisher.producer import AsyncMQFastProducer

__all__ = (
    "ContextRepo",
    "Logger",
    "MQBroker",
    "MQMessage",
    "MQProducer",
    "NoCast",
)

MQMessage = Annotated[MM, Context("message")]
MQBroker = Annotated[MB, Context("broker")]
MQProducer = Annotated[AsyncMQFastProducer, Context("broker._producer")]
