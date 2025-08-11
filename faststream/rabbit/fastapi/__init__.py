from typing import Annotated

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.rabbit.broker import RabbitBroker as RB
from faststream.rabbit.message import RabbitMessage as RM
from faststream.rabbit.publisher.producer import AioPikaFastProducer

from .fastapi import RabbitRouter

RabbitMessage = Annotated[RM, Context("message")]
RabbitBroker = Annotated[RB, Context("broker")]
RabbitProducer = Annotated[AioPikaFastProducer, Context("broker._producer")]

__all__ = (
    "Context",
    "ContextRepo",
    "Logger",
    "RabbitBroker",
    "RabbitMessage",
    "RabbitProducer",
    "RabbitRouter",
)
