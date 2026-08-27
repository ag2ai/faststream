from typing import Annotated

from aio_pika import RobustChannel, RobustConnection

from faststream._internal.context import Context
from faststream._internal.di import register_context_annotations
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast
from faststream.rabbit.broker import RabbitBroker as RB
from faststream.rabbit.message import RabbitMessage as RM
from faststream.rabbit.publisher.producer import AioPikaFastProducer

__all__ = (
    "Channel",
    "Connection",
    "ContextRepo",
    "Logger",
    "NoCast",
    "RabbitBroker",
    "RabbitMessage",
    "RabbitProducer",
)

RabbitMessage = Annotated[RM, Context("message")]
RabbitBroker = Annotated[RB, Context("broker")]
RabbitProducer = Annotated[AioPikaFastProducer, Context("broker._producer")]

Channel = Annotated[RobustChannel, Context("broker._channel")]
Connection = Annotated[RobustConnection, Context("broker._connection")]


register_context_annotations(
    __name__,
    {
        RobustConnection: "Connection",
        RobustChannel: "Channel",
        RB: "RabbitBroker",
        RM: "RabbitMessage",
        AioPikaFastProducer: "RabbitProducer",
    },
)
