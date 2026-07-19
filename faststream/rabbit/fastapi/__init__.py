import warnings
from typing import Annotated

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.rabbit.broker import RabbitBroker as RB
from faststream.rabbit.message import RabbitMessage as RM
from faststream.rabbit.publisher.producer import AioPikaFastProducer

from .fastapi import RabbitRouter

warnings.warn(
    "The integration has been moved to the faststream_fastapi package"
    " and will be removed in 1.0.0 version."
    "\n`pip install faststream_fastapi`"
    "\nhttps://github.com/faststream-community/faststream_fastapi",
    DeprecationWarning,
    stacklevel=2,
)

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
