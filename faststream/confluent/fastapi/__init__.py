from typing import Annotated

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.confluent.broker import KafkaBroker as KB
from faststream.confluent.message import KafkaMessage as KM
from faststream.confluent.publisher.producer import AsyncConfluentFastProducer

from .fastapi import KafkaRouter

__all__ = (
    "Context",
    "ContextRepo",
    "KafkaBroker",
    "KafkaMessage",
    "KafkaProducer",
    "KafkaRouter",
    "Logger",
)

KafkaMessage = Annotated[KM, Context("message")]
KafkaBroker = Annotated[KB, Context("broker")]
KafkaProducer = Annotated[AsyncConfluentFastProducer, Context("broker._producer")]
