import warnings
from typing import Annotated

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.confluent.broker import KafkaBroker as KB
from faststream.confluent.message import KafkaMessage as KM
from faststream.confluent.publisher.producer import AsyncConfluentFastProducer

from .fastapi import KafkaRouter

warnings.warn(
    "The integration has been moved to the faststream_fastapi package"
    " and will be removed in 1.0.0 version."
    "\n`pip install faststream_fastapi`"
    "\nhttps://github.com/faststream-community/faststream_fastapi",
    DeprecationWarning,
    stacklevel=2,
)

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
