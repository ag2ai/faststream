from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, Final

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.confluent.broker import KafkaBroker as KB
from faststream.confluent.helpers import AsyncConfluentConsumer
from faststream.confluent.message import KafkaMessage as KM
from faststream.confluent.publisher.producer import AsyncConfluentFastProducer
from faststream.params import NoCast

__all__ = (
    "Consumer",
    "ContextRepo",
    "KafkaBroker",
    "KafkaMessage",
    "KafkaProducer",
    "Logger",
    "NoCast",
)

Consumer = Annotated[AsyncConfluentConsumer, Context("handler_.consumer")]
KafkaMessage = Annotated[KM, Context("message")]
KafkaBroker = Annotated[KB, Context("broker")]
KafkaProducer = Annotated[AsyncConfluentFastProducer, Context("broker._producer")]


CONTEXT_ANNOTATIONS: Final[Mapping[type[Any], Any]] = MappingProxyType(
    {
        AsyncConfluentConsumer: Consumer,
        KB: KafkaBroker,
        KM: KafkaMessage,
        AsyncConfluentFastProducer: KafkaProducer,
    },
)
