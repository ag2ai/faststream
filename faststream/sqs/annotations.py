from aiobotocore.client import AioBaseClient

from faststream._compat import Annotated
from faststream.annotations import ContextRepo, Logger, NoCast
from faststream.sqs.broker import SQSBroker as SB  # NOQA
from faststream.sqs.message import SQSMessage as SM  # NOQA
from faststream.sqs.producer import SQSFastProducer
from faststream.utils.context import Context

__all__ = (
    "Logger",
    "ContextRepo",
    "NoCast",
    "SQSBroker",
    "SQSMessage",
    "SQSProducer",
    "client",
    "queue_url",
)

SQSBroker = Annotated[SB, Context("broker")]
SQSMessage = Annotated[SM, Context("message")]
SQSProducer = Annotated[SQSFastProducer, Context("broker._producer")]
client = Annotated[AioBaseClient, Context("client")]
queue_url = Annotated[str, Context("queue_url")]
